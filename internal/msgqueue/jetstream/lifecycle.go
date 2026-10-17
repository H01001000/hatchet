package jetstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	natsgo "github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
	"github.com/hatchet-dev/hatchet/pkg/telemetry"
)

const (
	queueMetadataKey = "hatchet_queue"
	deathHeader      = "Hatchet-Death-Count"
	delayHeader      = "Hatchet-Retry-Delay"
)

var ErrQueueInUse = errors.New("exclusive JetStream queue already has a subscriber")
var errClaimed = errors.New("JetStream message is already being processed")

type queueDefinition struct {
	QueueName     string `json:"name"`
	IsDurable     bool   `json:"durable"`
	DeleteOnClose bool   `json:"auto_delete"`
	IsExclusive   bool   `json:"exclusive"`
	Expirable     bool   `json:"expirable"`
	DeadLetter    string `json:"dlq,omitempty"`
	AutomaticDLQ  bool   `json:"auto_dlq,omitempty"`
}

func describeQueue(q msgqueue.Queue) queueDefinition {
	// Dispatcher DLQ publishers use a DLQ view of the static queue. Its physical
	// declaration also includes the automatic retry queue used by its controller.
	if q.Name() == msgqueue.DISPATCHER_DEAD_LETTER_QUEUE.Name() {
		q = msgqueue.DISPATCHER_DEAD_LETTER_QUEUE
	}
	d := queueDefinition{QueueName: q.Name(), IsDurable: q.Durable(), DeleteOnClose: q.AutoDeleted(), IsExclusive: q.Exclusive(), Expirable: q.IsExpirable()}
	if dlq := q.DLQ(); dlq != nil {
		d.DeadLetter, d.AutomaticDLQ = dlq.Name(), dlq.IsAutoDLQ()
	}
	return d
}
func (q queueDefinition) Name() string      { return q.QueueName }
func (q queueDefinition) Durable() bool     { return q.IsDurable }
func (q queueDefinition) AutoDeleted() bool { return q.DeleteOnClose }
func (q queueDefinition) Exclusive() bool   { return q.IsExclusive }
func (q queueDefinition) IsExpirable() bool { return q.Expirable }
func (q queueDefinition) IsDLQ() bool       { return false }
func (q queueDefinition) IsAutoDLQ() bool   { return q.AutomaticDLQ }
func (q queueDefinition) DLQ() msgqueue.Queue {
	if q.DeadLetter == "" {
		return nil
	}
	return queueDefinition{QueueName: q.DeadLetter, IsDurable: true, AutomaticDLQ: q.AutomaticDLQ}
}

func (t *MessageQueueImpl) initLifecycle() error {
	for _, spec := range []struct {
		suffix string
		ttl    time.Duration
		target *natsjs.KeyValue
	}{
		{"owners", t.leaseTimeout, &t.owners},
		{"claims", t.ackWait + 2*t.leaseTimeout + 2*t.requestTimeout, &t.claims},
		{"catalog", 0, &t.catalog},
	} {
		ctx, cancel := context.WithTimeout(t.ctx, t.requestTimeout)
		kv, err := t.pubJS.CreateKeyValue(ctx, natsjs.KeyValueConfig{
			Bucket: t.streamPrefix + "_" + spec.suffix, History: 1, TTL: spec.ttl,
			Storage: natsjs.FileStorage, Replicas: t.replicas, MaxBytes: 64 << 20,
		})
		cancel()
		if err != nil {
			return fmt.Errorf("initialize JetStream %s: %w", spec.suffix, err)
		}
		*spec.target = kv
	}
	return nil
}

func (t *MessageQueueImpl) markSubscribed(ctx context.Context, state queueState) error {
	_, err := t.catalog.Put(ctx, state.stream.CachedInfo().Config.Name, []byte(time.Now().UTC().Format(time.RFC3339Nano)))
	return err
}

func (t *MessageQueueImpl) invalidateQueue(name string) {
	t.queueMu.Lock()
	delete(t.queues, name)
	t.queueMu.Unlock()
}

type queueLease struct {
	t                 *MessageQueueImpl
	key               string
	revision          uint64
	exclusiveKey      string
	exclusiveRevision uint64
	stop              context.CancelFunc
	done              chan struct{}
	once              sync.Once
	ctx               context.Context
	operationCancel   context.CancelFunc
	catalogName       string
}

func (t *MessageQueueImpl) acquireSubscription(ctx context.Context, state queueState) (*queueLease, error) {
	key := "p." + state.stream.CachedInfo().Config.Name + "." + strings.ReplaceAll(uuid.NewString(), "-", "")
	revision, err := t.owners.Create(ctx, key, []byte("active"))
	if err != nil {
		return nil, err
	}
	lease := &queueLease{t: t, key: key, revision: revision, catalogName: state.stream.CachedInfo().Config.Name}
	if state.definition.IsExclusive {
		lease.exclusiveKey = "x." + state.stream.CachedInfo().Config.Name
		lease.exclusiveRevision, err = t.owners.Create(ctx, lease.exclusiveKey, []byte(key))
		if err != nil {
			_ = t.owners.Delete(ctx, key, natsjs.LastRevision(revision))
			return nil, fmt.Errorf("%w: %s: %v", ErrQueueInUse, state.definition.Name(), err)
		}
	}
	return lease, nil
}

// renew starts synchronously so release can always join the heartbeat goroutine.
func (l *queueLease) renew(ctx context.Context, onLost func()) {
	ctx, l.stop = context.WithCancel(ctx)
	l.done = make(chan struct{})
	go func() {
		defer close(l.done)
		ticker := time.NewTicker(l.t.leaseTimeout / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			request, cancel := context.WithTimeout(ctx, min(l.t.requestTimeout, l.t.leaseTimeout/3))
			rev, err := l.t.owners.Update(request, l.key, []byte("active"), l.revision)
			if err == nil {
				l.revision = rev
			}
			if err == nil && l.exclusiveKey != "" {
				rev, err = l.t.owners.Update(request, l.exclusiveKey, []byte(l.key), l.exclusiveRevision)
				if err == nil {
					l.exclusiveRevision = rev
				}
			}
			if err == nil && l.catalogName != "" {
				_, err = l.t.catalog.Put(request, l.catalogName, []byte(time.Now().UTC().Format(time.RFC3339Nano)))
			}
			cancel()
			if err != nil {
				l.t.l.Warn().Err(err).Msg("JetStream subscriber lease lost")
				onLost()
				return
			}
		}
	}()
}

func (l *queueLease) release() {
	l.once.Do(func() {
		if l.stop != nil {
			l.stop()
			<-l.done
		}
		if l.operationCancel != nil {
			l.operationCancel()
		}
		ctx, cancel := context.WithTimeout(context.Background(), l.t.requestTimeout)
		defer cancel()
		if l.catalogName != "" {
			_, _ = l.t.catalog.Put(ctx, l.catalogName, []byte(time.Now().UTC().Format(time.RFC3339Nano)))
		}
		if l.exclusiveKey != "" {
			_ = l.t.owners.Delete(ctx, l.exclusiveKey, natsjs.LastRevision(l.exclusiveRevision))
		}
		_ = l.t.owners.Delete(ctx, l.key, natsjs.LastRevision(l.revision))
	})
}

func (t *MessageQueueImpl) acquireGate(ctx context.Context, q msgqueue.Queue) (*queueLease, error) {
	if err := validateQueue(q); err != nil {
		return nil, err
	}
	name, _ := t.queueNames(q)
	request, cancel := context.WithTimeout(ctx, t.requestTimeout)
	defer cancel()
	for {
		revision, err := t.owners.Create(request, "g."+name, []byte("active"))
		if err == nil {
			gate := &queueLease{t: t, key: "g." + name, revision: revision}
			gate.ctx, gate.operationCancel = context.WithCancel(ctx)
			gate.renew(gate.ctx, gate.operationCancel)
			return gate, nil
		}
		if !errors.Is(err, natsjs.ErrKeyExists) {
			return nil, err
		}
		select {
		case <-request.Done():
			return nil, request.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (t *MessageQueueImpl) exclusiveConsumer(ctx context.Context, state queueState, lease *queueLease) (natsjs.Consumer, error) {
	lister := state.stream.ConsumerNames(ctx)
	for name := range lister.Name() {
		if err := state.stream.DeleteConsumer(ctx, name); err != nil && !errors.Is(err, natsjs.ErrConsumerNotFound) {
			return nil, err
		}
	}
	if err := lister.Err(); err != nil {
		return nil, err
	}
	parts := strings.Split(lease.key, ".")
	return state.stream.CreateConsumer(ctx, natsjs.ConsumerConfig{
		Durable: consumerName + "_" + parts[len(parts)-1], FilterSubject: state.subject,
		AckPolicy: natsjs.AckExplicitPolicy, AckWait: t.ackWait, MaxDeliver: -1, MaxAckPending: -1,
	})
}

type messageClaim struct {
	t        *MessageQueueImpl
	key      string
	revision uint64
}
type claimValue struct {
	Owner    string `json:"owner"`
	Expiring bool   `json:"expiring"`
}

func stateKey(t *MessageQueueImpl, q msgqueue.Queue, seq uint64, published time.Time) string {
	name, _ := t.queueNames(q)
	return name + "." + strconv.FormatUint(seq, 10) + "." + strconv.FormatInt(published.UnixNano(), 10)
}

func messageSource(stream string, published time.Time) string {
	return stream + "_" + strconv.FormatInt(published.UnixNano(), 10)
}

// ownerAlive verifies an apparently missing presence key with a leader-side CAS.
// A lagging KV read cannot turn a live subscriber into an expired one.
func (t *MessageQueueImpl) ownerAlive(ctx context.Context, key string) (bool, error) {
	entry, err := t.owners.Get(ctx, key)
	if err == nil {
		return string(entry.Value()) != "expired", nil
	}
	if !errors.Is(err, natsjs.ErrKeyNotFound) && !errors.Is(err, natsjs.ErrKeyDeleted) {
		return false, err
	}
	_, err = t.owners.Create(ctx, key, []byte("expired"))
	if errors.Is(err, natsjs.ErrKeyExists) {
		return true, nil
	}
	return false, err
}

func (t *MessageQueueImpl) claimMessage(ctx context.Context, key, owner string, expiring bool) (*messageClaim, error) {
	value, _ := json.Marshal(claimValue{Owner: owner, Expiring: expiring})
	entry, err := t.claims.Get(ctx, key)
	var revision uint64
	if errors.Is(err, natsjs.ErrKeyNotFound) || errors.Is(err, natsjs.ErrKeyDeleted) {
		revision, err = t.claims.Create(ctx, key, value)
	} else if err == nil {
		var previous claimValue
		if err = json.Unmarshal(entry.Value(), &previous); err != nil {
			return nil, err
		}
		active := time.Since(entry.Created()) < 2*t.requestTimeout
		if !previous.Expiring {
			active, err = t.ownerAlive(ctx, previous.Owner)
			if err != nil {
				return nil, err
			}
			active = active && time.Since(entry.Created()) < t.ackWait
		}
		if active {
			return nil, errClaimed
		}
		revision, err = t.claims.Update(ctx, key, value, entry.Revision())
	}
	if err != nil {
		return nil, err
	}
	return &messageClaim{t: t, key: key, revision: revision}, nil
}
func (c *messageClaim) release() {
	ctx, cancel := context.WithTimeout(context.Background(), c.t.requestTimeout)
	defer cancel()
	_ = c.t.claims.Delete(ctx, c.key, natsjs.LastRevision(c.revision))
}

func deathCount(headers natsgo.Header) uint64 {
	n, _ := strconv.ParseUint(headers.Get(deathHeader), 10, 64)
	return n
}
func readyAfter(headers natsgo.Header, published time.Time) time.Time {
	delay, err := time.ParseDuration(headers.Get(delayHeader))
	if err != nil {
		return published
	}
	return published.Add(delay)
}

func (t *MessageQueueImpl) forward(ctx context.Context, source, target msgqueue.Queue, body []byte, headers natsgo.Header, stream string, seq uint64, retry bool) error {
	if target == nil {
		return nil
	}
	state, err := t.initQueue(ctx, target)
	if err != nil {
		return err
	}
	copyHeaders := make(natsgo.Header)
	for k, values := range headers {
		copyHeaders[k] = append([]string(nil), values...)
	}
	copyHeaders.Del(natsgo.MsgIdHdr)
	if retry {
		count := deathCount(headers)
		if count < ^uint64(0) {
			count++
		}
		copyHeaders.Set(deathHeader, strconv.FormatUint(count, 10))
		copyHeaders.Set(delayHeader, t.deadLetterBackoff.String())
	} else {
		copyHeaders.Del(delayHeader)
	}
	copyHeaders.Set(natsgo.MsgIdHdr, stream+"_"+strconv.FormatUint(seq, 10)+"_"+state.stream.CachedInfo().Config.Name)
	_, err = t.pubJS.PublishMsg(ctx, &natsgo.Msg{Subject: state.subject, Header: copyHeaders, Data: body})
	return err
}

func (t *MessageQueueImpl) retryMessage(ctx context.Context, q, root msgqueue.Queue, delivery natsjs.Msg, metadata *natsjs.MsgMetadata) error {
	target := retryDestination(root)
	if target == nil {
		return nil
	}
	return t.forward(ctx, q, target, delivery.Data(), delivery.Headers(), messageSource(metadata.Stream, metadata.Timestamp), metadata.Sequence.Stream, target.IsAutoDLQ())
}

func retryDestination(root msgqueue.Queue) msgqueue.Queue {
	if root.IsDLQ() && root.IsAutoDLQ() {
		return root
	}
	return root.DLQ()
}

func (t *MessageQueueImpl) handleLive(ctx context.Context, q, root msgqueue.Queue, raw *natsgo.Msg, preAck, postAck msgqueue.MsgHandler) {
	if ctx.Err() != nil {
		return
	}
	reject := func() {
		target := retryDestination(root)
		if err := t.forward(ctx, q, target, raw.Data, raw.Header, "live_"+uuid.NewString(), 0, target != nil && target.IsAutoDLQ()); err != nil {
			t.l.Error().Err(err).Msg("could not forward immediately-expiring message")
		}
	}
	var msg msgqueue.Message
	if err := json.Unmarshal(raw.Data, &msg); err != nil {
		reject()
		return
	}
	if msg.Compressed {
		payloads, err := msgqueue.DecompressPayloads(msg.Payloads)
		if err != nil {
			reject()
			return
		}
		msg.Payloads, msg.Compressed = payloads, false
	}
	ctx, span := telemetry.NewSpanWithCarrier(ctx, "handle_message", msg.OtelCarrier)
	defer span.End()
	if err := preAck(&msg); err != nil {
		t.l.Warn().Err(err).Msg("immediately-expiring message rejected")
		if !isPermanentPreAckError(err) {
			reject()
		}
		return
	}
	if err := postAck(&msg); err != nil {
		t.l.Warn().Err(err).Msg("error in live post-ack hook")
	}
}

func (t *MessageQueueImpl) hasSubscribers(ctx context.Context, name string) (bool, error) {
	lister, err := t.owners.ListKeysFiltered(ctx, "p."+name+".>")
	if errors.Is(err, natsjs.ErrNoKeysFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer func() { _ = lister.Stop() }()
	for key := range lister.Keys() {
		active, err := t.ownerAlive(ctx, key)
		if err != nil {
			return false, err
		}
		if active {
			return true, nil
		}
	}
	return false, nil
}

func (t *MessageQueueImpl) maintainQueues() {
	defer close(t.maintenanceDone)
	ticker := time.NewTicker(t.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
		}
		ctx, cancel := context.WithTimeout(t.ctx, 2*t.requestTimeout)
		if err := t.sweepQueues(ctx); err != nil && t.ctx.Err() == nil {
			t.l.Warn().Err(err).Msg("JetStream queue maintenance failed")
		}
		cancel()
	}
}

func (t *MessageQueueImpl) sweepQueues(ctx context.Context) error {
	revision, err := t.owners.Create(ctx, "maintenance", []byte("active"))
	if errors.Is(err, natsjs.ErrKeyExists) {
		return nil
	}
	if err != nil {
		return err
	}
	lease := &queueLease{t: t, key: "maintenance", revision: revision}
	operation, cancel := context.WithCancel(ctx)
	defer cancel()
	lease.renew(operation, cancel)
	defer lease.release()
	ctx = operation
	lister := t.pubJS.ListStreams(ctx)
	for info := range lister.Info() {
		if !strings.HasPrefix(info.Config.Name, t.streamPrefix+"_") {
			continue
		}
		var definition queueDefinition
		if err := json.Unmarshal([]byte(info.Config.Metadata[queueMetadataKey]), &definition); err != nil {
			continue
		}
		if !definition.Expirable && !definition.DeleteOnClose {
			continue
		}
		stream, err := t.pubJS.Stream(ctx, info.Config.Name)
		if err != nil {
			return err
		}
		if definition.Expirable && info.State.Msgs > 0 {
			if err := t.expireMessages(ctx, stream, definition, info); err != nil {
				return err
			}
		}
		active, err := t.hasSubscribers(ctx, info.Config.Name)
		if err != nil {
			return err
		}
		if active {
			continue
		}
		last := info.State.LastTime
		if last.IsZero() {
			last = info.Created
		}
		record, recordErr := t.catalog.Get(ctx, info.Config.Name)
		if recordErr != nil && !errors.Is(recordErr, natsjs.ErrKeyNotFound) && !errors.Is(recordErr, natsjs.ErrKeyDeleted) {
			return recordErr
		}
		if recordErr == nil {
			if subscriberAt, err := time.Parse(time.RFC3339Nano, string(record.Value())); err == nil && subscriberAt.After(last) {
				last = subscriberAt
			}
		}
		if (definition.DeleteOnClose && recordErr == nil) || (definition.Expirable && time.Since(last) >= t.inactivityTTL) {
			if err := t.deleteInactive(ctx, definition); err != nil {
				return err
			}
		}
	}
	return lister.Err()
}

func (t *MessageQueueImpl) deleteInactive(ctx context.Context, q queueDefinition) error {
	gate, err := t.acquireGate(ctx, q)
	if err != nil {
		return err
	}
	defer gate.release()
	ctx = gate.ctx
	name, _ := t.queueNames(q)
	active, err := t.hasSubscribers(ctx, name)
	if err != nil || active {
		return err
	}
	stream, err := t.pubJS.Stream(ctx, name)
	if errors.Is(err, natsjs.ErrStreamNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return err
	}
	last := info.State.LastTime
	if last.IsZero() {
		last = info.Created
	}
	record, recordErr := t.catalog.Get(ctx, name)
	if recordErr != nil && !errors.Is(recordErr, natsjs.ErrKeyNotFound) && !errors.Is(recordErr, natsjs.ErrKeyDeleted) {
		return recordErr
	}
	if recordErr == nil {
		if at, err := time.Parse(time.RFC3339Nano, string(record.Value())); err == nil && at.After(last) {
			last = at
		}
	}
	shouldDelete := (q.DeleteOnClose && recordErr == nil) || (q.Expirable && time.Since(last) >= t.inactivityTTL)
	if !shouldDelete {
		return nil
	}
	// Expiring queued assignments must reach their DLQ before queue deletion.
	if q.Expirable && info.State.Msgs > 0 {
		return nil
	}
	if err := t.pubJS.DeleteStream(ctx, name); err != nil && !errors.Is(err, natsjs.ErrStreamNotFound) {
		return err
	}
	t.invalidateQueue(q.Name())
	_ = t.catalog.Delete(ctx, name)
	return nil
}

func (t *MessageQueueImpl) expireMessages(ctx context.Context, stream natsjs.Stream, q queueDefinition, info *natsjs.StreamInfo) error {
	start := max(info.State.FirstSeq, t.maintenanceCursor[info.Config.Name])
	if start > info.State.LastSeq {
		start = info.State.FirstSeq
	}
	for seq, checked := start, 0; seq <= info.State.LastSeq && checked < 256; seq, checked = seq+1, checked+1 {
		t.maintenanceCursor[info.Config.Name] = seq + 1
		raw, err := stream.GetMsg(ctx, seq)
		if errors.Is(err, natsjs.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if time.Since(raw.Time) < t.dispatcherTTL {
			t.maintenanceCursor[info.Config.Name] = info.State.FirstSeq
			break
		}
		claim, err := t.claimMessage(ctx, stateKey(t, q, seq, raw.Time), t.identity, true)
		if errors.Is(err, errClaimed) || errors.Is(err, natsjs.ErrKeyExists) || errors.Is(err, natsjs.ErrKeyRevisionMismatch) {
			continue
		}
		if err != nil {
			return err
		}
		err = t.forward(ctx, q, q.DLQ(), raw.Data, raw.Header, messageSource(info.Config.Name, raw.Time), seq, false)
		if err == nil {
			err = stream.DeleteMsg(ctx, seq)
		}
		claim.release()
		if err != nil && !errors.Is(err, natsjs.ErrMsgNotFound) {
			return err
		}
	}
	return nil
}
