package jetstream

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	natsgo "github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
	"github.com/hatchet-dev/hatchet/pkg/logger"
	"github.com/hatchet-dev/hatchet/pkg/telemetry"
)

const (
	consumerTimeout = 5 * time.Minute
	consumerName    = "workers"
	defaultPrefix   = "HATCHET_MQ"
)

var streamPrefixPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

type queueState struct {
	consumer   natsjs.Consumer
	stream     natsjs.Stream
	subject    string
	definition queueDefinition
}

// MessageQueueImpl implements the durable MessageQueue interface using JetStream.
// Each queue has a WorkQueue stream and a competing-consumer delivery path.
type MessageQueueImpl struct {
	ctx      context.Context
	cancel   context.CancelFunc
	configFs []MessageQueueImplOpt
	l        *zerolog.Logger
	qos      atomic.Int32

	pubConn *natsgo.Conn
	subConn *natsgo.Conn
	pubJS   natsjs.JetStream
	subJS   natsjs.JetStream

	streamPrefix           string
	replicas               int
	maxBytes               int64
	ackWait                time.Duration
	requestTimeout         time.Duration
	deadLetterBackoff      time.Duration
	compressor             msgqueue.Compressor
	enableMessageRejection bool
	maxDeathCount          uint64
	identity               string
	dispatcherTTL          time.Duration
	inactivityTTL          time.Duration
	leaseTimeout           time.Duration
	sweepInterval          time.Duration
	owners                 natsjs.KeyValue
	claims                 natsjs.KeyValue
	catalog                natsjs.KeyValue
	maintenanceDone        chan struct{}
	maintenanceCursor      map[string]uint64

	queueMu sync.Mutex
	queues  map[string]queueState

	lifecycleMu sync.Mutex
	closed      bool
	subs        map[*subscription]struct{}
	closeOnce   sync.Once
	supervisors sync.WaitGroup
}

var _ msgqueue.MessageQueue = (*MessageQueueImpl)(nil)

type MessageQueueImplOpt func(*MessageQueueImplOpts)

type MessageQueueImplOpts struct {
	l                      *zerolog.Logger
	url                    string
	username               string
	password               string
	tlsEnabled             bool
	tlsRootCAFile          string
	qos                    int32
	streamPrefix           string
	replicas               int
	maxBytes               int64
	ackWait                time.Duration
	requestTimeout         time.Duration
	deadLetterBackoff      time.Duration
	compressionEnabled     bool
	compressionThreshold   int
	enableMessageRejection bool
	maxDeathCount          uint64
	dispatcherTTL          time.Duration
	inactivityTTL          time.Duration
	leaseTimeout           time.Duration
	sweepInterval          time.Duration
}

func defaultMessageQueueImplOpts() *MessageQueueImplOpts {
	l := logger.NewDefaultLogger("jetstream")
	return &MessageQueueImplOpts{
		l: &l, qos: 100, streamPrefix: defaultPrefix, replicas: 3,
		maxBytes: 1 << 30, ackWait: consumerTimeout, requestTimeout: 5 * time.Second,
		deadLetterBackoff: 5 * time.Second, maxDeathCount: 5,
		compressionThreshold: msgqueue.DefaultCompressionThreshold,
		dispatcherTTL:        20 * time.Second, inactivityTTL: 10 * time.Minute,
		leaseTimeout: 15 * time.Second, sweepInterval: time.Second,
	}
}

func WithLogger(l *zerolog.Logger) MessageQueueImplOpt {
	return func(opts *MessageQueueImplOpts) { opts.l = l }
}

func WithURL(url string) MessageQueueImplOpt {
	return func(opts *MessageQueueImplOpts) { opts.url = url }
}

func WithCredentials(username, password string) MessageQueueImplOpt {
	return func(opts *MessageQueueImplOpts) { opts.username, opts.password = username, password }
}

// WithTLS uses the same TLS-first handshake as Hatchet's Core NATS backend.
// An empty rootCAFile uses the operating system's trusted roots.
func WithTLS(enabled bool, rootCAFile string) MessageQueueImplOpt {
	return func(opts *MessageQueueImplOpts) { opts.tlsEnabled, opts.tlsRootCAFile = enabled, rootCAFile }
}

func WithQos(qos int) MessageQueueImplOpt {
	return func(opts *MessageQueueImplOpts) {
		if qos < 1 || qos > 65536 {
			opts.qos = 0
			return
		}
		opts.qos = int32(qos)
	}
}

func WithStreamPrefix(prefix string) MessageQueueImplOpt {
	return func(opts *MessageQueueImplOpts) { opts.streamPrefix = prefix }
}

func WithReplicas(replicas int) MessageQueueImplOpt {
	return func(opts *MessageQueueImplOpts) { opts.replicas = replicas }
}

// WithMaxBytes bounds each queue's stream. A full stream rejects new messages;
// it never evicts an unacknowledged message to make room for a publication.
func WithMaxBytes(maxBytes int64) MessageQueueImplOpt {
	return func(opts *MessageQueueImplOpts) { opts.maxBytes = maxBytes }
}

func WithAckWait(ackWait time.Duration) MessageQueueImplOpt {
	return func(opts *MessageQueueImplOpts) { opts.ackWait = ackWait }
}

func WithRequestTimeout(timeout time.Duration) MessageQueueImplOpt {
	return func(opts *MessageQueueImplOpts) { opts.requestTimeout = timeout }
}

func WithDeadLetterBackoff(backoff time.Duration) MessageQueueImplOpt {
	return func(opts *MessageQueueImplOpts) { opts.deadLetterBackoff = backoff }
}

func WithDispatcherExpiry(messageTTL, inactivityTTL time.Duration) MessageQueueImplOpt {
	return func(opts *MessageQueueImplOpts) { opts.dispatcherTTL, opts.inactivityTTL = messageTTL, inactivityTTL }
}

func WithLeaseTimeout(timeout time.Duration) MessageQueueImplOpt {
	return func(opts *MessageQueueImplOpts) { opts.leaseTimeout = timeout }
}

func WithSweepInterval(interval time.Duration) MessageQueueImplOpt {
	return func(opts *MessageQueueImplOpts) { opts.sweepInterval = interval }
}

func WithGzipCompression(enabled bool, threshold int) MessageQueueImplOpt {
	return func(opts *MessageQueueImplOpts) {
		opts.compressionEnabled = enabled
		if threshold <= 0 {
			threshold = msgqueue.DefaultCompressionThreshold
		}
		opts.compressionThreshold = threshold
	}
}

// WithMessageRejection bounds rejected-handler attempts. A lost connection or
// acknowledgment does not consume the rejection budget.
func WithMessageRejection(enabled bool, maxDeathCount int) MessageQueueImplOpt {
	return func(opts *MessageQueueImplOpts) {
		opts.enableMessageRejection = enabled
		if maxDeathCount > 0 {
			opts.maxDeathCount = uint64(maxDeathCount)
		} else {
			opts.maxDeathCount = 5
		}
	}
}

func (opts *MessageQueueImplOpts) validate() error {
	if opts.url == "" {
		return errors.New("jetstream requires a URL")
	}
	if opts.l == nil || opts.qos < 1 || opts.qos > 65536 {
		return errors.New("jetstream requires a logger and qos between 1 and 65536")
	}
	if !streamPrefixPattern.MatchString(opts.streamPrefix) {
		return errors.New("jetstream stream prefix must contain 1-32 letters, digits, underscores or hyphens")
	}
	if opts.replicas < 1 || opts.replicas > 5 || opts.maxBytes < int64(msgqueue.MaxMessageSize) {
		return errors.New("jetstream requires 1-5 replicas and max bytes at least MaxMessageSize")
	}
	if opts.ackWait <= 0 || opts.requestTimeout <= 0 || opts.deadLetterBackoff <= 0 {
		return errors.New("jetstream timeouts and retry backoff must be positive")
	}
	if opts.tlsRootCAFile != "" && !opts.tlsEnabled {
		return errors.New("jetstream TLS root CA requires TLS to be enabled")
	}
	if opts.dispatcherTTL <= 0 || opts.inactivityTTL <= 0 || opts.sweepInterval <= 0 || opts.leaseTimeout < 3*opts.sweepInterval {
		return errors.New("jetstream expiry intervals must be positive and lease timeout at least three sweep intervals")
	}
	return nil
}

// New creates independent publisher/subscriber connections and initializes the
// ordinary durable queues, following the RabbitMQ backend's constructor contract.
func New(fs ...MessageQueueImplOpt) (func() error, *MessageQueueImpl, error) {
	opts := defaultMessageQueueImplOpts()
	for _, f := range fs {
		f(opts)
	}
	if err := opts.validate(); err != nil {
		return nil, nil, err
	}

	connectOpts := []natsgo.Option{
		natsgo.Timeout(opts.requestTimeout), natsgo.MaxReconnects(-1), natsgo.ReconnectBufSize(-1),
	}
	if opts.username != "" || opts.password != "" {
		connectOpts = append(connectOpts, natsgo.UserInfo(opts.username, opts.password))
	}
	if opts.tlsEnabled {
		connectOpts = append(connectOpts,
			natsgo.Secure(&tls.Config{MinVersion: tls.VersionTLS13}),
			natsgo.TLSHandshakeFirst(),
		)
		if opts.tlsRootCAFile != "" {
			connectOpts = append(connectOpts, natsgo.RootCAs(opts.tlsRootCAFile))
		}
	}
	pubConn, err := natsgo.Connect(opts.url, connectOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("connect JetStream publisher: %w", err)
	}
	subConn, err := natsgo.Connect(opts.url, connectOpts...)
	if err != nil {
		pubConn.Close()
		return nil, nil, fmt.Errorf("connect JetStream subscriber: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	newLogger := opts.l.With().Str("service", "jetstream").Logger()
	t := &MessageQueueImpl{
		ctx: ctx, cancel: cancel, configFs: append([]MessageQueueImplOpt(nil), fs...), l: &newLogger,
		pubConn: pubConn, subConn: subConn,
		streamPrefix: opts.streamPrefix, replicas: opts.replicas, maxBytes: opts.maxBytes,
		ackWait: opts.ackWait, requestTimeout: opts.requestTimeout, deadLetterBackoff: opts.deadLetterBackoff,
		compressor:             msgqueue.Compressor{Enabled: opts.compressionEnabled, Threshold: opts.compressionThreshold},
		enableMessageRejection: opts.enableMessageRejection, maxDeathCount: opts.maxDeathCount,
		queues: make(map[string]queueState), subs: make(map[*subscription]struct{}),
		identity: uuid.NewString(), dispatcherTTL: opts.dispatcherTTL, inactivityTTL: opts.inactivityTTL,
		leaseTimeout: opts.leaseTimeout, sweepInterval: opts.sweepInterval,
		maintenanceCursor: make(map[string]uint64),
	}
	t.qos.Store(opts.qos)
	if pubConn.MaxPayload() < msgqueue.MaxMessageSize || subConn.MaxPayload() < msgqueue.MaxMessageSize {
		_ = t.close()
		return nil, nil, fmt.Errorf("nats max_payload must be at least %d bytes", msgqueue.MaxMessageSize)
	}
	t.pubJS, err = natsjs.New(pubConn)
	if err == nil {
		t.subJS, err = natsjs.New(subConn)
	}
	if err != nil {
		_ = t.close()
		return nil, nil, fmt.Errorf("create JetStream client: %w", err)
	}
	if err := t.initLifecycle(); err != nil {
		_ = t.close()
		return nil, nil, err
	}
	for _, q := range []msgqueue.Queue{msgqueue.TASK_PROCESSING_QUEUE, msgqueue.OLAP_QUEUE, msgqueue.DISPATCHER_DEAD_LETTER_QUEUE} {
		if _, err := t.initQueue(ctx, q); err != nil {
			_ = t.close()
			return nil, nil, fmt.Errorf("initialize JetStream queue %q: %w", q.Name(), err)
		}
	}
	t.maintenanceDone = make(chan struct{})
	go t.maintainQueues()
	return t.close, t, nil
}

func (t *MessageQueueImpl) close() error {
	t.closeOnce.Do(func() {
		t.lifecycleMu.Lock()
		t.closed = true
		t.cancel()
		subs := make([]*subscription, 0, len(t.subs))
		for sub := range t.subs {
			subs = append(subs, sub)
		}
		t.lifecycleMu.Unlock()
		for _, sub := range subs {
			sub.stop()
		}
		for _, sub := range subs {
			<-sub.done
		}
		t.supervisors.Wait()
		if t.maintenanceDone != nil {
			<-t.maintenanceDone
		}
		t.pubConn.Close()
		t.subConn.Close()
	})
	return nil
}

func (t *MessageQueueImpl) IsReady() bool {
	return t.ctx.Err() == nil && t.pubConn.IsConnected() && t.subConn.IsConnected()
}

func (t *MessageQueueImpl) Clone() (func() error, msgqueue.MessageQueue, error) {
	fs := append(append([]MessageQueueImplOpt(nil), t.configFs...), WithQos(int(t.qos.Load())))
	return New(fs...)
}

// SetQOS sets the concurrency limit for subsequent subscriptions.
func (t *MessageQueueImpl) SetQOS(prefetchCount int) {
	if prefetchCount > 0 && prefetchCount <= 65536 {
		t.qos.Store(int32(prefetchCount))
	}
}

func validateQueue(q msgqueue.Queue) error {
	if q == nil || q.Name() == "" {
		return errors.New("jetstream queue name must not be empty")
	}
	return nil
}

func (t *MessageQueueImpl) queueNames(q msgqueue.Queue) (string, string) {
	digest := sha256.Sum256([]byte(q.Name()))
	name := fmt.Sprintf("%s_%x", t.streamPrefix, digest)
	return name, "hatchet.mq." + name
}

func (t *MessageQueueImpl) initQueue(ctx context.Context, q msgqueue.Queue) (queueState, error) {
	if err := validateQueue(q); err != nil {
		return queueState{}, err
	}
	t.queueMu.Lock()
	defer t.queueMu.Unlock()
	definition := describeQueue(q)
	if state, ok := t.queues[q.Name()]; ok {
		if state.definition != definition {
			return queueState{}, fmt.Errorf("queue %q has a conflicting definition", q.Name())
		}
		return state, nil
	}
	ctx, cancel := context.WithTimeout(ctx, t.requestTimeout)
	defer cancel()
	name, subject := t.queueNames(q)
	encoded, err := json.Marshal(definition)
	if err != nil {
		return queueState{}, err
	}
	storage := natsjs.FileStorage
	if !q.Durable() {
		storage = natsjs.MemoryStorage
	}
	stream, err := t.pubJS.CreateStream(ctx, natsjs.StreamConfig{
		Name: name, Description: "Hatchet durable queue: " + q.Name(), Subjects: []string{subject},
		Retention: natsjs.WorkQueuePolicy, Storage: storage, Replicas: t.replicas,
		Discard: natsjs.DiscardNew, MaxBytes: t.maxBytes, MaxMsgSize: msgqueue.MaxMessageSize,
		Metadata: map[string]string{queueMetadataKey: string(encoded)},
	})
	if err != nil {
		return queueState{}, fmt.Errorf("declare stream: %w", err)
	}
	var consumer natsjs.Consumer
	if !q.Exclusive() {
		consumer, err = t.subJS.CreateConsumer(ctx, name, natsjs.ConsumerConfig{
			Durable: consumerName, FilterSubject: subject, AckPolicy: natsjs.AckExplicitPolicy,
			AckWait: t.ackWait, MaxDeliver: -1, MaxAckPending: -1,
		})
	}
	if err != nil {
		return queueState{}, fmt.Errorf("declare consumer: %w", err)
	}
	state := queueState{consumer: consumer, stream: stream, subject: subject, definition: definition}
	t.queues[q.Name()] = state
	return state, nil
}

func (t *MessageQueueImpl) SendMessage(ctx context.Context, q msgqueue.Queue, msg *msgqueue.Message) error {
	ctx, span := telemetry.NewSpan(ctx, "MessageQueueImpl.SendMessage")
	defer span.End()
	if err := t.ctx.Err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateQueue(q); err != nil {
		return err
	}
	if msg == nil {
		return errors.New("jetstream requires a message")
	}
	msg = msg.Clone()
	msg.SetOtelCarrier(telemetry.GetCarrier(ctx))
	if q.IsExpirable() || q.AutoDeleted() {
		gate, err := t.acquireGate(ctx, q)
		if err != nil {
			return err
		}
		defer gate.release()
		ctx = gate.ctx
		t.invalidateQueue(q.Name())
	}
	// Retry headers and the stable forwarding identity share the broker's wire limit.
	bodies, err := t.messageBodies(msg, int(t.pubConn.MaxPayload())-1024)
	if err != nil {
		return err
	}
	state, err := t.initQueue(ctx, q)
	if err != nil {
		return err
	}
	for _, body := range bodies {
		pubCtx, cancel := context.WithTimeout(ctx, t.requestTimeout)
		var err error
		if msg.ImmediatelyExpire {
			err = t.pubConn.Publish(state.subject+".live", body)
			if err == nil {
				err = t.pubConn.FlushWithContext(pubCtx)
			}
		} else {
			_, err = t.pubJS.Publish(pubCtx, state.subject, body)
		}
		cancel()
		if err != nil {
			if errors.Is(err, natsjs.ErrNoStreamResponse) || errors.Is(err, natsjs.ErrStreamNotFound) {
				t.invalidateQueue(q.Name())
			}
			return fmt.Errorf("publish to queue %q: %w", q.Name(), err)
		}
	}
	return nil
}

// messageBodies validates every chunk before the first publication. A broker
// failure during a multi-chunk send can still require a retry of the whole send.
func (t *MessageQueueImpl) messageBodies(msg *msgqueue.Message, maxPayload int) ([][]byte, error) {
	copyMsg := msg.Clone()
	if !copyMsg.Compressed {
		compressed, err := t.compressor.CompressPayloads(copyMsg.Payloads)
		if err != nil {
			return nil, err
		}
		copyMsg.Payloads, copyMsg.Compressed = compressed.Payloads, compressed.WasCompressed
	}
	limit := min(maxPayload, msgqueue.MaxMessageSize)
	var bodies [][]byte
	var split func(*msgqueue.Message) error
	split = func(chunk *msgqueue.Message) error {
		body, err := json.Marshal(chunk)
		if err != nil {
			return err
		}
		if len(body) <= limit {
			bodies = append(bodies, body)
			return nil
		}
		if len(chunk.Payloads) <= 1 {
			return fmt.Errorf("single message size %d exceeds maximum %d", len(body), limit)
		}
		mid := len(chunk.Payloads) / 2
		left, right := chunk.Clone(), chunk.Clone()
		left.Payloads, right.Payloads = chunk.Payloads[:mid], chunk.Payloads[mid:]
		if err := split(left); err != nil {
			return err
		}
		return split(right)
	}
	if err := split(copyMsg); err != nil {
		return nil, err
	}
	return bodies, nil
}

type subscription struct {
	cancel       context.CancelFunc
	done         chan struct{}
	once         sync.Once
	live         *natsgo.Subscription
	liveMu       sync.Mutex
	stopped      bool
	liveHandlers sync.WaitGroup
	handlerSlots chan struct{}
}

func (s *subscription) stop() {
	s.once.Do(func() {
		s.liveMu.Lock()
		s.stopped = true
		s.liveMu.Unlock()
		s.cancel()
		if s.live != nil {
			_ = s.live.Unsubscribe()
		}
	})
}

func (t *MessageQueueImpl) Subscribe(q msgqueue.Queue, preAck, postAck msgqueue.MsgHandler) (func() error, error) {
	cleanup, err := t.subscribe(q, q, preAck, postAck)
	if err != nil {
		return nil, err
	}
	if dlq := q.DLQ(); dlq != nil && dlq.IsAutoDLQ() {
		cleanupDLQ, err := t.subscribe(dlq, q, preAck, postAck)
		if err != nil {
			_ = cleanup()
			return nil, err
		}
		return func() error { return errors.Join(cleanup(), cleanupDLQ()) }, nil
	}
	return cleanup, nil
}

func (t *MessageQueueImpl) subscribe(q, root msgqueue.Queue, preAck, postAck msgqueue.MsgHandler) (func() error, error) {
	ctx, cancel := context.WithCancel(t.ctx)
	t.lifecycleMu.Lock()
	if t.closed {
		t.lifecycleMu.Unlock()
		cancel()
		return nil, errors.New("jetstream backend is closed")
	}
	t.supervisors.Add(1)
	t.lifecycleMu.Unlock()
	first, err := t.startSubscription(ctx, q, root, preAck, postAck)
	if err != nil {
		cancel()
		t.supervisors.Done()
		return nil, err
	}
	done := make(chan struct{})
	go func() {
		defer t.supervisors.Done()
		defer close(done)
		current := first
		for {
			if current != nil {
				select {
				case <-ctx.Done():
					current.stop()
					<-current.done
					return
				case <-current.done:
					current = nil
				}
			}
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(t.sweepInterval):
			}
			var err error
			current, err = t.startSubscription(ctx, q, root, preAck, postAck)
			if err != nil {
				t.l.Warn().Err(err).Str("queue", q.Name()).Msg("retrying JetStream subscription")
			}
		}
	}()
	return func() error { cancel(); <-done; return nil }, nil
}

func (t *MessageQueueImpl) startSubscription(parent context.Context, q, root msgqueue.Queue, preAck, postAck msgqueue.MsgHandler) (*subscription, error) {
	if preAck == nil || postAck == nil {
		return nil, errors.New("jetstream requires both preAck and postAck hooks")
	}
	gate, err := t.acquireGate(parent, q)
	if err != nil {
		return nil, err
	}
	defer gate.release()
	t.invalidateQueue(q.Name())
	state, err := t.initQueue(gate.ctx, q)
	if err != nil {
		return nil, err
	}
	lease, err := t.acquireSubscription(gate.ctx, state)
	if err != nil {
		return nil, err
	}
	if err := t.markSubscribed(gate.ctx, state); err != nil {
		lease.release()
		return nil, err
	}
	if q.Exclusive() {
		state.consumer, err = t.exclusiveConsumer(gate.ctx, state, lease)
		if err != nil {
			lease.release()
			return nil, err
		}
	}
	qos := int(t.qos.Load())
	ctx, cancel := context.WithCancel(parent)
	sub := &subscription{cancel: cancel, done: make(chan struct{}), handlerSlots: make(chan struct{}, qos)}
	live, err := t.subConn.QueueSubscribe(state.subject+".live", state.subject, func(raw *natsgo.Msg) {
		sub.liveMu.Lock()
		if sub.stopped {
			sub.liveMu.Unlock()
			return
		}
		sub.liveHandlers.Add(1)
		sub.liveMu.Unlock()
		defer sub.liveHandlers.Done()
		select {
		case sub.handlerSlots <- struct{}{}:
			defer func() { <-sub.handlerSlots }()
		case <-ctx.Done():
			return
		}
		t.handleLive(ctx, q, root, raw, preAck, postAck)
	})
	if err != nil {
		sub.stop()
		lease.release()
		return nil, err
	}
	sub.live = live
	flushCtx, flushCancel := context.WithTimeout(ctx, t.requestTimeout)
	flushErr := t.subConn.FlushWithContext(flushCtx)
	flushCancel()
	if flushErr != nil {
		sub.stop()
		lease.release()
		return nil, flushErr
	}
	t.lifecycleMu.Lock()
	if t.closed {
		t.lifecycleMu.Unlock()
		sub.stop()
		lease.release()
		return nil, errors.New("jetstream backend is closed")
	}
	t.subs[sub] = struct{}{}
	t.lifecycleMu.Unlock()
	lease.renew(ctx, sub.stop)
	go func() {
		defer close(sub.done)
		defer func() {
			t.lifecycleMu.Lock()
			delete(t.subs, sub)
			t.lifecycleMu.Unlock()
		}()
		var handlers sync.WaitGroup
		defer func() { sub.stop(); handlers.Wait(); sub.liveHandlers.Wait(); lease.release() }()
		slots := make(chan struct{}, qos)
		for {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			reserved := 1
		reserve:
			for reserved < qos {
				select {
				case slots <- struct{}{}:
					reserved++
				default:
					break reserve
				}
			}
			batch, err := state.consumer.Fetch(reserved, natsjs.FetchMaxWait(250*time.Millisecond))
			if err != nil {
				for range reserved {
					<-slots
				}
				if ctx.Err() != nil {
					return
				}
				t.l.Warn().Err(err).Msg("JetStream pull failed")
				return
			}
			received := 0
			for delivery := range batch.Messages() {
				received++
				if ctx.Err() != nil {
					<-slots
					continue
				}
				select {
				case sub.handlerSlots <- struct{}{}:
				case <-ctx.Done():
					<-slots
					continue
				}
				handlers.Add(1)
				go func() {
					defer handlers.Done()
					defer func() { <-slots }()
					defer func() { <-sub.handlerSlots }()
					t.handleMessage(ctx, delivery, q, root, lease.key, preAck, postAck)
				}()
			}
			for i := received; i < reserved; i++ {
				<-slots
			}
			if err := batch.Error(); err != nil && !errors.Is(err, natsgo.ErrTimeout) && !errors.Is(err, natsjs.ErrNoMessages) {
				if ctx.Err() != nil {
					return
				}
				t.l.Warn().Err(err).Msg("JetStream pull interrupted")
				return
			}
		}
	}()
	return sub, nil
}

func (t *MessageQueueImpl) handleMessage(ctx context.Context, delivery natsjs.Msg, q, root msgqueue.Queue, ownerKey string, preAck, postAck msgqueue.MsgHandler) {
	metadata, err := delivery.Metadata()
	if err != nil {
		t.l.Error().Err(err).Msg("could not read JetStream delivery metadata")
		return
	}
	if ctx.Err() != nil {
		return
	}
	if readyAfter(delivery.Headers(), metadata.Timestamp).After(time.Now()) {
		_ = delivery.NakWithDelay(time.Until(readyAfter(delivery.Headers(), metadata.Timestamp)))
		return
	}
	if q.IsExpirable() {
		claim, err := t.claimMessage(ctx, stateKey(t, q, metadata.Sequence.Stream, metadata.Timestamp), ownerKey, false)
		if err != nil {
			_ = delivery.NakWithDelay(t.sweepInterval)
			return
		}
		defer claim.release()
		if time.Since(metadata.Timestamp) >= t.dispatcherTTL {
			if err := t.forward(ctx, q, root.DLQ(), delivery.Data(), delivery.Headers(), messageSource(metadata.Stream, metadata.Timestamp), metadata.Sequence.Stream, false); err == nil {
				t.ack(ctx, delivery)
			}
			return
		}
	}
	if t.enableMessageRejection && deathCount(delivery.Headers()) > t.maxDeathCount {
		t.l.Error().Uint64("death_count", deathCount(delivery.Headers())).Msg("dropping message after maximum rejected deliveries")
		t.ack(ctx, delivery)
		return
	}
	msg := &msgqueue.Message{}
	if err := json.Unmarshal(delivery.Data(), msg); err != nil {
		t.l.Error().Err(err).Msg("dropping malformed JetStream message")
		if err := t.retryMessage(ctx, q, root, delivery, metadata); err == nil {
			t.ack(ctx, delivery)
		}
		return
	}
	if msg.Compressed {
		payloads, err := msgqueue.DecompressPayloads(msg.Payloads)
		if err != nil {
			t.l.Error().Err(err).Msg("dropping invalid compressed JetStream message")
			if err := t.retryMessage(ctx, q, root, delivery, metadata); err == nil {
				t.ack(ctx, delivery)
			}
			return
		}
		msg.Payloads, msg.Compressed = payloads, false
	}
	ctx, span := telemetry.NewSpanWithCarrier(ctx, "handle_message", msg.OtelCarrier)
	defer span.End()
	if err := preAck(msg); err != nil {
		if isPermanentPreAckError(err) {
			t.l.Error().Err(err).Msg("dropping message due to permanent pre-ack error")
			t.ack(ctx, delivery)
			return
		}
		if err := t.retryMessage(ctx, q, root, delivery, metadata); err == nil {
			t.ack(ctx, delivery)
		} else {
			_ = delivery.NakWithDelay(t.deadLetterBackoff)
		}
		return
	}
	if !t.ack(ctx, delivery) {
		return
	}
	if err := postAck(msg); err != nil {
		t.l.Error().Err(err).Msg("error in post-ack hook")
	}
}

func (t *MessageQueueImpl) ack(ctx context.Context, delivery natsjs.Msg) bool {
	ctx, cancel := context.WithTimeout(ctx, t.requestTimeout)
	defer cancel()
	if err := delivery.DoubleAck(ctx); err != nil {
		t.l.Error().Err(err).Msg("could not confirm JetStream acknowledgment")
		return false
	}
	return true
}

// Invalid JSON is a permanent pre-ack failure on the RabbitMQ backend as well.
func isPermanentPreAckError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.InvalidTextRepresentation {
		return true
	}
	return err != nil && (strings.Contains(err.Error(), "SQLSTATE "+pgerrcode.InvalidTextRepresentation) ||
		strings.Contains(err.Error(), "invalid input syntax for type json"))
}
