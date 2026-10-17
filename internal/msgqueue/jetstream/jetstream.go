package jetstream

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	natsgo "github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
	"github.com/hatchet-dev/hatchet/pkg/logger"
)

const (
	consumerTimeout = 5 * time.Minute
	consumerName    = "workers"
	defaultPrefix   = "HATCHET_MQ"
)

// ErrUnsupportedQueue identifies queue lifecycles that require dispatcher
// expiry/rerouting or exclusive ownership, which this stage does not implement.
var ErrUnsupportedQueue = errors.New("unsupported JetStream queue lifecycle")

var streamPrefixPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

type queueState struct {
	consumer natsjs.Consumer
	subject  string
}

// MessageQueueImpl implements the durable MessageQueue interface using JetStream.
// Each supported queue has a file-backed WorkQueue stream and a shared consumer.
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

	queueMu sync.Mutex
	queues  map[string]queueState

	lifecycleMu sync.Mutex
	closed      bool
	subs        map[*subscription]struct{}
	closeOnce   sync.Once
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
}

func defaultMessageQueueImplOpts() *MessageQueueImplOpts {
	l := logger.NewDefaultLogger("jetstream")
	return &MessageQueueImplOpts{
		l: &l, qos: 100, streamPrefix: defaultPrefix, replicas: 3,
		maxBytes: 1 << 30, ackWait: consumerTimeout, requestTimeout: 5 * time.Second,
		deadLetterBackoff: 5 * time.Second, maxDeathCount: 5,
		compressionThreshold: msgqueue.DefaultCompressionThreshold,
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

func WithGzipCompression(enabled bool, threshold int) MessageQueueImplOpt {
	return func(opts *MessageQueueImplOpts) {
		opts.compressionEnabled = enabled
		if threshold <= 0 {
			threshold = msgqueue.DefaultCompressionThreshold
		}
		opts.compressionThreshold = threshold
	}
}

// WithMessageRejection bounds transport delivery attempts when enabled.
// Unlike RabbitMQ's x-death counter, JetStream counts crash redeliveries too.
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
		connectOpts = append(connectOpts, natsgo.TLSHandshakeFirst())
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
	for _, q := range []msgqueue.Queue{msgqueue.TASK_PROCESSING_QUEUE, msgqueue.OLAP_QUEUE, msgqueue.DISPATCHER_DEAD_LETTER_QUEUE} {
		if _, err := t.initQueue(ctx, q); err != nil {
			_ = t.close()
			return nil, nil, fmt.Errorf("initialize JetStream queue %q: %w", q.Name(), err)
		}
	}
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
	if !q.Durable() || q.AutoDeleted() || q.Exclusive() || q.IsExpirable() {
		return fmt.Errorf("%w: queue %q requires non-durable, auto-delete, exclusive or expiring semantics", ErrUnsupportedQueue, q.Name())
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
	if state, ok := t.queues[q.Name()]; ok {
		return state, nil
	}
	ctx, cancel := context.WithTimeout(ctx, t.requestTimeout)
	defer cancel()
	name, subject := t.queueNames(q)
	_, err := t.pubJS.CreateStream(ctx, natsjs.StreamConfig{
		Name: name, Description: "Hatchet durable queue: " + q.Name(), Subjects: []string{subject},
		Retention: natsjs.WorkQueuePolicy, Storage: natsjs.FileStorage, Replicas: t.replicas,
		Discard: natsjs.DiscardNew, MaxBytes: t.maxBytes, MaxMsgSize: msgqueue.MaxMessageSize,
	})
	if err != nil {
		return queueState{}, fmt.Errorf("declare stream: %w", err)
	}
	consumer, err := t.subJS.CreateConsumer(ctx, name, natsjs.ConsumerConfig{
		Durable: consumerName, FilterSubject: subject, AckPolicy: natsjs.AckExplicitPolicy,
		AckWait: t.ackWait, MaxDeliver: -1, MaxAckPending: -1,
	})
	if err != nil {
		return queueState{}, fmt.Errorf("declare consumer: %w", err)
	}
	state := queueState{consumer: consumer, subject: subject}
	t.queues[q.Name()] = state
	return state, nil
}

func (t *MessageQueueImpl) SendMessage(ctx context.Context, q msgqueue.Queue, msg *msgqueue.Message) error {
	if err := t.ctx.Err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateQueue(q); err != nil {
		return err
	}
	if msg == nil || msg.ImmediatelyExpire {
		return errors.New("jetstream requires a message without immediately-expire semantics")
	}
	bodies, err := t.messageBodies(msg, int(t.pubConn.MaxPayload()))
	if err != nil {
		return err
	}
	state, err := t.initQueue(ctx, q)
	if err != nil {
		return err
	}
	for _, body := range bodies {
		pubCtx, cancel := context.WithTimeout(ctx, t.requestTimeout)
		_, err := t.pubJS.Publish(pubCtx, state.subject, body)
		cancel()
		if err != nil {
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
	cancel context.CancelFunc
	iter   natsjs.MessagesContext
	done   chan struct{}
	once   sync.Once
}

func (s *subscription) stop() {
	s.once.Do(func() {
		s.cancel()
		s.iter.Stop()
	})
}

func (t *MessageQueueImpl) Subscribe(q msgqueue.Queue, preAck, postAck msgqueue.MsgHandler) (func() error, error) {
	if preAck == nil || postAck == nil {
		return nil, errors.New("jetstream requires both preAck and postAck hooks")
	}
	state, err := t.initQueue(t.ctx, q)
	if err != nil {
		return nil, err
	}
	qos := int(t.qos.Load())
	iter, err := state.consumer.Messages(natsjs.PullMaxMessages(qos))
	if err != nil {
		return nil, fmt.Errorf("subscribe to queue %q: %w", q.Name(), err)
	}
	ctx, cancel := context.WithCancel(t.ctx)
	sub := &subscription{cancel: cancel, iter: iter, done: make(chan struct{})}
	t.lifecycleMu.Lock()
	if t.closed {
		t.lifecycleMu.Unlock()
		sub.stop()
		return nil, errors.New("jetstream backend is closed")
	}
	t.subs[sub] = struct{}{}
	t.lifecycleMu.Unlock()
	go func() {
		defer close(sub.done)
		defer func() {
			t.lifecycleMu.Lock()
			delete(t.subs, sub)
			t.lifecycleMu.Unlock()
		}()
		defer sub.stop()
		var handlers sync.WaitGroup
		defer handlers.Wait()
		slots := make(chan struct{}, qos)
		for {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			delivery, err := iter.Next()
			if err != nil {
				<-slots
				if ctx.Err() != nil || errors.Is(err, natsjs.ErrMsgIteratorClosed) {
					return
				}
				t.l.Error().Err(err).Str("queue", q.Name()).Msg("error receiving JetStream message")
				continue
			}
			handlers.Add(1)
			go func() {
				defer handlers.Done()
				defer func() { <-slots }()
				t.handleMessage(ctx, delivery, preAck, postAck)
			}()
		}
	}()
	return func() error {
		sub.stop()
		<-sub.done
		return nil
	}, nil
}

func (t *MessageQueueImpl) handleMessage(ctx context.Context, delivery natsjs.Msg, preAck, postAck msgqueue.MsgHandler) {
	metadata, err := delivery.Metadata()
	if err != nil {
		t.l.Error().Err(err).Msg("could not read JetStream delivery metadata")
		return
	}
	msg := &msgqueue.Message{}
	if err := json.Unmarshal(delivery.Data(), msg); err != nil {
		t.l.Error().Err(err).Msg("dropping malformed JetStream message")
		t.ack(ctx, delivery)
		return
	}
	if msg.Compressed {
		payloads, err := msgqueue.DecompressPayloads(msg.Payloads)
		if err != nil {
			t.l.Error().Err(err).Msg("dropping invalid compressed JetStream message")
			t.ack(ctx, delivery)
			return
		}
		msg.Payloads, msg.Compressed = payloads, false
	}
	if t.enableMessageRejection && metadata.NumDelivered > t.maxDeathCount+1 {
		t.l.Error().Uint64("delivery_count", metadata.NumDelivered).Msg("dropping message after maximum transport retries")
		t.ack(ctx, delivery)
		return
	}
	if err := preAck(msg); err != nil {
		if isPermanentPreAckError(err) {
			t.l.Error().Err(err).Msg("dropping message due to permanent pre-ack error")
			t.ack(ctx, delivery)
			return
		}
		if err := delivery.NakWithDelay(t.deadLetterBackoff); err != nil {
			t.l.Error().Err(err).Msg("could not delay JetStream redelivery")
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
