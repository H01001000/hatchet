//go:build integration

package jetstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	natsgo "github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
)

type integrationFixture struct {
	q      *MessageQueueImpl
	close  func() error
	opts   []MessageQueueImplOpt
	admin  natsjs.JetStream
	prefix string
}

func testOptions(url, prefix string) []MessageQueueImplOpt {
	l := zerolog.New(io.Discard)
	return []MessageQueueImplOpt{
		WithURL(url), WithLogger(&l), WithStreamPrefix(prefix), WithReplicas(1),
		WithQos(4), WithAckWait(500 * time.Millisecond), WithRequestTimeout(10 * time.Second),
		WithDeadLetterBackoff(50 * time.Millisecond),
	}
}

func newTestMessageQueue(t *testing.T, fs ...MessageQueueImplOpt) *integrationFixture {
	t.Helper()
	url := os.Getenv("HATCHET_TEST_JETSTREAM_URL")
	if url == "" {
		url = "nats://127.0.0.1:4222"
	}
	prefix := "TEST_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	opts := append(testOptions(url, prefix), WithMaxBytes(64<<20))
	opts = append(opts, fs...)
	closeBackend, q, err := New(opts...)
	require.NoError(t, err)
	nc, err := natsgo.Connect(url)
	if err != nil {
		_ = closeBackend()
		t.Fatal(err)
	}
	admin, err := natsjs.New(nc)
	require.NoError(t, err)
	f := &integrationFixture{q: q, close: closeBackend, opts: opts, admin: admin, prefix: prefix}
	t.Cleanup(func() {
		require.NoError(t, closeBackend())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		lister := admin.StreamNames(ctx)
		for name := range lister.Name() {
			if strings.HasPrefix(name, prefix+"_") {
				require.NoError(t, admin.DeleteStream(ctx, name))
			}
		}
		require.NoError(t, lister.Err())
		nc.Close()
	})
	return f
}

func fixtureMessage(t *testing.T, index int) *msgqueue.Message {
	t.Helper()
	msg, err := msgqueue.NewTenantMessage(uuid.MustParse("00000000-0000-0000-0000-000000000001"), "same-message-kind", false, true, index)
	require.NoError(t, err)
	return msg
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for delivery")
		var zero T
		return zero
	}
}

func subscribe(t *testing.T, q *MessageQueueImpl, queue msgqueue.Queue, pre, post msgqueue.MsgHandler) {
	t.Helper()
	stop, err := q.Subscribe(queue, pre, post)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stop()) })
}

func waitDrained(t *testing.T, f *integrationFixture, queue msgqueue.Queue) {
	t.Helper()
	name, _ := f.q.queueNames(queue)
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		stream, err := f.admin.Stream(ctx, name)
		if err != nil {
			return false
		}
		info, err := stream.Info(ctx)
		return err == nil && info.State.Msgs == 0
	}, 5*time.Second, 10*time.Millisecond)
}

func TestMessageQueuePersistenceAndCompetingConsumers(t *testing.T) {
	f := newTestMessageQueue(t)
	queue := msgqueue.NewRandomStaticQueue()
	for i := range 24 {
		require.NoError(t, f.q.SendMessage(t.Context(), queue, fixtureMessage(t, i)))
	}
	// Recreate both backend connections after publication, before any worker exists.
	require.NoError(t, f.close())
	closeA, a, err := New(f.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeA()) })
	closeB, b, err := a.Clone()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeB()) })
	received := make(chan int, 48)
	handler := func(msg *msgqueue.Message) error {
		var index int
		if err := json.Unmarshal(msg.Payloads[0], &index); err != nil {
			return err
		}
		received <- index
		return nil
	}
	subscribe(t, a, queue, handler, msgqueue.NoOpHook)
	subscribe(t, b.(*MessageQueueImpl), queue, handler, msgqueue.NoOpHook)
	seen := make(map[int]int)
	for range 24 {
		seen[receive(t, received)]++
	}
	require.Len(t, seen, 24)
	for i := range 24 {
		require.Equal(t, 1, seen[i])
	}
	waitDrained(t, f, queue)
	select {
	case extra := <-received:
		t.Fatalf("unexpected extra delivery %d", extra)
	case <-time.After(150 * time.Millisecond):
	}
}

func TestMessageQueuePreAckRetriesThenPostAckDoesNotRetry(t *testing.T) {
	f := newTestMessageQueue(t, WithDeadLetterBackoff(150*time.Millisecond))
	queue := msgqueue.NewRandomStaticQueue()
	var calls atomic.Int32
	attempts := make(chan time.Time, 4)
	postCalls := make(chan struct{}, 4)
	subscribe(t, f.q, queue, func(*msgqueue.Message) error {
		attempts <- time.Now()
		if calls.Add(1) == 1 {
			return errors.New("transient pre-ack failure")
		}
		return nil
	}, func(*msgqueue.Message) error {
		postCalls <- struct{}{}
		return errors.New("post-ack failure")
	})
	require.NoError(t, f.q.SendMessage(t.Context(), queue, fixtureMessage(t, 1)))
	first, second := receive(t, attempts), receive(t, attempts)
	require.GreaterOrEqual(t, second.Sub(first), 125*time.Millisecond)
	receive(t, postCalls)
	waitDrained(t, f, queue)
	select {
	case <-attempts:
		t.Fatal("post-ack failure caused redelivery")
	case <-time.After(750 * time.Millisecond):
	}
	require.EqualValues(t, 2, calls.Load())
}

func TestMessageQueuePermanentPreAckErrorDoesNotRetry(t *testing.T) {
	f := newTestMessageQueue(t)
	queue := msgqueue.NewRandomStaticQueue()
	var calls, postCalls atomic.Int32
	subscribe(t, f.q, queue, func(*msgqueue.Message) error {
		calls.Add(1)
		return fmt.Errorf("invalid payload: %w", &pgconn.PgError{Code: pgerrcode.InvalidTextRepresentation})
	}, func(*msgqueue.Message) error {
		postCalls.Add(1)
		return nil
	})
	require.NoError(t, f.q.SendMessage(t.Context(), queue, fixtureMessage(t, 1)))
	waitDrained(t, f, queue)
	require.EqualValues(t, 1, calls.Load())
	require.Zero(t, postCalls.Load())
}

func TestMessageQueueRetryLimit(t *testing.T) {
	f := newTestMessageQueue(t, WithMessageRejection(true, 1))
	queue := msgqueue.NewRandomStaticQueue()
	var calls atomic.Int32
	subscribe(t, f.q, queue, func(*msgqueue.Message) error {
		calls.Add(1)
		return errors.New("retryable failure")
	}, msgqueue.NoOpHook)
	require.NoError(t, f.q.SendMessage(t.Context(), queue, fixtureMessage(t, 1)))
	waitDrained(t, f, queue)
	require.EqualValues(t, 2, calls.Load())
}

func TestMessageQueueTransportDisconnectRedeliversUnackedMessage(t *testing.T) {
	f := newTestMessageQueue(t)
	queue := msgqueue.NewRandomStaticQueue()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	stop, err := f.q.Subscribe(queue, func(*msgqueue.Message) error {
		entered <- struct{}{}
		<-release
		return nil
	}, msgqueue.NoOpHook)
	require.NoError(t, err)
	require.NoError(t, f.q.SendMessage(t.Context(), queue, fixtureMessage(t, 3)))
	receive(t, entered)
	f.q.subConn.Close()
	unblock()
	require.NoError(t, stop())
	require.NoError(t, f.close())
	closeAgain, again, err := New(f.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeAgain()) })
	received := make(chan struct{}, 2)
	subscribe(t, again, queue, msgqueue.NoOpHook, func(*msgqueue.Message) error {
		received <- struct{}{}
		return nil
	})
	receive(t, received)
	waitDrained(t, f, queue)
}

func TestMessageQueueQOSBoundsActiveHandlers(t *testing.T) {
	f := newTestMessageQueue(t, WithQos(2), WithAckWait(10*time.Second))
	queue := msgqueue.NewRandomStaticQueue()
	entered := make(chan struct{}, 12)
	finished := make(chan struct{}, 12)
	release := make(chan struct{})
	defer close(release)
	var active, peak atomic.Int32
	subscribe(t, f.q, queue, func(*msgqueue.Message) error {
		n := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); n > previous; previous = peak.Load() {
			if peak.CompareAndSwap(previous, n) {
				break
			}
		}
		entered <- struct{}{}
		<-release
		return nil
	}, func(*msgqueue.Message) error { finished <- struct{}{}; return nil })
	for i := range 8 {
		require.NoError(t, f.q.SendMessage(t.Context(), queue, fixtureMessage(t, i)))
	}
	receive(t, entered)
	receive(t, entered)
	select {
	case <-entered:
		t.Fatal("more than two handlers started while both slots were occupied")
	case <-time.After(150 * time.Millisecond):
	}
	state, err := f.q.initQueue(t.Context(), queue)
	require.NoError(t, err)
	info, err := state.consumer.Info(t.Context())
	require.NoError(t, err)
	require.LessOrEqual(t, info.NumAckPending, 2, "prefetched deliveries must count against QoS too")
	// Release each handler while keeping the channel open for the deferred cleanup.
	for range 8 {
		release <- struct{}{}
	}
	for range 8 {
		receive(t, finished)
	}
	require.EqualValues(t, 2, peak.Load())
	waitDrained(t, f, queue)
}

func TestMessageQueueCompressionAndChunking(t *testing.T) {
	f := newTestMessageQueue(t, WithGzipCompression(true, 1), WithAckWait(10*time.Second))
	queue := msgqueue.NewRandomStaticQueue()
	msg := &msgqueue.Message{ID: "compressed", Persistent: true, Payloads: [][]byte{bytes.Repeat([]byte("payload"), 1000)}}
	received := make(chan *msgqueue.Message, 2)
	subscribe(t, f.q, queue, func(msg *msgqueue.Message) error { received <- msg; return nil }, msgqueue.NoOpHook)
	require.NoError(t, f.q.SendMessage(t.Context(), queue, msg))
	got := receive(t, received)
	require.False(t, got.Compressed)
	require.Equal(t, msg.Payloads, got.Payloads)
	require.False(t, msg.Compressed)
	waitDrained(t, f, queue)

	// A second queue checks the real wire size limit without gzip reducing it.
	closePlain, plain, err := New(append(f.opts, WithGzipCompression(false, 1))...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closePlain()) })
	chunkQueue := msgqueue.NewRandomStaticQueue()
	chunks := make(chan *msgqueue.Message, 4)
	subscribe(t, plain, chunkQueue, func(msg *msgqueue.Message) error { chunks <- msg; return nil }, msgqueue.NoOpHook)
	large := &msgqueue.Message{ID: "chunked", Persistent: true, Payloads: [][]byte{
		bytes.Repeat([]byte("a"), 7<<20), bytes.Repeat([]byte("b"), 7<<20),
	}}
	require.NoError(t, plain.SendMessage(t.Context(), chunkQueue, large))
	first, second := receive(t, chunks), receive(t, chunks)
	require.Len(t, first.Payloads, 1)
	require.Len(t, second.Payloads, 1)
	seen := map[byte]bool{first.Payloads[0][0]: true, second.Payloads[0][0]: true}
	require.True(t, seen['a'] && seen['b'])
	waitDrained(t, f, chunkQueue)
}

func TestMessageQueueRejectsFullStreamWithoutEvictingBacklog(t *testing.T) {
	f := newTestMessageQueue(t, WithMaxBytes(msgqueue.MaxMessageSize))
	queue := msgqueue.NewRandomStaticQueue()
	msg := &msgqueue.Message{ID: "large", Persistent: true, Payloads: [][]byte{bytes.Repeat([]byte("x"), 7<<20)}}
	require.NoError(t, f.q.SendMessage(t.Context(), queue, msg))
	require.Error(t, f.q.SendMessage(t.Context(), queue, msg))
	name, _ := f.q.queueNames(queue)
	stream, err := f.admin.Stream(t.Context(), name)
	require.NoError(t, err)
	info, err := stream.Info(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, info.State.Msgs)
}

func TestMessageQueueMalformedEnvelopeAndUnsupportedSemantics(t *testing.T) {
	f := newTestMessageQueue(t)
	queue := msgqueue.NewRandomStaticQueue()
	state, err := f.q.initQueue(t.Context(), queue)
	require.NoError(t, err)
	var calls atomic.Int32
	subscribe(t, f.q, queue, func(*msgqueue.Message) error { calls.Add(1); return nil }, msgqueue.NoOpHook)
	_, err = f.admin.Publish(t.Context(), state.subject, []byte("not json"))
	require.NoError(t, err)
	waitDrained(t, f, queue)
	require.Zero(t, calls.Load())

	dispatcher := msgqueue.QueueTypeFromDispatcherID(uuid.New())
	require.ErrorIs(t, f.q.SendMessage(t.Context(), dispatcher, fixtureMessage(t, 1)), ErrUnsupportedQueue)
	_, err = f.q.Subscribe(dispatcher, msgqueue.NoOpHook, msgqueue.NoOpHook)
	require.ErrorIs(t, err, ErrUnsupportedQueue)
	msg := fixtureMessage(t, 2)
	msg.ImmediatelyExpire = true
	require.ErrorContains(t, f.q.SendMessage(t.Context(), queue, msg), "immediately-expire")
	_, err = f.q.Subscribe(queue, nil, msgqueue.NoOpHook)
	require.Error(t, err)
}

func TestMessageQueueDoesNotRewriteExistingStreamConfiguration(t *testing.T) {
	f := newTestMessageQueue(t)
	closeBackend, _, err := New(append(f.opts, WithMaxBytes(32<<20))...)
	require.Error(t, err)
	require.Nil(t, closeBackend)
	name, _ := f.q.queueNames(msgqueue.TASK_PROCESSING_QUEUE)
	stream, err := f.admin.Stream(t.Context(), name)
	require.NoError(t, err)
	info, err := stream.Info(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 64<<20, info.Config.MaxBytes)
}

func TestMessageQueueShutdownUnblocksIdleSubscriptions(t *testing.T) {
	f := newTestMessageQueue(t)
	require.True(t, f.q.IsReady())
	queue := msgqueue.NewRandomStaticQueue()
	stop, err := f.q.Subscribe(queue, msgqueue.NoOpHook, msgqueue.NoOpHook)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- f.close() }()
	require.NoError(t, receive(t, done))
	require.False(t, f.q.IsReady())
	require.NoError(t, stop())
	require.NoError(t, f.close())
	require.ErrorIs(t, f.q.SendMessage(t.Context(), queue, fixtureMessage(t, 1)), context.Canceled)
}

// This test restarts only the explicitly selected, dedicated test container.
func TestMessageQueueBrokerRestart(t *testing.T) {
	container := os.Getenv("HATCHET_TEST_JETSTREAM_CONTAINER")
	if container == "" {
		t.Skip("set HATCHET_TEST_JETSTREAM_CONTAINER to exercise a dedicated broker restart")
	}
	f := newTestMessageQueue(t)
	queue := msgqueue.NewRandomStaticQueue()
	require.NoError(t, f.q.SendMessage(t.Context(), queue, fixtureMessage(t, 9)))
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", "restart", container).CombinedOutput()
	require.NoError(t, err, "%s", output)
	require.Eventually(t, f.q.IsReady, 15*time.Second, 50*time.Millisecond)
	received := make(chan struct{}, 2)
	subscribe(t, f.q, queue, msgqueue.NoOpHook, func(*msgqueue.Message) error {
		received <- struct{}{}
		return nil
	})
	receive(t, received)
	waitDrained(t, f, queue)

	// Existing stream/consumer declarations must also remain reusable.
	closeAgain, again, err := New(f.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeAgain()) })
	require.NoError(t, again.SendMessage(t.Context(), queue, fixtureMessage(t, 10)))
	receive(t, received)
	waitDrained(t, f, queue)
}
