//go:build integration

package jetstream

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	natsjs "github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
)

func lifecycleOptions() []MessageQueueImplOpt {
	return []MessageQueueImplOpt{WithDispatcherExpiry(250*time.Millisecond, time.Second), WithLeaseTimeout(1500 * time.Millisecond), WithSweepInterval(50 * time.Millisecond)}
}

func TestDispatcherExclusiveOwnershipAndHandover(t *testing.T) {
	f := newTestMessageQueue(t, append(lifecycleOptions(), WithDispatcherExpiry(5*time.Second, 10*time.Second))...)
	queue := msgqueue.QueueTypeFromDispatcherID(uuid.New())
	stop, err := f.q.Subscribe(queue, msgqueue.NoOpHook, msgqueue.NoOpHook)
	require.NoError(t, err)
	closeOther, otherInterface, err := f.q.Clone()
	require.NoError(t, err)
	other := otherInterface.(*MessageQueueImpl)
	t.Cleanup(func() { require.NoError(t, closeOther()) })
	_, err = other.Subscribe(queue, msgqueue.NoOpHook, msgqueue.NoOpHook)
	require.ErrorIs(t, err, ErrQueueInUse)
	require.NoError(t, stop())
	received := make(chan struct{}, 1)
	subscribe(t, other, queue, msgqueue.NoOpHook, func(*msgqueue.Message) error { received <- struct{}{}; return nil })
	require.NoError(t, f.q.SendMessage(t.Context(), queue, fixtureMessage(t, 1)))
	receive(t, received)
	waitDrained(t, f, queue)
}

func TestDispatcherExpiryRoutesWithoutAnySubscriber(t *testing.T) {
	f := newTestMessageQueue(t, lifecycleOptions()...)
	queue := msgqueue.QueueTypeFromDispatcherID(uuid.New())
	received := make(chan *msgqueue.Message, 4)
	subscribe(t, f.q, msgqueue.DISPATCHER_DEAD_LETTER_QUEUE, msgqueue.NoOpHook, func(msg *msgqueue.Message) error { received <- msg; return nil })
	msg := fixtureMessage(t, 7)
	require.NoError(t, f.q.SendMessage(t.Context(), queue, msg))
	got := receive(t, received)
	require.Equal(t, msg.Payloads, got.Payloads)
	waitDrained(t, f, queue)
	select {
	case <-received:
		t.Fatal("expired assignment was routed twice")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestDispatcherInFlightAssignmentDoesNotExpire(t *testing.T) {
	f := newTestMessageQueue(t, append(lifecycleOptions(), WithAckWait(3*time.Second))...)
	queue := msgqueue.QueueTypeFromDispatcherID(uuid.New())
	entered := make(chan struct{}, 1)
	finished := make(chan struct{}, 1)
	dead := make(chan struct{}, 2)
	release := make(chan struct{})
	defer close(release)
	subscribe(t, f.q, msgqueue.DISPATCHER_DEAD_LETTER_QUEUE, msgqueue.NoOpHook, func(*msgqueue.Message) error { dead <- struct{}{}; return nil })
	subscribe(t, f.q, queue, func(*msgqueue.Message) error { entered <- struct{}{}; <-release; return nil }, func(*msgqueue.Message) error { finished <- struct{}{}; return nil })
	require.NoError(t, f.q.SendMessage(t.Context(), queue, fixtureMessage(t, 1)))
	receive(t, entered)
	select {
	case <-dead:
		t.Fatal("in-flight delivery expired")
	case <-time.After(700 * time.Millisecond):
	}
	release <- struct{}{}
	receive(t, finished)
	waitDrained(t, f, queue)
	select {
	case <-dead:
		t.Fatal("completed delivery was dead-lettered")
	default:
	}
}

func TestDispatcherLeaseLossRecoversUnackedAssignment(t *testing.T) {
	f := newTestMessageQueue(t, append(lifecycleOptions(), WithAckWait(10*time.Second))...)
	queue := msgqueue.QueueTypeFromDispatcherID(uuid.New())
	closeOther, otherInterface, err := f.q.Clone()
	require.NoError(t, err)
	other := otherInterface.(*MessageQueueImpl)
	t.Cleanup(func() { require.NoError(t, closeOther()) })
	dead := make(chan *msgqueue.Message, 2)
	subscribe(t, other, msgqueue.DISPATCHER_DEAD_LETTER_QUEUE, msgqueue.NoOpHook, func(msg *msgqueue.Message) error { dead <- msg; return nil })
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	_, err = f.q.Subscribe(queue, func(*msgqueue.Message) error { entered <- struct{}{}; <-release; return nil }, msgqueue.NoOpHook)
	require.NoError(t, err)
	msg := fixtureMessage(t, 8)
	require.NoError(t, f.q.SendMessage(t.Context(), queue, msg))
	receive(t, entered)
	// Losing the publisher connection also stops the owner from renewing its leases.
	f.q.pubConn.Close()
	got := receive(t, dead)
	require.Equal(t, msg.Payloads, got.Payloads)
	release <- struct{}{}
	waitDrained(t, f, queue)
}

func TestDispatcherRejectionRoutesImmediatelyToDedicatedDLQ(t *testing.T) {
	f := newTestMessageQueue(t, WithDispatcherExpiry(10*time.Second, time.Minute))
	queue := msgqueue.QueueTypeFromDispatcherID(uuid.New())
	dead := make(chan struct{}, 1)
	subscribe(t, f.q, msgqueue.DISPATCHER_DEAD_LETTER_QUEUE, msgqueue.NoOpHook, func(*msgqueue.Message) error { dead <- struct{}{}; return nil })
	subscribe(t, f.q, queue, func(*msgqueue.Message) error { return errors.New("dispatcher rejected assignment") }, msgqueue.NoOpHook)
	require.NoError(t, f.q.SendMessage(t.Context(), queue, fixtureMessage(t, 1)))
	receive(t, dead)
	waitDrained(t, f, queue)
}

func TestAutoDeleteWaitsForLastSubscriberAndCanBeRecreated(t *testing.T) {
	f := newTestMessageQueue(t, lifecycleOptions()...)
	queue := testQueue{name: "auto_" + uuid.NewString(), auto: true}
	stopA, err := f.q.Subscribe(queue, msgqueue.NoOpHook, msgqueue.NoOpHook)
	require.NoError(t, err)
	stopB, err := f.q.Subscribe(queue, msgqueue.NoOpHook, msgqueue.NoOpHook)
	require.NoError(t, err)
	require.NoError(t, stopA())
	name, _ := f.q.queueNames(queue)
	_, err = f.admin.Stream(t.Context(), name)
	require.NoError(t, err)
	require.NoError(t, stopB())
	require.Eventually(t, func() bool {
		_, err := f.admin.Stream(t.Context(), name)
		return errors.Is(err, natsjs.ErrStreamNotFound)
	}, 5*time.Second, 25*time.Millisecond)
	received := make(chan struct{}, 1)
	subscribe(t, f.q, queue, msgqueue.NoOpHook, func(*msgqueue.Message) error { received <- struct{}{}; return nil })
	require.NoError(t, f.q.SendMessage(t.Context(), queue, fixtureMessage(t, 1)))
	receive(t, received)
}

func TestIdleDispatcherQueueIsCleanedUp(t *testing.T) {
	f := newTestMessageQueue(t, lifecycleOptions()...)
	queue := msgqueue.QueueTypeFromDispatcherID(uuid.New())
	stop, err := f.q.Subscribe(queue, msgqueue.NoOpHook, msgqueue.NoOpHook)
	require.NoError(t, err)
	require.NoError(t, stop())
	name, _ := f.q.queueNames(queue)
	require.Eventually(t, func() bool {
		_, err := f.admin.Stream(t.Context(), name)
		return errors.Is(err, natsjs.ErrStreamNotFound)
	}, 5*time.Second, 25*time.Millisecond)
}

func TestImmediatelyExpiringMessagesAreLiveOnly(t *testing.T) {
	f := newTestMessageQueue(t)
	queue := msgqueue.NewRandomStaticQueue()
	msg := fixtureMessage(t, 1)
	msg.ImmediatelyExpire = true
	require.NoError(t, f.q.SendMessage(t.Context(), queue, msg))
	received := make(chan *msgqueue.Message, 3)
	subscribe(t, f.q, queue, msgqueue.NoOpHook, func(msg *msgqueue.Message) error { received <- msg; return nil })
	select {
	case <-received:
		t.Fatal("offline immediately-expiring message was retained")
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, f.q.SendMessage(t.Context(), queue, msg))
	got := receive(t, received)
	require.Equal(t, msg.Payloads, got.Payloads)
	waitDrained(t, f, queue)
}

func TestImmediatelyExpiringRejectionUsesRetryDLQ(t *testing.T) {
	f := newTestMessageQueue(t)
	queue := msgqueue.NewRandomStaticQueue()
	var calls atomic.Int32
	done := make(chan struct{}, 1)
	subscribe(t, f.q, queue, func(*msgqueue.Message) error {
		if calls.Add(1) == 1 {
			return errors.New("retry live delivery")
		}
		return nil
	}, func(*msgqueue.Message) error { done <- struct{}{}; return nil })
	msg := fixtureMessage(t, 1)
	msg.ImmediatelyExpire = true
	require.NoError(t, f.q.SendMessage(t.Context(), queue, msg))
	receive(t, done)
	require.EqualValues(t, 2, calls.Load())
}

func TestCrashRedeliveriesDoNotConsumeRejectionBudget(t *testing.T) {
	f := newTestMessageQueue(t, WithMessageRejection(true, 1))
	queue := msgqueue.NewRandomStaticQueue()
	require.NoError(t, f.q.SendMessage(t.Context(), queue, fixtureMessage(t, 1)))
	state, err := f.q.initQueue(t.Context(), queue)
	require.NoError(t, err)
	for range 3 {
		msg, err := state.consumer.Next(natsjs.FetchMaxWait(time.Second))
		require.NoError(t, err)
		require.NoError(t, msg.Nak())
	}
	done := make(chan struct{}, 1)
	subscribe(t, f.q, queue, msgqueue.NoOpHook, func(*msgqueue.Message) error { done <- struct{}{}; return nil })
	receive(t, done)
	waitDrained(t, f, queue)
}

func TestNonDurableQueueUsesMemoryStorage(t *testing.T) {
	f := newTestMessageQueue(t)
	queue := testQueue{name: "memory_" + uuid.NewString(), nonDurable: true}
	require.NoError(t, f.q.SendMessage(t.Context(), queue, fixtureMessage(t, 1)))
	name, _ := f.q.queueNames(queue)
	stream, err := f.admin.Stream(t.Context(), name)
	require.NoError(t, err)
	info, err := stream.Info(t.Context())
	require.NoError(t, err)
	require.Equal(t, natsjs.MemoryStorage, info.Config.Storage)
	done := make(chan struct{}, 1)
	subscribe(t, f.q, queue, msgqueue.NoOpHook, func(*msgqueue.Message) error { done <- struct{}{}; return nil })
	receive(t, done)
}

func TestImmediateDeliverySharesHandlerQOSAndCleanup(t *testing.T) {
	f := newTestMessageQueue(t, WithQos(1))
	queue := msgqueue.NewRandomStaticQueue()
	release := make(chan struct{})
	defer close(release)
	entered := make(chan struct{}, 1)
	var once sync.Once
	stop, err := f.q.Subscribe(queue, func(*msgqueue.Message) error { once.Do(func() { entered <- struct{}{} }); <-release; return nil }, msgqueue.NoOpHook)
	require.NoError(t, err)
	msg := fixtureMessage(t, 1)
	msg.ImmediatelyExpire = true
	require.NoError(t, f.q.SendMessage(t.Context(), queue, msg))
	receive(t, entered)
	done := make(chan error, 1)
	go func() { done <- stop() }()
	select {
	case <-done:
		t.Fatal("cleanup returned with a live handler still running")
	case <-time.After(100 * time.Millisecond):
	}
	release <- struct{}{}
	require.NoError(t, receive(t, done))
}

func TestCancelledGateAcquisitionDoesNotWaitForLeaseTTL(t *testing.T) {
	f := newTestMessageQueue(t)
	queue := msgqueue.NewRandomStaticQueue()
	gate, err := f.q.acquireGate(t.Context(), queue)
	require.NoError(t, err)
	defer gate.release()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = f.q.acquireGate(ctx, queue)
	require.ErrorIs(t, err, context.Canceled)
}
