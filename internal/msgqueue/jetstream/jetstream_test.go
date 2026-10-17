package jetstream

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
)

type testQueue struct {
	name                                   string
	nonDurable, auto, exclusive, expirable bool
}

func (q testQueue) Name() string        { return q.name }
func (q testQueue) Durable() bool       { return !q.nonDurable }
func (q testQueue) AutoDeleted() bool   { return q.auto }
func (q testQueue) Exclusive() bool     { return q.exclusive }
func (q testQueue) IsExpirable() bool   { return q.expirable }
func (q testQueue) DLQ() msgqueue.Queue { return nil }
func (q testQueue) IsDLQ() bool         { return false }
func (q testQueue) IsAutoDLQ() bool     { return false }

func TestValidateQueue(t *testing.T) {
	require.NoError(t, validateQueue(msgqueue.TASK_PROCESSING_QUEUE))
	require.NoError(t, validateQueue(msgqueue.DISPATCHER_DEAD_LETTER_QUEUE))
	require.ErrorIs(t, validateQueue(msgqueue.QueueTypeFromDispatcherID(uuid.New())), ErrUnsupportedQueue)
	for _, q := range []testQueue{
		{name: "temporary", nonDurable: true}, {name: "auto", auto: true},
		{name: "exclusive", exclusive: true}, {name: "expiring", expirable: true},
	} {
		t.Run(q.name, func(t *testing.T) { require.ErrorIs(t, validateQueue(q), ErrUnsupportedQueue) })
	}
	require.Error(t, validateQueue(nil))
	require.Error(t, validateQueue(testQueue{}))
}

func TestQueueNamesAreStableAndSubjectSafe(t *testing.T) {
	backend := &MessageQueueImpl{streamPrefix: "TEST"}
	q := testQueue{name: "untrusted.> * / queue"}
	stream, subject := backend.queueNames(q)
	otherStream, otherSubject := backend.queueNames(q)
	require.Equal(t, stream, otherStream)
	require.Equal(t, subject, otherSubject)
	require.NotContains(t, subject, ">")
	require.NotContains(t, subject, "*")
	require.NotContains(t, subject, " ")
	differentStream, _ := backend.queueNames(testQueue{name: "another queue"})
	require.NotEqual(t, stream, differentStream)
	otherNamespace := &MessageQueueImpl{streamPrefix: "ANOTHER"}
	_, differentSubject := otherNamespace.queueNames(q)
	require.NotEqual(t, subject, differentSubject)
}

func TestValidateOptions(t *testing.T) {
	for name, change := range map[string]MessageQueueImplOpt{
		"empty URL": WithURL(""), "invalid prefix": WithStreamPrefix("prefix.>"),
		"long prefix":   WithStreamPrefix(strings.Repeat("x", 33)),
		"zero replicas": WithReplicas(0), "too many replicas": WithReplicas(6),
		"zero qos": WithQos(0), "qos overflow": WithQos(65537),
		"small capacity": WithMaxBytes(1), "zero ack wait": WithAckWait(0),
		"zero timeout": WithRequestTimeout(0), "zero backoff": WithDeadLetterBackoff(0),
		"CA without TLS": WithTLS(false, "root.pem"), "nil logger": WithLogger(nil),
	} {
		t.Run(name, func(t *testing.T) {
			opts := defaultMessageQueueImplOpts()
			WithURL("nats://localhost:4222")(opts)
			change(opts)
			require.Error(t, opts.validate())
		})
	}
	opts := defaultMessageQueueImplOpts()
	WithURL("nats://localhost:4222")(opts)
	require.NoError(t, opts.validate())
	WithGzipCompression(true, 0)(opts)
	WithMessageRejection(true, 0)(opts)
	require.Equal(t, msgqueue.DefaultCompressionThreshold, opts.compressionThreshold)
	require.EqualValues(t, 5, opts.maxDeathCount)
}

func TestMessageBodiesPreservePayloadOrderAndCaller(t *testing.T) {
	msg := &msgqueue.Message{ID: "same-kind", Payloads: [][]byte{
		bytes.Repeat([]byte("a"), 180), bytes.Repeat([]byte("b"), 180),
		bytes.Repeat([]byte("c"), 180), bytes.Repeat([]byte("d"), 180),
	}}
	original, err := json.Marshal(msg)
	require.NoError(t, err)
	backend := &MessageQueueImpl{}
	bodies, err := backend.messageBodies(msg, 600)
	require.NoError(t, err)
	require.Greater(t, len(bodies), 1)
	var payloads [][]byte
	for _, body := range bodies {
		require.LessOrEqual(t, len(body), 600)
		var decoded msgqueue.Message
		require.NoError(t, json.Unmarshal(body, &decoded))
		require.Equal(t, msg.ID, decoded.ID)
		payloads = append(payloads, decoded.Payloads...)
	}
	require.Equal(t, msg.Payloads, payloads)
	after, err := json.Marshal(msg)
	require.NoError(t, err)
	require.Equal(t, original, after)
}

func TestMessageBodiesRejectBeforeReturningPartialChunks(t *testing.T) {
	backend := &MessageQueueImpl{}
	bodies, err := backend.messageBodies(&msgqueue.Message{Payloads: [][]byte{
		[]byte("small"), bytes.Repeat([]byte("x"), 1000),
	}}, 600)
	require.ErrorContains(t, err, "single message size")
	require.Nil(t, bodies)
}

func TestMessageBodiesCompressionDoesNotMutateCaller(t *testing.T) {
	msg := &msgqueue.Message{Payloads: [][]byte{bytes.Repeat([]byte("repeated"), 100)}}
	backend := &MessageQueueImpl{compressor: msgqueue.Compressor{Enabled: true, Threshold: 1}}
	bodies, err := backend.messageBodies(msg, 600)
	require.NoError(t, err)
	require.Len(t, bodies, 1)
	var decoded msgqueue.Message
	require.NoError(t, json.Unmarshal(bodies[0], &decoded))
	require.True(t, decoded.Compressed)
	plain, err := msgqueue.DecompressPayloads(decoded.Payloads)
	require.NoError(t, err)
	require.Equal(t, msg.Payloads, plain)
	require.False(t, msg.Compressed)
	require.Equal(t, bytes.Repeat([]byte("repeated"), 100), msg.Payloads[0])

	// Already-compressed envelopes may be forwarded without compression twice.
	forwarded, err := backend.messageBodies(&decoded, 600)
	require.NoError(t, err)
	require.Equal(t, bodies, forwarded)
}

func TestPermanentPreAckErrors(t *testing.T) {
	for _, err := range []error{
		&pgconn.PgError{Code: pgerrcode.InvalidTextRepresentation},
		fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: pgerrcode.InvalidTextRepresentation}),
		errors.New("failure (SQLSTATE 22P02)"),
		errors.New("invalid input syntax for type jsonb"),
	} {
		require.True(t, isPermanentPreAckError(err))
	}
	require.False(t, isPermanentPreAckError(nil))
	require.False(t, isPermanentPreAckError(errors.New("connection lost")))
	require.False(t, isPermanentPreAckError(&pgconn.PgError{Code: pgerrcode.SerializationFailure}))
}

func TestSetQOSBounds(t *testing.T) {
	backend := &MessageQueueImpl{}
	backend.SetQOS(7)
	require.EqualValues(t, 7, backend.qos.Load())
	backend.SetQOS(0)
	backend.SetQOS(-1)
	backend.SetQOS(65537)
	require.EqualValues(t, 7, backend.qos.Load())
}
