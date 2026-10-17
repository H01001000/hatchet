# JetStream durable queue backend — stage 1

This package implements Hatchet's existing `msgqueue.MessageQueue` interface for
ordinary durable queues. It follows `internal/msgqueue/rabbitmq/rabbitmq.go` in
its constructor, option names, publication/subscription methods and acknowledgment
hooks so the two backends can be reviewed side by side.

This first stage is a library and test boundary. It is **not registered as a
`SERVER_MSGQUEUE_KIND`** yet. A full engine switch requires dispatcher ownership
and expiry/rerouting support in the next stage. Existing backends and defaults
are unchanged.

## Behavior and RabbitMQ mapping

| RabbitMQ backend | JetStream backend |
| --- | --- |
| Durable named queue | File-backed WorkQueue stream per queue |
| Competing queue consumers | Shared durable `workers` consumer per stream |
| `SendMessage` | Synchronous publication, returns after PubAck |
| `preAck → ACK → postAck` | `preAck → confirmed ACK → postAck` |
| Retry through delayed DLQ | Delayed NAK/redelivery on the same consumer |
| Invalid JSON pre-ack failure is permanent | Same typed/wrapped PostgreSQL error classification |
| Optional maximum `x-death` count | Optional maximum transport redelivery count |
| Prefetch/QoS | Bounded pull batches and handler concurrency per subscription |
| Gzip and large multi-payload chunks | Shared compressor and JSON wire envelope |
| Independent publisher/subscriber resources | Two owned NATS connections |

Queue names are SHA-256 encoded into stream and subject names. This preserves
queue identity without interpreting queue names as NATS wildcards. The namespace
prefix separates deployments sharing a NATS account.

The stream uses `DiscardNew`, has no age expiry, and rejects new publications
when its byte limit is reached. It does not evict unacknowledged backlog.
Declarations fail on conflicting existing configurations instead of modifying
them. Successful ACKs remove messages through WorkQueue retention.

Delivery is at least once. Uncertain publication or acknowledgment outcomes can
produce retries; handlers must retain their existing idempotency behavior.
`Message.ID` identifies a message kind and is deliberately not used as a
`Nats-Msg-Id` deduplication key. A multi-chunk send can partially succeed before a
transport failure, so retrying that send can duplicate earlier chunks.

A pre-ack failure receives delayed redelivery unless it is permanent. A
post-ack failure is logged and does not request redelivery. As with RabbitMQ,
permanently rejected/malformed messages are acknowledged and dropped. When the
optional retry bound is enabled, it includes crash/ACK-timeout redeliveries as
well as handler failures; this differs from RabbitMQ's `x-death` counter.

## Supported boundary

Ordinary durable queues, including the dispatcher **dead-letter** queue, are
supported. Dispatcher destination queues, exclusive/non-durable/auto-deleted
queues, expiring queues and immediately-expiring publications are rejected
explicitly. No dispatcher message is silently stored with incorrect expiry
semantics. The queue interface and existing engine callers are not changed.

Next stages:

1. Dispatcher ownership, queue inactivity, 20-second message expiry and explicit
   delivery to the dispatcher DLQ, including unavailable-dispatcher recovery.
2. Server configuration/loader integration and an intentional choice of the
   separate best-effort pubsub backend.
3. Full engine/SDK and multi-node quorum/failover checks before enabling it for
   deployment.

## Options

`New` returns `(cleanup, backend, error)`, like RabbitMQ. `Clone` owns independent
connections but addresses the same queue namespace; closing a clone does not
close its parent. Subscription cleanup stops delivery and waits for active hooks.
Hooks must return so cleanup can finish. `SetQOS` affects new subscriptions.

| Option | Default |
| --- | --- |
| `WithURL` | Required |
| `WithCredentials` | Optional; existing URL credentials also work |
| `WithTLS` | Disabled; enabling uses TLS-first with system roots or supplied CA |
| `WithStreamPrefix` | `HATCHET_MQ` |
| `WithReplicas` | 3; use 1 for a single-node development broker |
| `WithMaxBytes` | 1 GiB per stream |
| `WithQos` | 100 |
| `WithAckWait` | 5 minutes |
| `WithRequestTimeout` | 5 seconds |
| `WithDeadLetterBackoff` | 5 seconds |
| `WithGzipCompression` | Disabled; threshold defaults to 5 KiB |
| `WithMessageRejection` | Disabled; helper defaults to 5 retries if its limit is nonpositive |

The broker must have JetStream enabled and `max_payload` at least 16 MiB, matching
Hatchet's existing message-size contract. Stream byte limits and replica counts
must fit the broker/account capacity. Separate deployments should use different
prefixes. Default construction needs three available JetStream servers.

## Tests

Unit tests require no server:

```sh
go test ./internal/msgqueue/jetstream
```

Run a dedicated test broker from the repository root:

```sh
docker run -d --name hatchet-jetstream-test \
  -p 127.0.0.1:4222:4222 \
  -v "$PWD/internal/msgqueue/jetstream/testdata/nats.conf:/etc/nats/nats.conf:ro" \
  nats:2.12.4 -c /etc/nats/nats.conf

HATCHET_TEST_JETSTREAM_URL=nats://127.0.0.1:4222 \
HATCHET_TEST_JETSTREAM_CONTAINER=hatchet-jetstream-test \
  go test -tags integration -count=1 -timeout=180s ./internal/msgqueue/jetstream

docker rm -f -v hatchet-jetstream-test
```

The optional container variable enables a test that restarts that broker. Use a
dedicated test container: the test intentionally interrupts every client on it.
Without the variable only that restart test is skipped. The suite uses unique
stream prefixes and removes its streams after testing.

Tests cover backlog across backend recreation, competing consumers, pre/post-ack
ordering, delayed retries, permanent errors, retry limits, unacknowledged delivery
after connection loss, QoS, compression/chunking, full-stream rejection without
backlog eviction, malformed envelopes, unsupported flags, configuration conflicts,
idempotent shutdown and optional broker restart. Run with `-race` where supported.
