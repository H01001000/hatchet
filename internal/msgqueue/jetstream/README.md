# JetStream message queue backend

This package implements Hatchet's `msgqueue.MessageQueue` interface and is
selectable with `SERVER_MSGQUEUE_KIND=jetstream`. Constructors, options, method
names, compression and acknowledgment hooks follow the RabbitMQ backend so the
implementations can be reviewed side by side. Engine callers and SDK contracts
are unchanged.

## RabbitMQ contract mapping

| RabbitMQ backend | JetStream backend |
| --- | --- |
| Named durable queue | File-backed WorkQueue stream per queue |
| Non-durable queue | Memory-backed stream |
| Competing consumers | Shared durable pull consumer |
| Exclusive dispatcher queue | Renewed KV ownership and a consumer per ownership generation |
| `preAck → ACK → postAck` | `preAck → confirmed ACK → postAck` |
| Delayed automatic DLQ retry | Confirmed forwarding to the retry stream, with stored retry delay |
| Rejected-handler `x-death` budget | Stored rejection count; connection loss does not spend it |
| Dedicated dispatcher DLQ | Explicit confirmed forwarding before source acknowledgment/deletion |
| Dispatcher message TTL | 20-second queued-assignment expiry; in-flight claims protect active delivery |
| Queue inactivity expiry | 10-minute idle cleanup |
| Auto-delete queue | Cleanup after the last subscriber leaves |
| Immediately-expiring publication | Live Core NATS delivery with no offline backlog |
| Prefetch/QoS | Bounded outstanding pulls and active hooks per subscription |
| Gzip, chunking, tracing | Shared envelope/compressor and propagated trace carrier |
| Clone and cleanup | Independent connections, shared namespace; cleanup joins active hooks |

A pre-ack failure is retried through the queue's DLQ unless it is a permanent
PostgreSQL invalid-JSON error. Malformed envelopes and invalid gzip follow the
same rejection route. A post-ack failure is logged without redelivery. Automatic
retry consumers use the same hooks as their primary queue. The optional rejection
limit drops a message when its stored count exceeds the configured maximum.

The durable queue and the engine's separate best-effort `PubSub` interface retain
their distinct contracts. Selecting JetStream defaults `PubSub` to the existing
Core NATS adapter, inheriting the JetStream URL/authentication/TLS settings.
An explicitly configured pubsub backend or URL takes precedence. No RabbitMQ
service is needed with that default. Pubsub remains at most once, as required by
that separate interface; it is not a durable event log.

## Configuration

Single-broker development example:

```sh
export SERVER_MSGQUEUE_KIND=jetstream
export SERVER_MSGQUEUE_JETSTREAM_URL=nats://127.0.0.1:4222
export SERVER_MSGQUEUE_JETSTREAM_REPLICAS=1
```

The broker must enable JetStream and `max_payload` of at least 16 MiB. The test
configuration in `testdata/nats.conf` provides both. Production defaults require
three available JetStream servers. Queue and KV replicas use the same setting.
Use a separate stream prefix for each deployment sharing a NATS account.

All options are available under `msgQueue.jetstream` in server configuration, or
as environment variables prefixed `SERVER_MSGQUEUE_JETSTREAM_`:

| Environment suffix | Default |
| --- | --- |
| `URL` | Required |
| `USERNAME`, `PASSWORD` | Optional; URL credentials also work |
| `TLS_ENABLED`, `TLS_ROOT_CA_FILE` | Disabled; TLS-first handshake, system or supplied roots |
| `STREAM_PREFIX` | `HATCHET_MQ` |
| `REPLICAS` | 3 |
| `MAX_BYTES` | 1073741824 per queue |
| `QOS` | 100 |
| `COMPRESSION_ENABLED`, `COMPRESSION_THRESHOLD` | false, 5120 bytes |
| `ENABLE_MESSAGE_REJECTION`, `MAX_DEATH_COUNT` | false, 1000 |
| `ACK_WAIT` | 5m |
| `REQUEST_TIMEOUT` | 5s |
| `DEAD_LETTER_BACKOFF` | 5s |
| `DISPATCHER_TTL` | 20s |
| `INACTIVITY_TTL` | 10m |
| `LEASE_TIMEOUT` | 15s |
| `SWEEP_INTERVAL` | 1s |

The library offers matching `With...` options. `WithMessageRejection` defaults to
five when called with a nonpositive limit; the server passes its configured limit.
`SetQOS` affects new subscriptions. Hooks must return for cleanup to finish.

## Durability and operational differences

Publication waits for a JetStream PubAck, and the post-ack hook runs only after
confirmed acknowledgment. Streams use `DiscardNew`: a full stream rejects writes
instead of evicting unacknowledged backlog. Conflicting queue declarations fail.
Queue names are hashed into safe stream/subject names.

Delivery is at least once. Publication/acknowledgment uncertainty and partial
multi-chunk sends can produce duplicates. Forwarding uses a stable deduplication
identity including the source publication timestamp, sequence and destination;
this remains bounded by the broker's deduplication window. `Message.ID` identifies
a message kind and is not used as a publication deduplication key. Handlers must
keep their existing idempotency behavior.

JetStream does not implement RabbitMQ's broker-native DLX, exclusive queue or
queue-expiry policies. This backend uses three namespace KV buckets (`owners`,
`claims`, `catalog`) and a renewable maintenance lease to implement those rules.
Expiry forwarding requires a running backend and broker quorum. Expiration is
asynchronous, scanned in bounded batches, and can lag the TTL during outages or
large backlogs. Ownership recovery is bounded by lease expiry, not TCP disconnect
detection. Per-generation consumers prevent stale acknowledgments from a previous
exclusive owner from acknowledging the next generation's deliveries. As with
other at-least-once workers, external handler side effects still need idempotency.

Immediately-expiring messages use a live-only Core NATS subject and do not wait
for durable storage. This models RabbitMQ's zero-TTL/no-backlog intent but does
not reproduce its broker-side arbitration exactly. Nonpersistent publications
on durable queues receive stronger storage durability than RabbitMQ's transient
message flag. These are deliberate transport differences, not claims of identical
broker internals.

The three KV buckets are file-backed, have 64 MiB limits each, and require account
capacity in addition to the per-queue limits. Keep timeout/expiry/replica settings
consistent among processes sharing a namespace. The runtime database remains
PostgreSQL; this change does not port Hatchet's database layer to CockroachDB.

## Verification

```sh
go test ./internal/msgqueue/... ./pkg/config/loader ./pkg/config/server

docker run -d --name hatchet-jetstream-test \
  -p 127.0.0.1:4222:4222 \
  -v "$PWD/internal/msgqueue/jetstream/testdata/nats.conf:/etc/nats/nats.conf:ro" \
  nats:2.12.4 -c /etc/nats/nats.conf
HATCHET_TEST_JETSTREAM_URL=nats://127.0.0.1:4222 \
HATCHET_TEST_JETSTREAM_CONTAINER=hatchet-jetstream-test \
  go test -race -tags integration -count=1 -timeout=240s ./internal/msgqueue/jetstream
docker rm -f -v hatchet-jetstream-test
```

`HATCHET_TEST_JETSTREAM_CONTAINER` enables an intentional restart; use a dedicated
broker. Integration tests use unique namespaces and remove their streams and KV
buckets. Coverage includes competing consumers, hook order, retries, permanent
errors, rejection budgets, crash recovery, exclusive handover, expiry without a
subscriber, in-flight expiry protection, idle/auto-delete cleanup, live-only
messages, QoS, compression/chunking and full-stream rejection.

Three-broker leader/quorum test (intentionally stops its managed containers):

```sh
docker compose -f internal/msgqueue/jetstream/testdata/cluster.compose.yaml up -d --wait
HATCHET_TEST_JETSTREAM_CLUSTER_URLS=nats://127.0.0.1:19423,nats://127.0.0.1:19424,nats://127.0.0.1:19425 \
  go test -tags integration -run TestMessageQueueReplicatedLeaderAndQuorumLoss -timeout=180s ./internal/msgqueue/jetstream
docker compose -f internal/msgqueue/jetstream/testdata/cluster.compose.yaml down -v
```

Full engine/SDK smoke test on Linux, requiring Go, Node, npm, curl and Docker:

```sh
bash internal/msgqueue/jetstream/testdata/e2e/run.sh
```

It builds the current engine/admin/migrations, starts isolated PostgreSQL and NATS,
and exercises 20 retried tasks, six concurrency-limited tasks, a DAG, event fanout,
and durable sleep/event wait. It kills the engine during that wait, keeps the SDK
worker alive, restarts with the same configuration keys and verifies completion.
It disables the server security check/analytics, never publishes an image, removes
its containers and prints the location of its temporary diagnostic files.
Set `HATCHET_TEST_QUEUE_KIND=rabbitmq` to run the same SDK/restart scenario against
RabbitMQ as a control. The focused GitHub workflow runs both backends, plus the
JetStream adapter/race and cluster tests. SDK dependencies are locked to 1.34.0.
