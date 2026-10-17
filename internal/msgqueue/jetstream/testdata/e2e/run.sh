#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
root="$(git -C "$script_dir" rev-parse --show-toplevel)"
work="$(mktemp -d)"
engine_pid=""
worker_pid=""
cleanup() {
  if [[ -n "$worker_pid" ]]; then kill "$worker_pid" 2>/dev/null || true; fi
  if [[ -n "$engine_pid" ]]; then kill "$engine_pid" 2>/dev/null || true; fi
  docker compose -f "$script_dir/compose.yaml" down -v
  echo "Engine test artifacts: $work"
}
trap cleanup EXIT

queue_kind="${HATCHET_TEST_QUEUE_KIND:-jetstream}"
case "$queue_kind" in
  jetstream) broker=nats ;;
  rabbitmq) broker=rabbitmq ;;
  *) echo "Unsupported test queue kind: $queue_kind"; exit 1 ;;
esac
docker compose -f "$script_dir/compose.yaml" up -d --wait postgres "$broker"
cd "$root"
for binary in hatchet-lite hatchet-admin hatchet-migrate; do
  go build -p 2 -ldflags '-X main.Version=v0.112.10-jetstream' -o "$work/$binary" "./cmd/$binary"
done
cp "$script_dir/"{smoke.mjs,package.json,package-lock.json} "$work/"
npm ci --prefix "$work" --ignore-scripts --no-audit --no-fund

export DATABASE_URL='postgresql://hatchet:lab-only@127.0.0.1:19432/hatchet?sslmode=disable'
export SERVER_MSGQUEUE_KIND="$queue_kind"
export SERVER_MSGQUEUE_RABBITMQ_URL=amqp://guest:guest@127.0.0.1:19433/
export SERVER_MSGQUEUE_JETSTREAM_URL=nats://127.0.0.1:19422
export SERVER_MSGQUEUE_JETSTREAM_REPLICAS=1
export SERVER_MSGQUEUE_JETSTREAM_MAX_BYTES=67108864
export SERVER_MSGQUEUE_JETSTREAM_STREAM_PREFIX=HATCHET_E2E
export SERVER_GRPC_BIND_ADDRESS=127.0.0.1
export SERVER_GRPC_INSECURE=true
export SERVER_GRPC_PORT=19777
export SERVER_GRPC_BROADCAST_ADDRESS=127.0.0.1:19777
export SERVER_INTERNAL_CLIENT_INTERNAL_GRPC_BROADCAST_ADDRESS=127.0.0.1:19777
export SERVER_PORT=19800
export SERVER_URL=http://127.0.0.1:19888
export SERVER_AUTH_COOKIE_DOMAIN=localhost
export SERVER_AUTH_COOKIE_INSECURE=true
export SERVER_AUTH_SET_EMAIL_VERIFIED=true
export SERVER_LOGGER_LEVEL=warn
export DATABASE_LOGGER_LEVEL=warn
export SERVER_SECURITY_CHECK_ENABLED=false
export SERVER_ANALYTICS_POSTHOG_ENABLED=false
export SERVER_ANALYTICS_AGGREGATE_ENABLED=false
export LITE_STATIC_ASSET_DIR="$work/public"
export LITE_FRONTEND_PORT=19881
export LITE_RUNTIME_PORT=19888
export HATCHET_TEST_TOKEN_FILE="$work/token.txt"
export HATCHET_TEST_RESTART_DIR="$work"
mkdir "$work/public"
echo 'JetStream engine test' > "$work/public/index.html"
cd "$work"
./hatchet-migrate > migrate.log 2>&1
./hatchet-admin quickstart --skip certs --generated-config-dir "$work/config" --overwrite=false > setup.log 2>&1
./hatchet-admin token create --config "$work/config" --name jetstream-test > token.txt 2>token.log
start_engine() {
  ./hatchet-lite --config "$work/config" >> engine.log 2>&1 &
  engine_pid=$!
  for _ in $(seq 1 120); do
    kill -0 "$engine_pid"
    if curl -fsS http://127.0.0.1:19888/ >/dev/null; then return; fi
    sleep 1
  done
  cat engine.log
  return 1
}
start_engine
node smoke.mjs > smoke.log 2>&1 &
worker_pid=$!
for _ in $(seq 1 180); do
  if [[ -f waiting ]]; then break; fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then cat smoke.log; exit 1; fi
  sleep 1
done
test -f waiting
kill -KILL "$engine_pid"
wait "$engine_pid" 2>/dev/null || true
engine_pid=""
start_engine
touch restarted
if ! wait "$worker_pid"; then cat smoke.log; tail -100 engine.log; exit 1; fi
worker_pid=""
cat smoke.log
