import fs from "node:fs";
import assert from "node:assert/strict";
import {
  HatchetClient,
  ConcurrencyLimitStrategy,
} from "@hatchet-dev/typescript-sdk/v1";
if (process.env.HATCHET_TEST_TOKEN_FILE)
  process.env.HATCHET_CLIENT_TOKEN = fs
    .readFileSync(process.env.HATCHET_TEST_TOKEN_FILE, "utf8")
    .trim();
process.env.HATCHET_CLIENT_HOST_PORT ??= "127.0.0.1:19777";
process.env.HATCHET_CLIENT_TLS_STRATEGY ??= "none";
process.env.HATCHET_CLIENT_SERVER_URL ??= "http://127.0.0.1:19888";
const hatchet = HatchetClient.init();
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const emit = (type, data = {}) =>
  console.log(JSON.stringify({ type, ...data }));
const retryAttempts = new Map();
const direct = hatchet.task({
  name: "jetstream-parity-direct",
  retries: 1,
  fn: async (input, ctx) => {
    retryAttempts.set(input.id, (retryAttempts.get(input.id) || 0) + 1);
    if (ctx.retryCount() === 0) throw new Error("intentional retry");
    return { id: input.id, attempt: ctx.retryCount() + 1 };
  },
});
let active = 0,
  peak = 0;
const limited = hatchet.task({
  name: "jetstream-parity-concurrency",
  concurrency: {
    maxRuns: 1,
    limitStrategy: ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
    expression: "'shared'",
  },
  fn: async (input) => {
    active++;
    peak = Math.max(peak, active);
    try {
      await delay(150);
      return { id: input.id };
    } finally {
      active--;
    }
  },
});
const dag = hatchet.workflow({ name: "jetstream-parity-dag" });
const parent = dag.task({
  name: "parent",
  fn: async (input) => ({ value: input.value + 1 }),
});
dag.task({
  name: "child",
  parents: [parent],
  fn: async (_input, ctx) => ({
    value: (await ctx.parentOutput(parent)).value * 2,
  }),
});
const observed = [];
const eventA = hatchet.task({
  name: "jetstream-parity-event-a",
  onEvents: ["jetstream.parity.created"],
  fn: async (input) => {
    observed.push({ destination: "a", id: input.id });
    return input;
  },
});
const eventB = hatchet.task({
  name: "jetstream-parity-event-b",
  onEvents: ["jetstream.parity.created"],
  fn: async (input) => {
    observed.push({ destination: "b", id: input.id });
    return input;
  },
});
let waiting = false;
const durable = hatchet.durableTask({
  name: "jetstream-parity-durable",
  executionTimeout: "2m",
  fn: async (input, ctx) => {
    await ctx.sleepFor("1s");
    waiting = true;
    const event = await ctx.waitForEvent(
      "jetstream.parity.release",
      `input.correlation == '${input.correlation}'`,
      undefined,
      input.correlation,
      "1m",
    );
    return { released: true, event };
  },
});
const worker = await hatchet.worker("jetstream-parity-worker", {
  workflows: [direct, limited, dag, eventA, eventB, durable],
  slots: 8,
});
worker.start().catch((error) => {
  emit("worker-error", { error: String(error) });
  process.exit(1);
});
const deadline = setTimeout(() => {
  emit("timeout");
  process.exit(2);
}, 120000);
try {
  await delay(1500);
  const results = await Promise.all(
    Array.from({ length: 20 }, (_, id) => direct.runAndWait({ id })),
  );
  assert(results.every((r) => r.attempt === 2));
  assert([...retryAttempts.values()].every((n) => n === 2));
  emit("direct-retry", {
    completed: results.length,
    attempts: [...retryAttempts],
  });
  const limitedResults = await Promise.all(
    Array.from({ length: 6 }, (_, id) => limited.runAndWait({ id })),
  );
  assert.equal(limitedResults.length, 6);
  assert.equal(peak, 1);
  emit("concurrency", { completed: 6, peak });
  const dagResult = await dag.runAndWait({ value: 3 });
  assert.equal(dagResult.child.value, 8);
  emit("dag", { result: dagResult });
  const eventId = String(Date.now());
  await hatchet.events.push("jetstream.parity.created", { id: eventId });
  for (
    let i = 0;
    i < 200 && observed.filter((e) => e.id === eventId).length < 2;
    i++
  )
    await delay(50);
  assert.deepEqual(
    observed
      .filter((e) => e.id === eventId)
      .map((e) => e.destination)
      .sort(),
    ["a", "b"],
  );
  emit("event-fanout", { observed });
  const correlation = "test-" + Date.now();
  const durableResult = durable.runAndWait({ correlation });
  for (let i = 0; i < 200 && !waiting; i++) await delay(50);
  assert(waiting);
  if (process.env.HATCHET_TEST_RESTART_DIR) {
    const folder = process.env.HATCHET_TEST_RESTART_DIR;
    fs.writeFileSync(`${folder}/waiting`, "ready");
    for (let i = 0; i < 600 && !fs.existsSync(`${folder}/restarted`); i++)
      await delay(100);
    assert(
      fs.existsSync(`${folder}/restarted`),
      "engine restart did not complete",
    );
    emit("engine-restarted-with-worker-alive");
  }
  await hatchet.events.push(
    "jetstream.parity.release",
    { correlation },
    { scope: correlation },
  );
  const released = await durableResult;
  assert.equal(released.released, true);
  emit("durable-wait", { result: released });
  emit("summary", {
    passed: true,
    direct: 20,
    concurrency: 6,
    dag: 1,
    eventDestinations: 2,
    durable: 1,
  });
  clearTimeout(deadline);
  process.exit(0);
} catch (error) {
  emit("failure", { error: String(error), stack: error.stack });
  clearTimeout(deadline);
  process.exit(1);
}
