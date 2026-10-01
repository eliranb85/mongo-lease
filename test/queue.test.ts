import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import type { Db } from "mongodb";
import { PriorityQueue } from "../src/index.js";
import { fakeClock, startMongo } from "./setup.js";

let db: Db;
let stop: () => Promise<void>;
let n = 0;

beforeAll(async () => {
  ({ db, stop } = await startMongo());
});
afterAll(async () => stop());

type Payload = { task: string };

/** Fresh collection per test so tests do not see each other's jobs. */
async function makeQueue(opts: ConstructorParameters<typeof PriorityQueue>[1] = {}) {
  const q = new PriorityQueue<Payload>(db, { collection: `jobs_${++n}`, ...opts });
  await q.ensureIndexes();
  return q;
}

describe("PriorityQueue", () => {
  let clock: ReturnType<typeof fakeClock>;
  beforeEach(() => {
    clock = fakeClock();
  });

  it("collapses duplicate pending jobs, even when enqueued concurrently", async () => {
    const q = await makeQueue();
    const results = await Promise.all(
      Array.from({ length: 10 }, () => q.enqueue({ dedupeKey: "post:42", payload: { task: "post" } })),
    );
    const ids = new Set(results.map((r) => r.id.toHexString()));
    expect(ids.size).toBe(1);
    expect(results.filter((r) => !r.deduped)).toHaveLength(1);
  });

  it("claims high priority before normal, then FIFO", async () => {
    const q = await makeQueue({ now: clock.now });
    await q.enqueue({ dedupeKey: "a", payload: { task: "a" } });
    clock.advance(1);
    await q.enqueue({ dedupeKey: "b", payload: { task: "b" } });
    clock.advance(1);
    await q.enqueue({ dedupeKey: "c", payload: { task: "c" }, priority: "high" });

    const order: string[] = [];
    for (let i = 0; i < 3; i++) order.push((await q.claimNext())!.job.payload.task);
    expect(order).toEqual(["c", "a", "b"]);
    expect(await q.claimNext()).toBeNull();
  });

  it("a high-priority request upgrades an existing normal job", async () => {
    const q = await makeQueue({ now: clock.now });
    const first = await q.enqueue({ dedupeKey: "x", payload: { task: "x" } });
    clock.advance(1);
    await q.enqueue({ dedupeKey: "y", payload: { task: "y" } });
    expect(await q.position(first.id)).toBe(1);

    const y = await q.enqueue({ dedupeKey: "y", payload: { task: "y" }, priority: "high" });
    expect(y.deduped).toBe(true);
    expect(await q.position(y.id)).toBe(1);
    expect(await q.position(first.id)).toBe(2);
  });

  it("never runs two jobs with the same concurrencyKey at once", async () => {
    const q = await makeQueue();
    for (let i = 0; i < 5; i++) {
      await q.enqueue({ dedupeKey: `job-${i}`, concurrencyKey: "account:7", payload: { task: `${i}` } });
    }
    await q.enqueue({ dedupeKey: "other", concurrencyKey: "account:8", payload: { task: "other" } });

    const claims = (await Promise.all(Array.from({ length: 8 }, () => q.claimNext()))).filter(Boolean);
    const keys = claims.map((c) => c!.job.concurrencyKey).sort();
    expect(keys).toEqual(["account:7", "account:8"]);

    const seven = claims.find((c) => c!.job.concurrencyKey === "account:7")!;
    await q.complete(seven.job._id, seven.executionId);
    expect((await q.claimNext())?.job.concurrencyKey).toBe("account:7");
  });

  it("retries with backoff, then fails after maxAttempts", async () => {
    const q = await makeQueue({ now: clock.now, backoffMs: [1_000, 5_000], maxAttempts: 3 });
    const { id } = await q.enqueue({ dedupeKey: "flaky", payload: { task: "flaky" } });

    let c = (await q.claimNext())!;
    expect(await q.fail(id, c.executionId, "timeout")).toBe("retry");
    expect(await q.claimNext()).toBeNull(); // still backing off
    clock.advance(1_000);

    c = (await q.claimNext())!;
    expect(await q.fail(id, c.executionId, "timeout")).toBe("retry");
    clock.advance(4_999);
    expect(await q.claimNext()).toBeNull();
    clock.advance(1);

    c = (await q.claimNext())!;
    expect(await q.fail(id, c.executionId, "timeout")).toBe("failed");
    expect(await q.get(id)).toMatchObject({ status: "failed", attempts: 3, error: "timeout" });
  });

  it("ignores a late answer from an older attempt", async () => {
    const q = await makeQueue({ now: clock.now, backoffMs: [0] });
    const { id } = await q.enqueue({ dedupeKey: "late", payload: { task: "late" } });

    const first = (await q.claimNext())!;
    await q.fail(id, first.executionId, "network");
    const second = (await q.claimNext())!;

    expect(await q.complete(id, first.executionId, "old")).toBe(false);
    expect(await q.fail(id, first.executionId, "old")).toBe("stale");
    expect(await q.complete(id, second.executionId, "new")).toBe(true);
    expect(await q.get(id)).toMatchObject({ status: "done", result: "new" });
  });

  it("reaps jobs stuck in running and frees their concurrencyKey", async () => {
    const q = await makeQueue({ now: clock.now, backoffMs: [0] });
    const { id } = await q.enqueue({ dedupeKey: "stuck", payload: { task: "stuck" } });
    await q.claimNext(); // worker dies here

    clock.advance(60_000);
    expect(await q.reapStuck(30_000)).toBe(1);
    const again = await q.claimNext();
    expect(again?.job._id.equals(id)).toBe(true);
    expect(again?.job.attempts).toBe(1);
  });

  it("respects delayMs", async () => {
    const q = await makeQueue({ now: clock.now });
    await q.enqueue({ dedupeKey: "later", payload: { task: "later" }, delayMs: 5_000 });
    expect(await q.claimNext()).toBeNull();
    clock.advance(5_000);
    expect(await q.claimNext()).not.toBeNull();
  });
});
