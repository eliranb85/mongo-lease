import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Db } from "mongodb";
import { LeaseLock } from "../src/index.js";
import { fakeClock, startMongo } from "./setup.js";

let db: Db;
let stop: () => Promise<void>;

beforeAll(async () => {
  ({ db, stop } = await startMongo());
});
afterAll(async () => stop());

describe("LeaseLock", () => {
  it("grants a key to exactly one of many concurrent callers", async () => {
    const lock = new LeaseLock(db);
    await lock.ensureIndexes();
    const results = await Promise.all(
      Array.from({ length: 25 }, (_, i) => lock.acquire("account:1", { owner: `w${i}` })),
    );
    expect(results.filter((r) => r.ok)).toHaveLength(1);
    const loser = results.find((r) => !r.ok);
    expect(loser).toMatchObject({ ok: false, reason: "held" });
  });

  it("locks different keys independently", async () => {
    const lock = new LeaseLock(db);
    expect((await lock.acquire("k:a")).ok).toBe(true);
    expect((await lock.acquire("k:b")).ok).toBe(true);
  });

  it("only the token holder can release", async () => {
    const lock = new LeaseLock(db);
    const res = await lock.acquire("tok");
    if (!res.ok) throw new Error("expected lock");

    expect(await lock.release("tok", "wrong-token")).toBe(false);
    expect((await lock.acquire("tok")).ok).toBe(false);
    expect(await lock.release("tok", res.token)).toBe(true);
    expect((await lock.acquire("tok")).ok).toBe(true);
  });

  it("takes over an expired lease without waiting for the TTL monitor", async () => {
    const clock = fakeClock();
    const lock = new LeaseLock(db, { ttlMs: 1_000, now: clock.now });
    const first = await lock.acquire("expire");
    if (!first.ok) throw new Error("expected lock");

    clock.advance(1_001);
    const second = await lock.acquire("expire");
    expect(second.ok).toBe(true);

    // The stale holder wakes up late and tries to release: the new holder keeps the lock.
    expect(await lock.release("expire", first.token)).toBe(false);
    expect(await lock.inspect("expire")).not.toBeNull();
  });

  it("extend keeps a long-running holder alive, and fails once the lease is lost", async () => {
    const clock = fakeClock();
    const lock = new LeaseLock(db, { ttlMs: 1_000, now: clock.now });
    const res = await lock.acquire("extend");
    if (!res.ok) throw new Error("expected lock");

    clock.advance(900);
    expect(await lock.extend("extend", res.token)).toBe(true);
    clock.advance(900);
    expect((await lock.acquire("extend")).ok).toBe(false);

    clock.advance(1_200);
    expect(await lock.extend("extend", res.token)).toBe(false);
  });

  it("withLock releases even when the work throws", async () => {
    const lock = new LeaseLock(db);
    await expect(
      lock.withLock("throws", async () => {
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");
    expect((await lock.acquire("throws")).ok).toBe(true);
  });

  it("withLock does not run the work when the key is held", async () => {
    const lock = new LeaseLock(db);
    await lock.acquire("busy");
    let ran = false;
    const out = await lock.withLock("busy", async () => {
      ran = true;
    });
    expect(out.ran).toBe(false);
    expect(ran).toBe(false);
  });

  it("sweepOrphans removes locks held past the cutoff", async () => {
    const clock = fakeClock();
    const lock = new LeaseLock(db, { ttlMs: 60_000, collection: "sweep_locks", now: clock.now });
    await lock.acquire("old");
    clock.advance(30_000);
    await lock.acquire("young");
    clock.advance(20_000);

    expect(await lock.sweepOrphans(40_000)).toBe(1);
    expect(await lock.inspect("old")).toBeNull();
    expect(await lock.inspect("young")).not.toBeNull();
  });

  it("fails closed when the database errors", async () => {
    const lock = new LeaseLock(db, { collection: "x".repeat(300) }); // invalid namespace length
    const res = await lock.acquire("any");
    expect(res).toMatchObject({ ok: false, reason: "error" });
  });
});
