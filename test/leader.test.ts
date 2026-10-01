import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Db } from "mongodb";
import { LeaderElector } from "../src/index.js";
import { fakeClock, startMongo } from "./setup.js";

let db: Db;
let stop: () => Promise<void>;

beforeAll(async () => {
  ({ db, stop } = await startMongo());
});
afterAll(async () => stop());

describe("LeaderElector", () => {
  it("elects exactly one leader when many instances race", async () => {
    const electors = Array.from({ length: 10 }, (_, i) =>
      new LeaderElector(db, { name: "race", instanceId: `node-${i}` }),
    );
    await electors[0]!.ensureIndexes();
    const results = await Promise.all(electors.map((e) => e.tryAcquire()));
    expect(results.filter(Boolean)).toHaveLength(1);

    const winner = electors[results.indexOf(true)]!;
    expect(await winner.currentLeader()).toBe(winner.instanceId);
  });

  it("lets the leader renew and keeps others out while the lease is live", async () => {
    const clock = fakeClock();
    const a = new LeaderElector(db, { name: "renew", instanceId: "a", ttlMs: 1_000, now: clock.now });
    const b = new LeaderElector(db, { name: "renew", instanceId: "b", ttlMs: 1_000, now: clock.now });

    expect(await a.tryAcquire()).toBe(true);
    clock.advance(800);
    expect(await b.tryAcquire()).toBe(false);
    expect(await a.tryAcquire()).toBe(true); // renewal pushes expiry to t+1800
    clock.advance(800);
    expect(await b.tryAcquire()).toBe(false);
  });

  it("hands leadership over once the lease expires", async () => {
    const clock = fakeClock();
    const a = new LeaderElector(db, { name: "failover", instanceId: "a", ttlMs: 1_000, now: clock.now });
    const b = new LeaderElector(db, { name: "failover", instanceId: "b", ttlMs: 1_000, now: clock.now });

    expect(await a.tryAcquire()).toBe(true);
    clock.advance(1_001); // a stopped renewing (crashed)
    expect(await b.tryAcquire()).toBe(true);
    expect(await a.tryAcquire()).toBe(false);
  });

  it("release frees the lease immediately, but only for the holder", async () => {
    const a = new LeaderElector(db, { name: "release", instanceId: "a" });
    const b = new LeaderElector(db, { name: "release", instanceId: "b" });

    expect(await a.tryAcquire()).toBe(true);
    await b.release(); // not the holder: no effect
    expect(await b.tryAcquire()).toBe(false);
    await a.release();
    expect(await b.tryAcquire()).toBe(true);
  });

  it("keeps separate names independent", async () => {
    const x = new LeaderElector(db, { name: "role-x", instanceId: "a" });
    const y = new LeaderElector(db, { name: "role-y", instanceId: "b" });
    expect(await x.tryAcquire()).toBe(true);
    expect(await y.tryAcquire()).toBe(true);
  });
});
