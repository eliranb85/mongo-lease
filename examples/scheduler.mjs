// A minimal multi-instance scheduler built on mongo-lease.
//
// Run several copies (see docker-compose.yml). Every instance competes for
// leadership; only the leader enqueues work on each tick. Every instance is
// also a worker that claims jobs from the shared queue, and a per-account
// lease lock guarantees one job per account at a time.
import { hostname } from "node:os";
import { MongoClient } from "mongodb";
import { LeaderElector, LeaseLock, PriorityQueue } from "../dist/index.js";

const uri = process.env.MONGODB_URI ?? "mongodb://localhost:27017";
const TICK_MS = Number(process.env.TICK_MS ?? 5_000);
const ACCOUNTS = ["acct-a", "acct-b", "acct-c"];

const client = await new MongoClient(uri).connect();
const db = client.db("mongo_lease_example");

const instanceId = `${hostname()}-${process.pid}`;
const leader = new LeaderElector(db, { name: "scheduler", ttlMs: 15_000, instanceId });
const locks = new LeaseLock(db, { ttlMs: 60_000 });
const queue = new PriorityQueue(db, { backoffMs: [2_000, 5_000] });
await Promise.all([leader.ensureIndexes(), locks.ensureIndexes(), queue.ensureIndexes()]);

let wasLeader = false;
let tick = 0;

async function schedulerTick() {
  const isLeader = await leader.tryAcquire();
  if (isLeader && !wasLeader) console.log(`LEADER ${instanceId}`);
  wasLeader = isLeader;
  if (!isLeader) return;

  tick++;
  for (const account of ACCOUNTS) {
    // Same dedupeKey within a tick window collapses repeated triggers.
    await queue.enqueue({ dedupeKey: `sync:${account}:${tick}`, concurrencyKey: account, payload: { account, tick } });
  }
}

async function workOnce() {
  const claimed = await queue.claimNext();
  if (!claimed) return;
  const { job, executionId } = claimed;
  const out = await locks.withLock(`account:${job.payload.account}`, async () => {
    await new Promise((r) => setTimeout(r, 300 + Math.random() * 700)); // pretend to work
    return `synced ${job.payload.account}`;
  }, { owner: instanceId });

  if (out.ran) {
    await queue.complete(job._id, executionId, out.value);
    console.log(`DONE ${job.dedupeKey} by ${instanceId}`);
  } else {
    await queue.fail(job._id, executionId, "account busy");
  }
}

const timers = [
  setInterval(() => schedulerTick().catch((e) => console.error("tick", e)), TICK_MS),
  setInterval(() => workOnce().catch((e) => console.error("work", e)), 250),
  setInterval(() => queue.reapStuck(30_000).catch((e) => console.error("reap", e)), 10_000),
];
await schedulerTick();

async function shutdown() {
  timers.forEach(clearInterval);
  await leader.release().catch(() => undefined);
  await client.close();
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
