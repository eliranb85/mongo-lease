# mongo-lease

[![CI](https://github.com/eliranb85/mongo-lease/actions/workflows/ci.yml/badge.svg)](https://github.com/eliranb85/mongo-lease/actions/workflows/ci.yml)

Leader election, lease locks and a deduplicating priority job queue, built on plain MongoDB.

If your app already runs on MongoDB and you need "only one instance runs the scheduler", "never two jobs on the same account at once" and "a queue that survives restarts", you don't have to add Redis for it. This library does all three with atomic single-document operations and a few indexes.

It is extracted from the scheduler of [Pine Cone](https://pine-cone.studio/portfolio/#pine-cone), a production SaaS that runs long browser automations (up to ~15 minutes each) for many users and accounts. There, two runs touching the same social account at the same time could get the account blocked, and several app instances each ticking the scheduler meant duplicated actions. The production code was coupled to the app (Mongoose models, NextAuth, app routes). This is the generic core, rewritten against the official `mongodb` driver and hardened along the way (see [Differences from the original](#differences-from-the-original)).

```
            ┌──────────────┐  tryAcquire()   ┌────────────────────┐
 instance A │ LeaderElector├────────────────►│ leader_leases      │ one doc per role, TTL
 instance B │  (each tick) │                 └────────────────────┘
 instance C └──────┬───────┘
                   │ leader only: enqueue()
                   ▼
            ┌──────────────┐  claimNext()    ┌────────────────────┐
            │ PriorityQueue├────────────────►│ lease_jobs         │ dedupe + per-key concurrency
            └──────┬───────┘                 └────────────────────┘
                   │ every instance is a worker
                   ▼
            ┌──────────────┐  acquire()      ┌────────────────────┐
            │  LeaseLock   ├────────────────►│ lease_locks        │ one doc per key, token-fenced
            └──────────────┘                 └────────────────────┘
```

## Install

```bash
npm install mongo-lease mongodb
```

Requires Node 20+ and MongoDB 6+. `mongodb` (v6 or later) is a peer dependency.

> Not published to npm yet. Until then, install from GitHub: `npm install github:eliranb85/mongo-lease`.

## Usage

### Leader election

```ts
import { MongoClient } from "mongodb";
import { LeaderElector } from "mongo-lease";

const db = (await new MongoClient(process.env.MONGODB_URI!).connect()).db("app");
const leader = new LeaderElector(db, { name: "scheduler", ttlMs: 90_000 });
await leader.ensureIndexes();

setInterval(async () => {
  if (!(await leader.tryAcquire())) return; // someone else is leader
  await runSchedulerTick();
}, 30_000);

process.on("SIGTERM", () => leader.release()); // hand over immediately instead of waiting for the TTL
```

Call `tryAcquire()` more often than `ttlMs`. It renews this instance's lease, or takes over a lease that expired because its holder died.

### Lease locks

```ts
import { LeaseLock } from "mongo-lease";

const locks = new LeaseLock(db, { ttlMs: 15 * 60_000 });
await locks.ensureIndexes();

const out = await locks.withLock(`account:${accountId}`, () => runBrowserAutomation(accountId), {
  owner: instanceId,
});
if (!out.ran) console.log("busy or unavailable:", out.result);
```

Or by hand, for long work that needs to extend its lease:

```ts
const res = await locks.acquire("account:42");
if (res.ok) {
  try {
    await step1();
    await locks.extend("account:42", res.token); // false means the lease was lost: stop
    await step2();
  } finally {
    await locks.release("account:42", res.token);
  }
}
```

### Priority queue

```ts
import { PriorityQueue } from "mongo-lease";

const queue = new PriorityQueue<{ accountId: string }>(db, { backoffMs: [30_000, 60_000], maxAttempts: 3 });
await queue.ensureIndexes();

// Producer: the scheduler and the UI may both trigger the same job. They collapse into one.
await queue.enqueue({ dedupeKey: `sync:${accountId}`, concurrencyKey: `account:${accountId}`, payload: { accountId } });
await queue.enqueue({ dedupeKey: `sync:${accountId}`, payload: { accountId }, priority: "high" }); // upgrades it

// Worker
const claimed = await queue.claimNext();
if (claimed) {
  const { job, executionId } = claimed;
  try {
    await sync(job.payload.accountId);
    await queue.complete(job._id, executionId);
  } catch (err) {
    await queue.fail(job._id, executionId, String(err)); // retries with backoff, then "failed"
  }
}

// Housekeeping, e.g. once a minute
await queue.reapStuck(16 * 60_000);
```

## How it works, and why

| Guarantee | Mechanism |
| --- | --- |
| One leader per role | A single document per role. One `findOneAndUpdate` with `upsert` matches only if *I* hold the lease or it has expired. If someone else holds a live lease, the upsert collides on `_id` and the duplicate-key error means "not leader". No read-then-write race. |
| One holder per lock key | Same pattern on `lease_locks`: `_id` is the key, so MongoDB's primary index is the mutex. An expired lease is taken over in the same atomic update. There's no need to wait up to a minute for the TTL monitor. |
| A late holder can't release someone else's lock | Each lease gets a random token. `release` and `extend` filter on it. |
| Fail closed | If the database errors during `acquire`, you get `ok: false` and the work doesn't run. Running unprotected during a replica-set blip is worse than skipping a tick. |
| No duplicate pending jobs | Partial unique index on `dedupeKey` where `status: "pending"`. Concurrent enqueues race to insert; the loser retries and finds the winner. |
| High priority can jump the line | Priority is stored as a number and upgraded with `$min`, so a duplicate enqueue can raise priority but never lower it. |
| Never two running jobs on one key | Partial unique index on `concurrencyKey` where `status: "running"`. Two workers claiming at the same instant can't both flip a same-key job to running. The loser gets a duplicate-key error and moves to the next candidate. |
| Retries don't overwrite each other | Each claim gets an `executionId`. `complete` and `fail` only apply to the current one, so a slow response from attempt 1 can't overwrite attempt 2. |
| Crashed workers don't block forever | `reapStuck()` fails jobs that have been running too long (with retry), and `sweepOrphans()` clears locks held past a cutoff. Finished jobs are deleted by a TTL index. |

### Trade-offs

- Lease-based, not consensus-based: correctness depends on clocks being roughly in sync and on `ttlMs` being longer than a GC pause or network hiccup. For "only one instance runs the cron" that's the right trade. If you need strict fencing for writes to external systems, also pass the lock token to the downstream system.
- `claimNext()` scans the first 50 available candidates (`scan` option). It favours simplicity over throughput; it's built for hundreds of long jobs, not millions of tiny ones.
- Deduplication applies to *pending* jobs. A request that arrives while the same job is running is queued to run after it, on purpose: its data may be newer.

## Differences from the original

The production version in Pine Cone works, but extracting it was a chance to fix a few things:

- **Token-fenced release.** The original released a lock by key alone, so a holder whose lease had expired could delete the next holder's lock. Releases now require the token.
- **Atomic takeover of expired locks.** The original relied on MongoDB's TTL monitor, which runs about once a minute, so an expired lock could block work for up to 60 extra seconds. Acquire now takes over an expired lease in the same update.
- **`extend()`** for long-running holders.
- **Dedup is enforced by an index.** The original deduplicated with a find-then-insert, plus a cleanup pass that removed duplicates after the fact. A partial unique index now makes concurrent enqueues collapse at write time.
- **No framework coupling.** The library uses the plain `mongodb` driver instead of Mongoose, with no app models, auth or HTTP dispatch.

## Development

```bash
npm install
npm test            # starts an in-memory mongod automatically
npm run check       # typecheck + tests + build
```

To test against a real MongoDB instead:

```bash
docker compose up -d mongo
MONGODB_URI=mongodb://localhost:27017 npm test
```

### See it run with several instances

```bash
docker compose up --build --scale worker=3
```

Exactly one container logs `LEADER`, and the `DONE` lines come from all three. Stop the leader's container and another takes over within the lease TTL. CI runs this as a smoke test on every push.

CI also runs the test suite against MongoDB 6.0 and 7.0 on Node 20, 22 and 24.

## License

MIT
