import { ObjectId, type Collection, type Db } from "mongodb";
import { randomUUID } from "node:crypto";
import { isDuplicateKeyError } from "./errors.js";

export type JobStatus = "pending" | "running" | "done" | "failed";
export type Priority = "high" | "normal";

const PRIORITY_RANK: Record<Priority, number> = { high: 0, normal: 1 };

export interface JobDoc<P = Record<string, unknown>> {
  _id: ObjectId;
  /** Jobs with the same dedupeKey collapse into one while pending. */
  dedupeKey: string;
  /** At most one job per concurrencyKey can be running at a time. */
  concurrencyKey: string;
  status: JobStatus;
  /** 0 = high, 1 = normal. Numeric so it sorts and can only be upgraded with $min. */
  priority: number;
  payload: P;
  enqueuedAt: Date;
  availableAt: Date;
  attempts: number;
  maxAttempts: number;
  executionId: string | null;
  startedAt: Date | null;
  completedAt: Date | null;
  error: string | null;
  result: unknown;
}

export interface PriorityQueueOptions {
  collection?: string;
  /** Retry delays in ms, indexed by attempt number. Default [30s, 60s]. */
  backoffMs?: number[];
  /** Total attempts including the first one. Default 3. */
  maxAttempts?: number;
  /** How long finished jobs are kept before MongoDB deletes them. Default 1h. */
  retainFinishedMs?: number;
  now?: () => Date;
}

export interface EnqueueInput<P> {
  dedupeKey: string;
  /** Defaults to dedupeKey. */
  concurrencyKey?: string;
  payload: P;
  priority?: Priority;
  /** Do not run before this time. */
  delayMs?: number;
}

export interface Claimed<P> {
  job: JobDoc<P>;
  /** Pass back to complete()/fail(). A late answer from an older attempt is ignored. */
  executionId: string;
}

/**
 * A MongoDB-backed job queue with deduplication, priorities, per-key
 * concurrency, retries with backoff, and a reaper for stuck jobs.
 *
 * Two partial unique indexes carry the guarantees:
 * - `dedupeKey` where status = "pending": concurrent enqueues of the same job collapse into one.
 * - `concurrencyKey` where status = "running": two workers can never run the same key,
 *   even if they claim at the same moment.
 */
export class PriorityQueue<P = Record<string, unknown>> {
  private readonly col: Collection<JobDoc<P>>;
  private readonly backoffMs: number[];
  private readonly maxAttempts: number;
  private readonly retainFinishedMs: number;
  private readonly now: () => Date;

  constructor(db: Db, opts: PriorityQueueOptions = {}) {
    this.col = db.collection<JobDoc<P>>(opts.collection ?? "lease_jobs");
    this.backoffMs = opts.backoffMs ?? [30_000, 60_000];
    this.maxAttempts = opts.maxAttempts ?? 3;
    this.retainFinishedMs = opts.retainFinishedMs ?? 3_600_000;
    this.now = opts.now ?? (() => new Date());
  }

  async ensureIndexes(): Promise<void> {
    await Promise.all([
      this.col.createIndex(
        { dedupeKey: 1 },
        { unique: true, partialFilterExpression: { status: "pending" }, name: "uniq_pending_dedupe" },
      ),
      this.col.createIndex(
        { concurrencyKey: 1 },
        { unique: true, partialFilterExpression: { status: "running" }, name: "uniq_running_key" },
      ),
      this.col.createIndex({ status: 1, priority: 1, enqueuedAt: 1 }),
      this.col.createIndex(
        { completedAt: 1 },
        { expireAfterSeconds: Math.round(this.retainFinishedMs / 1000), partialFilterExpression: { completedAt: { $type: "date" } } },
      ),
    ]);
  }

  /**
   * Adds a job, or returns the pending job with the same dedupeKey.
   * A high-priority request upgrades an existing normal-priority job.
   */
  async enqueue(input: EnqueueInput<P>): Promise<{ id: ObjectId; deduped: boolean }> {
    const now = this.now();
    const rank = PRIORITY_RANK[input.priority ?? "normal"];
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const res = await this.col.findOneAndUpdate(
          { dedupeKey: input.dedupeKey, status: "pending" },
          {
            $min: { priority: rank },
            $setOnInsert: {
              concurrencyKey: input.concurrencyKey ?? input.dedupeKey,
              payload: input.payload,
              enqueuedAt: now,
              availableAt: new Date(now.getTime() + (input.delayMs ?? 0)),
              attempts: 0,
              maxAttempts: this.maxAttempts,
              executionId: null,
              startedAt: null,
              completedAt: null,
              error: null,
              result: null,
            } as Partial<JobDoc<P>>,
          },
          { upsert: true, returnDocument: "after", includeResultMetadata: true },
        );
        const doc = res.value!;
        return { id: doc._id, deduped: Boolean(res.lastErrorObject?.updatedExisting) };
      } catch (err) {
        // Two enqueues raced to insert the same pending job; the retry finds the winner.
        if (!isDuplicateKeyError(err)) throw err;
      }
    }
    throw new Error(`enqueue: could not settle dedupeKey ${input.dedupeKey}`);
  }

  /**
   * Atomically moves the best available job to "running".
   * Order: priority, then FIFO. Jobs whose concurrencyKey is already running are skipped.
   */
  async claimNext(opts: { scan?: number } = {}): Promise<Claimed<P> | null> {
    const now = this.now();
    const candidates = await this.col
      .find({ status: "pending", availableAt: { $lte: now } }, { projection: { _id: 1 } })
      .sort({ priority: 1, enqueuedAt: 1 })
      .limit(opts.scan ?? 50)
      .toArray();

    for (const { _id } of candidates) {
      const executionId = randomUUID();
      try {
        const job = await this.col.findOneAndUpdate(
          { _id, status: "pending" },
          { $set: { status: "running", executionId, startedAt: now } },
          { returnDocument: "after" },
        );
        if (job) return { job, executionId };
      } catch (err) {
        if (isDuplicateKeyError(err)) continue; // same concurrencyKey is already running
        throw err;
      }
    }
    return null;
  }

  /** Marks a running job done. Ignored (returns false) if executionId is stale. */
  async complete(id: ObjectId, executionId: string, result: unknown = null): Promise<boolean> {
    const res = await this.col.updateOne(
      { _id: id, status: "running", executionId },
      { $set: { status: "done", completedAt: this.now(), result, error: null } },
    );
    return res.modifiedCount === 1;
  }

  /**
   * Records a failure. Schedules a retry with backoff while attempts remain,
   * otherwise marks the job failed. Ignored if executionId is stale.
   */
  async fail(id: ObjectId, executionId: string, error: string): Promise<"retry" | "failed" | "stale"> {
    const job = await this.col.findOne({ _id: id, status: "running", executionId });
    if (!job) return "stale";
    const now = this.now();
    const attempts = job.attempts + 1;

    if (attempts < job.maxAttempts) {
      const delay = this.backoffMs[Math.min(attempts - 1, this.backoffMs.length - 1)] ?? 0;
      try {
        const res = await this.col.updateOne(
          { _id: id, status: "running", executionId },
          { $set: { status: "pending", attempts, error, executionId: null, startedAt: null, availableAt: new Date(now.getTime() + delay) } },
        );
        if (res.modifiedCount === 1) return "retry";
        return "stale";
      } catch (err) {
        // A newer request for the same dedupeKey is already pending: that one will run instead.
        if (!isDuplicateKeyError(err)) throw err;
      }
    }
    const res = await this.col.updateOne(
      { _id: id, status: "running", executionId },
      { $set: { status: "failed", attempts, error, completedAt: now } },
    );
    return res.modifiedCount === 1 ? "failed" : "stale";
  }

  /** Fails (and possibly retries) jobs that have been running longer than `timeoutMs`. */
  async reapStuck(timeoutMs: number): Promise<number> {
    const cutoff = new Date(this.now().getTime() - timeoutMs);
    const stuck = await this.col.find({ status: "running", startedAt: { $lte: cutoff } }).toArray();
    let reaped = 0;
    for (const job of stuck) {
      if (!job.executionId) continue;
      const outcome = await this.fail(job._id, job.executionId, `timed out after ${timeoutMs}ms`);
      if (outcome !== "stale") reaped++;
    }
    return reaped;
  }

  /** 1-based position among pending jobs, or null if the job is not pending. */
  async position(id: ObjectId): Promise<number | null> {
    const job = await this.col.findOne({ _id: id, status: "pending" });
    if (!job) return null;
    const ahead = await this.col.countDocuments({
      status: "pending",
      $or: [
        { priority: { $lt: job.priority } },
        { priority: job.priority, enqueuedAt: { $lt: job.enqueuedAt } },
      ],
    });
    return ahead + 1;
  }

  async get(id: ObjectId): Promise<JobDoc<P> | null> {
    return this.col.findOne({ _id: id });
  }
}
