import type { Collection, Db } from "mongodb";
import { randomUUID } from "node:crypto";
import { isDuplicateKeyError } from "./errors.js";

interface LockDoc {
  _id: string;
  token: string;
  owner: string | null;
  lockedAt: Date;
  expiresAt: Date;
}

export interface LeaseLockOptions {
  /** Default lease duration. Default 15 minutes. */
  ttlMs?: number;
  /** Collection that stores locks. Default "lease_locks". */
  collection?: string;
  /** Clock override (useful in tests). */
  now?: () => Date;
}

export interface AcquireOptions {
  ttlMs?: number;
  /** Free-form label of who holds the lock, shown by `inspect()`. */
  owner?: string;
}

export type AcquireResult =
  | { ok: true; token: string; expiresAt: Date }
  | { ok: false; reason: "held"; heldBy: string | null; lockedAt: Date; expiresAt: Date }
  | { ok: false; reason: "error"; error: unknown };

/**
 * Mutual exclusion per key, stored as lease documents with a unique `_id`.
 *
 * - Acquire is one atomic upsert: it creates the lock, or takes it over if the
 *   previous lease has expired (no waiting for MongoDB's TTL monitor).
 * - Every lease gets a random token. Release and extend only work with the
 *   token, so a slow holder whose lease expired cannot release the new holder's lock.
 * - Unexpected database errors fail closed: the caller gets `ok: false` and the
 *   protected work does not run.
 */
export class LeaseLock {
  private readonly col: Collection<LockDoc>;
  private readonly ttlMs: number;
  private readonly now: () => Date;

  constructor(db: Db, opts: LeaseLockOptions = {}) {
    this.col = db.collection<LockDoc>(opts.collection ?? "lease_locks");
    this.ttlMs = opts.ttlMs ?? 15 * 60_000;
    this.now = opts.now ?? (() => new Date());
  }

  async ensureIndexes(): Promise<void> {
    await this.col.createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 });
  }

  async acquire(key: string, opts: AcquireOptions = {}): Promise<AcquireResult> {
    const now = this.now();
    const token = randomUUID();
    const expiresAt = new Date(now.getTime() + (opts.ttlMs ?? this.ttlMs));
    try {
      const doc = await this.col.findOneAndUpdate(
        { _id: key, expiresAt: { $lte: now } },
        { $set: { token, owner: opts.owner ?? null, lockedAt: now, expiresAt } },
        { upsert: true, returnDocument: "after" },
      );
      if (doc?.token === token) return { ok: true, token, expiresAt };
    } catch (err) {
      if (!isDuplicateKeyError(err)) return { ok: false, reason: "error", error: err };
    }
    try {
      const held = await this.col.findOne({ _id: key });
      if (!held) return { ok: false, reason: "held", heldBy: null, lockedAt: now, expiresAt: now };
      return { ok: false, reason: "held", heldBy: held.owner, lockedAt: held.lockedAt, expiresAt: held.expiresAt };
    } catch (err) {
      return { ok: false, reason: "error", error: err };
    }
  }

  /** Releases the lock only if `token` still owns it. Returns true if released. */
  async release(key: string, token: string): Promise<boolean> {
    const res = await this.col.deleteOne({ _id: key, token });
    return res.deletedCount === 1;
  }

  /** Pushes the expiry forward for a long-running holder. Returns false if the lease was lost. */
  async extend(key: string, token: string, ttlMs = this.ttlMs): Promise<boolean> {
    const now = this.now();
    const res = await this.col.updateOne(
      { _id: key, token, expiresAt: { $gt: now } },
      { $set: { expiresAt: new Date(now.getTime() + ttlMs) } },
    );
    return res.modifiedCount === 1;
  }

  /** Runs `fn` while holding the lock. Returns `{ ran: false }` if the lock was not acquired. */
  async withLock<T>(
    key: string,
    fn: () => Promise<T>,
    opts: AcquireOptions = {},
  ): Promise<{ ran: true; value: T } | { ran: false; result: Exclude<AcquireResult, { ok: true }> }> {
    const res = await this.acquire(key, opts);
    if (!res.ok) return { ran: false, result: res };
    try {
      return { ran: true, value: await fn() };
    } finally {
      await this.release(key, res.token).catch(() => undefined);
    }
  }

  /** Deletes locks held longer than `maxAgeMs`: holders that died without releasing. */
  async sweepOrphans(maxAgeMs: number): Promise<number> {
    const cutoff = new Date(this.now().getTime() - maxAgeMs);
    const res = await this.col.deleteMany({ lockedAt: { $lte: cutoff } });
    return res.deletedCount;
  }

  async inspect(key: string): Promise<{ owner: string | null; lockedAt: Date; expiresAt: Date } | null> {
    const doc = await this.col.findOne({ _id: key, expiresAt: { $gt: this.now() } });
    return doc ? { owner: doc.owner, lockedAt: doc.lockedAt, expiresAt: doc.expiresAt } : null;
  }
}
