import type { Collection, Db } from "mongodb";
import { randomUUID } from "node:crypto";
import { isDuplicateKeyError } from "./errors.js";

interface LeaderDoc {
  _id: string;
  instanceId: string;
  renewedAt: Date;
  expiresAt: Date;
}

export interface LeaderElectorOptions {
  /** Name of the role being elected, e.g. "scheduler". One leader per name. */
  name: string;
  /** How long a lease stays valid without renewal. Default 90s. */
  ttlMs?: number;
  /** Collection that stores leases. Default "leader_leases". */
  collection?: string;
  /** Override the instance id (useful in tests). */
  instanceId?: string;
  /** Clock override (useful in tests). */
  now?: () => Date;
}

/**
 * Lease-based leader election on a single MongoDB document.
 *
 * One atomic findOneAndUpdate either renews this instance's own lease or takes
 * over an expired one. If another instance holds a live lease, the filter does
 * not match, the upsert collides on `_id`, and the duplicate-key error simply
 * means "not leader".
 */
export class LeaderElector {
  readonly instanceId: string;
  private readonly col: Collection<LeaderDoc>;
  private readonly name: string;
  private readonly ttlMs: number;
  private readonly now: () => Date;

  constructor(db: Db, opts: LeaderElectorOptions) {
    this.name = opts.name;
    this.ttlMs = opts.ttlMs ?? 90_000;
    this.col = db.collection<LeaderDoc>(opts.collection ?? "leader_leases");
    this.instanceId = opts.instanceId ?? `${process.pid}-${randomUUID().slice(0, 8)}`;
    this.now = opts.now ?? (() => new Date());
  }

  /** Creates the TTL index that garbage-collects abandoned leases. Idempotent. */
  async ensureIndexes(): Promise<void> {
    await this.col.createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 });
  }

  /** Becomes (or stays) leader. Returns true if this instance holds the lease. */
  async tryAcquire(): Promise<boolean> {
    const now = this.now();
    try {
      const doc = await this.col.findOneAndUpdate(
        { _id: this.name, $or: [{ instanceId: this.instanceId }, { expiresAt: { $lte: now } }] },
        { $set: { instanceId: this.instanceId, renewedAt: now, expiresAt: new Date(now.getTime() + this.ttlMs) } },
        { upsert: true, returnDocument: "after" },
      );
      return doc?.instanceId === this.instanceId;
    } catch (err) {
      if (isDuplicateKeyError(err)) return false;
      throw err;
    }
  }

  /** Current lease holder, or null if nobody holds a live lease. */
  async currentLeader(): Promise<string | null> {
    const doc = await this.col.findOne({ _id: this.name, expiresAt: { $gt: this.now() } });
    return doc?.instanceId ?? null;
  }

  /** Gives up the lease, if this instance holds it. For graceful shutdown. */
  async release(): Promise<void> {
    await this.col.deleteOne({ _id: this.name, instanceId: this.instanceId });
  }
}
