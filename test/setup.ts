import { MongoClient, type Db } from "mongodb";
import type { MongoMemoryServer } from "mongodb-memory-server";

/**
 * Uses MONGODB_URI when set (Docker Compose or CI service container),
 * otherwise starts an in-memory mongod for the test run.
 */
export async function startMongo(): Promise<{ db: Db; stop: () => Promise<void> }> {
  let server: MongoMemoryServer | undefined;
  let uri = process.env.MONGODB_URI;
  if (!uri) {
    const { MongoMemoryServer } = await import("mongodb-memory-server");
    server = await MongoMemoryServer.create();
    uri = server.getUri();
  }
  const client = await new MongoClient(uri).connect();
  const db = client.db(`mongo_lease_test_${process.pid}_${Date.now()}`);
  return {
    db,
    stop: async () => {
      await db.dropDatabase().catch(() => undefined);
      await client.close();
      await server?.stop();
    },
  };
}

/** A controllable clock for lease-expiry tests. */
export function fakeClock(start = Date.parse("2026-01-01T00:00:00Z")) {
  let t = start;
  return {
    now: () => new Date(t),
    advance: (ms: number) => {
      t += ms;
    },
  };
}
