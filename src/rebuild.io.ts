import { setTimeout as sleep } from "node:timers/promises";
import { parseEvent, type RegisteredEvent } from "@famashines/ai-systems-contracts";
import { eventStoreMigration } from "@famashines/ai-systems-kit/event-store";
import { processedMigration, type Nats } from "@famashines/ai-systems-kit/messaging";
import type { Db } from "mongodb";
import { z } from "zod";
import { createNoticeStore, createProjector, tasksMigration } from "./projector.io.ts";

export interface Positioned {
  raw: Record<string, unknown>;
  position: number;
}

export type FetchPage = (afterPosition: number, limit: number) => Promise<Positioned[]>;

export interface RebuildResult {
  applied: number;
  skipped: number;
  skippedTypes: string[];
}

const ReplayPage = z.object({
  events: z.array(z.looseObject({ position: z.int().positive() })),
});

export const replayFrom =
  (baseUrl: string): FetchPage =>
  async (afterPosition, limit) => {
    const url = new URL("/internal/events", baseUrl);
    url.searchParams.set("afterPosition", String(afterPosition));
    url.searchParams.set("limit", String(limit));
    const response = await fetch(url, { signal: AbortSignal.timeout(30_000) });
    if (!response.ok) throw new Error(`The replay API gave ${String(response.status)}`);
    return ReplayPage.parse(await response.json()).events.map((raw) => ({
      raw,
      position: raw.position,
    }));
  };

const ConsumerInfo = z.object({ num_waiting: z.int().nonnegative() });
const ConsumerMissing = z.object({ error: z.object({ err_code: z.literal(10014) }) });

async function waitingPulls(nats: Nats): Promise<number> {
  const reply = await nats.connection.request(
    "$JS.API.CONSUMER.INFO.ORCHESTRATION.projections",
    "",
    { timeout: 5000 },
  );
  const answer: unknown = reply.json();
  if (ConsumerMissing.safeParse(answer).success) return 0;
  return ConsumerInfo.parse(answer).num_waiting;
}

export async function assertServiceStopped(nats: Nats, checkMs = 6000): Promise<void> {
  for (let check = 0; check < 2; check += 1) {
    if (check > 0) await sleep(checkMs);
    if ((await waitingPulls(nats)) > 0) {
      throw new Error(
        "The durable consumer projections has waiting pulls: a projections pod runs. Scale it to 0 first.",
      );
    }
  }
}

const markers = (db: Db) => db.collection<{ _id: string; startedAt: Date }>("rebuild");

export const rebuildRunning = async (db: Db): Promise<boolean> =>
  (await markers(db).findOne({ _id: "running" })) !== null;

const resetCollections = ["tasks", "events", "counters", "processed"];

const typeOf = (raw: Record<string, unknown>): string =>
  typeof raw.type === "string" ? raw.type.slice(0, 100) : "(no type)";

export async function rebuild(
  db: Db,
  fetchPage: FetchPage,
  pageSize = 100,
): Promise<RebuildResult> {
  await markers(db).replaceOne({ _id: "running" }, { startedAt: new Date() }, { upsert: true });
  for (const name of resetCollections) await db.dropCollection(name);
  for (const migration of [eventStoreMigration, processedMigration, tasksMigration]) {
    await migration.up(db);
  }
  const apply = createProjector(db, createNoticeStore(db));
  const result: RebuildResult = { applied: 0, skipped: 0, skippedTypes: [] };
  let after = 0;
  for (;;) {
    const page = await fetchPage(after, pageSize);
    const last = page.at(-1);
    if (!last) break;
    for (const { raw } of page) {
      let event: RegisteredEvent;
      try {
        event = parseEvent(raw);
      } catch {
        result.skipped += 1;
        if (!result.skippedTypes.includes(typeOf(raw))) result.skippedTypes.push(typeOf(raw));
        continue;
      }
      await apply(event);
      result.applied += 1;
    }
    after = last.position;
  }
  await markers(db).deleteOne({ _id: "running" });
  return result;
}
