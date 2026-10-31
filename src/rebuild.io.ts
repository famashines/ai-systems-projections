import { parseEvent, type RegisteredEvent } from "@famashines/ai-systems-contracts";
import { eventStoreMigration } from "@famashines/ai-systems-kit/event-store";
import { processedMigration } from "@famashines/ai-systems-kit/messaging";
import type { Db } from "mongodb";
import { z } from "zod";
import { createNoticeStore, createProjector, tasksMigration } from "./projector.io.ts";

export type FetchPage = (afterPosition: number, limit: number) => Promise<Positioned[]>;

export interface Positioned {
  event: RegisteredEvent;
  position: number;
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
      event: parseEvent(raw),
      position: raw.position,
    }));
  };

const resetCollections = ["tasks", "events", "counters", "processed"];

export async function rebuild(db: Db, fetchPage: FetchPage, pageSize = 100): Promise<number> {
  for (const name of resetCollections) await db.dropCollection(name);
  for (const migration of [eventStoreMigration, processedMigration, tasksMigration]) {
    await migration.up(db);
  }
  const apply = createProjector(db, createNoticeStore(db));
  let after = 0;
  let count = 0;
  for (;;) {
    const page = await fetchPage(after, pageSize);
    const last = page.at(-1);
    if (!last) return count;
    for (const { event } of page) await apply(event);
    count += page.length;
    after = last.position;
  }
}
