import { ViewMessageV1, type RegisteredEvent } from "@famashines/ai-systems-contracts";
import {
  ConcurrencyError,
  createEventStore,
  eventStoreMigration,
  type EventStore,
} from "@famashines/ai-systems-kit/event-store";
import { UnavailableError } from "@famashines/ai-systems-kit/http";
import type { Nats } from "@famashines/ai-systems-kit/messaging";
import { processedMigration } from "@famashines/ai-systems-kit/messaging";
import type { Migration } from "@famashines/ai-systems-kit/mongo";
import type { ClientSession, Collection, Db, Filter } from "mongodb";
import { z } from "zod";
import type { TaskListQuery } from "./query.ts";
import { project, taskIdOf, type TaskDoc } from "./tasks.ts";

export const tasksMigration: Migration = {
  name: "001-tasks",
  up: async (db) => {
    await db.collection("tasks").createIndex({ createdAt: -1, _id: -1 }, undefined, undefined);
  },
};

export const migrations = [eventStoreMigration, processedMigration, tasksMigration];

export type NoticeStore = EventStore<ViewMessageV1>;

export const createNoticeStore = (db: Db) =>
  createEventStore<ViewMessageV1>(db, { parse: (input) => ViewMessageV1.parse(input) });

export const tasksOf = (db: Db): Collection<TaskDoc> => db.collection<TaskDoc>("tasks");

export function createProjector(db: Db, store: NoticeStore) {
  const tasks = tasksOf(db);
  return async (
    event: RegisteredEvent,
    markProcessed?: (session: ClientSession) => Promise<void>,
  ): Promise<void> => {
    const current = await tasks.findOne({ _id: taskIdOf(event) });
    const change = project(current, event);
    if (!change) return;
    await store.append({
      streamId: `tasks-${change.doc._id}`,
      events: [change.notice],
      also: async (session) => {
        if (current) {
          const result = await tasks.replaceOne(
            { _id: current._id, rev: current.rev },
            change.doc,
            {
              session,
            },
          );
          if (result.matchedCount !== 1) throw new ConcurrencyError();
        } else {
          await tasks.insertOne(change.doc, { session });
        }
        await markProcessed?.(session);
      },
    });
  };
}

const pageFilter = ({ before, beforeId }: TaskListQuery): Filter<TaskDoc> => {
  if (before === undefined) return {};
  const at = new Date(before);
  if (beforeId === undefined) return { createdAt: { $lt: at } };
  return { $or: [{ createdAt: { $lt: at } }, { createdAt: at, _id: { $lt: beforeId } }] };
};

export const listTasks = (db: Db, query: TaskListQuery): Promise<TaskDoc[]> =>
  tasksOf(db).find(pageFilter(query)).sort({ createdAt: -1, _id: -1 }).limit(query.limit).toArray();

const StreamInfo = z.object({ state: z.object({ last_seq: z.int().nonnegative() }) });

export async function viewsHeadSeq(nats: Nats): Promise<number> {
  try {
    const reply = await nats.connection.request("$JS.API.STREAM.INFO.VIEWS", "", {
      timeout: 2000,
    });
    return StreamInfo.parse(reply.json()).state.last_seq;
  } catch {
    throw new UnavailableError("The VIEWS stream does not answer");
  }
}
