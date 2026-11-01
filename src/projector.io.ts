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
import type { ClientSession, Collection, Db, Filter, OptionalUnlessRequiredId } from "mongodb";
import { z } from "zod";
import { agentIdOf, projectAgent, type AgentDoc } from "./agents.ts";
import {
  bookMonth,
  capStreamId,
  emptyMonth,
  monthOf,
  setCap,
  type BudgetChange,
  type CapDoc,
  type MonthDoc,
} from "./budget.ts";
import {
  bookCost,
  linkTask,
  postMessage,
  type ChatChange,
  type CostDoc,
  type LinkDoc,
  type MessageDoc,
  type Patch,
} from "./chat.ts";
import type { Notice } from "./notices.ts";
import type { ListQuery } from "./query.ts";
import { projectDetail, type DetailDoc, type OutputDoc } from "./task-detail.ts";
import { project, taskIdOf, type TaskDoc } from "./tasks.ts";

export const tasksMigration: Migration = {
  name: "001-tasks",
  up: async (db) => {
    await db.collection("tasks").createIndex({ createdAt: -1, _id: -1 }, undefined, undefined);
  },
};

export const readModelsMigration: Migration = {
  name: "002-chat-budget-agents",
  up: async (db) => {
    const chat = db.collection("chat");
    await chat.createIndex({ conversationId: 1, createdAt: -1, _id: -1 }, undefined, undefined);
    await chat.createIndex({ taskId: 1 }, undefined, undefined);
    await db.collection("agents").createIndex({ name: 1 }, undefined, undefined);
  },
};

export const readModelMigrations = [tasksMigration, readModelsMigration];
export const migrations = [eventStoreMigration, processedMigration, ...readModelMigrations];

export const readModelCollections = [
  "tasks",
  "chat",
  "chat_links",
  "chat_costs",
  "budget_month",
  "budget_cap",
  "agents",
  "task_details",
  "task_outputs",
];

export type NoticeStore = EventStore<ViewMessageV1>;

export const createNoticeStore = (db: Db) =>
  createEventStore<ViewMessageV1>(db, { parse: (input) => ViewMessageV1.parse(input) });

export const collections = (db: Db) => ({
  tasks: db.collection<TaskDoc>("tasks"),
  chat: db.collection<MessageDoc>("chat"),
  links: db.collection<LinkDoc>("chat_links"),
  costs: db.collection<CostDoc>("chat_costs"),
  months: db.collection<MonthDoc>("budget_month"),
  caps: db.collection<CapDoc>("budget_cap"),
  agents: db.collection<AgentDoc>("agents"),
  details: db.collection<DetailDoc>("task_details"),
  outputs: db.collection<OutputDoc>("task_outputs"),
});

export const tasksOf = (db: Db): Collection<TaskDoc> => collections(db).tasks;

async function put<D extends { _id: string; rev: number }>(
  collection: Collection<D>,
  patch: Patch<D> | undefined,
  session: ClientSession,
): Promise<void> {
  if (!patch) return;
  if (patch.before) {
    const filter = { _id: patch.before._id, rev: patch.before.rev } as Filter<D>;
    const result = await collection.replaceOne(filter, patch.after, { session });
    if (result.matchedCount !== 1) throw new ConcurrencyError();
  } else {
    await collection.insertOne(patch.after as OptionalUnlessRequiredId<D>, { session });
  }
}

interface Plan {
  notices: Notice[];
  writes: (session: ClientSession) => Promise<void>;
}

type Models = ReturnType<typeof collections>;

const chatPlan = (models: Models, change: ChatChange): Plan => ({
  notices: change.notices,
  writes: async (session) => {
    await put(models.links, change.link, session);
    await put(models.costs, change.cost, session);
    for (const patch of change.messages) await put(models.chat, patch, session);
  },
});

const budgetPlan = (models: Models, change: BudgetChange): Plan => ({
  notices: change.notices,
  writes: async (session) => {
    await put(models.caps, change.cap, session);
    for (const patch of change.months) await put(models.months, patch, session);
  },
});

async function orchestrationPlan(
  models: Models,
  event: RegisteredEvent,
): Promise<Plan | undefined> {
  const taskId = taskIdOf(event);
  const current = taskId === undefined ? null : await models.tasks.findOne({ _id: taskId });
  const task = taskId === undefined ? undefined : project(current, event);
  const currentDetail = taskId === undefined ? null : await models.details.findOne({ _id: taskId });
  const detail = taskId === undefined ? undefined : projectDetail(currentDetail, event);
  const messageId =
    event.type === "orchestration.TaskSubmitted" ? event.payload.messageId : undefined;
  const chat =
    messageId === undefined
      ? undefined
      : linkTask(
          await models.links.findOne({ _id: messageId }),
          await models.chat.findOne({ _id: messageId }),
          event,
        );
  if (!task && !detail && !chat) return undefined;
  const linked = chat && chatPlan(models, chat);
  return {
    notices: [
      ...(task ? [task.notice] : []),
      ...(detail ? [detail.notice] : []),
      ...(linked?.notices ?? []),
    ],
    writes: async (session) => {
      if (task) await put(models.tasks, { before: current, after: task.doc }, session);
      if (detail) {
        await put(models.details, { before: currentDetail, after: detail.doc }, session);
        for (const output of detail.outputs) {
          await models.outputs.replaceOne({ _id: output._id }, output, { upsert: true, session });
        }
      }
      await linked?.writes(session);
    },
  };
}

async function conversationPlan(models: Models, event: RegisteredEvent): Promise<Plan | undefined> {
  if (event.type === "conversation.OwnerMessagePosted") {
    const { messageId, intent } = event.payload;
    const link = intent === "new_task" ? await models.links.findOne({ _id: messageId }) : null;
    const change = postMessage(await models.chat.findOne({ _id: messageId }), link, null, event);
    return change && chatPlan(models, change);
  }
  if (event.type === "conversation.AgentReplyPosted") {
    const { messageId, taskId, runId } = event.payload;
    const cost = runId === undefined ? null : await models.costs.findOne({ _id: taskId });
    const change = postMessage(await models.chat.findOne({ _id: messageId }), null, cost, event);
    return change && chatPlan(models, change);
  }
  return undefined;
}

async function llmPlan(models: Models, event: RegisteredEvent): Promise<Plan | undefined> {
  if (event.type !== "llm.CallCompleted" && event.type !== "llm.CallFailed") return undefined;
  const { taskId } = event.payload;
  const replies = await models.chat.find({ taskId, role: "agent" }).toArray();
  const change = bookCost(await models.costs.findOne({ _id: taskId }), replies, event);
  return change && chatPlan(models, change);
}

async function budgetEventPlan(
  models: Models,
  event: RegisteredEvent,
  timeZone: string,
): Promise<Plan | undefined> {
  const cap = await models.caps.findOne({ _id: "month" });
  if (event.streamId === capStreamId) {
    const months = await models.months.find().toArray();
    const change = setCap(cap, months, event, timeZone);
    return change && budgetPlan(models, change);
  }
  const month = monthOf(event.streamId);
  if (month === undefined) return undefined;
  const change = bookMonth(await models.months.findOne({ _id: month }), cap, event);
  return change && budgetPlan(models, change);
}

async function agentPlan(models: Models, event: RegisteredEvent): Promise<Plan | undefined> {
  const agentId = agentIdOf(event);
  if (agentId === undefined) return undefined;
  const current = await models.agents.findOne({ _id: agentId });
  const change = projectAgent(current, event);
  return (
    change && {
      notices: [change.notice],
      writes: (session) => put(models.agents, { before: current, after: change.doc }, session),
    }
  );
}

export function createProjector(db: Db, store: NoticeStore, timeZone: string) {
  const models = collections(db);
  const planOf = (event: RegisteredEvent): Promise<Plan | undefined> => {
    const context = event.type.slice(0, event.type.indexOf("."));
    switch (context) {
      case "orchestration":
        return orchestrationPlan(models, event);
      case "conversation":
        return conversationPlan(models, event);
      case "llm":
        return llmPlan(models, event);
      case "budget":
        return budgetEventPlan(models, event, timeZone);
      case "agents":
        return agentPlan(models, event);
      default:
        return Promise.resolve(undefined);
    }
  };
  return async (
    event: RegisteredEvent,
    markProcessed?: (session: ClientSession) => Promise<void>,
  ): Promise<void> => {
    const plan = await planOf(event);
    if (!plan) return;
    await store.append({
      streamId: `notice-${event.eventId}`,
      events: plan.notices,
      also: async (session) => {
        await plan.writes(session);
        await markProcessed?.(session);
      },
    });
  };
}

const pageFilter = ({ before, beforeId }: ListQuery) => {
  if (before === undefined) return {};
  const at = new Date(before);
  if (beforeId === undefined) return { createdAt: { $lt: at } };
  return { $or: [{ createdAt: { $lt: at } }, { createdAt: at, _id: { $lt: beforeId } }] };
};

const newestFirst = { createdAt: -1, _id: -1 } as const;

export const listTasks = (db: Db, query: ListQuery): Promise<TaskDoc[]> =>
  tasksOf(db).find(pageFilter(query)).sort(newestFirst).limit(query.limit).toArray();

export const listMessages = (
  db: Db,
  conversationId: string,
  query: ListQuery,
): Promise<MessageDoc[]> =>
  collections(db)
    .chat.find({ conversationId, ...pageFilter(query) })
    .sort(newestFirst)
    .limit(query.limit)
    .toArray();

export async function readMonth(db: Db, month: string): Promise<MonthDoc | null> {
  const { months, caps } = collections(db);
  const doc = await months.findOne({ _id: month });
  if (doc) return doc;
  const cap = await caps.findOne({ _id: "month" });
  return cap && emptyMonth(month, cap);
}

export const readDetail = (db: Db, taskId: string): Promise<DetailDoc | null> =>
  collections(db).details.findOne({ _id: taskId });

export const readOutput = (db: Db, id: string): Promise<OutputDoc | null> =>
  collections(db).outputs.findOne({ _id: id });

export const listAgents = (db: Db): Promise<AgentDoc[]> =>
  collections(db).agents.find().sort({ name: 1, _id: 1 }).toArray();

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
