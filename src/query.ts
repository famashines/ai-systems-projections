import {
  AgentListResultV1,
  BudgetMonth,
  BudgetMonthV1,
  ConversationThreadResultV1,
  Id,
  TaskListResultV1,
  Timestamp,
} from "@famashines/ai-systems-contracts";
import { NotFoundError, validate, type Route } from "@famashines/ai-systems-kit/http";
import { z } from "zod";
import { toAgent, type AgentDoc } from "./agents.ts";
import { monthKey, toView, type MonthDoc } from "./budget.ts";
import { fitPage, toMessage, type MessageDoc } from "./chat.ts";
import { toItem, type TaskDoc } from "./tasks.ts";

export const ListQuery = z
  .object({
    limit: z.coerce.number().int().min(1).max(100).default(50),
    before: Timestamp.optional(),
    beforeId: Id.optional(),
  })
  .refine((query) => query.beforeId === undefined || query.before !== undefined, {
    message: "beforeId needs before",
    path: ["beforeId"],
  });
export type ListQuery = z.infer<typeof ListQuery>;

const ConversationParams = z.object({ id: Id });
const MonthQuery = z.object({ month: BudgetMonth.optional() });

export interface Queries {
  headSeq: () => Promise<number>;
  tasks: (query: ListQuery) => Promise<TaskDoc[]>;
  messages: (conversationId: string, query: ListQuery) => Promise<MessageDoc[]>;
  month: (month: string) => Promise<MonthDoc | null>;
  agents: () => Promise<AgentDoc[]>;
  timeZone: string;
  now?: () => Date;
}

export const routes = (queries: Queries): Route[] => [
  {
    method: "GET",
    path: "/internal/tasks",
    handler: async (request) => {
      const { query } = await validate(request, { query: ListQuery });
      const headSeq = await queries.headSeq();
      const docs = await queries.tasks(query);
      return { body: TaskListResultV1.parse({ items: docs.map(toItem), headSeq }) };
    },
  },
  {
    method: "GET",
    path: "/internal/conversations/:id/messages",
    handler: async (request) => {
      const { params, query } = await validate(request, {
        params: ConversationParams,
        query: ListQuery,
      });
      const headSeq = await queries.headSeq();
      const docs = await queries.messages(params.id, query);
      const items = fitPage(docs.map(toMessage), headSeq);
      return { body: ConversationThreadResultV1.parse({ items, headSeq }) };
    },
  },
  {
    method: "GET",
    path: "/internal/budget",
    handler: async (request) => {
      const { query } = await validate(request, { query: MonthQuery });
      const month =
        query.month ?? monthKey((queries.now ?? (() => new Date()))(), queries.timeZone);
      const doc = await queries.month(month);
      if (!doc) throw new NotFoundError("No cap is set and the month has no spend");
      return { body: BudgetMonthV1.parse(toView(doc)) };
    },
  },
  {
    method: "GET",
    path: "/internal/agents",
    handler: async () => {
      const headSeq = await queries.headSeq();
      const docs = await queries.agents();
      return { body: AgentListResultV1.parse({ items: docs.map(toAgent), headSeq }) };
    },
  },
];
