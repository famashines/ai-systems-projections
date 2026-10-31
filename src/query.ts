import { TaskListResultV1, Timestamp } from "@famashines/ai-systems-contracts";
import { validate, type Route } from "@famashines/ai-systems-kit/http";
import { z } from "zod";
import { toItem, type TaskDoc } from "./tasks.ts";

export const TaskListQuery = z.object({
  limit: z.coerce.number().int().min(1).max(100).default(50),
  before: Timestamp.optional(),
});
export type TaskListQuery = z.infer<typeof TaskListQuery>;

export interface TaskQueries {
  headSeq: () => Promise<number>;
  list: (query: TaskListQuery) => Promise<TaskDoc[]>;
}

export const taskRoutes = (queries: TaskQueries): Route[] => [
  {
    method: "GET",
    path: "/internal/tasks",
    handler: async (request) => {
      const { query } = await validate(request, { query: TaskListQuery });
      const headSeq = await queries.headSeq();
      const docs = await queries.list(query);
      return { body: TaskListResultV1.parse({ items: docs.map(toItem), headSeq }) };
    },
  },
];
