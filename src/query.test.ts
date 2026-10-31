import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import { after, before, test } from "node:test";
import { TaskListResultV1 } from "@famashines/ai-systems-contracts";
import { createService, type Service } from "@famashines/ai-systems-kit/http";
import { createLogger } from "@famashines/ai-systems-kit/observability";
import { taskRoutes, type TaskListQuery } from "./query.ts";
import type { TaskDoc } from "./tasks.ts";

const doc: TaskDoc = {
  _id: "0b9d3c4e-6f1a-4c2b-9d8e-7a6b5c4d3e2f",
  title: "Plan",
  status: "submitted",
  createdAt: new Date("2026-10-31T09:15:00.000Z"),
  updatedAt: new Date("2026-10-31T09:15:00.000Z"),
  rev: 1,
  asOf: 1,
};

const calls: string[] = [];
const queries: TaskListQuery[] = [];
let app: Service;
let base: string;

before(async () => {
  app = createService({
    logger: createLogger({ service: "test", release: "test", level: "silent" }),
    drainDelayMs: 0,
    routes: taskRoutes({
      headSeq: () => {
        calls.push("headSeq");
        return Promise.resolve(7);
      },
      list: (query) => {
        calls.push("list");
        queries.push(query);
        return Promise.resolve([doc]);
      },
    }),
  });
  const address: AddressInfo = await app.listen(0, "127.0.0.1");
  base = `http://127.0.0.1:${String(address.port)}`;
});

after(() => app.close());

test("lists tasks with headSeq read before the documents", async () => {
  calls.length = 0;
  const response = await fetch(`${base}/internal/tasks`);
  assert.equal(response.status, 200);
  assert.deepEqual(TaskListResultV1.parse(await response.json()), {
    items: [
      {
        taskId: doc._id,
        title: "Plan",
        status: "submitted",
        createdAt: "2026-10-31T09:15:00.000Z",
        updatedAt: "2026-10-31T09:15:00.000Z",
        rev: 1,
        asOf: 1,
      },
    ],
    headSeq: 7,
  });
  assert.deepEqual(calls, ["headSeq", "list"]);
  assert.deepEqual(queries.at(-1), { limit: 50 });
});

test("passes limit and before to the query", async () => {
  const response = await fetch(`${base}/internal/tasks?limit=2&before=2026-10-31T09:00:00.000Z`);
  assert.equal(response.status, 200);
  assert.deepEqual(queries.at(-1), { limit: 2, before: "2026-10-31T09:00:00.000Z" });
  const tied = await fetch(
    `${base}/internal/tasks?limit=2&before=2026-10-31T09:00:00.000Z&beforeId=${doc._id}`,
  );
  assert.equal(tied.status, 200);
  assert.deepEqual(queries.at(-1), {
    limit: 2,
    before: "2026-10-31T09:00:00.000Z",
    beforeId: doc._id,
  });
});

test("refuses a bad limit or before with 400", async () => {
  for (const query of [
    "limit=0",
    "limit=101",
    "limit=1.5",
    "limit=x",
    "before=yesterday",
    `beforeId=${doc._id}`,
    "before=2026-10-31T09:00:00.000Z&beforeId=x",
  ]) {
    const response = await fetch(`${base}/internal/tasks?${query}`);
    assert.equal(response.status, 400, query);
    assert.equal(((await response.json()) as { code: string }).code, "validation_failed");
  }
});
