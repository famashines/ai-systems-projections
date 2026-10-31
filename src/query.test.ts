import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import { after, before, test } from "node:test";
import {
  AgentListResultV1,
  BudgetMonthV1,
  ConversationThreadResultV1,
  TaskListResultV1,
} from "@famashines/ai-systems-contracts";
import { createService, type Service } from "@famashines/ai-systems-kit/http";
import { createLogger } from "@famashines/ai-systems-kit/observability";
import type { AgentDoc } from "./agents.ts";
import type { MonthDoc } from "./budget.ts";
import type { MessageDoc } from "./chat.ts";
import { routes, type ListQuery } from "./query.ts";
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

const conversationId = "3e6a1b2c-4d5e-4f60-8a7b-9c0d1e2f3a4b";
const big = (index: number): MessageDoc => ({
  _id: `${String(index).padStart(8, "0")}-0000-4000-8000-000000000000`,
  conversationId,
  role: "agent",
  text: "\u0001".repeat(100_000),
  taskId: doc._id,
  createdAt: new Date("2026-10-31T09:15:00.000Z"),
  rev: 1,
  asOf: index + 1,
});
let messages: MessageDoc[] = [];
let month: MonthDoc | null = null;
const months: string[] = [];
const agent: AgentDoc = {
  _id: "00000000-0000-4000-8000-00000000a001",
  name: "orchestrator",
  enabled: true,
  activeVersion: 1,
  latestVersion: 1,
  rev: 4,
  asOf: 4,
};

const calls: string[] = [];
const queries: ListQuery[] = [];
let app: Service;
let base: string;

before(async () => {
  app = createService({
    logger: createLogger({ service: "test", release: "test", level: "silent" }),
    drainDelayMs: 0,
    routes: routes({
      headSeq: () => {
        calls.push("headSeq");
        return Promise.resolve(7);
      },
      tasks: (query) => {
        calls.push("list");
        queries.push(query);
        return Promise.resolve([doc]);
      },
      messages: (id, query) => {
        calls.push(`messages ${id}`);
        queries.push(query);
        return Promise.resolve(messages);
      },
      month: (key) => {
        months.push(key);
        return Promise.resolve(month && { ...month, _id: key });
      },
      agents: () => Promise.resolve([agent]),
      timeZone: "Europe/Berlin",
      now: () => new Date("2026-10-31T23:30:00.000Z"),
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

test("a thread page stays below 900 KiB, and the worst-case message still fits alone", async () => {
  messages = [0, 1, 2, 3].map(big);
  const response = await fetch(`${base}/internal/conversations/${conversationId}/messages?limit=4`);
  assert.equal(response.status, 200);
  const text = await response.text();
  assert.ok(Buffer.byteLength(text) <= 900 * 1024);
  const page = ConversationThreadResultV1.parse(JSON.parse(text));
  assert.equal(page.items.length, 1);
  assert.equal(page.items[0]?.messageId, messages[0]?._id);
  assert.ok(Buffer.byteLength(JSON.stringify(page.items[0])) > 600_000);
  assert.deepEqual(queries.at(-1), { limit: 4 });
  assert.equal(calls.at(-1), `messages ${conversationId}`);
});

test("an unknown thread gives 200 with no items", async () => {
  messages = [];
  const response = await fetch(`${base}/internal/conversations/${conversationId}/messages`);
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { items: [], headSeq: 7 });
  const bad = await fetch(`${base}/internal/conversations/not-an-id/messages`);
  assert.equal(bad.status, 400);
});

test("the budget view uses the month of the owner time zone, 404 with no data", async () => {
  month = null;
  const none = await fetch(`${base}/internal/budget`);
  assert.equal(none.status, 404);
  assert.equal(months.at(-1), "2026-11");
  month = {
    _id: "",
    spent: 700,
    reserved: 200,
    cap: { hard: 500, soft: 400 },
    monthVersion: 3,
    capVersion: 1,
    rev: 4,
  };
  const response = await fetch(`${base}/internal/budget?month=2026-10`);
  assert.equal(response.status, 200);
  assert.deepEqual(BudgetMonthV1.parse(await response.json()), {
    month: "2026-10",
    capMicroUsd: 500,
    softCapMicroUsd: 400,
    spentMicroUsd: 700,
    reservedMicroUsd: 200,
    remainingMicroUsd: 0,
    overCapMicroUsd: 200,
    rev: 4,
    asOf: 3,
  });
  assert.equal((await fetch(`${base}/internal/budget?month=2026-13`)).status, 400);
});

test("the agent list has headSeq", async () => {
  const response = await fetch(`${base}/internal/agents`);
  assert.deepEqual(AgentListResultV1.parse(await response.json()), {
    items: [
      {
        agentId: agent._id,
        name: "orchestrator",
        enabled: true,
        activeVersion: 1,
        latestVersion: 1,
        rev: 4,
        asOf: 4,
      },
    ],
    headSeq: 7,
  });
});
