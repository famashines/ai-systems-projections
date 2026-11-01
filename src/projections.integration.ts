import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { Writable } from "node:stream";
import { after, before, describe, test } from "node:test";
import {
  AgentListResultV1,
  BudgetMonthV1,
  ConversationMessageV1,
  ConversationThreadResultV1,
  parseEvent,
  TaskDetailV1,
  TaskListResultV1,
  TaskOutputV1,
  ViewMessageV1,
  type RegisteredEvent,
} from "@famashines/ai-systems-contracts";
import { createService, type Service } from "@famashines/ai-systems-kit/http";
import { connectNats, type Nats } from "@famashines/ai-systems-kit/messaging";
import { connectMongo, runMigrations, type Mongo } from "@famashines/ai-systems-kit/mongo";
import {
  createStreams,
  startMongo,
  startNats,
  waitFor,
  type Started,
} from "@famashines/ai-systems-kit/testing";
import { jetstream, jetstreamManager, type JetStreamManager } from "@nats-io/jetstream";
import { pino } from "pino";
import { startProjections, type Projections } from "./app.io.ts";
import { monthKey } from "./budget.ts";
import { collections, migrations, readModelCollections, tasksOf } from "./projector.io.ts";
import {
  assertServiceStopped,
  markAndCheck,
  rebuild,
  rebuildRunning,
  replayFrom,
  type FetchPage,
} from "./rebuild.io.ts";

const streams = ["ORCHESTRATION", "CONVERSATION", "LLM", "BUDGET", "AGENTS", "PROMPTS"];
const readGrants = streams.flatMap((stream) => {
  const filter = `${stream.toLowerCase()}.>`;
  return [
    `"$JS.API.STREAM.INFO.${stream}"`,
    `"$JS.API.CONSUMER.CREATE.${stream}.projections.${filter}"`,
    `"$JS.API.CONSUMER.INFO.${stream}.projections"`,
    `"$JS.API.CONSUMER.MSG.NEXT.${stream}.projections"`,
    `"$JS.ACK.${stream}.projections.>"`,
  ];
});

const config = `
jetstream { store_dir: /data }
accounts {
  AISYSTEMS {
    jetstream { max_file: 1073741824, max_memory: 0 }
    users = [
      {
        user: admin, password: admin
        permissions {
          publish {
            allow: [
              "$JS.API.>", "$JS.ACK.>", "orchestration.>", "conversation.>", "llm.>", "budget.>",
              "agents.>"
            ]
          }
          subscribe { allow: ["_INBOX_admin.>"] }
        }
      }
      {
        user: ai-systems-projections, password: projections
        permissions {
          publish {
            allow: [
              "view.>", "dlq.projections.>", "$JS.API.INFO", "$JS.API.STREAM.INFO.VIEWS",
              ${readGrants.join(", ")}
            ]
          }
          subscribe { allow: ["_INBOX_projections.>"] }
        }
      }
    ]
  }
}
`;

const lines: string[] = [];
const logger = pino(
  { level: "debug" },
  new Writable({
    write(chunk: Buffer, _encoding, done) {
      lines.push(chunk.toString());
      done();
    },
  }),
);

let mongo: Started;
let natsServer: Started;
let database: Mongo;
let admin: Nats;
let service: Nats;
let manager: JetStreamManager;
let app: { projections: Projections; http: Service; base: string } | undefined;
const zone = "Europe/Berlin";
const sources = ["orchestration", "conversation", "llm", "budget", "agents"] as const;
const published = new Map<string, RegisteredEvent[]>(sources.map((source) => [source, []]));
const submittedTasks: TaskSubmittedEvent[] = [];
type TaskSubmittedEvent = Extract<RegisteredEvent, { type: "orchestration.TaskSubmitted" }>;

const startApp = async () => {
  const projections = startProjections(database.db, service, zone);
  const http = createService({
    logger,
    drainDelayMs: 0,
    routes: projections.routes,
    readiness: { mongo: database.ready, nats: service.ready, ...projections.readiness },
    closeHooks: projections.closeHooks,
  });
  const { port } = await http.listen(0, "127.0.0.1");
  app = { projections, http, base: `http://127.0.0.1:${String(port)}` };
  await waitFor(async () => (await fetch(`${app?.base ?? ""}/ready`)).ok, 30_000, 100);
};

const stopApp = async () => {
  await app?.http.close();
  app = undefined;
};

before(async () => {
  [mongo, natsServer] = await Promise.all([startMongo(), startNats({ config })]);
  database = await connectMongo({ url: mongo.url, database: "projections" });
  await runMigrations(database.db, migrations);
  [admin, service] = await Promise.all([
    connectNats({
      servers: natsServer.url,
      user: "admin",
      pass: "admin",
      service: "admin",
      logger,
    }),
    connectNats({
      servers: natsServer.url,
      user: "ai-systems-projections",
      pass: "projections",
      service: "projections",
      logger,
    }),
  ]);
  manager = await jetstreamManager(admin.connection);
  await createStreams(admin.connection, [
    ...sources.map((source) => ({ name: source.toUpperCase(), subjects: [`${source}.>`] })),
    { name: "VIEWS", subjects: ["view.>"] },
    { name: "DLQ", subjects: ["dlq.>"] },
  ]);
  await startApp();
});

after(async () => {
  await stopApp();
  await Promise.all([admin.close(), service.close(), database.close()]);
  await Promise.all([mongo.stop(), natsServer.stop()]);
});

const submitted = (
  overrides: {
    taskId?: string;
    version?: number;
    text?: string;
    eventId?: string;
    occurredAt?: string;
  } = {},
): TaskSubmittedEvent => {
  const taskId = overrides.taskId ?? randomUUID();
  return parseEvent({
    eventId: overrides.eventId ?? randomUUID(),
    type: "orchestration.TaskSubmitted",
    schemaVersion: 1,
    streamId: `task-${taskId}`,
    version: overrides.version ?? 1,
    occurredAt: overrides.occurredAt ?? new Date().toISOString(),
    correlationId: taskId,
    causationId: randomUUID(),
    actor: { kind: "owner", id: "owner" },
    payload: { taskId, text: overrides.text ?? "Write a plan", mode: "auto" },
  }) as TaskSubmittedEvent;
};

const publish = async (event: RegisteredEvent, msgID = event.eventId): Promise<void> => {
  await jetstream(admin.connection).publish(event.type, JSON.stringify(event), { msgID });
};

const publishNew = async (event: RegisteredEvent): Promise<void> => {
  published.get(event.type.slice(0, event.type.indexOf(".")))?.push(event);
  if (event.type === "orchestration.TaskSubmitted" && event.payload.conversationId === undefined) {
    submittedTasks.push(event);
  }
  await publish(event);
};

const drained = async (): Promise<boolean> => {
  for (const source of sources) {
    const info = await manager.consumers.info(source.toUpperCase(), "projections");
    if (info.num_pending > 0 || info.num_ack_pending > 0) return false;
  }
  return true;
};

const views = async (): Promise<{ message: ViewMessageV1; msgId: string; seq: number }[]> => {
  const { state } = await manager.streams.info("VIEWS");
  const found = [];
  for (let seq = state.first_seq; state.messages > 0 && seq <= state.last_seq; seq += 1) {
    const stored = await manager.streams.getMessage("VIEWS", { seq });
    assert.ok(stored);
    found.push({
      message: ViewMessageV1.parse(stored.json()),
      msgId: stored.header.get("Nats-Msg-Id"),
      seq,
    });
  }
  return found;
};

const taskNotices = async () =>
  (await views()).filter(({ message }) => message.type === "view.tasks");

const noticesOf = async (taskId: string) =>
  (await taskNotices()).filter(({ message }) => message.payload.docId === taskId);

const doc = (taskId: string) => tasksOf(database.db).findOne({ _id: taskId });

const allDocs = async (): Promise<Record<string, unknown[]>> =>
  Object.fromEntries(
    await Promise.all(
      readModelCollections.map(async (name): Promise<[string, unknown[]]> => [
        name,
        await database.db.collection(name).find().sort({ _id: 1 }).toArray(),
      ]),
    ),
  );

describe("projections", () => {
  test("each TaskSubmitted gives one task document and one VIEWS notice", async () => {
    const events = [submitted({ text: `  ${"x".repeat(90)}  ` }), submitted()];
    for (const event of events) await publishNew(event);
    await waitFor(async () => (await views()).length === 4, 30_000, 100);
    const found = await taskNotices();
    for (const [index, event] of events.entries()) {
      const { taskId } = event.payload;
      const stored = await doc(taskId);
      assert.ok(stored);
      assert.equal(stored.rev, 1);
      assert.equal(stored.asOf, 1);
      assert.equal(stored.status, "submitted");
      assert.equal(stored.title, index === 0 ? "x".repeat(80) : "Write a plan");
      assert.deepEqual(stored.createdAt, new Date(event.occurredAt));
      const notice = found[index];
      assert.ok(notice);
      assert.equal(notice.msgId, notice.message.eventId);
      assert.equal(notice.message.type, "view.tasks");
      assert.equal(notice.message.causationId, event.eventId);
      assert.equal(notice.message.correlationId, event.correlationId);
      assert.deepEqual(notice.message.payload, {
        topic: "tasks",
        docId: taskId,
        rev: 1,
        commandId: event.causationId,
        data: {
          taskId,
          title: stored.title,
          status: "submitted",
          createdAt: event.occurredAt,
          updatedAt: event.occurredAt,
          rev: 1,
          asOf: 1,
        },
      });
    }
  });

  test("a redelivered or older event changes nothing and gives no notice", async () => {
    const [first] = submittedTasks;
    assert.ok(first);
    const before = await doc(first.payload.taskId);
    const count = (await views()).length;
    const outbox = database.db.collection("events");
    const notices = await outbox.countDocuments();
    await publish(first, randomUUID());
    await publish(
      submitted({ taskId: first.payload.taskId, version: 1, text: "Other text" }),
      randomUUID(),
    );
    await waitFor(drained, 30_000, 100);
    assert.deepEqual(await doc(first.payload.taskId), before);
    assert.equal(await outbox.countDocuments(), notices);
    assert.equal((await views()).length, count);
  });

  test("ordered: a failing event holds the next event of its stream until it is applied", async () => {
    const info = await manager.consumers.info("ORCHESTRATION", "projections");
    assert.equal(info.config.max_ack_pending, 1);
    const taskId = randomUUID();
    const first = submitted({ taskId, version: 1, text: "First" });
    const second = submitted({ taskId, version: 2, text: "Second" });
    await database.db.command({
      collMod: "tasks",
      validator: { title: { $ne: "First" } },
      validationAction: "error",
    });
    lines.length = 0;
    await publishNew(first);
    await publishNew(second);
    await waitFor(() => lines.some((line) => line.includes("the handler failed")), 30_000, 50);
    assert.equal(await doc(taskId), null);
    assert.equal((await manager.consumers.info("ORCHESTRATION", "projections")).num_pending, 1);
    await database.db.command({ collMod: "tasks", validator: {} });
    await waitFor(async () => (await noticesOf(taskId)).length === 2, 30_000, 100);
    const stored = await doc(taskId);
    assert.ok(stored);
    assert.equal(stored.title, "Second");
    assert.equal(stored.rev, 2);
    assert.equal(stored.asOf, 2);
    assert.deepEqual(
      (await noticesOf(taskId)).map(({ message }) => message.payload.rev),
      [1, 2],
    );
  });

  test("the query API lists tasks newest first, pages through ties, with headSeq", async () => {
    assert.ok(app);
    const base = app.base;
    const list = async (query: string) =>
      TaskListResultV1.parse(await (await fetch(`${base}/internal/tasks?${query}`)).json());
    const { state } = await manager.streams.info("VIEWS");
    const first = await list("");
    assert.equal(first.headSeq, state.last_seq);
    assert.equal(first.items.length, 3);
    const tie = new Date(Date.parse(first.items[0]?.createdAt ?? "") + 60_000).toISOString();
    const older = new Date(Date.parse(tie) - 1).toISOString();
    for (const [index, occurredAt] of [tie, tie, tie, older].entries()) {
      await publishNew(submitted({ occurredAt, text: index === 0 ? "\u202E" : "Write a plan" }));
    }
    await waitFor(async () => (await list("")).items.length === 7, 30_000, 100);
    const all = await list("limit=100");
    const times = all.items.map((item) => item.createdAt);
    assert.deepEqual(times, times.toSorted().reverse());
    const seen: string[] = [];
    let cursor = "";
    for (;;) {
      const page = await list(`limit=2${cursor}`);
      seen.push(...page.items.map((item) => item.taskId));
      const last = page.items.at(-1);
      if (!last) break;
      cursor = `&before=${last.createdAt}&beforeId=${last.taskId}`;
    }
    assert.deepEqual(
      seen,
      all.items.map((item) => item.taskId),
    );
    assert.equal(all.items.filter((item) => item.title === "(no title)").length, 1);
    const onlyBefore = await list(`limit=100&before=${tie}`);
    assert.equal(onlyBefore.items.length, 4);
    assert.equal((await fetch(`${base}/internal/tasks?limit=0`)).status, 400);
  });

  const make = (
    type: string,
    streamId: string,
    version: number,
    payload: object,
    occurredAt = new Date().toISOString(),
  ): RegisteredEvent =>
    parseEvent({
      eventId: randomUUID(),
      type,
      schemaVersion: 1,
      streamId,
      version,
      occurredAt,
      correlationId: randomUUID(),
      causationId: randomUUID(),
      actor: { kind: "system", id: "test" },
      payload,
    });

  const get = async (path: string) => {
    assert.ok(app);
    return fetch(`${app.base}${path}`);
  };

  const message = (messageId: string) => collections(database.db).chat.findOne({ _id: messageId });

  test("chat: the owner message gets its taskId in each order, a reply gets the cost of its task", async () => {
    const conversationId = randomUUID();
    const stream = `conversation-${conversationId}`;
    const orders = ["task first", "message first", "same time"] as const;
    const tasks: string[] = [];
    let version = 0;
    for (const order of orders) {
      const messageId = randomUUID();
      const taskId = randomUUID();
      tasks.push(taskId);
      version += 1;
      const posted = make("conversation.OwnerMessagePosted", stream, version, {
        conversationId,
        messageId,
        text: `Write a poem (${order})`,
        intent: "new_task",
      });
      const task = make("orchestration.TaskSubmitted", `task-${taskId}`, 1, {
        taskId,
        text: `Write a poem (${order})`,
        mode: "auto",
        conversationId,
        messageId,
      });
      if (order === "task first") {
        await publishNew(task);
        await waitFor(async () => (await doc(taskId)) !== null, 30_000, 100);
        await publishNew(posted);
      } else if (order === "message first") {
        await publishNew(posted);
        await waitFor(async () => (await message(messageId)) !== null, 30_000, 100);
        await publishNew(task);
      } else {
        await Promise.all([publishNew(posted), publishNew(task)]);
      }
      await waitFor(async () => (await message(messageId))?.taskId === taskId, 30_000, 100);
      assert.equal((await message(messageId))?.rev, 2, order);
    }
    const taskId = tasks[0] ?? "";
    const callId = randomUUID();
    const replyId = randomUUID();
    const llm = (attempt: number, costMicroUsd: number) =>
      make("llm.CallFailed", `call-${callId}`, attempt, {
        taskId,
        callId,
        attempt,
        model: "openai/gpt-4.1-nano",
        errorClass: "provider",
        reason: "down",
        costMicroUsd,
      });
    await publishNew(llm(1, 300));
    version += 1;
    await publishNew(
      make("conversation.AgentReplyPosted", stream, version, {
        conversationId,
        messageId: replyId,
        text: "A poem",
        taskId,
        runId: randomUUID(),
        sourceEventId: randomUUID(),
      }),
    );
    await publishNew(llm(2, 500));
    await waitFor(async () => (await message(replyId))?.costMicroUsd === 800, 30_000, 100);
    assert.equal((await message(replyId))?.rev, 3);
    await waitFor(drained, 30_000, 100);
    const thread = ConversationThreadResultV1.parse(
      await (await get(`/internal/conversations/${conversationId}/messages`)).json(),
    );
    assert.equal(thread.items.length, 4);
    assert.deepEqual(
      thread.items.map((item) => [item.role, item.taskId]),
      [["agent", taskId], ...tasks.toReversed().map((id) => ["owner", id])],
    );
    const notices = (await views()).filter(
      ({ message: view }) => view.type === `view.conversation.${conversationId}`,
    );
    for (const { message: view } of notices) {
      assert.equal(ConversationMessageV1.safeParse(view.payload.data).success, true);
    }
    const last = notices.filter(({ message: view }) => view.payload.docId === replyId).at(-1);
    assert.deepEqual(last?.message.payload.data, thread.items[0]);
    const empty = await get(`/internal/conversations/${randomUUID()}/messages`);
    assert.equal(empty.status, 200);
    assert.deepEqual(((await empty.json()) as { items: unknown[] }).items, []);
  });

  test("budget: 404 with no data, then the cap and the month numbers", async () => {
    assert.equal((await get("/internal/budget")).status, 404);
    const month = monthKey(new Date(), zone);
    await publishNew(
      make("budget.BudgetSet", "budget-cap-month", 1, {
        scope: { kind: "month" },
        hardCapMicroUsd: 1000,
      }),
    );
    const reservationId = randomUUID();
    await publishNew(
      make("budget.FundsReserved", `budget-month-${month}`, 1, {
        reservationId,
        taskId: randomUUID(),
        agentId: randomUUID(),
        scopes: [{ kind: "month", key: month }],
        amountMicroUsd: 400,
        expiresAt: new Date(Date.now() + 600_000).toISOString(),
      }),
    );
    await publishNew(
      make("budget.ReservationSettled", `budget-month-${month}`, 2, {
        reservationId,
        reservedMicroUsd: 400,
        actualMicroUsd: 1200,
      }),
    );
    await waitFor(async () => (await get("/internal/budget")).ok, 30_000, 100);
    await waitFor(
      async () =>
        BudgetMonthV1.parse(await (await get("/internal/budget")).json()).spentMicroUsd === 1200,
      30_000,
      100,
    );
    assert.deepEqual(
      BudgetMonthV1.parse(await (await get(`/internal/budget?month=${month}`)).json()),
      {
        month,
        capMicroUsd: 1000,
        spentMicroUsd: 1200,
        reservedMicroUsd: 0,
        remainingMicroUsd: 0,
        overCapMicroUsd: 200,
        rev: 3,
        asOf: 2,
      },
    );
    const other = BudgetMonthV1.parse(await (await get("/internal/budget?month=2020-01")).json());
    assert.equal(other.rev, 0);
    assert.equal(other.capMicroUsd, 1000);
    const budgetNotices = (await views()).filter(
      ({ message: view }) => view.type === "view.budget",
    );
    assert.deepEqual(
      budgetNotices.map(({ message: view }) => view.payload.rev),
      [1, 2, 3],
    );
  });

  test("agents: the list follows the AGENTS stream", async () => {
    const agentId = randomUUID();
    const stream = `agent-${agentId}`;
    const events = [
      make("agents.AgentCreated", stream, 1, { agentId, name: "writer" }),
      make("agents.AgentVersionPublished", stream, 2, {
        agentId,
        version: 1,
        prompt: { promptId: randomUUID(), version: 1 },
        models: ["openai/gpt-4.1-nano"],
        limits: { maxCalls: 10, maxTokens: 4096, maxCostMicroUsd: 50000 },
      }),
      make("agents.AgentVersionActivated", stream, 3, { agentId, version: 1 }),
      make("agents.AgentEnabled", stream, 4, { agentId }),
    ];
    for (const event of events) await publishNew(event);
    const list = async () => AgentListResultV1.parse(await (await get("/internal/agents")).json());
    await waitFor(async () => (await list()).items[0]?.enabled === true, 30_000, 100);
    assert.deepEqual((await list()).items, [
      {
        agentId,
        name: "writer",
        enabled: true,
        activeVersion: 1,
        latestVersion: 1,
        rev: 4,
        asOf: 4,
      },
    ]);
  });

  test("task detail: an M3 run gives the detail, the outputs and task:<id> notices; an unknown type goes to the DLQ", async () => {
    const taskId = randomUUID();
    const runId = randomUUID();
    const subtaskId = randomUUID();
    const callId = randomUUID();
    const stream = `task-${taskId}`;
    const subtasks = [
      {
        subtaskId,
        title: "Step",
        instructions: "Do it",
        agentId: randomUUID(),
        dependsOn: [],
        needsApproval: false,
      },
    ];
    const steps: [string, object][] = [
      ["orchestration.TaskSubmitted", { taskId, text: "Write a report", mode: "auto" }],
      ["orchestration.PlanProposed", { taskId, planVersion: 1, subtasks, awaitsApproval: false }],
      ["orchestration.PlanApproved", { taskId, planVersion: 1 }],
      ["orchestration.RunStarted", { taskId, runId }],
      ["orchestration.SubtaskQueued", { taskId, runId, subtaskId }],
      [
        "orchestration.SubtaskStarted",
        { taskId, runId, subtaskId, tryNumber: 1, callId, agentId: randomUUID(), agentVersion: 1 },
      ],
      [
        "orchestration.SubtaskCompleted",
        {
          taskId,
          runId,
          subtaskId,
          callId,
          text: "Part",
          inputTokens: 1,
          outputTokens: 2,
          costMicroUsd: 40,
        },
      ],
      [
        "orchestration.CallSettled",
        {
          taskId,
          callId,
          kind: "subtask",
          subtaskId,
          inputTokens: 1,
          outputTokens: 2,
          costMicroUsd: 40,
        },
      ],
      ["orchestration.RunCompleted", { taskId, runId, summary: "The report", costMicroUsd: 40 }],
    ];
    const events = steps.map(([type, payload], index) => make(type, stream, index + 1, payload));
    await publishNew(
      make("llm.CallFailed", `call-${callId}`, 1, {
        taskId,
        callId,
        attempt: 1,
        model: "openai/gpt-4.1-nano",
        errorClass: "cancelled",
        reason: "Stopped",
        costMicroUsd: 0,
      }),
    );
    for (const event of events.slice(0, 5)) await publishNew(event);
    await publish({
      ...events[4],
      eventId: randomUUID(),
      type: "orchestration.FutureThing",
    } as never);
    for (const event of events.slice(5)) await publishNew(event);
    await waitFor(async () => (await get(`/internal/tasks/${taskId}/output`)).ok, 30_000, 100);
    const detail = TaskDetailV1.parse(await (await get(`/internal/tasks/${taskId}`)).json());
    assert.equal(detail.status, "completed");
    assert.equal(detail.costMicroUsd, 40);
    assert.equal(detail.rev, 8);
    assert.equal(detail.asOf, 9);
    assert.deepEqual(
      detail.subtasks.map((state) => [state.status, state.tries, state.costMicroUsd]),
      [["done", 1, 40]],
    );
    assert.deepEqual(
      TaskOutputV1.parse(await (await get(`/internal/tasks/${taskId}/output`)).json()),
      { taskId, runId, text: "The report" },
    );
    assert.deepEqual(
      TaskOutputV1.parse(
        await (await get(`/internal/tasks/${taskId}/subtasks/${subtaskId}/output`)).json(),
      ),
      { taskId, runId, subtaskId, text: "Part" },
    );
    assert.equal((await get(`/internal/tasks/${randomUUID()}`)).status, 404);
    const notices = (await views()).filter(
      ({ message: view }) => view.type === `view.task.${taskId}`,
    );
    assert.deepEqual(
      notices.map(({ message: view }) => view.payload.rev),
      [1, 2, 3, 4, 5, 6, 7, 8],
    );
    assert.deepEqual(notices.at(-1)?.message.payload.data, detail);
    const list = TaskListResultV1.parse(await (await get("/internal/tasks?limit=100")).json());
    assert.equal(list.items.find((item) => item.taskId === taskId)?.status, "completed");
    const dlq = await manager.streams.info("DLQ", { subjects_filter: ">" });
    assert.ok(
      Object.keys(dlq.state.subjects ?? {}).includes("dlq.projections.orchestration.FutureThing"),
    );
  });

  let live: Record<string, unknown[]> = {};
  let livePayloads: ViewMessageV1["payload"][] = [];
  let viewsBefore = 0;
  const byDoc = (a: { docId: string; rev: number }, b: { docId: string; rev: number }) =>
    a.docId.localeCompare(b.docId) || a.rev - b.rev;

  test("the rebuild refuses to run while the service pulls from its consumer", async () => {
    await waitFor(drained, 30_000, 100);
    live = await allDocs();
    livePayloads = (await views()).map(({ message }) => message.payload);
    viewsBefore = livePayloads.length;
    await assert.rejects(assertServiceStopped(service, 100), /waiting pulls/);
    await assert.rejects(markAndCheck(database.db, service, 100), /waiting pulls/);
    assert.equal(await rebuildRunning(database.db), false);
    const rebuildMarkers = database.db.collection<{ _id: string }>("rebuild");
    await rebuildMarkers.insertOne({ _id: "running" });
    await assert.rejects(markAndCheck(database.db, service, 100), /waiting pulls/);
    assert.equal(await rebuildRunning(database.db), true);
    await rebuildMarkers.deleteOne({ _id: "running" });
    await stopApp();
    const waiting = async () =>
      (await manager.consumers.info("ORCHESTRATION", "projections")).num_waiting;
    await waitFor(async () => (await waiting()) === 0, 30_000, 100);
    await markAndCheck(database.db, service, 100);
    assert.equal(await rebuildRunning(database.db), true);
  });

  test("a rebuild that stops leaves its marker, and the service refuses to start", async () => {
    const failing: FetchPage = () => Promise.reject(new Error("replay API down"));
    await assert.rejects(rebuild(database.db, [failing], zone), /replay API down/);
    assert.equal(await rebuildRunning(database.db), true);
    const child = spawn(process.execPath, ["src/main.ts"], {
      env: {
        PATH: process.env.PATH,
        MONGODB_URI: mongo.url,
        NATS_SERVERS: natsServer.url,
        NATS_USER: "ai-systems-projections",
        NATS_PASSWORD: "projections",
        ORCHESTRATION_URL: "http://127.0.0.1:1",
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let output = "";
    for (const stream of [child.stdout, child.stderr]) {
      stream.on("data", (chunk: Buffer) => (output += chunk.toString()));
    }
    const code = await new Promise((resolve) => child.on("exit", resolve));
    assert.equal(code, 1);
    assert.match(output, /a rebuild runs or did not end/);
    assert.doesNotMatch(output, /connecting to NATS/);
  });

  test("a rebuild from the replay APIs gives the same read models, skips bad events, and writes the last notice of each document again with the same rev", async () => {
    const requests: string[] = [];
    const servers: Server[] = [];
    const urls: string[] = [];
    let served = 0;
    for (const source of sources) {
      const events: unknown[] = [...(published.get(source) ?? [])];
      if (source === "orchestration") {
        events.splice(1, 0, {
          ...submittedTasks[0],
          eventId: randomUUID(),
          type: "orchestration.Unknown",
        });
      }
      served += events.length;
      const replay = createServer((request, response) => {
        requests.push(`${source} ${request.url ?? ""}`);
        const url = new URL(request.url ?? "", "http://x");
        const afterPosition = Number(url.searchParams.get("afterPosition"));
        const limit = Number(url.searchParams.get("limit"));
        const page = events
          .map((event, index) => ({ ...(event as object), position: index + 1 }))
          .filter(({ position }) => position > afterPosition)
          .slice(0, limit);
        response.setHeader("content-type", "application/json");
        response.end(JSON.stringify({ events: page }));
      });
      await new Promise<void>((resolve) => replay.listen(0, "127.0.0.1", resolve));
      servers.push(replay);
      urls.push(`http://127.0.0.1:${String((replay.address() as AddressInfo).port)}`);
    }
    try {
      const result = await rebuild(database.db, urls.map(replayFrom), zone, 2);
      assert.deepEqual(result, {
        applied: served - 1,
        skipped: 1,
        skippedTypes: ["orchestration.Unknown"],
      });
    } finally {
      for (const replay of servers) await new Promise((resolve) => replay.close(resolve));
    }
    assert.equal(requests[0], "orchestration /internal/events?afterPosition=0&limit=2");
    assert.ok(requests.includes("agents /internal/events?afterPosition=0&limit=2"));
    assert.equal(await rebuildRunning(database.db), false);
    assert.deepEqual(await allDocs(), live);
    assert.equal(await database.db.collection("processed").countDocuments(), 0);
    await startApp();
    const lastOf = (payloads: ViewMessageV1["payload"][]) => [
      ...new Map(
        payloads.toSorted(byDoc).map((payload) => [`${payload.topic} ${payload.docId}`, payload]),
      ).values(),
    ];
    const expected = lastOf(livePayloads);
    await waitFor(
      async () =>
        lastOf((await views()).slice(viewsBefore).map(({ message: view }) => view.payload))
          .length === expected.length,
      30_000,
      100,
    );
    await waitFor(drained, 30_000, 100);
    const republished = (await views()).slice(viewsBefore).map(({ message: view }) => view.payload);
    assert.deepEqual(lastOf(republished), expected);
  });
});
