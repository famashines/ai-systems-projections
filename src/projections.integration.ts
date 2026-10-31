import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { Writable } from "node:stream";
import { after, before, describe, test } from "node:test";
import {
  parseEvent,
  TaskListResultV1,
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
import { migrations, tasksOf } from "./projector.io.ts";
import { rebuild, replayFrom } from "./rebuild.io.ts";
import type { TaskDoc } from "./tasks.ts";

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
          publish { allow: ["$JS.API.>", "$JS.ACK.>", "orchestration.>"] }
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
const published: RegisteredEvent[] = [];

const startApp = async () => {
  const projections = startProjections(database.db, service);
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
    { name: "ORCHESTRATION", subjects: ["orchestration.>"] },
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
  overrides: { taskId?: string; version?: number; text?: string; eventId?: string } = {},
): RegisteredEvent => {
  const taskId = overrides.taskId ?? randomUUID();
  return parseEvent({
    eventId: overrides.eventId ?? randomUUID(),
    type: "orchestration.TaskSubmitted",
    schemaVersion: 1,
    streamId: `task-${taskId}`,
    version: overrides.version ?? 1,
    occurredAt: new Date().toISOString(),
    correlationId: taskId,
    causationId: randomUUID(),
    actor: { kind: "owner", id: "owner" },
    payload: { taskId, text: overrides.text ?? "Write a plan", mode: "auto" },
  });
};

const publish = async (event: RegisteredEvent, msgID = event.eventId): Promise<void> => {
  await jetstream(admin.connection).publish(event.type, JSON.stringify(event), { msgID });
};

const publishNew = async (event: RegisteredEvent): Promise<void> => {
  published.push(event);
  await publish(event);
};

const drained = async (): Promise<boolean> => {
  const info = await manager.consumers.info("ORCHESTRATION", "projections");
  return info.num_pending === 0 && info.num_ack_pending === 0;
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

const noticesOf = async (taskId: string) =>
  (await views()).filter(({ message }) => message.payload.docId === taskId);

const doc = (taskId: string) => tasksOf(database.db).findOne({ _id: taskId });

const allDocs = (): Promise<TaskDoc[]> => tasksOf(database.db).find().sort({ _id: 1 }).toArray();

describe("projections", () => {
  test("each TaskSubmitted gives one task document and one VIEWS notice", async () => {
    const events = [submitted({ text: `  ${"x".repeat(90)}  ` }), submitted()];
    for (const event of events) await publishNew(event);
    await waitFor(async () => (await views()).length === 2, 30_000, 100);
    const found = await views();
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
    const [first] = published;
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

  test("the query API lists tasks newest first with headSeq from VIEWS", async () => {
    assert.ok(app);
    const { state } = await manager.streams.info("VIEWS");
    const all = TaskListResultV1.parse(await (await fetch(`${app.base}/internal/tasks`)).json());
    assert.equal(all.headSeq, state.last_seq);
    assert.equal(all.items.length, 3);
    const times = all.items.map((item) => item.createdAt);
    assert.deepEqual(times, times.toSorted().reverse());
    const page = TaskListResultV1.parse(
      await (await fetch(`${app.base}/internal/tasks?limit=1&before=${times[0] ?? ""}`)).json(),
    );
    assert.deepEqual(
      page.items.map((item) => item.taskId),
      [all.items[1]?.taskId],
    );
    assert.equal((await fetch(`${app.base}/internal/tasks?limit=0`)).status, 400);
  });

  test("a rebuild from the replay API gives the same read model and notices with the same revs", async () => {
    await waitFor(drained, 30_000, 100);
    const live = await allDocs();
    const liveRevs = new Map((await views()).map(({ message: m }) => [m.eventId, m.payload]));
    const viewsBefore = (await views()).length;
    await stopApp();
    const requests: string[] = [];
    const replay: Server = createServer((request, response) => {
      requests.push(request.url ?? "");
      const url = new URL(request.url ?? "", "http://x");
      const afterPosition = Number(url.searchParams.get("afterPosition"));
      const limit = Number(url.searchParams.get("limit"));
      const events = published
        .map((event, index) => ({ ...event, position: index + 1 }))
        .filter(({ position }) => position > afterPosition)
        .slice(0, limit);
      response.setHeader("content-type", "application/json");
      response.end(JSON.stringify({ events }));
    });
    await new Promise<void>((resolve) => replay.listen(0, "127.0.0.1", resolve));
    try {
      const { port } = replay.address() as AddressInfo;
      const count = await rebuild(database.db, replayFrom(`http://127.0.0.1:${String(port)}`), 2);
      assert.equal(count, published.length);
    } finally {
      await new Promise((resolve) => replay.close(resolve));
    }
    assert.equal(requests[0], "/internal/events?afterPosition=0&limit=2");
    assert.equal(requests.length, Math.ceil(published.length / 2) + 1);
    assert.deepEqual(await allDocs(), live);
    assert.equal(await database.db.collection("processed").countDocuments(), 0);
    await startApp();
    await waitFor(async () => (await views()).length === viewsBefore + liveRevs.size, 30_000, 100);
    const republished = (await views()).slice(viewsBefore).map(({ message }) => message.payload);
    assert.deepEqual(
      republished.toSorted((a, b) => a.docId.localeCompare(b.docId) || a.rev - b.rev),
      [...liveRevs.values()].toSorted((a, b) => a.docId.localeCompare(b.docId) || a.rev - b.rev),
    );
  });
});
