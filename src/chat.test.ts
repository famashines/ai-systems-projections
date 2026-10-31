import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import {
  ConversationMessageV1,
  ViewMessageV1,
  parseEvent,
  type RegisteredEvent,
} from "@famashines/ai-systems-contracts";
import {
  bookCost,
  fitPage,
  linkTask,
  postMessage,
  type ChatChange,
  type CostDoc,
  type LinkDoc,
  type MessageDoc,
} from "./chat.ts";

const conversationId = randomUUID();
const ownerMessageId = randomUUID();
const replyId = randomUUID();
const taskId = randomUUID();
const runId = randomUUID();
const callId = randomUUID();
const commandId = randomUUID();

const event = (type: string, payload: object, version = 1): RegisteredEvent =>
  parseEvent({
    eventId: randomUUID(),
    type,
    schemaVersion: 1,
    streamId: `x-${randomUUID()}`,
    version,
    occurredAt: "2026-10-31T09:15:00.000Z",
    correlationId: randomUUID(),
    causationId: commandId,
    actor: { kind: "system", id: "test" },
    payload,
  });

const posted = event(
  "conversation.OwnerMessagePosted",
  { conversationId, messageId: ownerMessageId, text: "Write a poem", intent: "new_task" },
  2,
);
const submitted = event("orchestration.TaskSubmitted", {
  taskId,
  text: "Write a poem",
  mode: "auto",
  conversationId,
  messageId: ownerMessageId,
});
const reply = event(
  "conversation.AgentReplyPosted",
  {
    conversationId,
    messageId: replyId,
    text: "A poem",
    taskId,
    runId,
    sourceEventId: randomUUID(),
  },
  3,
);
const call = (attempt: number, costMicroUsd: number, type = "llm.CallCompleted") =>
  event(type, {
    taskId,
    callId,
    attempt,
    model: "openai/gpt-4.1-nano",
    costMicroUsd,
    ...(type === "llm.CallCompleted"
      ? {
          text: "A poem",
          finishReason: "stop",
          inputTokens: 1,
          outputTokens: 1,
          cachedTokens: 0,
          latencyMs: 5,
          bodyId: randomUUID(),
        }
      : { errorClass: "provider", reason: "down", nextModel: "openai/gpt-4.1-mini" }),
  });

const after = <D>(patch: { after: D } | undefined): D => {
  assert.ok(patch);
  return patch.after;
};

const valid = (change: ChatChange) => {
  for (const notice of change.notices) {
    const message = ViewMessageV1.parse({
      ...notice,
      eventId: randomUUID(),
      streamId: "notice-x",
      version: 1,
      occurredAt: "2026-10-31T09:15:01.000Z",
    });
    assert.equal(message.type, `view.conversation.${conversationId}`);
    assert.equal(ConversationMessageV1.safeParse(message.payload.data).success, true);
  }
};

test("owner message first, then TaskSubmitted: the message gets the taskId with rev 2", () => {
  const created = postMessage(null, null, null, posted);
  assert.ok(created);
  valid(created);
  const owner = after(created.messages[0]);
  assert.equal(owner.rev, 1);
  assert.equal(owner.taskId, undefined);
  assert.equal(created.notices[0]?.payload.commandId, commandId);
  const link = after(created.link);
  const linked = linkTask(link, owner, submitted);
  assert.ok(linked);
  valid(linked);
  const patched = after(linked.messages[0]);
  assert.equal(patched.taskId, taskId);
  assert.equal(patched.rev, 2);
  assert.equal(linked.notices[0]?.payload.commandId, commandId);
  assert.deepEqual(after(linked.link), { _id: ownerMessageId, taskId, rev: 2 });
  assert.equal(linkTask(after(linked.link), patched, submitted), undefined);
});

test("TaskSubmitted first, then the owner message: the same document, rev 2", () => {
  const linked = linkTask(null, null, submitted);
  assert.ok(linked);
  assert.equal(linked.messages.length, 0);
  const created = postMessage(null, after(linked.link), null, posted);
  assert.ok(created);
  const owner = after(created.messages[0]);
  assert.equal(owner.taskId, taskId);
  assert.equal(owner.rev, 2);
  assert.equal(after(created.link).taskId, taskId);
  assert.equal(postMessage(owner, null, null, posted), undefined);
});

test("cost per reply: the same total and rev in both orders, and once for each attempt", () => {
  const events = [call(1, 300, "llm.CallFailed"), call(2, 500)];
  let cost: CostDoc | null = null;
  for (const item of events) cost = after(bookCost(cost, [], item)?.cost);
  const late = postMessage(null, null, cost, reply);
  assert.ok(late);
  valid(late);
  const lateDoc = after(late.messages[0]);

  const early = postMessage(null, null, null, reply);
  assert.ok(early);
  let doc: MessageDoc = after(early.messages[0]);
  assert.equal(doc.costMicroUsd, undefined);
  assert.equal(doc.rev, 1);
  let costs: CostDoc | null = after(early.cost);
  for (const item of events) {
    const change = bookCost(costs, [doc], item);
    assert.ok(change);
    valid(change);
    costs = after(change.cost);
    doc = after(change.messages[0]);
  }
  assert.deepEqual(doc, lateDoc);
  assert.equal(doc.costMicroUsd, 800);
  assert.equal(doc.rev, 3);
  assert.equal(bookCost(costs, [doc], call(2, 500)), undefined);
});

test("a reply with no run (a refusal) gets no cost; llm events of other types do nothing", () => {
  const refused = event("conversation.AgentReplyPosted", {
    conversationId,
    messageId: randomUUID(),
    text: "No reason was given.",
    taskId,
    sourceEventId: randomUUID(),
  });
  const created = postMessage(null, null, null, refused);
  assert.ok(created);
  assert.equal(created.cost, undefined);
  const doc = after(created.messages[0]);
  const change = bookCost(null, [doc], call(1, 100));
  assert.ok(change);
  assert.equal(change.messages.length, 0);
  assert.equal(linkTask(null, null, reply), undefined);
  assert.equal(postMessage(null, null, null, submitted), undefined);
});

test("a steer message has its taskId from the event and does not touch the link", () => {
  const steer = event("conversation.OwnerMessagePosted", {
    conversationId,
    messageId: randomUUID(),
    text: "Shorter",
    intent: "steer",
    taskId,
  });
  const created = postMessage(null, null, null, steer);
  assert.ok(created);
  assert.equal(created.link, undefined);
  assert.equal(after(created.messages[0]).taskId, taskId);
  assert.equal(after(created.messages[0]).rev, 1);
});

test("fitPage stops before the item that passes the limit", () => {
  const items = ["a".repeat(10), "b".repeat(10), "c".repeat(10)];
  const base = JSON.stringify({ items: [], headSeq: 1 }).length;
  assert.deepEqual(fitPage(items, 1, base + 12 + 1 + 12), items.slice(0, 2));
  assert.deepEqual(fitPage(items, 1, base + 12 + 1 + 11), items.slice(0, 1));
  assert.deepEqual(fitPage(items, 1, 10), []);
  const link: LinkDoc = { _id: "x", rev: 1 };
  assert.deepEqual(fitPage([link], 1), [link]);
});
