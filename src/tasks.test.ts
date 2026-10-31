import assert from "node:assert/strict";
import { test } from "node:test";
import {
  parseEvent,
  TaskListItemV1,
  ViewMessageV1,
  type RegisteredEvent,
} from "@famashines/ai-systems-contracts";
import { project, titleOf, toItem, type TaskDoc } from "./tasks.ts";

const taskId = "0b9d3c4e-6f1a-4c2b-9d8e-7a6b5c4d3e2f";
const commandId = "1c8e2b3d-5a4f-4e6d-8c7b-9a0b1c2d3e4f";
const eventId = "2d7f1a2b-4c3e-4d5f-9b6a-8e7f6d5c4b3a";

const submitted = (overrides: Partial<RegisteredEvent> = {}): RegisteredEvent =>
  parseEvent({
    eventId,
    type: "orchestration.TaskSubmitted",
    schemaVersion: 1,
    streamId: `task-${taskId}`,
    version: 1,
    occurredAt: "2026-10-31T09:15:00.000Z",
    correlationId: taskId,
    causationId: commandId,
    actor: { kind: "owner", id: "owner" },
    payload: { taskId, text: "  Write a plan  ", mode: "auto" },
    ...overrides,
  });

test("a new task gives a document with rev 1 and a tasks notice", () => {
  const change = project(null, submitted());
  assert.ok(change);
  const doc: TaskDoc = {
    _id: taskId,
    title: "Write a plan",
    status: "submitted",
    createdAt: new Date("2026-10-31T09:15:00.000Z"),
    updatedAt: new Date("2026-10-31T09:15:00.000Z"),
    rev: 1,
    asOf: 1,
  };
  assert.deepEqual(change.doc, doc);
  assert.deepEqual(change.notice, {
    type: "view.tasks",
    schemaVersion: 1,
    correlationId: taskId,
    causationId: eventId,
    actor: { kind: "system", id: "projections" },
    payload: { topic: "tasks", docId: taskId, rev: 1, data: toItem(doc), commandId },
  });
  assert.equal(TaskListItemV1.safeParse(change.notice.payload.data).success, true);
  const message = ViewMessageV1.parse({
    ...change.notice,
    eventId,
    streamId: `tasks-${taskId}`,
    version: 1,
    occurredAt: "2026-10-31T09:15:01.000Z",
  });
  assert.equal(message.payload.rev, 1);
});

test("a later version changes the document, keeps createdAt and adds 1 to rev", () => {
  const first = project(null, submitted());
  assert.ok(first);
  const second = project(
    first.doc,
    submitted({ version: 3, occurredAt: "2026-10-31T10:00:00.000Z" }),
  );
  assert.ok(second);
  assert.equal(second.doc.rev, 2);
  assert.equal(second.doc.asOf, 3);
  assert.deepEqual(second.doc.createdAt, first.doc.createdAt);
  assert.deepEqual(second.doc.updatedAt, new Date("2026-10-31T10:00:00.000Z"));
  assert.equal(second.notice.payload.rev, 2);
});

test("an event with a version that is not higher than asOf changes nothing", () => {
  const first = project(null, submitted({ version: 2 }));
  assert.ok(first);
  assert.equal(project(first.doc, submitted({ version: 2 })), undefined);
  assert.equal(project(first.doc, submitted({ version: 1 })), undefined);
});

test("the title is the first 80 characters of the trimmed text, trimmed", () => {
  assert.equal(titleOf("  Plan  "), "Plan");
  assert.equal(titleOf("a".repeat(100)), "a".repeat(80));
  assert.equal(titleOf(" ".repeat(90) + "Plan"), "Plan");
  assert.equal(titleOf("a".repeat(79) + " b"), "a".repeat(79));
  assert.equal(titleOf("😀".repeat(81)), "😀".repeat(80));
});
