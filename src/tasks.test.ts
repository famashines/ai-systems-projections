import assert from "node:assert/strict";
import { test } from "node:test";
import {
  parseEvent,
  TaskListItemV1,
  ViewMessageV1,
  type RegisteredEvent,
} from "@famashines/ai-systems-contracts";
import { noTitle, project, statusAfter, titleOf, toItem, type TaskDoc } from "./tasks.ts";

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

test("the title cuts by graphemes and replaces control and bidi characters", () => {
  const family = "👨‍👩‍👧";
  assert.equal(titleOf("a".repeat(79) + family + "b"), "a".repeat(79) + family);
  assert.equal(titleOf("\u001b[31mred\u001b[0m"), "[31mred [0m");
  assert.equal(titleOf("line one\r\nline two\u2028three"), "line one  line two three");
  assert.equal(titleOf("\u202Etxt.exe\u202C name"), "txt.exe  name");
  assert.equal(titleOf("\t\u200Fplan\u0000"), "plan");
});

test("the title has at most 200 UTF-16 units, for TaskListItemV1", () => {
  const title = titleOf("👨‍👩‍👧".repeat(80));
  assert.equal(title, "👨‍👩‍👧".repeat(25));
  assert.ok(title.length <= 200);
});

test("a text with no visible title gives the fixed title", () => {
  for (const text of ["\u0007", "\u202E", "\u200E\u200F", "\u2066\u2069", "\uD800", "\uDC00\t"]) {
    assert.equal(titleOf(text), noTitle, JSON.stringify(text));
  }
});

test("a first grapheme above 200 UTF-16 units is cut by code points", () => {
  const title = titleOf("e" + "\u0301".repeat(300) + " rest");
  assert.equal(title, "e" + "\u0301".repeat(199));
  assert.equal(title.length, 200);
});

test("a lone surrogate becomes a space", () => {
  assert.equal(titleOf("a\uD800b"), "a b");
});

test("each title is a valid TaskListItemV1 title", () => {
  const texts = ["\u0007", "\u202E", "e" + "\u0301".repeat(300), "👨‍👩‍👧".repeat(80), "Plan"];
  for (const text of texts) {
    assert.equal(
      TaskListItemV1.shape.title.safeParse(titleOf(text)).success,
      true,
      JSON.stringify(text),
    );
  }
});

const later = (type: string, version: number, payload: object): RegisteredEvent =>
  parseEvent({
    eventId: "3a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d",
    type,
    schemaVersion: 1,
    streamId: `task-${taskId}`,
    version,
    occurredAt: "2026-10-31T09:20:00.000Z",
    correlationId: taskId,
    causationId: eventId,
    actor: { kind: "system", id: "orchestration" },
    payload,
  });

test("run events set the task status, with no commandId; other events change nothing", () => {
  const first = project(null, submitted());
  assert.ok(first);
  const runId = "4b2c3d4e-5f6a-4b7c-9d8e-0f1a2b3c4d5e";
  const reason = "Monthly cap reached";
  const cases = [
    ["orchestration.RunStarted", { taskId, runId }, "running"],
    ["orchestration.RunBlocked", { taskId, runId, code: "budget_refused", reason }, "blocked"],
    ["orchestration.RunFailed", { taskId, runId, code: "empty_output", reason }, "failed"],
    [
      "orchestration.RunCompleted",
      { taskId, runId, summary: "Done", costMicroUsd: 10 },
      "completed",
    ],
  ] as const;
  for (const [type, payload, status] of cases) {
    const change = project(first.doc, later(type, 2, payload));
    assert.ok(change, type);
    assert.equal(change.doc.status, status);
    assert.equal(change.doc.rev, 2);
    assert.equal(change.doc.asOf, 2);
    assert.equal(change.doc.title, first.doc.title);
    assert.equal(change.notice.payload.commandId, undefined);
  }
  assert.equal(project(null, later("orchestration.RunStarted", 2, { taskId, runId })), undefined);
  const refused = later("orchestration.MessageRefused", 2, {
    taskId,
    conversationId: taskId,
    messageId: eventId,
    intent: "steer",
    reason,
  });
  assert.equal(project(first.doc, refused), undefined);
});

test("the list status follows the status table of contracts v0.4.1", () => {
  const runId = "3a4b5c6d-7e8f-4a0b-9c1d-2e3f4a5b6c7d";
  const subtaskId = "4b5c6d7e-8f9a-4b1c-8d2e-3f4a5b6c7d8e";
  const plan = [
    {
      subtaskId,
      title: "Step",
      instructions: "Do it",
      agentId: runId,
      dependsOn: [],
      needsApproval: false,
    },
  ];
  const cases: [string, object, string | undefined][] = [
    ["orchestration.ClarificationRequested", { taskId, questions: ["Which?"] }, "clarifying"],
    [
      "orchestration.ClarificationAnswered",
      { taskId, conversationId: runId, messageId: eventId, text: "This" },
      "planning",
    ],
    [
      "orchestration.PlanProposed",
      { taskId, planVersion: 1, subtasks: plan, awaitsApproval: true },
      "awaiting_approval",
    ],
    [
      "orchestration.PlanProposed",
      { taskId, planVersion: 1, subtasks: plan, awaitsApproval: false },
      "planning",
    ],
    ["orchestration.PlanEdited", { taskId, planVersion: 2, subtasks: plan }, "awaiting_approval"],
    ["orchestration.PlanApproved", { taskId, planVersion: 2 }, undefined],
    ["orchestration.PlanRejected", { taskId, planVersion: 2, reason: "No" }, "rejected"],
    ["orchestration.TaskCancelled", { taskId }, "cancelled"],
    ["orchestration.RunStarted", { taskId, runId }, "running"],
    ["orchestration.RunPaused", { taskId, runId }, "paused"],
    ["orchestration.RunResumed", { taskId, runId }, "running"],
    [
      "orchestration.RunBlocked",
      { taskId, runId, code: "subtask_failed", reason: "A step failed" },
      "blocked",
    ],
    [
      "orchestration.SubtaskBlocked",
      { taskId, runId, subtaskId, code: "forbidden", reason: "No" },
      undefined,
    ],
    ["orchestration.RunCancelled", { taskId, runId }, "cancelled"],
    ["orchestration.CallCancelRequested", { taskId, callId: runId }, undefined],
  ];
  const first = project(null, submitted());
  assert.ok(first);
  for (const [index, [type, payload, status]] of cases.entries()) {
    const event = submitted({ type, payload, version: index + 2 } as Partial<RegisteredEvent>);
    assert.equal(statusAfter(event), status, type);
    assert.equal(project(first.doc, event)?.doc.status, status, type);
  }
});
