import assert from "node:assert/strict";
import { test } from "node:test";
import {
  parseEvent,
  TaskDetailV1,
  TaskOutputV1,
  ViewMessageV1,
  type RegisteredEvent,
} from "@famashines/ai-systems-contracts";
import {
  maxCostMicroUsd,
  outputId,
  projectDetail,
  toDetail,
  toOutput,
  type DetailDoc,
  type OutputDoc,
} from "./task-detail.ts";

const id = (n: number): string => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
const taskId = id(1);
const runId = id(2);
const [a, b, c] = [id(11), id(12), id(13)];
const [agent, otherAgent] = [id(21), id(22)];
const [planCall, callA1, callA2, callC, aggregateCall] = [id(31), id(32), id(33), id(34), id(35)];
const checkpointId = id(41);
const conversationId = id(51);
const messageId = id(52);
const model = "openai/gpt-4.1-nano";
const taskModel = "anthropic/claude-opus-4.8";

const at = (second: number): string => new Date(Date.UTC(2026, 10, 1, 9, 0, second)).toISOString();

const stream = (
  steps: [string, object][],
  streamId = `task-${taskId}`,
  first = 1,
): RegisteredEvent[] =>
  steps.map(([type, payload], index) =>
    parseEvent({
      eventId: id(1000 + first + index),
      type,
      schemaVersion: 1,
      streamId,
      version: first + index,
      occurredAt: at(first + index),
      correlationId: taskId,
      causationId: id(5000 + first + index),
      actor: { kind: "system", id: "orchestration" },
      payload,
    }),
  );

const subtask = (subtaskId: string, dependsOn: string[], needsApproval = false) => ({
  subtaskId,
  title: `Step ${subtaskId.slice(-2)}`,
  instructions: "Do the step",
  agentId: agent,
  dependsOn,
  needsApproval,
  estimatedCostMicroUsd: 1000,
});

const plan = [subtask(a, []), subtask(b, [a], true), subtask(c, [a])];
const run = { taskId, runId };
const usage = (cost: number) => ({ inputTokens: 10, outputTokens: 20, costMicroUsd: cost });
const settled = (callId: string, kind: string, cost: number, subtaskId?: string) => ({
  taskId,
  callId,
  kind,
  ...(subtaskId === undefined ? {} : { subtaskId }),
  ...usage(cost),
  text: "out",
});
const called = (callId: string, kind: string) => ({
  taskId,
  callId,
  agentId: agent,
  agentVersion: 1,
  messages: [{ role: "user", content: "Plan" }],
  models: [model],
  maxTokens: 100,
  maxDurationMs: 60000,
  kind,
});

const full: [string, object][] = [
  [
    "orchestration.TaskSubmitted",
    {
      taskId,
      text: "Write a report",
      mode: "approval",
      conversationId,
      messageId,
      model: taskModel,
      effort: "high",
    },
  ],
  ["orchestration.CallRequested", called(planCall, "plan")],
  ["orchestration.CallSettled", settled(planCall, "plan", 100)],
  ["orchestration.PlanProposed", { taskId, planVersion: 1, subtasks: plan, awaitsApproval: true }],
  [
    "orchestration.SubtaskReassigned",
    { ...run, subtaskId: c, agentId: otherAgent, model: "openai/gpt-4.1-mini" },
  ],
  [
    "orchestration.MessageRefused",
    { taskId, conversationId, messageId: id(53), intent: "steer", reason: "No run" },
  ],
  ["orchestration.PlanApproved", { taskId, planVersion: 1 }],
  ["orchestration.RunStarted", run],
  ["orchestration.SubtaskQueued", { ...run, subtaskId: a }],
  ["orchestration.SubtaskWaiting", { ...run, subtaskId: c }],
  ["orchestration.SubtaskWaiting", { ...run, subtaskId: b }],
  [
    "orchestration.SubtaskStarted",
    { ...run, subtaskId: a, tryNumber: 1, callId: callA1, agentId: agent, agentVersion: 1 },
  ],
  ["orchestration.CallRequested", { ...called(callA1, "subtask"), subtaskId: a }],
  [
    "orchestration.SubtaskFailed",
    { ...run, subtaskId: a, tryNumber: 1, code: "unavailable", reason: "Down", ...usage(50) },
  ],
  ["orchestration.CallSettled", settled(callA1, "subtask", 50, a)],
  ["orchestration.SubtaskRetrying", { ...run, subtaskId: a, tryNumber: 2, retryAt: at(30) }],
  [
    "orchestration.SubtaskStarted",
    { ...run, subtaskId: a, tryNumber: 2, callId: callA2, agentId: agent, agentVersion: 1 },
  ],
  [
    "orchestration.SubtaskCompleted",
    { ...run, subtaskId: a, callId: callA2, text: "Part A", ...usage(70) },
  ],
  ["orchestration.CallSettled", settled(callA2, "subtask", 70, a)],
  [
    "orchestration.CheckpointOpened",
    { ...run, subtaskId: b, checkpointId, reason: "Approval needed for Step 12", deadline: at(90) },
  ],
  ["orchestration.SubtaskQueued", { ...run, subtaskId: c }],
  [
    "orchestration.SubtaskStarted",
    { ...run, subtaskId: c, tryNumber: 1, callId: callC, agentId: otherAgent, agentVersion: 1 },
  ],
  [
    "orchestration.CheckpointResolved",
    { ...run, checkpointId, decision: "approved", comment: "Go" },
  ],
  ["orchestration.RunPaused", run],
  ["orchestration.RunResumed", run],
  [
    "orchestration.SubtaskBlocked",
    { ...run, subtaskId: c, code: "budget_refused", reason: "Monthly cap reached" },
  ],
  ["orchestration.CallSettled", settled(callC, "subtask", 5, c)],
  [
    "orchestration.RunBlocked",
    { ...run, code: "budget_refused", reason: "Monthly cap reached: 19.80 of 20.00 USD" },
  ],
  ["orchestration.SteeringMessageAdded", { taskId, conversationId, messageId, text: "Hi", runId }],
  ["orchestration.RunResumed", run],
  ["orchestration.SubtaskQueued", { ...run, subtaskId: c }],
  ["orchestration.CallCancelRequested", { taskId, callId: callC }],
  ["orchestration.TaskModeChanged", { taskId, mode: "auto" }],
  ["orchestration.CallRequested", called(aggregateCall, "aggregate")],
  ["orchestration.RunCompleted", { ...run, summary: "The report", costMicroUsd: 9 }],
  ["orchestration.CallSettled", settled(aggregateCall, "aggregate", 25)],
];

interface State {
  doc: DetailDoc | null;
  outputs: Map<string, OutputDoc>;
  notices: number;
}

const replay = (events: readonly RegisteredEvent[], state?: State): State => {
  const next: State = state ?? { doc: null, outputs: new Map(), notices: 0 };
  for (const event of events) {
    const change = projectDetail(next.doc, event);
    if (!change) continue;
    next.doc = change.doc;
    for (const output of change.outputs) next.outputs.set(output._id, output);
    next.notices += 1;
    const data = change.notice.payload.data;
    const parsed = TaskDetailV1.safeParse(data);
    assert.ok(parsed.success, `${event.type}: ${JSON.stringify(parsed.error?.issues)}`);
    assert.deepEqual(parsed.data, data, event.type);
  }
  return next;
};

test("a full run gives a valid detail document after each event", () => {
  const events = stream(full);
  const { doc, outputs, notices } = replay(events);
  assert.ok(doc);
  assert.equal(notices, events.length - 5);
  assert.deepEqual(toDetail(doc), {
    taskId,
    title: "Write a report",
    status: "completed",
    mode: "auto",
    conversationId,
    model: taskModel,
    effort: "high",
    createdAt: at(1),
    updatedAt: at(36),
    plan: {
      planVersion: 1,
      subtasks: [
        plan[0],
        plan[1],
        { ...plan[2], agentId: otherAgent, model: "openai/gpt-4.1-mini" },
      ],
    },
    runId,
    currentCall: null,
    subtasks: [
      {
        subtaskId: a,
        status: "done",
        tries: 2,
        callId: callA2,
        startedAt: at(12),
        endedAt: at(18),
        inputTokens: 20,
        outputTokens: 40,
        costMicroUsd: 120,
      },
      {
        subtaskId: b,
        status: "queued",
        tries: 0,
        inputTokens: 0,
        outputTokens: 0,
        costMicroUsd: 0,
      },
      {
        subtaskId: c,
        status: "queued",
        tries: 1,
        callId: callC,
        startedAt: at(22),
        ...usage(5),
      },
    ],
    checkpoints: [
      {
        checkpointId,
        subtaskId: b,
        reason: "Approval needed for Step 12",
        status: "approved",
        comment: "Go",
        openedAt: at(20),
        deadline: at(90),
        resolvedAt: at(23),
      },
    ],
    costMicroUsd: 250,
    rev: events.length - 5,
    asOf: 36,
  });
  assert.deepEqual(
    [...outputs.values()].map((output) => TaskOutputV1.parse(toOutput(output))),
    [
      { taskId, runId, subtaskId: a, text: "Part A" },
      { taskId, runId, text: "The report" },
    ],
  );
  assert.equal(outputs.get(outputId(taskId))?.text, "The report");
});

test("each step shows the status table and the current call", () => {
  const events = stream(full);
  const statusAt = (count: number) => replay(events.slice(0, count)).doc;
  assert.deepEqual(statusAt(2)?.currentCall, { callId: planCall, kind: "plan" });
  assert.equal(statusAt(3)?.currentCall, null);
  assert.equal(statusAt(3)?.costMicroUsd, 100);
  assert.equal(statusAt(4)?.status, "awaiting_approval");
  assert.equal(statusAt(7)?.status, "awaiting_approval");
  assert.equal(statusAt(8)?.status, "running");
  assert.equal(statusAt(13)?.currentCall, null);
  const failed = statusAt(14)?.subtasks[0];
  assert.equal(failed?.status, "failed");
  assert.equal(failed.reason, "Down");
  assert.equal(failed.endedAt, at(14));
  assert.equal(statusAt(16)?.subtasks[0]?.retryAt, at(30));
  const restarted = statusAt(17)?.subtasks[0];
  assert.equal(restarted?.status, "running");
  assert.equal(restarted.reason, undefined);
  assert.equal(restarted.endedAt, undefined);
  assert.equal(statusAt(20)?.subtasks[1]?.status, "awaiting_approval");
  assert.equal(statusAt(20)?.checkpoints[0]?.status, "open");
  assert.equal(statusAt(24)?.status, "paused");
  const blocked = statusAt(28);
  assert.equal(blocked?.status, "blocked");
  assert.equal(blocked.reason, "Monthly cap reached: 19.80 of 20.00 USD");
  assert.equal(blocked.subtasks[2]?.status, "blocked");
  assert.equal(statusAt(30)?.reason, undefined);
  assert.equal(statusAt(30)?.status, "running");
  assert.deepEqual(statusAt(34)?.currentCall, { callId: aggregateCall, kind: "aggregate" });
  assert.equal(statusAt(35)?.currentCall, null);
});

test("RunCancelled and RunFailed end the open subtasks", () => {
  const start = stream(full.slice(0, 11));
  const cancelled = replay(
    stream(
      [
        [
          "orchestration.SubtaskStarted",
          { ...run, subtaskId: a, tryNumber: 1, callId: callA1, agentId: agent, agentVersion: 1 },
        ],
        ["orchestration.RunCancelled", { ...run, reason: "Not needed" }],
      ],
      `task-${taskId}`,
      12,
    ),
    replay(start),
  ).doc;
  assert.equal(cancelled?.status, "cancelled");
  assert.equal(cancelled.reason, "Not needed");
  assert.deepEqual(
    cancelled.subtasks.map((state) => [state.status, state.endedAt]),
    [
      ["cancelled", at(13)],
      ["skipped", undefined],
      ["skipped", undefined],
    ],
  );
  const failed = replay(
    stream(
      [["orchestration.RunFailed", { ...run, code: "internal", reason: "Broken" }]],
      `task-${taskId}`,
      12,
    ),
    replay(start),
  ).doc;
  assert.equal(failed?.status, "failed");
  assert.deepEqual(
    failed.subtasks.map((state) => state.status),
    ["cancelled", "skipped", "skipped"],
  );
});

test("a done, failed or blocked subtask stays unchanged at RunCancelled or RunFailed", () => {
  const ended = replay(
    stream(
      [
        [
          "orchestration.SubtaskStarted",
          { ...run, subtaskId: c, tryNumber: 1, callId: callC, agentId: agent, agentVersion: 1 },
        ],
        [
          "orchestration.SubtaskFailed",
          { ...run, subtaskId: c, tryNumber: 1, code: "unavailable", reason: "Down", ...usage(0) },
        ],
        [
          "orchestration.SubtaskBlocked",
          { ...run, subtaskId: b, code: "forbidden", reason: "No permission" },
        ],
      ],
      `task-${taskId}`,
      19,
    ),
    replay(stream(full.slice(0, 18))),
  );
  assert.ok(ended.doc);
  const before = ended.doc.subtasks;
  assert.deepEqual(
    before.map((state) => state.status),
    ["done", "blocked", "failed"],
  );
  for (const [type, payload] of [
    ["orchestration.RunCancelled", run],
    ["orchestration.RunFailed", { ...run, code: "internal", reason: "Broken" }],
  ] as [string, object][]) {
    const [event] = stream([[type, payload]], `task-${taskId}`, 22);
    assert.ok(event);
    const change = projectDetail(ended.doc, event);
    assert.ok(change);
    assert.deepEqual(change.doc.subtasks, before, type);
  }
});

test("a refused or expired checkpoint keeps the subtask status until SubtaskFailed", () => {
  const opened = stream(full.slice(0, 11).concat([full[19] as [string, object]]));
  for (const decision of ["refused", "expired"]) {
    const { doc } = replay(
      stream(
        [
          ["orchestration.CheckpointResolved", { ...run, checkpointId, decision }],
          [
            "orchestration.SubtaskFailed",
            {
              ...run,
              subtaskId: b,
              tryNumber: 1,
              code: "forbidden",
              reason: "Refused",
              ...usage(0),
            },
          ],
        ],
        `task-${taskId}`,
        13,
      ),
      replay(opened),
    );
    assert.equal(doc?.checkpoints[0]?.status, decision);
    assert.equal(doc.subtasks.find((state) => state.subtaskId === b)?.status, "failed");
  }
  const unknown = stream(
    [["orchestration.CheckpointResolved", { ...run, checkpointId: id(99), decision: "approved" }]],
    `task-${taskId}`,
    13,
  );
  const before = replay(opened);
  assert.equal(projectDetail(before.doc, unknown[0] as RegisteredEvent), undefined);
});

test("the task statuses before a run", () => {
  const states = (steps: [string, object][]) => {
    const events = stream([full[0] as [string, object], ...steps]);
    return events.map((_event, index) => replay(events.slice(0, index + 1)).doc?.status);
  };
  assert.deepEqual(
    states([
      ["orchestration.ClarificationRequested", { taskId, questions: ["Which year?"] }],
      [
        "orchestration.ClarificationAnswered",
        { taskId, conversationId, messageId: id(54), text: "2026" },
      ],
      [
        "orchestration.PlanProposed",
        { taskId, planVersion: 1, subtasks: plan, awaitsApproval: false },
      ],
      ["orchestration.PlanEdited", { taskId, planVersion: 2, subtasks: plan.slice(0, 1) }],
      ["orchestration.PlanRejected", { taskId, planVersion: 2, reason: "Too long" }],
    ]),
    ["submitted", "clarifying", "planning", "planning", "awaiting_approval", "rejected"],
  );
  const cancelled = replay(
    stream([
      full[0] as [string, object],
      full[1] as [string, object],
      ["orchestration.TaskCancelled", { taskId }],
    ]),
  ).doc;
  assert.equal(cancelled?.status, "cancelled");
  assert.equal(cancelled.currentCall, null);
  assert.equal(cancelled.reason, undefined);
});

test("a task with no model and no effort is auto: the detail has neither field", () => {
  const { doc } = replay(
    stream([["orchestration.TaskSubmitted", { taskId, text: "Write a report", mode: "auto" }]]),
  );
  assert.ok(doc);
  assert.equal(Object.hasOwn(doc, "model"), false);
  assert.equal(Object.hasOwn(doc, "effort"), false);
});

test("an event before TaskSubmitted, an old version, or an event with no effect changes nothing", () => {
  const events = stream(full);
  assert.equal(projectDetail(null, events[7] as RegisteredEvent), undefined);
  const { doc } = replay(events.slice(0, 8));
  assert.equal(projectDetail(doc, events[3] as RegisteredEvent), undefined);
  const reassignNoPlan = stream([full[0] as [string, object], full[4] as [string, object]]);
  assert.equal(
    projectDetail(replay(reassignNoPlan.slice(0, 1)).doc, reassignNoPlan[1] as RegisteredEvent),
    undefined,
  );
  const m2Payload = Object.fromEntries(
    Object.entries(called(planCall, "plan")).filter(([key]) => key !== "kind"),
  );
  const m2 = stream([full[0] as [string, object], ["orchestration.CallRequested", m2Payload]]);
  assert.equal(projectDetail(replay(m2.slice(0, 1)).doc, m2[1] as RegisteredEvent), undefined);
});

test("SubtaskReassigned with no model removes the model of the plan", () => {
  const withModel = { ...plan[0], model };
  const { doc } = replay(
    stream([
      full[0] as [string, object],
      [
        "orchestration.PlanProposed",
        { taskId, planVersion: 1, subtasks: [withModel], awaitsApproval: true },
      ],
      ["orchestration.SubtaskReassigned", { ...run, subtaskId: a, agentId: otherAgent }],
    ]),
  );
  assert.deepEqual(doc?.plan?.subtasks[0], { ...plan[0], agentId: otherAgent });
});

test("the cost stops at the CapMicroUsd limit", () => {
  const { doc } = replay(
    stream([
      full[0] as [string, object],
      ["orchestration.CallSettled", settled(planCall, "plan", maxCostMicroUsd)],
      ["orchestration.CallSettled", settled(id(36), "check", maxCostMicroUsd)],
    ]),
  );
  assert.equal(doc?.costMicroUsd, maxCostMicroUsd);
});

test("the notice is a task:<id> ViewMessageV1 with TaskDetailV1 data", () => {
  const [submitted] = stream(full.slice(0, 1));
  assert.ok(submitted);
  const change = projectDetail(null, submitted);
  assert.ok(change);
  const message = ViewMessageV1.parse({
    ...change.notice,
    eventId: id(9001),
    streamId: `notice-${submitted.eventId}`,
    version: 1,
    occurredAt: at(2),
  });
  assert.equal(message.type, `view.task.${taskId}`);
  assert.equal(message.payload.topic, `task:${taskId}`);
  assert.equal(message.payload.commandId, submitted.causationId);
  assert.equal(message.payload.rev, 1);
});

const random = (seed: number) => () => {
  seed = (seed * 1103515245 + 12345) % 2147483648;
  return seed / 2147483648;
};

test("events of other streams in any order and redeliveries give the same document", () => {
  const task = stream(full);
  const llm = stream(
    [1, 2].map((attempt) => [
      "llm.CallFailed",
      {
        taskId,
        callId: callA1,
        attempt,
        model,
        errorClass: "cancelled",
        reason: "Stopped",
        costMicroUsd: 3,
      },
    ]),
    `call-${callA1}`,
  );
  const conversation = stream(
    [
      [
        "conversation.OwnerMessagePosted",
        { conversationId, messageId, text: "Write a report", intent: "new_task" },
      ],
    ],
    `conversation-${conversationId}`,
  );
  const expected = replay(task);
  for (let seed = 1; seed <= 50; seed += 1) {
    const next = random(seed);
    const queues = [[...task], [...llm], [...conversation]];
    const merged: RegisteredEvent[] = [];
    while (queues.some((queue) => queue.length > 0)) {
      const open = queues.filter((queue) => queue.length > 0);
      const queue = open[Math.floor(next() * open.length)] ?? [];
      const event = queue.shift();
      if (!event) continue;
      merged.push(event);
      if (next() < 0.2) merged.push(event);
      if (next() < 0.1 && merged.length > 2)
        merged.push(merged[Math.floor(next() * merged.length)] ?? event);
    }
    const state = replay(merged);
    assert.deepEqual(state.doc, expected.doc, `seed ${String(seed)}`);
    assert.deepEqual(state.outputs, expected.outputs, `seed ${String(seed)}`);
  }
});
