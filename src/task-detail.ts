import type {
  CheckpointV1,
  RegisteredEvent,
  SubtaskStateV1,
  TaskDetailV1,
  TaskOutputV1,
} from "@famashines/ai-systems-contracts";
import { noticeOf, type Notice } from "./notices.ts";
import { statusAfter, titleOf } from "./tasks.ts";

export type DetailDoc = TaskDetailV1 & { _id: string };
export type OutputDoc = TaskOutputV1 & { _id: string };

export interface DetailChange {
  doc: DetailDoc;
  outputs: OutputDoc[];
  notice: Notice;
}

export const maxCostMicroUsd = 1_000_000_000_000;

export const outputId = (taskId: string, subtaskId?: string): string =>
  subtaskId === undefined ? taskId : `${taskId}:${subtaskId}`;

type Subtask = SubtaskStateV1;

const without = <T extends object, K extends keyof T>(value: T, ...keys: K[]): Omit<T, K> =>
  Object.fromEntries(Object.entries(value).filter(([key]) => !keys.includes(key as K))) as Omit<
    T,
    K
  >;

export const toDetail = (doc: DetailDoc): TaskDetailV1 => without(doc, "_id");
export const toOutput = (doc: OutputDoc): TaskOutputV1 => without(doc, "_id");

const withReason = <T extends { reason?: string }>(value: T, reason?: string): T =>
  reason === undefined ? (without(value, "reason") as T) : { ...value, reason };

const planIndex = (doc: DetailDoc, subtaskId: string): number => {
  const index = doc.plan?.subtasks.findIndex((subtask) => subtask.subtaskId === subtaskId) ?? -1;
  return index === -1 ? Number.MAX_SAFE_INTEGER : index;
};

const inPlanOrder = <T extends { subtaskId: string }>(doc: DetailDoc, items: T[]): T[] =>
  items.toSorted((a, b) => planIndex(doc, a.subtaskId) - planIndex(doc, b.subtaskId));

function patchSubtask(
  doc: DetailDoc,
  subtaskId: string,
  patch: (subtask: Subtask) => Subtask,
): DetailDoc {
  const found = doc.subtasks.find((subtask) => subtask.subtaskId === subtaskId);
  const fresh: Subtask = {
    subtaskId,
    status: "queued",
    tries: 0,
    inputTokens: 0,
    outputTokens: 0,
    costMicroUsd: 0,
  };
  const next = patch(found ?? fresh);
  const others = doc.subtasks.filter((subtask) => subtask.subtaskId !== subtaskId);
  return { ...doc, subtasks: inPlanOrder(doc, [...others, next]) };
}

const endRun = (subtask: Subtask, at: string): Subtask => {
  if (subtask.status === "waiting") return { ...subtask, status: "skipped" };
  if (["running", "queued", "retrying", "awaiting_approval"].includes(subtask.status)) {
    return { ...subtask, status: "cancelled", endedAt: at };
  }
  return subtask;
};

const capped = (sum: number): number => Math.min(sum, maxCostMicroUsd);

function apply(doc: DetailDoc, event: RegisteredEvent): DetailDoc | null {
  const at = event.occurredAt;
  switch (event.type) {
    case "orchestration.ClarificationRequested":
    case "orchestration.ClarificationAnswered":
    case "orchestration.RunPaused":
      return doc;
    case "orchestration.PlanProposed":
    case "orchestration.PlanEdited": {
      const { planVersion, subtasks } = event.payload;
      return { ...doc, plan: { planVersion, subtasks } };
    }
    case "orchestration.PlanRejected":
      return withReason({ ...doc, currentCall: null }, event.payload.reason);
    case "orchestration.TaskCancelled":
      return withReason({ ...doc, currentCall: null }, event.payload.reason);
    case "orchestration.TaskModeChanged":
      return { ...doc, mode: event.payload.mode };
    case "orchestration.RunStarted":
      return withReason({ ...doc, runId: event.payload.runId, subtasks: [], checkpoints: [] });
    case "orchestration.RunResumed":
      return withReason(doc);
    case "orchestration.RunBlocked":
      return withReason(doc, event.payload.reason);
    case "orchestration.RunCompleted":
      return withReason({ ...doc, currentCall: null });
    case "orchestration.RunFailed":
    case "orchestration.RunCancelled":
      return withReason(
        { ...doc, currentCall: null, subtasks: doc.subtasks.map((subtask) => endRun(subtask, at)) },
        event.payload.reason,
      );
    case "orchestration.CallRequested": {
      const { callId, kind } = event.payload;
      if (kind === undefined || kind === "subtask") return null;
      return { ...doc, currentCall: { callId, kind } };
    }
    case "orchestration.CallSettled": {
      const { callId, subtaskId, inputTokens, outputTokens, costMicroUsd } = event.payload;
      const settled: DetailDoc = {
        ...doc,
        currentCall: doc.currentCall?.callId === callId ? null : doc.currentCall,
        costMicroUsd: capped(doc.costMicroUsd + costMicroUsd),
      };
      if (subtaskId === undefined) return settled;
      if (!doc.subtasks.some((subtask) => subtask.subtaskId === subtaskId)) return settled;
      return patchSubtask(settled, subtaskId, (subtask) => ({
        ...subtask,
        inputTokens: subtask.inputTokens + inputTokens,
        outputTokens: subtask.outputTokens + outputTokens,
        costMicroUsd: capped(subtask.costMicroUsd + costMicroUsd),
      }));
    }
    case "orchestration.SubtaskWaiting":
      return patchSubtask(doc, event.payload.subtaskId, (subtask) => ({
        ...subtask,
        status: "waiting",
      }));
    case "orchestration.SubtaskQueued":
      return patchSubtask(doc, event.payload.subtaskId, (subtask) => ({
        ...without(subtask, "endedAt", "retryAt", "reason"),
        status: "queued",
      }));
    case "orchestration.SubtaskStarted": {
      const { subtaskId, tryNumber, callId } = event.payload;
      return patchSubtask(doc, subtaskId, (subtask) => ({
        ...without(subtask, "endedAt", "retryAt", "reason"),
        status: "running",
        tries: tryNumber,
        callId,
        startedAt: subtask.startedAt ?? at,
      }));
    }
    case "orchestration.SubtaskCompleted":
      return patchSubtask(doc, event.payload.subtaskId, (subtask) => ({
        ...without(subtask, "retryAt", "reason"),
        status: "done",
        callId: event.payload.callId,
        endedAt: at,
      }));
    case "orchestration.SubtaskFailed": {
      const { subtaskId, tryNumber, reason } = event.payload;
      return patchSubtask(doc, subtaskId, (subtask) => ({
        ...without(subtask, "retryAt"),
        status: "failed",
        tries: tryNumber,
        endedAt: at,
        reason,
      }));
    }
    case "orchestration.SubtaskRetrying":
      return patchSubtask(doc, event.payload.subtaskId, (subtask) => ({
        ...subtask,
        status: "retrying",
        retryAt: event.payload.retryAt,
      }));
    case "orchestration.SubtaskBlocked":
      return patchSubtask(doc, event.payload.subtaskId, (subtask) => ({
        ...subtask,
        status: "blocked",
        reason: event.payload.reason,
      }));
    case "orchestration.SubtaskReassigned": {
      const { subtaskId, agentId, model } = event.payload;
      if (!doc.plan?.subtasks.some((subtask) => subtask.subtaskId === subtaskId)) return null;
      const subtasks = doc.plan.subtasks.map((subtask) =>
        subtask.subtaskId !== subtaskId
          ? subtask
          : { ...without(subtask, "model"), agentId, ...(model === undefined ? {} : { model }) },
      );
      return { ...doc, plan: { ...doc.plan, subtasks } };
    }
    case "orchestration.CheckpointOpened": {
      const { subtaskId, checkpointId, reason, deadline } = event.payload;
      const checkpoint: CheckpointV1 = {
        checkpointId,
        subtaskId,
        reason,
        status: "open",
        openedAt: at,
        ...(deadline === undefined ? {} : { deadline }),
      };
      const others = doc.checkpoints.filter((item) => item.subtaskId !== subtaskId);
      const opened = { ...doc, checkpoints: inPlanOrder(doc, [...others, checkpoint]) };
      return patchSubtask(opened, subtaskId, (subtask) => ({
        ...subtask,
        status: "awaiting_approval",
      }));
    }
    case "orchestration.CheckpointResolved": {
      const { checkpointId, decision, comment } = event.payload;
      const found = doc.checkpoints.find((item) => item.checkpointId === checkpointId);
      if (!found) return null;
      const resolved: CheckpointV1 = {
        ...without(found, "comment"),
        status: decision,
        resolvedAt: at,
        ...(comment === undefined ? {} : { comment }),
      };
      const next = {
        ...doc,
        checkpoints: doc.checkpoints.map((item) => (item === found ? resolved : item)),
      };
      if (decision !== "approved") return next;
      return patchSubtask(next, found.subtaskId, (subtask) => ({ ...subtask, status: "queued" }));
    }
    default:
      return null;
  }
}

const outputsOf = (event: RegisteredEvent): OutputDoc[] => {
  if (event.type === "orchestration.RunCompleted") {
    const { taskId, runId, summary } = event.payload;
    return [{ _id: outputId(taskId), taskId, runId, text: summary }];
  }
  if (event.type === "orchestration.SubtaskCompleted") {
    const { taskId, runId, subtaskId, text } = event.payload;
    return [{ _id: outputId(taskId, subtaskId), taskId, runId, subtaskId, text }];
  }
  return [];
};

function submittedDoc(current: DetailDoc | null, event: RegisteredEvent): DetailDoc | null {
  if (event.type !== "orchestration.TaskSubmitted") return null;
  const { taskId, text, mode, conversationId } = event.payload;
  return {
    _id: taskId,
    taskId,
    title: titleOf(text),
    status: "submitted",
    mode,
    ...(conversationId === undefined ? {} : { conversationId }),
    createdAt: current?.createdAt ?? event.occurredAt,
    updatedAt: event.occurredAt,
    currentCall: null,
    subtasks: [],
    checkpoints: [],
    costMicroUsd: 0,
    rev: current?.rev ?? 0,
    asOf: event.version,
  };
}

export function projectDetail(
  current: DetailDoc | null,
  event: RegisteredEvent,
): DetailChange | undefined {
  if (current && event.version <= current.asOf) return undefined;
  const changed = submittedDoc(current, event) ?? (current && apply(current, event));
  if (!changed) return undefined;
  const doc: DetailDoc = {
    ...changed,
    status: statusAfter(event) ?? changed.status,
    updatedAt: event.occurredAt,
    rev: (current?.rev ?? 0) + 1,
    asOf: event.version,
  };
  const commandId = event.type === "orchestration.TaskSubmitted" ? event.causationId : undefined;
  return {
    doc,
    outputs: outputsOf(event),
    notice: noticeOf(event, `task:${doc._id}`, doc, toDetail(doc), commandId),
  };
}
