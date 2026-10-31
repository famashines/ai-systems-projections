import type { RegisteredEvent, TaskListItemV1, TaskStatus } from "@famashines/ai-systems-contracts";
import { noticeOf, type Notice } from "./notices.ts";

export interface TaskDoc {
  _id: string;
  title: string;
  status: TaskStatus;
  createdAt: Date;
  updatedAt: Date;
  rev: number;
  asOf: number;
}

export interface Change {
  doc: TaskDoc;
  notice: Notice;
}

const graphemes = new Intl.Segmenter("en", { granularity: "grapheme" });
const unsafe = /[\p{Cc}\p{Cs}\u2028\u2029\u061C\u200E\u200F\u202A-\u202E\u2066-\u2069]/gu;
const maxTitleLength = 200;
export const noTitle = "(no title)";

function firstUnits(segment: string): string {
  let part = "";
  for (const codePoint of segment) {
    if (part.length + codePoint.length > maxTitleLength) break;
    part += codePoint;
  }
  return part;
}

export function titleOf(text: string): string {
  let title = "";
  let count = 0;
  for (const { segment } of graphemes.segment(text.replace(unsafe, " ").trim())) {
    if (count === 80) break;
    if (title.length + segment.length > maxTitleLength) {
      if (count === 0) title = firstUnits(segment);
      break;
    }
    title += segment;
    count += 1;
  }
  return title.trimEnd() || noTitle;
}

export const toItem = (doc: TaskDoc): TaskListItemV1 => ({
  taskId: doc._id,
  title: doc.title,
  status: doc.status,
  createdAt: doc.createdAt.toISOString(),
  updatedAt: doc.updatedAt.toISOString(),
  rev: doc.rev,
  asOf: doc.asOf,
});

const statusAfter: Partial<Record<RegisteredEvent["type"], TaskStatus>> = {
  "orchestration.RunStarted": "running",
  "orchestration.RunBlocked": "blocked",
  "orchestration.RunCompleted": "completed",
  "orchestration.RunFailed": "failed",
};

export const taskIdOf = (event: RegisteredEvent): string | undefined =>
  event.type === "orchestration.TaskSubmitted" || statusAfter[event.type] !== undefined
    ? (event.payload as { taskId: string }).taskId
    : undefined;

export function project(current: TaskDoc | null, event: RegisteredEvent): Change | undefined {
  if (current && event.version <= current.asOf) return undefined;
  const at = new Date(event.occurredAt);
  const rev = (current?.rev ?? 0) + 1;
  let doc: TaskDoc;
  if (event.type === "orchestration.TaskSubmitted") {
    doc = {
      _id: event.payload.taskId,
      title: titleOf(event.payload.text),
      status: "submitted",
      createdAt: current?.createdAt ?? at,
      updatedAt: at,
      rev,
      asOf: event.version,
    };
  } else {
    const status = statusAfter[event.type];
    if (!current || status === undefined) return undefined;
    doc = { ...current, status, updatedAt: at, rev, asOf: event.version };
  }
  const commandId = event.type === "orchestration.TaskSubmitted" ? event.causationId : undefined;
  return { doc, notice: noticeOf(event, "tasks", doc, toItem(doc), commandId) };
}
