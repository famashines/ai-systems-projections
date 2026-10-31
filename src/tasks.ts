import {
  viewSubject,
  type RegisteredEvent,
  type TaskListItemV1,
  type TaskStatus,
  type ViewMessageV1,
} from "@famashines/ai-systems-contracts";
import type { NewEvent } from "@famashines/ai-systems-kit/event-store";

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
  notice: NewEvent<ViewMessageV1>;
}

const graphemes = new Intl.Segmenter("en", { granularity: "grapheme" });
const unsafe = /[\p{Cc}\u2028\u2029\u061C\u200E\u200F\u202A-\u202E\u2066-\u2069]/gu;
const maxTitleLength = 200;

export function titleOf(text: string): string {
  let title = "";
  let count = 0;
  for (const { segment } of graphemes.segment(text.replace(unsafe, " ").trim())) {
    if (count === 80 || title.length + segment.length > maxTitleLength) break;
    title += segment;
    count += 1;
  }
  return title.trimEnd();
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

export const taskIdOf = (event: RegisteredEvent): string => event.payload.taskId;

export function project(current: TaskDoc | null, event: RegisteredEvent): Change | undefined {
  if (current && event.version <= current.asOf) return undefined;
  const at = new Date(event.occurredAt);
  const doc: TaskDoc = {
    _id: taskIdOf(event),
    title: titleOf(event.payload.text),
    status: "submitted",
    createdAt: current?.createdAt ?? at,
    updatedAt: at,
    rev: (current?.rev ?? 0) + 1,
    asOf: event.version,
  };
  const notice: NewEvent<ViewMessageV1> = {
    type: viewSubject("tasks"),
    schemaVersion: 1,
    correlationId: event.correlationId,
    causationId: event.eventId,
    actor: { kind: "system", id: "projections" },
    payload: {
      topic: "tasks",
      docId: doc._id,
      rev: doc.rev,
      data: toItem(doc),
      commandId: event.causationId,
    },
  };
  return { doc, notice };
}
