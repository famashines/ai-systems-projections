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

export const titleOf = (text: string): string =>
  Array.from(text.trim()).slice(0, 80).join("").trimEnd();

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
