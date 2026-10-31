import {
  viewSubject,
  type RegisteredEvent,
  type Topic,
  type ViewMessageV1,
} from "@famashines/ai-systems-contracts";
import type { NewEvent } from "@famashines/ai-systems-kit/event-store";

export type Notice = NewEvent<ViewMessageV1>;

export interface Doc {
  _id: string;
  rev: number;
}

export const noticeOf = (
  event: RegisteredEvent,
  topic: Topic,
  doc: Doc,
  data: unknown,
  commandId?: string,
): Notice => ({
  type: viewSubject(topic),
  schemaVersion: 1,
  correlationId: event.correlationId,
  causationId: event.eventId,
  actor: { kind: "system", id: "projections" },
  payload: {
    topic,
    docId: doc._id,
    rev: doc.rev,
    data,
    ...(commandId === undefined ? {} : { commandId }),
  },
});
