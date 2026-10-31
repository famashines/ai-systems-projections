import type {
  ConversationMessageV1,
  MessageIntent,
  RegisteredEvent,
} from "@famashines/ai-systems-contracts";
import { noticeOf, type Notice } from "./notices.ts";

export interface MessageDoc {
  _id: string;
  conversationId: string;
  role: "owner" | "agent";
  text: string;
  createdAt: Date;
  intent?: MessageIntent;
  taskId?: string;
  runId?: string;
  costMicroUsd?: number;
  commandId?: string;
  rev: number;
  asOf: number;
}

export interface LinkDoc {
  _id: string;
  taskId?: string;
  rev: number;
}

export interface CostDoc {
  _id: string;
  calls: Record<string, number>;
  rev: number;
}

export interface Patch<D> {
  before: D | null;
  after: D;
}

export interface ChatChange {
  messages: Patch<MessageDoc>[];
  link?: Patch<LinkDoc>;
  cost?: Patch<CostDoc>;
  notices: Notice[];
}

export const pageBytes = 900 * 1024;

export const toMessage = (doc: MessageDoc): ConversationMessageV1 => {
  const shown = {
    messageId: doc._id,
    text: doc.text,
    createdAt: doc.createdAt.toISOString(),
    rev: doc.rev,
    asOf: doc.asOf,
  };
  if (doc.role === "owner") {
    return {
      ...shown,
      role: "owner",
      intent: doc.intent ?? "new_task",
      ...(doc.taskId === undefined ? {} : { taskId: doc.taskId }),
    };
  }
  return {
    ...shown,
    role: "agent",
    taskId: doc.taskId ?? "",
    ...(doc.costMicroUsd === undefined ? {} : { costMicroUsd: doc.costMicroUsd }),
  };
};

const noticeFor = (event: RegisteredEvent, doc: MessageDoc): Notice =>
  noticeOf(event, `conversation:${doc.conversationId}`, doc, toMessage(doc), doc.commandId);

const touch = <D extends { rev: number }>(before: D | null, fresh: D): Patch<D> => ({
  before,
  after: { ...(before ?? fresh), rev: (before?.rev ?? 0) + 1 },
});

const count = (cost: CostDoc | null): number => Object.keys(cost?.calls ?? {}).length;
const total = (cost: CostDoc | null): number =>
  Object.values(cost?.calls ?? {}).reduce((sum, value) => sum + value, 0);

const withCost = (doc: MessageDoc, cost: CostDoc | null): MessageDoc =>
  count(cost) === 0 ? doc : { ...doc, costMicroUsd: total(cost), rev: 1 + count(cost) };

export const callKey = (callId: string, attempt: number): string => `${callId}:${String(attempt)}`;

export function postMessage(
  existing: MessageDoc | null,
  link: LinkDoc | null,
  cost: CostDoc | null,
  event: RegisteredEvent,
): ChatChange | undefined {
  if (existing) return undefined;
  const base = { createdAt: new Date(event.occurredAt), asOf: event.version };
  if (event.type === "conversation.OwnerMessagePosted") {
    const { conversationId, messageId, text, intent } = event.payload;
    const taskId = event.payload.taskId ?? link?.taskId;
    const doc: MessageDoc = {
      _id: messageId,
      conversationId,
      role: "owner",
      text,
      intent,
      ...(taskId === undefined ? {} : { taskId }),
      commandId: event.causationId,
      ...base,
      rev: event.payload.taskId === undefined && link?.taskId !== undefined ? 2 : 1,
    };
    return {
      messages: [{ before: null, after: doc }],
      ...(intent === "new_task" ? { link: touch(link, { _id: messageId, rev: 0 }) } : {}),
      notices: [noticeFor(event, doc)],
    };
  }
  if (event.type === "conversation.AgentReplyPosted") {
    const { conversationId, messageId, text, taskId, runId } = event.payload;
    const plain: MessageDoc = {
      _id: messageId,
      conversationId,
      role: "agent",
      text,
      taskId,
      ...base,
      rev: 1,
    };
    const doc = runId === undefined ? plain : withCost({ ...plain, runId }, cost);
    return {
      messages: [{ before: null, after: doc }],
      ...(runId === undefined ? {} : { cost: touch(cost, { _id: taskId, calls: {}, rev: 0 }) }),
      notices: [noticeFor(event, doc)],
    };
  }
  return undefined;
}

export function linkTask(
  link: LinkDoc | null,
  message: MessageDoc | null,
  event: RegisteredEvent,
): ChatChange | undefined {
  if (event.type !== "orchestration.TaskSubmitted") return undefined;
  const { taskId, messageId } = event.payload;
  if (messageId === undefined || link?.taskId !== undefined) return undefined;
  const after: LinkDoc = { _id: messageId, taskId, rev: (link?.rev ?? 0) + 1 };
  const patched =
    message?.role === "owner" && message.taskId === undefined
      ? { ...message, taskId, rev: message.rev + 1 }
      : undefined;
  return {
    link: { before: link, after },
    messages: patched ? [{ before: message, after: patched }] : [],
    notices: patched ? [noticeFor(event, patched)] : [],
  };
}

export function bookCost(
  cost: CostDoc | null,
  replies: readonly MessageDoc[],
  event: RegisteredEvent,
): ChatChange | undefined {
  if (event.type !== "llm.CallCompleted" && event.type !== "llm.CallFailed") return undefined;
  const { taskId, callId, attempt, costMicroUsd } = event.payload;
  const key = callKey(callId, attempt);
  if (cost && Object.hasOwn(cost.calls, key)) return undefined;
  const after: CostDoc = {
    _id: taskId,
    calls: { ...cost?.calls, [key]: costMicroUsd },
    rev: (cost?.rev ?? 0) + 1,
  };
  const messages = replies
    .filter((reply) => reply.taskId === taskId && reply.runId !== undefined)
    .map((reply) => ({ before: reply, after: withCost(reply, after) }));
  return {
    cost: { before: cost, after },
    messages,
    notices: messages.map(({ after: doc }) => noticeFor(event, doc)),
  };
}

export function fitPage<T>(items: readonly T[], headSeq: number, limitBytes = pageBytes): T[] {
  let size = Buffer.byteLength(JSON.stringify({ items: [], headSeq }));
  const page: T[] = [];
  for (const item of items) {
    const bytes = Buffer.byteLength(JSON.stringify(item)) + (page.length > 0 ? 1 : 0);
    if (size + bytes > limitBytes) break;
    size += bytes;
    page.push(item);
  }
  return page;
}
