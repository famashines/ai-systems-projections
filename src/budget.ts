import {
  BudgetMonth,
  type BudgetMonthV1,
  type RegisteredEvent,
} from "@famashines/ai-systems-contracts";
import { noticeOf, type Notice } from "./notices.ts";
import type { Patch } from "./chat.ts";

export interface Cap {
  hard: number;
  soft?: number;
}

export interface CapDoc {
  _id: "month";
  cap: Cap;
  rev: number;
  asOf: number;
}

export interface MonthDoc {
  _id: string;
  spent: number;
  reserved: number;
  cap?: Cap;
  monthVersion: number;
  capVersion: number;
  rev: number;
}

export interface BudgetChange {
  cap?: Patch<CapDoc>;
  months: Patch<MonthDoc>[];
  notices: Notice[];
}

export const capStreamId = "budget-cap-month";
const monthPrefix = "budget-month-";

export function monthKey(date: Date, timeZone: string): string {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
  }).formatToParts(date);
  const part = (type: string) => parts.find((item) => item.type === type)?.value ?? "";
  return `${part("year")}-${part("month")}`;
}

export const monthOf = (streamId: string): string | undefined =>
  streamId.startsWith(monthPrefix)
    ? BudgetMonth.safeParse(streamId.slice(monthPrefix.length)).data
    : undefined;

export function toView(doc: MonthDoc): BudgetMonthV1 {
  const { spent, reserved, cap } = doc;
  const base = {
    month: doc._id,
    spentMicroUsd: spent,
    reservedMicroUsd: reserved,
    rev: doc.rev,
    asOf: doc.monthVersion > 0 ? doc.monthVersion : doc.capVersion,
  };
  if (cap === undefined) {
    return { ...base, capMicroUsd: null, remainingMicroUsd: 0, overCapMicroUsd: 0 };
  }
  return {
    ...base,
    capMicroUsd: cap.hard,
    ...(cap.soft === undefined ? {} : { softCapMicroUsd: cap.soft }),
    remainingMicroUsd: Math.max(0, cap.hard - spent - reserved),
    overCapMicroUsd: Math.max(0, spent - cap.hard),
  };
}

export const emptyMonth = (month: string, cap: CapDoc | null): MonthDoc => ({
  _id: month,
  spent: 0,
  reserved: 0,
  ...(cap ? { cap: cap.cap } : {}),
  monthVersion: 0,
  capVersion: cap?.asOf ?? 0,
  rev: 0,
});

const noticesOf = (event: RegisteredEvent, months: Patch<MonthDoc>[]): Notice[] =>
  months.map(({ after }) => noticeOf(event, "budget", after, toView(after)));

export function setCap(
  current: CapDoc | null,
  months: readonly MonthDoc[],
  event: RegisteredEvent,
  timeZone: string,
): BudgetChange | undefined {
  if (event.type !== "budget.BudgetSet" || event.payload.scope.kind !== "month") return undefined;
  if (current && event.version <= current.asOf) return undefined;
  const { hardCapMicroUsd, softCapMicroUsd } = event.payload;
  const cap: Cap =
    softCapMicroUsd === undefined
      ? { hard: hardCapMicroUsd }
      : { hard: hardCapMicroUsd, soft: softCapMicroUsd };
  const after: CapDoc = { _id: "month", cap, rev: (current?.rev ?? 0) + 1, asOf: event.version };
  const month = monthKey(new Date(event.occurredAt), timeZone);
  const all: (MonthDoc | null)[] = months.some((doc) => doc._id === month)
    ? [...months]
    : [...months, null];
  const patches = all.map((before) => ({
    before,
    after: {
      ...(before ?? emptyMonth(month, null)),
      cap,
      capVersion: event.version,
      rev: (before?.rev ?? 0) + 1,
    },
  }));
  return { cap: { before: current, after }, months: patches, notices: noticesOf(event, patches) };
}

export function bookMonth(
  current: MonthDoc | null,
  cap: CapDoc | null,
  event: RegisteredEvent,
): BudgetChange | undefined {
  const month = monthOf(event.streamId);
  if (month === undefined || (current && event.version <= current.monthVersion)) return undefined;
  const doc = current ?? emptyMonth(month, cap);
  let { spent, reserved } = doc;
  switch (event.type) {
    case "budget.FundsReserved":
      reserved += event.payload.amountMicroUsd;
      break;
    case "budget.ReservationSettled":
      reserved -= event.payload.reservedMicroUsd;
      spent += event.payload.actualMicroUsd;
      break;
    case "budget.ReservationExpired":
      reserved -= event.payload.amountMicroUsd;
      spent += event.payload.amountMicroUsd;
      break;
    default:
      return undefined;
  }
  const patches = [
    {
      before: current,
      after: { ...doc, spent, reserved, monthVersion: event.version, rev: doc.rev + 1 },
    },
  ];
  return { months: patches, notices: noticesOf(event, patches) };
}
