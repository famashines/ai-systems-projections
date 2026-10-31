import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { BudgetMonthV1, parseEvent, type RegisteredEvent } from "@famashines/ai-systems-contracts";
import {
  bookMonth,
  monthKey,
  monthOf,
  setCap,
  toView,
  type CapDoc,
  type MonthDoc,
} from "./budget.ts";

const zone = "Europe/Berlin";
const reservationId = randomUUID();

const event = (
  type: string,
  streamId: string,
  version: number,
  payload: object,
  occurredAt = "2026-10-15T10:00:00.000Z",
): RegisteredEvent =>
  parseEvent({
    eventId: randomUUID(),
    type,
    schemaVersion: 1,
    streamId,
    version,
    occurredAt,
    correlationId: randomUUID(),
    causationId: randomUUID(),
    actor: { kind: "system", id: "budget" },
    payload,
  });

const capSet = (version: number, hard: number, occurredAt?: string, soft?: number) =>
  event(
    "budget.BudgetSet",
    "budget-cap-month",
    version,
    {
      scope: { kind: "month" },
      hardCapMicroUsd: hard,
      ...(soft === undefined ? {} : { softCapMicroUsd: soft }),
    },
    occurredAt,
  );

const october = "budget-month-2026-10";
const reserved = (version: number, amount: number) =>
  event("budget.FundsReserved", october, version, {
    reservationId,
    taskId: randomUUID(),
    agentId: randomUUID(),
    scopes: [{ kind: "month", key: "2026-10" }],
    amountMicroUsd: amount,
    expiresAt: "2026-10-15T10:10:00.000Z",
  });

const step = (doc: MonthDoc | null, cap: CapDoc | null, item: RegisteredEvent): MonthDoc => {
  const change = bookMonth(doc, cap, item);
  assert.ok(change);
  assert.equal(change.notices[0]?.payload.topic, "budget");
  const next = change.months[0]?.after;
  assert.ok(next);
  return next;
};

test("monthKey uses the owner time zone, monthOf reads the stream id", () => {
  assert.equal(monthKey(new Date("2026-10-31T23:30:00.000Z"), zone), "2026-11");
  assert.equal(monthKey(new Date("2026-10-31T22:30:00.000Z"), zone), "2026-10");
  assert.equal(monthOf("budget-month-2026-10"), "2026-10");
  assert.equal(monthOf("budget-month-2026-13"), undefined);
  assert.equal(monthOf("budget-cap-month"), undefined);
});

test("reserve, settle above the reserve and expire give the budget numbers", () => {
  const capChange = setCap(null, [], capSet(1, 1000), zone);
  assert.ok(capChange);
  const cap = capChange.cap?.after ?? null;
  let doc = step(null, cap, reserved(1, 400));
  assert.deepEqual(BudgetMonthV1.parse(toView(doc)), {
    month: "2026-10",
    capMicroUsd: 1000,
    spentMicroUsd: 0,
    reservedMicroUsd: 400,
    remainingMicroUsd: 600,
    overCapMicroUsd: 0,
    rev: 1,
    asOf: 1,
  });
  doc = step(
    doc,
    cap,
    event("budget.ReservationSettled", october, 2, {
      reservationId,
      reservedMicroUsd: 400,
      actualMicroUsd: 900,
    }),
  );
  doc = step(doc, cap, reserved(3, 300));
  doc = step(
    doc,
    cap,
    event("budget.ReservationExpired", october, 4, {
      reservationId,
      amountMicroUsd: 300,
    }),
  );
  const view = toView(doc);
  assert.equal(view.spentMicroUsd, 1200);
  assert.equal(view.reservedMicroUsd, 0);
  assert.equal(view.remainingMicroUsd, 0);
  assert.equal(view.overCapMicroUsd, 200);
  assert.equal(view.rev, 4);
  assert.equal(bookMonth(doc, cap, reserved(4, 1)), undefined);
  const refused = event("budget.ReservationRefused", october, 5, {
    reservationId,
    taskId: randomUUID(),
    agentId: randomUUID(),
    scope: { kind: "month", key: "2026-10" },
    amountMicroUsd: 1,
    reason: "No room",
  });
  assert.equal(bookMonth(doc, cap, refused), undefined);
});

test("with no cap, the cap is null and remaining and overCap are 0", () => {
  const doc = step(null, null, reserved(1, 400));
  const view = toView(doc);
  assert.equal(view.capMicroUsd, null);
  assert.equal(view.remainingMicroUsd, 0);
  assert.equal(view.overCapMicroUsd, 0);
});

test("BudgetSet changes every month document and makes the month of the event", () => {
  const september: MonthDoc = {
    _id: "2026-09",
    spent: 50,
    reserved: 0,
    monthVersion: 7,
    capVersion: 0,
    rev: 3,
  };
  const change = setCap(null, [september], capSet(1, 1000, "2026-10-31T23:30:00.000Z", 800), zone);
  assert.ok(change);
  assert.deepEqual(
    change.months.map(({ before, after }) => [before?._id ?? null, after._id, after.rev]),
    [
      ["2026-09", "2026-09", 4],
      [null, "2026-11", 1],
    ],
  );
  const november = change.months[1]?.after;
  assert.ok(november);
  assert.deepEqual(BudgetMonthV1.parse(toView(november)), {
    month: "2026-11",
    capMicroUsd: 1000,
    softCapMicroUsd: 800,
    spentMicroUsd: 0,
    reservedMicroUsd: 0,
    remainingMicroUsd: 1000,
    overCapMicroUsd: 0,
    rev: 1,
    asOf: 1,
  });
  assert.equal(toView(change.months[0]?.after ?? september).asOf, 7);
  const cap = change.cap?.after ?? null;
  assert.equal(setCap(cap, [], capSet(1, 5), zone), undefined);
  const lower = setCap(cap, [november], capSet(2, 500, "2026-11-02T10:00:00.000Z"), zone);
  assert.ok(lower);
  assert.equal(lower.months.length, 1);
  assert.equal(lower.months[0]?.after.cap?.soft, undefined);
  assert.equal(lower.notices.length, 1);
});
