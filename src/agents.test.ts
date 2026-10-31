import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import {
  AgentListItemV1,
  parseEvent,
  type RegisteredEvent,
} from "@famashines/ai-systems-contracts";
import { projectAgent, toAgent, type AgentDoc } from "./agents.ts";

const agentId = randomUUID();
const event = (type: string, version: number, payload: object): RegisteredEvent =>
  parseEvent({
    eventId: randomUUID(),
    type,
    schemaVersion: 1,
    streamId: `agent-${agentId}`,
    version,
    occurredAt: "2026-10-31T09:15:00.000Z",
    correlationId: randomUUID(),
    causationId: randomUUID(),
    actor: { kind: "owner", id: "owner" },
    payload,
  });

const published = (version: number, streamVersion: number) =>
  event("agents.AgentVersionPublished", streamVersion, {
    agentId,
    version,
    prompt: { promptId: randomUUID(), version: 1 },
    models: ["openai/gpt-4.1-nano"],
    limits: { maxCalls: 10, maxTokens: 4096, maxCostMicroUsd: 50000 },
  });

test("an agent is off after AgentCreated, then follows its versions and switch", () => {
  const events = [
    event("agents.AgentCreated", 1, { agentId, name: "writer" }),
    published(1, 2),
    published(2, 3),
    event("agents.AgentVersionActivated", 4, { agentId, version: 1 }),
    event("agents.AgentEnabled", 5, { agentId }),
  ];
  let doc: AgentDoc | null = null;
  const enabled: boolean[] = [];
  for (const item of events) {
    const change = projectAgent(doc, item);
    assert.ok(change, item.type);
    assert.equal(change.notice.payload.topic, "agents");
    assert.equal(AgentListItemV1.safeParse(change.notice.payload.data).success, true);
    doc = change.doc;
    enabled.push(doc.enabled);
  }
  assert.deepEqual(enabled, [false, false, false, false, true]);
  assert.ok(doc);
  assert.deepEqual(toAgent(doc), {
    agentId,
    name: "writer",
    enabled: true,
    activeVersion: 1,
    latestVersion: 2,
    rev: 5,
    asOf: 5,
  });
  assert.equal(projectAgent(doc, event("agents.AgentDisabled", 5, { agentId })), undefined);
  assert.equal(
    projectAgent(doc, event("agents.AgentDisabled", 6, { agentId }))?.doc.enabled,
    false,
  );
  assert.equal(projectAgent(null, event("agents.AgentEnabled", 2, { agentId })), undefined);
});
