import type { AgentListItemV1, RegisteredEvent } from "@famashines/ai-systems-contracts";
import { noticeOf, type Notice } from "./notices.ts";

export interface AgentDoc {
  _id: string;
  name: string;
  enabled: boolean;
  activeVersion?: number;
  latestVersion?: number;
  rev: number;
  asOf: number;
}

export const toAgent = (doc: AgentDoc): AgentListItemV1 => ({
  agentId: doc._id,
  name: doc.name,
  enabled: doc.enabled,
  ...(doc.activeVersion === undefined ? {} : { activeVersion: doc.activeVersion }),
  ...(doc.latestVersion === undefined ? {} : { latestVersion: doc.latestVersion }),
  rev: doc.rev,
  asOf: doc.asOf,
});

export const agentIdOf = (event: RegisteredEvent): string | undefined =>
  event.type.startsWith("agents.") ? (event.payload as { agentId: string }).agentId : undefined;

function changed(current: AgentDoc | null, event: RegisteredEvent): AgentDoc | undefined {
  if (event.type === "agents.AgentCreated") {
    return (
      current ?? {
        _id: event.payload.agentId,
        name: event.payload.name,
        enabled: false,
        rev: 0,
        asOf: 0,
      }
    );
  }
  if (!current) return undefined;
  switch (event.type) {
    case "agents.AgentVersionPublished":
      return {
        ...current,
        latestVersion: Math.max(current.latestVersion ?? 0, event.payload.version),
      };
    case "agents.AgentVersionActivated":
      return { ...current, activeVersion: event.payload.version };
    case "agents.AgentEnabled":
      return { ...current, enabled: true };
    case "agents.AgentDisabled":
      return { ...current, enabled: false };
    default:
      return undefined;
  }
}

export function projectAgent(
  current: AgentDoc | null,
  event: RegisteredEvent,
): { doc: AgentDoc; notice: Notice } | undefined {
  if (current && event.version <= current.asOf) return undefined;
  const next = changed(current, event);
  if (!next) return undefined;
  const doc = { ...next, rev: (current?.rev ?? 0) + 1, asOf: event.version };
  return { doc, notice: noticeOf(event, "agents", doc, toAgent(doc)) };
}
