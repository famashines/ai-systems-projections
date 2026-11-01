import { parseEvent } from "@famashines/ai-systems-contracts";
import type { CloseHook, ReadinessCheck, Route } from "@famashines/ai-systems-kit/http";
import { startConsumer, startRelay, type Nats } from "@famashines/ai-systems-kit/messaging";
import type { Db } from "mongodb";
import {
  createNoticeStore,
  createProjector,
  listAgents,
  listMessages,
  listTasks,
  readDetail,
  readMonth,
  readOutput,
  viewsHeadSeq,
} from "./projector.io.ts";
import { routes } from "./query.ts";

export interface Projections {
  routes: Route[];
  readiness: Record<string, ReadinessCheck>;
  closeHooks: CloseHook[];
}

export const sourceStreams = ["ORCHESTRATION", "CONVERSATION", "LLM", "BUDGET", "AGENTS"];

export function startProjections(db: Db, nats: Nats, timeZone: string): Projections {
  const store = createNoticeStore(db);
  const apply = createProjector(db, store, timeZone);
  const consumers = sourceStreams.map((stream) => ({
    stream,
    consumer: startConsumer({
      nats,
      db,
      stream,
      filter: `${stream.toLowerCase()}.>`,
      ordered: true,
      parse: parseEvent,
      handler: (event, { markProcessed }) => apply(event, markProcessed),
    }),
  }));
  const relay = startRelay({ store, nats, streams: ["VIEWS"] });
  return {
    routes: routes({
      headSeq: () => viewsHeadSeq(nats),
      tasks: (query) => listTasks(db, query),
      detail: (taskId) => readDetail(db, taskId),
      output: (id) => readOutput(db, id),
      messages: (conversationId, query) => listMessages(db, conversationId, query),
      month: (month) => readMonth(db, month),
      agents: () => listAgents(db),
      timeZone,
    }),
    readiness: {
      ...Object.fromEntries(
        consumers.map(({ stream, consumer }) => [
          `consumer_${stream.toLowerCase()}`,
          consumer.ready,
        ]),
      ),
      relay: relay.ready,
    },
    closeHooks: [relay.close, ...consumers.map(({ consumer }) => consumer.close)],
  };
}
