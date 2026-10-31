import type { CloseHook, ReadinessCheck, Route } from "@famashines/ai-systems-kit/http";
import { startConsumer, startRelay, type Nats } from "@famashines/ai-systems-kit/messaging";
import type { Db } from "mongodb";
import { createNoticeStore, createProjector, listTasks, viewsHeadSeq } from "./projector.io.ts";
import { taskRoutes } from "./query.ts";

export interface Projections {
  routes: Route[];
  readiness: Record<string, ReadinessCheck>;
  closeHooks: CloseHook[];
}

export function startProjections(db: Db, nats: Nats): Projections {
  const store = createNoticeStore(db);
  const apply = createProjector(db, store);
  const consumer = startConsumer({
    nats,
    db,
    stream: "ORCHESTRATION",
    filter: "orchestration.>",
    ordered: true,
    handler: (event, { markProcessed }) => apply(event, markProcessed),
  });
  const relay = startRelay({ store, nats, streams: ["VIEWS"] });
  return {
    routes: taskRoutes({
      headSeq: () => viewsHeadSeq(nats),
      list: (query) => listTasks(db, query),
    }),
    readiness: { consumer: consumer.ready, relay: relay.ready },
    closeHooks: [relay.close, consumer.close],
  };
}
