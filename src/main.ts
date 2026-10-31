import { loadConfig } from "@famashines/ai-systems-kit/config";
import { createService } from "@famashines/ai-systems-kit/http";
import { connectNats } from "@famashines/ai-systems-kit/messaging";
import { connectMongo, runMigrations } from "@famashines/ai-systems-kit/mongo";
import { createLogger, startTelemetry } from "@famashines/ai-systems-kit/observability";
import { startProjections } from "./app.io.ts";
import { Env } from "./config.ts";
import { migrations } from "./projector.io.ts";

const config = loadConfig(Env);
const { SERVICE_NAME: service, RELEASE: release } = config;
const telemetry = startTelemetry({ serviceName: service, release });
const logger = createLogger({ service, release, level: config.LOG_LEVEL });
const mongo = await connectMongo({ url: config.MONGODB_URI, database: config.MONGODB_DATABASE });
await runMigrations(mongo.db, migrations);
const nats = await connectNats({
  servers: config.NATS_SERVERS,
  user: config.NATS_USER,
  pass: config.NATS_PASSWORD,
  service,
  logger,
});
const projections = startProjections(mongo.db, nats);
const app = createService({
  logger,
  metrics: telemetry.metricsHandler,
  routes: projections.routes,
  readiness: { mongo: mongo.ready, nats: nats.ready, ...projections.readiness },
  closeHooks: [telemetry.shutdown, mongo.close, nats.close, ...projections.closeHooks],
});
await app.listen(config.PORT);
app.handleSignals();
logger.info({ port: config.PORT }, "projections started");
