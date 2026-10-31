import { loadConfig } from "@famashines/ai-systems-kit/config";
import { connectNats } from "@famashines/ai-systems-kit/messaging";
import { connectMongo, runMigrations } from "@famashines/ai-systems-kit/mongo";
import { createLogger } from "@famashines/ai-systems-kit/observability";
import { Env } from "./config.ts";
import { migrations } from "./projector.io.ts";
import { markAndCheck, rebuild, replayFrom } from "./rebuild.io.ts";

const config = loadConfig(Env);
const logger = createLogger({
  service: "projections-rebuild",
  release: config.RELEASE,
  level: config.LOG_LEVEL,
});
logger.info({ database: config.MONGODB_DATABASE }, "connecting to MongoDB");
const mongo = await connectMongo({ url: config.MONGODB_URI, database: config.MONGODB_DATABASE });
try {
  await runMigrations(mongo.db, migrations);
  logger.info({ servers: config.NATS_SERVERS }, "connecting to NATS");
  const nats = await connectNats({
    servers: config.NATS_SERVERS,
    user: config.NATS_USER,
    pass: config.NATS_PASSWORD,
    service: config.SERVICE_NAME,
    logger,
  });
  try {
    await markAndCheck(mongo.db, nats);
  } finally {
    await nats.close();
  }
  const sources = [
    config.ORCHESTRATION_URL,
    config.CONVERSATION_HUB_URL,
    config.AI_PROXY_URL,
    config.BUDGET_URL,
    config.AGENT_MANAGER_URL,
  ].map(replayFrom);
  const result = await rebuild(mongo.db, sources, config.OWNER_TIME_ZONE);
  if (result.skipped > 0) {
    logger.warn(
      { skipped: result.skipped, types: result.skippedTypes },
      "events that do not parse were skipped",
    );
  }
  logger.info({ applied: result.applied }, "the read models are rebuilt");
} finally {
  await mongo.close();
}
