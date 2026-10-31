import { loadConfig } from "@famashines/ai-systems-kit/config";
import { connectMongo, runMigrations } from "@famashines/ai-systems-kit/mongo";
import { createLogger } from "@famashines/ai-systems-kit/observability";
import { RebuildEnv } from "./config.ts";
import { migrations } from "./projector.io.ts";
import { rebuild, replayFrom } from "./rebuild.io.ts";

const config = loadConfig(RebuildEnv);
const logger = createLogger({
  service: "projections-rebuild",
  release: config.RELEASE,
  level: config.LOG_LEVEL,
});
const mongo = await connectMongo({ url: config.MONGODB_URI, database: config.MONGODB_DATABASE });
try {
  await runMigrations(mongo.db, migrations);
  const events = await rebuild(mongo.db, replayFrom(config.ORCHESTRATION_URL));
  logger.info({ events }, "the read models are rebuilt");
} finally {
  await mongo.close();
}
