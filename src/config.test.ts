import assert from "node:assert/strict";
import { test } from "node:test";
import { ConfigError, loadConfig } from "@famashines/ai-systems-kit/config";
import { Env } from "./config.ts";

const env = {
  MONGODB_URI: "mongodb://localhost:27017/?replicaSet=rs0",
  NATS_SERVERS: "nats://a:4222, nats://b:4222",
  NATS_USER: "ai-systems-projections",
  NATS_PASSWORD: "x",
  ORCHESTRATION_URL: "http://orchestration:8080",
};

test("the service config has the defaults", () => {
  const config = loadConfig(Env, env);
  assert.equal(config.SERVICE_NAME, "projections");
  assert.equal(config.MONGODB_DATABASE, "projections");
  assert.equal(config.PORT, 8080);
  assert.deepEqual(config.NATS_SERVERS, ["nats://a:4222", "nats://b:4222"]);
});

test("the service name cannot change and the URL must be http", () => {
  assert.throws(() => loadConfig(Env, { ...env, SERVICE_NAME: "other" }), ConfigError);
  assert.throws(() => loadConfig(Env, { ...env, ORCHESTRATION_URL: "file:///x" }), ConfigError);
  assert.throws(() => loadConfig(Env, { ...env, NATS_PASSWORD: undefined }), ConfigError);
});
