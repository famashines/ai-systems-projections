import { commonEnv } from "@famashines/ai-systems-kit/config";
import { z } from "zod";

const httpUrl = z.url({ protocol: /^https?$/ });

const isTimeZone = (zone: string): boolean => {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: zone });
    return true;
  } catch {
    return false;
  }
};

export const Env = commonEnv.extend({
  SERVICE_NAME: z.literal("projections").default("projections"),
  MONGODB_URI: z.string().min(1),
  MONGODB_DATABASE: z.string().min(1).default("projections"),
  NATS_SERVERS: z
    .string()
    .min(1)
    .transform((value) => value.split(",").map((server) => server.trim())),
  NATS_USER: z.string().min(1),
  NATS_PASSWORD: z.string().min(1),
  ORCHESTRATION_URL: httpUrl,
  CONVERSATION_HUB_URL: httpUrl.default("http://ai-systems-conversation-hub:8080"),
  AI_PROXY_URL: httpUrl.default("http://ai-systems-ai-proxy:8080"),
  BUDGET_URL: httpUrl.default("http://ai-systems-budget:8080"),
  AGENT_MANAGER_URL: httpUrl.default("http://ai-systems-agent-manager:8080"),
  OWNER_TIME_ZONE: z.string().min(1).refine(isTimeZone).default("Europe/Berlin"),
});
