import { commonEnv } from "@famashines/ai-systems-kit/config";
import { z } from "zod";

export const RebuildEnv = z.object({
  MONGODB_URI: z.string().min(1),
  MONGODB_DATABASE: z.string().min(1).default("projections"),
  ORCHESTRATION_URL: z.url({ protocol: /^https?$/ }),
  LOG_LEVEL: commonEnv.shape.LOG_LEVEL,
  RELEASE: commonEnv.shape.RELEASE,
});

export const Env = commonEnv.extend({
  ...RebuildEnv.shape,
  SERVICE_NAME: z.literal("projections").default("projections"),
  NATS_SERVERS: z
    .string()
    .min(1)
    .transform((value) => value.split(",").map((server) => server.trim())),
  NATS_USER: z.string().min(1),
  NATS_PASSWORD: z.string().min(1),
});
