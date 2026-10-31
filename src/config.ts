import { commonEnv } from "@famashines/ai-systems-kit/config";
import { z } from "zod";

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
  ORCHESTRATION_URL: z.url({ protocol: /^https?$/ }),
});
