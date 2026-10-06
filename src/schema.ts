import { createHash } from "node:crypto";
import { z } from "zod";
export const id = z
  .string()
  .min(1)
  .max(200)
  .regex(/^[A-Za-z0-9_.:-]+$/);
export const model = z
  .object({
    instanceId: id,
    model: z.string().min(1).max(200),
    options: z
      .array(
        z
          .object({
            id: z.string().min(1),
            value: z.union([z.string().min(1), z.boolean()]),
          })
          .strict(),
      )
      .optional(),
  })
  .strict();
const scope = { environmentId: id, projectId: id };
const write = { ...scope, taskId: id, runId: id, commandId: id };
export const commandSchema = z.discriminatedUnion("operation", [
  z
    .object({
      ...write,
      operation: z.literal("create"),
      threadId: id,
      title: z.string().trim().min(1).max(200),
    })
    .strict(),
  z
    .object({
      ...write,
      operation: z.literal("send"),
      threadId: id,
      text: z.string().min(1).max(50000),
    })
    .strict(),
  z
    .object({
      ...write,
      operation: z.literal("interrupt"),
      threadId: id,
      turnId: id,
    })
    .strict(),
]);
export type Command = z.infer<typeof commandSchema>;
export const filterSchema = z
  .object({ ...scope, threadId: id.optional() })
  .strict();
export const readSchema = filterSchema.required().extend({
  turnLimit: z.number().int().min(1).max(100).default(20),
  beforeCursor: z.string().max(2048).optional(),
});
export type Filter = z.infer<typeof filterSchema>;
export const configSchema = z
  .object({
    port: z.number().int().min(1024).max(65535).default(4317),
    statePath: z.string().min(1),
    enableWrites: z.boolean().default(false),
    principal: id,
    clientTokenEnv: id,
    callbackHosts: z.array(z.string().min(1)).default([]),
    environments: z
      .array(
        z
          .object({
            id,
            baseUrl: z.string().url(),
            tokenEnv: id,
            projects: z
              .array(
                z
                  .object({
                    id,
                    model,
                    threadIds: z.array(id).default([]),
                    allowCreate: z.boolean().default(false),
                  })
                  .strict(),
              )
              .default([]),
          })
          .strict(),
      )
      .default([]),
  })
  .strict()
  .superRefine((c, ctx) => {
    if (new Set(c.environments.map((e) => e.id)).size !== c.environments.length)
      ctx.addIssue({ code: "custom", message: "Duplicate environment" });
    for (const e of c.environments) {
      const u = new URL(e.baseUrl);
      if (
        u.username ||
        u.password ||
        u.search ||
        u.hash ||
        u.pathname !== "/" ||
        (u.protocol !== "https:" &&
          !(
            u.protocol === "http:" &&
            ["127.0.0.1", "[::1]", "localhost"].includes(u.hostname)
          ))
      )
        ctx.addIssue({
          code: "custom",
          message:
            "T3 URL must be HTTPS or loopback HTTP, with no credentials or path",
        });
      if (new Set(e.projects.map((p) => p.id)).size !== e.projects.length)
        ctx.addIssue({ code: "custom", message: "Duplicate project" });
    }
  });
export type Config = z.infer<typeof configSchema>;
export type Environment = Config["environments"][number];
export function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object")
    return `{${Object.entries(value)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${JSON.stringify(k)}:${canonical(v)}`)
      .join(",")}}`;
  return JSON.stringify(value);
}
export function digest(value: unknown): string {
  return createHash("sha256").update(canonical(value)).digest("hex");
}
export class BridgeError extends Error {
  constructor(
    public code: string,
    public rpcCode = -32602,
  ) {
    super(code);
  }
}
export const threadSchema = z
  .object({
    id,
    projectId: id,
    title: z.string(),
    // Optional for read compatibility; sends fail closed unless all are verified.
    modelSelection: model.optional(),
    runtimeMode: z.string().optional(),
    interactionMode: z.string().optional(),
    updatedAt: z.string(),
    latestTurn: z
      .object({
        turnId: id,
        state: z.string(),
        completedAt: z.string().nullable().optional(),
      })
      .strip()
      .nullable(),
    session: z
      .object({
        status: z.string(),
        activeTurnId: id.nullable(),
        lastError: z.string().nullable(),
      })
      .strip()
      .nullable(),
    hasPendingApprovals: z.boolean().optional(),
    hasPendingUserInput: z.boolean().optional(),
    planProgress: z
      .object({
        step: z.string(),
        completedSteps: z.number(),
        totalSteps: z.number(),
      })
      .strip()
      .nullable()
      .optional(),
  })
  .strip();
export type Thread = z.infer<typeof threadSchema>;
export const shellSchema = z.object({
  snapshotSequence: z.number().int().nonnegative(),
  threads: z.array(threadSchema),
});
export const streamSchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("snapshot"), snapshot: shellSchema }),
  z.object({ kind: z.literal("synchronized") }),
  z.object({
    kind: z.literal("thread-upserted"),
    sequence: z.number().int().nonnegative(),
    thread: threadSchema,
  }),
  z.object({
    kind: z.literal("thread-removed"),
    sequence: z.number().int().nonnegative(),
    threadId: id,
  }),
  z.object({
    kind: z.enum(["project-upserted", "project-removed"]),
    sequence: z.number().int().nonnegative(),
  }),
]);
export type StreamItem = z.infer<typeof streamSchema>;
