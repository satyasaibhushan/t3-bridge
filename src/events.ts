import { z } from "zod";
import type { Bridge, Notice } from "./bridge.js";
import {
  type CallbackTransport,
  callbackUrl,
  signedPost,
  signingKey,
  verifyCallback,
} from "./callback.js";
import { BridgeError, type Filter, digest, filterSchema } from "./schema.js";
const identitySchema = z
  .object({
    name: z.literal("thread.changed"),
    arguments: filterSchema,
    delivery: z
      .object({ mode: z.literal("webhook"), url: z.string().url() })
      .strict(),
  })
  .strict();
const subscriptionSchema = identitySchema.extend({
  delivery: identitySchema.shape.delivery.extend({ secret: z.string() }),
  cursor: z.string().regex(/^\d+$/).nullable().optional(),
  ttlMs: z.number().int().positive().nullable().optional(),
});
export interface Subscription {
  id: string;
  revision: number;
  owner: string;
  filter: Filter;
  url: string;
  secret: string;
  expiresAt: number;
  cursor: number;
  oldSecret?: string;
  rotateUntil?: number;
  failures: number;
  nextAttempt: number;
  disabled?: string;
}
export class Events {
  private flushing: Promise<void> | undefined;
  private readonly verifying = new Map<string, Promise<void>>();
  private readonly verified = new Map<string, number>();
  constructor(
    readonly bridge: Bridge,
    readonly transport: CallbackTransport,
  ) {}
  private identity(input: z.infer<typeof identitySchema>): string {
    return `sub_${digest([this.bridge.config.principal, input.delivery.url, input.name, input.arguments])}`;
  }
  async subscribe(raw: unknown) {
    const input = subscriptionSchema.parse(raw);
    this.bridge.scope(input.arguments);
    callbackUrl(input.delivery.url, this.bridge.config.callbackHosts);
    signingKey(input.delivery.secret);
    const id = this.identity({
      name: input.name,
      arguments: input.arguments,
      delivery: { mode: "webhook", url: input.delivery.url },
    });
    // Reserve a generation before any network await. Unsubscribe and later
    // refreshes invalidate this attempt even if verification finishes afterward.
    const revision =
      (this.bridge.store.get<number>("subscriptionRevisions", id) ?? 0) + 1;
    this.bridge.store.set("subscriptionRevisions", id, revision);
    const key = digest([
      this.bridge.config.principal,
      input.delivery.url,
      input.delivery.secret,
    ]);
    if ((this.verified.get(key) ?? 0) < Date.now()) {
      let check = this.verifying.get(key);
      if (!check) {
        check = verifyCallback(this.transport, {
          id,
          url: input.delivery.url,
          secret: input.delivery.secret,
        });
        this.verifying.set(key, check);
      }
      try {
        await check;
        this.verified.set(key, Date.now() + 300000);
      } finally {
        this.verifying.delete(key);
      }
    }
    if (this.bridge.store.get<number>("subscriptionRevisions", id) !== revision)
      throw new BridgeError("subscription_superseded");
    const previous = this.bridge.store.get<Subscription>("subscriptions", id);
    const latest = this.bridge.store.get<number>("meta", "eventSequence") ?? 0;
    const earliest = this.bridge.store.all<Notice>("events")[0]?.[1];
    const floor = earliest ? Number(earliest.cursor) - 1 : latest;
    const requested =
      input.cursor == null
        ? (previous?.cursor ?? latest)
        : Number(input.cursor);
    if (!Number.isSafeInteger(requested) || requested > latest)
      throw new BridgeError("invalid_cursor");
    // A refresh must not advance past work still awaiting acknowledgment.
    const cursor = Math.max(
      floor,
      Math.min(requested, previous?.cursor ?? requested),
    );
    const expiresAt = Date.now() + Math.min(input.ttlMs ?? 86400000, 86400000);
    const sub: Subscription = {
      id,
      revision,
      owner: this.bridge.config.principal,
      filter: input.arguments,
      url: input.delivery.url,
      secret: input.delivery.secret,
      expiresAt,
      cursor,
      failures: 0,
      nextAttempt: 0,
      ...(previous && previous.secret !== input.delivery.secret
        ? { oldSecret: previous.secret, rotateUntil: Date.now() + 300000 }
        : {}),
    };
    this.bridge.store.set("subscriptions", id, sub);
    void this.flush().catch(() => console.error("callback_delivery_failed"));
    return {
      id,
      refreshBefore: new Date(expiresAt).toISOString(),
      cursor: String(cursor),
      truncated: requested < floor,
    };
  }
  unsubscribe(raw: unknown) {
    const input = identitySchema.parse(raw);
    this.bridge.scope(input.arguments);
    const id = this.identity(input);
    this.bridge.store.transaction(() => {
      this.bridge.store.set(
        "subscriptionRevisions",
        id,
        (this.bridge.store.get<number>("subscriptionRevisions", id) ?? 0) + 1,
      );
      this.bridge.store.delete("subscriptions", id);
    });
    return {};
  }
  flush(): Promise<void> {
    if (this.flushing) return this.flushing;
    this.flushing = this.deliver().finally(() => {
      this.flushing = undefined;
    });
    return this.flushing;
  }
  private async deliver(): Promise<void> {
    for (const [id, original] of this.bridge.store.all<Subscription>(
      "subscriptions",
    )) {
      let sub = original;
      if (
        sub.owner !== this.bridge.config.principal ||
        sub.expiresAt <= Date.now() ||
        sub.disabled ||
        sub.nextAttempt > Date.now()
      )
        continue;
      try {
        this.bridge.scope(sub.filter);
      } catch {
        this.bridge.store.set("subscriptions", id, {
          ...sub,
          disabled: "access_revoked",
        });
        continue;
      }
      for (const [, event] of this.bridge.store.all<Notice>("events")) {
        if (Number(event.cursor) <= sub.cursor) continue;
        const current = this.bridge.store.get<Subscription>(
          "subscriptions",
          id,
        );
        if (
          !current ||
          current.expiresAt <= Date.now() ||
          current.revision !== sub.revision ||
          current.secret !== sub.secret
        )
          break;
        const matches =
          event.data.environmentId === sub.filter.environmentId &&
          event.data.projectId === sub.filter.projectId &&
          (!sub.filter.threadId || event.data.threadId === sub.filter.threadId);
        let accepted = !matches;
        let status = 0;
        if (matches) {
          if (
            !event.data.threadId ||
            !this.bridge.allowed(
              event.data.environmentId,
              event.data.projectId,
              event.data.threadId,
            )
          ) {
            accepted = true;
          } else {
            try {
              const result = await signedPost(
                this.transport,
                sub,
                event,
                event.eventId,
              );
              status = result.status;
              accepted = status >= 200 && status < 300;
            } catch {
              status = 0;
            }
          }
        }
        // Unsubscribe or refresh during the HTTP request must never be overwritten.
        const after = this.bridge.store.get<Subscription>("subscriptions", id);
        if (
          !after ||
          after.revision !== sub.revision ||
          after.secret !== sub.secret ||
          after.expiresAt !== sub.expiresAt
        )
          break;
        if (accepted) {
          sub = {
            ...sub,
            cursor: Number(event.cursor),
            failures: 0,
            nextAttempt: 0,
          };
          this.bridge.store.set("subscriptions", id, sub);
          continue;
        }
        const failures = sub.failures + 1;
        sub = {
          ...sub,
          failures,
          nextAttempt: Date.now() + Math.min(60000, 1000 * 2 ** failures),
          ...([410, 413].includes(status) || failures >= 8
            ? {
                disabled:
                  status === 410
                    ? "gone"
                    : status === 413
                      ? "payload_rejected"
                      : "retry_exhausted",
              }
            : {}),
        };
        this.bridge.store.set("subscriptions", id, sub);
        break;
      }
    }
    // Bound replay storage. Subscribers behind the retained floor are disabled;
    // their next refresh receives truncated=true and an explicit new cursor.
    const events = this.bridge.store.all<Notice>("events");
    if (events.length > 10000) {
      const removed = events.slice(0, events.length - 10000);
      const floor = Number(removed.at(-1)?.[1].cursor ?? 0);
      this.bridge.store.transaction(() => {
        for (const [key] of removed) this.bridge.store.delete("events", key);
        for (const [id, s] of this.bridge.store.all<Subscription>(
          "subscriptions",
        ))
          if (s.cursor < floor)
            this.bridge.store.set("subscriptions", id, {
              ...s,
              disabled: "replay_truncated",
            });
      });
    }
  }
}
