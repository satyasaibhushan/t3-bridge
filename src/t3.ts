import { setTimeout as delay } from "node:timers/promises";
import WebSocket from "ws";
import { z } from "zod";
import {
  BridgeError,
  type Environment,
  type StreamItem,
  shellSchema,
  streamSchema,
  threadSchema,
} from "./schema.js";
import type { T3Port } from "./bridge.js";
export const detailSchema = z
  .object({
    snapshotSequence: z.number().int().nonnegative(),
    thread: threadSchema.extend({
      messages: z
        .array(
          z
            .object({
              id: z.string(),
              role: z.string(),
              text: z.string(),
              createdAt: z.string(),
              turnId: z.string().nullable().optional(),
            })
            .strip(),
        )
        .default([]),
    }),
    page: z
      .object({
        beforeCursor: z.string().nullable(),
        hasMore: z.boolean(),
        snapshotSequence: z.number(),
      })
      .optional(),
  })
  .strip();
export class T3Client implements T3Port {
  constructor(
    readonly environment: Environment,
    readonly token: () => string,
    readonly request: typeof fetch = fetch,
  ) {}
  private async http(path: string, body?: unknown): Promise<unknown> {
    await this.verifyIdentity();
    const token = this.token();
    if (!token) throw new BridgeError("pairing_required");
    let response: Response;
    try {
      response = await this.request(new URL(path, this.environment.baseUrl), {
        method: body === undefined ? "GET" : "POST",
        headers: {
          authorization: `Bearer ${token}`,
          ...(body !== undefined ? { "content-type": "application/json" } : {}),
        },
        ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
        redirect: "error",
        signal: AbortSignal.timeout(15000),
      });
    } catch {
      throw new BridgeError("t3_unavailable", -32000);
    }
    if (!response.ok)
      throw new BridgeError(
        response.status === 401 || response.status === 403
          ? "t3_access_denied"
          : "t3_request_failed",
        -32000,
      );
    try {
      return await response.json();
    } catch {
      throw new BridgeError("t3_invalid_response", -32000);
    }
  }
  async verifyIdentity(): Promise<void> {
    let response: Response;
    try {
      response = await this.request(
        new URL("/.well-known/t3/environment", this.environment.baseUrl),
        { redirect: "error", signal: AbortSignal.timeout(10000) },
      );
    } catch {
      throw new BridgeError("t3_unavailable", -32000);
    }
    if (!response.ok) throw new BridgeError("t3_descriptor_failed", -32000);
    const d = z
      .object({
        environmentId: z.string(),
        orchestrationProtocolVersion: z.number().optional(),
      })
      .parse(await response.json());
    if (d.environmentId !== this.environment.id)
      throw new BridgeError("environment_identity_mismatch");
    if ((d.orchestrationProtocolVersion ?? 1) !== 1)
      throw new BridgeError("unsupported_t3_protocol");
  }
  async shell() {
    return shellSchema.parse(await this.http("/api/orchestration/shell"));
  }
  async thread(
    threadId: string,
    window = { turnLimit: 20 } as { turnLimit: number; beforeCursor?: string },
  ) {
    const query = new URLSearchParams({ turnLimit: String(window.turnLimit) });
    if (window.beforeCursor) query.set("beforeCursor", window.beforeCursor);
    return detailSchema.parse(
      await this.http(
        `/api/orchestration/threads/${encodeURIComponent(threadId)}?${query}`,
      ),
    );
  }
  async dispatch(command: unknown) {
    return z
      .object({ sequence: z.number().int().nonnegative() })
      .parse(await this.http("/api/orchestration/dispatch", command));
  }
  async socketUrl(): Promise<URL> {
    const result = z
      .object({ ticket: z.string().min(1) })
      .parse(await this.http("/api/auth/websocket-ticket", {}));
    const url = new URL("/ws", this.environment.baseUrl);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    url.searchParams.set("wsTicket", result.ticket);
    url.searchParams.set("orchestrationProtocol", "1");
    return url;
  }
}
export interface StreamHealth {
  connected: boolean;
  lastError: string | null;
}
export class ShellStream {
  health: StreamHealth = { connected: false, lastError: null };
  constructor(
    readonly client: T3Client,
    readonly cursor: () => number | undefined,
    readonly ingest: (item: StreamItem) => void | Promise<void>,
  ) {}
  async run(signal: AbortSignal): Promise<void> {
    let failures = 0;
    while (!signal.aborted) {
      try {
        await this.connect(await this.client.socketUrl(), signal);
        failures = 0;
      } catch (error) {
        this.health = {
          connected: false,
          lastError:
            error instanceof BridgeError ? error.code : "t3_stream_unavailable",
        };
        failures++;
      }
      if (!signal.aborted)
        await delay(
          Math.min(30000, 500 * 2 ** Math.min(failures, 6)),
          undefined,
          { signal },
        ).catch(() => {});
    }
    this.health.connected = false;
  }
  connect(url: URL, signal: AbortSignal): Promise<void> {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(url, {
        handshakeTimeout: 10000,
        maxPayload: 16 * 1024 * 1024,
        followRedirects: false,
      });
      const abort = () => ws.close();
      signal.addEventListener("abort", abort, { once: true });
      let receivedAt = Date.now();
      const heartbeat = setInterval(() => {
        if (Date.now() - receivedAt > 45000) ws.terminate();
        else if (ws.readyState === WebSocket.OPEN)
          ws.send(JSON.stringify({ _tag: "Ping" }));
      }, 15000);
      const finish = () => {
        clearInterval(heartbeat);
        signal.removeEventListener("abort", abort);
        this.health.connected = false;
      };
      ws.on("open", () => {
        const afterSequence = this.cursor();
        ws.send(
          JSON.stringify({
            _tag: "Request",
            id: "1",
            tag: "orchestration.subscribeShell",
            payload: {
              ...(afterSequence === undefined ? {} : { afterSequence }),
              requestCompletionMarker: true,
            },
            headers: [],
          }),
        );
      });
      let processing = Promise.resolve();
      let streamError: BridgeError | undefined;
      ws.on("message", (data) => {
        receivedAt = Date.now();
        processing = processing.then(async () => {
          try {
            const parsed: unknown = JSON.parse(data.toString());
            for (const frame of Array.isArray(parsed) ? parsed : [parsed]) {
              const f = z
                .object({
                  _tag: z.string(),
                  requestId: z.string().optional(),
                  values: z.array(z.unknown()).optional(),
                })
                .passthrough()
                .parse(frame);
              if (f._tag === "Pong") continue;
              if (f._tag === "Ping") {
                ws.send(JSON.stringify({ _tag: "Pong" }));
                continue;
              }
              if (f._tag === "Chunk" && f.requestId === "1" && f.values) {
                for (const value of f.values) {
                  const item = streamSchema.parse(value);
                  await this.ingest(item);
                  if (item.kind === "synchronized")
                    this.health = { connected: true, lastError: null };
                }
                ws.send(JSON.stringify({ _tag: "Ack", requestId: "1" }));
              } else if (f._tag === "Exit" || f._tag === "Defect")
                throw new Error("stream_ended");
            }
          } catch (error) {
            streamError =
              error instanceof BridgeError
                ? error
                : new BridgeError("invalid_t3_stream");
            ws.terminate();
          }
        });
      });
      ws.on("error", () => {
        finish();
        void processing.then(() =>
          reject(streamError ?? new BridgeError("t3_stream_unavailable")),
        );
      });
      ws.on("close", () => {
        finish();
        void processing.then(() => {
          if (signal.aborted) resolve();
          else reject(streamError ?? new BridgeError("t3_stream_disconnected"));
        });
      });
      if (signal.aborted) abort();
    });
  }
}
