import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import test from "node:test";
import { WebSocketServer } from "ws";
import { T3Client, ShellStream } from "../src/t3.js";
import { Events } from "../src/events.js";
import { Mcp, httpServer } from "../src/server.js";
import { config, filter, send, setup, thread } from "./helpers.js";

test("T3 HTTP uses exact release routes, identity check precedes credentials, detail projects out private fields", async () => {
  const seen: Array<{ url: string; init?: RequestInit }> = [];
  const request: typeof fetch = async (url, init) => {
    const u = String(url);
    seen.push({ url: u, init });
    return Response.json(
      u.includes(".well-known")
        ? { environmentId: "env", orchestrationProtocolVersion: 1 }
        : u.includes("websocket-ticket")
          ? { ticket: "fixture-ticket" }
          : u.includes("/threads/")
            ? {
                snapshotSequence: 4,
                thread: {
                  ...thread,
                  messages: [],
                  providerSessionToken: "private",
                },
              }
            : u.includes("/dispatch")
              ? { sequence: 5 }
              : { snapshotSequence: 3, threads: [thread] },
    );
  };
  const environment = config.environments[0];
  assert.ok(environment);
  const client = new T3Client(environment, () => "fixture-token", request);
  await client.shell();
  assert.equal(new Headers(seen[0]?.init?.headers).has("authorization"), false);
  assert.equal(
    new Headers(seen[1]?.init?.headers).get("authorization"),
    "Bearer fixture-token",
  );
  const detail = await client.thread("thread", {
    turnLimit: 7,
    beforeCursor: "older",
  });
  assert.equal("providerSessionToken" in detail.thread, false);
  assert.ok(
    seen.some((x) =>
      x.url.endsWith("/threads/thread?turnLimit=7&beforeCursor=older"),
    ),
  );
  await client.dispatch({ type: "thread.turn.interrupt" });
  assert.ok(
    seen.some(
      (x) =>
        x.url.endsWith("/api/orchestration/dispatch") &&
        x.init?.method === "POST",
    ),
  );
  const url = await client.socketUrl();
  assert.equal(url.pathname, "/ws");
  assert.equal(url.searchParams.get("wsTicket"), "fixture-ticket");
  assert.equal(url.searchParams.get("orchestrationProtocol"), "1");
  assert.equal(url.searchParams.has("token"), false);
});
test("wrong environment and nightly protocol fail before token-bearing requests", async () => {
  const environment = config.environments[0];
  assert.ok(environment);
  for (const descriptor of [
    { environmentId: "other", orchestrationProtocolVersion: 1 },
    { environmentId: "env", orchestrationProtocolVersion: 2 },
  ]) {
    let reads = 0;
    const client = new T3Client(
      environment,
      () => {
        reads++;
        return "fixture-token";
      },
      async () => Response.json(descriptor),
    );
    await assert.rejects(client.shell());
    assert.equal(reads, 0);
  }
});
test("offline HTTP is sanitized and never dispatches an invented RPC envelope", async () => {
  const environment = config.environments[0];
  assert.ok(environment);
  const client = new T3Client(
    environment,
    () => "secret",
    async () => {
      throw new Error("secret http failure");
    },
  );
  await assert.rejects(client.shell(), /t3_unavailable/);
});
test("real loopback MCP enforces auth, advertises events, executes through approved shared path", async () => {
  const { bridge, store, port } = setup();
  const events = new Events(bridge, async () => ({ status: 200, body: "{}" }));
  const server = httpServer(
    new Mcp(bridge, events, () => ({})),
    "a".repeat(32),
  );
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const endpoint = `http://127.0.0.1:${(server.address() as AddressInfo).port}/mcp`;
  const call = async (
    method: string,
    params: unknown = {},
    auth = "a".repeat(32),
    origin?: string,
  ) =>
    fetch(endpoint, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${auth}`,
        ...(origin ? { origin } : {}),
      },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
    });
  try {
    assert.equal((await call("tools/list", {}, "wrong")).status, 401);
    assert.equal(
      (await call("tools/list", {}, "a".repeat(32), "https://evil.example"))
        .status,
      403,
    );
    const discovery = (await (await call("server/discover")).json()) as {
      result: { supportedVersions: string[] };
    };
    assert.deepEqual(discovery.result.supportedVersions, ["2026-07-28"]);
    const listed = (await (await call("tools/list")).json()) as {
      result: { tools: Array<{ name: string }> };
    };
    assert.deepEqual(
      listed.result.tools.map((t) => t.name),
      [
        "t3_list_threads",
        "t3_read_thread",
        "t3_status",
        "t3_create",
        "t3_send",
        "t3_interrupt",
      ],
    );
    const args = { ...send } as Record<string, unknown>;
    delete args.operation;
    const denied = (await (
      await call("tools/call", { name: "t3_send", arguments: args })
    ).json()) as { error: { message: string } };
    assert.equal(denied.error.message, "approval_required");
    assert.equal(port.calls.length, 0);
    bridge.approve(send);
    const sent = (await (
      await call("tools/call", { name: "t3_send", arguments: args })
    ).json()) as { result: { isError: boolean } };
    assert.equal(sent.result.isError, false);
    assert.equal(port.calls.length, 1);
    const hidden = (await (
      await call("tools/call", {
        name: "t3_read_thread",
        arguments: { ...filter, threadId: "hidden" },
      })
    ).json()) as { error: { message: string } };
    assert.equal(hidden.error.message, "thread_denied");
  } finally {
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
    store.close();
  }
});
test("real websocket fixture verifies Effect Request/Chunk/Ack and reconnect afterSequence", async () => {
  const server = createServer(),
    wss = new WebSocketServer({ server });
  const requests: Array<Record<string, unknown>> = [];
  let acks = 0;
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const port = (server.address() as AddressInfo).port;
  const fixtureUrl = new URL(
    `ws://127.0.0.1:${port}/ws?wsTicket=fixture&orchestrationProtocol=1`,
  );
  wss.on("connection", (ws) => {
    ws.on("message", (data) => {
      const frame = JSON.parse(data.toString()) as Record<string, unknown>;
      if (frame._tag === "Request") {
        requests.push(frame);
        const item =
          requests.length === 1
            ? {
                kind: "snapshot",
                snapshot: { snapshotSequence: 5, threads: [thread] },
              }
            : {
                kind: "thread-upserted",
                sequence: 6,
                thread: {
                  ...thread,
                  latestTurn: { turnId: "turn", state: "completed" },
                },
              };
        ws.send(
          JSON.stringify({
            _tag: "Chunk",
            requestId: "1",
            values: [item, { kind: "synchronized" }],
          }),
        );
      } else if (frame._tag === "Ack") {
        acks++;
        if (acks === 1) ws.close();
      }
    });
  });
  const { bridge, store } = setup();
  const environment = config.environments[0];
  assert.ok(environment);
  const client = new T3Client(environment, () => "fixture");
  client.socketUrl = async () => fixtureUrl;
  const stream = new ShellStream(
    client,
    () => store.get<number>("cursors", "env"),
    (item) => {
      bridge.ingest("env", item);
      if (item.kind === "synchronized" && requests.length === 2)
        queueMicrotask(() => abort.abort());
    },
  );
  const abort = new AbortController();
  const timeout = setTimeout(() => abort.abort(), 5000);
  try {
    await stream.run(abort.signal);
    assert.equal(requests.length, 2);
    assert.equal(requests[0]?.tag, "orchestration.subscribeShell");
    assert.equal("jsonrpc" in (requests[0] ?? {}), false);
    assert.deepEqual(requests[1]?.payload, {
      afterSequence: 5,
      requestCompletionMarker: true,
    });
    assert.equal(store.get("cursors", "env"), 6);
    assert.equal(store.all("events").length, 1);
  } finally {
    clearTimeout(timeout);
    abort.abort();
    for (const ws of wss.clients) ws.terminate();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
    await new Promise<void>((resolve) => server.close(() => resolve()));
    store.close();
  }
});
