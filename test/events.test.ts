import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import type { Notice } from "../src/bridge.js";
import {
  callbackUrl,
  publicAddress,
  signature,
  signingKey,
  verifyCallback,
  type CallbackTransport,
} from "../src/callback.js";
import { Events, type Subscription } from "../src/events.js";
import { filter, secret, setup, thread } from "./helpers.js";
const input = {
  name: "thread.changed",
  arguments: filter,
  delivery: {
    mode: "webhook",
    url: "https://receiver.example.com/callback",
    secret,
  },
};
function callback() {
  const posts: Array<{
    body: Record<string, unknown>;
    headers: Record<string, string>;
  }> = [];
  let status = 200;
  const transport: CallbackTransport = async (_url, body, headers) => {
    const parsed = JSON.parse(body) as Record<string, unknown>;
    posts.push({ body: parsed, headers });
    return { status, body: JSON.stringify({ challenge: parsed.challenge }) };
  };
  return {
    posts,
    transport,
    setStatus: (value: number) => {
      status = value;
    },
  };
}
function emit(bridge: ReturnType<typeof setup>["bridge"], sequence = 1) {
  bridge.ingest("env", {
    kind: "thread-upserted",
    sequence,
    thread: {
      ...thread,
      latestTurn: { turnId: `turn-${sequence}`, state: "completed" },
    },
  });
}

test("callback address policy blocks local, private, mapped IPv6 and reserved ranges", () => {
  for (const address of [
    "127.0.0.1",
    "10.0.0.1",
    "169.254.169.254",
    "192.168.1.1",
    "100.64.0.1",
    "::1",
    "::ffff:127.0.0.1",
    "fc00::1",
    "fe80::1",
    "224.0.0.1",
    "0.0.0.0",
    "2001:db8::1",
  ])
    assert.equal(publicAddress(address), false, address);
  assert.equal(publicAddress("8.8.8.8"), true);
  for (const url of [
    "http://receiver.example.com/cb",
    "https://evil.example/cb",
    "https://user:pass@receiver.example.com/cb",
    "https://receiver.example.com:444/cb",
    "https://receiver.example.com/cb#fragment",
  ])
    assert.throws(() => callbackUrl(url, ["receiver.example.com"]));
});
test("signatures cover exact bytes, timestamp and ID; signing keys validated", () => {
  const expected = createHmac("sha256", Buffer.alloc(32, 7))
    .update('evt.42.{"hello":"world"}')
    .digest("base64");
  assert.equal(
    signature(secret, "evt", "42", '{"hello":"world"}'),
    `v1,${expected}`,
  );
  for (const bad of [
    "secret",
    "whsec_abc",
    "whsec_!!!",
    `whsec_${Buffer.alloc(100).toString("base64")}`,
  ])
    assert.throws(() => signingKey(bad));
});
test("verification requires echoed signed fresh challenge and successful status", async () => {
  const c = callback();
  await verifyCallback(c.transport, {
    id: "sub",
    url: input.delivery.url,
    secret,
  });
  assert.equal(c.posts[0]?.body.type, "verification");
  assert.match(c.posts[0]?.headers["webhook-signature"] ?? "", /^v1,/);
  await assert.rejects(
    verifyCallback(
      async () => ({ status: 200, body: '{"challenge":"wrong"}' }),
      { id: "sub", url: input.delivery.url, secret },
    ),
    /challenge_failed/,
  );
  await assert.rejects(
    verifyCallback(async () => ({ status: 302, body: "{}" }), {
      id: "sub",
      url: input.delivery.url,
      secret,
    }),
    /challenge_failed/,
  );
});
test("subscribe is deterministic, verifies before events, persists cursor and unsubscribe is idempotent", async () => {
  const { bridge, store } = setup();
  const c = callback();
  const events = new Events(bridge, c.transport);
  try {
    const a = await events.subscribe(input),
      b = await events.subscribe({
        ...input,
        arguments: {
          threadId: "thread",
          projectId: "project",
          environmentId: "env",
        },
      });
    assert.equal(a.id, b.id);
    assert.equal(c.posts.length, 1);
    emit(bridge);
    await events.flush();
    assert.equal(c.posts.length, 2);
    assert.equal(c.posts[1]?.body.name, "thread.changed");
    assert.equal(store.get<Subscription>("subscriptions", a.id)?.cursor, 1);
    const unsub = {
      ...input,
      delivery: { mode: "webhook", url: input.delivery.url },
    };
    events.unsubscribe(unsub);
    events.unsubscribe(unsub);
    emit(bridge, 2);
    await events.flush();
    assert.equal(c.posts.length, 2);
  } finally {
    store.close();
  }
});
test("retries preserve event ID and do not advance unacknowledged cursor; 410/413 stop", async () => {
  const { bridge, store } = setup();
  const c = callback(),
    events = new Events(bridge, c.transport);
  try {
    const a = await events.subscribe(input);
    emit(bridge);
    c.setStatus(503);
    await events.flush();
    const sub = store.get<Subscription>("subscriptions", a.id);
    assert.ok(sub);
    assert.equal(sub.cursor, 0);
    const failedId = c.posts[1]?.body.eventId;
    store.set("subscriptions", a.id, { ...sub, nextAttempt: 0 });
    c.setStatus(200);
    await events.flush();
    assert.equal(c.posts[2]?.body.eventId, failedId);
    assert.equal(store.get<Subscription>("subscriptions", a.id)?.cursor, 1);
    emit(bridge, 2);
    c.setStatus(410);
    await events.flush();
    assert.equal(
      store.get<Subscription>("subscriptions", a.id)?.disabled,
      "gone",
    );
    const count = c.posts.length;
    await events.flush();
    assert.equal(c.posts.length, count);
  } finally {
    store.close();
  }
});
test("subscriptions resume after restart; expiry/revoked resource suppress delivery", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bridge-events-")),
    path = join(dir, "state.sqlite"),
    first = setup(path),
    c = callback();
  const a = await new Events(first.bridge, c.transport).subscribe(input);
  emit(first.bridge);
  first.store.close();
  const second = setup(path);
  try {
    const events = new Events(second.bridge, c.transport);
    await events.flush();
    assert.equal(c.posts.at(-1)?.body.name, "thread.changed");
    const s = second.store.get<Subscription>("subscriptions", a.id);
    assert.ok(s);
    second.store.set("subscriptions", a.id, { ...s, expiresAt: 0 });
    emit(second.bridge, 2);
    const count = c.posts.length;
    await events.flush();
    assert.equal(c.posts.length, count);
  } finally {
    second.store.close();
    rmSync(dir, { recursive: true });
  }
});
test("refresh rotates secret, canonical identity stays stable, cannot skip pending events", async () => {
  const { bridge, store } = setup();
  const c = callback(),
    events = new Events(bridge, c.transport);
  try {
    const a = await events.subscribe(input);
    emit(bridge);
    c.setStatus(503);
    await events.flush();
    c.setStatus(200);
    const replacement = `whsec_${Buffer.alloc(32, 8).toString("base64")}`;
    const b = await events.subscribe({
      ...input,
      cursor: "1",
      delivery: { ...input.delivery, secret: replacement },
    });
    assert.equal(a.id, b.id);
    assert.equal(b.cursor, "0");
    await events.flush();
    assert.equal(
      c.posts.at(-1)?.headers["webhook-signature"]?.split(" ").length,
      2,
    );
  } finally {
    store.close();
  }
});
test("failed verification stores no subscription and invalid cursor fails", async () => {
  const { bridge, store } = setup();
  try {
    const events = new Events(bridge, async () => ({
      status: 200,
      body: "{}",
    }));
    await assert.rejects(events.subscribe(input), /challenge_failed/);
    assert.equal(store.all("subscriptions").length, 0);
    await assert.rejects(
      new Events(bridge, callback().transport).subscribe({
        ...input,
        cursor: "100",
      }),
      /invalid_cursor/,
    );
  } finally {
    store.close();
  }
});
test("unsubscribing while delivery is in flight cannot resurrect subscription", async () => {
  const { bridge, store } = setup();
  let release: (value: { status: number; body: string }) => void = () => {};
  const transport: CallbackTransport = async (_url, body) => {
    const d = JSON.parse(body) as { type?: string; challenge?: string };
    if (d.type === "verification")
      return { status: 200, body: JSON.stringify({ challenge: d.challenge }) };
    return new Promise((resolve) => {
      release = resolve;
    });
  };
  try {
    const events = new Events(bridge, transport);
    await events.subscribe(input);
    emit(bridge);
    const pending = events.flush();
    events.unsubscribe({
      ...input,
      delivery: { mode: "webhook", url: input.delivery.url },
    });
    release({ status: 200, body: "" });
    await pending;
    assert.equal(store.all("subscriptions").length, 0);
  } finally {
    store.close();
  }
});
test("event filtering keeps other thread data out of callbacks", async () => {
  const { bridge, store } = setup();
  const c = callback(),
    events = new Events(bridge, c.transport);
  try {
    await events.subscribe(input);
    store.set("events", "0000000000000001", {
      eventId: "evt",
      name: "thread.changed",
      timestamp: thread.updatedAt,
      cursor: "1",
      data: {
        ...filter,
        threadId: "hidden",
        status: "completed",
        reason: "completion",
        turnId: null,
        taskId: null,
        runId: null,
        recovered: false,
      },
    } satisfies Notice);
    await events.flush();
    assert.equal(c.posts.length, 1);
  } finally {
    store.close();
  }
});

test("unsubscribe cancels a refresh waiting for callback verification, including after restart", async () => {
  const { bridge, store } = setup();
  const initial = new Events(bridge, callback().transport);
  const sub = await initial.subscribe(input);
  await initial.flush();
  let release: () => void = () => {};
  let notices = 0;
  const waiting = new Events(bridge, async (_url, body) => {
    const d = JSON.parse(body) as { type?: string; challenge?: string };
    if (d.type !== "verification") notices++;
    await new Promise<void>((resolve) => {
      release = resolve;
    });
    return { status: 200, body: JSON.stringify({ challenge: d.challenge }) };
  });
  try {
    const pending = waiting.subscribe(input);
    // A separate Events instance uses the same durable generation/tombstone.
    initial.unsubscribe({
      ...input,
      delivery: { mode: "webhook", url: input.delivery.url },
    });
    release();
    await assert.rejects(pending, /subscription_superseded/);
    emit(bridge);
    await waiting.flush();
    assert.equal(store.get("subscriptions", sub.id), undefined);
    assert.equal(notices, 0);
    // A later deliberate subscribe is still allowed.
    await initial.subscribe(input);
    await initial.flush();
    assert.ok(store.get("subscriptions", sub.id));
  } finally {
    store.close();
  }
});
