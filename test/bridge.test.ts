import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { Bridge, type Notice, stateOf } from "../src/bridge.js";
import { configSchema } from "../src/schema.js";
import { config, filter, send, setup, thread } from "./helpers.js";

test("allowlists filter shell reads and reject cross-project reads before transport", async () => {
  const { bridge, store } = setup();
  try {
    assert.equal((await bridge.list(filter)).threads.length, 1);
    await assert.rejects(
      bridge.read({ ...filter, threadId: "hidden" }),
      /thread_denied/,
    );
    await assert.rejects(
      bridge.read({ ...filter, projectId: "wrong" }),
      /scope_denied/,
    );
  } finally {
    store.close();
  }
});
test("default write-disabled config cannot approve or execute", async () => {
  const { port, store } = setup();
  try {
    const c = configSchema.parse({ ...config, enableWrites: undefined }),
      b = new Bridge(c, store, new Map([["env", port]]));
    assert.throws(() => b.approve(send), /writes_disabled/);
    await assert.rejects(b.execute(send), /writes_disabled/);
  } finally {
    store.close();
  }
});
test("approval binds exact body and target, duplicate and concurrent commands dispatch once", async () => {
  const { bridge, port, store } = setup();
  try {
    await assert.rejects(bridge.execute(send), /approval_required/);
    assert.equal(port.calls.length, 0);
    bridge.approve(send);
    await assert.rejects(
      bridge.execute({ ...send, text: "changed" }),
      /approval_required/,
    );
    const [a, b] = await Promise.all([
      bridge.execute(send),
      bridge.execute(send),
    ]);
    assert.deepEqual(a, b);
    assert.equal(port.calls.length, 1);
    await bridge.execute(send);
    assert.equal(port.calls.length, 1);
    await assert.rejects(
      bridge.execute({ ...send, text: "different" }),
      /command_id_conflict/,
    );
    const wire = port.calls[0] as Record<string, unknown>;
    assert.equal(wire.type, "thread.turn.start");
    assert.equal(wire.runtimeMode, "approval-required");
    assert.ok(!("bootstrap" in wire));
  } finally {
    store.close();
  }
});
test("pending command survives process restart and retries identical wire bytes", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bridge-test-"));
  const path = join(dir, "state.sqlite");
  const first = setup(path);
  first.bridge.approve(send);
  first.port.fail = true;
  await assert.rejects(first.bridge.execute(send), /offline/);
  const wire = first.port.calls[0];
  first.store.close();
  const second = setup(path);
  try {
    await second.bridge.execute(send);
    assert.deepEqual(second.port.calls[0], wire);
  } finally {
    second.store.close();
    rmSync(dir, { recursive: true });
  }
});
test("approval expires and policy/model changes invalidate it", async () => {
  const { bridge, store, port } = setup();
  try {
    bridge.approve(send, -1);
    await assert.rejects(bridge.execute(send), /approval_required/);
    bridge.approve(send);
    const c = structuredClone(config);
    const p = c.environments[0]?.projects[0];
    assert.ok(p);
    p.model.model = "other";
    await assert.rejects(
      new Bridge(c, store, new Map([["env", port]])).execute(send),
      /approval_required/,
    );
  } finally {
    store.close();
  }
});
test("explicit active turn is required to cancel; uncertain cancel retries after turn settles", async () => {
  const { bridge, port, store } = setup();
  const c = { ...send, operation: "interrupt", turnId: "turn" };
  delete (c as Partial<typeof send>).text;
  try {
    bridge.approve(c);
    await assert.rejects(bridge.execute(c), /turn_not_active/);
    port.detail.thread.session = {
      status: "running",
      activeTurnId: "turn",
      lastError: null,
    };
    bridge.approve(c);
    port.fail = true;
    await assert.rejects(bridge.execute(c), /offline/);
    port.detail.thread.session = null;
    port.fail = false;
    await bridge.execute(c);
    assert.equal(port.calls.length, 2);
  } finally {
    store.close();
  }
});
test("thread create uses configured model and grants only its returned mapping", async () => {
  const { bridge, port, store } = setup();
  try {
    const c = {
      operation: "create",
      environmentId: "env",
      projectId: "project",
      threadId: "new-thread",
      title: "New",
      commandId: "create1",
      taskId: "task",
      runId: "run",
    };
    bridge.approve(c);
    await bridge.execute(c);
    assert.ok(bridge.allowed("env", "project", "new-thread"));
    assert.equal(bridge.allowed("env", "other", "new-thread"), false);
    assert.deepEqual(
      (port.calls[0] as Record<string, unknown>).modelSelection,
      { instanceId: "test-provider", model: "test-model" },
    );
  } finally {
    store.close();
  }
});
test("stream cursors and events commit together; replay, duplicate, gap recovery, failure/input signals", () => {
  const { bridge, store } = setup();
  try {
    bridge.ingest("env", {
      kind: "snapshot",
      snapshot: { snapshotSequence: 1, threads: [thread] },
    });
    assert.equal(store.all("events").length, 0);
    const running = {
      ...thread,
      latestTurn: { turnId: "turn", state: "running" },
    };
    bridge.ingest("env", {
      kind: "thread-upserted",
      sequence: 2,
      thread: running,
    });
    bridge.ingest("env", {
      kind: "thread-upserted",
      sequence: 2,
      thread: running,
    });
    bridge.ingest("env", {
      kind: "thread-upserted",
      sequence: 3,
      thread: { ...running, id: "hidden" },
    });
    bridge.ingest("env", {
      kind: "thread-upserted",
      sequence: 4,
      thread: { ...running, hasPendingUserInput: true },
    });
    bridge.ingest("env", {
      kind: "snapshot",
      snapshot: {
        snapshotSequence: 10,
        threads: [
          { ...running, latestTurn: { turnId: "turn", state: "error" } },
        ],
      },
    });
    const events = store.all<Notice>("events").map(([, e]) => e);
    assert.equal(events.length, 3);
    assert.deepEqual(
      events.map((e) => e.data.reason),
      ["progress", "input-required", "failure"],
    );
    assert.equal(events[2]?.data.recovered, true);
    assert.equal(store.get("cursors", "env"), 10);
    assert.equal(
      stateOf({ ...thread, latestTurn: { turnId: "t", state: "completed" } }),
      "completed",
    );
  } finally {
    store.close();
  }
});

test("correlation uses actual dispatched message turn ID, never the next unrelated native turn", async () => {
  const { bridge, port, store } = setup();
  try {
    bridge.approve(send);
    await bridge.execute(send);
    const wire = port.calls[0] as { message: { messageId: string } };
    const native = {
      ...thread,
      latestTurn: { turnId: "native-turn", state: "completed" },
    };
    await bridge.observe("env", {
      kind: "thread-upserted",
      sequence: 1,
      thread: native,
    });
    assert.equal(store.all<Notice>("events")[0]?.[1].data.runId, null);
    port.detail.thread.messages.push({
      id: wire.message.messageId,
      role: "user",
      text: send.text,
      createdAt: thread.updatedAt,
      turnId: "bridge-turn",
    });
    await bridge.observe("env", {
      kind: "thread-upserted",
      sequence: 2,
      thread: {
        ...thread,
        latestTurn: { turnId: "bridge-turn", state: "completed" },
      },
    });
    assert.equal(store.all<Notice>("events")[1]?.[1].data.runId, "run");
    await bridge.observe("env", {
      kind: "thread-upserted",
      sequence: 3,
      thread: {
        ...thread,
        latestTurn: { turnId: "another-native-turn", state: "completed" },
      },
    });
    assert.equal(store.all<Notice>("events")[2]?.[1].data.runId, null);
  } finally {
    store.close();
  }
});
test("event persistence failure cannot advance replay cursor", () => {
  const { bridge, store } = setup();
  const set = store.set.bind(store);
  try {
    bridge.ingest("env", {
      kind: "snapshot",
      snapshot: { snapshotSequence: 1, threads: [thread] },
    });
    store.set = (bucket, key, value) => {
      if (bucket === "events") throw new Error("disk_full");
      set(bucket, key, value);
    };
    assert.throws(
      () =>
        bridge.ingest("env", {
          kind: "thread-upserted",
          sequence: 2,
          thread: {
            ...thread,
            latestTurn: { turnId: "turn", state: "completed" },
          },
        }),
      /disk_full/,
    );
    assert.equal(store.get("cursors", "env"), 1);
    assert.equal(store.all("events").length, 0);
  } finally {
    store.close();
  }
});
test("server cursor regression fails closed and deny-all configuration is valid", () => {
  const { bridge, store } = setup();
  try {
    bridge.ingest("env", {
      kind: "snapshot",
      snapshot: { snapshotSequence: 10, threads: [] },
    });
    assert.throws(
      () =>
        bridge.ingest("env", {
          kind: "snapshot",
          snapshot: { snapshotSequence: 1, threads: [] },
        }),
      /t3_cursor_regressed/,
    );
    assert.equal(store.get("cursors", "env"), 10);
    assert.equal(
      configSchema.parse({ ...config, environments: [] }).environments.length,
      0,
    );
  } finally {
    store.close();
  }
});

test("send rejects missing/changed effective runtime, interaction, provider and model after approval", async () => {
  for (const changed of [
    { runtimeMode: "full-access" },
    { runtimeMode: undefined },
    { interactionMode: "plan" },
    { interactionMode: undefined },
    { modelSelection: { instanceId: "other-provider", model: "test-model" } },
    { modelSelection: { instanceId: "test-provider", model: "other-model" } },
    { modelSelection: undefined },
    {
      modelSelection: {
        ...thread.modelSelection!,
        options: [{ id: "effort", value: "high" }],
      },
    },
  ]) {
    const { bridge, port, store } = setup();
    try {
      bridge.approve(send);
      Object.assign(port.detail.thread, changed);
      await assert.rejects(
        bridge.execute(send),
        /thread_execution_policy_mismatch/,
      );
      assert.equal(port.calls.length, 0);
    } finally {
      store.close();
    }
  }
});

test("pending send retries recheck execution policy and include the approved explicit model", async () => {
  const { bridge, port, store } = setup();
  try {
    bridge.approve(send);
    port.fail = true;
    await assert.rejects(bridge.execute(send), /offline/);
    assert.deepEqual(
      (port.calls[0] as { modelSelection: unknown }).modelSelection,
      thread.modelSelection,
    );
    port.detail.thread.runtimeMode = "full-access";
    port.fail = false;
    await assert.rejects(
      bridge.execute(send),
      /thread_execution_policy_mismatch/,
    );
    assert.equal(port.calls.length, 1);
  } finally {
    store.close();
  }
});

test("overlapping sends and delayed observations retain every proven turn mapping across restart", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bridge-correlation-"));
  const path = join(dir, "state.sqlite");
  const { bridge, port, store } = setup(path);
  let closed = false;
  const second = { ...send, commandId: "cmd2", runId: "run2" };
  try {
    bridge.approve(send);
    await bridge.execute(send);
    const firstMessage = (port.calls[0] as { message: { messageId: string } })
      .message.messageId;
    let release: (value: typeof port.detail) => void = () => {};
    const originalThread = port.thread.bind(port);
    port.thread = () =>
      new Promise((resolve) => {
        release = resolve;
      });
    const delayed = bridge.observe("env", {
      kind: "thread-upserted",
      sequence: 1,
      thread: {
        ...thread,
        latestTurn: { turnId: "turn1", state: "completed" },
      },
    });
    port.thread = originalThread;
    bridge.approve(second);
    await bridge.execute(second);
    const secondMessage = (port.calls[1] as { message: { messageId: string } })
      .message.messageId;
    const message = (id: string, turnId: string) => ({
      id,
      turnId,
      role: "user",
      text: "fixture",
      createdAt: thread.updatedAt,
    });
    release({
      snapshotSequence: 1,
      thread: { ...thread, messages: [message(firstMessage, "turn1")] },
    });
    await delayed;
    port.detail.thread.messages = [
      message(firstMessage, "turn1"),
      message(secondMessage, "turn2"),
    ];
    await bridge.observe("env", {
      kind: "thread-upserted",
      sequence: 2,
      thread: { ...thread, latestTurn: { turnId: "turn2", state: "running" } },
    });
    assert.deepEqual(
      store.all<Notice>("events").map(([, e]) => e.data.runId),
      ["run", "run2"],
    );
    assert.equal(store.all("mappings").length, 2);
    assert.equal(store.all("turnMappings").length, 2);
    store.close();
    closed = true;
    const restarted = setup(path);
    try {
      restarted.bridge.ingest("env", {
        kind: "thread-upserted",
        sequence: 3,
        thread: {
          ...thread,
          latestTurn: { turnId: "turn1", state: "completed" },
        },
      });
      assert.equal(
        restarted.store.all<Notice>("events").at(-1)?.[1].data.runId,
        "run",
      );
    } finally {
      restarted.store.close();
    }
  } finally {
    if (!closed) store.close();
    rmSync(dir, { recursive: true });
  }
});
