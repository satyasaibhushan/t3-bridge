import { Bridge, type T3Port } from "../src/bridge.js";
import { configSchema, type Thread } from "../src/schema.js";
import { Store } from "../src/store.js";
export const config = configSchema.parse({
  port: 4317,
  statePath: ":memory:",
  enableWrites: true,
  principal: "owner",
  clientTokenEnv: "BRIDGE_TOKEN",
  callbackHosts: ["receiver.example.com"],
  environments: [
    {
      id: "env",
      baseUrl: "http://127.0.0.1:3773",
      tokenEnv: "T3_TOKEN",
      projects: [
        {
          id: "project",
          model: { instanceId: "test-provider", model: "test-model" },
          threadIds: ["thread"],
          allowCreate: true,
        },
      ],
    },
  ],
});
export const thread: Thread = {
  id: "thread",
  projectId: "project",
  title: "Fixture",
  modelSelection: { instanceId: "test-provider", model: "test-model" },
  runtimeMode: "approval-required",
  interactionMode: "default",
  updatedAt: "2026-10-06T12:00:00Z",
  latestTurn: null,
  session: null,
  hasPendingApprovals: false,
  hasPendingUserInput: false,
};
export class FakeT3 implements T3Port {
  calls: unknown[] = [];
  fail = false;
  detail: {
    snapshotSequence: number;
    thread: Thread & {
      messages: Array<{
        id: string;
        role: string;
        text: string;
        createdAt: string;
        turnId: string | null;
      }>;
    };
  } = { snapshotSequence: 0, thread: { ...thread, messages: [] } };
  async shell() {
    return {
      snapshotSequence: 0,
      threads: [thread, { ...thread, id: "hidden" }],
    };
  }
  async thread() {
    return this.detail;
  }
  async dispatch(command: unknown) {
    this.calls.push(command);
    if (this.fail) throw new Error("offline");
    return { sequence: 3 };
  }
}
export function setup(path = ":memory:") {
  const store = new Store(path),
    port = new FakeT3(),
    bridge = new Bridge(config, store, new Map([["env", port]]));
  return { store, port, bridge };
}
export const send = {
  operation: "send" as const,
  environmentId: "env",
  projectId: "project",
  threadId: "thread",
  commandId: "cmd",
  taskId: "task",
  runId: "run",
  text: "fixture only",
};
export const filter = {
  environmentId: "env",
  projectId: "project",
  threadId: "thread",
};
export const secret = `whsec_${Buffer.alloc(32, 7).toString("base64")}`;
