import {
  BridgeError,
  type Command,
  type Config,
  type Filter,
  type StreamItem,
  type Thread,
  commandSchema,
  digest,
  filterSchema,
  readSchema,
} from "./schema.js";
import type { Store } from "./store.js";
export interface T3Port {
  shell(): Promise<{ snapshotSequence: number; threads: Thread[] }>;
  thread(
    id: string,
    window?: { turnLimit: number; beforeCursor?: string },
  ): Promise<unknown>;
  dispatch(command: unknown): Promise<{ sequence: number }>;
}
export interface Receipt {
  hash: string;
  command: Command;
  wire: Record<string, unknown>;
  state: "pending" | "accepted";
  result?: { sequence: number };
}
export interface Mapping {
  taskId: string;
  runId: string;
  projectId: string;
  threadId: string;
  commandId: string;
  messageId?: string;
  turnId?: string;
}
export interface Notice {
  eventId: string;
  name: "thread.changed";
  timestamp: string;
  data: Filter & {
    reason: string;
    status: string;
    turnId: string | null;
    taskId: string | null;
    runId: string | null;
    recovered: boolean;
  };
  cursor: string;
}
export interface Approval {
  hash: string;
  expiresAt: number;
}
export function stateOf(t: Thread): string {
  if (t.hasPendingApprovals || t.hasPendingUserInput) return "input-required";
  if (t.session?.status === "error" || t.latestTurn?.state === "error")
    return "failed";
  if (t.latestTurn?.state === "interrupted") return "interrupted";
  if (t.latestTurn?.state === "completed") return "completed";
  return t.latestTurn?.state ?? t.session?.status ?? "idle";
}
export class Bridge {
  readonly inFlight = new Map<
    string,
    { hash: string; promise: Promise<unknown> }
  >();
  onEvent: () => void = () => {};
  constructor(
    readonly config: Config,
    readonly store: Store,
    readonly ports: Map<string, T3Port>,
  ) {}
  scope(filter: Filter) {
    const e = this.config.environments.find(
      (e) => e.id === filter.environmentId,
    );
    const p = e?.projects.find((p) => p.id === filter.projectId);
    if (!e || !p) throw new BridgeError("scope_denied");
    if (
      filter.threadId &&
      !this.allowed(filter.environmentId, filter.projectId, filter.threadId)
    )
      throw new BridgeError("thread_denied");
    return { environment: e, project: p };
  }
  allowed(env: string, project: string, thread: string): boolean {
    const p = this.config.environments
      .find((e) => e.id === env)
      ?.projects.find((p) => p.id === project);
    const m = this.store.get<Mapping>("created", digest([env, thread]));
    return (
      !!p &&
      (p.threadIds.includes(thread) ||
        (p.allowCreate && m?.projectId === project))
    );
  }
  port(env: string): T3Port {
    const p = this.ports.get(env);
    if (!p) throw new BridgeError("environment_unavailable");
    return p;
  }
  async list(raw: unknown) {
    const filter = filterSchema.parse(raw);
    this.scope(filter);
    const s = await this.port(filter.environmentId).shell();
    return {
      snapshotSequence: s.snapshotSequence,
      threads: s.threads
        .filter(
          (t) =>
            t.projectId === filter.projectId &&
            this.allowed(filter.environmentId, t.projectId, t.id) &&
            (!filter.threadId || t.id === filter.threadId),
        )
        .map((t) => ({
          threadId: t.id,
          projectId: t.projectId,
          title: t.title,
          status: stateOf(t),
          turnId: t.latestTurn?.turnId ?? null,
          updatedAt: t.updatedAt,
        })),
    };
  }
  async read(raw: unknown) {
    const f = readSchema.parse(raw);
    this.scope(f);
    const result = await this.port(f.environmentId).thread(f.threadId, {
      turnLimit: f.turnLimit,
      ...(f.beforeCursor ? { beforeCursor: f.beforeCursor } : {}),
    });
    // The transport's projection is validated again at the authorization boundary.
    const { detailSchema } = await import("./t3.js");
    const detail = detailSchema.parse(result);
    if (
      detail.thread.id !== f.threadId ||
      detail.thread.projectId !== f.projectId
    )
      throw new BridgeError("thread_project_mismatch");
    return detail;
  }
  approvalHash(command: Command): string {
    const { environment, project } = this.scope({
      ...command,
      threadId: command.operation === "create" ? undefined : command.threadId,
    });
    return digest({
      principal: this.config.principal,
      command,
      target: environment.baseUrl,
      model: project.model,
      runtimeMode: "approval-required",
    });
  }
  approve(raw: unknown, ttlMs = 300000): { hash: string; expiresAt: number } {
    if (!this.config.enableWrites) throw new BridgeError("writes_disabled");
    const c = commandSchema.parse(raw);
    const approval = {
      hash: this.approvalHash(c),
      expiresAt: Date.now() + Math.min(ttlMs, 300000),
    };
    if (
      c.operation === "create" &&
      !this.scope({ ...c, threadId: undefined }).project.allowCreate
    )
      throw new BridgeError("create_denied");
    this.store.set(
      "approvals",
      digest([this.config.principal, c.environmentId, c.commandId]),
      approval,
    );
    return approval;
  }
  async execute(raw: unknown): Promise<unknown> {
    if (!this.config.enableWrites) throw new BridgeError("writes_disabled");
    const c = commandSchema.parse(raw),
      key = digest([this.config.principal, c.environmentId, c.commandId]);
    const active = this.inFlight.get(key);
    if (active) {
      if (active.hash !== this.approvalHash(c))
        throw new BridgeError("command_id_conflict");
      return active.promise;
    }
    const promise = this.dispatch(c, key);
    this.inFlight.set(key, { hash: this.approvalHash(c), promise });
    try {
      return await promise;
    } finally {
      this.inFlight.delete(key);
    }
  }
  private async dispatch(c: Command, key: string) {
    const { project } = this.scope({
      ...c,
      threadId: c.operation === "create" ? undefined : c.threadId,
    });
    if (c.operation === "create" && !project.allowCreate)
      throw new BridgeError("create_denied");
    const hash = this.approvalHash(c);
    let receipt = this.store.get<Receipt>("receipts", key);
    if (receipt && receipt.hash !== hash)
      throw new BridgeError("command_id_conflict");
    if (receipt?.state === "accepted")
      return { commandId: c.commandId, ...receipt.result };
    if (!receipt) {
      if (c.operation !== "create") {
        const detail = await this.read({
          environmentId: c.environmentId,
          projectId: c.projectId,
          threadId: c.threadId,
        });
        if (
          c.operation === "interrupt" &&
          detail.thread.session?.activeTurnId !== c.turnId
        )
          throw new BridgeError("turn_not_active");
        if (c.operation === "send" && detail.thread.session?.activeTurnId)
          throw new BridgeError("thread_busy");
      }
      const createdAt = new Date().toISOString();
      const common = {
        commandId: `bridge-${digest([this.config.principal, c.environmentId, c.commandId])}`,
        threadId: c.threadId,
        createdAt,
      };
      const wire: Record<string, unknown> =
        c.operation === "create"
          ? {
              ...common,
              type: "thread.create",
              projectId: c.projectId,
              title: c.title,
              modelSelection: project.model,
              runtimeMode: "approval-required",
              interactionMode: "default",
              branch: null,
              worktreePath: null,
            }
          : c.operation === "send"
            ? {
                ...common,
                type: "thread.turn.start",
                message: {
                  messageId: `bridge-${digest([c.environmentId, c.commandId]).slice(0, 32)}`,
                  role: "user",
                  text: c.text,
                  attachments: [],
                },
                runtimeMode: "approval-required",
                interactionMode: "default",
              }
            : { ...common, type: "thread.turn.interrupt", turnId: c.turnId };
      receipt = { hash, command: c, wire, state: "pending" };
      this.store.transaction(() => {
        const a = this.store.get<Approval>("approvals", key);
        if (!a || a.hash !== hash || a.expiresAt <= Date.now())
          throw new BridgeError("approval_required");
        this.store.set("receipts", key, receipt);
        this.store.delete("approvals", key);
        if (c.operation === "send")
          this.store.set("mappings", digest([c.environmentId, c.threadId]), {
            taskId: c.taskId,
            runId: c.runId,
            projectId: c.projectId,
            threadId: c.threadId,
            commandId: c.commandId,
            messageId: `bridge-${digest([c.environmentId, c.commandId]).slice(0, 32)}`,
          } satisfies Mapping);
      });
    }
    // A lost response remains pending. Retrying sends exactly the same persisted
    // command ID, timestamp and body, allowing T3's durable receipt to deduplicate.
    const result = await this.port(c.environmentId).dispatch(receipt.wire);
    this.store.transaction(() => {
      this.store.set("receipts", key, {
        ...receipt,
        state: "accepted",
        result,
      });
      const mapping: Mapping = {
        taskId: c.taskId,
        runId: c.runId,
        projectId: c.projectId,
        threadId: c.threadId,
        commandId: c.commandId,
      };
      if (c.operation === "create")
        this.store.set(
          "mappings",
          digest([c.environmentId, c.threadId]),
          mapping,
        );
      if (c.operation === "create")
        this.store.set(
          "created",
          digest([c.environmentId, c.threadId]),
          mapping,
        );
    });
    return { commandId: c.commandId, ...result };
  }
  async observe(env: string, item: StreamItem): Promise<void> {
    const threads =
      item.kind === "snapshot"
        ? item.snapshot.threads
        : item.kind === "thread-upserted"
          ? [item.thread]
          : [];
    for (const thread of threads) {
      if (!this.allowed(env, thread.projectId, thread.id)) continue;
      const key = digest([env, thread.id]),
        mapping = this.store.get<Mapping>("mappings", key);
      if (!mapping?.messageId || mapping.turnId || !thread.latestTurn) continue;
      const detail = await this.read({
        environmentId: env,
        projectId: thread.projectId,
        threadId: thread.id,
      });
      const message = detail.thread.messages.find(
        (m) => m.id === mapping.messageId,
      );
      if (message?.turnId)
        this.store.set("mappings", key, { ...mapping, turnId: message.turnId });
    }
    this.ingest(env, item);
  }
  ingest(env: string, item: StreamItem): void {
    if (item.kind === "synchronized") return;
    const sequence =
      item.kind === "snapshot" ? item.snapshot.snapshotSequence : item.sequence;
    const oldCursor = this.store.get<number>("cursors", env);
    if (
      item.kind === "snapshot" &&
      oldCursor !== undefined &&
      sequence < oldCursor
    )
      throw new BridgeError("t3_cursor_regressed");
    if (oldCursor !== undefined && sequence <= oldCursor) return;
    this.store.transaction(() => {
      const emit = (t: Thread, recovered: boolean) => {
        if (!this.allowed(env, t.projectId, t.id)) return;
        const key = digest([env, t.id]);
        const previous = this.store.get<Thread>("threads", key);
        this.store.set("threads", key, t);
        const status = stateOf(t);
        const changed =
          !previous ||
          stateOf(previous) !== status ||
          previous.latestTurn?.turnId !== t.latestTurn?.turnId ||
          digest(previous.planProgress ?? null) !==
            digest(t.planProgress ?? null);
        // First snapshot establishes a baseline. A fallback snapshot after a gap
        // emits current state with recovered=true; it cannot recreate lost history.
        if (!changed || (oldCursor === undefined && recovered)) return;
        const n = (this.store.get<number>("meta", "eventSequence") ?? 0) + 1;
        let m = this.store.get<Mapping>("mappings", key);
        const turnId = t.latestTurn?.turnId;
        if (m?.turnId !== turnId) m = undefined;
        const event: Notice = {
          eventId: `evt_${digest([env, sequence, t.id])}`,
          name: "thread.changed",
          timestamp: t.updatedAt,
          data: {
            environmentId: env,
            projectId: t.projectId,
            threadId: t.id,
            reason:
              status === "input-required"
                ? "input-required"
                : status === "completed"
                  ? "completion"
                  : status === "failed"
                    ? "failure"
                    : status === "interrupted"
                      ? "interrupted"
                      : "progress",
            status,
            turnId: t.latestTurn?.turnId ?? null,
            taskId: m?.taskId ?? null,
            runId: m?.runId ?? null,
            recovered,
          },
          cursor: String(n),
        };
        this.store.set("events", String(n).padStart(16, "0"), event);
        this.store.set("meta", "eventSequence", n);
      };
      if (item.kind === "snapshot")
        for (const t of item.snapshot.threads) emit(t, true);
      if (item.kind === "thread-upserted") emit(item.thread, false);
      if (item.kind === "thread-removed")
        this.store.delete("threads", digest([env, item.threadId]));
      this.store.set("cursors", env, sequence);
    });
    this.onEvent();
  }
}
