# t3-bridge

One private TypeScript service connecting dot MCP tools and MCP Events to T3 Code **0.0.45**, orchestration protocol **1**. No UI, task scheduler, provider configuration, or task database. Task Finder can later reuse `Bridge.execute`. Backfill is untouched.

**Implemented and fixture-tested, not yet connected end to end to dot or paired with live T3.** Installation and tests do not create persistent access, deploy a service, or launch agent tasks.

## Local use

Requires Node 22.13+ for `node:sqlite` (experimental on Node 22).

```sh
npm ci --ignore-scripts
npm run typecheck
npm run lint
npm test
npm run build
node dist/main.js check config.example.json
```

Tests use synthetic credentials and temporary loopback fixture servers, never the installed T3 app or a real callback service.

The example denies all resources and disables writes. When authorized, copy it to `config.local.json`, configure exact discovered IDs, and supply dedicated credentials using the named environment variables. The inbound client token requires at least 32 characters. Never put tokens in config, command files, Git, logs, or chat.

```sh
node dist/main.js serve config.local.json
```

The MCP endpoint binds **127.0.0.1 only**, at `/mcp`. All requests require bearer authentication; browser Origin requests are rejected. Remote access requires a separately approved authenticated HTTPS endpoint or private tunnel. The service creates neither.

An environment entry has this shape. These IDs are illustrative, not discovered resources:

```json
{
  "id": "verified-environment-id",
  "baseUrl": "http://127.0.0.1:3773",
  "tokenEnv": "T3_BRIDGE_ACCESS_TOKEN",
  "projects": [{
    "id": "verified-project-id",
    "model": { "instanceId": "existing-provider-instance", "model": "existing-model" },
    "threadIds": ["explicitly-approved-thread-id"],
    "allowCreate": false
  }]
}
```

The adapter verifies environment identity and protocol before sending its dedicated token. HTTPS is required except on loopback. Existing SSH routes and provider configuration stay outside the bridge. A model link does not prove a provider's backend; the bridge never changes that routing.

## Tools and write approvals

- `t3_list_threads`: explicitly allowed thread metadata for one project.
- `t3_read_thread`: recent conversation turns, default 20 and maximum 100, with T3's `beforeCursor` for older pages. Provider session internals are excluded.
- `t3_status`: stream connectivity, pending command IDs, subscription delivery health.
- `t3_create`, `t3_send`, `t3_interrupt`: advertised only with `enableWrites: true`, each requiring exact local approval. No generic shell, files, admin, project-create, approval-response, or dispatch tool.

Approval is a local operator action, never an MCP tool. After user approval of the exact action, review a JSON command and approve it for five minutes:

```sh
node dist/main.js approve config.local.json /path/to/reviewed-command.json
```

```json
{
  "operation": "send",
  "environmentId": "verified-environment-id",
  "projectId": "verified-project-id",
  "threadId": "explicitly-approved-thread-id",
  "commandId": "unique-command-id",
  "taskId": "source-task-id",
  "runId": "source-run-id",
  "text": "The exact approved prompt"
}
```

The matching tool takes these arguments without `operation`. Create uses `title` instead of `text`; interrupt uses an explicit `turnId`. Approval binds principal, target URL, model, operation, IDs and payload. Create/send use `approval-required` runtime mode. Create uses the configured model without worktree/bootstrap scripts. Send rejects an already active turn.

Receipts persist before dispatch. T3 receives deterministic namespaced command and message IDs. A lost response leaves a pending receipt: retry with the identical command ID and arguments to resend the exact timestamp/body. Changed content is rejected; accepted results return without another dispatch. Pending writes are not automatically executed on reconnect. Keep the state file to retain these guarantees.

## Events and recovery

MCP Events advertises `2026-07-28` through `server/discover`. It implements `events/list`, `events/subscribe`, and `events/unsubscribe`. Event `thread.changed` supports environment/project and optional thread filters. Payloads contain status/reason, T3 turn ID, proven task/run correlation and `recovered`, with no prompt/transcript text. Read tools retrieve details.

The initial T3 shell snapshot establishes a baseline. Status, turn, and plan changes emit progress, completion, failure, input-required or interrupted notices. Replay cursor and event persistence is atomic. Duplicate sequences are ignored. Reconnect resumes with `afterSequence`. A fallback snapshot reports changed current state with `recovered: true`, but cannot reconstruct intermediate history T3 no longer retains. A regressed server cursor stops processing and requires a reviewed state reset.

Run correlation joins the dispatched message ID to its turn ID in thread detail. Unproven correlation stays null. Unrelated native turns are not assigned to an earlier bridge run.

Subscriptions persist across restarts. Before application delivery, callbacks must echo a fresh signed challenge. Delivery uses Standard Webhooks HMAC signatures over exact request bytes, stable event IDs and fresh signing timestamps. Secrets must decode to 24–64 bytes. Rotation overlaps for five minutes. Callback destinations must be explicitly allowlisted HTTPS hosts, resolved and checked for public addresses on every connection, then pinned to the validated address with normal TLS verification. Redirects are forbidden.

Delivery is ordered per subscription and at least once. Transient failures retry exponentially up to eight attempts; 410/413 stop immediately. Exhaustion is visible in `t3_status`; refresh resumes the unacknowledged cursor. Expiration is at most 24 hours. Unsubscribe is idempotent. Up to 10,000 events are retained; expired replay history returns `truncated: true`. Consumers deduplicate by `eventId`.

The owner-only (0600) SQLite file contains receipts, correlation, compact thread status, cursors, events, approvals and subscriptions, not a transcript/task database. Pending command bodies and callback secrets are sensitive local state, not encrypted by this package. Use the host's encrypted private storage and one service process per state file.

See [live setup](docs/live-setup.md) for approval boundaries and [protocol references](docs/contracts.md) for source contracts.
