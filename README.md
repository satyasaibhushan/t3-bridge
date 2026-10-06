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

The example denies all resources and disables writes. When authorized, copy it to `config.local.json`, configure exact discovered IDs, and supply dedicated credentials using the named environment variables or an explicitly configured private `credentialFile` produced by the local helper. The inbound client token requires at least 32 characters. Never put tokens in config, command files, Git, logs, or chat.

```sh
node dist/main.js serve config.local.json
```

After creating an approved pairing in T3, the user runs the matching helper in an interactive terminal. The read-only option remains available:

```sh
node dist/main.js pair-readonly config.local.json VERIFIED_ENVIRONMENT_ID /absolute/private/directory/t3.credentials.json
```

For the separately approved read-and-control scope, start with T3's **Read only** preset, then check **Operate tasks**. Only **View environment** (`orchestration:read`) and **Operate tasks** (`orchestration:operate`) should remain checked. The **Standard** preset includes extra permissions and is unsuitable. Copy the new row's raw code using **Share → Copy code only** (or **Copy code** when shown), then run:

```sh
node dist/main.js pair-control config.local.json VERIFIED_ENVIRONMENT_ID /absolute/private/directory/t3.credentials.json
```

The helper verifies the configured literal loopback endpoint and environment, asks for the exact exchange confirmation (`pair` for read-only, `pair-control` for read plus operate), and reads the raw bootstrap credential with terminal echo disabled. The selected command requests exactly its named scope set. It rejects missing, extra, duplicate or proof-bound grants; it never upgrades scope automatically. A separate `save` confirmation permits a new 0600 credential file in an owner-only 0700 directory; it never overwrites a file or edits configuration. Set that environment's `credentialFile` to the absolute path after saving. With this option, the file takes precedence over `tokenEnv`; target binding, expiry and permissions are checked on every use. Keep it outside the repository. The file is plaintext and requires private encrypted host storage. No raw credential is printed or accepted as a CLI argument. Neither pairing mode enables bridge writes or starts a service. Do not run this helper through an agent: credential entry and submission belong to the user. See [live setup](docs/live-setup.md) for the exact owner controls and approval scope.

If exchange fails after submission or saving is declined/fails, inspect T3 Authorized clients and revoke the dedicated pairing if it is unused. The helper never silently retries an uncertain exchange or performs revocation.

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

The matching tool takes these arguments without `operation`. Create uses `title` instead of `text`; interrupt uses an explicit `turnId`. Approval binds principal, target URL, model, operation, IDs and payload. Create sets `approval-required` runtime and `default` interaction mode, with the configured model and no worktree/bootstrap scripts. Before every send attempt, including uncertain retries, the bridge verifies the thread has those modes and exactly the approved provider/model/options, then explicitly selects that model. New sends reject an active turn. Missing or mismatched policy fails closed.

This is preflight validation, not an atomic permission ceiling: stable T3 uses the thread's current modes at dispatch and has no expected-policy precondition. A concurrent native edit can change them after validation. Keep writes disabled for the read-first pilot. A later approved write test should use a fresh disposable bridge-created thread with no concurrent native edits.

Receipts persist before dispatch. T3 receives deterministic namespaced command and message IDs. A lost response leaves a pending receipt: retry with the identical command ID and arguments to resend the exact timestamp/body. Changed content is rejected; accepted results return without another dispatch. Pending writes are not automatically executed on reconnect. Keep the state file to retain these guarantees.

## Events and recovery

The endpoint implements only MCP `2026-07-28`, with per-request `_meta`, mirrored HTTP headers, complete result envelopes and `server/discover`; legacy `initialize` receives an unsupported-version response. The official MCP client v2.3.1 is exercised over loopback in tests. It implements `events/list`, `events/subscribe`, and `events/unsubscribe`. Event `thread.changed` supports environment/project and optional thread filters. Payloads contain status/reason, T3 turn ID, proven task/run correlation and `recovered`, with no prompt/transcript text. Read tools retrieve details.

The initial T3 shell snapshot establishes a baseline. Status, turn, and plan changes emit progress, completion, failure, input-required or interrupted notices. Replay cursor and event persistence is atomic. Duplicate sequences are ignored. Reconnect resumes with `afterSequence`. A fallback snapshot reports changed current state with `recovered: true`, but cannot reconstruct intermediate history T3 no longer retains. A regressed server cursor stops processing and requires a reviewed state reset.

Run correlation keeps a separate durable mapping for each command and proven turn, joining the dispatched message ID to its turn ID in thread detail. Later sends cannot overwrite earlier turn correlation; delayed reads use compare-and-set before saving. Observations inspect the latest 100 turns; older unseen messages remain unproven until their detail is available. Unproven correlation stays null. Unrelated native turns are not assigned to an earlier bridge run.

Subscriptions persist across restarts. Before application delivery, callbacks must echo a fresh signed challenge. Delivery uses Standard Webhooks HMAC signatures over exact request bytes, stable event IDs and fresh signing timestamps. Secrets must decode to 24–64 bytes. Rotation overlaps for five minutes. Callback destinations must be explicitly allowlisted HTTPS hosts, resolved and checked for public addresses on every connection, then pinned to the validated address with normal TLS verification. Redirects are forbidden.

Delivery is ordered per subscription and at least once. Transient failures retry exponentially up to eight attempts; 410/413 stop immediately. Exhaustion is visible in `t3_status`; refresh resumes the unacknowledged cursor. Expiration is at most 24 hours. Unsubscribe is idempotent and invalidates in-flight verification/refresh attempts through durable generations. Up to 10,000 events are retained; expired replay history returns `truncated: true`. Consumers deduplicate by `eventId`.

The owner-only (0600) SQLite file contains receipts, correlation, compact thread status, cursors, events, approvals and subscriptions, not a transcript/task database. Pending command bodies and callback secrets are sensitive local state, not encrypted by this package. Use the host's encrypted private storage and one service process per state file.

See [live setup](docs/live-setup.md) for approval boundaries and [protocol references](docs/contracts.md) for source contracts.
