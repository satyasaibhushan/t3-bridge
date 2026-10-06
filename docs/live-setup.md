# Live acceptance boundary

No live pairing, tunnel, service deployment, or agent task was created. The bridge is not yet connected to dot.

## Verified inventory

The connected Mac's installed T3 Code Alpha app reports 0.0.45. Its public loopback descriptor at port 3773 returned protocol 1. The normal settings UI independently displayed 0.0.45, Stable, Up to Date. Existing connection/provider state was untouched. The encrypted connection catalog was not decrypted; no hidden desktop/provider credentials were read.

The app menu opens Settings. During inspection, HTML Connections navigation produced no observed change; coordinate fallback returned `noWindowsAvailable`. A user-authorized page refresh produced no observed change. A working supported owner interaction remains necessary to create the scoped pairing.

## Read-only pairing

User approval covers a dedicated revocable `t3-bridge` pairing with **only `orchestration:read`**. No operate, terminal, review, relay, access-admin, tunnel, or dot credential grants are authorized by that approval.

The release CLI `auth pairing create` hardcodes standard scopes including operate/terminal/review/relay. Do not use it for this approval. The supported least-scope HTTP flow is:

1. Existing authorized owner context calls `POST /api/auth/pairing-token` with `{ "label": "t3-bridge", "scopes": ["orchestration:read"] }`.
2. Intentional form-encoded `POST /oauth/token` exchanges the bootstrap credential, explicitly requesting `scope=orchestration:read` with the documented token-exchange grant/types.
3. Verify returned scope is exactly the requested read scope; use only that dedicated bearer token. A proof-bound token cannot be used as a bearer token.

This requires supported owner access. Do not borrow internal credentials, scrape provider sessions, or create a broader pairing and treat a wrapper allowlist as equivalent. Hand off user credential entry/submission when required. Never paste credentials into chat or retain them as evidence.

After pairing, perform read-only discovery to identify the user's selected test project/thread. No project/thread identity has been invented or allowlisted. The example denies everything. Creating a missing disposable test project/thread requires approval for that exact mutation.

## Remaining acceptance

1. Complete approved scoped pairing and configure exact environment/project/thread IDs. Verify read tools expose only that scope.
2. Separately approve the authenticated HTTPS endpoint or private tunnel, dot plugin connection, inbound token and callback host.
3. In dot: discover tools/events, subscribe, verify challenge, observe one allowed event, confirm the dot received and processed it, then unsubscribe. Confirm an unallowed thread produces no callback. A callback 2xx alone is insufficient evidence.
4. For write testing, separately approve the operate scope, disposable thread and exact prompt/create/send/interrupt actions. Writes are disabled by default and require local command approvals. No real agent messages have been sent.
5. Task Finder follows dot acceptance and reuses `Bridge.execute` with stable task/run/command IDs. Do not add a second dispatcher, scheduler, task database or Backfill path.

Report “connected” only after live tools and callback acceptance. Fixture tests establish implementation behavior, not access or integration permissions.
