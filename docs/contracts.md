# Protocol references

T3 Code v0.0.45: commit `6c8fed35dded9ff71c5b46807125457acbb76be6`, orchestration protocol 1, Effect dependency `4.0.0-rc.115`.

- [HTTP contract](https://github.com/pingdotgg/t3code/blob/6c8fed35dded9ff71c5b46807125457acbb76be6/packages/contracts/src/environmentHttp.ts): shell, windowed thread detail, dispatch, environment descriptor and ticket route.
- [Orchestration contract](https://github.com/pingdotgg/t3code/blob/6c8fed35dded9ff71c5b46807125457acbb76be6/packages/contracts/src/orchestration.ts): command bodies, `{instanceId, model}`, stream variants, receipts and turn states.
- [Authorization client](https://github.com/pingdotgg/t3code/blob/6c8fed35dded9ff71c5b46807125457acbb76be6/packages/client-runtime/src/authorization/remote.ts): `wsTicket` and scoped token exchange.
- [Protocol negotiation](https://github.com/pingdotgg/t3code/blob/6c8fed35dded9ff71c5b46807125457acbb76be6/packages/client-runtime/src/connection/compatibility.ts): `orchestrationProtocol=1`.
- [RPC session](https://github.com/pingdotgg/t3code/blob/6c8fed35dded9ff71c5b46807125457acbb76be6/packages/client-runtime/src/rpc/session.ts): Effect JSON serialization.
- [Effect frame definitions](https://unpkg.com/effect@4.0.0-rc.115/src/unstable/rpc/RpcMessage.ts): Request, Chunk, Ack, Ping/Pong and Exit. Only shell subscription is implemented.
- [MCP Events](https://developers.openai.com/plugins/build/mcp-events): protocol 2026-07-28, persistent subscriptions, verified signed callbacks, replay/refresh and retry behavior.

No upstream code is vendored. Fixtures are synthetic contract examples. They verify routes and envelopes, not live platform access. The T3 socket uses Effect RPC; only the inbound MCP endpoint uses JSON-RPC.
