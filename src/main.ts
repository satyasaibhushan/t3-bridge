import { readFileSync } from "node:fs";
import { loadCredential, localTerminal, pairEnvironment } from "./pairing.js";
import { Bridge } from "./bridge.js";
import { httpsTransport } from "./callback.js";
import { Events } from "./events.js";
import { BridgeError, configSchema } from "./schema.js";
import { Mcp, httpServer } from "./server.js";
import { Store } from "./store.js";
import { ShellStream, T3Client } from "./t3.js";

async function main() {
  const [action, configPath, commandPath, credentialPath] =
    process.argv.slice(2);
  if (
    !configPath ||
    !["serve", "approve", "check", "pair-readonly", "pair-control"].includes(
      action ?? "",
    )
  )
    throw new BridgeError(
      "usage: serve|approve|check CONFIG [COMMAND_FILE]; pair-readonly|pair-control CONFIG ENVIRONMENT_ID CREDENTIAL_FILE",
    );
  const config = configSchema.parse(
    JSON.parse(readFileSync(configPath, "utf8")),
  );
  if (action === "check") {
    console.log("Configuration valid; no connection attempted");
    return;
  }
  if (action === "pair-readonly" || action === "pair-control") {
    if (!process.stdin.isTTY || !process.stdout.isTTY)
      throw new BridgeError("pairing_requires_interactive_terminal");
    const environment = config.environments.find((e) => e.id === commandPath);
    if (!environment || !credentialPath)
      throw new BridgeError("pairing_target_and_destination_required");
    await pairEnvironment(
      environment,
      credentialPath,
      localTerminal,
      fetch,
      action === "pair-control" ? "control" : "readonly",
    );
    return;
  }
  const store = new Store(config.statePath);
  const clients = new Map(
    config.environments.map((e) => [
      e.id,
      new T3Client(e, () => loadCredential(e)),
    ]),
  );
  const bridge = new Bridge(config, store, clients);
  if (action === "approve") {
    if (!commandPath) throw new BridgeError("command_file_required");
    console.log(
      JSON.stringify(
        bridge.approve(JSON.parse(readFileSync(commandPath, "utf8"))),
      ),
    );
    store.close();
    return;
  }
  for (const e of config.environments)
    if (!loadCredential(e)) throw new BridgeError("pairing_required");
  const events = new Events(bridge, httpsTransport(config.callbackHosts));
  bridge.onEvent = () => {
    void events.flush().catch(() => console.error("callback_delivery_failed"));
  };
  const streams = [...clients].map(
    ([env, client]) =>
      [
        env,
        new ShellStream(
          client,
          () => store.get<number>("cursors", env),
          (item) => bridge.observe(env, item),
        ),
      ] as const,
  );
  const server = httpServer(
    new Mcp(bridge, events, () =>
      Object.fromEntries(streams.map(([id, s]) => [id, s.health])),
    ),
    process.env[config.clientTokenEnv] ?? "",
  );
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(config.port, "127.0.0.1", resolve);
  });
  console.log(`Bridge listening on loopback port ${config.port}`);
  const abort = new AbortController();
  const retry = setInterval(() => {
    void events.flush().catch(() => console.error("callback_delivery_failed"));
  }, 1000);
  const jobs = streams.map(([, s]) => s.run(abort.signal));
  let stopping = false;
  const stop = async () => {
    if (stopping) return;
    stopping = true;
    abort.abort();
    clearInterval(retry);
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await Promise.all(jobs);
    await events.flush();
    store.close();
  };
  const shutdown = () => {
    void stop().catch(() => {
      console.error("shutdown_failed");
      process.exitCode = 1;
    });
  };
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
}
main().catch((error) => {
  console.error(error instanceof BridgeError ? error.code : "startup_failed");
  process.exitCode = 1;
});
