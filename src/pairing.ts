import {
  closeSync,
  constants,
  fstatSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  realpathSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { dirname, isAbsolute, resolve } from "node:path";
import { z } from "zod";
import { BridgeError, type Environment } from "./schema.js";
import { T3Client } from "./t3.js";

const scope = "orchestration:read";
const accessType = "urn:ietf:params:oauth:token-type:access_token";
const tokenSchema = z
  .string()
  .min(1)
  .max(16384)
  .regex(/^[\x21-\x7e]+$/);
const credentialSchema = z
  .object({
    environmentId: z.string(),
    baseUrl: z.string(),
    scope: z.literal(scope),
    accessToken: tokenSchema,
    expiresAt: z.number().finite().positive(),
  })
  .strict();
export type Credential = z.infer<typeof credentialSchema>;
export interface PairingTerminal {
  write(text: string): void;
  read(prompt: string, secret?: boolean): Promise<string>;
}
function localTarget(environment: Environment) {
  const url = new URL(environment.baseUrl);
  if (
    url.protocol !== "http:" ||
    !["127.0.0.1", "[::1]"].includes(url.hostname) ||
    url.username ||
    url.password ||
    url.pathname !== "/" ||
    url.search ||
    url.hash
  )
    throw new BridgeError("pairing_requires_literal_loopback");
}
export async function exchangeReadOnly(
  environment: Environment,
  bootstrap: string,
  request: typeof fetch = fetch,
): Promise<Credential> {
  localTarget(environment);
  // This is the supported intentional bootstrap exchange, never desktop auth.
  if (!tokenSchema.safeParse(bootstrap).success || bootstrap.includes("://"))
    throw new BridgeError("invalid_bootstrap_credential");
  await new T3Client(environment, () => "", request).verifyIdentity();
  let response: Response;
  try {
    response = await request(new URL("/oauth/token", environment.baseUrl), {
      method: "POST",
      redirect: "error",
      signal: AbortSignal.timeout(15000),
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
        subject_token: bootstrap,
        subject_token_type:
          "urn:t3:params:oauth:token-type:environment-bootstrap",
        requested_token_type: accessType,
        scope,
        client_label: "t3-bridge",
      }).toString(),
    });
  } catch {
    throw new BridgeError(
      "pairing_exchange_uncertain_check_authorized_clients",
    );
  }
  if (!response.ok) throw new BridgeError("pairing_exchange_rejected");
  // Never expose server error bodies or validation details that could contain tokens.
  try {
    const result = z
      .object({
        access_token: tokenSchema,
        issued_token_type: z.literal(accessType),
        token_type: z.literal("Bearer"),
        expires_in: z.number().finite().positive(),
        scope: z.literal(scope),
      })
      .parse(await response.json());
    return credentialSchema.parse({
      environmentId: environment.id,
      baseUrl: environment.baseUrl,
      accessToken: result.access_token,
      scope,
      expiresAt: Date.now() + result.expires_in * 1000,
    });
  } catch {
    throw new BridgeError("pairing_response_rejected_check_authorized_clients");
  }
}
function privateDirectory(path: string) {
  if (
    !isAbsolute(path) ||
    realpathSync(dirname(path)) !== resolve(dirname(path))
  )
    throw new BridgeError("credential_path_must_be_absolute_without_symlinks");
  const parent = statSync(dirname(path));
  if (
    !parent.isDirectory() ||
    parent.uid !== process.getuid?.() ||
    (parent.mode & 0o777) !== 0o700
  )
    throw new BridgeError("credential_directory_must_be_owner_only");
}
export function saveCredential(path: string, credential: Credential) {
  if (!isAbsolute(path))
    throw new BridgeError("credential_path_must_be_absolute_without_symlinks");
  credentialSchema.parse(credential);
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  privateDirectory(path);
  const fd = openSync(
    path,
    constants.O_WRONLY |
      constants.O_CREAT |
      constants.O_EXCL |
      constants.O_NOFOLLOW,
    0o600,
  );
  try {
    writeFileSync(fd, JSON.stringify(credential));
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
}
export function loadCredential(environment: Environment): string {
  const path = environment.credentialFile;
  if (!path) return process.env[environment.tokenEnv] ?? "";
  try {
    privateDirectory(path);
    const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW);
    try {
      const info = fstatSync(fd);
      if (
        !info.isFile() ||
        info.uid !== process.getuid?.() ||
        (info.mode & 0o777) !== 0o600 ||
        info.size > 65536
      )
        throw new Error("invalid_permissions");
      const credential = credentialSchema.parse(
        JSON.parse(readFileSync(fd, "utf8")),
      );
      if (
        credential.environmentId !== environment.id ||
        credential.baseUrl !== environment.baseUrl ||
        credential.expiresAt <= Date.now()
      )
        throw new Error("wrong_target_or_expired");
      return credential.accessToken;
    } finally {
      closeSync(fd);
    }
  } catch {
    throw new BridgeError("credential_file_invalid_or_expired");
  }
}
export async function pairReadOnly(
  environment: Environment,
  destination: string,
  terminal: PairingTerminal,
  request: typeof fetch = fetch,
) {
  localTarget(environment);
  if (!isAbsolute(destination))
    throw new BridgeError("credential_path_must_be_absolute_without_symlinks");
  await new T3Client(environment, () => "", request).verifyIdentity();
  terminal.write(
    `Verified ${environment.id} at ${environment.baseUrl}. This exchanges a user-created Read only pairing for t3-bridge, requesting only ${scope}. No agent task will run.\n`,
  );
  if (
    (await terminal.read(
      "Type pair to approve this exchange (anything else cancels): ",
    )) !== "pair"
  )
    throw new BridgeError("pairing_cancelled");
  let bootstrap = await terminal.read(
    "Paste the raw bootstrap credential, then press Enter to submit (hidden): ",
    true,
  );
  let credential: Credential;
  try {
    credential = await exchangeReadOnly(environment, bootstrap, request);
  } finally {
    bootstrap = "";
  }
  terminal.write(
    `Read-only bearer exchange succeeded. Save the credential to ${destination} in a private directory (0700), new file (0600). It is sensitive plaintext; existing files will not be overwritten.\n`,
  );
  if (
    (await terminal.read(
      "Type save to consent to local storage (anything else discards it): ",
    )) !== "save"
  ) {
    terminal.write(
      "Credential discarded from this process. Revoke the t3-bridge client in T3 Authorized clients if it is no longer needed.\n",
    );
    return;
  }
  try {
    saveCredential(destination, credential);
  } catch {
    throw new BridgeError("credential_save_failed_check_authorized_clients");
  }
  terminal.write(
    "Credential saved. Set credentialFile for this environment to that path; configuration was not changed. No bridge service was started.\n",
  );
}

// Reads only the controlling interactive terminal: no CLI arguments, env, pipe,
// shell history, echo, clipboard reads, or log output for bootstrap credentials.
export const localTerminal: PairingTerminal = {
  write: (text) => {
    process.stdout.write(text);
  },
  read: (prompt, secret = false) =>
    new Promise((resolve, reject) => {
      const input = process.stdin;
      if (!input.isTTY || !process.stdout.isTTY) {
        reject(new BridgeError("pairing_requires_interactive_terminal"));
        return;
      }
      let value = "";
      const wasRaw = input.isRaw,
        wasFlowing = input.readableFlowing === true;
      const finish = (error?: BridgeError) => {
        input.off("data", onData);
        input.off("end", onEnd);
        input.off("error", onEnd);
        process.off("SIGTERM", onEnd);
        process.off("SIGHUP", onEnd);
        input.setRawMode(wasRaw);
        if (!wasFlowing) input.pause();
        process.stdout.write("\n");
        if (error) reject(error);
        else resolve(value);
        value = "";
      };
      const onEnd = () => finish(new BridgeError("pairing_cancelled"));
      const onData = (data: Buffer) => {
        for (const char of data.toString("utf8")) {
          if (char === "\r" || char === "\n") {
            finish();
            return;
          }
          if (char === "\u0003" || char === "\u0004") {
            onEnd();
            return;
          }
          if (char === "\u007f" || char === "\b") {
            if (value.length) {
              value = value.slice(0, -1);
              if (!secret) process.stdout.write("\b \b");
            }
            continue;
          }
          if (char < " " || char > "~" || value.length >= 16384) {
            finish(new BridgeError("invalid_terminal_input"));
            return;
          }
          value += char;
          if (!secret) process.stdout.write(char);
        }
      };
      input.setRawMode(true);
      input.on("data", onData);
      input.once("end", onEnd);
      input.once("error", onEnd);
      process.once("SIGTERM", onEnd);
      process.once("SIGHUP", onEnd);
      process.stdout.write(prompt);
      input.resume();
    }),
};
