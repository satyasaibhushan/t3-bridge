import assert from "node:assert/strict";
import {
  chmodSync,
  existsSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  exchangeReadOnly,
  loadCredential,
  pairReadOnly,
  saveCredential,
  type Credential,
  type PairingTerminal,
} from "../src/pairing.js";
import { config } from "./helpers.js";
const environment = config.environments[0]!;
const bootstrap = "synthetic-bootstrap-fixture";
const accessToken = "synthetic-access-fixture";
const credential: Credential = {
  environmentId: environment.id,
  baseUrl: environment.baseUrl,
  scope: "orchestration:read",
  accessToken,
  expiresAt: Date.now() + 600000,
};
function fixture(result: Record<string, unknown> = {}) {
  const calls: Array<{ url: URL; init?: RequestInit }> = [];
  const request: typeof fetch = async (raw, init) => {
    const url = new URL(String(raw));
    calls.push({ url, init });
    return Response.json(
      url.pathname === "/.well-known/t3/environment"
        ? { environmentId: environment.id, orchestrationProtocolVersion: 1 }
        : {
            access_token: accessToken,
            issued_token_type: "urn:ietf:params:oauth:token-type:access_token",
            token_type: "Bearer",
            expires_in: 600,
            scope: "orchestration:read",
            ...result,
          },
    );
  };
  return { calls, request };
}
function terminal(answers: string[]) {
  const output: string[] = [],
    prompts: Array<{ prompt: string; secret: boolean }> = [];
  const io: PairingTerminal = {
    write: (text) => {
      output.push(text);
    },
    read: async (prompt, secret = false) => {
      prompts.push({ prompt, secret });
      return answers.shift() ?? "";
    },
  };
  return { io, output, prompts };
}
function directory() {
  return realpathSync(mkdtempSync(join(tmpdir(), "bridge-pairing-")));
}

test("pairing requests only read scope through pinned token exchange after identity verification", async () => {
  const f = fixture();
  const result = await exchangeReadOnly(environment, bootstrap, f.request);
  assert.equal(result.accessToken, accessToken);
  assert.equal(f.calls.length, 2);
  assert.equal(f.calls[0]?.url.pathname, "/.well-known/t3/environment");
  assert.equal(f.calls[0]?.init?.body, undefined);
  assert.equal(
    new Headers(f.calls[0]?.init?.headers).has("authorization"),
    false,
  );
  const exchange = f.calls[1]!;
  assert.equal(exchange.url.pathname, "/oauth/token");
  assert.equal(exchange.init?.redirect, "error");
  assert.equal(exchange.init?.method, "POST");
  assert.equal(
    new Headers(exchange.init?.headers).get("content-type"),
    "application/x-www-form-urlencoded",
  );
  assert.deepEqual(
    Object.fromEntries(new URLSearchParams(String(exchange.init?.body))),
    {
      grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
      subject_token: bootstrap,
      subject_token_type:
        "urn:t3:params:oauth:token-type:environment-bootstrap",
      requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
      scope: "orchestration:read",
      client_label: "t3-bridge",
    },
  );
});
test("pairing fails before credential transmission for remote, redirected or wrong identity targets", async () => {
  for (const baseUrl of [
    "https://example.com",
    "http://localhost:3773",
    "http://127.0.0.1:3773/path",
    "http://user@127.0.0.1:3773",
  ]) {
    const f = fixture();
    await assert.rejects(
      exchangeReadOnly({ ...environment, baseUrl }, bootstrap, f.request),
      /literal_loopback/,
    );
    assert.equal(f.calls.length, 0);
  }
  for (const descriptor of [
    { environmentId: "other", orchestrationProtocolVersion: 1 },
    { environmentId: "env", orchestrationProtocolVersion: 2 },
  ]) {
    const bodies: unknown[] = [];
    await assert.rejects(
      exchangeReadOnly(environment, bootstrap, async (_url, init) => {
        bodies.push(init?.body);
        return Response.json(descriptor);
      }),
    );
    assert.deepEqual(bodies, [undefined]);
  }
  let calls = 0;
  await assert.rejects(
    exchangeReadOnly(environment, bootstrap, async (_url, init) => {
      calls++;
      assert.equal(init?.redirect, "error");
      return new Response(null, {
        status: 302,
        headers: { location: "https://external.example" },
      });
    }),
    /descriptor_failed/,
  );
  assert.equal(calls, 1);
});
test("pairing rejects overbroad, proof-bound, expired and malformed responses without token-bearing errors", async () => {
  for (const result of [
    { scope: "orchestration:read orchestration:operate" },
    { token_type: "DPoP" },
    { expires_in: 0 },
    { access_token: "" },
    { issued_token_type: "unknown" },
  ]) {
    await assert.rejects(
      exchangeReadOnly(environment, bootstrap, fixture(result).request),
      (error: unknown) => {
        assert.ok(error instanceof Error);
        assert.match(error.message, /response_rejected/);
        assert.ok(!error.message.includes(accessToken));
        assert.ok(!error.message.includes(bootstrap));
        return true;
      },
    );
  }
});
test("interactive pairing requires explicit exchange consent, hidden entry, separate save consent and new private file", async () => {
  const dir = directory(),
    path = join(dir, "t3.credentials.json");
  const f = fixture(),
    t = terminal(["pair", bootstrap, "save"]);
  try {
    await pairReadOnly(environment, path, t.io, f.request);
    assert.equal(t.prompts[1]?.secret, true);
    assert.equal(t.prompts.length, 3);
    assert.equal(statSync(path).mode & 0o777, 0o600);
    assert.equal(
      loadCredential({ ...environment, credentialFile: path }),
      accessToken,
    );
    assert.ok(!t.output.join("").includes(accessToken));
    assert.ok(!t.output.join("").includes(bootstrap));
    assert.ok(
      !t.prompts
        .map((p) => p.prompt)
        .join("")
        .includes(bootstrap),
    );
    assert.throws(() => saveCredential(path, credential), /EEXIST/);
    assert.equal(
      JSON.parse(readFileSync(path, "utf8")).accessToken,
      accessToken,
    );
  } finally {
    rmSync(dir, { recursive: true });
  }
});
test("declining pairing or storage performs no unapproved write", async () => {
  const dir = directory(),
    path = join(dir, "new", "t3.credentials.json");
  try {
    const no = fixture();
    await assert.rejects(
      pairReadOnly(environment, path, terminal(["no"]).io, no.request),
      /cancelled/,
    );
    assert.equal(no.calls.length, 1);
    assert.equal(existsSync(path), false);
    const discard = terminal(["pair", bootstrap, "no"]);
    await pairReadOnly(environment, path, discard.io, fixture().request);
    assert.equal(existsSync(join(dir, "new")), false);
    assert.match(discard.output.join(""), /Revoke/);
  } finally {
    rmSync(dir, { recursive: true });
  }
});
test("credential files enforce permissions, target binding, expiry, no symlinks and no overwrite", () => {
  const dir = directory(),
    path = join(dir, "t3.credentials.json");
  try {
    saveCredential(path, credential);
    for (const changed of [
      { environmentId: "other" },
      { baseUrl: "http://127.0.0.1:9999" },
      { expiresAt: 1 },
      { scope: "orchestration:operate" },
    ]) {
      writeFileSync(path, JSON.stringify({ ...credential, ...changed }));
      assert.throws(
        () => loadCredential({ ...environment, credentialFile: path }),
        /invalid_or_expired/,
      );
    }
    writeFileSync(path, JSON.stringify(credential));
    chmodSync(path, 0o644);
    assert.throws(
      () => loadCredential({ ...environment, credentialFile: path }),
      /invalid_or_expired/,
    );
    chmodSync(path, 0o600);
    const link = join(dir, "link.credentials.json");
    symlinkSync(path, link);
    assert.throws(
      () => loadCredential({ ...environment, credentialFile: link }),
      /invalid_or_expired/,
    );
    assert.throws(() => saveCredential(link, credential));
    chmodSync(dir, 0o755);
    assert.throws(
      () => saveCredential(join(dir, "another.credentials.json"), credential),
      /owner_only/,
    );
    assert.throws(
      () => loadCredential({ ...environment, credentialFile: path }),
      /invalid_or_expired/,
    );
  } finally {
    rmSync(dir, { recursive: true });
  }
});
