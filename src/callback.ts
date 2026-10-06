import { createHmac, randomUUID, timingSafeEqual } from "node:crypto";
import { lookup } from "node:dns/promises";
import { request } from "node:https";
import ipaddr from "ipaddr.js";
import { BridgeError } from "./schema.js";
export interface CallbackResponse {
  status: number;
  body: string;
}
export type CallbackTransport = (
  url: string,
  body: string,
  headers: Record<string, string>,
) => Promise<CallbackResponse>;
export function signingKey(secret: string): Buffer {
  if (!/^whsec_[A-Za-z0-9+/]+={0,2}$/.test(secret))
    throw new BridgeError("invalid_signing_secret");
  const key = Buffer.from(secret.slice(6), "base64");
  if (
    key.length < 24 ||
    key.length > 64 ||
    key.toString("base64").replace(/=+$/, "") !==
      secret.slice(6).replace(/=+$/, "")
  )
    throw new BridgeError("invalid_signing_secret");
  return key;
}
export function signature(
  secret: string,
  id: string,
  timestamp: string,
  body: string,
): string {
  return `v1,${createHmac("sha256", signingKey(secret)).update(`${id}.${timestamp}.${body}`).digest("base64")}`;
}
export function publicAddress(address: string): boolean {
  try {
    return ipaddr.process(address).range() === "unicast";
  } catch {
    return false;
  }
}
export function callbackUrl(raw: string, hosts: string[]): URL {
  const url = new URL(raw);
  if (
    url.protocol !== "https:" ||
    url.username ||
    url.password ||
    url.hash ||
    (url.port && url.port !== "443") ||
    !hosts.includes(url.hostname) ||
    ipaddr.isValid(url.hostname)
  )
    throw new BridgeError("callback_destination_denied", -32015);
  return url;
}
export function httpsTransport(hosts: string[]): CallbackTransport {
  return async (raw, body, headers) => {
    const url = callbackUrl(raw, hosts);
    const addresses = await lookup(url.hostname, { all: true, verbatim: true });
    if (!addresses.length || addresses.some((a) => !publicAddress(a.address)))
      throw new BridgeError("callback_destination_denied", -32015);
    const address = addresses[0];
    if (!address) throw new BridgeError("callback_destination_denied", -32015);
    // Pin the validated address for this connection. Preserve hostname/SNI and
    // certificate validation; never follow a redirect or reuse an unchecked socket.
    return new Promise((resolve, reject) => {
      const req = request(
        url,
        {
          method: "POST",
          agent: false,
          servername: url.hostname,
          headers: {
            ...headers,
            "content-length": String(Buffer.byteLength(body)),
          },
          lookup: (_host, options, cb) =>
            options.all
              ? cb(null, [address])
              : cb(null, address.address, address.family),
        },
        (res) => {
          const chunks: Buffer[] = [];
          let bytes = 0;
          res.on("data", (chunk: Buffer) => {
            bytes += chunk.length;
            if (bytes > 65536) {
              res.destroy();
              reject(new BridgeError("callback_response_too_large", -32015));
            } else chunks.push(chunk);
          });
          res.on("error", () =>
            reject(new BridgeError("callback_failed", -32015)),
          );
          res.on("end", () =>
            resolve({
              status: res.statusCode ?? 0,
              body: Buffer.concat(chunks).toString("utf8"),
            }),
          );
        },
      );
      const timeout = setTimeout(
        () => req.destroy(new Error("timeout")),
        10000,
      );
      req.on("close", () => clearTimeout(timeout));
      req.on("error", () =>
        reject(new BridgeError("callback_timeout_or_network", -32015)),
      );
      req.end(body);
    });
  };
}
export async function signedPost(
  transport: CallbackTransport,
  sub: {
    id: string;
    url: string;
    secret: string;
    oldSecret?: string;
    rotateUntil?: number;
  },
  payload: unknown,
  eventId: string,
): Promise<CallbackResponse> {
  const body = JSON.stringify(payload);
  if (Buffer.byteLength(body) > 262144)
    throw new BridgeError("event_too_large");
  const timestamp = String(Math.floor(Date.now() / 1000));
  let sig = signature(sub.secret, eventId, timestamp, body);
  if (sub.oldSecret && (sub.rotateUntil ?? 0) > Date.now())
    sig += ` ${signature(sub.oldSecret, eventId, timestamp, body)}`;
  return transport(sub.url, body, {
    "content-type": "application/json",
    "webhook-id": eventId,
    "webhook-timestamp": timestamp,
    "webhook-signature": sig,
    "X-MCP-Subscription-Id": sub.id,
  });
}
export async function verifyCallback(
  transport: CallbackTransport,
  sub: { id: string; url: string; secret: string },
): Promise<void> {
  const challenge = randomUUID();
  const response = await signedPost(
    transport,
    sub,
    { type: "verification", challenge },
    `verify_${randomUUID()}`,
  );
  let echoed: unknown;
  try {
    echoed = (JSON.parse(response.body) as Record<string, unknown>).challenge;
  } catch {
    throw new BridgeError("challenge_failed", -32015);
  }
  if (
    response.status < 200 ||
    response.status >= 300 ||
    typeof echoed !== "string" ||
    Buffer.byteLength(echoed) !== Buffer.byteLength(challenge) ||
    !timingSafeEqual(Buffer.from(challenge), Buffer.from(echoed))
  )
    throw new BridgeError("challenge_failed", -32015);
}
