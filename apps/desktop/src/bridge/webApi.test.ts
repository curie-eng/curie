import { afterEach, describe, expect, it, vi } from "vitest";

import { needsSignIn, webConnection, webRequest } from "./webApi";

const realFetch = globalThis.fetch;
afterEach(() => { globalThis.fetch = realFetch; vi.restoreAllMocks(); });

const answer = (status: number, body: string, ok = status < 400) =>
  vi.fn().mockResolvedValue({ status, ok, text: async () => body });

describe("reaching the API from a browser tab", () => {
  it("goes same-origin through /api, so nothing needs the API's address", async () => {
    const f = answer(200, "[]");
    globalThis.fetch = f as never;
    await webRequest({ method: "GET", path: "/agents" });
    expect(f.mock.calls[0][0]).toBe("/api/agents");
  });

  it("sends cookies and never a key", async () => {
    // ADR-0083: the browser never receives the platform key on any path.
    const f = answer(200, "{}");
    globalThis.fetch = f as never;
    await webRequest({ method: "GET", path: "/config" });
    const init = f.mock.calls[0][1] as { credentials?: string; headers?: unknown };
    expect(init.credentials).toBe("include");
    expect(JSON.stringify(init.headers ?? {})).not.toMatch(/api-key/i);
  });

  it("carries query parameters and drops the undefined ones", async () => {
    const f = answer(200, "[]");
    globalThis.fetch = f as never;
    await webRequest({ method: "GET", path: "/runs", query: { limit: 10, agent: undefined } });
    expect(f.mock.calls[0][0]).toBe("/api/runs?limit=10");
  });

  it("survives an answer that is not JSON", async () => {
    // A proxy error page is HTML. Parsing it as JSON would throw where a status
    // is the useful information.
    globalThis.fetch = answer(502, "<html>bad gateway</html>", false) as never;
    const res = await webRequest({ method: "GET", path: "/agents" });
    expect(res.status).toBe(502);
    expect(res.ok).toBe(false);
  });

  it("reports a network failure as status 0, not as an API error", async () => {
    globalThis.fetch = vi.fn().mockRejectedValue(new Error("connection refused")) as never;
    const res = await webRequest({ method: "GET", path: "/agents" });
    expect(res.status).toBe(0);
    expect(res.ok).toBe(false);
  });

  it("separates 'no API here' from 'not signed in'", async () => {
    expect(needsSignIn({ status: 401 })).toBe(true);
    expect(needsSignIn({ status: 403 })).toBe(true);
    expect(needsSignIn({ status: 0 })).toBe(false);
    expect(needsSignIn({ status: 200 })).toBe(false);
  });

  it("probes an endpoint that needs authorization, not one that does not", async () => {
    // `/config` answers unauthenticated, so probing it showed "Connected" over
    // an empty screen while every real call was refused.
    const f = answer(200, "[]");
    globalThis.fetch = f as never;
    await webConnection();
    expect(f.mock.calls[0][0]).toBe("/api/agents");
  });

  it("calls itself reachable-but-unauthorised on a 401", async () => {
    globalThis.fetch = answer(401, '{"detail":"Not authenticated"}', false) as never;
    const c = await webConnection();
    expect(c.reachable).toBe(true);
    expect(c.hasKey).toBe(false);
  });

  it("calls itself unreachable when the request never landed", async () => {
    globalThis.fetch = vi.fn().mockRejectedValue(new Error("nope")) as never;
    const c = await webConnection();
    expect(c.reachable).toBe(false);
  });
});
