import { afterEach, describe, expect, it, vi } from "vitest";
import { bundleFileTree, buildBundleZip, bundleTreeFromFiles, nextVersionLabel } from "./bundle";
import {
  createAgent,
  updateAgent,
  // ADR-0118 (S5.2): moves a single channel binding via the /channels
  // subresource PATCH, naming the pair to move in the query string.
  patchAgentChannel,
  addAgentSurface,
  removeAgentSurface,
  uploadBundle,
  BundleValidationError,
  ApiError,
  getVersionFiles,
  listVersions,
  listDeployments,
  createDeployment,
  listTraces,
  listRunnerPods,
  getConfig,
  getAgents,
  listStateNamespaces,
  resolveApproval,
  getConsoleSession,
  exchangeConsoleLoginCode,
  onUnauthorized,
} from "./client";
import * as client from "./client";
import * as config from "./config";

// Unsubscribers registered by a test; drained after each so no listener leaks
// into the next test through the client module's shared listener set.
const unsubscribers: Array<() => void> = [];

afterEach(() => {
  vi.unstubAllGlobals();
  while (unsubscribers.length) unsubscribers.pop()?.();
  window.history.replaceState(null, "", "/");
});

// Every header name on a fetch init, whatever shape the client used.
function headerEntries(init: RequestInit | undefined): Array<[string, string]> {
  const h = init?.headers;
  if (!h) return [];
  if (h instanceof Headers) return Array.from(h.entries());
  if (Array.isArray(h)) return h.map(([k, v]) => [k, v]);
  return Object.entries(h as Record<string, string>);
}

function hasApiKeyHeader(init: RequestInit | undefined): boolean {
  return headerEntries(init).some(([name]) => /^x-api-key$/i.test(name));
}

describe("bundleFileTree", () => {
  it("lays out the canonical plugin bundle tree with a name-bearing manifest", () => {
    const tree = bundleFileTree({
      agentName: "deal-desk",
      versionLabel: "v0.1.0",
      skillMd: "---\nname: deal-desk\ndescription: Approves deals\n---\n# body",
    });
    expect(Object.keys(tree)).toContain(".claude-plugin/plugin.json");
    expect(Object.keys(tree)).toContain("skills/deal-desk/SKILL.md");
    const manifest = JSON.parse(tree[".claude-plugin/plugin.json"]);
    expect(manifest.name).toBe("deal-desk");
    expect(manifest.version).toBe("v0.1.0");
    expect(manifest.description).toBe("Approves deals");
    expect(tree["skills/deal-desk/SKILL.md"]).toContain("# body");
  });

  it("produces a real zip Blob", async () => {
    const blob = await buildBundleZip({ agentName: "x", versionLabel: "v0", skillMd: "ok" });
    expect(blob.size).toBeGreaterThan(0);
  });
});

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("api client", () => {
  it("sends no platform key and returns the parsed agent", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse(201, {
        id: "a1",
        name: "deal-desk",
        channel: { kind: "slack", address: "#revenue-ops" },
        created_at: "now",
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const agent = await createAgent({ name: "deal-desk", channel: { kind: "slack", address: "#revenue-ops" } });
    expect(agent.id).toBe("a1");
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("/api/agents");
    expect(init.credentials).toBe("same-origin");
    expect(hasApiKeyHeader(init)).toBe(false);
  });

  it("surfaces validator issues from a 422 as BundleValidationError", async () => {
    const body = {
      detail: {
        detail: "bundle failed validation",
        errors: [
          { code: "skill.frontmatter_invalid", message: "description is required", location: "skills/x/SKILL.md" },
        ],
      },
    };
    vi.stubGlobal("fetch", vi.fn().mockImplementation(() => Promise.resolve(jsonResponse(422, body))));
    const archive = await buildBundleZip({ agentName: "x", versionLabel: "v0", skillMd: "bad" });
    const err = await uploadBundle("a1", "v1", archive).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(BundleValidationError);
    expect((err as BundleValidationError).issues[0].code).toBe("skill.frontmatter_invalid");
  });

  it("throws ApiError for a non-validation failure", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse(409, { detail: "already stored" })));
    const archive = await buildBundleZip({ agentName: "x", versionLabel: "v0", skillMd: "ok" });
    await expect(uploadBundle("a1", "v1", archive)).rejects.toBeInstanceOf(ApiError);
  });

  it("passes agent_id to the traces list only when given", async () => {
    // A fresh Response per call: a Response body can only be read once.
    const fetchMock = vi.fn().mockImplementation(() => Promise.resolve(jsonResponse(200, [])));
    vi.stubGlobal("fetch", fetchMock);
    await listTraces(20);
    expect(fetchMock.mock.calls[0][0]).toBe("/api/langfuse/traces?limit=20");
    await listTraces(5, "agent-uuid-1");
    expect(fetchMock.mock.calls[1][0]).toBe("/api/langfuse/traces?limit=5&agent_id=agent-uuid-1");
  });

  it("reads the open /config endpoint and returns the parsed org name", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(200, { org_name: "Globex Corporation" }));
    vi.stubGlobal("fetch", fetchMock);
    const config = await getConfig();
    expect(fetchMock.mock.calls[0][0]).toBe("/api/config");
    expect(config.org_name).toBe("Globex Corporation");
  });

  it("lists runner pods and surfaces a 503 no-cluster as ApiError(status=503)", async () => {
    const ok = vi.fn().mockResolvedValue(jsonResponse(200, { namespace: "curie", pods: ["runner-a", "runner-b"] }));
    vi.stubGlobal("fetch", ok);
    const pods = await listRunnerPods();
    expect(ok.mock.calls[0][0]).toBe("/api/observability/runners");
    expect(pods.pods).toEqual(["runner-a", "runner-b"]);

    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(jsonResponse(503, { detail: "no kubernetes cluster configured for runner pods" })),
    );
    const err = (await listRunnerPods("preview-pr-1").catch((e: unknown) => e)) as ApiError;
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(503);
  });
});

describe("nextVersionLabel (redeploy)", () => {
  it("bumps the patch of the highest vX.Y.Z", () => {
    expect(nextVersionLabel(["v0.1.0"])).toBe("v0.1.1");
    expect(nextVersionLabel(["v0.1.0", "v0.1.4", "v0.1.2"])).toBe("v0.1.5");
    expect(nextVersionLabel(["v1.2.9", "v0.9.9"])).toBe("v1.2.10");
  });

  it("falls back to a v0.1.<count> label when none parse", () => {
    expect(nextVersionLabel(["nightly", "hotfix"])).toBe("v0.1.2");
    expect(nextVersionLabel([])).toBe("v0.1.0");
  });

  it("never collides with an existing label", () => {
    // v0.1.0 would bump to v0.1.1, but that is taken -> suffix -rN.
    const out = nextVersionLabel(["v0.1.0", "v0.1.1"]);
    expect(out).toBe("v0.1.2");
    const suffixed = nextVersionLabel(["v0.1.0", "v0.1.1", "v0.1.2"]);
    expect(suffixed).toBe("v0.1.3");
  });
});

describe("bundleTreeFromFiles (redeploy re-pack)", () => {
  it("preserves every file and keeps an existing manifest untouched", () => {
    const manifest = JSON.stringify({ name: "deal-desk", description: "keep me" });
    const tree = bundleTreeFromFiles("deal-desk", [
      { path: ".claude-plugin/plugin.json", content: manifest },
      { path: "skills/deal-desk/SKILL.md", content: "edited body" },
      { path: "policy.yaml", content: "approver: J. Whitfield" },
    ]);
    // nothing dropped, and the manifest we passed is not overwritten
    expect(Object.keys(tree).sort()).toEqual(
      [".claude-plugin/plugin.json", "policy.yaml", "skills/deal-desk/SKILL.md"].sort(),
    );
    expect(tree[".claude-plugin/plugin.json"]).toBe(manifest);
    expect(tree["skills/deal-desk/SKILL.md"]).toBe("edited body");
  });

  it("synthesizes a manifest from the first skill when the bundle lacks one", () => {
    const tree = bundleTreeFromFiles("deal-desk", [
      { path: "skills/deal-desk/SKILL.md", content: "---\nname: deal-desk\ndescription: Approves deals\n---\n# body" },
    ]);
    expect(Object.keys(tree)).toContain(".claude-plugin/plugin.json");
    const manifest = JSON.parse(tree[".claude-plugin/plugin.json"]);
    expect(manifest.name).toBe("deal-desk");
    expect(manifest.description).toBe("Approves deals");
  });
});

describe("agent-detail client calls", () => {
  it("lists versions and deployments at the right URLs", async () => {
    const fetchMock = vi.fn().mockImplementation(() => Promise.resolve(jsonResponse(200, [])));
    vi.stubGlobal("fetch", fetchMock);
    await listVersions("a1");
    await listDeployments("a1");
    expect(fetchMock.mock.calls[0][0]).toBe("/api/agents/a1/versions");
    expect(fetchMock.mock.calls[1][0]).toBe("/api/deployments?agent_id=a1");
  });

  it("reads bundle files and surfaces a 404 as ApiError(status=404)", async () => {
    const ok = vi.fn().mockResolvedValue(
      jsonResponse(200, { files: [{ path: "skills/x/SKILL.md", content: "body" }] }),
    );
    vi.stubGlobal("fetch", ok);
    const files = await getVersionFiles("a1", "v1");
    expect(ok.mock.calls[0][0]).toBe("/api/agents/a1/versions/v1/files");
    expect(files.files[0].path).toBe("skills/x/SKILL.md");

    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse(404, { detail: "no bundle stored for this version" })));
    const err = (await getVersionFiles("a1", "v9").catch((e: unknown) => e)) as ApiError;
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(404);
  });

  // Channel binding moves now go through patchAgentChannel's subresource PATCH
  // (below), not updateAgent -- ADR-0118 dropped `channel` from updateAgent's
  // patch type entirely (the API 422s it). This exercises updateAgent's
  // remaining mutable field, `model`, so the generic PATCH request shape
  // (URL, method, body, headers) stays covered at the client-test layer.
  it("PATCHes the agent's model and returns the updated agent", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse(200, {
        id: "a1",
        name: "deal-desk",
        channels: [{ kind: "slack", address: "C0123ABCD" }],
        model: "glm-5.2",
        created_at: "now",
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const agent = await updateAgent("a1", { model: "glm-5.2" });
    expect(agent.model).toBe("glm-5.2");
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("/api/agents/a1");
    expect(init.method).toBe("PATCH");
    expect(JSON.parse(init.body)).toEqual({ model: "glm-5.2" });
    expect(init.credentials).toBe("same-origin");
    expect(hasApiKeyHeader(init)).toBe(false);
  });

  // ADR-0118: an agent binds one-or-more channels, so AgentOut carries
  // `channels: ChannelBinding[]`, ordered `(kind, address)` -- never a bare
  // `channel` object.
  it("agent_out_carries_a_channels_array", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse(200, [
        {
          id: "a1",
          name: "deal-desk",
          channels: [{ kind: "slack", address: "C0EXAMPLE1" }],
          model: null,
          created_at: "now",
        },
      ]),
    );
    vi.stubGlobal("fetch", fetchMock);
    const agents = await getAgents();
    expect(Array.isArray((agents[0] as unknown as { channels: unknown }).channels)).toBe(true);
  });

  // ADR-0118 (S5.2): patchAgentChannel PATCHes the `/agents/{id}/channels`
  // subresource, naming the binding to move via a `?kind=&address=` selector
  // query string (never a body field, so the selector can never be confused
  // with the replacement value).
  it("patchAgentChannel_targets_the_subresource_with_the_pair_selector", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse(200, {
        id: "a1",
        name: "deal-desk",
        channels: [{ kind: "slack", address: "C0EXAMPLE2" }],
        model: null,
        created_at: "now",
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    await patchAgentChannel(
      "a1",
      { kind: "slack", address: "C0EXAMPLE1" },
      { kind: "slack", address: "C0EXAMPLE2" },
    );
    const [requestUrl, init] = fetchMock.mock.calls[0];
    expect(init.method).toBe("PATCH");
    expect(requestUrl).toBe("/api/agents/a1/channels?kind=slack&address=C0EXAMPLE1");
    expect(JSON.parse(init.body)).toEqual({ kind: "slack", address: "C0EXAMPLE2" });
  });

  it("adds and removes one surface through the binding subresource", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse(201, {
        id: "a1",
        name: "deal-desk",
        channels: [
          { kind: "slack", address: "C0EXAMPLE1" },
          { kind: "discord", address: "123456789012345678" },
        ],
        model: null,
        created_at: "now",
      }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);

    await addAgentSurface("a1", {
      kind: "discord",
      address: "123456789012345678",
      endpoint: "https://discord-adapter.example.com/replies",
      adapter: "discord",
    });
    await removeAgentSurface("a1", { kind: "slack", address: "C0EXAMPLE1" });

    expect(fetchMock.mock.calls[0][0]).toBe("/api/agents/a1/channels");
    expect(fetchMock.mock.calls[0][1].method).toBe("POST");
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({
      kind: "discord",
      address: "123456789012345678",
      endpoint: "https://discord-adapter.example.com/replies",
      adapter: "discord",
    });
    expect(fetchMock.mock.calls[1][0]).toBe(
      "/api/agents/a1/channels?kind=slack&address=C0EXAMPLE1",
    );
    expect(fetchMock.mock.calls[1][1].method).toBe("DELETE");
  });

  // ADR-0168 decision 3: the read side returns a binding's identity as
  // `adapter`, and the selector for a move or a removal carries it straight
  // through -- "default" is a real, sendable value (equivalent to
  // omitting it), not a sentinel the client has to strip.
  it("sends the selector's identity in the PATCH query when it is not null", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse(200, {
        id: "a1",
        name: "deal-desk",
        channels: [{ kind: "slack", address: "C0EXAMPLE2", adapter: "default" }],
        model: null,
        created_at: "now",
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    await patchAgentChannel(
      "a1",
      { kind: "slack", address: "C0EXAMPLE1", adapter: "default" },
      { kind: "slack", address: "C0EXAMPLE2" },
    );
    const [requestUrl] = fetchMock.mock.calls[0];
    expect(requestUrl).toBe(
      "/api/agents/a1/channels?kind=slack&address=C0EXAMPLE1&adapter=default",
    );
  });

  // A selector with no stored identity (the ordinary case today, and every
  // pair read from an API that predates ADR-0168 decision 3) must still omit
  // `adapter` entirely -- an older API has no such query parameter to parse.
  it("omits adapter from the PATCH/DELETE query when the selector's identity is null", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse(200, {
        id: "a1",
        name: "deal-desk",
        channels: [{ kind: "slack", address: "C0EXAMPLE2" }],
        model: null,
        created_at: "now",
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    await patchAgentChannel(
      "a1",
      { kind: "slack", address: "C0EXAMPLE1", adapter: null },
      { kind: "slack", address: "C0EXAMPLE2" },
    );
    expect(fetchMock.mock.calls[0][0]).toBe("/api/agents/a1/channels?kind=slack&address=C0EXAMPLE1");

    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(null, { status: 204 })));
    await removeAgentSurface("a1", { kind: "slack", address: "C0EXAMPLE1", adapter: null });
    const deleteFetch = vi.mocked(fetch);
    expect(deleteFetch.mock.calls[0][0]).toBe("/api/agents/a1/channels?kind=slack&address=C0EXAMPLE1");
  });

  it("sends the selector's identity in the DELETE query when it is not null", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);
    await removeAgentSurface("a1", { kind: "slack", address: "C0EXAMPLE1", adapter: "finance" });
    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/agents/a1/channels?kind=slack&address=C0EXAMPLE1&adapter=finance",
    );
  });

  it("activates a version by POSTing a deployment", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse(201, {
        id: "d1",
        agent_id: "a1",
        version_id: "v2",
        environment: "prod",
        commit_sha: null,
        status: "active",
        deployed_at: "now",
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const dep = await createDeployment({ agent_id: "a1", version_id: "v2", environment: "prod" });
    expect(fetchMock.mock.calls[0][0]).toBe("/api/deployments");
    expect(fetchMock.mock.calls[0][1].method).toBe("POST");
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({ agent_id: "a1", version_id: "v2", environment: "prod" });
    expect(dep.status).toBe("active");
  });
});

// #1047 (ADR-0083 slice 4): the browser authenticates only with the HttpOnly
// console session cookie. Error bodies below mirror the API verbatim:
// apps/api/src/curie_api/auth.py (require_api_key: 401 "missing or invalid API
// key"), apps/api/src/curie_api/routers/console.py (401 "missing, invalid, or
// expired console session" and "invalid or expired login code"), and
// apps/api/src/curie_api/schemas.py ConsoleSessionOut {subject, expires_at}.
describe("console cookie auth (#1047)", () => {
  const AGENT = {
    id: "a1",
    name: "deal-desk",
    channels: [{ kind: "slack", address: "C0EXAMPLE1" }],
    model: null,
    created_at: "2026-07-23T00:00:00+00:00",
  };
  const APPROVAL = {
    id: "ap-1",
    agent_id: "a1",
    conversation_id: "C-thread-1",
    author: "U-alice",
    summary: "Refund $4,200 to ACME Corp",
    reply_channel: "C0DEALS",
    reply_placeholder: "ts-1",
    reply_endpoint: null,
    dedupe_key: "dk-1",
    route: "managers",
    card_channel: "C0MANAGERS",
    gate_kind: "permission",
    granted_tool: "issue_refund",
    status: "approved",
    expires_at: "2026-07-24T00:00:00+00:00",
    resolved_by: "U0AUTHENTICATED",
    resolution_note: null,
    created_at: "2026-07-23T00:00:00+00:00",
    resolved_at: "2026-07-23T01:00:00+00:00",
  };
  const BUNDLE = { agent_id: "a1", version_id: "v1", sha256: "abc", size: 10, stored_at: "now" };

  it("every client call sends only the cookie", async () => {
    const fetchMock = vi.fn().mockImplementation((input: string) => {
      if (input === "/api/agents") return Promise.resolve(jsonResponse(200, [AGENT]));
      if (input.endsWith("/bundle")) return Promise.resolve(jsonResponse(201, BUNDLE));
      if (input.endsWith("/state")) return Promise.resolve(jsonResponse(200, []));
      if (input.endsWith("/resolve")) return Promise.resolve(jsonResponse(200, APPROVAL));
      if (input === "/api/config") return Promise.resolve(jsonResponse(200, { org_name: "Globex Corporation" }));
      return Promise.resolve(jsonResponse(404, { detail: "Not Found" }));
    });
    vi.stubGlobal("fetch", fetchMock);
    const archive = await buildBundleZip({ agentName: "x", versionLabel: "v0", skillMd: "ok" });

    await getAgents();
    await createAgent({ name: "deal-desk", channel: { kind: "slack", address: "C0EXAMPLE1" } });
    await uploadBundle("a1", "v1", archive);
    await listStateNamespaces("a1");
    await resolveApproval("ap-1", { decision: "approved" });
    await getConfig();

    expect(fetchMock).toHaveBeenCalledTimes(6);
    for (const [requestUrl, init] of fetchMock.mock.calls as Array<[string, RequestInit | undefined]>) {
      expect(init?.credentials, requestUrl).toBe("same-origin");
      expect(hasApiKeyHeader(init), requestUrl).toBe(false);
    }
    // The multipart upload must leave Content-Type to the browser (boundary).
    const uploadInit = (fetchMock.mock.calls as Array<[string, RequestInit]>).find(([u]) => u.endsWith("/bundle"))![1];
    expect(headerEntries(uploadInit).some(([name]) => /^content-type$/i.test(name))).toBe(false);
  });

  it("the key resolver is gone", () => {
    expect(Object.keys(config)).toEqual(["API_PREFIX"]);
    expect("apiKey" in client).toBe(false);
  });

  it("a ?api_key= URL parameter is never sent", async () => {
    window.history.replaceState(null, "", "/?api=1&api_key=leak-example");
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(200, [AGENT]));
    vi.stubGlobal("fetch", fetchMock);

    await getAgents();

    const [requestUrl, init] = fetchMock.mock.calls[0] as [string, RequestInit | undefined];
    expect(requestUrl).toBe("/api/agents");
    expect(requestUrl).not.toContain("leak-example");
    for (const [, value] of headerEntries(init)) expect(value).not.toContain("leak-example");
    expect(hasApiKeyHeader(init)).toBe(false);
  });

  it("a 401 notifies unauthorized listeners; other statuses do not", async () => {
    const spy = vi.fn();
    const unsubscribe = onUnauthorized(spy);
    unsubscribers.push(unsubscribe);

    // 403 and 500 are failures, but not a lost credential: no notification.
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse(403, { detail: "console session origin rejected" })));
    await expect(getAgents()).rejects.toMatchObject({ status: 403 });
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse(500, { detail: "boom" })));
    await expect(getAgents()).rejects.toMatchObject({ status: 500 });
    // A successful call does not notify either.
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse(200, [AGENT])));
    await getAgents();
    expect(spy).not.toHaveBeenCalled();

    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse(401, { detail: "missing or invalid API key" })));
    const err = (await getAgents().catch((e: unknown) => e)) as ApiError;
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(401);
    expect(err.message).toBe("missing or invalid API key");
    expect(spy).toHaveBeenCalledTimes(1);

    // Calls with their own error handling (multipart upload, resolve) notify too.
    const archive = await buildBundleZip({ agentName: "x", versionLabel: "v0", skillMd: "ok" });
    vi.stubGlobal("fetch", vi.fn().mockImplementation(() =>
      Promise.resolve(jsonResponse(401, { detail: "missing or invalid API key" })),
    ));
    await expect(uploadBundle("a1", "v1", archive)).rejects.toBeTruthy();
    expect(spy).toHaveBeenCalledTimes(2);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(
      jsonResponse(401, { detail: "missing, invalid, or expired console session" }),
    ));
    await expect(resolveApproval("ap-1", { decision: "rejected" })).rejects.toMatchObject({ status: 401 });
    expect(spy).toHaveBeenCalledTimes(3);

    unsubscribe();
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse(401, { detail: "missing or invalid API key" })));
    await expect(getAgents()).rejects.toMatchObject({ status: 401 });
    expect(spy).toHaveBeenCalledTimes(3);
  });

  it("session boundary calls never notify", async () => {
    const spy = vi.fn();
    unsubscribers.push(onUnauthorized(spy));

    const sessionFetch = vi.fn().mockResolvedValue(
      jsonResponse(401, { detail: "missing, invalid, or expired console session" }),
    );
    vi.stubGlobal("fetch", sessionFetch);
    const sessionErr = (await getConsoleSession().catch((e: unknown) => e)) as ApiError;
    expect(sessionErr).toBeInstanceOf(ApiError);
    expect(sessionErr.status).toBe(401);
    expect(sessionErr.message).toBe("missing, invalid, or expired console session");
    expect(sessionFetch.mock.calls[0][0]).toBe("/api/console/session");
    expect(sessionFetch.mock.calls[0][1]?.credentials).toBe("same-origin");
    expect(hasApiKeyHeader(sessionFetch.mock.calls[0][1])).toBe(false);

    const exchangeFetch = vi.fn().mockResolvedValue(jsonResponse(401, { detail: "invalid or expired login code" }));
    vi.stubGlobal("fetch", exchangeFetch);
    const exchangeErr = (await exchangeConsoleLoginCode("one-time-example").catch((e: unknown) => e)) as ApiError;
    expect(exchangeErr).toBeInstanceOf(ApiError);
    expect(exchangeErr.status).toBe(401);
    expect(exchangeErr.message).toBe("invalid or expired login code");
    const [exchangeUrl, exchangeInit] = exchangeFetch.mock.calls[0] as [string, RequestInit];
    expect(exchangeUrl).toBe("/api/console/session");
    expect(exchangeInit.method).toBe("POST");
    expect(JSON.parse(String(exchangeInit.body))).toEqual({ code: "one-time-example" });
    expect(hasApiKeyHeader(exchangeInit)).toBe(false);

    expect(spy).not.toHaveBeenCalled();
  });
});
