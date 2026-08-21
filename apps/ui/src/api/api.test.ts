import { afterEach, describe, expect, it, vi } from "vitest";
import { bundleFileTree, buildBundleZip, bundleTreeFromFiles, nextVersionLabel } from "./bundle";
import {
  createAgent,
  updateAgent,
  // ADR-0116 (S5.2): moves a single channel binding via the /channels
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
} from "./client";

afterEach(() => {
  vi.unstubAllGlobals();
});

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
  it("sends the API key and returns the parsed agent", async () => {
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
    expect((init.headers as Record<string, string>)["X-API-Key"]).toBeTruthy();
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
  // (below), not updateAgent -- ADR-0116 dropped `channel` from updateAgent's
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
    expect((init.headers as Record<string, string>)["X-API-Key"]).toBeTruthy();
  });

  // ADR-0116: an agent binds one-or-more channels, so AgentOut carries
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

  // ADR-0116 (S5.2): patchAgentChannel PATCHes the `/agents/{id}/channels`
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
