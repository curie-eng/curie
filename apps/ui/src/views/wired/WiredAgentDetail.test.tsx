import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useEffect } from "react";
import { WiredAgentDetail } from "./WiredAgentDetail";
import { StoreProvider, useStore } from "../../state/store";
import { WiredProvider } from "../../state/wired";
import {
  getAgents,
  listVersions,
  listDeployments,
  getVersionFiles,
  updateAgent,
  patchAgentChannel,
  addAgentSurface,
  removeAgentSurface,
  createDeployment,
  type AgentOut,
  type VersionOut,
  type DeploymentOut,
  type BundleFiles,
} from "../../api/client";

// Mock only the data-layer calls; preserve the real ApiError/BundleValidationError
// classes (the hooks branch on `instanceof ApiError`) and the untouched helpers.
// `patchAgentChannel` is mocked for the channel-edit tests; `updateAgent` for the
// model-edit tests; `createDeployment` for the promote-to-prod tests;
// createVersion/uploadBundle are stubbed so the deploy path never hits the
// network even though these tests don't exercise it.
vi.mock("../../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/client")>();
  return {
    ...actual,
    getAgents: vi.fn(),
    listVersions: vi.fn(),
    listDeployments: vi.fn(),
    getVersionFiles: vi.fn(),
    updateAgent: vi.fn(),
    patchAgentChannel: vi.fn(),
    addAgentSurface: vi.fn(),
    removeAgentSurface: vi.fn(),
    createVersion: vi.fn(),
    uploadBundle: vi.fn(),
    createDeployment: vi.fn(),
  };
});

// The mocked agent carries a Slack channel binding (the channel-edit test pre-fills
// the input with its address; the promote/bundle tests only need the detail to render).
const AGENT: AgentOut = {
  id: "a1",
  name: "deal-desk",
  channels: [{ kind: "slack", address: "C0123ABCD" }],
  model: null,
  created_at: "2026-07-01T00:00:00Z",
};

const version = (id: string, label: string): VersionOut => ({
  id,
  agent_id: "a1",
  version_label: label,
  bundle_ref: "ref",
  bundle_sha256: "sha",
  created_by: "ui",
  created_at: "2026-07-01T00:00:00Z",
});

const deployment = (version_id: string, environment: "prod" | "dev", deployed_at: string): DeploymentOut => ({
  id: `dep-${version_id}-${environment}`,
  agent_id: "a1",
  version_id,
  environment,
  commit_sha: null,
  status: "active",
  deployed_at,
});

// v1 is the prod-active version (what the editor loads); v2 is the dev-active
// version — the one promote-to-prod must ship. Both versions exist so the detail
// renders and devActiveVersionId resolves to v2.
const VERSIONS = [version("v1", "v0.1.0"), version("v2", "v0.2.0")];
const DEPLOYMENTS = [
  deployment("v1", "prod", "2026-07-05T00:00:00Z"),
  deployment("v2", "dev", "2026-07-07T00:00:00Z"),
];

// A bundle with a SKILL.md AND the two non-skill files the bundle-tree test must
// surface (item 4): the manifest and evals/cases.json.
const FILES: BundleFiles = {
  files: [
    { path: "skills/deal-desk/SKILL.md", content: "---\nname: deal-desk\ndescription: d\n---\n# body" },
    { path: ".claude-plugin/plugin.json", content: '{"name":"deal-desk","version":"v0.1.0"}' },
    { path: "evals/cases.json", content: "[]" },
  ],
};

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(getAgents).mockResolvedValue([AGENT]);
  vi.mocked(listVersions).mockResolvedValue(VERSIONS);
  vi.mocked(listDeployments).mockResolvedValue(DEPLOYMENTS);
  vi.mocked(getVersionFiles).mockResolvedValue(FILES);
  vi.mocked(updateAgent).mockResolvedValue(AGENT);
  vi.mocked(patchAgentChannel).mockResolvedValue({
    ...AGENT,
    channels: [{ kind: "slack", address: "C9999ZZZZ" }],
  });
  vi.mocked(addAgentSurface).mockResolvedValue(AGENT);
  vi.mocked(removeAgentSurface).mockResolvedValue();
  vi.mocked(createDeployment).mockResolvedValue(deployment("v2", "prod", "2026-07-08T00:00:00Z"));
});

// Render the detail surface directly, dispatching openAgentDetail on mount so the
// store points at the mocked agent (the same action the Agents list dispatches).
function Harness() {
  const { dispatch } = useStore();
  useEffect(() => {
    dispatch({ type: "openAgentDetail", id: AGENT.id });
  }, [dispatch]);
  return <WiredAgentDetail />;
}

function renderDetail() {
  // WiredAgentDetail consumes react-query hooks (useAgentVersions/useVersionFiles),
  // so it needs a QueryClientProvider. A fresh client per render with retry off,
  // mirroring main.tsx and hooks.rq.test.tsx, so error/404 sentinels resolve on the
  // first response instead of being retried.
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <StoreProvider>
        <WiredProvider>
          <Harness />
        </WiredProvider>
      </StoreProvider>
    </QueryClientProvider>,
  );
}

describe("WiredAgentDetail — channel edit (item 5)", () => {
  it("saves an edited Slack channel via patchAgentChannel and refetches the agent list", async () => {
    const user = userEvent.setup();
    renderDetail();

    // Agent header loads once getAgents resolves; getAgents fired exactly once.
    expect(await screen.findByTestId("agent-detail-name")).toHaveTextContent("deal-desk");
    await waitFor(() => expect(getAgents).toHaveBeenCalledTimes(1));

    // The channel is now an editable input pre-filled with the current value.
    const input = await screen.findByTestId("channel-input");
    expect(input).toHaveValue("C0123ABCD");

    await user.clear(input);
    await user.type(input, "C9999ZZZZ");
    await user.click(screen.getByTestId("channel-save"));

    // Exactly one PATCH, with the right args: the selector names the binding's
    // ORIGINAL pair, never updateAgent.
    await waitFor(() => expect(patchAgentChannel).toHaveBeenCalledTimes(1));
    expect(patchAgentChannel).toHaveBeenCalledWith(
      "a1",
      { kind: "slack", address: "C0123ABCD" },
      { kind: "slack", address: "C9999ZZZZ" },
    );
    expect(updateAgent).not.toHaveBeenCalled();

    // Save triggers a refetch of the wired agent data (getAgents runs a 2nd time).
    await waitFor(() => expect(getAgents).toHaveBeenCalledTimes(2));
  });

  it("blocks saving an empty channel (no patchAgentChannel call)", async () => {
    const user = userEvent.setup();
    renderDetail();

    const input = await screen.findByTestId("channel-input");
    await user.clear(input);

    // Save is disabled / a no-op while the channel is empty.
    const save = screen.getByTestId("channel-save");
    await user.click(save).catch(() => {});
    expect(patchAgentChannel).not.toHaveBeenCalled();
  });

  it("warns on a non-Slack-ID channel but still allows saving (soft check)", async () => {
    const user = userEvent.setup();
    renderDetail();

    const input = await screen.findByTestId("channel-input");
    await user.clear(input);
    await user.type(input, "revenue-ops");

    // Soft warning shows (mirrors NewAgentModal's CHANNEL_ID_RE warning)…
    expect(screen.getByTestId("channel-warn")).toBeInTheDocument();

    // …but the value still saves.
    await user.click(screen.getByTestId("channel-save"));
    await waitFor(() =>
      expect(patchAgentChannel).toHaveBeenCalledWith(
        "a1",
        { kind: "slack", address: "C0123ABCD" },
        { kind: "slack", address: "revenue-ops" },
      ),
    );
  });

  it("does not show the Slack-shape warning for a non-slack kind", async () => {
    // ADR-0096 (#1459): the "does not look like a channel ID (C…)" hint is a
    // SLACK fact. A webhook binding's address is an arbitrary string by design,
    // so a kind-blind check warns on every correct value an adapter will ever
    // use. A permanent warning on a correct value is worse than no warning: the
    // operator learns to ignore this line, and the hint stops working for the
    // Slack binding it was written for (#143's guidance is the deliverable).
    const user = userEvent.setup();
    vi.mocked(getAgents).mockResolvedValue([
      { ...AGENT, channels: [{ kind: "webhook", address: "acme-room-7" }] },
    ]);
    renderDetail();

    const input = await screen.findByTestId("channel-input");
    expect(input).toHaveValue("acme-room-7");
    expect(screen.queryByTestId("channel-warn")).not.toBeInTheDocument();

    // A different non-Slack-shaped address is still not warned on, so this
    // cannot be satisfied by the seeded value happening to pass some rule.
    await user.clear(input);
    await user.type(input, "room/42");
    expect(screen.queryByTestId("channel-warn")).not.toBeInTheDocument();

    // The kind rides along on the save, unchanged.
    await user.click(screen.getByTestId("channel-save"));
    await waitFor(() =>
      expect(patchAgentChannel).toHaveBeenCalledWith(
        "a1",
        { kind: "webhook", address: "acme-room-7" },
        { kind: "webhook", address: "room/42" },
      ),
    );
  });

  it("moves a surface without reading or echoing its write-only reply route", async () => {
    // AgentOut intentionally exposes only the routing pair. PATCH omission is
    // the server-side preserve gesture, so the console must send only what it
    // can honestly know instead of manufacturing secret route fields.
    const user = userEvent.setup();
    vi.mocked(getAgents).mockResolvedValue([
      {
        ...AGENT,
        channels: [{ kind: "webhook", address: "acme-room-7" }],
      },
    ]);
    renderDetail();

    const input = await screen.findByTestId("channel-input");
    await user.clear(input);
    await user.type(input, "acme-room-8");
    await user.click(screen.getByTestId("channel-save"));

    await waitFor(() =>
      expect(patchAgentChannel).toHaveBeenCalledWith(
        "a1",
        { kind: "webhook", address: "acme-room-7" },
        { kind: "webhook", address: "acme-room-8" },
      ),
    );
  });

  it("still warns on a non-Slack-ID address when the kind IS slack", async () => {
    // The negative control for the test above. Dispatching on kind is only
    // correct if the slack arm keeps firing; a fix that disabled the check
    // outright would pass the webhook test and silently re-open #143.
    const user = userEvent.setup();
    renderDetail();

    const input = await screen.findByTestId("channel-input");
    await user.clear(input);
    await user.type(input, "revenue-ops");
    expect(screen.getByTestId("channel-warn")).toBeInTheDocument();
  });

  it("adds a second surface and thereby opts the agent in", async () => {
    const user = userEvent.setup();
    renderDetail();

    await user.type(await screen.findByTestId("surface-kind-new"), "discord");
    await user.type(screen.getByTestId("surface-address-new"), "123456789012345678");
    await user.type(screen.getByTestId("surface-endpoint-new"), "https://discord-adapter.example.com/replies");
    await user.type(screen.getByTestId("surface-adapter-new"), "discord");
    await user.click(screen.getByTestId("surface-add"));

    await waitFor(() =>
      expect(addAgentSurface).toHaveBeenCalledWith("a1", {
        kind: "discord",
        address: "123456789012345678",
        endpoint: "https://discord-adapter.example.com/replies",
        adapter: "discord",
      }),
    );
  });

  it("removes a selected surface but leaves final-surface enforcement to the API", async () => {
    const user = userEvent.setup();
    vi.mocked(getAgents).mockResolvedValue([
      {
        ...AGENT,
        channels: [
          { kind: "slack", address: "C0123ABCD" },
          { kind: "discord", address: "123456789012345678" },
        ],
      },
    ]);
    renderDetail();

    await user.click(await screen.findByTestId("surface-remove-1"));
    await waitFor(() =>
      expect(removeAgentSurface).toHaveBeenCalledWith("a1", {
        kind: "discord",
        address: "123456789012345678",
      }),
    );
  });
});

describe("WiredAgentDetail — model edit (#254)", () => {
  it("saves an edited per-agent model via updateAgent and refetches", async () => {
    const user = userEvent.setup();
    renderDetail();

    expect(await screen.findByTestId("agent-detail-name")).toHaveTextContent("deal-desk");
    await waitFor(() => expect(getAgents).toHaveBeenCalledTimes(1));

    // Model field seeds empty (AGENT.model is null) and accepts a model id.
    const input = await screen.findByTestId("model-input");
    expect(input).toHaveValue("");
    await user.type(input, "glm-5.2");
    await user.click(screen.getByTestId("model-save"));

    await waitFor(() => expect(updateAgent).toHaveBeenCalledTimes(1));
    expect(updateAgent).toHaveBeenCalledWith("a1", { model: "glm-5.2" });
    // Save refetches the wired agent list (getAgents runs a 2nd time).
    await waitFor(() => expect(getAgents).toHaveBeenCalledTimes(2));
  });

  // #1355. Blanking the box is the only clear gesture there is, and it used to
  // send `""`, which the API now refuses, and which reached the worker falsy
  // and skipped the platform default this test claims to restore. The assertion
  // was green through the whole defect because it asserted the bug's behavior,
  // so pin the wire value, not just "some call happened".
  it("clears the model to the platform default by sending null on save", async () => {
    const user = userEvent.setup();
    // This agent already has a pinned model, so the field seeds with it.
    vi.mocked(getAgents).mockResolvedValue([{ ...AGENT, model: "kimi-k2" }]);
    renderDetail();

    const input = await screen.findByTestId("model-input");
    await waitFor(() => expect(input).toHaveValue("kimi-k2"));
    await user.clear(input);
    await user.click(screen.getByTestId("model-save"));

    await waitFor(() => expect(updateAgent).toHaveBeenCalledWith("a1", { model: null }));
  });

  // Whitespace is the same gesture typed differently, and it is worse than empty:
  // it survives the falsy check downstream and is stored AND forwarded as a
  // garbage model id. It must reach the wire as the same null, not as "  ".
  it("treats a whitespace-only box as the same clear", async () => {
    const user = userEvent.setup();
    vi.mocked(getAgents).mockResolvedValue([{ ...AGENT, model: "kimi-k2" }]);
    renderDetail();

    const input = await screen.findByTestId("model-input");
    await waitFor(() => expect(input).toHaveValue("kimi-k2"));
    await user.clear(input);
    await user.type(input, "   ");
    await user.click(screen.getByTestId("model-save"));

    await waitFor(() => expect(updateAgent).toHaveBeenCalledWith("a1", { model: null }));
  });
});

describe("WiredAgentDetail — bundle tree (item 4)", () => {
  it("renders every bundle file, not just SKILL.md", async () => {
    renderDetail();
    expect(await screen.findByTestId("agent-detail-name")).toBeInTheDocument();

    // The full bundle tree is visible: the non-skill files are findable in the DOM.
    await waitFor(() => {
      expect(screen.getAllByText("evals/cases.json").length).toBeGreaterThan(0);
      expect(screen.getAllByText(".claude-plugin/plugin.json").length).toBeGreaterThan(0);
    });

    // SKILL.md is still present so the existing edit/deploy path is not lost.
    expect(screen.getAllByText("skills/deal-desk/SKILL.md").length).toBeGreaterThan(0);
  });
});

describe("WiredAgentDetail — promote-to-prod (item 6)", () => {
  it("promotes the dev-active version to prod on confirm, then refreshes", async () => {
    const user = userEvent.setup();
    renderDetail();

    // The detail body renders once agents + versions + files have loaded.
    expect(await screen.findByTestId("agent-detail-name")).toHaveTextContent("deal-desk");

    const promote = await screen.findByRole("button", { name: "Promote to prod" });
    expect(vi.mocked(listDeployments)).toHaveBeenCalledTimes(1);

    await user.click(promote);
    // Inline confirm (kill-switch pattern): nothing deployed until confirmed.
    await user.click(await screen.findByRole("button", { name: "Confirm promote" }));

    await waitFor(() => expect(vi.mocked(createDeployment)).toHaveBeenCalledTimes(1));
    expect(vi.mocked(createDeployment)).toHaveBeenCalledWith({
      agent_id: "a1",
      version_id: "v2",
      environment: "prod",
    });

    // A refresh follows the promote (re-fetch versions + deployments).
    await waitFor(() => expect(vi.mocked(listDeployments).mock.calls.length).toBeGreaterThan(1));
  });

  it("deploys nothing when the promote confirm is cancelled", async () => {
    const user = userEvent.setup();
    renderDetail();

    const promote = await screen.findByRole("button", { name: "Promote to prod" });
    await user.click(promote);
    await user.click(await screen.findByRole("button", { name: "Cancel" }));

    expect(vi.mocked(createDeployment)).not.toHaveBeenCalled();
  });
});

describe("WiredAgentDetail — multi-channel edit (ADR-0116)", () => {
  // [FAIL-FIRST] S5.3/S5.4: the current component renders exactly one channel
  // editor, keyed off the singular `agent.channel`. Once channels go plural
  // each binding gets its own editor keyed by `${kind}:${address}`, and
  // saving one PATCHes only that binding via patchAgentChannel's pair
  // selector -- the other row is untouched. Fails today: only one editor
  // exists, so `findAllByTestId("channel-input")` never reaches length 2.
  it("renders_one_editor_per_binding_and_saves_only_that_one", async () => {
    const user = userEvent.setup();
    const twoBindingAgent = {
      ...AGENT,
      channels: [
        { kind: "slack", address: "C0EXAMPLE1" },
        { kind: "slack", address: "C0EXAMPLE2" },
      ],
    } as unknown as AgentOut;
    vi.mocked(getAgents).mockResolvedValue([twoBindingAgent]);
    renderDetail();

    expect(await screen.findByTestId("agent-detail-name")).toHaveTextContent("deal-desk");

    const inputs = await screen.findAllByTestId("channel-input");
    expect(inputs).toHaveLength(2);
    expect(inputs[0]).toHaveValue("C0EXAMPLE1");
    expect(inputs[1]).toHaveValue("C0EXAMPLE2");

    // Edit and save only the second binding's row.
    await user.clear(inputs[1]);
    await user.type(inputs[1], "C0EXAMPLE3");
    const saves = screen.getAllByTestId("channel-save");
    await user.click(saves[1]);

    // The PATCH names the second pair as the selector and its new address as
    // the replacement -- never a bare updateAgent call, and never the first
    // binding's pair.
    await waitFor(() => expect(patchAgentChannel).toHaveBeenCalledTimes(1));
    expect(patchAgentChannel).toHaveBeenCalledWith(
      "a1",
      { kind: "slack", address: "C0EXAMPLE2" },
      { kind: "slack", address: "C0EXAMPLE3" },
    );
    expect(updateAgent).not.toHaveBeenCalled();

    // The first row is untouched by the second row's save.
    expect(inputs[0]).toHaveValue("C0EXAMPLE1");
  });

  // [FAIL-FIRST] finding 3, #1525 code review: the retired-selector bug. The
  // fix is only real if the second save still resolves correctly while the
  // refetch triggered by the first save is still in flight -- so `getAgents`
  // is wired to hang past its first (initial-load) call, and only the row's
  // own save response can rebase the selector.
  it("rebases a row's selector onto the just-saved pair, and an in-flight refetch does not clobber another row's unsaved edit", async () => {
    const user = userEvent.setup();
    const twoBindingAgent = {
      ...AGENT,
      channels: [
        { kind: "slack", address: "C0EXAMPLE1" },
        { kind: "slack", address: "C0EXAMPLE2" },
      ],
    } as unknown as AgentOut;

    // Call 1 (initial load) resolves immediately. Every later call (the
    // refetches fired by each save) hangs until the test explicitly resolves
    // it, so "immediately save again" genuinely races an unresolved refetch
    // instead of a fast mock that happens to beat it.
    const refetchResolvers: Array<(agents: AgentOut[]) => void> = [];
    let getAgentsCalls = 0;
    vi.mocked(getAgents).mockImplementation(() => {
      getAgentsCalls += 1;
      if (getAgentsCalls === 1) return Promise.resolve([twoBindingAgent]);
      return new Promise<AgentOut[]>((resolve) => refetchResolvers.push(resolve));
    });

    // Save 1: move the second binding C0EXAMPLE2 -> C0EXAMPLE3.
    vi.mocked(patchAgentChannel).mockResolvedValueOnce({
      ...twoBindingAgent,
      channels: [
        { kind: "slack", address: "C0EXAMPLE1" },
        { kind: "slack", address: "C0EXAMPLE3" },
      ],
    });

    renderDetail();
    expect(await screen.findByTestId("agent-detail-name")).toHaveTextContent("deal-desk");

    const inputs = await screen.findAllByTestId("channel-input");
    expect(inputs).toHaveLength(2);
    const saves = screen.getAllByTestId("channel-save");

    await user.clear(inputs[1]);
    await user.type(inputs[1], "C0EXAMPLE3");
    await user.click(saves[1]);
    await waitFor(() => expect(patchAgentChannel).toHaveBeenCalledTimes(1));
    // The refetch save 1 triggered is now hanging (getAgentsCalls === 2).
    await waitFor(() => expect(getAgentsCalls).toBe(2));

    // Type an unsaved edit into the OTHER (untouched) row -- it must survive
    // the pending refetch from save 1's completion.
    await user.clear(inputs[0]);
    await user.type(inputs[0], "C0EXAMPLE9");

    // Save 2: immediately edit and save the SAME row again, before save 1's
    // refetch has landed.
    vi.mocked(patchAgentChannel).mockResolvedValueOnce({
      ...twoBindingAgent,
      channels: [
        { kind: "slack", address: "C0EXAMPLE1" },
        { kind: "slack", address: "C0EXAMPLE4" },
      ],
    });
    await user.clear(inputs[1]);
    await user.type(inputs[1], "C0EXAMPLE4");
    await user.click(saves[1]);

    // The second PATCH's selector names the pair from save 1 (C0EXAMPLE3) --
    // not the retired C0EXAMPLE2 that agent.channels is still stuck on while
    // save 1's refetch hangs. Fails against the current code, which reuses
    // the (stale) `agent.channels` entry as the selector.
    await waitFor(() => expect(patchAgentChannel).toHaveBeenCalledTimes(2));
    expect(patchAgentChannel).toHaveBeenNthCalledWith(
      2,
      "a1",
      { kind: "slack", address: "C0EXAMPLE3" },
      { kind: "slack", address: "C0EXAMPLE4" },
    );

    // Now let save 1's refetch land with server-fresh data, while row 0's
    // unsaved "C0EXAMPLE9" edit is still sitting in the input. Its completion
    // must not stomp that unsaved edit back to the server's C0EXAMPLE1.
    refetchResolvers[0]([
      {
        ...twoBindingAgent,
        channels: [
          { kind: "slack", address: "C0EXAMPLE1" },
          { kind: "slack", address: "C0EXAMPLE3" },
        ],
      },
    ]);
    await waitFor(() => expect(inputs[0]).toHaveValue("C0EXAMPLE9"));
  });
});

describe("WiredAgentDetail — CLI hint (#279)", () => {
  it("renders the deploy CLI hint sourced from the manifest next to Deploy", async () => {
    renderDetail();

    // Deploy only renders once versions + bundle files resolve.
    expect(await screen.findByRole("button", { name: "Deploy new version" })).toBeInTheDocument();

    // The hint copies the exact `curie cluster deploy` (env clamps to prod at
    // level 3), resolved from the command manifest via cliCommand().
    expect(
      screen.getByRole("button", { name: "Copy command: curie cluster deploy" }),
    ).toBeInTheDocument();
  });
});
