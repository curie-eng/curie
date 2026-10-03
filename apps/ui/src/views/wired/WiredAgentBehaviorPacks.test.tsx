import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { StoreProvider } from "../../state/store";
import { WiredAgentBehaviorPacks } from "./WiredAgentBehaviorPacks";
import {
  getBehaviorPacks,
  putBehaviorPacks,
  ApiError,
  type BehaviorPacksConfig,
} from "../../api/client";

// Mock only the behavior-packs data-layer calls; keep everything else real.
vi.mock("../../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/client")>();
  return {
    ...actual,
    getBehaviorPacks: vi.fn(),
    putBehaviorPacks: vi.fn(),
  };
});

function makeConfig(overrides: Partial<BehaviorPacksConfig> = {}): BehaviorPacksConfig {
  return {
    load: { enabled: false, lines: [] },
    tips: { enabled: false, tips: [] },
    greeting: { enabled: false, phrases: [], reply: "" },
    help: { enabled: false, phrases: [], reply: "" },
    settings: { enabled: false, settings: [] },
    nav: { enabled: false, hub_label: "", hub_command: "" },
    ...overrides,
  };
}

function renderPanel() {
  return render(
    <StoreProvider>
      <WiredAgentBehaviorPacks agentId="a1" />
    </StoreProvider>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("WiredAgentBehaviorPacks (#870)", () => {
  it("renders and edits a config with omitted optional defaults", async () => {
    // apps/api/openapi.json declares every BehaviorPacksConfig property optional.
    vi.mocked(getBehaviorPacks).mockResolvedValue({} as BehaviorPacksConfig);
    vi.mocked(putBehaviorPacks).mockImplementation(async (_id, cfg) => cfg);
    renderPanel();

    expect(await screen.findByTestId("load-lines")).toHaveValue("");
    expect(screen.getByTestId("pack-toggle-load")).not.toBeChecked();
    expect(screen.getByText("No settings declared.")).toBeInTheDocument();
    await userEvent.click(screen.getByTestId("pack-toggle-load"));
    await userEvent.type(screen.getByTestId("load-lines"), "thinking");
    await userEvent.click(screen.getByTestId("behavior-packs-save"));

    await waitFor(() => expect(putBehaviorPacks).toHaveBeenCalledTimes(1));
    expect(vi.mocked(putBehaviorPacks).mock.calls[0][1].load).toEqual({
      enabled: true,
      lines: ["thinking"],
    });
    expect(await screen.findByTestId("behavior-packs-saved")).toHaveTextContent("Saved");
  });

  it("renders every pack with its enabled state from the GET", async () => {
    vi.mocked(getBehaviorPacks).mockResolvedValue(
      makeConfig({ load: { enabled: true, lines: ["thinking…"] } }),
    );
    renderPanel();

    expect(await screen.findByTestId("pack-load")).toBeInTheDocument();
    for (const name of ["load", "tips", "greeting", "help", "nav", "settings"]) {
      expect(screen.getByTestId(`pack-${name}`)).toBeInTheDocument();
    }
    // load is on; the rest are off.
    expect(screen.getByTestId("pack-toggle-load")).toBeChecked();
    expect(screen.getByTestId("pack-toggle-tips")).not.toBeChecked();
    expect(screen.getByTestId("load-lines")).toHaveValue("thinking…");
  });

  it("Save is gated until an edit makes the panel dirty", async () => {
    vi.mocked(getBehaviorPacks).mockResolvedValue(makeConfig());
    renderPanel();

    const save = await screen.findByTestId("behavior-packs-save");
    expect(save).toBeDisabled();
    expect(screen.getByText("no changes")).toBeInTheDocument();

    await userEvent.click(screen.getByTestId("pack-toggle-tips"));
    expect(save).toBeEnabled();
    expect(screen.getByText("unsaved changes")).toBeInTheDocument();
  });

  it("toggling a pack and saving PUTs the full config with the flag flipped", async () => {
    const initial = makeConfig();
    vi.mocked(getBehaviorPacks).mockResolvedValue(initial);
    vi.mocked(putBehaviorPacks).mockImplementation(async (_id, cfg) => cfg);
    renderPanel();

    await userEvent.click(await screen.findByTestId("pack-toggle-greeting"));
    await userEvent.click(screen.getByTestId("behavior-packs-save"));

    await waitFor(() => expect(putBehaviorPacks).toHaveBeenCalledTimes(1));
    const [id, sent] = vi.mocked(putBehaviorPacks).mock.calls[0];
    expect(id).toBe("a1");
    expect(sent.greeting.enabled).toBe(true);
    // The rest of the config is round-tripped untouched.
    expect(sent.load.enabled).toBe(false);
    expect(await screen.findByTestId("behavior-packs-saved")).toHaveTextContent("Saved");
  });

  it("edits list content and drops blank lines on save", async () => {
    vi.mocked(getBehaviorPacks).mockResolvedValue(makeConfig({ load: { enabled: true, lines: [] } }));
    vi.mocked(putBehaviorPacks).mockImplementation(async (_id, cfg) => cfg);
    renderPanel();

    const lines = await screen.findByTestId("load-lines");
    await userEvent.clear(lines);
    // A blank middle line must not survive to the PUT body.
    await userEvent.type(lines, "one\n\ntwo");
    await userEvent.click(screen.getByTestId("behavior-packs-save"));

    await waitFor(() => expect(putBehaviorPacks).toHaveBeenCalledTimes(1));
    const [, sent] = vi.mocked(putBehaviorPacks).mock.calls[0];
    expect(sent.load.lines).toEqual(["one", "two"]);
  });

  it("round-trips the read-only settings pack unchanged", async () => {
    const declared = {
      enabled: true,
      settings: [
        { key: "tone", label: "Tone", kind: "str", default: "formal", help: "", choices: [], applies_live: true },
      ],
    };
    vi.mocked(getBehaviorPacks).mockResolvedValue(makeConfig({ settings: declared }));
    vi.mocked(putBehaviorPacks).mockImplementation(async (_id, cfg) => cfg);
    renderPanel();

    // The declared knob is surfaced read-only.
    expect(await screen.findByTestId("settings-knob")).toHaveTextContent("tone");

    // Make an unrelated edit, save, and confirm settings survived verbatim.
    await userEvent.click(screen.getByTestId("pack-toggle-tips"));
    await userEvent.click(screen.getByTestId("behavior-packs-save"));

    await waitFor(() => expect(putBehaviorPacks).toHaveBeenCalledTimes(1));
    const [, sent] = vi.mocked(putBehaviorPacks).mock.calls[0];
    expect(sent.settings).toEqual(declared);
  });

  it("Reset reverts edits back to the last-saved baseline", async () => {
    vi.mocked(getBehaviorPacks).mockResolvedValue(makeConfig());
    renderPanel();

    await userEvent.click(await screen.findByTestId("pack-toggle-nav"));
    expect(screen.getByTestId("pack-toggle-nav")).toBeChecked();

    await userEvent.click(screen.getByRole("button", { name: "Reset" }));
    expect(screen.getByTestId("pack-toggle-nav")).not.toBeChecked();
    expect(screen.getByText("no changes")).toBeInTheDocument();
  });

  it("surfaces a load error", async () => {
    vi.mocked(getBehaviorPacks).mockRejectedValue(new Error("boom"));
    renderPanel();
    expect(await screen.findByTestId("behavior-packs-error")).toHaveTextContent("boom");
  });

  it("surfaces a save error and leaves edits intact", async () => {
    vi.mocked(getBehaviorPacks).mockResolvedValue(makeConfig());
    vi.mocked(putBehaviorPacks).mockRejectedValue(new ApiError(422, "settings.key: invalid"));
    renderPanel();

    await userEvent.click(await screen.findByTestId("pack-toggle-load"));
    await userEvent.click(screen.getByTestId("behavior-packs-save"));

    expect(await screen.findByTestId("behavior-packs-save-error")).toHaveTextContent("settings.key: invalid");
    // The edit is still pending (not lost) so the operator can correct + retry.
    expect(screen.getByTestId("pack-toggle-load")).toBeChecked();
  });
});
