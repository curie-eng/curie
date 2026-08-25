// The pack editor is the one view in this app that cannot be verified by opening
// it: it renders nothing until a platform API answers and an agent exists. So
// the behaviours worth asserting are asserted here -- that it opens an agent the
// platform tolerates, that it names the two ways a pack is silently inert, that
// the preview agrees with the matcher, and that Save writes the draft to the
// right agent and nothing else.

import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { AppProvider } from "../bridge/app";
import { SlackPacks } from "./BuildPacks";
import type { ApiRequest, CurieBridge } from "../bridge/bridge";

const AGENT = "11111111-2222-3333-4444-555555555555";

interface Recorded {
  readonly method: string;
  readonly path: string;
  readonly body?: unknown;
}

let calls: Recorded[] = [];
let stored: unknown = null;
let reachable = true;
let agents: unknown[] = [];

function stubShell(): CurieBridge {
  const connection = async () => ({
    baseUrl: "http://localhost:8080",
    hasKey: true,
    reachable,
    orgName: "acme",
    checkedAt: 1,
  });
  return {
    env: async () => ({
      cliPath: "/usr/local/bin/curie",
      cliVersion: "curie 0.1.0",
      sourceCheckout: true,
      repoRoot: null,
      dockerAvailable: false,
      kubectlAvailable: false,
      helmAvailable: false,
      platform: "darwin",
      appVersion: "0.1.0",
      electronVersion: "34",
      chromeVersion: "132",
      drift: null,
    }),
    cli: {
      preview: async () => ({ argv: [], display: "", cwd: "/tmp" }),
      run: async () => ({ runId: "r1", command: { argv: [], display: "", cwd: "/tmp" } }),
      cancel: async () => {},
      write: async () => {},
      onChunk: () => () => {},
      onResult: () => () => {},
    },
    resources: { start: async () => {}, stop: async () => {}, onFrame: () => () => {}, logs: async () => "" },
    workspace: {
      list: async () => [],
      open: async () => null,
      add: async () => null,
      forget: async () => {},
      files: async () => [],
      readFile: async () => "",
      writeFile: async () => {},
      revealInFileManager: async () => {},
    },
    api: {
      connection,
      connect: connection,
      request: async (req: ApiRequest) => {
        calls.push({ method: req.method, path: req.path, body: req.body });
        if (req.path === "/agents") return { status: 200, ok: true, body: agents as never };
        if (req.path.endsWith("/behavior-packs")) {
          if (req.method === "PUT") stored = req.body;
          return { status: 200, ok: true, body: stored as never };
        }
        return { status: 404, ok: false, body: undefined as never, error: "404" };
      },
    },
    secrets: { list: async () => [], set: async () => {}, unset: async () => {} },
    graph: { load: async () => null, save: async () => {} },
    shell: { openExternal: async () => {}, copy: async () => {} },
  };
}

function mount(plugin?: Parameters<typeof SlackPacks>[0]["plugin"]) {
  return render(
    <AppProvider>
      <SlackPacks plugin={plugin} />
    </AppProvider>,
  );
}

/** The card for one pack, so a query cannot accidentally match another's field. */
function card(title: string): HTMLElement {
  return screen.getByText(title).closest("div[style]")!.parentElement!.parentElement!.parentElement!;
}

async function toggle(title: string) {
  const label = screen.getByText(title).closest("label")!;
  await userEvent.click(within(label).getByRole("switch"));
}

beforeEach(() => {
  calls = [];
  reachable = true;
  stored = null;
  agents = [{ id: AGENT, name: "sre-bot", channel: { kind: "slack" } }];
  window.curie = stubShell();
});

afterEach(() => {
  delete window.curie;
});

describe("gating", () => {
  it("says what to do when there is no API, because packs are not in the bundle", async () => {
    reachable = false;
    mount();
    expect(await screen.findByText("No platform API")).toBeInTheDocument();
    // And it must not have tried to read packs from nowhere.
    expect(calls.some((c) => c.path.includes("behavior-packs"))).toBe(false);
  });

  it("points at the ladder when nothing is deployed yet", async () => {
    agents = [];
    mount();
    expect(await screen.findByText("No agents deployed yet")).toBeInTheDocument();
  });

  it("says a pack is stored on the agent rather than in the bundle", async () => {
    mount();
    expect(await screen.findByText(/stored on the agent, not in the bundle/)).toBeInTheDocument();
  });
});

describe("opening an agent", () => {
  it("reads that agent's packs and offers all six", async () => {
    mount();
    await waitFor(() =>
      expect(calls).toContainEqual({
        method: "GET",
        path: `/agents/${AGENT}/behavior-packs`,
        body: undefined,
      }),
    );
    for (const title of ["Load lines", "Tips", "Greeting", "Help", "Settings", "Hub button"]) {
      expect(await screen.findByText(title)).toBeInTheDocument();
    }
    expect(await screen.findByText("all packs off")).toBeInTheDocument();
  });

  it("opens an agent whose stored blob is malformed, rather than refusing", async () => {
    // BehaviorPacks.from_config never raises for exactly this reason: a corrupt
    // blob must not brick the agent. An editor that threw could not open it.
    stored = { load: "not a pack", greeting: { phrases: "hi" }, future_pack: 1 };
    mount();
    expect(await screen.findByText("all packs off")).toBeInTheDocument();
  });

  it("marks the settings pack as having no runtime, because it has none", async () => {
    mount();
    expect(await screen.findByText("no runtime yet")).toBeInTheDocument();
  });

  it("warns when the agent has no surface bound", async () => {
    agents = [{ id: AGENT, name: "sre-bot", channel: null }];
    mount();
    expect(await screen.findByText(/no surface bound/)).toBeInTheDocument();
  });
});

describe("the ways a pack is silently inert", () => {
  it("names an enabled greeting with no reply, and marks the card", async () => {
    mount();
    await screen.findByText("Greeting");
    await toggle("Greeting");

    // The platform's own short circuit: no reply means the matcher returns before
    // it looks at the phrases, so the pack is on and dead. Matched on the issue's
    // own wording -- the field hint mentions "never fires" too.
    expect(await screen.findByText(/Enabled with no reply/)).toBeInTheDocument();
    expect(within(card("Greeting")).getByText("does nothing")).toBeInTheDocument();
  });

  it("stops saying it once the pack is usable", async () => {
    mount();
    await screen.findByText("Load lines");
    await toggle("Load lines");
    expect(await screen.findByText(/no lines/)).toBeInTheDocument();

    await userEvent.type(screen.getByPlaceholderText("is crunching the numbers..."), "is working");
    await userEvent.click(screen.getByRole("button", { name: "Add" }));

    await waitFor(() => expect(screen.queryByText(/no lines/)).not.toBeInTheDocument());
  });
});

describe("the preview", () => {
  beforeEach(() => {
    stored = {
      greeting: { enabled: true, phrases: ["hi", "good morning"], reply: "Hi! I triage alerts." },
      load: { enabled: true, lines: ["is triaging"] },
    };
  });

  it("shows the canned reply for a bare greeting, and says no model was called", async () => {
    mount();
    await screen.findByDisplayValue("hey there team");
    // "hey there team" trails filler, but "hey" is not one of this pack's phrases,
    // so it must NOT match. That is what proves the preview runs the real matcher
    // rather than a substring search.
    expect(await screen.findByText(/reaches the model as a normal turn/)).toBeInTheDocument();

    await userEvent.clear(screen.getByDisplayValue("hey there team"));
    // Punctuation, case and a filler tail all survive normalisation.
    await userEvent.type(screen.getByPlaceholderText("hi"), "Good Morning, everyone!");

    const answered = await screen.findByText(/Answered by the greeting pack/);
    // Scoped to the preview: the reply is also the greeting card's textarea value,
    // which jsdom exposes as text.
    expect(answered.parentElement).toHaveTextContent("Hi! I triage alerts.");
  });

  it("does not fire when a real request is glued to the greeting", async () => {
    mount();
    await screen.findByDisplayValue("hey there team");
    await userEvent.clear(screen.getByDisplayValue("hey there team"));
    await userEvent.type(screen.getByPlaceholderText("hi"), "hi show me the report");
    expect(await screen.findByText(/reaches the model as a normal turn/)).toBeInTheDocument();
  });

  it("shows the caption the load pack produces", async () => {
    mount();
    await waitFor(() => expect(screen.getAllByText("is triaging").length).toBeGreaterThan(0));
  });

  it("shows the platform default when no caption pack is on", async () => {
    stored = null;
    mount();
    expect(await screen.findByText(/That is the platform default/)).toBeInTheDocument();
    expect(screen.getAllByText("is working on your request...").length).toBe(3);
  });
});

describe("saving", () => {
  it("writes the draft to that agent and nothing else", async () => {
    mount();
    await screen.findByText("Hub button");
    await toggle("Hub button");

    await userEvent.type(screen.getByPlaceholderText("Help"), "Home");
    await userEvent.type(screen.getByPlaceholderText("hub"), "go_home");

    await userEvent.click(screen.getByRole("button", { name: "Save to agent" }));

    await waitFor(() => {
      const put = calls.find((c) => c.method === "PUT");
      expect(put?.path).toBe(`/agents/${AGENT}/behavior-packs`);
      expect(put?.body).toMatchObject({
        nav: { enabled: true, hub_label: "Home", hub_command: "go_home" },
        load: { enabled: false, lines: [] },
      });
    });
  });

  it("cannot save until something changed", async () => {
    mount();
    await screen.findByText("Hub button");
    expect(screen.getByRole("button", { name: "Save to agent" })).toBeDisabled();
  });

  it("reports a rejected write instead of pretending it landed", async () => {
    mount();
    await screen.findByText("Hub button");
    await toggle("Hub button");
    window.curie!.api.request = async () => ({
      status: 413,
      ok: false,
      body: undefined as never,
      error: "413 Payload Too Large",
    });
    await userEvent.click(screen.getByRole("button", { name: "Save to agent" }));
    expect(await screen.findByText("The write was rejected")).toBeInTheDocument();
    expect(screen.getByText("413 Payload Too Large")).toBeInTheDocument();
  });

  it("reverts to what the agent holds", async () => {
    mount();
    await screen.findByText("Hub button");
    await toggle("Hub button");
    await waitFor(() => expect(screen.getByText("unsaved")).toBeInTheDocument());
    await userEvent.click(screen.getByRole("button", { name: "Revert" }));
    await waitFor(() => expect(screen.queryByText("unsaved")).not.toBeInTheDocument());
  });
});

describe("drafting from the bundle", () => {
  it("turns the bundle's starter prompts into tips and writes a greeting", async () => {
    mount({
      name: "sre-bot",
      description: "Triages alerts and ranks cost leaks.",
      starterPrompts: ["Rank our cost leaks by dollars"],
    });
    await screen.findByText("Tips");
    await userEvent.click(screen.getByRole("button", { name: "Draft from this bundle" }));

    await waitFor(() =>
      expect(screen.getByDisplayValue("Rank our cost leaks by dollars")).toBeInTheDocument(),
    );
    // And what it drafted actually fires, rather than merely filling the form.
    expect(screen.queryByText(/Enabled with no reply/)).not.toBeInTheDocument();
    expect(screen.queryByText(/no trigger phrases/)).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Save to agent" }));
    await waitFor(() => {
      const put = calls.find((c) => c.method === "PUT");
      expect(put?.body).toMatchObject({ tips: { enabled: true, tips: ["Rank our cost leaks by dollars"] } });
    });
  });

  it("offers nothing to draft from when there is no manifest", async () => {
    mount();
    await screen.findByText("Tips");
    expect(screen.queryByRole("button", { name: "Draft from this bundle" })).not.toBeInTheDocument();
  });
});
