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
const SECOND_AGENT = "22222222-2222-3333-4444-555555555555";

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
      defaultCwd: "/Users/dev",
      appVersion: "0.1.0",
      electronVersion: "34",
      chromeVersion: "132",
      drift: null,
    }),
    cli: {
      run: async () => ({ runId: "r1", command: { argv: [], display: "", cwd: "/tmp" } }),
      cancel: async () => {},
      write: async () => {},
      onChunk: () => () => {},
      onResult: () => () => {},
    },
    resources: { start: async () => {}, stop: async () => {}, onFrame: () => () => {}, logs: async () => "" },
    dialog: { pick: async () => null, pathForFile: () => null },
    workspace: {
      list: async () => [],
      open: async () => null,
      add: async () => null,
      forget: async () => {},
      delete: async () => ({ ok: true as const }),
      createAgent: async () => ({ ok: false as const, error: "stub" }),
      files: async () => [],
      readFile: async () => "",
      writeFile: async () => {},
      revealInFileManager: async () => {},
    },
    api: {
      connection,
      connect: connection,
      signOut: connection,
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
    theme: {
      get: async () => ({ preference: "system" as const, effective: "dark" as const, appearance: "dark" as const }),
      set: async () => ({ preference: "system" as const, effective: "dark" as const, appearance: "dark" as const }),
      onChange: () => () => {},
    },
    shell: { openExternal: async () => {}, copy: async () => {} },
  };
}

/** Open an agent from the list, the way an operator reaches the editor. */
async function openAgent(name = "sre-bot") {
  await userEvent.click(await screen.findByText(name));
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

/** Mount and walk straight into the agent, for the editor's own behaviours. */
async function mountOpen(plugin?: Parameters<typeof SlackPacks>[0]["plugin"]) {
  const r = mount(plugin);
  await openAgent();
  return r;
}

beforeEach(() => {
  // The screen remembers the operator's place in localStorage, so a leaked
  // cursor would make these tests depend on the order they ran in.
  localStorage.clear();
  calls = [];
  reachable = true;
  stored = null;
  // Two by default: a one-agent fixture cannot tell "lists the agents" apart
  // from "opens the only agent", which is the distinction this screen turns on.
  agents = [
    { id: AGENT, name: "sre-bot", channel: { kind: "slack" } },
    { id: SECOND_AGENT, name: "deal-desk", channel: { kind: "slack" } },
  ];
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

describe("the agent list is the way in", () => {
  it("lists the agents rather than opening one, even when there is exactly one", async () => {
    // The point of the list. Landing straight in a single agent's editor reads as
    // "this is THE agent" and hides that the screen is per-agent at all.
    agents = [{ id: AGENT, name: "sre-bot", channel: { kind: "slack" } }];
    mount();
    expect(await screen.findByText("sre-bot")).toBeInTheDocument();
    // The editor's own furniture must not be on screen yet.
    expect(screen.queryByText("Load lines")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Save to agent" })).not.toBeInTheDocument();
  });

  it("shows every agent, not just the first", async () => {
    mount();
    expect(await screen.findByText("sre-bot")).toBeInTheDocument();
    expect(screen.getByText("deal-desk")).toBeInTheDocument();
  });

  it("reads each agent's packs so the row can say what state it is in", async () => {
    stored = { load: { enabled: true, lines: ["working"] }, tips: { enabled: true, tips: ["a tip"] } };
    mount();
    await waitFor(() => expect(screen.getAllByText(`2 of ${6} on`).length).toBe(2));
    // One read per agent, and none of them a write.
    expect(calls.filter((c) => c.path.endsWith("/behavior-packs"))).toHaveLength(2);
    expect(calls.every((c) => c.method === "GET")).toBe(true);
  });

  it("says a pack is on but unusable without making you open it", async () => {
    stored = { greeting: { enabled: true, phrases: ["hi"], reply: "" } };
    mount();
    expect((await screen.findAllByText("1 will not fire")).length).toBeGreaterThan(0);
  });

  it("distinguishes no packs from packs it could not read", async () => {
    mount();
    expect((await screen.findAllByText("no packs")).length).toBe(2);

    // A failing read must not render as "off": that would be a lie about the
    // agent's configuration, which is the one thing this list is for.
    calls = [];
    const shell = stubShell();
    window.curie = {
      ...shell,
      api: {
        ...shell.api,
        request: async (req: ApiRequest) => {
          if (req.path === "/agents") return { status: 200, ok: true, body: agents as never };
          return { status: 500, ok: false, body: undefined as never, error: "500" };
        },
      },
    };
    mount();
    expect((await screen.findAllByText("unreadable")).length).toBe(2);
  });

  it("flags an agent with no surface bound", async () => {
    agents = [{ id: AGENT, name: "sre-bot", channel: null }];
    mount();
    expect(await screen.findByText("no surface")).toBeInTheDocument();
  });

  it("opens the agent you click, and comes back to the list", async () => {
    mount();
    await openAgent("deal-desk");
    expect(await screen.findByText("Load lines")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: /All agents/ }));
    expect(await screen.findByText("sre-bot")).toBeInTheDocument();
    expect(screen.queryByText("Load lines")).not.toBeInTheDocument();
  });

  it("reads the packs of the agent you opened, not of the first one", async () => {
    mount();
    await openAgent("deal-desk");
    await waitFor(() =>
      expect(calls.some((c) => c.path === `/agents/${SECOND_AGENT}/behavior-packs`)).toBe(true),
    );
  });
});

describe("remembering where the operator was", () => {
  it("returns to the agent they had open", async () => {
    const first = mount();
    await openAgent();
    await screen.findByText("Load lines");
    first.unmount();

    // Remounting is what leaving the tab and coming back does.
    mount();
    expect(await screen.findByText("Load lines")).toBeInTheDocument();
  });

  it("returns to the list when that is where they left off", async () => {
    const first = mount();
    await openAgent();
    await userEvent.click(await screen.findByRole("button", { name: /All agents/ }));
    first.unmount();

    mount();
    expect(await screen.findByText("sre-bot")).toBeInTheDocument();
    expect(screen.queryByText("Load lines")).not.toBeInTheDocument();
  });

  it("shows the list when the remembered agent is gone", async () => {
    const first = mount();
    await openAgent();
    await screen.findByText("Load lines");
    first.unmount();

    // The agent was deleted while the app was elsewhere. Restoring a cursor that
    // points at nothing would be worse than forgetting it.
    agents = [{ id: "99999999-2222-3333-4444-555555555555", name: "other-bot", channel: { kind: "slack" } }];
    mount();
    expect(await screen.findByText("other-bot")).toBeInTheDocument();
    expect(screen.queryByText("Load lines")).not.toBeInTheDocument();
  });

  it("does not fall over when the browser denies localStorage", async () => {
    // Private-mode Safari and a locked-down profile both throw on access. A
    // cursor is a convenience; it must never take the screen down with it.
    const real = globalThis.localStorage;
    Object.defineProperty(globalThis, "localStorage", {
      configurable: true,
      get() {
        throw new Error("denied");
      },
    });
    try {
      mount();
      expect(await screen.findByText("sre-bot")).toBeInTheDocument();
      await openAgent();
      expect(await screen.findByText("Load lines")).toBeInTheDocument();
    } finally {
      Object.defineProperty(globalThis, "localStorage", { configurable: true, value: real });
    }
  });
});

describe("opening an agent", () => {
  it("reads that agent's packs and offers all six", async () => {
    await mountOpen();
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
    await mountOpen();
    expect(await screen.findByText("all packs off")).toBeInTheDocument();
  });

  it("marks the settings pack as having no runtime, because it has none", async () => {
    await mountOpen();
    expect(await screen.findByText("no runtime yet")).toBeInTheDocument();
  });

  it("warns when the agent has no surface bound", async () => {
    agents = [{ id: AGENT, name: "sre-bot", channel: null }];
    await mountOpen();
    expect(await screen.findByText(/no surface bound/)).toBeInTheDocument();
  });
});

describe("the ways a pack is silently inert", () => {
  it("names an enabled greeting with no reply, and marks the card", async () => {
    await mountOpen();
    await screen.findByText("Greeting");
    await toggle("Greeting");

    // The platform's own short circuit: no reply means the matcher returns before
    // it looks at the phrases, so the pack is on and dead. Matched on the issue's
    // own wording -- the field hint mentions "never fires" too.
    expect(await screen.findByText(/Enabled with no reply/)).toBeInTheDocument();
    expect(within(card("Greeting")).getByText("does nothing")).toBeInTheDocument();
  });

  it("stops saying it once the pack is usable", async () => {
    await mountOpen();
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
    await mountOpen();
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
    await mountOpen();
    await screen.findByDisplayValue("hey there team");
    await userEvent.clear(screen.getByDisplayValue("hey there team"));
    await userEvent.type(screen.getByPlaceholderText("hi"), "hi show me the report");
    expect(await screen.findByText(/reaches the model as a normal turn/)).toBeInTheDocument();
  });

  it("shows the caption the load pack produces", async () => {
    await mountOpen();
    await waitFor(() => expect(screen.getAllByText("is triaging").length).toBeGreaterThan(0));
  });

  it("shows the platform default when no caption pack is on", async () => {
    stored = null;
    await mountOpen();
    expect(await screen.findByText(/That is the platform default/)).toBeInTheDocument();
    expect(screen.getAllByText("is working on your request...").length).toBe(3);
  });
});

describe("saving", () => {
  it("writes the draft to that agent and nothing else", async () => {
    await mountOpen();
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
    await mountOpen();
    await screen.findByText("Hub button");
    expect(screen.getByRole("button", { name: "Save to agent" })).toBeDisabled();
  });

  it("reports a rejected write instead of pretending it landed", async () => {
    await mountOpen();
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
    await mountOpen();
    await screen.findByText("Hub button");
    await toggle("Hub button");
    await waitFor(() => expect(screen.getByText("unsaved")).toBeInTheDocument());
    await userEvent.click(screen.getByRole("button", { name: "Revert" }));
    await waitFor(() => expect(screen.queryByText("unsaved")).not.toBeInTheDocument());
  });
});

describe("drafting from the bundle", () => {
  it("turns the bundle's starter prompts into tips and writes a greeting", async () => {
    await mountOpen({
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
    await mountOpen();
    await screen.findByText("Tips");
    expect(screen.queryByRole("button", { name: "Draft from this bundle" })).not.toBeInTheDocument();
  });
});
