// The agent list on the Build tab.
//
// Switching used to be a chevron on the bundle's own name, which hid the set of
// agents behind a click on the one you had already picked. These assert the
// standing list instead: it says what exists, which one you are in, and how to
// add one, without being opened.

import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { AppProvider } from "../bridge/app";
import { ResourcesProvider } from "../bridge/resources";
import { RunsProvider } from "../bridge/runs";
import { Build } from "./Build";
import type { CurieBridge, Workspace } from "../bridge/bridge";

const WEATHER: Workspace = {
  path: "/w/weather",
  name: "weather",
  plugin: { name: "weather", version: "0.1.0", description: "The weather agent plugin." },
  skills: ["weather"],
  hasEvals: true,
  hasMcp: true,
  lastOpened: 2,
};
const SRE: Workspace = {
  path: "/w/sre-bot",
  name: "sre-bot",
  plugin: { name: "sre-bot" },
  skills: ["triage", "cost"],
  hasEvals: false,
  hasMcp: false,
  lastOpened: 1,
};

let listed: Workspace[] = [];

function stubShell(): CurieBridge {
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
    workspace: {
      list: async () => listed,
      open: async () => null,
      add: async () => null,
      forget: async () => {},
      files: async () => [],
      readFile: async () => "",
      writeFile: async () => {},
      revealInFileManager: async () => {},
    },
    api: {
      connection: async () => ({ baseUrl: "", hasKey: false, reachable: false, checkedAt: 0 }),
      connect: async () => ({ baseUrl: "", hasKey: false, reachable: false, checkedAt: 0 }),
      request: async () => ({ status: 0, ok: false, body: undefined as never }),
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

function mount() {
  return render(
    <AppProvider>
      <ResourcesProvider>
        <RunsProvider>
          <Build />
        </RunsProvider>
      </ResourcesProvider>
    </AppProvider>,
  );
}

/** The list column, found by its own heading. */
function list(): HTMLElement {
  return screen.getByText("Agents").closest("section")!;
}

/** The detail column beside it. Scoping matters: a list row and the header both
 *  carry the bundle path as a title, so an unscoped query matches twice. */
function detail(): HTMLElement {
  return list().parentElement!.lastElementChild as HTMLElement;
}

beforeEach(() => {
  listed = [WEATHER, SRE];
  window.curie = stubShell();
});

afterEach(() => {
  delete window.curie;
});

describe("the agent list", () => {
  it("shows every known agent without being opened", async () => {
    mount();
    await waitFor(() => expect(within(list()).getByText("weather")).toBeInTheDocument());
    expect(within(list()).getByText("sre-bot")).toBeInTheDocument();
  });

  it("summarises each one, so the list is worth reading", async () => {
    mount();
    await waitFor(() => expect(within(list()).getByText("weather")).toBeInTheDocument());
    // Singular vs plural, and evals only where they exist.
    expect(within(list()).getByText("1 skill · evals")).toBeInTheDocument();
    expect(within(list()).getByText("2 skills")).toBeInTheDocument();
  });

  it("offers a way to add one, and to import one that exists", async () => {
    mount();
    await waitFor(() => expect(screen.getByRole("button", { name: "New Agent…" })).toBeInTheDocument());
    expect(screen.getByRole("button", { name: "Import…" })).toBeInTheDocument();
  });

  it("no longer hides switching behind the bundle's own name", async () => {
    mount();
    await waitFor(() => expect(within(list()).getByText("weather")).toBeInTheDocument());
    expect(screen.queryByTitle("Switch bundle")).not.toBeInTheDocument();
  });

  it("is never a dead end while an agent exists", async () => {
    // AppProvider falls to the most recently opened bundle rather than sitting in
    // a "no bundle" limbo, so the detail pane always has something in it when the
    // list is non-empty. Asserted here because the list makes that visible.
    mount();
    await waitFor(() => expect(within(list()).getByText("weather")).toBeInTheDocument());
    expect(screen.queryByText("No bundle open")).not.toBeInTheDocument();
    expect(within(detail()).getByTitle("/w/weather")).toBeInTheDocument();
  });

  it("says so plainly when there are none at all", async () => {
    listed = [];
    mount();
    await waitFor(() => expect(within(list()).getByText("None yet.")).toBeInTheDocument());
  });

  it("switches the detail pane when another agent is picked", async () => {
    mount();
    // The header carries the open agent's path, which is the one thing that comes
    // from the workspace itself rather than from a manifest read off disk.
    await waitFor(() => expect(within(detail()).getByTitle("/w/weather")).toBeInTheDocument());

    await userEvent.click(within(list()).getByText("sre-bot"));
    await waitFor(() => expect(within(detail()).getByTitle("/w/sre-bot")).toBeInTheDocument());
    expect(within(detail()).queryByTitle("/w/weather")).not.toBeInTheDocument();
  });
});

describe("the list is a bounded container", () => {
  // The reason this exists: with two agents nothing tells you what happens at
  // twenty. The actions used to live outside the group, so a long list pushed
  // them away down the page and the column had no boundary at all.
  const many = Array.from({ length: 24 }, (_, i) => ({
    path: `/w/agent-${i}`,
    name: `agent-${i}`,
    plugin: { name: `agent-${i}` },
    skills: ["s"],
    hasEvals: false,
    hasMcp: false,
    lastOpened: 24 - i,
  }));

  it("scrolls the rows instead of growing the column", async () => {
    listed = many;
    mount();
    await waitFor(() => expect(within(list()).getByText("agent-0")).toBeInTheDocument());
    const scroller = within(list()).getByText("agent-0").closest("div[style*='max-height']")!;
    expect(scroller).toBeTruthy();
    const style = (scroller as HTMLElement).style;
    expect(style.overflowY).toBe("auto");
    expect(style.maxHeight).toBe("264px");
    // A flex child that will not shrink below its content never overflows.
    expect(style.minHeight).toBe("0px");
  });

  it("keeps the actions reachable however long the list is", async () => {
    listed = many;
    mount();
    await waitFor(() => expect(screen.getByRole("button", { name: "New Agent…" })).toBeInTheDocument());
    // Pinned in the footer, not pushed past the twenty-fourth row: they must be
    // outside the scrolling region but inside the container.
    const scroller = within(list()).getByText("agent-0").closest("div[style*='max-height']")!;
    const newAgent = screen.getByRole("button", { name: "New Agent…" });
    expect(scroller.contains(newAgent)).toBe(false);
    expect(list().contains(newAgent)).toBe(true);
  });

  it("puts the rows and the actions in one container", async () => {
    mount();
    await waitFor(() => expect(within(list()).getByText("weather")).toBeInTheDocument());
    const row = within(list()).getByText("weather");
    const importBtn = screen.getByRole("button", { name: "Import…" });
    // The nearest common ancestor is the panel; before, the buttons were a
    // sibling of it and the column had no outer edge.
    const panel = row.closest("div[style*='flex-direction: column']");
    expect(panel).toBeTruthy();
    expect(list().contains(row) && list().contains(importBtn)).toBe(true);
  });

  it("still reports the real count in the header", async () => {
    listed = many;
    mount();
    await waitFor(() => expect(within(list()).getByText("24")).toBeInTheDocument());
  });
});
