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
    dialog: { pick: async () => null, pathForFile: () => null },
    workspace: {
      list: async () => listed,
      open: async () => null,
      add: async () => null,
      forget: async () => {},
      delete: async (path: string) => {
      if (refuse) return { ok: false as const, error: refuse };
      deleted.push(path);
      listed = listed.filter((w) => w.path !== path);
      return { ok: true as const };
    },
      files: async () => [],
      readFile: async () => "",
      writeFile: async () => {},
      createAgent: async () => ({ ok: false as const, error: "stub" }),
      revealInFileManager: async () => {},
    },
    api: {
      connection: async () => ({ baseUrl: "", hasKey: false, reachable: false, checkedAt: 0 }),
      connect: async () => ({ baseUrl: "", hasKey: false, reachable: false, checkedAt: 0 }),
      signOut: async () => ({ baseUrl: "", hasKey: false, reachable: false, checkedAt: 0 }),
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

const deleted: string[] = [];
let refuse: string | null = null;

beforeEach(() => {
  deleted.length = 0;
  refuse = null;
  listed = [WEATHER, SRE];
  // The column's collapsed state is remembered across launches, so a test that
  // did not clear it would depend on the one before it.
  localStorage.clear();
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
    await waitFor(() => expect(screen.getByRole("button", { name: "New agent…" })).toBeInTheDocument());
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

  it("keeps the whole interface on a first run, and offers ONE way to create", async () => {
    // Both halves matter and the first attempt at this traded one for the
    // other. The five ways to start -- spread across the column's footer, the
    // detail's empty state and the scaffolding group, three of them running
    // `init` -- did need cutting to one. Replacing the entire Build interface
    // with a single first-run panel also cut it to one, and removed Build.
    listed = [];
    mount();
    await waitFor(() => expect(screen.getByText("No agents yet")).toBeInTheDocument());

    // The interface is still here: the agent column, the detail pane, and the
    // scaffolding group at the foot.
    expect(list()).toBeInTheDocument();
    expect(detail()).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "See one work, no setup" })).toBeInTheDocument();

    // Exactly one control that MAKES an agent, and it opens the gallery rather
    // than the scaffolder. The command-shaped path still exists at the foot of
    // the page -- that is the point of it being down there -- but it does not
    // compete for the same words.
    const creates = screen
      .getAllByRole("button")
      .map((b) => b.textContent?.trim() ?? "")
      .filter((t) => /^new agent/i.test(t));
    expect(creates).toEqual(["New agent…"]);

    // And exactly one way to bring in something that already exists.
    expect(screen.getAllByRole("button", { name: "Import…" })).toHaveLength(1);
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
    await waitFor(() => expect(screen.getByRole("button", { name: "New agent…" })).toBeInTheDocument());
    // Pinned in the footer, not pushed past the twenty-fourth row: they must be
    // outside the scrolling region but inside the container.
    const scroller = within(list()).getByText("agent-0").closest("div[style*='max-height']")!;
    const newAgent = screen.getByRole("button", { name: "New agent…" });
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

describe("deleting an agent", () => {
  it("does not delete on the first click, and needs the name typed", async () => {
    // A directory cannot be un-deleted, so this gate does more work than the
    // one on a destructive command -- a command can be re-run.
    mount();
    await waitFor(() => expect(within(list()).getByText("weather")).toBeInTheDocument());

    await userEvent.click(screen.getByRole("button", { name: "Actions for weather" }));
    await userEvent.click(await screen.findByRole("menuitem", { name: "Delete…" }));
    await waitFor(() => expect(screen.getByText("Delete this agent")).toBeInTheDocument());

    const confirm = screen.getByRole("button", { name: "Delete permanently" });
    expect(confirm).toBeDisabled();
    expect(deleted).toEqual([]);

    // The confirm field has focus, so you can type without clicking first.
    // `Sheet` used to focus its own panel unconditionally after mount, which
    // stole focus back out of the field the sheet exists to have you fill in.
    expect(within(screen.getByRole("dialog")).getByRole("textbox")).toHaveFocus();

    // A near miss is still a miss.
    const confirmField = within(screen.getByRole("dialog")).getByRole("textbox");
    await userEvent.type(confirmField, "weathe");
    expect(confirm).toBeDisabled();

    await userEvent.type(confirmField, "r");
    expect(confirm).toBeEnabled();
    await userEvent.click(confirm);
    await waitFor(() => expect(deleted).toEqual(["/w/weather"]));
  });

  it("shows the shell's refusal instead of pretending it worked", async () => {
    // The guards live in the shell because the argument is a path and the
    // renderer is untrusted. What comes back is a reason, and it has to be
    // readable -- silently leaving the row in place would look like a no-op.
    refuse = "That directory is a git repository. Delete it with your own tools, not from here.";
    mount();
    await waitFor(() => expect(within(list()).getByText("weather")).toBeInTheDocument());

    await userEvent.click(screen.getByRole("button", { name: "Actions for weather" }));
    await userEvent.click(await screen.findByRole("menuitem", { name: "Delete…" }));
    await userEvent.type(within(await screen.findByRole("dialog")).getByRole("textbox"), "weather");
    await userEvent.click(screen.getByRole("button", { name: "Delete permanently" }));

    await waitFor(() => expect(screen.getByText(/git repository/)).toBeInTheDocument());
    expect(screen.getByText("Delete this agent")).toBeInTheDocument();
  });
});

describe("putting the agent list away", () => {
  const toggle = () => screen.getByRole("button", { name: /(Show|Hide) the agent list/ });

  it("hides the column and gives its width back", async () => {
    mount();
    await waitFor(() => expect(within(list()).getByText("weather")).toBeInTheDocument());

    await userEvent.click(toggle());
    expect(screen.queryByText("Agents")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Actions for weather" })).not.toBeInTheDocument();
    // What is stored has to agree with what is on screen. Writing it from
    // inside the state updater did not: React invokes an updater more than
    // once, so the write ran an unknown number of times against an unknown
    // previous value and the two drifted apart.
    expect(localStorage.getItem("curie.ui.build.agents.collapsed")).toBe("true");
  });

  it("keeps the way back on screen while the list is gone", async () => {
    // The toggle lives in the DETAIL column's header for exactly this reason: on
    // the list's own header it would disappear with the panel it controls, and a
    // column with no way back is not collapsed, it is lost.
    mount();
    await waitFor(() => expect(within(list()).getByText("weather")).toBeInTheDocument());

    await userEvent.click(toggle());
    expect(toggle()).toHaveAccessibleName("Show the agent list");
    // And the count, which is the only thing left saying the list exists.
    expect(screen.getByText("2 in all")).toBeInTheDocument();

    await userEvent.click(toggle());
    expect(screen.getByText("Agents")).toBeInTheDocument();
  });

  it("stays where it was left", async () => {
    const first = mount();
    await waitFor(() => expect(within(list()).getByText("weather")).toBeInTheDocument());
    await userEvent.click(toggle());
    first.unmount();

    mount();
    await waitFor(() => expect(screen.getByText("No agent")).toBeInTheDocument());
    expect(screen.queryByText("Agents")).not.toBeInTheDocument();
  });
});
