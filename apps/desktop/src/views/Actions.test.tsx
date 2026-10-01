// The contextual controls, end to end.
//
// `surfaces.test.ts` proves the map is complete and that every surface is
// mentioned by a view. What it cannot prove is that pressing one of these
// controls produces the *right command with the right values* -- and that is the
// entire benefit of placing a command in context rather than leaving it in the
// list. A "Memory" button on an agent's row that opens a blank form is no better
// than searching for `local memory`, so the tests here follow the whole path:
// press the control, read the command string it built, and check the argv it
// starts.

import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { ReactNode } from "react";

import { AppProvider, type AgentSummary } from "../bridge/app";
import { RunsProvider } from "../bridge/runs";
import { surfacesById } from "../lib/surfaces";
import { Actions, RunSheetHost } from "./Actions";
import { AgentSheet } from "./AgentSheet";
import type { CliInvocation, CurieBridge } from "../bridge/bridge";

const started: CliInvocation[] = [];

function stubShell(): CurieBridge {
  return {
    env: async () => ({
      cliPath: "/usr/local/bin/curie",
      cliVersion: "curie 0.1.0",
      sourceCheckout: true,
      repoRoot: null,
      dockerAvailable: true,
      kubectlAvailable: true,
      helmAvailable: true,
      platform: "darwin",
      defaultCwd: "/Users/dev",
      appVersion: "0.1.0",
      electronVersion: "34",
      chromeVersion: "132",
      drift: null,
    }),
    cli: {
      run: async (inv: CliInvocation) => {
        started.push(inv);
        return { runId: `run-${started.length}`, command: { argv: [], display: "", cwd: "/tmp" } };
      },
      cancel: async () => {},
      write: async () => {},
      onChunk: () => () => {},
      onResult: () => () => {},
    },
    resources: {
      start: async () => {},
      stop: async () => {},
      onFrame: () => () => {},
      logs: async () => "",
    },
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
      connection: async () => ({ baseUrl: "", hasKey: false, reachable: false, checkedAt: 0 }),
      connect: async () => ({ baseUrl: "", hasKey: false, reachable: false, checkedAt: 0 }),
      signOut: async () => ({ baseUrl: "", hasKey: false, reachable: false, checkedAt: 0 }),
      request: async () => ({ status: 0, ok: false, body: undefined as never }),
    },
    secrets: { list: async () => [], set: async () => {}, unset: async () => {} },
    graph: { load: async () => null, save: async () => {} },
    theme: {
      get: async () => ({
        preference: "system" as const,
        effective: "dark" as const,
        appearance: "dark" as const,
      }),
      set: async () => ({
        preference: "system" as const,
        effective: "dark" as const,
        appearance: "dark" as const,
      }),
      onChange: () => () => {},
    },
    shell: { openExternal: async () => {}, copy: async () => {} },
  };
}

const AGENT: AgentSummary = {
  id: "6f2d0c1a-0000-4000-8000-000000000001",
  name: "deal-desk",
  model: "claude-sonnet-4",
  approval_required_tools: ["Bash"],
  channels: [{ kind: "slack", address: "C123" }],
};

/** The sheet host has to be mounted too: a control's whole job is to open it. */
function mount(children: ReactNode) {
  return render(
    <AppProvider>
      <RunsProvider>
        {children}
        <RunSheetHost />
      </RunsProvider>
    </AppProvider>,
  );
}

beforeEach(() => {
  started.length = 0;
  localStorage.clear();
  window.curie = stubShell();
});

afterEach(() => {
  delete window.curie;
});

describe("a surface's controls", () => {
  it("renders one control per command, labelled the way the map says", () => {
    mount(<Actions surface={surfacesById.get("tiers.local")!} />);
    expect(screen.getByRole("button", { name: "Start it here" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Shut it all down" })).toBeInTheDocument();
  });

  it("names the command it will run, so a button never hides one", () => {
    mount(<Actions surface={surfacesById.get("tiers.local")!} />);
    expect(screen.getByRole("button", { name: "Start it here" })).toHaveAttribute(
      "title",
      expect.stringContaining("curie local up") as unknown as string,
    );
  });

  it("opens the command form in place rather than navigating away", async () => {
    const user = userEvent.setup();
    mount(<Actions surface={surfacesById.get("tiers.local")!} />);
    await user.click(screen.getByRole("button", { name: "Start it here" }));
    await waitFor(() =>
      expect(screen.getByTestId("command-preview")).toHaveTextContent("curie local up"),
    );
    // Still on the same screen: the group that opened the sheet is behind it.
    expect(screen.getByRole("button", { name: "Shut it all down" })).toBeInTheDocument();
  });
});

describe("an agent's own commands", () => {
  it("fills the agent in, so the form opens pointed at the row you pressed", async () => {
    const user = userEvent.setup();
    mount(<AgentSheet agent={AGENT} onClose={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Memory" }));
    await waitFor(() =>
      expect(screen.getByTestId("command-preview")).toHaveTextContent(
        "curie local memory deal-desk",
      ),
    );
  });

  it("runs that exact command, not just previews it", async () => {
    const user = userEvent.setup();
    mount(<AgentSheet agent={AGENT} onClose={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Memory" }));
    await user.click(await screen.findByRole("button", { name: "Run" }));
    await waitFor(() => expect(started).toHaveLength(1));
    expect(started[0].action).toBe("local.memory");
    expect(started[0].positionals).toEqual(["deal-desk"]);
  });

  it("shows one tier at a time, and switches every group together", async () => {
    const user = userEvent.setup();
    mount(<AgentSheet agent={AGENT} onClose={() => {}} />);

    // Local by default: "Kill" is `local kill`.
    expect(screen.getByRole("button", { name: "Kill" })).toHaveAttribute(
      "title",
      expect.stringContaining("curie local kill") as unknown as string,
    );

    await user.click(screen.getByRole("button", { name: "cluster" }));
    expect(screen.getByRole("button", { name: "Kill" })).toHaveAttribute(
      "title",
      expect.stringContaining("curie cluster kill") as unknown as string,
    );
    // One "Kill", not two: the other tier's half is not merely deselected.
    expect(screen.getAllByRole("button", { name: "Kill" })).toHaveLength(1);
  });

  it("still asks before a destructive command, arriving from a row", async () => {
    const user = userEvent.setup();
    mount(<AgentSheet agent={AGENT} onClose={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Delete" }));
    await user.click(await screen.findByRole("button", { name: "Review and run" }));
    // The confirm step is a type-the-word gate, and nothing has run yet.
    expect(await screen.findByRole("button", { name: "Run it" })).toBeDisabled();
    expect(started).toHaveLength(0);
  });
});
