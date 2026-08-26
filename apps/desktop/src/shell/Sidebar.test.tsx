// The machine-status block, which is the app's answer to "can this machine
// actually do anything".
//
// It used to be four coloured dots. Four dots are the same picture whether you
// read them or not: when every tool is present they are four identical marks
// carrying nothing, and the case that matters looks like the others in a
// different hue. The rule now is that only absence gets ink, so this asserts the
// rule rather than the pixels -- a present tool is plain, a missing one is struck
// through, and the state is never carried by colour alone.

import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import { AppProvider } from "../bridge/app";
import { ResourcesProvider } from "../bridge/resources";
import { RunsProvider } from "../bridge/runs";
import { Sidebar } from "./Sidebar";
import type { CurieBridge, ShellEnvironment } from "../bridge/bridge";

function stubShell(env: Partial<ShellEnvironment>): CurieBridge {
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
      ...env,
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

function mount(env: Partial<ShellEnvironment> = {}) {
  window.curie = stubShell(env);
  return render(
    <AppProvider>
      <ResourcesProvider>
        <RunsProvider>
          <Sidebar />
        </RunsProvider>
      </ResourcesProvider>
    </AppProvider>,
  );
}

/** The tool's own label, found by the tooltip the block gives it. */
function tool(name: string): HTMLElement {
  return screen
    .getAllByTitle(new RegExp(`^${name}:`))
    .find((e) => e.textContent?.trim() === name)!;
}

afterEach(() => {
  delete window.curie;
});

describe("machine status", () => {
  it("leaves a present tool unmarked", async () => {
    mount();
    await waitFor(() => expect(tool("docker")).toBeInTheDocument());
    expect(tool("docker").style.textDecoration).toBe("");
  });

  it("strikes through a missing tool", async () => {
    mount({ helmAvailable: false });
    await waitFor(() => expect(tool("helm")).toBeInTheDocument());
    expect(tool("helm").style.textDecoration).toContain("line-through");
  });

  it("marks only what is actually missing", async () => {
    mount({ dockerAvailable: false });
    await waitFor(() => expect(tool("docker")).toBeInTheDocument());
    expect(tool("docker").style.textDecoration).toContain("line-through");
    for (const ok of ["curie", "kubectl", "helm"]) {
      expect(tool(ok).style.textDecoration, `${ok} is present and must stay unmarked`).toBe("");
    }
  });

  it("does not encode the state in colour alone", async () => {
    // The point of the redesign: a colourblind reader, or a screenshot in
    // greyscale, must still be able to see which tool is missing.
    mount({ helmAvailable: false });
    await waitFor(() => expect(tool("helm")).toBeInTheDocument());
    const missing = tool("helm");
    const present = tool("curie");
    expect(missing.style.textDecoration).not.toBe(present.style.textDecoration);
  });

  it("still says what is wrong on hover", async () => {
    mount({ helmAvailable: false });
    await waitFor(() => expect(tool("helm")).toBeInTheDocument());
    expect(tool("helm").title).toMatch(/cluster up cannot run/);
  });
});
