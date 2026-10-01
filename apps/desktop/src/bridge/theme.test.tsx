// The theme plumbing.
//
// Every colour in the app is a `var(--x)` resolved by `styles.css`, and the only
// thing that selects between the two palettes is `data-theme` on <html>. So the
// whole of light mode rests on this attribute being written, and re-written when
// the OS flips. That is invisible in a screenshot of either theme on its own,
// which is what these cover.

import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { AppProvider, useApp } from "./app";
import type { CurieBridge, ThemePreference, ThemeState } from "./bridge";

let state: ThemeState = { preference: "system", effective: "dark", appearance: "dark" };
let listener: ((s: ThemeState) => void) | null = null;
const setCalls: ThemePreference[] = [];

function stubShell(): CurieBridge {
  return {
    env: async () => ({
      cliPath: null,
      cliVersion: null,
      sourceCheckout: false,
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
      run: async () => ({ runId: "r", command: { argv: [], display: "", cwd: "/tmp" } }),
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
      connection: async () => ({ baseUrl: "", hasKey: false, reachable: false, checkedAt: 0 }),
      connect: async () => ({ baseUrl: "", hasKey: false, reachable: false, checkedAt: 0 }),
      signOut: async () => ({ baseUrl: "", hasKey: false, reachable: false, checkedAt: 0 }),
      request: async () => ({ status: 0, ok: false, body: undefined as never }),
    },
    secrets: { list: async () => [], set: async () => {}, unset: async () => {} },
    graph: { load: async () => null, save: async () => {} },
    theme: {
      get: async () => state,
      set: async (preference: ThemePreference) => {
        setCalls.push(preference);
        // The shell resolves the preference; the renderer must not.
        const effective = preference === "system" ? "dark" : preference;
        state = { preference, effective, appearance: effective === "light" ? "light" : "dark" };
        return state;
      },
      onChange: (cb) => {
        listener = cb;
        return () => {
          listener = null;
        };
      },
    },
    shell: { openExternal: async () => {}, copy: async () => {} },
  };
}

/** Reads the context, so the assertions run against the real provider. */
function Probe() {
  const app = useApp();
  return (
    <div>
      <span data-testid="pref">{app.theme?.preference ?? "-"}</span>
      <button onClick={() => app.setTheme("light")}>go light</button>
    </div>
  );
}

beforeEach(() => {
  state = { preference: "system", effective: "dark", appearance: "dark" };
  listener = null;
  setCalls.length = 0;
  delete document.documentElement.dataset.theme;
  window.curie = stubShell();
});

afterEach(() => {
  delete window.curie;
  vi.restoreAllMocks();
});

describe("theme", () => {
  it("puts the effective theme on <html>, which is what the palette keys off", async () => {
    render(
      <AppProvider>
        <Probe />
      </AppProvider>,
    );
    await waitFor(() => expect(document.documentElement.dataset.theme).toBe("dark"));
  });

  it("keeps the preference and the effective theme apart", async () => {
    // "System" must stay selected in the control while resolving to a concrete
    // palette; collapsing the two is how a settings toggle starts lying.
    render(
      <AppProvider>
        <Probe />
      </AppProvider>,
    );
    await waitFor(() => expect(screen.getByTestId("pref").textContent).toBe("system"));
    expect(document.documentElement.dataset.theme).toBe("dark");
  });

  it("follows an explicit choice", async () => {
    render(
      <AppProvider>
        <Probe />
      </AppProvider>,
    );
    await waitFor(() => expect(document.documentElement.dataset.theme).toBe("dark"));
    await userEvent.click(screen.getByRole("button", { name: "go light" }));
    await waitFor(() => expect(document.documentElement.dataset.theme).toBe("light"));
    expect(setCalls).toEqual(["light"]);
    expect(screen.getByTestId("pref").textContent).toBe("light");
  });

  it("follows the OS while the preference is system", async () => {
    // The default preference, so most installs depend on this subscription.
    render(
      <AppProvider>
        <Probe />
      </AppProvider>,
    );
    await waitFor(() => expect(listener).not.toBeNull());
    listener!({ preference: "system", effective: "light", appearance: "light" });
    await waitFor(() => expect(document.documentElement.dataset.theme).toBe("light"));
    // Still "System" in the control: the OS changed, the choice did not.
    expect(screen.getByTestId("pref").textContent).toBe("system");
  });

  it("does not ask the shell for a theme it was not told to set", async () => {
    render(
      <AppProvider>
        <Probe />
      </AppProvider>,
    );
    await waitFor(() => expect(document.documentElement.dataset.theme).toBe("dark"));
    expect(setCalls).toEqual([]);
  });
});
