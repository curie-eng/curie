// Signing in from the app itself.
//
// ADR-0083 says the BROWSER must never hold the platform key. The desktop shell
// is not a browser: it holds the key already and runs `curie` as you, exactly
// like a terminal does. So where the shell is present the app can mint a code
// and spend it without anyone copying anything, and where it is not, a pasted
// code is still the only way in. These pin both halves, because the difference
// between them is a security boundary rather than a convenience.

import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { AppProvider } from "../bridge/app";
import type { CurieBridge } from "../bridge/bridge";
import { SignIn } from "./SignIn";

type Ran = { action: string; flags?: Record<string, unknown>; json?: boolean };
const ran: Ran[] = [];
const posted: unknown[] = [];
let mintedCode = "CODE-FROM-SHELL";
let exchangeOk = true;
let onResultCb: ((r: unknown) => void) | null = null;

function stub(cliPath: string | null): CurieBridge {
  return {
    env: async () => ({
      cliPath,
      cliVersion: null,
      sourceCheckout: false,
      repoRoot: null,
      dockerAvailable: false,
      kubectlAvailable: false,
      helmAvailable: false,
      platform: "darwin",
      defaultCwd: "",
      appVersion: "0",
      electronVersion: "",
      chromeVersion: "",
      drift: null,
    }),
    cli: {
      run: async (inv: { action: string; flags?: Record<string, unknown>; json?: boolean }) => {
        // Record the flags too: what the subject is bound to is the point of the
        // call, and a stub that drops it lets an unattributed mint pass.
        ran.push({ action: inv.action, flags: inv.flags, json: inv.json });
        // The shell answers asynchronously, like the real one.
        queueMicrotask(() =>
          onResultCb?.({ runId: "r1", state: "ok", exitCode: 0, durationMs: 1, result: { code: mintedCode } }),
        );
        return { runId: "r1", command: { argv: [], display: "", cwd: "/" } };
      },
      cancel: async () => {},
      write: async () => {},
      onChunk: () => () => {},
      onResult: (cb: (r: unknown) => void) => {
        onResultCb = cb;
        return () => { onResultCb = null; };
      },
    },
    resources: { start: async () => {}, stop: async () => {}, onFrame: () => () => {}, logs: async () => "" },
    dialog: { pick: async () => null, pathForFile: () => null },
    workspace: {
      list: async () => [], open: async () => null, add: async () => null, forget: async () => {},
      delete: async () => ({ ok: true as const }), createAgent: async () => ({ ok: true as const, path: "/x" }),
      files: async () => [], readFile: async () => "", writeFile: async () => {}, revealInFileManager: async () => {},
    },
    api: {
      connection: async () => ({ baseUrl: "http://localhost:28000", hasKey: false, reachable: true, checkedAt: 0 }),
      connect: async () => ({ baseUrl: "", hasKey: false, reachable: false, checkedAt: 0 }),
      signOut: async () => ({ baseUrl: "", hasKey: false, reachable: false, checkedAt: 0 }),
      request: async (req: { path: string; body?: unknown }) => {
        if (req.path === "/console/session") posted.push(req.body);
        return { status: exchangeOk ? 200 : 401, ok: exchangeOk, body: undefined as never };
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
  } as unknown as CurieBridge;
}

const mount = () =>
  render(
    <AppProvider>
      <SignIn onClose={() => {}} />
    </AppProvider>,
  );

beforeEach(() => {
  ran.length = 0;
  posted.length = 0;
  mintedCode = "CODE-FROM-SHELL";
  exchangeOk = true;
  onResultCb = null;
});
afterEach(() => { delete window.curie; vi.restoreAllMocks(); });

describe("signing in", () => {
  it("offers to do it for you when the app can run commands", async () => {
    window.curie = stub("/usr/local/bin/curie");
    mount();
    expect(await screen.findByRole("button", { name: "Sign me in" })).toBeInTheDocument();
  });

  it("does not offer that in a plain browser tab", async () => {
    // No CLI means no way to mint, and offering a button that cannot work is
    // worse than the instruction to go and run the command.
    window.curie = stub(null);
    mount();
    await screen.findByText(/paste/i);
    expect(screen.queryByRole("button", { name: "Sign me in" })).not.toBeInTheDocument();
  });

  it("will not mint until told who the session is for", async () => {
    // Minting binds the subject to the code, so there is no anonymous version of
    // this to fall back on: without one the button cannot do anything.
    window.curie = stub("/usr/local/bin/curie");
    mount();
    expect(await screen.findByRole("button", { name: "Sign me in" })).toBeDisabled();
    expect(ran).toHaveLength(0);
  });

  it("mints through the shell and spends the code, with no copying", async () => {
    window.curie = stub("/usr/local/bin/curie");
    mount();
    await userEvent.type(await screen.findByPlaceholderText(/Slack member id/i), "U0EXAMPLE1");
    await userEvent.click(await screen.findByRole("button", { name: "Sign me in" }));
    await waitFor(() => expect(posted).toHaveLength(1));
    // Asked the CLI for a machine-readable answer rather than scraping text, and
    // carried the subject through rather than minting an unattributed code.
    expect(ran).toEqual([
      { action: "local.console.login", flags: { subject: "U0EXAMPLE1" }, json: true },
    ]);
    expect(posted[0]).toEqual({ code: "CODE-FROM-SHELL" });
  });

  it("says so when the shell mints nothing rather than posting an empty code", async () => {
    window.curie = stub("/usr/local/bin/curie");
    mintedCode = "";
    mount();
    await userEvent.type(await screen.findByPlaceholderText(/Slack member id/i), "U0EXAMPLE1");
    await userEvent.click(await screen.findByRole("button", { name: "Sign me in" }));
    expect(await screen.findByText(/returned no code/i)).toBeInTheDocument();
    expect(posted).toHaveLength(0);
  });

  it("still takes a pasted code, which is the only way in a browser", async () => {
    window.curie = stub(null);
    mount();
    await userEvent.type(await screen.findByPlaceholderText("paste the code"), "TYPED-CODE");
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));
    await waitFor(() => expect(posted).toEqual([{ code: "TYPED-CODE" }]));
  });

  it("does not guess why a code was refused", async () => {
    // The API answers one indistinguishable 401 for wrong, consumed and expired,
    // so the message must not claim to know which.
    window.curie = stub(null);
    exchangeOk = false;
    mount();
    await userEvent.type(await screen.findByPlaceholderText("paste the code"), "NOPE");
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));
    expect(await screen.findByText(/not accepted/i)).toBeInTheDocument();
  });
});
