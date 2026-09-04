// The form is where a mis-click becomes a real command against real
// infrastructure, so the behaviours worth testing are the guards: a destructive
// command must not run on one click, and when it does run it must carry the
// `--yes` the CLI would otherwise block on, because there is no TTY here to
// answer a prompt.

import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { AppProvider } from "../bridge/app";
import { RunsProvider } from "../bridge/runs";
import { commandsById } from "../lib/manifest";
import { CommandForm } from "./CommandForm";
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
      get: async () => ({ preference: "system" as const, effective: "dark" as const, appearance: "dark" as const }),
      set: async () => ({ preference: "system" as const, effective: "dark" as const, appearance: "dark" as const }),
      onChange: () => () => {},
    },
    shell: { openExternal: async () => {}, copy: async () => {} },
  };
}

function mount(id: string) {
  const cmd = commandsById.get(id)!;
  return render(
    <AppProvider>
      <RunsProvider>
        <CommandForm cmd={cmd} />
      </RunsProvider>
    </AppProvider>,
  );
}

beforeEach(() => {
  started.length = 0;
  window.curie = stubShell();
});

afterEach(() => {
  delete window.curie;
  vi.restoreAllMocks();
});

describe("safe commands", () => {
  it("shows the exact command that will run", async () => {
    mount("local.status");
    await waitFor(() =>
      expect(screen.getByTestId("command-preview")).toHaveTextContent("curie local status"),
    );
  });

  it("updates the preview as arguments are filled in", async () => {
    const user = userEvent.setup();
    mount("local.versions");
    await user.type(screen.getAllByRole("textbox")[0], "deal-desk");
    await waitFor(() =>
      expect(screen.getByTestId("command-preview")).toHaveTextContent(
        "curie local versions deal-desk",
      ),
    );
  });

  it("runs on one click", async () => {
    const user = userEvent.setup();
    mount("local.status");
    await user.click(screen.getByRole("button", { name: "Run" }));
    await waitFor(() => expect(started).toHaveLength(1));
    expect(started[0].action).toBe("local.status");
  });

  it("offers a dry run when the command supports one, and passes the flag", async () => {
    const user = userEvent.setup();
    mount("local.down");
    await user.click(screen.getByRole("button", { name: "Dry run" }));
    await waitFor(() => expect(started).toHaveLength(1));
    expect(started[0].flags?.["dry-run"]).toBe(true);
    // A dry run must not smuggle in the confirmation flag.
    expect(started[0].flags?.yes).toBeUndefined();
  });
});

describe("destructive commands", () => {
  it("does not run on the first click", async () => {
    const user = userEvent.setup();
    mount("local.down");
    await user.click(screen.getByRole("button", { name: "Review and run" }));
    expect(started).toHaveLength(0);
    expect(screen.getByText(/This changes or removes live state/)).toBeInTheDocument();
  });

  it("keeps the confirm disabled until the command name is typed", async () => {
    const user = userEvent.setup();
    mount("local.down");
    await user.click(screen.getByRole("button", { name: "Review and run" }));

    const dialog = within(screen.getByRole("dialog"));
    const confirm = dialog.getByRole("button", { name: "Run it" });
    expect(confirm).toBeDisabled();

    await user.type(dialog.getByRole("textbox"), "down");
    expect(confirm).toBeEnabled();
  });

  it("supplies --yes on confirm, because there is no TTY to answer a prompt", async () => {
    const user = userEvent.setup();
    mount("local.down");
    await user.click(screen.getByRole("button", { name: "Review and run" }));
    const dialog = within(screen.getByRole("dialog"));
    await user.type(dialog.getByRole("textbox"), "down");
    await user.click(dialog.getByRole("button", { name: "Run it" }));

    await waitFor(() => expect(started).toHaveLength(1));
    expect(started[0].flags?.yes).toBe(true);
  });
});

describe("required arguments", () => {
  it("blocks the run until a required positional is filled", async () => {
    const user = userEvent.setup();
    // `local versions <AGENT>` needs the agent name.
    mount("local.versions");
    const run = screen.getByRole("button", { name: "Run" });
    expect(run).toBeDisabled();

    await user.type(screen.getAllByRole("textbox")[0], "deal-desk");
    await waitFor(() => expect(run).toBeEnabled());
    await user.click(run);
    await waitFor(() => expect(started).toHaveLength(1));
    expect(started[0].positionals).toEqual(["deal-desk"]);
  });
});

describe("--json", () => {
  it("is off by default and threads through when enabled", async () => {
    const user = userEvent.setup();
    mount("local.status");
    // The --json control is a platform switch, not a checkbox.
    await user.click(screen.getByRole("switch"));
    await user.click(screen.getByRole("button", { name: "Run" }));
    await waitFor(() => expect(started).toHaveLength(1));
    expect(started[0].json).toBe(true);
  });
});

describe("the working directory", () => {
  // The bundle chosen in the sidebar sets `cwd` on every invocation this form
  // launches, so for a skill-tier command the directory is effectively an
  // argument. It used to be invisible, which made a global control look like it
  // belonged to the Build tab and made "the exact command that will run" less
  // than exact.
  it("names the directory the command will run in", async () => {
    mount("local.status");
    // No bundle open in this stub, so the shell's fallback is what runs.
    expect(await screen.findByText("/Users/dev")).toBeInTheDocument();
  });

  it("says WHICH directory it chose, not just the path", async () => {
    // The path alone does not tell you whether the app picked the bundle you
    // have open, your checkout, or a fallback -- and those produce different
    // results for the same command. This stub has neither a bundle nor a
    // checkout, so the honest answer is the default.
    mount("local.status");
    expect(await screen.findByText(/default directory/)).toBeInTheDocument();
  });

  it("prefers the checkout for a stack command when there is one", async () => {
    // A dev build resolves `compose.dev.yaml` against cwd, so a stack command
    // run from anywhere else fails on a missing file rather than on a wrong
    // directory. This is the assertion that keeps that from regressing.
    const shell = stubShell();
    window.curie = {
      ...shell,
      env: async () => ({ ...(await shell.env()), repoRoot: "/src/curie" }),
    };
    mount("local.status");
    expect(await screen.findByText("/src/curie")).toBeInTheDocument();
    expect(await screen.findByText(/source checkout/)).toBeInTheDocument();
  });

  it("prints no directory at all when the shell cannot say", async () => {
    // Never invent a path: one this app prints but does not use is worse than
    // printing none.
    const shell = stubShell();
    window.curie = {
      ...shell,
      env: async () => ({ ...(await shell.env()), defaultCwd: "" }),
    };
    mount("local.status");
    expect(await screen.findByText(/not known yet/)).toBeInTheDocument();
  });
});

describe("the form is an abstraction over the CLI, not a rendering of it", () => {
  // The whole reason this form exists is so nobody has to know the flags.
  // Labelling its controls `--file` and `--model` handed that back: it is the
  // CLI's own vocabulary, in a window, with extra steps.
  it("labels options in words, never as flag tokens", async () => {
    mount("local.up");
    await waitFor(() => expect(screen.getByTestId("command-preview")).toBeInTheDocument());
    await userEvent.click(screen.getByRole("button", { name: /All options/ }));

    const labels = [...document.querySelectorAll("label, div")]
      .filter((e) => e.children.length === 0)
      .map((e) => e.textContent?.trim() ?? "");
    expect(labels.filter((l) => l.startsWith("--"))).toEqual([]);
    expect(labels).toContain("File");
    expect(labels).toContain("Model");
  });

  it("gives a path a picker rather than asking for a typed absolute path", async () => {
    // The field was a bare text box, so supplying a compose file meant knowing
    // its absolute path and typing it correctly -- in a window that has a file
    // dialog sitting right there.
    mount("local.up");
    await waitFor(() => expect(screen.getByTestId("command-preview")).toBeInTheDocument());
    await userEvent.click(screen.getByRole("button", { name: /All options/ }));

    expect(screen.getAllByRole("button", { name: "Choose…" }).length).toBeGreaterThan(0);
    const box = screen
      .getAllByRole("textbox")
      .find((i) => (i as HTMLInputElement).placeholder.startsWith("Choose a file"));
    expect(box, "the file field should say it can be chosen or dropped").toBeTruthy();
  });

  it("puts a default in the box, not in a footnote beside the label", async () => {
    // The default used to be a chip next to the label, which was reported as
    // easy to miss. It belongs where the value will be.
    mount("local.deploy");
    await waitFor(() => expect(screen.getByTestId("command-preview")).toBeInTheDocument());
    await userEvent.click(screen.getByRole("button", { name: /All options/ }));

    // Every input, not just the textboxes: the API key is a password field and
    // is exactly the case where a default nobody notices costs a 401.
    const boxes = [...document.querySelectorAll("input")].filter((i) => i.type !== "checkbox");
    expect(boxes.map((i) => i.placeholder)).toEqual(
      expect.arrayContaining(["curie-dev-key", ".", "http://localhost:28000"]),
    );

    // And nothing TYPED. A default is what the CLI does when the flag is
    // absent, so seeding it as a value made the app restate every default
    // explicitly -- `curie local deploy --plugin-dir X --api-url Y --api-key Z`
    // for a command whose argv should have been `curie local deploy` -- and
    // overrode the CLI's own resolution with a value this app guessed.
    expect(boxes.map((i) => i.value)).toEqual(boxes.map(() => ""));
    expect(screen.getByTestId("command-preview")).toHaveTextContent("curie local deploy");

    // And no "default X" chip left behind saying the same thing twice.
    expect(screen.queryByText(/^default$/)).not.toBeInTheDocument();
  });
});

describe("controls stay where they are", () => {
  it("does not move a toggle out of the disclosure when it is switched on", async () => {
    // Which flags sit above "All options" used to be recomputed from the live
    // values, so using a control promoted it: switching `Minimal` on made it
    // "primary", it jumped out of the disclosure and up the form -- out from
    // under the cursor that had just pressed it -- and everything below moved
    // with it.
    mount("local.up");
    await waitFor(() => expect(screen.getByTestId("command-preview")).toBeInTheDocument());
    await userEvent.click(screen.getByRole("button", { name: /All options/ }));

    const order = () =>
      [...document.querySelectorAll("input, label")]
        .map((e) => e.textContent?.trim())
        .filter((t): t is string => !!t);

    const before = order();
    const minimal = screen
      .getAllByRole("switch")
      .find((sw) => sw.parentElement?.textContent?.includes("Minimal"));
    expect(minimal, "the Minimal switch should be under the disclosure").toBeTruthy();
    await userEvent.click(minimal!);

    // The flag is now set -- the preview proves the click landed...
    expect(screen.getByTestId("command-preview")).toHaveTextContent("--minimal");
    // ...and nothing reordered.
    expect(order()).toEqual(before);
  });
});
