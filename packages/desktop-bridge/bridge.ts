// The desktop bridge, declared once.
//
// The Electron shell loads `apps/ui` and injects `window.curie`, which lets the
// same console run a `curie` command and read Docker when it is hosted in the
// shell, and only offer to copy the command when it is in a browser.
//
// This file exists because the shell and the console are separate builds. If
// each declared the bridge for itself, the two declarations would drift, and
// nothing would catch it: the renderer's copy is a claim about what the other
// side provides, not a check. That is not hypothetical -- the abandoned desktop
// spike had a stale copy of the platform's own agent shape for weeks, said "no
// channel bound" about agents answering in Slack, and typechecked the whole
// time. One declaration, imported by both, is the only version of this that
// cannot rot.
//
// Types only, and no imports. The shell's preload compiles for Electron and the
// console's bundle compiles for a browser; anything else here would break one of
// them.

/** What the renderer asks for. A STRUCTURE, never a command line: the shell
 *  resolves this to argv against the CLI's own manifest and spawns with
 *  `shell: false`, so a value a user typed can never become a command. */
export interface Invocation {
  readonly action: string;
  readonly positionals?: readonly string[];
  readonly flags?: Readonly<Record<string, string | boolean | undefined>>;
  /** Omit it. The shell decides, because the right answer depends on the
   *  machine and a second client got it wrong when the renderer chose. */
  readonly cwd?: string;
  readonly json?: boolean;
}

/** What the shell resolved an invocation to, before running it. Shown verbatim
 *  so the console is never a black box wrapped around the CLI. */
export interface ResolvedCommand {
  readonly argv: readonly string[];
  readonly display: string;
  readonly cwd: string;
}

export type RunState = "pending" | "running" | "ok" | "failed" | "cancelled";

export interface RunHandle {
  readonly runId: string;
  readonly command: ResolvedCommand;
}

export interface RunChunk {
  readonly runId: string;
  readonly stream: "stdout" | "stderr";
  readonly text: string;
  readonly at: number;
}

export interface RunResult {
  readonly runId: string;
  readonly state: RunState;
  readonly exitCode: number | null;
  readonly durationMs: number;
  readonly result?: unknown;
  readonly jsonError?: string;
}

/** Unsubscribe. Every `on*` returns one; a view that forgets to call it leaks a
 *  listener per mount. */
export type Unsubscribe = () => void;

/** The part of `window.curie` the console uses. The shell exposes more; this is
 *  the contract, so anything not named here is not something the console may
 *  depend on. */
export interface DesktopBridge {
  readonly cli: {
    run(invocation: Invocation): Promise<RunHandle>;
    cancel(runId: string): Promise<void>;
    onChunk(cb: (chunk: RunChunk) => void): Unsubscribe;
    onResult(cb: (result: RunResult) => void): Unsubscribe;
  };
}

/**
 * The shell, or `null` in a browser.
 *
 * Fails closed: a partial or newer bridge reads as absent. The browser path is
 * always correct, so the cost of failing closed is a copy button where a run
 * button could have been, and the cost of failing open is a dead control.
 */
export function desktopBridge(): DesktopBridge | null {
  const injected = (globalThis as { curie?: unknown }).curie as DesktopBridge | undefined;
  if (!injected) return null;
  const cli = injected.cli as DesktopBridge["cli"] | undefined;
  const usable =
    typeof cli?.run === "function" &&
    typeof cli?.onChunk === "function" &&
    typeof cli?.onResult === "function";
  return usable ? injected : null;
}

export const inDesktop = (): boolean => desktopBridge() !== null;
