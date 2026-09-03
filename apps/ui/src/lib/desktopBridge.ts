// Is this console running inside the desktop shell?
//
// The browser can only ever show you a `curie` command to copy: it cannot spawn
// a process, and it cannot see Docker. The desktop shell loads this same app and
// injects `window.curie`, which can do both. So the difference between the web
// console and the desktop app stops being two codebases and becomes one
// capability check.
//
// What crosses the bridge is a STRUCTURE, never a command line. The shell
// resolves `{ action, positionals, flags }` to argv itself, against the CLI's own
// manifest, and spawns with `shell: false`. Handing it the rendered string would
// mean something a user typed could become a command, which is the one thing
// that must never be possible.

export interface Invocation {
  readonly action: string;
  readonly positionals?: readonly string[];
  readonly flags?: Readonly<Record<string, string | boolean>>;
}

interface DesktopBridge {
  readonly cli: {
    run(invocation: Invocation): Promise<{ runId: string }>;
  };
}

/** The shell, or `null` in a browser. Read through this rather than touching
 *  `window.curie`, so a view cannot accidentally assume it is there. */
export function desktopBridge(): DesktopBridge | null {
  const w = window as unknown as { curie?: unknown };
  const b = w.curie as DesktopBridge | undefined;
  return b && typeof b.cli?.run === "function" ? b : null;
}

export const inDesktop = (): boolean => desktopBridge() !== null;
