// Run the `curie` binary on behalf of the renderer and stream its output back.
//
// Two rules hold this together. First, nothing goes through a shell: argv is
// built from the manifest and handed to `spawn` directly, so a bundle path with
// a space in it is a path, not three arguments, and no value a user types can
// become a command. Second, the renderer sees the resolved command before it
// runs and the raw stream while it runs -- the desktop app wraps the CLI, it
// never hides it.

import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { execFile } from "node:child_process";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { randomUUID } from "node:crypto";
import type { BrowserWindow } from "electron";

import type {
  CliInvocation,
  ResolvedCommand,
  RunChunk,
  RunHandle,
  RunResult,
} from "../shared/contract.js";
import { CH } from "../shared/contract.js";
import { resolve as resolveInvocation } from "./manifest.js";
import { findRepoRoot } from "./repo.js";

const execFileAsync = promisify(execFile);

interface ActiveRun {
  child: ChildProcessWithoutNullStreams;
  startedAt: number;
  command: ResolvedCommand;
  json: boolean;
  stdout: string;
  cancelled: boolean;
}

const runs = new Map<string, ActiveRun>();

/** Where `curie` lives. Resolved once and cached; `PATH` under a GUI launch is
 *  not the login shell's `PATH`, which is the classic reason a desktop app
 *  "cannot find" a binary the user can run fine in a terminal. */
let cachedCliPath: string | null | undefined;

const EXTRA_PATH = [
  join(homedir(), ".cargo", "bin"),
  join(homedir(), ".local", "bin"),
  "/opt/homebrew/bin",
  "/usr/local/bin",
  "/usr/bin",
];

export function searchPath(): string {
  const current = (process.env.PATH ?? "").split(":").filter(Boolean);
  const merged = [...current];
  for (const dir of EXTRA_PATH) if (!merged.includes(dir)) merged.push(dir);
  return merged.join(":");
}

export function findCli(): string | null {
  if (cachedCliPath !== undefined) return cachedCliPath;
  const explicit = process.env.CURIE_CLI_PATH;
  if (explicit && existsSync(explicit)) {
    cachedCliPath = explicit;
    return cachedCliPath;
  }
  for (const dir of searchPath().split(":")) {
    const candidate = join(dir, "curie");
    if (existsSync(candidate)) {
      cachedCliPath = candidate;
      return cachedCliPath;
    }
  }
  cachedCliPath = null;
  return null;
}

export function resetCliCache(): void {
  cachedCliPath = undefined;
}

export function defaultCwd(): string {
  return process.env.CURIE_WORKSPACE ?? homedir();
}

/**
 * Where a command runs, decided here rather than by whoever is asking.
 *
 * The renderer used to compute this and pass it down, which meant the policy
 * lived in one client and any second client got it wrong -- and one did: the
 * browser console has no notion of a working directory, so every command it ran
 * landed in the home directory and `local status` could not find a compose file.
 *
 * Most of what an operator runs is repository-scoped: the dev stack's compose
 * file, the chart, the contract fixtures. So the checkout wins when there is
 * one, and a caller with a genuine reason to run somewhere else (a bundle
 * directory) still passes `cwd` explicitly and is honoured.
 */
export function cwdFor(explicit?: string | null): string {
  if (explicit) return explicit;
  // A configured workspace beats wherever the binary happened to be launched
  // from: one is a stated intent, the other is an accident of packaging. In dev
  // `process.cwd()` is the checkout, and in a packaged app it is not a
  // repository at all, so it is the last guess rather than the first.
  const configured = process.env.CURIE_WORKSPACE;
  return (
    (configured ? findRepoRoot(configured) : null) ??
    findRepoRoot(process.cwd()) ??
    defaultCwd()
  );
}

/** One-shot run collected into a string. Used for the shell's own probes
 *  (`curie --version`, `secrets list`), never for anything long-running. */
export async function runOnce(
  argv: string[],
  opts: { cwd?: string; timeoutMs?: number } = {},
): Promise<{ stdout: string; stderr: string; code: number }> {
  const cli = findCli();
  if (!cli) throw new Error("curie is not on PATH");
  try {
    const { stdout, stderr } = await execFileAsync(cli, argv, {
      cwd: opts.cwd ?? defaultCwd(),
      timeout: opts.timeoutMs ?? 20_000,
      env: { ...process.env, PATH: searchPath(), NO_COLOR: "1" },
      maxBuffer: 8 * 1024 * 1024,
    });
    return { stdout, stderr, code: 0 };
  } catch (err) {
    const e = err as { stdout?: string; stderr?: string; code?: number; message?: string };
    return { stdout: e.stdout ?? "", stderr: e.stderr ?? e.message ?? "", code: e.code ?? 1 };
  }
}

export function startRun(win: BrowserWindow, inv: CliInvocation): RunHandle {
  const cli = findCli();
  if (!cli) throw new Error("curie is not on PATH. Install it, then reopen this window.");

  const command = resolveInvocation(inv, cwdFor(inv.cwd));
  const runId = randomUUID();
  const startedAt = Date.now();

  const child = spawn(cli, [...command.argv], {
    cwd: command.cwd,
    env: {
      ...process.env,
      PATH: searchPath(),
      // The CLI colorizes for a TTY; we render the text ourselves, and ANSI
      // escapes in the transcript would just be noise to strip client-side.
      NO_COLOR: "1",
      CURIE_CLIENT: "desktop",
    },
    // No shell. argv is already structured; a shell here would be a way for a
    // typed value to become a command.
    shell: false,
  }) as ChildProcessWithoutNullStreams;

  const run: ActiveRun = { child, startedAt, command, json: !!inv.json, stdout: "", cancelled: false };
  runs.set(runId, run);

  const emit = (stream: "stdout" | "stderr", text: string) => {
    if (win.isDestroyed()) return;
    const chunk: RunChunk = { runId, stream, text, at: Date.now() - startedAt };
    win.webContents.send(CH.cliChunk, chunk);
  };

  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (text: string) => {
    // `--json` puts the payload on stdout and everything human on stderr, so
    // buffering stdout is enough to recover a structured result.
    if (run.json) run.stdout += text;
    emit("stdout", text);
  });
  child.stderr.on("data", (text: string) => emit("stderr", text));

  const finish = (code: number | null, error?: string) => {
    if (!runs.has(runId)) return;
    runs.delete(runId);
    let result: unknown;
    let jsonError: string | undefined;
    if (run.json && run.stdout.trim()) {
      try {
        result = JSON.parse(run.stdout);
      } catch (err) {
        jsonError = `stdout was not valid JSON: ${(err as Error).message}`;
      }
    }
    const payload: RunResult = {
      runId,
      state: run.cancelled ? "cancelled" : code === 0 ? "ok" : "failed",
      exitCode: code,
      durationMs: Date.now() - startedAt,
      result,
      jsonError: jsonError ?? error,
    };
    if (!win.isDestroyed()) win.webContents.send(CH.cliResult, payload);
  };

  child.on("error", (err) => {
    emit("stderr", `${err.message}\n`);
    finish(null, err.message);
  });
  child.on("close", (code) => finish(code));

  return { runId, command };
}

export function cancelRun(runId: string): void {
  const run = runs.get(runId);
  if (!run) return;
  run.cancelled = true;
  // SIGINT first: `local up`, `skill up` and the streaming commands treat it as
  // "wind down cleanly", which is what an operator pressing Stop means. SIGKILL
  // only if the process ignores it.
  run.child.kill("SIGINT");
  const child = run.child;
  setTimeout(() => {
    if (!child.killed) child.kill("SIGKILL");
  }, 4000);
}

export function writeToRun(runId: string, data: string): void {
  const run = runs.get(runId);
  if (!run) return;
  run.child.stdin.write(data);
}

export function cancelAll(): void {
  for (const runId of [...runs.keys()]) cancelRun(runId);
}

export function activeRunCount(): number {
  return runs.size;
}
