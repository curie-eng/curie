// The run registry: every `curie` invocation the app has started, with its live
// transcript.
//
// This is the app's memory of what it has done. It matters more than it sounds:
// the single biggest thing a GUI can take away from an operator is the terminal
// scrollback, so nothing here is ephemeral. A run's resolved command, its full
// interleaved output, its exit code and its duration stay available after it
// finishes, in a drawer reachable from every screen. The UI is a front end for
// the CLI, not a replacement that hides it.

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";

import { bridge } from "./bridge";
import type { CliInvocation, ResolvedCommand, RunChunk, RunResult, RunState } from "./bridge";

export interface TranscriptLine {
  readonly stream: "stdout" | "stderr";
  readonly text: string;
  readonly at: number;
}

export interface Run {
  readonly id: string;
  readonly action: string;
  readonly command: ResolvedCommand;
  readonly state: RunState;
  readonly startedAt: number;
  readonly endedAt?: number;
  readonly exitCode?: number | null;
  readonly durationMs?: number;
  readonly lines: readonly TranscriptLine[];
  /** Parsed `--json` payload, when the run asked for one. */
  readonly result?: unknown;
  readonly jsonError?: string;
}

interface RunsValue {
  readonly runs: readonly Run[];
  readonly active: readonly Run[];
  /** Start a run and return its id, or throw with a message worth showing. */
  start(inv: CliInvocation): Promise<string>;
  cancel(id: string): void;
  /** stdin, for the commands that interview you (`init`, `skill eval-init`). */
  send(id: string, data: string): void;
  clear(): void;
  get(id: string): Run | undefined;
  /** Which run the transcript drawer is showing, and whether it is open. */
  readonly focused: string | null;
  focus(id: string | null): void;
  /** Whether the console at the foot of the pane is expanded to show
   *  scrollback. It lives here rather than in the console because three other
   *  places open it: a run starting, the ⌘L focus shortcut, and History's
   *  "Open transcript". */
  /** Dismissed entirely, not merely collapsed. Cleared whenever a command
   *  starts: output with nowhere visible to land is worse than a panel someone
   *  asked to hide, and every other surface in the app starts runs too. */
  readonly consoleHidden: boolean;
  setConsoleHidden(hidden: boolean): void;
  readonly consoleOpen: boolean;
  setConsoleOpen(open: boolean): void;
}

const Ctx = createContext<RunsValue | null>(null);

/** Cap on retained transcript lines per run. A `cluster up --debug` can emit a
 *  lot; past this the head is dropped and the UI says so, which is better than
 *  the window going unresponsive halfway through a deploy. */
const MAX_LINES = 4000;

/** Split a chunk into lines without losing a partial final line: the CLI writes
 *  progress a character at a time, so chunk boundaries are not line boundaries. */
function appendChunk(lines: readonly TranscriptLine[], chunk: RunChunk): TranscriptLine[] {
  const parts = chunk.text.split("\n");
  const next = [...lines];
  const last = next[next.length - 1];
  // A chunk that continues the previous line (no newline yet) extends it in
  // place rather than starting a new row.
  if (last && !last.text.endsWith("\n") && last.stream === chunk.stream && parts.length) {
    next[next.length - 1] = { ...last, text: last.text + parts.shift()! };
  }
  for (const part of parts) {
    next.push({ stream: chunk.stream, text: part, at: chunk.at });
  }
  return next.length > MAX_LINES ? next.slice(next.length - MAX_LINES) : next;
}

export function RunsProvider({ children }: { children: ReactNode }) {
  const [runs, setRuns] = useState<Run[]>([]);
  const [focused, setFocused] = useState<string | null>(null);
  const [consoleOpen, setConsoleOpen] = useState(false);
  const [consoleHidden, setConsoleHidden] = useState(false);
  // Chunks can arrive before `start()`'s promise resolves and puts the run in
  // state, so early output is parked here and flushed when the run appears.
  const pending = useRef(new Map<string, RunChunk[]>());
  const actions = useRef(new Map<string, string>());

  useEffect(() => {
    const offChunk = bridge().cli.onChunk((chunk) => {
      setRuns((prev) => {
        const i = prev.findIndex((r) => r.id === chunk.runId);
        if (i < 0) {
          const parked = pending.current.get(chunk.runId) ?? [];
          parked.push(chunk);
          pending.current.set(chunk.runId, parked);
          return prev;
        }
        const next = [...prev];
        next[i] = { ...next[i], lines: appendChunk(next[i].lines, chunk) };
        return next;
      });
    });

    const offResult = bridge().cli.onResult((result: RunResult) => {
      setRuns((prev) => {
        const i = prev.findIndex((r) => r.id === result.runId);
        if (i < 0) return prev;
        const next = [...prev];
        next[i] = {
          ...next[i],
          state: result.state,
          exitCode: result.exitCode,
          durationMs: result.durationMs,
          endedAt: Date.now(),
          result: result.result,
          jsonError: result.jsonError,
        };
        return next;
      });
    });

    return () => {
      offChunk();
      offResult();
    };
  }, []);

  const start = useCallback(async (inv: CliInvocation) => {
    const handle = await bridge().cli.run(inv);
    const parked = pending.current.get(handle.runId) ?? [];
    pending.current.delete(handle.runId);
    actions.current.set(handle.runId, inv.action);
    const run: Run = {
      id: handle.runId,
      action: inv.action,
      command: handle.command,
      state: "running",
      startedAt: Date.now(),
      lines: parked.reduce<TranscriptLine[]>((acc, c) => appendChunk(acc, c), []),
    };
    setRuns((prev) => [run, ...prev].slice(0, 200));
    setFocused(handle.runId);
    setConsoleOpen(true);
    setConsoleHidden(false);
    return handle.runId;
  }, []);

  const cancel = useCallback((id: string) => void bridge().cli.cancel(id), []);
  const send = useCallback((id: string, data: string) => void bridge().cli.write(id, data), []);

  const clear = useCallback(() => {
    // Only finished runs: clearing while something is mid-deploy would drop the
    // only place its output is visible.
    setRuns((prev) => prev.filter((r) => r.state === "running" || r.state === "pending"));
  }, []);

  const value = useMemo<RunsValue>(
    () => ({
      runs,
      active: runs.filter((r) => r.state === "running" || r.state === "pending"),
      start,
      cancel,
      send,
      clear,
      get: (id: string) => runs.find((r) => r.id === id),
      focused,
      focus: setFocused,
      consoleOpen,
      setConsoleOpen,
      consoleHidden,
      setConsoleHidden,
    }),
    [runs, start, cancel, send, clear, focused, consoleOpen, consoleHidden],
  );

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useRuns(): RunsValue {
  const value = useContext(Ctx);
  if (!value) throw new Error("useRuns must be used inside <RunsProvider>");
  return value;
}

/** Flatten a run's transcript back to the text a terminal would have shown, for
 *  the "Copy output" affordance. */
export function transcriptText(run: Run): string {
  return run.lines.map((l) => l.text).join("\n");
}
