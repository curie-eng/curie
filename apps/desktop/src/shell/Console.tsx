// The console: type a curie command, watch it run, read the scrollback.
//
// This replaced a "Run a command" button that opened a searchable palette. The
// button was fine at finding a command you could not name and bad at everything
// else: it could not be typed into from muscle memory, it showed one command at
// a time, and the output went somewhere else. An operator console should have a
// prompt.
//
// It is NOT a terminal, and the difference is the app's central invariant rather
// than a shortcut taken. `CLAUDE.md`: nothing goes through a shell, and a value a
// user types must never be able to become a command. So this does not execute
// text. `parseCommand` turns text into `{ action, positionals, flags }` where the
// action must name a command the manifest declares, that struct crosses the same
// IPC call every button in the app uses, and the main process resolves argv
// independently and rejects what it does not recognise. A parse bug fails closed.
//
// What an operator gives up is shell syntax -- pipes, redirects, globs,
// substitution -- and the console says so when it sees any, rather than dropping
// it silently. What they get back is everything a terminal gives that a palette
// did not: history, completion, scrollback, and output in the same place as the
// prompt.
//
// The prompt has two modes because the CLI has two kinds of command. Normally it
// parses. While a run is waiting on stdin -- `init` and `skill eval-init`
// interview you -- it forwards the line verbatim to that process instead, which
// is the only way those are answerable in a window with no TTY.

import { useCallback, useEffect, useRef, useState } from "react";
import type { KeyboardEvent as ReactKeyboardEvent, RefObject } from "react";

import { useApp } from "../bridge/app";
import { transcriptText, useRuns, type Run } from "../bridge/runs";
import { complete, parseCommand } from "../lib/parseCommand";
import { duration } from "../lib/format";
import { ACCENT, F, FONT, LINE, R, S, STATUS, T } from "../tokens";
import { Button, CopyButton, Dot, Kbd, Mono } from "../primitives";

/** A line the console itself wrote, as opposed to one a process wrote. */
interface Note {
  readonly id: number;
  readonly kind: "input" | "error" | "info";
  readonly text: string;
}

/** Monotonic key for console lines. A module counter rather than a ref, because
 *  a ref read during render is what the React compiler refuses to memoize. */
let nextNoteId = 1;

const HISTORY_KEY = "curie.desktop.consoleHistory";
const MAX_HISTORY = 100;

function loadHistory(): string[] {
  try {
    const raw = JSON.parse(localStorage.getItem(HISTORY_KEY) ?? "[]");
    return Array.isArray(raw) ? raw.filter((x) => typeof x === "string").slice(0, MAX_HISTORY) : [];
  } catch {
    return [];
  }
}

export function Console() {
  const app = useApp();
  const runs = useRuns();

  const [text, setText] = useState("");
  const [notes, setNotes] = useState<readonly Note[]>([]);
  const [history, setHistory] = useState<readonly string[]>(loadHistory);
  const [histIndex, setHistIndex] = useState(-1);
  const [hints, setHints] = useState<readonly string[]>([]);
  // A destructive command types its own name back to run, the same gate the
  // form uses. Nothing about arriving by keyboard makes a teardown safer.
  const [confirming, setConfirming] = useState<{ text: string; word: string } | null>(null);

  const inputRef = useRef<HTMLInputElement>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  const cwd = app.workspace?.path;
  const focused = runs.focused ? runs.get(runs.focused) : undefined;
  const active = focused?.state === "running" || focused?.state === "pending";
  const expanded = runs.consoleOpen;

  const say = useCallback((kind: Note["kind"], text: string) => {
    setNotes((prev) => [...prev, { id: nextNoteId++, kind, text }].slice(-200));
  }, []);

  // Scrollback follows the tail, which is what a console is for. Reading back
  // is the History pane's job, and it has search and filters.
  useEffect(() => {
    const el = scrollRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [notes, focused?.lines.length, expanded]);

  const remember = useCallback((line: string) => {
    setHistory((prev) => {
      const next = [line, ...prev.filter((p) => p !== line)].slice(0, MAX_HISTORY);
      try {
        localStorage.setItem(HISTORY_KEY, JSON.stringify(next));
      } catch {
        // A full or disabled localStorage must not cost you the command you
        // just ran.
      }
      return next;
    });
    setHistIndex(-1);
  }, []);

  const launch = useCallback(
    async (line: string, extraFlags: Record<string, string | boolean> = {}) => {
      const parsed = parseCommand(line);
      if (!parsed.ok) {
        say("error", parsed.error + (parsed.suggestion ? `  Did you mean: ${parsed.suggestion}` : ""));
        return;
      }
      if (parsed.missing.length) {
        say(
          "error",
          `\`curie ${parsed.cmd.path.join(" ")}\` still needs ${parsed.missing
            .map((m) => `<${m.toUpperCase()}>`)
            .join(", ")}.`,
        );
        return;
      }
      try {
        await runs.start({
          action: parsed.cmd.id,
          positionals: parsed.positionals.map((p) => p.trim()),
          flags: { ...parsed.flags, ...extraFlags },
          cwd,
          json: parsed.json,
        });
      } catch (err) {
        say("error", (err as Error).message);
      }
    },
    [cwd, runs, say],
  );

  const submit = useCallback(() => {
    const line = text;
    if (!line.trim()) return;

    // Answering an interview, not naming a command.
    if (active && focused) {
      runs.send(focused.id, `${line}\n`);
      say("input", `› ${line}`);
      setText("");
      return;
    }

    if (confirming) {
      if (line.trim() !== confirming.word) {
        say("error", `Type \`${confirming.word}\` to confirm, or Esc to cancel.`);
        setText("");
        return;
      }
      const pending = confirming;
      setConfirming(null);
      setText("");
      const cmd = parseCommand(pending.text);
      // The CLI would prompt on a TTY and there is none here, so the app's own
      // confirm step is what supplies `--yes` -- exactly as CommandForm does.
      const yes: Record<string, string | boolean> =
        cmd.ok && cmd.cmd.flags.some((f) => f.long === "yes") ? { yes: true } : {};
      void launch(pending.text, yes);
      return;
    }

    setText("");
    setHints([]);
    remember(line);
    say("input", `curie ${line.replace(/^curie\s+/, "")}`);

    const parsed = parseCommand(line);
    if (parsed.ok && parsed.cmd.risk === "destructive" && !parsed.missing.length) {
      setConfirming({ text: line, word: parsed.cmd.name });
      say(
        "info",
        `\`curie ${parsed.cmd.path.join(" ")}\` changes or removes live state. Type \`${parsed.cmd.name}\` to run it.`,
      );
      return;
    }
    void launch(line);
  }, [active, confirming, focused, launch, remember, runs, say, text]);

  const onKey = (e: ReactKeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      e.preventDefault();
      return submit();
    }
    if (e.key === "Escape") {
      if (confirming) {
        setConfirming(null);
        say("info", "Cancelled.");
      }
      setHints([]);
      return;
    }
    if (e.key === "Tab") {
      e.preventDefault();
      const options = complete(text);
      if (!options.length) return;
      if (options.length === 1) {
        const only = options[0];
        setText(only.startsWith("--") ? text.replace(/--\S*$/, only) + " " : `${only} `);
        setHints([]);
        return;
      }
      setHints(options);
      return;
    }
    if (e.key === "ArrowUp" && !active) {
      e.preventDefault();
      const i = Math.min(history.length - 1, histIndex + 1);
      if (i >= 0) {
        setHistIndex(i);
        setText(history[i]);
      }
      return;
    }
    if (e.key === "ArrowDown" && !active) {
      e.preventDefault();
      const i = histIndex - 1;
      setHistIndex(i);
      setText(i < 0 ? "" : history[i]);
    }
  };

  return (
    <div
      style={{
        flex: "none",
        // An inset panel, not a full-bleed band. Reaching the sidebar seam meant
        // matching the pane's fade there, and a fading terminal edge reads as a
        // rendering fault rather than as softness -- a terminal is an object with
        // a boundary. Insetting it sidesteps the seam entirely: nothing to blend
        // into, because it no longer touches.
        margin: "0 14px 14px",
        borderRadius: R.group,
        // Deliberately thicker than a hairline. This is the one surface that is
        // a container for something else's output rather than a card of its own
        // content, and the outline is what says where that container ends.
        border: `2px solid ${LINE.strong}`,
        overflow: "hidden",
        background: S.well,
        display: "flex",
        flexDirection: "column",
        // Bounded: the console is a strip you type into, and the pane above is
        // the app. Expanded it takes a third of the window, never more.
        maxHeight: expanded ? "38vh" : undefined,
      }}
    >
      <Header run={focused} expanded={expanded} onToggle={() => runs.setConsoleOpen(!expanded)} />

      {expanded ? (
        <div
          ref={scrollRef}
          data-selectable
          style={{ overflow: "auto", padding: "8px 12px", flex: 1, minHeight: 0 }}
        >
          <Scrollback notes={notes} run={focused} />
        </div>
      ) : null}

      {hints.length ? <Hints hints={hints} /> : null}

      <Prompt
        inputRef={inputRef}
        text={text}
        setText={setText}
        onKey={onKey}
        mode={active ? "stdin" : confirming ? "confirm" : "command"}
        confirmWord={confirming?.word}
        onFocus={() => {
          if (!expanded) runs.setConsoleOpen(true);
        }}
      />
    </div>
  );
}

/** State of the run the scrollback is showing, and what you can do to it. */
function Header({
  run,
  expanded,
  onToggle,
}: {
  run: Run | undefined;
  expanded: boolean;
  onToggle(): void;
}) {
  const app = useApp();
  const runs = useRuns();
  const running = run?.state === "running" || run?.state === "pending";

  return (
    <button
      onClick={onToggle}
      className="no-drag"
      style={{
        display: "flex",
        alignItems: "center",
        gap: 9,
        border: "none",
        background: "transparent",
        padding: "6px 12px",
        cursor: "default",
        textAlign: "left",
        color: "inherit",
      }}
    >
      <span style={{ ...F.footnote, color: T.tertiary, letterSpacing: 0.5, fontWeight: 600 }}>
        CONSOLE
      </span>

      {run ? (
        <>
          <Dot
            color={
              run.state === "ok"
                ? STATUS.ok
                : run.state === "failed"
                  ? STATUS.danger
                  : running
                    ? ACCENT
                    : T.tertiary
            }
            pulse={running}
          />
          <Mono
            style={{
              flex: 1,
              minWidth: 0,
              fontSize: 11,
              color: T.secondary,
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
          >
            {run.command.display}
          </Mono>
          <span style={{ ...F.footnote, color: T.tertiary }}>
            {running ? "running" : `${run.state} · ${duration(run.durationMs)}`}
          </span>
        </>
      ) : (
        <span style={{ flex: 1, ...F.footnote, color: T.quaternary }}>
          Type a curie command. Tab completes, ↑ recalls.
        </span>
      )}

      <span onClick={(e) => e.stopPropagation()} style={{ display: "flex", gap: 6 }}>
        {running ? (
          <Button size="sm" tone="danger" onClick={() => runs.cancel(run!.id)}>
            Cancel
          </Button>
        ) : null}
        {run ? <CopyButton text={transcriptText(run)} label="Copy output" /> : null}
        {run ? (
          <Button size="sm" tone="plain" onClick={() => app.navigate("activity")}>
            History
          </Button>
        ) : null}
      </span>
      <span style={{ ...F.footnote, color: T.quaternary }}>{expanded ? "⌄" : "⌃"}</span>
    </button>
  );
}

/** The console's own lines interleaved with the focused run's output. */
function Scrollback({ notes, run }: { notes: readonly Note[]; run: Run | undefined }) {
  const colour: Record<Note["kind"], string> = {
    input: T.primary,
    error: STATUS.danger,
    info: T.tertiary,
  };
  return (
    <pre
      style={{
        margin: 0,
        fontFamily: FONT.mono,
        fontSize: 11.5,
        lineHeight: 1.55,
        whiteSpace: "pre-wrap",
        wordBreak: "break-word",
      }}
    >
      {notes.map((n) => (
        <div key={n.id} style={{ color: colour[n.kind] }}>
          {n.text}
        </div>
      ))}
      {run?.lines.map((l, i) => (
        <div key={i} style={{ color: l.stream === "stderr" ? STATUS.warn : T.secondary }}>
          {l.text}
        </div>
      ))}
      {run?.result !== undefined ? (
        <div style={{ color: T.tertiary, marginTop: 6 }}>
          {`--json → ${JSON.stringify(run.result, null, 2)}`}
        </div>
      ) : null}
      {run?.jsonError ? (
        <div style={{ color: STATUS.warn, marginTop: 6 }}>{`--json did not parse: ${run.jsonError}`}</div>
      ) : null}
    </pre>
  );
}

function Hints({ hints }: { hints: readonly string[] }) {
  return (
    <div
      style={{
        display: "flex",
        gap: 8,
        flexWrap: "wrap",
        padding: "6px 12px",
        borderTop: `1px solid ${LINE.separator}`,
      }}
    >
      {hints.map((h) => (
        <Mono key={h} style={{ fontSize: 11, color: T.tertiary }}>
          {h}
        </Mono>
      ))}
    </div>
  );
}

function Prompt({
  inputRef,
  text,
  setText,
  onKey,
  mode,
  confirmWord,
  onFocus,
}: {
  inputRef: RefObject<HTMLInputElement | null>;
  text: string;
  setText(v: string): void;
  onKey(e: ReactKeyboardEvent<HTMLInputElement>): void;
  mode: "command" | "stdin" | "confirm";
  confirmWord?: string;
  onFocus(): void;
}) {
  const lead =
    mode === "stdin" ? "stdin ›" : mode === "confirm" ? `type ${confirmWord} ›` : "curie";
  const leadColour = mode === "command" ? T.tertiary : mode === "confirm" ? STATUS.danger : ACCENT;

  return (
    <div
      onClick={() => inputRef.current?.focus()}
      style={{
        display: "flex",
        alignItems: "center",
        gap: 8,
        padding: "8px 12px",
        borderTop: `1px solid ${LINE.separator}`,
        background: S.field,
      }}
    >
      <Mono style={{ fontSize: 12, color: leadColour, flex: "none", fontWeight: 600 }}>{lead}</Mono>
      <input
        ref={inputRef}
        data-console-input
        value={text}
        onChange={(e) => setText(e.target.value)}
        onKeyDown={onKey}
        onFocus={onFocus}
        spellCheck={false}
        autoCapitalize="off"
        autoComplete="off"
        placeholder={
          mode === "stdin"
            ? "answer the prompt this command is waiting on"
            : mode === "confirm"
              ? "or Esc to cancel"
              : "local up --minimal"
        }
        style={{
          flex: 1,
          minWidth: 0,
          border: "none",
          outline: "none",
          background: "transparent",
          color: T.primary,
          fontFamily: FONT.mono,
          fontSize: 12.5,
        }}
      />
      <Kbd>↵</Kbd>
    </div>
  );
}
