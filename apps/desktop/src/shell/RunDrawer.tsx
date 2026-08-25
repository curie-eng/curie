// The transcript drawer: the terminal the GUI would otherwise have taken away.
//
// A run is never summarised into a spinner and a green tick. The resolved
// command, the interleaved stdout/stderr exactly as the CLI wrote it, the exit
// code and the duration are all here, for this run and every earlier one in the
// session. If something goes wrong the operator has the same material they
// would have had in a terminal, and a Copy button to paste it into an issue.

import { useEffect, useLayoutEffect, useRef, useState } from "react";

import { useRuns, transcriptText, type Run } from "../bridge/runs";
import { clock, duration, stripAnsi } from "../lib/format";
import { ACCENT, FONT, LINE, R, S, STATUS, T } from "../tokens";
import { Badge, Button, CopyButton, Dot, Input, Mono, Spinner } from "../primitives";

const MIN_HEIGHT = 160;
const MAX_HEIGHT = 620;

function stateColor(run: Run): string {
  switch (run.state) {
    case "running":
    case "pending":
      return ACCENT;
    case "ok":
      return STATUS.ok;
    case "failed":
      return STATUS.danger;
    default:
      return T.tertiary;
  }
}

export function RunDrawer() {
  const runs = useRuns();
  const [height, setHeight] = useState(300);
  const dragging = useRef<{ startY: number; startH: number } | null>(null);

  useEffect(() => {
    const move = (e: MouseEvent) => {
      if (!dragging.current) return;
      const next = dragging.current.startH + (dragging.current.startY - e.clientY);
      setHeight(Math.max(MIN_HEIGHT, Math.min(MAX_HEIGHT, next)));
    };
    const up = () => {
      dragging.current = null;
    };
    window.addEventListener("mousemove", move);
    window.addEventListener("mouseup", up);
    return () => {
      window.removeEventListener("mousemove", move);
      window.removeEventListener("mouseup", up);
    };
  }, []);

  if (!runs.drawerOpen || !runs.runs.length) return null;

  const run = runs.runs.find((r) => r.id === runs.focused) ?? runs.runs[0];

  return (
    <div
      style={{
        flex: "none",
        height,
        display: "flex",
        flexDirection: "column",
        background: S.well,
        borderTop: `1px solid ${LINE.border}`,
      }}
    >
      <div
        onMouseDown={(e) => {
          dragging.current = { startY: e.clientY, startH: height };
        }}
        style={{ height: 5, cursor: "ns-resize", flex: "none" }}
      />
      <div style={{ display: "flex", flex: 1, minHeight: 0 }}>
        <RunList />
        <RunTranscript run={run} />
      </div>
    </div>
  );
}

function RunList() {
  const runs = useRuns();
  return (
    <div
      style={{
        width: 230,
        flex: "none",
        borderRight: `1px solid ${LINE.separator}`,
        display: "flex",
        flexDirection: "column",
      }}
    >
      <div
        style={{
          padding: "7px 10px",
          borderBottom: `1px solid ${LINE.separator}`,
          display: "flex",
          alignItems: "center",
          gap: 8,
          fontSize: 11,
          color: T.tertiary,
        }}
      >
        <span style={{ flex: 1 }}>Runs</span>
        <Button size="sm" tone="plain" onClick={runs.clear} title="Clear finished runs">
          Clear
        </Button>
        <Button size="sm" tone="plain" onClick={() => runs.setDrawerOpen(false)}>
          ✕
        </Button>
      </div>
      <div style={{ overflow: "auto", flex: 1 }}>
        {runs.runs.map((r) => {
          const active = r.id === runs.focused;
          return (
            <button
              key={r.id}
              onClick={() => runs.focus(r.id)}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 7,
                width: "100%",
                textAlign: "left",
                padding: "6px 10px",
                border: "none",
                borderLeft: `2px solid ${active ? ACCENT : "transparent"}`,
                background: active ? S.raised : "transparent",
                cursor: "pointer",
                fontSize: 11,
                color: active ? T.primary : T.secondary,
              }}
            >
              {r.state === "running" ? (
                <Spinner size={9} color={ACCENT} />
              ) : (
                <Dot color={stateColor(r)} />
              )}
              <Mono
                style={{
                  flex: 1,
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                  fontSize: 11,
                }}
              >
                {r.action.replace(/\./g, " ")}
              </Mono>
              <span style={{ color: T.tertiary, fontSize: 10 }}>{clock(r.startedAt)}</span>
            </button>
          );
        })}
      </div>
    </div>
  );
}

function RunTranscript({ run }: { run: Run }) {
  const runs = useRuns();
  const scroller = useRef<HTMLDivElement>(null);
  const [stick, setStick] = useState(true);
  const [stdin, setStdin] = useState("");

  // Follow the tail while the operator is at the bottom, and stop the moment
  // they scroll up to read something -- an auto-scroll that fights the reader is
  // the classic log-viewer bug.
  useLayoutEffect(() => {
    if (!stick || !scroller.current) return;
    scroller.current.scrollTop = scroller.current.scrollHeight;
  }, [run.lines.length, stick]);

  const running = run.state === "running" || run.state === "pending";

  return (
    <div style={{ flex: 1, minWidth: 0, display: "flex", flexDirection: "column" }}>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 9,
          padding: "7px 11px",
          borderBottom: `1px solid ${LINE.separator}`,
        }}
      >
        <Mono
          data-selectable
          style={{
            flex: 1,
            color: T.secondary,
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
          title={run.command.display}
        >
          {run.command.display}
        </Mono>
        {run.state === "ok" ? <Badge color={STATUS.ok}>exit 0</Badge> : null}
        {run.state === "failed" ? (
          <Badge color={STATUS.danger} filled>
            exit {run.exitCode ?? "?"}
          </Badge>
        ) : null}
        {run.state === "cancelled" ? <Badge color={STATUS.warn}>cancelled</Badge> : null}
        {run.durationMs !== undefined ? (
          <span style={{ fontSize: 11, color: T.tertiary }}>{duration(run.durationMs)}</span>
        ) : null}
        <CopyButton text={run.command.display} label="Copy cmd" />
        <CopyButton text={transcriptText(run)} label="Copy output" />
        {running ? (
          <Button size="sm" tone="danger" onClick={() => runs.cancel(run.id)}>
            Stop
          </Button>
        ) : null}
      </div>

      <div
        ref={scroller}
        onScroll={(e) => {
          const el = e.currentTarget;
          setStick(el.scrollHeight - el.scrollTop - el.clientHeight < 24);
        }}
        style={{
          flex: 1,
          overflow: "auto",
          padding: "8px 11px",
          fontFamily: FONT.mono,
          fontSize: 11.5,
          lineHeight: 1.55,
        }}
        data-selectable
      >
        {run.lines.length === 0 && running ? (
          <span style={{ color: T.tertiary }}>waiting for output…</span>
        ) : null}
        {run.lines.map((line, i) => (
          <div
            key={i}
            style={{
              color: line.stream === "stderr" ? T.secondary : T.primary,
              whiteSpace: "pre-wrap",
              wordBreak: "break-word",
            }}
          >
            {stripAnsi(line.text) || " "}
          </div>
        ))}
        {run.jsonError ? (
          <div style={{ color: STATUS.warn, marginTop: 8 }}>{run.jsonError}</div>
        ) : null}
      </div>

      {/* Some commands interview you (`curie init`, `skill eval-init`). Without
          a way to answer, the GUI would simply be unable to run them -- which is
          exactly the "worse than the CLI" failure this app is trying to avoid. */}
      {running ? (
        <div
          style={{
            display: "flex",
            gap: 7,
            padding: "6px 10px",
            borderTop: `1px solid ${LINE.separator}`,
            alignItems: "center",
          }}
        >
          <span style={{ color: ACCENT, fontFamily: FONT.mono, fontSize: 12 }}>›</span>
          <Input
            value={stdin}
            placeholder="Answer a prompt from this command, then press Enter"
            spellCheck={false}
            onChange={(e) => setStdin(e.target.value)}
            onKeyDown={(e) => {
              if (e.key !== "Enter") return;
              runs.send(run.id, `${stdin}\n`);
              setStdin("");
              setStick(true);
            }}
            style={{ fontFamily: FONT.mono, border: "none", background: "transparent" }}
          />
        </div>
      ) : null}

      {!stick ? (
        <button
          onClick={() => {
            setStick(true);
            if (scroller.current) scroller.current.scrollTop = scroller.current.scrollHeight;
          }}
          style={{
            position: "absolute",
            right: 22,
            bottom: 44,
            background: S.raised,
            border: `1px solid ${LINE.border}`,
            borderRadius: R.control,
            padding: "3px 9px",
            fontSize: 11,
            color: T.secondary,
            cursor: "pointer",
          }}
        >
          Follow output
        </button>
      ) : null}
    </div>
  );
}
