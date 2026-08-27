// Activity: the session's log of everything this app has run.
//
// It exists because a GUI that runs commands on your behalf owes you an audit
// trail. Every row is the resolved command, its exit code, how long it took, and
// a way back to its full output — which is also what you paste into a bug
// report. Re-run is one click, because the second thing you want after a failed
// command is to run it again with one flag changed.

import { useMemo, useState } from "react";

import { useApp } from "../bridge/app";
import { useRuns, transcriptText, type Run } from "../bridge/runs";
import { ago, duration } from "../lib/format";
import { command } from "../lib/manifest";
import { ACCENT, F, FONT, LINE, R, S, STATUS, T } from "../tokens";
import { Badge, Button, CopyButton, Dot, EmptyState, Group, Input, Mono, SectionHeader, Segmented } from "../primitives";

type Filter = "all" | "running" | "failed";

export function Activity() {
  const app = useApp();
  const runs = useRuns();
  const [filter, setFilter] = useState<Filter>("all");
  const [query, setQuery] = useState("");

  const rows = useMemo(() => {
    const q = query.trim().toLowerCase();
    return runs.runs.filter((r) => {
      if (filter === "running" && r.state !== "running" && r.state !== "pending") return false;
      if (filter === "failed" && r.state !== "failed") return false;
      if (q && !r.command.display.toLowerCase().includes(q)) return false;
      return true;
    });
  }, [runs.runs, filter, query]);

  const failed = runs.runs.filter((r) => r.state === "failed").length;

  if (!runs.runs.length) {
    return (
      <EmptyState
        title="Nothing has run yet"
        action={
          <Button tone="primary" onClick={() => app.setPaletteOpen(true)}>
            Open the command palette
          </Button>
        }
      >
        Every <Mono>curie</Mono> command this app runs is recorded here with its full output, so a
        GUI never costs you the terminal scrollback.
      </EmptyState>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <div style={{ flex: 1, ...F.callout, color: T.tertiary }}>
          {runs.runs.length} command{runs.runs.length === 1 ? "" : "s"} this session
          {failed ? `, ${failed} failed` : ""}
        </div>
        <Input
          value={query}
          placeholder="Filter…"
          spellCheck={false}
          onChange={(e) => setQuery(e.target.value)}
          style={{ width: 200 }}
        />
        <Segmented<Filter>
          size="sm"
          value={filter}
          onChange={setFilter}
          options={[
            { value: "all", label: "All" },
            { value: "running", label: "Running" },
            { value: "failed", label: "Failed" },
          ]}
        />
      </div>

      <Group>
        {rows.map((run) => (
          <Row key={run.id} run={run} />
        ))}
        {rows.length === 0 ? (
          <div style={{ padding: 20, fontSize: 12, color: T.tertiary }}>Nothing matches that filter.</div>
        ) : null}
      </Group>
    </div>
  );
}

function Row({ run }: { run: Run }) {
  const runs = useRuns();
  const app = useApp();
  const [expanded, setExpanded] = useState(false);
  const cmd = command(run.action);

  const color =
    run.state === "ok"
      ? STATUS.ok
      : run.state === "failed"
        ? STATUS.danger
        : run.state === "running"
          ? ACCENT
          : T.tertiary;

  return (
    <div style={{ borderTop: `1px solid ${LINE.separator}` }}>
      <div
        onClick={() => setExpanded((v) => !v)}
        style={{
          display: "grid",
          gridTemplateColumns: "auto 1fr auto auto auto",
          gap: 12,
          alignItems: "center",
          padding: "9px 14px",
          cursor: "pointer",
          fontSize: 12,
        }}
      >
        <Dot color={color} pulse={run.state === "running"} />
        <Mono
          style={{
            color: T.secondary,
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
          title={run.command.display}
        >
          {run.command.display}
        </Mono>
        <span style={{ color: T.tertiary, fontSize: 11 }}>{duration(run.durationMs)}</span>
        {run.state === "failed" ? (
          <Badge color={STATUS.danger} filled>
            exit {run.exitCode ?? "?"}
          </Badge>
        ) : (
          <span style={{ color: T.tertiary, fontSize: 11 }}>{run.state}</span>
        )}
        <span style={{ color: T.tertiary, fontSize: 11 }}>{ago(run.startedAt)}</span>
      </div>

      {expanded ? (
        <div style={{ padding: "0 14px 14px 34px" }}>
          <div style={{ display: "flex", gap: 7, marginBottom: 10, flexWrap: "wrap" }}>
            <Button
              size="sm"
              onClick={() => {
                runs.focus(run.id);
                runs.setConsoleOpen(true);
              }}
            >
              Open transcript
            </Button>
            {cmd ? (
              <Button size="sm" onClick={() => app.runCommand(cmd.id)}>
                Run again
              </Button>
            ) : null}
            <CopyButton text={run.command.display} label="Copy command" />
            <CopyButton text={transcriptText(run)} label="Copy output" />
          </div>

          {/* The tail, inline: enough to see why something failed without
              leaving the list. The full transcript is one click away. */}
          <SectionHeader>Last lines</SectionHeader>
          <pre
            data-selectable
            style={{
              margin: 0,
              maxHeight: 220,
              overflow: "auto",
              background: S.well,
              border: `1px solid ${LINE.separator}`,
              borderRadius: R.control,
              padding: 10,
              fontFamily: FONT.mono,
              fontSize: 11,
              color: T.secondary,
              whiteSpace: "pre-wrap",
            }}
          >
            {run.lines.slice(-25).map((l) => l.text).join("\n") || "(no output)"}
          </pre>

          {run.result !== undefined ? (
            <>
              <div style={{ marginTop: 12 }}>
                <SectionHeader>Parsed --json result</SectionHeader>
              </div>
              <pre
                data-selectable
                style={{
                  margin: 0,
                  maxHeight: 260,
                  overflow: "auto",
                  background: S.well,
                  border: `1px solid ${LINE.separator}`,
                  borderRadius: R.control,
                  padding: 10,
                  fontFamily: FONT.mono,
                  fontSize: 11,
                  color: T.secondary,
                }}
              >
                {JSON.stringify(run.result, null, 2)}
              </pre>
            </>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
