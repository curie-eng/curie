// The resource monitor.
//
// The information architecture is lifted from Docker Desktop's container list,
// which gets several things right that a naive table does not:
//
//   - **Usage is shown over capacity.** "121% CPU" is alarming on two cores and
//     idle on twelve. Every headline number here carries its denominator, taken
//     from the daemon itself (`docker info`), because a percentage without one is
//     not information.
//   - **Compose projects are collapsible parent rows** carrying their own
//     aggregate and a mixed-state status glyph, not just a band of text. One
//     `curie local up` is one row until you open it.
//   - **Status is its own column**, a filled or hollow glyph. Colour on this
//     table means *role*, so it cannot also mean state.
//   - **Ports and image are columns**, not detail buried a click away: "where is
//     the API listening" is a question you ask constantly.
//
// What is deliberately NOT taken from it is per-row start/stop. Docker Desktop
// can offer that because it is a Docker client; this app's contract is that
// everything it does is a `curie` command you can see and copy. So each row's
// menu offers the commands that actually map -- `skill down` for a runner,
// `local rebuild <service>` for a compose service -- and raw container control
// is left to Docker Desktop, which is better at it.
//
// Anything unmeasurable renders as an em dash. A stopped container has no stats
// row at all, and drawing that as 0% would say "idle" when the truth is "gone".

import { useMemo, useState, type ReactNode } from "react";

import { useApp } from "../bridge/app";
import { ActionButtons, RunButton } from "./Actions";
import { resolve, surfacesById } from "../lib/surfaces";
import { useResources } from "../bridge/resources";
import { useRuns } from "../bridge/runs";
import { bridge } from "../bridge/bridge";
import type { PortBinding, ResourceSample } from "../bridge/bridge";
import { ago, bytes, DASH, percent } from "../lib/format";
import { aggregate, capacityNotes, groupRows, selectRows, type GroupKey, type Section, type SortKey } from "../lib/workloads";
import { ACCENT, F, KNOB, LINE, R, S, SHADOW, STATUS, T, roleColor, tint } from "../tokens";
import { FitWidth, Sparkline, StackedArea, UsageBar } from "../primitives/charts";
import {
  Badge,
  Button,
  EmptyState,
  Group,
  Input,
  Mono,
  Notice,
  SectionHeader,
  Segmented,
  Sheet,
  Toggle,
  Well,
} from "../primitives";


/** Optional columns, so a dense table can be trimmed to what you care about.
 *  Name, status, CPU and the trend are not optional -- they are the view. */
const OPTIONAL = ["id", "image", "ports", "memory", "net", "pids", "uptime"] as const;
type OptionalColumn = (typeof OPTIONAL)[number];

const COLUMN_LABEL: Record<OptionalColumn, string> = {
  id: "Container ID",
  image: "Image",
  ports: "Ports",
  memory: "Memory",
  net: "Net I/O",
  pids: "PIDs",
  uptime: "Started",
};

const DEFAULT_COLUMNS: OptionalColumn[] = ["ports", "memory", "net", "uptime"];

export function Resources() {
  const app = useApp();
  const res = useResources();

  const [sort, setSort] = useState<SortKey>("cpu");
  const [group, setGroup] = useState<GroupKey>("project");
  const [runningOnly, setRunningOnly] = useState(false);
  const [query, setQuery] = useState("");
  const [columns, setColumns] = useState<OptionalColumn[]>(DEFAULT_COLUMNS);
  const [chartOpen, setChartOpen] = useState(true);
  const [collapsed, setCollapsed] = useState<ReadonlySet<string>>(new Set());
  const [inspecting, setInspecting] = useState<ResourceSample | null>(null);
  const [columnsOpen, setColumnsOpen] = useState(false);

  const rows = useMemo(
    () => selectRows(res.samples, { query, runningOnly, sort }),
    [res.samples, query, runningOnly, sort],
  );

  const sections = useMemo<Section[]>(() => groupRows(rows, group), [rows, group]);

  // Band order is keyed off role rather than whatever is sorted first, so the
  // stack does not re-colour itself every tick.
  const bands = useMemo(() => {
    const roles = [...new Set(res.samples.map((s) => s.role))].sort();
    return roles.map((role) => {
      const names = res.samples.filter((s) => s.role === role).map((s) => s.name);
      const length = Math.max(0, ...names.map((n) => res.history.get(n)?.cpu.length ?? 0));
      const values = Array.from({ length }, (_, i) =>
        names.reduce((sum, n) => sum + (res.history.get(n)?.cpu[i] ?? 0), 0),
      );
      return { key: role, color: roleColor(role), values };
    });
  }, [res.samples, res.history]);

  // The stacked total at its highest point in the retained window -- the number
  // the autoscaled axis tops out at, so the caption and the drawing agree.
  const chartPeak = useMemo(() => {
    const length = Math.max(0, ...bands.map((b) => b.values.length));
    let peak = 0;
    for (let i = 0; i < length; i++) {
      peak = Math.max(peak, bands.reduce((sum, b) => sum + (b.values[i] ?? 0), 0));
    }
    return peak;
  }, [bands]);

  const noDocker = app.env && !app.env.dockerAvailable;
  const shown = columns;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
      <Headline />

      {noDocker ? (
        <Notice tone="warn" title="Docker is not reachable">
          The <Mono>skill</Mono> and <Mono>local</Mono> tiers run in containers, so without Docker
          there is nothing to measure here. Start Docker and this view fills in on its own.
        </Notice>
      ) : null}

      {res.error && !noDocker ? (
        <Notice tone="warn" title="The last sample failed">
          {res.error} — the numbers below are from the last frame that succeeded.
        </Notice>
      ) : null}

      <section>
        <SectionHeader
          right={
            <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
              {chartOpen ? (
                <div style={{ display: "flex", gap: 9, flexWrap: "wrap" }}>
                  {bands
                    .filter((b) => b.values.some((v) => v > 0))
                    .map((b) => (
                      <span
                        key={b.key}
                        style={{
                          display: "inline-flex",
                          alignItems: "center",
                          gap: 4,
                          ...F.footnote,
                          color: T.tertiary,
                        }}
                      >
                        <span
                          style={{
                            width: 6,
                            height: 6,
                            borderRadius: 2,
                            background: b.color,
                            display: "inline-block",
                          }}
                        />
                        {b.key}
                      </span>
                    ))}
                </div>
              ) : null}
              <Button size="sm" tone="plain" onClick={() => setChartOpen((v) => !v)}>
                {chartOpen ? "Hide chart" : "Show chart"}
              </Button>
            </div>
          }
        >
          CPU, last {Math.round((res.intervalMs * 60) / 1000)}s
        </SectionHeader>
        {chartOpen ? (
          <Group style={{ padding: 14 }}>
            {/* Autoscaled, with the scale stated.
             *
             *  Pinning the axis to the machine's ceiling was the first attempt
             *  and it was a mistake: on a 12-core box a real 95% load draws as a
             *  flat line at 8% of the height, which is technically honest and
             *  practically useless. So the axis follows the data and the caption
             *  carries the denominator, with a dashed guide at one core to keep
             *  the numbers grounded. */}
            <FitWidth height={92}>
              {(w) => (
                <StackedArea
                  bands={bands}
                  width={w}
                  height={92}
                  guides={[{ value: 100, label: "1 core" }]}
                />
              )}
            </FitWidth>
            <div style={{ ...F.footnote, color: T.quaternary, marginTop: 6 }}>
              Peak {percent(chartPeak, 0)}
              {res.totals.cpuCeiling ? ` of ${percent(res.totals.cpuCeiling, 0)} available` : ""} —
              axis follows the data, not the machine.
            </div>
          </Group>
        ) : null}
      </section>

      {/* Controls. Search first, because with a dozen containers it is the thing
          you reach for; the rest shape the table. */}
      <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
        <Input
          value={query}
          placeholder="Search name, image, port…"
          spellCheck={false}
          onChange={(e) => setQuery(e.target.value)}
          style={{ width: 240 }}
        />
        <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
          <Toggle checked={runningOnly} onChange={setRunningOnly} />
          <span style={{ ...F.caption, color: T.secondary }}>Only running</span>
        </div>
        <div style={{ flex: 1 }} />

        <div style={{ position: "relative" }}>
          <Button size="sm" onClick={() => setColumnsOpen((v) => !v)}>
            Columns
          </Button>
          {columnsOpen ? (
            <ColumnMenu
              columns={columns}
              onToggle={(c) =>
                setColumns((prev) =>
                  prev.includes(c) ? prev.filter((x) => x !== c) : [...prev, c],
                )
              }
              onClose={() => setColumnsOpen(false)}
            />
          ) : null}
        </div>

        <Segmented<GroupKey>
          size="sm"
          value={group}
          onChange={setGroup}
          options={[
            { value: "project", label: "Project", title: "Group by compose project" },
            { value: "agent", label: "Agent", title: "Group by owning agent" },
            { value: "role", label: "Role", title: "Group by workload role" },
            { value: "none", label: "Flat", title: "No grouping" },
          ]}
        />
        <Segmented<SortKey>
          size="sm"
          value={sort}
          onChange={setSort}
          options={[
            { value: "cpu", label: "CPU" },
            { value: "mem", label: "Mem" },
            { value: "net", label: "Net" },
            { value: "name", label: "A–Z" },
          ]}
        />
      </div>

      {rows.length === 0 ? (
        <Group>
          <EmptyState
            title={
              query
                ? `Nothing matches “${query}”`
                : noDocker
                  ? "Nothing to measure"
                  : "No Curie containers are running"
            }
            action={
              query ? (
                <Button onClick={() => setQuery("")}>Clear search</Button>
              ) : (
                <RunButton id="local.up" tone="primary" size="md">
                  Bring the local stack up
                </RunButton>
              )
            }
          >
            {query ? null : (
              <>
                Start a bundle with <Mono>curie skill up</Mono> for the single-container tier, or{" "}
                <Mono>curie local up</Mono> for the full platform. Both appear here as they come up.
              </>
            )}
          </EmptyState>
        </Group>
      ) : (
        <Group>
          <HeaderRow columns={shown} />
          {sections.map((section) => {
            const isCollapsed = collapsed.has(section.key);
            return (
              <div key={section.key}>
                {section.kind !== "none" ? (
                  <ProjectRow
                    section={section}
                    columns={shown}
                    collapsed={isCollapsed}
                    onToggle={() =>
                      setCollapsed((prev) => {
                        const next = new Set(prev);
                        if (next.has(section.key)) next.delete(section.key);
                        else next.add(section.key);
                        return next;
                      })
                    }
                  />
                ) : null}
                {isCollapsed
                  ? null
                  : section.rows.map((row) => (
                      <WorkloadRow
                        key={row.name}
                        sample={row}
                        columns={shown}
                        indented={section.kind !== "none"}
                        onInspect={() => setInspecting(row)}
                      />
                    ))}
              </div>
            );
          })}
        </Group>
      )}

      <div style={{ ...F.footnote, color: T.quaternary, lineHeight: 1.6 }}>
        Sourced from <Mono style={{ fontSize: 10 }}>docker stats</Mono>,{" "}
        <Mono style={{ fontSize: 10 }}>docker ps</Mono> and{" "}
        <Mono style={{ fontSize: 10 }}>docker info</Mono> on this machine. Cluster workloads are not
        measured here — the platform API reports runner pod names, not their resource use, so this
        view would be guessing.{" "}
        <RunButton id="cluster.status" tone="plain">
          Check cluster status instead
        </RunButton>
        .
      </div>

      {inspecting ? (
        <InspectSheet sample={inspecting} onClose={() => setInspecting(null)} />
      ) : null}
    </div>
  );
}

/** The headline numbers, each over its ceiling. */
function Headline() {
  const res = useResources();
  const cpu = res.totals.cpu;
  const cpuCeiling = res.totals.cpuCeiling;
  const mem = res.totals.mem;
  const memCeiling = res.totals.memCeiling;
  // The ceilings come from `docker info`, which on macOS and Windows describes a
  // VM and not the machine. Naming that is the difference between a limit and an
  // apparently wrong number.
  const notes = capacityNotes(res.capacity ?? null);

  return (
    <div style={{ display: "flex", gap: 40, alignItems: "flex-start", flexWrap: "wrap" }}>
      <Meter
        label="Container CPU"
        value={percent(cpu, 2)}
        ceiling={cpuCeiling ? percent(cpuCeiling, 0) : null}
        note={notes.cpu}
        ratio={cpu !== null && cpuCeiling ? cpu / cpuCeiling : null}
      />
      <Meter
        label="Container memory"
        value={bytes(mem)}
        ceiling={memCeiling ? bytes(memCeiling) : null}
        note={notes.mem}
        ratio={mem !== null && memCeiling ? mem / memCeiling : null}
      />
      <div style={{ flex: 1 }} />
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <Toggle checked={!res.paused} onChange={(on) => res.setPaused(!on)} />
        <span style={{ ...F.caption, color: T.secondary, minWidth: 42 }}>
          {res.paused ? "Paused" : "Live"}
        </span>
        <Segmented
          size="sm"
          value={String(res.intervalMs)}
          onChange={(v: string) => res.setIntervalMs(Number(v))}
          options={[
            { value: "1000", label: "1s" },
            { value: "2000", label: "2s" },
            { value: "5000", label: "5s" },
            { value: "10000", label: "10s" },
          ]}
        />
      </div>
    </div>
  );
}

function Meter({
  label,
  value,
  ceiling,
  note,
  ratio,
}: {
  label: string;
  value: string;
  ceiling: string | null;
  note: string;
  ratio: number | null;
}) {
  return (
    <div style={{ minWidth: 210 }}>
      <div style={{ ...F.caption, color: T.tertiary, marginBottom: 3 }}>{label}</div>
      <div style={{ display: "flex", alignItems: "baseline", gap: 5, marginBottom: 5 }}>
        <span
          style={{
            fontSize: 21,
            fontWeight: 600,
            letterSpacing: -0.5,
            color: ratio !== null && ratio > 0.85 ? STATUS.warn : ACCENT,
            fontVariantNumeric: "tabular-nums",
          }}
        >
          {value}
        </span>
        {ceiling ? (
          <span style={{ ...F.callout, color: T.tertiary, fontVariantNumeric: "tabular-nums" }}>
            / {ceiling}
          </span>
        ) : null}
      </div>
      <UsageBar value={ratio} max={ratio === null ? null : 1} height={3} />
      <div style={{ ...F.footnote, color: T.quaternary, marginTop: 4 }}>{note}</div>
    </div>
  );
}

// --- table -----------------------------------------------------------------

function template(columns: readonly OptionalColumn[]): string {
  const parts = ["18px", "minmax(180px, 1.6fr)", "62px", "84px"];
  if (columns.includes("id")) parts.push("96px");
  if (columns.includes("image")) parts.push("minmax(110px, 1fr)");
  if (columns.includes("ports")) parts.push("minmax(96px, 0.8fr)");
  if (columns.includes("memory")) parts.push("120px");
  if (columns.includes("net")) parts.push("110px");
  if (columns.includes("pids")) parts.push("48px");
  if (columns.includes("uptime")) parts.push("76px");
  parts.push("28px");
  return parts.join(" ");
}

function HeaderRow({ columns }: { columns: readonly OptionalColumn[] }) {
  const cell = { ...F.footnote, color: T.quaternary, fontWeight: 600 as const };
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: template(columns),
        gap: 10,
        alignItems: "center",
        padding: "7px 14px",
        borderBottom: `1px solid ${LINE.separator}`,
      }}
    >
      <span />
      <span style={cell}>NAME</span>
      <span style={cell}>CPU</span>
      <span style={cell}>TREND</span>
      {columns.includes("id") ? <span style={cell}>ID</span> : null}
      {columns.includes("image") ? <span style={cell}>IMAGE</span> : null}
      {columns.includes("ports") ? <span style={cell}>PORTS</span> : null}
      {columns.includes("memory") ? <span style={cell}>MEMORY</span> : null}
      {columns.includes("net") ? <span style={cell}>NET I/O</span> : null}
      {columns.includes("pids") ? <span style={cell}>PIDS</span> : null}
      {columns.includes("uptime") ? (
        <span style={{ ...cell, textAlign: "right" }}>STARTED</span>
      ) : null}
      <span />
    </div>
  );
}

/** A status glyph: filled for running, ring for stopped, half for a group with
 *  both. Shape and fill rather than colour, because colour on this table already
 *  means role. */
function StatusGlyph({ state }: { state: "running" | "stopped" | "mixed" }) {
  const color = state === "stopped" ? T.quaternary : ACCENT;
  return (
    <svg width={10} height={10} viewBox="0 0 10 10" aria-label={state} role="img">
      {state === "running" ? <circle cx={5} cy={5} r={4} fill={color} /> : null}
      {state === "stopped" ? (
        <circle cx={5} cy={5} r={3.4} fill="none" stroke={color} strokeWidth={1.4} />
      ) : null}
      {state === "mixed" ? (
        <>
          <circle cx={5} cy={5} r={3.4} fill="none" stroke={color} strokeWidth={1.4} />
          <path d="M5 1.6 A3.4 3.4 0 0 1 5 8.4 Z" fill={color} />
        </>
      ) : null}
    </svg>
  );
}

/** A compose project (or other grouping) as a real parent row: aggregate CPU,
 *  a mixed-state glyph, and a chevron. One `curie local up` collapses to one
 *  line. */
function ProjectRow({
  section,
  columns,
  collapsed,
  onToggle,
}: {
  section: Section;
  columns: readonly OptionalColumn[];
  collapsed: boolean;
  onToggle(): void;
}) {
  const { running, total, cpu, mem, startedAt, state } = aggregate(section.rows);

  return (
    <div
      onClick={onToggle}
      data-testid="group-row"
      data-group={section.key}
      aria-expanded={!collapsed}
      style={{
        display: "grid",
        gridTemplateColumns: template(columns),
        gap: 10,
        alignItems: "center",
        padding: "8px 14px",
        background: tint(KNOB, 0.03),
        borderBottom: `1px solid ${LINE.separator}`,
        cursor: "default",
      }}
    >
      <StatusGlyph state={state} />
      <span style={{ display: "flex", alignItems: "center", gap: 6, minWidth: 0 }}>
        <span
          aria-hidden
          style={{
            color: T.tertiary,
            fontSize: 9,
            transform: collapsed ? "rotate(-90deg)" : "none",
            transition: "transform 120ms ease",
          }}
        >
          ▼
        </span>
        <span style={{ ...F.headline, overflow: "hidden", textOverflow: "ellipsis" }}>
          {section.label}
        </span>
        <span style={{ ...F.footnote, color: T.quaternary }}>
          {running}/{total}
        </span>
      </span>
      <Mono style={{ fontSize: 11, color: T.secondary, fontVariantNumeric: "tabular-nums" }}>
        {percent(cpu, 1)}
      </Mono>
      <span />
      {columns.includes("id") ? <span /> : null}
      {columns.includes("image") ? <span /> : null}
      {columns.includes("ports") ? <span /> : null}
      {columns.includes("memory") ? (
        <Mono style={{ fontSize: 11, color: T.secondary, fontVariantNumeric: "tabular-nums" }}>
          {bytes(mem)}
        </Mono>
      ) : null}
      {columns.includes("net") ? <span /> : null}
      {columns.includes("pids") ? <span /> : null}
      {columns.includes("uptime") ? (
        <span
          style={{ ...F.footnote, color: T.quaternary, textAlign: "right" }}
        >
          {startedAt ? ago(startedAt) : DASH}
        </span>
      ) : null}
      <span />
    </div>
  );
}

function WorkloadRow({
  sample,
  columns,
  indented,
  onInspect,
}: {
  sample: ResourceSample;
  columns: readonly OptionalColumn[];
  indented: boolean;
  onInspect(): void;
}) {
  const res = useResources();
  const [hover, setHover] = useState(false);
  const history = res.history.get(sample.name);
  const running = sample.state === "running";
  const color = roleColor(sample.role);

  return (
    <div
      onClick={onInspect}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      data-testid="workload-row"
      data-workload={sample.name}
      style={{
        display: "grid",
        gridTemplateColumns: template(columns),
        gap: 10,
        alignItems: "center",
        padding: "7px 14px",
        paddingLeft: indented ? 30 : 14,
        borderBottom: `1px solid ${LINE.separator}`,
        background: hover ? S.hover : "transparent",
        opacity: running ? 1 : 0.55,
        cursor: "default",
      }}
    >
      <StatusGlyph state={running ? "running" : "stopped"} />

      <span style={{ minWidth: 0 }}>
        <Mono
          style={{
            display: "block",
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
            color: T.primary,
            fontSize: 12,
          }}
          title={sample.name}
        >
          {sample.service ?? sample.name}
        </Mono>
        <span style={{ ...F.footnote, color: T.quaternary }}>
          <span style={{ color }}>{sample.role}</span>
          {sample.agent ? ` · ${sample.agent}` : ""}
          {running ? "" : ` · ${sample.state}`}
        </span>
      </span>

      <Mono
        style={{
          fontSize: 12,
          color: (sample.cpuPercent ?? 0) > 80 ? STATUS.warn : T.primary,
          fontVariantNumeric: "tabular-nums",
        }}
      >
        {percent(sample.cpuPercent)}
      </Mono>

      <Sparkline
        values={history?.cpu ?? []}
        color={color}
        width={80}
        height={20}
        ariaLabel={`${sample.name} CPU trend`}
      />

      {columns.includes("id") ? (
        <Mono style={{ fontSize: 11, color: T.tertiary }}>{sample.id.slice(0, 12)}</Mono>
      ) : null}

      {columns.includes("image") ? (
        <Mono
          style={{
            fontSize: 11,
            color: T.tertiary,
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
            direction: "rtl",
            textAlign: "left",
          }}
          title={sample.image ?? undefined}
        >
          {sample.image ?? DASH}
        </Mono>
      ) : null}

      {columns.includes("ports") ? <Ports ports={sample.ports} /> : null}

      {columns.includes("memory") ? (
        <span>
          <Mono style={{ fontSize: 11, color: T.primary, fontVariantNumeric: "tabular-nums" }}>
            {bytes(sample.memBytes)}
          </Mono>
          <div style={{ marginTop: 3 }}>
            <UsageBar
              value={sample.memBytes}
              max={sample.memLimitBytes}
              color={color}
              height={3}
              title={
                sample.memLimitBytes
                  ? `${bytes(sample.memBytes)} of ${bytes(sample.memLimitBytes)}`
                  : "no memory limit set on this container"
              }
            />
          </div>
        </span>
      ) : null}

      {columns.includes("net") ? (
        <Mono
          style={{ fontSize: 11, color: T.tertiary, fontVariantNumeric: "tabular-nums" }}
          title="received / sent since the container started"
        >
          {bytes(sample.netRxBytes, 0)} / {bytes(sample.netTxBytes, 0)}
        </Mono>
      ) : null}

      {columns.includes("pids") ? (
        <Mono style={{ fontSize: 11, color: T.tertiary }}>{sample.pids ?? DASH}</Mono>
      ) : null}

      {columns.includes("uptime") ? (
        <span style={{ ...F.footnote, color: T.quaternary, textAlign: "right" }}>
          {ago(sample.startedAt)}
        </span>
      ) : null}

      <span style={{ color: T.quaternary, fontSize: 11, textAlign: "right" }}>›</span>
    </div>
  );
}

/** Published ports, with the host port clickable when it plausibly speaks HTTP.
 *  Opening it goes to the real browser, which is where a dashboard belongs. */
function Ports({ ports }: { ports: readonly PortBinding[] }) {
  const published = ports.filter((p) => p.host !== null);
  if (!published.length) {
    return <span style={{ ...F.footnote, color: T.quaternary }}>{DASH}</span>;
  }
  const [first, ...rest] = published;
  return (
    <span style={{ display: "flex", alignItems: "center", gap: 5, minWidth: 0 }}>
      <button
        onClick={(e) => {
          e.stopPropagation();
          void bridge().shell.openExternal(`http://localhost:${first.host}`);
        }}
        title={`Open http://localhost:${first.host} in your browser`}
        style={{
          border: "none",
          background: "none",
          padding: 0,
          color: ACCENT,
          fontFamily: "inherit",
          fontSize: 11,
          cursor: "default",
        }}
      >
        <Mono style={{ fontSize: 11, color: ACCENT }}>
          {first.host}:{first.container}
        </Mono>
      </button>
      {rest.length ? (
        <span
          style={{ ...F.footnote, color: T.quaternary }}
          title={rest.map((p) => `${p.host}:${p.container}/${p.proto}`).join(", ")}
        >
          +{rest.length}
        </span>
      ) : null}
    </span>
  );
}

function ColumnMenu({
  columns,
  onToggle,
  onClose,
}: {
  columns: readonly OptionalColumn[];
  onToggle(column: OptionalColumn): void;
  onClose(): void;
}) {
  return (
    <>
      <div onClick={onClose} style={{ position: "fixed", inset: 0, zIndex: 70 }} />
      <div
        className="rise"
        data-testid="column-menu"
        role="group"
        aria-label="Columns"
        style={{
          position: "absolute",
          top: "calc(100% + 5px)",
          right: 0,
          zIndex: 80,
          minWidth: 190,
          background: S.overlay,
          borderRadius: R.group,
          boxShadow: SHADOW.overlay,
          padding: 5,
        }}
      >
        {OPTIONAL.map((c) => (
          <button
            key={c}
            onClick={() => onToggle(c)}
            style={{
              display: "flex",
              alignItems: "center",
              gap: 8,
              width: "100%",
              border: "none",
              background: "transparent",
              borderRadius: 6,
              padding: "5px 8px",
              textAlign: "left",
              ...F.body,
              color: T.secondary,
              cursor: "default",
            }}
          >
            <span style={{ width: 12, color: ACCENT }}>{columns.includes(c) ? "✓" : ""}</span>
            {COLUMN_LABEL[c]}
          </button>
        ))}
      </div>
    </>
  );
}

// --- inspector -------------------------------------------------------------

/** The commands this workload is a legitimate target for.
 *
 *  Deliberately narrow. Docker Desktop offers start/stop/restart on any
 *  container because it is a Docker client; this app's contract is that every
 *  action is a `curie` command you can read and copy, so a container with no
 *  `curie` equivalent gets no button rather than a raw `docker stop`. */
/** Which of the inspector surface's commands apply to the container in front of
 *  you. The labels and the tones come from the placement map -- this only picks
 *  the subset, because "what applies to a runner" is a fact about the sample,
 *  not about the command. */
function actionsFor(sample: ResourceSample) {
  const surface = surfacesById.get("resources.inspect")!;
  const wanted =
    sample.role === "runner"
      ? ["skill.status", "skill.message", "skill.down"]
      : sample.service
        ? ["local.status", "local.rebuild", "local.down"]
        : ["local.status"];
  return resolve(surface).filter(({ action }) => wanted.includes(action.id));
}

function InspectSheet({ sample, onClose }: { sample: ResourceSample; onClose(): void }) {
  const res = useResources();
  const app = useApp();
  const runs = useRuns();
  const history = res.history.get(sample.name);
  // A command form opened from inside this sheet takes over; two stacked sheets
  // is a modal over a modal, and the one being read is the form. The inspector
  // is still here underneath and comes back when the form closes.
  const covered = !!app.runTarget;
  const [logs, setLogs] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const loadLogs = async () => {
    setLoading(true);
    try {
      setLogs(await bridge().resources.logs(sample.id, 400));
    } finally {
      setLoading(false);
    }
  };

  if (covered) return null;

  return (
    <Sheet
      title={<Mono>{sample.name}</Mono>}
      onClose={onClose}
      width={720}
      footer={
        <>
          <Button onClick={loadLogs} busy={loading}>
            {logs === null ? "Load logs" : "Reload logs"}
          </Button>
          {sample.role === "runner" ? (
            <Button
              tone="danger"
              onClick={() => {
                void runs.start({ action: "skill.down", flags: { name: sample.name } });
                onClose();
              }}
            >
              curie skill down
            </Button>
          ) : null}
        </>
      }
    >
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginBottom: 16 }}>
        <div style={{ background: S.raised, borderRadius: R.group, padding: "11px 13px" }}>
          <div style={{ ...F.caption, color: T.tertiary, marginBottom: 3 }}>CPU</div>
          <div style={{ fontSize: 20, fontWeight: 600, color: ACCENT }}>
            {percent(sample.cpuPercent)}
          </div>
        </div>
        <div style={{ background: S.raised, borderRadius: R.group, padding: "11px 13px" }}>
          <div style={{ ...F.caption, color: T.tertiary, marginBottom: 3 }}>Memory</div>
          <div style={{ fontSize: 20, fontWeight: 600 }}>{bytes(sample.memBytes)}</div>
          <div style={{ ...F.footnote, color: T.quaternary }}>
            {sample.memLimitBytes ? `of ${bytes(sample.memLimitBytes)}` : "no limit set"}
          </div>
        </div>
      </div>

      <SectionHeader>CPU history</SectionHeader>
      <Group style={{ padding: 12, marginBottom: 16 }}>
        <FitWidth height={64}>
          {(w) => (
            <Sparkline
              values={history?.cpu ?? []}
              width={w}
              height={64}
              color={roleColor(sample.role)}
            />
          )}
        </FitWidth>
      </Group>

      <SectionHeader>Details</SectionHeader>
      <Group style={{ padding: 12, marginBottom: 16 }}>
        <div style={{ display: "grid", gap: 6 }}>
          <Detail label="Role" value={<Badge color={roleColor(sample.role)} filled>{sample.role}</Badge>} />
          <Detail
            label="Project"
            value={sample.project ?? <span style={{ color: T.quaternary }}>standalone</span>}
          />
          <Detail
            label="Agent"
            value={sample.agent ?? <span style={{ color: T.quaternary }}>not attributed</span>}
          />
          <Detail label="State" value={sample.state} />
          <Detail label="Image" value={<Mono style={{ fontSize: 11 }}>{sample.image ?? DASH}</Mono>} />
          <Detail label="Container" value={<Mono style={{ fontSize: 11 }}>{sample.id}</Mono>} />
          <Detail
            label="Ports"
            value={
              sample.ports.length ? (
                <Mono style={{ fontSize: 11 }}>
                  {sample.ports
                    .map((p) => (p.host ? `${p.host}→${p.container}/${p.proto}` : `${p.container}/${p.proto} (exposed)`))
                    .join(", ")}
                </Mono>
              ) : (
                <span style={{ color: T.quaternary }}>none published</span>
              )
            }
          />
          <Detail
            label="Block I/O"
            value={`${bytes(sample.blockReadBytes)} read / ${bytes(sample.blockWriteBytes)} written`}
          />
          <Detail label="Started" value={sample.startedAt ?? DASH} />
        </div>
      </Group>

      <SectionHeader>Run against this</SectionHeader>
      <Group style={{ padding: 8, marginBottom: 16 }}>
        {/* Prefilled with the service this container actually is, so
            "Rebuild" on the api row opens `curie local rebuild api` rather than
            a form asking which service you meant. */}
        <ActionButtons
          actions={actionsFor(sample)}
          prefill={sample.service ? { positionals: [sample.service] } : undefined}
        />
        <div style={{ ...F.footnote, color: T.quaternary, marginTop: 8, lineHeight: 1.5 }}>
          Only commands the CLI actually has. Raw container control — start, restart, remove — is
          not offered here on purpose: it has no <Mono style={{ fontSize: 10 }}>curie</Mono>
          equivalent, and Docker Desktop is better at it.
        </div>
      </Group>

      {logs !== null ? (
        <>
          <SectionHeader>Last 400 lines</SectionHeader>
          <Well style={{ maxHeight: 260, overflow: "auto" }}>
            <pre
              data-selectable
              style={{
                margin: 0,
                fontSize: 11,
                whiteSpace: "pre-wrap",
                color: T.secondary,
                fontFamily: "inherit",
              }}
            >
              {logs || "(no output)"}
            </pre>
          </Well>
        </>
      ) : null}
    </Sheet>
  );
}

function Detail({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "96px 1fr", gap: 10, ...F.callout }}>
      <span style={{ color: T.tertiary }}>{label}</span>
      <span style={{ color: T.secondary, minWidth: 0, wordBreak: "break-all" }}>{value}</span>
    </div>
  );
}
