// What the agents have actually been doing, and what it cost.
//
// Ported from the separate browser console so there is one set of screens
// rather than two that drift. It reads the platform API, so it works in a
// browser tab and in the desktop shell without knowing which it is in.

import { useCallback, useEffect, useState } from "react";

import { bridge } from "../bridge/bridge";
import { useApp } from "../bridge/app";
import { ACCENT, F, LINE, STATUS, T } from "../tokens";
import { Button, Group, Notice, SectionHeader, Stat, Stats } from "../primitives";
import { FitWidth, Sparkline } from "../primitives/charts";
import { duration } from "../lib/format";

/** The five the API will graph. Anything else is a 400 naming these. */
const METRICS = [
  { id: "runs", label: "Runs" },
  { id: "cost_usd", label: "Spend" },
  { id: "latency_p95_ms", label: "p95 latency" },
  { id: "tokens", label: "Tokens" },
  { id: "error_rate", label: "Errors" },
] as const;

type MetricId = (typeof METRICS)[number]["id"];

interface Summary {
  runs: number;
  latency_p95_ms: number;
  tokens: number;
  cost_usd: number;
  cost_known: boolean;
  error_rate: number;
}

interface SeriesPoint {
  ts: string;
  value: number;
}

interface Series {
  metric: string;
  points: SeriesPoint[];
}

interface Trace {
  id: string;
  name: string;
  timestamp: string;
  metadata?: { attributes?: Record<string, unknown> };
}

/** Formats a metric the way its unit wants, so one component can render all
 *  five without a caller passing a formatter each time. */
function show(metric: MetricId, value: number, costKnown = true): string {
  switch (metric) {
    case "cost_usd":
      return costKnown ? `$${value.toFixed(2)}` : "unknown";
    case "latency_p95_ms":
      return duration(value);
    case "error_rate":
      return `${(value * 100).toFixed(1)}%`;
    case "tokens":
      return value >= 1000 ? `${(value / 1000).toFixed(1)}k` : String(value);
    default:
      return value.toLocaleString();
  }
}

export function Observability() {
  const app = useApp();
  const reachable = !!app.api?.reachable;
  const authorized = !!app.api?.hasKey;

  const [summary, setSummary] = useState<Summary | null>(null);
  const [series, setSeries] = useState<Series | null>(null);
  const [traces, setTraces] = useState<Trace[] | null>(null);
  const [metric, setMetric] = useState<MetricId>("runs");
  const [error, setError] = useState<string | null>(null);

  // A counter rather than a callback in the dependency list: a `refresh` the
  // button calls and the effect watches keeps every write to state inside the
  // effect, which is what the hooks rule is protecting.
  const [nonce, setNonce] = useState(0);
  const refresh = useCallback(() => setNonce((n) => n + 1), []);

  useEffect(() => {
    if (!reachable || !authorized) return;
    let cancelled = false;
    const load = async () => {
      const [s, ser, tr] = await Promise.all([
        bridge().api.request<Summary>({ method: "GET", path: "/observability/metrics/summary" }),
        bridge().api.request<Series>({
          method: "GET",
          path: "/observability/metrics/series",
          query: { metric, granularity: "hour" },
        }),
        bridge().api.request<Trace[]>({
          method: "GET",
          path: "/langfuse/traces",
          query: { limit: 25 },
        }),
      ]);
      if (cancelled) return;
      if (s.ok) setSummary(s.body);
      if (ser.ok) setSeries(ser.body);
      if (tr.ok && Array.isArray(tr.body)) setTraces(tr.body);
      // Only the summary is load-bearing here. Traces come from Langfuse and can
      // be down on their own, which is not the same as the platform being down
      // and should not blank the page.
      setError(s.ok ? null : "Could not read the metrics summary.");
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, [reachable, authorized, metric, nonce]);

  if (!reachable) {
    return (
      <section>
        <SectionHeader>What has been happening</SectionHeader>
        <Notice tone="warn" title="Curie is not reachable">
          Start it from the Overview and this will fill in.
        </Notice>
      </section>
    );
  }

  if (!authorized) {
    return (
      <section>
        <SectionHeader>What has been happening</SectionHeader>
        <Notice tone="warn" title="Not signed in">
          Run <strong>curie local console login</strong> and sign in from the toolbar to see
          traces and spend.
        </Notice>
      </section>
    );
  }

  const values = (series?.points ?? []).map((p) => p.value);

  return (
    <section style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <div>
        <SectionHeader right={<Button size="sm" onClick={refresh}>Refresh</Button>}>
          Last seven days
        </SectionHeader>
        <Stats>
          <Stat first label="Runs" value={summary ? show("runs", summary.runs) : "—"} />
          <Stat
            label="Spend"
            value={summary ? show("cost_usd", summary.cost_usd, summary.cost_known) : "—"}
            sub={summary && !summary.cost_known ? "no price for this model" : undefined}
          />
          <Stat
            label="p95 latency"
            value={summary ? show("latency_p95_ms", summary.latency_p95_ms) : "—"}
          />
          <Stat
            label="Errors"
            value={summary ? show("error_rate", summary.error_rate) : "—"}
            accent={summary && summary.error_rate > 0 ? STATUS.danger : undefined}
          />
        </Stats>
      </div>

      <div>
        <SectionHeader
          right={
            <span style={{ display: "flex", gap: 4 }}>
              {METRICS.map((m) => (
                <Button
                  key={m.id}
                  size="sm"
                  tone={m.id === metric ? "primary" : "plain"}
                  onClick={() => setMetric(m.id)}
                >
                  {m.label}
                </Button>
              ))}
            </span>
          }
        >
          By hour
        </SectionHeader>
        <Group style={{ padding: 14 }}>
          {values.length ? (
            <FitWidth height={90}>
              {(w) => <Sparkline values={values} width={w} height={90} color={ACCENT} />}
            </FitWidth>
          ) : (
            <div style={{ ...F.footnote, color: T.tertiary }}>Nothing recorded in this window.</div>
          )}
        </Group>
      </div>

      <div>
        <SectionHeader
          right={
            traces?.length ? (
              <span style={{ ...F.footnote, color: T.quaternary }}>{traces.length} most recent</span>
            ) : null
          }
        >
          Recent activity
        </SectionHeader>
        <Group>
          {traces === null ? (
            <div style={{ padding: "12px 14px", ...F.footnote, color: T.tertiary }}>Loading…</div>
          ) : traces.length === 0 ? (
            <div style={{ padding: "12px 14px", ...F.footnote, color: T.tertiary }}>
              No traces yet. Send an agent a message and it will show up here.
            </div>
          ) : (
            traces.map((t, i) => <TraceRow key={t.id} trace={t} first={i === 0} />)
          )}
        </Group>
      </div>

      {error ? <Notice tone="error" title="Could not load">{error}</Notice> : null}
    </section>
  );
}

function TraceRow({ trace, first }: { readonly trace: Trace; readonly first: boolean }) {
  const attrs = trace.metadata?.attributes ?? {};
  const service = typeof attrs["service.name"] === "string" ? attrs["service.name"] : null;
  const when = new Date(trace.timestamp);
  return (
    <div
      style={{
        display: "flex",
        alignItems: "baseline",
        gap: 10,
        padding: "9px 14px",
        borderTop: first ? "none" : `1px solid ${LINE.separator}`,
      }}
    >
      <span style={{ ...F.body, color: T.primary, flex: 1, minWidth: 0 }}>{trace.name}</span>
      {service ? (
        <span style={{ ...F.footnote, color: T.tertiary }}>{service}</span>
      ) : null}
      <span
        style={{ ...F.footnote, color: T.quaternary, fontVariantNumeric: "tabular-nums" }}
        title={when.toISOString()}
      >
        {when.toLocaleTimeString()}
      </span>
    </div>
  );
}
