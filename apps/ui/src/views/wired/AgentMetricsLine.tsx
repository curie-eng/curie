import { C } from "../../tokens";
import { useMetricsSummary } from "../../api/hooks";
import type { AgentOut } from "../../api/client";

// Shared by the Agents page and the Overview agents card. Its own module so the
// Overview (the landing chunk) does not pull in the deferred Agents view.

// Default observability window is metrics_default_window_hours = 168h (7 days);
// see apps/api/src/curie_api/routers/observability.py _resolve_window.
const METRICS_WINDOW_LABEL = "last 7d";

// One card's metrics fetch, isolated so each card owns its own hook call and
// its own loading/error state.
export function AgentMetricsLine({ agent }: { agent: AgentOut }) {
  // No `environment` filter: runner traces carry no deployment environment
  // today, so an environment filter matches nothing; the agent trace-name
  // filter already scopes the query to this agent (mirrors agent_trace_filter
  // in apps/api/src/curie_api/metrics.py).
  const summary = useMetricsSummary(true, { agent: `agent-${agent.id}` });
  const s = summary.data;
  // A telemetry fetch failure (or loading) must not read as "0 runs" — show a
  // neutral dash so an observability outage is never reported as no traffic
  // (mirrors apps/ui/src/views/wired/WiredOverview.tsx).
  const runs = s ? s.runs.toLocaleString() : "—";
  const tokens = s ? s.tokens.toLocaleString() : "—";
  const cost = !s || s.cost_known === false ? "—" : `$${s.cost_usd.toFixed(2)}`;
  return (
    <span style={{ fontSize: 12, color: C.muted, fontFamily: C.mono }}>
      {`${runs} runs · ${tokens} tokens · ${cost} (${METRICS_WINDOW_LABEL})`}
    </span>
  );
}
