// Overview: the state of things, ordered by urgency.
//
// Anything blocked on a human comes first, then anything broken, then the
// steady-state picture. A dashboard that puts a chart above a stuck approval has
// its priorities backwards.
//
// The view does not render its own title -- the toolbar owns that. A page that
// repeats its own name under the window's title bar is a web habit.

import { useCallback, useEffect, useState } from "react";

import { useApp, type AgentSummary } from "../bridge/app";
import { useResources } from "../bridge/resources";
import { useRuns } from "../bridge/runs";
import { bridge } from "../bridge/bridge";
import { ago, bytes, count, duration, percent, usd } from "../lib/format";
import { ACCENT, F, STATUS, T } from "../tokens";
import { FitWidth, RankedBars, Sparkline } from "../primitives/charts";
import {
  Badge,
  Button,
  Dot,
  EmptyState,
  Group,
  Mono,
  Notice,
  Row,
  SectionHeader,
  Stat,
} from "../primitives";

interface MetricsSummary {
  runs: number;
  latency_p95_ms: number;
  tokens: number;
  cost_usd: number;
  cost_known: boolean;
  error_rate: number;
}

interface ApprovalOut {
  id: string;
  agent_id?: string;
  tool?: string;
  status?: string;
  created_at?: string;
}

export function Overview() {
  const app = useApp();
  const res = useResources();
  const runs = useRuns();

  const [metrics, setMetrics] = useState<MetricsSummary | null>(null);
  const [approvals, setApprovals] = useState<readonly ApprovalOut[]>([]);
  const [nonce, setNonce] = useState(0);
  const refresh = useCallback(() => setNonce((n) => n + 1), []);
  const reachable = !!app.api?.reachable;

  useEffect(() => {
    if (!reachable) return;
    let cancelled = false;
    const load = async () => {
      const [m, a] = await Promise.all([
        bridge().api.request<MetricsSummary>({
          method: "GET",
          path: "/observability/metrics/summary",
        }),
        bridge().api.request<ApprovalOut[]>({
          method: "GET",
          path: "/approvals",
          query: { status: "pending" },
        }),
      ]);
      if (cancelled) return;
      setMetrics(m.ok ? m.body : null);
      setApprovals(a.ok && Array.isArray(a.body) ? a.body : []);
    };
    void load();
    const t = setInterval(() => void load(), 20_000);
    return () => {
      cancelled = true;
      clearInterval(t);
    };
  }, [reachable, nonce]);

  // Sum the per-runner traces into one machine-wide line.
  const runnerSeries = res.samples
    .filter((s) => s.role === "runner")
    .map((s) => res.history.get(s.name)?.cpu ?? []);
  const runnerCpu = runnerSeries.length
    ? Array.from({ length: Math.max(...runnerSeries.map((h) => h.length)) }, (_, i) =>
        runnerSeries.reduce((sum, h) => sum + (h[i] ?? 0), 0),
      )
    : [];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
      <Blockers approvals={approvals} />
      <Health onRefresh={refresh} />

      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 10 }}>
        <Stat
          label="Agents"
          value={app.agents.length}
          sub={reachable ? "deployed identities" : "API unreachable"}
        />
        <Stat
          label="Runners live"
          value={res.samples.filter((s) => s.role === "runner" && s.state === "running").length}
          sub="sandboxes on this machine"
          accent={ACCENT}
        />
        <Stat
          label="Spend"
          value={metrics ? (metrics.cost_known ? usd(metrics.cost_usd) : "unknown") : "—"}
          sub={metrics && !metrics.cost_known ? "no price row for this model" : "from Langfuse"}
        />
        <Stat
          label="p95 latency"
          value={metrics ? duration(metrics.latency_p95_ms) : "—"}
          sub={metrics ? `${count(metrics.runs)} runs` : "unavailable"}
        />
      </div>

      <div
        // `stretch`, so the two cards in this row are the same height. With
        // `start` each sized to its own content, and a sparkline next to a
        // seven-row bar list left the shorter card floating in dead pane. The
        // sections are flex columns and the cards take the remaining height, so
        // the taller content sets the row and both cards meet at the bottom.
        style={{ display: "grid", gridTemplateColumns: "1.35fr 1fr", gap: 18, alignItems: "stretch" }}
      >
        <section style={{ display: "flex", flexDirection: "column" }}>
          <SectionHeader
            right={
              <Button size="sm" tone="plain" onClick={() => app.navigate("resources")}>
                Resources
              </Button>
            }
          >
            Runner CPU
          </SectionHeader>
          {/* A column, so the totals sit on the card's bottom edge like a footer
              and the chart or its empty state takes the rest. The card is as tall
              as the bar list beside it, and without this the content bunched at
              the top and left the height it had been given unused. */}
          <Group
            style={{ padding: 14, flex: 1, display: "flex", flexDirection: "column" }}
          >
            {runnerCpu.length ? (
              <FitWidth height={96}>
                {(w) => <Sparkline values={runnerCpu} width={w} height={96} color={ACCENT} />}
              </FitWidth>
            ) : (
              // The centring wrapper is separate from the sentence on purpose: a
              // flex container makes each child its own item, which trims the
              // space between the text and the <Mono> and reads as "withcurie".
              <div
                style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center" }}
              >
                <div style={{ ...F.callout, color: T.tertiary, textAlign: "center" }}>
                  No runner sandboxes are running. Start one with <Mono>curie skill up</Mono>.
                </div>
              </div>
            )}
            <div
              style={{
                display: "flex",
                gap: 18,
                marginTop: "auto",
                paddingTop: 12,
                ...F.footnote,
                color: T.tertiary,
                fontVariantNumeric: "tabular-nums",
              }}
            >
              <span>
                CPU{" "}
                <Mono style={{ color: T.secondary, fontSize: 11 }}>
                  {percent(res.totals.cpu, 0)}
                </Mono>
              </span>
              <span>
                Memory{" "}
                <Mono style={{ color: T.secondary, fontSize: 11 }}>{bytes(res.totals.mem)}</Mono>
              </span>
              <span>
                Containers{" "}
                <Mono style={{ color: T.secondary, fontSize: 11 }}>{res.totals.running}</Mono>
              </span>
            </div>
          </Group>
        </section>

        <section style={{ display: "flex", flexDirection: "column" }}>
          <SectionHeader
            right={
              <Button size="sm" tone="plain" onClick={() => app.navigate("canvas")}>
                Canvas
              </Button>
            }
          >
            Memory by workload
          </SectionHeader>
          <Group style={{ padding: 14, flex: 1 }}>
            {res.samples.length ? (
              <RankedBars
                rows={res.samples
                  .filter((s) => s.memBytes)
                  .sort((a, b) => (b.memBytes ?? 0) - (a.memBytes ?? 0))
                  .slice(0, 7)
                  .map((s) => ({ label: s.name, value: s.memBytes ?? 0 }))}
                format={(v) => bytes(v)}
              />
            ) : (
              <div
                style={{ ...F.callout, color: T.tertiary, padding: "34px 0", textAlign: "center" }}
              >
                Nothing running.
              </div>
            )}
          </Group>
        </section>
      </div>

      <Agents />

      {runs.runs.length ? (
        <section>
          <SectionHeader
            right={
              <Button size="sm" tone="plain" onClick={() => app.navigate("activity")}>
                All activity
              </Button>
            }
          >
            Recent commands
          </SectionHeader>
          <Group>
            {runs.runs.slice(0, 5).map((r, i) => (
              <Row
                key={r.id}
                first={i === 0}
                onClick={() => {
                  runs.focus(r.id);
                  runs.setDrawerOpen(true);
                }}
              >
                <Dot
                  color={
                    r.state === "ok"
                      ? STATUS.ok
                      : r.state === "failed"
                        ? STATUS.danger
                        : r.state === "running"
                          ? ACCENT
                          : T.tertiary
                  }
                  pulse={r.state === "running"}
                />
                <Mono
                  style={{
                    flex: 1,
                    color: T.secondary,
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                  }}
                >
                  {r.command.display}
                </Mono>
                <span style={{ ...F.footnote, color: T.tertiary }}>{ago(r.startedAt)}</span>
              </Row>
            ))}
          </Group>
        </section>
      ) : null}
    </div>
  );
}

/** Work that is stopped waiting for a person. The one panel allowed above
 *  everything unconditionally: an approval nobody notices is the failure mode
 *  that costs the most. */
function Blockers({ approvals }: { approvals: readonly ApprovalOut[] }) {
  const app = useApp();
  if (!approvals.length) return null;
  return (
    <Notice
      tone="warn"
      title={`${approvals.length} approval${approvals.length === 1 ? "" : "s"} waiting on a human`}
      action={
        <Button size="sm" onClick={() => app.navigate("commands", "local.approvals")}>
          Resolve
        </Button>
      }
    >
      {approvals
        .slice(0, 3)
        .map((a) => a.tool ?? a.id)
        .join(", ")}
      {approvals.length > 3 ? `, and ${approvals.length - 3} more` : ""}. An agent is paused until
      each of these is answered.
    </Notice>
  );
}

/** What is broken on this machine, with the command that fixes it. */
function Health({ onRefresh }: { onRefresh(): void }) {
  const app = useApp();
  const env = app.env;
  if (!env) return null;

  const issues: { text: string; fix?: string; label?: string }[] = [];
  if (!env.cliPath) {
    issues.push({ text: "The curie binary is not on PATH, so this app cannot run anything." });
  }
  if (!env.dockerAvailable) {
    issues.push({ text: "Docker is not reachable: the skill and local tiers cannot start." });
  }
  if (app.api && !app.api.reachable && app.api.baseUrl) {
    issues.push({
      text: `The platform API at ${app.api.baseUrl} is not answering. Agents, versions, memory and traces are unavailable until it is.`,
      fix: "local.up",
      label: "Start the stack",
    });
  }
  if (app.agentsError && app.api?.reachable) {
    issues.push({ text: `Reached the API but could not list agents: ${app.agentsError}` });
  }
  if (!issues.length) return null;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      {issues.map((issue, i) => (
        <Notice
          key={i}
          tone="error"
          action={
            <Button
              size="sm"
              onClick={() => (issue.fix ? app.navigate("commands", issue.fix) : onRefresh())}
            >
              {issue.label ?? "Recheck"}
            </Button>
          }
        >
          {issue.text}
        </Notice>
      ))}
    </div>
  );
}

function Agents() {
  const app = useApp();
  const res = useResources();

  if (!app.api?.reachable) {
    return (
      <section>
        <SectionHeader>Agents</SectionHeader>
        <Group style={{ padding: 14 }}>
          <div style={{ ...F.callout, color: T.tertiary }}>
            Agents live in the platform API. Point this app at one in Settings, or bring the local
            stack up with <Mono>curie local up</Mono>.
          </div>
        </Group>
      </section>
    );
  }

  if (!app.agents.length) {
    return (
      <Group>
        <EmptyState
          title="No agents deployed yet"
          action={
            <Button tone="primary" onClick={() => app.navigate("commands", "local.deploy")}>
              Deploy a bundle
            </Button>
          }
        >
          The API is reachable and reports no agents. Deploy the bundle you have open with{" "}
          <Mono>curie local deploy</Mono>.
        </EmptyState>
      </Group>
    );
  }

  return (
    <section>
      <SectionHeader>Agents</SectionHeader>
      <Group>
        {app.agents.map((agent, i) => (
          <AgentRow key={agent.id} agent={agent} first={i === 0} samples={res.samples} />
        ))}
      </Group>
    </section>
  );
}

function AgentRow({
  agent,
  first,
  samples,
}: {
  agent: AgentSummary;
  first: boolean;
  samples: readonly { name: string; role: string; state: string }[];
}) {
  const app = useApp();
  const live = samples.some(
    (s) => s.role === "runner" && s.state === "running" && s.name.includes(agent.name),
  );

  return (
    <Row first={first} onClick={() => app.navigate("canvas")}>
      <Dot color={live ? ACCENT : T.quaternary} pulse={live} />

      <div style={{ width: 160, minWidth: 0 }}>
        <div style={{ ...F.headline }}>{agent.name}</div>
        <Mono style={{ fontSize: 10, color: T.tertiary }}>{agent.id.slice(0, 8)}</Mono>
      </div>

      <div style={{ width: 200 }}>
        {agent.channel?.channel_id ? (
          <Badge color={STATUS.warn} filled>
            {agent.channel.kind ?? "channel"} · {agent.channel.channel_id}
          </Badge>
        ) : (
          <span style={{ ...F.footnote, color: T.quaternary }}>no channel bound</span>
        )}
      </div>

      <div style={{ flex: 1, minWidth: 0 }}>
        <Mono style={{ fontSize: 11, color: T.secondary }}>{agent.model ?? "platform default"}</Mono>
        {agent.approval_required_tools?.length ? (
          <div style={{ ...F.footnote, color: STATUS.warn, marginTop: 1 }}>
            {agent.approval_required_tools.length} approval gate
            {agent.approval_required_tools.length === 1 ? "" : "s"}
          </div>
        ) : null}
      </div>

      <div style={{ display: "flex", gap: 6 }} onClick={(e) => e.stopPropagation()}>
        <Button size="sm" onClick={() => app.navigate("commands", "local.message")}>
          Message
        </Button>
        <Button size="sm" tone="plain" onClick={() => app.navigate("commands", "local.memory")}>
          Memory
        </Button>
      </div>
    </Row>
  );
}
