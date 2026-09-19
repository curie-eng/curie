// Overview: the state of things, ordered by urgency.
//
// Anything blocked on a human comes first, then anything broken, then the
// steady-state picture. A dashboard that puts a chart above a stuck approval has
// its priorities backwards.
//
// The view does not render its own title -- the toolbar owns that. A page that
// repeats its own name under the window's title bar is a web habit.

import { useCallback, useEffect, useState } from "react";
import { channelLabel, primaryChannel } from "../lib/channels";

import { useApp, type AgentSummary } from "../bridge/app";
import { useResources } from "../bridge/resources";
import { useRuns } from "../bridge/runs";
import { bridge } from "../bridge/bridge";
import { ago, bytes, count, DASH, duration, percent, usd } from "../lib/format";
import { ACCENT, F, M, STATUS, T } from "../tokens";
import { stackPhase, stackProgress } from "../lib/startup";
import { FitWidth, RankedBars, Sparkline, UsageBar } from "../primitives/charts";
import { RunButton } from "./Actions";
import { useAgentSheet } from "./AgentSheet";
import { LadderStrip } from "./Tiers";
import {
  Badge,
  Button,
  Dot,
  EmptyState,
  Group,
  Mono,
  Notice,
  LiveRing,
  Row,
  SectionHeader,
  Spinner,
  Stat,
  Stats,
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

  // `null` (no worker to ask) is deliberately NOT falsy-collapsed into "real":
  // a lookup that failed must not make a priced figure look trustworthy.
  const fakeModel = res.frame?.fakeModel === true;

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

      <Stats>
        <Stat
          first
          label="Agents"
          value={app.agents.length}
          sub={reachable ? "put to work" : "cannot reach Curie"}
        />
        <Stat
          label="Test copies"
          value={res.samples.filter((s) => s.role === "runner" && s.state === "running").length}
          sub="running on this computer"
          accent={ACCENT}
        />
        {/* Two things this stat must not do, both of which it did.
            
            It must not report a number as money when no money moved. Langfuse
            prices observations from token counts and a price row for the model
            name, and it does that whether or not a request ever left the
            machine -- so a stack pinned to the offline fake model reported
            $0.04 of spend that had not happened. `fakeModel` comes off the
            worker's own environment, and `null` (no worker to ask) is treated
            as "cannot say", never as "real".

            And its caption must not name the SOURCE where a reader expects the
            payee. "$0.04 / from Langfuse" reads as Langfuse having charged you
            four cents; somebody read it exactly that way. The caption says what
            the figure covers now, and the source moved into the tooltip. */}
        <Stat
          label="Spend"
          value={
            fakeModel
              ? "none"
              : metrics
                ? metrics.cost_known
                  ? usd(metrics.cost_usd)
                  : "unknown"
                : DASH
          }
          sub={
            fakeModel
              ? "fake model — nothing billed"
              : metrics
                ? metrics.cost_known
                  ? "model usage, last 7 days"
                  : "no price row for this model"
                : "unavailable"
          }
          title={
            fakeModel
              ? `The worker is pinned to the offline fake model, so no request reaches a provider. Langfuse still prices the token counts, which is where the ${metrics ? usd(metrics.cost_usd) : "figure"} it reports comes from.`
              : "Summed from Langfuse observations over the last 7 days."
          }
        />
        <Stat
          label="p95 latency"
          value={metrics ? duration(metrics.latency_p95_ms) : "—"}
          sub={metrics ? `${count(metrics.runs)} runs` : "unavailable"}
        />
      </Stats>

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
              // An empty chart card is the biggest block of dead space on this
              // screen, so it carries the thing you would do about it rather
              // than a sentence describing the absence. The centring wrapper is
              // separate from the sentence on purpose: a flex container makes
              // each child its own item, which trims the space between the text
              // and the <Mono> and reads as "withcurie".
              <div
                style={{
                  flex: 1,
                  display: "flex",
                  flexDirection: "column",
                  alignItems: "center",
                  justifyContent: "center",
                  gap: 10,
                }}
              >
                {/* `skill up` snapshots the directory it is invoked in, so with
                    no bundle open it would boot a runner over the fallback
                    directory and produce a container with nothing in it. The
                    empty state's own action must not be a dead end: with
                    nothing open this names what is missing and goes to Build. */}
                <div style={{ ...F.callout, color: T.tertiary, textAlign: "center" }}>
                  {app.workspace
                    ? "No test copies running on this computer."
                    : "Nothing running, and no agent open to try."}
                </div>
                {app.workspace ? (
                  <RunButton id="skill.up" tone="primary">
                    Boot a runner
                  </RunButton>
                ) : (
                  <Button size="sm" onClick={() => app.navigate("build")}>
                    Open a bundle
                  </Button>
                )}
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

      <LadderStrip />

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
                  runs.setConsoleOpen(true);
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
  if (!approvals.length) return null;
  return (
    <Notice
      tone="warn"
      title={`${approvals.length} approval${approvals.length === 1 ? "" : "s"} waiting on a human`}
      action={<RunButton id="local.approvals">Resolve</RunButton>}
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
  const res = useResources();
  const runs = useRuns();
  const env = app.env;

  const progress = stackProgress(res.samples);

  // When everything went ready, so the settling grace period can be bounded.
  //
  // The clock is the resource FRAME's own timestamp, not `Date.now()`. Two
  // reasons, and the second is the real one: `Date.now()` in render is impure
  // and the hook lint rejects it, but more importantly the frame's `at` is the
  // clock this measurement actually runs on -- the poll is what re-renders this
  // component, so the deadline can only ever be noticed on a frame boundary
  // anyway. Adjusted during render rather than in an effect, per the app's
  // rule; an effect would show one frame of the wrong answer first.
  const now = res.frame?.at ?? 0;
  const allReady = progress.total > 0 && progress.ready === progress.total;
  const [readyAt, setReadyAt] = useState<number | null>(null);
  if (allReady && readyAt === null) setReadyAt(now);
  if (!allReady && readyAt !== null) setReadyAt(null);

  const phase = stackPhase(progress, {
    apiReachable: !!app.api?.reachable,
    runActive: runs.active.some((r) => r.action === "local.up" || r.action === "local.rebuild"),
    settlingForMs: readyAt === null ? 0 : Math.max(0, now - readyAt),
  });

  if (!env) return null;

  const issues: {
    text: string;
    fix?: string;
    label?: string;
    goto?: "settings";
    /** Defaults to `error`. A thing that narrows what the app can show is not
     *  the same as a thing that stops it working, and painting both red makes
     *  the difference invisible at the moment it matters most. */
    tone?: "error" | "warn";
  }[] = [];
  if (!env.cliPath) {
    issues.push({ text: "The curie binary is not on PATH, so this app cannot run anything." });
  }
  if (!env.dockerAvailable) {
    issues.push({ text: "Docker is not reachable: the skill and local tiers cannot start." });
  }
  // The API being down is only an ERROR when nothing is being done about it.
  // While the stack is coming up the same fact is progress, and `StackStarting`
  // says it that way -- with a spinner rather than a red glyph, because a
  // failure mark standing over a working process is the screen calling its own
  // work broken.
  const showStack = phase !== "idle" && phase !== "absent";
  if (app.api && !app.api.reachable && app.api.baseUrl && !showStack && phase !== "absent") {
    issues.push({
      text: `The platform API at ${app.api.baseUrl} is not answering. Agents, versions, memory and traces are unavailable until it is.`,
      fix: "local.up",
      label: "Start the stack",
    });
  }
  if (app.agentsError && app.api?.reachable) {
    // A 401 is not a fault to recheck, it is a missing credential, and offering
    // "Recheck" for it sends the operator round a loop that cannot terminate.
    // Name the actual problem and open the place it is fixed.
    const unauthorized = /\b40[13]\b|unauthorized|forbidden/i.test(app.agentsError);
    issues.push(
      unauthorized
        ? {
            // A warning, not an error, and the wording matters as much as the
            // colour. The platform is up and agents run without this app having
            // a key -- a bot answering in Slack does not care what this window
            // can list. All that is missing is THIS app's read access to the
            // agent list, so it must not be painted as the stack being broken.
            tone: "warn",
            // No "the stack is up" here: the card directly above says exactly
            // that, and a notice repeating the line it sits under reads as two
            // systems that have not been introduced.
            text: `This app has no API key for ${app.api.baseUrl}, so it cannot list agents, versions or memory here. Agents themselves are unaffected — they do not need this app to have a key.`,
            label: "Add an API key",
            goto: "settings",
          }
        : { text: `Reached the API but could not list agents: ${app.agentsError}` },
    );
  }
  if (!issues.length && !showStack && phase !== "absent") return null;

  return (
    // The progress card is FIRST and is rendered independently of the issues
    // below it. A start in flight and a problem worth naming are both true at
    // once more often than not -- that is what a start is -- and letting either
    // one suppress the other means the screen answers "what is happening" with
    // only half of it.
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      {phase === "absent" ? <SetUp dockerAvailable={env.dockerAvailable} /> : null}
      {showStack ? (
        <StackCard phase={phase} progress={progress} apiBaseUrl={app.api?.baseUrl} />
      ) : null}
      {issues.map((issue, i) => (
        <Notice
          key={i}
          tone={issue.tone ?? "error"}
          action={
            issue.fix ? (
              <RunButton id={issue.fix}>{issue.label ?? "Fix"}</RunButton>
            ) : issue.goto ? (
              <Button size="sm" onClick={() => app.navigate(issue.goto!)}>
                {issue.label ?? "Open settings"}
              </Button>
            ) : (
              <Button size="sm" onClick={onRefresh}>
                {issue.label ?? "Recheck"}
              </Button>
            )
          }
        >
          {issue.text}
        </Notice>
      ))}
    </div>
  );
}

/**
 * First run: one button, and nothing else competing with it.
 *
 * Nothing is running, so every other panel on this page has an em dash in it
 * and none of them can be acted on. The only useful thing the app can say is
 * "press this", and saying it as one primary control -- rather than as a red
 * warning about an API that was never started -- is the difference between a
 * screen that reads as broken and one that reads as new.
 *
 * `local.up` is the whole setup: compose brings up the API, the worker, the
 * dispatcher, the object store and the rest, which is what a bundle gets
 * deployed onto. It goes through the same generated form as every other
 * command, so the flags are all still there for anyone who wants them -- the
 * button is a shortcut past the list, not a second way to run things.
 *
 * Docker is the one precondition this app cannot press a button about, so when
 * it is missing the card says that instead of offering an action that would
 * fail ten seconds later.
 */
function SetUp({ dockerAvailable }: { readonly dockerAvailable: boolean }) {
  const app = useApp();
  return (
    <Group style={{ display: "flex", gap: 12, alignItems: "flex-start", padding: "14px 14px" }}>
      <div style={{ flex: 1, minWidth: 0, display: "grid", gap: 4 }}>
        <div style={{ ...F.headline }}>
          {dockerAvailable ? "Set up Curie on this computer" : "Docker is not running"}
        </div>
        <div style={{ ...F.callout, color: T.secondary, lineHeight: 1.55, maxWidth: M.prose }}>
          {dockerAvailable ? (
            <>
              One button starts everything an agent needs to run here. Then{" "}
              <button
                onClick={() => app.navigate("build")}
                style={{
                  border: "none",
                  background: "transparent",
                  padding: 0,
                  font: "inherit",
                  color: ACCENT,
                  cursor: "default",
                }}
              >
                Build
              </button>{" "}
              is where you make one and put it to work.
            </>
          ) : (
            <>
              Curie runs its agents in containers, so Docker has to be running first. Start Docker
              Desktop and this card will offer the button.
            </>
          )}
        </div>
      </div>
      {dockerAvailable ? (
        <span style={{ flex: "none" }}>
          <RunButton id="local.up" tone="primary" size="md">
            Set up Curie
          </RunButton>
        </span>
      ) : null}
    </Group>
  );
}

/**
 * The local stack's state, as one card that stays put.
 *
 * It does not disappear when the stack comes up. That was the thing worth
 * changing: the only signal that a start had worked was a warning going away,
 * and a screen that reports success by removing something is asking you to have
 * been watching it. The card stays and the MARKER changes -- a spinner while
 * there is something to wait for, a slow ping once there is not. A spinner is a
 * promise that something will finish, so leaving one up after the work is done
 * says the opposite of the truth; a ping finishes nothing and is not trying to.
 *
 * Everything on it is measured. The bar is containers Docker reports ready over
 * containers compose has created, which is the same condition `compose up
 * --wait` is itself blocking on, and the step line names the services actually
 * outstanding. Nothing advances because time passed.
 *
 * There is no error glyph in any of these states. A red mark standing over a
 * process that is working is the screen calling its own work broken, and it is
 * what made this read as a failure for the whole minute a start takes.
 */
function StackCard({
  phase,
  progress,
  apiBaseUrl,
}: {
  readonly phase: "starting" | "settling" | "up";
  readonly progress: ReturnType<typeof stackProgress>;
  readonly apiBaseUrl: string | undefined;
}) {
  const { total, ready, waiting, failed } = progress;
  const up = phase === "up";
  const color = failed.length ? STATUS.warn : ACCENT;

  const title = failed.length
    ? "Something is wrong"
    : up
      ? "Curie is running"
      : "Starting Curie";

  const detail = failed.length
    ? `${failed.join(", ")} ${failed.length === 1 ? "is" : "are"} not healthy — the console below has the details`
    : up
      ? `${total} ${total === 1 ? "piece" : "pieces"} running · ready for agents${apiBaseUrl ? ` at ${apiBaseUrl}` : ""}`
      : phase === "settling"
        ? "Everything has started. Waiting for it to answer."
        : waiting.length
          ? `Still starting: ${waiting.slice(0, 3).join(", ")}${waiting.length > 3 ? ` and ${waiting.length - 3} more` : ""}`
          : // Compose has created nothing yet, which means it is still pulling
            // images. That is the longest phase and the one with no output at
            // all, so it needs naming rather than leaving blank.
            "Downloading what it needs. This is the slow part the first time.";

  return (
    <Group style={{ display: "flex", gap: 10, alignItems: "flex-start", padding: "10px 12px" }}>
      {/* A 16px slot, whatever is in it. `Notice` gives its glyph exactly this,
          and without it the marker's own width set the text column: a 9px ring
          started the text 7px to the left of the notice stacked underneath, so
          two cards on the same screen had two left margins. Centring inside a
          16px box also puts the marker on the first line's optical middle
          without a hand-tuned `marginTop` per state. */}
      <span
        style={{
          flex: "none",
          width: 16,
          height: 16,
          marginTop: 1,
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        {up ? <LiveRing color={color} /> : <Spinner size={14} color={color} />}
      </span>
      {/* 6 when the bar is there and needs air on both sides of it, 2 when this
          is two lines of prose -- at 6 they read as unrelated. */}
      <div style={{ flex: 1, minWidth: 0, display: "grid", gap: up ? 2 : 6 }}>
        <div style={{ ...F.headline }}>{title}</div>

        {/* The bar belongs to the wait. Once there is nothing left to wait for
            it is a full bar reporting that a finished thing is finished, so it
            goes and the count moves into the line below. */}
        {up ? null : (
          // `warnAt` is null on purpose: `total` is a target to reach, not a
          // ceiling to stay under, and amber at 85% would warn that the stack
          // is nearly up.
          <UsageBar
            value={total ? ready : null}
            max={total || null}
            height={4}
            warnAt={null}
            color={color}
            title={total ? `${ready} of ${total} containers ready` : "Nothing created yet"}
          />
        )}

        <div style={{ ...F.footnote, color: T.tertiary }}>
          {!up && total ? `${ready} of ${total} ready · ` : ""}
          {detail}
        </div>
      </div>

      {/* Taking the stack down belongs on the card that says it is up: that is
          where somebody is standing when they decide they are done with it, and
          sending them to Tiers to find the same command is the "go and look for
          it yourself" answer this app is built against. Nothing about the
          placement makes it easier to fire -- `local.down` is destructive, so
          the button opens the same generated form with the same confirm gate as
          everywhere else, and it is painted the same red it wears on Tiers. */}
      {total ? (
        <span style={{ flex: "none" }}>
          <RunButton id="local.down" tone="danger">
            Shut it down
          </RunButton>
        </span>
      ) : null}
    </Group>
  );
}

function Agents() {
  const app = useApp();
  const res = useResources();
  // One sheet for the whole list. Opening it from a row is what gives the 26
  // agent-scoped commands somewhere to be that is not a search box.
  const sheet = useAgentSheet();

  if (!app.api?.reachable) {
    // With no containers at all, the setup card at the top of this page already
    // owns starting the stack, and offering the same command again here made
    // three green buttons compete on a screen whose whole point is that there
    // is one thing to do. Pointing at a different API stays either way -- that
    // is a genuinely different action, and it is the only one left when the
    // stack is not the problem.
    const firstRun = stackProgress(res.samples).total === 0;
    return (
      <section>
        <SectionHeader>Agents</SectionHeader>
        <Group style={{ padding: 14 }}>
          <div style={{ ...F.callout, color: T.tertiary, marginBottom: 10 }}>
            {firstRun
              ? "Agents live in the platform API. Start it with the button at the top of this page, or point this app at one that is already running."
              : "Agents live in the platform API, and this app is not pointed at one that answers."}
          </div>
          {/* Both ways out, rather than prose naming them. */}
          <div style={{ display: "flex", gap: 7, flexWrap: "wrap" }}>
            {firstRun ? null : (
              <RunButton id="local.up" tone="primary">
                Bring the local stack up
              </RunButton>
            )}
            <Button size="sm" onClick={() => app.navigate("settings")}>
              Point at an API
            </Button>
          </div>
        </Group>
      </section>
    );
  }

  if (!app.agents.length) {
    // "Deploy a bundle" with no bundle open is a button that opens a form you
    // cannot complete -- the empty state's own call to action being a dead end.
    // What is actually missing first is something to deploy, so with nothing
    // open this sends you to the tab that makes one.
    const hasBundle = !!app.workspace;
    return (
      <Group>
        <EmptyState
          title="Nothing running yet"
          action={
            // One destination, whether or not something happens to be open. This
            // panel is about the PLATFORM being empty; which agent you last had
            // open is incidental to that, and naming it here read as this screen
            // having opinions about a particular agent. Build is where an agent
            // is put to work, and it says there whether it already has been.
            <Button tone="primary" size="md" onClick={() => app.navigate("build")}>
              {hasBundle ? "Go to Build" : "Make an agent"}
            </Button>
          }
        >
          {hasBundle
            ? "Curie is running and has no agents on it. Build is where you send one — it will show up here once it is answering."
            : "Curie is running and has nothing on it yet. Build is where an agent is made, tried and sent — then it appears here."}
        </EmptyState>
      </Group>
    );
  }

  return (
    <section>
      <SectionHeader
        right={
          <span style={{ ...F.footnote, color: T.quaternary }}>
            a row opens everything you can do to it
          </span>
        }
      >
        Agents
      </SectionHeader>
      <Group>
        {app.agents.map((agent, i) => (
          <AgentRow
            key={agent.id}
            agent={agent}
            first={i === 0}
            samples={res.samples}
            onOpen={() => sheet.open(agent)}
          />
        ))}
      </Group>
      {sheet.element}
    </section>
  );
}

function AgentRow({
  agent,
  first,
  samples,
  onOpen,
}: {
  agent: AgentSummary;
  first: boolean;
  samples: readonly { name: string; role: string; state: string }[];
  onOpen(): void;
}) {
  const live = samples.some(
    (s) => s.role === "runner" && s.state === "running" && s.name.includes(agent.name),
  );

  // The row itself opens the agent, rather than jumping to the Canvas. A list
  // whose rows navigate somewhere the row is not the subject of is a list you
  // cannot act on.
  return (
    <Row first={first} onClick={onOpen}>
      <Dot color={live ? ACCENT : T.quaternary} pulse={live} />

      <div style={{ width: 160, minWidth: 0 }}>
        <div style={{ ...F.headline }}>{agent.name}</div>
        <Mono style={{ fontSize: 10, color: T.tertiary }}>{agent.id.slice(0, 8)}</Mono>
      </div>

      <div style={{ width: 200 }}>
        {channelLabel(agent) ? (
          <Badge color={STATUS.warn} filled>
            {channelLabel(agent)}
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

      {/* The two most common verbs inline, already pointed at this agent; the
          rest are one click away in the sheet the row opens. */}
      <div style={{ display: "flex", gap: 6 }} onClick={(e) => e.stopPropagation()}>
        {/* `local message` takes the MESSAGE as its positional, not an agent --
            unlike `local memory` next to it, which does. Passing the agent name
            here put it in the message body, so the button offered to send an
            agent its own name. An agent is targeted by one of its channels. */}
        <RunButton
          id="local.message"
          prefill={
            primaryChannel(agent)?.address
              ? { flags: { channel: primaryChannel(agent)!.address! } }
              : undefined
          }
        >
          Message
        </RunButton>
        <RunButton id="local.memory" tone="plain" prefill={{ positionals: [agent.name] }}>
          Memory
        </RunButton>
      </div>
    </Row>
  );
}
