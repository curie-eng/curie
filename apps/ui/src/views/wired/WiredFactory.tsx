import { useMemo, useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { C, R } from "../../tokens";
import { Card, Dot, EmptyState, Notice, CliHint, cliCommand } from "../../primitives";
import { useStore } from "../../state/store";
import { listWorkItems, type WorkItemOutcome } from "../../api/client";
import { FactoryDetail } from "./FactoryDetail";
import { ExtLink, LANES, REFETCH_MS, STAGE_COLOR, STATE_LANE, StatePill, issueRef, type LaneId, type State } from "./factoryParts";

// Factory view (#4102): every issue the factory agent took on, in four lanes
// (Decision 5), with a read-only detail panel (Decisions 2 and 8). Replaces the
// flat Work items list (#2577). State and cause strings come straight from the
// API and are rendered verbatim; only the lane placement is decided here.

const SHIPPED_CAP = 10;
const WEEK_MS = 7 * 24 * 3_600_000;

// Needs-you states in the order the KPI breakdown lists them.
const NEEDS_YOU_STATES: State[] = ["failed", "expired", "awaiting_approval", "completed_unpublished"];

function byLane(items: WorkItemOutcome[]) {
  const lanes: Record<LaneId, WorkItemOutcome[]> = { queued: [], running: [], "needs-you": [], shipped: [] };
  let cancelled = 0;
  for (const item of items) {
    const lane = STATE_LANE[item.state];
    if (lane) lanes[lane].push(item);
    else cancelled += 1;
  }
  lanes.shipped.sort((a, b) => Date.parse(b.updated_at) - Date.parse(a.updated_at));
  return { lanes, cancelled };
}

export function WiredFactory() {
  const { state } = useStore();
  const [selectedId, setSelectedId] = useState<string | null>(null);

  const listQuery = useQuery({
    queryKey: ["factory", "workItems"],
    queryFn: () => listWorkItems(),
    refetchInterval: REFETCH_MS,
  });

  const items = listQuery.data?.items ?? [];

  return (
    <div>
      <div style={{ display: "flex", alignItems: "flex-start", gap: 16, marginBottom: 18 }}>
        <div>
          <h1 style={{ fontSize: 20, fontWeight: 600, color: C.text, margin: "0 0 4px" }}>Factory</h1>
          <div style={{ fontSize: 13, color: C.muted }}>
            Every issue the factory took on, where it stands, and which ones need you.
          </div>
        </div>
        <div style={{ marginLeft: "auto" }}>
          <CliHint
            command={cliCommand(state.env === "prod" ? "cluster.work-items" : "local.work-items")}
            label={cliCommand(state.env === "prod" ? "cluster.work-items" : "local.work-items")}
          />
        </div>
      </div>

      {listQuery.data?.truncated ? (
        <div
          data-testid="factory-truncated"
          style={{ marginBottom: 12, padding: "8px 12px", textAlign: "center", color: C.muted, fontSize: 13 }}
        >
          {`Showing the first ${listQuery.data.limit} work items; more exist than fit this view.`}
        </div>
      ) : null}

      {listQuery.isPending ? <Notice>Loading work items…</Notice> : null}
      {listQuery.error ? <Notice>{`Could not load work items: ${String(listQuery.error)}`}</Notice> : null}
      {!listQuery.isPending && !listQuery.error && items.length === 0 ? (
        <EmptyState title="No factory work items yet" sub="Once the factory agent picks up a GitHub issue, it shows up here." />
      ) : null}

      {items.length > 0 ? (
        <Board items={items} fetchedAt={listQuery.dataUpdatedAt} selectedId={selectedId} onSelect={setSelectedId} />
      ) : null}
    </div>
  );
}

function Board({
  items,
  fetchedAt,
  selectedId,
  onSelect,
}: {
  items: WorkItemOutcome[];
  fetchedAt: number;
  selectedId: string | null;
  onSelect: (id: string) => void;
}) {
  const { lanes, cancelled } = useMemo(() => byLane(items), [items]);
  // "Shipped, 7d" is measured from when this list was fetched, so the render stays pure.
  const shipped7d = lanes.shipped.filter((i) => fetchedAt - Date.parse(i.updated_at) <= WEEK_MS).length;
  const running = lanes.running.length;
  const queued = lanes.queued.length;
  const needsYou = lanes["needs-you"];
  const breakdown = NEEDS_YOU_STATES.map((s) => [s, needsYou.filter((i) => i.state === s).length] as const)
    .filter(([, n]) => n > 0)
    .map(([s, n]) => `${n} ${s.replace(/_/g, " ")}`)
    .join(", ");

  return (
    <>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12, marginBottom: 16 }}>
        <Kpi testId="factory-kpi-in-flight" label="In flight" value={running + queued} detail={`${running} running, ${queued} queued`} />
        <Kpi testId="factory-kpi-needs-you" label="Needs you" value={needsYou.length} detail={breakdown || "nothing waiting on you"} alert />
        <Kpi testId="factory-kpi-shipped" label="Shipped, 7d" value={shipped7d} detail="PRs published" />
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "minmax(0, 1fr) 420px", gap: 16, alignItems: "start" }}>
        <div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 10 }}>
            {LANES.map((lane) => {
              const all = lanes[lane.id];
              const shown = lane.id === "shipped" ? all.slice(0, SHIPPED_CAP) : all;
              const more = all.length - shown.length;
              return (
                <section
                  key={lane.id}
                  data-testid={`factory-lane-${lane.id}`}
                  style={{
                    background: C.darkest,
                    border: "1px solid " + (lane.id === "needs-you" ? C.warn : C.border),
                    borderRadius: R.card,
                    padding: 10,
                    minHeight: 420,
                  }}
                >
                  <h3
                    style={{
                      fontSize: 12,
                      margin: "2px 4px 10px",
                      display: "flex",
                      alignItems: "center",
                      gap: 6,
                      fontWeight: 600,
                      color: C.text2,
                    }}
                  >
                    <Dot color={lane.color} size={7} />
                    {lane.label}
                    <span style={{ marginLeft: "auto", fontFamily: C.mono, color: C.muted, fontWeight: 400 }}>
                      {all.length}
                    </span>
                  </h3>
                  {shown.map((item) => (
                    <LaneCard
                      key={item.id}
                      item={item}
                      lane={lane.id}
                      selected={item.id === selectedId}
                      onSelect={() => onSelect(item.id)}
                    />
                  ))}
                  {more > 0 ? (
                    <div
                      data-testid="factory-shipped-more"
                      style={{ textAlign: "center", color: C.muted, fontSize: 11, padding: 6 }}
                    >
                      {`${more} more`}
                    </div>
                  ) : null}
                </section>
              );
            })}
          </div>
          {cancelled > 0 ? (
            <div data-testid="factory-cancelled" style={{ marginTop: 10, fontSize: 12, color: C.muted }}>
              {`${cancelled} cancelled`}
            </div>
          ) : null}
        </div>

        {selectedId ? (
          <FactoryDetail id={selectedId} />
        ) : (
          <Card style={{ padding: 18 }}>
            <Notice>Select a card to see its stages, outcome, and cost.</Notice>
          </Card>
        )}
      </div>
    </>
  );
}

function Kpi({
  testId,
  label,
  value,
  detail,
  alert,
}: {
  testId: string;
  label: string;
  value: number;
  detail: string;
  alert?: boolean;
}) {
  return (
    <Card style={{ padding: "14px 16px", border: "1px solid " + (alert ? C.warn : C.border) }}>
      <div data-testid={testId}>
        <div style={{ color: C.muted, fontSize: 11, textTransform: "uppercase", letterSpacing: ".04em" }}>{label}</div>{" "}
        <div
          style={{
            fontSize: 24,
            fontWeight: 600,
            marginTop: 6,
            fontVariantNumeric: "tabular-nums",
            color: alert && value > 0 ? C.warn : C.text,
          }}
        >
          {value}
        </div>{" "}
        <div style={{ fontSize: 11, color: C.muted, marginTop: 2 }}>{detail}</div>
      </div>
    </Card>
  );
}

function LaneCard({
  item,
  lane,
  selected,
  onSelect,
}: {
  item: WorkItemOutcome;
  lane: LaneId;
  selected: boolean;
  onSelect: () => void;
}) {
  const ref = issueRef(item);
  const stages = item.progress?.stages ?? [];
  return (
    <div
      role="button"
      tabIndex={0}
      aria-label={`Open ${ref}`}
      aria-pressed={selected}
      data-testid="factory-card"
      onClick={onSelect}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onSelect();
        }
      }}
      style={{
        background: C.card,
        border: "1px solid " + (selected ? C.brand : C.border),
        boxShadow: selected ? `inset 0 0 0 1px ${C.brand}` : undefined,
        borderRadius: 10,
        padding: 10,
        marginBottom: 8,
        cursor: "pointer",
        color: C.text,
        fontSize: 13,
      }}
    >
      <ExtLink href={item.issue_url} style={{ fontFamily: C.mono, fontSize: 11 }}>
        {ref}
      </ExtLink>
      <div style={{ margin: "4px 0 8px", lineHeight: 1.35 }}>{item.title ?? ref}</div>
      {lane === "needs-you" ? (
        <div style={{ fontSize: 11, color: C.warn, margin: "-2px 0 8px", lineHeight: 1.35 }}>{item.actionable_cause}</div>
      ) : null}
      {stages.length > 0 ? (
        <div data-testid="factory-card-track" style={{ display: "flex", gap: 3, marginBottom: 8 }}>
          {stages.map((s) => (
            <b
              key={s.id}
              data-state={s.state}
              title={`${s.label}: ${s.state}`}
              style={{ flex: 1, height: 4, borderRadius: 2, background: STAGE_COLOR[s.state] }}
            />
          ))}
        </div>
      ) : null}
      <Meta>
        <StatePill state={item.state} />
        {lane === "shipped" && item.pr ? (
          <ExtLink href={item.pr.url} style={{ fontSize: 11 }}>{`PR #${item.pr.number}`}</ExtLink>
        ) : null}
      </Meta>
    </div>
  );
}

function Meta({ children }: { children: ReactNode }) {
  return (
    <div style={{ display: "flex", gap: 8, alignItems: "center", fontSize: 11, color: C.muted, flexWrap: "wrap" }}>
      {children}
    </div>
  );
}
