import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { C } from "../../tokens";
import { Card, Notice } from "../../primitives";
import {
  ApiError,
  getWorkItem,
  getWorkItemUsage,
  type WorkItemOutcome,
  type WorkItemStage,
  type WorkItemUsage,
} from "../../api/client";
import { ExtLink, REFETCH_MS, STAGE_COLOR, StatePill, issueRef } from "./factoryParts";

// The Factory detail panel (#4102 Decision 8). Read-only: no action buttons
// (Decision 2). The detail and its usage refetch every 15 seconds (Decision 9).

type Found<T> = { value: T | null; notFound: boolean };

async function orNotFound<T>(load: () => Promise<T>): Promise<Found<T>> {
  try {
    return { value: await load(), notFound: false };
  } catch (e) {
    if (e instanceof ApiError && e.status === 404) return { value: null, notFound: true };
    throw e;
  }
}

export function FactoryDetail({ id }: { id: string }) {
  const detail = useQuery({
    queryKey: ["factory", "workItem", id],
    queryFn: () => orNotFound(() => getWorkItem(id)),
    refetchInterval: REFETCH_MS,
  });
  const usage = useQuery({
    queryKey: ["factory", "workItemUsage", id],
    queryFn: () => orNotFound(() => getWorkItemUsage(id)),
    refetchInterval: REFETCH_MS,
  });

  const item = detail.data?.value ?? null;

  return (
    <Card style={{ padding: 18, position: "sticky", top: 16 }}>
      {detail.isPending ? <Notice>Loading work item…</Notice> : null}
      {detail.error ? <Notice>{`Could not load work item: ${String(detail.error)}`}</Notice> : null}
      {detail.data?.notFound ? <Notice>This work item no longer exists.</Notice> : null}
      {item ? <DetailBody item={item} usage={usage} /> : null}
    </Card>
  );
}

function DetailBody({
  item,
  usage,
}: {
  item: WorkItemOutcome;
  usage: { data?: Found<WorkItemUsage>; isPending: boolean; error: unknown };
}) {
  const stages = item.progress?.stages ?? [];
  const note = item.progress?.note;
  return (
    <div data-testid="factory-detail">
      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <ExtLink href={item.issue_url} style={{ fontFamily: C.mono, fontSize: 12 }}>
          {issueRef(item)}
        </ExtLink>
        <StatePill state={item.state} />
      </div>
      <h2 style={{ fontSize: 15, fontWeight: 600, margin: "6px 0 2px", lineHeight: 1.35, color: C.text }}>
        {item.title ?? issueRef(item)}
      </h2>

      <Section label="Stages">
        {stages.length > 0 ? <StageTrack stages={stages} /> : <Muted>No stage reports yet.</Muted>}
        {note ? (
          <div style={{ fontSize: 12, color: C.text2, marginTop: 10, fontStyle: "italic" }}>{`"${note}"`}</div>
        ) : null}
      </Section>

      <Section label="Outcome">
        <Row label="Pull request">
          {item.pr ? (
            <span>
              <ExtLink href={item.pr.url}>#{item.pr.number}</ExtLink> {item.pr.status}
            </span>
          ) : (
            <Muted>none</Muted>
          )}
        </Row>
        <Row label="CI">
          {item.ci ? (
            <span>
              {item.ci.state}
              {item.ci.reason ? <Muted>{` ${item.ci.reason}`}</Muted> : null}
            </span>
          ) : (
            <Muted>no observation yet</Muted>
          )}
        </Row>
        <Row label="Correctness">
          <Muted>owned by the bundle</Muted>
        </Row>
      </Section>

      <Section label="Cost">
        <div data-testid="factory-cost">
          <CostBody usage={usage} />
        </div>
      </Section>

      <Section label="Requests">
        <div data-testid="factory-requests">
          {item.requests.length === 0 ? <Muted>No requests yet.</Muted> : null}
          {item.requests.map((r) => (
            <div key={r.sequence} style={{ display: "flex", gap: 10, fontSize: 12, padding: "4px 0", color: C.text2 }}>
              <span style={{ fontFamily: C.mono, fontSize: 11, color: C.muted, width: 42, flex: "none" }}>
                #{r.sequence}
              </span>
              <span>
                {r.status}
                {r.terminal_cause && r.terminal_cause !== r.status ? <Muted>{` ${r.terminal_cause}`}</Muted> : null}
              </span>
            </div>
          ))}
        </div>
      </Section>
    </div>
  );
}

function money(usd: string | null | undefined): string {
  if (usd == null) return "no estimate";
  const n = Number(usd);
  return Number.isFinite(n) ? `$${n.toFixed(2)}` : "no estimate";
}

function CostBody({ usage }: { usage: { data?: Found<WorkItemUsage>; isPending: boolean; error: unknown } }) {
  if (usage.isPending) return <Muted>Loading cost…</Muted>;
  if (usage.error) return <Muted>{`Could not load cost: ${String(usage.error)}`}</Muted>;
  const u = usage.data?.value;
  if (!u) return <Muted>No usage reported.</Muted>;
  return (
    <>
      <Row label="Total so far">
        <span style={{ fontFamily: C.mono }}>{money(u.estimated_cost_usd)}</span>
      </Row>
      {u.models.map((m) => (
        <div key={`${m.role}:${m.model}`} data-testid="factory-cost-model">
          <Row label={m.model}>
            <span style={{ fontFamily: C.mono }}>{money(m.estimated_cost_usd)}</span>
          </Row>
        </div>
      ))}
      {u.cost_complete ? null : (
        <div style={{ fontSize: 11, color: C.warn, marginTop: 4 }}>estimate incomplete</div>
      )}
    </>
  );
}

function StageTrack({ stages }: { stages: WorkItemStage[] }) {
  const hasRound = stages.some((s) => s.round_label);
  return (
    <div
      data-testid="factory-stage-track"
      style={{
        display: "flex",
        alignItems: "flex-start",
        justifyContent: "space-between",
        position: "relative",
        padding: "0 6px",
        marginTop: hasRound ? 14 : 0,
      }}
    >
      <div
        aria-hidden="true"
        style={{ position: "absolute", left: 18, right: 18, top: 10, height: 2, background: C.borderStrong }}
      />
      {stages.map((s) => {
        const color = STAGE_COLOR[s.state];
        const filled = s.state === "done" || s.state === "blocked";
        return (
          <div
            key={s.id}
            data-testid="factory-stage"
            data-state={s.state}
            style={{
              position: "relative",
              textAlign: "center",
              width: 70,
              fontSize: 10,
              color: s.state === "current" ? C.text : C.muted,
            }}
          >
            {s.round_label ? (
              <span
                style={{
                  position: "absolute",
                  top: -15,
                  left: "50%",
                  transform: "translateX(-50%)",
                  whiteSpace: "nowrap",
                  fontFamily: C.mono,
                  fontSize: 9,
                  color: C.page,
                  background: C.warn,
                  borderRadius: 8,
                  padding: "0 4px",
                }}
              >
                {s.round_label}
              </span>
            ) : null}
            <i
              style={{
                display: "block",
                width: 20,
                height: 20,
                boxSizing: "border-box",
                borderRadius: "50%",
                margin: "0 auto 6px",
                background: filled ? color : C.card,
                border: "2px solid " + color,
                boxShadow: s.state === "current" ? `0 0 0 3px ${C.brand}33` : undefined,
                position: "relative",
              }}
            />
            {s.label}
          </div>
        );
      })}
    </div>
  );
}

function Section({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div style={{ borderTop: "1px solid " + C.border, marginTop: 14, paddingTop: 12 }}>
      <div style={{ color: C.muted, fontSize: 11, marginBottom: 8 }}>{label}</div>
      {children}
    </div>
  );
}

function Row({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div style={{ display: "flex", justifyContent: "space-between", gap: 12, padding: "4px 0", fontSize: 12 }}>
      <span style={{ color: C.text2 }}>{label}</span>
      <span style={{ color: C.text, textAlign: "right" }}>{children}</span>
    </div>
  );
}

function Muted({ children }: { children: ReactNode }) {
  return <span style={{ color: C.muted }}>{children}</span>;
}
