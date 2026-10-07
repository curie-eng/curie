import type { CSSProperties, ReactNode } from "react";
import { C } from "../../tokens";
import { Chip, Dot } from "../../primitives";
import type { WorkItemOutcome, WorkItemStage } from "../../api/client";

// Shared pieces of the Factory view (#4102): the lane mapping, the state pill,
// the issue link, and the stage colors. State strings come from the API and
// are rendered verbatim; the console never re-derives them (#2577).

// The list, the open detail and its usage all refetch this often (Decision 9).
export const REFETCH_MS = 15_000;

export type State = WorkItemOutcome["state"];
export type LaneId = "queued" | "running" | "needs-you" | "shipped";

// Decision 5. `cancelled` is in no lane. Typed as Record<State, ...> so a new
// enum value fails typecheck until it is placed.
export const STATE_LANE: Record<State, LaneId | null> = {
  queued: "queued",
  waiting: "queued",
  running: "running",
  cancellation_requested: "running",
  publishing: "running",
  failed: "needs-you",
  expired: "needs-you",
  awaiting_approval: "needs-you",
  completed_unpublished: "needs-you",
  published: "shipped",
  cancelled: null,
};

export const LANES: { id: LaneId; label: string; color: string }[] = [
  { id: "queued", label: "Queued", color: C.mutedStatus },
  { id: "running", label: "Running", color: C.brand },
  { id: "needs-you", label: "Needs you", color: C.warn },
  { id: "shipped", label: "Shipped", color: C.success },
];

// Decision 10: stage states on existing tokens only.
export const STAGE_COLOR: Record<WorkItemStage["state"], string> = {
  done: C.success,
  current: C.brand,
  redo: C.warn,
  blocked: C.warn,
  pending: C.borderStrong,
};

const PILL_COLOR: Record<LaneId, string> = {
  queued: C.text2,
  running: C.text2,
  "needs-you": C.warn,
  shipped: C.success,
};

export function StatePill({ state }: { state: State }) {
  const lane = STATE_LANE[state];
  const color = lane ? PILL_COLOR[lane] : C.text2;
  return (
    <Chip
      color={color}
      border={lane === "queued" || lane === "running" || !lane ? C.borderStrong : color}
      pre={lane === "running" ? <Dot color={C.brand} size={7} /> : undefined}
    >
      {state}
    </Chip>
  );
}

export function issueRef(item: WorkItemOutcome): string {
  return `${item.repo_full_name}#${item.github_issue_number}`;
}

// An outbound link that never selects the card it sits in.
export function ExtLink({ href, children, style }: { href: string; children: ReactNode; style?: CSSProperties }) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noreferrer"
      onClick={(e) => e.stopPropagation()}
      onKeyDown={(e) => e.stopPropagation()}
      style={{ color: C.link, textDecoration: "none", ...style }}
    >
      {children}
    </a>
  );
}
