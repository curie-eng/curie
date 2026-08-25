// Filtering, sorting and grouping for the resource monitor.
//
// Pulled out of the view as pure functions because this is where the bugs live.
// Grouping logic that only exists inside a component can only be checked by
// opening a browser and counting rows, which is how a duplicate row and a
// section that lost its header went unnoticed until someone looked.

import type { ResourceSample } from "../bridge/bridge";

export type SortKey = "name" | "cpu" | "mem" | "net";
export type GroupKey = "project" | "agent" | "role" | "none";

export interface Section {
  readonly key: string;
  readonly label: string;
  readonly kind: GroupKey;
  readonly rows: readonly ResourceSample[];
}

/** Names for the buckets that exist because the real answer is "none". Spelled
 *  out rather than left as an empty string, so a `skill up` runner reads as
 *  standalone instead of as a project with no name. */
export const NO_PROJECT = "Standalone containers";
export const NO_AGENT_RUNNER = "Runner sessions";
export const NO_AGENT_OTHER = "Platform";

export function matches(sample: ResourceSample, query: string): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  return (
    sample.name.toLowerCase().includes(q) ||
    (sample.service ?? "").toLowerCase().includes(q) ||
    sample.role.toLowerCase().includes(q) ||
    (sample.agent ?? "").toLowerCase().includes(q) ||
    (sample.image ?? "").toLowerCase().includes(q) ||
    (sample.project ?? "").toLowerCase().includes(q) ||
    sample.id.toLowerCase().includes(q) ||
    // Search the host port an operator would type, and the container port they
    // might remember from the compose file.
    sample.ports.some(
      (p) => String(p.host ?? "").includes(q) || String(p.container).includes(q),
    )
  );
}

const COMPARATORS: Record<SortKey, (a: ResourceSample, b: ResourceSample) => number> = {
  // Ties broken by name throughout, so the table does not shuffle rows with
  // equal values on every frame.
  name: (a, b) => a.name.localeCompare(b.name),
  cpu: (a, b) => (b.cpuPercent ?? -1) - (a.cpuPercent ?? -1) || a.name.localeCompare(b.name),
  mem: (a, b) => (b.memBytes ?? -1) - (a.memBytes ?? -1) || a.name.localeCompare(b.name),
  net: (a, b) => (b.netRxBytes ?? -1) - (a.netRxBytes ?? -1) || a.name.localeCompare(b.name),
};

export function selectRows(
  samples: readonly ResourceSample[],
  opts: { query?: string; runningOnly?: boolean; sort: SortKey },
): ResourceSample[] {
  const visible = samples.filter(
    (s) => (!opts.runningOnly || s.state === "running") && matches(s, opts.query ?? ""),
  );
  return [...visible].sort(COMPARATORS[opts.sort]);
}

function bucketOf(sample: ResourceSample, group: GroupKey): string {
  if (group === "project") return sample.project ?? NO_PROJECT;
  if (group === "agent") {
    return sample.agent ?? (sample.role === "runner" ? NO_AGENT_RUNNER : NO_AGENT_OTHER);
  }
  return sample.role;
}

/** Infrastructure last: the operator's own agents should not sit below Postgres. */
function rank(label: string): number {
  if (label === NO_AGENT_OTHER) return 2;
  if (label === NO_AGENT_RUNNER || label === NO_PROJECT) return 1;
  return 0;
}

export function groupRows(rows: readonly ResourceSample[], group: GroupKey): Section[] {
  if (group === "none") {
    return rows.length ? [{ key: "all", label: "All workloads", kind: "none", rows }] : [];
  }
  const map = new Map<string, ResourceSample[]>();
  for (const row of rows) {
    const key = bucketOf(row, group);
    const list = map.get(key);
    if (list) list.push(row);
    else map.set(key, [row]);
  }
  return [...map.entries()]
    .map(([key, list]) => ({ key, label: key, kind: group, rows: list }))
    .sort((a, b) => rank(a.label) - rank(b.label) || a.label.localeCompare(b.label));
}

export interface Aggregate {
  readonly running: number;
  readonly total: number;
  readonly cpu: number | null;
  readonly mem: number | null;
  readonly startedAt: number | null;
  readonly state: "running" | "stopped" | "mixed";
}

/** Roll a section up for its parent row. Sums skip unmeasurable values rather
 *  than treating them as zero, and stay null when nothing was measurable at all
 *  -- a collapsed group of stopped containers reports a dash, not 0%. */
export function aggregate(rows: readonly ResourceSample[]): Aggregate {
  const running = rows.filter((r) => r.state === "running").length;
  const sum = (pick: (r: ResourceSample) => number | null) =>
    rows.reduce<number | null>((acc, r) => {
      const v = pick(r);
      return v === null ? acc : (acc ?? 0) + v;
    }, null);
  const started = rows
    .map((r) => (r.startedAt ? Date.parse(r.startedAt) : NaN))
    .filter((n) => Number.isFinite(n));
  return {
    running,
    total: rows.length,
    cpu: sum((r) => r.cpuPercent),
    mem: sum((r) => r.memBytes),
    startedAt: started.length ? Math.max(...started) : null,
    state: running === 0 ? "stopped" : running === rows.length ? "running" : "mixed",
  };
}
