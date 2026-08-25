// The canvas layout has produced three separate visual bugs, none of which a
// typecheck could see: nodes stacked in one column because a role never matched,
// coordinates pinned by an older layout, and a graph pushed off to the right by
// columns that were empty. All three are layout arithmetic, so they are asserted
// here rather than by looking at a screenshot.

import { describe, expect, it } from "vitest";

import type { ResourceSample } from "../bridge/bridge";
import { buildGraph, EMPTY_DOC, LAYOUT, migrateDoc, NODE_W, type GraphDoc } from "./model";

function container(over: Partial<ResourceSample> & { name: string; role: string }): ResourceSample {
  return {
    id: over.name,
    origin: "docker",
    project: "curie",
    service: over.role,
    state: "running",
    cpuPercent: 1,
    memBytes: 1024,
    memLimitBytes: null,
    netRxBytes: 0,
    netTxBytes: 0,
    blockReadBytes: null,
    blockWriteBytes: null,
    pids: 1,
    startedAt: null,
    image: null,
    ports: [],
    at: 0,
    ...over,
  };
}

/** The platform with no bundle open and no API reachable -- the state the app is
 *  actually in most of the time, and the one that looked broken. */
const PLATFORM: ResourceSample[] = [
  container({ name: "curie-curie-dispatcher-1", role: "dispatcher" }),
  container({ name: "curie-valkey-1", role: "valkey" }),
  container({ name: "curie-curie-worker-1", role: "worker" }),
  container({ name: "curie-curie-api-1", role: "api" }),
  container({ name: "curie-postgres-1", role: "postgres" }),
  container({ name: "curie-rustfs-1", role: "objectstore" }),
];

const NO_SOURCES = { workspace: null, agents: [] };

describe("infrastructure-only graph", () => {
  it("draws every canonical infrastructure role", () => {
    const { nodes } = buildGraph({ ...NO_SOURCES, samples: PLATFORM }, EMPTY_DOC);
    expect(nodes.map((n) => n.label).sort()).toEqual([
      "api",
      "dispatcher",
      "objectstore",
      "postgres",
      "valkey",
      "worker",
    ]);
  });

  it("spreads them across columns instead of stacking them in one", () => {
    // The original bug drew all six at the same x, which reads as a list with
    // edges looping around the outside rather than as a flow.
    const { nodes } = buildGraph({ ...NO_SOURCES, samples: PLATFORM }, EMPTY_DOC);
    const columns = new Set(nodes.map((n) => n.x));
    expect(columns.size).toBeGreaterThan(3);
  });

  it("starts at the left edge, because the empty columns are compacted away", () => {
    // Without compaction the graph is pushed to the infra column's x, leaving
    // several columns of blank canvas to its left.
    const { nodes } = buildGraph({ ...NO_SOURCES, samples: PLATFORM }, EMPTY_DOC);
    expect(Math.min(...nodes.map((n) => n.x))).toBeLessThan(NODE_W);
  });

  it("wires the message path, so the graph is not a set of orphans", () => {
    const { edges } = buildGraph({ ...NO_SOURCES, samples: PLATFORM }, EMPTY_DOC);
    const labels = edges.map((e) => e.label);
    expect(labels).toContain("enqueue");
    expect(labels).toContain("consume");
    expect(labels).toContain("state");
    expect(edges.length).toBeGreaterThanOrEqual(4);
  });

  it("points the flow forwards, never right-to-left within the path", () => {
    const { nodes, edges } = buildGraph({ ...NO_SOURCES, samples: PLATFORM }, EMPTY_DOC);
    const at = new Map(nodes.map((n) => [n.id, n.x]));
    const flow = edges.filter((e) => ["enqueue", "consume", "aci"].includes(e.label ?? ""));
    for (const e of flow) {
      expect(at.get(e.to)!, `${e.from} -> ${e.to}`).toBeGreaterThan(at.get(e.from)!);
    }
  });

  it("leaves one-shot jobs out of the topology", () => {
    const withJobs = [...PLATFORM, container({ name: "curie-rustfs-init-1", role: "job" })];
    const { nodes } = buildGraph({ ...NO_SOURCES, samples: withJobs }, EMPTY_DOC);
    expect(nodes.some((n) => n.label === "job")).toBe(false);
  });

  it("never places two nodes on top of each other", () => {
    const { nodes } = buildGraph({ ...NO_SOURCES, samples: PLATFORM }, EMPTY_DOC);
    const spots = nodes.map((n) => `${n.x},${n.y}`);
    expect(new Set(spots).size).toBe(spots.length);
  });
});

describe("saved layouts", () => {
  const stale: GraphDoc = {
    version: 1,
    layout: 1,
    positions: { "infra:postgres": { x: 714, y: 45 } },
    extraNodes: [],
    extraEdges: [],
  };

  it("discards coordinates from a layout that no longer exists", () => {
    // Pixel coordinates saved against an older algorithm pin nodes where that
    // algorithm put them, and nothing on screen says they are stale.
    const migrated = migrateDoc(stale);
    expect(migrated.positions).toEqual({});
    expect(migrated.layout).toBe(LAYOUT);
  });

  it("keeps coordinates from the current layout", () => {
    const current: GraphDoc = { ...stale, layout: LAYOUT };
    expect(migrateDoc(current).positions).toEqual(stale.positions);
  });

  it("keeps nodes the operator added, even when the layout changed", () => {
    const withExtra: GraphDoc = {
      ...stale,
      extraNodes: [
        {
          id: "planned:agent:1",
          kind: "agent",
          label: "New agent",
          x: 10,
          y: 20,
          status: "planned",
          userAdded: true,
        },
      ],
    };
    expect(migrateDoc(withExtra).extraNodes).toHaveLength(1);
  });

  it("honours a current-layout position over the derived one", () => {
    const pinned: GraphDoc = {
      version: 1,
      layout: LAYOUT,
      positions: { "infra:postgres": { x: 999, y: 111 } },
      extraNodes: [],
      extraEdges: [],
    };
    const { nodes } = buildGraph({ ...NO_SOURCES, samples: PLATFORM }, pinned);
    const pg = nodes.find((n) => n.id === "infra:postgres")!;
    expect([pg.x, pg.y]).toEqual([999, 111]);
  });
});
