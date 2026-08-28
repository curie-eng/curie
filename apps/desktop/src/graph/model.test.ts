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
    health: null,
    exitCode: null,
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

  it("carries each node's role, which is what tells the services apart", () => {
    // `kind` is `infra` for every one of these, so a canvas colouring by kind
    // paints the whole platform one grey. The role is the distinguishing fact
    // and `ROLE_COLOR` has a hue per role; if it stops reaching the node, the
    // graph silently goes monochrome again and nothing else fails.
    const { nodes } = buildGraph({ ...NO_SOURCES, samples: PLATFORM }, EMPTY_DOC);
    const infra = nodes.filter((n) => n.kind === "infra");
    expect(infra.length).toBeGreaterThan(1);
    for (const node of infra) {
      expect(node.role, `${node.label} has no role`).toBeTruthy();
    }
    // Distinct roles, so distinct colours are actually possible.
    const roles = infra.map((n) => n.role);
    expect(new Set(roles).size).toBe(infra.length);
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

describe("lanes and live load", () => {
  it("labels the band the nodes actually sit in", () => {
    const { lanes, nodes } = buildGraph({ ...NO_SOURCES, samples: PLATFORM }, EMPTY_DOC);
    expect(lanes.map((l) => l.label)).toEqual(["Platform"]);
    // The band must span the columns it claims to.
    const lane = lanes[0];
    for (const n of nodes) {
      expect(n.x).toBeGreaterThanOrEqual(lane.x);
      expect(n.x).toBeLessThan(lane.x + lane.width);
      expect(n.lane).toBe("Platform");
    }
  });

  it("merges adjacent columns that share a label into one band", () => {
    // Six infrastructure nodes occupy five columns; that is one Platform band,
    // not five.
    const { lanes } = buildGraph({ ...NO_SOURCES, samples: PLATFORM }, EMPTY_DOC);
    expect(lanes).toHaveLength(1);
  });

  it("drops the bands once the operator has arranged things by hand", () => {
    // A band that claims to cover a column stops being true the moment a node
    // is dragged out of it.
    const dragged: GraphDoc = {
      version: 1,
      layout: LAYOUT,
      positions: { "infra:api": { x: 5, y: 900 } },
      extraNodes: [],
      extraEdges: [],
    };
    expect(buildGraph({ ...NO_SOURCES, samples: PLATFORM }, dragged).lanes).toEqual([]);
  });

  it("carries live load onto the nodes backed by a container", () => {
    const busy = [container({ name: "curie-curie-api-1", role: "api", cpuPercent: 61.5 })];
    const { nodes } = buildGraph({ ...NO_SOURCES, samples: busy }, EMPTY_DOC);
    expect(nodes[0].metric).toEqual({ cpu: 61.5, mem: 1024 });
  });

  it("keeps an unmeasurable load null rather than zero", () => {
    const stopped = [
      container({ name: "curie-curie-api-1", role: "api", state: "exited", cpuPercent: null, memBytes: null }),
    ];
    const { nodes } = buildGraph({ ...NO_SOURCES, samples: stopped }, EMPTY_DOC);
    expect(nodes[0].metric).toEqual({ cpu: null, mem: null });
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

describe("an agent with a model override", () => {
  // The fourth layout bug, and the worst: the model-node dedup checked the
  // `nodes` array that buildGraph returns, which is declared further down the
  // same function. Reading it threw "Cannot access 'm' before initialization"
  // and React unmounted the tree, so the ENTIRE WINDOW went blank -- not just
  // the canvas. Every other test here passed, because none of them gave an agent
  // a model: with no reachable API there are no agents at all, so the crash only
  // reached anyone whose platform actually had one.
  const agent = (id: string, name: string, model: string | null) => ({ id, name, model });

  it("does not throw, and draws the model", () => {
    const { nodes } = buildGraph(
      { workspace: null, samples: [], agents: [agent("a1", "sre-bot", "claude-opus-5")] },
      EMPTY_DOC,
    );
    expect(nodes.map((n) => n.label)).toContain("claude-opus-5");
  });

  it("draws one model node for two agents on the same model", () => {
    // What the dedup was there for.
    const { nodes } = buildGraph(
      {
        workspace: null,
        samples: [],
        agents: [agent("a1", "sre-bot", "claude-opus-5"), agent("a2", "deal-desk", "claude-opus-5")],
      },
      EMPTY_DOC,
    );
    expect(nodes.filter((n) => n.kind === "model")).toHaveLength(1);
    // ...and both agents point at it.
    expect(nodes.filter((n) => n.kind === "agent")).toHaveLength(2);
  });

  it("draws a model node per distinct model", () => {
    const { nodes } = buildGraph(
      {
        workspace: null,
        samples: [],
        agents: [agent("a1", "sre-bot", "claude-opus-5"), agent("a2", "deal-desk", "claude-sonnet-5")],
      },
      EMPTY_DOC,
    );
    expect(nodes.filter((n) => n.kind === "model").map((n) => n.label).sort()).toEqual([
      "claude-opus-5",
      "claude-sonnet-5",
    ]);
  });

  it("draws no model node for an agent on the platform default", () => {
    const { nodes } = buildGraph(
      { workspace: null, samples: [], agents: [agent("a1", "sre-bot", null)] },
      EMPTY_DOC,
    );
    expect(nodes.filter((n) => n.kind === "model")).toHaveLength(0);
  });

  it("keeps every node id unique, which is what the React keys rely on", () => {
    const { nodes } = buildGraph(
      {
        workspace: null,
        samples: PLATFORM,
        agents: [agent("a1", "sre-bot", "claude-opus-5"), agent("a2", "deal-desk", "claude-opus-5")],
      },
      EMPTY_DOC,
    );
    expect(new Set(nodes.map((n) => n.id)).size).toBe(nodes.length);
  });
});

describe("the deploy edge", () => {
  const ws = {
    path: "/w/shift-notes",
    name: "shift-notes",
    plugin: { name: "shift-notes", version: "0.1.0" },
    skills: ["shift-notes"],
    hasEvals: true,
    hasMcp: false,
    lastOpened: 1,
  };
  const agent = (name: string, id: string) => ({ id, name, model: null, thinking: null });
  const deployEdges = (agents: ReturnType<typeof agent>[]) =>
    buildGraph({ workspace: ws, agents, samples: PLATFORM } as never, EMPTY_DOC)
      .edges.filter((e) => e.kind === "deploy");

  it("connects the open bundle to the agent it is deployed as", () => {
    const edges = deployEdges([agent("shift-notes", "a1")]);
    expect(edges).toHaveLength(1);
    expect(edges[0].from).toBe("bundle");
    expect(edges[0].to).toBe("agent:a1");
  });

  it("does NOT connect it to an unrelated agent", () => {
    // The bug this pins: the edge was drawn for EVERY agent the platform
    // reported, so a machine running two unrelated agents showed the bundle you
    // have open shipping as both. A derived diagram asserting a relationship
    // that does not exist is worse than one that omits it.
    const edges = deployEdges([agent("squawk", "a2")]);
    expect(edges).toHaveLength(0);
  });

  it("picks out only its own agent when several are running", () => {
    const edges = deployEdges([agent("squawk", "a2"), agent("shift-notes", "a1"), agent("weather", "a3")]);
    expect(edges.map((e) => e.to)).toEqual(["agent:a1"]);
  });
});

describe("what the inspector can tell you about a piece of infrastructure", () => {
  const node = (role: string, over: Partial<ResourceSample> = {}) =>
    buildGraph(
      { ...NO_SOURCES, samples: [container({ name: `curie-${role}-1`, role, ...over })] } as never,
      EMPTY_DOC,
    ).nodes.find((n) => n.id === `infra:${role}`)!;

  it("leads with what the thing does, not with three spellings of its name", () => {
    // Container / Image / State answered "which container is this", which is a
    // different and much rarer question than "what is this".
    const d = node("valkey").detail!;
    expect(d.About).toMatch(/queue/i);
    expect(Object.keys(d)[0]).toBe("About");
  });

  it("carries the live numbers the node badge only hints at", () => {
    const d = node("api", { cpuPercent: 12.34, memBytes: 268435456, ports: [{ host: 28000, container: 8000, proto: "tcp" }] }).detail!;
    expect(d.CPU).toBe("12.3%");
    expect(d.Memory).toMatch(/256|268/);
    expect(d.Ports).toBe("28000:8000");
    // An unpublished port is not reachable; claiming it is would be worse than
    // saying nothing.
    expect(node("api", { ports: [{ host: null, container: 8000, proto: "tcp" }] }).detail!.Ports).toBeUndefined();
  });

  it("reports uptime from Docker's ISO start, and nothing at all when it cannot", () => {
    const at = Date.parse("2026-08-28T12:00:00Z");
    expect(node("worker", { startedAt: "2026-08-28T10:48:00Z", at }).detail!.Up).toBe("1h 12m");
    expect(node("worker", { startedAt: "not a date", at }).detail!.Up).toBeUndefined();
    expect(node("worker", { startedAt: null, at }).detail!.Up).toBeUndefined();
  });

  it("folds health into state when Docker reports one", () => {
    expect(node("postgres", { state: "running", health: "healthy" }).detail!.State).toBe("running (healthy)");
    expect(node("postgres", { state: "running", health: null }).detail!.State).toBe("running");
  });

  it("describes every role it will draw", () => {
    // A node with no blurb is the bug this whole change is about, so the set of
    // roles the canvas draws and the set it can explain must not drift apart.
    for (const role of ["dispatcher", "valkey", "worker", "api", "postgres", "objectstore", "langfuse", "clickhouse", "otel"]) {
      expect(node(role).detail!.About, `no blurb for ${role}`).toBeTruthy();
    }
  });
});
