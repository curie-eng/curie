// The canvas graph, and where it comes from.
//
// The graph is *derived*, not drawn. A hand-maintained architecture diagram is
// wrong within a week; this one is rebuilt on every render from three live
// sources -- the open bundle on disk, the agents the platform API reports, and
// the containers Docker says are running -- so what you see is what is actually
// deployed. Only two things are persisted: where the operator dragged each node,
// and any node they added themselves.
//
// That second part is what makes it an editor rather than a picture. A node you
// add is a `planned` node: it is not running, it is drawn as an outline, and it
// carries the `curie` command that would make it real. So the canvas doubles as
// a place to sketch "this agent should also talk to Postgres" and then run the
// command that does it.

import type { AgentSummary } from "../bridge/app";
import { channelsOf } from "../lib/channels";
import type { ResourceSample, Workspace } from "../bridge/bridge";
import type { NodeKind } from "../tokens";
import { isDeployedFrom } from "../lib/deployment";
import { bytes } from "../lib/format";

export interface GraphNode {
  readonly id: string;
  readonly kind: NodeKind;
  readonly label: string;
  /** Second line on the node: an image tag, a channel id, a model name. */
  readonly sub?: string;
  x: number;
  y: number;
  /** `live` is running right now, `known` exists but is not running, `planned`
   *  is something the operator drew that does not exist yet. */
  readonly status: "live" | "known" | "planned";
  /** Free-form detail for the inspector panel. */
  readonly detail?: Readonly<Record<string, string | undefined>>;
  /** Commands this node makes sense as a target for, as manifest ids. */
  readonly actions?: readonly string[];
  /** Set on nodes the operator added, which are the only ones that persist. */
  readonly userAdded?: boolean;
  /** The workload role behind this node, for the infrastructure ones. Carried
   *  because `kind` is deliberately coarse -- every platform service is
   *  `infra` -- and colouring by kind therefore painted valkey, postgres, the
   *  api and the object store the same grey. The role is what tells them apart,
   *  and `ROLE_COLOR` already has a hue per role for the Resources table. */
  readonly role?: string;
  /** Which band of the diagram this belongs to. Drawn as a labelled lane, so
   *  the graph reads as an architecture view rather than as loose boxes. */
  readonly lane?: string;
  /** Live load, when this node is backed by a container. A topology diagram
   *  that also shows load is a monitoring surface; one that does not is a
   *  picture that happens to be accurate. */
  readonly metric?: { readonly cpu: number | null; readonly mem: number | null };
}

export interface GraphEdge {
  readonly id: string;
  readonly from: string;
  readonly to: string;
  readonly label?: string;
  /** `flow` is a message path, `deploy` is a build/ship path, `data` is storage,
   *  `planned` is an edge the operator drew. */
  readonly kind: "flow" | "deploy" | "data" | "planned";
}

/** A labelled vertical band covering one or more columns. */
export interface Lane {
  readonly label: string;
  readonly x: number;
  readonly width: number;
}

export interface Graph {
  readonly nodes: readonly GraphNode[];
  readonly edges: readonly GraphEdge[];
  readonly lanes: readonly Lane[];
}

/** What is persisted between sessions: positions, plus whatever was added by
 *  hand. Everything else is rederived, so a stale saved doc can never make the
 *  canvas disagree with reality. */
export interface GraphDoc {
  readonly version: 1;
  /** Which layout algorithm produced the coordinates below. See `LAYOUT`. */
  readonly layout?: number;
  readonly positions: Record<string, { x: number; y: number }>;
  readonly extraNodes: GraphNode[];
  readonly extraEdges: GraphEdge[];
}

/**
 * Bump when the derived layout changes shape.
 *
 * Saved positions are absolute pixels, so a layout change silently invalidates
 * them: nodes stay pinned where an older algorithm put them, the new columns
 * never appear, and there is no way to tell from the screen that the coordinates
 * are stale. Dropping positions from a different layout version is the honest
 * behaviour -- the operator loses a hand-arrangement they made against a layout
 * that no longer exists, and gets one that matches what the app now draws.
 *
 * 2: infrastructure spread across flow-order columns, with empty columns
 *    compacted away (was: one infra column at a fixed x).
 */
export const LAYOUT = 2;

export const EMPTY_DOC: GraphDoc = {
  version: 1,
  layout: LAYOUT,
  positions: {},
  extraNodes: [],
  extraEdges: [],
};

export function isGraphDoc(value: unknown): value is GraphDoc {
  const doc = value as GraphDoc | null;
  return !!doc && doc.version === 1 && typeof doc.positions === "object";
}

/** Normalise a loaded doc: keep nodes the operator added, discard coordinates
 *  from a layout that no longer exists. */
export function migrateDoc(doc: GraphDoc): GraphDoc {
  if (doc.layout === LAYOUT) return doc;
  return { ...doc, layout: LAYOUT, positions: {} };
}

export const NODE_W = 168;
export const NODE_H = 54;

/** Columns, left to right: what you author, what runs it, what carries it, what
 *  it talks to. The layout is a pipeline read left-to-right because that is the
 *  direction the product's own docs describe the flow in. */
/**
 * Logical columns, left to right: what you author, what runs it, what carries
 * it, what it talks to. Numbers rather than pixels because the layout is
 * *compacted* before it is drawn -- see `materialise`.
 */
const COL = {
  source: 0,
  agent: 2,
  runtime: 3,
  /** Infrastructure spreads across several columns by its place in the message
   *  path, so the platform reads as a flow rather than as a stack. */
  infra: 4,
  external: 10,
} as const;

/**
 * Where each infrastructure role sits in the message path, as an offset from the
 * infra column.
 *
 * Stacking every service in one column made the platform look like a list, and
 * put every edge between two nodes at the same x -- which the router then drew
 * as a loop all the way around the outside. Spreading them along the flow the
 * architecture doc describes (ingress, queue, worker, api, stores) makes the
 * edges short and forward-pointing.
 */
const FLOW_DEPTH: Record<string, number> = {
  dispatcher: 0,
  valkey: 1,
  worker: 2,
  api: 3,
  postgres: 4,
  objectstore: 4,
  langfuse: 4,
  clickhouse: 5,
  otel: 4,
  model: 4,
};

interface Sources {
  readonly workspace: Workspace | null;
  readonly agents: readonly AgentSummary[];
  readonly samples: readonly ResourceSample[];
}

const GAP = 74;
const COL_WIDTH = 228;

/** A node before its pixel position is known. */
interface Placed {
  readonly node: Omit<GraphNode, "x" | "y">;
  readonly column: number;
  readonly row: number;
}

/** Lane label per logical column. Infrastructure spans several columns and they
 *  all carry one label, so the band is drawn once across the whole run. */
function laneFor(column: number): string {
  if (column <= COL.source) return "Bundle";
  if (column === COL.agent) return "Agents";
  if (column === COL.runtime) return "Runtime";
  if (column >= COL.external) return "Integrations";
  return "Platform";
}

/** Hands out the next free row in a logical column.
 *
 *  A cursor rather than an index-of-N calculation, because the number of nodes a
 *  column ends up with is not known when the first one is placed: the source
 *  column gets a bundle, maybe eval cases, each decided by a separate branch.
 *  Computing a row from a guessed total is how two nodes end up on top of each
 *  other. */
function rowCursor() {
  const used = new Map<number, number>();
  return (column: number): number => {
    const row = used.get(column) ?? 0;
    used.set(column, row + 1);
    return row;
  };
}

/**
 * Turn logical columns into pixels, dropping the empty ones.
 *
 * Compaction is what makes a sparse graph look deliberate. With no bundle open
 * and no API reachable, the only populated column is infrastructure -- and
 * without this the graph would be pushed to column four's x, leaving three
 * columns of blank canvas to its left and reading as a rendering fault.
 */
function materialise(placed: readonly Placed[]): { nodes: GraphNode[]; lanes: Lane[] } {
  const occupied = [...new Set(placed.map((p) => p.column))].sort((a, b) => a - b);
  const compacted = new Map(occupied.map((c, i) => [c, i]));
  const xOf = (column: number) => 30 + (compacted.get(column) ?? 0) * COL_WIDTH;

  const nodes = placed.map((p) => ({
    ...p.node,
    lane: laneFor(p.column),
    x: xOf(p.column),
    y: 60 + p.row * GAP,
  }));

  // Merge adjacent occupied columns that share a label into one band.
  const lanes: Lane[] = [];
  for (const column of occupied) {
    const label = laneFor(column);
    const last = lanes[lanes.length - 1];
    const right = xOf(column) + NODE_W;
    if (last && last.label === label) {
      lanes[lanes.length - 1] = { ...last, width: right - last.x };
    } else {
      lanes.push({ label, x: xOf(column), width: NODE_W });
    }
  }
  return { nodes, lanes };
}

/**
 * What each piece of the platform is for, in one line.
 *
 * The inspector used to answer "what is this valkey node" with the container's
 * name, its image tag and the word `running` -- three restatements of the label
 * you already clicked. None of them say what the thing DOES, which is the only
 * question a diagram of an architecture you did not write can be asked.
 *
 * These follow `ARCHITECTURE.md`'s own component map rather than being invented
 * here, so the canvas and the doc describe the same system. The canvas already
 * draws the message path from that doc; this is the same source for the nouns.
 */
const ROLE_ABOUT: Readonly<Record<string, string>> = {
  dispatcher: "Takes messages in from Slack, drops duplicates, and puts the survivors on the queue.",
  valkey: "The queue. Work waits here between arriving and being picked up, and thread locks live here so one conversation only ever has one live session.",
  worker: "Pulls work off the queue and runs one session per thread, in a sandbox.",
  api: "Deploys bundles, keeps the record of agents and versions, and answers reads. This app talks to it.",
  postgres: "The record: agents, their versions, and what is deployed where.",
  objectstore: "Where skill bundles are kept once uploaded, addressed by digest.",
  langfuse: "Traces and cost. Every model call an agent makes is recorded here.",
  clickhouse: "The column store Langfuse keeps its traces in.",
  otel: "Collects traces from everything else and forwards them to Langfuse.",
  runner: "A sandbox running Claude Code plus one skill \u2014 this is where an agent's turn actually happens.",
  model: "The model an agent's turns are sent to.",
};

/** `28000:8000`, the way the Resources table spells the same thing. Only the
 *  published ones: an unpublished container port is not reachable and saying it
 *  is would be worse than saying nothing. */
function publishedPorts(ports: ResourceSample["ports"]): string | undefined {
  const published = ports.filter((p) => p.host !== null);
  if (!published.length) return undefined;
  return published.map((p) => `${p.host}:${p.container}`).join(", ");
}

/** `1h 12m`, `4m`, `38s` -- how long the container has been up. Docker reports
 *  the start as an ISO string; an unparseable one yields nothing rather than
 *  `NaNs`. */
function uptime(startedAt: string | null, now: number): string | undefined {
  if (!startedAt) return undefined;
  const began = Date.parse(startedAt);
  if (!Number.isFinite(began)) return undefined;
  const secs = Math.max(0, Math.round((now - began) / 1000));
  if (secs < 90) return `${secs}s`;
  const mins = Math.round(secs / 60);
  if (mins < 60) return `${mins}m`;
  return `${Math.floor(mins / 60)}h ${mins % 60}m`;
}

export function buildGraph(sources: Sources, doc: GraphDoc): Graph {
  const placed: Placed[] = [];
  const edges: GraphEdge[] = [];
  const nextRow = rowCursor();
  const add = (node: Omit<GraphNode, "x" | "y">, column: number) => {
    placed.push({ node, column, row: nextRow(column) });
  };
  const link = (from: string, to: string, kind: GraphEdge["kind"], label?: string) => {
    edges.push({ id: `${from}->${to}`, from, to, kind, label });
  };
  // "Has this node been added yet?" against the accumulator `add` writes into.
  // Declared here with the other helpers rather than further down, because the
  // model-node dedup below needs it too, and reaching for the finished `nodes`
  // array instead threw a temporal-dead-zone ReferenceError that blanked the
  // whole window (see the test named for it).
  const has = (id: string) => placed.some((p) => p.node.id === id);

  // --- what you author ----------------------------------------------------
  const ws = sources.workspace;
  // The same name Build and the deployment panel match on: the plugin's name
  // when it declares one, the directory's otherwise.
  const openBundleName = ws ? (ws.plugin?.name ?? ws.name) : "";
  if (ws) {
    add({
      id: "bundle",
      kind: "repo",
      label: ws.name,
      sub: `${ws.skills.length} skill${ws.skills.length === 1 ? "" : "s"}`,
      status: "known",
      detail: {
        Path: ws.path,
        Version: ws.plugin?.version,
        Skills: ws.skills.join(", ") || undefined,
        Evals: ws.hasEvals ? "evals/cases.json present" : "no eval cases",
        MCP: ws.hasMcp ? ".mcp.json present" : undefined,
      },
      actions: ["skill.up", "skill.check", "skill.eval", "local.deploy", "cluster.deploy"],
    }, COL.source);

    if (ws.hasEvals) {
      add({
        id: "evals",
        kind: "eval",
        label: "Eval cases",
        sub: "evals/cases.json",
        status: "known",
        actions: ["skill.eval", "local.eval", "cluster.eval"],
      }, COL.source);
    }
    if (ws.hasMcp) {
      add({
        id: "mcp",
        kind: "mcp",
        label: "MCP servers",
        sub: "from .mcp.json",
        status: "known",
        detail: { Source: `${ws.path}/.mcp.json` },
        actions: ["skill.check"],
      }, COL.external);
    }
  }

  // --- agents the platform knows about -------------------------------------
  const agents = sources.agents;
  agents.forEach((agent) => {
    const id = `agent:${agent.id}`;
    add({
      id,
      kind: "agent",
      label: agent.name,
      sub: agent.model ?? "platform default model",
      // An agent row in the API is a deployed identity; whether it is *running*
      // is a runner question, answered by the runtime column.
      status: "known",
      detail: {
        ID: agent.id,
        Model: agent.model ?? undefined,
        Thinking: agent.thinking ?? undefined,
        Repo: agent.repo_full_name ?? undefined,
        Secrets: agent.secrets?.join(", ") || undefined,
        "Approval gates": agent.approval_required_tools?.join(", ") || undefined,
      },
      actions: [
        "local.versions",
        "local.memory",
        "local.approvals",
        "local.overrides",
        "local.budget",
        "local.kill",
        "local.message",
      ],
    }, COL.agent);

    // Only to the agent this bundle is actually deployed as. Drawn to every
    // agent, this edge told you the bundle you have open ships as each of them.
    if (ws && isDeployedFrom(agent, openBundleName)) link("bundle", id, "deploy", "deploy");

    // Channel: the agent's front door.
    // One node per binding: an agent answering in two channels has two front
    // doors, and drawing one of them was the diagram asserting the other did
    // not exist.
    for (const channel of channelsOf(agent)) {
      const chId = `channel:${agent.id}:${channel.address}`;
      add({
        id: chId,
        kind: "channel",
        label: channel.kind === "slack" ? "Slack" : (channel.kind ?? "channel"),
        sub: channel.address,
        status: "known",
        detail: {
          Kind: channel.kind,
          Channel: channel.address,
        },
        actions: ["local.comms", "cluster.comms", "local.message"],
      }, COL.external);
      link(chId, id, "flow", "mention");
    }

    if (agent.model) {
      const modelId = `model:${agent.model}`;
      if (!has(modelId)) {
        add({
          id: modelId,
          kind: "model",
          label: agent.model,
          sub: "model",
          status: "known",
          actions: ["local.overrides", "cluster.overrides"],
        }, COL.external);
      }
      link(`agent:${agent.id}`, modelId, "flow", "inference");
    }
  });

  // --- what is actually running -------------------------------------------
  const runners = sources.samples.filter((s) => s.role === "runner");
  runners.forEach((sample) => {
    const id = `runner:${sample.name}`;
    add({
      id,
      kind: "agent",
      label: sample.name,
      sub: sample.state === "running" ? "runner · live" : `runner · ${sample.state}`,
      status: sample.state === "running" ? "live" : "known",
      metric: { cpu: sample.cpuPercent, mem: sample.memBytes },
      detail: {
        Image: sample.image ?? undefined,
        State: sample.state,
        Container: sample.id.slice(0, 12),
      },
      actions: ["skill.status", "skill.message", "skill.approvals", "skill.down"],
    }, COL.runtime);
    // Attribute the runner to its agent when the name says which one; otherwise
    // hang it off the bundle, which is what `skill up` actually booted.
    const owner = sources.agents.find((a) => sample.agent === a.name || sample.name.includes(a.name));
    if (owner) link(`agent:${owner.id}`, id, "flow", "sandbox");
    else if (ws) link("bundle", id, "flow", "skill up");
  });

  // --- the platform's own infrastructure -----------------------------------
  // Canonical roles, matched exactly. `startsWith` was the original bug here:
  // roles used to be raw compose service names, so `curie-api` never matched
  // `api` and most of the platform silently vanished from the graph.
  //
  // One-shot jobs (`rustfs-init`, `curie-migrate`) are excluded on purpose --
  // they are not part of the running topology, and drawing an exited init
  // container next to the store it initialised is noise, not information.
  const INFRA_ROLES = new Set([
    "api",
    "worker",
    "dispatcher",
    "postgres",
    "valkey",
    "langfuse",
    "clickhouse",
    "objectstore",
    "otel",
    "model",
  ]);
  const infra = sources.samples.filter((s) => INFRA_ROLES.has(s.role));
  const seenRole = new Set<string>();
  for (const sample of infra) {
    if (seenRole.has(sample.role)) continue;
    seenRole.add(sample.role);
    const id = `infra:${sample.role}`;
    add({
      id,
      kind: "infra",
      role: sample.role,
      label: sample.role,
      sub: sample.state === "running" ? "live" : sample.state,
      status: sample.state === "running" ? "live" : "known",
      metric: { cpu: sample.cpuPercent, mem: sample.memBytes },
      detail: {
        // What it does comes first. The identifiers are the answer to "which
        // container is this", which is a different and much rarer question.
        About: ROLE_ABOUT[sample.role],
        State: sample.health ? `${sample.state} (${sample.health})` : sample.state,
        Up: uptime(sample.startedAt, sample.at),
        CPU: sample.cpuPercent === null ? undefined : `${sample.cpuPercent.toFixed(1)}%`,
        Memory: sample.memBytes === null ? undefined : bytes(sample.memBytes),
        Ports: publishedPorts(sample.ports),
        Container: sample.name,
        Image: sample.image ?? undefined,
      },
      actions: ["local.status", "local.rebuild", "local.observability"],
    }, COL.infra + (FLOW_DEPTH[sample.role] ?? 0));
  }
  // The message path the product's own architecture doc describes, drawn only
  // between components that are actually present.
  if (has("infra:dispatcher") && has("infra:valkey")) link("infra:dispatcher", "infra:valkey", "flow", "enqueue");
  if (has("infra:valkey") && has("infra:worker")) link("infra:valkey", "infra:worker", "flow", "consume");
  if (has("infra:worker") && has("infra:api")) link("infra:worker", "infra:api", "flow", "aci");
  if (has("infra:api") && has("infra:postgres")) link("infra:api", "infra:postgres", "data", "state");
  if (has("infra:api") && has("infra:objectstore")) link("infra:api", "infra:objectstore", "data", "bundles");
  if (has("infra:worker") && has("infra:langfuse")) link("infra:worker", "infra:langfuse", "data", "traces");
  for (const runner of runners) {
    if (has("infra:worker")) link("infra:worker", `runner:${runner.name}`, "flow", "sandbox");
  }

  // --- position, then overlay what the operator saved or drew ----------------
  const built = materialise(placed);
  const nodes: GraphNode[] = built.nodes.map((n) => {
    const saved = doc.positions[n.id];
    return saved ? { ...n, x: saved.x, y: saved.y } : n;
  });

  for (const extra of doc.extraNodes) {
    const saved = doc.positions[extra.id];
    nodes.push({ ...extra, ...(saved ?? { x: extra.x, y: extra.y }), userAdded: true });
  }
  const byId = new Set(nodes.map((n) => n.id));
  for (const extra of doc.extraEdges) {
    // Drop an edge whose endpoint no longer exists rather than drawing a line
    // into empty space.
    if (byId.has(extra.from) && byId.has(extra.to)) edges.push(extra);
  }

  return {
    nodes,
    edges: edges.filter((e) => byId.has(e.from) && byId.has(e.to)),
    // Lanes describe the derived layout. Once the operator has dragged nodes
    // around, the bands no longer describe where anything is, so they are
    // dropped rather than left pointing at the wrong columns.
    lanes: Object.keys(doc.positions).length ? [] : built.lanes,
  };
}

/** Bounding box of the graph, for fit-to-view. */
export function bounds(nodes: readonly GraphNode[]) {
  if (!nodes.length) return { minX: 0, minY: 0, maxX: 800, maxY: 500 };
  return nodes.reduce(
    (acc, n) => ({
      minX: Math.min(acc.minX, n.x),
      minY: Math.min(acc.minY, n.y),
      maxX: Math.max(acc.maxX, n.x + NODE_W),
      maxY: Math.max(acc.maxY, n.y + NODE_H),
    }),
    { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity },
  );
}
