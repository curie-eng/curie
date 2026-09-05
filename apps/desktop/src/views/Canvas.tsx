// The canvas: agents, their pipelines, their integrations, and the infra under
// them, as one editable picture.
//
// Hand-rolled SVG rather than a graph library. Three reasons, in order: the app
// is meant to stay light and a flow library is a larger dependency than the
// entire renderer; the nodes must obey the design tokens exactly, which means
// fighting a library's own styling; and the interactions here are few enough
// (pan, zoom, drag, select, connect) that owning them is cheaper than
// configuring someone else's.
//
// The important behaviour is that selecting a node does not just show you
// properties -- it shows you the `curie` commands that node is a valid target
// for, pre-filled with its identity. The canvas is a way to reach the CLI by
// pointing at the thing you mean.

import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type MouseEvent as ReactMouseEvent,
} from "react";

import { useApp, type AgentSummary } from "../bridge/app";
import { useResources } from "../bridge/resources";
import { bridge } from "../bridge/bridge";
import {
  buildGraph,
  bounds,
  isGraphDoc,
  migrateDoc,
  EMPTY_DOC,
  NODE_H,
  NODE_W,
  type Graph,
  type GraphDoc,
  type GraphEdge,
  type GraphNode,
  type Lane as GraphLane,
  LAYOUT,
} from "../graph/model";
import { command } from "../lib/manifest";
import { percent } from "../lib/format";
import { CommandForm } from "./CommandForm";
import { AgentSheet } from "./AgentSheet";
import { ACCENT, F, FONT, HUE, KIND_COLOR, LINE, R, S, STATUS, T, roleColor, tint, type NodeKind } from "../tokens";
import { Badge, Button, EmptyState, Group, Mono, Notice, SectionHeader, Select } from "../primitives";

interface Viewport {
  x: number;
  y: number;
  scale: number;
}

const MIN_SCALE = 0.3;
const MAX_SCALE = 2.2;

export function Canvas() {
  const app = useApp();
  const res = useResources();

  const [doc, setDoc] = useState<GraphDoc>(EMPTY_DOC);
  const [loaded, setLoaded] = useState(false);
  const [view, setView] = useState<Viewport>({ x: 40, y: 20, scale: 0.9 });
  const [selected, setSelected] = useState<string | null>(null);
  const [connecting, setConnecting] = useState<string | null>(null);
  const [hovered, setHovered] = useState<string | null>(null);
  const [panning, setPanning] = useState(false);
  const [adding, setAdding] = useState<NodeKind | "">("");
  const svgRef = useRef<SVGSVGElement>(null);
  // `moved` is what separates a drag from a click. Without it, selecting a node
  // saved its current position as a "custom layout", which then pinned every
  // node and permanently disabled the auto-fit and any future relayout -- from a
  // single click, with nothing on screen to say it had happened.
  const drag = useRef<{ id: string; dx: number; dy: number; moved: boolean } | null>(null);
  const pan = useRef<{ x: number; y: number; vx: number; vy: number } | null>(null);
  // The latest doc, readable from the window-level mouseup handler without
  // making that handler a dependency of every render. Written in an effect
  // rather than during render, because a render can be discarded and a ref
  // write cannot be taken back.
  const docRef = useRef(doc);
  useEffect(() => {
    docRef.current = doc;
  }, [doc]);

  useEffect(() => {
    void bridge()
      .graph.load()
      .then((value) => {
        // Coordinates from an older layout would pin nodes where an algorithm
        // that no longer exists put them.
        if (isGraphDoc(value)) setDoc(migrateDoc(value));
        setLoaded(true);
      });
  }, []);

  const graph: Graph = useMemo(
    () => buildGraph({ workspace: app.workspace, agents: app.agents, samples: res.samples }, doc),
    [app.workspace, app.agents, res.samples, doc],
  );

  // Persist on change, but not on the first render -- writing the empty doc
  // before `load` returns would erase the saved layout.
  const persist = useCallback(
    (next: GraphDoc) => {
      setDoc(next);
      if (loaded) void bridge().graph.save(next);
    },
    [loaded],
  );

  const toGraphSpace = useCallback(
    (clientX: number, clientY: number) => {
      const rect = svgRef.current?.getBoundingClientRect();
      if (!rect) return { x: 0, y: 0 };
      return {
        x: (clientX - rect.left - view.x) / view.scale,
        y: (clientY - rect.top - view.y) / view.scale,
      };
    },
    [view],
  );

  // Pointer handling lives on the SVG rather than per node so a drag that leaves
  // a node's box does not stall -- the classic "node sticks to the cursor edge"
  // bug.
  useEffect(() => {
    const move = (e: MouseEvent) => {
      if (drag.current) {
        const p = toGraphSpace(e.clientX, e.clientY);
        const id = drag.current.id;
        drag.current.moved = true;
        const x = Math.round(p.x - drag.current.dx);
        const y = Math.round(p.y - drag.current.dy);
        setDoc((prev) => ({ ...prev, positions: { ...prev.positions, [id]: { x, y } } }));
        return;
      }
      if (pan.current) {
        setView((v) => ({
          ...v,
          x: pan.current!.vx + (e.clientX - pan.current!.x),
          y: pan.current!.vy + (e.clientY - pan.current!.y),
        }));
      }
    };
    const up = () => {
      setPanning(false);
      // One save per gesture, not one per mouse-move frame -- and read the doc
      // from a ref rather than from inside a state updater, which React may
      // invoke twice.
      if (drag.current?.moved && loaded) void bridge().graph.save(docRef.current);
      drag.current = null;
      pan.current = null;
    };
    window.addEventListener("mousemove", move);
    window.addEventListener("mouseup", up);
    return () => {
      window.removeEventListener("mousemove", move);
      window.removeEventListener("mouseup", up);
    };
  }, [toGraphSpace, loaded]);

  const fit = useCallback(() => {
    const rect = svgRef.current?.getBoundingClientRect();
    if (!rect || !graph.nodes.length) return;
    const b = bounds(graph.nodes);
    // Never magnify. Fitting a three-node graph to the pane blows the nodes up
    // to 120% and makes a sparse graph look like a rendering fault rather than
    // like three containers.
    const scale = Math.min(
      1,
      MAX_SCALE,
      Math.max(
        MIN_SCALE,
        Math.min(
          (rect.width - 80) / (b.maxX - b.minX),
          (rect.height - 80) / (b.maxY - b.minY),
        ),
      ),
    );
    setView({
      scale,
      x: 40 - b.minX * scale,
      y: (rect.height - (b.maxY - b.minY) * scale) / 2 - b.minY * scale,
    });
  }, [graph.nodes]);

  // Keep the whole graph in view while it is still being discovered.
  //
  // Fitting once on mount is not enough: the graph is derived from three sources
  // that arrive at different times, so the first non-empty render is usually a
  // fraction of the eventual graph. Fitting to that and stopping leaves the
  // containers -- which arrive last -- off the right edge. So the fit is keyed to
  // the node count and re-runs as the graph grows.
  //
  // It stops the moment the operator has a layout of their own: once anything
  // has been dragged, re-framing the viewport under them would be rude. "Reset
  // layout" clears those positions and bumps `fitNonce` to ask for a fresh pass.
  //
  // The latch is a ref, not state: "have I already fitted this shape" changes
  // nothing about what renders, and making it state would mean a setState inside
  // the very effect doing the fitting.
  const [fitNonce, setFitNonce] = useState(0);
  const fittedFor = useRef("");
  const hasCustomLayout = Object.keys(doc.positions).length > 0;

  useEffect(() => {
    if (!loaded || !graph.nodes.length) return;
    const shape = `${fitNonce}:${hasCustomLayout ? "custom" : graph.nodes.length}`;
    if (fittedFor.current === shape) return;
    fittedFor.current = shape;
    fit();
  }, [fitNonce, loaded, graph.nodes.length, hasCustomLayout, fit]);

  // What the graph cannot show, and the way to fix it.
  const missing = useMemo(() => {
    const out: { label: string; onClick(): void }[] = [];
    if (!app.workspace) {
      out.push({
        label: "No bundle open — nothing you author is shown",
        onClick: () => void app.openWorkspace(),
      });
    }
    if (!app.api?.reachable) {
      out.push({
        label: "API unreachable — no agents or channels are shown",
        onClick: () => app.runCommand("local.up"),
      });
    }
    return out;
  }, [app]);

  const selectedNode = graph.nodes.find((n) => n.id === selected) ?? null;

  // Tracing a path is the main thing you do with a graph, and it is impossible
  // when everything is drawn at the same weight. Hovering (or selecting) a node
  // keeps it and its immediate neighbours lit and drops everything else back.
  const focusId = hovered ?? selected;
  const related = useMemo(() => {
    if (!focusId) return null;
    const set = new Set<string>([focusId]);
    for (const e of graph.edges) {
      if (e.from === focusId) set.add(e.to);
      if (e.to === focusId) set.add(e.from);
    }
    return set;
  }, [focusId, graph.edges]);

  const addNode = (kind: NodeKind) => {
    const rect = svgRef.current?.getBoundingClientRect();
    const centre = rect
      ? toGraphSpace(rect.left + rect.width / 2, rect.top + rect.height / 2)
      : { x: 200, y: 200 };
    const id = `planned:${kind}:${Date.now().toString(36)}`;
    const node: GraphNode = {
      id,
      kind,
      label: `New ${kind}`,
      sub: "planned",
      x: Math.round(centre.x - NODE_W / 2),
      y: Math.round(centre.y - NODE_H / 2),
      status: "planned",
      userAdded: true,
    };
    persist({ ...doc, extraNodes: [...doc.extraNodes, node] });
    setSelected(id);
    setAdding("");
  };

  const removeNode = (id: string) => {
    persist({
      ...doc,
      extraNodes: doc.extraNodes.filter((n) => n.id !== id),
      extraEdges: doc.extraEdges.filter((e) => e.from !== id && e.to !== id),
    });
    setSelected(null);
  };

  const renameNode = (id: string, label: string) => {
    persist({
      ...doc,
      extraNodes: doc.extraNodes.map((n) => (n.id === id ? { ...n, label } : n)),
    });
  };

  const connect = (from: string, to: string) => {
    if (from === to) return;
    const id = `planned:${from}->${to}`;
    if (doc.extraEdges.some((e) => e.id === id)) return;
    persist({
      ...doc,
      extraEdges: [...doc.extraEdges, { id, from, to, kind: "planned", label: "planned" }],
    });
  };

  if (!app.workspace && !app.agents.length && !res.samples.length) {
    return (
      <EmptyState
        title="Nothing to draw yet"
        action={
          <div style={{ display: "flex", gap: 8, justifyContent: "center" }}>
            <Button tone="primary" onClick={() => void app.openWorkspace()}>
              Open a bundle
            </Button>
            <Button onClick={() => app.runCommand("local.up")}>Start the local stack</Button>
          </div>
        }
      >
        The canvas is built from what actually exists: the bundle you have open, the agents the
        platform API reports, and the containers Docker is running. Open a bundle or bring a tier up
        and it fills in.
      </EmptyState>
    );
  }

  return (
    <div style={{ display: "flex", gap: 14, height: "100%", minHeight: 520 }}>
      <div
        style={{
          flex: 1,
          minWidth: 0,
          position: "relative",
          border: `1px solid ${LINE.separator}`,
          borderRadius: R.group,
          overflow: "hidden",
          background: S.well,
        }}
      >
        <svg
          ref={svgRef}
          width="100%"
          height="100%"
          style={{ display: "block", cursor: panning ? "grabbing" : "default" }}
          onMouseDown={(e) => {
            if (e.target !== e.currentTarget && !(e.target as Element).classList.contains("bg")) return;
            setSelected(null);
            setConnecting(null);
            setPanning(true);
            pan.current = { x: e.clientX, y: e.clientY, vx: view.x, vy: view.y };
          }}
          onWheel={(e) => {
            // Trackpad pinch arrives as ctrlKey+wheel; a plain wheel pans, which
            // is what people expect from a canvas rather than a document.
            if (!e.ctrlKey && !e.metaKey) {
              setView((v) => ({ ...v, x: v.x - e.deltaX, y: v.y - e.deltaY }));
              return;
            }
            const rect = svgRef.current!.getBoundingClientRect();
            const px = e.clientX - rect.left;
            const py = e.clientY - rect.top;
            setView((v) => {
              const scale = Math.min(MAX_SCALE, Math.max(MIN_SCALE, v.scale * (1 - e.deltaY / 400)));
              // Zoom about the cursor, not the origin.
              return {
                scale,
                x: px - ((px - v.x) / v.scale) * scale,
                y: py - ((py - v.y) / v.scale) * scale,
              };
            });
          }}
        >
          <defs>
            <pattern id="grid" width={24} height={24} patternUnits="userSpaceOnUse">
              <circle cx={1} cy={1} r={0.9} fill={LINE.separator} />
            </pattern>
            <marker id="arrow" viewBox="0 0 8 8" refX={7} refY={4} markerWidth={7} markerHeight={7} orient="auto">
              <path d="M0,0 L8,4 L0,8 z" fill={LINE.strong} />
            </marker>
          </defs>
          <rect className="bg" width="100%" height="100%" fill={S.well} />
          <rect
            className="bg"
            width="100%"
            height="100%"
            fill="url(#grid)"
            style={{ transform: `translate(${view.x % (24 * view.scale)}px, ${view.y % (24 * view.scale)}px)` }}
          />

          <g transform={`translate(${view.x},${view.y}) scale(${view.scale})`}>
            {/* Lane bands first, so everything else sits on top of them. */}
            {graph.lanes.map((lane) => (
              <Lane key={`${lane.label}:${lane.x}`} lane={lane} nodes={graph.nodes} />
            ))}
            {graph.edges.map((edge) => (
              <Edge
                key={edge.id}
                edge={edge}
                nodes={graph.nodes}
                dimmed={!!related && !(related.has(edge.from) && related.has(edge.to))}
                lit={!!focusId && (edge.from === focusId || edge.to === focusId)}
              />
            ))}
            {graph.nodes.map((node) => (
              <Node
                key={node.id}
                node={node}
                selected={node.id === selected}
                connecting={connecting === node.id}
                dimmed={!!related && !related.has(node.id)}
                onMouseEnter={() => setHovered(node.id)}
                onMouseLeave={() => setHovered(null)}
                onMouseDown={(e) => {
                  e.stopPropagation();
                  if (connecting) {
                    connect(connecting, node.id);
                    setConnecting(null);
                    return;
                  }
                  setSelected(node.id);
                  const p = toGraphSpace(e.clientX, e.clientY);
                  drag.current = { id: node.id, dx: p.x - node.x, dy: p.y - node.y, moved: false };
                }}
              />
            ))}
          </g>
        </svg>

        {/* The graph is only ever as complete as its sources. With no bundle
            open and no API reachable it shows infrastructure and nothing else,
            which reads as a broken diagram unless the canvas says why. */}
        {missing.length ? (
          <div
            style={{
              position: "absolute",
              left: 12,
              top: 12,
              maxWidth: "58%",
              display: "flex",
              flexDirection: "column",
              gap: 6,
              alignItems: "flex-start",
            }}
          >
            {missing.map((m) => (
              <button
                key={m.label}
                onClick={m.onClick}
                style={{
                  border: "none",
                  background: tint(STATUS.warn, 0.11),
                  borderRadius: R.pill,
                  padding: "4px 11px",
                  ...F.footnote,
                  color: STATUS.warn,
                  cursor: "default",
                  textAlign: "left",
                }}
              >
                {m.label}
              </button>
            ))}
          </div>
        ) : null}

        <div
          style={{
            position: "absolute",
            left: 12,
            bottom: 12,
            display: "flex",
            gap: 6,
            alignItems: "center",
          }}
        >
          <Button size="sm" onClick={fit}>
            Fit
          </Button>
          <Button size="sm" onClick={() => setView((v) => ({ ...v, scale: Math.min(MAX_SCALE, v.scale * 1.2) }))}>
            +
          </Button>
          <Button size="sm" onClick={() => setView((v) => ({ ...v, scale: Math.max(MIN_SCALE, v.scale / 1.2) }))}>
            −
          </Button>
          <span style={{ fontSize: 11, color: T.tertiary, marginLeft: 4 }}>
            {Math.round(view.scale * 100)}%
          </span>
        </div>

        <div style={{ position: "absolute", right: 12, top: 12, display: "flex", gap: 6 }}>
          <Select
            value={adding}
            onChange={(e) => {
              const kind = e.target.value as NodeKind | "";
              if (kind) addNode(kind);
            }}
            style={{ width: 150 }}
          >
            <option value="">Add planned node…</option>
            <option value="agent">Agent</option>
            <option value="channel">Channel</option>
            <option value="mcp">MCP server</option>
            <option value="model">Model</option>
            <option value="infra">Infrastructure</option>
            <option value="repo">Repository</option>
          </Select>
          <Button
            size="sm"
            onClick={() => {
              persist({ ...doc, layout: LAYOUT, positions: {} });
              setFitNonce((n) => n + 1);
            }}
            title="Discard saved positions and re-derive the layout"
          >
            Reset layout
          </Button>
        </div>

        <Legend />
      </div>

      <Inspector
        key={selectedNode?.id ?? "none"}
        node={selectedNode}
        onClose={() => setSelected(null)}
        onRename={renameNode}
        onDelete={removeNode}
        onStartConnect={(id) => setConnecting(id)}
        connecting={connecting}
      />
    </div>
  );
}

function Legend() {
  const entries: [NodeKind, string][] = [
    ["agent", "Agent"],
    ["channel", "Channel"],
    ["model", "Model"],
    ["mcp", "MCP"],
    ["infra", "Infra"],
    ["repo", "Bundle"],
  ];
  return (
    <div
      style={{
        position: "absolute",
        right: 12,
        bottom: 12,
        display: "flex",
        gap: 10,
        padding: "6px 10px",
        // `${S.raised}dd` before, which stopped meaning anything the moment the
        // token became a var(): you cannot append a hex alpha to a variable.
        background: tint(S.raised, 0.87),
        border: `1px solid ${LINE.separator}`,
        borderRadius: R.control,
        fontSize: 10,
        color: T.tertiary,
      }}
    >
      {entries.map(([kind, label]) => (
        <span key={kind} style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
          <span
            style={{
              width: 8,
              height: 8,
              borderRadius: 2,
              background: KIND_COLOR[kind],
              display: "inline-block",
            }}
          />
          {label}
        </span>
      ))}
    </div>
  );
}

/** A labelled band behind one stage of the pipeline. Cheap, and it turns six
 *  boxes and five arrows into something you can read as an architecture. */
function Lane({ lane, nodes }: { lane: GraphLane; nodes: readonly GraphNode[] }) {
  const inLane = nodes.filter((n) => n.x >= lane.x && n.x < lane.x + lane.width);
  if (!inLane.length) return null;
  const top = Math.min(...inLane.map((n) => n.y)) - 34;
  const bottom = Math.max(...inLane.map((n) => n.y + NODE_H)) + 14;
  return (
    <g style={{ pointerEvents: "none" }}>
      <rect
        x={lane.x - 14}
        y={top}
        width={lane.width + 28}
        height={bottom - top}
        rx={12}
        // `S.stripe` is 2% ink -- meant for alternating table rows, where the
        // eye only has to feel a rhythm. A labelled band that groups nodes has
        // to actually be seen, so it takes a real fill and an edge.
        fill={S.subtle}
        stroke={LINE.separator}
      />
      <text
        x={lane.x - 4}
        y={top + 15}
        fill={T.quaternary}
        fontSize={9.5}
        fontWeight={600}
        letterSpacing={0.6}
        fontFamily={FONT.ui}
      >
        {lane.label.toUpperCase()}
      </text>
    </g>
  );
}

function Edge({
  edge,
  nodes,
  dimmed,
  lit,
}: {
  edge: GraphEdge;
  nodes: readonly GraphNode[];
  dimmed?: boolean;
  lit?: boolean;
}) {
  const from = nodes.find((n) => n.id === edge.from);
  const to = nodes.find((n) => n.id === edge.to);
  if (!from || !to) return null;

  const y1 = from.y + NODE_H / 2;
  const y2 = to.y + NODE_H / 2;
  const dx = to.x - from.x;

  // Three cases, because one rule cannot draw all of them tidily.
  //
  //  - Same column (|dx| smaller than a node): both ends leave the RIGHT edge
  //    and the curve arcs out and back. Treating these as forward made them
  //    exit right and enter left, which is a loop all the way around the
  //    outside -- the tangle you get with no bundle open, when infrastructure is
  //    the only populated column.
  //  - Forward: exit right, enter left. The normal pipeline case.
  //  - Backward: exit left, enter right. A channel sits in the rightmost column
  //    and points back at its agent.
  const sameColumn = Math.abs(dx) < NODE_W;
  let d: string;
  if (sameColumn) {
    const x1 = from.x + NODE_W;
    const x2 = to.x + NODE_W;
    const arc = 34 + Math.min(60, Math.abs(y2 - y1) * 0.35);
    d = `M${x1},${y1} C${x1 + arc},${y1} ${x2 + arc},${y2} ${x2},${y2}`;
  } else {
    const forward = dx > 0;
    const x1 = forward ? from.x + NODE_W : from.x;
    const x2 = forward ? to.x : to.x + NODE_W;
    const reach = Math.max(36, Math.abs(x2 - x1) * 0.45);
    const c1 = forward ? x1 + reach : x1 - reach;
    const c2 = forward ? x2 - reach : x2 + reach;
    d = `M${x1},${y1} C${c1},${y1} ${c2},${y2} ${x2},${y2}`;
  }

  // The label sits at the curve's midpoint. Same-column arcs bulge right, so
  // their label follows, otherwise every arc in a column stacks its text in the
  // same place.
  const labelX = sameColumn
    ? Math.max(from.x, to.x) + NODE_W + 26
    : (from.x + NODE_W + to.x) / 2;
  const labelY = (y1 + y2) / 2;

  // A plain flow edge used to draw in `LINE.strong` at 0.7 opacity, which lands
  // around 18% ink -- a hairline meant for separating stacked rows, not for a
  // line crossing open canvas. It takes text ink instead.
  const color =
    edge.kind === "data"
      ? HUE.cyan
      : edge.kind === "deploy"
        ? HUE.violet
        : edge.kind === "planned"
          ? STATUS.warn
          : T.quaternary;

  return (
    <g style={{ transition: "opacity 120ms ease" }} opacity={dimmed ? 0.12 : 1}>
      <path
        d={d}
        fill="none"
        stroke={color}
        strokeWidth={lit ? 2 : 1.4}
        strokeDasharray={edge.kind === "planned" ? "5 4" : undefined}
        markerEnd="url(#arrow)"
        opacity={lit ? 1 : 0.9}
      />
      {edge.label ? (
        <text
          x={labelX}
          y={labelY - 5}
          textAnchor={sameColumn ? "start" : "middle"}
          fill={T.tertiary}
          fontSize={9}
          fontFamily={FONT.ui}
          style={{ pointerEvents: "none" }}
        >
          {edge.label}
        </text>
      ) : null}
    </g>
  );
}

function Node({
  node,
  selected,
  connecting,
  dimmed,
  onMouseDown,
  onMouseEnter,
  onMouseLeave,
}: {
  node: GraphNode;
  selected: boolean;
  connecting: boolean;
  dimmed?: boolean;
  onMouseDown(e: ReactMouseEvent): void;
  onMouseEnter?(): void;
  onMouseLeave?(): void;
}) {
  // Role first, kind second. `kind` is coarse on purpose -- everything the
  // platform runs is `infra` -- so colouring by it alone drew every service in
  // the same grey and the graph read as one undifferentiated cluster.
  const color = node.role ? roleColor(node.role) : KIND_COLOR[node.kind];
  const planned = node.status === "planned";
  // Load is drawn against one core, not against the machine: a container at
  // 60% of a core is the interesting reading, and dividing by twelve would
  // flatten every bar to nothing.
  const load = node.metric?.cpu ?? null;
  const loadRatio = load === null ? null : Math.min(1, load / 100);
  return (
    <g
      transform={`translate(${node.x},${node.y})`}
      onMouseDown={onMouseDown}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
      opacity={dimmed ? 0.22 : 1}
      style={{ cursor: "grab", transition: "opacity 120ms ease" }}
    >
      <rect
        width={NODE_W}
        height={NODE_H}
        rx={10}
        fill={planned ? "transparent" : S.raised}
        stroke={selected || connecting ? color : planned ? `${color}88` : LINE.border}
        strokeWidth={selected || connecting ? 1.8 : 1}
        strokeDasharray={planned ? "5 4" : undefined}
      />
      {/* A left rail rather than a filled header: colour identifies the kind
          without the node becoming a block of colour at a distance. */}
      <rect width={3} height={NODE_H} rx={1.5} fill={color} opacity={planned ? 0.5 : 1} />
      <text x={14} y={22} fill={T.primary} fontSize={12} fontWeight={600} fontFamily={FONT.ui}>
        {truncate(node.label, 20)}
      </text>
      {node.sub ? (
        <text x={14} y={38} fill={T.tertiary} fontSize={10} fontFamily={FONT.mono}>
          {truncate(node.sub, 24)}
        </text>
      ) : null}
      {node.status === "live" ? (
        <circle cx={NODE_W - 13} cy={14} r={3.5} fill={ACCENT}>
          <animate attributeName="opacity" values="1;0.35;1" dur="1.8s" repeatCount="indefinite" />
        </circle>
      ) : null}

      {/* Live load, as a hairline along the bottom edge. This is what makes the
          canvas a view of a running system rather than a diagram of one. */}
      {loadRatio !== null ? (
        <>
          <rect
            x={10}
            y={NODE_H - 6}
            width={NODE_W - 20}
            height={2.5}
            rx={1.25}
            fill={LINE.separator}
          />
          <rect
            x={10}
            y={NODE_H - 6}
            width={Math.max(2, (NODE_W - 20) * loadRatio)}
            height={2.5}
            rx={1.25}
            fill={loadRatio > 0.85 ? STATUS.warn : color}
          />
          <text
            x={NODE_W - 13}
            y={NODE_H - 9}
            textAnchor="end"
            fill={T.quaternary}
            fontSize={8.5}
            fontFamily={FONT.mono}
          >
            {percent(load, 0)}
          </text>
        </>
      ) : null}
    </g>
  );
}

function truncate(text: string, max: number): string {
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

function Inspector({
  node,
  onClose,
  onRename,
  onDelete,
  onStartConnect,
  connecting,
}: {
  node: GraphNode | null;
  onClose(): void;
  onRename(id: string, label: string): void;
  onDelete(id: string): void;
  onStartConnect(id: string): void;
  connecting: string | null;
}) {
  // Keyed by the caller on `node.id`, so the chosen action resets by remount
  // rather than by an effect that would briefly render the previous command.
  const [action, setAction] = useState<string | null>(null);
  const [sheetAgent, setSheetAgent] = useState<AgentSummary | null>(null);
  const app = useApp();

  // Node ids for API agents are `agent:<uuid>`, so the match is on the id the
  // graph was built from rather than on the label -- two agents can share a
  // display name, and a runner node borrows one.
  const agent =
    node && node.kind === "agent" && node.id.startsWith("agent:")
      ? (app.agents.find((a) => `agent:${a.id}` === node.id) ?? null)
      : null;

  if (!node) {
    return (
      <div style={{ width: 330, flex: "none" }}>
        <SectionHeader>Canvas</SectionHeader>
        <Group style={{ padding: 14 }}>
          <div style={{ ...F.callout, color: T.tertiary, lineHeight: 1.6 }}>
            Select a node to see what it is and what you can do to it. Drag to move, scroll to pan,
            pinch or <Mono>⌘</Mono>-scroll to zoom.
            <br />
            <br />
            The graph is rebuilt from live state every render — the open bundle, the agents the API
            reports, and the running containers. Only your layout and any nodes you add are saved.
          </div>
        </Group>
      </div>
    );
  }

  const cmd = action ? command(action) : null;

  return (
    <div style={{ width: 330, flex: "none", overflow: "auto" }}>
      <SectionHeader>{node.userAdded ? "Planned node" : "Selected"}</SectionHeader>
      <Group style={{ padding: 14 }}>
        <div style={{ display: "flex", alignItems: "flex-start", gap: 8, marginBottom: 10 }}>
          <div style={{ flex: 1, minWidth: 0 }}>
            {node.userAdded ? (
              <input
                value={node.label}
                onChange={(e) => onRename(node.id, e.target.value)}
                style={{
                  width: "100%",
                  background: "transparent",
                  border: `1px solid ${LINE.separator}`,
                  borderRadius: 6,
                  color: T.primary,
                  fontSize: 14,
                  fontWeight: 600,
                  padding: "3px 6px",
                }}
              />
            ) : (
              <div style={{ fontSize: 14, fontWeight: 600 }}>{node.label}</div>
            )}
            <div style={{ marginTop: 5, display: "flex", gap: 6, flexWrap: "wrap" }}>
              <Badge color={KIND_COLOR[node.kind]}>{node.kind}</Badge>
              <Badge color={node.status === "live" ? ACCENT : node.status === "planned" ? STATUS.warn : T.tertiary}>
                {node.status}
              </Badge>
            </div>
          </div>
          <Button size="sm" tone="plain" onClick={onClose}>
            ✕
          </Button>
        </div>

        {node.detail ? (
          <div style={{ display: "grid", gap: 5, fontSize: 11, marginBottom: 12 }}>
            {Object.entries(node.detail)
              .filter(([, v]) => v)
              .map(([k, v]) => (
                <div key={k} style={{ display: "grid", gridTemplateColumns: "88px 1fr", gap: 8 }}>
                  <span style={{ color: T.tertiary }}>{k}</span>
                  <Mono style={{ color: T.secondary, fontSize: 10, wordBreak: "break-all" }}>{v}</Mono>
                </div>
              ))}
          </div>
        ) : null}

        {node.status === "planned" ? (
          <div style={{ marginBottom: 12 }}>
            <Notice tone="warn" title="Planned, not real">
              This node exists only on your canvas. Nothing is deployed until you run the command
              that creates it.
            </Notice>
          </div>
        ) : null}

        <div style={{ display: "flex", gap: 6, marginBottom: 14, flexWrap: "wrap" }}>
          <Button
            size="sm"
            onClick={() => onStartConnect(node.id)}
            tone={connecting === node.id ? "primary" : "default"}
          >
            {connecting === node.id ? "Pick a target…" : "Draw a link"}
          </Button>
          {/* A deployed agent has twenty-six commands of its own, and the four or
              five listed below are the subset this node happens to name. The
              sheet is the whole set, and it is the same one the Overview's rows
              open -- an agent should not mean different things on two screens. */}
          {agent ? (
            <Button size="sm" onClick={() => setSheetAgent(agent)}>
              Everything for this agent
            </Button>
          ) : null}
          {node.userAdded ? (
            <Button size="sm" tone="danger" onClick={() => onDelete(node.id)}>
              Remove
            </Button>
          ) : null}
        </div>

        {node.actions?.length ? (
          <>
            <SectionHeader>Run against this</SectionHeader>
            <div style={{ display: "flex", flexDirection: "column", gap: 4, marginBottom: 12 }}>
              {node.actions.map((id) => {
                const c = command(id);
                if (!c) return null;
                const open = action === id;
                return (
                  <button
                    key={id}
                    onClick={() => setAction(open ? null : id)}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: 7,
                      background: open ? S.selected : "transparent",
                      border: `1px solid ${open ? LINE.border : LINE.separator}`,
                      borderRadius: R.control,
                      padding: "5px 8px",
                      cursor: "pointer",
                      textAlign: "left",
                    }}
                  >
                    <Mono style={{ flex: 1, color: open ? T.primary : T.secondary, fontSize: 11 }}>
                      curie {c.path.join(" ")}
                    </Mono>
                    {c.risk === "destructive" ? <Badge color={STATUS.danger}>!</Badge> : null}
                  </button>
                );
              })}
            </div>
          </>
        ) : null}

        {cmd ? (
          <div style={{ borderTop: `1px solid ${LINE.separator}`, paddingTop: 12 }}>
            <CommandForm
              key={cmd.id}
              cmd={cmd}
              compact
              // The node IS the target, so the form opens pointed at it.
              prefill={agent ? { positionals: [agent.name] } : undefined}
              onRan={() => setAction(null)}
            />
          </div>
        ) : null}
      </Group>

      {sheetAgent && !app.runTarget ? (
        <AgentSheet agent={sheetAgent} onClose={() => setSheetAgent(null)} />
      ) : null}
    </div>
  );
}
