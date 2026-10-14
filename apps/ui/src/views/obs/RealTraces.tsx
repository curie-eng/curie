import { useState } from "react";
import { C } from "../../tokens";
import { Card, Chip, CliHint, Dot, EmptyState, Notice, cliCommand } from "../../primitives";
import { hoverBg } from "../../lib/style";
import { useStore } from "../../state/store";
import { useTraces, useTrace } from "../../api/hooks";
import { promoteTraceToEvalCase } from "../../api/client";
import type { RawTrace, ObservationNode } from "../../api/client";

function str(o: RawTrace, key: string): string | null {
  const v = o[key];
  return typeof v === "string" ? v : typeof v === "number" ? String(v) : null;
}

// Live Runs list: real Langfuse traces via the API proxy. Rendered in place of
// the fixture list when the app is wired to a backend.
export function RealTracesList() {
  const { state, dispatch } = useStore();
  const agentId = state.tracesAgentId;
  const { data, loading, error } = useTraces(true, agentId);
  const grid = "1.9fr 1.1fr 1fr";

  if (loading) return <Notice padding="40px 20px" fontSize={13.5}>Loading traces…</Notice>;
  if (error) return <Notice padding="40px 20px" fontSize={13.5}>Could not load traces: {error}</Notice>;
  const traces = data ?? [];
  if (traces.length === 0)
    return (
      <Notice padding="40px 20px" fontSize={13.5}>
        {agentId
          ? "No traces for this agent yet. Send it a message to generate one."
          : "No traces yet. Send the agent a message to generate one."}
      </Notice>
    );

  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 14 }}>
        <div
          style={{
            flex: 1,
            display: "flex",
            alignItems: "center",
            gap: 8,
            background: C.input,
            border: "1px solid " + C.border,
            borderRadius: 8,
            padding: "7px 11px",
            fontFamily: C.mono,
            fontSize: 12.5,
            color: C.text2,
          }}
        >
          <span style={{ color: C.muted }}>trace</span>
          <span style={{ color: C.text }}>
            {agentId ? `{ source="langfuse", agent~"agent-${agentId}" }` : '{ source="langfuse" }'}
          </span>
          <span style={{ marginLeft: "auto", color: C.muted }}>{traces.length + " traces"}</span>
        </div>
        {agentId ? (
          <button
            type="button"
            data-testid="trace-filter-clear"
            onClick={() => dispatch({ type: "viewTraces", agentId: null })}
            style={{
              background: "transparent",
              border: "1px solid " + C.border,
              borderRadius: 20,
              padding: "5px 11px",
              color: C.text2,
              fontFamily: C.mono,
              fontSize: 12,
              cursor: "pointer",
            }}
          >
            agent filter · clear ✕
          </button>
        ) : null}
        <CliHint
          command={cliCommand(
            state.env === "prod"
              ? "cluster.observability.runs"
              : "local.observability.runs",
            {
              limit: "20",
              "agent-id": agentId ?? undefined,
            },
          )}
        />
        <Chip color={C.mutedStatus}>OTel · live</Chip>
      </div>
      <Card>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: grid,
            gap: 12,
            padding: "0 0 12px",
            fontSize: 12,
            color: C.muted,
            borderBottom: "1px solid " + C.border,
          }}
        >
          {["Name", "Trace", "When"].map((c) => (
            <div key={c}>{c}</div>
          ))}
        </div>
        {traces.map((t) => {
          const id = str(t, "id") ?? "";
          const name = str(t, "name") ?? "(unnamed trace)";
          const when = str(t, "timestamp") ?? "";
          return (
            <button
              key={id}
              type="button"
              data-testid="trace-row"
              onClick={() => dispatch({ type: "openTrace", id })}
              style={{
                display: "grid",
                gridTemplateColumns: grid,
                gap: 12,
                padding: "13px 0 13px 12px",
                alignItems: "center",
                borderBottom: "1px solid " + C.border,
                background: "transparent",
                border: "none",
                width: "100%",
                textAlign: "left",
                cursor: "pointer",
                color: C.text,
                fontSize: 13,
              }}
              {...hoverBg("transparent", C.hover)}
            >
              <div style={{ display: "flex", alignItems: "center", gap: 8, overflow: "hidden" }}>
                <Dot color={C.success} size={7} />
                <span style={{ whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", color: C.text2 }}>{name}</span>
              </div>
              <span style={{ fontFamily: C.mono, fontSize: 12, color: C.link, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{id}</span>
              <span style={{ color: C.muted, fontSize: 12.5, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{when}</span>
            </button>
          );
        })}
      </Card>
    </div>
  );
}

function typeColor(type: string): string {
  if (type === "GENERATION") return C.brand;
  if (type === "SPAN") return C.mutedStatus;
  if (type === "EVENT") return C.link;
  return C.text2;
}

function SpanRow({ node, depth }: { node: ObservationNode; depth: number }) {
  return (
    <>
      <div style={{ display: "flex", gap: 10, alignItems: "baseline", padding: "7px 0", paddingLeft: depth * 22 }}>
        <Dot color={typeColor(node.type)} size={8} />
        <span style={{ fontSize: 13, fontWeight: 500 }}>{node.name ?? node.type}</span>
        <span
          style={{
            fontFamily: C.mono,
            fontSize: 10.5,
            color: typeColor(node.type),
            border: "1px solid " + C.border,
            borderRadius: 20,
            padding: "1px 7px",
          }}
        >
          {node.type}
        </span>
        {node.model ? <span style={{ fontFamily: C.mono, fontSize: 11.5, color: C.muted }}>{node.model}</span> : null}
        {node.startTime ? <span style={{ marginLeft: "auto", fontFamily: C.mono, fontSize: 11, color: C.muted }}>{node.startTime}</span> : null}
      </div>
      {(node.children ?? []).map((c) => (
        <SpanRow key={c.id} node={c} depth={depth + 1} />
      ))}
    </>
  );
}

// Probe one payload for the runner's OTel resource attribute (`curie.sandbox_id`,
// the Docker container name locally / the sandbox pod name on k8s). Checks the
// object itself plus the keys Langfuse may nest resource/metadata attributes under.
function probeSandboxAttr(source: unknown): string | null {
  if (!source || typeof source !== "object") return null;
  const candidates: unknown[] = [source];
  for (const key of ["metadata", "resourceAttributes", "resource"]) {
    const v = (source as Record<string, unknown>)[key];
    if (v && typeof v === "object") {
      candidates.push(v);
      const nested = (v as Record<string, unknown>).attributes;
      if (nested && typeof nested === "object") candidates.push(nested);
    }
  }
  for (const bag of candidates) {
    const rec = bag as Record<string, unknown>;
    const hit = rec["curie.sandbox_id"] ?? rec["sandbox_id"];
    if (typeof hit === "string" && hit.trim() !== "") return hit;
  }
  return null;
}

// Resolve the serving sandbox id. Prefers the API's typed field (TraceTree
// `sandbox_id`, hoisted server-side from the trace/observation resource attrs);
// falls back to probing the raw trace payload for older traces that predate the
// hoist. Accepts either a full TraceTree or a bare trace dict (the probe path).
// Returns null when absent, so the UI degrades silently.
export function sandboxIdFromTrace(source: RawTrace | null | undefined): string | null {
  if (!source || typeof source !== "object") return null;
  const typed = (source as Record<string, unknown>)["sandbox_id"];
  if (typeof typed === "string" && typed.trim() !== "") return typed;
  const rec = source as Record<string, unknown>;
  return probeSandboxAttr(rec) ?? probeSandboxAttr(rec["trace"]);
}

// Live trace drill-in: the reconstructed observation tree from the API proxy.
export function RealTraceDetail() {
  const { state, dispatch } = useStore();
  const { data, loading, error, notFound } = useTrace(state.traceOpen);
  const [promoting, setPromoting] = useState(false);
  const [promoteError, setPromoteError] = useState<string | null>(null);
  const traceName = data ? (str(data.trace as RawTrace, "name") ?? state.traceOpen) : state.traceOpen;

  const promote = async () => {
    if (!state.traceOpen || promoting) return;
    setPromoting(true);
    setPromoteError(null);
    try {
      const evalCase = await promoteTraceToEvalCase(state.traceOpen);
      dispatch({ type: "addPromotedEvalCase", evalCase });
      dispatch({ type: "toast", message: `Promoted to eval case ${evalCase.id} (anonymized)` });
    } catch (e) {
      setPromoteError(e instanceof Error ? e.message : String(e));
    } finally {
      setPromoting(false);
    }
  };
  // Prefer the API's typed field; fall back to probing the raw trace payload.
  const typedSandboxId =
    typeof data?.sandbox_id === "string" && data.sandbox_id.trim() !== "" ? data.sandbox_id : null;
  const sandboxId = typedSandboxId ?? sandboxIdFromTrace(data?.trace as RawTrace | undefined);

  return (
    <div>
      <button
        type="button"
        onClick={() => dispatch({ type: "closeTrace" })}
        style={{ background: "none", border: "none", color: C.muted, fontSize: 13, cursor: "pointer", marginBottom: 14, padding: 0 }}
      >
        ← Traces
      </button>
      {loading ? <Notice padding="40px 20px" fontSize={13.5}>Loading trace…</Notice> : null}
      {error ? <Notice padding="40px 20px" fontSize={13.5}>Could not load trace: {error}</Notice> : null}
      {notFound ? (
        <EmptyState
          title="No spans recorded for this run yet"
          sub="This trace exists but has no observations. Spans appear here once the agent finishes a run against it."
        />
      ) : null}
      {data ? (
        <>
          <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 20 }}>
            <h1 style={{ fontSize: 22, fontWeight: 400, margin: 0, fontFamily: C.mono }}>{traceName}</h1>
            <Chip color={C.success} border="rgba(46,160,67,.4)" pre={<Dot color={C.success} size={6} />}>
              live
            </Chip>
            <span style={{ marginLeft: "auto", fontSize: 12.5, color: C.muted, fontFamily: C.mono }}>{state.traceOpen}</span>
            <CliHint
              command={cliCommand(
                state.env === "prod"
                  ? "cluster.observability.run"
                  : "local.observability.run",
                { trace_id: state.traceOpen ?? "" },
              )}
            />
            <button
              type="button"
              data-testid="promote-eval-case"
              onClick={() => void promote()}
              disabled={promoting}
              title="Turn this conversation into an anonymized eval case"
              style={{
                background: "transparent",
                border: "1px solid " + C.border,
                borderRadius: 20,
                padding: "3px 12px",
                color: C.link,
                fontFamily: C.mono,
                fontSize: 11.5,
                cursor: promoting ? "default" : "pointer",
                opacity: promoting ? 0.6 : 1,
              }}
            >
              {promoting ? "Promoting…" : "Promote to eval case"}
            </button>
          </div>
          {promoteError ? (
            <div
              data-testid="promote-error"
              style={{ fontSize: 12, color: C.destructive, fontFamily: C.mono, marginBottom: 16 }}
            >
              Could not promote: {promoteError}
            </div>
          ) : null}
          {sandboxId ? (
            <div
              data-testid="trace-sandbox"
              style={{ fontSize: 12.5, color: C.muted, fontFamily: C.mono, marginBottom: 16, display: "flex", alignItems: "center", gap: 7 }}
            >
              <Dot color={C.mutedStatus} size={6} />
              Served by sandbox <span style={{ color: C.text2 }}>{sandboxId}</span>
              <button
                type="button"
                data-testid="view-sandbox-logs"
                onClick={() => dispatch({ type: "openLogs", sandboxId })}
                style={{
                  marginLeft: 6,
                  background: "transparent",
                  border: "1px solid " + C.border,
                  borderRadius: 20,
                  padding: "2px 10px",
                  color: C.link,
                  fontFamily: C.mono,
                  fontSize: 11.5,
                  cursor: "pointer",
                }}
              >
                View sandbox logs →
              </button>
            </div>
          ) : null}
          <Card>
            <div data-testid="span-tree">
              {data.tree.map((n) => (
                <SpanRow key={n.id} node={n} depth={0} />
              ))}
            </div>
          </Card>
        </>
      ) : null}
    </div>
  );
}
