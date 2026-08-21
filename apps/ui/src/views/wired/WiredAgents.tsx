import { useState } from "react";
import { C } from "../../tokens";
import { Card, SectionTitle, Button, CliHint, Dot, EmptyState, Notice, cliCommand } from "../../primitives";
import { useStore } from "../../state/store";
import { useWired } from "../../state/wired";
import { useAllDeployments } from "../../api/hooks";
import { hiddenAgentIdsForEnv } from "../../state/env";
import { deleteAgent } from "../../api/client";

export function WiredAgents() {
  const { state, dispatch } = useStore();
  const { agents, loading, error, refetch, wired } = useWired();
  const deps = useAllDeployments(wired);
  const [deletingId, setDeletingId] = useState<string | null>(null);

  async function onDelete(id: string, name: string) {
    if (deletingId) return;
    // Native confirm is an acceptable guard for this destructive action.
    if (!window.confirm(`Delete agent "${name}"? This removes its versions and deployment history.`)) return;
    setDeletingId(id);
    try {
      await deleteAgent(id);
      dispatch({ type: "toast", message: `Deleted ${name}` });
      refetch();
    } catch (e: unknown) {
      dispatch({ type: "toast", message: `Could not delete ${name}: ${e instanceof Error ? e.message : String(e)}` });
    } finally {
      setDeletingId(null);
    }
  }

  if (loading) return <Notice>Loading agents…</Notice>;
  if (error) return <Notice>{`Could not load agents: ${error}`}</Notice>;

  // Env-gating is best-effort: hide agents deployed exclusively to the other
  // env, but never gate (and never error the view out) while deployments are
  // still loading or the fetch failed — show the full list instead.
  const gate = deps.data && !deps.error;
  const hiddenIds = gate ? hiddenAgentIdsForEnv(deps.data ?? [], state.env) : new Set<string>();
  const shown = agents.filter((a) => !hiddenIds.has(a.id));

  if (agents.length === 0) {
    return (
      <div>
        <SectionTitle title="Agents" />
        <EmptyState
          title="Create your first agent"
          sub="Write a skill.md in the browser, pick a channel, and deploy it to Slack in seconds."
          ctaLabel="New agent"
          onCta={() => dispatch({ type: "openModal", modal: "new-agent" })}
        />
      </div>
    );
  }

  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", marginBottom: 20 }}>
        <div>
          <SectionTitle title="Agents" />
        </div>
        <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 8 }}>
          <CliHint command={cliCommand("init")} />
          <Button label="New agent" variant="primary" icon="+" onClick={() => dispatch({ type: "openModal", modal: "new-agent" })} />
        </div>
      </div>
      {shown.length === 0 ? (
        <Notice>{`No agents deployed to ${state.env} yet.`}</Notice>
      ) : (
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(320px,1fr))", gap: 16 }}>
        {shown.map((a) => (
          <Card key={a.id}>
            <div style={{ display: "flex", alignItems: "center", gap: 9, marginBottom: 12 }}>
              <Dot color={C.brand} size={9} />
              <button
                type="button"
                data-testid="agent-card-name"
                onClick={() => dispatch({ type: "openAgentDetail", id: a.id })}
                style={{
                  fontFamily: C.mono,
                  fontSize: 15,
                  fontWeight: 500,
                  background: "none",
                  border: "none",
                  color: C.text,
                  cursor: "pointer",
                  padding: 0,
                }}
              >
                {a.name}
              </button>
              <span style={{ marginLeft: "auto", fontSize: 12, color: C.muted, fontFamily: C.mono }}>
                {a.channels.map((c) => c.address).join(", ")}
              </span>
            </div>
            <div
              style={{
                display: "flex",
                gap: 20,
                padding: "12px 0",
                borderTop: "1px solid " + C.border,
                borderBottom: "1px solid " + C.border,
                marginBottom: 12,
              }}
            >
              <div>
                <div style={{ fontSize: 11, color: C.muted, marginBottom: 3 }}>channel</div>
                {a.channels.map((c) => (
                  <div key={`${c.kind}:${c.address}`} style={{ fontFamily: C.mono, fontSize: 13 }}>
                    {c.address}
                  </div>
                ))}
              </div>
              <div>
                <div style={{ fontSize: 11, color: C.muted, marginBottom: 3 }}>created</div>
                <div style={{ fontFamily: C.mono, fontSize: 13 }}>{new Date(a.created_at).toLocaleDateString()}</div>
              </div>
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
              {/* No status chip: GET /agents carries no bundle/deploy state, so we
                  cannot honestly claim an agent is "live" from the list alone. */}
              <button
                type="button"
                data-testid="edit-skills-link"
                onClick={() => dispatch({ type: "openAgentDetail", id: a.id })}
                style={{ background: "none", border: "none", color: C.link, fontSize: 13, cursor: "pointer", padding: 0 }}
              >
                Edit skills →
              </button>
              <button
                type="button"
                data-testid="view-traces-link"
                onClick={() => dispatch({ type: "viewTraces", agentId: a.id })}
                style={{ marginLeft: "auto", background: "none", border: "none", color: C.link, fontSize: 13, cursor: "pointer" }}
              >
                View traces →
              </button>
              <button
                type="button"
                data-testid="delete-agent"
                aria-label={`Delete ${a.name}`}
                title="Delete agent"
                disabled={deletingId === a.id}
                onClick={() => onDelete(a.id, a.name)}
                style={{
                  background: "none",
                  border: "none",
                  color: C.destructive,
                  fontSize: 13,
                  cursor: deletingId === a.id ? "default" : "pointer",
                  opacity: deletingId === a.id ? 0.5 : 1,
                  padding: 0,
                }}
              >
                {deletingId === a.id ? "Deleting…" : "Delete"}
              </button>
            </div>
          </Card>
        ))}
      </div>
      )}
    </div>
  );
}
