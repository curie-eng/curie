import { useState } from "react";
import { C } from "../../tokens";
import { Button, Card, SectionTitle, Chip, CliHint, Dot, Modal, Notice, cliCommand } from "../../primitives";
import { useStore } from "../../state/store";
import { useAgents, useAgentVersions } from "../../api/hooks";
import { ComingSoon } from "./WiredStubs";
import { createDeployment, type DeploymentOut, type Environment, type VersionOut } from "../../api/client";

export interface Row {
  version: VersionOut;
  deployment: DeploymentOut | null;
}

// Flatten versions x their deployments: one row per deployment, plus a row for
// any version that was never deployed. Newest activity first.
export function buildRows(versions: VersionOut[], deployments: DeploymentOut[]): Row[] {
  const rows: Row[] = versions.flatMap((version): Row[] => {
    const deps = deployments.filter((d) => d.version_id === version.id);
    if (deps.length === 0) return [{ version, deployment: null }];
    return deps.map((deployment) => ({ version, deployment }));
  });
  return rows.sort((a, b) => {
    const at = a.deployment?.deployed_at ?? a.version.created_at;
    const bt = b.deployment?.deployed_at ?? b.version.created_at;
    return bt.localeCompare(at);
  });
}

function statusColor(status: string | null): string {
  if (status === "active") return C.success;
  if (status === null) return C.mutedStatus;
  return C.warn;
}

// Versions table for one agent: real versions joined with their deployments
// (environment, status, deployed_at). No Eval column. Agent-scoped via a
// selector, matching the Cost view. Falls back to ComingSoon when the API is
// unreachable (there is no fixture leak in wired mode).
// The version+environment a rollback confirmation is pending for. Local to the
// table — no global ModalKind — so the confirm dialog is self-contained.
interface RollbackTarget {
  versionId: string;
  versionLabel: string;
  environment: Environment;
}

function VersionsTable({ agentId }: { agentId: string }) {
  const { state } = useStore();
  const { versions, deployments, activeVersionId, loading, error, reload } = useAgentVersions(agentId);
  const [target, setTarget] = useState<RollbackTarget | null>(null);
  const [rolling, setRolling] = useState(false);
  const [rollbackError, setRollbackError] = useState<string | null>(null);

  const confirmRollback = async () => {
    if (!target || rolling) return;
    setRolling(true);
    setRollbackError(null);
    try {
      await createDeployment({
        agent_id: agentId,
        version_id: target.versionId,
        environment: target.environment,
        status: "active",
      });
      setTarget(null);
      reload(); // refetch versions + deployments -> the rolled-back version becomes active
    } catch (e) {
      setRollbackError(e instanceof Error ? e.message : String(e));
    } finally {
      setRolling(false);
    }
  };

  const cancelRollback = () => {
    if (rolling) return;
    setTarget(null);
    setRollbackError(null);
  };

  if (error) {
    return (
      <ComingSoon
        title="Versions are not available"
        body={`Could not load versions from the backend: ${error}`}
      />
    );
  }
  if (loading) return <Notice>Loading versions…</Notice>;
  if (versions.length === 0) {
    return (
      <ComingSoon
        title="No versions yet"
        body="Deploy this agent or push to a connected git branch and its versions appear here."
      />
    );
  }

  // Scope to the selected environment first, then join with versions. Rollback
  // eligibility (previously-deployed, non-active) composes on top: a row shows
  // only if its deployment is in state.env, and gets a Roll back button if
  // eligible. The rollback column keeps the 6-col grid from the rollback work.
  const scopedDeployments = deployments.filter((d) => d.environment === state.env);
  const rows = buildRows(versions, scopedDeployments);
  const grid = "1.1fr 0.9fr 1fr 1.3fr 1fr 0.8fr";
  return (
    <Card>
      <div style={{ display: "flex", alignItems: "center", marginBottom: 12 }}>
        <span style={{ fontSize: 13, fontWeight: 500, color: C.text }}>Versions</span>
        <div style={{ marginLeft: "auto" }}>
          <CliHint command={cliCommand(state.env === "prod" ? "cluster.status" : "local.status")} />
        </div>
      </div>
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
        {["Version", "Environment", "Status", "Deployed", "Created by", ""].map((c, i) => (
          <div key={c || `col-${i}`}>{c}</div>
        ))}
      </div>
      {rows.map((r, i) => {
        const dep = r.deployment;
        const env = dep?.environment ?? null;
        const isActive = dep?.status === "active" && r.version.id === activeVersionId;
        const canRollback = dep !== null && !isActive;
        return (
          <div
            key={`${r.version.id}-${r.deployment?.id ?? "none"}-${i}`}
            data-testid="version-row"
            style={{
              display: "grid",
              gridTemplateColumns: grid,
              gap: 12,
              padding: "13px 0",
              alignItems: "center",
              borderBottom: "1px solid " + C.border,
              fontSize: 13.5,
            }}
          >
            <span style={{ fontFamily: C.mono, fontSize: 12.5, display: "flex", alignItems: "center", gap: 8 }}>
              {r.version.version_label}
              {isActive ? <Chip color={C.brand} border="rgba(62,207,142,.4)">active</Chip> : null}
            </span>
            <div>
              {env ? (
                <Chip
                  color={env === "prod" ? C.brand : C.warn}
                  border={env === "prod" ? "rgba(62,207,142,.4)" : "rgba(191,135,0,.4)"}
                >
                  {env}
                </Chip>
              ) : (
                <span style={{ color: C.muted }}>—</span>
              )}
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
              <Dot color={statusColor(dep?.status ?? null)} size={7} />
              {/* The currently-served row reads "active" (the live version, named
                  by the chip too); other rows show their raw deployment status. */}
              <span data-testid="version-status" style={{ fontSize: 12.5, color: C.text2 }}>
                {isActive ? "active" : (dep?.status ?? "not deployed")}
              </span>
            </div>
            <span style={{ color: C.muted, fontFamily: C.mono, fontSize: 12 }}>
              {r.deployment ? new Date(r.deployment.deployed_at).toLocaleString() : "—"}
            </span>
            <span style={{ color: C.text2, fontFamily: C.mono, fontSize: 12 }}>{r.version.created_by}</span>
            <div style={{ display: "flex", justifyContent: "flex-end" }}>
              {canRollback && dep ? (
                <Button
                  label="Roll back"
                  variant="secondary"
                  size="sm"
                  onClick={() =>
                    setTarget({
                      versionId: r.version.id,
                      versionLabel: r.version.version_label,
                      environment: dep.environment,
                    })
                  }
                />
              ) : null}
            </div>
          </div>
        );
      })}
      {target ? (
        <Modal onClose={cancelRollback}>
          <Card style={{ maxWidth: 420 }}>
            <div style={{ fontSize: 15, fontWeight: 500, color: C.text, marginBottom: 8 }}>Roll back version</div>
            <div style={{ fontSize: 13, color: C.text2, lineHeight: 1.5, marginBottom: 16 }}>
              Redeploy{" "}
              <span style={{ fontFamily: C.mono, color: C.text }}>{target.versionLabel}</span> to{" "}
              <span style={{ fontFamily: C.mono, color: C.text }}>{target.environment}</span> as the active version.
              This creates a new deployment; it does not remove existing history.
            </div>
            {rollbackError ? (
              <div style={{ fontSize: 12.5, color: C.destructive, fontFamily: C.mono, marginBottom: 12 }}>
                Roll back failed: {rollbackError}
              </div>
            ) : null}
            <div style={{ display: "flex", justifyContent: "flex-end", gap: 10 }}>
              <Button label="Cancel" variant="ghost" onClick={cancelRollback} disabled={rolling} />
              <Button
                label={rolling ? "Rolling back…" : "Roll back"}
                variant="primary"
                onClick={() => void confirmRollback()}
                disabled={rolling}
              />
            </div>
          </Card>
        </Modal>
      ) : null}
    </Card>
  );
}

export function WiredVersions() {
  const agents = useAgents(true);
  const [selected, setSelected] = useState<string>("");

  if (agents.error) {
    return (
      <div>
        <SectionTitle title="Versions" sub="main → your prod bot · dev → your dev bot." />
        <ComingSoon
          title="Versions are not available"
          body={`Could not load agents from the backend: ${agents.error}`}
        />
      </div>
    );
  }
  if (agents.loading) {
    return (
      <div>
        <SectionTitle title="Versions" sub="main → your prod bot · dev → your dev bot." />
        <Notice>Loading agents…</Notice>
      </div>
    );
  }

  const list = agents.data ?? [];
  if (list.length === 0) {
    return (
      <div>
        <SectionTitle title="Versions" sub="main → your prod bot · dev → your dev bot." />
        <ComingSoon
          title="No versions deployed yet"
          body="Deploy an agent or push to a connected git branch and its versions appear here. This workspace has none yet."
        />
      </div>
    );
  }

  const activeId = selected || list[0]?.id || "";

  return (
    <div>
      <SectionTitle title="Versions" sub="main → your prod bot · dev → your dev bot." />
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 16 }}>
        <span style={{ fontSize: 12.5, color: C.muted }}>Agent</span>
        <select
          data-testid="versions-agent-select"
          value={activeId}
          onChange={(e) => setSelected(e.target.value)}
          style={{
            background: C.input,
            border: "1px solid " + C.borderStrong,
            borderRadius: 7,
            padding: "7px 10px",
            color: C.text,
            fontFamily: C.mono,
            fontSize: 13,
          }}
        >
          {list.map((a) => (
            <option key={a.id} value={a.id}>
              {a.name}
            </option>
          ))}
        </select>
      </div>
      <VersionsTable key={activeId} agentId={activeId} />
    </div>
  );
}
