import { useEffect, useMemo, useRef, useState } from "react";
import { C } from "../../tokens";
import { Button, Card, Chip, CliHint, Dot, Notice, cliCommand } from "../../primitives";
import { SkillEditor } from "../../components/SkillEditor";
import { WiredAgentMemory } from "./WiredAgentMemory";
import { WiredAgentState } from "./WiredAgentState";
import { WiredThreadReset } from "./WiredThreadReset";
import { WiredAgentBehaviorPacks } from "./WiredAgentBehaviorPacks";
import { useStore } from "../../state/store";
import { useWired } from "../../state/wired";
import { useAgentVersions, useVersionFiles } from "../../api/hooks";
import {
  createVersion,
  uploadBundle,
  createDeployment,
  updateAgent,
  patchAgentChannel,
  addAgentSurface,
  removeAgentSurface,
  BundleValidationError,
  ApiError,
  SLACK_ADDRESS_RE,
  type BundleFile,
  type BundleIssue,
  type ChannelBinding,
} from "../../api/client";
import { buildBundleZipFromFiles, nextVersionLabel } from "../../api/bundle";

function isSkillFile(path: string): boolean {
  return path.endsWith("/SKILL.md") || path === "SKILL.md";
}

// Read-only view of a non-SKILL bundle file (manifest, evals/cases.json, …). Only
// SKILL.md files are editable; everything else in the tree is viewable, not edited.
function FileView({ path, content }: { path: string; content: string }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", minHeight: 0 }}>
      <div
        style={{
          padding: "8px 14px",
          borderBottom: "1px solid " + C.border,
          fontFamily: C.mono,
          fontSize: 12,
          color: C.muted,
          display: "flex",
          alignItems: "center",
          gap: 8,
        }}
      >
        <span style={{ color: C.text2 }}>{path}</span>
        <span style={{ marginLeft: "auto", fontSize: 11 }}>read-only</span>
      </div>
      <pre
        data-testid="file-view"
        style={{
          margin: 0,
          padding: "12px 16px",
          height: 360,
          overflow: "auto",
          background: C.darkest,
          color: C.text2,
          fontFamily: C.mono,
          fontSize: 12.5,
          lineHeight: 1.55,
          whiteSpace: "pre",
        }}
      >
        {content}
      </pre>
    </div>
  );
}

// Everything one channel-binding row owns, held as a single record so the
// per-row fields cannot drift out of lockstep (they are all keyed by the same
// row position and only ever written together).
type ChannelRowState = {
  // The `(kind, address)` pair a save for this row must name as its selector.
  selector: ChannelBinding;
  // The typed address, which diverges from `selector.address` while editing.
  edit: string;
  saving: boolean;
  error: string | null;
  // True once the row has a typed-but-unsaved edit, so a refetch landing from
  // another row's save can never clobber it.
  dirty: boolean;
};

// The wired agent-detail surface (FX2 headline). Opens from the Agents list:
// loads the agent's active version, shows its bundle's skills, lets you edit each
// skills/*/SKILL.md in the same editor as the create modal, and ships a new
// version via the create-path sequence (POST version + PUT bundle + activate
// deployment) carrying the edited content — nothing else in the bundle is lost.
export function WiredAgentDetail() {
  const { state, dispatch } = useStore();
  const { agents, refetch } = useWired();
  const agentId = state.agentDetail;
  const agent = agents.find((a) => a.id === agentId) ?? null;

  const versions = useAgentVersions(agentId);
  const activeVersion = versions.versions.find((v) => v.id === versions.activeVersionId) ?? null;
  const files = useVersionFiles(agentId, versions.activeVersionId);

  // Edited SKILL.md content keyed by bundle path, seeded from the loaded files.
  const [edited, setEdited] = useState<Record<string, string>>({});
  const [selectedPath, setSelectedPath] = useState<string | null>(null);
  const [deploying, setDeploying] = useState(false);
  const [deployError, setDeployError] = useState<string | null>(null);
  const [issues, setIssues] = useState<BundleIssue[]>([]);
  const [deployedLabel, setDeployedLabel] = useState<string | null>(null);
  const [confirmingPromote, setConfirmingPromote] = useState(false);
  const [promoting, setPromoting] = useState(false);
  const [promoteError, setPromoteError] = useState<string | null>(null);

  // Editable channel bindings (item 5, ADR-0116): one editor per binding, keyed
  // by ROW POSITION, not by `${kind}:${address}` (finding 3, #1525 review) --
  // that pair is exactly what a save mutates, so keying state by it means the
  // very next save reuses the just-retired selector until a refetch lands.
  // Each row's `selector` starts synced from the agent and is rebased locally
  // the instant a PATCH succeeds, without waiting on the refetch. `dirty` is
  // only ever read through a functional updater, never as an effect dep, so
  // marking a row dirty cannot itself retrigger the reseed effect below.
  const [channelRows, setChannelRows] = useState<Record<number, ChannelRowState>>({});
  const lastAgentIdForChannels = useRef<string | null>(null);
  const [newSurfaceKind, setNewSurfaceKind] = useState("");
  const [newSurfaceAddress, setNewSurfaceAddress] = useState("");
  const [newSurfaceEndpoint, setNewSurfaceEndpoint] = useState("");
  const [newSurfaceAdapter, setNewSurfaceAdapter] = useState("");
  const [surfaceBusy, setSurfaceBusy] = useState(false);
  const [surfaceError, setSurfaceError] = useState<string | null>(null);

  // Editable per-agent model (#254). Seeded from the agent; blank = platform default.
  const [model, setModel] = useState("");
  const [savingModel, setSavingModel] = useState(false);
  const [modelError, setModelError] = useState<string | null>(null);

  // A stable string dep (not the array reference) so the reseed effect only
  // fires when a binding's identity actually changes.
  const channelsDepKey = (agent?.channels ?? []).map((b) => `${b.kind}:${b.address}`).join(",");

  useEffect(() => {
    const channels = agent?.channels ?? [];
    // A fresh agent starts with no dirty rows -- stale row-index flags from a
    // previous agent must not leak into this one's rows.
    const agentChanged = lastAgentIdForChannels.current !== (agent?.id ?? null);
    lastAgentIdForChannels.current = agent?.id ?? null;
    setChannelRows((prev) => {
      const next: Record<number, ChannelRowState> = {};
      channels.forEach((b, i) => {
        const dirty = agentChanged ? false : (prev[i]?.dirty ?? false);
        next[i] = {
          selector: b,
          // A dirty row keeps its unsaved text; everything else reseeds from
          // the fresh agent (finding 3, #1525 review: a refetch triggered by
          // ANY row's save must not clobber another row's unsaved edit).
          edit: dirty ? (prev[i]?.edit ?? b.address) : b.address,
          saving: prev[i]?.saving ?? false,
          error: null,
          dirty,
        };
      });
      return next;
    });
    // channelsDepKey mirrors agent?.channels; agent?.id covers a fresh agent
    // whose bindings happen to collide with the previous one's row shape.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [agent?.id, channelsDepKey]);

  useEffect(() => {
    setModel(agent?.model ?? "");
    setModelError(null);
  }, [agent?.id, agent?.model]);

  // The whole bundle tree (item 4): every file is browsable; only SKILL.md files
  // are editable, the rest are read-only views.
  const allFiles = files.files ?? [];
  const skillFiles = useMemo(() => (files.files ?? []).filter((f) => isSkillFile(f.path)), [files.files]);

  // The version currently active in dev — the one promote-to-prod ships. Newest
  // active dev deployment whose version still exists; null when there is nothing
  // in dev to promote (the button is then hidden).
  const devActiveVersionId = useMemo(() => {
    const dev = versions.deployments
      .filter((d) => d.status === "active" && d.environment === "dev")
      .sort((a, b) => b.deployed_at.localeCompare(a.deployed_at))
      .find((d) => versions.versions.some((v) => v.id === d.version_id));
    return dev?.version_id ?? null;
  }, [versions.deployments, versions.versions]);

  useEffect(() => {
    // Reseed the editor whenever a new version's files arrive.
    const map: Record<string, string> = {};
    for (const f of files.files ?? []) map[f.path] = f.content;
    setEdited(map);
    // Prefer a SKILL.md so the edit/deploy path is front and center, else the
    // first file in the tree.
    setSelectedPath(skillFiles[0]?.path ?? files.files?.[0]?.path ?? null);
    setDeployError(null);
    setIssues([]);
    // skillFiles is derived from files.files, so files.files is the real dep.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [files.files]);

  const saveChannel = async (rowIndex: number) => {
    // The row record only exists once the reseed effect has run.
    const row = channelRows[rowIndex];
    const binding = row?.selector;
    const value = (row?.edit ?? "").trim();
    if (!agent || !binding || row.saving || value === "") return;
    setChannelRows((prev) => ({ ...prev, [rowIndex]: { ...prev[rowIndex], saving: true, error: null } }));
    try {
      // A channel-move PATCH preserves the binding's existing kind and only
      // replaces the address. `binding` (not the edited value) is
      // the selector -- it names the row being moved, by its CURRENT pair
      // (rebased below on every prior successful save, so this is never the
      // just-retired pair even if a refetch is still in flight).
      //
      const next: ChannelBinding = {
        kind: binding.kind,
        address: value,
      };
      const updated = await patchAgentChannel(agent.id, binding, next);
      // Rebase this row's selector onto the pair just saved (finding 3, #1525
      // review): without this, a second immediate save on the same row still
      // names the retired pair and 404s, because `agent.channels` (and thus
      // `binding` above) does not reflect the move until the refetch below
      // lands. Prefer the response's own record of the saved binding; fall
      // back to the submitted values if the response shape ever omits it.
      const saved = updated.channels.find((b) => b.kind === next.kind && b.address === next.address) ?? next;
      setChannelRows((prev) => ({
        ...prev,
        [rowIndex]: { ...prev[rowIndex], selector: saved, dirty: false },
      }));
      refetch(); // refresh the wired agent data so the displayed channel updates
      dispatch({ type: "toast", message: `Channel set to ${value}` });
    } catch (e) {
      setChannelRows((prev) => ({
        ...prev,
        [rowIndex]: {
          ...prev[rowIndex],
          error: e instanceof ApiError ? e.message : e instanceof Error ? e.message : String(e),
        },
      }));
    } finally {
      setChannelRows((prev) => ({ ...prev, [rowIndex]: { ...prev[rowIndex], saving: false } }));
    }
  };

  const addSurface = async () => {
    const kind = newSurfaceKind.trim();
    const address = newSurfaceAddress.trim();
    const endpoint = newSurfaceEndpoint.trim();
    const adapter = newSurfaceAdapter.trim();
    const needsRoute = kind !== "slack";
    if (!agent || surfaceBusy || !kind || !address || (needsRoute && (!endpoint || !adapter))) return;
    setSurfaceBusy(true);
    setSurfaceError(null);
    try {
      await addAgentSurface(agent.id, needsRoute ? { kind, address, endpoint, adapter } : { kind, address });
      setNewSurfaceKind("");
      setNewSurfaceAddress("");
      setNewSurfaceEndpoint("");
      setNewSurfaceAdapter("");
      refetch();
      dispatch({ type: "toast", message: `Surface added: ${kind}:${address}` });
    } catch (e) {
      setSurfaceError(e instanceof Error ? e.message : String(e));
    } finally {
      setSurfaceBusy(false);
    }
  };

  const removeSurface = async (surface: ChannelBinding) => {
    if (!agent || surfaceBusy) return;
    setSurfaceBusy(true);
    setSurfaceError(null);
    try {
      await removeAgentSurface(agent.id, surface);
      refetch();
      dispatch({ type: "toast", message: `Surface removed: ${surface.kind}:${surface.address}` });
    } catch (e) {
      setSurfaceError(e instanceof Error ? e.message : String(e));
    } finally {
      setSurfaceBusy(false);
    }
  };

  const saveModel = async () => {
    if (!agent || savingModel) return;
    setSavingModel(true);
    setModelError(null);
    try {
      // A blank box is the clear gesture, and the clear is NULL, not "" (#1355).
      // apply_model_env reads `override if override is not None else config.model`,
      // so an empty string is not None, wins the ternary, and is then falsy: no
      // boot key is emitted and the platform default is skipped, which is the
      // opposite of clearing. Null is the only value that restores it.
      const trimmed = model.trim();
      const next = trimmed === "" ? null : trimmed;
      await updateAgent(agent.id, { model: next });
      refetch();
      dispatch({
        type: "toast",
        message: next === null ? "Model cleared (platform default)" : `Model set to ${next}`,
      });
    } catch (e) {
      setModelError(e instanceof ApiError ? e.message : e instanceof Error ? e.message : String(e));
    } finally {
      setSavingModel(false);
    }
  };

  // Return to the Agents list this detail was opened from. closeAgentDetail's
  // reducer lands on Overview, so navigate explicitly instead.
  const back = () => dispatch({ type: "go", nav: "agents" });

  const dirty = useMemo(
    () => (files.files ?? []).some((f) => (edited[f.path] ?? f.content) !== f.content),
    [files.files, edited],
  );

  const deploy = async () => {
    if (!agentId || !agent || deploying || !files.files) return;
    setDeploying(true);
    setDeployError(null);
    setIssues([]);
    const label = nextVersionLabel(versions.versions.map((v) => v.version_label));
    const merged: BundleFile[] = files.files.map((f) => ({ path: f.path, content: edited[f.path] ?? f.content }));
    try {
      const version = await createVersion(agentId, { version_label: label, created_by: "ui" });
      const archive = await buildBundleZipFromFiles(agent.name, merged);
      await uploadBundle(agentId, version.id, archive);
      await createDeployment({ agent_id: agentId, version_id: version.id, environment: state.env });
      setDeployedLabel(label);
      dispatch({ type: "toast", message: `Deployed ${label}` });
      versions.reload(); // refetch versions + deployments -> active version flips to the new one
    } catch (e) {
      if (e instanceof BundleValidationError) {
        setIssues(e.issues);
      } else {
        setDeployError(e instanceof ApiError ? e.message : e instanceof Error ? e.message : String(e));
      }
    } finally {
      setDeploying(false);
    }
  };

  // Promote the dev-active version to prod: a single createDeployment (prod gets
  // the server-default active status), then refresh so the Versions/active state
  // reflects the new prod deployment.
  const promote = async () => {
    if (!agentId || promoting || !devActiveVersionId) return;
    setPromoting(true);
    setPromoteError(null);
    try {
      await createDeployment({ agent_id: agentId, version_id: devActiveVersionId, environment: "prod" });
      dispatch({ type: "toast", message: "Promoted dev → prod" });
      setConfirmingPromote(false);
      versions.reload();
    } catch (e) {
      setPromoteError(e instanceof ApiError ? e.message : e instanceof Error ? e.message : String(e));
    } finally {
      setPromoting(false);
    }
  };

  const backLink = (
    <button
      type="button"
      onClick={back}
      style={{ background: "none", border: "none", color: C.muted, fontSize: 13, cursor: "pointer", marginBottom: 14, padding: 0 }}
    >
      ← Agents
    </button>
  );

  if (!agent) {
    return (
      <div>
        {backLink}
        <Notice padding="34px 20px">Agent not found.</Notice>
      </div>
    );
  }

  return (
    <div data-testid="agent-detail">
      {backLink}
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 18 }}>
        <Dot color={C.brand} size={10} />
        <h1 style={{ fontSize: 22, fontWeight: 500, margin: 0, fontFamily: C.mono }} data-testid="agent-detail-name">
          {agent.name}
        </h1>
        {activeVersion ? (
          <Chip color={C.mutedStatus}>
            active {activeVersion.version_label}
          </Chip>
        ) : null}
        <div style={{ marginLeft: "auto", display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 4 }}>
          {(agent.channels ?? []).map((binding, rowIndex) => {
            // The row's own `selector` (rebased on every successful save) is
            // this row's source of truth once the component has mounted;
            // `binding` from the just-rendered `agent.channels` is only the
            // fallback for the very first render before the reseed effect has
            // run.
            const row = channelRows[rowIndex];
            const effective = row?.selector ?? binding;
            const value = row?.edit ?? effective.address;
            const trimmed = value.trim();
            const blank = trimmed === "";
            // The Slack shape check only applies to slack-kind bindings; other
            // kinds (webhook, ms-teams, …) are governed by the API's generic
            // rule instead.
            const looksOff = effective.kind === "slack" && trimmed !== "" && !SLACK_ADDRESS_RE.test(trimmed);
            const saving = row?.saving ?? false;
            const error = row?.error ?? null;
            return (
              <div key={rowIndex} style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 4 }}>
                <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                  <span style={{ fontSize: 11, color: C.muted, fontFamily: C.mono }}>{effective.kind}</span>
                  <input
                    data-testid="channel-input"
                    value={value}
                    onChange={(e) => {
                      const edit = e.target.value;
                      setChannelRows((prev) => ({
                        ...prev,
                        [rowIndex]: {
                          ...(prev[rowIndex] ?? { selector: effective, saving: false }),
                          edit,
                          error: null,
                          dirty: true,
                        },
                      }));
                    }}
                    placeholder="C0123ABCD"
                    style={{
                      background: C.input,
                      border: "1px solid " + (looksOff ? C.warn : C.borderStrong),
                      borderRadius: 7,
                      padding: "5px 9px",
                      color: C.text,
                      fontFamily: C.mono,
                      fontSize: 12.5,
                      width: 150,
                    }}
                  />
                  <Button
                    label={saving ? "Saving…" : "Save"}
                    variant="secondary"
                    size="sm"
                    testId="channel-save"
                    disabled={blank || saving}
                    title={blank ? "Enter the Slack channel ID first" : undefined}
                    onClick={() => void saveChannel(rowIndex)}
                  />
                  <Button
                    label="Remove"
                    variant="secondary"
                    size="sm"
                    testId={`surface-remove-${rowIndex}`}
                    disabled={surfaceBusy}
                    onClick={() => void removeSurface(effective)}
                  />
                </div>
                {looksOff ? (
                  <div data-testid="channel-warn" style={{ fontSize: 11, color: C.warn, maxWidth: 280, textAlign: "right", lineHeight: 1.4 }}>
                    That does not look like a channel ID (C…). Mentions match on the ID, not the name — save anyway if
                    you are using the CLI.
                  </div>
                ) : null}
                {error ? (
                  <div data-testid="channel-error" style={{ fontSize: 11, color: C.destructive, maxWidth: 280, textAlign: "right", lineHeight: 1.4 }}>
                    Could not update channel: {error}
                  </div>
                ) : (
                  <div style={{ fontSize: 10.5, color: C.muted, maxWidth: 280, textAlign: "right", lineHeight: 1.4 }}>
                    Saved to the stored config; the live worker keeps its channel until the next deploy.
                  </div>
                )}
              </div>
            );
          })}
          <div style={{ display: "flex", alignItems: "center", gap: 6, marginTop: 6 }}>
            <input
              data-testid="surface-kind-new"
              value={newSurfaceKind}
              onChange={(e) => setNewSurfaceKind(e.target.value)}
              placeholder="kind"
              style={{ background: C.input, border: "1px solid " + C.borderStrong, borderRadius: 7, padding: "5px 9px", color: C.text, fontFamily: C.mono, fontSize: 12.5, width: 80 }}
            />
            <input
              data-testid="surface-address-new"
              value={newSurfaceAddress}
              onChange={(e) => setNewSurfaceAddress(e.target.value)}
              placeholder="address"
              style={{ background: C.input, border: "1px solid " + C.borderStrong, borderRadius: 7, padding: "5px 9px", color: C.text, fontFamily: C.mono, fontSize: 12.5, width: 150 }}
            />
            {newSurfaceKind.trim() !== "slack" && newSurfaceKind.trim() !== "" ? (
              <>
                <input
                  data-testid="surface-endpoint-new"
                  value={newSurfaceEndpoint}
                  onChange={(e) => setNewSurfaceEndpoint(e.target.value)}
                  placeholder="reply endpoint"
                  style={{ background: C.input, border: "1px solid " + C.borderStrong, borderRadius: 7, padding: "5px 9px", color: C.text, fontFamily: C.mono, fontSize: 12.5, width: 220 }}
                />
                <input
                  data-testid="surface-adapter-new"
                  value={newSurfaceAdapter}
                  onChange={(e) => setNewSurfaceAdapter(e.target.value)}
                  placeholder="adapter credential"
                  style={{ background: C.input, border: "1px solid " + C.borderStrong, borderRadius: 7, padding: "5px 9px", color: C.text, fontFamily: C.mono, fontSize: 12.5, width: 150 }}
                />
              </>
            ) : null}
            <Button
              label={surfaceBusy ? "Saving…" : "Add surface"}
              variant="secondary"
              size="sm"
              testId="surface-add"
              disabled={
                surfaceBusy ||
                !newSurfaceKind.trim() ||
                !newSurfaceAddress.trim() ||
                (newSurfaceKind.trim() !== "slack" && (!newSurfaceEndpoint.trim() || !newSurfaceAdapter.trim()))
              }
              onClick={() => void addSurface()}
            />
          </div>
          {surfaceError ? (
            <div data-testid="surface-error" style={{ fontSize: 11, color: C.destructive, maxWidth: 360, textAlign: "right", lineHeight: 1.4 }}>
              Could not update surfaces: {surfaceError}
            </div>
          ) : null}
          <div style={{ display: "flex", alignItems: "center", gap: 6, marginTop: 6 }}>
            <span style={{ fontSize: 11, color: C.muted, fontFamily: C.mono }}>model</span>
            <input
              data-testid="model-input"
              value={model}
              onChange={(e) => {
                setModel(e.target.value);
                setModelError(null);
              }}
              placeholder="platform default"
              style={{
                background: C.input,
                border: "1px solid " + C.borderStrong,
                borderRadius: 7,
                padding: "5px 9px",
                color: C.text,
                fontFamily: C.mono,
                fontSize: 12.5,
                width: 150,
              }}
            />
            <Button
              label={savingModel ? "Saving…" : "Save"}
              variant="secondary"
              size="sm"
              testId="model-save"
              disabled={savingModel}
              onClick={() => void saveModel()}
            />
          </div>
          {modelError ? (
            <div data-testid="model-error" style={{ fontSize: 11, color: C.destructive, maxWidth: 280, textAlign: "right", lineHeight: 1.4 }}>
              Could not update model: {modelError}
            </div>
          ) : (
            <div style={{ fontSize: 10.5, color: C.muted, maxWidth: 280, textAlign: "right", lineHeight: 1.4 }}>
              Sets CURIE_MODEL at boot; blank uses the platform default.
            </div>
          )}
        </div>
      </div>

      {devActiveVersionId ? (
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 16 }}>
          {confirmingPromote ? (
            <>
              <span style={{ fontSize: 12.5, color: C.text2, fontFamily: C.mono }}>Promote the dev-active version to prod?</span>
              <Button label="Cancel" variant="ghost" size="sm" onClick={() => setConfirmingPromote(false)} />
              {promoting ? (
                <Button label="Promoting…" variant="primary" size="sm" disabled />
              ) : (
                <Button label="Confirm promote" variant="primary" size="sm" onClick={() => void promote()} />
              )}
            </>
          ) : (
            <Button label="Promote to prod" size="sm" onClick={() => setConfirmingPromote(true)} />
          )}
          {promoteError ? (
            <span data-testid="promote-error" style={{ fontSize: 12, color: C.destructive, fontFamily: C.mono }}>
              Promote failed: {promoteError}
            </span>
          ) : null}
        </div>
      ) : null}

      {versions.loading ? (
        <Notice padding="34px 20px">Loading versions…</Notice>
      ) : versions.error ? (
        <Notice padding="34px 20px">{`Could not load versions: ${versions.error}`}</Notice>
      ) : versions.versions.length === 0 ? (
        <Notice padding="34px 20px">No versions yet for this agent.</Notice>
      ) : files.loading ? (
        <Notice padding="34px 20px">Loading skills…</Notice>
      ) : files.noBundle ? (
        <Card>
          <div data-testid="agent-detail-nobundle" style={{ padding: "8px 4px", color: C.text2, fontSize: 13.5 }}>
            <div style={{ fontWeight: 500, marginBottom: 4 }}>No bundle stored for {activeVersion?.version_label ?? "this version"}</div>
            <div style={{ color: C.muted }}>This version has no plugin bundle yet, so there are no skills to edit.</div>
          </div>
        </Card>
      ) : files.error ? (
        <Notice padding="34px 20px">{`Could not load skills: ${files.error}`}</Notice>
      ) : allFiles.length === 0 ? (
        <Card>
          <Notice padding="34px 20px">This bundle has no files.</Notice>
        </Card>
      ) : (
        <div>
          {allFiles.length > 1 ? (
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
              {allFiles.map((f) => {
                const editable = isSkillFile(f.path);
                return (
                  <button
                    key={f.path}
                    type="button"
                    data-testid={editable ? "skill-tab" : "file-tab"}
                    onClick={() => setSelectedPath(f.path)}
                    title={editable ? undefined : "read-only"}
                    style={{
                      fontFamily: C.mono,
                      fontSize: 12,
                      padding: "5px 10px",
                      borderRadius: 7,
                      cursor: "pointer",
                      background: f.path === selectedPath ? C.hover : C.card,
                      color: f.path === selectedPath ? C.text : editable ? C.text2 : C.muted,
                      border: "1px solid " + (f.path === selectedPath ? C.borderStrong : C.border),
                    }}
                  >
                    {f.path}
                  </button>
                );
              })}
            </div>
          ) : null}

          <div
            style={{
              border: "1px solid " + C.borderStrong,
              borderRadius: 12,
              overflow: "hidden",
              display: "flex",
              flexDirection: "column",
              marginBottom: 16,
            }}
          >
            {selectedPath && isSkillFile(selectedPath) ? (
              <SkillEditor
                key={selectedPath}
                path={selectedPath}
                value={edited[selectedPath] ?? ""}
                onChange={(next) => {
                  setEdited((prev) => ({ ...prev, [selectedPath]: next }));
                  setDeployedLabel(null);
                  if (deployError || issues.length) {
                    setDeployError(null);
                    setIssues([]);
                  }
                }}
                testId="skill-editor"
                height={360}
              />
            ) : selectedPath ? (
              <FileView
                key={selectedPath}
                path={selectedPath}
                content={allFiles.find((f) => f.path === selectedPath)?.content ?? ""}
              />
            ) : null}
            {issues.length > 0 ? (
              <div
                data-testid="deploy-errors"
                style={{
                  borderTop: "1px solid rgba(229,77,46,.3)",
                  background: "rgba(229,77,46,.06)",
                  padding: "10px 16px",
                  maxHeight: 140,
                  overflow: "auto",
                }}
              >
                <div style={{ fontSize: 12, fontWeight: 600, color: C.destructive, marginBottom: 6 }}>Bundle validation failed</div>
                {issues.map((issue, i) => (
                  <div key={i} style={{ fontFamily: C.mono, fontSize: 11.5, color: C.text2, marginBottom: 3 }}>
                    <span style={{ color: C.destructive }}>{issue.code}</span>
                    {issue.location ? <span style={{ color: C.muted }}> · {issue.location}</span> : null}
                    <span style={{ color: C.text2 }}> — {issue.message}</span>
                  </div>
                ))}
              </div>
            ) : null}
            {deployError ? (
              <div
                data-testid="deploy-error"
                style={{
                  borderTop: "1px solid rgba(229,77,46,.3)",
                  background: "rgba(229,77,46,.06)",
                  padding: "10px 16px",
                  fontSize: 12.5,
                  color: C.destructive,
                  fontFamily: C.mono,
                }}
              >
                Deploy failed: {deployError}
              </div>
            ) : null}
          </div>

          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            {deployedLabel ? (
              <span data-testid="deploy-success" style={{ fontSize: 12.5, color: C.brand, fontFamily: C.mono }}>
                ✓ Deployed {deployedLabel}
              </span>
            ) : (
              <span style={{ fontSize: 12, color: C.muted, fontFamily: C.mono }}>
                {dirty ? "unsaved edits" : "no changes"}
              </span>
            )}
            <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 8 }}>
              <CliHint command={cliCommand(state.env === "prod" ? "cluster.deploy" : "local.deploy")} />
              {deploying ? (
                <Button label="Deploying…" variant="primary" disabled />
              ) : (
                <Button label="Deploy new version" variant="primary" onClick={() => void deploy()} />
              )}
            </div>
          </div>
        </div>
      )}

      {agentId ? (
        <div style={{ marginTop: 16 }}>
          <WiredAgentMemory agentId={agentId} />
        </div>
      ) : null}

      {agentId ? (
        <div style={{ marginTop: 16 }}>
          <WiredAgentState agentId={agentId} />
        </div>
      ) : null}

      {agentId ? (
        <div style={{ marginTop: 16 }}>
          <WiredThreadReset agentId={agentId} agentName={agent.name} />
        </div>
      ) : null}

      {agentId ? (
        <div style={{ marginTop: 16 }}>
          <WiredAgentBehaviorPacks agentId={agentId} />
        </div>
      ) : null}
    </div>
  );
}
