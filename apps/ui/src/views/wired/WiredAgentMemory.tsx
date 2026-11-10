import { useCallback, useEffect, useState } from "react";
import { C } from "../../tokens";
import { Button, Card, CliHint, Notice, PARITY_TRACKING_ISSUE } from "../../primitives";
import {
  listMemory,
  editMemory,
  deleteMemory,
  type MemoryEntry,
} from "../../api/client";

// The wired "inspect / edit / delete learned memory" surface (#267). Lists an
// agent's learned memory entries with their provenance (the entry->source-trace
// link is the differentiator — an operator can see how a lesson was learned),
// and lets them correct an entry's text or remove it. Edits/deletes hit the
// apps/api memory endpoints over the same-origin /api proxy; because the runner
// rehydrates the same memory log at boot, changes take effect at the next
// session boot (the #267 acceptance criterion, surfaced in the panel note).
//
// There is no dedicated CLI verb for memory yet, so the edit/delete actions
// render the honest amber CliHint gap (parity ids memory-edit / memory-delete).
export function WiredAgentMemory({ agentId }: { agentId: string }) {
  const [entries, setEntries] = useState<MemoryEntry[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [busyIndex, setBusyIndex] = useState<number | null>(null);
  const [editingIndex, setEditingIndex] = useState<number | null>(null);
  const [draft, setDraft] = useState("");

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setEntries(await listMemory(agentId));
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [agentId]);

  useEffect(() => {
    void load();
  }, [load]);

  const startEdit = (entry: MemoryEntry) => {
    setEditingIndex(entry.index);
    setDraft(entry.content);
    setError(null);
  };

  const saveEdit = async (index: number, expectedVersion: number) => {
    setBusyIndex(index);
    setError(null);
    try {
      await editMemory(agentId, index, draft, expectedVersion);
      setEditingIndex(null);
      await load();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusyIndex(null);
    }
  };

  const remove = async (index: number, expectedVersion: number) => {
    setBusyIndex(index);
    setError(null);
    try {
      await deleteMemory(agentId, index, expectedVersion);
      await load();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusyIndex(null);
    }
  };

  return (
    <Card>
      <div data-testid="agent-memory" style={{ padding: "4px 4px" }}>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 10,
            marginBottom: 6,
          }}
        >
          <div style={{ fontWeight: 600, fontSize: 14, color: C.text2 }}>
            Learned memory
          </div>
          <CliHint
            noCliEquivalent={PARITY_TRACKING_ISSUE}
            actionIds={["memory-edit", "memory-delete"]}
            label="No CLI equivalent"
          />
          <div style={{ marginLeft: "auto" }}>
            <Button label="Refresh" variant="ghost" size="sm" onClick={() => void load()} />
          </div>
        </div>
        <div style={{ color: C.muted, fontSize: 12.5, marginBottom: 12 }}>
          What this agent has learned from prior sessions. Edits and deletes take
          effect at the agent's next session boot.
        </div>

        {error ? (
          <div
            data-testid="memory-error"
            style={{ color: C.destructive, fontSize: 12.5, marginBottom: 10, fontFamily: C.mono }}
          >
            {error}
          </div>
        ) : null}

        {loading ? (
          <Notice padding="28px 20px">Loading memory…</Notice>
        ) : !entries || entries.length === 0 ? (
          <Notice padding="28px 20px">This agent has not learned anything yet.</Notice>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {entries.map((entry) => {
              const busy = busyIndex === entry.index;
              const editing = editingIndex === entry.index;
              const traces = entry.provenance.source_trace_ids ?? [];
              return (
                <div
                  key={entry.index}
                  data-testid="memory-entry"
                  style={{
                    border: "1px solid " + C.border,
                    borderRadius: 6,
                    padding: "10px 12px",
                    background: C.darkest,
                  }}
                >
                  {editing ? (
                    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                      <textarea
                        aria-label="memory-content"
                        value={draft}
                        onChange={(e) => setDraft(e.target.value)}
                        rows={3}
                        style={{
                          width: "100%",
                          background: C.input,
                          color: C.text,
                          border: "1px solid " + C.border,
                          borderRadius: 4,
                          fontFamily: C.mono,
                          fontSize: 12.5,
                          padding: 8,
                          resize: "vertical",
                        }}
                      />
                      <div style={{ display: "flex", gap: 8 }}>
                        <Button
                          label={busy ? "Saving…" : "Save"}
                          variant="primary"
                          size="sm"
                          disabled={busy}
                          onClick={() => void saveEdit(entry.index, entry.version)}
                        />
                        <Button
                          label="Cancel"
                          variant="ghost"
                          size="sm"
                          disabled={busy}
                          onClick={() => setEditingIndex(null)}
                        />
                      </div>
                    </div>
                  ) : (
                    <div style={{ display: "flex", alignItems: "flex-start", gap: 10 }}>
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <div style={{ color: C.text, fontSize: 13, lineHeight: 1.5 }}>
                          {entry.content}
                        </div>
                        <div
                          style={{
                            color: C.muted,
                            fontSize: 11,
                            fontFamily: C.mono,
                            marginTop: 4,
                          }}
                        >
                          {entry.provenance.learned_from_session_id
                            ? `session ${entry.provenance.learned_from_session_id}`
                            : "no session"}
                          {traces.length > 0
                            ? ` · traces: ${traces.join(", ")}`
                            : " · no traces"}
                        </div>
                      </div>
                      <Button
                        label="Edit"
                        variant="ghost"
                        size="sm"
                        disabled={busy}
                        onClick={() => startEdit(entry)}
                      />
                      <Button
                        label={busy ? "…" : "Delete"}
                        variant="ghost"
                        size="sm"
                        disabled={busy}
                        onClick={() => void remove(entry.index, entry.version)}
                      />
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>
    </Card>
  );
}
