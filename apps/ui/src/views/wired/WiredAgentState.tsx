import { useCallback, useEffect, useState } from "react";
import { C } from "../../tokens";
import { Button, Card, Chip, Notice } from "../../primitives";
import {
  isUnauthorized,
  listStateNamespaces,
  listStateEntries,
  type StateNamespace,
  type StateEntry,
} from "../../api/client";

// The operator read/inspect surface for the durable state store (#250, part of
// #23). An agent's cross-turn state lives as namespace+key JSON documents in the
// store; this panel lets an operator see WHAT an agent has stored: its
// namespaces (with key counts and last-write time), and, for a selected
// namespace, every key with its value, compare-and-set version, and most recent
// write. Read-only: it never mutates the store. Backed by the real API over the
// same-origin /api proxy (GET /agents/{id}/state and .../state/{namespace}).
//
// Those routes accept only the platform key or a sandbox state token, never the
// console session (#1047, ADR-0083), so under cookie auth a read is refused with
// a 401. The panel says so instead of claiming the agent stored nothing; letting
// a session read state is a deferred follow-up that needs an ADR amendment.

function StateUnavailable({ padding }: { padding: string }) {
  return (
    <div data-testid="state-unavailable">
      <Notice padding={padding}>Durable state is not readable from a console session yet.</Notice>
    </div>
  );
}

function formatWhen(iso: string): string {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

function formatValue(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

export function WiredAgentState({ agentId }: { agentId: string }) {
  const [namespaces, setNamespaces] = useState<StateNamespace[] | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [entries, setEntries] = useState<StateEntry[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refused, setRefused] = useState(false);
  const [entriesLoading, setEntriesLoading] = useState(false);
  const [entriesError, setEntriesError] = useState<string | null>(null);
  const [entriesRefused, setEntriesRefused] = useState(false);

  const loadNamespaces = useCallback(async () => {
    setLoading(true);
    setError(null);
    setRefused(false);
    try {
      setNamespaces(await listStateNamespaces(agentId));
    } catch (e) {
      if (isUnauthorized(e)) setRefused(true);
      else setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [agentId]);

  const loadEntries = useCallback(
    async (namespace: string) => {
      setSelected(namespace);
      setEntriesLoading(true);
      setEntriesError(null);
      setEntriesRefused(false);
      setEntries(null);
      try {
        const rows = await listStateEntries(agentId, namespace);
        // Most-recently-written first, so the "recent writes" read the way an
        // operator expects (the API lists by key; we re-sort by write time).
        rows.sort((a, b) => b.updated_at.localeCompare(a.updated_at));
        setEntries(rows);
      } catch (e) {
        if (isUnauthorized(e)) setEntriesRefused(true);
        else setEntriesError(e instanceof Error ? e.message : String(e));
      } finally {
        setEntriesLoading(false);
      }
    },
    [agentId],
  );

  useEffect(() => {
    void loadNamespaces();
  }, [loadNamespaces]);

  return (
    <Card>
      <div data-testid="agent-state" style={{ padding: "4px 4px" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
          <div style={{ fontWeight: 600, fontSize: 14, color: C.text2 }}>Durable state</div>
          <div style={{ marginLeft: "auto" }}>
            <Button
              label="Refresh"
              variant="ghost"
              size="sm"
              onClick={() => {
                void loadNamespaces();
                if (selected) void loadEntries(selected);
              }}
            />
          </div>
        </div>
        <div style={{ color: C.muted, fontSize: 12.5, marginBottom: 12 }}>
          What this agent has stored across turns: its namespaces and keys, and the
          most recent write to each. Read-only.
        </div>

        {error ? (
          <div
            data-testid="state-error"
            style={{ color: C.destructive, fontSize: 12.5, marginBottom: 10, fontFamily: C.mono }}
          >
            {error}
          </div>
        ) : null}

        {loading ? (
          <Notice padding="28px 20px">Loading state…</Notice>
        ) : refused ? (
          <StateUnavailable padding="28px 20px" />
        ) : !namespaces ? null : namespaces.length === 0 ? (
          // Only a successful empty read is evidence that nothing was stored.
          error ? null : <Notice padding="28px 20px">This agent has not stored any durable state yet.</Notice>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
              {namespaces.map((ns) => {
                const active = ns.namespace === selected;
                return (
                  <button
                    key={ns.namespace}
                    type="button"
                    data-testid="state-namespace"
                    onClick={() => void loadEntries(ns.namespace)}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: 8,
                      border: "1px solid " + (active ? C.brand : C.border),
                      borderRadius: 6,
                      padding: "7px 10px",
                      background: active ? C.hover : C.darkest,
                      color: C.text,
                      cursor: "pointer",
                      fontSize: 12.5,
                    }}
                  >
                    <span style={{ fontFamily: C.mono }}>{ns.namespace}</span>
                    <Chip color={C.mutedStatus} border={C.border}>
                      {`${ns.key_count} ${ns.key_count === 1 ? "key" : "keys"}`}
                    </Chip>
                    <span style={{ color: C.muted, fontSize: 11 }}>{formatWhen(ns.last_updated)}</span>
                  </button>
                );
              })}
            </div>

            {selected ? (
              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                <div style={{ color: C.text2, fontSize: 12.5, fontFamily: C.mono }}>
                  {selected} · recent writes first
                </div>
                {entriesError ? (
                  <div
                    data-testid="state-entries-error"
                    style={{ color: C.destructive, fontSize: 12.5, fontFamily: C.mono }}
                  >
                    {entriesError}
                  </div>
                ) : null}
                {entriesLoading ? (
                  <Notice padding="20px">Loading keys…</Notice>
                ) : entriesRefused ? (
                  <StateUnavailable padding="20px" />
                ) : !entries || entries.length === 0 ? (
                  <Notice padding="20px">No keys in this namespace.</Notice>
                ) : (
                  entries.map((entry) => (
                    <div
                      key={entry.key}
                      data-testid="state-entry"
                      style={{
                        border: "1px solid " + C.border,
                        borderRadius: 6,
                        padding: "10px 12px",
                        background: C.darkest,
                      }}
                    >
                      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
                        <span style={{ fontFamily: C.mono, fontSize: 12.5, color: C.text }}>{entry.key}</span>
                        <Chip color={C.mutedStatus} border={C.border}>{`v${entry.version}`}</Chip>
                        <span style={{ marginLeft: "auto", color: C.muted, fontSize: 11, fontFamily: C.mono }}>
                          {formatWhen(entry.updated_at)}
                        </span>
                      </div>
                      <pre
                        style={{
                          margin: 0,
                          background: C.input,
                          color: C.text,
                          border: "1px solid " + C.border,
                          borderRadius: 4,
                          fontFamily: C.mono,
                          fontSize: 12,
                          padding: 8,
                          overflowX: "auto",
                          whiteSpace: "pre-wrap",
                          wordBreak: "break-word",
                        }}
                      >
                        {formatValue(entry.value)}
                      </pre>
                    </div>
                  ))
                )}
              </div>
            ) : null}
          </div>
        )}
      </div>
    </Card>
  );
}
