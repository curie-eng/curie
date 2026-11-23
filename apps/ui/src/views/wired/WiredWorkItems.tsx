import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { C } from "../../tokens";
import { Card, SectionTitle, EmptyState, Notice, Button, CliHint, cliCommand } from "../../primitives";
import { useStore } from "../../state/store";
import { useWired } from "../../state/wired";
import {
  ApiError,
  getWorkItem,
  listMemoryRules,
  listWorkItems,
  type MemoryRule,
  type WorkItemOutcome,
} from "../../api/client";
import { issueRef } from "./factoryParts";

// Factory outcomes console (#2577): the GitHub-issue-driven agent's queue of
// work items, one row per issue it took on. State and cause strings come
// straight from the API and are rendered verbatim — the console never
// re-derives them (D1).

export function WiredWorkItems() {
  const { state } = useStore();
  const { agents } = useWired();
  const queryClient = useQueryClient();
  const [agentId, setAgentId] = useState("");
  const [selectedId, setSelectedId] = useState<string | null>(null);

  const listQuery = useQuery({
    queryKey: ["workItems", agentId || null],
    queryFn: () => listWorkItems({ agentId: agentId || undefined }),
  });

  const detailQuery = useQuery({
    queryKey: ["workItem", selectedId],
    enabled: !!selectedId,
    queryFn: async (): Promise<{ item: WorkItemOutcome | null; notFound: boolean }> => {
      try {
        const item = await getWorkItem(selectedId!);
        return { item, notFound: false };
      } catch (e) {
        if (e instanceof ApiError && e.status === 404) return { item: null, notFound: true };
        throw e;
      }
    },
  });

  const items = listQuery.data?.items ?? [];

  function refresh() {
    void queryClient.invalidateQueries({ queryKey: ["workItems", agentId || null] });
    if (selectedId) void queryClient.invalidateQueries({ queryKey: ["workItem", selectedId] });
  }

  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", marginBottom: 20 }}>
        <div>
          <SectionTitle
            title="Work items"
            sub="Issues the factory agent has taken on, and where each one stands."
            right={
              <CliHint command={cliCommand(state.env === "prod" ? "cluster.work-items" : "local.work-items")} />
            }
          />
        </div>
        <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 8 }}>
          <label htmlFor="work-items-agent-filter" style={{ fontSize: 12, color: C.muted }}>
            Agent
          </label>
          <select
            id="work-items-agent-filter"
            aria-label="Agent"
            value={agentId}
            onChange={(e) => {
              // The detail pane is scoped to the selected agent; switching
              // agents drops the prior scope's item rather than leaving a
              // stale detail on screen.
              setAgentId(e.target.value);
              setSelectedId(null);
            }}
            style={{
              background: C.input,
              border: "1px solid " + C.border,
              borderRadius: 7,
              color: C.text,
              fontSize: 13,
              padding: "6px 8px",
            }}
          >
            <option value="">All agents</option>
            {agents.map((a) => (
              <option key={a.id} value={a.id}>
                {a.name}
              </option>
            ))}
          </select>
          <Button label="Refresh" variant="secondary" size="sm" onClick={refresh} />
        </div>
      </div>

      {listQuery.data?.truncated ? (
        <div
          data-testid="work-items-truncated"
          style={{ marginBottom: 12, padding: "8px 12px", textAlign: "center", color: C.muted, fontSize: 13 }}
        >
          {`Showing the first ${listQuery.data.limit} work items; more exist than fit this view.`}
        </div>
      ) : null}

      {listQuery.isPending ? <Notice>Loading work items…</Notice> : null}
      {listQuery.error ? <Notice>{`Could not load work items: ${String(listQuery.error)}`}</Notice> : null}
      {!listQuery.isPending && !listQuery.error && items.length === 0 ? (
        <EmptyState title="No factory work items yet" sub="Once the factory agent picks up a GitHub issue, it shows up here." />
      ) : null}

      {items.length > 0 ? (
        <Card>
          <div style={{ display: "flex", flexDirection: "column", gap: 0 }}>
            {items.map((item) => (
              <WorkItemRow key={item.id} item={item} onOpen={() => setSelectedId(item.id)} />
            ))}
          </div>
        </Card>
      ) : null}

      {selectedId ? (
        <div style={{ marginTop: 20 }}>
          {detailQuery.isPending ? <Notice>Loading work item…</Notice> : null}
          {detailQuery.error ? <Notice>{`Could not load work item: ${String(detailQuery.error)}`}</Notice> : null}
          {detailQuery.data?.notFound ? <Notice>This work item no longer exists.</Notice> : null}
          {detailQuery.data?.item ? <WorkItemDetail item={detailQuery.data.item} /> : null}
        </div>
      ) : null}
    </div>
  );
}

function WorkItemRow({ item, onOpen }: { item: WorkItemOutcome; onOpen: () => void }) {
  return (
    <div
      role="button"
      tabIndex={0}
      data-testid="work-item-row"
      onClick={onOpen}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") onOpen();
      }}
      style={{
        display: "flex",
        alignItems: "center",
        gap: 16,
        width: "100%",
        textAlign: "left",
        background: "none",
        border: "none",
        borderBottom: "1px solid " + C.border,
        padding: "12px 4px",
        cursor: "pointer",
        color: C.text,
        fontFamily: C.sans,
      }}
    >
      <span
        style={{
          fontFamily: C.mono,
          fontSize: 12,
          padding: "2px 8px",
          borderRadius: 20,
          border: "1px solid " + C.borderStrong,
          color: C.text2,
        }}
      >
        {item.state}
      </span>
      <a
        href={item.tracker.url}
        target="_blank"
        rel="noreferrer"
        onClick={(e) => e.stopPropagation()}
        style={{ color: C.link, fontFamily: C.mono, fontSize: 13 }}
      >
        {issueRef(item)}
      </a>
      <span style={{ fontSize: 13, color: C.text2, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
        {item.actionable_cause}
      </span>
      {item.pr ? (
        <a
          href={item.pr.url}
          target="_blank"
          rel="noreferrer"
          onClick={(e) => e.stopPropagation()}
          style={{ color: C.link, fontFamily: C.mono, fontSize: 13 }}
        >
          #{item.pr.number}
        </a>
      ) : (
        <span style={{ fontSize: 13, color: C.muted }}>none</span>
      )}
    </div>
  );
}

// The pinned rules are named by number and digest on the work item; the text
// is read from the agent's rule list (every state, so a rule removed after
// admission still resolves). A rule matches on the repository scope the work
// item froze at admission plus its number, since numbers repeat across scopes.
function repositoryScopeKey(repo: WorkItemOutcome["repository"]): string {
  return `${repo.code_host_kind}:${repo.host}:${repo.project_id}`;
}

function PinnedRules({ item }: { item: WorkItemOutcome }) {
  const pinned = item.memory_rules;
  const numbers = pinned?.rule_numbers ?? [];
  const rulesQuery = useQuery({
    queryKey: ["memoryRules", item.agent_id],
    enabled: numbers.length > 0,
    queryFn: () => listMemoryRules(item.agent_id),
  });
  if (!pinned || pinned.state !== "selected" || numbers.length === 0) return null;
  const scopeKey = repositoryScopeKey(item.repository);
  const byNumber = new Map<number, MemoryRule>();
  for (const rule of rulesQuery.data?.rules ?? []) {
    if (rule.scope_kind === "repository" && rule.scope_key === scopeKey) byNumber.set(rule.number, rule);
  }
  return (
    <div data-testid="work-item-learned-rules">
      <div style={{ color: C.muted, fontSize: 11, marginBottom: 3 }}>learned rules</div>
      <div style={{ color: C.muted, fontSize: 12, marginBottom: 6 }}>
        {`${pinned.count} pinned when this work item was admitted`}
        {pinned.sha256 ? (
          <>
            {" · "}
            <span style={{ fontFamily: C.mono }} title={pinned.sha256}>
              sha256 {pinned.sha256.slice(0, 12)}
            </span>
          </>
        ) : null}
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        {numbers.map((n) => {
          const rule = byNumber.get(n);
          return (
            <div key={n} data-testid="work-item-learned-rule" style={{ display: "flex", gap: 10, alignItems: "baseline" }}>
              <span style={{ fontFamily: C.mono, fontSize: 12, color: C.text2 }}>{`R${n}`}</span>
              <span>
                {rule ? rule.text : <span style={{ color: C.muted }}>{rulesQuery.isPending ? "Loading rule text…" : "Rule text unavailable."}</span>}
                {rule && rule.state !== "accepted" ? (
                  <span style={{ color: C.muted, fontSize: 12 }}>{` (${rule.state} since)`}</span>
                ) : null}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function WorkItemDetail({ item }: { item: WorkItemOutcome }) {
  return (
    <Card>
      <div data-testid="work-item-detail" style={{ display: "flex", flexDirection: "column", gap: 12, fontSize: 13 }}>
        <div data-testid="work-item-detail-header" style={{ display: "flex", alignItems: "baseline", gap: 12 }}>
          <a
            href={item.tracker.url}
            target="_blank"
            rel="noreferrer"
            style={{ color: C.link, fontFamily: C.mono, fontSize: 15, fontWeight: 600 }}
          >
            {issueRef(item)}
          </a>
          {item.pr ? (
            <a
              href={item.pr.url}
              target="_blank"
              rel="noreferrer"
              style={{ color: C.link, fontFamily: C.mono, fontSize: 13 }}
            >
              PR #{item.pr.number} ({item.pr.status})
            </a>
          ) : (
            <span style={{ fontSize: 13, color: C.muted }}>no PR</span>
          )}
        </div>
        <div>
          <div style={{ color: C.muted, fontSize: 11, marginBottom: 3 }}>state</div>
          <div style={{ fontFamily: C.mono }}>{item.state}</div>
        </div>
        <div>
          <div style={{ color: C.muted, fontSize: 11, marginBottom: 3 }}>cause</div>
          <div>{item.actionable_cause}</div>
        </div>
        <div>
          <div style={{ color: C.muted, fontSize: 11, marginBottom: 3 }}>objective</div>
          <div>{item.objective}</div>
        </div>
        <div>
          <div style={{ color: C.muted, fontSize: 11, marginBottom: 3 }}>CI</div>
          {item.ci ? (
            <div style={{ fontFamily: C.mono }}>
              {item.ci.state}
              {item.ci.reason ? ` — ${item.ci.reason}` : ""}
              {" · observed "}
              <time dateTime={item.ci.observed_at ?? undefined}>{item.ci.observed_at}</time>
            </div>
          ) : (
            <div style={{ color: C.muted }}>no CI observation yet</div>
          )}
        </div>
        <div>
          <div style={{ color: C.muted, fontSize: 11, marginBottom: 3 }}>correctness</div>
          <div style={{ color: C.muted }}>
            Correctness is not asserted by the platform. Owned by the bundle.
          </div>
        </div>
        {item.publication ? (
          <div>
            <div style={{ color: C.muted, fontSize: 11, marginBottom: 3 }}>publication</div>
            <div style={{ fontFamily: C.mono }}>
              {item.publication.status} · revision {item.publication.revision_number} · {item.publication.approval_status}
            </div>
          </div>
        ) : null}
        <PinnedRules item={item} />
        {item.requests.length > 0 ? (
          <div>
            <div style={{ color: C.muted, fontSize: 11, marginBottom: 3 }}>requests</div>
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              {item.requests.map((r) => (
                <div key={r.sequence} style={{ fontFamily: C.mono, fontSize: 12 }}>
                  #{r.sequence} {r.status}
                </div>
              ))}
            </div>
          </div>
        ) : null}
      </div>
    </Card>
  );
}
