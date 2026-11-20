import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { C } from "../../tokens";
import { Button, Card, Chip, CliHint, Notice, SectionTitle } from "../../primitives";
import { cliCommand } from "../../primitives/cliCommand";
import { useAgents } from "../../api/hooks";
import {
  acceptMemoryRule,
  listMemoryRules,
  rejectMemoryRule,
  removeMemoryRule,
  ApiError,
  MemoryRuleRefusal,
  type MemoryRule,
  type MemoryRules,
} from "../../api/client";
import { WiredAgentMemory } from "./WiredAgentMemory";

// The Memory page (#5800): the learned rules of one repository, with accept,
// dismiss and remove, beside the Facts panel the Observability Memory tab used
// to carry. Rules come from GET /agents/{id}/memory/rules; every action sends
// the rule's current version as `expected_version` and reloads the list. A
// refusal shows on the rule's own card and never replaces the page.

type Section = "rules" | "facts";
type Action = "accept" | "reject" | "remove";
type Decide = (agentId: string, ruleId: string, version: number) => Promise<unknown>;

const SECTIONS: readonly (readonly [Section, string])[] = [
  ["rules", "Learned rules"],
  ["facts", "Facts"],
];

const DECIDE: Record<Action, Decide> = {
  accept: acceptMemoryRule,
  reject: rejectMemoryRule,
  remove: removeMemoryRule,
};

interface RepositoryScope {
  key: string;
  label: string;
}

// The login part of `github:<id>:<login>`; null when the author is not in that form.
export function authorOf(proposedBy: string): string | null {
  const m = /^github:[^:\s]+:([^:\s]+)$/.exec(proposedBy);
  return m ? m[1] : null;
}

// The pull request number of the source comment's URL, across the code hosts the
// factory supports; null when the URL has none.
export function pullRequestNumberOf(sourceUrl: string | null): string | null {
  if (!sourceUrl) return null;
  const m = /\/(?:pull|pulls|pull-requests|merge_requests|issues)\/(\d+)(?:[/?#]|$)/.exec(sourceUrl);
  return m ? m[1] : null;
}

// `2026-11-18T10:31:07Z` -> `on 2026-11-18 10:31 UTC`, so the text never depends
// on the viewer's clock or time zone.
export function utcWhen(iso: string | null): string {
  const t = iso ? new Date(iso) : null;
  if (!t || Number.isNaN(t.getTime())) return "at an unknown time";
  return `on ${t.toISOString().slice(0, 16).replace("T", " ")} UTC`;
}

function safeHref(url: string | null): string | null {
  return url && /^https?:\/\//i.test(url) ? url : null;
}

// The refusal sentences of the learned rule routes; an unlisted code shows the
// API's own message (for example write_access_unverifiable).
export function refusalText(error: unknown, repository: string, limit: number): string {
  if (error instanceof MemoryRuleRefusal) {
    switch (error.code) {
      case "memory_rules.write_access_required":
        return `Only someone who can merge to ${repository} can do this. Link your code host account if you can.`;
      case "memory_rules.stale_version":
        return "This rule changed. The list was refreshed.";
      case "memory_rules.scope_full":
        return `${repository} already has ${error.limit ?? limit} rules in force. Remove one first.`;
      case "memory_rules.code_host_unavailable":
        return "The code host could not be reached. Nothing changed.";
      default:
        if (error.status === 503) return "The code host could not be reached. Nothing changed.";
        return error.message;
    }
  }
  if (error instanceof ApiError && error.status === 503) {
    return "The code host could not be reached. Nothing changed.";
  }
  return error instanceof Error ? error.message : String(error);
}

function repositoryScopes(rules: readonly MemoryRule[]): RepositoryScope[] {
  const seen = new Map<string, RepositoryScope>();
  for (const r of rules) {
    if (r.scope_kind === "repository" && !seen.has(r.scope_key)) {
      seen.set(r.scope_key, { key: r.scope_key, label: r.scope_label });
    }
  }
  return [...seen.values()];
}

const meta = { color: C.muted, fontSize: 12, lineHeight: 1.5 } as const;

export function WiredMemoryPage() {
  const agents = useAgents(true);
  const [selected, setSelected] = useState("");
  const [section, setSection] = useState<Section>("rules");
  const [scopeKey, setScopeKey] = useState("");

  const list = agents.data ?? [];
  const agent = list.find((a) => a.id === selected) ?? list[0];

  return (
    <div data-testid="memory-page">
      {agents.loading ? (
        <Notice>Loading agents…</Notice>
      ) : agents.error ? (
        <Notice>Could not load agents: {agents.error}</Notice>
      ) : !agent ? (
        <>
          <SectionTitle title="Memory" />
          <Notice>No agents yet. Deploy an agent to see what it has learned.</Notice>
        </>
      ) : (
        <MemoryBody
          key={agent.id}
          agentId={agent.id}
          agentName={agent.name}
          agentSelector={
            <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 16 }}>
              <span style={{ fontSize: 12.5, color: C.muted }}>Agent</span>
              <select
                data-testid="memory-agent-select"
                aria-label="Agent"
                value={agent.id}
                onChange={(e) => {
                  setSelected(e.target.value);
                  setScopeKey("");
                }}
                style={selectStyle}
              >
                {list.map((a) => (
                  <option key={a.id} value={a.id}>
                    {a.name}
                  </option>
                ))}
              </select>
            </div>
          }
          section={section}
          onSection={setSection}
          scopeKey={scopeKey}
          onScopeKey={setScopeKey}
        />
      )}
    </div>
  );
}

const selectStyle = {
  background: C.input,
  border: "1px solid " + C.borderStrong,
  borderRadius: 7,
  padding: "7px 10px",
  color: C.text,
  fontFamily: C.mono,
  fontSize: 13,
} as const;

function MemoryBody({
  agentId,
  agentName,
  agentSelector,
  section,
  onSection,
  scopeKey,
  onScopeKey,
}: {
  agentId: string;
  agentName: string;
  agentSelector: React.ReactNode;
  section: Section;
  onSection: (s: Section) => void;
  scopeKey: string;
  onScopeKey: (key: string) => void;
}) {
  const [data, setData] = useState<MemoryRules | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState<ReadonlySet<string>>(new Set());
  const newest = useRef(0);
  // One refusal per rule, shown on its card; a note per section after a Dismiss
  // or a Remove (neither has an undo).
  const [refusals, setRefusals] = useState<Record<string, string>>({});
  const [notes, setNotes] = useState<Record<string, { proposed?: string; inForce?: string }>>({});

  const load = useCallback(async () => {
    // Only the newest request's answer is applied: two decisions in a row start
    // two reloads, and the older one may read before the second decision lands.
    const mine = ++newest.current;
    try {
      const fresh = await listMemoryRules(agentId);
      if (mine === newest.current) {
        setData(fresh);
        setError(null);
      }
    } catch (e) {
      if (mine === newest.current) setError(e instanceof Error ? e.message : String(e));
    }
  }, [agentId]);

  useEffect(() => {
    void load();
  }, [load]);

  const scopes = useMemo(() => repositoryScopes(data?.rules ?? []), [data]);
  const scope = scopes.find((s) => s.key === scopeKey) ?? scopes[0];
  const inScope = (data?.rules ?? []).filter(
    (r) => r.scope_kind === "repository" && r.scope_key === scope?.key,
  );
  const proposed = inScope.filter((r) => r.state === "proposed");
  const inForce = inScope.filter((r) => r.state === "accepted");
  const repository = scope?.label ?? "";
  // A refusal belongs on its card; when the refreshed list no longer shows that
  // rule (another person already decided it), it is shown here so it is not lost.
  const shown = new Set([...proposed, ...inForce].map((r) => r.id));
  const orphanRefusals = Object.entries(refusals).filter(([id]) => !shown.has(id));

  const act = async (action: Action, rule: MemoryRule) => {
    setBusy((b) => new Set(b).add(rule.id));
    setRefusals((r) => {
      const { [rule.id]: _drop, ...rest } = r;
      return rest;
    });
    try {
      await DECIDE[action](agentId, rule.id, rule.version);
      if (action === "reject") {
        setNotes((n) => ({ ...n, [rule.scope_key]: { ...n[rule.scope_key], proposed: "Dismissed" } }));
      }
      if (action === "remove") {
        setNotes((n) => ({ ...n, [rule.scope_key]: { ...n[rule.scope_key], inForce: "Removed" } }));
      }
    } catch (e) {
      setRefusals((r) => ({ ...r, [rule.id]: refusalText(e, repository, data?.in_force_limit ?? 16) }));
    } finally {
      // Every outcome reloads: a stale version, an invalid transition and a
      // success all leave the list different from what the person saw.
      await load();
      setBusy((b) => {
        const rest = new Set(b);
        rest.delete(rule.id);
        return rest;
      });
    }
  };

  return (
    <>
      <SectionTitle
        title="Memory"
        sub={
          section === "rules" && scope
            ? `What this factory has learned on ${scope.label}. A rule applies only after someone who can merge here accepts it.`
            : undefined
        }
        right={
          scopes.length > 1 ? (
            <select
              data-testid="memory-repository-select"
              aria-label="Repository"
              value={scope?.key}
              onChange={(e) => onScopeKey(e.target.value)}
              style={selectStyle}
            >
              {scopes.map((s) => (
                <option key={s.key} value={s.key}>
                  {s.label}
                </option>
              ))}
            </select>
          ) : scope ? (
            <Chip>{scope.label}</Chip>
          ) : undefined
        }
      />

      {agentSelector}

      <div
        role="group"
        aria-label="Memory sections"
        style={{ display: "flex", gap: 8, marginBottom: 20, flexWrap: "wrap" }}
      >
        {SECTIONS.map(([id, label]) => (
          <button
            key={id}
            type="button"
            aria-pressed={section === id}
            onClick={() => onSection(id)}
            style={{
              padding: "6px 14px",
              borderRadius: 7,
              border: "1px solid " + (section === id ? C.brand : C.border),
              background: section === id ? C.sel : "transparent",
              color: section === id ? C.text : C.muted,
              fontSize: 13,
              cursor: "pointer",
            }}
          >
            {label}
          </button>
        ))}
      </div>

      {section === "facts" ? (
        <WiredAgentMemory key={`memory-${agentId}`} agentId={agentId} />
      ) : (
        <>
          <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: 8 }}>
            <CliHint command={cliCommand("cluster.memory", { agent: agentName, rules: true })} />
          </div>
          {error ? (
            <div data-testid="memory-rules-error" role="alert" style={{ ...meta, color: C.destructive, marginBottom: 12 }}>
              {error}
            </div>
          ) : null}
          {orphanRefusals.map(([id, text]) => (
            <div key={id} role="alert" data-testid="memory-rule-refusal" style={{ ...meta, color: C.destructive, marginBottom: 12 }}>
              {text}
            </div>
          ))}
          {!data && !error ? (
            <Notice>Loading learned rules…</Notice>
          ) : data && !scope ? (
            <Notice>No repository has learned rules for this agent yet.</Notice>
          ) : data ? (
            <>
              <RuleSection title="Proposed" count={proposed.length} note={notes[scope.key]?.proposed} empty="Nothing waiting for review.">
                {proposed.map((rule) => (
                  <RuleCard key={rule.id} rule={rule} refusal={refusals[rule.id]}>
                    <div style={{ display: "flex", gap: 18, flexWrap: "wrap", ...meta }}>
                      <span>Scope: this repository</span>
                      <span>Applies to: every factory run here, for everyone</span>
                    </div>
                    <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 12 }}>
                      <Button
                        label="Accept"
                        variant="primary"
                        size="sm"
                        disabled={busy.has(rule.id)}
                        onClick={() => void act("accept", rule)}
                      />
                      <Button
                        label="Dismiss"
                        size="sm"
                        disabled={busy.has(rule.id)}
                        onClick={() => void act("reject", rule)}
                      />
                    </div>
                  </RuleCard>
                ))}
              </RuleSection>
              <RuleSection title="In force" count={inForce.length} note={notes[scope.key]?.inForce} empty="No rules yet.">
                {inForce.map((rule) => (
                  <RuleCard key={rule.id} rule={rule} refusal={refusals[rule.id]}>
                    <div style={{ color: C.text2, fontSize: 12.5 }}>
                      <span style={{ color: C.brand }}>✓</span> Accepted by{" "}
                      {rule.accepted_by?.display_name ?? "someone"}, who can merge to this repository,{" "}
                      {utcWhen(rule.accepted_at)}
                    </div>
                    <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 12 }}>
                      <Button
                        label="Remove"
                        variant="danger"
                        size="sm"
                        disabled={busy.has(rule.id)}
                        onClick={() => void act("remove", rule)}
                      />
                    </div>
                  </RuleCard>
                ))}
              </RuleSection>
            </>
          ) : null}
          <WhatWins />
        </>
      )}
    </>
  );
}

function RuleSection({
  title,
  count,
  note,
  empty,
  children,
}: {
  title: string;
  count: number;
  note: string | undefined;
  empty: string;
  children: React.ReactNode;
}) {
  return (
    <section style={{ marginBottom: 24 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
        <h2 style={{ fontSize: 15, fontWeight: 600, color: C.text2, margin: 0 }}>{title}</h2>
        <Chip>{count}</Chip>
      </div>
      {note ? (
        <div role="status" style={{ ...meta, marginBottom: 8 }}>
          {note}
        </div>
      ) : null}
      {count === 0 ? (
        <Notice padding="18px 20px">{empty}</Notice>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>{children}</div>
      )}
    </section>
  );
}

function RuleCard({
  rule,
  refusal,
  children,
}: {
  rule: MemoryRule;
  refusal: string | undefined;
  children: React.ReactNode;
}) {
  const author = authorOf(rule.proposed_by);
  const number = pullRequestNumberOf(rule.source_url);
  const linkText = author && number ? `${author}'s comment on #${number}` : "the source comment";
  const href = safeHref(rule.source_url);
  return (
    <Card style={{ padding: 16 }}>
      <div data-testid="memory-rule" style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        <div style={{ color: C.text, fontSize: 14, lineHeight: 1.5, overflowWrap: "anywhere" }}>{rule.text}</div>
        <div style={meta}>
          Learned from{" "}
          {href ? (
            <a href={href} target="_blank" rel="noopener noreferrer" style={{ color: C.link }}>
              {linkText}
            </a>
          ) : (
            linkText
          )}
        </div>
        <div style={{ ...meta, overflowWrap: "anywhere" }}>
          Why this is a rule and not a one-off: {rule.reason}
        </div>
        {children}
        {refusal ? (
          <div role="alert" data-testid="memory-rule-refusal" style={{ ...meta, color: C.destructive, marginTop: 6 }}>
            {refusal}
          </div>
        ) : null}
      </div>
    </Card>
  );
}

const WINS: readonly (readonly [string, string?, string?])[] = [
  ["Platform and bundle rules"],
  ["The repository's own instruction files"],
  ["Learned rules for this repository", "managed here"],
  ["Your personal coding instructions", undefined, "set per developer, never override a repository rule"],
];

function WhatWins() {
  return (
    <Card style={{ padding: 16 }}>
      <div style={{ display: "flex", alignItems: "baseline", gap: 10, marginBottom: 10 }}>
        <h2 style={{ fontSize: 15, fontWeight: 600, color: C.text2, margin: 0 }}>What wins</h2>
        <span style={meta}>strongest first</span>
      </div>
      <ol style={{ margin: 0, paddingLeft: 22, color: C.text, fontSize: 13.5, lineHeight: 1.8 }}>
        {WINS.map(([label, tag, small]) => (
          <li key={label}>
            {label}
            {tag ? (
              <>
                {" "}
                <Chip color={C.brand} border={C.brand}>
                  {tag}
                </Chip>
              </>
            ) : null}
            {small ? (
              <>
                {" "}
                <span style={{ ...meta, fontSize: 11.5 }}>{small}</span>
              </>
            ) : null}
          </li>
        ))}
      </ol>
      <div style={{ ...meta, marginTop: 10 }}>Like a project file winning over a personal one.</div>
    </Card>
  );
}
