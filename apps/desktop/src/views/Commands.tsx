// Every command the CLI has, as a browsable reference you can run from.
//
// The list on the left is not a curated menu -- it is the manifest, grouped by
// the parity ladder the product is organised around (author it, run it as a
// skill, run it locally, run it on a cluster) with the repo-dev namespace fenced
// off at the bottom the way the CLI fences it. Nothing is omitted and nothing is
// hand-added, so this view is a complete answer to "what can I do", which is the
// question a GUI usually answers worse than `--help`.

import { useEffect, useMemo, useRef, useState } from "react";

import { useApp } from "../bridge/app";
import { commands, type Command, type Tier } from "../lib/manifest";
import { CommandForm } from "./CommandForm";
import { ACCENT, HUE, LINE, R, S, STATUS, T } from "../tokens";
import { Badge, EmptyState, Group, Input, Mono, Notice, SectionHeader } from "../primitives";

const TIER_ORDER: readonly Tier[] = ["author", "skill", "local", "cluster", "platform", "dev"];

const TIER_META: Record<Tier, { label: string; color: string; blurb: string }> = {
  author: {
    label: "Author",
    color: HUE.teal,
    blurb: "Scaffold a bundle and see what you have built.",
  },
  skill: {
    label: "Skill tier",
    color: ACCENT,
    blurb: "One container, straight from your working directory. The fast loop.",
  },
  local: {
    label: "Local tier",
    color: STATUS.info,
    blurb: "The whole platform on Docker Compose, on this machine.",
  },
  cluster: {
    label: "Cluster tier",
    color: HUE.violet,
    blurb: "The same platform on Kubernetes, via Helm.",
  },
  platform: {
    label: "Platform & tooling",
    color: STATUS.neutral,
    blurb: "Secrets, schemas, diagnostics, and the declarative install file.",
  },
  dev: {
    label: "Repo dev",
    color: STATUS.warn,
    blurb: "Contributor scripts. These need a source checkout, not a released binary.",
  },
};

export function Commands() {
  const app = useApp();
  const [query, setQuery] = useState("");
  const [selectedId, setSelectedId] = useState<string>(commands[0]?.id ?? "");
  const listRef = useRef<HTMLDivElement>(null);

  // The palette, the canvas and the native menu all navigate here with a
  // command in mind. This is React's "adjust state when a prop changes" pattern
  // rather than an effect: the correction happens during the same render, so
  // the list never paints the old selection first and then jump-cuts.
  const [lastFocus, setLastFocus] = useState<string | null>(app.focus);
  if (app.focus !== lastFocus) {
    setLastFocus(app.focus);
    if (app.focus && commands.some((c) => c.id === app.focus)) {
      setSelectedId(app.focus);
      setQuery("");
    }
  }

  // Scrolling is a DOM side effect, so it does belong in an effect.
  useEffect(() => {
    if (!selectedId) return;
    listRef.current
      ?.querySelector<HTMLElement>(`[data-cmd="${selectedId}"]`)
      ?.scrollIntoView({ block: "nearest" });
  }, [selectedId]);

  const byTier = useMemo(() => {
    const q = query.trim().toLowerCase();
    const matches = (c: Command) =>
      !q ||
      c.path.join(" ").includes(q) ||
      c.about.toLowerCase().includes(q) ||
      c.flags.some((f) => f.long?.includes(q));

    // Grouped by each command's own tier. Grouping by its manifest *group*
    // would put the whole top level under one heading -- and the top level is
    // the one place where the tier genuinely varies command to command
    // (`deploy-local` is local-tier, `doctor` is tooling, `init` is authoring).
    return TIER_ORDER.map((tier) => ({
      tier,
      commands: commands.filter((c) => c.tier === tier && matches(c)),
    })).filter((t) => t.commands.length);
  }, [query]);

  const selected = commands.find((c) => c.id === selectedId) ?? null;
  const shown = byTier.reduce((n, t) => n + t.commands.length, 0);

  return (
    <div style={{ display: "flex", gap: 16, height: "100%", minHeight: 520 }}>
      <div
        style={{
          width: 300,
          flex: "none",
          display: "flex",
          flexDirection: "column",
          border: `1px solid ${LINE.separator}`,
          borderRadius: R.group,
          overflow: "hidden",
          background: S.raised,
        }}
      >
        <div style={{ padding: 10, borderBottom: `1px solid ${LINE.separator}` }}>
          <Input
            value={query}
            spellCheck={false}
            placeholder={`Filter ${commands.length} commands…`}
            onChange={(e) => setQuery(e.target.value)}
          />
        </div>

        <div ref={listRef} style={{ overflow: "auto", flex: 1 }}>
          {shown === 0 ? (
            <div style={{ padding: 16, fontSize: 12, color: T.tertiary }}>
              Nothing matches “{query}”.
            </div>
          ) : null}
          {byTier.map(({ tier, commands: tierCommands }) => (
            <div key={tier}>
              <div
                style={{
                  position: "sticky",
                  top: 0,
                  zIndex: 1,
                  background: S.well,
                  borderTop: `1px solid ${LINE.separator}`,
                  borderBottom: `1px solid ${LINE.separator}`,
                  padding: "6px 12px",
                  display: "flex",
                  alignItems: "center",
                  gap: 7,
                }}
              >
                <span
                  style={{ width: 6, height: 6, borderRadius: 2, background: TIER_META[tier].color }}
                />
                <span style={{ fontSize: 11, fontWeight: 600, color: T.secondary }}>
                  {TIER_META[tier].label}
                </span>
              </div>
              {tierCommands.map((cmd) => {
                const active = cmd.id === selectedId;
                return (
                    <button
                      key={cmd.id}
                      data-cmd={cmd.id}
                      onClick={() => setSelectedId(cmd.id)}
                      title={cmd.about}
                      style={{
                        display: "flex",
                        alignItems: "center",
                        gap: 7,
                        width: "100%",
                        textAlign: "left",
                        border: "none",
                        borderLeft: `2px solid ${active ? TIER_META[tier].color : "transparent"}`,
                        background: active ? S.selected : "transparent",
                        padding: "5px 12px",
                        cursor: "pointer",
                      }}
                    >
                      <Mono
                        style={{
                          flex: 1,
                          fontSize: 11.5,
                          color: active ? T.primary : T.secondary,
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                        }}
                      >
                        {cmd.path.join(" ")}
                      </Mono>
                    {cmd.risk === "destructive" ? (
                      <span title="destructive" style={{ color: STATUS.danger, fontSize: 11 }}>
                        ●
                      </span>
                    ) : null}
                  </button>
                );
              })}
            </div>
          ))}
        </div>

        <div
          style={{
            padding: "7px 12px",
            borderTop: `1px solid ${LINE.separator}`,
            fontSize: 10.5,
            color: T.tertiary,
          }}
        >
          {shown} of {commands.length} · generated from <Mono style={{ fontSize: 10 }}>curie schema</Mono>
        </div>
      </div>

      <div style={{ flex: 1, minWidth: 0, overflow: "auto" }}>
        {selected ? (
          <>
            <div style={{ marginBottom: 14 }}>
              <Notice tone="info">
                <span style={{ color: T.secondary }}>{TIER_META[selected.tier].blurb}</span>
              </Notice>
            </div>
            <Group>
              <CommandForm key={selected.id} cmd={selected} />
            </Group>
            <RelatedCommands cmd={selected} onPick={setSelectedId} />
          </>
        ) : (
          <EmptyState title="Pick a command">
            Every command is here, exactly as the CLI declares it.
          </EmptyState>
        )}
      </div>
    </div>
  );
}

/** The same verb at the other tiers. `local kill` and `cluster kill` are the
 *  same operation against different deployments, and an operator moving up the
 *  ladder wants that jump to be one click, not a re-search. */
function RelatedCommands({ cmd, onPick }: { cmd: Command; onPick(id: string): void }) {
  const siblings = commands.filter((c) => c.id !== cmd.id && c.name === cmd.name);
  if (!siblings.length) return null;
  return (
    <div style={{ marginTop: 16 }}>
      <SectionHeader>Same command, other tiers</SectionHeader>
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
        {siblings.map((s) => (
          <button
            key={s.id}
            onClick={() => onPick(s.id)}
            style={{
              background: S.raised,
              border: `1px solid ${LINE.separator}`,
              borderRadius: R.control,
              padding: "6px 10px",
              cursor: "pointer",
              display: "inline-flex",
              alignItems: "center",
              gap: 8,
            }}
          >
            <Mono style={{ color: T.secondary }}>curie {s.path.join(" ")}</Mono>
            <Badge color={TIER_META[s.tier].color}>{s.tier}</Badge>
          </button>
        ))}
      </div>
    </div>
  );
}
