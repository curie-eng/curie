// Every command the CLI has, as a browsable reference you can run from.
//
// This view is deliberately not the way you are meant to *find* things. Every
// command has a home on a real screen -- an agent's row, a tier's panel, the
// bundle you have open -- declared in `src/lib/surfaces.ts` and rendered there
// as an actual control. What lives here is the complete index: the answer to
// "what can this thing do", which a GUI usually answers worse than `--help`, and
// the place to land when you already know the command's name.
//
// Two things follow from that, and both are visible on screen:
//
//   - Every command shows **where it lives**, with a button that takes you
//     there. A reference whose entries do not say where they belong is how the
//     rest of the app ends up unused.
//   - The list groups **two ways**. "By tier" is the CLI's own shape -- the
//     parity ladder the product is organised around, with the repo-dev namespace
//     fenced off at the bottom the way the CLI fences it. "By place" is this
//     app's shape, and is the one that answers "I know what I want to do, which
//     screen is it on". Neither hides anything: both show all of it.

import { useEffect, useMemo, useRef, useState } from "react";

import { useApp, type Route } from "../bridge/app";
import { commands, type Command, type Tier } from "../lib/manifest";
import { SURFACES, placementsOf } from "../lib/surfaces";
import { CommandForm } from "./CommandForm";
import { ACCENT, F, HUE, LINE, R, S, STATUS, T } from "../tokens";
import {
  Badge,
  Button,
  EmptyState,
  Group,
  Input,
  Mono,
  SectionHeader,
  Segmented,
} from "../primitives";

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

/** One colour per screen, so the "By place" sections read as the four or five
 *  destinations they actually are rather than as fourteen unrelated headings. */
const ROUTE_COLOR: Partial<Record<Route, string>> = {
  build: HUE.teal,
  tiers: ACCENT,
  overview: STATUS.info,
  settings: STATUS.neutral,
  resources: HUE.violet,
};

export function Commands() {
  const app = useApp();
  const [query, setQuery] = useState("");
  // Two ways to read the same 84 commands. "Tier" is the CLI's own shape and is
  // what you want when you know roughly where a command sits on the ladder;
  // "Place" is the app's shape, and is what answers "I know what I want to DO --
  // which screen is it on". Neither is a filter: both show everything.
  const [group, setGroup] = useState<"tier" | "place">("tier");
  const [selectedId, setSelectedId] = useState<string>(commands[0]?.id ?? "");
  const listRef = useRef<HTMLDivElement>(null);

  // The palette, the canvas and the native menu all navigate here with a
  // command in mind. This is React's "adjust state when a prop changes" pattern
  // rather than an effect: the correction happens during the same render, so
  // the list never paints the old selection first and then jump-cuts.
  const [lastFocus, setLastFocus] = useState(app.focus);
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

  const sections = useMemo(() => {
    const q = query.trim().toLowerCase();
    const matches = (c: Command) =>
      !q ||
      c.path.join(" ").includes(q) ||
      c.about.toLowerCase().includes(q) ||
      c.flags.some((f) => f.long?.includes(q)) ||
      // Searching for the place also finds the command, so "agent" turns up
      // everything on the agent sheet rather than only the commands with the
      // word in their help text.
      placementsOf(c.id).some((p) => p.surface.title.toLowerCase().includes(q));

    const hits = commands.filter(matches);

    if (group === "place") {
      // Sectioned by each command's *home*, which is by construction the first
      // surface that lists it. A command reachable from three screens appears
      // once, under the one it belongs to -- the shortcuts are named in the
      // detail panel rather than duplicated in the list.
      return SURFACES.map((surface) => ({
        key: surface.id,
        label: surface.title,
        color: ROUTE_COLOR[surface.route] ?? T.tertiary,
        commands: hits.filter((c) => placementsOf(c.id)[0]?.surface.id === surface.id),
      })).filter((sect) => sect.commands.length);
    }

    // Grouped by each command's own tier. Grouping by its manifest *group*
    // would put the whole top level under one heading -- and the top level is
    // the one place where the tier genuinely varies command to command
    // (`deploy-local` is local-tier, `doctor` is tooling, `init` is authoring).
    return TIER_ORDER.map((tier) => ({
      key: tier,
      label: TIER_META[tier].label,
      color: TIER_META[tier].color,
      commands: hits.filter((c) => c.tier === tier),
    })).filter((sect) => sect.commands.length);
  }, [query, group]);

  const selected = commands.find((c) => c.id === selectedId) ?? null;
  const shown = sections.reduce((n, t) => n + t.commands.length, 0);

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
        <div
          style={{
            padding: 10,
            borderBottom: `1px solid ${LINE.separator}`,
            display: "flex",
            flexDirection: "column",
            gap: 8,
          }}
        >
          <Input
            value={query}
            spellCheck={false}
            placeholder={`Filter ${commands.length} commands…`}
            onChange={(e) => setQuery(e.target.value)}
          />
          <Segmented<"tier" | "place">
            size="sm"
            value={group}
            onChange={setGroup}
            options={[
              { value: "tier", label: "By tier", title: "Grouped the way the CLI is" },
              { value: "place", label: "By place", title: "Grouped by where each one lives in this app" },
            ]}
          />
        </div>

        <div ref={listRef} style={{ overflow: "auto", flex: 1 }}>
          {shown === 0 ? (
            <div style={{ padding: 16, fontSize: 12, color: T.tertiary }}>
              Nothing matches “{query}”.
            </div>
          ) : null}
          {sections.map((section) => (
            <div key={section.key}>
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
                <span style={{ width: 6, height: 6, borderRadius: 2, background: section.color }} />
                <span style={{ fontSize: 11, fontWeight: 600, color: T.secondary }}>
                  {section.label}
                </span>
              </div>
              {section.commands.map((cmd) => {
                const active = cmd.id === selectedId;
                const home = placementsOf(cmd.id)[0];
                return (
                  <button
                    key={cmd.id}
                    data-cmd={cmd.id}
                    onClick={() => setSelectedId(cmd.id)}
                    title={cmd.about}
                    style={{
                      display: "flex",
                      alignItems: "baseline",
                      gap: 7,
                      width: "100%",
                      textAlign: "left",
                      border: "none",
                      borderLeft: `2px solid ${active ? section.color : "transparent"}`,
                      background: active ? S.selected : "transparent",
                      padding: "5px 12px",
                      cursor: "pointer",
                    }}
                  >
                    {/* In place mode the section header already says where this
                        is, so the row carries the label it actually wears on
                        that screen and the command underneath it. In tier mode
                        the command is the subject and nothing competes with it:
                        a truncated second name in every row was noise in the
                        place a scannable list needs least of it. */}
                    {group === "place" ? (
                      <span style={{ flex: 1, minWidth: 0 }}>
                        <span
                          style={{
                            ...F.callout,
                            color: active ? T.primary : T.secondary,
                            display: "block",
                            overflow: "hidden",
                            textOverflow: "ellipsis",
                            whiteSpace: "nowrap",
                          }}
                        >
                          {home?.action.label ?? cmd.name}
                        </span>
                        <Mono
                          style={{
                            fontSize: 10.5,
                            color: T.quaternary,
                            display: "block",
                            overflow: "hidden",
                            textOverflow: "ellipsis",
                            whiteSpace: "nowrap",
                          }}
                        >
                          {cmd.path.join(" ")}
                        </Mono>
                      </span>
                    ) : (
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
                    )}
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
          {shown} of {commands.length} · generated from{" "}
          <Mono style={{ fontSize: 10 }}>curie schema</Mono>
        </div>
      </div>

      <div style={{ flex: 1, minWidth: 0, overflow: "auto" }}>
        {selected ? (
          <>
            <WhereItLives cmd={selected} />
            {/* `Group` has no padding of its own -- a grouped LIST gets it from
                its rows. A form is not a list, so it has to say so, and without
                this the whole form sat flush against the card's edges. */}
            <Group style={{ padding: 16 }}>
              <CommandForm key={selected.id} cmd={selected} prefill={app.prefill ?? undefined} />
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

/**
 * Where this command lives in the app, and the way to go there.
 *
 * The point of the panel is to make this list the *second* place you look. A
 * command that only exists here is a feature with no interface; naming the
 * screen it belongs to, on the command itself, is what turns the reference into
 * a map of the app rather than a substitute for it.
 */
function WhereItLives({ cmd }: { cmd: Command }) {
  const app = useApp();
  const places = placementsOf(cmd.id);
  const home = places[0];
  const also = places.slice(1);

  return (
    // A quiet strip, not a banner. The command is the subject of this pane; this
    // says where else it can be reached from, which is supporting information
    // however important the point behind it is.
    <div
      style={{
        borderBottom: `1px solid ${LINE.separator}`,
        padding: "0 2px 11px",
        marginBottom: 14,
        display: "flex",
        alignItems: "center",
        gap: 12,
        flexWrap: "wrap",
      }}
    >
      {home ? (
        <>
          <div style={{ minWidth: 0, flex: 1 }}>
            <div style={{ ...F.callout, color: T.secondary }}>
              In the app this is{" "}
              <strong style={{ color: T.primary, fontWeight: 600 }}>{home.action.label}</strong>,
              under <strong style={{ color: T.primary, fontWeight: 600 }}>{home.surface.title}</strong>
              .
            </div>
            <div style={{ ...F.footnote, color: T.quaternary, marginTop: 2 }}>
              {/* Where, in directions you could follow. A route name alone is
                  not an answer for a group that lives inside something you have
                  to open first. */}
              You will find it {home.surface.where}.
              {also.length ? ` Also on ${also.map((p) => p.surface.title).join(", ")}.` : ""}
            </div>
          </div>
          <Button
            size="sm"
            onClick={() => app.navigate(home.surface.route)}
            title={`Open the ${home.surface.route} view — this command is ${home.surface.where}`}
          >
            Take me there
          </Button>
        </>
      ) : (
        // The placement test makes this unreachable in a built app, so it is a
        // statement of fact rather than a fallback: if it ever shows, a command
        // shipped without anyone deciding where it belongs.
        <div style={{ ...F.callout, color: STATUS.warn }}>
          This command has no home anywhere in the app yet — it is reachable only from this list.
        </div>
      )}
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
