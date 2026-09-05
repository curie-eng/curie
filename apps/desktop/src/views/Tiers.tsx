// Tiers: the parity ladder as a place, not as a prefix on a command name.
//
// Half this app's commands are `local ...` or `cluster ...`, and until this view
// existed the only thing that said so was the word at the front of a monospace
// string in a list. The ladder is the product's central idea -- the same verbs
// against a bigger deployment each rung up -- so it deserves a screen where each
// rung says what it costs, whether it is running here, and what you can do to it.
//
// It is drawn as a MATRIX: one column per rung, one row per verb. That is not a
// way of filling the window, it is the claim the view exists to make. Three
// stacked panels of buttons made the reader hold `skill up`, `local up` and
// `cluster up` in their head and notice for themselves that they were the same
// verb; a row does it for them, and the gaps in a row say the other half --
// the skill tier has no `observability` because it has no platform to record a
// run in. Stacked panels also read badly in a wide window for a reason that is
// really the same fact: three lists of six, ten and eleven buttons are a tall
// ribbon down the left with the width doing nothing.
//
// The rows are DERIVED from the surfaces, never listed here. A verb is whatever
// follows the tier in a command id, so a command added to `tiers.local` grows a
// row (or fills a gap in one) without this file being touched -- which is what
// stops the matrix quietly dropping a command that `surfaces.test.ts` believes
// has a home.
//
// Everything else on it comes from `src/lib/surfaces.ts` too. This file supplies
// only what the map cannot know: whether this machine can reach Docker, how many
// containers are actually up, whether the API answers, and the prose that
// explains a rung to somebody meeting it for the first time.

import { Fragment } from "react";

import { useApp } from "../bridge/app";
import { useResources } from "../bridge/resources";
import { DASH } from "../lib/format";
import type { Command } from "../lib/manifest";
import { resolve, surfacesById, type Action, type Surface } from "../lib/surfaces";
import { ActionButton, Actions, NeedNotice } from "./Actions";
import { ACCENT, F, HUE, LINE, M, R, S, STATUS, T, tint } from "../tokens";
import { Badge, Group, SectionHeader } from "../primitives";

/** What each rung actually is, in one honest sentence about cost and reach. The
 *  manifest describes commands; nothing in it describes a *tier*. */
interface Rung {
  readonly surfaceId: string;
  readonly color: string;
  /** What it takes to have one. */
  readonly cost: string;
  /** What you get for that. The ladder is exactly these two trading off, so
   *  they are two lines and not one: costs more, reaches further. */
  readonly reach: string;
}

const RUNGS: readonly Rung[] = [
  {
    surfaceId: "tiers.skill",
    color: ACCENT,
    cost: "Seconds to start",
    reach: "Only you can reach it, and it forgets everything when it stops",
  },
  {
    surfaceId: "tiers.local",
    color: STATUS.info,
    cost: "About a minute to start",
    reach: "Everything works for real, but only on this computer",
  },
  {
    surfaceId: "tiers.cluster",
    color: HUE.violet,
    cost: "Needs a server",
    reach: "Stays up, and anyone who needs it can reach it",
  },
];

const RUNG_SURFACES: readonly Surface[] = RUNGS.map((r) => surfacesById.get(r.surfaceId)!);

// ---------------------------------------------------------------------------
// The matrix
// ---------------------------------------------------------------------------

/** The command at the end of a tier-scoped id: `local.observability.runs` is the
 *  `observability.runs` verb at the local rung. This is what makes a row. */
function verbOf(id: string): string {
  return id.split(".").slice(1).join(".");
}

/** The concept a row is about, as against the words on its buttons -- which
 *  deliberately differ per rung, because "Boot runner", "Bring up" and "Install
 *  or upgrade" are honestly different acts that happen to be the same verb. */
const VERB_LABEL: Record<string, string> = {
  up: "Start it",
  status: "Check it",
  message: "Talk to it",
  eval: "Score it",
  down: "Stop it",
  comms: "Slack",
  check: "Its tools",
  rebuild: "One piece",
  "github-app": "GitHub identity",
  "migrate-store": "Stored files",
  "observability.runs": "Recent activity",
  "observability.run": "One conversation",
  "observability.metrics": "Usage",
};

/** A verb nobody has named still gets a readable row rather than being dropped:
 *  the matrix has to render whatever the map placed here, or it stops being a
 *  complete answer to "what can I do to this rung". */
function labelFor(verb: string): string {
  const known = VERB_LABEL[verb];
  if (known) return known;
  const last = verb.split(".").pop() ?? verb;
  return last.replace(/-/g, " ").replace(/^./, (c) => c.toUpperCase());
}

type Cell = { readonly action: Action; readonly cmd: Command } | null;

interface MatrixRow {
  readonly verb: string;
  readonly label: string;
  /** One entry per rung, in `RUNGS` order. `null` is "not at this rung". */
  readonly cells: readonly Cell[];
  /** How many rungs offer it -- which is what bands the rows. */
  readonly rungs: number;
}

function buildMatrix(surfaces: readonly Surface[]): readonly MatrixRow[] {
  const order: string[] = [];
  const byVerb = new Map<string, Cell[]>();

  surfaces.forEach((surface, col) => {
    for (const item of resolve(surface)) {
      const verb = verbOf(item.action.id);
      let row = byVerb.get(verb);
      if (!row) {
        row = surfaces.map(() => null);
        byVerb.set(verb, row);
        order.push(verb);
      }
      row[col] = item;
    }
  });

  return (
    order
      .map((verb) => {
        const cells = byVerb.get(verb)!;
        return { verb, label: labelFor(verb), cells, rungs: cells.filter(Boolean).length };
      })
      // Widest first, so the parity spine is at the top and the one-offs sink to
      // the bottom. `sort` is stable, which is what keeps each band in the order
      // the surfaces declared -- up, status, message, eval, down is a lifecycle
      // and re-sorting it alphabetically would lose that.
      .sort((a, b) => b.rungs - a.rungs)
  );
}

const ROWS = buildMatrix(RUNG_SURFACES);

/** The verbs more than one rung offers: real rows, where a gap is a fact worth
 *  stating -- the skill tier has no `observability` because it has no platform
 *  to record a run in. */
const SHARED = ROWS.filter((r) => r.rungs > 1);

/** The verbs exactly one rung offers, per rung.
 *
 *  These are deliberately NOT rows. As rows they were four lines of two dashes
 *  and one button, under a band already titled "only at this rung" -- so every
 *  blank was expected and the dash marking it was noise, and the row label
 *  ("GitHub identity") only restated the button beside it ("GitHub identity").
 *  A column of stacked buttons says the same thing in a quarter of the height. */
const ONLY: readonly (readonly NonNullable<Cell>[])[] = RUNGS.map((_, col) =>
  ROWS.filter((r) => r.rungs === 1)
    .map((r) => r.cells[col])
    .filter((c): c is NonNullable<Cell> => !!c),
);

/** What a band of rows means. Keyed by how many rungs offer the verb, which is
 *  the fact the band is grouping on. */
const BAND: Record<number, string> = {
  3: "Everywhere",
  2: "Once it has a home",
  1: "Only here",
};

/** The label column is bounded rather than `auto` so the three rung columns stay
 *  equal to each other, which is the whole point of drawing this as a grid. */
const COLUMNS = "minmax(78px, 118px) repeat(3, minmax(0, 1fr))";
const PAD = 14;

export function Tiers() {
  const app = useApp();
  const res = useResources();

  const runners = res.samples.filter((s) => s.role === "runner" && s.state === "running").length;
  const stack = res.samples.filter((s) => !!s.service && s.state === "running").length;

  /** What is actually up at each rung, in `RUNGS` order. */
  const live: readonly (string | null)[] = [
    runners ? `${runners} runner${runners === 1 ? "" : "s"} up` : null,
    stack ? `${stack} service${stack === 1 ? "" : "s"} up` : null,
    app.api?.reachable && !app.api.baseUrl.includes("localhost") ? "API answering" : null,
  ];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
      <Explainer />

      {/* No section header. The explainer directly above is this card's
          introduction, and "The same verbs, three rungs" under a box already
          headed "The same agent, three deployments" was the same sentence
          twice -- and a slogan where a label belongs. */}
      <section>
        <Group>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: COLUMNS,
              alignItems: "center",
              columnGap: 10,
              rowGap: 4,
              padding: `0 ${PAD}px ${PAD}px`,
            }}
          >
            {/* The label column has no header: the row labels below name
                themselves, and a word like "Verb" over them would be the table
                explaining its own mechanics. */}
            <div />
            {RUNGS.map((rung, i) => (
              <RungHead
                key={rung.surfaceId}
                rung={rung}
                surface={RUNG_SURFACES[i]}
                live={live[i]}
              />
            ))}

            {SHARED.map((row, i) => (
              <Fragment key={row.verb}>
                {i === 0 || SHARED[i - 1].rungs !== row.rungs ? (
                  <BandLabel first={i === 0}>{BAND[row.rungs] ?? ""}</BandLabel>
                ) : null}

                <div style={{ ...F.footnote, color: T.tertiary }}>{row.label}</div>
                {row.cells.map((cell, c) =>
                  cell ? (
                    <ActionCell key={RUNGS[c].surfaceId} cell={cell} />
                  ) : (
                    <div
                      key={RUNGS[c].surfaceId}
                      style={{ ...F.footnote, color: T.quaternary, paddingLeft: 9 }}
                      title={`No ${row.label.toLowerCase()} at the ${short(RUNG_SURFACES[c])} rung.`}
                    >
                      {DASH}
                    </div>
                  ),
                )}
              </Fragment>
            ))}

            <BandLabel first={false}>{BAND[1]}</BandLabel>
            <div />
            {ONLY.map((cells, c) => (
              <div
                key={RUNGS[c].surfaceId}
                style={{ display: "grid", gap: 4, alignSelf: "start" }}
              >
                {cells.map((cell) => (
                  <ActionCell key={cell.action.id} cell={cell} />
                ))}
              </div>
            ))}
          </div>
        </Group>
      </section>

      {/* Two small groups, side by side rather than stacked: one button in a
          full-width band reads as a card that failed to load. */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 18, alignItems: "start" }}>
        <Actions surface={surfacesById.get("tiers.declarative")!}>
          <div style={{ ...F.footnote, color: T.quaternary, marginTop: 10, lineHeight: 1.55 }}>
            One file describes the setup you want, and applying it changes the server to match.
            Check what would change first — it is the only one of the three that changes nothing.
          </div>
        </Actions>

        <Actions surface={surfacesById.get("tiers.examples")!} />
      </div>
    </div>
  );
}

/** One action in the grid.
 *
 *  Stretched, but capped. Stretching alone made "Stop" a 347px slab; a
 *  natural-width button left the column with the dead band this layout exists to
 *  remove. Capped, the cells line up on both edges and still read as buttons. */
function ActionCell({ cell }: { readonly cell: NonNullable<Cell> }) {
  return (
    <ActionButton
      action={cell.action}
      cmd={cell.cmd}
      // Every cell is a control, `quiet` or not -- see the note on
      // `ActionButton`'s tone override.
      tone={cell.action.tone ?? "default"}
      style={{ width: "100%", maxWidth: 210, justifyContent: "flex-start" }}
    />
  );
}

/** "Skill tier" heads a column under a toolbar that already says Tiers, so the
 *  word is dropped -- derived rather than copied, so the surface stays the one
 *  place the rung is named. */
function short(surface: Surface): string {
  return surface.title.replace(/\s+tier$/i, "");
}

/** One column head: which rung, whether anything is up, and the trade. */
function RungHead({
  rung,
  surface,
  live,
}: {
  readonly rung: Rung;
  readonly surface: Surface;
  readonly live: string | null;
}) {
  return (
    <div style={{ alignSelf: "stretch", padding: `${PAD}px 0 10px`, display: "grid", gap: 5 }}>
      <div style={{ display: "flex", alignItems: "baseline", gap: 8, flexWrap: "wrap" }}>
        <span style={{ ...F.headline, color: T.primary }}>{short(surface)}</span>
        {live ? (
          <Badge color={rung.color} filled>
            {live}
          </Badge>
        ) : (
          <span style={{ ...F.footnote, color: T.quaternary }}>idle</span>
        )}
      </div>
      <div style={{ ...F.footnote, color: T.tertiary, lineHeight: 1.5 }}>{surface.blurb}</div>
      <div style={{ ...F.footnote, color: T.quaternary, lineHeight: 1.5 }}>
        {rung.cost}
        <br />
        {rung.reach}
      </div>
      <NeedNotice need={surface.needs} />
    </div>
  );
}

/** A band header inside the matrix, with its rule bled to the card's edges.
 *  Full width and not inset like a `Row` separator: this is a table, and a
 *  hairline that stops short of the first column would look like it belonged to
 *  the column rather than to the band. */
function BandLabel({ children, first }: { children: string; first: boolean }) {
  return (
    <div
      style={{
        gridColumn: "1 / -1",
        marginLeft: -PAD,
        marginRight: -PAD,
        borderTop: `1px solid ${LINE.separator}`,
        marginTop: first ? 0 : 12,
        paddingTop: 9,
        marginBottom: 3,
      }}
    >
      <div style={{ ...F.section, color: T.quaternary, paddingLeft: PAD }}>{children}</div>
    </div>
  );
}

/** The one paragraph that makes the matrix below read as one idea rather than as
 *  a grid of unrelated buttons. Its measure is capped: the pane is wider than
 *  prose wants to be, and a 150-character line is hard to track back from. */
function Explainer() {
  return (
    <div
      style={{
        background: tint(ACCENT, 0.07),
        border: `1px solid ${LINE.separator}`,
        borderRadius: R.group,
        padding: "12px 14px",
      }}
    >
      <div style={{ ...F.headline, marginBottom: 4 }}>One agent, three places to put it</div>
      <div style={{ ...F.callout, color: T.secondary, lineHeight: 1.6, maxWidth: M.prose }}>
        The same agent works in all three without being changed. Moving right costs more to set up
        and lets more people reach it — and you do the same things to it wherever it is, so nothing
        you learn here has to be learned again there.
      </div>
    </div>
  );
}

/** The whole ladder as one compact strip, for the Overview. Not a duplicate of
 *  the view: it says which rung is live and gets you there, and offers no
 *  commands of its own. */
export function LadderStrip() {
  const app = useApp();
  const res = useResources();
  const runners = res.samples.some((s) => s.role === "runner" && s.state === "running");
  const stack = res.samples.some((s) => !!s.service && s.state === "running");
  const cluster = !!app.api?.reachable && !app.api.baseUrl.includes("localhost");

  const rungs: [string, boolean, string][] = [
    ["Just you", runners, ACCENT],
    ["This computer", stack, STATUS.info],
    ["Your team", cluster, HUE.violet],
  ];

  return (
    <section>
      <SectionHeader>Where your agents can run</SectionHeader>
      {/* Slim: three pills and a link do not need a card's worth of padding,
          and the band of empty space between them was reading as a mistake. */}
      <Group style={{ padding: "7px 12px" }}>
        <button
          onClick={() => app.navigate("tiers")}
          title="See where an agent can run"
          style={{
            display: "flex",
            alignItems: "center",
            gap: 10,
            width: "100%",
            border: "none",
            background: "transparent",
            padding: 0,
            cursor: "default",
            color: "inherit",
          }}
        >
          {rungs.map(([label, on, color], i) => (
            <span key={label} style={{ display: "inline-flex", alignItems: "center", gap: 10 }}>
              {i > 0 ? <span style={{ color: T.quaternary, fontSize: 10 }}>›</span> : null}
              <span
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 6,
                  padding: "3px 9px",
                  borderRadius: R.pill,
                  // `S.control` and not `S.well`: a chip sits ON a card, so it
                  // wants the standard control fill, which is a step lighter
                  // than the surface behind it. `well` is the RECESSED surface --
                  // the darkest thing in the dark palette -- and putting the
                  // dimmest ink on it made an idle tier genuinely unreadable.
                  background: on ? tint(color, 0.16) : S.control,
                  ...F.caption,
                  // Idle is a real state, not a disabled control. `quaternary`
                  // is the placeholder level and says "you cannot use this".
                  color: on ? T.primary : T.secondary,
                }}
              >
                {label}
                <span style={{ ...F.footnote, color: on ? color : T.tertiary }}>
                  {on ? "live" : "idle"}
                </span>
              </span>
            </span>
          ))}
          <span style={{ flex: 1 }} />
          <span style={{ ...F.footnote, color: T.tertiary }}>Where it runs ›</span>
        </button>
      </Group>
    </section>
  );
}
