// What a theme looks like, without wearing it.
//
// A three-colour swatch tells you the background and the accent. It cannot tell
// you whether text on a card is comfortable, whether the accent shouts, or
// whether two themes that share a background differ anywhere that matters.
//
// The first version drew a sidebar, a card and a command line: about fifteen of
// the fifty-four colours a theme defines. Themes whose real differences live in
// their status or node colours looked identical in it. This shows the surfaces
// where themes actually diverge, and `themePreview.test.ts` fails if any two
// still land close enough to be indistinguishable.
//
// The palettes are generated with a second selector, `[data-theme-preview]`, so
// the same variable set can be scoped to a subtree. Everything below is drawn
// with ordinary tokens; the wrapper decides which palette they resolve against.

import { ACCENT, F, FONT, HUE, LINE, ON_ACCENT, R, S, STATUS, T } from "../tokens";

/**
 * The variables this preview puts on screen.
 *
 * The test reads it to prove no two themes look alike here, so it is a claim
 * about the component below: anything listed must be visible, and anything
 * shown should be listed.
 */
const NODE_HUES: readonly [string, string][] = [
  ["blue", HUE.blue],
  ["purple", HUE.purple],
  ["orange", HUE.orange],
  ["cyan", HUE.cyan],
  ["teal", HUE.teal],
  ["yellow", HUE.yellow],
  ["red", HUE.red],
  ["grey", HUE.grey],
];

const STATUSES: readonly [string, string][] = [
  ["ok", STATUS.ok],
  ["warn", STATUS.warn],
  ["error", STATUS.danger],
  ["info", STATUS.info],
];

export function ThemePreview({ theme, label }: { readonly theme: string; readonly label: string }) {
  return (
    <div
      data-theme-preview={theme}
      style={{
        // Its own palette and its own surface: sitting on the page's background
        // would blend two themes into one picture and misrepresent both.
        background: S.window,
        borderRadius: R.group,
        border: `1px solid ${LINE.border}`,
        overflow: "hidden",
        display: "flex",
        minHeight: 420,
      }}
    >
      <Rail />
      <div
        style={{
          flex: 1,
          minWidth: 0,
          background: S.content,
          padding: 14,
          display: "grid",
          gap: 11,
          alignContent: "start",
        }}
      >
        <div style={{ ...F.section, color: T.tertiary }}>{label.toUpperCase()}</div>
        <Card />
        <StatusRow />
        <FieldRow />
        <Table />
        <Hues />
        <div
          style={{
            background: S.well,
            borderRadius: R.control,
            padding: "8px 10px",
            fontFamily: FONT.mono,
            fontSize: 11,
            color: T.secondary,
          }}
        >
          curie local status
        </div>
      </div>
    </div>
  );
}

/** The rail, including a selected row, which is a colour of its own. */
function Rail() {
  const rows: [string, boolean][] = [
    ["Overview", false],
    ["Build", true],
    ["Activity", false],
    ["Resources", false],
  ];
  return (
    <div
      style={{
        width: 104,
        flex: "none",
        background: S.sidebarFallback,
        borderRight: `1px solid ${LINE.separator}`,
        padding: "12px 8px",
        display: "flex",
        flexDirection: "column",
        gap: 3,
      }}
    >
      {rows.map(([row, on]) => (
        <div
          key={row}
          style={{
            ...F.footnote,
            color: on ? T.primary : T.secondary,
            background: on ? S.selected : "transparent",
            borderRadius: R.control,
            padding: "4px 7px",
          }}
        >
          {row}
        </div>
      ))}
      <div style={{ flex: 1 }} />
      <div style={{ ...F.footnote, color: T.quaternary, padding: "0 7px" }}>10 running</div>
    </div>
  );
}

function Card() {
  return (
    <div
      style={{
        background: S.cardFill,
        border: `1px solid ${LINE.border}`,
        borderRadius: R.group,
        padding: 11,
        display: "grid",
        gap: 5,
      }}
    >
      <div style={{ ...F.body, color: T.primary }}>An agent is running</div>
      <div style={{ ...F.footnote, color: T.secondary, lineHeight: 1.5 }}>
        Body text at the size you would read it, over the card surface.
      </div>
      <div style={{ ...F.footnote, color: T.quaternary }}>and a quieter caption beneath it</div>
      <div style={{ display: "flex", gap: 6, marginTop: 4 }}>
        <span
          style={{
            ...F.footnote,
            background: ACCENT,
            color: ON_ACCENT,
            borderRadius: R.control,
            padding: "3px 10px",
          }}
        >
          Primary
        </span>
        <span
          style={{
            ...F.footnote,
            background: S.control,
            color: T.secondary,
            borderRadius: R.control,
            padding: "3px 10px",
          }}
        >
          Secondary
        </span>
      </div>
    </div>
  );
}

/** Where many themes actually differ: two dark palettes can share a background
 *  and disagree entirely about what a warning looks like. */
function StatusRow() {
  return (
    <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
      {STATUSES.map(([name, colour]) => (
        <span
          key={name}
          style={{ display: "inline-flex", alignItems: "center", gap: 5, ...F.footnote, color: T.tertiary }}
        >
          <span style={{ width: 8, height: 8, borderRadius: 999, background: colour, flex: "none" }} />
          {name}
        </span>
      ))}
    </div>
  );
}

function FieldRow() {
  return (
    <div
      style={{
        background: S.field,
        border: `1px solid ${LINE.border}`,
        borderRadius: R.control,
        padding: "6px 9px",
        ...F.footnote,
        color: T.tertiary,
      }}
    >
      a text field, unfocused
    </div>
  );
}

/** A striped list: the alternating wash is its own variable and appears nowhere
 *  else in this preview. */
function Table() {
  const rows = ["curie-api", "valkey", "postgres"];
  return (
    <div style={{ borderRadius: R.control, overflow: "hidden", border: `1px solid ${LINE.separator}` }}>
      {rows.map((r, i) => (
        <div
          key={r}
          style={{
            display: "flex",
            justifyContent: "space-between",
            padding: "5px 9px",
            background: i % 2 ? S.subtle : "transparent",
            ...F.footnote,
            color: T.secondary,
          }}
        >
          <span>{r}</span>
          <span style={{ color: T.quaternary, fontVariantNumeric: "tabular-nums" }}>{(i + 1) * 7}%</span>
        </div>
      ))}
    </div>
  );
}

/** The canvas node colours: eight hues shown side by side, which no other part
 *  of the app does, and the clearest single tell between two similar themes. */
function Hues() {
  return (
    <div style={{ display: "flex", gap: 4 }}>
      {NODE_HUES.map(([name, colour]) => (
        <span key={name} title={name} style={{ flex: 1, height: 12, borderRadius: 3, background: colour }} />
      ))}
    </div>
  );
}
