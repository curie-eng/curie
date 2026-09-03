// What a theme looks like, without wearing it.
//
// The theme list is a column of names and three-colour swatches. A swatch tells
// you the background and the accent and nothing about how the two sit together
// -- whether text on a card is comfortable, whether the accent shouts. The only
// way to find that out was to apply the theme and look at the whole window.
//
// The palettes are generated with a second selector, `[data-theme-preview]`, so
// the same variable set can be scoped to a subtree. Everything below is drawn
// with ordinary tokens; the wrapper decides which palette they resolve against.

import { ACCENT, F, FONT, LINE, ON_ACCENT, R, S, T } from "../tokens";

export function ThemePreview({ theme, label }: { readonly theme: string; readonly label: string }) {
  return (
    <div
      data-theme-preview={theme}
      style={{
        // Its own palette, and its own surface: sitting on the page's
        // background would blend two themes into one picture and misrepresent
        // both.
        background: S.window,
        borderRadius: R.group,
        border: `1px solid ${LINE.border}`,
        overflow: "hidden",
        display: "flex",
        minHeight: 232,
      }}
    >
      <div
        style={{
          width: 92,
          flex: "none",
          background: S.sidebarFallback,
          borderRight: `1px solid ${LINE.separator}`,
          padding: "10px 8px",
          display: "flex",
          flexDirection: "column",
          gap: 4,
        }}
      >
        {["Overview", "Build", "Activity"].map((row, i) => (
          <div
            key={row}
            style={{
              ...F.footnote,
              color: i === 0 ? T.primary : T.secondary,
              background: i === 0 ? S.controlHover : "transparent",
              borderRadius: R.control,
              padding: "3px 6px",
            }}
          >
            {row}
          </div>
        ))}
      </div>

      <div style={{ flex: 1, minWidth: 0, background: S.content, padding: 12 }}>
        <div style={{ ...F.section, color: T.tertiary, marginBottom: 7 }}>{label.toUpperCase()}</div>

        <div
          style={{
            background: S.cardFill,
            border: `1px solid ${LINE.border}`,
            borderRadius: R.group,
            padding: 10,
            display: "grid",
            gap: 5,
          }}
        >
          <div style={{ ...F.body, color: T.primary }}>An agent is running</div>
          <div style={{ ...F.footnote, color: T.tertiary, lineHeight: 1.5 }}>
            Body text sits here, at the size you would actually read it.
          </div>
          <div style={{ display: "flex", gap: 6, alignItems: "center", marginTop: 3 }}>
            <span
              style={{
                ...F.footnote,
                background: ACCENT,
                color: ON_ACCENT,
                borderRadius: R.control,
                padding: "3px 9px",
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
                padding: "3px 9px",
              }}
            >
              Secondary
            </span>
          </div>
        </div>

        <div
          style={{
            marginTop: 9,
            background: S.well,
            borderRadius: R.control,
            padding: "7px 9px",
            ...F.footnote,
            color: T.secondary,
            fontFamily: FONT.mono,
          }}
        >
          curie local status
        </div>
      </div>
    </div>
  );
}
