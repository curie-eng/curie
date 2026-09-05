// Design tokens.
//
// These are deliberately NOT the web console's tokens. `apps/ui` is a page in a
// browser and is styled like one: flat cards on a flat field, separated by 1px
// borders. Reproducing that in a window is what makes an app read as "a website
// someone wrapped", which is the single most common way a desktop app built with
// web technology gives itself away.
//
// So the vocabulary here is the platform's, not the web's:
//
//   - Depth comes from *layered surfaces* (a translucent sidebar over the
//     desktop, a less translucent content pane inset above it), not from
//     borders. Both let the window's vibrancy through; the pane lets through
//     much less, because that is where the text is.
//   - Grouping comes from *inset grouped lists* -- one rounded container with
//     hairline separators between rows -- not from a card per item.
//   - Separators are hairlines at low alpha, used sparingly. A border around
//     everything is a web habit.
//   - Text uses a real type scale with semantic roles, not one size with
//     ad-hoc overrides.
//
// What carries over from the console is the brand: the same green accent, and
// monospace for anything that is literally a command, path, or id.

/** The accent. Unchanged from the console -- this is the one thing that should
 *  make the two surfaces recognisably the same product. */
export const ACCENT = "var(--accent)";
export const ACCENT_DEEP = "var(--accent-deep)";
/** Text drawn on top of a filled accent surface. */
export const ON_ACCENT = "var(--on-accent)";

/** Hover state of a filled accent surface. */
export const ACCENT_HOVER = "var(--accent-hover)";

/** The travelling dot on a switch: white on both themes, because it rides on the
 *  accent in one state and on a grey track in the other. */
export const KNOB = "var(--knob)";

/**
 * Elevation and the scrim, as whole shadow values rather than colours.
 *
 * A dark theme's shadow is near-black at high alpha; a light theme's is much
 * softer, and the same value reads as dirt. Keeping the entire shadow in the
 * palette means a call site never has to know which theme it is in.
 */
export const SHADOW = {
  overlay: "var(--shadow-overlay)",
  sheet: "var(--shadow-sheet)",
  /** The top inner highlight that makes a control look raised. */
  raised: "var(--shadow-raised)",
  /** A grouped list or panel. `none` on dark, a hairline plus a faint lift on
   *  light, where a surface alone does not separate a card from its pane. */
  card: "var(--shadow-card)",
  knob: "var(--shadow-knob)",
  /** Behind a modal. */
  scrim: "var(--scrim)",
} as const;

/**
 * Categorical hues, for container roles and canvas node kinds.
 *
 * These identify rather than rank, so they must stay distinguishable from each
 * other and legible on the surface behind them. Light is not dark at a different
 * opacity: the dark set is all light colours, and yellow in particular vanishes
 * on white, so the two sets are defined independently in `styles.css`.
 */
export const HUE = {
  blue: "var(--hue-blue)",
  blueSoft: "var(--hue-blue-soft)",
  purple: "var(--hue-purple)",
  violet: "var(--hue-violet)",
  orange: "var(--hue-orange)",
  orangeSoft: "var(--hue-orange-soft)",
  cyan: "var(--hue-cyan)",
  teal: "var(--hue-teal)",
  yellow: "var(--hue-yellow)",
  red: "var(--hue-red)",
  grey: "var(--hue-grey)",
  greyDim: "var(--hue-grey-dim)",
} as const;

/**
 * Surfaces, back to front.
 *
 * `sidebar` is intentionally transparent: on macOS the window is given real
 * vibrancy and the desktop shows through, so painting a colour here would defeat
 * it. `sidebarFallback` is what platforms without vibrancy get instead.
 */
export const S = {
  /** Behind everything; only visible where vibrancy is unavailable. */
  window: "var(--s-window)",
  sidebar: "var(--s-sidebar)",
  /** What the content pane and its toolbar paint: `content` with the window's
   *  vibrancy allowed through. `content` itself stays opaque -- the theme
   *  swatch needs a real colour, and a platform without vibrancy falls back to
   *  it. */
  contentFill: "var(--s-content-fill)",
  sidebarFallback: "var(--s-sidebar-fallback)",
  /** The inset pane holding the current view. Opaque, so text on it stays
   *  readable regardless of what is behind the window. */
  content: "var(--s-content)",
  /** A grouped list or panel sitting on `content`. A plain colour, because the
   *  canvas uses it as an SVG fill. */
  raised: "var(--s-raised)",
  /** A panel that floats over arbitrary content: a sheet, a row menu. Nearly
   *  opaque, with `cardBackdrop` under it. See the note in `styles.css`. */
  sheetFill: "var(--sheet-fill)",
  /** What a card actually paints: may be a gradient, and on light themes is one.
   *  Use this for a panel's background and `raised` only where a flat colour is
   *  required. */
  cardFill: "var(--card-fill)",
  /** The filter a card runs over what is behind it. See `--card-backdrop`. */
  cardBackdrop: "var(--card-backdrop)",
  /** A row inside a grouped list, on hover. */
  hover: "var(--s-hover)",
  /** A selected row. */
  selected: "var(--s-selected)",
  /** Recessed wells: transcripts, code, command previews. */
  well: "var(--s-well)",
  /** A text input's own surface. */
  field: "var(--s-field)",
  /** Overlays (palette, sheets) float above everything. */
  overlay: "var(--s-overlay)",
  /** A filled control on a raised surface, and its hover. Named because the
   *  literal it replaced -- a translucent white -- is invisible on a white
   *  surface, so every inline use of it was a light-mode bug. */
  control: "var(--s-control)",
  controlHover: "var(--s-control-hover)",
  /** A barely-there fill: pills, inline chips. */
  subtle: "var(--s-subtle)",
  /** Alternating row wash. */
  stripe: "var(--s-stripe)",
} as const;

/**
 * Text, in macOS's four levels of emphasis. Using a named level instead of
 * picking a grey per component is what keeps hierarchy consistent across
 * sixteen screens.
 */
export const T = {
  primary: "var(--t-primary)",
  secondary: "var(--t-secondary)",
  tertiary: "var(--t-tertiary)",
  quaternary: "var(--t-quaternary)",
  accent: ACCENT,
} as const;

/** Hairlines. `separator` is for inside a grouped list; `border` outlines a
 *  surface; `strong` is for a control that must read as interactive. */
export const LINE = {
  separator: "var(--line-separator)",
  border: "var(--line-border)",
  strong: "var(--line-strong)",
} as const;

/** Semantic status colours, tuned to sit on a dark surface without vibrating. */
export const STATUS = {
  ok: "var(--status-ok)",
  warn: "var(--status-warn)",
  danger: "var(--status-danger)",
  info: "var(--status-info)",
  neutral: "var(--status-neutral)",
} as const;

/**
 * Type scale, named after macOS's roles rather than by pixel size, so a call
 * site says what a piece of text *is*.
 */
export const F = {
  largeTitle: { fontSize: 22, fontWeight: 700, letterSpacing: -0.4 },
  title: { fontSize: 17, fontWeight: 600, letterSpacing: -0.3 },
  headline: { fontSize: 13, fontWeight: 600, letterSpacing: -0.08 },
  body: { fontSize: 13, fontWeight: 400, letterSpacing: -0.08 },
  callout: { fontSize: 12, fontWeight: 400, letterSpacing: -0.05 },
  /** Section headers above grouped lists: small, uppercase, wide-tracked. */
  section: {
    fontSize: 11,
    fontWeight: 600,
    letterSpacing: 0.5,
    textTransform: "uppercase" as const,
  },
  caption: { fontSize: 11, fontWeight: 400, letterSpacing: 0 },
  footnote: { fontSize: 10, fontWeight: 400, letterSpacing: 0.1 },
} as const;

/** SF on Apple platforms, the platform default elsewhere. Never a webfont: a
 *  downloaded font is the other classic tell that an app is a web page. */
export const FONT = {
  ui: '-apple-system, BlinkMacSystemFont, "Segoe UI Variable Text", "Segoe UI", system-ui, sans-serif',
  mono: 'ui-monospace, "SF Mono", SFMono-Regular, Menlo, "Cascadia Mono", Consolas, monospace',
} as const;

/** Corner radii. Larger than the web console's, matching the platform's
 *  continuous-corner look. */
export const R = {
  control: 6,
  field: 6,
  group: 10,
  /** Unused since the content pane went square against the sidebar; kept so a
   *  future pane that genuinely floats has a value to reach for. */
  pane: 12,
  sheet: 14,
  pill: 999,
} as const;

/** Chrome metrics. */
export const M = {
  titlebar: 52,
  /**
   * The comfortable measure for a paragraph, and the ONLY width a view may cap.
   *
   * A long line of prose is genuinely hard to read, so text gets a ceiling. A
   * panel does not: capping one leaves dead space to its right that grows with
   * the window, which is what the Settings page did at 760 -- a third of a wide
   * window empty beside a form that could have used it.
   *
   * So the rule is: cap the measure of TEXT, let panels fill. `layout.test.ts`
   * enforces it by rejecting any other large `maxWidth` in a view.
   */
  prose: 720,
  // Sized to its widest label plus its icon, not to a round number. 218 was
  // most of a Finder window's sidebar for a rail of seven short words, and the
  // pane beside it is where the work is.
  sidebar: 186,
  /** The rail, collapsed to icons.
   *
   *  Not narrower, and the number is not arbitrary: on macOS the OS draws the
   *  traffic lights over our content at the window's top-left, and they end at
   *  `trafficLights`. A rail thinner than that would hand the content pane a
   *  top-left corner it cannot put anything in. Collapsed, the rail ends exactly
   *  where the lights do. */
  sidebarCollapsed: 78,
  /** Space reserved for the macOS traffic lights. */
  trafficLights: 78,
  rowHeight: 30,
} as const;

// --- domain colour ---------------------------------------------------------

/** One hue per workload kind, shared by the resource monitor and the canvas, so
 *  a runner is the same colour wherever you meet it. */
export const ROLE_COLOR: Record<string, string> = {
  runner: ACCENT,
  api: "var(--hue-blue)",
  worker: "var(--hue-purple)",
  dispatcher: "var(--hue-orange)",
  postgres: "var(--hue-cyan)",
  valkey: "var(--hue-red)",
  langfuse: "var(--hue-orange)",
  "langfuse-web": "var(--hue-orange)",
  "langfuse-worker": "var(--hue-orange-soft)",
  clickhouse: "var(--hue-yellow)",
  objectstore: "var(--hue-teal)",
  rustfs: "var(--hue-teal)",
  otel: "var(--hue-grey)",
  "otel-collector": "var(--hue-grey)",
  model: "var(--hue-violet)",
  ui: "var(--hue-blue-soft)",
  /** One-shot init/migrate containers. Normally exited, and not part of the
   *  running topology, so they are dimmer than a live service. */
  job: "var(--hue-grey-dim)",
  other: "var(--hue-grey)",
};

export function roleColor(role: string): string {
  return ROLE_COLOR[role] ?? ROLE_COLOR.other;
}

/** Canvas node kinds: what you author, what it talks to, what carries it. */
export const KIND_COLOR = {
  agent: ACCENT,
  channel: "var(--hue-orange)",
  model: "var(--hue-violet)",
  mcp: "var(--hue-cyan)",
  infra: "var(--hue-grey)",
  repo: "var(--hue-purple)",
  eval: "var(--hue-yellow)",
  approval: "var(--hue-orange)",
} as const;

export type NodeKind = keyof typeof KIND_COLOR;

/** Mix a colour with the surface behind it, for tinted backgrounds that stay
 *  flat rather than glowing. Alpha is expressed as a two-digit hex suffix,
 *  which every colour above supports because they are all 6-digit hex or rgba. */
/**
 * A categorical colour, pulled far enough toward the text colour to be read as
 * text on a tint of itself.
 *
 * The hue tokens carry one value doing two jobs: a *fill* (a chart stroke, a
 * dot, a badge background) wants saturation, and *text* wants contrast against
 * the surface. Those pull in opposite directions, and in each theme it is the
 * text job that loses -- a saturated blue is unreadable on dark, and a blue dark
 * enough to read on white is mud as a bar.
 *
 * Mixing toward `--t-primary` resolves it without a second token per hue,
 * because the direction is whatever the theme's ink is: on dark that lightens
 * the colour, on light it darkens it. So the palette keeps one vivid value per
 * hue and the ink is derived where it is used.
 */
export function readable(color: string): string {
  return `color-mix(in srgb, ${color} 68%, var(--t-primary))`;
}

/**
 * The ramp the content pane and its toolbar paint at the sidebar seam.
 *
 * Eased, not linear, and that is the whole point. A linear ramp is smooth in
 * VALUE -- measured across the seam the largest step is 2/255, far under
 * anything the eye can resolve -- but its SLOPE jumps from zero to constant on
 * one pixel, and a first-derivative discontinuity is exactly what triggers Mach
 * banding: lateral inhibition in the visual system exaggerates the junction and
 * paints a band that is not in the signal. The line people see there is real
 * perception of a real geometric fact, not a rendering artifact.
 *
 * The stops approximate smoothstep (t^2 * (3 - 2t)), so the ramp leaves zero and
 * arrives at full strength with the slope near zero at both ends. Over the first
 * 5px it rises about an eighth as fast as the linear version did.
 *
 * Shared because the toolbar has to paint the identical ramp from the identical
 * origin -- it is a child of the pane at the same left edge, and any divergence
 * puts a corner back at the top of the seam.
 */
export const PANE_FADE = paneFadeTo("var(--s-content-fill)");

/**
 * Any surface that reaches the sidebar seam, ramped in the same way.
 *
 * Every full-width band inside the content pane needs this, not just the pane
 * itself. The console at the foot painted a flat `S.well` starting on the exact
 * seam pixel and undid the pane's ramp one row lower -- the eye reads the whole
 * left edge at once, so one hard band is enough to make the join look wrong
 * again. The prompt row inside it had the same problem with an opaque field
 * fill.
 */
export function paneFadeTo(fill: string): string {
  return [
    "linear-gradient(90deg",
    "transparent 0",
    `${tintOf(fill, 0.014)} 5px`,
    `${tintOf(fill, 0.156)} 10px`,
    `${tintOf(fill, 0.5)} 20px`,
    `${tintOf(fill, 0.844)} 30px`,
    `${tintOf(fill, 0.986)} 35px`,
    `${fill} 40px)`,
  ].join(", ");
}

/** `tint` by another name, declared before the const above can use it. */
function tintOf(color: string, alpha: number): string {
  const pct = Math.round(Math.max(0, Math.min(1, alpha)) * 100);
  return `color-mix(in srgb, ${color} ${pct}%, transparent)`;
}

export function tint(color: string, alpha: number): string {
  // `color-mix` rather than appending a hex alpha, because every colour is now a
  // `var(--x)` and you cannot concatenate an alpha onto a variable reference.
  // This also works for the rgba() literals the old implementation had to skip.
  const pct = Math.round(Math.max(0, Math.min(1, alpha)) * 100);
  return `color-mix(in srgb, ${color} ${pct}%, transparent)`;
}
