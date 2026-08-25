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
//     desktop, an opaque content pane inset above it), not from borders.
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
export const ACCENT = "#3ecf8e";
export const ACCENT_DEEP = "#2bb377";
/** Text drawn on top of a filled accent surface. */
export const ON_ACCENT = "#062015";

/**
 * Surfaces, back to front.
 *
 * `sidebar` is intentionally transparent: on macOS the window is given real
 * vibrancy and the desktop shows through, so painting a colour here would defeat
 * it. `sidebarFallback` is what platforms without vibrancy get instead.
 */
export const S = {
  /** Behind everything; only visible where vibrancy is unavailable. */
  window: "#1c1c1e",
  sidebar: "transparent",
  sidebarFallback: "#1f1f21",
  /** The inset pane holding the current view. Opaque, so text on it stays
   *  readable regardless of what is behind the window. */
  content: "#242426",
  /** A grouped list or panel sitting on `content`. */
  raised: "#2c2c2e",
  /** A row inside a grouped list, on hover. */
  hover: "#333336",
  /** A selected row. */
  selected: "#3a3a3d",
  /** Recessed wells: transcripts, code, command previews. */
  well: "#1a1a1c",
  /** Overlays (palette, sheets) float above everything. */
  overlay: "#2e2e31",
} as const;

/**
 * Text, in macOS's four levels of emphasis. Using a named level instead of
 * picking a grey per component is what keeps hierarchy consistent across
 * sixteen screens.
 */
export const T = {
  primary: "rgba(255,255,255,0.92)",
  secondary: "rgba(235,235,245,0.62)",
  tertiary: "rgba(235,235,245,0.38)",
  quaternary: "rgba(235,235,245,0.22)",
  accent: ACCENT,
} as const;

/** Hairlines. `separator` is for inside a grouped list; `border` outlines a
 *  surface; `strong` is for a control that must read as interactive. */
export const LINE = {
  separator: "rgba(255,255,255,0.07)",
  border: "rgba(255,255,255,0.10)",
  strong: "rgba(255,255,255,0.16)",
} as const;

/** Semantic status colours, tuned to sit on a dark surface without vibrating. */
export const STATUS = {
  ok: "#32d74b",
  warn: "#ffd426",
  danger: "#ff453a",
  info: "#0a84ff",
  neutral: "rgba(235,235,245,0.38)",
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
  pane: 12,
  sheet: 14,
  pill: 999,
} as const;

/** Chrome metrics. */
export const M = {
  titlebar: 52,
  sidebar: 218,
  /** Space reserved for the macOS traffic lights. */
  trafficLights: 78,
  rowHeight: 30,
} as const;

// --- domain colour ---------------------------------------------------------

/** One hue per workload kind, shared by the resource monitor and the canvas, so
 *  a runner is the same colour wherever you meet it. */
export const ROLE_COLOR: Record<string, string> = {
  runner: ACCENT,
  api: "#0a84ff",
  worker: "#bf5af2",
  dispatcher: "#ff9f0a",
  postgres: "#64d2ff",
  valkey: "#ff6961",
  langfuse: "#ff9f0a",
  "langfuse-web": "#ff9f0a",
  "langfuse-worker": "#ffb340",
  clickhouse: "#ffd426",
  objectstore: "#66d4cf",
  rustfs: "#66d4cf",
  otel: "#98989d",
  "otel-collector": "#98989d",
  model: "#da8fff",
  ui: "#5e9eff",
  /** One-shot init/migrate containers. Normally exited, and not part of the
   *  running topology, so they are dimmer than a live service. */
  job: "#8e8e93",
  other: "#98989d",
};

export function roleColor(role: string): string {
  return ROLE_COLOR[role] ?? ROLE_COLOR.other;
}

/** Canvas node kinds: what you author, what it talks to, what carries it. */
export const KIND_COLOR = {
  agent: ACCENT,
  channel: "#ff9f0a",
  model: "#da8fff",
  mcp: "#64d2ff",
  infra: "#98989d",
  repo: "#bf5af2",
  eval: "#ffd426",
  approval: "#ff9f0a",
} as const;

export type NodeKind = keyof typeof KIND_COLOR;

/** Mix a colour with the surface behind it, for tinted backgrounds that stay
 *  flat rather than glowing. Alpha is expressed as a two-digit hex suffix,
 *  which every colour above supports because they are all 6-digit hex or rgba. */
export function tint(color: string, alpha: number): string {
  if (color.startsWith("rgba")) return color;
  const hex = Math.round(Math.max(0, Math.min(1, alpha)) * 255)
    .toString(16)
    .padStart(2, "0");
  return `${color}${hex}`;
}
