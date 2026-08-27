// Generates every theme's palette from a handful of anchor colours.
//
// Seventeen themes times fifty variables is eight hundred and fifty values. Hand
// written they would be inconsistent within a week and unreviewable from the
// first commit: nobody can tell whether the tertiary text in Kimbie Dark is the
// same relative step as the tertiary text in Abyss by reading two hex codes.
//
// So each theme declares only what makes it that theme -- an editor background, a
// foreground, an accent, and any signature hues -- and everything else is DERIVED
// by the same rules for every theme. Surfaces step away from the background by
// fixed amounts, text sits at fixed alphas of the foreground, hairlines at fixed
// alphas. That is what makes the set feel like one system rather than fifteen
// downloads.
//
// The two hand-tuned Curie palettes are the bases, read straight out of
// `styles.css`, so anything a theme does not override falls back to a value a
// human chose. Status colours, shadows and the categorical hues come from there
// unless a theme has an opinion.
//
// The palettes are keyed to each scheme's editor background/foreground/accent as
// published in the MIT-licensed VS Code built-in themes. They are not ports of
// the syntax token sets -- this app has no syntax to highlight.

import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");

// --- colour maths ------------------------------------------------------------

function parse(hex) {
  const h = hex.replace("#", "").trim();
  const n = h.length === 3 ? h.split("").map((c) => c + c).join("") : h.slice(0, 6);
  return [0, 2, 4].map((i) => parseInt(n.slice(i, i + 2), 16));
}
const clamp = (n) => Math.max(0, Math.min(255, Math.round(n)));
const hex = (rgb) => "#" + rgb.map((c) => clamp(c).toString(16).padStart(2, "0")).join("");
/** `amount` of `b` mixed into `a`. */
const mix = (a, b, amount) => {
  const [x, y] = [parse(a), parse(b)];
  return hex(x.map((c, i) => c + (y[i] - c) * amount));
};
const alpha = (color, a) => {
  const [r, g, b] = parse(color);
  return `rgba(${r}, ${g}, ${b}, ${a})`;
};
/** Perceived luminance, for deciding what reads on top of a fill. */
const luma = (color) => {
  const [r, g, b] = parse(color).map((c) => c / 255);
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
};
const onTop = (fill) => (luma(fill) > 0.55 ? "#0b1410" : "#ffffff");

// --- the two hand-tuned bases, read from styles.css --------------------------

const css = readFileSync(join(root, "src", "styles.css"), "utf8");

function baseVars(selector) {
  const start = css.indexOf(selector);
  if (start < 0) throw new Error(`no ${selector} block in styles.css`);
  const end = css.indexOf("\n}", start);
  // Comments come out FIRST, then split. A prose comment can contain a
  // semicolon -- one of them does -- and splitting first severs the declaration
  // that follows it from its own name, which drops the variable silently.
  const body = css.slice(start + selector.length, end).replace(/\/\*[\s\S]*?\*\//g, "");
  const out = {};
  // Split on declarations rather than lines: a gradient or a layered shadow is
  // written across several lines, and a line-based reader silently drops it.
  for (const decl of body.split(";")) {
    const clean = decl.trim();
    const i = clean.indexOf(":");
    if (i < 0 || !clean.startsWith("--")) continue;
    out[clean.slice(0, i).trim()] = clean
      .slice(i + 1)
      .split("\n")
      .map((l) => l.trim())
      .filter(Boolean)
      .join(" ");
  }
  return out;
}

const BASE = {
  dark: baseVars(":root {"),
  light: baseVars(':root[data-theme="light"] {'),
};
for (const [k, v] of Object.entries(BASE)) {
  if (Object.keys(v).length < 40) throw new Error(`${k} base looks truncated: ${Object.keys(v).length} vars`);
}

// --- the themes --------------------------------------------------------------
//
// `bg`/`fg`/`accent` are the anchors. `hues` overrides the categorical set for
// schemes whose palette is the recognisable thing about them. `contrast` swaps
// hairlines for hard borders and drops the shadows.

const THEMES = [
  { id: "dark", label: "Curie Dark", appearance: "dark", base: true },
  { id: "light", label: "Curie Light", appearance: "light", base: true },

  { id: "dark-plus", label: "Dark+", appearance: "dark", bg: "#1e1e1e", fg: "#d4d4d4", accent: "#007acc" },
  { id: "light-plus", label: "Light+", appearance: "light", bg: "#ffffff", fg: "#1e1e1e", accent: "#007acc" },
  { id: "dark-modern", label: "Dark Modern", appearance: "dark", bg: "#1f1f1f", fg: "#cccccc", accent: "#0078d4" },
  { id: "light-modern", label: "Light Modern", appearance: "light", bg: "#ffffff", fg: "#3b3b3b", accent: "#005fb8" },
  {
    id: "hc-dark",
    label: "Dark High Contrast",
    appearance: "dark",
    bg: "#000000",
    fg: "#ffffff",
    accent: "#f38518",
    contrast: true,
  },
  {
    id: "hc-light",
    label: "Light High Contrast",
    appearance: "light",
    bg: "#ffffff",
    fg: "#000000",
    accent: "#0f4a85",
    contrast: true,
  },
  { id: "abyss", label: "Abyss", appearance: "dark", bg: "#000c18", fg: "#6688cc", accent: "#2277ff" },
  { id: "kimbie-dark", label: "Kimbie Dark", appearance: "dark", bg: "#221a0f", fg: "#d3af86", accent: "#a57a4c" },
  {
    id: "monokai",
    label: "Monokai",
    appearance: "dark",
    bg: "#272822",
    fg: "#f8f8f2",
    accent: "#a6e22e",
    hues: {
      "--hue-blue": "#66d9ef",
      "--hue-blue-soft": "#7fd7e8",
      "--hue-purple": "#ae81ff",
      "--hue-violet": "#c9a0ff",
      "--hue-orange": "#fd971f",
      "--hue-orange-soft": "#e6a75c",
      "--hue-cyan": "#66d9ef",
      "--hue-teal": "#a1efe4",
      "--hue-yellow": "#e6db74",
      "--hue-red": "#f92672",
      "--hue-grey": "#a59f85",
      "--hue-grey-dim": "#75715e",
    },
  },
  { id: "monokai-dimmed", label: "Monokai Dimmed", appearance: "dark", bg: "#1e1e1e", fg: "#c5c8c6", accent: "#6a9fb5" },
  { id: "quiet-light", label: "Quiet Light", appearance: "light", bg: "#f5f5f5", fg: "#333333", accent: "#705697" },
  { id: "red", label: "Red", appearance: "dark", bg: "#390000", fg: "#f8f8f8", accent: "#e35353" },
  {
    id: "solarized-dark",
    label: "Solarized Dark",
    appearance: "dark",
    bg: "#002b36",
    fg: "#93a1a1",
    accent: "#268bd2",
    hues: {
      "--hue-blue": "#268bd2",
      "--hue-blue-soft": "#4a9fd8",
      "--hue-purple": "#6c71c4",
      "--hue-violet": "#d33682",
      "--hue-orange": "#cb4b16",
      "--hue-orange-soft": "#c07a4a",
      "--hue-cyan": "#2aa198",
      "--hue-teal": "#2aa198",
      "--hue-yellow": "#b58900",
      "--hue-red": "#dc322f",
      "--hue-grey": "#839496",
      "--hue-grey-dim": "#657b83",
    },
  },
  {
    id: "solarized-light",
    label: "Solarized Light",
    appearance: "light",
    bg: "#fdf6e3",
    fg: "#586e75",
    accent: "#268bd2",
    hues: {
      "--hue-blue": "#1f6f9f",
      "--hue-blue-soft": "#2b7fb0",
      "--hue-purple": "#5b60ab",
      "--hue-violet": "#a62a68",
      "--hue-orange": "#a33c11",
      "--hue-orange-soft": "#93602f",
      "--hue-cyan": "#1f7f78",
      "--hue-teal": "#1f7f78",
      "--hue-yellow": "#8a6800",
      "--hue-red": "#b02724",
      "--hue-grey": "#657b83",
      "--hue-grey-dim": "#839496",
    },
  },
  {
    id: "tomorrow-night-blue",
    label: "Tomorrow Night Blue",
    appearance: "dark",
    bg: "#002451",
    fg: "#ffffff",
    accent: "#bbdaff",
  },
];

// --- derivation --------------------------------------------------------------

function derive(theme) {
  if (theme.base) return { ...BASE[theme.appearance] };
  const base = BASE[theme.appearance];
  const { bg, fg, accent, contrast } = theme;
  const dark = theme.appearance === "dark";
  const vars = { ...base };

  // Surfaces step away from the editor background by fixed amounts, so every
  // theme has the same sense of elevation.
  if (dark) {
    Object.assign(vars, {
      "--s-window": bg,
      "--s-sidebar-fallback": mix(bg, fg, 0.05),
      "--s-content": mix(bg, fg, 0.05),
      // Same colour, vibrancy allowed through. See the note in styles.css.
      "--s-content-fill": alpha(mix(bg, fg, 0.05), 0.6),
      "--s-raised": mix(bg, fg, 0.1),
      "--s-hover": mix(bg, fg, 0.15),
      "--s-selected": mix(bg, fg, 0.21),
      "--s-well": mix(bg, "#000000", 0.35),
      "--s-field": alpha("#000000", 0.24),
      "--s-overlay": mix(bg, fg, 0.12),
      "--s-control": alpha(fg, 0.11),
      "--s-control-hover": alpha(fg, 0.17),
      "--s-subtle": alpha(fg, 0.07),
      "--s-stripe": alpha(fg, 0.02),
      // Flat: a lighter panel on a darker pane already reads as raised, and a
      // gradient here only muddies it.
      "--card-fill": mix(bg, fg, 0.1),
    });
  } else {
    Object.assign(vars, {
      "--s-window": mix(bg, "#000000", 0.11),
      "--s-sidebar-fallback": mix(bg, "#000000", 0.06),
      "--s-content": mix(bg, "#000000", 0.045),
      "--s-content-fill": alpha(mix(bg, "#000000", 0.045), 0.72),
      "--s-raised": bg,
      "--s-hover": mix(bg, "#000000", 0.045),
      "--s-selected": mix(bg, accent, 0.14),
      "--s-well": mix(bg, "#000000", 0.075),
      "--s-field": bg,
      "--s-overlay": bg,
      "--s-control": alpha("#000000", 0.07),
      "--s-control-hover": alpha("#000000", 0.12),
      "--s-subtle": alpha("#000000", 0.05),
      "--s-stripe": alpha("#000000", 0.022),
      // Gradient plus real translucency, so the bottom edge picks up the pane.
      // Flat white with a hairline is what reads as unstyled.
      "--card-fill": `linear-gradient(180deg, ${alpha(bg, 0.98)} 0%, ${alpha(bg, 0.82)} 100%)`,
      "--shadow-card": [
        "inset 0 1px 0 rgba(255, 255, 255, 0.9)",
        `0 0 0 0.5px ${alpha(mix(bg, "#000000", 0.85), 0.09)}`,
        `0 1px 2px ${alpha(mix(bg, "#000000", 0.85), 0.05)}`,
        `0 8px 20px -6px ${alpha(mix(bg, "#000000", 0.85), 0.12)}`,
      ].join(", "),
    });
  }

  // Text is the foreground at four fixed alphas, which is what keeps hierarchy
  // comparable across themes.
  Object.assign(vars, {
    "--t-primary": alpha(fg, dark ? 0.95 : 0.9),
    "--t-secondary": alpha(fg, dark ? 0.7 : 0.74),
    "--t-tertiary": alpha(fg, dark ? 0.48 : 0.56),
    "--t-quaternary": alpha(fg, dark ? 0.3 : 0.4),
  });

  const lineBase = dark ? fg : "#000000";
  Object.assign(vars, {
    "--line-separator": alpha(lineBase, contrast ? 0.4 : dark ? 0.1 : 0.1),
    "--line-border": alpha(lineBase, contrast ? 0.6 : dark ? 0.15 : 0.15),
    "--line-strong": alpha(lineBase, contrast ? 0.9 : dark ? 0.24 : 0.26),
  });

  Object.assign(vars, {
    "--accent": accent,
    "--accent-deep": mix(accent, "#000000", 0.25),
    "--accent-hover": mix(accent, dark ? "#ffffff" : "#000000", 0.15),
    "--on-accent": onTop(accent),
    "--focus-ring": alpha(accent, 0.5),
  });

  // High contrast means a visible edge, not a soft one, and no elevation blur.
  if (contrast) {
    vars["--card-fill"] = dark ? mix(bg, fg, 0.1) : bg;
    vars["--shadow-card"] = `0 0 0 1px ${alpha(lineBase, 0.7)}`;
    vars["--shadow-overlay"] = `0 0 0 1px ${alpha(lineBase, 0.7)}`;
    vars["--shadow-sheet"] = `0 0 0 1px ${alpha(lineBase, 0.7)}`;
  }

  if (theme.hues) Object.assign(vars, theme.hues);
  return vars;
}

// --- emit --------------------------------------------------------------------

const order = Object.keys(BASE.dark);
const blocks = THEMES.map((t) => {
  const vars = derive(t);
  const body = order
    .map((k) => `  ${k}: ${vars[k]};`)
    .join("\n");
  return `:root[data-theme="${t.id}"] {\n  color-scheme: ${t.appearance};\n${body}\n}`;
});

mkdirSync(join(root, "src", "generated"), { recursive: true });
writeFileSync(
  join(root, "src", "generated", "themes.css"),
  `/* GENERATED by scripts/gen-themes.mjs -- do not edit.\n *\n * One block per theme, every block defining the complete variable set. That is\n * load bearing: switching themes only replaces the variables the new block\n * declares, so a partial block would leave the previous theme's values behind\n * for anything it forgot.\n */\n\n${blocks.join("\n\n")}\n`,
);

const registry = THEMES.map((t) => {
  const v = derive(t);
  return {
    id: t.id,
    label: t.label,
    appearance: t.appearance,
    // Three colours the picker draws as a swatch, so a theme can be recognised
    // without being applied.
    swatch: [v["--s-content"], v["--s-raised"], v["--accent"]],
  };
});

writeFileSync(
  join(root, "electron", "shared", "themes.ts"),
  `// GENERATED by scripts/gen-themes.mjs -- do not edit.
//
// Lives beside the contract because BOTH sides need it: the shell maps a theme's
// appearance onto the native window, and the renderer draws the picker from it.

export type ThemeId =\n${registry.map((t) => `  | "${t.id}"`).join("\n")};

export interface ThemeInfo {
  readonly id: ThemeId;
  /** Whether the shell should put the native window in light or dark. */
  readonly appearance: "light" | "dark";
  readonly label: string;
  /** content, raised, accent -- enough to recognise the theme in a picker. */
  readonly swatch: readonly [string, string, string];
}

export const THEMES: readonly ThemeInfo[] = ${JSON.stringify(registry, null, 2)} as const;

export const THEME_IDS: readonly ThemeId[] = THEMES.map((t) => t.id);

export function themeInfo(id: string): ThemeInfo | undefined {
  return THEMES.find((t) => t.id === id);
}
`,
);

console.log(`themes: ${THEMES.length} palettes, ${order.length} variables each`);
