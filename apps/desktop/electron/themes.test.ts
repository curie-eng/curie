// The theme registry and the generated stylesheet must agree.
//
// Switching themes only replaces the variables the new block declares, so a block
// that omits one leaves the PREVIOUS theme's value in place. That failure is
// invisible until someone switches from Monokai to Abyss and one colour stays
// green, which is exactly the kind of thing a screenshot of either theme on its
// own cannot show. So this asserts the property directly: every theme in the
// registry has a block, and every block is complete.

import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { describe, expect, it } from "vitest";

import { THEMES, THEME_IDS, themeInfo } from "./shared/themes";

const HERE = dirname(new URL(import.meta.url).pathname);
const CSS = readFileSync(resolve(HERE, "..", "src", "generated", "themes.css"), "utf8");
const BASE = readFileSync(resolve(HERE, "..", "src", "styles.css"), "utf8");

function blocks(css: string): Map<string, Set<string>> {
  const out = new Map<string, Set<string>>();
  // Each block is a selector LIST: the root form a theme is worn with, and the
  // `[data-theme-preview]` form that scopes the same palette to a subtree so
  // settings can show a theme without the window putting it on.
  const re = /:root\[data-theme="([^"]+)"\],\s*\[data-theme-preview="[^"]+"\]\s*\{([^}]*)\}/g;
  for (let m = re.exec(css); m; m = re.exec(css)) {
    const vars = new Set<string>();
    for (const line of m[2].split("\n")) {
      const v = line.match(/^\s*(--[\w-]+)\s*:/);
      if (v) vars.add(v[1]);
    }
    out.set(m[1], vars);
  }
  return out;
}

/** The `:root` block is the first-paint default and defines the full set. */
function rootVars(): Set<string> {
  const start = BASE.indexOf(":root {");
  const body = BASE.slice(start, BASE.indexOf("}", start));
  const vars = new Set<string>();
  for (const line of body.split("\n")) {
    const v = line.match(/^\s*(--[\w-]+)\s*:/);
    if (v) vars.add(v[1]);
  }
  return vars;
}

const parsed = blocks(CSS);
const expected = rootVars();

describe("the generated themes", () => {
  it("ships a non-trivial set", () => {
    expect(THEMES.length).toBeGreaterThanOrEqual(15);
    expect(expected.size).toBeGreaterThanOrEqual(40);
  });

  it("gives every theme a preview selector as well as a root one", () => {
    // The preview panel scopes a palette to a subtree. If the generator ever
    // emitted only the root selector again, the preview would silently inherit
    // the window's theme and show every option looking identical.
    for (const t of THEMES) {
      expect(CSS, `no preview selector for ${t.id}`).toContain(`[data-theme-preview="${t.id}"]`);
    }
  });

  it("has a stylesheet block for every registered theme", () => {
    const missing = THEME_IDS.filter((id) => !parsed.has(id));
    expect(missing, "registered but not in themes.css").toEqual([]);
  });

  it("registers every stylesheet block", () => {
    const orphans = [...parsed.keys()].filter((id) => !THEME_IDS.includes(id as never));
    expect(orphans, "in themes.css but not registered").toEqual([]);
  });

  it("declares the COMPLETE variable set in every block", () => {
    // The load-bearing one. A partial block inherits the outgoing theme.
    const gaps = [...parsed.entries()]
      .map(([id, vars]) => ({ id, missing: [...expected].filter((v) => !vars.has(v)) }))
      .filter((r) => r.missing.length);
    expect(gaps).toEqual([]);
  });

  it("keeps the two base themes byte-identical to the hand-tuned palettes", () => {
    // `dark` and `light` are generated FROM styles.css, so a drift here means the
    // generator mangled a value a human chose.
    const darkBlock = CSS.slice(CSS.indexOf(':root[data-theme="dark"]'));
    expect(darkBlock).toContain("--accent: #3ecf8e;");
    const lightStart = CSS.indexOf(':root[data-theme="light"]');
    expect(CSS.slice(lightStart, CSS.indexOf("}", lightStart))).toContain("--accent: #0f8f62;");
  });

  it("gives every theme a light or dark appearance, and both kinds exist", () => {
    // The shell maps this onto the native window; an unknown value would leave a
    // light theme in a dark frame.
    expect(THEMES.every((t) => t.appearance === "light" || t.appearance === "dark")).toBe(true);
    expect(THEMES.some((t) => t.appearance === "light")).toBe(true);
    expect(THEMES.some((t) => t.appearance === "dark")).toBe(true);
  });

  it("declares color-scheme per block, so form controls and scrollbars follow", () => {
    for (const t of THEMES) {
      const i = CSS.indexOf(`:root[data-theme="${t.id}"]`);
      expect(CSS.slice(i, CSS.indexOf("}", i)), t.id).toContain(`color-scheme: ${t.appearance}`);
    }
  });

  it("gives every theme a three-colour swatch of real colours", () => {
    for (const t of THEMES) {
      expect(t.swatch, t.id).toHaveLength(3);
      for (const c of t.swatch) expect(c, `${t.id} swatch`).toMatch(/^(#[0-9a-f]{6}|rgba?\()/i);
    }
  });

  it("has unique ids and labels", () => {
    expect(new Set(THEME_IDS).size).toBe(THEMES.length);
    expect(new Set(THEMES.map((t) => t.label)).size).toBe(THEMES.length);
  });

  it("resolves a known id and refuses an unknown one", () => {
    expect(themeInfo("monokai")?.label).toBe("Monokai");
    expect(themeInfo("no-such-theme")).toBeUndefined();
  });
});
