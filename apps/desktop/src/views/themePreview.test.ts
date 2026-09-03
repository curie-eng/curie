// Two themes must not preview identically.
//
// The first preview drew a sidebar, a card and a command line: about fifteen of
// the fifty-four colours a theme defines. Themes that agreed on those and
// differed in their status or node colours rendered the same, so the panel
// answered "are these different" with a confident and wrong no.
//
// This reads the palettes the generator emitted and compares them across only
// the variables the preview actually puts on screen. If two themes collapse to
// the same set, the preview is not showing enough to tell them apart, and the
// fix is to show more rather than to relax the test.

import { describe, expect, it } from "vitest";

import CSS from "../generated/themes.css?raw";
import { THEMES } from "../../electron/shared/themes";
import { PREVIEW_VARS } from "./ThemePreview";

/** Every theme's variables, by id. */
function palettes(): Map<string, Map<string, string>> {
  const out = new Map<string, Map<string, string>>();
  const re = /:root\[data-theme="([^"]+)"\],\s*\[data-theme-preview="[^"]+"\]\s*\{([^}]*)\}/g;
  for (let m = re.exec(CSS); m; m = re.exec(CSS)) {
    const vars = new Map<string, string>();
    for (const line of m[2].split("\n")) {
      const kv = line.match(/^\s*(--[\w-]+)\s*:\s*(.+?);\s*$/);
      if (kv) vars.set(kv[1], kv[2].trim());
    }
    out.set(m[1], vars);
  }
  return out;
}

const ALL = palettes();

describe("the theme preview", () => {
  it("shows a variable that exists in every theme", () => {
    // A name that no palette defines would render as nothing and silently
    // weaken the comparison below.
    for (const [id, vars] of ALL) {
      for (const name of PREVIEW_VARS) {
        expect(vars.has(name), `${id} has no ${name}`).toBe(true);
      }
    }
  });

  it("renders every pair of themes differently", () => {
    const fingerprint = (id: string) =>
      PREVIEW_VARS.map((v) => ALL.get(id)?.get(v) ?? "?").join("|");

    const seen = new Map<string, string>();
    const collisions: string[] = [];
    for (const t of THEMES) {
      if (!ALL.has(t.id)) continue;
      const fp = fingerprint(t.id);
      const first = seen.get(fp);
      if (first) collisions.push(`${first} and ${t.id} preview identically`);
      else seen.set(fp, t.id);
    }
    expect(
      collisions,
      "Two themes draw the same preview, so the panel cannot tell them apart. " +
        "Show another surface in ThemePreview and add its variable to PREVIEW_VARS:",
    ).toEqual([]);
  });

  it("distinguishes the pairs that are meant to be close", () => {
    // These share a family and a background. If the preview cannot separate
    // them it is not doing its job, whatever the pairwise check says.
    const close: [string, string][] = [
      ["monokai", "monokai-dimmed"],
      ["dark-plus", "dark-modern"],
      ["light-plus", "light-modern"],
    ];
    for (const [a, b] of close) {
      if (!ALL.has(a) || !ALL.has(b)) continue;
      const differing = PREVIEW_VARS.filter((v) => ALL.get(a)?.get(v) !== ALL.get(b)?.get(v));
      expect(differing.length, `${a} and ${b} differ in nothing the preview shows`).toBeGreaterThan(0);
    }
  });
});
