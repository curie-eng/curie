// Two themes must not preview alike.
//
// Not "must not be byte-identical": near-identical colours are what the eye
// actually sees as the same, and comparing strings would call #272727 and
// #282828 different while a person calls them the same colour. So this measures
// how far apart two palettes look, across the surfaces the preview shows.
//
// It counts how many surfaces differ rather than taking the largest single
// difference. One tell is not enough in practice: Light+ and Light Modern
// differ only in their accent, and a theme picker where two entries are the
// same but for one small button is the complaint this test exists to catch.
//
// A failure here has two honest fixes: show another surface in the preview and
// list its variable in PREVIEW_VARS, or give the theme its own `chrome` in
// scripts/gen-themes.mjs so it stops deriving a sidebar it does not have.

import { describe, expect, it } from "vitest";

import { THEMES } from "../../electron/shared/themes";
import { PALETTES } from "../generated/themePalettes";
import { PREVIEW_VARS } from "./previewVars";

/** Roughly a just-noticeable difference on a flat area of colour. Not a precise
 *  figure; the point is to reject "same to the eye", not to grade. */
const NOTICEABLE = 14;

/** How many surfaces have to differ before two themes read as different. */
const TELLS = 2;

type RGBA = readonly [number, number, number, number];

/** Hex, `rgb()`/`rgba()`, or a gradient, which is what the palettes contain. An
 *  unreadable value returns null rather than a guess, and the first test
 *  reports it instead of letting it pass as "no difference". */
function parse(value: string): RGBA | null {
  const text = value.trim();
  const hex = text.match(/^#([0-9a-f]{6})$/i) ?? text.match(/#([0-9a-f]{6})/i);
  const fn = text.match(/rgba?\(\s*([\d.]+)[\s,]+([\d.]+)[\s,]+([\d.]+)(?:[\s,/]+([\d.]+))?/i);
  // Whichever comes first in the string is the colour this surface leads with.
  const at = (m: RegExpMatchArray | null) => (m?.index ?? Infinity);
  if (hex && at(hex) <= at(fn)) {
    const n = parseInt(hex[1], 16);
    return [(n >> 16) & 255, (n >> 8) & 255, n & 255, 1];
  }
  if (fn) return [Number(fn[1]), Number(fn[2]), Number(fn[3]), fn[4] === undefined ? 1 : Number(fn[4])];
  return null;
}

/** What the eye receives, not what the variable says. Half this palette is
 *  translucent, and a border at 0.6 black and one at 0.15 black are the same
 *  three numbers until they are laid over the pane behind them. High contrast
 *  themes differ from their plain counterparts almost entirely this way. */
function flatten(c: RGBA, under: RGBA): readonly [number, number, number] {
  const a = c[3];
  return [c[0] * a + under[0] * (1 - a), c[1] * a + under[1] * (1 - a), c[2] * a + under[2] * (1 - a)];
}

/** Weighted so green counts most and blue least, which is roughly how the eye
 *  weighs them. Plain Euclidean RGB calls two blues further apart than they
 *  look and two greens closer. */
function distance(a: readonly [number, number, number], b: readonly [number, number, number]): number {
  const dr = a[0] - b[0];
  const dg = a[1] - b[1];
  const db = a[2] - b[2];
  return Math.sqrt(2 * dr * dr + 4 * dg * dg + 3 * db * db) / 3;
}

const ALL = new Map(Object.entries(PALETTES).map(([id, vars]) => [id, new Map(Object.entries(vars))]));

/** One preview surface as it is actually drawn: over the content pane, since
 *  that is what the preview sits on. */
function surface(theme: string, name: string): readonly [number, number, number] | null {
  const raw = ALL.get(theme)?.get(name);
  const under = parse(ALL.get(theme)?.get("--s-content") ?? "") ?? [0, 0, 0, 1];
  const c = raw ? parse(raw) : null;
  return c ? flatten(c, under) : null;
}

/** The preview surfaces on which two themes differ enough to notice. */
function tells(a: string, b: string): string[] {
  return PREVIEW_VARS.filter((name) => {
    const x = surface(a, name);
    const y = surface(b, name);
    return x !== null && y !== null && distance(x, y) >= NOTICEABLE;
  });
}

describe("the theme preview", () => {
  // Without this the rest of the file passes whatever happens: an empty palette
  // map means no pairs to compare and no variables to check, and every
  // expectation below holds vacuously. That is exactly how this file was first
  // written, and it reported success while measuring nothing.
  it("reads the generated palettes", () => {
    expect(ALL.size, "no palettes to compare").toBe(THEMES.length);
    for (const t of THEMES) expect(ALL.has(t.id), `${t.id} has no palette`).toBe(true);
  });

  it("shows a variable every theme defines, in a form this can read", () => {
    for (const [id, vars] of ALL) {
      for (const name of PREVIEW_VARS) {
        expect(vars.has(name), `${id} has no ${name}`).toBe(true);
        expect(parse(vars.get(name)!), `${id} ${name} is not a colour this can compare`).not.toBeNull();
      }
    }
  });

  it("draws every pair of themes differently enough to tell apart", () => {
    const ids = THEMES.map((t) => t.id).filter((id) => ALL.has(id));
    const tooClose: string[] = [];
    for (let i = 0; i < ids.length; i++) {
      for (let j = i + 1; j < ids.length; j++) {
        const found = tells(ids[i], ids[j]);
        if (found.length < TELLS) {
          tooClose.push(`${ids[i]} vs ${ids[j]} (${found.length}: ${found.join(", ") || "nothing"})`);
        }
      }
    }
    expect(
      tooClose,
      `These pairs preview with fewer than ${TELLS} surfaces a person could tell apart. ` +
        "Either show another surface in ThemePreview and list its variable in PREVIEW_VARS, " +
        "or give the theme its own `chrome` in scripts/gen-themes.mjs:",
    ).toEqual([]);
  });
});
