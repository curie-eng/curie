// The words this app is allowed to say.
//
// The point of a window over a CLI is that you do not have to know the CLI. So
// the copy an operator reads while deciding what to build must not be the CLI's
// own vocabulary -- "scaffold a bundle", "one runner container", "the skill
// tier", "push it at a platform". Those are true and they are also the thing
// this app exists to save somebody from.
//
// Command names have not been hidden, they have been MOVED: they live in each
// control's tooltip (`curie local up — ...`), in the groups explicitly titled as
// commands, in the Commands reference, and in the console. Those are the places
// somebody has gone looking for them.

import { describe, expect, it } from "vitest";

import { SURFACES } from "./surfaces";
import { TEMPLATES } from "./templates";

/** Words that name a mechanism the operator did not ask about. */
const JARGON =
  /\b(bundle|scaffold|runner|container|tier|compose|helm|kubernetes|kubectl|CLI|binary|manifest|MCP|plugin|stdout|stderr|argv|PATH)\b/i;

/** Surfaces whose whole purpose IS the command-shaped path, and the two that
 *  are about this machine's own plumbing. Named individually so a NEW surface
 *  cannot join them by accident. */
const ALLOWED = new Set(["build.author", "settings.machine", "settings.dev", "settings.reference"]);

describe("surface copy", () => {
  for (const s of SURFACES) {
    if (ALLOWED.has(s.id)) continue;
    it(`${s.id} says what it does, not how it is built`, () => {
      const hit = JARGON.exec(`${s.title} ${s.blurb}`);
      expect(hit?.[0], `"${s.title}: ${s.blurb}"`).toBeUndefined();
    });
  }

  it("never names a command in a title or blurb", () => {
    for (const s of SURFACES) {
      expect(`${s.title} ${s.blurb}`, s.id).not.toMatch(/\bcurie [a-z]/);
    }
  });
});

describe("template copy", () => {
  // The gallery is the front door. Somebody reading it has not decided to build
  // anything yet, so it is the last place that may assume the vocabulary.
  for (const t of TEMPLATES) {
    it(`${t.id} reads as what it does`, () => {
      const text = `${t.name} ${t.tagline} ${t.about}`;
      expect(JARGON.exec(text)?.[0], text).toBeUndefined();
      expect(text).not.toMatch(/\bcurie [a-z]/);
    });
  }
});
