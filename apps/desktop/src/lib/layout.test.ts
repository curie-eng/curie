// Panels fill the window; only text gets a ceiling.
//
// The Settings page capped its whole column at 760px. On a wide window that
// left a third of the screen empty beside a form that could have used it, and
// the dead band grew with the window. The cap was there for a real reason -- a
// paragraph running the full width of a large display is hard to read -- but it
// was applied to the panels rather than to the prose inside them.
//
// So the rule is: a view may cap the measure of TEXT, using `M.prose`, and may
// not otherwise pin a large width. Small fixed widths are component sizing (a
// truncated path, a fixed-width button) and are left alone.

import { describe, expect, it } from "vitest";

/** Below this a `maxWidth` is sizing a component, not framing a page. */
const LARGE = 400;

const SOURCES = import.meta.glob("../{views,shell}/*.tsx", {
  eager: true,
  query: "?raw",
  import: "default",
}) as Record<string, string>;

describe("no view frames itself into a fixed column", () => {
  it("caps large widths only with the shared prose measure", () => {
    const offenders: string[] = [];
    for (const [path, src] of Object.entries(SOURCES)) {
      if (path.includes(".test.")) continue;
      for (const line of src.split("\n")) {
        // Comments discuss past decisions; they are not layout.
        if (/^\s*(\/\/|\*|\/\*)/.test(line)) continue;
        for (const m of line.matchAll(/maxWidth:\s*(\d+)/g)) {
          if (Number(m[1]) >= LARGE) offenders.push(`${path.split("/").pop()}: ${line.trim()}`);
        }
      }
    }
    expect(
      offenders,
      "A large maxWidth pins a panel and leaves dead space beside it that grows " +
        "with the window. Cap the text with M.prose instead, and let the panel fill:",
    ).toEqual([]);
  });

  it("keeps the prose measure readable rather than merely narrow", async () => {
    const { M } = await import("../tokens");
    // Wide enough for a real paragraph, narrow enough to stay readable. If this
    // ever needs changing it should be changed here, once, not per view.
    expect(M.prose).toBeGreaterThanOrEqual(600);
    expect(M.prose).toBeLessThanOrEqual(900);
  });
});
