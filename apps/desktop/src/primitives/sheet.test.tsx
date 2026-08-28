// Two properties of a modal that have each been got wrong once.
//
// They are pinned here rather than left to review because both are invisible in
// the source (one token reference, one padding value) and glaring on screen.

import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { Sheet } from "./index";
import { M } from "../tokens";

function open() {
  render(
    <Sheet title="A modal" onClose={() => {}}>
      <p>body</p>
    </Sheet>,
  );
  return screen.getByRole("dialog");
}

describe("Sheet", () => {
  it("is opaque, because it floats over arbitrary content", () => {
    // A card is glass -- it sits on the pane and the window's vibrancy carrying
    // through it is the point. A sheet covers whatever happens to be behind it,
    // and on glass that content came through hard enough to compete with the
    // sheet's own text: a page heading reading through the sheet's title, a
    // section label crossing a paragraph. `--card-fill` here is the bug.
    const panel = open();
    expect(panel.style.background).toBe("var(--s-raised)");
    expect(panel.style.background).not.toContain("card-fill");
    // A blur over an opaque fill is a compositing layer that does nothing.
    expect(panel.style.backdropFilter).toBe("");
  });

  it("centres on the content pane, not the window", () => {
    // The sidebar is permanent chrome, so the lit area is the frame the eye
    // measures against. Centred on the window, a sheet sits half the sidebar's
    // width left of where it looks like it belongs -- reported as "not
    // centered", and it was. The scrim still spans the whole window: a modal
    // that leaves part of the window looking live lies about what you can click.
    const overlay = open().parentElement!;
    expect(overlay.style.inset).toBe("0px");
    expect(overlay.style.paddingLeft).toBe(`${M.sidebar + 24}px`);
    expect(overlay.style.justifyContent).toBe("center");
  });
});
