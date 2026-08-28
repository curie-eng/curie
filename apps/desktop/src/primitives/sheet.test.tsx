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
  it("is nearly opaque, not glass and not a system dialog", () => {
    // A card is glass -- it sits on the pane and the window's vibrancy carrying
    // through it is the point. A sheet covers whatever happens to be behind it,
    // and on glass that content came through hard enough to compete with the
    // sheet's own text. Fully opaque fixed that and went too far: it read as a
    // dialog dropped on the app rather than part of it. `--sheet-fill` is the
    // film between, and the blur under it is what keeps the film legible.
    const panel = open();
    expect(panel.style.background).toBe("var(--sheet-fill)");
    expect(panel.style.background).not.toContain("card-fill");
    expect(panel.style.backdropFilter).toBe("var(--card-backdrop)");
  });

  it("renders at the top of the document, not where it was declared", () => {
    // `position: fixed` only escapes to the viewport while no ancestor makes a
    // containing block or a stacking context. `main` carries a mask (the fade
    // into the console) and a mask does exactly that, so an in-place sheet was
    // trapped in `main` and the console painted over its scrim. A z-index on the
    // console would have fixed that one case and left the next masked ancestor
    // to bring it back somewhere else.
    const overlay = open().parentElement!;
    expect(overlay.parentElement).toBe(document.body);
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
