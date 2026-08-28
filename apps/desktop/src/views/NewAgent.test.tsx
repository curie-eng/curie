// The wizard's geometry does not move.
//
// jsdom cannot measure layout, so these assert the two decisions that produce
// the stable geometry rather than the pixels themselves. Both are one style
// away from regressing and both were reported from a real screen.

import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";

import { NewAgent } from "./NewAgent";
import { AppProvider } from "../bridge/app";
import { RunsProvider } from "../bridge/runs";
import { ResourcesProvider } from "../bridge/resources";
import { TEMPLATES } from "../lib/templates";

function mount() {
  render(
    <AppProvider>
      <ResourcesProvider>
        <RunsProvider>
          <NewAgent onClose={() => {}} />
        </RunsProvider>
      </ResourcesProvider>
    </AppProvider>,
  );
}

describe("the new-agent wizard", () => {
  it("fixes the height on the SHEET's own body, not a second box inside it", () => {
    // Two things at once. The height is fixed because every step has a different
    // amount to say and a self-sizing body made the sheet jump on every press.
    // And it belongs to the sheet's own scrolling box, because a wrapper with
    // its own `overflow` clips at ITS padding edge -- which was zero, so every
    // card inside had its shadow cut off left, right and bottom. There is one
    // scrolling box, and it carries the sheet's 18px inset.
    mount();
    const scrollers = [...document.querySelectorAll("div")].filter((d) =>
      /auto/.test(d.style.overflow + d.style.overflowY),
    );
    expect(scrollers, "exactly one scrolling box").toHaveLength(1);

    const body = scrollers[0];
    expect(body.style.padding).toBe("0px 18px 18px");
    // jsdom rewrites `calc(84vh - 168px)` as `-168px + 84vh`, so match the parts
    // rather than the spelling.
    expect(body.style.height).toMatch(/^min\(\d+px, /);
    expect(body.style.height).toContain("84vh");
  });

  it("does not put the description inside the card it belongs to", async () => {
    // It used to appear only in the selected card, so picking one resized it and
    // shoved the cards below it down the page. Selection must not move the thing
    // you are selecting between.
    mount();
    const stack = TEMPLATES[0];
    const card = screen.getByRole("button", { name: new RegExp(stack.name) });
    expect(card.textContent).not.toContain(stack.about);

    // Picking another one does not change that.
    const other = screen.getByRole("button", { name: new RegExp(TEMPLATES[1].name) });
    await userEvent.click(other);
    expect(other.textContent).not.toContain(TEMPLATES[1].about);
  });
});
