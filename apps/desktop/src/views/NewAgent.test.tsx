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
  it("gives its body a fixed height, so no step resizes the sheet", () => {
    // Every step and every template has a different amount to say. A body that
    // sized itself made the sheet jump each time somebody pressed a card or
    // Next -- the controls moving under the cursor that had just used them.
    mount();
    const body = [...document.querySelectorAll("div")].find(
      (d) => d.style.height !== "" && d.style.overflowY === "auto",
    );
    expect(body, "the wizard body should have an explicit height").toBeTruthy();
    expect(body!.style.height).toMatch(/^\d+px$/);
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
