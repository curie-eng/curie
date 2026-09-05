// The two panel toggles.
//
// The app shows both at once -- the window's rail in the toolbar, the agent
// list in the Build tab's header -- and a control that looks identical in two
// places is a promise that it does the same thing in both. It does not: one
// narrows the whole window, the other hides a list inside a single view.

import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { PanelToggle } from "./index";

const markup = (el: HTMLElement) => el.querySelector("svg")!.innerHTML;

describe("PanelToggle", () => {
  it("draws a different mark for each panel", () => {
    const rail = render(
      <PanelToggle collapsed={false} onToggle={() => {}} label="the sidebar" />,
    ).container.firstElementChild as HTMLElement;
    const list = render(
      <PanelToggle collapsed={false} onToggle={() => {}} label="the agent list" variant="list" />,
    ).container.firstElementChild as HTMLElement;
    expect(markup(rail)).not.toBe(markup(list));
  });

  it("says which way it goes, in the title and to a screen reader", async () => {
    const { rerender } = render(
      <PanelToggle collapsed={false} onToggle={() => {}} label="the agent list" variant="list" />,
    );
    expect(screen.getByRole("button")).toHaveAttribute("title", "Hide the agent list");
    expect(screen.getByRole("button")).toHaveAttribute("aria-pressed", "true");

    rerender(
      <PanelToggle collapsed onToggle={() => {}} label="the agent list" variant="list" />,
    );
    expect(screen.getByRole("button")).toHaveAttribute("title", "Show the agent list");
    expect(screen.getByRole("button")).toHaveAttribute("aria-pressed", "false");
  });

  it("changes its mark with its state, so the glyph is not the only thing carrying it", () => {
    const shown = render(<PanelToggle collapsed={false} onToggle={() => {}} label="x" />)
      .container.firstElementChild as HTMLElement;
    const hidden = render(<PanelToggle collapsed onToggle={() => {}} label="x" />)
      .container.firstElementChild as HTMLElement;
    expect(markup(shown)).not.toBe(markup(hidden));
  });

  it("toggles", async () => {
    const onToggle = vi.fn();
    render(<PanelToggle collapsed={false} onToggle={onToggle} label="the sidebar" />);
    await userEvent.click(screen.getByRole("button"));
    expect(onToggle).toHaveBeenCalledOnce();
  });
});
