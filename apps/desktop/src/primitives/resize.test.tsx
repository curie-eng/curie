// The drag handle on a resizable panel.
//
// The drag itself is a pointer-capture sequence jsdom cannot fake honestly --
// no layout, and `setPointerCapture` is a stub -- so what these pin down is
// everything around it: the clamp at both stops, the keyboard path that is the
// only way a handle like this is reachable without a mouse, and the promise
// that persistence happens once per gesture rather than once per pixel.

import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { useState } from "react";

import { ResizeHandle } from "./index";

function Panel({ start = 300, onCommit }: { start?: number; onCommit?(n: number): void }) {
  const [h, setH] = useState(start);
  return (
    <ResizeHandle
      label="Console height"
      value={h}
      min={148}
      max={600}
      step={24}
      onChange={setH}
      onCommit={onCommit}
      onReset={() => setH(300)}
    />
  );
}

const handle = () => screen.getByRole("separator", { name: "Console height" });
const at = () => handle().getAttribute("aria-valuenow");

describe("ResizeHandle", () => {
  it("is a splitter that says where it is and where its stops are", () => {
    render(<Panel />);
    expect(handle()).toHaveAttribute("aria-orientation", "horizontal");
    expect(handle()).toHaveAttribute("aria-valuemin", "148");
    expect(handle()).toHaveAttribute("aria-valuemax", "600");
    expect(at()).toBe("300");
  });

  it("moves with the arrow keys, and up means taller", async () => {
    const user = userEvent.setup();
    render(<Panel />);
    handle().focus();
    await user.keyboard("{ArrowUp}");
    expect(at()).toBe("324");
    await user.keyboard("{ArrowDown}{ArrowDown}");
    expect(at()).toBe("276");
  });

  it("stops at both ends rather than running past them", async () => {
    const user = userEvent.setup();
    render(<Panel start={160} />);
    handle().focus();
    await user.keyboard("{ArrowDown}{ArrowDown}{ArrowDown}");
    expect(at()).toBe("148");
    await user.keyboard("{End}");
    expect(at()).toBe("148");
    await user.keyboard("{Home}");
    expect(at()).toBe("600");
    await user.keyboard("{ArrowUp}");
    expect(at()).toBe("600");
  });

  it("commits each keyboard step, so a panel sized without a mouse is remembered too", async () => {
    const user = userEvent.setup();
    const onCommit = vi.fn();
    render(<Panel onCommit={onCommit} />);
    handle().focus();
    await user.keyboard("{ArrowUp}");
    expect(onCommit).toHaveBeenCalledWith(324);
  });

  it("leaves keys it does not own to whatever else wants them", async () => {
    const user = userEvent.setup();
    const onCommit = vi.fn();
    render(<Panel onCommit={onCommit} />);
    handle().focus();
    await user.keyboard("{Enter}{ArrowLeft}a");
    expect(at()).toBe("300");
    expect(onCommit).not.toHaveBeenCalled();
  });

  it("goes back to where it started on a double-click", async () => {
    const user = userEvent.setup();
    render(<Panel />);
    handle().focus();
    await user.keyboard("{ArrowUp}{ArrowUp}");
    expect(at()).toBe("348");
    await user.dblClick(handle());
    expect(at()).toBe("300");
  });
});
