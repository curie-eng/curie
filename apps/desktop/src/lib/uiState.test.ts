import { beforeEach, describe, expect, it, vi } from "vitest";

import { readBool, readNumber, write } from "./uiState";

beforeEach(() => localStorage.clear());

describe("remembered UI positions", () => {
  it("round-trips a number", () => {
    write("console.height", 320);
    expect(readNumber("console.height", 200, 100, 800)).toBe(320);
  });

  it("clamps a stored value that no longer fits", () => {
    // A stored size outlives the layout that produced it -- a smaller window, a
    // panel whose bounds changed -- and a height nobody can drag back is worse
    // than a default.
    write("console.height", 5000);
    expect(readNumber("console.height", 200, 100, 800)).toBe(800);
    write("console.height", 1);
    expect(readNumber("console.height", 200, 100, 800)).toBe(100);
  });

  it("falls back for anything it cannot read as a number", () => {
    write("console.height", "tall");
    expect(readNumber("console.height", 200, 100, 800)).toBe(200);
    expect(readNumber("never.set", 200, 100, 800)).toBe(200);
  });

  it("round-trips a boolean and keeps the fallback distinct from false", () => {
    expect(readBool("sidebar.collapsed", true)).toBe(true);
    write("sidebar.collapsed", false);
    expect(readBool("sidebar.collapsed", true)).toBe(false);
  });

  it("survives a storage that throws", () => {
    const spy = vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw new Error("denied");
    });
    expect(readNumber("x", 42, 0, 100)).toBe(42);
    expect(readBool("x", true)).toBe(true);
    spy.mockRestore();
    // Writing must not throw either: the panel works, it just will not persist.
    const setSpy = vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new Error("full");
    });
    expect(() => write("x", 1)).not.toThrow();
    setSpy.mockRestore();
  });
});
