import { afterEach, describe, expect, it } from "vitest";

import { desktopBridge, inDesktop } from "./desktopBridge";

const w = window as unknown as { curie?: unknown };
afterEach(() => { delete w.curie; });

describe("detecting the desktop shell", () => {
  it("is absent in a browser", () => {
    expect(desktopBridge()).toBeNull();
    expect(inDesktop()).toBe(false);
  });

  it("is present when the shell injected a usable cli", () => {
    w.curie = { cli: { run: async () => ({ runId: "r1" }) } };
    expect(inDesktop()).toBe(true);
  });

  it("rejects a bridge that cannot actually run anything", () => {
    // A half-injected or future bridge should read as absent rather than as
    // present-and-broken: the browser fallback is always correct, so failing
    // closed costs a copy button instead of a dead one.
    w.curie = {};
    expect(inDesktop()).toBe(false);
    w.curie = { cli: {} };
    expect(inDesktop()).toBe(false);
  });
});
