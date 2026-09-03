import { afterEach, describe, expect, it } from "vitest";

import { desktopBridge, inDesktop } from "@bridge";

const w = window as unknown as { curie?: unknown };
afterEach(() => { delete w.curie; });

describe("detecting the desktop shell", () => {
  it("is absent in a browser", () => {
    expect(desktopBridge()).toBeNull();
    expect(inDesktop()).toBe(false);
  });

  it("is present when the shell injected a usable cli", () => {
    w.curie = {
      cli: { run: async () => ({ runId: "r1" }), onChunk: () => () => {}, onResult: () => () => {} },
    };
    expect(inDesktop()).toBe(true);
  });

  it("rejects a bridge that cannot actually run anything", () => {
    // Fails closed on purpose: the browser path is always correct, so the cost
    // is a copy button where a run button could have been.
    // A half-injected or future bridge should read as absent rather than as
    // present-and-broken: the browser fallback is always correct, so failing
    // closed costs a copy button instead of a dead one.
    w.curie = {};
    expect(inDesktop()).toBe(false);
    w.curie = { cli: {} };
    expect(inDesktop()).toBe(false);
    // run but no streaming is still unusable: a run whose output cannot be
    // observed is worse than one that was never offered.
    w.curie = { cli: { run: async () => ({ runId: "r1" }) } };
    expect(inDesktop()).toBe(false);
  });
});
