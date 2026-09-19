// The shell must provide what the console's contract says it provides.
//
// `packages/desktop-bridge` is the one declaration of `window.curie`, imported
// by the console. Nothing forces the shell to match it -- the preload builds
// separately, and a renderer's type is a claim about the other side, not a
// check. So this asserts the claim against the real preload surface. Drop a
// method the console depends on and this fails here, not in someone's hands.

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import type { DesktopBridge } from "../../../packages/desktop-bridge/bridge";
import { CH } from "./shared/contract";

describe("the shell honours the console's bridge contract", () => {
  it("has a channel behind every cli method the contract names", () => {
    // The contract's cli surface, spelled out so adding to it without adding a
    // channel is a failure rather than a silent gap.
    const promised: (keyof DesktopBridge["cli"])[] = ["run", "cancel", "onChunk", "onResult"];
    const channels: Record<keyof DesktopBridge["cli"], string> = {
      run: CH.cliRun,
      cancel: CH.cliCancel,
      onChunk: CH.cliChunk,
      onResult: CH.cliResult,
    };
    for (const method of promised) {
      expect(channels[method], `no IPC channel behind cli.${method}`).toBeTruthy();
    }
  });

  it("keeps the contract free of anything a browser bundle cannot compile", () => {
    // The console imports this file into a browser build, so a Node import or a
    // runtime value pulled from Electron would break that build. Types and pure
    // functions only.
    const here = dirname(fileURLToPath(import.meta.url));
    const src = readFileSync(
      join(here, "..", "..", "..", "packages", "desktop-bridge", "bridge.ts"),
      "utf8",
    );
    expect(src).not.toMatch(/from "(node:|electron)/);
    expect(src).not.toMatch(/require\(/);
  });
});
