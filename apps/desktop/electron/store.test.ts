// The local API URL, pinned to the CLI's own constant.
//
// These two values have to agree and live in different languages: compose maps
// the API to 28000 on the host, the CLI prints that URL after `local up`, and
// this app has to talk to the same place. When they disagreed, every
// API-backed screen sat empty behind "not answering" while the stack was
// perfectly healthy -- a failure that looks like a broken platform and is
// actually a wrong port.

import { readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { LOCAL_API_URL } from "./shared/contract";

/** `cli/src/observability.rs`, four levels up from `apps/desktop/electron`. */
const OBSERVABILITY = join(__dirname, "..", "..", "..", "cli", "src", "observability.rs");

describe("the local API URL", () => {
  it("matches the CLI's LOCAL_API_URL", () => {
    let source: string;
    try {
      source = readFileSync(OBSERVABILITY, "utf8");
    } catch {
      // A released build has no checkout. Skipping loudly beats a red test that
      // only means "not run from source".
      console.warn(`skipped: ${OBSERVABILITY} not found (not a source checkout)`);
      return;
    }
    const m = /pub const LOCAL_API_URL: &str = "([^"]+)";/.exec(source);
    expect(m, "LOCAL_API_URL not found in cli/src/observability.rs").toBeTruthy();
    expect(LOCAL_API_URL).toBe(m![1]);
  });

  it("is not the port nothing serves", () => {
    // The specific regression: the container listens on 8000, compose maps it
    // elsewhere, and defaulting to the container's port means the app that
    // starts the stack cannot then reach it.
    expect(LOCAL_API_URL).not.toBe("http://localhost:8000");
  });
});
