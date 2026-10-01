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

import { isLoopback, LOCAL_API_KEY, LOCAL_API_URL } from "./shared/contract";

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

describe("the local dev key", () => {
  // `curie local deploy` defaults `--api-key` to this, which is why deploying
  // from a terminal needs no setup. The app sent no key at all and 401'd against
  // the stack it had itself just started.
  it("is the same value the CLI defaults to", () => {
    let source: string;
    try {
      source = readFileSync(join(__dirname, "..", "..", "..", "cli", "src", "main.rs"), "utf8");
    } catch {
      console.warn("skipped: cli/src/main.rs not found (not a source checkout)");
      return;
    }
    expect(source).toContain(`default_value = "${LOCAL_API_KEY}"`);
  });
});

describe("isLoopback", () => {
  it("accepts the addresses that mean this machine", () => {
    expect(isLoopback("http://localhost:28000")).toBe(true);
    expect(isLoopback("http://127.0.0.1:28000")).toBe(true);
    expect(isLoopback("http://[::1]:28000")).toBe(true);
  });

  it("refuses a host that merely starts with localhost", () => {
    // The reason this parses rather than string-matches. A `startsWith` check
    // would post a credential to someone else's server.
    expect(isLoopback("http://localhost.evil.com")).toBe(false);
    expect(isLoopback("http://notlocalhost")).toBe(false);
    expect(isLoopback("https://api.curie.example")).toBe(false);
  });

  it("refuses anything it cannot parse rather than guessing", () => {
    expect(isLoopback("")).toBe(false);
    expect(isLoopback("localhost:28000")).toBe(false);
  });
});
