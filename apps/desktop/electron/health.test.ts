// `docker ps` reports health inside a prose Status string, and the difference
// between "no healthcheck" and "still starting" is what decides whether a stack
// coming up ever finishes on screen.

import { describe, expect, it } from "vitest";

import { parseHealth } from "./ipc/resources";

describe("parseHealth", () => {
  it("reads the three verdicts Docker appends", () => {
    expect(parseHealth("Up 2 minutes (healthy)")).toBe("healthy");
    expect(parseHealth("Up 3 seconds (health: starting)")).toBe("starting");
    expect(parseHealth("Up 5 minutes (unhealthy)")).toBe("unhealthy");
  });

  it("says nothing for a container with no healthcheck", () => {
    // The case that matters: this must NOT read as "starting", or a stack part
    // of whose services declare no check would sit at "not ready" forever.
    expect(parseHealth("Up 2 minutes")).toBeNull();
    expect(parseHealth("Exited (0) 4 minutes ago")).toBeNull();
    expect(parseHealth(undefined)).toBeNull();
  });

  it("is not fooled by a word appearing elsewhere in the line", () => {
    expect(parseHealth("Up 2 minutes")).toBeNull();
    expect(parseHealth("Created")).toBeNull();
  });
});
