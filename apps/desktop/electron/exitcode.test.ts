import { describe, expect, it } from "vitest";

import { parseExitCode } from "./ipc/resources";

describe("parseExitCode", () => {
  it("reads the code out of a stopped container's status", () => {
    expect(parseExitCode("Exited (0) 8 minutes ago")).toBe(0);
    expect(parseExitCode("Exited (137) 2 seconds ago")).toBe(137);
  });

  it("is null while the container is still running", () => {
    // Not zero. Zero is "finished successfully", which is the one wrong answer
    // that would make a starting container look done.
    expect(parseExitCode("Up 2 minutes (healthy)")).toBeNull();
    expect(parseExitCode("Created")).toBeNull();
    expect(parseExitCode(undefined)).toBeNull();
  });
});
