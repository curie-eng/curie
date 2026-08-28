import { describe, expect, it } from "vitest";

import { diagnose } from "./diagnose";

const CHECKOUT = { repoRoot: "/repo", sourceCheckout: true };
const RELEASED = { repoRoot: null, sourceCheckout: false };

describe("diagnose", () => {
  // The real one, verbatim from a deploy that had already succeeded.
  const REAL =
    "deploying squawk as squawk: failed (failed) (0.0s)\n" +
    "Error: decoding agent list: error decoding response body: missing field `channels` at line 1 column 297";

  it("recognises a contract mismatch and names the field", () => {
    const d = diagnose(REAL, CHECKOUT);
    expect(d?.title).toMatch(/different versions/);
    expect(d?.detail).toContain("`channels`");
  });

  it("points a checkout at rebuilding the platform", () => {
    // `local up` pulls published images unless told otherwise, so a source-built
    // CLI against a registry API is the DEFAULT way to end up here.
    expect(diagnose(REAL, CHECKOUT)?.fix).toBe("local up --build");
  });

  it("points a released install at updating the CLI instead", () => {
    expect(diagnose(REAL, RELEASED)?.fix).toBe("update");
  });

  it("says nothing about an ordinary failure", () => {
    // A hint under every failure is noise, and a wrong hint costs more than
    // none: somebody follows it instead of reading the error in front of them.
    expect(diagnose("Error: connection refused", CHECKOUT)).toBeUndefined();
    expect(diagnose("no such file or directory", CHECKOUT)).toBeUndefined();
    expect(diagnose("", CHECKOUT)).toBeUndefined();
  });

  it("catches the other shapes the decoder produces", () => {
    for (const line of [
      "unknown field `surfaces`",
      "unknown variant `email`, expected one of",
      "invalid type: null, expected a sequence",
      "error decoding response body",
    ]) {
      expect(diagnose(line, CHECKOUT), line).toBeTruthy();
    }
  });
});
