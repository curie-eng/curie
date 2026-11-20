import { describe, expect, it } from "vitest";
import vectors from "../../../../tests/vectors/personal-enrollment-token.json";
import { checkEnrollmentToken, ENROLLMENT_TOKEN_MAX_LENGTH } from "./enrollmentToken";

type Case = { name: string; token?: string; token_repeat?: { char: string; count: number }; valid: boolean };

function tokenOf(c: Case): string {
  return c.token_repeat ? c.token_repeat.char.repeat(c.token_repeat.count) : (c.token ?? "");
}

describe("claude_plan token check", () => {
  it("claude_plan token check agrees with the shared vector", () => {
    expect(vectors.max_length).toBe(ENROLLMENT_TOKEN_MAX_LENGTH);
    for (const c of vectors.cases as Case[]) {
      expect(checkEnrollmentToken(tokenOf(c)).ok, c.name).toBe(c.valid);
    }
  });

  it("claude_plan token check trims exactly one trailing newline", () => {
    expect(checkEnrollmentToken("EXAMPLE\n")).toEqual({ ok: true, token: "EXAMPLE" });
    expect(checkEnrollmentToken("EXAMPLE\r\n")).toEqual({ ok: true, token: "EXAMPLE" });
    expect(checkEnrollmentToken("EXAMPLE\n\n")).toEqual({ ok: false, reason: "invalid_character" });
    expect(checkEnrollmentToken("\n")).toEqual({ ok: false, reason: "empty" });
  });

  it("claude_plan token check names why a token is refused", () => {
    expect(checkEnrollmentToken("")).toEqual({ ok: false, reason: "empty" });
    expect(checkEnrollmentToken("a".repeat(4097))).toEqual({ ok: false, reason: "too_long" });
    expect(checkEnrollmentToken("a b")).toEqual({ ok: false, reason: "invalid_character" });
  });
});
