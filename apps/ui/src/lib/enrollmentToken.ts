// The personal Claude token rule, the same one the CLI and the API apply (shared
// vector tests/vectors/personal-enrollment-token.json): one trailing newline
// trimmed, then 1 to 4096 characters, each printable ASCII without a space
// (0x21 to 0x7E). It is a format check only: acceptance never means the token
// is valid or unexpired.

export const ENROLLMENT_TOKEN_MAX_LENGTH = 4096;

export type EnrollmentTokenCheck =
  | { ok: true; token: string }
  | { ok: false; reason: "empty" | "too_long" | "invalid_character" };

const PRINTABLE = /^[\x21-\x7e]+$/;

export function checkEnrollmentToken(raw: string): EnrollmentTokenCheck {
  const token = raw.endsWith("\r\n") ? raw.slice(0, -2) : raw.endsWith("\n") ? raw.slice(0, -1) : raw;
  if (token.length === 0) return { ok: false, reason: "empty" };
  if (token.length > ENROLLMENT_TOKEN_MAX_LENGTH) return { ok: false, reason: "too_long" };
  if (!PRINTABLE.test(token)) return { ok: false, reason: "invalid_character" };
  return { ok: true, token };
}
