import { useState, type FormEvent, type ReactNode } from "react";
import { C, R } from "../tokens";
import { Button, Card, CliHint, cliCommand } from "../primitives";
import { ApiError, exchangeConsoleLoginCode, type ConsoleSession } from "../api/client";

// The console's only way in (#1047, ADR-0083). The browser never handles the
// platform key: an operator mints a single-use login code with the CLI and
// pastes it here; the API answers with an HttpOnly session cookie that every
// later call carries. Rendered by ConsoleSessionGate while signed out.

// Placeholder subject shown in the CLI hints; the operator substitutes their own.
const EXAMPLE_SUBJECT = "you@example.com";

// Full-page centered frame shared by the gate's pre-console states.
export function ConsoleGateFrame({ children, testId }: { children: ReactNode; testId?: string }) {
  return (
    <div
      data-testid={testId}
      style={{
        fontFamily: C.sans,
        color: C.text,
        minHeight: "100vh",
        background: C.page,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: 20,
        boxSizing: "border-box",
      }}
    >
      <div style={{ width: "min(440px, 100%)" }}>{children}</div>
    </div>
  );
}

// One tier's mint command: the resolved text to read, plus its copy hint.
function HintRow({ label, command, children }: { label: string; command: string; children: ReactNode }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 12.5, minWidth: 0 }}>
      <span style={{ color: C.muted, width: 92, flexShrink: 0 }}>{label}</span>
      <code style={{ fontFamily: C.mono, fontSize: 12, color: C.text2, overflowWrap: "anywhere", flex: 1 }}>
        {command}
      </code>
      {children}
    </div>
  );
}

function describeFailure(e: unknown): string {
  if (e instanceof ApiError) {
    if (e.status === 401) return e.message;
    if (e.status === 429) return "Too many sign-in attempts. Wait a minute and try again.";
    if (e.status === 503) return "Console sign-in is temporarily unavailable. Try again shortly.";
    return `${e.status}: ${e.message}`;
  }
  return e instanceof Error ? e.message : String(e);
}

export function LoginScreen({
  notice,
  onSignedIn,
}: {
  notice: string | null;
  onSignedIn: (session: ConsoleSession) => void;
}) {
  const [code, setCode] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = async () => {
    const trimmed = code.trim();
    if (!trimmed) {
      setError("Enter a login code minted by the Curie CLI.");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const session = await exchangeConsoleLoginCode(trimmed);
      // Historical subject-less rows are not a usable principal.
      if (!session.subject?.trim()) throw new ApiError(401, "missing, invalid, or expired console session");
      setCode("");
      onSignedIn(session);
    } catch (e) {
      setError(describeFailure(e));
    } finally {
      setBusy(false);
    }
  };

  const onSubmit = (e: FormEvent) => {
    e.preventDefault();
    if (!busy) void submit();
  };

  return (
    <ConsoleGateFrame testId="console-login">
      <Card>
        <h1 style={{ fontSize: 19, fontWeight: 500, color: C.text, margin: "0 0 6px" }}>Sign in to Curie Console</h1>
        <p style={{ fontSize: 13, color: C.muted, margin: "0 0 16px", lineHeight: 1.5 }}>
          The console never handles the platform key. Mint a single-use login code with the Curie CLI and paste it
          below.
        </p>

        {notice ? (
          <div
            data-testid="console-login-notice"
            style={{
              border: "1px solid " + C.border,
              borderLeft: "3px solid " + C.warn,
              borderRadius: 6,
              padding: "8px 10px",
              color: C.text2,
              fontSize: 12.5,
              marginBottom: 14,
            }}
          >
            {notice}
          </div>
        ) : null}

        <div style={{ display: "flex", flexDirection: "column", gap: 4, marginBottom: 16 }}>
          <HintRow label="Local stack" command={cliCommand("local.console.login", { subject: EXAMPLE_SUBJECT })}>
            <CliHint command={cliCommand("local.console.login", { subject: EXAMPLE_SUBJECT })} />
          </HintRow>
          <HintRow label="Cluster" command={cliCommand("cluster.console.login", { subject: EXAMPLE_SUBJECT })}>
            <CliHint command={cliCommand("cluster.console.login", { subject: EXAMPLE_SUBJECT })} />
          </HintRow>
        </div>

        <form onSubmit={onSubmit} style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          <input
            aria-label="login code"
            value={code}
            onChange={(e) => setCode(e.target.value)}
            placeholder="Login code"
            autoComplete="one-time-code"
            spellCheck={false}
            autoFocus
            style={{
              background: C.input,
              border: "1px solid " + C.borderStrong,
              borderRadius: R.input,
              padding: "8px 11px",
              color: C.text,
              fontSize: 13,
              fontFamily: C.mono,
              width: "100%",
              boxSizing: "border-box",
            }}
          />
          {error ? (
            <div data-testid="console-login-error" style={{ color: C.destructive, fontSize: 12.5, fontFamily: C.mono }}>
              {error}
            </div>
          ) : null}
          <Button
            label={busy ? "Signing in…" : "Sign in"}
            variant="primary"
            full
            testId="console-login-submit"
            disabled={busy}
            onClick={() => void submit()}
          />
        </form>
      </Card>
    </ConsoleGateFrame>
  );
}
