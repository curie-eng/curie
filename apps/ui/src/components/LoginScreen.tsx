import { useEffect, useRef, useState, type FormEvent, type ReactNode } from "react";
import { C, R } from "../tokens";
import { Button, Card, CliHint, cliCommand } from "../primitives";
import { API_PREFIX } from "../api/config";
import { ApiError, exchangeConsoleLoginCode, getConsoleSignInMethods, type ConsoleSession } from "../api/client";

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

// Same-origin /api so the OIDC cookies land on the console host. return_to is
// attached only for the two paths the API will honor.
export function organizationSignInHref(pathname: string): string {
  const href = `${API_PREFIX}/console/oidc/login`;
  if (pathname === "/" || pathname === "/settings/claude-plan") {
    return `${href}?return_to=${encodeURIComponent(pathname)}`;
  }
  return href;
}

// The CLI mints codes with Python's `secrets.token_urlsafe`, so a real code is
// only URL-safe base64 characters. Anything else (an email address, a URL, a
// sentence, a key with punctuation) was pasted by someone who took the box for
// a different sign-in and is told so before any request is sent.
const LOGIN_CODE_FORMAT = /^[A-Za-z0-9_-]+$/;

function notACodeMessage(orgSignIn: boolean): string {
  return orgSignIn
    ? 'That is not a sign-in code. A one-time sign-in code from the Curie CLI was expected. To sign in with your account, use "Sign in with your organization" above.'
    : "That is not a sign-in code. A one-time sign-in code from the Curie CLI was expected.";
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
  const [oidc, setOidc] = useState(false);
  const orgLink = useRef<HTMLAnchorElement>(null);
  const codeInput = useRef<HTMLInputElement>(null);

  // Organization sign-in is the primary action when it is configured, so it
  // takes focus from the code box once the methods answer, unless the person
  // has already started typing a code.
  useEffect(() => {
    if (!oidc) return;
    const active = document.activeElement;
    if (active === document.body || (active === codeInput.current && !codeInput.current?.value)) {
      orgLink.current?.focus();
    }
  }, [oidc]);

  useEffect(() => {
    let live = true;
    getConsoleSignInMethods()
      .then((methods) => {
        if (live) setOidc(methods.oidc === true);
      })
      .catch(() => {
        if (live) setOidc(false);
      });
    return () => {
      live = false;
    };
  }, []);

  const submit = async () => {
    const trimmed = code.trim();
    if (!trimmed) {
      setError("Enter a login code minted by the Curie CLI.");
      return;
    }
    if (!LOGIN_CODE_FORMAT.test(trimmed)) {
      setError(notACodeMessage(oidc));
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
          {oidc
            ? "Sign in with your organization account. The console never handles the platform key."
            : "The console never handles the platform key. Mint a single-use login code with the Curie CLI and paste it below."}
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

        {oidc ? (
          <a
            ref={orgLink}
            href={organizationSignInHref(window.location.pathname)}
            data-testid="console-oidc-sign-in"
            style={{
              display: "block",
              boxSizing: "border-box",
              width: "100%",
              textAlign: "center",
              marginBottom: 18,
              padding: "9px 14px",
              borderRadius: 7,
              background: C.brand,
              border: "1px solid " + C.brand,
              color: "#08120c",
              fontSize: 13.5,
              fontWeight: 600,
              textDecoration: "none",
            }}
          >
            Sign in with your organization
          </a>
        ) : null}

        {oidc ? (
          <p
            data-testid="console-login-code-heading"
            style={{ fontSize: 12.5, color: C.muted, margin: "0 0 10px", lineHeight: 1.5 }}
          >
            Or use a one-time code from the Curie CLI.
          </p>
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
            ref={codeInput}
            autoFocus={!oidc}
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
            variant={oidc ? "secondary" : "primary"}
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
