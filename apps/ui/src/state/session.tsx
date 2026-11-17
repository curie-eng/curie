import { createContext, useContext, useEffect, useRef, useState, type ReactNode } from "react";
import { C } from "../tokens";
import { Button, Card, Notice } from "../primitives";
import {
  ApiError,
  getConsoleSession,
  isUnauthorized,
  logoutConsoleSession,
  onUnauthorized,
  type ConsoleSession,
} from "../api/client";
import { ConsoleGateFrame, LoginScreen } from "../components/LoginScreen";

// The global console sign-in gate (#1047, ADR-0083). The console authenticates
// only with the HttpOnly session cookie, so nothing behind this gate mounts
// (and no protected request is sent) until GET /console/session confirms a
// live, subject-bound session. Mounted in main.tsx inside StoreProvider (the
// login screen's CLI hints toast through it) and outside WiredProvider.
//
// 401 policy: a 401 from a protected call does NOT sign the operator out by
// itself, because some routes refuse a live session by design (the state
// router accepts only the platform key or a sandbox token). It triggers one
// deduplicated re-probe of the session; only a 401 from that probe returns to
// the login screen. A live probe, or one the rate limiter refused, keeps the
// operator where they are and leaves the view to show its own error.

const EXPIRED_NOTICE = "Your console session expired or was revoked. Mint a new login code and sign in again.";
const PRINCIPAL_EXPIRED_NOTICE = "Your session expired. Sign in again.";

// A login-code stub from before `kind` existed still counts as an operator
// session. A principal session is live even when its subject is only the
// principal id the API now returns.
function acceptsSession(session: ConsoleSession): boolean {
  if (session.kind === "principal") return true;
  return Boolean(session.subject?.trim());
}

type GateState =
  | { kind: "checking" }
  | { kind: "signedOut"; notice: string | null }
  | { kind: "unavailable"; message: string }
  | { kind: "signedIn"; session: ConsoleSession };

function unavailableMessage(e: unknown): string {
  if (e instanceof ApiError && e.status === 429) return "Too many requests; wait a minute and retry.";
  if (e instanceof ApiError && e.status === 503) return "Console sign-in is temporarily unavailable.";
  return e instanceof Error ? e.message : String(e);
}

const Ctx = createContext<ConsoleSession | null>(null);
const SignOutCtx = createContext<(() => Promise<void>) | null>(null);

export function ConsoleSessionGate({ children }: { children: ReactNode }) {
  const [state, setState] = useState<GateState>({ kind: "checking" });
  // Bumped by Retry to re-run the initial probe.
  const [attempt, setAttempt] = useState(0);
  // The in-flight mid-session re-probe, so concurrent 401s share one request.
  const reprobe = useRef<Promise<void> | null>(null);
  // The session the re-probe is judging, kept after sign-out so a late 401
  // still knows whether the person or the operator was signed in.
  const sessionRef = useRef<ConsoleSession | null>(null);
  useEffect(() => {
    if (state.kind === "signedIn") sessionRef.current = state.session;
  }, [state]);

  const signOut = async () => {
    try {
      if (await logoutConsoleSession()) setState({ kind: "signedOut", notice: null });
    } catch {
      // A failed logout leaves the console up. The cookie is still live.
    }
  };

  useEffect(() => {
    let live = true;
    getConsoleSession()
      .then((session) => {
        if (!live) return;
        setState(acceptsSession(session) ? { kind: "signedIn", session } : { kind: "signedOut", notice: null });
      })
      .catch((e: unknown) => {
        if (!live) return;
        setState(
          isUnauthorized(e) ? { kind: "signedOut", notice: null } : { kind: "unavailable", message: unavailableMessage(e) },
        );
      });
    return () => {
      live = false;
    };
  }, [attempt]);

  const signedIn = state.kind === "signedIn";

  useEffect(() => {
    if (!signedIn) return;
    let live = true;
    const unsubscribe = onUnauthorized(() => {
      if (reprobe.current) return;
      reprobe.current = getConsoleSession()
        .then(
          () => {},
          (e: unknown) => {
            if (live && isUnauthorized(e)) {
              const notice =
                sessionRef.current?.kind === "principal" ? PRINCIPAL_EXPIRED_NOTICE : EXPIRED_NOTICE;
              setState({ kind: "signedOut", notice });
            }
          },
        )
        .finally(() => {
          reprobe.current = null;
        });
    });
    return () => {
      live = false;
      unsubscribe();
    };
  }, [signedIn]);

  switch (state.kind) {
    case "checking":
      return (
        <ConsoleGateFrame>
          <Notice>Checking console session…</Notice>
        </ConsoleGateFrame>
      );
    case "signedOut":
      return <LoginScreen notice={state.notice} onSignedIn={(session) => setState({ kind: "signedIn", session })} />;
    case "unavailable":
      return (
        <ConsoleGateFrame testId="console-session-unavailable">
          <Card>
            <div style={{ fontSize: 15, fontWeight: 500, color: C.text, marginBottom: 6 }}>
              Console session unavailable
            </div>
            <div style={{ color: C.text2, fontSize: 13, marginBottom: 14 }}>{state.message}</div>
            <Button
              label="Retry"
              variant="primary"
              onClick={() => {
                setState({ kind: "checking" });
                setAttempt((n) => n + 1);
              }}
            />
          </Card>
        </ConsoleGateFrame>
      );
    case "signedIn":
      return (
        <SignOutCtx.Provider value={signOut}>
          <Ctx.Provider value={state.session}>{children}</Ctx.Provider>
        </SignOutCtx.Provider>
      );
  }
}

// The signed-in console session. Only components rendered behind
// ConsoleSessionGate may call it.
export function useConsoleSession(): ConsoleSession {
  const session = useContext(Ctx);
  if (!session) throw new Error("useConsoleSession must be used within ConsoleSessionGate");
  return session;
}

// Null outside the gate, so chrome rendered by unit tests without a provider
// stays the operator shell.
export function useOptionalConsoleSession(): ConsoleSession | null {
  return useContext(Ctx);
}

export function useConsoleSignOut(): (() => Promise<void>) | null {
  return useContext(SignOutCtx);
}
