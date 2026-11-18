import { useEffect, useState, type ReactNode } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { C } from "../../tokens";
import { Button, Card, CliHint, Modal, Notice, cliCommand } from "../../primitives";
import { useStore } from "../../state/store";
import { useOptionalConsoleSession } from "../../state/session";
import { CLAUDE_PLAN_PATH } from "../../state/route";
import { organizationSignInHref } from "../../components/LoginScreen";
import {
  EnrollmentApiError,
  disconnectPersonalEnrollment,
  getConsoleSignInMethods,
  getPersonalEnrollment,
  writePersonalEnrollment,
  type EnrollmentIntent,
  type PersonalEnrollment,
} from "../../api/client";
import { checkEnrollmentToken, type EnrollmentTokenCheck } from "../../lib/enrollmentToken";
import { LinkedAccounts } from "./LinkedAccounts";

// The Claude plan section of Settings (#5716): self-service enrollment for the
// person's own Claude subscription, the same owner API `curie cluster
// subscription` calls, authenticated by this browser's principal session.
//
// Token handling is the point of this file. The token is held only in the paste
// box's state, goes out only in one PUT body, and is cleared the moment that
// request settles. It is never put in a query, a mutation, a toast, a log line
// or any browser storage, and no error message is built from it. Status is the
// only thing the query cache holds.

const STATUS_KEY = ["personal-claude-enrollment"] as const;
const POLL_MS = 5000;

const TOKEN_REFUSALS: Record<Exclude<EnrollmentTokenCheck, { ok: true }>["reason"], string> = {
  empty: "Paste the token that `claude setup-token` printed.",
  too_long: "That is too long to be a Claude token. Paste only the token that `claude setup-token` printed.",
  invalid_character:
    "That is not a token. It must be one line of visible characters with no spaces; paste only what `claude setup-token` printed.",
};

const RETRY_LATER = "Curie cannot store your token right now. Try again in a minute.";
const RELOADED = "The page has reloaded it";

// One plain sentence per API code (docs/guides/personal-subscription.md, "The
// version check"). A code that is not listed gets the generic sentence.
const ERROR_SENTENCES: Record<string, string> = {
  stale_version: `Your plan changed in another window. ${RELOADED}; try again.`,
  enrollment_absent: `No plan is connected yet. ${RELOADED}; connect one first.`,
  enrollment_exists: `A plan is already connected. ${RELOADED}; replace its token instead.`,
  enrollment_inactive: `Your plan is disconnected. ${RELOADED}; reconnect it instead.`,
  enrollment_active: `Your plan is still connected. ${RELOADED}; replace its token instead.`,
  staging_conflict: `Another change was still settling. ${RELOADED}; try again in a minute.`,
  enrollment_busy: RETRY_LATER,
  custody_unavailable: RETRY_LATER,
  personal_rollout_in_progress: RETRY_LATER,
  custody_unconfigured: "Your operator has not turned on personal Claude plans for this installation.",
  invalid_enrollment_request:
    "Curie did not accept that token. Paste only the token that `claude setup-token` printed.",
};

export function describeEnrollmentError(e: unknown): string {
  if (e instanceof EnrollmentApiError) {
    if (e.code && ERROR_SENTENCES[e.code]) return ERROR_SENTENCES[e.code];
    if (e.status === 401) return "Your session expired. Sign in again.";
    if (e.status === 403) return "Curie refused the request because it did not come from this page. Reload the page and try again.";
    if (e.status === 409) return `Your plan changed. ${RELOADED}; try again.`;
  }
  return "Something went wrong. Try again in a minute.";
}

export function statusLine(e: PersonalEnrollment): string {
  if (e.state === "active") return `Connected, generation ${e.generation ?? "unknown"}`;
  if (e.state === "inactive") return "Disconnected";
  return "Not connected";
}

export function teardownLine(e: PersonalEnrollment): string | null {
  const { pending_runtimes: runs, pending_custody_cleanup: tokens } = e.teardown;
  if (runs <= 0 && tokens <= 0) return null;
  return `Disconnect still finishing: ${runs} runs and ${tokens} stored tokens to clean up.`;
}

function hasTeardown(e: PersonalEnrollment | undefined): boolean {
  return e !== undefined && teardownLine(e) !== null;
}

const muted = { fontSize: 12.5, color: C.muted, lineHeight: 1.5 } as const;

function SignInPrompt() {
  const [oidc, setOidc] = useState(false);
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
  return (
    <Card>
      <div style={{ fontSize: 15, fontWeight: 500, marginBottom: 6 }}>Claude plan</div>
      <div data-testid="claude-plan-sign-in" style={{ fontSize: 13.5, color: C.text2, lineHeight: 1.5 }}>
        Sign in with your organization to manage your Claude plan.
      </div>
      {oidc ? (
        <a
          href={organizationSignInHref(CLAUDE_PLAN_PATH)}
          data-testid="claude-plan-oidc-sign-in"
          style={{ display: "inline-block", marginTop: 12, color: C.link, fontSize: 13.5, fontWeight: 500 }}
        >
          Sign in with your organization
        </a>
      ) : null}
    </Card>
  );
}

function Hint({ children }: { children: ReactNode }) {
  return <div style={{ ...muted, marginTop: 8, maxWidth: 560 }}>{children}</div>;
}

function ClaudePlanBody() {
  const { dispatch } = useStore();
  const queryClient = useQueryClient();
  const [token, setToken] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [confirming, setConfirming] = useState(false);

  // The query holds status only. Polling runs while teardown is owed and stops
  // at zero.
  const query = useQuery({
    queryKey: STATUS_KEY,
    queryFn: getPersonalEnrollment,
    refetchInterval: (q) => (hasTeardown(q.state.data) ? POLL_MS : false),
    gcTime: 0,
  });
  const current = query.data;

  const reload = () => queryClient.invalidateQueries({ queryKey: STATUS_KEY });

  // Run one write. The token leaves this scope only inside `send`, and the
  // paste box is emptied whether the request succeeded or failed.
  async function run(send: () => Promise<PersonalEnrollment>, done: string) {
    setBusy(true);
    setError(null);
    try {
      queryClient.setQueryData(STATUS_KEY, await send());
      dispatch({ type: "toast", message: done });
    } catch (e) {
      setError(describeEnrollmentError(e));
      if (e instanceof EnrollmentApiError && (e.status === 409 || e.status === 404)) void reload();
    } finally {
      setToken("");
      setBusy(false);
    }
  }

  function submit(intent: EnrollmentIntent, done: string) {
    if (!current) return;
    const checked = checkEnrollmentToken(token);
    if (!checked.ok) {
      setToken("");
      setError(TOKEN_REFUSALS[checked.reason]);
      return;
    }
    const version = intent === "enroll" ? null : current.version;
    void run(() => writePersonalEnrollment(intent, version, checked.token), done);
  }

  function disconnect() {
    setConfirming(false);
    if (!current || current.version === null) return;
    const version = current.version;
    void run(() => disconnectPersonalEnrollment(version), "Claude plan disconnected");
  }

  if (query.isPending) return <Notice>Loading…</Notice>;
  if (!current) {
    return (
      <Card>
        <div data-testid="claude-plan-load-error" style={{ fontSize: 13.5, color: C.text2, marginBottom: 12 }}>
          Curie could not load your Claude plan. Try again in a minute.
        </div>
        <Button label="Retry" onClick={() => void reload()} />
      </Card>
    );
  }

  const teardown = teardownLine(current);
  const version = current.version === null ? undefined : String(current.version);
  const hintFlags = { "session-file": "session.txt", "token-file": "token.txt" };
  const write =
    current.state === "absent"
      ? { intent: "enroll" as const, label: "Connect", done: "Claude plan connected" }
      : current.state === "active"
        ? { intent: "replace" as const, label: "Replace", done: "Claude plan replaced" }
        : { intent: "reconnect" as const, label: "Reconnect", done: "Claude plan reconnected" };
  return (
    <Card>
      <div style={{ fontSize: 15, fontWeight: 500, marginBottom: 10 }}>Claude plan</div>
      <div data-testid="claude-plan-status" data-state={current.state} style={{ fontSize: 14, color: C.text }}>
        {statusLine(current)}
      </div>
      {teardown ? (
        <div data-testid="claude-plan-teardown" style={{ ...muted, marginTop: 4 }}>
          {teardown}
        </div>
      ) : null}

      <div style={{ marginTop: 18 }}>
        <div style={{ fontSize: 13, color: C.text2, lineHeight: 1.5, maxWidth: 560, marginBottom: 10 }}>
          Run <code style={{ fontFamily: C.mono }}>claude setup-token</code> in a terminal and paste the token it
          prints. Curie stores it for runs you start and never shows it again.
        </div>
        <form
          onSubmit={(e) => {
            e.preventDefault();
            if (!busy) submit(write.intent, write.done);
          }}
          style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}
        >
          <input
            type="password"
            aria-label="Claude token"
            data-testid="claude-plan-token"
            value={token}
            onChange={(e) => setToken(e.target.value)}
            autoComplete="off"
            autoCorrect="off"
            autoCapitalize="off"
            spellCheck={false}
            disabled={busy}
            style={{
              flex: "1 1 320px",
              maxWidth: 460,
              background: C.card,
              color: C.text,
              border: "1px solid " + C.borderStrong,
              borderRadius: 7,
              padding: "8px 10px",
              fontSize: 13,
              fontFamily: C.mono,
            }}
          />
          <Button label={write.label} variant="primary" testId="claude-plan-write" disabled={busy || token === ""} onClick={() => submit(write.intent, write.done)} />
          {write.intent === "enroll" ? (
            <CliHint command={cliCommand("cluster.subscription.enroll", hintFlags)} />
          ) : write.intent === "replace" ? (
            <CliHint command={cliCommand("cluster.subscription.replace", { ...hintFlags, "expected-version": version })} />
          ) : (
            <CliHint command={cliCommand("cluster.subscription.reconnect", { ...hintFlags, "expected-version": version })} />
          )}
        </form>
      </div>

      {current.state === "active" && current.version !== null ? (
        <div style={{ marginTop: 20, paddingTop: 16, borderTop: "1px solid " + C.border }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <Button label="Disconnect" variant="danger" testId="claude-plan-disconnect" disabled={busy} onClick={() => setConfirming(true)} />
            <CliHint command={cliCommand("cluster.subscription.disconnect", { "session-file": "session.txt", "expected-version": version })} />
          </div>
          <Hint>
            Disconnecting stops your runs and deletes the stored token. It does not revoke the token with Anthropic;
            revoke it in your Claude account settings.
          </Hint>
        </div>
      ) : null}

      {error ? (
        <div role="alert" data-testid="claude-plan-error" style={{ marginTop: 14, fontSize: 13, color: C.destructive }}>
          {error}
        </div>
      ) : null}

      {confirming ? (
        <Modal onClose={() => setConfirming(false)}>
          <div
            style={{ background: C.card, border: "1px solid " + C.borderStrong, borderRadius: 10, padding: 22, width: 400, maxWidth: "100%" }}
          >
            <div style={{ fontSize: 16, marginBottom: 8 }}>Disconnect your Claude plan?</div>
            <div style={{ ...muted, fontSize: 13, marginBottom: 16 }}>
              This stops your runs and deletes the stored token. It does not revoke the token with Anthropic; revoke it
              in your Claude account settings.
            </div>
            <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
              <Button label="Cancel" testId="claude-plan-disconnect-cancel" onClick={() => setConfirming(false)} />
              <Button label="Disconnect" variant="danger" testId="claude-plan-disconnect-confirm" onClick={disconnect} />
            </div>
          </div>
        </Modal>
      ) : null}
    </Card>
  );
}

// An operator (login-code) session cannot call the owner API, which accepts only
// a principal session, so it gets the prompt and no form. Linked accounts (#5719)
// join this section as further cards below the plan.
export function WiredClaudePlan() {
  const session = useOptionalConsoleSession();
  const principal = session === null || session.kind === "principal";
  return (
    <div data-testid="claude-plan-section" style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {principal ? <><ClaudePlanBody /><LinkedAccounts /></> : <SignInPrompt />}
    </div>
  );
}
