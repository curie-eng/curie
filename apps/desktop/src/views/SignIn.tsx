// Signing the console in, by pasting a code the CLI minted.
//
// ADR-0083: the browser never receives the platform key on any path. What
// crosses is a short-lived single-use code the operator copies out of a
// terminal; the API exchanges it for a revocable `HttpOnly` session cookie that
// page script cannot read. So there is no password field here, nothing is
// remembered, and nothing this component touches is a long-lived credential.

import { useState } from "react";

import { useApp } from "../bridge/app";
import { bridge } from "../bridge/bridge";
import { F, T } from "../tokens";
import { Button, Field, Input, Notice, Sheet } from "../primitives";

/**
 * Ask the shell to mint a code for us.
 *
 * The rule ADR-0083 sets is that the BROWSER must never hold the platform key.
 * The desktop shell is not a browser: it already holds the key and already runs
 * `curie` as you, exactly like the terminal does. So where the shell is present
 * there is no reason to make somebody go and copy a code by hand -- the app can
 * mint one and spend it immediately, and the browser rule is untouched.
 *
 * Runs with `--json` so the code is read from a parsed field rather than
 * scraped out of human-readable output.
 */
async function mintThroughShell(subject: string): Promise<string> {
  return new Promise((resolve, reject) => {
    let runId: string | null = null;
    const off = bridge().cli.onResult((r) => {
      if (runId !== null && r.runId !== runId) return;
      off();
      if (r.state !== "ok") {
        reject(new Error("The sign-in command did not succeed."));
        return;
      }
      const code = (r.result as { code?: unknown } | undefined)?.code;
      if (typeof code !== "string" || !code) {
        reject(new Error("The sign-in command returned no code."));
        return;
      }
      resolve(code);
    });
    bridge()
      .cli.run({ action: "local.console.login", flags: { subject }, json: true })
      .then((handle) => {
        runId = handle.runId;
      })
      .catch((e: unknown) => {
        off();
        reject(e instanceof Error ? e : new Error(String(e)));
      });
  });
}

/** The exchange is one unauthenticated POST; the cookie comes back on it. */
async function exchange(code: string): Promise<string | null> {
  const res = await bridge().api.request<{ detail?: string }>({
    method: "POST",
    path: "/console/session",
    body: { code: code.trim() },
  });
  if (res.ok) return null;
  // The API answers one indistinguishable failure for a wrong, consumed or
  // expired code, so this cannot say which -- and should not pretend to.
  if (res.status === 0) return "Could not reach the platform.";
  return "That code was not accepted. It may have been used already, or expired.";
}

export function SignIn({ onClose }: { readonly onClose: () => void }) {
  const app = useApp();
  const [code, setCode] = useState("");
  // Who the session is for. Minting binds it to the code (ADR-0106), and this
  // app has no idea who you are: it holds the platform key, which says what it
  // may do and nothing about whose hands are on it. So it asks rather than
  // inventing an identity and attributing your approvals to it.
  const [subject, setSubject] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Can this window run commands itself? In the desktop app, yes, and then
  // signing in needs no terminal at all.
  const canMint = !!app.env?.cliPath;

  async function signInHere() {
    if (busy || !subject.trim()) return;
    setBusy(true);
    setError(null);
    try {
      const minted = await mintThroughShell(subject.trim());
      const failure = await exchange(minted);
      if (failure) {
        setError(failure);
        return;
      }
      app.refreshApi();
      app.refreshAgents();
      onClose();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function submit() {
    if (!code.trim() || busy) return;
    setBusy(true);
    setError(null);
    const failure = await exchange(code);
    setBusy(false);
    if (failure) {
      setError(failure);
      return;
    }
    // The cookie is set. Re-probe so every view that reads connection state
    // learns about it at once, rather than each discovering it on its own next
    // request.
    app.refreshApi();
    // And reload what the refusals emptied. Re-probing only fixes the pill: the
    // agent list is still whatever the signed-out window managed to fetch,
    // which is nothing, so signing in landed on "no agents yet" plus a stale
    // error until something else happened to refresh. Signing in should show
    // you what you just got access to.
    app.refreshAgents();
    onClose();
  }

  return (
    <Sheet
      title="Sign in to Curie"
      onClose={onClose}
      width={520}
      footer={
        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end", width: "100%" }}>
          <Button tone="plain" onClick={onClose}>
            Cancel
          </Button>
          <Button tone="primary" busy={busy} disabled={!code.trim()} onClick={() => void submit()}>
            Sign in
          </Button>
        </div>
      }
    >
      {canMint ? (
        <div style={{ marginBottom: 16 }}>
          <div style={{ ...F.callout, color: T.secondary, lineHeight: 1.6, marginBottom: 10 }}>
            This app can sign itself in. It already runs Curie commands as you, so it can
            get a code and use it without you copying anything. Say who the session is
            for and it is recorded against everything that session approves.
          </div>
          <Field label="Who this session is for">
            <Input
              value={subject}
              onChange={(e) => setSubject(e.currentTarget.value)}
              placeholder="a Slack member id, e.g. U0EXAMPLE1"
              spellCheck={false}
              onKeyDown={(e) => {
                if (e.key === "Enter") void signInHere();
              }}
            />
          </Field>
          <Button
            tone="primary"
            busy={busy}
            disabled={!subject.trim()}
            onClick={() => void signInHere()}
          >
            Sign me in
          </Button>
        </div>
      ) : null}

      <div style={{ ...F.callout, color: T.secondary, lineHeight: 1.6, marginBottom: 14 }}>
        {canMint ? "Or paste a code: run " : "Run "}
        <strong>curie local console login --subject you</strong>
        {canMint ? " anywhere and paste what it prints." : " in a terminal and paste the code it prints."}
        {" "}A code works once and only signs in this window.
      </div>

      <Field label="Login code">
        <Input
          value={code}
          onChange={(e) => setCode(e.currentTarget.value)}
          placeholder="paste the code"
          autoFocus
          invalid={!!error}
          onKeyDown={(e) => {
            if (e.key === "Enter") void submit();
          }}
        />
      </Field>

      {error ? (
        <div style={{ marginTop: 12 }}>
          <Notice tone="error" title="Not signed in">
            {error}
          </Notice>
        </div>
      ) : null}
    </Sheet>
  );
}
