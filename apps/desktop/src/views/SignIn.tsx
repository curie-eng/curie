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
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

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
      <div style={{ ...F.callout, color: T.secondary, lineHeight: 1.6, marginBottom: 14 }}>
        Run <strong>curie local console login</strong> in a terminal and paste the code it prints.
        The code works once and only signs in this browser.
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
