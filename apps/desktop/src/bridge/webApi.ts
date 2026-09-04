// Reaching the platform API from a browser tab.
//
// This UI is one codebase with two hosts. In the Electron shell the API goes
// through the main process, which holds the key and sidesteps CORS. In a browser
// there is no main process, so it goes same-origin through `/api/*` -- the same
// path `apps/ui` uses, and the reason the API needs no CORS middleware.
//
// Credentials are cookies, never a key. ADR-0083 is Accepted and specifies
// exactly this: the console authenticates with a revocable `HttpOnly` session
// cookie exchanged for a CLI-minted login code, and "the browser never receives
// the platform key on any path". So this sends `credentials: "include"` and
// nothing else. Until that endpoint exists the API answers 401, which is the
// honest state for an unauthenticated console and is what the sign-in prompt
// keys off -- rather than this file inventing a key and quietly holding admin
// rights in page scope, which is the thing 0083 exists to stop.

import type { ApiConnection, ApiRequest, ApiResponse } from "../../electron/shared/contract";

/** Same-origin, so a reverse proxy or the dev server's `/api` rule decides where
 *  the API actually is. Nothing here needs to know its address. */
const PREFIX = "/api";

function url(req: ApiRequest): string {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(req.query ?? {})) {
    if (v !== undefined) qs.set(k, String(v));
  }
  const q = qs.toString();
  return `${PREFIX}${req.path}${q ? `?${q}` : ""}`;
}

export async function webRequest<T>(req: ApiRequest): Promise<ApiResponse<T>> {
  try {
    const res = await fetch(url(req), {
      method: req.method,
      credentials: "include",
      headers: req.body === undefined ? {} : { "Content-Type": "application/json" },
      ...(req.body === undefined ? {} : { body: JSON.stringify(req.body) }),
    });
    // A 204 and an empty 200 both have no body to parse, and a proxy error page
    // is HTML. Reading the text first means a non-JSON answer surfaces as its
    // status rather than as a parse exception.
    const text = await res.text();
    let body: unknown = undefined;
    if (text) {
      try {
        body = JSON.parse(text);
      } catch {
        body = text;
      }
    }
    return { status: res.status, ok: res.ok, body: body as T };
  } catch (e) {
    // A network failure is not a status. Zero says "never reached the server",
    // which the connection probe reports as unreachable rather than as an error
    // the API returned.
    return { status: 0, ok: false, body: undefined as T, error: String(e) } as ApiResponse<T>;
  }
}

export async function webConnection(): Promise<ApiConnection> {
  // Probe an endpoint that REQUIRES authorization. `/config` answers without
  // it, so probing that reported "Connected" while every call the views make
  // came back 401 -- a green light over an empty screen, which is the exact
  // shape of dishonesty this console is supposed to avoid.
  const res = await webRequest<unknown>({ method: "GET", path: "/agents" });
  return {
    // Same-origin: the browser's own origin IS the base, and showing anything
    // else would be a guess.
    baseUrl: window.location.origin,
    // A cookie this page cannot read is exactly the point of `HttpOnly`. What
    // the UI can honestly report is whether the API accepted an authorized
    // call, which is the fact the toolbar is really claiming.
    hasKey: res.ok,
    // A browser tab has exactly one way to be authorized, so there is no
    // guessing here: if the call was accepted, a session cookie did it.
    ...(res.ok ? { via: "session" as const } : {}),
    reachable: res.status !== 0,
    checkedAt: Date.now(),
  };
}

/** 401/403 from a reachable API means "not signed in", which is a different
 *  state from "no API here" and gets a different prompt. */
export function needsSignIn(res: { status: number }): boolean {
  return res.status === 401 || res.status === 403;
}


/** Revoke this console's session at the server, then re-probe.
 *
 *  Server-side, not just a cookie drop, because the cookie is `HttpOnly` and
 *  this page could not delete it anyway. Revoking is also the stronger promise:
 *  a token copied out of a proxy log stops working, rather than only stopping
 *  being sent. The response clears the cookie as well, so the browser does not
 *  keep presenting something already dead.
 *
 *  The re-probe is what the caller renders. Signing out leaves the API perfectly
 *  reachable and simply unauthorized, and that is the state the sign-in prompt
 *  keys off. */
export async function webSignOut(): Promise<ApiConnection> {
  await webRequest({ method: "DELETE", path: "/console/session" });
  return webConnection();
}
