// Platform API access, proxied through the main process.
//
// Two things fall out of doing this in main rather than with `fetch` in the
// renderer, and both are the reason the desktop app is not just the web console
// in a window. First, `apps/api` ships no CORS middleware on purpose, so a page
// cannot call it cross-origin -- from Node there is no origin to check, and the
// app can point at a cluster API on any host without the API growing a CORS
// surface for it. Second, the API key stays in the main process: the renderer
// asks whether a key is held, never for the key.

import {
  isLoopback,
  LOCAL_API_KEY,
  type ApiConnection,
  type ApiRequest,
  type ApiResponse,
} from "../shared/contract.js";
import { prefs, update } from "./store.js";

function url(base: string, path: string, query?: ApiRequest["query"]): string {
  const u = new URL(path.replace(/^\/+/, ""), base.endsWith("/") ? base : `${base}/`);
  for (const [k, v] of Object.entries(query ?? {})) {
    if (v !== undefined && v !== "") u.searchParams.set(k, String(v));
  }
  return u.toString();
}

export async function request<T = unknown>(req: ApiRequest): Promise<ApiResponse<T>> {
  const { apiBaseUrl, apiKey } = prefs();
  const headers: Record<string, string> = { Accept: "application/json" };
  // A stored key always wins. With none set, fall back to the key the local dev
  // stack ships with -- the same default `curie local deploy` uses, which is why
  // deploying from a terminal needs no setup. Loopback only: it is a well-known
  // development credential, fine to assume against this machine and unacceptable
  // to send anywhere else.
  const key = apiKey || (isLoopback(apiBaseUrl) ? LOCAL_API_KEY : null);
  if (key) headers["X-API-Key"] = key;
  if (req.body !== undefined) headers["Content-Type"] = "application/json";

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20_000);
  try {
    const res = await fetch(url(apiBaseUrl, req.path, req.query), {
      method: req.method,
      headers,
      body: req.body === undefined ? undefined : JSON.stringify(req.body),
      signal: controller.signal,
    });
    const text = await res.text();
    let body: unknown = text;
    if (text) {
      try {
        body = JSON.parse(text);
      } catch {
        // Non-JSON error pages (a proxy's 502 HTML, say) reach the UI as text
        // rather than becoming an opaque parse failure.
      }
    }
    return {
      status: res.status,
      ok: res.ok,
      body: body as T,
      error: res.ok ? undefined : `${res.status} ${res.statusText}`,
    };
  } catch (err) {
    const message = (err as Error).name === "AbortError" ? "request timed out" : (err as Error).message;
    return { status: 0, ok: false, body: undefined as T, error: message };
  } finally {
    clearTimeout(timeout);
  }
}

/** Probe the configured API. `/config` is the one endpoint that answers before
 *  auth, so it distinguishes "cannot reach the API" from "reached it, key is
 *  wrong" -- two problems with completely different fixes. */
export async function connection(): Promise<ApiConnection> {
  const { apiBaseUrl, apiKey } = prefs();
  const held = !!(apiKey || (isLoopback(apiBaseUrl) ? LOCAL_API_KEY : null));
  const res = await request<{ org_name?: string }>({ method: "GET", path: "/config" });
  return {
    baseUrl: apiBaseUrl,
    // "Can this console make authorized calls", not "is a key stored". A
    // loopback API takes the dev key without one being configured, and `request`
    // below already applies it -- so reporting "no key" there made the shell
    // claim it was signed out while its own requests were succeeding. Both hosts
    // now answer the same question with this field, which is what lets one
    // screen gate on it.
    hasKey: held,
    // The shell's credential is always the platform key: it never holds a
    // console session, because the session exists so a BROWSER can be
    // authorized without one.
    ...(held ? { via: "key" as const } : {}),
    reachable: res.ok,
    orgName: res.ok ? res.body?.org_name : undefined,
    checkedAt: Date.now(),
  };
}

export async function connect(baseUrl: string, apiKey: string | null): Promise<ApiConnection> {
  // `null` means "leave the stored key alone"; clearing is an explicit "".
  update({
    apiBaseUrl: baseUrl.trim() || prefs().apiBaseUrl,
    ...(apiKey === null ? {} : { apiKey: apiKey === "" ? null : apiKey }),
  });
  return connection();
}


/** Forget the stored platform key, leaving the API address alone.
 *
 *  What "signing out" means for this host. There is no session to revoke: the
 *  shell authorizes with the key, so dropping it is the whole of it. The
 *  address stays so the console still knows where to sign back in to.
 *
 *  A loopback API still answers afterwards, because `request` falls back to the
 *  dev key for localhost. That is deliberate and `connection()` reports it
 *  honestly rather than claiming a signed-out state the next call would
 *  contradict. */
export async function signOut(): Promise<ApiConnection> {
  update({ apiKey: null });
  return connection();
}
