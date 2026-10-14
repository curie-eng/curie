import { test, expect, type Page, type Route } from "@playwright/test";
import { SESSION_EXPIRES_AT } from "./support/consoleSession";

// The console login gate (#1047, ADR-0083) in the stackless suite. The browser
// never holds the platform key: it probes GET /console/session, and when that
// 401s it shows a login screen that exchanges a CLI-minted code through
// POST /console/session. Every stub mirrors apps/api/src/curie_api/routers/console.py
// (bodies and statuses) and apps/api/src/curie_api/rate_limit.py (429/503).
// The real Set-Cookie cannot be stored here (a Secure __Host- cookie on an http
// preview); the integration suite proves the cookie with the real API.

type Json = Record<string, unknown> | unknown[];

const json = (route: Route, status: number, body: Json, headers: Record<string, string> = {}) =>
  route.fulfill({ status, contentType: "application/json", headers, body: JSON.stringify(body) });

// routers/console.py `current_session`: 401 with this detail for a missing,
// unknown, expired or subject-less session.
const NO_SESSION = { detail: "missing, invalid, or expired console session" };
// routers/console.py `exchange_login_code`: every rejection is this one 401.
const BAD_CODE = { detail: "invalid or expired login code" };
// rate_limit.py `require_rate_limit`.
const RATE_LIMITED = { detail: "rate limit exceeded" };
const LIMITER_DOWN = { detail: "rate limiter unavailable" };
// auth.py `require_api_key` refusal (the shared console API boundary).
const NO_CREDENTIAL = { detail: "missing or invalid API key" };

const SUBJECT = "operator@example.com";
const AGENT = { id: "a1", name: "deal-desk", channels: [{ kind: "slack", address: "C0123ABCD" }], model: null, created_at: "2026-07-01T00:00:00Z" };

type Reply = { status: number; body: Json; headers?: Record<string, string> };
const OK_SESSION: Reply = { status: 200, body: { subject: SUBJECT, expires_at: SESSION_EXPIRES_AT } };

interface ApiRecord {
  seq: number;
  method: string;
  path: string;
  url: string;
  headers: Record<string, string>;
  postData: string | null;
}

// One mutable fake of the session boundary plus a request log. `signedIn` is
// the server's view of the cookie: a successful exchange flips it, exactly as
// the real Set-Cookie would make the next GET /console/session succeed.
interface Harness {
  signedIn: boolean;
  // Queue of exchange replies; when empty, a code exchange succeeds.
  exchangeReplies: Reply[];
  log: ApiRecord[];
  sessionGets: () => ApiRecord[];
  exchanges: () => ApiRecord[];
  hits: (method: string, path: string) => ApiRecord[];
}

async function stubConsole(page: Page, opts: { signedIn: boolean }): Promise<Harness> {
  const h: Harness = {
    signedIn: opts.signedIn,
    exchangeReplies: [],
    log: [],
    sessionGets: () => h.hits("GET", "/api/console/session"),
    exchanges: () => h.hits("POST", "/api/console/session"),
    hits: (method, path) => h.log.filter((r) => r.method === method && r.path === path),
  };

  // Registered first, so consulted last: any /api call this file did not stub
  // gets FastAPI's 404 instead of leaking to whatever listens behind the preview
  // proxy (a real API there would 401 and fire the gate's re-probe).
  await page.route(
    (url) => url.pathname.startsWith("/api/"),
    (route) => json(route, 404, { detail: "Not Found" }),
  );

  await page.route(
    (url) => url.pathname === "/api/console/session",
    (route) => {
      if (route.request().method() === "POST") {
        const reply = h.exchangeReplies.shift();
        if (reply) return json(route, reply.status, reply.body, { "Cache-Control": "no-store", ...reply.headers });
        h.signedIn = true;
        return json(route, OK_SESSION.status, OK_SESSION.body, { "Cache-Control": "no-store" });
      }
      return h.signedIn
        ? json(route, 200, OK_SESSION.body, { "Cache-Control": "no-store" })
        : json(route, 401, NO_SESSION, { "Cache-Control": "no-store" });
    },
  );
  // routers/config.py: the open org-name endpoint.
  await page.route(
    (url) => url.pathname === "/api/config",
    (route) => json(route, 200, { org_name: "Globex Corporation" }),
  );
  return h;
}

// Registered LAST so it runs first: logs every /api request (with all headers,
// including the ones Request.headers() hides) and falls through to the stubs.
async function recordApi(page: Page, h: Harness) {
  let seq = 0;
  await page.route(
    (url) => url.pathname.startsWith("/api/"),
    async (route) => {
      const req = route.request();
      h.log.push({
        seq: ++seq,
        method: req.method(),
        path: new URL(req.url()).pathname,
        url: req.url(),
        headers: await req.allHeaders(),
        postData: req.postData(),
      });
      return route.fallback();
    },
  );
}

async function signInWith(page: Page, code: string) {
  const input = page.getByLabel("login code");
  await input.fill("");
  await input.fill(code);
  await page.getByTestId("console-login-submit").click();
}

test("T12: an unauthenticated visit shows the login screen and loads nothing behind it", async ({ page }) => {
  const h = await stubConsole(page, { signedIn: false });
  await page.route(/\/api\/agents(\?.*)?$/, (route) => json(route, 200, [AGENT]));
  await recordApi(page, h);

  await page.goto("/?api=1");

  await expect(page.getByTestId("console-login")).toBeVisible();
  await expect(page.getByRole("navigation")).toHaveCount(0);
  // The gate probed, and nothing behind it ran.
  expect(h.sessionGets().length).toBeGreaterThan(0);
  expect(h.hits("GET", "/api/agents")).toHaveLength(0);
  expect(h.log.filter((r) => r.path !== "/api/console/session")).toEqual([]);
});

test("T13: a bad code shows the server's refusal and stays on the login screen", async ({ page }) => {
  const h = await stubConsole(page, { signedIn: false });
  await recordApi(page, h);
  h.exchangeReplies.push({ status: 401, body: BAD_CODE });

  await page.goto("/?api=1");
  await signInWith(page, "not-a-real-code");

  await expect(page.getByTestId("console-login-error")).toContainText("invalid or expired login code");
  await expect(page.getByTestId("console-login")).toBeVisible();
  await expect(page.getByRole("navigation")).toHaveCount(0);
  expect(h.exchanges()).toHaveLength(1);
});

test("T14: a good code enters the console, posting only the code", async ({ page }) => {
  const h = await stubConsole(page, { signedIn: false });
  await page.route(/\/api\/agents(\?.*)?$/, (route) => json(route, 200, []));
  await recordApi(page, h);

  await page.goto("/?api=1");
  await expect(page.getByTestId("console-login")).toBeVisible();
  await signInWith(page, "minted-code-example");

  await expect(page.getByRole("navigation")).toBeVisible();
  await expect(page.getByText("Welcome to Curie")).toBeVisible();
  await expect(page.getByTestId("console-login")).toHaveCount(0);

  const posts = h.exchanges();
  expect(posts).toHaveLength(1);
  expect(JSON.parse(posts[0].postData ?? "null")).toEqual({ code: "minted-code-example" });
  expect(posts[0].headers["x-api-key"]).toBeUndefined();
  await expect.poll(() => h.hits("GET", "/api/agents").length).toBeGreaterThan(0);
});

test("T15: an api_key URL parameter never authenticates and never reaches an API request", async ({ page }) => {
  const h = await stubConsole(page, { signedIn: false });
  await page.route(/\/api\/agents(\?.*)?$/, (route) => json(route, 200, [AGENT]));
  await recordApi(page, h);

  await page.goto("/?api=1&api_key=leak-example");
  // The parameter is not a credential: the gate still asks for a code.
  await expect(page.getByTestId("console-login")).toBeVisible();

  await signInWith(page, "minted-code-example");
  await expect(page.getByRole("navigation")).toBeVisible();
  await expect.poll(() => h.hits("GET", "/api/agents").length).toBeGreaterThan(0);
  await expect.poll(() => h.hits("GET", "/api/config").length).toBeGreaterThan(0);

  // Every /api request: no key in the URL, no x-api-key header, and no header
  // value carrying the parameter. Referer is excluded on purpose: it echoes the
  // address bar, which is #1048's leak proof, not this PR's.
  expect(h.log.length).toBeGreaterThan(0);
  for (const r of h.log) {
    expect(r.url, r.url).not.toMatch(/api_key|leak-example/);
    expect(Object.keys(r.headers).map((k) => k.toLowerCase()), r.url).not.toContain("x-api-key");
    for (const [name, value] of Object.entries(r.headers)) {
      if (name.toLowerCase() === "referer") continue;
      expect(value, `${r.method} ${r.path} header ${name}`).not.toMatch(/api_key|leak-example/);
    }
  }
});

function approval() {
  // ApprovalOut, apps/api/src/curie_api/schemas.py (same row approvals-wired.spec.ts uses).
  return {
    id: "ap-1",
    agent_id: "ag-1",
    conversation_id: "C-thread-1",
    author: "U-alice",
    summary: "Refund $4,200 to ACME Corp",
    reply_channel: "C0DEALS",
    reply_placeholder: "ts-1",
    reply_endpoint: null,
    dedupe_key: "dk-1",
    route: "managers",
    card_channel: "C0MANAGERS",
    gate_kind: "permission",
    granted_tool: "issue_refund",
    status: "pending",
    expires_at: "2026-07-24T00:00:00+00:00",
    resolved_by: null,
    resolution_note: null,
    created_at: "2026-07-23T00:00:00+00:00",
    resolved_at: null,
  };
}

// The Approvals toolbar's own Refresh (other panels have one too).
const approvalsRefresh = (page: Page) =>
  page.getByTestId("approvals-status-filter").locator("..").getByRole("button", { name: "Refresh" });

async function openApprovals(page: Page) {
  await page.getByRole("navigation").getByText("Observability", { exact: true }).click();
  await page.getByRole("button", { name: "Approvals" }).click();
}

test("T16: a session that expires mid-use returns to login, then a new code resumes work", async ({ page }) => {
  const h = await stubConsole(page, { signedIn: true });
  await page.route(/\/api\/agents(\?.*)?$/, (route) => json(route, 200, [AGENT]));
  let approvalsStatus = 200;
  const approvalStatuses: number[] = [];
  await page.route(
    (url) => url.pathname === "/api/approvals",
    (route) => {
      approvalStatuses.push(approvalsStatus);
      return approvalsStatus === 200 ? json(route, 200, [approval()]) : json(route, 401, NO_CREDENTIAL);
    },
  );
  await recordApi(page, h);

  await page.goto("/?api=1");
  await openApprovals(page);
  await expect(page.getByTestId("approval-summary")).toContainText("Refund $4,200 to ACME Corp");
  const okBefore = approvalStatuses.length;
  expect(approvalStatuses).not.toContain(401);

  // The server revokes the session: the next real request the user sends 401s.
  approvalsStatus = 401;
  h.signedIn = false;
  const probesBefore = h.sessionGets().length;
  await approvalsRefresh(page).click();

  // The refusal was actually exercised, and the gate re-probed the session.
  await expect.poll(() => approvalStatuses.filter((s) => s === 401).length).toBe(1);
  await expect.poll(() => h.sessionGets().length).toBeGreaterThan(probesBefore);
  await expect(page.getByTestId("console-login")).toBeVisible();
  await expect(page.getByTestId("console-login-notice")).toContainText("expired or was revoked");
  await expect(page.getByRole("navigation")).toHaveCount(0);

  // Recovery: a fresh code signs in (the exchange flips the session back on),
  // and the same protected read succeeds again.
  approvalsStatus = 200;
  await signInWith(page, "second-code-example");
  await expect(page.getByRole("navigation")).toBeVisible();
  expect(h.exchanges()).toHaveLength(1);
  await openApprovals(page);
  await expect(page.getByTestId("approval-summary")).toContainText("Refund $4,200 to ACME Corp");
  await approvalsRefresh(page).click();
  await expect.poll(() => approvalStatuses.length).toBeGreaterThan(okBefore + 2);
  const afterRefusal = approvalStatuses.slice(approvalStatuses.indexOf(401) + 1);
  expect(afterRefusal.length).toBeGreaterThan(0);
  expect(afterRefusal.every((s) => s === 200)).toBe(true);
  await expect(page.getByTestId("console-login")).toHaveCount(0);
});

const REFUSALS: { name: string; reply: Reply; message: RegExp }[] = [
  { name: "a 401 bad code", reply: { status: 401, body: BAD_CODE }, message: /invalid or expired login code/ },
  {
    name: "a 429 rate limit",
    reply: { status: 429, body: RATE_LIMITED, headers: { "Retry-After": "60" } },
    message: /Too many sign-in attempts/,
  },
  { name: "a 503 limiter outage", reply: { status: 503, body: LIMITER_DOWN }, message: /temporarily unavailable/ },
];

for (const { name, reply, message } of REFUSALS) {
  test(`T21: after ${name}, the same form signs in with the next code`, async ({ page }) => {
    const h = await stubConsole(page, { signedIn: false });
    await page.route(/\/api\/agents(\?.*)?$/, (route) => json(route, 200, [AGENT]));
    await recordApi(page, h);
    h.exchangeReplies.push(reply);

    await page.goto("/?api=1");
    await signInWith(page, "first-code-example");

    await expect(page.getByTestId("console-login-error")).toContainText(message);
    await expect(page.getByTestId("console-login-submit")).toBeEnabled();
    expect(h.exchanges()).toHaveLength(1);

    // Same page, same form, no reload.
    await signInWith(page, "second-code-example");
    await expect(page.getByRole("navigation")).toBeVisible();
    await expect(page.getByTestId("console-login")).toHaveCount(0);
    const posts = h.exchanges();
    expect(posts).toHaveLength(2);
    expect(posts.map((p) => JSON.parse(p.postData ?? "null"))).toEqual([
      { code: "first-code-example" },
      { code: "second-code-example" },
    ]);
  });
}

test("T22: a state read refused for the session shows an honest notice and keeps the user signed in", async ({ page }) => {
  const h = await stubConsole(page, { signedIn: true });
  // Agent-detail scaffolding (same shapes as behavior-packs-wired.spec.ts).
  await page.route("**/api/agents/*/versions/*/files*", (route) =>
    json(route, 200, { files: [{ path: "skills/deal-desk/SKILL.md", content: "# Policy" }] }),
  );
  await page.route("**/api/agents/*/versions", (route) =>
    json(route, 200, [
      { id: "v1", agent_id: "a1", version_label: "v0.1.0", bundle_ref: "b", bundle_sha256: "s", commit_sha: null, created_by: "ui", created_at: "2026-07-01T00:00:00Z" },
    ]),
  );
  await page.route("**/api/deployments*", (route) =>
    json(route, 200, [
      { id: "d1", agent_id: "a1", version_id: "v1", environment: "prod", commit_sha: null, status: "active", deployed_at: "2026-07-01T00:00:00Z" },
    ]),
  );
  await page.route("**/api/agents/*/memory*", (route) => json(route, 200, []));
  // routers/state.py `require_state_access`: a console session is not accepted.
  await page.route(
    (url) => /^\/api\/agents\/[^/]+\/state(\/.*)?$/.test(url.pathname),
    (route) => json(route, 401, { detail: "missing or invalid credential" }),
  );
  await page.route(/\/api\/agents(\?.*)?$/, (route) => json(route, 200, [AGENT]));
  await recordApi(page, h);

  await page.goto("/?api=1");
  await page.getByRole("navigation").getByText("Agents", { exact: true }).click();
  await page.getByTestId("agent-card-name").click();

  await expect(page.getByTestId("state-unavailable")).toBeVisible();
  const stateHits = h.log.filter((r) => /^\/api\/agents\/[^/]+\/state/.test(r.path));
  expect(stateHits.length).toBeGreaterThan(0);
  const firstRefusal = stateHits[0].seq;
  // The re-probe ran after the refusal and found the session live.
  await expect.poll(() => h.sessionGets().some((r) => r.seq > firstRefusal)).toBe(true);

  await expect(page.getByText("has not stored any durable state")).toHaveCount(0);
  await expect(page.getByTestId("console-login")).toHaveCount(0);
  await expect(page.getByRole("navigation")).toBeVisible();
});
