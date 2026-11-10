import { test, expect, type Page } from "@playwright/test";

// Run against the real API, Postgres and Valkey through the production preview.
// The platform key is used only by this Node harness to mint the CLI's code.
// No route interception, session injection or trace seed is used here.
const API = process.env.CURIE_API_TARGET ?? "http://localhost:8000";
const PLATFORM_KEY = process.env.CURIE_API_KEY ?? "curie-dev-key";
const SESSION_COOKIE = "__Host-curie_console_session";
const SUBJECT = "console-proof@example.com";

interface UrlObservation {
  kind: string;
  url: string;
}

interface RequestObservation {
  url: string;
  headers: Record<string, string>;
  postData?: Buffer | null;
}

interface ResponseBodyObservation {
  url: string;
  status: number;
  json: boolean;
  body: Buffer | null;
}

function expectCredentialFreeUrl(raw: string, values: string[], label: string) {
  const url = new URL(raw);
  const credentialNames = /^(?:api[-_]?key|platform[-_]?key|(?:console[-_]?)?login[-_]?code|code|(?:console[-_]?)?session(?:[-_]?token)?|token|access[-_]?token|__Host-curie_console_session)$/i;
  expect(
    [...url.searchParams.keys()].some((name) => credentialNames.test(name)),
    `${label} has no credential query parameter`,
  ).toBe(false);
  // Check decoded query values too, so percent encoding cannot hide a leak.
  const parts = [raw, ...url.searchParams.values(), url.hash];
  for (const value of values) {
    expect(
      parts.some((part) => part.includes(value) || part.includes(encodeURIComponent(value))),
      `${label} contains no platform key, session token or login code`,
    ).toBe(false);
  }
}

async function finishResponseBodies(page: Page, snapshots: Promise<ResponseBodyObservation>[]) {
  await page.waitForLoadState("networkidle");
  const bodies = await Promise.all(snapshots);
  for (const [index, response] of bodies.entries()) {
    expect(response.body !== null,
      `API response ${index} (${new URL(response.url).pathname}, status ${response.status}) body snapshot completed`).toBe(true);
  }
}

async function waitForLiveShell(
  page: Page,
  responseBodies: Promise<ResponseBodyObservation>[],
  action: () => Promise<unknown>,
) {
  const successfulGet = (path: string) => page.waitForResponse(
    (response) => new URL(response.url()).pathname === path
      && response.request().method() === "GET" && response.status() === 200,
  );
  const agents = successfulGet("/api/agents");
  const config = successfulGet("/api/config");
  await action();
  const responses = await Promise.all([agents, config]);
  // Full headers expose Cookie. Capture only these completed, successful
  // requests while their document is still alive, before the next navigation.
  const authenticated = await Promise.all(responses.map(async (response) => ({
    url: response.url(),
    headers: await response.request().allHeaders(),
  })));
  await expect(page.getByRole("navigation")).toBeVisible();
  await expect(page.getByTestId("console-login")).toHaveCount(0);
  await finishResponseBodies(page, responseBodies);
  return authenticated;
}

test("real console login, views and browser history never expose credentials", async ({ page, context }) => {
  test.setTimeout(60_000);
  expect(PLATFORM_KEY.length, "harness platform key is nonempty").toBeGreaterThan(0);
  const requests: RequestObservation[] = [];
  const authenticated: RequestObservation[] = [];
  const responseBodies: Promise<ResponseBodyObservation>[] = [];
  const urls: UrlObservation[] = [];
  // Synchronous headers include Referer and X-API-Key, even for requests
  // canceled by navigation. Later inspection never contacts a retired frame.
  context.on("request", (request) => requests.push({
    url: request.url(),
    headers: { ...request.headers() },
    postData: request.postDataBuffer(),
  }));
  // requestfinished means the response download completed. Start the body
  // snapshot immediately, including failed HTTP statuses, while its frame lives.
  // A capture failure stays an assertion failure without logging a secret body.
  context.on("requestfinished", (request) => {
    const url = request.url();
    if (!new URL(url).pathname.startsWith("/api/")) return;
    responseBodies.push(request.response().then(async (response) => ({
      url,
      status: response?.status() ?? 0,
      json: /application\/json/i.test(response?.headers()["content-type"] ?? ""),
      body: response ? await response.body() : null,
    })).catch(() => ({ url, status: 0, json: false, body: null })));
  });
  page.on("framenavigated", (frame) => urls.push({ kind: "navigation", url: frame.url() }));

  await context.exposeBinding("recordConsoleProofUrl", (_source, observation: UrlObservation) => {
    urls.push(observation);
  });
  await context.addInitScript(() => {
    const observedWindow = window as Window & {
      recordConsoleProofUrl: (observation: { kind: string; url: string }) => Promise<void>;
    };
    const record = (kind: string, url = location.href) => {
      void observedWindow.recordConsoleProofUrl({ kind, url });
    };
    // Record targets before mutation, including intermediate URLs overwritten
    // in the same turn before an ordinary page.url() check could observe them.
    const pushState = history.pushState.bind(history);
    history.pushState = (data, unused, url) => {
      record("pushState", url == null ? location.href : new URL(String(url), location.href).href);
      pushState(data, unused, url);
    };
    const replaceState = history.replaceState.bind(history);
    history.replaceState = (data, unused, url) => {
      record("replaceState", url == null ? location.href : new URL(String(url), location.href).href);
      replaceState(data, unused, url);
    };
    record("initial");
    addEventListener("popstate", () => record("popstate"));
    addEventListener("hashchange", () => record("hashchange"));
    addEventListener("pageshow", () => record("pageshow"));
  });

  // auth.py require_api_key refuses the real same-origin resource without a
  // session. This context is fresh and carries no platform key or cookie.
  expect((await page.request.get("/api/agents")).status(), "agents requires authentication").toBe(401);
  await page.goto("/?api=1");
  await expect(page.getByTestId("console-login")).toBeVisible();

  // routers/console.py create_login_code: platform-key-only, 201 with
  // { code, subject, expires_at }; exchange_login_code accepts { code }.
  const mint = await fetch(`${API}/console/login-codes`, {
    method: "POST",
    headers: { "X-API-Key": PLATFORM_KEY, "Content-Type": "application/json" },
    body: JSON.stringify({ subject: SUBJECT }),
  });
  expect(mint.status, "real login code is minted").toBe(201);
  const minted = await mint.json() as { code: string; subject: string; expires_at: string };
  expect(typeof minted.code, "mint returned a code").toBe("string");
  expect(minted.code.length, "minted code is nonempty").toBeGreaterThan(0);
  expect(minted.subject).toBe(SUBJECT);

  await page.getByLabel("login code").fill(minted.code);
  const exchanged = page.waitForResponse(
    (response) => new URL(response.url()).pathname === "/api/console/session"
      && response.request().method() === "POST",
  );
  authenticated.push(...await waitForLiveShell(page, responseBodies, () => page.getByTestId("console-login-submit").click()));
  const exchange = await exchanged;
  expect(exchange.status(), "real code exchange succeeds").toBe(200);
  const submitted = exchange.request().postDataJSON() as Record<string, unknown>;
  expect(Object.keys(submitted), "only the code field is posted").toEqual(["code"]);
  expect(submitted.code === minted.code, "the minted code is posted").toBe(true);
  const identity = await exchange.json();
  expect(Object.keys(identity).sort(), "exchange body contains identity and expiry only").toEqual(["expires_at", "subject"]);
  expect(identity.subject).toBe(SUBJECT);

  // approval_auth.py set_console_session_cookie pins all these properties.
  // Playwright's Node cookie inspection can see the token; page script cannot.
  const session = (await context.cookies()).find((cookie) => cookie.name === SESSION_COOKIE);
  expect(session, "real session cookie is installed").toBeDefined();
  expect(session!.value.length, "session token is nonempty").toBeGreaterThan(0);
  expect(session!.httpOnly).toBe(true);
  expect(session!.secure).toBe(true);
  expect(session!.sameSite).toBe("Strict");
  expect(session!.path).toBe("/");
  const visibleCookies = await page.evaluate(() => document.cookie);
  expect(visibleCookies.includes(SESSION_COOKIE), "page script cannot read the session cookie").toBe(false);
  expect(visibleCookies.includes(session!.value), "page script cannot read the session token").toBe(false);

  for (const view of ["Agents", "Work items", "Evals"]) {
    const navigation = page.getByRole("navigation");
    await navigation.getByText(view, { exact: true }).click();
    await expect(navigation.getByRole("button", { name: new RegExp(`^${view}(?:\\s|$)`) }))
      .toHaveAttribute("aria-current", "page");
    await page.waitForLoadState("networkidle");
    urls.push({ kind: `view ${view}`, url: page.url() });
  }

  await finishResponseBodies(page, responseBodies);
  authenticated.push(...await waitForLiveShell(page, responseBodies, () => page.reload()));
  const firstUrl = page.url();
  // Views use in-memory navigation, so create another real document entry
  // before exercising browser back/forward. Both URLs are credential-free.
  await finishResponseBodies(page, responseBodies);
  authenticated.push(...await waitForLiveShell(page, responseBodies, () => page.goto("/")));
  const secondUrl = page.url();
  expect(secondUrl === firstUrl, "two distinct document history entries exist").toBe(false);
  await finishResponseBodies(page, responseBodies);
  await page.goBack();
  await expect(page).toHaveURL(firstUrl);
  await expect(page.getByRole("navigation")).toBeVisible();
  await finishResponseBodies(page, responseBodies);
  await page.goForward();
  await expect(page).toHaveURL(secondUrl);
  await expect(page.getByRole("navigation")).toBeVisible();
  await finishResponseBodies(page, responseBodies);
  expect((await context.cookies()).some((cookie) => cookie.name === SESSION_COOKIE && cookie.value === session!.value),
    "same session survives reload and history navigation").toBe(true);

  const values = [...new Set([PLATFORM_KEY, "curie-dev-key", minted.code, session!.value])];
  await expect.poll(() => urls.filter((entry) => entry.kind === "initial").length)
    .toBeGreaterThanOrEqual(3);
  expect(urls.some((entry) => entry.kind === "navigation"), "document navigations were observed").toBe(true);
  expect(urls.some((entry) => entry.kind === "pageshow"), "page history lifecycle was observed").toBe(true);
  for (const [index, entry] of urls.entries()) {
    expectCredentialFreeUrl(entry.url, values, `history ${index} (${entry.kind})`);
  }

  expect(requests.length, "browser requests were observed").toBeGreaterThan(0);
  const platformKeys = [...new Set([PLATFORM_KEY, "curie-dev-key"])];
  let referrers = 0;
  for (const [index, request] of requests.entries()) {
    expectCredentialFreeUrl(request.url, values, `request ${index}`);
    const headers = request.headers;
    expect(Object.keys(headers).some((name) => name.toLowerCase() === "x-api-key"),
      `request ${index} has no platform-key header`).toBe(false);
    if (headers.referer) {
      referrers += 1;
      expectCredentialFreeUrl(headers.referer, values, `request ${index} Referer`);
    }
    // The login code belongs in the exchange POST body. Only the immutable
    // platform key is forbidden in request and response bodies.
    for (const key of platformKeys) {
      expect(request.postData?.includes(Buffer.from(key)) ?? false,
        `request ${index} body contains no platform key`).toBe(false);
    }
  }
  expect(referrers, "real browser Referer headers were observed").toBeGreaterThan(0);
  expect(requests.some((request) => new URL(request.url).pathname === "/api/console/session"
    && (request.postData?.length ?? 0) > 0), "real login POST body was inspected").toBe(true);

  const bodies = await Promise.all(responseBodies);
  expect(bodies.length, "completed API response bodies were inspected").toBeGreaterThan(0);
  for (const [index, response] of bodies.entries()) {
    expect(response.body !== null, `API response ${index} body snapshot completed`).toBe(true);
    for (const key of platformKeys) {
      expect(response.body?.includes(Buffer.from(key)) ?? false,
        `API response ${index} body contains no platform key`).toBe(false);
    }
  }

  // WiredProvider reads both routes on sign-in, reload and document navigation.
  // /config is open, but it must still carry the ambient cookie. /agents is
  // protected, so its 200 proves the cookie authenticated a real UI request.
  for (const path of ["/api/agents", "/api/config"]) {
    expect(bodies.some((response) => new URL(response.url).pathname === path
      && response.status === 200 && response.json && (response.body?.length ?? 0) > 0),
    `${path} has an inspected successful JSON response body`).toBe(true);
    const successful = authenticated.filter((request) => new URL(request.url).pathname === path);
    expect(successful.length, `${path} succeeded across real document loads`).toBeGreaterThanOrEqual(3);
    for (const request of successful) {
      expect(request.headers.cookie?.split(/;\s*/).includes(`${SESSION_COOKIE}=${session!.value}`),
        `${path} request carries the real session cookie`).toBe(true);
    }
  }
});
