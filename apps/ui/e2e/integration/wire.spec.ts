import { test, expect, type Page, type Request, type Response } from "@playwright/test";
import { execFileSync } from "node:child_process";
import { resolve } from "node:path";

// Live-backend E2E for H1b. Requires: the compose dev stack (Valkey included:
// the console session routes are rate limited through it), apps/api uvicorn,
// and CURIE_API_TARGET pointing at it (the preview proxies /api there). Run:
//   PW_INTEGRATION=1 pnpm exec playwright test --project=integration
//
// Since #1047 the browser never holds the platform key. The key below lives in
// this Node test harness only: it mints a console login code (the CLI's job in
// production) and reads back server state. The browser signs in through the
// login screen and every UI call rides the HttpOnly session cookie.

const API = process.env.CURIE_API_TARGET ?? "http://localhost:8000";
const API_KEY = process.env.CURIE_API_KEY ?? "curie-dev-key";
const REPO_ROOT = resolve(process.cwd(), "../..");
const SESSION_COOKIE = "__Host-curie_console_session";

const api = (path: string, init?: RequestInit) =>
  fetch(`${API}${path}`, { ...init, headers: { "X-API-Key": API_KEY, ...(init?.headers ?? {}) } });

// A Slack-shaped channel ID, unique per call. The API validates Slack addresses
// against ^[CDG][A-Z0-9]{7,}$ (apps/api/src/curie_api/schemas.py) and refuses a
// second agent bound to the same channel, so a fixed ID fails on the second run.
function slackChannelId(): string {
  const stamp = Date.now().toString(36).toUpperCase();
  const salt = Math.floor(Math.random() * 36 * 36)
    .toString(36)
    .toUpperCase()
    .padStart(2, "0");
  return `C${stamp}${salt}`;
}

// POST /console/login-codes (routers/console.py `create_login_code`): platform
// key only, body ConsoleLoginCodeMint `{ subject }`, 201 ConsoleLoginCodeOut
// `{ code, subject, expires_at }`. Single use, so mint one per sign-in.
async function mintLoginCode(): Promise<string> {
  const resp = await api("/console/login-codes", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ subject: "wire-spec@example.com" }),
  });
  expect(resp.status, "login code mint").toBe(201);
  const body = await resp.json();
  expect(typeof body.code).toBe("string");
  return body.code;
}

async function submitLoginCode(page: Page) {
  await expect(page.getByTestId("console-login")).toBeVisible({ timeout: 15_000 });
  await page.getByLabel("login code").fill(await mintLoginCode());
  await page.getByTestId("console-login-submit").click();
  await expect(page.getByRole("navigation")).toBeVisible({ timeout: 15_000 });
  await expect(page.getByTestId("console-login")).toHaveCount(0);
}

// Open the console signed out, sign in through the login screen, and prove the
// browser now holds the real session cookie and no platform key. Returns the
// live request log so a test can re-check it after its own traffic.
async function signIn(page: Page, path = "/?api=1"): Promise<Request[]> {
  const requests: Request[] = [];
  page.on("request", (req) => requests.push(req));
  await page.goto(path);
  await submitLoginCode(page);
  await expectCookieSession(page, requests);
  return requests;
}

async function expectCookieSession(page: Page, requests: Request[]) {
  const cookie = (await page.context().cookies()).find((c) => c.name === SESSION_COOKIE);
  expect(cookie, `${SESSION_COOKIE} is stored`).toBeTruthy();
  expect(cookie?.httpOnly).toBe(true);
  expect(cookie?.secure).toBe(true);
  expect(cookie?.sameSite).toBe("Strict");
  expect(page.url()).not.toContain("api_key");
  for (const req of requests) {
    expect(req.url(), req.url()).not.toContain("api_key");
    expect(req.headers()["x-api-key"], `${req.method()} ${req.url()}`).toBeUndefined();
  }
}

// Seed one OTLP trace and wait until it surfaces (with observations) through the
// API proxy, so the Runs assertions have a real span tree to render.
async function seedTrace(): Promise<string> {
  const out = execFileSync("uv", ["run", "python", "apps/ui/e2e/integration/seed_trace.py"], {
    cwd: REPO_ROOT,
    encoding: "utf8",
  });
  const traceId = out.trim().split("\n").pop()!.trim();
  const deadline = Date.now() + 90_000;
  while (Date.now() < deadline) {
    const resp = await api(`/langfuse/traces/${traceId}`);
    if (resp.status === 200) {
      const body = await resp.json();
      if (Array.isArray(body.tree) && body.tree.length > 0) return traceId;
    }
    await new Promise((r) => setTimeout(r, 3000));
  }
  throw new Error(`seeded trace ${traceId} never surfaced through the proxy`);
}

test.beforeAll(async () => {
  // Ensure a real trace exists and has propagated through the proxy before the
  // Runs assertions read it. The UI matches it by name (h1b-ui-wire-demo).
  await seedTrace();
});

test("create agent -> Deploy -> version + stored bundle exist via API", async ({ page }) => {
  const agentName = `dealdesk-${Date.now()}`;
  const channel = slackChannelId();
  const requests = await signIn(page);
  const responses: Response[] = [];
  page.on("response", (resp) => responses.push(resp));

  await page.getByRole("navigation").getByText("Agents", { exact: true }).click();
  await page.getByRole("button", { name: /New agent/ }).click();
  await page.getByTestId("agent-name").fill(agentName);
  await page.getByTestId("agent-channel").fill(channel);
  await page.getByRole("button", { name: "Deploy" }).click();

  // Honest post-deploy panel proves the whole chain (create agent + version +
  // bundle) ran and names the real next step.
  const panel = page.getByTestId("deployed-panel");
  await expect(panel).toBeVisible({ timeout: 15_000 });
  await expect(panel).toContainText(agentName);
  await expect(panel).toContainText(channel);

  // The cookie carried the unsafe writes past the API's origin check: the
  // JSON POST and the multipart bundle PUT both succeeded through the proxy.
  const sent = (method: string, re: RegExp) =>
    responses.filter((r) => r.request().method() === method && re.test(new URL(r.url()).pathname));
  const createAgent = sent("POST", /^\/api\/agents$/);
  expect(createAgent.map((r) => r.status())).toEqual([201]);
  const bundlePut = sent("PUT", /^\/api\/agents\/[^/]+\/versions\/[^/]+\/bundle$/);
  expect(bundlePut).toHaveLength(1);
  expect(bundlePut[0].ok(), `bundle PUT ${bundlePut[0].status()}`).toBe(true);
  expect(bundlePut[0].request().headers()["content-type"]).toMatch(/^multipart\/form-data; boundary=/);
  await expectCookieSession(page, requests);

  // Verify server-side: the agent, its version, and a stored bundle all exist.
  const agents = await (await api("/agents")).json();
  const agent = agents.find((a: { name: string }) => a.name === agentName);
  expect(agent, "created agent is listed by the API").toBeTruthy();

  const versions = await (await api(`/agents/${agent.id}/versions`)).json();
  expect(versions.length).toBeGreaterThan(0);
  const version = versions[0];
  expect(version.bundle_ref, "version has a stored bundle_ref").toBeTruthy();
  expect(version.bundle_sha256, "version has a bundle sha256").toBeTruthy();

  const bundleResp = await api(`/agents/${agent.id}/versions/${version.id}/bundle`);
  expect(bundleResp.status, "bundle bytes are fetchable").toBe(200);
  const bytes = await bundleResp.arrayBuffer();
  expect(bytes.byteLength).toBeGreaterThan(0);
});

test("a malformed skill.md surfaces the validator error inline", async ({ page }) => {
  const agentName = `broken-${Date.now()}`;
  const requests = await signIn(page);

  await page.getByRole("navigation").getByText("Agents", { exact: true }).click();
  await page.getByRole("button", { name: /New agent/ }).click();
  await page.getByTestId("agent-name").fill(agentName);
  // A valid, unused channel: the agent is created before the bundle is
  // validated, so only the SKILL.md can be what fails.
  await page.getByTestId("agent-channel").fill(slackChannelId());
  // No YAML frontmatter -> the plugin_format validator rejects the SKILL.md.
  await page.getByTestId("skill-editor").fill("this skill has no frontmatter at all");
  await page.getByRole("button", { name: "Deploy" }).click();

  const errors = page.getByTestId("deploy-errors");
  await expect(errors).toBeVisible({ timeout: 15_000 });
  await expect(errors).toContainText(/skill|frontmatter|manifest/i);
  // The modal stays open and the (new) success panel never appears on failure.
  await expect(page.getByTestId("deployed-panel")).toHaveCount(0);
  await expectCookieSession(page, requests);
});

test("Runs tab lists the seeded trace and drill-in renders its span tree", async ({ page }) => {
  const requests = await signIn(page);
  await page.getByRole("navigation").getByText("Observability", { exact: true }).click();

  // The live traces list shows real Langfuse traces.
  const row = page.getByTestId("trace-row").filter({ hasText: "h1b-ui-wire-demo" }).first();
  await expect(row).toBeVisible({ timeout: 15_000 });
  await row.click();

  // Drill-in reconstructs the observation tree; the model span maps to GENERATION.
  const tree = page.getByTestId("span-tree");
  await expect(tree).toBeVisible();
  await expect(tree).toContainText("GENERATION");
  await expect(tree).toContainText(/execute_tool|salesforce|slack/i);
  await expectCookieSession(page, requests);
});

test("a request without a console session is refused", async ({ page, request }) => {
  // Straight at the API: no key, no cookie (auth.py `require_api_key`).
  const direct = await fetch(`${API}/agents`);
  expect(direct.status).toBe(401);

  // Through the preview proxy from a fresh request context with no cookie jar.
  const proxied = await request.get("/api/agents");
  expect(proxied.status()).toBe(401);

  // In the browser: refused before sign-in, accepted after, on the same page.
  const browserStatus = () => page.evaluate(async () => (await fetch("/api/agents")).status);
  await page.goto("/?api=1");
  await expect(page.getByTestId("console-login")).toBeVisible({ timeout: 15_000 });
  expect(await browserStatus()).toBe(401);
  await submitLoginCode(page);
  expect(await browserStatus()).toBe(200);
});
