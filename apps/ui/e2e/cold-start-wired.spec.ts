import { test, expect, type Page } from "@playwright/test";

// The cold-start loop (H2 backend-driven shell), stackless via route stubs:
// empty DB -> onboarding -> create an agent -> it appears in the real Agents
// list -> honest post-deploy copy -> no fixture agent (deal-desk) anywhere in
// wired mode (#542 removed the fixture/demo world entirely).

function json(status: number, body: unknown) {
  return { status, contentType: "application/json", body: JSON.stringify(body) };
}

const SUMMARY = {
  start: "2026-06-28",
  end: "2026-07-05",
  runs: 0,
  latency_p95_ms: 0,
  tokens: 0,
  cost_usd: 0,
  error_rate: 0,
};

// Mutable agent list: starts empty, gains the created agent after POST /agents.
// `posted` records the raw create bodies so a test can assert exactly what the UI
// sent (e.g. the channel is trimmed before it hits the backend).
async function stubBackend(page: Page) {
  const agents: {
    id: string;
    name: string;
    // The console still creates with a singular `channel` on the wire (plan
    // keeps create-binds-one), but AgentOut -- what GET /agents returns and
    // the Agents list renders -- carries `channels` (plural, ADR-0116).
    channels: { kind: string; address: string }[];
    created_at: string;
  }[] = [];
  const posted: { name: string; channel: { kind: string; address: string } }[] = [];

  await page.route(/\/api\/agents(\?.*)?$/, async (route) => {
    if (route.request().method() === "POST") {
      const body = JSON.parse(route.request().postData() ?? "{}");
      posted.push(body);
      const agent = { id: "ag-" + (agents.length + 1), name: body.name, channels: [body.channel], created_at: "2026-07-05T00:00:00Z" };
      agents.push(agent);
      return route.fulfill(json(201, agent));
    }
    return route.fulfill(json(200, agents));
  });
  await page.route("**/api/agents/*/versions", (route) =>
    route.fulfill(json(201, { id: "v1", agent_id: "ag-1", version_label: "v0.1.0", bundle_ref: null, bundle_sha256: null, created_by: "ui", created_at: "now" })),
  );
  await page.route("**/api/agents/*/versions/*/bundle", (route) =>
    route.fulfill(json(201, { version_id: "v1", bundle_ref: "bundles/ag-1/v1.zip", bundle_sha256: "abc", size_bytes: 123 })),
  );
  await page.route("**/api/observability/metrics/summary*", (route) => route.fulfill(json(200, SUMMARY)));
  await page.route("**/api/langfuse/traces*", (route) => route.fulfill(json(200, [])));
  // A fresh workspace has no eval runs: the matrix comes back empty, so the
  // wired Evals view degrades to its honest empty state (no fixture cases).
  await page.route("**/api/evals/matrix*", (route) =>
    route.fulfill(json(200, { suite: "default", versions: [], cases: [], rows: [], models: [], model_summaries: [] })),
  );
  return { agents, posted };
}

test("empty DB shows onboarding with honest Slack guidance, no fixture agent", async ({ page }) => {
  await stubBackend(page);
  await page.goto("/?api=1");
  await expect(page.getByText("Welcome to Curie")).toBeVisible();
  await expect(page.getByText("Connect Slack").first()).toBeVisible();
  await expect(page.getByText("apps/dispatcher/slack-app-manifest.yaml").first()).toBeVisible();
  await expect(page.getByTestId("slack-test-hint")).toBeVisible();
  // no fake Allow button, no fixture agent
  await expect(page.getByRole("button", { name: "Allow" })).toHaveCount(0);
  await expect(page.getByText("deal-desk")).toHaveCount(0);
});

test("create-agent -> the real agent appears in the list with its channel, honest post-deploy copy", async ({ page }) => {
  await stubBackend(page);
  await page.goto("/?api=1");

  await page.getByRole("button", { name: /New agent/ }).first().click();
  await page.getByTestId("agent-name").fill("support-bot");
  await page.getByTestId("agent-channel").fill("C01SUPPORT");
  await page.getByRole("button", { name: "Deploy" }).click();

  // honest post-deploy panel: real next step, not "replied in 42ms"
  const panel = page.getByTestId("deployed-panel");
  await expect(panel).toBeVisible({ timeout: 10_000 });
  await expect(panel).toContainText("support-bot");
  await expect(panel).toContainText("C01SUPPORT");
  await expect(page.getByText(/replied to its first ping in 42ms/)).toHaveCount(0);

  // the real agent now shows in the Agents list with its channel id
  await page.getByRole("navigation").getByText("Agents", { exact: true }).click();
  const nameCell = page.getByTestId("agent-card-name").filter({ hasText: "support-bot" });
  await expect(nameCell).toBeVisible();
  await expect(page.getByText("C01SUPPORT").first()).toBeVisible();
});

test("a channel that is not an ID warns but still deploys (CLI synthetic channels)", async ({ page }) => {
  await stubBackend(page);
  await page.goto("/?api=1");
  await page.getByRole("button", { name: /New agent/ }).first().click();
  await page.getByTestId("agent-name").fill("cli-bot");
  await page.getByTestId("agent-channel").fill("#not-an-id");
  // soft warning, not a block
  await expect(page.getByTestId("channel-warn")).toBeVisible();
  await page.getByRole("button", { name: "Deploy" }).click();
  await expect(page.getByTestId("deployed-panel")).toBeVisible({ timeout: 10_000 });
});

test("a blank channel keeps Deploy disabled so no unreachable agent is created", async ({ page }) => {
  const { posted } = await stubBackend(page);
  await page.goto("/?api=1");
  await page.getByRole("button", { name: /New agent/ }).first().click();
  await page.getByTestId("agent-name").fill("no-channel-bot");
  // Channel left empty: a blank channel can never match a Slack mention, so the
  // success panel must not claim a deploy that will silently drop every message.
  await expect(page.getByRole("button", { name: "Deploy" })).toBeDisabled();
  expect(posted).toHaveLength(0);
});

test("a channel copied with surrounding spaces is trimmed before it is stored", async ({ page }) => {
  const { posted } = await stubBackend(page);
  await page.goto("/?api=1");
  await page.getByRole("button", { name: /New agent/ }).first().click();
  await page.getByTestId("agent-name").fill("trim-bot");
  await page.getByTestId("agent-channel").fill("  C01SPACED  ");
  await page.getByRole("button", { name: "Deploy" }).click();
  await expect(page.getByTestId("deployed-panel")).toBeVisible({ timeout: 10_000 });
  // The worker matches the stored channel value exactly, so the spaces must be
  // gone by the time the create request is sent. Both sub-fields are asserted:
  // the binding carries an explicit kind, not a bare channel string.
  expect(posted.at(-1)?.channel.kind).toBe("slack");
  expect(posted.at(-1)?.channel.address).toBe("C01SPACED");
});

test("no fixture agent (deal-desk) leaks anywhere in wired mode", async ({ page }) => {
  await stubBackend(page);
  await page.goto("/?api=1");
  for (const nav of ["Overview", "Agents", "Evals", "Versions", "Connections", "Observability"]) {
    await page.getByRole("navigation").getByText(nav, { exact: true }).click();
    await expect(page.getByText("deal-desk")).toHaveCount(0);
  }
});
