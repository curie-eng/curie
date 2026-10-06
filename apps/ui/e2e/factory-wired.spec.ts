import { test, expect, type Page } from "@playwright/test";
import { stubConsoleSession } from "./support/consoleSession";

// Wired Factory view (#4102) in the stackless suite: GET /work-items,
// GET /work-items/{id} and GET /work-items/{id}/usage are stubbed with
// real-shaped responses (including title and progress) via route
// interception, so these run headless with no backend.

// The console sits behind the login gate (#1047): sign this spec in.
test.beforeEach(async ({ page }) => {
  await stubConsoleSession(page);
});

const AGENT = { id: "ag-1", name: "factory", channels: [{ kind: "github", address: "acme/payments" }], created_at: "2026-09-01T00:00:00Z" };

const HOUR = 3_600_000;

function iso(msAgo: number): string {
  return new Date(Date.now() - msAgo).toISOString();
}

function json(status: number, body: unknown) {
  return { status, contentType: "application/json", body: JSON.stringify(body) };
}

type Stage = { id: string; label: string; state: "done" | "current" | "redo" | "blocked" | "pending"; round_label: string | null };

function stages(states: Stage["state"][], rounds: Record<number, string> = {}): Stage[] {
  const labels = ["Plan", "Implement", "Review", "Verify", "Publish"];
  return labels.map((label, i) => ({ id: label.toLowerCase(), label, state: states[i], round_label: rounds[i] ?? null }));
}

function request(status: string, sequence = 1) {
  return {
    sequence,
    status,
    created_at: iso(3 * HOUR),
    wait_deadline: null,
    started_at: status === "queued" ? null : iso(3 * HOUR),
    execution_deadline: null,
    terminal_at: status === "completed" || status === "failed" ? iso(HOUR) : null,
    terminal_cause: status === "completed" || status === "failed" ? status : null,
    termination_observation: null,
    capacity_deferrals: 0,
    last_deferral_reason: null,
  };
}

function item(overrides: Record<string, unknown>) {
  const repo = (overrides.repo_full_name as string | undefined) ?? "acme/payments";
  const number = overrides.github_issue_number as number;
  return {
    agent_id: AGENT.id,
    repo_full_name: repo,
    issue_url: `https://github.com/${repo}/issues/${number}`,
    cancelled_at: null,
    created_at: iso(4 * HOUR),
    updated_at: iso(HOUR),
    objective: "Implement the admitted work item",
    objective_truncated: false,
    requester: "U0REQUEST1",
    actionable_cause: "In progress",
    title: null,
    progress: null,
    pr: null,
    publication: null,
    correctness: { asserted: false, owner: "bundle" },
    ci: null,
    requests: [request("running")],
    ...overrides,
  };
}

const RUNNING = item({
  id: "wi-398",
  github_issue_number: 398,
  state: "running",
  title: "Retry webhook delivery with exponential backoff",
  progress: {
    current: "implement",
    note: "Reviewer asked for jitter on the backoff; adding it now.",
    stages: stages(["done", "current", "redo", "pending", "pending"], { 1: "round 2 of 3" }),
  },
  pr: { number: 412, url: "https://github.com/acme/payments/pull/412", status: "open" },
  ci: { state: "pending", reason: null, observed_at: iso(HOUR / 4), head_sha: "1123456789abcdef0123456789abcdef01234567" },
});

const ITEMS = [
  item({
    id: "wi-401",
    repo_full_name: "acme/ledger",
    github_issue_number: 401,
    state: "queued",
    title: "Add idempotency keys to ledger writes",
    requests: [request("queued")],
  }),
  item({
    id: "wi-402",
    repo_full_name: "acme/web",
    github_issue_number: 402,
    state: "waiting",
    title: "Paginate the invoices table",
    actionable_cause: "Waiting for runner capacity",
    requests: [request("queued")],
  }),
  RUNNING,
  item({
    id: "wi-405",
    repo_full_name: "acme/web",
    github_issue_number: 405,
    state: "publishing",
    title: "Dark mode for the settings page",
    progress: { current: "publish", note: null, stages: stages(["done", "done", "done", "done", "current"]) },
  }),
  item({
    id: "wi-387",
    github_issue_number: 387,
    state: "failed",
    title: "Migrate refunds to the v2 processor API",
    actionable_cause: "Run failed: tests could not reach the sandbox database",
    progress: { current: "verify", note: "Integration tests timed out.", stages: stages(["done", "done", "done", "blocked", "pending"]) },
    requests: [request("failed")],
  }),
  item({
    id: "wi-390",
    repo_full_name: "acme/ledger",
    github_issue_number: 390,
    state: "awaiting_approval",
    title: "Rotate the ledger signing key",
    actionable_cause: "Publication awaits approval",
    publication: { status: "pending", revision_number: 1, approval_status: "pending" },
    requests: [request("completed")],
  }),
  ...[371, 368, 362].map((n, i) =>
    item({
      id: `wi-${n}`,
      repo_full_name: i === 1 ? "acme/web" : "acme/payments",
      github_issue_number: n,
      state: "published",
      title: ["Cache exchange rates for 60 seconds", "Fix focus trap in the checkout modal", "Log webhook signature failures"][i],
      actionable_cause: "PR opened",
      updated_at: iso((i + 1) * 5 * HOUR),
      pr: { number: 380 + i, url: `https://github.com/acme/${i === 1 ? "web" : "payments"}/pull/${380 + i}`, status: "merged" },
      publication: { status: "succeeded", revision_number: 1, approval_status: "approved" },
      requests: [request("completed")],
    }),
  ),
  item({ id: "wi-355", github_issue_number: 355, state: "cancelled", cancelled_at: iso(20 * HOUR), requests: [request("completed")] }),
];

function usageFor(id: string) {
  return {
    work_item_id: id,
    total_tokens: 1_284_000,
    estimated_cost_usd: "3.42",
    cost_complete: true,
    requests_without_usage: 0,
    roles: {
      implementer: { tokens: 1_100_000, estimated_cost_usd: "2.91", cost_complete: true },
      reviewer: { tokens: 184_000, estimated_cost_usd: "0.51", cost_complete: true },
    },
    models: [
      {
        model: "claude-opus-4-1",
        role: "implementer",
        input_tokens: 900_000,
        cached_input_tokens: 120_000,
        cache_write_tokens: 30_000,
        output_tokens: 50_000,
        estimated_cost_usd: "2.91",
      },
      {
        model: "gpt-5-codex",
        role: "reviewer",
        input_tokens: 170_000,
        cached_input_tokens: 0,
        cache_write_tokens: 0,
        output_tokens: 14_000,
        estimated_cost_usd: "0.51",
      },
    ],
    requests: [{ request_id: "8c1f0000-0000-4000-8000-000000000001", tokens: 1_284_000, estimated_cost_usd: "3.42" }],
    price_sources: [{ source: "litellm", as_of: "2026-10-01T00:00:00Z" }],
    pr_number: 412,
    pr_url: "https://github.com/acme/payments/pull/412",
  };
}

type Hits = { list: number; detail: number; usage: number };

// Precise regexes so the list route does not swallow /{id} or /{id}/usage,
// and the detail route does not swallow /usage.
const LIST = /\/api\/work-items(\?.*)?$/;
const DETAIL = /\/api\/work-items\/([^/?]+)(\?.*)?$/;
const USAGE = /\/api\/work-items\/([^/?]+)\/usage(\?.*)?$/;

async function stubFactory(page: Page, items: object[] = ITEMS): Promise<Hits> {
  const hits: Hits = { list: 0, detail: 0, usage: 0 };
  const byId = new Map(items.map((i) => [(i as { id: string }).id, i]));
  await page.route(/\/api\/agents(\?.*)?$/, (route) => route.fulfill(json(200, [AGENT])));
  await page.route(LIST, (route) => {
    hits.list += 1;
    return route.fulfill(json(200, { items, limit: 200, truncated: false }));
  });
  await page.route(DETAIL, (route) => {
    hits.detail += 1;
    const id = decodeURIComponent(DETAIL.exec(route.request().url())![1]);
    const found = byId.get(id);
    return route.fulfill(found ? json(200, found) : json(404, { code: "not_found" }));
  });
  await page.route(USAGE, (route) => {
    hits.usage += 1;
    const id = decodeURIComponent(USAGE.exec(route.request().url())![1]);
    return route.fulfill(byId.has(id) ? json(200, usageFor(id)) : json(404, { code: "not_found" }));
  });
  return hits;
}

async function openFactory(page: Page) {
  await page.goto("/?api=1");
  await page.getByRole("navigation").getByText("Factory", { exact: true }).click();
}

function cardFor(page: Page, repo: string, issue: number) {
  return page.getByTestId("factory-card").filter({ hasText: `${repo}#${issue}` });
}

test("cards land in all four lanes", async ({ page }) => {
  await stubFactory(page);
  await openFactory(page);

  await expect(page.getByTestId("factory-lane-queued").getByTestId("factory-card")).toHaveCount(2);
  await expect(page.getByTestId("factory-lane-running").getByTestId("factory-card")).toHaveCount(2);
  await expect(page.getByTestId("factory-lane-needs-you").getByTestId("factory-card")).toHaveCount(2);
  await expect(page.getByTestId("factory-lane-shipped").getByTestId("factory-card")).toHaveCount(3);
  await expect(page.getByTestId("factory-cancelled")).toHaveText(/1 cancelled/);

  await expect(page.getByTestId("factory-kpi-in-flight")).toContainText("4");
  await expect(page.getByTestId("factory-kpi-needs-you")).toContainText("2");
  await expect(page.getByTestId("factory-kpi-shipped")).toContainText("3");

  await expect(page.getByTestId("factory-lane-needs-you")).toContainText("Publication awaits approval");
  await expect(cardFor(page, "acme/payments", 398).getByTestId("factory-card-track")).toBeVisible();
});

test("clicking a card opens the detail panel with stages, note and cost", async ({ page }) => {
  const hits = await stubFactory(page);
  await openFactory(page);

  await cardFor(page, "acme/payments", 398).click();
  const detail = page.getByTestId("factory-detail");
  await expect(detail).toBeVisible();
  await expect(detail).toContainText("Retry webhook delivery with exponential backoff");

  const stagesInPanel = detail.getByTestId("factory-stage-track").getByTestId("factory-stage");
  await expect(stagesInPanel).toHaveCount(5);
  await expect(stagesInPanel.nth(1)).toHaveAttribute("data-state", "current");
  await expect(stagesInPanel.nth(1)).toContainText("Implement");
  await expect(stagesInPanel.nth(1)).toContainText("round 2 of 3");
  await expect(detail).toContainText("Reviewer asked for jitter on the backoff; adding it now.");
  await expect(detail).toContainText(/owned by the bundle/i);

  const cost = detail.getByTestId("factory-cost");
  await expect(cost).toContainText("$3.42");
  await expect(cost.getByTestId("factory-cost-model")).toHaveCount(2);
  await expect(cost.getByTestId("factory-cost-model").first()).toContainText("claude-opus-4-1");
  await expect(cost).not.toContainText(/estimate incomplete/i);
  await expect(detail.getByTestId("factory-requests")).toBeVisible();
  await expect(detail.getByRole("button")).toHaveCount(0);

  expect(hits.detail).toBeGreaterThan(0);
  expect(hits.usage).toBeGreaterThan(0);
});

test("the list, open detail and usage refetch every 15 seconds", async ({ page }) => {
  await page.clock.install();
  const hits = await stubFactory(page);
  await openFactory(page);

  await cardFor(page, "acme/payments", 398).click();
  await expect(page.getByTestId("factory-detail").getByTestId("factory-cost")).toContainText("$3.42");
  await expect.poll(() => hits.usage).toBeGreaterThan(0);

  const before = { ...hits };
  await page.clock.fastForward(15_000);

  await expect.poll(() => hits.list).toBeGreaterThan(before.list);
  await expect.poll(() => hits.detail).toBeGreaterThan(before.detail);
  await expect.poll(() => hits.usage).toBeGreaterThan(before.usage);
});

test("a detail 404 says the item no longer exists", async ({ page }) => {
  await stubFactory(page);
  await page.route(DETAIL, (route) => route.fulfill(json(404, { code: "not_found" })));
  await page.route(USAGE, (route) => route.fulfill(json(404, { code: "not_found" })));
  await openFactory(page);

  await cardFor(page, "acme/payments", 398).click();
  await expect(page.getByText("This work item no longer exists.")).toBeVisible();
});

test("design review screenshot", async ({ page }) => {
  test.skip(process.env.FACTORY_IMPL_SCREENSHOT !== "1", "set FACTORY_IMPL_SCREENSHOT=1 to write design-review/factory-impl.png");
  await page.setViewportSize({ width: 1600, height: 1000 });
  await stubFactory(page);
  await openFactory(page);

  for (const id of ["queued", "running", "needs-you", "shipped"]) {
    await expect(page.getByTestId(`factory-lane-${id}`).getByTestId("factory-card").first()).toBeVisible();
  }
  await cardFor(page, "acme/payments", 398).click();
  await expect(page.getByTestId("factory-detail").getByTestId("factory-cost")).toContainText("$3.42");

  // Relative to apps/ui, the Playwright cwd (`pnpm e2e` runs there).
  await page.screenshot({ path: "design-review/factory-impl.png" });
});
