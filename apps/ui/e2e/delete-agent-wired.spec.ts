import { test, expect, type Page } from "@playwright/test";

// Wired Agents delete flow (stackless via route stubs): the agent list is
// mutable so a successful DELETE drops the card, and a 409 (active deployment)
// surfaces as a toast while the card stays put. No backend needed.

function json(status: number, body: unknown) {
  return { status, contentType: "application/json", body: JSON.stringify(body) };
}

const AGENT = { id: "a1", name: "deal-desk", channels: [{ kind: "slack", address: "#revenue-ops" }], created_at: "2026-07-01T00:00:00Z" };

// deleteStatus 204 => the card disappears on refetch; 409 => it survives.
async function stubAgents(page: Page, deleteStatus: 204 | 409) {
  const agents: (typeof AGENT)[] = [{ ...AGENT }];
  await page.route(/\/api\/agents(\?.*)?$/, (route) => route.fulfill(json(200, agents)));
  await page.route("**/api/agents/a1", (route) => {
    if (route.request().method() !== "DELETE") return route.fallback();
    if (deleteStatus === 204) {
      agents.length = 0;
      return route.fulfill({ status: 204, body: "" });
    }
    return route.fulfill(json(409, { detail: "agent has an active deployment; stop it before deleting" }));
  });
  await page.route("**/api/observability/metrics/summary*", (route) => route.fulfill(json(200, {
    start: "s", end: "e", runs: 0, latency_p95_ms: 0, tokens: 0, cost_usd: 0, error_rate: 0,
  })));
  await page.route("**/api/langfuse/traces*", (route) => route.fulfill(json(200, [])));
}

async function openAgents(page: Page) {
  await page.goto("/?api=1");
  await page.getByRole("navigation").getByText("Agents", { exact: true }).click();
  await expect(page.getByTestId("agent-card-name").filter({ hasText: "deal-desk" })).toBeVisible();
}

test("delete removes the agent card after confirming", async ({ page }) => {
  await stubAgents(page, 204);
  page.on("dialog", (d) => d.accept());
  await openAgents(page);

  await page.getByTestId("delete-agent").click();
  await expect(page.getByTestId("agent-card-name").filter({ hasText: "deal-desk" })).toHaveCount(0);
  await expect(page.getByText("Deleted deal-desk")).toBeVisible();
});

test("cancelling the confirm keeps the agent", async ({ page }) => {
  await stubAgents(page, 204);
  page.on("dialog", (d) => d.dismiss());
  await openAgents(page);

  await page.getByTestId("delete-agent").click();
  await expect(page.getByTestId("agent-card-name").filter({ hasText: "deal-desk" })).toBeVisible();
});

test("a 409 (active deployment) surfaces as a toast and keeps the card", async ({ page }) => {
  await stubAgents(page, 409);
  page.on("dialog", (d) => d.accept());
  await openAgents(page);

  await page.getByTestId("delete-agent").click();
  await expect(page.getByText(/active deployment/)).toBeVisible();
  await expect(page.getByTestId("agent-card-name").filter({ hasText: "deal-desk" })).toBeVisible();
});
