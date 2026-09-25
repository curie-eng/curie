import { test, expect, type Page } from "@playwright/test";

// Wired Work items (#2577) in the stackless suite: GET /work-items and
// GET /work-items/{id} are stubbed with real-shaped responses via route
// interception, so these run headless with no backend.

const AGENT = { id: "ag-1", name: "factory", channels: [{ kind: "github", address: "acme-corp/acme-bot" }], created_at: "2026-09-01T00:00:00Z" };

function json(status: number, body: unknown) {
  return { status, contentType: "application/json", body: JSON.stringify(body) };
}

function item(overrides: Record<string, unknown> = {}) {
  return {
    id: "wi-1",
    agent_id: AGENT.id,
    repo_full_name: "acme-corp/acme-bot",
    github_issue_number: 2577,
    issue_url: "https://github.com/acme-corp/acme-bot/issues/2577",
    cancelled_at: null,
    created_at: "2026-09-22T10:00:00Z",
    updated_at: "2026-09-22T10:05:00Z",
    objective: "Implement the admitted work item",
    objective_truncated: false,
    requester: "U0REQUEST1",
    state: "published",
    actionable_cause: "PR opened; CI and review are the next signals",
    pr: { number: 123, url: "https://github.com/acme-corp/acme-bot/pull/123", status: "open" },
    publication: { status: "succeeded", revision_number: 1, approval_status: "approved" },
    correctness: { asserted: false, owner: "bundle" },
    ci: null,
    requests: [
      {
        sequence: 1,
        status: "completed",
        created_at: "2026-09-22T10:00:00Z",
        wait_deadline: "2026-09-23T10:00:00Z",
        started_at: "2026-09-22T10:01:00Z",
        execution_deadline: "2026-09-22T10:31:00Z",
        terminal_at: "2026-09-22T10:04:00Z",
        terminal_cause: "completed",
        termination_observation: null,
        capacity_deferrals: 0,
        last_deferral_reason: null,
      },
    ],
    ...overrides,
  };
}

async function stubWorkItems(page: Page, items: object[]) {
  await page.route(/\/api\/agents(\?.*)?$/, (route) => route.fulfill(json(200, [AGENT])));
  await page.route(/\/api\/work-items\/[^/?]+(\?.*)?$/, (route) =>
    route.fulfill(
      json(
        200,
        item({
          ci: {
            state: "unavailable",
            reason: "app_not_configured",
            observed_at: "2026-09-22T10:06:00Z",
            head_sha: "1123456789abcdef0123456789abcdef01234567",
          },
        }),
      ),
    ),
  );
  await page.route(/\/api\/work-items(\?.*)?$/, (route) =>
    route.fulfill(json(200, { items, limit: 50, truncated: false })),
  );
}

async function openWorkItems(page: Page) {
  await page.goto("/?api=1");
  await page.getByRole("navigation").getByText("Work items", { exact: true }).click();
}

test("lists work items and opens a detail with the CI unavailable reason", async ({ page }) => {
  await stubWorkItems(page, [item()]);
  await openWorkItems(page);

  const row = page.getByTestId("work-item-row");
  await expect(row).toHaveCount(1);
  await expect(row).toContainText("published");
  await expect(row.getByRole("link", { name: /acme-corp\/acme-bot#2577/ })).toHaveAttribute(
    "href",
    "https://github.com/acme-corp/acme-bot/issues/2577",
  );

  await row.click();
  const detail = page.getByTestId("work-item-detail");
  await expect(detail).toBeVisible();
  await expect(detail).toContainText("unavailable");
  await expect(detail).toContainText("app_not_configured");
  await expect(detail).toContainText(/not asserted by the platform/i);
});

test("an empty install shows the truthful empty state", async ({ page }) => {
  await stubWorkItems(page, []);
  await openWorkItems(page);

  await expect(page.getByText(/no factory work items/i)).toBeVisible();
  await expect(page.getByTestId("work-item-row")).toHaveCount(0);
});

test("a detail 404 says the item no longer exists", async ({ page }) => {
  await stubWorkItems(page, [item()]);
  await page.route(/\/api\/work-items\/[^/?]+(\?.*)?$/, (route) =>
    route.fulfill(json(404, { code: "not_found" })),
  );
  await openWorkItems(page);

  await page.getByTestId("work-item-row").click();
  await expect(page.getByText(/no longer exists/i)).toBeVisible();
});
