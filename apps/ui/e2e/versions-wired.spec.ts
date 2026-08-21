import { test, expect, type Page } from "@playwright/test";

// Wired Versions tab (stackless via route stubs): real versions joined with the
// agent's deployments (environment / status / deployed_at), no Eval column, and
// a ComingSoon fallback when the backend is unreachable.

const AGENT = { id: "ag-1", name: "deal-desk", channels: [{ kind: "slack", address: "C0DEAL" }], created_at: "2026-07-01T00:00:00Z" };

function json(status: number, body: unknown) {
  return { status, contentType: "application/json", body: JSON.stringify(body) };
}

async function stubVersions(page: Page) {
  await page.route(/\/api\/agents(\?.*)?$/, (route) => route.fulfill(json(200, [AGENT])));
  await page.route("**/api/agents/*/versions", (route) =>
    route.fulfill(
      json(200, [
        { id: "v1", agent_id: AGENT.id, version_label: "v0.1.0", bundle_ref: "r", bundle_sha256: "s", created_by: "brian", created_at: "2026-07-01T00:00:00Z" },
        { id: "v2", agent_id: AGENT.id, version_label: "v0.1.1", bundle_ref: "r", bundle_sha256: "s", created_by: "push:main", created_at: "2026-07-02T00:00:00Z" },
      ]),
    ),
  );
  await page.route("**/api/deployments*", (route) =>
    route.fulfill(
      json(200, [
        { id: "d1", agent_id: AGENT.id, version_id: "v1", environment: "prod", commit_sha: "abc", status: "active", deployed_at: "2026-07-03T00:00:00Z" },
      ]),
    ),
  );
}

test("Versions tab renders real version rows without an Eval column", async ({ page }) => {
  await stubVersions(page);
  await page.goto("/?api=1");
  await page.getByRole("navigation").getByText("Versions", { exact: true }).click();

  const rows = page.getByTestId("version-row");
  await expect(rows).toHaveCount(2); // v1 (deployed prod) + v2 (undeployed)
  const deployedRow = rows.filter({ hasText: "v0.1.0" });
  await expect(deployedRow).toContainText("prod");
  await expect(deployedRow).toContainText("active");
  const undeployedRow = rows.filter({ hasText: "v0.1.1" });
  await expect(undeployedRow).toContainText("not deployed");
  // The Eval column was dropped.
  await expect(page.getByText("Eval", { exact: true })).toHaveCount(0);
  // No fixture agents leak (the fixture Versions table had synthetic rows).
  await expect(page.getByText("@curie-dev")).toHaveCount(0);
});

test("Versions tab falls back to ComingSoon when the API is unreachable", async ({ page }) => {
  await page.route(/\/api\/agents(\?.*)?$/, (route) => route.fulfill(json(500, { detail: "boom" })));
  await page.goto("/?api=1");
  await page.getByRole("navigation").getByText("Versions", { exact: true }).click();
  await expect(page.getByText("Versions are not available")).toBeVisible();
});
