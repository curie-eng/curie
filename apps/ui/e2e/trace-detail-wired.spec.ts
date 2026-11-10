import { test, expect, type Page, type Route } from "@playwright/test";
import { stubConsoleSession } from "./support/consoleSession";

// FX2 items 3 & 4: the wired trace drill-in. A trace with no observations is a
// legitimate empty state (honest empty view, not an error toast); a trace whose
// resource/metadata carries curie.sandbox_id shows which sandbox served it.

// The console sits behind the login gate (#1047): sign this spec in.
test.beforeEach(async ({ page }) => {
  await stubConsoleSession(page);
});

const TRACES = [{ id: "tr1", name: "deal-desk run", timestamp: "2026-07-05T14:02:07Z" }];

const json = (route: Route, status: number, body: unknown) =>
  route.fulfill({ status, contentType: "application/json", body: JSON.stringify(body) });

async function openTrace(page: Page) {
  // WiredProvider fetches the agent list on mount regardless of view; stub it so
  // the test does not depend on a live backend.
  await page.route("**/api/agents", (route) => json(route, 200, []));
  await page.route("**/api/langfuse/traces?*", (route) => json(route, 200, TRACES));
  await page.goto("/?api=1&state=3");
  await page.getByRole("navigation").getByText("Observability", { exact: true }).click();
  await page.getByRole("button", { name: "Traces" }).click();
  await page.getByTestId("trace-row").first().click();
}

test("a trace with no observations renders an honest empty state, not an error", async ({ page }) => {
  await page.route("**/api/langfuse/traces/tr1*", (route) =>
    json(route, 404, { detail: "trace has no observations yet" }),
  );
  await openTrace(page);

  await expect(page.getByText("No spans recorded for this run yet")).toBeVisible();
  // The old behavior surfaced the raw API error; assert it is gone.
  await expect(page.getByText(/Could not load trace/)).toHaveCount(0);
  await expect(page.getByText(/trace has no observations yet/)).toHaveCount(0);
});

test("a trace carrying curie.sandbox_id shows which sandbox served it", async ({ page }) => {
  await page.route("**/api/langfuse/traces/tr1*", (route) =>
    json(route, 200, {
      trace: { id: "tr1", name: "deal-desk run", metadata: { "curie.sandbox_id": "runner-deal-desk-abc123" } },
      tree: [{ id: "o1", type: "SPAN", name: "handle_message", model: null, startTime: null, usageDetails: null, children: [] }],
    }),
  );
  await openTrace(page);

  const sandbox = page.getByTestId("trace-sandbox");
  await expect(sandbox).toBeVisible();
  await expect(sandbox).toContainText("Served by sandbox");
  await expect(sandbox).toContainText("runner-deal-desk-abc123");
  await expect(page.getByTestId("span-tree")).toContainText("handle_message");
});

test("a trace without sandbox metadata simply omits the line", async ({ page }) => {
  await page.route("**/api/langfuse/traces/tr1*", (route) =>
    json(route, 200, {
      trace: { id: "tr1", name: "deal-desk run" },
      tree: [{ id: "o1", type: "SPAN", name: "handle_message", model: null, startTime: null, usageDetails: null, children: [] }],
    }),
  );
  await openTrace(page);

  await expect(page.getByTestId("span-tree")).toBeVisible();
  await expect(page.getByTestId("trace-sandbox")).toHaveCount(0);
});
