import { test, expect, type Page } from "@playwright/test";

// Wired Cost view + kill switch (L1) in the stackless suite: the app runs in
// ?api=1 mode with the L1 endpoints stubbed via route interception (mutable
// state so PUT/POST transitions are reflected on refetch). No backend needed.

const AGENT = { id: "a1", name: "deal-desk", channels: [{ kind: "slack", address: "#revenue-ops" }], created_at: "2026-07-01T00:00:00Z" };

interface StubState {
  cost: { start: string; end: string; total_usd: number; points: { ts: string; value: number }[] };
  budget: { max_usd_per_day: number | null; max_output_tokens_per_run: number | null };
  killed: boolean;
  budgetPutStatus: number; // 200 or 422
}

function json(status: number, body: unknown) {
  return { status, contentType: "application/json", body: JSON.stringify(body) };
}

async function stubL1(page: Page, override: Partial<StubState> = {}) {
  const state: StubState = {
    cost: {
      start: "2026-06-28",
      end: "2026-07-05",
      total_usd: 12.34,
      points: [
        { ts: "2026-07-03", value: 4.0 },
        { ts: "2026-07-04", value: 3.34 },
        { ts: "2026-07-05", value: 5.0 },
      ],
    },
    budget: { max_usd_per_day: 25, max_output_tokens_per_run: 4096 },
    killed: false,
    budgetPutStatus: 200,
    ...override,
  };

  await page.route(/\/api\/agents(\?.*)?$/, (route) => route.fulfill(json(200, [AGENT])));
  await page.route("**/api/agents/*/cost*", (route) => route.fulfill(json(200, state.cost)));
  await page.route("**/api/agents/*/budget", async (route) => {
    if (route.request().method() === "PUT") {
      if (state.budgetPutStatus === 422) {
        return route.fulfill(
          json(422, { detail: [{ loc: ["body", "max_usd_per_day"], msg: "Input should be greater than 0", type: "greater_than" }] }),
        );
      }
      state.budget = JSON.parse(route.request().postData() ?? "{}");
      return route.fulfill(json(200, state.budget));
    }
    return route.fulfill(json(200, state.budget));
  });
  await page.route("**/api/agents/*/kill", (route) => {
    if (route.request().method() === "POST") state.killed = true;
    return route.fulfill(json(200, { killed: state.killed }));
  });
  await page.route("**/api/agents/*/resume", (route) => {
    state.killed = false;
    return route.fulfill(json(200, { killed: false }));
  });
  return state;
}

async function openCost(page: Page) {
  await page.goto("/?state=3&api=1");
  await page.getByRole("navigation").getByText("Observability", { exact: true }).click();
  await page.getByRole("button", { name: "Cost" }).click();
}

test("Cost view renders total, chart, and budget from the API", async ({ page }) => {
  await stubL1(page);
  await openCost(page);
  await expect(page.getByTestId("cost-total")).toHaveText("$12.34");
  await expect(page.getByTestId("cost-chart")).toBeVisible();
  const budget = page.getByTestId("budget-display");
  await expect(budget).toContainText("$25");
  await expect(budget).toContainText("4096");
});

test("Cost view shows an honest empty state for a zero series", async ({ page }) => {
  await stubL1(page, { cost: { start: "s", end: "e", total_usd: 0, points: [{ ts: "2026-07-05", value: 0 }] } });
  await openCost(page);
  await expect(page.getByTestId("cost-total")).toHaveText("$0.00");
  await expect(page.getByText(/No spend recorded/)).toBeVisible();
});

test("budget edit round-trips to the API", async ({ page }) => {
  await stubL1(page);
  await openCost(page);
  await page.getByRole("button", { name: "Edit" }).click();
  await page.getByTestId("budget-usd").fill("50");
  await page.getByTestId("budget-tokens").fill("8192");
  await page.getByRole("button", { name: "Save budget" }).click();
  const budget = page.getByTestId("budget-display");
  await expect(budget).toContainText("$50");
  await expect(budget).toContainText("8192");
});

test("budget rejects a non-positive value client-side", async ({ page }) => {
  await stubL1(page);
  await openCost(page);
  await page.getByRole("button", { name: "Edit" }).click();
  await page.getByTestId("budget-usd").fill("-5");
  await page.getByRole("button", { name: "Save budget" }).click();
  await expect(page.getByTestId("budget-error")).toContainText(/positive/i);
  // still editing (no round-trip happened)
  await expect(page.getByTestId("budget-form")).toBeVisible();
});

test("budget rejects a malformed amount instead of truncating it", async ({ page }) => {
  // parseFloat("1,000") is 1 and parseFloat("25usd") is 25; the strict Number()
  // parse must reject the whole string rather than silently save a smaller cap.
  await stubL1(page);
  await openCost(page);
  for (const bad of ["1,000", "25usd"]) {
    await page.getByRole("button", { name: "Edit" }).click();
    await page.getByTestId("budget-usd").fill(bad);
    await page.getByRole("button", { name: "Save budget" }).click();
    await expect(page.getByTestId("budget-error")).toContainText(/positive/i);
    await expect(page.getByTestId("budget-form")).toBeVisible();
    await page.getByRole("button", { name: "Cancel" }).click();
  }
});

test("budget surfaces a server 422", async ({ page }) => {
  await stubL1(page, { budgetPutStatus: 422 });
  await openCost(page);
  await page.getByRole("button", { name: "Edit" }).click();
  await page.getByTestId("budget-usd").fill("25");
  await page.getByRole("button", { name: "Save budget" }).click();
  await expect(page.getByTestId("budget-error")).toContainText(/greater than 0/i);
});

test("kill switch confirms, kills, and resumes", async ({ page }) => {
  await stubL1(page);
  await openCost(page);
  await expect(page.getByTestId("kill-panel")).toHaveAttribute("data-killed", "false");

  // confirm-before-kill
  await page.getByRole("button", { name: "Kill agent" }).click();
  await expect(page.getByText("Stop the agent now?")).toBeVisible();
  await page.getByRole("button", { name: "Cancel" }).click();
  await expect(page.getByRole("button", { name: "Kill agent" })).toBeVisible();

  // kill for real
  await page.getByRole("button", { name: "Kill agent" }).click();
  await page.getByRole("button", { name: "Confirm kill" }).click();
  await expect(page.getByTestId("kill-panel")).toHaveAttribute("data-killed", "true");
  await expect(page.getByTestId("kill-status")).toHaveText("AGENT KILLED");

  // resume
  await page.getByRole("button", { name: "Resume agent" }).click();
  await expect(page.getByTestId("kill-panel")).toHaveAttribute("data-killed", "false");
});
