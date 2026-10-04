import { test, expect, type Page } from "@playwright/test";
import { stubConsoleSession } from "./support/consoleSession";

// Pins the landing Overview split: the initial document must not download the
// non-landing views or jszip, and navigating to Agents / Observability must
// then request their deferred chunks. Chunk hashes change per build; identity
// is "referenced by index.html" vs "requested only after navigation".

// The console sits behind the login gate (#1047): sign this spec in.
test.beforeEach(async ({ page }) => {
  await stubConsoleSession(page);
});

const JS_ASSET = /\/assets\/([^/?]+\.js)(?:\?|$)/;

function jsAssetName(url: string): string | null {
  const match = url.match(JS_ASSET);
  return match ? match[1] : null;
}

function jsAssetNames(urls: Iterable<string>): string[] {
  const names = new Set<string>();
  for (const url of urls) {
    const name = jsAssetName(url);
    if (name) names.add(name);
  }
  return [...names].sort();
}

function htmlJsAssets(html: string): string[] {
  const names = new Set<string>();
  for (const match of html.matchAll(/\/assets\/([^"' ]+\.js)/g)) {
    names.add(match[1]);
  }
  return [...names].sort();
}

async function stubShell(page: Page) {
  await page.route("**/api/agents**", (route) => {
    if (route.request().method() === "GET") {
      return route.fulfill({ status: 200, contentType: "application/json", body: "[]" });
    }
    return route.fulfill({ status: 200, contentType: "application/json", body: "{}" });
  });
  await page.route("**/api/config**", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ org_name: "Globex Corporation" }),
    }),
  );
  await page.route("**/api/deployments**", (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: "[]" }),
  );
  await page.route("**/api/langfuse/**", (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: "[]" }),
  );
  await page.route("**/api/observability/**", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        start: "2026-06-28",
        end: "2026-07-05",
        runs: 0,
        latency_p95_ms: 0,
        tokens: 0,
        cost_usd: 0,
        error_rate: 0,
        metric: "runs",
        granularity: "day",
        points: [],
      }),
    }),
  );
  await page.route("**/api/evals/**", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        suite: "default",
        versions: [],
        cases: [],
        rows: [],
        models: [],
        model_summaries: [],
      }),
    }),
  );
}

test("landing document requests only the landing chunk; Agents and Observability load theirs on navigate", async ({
  page,
}) => {
  const requestedJs: string[] = [];
  page.on("request", (req) => {
    if (jsAssetName(req.url())) requestedJs.push(req.url());
  });

  await stubShell(page);

  const document = await page.goto("/?api=1");
  expect(document, "index.html must load").not.toBeNull();
  const landingHtml = await document!.text();
  const htmlChunks = htmlJsAssets(landingHtml);
  expect(htmlChunks.length, `index.html JS refs: ${htmlChunks.join(", ")}`).toBeGreaterThan(0);
  expect(
    htmlChunks.filter((name) => /jszip/i.test(name)),
    `index.html must not reference jszip: ${htmlChunks.join(", ")}`,
  ).toEqual([]);

  await expect(page.getByText("Welcome to Curie")).toBeVisible();
  await page.waitForLoadState("networkidle");

  const landingRequested = jsAssetNames(requestedJs);
  const extraOnLanding = landingRequested.filter((name) => !htmlChunks.includes(name));
  expect(
    extraOnLanding,
    `initial document requested JS beyond the landing HTML refs (html=${htmlChunks.join(", ")}; requested=${landingRequested.join(", ")})`,
  ).toEqual([]);

  const beforeAgents = requestedJs.length;
  await page.getByRole("navigation").getByText("Agents", { exact: true }).click();
  await expect(page.getByText("Create your first agent")).toBeVisible();
  await page.waitForLoadState("networkidle");
  const agentsChunks = jsAssetNames(requestedJs.slice(beforeAgents)).filter(
    (name) => !htmlChunks.includes(name),
  );
  expect(
    agentsChunks.length,
    `navigating to Agents must request a deferred JS chunk; got [${agentsChunks.join(", ")}] (landing html=${htmlChunks.join(", ")})`,
  ).toBeGreaterThan(0);

  const beforeObs = requestedJs.length;
  await page.getByRole("navigation").getByText("Observability", { exact: true }).click();
  await expect(
    page.getByText("OpenTelemetry traces, Prometheus-style metrics, and Loki-style logs", {
      exact: false,
    }),
  ).toBeVisible();
  await page.waitForLoadState("networkidle");
  const obsChunks = jsAssetNames(requestedJs.slice(beforeObs)).filter(
    (name) => !htmlChunks.includes(name) && !agentsChunks.includes(name),
  );
  expect(
    obsChunks.length,
    `navigating to Observability must request a deferred JS chunk; got [${obsChunks.join(", ")}] (agents=${agentsChunks.join(", ")})`,
  ).toBeGreaterThan(0);

  expect(
    jsAssetNames(requestedJs).filter((name) => /jszip/i.test(name)),
    "jszip must stay off the Overview / Agents / Observability path",
  ).toEqual([]);
});
