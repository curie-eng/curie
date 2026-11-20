import { test, expect, type Page } from "@playwright/test";
import { stubConsoleSession } from "./support/consoleSession";

// The Memory page (#5800) in the stackless suite: the learned rules routes and
// the facts route are stubbed with real-shaped answers (the shapes recorded in
// tests/vectors/memory-rules-responses.json) so the built page runs headless
// with no backend. The stub enforces `expected_version` like the real routes do.

test.beforeEach(async ({ page }) => {
  await stubConsoleSession(page);
});

const AGENT = { id: "ag-mem", name: "deal-desk", channels: [], created_at: "2026-07-05T00:00:00Z" };
const REPO = "acme-corp/acme-bot";

interface Rule {
  id: string;
  number: number;
  label: string;
  scope_kind: "repository";
  scope_key: string;
  scope_label: string;
  state: "proposed" | "accepted" | "rejected" | "removed";
  text: string;
  reason: string;
  source_url: string;
  proposed_by: string;
  proposed_at: string;
  accepted_by: { principal_id: string; display_name: string } | null;
  accepted_at: string | null;
  rejected_by: null;
  rejected_at: string | null;
  removed_by: null;
  removed_at: string | null;
  version: number;
}

function proposed(n: number, text: string): Rule {
  return {
    id: `00000000-0000-4000-8000-00000000000${n}`,
    number: n,
    label: `R${n}`,
    scope_kind: "repository",
    scope_key: "github:github.com:4401",
    scope_label: REPO,
    state: "proposed",
    text,
    reason: "Reviewers asked for it twice.",
    source_url: "https://github.com/acme-corp/acme-bot/pull/12",
    proposed_by: "github:6601:octocat",
    proposed_at: "2026-11-18T09:00:00Z",
    accepted_by: null,
    accepted_at: null,
    rejected_by: null,
    rejected_at: null,
    removed_by: null,
    removed_at: null,
    version: 1,
  };
}

async function stubApi(page: Page, rules: Rule[]) {
  const posts: { action: string; body: unknown }[] = [];
  await page.route(/\/api\/agents(\?.*)?$/, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify([AGENT]) }),
  );
  await page.route(
    (url) => /\/api\/agents\/[^/]+\/memory$/.test(url.pathname),
    (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([
          {
            index: 0,
            version: 3,
            content: "deploy is a git push",
            provenance: { learned_from_session_id: "sess-1", source_trace_ids: ["trace-a"], recorded_at: "2026-07-13T00:00:00+00:00" },
          },
        ]),
      }),
  );
  await page.route(
    (url) => /\/api\/agents\/[^/]+\/memory\/rules(\/.*)?$/.test(url.pathname),
    (route) => {
      const req = route.request();
      const m = /\/memory\/rules\/([^/]+)\/(accept|reject|remove)$/.exec(new URL(req.url()).pathname);
      if (req.method() === "GET" || !m) {
        return route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({ rules, scopes: [], in_force_limit: 16 }),
        });
      }
      const [, id, action] = m;
      const body = req.postDataJSON() as { expected_version: number };
      posts.push({ action, body });
      const rule = rules.find((r) => r.id === id);
      if (!rule || rule.version !== body.expected_version) {
        return route.fulfill({
          status: 409,
          contentType: "application/json",
          body: JSON.stringify({ detail: { code: "memory_rules.stale_version", message: "stale", fix: "reload", current_version: rule?.version ?? null } }),
        });
      }
      rule.version += 1;
      if (action === "accept") {
        rule.state = "accepted";
        rule.accepted_by = { principal_id: "00000000-0000-4000-8000-000000000009", display_name: "Acme Owner" };
        rule.accepted_at = "2026-11-18T10:31:42Z";
      } else {
        rule.state = action === "reject" ? "rejected" : "removed";
      }
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ rule, applies_from: "next_ticket" }),
      });
    },
  );
  return posts;
}

async function openMemory(page: Page) {
  await page.goto("/?api=1");
  await page.getByRole("navigation").getByText("Memory", { exact: true }).click();
  await expect(page.getByRole("heading", { name: "Memory", level: 1 })).toBeVisible();
}

test("Memory page shows the design, accepts a proposed rule and removes it again", async ({ page }) => {
  const rules = [proposed(1, "Run the formatter before every commit.")];
  const posts = await stubApi(page, rules);
  await openMemory(page);

  // Heading, description, repository chip, sub-tabs.
  await expect(
    page.getByText(`What this factory has learned on ${REPO}. A rule applies only after someone who can merge here accepts it.`),
  ).toBeVisible();
  await expect(page.getByText(REPO, { exact: true })).toBeVisible();
  const sections = page.getByRole("group", { name: "Memory sections" });
  await expect(sections.getByRole("button", { name: "Learned rules" })).toHaveAttribute("aria-pressed", "true");
  await expect(sections.getByRole("button", { name: "Facts" })).toHaveAttribute("aria-pressed", "false");

  // Proposed: one card with every line of the design; In force: empty.
  const card = page.getByTestId("memory-rule");
  await expect(page.getByRole("heading", { name: "Proposed" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Proposed" }).locator("..").getByText("1", { exact: true })).toBeVisible();
  await expect(card).toHaveCount(1);
  await expect(card.getByText("Run the formatter before every commit.")).toBeVisible();
  await expect(card.getByText("Learned from", { exact: false })).toBeVisible();
  await expect(card.getByRole("link", { name: "octocat's comment on #12" })).toHaveAttribute(
    "href",
    "https://github.com/acme-corp/acme-bot/pull/12",
  );
  await expect(card.getByText("Why this is a rule and not a one-off: Reviewers asked for it twice.")).toBeVisible();
  await expect(card.getByText("Scope: this repository")).toBeVisible();
  await expect(card.getByText("Applies to: every factory run here, for everyone")).toBeVisible();
  await expect(card.getByRole("button", { name: "Accept", exact: true })).toBeVisible();
  await expect(card.getByRole("button", { name: "Dismiss", exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { name: "In force" })).toBeVisible();
  await expect(page.getByText("No rules yet.")).toBeVisible();

  // What wins.
  await expect(page.getByRole("heading", { name: "What wins" })).toBeVisible();
  await expect(page.getByText("strongest first")).toBeVisible();
  const wins = page.getByRole("listitem");
  await expect(wins).toHaveText([
    "Platform and bundle rules",
    "The repository's own instruction files",
    /^Learned rules for this repository\s*managed here$/,
    /^Your personal coding instructions\s*set per developer, never override a repository rule$/,
  ]);
  await expect(page.getByText("Like a project file winning over a personal one.")).toBeVisible();

  // Accept moves the rule to In force with the accepted line.
  await card.getByRole("button", { name: "Accept", exact: true }).click();
  await expect(page.getByText("Nothing waiting for review.")).toBeVisible();
  await expect(
    page.getByText("Accepted by Acme Owner, who can merge to this repository, on 2026-11-18 10:31 UTC"),
  ).toBeVisible();
  expect(posts).toEqual([{ action: "accept", body: { expected_version: 1 } }]);

  // Remove leaves the note and the empty text; the rule stays removed.
  await page.getByRole("button", { name: "Remove", exact: true }).click();
  await expect(page.getByText("Removed", { exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: /undo|restore/i })).toHaveCount(0);
  await expect(page.getByText("No rules yet.")).toBeVisible();
  expect(posts[1]).toEqual({ action: "remove", body: { expected_version: 2 } });

  // Facts: the facts panel, without the rules description.
  await sections.getByRole("button", { name: "Facts" }).click();
  await expect(sections.getByRole("button", { name: "Facts" })).toHaveAttribute("aria-pressed", "true");
  await expect(page.getByTestId("agent-memory")).toBeVisible();
  await expect(page.getByText("deploy is a git push")).toBeVisible();
  await expect(page.getByText(/What this factory has learned on/)).toHaveCount(0);
});

test("Memory page dismisses a proposed rule for good", async ({ page }) => {
  const rules = [proposed(1, "Run the formatter before every commit.")];
  const posts = await stubApi(page, rules);
  await openMemory(page);

  await page.getByRole("button", { name: "Dismiss", exact: true }).click();
  await expect(page.getByText("Dismissed", { exact: true })).toBeVisible();
  await expect(page.getByText("Nothing waiting for review.")).toBeVisible();
  await expect(page.getByRole("button", { name: /undo|restore/i })).toHaveCount(0);
  expect(posts).toEqual([{ action: "reject", body: { expected_version: 1 } }]);
});

test("Memory page shows a stale version refusal on the card and refreshes", async ({ page }) => {
  const rules = [proposed(1, "Run the formatter before every commit.")];
  await stubApi(page, rules);
  await openMemory(page);

  // Another person changed the rule after the page loaded.
  rules[0].version = 5;
  await page.getByRole("button", { name: "Accept", exact: true }).click();
  await expect(page.getByTestId("memory-rule-refusal")).toHaveText("This rule changed. The list was refreshed.");
  // The refreshed card carries the new version, so the next click goes through.
  await page.getByRole("button", { name: "Accept", exact: true }).click();
  await expect(page.getByText(/Accepted by Acme Owner/)).toBeVisible();
});

for (const [width, height] of [
  [1280, 900],
  [390, 844],
] as const) {
  test(`Memory page screenshots at ${width}x${height} have no horizontal scroll`, async ({ page }, testInfo) => {
    await page.setViewportSize({ width, height });
    const accepted = { ...proposed(2, "Keep pull requests under 400 changed lines."), state: "accepted" as const, version: 2 };
    accepted.accepted_by = { principal_id: "00000000-0000-4000-8000-000000000009", display_name: "Acme Owner" };
    accepted.accepted_at = "2026-11-18T10:31:42Z";
    await stubApi(page, [proposed(1, "Run the formatter before every commit."), accepted]);
    await openMemory(page);
    await expect(page.getByTestId("memory-rule")).toHaveCount(2);

    for (const [name, tab] of [
      ["learned-rules", "Learned rules"],
      ["facts", "Facts"],
    ] as const) {
      await page.getByRole("group", { name: "Memory sections" }).getByRole("button", { name: tab }).click();
      if (name === "facts") await expect(page.getByText("deploy is a git push")).toBeVisible();
      else await expect(page.getByRole("heading", { name: "What wins" })).toBeVisible();
      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);
      const path = testInfo.outputPath(`memory-${name}-${width}x${height}.png`);
      await page.screenshot({ path, fullPage: true });
      await testInfo.attach(`memory-${name}-${width}x${height}`, { path, contentType: "image/png" });
    }
  });
}

test("Observability no longer has a Memory tab", async ({ page }) => {
  await stubApi(page, []);
  await page.goto("/?api=1");
  await page.getByRole("navigation").getByText("Observability", { exact: true }).click();
  await expect(page.getByRole("button", { name: "Traces", exact: true })).toBeVisible();
  // The sidebar's own Memory item stays; only the Observability tabs lost theirs.
  await expect(page.getByRole("navigation").getByRole("button", { name: "Memory" })).toHaveCount(1);
  await expect(page.locator(".app-main").getByRole("button", { name: "Memory", exact: true })).toHaveCount(0);
});
