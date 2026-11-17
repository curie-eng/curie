import { expect, test, type Page } from "@playwright/test";

const github = { id: "11111111-1111-1111-1111-111111111111", provider: "github", display_label: "acme-developer", created_at: "2026-11-17T12:00:00Z" };
const slack = { ...github, id: "22222222-2222-2222-2222-222222222222", provider: "slack", display_label: "U0EXAMPLE1" };

async function stub(page: Page, available: boolean, initial = [github, slack]) {
  let links = [...initial];
  const writes: string[] = [];
  await page.route("**/api/**", (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    let body: unknown = {};
    if (path === "/api/console/session") {
      body = { subject: "principal:33333333-3333-3333-3333-333333333333", kind: "principal", expires_at: "2099-01-01T00:00:00Z", principal: { id: "33333333-3333-3333-3333-333333333333", display_name: "Acme Owner", email: "owner@example.com" } };
    } else if (path === "/api/personal-claude/enrollment") {
      body = { state: "absent", generation: null, version: null, teardown: { pending_runtimes: 0, pending_custody_cleanup: 0 } };
    } else if (path === "/api/identity/me/links") {
      body = { links, github_link_available: available };
    } else if (request.method() === "DELETE" && path.startsWith("/api/identity/me/links/")) {
      writes.push(path);
      links = links.filter((link) => !path.endsWith(link.id));
      body = { link_id: path.split("/").at(-1), revoked_at: "2026-11-17T13:00:00Z" };
    } else if (path === "/api/identity/me/github-link") {
      writes.push(path);
      body = { authorize_url: "https://github.example.com/login/oauth/authorize?client_id=example&state=example" };
    } else if (path === "/api/agents") {
      body = [];
    } else if (path === "/api/config") {
      body = { org_name: "Acme" };
    }
    return route.fulfill({ contentType: "application/json", body: JSON.stringify(body) });
  });
  return writes;
}

test("github_link lists accounts and confirms each unlink", async ({ page }) => {
  const writes = await stub(page, false);
  await page.goto("/settings/claude-plan");
  await expect(page.getByTestId("linked-account")).toHaveCount(2);
  await expect(page.getByText("GitHub: acme-developer", { exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "Link GitHub", exact: true })).toHaveCount(0);
  await page.getByRole("button", { name: "Unlink", exact: true }).first().click();
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  expect(writes).toEqual([]);
  for (const link of [github, slack]) {
    await page.getByRole("button", { name: "Unlink", exact: true }).first().click();
    await expect(page.getByText(`Unlink ${link.display_label}?`, { exact: true })).toBeVisible();
    await page.getByRole("button", { name: "Unlink account", exact: true }).click();
    await expect(page.getByText(`${link.provider === "github" ? "GitHub" : "Slack"}: ${link.display_label}`, { exact: true })).toHaveCount(0);
  }
  expect(writes).toEqual([`/api/identity/me/links/${github.id}`, `/api/identity/me/links/${slack.id}`]);
  await expect(page.getByText("No accounts linked yet.", { exact: true })).toBeVisible();
});

for (const [result, sentence] of [
  ["linked", "Your GitHub account is linked."],
  ["taken", "That GitHub account is already linked; unlink it from its current account first."],
  ["failed", "GitHub could not be linked; try again from this page."],
]) {
  test(`github_link shows ${result} and removes the result from the address bar`, async ({ page }) => {
    await stub(page, false, []);
    await page.goto(`/settings/claude-plan?github=${result}&keep=1`);
    await expect(page.getByRole("status").filter({ hasText: sentence })).toBeVisible();
    await expect(page).toHaveURL(/\/settings\/claude-plan\?keep=1$/);
    await expect(page.getByRole("button", { name: "Link GitHub", exact: true })).toHaveCount(0);
  });
}

test("github_link navigates through the authorization start", async ({ page }) => {
  const writes = await stub(page, true, []);
  await page.route("https://github.example.com/**", (route) => route.fulfill({ contentType: "text/html", body: "<p>GitHub authorization</p>" }));
  await page.goto("/settings/claude-plan");
  await page.getByRole("button", { name: "Link GitHub", exact: true }).click();
  await expect(page).toHaveURL("https://github.example.com/login/oauth/authorize?client_id=example&state=example");
  expect(writes).toEqual(["/api/identity/me/github-link"]);
});
