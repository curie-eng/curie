import { randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { expect, test, request, type Page } from "@playwright/test";

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Set ${name} for the GitHub link live fixture`);
  return value;
}

const issuerHost = process.env.CURIE_OIDC_ISSUER_HOST ?? "";
const issuerMap = process.env.CURIE_OIDC_ISSUER_MAP ?? "";
const stubHost = process.env.CURIE_GITHUB_LINK_STUB_HOST ?? "";
const stubMap = process.env.CURIE_GITHUB_LINK_STUB_MAP ?? "";
const maps = [[issuerHost, issuerMap], [stubHost, stubMap]].filter(([host, target]) => host && target).map(([host, target]) => `MAP ${host} ${target}`);

test.use({ ignoreHTTPSErrors: true, launchOptions: { args: maps.length ? [`--host-resolver-rules=${maps.join(", ")}`] : [] } });

test.beforeAll(() => {
  for (const name of ["CURIE_GITHUB_LINK_STUB_URL", "CURIE_GITHUB_LINK_AGENT_ID", "CURIE_GITHUB_LINK_API_URL", "CURIE_GITHUB_LINK_WORKER_TOKEN_FILE"]) required(name);
});

async function signInAtDex(page: Page): Promise<void> {
  const passwordInput = page.locator('input[name="password"]');
  const chooser = page.locator('a[href*="/dex/auth/local"]');
  await expect(passwordInput.or(chooser.first())).toBeVisible();
  if (!(await passwordInput.isVisible())) await chooser.first().click();
  await passwordInput.fill(process.env.CURIE_OIDC_TEST_PASSWORD ?? "password");
  await page.locator('input[name="login"]').fill(process.env.CURIE_OIDC_TEST_EMAIL ?? "owner-a@example.com");
  await page.locator("form").filter({ has: passwordInput }).locator('[type="submit"]').click();
}

test("github_link live browser proves account ownership and label admission owner", async ({ page }) => {
  await page.goto("/settings/claude-plan");
  await page.getByTestId("console-oidc-sign-in").click();
  await signInAtDex(page);
  await expect(page).toHaveURL(/\/settings\/claude-plan$/);
  const sessionResponse = await page.request.get("/api/console/session");
  expect(sessionResponse.status()).toBe(200);
  const session = await sessionResponse.json();
  const principal = session.principal.id;
  await page.getByRole("button", { name: "Link GitHub", exact: true }).click();
  await expect(page).toHaveURL(/\/login\/oauth\/authorize\?/);
  await page.getByRole("button", { name: "Authorize GitHub link", exact: true }).click();
  await expect(page.getByRole("status").filter({ hasText: "Your GitHub account is linked." })).toBeVisible();
  await expect(page).toHaveURL(/\/settings\/claude-plan$/);
  await expect(page.getByText("GitHub: octocat", { exact: true })).toBeVisible();
  const linked = await page.request.get("/api/identity/me/links");
  expect((await linked.json()).links).toEqual(expect.arrayContaining([expect.objectContaining({ provider: "github", display_label: "octocat" })]));

  // No provider grant or model call: an unused well-formed token enables
  // admission while the fixture keeps workers stopped.
  const enrollment = await page.request.put("/api/personal-claude/enrollment", {
    headers: { Origin: new URL(page.url()).origin },
    data: { intent: "enroll", expected_version: null, token: "sk-ant-oat01-" + randomBytes(48).toString("base64url") },
  });
  expect(enrollment.status()).toBe(200);
  const generation = (await enrollment.json()).generation;
  const stub = await request.newContext({ baseURL: required("CURIE_GITHUB_LINK_STUB_URL"), ignoreHTTPSErrors: true, extraHTTPHeaders: { Authorization: "Bearer example-human-token" } });
  const issue = process.env.CURIE_GITHUB_LINK_ISSUE ?? "3815";
  const label = process.env.CURIE_GITHUB_LINK_LABEL ?? "factory";
  try {
    const agent = required("CURIE_GITHUB_LINK_AGENT_ID");
    const before = await page.request.get(`/api/work-items?agent_id=${encodeURIComponent(agent)}`);
    expect(before.status()).toBe(200);
    expect((await before.json()).items).toEqual([]);
    const removed = await stub.delete(`/api/v3/repos/acme-corp/acme-bot/issues/${issue}/labels/${encodeURIComponent(label)}`);
    expect(removed.status()).toBe(200);
    const opened = await stub.patch(`/api/v3/repos/acme-corp/acme-bot/issues/${issue}`, { data: { state: "open" } });
    expect(opened.status()).toBe(200);
    const labelled = await stub.post(`/api/v3/repos/acme-corp/acme-bot/issues/${issue}/labels`, { data: { labels: [label] } });
    expect(labelled.status()).toBe(200);
    let admitted: { id: string; tenant_id: string; owner_principal_id: string; model_auth_mode: string; enrollment_generation: number } | undefined;
    await expect.poll(async () => {
      const response = await page.request.get(`/api/work-items?agent_id=${encodeURIComponent(agent)}`);
      expect(response.status()).toBe(200);
      const body = await response.json();
      admitted = body.items.find((item: { tracker: { issue_id: string } }) => item.tracker.issue_id === issue);
      return admitted?.owner_principal_id;
    }, { timeout: 90_000, intervals: [1_000] }).toBe(principal);
    expect(admitted).toMatchObject({ model_auth_mode: "personal", enrollment_generation: generation });
    const machine = await request.newContext({ baseURL: required("CURIE_GITHUB_LINK_API_URL"), extraHTTPHeaders: { "X-Curie-Worker-Token": readFileSync(required("CURIE_GITHUB_LINK_WORKER_TOKEN_FILE"), "utf8").trim() } });
    try {
      const context = await machine.get(`/v1/internal/work-items/${admitted!.id}/personal-context?` + new URLSearchParams({ tenant_id: admitted!.tenant_id, agent_id: agent, owner_principal_id: principal }));
      expect(context.status()).toBe(200);
      expect(await context.json()).toMatchObject({ model_auth_mode: "personal", owner_principal_id: principal, enrollment_generation: generation });
    } finally {
      await machine.dispose();
    }
  } finally {
    await stub.dispose();
  }
});
