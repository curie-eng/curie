import { randomBytes } from "node:crypto";
import { expect, test, type Page } from "@playwright/test";

// Live Claude plan page (#5716) against a throwaway install with Dex, run with
//   PW_INTEGRATION=1 PW_BASE_URL=<port-forwarded Console> \
//     pnpm e2e --project integration -g claude_plan
// The driver publishes the same variables as console-principal-session.spec.ts:
//   CURIE_OIDC_ISSUER_HOST / CURIE_OIDC_ISSUER_MAP  Dex host and Chromium target
//   CURIE_OIDC_TEST_EMAIL / CURIE_OIDC_TEST_PASSWORD  the Dex password user
// The tokens are well formed and were never issued, so no model is called. They
// are generated here and exist only in this process.

const issuerHost = process.env.CURIE_OIDC_ISSUER_HOST ?? "";
const issuerMap = process.env.CURIE_OIDC_ISSUER_MAP ?? "";
const email = process.env.CURIE_OIDC_TEST_EMAIL ?? "owner-a@example.com";
const password = process.env.CURIE_OIDC_TEST_PASSWORD ?? "password";

test.use({
  launchOptions: {
    args: issuerHost && issuerMap ? [`--host-resolver-rules=MAP ${issuerHost} ${issuerMap}`] : [],
  },
});

const neverIssued = () => "sk-ant-oat01-" + randomBytes(48).toString("base64url");

async function signInAtDex(page: Page): Promise<void> {
  const passwordInput = page.locator('input[name="password"]');
  const chooser = page.locator('a[href*="/dex/auth/local"]');
  await expect(passwordInput.or(chooser.first())).toBeVisible();
  if (!(await passwordInput.isVisible())) {
    await chooser.first().click();
    await expect(passwordInput).toBeVisible();
  }
  await page.locator('input[name="login"]').fill(email);
  await passwordInput.fill(password);
  await page.locator("form").filter({ has: passwordInput }).locator('[type="submit"]').click();
}

interface Enrollment {
  state: "absent" | "active" | "inactive";
  generation: number | null;
  version: number | null;
  teardown: { pending_runtimes: number; pending_custody_cleanup: number };
}

// The owner API read, through the same browser session the page uses.
async function owned(page: Page): Promise<Enrollment> {
  const resp = await page.request.get("/api/personal-claude/enrollment");
  expect(resp.status()).toBe(200);
  return (await resp.json()) as Enrollment;
}

test.describe("claude_plan", () => {
  test("claude_plan drives absent, connect, replace, disconnect and reconnect, each confirmed by the API", async ({ page }) => {
    await page.goto("/settings/claude-plan");
    await page.getByTestId("console-oidc-sign-in").click();
    await signInAtDex(page);
    await expect(page).toHaveURL(/\/settings\/claude-plan$/);

    const status = page.getByTestId("claude-plan-status");
    const input = page.getByTestId("claude-plan-token");
    const write = page.getByTestId("claude-plan-write");

    await expect(status).toHaveText("Not connected");
    expect((await owned(page)).state).toBe("absent");

    await input.fill(neverIssued());
    await write.click();
    await expect(status).toHaveText("Connected");
    await expect(page.getByTestId("claude-plan-generation")).toHaveText("Generation 1");
    await expect(input).toHaveValue("");
    expect(await owned(page)).toMatchObject({ state: "active", generation: 1 });

    await expect(write).toHaveText("Replace");
    await input.fill(neverIssued());
    await write.click();
    await expect(status).toHaveText("Connected");
    await expect(page.getByTestId("claude-plan-generation")).toHaveText("Generation 2");
    expect(await owned(page)).toMatchObject({ state: "active", generation: 2 });

    await page.getByTestId("claude-plan-disconnect").click();
    await page.getByTestId("claude-plan-disconnect-confirm").click();
    await expect(status).toHaveText("Disconnected");
    expect((await owned(page)).state).toBe("inactive");
    await expect
      .poll(async () => (await owned(page)).teardown, { timeout: 120_000, intervals: [2_000] })
      .toEqual({ pending_runtimes: 0, pending_custody_cleanup: 0 });
    await expect(page.getByTestId("claude-plan-teardown")).toHaveCount(0, { timeout: 30_000 });

    await expect(write).toHaveText("Reconnect");
    await input.fill(neverIssued());
    await write.click();
    await expect(status).toHaveText("Connected");
    await expect(page.getByTestId("claude-plan-generation")).toHaveText(/^Generation \d+$/);
    expect((await owned(page)).state).toBe("active");
  });
});
