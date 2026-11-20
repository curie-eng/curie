import { expect, test, type Page } from "@playwright/test";

// Live organization sign-in (#5682). No driver in this repo sets these.
// The caller does, as documented in apps/ui/README.md under Integration E2E:
//   CURIE_OIDC_ISSUER_HOST   hostname in the authorize redirect
//   CURIE_OIDC_ISSUER_MAP    Chromium host-resolver target, "ip" or "ip:port"
//   CURIE_OIDC_TEST_EMAIL    password user (default owner-a@example.com)
//   CURIE_OIDC_TEST_PASSWORD that user's password (default the public fixture password)
// PW_BASE_URL is the console origin. The redirect URI for that run is the
// console origin plus /api/console/oidc/callback, so the session cookie is
// set on the page the browser is looking at.

const issuerHost = process.env.CURIE_OIDC_ISSUER_HOST ?? "";
const issuerMap = process.env.CURIE_OIDC_ISSUER_MAP ?? "";
const email = process.env.CURIE_OIDC_TEST_EMAIL ?? "owner-a@example.com";
const password = process.env.CURIE_OIDC_TEST_PASSWORD ?? "password";
// This Dex password user has no separate name. Dex presents the username as
// the name claim, and the console shows that. The overview notice is only on
// Overview; the settings landing names the person in the top bar.
const accountName = email.split("@")[0] ?? email;

test.use({
  launchOptions: {
    args:
      issuerHost && issuerMap
        ? [`--host-resolver-rules=MAP ${issuerHost} ${issuerMap}`]
        : [],
  },
});

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

test.describe("principal_session", () => {
  test("organization sign-in lands on a console that names the person", async ({ page }) => {
    await page.goto("/");
    await page.getByTestId("console-oidc-sign-in").click();
    await signInAtDex(page);
    const notice = page.getByTestId("console-principal-notice");
    await expect(notice).toBeVisible();
    await expect(notice).toContainText("Signed in as");
    await expect(notice).toContainText("`curie cluster subscription`");
    await expect(notice).toContainText(accountName);
    await expect(page.getByTestId("console-principal")).toContainText(accountName);
    await expect(page.getByTestId("console-login")).toHaveCount(0);
  });

  test("a login started at /settings/claude-plan lands on that path", async ({ page }) => {
    await page.goto("/settings/claude-plan");
    await page.getByTestId("console-oidc-sign-in").click();
    await signInAtDex(page);
    await expect(page).toHaveURL(/\/settings\/claude-plan$/);
    await expect(page.getByRole("heading", { name: "Settings" })).toBeVisible();
    await expect(page.getByTestId("console-principal")).toContainText(accountName);
    await expect(page.getByTestId("console-login")).toHaveCount(0);
  });
});
