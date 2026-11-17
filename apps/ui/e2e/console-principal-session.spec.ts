import { expect, test, type Page, type Route } from "@playwright/test";

// Stackless principal console (#5682). The describe name is the `-g principal_session`
// selector. Stubs mirror ConsoleSessionOut: an operator body may omit `kind`,
// which still means a login-code session.

const EXPIRES_AT = "2026-07-24T12:00:00+00:00";
const PRINCIPAL = {
  subject: "principal:11111111-1111-1111-1111-111111111111",
  expires_at: EXPIRES_AT,
  kind: "principal",
  principal: {
    id: "11111111-1111-1111-1111-111111111111",
    display_name: "Alice Example",
    email: "alice@example.com",
  },
};
const NOTICE = "Signed in as Alice Example. This session also works with `curie cluster subscription`.";

async function stubConsole(
  page: Page,
  opts: { session: "principal" | "operator" | "out"; oidc: boolean },
): Promise<void> {
  const json = (route: Route, status: number, body: unknown) =>
    route.fulfill({
      status,
      contentType: "application/json",
      headers: { "Cache-Control": "no-store" },
      body: JSON.stringify(body),
    });

  await page.route(
    (url) => url.pathname.startsWith("/api/"),
    (route) => json(route, 404, { detail: "Not Found" }),
  );
  await page.route(
    (url) => url.pathname === "/api/console/session",
    (route) => {
      if (opts.session === "out") {
        return json(route, 401, { detail: "missing, invalid, or expired console session" });
      }
      const body =
        opts.session === "principal"
          ? PRINCIPAL
          : { subject: "operator@example.com", expires_at: EXPIRES_AT };
      return json(route, 200, body);
    },
  );
  await page.route(
    (url) => url.pathname === "/api/console/sign-in-methods",
    (route) => json(route, 200, { oidc: opts.oidc }),
  );
  await page.route(
    (url) => url.pathname === "/api/console/logout",
    (route) => {
      if (route.request().method() !== "POST") return json(route, 405, { detail: "Method Not Allowed" });
      return route.fulfill({ status: 204, headers: { "Cache-Control": "no-store" } });
    },
  );
  await page.route(
    (url) => url.pathname === "/api/agents",
    (route) => json(route, 200, []),
  );
  await page.route(
    (url) => url.pathname === "/api/config",
    (route) => json(route, 200, { org_name: "Globex Corporation" }),
  );
}

test.describe("principal_session", () => {
  test("a principal session shows the person's name and the overview notice", async ({ page }) => {
    await stubConsole(page, { session: "principal", oidc: false });
    await page.goto("/?api=1");
    await expect(page.getByTestId("console-login")).toHaveCount(0);
    await expect(page.getByTestId("console-principal")).toHaveText("Alice Example");
    await expect(page.getByTestId("console-principal-notice")).toHaveText(NOTICE);
    await expect(page.getByRole("button", { name: "Sign out" })).toBeVisible();
  });

  test("an operator session does not gain principal chrome", async ({ page }) => {
    await stubConsole(page, { session: "operator", oidc: false });
    await page.goto("/?api=1");
    await expect(page.getByRole("navigation")).toBeVisible();
    await expect(page.getByRole("heading", { name: "Welcome to Curie" })).toBeVisible();
    await expect(page.getByTestId("console-principal")).toHaveCount(0);
    await expect(page.getByTestId("console-principal-notice")).toHaveCount(0);
    await expect(page.getByTestId("console-sign-out")).toHaveCount(0);
    await expect(page.getByTestId("console-login")).toHaveCount(0);
  });

  test("the organization link follows oidc and the allowed return paths", async ({ page }) => {
    await stubConsole(page, { session: "out", oidc: true });
    await page.goto("/?api=1");
    await expect(page.getByTestId("console-oidc-sign-in")).toHaveAttribute(
      "href",
      "/api/console/oidc/login?return_to=%2F",
    );
    await page.goto("/settings/claude-plan?api=1");
    await expect(page.getByTestId("console-oidc-sign-in")).toHaveAttribute(
      "href",
      "/api/console/oidc/login?return_to=%2Fsettings%2Fclaude-plan",
    );
    await page.goto("/agents?api=1");
    await expect(page.getByTestId("console-oidc-sign-in")).toHaveAttribute("href", "/api/console/oidc/login");
  });

  test("hides the organization link when oidc is off", async ({ page }) => {
    await stubConsole(page, { session: "out", oidc: false });
    const methods = page.waitForResponse((response) => response.url().includes("/api/console/sign-in-methods"));
    await page.goto("/?api=1");
    await methods;
    await expect(page.getByTestId("console-login")).toBeVisible();
    await expect(page.getByTestId("console-oidc-sign-in")).toHaveCount(0);
  });

  test("sign-out returns to the login screen", async ({ page }) => {
    await stubConsole(page, { session: "principal", oidc: false });
    await page.goto("/?api=1");
    await page.getByTestId("console-sign-out").click();
    await expect(page.getByTestId("console-login")).toBeVisible();
    await expect(page.getByTestId("console-principal")).toHaveCount(0);
  });

  test("only /settings/claude-plan opens settings", async ({ page }) => {
    await stubConsole(page, { session: "principal", oidc: false });
    await page.goto("/settings/claude-plan?api=1");
    await expect(page.getByRole("heading", { name: "Settings" })).toBeVisible();
    await page.goto("/agents?api=1");
    await expect(page.getByRole("heading", { name: "Welcome to Curie" })).toBeVisible();
    await page.goto("/?api=1");
    await expect(page.getByRole("heading", { name: "Welcome to Curie" })).toBeVisible();
  });
});
