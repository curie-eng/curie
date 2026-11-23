import { expect, test, type Page, type Route } from "@playwright/test";

// Stackless Claude plan page (#5716). Titles carry the `claude_plan` selector
// the ticket's -g filters use. The API is stubbed with the owner routes' shapes
// (apps/api/src/curie_api/schemas/personal_enrollments.py).

const EXPIRES_AT = "2026-07-24T12:00:00+00:00";
const PRINCIPAL = {
  subject: "principal:11111111-1111-1111-1111-111111111111",
  expires_at: EXPIRES_AT,
  kind: "principal",
  principal: { id: "11111111-1111-1111-1111-111111111111", display_name: "Alice Example", email: "alice@example.com" },
};
const SENTINEL = "SENTINEL-claude-plan-token-9f8e7d6c";
const ENROLLMENT = "/api/personal-claude/enrollment";

type Enrollment = {
  state: "absent" | "active" | "inactive";
  generation: number | null;
  version: number | null;
  teardown: { pending_runtimes: number; pending_custody_cleanup: number };
};
const none = { pending_runtimes: 0, pending_custody_cleanup: 0 };

interface Seen {
  method: string;
  url: string;
  headers: Record<string, string>;
  body: string | null;
}

async function stub(
  page: Page,
  opts: { session: "principal" | "operator"; start?: Enrollment; staleWrites?: number[]; holdWrites?: boolean },
): Promise<{ seen: Seen[]; releaseWrites: () => void }> {
  const seen: Seen[] = [];
  let current: Enrollment = opts.start ?? { state: "absent", generation: null, version: null, teardown: none };
  let writes = 0;
  let releaseWrites = () => {};
  const held = opts.holdWrites
    ? new Promise<void>((resolve) => {
        releaseWrites = resolve;
      })
    : null;
  const json = (route: Route, status: number, body: unknown) =>
    route.fulfill({ status, contentType: "application/json", headers: { "Cache-Control": "no-store" }, body: JSON.stringify(body) });

  await page.route((u) => u.pathname.startsWith("/api/"), (route) => json(route, 404, { detail: "Not Found" }));
  await page.route(
    (u) => u.pathname === "/api/console/session",
    (route) =>
      json(
        route,
        200,
        opts.session === "principal" ? PRINCIPAL : { subject: "operator@example.com", expires_at: EXPIRES_AT },
      ),
  );
  await page.route((u) => u.pathname === "/api/console/sign-in-methods", (route) => json(route, 200, { oidc: true }));
  await page.route((u) => u.pathname === "/api/agents", (route) => json(route, 200, []));
  await page.route((u) => u.pathname === "/api/config", (route) => json(route, 200, { org_name: "Globex Corporation" }));
  await page.route(
    (u) => u.pathname === ENROLLMENT,
    async (route) => {
      const req = route.request();
      seen.push({ method: req.method(), url: req.url(), headers: req.headers(), body: req.postData() });
      if (req.method() === "GET") return json(route, 200, current);
      if (req.method() === "PUT") {
        const attempt = writes++;
        if (held && attempt === 0) await held;
        if (opts.staleWrites?.includes(attempt)) return json(route, 409, { detail: { code: "stale_version" } });
        current = { state: "active", generation: (current.generation ?? 0) + 1, version: (current.version ?? 0) + 1, teardown: none };
        return json(route, 200, current);
      }
      current = { state: "inactive", generation: (current.generation ?? 0) + 1, version: (current.version ?? 0) + 1, teardown: none };
      return json(route, 200, current);
    },
  );
  return { seen, releaseWrites };
}

test.describe("claude_plan", () => {
  test("claude_plan walks absent, connect, replace, disconnect and reconnect with each version", async ({ page }) => {
    const { seen } = await stub(page, { session: "principal" });
    await page.goto("/settings/claude-plan?api=1");
    const status = page.getByTestId("claude-plan-status");
    await expect(status).toHaveText("Not connected");
    await expect(page.getByTestId("claude-plan-disconnect")).toHaveCount(0);

    await page.getByTestId("claude-plan-token").fill("tok-one");
    await page.getByTestId("claude-plan-write").click();
    await expect(status).toHaveText("Connected");
    await expect(page.getByTestId("claude-plan-generation")).toHaveText("Generation 1");

    await page.getByTestId("claude-plan-token").fill("tok-two");
    await page.getByTestId("claude-plan-write").click();
    await expect(status).toHaveText("Connected");
    await expect(page.getByTestId("claude-plan-generation")).toHaveText("Generation 2");

    await page.getByTestId("claude-plan-disconnect").click();
    await page.getByTestId("claude-plan-disconnect-confirm").click();
    await expect(status).toHaveText("Disconnected");
    await expect(page.getByTestId("claude-plan-disconnect")).toHaveCount(0);

    await page.getByTestId("claude-plan-token").fill("tok-three");
    await expect(page.getByTestId("claude-plan-write")).toHaveText("Reconnect");
    await page.getByTestId("claude-plan-write").click();
    await expect(status).toHaveText("Connected");
    await expect(page.getByTestId("claude-plan-generation")).toHaveText("Generation 4");

    const writes = seen
      .filter((s) => s.method !== "GET")
      .map((s) => (s.method === "PUT" ? JSON.parse(s.body ?? "{}") : { disconnect: new URL(s.url).search }));
    expect(writes).toEqual([
      { intent: "enroll", expected_version: null, token: "tok-one" },
      { intent: "replace", expected_version: 1, token: "tok-two" },
      { disconnect: "?expected_version=2" },
      { intent: "reconnect", expected_version: 3, token: "tok-three" },
    ]);
  });

  test("claude_plan and token never echoed on success and on a 409", async ({ page }) => {
    const consoleText: string[] = [];
    page.on("console", (m) => consoleText.push(m.text()));
    page.on("pageerror", (e) => consoleText.push(String(e)));
    const { seen } = await stub(page, { session: "principal", staleWrites: [1] });

    await page.goto("/settings/claude-plan?api=1");
    const input = page.getByTestId("claude-plan-token");
    await expect(page.getByTestId("claude-plan-status")).toHaveText("Not connected");

    await input.fill(SENTINEL);
    await page.getByTestId("claude-plan-write").click();
    await expect(page.getByTestId("claude-plan-status")).toHaveText("Connected");
    await expect(page.getByTestId("claude-plan-generation")).toHaveText("Generation 1");
    await expect(input).toHaveValue("");

    await input.fill(SENTINEL);
    await page.getByTestId("claude-plan-write").click();
    await expect(page.getByTestId("claude-plan-error")).toContainText("Your plan changed in another window");
    await expect(input).toHaveValue("");

    const bodies = seen.filter((s) => s.method === "PUT");
    expect(bodies).toHaveLength(2);
    for (const s of seen) {
      expect(s.url).not.toContain(SENTINEL);
      expect(JSON.stringify(s.headers)).not.toContain(SENTINEL);
    }
    for (const s of bodies) expect(JSON.parse(s.body ?? "{}").token).toBe(SENTINEL);
    expect(seen.filter((s) => s.method === "GET").every((s) => s.body === null)).toBe(true);

    const stored = await page.evaluate(async () => {
      const dbs = (await indexedDB.databases?.()) ?? [];
      return JSON.stringify({ local: { ...localStorage }, session: { ...sessionStorage }, dbs });
    });
    expect(stored).not.toContain(SENTINEL);
    expect(consoleText.join("\n")).not.toContain(SENTINEL);
    expect(await page.content()).not.toContain(SENTINEL);
    expect(await page.locator("body").innerText()).not.toContain(SENTINEL);
  });

  test("claude_plan and route opens directly and follows Back and Forward", async ({ page }) => {
    await stub(page, { session: "principal" });
    await page.goto("/settings/claude-plan?api=1");
    await expect(page.getByTestId("claude-plan-status")).toBeVisible();

    await page.getByRole("button", { name: "Workspace" }).click();
    await expect(page).toHaveURL(/\/settings\?api=1$|\/settings$/);
    await expect(page.getByText("Workspace name")).toBeVisible();
    await expect(page.getByTestId("claude-plan-status")).toHaveCount(0);

    await page.goBack();
    await expect(page).toHaveURL(/\/settings\/claude-plan/);
    await expect(page.getByTestId("claude-plan-status")).toBeVisible();

    await page.goForward();
    await expect(page.getByText("Workspace name")).toBeVisible();

    await page.getByRole("button", { name: "Claude plan" }).click();
    await expect(page).toHaveURL(/\/settings\/claude-plan$/);
    await expect(page.getByTestId("claude-plan-status")).toBeVisible();

    await page.getByRole("button", { name: "Overview" }).click();
    await expect(page).toHaveURL(/\/$/);
    await page.goBack();
    await expect(page.getByTestId("claude-plan-status")).toBeVisible();
  });

  test("claude_plan history restores Agents across Back and Forward", async ({ page }) => {
    await stub(page, { session: "principal" });
    await page.goto("/settings/claude-plan?api=1");
    await expect(page.getByTestId("claude-plan-status")).toBeVisible();

    await page.getByRole("button", { name: "Agents", exact: true }).click();
    await expect(page.getByRole("heading", { name: "Agents", exact: true })).toBeVisible();
    await page.getByRole("button", { name: "Settings", exact: true }).click();
    await expect(page).toHaveURL(/\/settings\/claude-plan$/);
    await expect(page.getByTestId("claude-plan-status")).toBeVisible();
    await page.goBack();
    await expect(page.getByRole("heading", { name: "Agents", exact: true })).toBeVisible();
    await expect(page).toHaveURL(/\/$/);

    await page.goto("/settings/claude-plan?api=1");
    await expect(page.getByTestId("claude-plan-status")).toBeVisible();
    await page.getByRole("button", { name: "Agents", exact: true }).click();
    await page.goBack();
    await expect(page).toHaveURL(/\/settings\/claude-plan/);
    await expect(page.getByTestId("claude-plan-status")).toBeVisible();
    await page.goForward();
    await expect(page.getByRole("heading", { name: "Agents", exact: true })).toBeVisible();

    await page.goto("/?api=1");
    await expect(page.getByRole("heading", { name: "Welcome to Curie" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Overview", exact: true })).toHaveAttribute("aria-current", "page");
    await page.getByRole("button", { name: "Agents", exact: true }).click();
    await expect(page.getByRole("heading", { name: "Agents", exact: true })).toBeVisible();
    await page.goBack();
    await expect(page.getByRole("heading", { name: "Welcome to Curie" })).toBeVisible();

    await page.getByRole("button", { name: "Agents", exact: true }).click();
    await expect(page.getByRole("heading", { name: "Agents", exact: true })).toBeVisible();
    await page.reload();
    await expect(page.getByRole("heading", { name: "Agents", exact: true })).toBeVisible();
    await expect(page.getByRole("button", { name: "Agents", exact: true })).toHaveAttribute("aria-current", "page");
    await page.getByRole("button", { name: "Settings", exact: true }).click();
    await expect(page.getByText("Workspace name")).toBeVisible();
    await page.goBack();
    await expect(page.getByRole("heading", { name: "Agents", exact: true })).toBeVisible();
    await expect(page).toHaveURL(/\/$/);
  });

  test("claude_plan a pending connect stays blocked after Workspace and back", async ({ page }) => {
    const { seen, releaseWrites } = await stub(page, { session: "principal", holdWrites: true });
    await page.goto("/settings/claude-plan?api=1");
    await page.getByTestId("claude-plan-token").fill(SENTINEL);
    await page.getByTestId("claude-plan-write").click();
    await expect(page.getByTestId("claude-plan-token")).toBeDisabled();
    await expect.poll(() => seen.filter((entry) => entry.method === "PUT").length).toBe(1);

    await page.getByRole("button", { name: "Workspace" }).click();
    await expect(page.getByText("Workspace name")).toBeVisible();
    await page.getByRole("button", { name: "Claude plan" }).click();
    await expect(page.getByTestId("claude-plan-token")).toBeDisabled();
    expect(seen.filter((entry) => entry.method === "PUT").length).toBe(1);

    releaseWrites();
    await expect(page.getByTestId("claude-plan-generation")).toHaveText("Generation 1");
    expect(seen.filter((entry) => entry.method === "PUT").length).toBe(1);
    expect(await page.locator("body").innerText()).not.toContain(SENTINEL);
  });

  test("claude_plan and route: an operator session sees the sign-in prompt and no form", async ({ page }) => {
    await stub(page, { session: "operator" });
    await page.goto("/settings/claude-plan?api=1");
    await expect(page.getByTestId("claude-plan-sign-in")).toHaveText(
      "Sign in with your organization to manage your Claude plan.",
    );
    await expect(page.getByTestId("claude-plan-oidc-sign-in")).toHaveAttribute(
      "href",
      "/api/console/oidc/login?return_to=%2Fsettings%2Fclaude-plan",
    );
    await expect(page.getByTestId("claude-plan-token")).toHaveCount(0);
  });
});
