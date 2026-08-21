import { test, expect, type Page, type Route } from "@playwright/test";

// #870: the wired behavior-packs panel on the agent-detail page. Open an agent,
// toggle a pack + edit a load line, Save, and assert the PUT carried the full
// config with the edit applied. Backend stubbed with real-shaped responses, so
// this runs stackless (chromium project).

const AGENT = { id: "a1", name: "deal-desk", channels: [{ kind: "slack", address: "C0123ABCD" }], model: null, created_at: "2026-07-01T00:00:00Z" };

const DEFAULT_PACKS = {
  load: { enabled: false, lines: [] as string[] },
  tips: { enabled: false, tips: [] as string[] },
  greeting: { enabled: false, phrases: [] as string[], reply: "" },
  help: { enabled: false, phrases: [] as string[], reply: "" },
  settings: { enabled: false, settings: [] as unknown[] },
  nav: { enabled: false, hub_label: "", hub_command: "" },
};

interface Recorder {
  putBody: Record<string, unknown> | null;
}

async function stub(page: Page, rec: Recorder) {
  const json = (route: Route, status: number, body: unknown) =>
    route.fulfill({ status, contentType: "application/json", body: JSON.stringify(body) });

  // Minimal agent-detail scaffolding so the page renders down to the panel.
  await page.route("**/api/agents/*/versions/*/files*", (route) =>
    json(route, 200, { files: [{ path: "skills/deal-desk/SKILL.md", content: "# Policy" }] }),
  );
  await page.route("**/api/agents/*/versions", (route) =>
    json(route, 200, [
      { id: "v1", agent_id: "a1", version_label: "v0.1.0", bundle_ref: "b", bundle_sha256: "s", commit_sha: null, created_by: "ui", created_at: "2026-07-01T00:00:00Z" },
    ]),
  );
  await page.route("**/api/deployments*", (route) =>
    json(route, 200, [
      { id: "d1", agent_id: "a1", version_id: "v1", environment: "prod", commit_sha: null, status: "active", deployed_at: "2026-07-01T00:00:00Z" },
    ]),
  );

  // The read/inspect sibling panels: empty is fine, they must not error the page.
  await page.route("**/api/agents/*/memory*", (route) => json(route, 200, []));
  await page.route("**/api/agents/*/state", (route) => json(route, 200, []));

  // The subject under test: GET returns the all-off default; PUT echoes the body.
  await page.route("**/api/agents/*/behavior-packs", (route) => {
    if (route.request().method() === "PUT") {
      rec.putBody = JSON.parse(route.request().postData() ?? "{}");
      return json(route, 200, rec.putBody);
    }
    return json(route, 200, DEFAULT_PACKS);
  });

  await page.route("**/api/agents", (route) => json(route, 200, [AGENT]));
}

test("view and save an agent's behavior packs", async ({ page }) => {
  const rec: Recorder = { putBody: null };
  await stub(page, rec);

  await page.goto("/?api=1");
  await page.getByRole("navigation").getByText("Agents", { exact: true }).click();
  await page.getByTestId("agent-card-name").click();

  const panel = page.getByTestId("agent-behavior-packs");
  await expect(panel).toBeVisible();

  // Save is gated until an edit makes the panel dirty.
  const save = page.getByTestId("behavior-packs-save");
  await expect(save).toBeDisabled();

  // Enable the load pack and add a rotating line.
  await panel.getByTestId("pack-toggle-load").check();
  await panel.getByTestId("load-lines").fill("crunching numbers…");
  await expect(save).toBeEnabled();

  await save.click();

  // Success indicator, and the PUT carried the full config with the edit applied.
  await expect(page.getByTestId("behavior-packs-saved")).toHaveText(/Saved/);
  expect(rec.putBody?.load).toEqual({ enabled: true, lines: ["crunching numbers…"] });
  // Untouched packs are round-tripped intact.
  expect(rec.putBody?.nav).toEqual({ enabled: false, hub_label: "", hub_command: "" });
});
