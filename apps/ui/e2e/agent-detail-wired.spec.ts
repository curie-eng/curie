import { test, expect, type Page } from "@playwright/test";

// FX2 headline: the wired agent-detail surface. Open an agent from the Agents
// list, see its active version's SKILL.md, edit it, and ship a new version via
// the create-path sequence (POST version + PUT bundle + activate deployment).
// The backend is stubbed with real-shaped responses, so this runs stackless.

const AGENT = { id: "a1", name: "deal-desk", channels: [{ kind: "slack", address: "C0123ABCD" }], created_at: "2026-07-01T00:00:00Z" };

const SKILL_V1 =
  "---\nname: deal-desk\ndescription: Approves deals\ntools: [slack]\n---\n# Policy\nAuto-approve up to 15%.";

interface Recorder {
  postedVersionLabel: string | null;
  putBundle: boolean;
  postedDeploymentVersion: string | null;
}

async function stubAgentDetail(page: Page, rec: Recorder) {
  // Mutable server state so the post-deploy refetch sees the new active version.
  const versions: Array<Record<string, unknown>> = [
    {
      id: "v1",
      agent_id: "a1",
      version_label: "v0.1.0",
      bundle_ref: "bundles/a1/v1.zip",
      bundle_sha256: "sha1",
      commit_sha: null,
      created_by: "ui",
      created_at: "2026-07-01T00:00:00Z",
    },
  ];
  const deployments: Array<Record<string, unknown>> = [
    { id: "d1", agent_id: "a1", version_id: "v1", environment: "prod", commit_sha: null, status: "active", deployed_at: "2026-07-01T00:00:00Z" },
  ];
  const filesByVersion: Record<string, string> = { v1: SKILL_V1 };

  const json = (route: import("@playwright/test").Route, status: number, body: unknown) =>
    route.fulfill({ status, contentType: "application/json", body: JSON.stringify(body) });

  await page.route("**/api/agents/*/versions/*/files*", (route) => {
    const vid = route.request().url().split("/versions/")[1].split("/")[0];
    const content = filesByVersion[vid];
    if (!content) return json(route, 404, { detail: "no bundle stored for this version" });
    return json(route, 200, { files: [{ path: "skills/deal-desk/SKILL.md", content }] });
  });

  await page.route("**/api/agents/*/versions/*/bundle*", (route) => {
    rec.putBundle = true;
    return json(route, 201, { version_id: "v2", bundle_ref: "bundles/a1/v2.zip", bundle_sha256: "sha2", size_bytes: 512 });
  });

  await page.route("**/api/agents/*/versions", (route) => {
    if (route.request().method() === "POST") {
      const body = JSON.parse(route.request().postData() ?? "{}");
      rec.postedVersionLabel = body.version_label;
      const v = {
        id: "v2",
        agent_id: "a1",
        version_label: body.version_label,
        bundle_ref: null,
        bundle_sha256: null,
        commit_sha: null,
        created_by: "ui",
        created_at: "2026-07-02T00:00:00Z",
      };
      versions.push(v);
      // The new version's bundle becomes readable after the PUT above.
      filesByVersion["v2"] = SKILL_V1 + "\n# Extra rule added in the UI.";
      return json(route, 201, v);
    }
    return json(route, 200, versions);
  });

  await page.route("**/api/deployments*", (route) => {
    if (route.request().method() === "POST") {
      const body = JSON.parse(route.request().postData() ?? "{}");
      rec.postedDeploymentVersion = body.version_id;
      deployments.push({ id: "d2", agent_id: "a1", version_id: body.version_id, environment: "prod", commit_sha: null, status: "active", deployed_at: "2026-07-02T00:00:00Z" });
      return json(route, 201, deployments[deployments.length - 1]);
    }
    return json(route, 200, deployments);
  });

  await page.route("**/api/agents", (route) => json(route, 200, [AGENT]));
}

test("open an agent, edit its skill, and deploy a new version", async ({ page }) => {
  const rec: Recorder = { postedVersionLabel: null, putBundle: false, postedDeploymentVersion: null };
  await stubAgentDetail(page, rec);

  await page.goto("/?api=1");
  await page.getByRole("navigation").getByText("Agents", { exact: true }).click();

  // Open the agent from the list.
  await page.getByTestId("agent-card-name").click();
  await expect(page.getByTestId("agent-detail-name")).toHaveText("deal-desk");
  await expect(page.getByText("active v0.1.0")).toBeVisible();

  // The active version's SKILL.md is shown in the editor.
  const editor = page.getByTestId("skill-editor");
  await expect(editor).toHaveValue(/Auto-approve up to 15%/);

  // Edit it and deploy a new version.
  await editor.fill(SKILL_V1 + "\nAuto-approve up to 20%.");
  await page.getByRole("button", { name: "Deploy new version" }).click();

  // Success: the bumped label is shown and the full sequence fired.
  await expect(page.getByTestId("deploy-success")).toContainText("Deployed v0.1.1");
  expect(rec.postedVersionLabel).toBe("v0.1.1");
  expect(rec.putBundle).toBe(true);
  expect(rec.postedDeploymentVersion).toBe("v2");

  // After the refetch the newly deployed version is the active one.
  await expect(page.getByText("active v0.1.1")).toBeVisible();

  // Back returns to the Agents list it was opened from (not Overview).
  await page.getByRole("button", { name: "← Agents" }).click();
  await expect(page.getByTestId("agent-card-name")).toBeVisible();
  await expect(page.getByRole("button", { name: "New agent" })).toBeVisible();
});

// ADR-0116/S5.5: a two-binding agent renders every bound address
// (`a.channels.map(c => c.address)`), not just the first one.
test("a two-binding agent renders both addresses", async ({ page }) => {
  const json = (route: import("@playwright/test").Route, status: number, body: unknown) =>
    route.fulfill({ status, contentType: "application/json", body: JSON.stringify(body) });
  const twoBindingAgent = {
    id: "a3",
    name: "multi-channel-bot",
    channels: [
      { kind: "slack", address: "C0EXAMPLE1" },
      { kind: "slack", address: "C0EXAMPLE2" },
    ],
    created_at: "2026-07-01T00:00:00Z",
  };
  await page.route("**/api/agents", (route) => json(route, 200, [twoBindingAgent]));
  await page.route("**/api/agents/*/versions", (route) => json(route, 200, []));
  await page.route("**/api/deployments*", (route) => json(route, 200, []));

  await page.goto("/?api=1");
  await page.getByRole("navigation").getByText("Agents", { exact: true }).click();

  await expect(page.getByText("C0EXAMPLE1").first()).toBeVisible();
  await expect(page.getByText("C0EXAMPLE2").first()).toBeVisible();
});

test("adding Discord implicitly opts the same agent into multiple surfaces", async ({ page }) => {
  const json = (route: import("@playwright/test").Route, status: number, body: unknown) =>
    route.fulfill({ status, contentType: "application/json", body: JSON.stringify(body) });
  let posted: Record<string, unknown> | null = null;
  await page.route("**/api/agents/a1/channels", async (route) => {
    posted = JSON.parse(route.request().postData() ?? "{}");
    return json(route, 201, {
      ...AGENT,
      channels: [
        ...AGENT.channels,
        { kind: "discord", address: "111111111111111111" },
      ],
    });
  });
  await page.route("**/api/agents/*/versions", (route) => json(route, 200, []));
  await page.route("**/api/deployments*", (route) => json(route, 200, []));
  await page.route("**/api/agents", (route) => json(route, 200, [AGENT]));

  await page.goto("/?api=1");
  await page.getByRole("navigation").getByText("Agents", { exact: true }).click();
  await page.getByTestId("agent-card-name").click();
  await page.getByTestId("surface-kind-new").fill("discord");
  await page.getByTestId("surface-address-new").fill("111111111111111111");
  await page.getByTestId("surface-endpoint-new").fill("https://discord-adapter.example.com/replies");
  await page.getByTestId("surface-adapter-new").fill("discord-main");
  await page.getByTestId("surface-add").click();

  await expect.poll(() => posted).toEqual({
    kind: "discord",
    address: "111111111111111111",
    endpoint: "https://discord-adapter.example.com/replies",
    adapter: "discord-main",
  });
  await expect(page.getByText("Surface added: discord:111111111111111111")).toBeVisible();
});

test("an agent whose active version has no bundle shows an honest empty state", async ({ page }) => {
  const json = (route: import("@playwright/test").Route, status: number, body: unknown) =>
    route.fulfill({ status, contentType: "application/json", body: JSON.stringify(body) });
  await page.route("**/api/agents/*/versions/*/files*", (route) =>
    json(route, 404, { detail: "no bundle stored for this version" }),
  );
  await page.route("**/api/agents/*/versions", (route) =>
    json(route, 200, [
      { id: "v1", agent_id: "a1", version_label: "v0.1.0", bundle_ref: null, bundle_sha256: null, commit_sha: null, created_by: "ui", created_at: "2026-07-01T00:00:00Z" },
    ]),
  );
  await page.route("**/api/deployments*", (route) => json(route, 200, []));
  await page.route("**/api/agents", (route) => json(route, 200, [AGENT]));

  await page.goto("/?api=1");
  await page.getByRole("navigation").getByText("Agents", { exact: true }).click();
  await page.getByTestId("agent-card-name").click();

  await expect(page.getByTestId("agent-detail-nobundle")).toBeVisible();
  await expect(page.getByRole("button", { name: "Deploy new version" })).toHaveCount(0);
});
