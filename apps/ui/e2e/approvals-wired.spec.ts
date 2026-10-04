import { test, expect, type Page } from "@playwright/test";
import { SESSION_SUBJECT, stubConsoleSession } from "./support/consoleSession";

// Wired Approvals (#867) in the stackless suite: the app is served with the
// approvals API stubbed with real-shaped responses via route interception, so
// these run headless with no backend. Consumes GET /approvals,
// GET /approvals/{id}/audit, and POST /approvals/{id}/resolve.

function approval(overrides: Record<string, unknown> = {}) {
  return {
    id: "ap-1",
    agent_id: "ag-1",
    conversation_id: "C-thread-1",
    author: "U-alice",
    summary: "Refund $4,200 to ACME Corp",
    reply_channel: "C0DEALS",
    reply_placeholder: "ts-1",
    reply_endpoint: null,
    dedupe_key: "dk-1",
    route: "managers",
    card_channel: "C0MANAGERS",
    gate_kind: "permission",
    granted_tool: "issue_refund",
    status: "pending",
    expires_at: "2026-07-24T00:00:00+00:00",
    resolved_by: null,
    resolution_note: null,
    created_at: "2026-07-23T00:00:00+00:00",
    resolved_at: null,
    ...overrides,
  };
}

// Stub the approvals list behind an authenticated console session (the global
// login gate owns sign-in since #1047; console-login.spec.ts covers it). The
// audit endpoint (more specific path) is stubbed first so the list matcher does
// not swallow it.
async function stubApprovals(page: Page, rows: object[]) {
  await stubConsoleSession(page);
  await page.route("**/api/approvals/*/audit*", (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify([]) }),
  );
  await page.route(
    (url) => url.pathname.endsWith("/api/approvals"),
    (route) => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(rows) }),
  );
}

async function openApprovalsTab(page: Page) {
  await page.goto("/?api=1");
  await page.getByRole("navigation").getByText("Observability", { exact: true }).click();
  await page.getByRole("button", { name: "Approvals" }).click();
}

test("lists pending approvals and opens the detail with its audit trail", async ({ page }) => {
  await stubApprovals(page, [approval()]);
  await openApprovalsTab(page);

  await expect(page.getByTestId("approval-summary")).toContainText("Refund $4,200 to ACME Corp");
  await page.getByTestId("approval-summary").click();

  const detail = page.getByTestId("approval-detail");
  await expect(detail).toBeVisible();
  await expect(detail).toContainText("managers");
  await expect(detail).toContainText("issue refund");
  await expect(detail).not.toContainText("issue_refund");
});

test("shows the immutable session principal from the global console session", async ({ page }) => {
  await stubApprovals(page, []);
  await openApprovalsTab(page);

  await expect(page.getByTestId("approval-principal")).toContainText(SESSION_SUBJECT);
  // The view no longer owns a sign-in path or an asserted resolver identity.
  await expect(page.getByLabel("login code")).toHaveCount(0);
  await expect(page.getByLabel("resolved by")).toHaveCount(0);
  await expect(page.getByLabel("actor channel")).toHaveCount(0);
});

test("resolves with exactly decision/note and never the platform key", async ({ page }) => {
  await stubApprovals(page, [approval()]);

  let resolveBody: Record<string, unknown> | null = null;
  let resolveHeaders: Record<string, string> | null = null;
  await page.route("**/api/approvals/*/resolve", (route) => {
    resolveBody = JSON.parse(route.request().postData() ?? "{}");
    resolveHeaders = route.request().headers();
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(approval({ status: "approved", resolved_by: SESSION_SUBJECT })),
    });
  });

  await openApprovalsTab(page);
  await page.getByTestId("approval-summary").click();
  await page.getByLabel("note").fill("Confirmed in console");
  await page.getByTestId("approve-btn").click();

  await expect.poll(() => resolveBody).toEqual({ decision: "approved", note: "Confirmed in console" });
  await expect.poll(() => resolveHeaders).not.toBeNull();
  expect(resolveHeaders?.["x-api-key"]).toBeUndefined();
});

test("surfaces a 409 already-resolved conflict from the resolve route", async ({ page }) => {
  await stubApprovals(page, [approval()]);
  await page.route("**/api/approvals/*/resolve", (route) =>
    route.fulfill({
      status: 409,
      contentType: "application/json",
      body: JSON.stringify({ detail: "already resolved by U-bob (approved)" }),
    }),
  );

  await openApprovalsTab(page);
  await page.getByTestId("approval-summary").click();
  await page.getByTestId("reject-btn").click();

  await expect(page.getByTestId("resolve-error")).toContainText("Already resolved: already resolved by U-bob");
});

test("shows the pending empty state for a fresh workspace", async ({ page }) => {
  await stubApprovals(page, []);
  await openApprovalsTab(page);
  await expect(page.getByTestId("approvals")).toContainText("No pending approvals");
});


for (const legacy of [false, true]) {
  test(`approval shows plain action and values with ${legacy ? "legacy" : "current"} API`, async ({ page }) => {
    const summary = 'Tool call awaiting approval: mcp__acme__file_attachment {"file_name":"example.pdf"}';
    await stubApprovals(page, [approval({
      summary,
      granted_tool: "mcp__acme__file_attachment",
      ...(legacy ? {} : { display_summary: "Approve file attachment. File name: example.pdf" }),
    })]);
    await openApprovalsTab(page);
    await expect(page.getByTestId("approval-summary")).toContainText("Approve file attachment. File name: example.pdf");
    await page.getByTestId("approval-summary").click();
    const detail = page.getByTestId("approval-detail");
    await expect(detail).toContainText("example.pdf");
    await expect(detail).not.toContainText("mcp__acme__file_attachment");
  });
}
