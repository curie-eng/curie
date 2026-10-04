import type { Page } from "@playwright/test";

// Shared console-session stub for the stackless suite (#1047). The console sits
// behind a login gate that probes GET /console/session on load, so every spec
// that drives the signed-in console stubs that probe as an authenticated
// session. The shape mirrors ConsoleSessionOut (apps/api/src/curie_api/schemas.py)
// returned by `current_session` in apps/api/src/curie_api/routers/console.py:
// `{ subject, expires_at }`, never a token.
//
// Not a *.spec.ts file, so Playwright does not collect it as a test.

export const SESSION_SUBJECT = "U0AUTHENTICATED";
export const SESSION_EXPIRES_AT = "2026-07-24T12:00:00+00:00";

export async function stubConsoleSession(page: Page, subject: string = SESSION_SUBJECT): Promise<void> {
  await page.route(
    (url) => url.pathname === "/api/console/session",
    (route) => {
      if (route.request().method() === "GET") {
        return route.fulfill({
          status: 200,
          contentType: "application/json",
          headers: { "Cache-Control": "no-store" },
          body: JSON.stringify({ subject, expires_at: SESSION_EXPIRES_AT }),
        });
      }
      // A signed-in console never re-exchanges a code; anything but the GET
      // probe is a test bug, so refuse it loudly in FastAPI's 405 shape.
      return route.fulfill({
        status: 405,
        contentType: "application/json",
        body: JSON.stringify({ detail: "Method Not Allowed" }),
      });
    },
  );
}
