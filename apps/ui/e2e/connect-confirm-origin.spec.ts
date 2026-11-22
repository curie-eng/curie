import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test } from "@playwright/test";

// #5836. Fetch sets Origin to the value null for a non-GET request when the
// document's referrer policy is no-referrer. The connect page is that document.
// strict-origin keeps the real Origin on a same-scheme POST and leaves the
// token path out of Referer. https://fetch.spec.whatwg.org/#append-a-request-origin-header

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const TOKEN = "example-connect-token";

function headerValue(value: string | string[] | undefined): string {
  if (Array.isArray(value)) return value.join(",");
  return value ?? "";
}

function locationBlock(source: string, marker: string): string {
  const start = source.indexOf(marker);
  if (start < 0) throw new Error(`missing nginx location ${marker}`);
  const rest = source.slice(start);
  const end = rest.indexOf("\n    location ", marker.length);
  return end < 0 ? rest : rest.slice(0, end);
}

function policyIn(block: string): string {
  const found = /add_header Referrer-Policy "([^"]+)"/.exec(block);
  if (!found) throw new Error("nginx location has no Referrer-Policy");
  return found[1];
}

function connectPolicies(): { api: string; nginx: string; github: string; callback: string } {
  const py = readFileSync(
    resolve(repoRoot, "apps/api/src/curie_api/routers/identity_connect.py"),
    "utf8",
  );
  const api = /"Referrer-Policy": "([^"]+)"/.exec(py)?.[1];
  if (!api) throw new Error("connect API Referrer-Policy missing");
  const docker = readFileSync(resolve(repoRoot, "apps/ui/Dockerfile"), "utf8");
  return {
    api,
    nginx: policyIn(locationBlock(docker, "location ^~ /api/identity/connect/")),
    github: policyIn(locationBlock(docker, "location ^~ /api/identity/me/github-link")),
    callback: policyIn(locationBlock(docker, "location = /api/console/oidc/callback")),
  };
}

function listen(
  onRequest: (req: IncomingMessage, res: ServerResponse) => void,
): Promise<{ origin: string; close: () => Promise<void> }> {
  const server = createServer(onRequest);
  return new Promise((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        reject(new Error("test server did not bind a port"));
        return;
      }
      resolveListen({
        origin: `http://127.0.0.1:${address.port}`,
        close: () =>
          new Promise((done, fail) => {
            server.close((error) => (error ? fail(error) : done()));
          }),
      });
    });
  });
}

test("connect confirm in Chromium sends the page origin and records the link", async ({ page }) => {
  const policies = connectPolicies();
  expect(policies.api).toBe(policies.nginx);
  // GitHub link and the OIDC callback are JSON and redirects, not form documents.
  expect(policies.github).toBe("no-referrer");
  expect(policies.callback).toBe("no-referrer");
  const policy = policies.nginx;

  const referers: string[] = [];
  const probe = await listen((req, res) => {
    referers.push(headerValue(req.headers.referer));
    res.writeHead(204);
    res.end();
  });
  const links: { token: string; origin: string }[] = [];
  let confirmReferer = "";
  const connect = await listen((req, res) => {
    const host = headerValue(req.headers.host);
    if (req.method === "GET" && req.url === `/connect/${TOKEN}`) {
      res.writeHead(200, {
        "content-type": "text/html; charset=utf-8",
        "cache-control": "private, no-store",
        "referrer-policy": policy,
      });
      res.end(
        "<!doctype html><html lang=\"en\"><meta charset=\"utf-8\"><title>Connect account</title>" +
          "<h1>Connect account</h1>" +
          `<form method="post" action="/connect/${TOKEN}/confirm">` +
          "<button type=\"submit\">Confirm account connection</button></form>" +
          `<img src="${probe.origin}/pixel" alt=""></html>`,
      );
      return;
    }
    if (req.method === "POST" && req.url === `/connect/${TOKEN}/confirm`) {
      const origin = headerValue(req.headers.origin);
      confirmReferer = headerValue(req.headers.referer);
      let accepted: boolean;
      try {
        accepted = origin !== "" && origin !== "null" && new URL(origin).host === host;
      } catch {
        accepted = false;
      }
      if (!accepted) {
        res.writeHead(403, { "content-type": "application/json" });
        res.end(JSON.stringify({ detail: "console session origin rejected" }));
        return;
      }
      links.push({ token: TOKEN, origin });
      res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
      res.end(
        "<!doctype html><html lang=\"en\"><title>Account connected</title>" +
          "<h1>Account connected</h1><p>Return to your chat and send your request again.</p></html>",
      );
      return;
    }
    res.writeHead(404);
    res.end();
  });
  try {
    const response = await page.goto(`${connect.origin}/connect/${TOKEN}`);
    expect(response?.headers()["referrer-policy"]).toBe(policy);
    await expect.poll(() => referers.length).toBeGreaterThan(0);
    await page.getByRole("button", { name: "Confirm account connection" }).click();
    await expect(page.getByRole("heading", { name: "Account connected" })).toBeVisible();
    expect(links).toEqual([{ token: TOKEN, origin: connect.origin }]);
    expect(confirmReferer).not.toContain(TOKEN);
    expect(referers.join("\n")).not.toContain(TOKEN);
  } finally {
    await connect.close();
    await probe.close();
  }
});

test("the console plan page posts with its own origin", async ({ page }) => {
  const seen: { path: string; origin: string; policy: string }[] = [];
  const plan = await listen((req, res) => {
    if (req.method === "GET" && req.url === "/settings/claude-plan") {
      res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
      res.end(
        "<!doctype html><html lang=\"en\"><title>Claude plan</title>" +
          "<button type=\"button\" id=\"save\">Save plan</button>" +
          "<script>document.getElementById('save').onclick = () => " +
          "fetch('/api/personal-claude/enrollment', {method: 'POST', body: '{}'});</script></html>",
      );
      return;
    }
    if (req.method === "POST" && req.url === "/api/identity/me/github-link") {
      seen.push({
        path: req.url,
        origin: headerValue(req.headers.origin),
        policy: "no-referrer",
      });
      res.writeHead(200, {
        "content-type": "application/json",
        "referrer-policy": "no-referrer",
      });
      res.end("{\"authorize_url\":\"https://github.example/login/oauth/authorize\"}");
      return;
    }
    if (req.method === "POST" && req.url === "/api/personal-claude/enrollment") {
      seen.push({
        path: req.url,
        origin: headerValue(req.headers.origin),
        policy: "",
      });
      res.writeHead(200, { "content-type": "application/json" });
      res.end("{}");
      return;
    }
    res.writeHead(404);
    res.end();
  });
  try {
    await page.goto(`${plan.origin}/settings/claude-plan`);
    await page.evaluate(() => fetch("/api/identity/me/github-link", { method: "POST" }));
    await page.getByRole("button", { name: "Save plan" }).click();
    await expect.poll(() => seen.map((item) => item.path)).toEqual([
      "/api/identity/me/github-link",
      "/api/personal-claude/enrollment",
    ]);
    expect(seen.every((item) => item.origin === plan.origin)).toBe(true);
  } finally {
    await plan.close();
  }
});
