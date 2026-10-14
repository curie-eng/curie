import { test, expect } from "@playwright/test";
import { execFileSync } from "node:child_process";
import { readdir, readFile } from "node:fs/promises";
import { join, relative, resolve } from "node:path";

// Inspect every artifact, including deferred chunks and generated manifests.
async function artifactPaths(directory: string): Promise<string[]> {
  const paths: string[] = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) paths.push(...await artifactPaths(path));
    else paths.push(path);
  }
  return paths.sort();
}

test("production assets contain no platform key or browser key resolution", async () => {
  test.setTimeout(150_000);
  // Rebuild even when PW_BASE_URL or an existing preview bypasses webServer.
  // The scanned artifacts must come from this candidate on every invocation.
  execFileSync("pnpm", ["build"], { cwd: process.cwd(), stdio: "pipe", timeout: 120_000 });
  const dist = resolve(process.cwd(), "dist");
  const paths = await artifactPaths(dist);
  expect(paths.length, "production build contains artifacts").toBeGreaterThan(0);
  expect(paths.some((path) => relative(dist, path) === "index.html"), "production index exists").toBe(true);
  expect(paths.some((path) => path.endsWith(".js")), "production JavaScript exists").toBe(true);

  const platformKey = process.env.CURIE_API_KEY ?? "curie-dev-key";
  expect(platformKey.length, "test harness has a nonempty platform key").toBeGreaterThan(0);
  const keys = new Set([platformKey, "curie-dev-key"]);
  for (const path of paths) {
    const name = relative(dist, path);
    const bytes = await readFile(path);
    expect(bytes.length, `${name} is nonempty`).toBeGreaterThan(0);
    for (const key of keys) {
      // The key value stays out of assertion output, including on failure.
      expect(bytes.includes(Buffer.from(key)), `${name} contains no platform key value`).toBe(false);
    }
    const source = bytes.toString("utf8");
    expect(/[?&]api[_-]?key=/i.test(source), `${name} contains no credential query construction`).toBe(false);
    expect(/VITE_API_KEY/i.test(source), `${name} contains no browser key build variable`).toBe(false);
    expect(
      /(?:["'`]X-API-Key["'`]\s*:|\.(?:set|append)\(\s*["'`]X-API-Key["'`]\s*,|\[\s*["'`]X-API-Key["'`]\s*\]\s*=|\[\s*["'`]X-API-Key["'`]\s*,)/i.test(source),
      `${name} contains no code setting a browser platform-key header`,
    ).toBe(false);
    expect(
      /\.(?:get|getAll|set|append)\(\s*["'`]api[_-]?key["'`]\s*[,)]/i.test(source),
      `${name} contains no credential query lookup or mutation`,
    ).toBe(false);
    expect(
      /\bURLSearchParams\s*\(\s*\{[^}]*?(?:["'`]api[_-]?key["'`]|api_key)\s*:/i.test(source)
        || /\bURLSearchParams\s*\(\s*["'`](?:[^"'`]*?[?&])?api[_-]?key=/i.test(source)
        || /\bURLSearchParams\s*\(\s*\[\s*\[\s*["'`]api[_-]?key["'`]\s*,/i.test(source),
      `${name} contains no URLSearchParams credential construction`,
    ).toBe(false);
  }
});
