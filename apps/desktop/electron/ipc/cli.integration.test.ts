// Integration: drive the real `curie` binary through the shell's own resolution
// and spawn path.
//
// Everything else in this suite proves the app builds the argv it intends to.
// This proves the binary agrees -- the gap unit tests cannot close, because they
// compare the app against its own copy of the manifest rather than against a
// real CLI.
//
// One thing this suite deliberately does NOT assert is that the installed binary
// matches the checkout. It routinely will not: the app's command surface is
// generated from `cli/command-manifest.json` in this repo, while `curie` on PATH
// is whatever the developer last installed. That mismatch is a real condition
// with real consequences, so it is *detected and reported* -- by
// `compareToLive`, which the app calls at startup and renders in Settings -- and
// asserted here only in the direction that is unambiguously a bug.
//
// Skipped entirely when `curie` is not installed, so a fresh checkout still has
// a green suite.

import { describe, expect, it } from "vitest";

import { findCli, runOnce, resetCliCache, searchPath } from "./cli";
import { compareToLive, leafActions, manifest, resolve, resolveNode } from "./manifest";

resetCliCache();
const cli = findCli();
const withCli = cli ? describe : describe.skip;

/** The live binary's schema, fetched once for the whole file. */
async function liveSchema(): Promise<unknown> {
  const { stdout, code } = await runOnce(["schema"], { timeoutMs: 20_000 });
  if (code !== 0) throw new Error("curie schema failed");
  return JSON.parse(stdout);
}

withCli("the resolved binary", () => {
  it("is a real executable that reports a version", async () => {
    const { stdout, code } = await runOnce(["--version"]);
    expect(code).toBe(0);
    expect(stdout).toMatch(/^curie \d/);
  });

  it("accepts the argv this app builds for a read-only command", async () => {
    // `schema` prints the manifest and touches nothing, so it is safe to run
    // anywhere -- and its output is the one thing we can check hard.
    const { argv } = resolve({ action: "schema" }, process.cwd());
    const { stdout, code } = await runOnce([...argv]);
    expect(code).toBe(0);
    const parsed = JSON.parse(stdout) as { name?: string; subcommands?: unknown[] };
    expect(parsed.name).toBe("curie");
    expect(Array.isArray(parsed.subcommands)).toBe(true);
  });

  it("surfaces a non-zero exit rather than throwing", async () => {
    const { code } = await runOnce(["definitely-not-a-command"]);
    expect(code).not.toBe(0);
  });

  it("rejects a flag the manifest does not declare, before spawning anything", () => {
    // The guard is in `resolve`, not in the CLI: a typo never reaches a process.
    expect(() =>
      resolve({ action: "local.status", flags: { "made-up": "x" } }, process.cwd()),
    ).toThrow();
  });
});

withCli("every command path the app can reach is one the binary accepts", () => {
  it("resolves and is understood, for each command both sides have", async () => {
    // `--help` is the only argv that is safe to run for all 80 commands: it
    // parses the full subcommand path, then exits without doing anything. If a
    // path this app can produce were malformed, clap would reject it here.
    const drift = compareToLive(await liveSchema(), null);
    expect(drift).not.toBeNull();
    const unavailable = new Set(drift!.missingFromCli);

    const shared = leafActions(manifest).filter((id) => !unavailable.has(id));
    expect(shared.length).toBeGreaterThan(50);

    const failures: string[] = [];
    // Batched rather than all at once: 80 concurrent spawns is enough to make a
    // laptop swap, and this is a test, not a benchmark.
    for (let i = 0; i < shared.length; i += 8) {
      const batch = shared.slice(i, i + 8);
      const results = await Promise.all(
        batch.map(async (id) => {
          // `resolve` refuses a missing required positional -- correctly -- so
          // the probe supplies a placeholder for each. `--help` short-circuits
          // before the value is used for anything.
          const { node } = resolveNode(id);
          const positionals = (node.args ?? [])
            .filter((a) => a.positional)
            .map((a) => `probe-${a.id}`);
          const { argv } = resolve({ action: id, positionals }, process.cwd());
          const { code } = await runOnce([...argv, "--help"], { timeoutMs: 20_000 });
          return { id, code };
        }),
      );
      for (const r of results) if (r.code !== 0) failures.push(r.id);
    }

    expect(failures).toEqual([]);
  }, 180_000);
});

withCli("drift detection", () => {
  it("produces an accurate, renderable report", async () => {
    const drift = compareToLive(await liveSchema(), "curie 0.0.0-test");
    expect(drift).not.toBeNull();
    expect(drift!.cliVersion).toBe("curie 0.0.0-test");
    expect(Array.isArray(drift!.missingFromApp)).toBe(true);
    expect(Array.isArray(drift!.missingFromCli)).toBe(true);
    // The two sets are disjoint by construction; a command in both would mean
    // the comparison is nonsense.
    for (const id of drift!.missingFromApp) {
      expect(drift!.missingFromCli).not.toContain(id);
    }

    // Diagnostics, not assertions: this is the state of *this machine*, and the
    // app reports the same thing at startup.
    if (drift!.missingFromCli.length) {
      console.warn(
        `[parity] this checkout offers ${drift!.missingFromCli.length} command(s) the installed ` +
          `curie does not have: ${drift!.missingFromCli.join(", ")}. ` +
          `The app disables nothing automatically -- it reports this in Settings.`,
      );
    }
    if (drift!.missingFromApp.length) {
      console.warn(
        `[parity] the installed curie has ${drift!.missingFromApp.length} command(s) this ` +
          `checkout does not: ${drift!.missingFromApp.join(", ")}. ` +
          `Run \`pnpm gen:manifest\` against the CLI you are driving.`,
      );
    }
  });

  it("reports no drift rather than a wrong one when the schema cannot be read", () => {
    expect(compareToLive(null, null)).toBeNull();
    expect(compareToLive({ nope: true }, null)).toBeNull();
    expect(compareToLive("not json at all", null)).toBeNull();
  });

  it("finds a command the app offers and a fake CLI lacks", () => {
    // A hand-built "CLI" missing the whole local group must show up as broken
    // buttons, not as silence.
    const stunted = { name: "curie", subcommands: [{ name: "doctor" }] };
    const drift = compareToLive(stunted, null)!;
    expect(drift.missingFromCli).toContain("local.up");
    expect(drift.missingFromCli).not.toContain("doctor");
  });

  it("finds a command a newer CLI has and the app does not", () => {
    const richer = {
      name: "curie",
      subcommands: [...(manifest.subcommands ?? []), { name: "brand-new-command" }],
    };
    const drift = compareToLive(richer, null)!;
    expect(drift.missingFromApp).toContain("brand-new-command");
    expect(drift.missingFromCli).toEqual([]);
  });
});

describe("cli discovery", () => {
  it("looks beyond the GUI PATH", () => {
    // A double-clicked app does not inherit the login shell's PATH, which is the
    // only reason the search list exists.
    const dirs = searchPath().split(":");
    expect(dirs.some((d) => d.endsWith("/.cargo/bin"))).toBe(true);
    expect(dirs).toContain("/usr/local/bin");
  });
});
