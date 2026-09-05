// Run the bundle parsers over the real `examples/` bundles the repo ships and
// its own `check-plugin-compat.sh` validates.
//
// This lives on the electron side rather than next to the module it tests
// because it reads the filesystem, and the renderer's tsconfig correctly has no
// node types. It is the strongest available check on the parsers: if the bundle
// format moves, this fails here rather than in someone's editor.

import { existsSync, readFileSync, readdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { describe, expect, it } from "vitest";

import type { Workspace } from "./shared/contract";
import { parseEvalSuite, parsePlugin, parseSkill, readiness } from "../src/lib/bundle";

const HERE = dirname(new URL(import.meta.url).pathname);
const EXAMPLES = resolve(HERE, "..", "..", "..", "examples");

const bundles = existsSync(EXAMPLES)
  ? readdirSync(EXAMPLES, { withFileTypes: true })
      .filter(
        (e) => e.isDirectory() && existsSync(join(EXAMPLES, e.name, ".claude-plugin/plugin.json")),
      )
      .map((e) => e.name)
  : [];

const describeExamples = bundles.length ? describe : describe.skip;

describeExamples("the repo's own example bundles", () => {
  it("finds some to check", () => {
    expect(bundles.length).toBeGreaterThan(0);
  });

  it.each(bundles)("%s: plugin.json parses and names itself", (name: string) => {
    const r = parsePlugin(readFileSync(join(EXAMPLES, name, ".claude-plugin/plugin.json"), "utf8"));
    expect(r.ok).toBe(true);
    if (!r.ok) return;
    expect(r.value.name, "a bundle must name itself").toBeTruthy();
  });

  it.each(bundles)("%s: any eval suite parses against the frozen schema", (name: string) => {
    const cases = join(EXAMPLES, name, "evals/cases.json");
    if (!existsSync(cases)) return;
    const r = parseEvalSuite(readFileSync(cases, "utf8"));
    if (!r.ok) throw new Error(`${name}: ${r.error}`);
    expect(r.value.cases.length).toBeGreaterThan(0);
  });

  it.each(bundles)("%s: every skill declares a description", (name: string) => {
    const skillsDir = join(EXAMPLES, name, "skills");
    if (!existsSync(skillsDir)) return;
    for (const entry of readdirSync(skillsDir, { withFileTypes: true })) {
      const md = join(skillsDir, entry.name, "SKILL.md");
      if (!entry.isDirectory() || !existsSync(md)) continue;
      // The description is how the model decides to invoke a skill, so a
      // shipped example without one would be a bad example.
      expect(parseSkill(readFileSync(md, "utf8")).description, `${name}/${entry.name}`).toBeTruthy();
    }
  });

  it("reports no blocking problem in any shipped bundle", () => {
    // The severity levels in `readiness` must not be stricter than the platform
    // itself: calling a bundle the repo ships invalid would be a false alarm.
    for (const name of bundles) {
      const root = join(EXAMPLES, name);
      const skillsDir = join(root, "skills");
      const skillNames = existsSync(skillsDir)
        ? readdirSync(skillsDir, { withFileTypes: true })
            .filter((e) => e.isDirectory() && existsSync(join(skillsDir, e.name, "SKILL.md")))
            .map((e) => e.name)
        : [];
      const plugin = parsePlugin(
        readFileSync(join(root, ".claude-plugin/plugin.json"), "utf8"),
      );
      const evalsPath = join(root, "evals/cases.json");
      const ws: Workspace = {
        path: root,
        name,
        plugin: plugin.ok ? plugin.value : undefined,
        skills: skillNames,
        hasEvals: existsSync(evalsPath),
        hasMcp: existsSync(join(root, ".mcp.json")),
        lastOpened: 0,
      };
      const checks = readiness(ws, {
        plugin,
        evals: existsSync(evalsPath)
          ? parseEvalSuite(readFileSync(evalsPath, "utf8"))
          : undefined,
        skills: skillNames.map((s) =>
          parseSkill(readFileSync(join(skillsDir, s, "SKILL.md"), "utf8")),
        ),
      });
      expect(
        checks.filter((c) => c.level === "error").map((c) => `${name}: ${c.title}`),
      ).toEqual([]);
    }
  });
});
