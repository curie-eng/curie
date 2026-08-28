// These assert against the real shapes from `examples/weather`, which is a
// bundle the repo itself ships and its CI validates. Inventing a fixture would
// only prove this module agrees with my guess about the format.

import { describe, expect, it } from "vitest";

import { deployedAs } from "./deployment";

import type { Workspace } from "../../electron/shared/contract";
import {
  skillBody,
  withSkillBody,
  withPluginField,

  classifyFile,
  organise,
  parseEvalSuite,
  parsePlugin,
  parseSkill,
  readiness,
  validateForSave,
  verdict,
} from "./bundle";

const WEATHER_PLUGIN = JSON.stringify({
  name: "weather",
  description: "The weather agent plugin.",
  version: "0.1.0",
  starterPrompts: ["What can this weather agent help me with?"],
});

const WEATHER_SKILL = `---
name: weather
description: Look up a location's weather forecast using a live web search.
allowed-tools:
  - WebSearch
  - WebFetch
---

# Weather

## When to run
The user asks about the weather.
`;

const WEATHER_CASES = JSON.stringify({
  name: "weather",
  cases: [
    {
      id: "reports-a-temperature",
      input: "What's the weather in San Francisco today?",
      grader: { kind: "regex", expected: "\\d+\\s*(°|deg)", case_sensitive: false },
      note: "trajectory overrides this",
    },
  ],
});

function workspace(over: Partial<Workspace> = {}): Workspace {
  return {
    path: "/Users/dev/agents/weather",
    name: "weather",
    plugin: { name: "weather", version: "0.1.0", description: "The weather agent plugin." },
    skills: ["weather"],
    hasEvals: true,
    hasMcp: true,
    lastOpened: 0,
    ...over,
  };
}

describe("file classification", () => {
  it("recognises every part of a real bundle", () => {
    expect(classifyFile(".claude-plugin/plugin.json").group).toBe("plugin");
    expect(classifyFile("skills/weather/SKILL.md").group).toBe("skill");
    expect(classifyFile(".mcp.json").group).toBe("integration");
    expect(classifyFile("connectors.yaml").group).toBe("integration");
    expect(classifyFile("evals/cases.json").group).toBe("eval");
    expect(classifyFile("evals/trajectory.json").group).toBe("eval");
    expect(classifyFile("AGENTS.md").group).toBe("doc");
    expect(classifyFile("deploy.yaml").group).toBe("deploy");
  });

  it("reads Curie's plugin.json authoring extensions", () => {
    // systemPrompt, starterPrompts, secrets, triggers and approvalPolicy are
    // Curie additions that Claude Code warns-and-ignores. They are authoring
    // surface, so the Build view shows them.
    const r = parsePlugin(
      JSON.stringify({
        name: "f",
        secrets: ["FIXTURE_TOKEN"],
        triggers: [{ type: "cron", schedule: "0 9 * * 1" }, { type: "webhook", path: "h" }],
        approvalPolicy: { gates: [{ gate: "Bash", route: "r" }] },
        systemPrompt: "you are a fixture",
      }),
    );
    expect(r.ok).toBe(true);
    if (!r.ok) return;
    expect(r.value.secrets).toEqual(["FIXTURE_TOKEN"]);
    expect(r.value.triggerCount).toBe(2);
    expect(r.value.approvalGates).toEqual(["Bash"]);
    expect(r.value.systemPrompt).toBeTruthy();
  });

  it("labels a skill by its directory, which is its identity", () => {
    expect(classifyFile("skills/discount-policy/SKILL.md").label).toBe("discount-policy");
  });

  it("marks contract files as structured so the editor can guard them", () => {
    expect(classifyFile(".claude-plugin/plugin.json").structured).toBe(true);
    expect(classifyFile("evals/cases.json").structured).toBe(true);
    expect(classifyFile("skills/weather/SKILL.md").structured).toBe(false);
    expect(classifyFile("AGENTS.md").structured).toBe(false);
  });

  it("orders groups the way a bundle is read, and drops incidental files", () => {
    const groups = organise([
      "AGENTS.md",
      "evals/cases.json",
      ".mcp.json",
      "skills/weather/SKILL.md",
      ".claude-plugin/plugin.json",
      ".gitignore",
      "notes.txt",
    ]);
    expect(groups.map((g) => g.group)).toEqual(["plugin", "skill", "integration", "eval", "doc"]);
    const all = groups.flatMap((g) => g.files.map((f) => f.path));
    expect(all).not.toContain("notes.txt");
  });
});

describe("plugin manifest", () => {
  it("reads the real weather manifest", () => {
    const r = parsePlugin(WEATHER_PLUGIN);
    expect(r.ok).toBe(true);
    if (!r.ok) return;
    expect(r.value.name).toBe("weather");
    expect(r.value.version).toBe("0.1.0");
    expect(r.value.starterPrompts).toHaveLength(1);
  });

  it("reports a parse failure rather than returning an empty manifest", () => {
    // A silent default here would let a broken bundle read as merely sparse.
    const r = parsePlugin("{ not json");
    expect(r.ok).toBe(false);
  });

  it("refuses a JSON array, which parses but is not a manifest", () => {
    expect(parsePlugin("[]").ok).toBe(false);
  });

  it("ignores fields of the wrong type instead of trusting them", () => {
    const r = parsePlugin(JSON.stringify({ name: 42, starterPrompts: "nope" }));
    expect(r.ok).toBe(true);
    if (!r.ok) return;
    expect(r.value.name).toBeUndefined();
    expect(r.value.starterPrompts).toBeUndefined();
  });
});

describe("eval suite", () => {
  it("reads the real weather suite", () => {
    const r = parseEvalSuite(WEATHER_CASES);
    expect(r.ok).toBe(true);
    if (!r.ok) return;
    expect(r.value.name).toBe("weather");
    expect(r.value.cases[0].grader).toEqual({
      kind: "regex",
      expected: "\\d+\\s*(°|deg)",
      case_sensitive: false,
    });
  });

  it("tolerates additive fields, which the frozen schema explicitly allows", () => {
    const r = parseEvalSuite(
      JSON.stringify({
        name: "s",
        cases: [
          {
            id: "a",
            input: "i",
            grader: { kind: "contains", expected: "x" },
            shared_history: true,
            expect_status: "awaiting-approval",
            somethingNew: 1,
          },
        ],
      }),
    );
    expect(r.ok).toBe(true);
    if (!r.ok) return;
    expect(r.value.cases[0].shared_history).toBe(true);
    expect(r.value.cases[0].expect_status).toBe("awaiting-approval");
  });

  it("names the offending case when a required field is missing", () => {
    const r = parseEvalSuite(
      JSON.stringify({ name: "s", cases: [{ id: "has-id", grader: { kind: "exact", expected: "x" } }] }),
    );
    expect(r.ok).toBe(false);
    if (r.ok) return;
    expect(r.error).toContain("has-id");
    expect(r.error).toContain("input");
  });

  it("rejects a grader kind outside the frozen enum", () => {
    const r = parseEvalSuite(
      JSON.stringify({ name: "s", cases: [{ id: "a", input: "i", grader: { kind: "vibes", expected: "x" } }] }),
    );
    expect(r.ok).toBe(false);
    if (r.ok) return;
    expect(r.error).toContain("tool_called");
  });

  it("accepts every grader kind the schema allows", () => {
    for (const kind of ["exact", "contains", "regex", "tool_called"]) {
      const r = parseEvalSuite(
        JSON.stringify({ name: "s", cases: [{ id: "a", input: "i", grader: { kind, expected: "x" } }] }),
      );
      expect(r.ok, kind).toBe(true);
    }
  });

  it("rejects an empty suite, which the schema forbids", () => {
    expect(parseEvalSuite(JSON.stringify({ name: "s", cases: [] })).ok).toBe(false);
  });

  it("rejects duplicate case ids, which make a result table ambiguous", () => {
    const r = parseEvalSuite(
      JSON.stringify({
        name: "s",
        cases: [
          { id: "same", input: "a", grader: { kind: "exact", expected: "x" } },
          { id: "same", input: "b", grader: { kind: "exact", expected: "y" } },
        ],
      }),
    );
    expect(r.ok).toBe(false);
    if (r.ok) return;
    expect(r.error).toContain("duplicate");
  });
});

describe("skill frontmatter", () => {
  it("reads the real weather skill", () => {
    const m = parseSkill(WEATHER_SKILL);
    expect(m.name).toBe("weather");
    expect(m.description).toContain("weather forecast");
    expect(m.allowedTools).toEqual(["WebSearch", "WebFetch"]);
  });

  it("reads the inline list form too", () => {
    const m = parseSkill(`---\nname: x\nallowed-tools: [Bash, Read]\n---\nbody`);
    expect(m.allowedTools).toEqual(["Bash", "Read"]);
  });

  it("reports absence rather than guessing when there is no frontmatter", () => {
    const m = parseSkill("# Just a heading\n\nSome prose.");
    expect(m.name).toBeUndefined();
    expect(m.description).toBeUndefined();
    expect(m.allowedTools).toEqual([]);
  });

  it("does not read keys from the body as frontmatter", () => {
    const m = parseSkill(`---\nname: real\n---\n\nname: not-frontmatter\n`);
    expect(m.name).toBe("real");
  });
});

describe("readiness", () => {
  it("passes a complete bundle", () => {
    const checks = readiness(workspace(), {
      plugin: parsePlugin(WEATHER_PLUGIN),
      evals: parseEvalSuite(WEATHER_CASES),
      skills: [parseSkill(WEATHER_SKILL)],
    });
    expect(checks.filter((c) => c.level === "error")).toEqual([]);
    expect(verdict(checks).level).toBe("ok");
  });

  it("treats a missing manifest as blocking, and offers the command that fixes it", () => {
    const checks = readiness(workspace({ plugin: undefined }));
    const blocker = checks.find((c) => c.id === "plugin-missing");
    expect(blocker?.level).toBe("error");
    expect(blocker?.fix).toBe("init");
    expect(verdict(checks).level).toBe("error");
  });

  it("treats a bundle with no skills the way the platform does: a warning", () => {
    // plugin_format's validator emits `skills.empty` as a warn, and the repo
    // ships examples/compat-fixture with no skills at all. Being stricter than
    // the platform would call a shipped bundle invalid.
    const checks = readiness(workspace({ skills: [] }));
    const c = checks.find((x) => x.id === "no-skills");
    expect(c?.level).toBe("warn");
    expect(verdict(checks).level).not.toBe("error");
  });

  it("treats missing evals as a warning, not a blocker, and points at eval-init", () => {
    // A bundle without evals deploys. It just is not falsifiable, which is worth
    // saying without pretending it is broken.
    const checks = readiness(workspace({ hasEvals: false }));
    const c = checks.find((x) => x.id === "no-evals");
    expect(c?.level).toBe("warn");
    expect(c?.fix).toBe("skill.eval-init");
    expect(verdict(checks).level).toBe("warn");
  });

  it("treats a broken eval file as blocking, because the driver would reject it", () => {
    const checks = readiness(workspace(), { evals: parseEvalSuite("{oops") });
    expect(checks.some((c) => c.id === "evals-invalid" && c.level === "error")).toBe(true);
  });

  it("flags a skill with no description, which is close to unreachable", () => {
    const checks = readiness(workspace(), { skills: [parseSkill("# no frontmatter")] });
    expect(checks.some((c) => c.id.startsWith("skill-description") && c.level === "warn")).toBe(true);
  });

  it("treats a missing .mcp.json as an option not taken, not a problem", () => {
    const checks = readiness(workspace({ hasMcp: false }));
    expect(checks.find((c) => c.id === "no-mcp")?.level).toBe("info");
    expect(verdict(checks).level).toBe("ok");
  });

  it("sorts blockers above warnings above notes", () => {
    const checks = readiness(workspace({ plugin: undefined, skills: [], hasEvals: false, hasMcp: false }));
    const levels = checks.map((c) => c.level);
    expect(levels).toEqual([...levels].sort((a, b) => ({ error: 0, warn: 1, info: 2 })[a] - ({ error: 0, warn: 1, info: 2 })[b]));
  });
});

describe("save validation", () => {
  it("refuses to write a plugin.json that does not parse", () => {
    expect(validateForSave(".claude-plugin/plugin.json", "{ oops")).toBeTruthy();
  });

  it("refuses to write eval cases the driver would reject", () => {
    expect(validateForSave("evals/cases.json", JSON.stringify({ name: "s", cases: [] }))).toBeTruthy();
  });

  it("allows a valid contract file", () => {
    expect(validateForSave(".claude-plugin/plugin.json", WEATHER_PLUGIN)).toBeNull();
    expect(validateForSave("evals/cases.json", WEATHER_CASES)).toBeNull();
  });

  it("does not stand in the way of prose", () => {
    // A half-written SKILL.md is a normal state to save in.
    expect(validateForSave("skills/weather/SKILL.md", "--- broken yaml? maybe")).toBeNull();
  });

  it("leaves YAML to the CLI rather than guessing", () => {
    expect(validateForSave("connectors.yaml", ": : :")).toBeNull();
  });
});

describe("an eval case's input may be empty", () => {
  // The frozen schema types `input` as a plain string with no `minLength`, and
  // the eval driver has no emptiness check. Refusing `""` here made this app
  // stricter than the platform it is a client of -- and it refused a real
  // bundle: `examples/squawk` is a stack whose entire contract is that a
  // non-empty message pushes and an EMPTY message pops, so the empty case is
  // the only one that exercises half the behaviour.
  const suite = (input: string) =>
    JSON.stringify({
      name: "s",
      cases: [{ id: "pop", input, grader: { kind: "regex", expected: "^Squawk" } }],
    });

  it("accepts an empty input", () => {
    const parsed = parseEvalSuite(suite(""));
    expect(parsed.ok, parsed.ok ? "" : parsed.error).toBe(true);
  });

  it("still requires the field to be present and a string", () => {
    const missing = JSON.stringify({
      name: "s",
      cases: [{ id: "pop", grader: { kind: "regex", expected: "x" } }],
    });
    const parsed = parseEvalSuite(missing);
    expect(parsed.ok).toBe(false);
    if (!parsed.ok) expect(parsed.error).toMatch(/input/);
  });
});

describe("editing a bundle through fields instead of a text editor", () => {
  const SKILL = `---
name: shift-notes
description: Keeps a list.
allowed-tools:
  - mcp__curie-state__get
---

# shift-notes

Say something to save it.
`;

  it("separates the prose from the frontmatter", () => {
    expect(skillBody(SKILL)).toBe("# shift-notes\n\nSay something to save it.\n");
  });

  it("rewrites the prose and leaves the frontmatter exactly as it was", () => {
    // The frontmatter carries `allowed-tools`, which decides what the agent may
    // call. A body edit that reformatted or dropped it would change what the
    // agent can DO, from a field that says it edits what the agent should do.
    const out = withSkillBody(SKILL, "# shift-notes\n\nNew instructions.");
    expect(out).toContain("allowed-tools:\n  - mcp__curie-state__get");
    expect(out).toContain("New instructions.");
    expect(out).not.toContain("Say something to save it.");
    expect(parseSkill(out).allowedTools).toEqual(["mcp__curie-state__get"]);
  });

  it("does not invent a frontmatter block for a file that has none", () => {
    // Writing one would put a `name` in the file the author never chose, and the
    // readiness check already reports the absence.
    const out = withSkillBody("just prose", "still prose");
    expect(out).toBe("still prose\n");
  });

  it("sets a plugin.json field", () => {
    const out = withPluginField('{"name":"x","version":"0.1.0"}', "description", " A thing ");
    expect(out.ok).toBe(true);
    if (out.ok) expect(JSON.parse(out.value)).toEqual({
      name: "x",
      version: "0.1.0",
      description: "A thing",
    });
  });

  it("REMOVES a field rather than writing it empty", () => {
    // These fields are absent-or-present. An empty `description` is a
    // description, and the platform will faithfully show it as blank.
    const out = withPluginField('{"name":"x","description":"old"}', "description", "  ");
    expect(out.ok).toBe(true);
    if (out.ok) expect(JSON.parse(out.value)).toEqual({ name: "x" });
  });

  it("keeps fields it does not model", () => {
    const out = withPluginField('{"name":"x","triggers":[{"type":"cron"}]}', "description", "hi");
    expect(out.ok).toBe(true);
    if (out.ok) expect(JSON.parse(out.value).triggers).toEqual([{ type: "cron" }]);
  });

  it("refuses to write over a file it cannot parse", () => {
    // Overwriting a broken file with what the panel could salvage is how an
    // author loses the half of it the panel does not model.
    const out = withPluginField("{ not json", "description", "hi");
    expect(out.ok).toBe(false);
  });
});

describe("matching a bundle to a running agent", () => {
  const agents = [{ name: "squawk" }, { name: "weather" }];

  it("finds the one deployed under this bundle's name", () => {
    expect(deployedAs(agents, "squawk")).toEqual({ name: "squawk" });
  });

  it("finds nothing when the platform is running nothing by that name", () => {
    expect(deployedAs(agents, "shift-notes")).toBeUndefined();
    expect(deployedAs([], "squawk")).toBeUndefined();
  });

  it("does not match a name that merely contains it", () => {
    // `deploy.yaml` sends a bundle out as `squawk-dev` in one environment and
    // `squawk` in another. Those are DIFFERENT agents with separate identity,
    // memory and approval routing, and treating one as the other would report a
    // dev deployment as production being live.
    expect(deployedAs([{ name: "squawk-dev" }], "squawk")).toBeUndefined();
  });
});
