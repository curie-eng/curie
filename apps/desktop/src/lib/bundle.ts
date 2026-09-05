// Reading and judging a plugin bundle.
//
// Curie is a platform for *building* and deploying agents, and the build half is
// authoring a bundle: a plugin manifest, one or more skills, optional MCP
// servers, and the eval cases that make a change falsifiable. This module knows
// the shape of those files and what it means for a bundle to be incomplete.
//
// It is deliberately pure. Everything here takes text and returns a verdict, so
// the judgements the Build view renders can be asserted directly instead of by
// clicking through a window. Parsing is also deliberately forgiving in one
// direction only: a file that cannot be read produces a *stated problem*, never
// a silent default, because "your bundle looks fine" is the one answer a broken
// bundle must never get.

import type { Workspace } from "../../electron/shared/contract";

// --- file classification ---------------------------------------------------

export type FileGroup = "plugin" | "skill" | "integration" | "eval" | "deploy" | "doc" | "other";

export interface BundleFile {
  /** Path relative to the bundle root, which is what the bridge takes. */
  readonly path: string;
  readonly group: FileGroup;
  /** What to show in a list: the skill name, or the bare filename. */
  readonly label: string;
  /** True for files the platform reads as contract, so editing them by hand
   *  carries more risk than editing prose. */
  readonly structured: boolean;
}

const GROUP_ORDER: readonly FileGroup[] = [
  "plugin",
  "skill",
  "integration",
  "eval",
  "deploy",
  "doc",
  "other",
];

export const GROUP_LABEL: Record<FileGroup, string> = {
  plugin: "Plugin",
  skill: "Skills",
  integration: "Integrations",
  eval: "Evals",
  deploy: "Deploy",
  doc: "Docs",
  other: "Other",
};

export function classifyFile(rel: string): BundleFile {
  const parts = rel.split("/");
  const name = parts[parts.length - 1];

  if (rel === ".claude-plugin/plugin.json") {
    return { path: rel, group: "plugin", label: "plugin.json", structured: true };
  }
  if (parts[0] === "skills" && name === "SKILL.md") {
    // `skills/<name>/SKILL.md` -- the directory is the skill's identity.
    return { path: rel, group: "skill", label: parts[1] ?? name, structured: false };
  }
  if (parts[0] === "skills") {
    return { path: rel, group: "skill", label: rel.replace(/^skills\//, ""), structured: false };
  }
  if (rel === ".mcp.json" || name === "connectors.yaml" || name === "connectors.yml") {
    return { path: rel, group: "integration", label: name, structured: true };
  }
  if (parts[0] === "evals") {
    return { path: rel, group: "eval", label: name, structured: true };
  }
  // `deploy.yaml` is where this bundle goes: `cluster deploy --target` reads it,
  // so routing is a reviewable diff rather than flags in CI. It is authored, so
  // it belongs in the editor.
  if (/^deploy\.ya?ml$/.test(name) && parts.length === 1) {
    return { path: rel, group: "deploy", label: name, structured: true };
  }
  if (/^(README|AGENTS|CLAUDE)\.md$/i.test(name)) {
    return { path: rel, group: "doc", label: name, structured: false };
  }
  return { path: rel, group: "other", label: rel, structured: /\.(json|ya?ml|toml)$/.test(name) };
}

/** Files worth offering in the editor, grouped and ordered the way a bundle is
 *  actually read: what it is, what it can do, what it talks to, what proves it. */
export function organise(paths: readonly string[]): { group: FileGroup; files: BundleFile[] }[] {
  const all = paths.map(classifyFile).filter((f) => f.group !== "other" || f.structured);
  const byGroup = new Map<FileGroup, BundleFile[]>();
  for (const f of all) {
    const list = byGroup.get(f.group) ?? [];
    list.push(f);
    byGroup.set(f.group, list);
  }
  return GROUP_ORDER.filter((g) => byGroup.has(g)).map((group) => ({
    group,
    files: (byGroup.get(group) ?? []).sort((a, b) => a.label.localeCompare(b.label)),
  }));
}

// --- plugin manifest -------------------------------------------------------

/**
 * The manifest, including Curie's five authoring extensions.
 *
 * `systemPrompt`, `starterPrompts`, `secrets`, `triggers` and `approvalPolicy`
 * are Curie additions on top of the Claude Code plugin shape. Claude Code warns
 * and ignores them, which is the documented degradation contract, so they are
 * read here as authoring surface rather than validated as required.
 */
export interface PluginManifest {
  readonly name?: string;
  readonly version?: string;
  readonly description?: string;
  readonly systemPrompt?: string;
  readonly starterPrompts?: readonly string[];
  readonly secrets?: readonly string[];
  readonly triggerCount?: number;
  readonly approvalGates?: readonly string[];
}

export type Parsed<T> = { ok: true; value: T } | { ok: false; error: string };

/** Gate names out of `approvalPolicy.gates`, tolerating a shape this app has
 *  not seen: the extension set is additive and warn-and-ignore by contract. */
function gatesOf(policy: unknown): string[] | undefined {
  const gates = (policy as { gates?: unknown } | null)?.gates;
  if (!Array.isArray(gates)) return undefined;
  const names = gates
    .map((g) => (g as { gate?: unknown })?.gate)
    .filter((g): g is string => typeof g === "string");
  return names.length ? names : undefined;
}

export function parsePlugin(text: string): Parsed<PluginManifest> {
  try {
    const raw = JSON.parse(text) as Record<string, unknown>;
    if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
      return { ok: false, error: "plugin.json must be a JSON object" };
    }
    return {
      ok: true,
      value: {
        name: typeof raw.name === "string" ? raw.name : undefined,
        version: typeof raw.version === "string" ? raw.version : undefined,
        description: typeof raw.description === "string" ? raw.description : undefined,
        systemPrompt: typeof raw.systemPrompt === "string" ? raw.systemPrompt : undefined,
        starterPrompts: Array.isArray(raw.starterPrompts)
          ? raw.starterPrompts.filter((p): p is string => typeof p === "string")
          : undefined,
        secrets: Array.isArray(raw.secrets)
          ? raw.secrets.filter((x): x is string => typeof x === "string")
          : undefined,
        triggerCount: Array.isArray(raw.triggers) ? raw.triggers.length : undefined,
        approvalGates: gatesOf(raw.approvalPolicy),
      },
    };
  } catch (err) {
    return { ok: false, error: (err as Error).message };
  }
}

// --- eval suite ------------------------------------------------------------

/** The grader kinds the frozen eval-case schema allows. `tool_called` reads the
 *  observed tool trajectory rather than the answer text. */
export const GRADER_KINDS = ["exact", "contains", "regex", "tool_called"] as const;
export type GraderKind = (typeof GRADER_KINDS)[number];

export interface EvalCase {
  readonly id: string;
  readonly input: string;
  readonly grader: { kind: GraderKind; expected: string; case_sensitive?: boolean };
  readonly shared_history?: boolean;
  readonly expect_status?: "done" | "awaiting-approval";
  readonly note?: string;
}

export interface EvalSuite {
  readonly name: string;
  readonly cases: readonly EvalCase[];
}

/**
 * Read `evals/cases.json` against the frozen schema's required fields.
 *
 * Validation is per case and additive-tolerant: a case carrying a field this
 * app has not heard of is fine (the schema is explicitly additive), but a case
 * missing `id`, `input` or a usable `grader` is reported, because the eval
 * driver would reject the suite and the operator should learn that here rather
 * than from a red run.
 */
export function parseEvalSuite(text: string): Parsed<EvalSuite> {
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch (err) {
    return { ok: false, error: `not valid JSON: ${(err as Error).message}` };
  }
  const obj = raw as { name?: unknown; cases?: unknown };
  if (typeof obj !== "object" || obj === null) {
    return { ok: false, error: "expected a JSON object with `name` and `cases`" };
  }
  if (typeof obj.name !== "string" || !obj.name) {
    return { ok: false, error: "missing a suite `name`" };
  }
  if (!Array.isArray(obj.cases) || obj.cases.length === 0) {
    return { ok: false, error: "`cases` must be a non-empty array" };
  }

  const cases: EvalCase[] = [];
  for (const [i, entry] of obj.cases.entries()) {
    const c = entry as Record<string, unknown>;
    const g = c?.grader as Record<string, unknown> | undefined;
    const where = typeof c?.id === "string" ? `case "${c.id}"` : `case ${i + 1}`;
    if (typeof c?.id !== "string" || !c.id) return { ok: false, error: `${where}: missing \`id\`` };
    // Present, but not necessarily non-empty. The frozen schema
    // (`apps/worker/schema/eval-cases.schema.json`) types `input` as a plain
    // string with no `minLength`, and the eval driver has no emptiness check,
    // so `""` is a valid case -- and for some agents it is the only interesting
    // one. `examples/squawk` is a stack whose whole contract is that a non-empty
    // message pushes and an EMPTY message pops; refusing that case here made
    // this app stricter than the platform it is a client of, which is the rule
    // this file exists to keep.
    if (typeof c.input !== "string") {
      return { ok: false, error: `${where}: missing \`input\`` };
    }
    if (!g || typeof g.kind !== "string" || !GRADER_KINDS.includes(g.kind as GraderKind)) {
      return {
        ok: false,
        error: `${where}: grader \`kind\` must be one of ${GRADER_KINDS.join(", ")}`,
      };
    }
    if (typeof g.expected !== "string") {
      return { ok: false, error: `${where}: grader \`expected\` must be a string` };
    }
    cases.push({
      id: c.id,
      input: c.input,
      grader: {
        kind: g.kind as GraderKind,
        expected: g.expected,
        case_sensitive: typeof g.case_sensitive === "boolean" ? g.case_sensitive : undefined,
      },
      shared_history: typeof c.shared_history === "boolean" ? c.shared_history : undefined,
      expect_status:
        c.expect_status === "awaiting-approval" || c.expect_status === "done"
          ? c.expect_status
          : undefined,
      note: typeof c.note === "string" ? c.note : undefined,
    });
  }
  // Duplicate ids make a result table ambiguous about which case a row is.
  const ids = cases.map((c) => c.id);
  const dupe = ids.find((id, i) => ids.indexOf(id) !== i);
  if (dupe) return { ok: false, error: `duplicate case id "${dupe}"` };

  return { ok: true, value: { name: obj.name, cases } };
}

// --- skill frontmatter -----------------------------------------------------

export interface SkillMeta {
  readonly name?: string;
  readonly description?: string;
  readonly allowedTools: readonly string[];
}

/**
 * Read a SKILL.md's YAML frontmatter.
 *
 * Deliberately not a YAML parser. A skill's frontmatter is a flat map of scalars
 * plus one list, and pulling in a YAML dependency to read three keys would be
 * more surface than the feature. Anything it cannot read is reported as absent,
 * which the readiness check then surfaces, rather than guessed at.
 */
export function parseSkill(text: string): SkillMeta {
  const match = /^---\r?\n([\s\S]*?)\r?\n---/.exec(text.trimStart());
  if (!match) return { allowedTools: [] };

  const out: { name?: string; description?: string; allowedTools: string[] } = { allowedTools: [] };
  let listKey: string | null = null;

  for (const line of match[1].split(/\r?\n/)) {
    const item = /^\s*-\s+(.*)$/.exec(line);
    if (item && listKey === "allowed-tools") {
      out.allowedTools.push(item[1].trim().replace(/^["']|["']$/g, ""));
      continue;
    }
    const pair = /^([A-Za-z][\w-]*)\s*:\s*(.*)$/.exec(line);
    if (!pair) continue;
    const [, key, rawValue] = pair;
    const value = rawValue.trim().replace(/^["']|["']$/g, "");
    listKey = value === "" ? key : null;
    if (key === "name" && value) out.name = value;
    if (key === "description" && value) out.description = value;
    if (key === "allowed-tools" && value) {
      // Inline form: `allowed-tools: [WebSearch, WebFetch]` or a bare scalar.
      out.allowedTools.push(
        ...value
          .replace(/^\[|\]$/g, "")
          .split(",")
          .map((t) => t.trim())
          .filter(Boolean),
      );
    }
  }
  return out;
}

// --- readiness -------------------------------------------------------------

export type Level = "error" | "warn" | "info";

export interface Check {
  readonly id: string;
  readonly level: Level;
  readonly title: string;
  readonly detail: string;
  /** Manifest id of the command that addresses this, when one does. */
  readonly fix?: string;
}

/**
 * What is missing or questionable about this bundle, worst first.
 *
 * `error` means the tiers will reject it. `warn` means it will run but something
 * a reviewer would ask about is absent. `info` is an option not taken. The point
 * is that the answer is never a bare "invalid": every item names the thing to do
 * about it.
 */
export function readiness(
  ws: Workspace,
  opts: { plugin?: Parsed<PluginManifest>; evals?: Parsed<EvalSuite>; skills?: readonly SkillMeta[] } = {},
): Check[] {
  const out: Check[] = [];

  if (!ws.plugin) {
    out.push({
      id: "plugin-missing",
      level: "error",
      title: "No plugin manifest",
      detail:
        "A bundle is identified by .claude-plugin/plugin.json. Without it no tier can load this directory.",
      fix: "init",
    });
  } else if (opts.plugin && !opts.plugin.ok) {
    out.push({
      id: "plugin-invalid",
      level: "error",
      title: "plugin.json does not parse",
      detail: opts.plugin.error,
    });
  } else {
    if (!ws.plugin.name) {
      out.push({
        id: "plugin-name",
        level: "error",
        title: "plugin.json has no name",
        detail: "The name is the bundle's identity; deploys are keyed on it.",
      });
    }
    if (!ws.plugin.version) {
      out.push({
        id: "plugin-version",
        level: "warn",
        title: "plugin.json has no version",
        detail: "Versions are what make a deploy an immutable snapshot you can roll back to.",
      });
    }
    if (!ws.plugin.description) {
      out.push({
        id: "plugin-description",
        level: "info",
        title: "No description",
        detail: "The description is what a human sees when choosing between agents.",
      });
    }
  }

  if (ws.skills.length === 0) {
    // A warning, not a blocker, because that is what the platform's own
    // validator says: plugin_format's `skills.empty` is a warn, and the repo
    // ships `examples/compat-fixture`, a deliberately skill-less bundle. A
    // bundle with no skills loads and does nothing, which is worth saying
    // without claiming it is invalid.
    out.push({
      id: "no-skills",
      level: "warn",
      title: "No skills",
      detail:
        "This bundle loads but has nothing to do. A skill is skills/<name>/SKILL.md with name and description frontmatter.",
      fix: "init",
    });
  }

  for (const [i, skill] of (opts.skills ?? []).entries()) {
    if (!skill.description) {
      out.push({
        id: `skill-description-${i}`,
        level: "warn",
        title: `Skill "${skill.name ?? ws.skills[i] ?? i + 1}" has no description`,
        detail:
          "The description is how the model decides whether to invoke a skill. Without it the skill is close to unreachable.",
      });
    }
  }

  if (!ws.hasEvals) {
    out.push({
      id: "no-evals",
      level: "warn",
      title: "No eval cases",
      detail:
        "evals/cases.json is what makes a change to this bundle falsifiable, and it is the same file every tier grades against.",
      fix: "skill.eval-init",
    });
  } else if (opts.evals && !opts.evals.ok) {
    out.push({
      id: "evals-invalid",
      level: "error",
      title: "evals/cases.json does not parse",
      detail: opts.evals.error,
    });
  }

  if (!ws.hasMcp) {
    out.push({
      id: "no-mcp",
      level: "info",
      title: "No MCP servers",
      detail:
        "Optional. A bundle needs .mcp.json only if its skills call tools this platform does not already provide.",
    });
  }

  const rank: Record<Level, number> = { error: 0, warn: 1, info: 2 };
  return out.sort((a, b) => rank[a.level] - rank[b.level]);
}

/** One-line verdict for the header. */
export function verdict(checks: readonly Check[]): { level: Level | "ok"; text: string } {
  const errors = checks.filter((c) => c.level === "error").length;
  const warns = checks.filter((c) => c.level === "warn").length;
  if (errors) {
    return { level: "error", text: `${errors} thing${errors === 1 ? "" : "s"} will stop a deploy` };
  }
  if (warns) {
    return { level: "warn", text: `Deployable, ${warns} thing${warns === 1 ? "" : "s"} to look at` };
  }
  return { level: "ok", text: "Ready to deploy" };
}

/** Does this text parse as the thing its filename claims to be? Used to refuse
 *  a save that would write a broken contract file. */
export function validateForSave(path: string, text: string): string | null {
  const file = classifyFile(path);
  if (!file.structured) return null;
  if (path.endsWith(".json")) {
    if (path === ".claude-plugin/plugin.json") {
      const r = parsePlugin(text);
      return r.ok ? null : r.error;
    }
    if (path === "evals/cases.json") {
      const r = parseEvalSuite(text);
      return r.ok ? null : r.error;
    }
    try {
      JSON.parse(text);
      return null;
    } catch (err) {
      return (err as Error).message;
    }
  }
  // YAML is not parsed here; there is no parser and guessing would be worse
  // than letting the CLI be the judge.
  return null;
}

// --- editing, for the controls that stand in for a text editor ---------------
//
// The Build view lets somebody configure an agent with fields rather than by
// opening its files, so these are the writes behind those fields. Each one is a
// pure text-to-text function with a test, because "the app corrupted my agent"
// is the one outcome a settings panel must never produce, and a round trip
// through a form is exactly where that happens.

/**
 * The prose under a skill's frontmatter: what the agent should actually do.
 *
 * Returned separately from the frontmatter because it is the thing an author
 * writes and rewrites, while the frontmatter is three fields that change rarely.
 */
export function skillBody(text: string): string {
  const match = /^---\r?\n[\s\S]*?\r?\n---\r?\n?/.exec(text.trimStart());
  return match ? text.trimStart().slice(match[0].length).replace(/^\r?\n/, "") : text;
}

/**
 * Replace that prose, leaving the frontmatter untouched.
 *
 * A file with no frontmatter is returned with the new body and nothing else --
 * inventing a frontmatter block here would put a `name` in the file that the
 * author never chose, and the readiness check already reports its absence.
 */
export function withSkillBody(text: string, body: string): string {
  const trimmed = text.trimStart();
  const match = /^---\r?\n[\s\S]*?\r?\n---\r?\n?/.exec(trimmed);
  if (!match) return body.endsWith("\n") ? body : `${body}\n`;
  const head = trimmed.slice(0, match[0].length).replace(/\r?\n*$/, "\n");
  return `${head}\n${body.replace(/\s*$/, "")}\n`;
}

/**
 * Set one top-level field in `plugin.json`, or remove it when the value is empty.
 *
 * Removing rather than writing `""` or `[]` matters: the manifest's optional
 * fields are absent-or-present, and an empty string in `description` is a
 * description the platform will faithfully show as blank.
 *
 * Refuses to write anything if the file does not parse. A settings panel that
 * "fixes" a broken file by overwriting it with what it could salvage is how an
 * author loses the half of the file the panel does not model.
 */
export function withPluginField(
  text: string,
  key: string,
  value: string | readonly string[] | undefined,
): Parsed<string> {
  let doc: unknown;
  try {
    doc = JSON.parse(text);
  } catch (err) {
    return { ok: false, error: `plugin.json does not parse: ${(err as Error).message}` };
  }
  if (typeof doc !== "object" || doc === null || Array.isArray(doc)) {
    return { ok: false, error: "plugin.json is not a JSON object" };
  }
  const next = { ...(doc as Record<string, unknown>) };
  const empty =
    value === undefined || (typeof value === "string" ? !value.trim() : value.length === 0);
  if (empty) delete next[key];
  else next[key] = typeof value === "string" ? value.trim() : [...value];
  return { ok: true, value: `${JSON.stringify(next, null, 2)}\n` };
}
