// The renderer's view of the CLI's command manifest.
//
// This is the load-bearing idea in the whole app. The brief was that the UI must
// not be a worse experience than the CLI, and the way to guarantee that is to
// stop hand-writing UI per command: every runnable command in
// `cli/command-manifest.json` gets a real form here, derived from its own
// declared arguments, with its own help text as the field description. A command
// added to the CLI shows up in this app on the next `pnpm gen:manifest` without
// anyone building a screen for it, and a flag that is removed cannot linger,
// because there is no hand-written copy of it to go stale.
//
// What is hand-written is only the *judgement* the manifest cannot carry: which
// commands change something irreversible, which argument is a path versus a
// number, and which handful of commands deserve a purpose-built surface on top
// of the generic form. Those live in small tables below, each defaulting to the
// safe answer when a command is not listed.

import { commandManifest } from "../generated/commandManifest";

export interface ManifestArg {
  readonly id: string;
  readonly long?: string;
  readonly short?: string;
  readonly help?: string;
  readonly positional: boolean;
  readonly required: boolean;
  readonly global?: boolean;
  readonly possible_values?: readonly string[];
  readonly default_values?: readonly string[];
}

export interface ManifestNode {
  readonly name: string;
  readonly about?: string;
  readonly hidden?: boolean;
  readonly args?: readonly ManifestArg[];
  readonly subcommands?: readonly ManifestNode[];
}

export const root = commandManifest as unknown as ManifestNode;

/** Flags declared on the root and accepted by every command. */
export const globalArgs: readonly ManifestArg[] = (root.args ?? []).filter((a) => a.global);

export function isGroup(node: ManifestNode): boolean {
  return (node.subcommands ?? []).length > 0;
}

export interface Command {
  /** Dotted id, e.g. `local.deploy`. */
  readonly id: string;
  /** argv words, e.g. `["local", "deploy"]`. */
  readonly path: readonly string[];
  readonly name: string;
  readonly about: string;
  readonly group: string;
  readonly node: ManifestNode;
  readonly positionals: readonly ManifestArg[];
  readonly flags: readonly ManifestArg[];
  readonly tier: Tier;
  readonly risk: Risk;
}

/** Which rung of the parity ladder a command belongs to. The ladder is the
 *  product's central idea, so it is also how this app groups its commands. */
export type Tier = "author" | "skill" | "local" | "cluster" | "dev" | "platform";

/** How much damage running this can do, which decides whether the UI asks
 *  first. Default is `safe`; anything unlisted that reads like a teardown is
 *  caught by the name heuristic, so a new destructive command is guarded from
 *  the day it lands rather than the day someone remembers to list it. */
export type Risk = "safe" | "mutating" | "destructive";

const TIER_OF: Record<string, Tier> = {
  init: "author",
  "list-agents": "author",
  skill: "skill",
  local: "local",
  "deploy-local": "local",
  cluster: "cluster",
  dev: "dev",
};

/** Commands that destroy state or stop work in flight. Everything here gets a
 *  confirm step and a red action button. */
const DESTRUCTIVE = new Set([
  "skill.down",
  "local.down",
  "cluster.down",
  "cluster.delete",
  "local.kill",
  "cluster.kill",
  "local.reset-thread",
  "cluster.reset-thread",
  "secrets.unset",
  "cluster.migrate-store",
]);

/** Commands that change live state without destroying it. */
const MUTATING = new Set([
  "init",
  "skill.up",
  "local.up",
  "local.rebuild",
  "local.deploy",
  "local.comms",
  "local.overrides",
  "local.budget",
  "local.resume",
  "cluster.up",
  "cluster.deploy",
  "cluster.comms",
  "cluster.github-app",
  "cluster.overrides",
  "cluster.budget",
  "cluster.resume",
  "deploy-local",
  "build",
  "install",
  "update",
  "apply",
  "seal",
  "secrets.set",
  // These two rewrite files in the checkout rather than doing anything to a
  // deployment, but "it edited tracked files" is still a change somebody has to
  // review, so they are not `safe`.
  "dev.schema-baseline",
  "dev.bump-version",
]);

/** Commands the CLI itself refuses without a terminal, and what this app offers
 *  instead.
 *
 *  A spawned process here has no TTY, so these would fail the moment they run.
 *  Rather than a button that always errors, the form says so up front and names
 *  the surface that does the same job. Both entries are grounded in the CLI's own
 *  `is_terminal()` guards (`cli/src/interactive.rs`, `cli/src/secrets.rs`), not
 *  in a guess about what might need a prompt.
 *
 *  Note what is NOT here: `init` and `skill eval-init` also interview you, but
 *  they read stdin rather than requiring a TTY, so the transcript drawer's stdin
 *  box answers them and they stay fully runnable. */
export const NEEDS_TERMINAL: Record<string, string> = {
  interactive:
    "This opens the CLI's own terminal UI, which needs a real terminal. This app is the same thing in a window — use the command palette (⌘K) instead, or run `curie` in a terminal.",
  "secrets.set":
    "Typing a secret at a hidden prompt needs a real terminal. Use Settings → Secrets, which hands the value to the CLI through the environment (`--from-env`) so it never appears in `ps`.",
};

function riskOf(id: string): Risk {
  if (DESTRUCTIVE.has(id)) return "destructive";
  if (MUTATING.has(id)) return "mutating";
  // Belt and braces for commands that land after this table was written.
  const leaf = id.split(".").pop() ?? "";
  if (/^(down|delete|destroy|kill|remove|uninstall|purge|wipe)$/.test(leaf)) return "destructive";
  return "safe";
}

function walk(node: ManifestNode, path: string[], out: Command[]): void {
  for (const sub of node.subcommands ?? []) {
    if (sub.hidden) continue;
    const next = [...path, sub.name];
    if (isGroup(sub)) {
      walk(sub, next, out);
      continue;
    }
    const id = next.join(".");
    const args = sub.args ?? [];
    out.push({
      id,
      path: next,
      name: sub.name,
      about: sub.about ?? "",
      group: next.length > 1 ? next[0] : "",
      node: sub,
      positionals: args.filter((a) => a.positional),
      flags: args.filter((a) => !a.positional && a.long),
      tier: TIER_OF[next[0]] ?? "platform",
      risk: riskOf(id),
    });
  }
}

/** Every runnable command, in manifest order. */
export const commands: readonly Command[] = (() => {
  const out: Command[] = [];
  walk(root, [], out);
  return out;
})();

export const commandsById = new Map(commands.map((c) => [c.id, c]));

export function command(id: string): Command | undefined {
  return commandsById.get(id);
}

// --- argument kinds --------------------------------------------------------

/** How to render one argument. The manifest says what a flag is *called* and
 *  what values clap will accept, but not whether a string is a filesystem path,
 *  a port, or a secret -- and that difference is most of what makes a form
 *  pleasant. These are inferred from the argument id, with the manifest's own
 *  `possible_values` always winning where it exists. */
export type FieldKind = "boolean" | "enum" | "path" | "file" | "number" | "secret" | "json" | "text";

const PATH_IDS = /^(dir|plugin_dir|out|chart|clone_base)$/;
const FILE_IDS = /^(file|cases|from_spec|env_file|private_key|public_key|routes_from|path)$/;
const NUMBER_IDS = /(port|timeout|timeout_secs|concurrency|lines|limit|app_id)$/;
const SECRET_IDS = /(token|api_key|key|secret|password)$/;
const JSON_IDS = /^(budget|set)$/;

export function fieldKind(arg: ManifestArg): FieldKind {
  const values = arg.possible_values ?? [];
  if (values.length === 2 && values.includes("true") && values.includes("false")) return "boolean";
  if (values.length > 0) return "enum";
  if (JSON_IDS.test(arg.id)) return "json";
  if (FILE_IDS.test(arg.id)) return "file";
  if (PATH_IDS.test(arg.id)) return "path";
  if (NUMBER_IDS.test(arg.id)) return "number";
  // `--secret` on the deploy/up commands is a NAME=VALUE binding, not a value
  // to hide; only the flags that carry raw material are masked.
  if (SECRET_IDS.test(arg.id) && arg.id !== "secret") return "secret";
  return "text";
}

export function defaultValue(arg: ManifestArg): string {
  return arg.default_values?.[0] ?? "";
}

/** Search over id, name, and help text. Ranked so an exact command name beats a
 *  help-text mention, which is what makes the palette feel like it read your
 *  mind on `curie local up`. */
export function search(query: string, limit = 40): Command[] {
  const q = query.trim().toLowerCase();
  if (!q) return commands.slice(0, limit);
  const terms = q.split(/\s+/);
  const scored: { c: Command; score: number }[] = [];
  for (const c of commands) {
    const spaced = c.path.join(" ");
    let score = 0;
    for (const t of terms) {
      if (c.name === t) score += 100;
      else if (spaced.startsWith(t)) score += 60;
      else if (c.name.startsWith(t)) score += 40;
      else if (spaced.includes(t)) score += 25;
      else if (c.about.toLowerCase().includes(t)) score += 8;
      else {
        score = -1;
        break;
      }
    }
    if (score > 0) scored.push({ c, score });
  }
  scored.sort((a, b) => b.score - a.score || a.c.id.localeCompare(b.c.id));
  return scored.slice(0, limit).map((s) => s.c);
}

/** Render the exact `curie ...` string for a filled-in form. The main process
 *  resolves argv independently for the actual run; this is the copy-pasteable
 *  form shown in the UI, and the two agreeing is checked by test. */
export function renderCommand(
  cmd: Command,
  positionals: readonly string[],
  flags: Readonly<Record<string, string | boolean | undefined>>,
  opts: { json?: boolean } = {},
): string {
  const tokens: string[] = ["curie", ...cmd.path];
  cmd.positionals.forEach((_spec, i) => {
    const value = (positionals[i] ?? "").trim();
    if (value) tokens.push(value);
  });
  for (const spec of cmd.flags) {
    const long = spec.long!;
    const raw = flags[long];
    if (raw === undefined || raw === false || raw === "") continue;
    if (raw === true) {
      tokens.push(`--${long}`);
      continue;
    }
    const value = String(raw);
    if (fieldKind(spec) === "boolean") {
      if (value === "true") tokens.push(`--${long}`);
      continue;
    }
    tokens.push(`--${long}`, value);
  }
  if (opts.json) tokens.push("--json");
  return tokens.map(quote).join(" ");
}

function quote(token: string): string {
  return /^[A-Za-z0-9_@%+=:,./-]+$/.test(token) ? token : `'${token.replace(/'/g, `'\\''`)}'`;
}

/** Commands whose values are worth remembering between runs, keyed by the flag
 *  that identifies the target. Re-typing `--api-url` on every command is the
 *  main tax of driving this CLI by hand, and the desktop app is the right place
 *  to stop paying it. */
export const STICKY_FLAGS = new Set([
  "api-url",
  "api-key",
  "namespace",
  "release",
  "plugin-dir",
  "channel",
  "slack-channel",
  "model",
  "file",
  "agent",
  "target",
]);
