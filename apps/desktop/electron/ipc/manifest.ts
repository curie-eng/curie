// Resolve a structural CliInvocation into argv, using the CLI's own command
// manifest as the only authority.
//
// The manifest (`cli/command-manifest.json`, emitted by `curie schema`) is what
// makes desktop/CLI parity structural rather than a promise someone has to keep
// updating: every command the CLI exposes is reachable here the moment it lands
// in the manifest, and a flag this app names that the CLI does not have is a
// hard error at resolve time instead of a confusing runtime failure.

import manifestJson from "../../src/generated/commandManifest.json" with { type: "json" };
import type { CliInvocation, ResolvedCommand } from "../shared/contract.js";

export interface ManifestArg {
  id: string;
  long?: string;
  short?: string;
  help?: string;
  positional: boolean;
  required: boolean;
  global?: boolean;
  possible_values?: string[];
  default_values?: string[];
  num_args?: { min?: number; max?: number };
}

export interface ManifestNode {
  name: string;
  about?: string;
  hidden?: boolean;
  args?: ManifestArg[];
  subcommands?: ManifestNode[];
}

export const manifest = manifestJson as unknown as ManifestNode;

/** Global flags (`--json`, `--quiet`, ...) declared on the root node. */
export const globalArgs: ManifestArg[] = (manifest.args ?? []).filter((a) => a.global);

export function resolveNode(action: string): { node: ManifestNode; path: string[] } {
  const parts = action.split(".").filter(Boolean);
  let node: ManifestNode = manifest;
  const path: string[] = [];
  for (const part of parts) {
    const next = (node.subcommands ?? []).find((s) => s.name === part);
    if (!next) {
      const where = path.length ? `curie ${path.join(" ")}` : "curie";
      throw new Error(`unknown command: ${part} is not a subcommand of ${where}`);
    }
    node = next;
    path.push(part);
  }
  return { node, path };
}

/** True for a group like `local` that only exists to hold subcommands. */
export function isGroup(node: ManifestNode): boolean {
  return (node.subcommands ?? []).length > 0;
}

/** Every runnable (leaf) command, dotted, in manifest order.
 *
 *  `includeHidden` matters for drift detection: the app only *offers* the
 *  commands clap shows, but a hidden command the binary still accepts is not a
 *  broken button, so the two directions of the comparison want different sets. */
export function leafActions(
  node: ManifestNode = manifest,
  prefix = "",
  includeHidden = false,
): string[] {
  const out: string[] = [];
  for (const sub of node.subcommands ?? []) {
    if (sub.hidden && !includeHidden) continue;
    const id = prefix ? `${prefix}.${sub.name}` : sub.name;
    if (isGroup(sub)) out.push(...leafActions(sub, id, includeHidden));
    else out.push(id);
  }
  return out;
}

/** How the CLI on this machine differs from the manifest this app was built
 *  against.
 *
 *  The app's command surface is generated from `cli/command-manifest.json` at
 *  build time, but it drives whatever `curie` is on PATH at run time, and those
 *  are not always the same version. The two directions are not equally bad:
 *
 *  - `missingFromCli` is a broken button: the app offers a command the installed
 *    binary does not have, so clicking it fails.
 *  - `missingFromApp` means the installed CLI can do something this app does not
 *    offer -- the app has quietly become the lesser surface, which is the exact
 *    failure it exists to avoid.
 *
 *  Both are fixed the same way: regenerate the manifest against the CLI the app
 *  is actually driving. */
export interface ManifestDrift {
  readonly cliVersion: string | null;
  readonly missingFromApp: string[];
  readonly missingFromCli: string[];
}

export function compareToLive(liveSchema: unknown, cliVersion: string | null): ManifestDrift | null {
  const live = liveSchema as ManifestNode | null;
  if (!live || !Array.isArray(live.subcommands)) return null;

  const appOffers = new Set(leafActions(manifest));
  const cliAccepts = new Set(leafActions(live, "", true));
  const cliShows = new Set(leafActions(live));

  return {
    cliVersion,
    missingFromApp: [...cliShows].filter((id) => !appOffers.has(id)).sort(),
    missingFromCli: [...appOffers].filter((id) => !cliAccepts.has(id)).sort(),
  };
}

function quote(token: string): string {
  return /^[A-Za-z0-9_@%+=:,./-]+$/.test(token) ? token : `'${token.replace(/'/g, `'\\''`)}'`;
}

export function resolve(inv: CliInvocation, defaultCwd: string): ResolvedCommand {
  const { node, path } = resolveNode(inv.action);
  if (isGroup(node)) {
    throw new Error(`${["curie", ...path].join(" ")} is a command group, not a runnable command`);
  }

  const args = node.args ?? [];
  const positionalSpecs = args.filter((a) => a.positional);
  const flagSpecs = new Map<string, ManifestArg>();
  for (const a of [...args, ...globalArgs]) {
    if (!a.positional && a.long) flagSpecs.set(a.long, a);
  }

  const argv: string[] = [...path];

  // Positionals go in the manifest's declared order; a gap in the middle would
  // silently shift meaning, so refuse rather than guess.
  const positionals = (inv.positionals ?? []).map((v) => (v ?? "").trim());
  positionals.forEach((value, i) => {
    if (!value) return;
    if (i >= positionalSpecs.length) {
      throw new Error(`${inv.action} takes ${positionalSpecs.length} positional argument(s)`);
    }
    argv.push(value);
  });
  const firstGap = positionals.findIndex((v) => !v);
  if (firstGap >= 0 && positionals.slice(firstGap).some(Boolean)) {
    throw new Error(
      `${inv.action}: <${positionalSpecs[firstGap]?.id.toUpperCase()}> is required before the ` +
        `later positional arguments can be given`,
    );
  }
  positionalSpecs.forEach((spec, i) => {
    if (spec.required && !positionals[i]) {
      throw new Error(`${inv.action}: <${spec.id.toUpperCase()}> is required`);
    }
  });

  for (const [long, raw] of Object.entries(inv.flags ?? {})) {
    // Skip BEFORE validating. A flag that is unset -- `false`, empty, absent --
    // contributes nothing to argv, and `renderCommand` already omits it from the
    // preview, so rejecting it here made the two disagree about a command that
    // was going to run identically either way.
    //
    // What that cost in practice: the form seeds every boolean flag as `false`,
    // so a renderer holding a manifest one command newer than the main process
    // sent a `false` for a flag main had never heard of, and a preview reading
    // `curie local up` refused to start. Validation belongs to what reaches
    // argv, not to what was mentioned.
    if (raw === undefined || raw === false || raw === "") continue;
    const spec = flagSpecs.get(long);
    if (!spec) throw new Error(`${inv.action} has no --${long} flag`);
    if (raw === true) {
      argv.push(`--${long}`);
      continue;
    }
    const value = String(raw);
    // clap booleans are declared with possible_values true/false; the CLI wants
    // the bare flag, not `--flag true`.
    if (spec.possible_values?.length === 2 && spec.possible_values.includes("true")) {
      if (value === "true") argv.push(`--${long}`);
      continue;
    }
    if (spec.possible_values?.length && !spec.possible_values.includes(value)) {
      throw new Error(
        `${inv.action}: --${long} must be one of ${spec.possible_values.join(", ")} (got ${value})`,
      );
    }
    argv.push(`--${long}`, value);
  }

  if (inv.json && !argv.includes("--json")) argv.push("--json");

  const cwd = inv.cwd || defaultCwd;
  return {
    argv,
    display: ["curie", ...argv].map(quote).join(" "),
    cwd,
  };
}
