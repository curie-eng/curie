// Where each command lives in the app.
//
// The Commands view answers "what can I do" completely, because it is the
// manifest. What it cannot answer is "where would I have found this without
// searching for it" -- and a console whose only answer to that is a filter box
// over 80 monospace strings has reproduced `--help` in a window. The list is a
// reference; a reference is not an interface.
//
// So every command also belongs to a *surface*: a named group of controls on a
// real screen, in the place an operator would already be when they want it.
// Deploying is on the bundle you have open. Killing an agent is on that agent's
// row. Bringing the cluster up is on the tier that owns it. The surfaces below
// are the data those controls are rendered from -- the views map over these
// arrays, they do not hand-write buttons -- so the map cannot claim a home that
// does not exist on screen.
//
// `surfaces.test.ts` asserts the two directions that matter: every command in
// the manifest is on at least one surface, and no surface names a command the
// CLI does not have. A command added to the CLI therefore fails the build until
// somebody decides where in the app it belongs, which is the decision that was
// being skipped when everything defaulted into the list.

import type { Route } from "../bridge/app";
import { commandsById, type Command } from "./manifest";

/** What a control needs beyond the command itself: how it reads, and how loud
 *  it is. The description comes from the manifest -- a second copy of the help
 *  text here is a copy that goes stale. */
export interface Action {
  /** Dotted command id, e.g. `local.kill`. */
  readonly id: string;
  /** The words on the control. An imperative phrase in the operator's language,
   *  not the command name: "Stop the stack", not "down". */
  readonly label: string;
  /** Extra context for a tooltip, where the manifest's own `about` is not the
   *  thing worth saying at this particular button. */
  readonly hint?: string;
  readonly tone?: "primary" | "danger" | "plain";
  /** A command that runs *about* the tier rather than doing anything to it:
   *  rendered quieter, and never the primary control. */
  readonly quiet?: boolean;
}

/** Which live precondition a surface depends on, so a view can say why a group
 *  is inert instead of letting each command fail separately three seconds in. */
export type Need = "docker" | "kubectl" | "api" | "checkout" | "bundle";

export interface Surface {
  /** Stable key, and the anchor a "take me there" link scrolls to. */
  readonly id: string;
  readonly route: Route;
  /** The section header this group renders under. */
  readonly title: string;
  /** One sentence under the header: what this group of controls is for. */
  readonly blurb: string;
  /** Where on that route the controls actually are, as directions you could
   *  follow. A route name alone is not an answer for a group that lives inside
   *  something you have to open first -- "Overview" does not tell you the agent
   *  commands are on a row. */
  readonly where: string;
  readonly needs?: Need;
  readonly actions: readonly Action[];
}

/**
 * Every surface, in the order an operator meets them: author a bundle, run it up
 * the ladder, operate the agents it becomes, then the machine underneath.
 *
 * Order is meaningful. `homeOf()` returns the first surface that lists a
 * command, so the first mention is the canonical home and later ones are
 * shortcuts -- `local deploy` is *filed* under the bundle you are shipping and
 * *also* reachable from the agent it deploys.
 */
export const SURFACES: readonly Surface[] = [
  // --- Build -------------------------------------------------------------
  {
    id: "build.author",
    route: "build",
    title: "Start something",
    blurb: "Scaffold a bundle, or find the ones already on this machine.",
    where: "at the foot of the Build view, with or without a bundle open",
    actions: [
      { id: "init", label: "New agent…", tone: "primary", hint: "Scaffold a Claude Code plugin bundle" },
      { id: "try", label: "First reply, no keys", hint: "Scaffold a keyless first reply to prove the loop works" },
      { id: "list-agents", label: "Find local bundles", quiet: true, hint: "List bundles under agents/ in a source checkout" },
    ],
  },
  {
    id: "build.loop",
    route: "build",
    title: "The loop",
    blurb: "One container, straight from the open bundle. The fast edit-run-grade cycle.",
    where: "on the Build view, under the open bundle",
    needs: "docker",
    actions: [
      { id: "skill.check", label: "Check the servers load", tone: "primary", hint: "Do the MCP servers load, offline?" },
      { id: "skill.up", label: "Boot runner", hint: "One container, straight from this directory" },
      { id: "skill.status", label: "Session status", quiet: true },
      { id: "skill.message", label: "Say something to it", hint: "Send a synthetic event and read the reply" },
      { id: "skill.eval", label: "Grade", hint: "Run evals/cases.json through the runner" },
      { id: "skill.eval-init", label: "Write eval cases", hint: "Interview to generate a starter evals/cases.json" },
      { id: "skill.approvals", label: "Approval gates", quiet: true, hint: "What this bundle declares as gated" },
      { id: "skill.down", label: "Stop runner", tone: "danger" },
    ],
  },
  {
    id: "build.not-here",
    route: "build",
    title: "Not at this tier",
    blurb:
      "Two verbs the ladder has further up but the skill tier does not. They run, and explain why.",
    where: "on the Build view, under the open bundle",
    actions: [
      { id: "skill.versions", label: "Why no versions here", quiet: true },
      { id: "skill.memory", label: "Why no memory here", quiet: true },
    ],
  },
  {
    id: "build.ship",
    route: "build",
    title: "Ship it",
    blurb: "Push the open bundle at a platform and deploy it as an agent.",
    where: "on the Build view, under the open bundle",
    actions: [
      { id: "local.deploy", label: "Deploy to local", tone: "primary" },
      { id: "cluster.deploy", label: "Deploy to cluster" },
      { id: "deploy-local", label: "Deploy a repo bundle by name", quiet: true },
    ],
  },

  // --- Tiers -------------------------------------------------------------
  {
    id: "tiers.skill",
    route: "tiers",
    title: "Skill tier",
    blurb: "One runner container from a directory. No database, no queue, no platform.",
    where: "on the Tiers view, first panel",
    needs: "docker",
    actions: [
      { id: "skill.up", label: "Boot runner", tone: "primary" },
      { id: "skill.status", label: "Session status", quiet: true },
      { id: "skill.check", label: "Check MCP servers", quiet: true },
      { id: "skill.message", label: "Message it" },
      { id: "skill.eval", label: "Grade it" },
      { id: "skill.down", label: "Stop", tone: "danger" },
    ],
  },
  {
    id: "tiers.local",
    route: "tiers",
    title: "Local tier",
    blurb: "The whole platform on Docker Compose, on this machine.",
    where: "on the Tiers view, second panel",
    needs: "docker",
    actions: [
      { id: "local.up", label: "Bring up", tone: "primary" },
      { id: "local.status", label: "Service status", quiet: true },
      { id: "local.rebuild", label: "Rebuild a service", hint: "Recreate one compose service after a code change" },
      { id: "local.comms", label: "Connect Slack", hint: "Connect or disconnect a real Slack workspace" },
      { id: "local.observability", label: "Observability URLs", quiet: true },
      { id: "local.message", label: "Message an agent" },
      { id: "local.eval", label: "Grade a bundle" },
      { id: "local.down", label: "Stop the stack", tone: "danger" },
    ],
  },
  {
    id: "tiers.cluster",
    route: "tiers",
    title: "Cluster tier",
    blurb: "The same platform on Kubernetes, via Helm.",
    where: "on the Tiers view, third panel",
    needs: "kubectl",
    actions: [
      { id: "cluster.up", label: "Install or upgrade", tone: "primary" },
      { id: "cluster.status", label: "Release health", quiet: true },
      { id: "cluster.comms", label: "Connect Slack" },
      { id: "cluster.github-app", label: "GitHub identity", hint: "Give the platform its own GitHub App" },
      { id: "cluster.observability", label: "Observability URLs", quiet: true },
      { id: "cluster.message", label: "Message an agent" },
      { id: "cluster.eval", label: "Grade a bundle" },
      { id: "cluster.migrate-store", label: "Migrate object store", tone: "danger", hint: "Carry bundles across a chart upgrade that renames the store" },
      { id: "cluster.down", label: "Uninstall", tone: "danger" },
    ],
  },
  {
    id: "tiers.declarative",
    route: "tiers",
    title: "Declarative install",
    blurb: "A cluster described by a curie.yaml file, converged rather than clicked.",
    where: "on the Tiers view, below the three rungs",
    needs: "kubectl",
    actions: [
      { id: "diff", label: "What would change", tone: "primary", hint: "Show what apply would do to the live release" },
      { id: "apply", label: "Converge", hint: "Bring the cluster to the state the file describes" },
      { id: "seal", label: "Seal a credential", hint: "Encrypt a connector credential to this cluster" },
    ],
  },
  {
    id: "tiers.examples",
    route: "tiers",
    // "Worked example" is a textbook phrase, not a label. A section header is a
    // short noun naming what is in the box.
    title: "Example",
    blurb: "A complete installation you can read the source of, end to end.",
    where: "at the foot of the Tiers view",
    actions: [
      { id: "example.sre-bot.install", label: "Install the SRE bot", hint: "Curie, its observability stack, and the SRE bundle" },
    ],
  },

  // --- Agents (a sheet, opened from an agent anywhere in the app) ---------
  {
    id: "agent.talk",
    route: "overview",
    title: "Agent · Talk to it",
    blurb: "Drive a deployed agent end to end without touching Slack.",
    where: "on each agent's row — click one to open its sheet",
    needs: "api",
    actions: [
      { id: "local.message", label: "Send a message", tone: "primary" },
      { id: "cluster.message", label: "Send a message", tone: "primary" },
      { id: "local.eval", label: "Run its evals" },
      { id: "cluster.eval", label: "Run its evals" },
    ],
  },
  {
    id: "agent.inspect",
    route: "overview",
    title: "Agent · Look at it",
    blurb: "What it has shipped, what it has learned, and what it is waiting on.",
    where: "on each agent's row — click one to open its sheet",
    needs: "api",
    actions: [
      { id: "local.versions", label: "Versions" },
      { id: "cluster.versions", label: "Versions" },
      { id: "local.memory", label: "Memory" },
      { id: "cluster.memory", label: "Memory" },
      { id: "local.approvals", label: "Approvals" },
      { id: "cluster.approvals", label: "Approvals" },
    ],
  },
  {
    id: "agent.configure",
    route: "overview",
    title: "Agent · Change it",
    blurb: "Model, thinking, the surfaces it answers on, and what it may spend.",
    where: "on each agent's row — click one to open its sheet",
    needs: "api",
    actions: [
      { id: "local.overrides", label: "Model & thinking" },
      { id: "cluster.overrides", label: "Model & thinking" },
      { id: "local.surfaces", label: "Surfaces" },
      { id: "cluster.surfaces", label: "Surfaces" },
      { id: "local.budget", label: "Daily budget" },
      { id: "cluster.budget", label: "Daily budget" },
    ],
  },
  {
    id: "agent.control",
    route: "overview",
    title: "Agent · Stop it",
    blurb: "The emergency controls. Every one of these changes live state.",
    where: "on each agent's row — click one to open its sheet",
    needs: "api",
    actions: [
      { id: "local.kill", label: "Kill", tone: "danger" },
      { id: "cluster.kill", label: "Kill", tone: "danger" },
      { id: "local.resume", label: "Resume" },
      { id: "cluster.resume", label: "Resume" },
      { id: "local.reset-thread", label: "Release a stuck thread", tone: "danger" },
      { id: "cluster.reset-thread", label: "Release a stuck thread", tone: "danger" },
      { id: "local.delete", label: "Delete", tone: "danger" },
      { id: "cluster.delete", label: "Delete", tone: "danger" },
    ],
  },

  // --- Settings ----------------------------------------------------------
  {
    id: "settings.secrets",
    route: "settings",
    title: "Secrets",
    blurb: "Names only. Values go to the CLI through the environment and are never read back.",
    where: "in Settings, under Secrets",
    actions: [
      { id: "secrets.list", label: "List", quiet: true },
      { id: "secrets.set", label: "Add secret", tone: "primary" },
      { id: "secrets.unset", label: "Remove", tone: "danger" },
    ],
  },
  {
    id: "settings.machine",
    route: "settings",
    title: "This machine",
    blurb: "Setting up, updating, and diagnosing the CLI this app is a front end for.",
    where: "in Settings, under This machine",
    actions: [
      { id: "doctor", label: "Diagnose", tone: "primary", hint: "What is set up, what is missing, and the command that fixes it" },
      { id: "install", label: "Bootstrap a checkout", hint: "Install deps and build; starts nothing" },
      { id: "update", label: "Rebuild the CLI", hint: "Reinstall curie on PATH from this checkout" },
      { id: "build", label: "Build the runner image" },
      { id: "interactive", label: "Terminal interface", quiet: true, hint: "The CLI's own TUI — needs a real terminal" },
    ],
  },
  {
    id: "settings.reference",
    route: "settings",
    title: "Reference output",
    blurb: "The two commands that print something to read rather than doing anything.",
    where: "in Settings, under Reference output",
    actions: [
      { id: "guide", label: "Agent primer", hint: "A self-contained primer for a coding agent driving the harness" },
      { id: "schema-index", label: "JSON schemas", hint: "The committed schemas for every --json result" },
    ],
  },
  {
    id: "settings.dev",
    route: "settings",
    title: "Repo checks",
    blurb:
      "Contributor scripts. These need a source checkout and the repo's toolchains, not a released binary.",
    where: "in Settings, under Repo checks",
    needs: "checkout",
    actions: [
      { id: "dev.contracts", label: "Frozen contracts" },
      { id: "dev.docs-lint", label: "Docs lint" },
      { id: "dev.chart-check", label: "Chart render-assert" },
      { id: "dev.chart-runtime-e2e", label: "Chart runtime E2E" },
      { id: "dev.netpol-check", label: "Network policy enforced" },
      { id: "dev.e2e", label: "CLI end-to-end" },
      { id: "dev.e2e-ladder", label: "Cold-start ladder" },
      { id: "dev.plugin-compat", label: "Bundle compatibility" },
      { id: "dev.eval-falsifiability", label: "Evals can fail" },
      { id: "dev.field-parity", label: "Field parity" },
      { id: "dev.emit-parity", label: "Emit parity" },
      { id: "dev.verb-parity", label: "Verb parity" },
      { id: "dev.wire-tolerance", label: "Wire tolerance" },
      { id: "dev.verify-fix-pin", label: "Fix is pinned by a test" },
      { id: "dev.version-check", label: "Release versions agree" },
      { id: "dev.schema-baseline", label: "Refresh schema baseline", hint: "Rewrites the committed ADR-0101 baseline" },
      { id: "dev.bump-version", label: "Bump the release version", hint: "Rewrites Cargo.toml, Chart.yaml and the chart appVersion" },
    ],
  },

  // --- Resources ---------------------------------------------------------
  //
  // Declared last on purpose: everything here already has a home above, and the
  // inspector offers it again against the one container you are looking at.
  {
    id: "resources.inspect",
    route: "resources",
    title: "Run against a container",
    blurb: "The commands that apply to the workload open in the inspector.",
    where: "in Resources, in the sheet a container row opens",
    actions: [
      { id: "skill.status", label: "Check session status" },
      { id: "skill.message", label: "Send a message" },
      { id: "skill.down", label: "Stop this runner", tone: "danger" },
      { id: "local.status", label: "Stack status" },
      { id: "local.rebuild", label: "Rebuild this service" },
      { id: "local.down", label: "Stop the whole stack", tone: "danger" },
      { id: "cluster.status", label: "Check cluster status" },
    ],
  },
];

export const surfacesById = new Map(SURFACES.map((s) => [s.id, s]));

/** Surfaces on one route, in declaration order. */
export function surfacesFor(route: Route): readonly Surface[] {
  return SURFACES.filter((s) => s.route === route);
}

export interface Placement {
  readonly surface: Surface;
  readonly action: Action;
}

/** Every place a command is reachable from, in declaration order. The first is
 *  its home; the rest are shortcuts from context. */
export function placementsOf(commandId: string): readonly Placement[] {
  const out: Placement[] = [];
  for (const surface of SURFACES) {
    const action = surface.actions.find((a) => a.id === commandId);
    if (action) out.push({ surface, action });
  }
  return out;
}

export function homeOf(commandId: string): Placement | undefined {
  return placementsOf(commandId)[0];
}

/** The commands a surface offers, resolved against the manifest. An id with no
 *  command is dropped rather than rendered as a dead control -- the test is what
 *  stops that happening silently. */
export function resolve(surface: Surface): readonly { action: Action; cmd: Command }[] {
  return surface.actions
    .map((action) => ({ action, cmd: commandsById.get(action.id) }))
    .filter((x): x is { action: Action; cmd: Command } => !!x.cmd);
}

/** Every command id any surface offers. */
export const placedIds: ReadonlySet<string> = new Set(
  SURFACES.flatMap((s) => s.actions.map((a) => a.id)),
);
