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
    title: "The same thing, as commands",
    blurb:
      "New agent above does this for you. These are here for anyone who would rather run it, or wants the line to paste into a terminal.",
    where: "at the foot of the Build view, with or without a bundle open",
    actions: [
      { id: "init", label: "Make an empty one", hint: "Writes the files and leaves the rest to you" },
      { id: "try", label: "See one work, no setup", hint: "Makes a small demo agent and gets a reply out of it, with no keys and nothing to configure" },
      { id: "list-agents", label: "Find ones already here", quiet: true, hint: "Lists agents already saved on this computer" },
    ],
  },
  {
    id: "build.loop",
    route: "build",
    title: "Try it",
    blurb:
      "A private copy of this agent, running here on your computer. Change something, say something to it, see what it does.",
    where: "on the Build view, under the open bundle",
    needs: "docker",
    actions: [
      { id: "skill.check", label: "Check its tools work", tone: "primary", hint: "Loads the outside tools this agent calls, without going online" },
      { id: "skill.up", label: "Start a test copy", hint: "A private copy of this agent, straight from this folder" },
      { id: "skill.status", label: "Is it running?", quiet: true },
      { id: "skill.message", label: "Say something to it", hint: "Sends it a message and shows you the reply" },
      { id: "skill.eval", label: "Score it", hint: "Runs this agent's saved examples and scores the answers" },
      { id: "skill.eval-init", label: "Write some examples", hint: "Asks you questions and writes a starter set of examples to score against" },
      { id: "skill.approvals", label: "What needs approval", quiet: true, hint: "Which of this agent's actions have to be approved first" },
      { id: "skill.down", label: "Stop the test copy", tone: "danger" },
    ],
  },
  {
    id: "build.not-here",
    route: "build",
    title: "Not available yet",
    blurb:
      "These need somewhere the agent actually lives, which a private copy is not. Each one explains itself if you run it.",
    where: "on the Build view, under the open bundle",
    actions: [
      { id: "skill.versions", label: "Why no versions here", quiet: true },
      { id: "skill.memory", label: "Why no memory here", quiet: true },
      { id: "skill.observability.runs", label: "Why no recent activity here", quiet: true },
      { id: "skill.observability.run", label: "Why no conversations here", quiet: true },
      { id: "skill.observability.metrics", label: "Why no usage here", quiet: true },
    ],
  },
  {
    id: "build.ship",
    route: "build",
    title: "Put it to work",
    blurb: "Send this agent somewhere people can reach it and start using it.",
    where: "on the Build view, under the open bundle",
    actions: [
      { id: "local.deploy", label: "Put it on this computer", tone: "primary" },
      { id: "cluster.deploy", label: "Share it with the team" },
      { id: "deploy-local", label: "Send one by name", quiet: true },
    ],
  },

  // --- Tiers -------------------------------------------------------------
  {
    id: "tiers.skill",
    route: "tiers",
    title: "Just you, right here",
    blurb:
      "A private copy on your own computer. Starts in seconds, forgets everything when it stops, and nobody else can reach it.",
    where: "on the Tiers view, first panel",
    needs: "docker",
    actions: [
      { id: "skill.up", label: "Start a test copy", tone: "primary" },
      { id: "skill.status", label: "Is it running?", quiet: true },
      { id: "skill.check", label: "Check its tools work", quiet: true },
      { id: "skill.message", label: "Send it a message" },
      { id: "skill.eval", label: "Score it" },
      { id: "skill.down", label: "Stop it", tone: "danger" },
    ],
  },
  {
    id: "tiers.local",
    route: "tiers",
    title: "Everything, on this computer",
    blurb:
      "The full setup running locally, so an agent you put here behaves the way it will when other people use it.",
    where: "on the Tiers view, second panel",
    needs: "docker",
    actions: [
      { id: "local.up", label: "Start it here", tone: "primary" },
      { id: "local.status", label: "What is running", quiet: true },
      { id: "local.rebuild", label: "Restart one piece", hint: "Rebuilds and restarts a single service after a code change" },
      { id: "local.comms", label: "Connect Slack", hint: "Connect or disconnect a real Slack workspace" },
      { id: "local.console.login", label: "Sign in to the console", hint: "Mints a single-use code for one person, to paste into the console; the console never gets the platform key" },
      { id: "local.observability.runs", label: "Recent activity", quiet: true },
      { id: "local.observability.run", label: "Read one conversation", quiet: true },
      { id: "local.observability.metrics", label: "Usage", quiet: true },
      { id: "local.message", label: "Send it a message" },
      { id: "local.eval", label: "Score an agent" },
      { id: "local.down", label: "Shut it all down", tone: "danger" },
    ],
  },
  {
    id: "tiers.cluster",
    route: "tiers",
    title: "Shared with your team",
    blurb: "A real server, so the agent keeps running and anyone who needs it can reach it.",
    where: "on the Tiers view, third panel",
    needs: "kubectl",
    actions: [
      { id: "cluster.up", label: "Set up the server", tone: "primary" },
      { id: "cluster.status", label: "Is the server healthy?", quiet: true },
      { id: "cluster.comms", label: "Connect Slack" },
      { id: "cluster.console.login", label: "Sign in to the console", hint: "Mints a single-use code for one person, to paste into the console; the console never gets the platform key" },
      { id: "cluster.github-app", label: "Give it a GitHub identity", hint: "Give the platform its own GitHub App" },
      { id: "cluster.observability.runs", label: "Recent activity", quiet: true },
      { id: "cluster.observability.run", label: "Read one conversation", quiet: true },
      { id: "cluster.observability.metrics", label: "Usage", quiet: true },
      { id: "cluster.message", label: "Send it a message" },
      { id: "cluster.eval", label: "Score an agent" },
      { id: "cluster.migrate-store", label: "Move stored files", tone: "danger", hint: "Carries saved agents across an upgrade that changes where files are kept" },
      { id: "cluster.upgrade", label: "Move it to a new version", hint: "Picks up where it left off if an upgrade stops partway" },
      { id: "cluster.rollback", label: "Put the last good version back", hint: "Skips versions that never finished installing, so this lands on one that actually ran" },
      { id: "cluster.down", label: "Tear it down", tone: "danger" },
    ],
  },
  {
    id: "tiers.declarative",
    route: "tiers",
    title: "Set it up from a file",
    blurb:
      "Write down the setup you want and have it applied, instead of clicking through it every time.",
    where: "on the Tiers view, below the three rungs",
    needs: "kubectl",
    actions: [
      { id: "diff", label: "See what would change", tone: "primary", hint: "Show what apply would do to the live release" },
      { id: "apply", label: "Apply it", hint: "Changes the server to match what the file says" },
      { id: "seal", label: "Lock a secret to this server", hint: "Encrypts a value so only this server can read it" },
    ],
  },
  {
    id: "tiers.examples",
    route: "tiers",
    // "Worked example" is a textbook phrase, not a label. A section header is a
    // short noun naming what is in the box.
    title: "A worked example",
    blurb: "A complete, working setup you can read start to finish and copy from.",
    where: "at the foot of the Tiers view",
    actions: [
      { id: "example.sre-bot.install", label: "Install the example", hint: "Sets up Curie, its monitoring, and a working agent, all together" },
    ],
  },

  // --- Agents (a sheet, opened from an agent anywhere in the app) ---------
  {
    id: "agent.talk",
    route: "overview",
    title: "Agent · Talk to it",
    blurb: "Send it a message and read the reply, without going through Slack.",
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
    blurb: "Which model it uses, how hard it thinks, where it answers, and what it may spend.",
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
    blurb: "Emergency controls. Every one of these changes something that is live right now.",
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
    blurb: "Names only. A value is handed over once and never shown back to you, not even here.",
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
    title: "This computer",
    blurb: "Setting up, updating and checking the pieces this app needs to do anything.",
    where: "in Settings, under This machine",
    actions: [
      { id: "doctor", label: "Diagnose", tone: "primary", hint: "What is set up, what is missing, and the command that fixes it" },
      { id: "install", label: "Bootstrap a checkout", hint: "Install deps and build; starts nothing" },
      { id: "update", label: "Update Curie", hint: "Rebuilds and reinstalls Curie from the source on this computer" },
      { id: "build", label: "Rebuild the agent engine" },
      { id: "interactive", label: "Terminal interface", quiet: true, hint: "Curie's own text interface. It needs a real terminal, so it opens here as a reference only" },
    ],
  },
  {
    id: "settings.reference",
    route: "settings",
    title: "Things to read",
    blurb: "These two print something and change nothing.",
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
      "For people working on Curie itself. These need the source code and its build tools, so they do nothing on an ordinary install.",
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
      { id: "dev.e2e-ci-selection", label: "Which end-to-end tiers CI picks" },
      { id: "dev.sre-demo-e2e", label: "SRE demo end-to-end" },
      { id: "dev.plugin-compat", label: "Bundle compatibility" },
      { id: "dev.agent-skills", label: "Skills match the spec" },
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
    title: "Act on this one",
    blurb: "Things you can do to whatever is open in the inspector.",
    where: "in Resources, in the sheet a container row opens",
    actions: [
      { id: "skill.status", label: "Check session status" },
      { id: "skill.message", label: "Send a message" },
      { id: "skill.down", label: "Stop this test copy", tone: "danger" },
      { id: "local.status", label: "What is running here" },
      { id: "local.rebuild", label: "Restart this piece" },
      { id: "local.down", label: "Shut it all down", tone: "danger" },
      { id: "cluster.status", label: "Check the server" },
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

/**
 * What to call a command in a heading, in this app's own words.
 *
 * The generated form's sheet was titled `curie local deploy`, which is the one
 * place the whole de-unixified surface leaked: every button that opens it says
 * something like "Put it to work", and then the panel that opens announces a
 * command line. The placement label is what the operator pressed, so it is what
 * the heading should say. The command line has not gone anywhere -- the form
 * still shows it, copyable, above the Run button, which is where somebody who
 * wants it will look.
 *
 * Falls back to the command's own path for anything not placed on a surface;
 * that is a heading nobody should see, because `surfaces.test.ts` requires every
 * runnable command to be placed.
 */
export function commandTitle(commandId: string, fallbackPath: readonly string[]): string {
  return homeOf(commandId)?.action.label ?? `curie ${fallbackPath.join(" ")}`;
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
