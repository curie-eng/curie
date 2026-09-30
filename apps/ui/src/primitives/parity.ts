// Console/CLI parity registry (epic #145).
//
// The single source of truth for "which wired console action maps to which
// `curie` command". Every action a wired surface exposes is listed here with
// EITHER a manifest-derived `command` (an `ActionId`, so a renamed/removed CLI
// verb breaks `pnpm typecheck` at this call site) OR an explicit
// `noCliEquivalent` marker carrying the tracking issue for the gap.
//
// Direct `cliCommand` calls are typed against the manifest. The parity test
// recursively inventories those calls across the complete production `src`
// tree, excluding tests, and verifies each literal command has a registry
// mapping. It also checks that typed `noCliEquivalent` action IDs resolve to
// entries linked to `PARITY_TRACKING_ISSUE`. Registry enumeration separately
// verifies every entry is a real command or an explicit gap.

import type { ActionId } from "./cliCommand";

// The parity epic tracks every still-unmapped wired action; a `noCliEquivalent`
// entry links here until a dedicated verb lands.
export const PARITY_TRACKING_ISSUE = "https://github.com/curie-eng/curie/issues/145";

export type CliMapping =
  | { readonly command: ActionId }
  | { readonly noCliEquivalent: string };

export interface WiredAction {
  /** Stable id for the wired action (surface-agnostic). */
  readonly id: string;
  /** Human-readable description of the console action. */
  readonly label: string;
  /** Its CLI mapping: a real command, or an explicit no-equivalent marker. */
  readonly mapping: CliMapping;
}

// The wired actions, grouped by surface in comments. `deploy`/`status`/`message`
// are env-scoped in the UI (prod -> cluster, dev -> local); both tiers are
// listed so the parity gate covers each concrete command the surface can emit.
export const WIRED_ACTIONS = [
  // WiredAgents / NewAgentModal
  { id: "scaffold-agent", label: "New agent / scaffold", mapping: { command: "init" } },

  // WiredAgentDetail — Deploy new version (env-scoped)
  { id: "deploy-cluster", label: "Deploy new version (prod)", mapping: { command: "cluster.deploy" } },
  { id: "deploy-local", label: "Deploy new version (dev)", mapping: { command: "local.deploy" } },

  // WiredOverview / WiredVersions — status (env-scoped)
  { id: "status-cluster", label: "Overview / versions status (prod)", mapping: { command: "cluster.status" } },
  { id: "status-local", label: "Overview / versions status (dev)", mapping: { command: "local.status" } },

  // Lifecycle controls — real verbs since #149 landed kill/resume/budget/delete.
  { id: "kill", label: "Kill a run", mapping: { command: "cluster.kill" } },
  { id: "resume", label: "Resume a run", mapping: { command: "cluster.resume" } },
  { id: "budget", label: "Set budget", mapping: { command: "cluster.budget" } },
  { id: "delete", label: "Delete an agent", mapping: { command: "cluster.delete" } },

  // WiredThreadReset (#871) — force a thread's sandbox to be released. Env-scoped
  // in the UI (prod -> cluster, dev -> local), mirroring deploy; both tiers are
  // listed so the parity gate covers each concrete command the surface emits.
  { id: "reset-thread-cluster", label: "Reset a thread (prod)", mapping: { command: "cluster.reset-thread" } },
  { id: "reset-thread-local", label: "Reset a thread (dev)", mapping: { command: "local.reset-thread" } },

  // RealTraces / RealMetrics (#866) — API-proxied observability reads. Each
  // view selects the tier from the active console environment and resolves the
  // exact query grammar from the generated command manifest.
  { id: "observability-runs-cluster", label: "List runs (prod)", mapping: { command: "cluster.observability.runs" } },
  { id: "observability-runs-local", label: "List runs (dev)", mapping: { command: "local.observability.runs" } },
  { id: "observability-run-cluster", label: "View a run (prod)", mapping: { command: "cluster.observability.run" } },
  { id: "observability-run-local", label: "View a run (dev)", mapping: { command: "local.observability.run" } },
  { id: "observability-metrics-cluster", label: "Query metrics (prod)", mapping: { command: "cluster.observability.metrics" } },
  { id: "observability-metrics-local", label: "Query metrics (dev)", mapping: { command: "local.observability.metrics" } },

  // Genuinely-unmapped actions: no dedicated CLI verb exists yet. These render
  // the honest amber glyph linking to the parity epic instead of a command.
  { id: "rollback", label: "Roll back to an earlier version", mapping: { noCliEquivalent: PARITY_TRACKING_ISSUE } },
  { id: "promote-to-prod", label: "Promote dev version to prod", mapping: { noCliEquivalent: PARITY_TRACKING_ISSUE } },

  // WiredAgentMemory (#267) — inspect/edit/delete learned memory. No dedicated
  // CLI verb exists yet, so both render the honest amber gap glyph.
  { id: "memory-edit", label: "Edit a learned memory entry", mapping: { noCliEquivalent: PARITY_TRACKING_ISSUE } },
  { id: "memory-delete", label: "Delete a learned memory entry", mapping: { noCliEquivalent: PARITY_TRACKING_ISSUE } },

  // WiredEvals (#868) — the eval matrix is read from GET /evals/matrix. There is
  // no top-level `curie` verb that just reads the matrix (the CLI polls it
  // internally during a model sweep), so the view renders the honest amber gap.
  { id: "eval-matrix", label: "View the eval matrix", mapping: { noCliEquivalent: PARITY_TRACKING_ISSUE } },

  // WiredWorkItems (#2577) — factory outcomes console, env-scoped like status.
  { id: "work-items-cluster", label: "List factory work items (prod)", mapping: { command: "cluster.work-items" } },
  { id: "work-items-local", label: "List factory work items (dev)", mapping: { command: "local.work-items" } },

  // WiredAgentBehaviorPacks (#870) — view/edit the agent's opt-in behavior packs.
  // No CLI path reads or writes behavior_packs (see cli/api-mirrors.json), so the
  // save action renders the honest amber gap glyph linking to the parity epic.
  { id: "behavior-packs-edit", label: "Edit an agent's behavior packs", mapping: { noCliEquivalent: PARITY_TRACKING_ISSUE } },
] as const satisfies readonly WiredAction[];

export type WiredActionId = (typeof WIRED_ACTIONS)[number]["id"];
