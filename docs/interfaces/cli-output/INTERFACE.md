---
seam: CLI output (agent-facing `--json`)
kind: CLEAN
impls: 67 outputs behind one trait
grade: not separately graded
epics:
  - "#456"
order: 18
---

# INTERFACE: CLI output (agent-facing `--json`)

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN &nbsp;·&nbsp; **Implementations today:** 67 outputs behind one trait &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

The port is the `CliOutput` trait in `cli/src/ui.rs`. Two methods:

```rust
pub trait CliOutput {
    /// The single JSON object emitted under `--json`.
    fn to_json(&self) -> serde_json::Value;
    /// The human render (stdout payload lines) when not under `--json`.
    fn render(&self, ui: &Ui);
}
```

The swappable thing is **the rendering of a command's result**, not the command.
A verb computes a result value and hands it to `emit`; the machine-vs-human
decision is made in exactly one place (`Ui::emit`, `cli/src/ui.rs`) rather than at
every call site. This is the code-level enforcement of ADR-0021's first decision:
the CLI's primary user is a coding agent, so every verb must have a parseable
`--json` form and no verb may silently emit empty stdout under `--json`.

This is the catalog's first **Rust** seam. It is listed here because the agent-facing
`--json` contract is a public surface an agent branches on, the same way the ACI is.

## Current contract

- **One decision point.** `Ui::emit(&dyn CliOutput)` (`cli/src/ui.rs`) is the only
  success-path branch: under `--json` it writes `to_json()` as one compact line via
  `emit_json`; otherwise it calls `render(self)`. Handlers must not call a stdout
  emitter directly, apart from the pinned sites listed under `raw_emit_sites` in
  `cli/schema/index.json`. `main`'s `emit<T: CliOutput>` helper (`cli/src/main.rs`) is the
  dispatch-side funnel that routes every read verb's return value through it.
- **One JSON object per invocation.** `to_json` returns a single
  `serde_json::Value`, emitted as one line. A multi-line or streamed stdout payload
  is outside this contract.
- **The error path is the mirror, not part of this trait.** Failures are emitted
  centrally by `main` and classified by `cli/src/exit.rs` into four stable exit codes
  agents branch on: `0` Success, `1` Failure, `2` Usage (deterministic input error;
  same argv fails identically), `3` Transient (retryable; dependency unreachable or
  timed out). `CliOutput` covers only exit-0 stdout.
- **Human and JSON render the same value.** Both methods read the same owned data,
  so the two paths cannot disagree about content — only about form. `VersionsOutput`
  (`cli/src/commands.rs`) documents this obligation explicitly: it holds versions
  newest-first, normalized once by the handler, because `to_json` and `render` each
  iterate it plainly and a constructor that broke the order would let the two paths
  silently diverge.

## Implementations today

67 `CliOutput` implementations, all in the CLI crate, grouped by owning module:

- **`DryRunPlan`** (`cli/src/ui.rs`) — the generic `--dry-run` plan; JSON is
  `{"dry_run":true,"plan":[lines]}` and the human render is the same lines verbatim,
  so operator dry-run output stayed byte-identical when the seam landed. Lines come
  from `OpsCommand::display()` (already credential-masked), so this type never
  re-derives argv or reads a raw secret. It is also **composed** rather than
  duplicated: the outputs whose verbs take `--dry-run` are enums carrying a `DryRun`
  variant that delegates to it instead of re-rendering the plan.
- **`cli/src/commands.rs`**, the largest group, covering the skill and agent verbs
  and the shared lifecycle results: `InitOutput`, `CheckOutput`, `ChartCheckOutput`,
  `ListAgentsOutput`, `BumpVersionOutput`, `StatusOutput`, `SkillMessageOutput`,
  `EvalOutput`, `DeployOutput`, `AllTargetsDeployOutput`, `KillOutput`, `ResumeOutput`,
  `BudgetOutput`, `ResetThreadOutput`,
  `DeleteOutput`, `VersionsOutput`, `MemoryOutput`, `MemoryGuidanceOutput`, `HookOutput`, `ApprovalsOutput`,
  `SkillApprovalsOutput`, `OverridesOutput`, `PublicationPolicyOutput`, `WorkItemsOutput`,
  `SchedulesOutput`, `HookFireOutput`, `ChannelsOutput`, `CallersOutput`,
  `ConnectorBuildOutput`. The last is
  the `curie build --plugin-dir` receipt for connector source builds (ADR-0113):
  it emits one object even when the bundle declares nothing to build, because
  under `--json` an agent cannot tell "nothing to build" from "the command
  produced nothing" — the #485 failure this seam exists to prevent.
- **`cli/src/local.rs`**, **`cli/src/ops/up.rs`**, **`cli/src/ops/upgrade.rs`**, and **`cli/src/ops/verbs.rs`**, the operator verbs, one output per
  verb per tier: `LocalUpOutput`, `LocalRebuildOutput`, `LocalStatusOutput`,
  `LocalDownOutput`; `ClusterUpOutput`, `ClusterUpgradeOutput`, `ClusterStatusOutput`, `ClusterDownOutput`,
  `ClusterRollbackOutput`; `LintValuesOutput` in `cli/src/ops/lint_values.rs`.
- **`cli/src/message.rs`**: `MessageDryRunOutput` and `MessageOutcomeOutput`, the
  multi-variant outcome whose covered variant set the enum-variant walk derives (see
  Known leakage).
- **`cli/src/examples.rs`**: `ObservabilityProvisionOutput`, the receipt that Secret `curie-grafana-connector` holds a token (never the token value); `ObservabilityOnlyOutput`, which reports the stack namespace without claiming the Curie release changed; `SreBotRenderOutput`, which reports the new bundle directory; and `DarkFactoryRenderOutput`, which reports the rendered dark-factory bundle directory, the published runner layer `runner_image` its lock records (or `null`), and a `runner_note` naming the `curie build` to run when no layer is published for the CLI's version.
- **`cli/src/installation.rs`**: `ApplyOutput`, `DiffOutput`.
- **`cli/src/observability.rs`**: `ObservabilityOutput`, `ObservabilityRunsOutput`,
  `ObservabilityRunOutput`, `ObservabilityMetricsOutput` — the tier-aware
  observability surfaces (#460). Notable as the shape the seam is for: both the local
  and cluster tiers resolve their own `Endpoint` values and return *the same* output
  type, so tier parity is structural rather than two hand-aligned printers. That
  module is a deliberate leaf and never bypasses `CliOutput`.
- **Other module outputs** in `cli/src/channel_token.rs` (`ChannelTokenOutput`),
  `cli/src/comms.rs` (`CommsOutput`), `cli/src/doctor.rs`
  (`DoctorOutput`), `cli/src/factory_intake.rs` (`FactoryIntakeOutput`), `cli/src/factory_app.rs` (`FactoryAppRegistrationOutput`, `FactoryAppSetupOutput`), `cli/src/factory_quickstart.rs` (`QuickstartOutput`), `cli/src/github_app.rs` (`GithubAppOutput`), `cli/src/guide.rs`
  (`GuideOutput`), `cli/src/migrate_store.rs` (`MigrateStoreOutput`),
  `cli/src/openrouter_credit.rs` (`ModelCreditOutput`),
  `cli/src/release_accept.rs` (`ReleaseAcceptOutput`), `cli/src/seal.rs`
  (`SealOutput`), and `cli/src/secrets.rs` (`SecretsListOutput`).

That set is not hand-maintained prose: `cli/schema/index.json` carries one
`CliOutput` entry per implementation, and the `syn` walk in
`cli/tests/support/schema_inventory.rs` fails the gate
(`cli/tests/schema_inventory.rs`) from either side, an impl with no entry
(`UndeclaredResult`) or an entry with no impl (`ResultNotFound`).

## Known leakage

- **The trait is not the whole `--json` surface, but a new raw emitter now fails
  the build (since #841).** `CliOutput` governs the success path of the verbs that
  were converted. `schema_inventory.rs` pins the per-file `.emit_json(` call-site
  count and raises `UnexpectedRawEmitter` when a new direct emitter appears, so a
  handler that bypasses the seam and prints to stdout directly breaks CI rather
  than sliding through on convention alone. Three accepted raw sites are pinned:
  `Ui::emit` itself in `cli/src/ui.rs`, the error path in `main` in `cli/src/main.rs`,
  and `report_sweep` in `cli/src/commands.rs`, which emits the sweep payload through
  `Ui::emit_json` directly. The residual is that this is a
  syntactic call-site inventory, not a type-level proof that *every* verb returns
  a `CliOutput`.
- **Committed JSON Schemas with a drift gate (since #841).** Each `to_json` is no
  longer schema-free: there are 64 committed schemas under `cli/schema/` with an
  index (`cli/schema/index.json`), a `syn`-based inventory gate over every `impl
  CliOutput`, and per-family output validation — result families are validated
  against real `to_json()` output across 82 tests in `cli/tests/json_contract.rs`.
  The dark factory render receipt uses schema version 2. Its `runner_image`
  and `runner_note` fields are required and accept a string or `null`; the
  binary render tests validate the actual receipt against the committed schema.
  Every `CliOutput` enum variant is also validated against its mapped schema by
  `cli/tests/cli_output_variants.rs`. Its sample registry is checked against a
  `syn` walk (`cli/tests/support/enum_variants.rs`) that derives enum and variant
  names from their declarations, so an added variant without a sample turns the
  gate red rather than passing vacuously (#965). Five tests exercise
  the walk's own rejection paths -- a source that fails to parse, a second
  declaration of the same enum, a `#[cfg]`-gated variant, and an enum inside a
  `#[cfg]`-gated module -- plus a positive control, so those rejections are now
  proven by execution rather than asserted. An agent
  parsing this output is now coupled to shapes enforced by committed schemas and a
  drift gate, like the ACI (`packages/aci-protocol`, ADR-0017), not by tests alone.
- **Not separately graded.** This is not one of the six swap-readiness Jobs in the
  vision doc: the "second implementation" here would be a second *output format*
  (YAML, a table protocol), which nobody has asked for. Per the governing restraint,
  the port is documented where the code already draws the line and no speculative
  formatter layer is added ahead of a real demand.

## Cross-links

- **Issue:** [#456](https://github.com/curie-eng/curie/issues/456) — the `--json` contract broke per-command; `Ui::emit` + `CliOutput` + `DryRunPlan` are its fix
- **Issue:** [#460](https://github.com/curie-eng/curie/issues/460) — the observability twin, whose local/cluster tiers share one `CliOutput`
- **Issue:** [#841](https://github.com/curie-eng/curie/issues/841) — added the committed `cli/schema/` JSON Schemas, the `schema_inventory` build gate, and the `json_contract` output validation this seam now relies on
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — CLI output is not one of the six swap-readiness Jobs; not separately graded
- **ADR(s):** [ADR-0021](../../adr/0021-curie-is-a-harness-for-coding-agents.md) — Curie is a harness for coding agents: the CLI's primary user is Claude Code (this seam is decision 1's enforcement); [ADR-0038](../../adr/0038-observability-cli-helper-for-the-agent-dev-loop.md) — the observability CLI is a thin client over the API proxy, not a second backend
