# Factory preparation resources

The `skills/prepare-factory-ticket` folder contains the text-only preparation
workflow for a developer's existing coding agent. Install it directly with
`curie factory prepare install --agent codex` (or `claude-code`, `copilot`,
`gemini`, `cursor`). Add `--global` for personal installation across projects.
The installer copies only that skill folder and its references, offline.
It requires no Python interpreter,
parser installation, container runtime or reachable Curie deployment.

Preparation uses the host coding agent to review the specification format,
coverage and dependencies. This release contains no Python parser, deterministic
specification validator or ticket-set envelope. The canonical format is documented
in the packaged skill reference and the [specification guide](../../../docs/guides/factory-specification.md).

The skill presents complete drafts for review, then offers publication only,
publication with factory selection, or keeping local drafts. Approved publication
uses the host agent's existing authenticated tracker tools; the skill verifies
published content before applying the confirmed factory label to eligible issues.
Before review it assesses every criterion's verification route against repository
sandbox and factory evidence. Missing coverage becomes tests to add in the PR.
External qualification is flagged in the published issue and does not block selection
after approval of the testable implementation scope. Only gaps preventing meaningful
verification of that scope remain blockers; publish-only stays available. Host-only test
success and the existence of a CI workflow do not establish factory support.
No tracker connection is needed for drafting or installation. Readiness remains
unvalidated. Connected validation and validated handoff remain #4488 and #4490.
See the factory bundle README for rendering and provider-specific installation.

Publication behavior replay scenarios live in [evals/README.md](evals/README.md).
They use synthetic tracker state to exercise approval, publication, selection and
recovery without writing real issues. These are opt-in host-agent evaluations,
not automated CI tests or proof of deployed factory intake.

Installation uses the Markdown bundled with the chosen CLI build. It refuses to
overwrite a different or edited copy. Upgrades are manual: preserve the old folder
outside the client's skill discovery paths, install the reviewed replacement and
reapply customizations deliberately. Automatic update checks, managed upgrades,
rollback and repository version resolution are deferred to follow-up work. See
the [preparation guide](../../../docs/guides/factory-preparation.md#manual-upgrades).

The optional [pilot protocol](pilot/README.md) and blank [ledger](pilot/outcomes.csv)
compare usual intake with prepared intake. They measure accepted outcomes,
postsubmission interventions and total human effort using existing factory
observations, without a new runtime service.
