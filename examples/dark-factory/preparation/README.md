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
No tracker connection is needed for drafting or installation. Readiness remains
unvalidated. Connected validation and validated handoff remain #4488 and #4490.
See the factory bundle README for rendering and provider-specific installation.

Publication behavior replay scenarios live in [evals/README.md](evals/README.md).
They use synthetic tracker state to exercise approval, publication, selection and
recovery without writing real issues. These are opt-in host-agent evaluations,
not automated CI tests or proof of deployed factory intake.

Managed installation supports update checks, unchanged-file upgrades and rollback.
Optional repository pins in `.curie-factory.json` select preparation instructions
and output format independently for each checkout. They do not bind a deployment
or gate existing label intake. Without a pin or CLI, the installed text workflow
can still prepare, review, publish and select work. Local preferences remain
outside managed assets. The factory retains ownership of specification semantics.
See the [preparation guide](../../../docs/guides/factory-preparation.md#versions-and-upgrades)
for version and customization details.

The optional [pilot protocol](pilot/README.md) and blank [ledger](pilot/outcomes.csv)
compare usual intake with prepared intake. They measure accepted outcomes,
postsubmission interventions and total human effort using existing factory
observations, without a new runtime service.
