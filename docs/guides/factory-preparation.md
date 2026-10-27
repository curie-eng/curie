# Prepare, publish and select a factory specification

Use `prepare-factory-ticket` in your local coding agent to review existing work
for factory execution. Start with an issue, specification, plan or discussion
produced by your existing workflow, including open source or internally developed
skills. The skill inspects repository context, resolves consequential gaps and
assesses whether the agreed implementation has a feasible verification route.
It preserves an existing sufficient specification and proposes targeted additions
for actual gaps, then presents the complete final issue for approval and handoff.
An unchanged published issue can be verified and selected without rewriting it.

Think of it as the readiness review and handoff step after local preparation.
It can also help you start from rough intent. For larger requests it can propose
child specifications, coverage and dependency ordering. The factory can accept
an issue prepared without this skill; using it is an optional guided workflow.
See [existing local workflows](../software-factory.md#bring-your-existing-local-workflow)
for examples from Matt Pocock, Superpowers and Addy Osmani, and how the same handoff
applies to your organization's internal skills.

Specifications remain **deployment-unvalidated**, even after publication and
selection. Developer review and local repository evidence do not prove that Curie
can execute a specification. Publication uses your coding agent's existing tracker
connection, and selection uses the deployment's configured issue label. Connected
validation and validated handoff remain tracked in #4488 and #4490.

## Install, prepare and hand off

For guided preparation, open the repository the factory will change and install
the skill for your coding agent. With a CLI build containing this command:

```bash
curie skills install --agent codex
```

Bring the existing issue link or make the specification, plan or discussion
available in the local session. For example:

```text
Use prepare-factory-ticket to review this specification for factory execution.
Keep the decisions we have already made and identify any remaining gaps.
```

Existing settled decisions do not need another interview. New drafts use the
seven-section template when there is no established source format; existing
issues do not need those headings to pass the substantive review.

The review considers scope, unresolved behavior, repository prerequisites and
per-criterion verification. Missing coverage becomes tests to add during
implementation. Unavailable external qualification is made visible for explicit
scope approval. This review is advisory: it is not a connected deployment check
or a guarantee of successful execution.

This also installs the companion `review-pr` skill; no second install is needed.

In Codex, start a new session if the skill is not visible, then use this minimal
prompt with the actual issue URL:

```text
$prepare-factory-ticket <issue-url>
```

For new work, replace the URL with your intent, for example:

```text
$prepare-factory-ticket Add CSV export for the counter history.
```

Answer questions about unresolved behavior and review the complete specification.
When it matches your intent, choose **publish and start the factory**. The skill
uses your coding agent's authenticated tracker tools to create or update the
reviewed issue, verifies it, and adds the confirmed configured factory label to
eligible work. You can instead publish without starting or keep the draft local.
The [client table](#get-the-skill) below covers the other coding agents.

Installing the skill does not install or configure the factory. You can draft
before a deployment exists; starting implementation requires a configured factory
watching that repository and an account permitted to start work. Follow the
[factory quickstart](../software-factory.md#what-you-are-trying) if it is not set up.
Your coding agent's tracker connection is separate from the factory's connection.
If the agent cannot write issues, publish the reviewed body yourself, then give
it the actual issue link for verification and any separately approved selection.

## Get the skill

Use a Curie CLI built from this change, or a release that contains it. From the
repository you want to prepare work for, choose your coding agent:

```bash
curie skills install --agent codex
# Or select claude-code, copilot, gemini, or cursor instead.
```

The single command installs `prepare-factory-ticket`, `review-pr`,
`factory-environment-setup`, and `factory-environment-maintenance`,
including their Markdown references, into the selected client's directory. It is offline and requires no source checkout,
Python, parser package, Docker or running Curie service. The default destination
is the nearest ancestor repository root, including Git worktrees; outside a
repository it is the current directory. Use `--dir /path/to/project` to select
exactly an existing directory, or `--global` to install for your user across
repositories. `--dir` and `--global` cannot be combined.

```bash
curie skills install --agent claude-code --global
curie skills install --agent cursor --dir /path/to/project --dry-run
```

Repeating an installation keeps an identical copy. An edited or incomplete copy
is refused before writing any skill files. Review and move the existing folder
aside before installing a different version. Symlinked client directories and
skill files are refused. `--dry-run` checks the destination without creating it;
`--json` returns one result object. Installation reports how to invoke or refresh
the skill; it does not change agent settings, workspace trust or consent.

| Local client | Repository skill destination | Invocation |
| --- | --- | --- |
| Codex | `.agents/skills/prepare-factory-ticket/` | `$prepare-factory-ticket` followed by your intent or ticket content |
| Claude Code | `.claude/skills/prepare-factory-ticket/` | `/prepare-factory-ticket` followed by your intent or ticket content |
| GitHub Copilot in a client supporting agent skills | `.github/skills/prepare-factory-ticket/` | Ask Copilot to use `prepare-factory-ticket` for your intent or ticket content |
| Gemini CLI | `.gemini/skills/prepare-factory-ticket/` | Run `/skills reload`, then ask Gemini to use `prepare-factory-ticket` |
| Cursor Agent | `.cursor/skills/prepare-factory-ticket/` | Ask Cursor to use `prepare-factory-ticket` for your intent or ticket content |

Global installation uses the same directories under your home directory, except
Copilot uses `~/.copilot/skills/`. Choose one client installation per workspace
unless you need several: clients may also discover other clients' compatibility
directories, so multiple copies can expose the same skill more than once.

These paths follow the providers' published discovery conventions:
[Codex](https://learn.chatgpt.com/docs/build-skills),
[Claude Code](https://code.claude.com/docs/en/skills), and
[GitHub Copilot](https://docs.github.com/en/copilot/how-tos/copilot-on-github/customize-copilot/customize-cloud-agent/add-skills),
[Gemini CLI](https://geminicli.com/docs/cli/using-agent-skills/), and
[Cursor](https://cursor.com/docs/skills).
Client versions and workspace settings determine skill availability. Use the
client's normal refresh or restart procedure if a newly installed skill is absent.
Copilot CLI supports `/skills reload`; Gemini CLI also requires activation consent.
Local installation does not automatically sync personal skills to cloud agents.
Installer tests verify file layouts and behavior; they do not certify discovery
or invocation in every native client or operating-system version.

For manual installation, the same self-contained source folder is
`examples/dark-factory/preparation/skills/prepare-factory-ticket/` in this repo.
The full factory renderer also includes it at
`dark-factory/preparation/skills/prepare-factory-ticket/` after
`curie example dark-factory render --out ./dark-factory`; that separate renderer
may contact the release registry to resolve the factory runner image.

Install only the preparation skill folder. Do not register the entire factory
execution plugin for preparation: its implementation hooks gate edits and
publication. The preparation folder contains its own references and carries no
execution hooks or plugin manifest.

If your agent cannot discover skills, open the copied `SKILL.md` and ask it to
follow that file and its linked reference. Keep the folder intact. The fallback
uses the same readiness review, source-preservation and new-draft rules without
requiring a parser.
Do not overwrite an existing local skill or draft without reviewing the change.

## Install from a source checkout

Check whether your CLI contains the installer:

```bash
curie skills install --help
```

An older released CLI will report an unknown command. Until a release contains
this feature, use a checkout containing the preparation change and a Rust/Cargo
toolchain to build and run just the installer. From any directory, replace the
two absolute paths with your Curie checkout and the repository you want to work on:

```bash
cargo run --locked --manifest-path /path/to/curie/cli/Cargo.toml -- \
  skills install --agent codex --dir /path/to/your/repository
```

The target directory must already exist. Use the client names from the table
above, or replace `--dir ...` with `--global` for personal installation. This
builds the CLI and copies the skill; it does not start containers, deploy a
factory or install Python on the client. Factory setup still follows its own
quickstart and released runner requirements. Once a released CLI contains the
installer, no Curie source checkout or Rust toolchain is needed to install it.

## Prepare and review

Work from the repository the factory will change. Give the agent either the
existing ticket text and identity or the new behavior you want. If tracker access
is unavailable, supply the existing ticket content locally; the skill must not
pretend to have read it remotely.

For example:

> Use prepare-factory-ticket to prepare this existing issue for the factory.
> Preserve its identity and my edits. Inspect this checkout, ask me about missing
> behavior, and save a local draft and preparation report. Do not implement it.

Or:

> Use prepare-factory-ticket to turn this intent into a specification: export the
> counter history as a CSV file. Inspect the repository and ask about any missing
> decisions. Propose a split only if the deliverables justify it.

The agent inspects guidance, code and verification routes, then asks about product
choices the repository cannot answer. Review the proposed behavior, scope and any
decomposition. A prerequisite still needing implementation remains a blocker;
missing deployment capability remains an operator gap. Neither is silently removed
by splitting a request.

The number and depth of questions follow the remaining ambiguity and the impact
of different answers, with no fixed quota or cap. Related questions are grouped
into manageable rounds, using concrete behavioral examples where useful. A clear
request can go straight to a draft. Further questions follow investigation and
revised answers when a material choice remains. Clarification ends when material
choices are resolved and minor assumptions are explicit for review. Material
answers become decisions or acceptance criteria.
The local report counts questions and batches separately from review/publication
choices, with preparation duration marked unknown unless observed or supplied.

Choose a local output location when needed. Sufficient existing issues retain
their structure; new drafts without an established format use the seven
[template](factory-specification.md) sections. A separate preparation
report records original identity, inspected/base revisions, unresolved questions,
criterion traceability, observed evidence and developer review status. Raw
conversations and credentials do not belong in the report.

Before final review, the skill checks whether each acceptance criterion has a
verification route the repository's factory environment supports. It inspects
sandbox setup, test tooling and CI configuration, recording the observable result,
command or proposed test, required capabilities and evidence. Each route is
classified as sandbox-verifiable, CI-verifiable, needs external verification or
unknown. A command passing on your computer does not prove sandbox support, and
a CI workflow alone does not prove the factory can use or observe its checks.

Acceptance criteria describe behavior that existing tests or tests added during
implementation can verify. Missing coverage becomes an explicit requirement to add
meaningful regression/integration tests in the PR, with proposed assertions, runner
and fixtures. The tests need not exist or pass before factory selection.

Unavailable live-service checks or human qualification are flagged in the published
Out of scope section, with what remains unproved and a proposed owner. You review
that boundary alongside the full specification; the original outcome is not silently
replaced by a mock. Once you approve the testable implementation scope, those
external qualifications do not block tagging. A missing capability that prevents
meaningful testing of the agreed implementation still blocks selection, as does an
unsupported live check you retain as mandatory acceptance. Drafts and approved
publish-only remain available. Deployment readiness stays unvalidated.

A split retains the original requirements and identifies which child criterion
covers each one, who owns integration proof, dependency ordering and shared-file
constraints. A dependent child can be complete while still blocked. Save a
readable coverage and dependency map alongside the child drafts; no hashes,
JSON envelope or new client runtime are required.

## Publish and start

Once the questions are resolved, the skill presents the complete specification,
not just its evidence report. Review each full title/body and destination. For a
split, review all children, coverage and dependency ordering together. Corrections
return to review before any publication.

After you approve the spec, choose **publish only**, **publish and start the
factory**, or **keep local drafts**. For example:

> Publish this reviewed specification to the existing issue and start the factory
> using the confirmed configured label.

The agent asks for this choice with the actual repository, issue identities and
factory label. A combined publish-and-start approval covers both operations;
there is no second confirmation between them when the reviewed plan is unchanged.
Spec approval alone does not authorize publication. Publish-only never selects
work. The usual label is `curie-factory`, but the skill confirms your deployment's
actual label and intended repository instead of assuming the default.

With an existing authenticated tracker connection, the agent updates changed
issues or creates agreed new issues, then reads back their full content. For a
sufficient unchanged published issue, it skips the body write and offers to verify
and start that existing issue. Existing selection or active work is inspected
before any change; a source update waits for supported coordination and an
observed safe state, rather than treating a label removal as completed cancellation. It
keeps `report.md` local and returns real tracker links. If you publish manually,
give it the links so it can verify and reuse those issues. Missing tracker access
leaves an actionable gap rather than a claim that local files were published.

Before selection, the agent rechecks the approved content, open state and actual
prerequisites. It adds the configured label only to the explicitly selected,
eligible issues. Split children remain unselected until their prerequisites merge
into their base; the parent epic is not automatically selected. Existing labels
are preserved, and an already selected issue is not relabelled to restart it.
Conflicting remote edits require another review. Partial or timed-out operations
are reconciled against actual tracker state before retrying to avoid duplicates.

The result distinguishes publication, observed selection and confirmed factory
status. A verified label means selected for intake; queued or running requires an
observed factory status. This workflow does not configure a deployment or establish
execution readiness. The report stays `unvalidated`, and no parser or deployment
evaluator result is invented.

## Scope of the first release

The local authoring skill can ship before migration of existing factory prompts,
CI repair policy and labels. The release contains the portable instructions,
offline installer and optional impact pilot. Format, coverage and dependency
review is performed by the coding agent and developer; no Python parser or
runtime admission validator is included.
[ADR 0222](../adr/0222-coworker-bundles-own-workflow-policy-and-curie-enforces-execution.md)
sets the ownership boundary for future factory policy; #4589 tracks isolated
evaluation support.

## Manual upgrades

The installer copies the skill bundled with the selected Curie CLI build. It
accepts an identical existing copy and refuses a different, edited or incomplete
copy before writing. This release has no automatic update check, managed updater,
rollback command, release cache or repository version resolver.

To replace an installed skill, review the new instructions, preserve the old skill
folder outside every directory your client discovers, then rerun the install
command using the chosen newer CLI build. Keep the original until the replacement
works. Reapply any local customizations manually after checking that they still
fit the new workflow; installation does not merge or discard them.

For example, after backing up and moving the existing skill folder:

```bash
curie skills install --agent codex
```

Use the same `--global` or `--dir` scope as the installation being replaced. Keep
existing drafts and evidence intact. If instructions change an ongoing draft,
review its complete updated specification before publication.

Managed updates and repository specific instruction versions are follow-up work.
This workflow does not interpret `.curie-factory.json` or promise compatibility
with an installed factory version. Follow the current repository's guidance and
resolve any conflicting format requirement during review. Reviewed publication
uses existing tracker tools and the confirmed configured factory label.

## Measure whether preparation helps

Use the [preparation pilot protocol](../../examples/dark-factory/preparation/pilot/README.md)
and its blank ledger to compare ordinary intake with prepared intake. Enroll
original requests before preparing them, keep the factory setup comparable, and
measure accepted outcomes and postsubmission interventions at the same cutoff.
Include preparation time as well as supervision, review and rework so that moving
questions earlier is not mistaken for eliminating human effort.

The pilot uses a private local ledger and existing
[outcome and usage evidence](measuring-factory-outcomes.md). It installs no
analytics service and uploads nothing. Participation is optional and does not
block publication or factory selection. A small exploratory trial can guide the
next change; it does not establish a universal productivity improvement.
