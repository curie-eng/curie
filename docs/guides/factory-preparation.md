# Prepare, publish and select a factory specification

Use the factory's `prepare-factory-ticket` skill in your existing coding agent to
turn an existing ticket or new intent into a reviewed specification. For larger
requests it can propose complete child specifications, coverage and dependency
ordering. The skill writes local artifacts, presents the full specification for
review, then offers to publish it and select eligible issues for the factory.

Specifications remain **deployment-unvalidated**, even after publication and
selection. Developer review and local repository evidence do not prove that Curie
can execute a specification. Publication uses your coding agent's existing tracker
connection, and selection uses the deployment's configured issue label. Connected
validation and validated handoff remain tracked in #4488 and #4490.

## Get the skill

Use a Curie CLI built from this change, or a release that contains it. From the
repository you want to prepare work for, choose your coding agent:

```bash
curie factory prepare install --agent codex
# Or select claude-code, copilot, gemini, or cursor instead.
```

Installation copies the bundled Markdown skill and its reference directly into
the selected client's directory. It is offline and requires no source checkout,
Python, parser package, Docker or running Curie service. The default destination
is the nearest ancestor repository root, including Git worktrees; outside a
repository it is the current directory. Use `--dir /path/to/project` to select
exactly an existing directory, or `--global` to install for your user across
repositories. `--dir` and `--global` cannot be combined.

```bash
curie factory prepare install --agent claude-code --global
curie factory prepare install --agent cursor --dir /path/to/project --dry-run
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
uses the same instructions and canonical format rather than a different parser.
Do not overwrite an existing local skill or draft without reviewing the change.

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

Choose a local output location when needed. The canonical body has the seven
[factory contract](factory-specification.md) sections. A separate preparation
report records original identity, inspected/base revisions, unresolved questions,
criterion traceability, observed evidence and developer review status. Raw
conversations and credentials do not belong in the report.

A split retains the original requirements and identifies which child criterion
covers each one, who owns integration proof, dependency ordering and shared-file
constraints. A dependent child can be complete while still blocked. If exact
fingerprints cannot be computed with existing tooling, the agent saves an
incomplete mapping and names the gap rather than inventing a valid envelope or
requiring a new client runtime.

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

With an existing authenticated tracker connection, the agent updates the original
issue or creates the agreed new issues, then reads back their full content. It
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
CI repair policy and labels. The bundle-owned Python library under
`preparation/src/` preserves the structural contract for managed evaluation and
contributor tests. It is not loaded into the API or installed on users' computers.
[ADR 0222](../adr/0222-coworker-bundles-own-workflow-policy-and-curie-enforces-execution.md)
sets that ownership boundary; #4589 tracks the minimum isolated evaluation support.

## Versions and upgrades

A preparation version identifies the installed instructions. An output contract
identifies the format and semantics the factory accepts. The selected deployment
and its immutable bundle digest identify the actual consumer. The local Curie CLI
release does not prove that those three match.

The installer records a managed receipt and original file hashes outside the
client's discovered skill folder. Check for a release without changing files:

```bash
curie factory prepare update --agent codex --global --check
```

After reviewing the notification, apply an update or restore its backup:

```bash
curie factory prepare update --agent codex --global
curie factory prepare update --agent codex --global --rollback
```

Both operations refuse to overwrite edited managed files. There is no three-way
merge. Preserve customizations separately or reconcile them manually, then retry.
Updates retain local preferences and unrelated files, stage the complete payload,
retain the previous installation and recover interrupted activation on the next
explicit update. A rollback also checks for edits rather than discarding them.
Read-only checks and dry runs never repair or modify an installation.

An unmanaged older installation has no trustworthy original receipt. It must be
reviewed and backed up before adopting managed installation; the updater does not
pretend its current files are an unmodified released version. The prior CLI build
used in early branch testing still embeds its original files until rebuilt.

## One agent, multiple repository pins

Keep the repository binding in `.curie-factory.json`, which can be committed.
Unlike `.curie/`, this file is not ignored by the default repository configuration.
Use actual observed identities in place of these illustrative values:

```json
{
  "schema_version": 1,
  "preparation_version": "0.2.0",
  "target": {
    "deployment_id": "00000000-0000-4000-8000-000000000001",
    "bundle_sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "contract_version": "curie.factory-specification/v1"
  }
}
```

`preparation_release` is an optional exact package locator, not the factory's
platform version or proof of compatibility. The named release must actually carry
`factory-preparation.json`; an older release without that new asset cannot provide
it. Use cached assets or an explicitly supplied local release for unpublished
branch testing. The CLI never guesses a release tag from a preparation version.

Resolve the pin for the current checkout with the installed client's scope:

```bash
curie factory prepare resolve --agent codex --global --repo /path/to/repository
```

The result identifies the matching versioned instructions, preferences file and
factory target. Each repository can select a different cached release through the
same global entry skill. Cached versions remain outside the client's discovery
directories, avoiding duplicate skill names. Resolution checks file hashes and
contract support; it does not change the repository's pin or another repository's
active workflow. Use `--offline` when only existing cached assets may be used.
A missing uncached pin requires its actual release locator or a local `--source`
directory containing the self-contained `release.json` package.

An invocation checks for entry skill updates once per preparation session when the
CLI and network are available, then notifies before an update is applied. It
records and keeps the selected workflow for the draft. Moving a draft to another
version or target requires review of the entire changed specification. A network
failure does not discard a compatible cached release. This is an on-use check,
not an idle background service installed into the coding agent.

Local preferences live in the checkout's `.curie` directory, in
`factory-preparation.local.json`:

```json
{
  "schema_version": 1,
  "preferences": {
    "output_directory": ".projects/preparation",
    "review_style": "Show the complete specification in the chat",
    "repository_notes": "Preserve the documented public API"
  }
}
```

The skill checks these preferences when loading them, explains unknown or
incompatible choices, and keeps the original file intact. Preferences cannot
change the output contract, grant publication authority or bypass target checks.

## Target compatibility

Before selecting published work, the skill uses the configured Curie connection
to read the actual deployment's contract declaration. A missing endpoint, absent
declaration, changed deployment digest or unsupported contract leaves compatibility
unknown or incompatible. The issue may still be published after review, but the
skill does not add the factory label.

Versioned input carries one generic identity prefix bound to the exact canonical
body bytes, output contract, deployment UUID and bundle digest. The full published
representation is reviewed before submission. Intake and execution start compare
that binding independently; the local check is not a reservation. Existing legacy
bundles without the declaration retain their legacy workflow. Their absence of
contract support does not authorize the preparation skill to select versioned work.

The generic declaration and prefix are specified in the separate contract PR
#4615. Dependent runtime integration must not merge or deploy before that reviewed
contract lands. Static compatibility does not replace the future bounded semantic
evaluator or establish deployment readiness.
