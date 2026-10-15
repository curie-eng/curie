# Prepare a factory specification locally

Use the factory's `prepare-factory-ticket` skill in your existing coding agent to
turn an existing ticket or new intent into a reviewed specification. For larger
requests it can propose complete child specifications, coverage and dependency
ordering. Preparation writes local artifacts and leaves the requested feature
for the factory to implement.

This first release produces **unvalidated drafts**. It includes no connected
deployment validator or tracker handoff. Developer review and local repository
evidence do not prove that Curie can execute a specification. Connected validation
and explicit handoff are tracked in #4488 and #4490.

## Get the skill

Use a Curie CLI built from this change, or a release that contains it. The existing
compiled CLI renders the files without a Curie source checkout:

```bash
curie example dark-factory render --out ./dark-factory
```

The skill folder is `dark-factory/preparation/skills/prepare-factory-ticket/`.
Copy that whole folder, including `references/`, to the location below for the
client you use. The preparation skill itself is Markdown only. It requires no
Python, parser package, Docker, running Curie service or model-specific dependency.
The rendering command may contact the release registry to resolve the factory's
runner image; it does not start that image. Once copied, the skill files can be
used without a Curie deployment.

| Local client | Repository skill destination | Invocation |
| --- | --- | --- |
| Codex | `.agents/skills/prepare-factory-ticket/` | `$prepare-factory-ticket` followed by your intent or ticket content |
| Claude Code | `.claude/skills/prepare-factory-ticket/` | `/prepare-factory-ticket` followed by your intent or ticket content |
| GitHub Copilot in a client supporting agent skills | `.github/skills/prepare-factory-ticket/` | Ask Copilot to use `prepare-factory-ticket` for your intent or ticket content |

These paths follow the providers' published discovery conventions:
[Codex](https://learn.chatgpt.com/docs/build-skills),
[Claude Code](https://code.claude.com/docs/en/skills), and
[GitHub Copilot](https://docs.github.com/en/copilot/how-tos/copilot-on-github/customize-copilot/customize-cloud-agent/add-skills).
Client versions and workspace settings determine skill availability. Use the
client's normal refresh or restart procedure if a newly copied skill is absent.
This documents supported file layouts, not an end-to-end certification of every
client or operating-system version.

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

The report remains `unvalidated` after review. The agent must not claim that a
deterministic parser, Curie deployment evaluator or tracker handoff ran. You can
use the reviewed artifacts to continue planning; execution readiness requires the
later connected validation and explicit handoff workflow.

## Scope of the first release

The local authoring skill can ship before migration of existing factory prompts,
CI repair policy and labels. The bundle-owned Python library under
`preparation/src/` preserves the structural contract for managed evaluation and
contributor tests. It is not loaded into the API or installed on users' computers.
[ADR 0222](../adr/0222-coworker-bundles-own-workflow-policy-and-curie-enforces-execution.md)
sets that ownership boundary; #4589 tracks the minimum isolated evaluation support.
