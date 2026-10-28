# Inspect and test a pull request

The `review-pr` skill helps you understand any PR, test it locally, publish
approved feedback and merge when you explicitly approve. No factory label or
configuration is needed for those steps. For an eligible factory-owned PR, it
can also request a factory revision when you choose that action.
Use it in the coding agent where you already have repository and GitHub access.
It complements [ticket preparation](factory-preparation.md).

If your organization already uses local review skills, you can keep using them.
Their findings and your decisions can become approved feedback on a factory PR.
The `review-pr` skill provides a guided walkthrough, local tests and that feedback
handoff in one workflow. Its factory revision action uses the configured intake;
it does not require the factory to run the local review skills.

## Install and start

Install the complete Curie skill set from your project with a CLI build containing
this command. If you already installed the set, no second installation is needed:

```bash
curie factory skills install --agent codex
```

For Claude Code, use `--agent claude-code`. Other supported clients are `copilot`,
`gemini` and `cursor`. The same [client directories and refresh behavior](factory-preparation.md#get-the-skill)
apply, with the folder named `review-pr`. Add `--global` for use across
repositories, `--dir DIR` for an exact existing project directory, or `--dry-run`
to inspect the destinations. The command installs both `prepare-factory-ticket`
and `review-pr` offline, with only Markdown payloads.
It requires no Python, Docker or running Curie deployment. Project tests still
require the project's tooling. Installer receipts prove copied files, not client
activation or access to a factory.

In Codex, refresh the session if necessary, then start with:

```text
$review-pr <pr-url>
```

In Claude Code:

```text
/review-pr <pr-url>
```

You can also ask your skill-capable coding agent to use `review-pr` with
the PR link. Client settings determine automatic discovery and activation.

To install manually, copy the entire
`examples/dark-factory/review/skills/review-pr/` folder from a reviewed
source revision to your client's skill directory, including both references.
Keep these files together; the installed workflow uses relative links. Upgrades
are manual, with the same preservation rules as preparation: the installer accepts
an identical copy and refuses to overwrite a different or edited copy. Move the
old folder outside skill discovery, install the desired version and deliberately
reapply customizations. There is no automatic update checker or version resolver.

## Review and test

The agent pins the PR head and base, preserves your checkout and explains:

* The components, their files and how calls or data move between them.
* Significant diff changes and observable changes to behavior.
* New test scenarios and assertions, relevant existing regression tests, and gaps.
* How the implementation meets the issue's criteria, including deferred checks.

Ask to inspect a component more deeply at any point. For example:

```text
Walk me through the request handler and its failure cases.
```

Then ask for guided testing:

```text
Help me test this locally, one step at a time.
```

Each step gives the command or UI action, expected observation, failure signal
and cleanup. Share the result before proceeding. You can instead authorize the
agent to run the applicable local tests. It records the exact revision and
observations, distinguishing your tests, its runs, CI evidence and author claims.
Instructions that have not been executed do not count as passing tests.

## Send feedback and request a revision

The agent gathers your findings and decisions into an exact comment or review
for approval. It can use current diff locations for inline comments or publish
one consolidated PR review. It posts only after your authorization and reads
the result back. Your conversation and local review notes stay local unless you
approve their inclusion. Existing authenticated host tracker tools are required.

To request factory work, the agent identifies the configured App mention and
available factory ownership and intake evidence. On a supported factory-owned
GitHub PR, fresh comments or submitted commented/changes-requested reviews with
that mention can request a revision. It uses one actionable summary per round,
then distinguishes posting from observed factory admission and publication.
An ordinary comment, edited old comment or approval alone does not start work.
Posting on an ordinary PR does not enroll it in the factory.

The factory retains its existing authority, review, testing and publication gates.
Missing configuration or an unobserved acknowledgment is reported explicitly.
See [factory operation](../software-factory.md#delegate-inspect-and-refine) and the target
deployment's configuration; the separate Slack-bound review ingress settings
are not a universal prerequisite for factory-owned feedback.

## Complete the change

When satisfied, explicitly ask to merge. The agent refreshes checks, required
approvals and blockers, and uses a merge operation conditional on the reviewed
head. New commits require reviewing the new delta and affected tests. It does not
bypass protections. After observing the merge, it makes approved issue updates
according to repository policy. A feature-train merge may intentionally leave an
issue open until release promotion; deferred qualification stays visible.

The skill automates host-agent workflow. It does not add a tracker integration,
factory service, merge authority or guarantee that local tests prove production
correctness. The source replay cases distinguish these boundaries explicitly.
