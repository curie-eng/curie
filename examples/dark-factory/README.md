# dark-factory: the default factory agent

This is the agent Curie runs for a labelled GitHub issue. One issue goes in.
One pull request, or one stated reason, comes out.

One agent does the work with one skill,
[`skills/implement-issue/SKILL.md`](skills/implement-issue/SKILL.md), and two
reviewer subagents check it on a stronger model. The skill walks nine phases:

1. `read_issue`: read the issue by link.
2. `pin_criteria`: pin the acceptance criteria, or stop and list the
   questions when the request is ambiguous.
3. `plan`: read the repository's own guidance, find its test commands, and
   write a plan.
4. `plan_review`: [`agents/plan-reviewer.md`](agents/plan-reviewer.md)
   approves the plan or sends it back to `plan`.
5. `failing_test`: write a failing test first where one is feasible.
6. `implement`: make the smallest change and run the repository's own checks.
7. `review_diff`: [`agents/diff-reviewer.md`](agents/diff-reviewer.md) reviews
   the working diff against every criterion, and approves it or sends it back
   to `implement` (never to `plan`).
8. `publish`: publish one pull request, or end with `Could not complete:` and
   the reason.
9. `wait_ci`: the pull request's checks run and the platform waits on them
   and reports this phase.
   A failure sends a new message in the same run with the failing checks, and
   the run loops back to `implement` to fix them, then republishes to the
   same pull request. A green result, or no checks at all, ends the run
   successfully; an unreadable checks result or one that never settles ends
   it unverified or timed out instead.

The skill budgets its own time against the platform's 10800 second (3 hour)
execution bound and treats the issue text and repository files as untrusted data.
Running the repository's tests is an instruction in this skill. The platform
does not enforce it, and a different bundle can choose differently.

## Review loops

Each loop (`plan` and `plan_review`, `implement` and `review_diff`, and
`wait_ci` back to `implement`) runs at most 3 rounds. When a reviewer still asks for changes on round 3, or a review
call fails, the run publishes nothing. It posts the reviewer's unresolved
findings and open questions on the issue and ends with `Could not complete:`.

A reviewer tags every finding `blocking` (the change would be incorrect or
unverifiable, or misses a criterion) or `note` (an improvement that does not
change correctness). It returns `VERDICT: APPROVE`, listing the notes under
`NOTES:`, when only notes remain, so refinements stop costing rounds. The
agent carries plan-review notes into `implement`, and lists diff-review notes
in the pull request body instead of editing code the diff reviewer already
approved. Notes never start another review round.

[`hooks/review_gate.py`](hooks/review_gate.py) enforces this, because the main
model does not follow the protocol reliably. It routes every sub-agent call to
the right reviewer, strips `isolation` and `model`, forces
`run_in_background: false`, refuses reviews out of order, counts rounds and
applies the cap, stops the run on a reviewer reply without a verdict, and
refuses `publish_changes` until the diff reviewer approves. It also writes the
`plan_review` and `review_diff` phase lines, with the round, to the pod log.

Both reviewers default to `anthropic/claude-opus-5.5`, served through the same
OpenRouter key as the main loop. The Agent tool's own `model` argument only
takes Claude aliases, so the per-deployment override is the `model:` line in
each file under `agents/`: change it in the bundle you deploy. Opus 5.5 needs
the runner's bundled Claude Code CLI 2.1.280 or later (claude-agent-sdk
0.2.158 or later).

## What the bundle can reach

- **The checkout.** Curie mounts the issue's repository at `/workspace` and
  gives every session the built-in file tools. The sandbox has no general
  network access, and it holds no push or publication credential.
- **The issue.** `.mcp.json` declares the GitHub MCP server that the runner
  image preinstalls, authenticated with the bundle's own
  `GITHUB_PERSONAL_ACCESS_TOKEN` (ADR 0145: reading the ticket is the bundle's
  job). The manifest's `toolPolicy` allows `github/get_issue` and
  `github/add_issue_comment`. The review gate hook refuses the comment tool
  except for one comment on the run's own issue, after a failed or capped
  review, to post the unresolved findings. Every other GitHub tool, including every other write tool, is
  denied by the runner, and so is any tool the server adds later.
- **Publication.** The built-in `mcp__curie__publish_changes` tool. The platform
  captures the patch and publishes it from a separate trusted job. The agent
  never pushes.

Give the bundle a narrow token: a fine-grained token (or a GitHub App
installation token) limited to the factory repositories with **Issues: Read
and write** and nothing else. Write is for the findings comment. With a
read-only token a capped run still stops and states its findings in its final
reply, but they do not reach the issue. That token is in the sandbox's
environment, so any tool in the session can read it. The tool policy limits
which GitHub MCP tools the agent can call; it does not hide the credential
from other tools. The token scope is the real bound.

## Deploy it as the factory agent

Enable factory intake first (see "Admitting a labelled GitHub issue" in
[`docs/operations.md`](../../docs/operations.md)). Then:

```bash
# The skill plans for a 3 hour run. The execution deadline defaults to 1800 s
# and the worker budget to 600 s, so raise all three. The chart raises the
# worker termination grace with the budget.
helm upgrade curie <chart> -n curie --reuse-values \
  --set worker.deliveryBudgetSeconds=10800 \
  --set worker.runnerTotalTimeoutSeconds=10800
curie cluster overrides dark-factory --execution-deadline 10800

# The factory's default model: GLM 5.3 Flash through OpenRouter.
helm upgrade curie <chart> -n curie --reuse-values \
  --set agentSandbox.runner.fakeModel=false \
  --set agentSandbox.runner.model=z-ai/glm-5.3-flash \
  --set agentSandbox.runner.credentials=<openrouter-api-key>

# Runner egress to the GitHub API for the MCP server, one entry per CIDR
# from the "api" list at https://api.github.com/meta.
helm upgrade curie <chart> -n curie --reuse-values \
  --set 'agentSandbox.connectorEgress.dark-factory[0].cidr=<github-api-cidr>' \
  --set 'agentSandbox.connectorEgress.dark-factory[0].ports[0].port=443' \
  --set 'agentSandbox.connectorEgress.dark-factory[0].ports[0].protocol=TCP'

export GITHUB_PERSONAL_ACCESS_TOKEN=<read-only token>
curie cluster deploy --plugin-dir examples/dark-factory \
  --agent dark-factory --env prod --repo acme-corp/acme-bot \
  --secret GITHUB_PERSONAL_ACCESS_TOKEN
curie cluster surfaces dark-factory --add github=acme-corp/acme-bot

# Optional. Human approval of each pull request stays the default.
curie cluster publication-policy dark-factory --policy auto
```

Label an issue in `acme-corp/acme-bot` with the configured factory label. The
run ends as one pull request or one comment on the issue that names the cause.

`curie dev factory-e2e` deploys this bundle by default when it drives the
factory against a disposable install.

## Live status

`progress/phases.json` declares the nine phases and the two review loops. At
the start of each phase the skill calls `mcp__curie__report_progress`, and the
platform edits one status comment on the issue with a live card showing the
current phase, the loop rounds and the run's activity. A failed report never
stops the run.

## Evals

`evals/cases.json` checks the parts of the workflow a single turn can show: the
issue tool it reads with, the execution bound, refusing an injected credential
request, stopping on an ambiguous request, never pushing, and approvals with
notes ending the review loop. With a
live-model runner up from this directory:

```bash
curie skill eval
```
