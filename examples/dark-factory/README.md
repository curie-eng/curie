# dark-factory: the default factory agent

This is the agent Curie runs for a labelled GitHub issue. One issue goes in.
One pull request, or one stated reason, comes out.

Trying it for the first time? The
[dark factory quickstart](../../docs/guides/dark-factory-quickstart.md) takes
you from nothing to a pull request on a new repository with
`curie factory quickstart`, polling intake and a local kind cluster.

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
call fails, the run publishes nothing. It ends its reply with `Could not complete:` and the
reviewer's unresolved findings and open questions, and the platform's status
comment on the issue carries that result.

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
It hands the agent and both reviewers the same verification contract,
[`verification/contract.md`](verification/contract.md): the per-criterion
verification table, when a check that needs an absent service may be delegated
to a required pull request check, what always blocks, and the `VERIFICATION:`
block each reviewer returns, which the hook records as a
`curie_gate: verification_classified` line. When the bundle ships a
`verification/checks.json`, both reviewers also get it after the contract.
Without the contract no review runs.

The same hook gates repository edits. `Edit`, `Write`, `MultiEdit` and
`NotebookEdit` are refused until the `dark-factory:implement-issue` skill is
loaded and the plan reviewer approves. After approval only test edits may run:
an edit to a test file, or an edit to a source file that adds a test (an
inline Rust `#[test]` module, a `def test_` function, and the like) beyond the
ones the replaced text already held. The model writes the failing test (a
test file, or an inline test that adds a test), runs it with Bash, then calls
`report_progress` with phase `implement`, then edits source. When no test is
feasible it reports `implement` directly and says why. That report is the one
event that opens source edits; no Bash command does. The hook records evidence
only: after a test edit it counts finished Bash commands and failed ones,
never their text, and writes `test_edited`, `bash_after_test_edit` and
`bash_failed_after_test_edit` on its `implement` phase line. It never claims
a test ran. The hook fingerprints the checkout at the prompt and refuses the plan
approval, stopping the run, if the checkout changed before it (for example
through Bash). Its evidence lines share one `seq` counter per run:
`curie_phase` lines carry `source: "hook"`, `curie_reported` lines record each
`report_progress` call with the hook's last observed phase and `late: true`
when the report trails it, and `curie_gate` lines record refusals.

Both reviewers declare the `opus` alias. The runner resolves it to
`claude-opus-5-5` for a direct Anthropic credential or
`openai/gpt-6.1-sol` for an OpenRouter credential, using the same
credential as the main loop. Pin a reviewer model with
`curie cluster overrides dark-factory --reviewer-model <provider-model-id>`;
`--clear-reviewer-model` restores the credential default without changing the
bundle. The local overrides command accepts the same flags.

The runner image caps every model request at 16000 output tokens
(`CLAUDE_CODE_MAX_OUTPUT_TOKENS` in [`runner.Dockerfile`](runner.Dockerfile)).
Claude Code otherwise asks for up to 64000 output tokens per Opus request, and
OpenRouter reserves credit for that whole ceiling, so a low balance refused
every reviewer call. A verdict needs a few thousand tokens. Claude Code has no
per-subagent ceiling, so the cap applies to the factory model's requests too.
Plan on 5 USD of OpenRouter credit per run. Both the account balance and the
key's own limit count, and the smaller one is what a run can spend. When
OpenRouter refuses a reviewer call for credit (HTTP 402), the run ends with the
out-of-credits cause and the review is not retried.

## What the bundle can reach

- **The checkout.** Curie mounts the issue's repository at `/workspace` and
  gives every session the built-in file tools. The sandbox has no general
  network access. The operator may allow package registry routes for locked
  dependency fetches, and the sandbox holds no push or publication credential.
- **The issue.** The platform tool `mcp__curie__get_issue` (arguments
  `owner`, `repo`, `issue_number`), mounted for executions that have a
  WorkItem. The tool presents an execution scoped capability the API minted
  for that execution. The API checks that it names this execution and the
  WorkItem's issue, reads the issue and its comments with the GitHub App
  installation token (minted fresh per read, so runs longer than one hour
  keep reading), and returns the title, body, state, author and comments
  verbatim. It stores nothing. The sandbox holds no GitHub credential at all.
  A failed or capped review ends the reply with `Could not complete:` and the
  unresolved findings, and the platform's factory status comment on the issue
  carries that result.
- **Publication.** The built-in `mcp__curie__publish_changes` tool. The platform
  captures the patch and publishes it from a separate trusted job. The agent
  never pushes.

## Deploy it as the factory agent

For a laptop trial on a small repository, follow the
[quickstart](../../docs/guides/dark-factory-quickstart.md) instead; it needs
none of the long run sizing below. This section is the production recipe for a
repository the size of Curie itself.

Write the bundle out first. No clone of this repository is needed:

```bash
curie example dark-factory render --out ./dark-factory
```

A release CLI records the runner layer its release published (the repository
toolchains, for linux/amd64 and linux/arm64) in `connectors.lock.yaml`, so the
deploy below builds nothing. A CLI built from source has no published layer,
says so, and names the `curie build` that builds one instead.

Then enable the model and intake. The skill plans for a 3 hour run. The chart
worker budget and runner ceiling already default to 10800. The execution
deadline still defaults to 1800, so it needs an override.

```bash
# The factory's default model: GLM 5.3 Flash through OpenRouter. The
# credential comes from the environment, never argv.
export CURIE_CREDENTIALS=<openrouter-api-key>
curie cluster up --model z-ai/glm-5.3-flash --allow-egress-host openrouter

# The helm sets pin the worker budget explicitly. The chart raises the worker
# termination grace with the budget. At this budget, the drain Job publishes a
# minimum Helm timeout of 21900 seconds in its annotation.
helm upgrade curie <chart> -n curie --reuse-values \
  --timeout 21900s \
  --set worker.deliveryBudgetSeconds=10800 \
  --set worker.runnerTotalTimeoutSeconds=10800
curie cluster overrides dark-factory --execution-deadline 10800

# Intake. On a release with no GitHub App, the first run prints a prefilled App
# registration link (add --org <org> for an organization) and four manual
# steps, and applies nothing. It never opens a browser. Click Create, copy the
# App ID, generate a private key, and install the App on the repositories.
# Polling is the default and needs no webhook URL or secret. Then rerun with
# the App's details.
curie cluster factory

# The rerun confirms the App with GET /app, takes the mention from its slug,
# and uses its installed repositories as the allowlist (--repo, repeatable,
# is checked against them instead). The label defaults to curie-factory and
# is created in each repository. The key goes into the Secret
# curie-github-app (key privateKey) through kubectl stdin; a Secret that
# holds another App's key is refused. Polling does not need a webhook secret.
# Pass --webhook-secret-file (or CURIE_GITHUB_WEBHOOK_SECRET) only for webhook
# mode. Before applying anything, the command checks the merged config
# against the API boot gate and applies nothing if one value is missing. The
# values survive a later `curie cluster up`.
curie cluster factory \
  --app-id <app-id> --private-key-file ./app.pem --intake poll

curie cluster deploy --plugin-dir ./dark-factory \
  --agent dark-factory --env prod --repo acme-corp/acme-bot
# Illustrative USD cap for a run that can last 3 hours. Tune it for your model.
curie cluster budget dark-factory --limit 100
curie cluster surfaces dark-factory --add github=acme-corp/acme-bot

# Optional. Human approval of each pull request stays the default.
curie cluster publication-policy dark-factory --policy auto
```

`curie cluster factory --disable` turns intake off. See "Admitting a labelled
GitHub issue" in [`docs/operations.md`](../../docs/operations.md) for what the
gate requires.

### On a laptop (kind)

A kind cluster works for a single operator. It pulls the published runner
layer from GHCR, so it needs no local registry.

Polling needs no inbound GitHub connection. If you choose webhook mode,
GitHub must reach the API. Start a port forward and a cloudflared quick tunnel
to it:

```bash
kubectl --context kind-<name> -n curie port-forward svc/curie-api 8000:8000
cloudflared tunnel --url http://localhost:8000
```

Set the App's webhook URL to `<tunnel>/github/webhook`, enable the webhook,
and subscribe to Issues, Issue comment, Pull request review, and Pull request
review comment. Pass `--intake webhook`, `--webhook-secret-file <path>` and
`--card-base-url <tunnel>` to `curie cluster factory`. The port forward and
tunnel both die when the laptop sleeps or the API restarts. Restart them and
update the App webhook URL if the quick tunnel hostname changed.

Before a production factory run resolves dependencies, provide an operator
controlled registry mirror or terminating proxy. Configure uv, Cargo and pnpm
to use it, including package archive URLs
and redirects recorded in their lockfiles. Restrict its upstream hosts to
`pypi.org`, `files.pythonhosted.org`, `index.crates.io`, `static.crates.io`,
`registry.npmjs.org` and any reviewed redirect destinations. Permit only the
read methods `GET` and `HEAD`; reject uploads and arbitrary `CONNECT` tunnels,
which cannot enforce HTTP methods inside TLS. Allow only exact reviewed package
metadata and artifact paths needed by the committed lockfiles. Permit only
reviewed query keys and values; reject unknown paths and queries. Never forward
client supplied authentication headers, including `Authorization`,
`Proxy-Authorization` and `Cookie`. The proxy must supply its own upstream
authentication if needed. Allow the sandbox to reach only the proxy CIDR on
TCP port 443:

```yaml
agentSandbox:
  registryEgress:
    dark-factory:
      - cidr: "<operator registry proxy CIDR>"
        ports: [{ protocol: TCP, port: 443 }]
```

Put this in an operator maintained Helm values file and apply it:

```bash
helm upgrade curie <chart> -n curie --reuse-values --timeout 21900s \
  -f <factory-egress-values.yaml>
```

PyPI resolves package metadata through `pypi.org` and downloads artifacts from
`files.pythonhosted.org`. Cargo uses `index.crates.io` for the sparse index and
`static.crates.io` for archives. pnpm uses `registry.npmjs.org`. The proxy must
check redirects against its allowed upstream hosts. The `registryEgress` CIDR
above points only to that proxy. NetworkPolicy matches IP addresses, not
hostnames, and a shared CDN CIDR can also serve unrelated hosts. Direct CDN
CIDRs are unsafe for factory runs because dependency build scripts or other
runner code could reach unrelated hosts on the same address range. If the
proxy is unavailable, leave registry egress closed and report the locked
installs as unavailable. Verify the rendered policy
selects the `dark-factory` runner pods.

If runs fetch a registry on some attempts and fail with `Failed to connect` on
others, the `registryEgress` list covers only part of that host's addresses.
`index.crates.io` resolves through Fastly to several rotating IPv4 addresses
and IPv6 addresses in `2a04:4e42::/32`, and each fetch picks one. A CIDR that
covers one of those addresses passes the runs that pick it and fails the rest.
The factory reports this as `registry_egress_unreachable` with the addresses
that connected and the ones that did not. Point `registryEgress` at the
proxy's fixed address instead of the CDN.

The platform runner already supplies Python 3.13 and Node 22. This bundle
adds pinned uv, Rust and pnpm 9 for repositories with committed lockfiles. It
does not bake repository dependencies into the image. The published layer is
built on the platform runner of the same release, so after upgrading Curie,
render the bundle again with the upgraded CLI. After changing the toolchain
layer, build it yourself into a registry your nodes can pull from; the build
replaces the published entry in the lock:

```bash
curie build --plugin-dir ./dark-factory --registry <registry-ref> \
  --platform linux/amd64
curie cluster deploy --plugin-dir ./dark-factory \
  --agent dark-factory --env prod --repo acme-corp/acme-bot
```

The build updates the layer digest, and the deploy selects that digest.

Measure writable disk use in a runner pod after the frozen installs:

```bash
du -sh /workspace/.venv /workspace/.cache/uv /workspace/.cargo \
  /workspace/.cargo-target \
  /workspace/apps/ui/node_modules "$HOME/.local/share/pnpm" 2>/dev/null
```

In a runner pod for this repository, the root uv environment used 538 MiB, the
Cargo cache used 585 MiB, and UI modules used 223 MiB. After
`cargo test --no-run` completed successfully, the Cargo target directory used
8.7 GiB.
These are measured values for this checkout; other repositories and build
profiles can need different amounts. The chart's default 1 GiB workspace,
512 MiB home scratch, and 4 GiB runner ephemeral storage limit cannot hold this
workload. Give the factory agent its own workspace ceiling with
`agentSandbox.workspaceSizeLimits`, which applies to that agent's sandboxes
only. Merge these values into the same Helm values file as the registry routes:

```yaml
agentSandbox:
  workspaceSizeLimits:
    dark-factory: 24Gi
  runner:
    hardening:
      writablePathSizeLimit: 2Gi
```

The kubelet counts the workspace against the pod's `ephemeral-storage` limit
too, so raise the factory agent's runner resources to match. This override
applies to the `dark-factory` agent only:

```bash
curie cluster overrides dark-factory --runner-resources \
  '{"requests":{"cpu":"500m","memory":"2Gi","ephemeral-storage":"16Gi"},"limits":{"cpu":"2","memory":"6Gi","ephemeral-storage":"28Gi"}}'
```

With these settings, a sandbox running this layer built every `cli/` test
target (`cargo test --locked --manifest-path cli/Cargo.toml --no-run`) in about
three minutes. The workspace peaked at 8.8 GiB and the build used the full 6 GiB
of memory. With a 2 GiB memory limit, the same build was OOMKilled while
linking.

`writablePathSizeLimit` still applies to every sandbox runner in this Helm
release. The larger home scratch holds the default uv, Cargo and pnpm caches;
a run can instead put them under `/workspace`. Repeat the frozen installs and
Rust build in a pod with these values, measure peak disk and memory use, and
raise the limits if the completed build needs more room.

The $100 cap is an example for this three hour recipe. Tune it to the model
and expected workload. The SDK applies it to each session; it does not meter
daily spend across runs. It does not guarantee a $100 bill. On OpenRouter's
Anthropic Messages route, the
[documented response](https://openrouter.ai/docs/api/api-reference/anthropic-messages/create-a-message)
contains token usage but no billed cost field. The conclusion that the SDK
cost used for Curie's USD cap is an estimate is an inference from those
documented response fields, not a live billing measurement. OpenRouter reports
cost through its separate [generation metadata endpoint](https://openrouter.ai/docs/api/api-reference/generations/get-generation).
Check OpenRouter Activity or the cost of each generation for actual billing.
The [SDK budget example](https://github.com/anthropics/claude-agent-sdk-python/blob/main/examples/max_budget_usd.py)
checks the cap after each API call, so the estimate can exceed the limit by
one API call.

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
request, stopping on an ambiguous request, never pushing, reporting each phase
once, never ending a message without a tool call, and approvals with notes
ending the review loop. With a
live-model runner up from this directory:

```bash
curie skill eval
```
