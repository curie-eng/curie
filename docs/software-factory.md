# Software Factory

Delegate an issue to a Curie co-worker and get a pull request or merge request to review,
or a stated reason it could not complete. You can leave the work running
asynchronously, inspect progress on the issue, and request revisions on the
same change. Curie runs in your infrastructure; configured model providers,
issue trackers and code hosts still receive the data needed for their work.

Software Factory is the first developer adoption path for the broader
[co-worker platform](co-workers.md). `dark-factory` remains the bundle name,
agent identifier, and CLI example name. Existing commands and paths keep working.

**Start here:** choose your issues and code:

| Issues | Code host | Quickstart |
| --- | --- | --- |
| GitHub | GitHub | [GitHub](guides/dark-factory-quickstart.md#what-you-need) |
| GitLab | GitLab | [GitLab, including self-managed](guides/factory-gitlab-quickstart.md) |
| Jira Cloud | GitHub | [Jira Cloud with GitHub](guides/factory-jira-github-quickstart.md) |

Bitbucket Cloud and Data Center adapters ship, but their combined Jira Cloud
quickstart is unavailable. Jira Cloud with GitLab also has no supported
installation path. See [integration limits](capabilities.md).

For a larger repository or production deployment, use the
[bundle reference](../examples/dark-factory/README.md#deploy-it-as-the-factory-agent).

## What you are trying

This overview describes the default factory bundle and supported workflow.
The [quickstart](guides/dark-factory-quickstart.md) uses the latest release by default.
Check the [capability reference](capabilities.md) for supported integrations and
their limits.

| Before setup | What to expect |
|---|---|
| Host | Released CLI assets support Linux x86_64, Linux arm64, and Apple silicon macOS. The quickstart has no Intel macOS asset. |
| Cluster | Kubernetes with `kubectl` and Helm; Docker and kind v0.24+ are needed only if quickstart must create a local cluster. Review the selected context before installing. |
| Model | An Anthropic or OpenRouter credential, with available provider credit. Implementation and review use separately configured models. |
| Repository | A small repository with an initial commit and a bounded issue. GitHub and Jira Cloud with GitHub need a GitHub App installed on the repository. GitLab needs an active token with `api` and `write_repository` scopes and Developer access or higher. |
| Tracker authority | GitHub requires a permitted user with repository write or admin access; GitLab requires Developer access or higher. Jira Cloud requires an explicit account or group allowed to start work, plus the factory account's project permissions. |
| Task | Clear acceptance criteria and documented checks. A small repository is the first trial; Curie-sized builds need additional disk, memory, registry access, and CI configuration. |
| Toolchains | The stock layer supplies Python 3.13, Node 22, uv, Rust, and pnpm 9. Locked dependency fetching is restricted to the bundle's supported uv, Cargo, and pnpm commands and operator-approved registry routes. Other toolchains require a custom layer. |
| Result | A pull request or merge request with verification information, or a classified failure/clarification request. Model review and passing checks do not certify correctness or merge the PR. |
| Time and cost | Quickstart reserves about 190 minutes of configured waiting windows plus variable setup time. This is neither measured latency nor a completion promise. About 5 USD of available OpenRouter credit is planning guidance; actual usage varies. |

The quickstart documents precise prerequisites, models, installation, inspection,
and cleanup in one place. It uses a released runner layer; a source-built CLI
cannot deploy an unpublished layer through that path. Repository dependencies
are not baked into the stock image. The
[managed sandbox toolchain guide](guides/repository-toolchain-in-the-managed-sandbox.md)
and bundle reference explain locked installs, absent services, and resource limits.

## Bring your existing local workflow

Use the skills your team already has in its local coding agents to explore an
idea and prepare a specification. Both open source collections and skills
developed internally can produce the starting material for factory preparation.

Examples of open source local workflows include:

| Project | Preparation examples |
| --- | --- |
| [Matt Pocock's skills](https://github.com/mattpocock/skills) | Requirements clarification with `grill-with-docs`, specifications with `to-spec`, and decomposition with `to-tickets` |
| [Superpowers](https://github.com/obra/superpowers) | Brainstorming, design and implementation planning |
| [Addy Osmani's agent skills](https://github.com/addyosmani/agent-skills) | Requirements interviews, specification development and task planning |

Then use `prepare-factory-ticket` to review the existing issue, specification,
plan or discussion for factory execution. It inspects repository context, asks
about consequential gaps, assesses verification feasibility, and presents the
complete final issue for approval before publication and selection. Sufficient
existing issues keep their format and wording; an unchanged published issue needs
verification and selection rather than a body update. Settled decisions remain
inputs to the review, with questions focused on consequential gaps or conflicts. The skill can also help when you are starting with rough intent.

The same handoff works with internally developed local skills: include the
requirements, decisions and necessary context in the issue or repository material
the factory can access. A local conversation, installed skill or private file is
not automatically transferred to the factory. The factory reads the resulting
issue; it does not require that any particular skill authored it.

These links describe local authoring workflows. Compatibility here means using
their output as input to factory preparation and issue handoff. It does not assert
that the projects are installed in the factory or that each has passed a factory
integration test. The readiness review uses repository and available environment
evidence; deployment readiness remains unvalidated until connected validation is
implemented. A review cannot guarantee that a factory run will complete.

You can also use your team's local review skills to inspect a resulting PR and
prepare feedback, or use the companion [PR review skill](guides/factory-pr-review.md).
Approved feedback can request another factory revision through the configured
intake. A local review alone does not start implementation.

## Install Software Factory skills

Use `prepare-factory-ticket` in your local coding agent as the factory readiness
review and handoff step for existing material. Install the complete set, including
`review-pr`, once:

```bash
curie factory skills install --agent codex
```

In Codex, invoke `$prepare-factory-ticket <issue-url>` or give it new feature
intent. The skill inspects the checkout, asks about consequential ambiguity and
presents the entire specification for review. After review, choose **publish and
start the factory** to publish through your existing tracker tools and select
eligible issues with the configured factory label. Reports stay local.

The same install provides `factory-environment-setup` and
`factory-environment-maintenance` for administrators. Use the
[repository environment guide](guides/factory-environments.md) to plan a
repository runner image from issue and PR history, review its build and
deployment, and periodically classify failed work for targeted repairs.

The [preparation guide](guides/factory-preparation.md) has the installation and
invocation table for Codex, Claude Code, Copilot, Gemini and Cursor, global
installation, manual upgrades, and a source build option until the installer is
released. Installation needs no Python or running Curie service. Starting work
still needs a configured factory, tracker access and the start authority described
below. The skill does not deploy a factory or guarantee that a task can finish.

## Delegate, inspect, and refine

The GitHub workflow is below. The GitLab and Jira Cloud quickstarts above
provide the equivalent provider-specific steps.

1. **Prepare a bounded GitHub issue.** Describe the behavior, acceptance criteria,
   and how to verify it. Bring output from your [existing local workflow](#bring-your-existing-local-workflow)
   to the [preparation skill](#install-software-factory-skills) for factory readiness review
   and approved handoff, or prepare and select the issue yourself.
   Curie's factory runner takes over after admission. Start with a small change.
   The sandbox receives the
   repository checkout and reads the issue and comments through a scoped
   platform tool; it holds no GitHub publication credential.
2. **Add the configured label.** The default is `curie-factory`. A permitted
   user adding it admits work. Polling is the quickstart default, with a
   45-second interval; production can also use authenticated webhooks. By
   default, the factory waits 60 seconds after selection before acquiring a
   sandbox and reads the current `base:<branch>` label then. With no base label
   it uses the deployment default. Later base changes restart unpublished work
   or rebase and retarget its PR; see the
   [base selection rules](operations.md#admitting-a-labelled-github-issue).
3. **Follow the issue status comment.** It shows planning, implementation,
   review, publication, and CI phases. State labels distinguish queued,
   running, PR open, and needs human. Queueing for capacity is distinct from
   an executing model turn.
4. **Inspect the pull request and checks.** Read the diff, verification table,
   and any reviewer notes or unavailable checks. An opened PR can still be
   waiting on CI or need human intervention. The factory does not merge it.
   For a guided walkthrough and local testing, use the already installed companion:
   invoke
   `$review-pr <pr-url>`. The [PR review guide](guides/factory-pr-review.md)
   covers component details, coverage, manual tests and approved feedback.
5. **Request a revision.** Use a new issue comment explicitly mentioning the
   configured App login, such as `@<app-slug>`, with your requested change.
   Ordinary comments and edits do not execute work. Authorized PR mentions
   and review feedback are also supported when configured. Feedback
   publication requires a new human approval even where initial factory
   publication is automatic; see the operations reference.
6. **Stop or retry deliberately.** Remove the factory label or close the issue
   to cancel. Running work remains cancellation requested until termination
   is observed; an existing PR remains open and linked. After resolving a
   failure or answering clarification, re-add the label or send an authorized
   mention. Relabelling replaces the current run; inspect existing work first.
   After a reinstall or lost factory storage, a still-labelled issue whose
   factory pull request is open is not run again; its status comment links
   that pull request, and several open ones are refused until one remains. A
   closed or merged one stops the old label from running again, while a label
   added after it finished starts a new run. On Bitbucket Cloud, and on
   Bitbucket Data Center when the authenticated-user header is absent,
   ownership cannot be verified, so this does not suppress there and a
   reinstall can still open one duplicate pull request. When the code host cannot be read
   completely, admission waits for the next pass. The
   [operations reference](operations.md#admitting-a-labelled-github-issue)
   has the full order.

For operator inspection, `curie cluster work-items --agent dark-factory`
shows work item outcomes; supply an item ID for detail. This is distinct from
release/pod health inspection in the quickstart. See
[reading outcomes](operations.md#reading-work-item-outcomes),
[intake and cancellation](operations.md#admitting-a-labelled-github-issue), and
[review feedback](operations.md#accepting-review-feedback-from-github).

## What review and publication enforce

The default bundle has an implementer, a plan reviewer, and a diff reviewer.
It requires a plan verdict before source edits and a diff verdict before a
publication request. Each review loop has at most three rounds. Blocking
findings request changes; nonblocking notes can accompany approval. Ambiguous
requirements, exhausted review rounds, and unavailable reviewer calls end
with `Could not complete:` rather than an invented result.

These gates belong to the installed bundle. The platform does not claim that
all bundles ran tests or that a model's approval proves correctness. The
factory skill instructs the implementer to run repository checks and record
verification, with required CI delegation only where its verification contract
allows it. See the [review protocol](../examples/dark-factory/README.md#review-loops).

**Publication policy is a separate control.** New agents default to `approve`,
requiring a human publication decision with a configured approval route.
The quickstart deliberately sets `auto`, allowing its initial pull request
without a human publication click. `auto` preserves patch validation, scope,
and the trusted publisher outside the sandbox. It does not mean automatic
merge. The [production recipe](../examples/dark-factory/README.md#deploy-it-as-the-factory-agent)
keeps human publication approval unless the operator explicitly changes it.
See [administering co-workers](administering-co-workers.md).

The factory cannot publish changes under `.github/`, regardless of the issue's
request. Split such work out or implement it outside the factory. Toolchain
changes, required services, dependency routes, execution deadlines, and build
resources also constrain which tickets can finish.

## CI is part of the result

After publication, the platform waits for checks within the execution deadline.
Failures can trigger up to three repair rounds on the same PR or merge request.
Failed GitHub Actions jobs are rerun once before repair when the App has Actions write access;
without it, check summaries remain available but logs/reruns can be unavailable.

GitLab uses pipeline status on the merge-request head; GitHub Actions log and
rerun behavior does not apply to it. Jira Cloud with GitHub uses GitHub CI.

The default CI wait is 3600 seconds (60 minutes) from the push. An explicit
operator value, including 1200 seconds, is kept. The wait is still bounded by
the remaining execution deadline. Unreadable checks, missing required evidence, and a wait
that expires produce unverified or timed-out results. A merge conflict is
reported for human resolution. Zero checks complete only after the 120-second
no-check grace, with no required check missing, and a PR that the code host
proves mergeable or merged; each code host supplies that proof its own way,
Bitbucket Cloud through its pull request mergeability checks. An unknown mergeability answer never completes.
That result carries a no-CI note; it is not evidence that tests passed.
Required repository policies can impose stricter evidence. The detailed
[factory operations reference](operations.md#admitting-a-labelled-github-issue)
is authoritative for base-branch failures, required Python checks, and timeouts.

## Operate and evaluate

Use [measuring factory outcomes](guides/measuring-factory-outcomes.md) to
compare completed work, PR acceptance, estimated costs, and human effort.
Configure implementer and reviewer models explicitly; this is not a promise
of automatic task-complexity routing.

When upgrading, re-render the stock bundle with the upgraded release CLI so
its toolchain layer matches that release's platform runner. A custom toolchain
layer must be built and published to a registry the nodes can pull from.
Use the [operations upgrade procedure](operations.md) and bundle reference;
changing an image tag or using an unreleased commit is not the quickstart path.
