# Prepare and maintain factory repository environments

The `factory-environment-setup` and `factory-environment-maintenance` skills
help an administrator decide what a repository's factory sandbox needs. They
run in your local coding agent, alongside `prepare-factory-ticket` and
`review-pr`. They are not execution skills inside the deployed factory.

Use setup when onboarding a repository or replacing its runner image. Use
maintenance after repeated `needs human` outcomes or on an administrator-chosen
schedule. Each repository binding can select a different, digest-pinned runner
image. The [repository runner reference](../../cli/README.md#repository-runner-images)
describes the input format, build probes, and binding command.

## Install and invoke

From the repository checkout, install the four local skills with a Curie CLI
release that includes them:

```bash
curie skills install --agent codex
```

Choose `claude-code`, `copilot`, `gemini`, or `cursor` instead when that is your
coding agent. Use `--dir` to choose an existing project directory or `--global`
for personal installation across repositories. The command copies Markdown
skills offline; it does not inspect issues, change the factory, or install a
runner image. See the [skill installation guide](factory-preparation.md#get-the-skill)
for client paths and update behavior.

In Codex, start a setup review with:

```text
$factory-environment-setup <repository binding>
```

Give the skill the configured factory binding and access to the repository,
its issue tracker, code-host PR or merge-request history, and current runner
image information. GitHub and GitLab may be both tracker and code host. Jira
Cloud issues may be paired with GitHub or Bitbucket Cloud/Data Center PRs.
The skill follows the actual binding; it does not infer the tracker from the
repository URL. Check [supported integrations and limits](../capabilities.md)
before deploying a new provider pairing.

## Review the setup proposal

The skill reads the repository's verification declaration, workflows,
lockfiles, and runner inputs at one pinned base commit. It samples recent
issues and completed PRs or merge requests, including failed checks and review
feedback, and compares the required tools with the selected image's
<!-- doclint:ignore-line -->
`/etc/curie/tool-manifest.json`. If history or the image inventory is
unavailable, the proposal records an unknown rather than assuming readiness.

Review a proposal that separates:

| Finding | Administrator action |
| --- | --- |
| Missing executable or wrong version | Change the operator-owned repository runner spec using catalog tools or a custom Dockerfile and probe. |
| Registry egress, service access, or credentials | Change the relevant sandbox or deployment configuration and verify it from a sandbox claim. An image rebuild alone cannot supply access. |
| Missing acceptance criteria or checks | Improve the repository verification declaration or ticket-preparation instructions. |
| CI or base branch failure | Repair that upstream condition before treating the issue as factory-ready. |

The proposal should show the exact runner spec, version probes, representative
checks, an absent-tool control, non-image prerequisites, and the previous image
digest for rollback. Keep repository-specific evidence and deployment details
in the operator's private record.

After reviewing the concrete spec and binding change, use the setup skill's
approved apply step. For an already bound repository, it uses the existing
`curie cluster factory --repo-runner` path to build, probe, push, and bind the
image digest. The [CLI reference](../../cli/README.md#configure-an-installed-factory-repository)
has the full command and required context, namespace, release, registry, and
repository arguments. A standalone `curie build --repo-runner` is available
when you need only a build receipt; running both paths builds twice.

Verify the deployed binding names the new digest, then run the declared
version commands and representative checks inside a sandbox claim. Check
required registry access there as well: a successful image build does not
prove sandbox egress. If post-deployment checks fail, restore the recorded
binding and hold further issue selection for that repository.

## Review failures over time

In Codex, invoke the maintenance skill with a factory and time window:

```text
$factory-environment-maintenance <factory> over the last 30 days
```

It reads `curie cluster work-items --json` outcomes and the configured
tracker and code-host records. State the window and number of outcomes
examined. The work-items list is bounded; a truncated result requires
additional provider history or an explicit coverage gap. Group retries of
one root cause and distinguish image tools from egress, services, credentials,
capacity, CI, publication restrictions, and incomplete instructions.

The output is a prioritized set of proposed repairs with affected examples,
the expected reduction in repeat failures, a negative control, build or
operational cost, and rollback digest. It makes no binding or image change on
its own. Approve a specific image candidate to follow the setup workflow;
handle non-image causes in their owning configuration or preparation
instructions. Compare later failures with the previous window to see whether
the cause recurred. An updated image does not automatically retry old issues;
use the configured tracker's explicit retry or selection policy.

These skills do not schedule themselves. Run maintenance manually or invoke it
from your own administrator schedule. A first planning pass can use read-only
provider access and stop after the proposed plan; no registry push or cluster
deployment is needed to inspect the recommendation.
