# Software Factory administrator guide

Use this path to set up a factory, prepare a repository's runner environment,
and improve it using observed outcomes. The [user guide](factory-user-guide.md)
covers the issue and PR workflow after a repository is bound. Curie runs in
your Kubernetes installation, while configured model providers, trackers,
code hosts, and registries remain separate services.

## Set up a factory

1. **Choose a supported pairing and target.** The [factory quickstart](dark-factory-quickstart.md#choose-your-tracker-and-code-host)
   covers GitHub with GitHub, GitLab with GitLab, and the supported Jira Cloud
   pairings. For a local kind trial, prepare Docker, kind, kubectl, Helm, a
   released Curie CLI, and an Anthropic or OpenRouter credential. Allocate at
   least 16 GiB to Docker Desktop for the default local trial. For an existing
   cluster, use the explicit context path in the quickstart. Review the
   [prerequisites and capacity guidance](dark-factory-quickstart.md#what-you-need).

2. **Install and connect the factory.** For GitHub, create a repository with an
   initial commit and run the [GitHub quickstart](dark-factory-quickstart.md#choose-the-target-and-run-the-command)
   from an interactive Bash session. Its first `curie factory quickstart`
   invocation installs Curie and prints a prefilled GitHub App registration
   link. Create the App, generate its private key, and install it on the exact
   repository. Rerun the command with its App ID and key file, as shown in
   [Rerun with the App](dark-factory-quickstart.md#rerun-with-the-app).
   GitLab and Jira Cloud use their linked provider quickstarts instead.

3. **Verify the binding before inviting users.** Confirm the second command
   prints `Factory quickstart ready` with the intended Kubernetes context,
   repository, intake label, and deployed runner. Check the configured label
   and start authority for the selected tracker. The GitHub default is
   `curie-factory`; a GitHub user adding it needs repository write or admin
   access. Inspect [label admission and status](dark-factory-quickstart.md#label-an-issue)
   with a bounded trial issue. A command's success does not prove a work item
   completed; inspect the issue's final status and the PR or merge request.

For a larger installation, use the [factory bundle reference](../../examples/dark-factory/README.md#deploy-it-as-the-factory-agent)
and [production operations](../operations.md) for credentials, ingress,
storage, egress, upgrades, and publication policy. The quickstart's local kind
settings are a trial configuration.

## Prepare a repository environment

Install the four local skills from the repository checkout with a CLI that
contains the installer:

```bash
curie factory skills install --help
```

If the help succeeds, install the skills:

```bash
curie factory skills install --agent codex
```

If `skills` is unrecognized, use the [source checkout installation](factory-preparation.md#install-from-a-source-checkout)
or a newer release containing the installer.

In Codex, start an evidence review for the actual configured binding:

```text
$factory-environment-setup <repository binding>
```

Give the skill access to the repository, its issue and PR history, and the
current runner image. It examines the pinned base commit, declared checks,
lockfiles, workflows, recent failures, and image tool inventory. Review its
proposed runner spec, version probes, representative checks, network and
credential prerequisites, and previous image digest. Unknown history or
inventory remains an explicit gap. See the [environment setup guide](factory-environments.md#review-the-setup-proposal).

After reviewing the concrete change, approve the setup skill's apply step.
For an already bound repository it uses `curie cluster factory --repo-runner`
to build, probe, push, and bind a digest-pinned image. Verify the deployed
binding names the new digest. Run declared version commands, representative
checks, and any required registry access inside a sandbox claim. A successful
image build alone does not prove that the sandbox can reach a service. If the
post-deployment checks fail, restore the recorded binding and hold further
issue selection. The [runner reference](../../cli/README.md#repository-runner-images)
has the exact input and command requirements.

## Improve the factory from outcomes

Inspect recent work with `curie cluster work-items --agent dark-factory` and
the [work item outcome procedure](../operations.md#reading-work-item-outcomes).
Use a stated time window when comparing failures. In Codex, run:

```text
$factory-environment-maintenance <factory> over the last 30 days
```

The skill groups repeated causes and separates missing image tools from
network access, credentials, capacity, CI, publication limits, and incomplete
ticket instructions. Review the affected examples, proposed repair, cost,
negative control, and rollback digest. Approve a specific image candidate
through the setup workflow; fix non-image causes in their owning configuration
or instructions. Compare outcomes in a later window to see whether the same
cause recurs. Maintenance does not change a binding or retry old issues by
itself. See [maintenance details](factory-environments.md#review-failures-over-time)
and [outcome measurement](measuring-factory-outcomes.md).

For ongoing operation, use the [co-worker administrator guide](../administering-co-workers.md)
for approvals, model roles, budgets, inspection, and stop controls. Use the
[capability reference](../capabilities.md) to check the support status of a
provider pairing before extending a deployment.
