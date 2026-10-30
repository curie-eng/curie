# Production operator journey

Use this page to plan, install, verify, and operate a Curie cluster deployment. It orders the existing procedures; the linked runbooks hold the commands and detailed limits. Check the [release notes](https://github.com/curie-eng/curie/releases) and the documentation matching your installed CLI and chart before changing a production installation.

This page answers what to do and verify at each production stage. Use the [administrator controls guide](administering-co-workers.md) to decide access, approvals, models, budgets, memory, and stop policy. Consult the [capability reference](capabilities.md) for released behavior and limits and the [security policy](../SECURITY.md) for the trust boundary. Curie runs platform and sandbox workloads in your Kubernetes installation. Configured model providers and workspace services receive the data needed for their selected workflows.

## Before installation

Record the target release, Kubernetes context, namespace, Helm release, chart values file, model provider, workspace connections, and the people who can approve restricted actions. Keep credentials out of bundles and chat. The [cluster operations runbook](operations.md#installing-and-inspecting-the-curie-platform-on-the-cluster) explains context pinning and installation behavior.

| Decision | Required access and source | Success check | If it fails |
| --- | --- | --- | --- |
| Choose a cluster with enough capacity for the platform and sandbox workload. | Cluster administrator access; [chart footprint and resource envelope](../charts/curie/README.md#single-node-footprint-measured-on-a-disposable-single-node-k3s-cluster-4-gb--4-core) and [sandbox resources](../charts/curie/README.md#sandbox-resource-envelope-adr-0059). | The planned nodes can schedule the selected chart components and expected concurrent sandboxes. | Adjust capacity or sandbox limits before intake; inspect [capacity waiting](operations.md#factory-work-items-wait-for-capacity). |
| Establish isolation and network paths. | Cluster administrator access; [chart preflights](../charts/curie/README.md#the-three-preflights), [security rails](../charts/curie/README.md#security-rails), and [security policy](../SECURITY.md#the-sandbox-rails). | Required preflights pass and intended egress is explicit. | Stop the install and correct the CNI, runtime, or egress configuration. Do not treat a rendered NetworkPolicy as proof of enforcement. |
| Choose identities and credentials for each external service. | Platform administrator and service owner access; [administrator guide](administering-co-workers.md#bind-workspaces-and-credentials-deliberately), [chart secret handling](../charts/curie/README.md#secrets-at-rest), and [capabilities](capabilities.md#workspaces-and-integrations). | Each selected connection has a named owner, intended scope, and rotation path. | Keep the affected integration disabled until its permissions and storage are understood. |
| Decide approval, model, budget, memory, and stop policy. | Platform administrator access; [administrator controls](administering-co-workers.md) and [capability limits](capabilities.md#memory-approvals-and-cost-controls). | The people who approve actions and the permitted model and spend settings are recorded. | Keep the agent or route inactive until the policy is explicit. |

The chart's measured single node footprint is a test observation, not a production sizing guarantee. Size for your workload and verify the actual release under your expected concurrency.

## Install and prove the deployment

1. Install a released CLI and chart using the [released chart procedure](../charts/curie/README.md#install-a-released-chart) and [cluster install runbook](operations.md#installing-and-inspecting-the-curie-platform-on-the-cluster). Pin the intended Kubernetes context explicitly when a workstation has multiple contexts. Keep the values file used for the install.
2. Run `curie cluster status` and inspect the [status interpretation](operations.md#curie-cluster-status). A nonconverged release returns a failure result with rollout reasons. Inspect `curie cluster observability` using the [administrator inspection guidance](administering-co-workers.md#inspect-work-and-stop-it-when-needed).
3. Exercise one supported workflow in a controlled repository or workspace. For Factory, follow the [canonical quickstart](guides/dark-factory-quickstart.md) for the chosen tracker and code host. Record the issue admission, work item state, review result, and publication outcome. A healthy platform status alone does not prove an agent can complete work.
4. Record the installed release, chart values revision, status result, test work item or conversation, and where an operator can inspect its trace. Remove the test route or fixture when its verification purpose is complete.

If installation fails, use the [cluster status diagnosis](operations.md#curie-cluster-status), [chart preflights](../charts/curie/README.md#the-three-preflights), and [known install gotchas](operations.md#known-gotchas) before retrying. Preserve the failed release and relevant logs long enough to identify the cause. Do not replace a failed install with an unreviewed security override.

## Protect state before production traffic

Choose an operator owned backup and restore procedure before accepting durable work. The installation may use PostgreSQL, the configured bundle object store, Valkey, and, when the mail adapter is enabled, its durable delivery state. Inventory the actual stores and encryption keys in your deployment, define a consistent backup point, retain the values and release metadata needed to recreate the installation, and test a restore into an isolated environment. Verify that the restored platform can read its records and bundle objects and that pending deliveries or work are accounted for before resuming intake.

The repository provides a [synthetic Compose restore drill](../cli/scripts/restore-drill.sh) and [inventory checks](../tools/restore-drill/restore_inventory.py). Those checks exercise a disposable environment. They do not provide a production backup product or an RPO or RTO commitment. This production restore path remains unverified for this guide. If your operator procedure has not been rehearsed, do not assume a Helm rollback can restore lost data. Preserve the affected state and escalate through your deployment's incident process.

## Operate and respond

| Situation | First check | Next action and recovery path |
| --- | --- | --- |
| A workload is unhealthy after install. | Run `curie cluster status`; inspect its [rollout reasons](operations.md#curie-cluster-status) and [chart preflights](../charts/curie/README.md#the-three-preflights). | Correct the reported capacity, image, network, or runtime cause. Recheck status and the controlled workflow before accepting traffic. |
| A Factory issue appears stuck. | Read [work item outcomes](operations.md#reading-work-item-outcomes) and [capacity waiting](operations.md#factory-work-items-wait-for-capacity). | Classify waiting, execution, review, or publication failure from the recorded state. Use the [administrator stop controls](administering-co-workers.md#inspect-work-and-stop-it-when-needed) when work must cease; preserve its record for diagnosis. |
| A model or workspace connection fails. | Inspect the [configured credential and route](administering-co-workers.md#bind-workspaces-and-credentials-deliberately) and the [supported pairing](capabilities.md#workspaces-and-integrations). | Repair the configuration or pause the affected route. Verify with a controlled request before reopening it. |
| Spend or action authority needs intervention. | Inspect [model and budget settings](administering-co-workers.md#configure-model-roles-and-bounded-runs) and [approval authority](administering-co-workers.md#separate-approval-authority-tool-policy-and-service-permissions). | Adjust the operator policy and use the [stop controls](administering-co-workers.md#inspect-work-and-stop-it-when-needed). Check the resulting work item or approval record. |

## Upgrade and recover from a failed upgrade

Before an upgrade, read the target [release notes](https://github.com/curie-eng/curie/releases) and the [upgrade runbook](operations.md#upgrading-the-chart). Back up the required state, keep an explicit values file, inspect what the new chart removes, and check whether the schema change permits rollback. The runbook warns that a chart upgrade can remove objects the new chart no longer renders. Use the [CLI upgrade procedure](operations.md#curie-cluster-upgrade) for the selected release. Check `curie cluster status` and repeat the controlled workflow after convergence.

If the upgrade refuses before mutation, follow the specific refusal in the [upgrade runbook](operations.md#upgrading-the-chart). If it leaves an unhealthy or pending revision, inspect Helm history and the [CLI rollback selection and schema checks](operations.md#curie-cluster-rollback) before choosing a revision. Run the documented dry run, then perform the selected recovery under your change procedure. Recheck platform status and a representative workflow. A rollback changes the release revision; it is not a substitute for restoring missing or incompatible data. Keep the old values file and backup until the result is verified.

## Evidence to retain

For each install, restore rehearsal, or upgrade, record the release and chart versions, context and namespace, values revision without secret contents, operator decision, command result, status output, controlled workflow result, and cleanup. Mark a path unverified when a step was not run. Use the [release verification guide](release-verification.md) to verify release artifacts, and [report a security issue](../SECURITY.md#reporting-a-vulnerability) through the security policy rather than a public operational log.
