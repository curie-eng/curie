# Agents as co-workers

**Turn agents into co-workers. Run them in your infrastructure.**

A Curie co-worker is a deployed agent that receives delegated work, uses its
configured tools and context, and returns an inspectable result. Its bundle
specifies the job; the platform supplies versioned deployment, sandbox execution,
approval facilities, memory, tracing, and operational controls.

Start with the [Software Factory](software-factory.md). For current support and
integration limits, consult the
[capability reference](capabilities.md). This overview describes the collaboration
model; it is not a promise that every bundle implements every behavior.

## What collaboration looks like

| Behavior | Concrete example and boundary |
| --- | --- |
| Receive delegated work | A labelled issue from a supported tracker enters the factory; a workspace message starts an SRE investigation. Intake must be configured. |
| Inspect and clarify | The factory reads the issue and criteria. Ambiguous work can end with stated questions rather than a pull request. |
| Show progress and a result | The factory reports execution phases and returns a PR or a failure reason. Bundle and channel behavior determine the available response surface. |
| Use appropriate context | Memory can supply scoped records and facts. Writes depend on agent configuration and tools; memory is not automatic synthesis of every conversation. |
| Request human approval | The SRE bundle gates Kubernetes mutations. Approval is separate from connector RBAC and does not grant additional access. |
| Produce reviewable work | Factory plan and diff reviewers inspect work before publication. A reviewed PR is still subject to the repository's checks and human acceptance. |

A co-worker's autonomy depends on its configured tools and policies. Do not infer
an approval requirement for every tool from the presence of platform approvals,
or infer broad access from a human approving one action.

## Three ways to start

| Path | Bundle responsibility | Shared Curie platform |
| --- | --- | --- |
| [Software Factory](software-factory.md) | Implement an issue, run bounded review loops, and publish a PR or a reason for failure. | Supported tracker intake, code-host publication controls, execution, model configuration, and cost visibility. |
| [SRE co-worker](../examples/sre-bot/README.md) | Investigate using Grafana, Tempo, and the pinned Kubernetes connector. | Deployment, sandbox execution, approval routes, traces, and memory facilities. |
| [Bring your own agent](bring-your-agent.md) | Package supported skills, instructions, MCP tools, and evals. | The same versioned bundle and operational lifecycle. Arbitrary existing runtimes are not drop-in compatible. |

`dark-factory` remains the factory's implementation identifier. User-facing
Software Factory terminology does not change CLI names, bundle paths, or APIs.
The SRE example's exact authority model is documented in its README and remains
specific to that bundle.

## Who controls the work

Developers choose a bounded task and inspect its result. Engineering leads
compare useful completed outcomes, human review effort, elapsed time, and cost.
Administrators configure deployment, identities, permissions, approval routes,
provider settings, and budgets. These roles can overlap in a small team, but
platform access and tool authority still need explicit configuration.

Choose supported implementation and reviewer models explicitly. Observe actual
costs before changing those choices. Curie does not currently promise automatic
routing by task complexity or an ROI calculation without a comparison baseline.

## Workspace and data boundaries

Slack is the primary documented workspace path. Other adapters have different
configuration, deployment, security, and verification limits. In particular,
the AgentMail inbound path currently rejects messages when sender authentication
cannot be verified. Teams has no supported first-party channel. Use the
[capability reference](capabilities.md) for channel rollout limits and the
scope of GitLab, Jira Cloud, and Bitbucket factory integrations.

The platform and its backing services can run in your infrastructure. Configured
external model providers, issue trackers, code hosts, and workspace services receive the data needed
for their integrations. A supported local model endpoint changes the model data
boundary, but does not remove other configured external destinations.

For technical detail, continue to [architecture](../ARCHITECTURE.md),
[approvals](approvals.md), or the [vision](vision.md).
