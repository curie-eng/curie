# Vision

Curie turns agents into co-workers. Teams delegate work where they already
collaborate, supervise consequential actions, and operate the platform in their
own infrastructure. This document describes the direction of the project;
[capabilities](capabilities.md) describes current support, and
[GitHub issues](https://github.com/curie-eng/curie/issues) tracks implementation.

## What Curie is

An open-source platform for deploying and operating AI co-workers: agents with
an identity, tools, context, and a way to collaborate with people on delegated
work. The Software Factory is the first developer adoption path. It takes an
assigned GitHub issue through implementation and plan and diff review to a pull
request, or reports why it could not complete the work.

The [SRE co-worker](../examples/sre-bot/README.md) and
[supported custom agents](bring-your-agent.md) use the same platform. Their
skills and permissions differ, while versioned deployment, sandbox execution,
approval infrastructure, memory facilities, and operational visibility are
shared. [Co-workers](co-workers.md) explains the observable collaboration model.

## Why it exists

An agent that can generate an answer is only part of a useful team workflow.
Someone must delegate the task, supply appropriate context and permissions,
inspect progress and cost, approve restricted actions, and accept the result.
Assembling those controls independently for every agent makes adoption harder
and oversight inconsistent.

Curie supplies a shared operating platform while keeping the agent's skills and
tools in an inspectable bundle. Customers own their deployment and choose its
configured providers. External models and workspace services remain data
boundaries that administrators must account for; self-hosting the platform does
not make every connected service local.

## Who it is for

- **Developers** delegate bounded issues, review pull requests, or package their
  existing supported skills and tools as co-workers. They need a clear first
  outcome and a repeatable local test loop.
- **Engineering leads** decide which work to delegate and evaluate accepted
  results, review effort, elapsed time, and cost against a stated baseline.
- **Administrators and operators** configure identities, credentials,
  permissions, approval routes, models, budgets, and deployment lifecycle.
- **Teammates** interact with deployed co-workers through supported workspaces,
  provide clarification, and inspect or approve work within their authority.

Developer adoption is the starting point. Software Factory provides a concrete
reason to install Curie, and other co-workers provide a path to expand its use.

## What makes it Curie

These commitments guide new work:

- **Work happens where the team collaborates.** Delegation, progress, questions,
  approvals, and results should fit the team's supported workspaces. New channel
  adapters are a direction, not a claim that every channel is supported today.
- **Customer ownership.** The platform is Apache-2.0 and runs in customer
  infrastructure. Sandboxes, scoped credentials, and explicit permissions make
  the deployment boundary inspectable.
- **Human supervision.** Restricted actions require the configured approval
  policy. Approval records intent; underlying tool authority still determines
  what an action can do. The human remains responsible for accepting results.
- **Appropriate memory.** Retain useful context with clear scope and provenance.
  Shared memory must respect access boundaries. Current facilities do not imply
  automatic synthesis of all conversations or universal cross-channel recall.
- **Visible economics.** Expose model configuration, budgets, traces, and costs
  so teams can evaluate work. Model choice should match the task; automatic
  complexity routing and guaranteed savings are not current promises.
- **The local loop supports the production loop.** Use versioned, immutable
  bundles and the same eval suite across skill, local, and cluster tiers.
  Environment parity helps catch deployment differences, but cannot guarantee
  production model behavior or that independently configured tiers are identical.
- **Git is the deployment model.** Keep agent instructions, tools, and evals
  reviewable as code, with versioning, promotion, and rollback through the
  platform's deployment workflow.
- **Opinionated defaults, explicit seams.** Provide supported harness, channel,
  storage, and provider integrations without requiring each adopter to assemble
  the stack. A declared seam alone does not make an arbitrary runtime compatible.
- **Agent-operable tools.** The skill, local, and cluster CLI surfaces have
  structured, non-interactive commands that coding agents can drive. The guided
  human walkthrough remains a supported adoption path. `curie dev` is internal
  contributor tooling, outside that product-tier contract.

The [eight pillars](pillars.md) remain the contributor taxonomy for these
concerns. This vision does not introduce a parallel issue-label system.

## What it could become

The near-term focus is a Software Factory developers can try, supervise, and
measure. A successful first run produces an inspectable work product and a clear
record of the work and its costs, rather than an unexplained completion claim.

The longer direction is a shared platform for co-workers across engineering and
other team workflows. Supported existing skills and tools can become deployed
co-workers without every team recreating supervision and operations. Expand
integrations and runtime support through implemented, verified contracts.

The test for a feature is whether it makes delegated work more useful, human
supervision clearer, or customer ownership stronger. Current shipped behavior
belongs in the capability reference, not in this future-facing statement.
