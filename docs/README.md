# Documentation

Curie turns supported agents into co-workers in team workspaces. Start with
Software Factory to delegate GitHub issues, or bring your existing skills and
tools onto the same platform. [Capabilities and release status](capabilities.md)
separates what you can deploy from what is planned.

## Living documentation

The guides below describe current workflows. Historical pre-build plans remain
in git history. Architecture and interface references explain implementation;
they are not substitutes for a supported quickstart.

## Get started

- [Software Factory](software-factory.md): the primary engineering-team path,
  from assigned issue to reviewed PR or a clear failure reason.
- [Factory quickstart](guides/dark-factory-quickstart.md): install the released
  factory, create/install your GitHub App, label an issue, and inspect the result.
- [Bring your agent](bring-your-agent.md): compatibility and reuse of supported
  skills and tools.
- [SRE co-worker](../examples/sre-bot/README.md): investigate Kubernetes workloads
  and request approval for configured mutations.
- [Quickstart chooser](../QUICKSTART.md#choose-your-starting-point): select a use
  case or try a local scripted first reply without a workspace.
- [Release verification](release-verification.md): verify CLI checksums,
  signatures, provenance, and SBOMs.
- [Agent install](agent-install.md): unattended release installation and result
  reporting for a coding agent.

## Use co-workers

- [Co-worker platform](co-workers.md): delegation, workspace interaction, memory,
  approvals, and the shared platform beneath the examples.
- [First Slack agent](your-first-slack-agent.md): connect a supported bundle to a
  real Slack workspace.
- [Factory outcomes and costs](guides/measuring-factory-outcomes.md): evaluate
  completed work, estimated spend, and human review effort.
- [Behavior packs](behavior-packs.md): opt-in greetings, capability tips, and
  progress behavior around a turn.
- [Local models](local-model.md): opt-in Ollama evaluation, sizing, and limits.

## Bring your own agent

- [Adoption guide](bring-your-agent.md): what can be reused and what must change.
- [Legacy bundle conversion](adopting-a-bundle.md): the specific migration from
  an older Python/Slack project into a plugin bundle.
- [Bundle format](../packages/plugin-format/README.md): validated instructions,
  tools, MCP configuration, and optional bundle artifacts.
- [In-bundle tools example](../examples/text-stats-engine/README.md): reuse a
  deterministic engine through a stdio MCP tool.
- [Repository toolchain](guides/repository-toolchain-in-the-managed-sandbox.md):
  dependencies and verification in a managed sandbox.
- [Reversible connectors](writing-a-reversible-connector.md): connector authoring
  and the supported recovery contract.

## Administer Curie

- [Administrator guide](administering-co-workers.md): access, approvals, model
  configuration, budgets, memory, inspection, and stopping work.
- [Approvals](approvals.md): route binding, approver authority, and resumed turns.
- [Operations](operations.md): cluster installation, credentials, egress, factory
  operation, upgrades, and operational limits.
- [Memory contract](interfaces/memory/INTERFACE.md): agent/channel scopes, read
  and write credentials, and operator management.
- [Thinking and model costs](thinking.md): reasoning latency and configuration.
- [Slack local runbook](slack-local-runbook.md): real workspace testing against a
  local Compose deployment.
- [Security](../SECURITY.md): trust model and vulnerability reporting.

## Develop and contribute

- [Contributor onboarding](onboarding.md): fresh source checkout to a verified
  local turn; separate from adopting the released product.
- [Contributing](../CONTRIBUTING.md) and [repository rules](../AGENTS.md): setup,
  affected checks, and review requirements.
- [CLI reference](../cli/README.md): commands and flags across targets.
- [Agent verification contract](agents.md): exact commands and evidence for
  an agent operating Curie.
- [Vision](vision.md): product identity, audiences, and technical commitments.
- [Pillars](pillars.md): issue/PR taxonomy for contributor work.

## Architecture and design

- [As-built architecture](../ARCHITECTURE.md): component and message/deploy flows,
  with implementation references and status. [architecture.md](architecture.md)
  is the redirect to that canonical reference.
- [Diagrams](diagrams/): [message flow](diagrams/message-flow.md),
  [Kubernetes](diagrams/kubernetes.md), [agent interface](diagrams/aci.md), and
  [seams](diagrams/seams.md).
- [Architecture vision](architecture-vision.md) and [interface catalog](interfaces.md):
  component boundaries, current implementations, and swap readiness.
- [Shared developer cluster](design/multi-dev-shared-cluster.md): design and
  limits for concurrent development on one cluster.
- [ADRs](adr/): durable architecture decisions and their evidence.

Forward-looking work lives in [GitHub issues](https://github.com/curie-eng/curie/issues),
with larger journeys labelled `epic`.
