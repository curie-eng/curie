# Build on Curie and contribute

Curie can run a Software Factory for your repositories and can package supported
existing skills and tools as a co-worker. The path depends on whether you need
to change agent behavior, the tools available to factory work, or the runtime
that isolates that work. Check the [capability status](../capabilities.md) for
the release you install before relying on a path in production.

| Your goal | Start here | Current boundary |
| --- | --- | --- |
| Use local coding agent skills to prepare factory issues | [Factory preparation](../software-factory.md#bring-your-existing-local-workflow) | Use the resulting specification as issue or repository material the factory can read. A local skill or conversation is not transferred automatically. |
| Reuse locally developed instructions, skills, scripts or MCP tools | [Bring your agent](../bring-your-agent.md) and the [working tool example](../../examples/text-stats-engine/README.md) | Adapt them into a validated Curie bundle. Curie runs the model loop; an existing agent service cannot attach unchanged. |
| Adapt the default factory workflow | [Factory bundle reference](../../examples/dark-factory/README.md#deploy-it-as-the-factory-agent) | Render and review the bundle and its deployment requirements. Changes to platform intake or publication need platform work. |
| Use a different agent runtime | [Harness contract](../interfaces/harness-package/INTERFACE.md) | The shipped harness uses the Claude Agent SDK. Another runtime needs platform integration and verification, not only a bundle setting. |
| Give the factory a repository specific toolchain | [Factory environment guide](factory-environments.md) and [repository runner reference](../../cli/README.md#repository-runner-images) | An administrator can build, probe and bind a digest pinned runner image for a repository. This changes the runner environment, not the sandbox substrate. |
| Use a different sandbox substrate | [Substrate interface](../interfaces/substrate/INTERFACE.md) and [architecture decision](../adr/0028-substrate-is-resilience-fallback-not-product-swap-axis.md) | Kubernetes, local Docker and a [static pool](../static-pool-substrate.md) have implementations. Another substrate needs worker integration, deployment wiring and verification. The interface alone is not a plug in installation path. |
| Add a workspace channel or external tool | [Channel adapter guide](building-a-channel-adapter.md) or [connector guide](deciding-what-belongs-in-a-connector.md) | Check the [supported integration list](../capabilities.md#workspaces-and-integrations) and the relevant interface before promising an adoption path. |

## Bring an existing agent or skill

Skills in your local coding agent can help research and prepare an issue for the
factory. Put decisions and requirements in the issue or repository material the
factory can access. The [factory workflow](../software-factory.md#bring-your-existing-local-workflow)
explains this handoff.

To run an existing agent as a Curie co-worker, start with
`curie init --adopt ./my-existing-agent` for an existing directory or copy the
[text analysis example](../../examples/text-stats-engine/README.md) to preserve
existing tool code behind an MCP wrapper. Put supported instructions in bundle
skills and declare tools through MCP. Run `curie skill check --json`, then run a
representative task with a real model and inspect the observed tool call and
result. The [adoption guide](../bring-your-agent.md) gives the full local and
workspace verification sequence. A valid skill file alone does not prove the
agent can complete a task in a deployed workspace.

## Change the factory execution environment

If a repository needs another executable, dependency version or build tool,
first use the [factory environment setup guide](factory-environments.md). It
describes an operator owned runner image per repository binding, probes, a
digest pinned deployment and rollback. Check network access, credentials and
verification commands inside a sandbox claim; an image build cannot prove
those conditions. The [managed sandbox toolchain guide](repository-toolchain-in-the-managed-sandbox.md)
covers repository dependencies and checks.

If the requirement is instead to run the worker on another kind of sandbox,
read the [substrate contract](../interfaces/substrate/INTERFACE.md), the
[static pool design](../static-pool-substrate.md) and the
[architecture decision](../adr/0028-substrate-is-resilience-fallback-not-product-swap-axis.md).
The worker currently selects its built in Kubernetes, Docker or static pool
implementation. Adding another runtime changes platform code and operations.
The contract describes claim lifecycle, routing and cleanup, but it also records
behavior that differs between implementations, including secret delivery,
workspace staging and pool selection. Propose the concrete runtime and the
needed guarantees in an issue before implementing it.

## Report a gap or contribute a change

Search [existing issues](https://github.com/curie-eng/curie/issues) first. For a
missing capability, [open an issue](https://github.com/curie-eng/curie/issues/new)
that describes the desired outcome, the current path you tried, the gap, and a
way to verify a proposed solution. For a bug, include the Curie version or
commit, environment, reproduction steps, expected result and observed result.
Keep customer identities, deployment details and credentials out of this public
repository. See [SUPPORT.md](../../SUPPORT.md) for questions and private security
reports.

For a proposed change, read [CONTRIBUTING.md](../../CONTRIBUTING.md) and the
relevant area instructions before coding. Open an issue for work larger than a
small fix or documentation edit, especially when an interface or cross component
boundary changes. A pull request should link the issue, explain the user facing
behavior, and include the checks required by the touched area. The
[interface catalog](../interfaces.md) and [as built architecture](../../ARCHITECTURE.md)
help locate code ownership; they do not turn every internal seam into a
supported extension API.
