# Evaluating Curie: a technical guide

Checked against release v0.14.3 and main at fe6812455fcae87ce44072b3e8157e931bc3eceb

This index distinguishes release-tag source evidence from checked-main changes and unrealized design. “Release/main” means the cited implementation was compared at both refs, not that a live deployment was tested. Status words follow the [capability reference](capabilities.md#reading-the-status); interface existence alone establishes no supported adoption path. Canonical documentation discrepancies belong to #3844.

## Components and deployment topology

Release/main: the [chart](../charts/curie/values.yaml) configures API, dispatcher, mail-adapter, worker, UI and backing services: Postgres, Valkey, Langfuse, ClickHouse, RustFS and OTel Collector. The [local stack](../compose.dev.yaml) uses Compose; the runner-only skill tier lacks the platform's durable state and approval plane ([deployment ownership](capabilities.md#deployment-ownership)). The [component map](../ARCHITECTURE.md#component-map), [message-flow](diagrams/message-flow.md), [Kubernetes](diagrams/kubernetes.md) and [ACI](diagrams/aci.md) diagrams are navigation aids, not release acceptance receipts.

The frozen contracts are the [ACI session protocol](../packages/aci-protocol) and [plugin bundle format](../packages/plugin-format). Frozen schema identity does not promise unchanged execution configuration. Release/main validates and unwraps archives, expands MCP declarations, omits absent optional-secret references, and composes platform tools, hooks and SDK settings ([`apps/api/src/curie_api/bundles.py::extract_and_validate`](../apps/api/src/curie_api/bundles.py), [`runner/src/curie_runner/plugin.py::bundle_mcp_servers`](../runner/src/curie_runner/plugin.py), [`runner/src/curie_runner/adapter.py::build_options`](../runner/src/curie_runner/adapter.py), [`runner/src/curie_runner/__main__.py`](../runner/src/curie_runner/__main__.py)). The stored archive stays immutable; effective configuration is generated, not byte-for-byte execution of that archive. This does not grant authority to rewrite the frozen format.

Main-only changes are identified below. Design limits include no general arbitrary-runtime compatibility and no native OpenAI harness; both are **Unavailable** in the [capability reference](capabilities.md#existing-agents-and-models), not merely presumed unreleased.

## One factory issue, end to end

Release/main: the API stores a [WorkItem and ExecutionRequest](../apps/api/src/curie_api/models.py); the tracker remains the backlog. The [canonical sequence](../ARCHITECTURE.md#one-factory-run-end-to-end) supplies context; step 4 distinguishes the changed bundle behavior.

1. Intake/admission. The [poller](../apps/api/src/curie_api/factory_poll_intake.py) is default; the signed [webhook](../apps/api/src/curie_api/github_factory.py) is opt-in. Installation, repository, sender permission and [base resolution](../apps/api/src/curie_api/factory_base.py) gate admission. Base-resolution refusal comments without creating a WorkItem; earlier refusals can return silently.
2. Records. [Admission](../apps/api/src/curie_api/workitems/lifecycle.py) writes a waiting WorkItem and bounded ExecutionRequest with a capacity-wait deadline.
3. Execution. The [reconciler](../apps/api/src/curie_api/workitem_reconciler.py) leases requests and publishes `curie:runs` wakes; the [worker](../apps/worker/src/curie_worker/run.py) defers the same request when capacity is unavailable. The sandbox receives scoped [issue-read authority](../apps/api/src/curie_api/routers/work_item_issue.py), not the publication credential.
4. Review/verification. [Release v0.14.3](https://github.com/curie-eng/curie/blob/v0.14.3/examples/dark-factory/README.md#review-loops) documents three-round review loops. Main-only: plan review is advisory and runs once; diff review permits five completed rounds, with time-budget gates ([bundle guide](../examples/dark-factory/README.md#review-loops), [`examples/dark-factory/hooks/review_gate.py`](../examples/dark-factory/hooks/review_gate.py)). Both versions enforce the bundle’s verification contract.
5. Publication. [Approval and publication](../apps/api/src/curie_api/crud/publications.py) are persisted together under per-agent operator policy ([ADR-0147](adr/0147-publication-approval-is-a-per-agent-operator-policy.md), Accepted). The [worker publication loop](../apps/worker/src/curie_worker/publication_loop.py) runs a push Job; the [API](../apps/api/src/curie_api/routers/publication_code_host.py) proves the commit and finds or opens the PR.
6. Outcome. The [CI gate](../apps/api/src/curie_api/factory_ci.py) observes the published head and reruns failed jobs once. The reconciler updates the status comment and labels; [outcomes](operations.md#reading-work-item-outcomes) distinguish waiting, running, published, cancelled and failed. Published does not assert correctness.

This describes implemented paths, not authorization from an unrealized ADR.

## Trust, tenant, credential, data and approval boundaries

Release/main source boundaries:

| Store or service | Holds or receives | Source |
| --- | --- | --- |
| Postgres | Agents, versions, work items, publications, approvals, memory | [Models](../apps/api/src/curie_api/models.py) |
| Valkey | Queues, streams and trace context | [Queue seam](interfaces/queue-stream/INTERFACE.md) |
| ClickHouse | Langfuse trace/event backend | [Chart](../charts/curie/values.yaml) |
| S3-compatible object store (RustFS) | Bundle archives and Langfuse events | [Storage seam](interfaces/blob-storage/INTERFACE.md), [chart](../charts/curie/values.yaml) |
| Kubernetes Secrets | Store, model, API, webhook and Slack credentials; per-agent connector secrets | [Secret template](../charts/curie/templates/secrets.yaml), [credential path](../ARCHITECTURE.md#the-credential-path) |
| Configured model provider | Prompts, model output, provider authentication | [Provider seam](interfaces/model-provider/INTERFACE.md) |
| Code host and tracker | Branches, PRs, issues, comments, labels | [Publication API](../apps/api/src/curie_api/routers/publication_code_host.py) |
| Slack (when connected) | Conversation messages and replies | [Dispatcher](../apps/dispatcher/src/curie_dispatcher/handlers.py) |

Local connector secret values can reside in agent JSONB; cluster values use per-agent Secrets ([Agent model](../apps/api/src/curie_api/models.py), [ADR-0009](adr/0009-per-agent-connector-auth.md), Accepted). [Approvals](approvals.md) are authorized in the API from authenticated resolver identity; publication has its own policy.

The execution boundary is a Kubernetes pod or Docker container ([substrates](interfaces/substrate/INTERFACE.md)); scoped capabilities limit platform access ([issue-read authority](../apps/api/src/curie_api/routers/work_item_issue.py)). Release/main cluster gVisor is conditional: `require` stamps a RuntimeClass and enforces preflight; real-model `auto` also requires it, while fake-model `auto` can omit it when absent; `off` disables it ([chart helper](../charts/curie/templates/_helpers.tpl)). With no explicit mode, the [CLI](../cli/src/ops/up.rs) can infer `off` from an absent or unschedulable RuntimeClass. [Pod hardening](../charts/curie/templates/agent-sandbox.yaml), when enabled, separately configures RuntimeDefault seccomp; seccomp is not gVisor kernel isolation. Network enforcement and budgets depend on configuration ([values](../charts/curie/values.yaml), [budget limits](capabilities.md#memory-approvals-and-cost-controls)).

Design gap, release/main: [ADR-0008](adr/0008-multi-tenancy.md) (Accepted) specifies database-forced RLS and tenant namespaces. Code has [tenant columns](../apps/api/alembic/versions/0101_tenant_scope_core.py) and [application filtering](../apps/worker/src/curie_worker/binding.py), but [database setup](../apps/api/src/curie_api/db.py) supplies no RLS policy and the sandbox template uses the release namespace. A default tenant does not establish the ADR's isolation boundary; see #3844. No additional main-only boundary is asserted here.

## Persistence, failure and recovery

Release/main behavior; no additional main-only recovery guarantee is asserted:

1. Retry. Streams deliver at least once with bounded dead-letter handling ([queue seam](interfaces/queue-stream/INTERFACE.md)); [factory settlement](../ARCHITECTURE.md#one-factory-run-end-to-end) reruns failed CI once, refusing unreadable CI as success.
2. Cancellation. Issue changes, deadlines or owner loss request termination ([stopping work](../ARCHITECTURE.md#one-factory-run-end-to-end)). [Overdue cancellation](../apps/api/src/curie_api/workitems/lifecycle.py) can settle without a teardown receipt; late receipts are stale.
3. Lease expiry. [Dispatch](../apps/api/src/curie_api/workitem_dispatch.py) redispatches lapsed unstarted acquisitions; expired runtime owners enter termination. Admission's capacity-wait deadline expires separately ([capacity waits](operations.md#factory-work-items-wait-for-capacity)).
4. Operator recovery. Inspect [work item outcomes](operations.md#reading-work-item-outcomes); use the documented [upgrade recovery](operations.md#upgrading-the-chart) for pending approvals and schema fences.

Design promises beyond these paths require separate implementation evidence.

## Extension points and their limits

Release/main inventory: the 31 [catalogued seams](interfaces.md#the-seams) describe implementations, not 31 supported capabilities. Status below is the corresponding [canonical capability](capabilities.md), where one is classified; “Not classified” is absence of a canonical classification, not an added status or release promise. No additional main-only implementation is claimed. Unbuilt adapter lifecycle and commissioning installation are design gaps, even where supporting code exists.

| Extension point | Implementations | Limit | Capability status |
| --- | --- | --- | --- |
| [Substrate](interfaces/substrate/INTERFACE.md) | Kubernetes, Docker | Deployment-owned | Not classified |
| [Harness](interfaces/harness-modelsession/INTERFACE.md) | Claude SDK | Arbitrary runtimes unsupported | Released with limits (existing skills) |
| [ACI producer](interfaces/aci-producer/INTERFACE.md) | Runner, reference | Frozen protocol | Not classified |
| [Channel ingress](interfaces/channel-ingress/INTERFACE.md) | Slack, HTTP, relay, tracker, CLI | Per-channel adoption limits | Released (Slack) |
| [Interaction message](interfaces/channel-interaction/INTERFACE.md) | Slack, terminal, mail, Discord | Renderer does not prove ingress | Released with limits (Discord) |
| [Model provider](interfaces/model-provider/INTERFACE.md) | Anthropic, compatible endpoints, Ollama | Native OpenAI: Unavailable | Released with limits |
| [Telemetry](interfaces/telemetry-otel/INTERFACE.md) | OTLP, Langfuse | Retention configured | Released with limits |
| [Evals](interfaces/evals/INTERFACE.md) | Graders, trajectory matcher | No LLM judge | Not classified |
| [Blob storage](interfaces/blob-storage/INTERFACE.md) | S3/RustFS | Non-S3 adapter deferred | Not classified |
| [Relational DB](interfaces/relational-db/INTERFACE.md) | Postgres | Postgres-specific behavior | Not classified |
| [Queue](interfaces/queue-stream/INTERFACE.md) | Valkey | Single backend | Not classified |
| [Bundle format](interfaces/bundle-format/INTERFACE.md) | Claude plugin schema | Effective config composed | Released with limits (existing skills) |
| [Approval](interfaces/approval/INTERFACE.md) | Six approver sets | Resolver authentication required | Released with limits |
| [Workflow state](interfaces/workflow-state/INTERFACE.md) | API, affinity store | No general typed port | Not classified |
| [Memory](interfaces/memory/INTERFACE.md) | Transcript-shaped loader, facts store | Automatic extraction: Unavailable | Released with limits |
| [Conversation history](interfaces/conversation-history/INTERFACE.md) | StateApiTranscriptStore | Per-thread replay | Not classified |
| [Triggers](interfaces/triggers/INTERFACE.md) | Hardcoded producers, cron | Not arbitrary scheduling | Not classified |
| [CLI output](interfaces/cli-output/INTERFACE.md) | Typed JSON outputs | CLI-specific | Not classified |
| [Harness package](interfaces/harness-package/INTERFACE.md) | Claude contribution | One supported harness | Not classified |
| [Connector host](interfaces/connector-host/INTERFACE.md) | Kubernetes, test fake | Cluster hosting | Not classified |
| [Sealed credential](interfaces/sealed-credential/INTERFACE.md) | Rust sealer, Python opener | Cluster custody | Not classified |
| [Port adapter](interfaces/port-adapter-service/INTERFACE.md) | Generic HTTP edges | Third-party lifecycle unbuilt | Not classified |
| [Publication lineage](interfaces/publication-lineage-authority/INTERFACE.md) | API authority/client | WorkItem lineage | Released with limits (factory) |
| [Cluster lifecycle](interfaces/cluster-lifecycle/INTERFACE.md) | CLI, Helm, kubectl | Operator-owned cluster | Not classified |
| [Model pricing](interfaces/model-pricing/INTERFACE.md) | OpenRouterPriceBook | Estimates, not billing | Released with limits |
| [Forge](interfaces/forge-tracker/INTERFACE.md) | GitHub, GitLab, Jira, Bitbucket | Only documented pairings | Released with limits |
| [MCP service auth](interfaces/mcp-service-auth/INTERFACE.md) | Deployment service identity | No general user delegation | Not classified |
| [Protected hook source](interfaces/protected-hook-source-authority/INTERFACE.md) | ProvisionedSourceAuthority | No supported installation | Unavailable (protected lane) |
| [Channel read](interfaces/channel-read/INTERFACE.md) | Slack history/canvas | Slack only | Not classified |
| [Mail provider](interfaces/mail-provider/INTERFACE.md) | AgentMail, entry-point providers | AgentMail authentication refused | Unavailable (AgentMail inbound) |
| [Commissioning network](interfaces/commissioning-network-producer/INTERFACE.md) | ProbeKit, LocalCiliumNetworkProducer | Not constructed by production roots | Unavailable (protected lane) |

The API [composition root](../apps/api/src/curie_api/main.py) registers protected source-policy routes; the [worker root](../apps/worker/src/curie_worker/run.py) does not construct commissioning producers. Neither implementation existence nor unbuilt lifecycle changes the broader capability's canonical status.

## Verification evidence

Release evidence is the [published release](https://github.com/curie-eng/curie/releases/tag/v0.14.3) plus compared tag sources, not fresh runtime acceptance. Main's [CI workflow](../.github/workflows/ci.yaml) defines checks; this page does not report their current results. [Release verification](release-verification.md) describes separate release authorization. [Historical live observations](../ARCHITECTURE.md#what-is-built-vs-deferred) are not guarantees for another deployment. Accepted ADRs record design decisions, not executed proof. Every work item's correctness remains bundle-owned, with `correctness.asserted: false` ([outcomes](operations.md#reading-work-item-outcomes)).
