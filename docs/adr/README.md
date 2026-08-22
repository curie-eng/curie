# Architecture Decision Records

Every architecture decision in Curie is recorded here as a numbered ADR. An ADR
captures **what was decided and when**, along with the reasoning and the
alternatives weighed at that time.

ADRs are immutable once Accepted. When the thinking changes you add a **new ADR
that supersedes** the old one rather than editing it, so the chain of
supersessions is the history of how intent shifted — not just a snapshot of the
current state.

## Claiming a number

**The next free number is one past the last row of the table below.** Read it
from this index, not from an `ls` of your branch: a branch that has not been
rebased shows a stale tail, and every number collision this repo has had came
from exactly that. Check open PRs for a number already reserved off-main before
claiming one.

The number-prefix uniqueness of `docs/adr/` is gated in CI (`scripts/check-docs.sh`),
so a collision fails the build rather than reaching main.

## The index

The table below is generated from the ADR files themselves by
`curie_doclint` — number from each filename, title from its heading, status
read verbatim from its `Status:` line. **Do not hand-edit it.** Run
`bash scripts/check-docs.sh` to regenerate, then commit the result.

<!-- BEGIN GENERATED: adr-index (do not edit by hand; run scripts/check-docs.sh) -->
| # | Decision | Status |
|---|---|---|
| 0001 | [Record architecture decisions](0001-record-architecture-decisions.md) | Accepted |
| 0002 | [Kubernetes Agent Sandbox as the interactive runtime substrate](0002-kubernetes-agent-sandbox-as-runtime-substrate.md) | Accepted |
| 0003 | [Stateless-first sessions; rehydrate on resume; no cross-hibernation cache assumption](0003-stateless-first-rehydrate-on-resume.md) | Accepted |
| 0004 | [Langfuse as the single observability + eval backbone](0004-langfuse-observability-and-eval-backbone.md) | Accepted |
| 0005 | [claude-agent-sdk adapter behind a frozen ACI session contract](0005-claude-agent-sdk-adapter-and-frozen-aci.md) | Accepted |
| 0006 | [Security rails are chart defaults, not hardening backlog](0006-security-rails-as-chart-defaults.md) | Accepted |
| 0007 | [Adopt-not-build boundaries; build only five things](0007-adopt-not-build-boundaries.md) | Accepted |
| 0008 | [Multi-tenancy: one code path, pooled RLS, hard-siloed compute](0008-multi-tenancy.md) | Accepted |
| 0009 | [Per-agent secrets and connector credentials](0009-per-agent-connector-auth.md) | Accepted |
| 0010 | [Approval gates and human-in-the-loop](0010-approval-gates-and-human-in-the-loop.md) | Accepted |
| 0011 | [OpenCode as the second harness behind the ACI](0011-opencode-second-harness.md) | Superseded by [ADR-0060](0060-the-harness-is-a-declared-package.md) (the steer spike passed; adoption was withdrawn for other reasons, recorded there) |
| 0012 | [A substrate-agnostic worker and a channel-agnostic runner (the thin-shim thesis)](0012-substrate-and-channel-agnostic-core.md) | Accepted |
| 0013 | [Concurrency and delivery: at-least-once streams with an idempotent, side-effect-aware kernel](0013-concurrency-and-delivery-model.md) | Accepted |
| 0014 | [A git push is the deploy: immutable bundles, promote-not-rebuild, evals as a CI gate](0014-git-push-is-the-deploy.md) | Accepted |
| 0015 | [The credential plane: no broker, prefix-mapped, fail-loud and fail-closed](0015-credential-plane.md) | Accepted |
| 0016 | [Swappable jobs around an opinionated core; the second implementation teaches the interface](0016-swappable-jobs-around-an-opinionated-core.md) | Accepted |
| 0017 | [Tri-language frozen contracts: Pydantic as source of truth, generate the rest, gate on drift](0017-tri-language-contract-codegen.md) | Accepted |
| 0018 | [Greeting/help pre-model short-circuit in the kernel](0018-greeting-help-pre-model-short-circuit.md) | Accepted |
| 0019 | [Freeze the eval-case format: one schema, frozen in place under apps/worker](0019-freeze-eval-case-format.md) | Accepted |
| 0020 | [The message port: a rendering-free channel interface with capability negotiation](0020-message-port-rendering-free-channel-interface.md) | Accepted |
| 0021 | [Curie is a harness for coding agents: the CLI's primary user is Claude Code](0021-agentos-is-a-harness-for-coding-agents.md) | Accepted |
| 0022 | [Eval completeness: run the same evals at every tier, grade what actually happened, and promote real traces into cases](0022-eval-completeness-tier-parity-and-trace-promotion.md) | Accepted |
| 0023 | [Controller NetworkPolicy RBAC: cluster-scope read, namespace-scope mutate](0023-controller-networkpolicy-rbac-cluster-read-namespace-mutate.md) | Accepted |
| 0024 | [`cluster deploy` reaches the platform API via the UI `/api` proxy](0024-deploy-reaches-api-via-ui-proxy.md) | Accepted |
| 0025 | [The memory port and its first loader: a scoped namespace over the durable state store](0025-memory-port-and-first-loader.md) | Accepted |
| 0026 | [Extract the storage port now; defer the second (GCS/Azure) adapter](0026-extract-storage-port-defer-second-adapter.md) | Accepted |
| 0027 | [A thin broker port at the non-sacred seams; defer the second broker](0027-thin-broker-port-defer-second-broker.md) | Accepted |
| 0028 | [The sandbox substrate is a resilience-only fallback, not a product swap axis](0028-substrate-is-resilience-fallback-not-product-swap-axis.md) | Accepted |
| 0029 | [The conversation-history port and its first loader: transcript replay over the durable state store](0029-conversation-history-port-and-first-loader.md) | Accepted |
| 0030 | [A proactive within-episode memory agent over the frozen ACI injection seam](0030-proactive-within-episode-memory-agent.md) | Draft |
| 0031 | [Harness-neutral runner seams for the second harness](0031-harness-neutral-runner-seams.md) | Superseded by [ADR-0060](0060-the-harness-is-a-declared-package.md) and [ADR-0061](0061-out-of-process-harness-boundary.md) (decisions 2/3/5 become manifest fields in 0060; decision 4 carries forward unchanged; decision 1 becomes 0061's recorded fallback) |
| 0032 | [Explicit named-provider egress, resolved at install](0032-explicit-provider-egress.md) | Accepted |
| 0033 | [Scoped, least-privilege sandbox state token](0033-scoped-sandbox-state-token.md) | Accepted |
| 0034 | [Approval authorizers resolve membership in the API](0034-approval-authorizers-resolve-membership-in-the-api.md) | Accepted |
| 0035 | [One-shot post-approval allowance at resume boot](0035-one-shot-post-approval-allowance.md) | Accepted |
| 0036 | [ACI semver, reader-policy asymmetry, and a wire-lock gate that enforces it](0036-aci-semver-and-reader-policy.md) | Accepted |
| 0037 | [Opt-in binding hook and Pareto model routing](0037-opt-in-binding-hook-and-pareto-model-routing.md) | Accepted |
| 0038 | [An observability CLI helper for the agent-dev loop](0038-observability-cli-helper-for-the-agent-dev-loop.md) | Accepted |
| 0039 | [Bounded delivery: a delivery cap and a dead-letter graveyard](0039-bounded-delivery-and-a-dead-letter-graveyard.md) | Accepted |
| 0040 | [Adopt the Agent Client Protocol as an edge projection](0040-adopt-acp-as-an-edge-projection.md) | Accepted |
| 0041 | [Every verb is answered at every tier](0041-every-verb-is-answered-at-every-tier.md) | Accepted |
| 0042 | [Adopt LLM-as-a-Verifier: a continuous-score semantic grader and a live progress signal](0042-llm-as-a-verifier-grader-and-progress-signal.md) | Accepted |
| 0043 | [The interface catalog is generated from declared front-matter and gated in CI](0043-generated-interface-catalog-and-doc-lint-gate.md) | Accepted |
| 0044 | [Workflow-controlled agents run on Curie as a callable substrate first, a unified plane only on demand](0044-workflow-controlled-agents-callable-substrate.md) | Accepted |
| 0045 | [The status line is the mutable part of an immutable ADR](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md) | Accepted |
| 0046 | [Converged approval gates: durable provenance, loud route resolution, and observe-only resume reconciliation](0046-converged-approval-gates-and-durable-provenance.md) | Accepted |
| 0047 | [ADR-0047: Canonical env-var names for the API base URL and model credential](0047-canonical-env-var-names.md) | Accepted |
| 0048 | [ADR-0048: Declare the model endpoint's wire protocol and credential keys](0048-declared-model-wire-protocol-and-credential-keys.md) | Accepted |
| 0049 | [The boot env is a declared contract: BootEnv, producer ownership, and generated env keys](0049-boot-env-contract.md) | Accepted |
| 0050 | [A declared approval policy is armed exactly as declared, or the runner refuses to boot](0050-declared-approval-policy-is-armed-exactly-or-not-at-all.md) | Accepted |
| 0051 | [Eval cases run in a fresh conversation by default, with per-case opt-in shared history](0051-eval-cases-run-in-a-fresh-conversation-by-default.md) | Accepted |
| 0052 | [The release trust model: one signed manifest, closed-world coverage](0052-release-asset-trust-model.md) | Accepted |
| 0053 | [Add an expected terminal status to the frozen eval-case format](0053-expected-terminal-status-in-eval-cases.md) | Accepted |
| 0054 | [Local Docker runner containers are hardened and network-isolated](0054-local-docker-runner-hardening.md) | Accepted |
| 0055 | [The fake model is a plumbing fixture, not a subject under test](0055-the-fake-model-is-a-plumbing-fixture.md) | Accepted |
| 0056 | [An operator opt-in makes a policy gate grantable; a heuristic never does](0056-operator-opt-in-for-policy-gate-grantability.md) | Accepted |
| 0057 | [`cluster deploy` self-plumbs a port-forward so the generated key stays off the cleartext proxy](0057-cluster-deploy-self-plumbs-port-forward-for-generated-key.md) | Accepted |
| 0058 | [A pushed tag is not release authority: ancestry, checks, and an approval environment gate publication](0058-tag-push-is-not-release-authority.md) | Accepted |
| 0059 | [The sandbox is a bounded resource envelope; capacity is a tenant boundary](0059-sandbox-is-a-bounded-resource-envelope.md) | Accepted |
| 0060 | [The harness is a declared package, not a class](0060-the-harness-is-a-declared-package.md) | Accepted |
| 0061 | [The harness boundary is an out-of-process adapter, wire-compatible with Omnigent](0061-out-of-process-harness-boundary.md) | Draft |
| 0062 | [Harness conformance has teeth](0062-harness-conformance-has-teeth.md) | Accepted |
| 0063 | [Message-driven approval reply surface](0063-message-driven-approval-reply-surface.md) | Accepted |
| 0064 | [Fail-forward cluster teardown](0064-fail-forward-cluster-teardown.md) | Accepted |
| 0065 | [Tag protection, not the workflow gate, binds a write actor](0065-tag-protection-not-the-workflow-gate-binds-a-write-actor.md) | Accepted |
| 0066 | [A skip propagates through the whole ancestor graph, so every publishing job carries its own guard](0066-a-skip-propagates-through-the-whole-ancestor-graph.md) | Accepted |
| 0067 | [The runner SandboxTemplate sets networkPolicyManagement: Unmanaged so Rail 1 is not additively defeated](0067-controller-networkpolicymanagement-unmanaged-for-rail-1.md) | Accepted |
| 0068 | [A `--model` sweep row with zero completed turns fails loudly instead of reporting 0%](0068-eval-sweep-fails-loudly-on-zero-completed-turns.md) | Accepted |
| 0069 | [The policy-gate resume reconciliation stays observe-only](0069-policy-gate-resume-reconciliation-stays-observe-only.md) | Accepted |
| 0070 | [A bundle-local `.env` is an opt-in, lowest-priority credential source](0070-bundle-local-dotenv-is-an-opt-in-lowest-priority-credential-source.md) | Accepted |
| 0071 | [Adopting a pre-plugin bundle scaffolds the skeleton, not the logic](0071-adopting-a-pre-plugin-bundle-scaffolds-the-skeleton-not-the-logic.md) | Accepted |
| 0072 | [Keep the hand-rolled Rust ACI emitter; typify cannot express the wire contract's runtime semantics](0072-keep-the-hand-rolled-rust-aci-emitter-over-typify.md) | Accepted |
| 0073 | [The durable state store reaches bundle code as an auto-mounted MCP server, not a bundle-shipped one](0073-agentos-state-mcp-server-and-state-boot-env.md) | Accepted |
| 0074 | [Versioned JSON Schemas for every agent-facing CLI result](0074-versioned-json-schemas-for-cli-results.md) | Accepted |
| 0075 | [The Agent Proxy: credentials and egress leave the sandbox](0075-the-agent-proxy-credential-and-egress-boundary.md) | Draft |
| 0076 | [A closed, versioned attribute schema for the runner's OTel span stream](0076-closed-typed-telemetry-attribute-schema.md) | Accepted |
| 0077 | [Skill-tier durable approvals stay unavailable, reported not absent](0077-skill-tier-durable-approvals-stay-unavailable.md) | Accepted |
| 0078 | [Route message-driven approval cards through the connected Slack transport](0078-approval-cards-over-connected-transport.md) | Accepted |
| 0079 | [Inbound triggers as a new event kind, ingested by the API](0079-inbound-triggers-as-a-new-event-kind.md) | Accepted |
| 0080 | [Rename the project to Curie](0080-rename-to-curie.md) | Accepted |
| 0081 | [A nightly graded parity ladder verifies the real-model promise CI cannot](0081-nightly-graded-parity-ladder.md) | Accepted |
| 0082 | [Connected-transport approval cards ride a real CLI-posted placeholder](0082-connected-transport-approval-cards-via-a-real-placeholder.md) | Accepted |
| 0083 | [Console sessions and CLI-minted login codes](0083-console-sessions-and-cli-minted-login-codes.md) | Accepted |
| 0084 | [Publish a verification contract for agents driving Curie](0084-publish-a-verification-contract-for-agents-driving-curie.md) | Accepted |
| 0085 | [Acceptance, not implementation, authorizes an ADR](0085-acceptance-not-implementation-authorizes-an-adr.md) | Accepted |
| 0086 | [Bundles declare connectors; the platform hosts them](0086-bundles-declare-connectors-the-platform-hosts-them.md) | Accepted |
| 0087 | [The API renders connector objects; the CLI applies them](0087-the-api-renders-connector-objects-the-cli-applies-them.md) | Accepted |
| 0088 | [Per user delegated OAuth for MCP](0088-per-user-delegated-oauth-for-mcp.md) | Accepted |
| 0089 | [Bundles declare their deploy targets](0089-bundles-declare-their-deploy-targets.md) | Accepted |
| 0090 | [A reconciler applies connectors, so agent repos need no CLI](0090-a-reconciler-applies-connectors-so-agent-repos-need-no-cli.md) | Accepted |
| 0091 | [Git-flow resolves deploy targets, so one repo serves many agents](0091-git-flow-resolves-deploy-targets-so-one-repo-serves-many-agents.md) | Accepted |
| 0092 | [A GitHub App gives the platform its own repository identity](0092-a-github-app-gives-the-platform-its-own-repository-identity.md) | Accepted |
| 0093 | [Local-model assets are pre-provisioned, never implicitly downloaded](0093-local-model-assets-are-pre-provisioned-never-implicitly-downloaded.md) | Accepted |
| 0094 | [A bundle carries its own sealed connector keys](0094-a-bundle-carries-its-own-sealed-connector-keys.md) | Accepted |
| 0095 | [One tiered memory lifecycle for agent and channel memory](0095-tiered-memory-lifecycle.md) | Draft |
| 0096 | [A third-party port adapter is a deployed service, not a loaded plugin](0096-port-adapters-are-deployed-services.md) | Accepted |
| 0097 | [One file declares an installation](0097-one-file-declares-an-installation.md) | Accepted |
| 0098 | [Thinking depth is an operator knob, never a bundle one](0098-thinking-depth-is-an-operator-knob-never-a-bundle-one.md) | Accepted |
| 0099 | [Hooks are bundle-declared turns the system starts](0099-hooks-are-bundle-declared-turns-the-system-starts.md) | Draft |
| 0100 | [Agents search their own surface through the channel port](0100-agents-search-their-own-surface-through-the-channel-port.md) | Draft |
| 0101 | [Closed schemas version on every change: minor for optional, major for required](0101-schema-compatibility-for-closed-schemas.md) | Accepted |
| 0102 | [Accepted alongside implementation with explicit approval](0102-accepted-alongside-implementation-with-explicit-approval.md) | Accepted |
| 0103 | [Previous schema shape gate](0103-previous-schema-shape-gate.md) | Accepted |
| 0104 | [A named security posture is computed from the layers that exist, never configured](0104-a-named-security-posture-is-computed-not-configured.md) | Accepted |
| 0105 | [External native session checkpoints are a harness capability](0105-external-native-session-checkpoints.md) | Draft |
| 0106 | [An approver is an authenticated principal, never a caller-asserted string](0106-an-approver-is-an-authenticated-principal.md) | Draft |
| 0107 | [An agent reads its own runs through a first-party surface, scoped by structured identity](0107-an-agent-reads-its-own-runs-through-a-first-party-scoped-surface.md) | Draft |
| 0108 | [A deploy target carries its environment's values](0108-a-deploy-target-carries-its-environments-values.md) | Draft |
| 0109 | [Retention, capacity, and back pressure are one policy: a claim is admitted, not discovered](0109-retention-capacity-and-back-pressure-are-one-policy.md) | Draft |
| 0110 | [Deterministic security posture classification distinguishes manifest declarations from operator gates](0110-deterministic-security-posture-classification.md) | Draft |
| 0111 | [The default memory compaction algorithm](0111-the-default-memory-compaction-algorithm.md) | Draft |
| 0113 | [Bundles declare connector build inputs and tiers deliver pinned images](0113-bundles-declare-connector-build-inputs-and-tiers-deliver-pinned-images.md) | Accepted |
| 0116 | [Session identity arrives over the ACI so a sandbox can be pre-bound](0116-session-identity-arrives-over-the-aci-so-a-sandbox-can-be-pre-bound.md) | Draft |
| 0119 | [A resumed thread rebuilds its prefix so the prompt cache still hits](0119-a-resumed-thread-rebuilds-its-prefix-so-the-prompt-cache-still-hits.md) | Draft |
<!-- END GENERATED: adr-index -->
