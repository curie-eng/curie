# Inspection and evidence guidance

## Find the actual runtime path

Start with the requested outcome and trace from public entry point to changed implementation, downstream component, and externally visible result. Read enough unchanged callers and configuration to detect behavior that the PR diff alone hides. Check all supported paths that share the changed contract. An added file is not active functionality until a production caller or registration path reaches it.

For a feature slice, read linked issue bodies and explicit dependency or follow up PRs. Give the reader the whole feature goal, what this slice delivers, and what is still pending. Do not infer a roadmap from branch names or labels.

For each significant change, ask: What input selects this path? What state changes? What is returned or displayed? What happens on failure, retry, rollback, or a repeated run? What changes on a fresh installation versus an upgrade or existing deployment?

## Separate proof levels

| Evidence | Describe accurately |
| --- | --- |
| Automated end to end | Real product entry point and external behavior asserted; name any fake external dependency. |
| Integration or plan | Connected modules or a generated plan; do not call it a live outcome when no service was reached. |
| Unit | Decision or helper behavior; name negative cases and what the test does not execute. |
| Manual report | Who reported it, environment, observed result, and whether it was independently repeated. |
| CI | Check name, result, and head SHA; distinguish passing from skipped, pending, or stale. |
| Your run | Exact command, checkout SHA, relevant environment, result, and limitations. |

Read the test body, not only its name. State whether an assertion would fail if the claimed capability were absent. Tie material acceptance criteria to a test or explicit gap. A successful dry run, fixture, or mock does not establish production reachability.

## Explain architectural change

Use a small component table for PRs changing several files. Include service or deployment changes, CLI/API boundaries, configuration, persistence, and external integrations only when touched or affected. Distinguish added, modified, replaced, and deleted. A removed code path may be a representation change rather than removal of product behavior.

Use sequence diagrams to show the materially changed scenario, not every function call. Include an `alt` branch for fallbacks or failure behavior and a before/after view if that clarifies the decision. Label what persists beyond the command's lifetime. Avoid diagrams for pure prose changes or a single direct action.

For algorithmic changes, explain the rule with concrete inputs and outcomes. Mention computational complexity only if it materially changes or affects scaling. For data changes, inspect the old and new schema, migration order and dependency, handling of existing records, version compatibility, and rollback or irreversibility. Do not call an in memory enum a database migration.

Review the security posture of changed defaults. Check host and network binding, identity, authorization, credential handling, public endpoints, tenant boundaries, and whether access lasts only during a command or for a deployment lifetime. Inspect production configuration separately from development defaults. A loopback listener still admits other processes on the host; a port forward may require cluster RBAC and can bypass network policy. State the actual threat model and uncertainty rather than labeling every exposure a vulnerability.

## Links and findings

Prefer links to verified added or modified lines in the PR diff. Use the current head for line comments. Link unchanged dependencies to their source without implying they are in the PR. Explain when a conclusion is inferred from multiple sources.

For each actionable finding, give severity, triggering scenario, consequence, and a specific change or decision needed. Separate confirmed defect, design tradeoff, and missing proof. A disclosed limitation can still matter to the architect, but do not automatically describe it as an undisclosed bug. If none remain, state that plainly and name the most important behavior not verified.
