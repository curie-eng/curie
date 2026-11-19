---
name: review-pr
description: Review a pull request for an architect or technical lead. Explain its purpose, system impact, runtime scenarios, tests, algorithms, data changes, and consequential risks with links to the current diff. Use for holistic PR reviews and explanations, not implementation.
---

# Architecture review of a pull request

Produce an explanation that helps an architect decide what to do without reading the whole diff. This skill is portable across repositories and requires no scripts, particular language, CI system, or project configuration. Adapt depth to the PR; do not fill sections with guesses or generic boilerplate.

## Establish the reviewed change

Read the current PR metadata, base and head SHAs, complete list and diff of changed files, linked issues, relevant decisions, and repository instructions. Follow changed production code through its callers and dependencies to establish what actually runs. Inspect tests and checks on the current head. Use an isolated checkout if local source is needed; preserve existing work. Treat the PR description and test claims as evidence to verify, not as proof by themselves. Refresh head and base if they move before delivering the review.

Read [inspection guidance](references/review.md) for evidence and scenario analysis. Source access is required for a claim about code; if unavailable, say what could not be inspected. Running project code is optional and should be purposeful, isolated, and feasible with the available environment.

## Present the review

Use the following order, combining short sections when that improves readability:

1. **Classification and context.** Label the PR as a bug fix, documentation change, security fix, performance change, refactor, feature slice, or another accurate type; more than one may apply. State the problem, intended outcome, and a one line summary. For a feature slice, explain its place in the larger feature using verified related issues, PRs, dependencies, and remaining work. Distinguish documented context from inference.
2. **End to end evidence.** Say which end to end scenarios were *added to this PR*, the real entry point they exercise, what they assert, and what they leave unproved. Keep automated end to end tests, integration or plan tests, manual runs reported by the author, your own runs, and CI results distinct. State explicitly when no automated end to end test was added.
3. **System map.** Name the primary components changed, with one line each for responsibility and effect. Identify new components and why they exist; identify removed or replaced components and why. Say “none” when appropriate. Distinguish a new internal helper or enum variant from a new service or public component.
4. **Changed scenarios.** Show a compact Mermaid sequence diagram when timing, actors, boundaries, or fallback paths matter. Show the old and new flows or branches for materially different scenarios; include external systems and trust boundaries where relevant. Explain the diagram in plain language. Skip a diagram only when it would repeat a single action.
5. **Unit scenarios.** Describe added or changed unit tests by behavior and meaningful positive, negative, or edge cases. Explain the assertion's limit; avoid an inventory of test names.
6. **Algorithms and data.** Explain material changes to decision rules, ordering, retries, complexity, or state transitions, and whether an algorithm is new or changed. Describe new or changed data models, storage/API contracts, compatibility, migration sequence, rollback, and data backfill when applicable. State when there is no persistent model or migration change.
7. **Architectural assessment.** Lead with actionable findings if any, each with severity, trigger, consequence, and a current diff location. Examine changed defaults, security and network exposure, permissions, production versus development behavior, observability, failure handling, and operational reversibility. Identify consequential test gaps and questions for a decision. Do not report “no findings” until these paths are checked.

Link each substantive section to precise lines in the current PR diff where possible, and link related issues directly. Verify path, side, and line before using an inline diff link. For behavior outside the diff, link the relevant source and mark it as existing behavior. Keep the summary and component descriptions understandable without code terminology.

## Follow up actions

This review does not authorize editing code, posting comments, requesting changes, or merging. If the user separately asks for one of those actions, read [follow up guidance](references/feedback.md), refresh the PR, perform only the requested action, and verify its outcome. Do not turn a request to explain or review into a remote write.
