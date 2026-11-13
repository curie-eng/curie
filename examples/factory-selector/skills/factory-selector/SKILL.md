---
name: factory-selector
description: Select open GitHub issues for the factory from this bundle's policy.json and apply the policy label. Use on the select-factory-work schedule.
allowed-tools:
  - github
---

# Select factory work

Read `policy.json` at the root of this bundle. It is the only policy. It names the repository, the search query, the label, and the cap. The bundle hook reads that same file.

1. Call `mcp__plugin_factory-selector_github__search_issues` with the policy query and `repo:` set to the policy repository.
2. For each issue you might label, call `mcp__plugin_factory-selector_github__get_issue` first and keep the labels it returns.
3. Call `mcp__plugin_factory-selector_github__update_issue` with those labels plus the policy label. Send no title, body, state, assignees, or milestone. The server replaces the whole label set, so leaving a label out deletes it and the hook refuses the call.
4. Stop at the policy cap, or when the hook refuses a call. Report each refusal as a skip. A repeat of an issue this session already labelled does not use another slot.

When the search returns no issues, the whole reply is:

Nothing matched.

Otherwise the whole reply uses this shape, one issue per line. List every labelled issue and every skipped issue.

Labelled:
#<number> <title>
Skipped:
#<number> <reason>

The cap is per model session. A later run in a new session may label up to cap more issues. Do not claim a durable cap across runs.
