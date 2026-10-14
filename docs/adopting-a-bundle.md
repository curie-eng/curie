# Adopting an existing agent into a plugin bundle

For the general compatibility and adoption path, start with
[Bring your agent](bring-your-agent.md). This page preserves the migration
recipe for a legacy Python agent with its own Slack server. For workspace setup
after conversion, continue to [Your first Slack agent](your-first-slack-agent.md).

You already have an agent — a pre-plugin (`agent-ss-template`) project: a Python
package with `src/`, a `Makefile`, a `Dockerfile`, a `compose.yaml`, and a
Flask/Slack `server.py`. Curie runs a different shape — a **plugin bundle**
(`.claude-plugin/plugin.json`, `skills/<name>/SKILL.md`, `.mcp.json`, <!-- doclint:ignore-line -->
`evals/cases.json`) — so `curie skill up` on your directory dead-ends at *"no <!-- doclint:ignore-line -->
plugin manifest"*.

This guide is the supported path from the old shape to a runnable bundle. It has
two parts: a **mechanical** step (scaffold the skeleton) and a **manual** step
(port the logic). The logic port is deliberately manual — deciding what becomes
a skill, an MCP tool, or the system prompt needs your judgment, not a converter
(see [ADR-0071](adr/0071-adopting-a-pre-plugin-bundle-scaffolds-the-skeleton-not-the-logic.md)).

## Step 1 — scaffold the plugin skeleton (mechanical)

From anywhere, point `init --adopt` at your existing directory:

```bash
curie init --adopt ./revenue-leak-agent
```

This creates the plugin file-set **alongside** your existing code — it never
overwrites a file you already have, and it does not touch `src/`, your
`Makefile`, or anything else. The bundle name is derived from the directory
(`revenue-leak-agent`); pass an explicit name to override:

```bash
curie init revenue-leak --adopt ./revenue-leak-agent
```

You now have, next to your old files:

```
.claude-plugin/plugin.json     # the bundle manifest
skills/<name>/SKILL.md         # a starter skill (edit this)
.mcp.json                      # MCP servers (empty to start)
evals/cases.json               # a starter smoke eval
AGENTS.md                      # harness instructions
```

## Step 2 — port the logic (manual)

Move your agent's behavior from the old app into the bundle. The mapping:

| In the old shape | Goes to |
|---|---|
| The system prompt / agent instructions | the body of `skills/<name>/SKILL.md` |
| A deterministic tool / API call your code made (e.g. a CRM lookup) | an **MCP server** entry in `.mcp.json` (stdio `command` or `url`) — often your existing engine wrapped as a small stdio MCP server |
| The LLM call and its loop | handled by the runner; you supply the skill + tools, not the loop |
| `server.py`'s Slack handling | **dropped** — Curie owns ingress; you keep only the agent's decision logic |

Your existing Python doesn't have to be rewritten — the pragmatic path is to
expose its capabilities as an in-bundle stdio MCP server that `.mcp.json` points
at, and let the skill drive it. Start by copying your prompt into `SKILL.md` and
adding one MCP tool, then grow from there.

## Step 3 — run it

```bash
curie skill up --fake-model      # boots offline, no credential; proves the bundle loads
curie skill message "..."        # canned reply under fake model — plumbing, not a graded answer
curie skill down
```

Then write a **falsifiable** eval in `evals/cases.json` (one a broken agent would <!-- doclint:ignore-line -->
fail), and re-run `curie skill eval` with a real credential to grade it — that
green is your promotion gate. `curie guide` has the full authoring loop.
