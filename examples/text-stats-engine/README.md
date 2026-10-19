# text-stats-engine: reuse your analysis tools

Bring existing text-analysis instructions and deterministic code into a Curie
co-worker. This bundle keeps the analysis engine as an in-bundle MCP server,
uses a skill to direct its tools, and lets Curie supply the model loop and
workspace connection. Start with [Bring your agent](../../docs/bring-your-agent.md)
to copy it, prove a real local tool call, and deploy the same bundle to Slack.

No separate tool service is required. The engine is a stdio subprocess shipped
inside the bundle with no external dependencies.

## What's here

```
text-stats-engine/
  .claude-plugin/plugin.json   bundle manifest; the local .mcp.json is auto-loaded
  .mcp.json                    declares the stdio server (command + args)
  scripts/engine_server.py     the engine: a stdlib-only stdio MCP server
  skills/text-stats/SKILL.md   a skill that drives the engine's tools
  evals/cases.json             numerical requests for the engine
  evals/trajectory.json        requires each requested MCP tool
```

The engine (`scripts/engine_server.py`) speaks the MCP **stdio transport**:
newline-delimited JSON-RPC 2.0 on stdin/stdout. It is deliberately
dependency-free (Python stdlib only) so it runs end-to-end from a clean clone
with no install step. It implements `initialize`, `tools/list`, and `tools/call`
for a tiny deterministic text-statistics engine (`word_count`, `char_count`,
`reading_time_minutes`).

The `.mcp.json` declaration is what makes it "in-bundle": the `command` +
`args` point at a script *inside the bundle*, `${CLAUDE_PLUGIN_ROOT}`-qualified
so the harness spawns it as a child process regardless of cwd. This validates
against `plugin_format.validate_bundle` unchanged — the format already accepts
stdio MCP servers (the `McpServer` model), which is exactly the surface this
template exploits.

## Run it end-to-end

Drive the engine directly (no model, no network) to see the transport work:

```bash
cd examples/text-stats-engine
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' \
  '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"word_count","arguments":{"text":"one two three"}}}' \
  | python3 scripts/engine_server.py
```

You will see three JSON-RPC responses: the handshake, the tool catalog, and
`3` (the word count). Under a real agent, `curie skill up` spawns the same
server from the `.mcp.json` declaration and the `text-stats` skill calls its
tools.

## Verify a real model uses the engine

With a supported model credential configured:

```bash
curie skill check --json
curie skill up
curie skill message --json "Use the text-stats skill and the text-stats-engine word_count tool to count the words in: one two three. Report the tool result."
curie skill eval --json
curie skill down
```

Check the answer is three and the tool activity includes `word count`. The
trajectory sidecar requires the MCP request for each eval case; it does not
assert successful execution or the answer. Check the numerical reply separately.
The bare skill CLI shows request activity, not returned MCP payloads. After
deployment, use the [workspace run inspection](../../docs/your-first-slack-agent.md#verify-your-workspace-reply)
to verify the successful tool result. A short word-count question can be
answered without using a tool, so it
is insufficient as a tool-execution check.

To test that the trajectory oracle detects a missing capability, use a
disposable copy without the MCP server declaration and rerun the same suite.
The missing-tool case must fail. Keep the working bundle intact.

## When to use this shape vs. a CLI subprocess

Both shapes run engine code you ship inside the bundle. They differ in how the
agent talks to it.

**Use an in-bundle stdio MCP server (this template) when:**

- The engine exposes **discrete, callable tools** the model chooses among
  mid-turn — `tools/list` advertises them and the model calls them by name with
  typed arguments (the `inputSchema`).
- You want **structured request/response** with argument validation, multiple
  round-trips within one turn, and per-tool results, rather than one opaque
  invocation.
- You want the engine to be a **long-lived process** for the session (spawned
  once, many calls) instead of re-exec'd per call.
- You want it to work on a substrate that cannot host a network MCP endpoint —
  the whole point of the differentiator.

**Use a CLI subprocess (e.g. a `scripts/` command the skill shells out to via
`Bash`) when:**

- The work is a **one-shot command** — run it, read stdout, done — with no need
  for a tool catalog or a persistent session.
- The engine is an **existing binary or script** you do not want to wrap in an
  MCP server (a Makefile target, a linter, a codegen step).
- The model does not need to *choose* among tools; the skill already knows the
  exact command to run.

Rule of thumb: reach for the stdio MCP engine when the model needs to **pick and
call typed tools**; reach for a CLI subprocess when the skill needs to **run one
known command**. The CLI-subprocess shape keeps working exactly as before; this
template adds the MCP-engine option alongside it.
