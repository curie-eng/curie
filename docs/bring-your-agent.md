# Bring your agent to Curie

Reuse your agent's instructions and tools in a supported Curie bundle, then
bind it to a workspace. Curie supplies the runner, deployment lifecycle and
workspace ingress. This is a packaging and adaptation path, not a promise that
an arbitrary running agent can be attached unchanged.

For a ready software development workflow, start with the
[Software Factory](software-factory.md). For the platform behavior behind both
paths, read [Co-workers](co-workers.md) and the
[capability and release reference](capabilities.md).

## What you can reuse

Install the [latest release](https://github.com/curie-eng/curie/releases/latest)
and check the [capability reference](capabilities.md) before choosing a runtime
or workspace integration.

| Existing asset | Reuse path | What changes or remains unsupported |
|---|---|---|
| Prompt or agent instructions | Put the behavior in `skills/<name>/SKILL.md` | Add valid skill metadata and instructions for tools, approvals and resume. Curie owns the model loop. |
| Existing Agent Skills files | Include supported skills in the bundle | Validate the layout and dependencies. A skill file alone is not a deployed agent. |
| MCP tools over stdio | Declare the server in `.mcp.json` | Ship its code and runtime dependencies; qualify bundled paths with `${CLAUDE_PLUGIN_ROOT}`. |
| Hosted MCP tools | Use the supported MCP configuration or managed connectors | Provision credentials, network access and the server. Discovery is not proof that a real tool call succeeds. |
| Scripts or deterministic application code | Keep scripts in the bundle or expose the code through MCP | Curie does not automatically translate application logic or install arbitrary dependencies. |
| Legacy Python agent with its own Slack server | Use [the legacy migration recipe](adopting-a-bundle.md) | `curie init --adopt` scaffolds files; port behavior manually and let Curie own ingress. |
| A different agent runtime or an already running agent service | Requires separate runtime integration work | The default supported runtime is the Claude harness. Existing extension seams are not an attach-any-runtime adoption interface. |

See the [bundle format](interfaces/bundle-format/INTERFACE.md),
[harness package contract](interfaces/harness-package/INTERFACE.md) and
[model provider contract](interfaces/model-provider/INTERFACE.md) for exact
compatibility. Changing the model provider does not by itself change the agent
runtime.

## Minimal conversion: reuse a text analysis engine and its instructions

The committed [text-stats-engine bundle](../examples/text-stats-engine/README.md)
shows this conversion: existing deterministic analysis code lives in
[the engine script](../examples/text-stats-engine/scripts/engine_server.py), an MCP wrapper exposes named tools, and an existing
text analysis skill directs the model to use them. The model loop and Slack
handling are supplied by Curie.

From a release checkout, copy that working bundle as your starting
point. This preserves the existing tool implementation and skill rather than
starting from an empty scaffold:

```bash
cp -R examples/text-stats-engine ./my-text-agent
cd my-text-agent
```

The copied [manifest](../examples/text-stats-engine/.claude-plugin/plugin.json)
identifies the bundle. Its tool declaration is:

```json
{
  "mcpServers": {
    "text-stats-engine": {
      "command": "python3",
      "args": ["${CLAUDE_PLUGIN_ROOT}/scripts/engine_server.py"]
    }
  }
}
```

Keep the tool implementation, replace the instructions with your own behavior,
and add your eval cases. Keep names consistent if you rename the bundle. For
an existing engine that does not speak MCP, adapt the wrapper rather than
replacing the engine. The example README explains the alternative of calling a
known script through the runner's shell tool.

## Prove the bundle, then prove the workspace

Docker is required. First validate tool discovery without a model credential:

```bash
curie skill check --json
```

Then supply a supported model credential using the
[CLI credential procedures](../cli/README.md#skill-target) and run a real reply:

```bash
curie skill up
curie skill message --json "Use the text-stats skill and the text-stats-engine word_count tool to count the words in: one two three. Report the tool result."
curie skill down
```

Check that the JSON reply reports three words and that the tool activity on
stderr includes `word count`. The explicit skill and tool request makes the
adoption check reproducible; a short word-count question can be answered
without calling the engine. A plausible answer alone is insufficient.
A `--fake-model` run proves plumbing only; it cannot prove model behavior or MCP
tool execution. The copied example includes numerical cases and a trajectory
sidecar. Before
promotion, start the runner again and run:

```bash
curie skill up
curie skill eval --json
curie skill down
```

The trajectory grade requires the requested MCP tool to appear in the observed
sequence. It grades requests, not successful execution or the numerical answer.
Check the numerical reply separately. The bare skill CLI exposes request
activity, not returned MCP payloads; use the workspace run inspection below to
verify the successful tool result. See the
[verification contract](agents.md#the-verification-contract) for a falsifiable
capability-removal control.

Stay in `my-text-agent` and continue at
[Get the Slack tokens](your-first-slack-agent.md#2-get-the-slack-tokens). Skip
that guide's scaffold step: `curie init` creates a different bundle. Configure
Socket Mode, deploy this same bundle with `--plugin-dir .`, and bind its channel.
One channel binds one agent; exactly one release may connect to a given Slack
app. Send a fresh Slack mention with the same explicit skill-and-tool request.
Inspect the visible reply, then use the
[run inspection commands](your-first-slack-agent.md#verify-your-workspace-reply)
to find that message's successful `word_count` call and result. Record the
release, bundle version and deployed bundle digest.
A terminal reply stub without a connected dispatcher does not prove Slack
transport.

The example supplies a direct MCP transport probe; the repository
[test fixtures](../runner/tests/test_mcp_tool_capability.py) exercise MCP
capability behavior. Neither substitutes for your real model and Slack reply.
Email adoption is blocked by the adapter's authentication refusal; see the
[mail adapter status](../apps/mail-adapter/README.md) and capability reference.

## Add the co-worker controls

| Need | Procedure and boundary |
|---|---|
| Human approval | [Approval routes and authenticated approvers](approvals.md). Declare gates, bind every route before deployment, and handle approval resume in the skill. A bare skill runner has no durable approval store. |
| Durable memory | [Memory operation](operations.md#letting-the-agent-remember-facts). Writes are off by default; enabling them is an operator decision. Agent facts can be loaded across bindings, while channel facts stay binding scoped. |
| Credentials and tools | [Slack deployment and connector secrets](your-first-slack-agent.md#giving-it-tools). Store secret values outside the bundle and scope cluster secrets to the intended installation. |
| Trace and supervise | [Administering co-workers](administering-co-workers.md). Inspect runs, model overrides, budgets and supported stop controls. |

Approval does not grant permissions the tool's underlying credential lacks.
