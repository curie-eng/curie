# Permission map

Every write this agent can perform. All of them go through the `platform`
connector, which calls the Curie platform API with the platform key
(`MANAGER_PLATFORM_KEY`). That key permits far more than this list, because
the API has no scoped key. **What bounds the agent is the connector's tool list
and the bundle's `toolPolicy`, not the key.** A route with no tool cannot be
reached, and a tool the policy does not name is refused.

## The four writes, each with approval

Each call pauses the turn and posts an approval card in the conversation that
asked. A member of that channel resolves it: with no approver list on the
route, Curie's default is the channel's members. The agent has no tool that
resolves approvals. Each write is undone by its opposite.

| Tool (live name) | API call | Effect | Undo |
| --- | --- | --- | --- |
| `mcp__platform__pause_schedule` | `POST /schedules/{agent}/{name}/pause` | stops a cron hook firing | `resume_schedule` |
| `mcp__platform__resume_schedule` | `POST /schedules/{agent}/{name}/resume` | lets a paused hook fire again | `pause_schedule` |
| `mcp__platform__kill_agent` | `POST /agents/{id}/kill` | the agent takes no new turns. Refuses `curie-manager` itself | `resume_agent` |
| `mcp__platform__resume_agent` | `POST /agents/{id}/resume` | lifts a kill | `kill_agent` |

To narrow who may approve, bind approvers on the route (a Slack user group,
named users, or email addresses; see [`docs/approvals.md`](../../../docs/approvals.md)).
To narrow who may talk to the agent at all, set the channel's allowed callers
with `curie <tier> callers`.

The agent also writes its own `curie-state` namespace `manager`. The scheduled
check uses it for a canary value, through the built-in state tools and a
sandbox-scoped token rather than the platform key.

## Not yet

These are absent from the connector, not merely gated, so no message can reach
them. They are planned, one at a time.

| Operation | Why it waits |
| --- | --- |
| Delete an agent, a deployment or a memory line | Curie cannot yet undo or restore a delete |
| Change a budget | Raising one spends money; it needs a cap rule first |
| Roll back or redeploy a version | It changes what an agent runs, and in-flight work needs care |
| Fire a hook | It starts new turns, which cost money and may post |
| Add memory to another agent | Memory loads into that agent's prompt, so it is instructions to another bot |

## Never

No tool reaches these routes, so the agent cannot do them however it is asked.
They remain the operator's, through the `curie` CLI.

- `GET /agents/{id}/hook-secret`: the webhook signing secret.
- `POST /console/login-codes`: console logins.
- `POST /approvals/principals/*`: operator and adapter approval principals.
- `POST /approvals/{id}/resolve`: resolving any approval, including its own.
- `POST /channels/token`: channel tokens.
- `PATCH /agents/{id}`, `POST|PATCH|DELETE /agents/{id}/channels`, and
  `PUT /agents/{id}/channels/callers`: secrets, model, channels and caller
  allowlists.
- `POST /agents` and `POST /agents/{id}/versions`: creating agents and
  uploading bundles.
- State writes into another agent's namespaces.

`connectors/platform/test_server.py` fails if the connector's source names any of
the first five routes, if any write is not gated, or if a "not yet" operation
reappears.
