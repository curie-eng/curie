# Permission map

Every write this agent can perform. All of them go through the `platform`
connector, which calls the Curie platform API with the platform key
(`MANAGER_PLATFORM_KEY`). That key permits far more than this list, because
the API has no scoped key. **What bounds the agent is the connector's tool list
and the bundle's `toolPolicy`, not the key.** A route with no tool cannot be
reached, and a tool the policy does not name is refused.

## Without approval

These run as soon as the agent calls them. Anyone who can post in the agent's
channel can ask for them.

| Tool (live name) | API call | Effect | Undo |
| --- | --- | --- | --- |
| `mcp__platform__fire_hook` | `POST /agents/{id}/hooks/{name}/fire` | runs one cron hook now | none; the run happened |
| `mcp__platform__pause_schedule` | `POST /schedules/{agent}/{name}/pause` | stops a hook firing | `resume_schedule` |
| `mcp__platform__resume_schedule` | `POST /schedules/{agent}/{name}/resume` | lets a paused hook fire | `pause_schedule` |
| `mcp__platform__kill_agent` | `POST /agents/{id}/kill` | the agent takes no new turns. Refuses `curie-manager` itself | `resume_agent` |
| `mcp__platform__resume_agent` | `POST /agents/{id}/resume` | lifts a kill | `kill_agent` |
| `mcp__platform__set_budget` | `PUT /agents/{id}/budget` | changes the daily spend cap or per-run token cap; the reply carries the old values | `set_budget` with the old values |
| `mcp__platform__add_memory` | `POST /agents/{id}/memory` | adds a line to an agent's prompt-loaded memory | `delete_memory` (gated) |
| `mcp__platform__deploy_version` | `POST /deployments` | puts an EXISTING version of an agent in force; refuses a version id the agent does not have | `deploy_version` with the previous version |

The agent also writes its own `curie-state` namespace `manager`. The scheduled
check uses it for a canary value, through the built-in state tools and a
sandbox-scoped token rather than the platform key.

## With approval

Each call pauses the turn and posts an approval card in the conversation that
asked, and a member of that channel resolves it. The agent
has no tool that resolves approvals.

| Tool (live name) | API call | Effect | Undo |
| --- | --- | --- | --- |
| `mcp__platform__delete_agent` | `DELETE /agents/{id}` | deletes an agent with its channels, versions and state. Refuses `curie-manager` itself | none |
| `mcp__platform__end_deployment` | `DELETE /deployments/{id}` | ends a deployment, so its version is no longer in force | `deploy_version` |
| `mcp__platform__delete_memory` | `DELETE /agents/{id}/memory/{index}?expected_version=` | removes one memory line; a stale version is refused | `add_memory` with the same text |

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
the first five routes.
