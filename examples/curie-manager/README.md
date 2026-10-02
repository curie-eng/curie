# curie-manager — an agent that manages Curie

An example bundle for an agent that operates the Curie install it runs on. Ask
it in Slack what the platform is doing, and ask it to change things:

```
@curie-manager is the platform healthy?
@curie-manager which schedules failed their last run?
@curie-manager pause acme-bot's nightly-cleanup hook
@curie-manager raise acme-bot's budget to $20 a day
@curie-manager roll acme-bot back to its previous version
@curie-manager delete acme-old-bot
```

Every weekday at 09:00 Eastern it also checks the platform and posts the
result to its channel. The check exercises real paths rather than only reading
status: it writes and reads back a state value, and reports failed schedules,
killed agents, agents at their spend cap, stuck approvals and the last day's
error rate. So the install tests itself.

## What it is allowed to do

The agent has elevated permissions by default. Routine operations run without
asking anyone. Deletes wait for a person.

| Kind | Tools | Approval |
| --- | --- | --- |
| Read | `platform_health`, `list_agents`, `get_agent`, `list_versions`, `list_deployments`, `list_schedules`, `get_hook_run`, `get_controls`, `list_memory`, `list_approvals`, `metrics_summary`, `list_traces`, `get_trace` | none |
| Operate | `fire_hook`, `pause_schedule`, `resume_schedule`, `kill_agent`, `resume_agent`, `set_budget`, `add_memory`, `deploy_version` | none |
| Delete | `delete_agent`, `end_deployment`, `delete_memory` | one person approves each call |

The bundle's `toolPolicy` enforces this table. A tool the policy does not name
is refused, so a tool added to the connector does nothing until it is classified
there. `test_server.py` fails if any destructive tool is not gated.

Some things it cannot do at all, because no tool calls them:

- read a secret value or the webhook secret;
- mint a console login or an approval principal;
- resolve an approval, including its own;
- change an agent's secrets, channels or caller allowlist;
- create an agent or upload a bundle.

Those stay with an operator and the `curie` CLI. It also will not kill or
delete itself, since nothing would be left to undo that.

**Anyone who can post in its channel can ask it to operate the platform.** Put
it in a channel whose members you would trust with `curie local` or `curie
cluster` themselves.

## Where the platform key lives

Curie's platform API has one key. It is all or nothing: the key reads, changes
and deletes every agent, and mints logins and approval principals. So the key
never enters the agent's sandbox.

```
sandbox ──Bearer MANAGER_MCP_TOKEN──▶ platform connector ──X-API-Key MANAGER_PLATFORM_KEY──▶ Curie API
```

- `MANAGER_PLATFORM_KEY` is the platform key. It is a connector-only secret.
- `MANAGER_MCP_TOKEN` is a random token, and it is the connector's
  `bearer_secret`. It is the only secret Curie puts in the sandbox, and it opens
  only this connector's tools.

A prompt injection that talks the model into calling the API directly has no
key to call it with.

On the local tier nothing but the connector itself checks who is calling it, so
the server refuses any request without the token, and refuses to start without
both secrets.

## What's here

```
curie-manager/
  .claude-plugin/plugin.json      manifest: the tool policy and the weekday platform check
  connectors.yaml                 the platform connector, built from connectors/platform
  connectors/platform/            the MCP server, its Dockerfile and its tests
  skills/curie-manager/SKILL.md   the behavior: answering, operating, deletes, the scheduled check
  evals/cases.json                the promotion gate
  deploy.yaml                     the channel the agent is bound to
```

## Run it on a local stack

The trigger in `plugin.json` and `deploy.yaml` name the channel
`C0YOURCHANNEL`. Replace both with your channel's ID.

Save the two secrets. The platform key of a local stack is the `api_key` field
of `~/.config/curie/local/curie.json`.

```bash
curie secrets set MANAGER_PLATFORM_KEY
TOKEN=$(openssl rand -hex 32) curie secrets set MANAGER_MCP_TOKEN --from-env TOKEN
```

Then build the connector, start a stack with Slack, and deploy:

```bash
curie build --plugin-dir examples/curie-manager
curie local up --build --slack
curie local deploy --plugin-dir examples/curie-manager --target prod
```

[`docs/slack-local-runbook.md`](../../docs/slack-local-runbook.md) covers the
Slack app and its tokens. Give the agent its own Slack app, because only one
Curie release may connect to a given app.

Run the check now instead of waiting for 09:00:

```bash
curie local hook fire curie-manager platform-check
```

On a cluster, set `PLATFORM_API_URL` in `connectors.yaml` to
`http://<release>-api.<namespace>:8000` and scope both secrets to the cluster
with `curie secrets set ... --cluster-identity ... --release ... --namespace ...`.

## Evals

```bash
curie skill eval
```

| Case | The failure it catches |
| --- | --- |
| listing agents reads the platform | answering from memory instead of calling `list_agents` |
| it names itself among the agents | a list that was not read from this install |
| an agent that does not exist is not invented | reporting health for an agent that is not there |
| it will not kill itself | an agent that takes itself offline with nothing left to resume it |
| a channel change is handed to the CLI | claiming to do something it has no tool for, instead of naming `curie local surfaces` |
