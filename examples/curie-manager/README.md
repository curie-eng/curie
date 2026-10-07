# curie-manager — an agent that manages Curie

An example bundle for an agent that manages the Curie install it runs on, from
Slack. Ask it what the platform is doing, and use it for the two emergency
controls that otherwise need the CLI:

```
@Curie Manager is the platform healthy?
@Curie Manager which schedules failed their last run?
@Curie Manager pause acme-bot's nightly-cleanup hook
@Curie Manager stop acme-bot now
```

Every weekday at 09:00 Eastern it also checks the platform and posts the
result to its channel. The check exercises real paths rather than only reading
status: it writes and reads back a state value, and reports failed schedules,
killed agents, agents at their spend cap, stuck approvals and the last day's
error rate. So the install tests itself.

## What it is allowed to do

It reads freely, and it can make exactly two kinds of change, each only after
a person approves it. Both are undone by their opposite.

| Kind | Tools | Approval |
| --- | --- | --- |
| Read | `platform_health`, `list_agents`, `get_agent`, `list_versions`, `list_deployments`, `list_schedules`, `get_hook_run`, `get_controls`, `list_memory`, `list_approvals`, `metrics_summary`, `list_traces`, `get_trace` | none |
| Pause or resume a schedule | `pause_schedule`, `resume_schedule` | a person approves each call |
| Stop or restart an agent | `kill_agent`, `resume_agent` | a person approves each call |

The bundle's `toolPolicy` enforces this table. A tool the policy does not name
is refused, so a tool added to the connector does nothing until it is classified
there. `test_server.py` fails if any write is not gated.

**Not yet.** Deleting anything, changing a budget, rolling back or redeploying
a version, firing a hook, and writing another agent's memory are absent from
the connector, not merely gated. Deletes wait until Curie can undo or restore
them; the rest come later, one at a time.

**Never.** It cannot read a secret value or the webhook secret, mint a console
login or an approval principal, resolve an approval (including its own), change
an agent's secrets, channels or caller allowlist, or create an agent or upload a
bundle. Those stay with an operator and the `curie` CLI, and the agent names
the exact command. It also will not stop itself, since nothing would be left to
restart it.

**Who can use it.** By default anyone who can post in its channel can ask, and
any member of that channel can approve. Narrow either with the install's own
controls: `curie <tier> callers` for who may talk to it, and approvers on its
approval route for who may approve (see
[`docs/approvals.md`](../../docs/approvals.md)). The full list of writes and
their undo is in [`docs/PERMISSION-MAP.md`](docs/PERMISSION-MAP.md).

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
| a delete is not yet available | requesting approval for, or claiming, a delete it deliberately cannot do |
