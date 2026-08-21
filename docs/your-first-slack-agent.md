# Your first Slack agent

From nothing to a bot that answers in Slack and redeploys itself when you push.

Four commands do the work. Everything else on this page is the accounts you
need first and the six mistakes that actually cost people time.

---

## What you need first

| | Where to get it | Why |
|---|---|---|
| **Model credential** | [console.anthropic.com](https://console.anthropic.com/) | The agent's brain |
| **Docker** | [get-docker.sh](https://docs.docker.com/get-docker/) | Runs the agent locally |
| **A cluster** | k3s on one small VM is enough | Only needed for the Slack step |
| **A Slack app** | [api.slack.com/apps](https://api.slack.com/apps) → *From a manifest* | Two tokens, below |

A single-node k3s box with **no inbound ports** serves a Slack bot fine —
Slack Socket Mode is outbound-only. You do not need a load balancer, a domain,
or a public IP.

Export the model credential once. Every step below reuses it:

```bash
export CURIE_CREDENTIALS=sk-ant-...
```

---

## 1. Build it on your laptop (2 min)

```bash
curl -fsSL https://raw.githubusercontent.com/curie-eng/curie/main/get-curie.sh | bash
curie init my-agent && cd my-agent
curie skill up && curie skill message "hello, what can you do?"
```

You now have a working agent. Edit `skills/my-agent/SKILL.md` to change what it <!-- doclint:ignore-line -->
does, then `curie skill up --replace` to reload it.

No credential handy? `curie skill up --fake-model` runs the whole loop offline
with scripted replies — enough to prove the plumbing, not to judge the agent.

```bash
curie skill down
```

## 2. Get the Slack tokens

At [api.slack.com/apps](https://api.slack.com/apps) → **Create New App** →
**From a manifest**, paste
[`apps/dispatcher/slack-app-manifest.yaml`](../apps/dispatcher/slack-app-manifest.yaml).

Then collect two tokens:

- **App-Level Token** with scope `connections:write` → `xapp-…`
- **Install to Workspace**, then **Bot User OAuth Token** → `xoxb-…`

Invite the bot to your channel, and copy the **channel ID** — right-click the
channel → *View channel details*, it's at the bottom and looks like `C0…`.

## 3. Put it on the cluster (10 min)

```bash
curie cluster up --namespace my-agent --release my-agent \
  --set agentSandbox.runner.credentials="$CURIE_CREDENTIALS" \
  --allow-egress-host anthropic

curie cluster comms --slack --namespace my-agent --release my-agent \
  --app-token "$SLACK_APP_TOKEN" --bot-token "$SLACK_BOT_TOKEN"

curie cluster deploy --plugin-dir . --namespace my-agent --release my-agent \
  --repo <owner>/<repo> --slack-channel C0YOURCHANNEL
```

`@mention` the bot in that channel. That's the whole thing.

## 4. Make it ship itself

Add `deploy.yaml` next to your bundle:

```yaml
targets:
  dev:
    agent: my-agent-dev
    env: dev
    slack_channel: C0YOURDEVCHANNEL
  prod:
    agent: my-agent
    env: prod
    slack_channel: C0YOURPRODCHANNEL
```

Point a GitHub webhook at the release's API and push. A push to `dev` deploys
to your dev bot; a merge to `main` promotes **that same artifact** to prod — not
a rebuild, so what you tested is what ships. Full webhook wiring:
[`docs/operations.md`](operations.md#automatically-with-git-flow).

## Giving it tools

Add `connectors.yaml` and Curie derives the Deployment, Service, network
policy, and secret wiring for you:

```yaml
connectors:
  grafana:
    image: docker.io/grafana/mcp-grafana:0.17.2
    args: [-t, streamable-http, -address, "0.0.0.0:8000",
           -allowed-hosts, "${CURIE_ALLOWED_HOSTS}", -disable-write]
    env:
      GRAFANA_URL: https://grafana.example.com
    secrets:
      - GRAFANA_SERVICE_ACCOUNT_TOKEN
```

`secrets` are **names only** — the value never enters your repository:

```bash
curie secrets set GRAFANA_SERVICE_ACCOUNT_TOKEN --from-env GRAFANA_SERVICE_ACCOUNT_TOKEN
```

At cluster deploy time, Curie delivers both connector `secrets` and
`secret_files` values into the agent's Kubernetes Secret. The host secret store
is install global, so agents that use the same secret name share one stored
value and can collide. Issue #440 tracks the future per agent delivery path.

---

## Six things that cost people an hour

1. **Export the credential before `skill up`.** Otherwise the boot succeeds and
   the *next* command fails with `model-credential-rejected`.
2. **`--repo` binds once and is never re-pointed.** An agent with *no* binding
   can still be bound by a later `deploy --repo` (#1194) -- but one already
   pointing at a different repository is left alone and only warned about, so
   there the fix really is deleting the agent and starting over. `curie doctor`
   reports which agents are unbound.
3. **Bind by channel ID (`C0…`), never `#name`.**
4. **Run `cluster comms` after `cluster up`, not before.**
5. **Renaming the bot in Slack rotates the bot token** — re-run `cluster comms`.
6. **One channel binds one agent.** An agent may serve several channels
   (`curie cluster surfaces <agent> --add slack=C0EXAMPLE2`), but pointing a
   SECOND agent at an occupied channel still returns 409.

## Which rung do I want?

| | Runs where | Use it for |
|---|---|---|
| `curie skill` | one container on your laptop | writing the agent |
| `curie local` | Docker Compose, full backend | the real queue → worker → sandbox path |
| `curie cluster` | Kubernetes | Slack, and anything real |

Same bundle at every rung. See [`README.md`](../README.md#quickstart) for the
long-form walkthrough, and [`docs/operations.md`](operations.md) for running one
in production.
