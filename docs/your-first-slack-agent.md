# Your first Slack agent

This is the workspace deployment path for a supported bundle. To reuse
existing instructions and tools, start with [Bring your agent](bring-your-agent.md).
For the primary developer use case, start with the
[Software Factory](software-factory.md). Check the
[capability and release reference](capabilities.md) before choosing a channel.
The setup below creates and connects your Slack app.

From nothing to a bot that answers in Slack and redeploys itself when you push.

Four commands do the work. Everything else on this page is the accounts you
need first and the seven mistakes that actually cost people time.

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

Already have a bundle? Validate it locally with [Bring your agent](bring-your-agent.md),
stay in its directory, and continue at [Get the Slack tokens](#2-get-the-slack-tokens).
The scaffold below is for a new agent.

```bash
curl -fsSL https://raw.githubusercontent.com/curie-eng/curie/main/get-curie.sh | bash
curie init my-agent && cd my-agent
curie skill up && curie skill message "hello, what can you do?"
```

You now have a working agent. Edit `skills/my-agent/SKILL.md` to change what it <!-- doclint:ignore-line -->
does, then `curie skill up` again to reload it.

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

In Bash, load the two tokens into the environment without putting their values
in command arguments or shell history:

```bash
read -r -s -p "Slack app token: " SLACK_APP_TOKEN
printf '\n'
read -r -s -p "Slack bot token: " SLACK_BOT_TOKEN
printf '\n'
export SLACK_APP_TOKEN SLACK_BOT_TOKEN
```

Keep token values outside your bundle and repository. The comms command below
reads these environment variables directly.

Invite the bot to your channel, and copy the **channel ID** — right-click the
channel → *View channel details*, it's at the bottom and looks like `C0…`.

## 3. Put it on the cluster (10 min)

```bash
CURIE_CONTEXT="$(kubectl config current-context)"
CURIE_NAMESPACE=my-agent
CURIE_RELEASE=my-agent

curie cluster up --context "$CURIE_CONTEXT" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE"

curie cluster comms --slack --context "$CURIE_CONTEXT" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE"

curie cluster deploy --plugin-dir . --context "$CURIE_CONTEXT" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE" \
  --repo <owner>/<repo> --slack-channel C0YOURCHANNEL
```

Exactly one Curie release may connect to a given Slack app. Slack Socket Mode
fans events across every connected client, so two releases sharing one app
silently split mentions. Do not connect a second dispatcher, local or cluster,
to this app.

The plain install reads the exported `sk-ant-` credential and infers Anthropic
egress. If admission reports that the cluster has no `gvisor` RuntimeClass,
Curie shows that attempt as retrying, applies `security.gvisor.mode=off`, and
retries once. It also reuses
PriorityClasses and the sandbox controller when their complete Helm ownership
metadata names an existing release. Every applied inference is printed with
its equivalent override.

`@mention` the bot in that channel. Keep the deploy receipt: its
`bundle.sha256` in the JSON receipt identifies the artifact running in the workspace.

### Verify your workspace reply

Send a fresh mention with the request you already tested locally. Check the
visible Slack reply, then inspect the run through the same release:

```bash
curie cluster observability --context "$CURIE_CONTEXT" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE" --json runs --agent-id <agent-id>

curie cluster observability --context "$CURIE_CONTEXT" \
  --namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE" --json run <trace-id>
```

Choose the run matching your message, agent and channel. The run command shows
the tool names and timing; it does not expose their input and returned payloads.
An administrator can read the matching conversation through the
[workflow state API](interfaces/workflow-state/INTERFACE.md):
`GET /agents/<agent-id>/state/transcript/<url-encoded-thread-key>`.
For Slack, the thread key is `slack:<channel-id>:<parent-message-ts>`.
Use the configured platform API authentication and inspect only your agent's
thread. In `value[].messages`, match the `tool_use` block's `id` to the
`tool_result` block's `tool_use_id`.

For the text-stats example, confirm that the `word_count` input is
`{"text":"one two three"}`, the matching result contains `3`, and
`is_error` is absent or false. A tool request alone does not prove execution;
an unrelated older trace does not prove your fresh Slack message reached the agent.

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

Two optional keys per target. `identity` names the bot the target speaks
through when your installation runs more than one; leave it out for the
installation's own bot. `connectors` lists which of your `connectors.yaml`
connectors run for that target; leave it out to run all of them, or write `[]`
for none:

```yaml
  prod:
    agent: my-agent
    env: prod
    identity: ops-bot
    slack_channel: C0YOURPRODCHANNEL
    connectors: [grafana]
```

An agent runs one set of connectors, so two targets that bind the same agent
must list the same ones. `curie cluster deploy --identity <name>` overrides a
target's `identity` for one deploy.

A named `identity` must be one your installation declares; a deploy naming
one it does not declare is refused.

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
CURIE_CLUSTER_ID="ca:$(kubectl --context "$CURIE_CONTEXT" config view --minify --raw -o json | jq -r '.clusters[0].cluster | ((.server // "") + "\\n" + (."certificate-authority-data" // ."certificate-authority" // ""))' | sha256sum | awk '{print $1}')"
curie secrets set GRAFANA_SERVICE_ACCOUNT_TOKEN --from-env GRAFANA_SERVICE_ACCOUNT_TOKEN \
  --cluster-identity "$CURIE_CLUSTER_ID" --release "$CURIE_RELEASE" --namespace "$CURIE_NAMESPACE"
```

At cluster deploy time, Curie delivers both connector `secrets` and
`secret_files` values into the agent's Kubernetes Secret. Those stored values
must be scoped to the target cluster identity, release, and namespace; a
mismatch is refused rather than silently reused. Unscoped names remain for
skill/local credentials.

---

## Seven things that cost people an hour

1. **Export the credential before `skill up`.** Otherwise the boot succeeds and
   the *next* command fails with `model-credential-rejected`.
2. **`--repo` binds once and is never re-pointed.** An agent with *no* binding
   can still be bound by a later `deploy --repo` -- but one already
   pointing at a different repository is left alone and only warned about, so
   there the fix really is deleting the agent and starting over. `curie doctor`
   reports which agents are unbound.
3. **Bind by channel ID (`C0…`), never `#name`.**
4. **Run `cluster comms` after `cluster up`, not before.**
5. **Renaming the bot in Slack rotates the bot token** — re-run `cluster comms`.
6. **One channel binds one agent.** An agent may serve several channels
   (`curie cluster surfaces <agent> --add slack=C0EXAMPLE2`), but pointing a
   SECOND agent at an occupied channel still returns 409.
7. **Exactly one Curie release may connect to a given Slack app.** Slack Socket
   Mode fans events across every connected client, so two releases sharing one
   app silently split mentions. Give each long-lived release its own app; stop
   extra dispatchers instead of retrying clicks or mentions.

## Which rung do I want?

| | Runs where | Use it for |
|---|---|---|
| `curie skill` | one container on your laptop | writing the agent |
| `curie local` | Docker Compose, full backend | the real queue → worker → sandbox path |
| `curie cluster` | Kubernetes | Slack, and anything real |

Same bundle at every rung. See the [CLI target guide](../cli/README.md#using-different-tiers) for the
long-form walkthrough, and [`docs/operations.md`](operations.md) for running one
in production.
