# Connect one Curie agent to Discord and Slack

Multi surface is implicit. An agent with one surface behaves as before. Adding
a second surface opts that same agent into multi surface operation. There is no
mode or feature flag to enable, and removing surfaces until one remains opts it
back out.

The Discord service in `adapters/discord` is a real channel adapter. Discord
Gateway events enter Curie's existing `/channels/turns` endpoint, and the
worker sends neutral reply events back to the adapter's `/replies` endpoint.
The agent id, active deployment, memory, and bundle are shared. Conversation
state remains isolated by surface kind and Discord thread id.

## 1. Configure the Discord application

Create a Discord bot, enable the Message Content intent, and grant it these
permissions in the target server:

- view channels and read message history
- send messages
- create public threads
- send messages in threads
- manage its own messages

The Discord bot token belongs only in the adapter process.

## 2. Add the Discord surface

Choose an adapter credential name and configure the worker with a JSON map:

```bash
export CURIE_ADAPTER_CREDENTIALS='{"discord-main":"replace-with-a-shared-reply-secret"}'
```

Add the Discord parent channel to the same Curie agent that already owns the
Slack surface:

```bash
curie local surfaces acme-bot \
  --add discord=111111111111111111 \
  --endpoint https://discord-adapter.example.com/replies \
  --adapter discord-main
```

The command returns all surfaces on the agent. Seeing both `slack` and
`discord` under the same agent name proves this is one bot identity rather than
two copies of its source.

Mint a token scoped to the Discord binding using the existing channel-token
endpoint, then configure the adapter:

```bash
export DISCORD_BOT_TOKEN='replace-with-the-discord-bot-token'
export CURIE_DISCORD_ADAPTER_SECRET='replace-with-a-shared-reply-secret'
export CURIE_API_URL='https://curie.example.com'
export CURIE_DISCORD_BINDINGS='[{"parent_channel_id":"111111111111111111","address":"111111111111111111","token":"chn_example"}]'
python -m curie_discord_adapter
```

For token rotation without reconnecting Discord, mount that JSON array as a
file and set `CURIE_DISCORD_BINDINGS_PATH` instead. The adapter rereads it for
each intake. Its SQLite state stores thread routing and delivery ids, never the
scoped token.

`CURIE_DISCORD_ADAPTER_SECRET` must equal the value selected by
`discord-main` in `CURIE_ADAPTER_CREDENTIALS`. The scoped `chn_` token is sent
only from the adapter to Curie's turn ingress.

## 3. Verify one agent on both surfaces

Run `curie local surfaces acme-bot --json` and capture the single agent plus
its Slack and Discord pairs. Mention the bot in the configured Discord parent
channel. The adapter creates a thread and placeholder, and Curie edits the
placeholder with the response. Send a follow-up inside that thread without a
new mention. Then mention the same agent in Slack and continue its Slack
thread.

The observable success conditions are:

- both pairs belong to one Curie agent id
- both turns resolve the same active deployment and bundle
- the Discord response stays in its Discord thread
- the Slack response stays in its Slack thread
- a failure on either adapter never redirects the response to the other

Discord v1 supports text, mentions, adapter-created threads, streamed text
edits, and text fallbacks for platform posts. Direct messages, files, Discord
buttons, and interactive approvals are outside this first adapter version.
