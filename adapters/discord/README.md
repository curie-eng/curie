# Curie Discord adapter

This service connects Discord Gateway messages to Curie's channel-neutral
ingress and renders Curie's neutral reply events through Discord REST. It is a
real second surface adapter. The Curie worker does not import Discord and the
adapter does not run an agent bundle.

## Discord application

Create a Discord application with a bot, enable the Message Content intent,
and invite it with permission to view channels, send messages, create public
threads, send messages in threads, read message history, and manage its own
messages. Keep the bot token in this adapter only.

## Configuration

Set these environment variables:

- `DISCORD_BOT_TOKEN`: Discord bot token.
- `CURIE_DISCORD_ADAPTER_SECRET`: secret expected in `X-Curie-Adapter-Secret` on
  Curie reply requests.
- `CURIE_API_URL`: Curie API origin, for example `http://api:8000`.
- `CURIE_DISCORD_BINDINGS`: JSON array of parent channel bindings. Each item is
  `{"parent_channel_id":"111111111111111111","address":"111111111111111111","token":"chn_example"}`.
- `CURIE_DISCORD_BINDINGS_PATH`: optional path to the same JSON array. When set,
  the adapter rereads this file for each intake so scoped tokens can rotate
  without reconnecting the Gateway client.
- `CURIE_DISCORD_STATE_PATH`: SQLite path. Default is
  `/var/lib/curie-discord/state.sqlite3`.
- `CURIE_DISCORD_REPLY_HOST` and `CURIE_DISCORD_REPLY_PORT`: reply HTTP bind.

For each item, add the same agent surface through Curie and mint its scoped
channel token. Configure the Curie binding's reply endpoint as this service's
`/replies` URL and its adapter credential as the value of
`CURIE_DISCORD_ADAPTER_SECRET`.

## Behavior

A bot mention in a configured parent channel creates a public thread and a
placeholder. Messages in that adapter-created thread continue the same Curie
conversation without another mention. Discord message ids are stable delivery
ids, Discord thread ids are conversation ids, and placeholder ids are reply
references.

Discord v1 supports text, mentions, threads, streamed text edits, and text
fallbacks for platform posts. It does not implement Discord buttons, direct
messages, file attachments, or interactive approvals. A Discord delivery
failure never falls back to Slack or another surface.

SQLite stores thread routing and delivery ids but not scoped channel tokens.
Those remain in the environment or mounted bindings file.

## Telemetry

`main()` calls `bootstrap_service_telemetry` on the `curie_discord_adapter`
package logger, so every module beneath it — including ones added later —
writes single-line JSON that has passed
`curie_telemetry.redact.RedactingLogFilter`, and those logs export over OTLP
when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. The bootstrap also installs the
trace and metric exporters, but the adapter authors no spans and records no
instruments yet, so those two signals carry nothing until someone
instruments a call path. The three
third-party namespaces the process runs under (`discord`, `uvicorn`, `httpx`)
are bootstrapped alongside it, because they lost their handler when
`logging.basicConfig` was removed and would otherwise print unredacted text via
`logging.lastResort`. No endpoint set is a supported no-op, not a boot failure:
that is what every local, offline and CI install runs.

The adapter is a first-party workload that instruments itself, and it stays
**outside** the instrumented-set enumeration in `charts/curie/CLAUDE.md`. That
rule defines membership as exactly the workloads whose container `env` block
includes the `curie.env.otel` helper, with
`grep -n 'curie.env.otel' charts/curie/templates/` as the authoritative answer
at any commit. This adapter has no chart template at all, and no compose
service, so it renders no container `env` block and cannot be a member of a set
defined by a template include; writing it in as prose would recreate exactly the
stale hand-maintained list that rule exists to abolish. Whoever later adds a
chart template for this adapter must add the `curie.env.otel` include, and that
is the moment it joins the enumerated set.
