# 200. Agents read and edit canvases shared into their bound channels

Date: 2026-10-05

Status: Accepted

Accepted with the implementation of [issue 3819](https://github.com/curie-eng/curie/issues/3819) under [ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).

## Context

Teams keep a weekly plan in a Slack canvas. [ADR 0100](0100-agents-search-their-own-surface-through-the-channel-port.md) lets an agent read messages and threads on its own bound surface through the channel port and the platform-owned `curie-slack` MCP server, but it covers no canvases. An Accepted ADR is immutable, so canvas operations need their own decision.

The provider facts below were observed with a bot token in a dev workspace on 2026-10-02 and 2026-10-05.

1. Slack serves canvas content only as HTML, downloaded from the file's `url_private` after `files.info`. The file has filetype `quip` and mimetype `application/vnd.slack-docs`.
2. Tables have no header element, so the header is the first row by position. Every cell paragraph carries a section id.
3. `canvases.edit` replaces one section by id. A cell replace changed exactly that cell, left the table and every section id intact, and replacing the text back restored the original bytes.
4. Tables cannot gain rows through the API.
5. `files.list` with `types=canvas` lists the canvases shared into a channel.
6. Free Slack plans allow one canvas per channel and no standalone canvases.

## Decision

1. Canvas list, read and cell edit are optional channel-port operations served by the same platform-owned `curie-slack` server, under the bound-surface rule of ADR 0100. A canvas is usable only if `files.info` shows it shared into a channel that is one of this agent's bindings of that kind and the platform bot is a member of that channel. `files.info` is the one metadata lookup the platform makes to decide this. A canvas outside the bound surface is refused with a named reason (`channel_read.canvas_not_bound`) before any content call, meaning a download or an edit. A malformed canvas id, a missing grant, an unrecorded section or invalid edit text is refused before any provider call.
2. `list_channel_canvases` returns the canvases shared into one bound channel (id, title, created), so an agent can find the newest weekly canvas without a stored pointer.
3. `read_canvas` returns every table as rows, with the header taken by position and a section id per editable cell, plus non-table text as paragraphs. The content is untrusted data and is labelled as such. The platform keeps no copy.
4. `edit_canvas_cell` replaces the text of one existing table cell, named by canvas id and by a section id that a read of that canvas returned in the same logical turn. There is no insert, delete, rename, row creation or canvas creation. The text must be plain single-line text that Slack's markdown will not reinterpret, so what is written reads back exactly.
5. The platform bot token is the credential, and the runner never sees it. The per-turn channel read capability from ADR 0100 carries which grants the bundle declares, and each canvas operation charges one page of that turn's existing eight-page budget. The reference Slack app manifest adds `canvases:read`, `canvases:write` and `groups:read`, because membership of a private channel is read with `conversations.info`.
6. `canvasList`, `canvasRead` and `canvasEdit` are three separate bundle grants, default off, declared like `channelRead`. Only a literal `true` grants, and none implies another. Each is enforced at mount, in the tool catalogue, at execution and on the platform API route. The `curie-slack` tools stay governed by `toolPolicy`, so an operator can still require approval for the edit or deny it.
7. Every edit writes an audit record before the provider call (agent, turn, canvas, section, before text, after text) and settles it as applied or failed. An edit whose outcome Slack did not confirm stays recorded as attempted.

The realizing paths are `curie_api.channel_read.canvas.authorize_and_canvas`, `curie_api.channel_read.slack_canvas.SlackCanvasReader`, `curie_api.channel_read.canvas_sections`, the `POST /channel-canvas` route in `curie_api.routers.channel_read`, the `ChannelCanvasEdit` model and its migration, `plugin_format.PLATFORM_SLACK_GRANT_FIELDS` and `PluginManifest.platform_slack_grants`, `curie_runner.platform_slack.canvases`, and the worker's capability mint in `curie_worker.kernel.channel_read`.

## Consequences

An agent can keep a team plan current without a person retyping it.

Section ids were observed stable across edits, but an edit still follows a fresh read in the same turn. In practice an edit therefore needs `canvasRead` as well as `canvasEdit`.

Free plans allow one canvas per channel. Paid workspaces may share standalone canvases into a channel, and the same fence applies. Adding rows and creating a new week's canvas stay human.

Adapters without canvases advertise neither canvas capability.

The edit audit rows belong to the agent like every other agent-scoped table, so deleting an agent deletes its edit records with it.

Installs must reinstall the Slack app to gain the new scopes. Until then the operations are refused with a named scope error.

## Alternatives considered

1. Amend ADR 0100. Rejected because Accepted ADRs are immutable.
2. Use a bundle-supplied user token or Slack's hosted MCP canvas tools. Rejected because they take user tokens only and would put a credential in the sandbox.
3. Use `canvases.sections.lookup` as the read path. Rejected because it returns ids only, not text.
4. Replace the whole canvas with markdown. Rejected because it destroys structure and is an unbounded write.
5. Use one combined canvas grant. Rejected because a read-only bundle would gain write.
6. Escape markdown in edit text instead of refusing it. Rejected because the escaping is not proven against Slack, while refusal is verifiable.
