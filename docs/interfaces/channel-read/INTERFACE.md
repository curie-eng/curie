---
seam: Channel read
kind: CLEAN
impls: 1 Slack reader pair (history and canvas) plus HTTP test fakes
grade: not separately graded
epics:
  - "ADR-0100"
  - "ADR-0200"
  - "#2877"
  - "#3819"
order: 29
---

# INTERFACE: Channel read

> Part of the Curie swappable-seam catalog. See the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN &nbsp;·&nbsp; **Implementations today:** 1 Slack reader pair (history and canvas) plus HTTP test fakes &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

An agent reads a channel it is already bound to through a surface reader. Windows, page budgets, cursors and the repeat of authorization live in `apps/api/src/curie_api/channel_read/service.py` for history and in `apps/api/src/curie_api/channel_read/canvas.py` for canvases. A surface joins by implementing a protocol in `apps/api/src/curie_api/channel_read/readers.py` and registering that implementation under its binding kind. The caller passes the channel. The reader answers for that channel.

This is the read half of [ADR-0100](../../adr/0100-agents-search-their-own-surface-through-the-channel-port.md). Canvases are the second family, from [ADR-0200](../../adr/0200-agents-read-and-edit-canvases-shared-into-their-bound-channels.md). Outbound rendering is a separate line, on [channel interaction](../channel-interaction/INTERFACE.md). Inbound turns and reply egress are on [channel ingress](../channel-ingress/INTERFACE.md).

## Current contract

`apps/api/src/curie_api/channel_read/readers.py::ChannelReader` is the history port. It exposes `capabilities`, resolves a credential through `identity`, `has_identity` and `identity_key`, names a cooldown method through `rate_limit_key`, and checks ids through `valid_thread_id` and `valid_message_id`. The reads are `history`, `thread` and `message`, returning `apps/api/src/curie_api/channel_read/readers.py::ChannelMessage` values inside `apps/api/src/curie_api/channel_read/readers.py::ProviderPage`. `apps/api/src/curie_api/channel_read/readers.py::reader_for` returns a reader only when that reader advertises `history-read` (`packages/channel-protocol/src/channel_protocol/models.py::ChannelCapability`). A kind with no such reader is `capability_unsupported`.

`apps/api/src/curie_api/channel_read/readers.py::CanvasReader` is the optional canvas port. It uses the same identity trio, checks `valid_canvas_id` and `valid_section_id`, and answers `locate`, `is_member`, `list`, `document` and `replace_cell`. The records are `apps/api/src/curie_api/channel_read/readers.py::CanvasFile`, `apps/api/src/curie_api/channel_read/readers.py::CanvasPage` and `apps/api/src/curie_api/channel_read/readers.py::CanvasDocument`. `apps/api/src/curie_api/channel_read/readers.py::canvas_reader_for` returns a reader only when it advertises the capability the operation needs: `canvas-read` for list and read, `canvas-edit` for a cell replace. A surface that has no canvases advertises neither, and its kind is refused by name.

`apps/api/src/curie_api/channel_read/service.py::authorize_and_read` repeats authorization on every history request and checks authority, grants, bindings and request shape before any provider call. History requires the bundle grant `channelRead`. `apps/api/src/curie_api/channel_read/canvas.py::authorize_and_canvas` repeats those checks for canvas list, read and edit, which require `canvasList`, `canvasRead` and `canvasEdit`. Refusals carry a named `channel_read` code. Those four grant names are manifest fields. See [bundle format](../bundle-format/INTERFACE.md). The provider decides membership and whether a requested message exists. Canvas lookup also uses provider metadata: an absent file is `canvas_not_found`, a file shared into none of the agent's bound channels is `canvas_not_bound`, and a bound file whose metadata says it is not a canvas is `not_a_canvas`. These checks precede canvas content reads.

## Implementations today

One production pair. Both registries construct it with the API HTTP client and `apps/api/src/curie_api/identities.py::slack_bot_tokens`, keyed by `packages/aci-protocol/src/aci_protocol/turn.py::SLACK_KIND`.

1. History. `apps/api/src/curie_api/channel_read/service.py::channel_readers` maps that kind to `apps/api/src/curie_api/channel_read/slack_reads.py::SlackChannelReader`, which advertises only `history-read`. It reads Slack `conversations.history` and `conversations.replies`, and it learns a permalink base from `auth.test`. A binding of any other kind, including email, is refused `capability_unsupported` with no provider call.
2. Canvas. `apps/api/src/curie_api/channel_read/canvas.py::canvas_readers` maps the same kind to `apps/api/src/curie_api/channel_read/slack_canvas.py::SlackCanvasReader`, which advertises `canvas-read` and `canvas-edit`. It locates and lists canvases, downloads the canvas document, and replaces one section. `apps/api/src/curie_api/channel_read/slack_canvas.py::SlackCanvasReader.identity` delegates to an inner `SlackChannelReader` built with the same token map.

The tests stand in for Slack's HTTP API. They do not implement the ports.

1. `apps/api/tests/test_channel_read.py::FakeSlack` answers history and thread calls.
2. `apps/api/tests/test_channel_canvas.py::FakeCanvas` holds one canvas's metadata and HTML. `apps/api/tests/test_channel_canvas.py::FakeCanvasSlack` serves the canvas methods on top of `FakeSlack`.
3. `apps/api/tests/test_channel_read.py::test_a_reader_without_history_read_is_unsupported` builds a local stand-in to show that `reader_for` drops a reader that does not advertise `history-read`.

## Known leakage

1. **Production reads are Slack only.** `channel_readers` and `canvas_readers` each return a one entry map. The only key is `SLACK_KIND`, and the only values are `SlackChannelReader` and `SlackCanvasReader`. Discord, mail and the terminal implement neither protocol. A second surface needs a reader module and a new map entry. The protocols are the line. The maps are still Slack.
2. **The credential is a Slack bot token chosen by Slack identity.** `apps/api/src/curie_api/channel_read/slack_reads.py::SlackChannelReader.identity` calls `packages/aci-protocol/src/aci_protocol/turn.py::slack_speaking_identity` for every route, always with `SLACK_KIND`. When any route speaks as the default identity, that identity wins. Otherwise the first name in sorted order wins. `slack_bot_tokens` fills the map from `SLACK_BOT_TOKEN` for the default identity and from each extra identity's declared env var. A missing token is `provider_unconfigured`. The route endpoint does not select a token: `slack_speaking_identity` ignores it. There is no neutral credential handle on the registry. Both constructors take this Slack token map.
3. **Canvas orchestration names Slack methods.** History asks the reader for `apps/api/src/curie_api/channel_read/readers.py::ChannelReader.rate_limit_key`. Canvas does not. `apps/api/src/curie_api/channel_read/canvas.py` imports `apps/api/src/curie_api/channel_read/slack_canvas.py::CONVERSATIONS_INFO`, `apps/api/src/curie_api/channel_read/slack_canvas.py::FILES_INFO`, `apps/api/src/curie_api/channel_read/slack_canvas.py::FILES_LIST`, `apps/api/src/curie_api/channel_read/slack_canvas.py::FILES_DOWNLOAD` and `apps/api/src/curie_api/channel_read/slack_canvas.py::CANVASES_EDIT`, and passes those strings as the cooldown key around each `CanvasReader` call. A second canvas surface would still be charged against those Slack method names.
4. **Canvas edit text is checked by the Slack validator for every kind.** `apps/api/src/curie_api/channel_read/canvas.py::authorize_and_canvas` calls `apps/api/src/curie_api/channel_read/slack_canvas.py::cell_text_problem` before `canvas_reader_for` selects the edit-capable reader. Its plain-cell restrictions therefore apply to any binding kind, including a future non-Slack reader.

## Cross-links

**ADR(s):** [ADR-0100](../../adr/0100-agents-search-their-own-surface-through-the-channel-port.md) is the channel port agents search through. [ADR-0200](../../adr/0200-agents-read-and-edit-canvases-shared-into-their-bound-channels.md) adds canvas list, read and cell edit on bound channels. History shipped with #2877. Canvases shipped with #3819.

**Sibling seams:** [channel-interaction](../channel-interaction/INTERFACE.md) renders outbound messages and progress. [channel-ingress](../channel-ingress/INTERFACE.md) is the graded ingress and reply egress swap. [bundle-format](../bundle-format/INTERFACE.md) declares `channelRead`, `canvasList`, `canvasRead` and `canvasEdit`.

**Vision doc:** [architecture-vision.md](../../architecture-vision.md). Channel read is outside the six swap-readiness jobs, so this seam is not separately graded.
