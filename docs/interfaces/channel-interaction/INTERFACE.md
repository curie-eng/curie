---
seam: Channel interaction message
kind: CLEAN
impls: 4 renderers (Slack, terminal, mail, Discord)
grade: not separately graded
epics:
  - "ADR-0020"
order: 5
---

# INTERFACE: Channel interaction

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN &nbsp;·&nbsp; **Implementations today:** 4 renderers (Slack, terminal, mail, Discord) &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

Agents produce a semantic `OutboundMessage`; channel adapters render it. Slack
may use Block Kit and the terminal may use a numbered selector, but neither
widget appears in the contract. This is the interaction half of ADR-0020.
Deliberate progress (ADR-0130) is a second semantic object on the same line: the
platform produces a task card and milestones, and adapters render them.

The source of truth is
`packages/channel-protocol/src/channel_protocol/models.py` for messages and
`packages/channel-protocol/src/channel_protocol/progress.py` for progress; the
committed JSON Schema is
`packages/channel-protocol/schema/channel-protocol.schema.json`.

## Message contract

Every message has `version: "1.0"` and mandatory `text`. The text must be a
complete usable reply after all optional fields are removed. Optional `status`,
`header`, `fields`, `links`, and `footer` enrich presentation.

An optional `interaction` is one semantic intent:

- `choice`: an id, optional prompt, one to ten `{label, value}` options, and
  `allow_free_text` (default `true`).
- `confirm`: an id, prompt, semantic confirm and cancel actions, and
  `allow_free_text` (default `false`).

Action `label` is display text. Action `value` is the exact inbound message sent
when selected. Values are conversation input, not trusted authorization tokens;
the server must still authorize side effects and approvals.

## Progress contract

Progress is not an `OutboundMessage`. It has its own closed models, so an
adapter never has to guess whether a message is an answer, an approval card or
a task card, and a progress edit can never be read as a settled approval.

The values are closed:

- `ProgressState` is exactly `queued`, `investigating`, `awaiting-approval`,
  `preparing-workspace`, `testing`, `publishing`, `complete`, `failed`,
  `cancelled`. `TERMINAL_PROGRESS_STATES` holds the last three.
- `MilestoneClass` is exactly `evidence` (intake or material evidence
  acquired), `scope` (a material hypothesis or scope change) and
  `verification` (a verification result). A class says why a durable
  interruption is warranted; the classes are not a sequence.
- A summary is a single line of 1 to 200 characters, stripped of surrounding
  whitespace. Every Python line boundary is refused, not only `\n`. It holds
  short task state, never reasoning, tool output, secrets or a draft answer.

Three models carry them:

- `ProgressCommand` is what a model submits through the platform's
  `curie_progress` tool: `version` (`"1.0"`), `update_id`, `state`, `summary`
  and an optional `milestone`. `update_id` is 1 to 64 characters of letters,
  digits, `.`, `_`, `:` and `-`, starting with a letter or digit, and
  identifies one update for idempotency. The
  model is closed: a command naming a channel kind, channel address, reply
  ref, endpoint, credential, adapter payload, delivery id, progress record or
  milestone budget is refused, not ignored. It names no progress record at
  all. The record is the running turn chain's, resolved from the authenticated
  turn, and its `progress_id` is minted by the platform, so the command has no
  field through which to address another turn's record. A command never
  reaches an adapter.
- `ProgressCard` is the semantic form of the chain's one mutable card:
  `kind: "card"`, `state`, `summary`, `revision` (from 1, a higher revision
  supersedes a lower one) and `terminal`. `terminal` must equal whether `state`
  is in `TERMINAL_PROGRESS_STATES`, so an adapter in any language renders a
  closed card without keeping its own copy of that set.
- `ProgressMilestone` is one durable milestone reply: `kind: "milestone"`,
  `milestone` (its class), `summary` and `ordinal`, the chain's reserved slot
  from 1 to `MAX_PROGRESS_MILESTONES` (3). A fourth milestone cannot be
  expressed.

A card and a milestone reach adapters in the reply wire's 1.1 `progress` field,
together with the delivery identity that makes their delivery idempotent; see
[building-a-channel-adapter.md](../../guides/building-a-channel-adapter.md#wire-versions).
Adapters render them: the card as one message edited in place where the channel
can edit, kept after completion and marked with its terminal state, and a
milestone as a new message, both through a plain-text fallback where the
channel has nothing richer.

## ACI envelope

Until ACI gains a native semantic-message event, a runner carries the message
inside its final text as a complete fenced block:

````text
```curie-reply
{"version":"1.0","text":"Which view?","interaction":{"kind":"choice","id":"view","options":[{"label":"Open issues","value":"show open issues"}]}}
```
````

Adapters must hide incomplete or malformed envelopes and fall back to ordinary
text. The legacy unversioned `buttons: [[label, value]]` shape remains readable
during migration but is not valid v1 authoring.

## Adapter requirements

- Render `text` even when no optional capability is supported.
- Advertise capabilities; never infer them from the channel name. (ADR-0020's
  intent. `ChannelCapabilities` is modeled but has no producer or consumer today,
  so the shipped adapters decide statically; see Known leakage.)
- Render choices and confirmations using native affordances when the channel has
  them and as numbered text otherwise.
- Preserve action values exactly when converting a selection into inbound text.
- Keep rendering and channel-native payloads inside the adapter.
- Treat links as navigation, not conversation responses.
- Never grant authority based only on an action id or value.

## TUI behavior

The Curie TUI advertises nothing: no adapter emits `ChannelCapabilities`, so its
support for interactive actions is expressed only by what it renders. It renders
agent-authored actions first and appends `Type a
message...` as the final selector option when free text is allowed. Selecting
that option enters an explicit compose mode; it is a terminal affordance and is
never added to the agent-authored contract. The selector expands to keep every
contract option and the appended free-response option visible. The compose
field is hidden until free response is selected. The TUI sends the selected
action value or composed text as the next turn, replaces stale actions after
each reply, and never prints protocol fences or terminal status frames.

The conversation transcript remains scrollable while selecting, composing, and
waiting for a response. Scrolling suspends tail-follow until the user returns to
the latest output. Transcript navigation is adapter state and must not alter the
outbound message or its interaction intent.

Starter prompts are bundle metadata (`starterPrompts`), not response actions and
not hardcoded into the TUI. They disappear after the first turn unless the agent
returns a new interaction.

## Implementations today

Four renderers consume the same `OutboundMessage`, and none leaks its widgets
back into the contract:

1. **Slack (Block Kit)** — `apps/worker/src/curie_worker/blocks.py`. The worker
   parses the `curie-reply` envelope out of the runner's final text
   (`apps/worker/src/curie_worker/blocks.py::parse_reply`), maps it onto the
   internal `apps/worker/src/curie_worker/blocks.py::Reply` shape
   (`apps/worker/src/curie_worker/blocks.py::_reply_from_message`), and renders
   Block Kit sections/buttons via
   `apps/worker/src/curie_worker/blocks.py::to_blocks`. `apps/worker/src/curie_worker/blocks.py::render`
   is the fallback boundary: anything that is not a complete, valid envelope
   degrades to plain text, so a half-streamed block never shows raw JSON. Slack's
   3000-char section cap is absorbed here by
   `apps/worker/src/curie_worker/blocks.py::chunk`, not pushed onto the agent.
   The approval card (ADR-0010) travels the same seam: the kernel emits a
   `confirm` intent and the adapter renders it below the line via
   `apps/worker/src/curie_worker/blocks.py::approval_card` inside
   `apps/worker/src/curie_worker/slack_sink.py::SlackReplyAdapter.emit`, so no Block
   Kit is built above the seam. Its settled forms are split by which outcome
   arrived: nobody decided renders the expired form via
   `apps/worker/src/curie_worker/blocks.py::expired_approval_card`, and a
   decision renders the resolved form via
   `apps/worker/src/curie_worker/blocks.py::resolved_approval_card`, both reached
   through the same adapter on a settling `reply.update`.
   `allow_free_text` on that intent is rendered here
   too: Slack expresses "this decision may carry free text" as a dialog opened by
   the click, so the card carries the note-collecting action ids and the
   dispatcher's `apps/dispatcher/src/curie_dispatcher/approval_actions.py::open_note_dialog`
   opens the view. Stamping a settled card re-reads the original through
   `conversations.replies` rather than `conversations.history`, because the
   default card is a thread reply and the channel timeline does not carry one
   (#1073); a card that cannot be read is left unstamped rather than rebuilt
   from nothing.

   Block Kit for that card is built in **two** modules, not one. The dispatcher
   owns the settled form:
   `apps/dispatcher/src/curie_dispatcher/approval_actions.py::settled_approval_card`
   assembles the header, summary, requested-by and verdict blocks, and
   `apps/dispatcher/src/curie_dispatcher/approval_actions.py::_resolved_card_blocks`
   strips the actions block off a clicked card and appends the verdict context
   line. `resolved_approval_card` above is a thin adapter over the first of those,
   not a second renderer. The direction is deliberate (#1084): the worker already
   imports this module for the action ids, so the reverse import would be a cycle,
   and one renderer shared by the click path and the resume path is what stops the
   two producing different-looking cards for the same decision.
2. **Terminal (TUI selector)** — `cli/src/channel.rs`. It parses the same fence
   (`REPLY_FENCE`) into a `TerminalMessage` of plain lines plus actions, which the
   TUI renders as a numbered selector per the TUI behavior above. It expresses
   `allow_free_text` as a typed reply alongside the numbered actions.
3. **Mail**: `apps/mail-adapter/src/curie_mail_adapter/egress.py`. A `ReplyPost`
   whose interaction is a `ConfirmIntent` becomes an approval-card email through
   `apps/mail-adapter/src/curie_mail_adapter/adapter.py::MailAdapter.record_approval_card`,
   answered by an emailed APPROVE or REJECT with an optional note (ADR-0177). Other
   posts and updates are recorded as the turn's one reply text. A settling
   `reply.update` closes the card through
   `apps/mail-adapter/src/curie_mail_adapter/adapter.py::MailAdapter.settle_approval_card`.
4. **Discord**: `adapters/discord/src/curie_discord_adapter/egress.py`. It renders
   the message text and progress as plain text posts and edits, and appends the
   settled decision to the text of a settling update. It builds no component
   widgets for an interaction.

The Slack and terminal renderers honor `allow_free_text` on different subsets. The terminal one
carries it through for both intents
(`cli/src/channel.rs`, consumed by `cli/src/interactive.rs`). The Slack one reads
it on exactly one path, the kernel-emitted `confirm` that becomes the approval
card (#1053, at
`apps/worker/src/curie_worker/slack_sink.py::SlackReplyAdapter.emit`); the
agent-authored envelope path drops it, since
`apps/worker/src/curie_worker/blocks.py::_reply_from_message` never reads the
field. See Known leakage for why that is not simply a bug.

The split is the point: Block Kit lives only in the two Slack-side modules named
above (`blocks.py` and `approval_actions.py`), the numbered selector only in
`channel.rs`, and the agent authors none of them.

The Slack and Discord renderers consume progress; the terminal and mail ones do not. A card and a
milestone become Block Kit in
`apps/worker/src/curie_worker/blocks.py::progress_card` and
`apps/worker/src/curie_worker/blocks.py::progress_milestone`, every text element
`plain_text` so a model-authored summary cannot mention anyone, and
`apps/worker/src/curie_worker/slack_sink.py::SlackReplyAdapter.emit` routes a
progress body to them before any answer or approval path. The channel-neutral
text for a channel with nothing richer is
`packages/channel-protocol/src/channel_protocol/progress.py::progress_text`, which the Discord adapter posts and edits as plain text. The mail adapter drops a progress-bearing post or update silently, because one message per turn has no card to edit. Nothing produces a progress body yet: the worker's coordinator stores what it
owes, and no deliverer calls an adapter with it.

## Known leakage

- **The terminal renderer is a hand-written mirror, not generated.** The source of
  truth is the Pydantic model in
  `packages/channel-protocol/src/channel_protocol/models.py` with a committed JSON
  Schema (`packages/channel-protocol/schema/channel-protocol.schema.json`), but
  `channel-protocol` ships **no Rust binding**. `cli/src/channel.rs` re-declares the
  wire shape by hand (`#[serde(deny_unknown_fields)]` on each struct). Nothing gates
  that mirror against the schema the way ADR-0017 gates the ACI's tri-language
  contract, so a field added in Python is not mechanically caught here — and
  `deny_unknown_fields` means the mirror *rejects* the new field rather than
  ignoring it. This is the seam's real drift risk, and it is why "4 renderers" does
  not imply "4 generated adapters".
- **Capability negotiation is modeled but unwired.**
  `packages/channel-protocol/src/channel_protocol/models.py::ChannelCapability`
  and
  `packages/channel-protocol/src/channel_protocol/models.py::ChannelCapabilities`
  exist and are exported to the committed JSON Schema, but a repo-wide sweep
  (Python, Rust, TypeScript) finds no reference outside that package and its
  schema export: nothing advertises capabilities and nothing reads them. Both
  shipped adapters decide their affordances statically instead. So the "Advertise
  capabilities" adapter requirement above states ADR-0020's intent, not a live
  wire, and a third channel would find no negotiation to plug into. Documented
  rather than built: what the capability set should contain is exactly what a real
  third channel would teach, and inventing it ahead of one is the speculative
  layer the architecture vision rules out.
- **`allow_free_text` reaches the Slack renderer on one path only.** The terminal
  renderer carries the field for both `choice` and `confirm`; the Slack renderer
  reads it only for the kernel's approval-card `confirm`, because
  `apps/worker/src/curie_worker/blocks.py::_reply_from_message` projects
  `OutboundMessage` onto its internal `Reply` shape without it. Part of this is
  inherent: a Slack channel is always typeable, so `allow_free_text: true` needs
  no affordance and `false` cannot be enforced there at all, where the TUI can and
  does hide its compose option. But the field is dropped at the projection rather
  than deliberately declined, so the asymmetry is invisible in the code, and the
  next Slack-side interaction that wants it will find nothing to read.
- **Progress is accepted but not rendered.** The `curie_progress` operation is
  the runner's platform `progress` tool (`mcp__curie__progress`), and a
  person's Slack turn carries a per-turn capability for it. The tool posts each
  `ProgressCommand` to the API's scoped ingress
  (`apps/api/src/curie_api/routers/turn_progress.py::accept_turn_progress`),
  which resolves it to the turn chain's record by the token's subject and
  appends it to that chain's inbox, and a per-turn pump in the kernel applies
  it to the worker coordinator's durable store
  (`apps/worker/src/curie_worker/progress.py::ProgressStore`). The store
  applies the idempotency, ordering, terminal, milestone-budget and
  delivery-identity rules ADR-0130 places on the coordinator, but with
  rendering off nothing delivers from it. Its rules are in the worker's
  [README](../../../apps/worker/README.md#deliberate-progress-adr-0130).
  What the models realize on the wire is the closed shape: a command that names
  a routing, credential, delivery or budget field is refused at validation. The
  terminal mirror in `cli/src/channel.rs` does not model progress either.
- **The mail renderer keys an approval card on a display label.**
  `apps/mail-adapter/src/curie_mail_adapter/egress.py::EgressHandler` collects the
  approver addresses by comparing each `MessageField.label` with
  `APPROVER_FIELD_LABEL`, the string `"Approver"`. The constant is defined twice, in
  `apps/worker/src/curie_worker/approvals.py` (the producer) and in
  `apps/mail-adapter/src/curie_mail_adapter/adapter.py` (the reader), and nothing
  gates the two against each other. Rewording the label on the worker side silently
  leaves the email naming no approvers, because a field label is display text, not a
  typed key. Only the platform decides who may answer; the label only words the email.
- **The envelope is a text-channel workaround.** ACI has no native
  semantic-message event, so the message rides inside the runner's final text as a
  fenced block. Every adapter therefore carries fence-parsing and partial-envelope
  suppression that a native event would delete. Named in "ACI envelope" above as
  explicitly interim.
- **Not separately graded** — this seam is the interaction half of ADR-0020 and is
  not one of the six swap-readiness Jobs. The channel *ingress/egress* swap story is
  graded, and graded `C`, on the [channel-ingress](../channel-ingress/INTERFACE.md)
  seam. Read that grade as the honest one for "can we add a second channel"; this
  file only covers the rendering-free message contract.

## Cross-links

- **ADR(s):** [ADR-0020](../../adr/0020-message-port-rendering-free-channel-interface.md) — the message port: a rendering-free channel interface with capability negotiation (this file is its interaction half); [ADR-0130](../../adr/0130-deliberate-progress-is-bounded-durable-channel-state.md) — deliberate progress as bounded durable channel state, the source of the progress contract; [ADR-0017](../../adr/0017-tri-language-contract-codegen.md) — the tri-language codegen pattern this seam's Rust mirror does **not** yet follow
- **Sibling seams:** [channel-ingress](../channel-ingress/INTERFACE.md) — the graded (`C`) ingress/egress swap story; [aci-producer](../aci-producer/INTERFACE.md) — the frozen ACI the envelope currently tunnels through
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — the interaction contract is not one of the six swap-readiness Jobs; not separately graded
