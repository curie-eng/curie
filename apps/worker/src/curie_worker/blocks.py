"""Structured Slack replies: a ``Reply`` type, a Block Kit renderer, and a
convention for a plugin to emit one over the plain-text ACI channel.

Curie streams the model's answer as text and edits a placeholder in place, so
a reply is normally just markdown. This module lets a plugin opt into a richer
reply -- a header, a two-column field section, chunked body sections, and a
footer -- without changing the frozen ACI event contract: the plugin emits a
fenced block as the last thing in its answer,

    ```curie-reply
    {"header": "Top leaks", "text": "...", "fields": [["Open", "12"]],
     "footer": "as of today"}
    ```

and the SlackSink renders it as Block Kit. Anything that is not a *complete*,
valid block (including a half-streamed one) falls back to the existing text
path, so streaming partials never show raw JSON and a malformed block degrades
to plain text rather than breaking the reply.

Ported from the CurieTech agent-ss-template ``blocks.py`` (the 3000-char section
cap, mrkdwn normalization, nav-leftmost button ordering); rendering reuses this
package's ``mrkdwn.to_mrkdwn``. Buttons are rendered here but only become
interactive once the dispatcher handles their clicks (a separate change).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from channel_protocol import ChoiceIntent, ConfirmIntent, OutboundMessage
from pydantic import ValidationError

from .behaviorpacks import BehaviorPacks, NavPack, ensure_hub_button
from .mrkdwn import to_mrkdwn

# Slack hard-caps a section's text at 3000 chars; stay under it with margin.
_CHUNK_TARGET = 2900

# Slack hard-caps on Block Kit output; exceed any and chat.update is rejected and
# the whole reply is lost, so ``to_blocks`` clamps every one of these.
_HEADER_MAX = 150  # header plain_text -> 150 chars
_BUTTON_LABEL_MAX = 75  # button plain_text label -> 75 chars
_ACTION_ID_MAX = 255  # action_id -> 255 chars
_SECTION_FIELDS_MAX = 10  # a section's ``fields`` -> 10 entries
_BLOCKS_MAX = 50  # blocks per message -> 50

# Slack caps chat.update's ``text`` field at 40000 chars; stay under it with margin.
_SLACK_TEXT_MAX = 39000


def _truncate(text: str, limit: int) -> str:
    """Truncate display text so the result length is ``<= limit``, marking the cut
    with an ellipsis. For header text and button labels (human-read, not opaque)."""
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"

_FENCE = re.compile(r"```curie-reply\s*\n(.*?)\n```", re.DOTALL)
_FENCE_OPEN = "```curie-reply"


@dataclass
class Reply:
    """A formatted reply, independent of Block Kit. ``text`` is markdown (also
    the plain-text fallback); ``status`` a leading context line (rendered
    as-authored, like ``footer``); ``header`` a header block; ``fields`` a
    two-column section; ``buttons`` an actions block of ``(label, action_id)``
    pairs; ``links`` an actions block of ``(label, url)`` URL buttons; ``footer``
    a trailing context line."""

    text: str = ""
    status: str | None = None
    header: str | None = None
    fields: list[tuple[str, str]] = field(default_factory=list)
    buttons: list[tuple[str, str]] = field(default_factory=list)
    links: list[tuple[str, str]] = field(default_factory=list)
    footer: str | None = None
    # The question attached to `buttons` (ChoiceIntent/ConfirmIntent's `prompt`,
    # #454) -- distinct from `text`, the reply's main body. Rendered immediately
    # before the actions block so an approver/chooser sees the actual question
    # next to the buttons, not just bare labels.
    action_prompt: str | None = None


def chunk(text: str, limit: int = _CHUNK_TARGET) -> list[str]:
    """Split ``text`` into pieces no longer than ``limit``, preferring line
    boundaries but hard-splitting any single over-long line. Empty -> []."""
    if not text:
        return []
    out: list[str] = []
    buf = ""
    for line in text.split("\n"):
        while len(line) > limit:
            if buf:
                out.append(buf)
                buf = ""
            out.append(line[:limit])
            line = line[limit:]
        candidate = f"{buf}\n{line}" if buf else line
        if len(candidate) > limit:
            out.append(buf)
            buf = line
        else:
            buf = candidate
    if buf:
        out.append(buf)
    return out


def _button_shell(label: str) -> dict[str, Any]:
    """The shared button element; callers add either ``action_id`` (interactive,
    dispatcher-handled) or ``url`` (a navigational link button)."""
    # Slack hard-caps a button label at 75 chars; clamp here so both interactive
    # and URL link buttons stay under it.
    label = _truncate(label, _BUTTON_LABEL_MAX)
    return {"type": "button", "text": {"type": "plain_text", "text": label, "emoji": True}}


def _button(label: str, action_id: str) -> dict[str, Any]:
    # Slack hard-caps action_id at 255 chars; it is an opaque id, not display text,
    # so truncate hard with no marker.
    return {**_button_shell(label), "action_id": action_id[:_ACTION_ID_MAX]}


def _context_block(text: str) -> dict[str, Any]:
    """A context block (small greyed line) rendering ``text`` as-authored mrkdwn.
    Used for the leading ``status`` line and the trailing ``footer``."""
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}


def _is_nav(label: str) -> bool:
    """Back ('<-') / up-to-hub ('^') buttons sort leftmost."""
    return label.lstrip()[:1] in ("←", "↑")


def _is_http_url(url: str) -> bool:
    """A link button's url must be an absolute http(s) URL; Slack rejects the
    whole message otherwise. Anything else (empty, relative, other scheme) is
    dropped so one bad link never breaks the reply."""
    u = url.strip().lower()
    return u.startswith("http://") or u.startswith("https://")


def _link_button(label: str, url: str) -> dict[str, Any]:
    """A URL button: interactive without the dispatcher (carries ``url``, no
    ``action_id``)."""
    return {**_button_shell(label), "url": url}


def to_blocks(reply: Reply, nav: NavPack | None = None) -> list[dict[str, Any]]:
    """Render a ``Reply`` to a Block Kit blocks array. Order: status (context) ->
    header -> fields -> body section(s) (chunked) -> action prompt (chunked) ->
    buttons (nav-leftmost) -> links (URL buttons) -> footer.

    ``nav`` is the agent's no-dead-ends hub button: when present and enabled, the
    hub button is appended to the reply's buttons (via ``ensure_hub_button``, the
    platform-owned append policy) before the actions block is built. When ``nav``
    is None or disabled the output is byte-identical to no-nav; ``ensure_hub_button``
    also no-ops when a button already links to the hub."""
    blocks: list[dict[str, Any]] = []
    if reply.status:
        blocks.append(_context_block(reply.status))
    if reply.header:
        # Slack hard-caps a header's plain_text at 150 chars.
        header_text = _truncate(reply.header, _HEADER_MAX)
        blocks.append({"type": "header", "text": {"type": "plain_text", "text": header_text}})
    if reply.fields:
        # Slack hard-caps a section at 10 fields; keep the first 10, order preserved.
        blocks.append(
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*{k}*\n{v}"}
                    for k, v in reply.fields[:_SECTION_FIELDS_MAX]
                ],
            }
        )
    for piece in chunk(to_mrkdwn(reply.text)):
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": piece}})
    # The question attached to the buttons (#454) -- rendered as its own section
    # immediately before the actions block, never merged into `reply.text`, so a
    # ConfirmIntent/ChoiceIntent's `prompt` reaches the approver/chooser instead
    # of showing bare button labels with no question attached.
    if reply.action_prompt:
        for piece in chunk(to_mrkdwn(reply.action_prompt)):
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": piece}})
    # ``reply.buttons`` are already (label, action_id) pairs, the same (label,
    # command) shape ensure_hub_button operates on, so no conversion is needed.
    buttons = (
        reply.buttons if nav is None else ensure_hub_button(BehaviorPacks(nav=nav), reply.buttons)
    )
    if buttons:
        ordered = sorted(buttons, key=lambda b: 0 if _is_nav(b[0]) else 1)
        blocks.append(
            {"type": "actions", "elements": [_button(label, aid) for label, aid in ordered]}
        )
    valid_links = [(label, url) for label, url in reply.links if _is_http_url(url)]
    if valid_links:
        blocks.append(
            {
                "type": "actions",
                "elements": [_link_button(label, url) for label, url in valid_links],
            }
        )
    if reply.footer:
        blocks.append(_context_block(reply.footer))
    # Slack hard-caps a message at 50 blocks; drop the tail overflow last so the
    # ordering/priority of the earlier blocks is preserved.
    return blocks[:_BLOCKS_MAX]


def _pairs(raw: Any) -> list[tuple[str, str]]:
    """Coerce a JSON list of [k, v] pairs to tuples, dropping malformed entries."""
    out: list[tuple[str, str]] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, list | tuple) and len(item) == 2:
                out.append((str(item[0]), str(item[1])))
    return out


def _reply_from_message(message: OutboundMessage) -> Reply:
    """Project the channel-neutral contract into this Slack renderer's inputs."""
    buttons: list[tuple[str, str]] = []
    action_prompt: str | None = None
    if isinstance(message.interaction, ChoiceIntent):
        buttons = [(option.label, option.value) for option in message.interaction.options]
        action_prompt = message.interaction.prompt
    elif isinstance(message.interaction, ConfirmIntent):
        buttons = [
            (message.interaction.confirm.label, message.interaction.confirm.value),
            (message.interaction.cancel.label, message.interaction.cancel.value),
        ]
        action_prompt = message.interaction.prompt
    return Reply(
        text=message.text,
        status=message.status,
        header=message.header,
        fields=[(item.label, item.value) for item in message.fields],
        buttons=buttons,
        links=[(item.label, item.url) for item in message.links],
        footer=message.footer,
        action_prompt=action_prompt,
    )


def parse_reply(text: str) -> Reply | None:
    """A ``Reply`` if ``text`` contains a complete, valid ``curie-reply`` block,
    else None. Defensive: any parse/shape error returns None so the caller falls
    back to plain text rather than surfacing a broken reply."""
    match = _FENCE.search(text)
    if match is None:
        return None
    try:
        data = json.loads(match.group(1))
    except (ValueError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    if "version" in data:
        try:
            return _reply_from_message(OutboundMessage.model_validate(data))
        except ValidationError:
            return None
    # Migration-only compatibility for the original unversioned convention.
    # New agents author the versioned channel-protocol shape above.
    status = data.get("status")
    header = data.get("header")
    footer = data.get("footer")
    return Reply(
        text=str(data.get("text", "")),
        status=str(status) if status is not None else None,
        header=str(header) if header is not None else None,
        fields=_pairs(data.get("fields")),
        buttons=_pairs(data.get("buttons")),
        links=_pairs(data.get("links")),
        footer=str(footer) if footer is not None else None,
    )


def render(text: str, nav: NavPack | None = None) -> tuple[str, list[dict[str, Any]] | None]:
    """Map an outbound reply string to ``(text, blocks)`` for ``chat.update``.

    - A complete ``curie-reply`` block -> (plain fallback text, blocks).
    - A half-streamed block (fence opened, not closed) -> (text before the fence,
      None), so streaming never shows raw JSON.
    - Anything else -> (mrkdwn text, None), the existing plain path.

    ``nav`` (the agent's hub-button pack, threaded from the kernel) is applied to
    a complete structured reply's buttons; None/disabled leaves output unchanged.
    """
    reply = parse_reply(text)
    if reply is not None:
        return (
            _truncate(to_mrkdwn(reply.text) or "(reply)", _SLACK_TEXT_MAX),
            to_blocks(reply, nav),
        )
    open_at = text.find(_FENCE_OPEN)
    if open_at != -1:
        prefix = text[:open_at].strip()
        return (to_mrkdwn(prefix) if prefix else "…", None)
    return (to_mrkdwn(text), None)


# --- The approval card (#246, ADR-0010) ------------------------------------------

# Slack caps a section's mrkdwn at 3000 chars; the summary is clamped well under
# it so the card never bounces (the durable record keeps the full text).
_APPROVAL_SUMMARY_MAX = 2900


def approval_card(
    *,
    approval_id: str,
    summary: str,
    requested_by: str,
    allow_free_text: bool = False,
) -> tuple[str, list[dict[str, Any]]]:
    """The Block Kit approval card: what needs approval plus Approve/Reject.

    The button action ids are the dispatcher's click-to-resolve contract
    (``curie_dispatcher.approval_actions``); each button's ``value`` carries
    the durable record id, so a click resolves exactly this approval. Returns
    ``(fallback_text, blocks)`` for ``chat.postMessage``.

    ``allow_free_text`` is the ``ConfirmIntent`` field of the same name (ADR-0020),
    rendered HERE because that is where a semantic intent becomes a widget: Slack
    expresses "this decision may carry free text" as a dialog opened by the click,
    so the card carries the note-collecting action ids instead of the immediate
    ones. The terminal renderer expresses the same field as a typed reply
    (``cli/src/channel.rs``); until now the Slack side ignored it, which left one
    renderer honoring the field and its twin dropping it.
    """

    # Imported here (not module top) to keep the module importable in isolation;
    # the worker package already depends on the dispatcher for the queue seam.
    from curie_dispatcher.approval_actions import (
        APPROVAL_CARD_HEADER,
        APPROVE_ACTION_ID,
        APPROVE_NOTE_ACTION_ID,
        REJECT_ACTION_ID,
        REJECT_NOTE_ACTION_ID,
    )

    approve_action = APPROVE_NOTE_ACTION_ID if allow_free_text else APPROVE_ACTION_ID
    reject_action = REJECT_NOTE_ACTION_ID if allow_free_text else REJECT_ACTION_ID

    clamped = _truncate(to_mrkdwn(summary), _APPROVAL_SUMMARY_MAX)
    fallback = _truncate(f"Approval required: {summary}", _SLACK_TEXT_MAX)
    blocks: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": _truncate(APPROVAL_CARD_HEADER, _HEADER_MAX),
                "emoji": True,
            },
        },
        {"type": "section", "text": {"type": "mrkdwn", "text": clamped}},
        _context_block(f"Requested by <@{requested_by}>"),
        {
            "type": "actions",
            "elements": [
                {
                    **_button_shell("Approve"),
                    "style": "primary",
                    "action_id": _truncate(approve_action, _ACTION_ID_MAX),
                    "value": approval_id,
                },
                {
                    **_button_shell("Reject"),
                    "style": "danger",
                    "action_id": _truncate(reject_action, _ACTION_ID_MAX),
                    "value": approval_id,
                },
            ],
        },
    ]
    return fallback, blocks


def resolved_approval_card(
    *, summary: str, requested_by: str, decision: str, resolver: str, note: str | None
) -> tuple[str, list[dict[str, Any]]]:
    """The approval card rebuilt in its RESOLVED form (#1084).

    A thin adapter over ``curie_dispatcher.approval_actions.settled_approval_card``
    and ``settled_verdict_line`` rather than a second renderer: the dispatcher's
    click path settles the very same card, and two renderers on one surface is
    exactly the divergence #1084 was filed to stop. Same import direction as the
    action ids above, for the same reason.

    Unlike ``expired_approval_card`` this needs the requester and the verdict,
    because a resolved card states who decided and why, where an expired one
    states only that nobody did.
    """

    from curie_dispatcher.approval_actions import (
        settled_approval_card,
        settled_verdict_line,
    )

    return settled_approval_card(
        summary=_truncate(to_mrkdwn(summary), _APPROVAL_SUMMARY_MAX),
        requested_by=requested_by,
        verdict=settled_verdict_line(decision=decision, resolver=resolver, note=note),
    )


def expired_approval_card(*, summary: str) -> tuple[str, list[dict[str, Any]]]:
    """The approval card rebuilt in its EXPIRED, no-longer-actionable form (#419).

    The same summary the live card showed, with the Approve/Reject actions block
    dropped and an expiry line in its place -- so the card cannot be clicked and
    reads as settled. This is the expiry mirror of the dispatcher's resolved-card
    edit, rebuilt from the remembered summary because the worker does not keep the
    original card's blocks. Returns ``(fallback_text, blocks)`` for ``chat.update``.
    """

    clamped = _truncate(to_mrkdwn(summary), _APPROVAL_SUMMARY_MAX)
    fallback = _truncate(f"Approval expired: {summary}", _SLACK_TEXT_MAX)
    blocks: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": _truncate("Approval expired", _HEADER_MAX),
                "emoji": True,
            },
        },
        {"type": "section", "text": {"type": "mrkdwn", "text": clamped}},
        _context_block(
            "This request expired and can no longer be approved or rejected."
        ),
    ]
    return fallback, blocks


# The dispatcher's click-to-undo contract, defined here beside the renderer that
# emits it. A click carries the durable action id in the button's ``value``, the
# same way an approval click carries its record id.
UNDO_ACTION_ID = "curie_undo_action"

_RECEIPT_SUMMARY_MAX = 200


def receipt_card(actions: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    """What this turn did to the world, one line per action.

    Both kinds of line are on the card on purpose. A receipt that listed only the
    undoable actions would be a receipt that hides the ones that matter most: the
    value of showing "restarting pods cannot be undone" next to "scaled 3 to 10,
    undo" is that an operator can see the system knows the difference.

    Each row is a dict from the ledger's ``ActionOut``: ``id``, ``tool``,
    ``undoable``, an optional ``summary``, and an optional
    ``irreversible_reason``. A row with neither a summary nor a reason still
    renders, naming the tool, because an action nobody can describe is still an
    action that happened.
    """

    undoable = [a for a in actions if a.get("undoable")]
    header = (
        f"Did {len(actions)} thing{'s' if len(actions) != 1 else ''} to your systems"
        f"{f', {len(undoable)} undoable' if undoable else ''}"
    )
    blocks: list[dict[str, Any]] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*{header}*"}}
    ]
    lines: list[str] = []
    for action in actions:
        described = action.get("summary") or f"called `{action.get('tool', 'a tool')}`"
        text = _truncate(to_mrkdwn(str(described)), _RECEIPT_SUMMARY_MAX)
        if action.get("undoable"):
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": text},
                    "accessory": {
                        **_button("Undo", UNDO_ACTION_ID),
                        "value": str(action.get("id", "")),
                    },
                }
            )
            lines.append(f"{described} (undoable)")
        else:
            # The stated reason, or the honest absence of one. An undeclared tool
            # is not the same as a tool that explained itself, and the card says
            # which happened rather than flattening both to "cannot be undone".
            reason = action.get("irreversible_reason") or (
                "cannot be undone: nothing reported a prior state"
            )
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
            blocks.append(_context_block(_truncate(str(reason), _RECEIPT_SUMMARY_MAX)))
            lines.append(f"{described} ({reason})")
    fallback = _truncate(header + ": " + "; ".join(lines), _SLACK_TEXT_MAX)
    return fallback, blocks
