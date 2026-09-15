"""The click-to-resolve flow for approval cards (#246, ADR-0010).

The worker posts a Block Kit approval card whose Approve/Reject buttons carry
these action ids; a click arrives here over the authenticated Socket Mode
websocket and is forwarded to the platform API's resolve endpoint, where the
authorizer decides server-side whether this actor may resolve (channel
membership). The dispatcher never decides authorization itself -- it attests
the user and channel Slack authenticated, then renders the API's verdict back
into Slack:

- the winner's card is edited in place (buttons removed, verdict stamped);
- a non-approver gets the ephemeral "you are not an approver" rejection;
- a loser of the claim race gets the ephemeral "already resolved by X";
- an expired record gets the ephemeral expiry notice.

The action-id constants live here (not in the worker, which renders the card)
because the worker already depends on this package for the queue seam; the
card renderer imports them so the two sides cannot drift.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

import httpx
from curie_telemetry import inject_trace_context
from slack_sdk.web import WebClient

from .approval_principal import mint_chat_principal
from .config import DispatcherConfig

logger = logging.getLogger(__name__)

# Block Kit action ids for the approval card's buttons. The button ``value``
# carries the approval record id. process_action's catch-all must skip these
# (Bolt runs every matching listener, so without the skip a click would ALSO
# be normalized into an ordinary turn).
APPROVE_ACTION_ID = "curie-approval-approve"
REJECT_ACTION_ID = "curie-approval-reject"

# The note-collecting variants (#1053). A card rendered from a ``ConfirmIntent``
# with ``allow_free_text`` carries these instead, and a click on one opens a
# dialog for an optional note before resolving. They are a SECOND action-id pair
# rather than a flag inside the button ``value`` so the behavior is visible in
# the interaction payload itself and so the renderer and the handler keep sharing
# one set of literals -- the worker's ``blocks.approval_card`` imports these, the
# same anti-drift arrangement the original pair already uses.
APPROVE_NOTE_ACTION_ID = "curie-approval-approve-note"
REJECT_NOTE_ACTION_ID = "curie-approval-reject-note"

# The dialog's ``callback_id`` and the block/action ids of its one input. The
# approval id, the card's channel and ts, and the decision ride in
# ``private_metadata`` because a view submission carries no channel or message of
# its own.
NOTE_MODAL_CALLBACK_ID = "curie-approval-note"
_NOTE_BLOCK_ID = "note"
_NOTE_ACTION_ID = "note-input"

# The human-facing note limit, declared on the modal's input so Slack shows a
# counter and blocks submit (#1077). Slack would otherwise accept up to 3000, and
# the verdict line concatenates the note onto an attribution prefix, so an
# unbounded note produces a text object over the cap and ``chat_update`` raises.
_NOTE_MAX_LENGTH = 2000

# The structural backstop on the stamped context line, applied in
# ``_verdict_line`` so a non-modal caller is guarded too. It mirrors the value
# the codebase already settled on for the same class of problem: the worker's
# ``_APPROVAL_SUMMARY_MAX`` and ``_CHUNK_TARGET``, both in
# ``curie_worker.blocks``, are the same 2900 (copied rather than imported: the
# dispatcher must not depend on apps/worker). The three share a derivation from
# Slack's text-object limit, not a coupling to each other -- two of them guard
# different block types in a different service, and nothing gates any of the
# three against the others; this comment naming the other two is the only
# cross-reference there is.
#
# Assumption, stated because it is inferred rather than cited: Slack documents
# 3000 as the general maximum for a text object's ``text``, but documents no
# per-element cap for the elements of a CONTEXT block specifically. 2900 is
# therefore a deliberate margin under an inferred limit, not a documented one.
_VERDICT_LINE_MAX = 2900

# The ``chat_update`` ``text`` argument is the notification/preview/screen-reader
# fallback, NOT a block text object, and Slack caps it far higher (the worker's
# own renderer clamps at 39000 in ``curie_worker.blocks``). It gets its own bound
# because #1073 made it carry the card summary as well as the verdict, so the
# verdict's context-block clamp above no longer covers it.
_FALLBACK_TEXT_MAX = 39000

# The live card's header, shared with the settled rebuild so the two cannot
# drift: a rebuild under a different heading is a visibly different card for the
# same decision. ``curie_worker.blocks.approval_card`` imports this rather than
# repeating the literal.
_APPROVAL_CARD_HEADER = "Approval required"
APPROVAL_CARD_HEADER = _APPROVAL_CARD_HEADER

_APPROVAL_ACTION_IDS = frozenset(
    {
        APPROVE_ACTION_ID,
        REJECT_ACTION_ID,
        APPROVE_NOTE_ACTION_ID,
        REJECT_NOTE_ACTION_ID,
    }
)

# Which decision each action id resolves to, so the two pairs cannot drift on it.
_DECISION_BY_ACTION_ID = {
    APPROVE_ACTION_ID: "approved",
    APPROVE_NOTE_ACTION_ID: "approved",
    REJECT_ACTION_ID: "rejected",
    REJECT_NOTE_ACTION_ID: "rejected",
}

# FastAPI's approvals router uses this exact detail when the requested row is
# absent from THIS release's database. With two releases sharing one Socket Mode
# app, that absence is an ownership signal rather than proof that the approval
# was deleted: Slack may have delivered the interaction to the other release.
# Keep this narrower than status 404 so an ingress/proxy route miss ("Not Found")
# does not masquerade as release affinity.
_APPROVAL_NOT_FOUND_DETAIL = "approval not found"


def is_approval_action(action_id: str) -> bool:
    """True when a Block Kit action id belongs to the approval card."""

    return action_id in _APPROVAL_ACTION_IDS


def is_release_ownership_miss(outcome: ResolveOutcome) -> bool:
    """True when this release's API does not have the approval row (#2248)."""

    return (
        outcome.status_code == 404
        and outcome.detail.strip().casefold() == _APPROVAL_NOT_FOUND_DETAIL
    )


def decline_unowned_envelope(
    ack: Any,
    *,
    approval_id: str,
    log: logging.Logger,
    web_client: WebClient | None = None,
    channel: str | None = None,
    user: str | None = None,
) -> None:
    """Leave the Socket Mode envelope unacked so Slack retries another connection.

    Bolt's SocketModeHandler only emits the envelope ack when the BoltResponse
    status is 200 (slack_bolt.adapter.socket_mode.internals.send_response).
    Assigning a non-200 to ack.response unblocks the listener runner, which
    waits on ``ack.response is None``, without acknowledging. Slack then
    retries the same envelope on another connection of the same app.

    When ``web_client``, ``channel``, and ``user`` are all present, also post
    an ephemeral telling the clicker to disconnect the extra Socket Mode
    client. That notice must not ack the envelope or mutate the card.
    """

    from slack_bolt.response import BoltResponse

    log.warning(
        "approval %s was not found in this release and may be owned by another Curie release",
        approval_id,
    )
    ack.response = BoltResponse(status=404, body="")
    if web_client is not None and channel and user:
        _ephemeral(
            web_client,
            channel=channel,
            user=user,
            text=_refusal_text(ResolveOutcome(status_code=404, detail=_APPROVAL_NOT_FOUND_DETAIL)),
            log=log,
        )


def this_release_owns_action(body: dict[str, Any], resolver: ApprovalResolveClient) -> bool | None:
    """Ownership probe for a note-dialog click, before views.open."""

    actions = body.get("actions") or []
    approval_id = str(actions[0].get("value") or "") if actions else ""
    if not approval_id:
        return True
    return resolver.exists(approval_id)


def _http_json(response: httpx.Response) -> tuple[str, dict[str, Any] | None]:
    """Parse a JSON body for detail; never surface a non-JSON intermediary page."""

    try:
        parsed = response.json()
    except ValueError:
        return "", None
    if not isinstance(parsed, dict):
        return "", None
    return str(parsed.get("detail", "")), parsed


@dataclass(frozen=True)
class ResolveOutcome:
    """The API's verdict on one resolution attempt, normalized for rendering."""

    status_code: int
    detail: str = ""
    resolved_by: str | None = None
    decision: str | None = None


@dataclass(frozen=True)
class NoteSubmission:
    """A note dialog that has already been resolved, carried across the ack.

    Everything the render half needs travels in here so that no Slack call has
    to precede the ack (#1077): ``resolve_note_submission`` fills it in and
    ``render_note_submission`` consumes it, with ``ack()`` in between.
    ``response_action`` is the body Bolt must ack the view with -- None closes
    the dialog, and an ``errors`` response keeps it open with the reason
    attached to the note field.
    """

    approval_id: str
    decision: str
    user: str
    channel: str
    card_ts: str
    note: str | None
    outcome: ResolveOutcome
    response_action: dict[str, Any] | None


# The resolve POST is the only network round trip left inside the
# ``view_submission`` ack budget (#1077), so it must give up well inside Slack's
# three seconds. All four phases are named explicitly because httpx timeouts are
# PER PHASE and the phases are sequential: any phase left at its default silently
# joins the sum. A single-argument ``httpx.Timeout(1.0, connect=0.5)`` reads like
# 1.5s and is actually 3.5s, over the deadline this constant exists to fit inside.
#
# Be precise about what the four numbers bound: each caps INACTIVITY inside its own
# I/O operation, not elapsed wall clock. pool 0.1 + connect 0.4 + write 0.3 + read
# 1.4 = 2.2s is the worst case for a response that arrives in the normal way, which
# leaves real margin inside the three seconds for websocket transit and Bolt's own
# dispatch. It is NOT a guaranteed ceiling. A slowly-streaming response resets the
# read clock on every chunk, so it can run past the ack budget without ever
# tripping a timeout, and httpx cannot express a total-request deadline at all.
# What makes that acceptable is the peer rather than the setting: this talks to the
# platform API, which answers with one small JSON body that lands in a single read,
# so there is no trickle for the read clock to keep forgiving.
#
# The shape of the split is deliberate. Read gets the largest share because it is
# the phase that legitimately waits on the authorizer doing work: a group-bound
# approval whose membership cache has expired makes the API perform a live Slack
# lookup inside this request. Connect is tighter because a platform API that has
# not accepted the socket in 0.4s is down, not busy. Pool is tiny because pool
# exhaustion is near-impossible here (five Bolt listener workers against httpx's
# default pool of 100), so failing fast is the right answer if it ever happens.
#
# Timing out yields ``ResolveOutcome(status_code=0)``. Be clear about what that
# does and does not mean: for anything past the connect phase the request WAS
# delivered, so the outcome is UNKNOWN to the dispatcher and the server may well
# have committed the resolution. What keeps a retry safe is the server-side
# compare-and-set, not the record being untouched -- a retry of a decision that
# did land comes back 409 instead of resolving it a second time.
_RESOLVE_TIMEOUT = httpx.Timeout(connect=0.4, read=1.4, write=0.3, pool=0.1)


class ApprovalResolveClient:
    """Thin client for authenticated chat approval resolution."""

    def __init__(
        self,
        *,
        api_base_url: str,
        api_key: str,
        approval_chat_attester_secret: str,
        client: httpx.Client | None = None,
    ) -> None:
        self._base = api_base_url.rstrip("/")
        self._headers = {"X-API-Key": api_key} if api_key else {}
        self._approval_chat_attester_secret = approval_chat_attester_secret
        self._client = client or httpx.Client(timeout=_RESOLVE_TIMEOUT)

    def resolve(
        self,
        approval_id: str,
        *,
        decision: str,
        attested_user: str,
        attested_channel: str,
        note: str | None = None,
    ) -> ResolveOutcome:
        body: dict[str, Any] = {"decision": decision}
        # Only send the key when the approver typed something. The field is
        # optional on ``ApprovalResolve`` and an empty string is not the same
        # statement as leaving the note blank: it would persist as a
        # resolution_note the resume turn then interpolates as ``Note: .``.
        if note:
            body["note"] = note
        headers = {
            **self._headers,
            "X-Curie-Approval-Principal": mint_chat_principal(
                self._approval_chat_attester_secret,
                subject=attested_user,
                actor_channel=attested_channel,
                approval_id=approval_id,
            ),
        }
        inject_trace_context(headers)
        try:
            response = self._client.post(
                f"{self._base}/approvals/{approval_id}/resolve",
                json=body,
                headers=headers,
            )
        except httpx.HTTPError as exc:
            logger.warning("approval resolve call failed for %s: %s", approval_id, exc)
            return ResolveOutcome(status_code=0, detail=str(exc))
        detail, parsed = _http_json(response)
        resolved = parsed.get("resolved_by") if parsed else None
        decided = parsed.get("status") if parsed else None
        return ResolveOutcome(
            status_code=response.status_code,
            detail=detail,
            resolved_by=str(resolved) if resolved else None,
            decision=str(decided) if decided else None,
        )

    def exists(self, approval_id: str) -> bool | None:
        """Whether THIS release's API has the approval row.

        True when GET /approvals/{id} is 200. False when the response is the
        exact row-miss 404 the API and dispatcher freeze in
        tests/vectors/approval-ownership.json -- that is the ownership signal
        for two dispatchers sharing one Slack app. None when the probe failed
        or returned some other error; callers treat None as "do not decline".
        """

        try:
            response = self._client.get(
                f"{self._base}/approvals/{approval_id}",
                headers=self._headers,
            )
        except httpx.HTTPError as exc:
            logger.warning("approval existence probe failed for %s: %s", approval_id, exc)
            return None
        if response.status_code == 200:
            return True
        detail, _parsed = _http_json(response)
        if response.status_code == 404 and detail.strip().casefold() == _APPROVAL_NOT_FOUND_DETAIL:
            return False
        return None


def settled_approval_card(
    *, summary: str, requested_by: str, verdict: str
) -> tuple[str, list[dict[str, Any]]]:
    """The approval card in its SETTLED form, rebuilt rather than edited (#1084).

    Two paths settle a card and they have different inputs. The dispatcher holds
    the live message and edits it (``_resolved_card_blocks``); the worker's
    resume path holds only what it remembered at pause time and must rebuild.
    Left to themselves those two produce different-looking cards for the same
    decision, which is the divergence #1084 exists to prevent, so this renders
    the rebuild to match the edit exactly: the same header, the same summary
    section, the same requested-by context, the actions block gone, and the
    verdict appended as a context line.

    "Exactly" is asserted rather than asserted-in-a-docstring: a test renders a
    live card through ``curie_worker.blocks.approval_card``, settles it both
    ways, and compares. That test is the contract; this docstring only says why
    it exists.

    Lives here, not in the worker's renderer, because the dependency runs one
    way: ``curie_worker.blocks`` already imports this module for the action ids,
    for the same anti-drift reason, and the reverse import would be a cycle.
    """

    blocks: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": _APPROVAL_CARD_HEADER,
                "emoji": True,
            },
        },
        {"type": "section", "text": {"type": "mrkdwn", "text": summary}},
    ]
    # Omitted rather than rendered empty when unknown: a card remembered before
    # #1084 carries no requester, and "Requested by <@>" reads as a bug.
    if requested_by:
        blocks.append(
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"Requested by <@{requested_by}>"}],
            }
        )
    blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": verdict}]})
    fallback = f"{verdict}\n{summary}".strip()
    if len(fallback) > _FALLBACK_TEXT_MAX:
        fallback = fallback[: _FALLBACK_TEXT_MAX - 1] + "\u2026"
    return fallback, blocks


def settled_verdict_line(*, decision: str, resolver: str, note: str | None) -> str:
    """The verdict line a settled card shows, for callers outside this module.

    The public name for ``_verdict_line``: the worker needs the identical string
    the click path stamps, and re-deriving it there is how the two surfaces would
    start wording the same decision differently.
    """

    return _verdict_line(decision, resolver, note)


def _card_is_readable(message: dict[str, Any]) -> bool:
    """Whether ``message`` carries enough to stamp WITHOUT losing the card body.

    The defense in depth behind the ``conversations.replies`` fix (#1073). That
    fix makes the read work for both card shapes; this makes the failure mode
    survivable if it ever stops working, because the two costs are wildly
    asymmetric. Skipping a stamp leaves a settled record with live-looking
    buttons, which the next click reports as already-resolved. Stamping from an
    unread card DESTROYS the only Slack-side record of what was approved, and
    the resumed run then streams over the thread's other copy of it.

    So: no blocks, no stamp. A card with only its actions block is equally
    unstampable -- filtering that leaves nothing but the verdict.
    """

    return any(b.get("type") != "actions" for b in message.get("blocks") or [])


def _card_summary_text(message: dict[str, Any]) -> str:
    """The card's body as plain text, for the ``chat_update`` ``text`` fallback.

    ``text`` is what notifications, previews, and screen readers use, so
    stamping with the verdict alone loses the summary from all three even when
    the blocks keep it (#1073). Pulls the section blocks' text and leaves the
    header and the context lines out: the header is a constant and the context
    is the "requested by" line, neither of which is the thing being approved.
    """

    parts = [
        text
        for block in message.get("blocks") or []
        if block.get("type") == "section"
        for text in [(block.get("text") or {}).get("text")]
        if text
    ]
    return "\n".join(parts)


def _fallback_text(verdict: str, message: dict[str, Any]) -> str:
    """The ``chat_update`` ``text`` for a settled card: verdict then summary.

    Clamped as a whole. The verdict is already bounded by ``_VERDICT_LINE_MAX``,
    but the summary appended after it is not, so the pair needs its own bound or
    a long card body can push the fallback past what Slack accepts and lose the
    edit entirely -- which would leave the live buttons up, the failure this
    whole change exists to avoid.
    """

    combined = f"{verdict}\n{_card_summary_text(message)}".strip()
    if len(combined) <= _FALLBACK_TEXT_MAX:
        return combined
    return combined[: _FALLBACK_TEXT_MAX - 1] + "\u2026"


def _resolved_card_blocks(original: dict[str, Any], verdict: str) -> list[dict[str, Any]]:
    """The clicked card with its buttons replaced by the verdict line.

    Every non-actions block of the original message is kept (the summary stays
    readable in place); the actions block is swapped for a context line naming
    the decision and the resolver, so the card cannot be clicked twice.

    Callers must gate this on ``_card_is_readable``: handed an unread message it
    returns the verdict alone, which as a ``chat_update`` payload is a wipe.
    """

    blocks = [b for b in original.get("blocks", []) if b.get("type") != "actions"]
    blocks.append(
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": verdict}],
        }
    )
    return blocks


def escape_mrkdwn(text: str) -> str:
    """Neutralize Slack's control sequences in text a person typed (#1074).

    The verdict line is rendered as ``mrkdwn`` in a bot-authored context block,
    so an approver's note was interpreted rather than shown: ``<!channel>``
    pinged the whole room and ``*text*`` forged emphasis in a message attributed
    to Curie. An approver is authorized to make a binary decision on one gated
    action, not to broadcast to a channel or to write in the platform's voice.

    Escaping the three characters Slack's own documentation names is enough and
    is what Slack recommends: ``<`` is what opens every control sequence (a
    broadcast, a user mention, a link), and ``&`` must go first or it would
    double-escape the entities the other two produce. ``*``/``_``/`` ` `` are
    deliberately NOT escaped -- they are cosmetic, they render inertly, and
    stripping them would mangle a note that legitimately quotes code.

    Args:
        text: the approver-supplied note, verbatim.

    Returns:
        The note with Slack's control characters rendered as literals.
    """
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _verdict_line(decision: str, user: str, note: str | None) -> str:
    """The context line stamped onto a settled card.

    The note is shown HERE as well as on the requester's thread: the approver
    channel is where the next person looks to understand a decision, and a
    reason that only reached the requester leaves that channel with a bare
    verdict.
    """

    # Build the whole line, then cut only if it does not fit. The cut takes from
    # the tail, which is the note, so the attribution survives in preference to
    # it: who decided is the part of this line that must survive. The cut is
    # marked so a reader can tell the card is showing an excerpt; the durable
    # record still holds the whole note, and the requester's resume turn
    # interpolates that one. The marker is the same single ellipsis character
    # the worker's ``_truncate`` in ``curie_worker.blocks`` uses, so a truncated
    # note ends the same way whichever service stamped the card.
    line = f"{decision.capitalize()} by <@{user}>"
    if note:
        line = f"{line}\nNote: {escape_mrkdwn(note)}"
    if len(line) <= _VERDICT_LINE_MAX:
        return line
    return line[: _VERDICT_LINE_MAX - 1] + "…"


def build_note_modal(
    *, approval_id: str, channel: str, card_ts: str, decision: str
) -> dict[str, Any]:
    """The dialog that collects an OPTIONAL note before resolving (#1053).

    A view submission carries no channel and no message of its own, so
    everything the submit handler needs to finish the job rides in
    ``private_metadata``. The input block is ``optional``: the note is an
    enrichment, and requiring one would turn a decision into a form.
    """

    verb = "Approve" if decision == "approved" else "Reject"
    label = "Note (optional)" if decision == "approved" else "Reason (optional)"
    return {
        "type": "modal",
        "callback_id": NOTE_MODAL_CALLBACK_ID,
        "private_metadata": json.dumps(
            {
                "approval_id": approval_id,
                "channel": channel,
                "card_ts": card_ts,
                "decision": decision,
            }
        ),
        "title": {"type": "plain_text", "text": f"{verb} request"},
        "submit": {"type": "plain_text", "text": verb},
        "close": {"type": "plain_text", "text": "Cancel"},
        "blocks": [
            {
                "type": "input",
                "block_id": _NOTE_BLOCK_ID,
                "optional": True,
                "label": {"type": "plain_text", "text": label},
                "element": {
                    "type": "plain_text_input",
                    "action_id": _NOTE_ACTION_ID,
                    "multiline": True,
                    "max_length": _NOTE_MAX_LENGTH,
                },
            }
        ],
    }


def open_note_dialog(
    *,
    body: dict[str, Any],
    decision: str,
    web_client: WebClient,
    resolver: ApprovalResolveClient,
    logger: logging.Logger | None = None,
) -> ResolveOutcome | None:
    """Open the note dialog for a click, or fall forward and resolve without one.

    A ``trigger_id`` is valid for about three seconds, so ``views.open`` can
    genuinely fail. When it does this resolves anyway, with no note, and says so
    in an ephemeral. The human already expressed the decision by clicking;
    refusing the click because an optional enrichment could not be collected
    would be the worse failure, and it would leave the record pending with no
    feedback.

    Returns None on the normal path (nothing is resolved until the dialog is
    submitted), or the fall-forward resolution's outcome.
    """

    log = logger or logging.getLogger(__name__)

    actions = body.get("actions") or []
    approval_id = str(actions[0].get("value") or "") if actions else ""
    channel = (body.get("channel") or {}).get("id") or ""
    user = (body.get("user") or {}).get("id") or ""
    message = body.get("message") or {}
    card_ts = message.get("ts") or ""
    trigger_id = body.get("trigger_id") or ""
    if not approval_id or not channel or not user or not card_ts:
        log.info("approval action without id/channel/user/message, skipping")
        return None

    try:
        web_client.views_open(
            trigger_id=trigger_id,
            view=build_note_modal(
                approval_id=approval_id,
                channel=channel,
                card_ts=card_ts,
                decision=decision,
            ),
        )
        return None
    except Exception as exc:  # noqa: BLE001 - the decision must still land
        log.warning("note dialog failed to open for %s: %s", approval_id, exc)

    outcome = _resolve_and_render(
        approval_id=approval_id,
        decision=decision,
        user=user,
        channel=channel,
        card_ts=card_ts,
        message=message,
        note=None,
        web_client=web_client,
        resolver=resolver,
        log=log,
    )
    # Every outcome is rendered, not just the success (#1085). The in-view error
    # channel the submit path uses cannot fire here -- there is no view, that is
    # why we are on this path at all -- so the ephemeral is the ONLY surface left.
    # Reporting only the 200 left a refused clicker with total silence, which for
    # a non-approver is indistinguishable from the platform being down, and for an
    # expired approval hides the one fact they need.
    if outcome.status_code == 200:
        text = (
            "The note box could not be opened, so this was resolved without a "
            "note. Add one with `curie <tier> approvals <agent> --resolve` if "
            "it matters."
        )
    else:
        # The same wording the in-view path renders, so a refusal reads
        # identically whether or not the dialog managed to open, and the three
        # refusal classes stay distinguishable (#453 AC5).
        text = _refusal_text(outcome)
    _ephemeral(web_client, channel=channel, user=user, text=text, log=log)
    return outcome


def resolve_note_submission(
    *,
    body: dict[str, Any],
    resolver: ApprovalResolveClient,
    logger: logging.Logger | None = None,
) -> NoteSubmission | None:
    """Decide a submitted note dialog, making no Slack call at all (#1053, #1077).

    The pre-ack half. It parses ``private_metadata``, extracts the note, and
    resolves; the returned ``NoteSubmission`` carries the body Bolt must ack the
    view with, so the caller can ack immediately and render afterwards. Returns
    None when the payload is unusable, in which case there is nothing to render.

    The resolve genuinely cannot move past the ack: a view ack CARRIES the
    response. That second channel exists because the claim race widened.
    Resolution now happens at submit rather than at click, so two approvers can
    hold open dialogs at once. The compare-and-set still makes exactly one win,
    but the loser is now standing inside a modal, where an ephemeral is
    invisible -- so every refusal is rendered INTO the view rather than posted
    behind it.

    It takes no ``web_client``: that is what makes "nothing talks to Slack before
    the ack" a structural property of the signature rather than a matter of
    ordering discipline inside the body.
    """

    log = logger or logging.getLogger(__name__)

    view = body.get("view") or {}
    try:
        meta = json.loads(view.get("private_metadata") or "{}")
    except ValueError:
        meta = {}
    approval_id = str(meta.get("approval_id") or "")
    channel = str(meta.get("channel") or "")
    card_ts = str(meta.get("card_ts") or "")
    decision = str(meta.get("decision") or "")
    user = (body.get("user") or {}).get("id") or ""
    if not approval_id or not channel or not user or decision not in ("approved", "rejected"):
        log.info("note submission with unusable private_metadata, skipping")
        return None

    values = (view.get("state") or {}).get("values") or {}
    note = ((values.get(_NOTE_BLOCK_ID) or {}).get(_NOTE_ACTION_ID) or {}).get("value")
    note = (note or "").strip() or None

    outcome = resolver.resolve(
        approval_id,
        decision=decision,
        attested_user=user,
        attested_channel=channel,
        note=note,
    )
    response_action = (
        None
        if outcome.status_code == 200
        else {
            "response_action": "errors",
            "errors": {_NOTE_BLOCK_ID: _refusal_text(outcome)},
        }
    )
    return NoteSubmission(
        approval_id=approval_id,
        decision=decision,
        user=user,
        channel=channel,
        card_ts=card_ts,
        note=note,
        outcome=outcome,
        response_action=response_action,
    )


def render_note_submission(
    submission: NoteSubmission,
    *,
    web_client: WebClient,
    logger: logging.Logger | None = None,
) -> None:
    """Stamp the card for an already-decided submission (#1077).

    The post-ack half, and the only place the dialog path talks to Slack. It
    never raises: by the time it runs the view has been acked and there is no
    surface left to report an error on.

    Note the deliberate consequence of running here: the card is now read AFTER
    the resolve rather than before, so a claim-race loser commonly reads a card
    the winner has already stamped and appends a second context line under it.
    That is a redundant extra line, not a lost decision.
    """

    log = logger or logging.getLogger(__name__)

    # The outer net that makes "never raises" structural rather than incidental.
    # Every Slack call below already carries its own best-effort handler, so
    # totality holds by inspection today -- but only by inspection, and the cost
    # of it lapsing is not a logged traceback. Bolt's thread runner, on a raise
    # from a post-ack listener, sets ``ack.response`` back to None while the
    # dispatch thread is still polling it, so a raise inside that window eats the
    # ack entirely and the view never closes: the exact failure #1077 exists to
    # remove. A future edit that adds a call here must not be able to
    # reintroduce it by forgetting a handler.
    try:
        # The card's original blocks are not in a view_submission payload, so
        # re-read the message the card lives on to stamp it in place.
        # Best-effort: a failed read only costs the in-place edit, never the
        # resolution. Skip the read entirely on the outcomes that discard it --
        # only the 200 and 409 branches of ``_render_outcome`` use ``message``,
        # and a fetch nobody reads still costs a Slack round trip and holds one
        # of Bolt's five shared listener workers.
        message: dict[str, Any] = {}
        if submission.outcome.status_code in (200, 409):
            message = _fetch_card_message(
                web_client,
                channel=submission.channel,
                card_ts=submission.card_ts,
                log=log,
            )

        _render_outcome(
            approval_id=submission.approval_id,
            decision=submission.decision,
            user=submission.user,
            channel=submission.channel,
            card_ts=submission.card_ts,
            message=message,
            note=submission.note,
            outcome=submission.outcome,
            web_client=web_client,
            log=log,
        )
    except Exception as exc:  # noqa: BLE001 - a raise past the ack eats the ack
        log.warning("post-ack render failed for approval %s: %s", submission.approval_id, exc)


def _fetch_card_message(
    web_client: WebClient, *, channel: str, card_ts: str, log: logging.Logger
) -> dict[str, Any]:
    """The approval card's message, or an empty dict when it cannot be read.

    ``conversations.replies``, not ``conversations.history`` (#1073). The default
    card is a THREAD REPLY -- with no route bound the card posts into the
    requesting thread -- and ``conversations.history`` walks the channel
    timeline, which does not include thread replies. It answered with an empty
    list for every unrouted card, and the caller then wrote a "settled" card
    rebuilt from nothing, destroying the summary.

    ``conversations.replies`` accepts either a thread parent's ts or a reply's
    own ts, so it reads BOTH card shapes: the in-thread card of an unrouted
    approval and the top-level card of a routed one (a top-level message is the
    parent of its own, possibly empty, thread). Same history scope family as the
    call it replaces (`channels:history` / `groups:history` / `im:history`), so
    no manifest change.

    The ts match is explicit rather than assumed. ``limit=1`` bounds the page,
    but for a parent ts the first entry IS the parent, and returning the wrong
    message would stamp a verdict onto someone else's post.
    """

    try:
        replies = web_client.conversations_replies(
            channel=channel, ts=card_ts, inclusive=True, limit=1
        )
        for message in replies.get("messages") or []:
            if message.get("ts") == card_ts:
                return dict(message)
        log.warning("approval card %s not found in %s; leaving it unstamped", card_ts, channel)
    except Exception as exc:  # noqa: BLE001 - the stamp is best-effort
        log.warning("could not read approval card %s in %s: %s", card_ts, channel, exc)
    return {}


def _refusal_text(outcome: ResolveOutcome) -> str:
    """The one wording for a non-200 resolution, shared by both surfaces."""

    if outcome.status_code == 403:
        # The API already words each refusal class distinctly (non-membership,
        # not-authorized, could-not-verify), so render its reason verbatim rather
        # than guessing the class. An empty detail stays class-neutral (#453 AC5).
        return outcome.detail.strip() or "This click was refused and the platform gave no reason."
    if outcome.status_code == 409:
        return (
            f"Already resolved by {outcome.resolved_by}."
            if outcome.resolved_by
            else (outcome.detail or "This request was already resolved.")
        )
    if outcome.status_code == 410:
        return "This approval expired and can no longer be resolved."
    if outcome.status_code == 404:
        if outcome.detail.strip().casefold() == _APPROVAL_NOT_FOUND_DETAIL:
            return (
                "This Curie release does not have this approval, so nothing was "
                "changed. Another Socket Mode client is likely serving this Slack app. "
                "Disconnect the extra client; do not retry from this side."
            )
        return "Resolving failed; try again shortly."
    return "Resolving failed; try again shortly."


def _ephemeral(
    web_client: WebClient, *, channel: str, user: str, text: str, log: logging.Logger
) -> None:
    try:
        web_client.chat_postEphemeral(channel=channel, user=user, text=text)
    except Exception as exc:  # noqa: BLE001 - the verdict stands regardless
        log.warning("ephemeral notice failed in %s: %s", channel, exc)


def _resolve_and_render(
    *,
    approval_id: str,
    decision: str,
    user: str,
    channel: str,
    card_ts: str,
    message: dict[str, Any],
    note: str | None,
    web_client: WebClient,
    resolver: ApprovalResolveClient,
    log: logging.Logger,
) -> ResolveOutcome:
    """Resolve one approval and hand the verdict to ``_render_outcome``.

    The resolve-then-render pair for a caller that is free to talk to Slack
    inline: the immediate-click path, and the note-dialog path's fall-forward
    when the dialog could not be opened. The rendering itself lives in
    ``_render_outcome``, which the post-ack dialog path calls on its own, so
    this is only the ordering of the two halves plus the returned outcome.
    """

    outcome = resolver.resolve(
        approval_id,
        decision=decision,
        attested_user=user,
        attested_channel=channel,
        note=note,
    )
    _render_outcome(
        approval_id=approval_id,
        decision=decision,
        user=user,
        channel=channel,
        card_ts=card_ts,
        message=message,
        note=note,
        outcome=outcome,
        web_client=web_client,
        log=log,
    )
    return outcome


def _render_outcome(
    *,
    approval_id: str,
    decision: str,
    user: str,
    channel: str,
    card_ts: str,
    message: dict[str, Any],
    note: str | None,
    outcome: ResolveOutcome,
    web_client: WebClient,
    log: logging.Logger,
) -> None:
    """Render an already-decided outcome back into Slack.

    Split out of ``_resolve_and_render`` so the note-dialog path can run it
    AFTER ``ack()`` (#1077): every call in here talks to Slack, and none of it
    feeds the ack body. It must never raise -- past the ack a raise does not just
    go unreported, it can eat the ack: Bolt's thread runner sets ``ack.response``
    back to None on a listener exception while the dispatch thread is still
    polling it, so the view is left open with no response at all. Every Slack
    call therefore keeps its own best-effort handler, and the caller wraps the
    whole of this in an outer net.
    """

    if outcome.status_code == 200:
        verdict = _verdict_line(outcome.decision or decision, user, note)
        # Best-effort: the record is already resolved and the resume turn is
        # enqueued; a failed card edit must not undo either. And an UNREAD card
        # is not stamped at all (#1073): writing the verdict over a body we
        # could not read replaces the record of what was approved with a single
        # line, which is worse than leaving the buttons looking live.
        if _card_is_readable(message):
            try:
                web_client.chat_update(
                    channel=channel,
                    ts=card_ts,
                    text=_fallback_text(verdict, message),
                    blocks=_resolved_card_blocks(message, verdict),
                )
            except Exception as exc:  # noqa: BLE001 - render is best-effort
                log.warning("approval card update failed for %s: %s", approval_id, exc)
        else:
            log.warning(
                "approval %s resolved but its card could not be read; leaving it "
                "unstamped rather than overwriting the summary",
                approval_id,
            )
        log.info("approval %s %s by %s", approval_id, decision, user)
        return

    if outcome.status_code == 409:
        # Refresh a stale card so it stops offering buttons for a settled
        # record (the winner's edit normally did this; a race can leave it).
        _refresh_settled_card(
            web_client,
            channel=channel,
            card_ts=card_ts,
            message=message,
            detail=_refusal_text(outcome),
            log=log,
        )
    elif (
        outcome.status_code == 404
        and outcome.detail.strip().casefold() == _APPROVAL_NOT_FOUND_DETAIL
    ):
        log.warning(
            "approval %s was not found in this release and may be owned by another Curie release",
            approval_id,
        )
    log.info(
        "approval %s click by %s rejected: HTTP %s %s",
        approval_id,
        user,
        outcome.status_code,
        outcome.detail,
    )


@dataclass(frozen=True)
class ImmediateClick:
    """A no-dialog click that has already been resolved, carried across the ack.

    Same split as ``NoteSubmission`` (#1077, #2248): resolve (API only) before
    ack, render Slack after. Ownership miss never reaches render; the handler
    declines the envelope instead.
    """

    approval_id: str
    decision: str
    user: str
    channel: str
    card_ts: str
    message: dict[str, Any]
    outcome: ResolveOutcome


def resolve_approval_action(
    *,
    body: dict[str, Any],
    decision: str,
    resolver: ApprovalResolveClient,
    logger: logging.Logger | None = None,
) -> ImmediateClick | None:
    """Parse one immediate card click and POST resolve. No Slack I/O."""

    log = logger or logging.getLogger(__name__)

    actions = body.get("actions") or []
    approval_id = str(actions[0].get("value") or "") if actions else ""
    channel = (body.get("channel") or {}).get("id") or ""
    user = (body.get("user") or {}).get("id") or ""
    message = body.get("message") or {}
    card_ts = message.get("ts") or ""
    if not approval_id or not channel or not user or not card_ts:
        log.info("approval action without id/channel/user/message, skipping")
        return None

    outcome = resolver.resolve(
        approval_id,
        decision=decision,
        attested_user=user,
        attested_channel=channel,
        note=None,
    )
    return ImmediateClick(
        approval_id=approval_id,
        decision=decision,
        user=user,
        channel=channel,
        card_ts=card_ts,
        message=message,
        outcome=outcome,
    )


def render_approval_action(
    click: ImmediateClick,
    *,
    web_client: WebClient,
    logger: logging.Logger | None = None,
) -> None:
    """Stamp or refuse an already-resolved immediate click. Post-ack; never raises."""

    log = logger or logging.getLogger(__name__)
    if is_release_ownership_miss(click.outcome):
        return
    try:
        _render_outcome(
            approval_id=click.approval_id,
            decision=click.decision,
            user=click.user,
            channel=click.channel,
            card_ts=click.card_ts,
            message=click.message,
            note=None,
            outcome=click.outcome,
            web_client=web_client,
            log=log,
        )
        if click.outcome.status_code != 200:
            _ephemeral(
                web_client,
                channel=click.channel,
                user=click.user,
                text=_refusal_text(click.outcome),
                log=log,
            )
    except Exception as exc:  # noqa: BLE001 - a raise past the ack eats the ack
        log.warning("post-ack render failed for approval %s: %s", click.approval_id, exc)


def _refresh_settled_card(
    web_client: WebClient,
    *,
    channel: str,
    card_ts: str,
    message: dict[str, Any],
    detail: str,
    log: logging.Logger,
) -> None:
    # Same guard as the 200 path (#1073): a refresh exists to REMOVE stale
    # buttons, and doing that at the cost of the card body is not a trade worth
    # making on a race the winner has usually already settled.
    if not _card_is_readable(message):
        log.debug("settled-card refresh skipped: the card could not be read")
        return
    try:
        web_client.chat_update(
            channel=channel,
            ts=card_ts,
            text=_fallback_text(detail, message),
            blocks=_resolved_card_blocks(message, detail),
        )
    except Exception as exc:  # noqa: BLE001 - best-effort refresh
        log.debug("settled-card refresh skipped: %s", exc)


def build_resolver(config: DispatcherConfig) -> ApprovalResolveClient:
    """The production resolver, from the dispatcher's API settings."""

    return ApprovalResolveClient(
        api_base_url=config.api_base_url,
        api_key=config.api_key,
        approval_chat_attester_secret=config.approval_chat_attester_secret,
    )
