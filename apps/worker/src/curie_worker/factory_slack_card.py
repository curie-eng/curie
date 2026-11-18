"""The one live progress card of a factory WorkItem's Slack thread (#5715).

A pure renderer: the progress lane reads the records, this module turns them
into the channel neutral message the reply wire 1.3 replace edit carries. The
Slack sink renders that message as Block Kit: the header, one section for the
body (the issue, who started it, the checklist and the outcome sentence), a
link button and a context line with the elapsed time. The message ``text`` is
both that body and the notification fallback, because the 1.3 message has one
text field.

Steps are the bundle's declared stages through ``phase_view``, the same view
the GitHub status comment renders. Every untrusted value (titles, notes,
logins) is escaped so it can neither mention anyone nor add a link.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Final, Literal

from channel_protocol import MESSAGE_VERSION, MessageLink, OutboundMessage
from curie_internal.factory_cause import cause_text
from curie_internal.factory_phase_view import WAIT_CI_PHASE, PhaseView, phase_view

CardState = Literal["starting", "working", "needs a person", "finished", "stopped"]

_TEXT_MAX: Final = 2000
_NOTE_MAX: Final = 100
_TITLE_MAX: Final = 200
_LINK_LABEL_MAX: Final = 75
_MARKS: Final[dict[str, str]] = {
    "done": ":white_check_mark:",
    "current": ":arrows_counterclockwise:",
    "pending": ":white_circle:",
    "blocked": ":warning:",
    "redo": ":repeat:",
}
# A Slack user id as the dispatcher verified it; anything else is not mentioned.
_SLACK_USER: Final = re.compile(r"^[UW][A-Z0-9]{2,}$")
_NEEDS_A_PERSON: Final = frozenset({"failed", "expired"})


def escaped(value: object, *, limit: int = _TEXT_MAX) -> str:
    """``value`` with nothing live in Slack mrkdwn: no mention, link or markup link.

    Slack's three control characters are escaped, and ``](`` is broken so the
    sink's Markdown conversion cannot turn ``[text](url)`` into a link.
    """

    raw = " ".join(str(value).split())
    bounded = raw if len(raw) <= limit else raw[: limit - 3] + "..."
    return (
        bounded.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("](", "] (")
    )


def mention(user: str | None) -> str | None:
    """``<@user>`` for a well formed Slack user id, else None."""

    if user and _SLACK_USER.fullmatch(user):
        return f"<@{user}>"
    return None


def _http(url: str | None) -> str | None:
    if url and (url.startswith("https://") or url.startswith("http://")):
        return url
    return None


def _sentence(value: str) -> str:
    return value[:1].upper() + value[1:] if value else value


@dataclass(frozen=True)
class CardReport:
    """One phase report of the shown request, in report id order."""

    id: int | None
    phase: str
    loop_round: int | None
    note: str | None


@dataclass(frozen=True)
class CardSubject:
    """The WorkItem facts that do not change between requests."""

    repository: str
    issue_label: str
    issue_title: str | None
    issue_url: str | None
    requester_slack_user: str | None
    requester_login: str | None


@dataclass(frozen=True)
class CardRequest:
    """The shown request: the latest one at or after the thread's first sequence.

    ``pr_number``, ``pr_url``, ``pr_title`` and ``published_at`` are set only
    when a publication of this request succeeded, so a later request on the
    same pull request is not finished by its predecessor's publication.
    """

    status: str
    terminal_cause: str | None
    first: bool
    declaration: dict[str, object] | None
    reports: tuple[CardReport, ...]
    started_at: datetime
    terminal_at: datetime | None
    published_at: datetime | None
    pr_number: int | None
    pr_url: str | None
    pr_title: str | None
    # Whether this personal run's enrollment is still active at its generation.
    # The card sentence follows the recorded terminal cause, not this flag.
    plan_still_connected: bool = False
    personal: bool = False


def _view(request: CardRequest) -> PhaseView | None:
    if request.declaration is None:
        return None
    return phase_view(request.declaration, request.reports, request.status, request.terminal_cause)


def card_state(request: CardRequest, view: PhaseView | None) -> CardState:
    """The header word for the shown request."""

    if request.status == "cancelled":
        return "stopped"
    if request.status in _NEEDS_A_PERSON:
        return "needs a person"
    if request.status == "completed":
        return "finished"
    published = request.pr_url is not None
    if published and (view is None or view.current == WAIT_CI_PHASE):
        return "finished"
    if request.status in ("queued", "waiting") and request.first and not request.reports:
        return "starting"
    return "working"


def _ci(view: PhaseView | None) -> str:
    if view is None:
        return "CI is still running."
    stage = next(
        (slot for slot in view.stages if WAIT_CI_PHASE in slot.phase_ids),
        None,
    )
    if stage is None or stage.state == "done":
        return "CI passed."
    if stage.state in ("blocked", "redo"):
        return "CI failed."
    return "CI is still running."


def _checklist(request: CardRequest, view: PhaseView) -> list[str]:
    lines = []
    for stage in view.stages:
        notes = [
            report.note
            for report in sorted(request.reports, key=lambda report: report.id or 0)
            if report.phase in stage.phase_ids and report.note
        ]
        detail_parts = []
        if notes:
            detail_parts.append(escaped(notes[-1], limit=_NOTE_MAX))
        rounds = [
            loop.round for loop in view.loops if loop.stage_review == stage.id and loop.round > 1
        ]
        if rounds:
            detail_parts.append(f"round {max(rounds)}")
        line = f"{_MARKS[stage.state]} {escaped(stage.label, limit=_TITLE_MAX)}"
        if detail_parts:
            line += f" _{' · '.join(detail_parts)}_"
        lines.append(line)
    return lines


def _clock(seconds: float) -> str:
    whole = max(0, int(seconds))
    return f"{whole // 60}m {whole % 60:02d}s"


def _footer(state: CardState, request: CardRequest, now: datetime) -> str:
    if state == "finished":
        end = request.published_at or request.terminal_at or now
        return f"Finished in {_clock((end - request.started_at).total_seconds())}"
    if state in ("stopped", "needs a person"):
        end = request.terminal_at or now
        return f"Stopped after {_clock((end - request.started_at).total_seconds())}"
    minutes = max(0, int((now - request.started_at).total_seconds()) // 60)
    return f"Running for {minutes} min"


def _issue_line(subject: CardSubject) -> str:
    title = escaped(subject.issue_title, limit=_TITLE_MAX) if subject.issue_title else ""
    label = escaped(subject.issue_label, limit=_TITLE_MAX)
    shown = f"{label} {title}".strip()
    url = _http(subject.issue_url)
    if url is None:
        return shown
    return f"<{escaped(url).replace('|', '%7C')}|{shown}>"


def _started_by(subject: CardSubject) -> str:
    who = mention(subject.requester_slack_user)
    if who is None and subject.requester_login:
        who = escaped(subject.requester_login, limit=_TITLE_MAX)
    repository = escaped(subject.repository, limit=_TITLE_MAX).replace("`", "'")
    line = f"`{repository}`"
    if who:
        line += f" · Started by {who}"
    return line


@dataclass(frozen=True)
class CardRender:
    """One render of the card: its message, state, and digest.

    ``final`` is a state that is always sent at once: finished, stopped or
    needs a person. A live render waits for the edit spacing.
    """

    message: OutboundMessage
    state: CardState
    digest: str

    @property
    def final(self) -> bool:
        return self.state not in ("starting", "working")


def render_card(subject: CardSubject, request: CardRequest, *, now: datetime) -> CardRender:
    """The card for the shown request at ``now``."""

    view = _view(request)
    state = card_state(request, view)
    body = [_issue_line(subject), _started_by(subject)]
    if view is not None and view.stages:
        body.append("")
        body.extend(_checklist(request, view))
    links: list[MessageLink] = []
    if state == "finished" and request.pr_url is not None:
        number = f"#{request.pr_number}" if request.pr_number is not None else ""
        title = escaped(request.pr_title, limit=_TITLE_MAX) if request.pr_title else ""
        opened = f"Opened pull request {number}".rstrip()
        summary = f"{opened}: {title}." if title else f"{opened}."
        body.extend(["", f":tada: *Done.* {summary} {_ci(view)}"])
        url = _http(request.pr_url)
        if url is not None:
            label = f"View pull request {number}".rstrip()
            links.append(MessageLink(label=label[:_LINK_LABEL_MAX], url=url))
    elif state == "needs a person":
        # A recorded personal provider rejection stays the rejection sentence
        # after the owner reconnects. A recorded enrollment fence stays the
        # fence sentence. Service mode keeps the shared cause sentence.
        # The notify reply keeps the shared cause sentence (#5720).
        if request.terminal_cause == "authentication_required" and request.personal:
            sentence = "Your Claude plan was rejected. Reconnect it on the Claude plan page."
        elif request.terminal_cause == "enrollment_fenced":
            sentence = _sentence(cause_text("enrollment_fenced"))
        else:
            sentence = _sentence(cause_text(str(request.terminal_cause or "")))
        body.extend(["", f":raising_hand: *Needs a person.* {escaped(sentence)}"])
        url = _http(subject.issue_url)
        if url is not None:
            label = f"View issue {subject.issue_label}"
            links.append(MessageLink(label=label[:_LINK_LABEL_MAX], url=url))
    message = OutboundMessage(
        version=MESSAGE_VERSION,
        header=f"Factory run: {state}",
        text="\n".join(body),
        links=links,
        footer=_footer(state, request, now),
    )
    canonical = json.dumps(message.model_dump(mode="json"), sort_keys=True)
    return CardRender(message, state, hashlib.sha256(canonical.encode()).hexdigest())


def starting_text(subject: CardSubject) -> str:
    """The card's first post, before the first edit renders the blocks."""

    title = escaped(subject.issue_title, limit=_TITLE_MAX) if subject.issue_title else ""
    return f"Factory run starting: {escaped(subject.issue_label)} {title}".strip()


def needs_person_text(user: str | None, cause: str | None) -> str:
    """The one notify reply for a request that needs a person."""

    sentence = escaped(cause_text(str(cause or "")))
    who = mention(user)
    if who is None:
        return f"This run needs you: {sentence}"
    return f"{who} this run needs you: {sentence}"


def ready_text(user: str | None, url: str) -> str:
    """The one notify reply for a request whose pull request is open."""

    who = mention(user)
    shown = escaped(url)
    if who is None:
        return f"Your pull request is ready for review: {shown}"
    return f"{who} your pull request is ready for review: {shown}"


def card_reports(rows: Sequence[dict[str, object]]) -> tuple[CardReport, ...]:
    """Phase report rows as the card reads them."""

    reports = []
    for row in rows:
        raw_id = row.get("id")
        raw_round = row.get("loop_round")
        note = row.get("note")
        reports.append(
            CardReport(
                id=int(str(raw_id)) if raw_id is not None else None,
                phase=str(row.get("phase") or ""),
                loop_round=int(str(raw_round)) if raw_round is not None else None,
                note=str(note) if note is not None else None,
            )
        )
    return tuple(reports)


__all__ = [
    "CardRender",
    "CardReport",
    "CardRequest",
    "CardState",
    "CardSubject",
    "card_reports",
    "card_state",
    "escaped",
    "mention",
    "needs_person_text",
    "ready_text",
    "render_card",
    "starting_text",
]
