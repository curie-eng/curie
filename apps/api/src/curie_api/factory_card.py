"""The live SVG status card for a factory run (#3077).

``render_card`` is pure: the card route loads DB state and calls it on every
fetch, so the image GitHub's proxy shows stays live without editing the
comment. Free text from the issue, the bundle, or the model enters the SVG
only as escaped text content. Validated stage ids enter escaped attributes.

Each stage is a group with its id and state. Declared loops are paths with
numbered badges. The pill dot pulses only for live request states.
"""

from __future__ import annotations

import html
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from curie_internal.factory_phase_view import PhaseView, StageSlot

from .factory_progress import pill_for
from .factory_tracker_attention import live_lines

WIDTH, HEIGHT = 878, 300
_TITLE_MAX = 85
_NOTE_MAX = 72
_CAUSE_MAX = 105
_STAGE_LEFT = 85
_STAGE_RIGHT = WIDTH - 85
_STAGE_Y = 216
# A row whose labels wrap onto two lines sits higher, so the round labels
# under the second line stay clear of the footer.
_WRAPPED_STAGE_Y = 204
_STAGE_LABEL_MAX = 20
# Average advance of a 13 px semibold label character, and the clear space
# kept between two neighbouring stage labels.
_LABEL_CHAR_PX = 7.5
_LABEL_GAP_PX = 8
_LABEL_LINE_PX = 15
_LEGACY_Y = (150, 190, 230)

_STYLE = """
.bg { fill: #ffffff; stroke: #d0d7de; }
.fg { fill: #1f2328; }
.muted { fill: #656d76; }
.note { fill: #1f2328; font-style: italic; }
.pill-text { fill: #ffffff; font-weight: 600; }
.icon-done { fill: #1a7f37; }
.icon-current { fill: none; stroke: #2f81f7; stroke-width: 2.5; }
.icon-redo { stroke: #bf8700; stroke-width: 2.5; }
.icon-blocked { stroke: #bf8700; stroke-width: 2.5; }
.icon-pending { fill: none; stroke: #8c959f; stroke-width: 1.5; }
.tick { fill: none; stroke: #ffffff; stroke-width: 2; }
.track { fill: none; stroke: #d0d7de; stroke-width: 2; }
.loop-arc { fill: none; stroke-width: 2; }
.loop-arc.pending { stroke: #8c959f; }
.loop-arc.live { stroke: #2f81f7; }
.loop-arc.approved { stroke: #1a7f37; }
.loop-badge, .badge-halo { font-weight: 700; text-anchor: middle; }
.badge-halo { fill: #ffffff; stroke: #ffffff; stroke-width: 4px; stroke-linejoin: round; }
.loop-badge.pending { fill: #8c959f; }
.loop-badge.live { fill: #2f81f7; }
.loop-badge.approved { fill: #1a7f37; }
.pending .label { fill: #656d76; }
.pill-dot.live { animation: pulse 1.6s ease-in-out infinite; }
@keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.25; } }
text { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
       font-size: 13px; }
.title { font-size: 15px; font-weight: 600; }
.stats { font-family: ui-monospace, SFMono-Regular, Consolas, monospace; font-size: 11px; }
.review-meta, .footer { font-size: 11px; }
.stage .label { font-size: 13px; font-weight: 600; text-anchor: middle; }
.stage .round-label { font-size: 11px; text-anchor: middle; }
@media (prefers-reduced-motion: reduce) {
  .pill-dot.live { animation: none; }
}
@media (prefers-color-scheme: dark) {
  .bg { fill: #0d1117; stroke: #30363d; }
  .fg { fill: #e6edf3; }
  .muted { fill: #8d96a0; }
  .note { fill: #e6edf3; }
  .pending .label { fill: #8d96a0; }
  .icon-pending { stroke: #6e7681; }
  .track { stroke: #30363d; }
  .badge-halo { fill: #0d1117; stroke: #0d1117; }
}
"""


@dataclass(frozen=True)
class CardInput:
    repo: str
    issue_number: int
    title: str | None
    revision_pr: int | None
    status: str
    publishing: bool
    started_at: datetime | None
    terminal_at: datetime | None
    now: datetime
    activity: dict[str, Any] | None
    note: str | None
    phase_view: PhaseView
    cause_text: str | None
    # This failed request's settlement admitted an automatic successor (ADR 0269).
    retrying: bool = False
    note_at: datetime | None = None
    wait_at: datetime | None = None
    no_pull_request: bool = False
    pr_open: bool = False
    next_owner: str | None = None


def _text(value: object, limit: int | None = None) -> str:
    raw = str(value)
    if limit is not None and len(raw) > limit:
        raw = raw[: limit - 3].rstrip() + "..."
    return html.escape(raw, quote=True)


def _elapsed(card: CardInput) -> str:
    if card.started_at is None:
        return "-"
    end = card.terminal_at or card.now
    seconds = max(0, int((end - card.started_at).total_seconds()))
    return f"{seconds // 60}m {seconds % 60:02d}s"


def _stage_step(count: int) -> float:
    return (_STAGE_RIGHT - _STAGE_LEFT) / max(1, count - 1)


def _stage_xy(index: int, count: int, staged: bool, row_y: int = _STAGE_Y) -> tuple[int, int]:
    if staged:
        return round(_STAGE_LEFT + index * _stage_step(count)), row_y
    columns = max(3, -(-count // 3))
    return 48 + (index // 3) * ((WIDTH - 80) // columns), _LEGACY_Y[index % 3]


def _icon(state: str, x: int, y: int) -> str:
    cx, cy = x, y
    if state == "done":
        return (
            f'<circle class="icon-done" cx="{cx}" cy="{cy}" r="8"/>'
            f'<path class="tick" d="M{cx - 4} {cy} l3 3 l5 -6"/>'
        )
    if state == "current":
        return f'<circle class="icon-current" cx="{cx}" cy="{cy}" r="7"/>'
    if state == "redo":
        return (
            f'<path class="icon-redo" d="M{cx - 5} {cy - 5} L{cx + 5} {cy + 5} '
            f'M{cx + 5} {cy - 5} L{cx - 5} {cy + 5}"/>'
        )
    if state == "blocked":
        return (
            f'<path class="icon-blocked" d="M{cx - 5} {cy - 5} L{cx + 5} {cy + 5} '
            f'M{cx + 5} {cy - 5} L{cx - 5} {cy + 5}"/>'
        )
    return f'<circle class="icon-pending" cx="{cx}" cy="{cy}" r="7"/>'


def _label_lines(label: str, count: int) -> list[str]:
    """A staged label on one line, or split at a word when it is too wide.

    The split falls at the word boundary that keeps the longer line shortest,
    so neighbouring labels of a seven stage row do not run into each other.
    """

    width = _stage_step(count) - _LABEL_GAP_PX
    words = label.split()
    if count < 2 or len(label) * _LABEL_CHAR_PX <= width or len(words) < 2:
        return [label]
    split = min(
        range(1, len(words)),
        key=lambda i: max(len(" ".join(words[:i])), len(" ".join(words[i:]))),
    )
    return [" ".join(words[:split]), " ".join(words[split:])]


def _row_y(view: PhaseView) -> int:
    count = len(view.stages)
    wraps = view.staged and any(len(_label_lines(slot.label, count)) > 1 for slot in view.stages)
    return _WRAPPED_STAGE_Y if wraps else _STAGE_Y


def _stage(index: int, count: int, slot: StageSlot, staged: bool, row_y: int = _STAGE_Y) -> str:
    x, y = _stage_xy(index, count, staged, row_y)
    phase_attr = "" if staged else f' data-phase="{_text(slot.id)}"'
    lines = _label_lines(slot.label, count) if staged else [slot.label]
    if len(lines) == 1:
        label = _text(slot.label, _STAGE_LABEL_MAX if staged else 40)
    else:
        label = "".join(
            f'<tspan x="{x}" y="{y + 28 + i * _LABEL_LINE_PX}">'
            f"{_text(line, _STAGE_LABEL_MAX)}</tspan>"
            for i, line in enumerate(lines)
        )
    parts = [
        f'<g data-stage="{_text(slot.id)}"{phase_attr} class="stage slot {slot.state}">',
        _icon(slot.state, x, y),
        f'<text class="label fg" x="{x}" y="{y + 28}">{label}</text>',
    ]
    if slot.round_label:
        # Every round label of a wrapped row sits under the second label line.
        round_y = y + 46 + (_LABEL_LINE_PX if row_y == _WRAPPED_STAGE_Y else 0)
        parts.append(
            f'<text class="round-label muted" x="{x}" y="{round_y}">'
            f"{_text(slot.round_label)}</text>"
        )
    parts.append("</g>")
    return "".join(parts)


def _arcs(view: PhaseView) -> list[str]:
    if not view.staged:
        return []
    index = {slot.id: i for i, slot in enumerate(view.stages)}
    row_y = _row_y(view)
    out: list[str] = []
    badges: list[str] = []
    for loop in view.loops:
        if loop.kickbacks < 1 or loop.stage_start not in index or loop.stage_review not in index:
            continue
        rx, ry = _stage_xy(index[loop.stage_review], len(index), True, row_y)
        sx, sy = _stage_xy(index[loop.stage_start], len(index), True, row_y)
        top = 126 if loop.review == "wait_ci" else 157
        arc_id = "wait_ci" if loop.review == "wait_ci" else loop.start
        arc_state = "live" if loop.active else "approved" if loop.approved else "pending"
        ry -= 13
        sy -= 13
        out.append(
            f'<path class="loop-arc {arc_state}" data-loop="{arc_id}" '
            f'd="M{rx} {ry} C{rx} {top} {sx} {top} {sx} {sy}"/>'
        )
        bx = (rx + sx) / 2
        by = top - 5
        badges.append(
            f'<text class="badge-halo" x="{bx:.1f}" y="{by}">{loop.kickbacks}</text>'
            f'<text class="loop-badge {arc_state}" data-loop="{arc_id}" '
            f'x="{bx:.1f}" y="{by}">{loop.kickbacks}</text>'
        )
    # Badges follow every arc on a halo, so an outer arc never crosses an inner badge.
    return out + badges


def render_card(card: CardInput) -> str:
    """Render the WIDTH x HEIGHT status card as a standalone SVG document."""

    label, color, live = pill_for(card.status, card.publishing, retrying=card.retrying)
    subject = f"{card.repo} #{card.issue_number}"
    if card.revision_pr is not None:
        subject += f" (PR #{card.revision_pr})"
    activity = card.activity or {}
    stats = [f"elapsed {_elapsed(card)}"]
    if activity.get("model"):
        stats.append(f"model {activity['model']}")
    if activity.get("turns") is not None:
        stats.append(f"turns {activity['turns']}")
    if activity.get("tool_calls") is not None:
        stats.append(f"tool calls {activity['tool_calls']}")
    if activity.get("last_tool"):
        stats.append(f"last tool {activity['last_tool']}")
    pill_width = 34 + 8 * len(label)
    pill_x = WIDTH - 24 - pill_width
    dot_class = "pill-dot live" if live else "pill-dot"
    title_line = f"{subject}  ·  {card.title or ''}"
    reviewer = card.phase_view.reviewer_model
    cap = max((loop.cap for loop in card.phase_view.loops), default=None)
    review_text = ""
    state_line, update_line = live_lines(
        status=card.status,
        publishing=card.publishing,
        retrying=card.retrying,
        view=card.phase_view,
        note=card.note,
        note_at=card.note_at,
        wait_at=card.wait_at,
        pr_open=card.pr_open,
    )
    if card.no_pull_request:
        state_line = "No pull request published"
    if reviewer:
        review_text = f"reviewer {reviewer}"
    if cap is not None and card.phase_view.staged:
        review_text += f"  ·  loop cap {cap}"

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" '
        f'viewBox="0 0 {WIDTH} {HEIGHT}" role="img">',
        f"<style>{_STYLE}</style>",
        f'<rect class="bg" x="0.5" y="0.5" width="{WIDTH - 1}" height="{HEIGHT - 1}" rx="8"/>',
        f'<text class="title fg" x="24" y="32">{_text(title_line, _TITLE_MAX)}</text>',
        f'<rect x="{pill_x}" y="18" width="{pill_width}" height="24" rx="12" fill="{color}"/>',
        f'<circle class="{dot_class}" cx="{pill_x + 14}" cy="30" r="4" fill="#ffffff"/>',
        f'<text class="pill-text" x="{pill_x + 24}" y="35">{_text(label)}</text>',
        f'<text class="stats muted" x="24" y="61">'
        f"{'  |  '.join(_text(s, 45) for s in stats)}</text>",
    ]
    parts.append(
        f'<text class="fg" x="24" y="82">{_text(state_line, 55 if review_text else 95)}</text>'
    )
    if card.status in {"queued", "waiting", "running", "cancellation_requested"}:
        parts.append(f'<text class="note" x="24" y="106">{_text(update_line, 95)}</text>')
    elif card.status in {"completed", "failed", "expired", "cancelled"}:
        owner = f"{card.next_owner} (agent reported)" if card.next_owner else "unknown"
        parts.append(f'<text class="fg" x="24" y="106">Next owner: {_text(owner, 70)}</text>')
    elif card.note:
        parts.append(
            f'<text class="note" x="24" y="106" font-style="italic">'
            f"{_text(card.note, 60 if review_text else _NOTE_MAX)}</text>"
        )
    elif card.cause_text:
        parts.append(
            f'<text class="fg" x="24" y="106">'
            f"{_text(card.cause_text, 60 if review_text else _CAUSE_MAX)}</text>"
        )
    if card.cause_text:
        parts.append(f'<text class="fg" x="24" y="123">{_text(card.cause_text, _CAUSE_MAX)}</text>')
    if review_text:
        parts.append(
            f'<text class="review-meta muted" x="{WIDTH - 24}" y="91" text-anchor="end">'
            f"{_text(review_text, 52)}</text>"
        )
    row_y = _row_y(card.phase_view)
    if card.phase_view.staged and card.phase_view.stages:
        start_x, _ = _stage_xy(0, len(card.phase_view.stages), True)
        end_x, _ = _stage_xy(len(card.phase_view.stages) - 1, len(card.phase_view.stages), True)
        parts.append(f'<path class="track" d="M{start_x} {row_y} L{end_x} {row_y}"/>')
    parts.extend(_arcs(card.phase_view))
    count = len(card.phase_view.stages)
    parts.extend(
        _stage(i, count, slot, card.phase_view.staged, row_y)
        for i, slot in enumerate(card.phase_view.stages)
    )
    stamp = card.now.astimezone(UTC).strftime("%H:%M:%S")
    parts.append(
        f'<text class="footer muted" x="24" y="{HEIGHT - 16}">'
        f"phases reported by the agent | {stamp} UTC</text>"
    )
    parts.append("</svg>")
    return "".join(parts)
