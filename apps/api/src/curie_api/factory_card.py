"""The live SVG status card for a factory run (#3077).

``render_card`` is pure: the card route loads DB state and calls it on every
fetch, so the image GitHub's proxy shows stays live without editing the
comment. Everything from the issue, the bundle, or the model enters the SVG
only as escaped text content, never as markup or an attribute value.

Markup contract (pinned by tests): each phase slot is a ``<g>`` with
``data-phase`` and a state class (``done``, ``current``, ``redo``,
``pending``); each drawn loop arc is a ``<path class="loop-arc">`` with a
``<text class="loop-badge">``; the pill dot carries ``live`` iff it pulses.
"""

from __future__ import annotations

import html
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .factory_progress import PhaseSlot, PhaseView, pill_for

# 878 is the content width of a GitHub issue comment at the capped desktop
# layout (viewport 1366 and wider), so the card fills the box with no gutter.
# Narrower columns scale it down through GitHub's img max-width: 100%.
WIDTH, HEIGHT = 878, 300
_TITLE_MAX = 70
_NOTE_MAX = 110
_TEXT_MAX = 40
_CAUSE_MAX = 110
_GRID_LEFT = 48
_GRID_WIDTH = WIDTH - 50
_GRID_Y = (150, 190, 230)
_ROWS = 3
_BOW = 22

_STYLE = """
.bg { fill: #ffffff; stroke: #d0d7de; }
.fg { fill: #1f2328; }
.muted { fill: #656d76; }
.note { fill: #1f2328; font-style: italic; }
.pill-text { fill: #ffffff; font-weight: 600; }
.icon-done { fill: #1a7f37; }
.icon-current { fill: none; stroke: #2f81f7; stroke-width: 2.5; }
.icon-redo { stroke: #bf8700; stroke-width: 2.5; }
.icon-pending { fill: none; stroke: #8c959f; stroke-width: 1.5; }
.tick { fill: none; stroke: #ffffff; stroke-width: 2; }
.loop-arc { fill: none; stroke: #bf8700; stroke-width: 1.5; stroke-dasharray: 4 3; }
.loop-badge { fill: #bf8700; font-weight: 700; }
.pending .label { fill: #656d76; }
.live { animation: pulse 1.6s ease-in-out infinite; }
@keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.25; } }
text { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
       font-size: 13px; }
@media (prefers-color-scheme: dark) {
  .bg { fill: #0d1117; stroke: #30363d; }
  .fg { fill: #e6edf3; }
  .muted { fill: #8d96a0; }
  .note { fill: #e6edf3; }
  .pending .label { fill: #8d96a0; }
  .icon-pending { stroke: #6e7681; }
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


def _slot_xy(index: int, count: int) -> tuple[int, int]:
    """Column-major: three rows, as many columns as the declaration needs."""

    columns = max(3, -(-count // _ROWS))
    return _GRID_LEFT + (index // _ROWS) * (_GRID_WIDTH // columns), _GRID_Y[index % _ROWS]


def _icon(state: str, x: int, y: int) -> str:
    cx, cy = x + 8, y - 4
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
    return f'<circle class="icon-pending" cx="{cx}" cy="{cy}" r="7"/>'


def _slot(index: int, count: int, slot: PhaseSlot) -> str:
    x, y = _slot_xy(index, count)
    parts = [
        f'<g data-phase="{_text(slot.id)}" class="slot {slot.state}">',
        _icon(slot.state, x, y),
        f'<text class="label fg" x="{x + 24}" y="{y}">{_text(slot.label, _TEXT_MAX)}</text>',
    ]
    if slot.round_label:
        parts.append(
            f'<text class="muted" x="{x + 24}" y="{y + 15}" font-size="11">'
            f"{_text(slot.round_label)}</text>"
        )
    parts.append("</g>")
    return "".join(parts)


def _arcs(view: PhaseView) -> list[str]:
    index = {slot.id: i for i, slot in enumerate(view.phases)}
    out: list[str] = []
    for loop in view.loops:
        if loop.kickbacks < 1 or loop.start not in index or loop.review not in index:
            continue
        rx, ry = _slot_xy(index[loop.review], len(index))
        sx, sy = _slot_xy(index[loop.start], len(index))
        rx, ry, sx, sy = rx - 4, ry - 4, sx - 4, sy - 4
        out.append(
            f'<path class="loop-arc" d="M{rx} {ry} C{rx - _BOW} {ry} '
            f'{sx - _BOW} {sy} {sx} {sy}"/>'
        )
        bx = (rx + sx) / 2 - 0.75 * _BOW - 6
        by = (ry + sy) / 2 + 4
        out.append(f'<text class="loop-badge" x="{bx:.1f}" y="{by:.1f}">{loop.kickbacks}</text>')
    return out


def render_card(card: CardInput) -> str:
    """Render the WIDTH x HEIGHT status card as a standalone SVG document."""

    label, color, live = pill_for(card.status, card.publishing)
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
    pill_width = 18 + 8 * len(label) + 16
    pill_x = WIDTH - 24 - pill_width
    dot_class = "pill-dot live" if live else "pill-dot"

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" '
        f'viewBox="0 0 {WIDTH} {HEIGHT}" role="img">',
        f"<style>{_STYLE}</style>",
        f'<rect class="bg" x="0.5" y="0.5" width="{WIDTH - 1}" height="{HEIGHT - 1}" rx="8"/>',
        f'<text class="muted" x="24" y="30">{_text(subject, _TITLE_MAX)}</text>',
        f'<text class="fg" x="24" y="52" font-size="16" font-weight="600">'
        f"{_text(card.title or '', _TITLE_MAX)}</text>",
        f'<rect x="{pill_x}" y="18" width="{pill_width}" height="24" rx="12" fill="{color}"/>',
        f'<circle class="{dot_class}" cx="{pill_x + 14}" cy="30" r="4" fill="#ffffff"/>',
        f'<text class="pill-text" x="{pill_x + 24}" y="35">{_text(label)}</text>',
        f'<text class="muted" x="24" y="84">{" | ".join(_text(s, 60) for s in stats)}</text>',
    ]
    if card.note:
        parts.append(
            f'<text class="note" x="24" y="110" font-style="italic">'
            f"{_text(card.note, _NOTE_MAX)}</text>"
        )
    elif card.cause_text:
        parts.append(
            f'<text class="fg" x="24" y="110">{_text(card.cause_text, _CAUSE_MAX)}</text>'
        )
    if card.note and card.cause_text:
        parts.append(
            f'<text class="fg" x="24" y="128">{_text(card.cause_text, _CAUSE_MAX)}</text>'
        )
    parts.extend(_arcs(card.phase_view))
    count = len(card.phase_view.phases)
    parts.extend(_slot(i, count, slot) for i, slot in enumerate(card.phase_view.phases))
    stamp = card.now.astimezone(UTC).strftime("%H:%M:%S")
    parts.append(
        f'<text class="muted" x="24" y="{HEIGHT - 16}" font-size="11">'
        f"phases reported by the agent | {stamp} UTC</text>"
    )
    parts.append("</svg>")
    return "".join(parts)
