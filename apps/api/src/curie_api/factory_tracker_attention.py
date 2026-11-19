"""Short, source-labelled run facts shared by the SVG and tracker text."""

from __future__ import annotations

from datetime import UTC, datetime

from curie_internal.factory_phase_view import WAIT_CI_PHASE, PhaseView

from .factory_run_handoff import redact_tracker_text


def live_lines(
    *,
    status: str,
    publishing: bool,
    retrying: bool,
    view: PhaseView,
    note: str | None,
    note_at: datetime | None,
    wait_at: datetime | None = None,
) -> tuple[str, str]:
    """Return bounded facts only; neither a forecast nor an inferred owner."""

    state = {
        "queued": "Queued",
        "waiting": "Waiting",
        "running": "Running",
        "cancellation_requested": "Stopping",
        "completed": "PR open",
        "failed": "Failed",
        "expired": "Expired",
        "cancelled": "Stopped",
    }.get(status, "Status unknown")
    if retrying:
        state = "Retrying"
    elif publishing:
        state = "Publishing"
    active = next((slot.label for slot in view.stages if slot.state == "current"), None)
    if active is None and view.current is not None:
        active = next((slot.label for slot in view.phases if slot.id == view.current), None)
    platform_wait = view.current == WAIT_CI_PHASE or status == "waiting"
    if view.current == WAIT_CI_PHASE:
        state = "Waiting · CI"
    elif active and status in {"running", "cancellation_requested"}:
        state = f"{state} · {active}"
    elif status == "waiting":
        state += " · recorded wait"
    if platform_wait:
        stamp = f" {wait_at.astimezone(UTC).strftime('%H:%M UTC')}" if wait_at else ""
        update = f"Platform wait recorded{stamp}"
        if note and note_at:
            update += f"; last agent note {note_at.astimezone(UTC).strftime('%H:%M UTC')}"
    elif note and note_at:
        stamp = note_at.astimezone(UTC).strftime("%H:%M UTC")
        update = f"Agent update {stamp}: {note}"
    elif note:
        update = f"Agent update (time unavailable): {note}"
    else:
        update = "No progress note reported"
    return (
        redact_tracker_text(state),
        redact_tracker_text(update),
    )
