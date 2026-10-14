from __future__ import annotations


def _is_approval_resume(event_id: str) -> bool:
    """Approval resume ids end with the frozen ``-resolved`` suffix."""

    return event_id.startswith("approval-") and event_id.endswith("-resolved")
