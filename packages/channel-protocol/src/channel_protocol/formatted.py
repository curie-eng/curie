"""Formatted reply values independent of provider rendering."""

from dataclasses import dataclass, field


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
