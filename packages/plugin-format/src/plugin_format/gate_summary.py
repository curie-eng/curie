"""Safe human-summary templates for a permission-gate card (#2565).

A bundle may declare ``approvalPolicy.gates[].summary`` as a sentence with
placeholders over the blocked call's arguments. The deploy validator and the
runtime renderer share this module so a template that validates green cannot
render under different rules (the #453/#544 class).

Grammar: ``{ident}`` interpolates a scalar (str/int/float/bool); ``{ident|count}``
and ``{ident|length}`` interpolate ``len`` of a str/list/tuple/dict. Nested
dumps, attribute access, and unknown filters are not in the grammar. Runtime
type or missing-key failure returns ``None`` so the runner falls back to the
machine string rather than stranding the approval.
"""

from __future__ import annotations

import re
from typing import Any

# Byte-identical to ``curie_runner.approval.APPROVAL_SUMMARY_PREFIX``. Kept here
# so deploy validation can reject a template that would park a forged prefix on
# the display path without importing the runner. A runner test pins the two
# literals equal.
RESERVED_PERMISSION_PREFIX = "Tool call awaiting approval: "

_TEMPLATE_MAX = 400
_SCALAR_MAX = 80
_IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
_PLACEHOLDER = re.compile(r"\{(" + _IDENT + r")(?:\|(" + _IDENT + r"))?\}")
_FILTERS = frozenset({"count", "length"})
_SCALARS = (str, int, float, bool)
_COUNTED = (str, list, tuple, dict)


def check_gate_summary_template(template: str) -> str | None:
    """Return an error message if ``template`` is not a legal summary, else None."""

    if not isinstance(template, str):
        return "summary must be a string"
    stripped = template.strip()
    if not stripped:
        return "summary must be a non-empty string"
    if len(stripped) > _TEMPLATE_MAX:
        return f"summary must be at most {_TEMPLATE_MAX} characters"
    if stripped.startswith(RESERVED_PERMISSION_PREFIX):
        return "summary must not start with the reserved permission-gate prefix"
    remainder = _PLACEHOLDER.sub("", stripped)
    if "{" in remainder or "}" in remainder:
        return "summary has unmatched braces or an invalid placeholder"
    for match in _PLACEHOLDER.finditer(stripped):
        filt = match.group(2)
        if filt is not None and filt not in _FILTERS:
            return f"summary filter {filt!r} is not count or length"
    return None


def render_gate_summary(template: str, args: dict[str, Any]) -> str | None:
    """Render ``template`` from ``args``, or None when it cannot render safely."""

    if check_gate_summary_template(template) is not None:
        return None
    if not isinstance(args, dict):
        return None
    stripped = template.strip()

    def repl(match: re.Match[str]) -> str:
        name, filt = match.group(1), match.group(2)
        if name not in args:
            raise KeyError(name)
        value = args[name]
        if filt is None:
            if not isinstance(value, _SCALARS):
                raise TypeError(name)
            return _escape_scalar(str(value))
        if isinstance(value, _COUNTED):
            return str(len(value))
        raise TypeError(name)

    try:
        rendered = _PLACEHOLDER.sub(repl, stripped)
    except (KeyError, TypeError):
        return None
    if not rendered.strip():
        return None
    if rendered.startswith(RESERVED_PERMISSION_PREFIX):
        return None
    return rendered


def _escape_scalar(value: str) -> str:
    collapsed = " ".join(value.split())
    if len(collapsed) > _SCALAR_MAX:
        collapsed = collapsed[:_SCALAR_MAX]
    # Neutralize Markdown constructs ``to_mrkdwn`` would rewrite into Slack
    # markup ([text](url) -> <url|text>, **bold**, ATX headings). The card
    # renderer always runs ``to_mrkdwn``, so escaping ``& < >`` alone is not
    # enough: a model-supplied ``[Review](https://evil.example.com)`` would
    # become a live link on the card.
    collapsed = (
        collapsed.replace("[", "")
        .replace("]", "")
        .replace("*", "")
        .replace("#", "")
    )
    return collapsed.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
