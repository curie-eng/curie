"""The factory publication Decisions section grammar, shared by bundle consumers."""

from __future__ import annotations

import re
from typing import Any

NONE_DECLARATION = "None: the change follows directly from the acceptance criteria."
_DECISIONS_ENTRY = re.compile(
    r"(?P<n>[1-7])\. (?P<choice>.+?)\. Rejected: (?P<rejected>.+?), "
    r"because (?P<reason>.+?)\. \((?P<source>plan|plan review|diff review)\) "
    r"Evidence: (?P<evidence>.+)\."
)


def parse_decisions_section(body: object) -> list[dict[str, Any]]:
    """Parse the publication body's Decisions section, rejecting malformed records."""
    if not isinstance(body, str):
        raise ValueError("the body must be text")
    lines = body.splitlines()
    headings = [index for index, line in enumerate(lines) if line == "## Decisions"]
    if len(headings) != 1:
        raise ValueError("exactly one heading line must be ## Decisions")
    section = []
    for line in lines[headings[0] + 1 :]:
        if line.startswith("## "):
            break
        if line.strip():
            section.append(line)
    if section == [NONE_DECLARATION]:
        return []
    if not 1 <= len(section) <= 7:
        raise ValueError("record one to seven decisions, or the sole None declaration")
    entries: list[dict[str, Any]] = []
    for number, line in enumerate(section, 1):
        match = _DECISIONS_ENTRY.fullmatch(line)
        if match is None:
            raise ValueError("each numbered decision must follow the required single-line grammar")
        entry: dict[str, Any] = match.groupdict()
        entry["n"] = int(entry["n"])
        if entry["n"] != number:
            raise ValueError("decision numbers must be contiguous from 1")
        if any(not entry[field].strip() for field in ("choice", "rejected", "reason", "evidence")):
            raise ValueError("choice, rejected alternative, reason and evidence must be nonblank")
        entries.append(entry)
    return entries
