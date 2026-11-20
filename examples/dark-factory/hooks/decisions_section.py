"""The factory publication Decisions section grammar, shared by bundle consumers."""

from __future__ import annotations

from typing import Any

NONE_DECLARATION = "None: the change follows directly from the acceptance criteria."
# The section's nonblank lines may hold at most this many characters in total.
# The cap is checked before any entry is parsed, so a body of any size is
# decided in time linear in its length.
MAX_SECTION_CHARACTERS = 7000
_SOURCES = ("plan", "plan review", "diff review")
_REJECTED = ". Rejected: "
_BECAUSE = ", because "
_EVIDENCE = ") Evidence: "


def _source_marker(line: str, start: int) -> tuple[int, int, str] | None:
    """The earliest `. (<source>) Evidence: ` at or after ``start``: its index,
    the index just past it, and the source."""
    found: tuple[int, int, str] | None = None
    for source in _SOURCES:
        marker = f". ({source}{_EVIDENCE}"
        index = line.find(marker, start)
        if index != -1 and (found is None or index < found[0]):
            found = (index, index + len(marker), source)
    return found


def _parse_entry(line: str) -> dict[str, Any] | None:
    """One `n. <choice>. Rejected: <alternative>, because <reason>. (<source>)
    Evidence: <evidence>.` line, or None when it does not have that shape.

    Each separator is the earliest one that leaves at least one character in
    the field before it, which is the reading a lazy match would take."""
    if len(line) < 3 or line[0] not in "1234567" or line[1:3] != ". ":
        return None
    choice_start = 3
    rejected = line.find(_REJECTED, choice_start + 1)
    if rejected == -1:
        return None
    alternative_start = rejected + len(_REJECTED)
    because = line.find(_BECAUSE, alternative_start + 1)
    if because == -1:
        return None
    reason_start = because + len(_BECAUSE)
    marker = _source_marker(line, reason_start + 1)
    if marker is None:
        return None
    reason_end, evidence_start, source = marker
    if not line.endswith(".") or len(line) - 1 <= evidence_start:
        return None
    return {
        "n": int(line[0]),
        "choice": line[choice_start:rejected],
        "rejected": line[alternative_start:because],
        "reason": line[reason_start:reason_end],
        "source": source,
        "evidence": line[evidence_start:-1],
    }


def parse_decisions_section(body: object) -> list[dict[str, Any]]:
    """Parse the publication body's Decisions section, rejecting malformed records."""
    if not isinstance(body, str):
        raise ValueError("the body must be text")
    lines = body.splitlines()
    headings = [index for index, line in enumerate(lines) if line == "## Decisions"]
    if len(headings) != 1:
        raise ValueError("exactly one heading line must be ## Decisions")
    section = []
    size = 0
    for line in lines[headings[0] + 1 :]:
        if line.startswith("## "):
            break
        if line.strip():
            size += len(line)
            if size > MAX_SECTION_CHARACTERS:
                raise ValueError(
                    f"the Decisions section must be at most {MAX_SECTION_CHARACTERS} characters"
                )
            section.append(line)
    if section == [NONE_DECLARATION]:
        return []
    if not 1 <= len(section) <= 7:
        raise ValueError("record one to seven decisions, or the sole None declaration")
    entries: list[dict[str, Any]] = []
    for number, line in enumerate(section, 1):
        entry = _parse_entry(line)
        if entry is None:
            raise ValueError("each numbered decision must follow the required single-line grammar")
        if entry["n"] != number:
            raise ValueError("decision numbers must be contiguous from 1")
        if any(not entry[field].strip() for field in ("choice", "rejected", "reason", "evidence")):
            raise ValueError("choice, rejected alternative, reason and evidence must be nonblank")
        entries.append(entry)
    return entries
