"""Transient, structural specification contract (#4484); never a readiness verdict.

No command execution, persistence, environment probes or model calls belong here.
The admission evaluator (#4487) and validation API (#4488) reuse this boundary.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

CONTRACT_VERSION = 1
SECTIONS = (
    "Problem",
    "Base",
    "Blocked by",
    "Decisions",
    "Preconditions",
    "Acceptance criteria",
    "Out of scope",
)
_HEADINGS = {name.casefold(): name for name in SECTIONS}
_HEADING = re.compile(r"^ {0,3}(#{1,6})[ \t]+(.+?)\s*$")
_FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
_CRITERION = re.compile(r"^( {0,3})(\d+)[.)][ \t]+(.*)$")
_FIELD = re.compile(
    r"^ {0,3}(?:\*\*(Verified by|Runs in):\*\*|"
    r"\*\*(Verified by|Runs in)\*\*:|(Verified by|Runs in):)[ \t]*(.*)$",
    re.IGNORECASE,
)
_MESSAGES = {
    "spec_missing_section": "Add this required h2 or h3 section.",
    "spec_duplicate_section": "Keep exactly one occurrence of this section.",
    "spec_empty_section": "Supply nonempty content for this section.",
    "spec_invalid_none": "None is permitted only for Blocked by and Preconditions.",
    "spec_invalid_criteria": "Number nonempty criteria consecutively from 1 at the top list level.",
    "spec_missing_field": "Add one Verified by and one Runs in field to each criterion.",
    "spec_duplicate_field": "Keep exactly one occurrence of each verification field.",
    "spec_empty_field": "Supply a nonempty verification field value.",
    "spec_invalid_runs_in": "Use sandbox or ci: followed by an actual required check name.",
    "spec_invalid_encoding": "Supply Unicode text encodable as UTF-8.",
}


@dataclass(frozen=True)
class Diagnostic:
    code: str
    message: str
    path: str
    section: str | None = None
    criterion: int | None = None


@dataclass(frozen=True)
class Criterion:
    number: int
    text: str = field(repr=False)
    verified_by: str = field(repr=False)
    runs_in: str = field(repr=False)


@dataclass(frozen=True)
class Specification:
    sections: Mapping[str, str] = field(repr=False)
    criteria: tuple[Criterion, ...] = field(repr=False)
    fingerprint: str
    version: int = CONTRACT_VERSION


@dataclass(frozen=True)
class SpecificationResult:
    specification: Specification | None
    diagnostics: tuple[Diagnostic, ...] = ()

    @property
    def valid(self) -> bool:
        return self.specification is not None and not self.diagnostics


def _visible_lines(markdown: str) -> list[tuple[str, str]]:
    """Keep original prose, but hide fenced/commented structural tokens."""
    lines: list[tuple[str, str]] = []
    fence_char, fence_length = "", 0
    comment = False
    for raw in markdown.splitlines():
        marker = _FENCE.match(raw)
        if fence_char:
            if marker and marker[1][0] == fence_char and len(marker[1]) >= fence_length:
                if not marker[2].strip():
                    fence_char, fence_length = "", 0
            lines.append((raw, ""))
            continue
        if comment or (not raw.startswith(("    ", "\t")) and "<!--" in raw):
            # Walk every transition: one line may close and reopen a comment.
            # An active comment can close on an indented line (not literal code).
            for transition in re.finditer(r"<!--|-->", raw):
                if transition[0] == "<!--" and not comment:
                    comment = True
                elif transition[0] == "-->" and comment:
                    comment = False
            # Never manufacture structural tokens by joining comment neighbors.
            lines.append((raw, ""))
        elif raw.startswith(("    ", "\t")):
            # A new literal code line cannot open a cross-line HTML comment.
            lines.append((raw, ""))
        elif marker:
            fence_char, fence_length = marker[1][0], len(marker[1])
            lines.append((raw, ""))
        else:
            lines.append((raw, raw))
    return lines


def _parse_criteria(
    lines: list[tuple[str, str]], diagnostics: list[Diagnostic]
) -> tuple[Criterion, ...]:
    section = "Acceptance criteria"
    rows: list[Criterion] = []
    number: int | None = None
    list_indent: int | None = None
    text: list[str] = []
    fields: dict[str, list[str]] = {}

    def error(code: str, suffix: str = "") -> None:
        path = f"/sections/Acceptance criteria/criteria/{len(rows)}{suffix}"
        diagnostics.append(Diagnostic(code, _MESSAGES[code], path, section, number))

    def finish() -> None:
        if number is None:
            return
        if number != len(rows) + 1 or not "\n".join(text).strip():
            error("spec_invalid_criteria")
        for name in ("verified_by", "runs_in"):
            values = fields.get(name, [])
            if not values:
                error("spec_missing_field", f"/{name}")
            elif len(values) != 1:
                error("spec_duplicate_field", f"/{name}")
            elif not values[0] or values[0].casefold() == "none":
                error("spec_empty_field", f"/{name}")
        runs = fields.get("runs_in", [""])[0]
        if runs and runs != "sandbox":
            check = runs.removeprefix("ci:")
            if not runs.startswith("ci:") or not check.strip() or any(c in check for c in "<>\r\n"):
                error("spec_invalid_runs_in", "/runs_in")
        rows.append(
            Criterion(
                number,
                "\n".join(text).strip(),
                fields.get("verified_by", [""])[0],
                runs,
            )
        )

    for _raw, line in lines:
        item = _CRITERION.match(line)
        if item:
            indent = len(item[1])
            if list_indent is None:
                list_indent = indent
            if indent > list_indent:
                continue
            if indent < list_indent:
                error("spec_invalid_criteria")
            finish()
            try:
                number = int(item[2])
            except ValueError:
                number = 0
            text, fields = [item[3].strip()], {}
            continue
        label = _FIELD.match(line)
        if label:
            if number is None:
                error("spec_invalid_criteria")
            else:
                name = next(value for value in label.groups()[:3] if value).casefold()
                fields.setdefault(name.replace(" ", "_"), []).append(label[4].strip())
        elif number is not None and not fields and line.strip():
            text.append(line.strip())
    finish()
    if not rows:
        error("spec_invalid_criteria")
    return tuple(rows)


def parse_specification(markdown: str) -> SpecificationResult:
    """Parse shape only. The returned bodies must never be logged or persisted."""
    try:
        fingerprint = hashlib.sha256(markdown.encode("utf-8")).hexdigest()
    except UnicodeEncodeError:
        code = "spec_invalid_encoding"
        return SpecificationResult(None, (Diagnostic(code, _MESSAGES[code], "/"),))
    diagnostics: list[Diagnostic] = []
    sections: dict[str, list[tuple[str, str]]] = {}
    current: str | None = None
    for raw, line in _visible_lines(markdown):
        heading = _HEADING.match(line)
        if heading:
            name = re.sub(r"[ \t]+#+[ \t]*$", "", heading[2]).strip().casefold()
            current = _HEADINGS.get(name) if len(heading[1]) in (2, 3) else None
            if current is not None:
                if current in sections:
                    code = "spec_duplicate_section"
                    diagnostics.append(
                        Diagnostic(code, _MESSAGES[code], f"/sections/{current}", current)
                    )
                    current = None
                else:
                    sections[current] = []
        elif current is not None:
            sections[current].append((raw, line))
    normalized: dict[str, str] = {}
    for name in SECTIONS:
        code = ""
        if name not in sections:
            code = "spec_missing_section"
        else:
            normalized[name] = "\n".join(raw for raw, _ in sections[name]).strip()
            visible = "\n".join(line for _, line in sections[name]).strip()
            if not visible:
                code = "spec_empty_section"
            elif visible.casefold() == "none" and name not in ("Blocked by", "Preconditions"):
                code = "spec_invalid_none"
        if code:
            diagnostics.append(Diagnostic(code, _MESSAGES[code], f"/sections/{name}", name))
    criteria = _parse_criteria(sections.get("Acceptance criteria", []), diagnostics)
    if diagnostics:
        return SpecificationResult(None, tuple(diagnostics))
    return SpecificationResult(Specification(MappingProxyType(normalized), criteria, fingerprint))
