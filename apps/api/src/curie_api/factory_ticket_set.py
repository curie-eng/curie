"""Local ticket-set metadata (#4484), with no workflow store or tracker writes."""

from __future__ import annotations

import hashlib
import json
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, ValidationError

from .factory_specification import Diagnostic, Specification, parse_specification

LocalId = Annotated[str, StringConstraints(pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")]
Text = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
Digest = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
CriterionNumber = Annotated[int, Field(gt=0)]


class _Record(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    def __repr__(self) -> str:
        return f"{type(self).__name__}()"

    def __str__(self) -> str:
        return repr(self)


class Source(_Record):
    kind: Literal["existing-ticket", "new-intent"]
    identity: Text = Field(repr=False)


class Requirement(_Record):
    id: LocalId = Field(repr=False)
    text: Text = Field(repr=False)


class Child(_Record):
    id: LocalId = Field(repr=False)
    specification_fingerprint: Digest


class Coverage(_Record):
    requirement: LocalId = Field(repr=False)
    child: LocalId = Field(repr=False)
    criterion: CriterionNumber


class Integration(_Record):
    child: LocalId = Field(repr=False)
    criterion: CriterionNumber
    check: Text = Field(repr=False)


class Dependency(_Record):
    child: LocalId = Field(repr=False)
    prerequisite: LocalId = Field(repr=False)


class ScopeExclusion(_Record):
    requirement: LocalId = Field(repr=False)
    reason: Text = Field(repr=False)
    reviewed: bool


class TicketSetEnvelope(_Record):
    version: Annotated[int, Field(ge=1, le=1)]
    source: Source = Field(repr=False)
    requirements: Annotated[tuple[Requirement, ...], Field(min_length=1)] = Field(repr=False)
    children: Annotated[tuple[Child, ...], Field(min_length=1)] = Field(repr=False)
    coverage: Annotated[tuple[Coverage, ...], Field(min_length=1)] = Field(repr=False)
    integration: Integration = Field(repr=False)
    dependencies: tuple[Dependency, ...] = Field(repr=False)
    scope_exclusions: tuple[ScopeExclusion, ...] = Field(repr=False)


@dataclass(frozen=True)
class TicketSet:
    envelope: TicketSetEnvelope = field(repr=False)
    fingerprint: str
    _canonical: str = field(repr=False)

    def canonical_json(self) -> str:
        """Explicit serialization of local authoring content; never log this."""
        return self._canonical


@dataclass(frozen=True)
class TicketSetResult:
    ticket_set: TicketSet | None
    diagnostics: tuple[Diagnostic, ...] = ()

    @property
    def valid(self) -> bool:
        return self.ticket_set is not None and not self.diagnostics


_MESSAGES = {
    "set_invalid_json": "Supply a JSON object with unique keys and valid JSON values.",
    "set_invalid_schema": "Supply the required v1 fields with their documented types.",
    "set_duplicate_id": "Use unique IDs within each requirement or child collection.",
    "set_unknown_reference": "Reference an ID declared in the corresponding collection.",
    "set_duplicate_dependency": "Keep only one copy of each dependency edge.",
    "set_dependency_cycle": "Remove self dependencies and cycles from the dependency graph.",
    "set_uncovered_requirement": (
        "Map this requirement to a child criterion or a reviewed exclusion."
    ),
    "set_invalid_criterion": (
        "Reference an existing numbered criterion in the child's specification."
    ),
    "set_invalid_child_specification": "Supply a complete canonical specification for this child.",
    "set_fingerprint_mismatch": "Refresh this child's fingerprint from its exact UTF-8 body.",
    "set_missing_integration": "Name a child, criterion and check owning cross-ticket integration.",
    "set_unreviewed_exclusion": "Obtain developer review before excluding an original requirement.",
    "set_duplicate_coverage": "Keep only one copy of each coverage row.",
    "set_duplicate_exclusion": "Keep only one exclusion for each original requirement.",
}
_SCHEMA_FIELDS = frozenset(
    {
        "version",
        "source",
        "kind",
        "identity",
        "requirements",
        "id",
        "text",
        "children",
        "specification_fingerprint",
        "coverage",
        "requirement",
        "child",
        "criterion",
        "integration",
        "check",
        "dependencies",
        "prerequisite",
        "scope_exclusions",
        "reason",
        "reviewed",
    }
)


def _diagnostic(code: str, path: str) -> Diagnostic:
    return Diagnostic(code, _MESSAGES[code], path)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _canonical(envelope: TicketSetEnvelope) -> str:
    data = envelope.model_dump(mode="json")
    for key in ("requirements", "children"):
        data[key].sort(key=lambda row: row["id"])
    data["coverage"].sort(key=lambda row: (row["requirement"], row["child"], row["criterion"]))
    data["dependencies"].sort(key=lambda row: (row["child"], row["prerequisite"]))
    data["scope_exclusions"].sort(key=lambda row: row["requirement"])
    return json.dumps(data, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def parse_ticket_set(envelope_json: str, specifications: Mapping[str, str]) -> TicketSetResult:
    """Validate local structure and exact child bodies, not semantic coverage/readiness."""
    try:
        raw = json.loads(envelope_json, object_pairs_hook=_unique_object)
    except (ValueError, RecursionError):
        return TicketSetResult(None, (_diagnostic("set_invalid_json", "/"),))
    if not isinstance(raw, dict):
        return TicketSetResult(None, (_diagnostic("set_invalid_schema", "/"),))
    if raw.get("integration") is None:
        return TicketSetResult(None, (_diagnostic("set_missing_integration", "/integration"),))
    try:
        envelope = TicketSetEnvelope.model_validate_json(envelope_json)
    except ValidationError as exc:
        diagnostics = []
        for error in exc.errors(include_input=False, include_url=False, include_context=False):
            # Error locations may contain attacker-controlled extra keys. Never reflect them.
            parts = [
                str(part)
                for part in error["loc"]
                if isinstance(part, int) or part in _SCHEMA_FIELDS
            ]
            path = "/" + "/".join(parts)
            code = (
                "set_missing_integration" if parts[:1] == ["integration"] else "set_invalid_schema"
            )
            diagnostics.append(_diagnostic(code, path))
        return TicketSetResult(None, tuple(dict.fromkeys(diagnostics)))
    diagnostics = []
    requirements: set[str] = set()
    children: set[str] = set()
    parsed: dict[str, Specification] = {}
    for key, rows, ids in (
        ("requirements", tuple(row.id for row in envelope.requirements), requirements),
        ("children", tuple(row.id for row in envelope.children), children),
    ):
        for index, row_id in enumerate(rows):
            if row_id in ids:
                diagnostics.append(_diagnostic("set_duplicate_id", f"/{key}/{index}/id"))
            ids.add(row_id)
    for index, child in enumerate(envelope.children):
        body = specifications.get(child.id)
        result = parse_specification(body) if body is not None else None
        if result is None or result.specification is None:
            diagnostics.append(_diagnostic("set_invalid_child_specification", f"/children/{index}"))
            continue
        parsed[child.id] = result.specification
        if result.specification.fingerprint != child.specification_fingerprint:
            diagnostics.append(
                _diagnostic(
                    "set_fingerprint_mismatch", f"/children/{index}/specification_fingerprint"
                )
            )

    def criterion(child: str, number: int, path: str) -> bool:
        if child not in children:
            diagnostics.append(_diagnostic("set_unknown_reference", path + "/child"))
            return False
        if child not in parsed:
            return False
        if not any(row.number == number for row in parsed[child].criteria):
            diagnostics.append(_diagnostic("set_invalid_criterion", path + "/criterion"))
            return False
        return True

    covered: set[str] = set()
    coverage_rows: set[tuple[str, str, int]] = set()
    for index, row in enumerate(envelope.coverage):
        path = f"/coverage/{index}"
        if row.requirement not in requirements:
            diagnostics.append(_diagnostic("set_unknown_reference", path + "/requirement"))
        if criterion(row.child, row.criterion, path):
            covered.add(row.requirement)
        identity = (row.requirement, row.child, row.criterion)
        if identity in coverage_rows:
            diagnostics.append(_diagnostic("set_duplicate_coverage", path))
        coverage_rows.add(identity)
    criterion(envelope.integration.child, envelope.integration.criterion, "/integration")
    excluded: set[str] = set()
    for index, exclusion in enumerate(envelope.scope_exclusions):
        path = f"/scope_exclusions/{index}"
        if exclusion.requirement not in requirements:
            diagnostics.append(_diagnostic("set_unknown_reference", path + "/requirement"))
        if not exclusion.reviewed:
            diagnostics.append(_diagnostic("set_unreviewed_exclusion", path + "/reviewed"))
        if exclusion.requirement in excluded:
            diagnostics.append(_diagnostic("set_duplicate_exclusion", path + "/requirement"))
        if exclusion.reviewed:
            excluded.add(exclusion.requirement)
    for index, requirement in enumerate(envelope.requirements):
        if requirement.id not in covered | excluded:
            diagnostics.append(_diagnostic("set_uncovered_requirement", f"/requirements/{index}"))

    edges: set[tuple[str, str]] = set()
    successors: dict[str, set[str]] = {child: set() for child in children}
    degrees = dict.fromkeys(children, 0)
    for index, edge in enumerate(envelope.dependencies):
        path = f"/dependencies/{index}"
        for name in ("child", "prerequisite"):
            if getattr(edge, name) not in children:
                diagnostics.append(_diagnostic("set_unknown_reference", path + "/" + name))
        edge_identity = (edge.child, edge.prerequisite)
        if edge_identity in edges:
            diagnostics.append(_diagnostic("set_duplicate_dependency", path))
        elif edge.child in children and edge.prerequisite in children:
            successors[edge.prerequisite].add(edge.child)
            degrees[edge.child] += 1
        edges.add(edge_identity)
    roots = deque(sorted(child for child, degree in degrees.items() if degree == 0))
    visited = 0
    while roots:
        node = roots.popleft()
        visited += 1
        for dependent_id in sorted(successors[node]):
            degrees[dependent_id] -= 1
            if degrees[dependent_id] == 0:
                roots.append(dependent_id)
    if visited != len(children):
        diagnostics.append(_diagnostic("set_dependency_cycle", "/dependencies"))
    if diagnostics:
        return TicketSetResult(None, tuple(diagnostics))
    canonical = _canonical(envelope)
    return TicketSetResult(
        TicketSet(envelope, hashlib.sha256(canonical.encode()).hexdigest(), canonical)
    )
