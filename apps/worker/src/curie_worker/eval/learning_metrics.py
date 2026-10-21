"""Pure learning-eval observations, state differences and classification metrics.

Every input here is a serialized observation the orchestrator captured from the
pinned CLI or the transcript state API. Nothing in this module reads a store,
calls a model, or classifies prose. Absent capabilities are admitted by the
caller through ``available``; malformed present data always raises
:class:`EvidenceError` rather than becoming an empty collection or a pass.
"""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

from .scorer import ScoreResult

JsonObject = Mapping[str, object]


class LearningClass(StrEnum):
    SKILL = "skill"
    MEMORY_FACT = "memory-fact"
    INSTRUCTION = "instruction"
    SCHEDULE = "schedule"
    CLARIFY = "clarify"
    NOTHING = "nothing"


class CaseStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    ERROR = "ERROR"
    NOT_AVAILABLE = "NOT_AVAILABLE"


class EvidenceError(ValueError):
    """Missing, malformed or unadmitted evidence; never a pass or an absence."""


@dataclass(frozen=True)
class Thresholds:
    precision: float = 0.9
    recall: float = 0.8
    falsewrite: float = 0.05
    correction_duplicates: int = 0
    heldout_control_improvement: float = 0.0
    scheduled_deliveries: int = 1


@dataclass(frozen=True)
class StateSnapshot:
    skills: tuple[JsonObject, ...]
    facts: tuple[JsonObject, ...]
    schedules: tuple[JsonObject, ...]
    available: frozenset[LearningClass]


@dataclass(frozen=True)
class Mutation:
    kind: LearningClass
    item_id: str
    operation: Literal["create", "update", "delete", "archive", "revert"]
    before: JsonObject | None
    after: JsonObject | None


@dataclass(frozen=True)
class ToolEvidence:
    call_id: str
    name: str
    arguments: JsonObject
    result: object
    matched: bool
    is_error: bool | None


@dataclass(frozen=True)
class DecisionResult:
    case_id: str
    decision_id: str
    expected: LearningClass
    predicted: frozenset[LearningClass]
    status: CaseStatus
    mutations: tuple[Mutation, ...]
    attempts: tuple[ToolEvidence, ...]
    distractor: bool
    correction_duplicates: int
    detail: str


DURABLE = frozenset(
    {
        LearningClass.SKILL,
        LearningClass.MEMORY_FACT,
        LearningClass.INSTRUCTION,
        LearningClass.SCHEDULE,
    }
)
_NO_ACTION = frozenset({LearningClass.CLARIFY, LearningClass.NOTHING})
_MEMORY = frozenset({LearningClass.MEMORY_FACT, LearningClass.INSTRUCTION})
# Timestamps alone never constitute learning; every other field is compared.
_TIMESTAMPS = frozenset({"stated_at", "updated_at"})
_SCHEDULE_MATCH_FIELDS = frozenset({"state", "cron", "run_at", "timezone", "prompt", "created_by"})
_INSTANCE = re.compile(r"^(F[1-9][0-9]*-P[1-9][0-9]*)-R([1-9][0-9]*)$")
_BASE_CASE = re.compile(r"^F[1-9][0-9]*-P[1-9][0-9]*$")


# Field validators -----------------------------------------------------------


def _is_str(value: object) -> bool:
    return isinstance(value, str)


def _is_nonempty_str(value: object) -> bool:
    return isinstance(value, str) and bool(value)


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_optional_str(value: object) -> bool:
    return value is None or isinstance(value, str)


def _is_channel(value: object) -> bool:
    if value is None:
        return True
    return (
        isinstance(value, Mapping)
        and set(value) == {"kind", "address"}
        and _is_nonempty_str(value["kind"])
        and _is_nonempty_str(value["address"])
    )


_Check = Callable[[object], bool]
_FACT_FIELDS: dict[str, _Check] = {
    "id": _is_nonempty_str,
    "kind": lambda value: value in ("fact", "instruction") and _is_str(value),
    "archived": lambda value: isinstance(value, bool),
    "scope": lambda value: value in ("agent", "channel") and _is_str(value),
    "channel": _is_channel,
    "statement": _is_str,
    "author": _is_str,
    "stated_at": _is_str,
}
_SKILL_FIELDS: dict[str, _Check] = {
    "id": _is_nonempty_str,
    "slug": _is_nonempty_str,
    "revision": _is_int,
    "description": _is_str,
    "author": _is_str,
    "updated_at": _is_str,
}
_SCHEDULE_FIELDS: dict[str, _Check] = {
    "id": _is_nonempty_str,
    "revision": _is_int,
    "state": _is_nonempty_str,
    "cron": _is_optional_str,
    "run_at": _is_optional_str,
    "timezone": _is_str,
    "prompt": _is_str,
    "created_by": _is_str,
}


def _rows(value: object, label: str, fields: Mapping[str, _Check]) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise EvidenceError(f"{label} must be a JSON array")
    rows: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw in value:
        if not isinstance(raw, Mapping) or not all(isinstance(key, str) for key in raw):
            raise EvidenceError(f"{label} row must be a JSON object")
        row = deepcopy(dict(raw))
        for name, check in fields.items():
            if name in row and not check(row[name]):
                raise EvidenceError(f"{label} row has malformed {name!r}")
        identity = row.get("id")
        if isinstance(identity, str):
            if identity in seen:
                raise EvidenceError(f"{label} repeats immutable identity {identity!r}")
            seen.add(identity)
        rows.append(row)
    return rows


def _parse_facts(
    memory: object, agent: str, available: frozenset[LearningClass]
) -> tuple[JsonObject, ...]:
    if not isinstance(memory, Mapping) or not {"agent", "entries", "facts"} <= set(memory):
        raise EvidenceError("memory output must be an object with agent, entries and facts")
    if memory["agent"] != agent:
        raise EvidenceError("memory output belongs to a different agent")
    if not isinstance(memory["entries"], list):
        raise EvidenceError("memory entries must be a JSON array")
    facts = _rows(memory["facts"], "memory fact", _FACT_FIELDS)
    for row in facts:
        row.setdefault("kind", "fact")
        row.setdefault("archived", False)
        if row["kind"] == "instruction" and LearningClass.INSTRUCTION not in available:
            raise EvidenceError("instruction row contradicts admitted instruction absence")
        if "scope" in row and "channel" in row:
            if (row["scope"] == "agent") != (row["channel"] is None):
                raise EvidenceError("memory fact scope and channel disagree")
    return tuple(facts)


def _parse_schedules(schedules: object, agent: str) -> tuple[JsonObject, ...]:
    if not isinstance(schedules, Mapping) or "agent_schedules" not in schedules:
        raise EvidenceError("schedule output must carry the agent_schedules array")
    if "agent" in schedules and schedules["agent"] != agent:
        raise EvidenceError("schedule output belongs to a different agent")
    rows = _rows(schedules["agent_schedules"], "agent schedule", _SCHEDULE_FIELDS)
    if any("agent" in row and row["agent"] != agent for row in rows):
        raise EvidenceError("agent schedule row belongs to a different agent")
    return tuple(rows)


def parse_cli_snapshot(
    *,
    agent: str,
    skills: object,
    memory: object,
    schedules: object,
    available: frozenset[LearningClass],
) -> StateSnapshot:
    """Validate pinned CLI outputs into one snapshot, copying every accepted row.

    ``None`` is accepted only for a store whose capability is absent from
    ``available``; a command failure must never reach here as an empty list.
    """
    if not isinstance(available, frozenset) or not available <= DURABLE:
        raise EvidenceError("available may contain only durable capability classes")
    if not _is_nonempty_str(agent):
        raise EvidenceError("agent identity is required")

    def required(value: object, classes: Iterable[LearningClass], label: str) -> bool:
        if value is not None:
            return True
        if any(kind in available for kind in classes):
            raise EvidenceError(f"{label} output is missing for an available capability")
        return False

    skill_rows = (
        tuple(_rows(skills, "skill", _SKILL_FIELDS))
        if required(skills, (LearningClass.SKILL,), "skills")
        else ()
    )
    fact_rows = (
        _parse_facts(memory, agent, available) if required(memory, _MEMORY, "memory") else ()
    )
    schedule_rows = (
        _parse_schedules(schedules, agent)
        if required(schedules, (LearningClass.SCHEDULE,), "schedules")
        else ()
    )
    return StateSnapshot(
        skills=skill_rows, facts=fact_rows, schedules=schedule_rows, available=available
    )


# Transcript tool pairs ------------------------------------------------------


def match_tool_results(records: Sequence[JsonObject]) -> tuple[ToolEvidence, ...]:
    """Pair assistant ``tool_use`` blocks with later user ``tool_result`` blocks.

    Prose never counts. Duplicate calls or results, results before their call
    and orphan results are invalid. An unmatched call stays an attempt.
    """
    calls: dict[str, dict[str, object]] = {}
    for record in records:
        if not isinstance(record, Mapping):
            raise EvidenceError("transcript message must be an object")
        role, content = record.get("role"), record.get("content")
        if not _is_str(role) or not (isinstance(content, str) or isinstance(content, list)):
            raise EvidenceError("transcript message needs a role and content")
        if isinstance(content, str):
            continue
        for block in content:
            if not isinstance(block, Mapping):
                raise EvidenceError("transcript content block must be an object")
            if block.get("type") == "tool_use":
                call_id, name, arguments = block.get("id"), block.get("name"), block.get("input")
                if (
                    role != "assistant"
                    or not _is_nonempty_str(call_id)
                    or not _is_nonempty_str(name)
                ):
                    raise EvidenceError("tool_use needs an assistant role, id and name")
                if not isinstance(arguments, Mapping):
                    raise EvidenceError("tool_use input must be an object")
                if call_id in calls:
                    raise EvidenceError(f"duplicate tool_use id {call_id!r}")
                calls[str(call_id)] = {
                    "name": name,
                    "arguments": deepcopy(dict(arguments)),
                    "result": None,
                    "matched": False,
                    "is_error": None,
                }
            elif block.get("type") == "tool_result":
                call_id, is_error = block.get("tool_use_id"), block.get("is_error", False)
                if role != "user" or not _is_nonempty_str(call_id):
                    raise EvidenceError("tool_result needs a user role and tool_use_id")
                call = calls.get(str(call_id))
                if call is None:
                    raise EvidenceError(f"tool_result {call_id!r} has no earlier tool_use")
                if call["matched"]:
                    raise EvidenceError(f"duplicate tool_result for {call_id!r}")
                if not isinstance(is_error, bool):
                    raise EvidenceError("tool_result is_error must be a boolean")
                call.update(result=deepcopy(block.get("content")), matched=True, is_error=is_error)
    return tuple(
        ToolEvidence(
            call_id=call_id,
            name=str(call["name"]),
            arguments=call["arguments"],  # type: ignore[arg-type]
            result=call["result"],
            matched=bool(call["matched"]),
            is_error=call["is_error"],  # type: ignore[arg-type]
        )
        for call_id, call in calls.items()
    )


# State differences ----------------------------------------------------------


def _row_class(store: str, row: JsonObject) -> LearningClass:
    if store == "skills":
        return LearningClass.SKILL
    if store == "schedules":
        return LearningClass.SCHEDULE
    return (
        LearningClass.INSTRUCTION if row.get("kind") == "instruction" else LearningClass.MEMORY_FACT
    )


def _by_id(rows: Sequence[JsonObject], store: str) -> dict[str, JsonObject]:
    indexed: dict[str, JsonObject] = {}
    for row in rows:
        identity = row.get("id")
        if not _is_nonempty_str(identity):
            raise EvidenceError(f"{store} row lacks the immutable identity a difference needs")
        indexed[str(identity)] = row
    return indexed


def _content(row: JsonObject) -> dict[str, object]:
    return {key: value for key, value in row.items() if key not in _TIMESTAMPS}


def diff_state(before: StateSnapshot, after: StateSnapshot) -> tuple[Mutation, ...]:
    """Persisted identity-bound differences; order and timestamps are ignored."""
    if before.available != after.available:
        raise EvidenceError("snapshots disagree about admitted capabilities")
    mutations: list[Mutation] = []
    for store in ("skills", "facts", "schedules"):
        old = _by_id(getattr(before, store), store)
        new = _by_id(getattr(after, store), store)
        for identity in sorted(old.keys() | new.keys()):
            prior, current = old.get(identity), new.get(identity)
            if prior is None and current is not None:
                mutations.append(
                    Mutation(_row_class(store, current), identity, "create", None, current)
                )
            elif current is None and prior is not None:
                mutations.append(
                    Mutation(_row_class(store, prior), identity, "delete", prior, None)
                )
            elif prior is not None and current is not None and _content(prior) != _content(current):
                archived = prior.get("archived") is False and current.get("archived") is True
                operation: Literal["update", "archive"] = "archive" if archived else "update"
                mutations.append(
                    Mutation(_row_class(store, current), identity, operation, prior, current)
                )
    return tuple(mutations)


# Deterministic state predicates ---------------------------------------------

_COUNTED_OPERATION = {
    "created_count": "create",
    "updated_count": "update",
    "deleted_count": "delete",
}
_COUNT_PREDICATES = frozenset({"active_count", *_COUNTED_OPERATION})
_FLAG_PREDICATES = frozenset({"same_id", "revision_increased", "unchanged_other_stores"})
_FIELD_PREDICATES = {
    "statement_equals": "statement",
    "author_equals": "author",
    "scope_equals": "scope",
    "channel_equals": "channel",
}
_PREDICATES = (
    _COUNT_PREDICATES
    | _FLAG_PREDICATES
    | frozenset(_FIELD_PREDICATES)
    | {"body_contains_steps", "schedule_matches"}
)
_STORE_OF = {
    LearningClass.SKILL: "skills",
    LearningClass.MEMORY_FACT: "facts",
    LearningClass.INSTRUCTION: "facts",
    LearningClass.SCHEDULE: "schedules",
}
# The fixture subject of a row: a skill slug, or a synthetic token in a fact
# statement or schedule prompt. An immutable ID always matches as well.
_SUBJECT_FIELD = {"skills": "slug", "facts": "statement", "schedules": "prompt"}


@dataclass(frozen=True)
class _Predicate:
    name: str
    kind: LearningClass
    target: str
    value: object


def _is_normalized_text(value: object) -> bool:
    """Containment needles must survive normalization, or they would match anything."""
    return isinstance(value, str) and bool(_norm(value))


def _predicate(raw: object) -> _Predicate:
    if not isinstance(raw, Mapping):
        raise EvidenceError("predicate must be an object")
    name, kind, target = raw.get("name"), raw.get("class"), raw.get("target")
    if not isinstance(name, str) or name not in _PREDICATES:
        raise EvidenceError(f"unsupported predicate {name!r}")
    if not _is_str(kind) or kind not in {item.value for item in DURABLE}:
        raise EvidenceError(f"predicate {name} has an invalid class")
    if not _is_nonempty_str(target) or ((target == "*") != (name == "unchanged_other_stores")):
        raise EvidenceError(f"predicate {name} has an invalid target")
    argument = "steps" if name == "body_contains_steps" else "value"
    if set(raw) != {"name", "class", "target", argument}:
        raise EvidenceError(f"predicate {name} must carry exactly name, class, target, {argument}")
    value = raw[argument]
    learning_class = LearningClass(str(kind))
    if name in _COUNT_PREDICATES:
        valid = isinstance(value, int) and not isinstance(value, bool) and value >= 0
    elif name in _FLAG_PREDICATES:
        valid = value is True
    elif name == "body_contains_steps":
        valid = (
            learning_class is LearningClass.SKILL
            and isinstance(value, list)
            and bool(value)
            and all(isinstance(step, str) and _required_tokens(step) for step in value)
        )
    elif name == "schedule_matches":
        valid = (
            learning_class is LearningClass.SCHEDULE
            and isinstance(value, Mapping)
            and bool(value)
            and set(value) <= _SCHEDULE_MATCH_FIELDS
            and ("prompt" not in value or _is_normalized_text(value["prompt"]))
        )
    elif name == "scope_equals":
        valid = value in ("agent", "channel") and _is_str(value)
    elif name == "channel_equals":
        valid = _is_nonempty_str(value) or (value is not None and _is_channel(value))
    else:
        valid = _is_str(value)
    if not valid:
        raise EvidenceError(f"predicate {name} has a malformed {argument}")
    return _Predicate(str(name), learning_class, str(target), value)


def _class_rows(snapshot: StateSnapshot, kind: LearningClass) -> list[JsonObject]:
    store = _STORE_OF[kind]
    return [row for row in getattr(snapshot, store) if _row_class(store, row) is kind]


def _active(kind: LearningClass, row: JsonObject) -> bool:
    if kind is LearningClass.SCHEDULE:
        return row.get("state") == "active"
    return row.get("archived") is not True


def _matches(kind: LearningClass, row: JsonObject, target: str) -> bool:
    if row.get("id") == target:
        return True
    field = _SUBJECT_FIELD[_STORE_OF[kind]]
    if field not in row:
        raise EvidenceError(f"row lacks {field!r} needed to resolve target {target!r}")
    subject = row[field]
    return subject == target if kind is LearningClass.SKILL else target in str(subject)


def _targeted(snapshot: StateSnapshot, check: _Predicate) -> list[JsonObject]:
    rows = _class_rows(snapshot, check.kind)
    _by_id(rows, _STORE_OF[check.kind])
    return [row for row in rows if _matches(check.kind, row, check.target)]


def _field(row: JsonObject, name: str) -> object:
    if name not in row:
        raise EvidenceError(f"unadmitted predicate needs absent stored field {name!r}")
    return row[name]


_LIST_MARKER = re.compile(r"^[ \t]*(?:[0-9]+[.)]|[-*+\u2022])[ \t]+", re.MULTILINE)
_EMPHASIS_UNDERSCORE = re.compile(r"(?<![0-9A-Za-z])_+|_+(?![0-9A-Za-z])")
_QUOTES = "\"'\u201c\u201d\u2018\u2019"


def _norm(text: str) -> str:
    """Normalize stored prose for the three content oracles (decision after live run 4).

    Drops a leading list marker per line, inline Markdown marks (backticks,
    asterisks, emphasis underscores but not underscores inside identifiers),
    collapses whitespace, casefolds, and strips trailing ``.;:`` and
    surrounding quotes.
    """
    text = _LIST_MARKER.sub("", text)
    text = _EMPHASIS_UNDERSCORE.sub("", text.replace("`", "").replace("*", ""))
    text = " ".join(text.split()).casefold()
    while True:
        trimmed = text.strip().rstrip(".;:").strip().strip(_QUOTES)
        if trimmed == text:
            return text
        text = trimmed


# Section A3: share of a step's distinct non-nonce tokens the body must contain.
_STEP_TOKEN_COVERAGE = 0.85
_TOKEN_SPLIT = re.compile(r"[^a-z0-9-]+")
_NONCE = re.compile(r"^[a-z]+-[0-9a-f]{6}$")
_STOPWORDS = frozenset(
    "a an the and or of to in on for by as is it its this that then with from each their"
    " those these be".split()
)


def _tokens(text: str) -> list[str]:
    stripped = (token.strip("-") for token in _TOKEN_SPLIT.split(_norm(text)))
    return [token for token in stripped if token]


def _required_tokens(step: str) -> list[str]:
    return [token for token in _tokens(step) if token not in _STOPWORDS]


def _contains_in_order(body: object, steps: Sequence[object]) -> bool:
    """Token anchors (decision after live run 7).

    Every nonce of every step and at least 85 percent of each step's other distinct
    non-stopword tokens must be words of the body, nonce
    tokens must first occur in the body in the same relative order as in the steps,
    and each step's unique-token anchor must appear in step order.
    """
    if not isinstance(body, str):
        raise EvidenceError("stored skill body must be text")
    words = _tokens(body)
    first: dict[str, int] = {}
    for index, word in enumerate(words):
        first.setdefault(word, index)
    required = [_required_tokens(str(step)) for step in steps]
    nonces: list[str] = []
    for tokens in required:
        plain = {token for token in tokens if not _NONCE.match(token)}
        if plain and len(plain & first.keys()) < _STEP_TOKEN_COVERAGE * len(plain):
            return False
        for token in tokens:
            if _NONCE.match(token):
                if token not in first:
                    return False
                if token not in nonces:
                    nonces.append(token)
    positions = [first[token] for token in nonces]
    if positions != sorted(positions):
        return False
    # Section A2: each step's anchor is its first token found in no other step; anchors
    # must occur in step order under a greedy forward cursor.
    cursor = 0
    for index, tokens in enumerate(required):
        others = {token for other, rest in enumerate(required) if other != index for token in rest}
        anchor = next((token for token in tokens if token not in others), None)
        if anchor is None:
            continue
        found = next((at for at in range(cursor, len(words)) if words[at] == anchor), None)
        if found is None:
            return False
        cursor = found
    return True


def _statement_agrees(statement: object, expected: str) -> bool:
    """Word-bounded normalized containment with no competing value of the nonce shape."""
    if not isinstance(statement, str):
        return False
    needle, haystack = _norm(expected), _norm(statement)
    bounded = rf"(?<![a-z0-9-]){re.escape(needle)}(?![a-z0-9-])"
    if not needle or re.search(bounded, haystack) is None:
        return False
    allowed = {token for token in _tokens(expected) if _NONCE.match(token)}
    if not allowed:
        return True
    return all(token in allowed for token in _tokens(statement) if _NONCE.match(token))


def _schedule_agrees(observed: Mapping[str, object], expected: Mapping[str, object]) -> bool:
    for key, value in expected.items():
        stored = observed[key]
        if key == "prompt":
            if not isinstance(stored, str) or _norm(str(value)) not in _norm(stored):
                return False
        elif stored != value:
            return False
    return True


def _evaluate(check: _Predicate, before: StateSnapshot, after: StateSnapshot) -> bool:
    if check.name == "unchanged_other_stores":
        return not any(item.kind is check.kind for item in diff_state(before, after))
    operation = _COUNTED_OPERATION.get(check.name)
    if operation is not None:
        count = 0
        for item in diff_state(before, after):
            row = item.after if item.after is not None else item.before
            if item.kind is check.kind and item.operation == operation and row is not None:
                count += _matches(check.kind, row, check.target)
        return count == check.value
    current = _targeted(after, check)
    active = [row for row in current if _active(check.kind, row)]
    if check.name == "active_count":
        return len(active) == check.value
    if check.name == "same_id":
        earlier = [row for row in _targeted(before, check) if _active(check.kind, row)]
        identities = {row["id"] for row in active}
        return bool(identities) and identities == {row["id"] for row in earlier}
    if check.name == "revision_increased":
        prior = {row["id"]: row for row in _targeted(before, check)}
        increased: list[bool] = []
        for row in current:
            if row["id"] not in prior:
                continue
            old, new = _field(prior[row["id"]], "revision"), _field(row, "revision")
            if not isinstance(old, int) or not isinstance(new, int):
                raise EvidenceError("revision must be an integer")
            increased.append(new > old)
        return bool(increased) and all(increased)
    if check.name == "schedule_matches":
        expected = check.value
        assert isinstance(expected, Mapping)
        values = [{key: _field(row, key) for key in expected} for row in current]
        return bool(values) and all(_schedule_agrees(value, expected) for value in values)
    if check.name == "body_contains_steps":
        bodies = [_field(row, "body") for row in active]
        assert isinstance(check.value, list)
        return bool(bodies) and all(_contains_in_order(body, check.value) for body in bodies)
    observed = [_field(row, _FIELD_PREDICATES[check.name]) for row in active]
    if check.name == "statement_equals":
        return bool(observed) and all(
            _statement_agrees(value, str(check.value)) for value in observed
        )
    return bool(observed) and all(value == check.value for value in observed)


def score_state(
    before: StateSnapshot,
    after: StateSnapshot,
    expectation: JsonObject,
) -> ScoreResult:
    """Apply admitted deterministic predicates; never classifies prose or calls a model."""
    raw = expectation.get("predicates") if isinstance(expectation, Mapping) else None
    if not isinstance(raw, list) or not raw:
        raise EvidenceError("expectation needs a nonempty predicates array")
    checks = [_predicate(item) for item in raw]
    failed: list[str] = []
    for check in checks:
        if check.kind not in before.available or check.kind not in after.available:
            raise EvidenceError(f"predicate {check.name} targets unadmitted {check.kind}")
        if not _evaluate(check, before, after):
            failed.append(f"{check.name}[{check.kind}:{check.target}]")
    if failed:
        return ScoreResult(passed=False, detail="failed state predicates: " + ", ".join(failed))
    return ScoreResult(passed=True)


# Classification summary -----------------------------------------------------


def _validate_thresholds(thresholds: Thresholds) -> None:
    for name in ("precision", "recall", "falsewrite", "heldout_control_improvement"):
        value = getattr(thresholds, name)
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise EvidenceError(f"threshold {name} must be a number")
        if not math.isfinite(value) or not 0 <= value <= 1:
            raise EvidenceError(f"threshold {name} must be a finite ratio in [0, 1]")
    if not _is_int(thresholds.correction_duplicates) or thresholds.correction_duplicates < 0:
        raise EvidenceError("threshold correction_duplicates must be a nonnegative integer")
    if not _is_int(thresholds.scheduled_deliveries) or thresholds.scheduled_deliveries < 1:
        raise EvidenceError("threshold scheduled_deliveries must be a positive integer")


def _validate_row(row: object) -> None:
    if not isinstance(row, DecisionResult):
        raise EvidenceError("summary input must be DecisionResult rows")
    if not isinstance(row.expected, LearningClass) or not isinstance(row.status, CaseStatus):
        raise EvidenceError(f"{row.case_id}: expected class and status must be typed")
    if not isinstance(row.predicted, frozenset) or not all(
        isinstance(kind, LearningClass) for kind in row.predicted
    ):
        raise EvidenceError(f"{row.case_id}: predicted holds an unsupported class")
    if not _is_nonempty_str(row.decision_id) or not isinstance(row.distractor, bool):
        raise EvidenceError(f"{row.case_id}: decision identity or distractor flag is malformed")
    if not _is_int(row.correction_duplicates) or row.correction_duplicates < 0:
        raise EvidenceError(f"{row.case_id}: correction_duplicates must be a nonnegative integer")
    if not all(isinstance(item, Mutation) for item in row.mutations) or not all(
        isinstance(item, ToolEvidence) for item in row.attempts
    ):
        raise EvidenceError(f"{row.case_id}: mutations or attempts are malformed")
    durable = row.predicted & DURABLE
    if durable != {item.kind for item in row.mutations}:
        raise EvidenceError(f"{row.case_id}: durable predictions must equal persisted mutations")
    if row.predicted & _NO_ACTION and (durable or len(row.predicted) > 1):
        raise EvidenceError(f"{row.case_id}: a no-action prediction cannot carry other classes")
    if row.status is CaseStatus.NOT_AVAILABLE and row.predicted & _NO_ACTION:
        raise EvidenceError(f"{row.case_id}: an unavailable row cannot predict clarify or nothing")


def _ratio(numerator: int, denominator: int) -> float | None:
    return None if denominator == 0 else numerator / denominator


def _metrics(rows: Sequence[DecisionResult], thresholds: Thresholds) -> dict[str, object]:
    classes = list(LearningClass)
    eligible = [row for row in rows if row.status is not CaseStatus.NOT_AVAILABLE]
    unavailable = [row for row in rows if row.status is CaseStatus.NOT_AVAILABLE]
    per_class: dict[str, dict[str, object]] = {}
    for kind in classes:
        tp = sum(row.expected is kind and kind in row.predicted for row in eligible)
        fn = sum(row.expected is kind and kind not in row.predicted for row in eligible)
        fp = sum(row.expected is not kind and kind in row.predicted for row in eligible)
        per_class[kind.value] = {
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "support": tp + fn,
            "precision": _ratio(tp, tp + fp),
            "recall": _ratio(tp, tp + fn),
            "errors": sum(
                row.expected is kind and row.status is CaseStatus.ERROR for row in eligible
            ),
            "unavailable": sum(row.expected is kind for row in unavailable),
        }
    confusion = {
        expected.value: {
            predicted.value: sum(
                row.expected is expected and predicted in row.predicted for row in eligible
            )
            for predicted in classes
        }
        for expected in classes
    }
    off_target = {
        expected.value: {
            observed.value: sum(
                item.kind is observed
                for row in unavailable
                if row.expected is expected
                for item in row.mutations
            )
            for observed in sorted(DURABLE)
        }
        for expected in classes
    }
    missing = sorted(
        kind.value
        for kind in classes
        if per_class[kind.value]["support"] == 0 or per_class[kind.value]["unavailable"]
    )
    complete = not missing
    gates: dict[str, dict[str, object]] = {}
    for gate, threshold in (("precision", thresholds.precision), ("recall", thresholds.recall)):
        if not complete:
            gates[gate] = {"status": CaseStatus.NOT_AVAILABLE.value, "value": None}
            continue
        values = [per_class[kind.value][gate] for kind in classes]
        ratios = [value for value in values if isinstance(value, float)]
        floor = min(ratios) if len(ratios) == len(values) else None
        passed = floor is not None and floor >= threshold
        gates[gate] = {"status": "PASS" if passed else "FAIL", "value": floor}
    distractors = [row for row in eligible if row.distractor]
    writes = sum(bool(row.mutations) for row in distractors)
    attempts = sum(bool(row.mutations or row.attempts) for row in distractors)
    rate = _ratio(writes, len(distractors))
    gates["falsewrite"] = (
        {"status": CaseStatus.NOT_AVAILABLE.value, "value": None}
        if rate is None
        else {"status": "PASS" if rate <= thresholds.falsewrite else "FAIL", "value": rate}
    )
    # Only the F8 "correct" decision measures correction duplicates; seed rows never count.
    corrections = [
        row
        for row in eligible
        if row.case_id.startswith(_CORRECTION_FAMILY) and row.decision_id == "correct"
    ]
    duplicates = sum(row.correction_duplicates for row in corrections)
    gates["correction_duplicates"] = (
        {"status": CaseStatus.NOT_AVAILABLE.value, "value": None}
        if not corrections
        else {
            "status": "PASS" if duplicates <= thresholds.correction_duplicates else "FAIL",
            "value": duplicates,
        }
    )
    return {
        "per_class": per_class,
        "gates": gates,
        "coverage": {
            "status": "PASS" if complete else CaseStatus.NOT_AVAILABLE.value,
            "incomplete_classes": missing,
        },
        "distractor": {
            "evaluated": len(distractors),
            "falsewrite_count": writes,
            "falsewrite_rate": rate,
            "attempted_write_rate": _ratio(attempts, len(distractors)),
        },
        "unavailable_off_target_writes": off_target,
        "confusion": confusion,
        "correction_duplicates": duplicates,
    }


_DEFAULT_THRESHOLDS = Thresholds()
# F8 is the correction-of-an-existing-skill family; its rows are the correction decisions.
_CORRECTION_FAMILY = "F8-"
_SEVERITY = {"PASS": 0, "NOT_AVAILABLE": 1, "FAIL": 2}


def summarize(
    results: Sequence[DecisionResult],
    *,
    expected_case_ids: Sequence[str],
    repetitions: int,
    thresholds: Thresholds = _DEFAULT_THRESHOLDS,
) -> dict[str, object]:
    """Per-repetition classification, false-write and duplicate gates plus pooled counts.

    Each top-level gate takes the worst repetition status, so a pooled average
    cannot rescue a failed repetition; its value is the pooled value.
    """
    _validate_thresholds(thresholds)
    if not _is_int(repetitions) or repetitions < 1:
        raise EvidenceError("repetitions must be a positive integer")
    bases = list(expected_case_ids)
    if not bases or len(set(bases)) != len(bases):
        raise EvidenceError("expected case IDs must be nonempty and unique")
    if not all(isinstance(base, str) and _BASE_CASE.match(base) for base in bases):
        raise EvidenceError("expected case IDs must look like F<n>-P<n>")

    identities: set[tuple[str, str]] = set()
    decisions: dict[tuple[int, str], dict[str, tuple[LearningClass, bool]]] = {}
    by_repetition: dict[int, list[DecisionResult]] = {n: [] for n in range(1, repetitions + 1)}
    for row in results:
        _validate_row(row)
        found = _INSTANCE.match(row.case_id) if isinstance(row.case_id, str) else None
        if found is None or found.group(1) not in bases:
            raise EvidenceError(f"unexpected scenario instance {row.case_id!r}")
        base, repetition = found.group(1), int(found.group(2))
        if repetition > repetitions:
            raise EvidenceError(f"{row.case_id}: repetition exceeds the requested count")
        if (row.case_id, row.decision_id) in identities:
            raise EvidenceError(f"duplicate decision {row.case_id}/{row.decision_id}")
        identities.add((row.case_id, row.decision_id))
        decisions.setdefault((repetition, base), {})[row.decision_id] = (
            row.expected,
            row.distractor,
        )
        by_repetition[repetition].append(row)
    for base in bases:
        inventories = [decisions.get((n, base)) for n in range(1, repetitions + 1)]
        if any(inventory is None for inventory in inventories):
            raise EvidenceError(f"scenario {base} is missing from a repetition")
        if any(inventory != inventories[0] for inventory in inventories):
            raise EvidenceError(f"scenario {base} changed its decisions across repetitions")

    pooled = _metrics([row for rows in by_repetition.values() for row in rows], thresholds)
    individual = [
        {"repetition": n, **_metrics(rows, thresholds)} for n, rows in by_repetition.items()
    ]
    gates = pooled["gates"]
    assert isinstance(gates, dict)
    for name, gate in gates.items():
        statuses = [str(summary["gates"][name]["status"]) for summary in individual]  # type: ignore[index]
        worst = max(statuses, key=_SEVERITY.__getitem__)
        if worst == CaseStatus.NOT_AVAILABLE.value:
            gates[name] = {"status": worst, "value": None}
        else:
            gate["status"] = worst
    return {**pooled, "repetitions": individual}
