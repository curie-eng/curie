"""The nomination block parser and the per-entry policy validation.

@spec AUTOMATED-REMEDIATION-5 @spec AUTOMATED-REMEDIATION-7 @spec AUTOMATED-REMEDIATION-26.

``parse_nomination_block`` takes the exact text the protected worker withheld
and submitted (fences included) and is the one production parser: a block
outside the grammar of AUTOMATED-REMEDIATION-5 raises ``NominationMalformed`` as
a whole; a valid block yields one entry per nomination in order, with its
arguments in the executor's canonical form (ACTION-EXECUTOR-7: sorted keys,
``,``/``:`` separators, non-ASCII unescaped, SHA-256 hex over the UTF-8 bytes)
and a later entry naming the same action and canonical arguments refused
``nomination_duplicate``. The grammar is frozen in
``tests/vectors/remediation-nomination.json``.

The text is accepted only as exactly one opening fence line, the JSON lines,
and one closing fence line that ends the text (a final line terminator is
optional), so two blocks, an unclosed block and anything after the closing
fence are malformed. A string the store cannot hold exactly (a lone surrogate
or a NUL character) is malformed too, as a value JSON cannot represent exactly.

``validate_entry`` checks one well-formed entry against the bound policy's
action (AUTOMATED-REMEDIATION-7): ``unknown_action`` and
``arguments_schema_mismatch`` (a key outside the action's argument schema, a
declared key missing, or a value of the wrong type). Values of the right type
outside the allowed set, range or target list are not refused here; admission
sends them to approval. ``target_key`` is the AUTOMATED-REMEDIATION-10 target
key: the action's connector and the canonical JSON of the target argument.

A ``tune`` entry (AUTOMATED-REMEDIATION-25) is checked against the action's own
shape instead: its rule one the action declares, its field one the change
schema declares, its value of that field's type, and for ``retire`` exactly
``{"duplicate_of": <rule>}`` naming a rule the action lists as a duplicate
target. Its target key is the rule owner connector and the canonical rule.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

# @spec AUTOMATED-REMEDIATION-5
OPENING_FENCE: Final = "```curie-remediation"
CLOSING_FENCE: Final = "```"
LINE_TERMINATOR: Final = "\n"
MAX_BLOCK_BYTES: Final = 16384
MAX_ENTRIES: Final = 5
MAX_REASON_CHARACTERS: Final = 500
VERSION: Final = 1
MALFORMED_CODE: Final = "nomination_malformed"
DUPLICATE_CODE: Final = "nomination_duplicate"
# @spec AUTOMATED-REMEDIATION-7
UNKNOWN_ACTION_CODE: Final = "unknown_action"
SCHEMA_MISMATCH_CODE: Final = "arguments_schema_mismatch"

_TOP_KEYS: Final = frozenset({"version", "nominations"})
_ARGUMENTS_REQUIRED: Final = frozenset({"action", "arguments"})
_CHANGE_REQUIRED: Final = frozenset({"action", "rule", "field", "value"})
_OPTIONAL: Final = frozenset({"reason"})
# @spec AUTOMATED-REMEDIATION-25
TUNE_KIND: Final = "tune"
TUNE_RETIRE: Final = "retire"
_TUNE_ARGUMENTS: Final = frozenset({"field", "rule", "value"})


class NominationMalformed(Exception):
    """The whole block is outside the grammar. @spec AUTOMATED-REMEDIATION-5."""

    code: Final = MALFORMED_CODE

    def __init__(self) -> None:
        super().__init__(MALFORMED_CODE)


@dataclass(frozen=True, slots=True)
class ParsedNomination:
    """One entry of a valid block. @spec AUTOMATED-REMEDIATION-5 @spec AUTOMATED-REMEDIATION-7."""

    action: str
    arguments: str
    arguments_sha256: str
    reason: str | None
    refusal: str | None
    value: Mapping[str, Any]


def canonical_text(value: Any) -> str:
    """The executor's canonical JSON text (ACTION-EXECUTOR-7). @spec AUTOMATED-REMEDIATION-5."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise NominationMalformed()
        result[key] = value
    return result


def _finite_float(literal: str) -> float:
    value = float(literal)
    if not math.isfinite(value):
        raise NominationMalformed()
    return value


def _refuse_constant(_literal: str) -> Any:  # noqa: ANN401 - json's parse_constant hook
    raise NominationMalformed()


def _representable(value: Any) -> None:
    """Every string (keys included) is UTF-8 text without NUL.

    @spec AUTOMATED-REMEDIATION-5.
    """
    if isinstance(value, str):
        if "\x00" in value:
            raise NominationMalformed()
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:
            raise NominationMalformed() from None
    elif isinstance(value, dict):
        for key, item in value.items():
            _representable(key)
            _representable(item)
    elif isinstance(value, list):
        for item in value:
            _representable(item)


def _body(text: str) -> str:
    """The JSON between the fences, or malformed. @spec AUTOMATED-REMEDIATION-5."""
    if type(text) is not str:
        raise NominationMalformed()
    try:
        size = len(text.encode("utf-8"))
    except UnicodeEncodeError:
        raise NominationMalformed() from None
    if size > MAX_BLOCK_BYTES:
        raise NominationMalformed()
    lines = text.split(LINE_TERMINATOR)
    if lines and lines[-1] == "" and len(lines) > 1:
        lines.pop()
    if len(lines) < 3 or lines[0] != OPENING_FENCE:
        raise NominationMalformed()
    try:
        close = lines.index(CLOSING_FENCE, 1)
    except ValueError:
        raise NominationMalformed() from None
    if close != len(lines) - 1:
        raise NominationMalformed()
    return LINE_TERMINATOR.join(lines[1:close])


def _entry(item: Any) -> tuple[str, dict[str, Any], str | None]:
    """One entry's action, arguments object and reason. @spec AUTOMATED-REMEDIATION-5."""
    if type(item) is not dict:
        raise NominationMalformed()
    keys = frozenset(item)
    if "arguments" in item:
        required = _ARGUMENTS_REQUIRED
    else:
        # A tune nomination (AUTOMATED-REMEDIATION-25): its arguments are the
        # canonical {field, rule, value} object.
        required = _CHANGE_REQUIRED
    if not required <= keys or not keys <= required | _OPTIONAL:
        raise NominationMalformed()
    action = item["action"]
    if type(action) is not str:
        raise NominationMalformed()
    if required is _ARGUMENTS_REQUIRED:
        arguments = item["arguments"]
        if type(arguments) is not dict:
            raise NominationMalformed()
    else:
        if type(item["rule"]) is not str or type(item["field"]) is not str:
            raise NominationMalformed()
        arguments = {"field": item["field"], "rule": item["rule"], "value": item["value"]}
    reason = item.get("reason")
    if "reason" in item and (type(reason) is not str or len(reason) > MAX_REASON_CHARACTERS):
        raise NominationMalformed()
    return action, arguments, reason


def parse_nomination_block(text: str) -> list[ParsedNomination]:
    """Parse one submitted block, or raise ``NominationMalformed``.

    @spec AUTOMATED-REMEDIATION-5 @spec AUTOMATED-REMEDIATION-7.
    """
    body = _body(text)
    try:
        document = json.loads(
            body,
            object_pairs_hook=_no_duplicate_keys,
            parse_float=_finite_float,
            parse_constant=_refuse_constant,
        )
    except (ValueError, RecursionError):
        raise NominationMalformed() from None
    if type(document) is not dict or frozenset(document) != _TOP_KEYS:
        raise NominationMalformed()
    version = document["version"]
    if type(version) is not int or version != VERSION:
        raise NominationMalformed()
    nominations = document["nominations"]
    if type(nominations) is not list or not 1 <= len(nominations) <= MAX_ENTRIES:
        raise NominationMalformed()
    _representable(document)
    parsed: list[ParsedNomination] = []
    seen: set[tuple[str, str]] = set()
    for item in nominations:
        action, arguments, reason = _entry(item)
        canonical = canonical_text(arguments)
        identity = (action, canonical)
        refusal = DUPLICATE_CODE if identity in seen else None
        seen.add(identity)
        parsed.append(
            ParsedNomination(
                action=action,
                arguments=canonical,
                arguments_sha256=hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
                reason=reason,
                refusal=refusal,
                value=arguments,
            )
        )
    return parsed


def _typed(kind: Any, value: Any) -> bool:
    """Whether ``value`` has the declared argument type. @spec AUTOMATED-REMEDIATION-7."""
    if kind == "string":
        return type(value) is str
    if kind == "integer":
        return type(value) is int
    if kind == "number":
        return type(value) in (int, float)
    if kind == "boolean":
        return type(value) is bool
    return False


def find_action(document: Mapping[str, Any] | None, name: str) -> Mapping[str, Any] | None:
    """The policy action named ``name``, if declared. @spec AUTOMATED-REMEDIATION-7."""
    if document is None:
        return None
    for action in document.get("actions") or ():
        if isinstance(action, Mapping) and action.get("name") == name:
            return action
    return None


def _tune_refusal(action: Mapping[str, Any], arguments: Mapping[str, Any]) -> str | None:
    """A tune entry's shape against its action. @spec AUTOMATED-REMEDIATION-25."""
    if set(arguments) != _TUNE_ARGUMENTS:
        return SCHEMA_MISMATCH_CODE
    rules, change = action.get("rules"), action.get("change")
    rule, field, value = arguments["rule"], arguments["field"], arguments["value"]
    if not isinstance(rules, Mapping) or not isinstance(change, Mapping):
        return SCHEMA_MISMATCH_CODE
    if type(rule) is not str or rule not in rules:
        return SCHEMA_MISMATCH_CODE
    if type(field) is not str or field not in change:
        return SCHEMA_MISMATCH_CODE
    spec = change[field]
    if not isinstance(spec, Mapping):
        return SCHEMA_MISMATCH_CODE
    if field == TUNE_RETIRE:
        duplicates = spec.get("duplicate_of")
        if (
            type(value) is not dict
            or set(value) != {"duplicate_of"}
            or type(value["duplicate_of"]) is not str
            or value["duplicate_of"] == rule
            or not isinstance(duplicates, list)
            or value["duplicate_of"] not in duplicates
        ):
            return SCHEMA_MISMATCH_CODE
        return None
    if not _typed(spec.get("type"), value):
        return SCHEMA_MISMATCH_CODE
    return None


def validate_entry(action: Mapping[str, Any] | None, arguments: Mapping[str, Any]) -> str | None:
    """The entry's parse refusal against its policy action, or None.

    Only shape is refused: an unknown action, a key outside the action's
    argument schema, a declared key missing, or a value of the wrong type.
    Bounds are admission's (AUTOMATED-REMEDIATION-8 check 8). A ``tune``
    action's entry is ``{rule, field, value}`` against its declared rules and
    change schema (``_tune_refusal``).
    @spec AUTOMATED-REMEDIATION-7 @spec AUTOMATED-REMEDIATION-25.
    """
    if action is None:
        return UNKNOWN_ACTION_CODE
    if action.get("kind") == TUNE_KIND:
        return _tune_refusal(action, arguments)
    schema = action.get("arguments")
    if not isinstance(schema, Mapping) or set(arguments) != set(schema):
        return SCHEMA_MISMATCH_CODE
    for key, value in arguments.items():
        spec = schema[key]
        if not isinstance(spec, Mapping) or not _typed(spec.get("type"), value):
            return SCHEMA_MISMATCH_CODE
    return None


def target_key(action: Mapping[str, Any], arguments: Mapping[str, Any]) -> str | None:
    """The AUTOMATED-REMEDIATION-10 target key: connector and canonical target value.

    For a ``tune`` action it is the rule owner connector and the canonical rule
    identifier (AUTOMATED-REMEDIATION-25).
    @spec AUTOMATED-REMEDIATION-7 @spec AUTOMATED-REMEDIATION-10 @spec AUTOMATED-REMEDIATION-25.
    """
    target = action.get("target")
    connector = action.get("connector")
    if action.get("kind") == TUNE_KIND:
        rule = arguments.get("rule")
        if type(connector) is not str or type(rule) is not str:
            return None
        return f"{connector}:{canonical_text(rule)}"
    if not isinstance(target, Mapping) or type(connector) is not str:
        return None
    argument = target.get("argument")
    if type(argument) is not str or argument not in arguments:
        return None
    return f"{connector}:{canonical_text(arguments[argument])}"
