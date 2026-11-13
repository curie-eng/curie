"""The bundle's tracker result declaration, ``workflow/result.json`` (ADR 0277, #5585).

ADR 0277 decision 1 lets a deployed bundle declare that its runs end in a
tracker result (a published ticket or a findings comment) instead of a pull
request. The declaration is one small JSON file at the bundle root. It is not
part of ``.claude-plugin/`` and not part of the frozen ``plugin_format``
manifest schema, so the API parses it on top of ``validate_bundle`` the same
way ``bundles.sealing_key_custody_issues`` adds its API-only issues. A bundle
without the file has the implementation contract (completion by pull request),
unchanged.

Every rule fails closed. A present file that cannot be read, parsed or
validated is a validation error, never "no declaration". The digest is the
SHA-256 of the canonical JSON of the parsed object (keys sorted, UTF-8, no
insignificant whitespace), so key order and whitespace do not change it while
list order does. ``tests/vectors/tracker-result-declaration.json`` freezes the
valid and invalid inputs and the expected digests for every later reader.

Messages are fixed text. They never echo file content beyond a key name in the
location, so an issue is safe to log and to return to the uploader.
"""

import enum
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from plugin_format import ValidationIssue

RESULT_DECLARATION_FILE = "workflow/result.json"
SCHEMA_VERSION = 1
CONTRACT = "tracker_result"
MAX_IMPLEMENTATION_LABEL = 50
MAX_DECLARATION_BYTES = 4096

_CODE_UNREADABLE = "tracker_result.unreadable"
_CODE_INVALID_JSON = "tracker_result.invalid_json"
_CODE_NOT_OBJECT = "tracker_result.not_object"
_CODE_UNKNOWN_FIELD = "tracker_result.unknown_field"
_CODE_MISSING_FIELD = "tracker_result.missing_field"
_CODE_SCHEMA_VERSION = "tracker_result.schema_version"
_CODE_CONTRACT = "tracker_result.contract"
_CODE_RESULTS = "tracker_result.results"
_CODE_IMPLEMENTATION_LABEL = "tracker_result.implementation_label"

_REQUIRED_FIELDS = ("schema_version", "contract", "results")
_KNOWN_FIELDS = frozenset({*_REQUIRED_FIELDS, "implementation_label"})


class ResultKind(enum.StrEnum):
    """A result shape a tracker result contract may produce."""

    TICKET = "ticket"
    FINDINGS = "findings"


@dataclass(frozen=True)
class ResultDeclaration:
    """A parsed, valid declaration and its result-policy identity."""

    schema_version: int
    results: tuple[ResultKind, ...]
    implementation_label: str | None
    digest: str


class DeclarationInvalid(ValueError):
    """The declaration is present and fails validation; carries every issue found."""

    def __init__(self, issues: Sequence[ValidationIssue]) -> None:
        self.issues: tuple[ValidationIssue, ...] = tuple(issues)
        codes = ", ".join(sorted({issue.code for issue in self.issues}))
        super().__init__(f"the tracker result declaration failed validation: {codes}")


def canonical_json(value: object) -> bytes:
    """The canonical JSON bytes of ``value``: keys sorted, UTF-8, no insignificant whitespace.

    ``allow_nan=False`` so a non-finite float raises instead of producing text
    no strict JSON reader accepts.
    """

    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def declaration_digest(value: Mapping[str, object]) -> str:
    """The SHA-256 hex digest of the declaration object's canonical JSON."""

    return hashlib.sha256(canonical_json(value)).hexdigest()


def _location(field: str | None = None) -> str:
    return RESULT_DECLARATION_FILE if field is None else f"{RESULT_DECLARATION_FILE} ({field})"


def _issue(code: str, message: str, field: str | None = None) -> ValidationIssue:
    return ValidationIssue(code=code, message=message, location=_location(field))


def _refuse_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    obj: dict[str, object] = {}
    for key, value in pairs:
        if key in obj:
            raise ValueError("duplicate key")
        obj[key] = value
    return obj


def _refuse_constant(_name: str) -> object:
    raise ValueError("non-finite number")


def _decode(data: bytes) -> object:
    """Strict UTF-8 then strict JSON, or ``DeclarationInvalid``."""

    if len(data) > MAX_DECLARATION_BYTES:
        raise DeclarationInvalid(
            [
                _issue(
                    _CODE_UNREADABLE,
                    f"the result declaration exceeds {MAX_DECLARATION_BYTES} bytes",
                )
            ]
        )
    try:
        text = data.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise DeclarationInvalid(
            [_issue(_CODE_UNREADABLE, "the result declaration is not valid UTF-8")]
        ) from None
    try:
        value = json.loads(
            text, object_pairs_hook=_refuse_duplicate_keys, parse_constant=_refuse_constant
        )
    except (ValueError, RecursionError):
        # `JSONDecodeError` is a `ValueError`, as are the two hooks' refusals;
        # nesting deep enough to exhaust the stack is refused the same way.
        raise DeclarationInvalid(
            [
                _issue(
                    _CODE_INVALID_JSON,
                    "the result declaration is not strict JSON (one value per key, "
                    "no NaN or Infinity)",
                )
            ]
        ) from None
    return value


def _results_issues(value: object) -> tuple[list[ValidationIssue], list[ResultKind]]:
    """Issues for ``results`` and the kinds that were valid, in declared order."""

    if not isinstance(value, list) or not value:
        return [
            _issue(
                _CODE_RESULTS,
                "results must be a non-empty list drawn from ticket and findings",
                "results",
            )
        ], []
    issues: list[ValidationIssue] = []
    kinds: list[ResultKind] = []
    allowed = {kind.value for kind in ResultKind}
    for index, item in enumerate(value):
        field = f"results[{index}]"
        if not isinstance(item, str) or item not in allowed:
            issues.append(_issue(_CODE_RESULTS, "a result kind must be ticket or findings", field))
            continue
        kind = ResultKind(item)
        if kind in kinds:
            issues.append(_issue(_CODE_RESULTS, "a result kind is declared twice", field))
            continue
        kinds.append(kind)
    if ResultKind.FINDINGS not in kinds:
        issues.append(_issue(_CODE_RESULTS, "results must include findings", "results"))
    return issues, kinds


def _label_issues(obj: Mapping[str, object], ticket_declared: bool | None) -> list[ValidationIssue]:
    """Issues for ``implementation_label``.

    ``ticket_declared`` is None when ``results`` itself is unusable; then only
    the label's own shape is checked, since whether it is required is unknown.
    """

    present = "implementation_label" in obj
    if ticket_declared is True and not present:
        return [
            _issue(
                _CODE_IMPLEMENTATION_LABEL,
                "implementation_label is required when results contains ticket",
                "implementation_label",
            )
        ]
    if not present:
        return []
    if ticket_declared is False:
        return [
            _issue(
                _CODE_IMPLEMENTATION_LABEL,
                "implementation_label is allowed only when results contains ticket",
                "implementation_label",
            )
        ]
    label = obj["implementation_label"]
    if not isinstance(label, str) or not 1 <= len(label) <= MAX_IMPLEMENTATION_LABEL:
        return [
            _issue(
                _CODE_IMPLEMENTATION_LABEL,
                f"implementation_label must be a string of 1 to {MAX_IMPLEMENTATION_LABEL} "
                "characters",
                "implementation_label",
            )
        ]
    return []


def parse_declaration(data: bytes) -> ResultDeclaration:
    """Parse and validate the declaration file's bytes, or raise ``DeclarationInvalid``.

    Collects every issue the object has rather than stopping at the first, so
    an author sees the whole list in one upload.
    """

    obj = _decode(data)
    if not isinstance(obj, dict):
        raise DeclarationInvalid(
            [_issue(_CODE_NOT_OBJECT, "the result declaration must be a JSON object")]
        )

    issues: list[ValidationIssue] = [
        _issue(_CODE_UNKNOWN_FIELD, "unknown field in the result declaration", key)
        for key in obj
        if key not in _KNOWN_FIELDS
    ]
    issues.extend(
        _issue(_CODE_MISSING_FIELD, "a required field is missing", field)
        for field in _REQUIRED_FIELDS
        if field not in obj
    )

    if "schema_version" in obj:
        version = obj["schema_version"]
        # `type(...) is int`: JSON `true` is a bool (an int subclass) and `1.0`
        # is a float equal to 1, and neither is the integer 1.
        if not (type(version) is int and version == SCHEMA_VERSION):
            issues.append(
                _issue(
                    _CODE_SCHEMA_VERSION,
                    f"schema_version must be the integer {SCHEMA_VERSION}",
                    "schema_version",
                )
            )

    if "contract" in obj:
        contract = obj["contract"]
        if not (isinstance(contract, str) and contract == CONTRACT):
            issues.append(_issue(_CODE_CONTRACT, f"contract must be {CONTRACT}", "contract"))

    kinds: list[ResultKind] = []
    ticket_declared: bool | None = None
    if "results" in obj:
        result_issues, kinds = _results_issues(obj["results"])
        issues.extend(result_issues)
        if isinstance(obj["results"], list):
            ticket_declared = ResultKind.TICKET in kinds
    issues.extend(_label_issues(obj, ticket_declared))

    if issues:
        raise DeclarationInvalid(issues)

    # Every value is now a str, the integer 1 or a list of str, so the
    # canonical form cannot meet a float.
    label = obj.get("implementation_label")
    return ResultDeclaration(
        schema_version=SCHEMA_VERSION,
        results=tuple(kinds),
        implementation_label=label if isinstance(label, str) else None,
        digest=declaration_digest(obj),
    )


def read_declaration(root: Path) -> ResultDeclaration | None:
    """The bundle's declaration under ``root`` (a bundle root), or None when absent.

    Absent means no path entry at all. A directory, a symlink, an unreadable
    file or one over ``MAX_DECLARATION_BYTES`` is refused, never read as "no
    declaration". Raises ``DeclarationInvalid``.
    """

    path = root / RESULT_DECLARATION_FILE
    if not path.is_symlink() and not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise DeclarationInvalid(
            [_issue(_CODE_UNREADABLE, "the result declaration must be a regular file")]
        )
    try:
        with path.open("rb") as handle:
            # One byte past the cap is enough to know the cap was exceeded.
            data = handle.read(MAX_DECLARATION_BYTES + 1)
    except OSError:
        raise DeclarationInvalid(
            [_issue(_CODE_UNREADABLE, "the result declaration could not be read")]
        ) from None
    return parse_declaration(data)


def declaration_issues(root: Path) -> list[ValidationIssue]:
    """Validation issues for the declaration under ``root``; empty when absent or valid."""

    try:
        read_declaration(root)
    except DeclarationInvalid as exc:
        return list(exc.issues)
    return []
