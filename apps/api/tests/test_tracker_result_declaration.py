"""The tracker result declaration, ``workflow/result.json`` (ADR 0277 decision 1, #5585).

Pure unit tests: no database, no object store. The parser runs against the
shared vector file, and every expected digest is checked twice, once against
the parser and once against an independent derivation, so a parser that
hashed a non-canonical form fails here. The ``validate_archive`` tests at the
end prove the ingestion refusal and its ordering without a database; the
REST and git-flow paths that need Postgres live in ``test_bundles.py`` and
``test_gitflow_integration.py``.
"""

import hashlib
import io
import json
import os
import tarfile
import zipfile
from pathlib import Path
from typing import Any

import pytest
from curie_api import deploy
from curie_api.config import get_settings
from curie_api.tracker_result_declaration import (
    MAX_DECLARATION_BYTES,
    RESULT_DECLARATION_FILE,
    DeclarationInvalid,
    ResultDeclaration,
    ResultKind,
    canonical_json,
    declaration_digest,
    declaration_issues,
    parse_declaration,
    read_declaration,
)

_VECTOR = (
    Path(__file__).resolve().parents[3] / "tests" / "vectors" / "tracker-result-declaration.json"
)
VECTORS: dict[str, Any] = json.loads(_VECTOR.read_text(encoding="utf-8"))

_VALID_KEYS = {"name", "text", "digest", "schema_version", "results", "implementation_label"}
_INVALID_KEYS = {"name", "text", "codes"}

# The seven invalid inputs acceptance criterion 1 names.
_CRITERION_INVALID = {
    "missing_schema_version",
    "wrong_contract",
    "empty_results",
    "results_without_findings",
    "ticket_without_implementation_label",
    "unknown_field",
    "non_object_file",
}


def _independent_digest(text: str) -> str:
    """The digest by a derivation that shares no code with the parser."""

    canonical = json.dumps(
        json.loads(text), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _valid(name: str) -> dict[str, Any]:
    return next(case for case in VECTORS["valid"] if case["name"] == name)


def _codes(exc: DeclarationInvalid) -> list[str]:
    return [issue.code for issue in exc.issues]


# --- the vector file --------------------------------------------------------


def test_vector_file_has_only_known_keys() -> None:
    assert set(VECTORS) == {"comment", "path", "canonical", "valid", "invalid"}
    assert VECTORS["path"] == RESULT_DECLARATION_FILE
    for case in VECTORS["valid"]:
        assert set(case) == _VALID_KEYS, case["name"]
    for case in VECTORS["invalid"]:
        assert set(case) == _INVALID_KEYS, case["name"]
    names = [case["name"] for case in [*VECTORS["valid"], *VECTORS["invalid"]]]
    assert len(names) == len(set(names))


def test_vector_file_holds_every_invalid_case_the_criterion_names() -> None:
    assert _CRITERION_INVALID <= {case["name"] for case in VECTORS["invalid"]}


# --- criterion 1: valid vectors --------------------------------------------


@pytest.mark.parametrize("case", VECTORS["valid"], ids=lambda case: case["name"])
def test_valid_vector_parses_with_its_digest(case: dict[str, Any]) -> None:
    parsed = parse_declaration(case["text"].encode("utf-8"))

    assert isinstance(parsed, ResultDeclaration)
    assert parsed.schema_version == case["schema_version"]
    assert [kind.value for kind in parsed.results] == case["results"]
    assert all(isinstance(kind, ResultKind) for kind in parsed.results)
    assert parsed.implementation_label == case["implementation_label"]
    assert parsed.digest == case["digest"]
    assert _independent_digest(case["text"]) == case["digest"]


def test_key_order_and_whitespace_do_not_change_the_digest() -> None:
    plain = _valid("ticket_and_findings")
    reordered = _valid("ticket_and_findings_keys_reordered")
    assert plain["text"] != reordered["text"]

    assert (
        parse_declaration(plain["text"].encode()).digest
        == parse_declaration(reordered["text"].encode()).digest
        == plain["digest"]
    )


def test_list_order_changes_the_digest() -> None:
    forward = {
        "schema_version": 1,
        "contract": "tracker_result",
        "results": ["ticket", "findings"],
        "implementation_label": "x",
    }
    backward = {**forward, "results": ["findings", "ticket"]}

    assert declaration_digest(forward) != declaration_digest(backward)


def test_digest_is_the_sha256_of_the_literal_canonical_text() -> None:
    # Written out by hand, so neither side of the comparison runs a JSON encoder.
    canonical = '{"contract":"tracker_result","results":["findings"],"schema_version":1}'
    expected = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    assert _valid("findings_only")["digest"] == expected
    assert parse_declaration(_valid("findings_only")["text"].encode()).digest == expected


def test_non_ascii_label_hashes_as_utf8_not_escapes() -> None:
    case = _valid("non_ascii_label_findings_first")
    canonical = (
        '{"contract":"tracker_result","implementation_label":"implementación",'
        '"results":["findings","ticket"],"schema_version":1}'
    )

    assert case["digest"] == hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    assert parse_declaration(case["text"].encode("utf-8")).digest == case["digest"]


# --- criterion 1: invalid vectors ------------------------------------------


@pytest.mark.parametrize("case", VECTORS["invalid"], ids=lambda case: case["name"])
def test_invalid_vector_is_refused_with_its_codes(case: dict[str, Any]) -> None:
    with pytest.raises(DeclarationInvalid) as raised:
        parse_declaration(case["text"].encode("utf-8"))

    assert _codes(raised.value) == case["codes"]
    assert isinstance(raised.value, ValueError)


def test_every_unknown_field_is_its_own_issue_located_by_key() -> None:
    text = json.dumps(
        {
            "schema_version": 1,
            "contract": "tracker_result",
            "results": ["findings"],
            "repository": "octo/other",
            "labels": ["x"],
        }
    )

    with pytest.raises(DeclarationInvalid) as raised:
        parse_declaration(text.encode())

    assert [(i.code, i.location) for i in raised.value.issues] == [
        ("tracker_result.unknown_field", "workflow/result.json (repository)"),
        ("tracker_result.unknown_field", "workflow/result.json (labels)"),
    ]


def test_a_bad_result_item_is_located_by_index() -> None:
    text = '{"schema_version": 1, "contract": "tracker_result", "results": ["findings", 3]}'

    with pytest.raises(DeclarationInvalid) as raised:
        parse_declaration(text.encode())

    assert [(i.code, i.location) for i in raised.value.issues] == [
        ("tracker_result.results", "workflow/result.json (results[1])"),
    ]


def test_all_issues_are_reported_together() -> None:
    text = '{"contract": "other", "results": ["ticket"], "extra": 1}'

    with pytest.raises(DeclarationInvalid) as raised:
        parse_declaration(text.encode())

    assert _codes(raised.value) == [
        "tracker_result.unknown_field",
        "tracker_result.missing_field",
        "tracker_result.contract",
        "tracker_result.results",
        "tracker_result.implementation_label",
    ]


def test_messages_never_echo_submitted_values() -> None:
    sentinel = "SENTINEL-7f3a"
    text = json.dumps(
        {
            "schema_version": sentinel,
            "contract": sentinel,
            "results": [sentinel],
            "implementation_label": sentinel * 10,
            "surplus": sentinel,
        }
    )

    with pytest.raises(DeclarationInvalid) as raised:
        parse_declaration(text.encode())

    assert raised.value.issues
    for issue in raised.value.issues:
        assert sentinel not in issue.message
        assert sentinel not in issue.location
    assert sentinel not in str(raised.value)


def test_over_the_byte_cap_is_unreadable() -> None:
    valid = _valid("findings_only")["text"]
    padded = valid[:-1] + " " * (MAX_DECLARATION_BYTES - len(valid) + 1) + "}"
    assert len(padded.encode()) == MAX_DECLARATION_BYTES + 1

    with pytest.raises(DeclarationInvalid) as raised:
        parse_declaration(padded.encode())

    assert _codes(raised.value) == ["tracker_result.unreadable"]
    # Exactly at the cap still parses.
    at_cap = valid[:-1] + " " * (MAX_DECLARATION_BYTES - len(valid)) + "}"
    assert len(at_cap.encode()) == MAX_DECLARATION_BYTES
    assert parse_declaration(at_cap.encode()).digest == _valid("findings_only")["digest"]


def test_invalid_utf8_is_unreadable() -> None:
    with pytest.raises(DeclarationInvalid) as raised:
        parse_declaration(b'{"contract": "\xff"}')

    assert _codes(raised.value) == ["tracker_result.unreadable"]


@pytest.mark.parametrize("literal", ["Infinity", "-Infinity"])
def test_non_finite_literals_are_invalid_json(literal: str) -> None:
    text = f'{{"schema_version": {literal}, "contract": "tracker_result", "results": ["findings"]}}'

    with pytest.raises(DeclarationInvalid) as raised:
        parse_declaration(text.encode())

    assert _codes(raised.value) == ["tracker_result.invalid_json"]


# --- reading from a bundle root --------------------------------------------


def _write(root: Path, content: bytes) -> Path:
    path = root / RESULT_DECLARATION_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def test_absent_file_is_no_declaration(tmp_path: Path) -> None:
    assert read_declaration(tmp_path) is None
    assert declaration_issues(tmp_path) == []
    (tmp_path / "workflow").mkdir()
    assert read_declaration(tmp_path) is None


def test_present_file_is_read(tmp_path: Path) -> None:
    case = _valid("ticket_and_findings")
    _write(tmp_path, case["text"].encode())

    declaration = read_declaration(tmp_path)

    assert declaration is not None
    assert declaration.digest == case["digest"]
    assert declaration.results == (ResultKind.TICKET, ResultKind.FINDINGS)
    assert declaration_issues(tmp_path) == []


def test_a_malformed_file_is_reported_as_issues(tmp_path: Path) -> None:
    _write(tmp_path, b"[]")

    issues = declaration_issues(tmp_path)

    assert [(i.code, i.location) for i in issues] == [
        ("tracker_result.not_object", "workflow/result.json")
    ]
    with pytest.raises(DeclarationInvalid):
        read_declaration(tmp_path)


def test_a_directory_at_the_path_is_unreadable(tmp_path: Path) -> None:
    (tmp_path / RESULT_DECLARATION_FILE).mkdir(parents=True)

    assert [i.code for i in declaration_issues(tmp_path)] == ["tracker_result.unreadable"]


def test_a_symlink_at_the_path_is_unreadable(tmp_path: Path) -> None:
    target = tmp_path / "elsewhere.json"
    target.write_text(_valid("findings_only")["text"])
    (tmp_path / "workflow").mkdir()
    os.symlink(target, tmp_path / RESULT_DECLARATION_FILE)

    assert [i.code for i in declaration_issues(tmp_path)] == ["tracker_result.unreadable"]


def test_an_oversized_file_is_unreadable(tmp_path: Path) -> None:
    _write(tmp_path, b" " * (MAX_DECLARATION_BYTES + 1))

    assert [i.code for i in declaration_issues(tmp_path)] == ["tracker_result.unreadable"]


# --- canonical form ---------------------------------------------------------


def test_canonical_json_sorts_keys_and_keeps_utf8() -> None:
    assert canonical_json({"b": [2, 1], "a": "é"}) == '{"a":"é","b":[2,1]}'.encode()


def test_canonical_json_refuses_non_finite_numbers() -> None:
    with pytest.raises(ValueError):
        canonical_json({"a": float("nan")})


def test_result_kind_values() -> None:
    assert [kind.value for kind in ResultKind] == ["ticket", "findings"]


# --- ingestion refusal and its ordering (criterion 2, no database) ----------

_MANIFEST = '{"name": "demo-plugin", "version": "0.1.0"}'
_SKILL = "---\nname: alpha\ndescription: does alpha things\n---\n\n# alpha\n"


def _files(declaration: str | None) -> dict[str, str]:
    files = {".claude-plugin/plugin.json": _MANIFEST, "skills/alpha/SKILL.md": _SKILL}
    if declaration is not None:
        files[RESULT_DECLARATION_FILE] = declaration
    return files


def _tar_gz(files: dict[str, str], top: str = "demo-plugin") -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for rel, content in files.items():
            data = content.encode("utf-8")
            info = tarfile.TarInfo(f"{top}/{rel}")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for rel, content in files.items():
            zf.writestr(rel, content)
    return buf.getvalue()


@pytest.mark.parametrize("pack", [_tar_gz, _zip], ids=["wrapped_tar_gz", "flat_zip"])
def test_validate_archive_refuses_a_valid_declaration(pack: Any) -> None:
    case = _valid("ticket_and_findings")

    with pytest.raises(deploy.TrackerResultNotEnabled) as raised:
        deploy.validate_archive(pack(_files(case["text"])), get_settings())

    assert raised.value.code == "tracker_result_not_enabled"
    assert raised.value.digest == case["digest"]
    assert raised.value.schema_version == 1


@pytest.mark.parametrize("pack", [_tar_gz, _zip], ids=["wrapped_tar_gz", "flat_zip"])
def test_validate_archive_accepts_the_same_bundle_without_the_file(pack: Any) -> None:
    extension, _ = deploy.validate_archive(pack(_files(None)), get_settings())

    assert extension in {".tar.gz", ".zip"}


def test_a_malformed_declaration_is_a_validation_error_not_the_refusal() -> None:
    archive = _tar_gz(_files('{"schema_version": 2, "contract": "tracker_result"}'))

    with pytest.raises(deploy.BundleInvalid) as raised:
        deploy.validate_archive(archive, get_settings())

    codes = [error["code"] for error in raised.value.errors]
    assert codes == ["tracker_result.missing_field", "tracker_result.schema_version"]


def test_another_validation_error_wins_over_the_refusal() -> None:
    files = _files(_valid("findings_only")["text"])
    files["skills/alpha/SKILL.md"] = "# no frontmatter\n"

    with pytest.raises(deploy.BundleInvalid) as raised:
        deploy.validate_archive(_tar_gz(files), get_settings())

    codes = {error["code"] for error in raised.value.errors}
    assert "skill.frontmatter_missing" in codes
    assert not any(code.startswith("tracker_result") for code in codes)
