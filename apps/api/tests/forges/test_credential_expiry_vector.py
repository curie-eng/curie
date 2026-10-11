"""Forge credential expiry against the shared vector (#4397 AC 7, ADR 0197).

`tests/vectors/forge-credential-expiry.json` pins the known, unknown and none
classification and the 14-day warning text for both the API
(`credential_providers.expiry_for`, `expiry_warning`) and the CLI
(`curie::forge_expiry`, which `curie cluster status` and `curie doctor` read
from the release values). The vector holds credentials in the chart's recorded
camelCase shape with no secret material; this test adds dummy key material and
validates them as `CredentialConfig`. The registry case proves
`ForgeRegistry.credential_reports()` uses the same pure function.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from curie_api.forges.config import CredentialConfig, ForgesConfig
from curie_api.forges.credential_providers import expiry_for, expiry_warning
from curie_api.forges.registry import build_registry
from pydantic import TypeAdapter

VECTOR_PATH = Path(__file__).resolve().parents[4] / "tests" / "vectors" / "forge-credential-expiry.json"
VECTOR: dict[str, Any] = json.loads(VECTOR_PATH.read_text())
WARN_BEFORE = timedelta(days=VECTOR["warn_before_days"])
CREDENTIAL = TypeAdapter(CredentialConfig)
# The chart's Secret reference keys, which never reach CredentialConfig.
SECRET_REFERENCES = {"existingSecret", "existingSecretKey"}
SECRET_MATERIAL = {
    "github_app": "private_key",
    "oauth_client_credentials": "client_secret",
    "static_token": "token",
}


@pytest.fixture(scope="module")
def private_key() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


def _snake(key: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", key).lower()


def _config_document(recorded: dict[str, Any], private_key: str) -> dict[str, Any]:
    document = {_snake(k): v for k, v in recorded.items() if k not in SECRET_REFERENCES}
    material = SECRET_MATERIAL[recorded["type"]]
    document[material] = private_key if material == "private_key" else "dummy-secret-value"
    return document


def _credential(recorded: dict[str, Any], private_key: str) -> Any:
    return CREDENTIAL.validate_python(_config_document(recorded, private_key))


def _at(text: str | None) -> datetime | None:
    return None if text is None else datetime.fromisoformat(text.replace("Z", "+00:00"))


def test_the_vector_covers_every_credential_type_and_every_expiry() -> None:
    cases = VECTOR["classify"]
    assert {case["credential"]["type"] for case in cases} == set(SECRET_MATERIAL)
    assert {case["expiry"] for case in cases} == {"known", "unknown", "none"}
    names = {case["name"] for case in VECTOR["warn"]}
    assert {"fifteen_days_left", "exactly_fourteen_days_left", "already_past"} <= names


@pytest.mark.parametrize("case", VECTOR["classify"], ids=lambda case: case["name"])
def test_expiry_for_matches_the_vector(case: dict[str, Any], private_key: str) -> None:
    report = expiry_for(_credential(case["credential"], private_key))
    assert str(report.expiry) == case["expiry"]
    assert report.expires_at == _at(case["expires_at"])


@pytest.mark.parametrize("case", VECTOR["warn"], ids=lambda case: case["name"])
def test_expiry_warning_matches_the_vector(case: dict[str, Any], private_key: str) -> None:
    report = expiry_for(_credential(case["credential"], private_key))
    now = _at(case["now"])
    assert now is not None and now.utcoffset() == timedelta(0)
    assert expiry_warning(report, now, WARN_BEFORE) == case["warning"]


def test_every_report_line_matches_expiry_for(private_key: str) -> None:
    report = VECTOR["report"]
    now = _at(report["now"])
    assert now is not None
    entries = [
        ("code_host", entry) for entry in report["factory"]["codeHosts"]
    ] + [("tracker", entry) for entry in report["factory"]["trackers"]]
    got = []
    for section, entry in entries:
        expiry = expiry_for(_credential(entry["credential"], private_key))
        got.append(
            {
                "section": section,
                "name": entry["name"],
                "type": entry["credential"]["type"],
                "expiry": str(expiry.expiry),
                "expires_at": expiry.expires_at,
                "warning": expiry_warning(expiry, now, WARN_BEFORE),
            }
        )
    want = [{**line, "expires_at": _at(line["expires_at"])} for line in report["lines"]]
    assert got == want


def test_the_registry_reports_the_same_expiry_for_its_github_entries(private_key: str) -> None:
    report = VECTOR["report"]
    sections = {"codeHosts": "code_hosts", "trackers": "trackers"}
    document: dict[str, Any] = {}
    for chart_key, config_key in sections.items():
        document[config_key] = []
        for entry in report["factory"][chart_key]:
            if entry["kind"] != "github":
                continue
            converted = {
                _snake(k): v for k, v in entry.items() if k != "credential"
            }
            converted["credential"] = _config_document(entry["credential"], private_key)
            document[config_key].append(converted)
    registry = build_registry(ForgesConfig.model_validate(document))
    got = [
        (r.section, r.name, r.credential_type, str(r.expiry.expiry), r.expiry.expires_at)
        for r in registry.credential_reports()
    ]
    want = [
        (line["section"], line["name"], line["type"], line["expiry"], _at(line["expires_at"]))
        for line in report["lines"]
        if line["type"] == "github_app"
    ]
    assert got == want
