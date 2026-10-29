"""The open `/config` endpoint: it exposes the configurable org name.

Mirrors the `/health`-is-open test in test_auth.py: `/config` carries no
`require_api_key` dependency, so the UI can read the workspace name before the
user has supplied a key. The org name defaults to "Curie" and is overridable
via the `ORG_NAME` environment variable (the same env-driven Settings mechanism
conftest uses for DATABASE_URL).
"""

import json
from pathlib import Path
from typing import Any

import pytest
from curie_api.config import Settings, get_settings
from pydantic import ValidationError

_PUBLICATION_LABEL_VECTORS = json.loads(
    (
        Path(__file__).resolve().parents[3] / "tests/vectors/factory-publication-labels.json"
    ).read_text()
)


@pytest.mark.parametrize("vector", _PUBLICATION_LABEL_VECTORS, ids=lambda vector: vector["name"])
def test_publication_labels_shared_vectors(
    monkeypatch: pytest.MonkeyPatch, vector: dict[str, Any]
) -> None:
    monkeypatch.setenv("CURIE_FACTORY_PUBLICATION_LABELS", json.dumps(vector["value"]))

    if vector["valid"]:
        assert Settings().factory_publication_labels == vector["expected"]
    else:
        with pytest.raises(ValidationError) as raised:
            Settings()
        assert vector["key"] in str(raised.value)


def test_config_is_open(client: Any) -> None:
    # No X-API-Key header, exactly like the health probe: the endpoint is open.
    resp = client.get("/config")
    assert resp.status_code == 200
    body = resp.json()
    assert "org_name" in body
    assert isinstance(body["org_name"], str)


def test_config_ignores_a_bogus_key(client: Any) -> None:
    # A wrong key does not 401 the way /agents does -- /config is unauthenticated.
    resp = client.get("/config", headers={"X-API-Key": "wrong"})
    assert resp.status_code == 200


def test_config_defaults_to_curie(client: Any) -> None:
    # With no ORG_NAME override the documented default is surfaced.
    assert client.get("/config").json()["org_name"] == "Curie"


def test_config_reflects_the_configured_org_name(
    client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Override via the env-driven setting and clear the cache so a fresh Settings
    # reads it (the same override path conftest uses for DATABASE_URL).
    monkeypatch.setenv("ORG_NAME", "Globex Corporation")
    get_settings.cache_clear()
    try:
        assert client.get("/config").json()["org_name"] == "Globex Corporation"
    finally:
        get_settings.cache_clear()


@pytest.mark.parametrize(
    ("api_url", "html_base"),
    [
        ("https://api.github.com", "https://github.com"),
        ("https://api.github.com/", "https://github.com"),
        ("https://github.example.com/api/v3", "https://github.example.com"),
        ("https://github.example.com/api/v3/", "https://github.example.com"),
        ("https://github.example.com/forge/api/v3", "https://github.example.com/forge"),
        ("https://github.example.com:8443/forge/api/v3/", "https://github.example.com:8443/forge"),
    ],
)
def test_github_html_base_is_derived_from_the_configured_api_url(
    monkeypatch: pytest.MonkeyPatch, api_url: str, html_base: str
) -> None:
    monkeypatch.setenv("GITHUB_API_URL", api_url)

    assert Settings().github_html_base == html_base


def test_publication_labels_default_to_an_empty_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CURIE_FACTORY_PUBLICATION_LABELS", raising=False)

    assert Settings().factory_publication_labels == {}


def test_publication_labels_parse_environment_and_normalize_repository_case(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "CURIE_FACTORY_PUBLICATION_LABELS",
        json.dumps({"Acme-Corp/Acme-Bot": ["SMOKE", "x" * 50], "acme-corp/other": []}),
    )
    get_settings.cache_clear()
    try:
        assert get_settings().factory_publication_labels == {
            "acme-corp/acme-bot": ["SMOKE", "x" * 50],
            "acme-corp/other": [],
        }
    finally:
        get_settings.cache_clear()


@pytest.mark.parametrize(
    ("value", "key"),
    [
        ({"missing-slash": ["SMOKE"]}, "missing-slash"),
        ({"acme-corp/*": ["SMOKE"]}, "acme-corp/*"),
        ({"acme-corp/acme-bot/extra": ["SMOKE"]}, "acme-corp/acme-bot/extra"),
        ({"acme-corp/acme-bot": [""]}, "acme-corp/acme-bot"),
        ({"acme-corp/acme-bot": ["x" * 51]}, "acme-corp/acme-bot"),
        ({"acme-corp/acme-bot": [f"label-{i}" for i in range(11)]}, "acme-corp/acme-bot"),
        ({"acme-corp/acme-bot": "SMOKE"}, "acme-corp/acme-bot"),
        ({"acme-corp/acme-bot": [7]}, "acme-corp/acme-bot"),
        ([], "CURIE_FACTORY_PUBLICATION_LABELS"),
        ("SMOKE", "CURIE_FACTORY_PUBLICATION_LABELS"),
        (None, "CURIE_FACTORY_PUBLICATION_LABELS"),
    ],
)
def test_publication_labels_reject_invalid_environment_values(
    monkeypatch: pytest.MonkeyPatch, value: Any, key: str
) -> None:
    monkeypatch.setenv("CURIE_FACTORY_PUBLICATION_LABELS", json.dumps(value))

    with pytest.raises(ValidationError) as raised:
        Settings()

    assert key in str(raised.value)


def test_publication_labels_reject_malformed_json_with_the_setting_named(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CURIE_FACTORY_PUBLICATION_LABELS", "{")

    with pytest.raises(ValidationError, match="CURIE_FACTORY_PUBLICATION_LABELS"):
        Settings()


def test_publication_labels_validate_direct_settings_construction() -> None:
    settings = Settings(factory_publication_labels={"acme-corp/acme-bot": ["SMOKE"] * 10})

    assert settings.factory_publication_labels == {
        "acme-corp/acme-bot": ["SMOKE"] * 10
    }
    with pytest.raises(ValidationError, match="acme-corp/acme-bot"):
        Settings(factory_publication_labels={"acme-corp/acme-bot": [False]})
