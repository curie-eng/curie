"""The open `/config` endpoint: it exposes the configurable org name.

Mirrors the `/health`-is-open test in test_auth.py: `/config` carries no
`require_api_key` dependency, so the UI can read the workspace name before the
user has supplied a key. The org name defaults to "Curie" and is overridable
via the `ORG_NAME` environment variable (the same env-driven Settings mechanism
conftest uses for DATABASE_URL).
"""

from typing import Any

import pytest
from curie_api.config import Settings, get_settings


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
