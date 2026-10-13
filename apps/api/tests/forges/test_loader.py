"""``CURIE_FORGES`` is loaded through the file loader (#4397, plan commit 2).

Credentials reach the API as mounted files: each credential names a
``*_file`` path, the loader reads it, and an inline secret in the env JSON is
refused. A GitHub entry must use a GitHub App (the tracker's marked-comment
identity needs the App id). A kind this build does not install is refused at
load. Every refusal names the entry and never the secret.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from curie_api.forges import registry as forge_registry
from curie_api.forges.loader import load_forges_json
from forge_fakes import config_samples as samples

KEY = "-----BEGIN KEY-----\nfixture-not-a-real-key\n-----END KEY-----"


def _key_file(tmp_path: Path, content: str = KEY, name: str = "private_key") -> Path:
    path = tmp_path / name
    path.write_text(content)
    return path


def _github_document(key_file: Path) -> dict[str, Any]:
    credential = {"type": "github_app", "app_id": "123456", "private_key_file": str(key_file)}
    return {
        "code_hosts": [
            {
                "name": "github",
                "kind": "github",
                "host": "github.com",
                "api_url": "https://api.github.com",
                "credential": dict(credential),
            }
        ],
        "trackers": [
            {
                "name": "github-issues",
                "kind": "github",
                "host": "github.com",
                "api_url": "https://api.github.com",
                "credential": dict(credential),
                "label": "curie",
                "mention": "curie-bot",
            }
        ],
        "card_base_url": "https://curie.example.com",
    }


def _refused(document: dict[str, Any], *needles: str) -> str:
    with pytest.raises(ValueError) as refused:
        load_forges_json(json.dumps(document))
    message = str(refused.value)
    for needle in needles:
        assert needle in message, message
    return message


def test_github_app_config_loads(tmp_path: Path) -> None:
    config = load_forges_json(json.dumps(_github_document(_key_file(tmp_path))))

    (code_host,) = config.code_hosts
    (tracker,) = config.trackers
    assert code_host.credential.private_key.get_secret_value() == KEY
    assert tracker.credential.private_key.get_secret_value() == KEY
    assert (tracker.label, tracker.mention) == ("curie", "curie-bot")
    assert config.card_base_url == "https://curie.example.com"


def test_an_empty_string_is_an_empty_config() -> None:
    config = load_forges_json("")
    assert (config.code_hosts, config.trackers, config.bindings) == ((), (), ())


def test_a_trailing_newline_in_the_key_file_is_not_part_of_the_key(tmp_path: Path) -> None:
    config = load_forges_json(json.dumps(_github_document(_key_file(tmp_path, KEY + "\n"))))
    assert config.code_hosts[0].credential.private_key.get_secret_value().strip() == KEY


def test_a_missing_key_file_is_refused_naming_the_entry_and_path(tmp_path: Path) -> None:
    missing = tmp_path / "absent" / "private_key"
    _refused(_github_document(missing), "github", str(missing))


def test_an_empty_key_file_is_refused_naming_the_entry(tmp_path: Path) -> None:
    empty = _key_file(tmp_path, "  \n")
    _refused(_github_document(empty), "github", str(empty))


@pytest.mark.parametrize("section", ["code_hosts", "trackers"])
def test_an_inline_private_key_is_refused_and_never_echoed(tmp_path: Path, section: str) -> None:
    document = _github_document(_key_file(tmp_path))
    credential = document[section][0]["credential"]
    del credential["private_key_file"]
    credential["private_key"] = "inline-secret-must-not-echo"

    message = _refused(document, document[section][0]["name"])
    assert "inline-secret-must-not-echo" not in message


def test_an_inline_client_secret_or_token_is_refused() -> None:
    jira = samples.jira_tracker("corp-jira")
    _refused({"trackers": [jira]}, "corp-jira")
    gitlab = samples.gitlab_code_host("corp-gitlab")
    message = _refused({"code_hosts": [gitlab]}, "corp-gitlab")
    assert "glpat-example" not in message


def test_an_unknown_field_is_refused(tmp_path: Path) -> None:
    document = _github_document(_key_file(tmp_path))
    document["code_hosts"][0]["egress"] = []
    _refused(document, "egress")


def test_a_key_file_content_never_appears_in_an_error(tmp_path: Path) -> None:
    document = _github_document(_key_file(tmp_path, "secret-content-must-not-echo"))
    document["trackers"][0]["mention"] = "not a login"
    message = _refused(document, "mention")
    assert "secret-content-must-not-echo" not in message


def _as_files(tmp_path: Path, entry: dict[str, Any]) -> dict[str, Any]:
    credential = entry["credential"]
    for inline, field in (("token", "token_file"), ("client_secret", "client_secret_file")):
        if inline in credential:
            credential[field] = str(_key_file(tmp_path, credential.pop(inline), inline))
    return entry


def test_a_not_installed_kind_is_refused_at_load_naming_kind_and_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No configurable kind is reserved since #4399 installed GitLab, so the
    refusal is proven on gitlab reserved for this test only."""

    monkeypatch.delitem(forge_registry._KINDS, "gitlab")
    forge_registry.register_not_installed("gitlab")
    entry = _as_files(tmp_path, samples.gitlab_code_host("corp-gitlab"))
    _refused({"code_hosts": [entry]}, entry["kind"], entry["name"], "not installed")


def test_an_installed_gitlab_code_host_loads(tmp_path: Path) -> None:
    """Liveness for the refusal above: the shipped build installs GitLab."""

    entry = _as_files(tmp_path, samples.gitlab_code_host("corp-gitlab"))
    (code_host,) = load_forges_json(json.dumps({"code_hosts": [entry]})).code_hosts
    assert (code_host.name, code_host.kind) == ("corp-gitlab", "gitlab")


@pytest.mark.parametrize("section", ["code_hosts", "trackers"])
def test_github_static_token_is_refused_naming_the_entry(tmp_path: Path, section: str) -> None:
    document = _github_document(_key_file(tmp_path))
    name = document[section][0]["name"]
    document[section][0]["credential"] = {
        "type": "static_token",
        "token_file": str(_key_file(tmp_path, "ghp_fixture", "token")),
    }
    message = _refused(document, name, "github_app")
    assert "ghp_fixture" not in message


def test_a_jira_static_token_entry_loads_its_token_from_the_file(tmp_path: Path) -> None:
    """#4358 decisions 2 and 22: the Jira API token reaches the API as a mounted
    file, beside the bot's email as the basic auth username."""

    token_file = _key_file(tmp_path, "jira-token-from-file\n", "token")
    tracker = {
        "name": "corp-site",
        "kind": "jira_cloud",
        "host": "acme.atlassian.example",
        "api_url": "https://acme.atlassian.example",
        "credential": {
            "type": "static_token",
            "token_file": str(token_file),
            "username": "factory-bot@example.com",
        },
        "label": "curie",
        "mention": "712020:bbbbbbbb-0000-4000-8000-000000000002",
    }

    config = load_forges_json(json.dumps({"trackers": [tracker]}))

    (loaded,) = config.trackers
    assert loaded.kind == "jira_cloud"
    assert loaded.credential.username == "factory-bot@example.com"
    assert loaded.credential.token.get_secret_value().strip() == "jira-token-from-file"
