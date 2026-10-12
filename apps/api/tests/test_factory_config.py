"""Boot-time configuration contract for the factory (#2574, #4397).

The factory is configured by ``FACTORY_ENABLED``, ``FACTORY_INTAKE``,
``FACTORY_CI_WAIT_S``, ``FACTORY_RECONCILE_*`` and one typed ``CURIE_FORGES``
document. ``CURIE_FORGES`` is read through the file loader, so credentials are
files and an inline secret is refused. Each refusal below has a neighbouring
configuration shown to boot.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from curie_api.config import Settings
from forge_fakes.github import forges_document, forges_env_json
from pydantic import ValidationError

_FACTORY_ENV = (
    "FACTORY_ENABLED",
    "FACTORY_INTAKE",
    "FACTORY_CI_WAIT_S",
    "FACTORY_RECONCILE_INTERVAL_S",
    "FACTORY_RECONCILE_GRACE_S",
    "CURIE_FORGES",
    "GITHUB_API_URL",
    "GITHUB_APP_ID",
    "GITHUB_APP_PRIVATE_KEY",
    "GITHUB_WEBHOOK_SECRET",
    "GITHUB_REPO_ALLOWLIST",
    "ENVIRONMENT",
)

KEY = "-----BEGIN KEY-----\nfixture-not-a-real-key\n-----END KEY-----"


@pytest.fixture(autouse=True)
def _clear_factory_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _FACTORY_ENV:
        monkeypatch.delenv(name, raising=False)


def _enabled(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "factory_enabled": True,
        "forges": forges_document(),
        "github_webhook_secret": "example-factory-hmac-secret",
        "github_repo_allowlist": ("acme-corp/*",),
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def _refused(*offenders: str, **overrides: Any) -> str:
    with pytest.raises(ValidationError) as refused:
        _enabled(**overrides)
    message = str(refused.value)
    for offender in offenders:
        assert offender in message, message
    return message


# Defaults ---------------------------------------------------------------------------


def test_the_factory_defaults_off_with_no_forges() -> None:
    settings = Settings(_env_file=None)

    assert settings.factory_enabled is False
    assert settings.factory_intake == "poll"
    assert settings.factory_ci_wait_s == 1200
    assert settings.factory_reconcile_interval_s == 300.0
    assert settings.factory_reconcile_grace_s == 300.0
    assert (settings.forges.code_hosts, settings.forges.trackers) == ((), ())


def test_a_disabled_factory_needs_no_forges_or_credentials() -> None:
    settings = Settings(_env_file=None, factory_enabled=False, github_app_id="")
    assert settings.factory_enabled is False


# Enabled ------------------------------------------------------------------------------


def test_an_enabled_factory_with_a_github_tracker_code_host_and_allowlist_boots() -> None:
    settings = _enabled()

    (tracker,) = settings.forges.trackers
    assert (tracker.kind, tracker.label, tracker.mention) == ("github", "factory", "curie")
    assert settings.forges.code_hosts[0].kind == "github"


def test_an_enabled_factory_without_a_github_tracker_is_refused() -> None:
    document = forges_document()
    document["trackers"] = []
    _refused("FACTORY_ENABLED", "CURIE_FORGES", forges=document)


def test_an_enabled_factory_without_a_github_code_host_is_refused() -> None:
    document = forges_document()
    document["code_hosts"] = []
    _refused("FACTORY_ENABLED", "CURIE_FORGES", forges=document)


def test_an_enabled_factory_without_an_allowlist_is_refused() -> None:
    _refused("GITHUB_REPO_ALLOWLIST", github_repo_allowlist=())


def test_an_invalid_label_or_mention_is_refused_by_the_tracker_entry() -> None:
    _refused("label", forges=forges_document(label="factory label"))
    _refused("mention", forges=forges_document(mention="curie[bot]"))


# Webhook secret, keyed on FACTORY_INTAKE ----------------------------------------------


def test_poll_intake_accepts_an_empty_webhook_secret() -> None:
    settings = _enabled(github_webhook_secret="", factory_intake="poll")
    assert (settings.github_webhook_secret, settings.factory_intake) == ("", "poll")


def test_poll_intake_refuses_the_published_webhook_secret() -> None:
    _refused(
        "GITHUB_WEBHOOK_SECRET", factory_intake="poll", github_webhook_secret="dev-webhook-secret"
    )


@pytest.mark.parametrize("secret", ["", "dev-webhook-secret"])
def test_webhook_intake_refuses_an_empty_or_published_secret(secret: str) -> None:
    _refused("GITHUB_WEBHOOK_SECRET", factory_intake="webhook", github_webhook_secret=secret)


def test_webhook_intake_accepts_a_configured_secret() -> None:
    assert _enabled(factory_intake="webhook").factory_intake == "webhook"


# The card base url ----------------------------------------------------------------------


def test_an_http_localhost_card_base_url_is_refused_outside_dev() -> None:
    _refused(
        "factory.cardBaseUrl",
        environment="staging",
        forges=forges_document(card_base_url="http://localhost:8000"),
    )


def test_an_http_localhost_card_base_url_is_accepted_in_dev() -> None:
    settings = _enabled(
        environment="dev", forges=forges_document(card_base_url="http://localhost:8000")
    )
    assert settings.forges.card_base_url == "http://localhost:8000"


def test_an_https_card_base_url_is_accepted_outside_dev() -> None:
    settings = _enabled(
        environment="staging", forges=forges_document(card_base_url="https://curie.example.com/")
    )
    assert settings.forges.card_base_url == "https://curie.example.com"


# One GitHub host ------------------------------------------------------------------------


def test_a_github_code_host_on_another_host_than_github_api_url_is_refused() -> None:
    document = forges_document(api_url="https://ghe.example.com/api/v3")
    _refused("CURIE_FORGES", "GITHUB_API_URL", forges=document)


def test_a_github_code_host_on_the_github_api_url_host_boots() -> None:
    api = "https://ghe.example.com/api/v3"
    settings = _enabled(github_api_url=api, forges=forges_document(api_url=api))
    assert settings.forges.code_hosts[0].host == "ghe.example.com"


# Bounded intervals -------------------------------------------------------------------------

_RECONCILE_SETTINGS = ("factory_reconcile_interval_s", "factory_reconcile_grace_s")
_RECONCILE_ENV = {
    "factory_reconcile_interval_s": "FACTORY_RECONCILE_INTERVAL_S",
    "factory_reconcile_grace_s": "FACTORY_RECONCILE_GRACE_S",
}


@pytest.mark.parametrize("field", _RECONCILE_SETTINGS)
def test_factory_reconcile_settings_accept_zero(field: str) -> None:
    assert getattr(Settings(_env_file=None, **{field: 0}), field) == 0.0


@pytest.mark.parametrize("field", _RECONCILE_SETTINGS)
@pytest.mark.parametrize("value", [-5, float("nan"), float("inf")])
def test_factory_reconcile_settings_refuse_negative_or_non_finite(field: str, value: float) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field: value})


@pytest.mark.parametrize("field", _RECONCILE_SETTINGS)
@pytest.mark.parametrize("value", ["-5", "nan", "inf"])
def test_factory_reconcile_settings_refuse_the_same_via_the_environment(
    field: str, value: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(_RECONCILE_ENV[field], value)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


@pytest.mark.parametrize("value", ["0", "10801"])
def test_factory_ci_wait_is_refused_outside_its_bounds(
    value: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FACTORY_CI_WAIT_S", value)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_factory_ci_wait_is_read_from_its_env_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FACTORY_CI_WAIT_S", "10800")
    assert Settings(_env_file=None).factory_ci_wait_s == 10800


# CURIE_FORGES from the real environment --------------------------------------------------


def _set_factory_env(monkeypatch: pytest.MonkeyPatch, forges_json: str) -> None:
    monkeypatch.setenv("FACTORY_ENABLED", "true")
    monkeypatch.setenv("FACTORY_INTAKE", "poll")
    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", "example-factory-hmac-secret")
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    monkeypatch.setenv("CURIE_FORGES", forges_json)


def test_curie_forges_env_is_loaded_through_the_file_loader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R1 finding 1: without NoDecode the settings source would JSON-decode the
    env value itself and the file loader, with its inline-secret refusal, would
    never run."""

    _set_factory_env(monkeypatch, forges_env_json(forges_document(private_key=KEY), tmp_path))

    settings = Settings(_env_file=None)

    assert settings.factory_enabled is True
    for entry in (*settings.forges.code_hosts, *settings.forges.trackers):
        assert entry.credential.private_key.get_secret_value() == KEY


def test_an_inline_private_key_in_curie_forges_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_factory_env(monkeypatch, json.dumps(forges_document(private_key="inline-must-not-echo")))

    with pytest.raises(ValidationError) as refused:
        Settings(_env_file=None)
    assert "inline-must-not-echo" not in str(refused.value)
    assert "github" in str(refused.value)


def test_a_github_static_token_in_curie_forges_is_refused_naming_the_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    document = forges_document()
    document["trackers"][0]["credential"] = {"type": "static_token", "token": "ghp_fixture"}
    _set_factory_env(monkeypatch, forges_env_json(document, tmp_path))

    with pytest.raises(ValidationError) as refused:
        Settings(_env_file=None)
    assert "github" in str(refused.value)
    assert "ghp_fixture" not in str(refused.value)


@pytest.mark.parametrize(
    ("section", "entry"),
    [
        (
            "code_hosts",
            {
                "name": "corp-gitlab",
                "kind": "gitlab",
                "host": "gitlab.example",
                "api_url": "https://gitlab.example/api/v4",
                "credential": {"type": "static_token", "token": "glpat-fixture"},
            },
        ),
    ],
)
def test_a_reserved_kind_in_curie_forges_is_refused_at_load_naming_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, section: str, entry: dict[str, Any]
) -> None:
    """#4397 AC 1: the refusal is at config load, not at first use."""

    document = forges_document()
    document[section].append(entry)
    _set_factory_env(monkeypatch, forges_env_json(document, tmp_path))

    with pytest.raises(ValidationError) as refused:
        Settings(_env_file=None)
    assert entry["kind"] in str(refused.value)
    assert entry["name"] in str(refused.value)
    assert "not installed" in str(refused.value)


def test_an_empty_curie_forges_env_is_an_empty_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CURIE_FORGES", "")
    assert Settings(_env_file=None).forges.trackers == ()
