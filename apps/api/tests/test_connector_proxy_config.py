"""The API's caller proxy settings (ADR-0168 decision 7).

Pure: each case builds `Settings()` from the env it set, with no fixtures.
The keys are the public halves frozen in tests/vectors/connector-caller-token.json.
"""

from __future__ import annotations

from urllib.parse import quote

import pytest
from curie_api.config import GRANT_STORE_SECRET_KEY, Settings
from plugin_format.connector_render import ConnectorProxy, GrantStore
from pydantic import ValidationError

_CURRENT = "A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg="
_PREVIOUS = "Kay64UG8yvCyLhqU000LxzYeUm0L/hLIl5S8kyKWbdc="
_IMAGE = "ghcr.io/curie-eng/curie-worker:0.0.0"
_NAMES = (
    "CURIE_CONNECTOR_CALLER_PUBLIC_KEY",
    "CURIE_CONNECTOR_CALLER_PREVIOUS_PUBLIC_KEY",
    "CURIE_CONNECTOR_PROXY_IMAGE",
    "CURIE_CONNECTOR_PROXY_IMAGE_PULL_POLICY",
    "CURIE_CONNECTOR_PROXY_IMAGE_PULL_SECRETS",
    "CURIE_CONNECTOR_GRANT_STORE_SECRET_NAME",
)


@pytest.fixture(autouse=True)
def _no_caller_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _NAMES:
        monkeypatch.delenv(name, raising=False)


# @spec ADR-0168 d7
def test_no_public_key_renders_no_proxy() -> None:
    assert Settings().connector_proxy() is None


# @spec ADR-0168 d7
def test_a_public_key_and_the_image_render_a_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CURIE_CONNECTOR_CALLER_PUBLIC_KEY", f"{_CURRENT}\n")
    monkeypatch.setenv("CURIE_CONNECTOR_PROXY_IMAGE", _IMAGE)
    assert Settings().connector_proxy() == ConnectorProxy(image=_IMAGE, public_keys=(_CURRENT,))


# @spec ADR-0168 d7
def test_a_rotation_carries_the_previous_key_after_the_current(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CURIE_CONNECTOR_CALLER_PUBLIC_KEY", _CURRENT)
    monkeypatch.setenv("CURIE_CONNECTOR_CALLER_PREVIOUS_PUBLIC_KEY", _PREVIOUS)
    monkeypatch.setenv("CURIE_CONNECTOR_PROXY_IMAGE", _IMAGE)
    proxy = Settings().connector_proxy()
    assert proxy is not None and proxy.public_keys == (_CURRENT, _PREVIOUS)


# @spec ADR-0168 d7
def test_the_proxy_pulls_as_the_worker_does(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CURIE_CONNECTOR_CALLER_PUBLIC_KEY", _CURRENT)
    monkeypatch.setenv("CURIE_CONNECTOR_PROXY_IMAGE", _IMAGE)
    monkeypatch.setenv("CURIE_CONNECTOR_PROXY_IMAGE_PULL_POLICY", "Always")
    monkeypatch.setenv("CURIE_CONNECTOR_PROXY_IMAGE_PULL_SECRETS", "ghcr-pull, mirror-pull")
    assert Settings().connector_proxy() == ConnectorProxy(
        image=_IMAGE,
        public_keys=(_CURRENT,),
        pull_policy="Always",
        pull_secrets=("ghcr-pull", "mirror-pull"),
    )


# @spec ADR-0168 d7
def test_unset_pull_settings_leave_the_cluster_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CURIE_CONNECTOR_CALLER_PUBLIC_KEY", _CURRENT)
    monkeypatch.setenv("CURIE_CONNECTOR_PROXY_IMAGE", _IMAGE)
    monkeypatch.setenv("CURIE_CONNECTOR_PROXY_IMAGE_PULL_SECRETS", "")
    proxy = Settings().connector_proxy()
    assert proxy is not None
    assert (proxy.pull_policy, proxy.pull_secrets) == (None, ())


# @spec ADR-0168 d7
@pytest.mark.parametrize(
    "env",
    [
        {"CURIE_CONNECTOR_CALLER_PUBLIC_KEY": "not base64!", "CURIE_CONNECTOR_PROXY_IMAGE": _IMAGE},
        {"CURIE_CONNECTOR_CALLER_PUBLIC_KEY": "c2hvcnQ=", "CURIE_CONNECTOR_PROXY_IMAGE": _IMAGE},
        {"CURIE_CONNECTOR_CALLER_PUBLIC_KEY": _CURRENT},
        {
            "CURIE_CONNECTOR_CALLER_PREVIOUS_PUBLIC_KEY": _PREVIOUS,
            "CURIE_CONNECTOR_PROXY_IMAGE": _IMAGE,
        },
        {
            "CURIE_CONNECTOR_CALLER_PUBLIC_KEY": _CURRENT,
            "CURIE_CONNECTOR_PROXY_IMAGE": _IMAGE,
            "CURIE_CONNECTOR_PROXY_IMAGE_PULL_POLICY": "Sometimes",
        },
    ],
    ids=["not_base64", "not_32_bytes", "no_image", "previous_without_current", "pull_policy"],
)
def test_a_proxy_the_render_could_not_use_fails_at_boot(
    env: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    with pytest.raises(ValidationError, match="(?i)connector.caller"):
        Settings()


# #5550: the grant store the caller proxy is rendered with. The address is the
# Valkey DSN without its password; the password arrives from the chart Secret.
_SECRET = "acme-rel-secrets"
_PASSWORD = "Sentinel5550Valkey/Pass@word"


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "connector_grant_store_secret_name": _SECRET,
        "valkey_password": _PASSWORD,
        "valkey_host": "acme-rel-valkey",
        "valkey_port": 6379,
        "valkey_tls": False,
        "valkey_url": None,
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def test_the_grant_store_is_the_valkey_parts_without_the_password() -> None:
    assert GRANT_STORE_SECRET_KEY == "valkeyPassword"
    assert _settings().connector_grant_store() == GrantStore(
        address="redis://acme-rel-valkey:6379/0",
        secret_name=_SECRET,
        secret_key="valkeyPassword",
    )


def test_the_grant_store_scheme_follows_valkey_tls() -> None:
    store = _settings(valkey_tls=True, valkey_port=6380).connector_grant_store()
    assert store.address == "rediss://acme-rel-valkey:6380/0"


@pytest.mark.parametrize(
    ("valkey_url", "address"),
    [
        (f"redis://:{quote(_PASSWORD, safe='')}@valkey:6379/0", "redis://valkey:6379/0"),
        (
            f"redis://grant-user:{quote(_PASSWORD, safe='')}@valkey:6390/4",
            "redis://grant-user@valkey:6390/4",
        ),
        (
            f"rediss://:{quote(_PASSWORD, safe='')}@[fd00::1]:6379/0",
            "rediss://[fd00::1]:6379/0",
        ),
        ("redis://valkey:6379/1", "redis://valkey:6379/1"),
        ("redis://grant-user@valkey", "redis://grant-user@valkey"),
        ("redis://:@valkey:6379/0", "redis://valkey:6379/0"),
    ],
    ids=["password_only", "username_port_db", "ipv6_tls", "no_password", "username_only", "empty"],
)
def test_the_grant_store_keeps_everything_in_valkey_url_but_the_password(
    valkey_url: str, address: str
) -> None:
    store = _settings(valkey_url=valkey_url).connector_grant_store()
    assert store.address == address
    assert (store.secret_name, store.secret_key) == (_SECRET, "valkeyPassword")


@pytest.mark.parametrize("name", ["", "   "])
def test_no_grant_store_secret_name_is_refused_by_name(name: str) -> None:
    with pytest.raises(ValueError, match="hosted_connector_requires_grant_store_secret") as caught:
        _settings(connector_grant_store_secret_name=name).connector_grant_store()
    assert "CURIE_CONNECTOR_GRANT_STORE_SECRET_NAME" in str(caught.value)


def test_the_grant_store_secret_name_reads_from_its_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CURIE_CONNECTOR_GRANT_STORE_SECRET_NAME", _SECRET)
    assert Settings().connector_grant_store_secret_name == _SECRET


def test_a_valkey_url_password_the_secret_does_not_hold_is_refused() -> None:
    other = "Other5550UrlPassword"
    with pytest.raises(ValueError, match="grant_store_password_not_in_secret") as caught:
        _settings(valkey_url=f"redis://:{other}@valkey:6379/0").connector_grant_store()
    assert other not in str(caught.value)
    assert _PASSWORD not in str(caught.value)
