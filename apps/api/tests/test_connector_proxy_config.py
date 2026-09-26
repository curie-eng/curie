"""The API's caller proxy settings (ADR-0168 decision 7).

Pure: each case builds `Settings()` from the env it set, with no fixtures.
The keys are the public halves frozen in tests/vectors/connector-caller-token.json.
"""

from __future__ import annotations

import pytest
from curie_api.config import Settings
from plugin_format.connector_render import ConnectorProxy
from pydantic import ValidationError

_CURRENT = "A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg="
_PREVIOUS = "Kay64UG8yvCyLhqU000LxzYeUm0L/hLIl5S8kyKWbdc="
_IMAGE = "ghcr.io/curie-eng/curie-worker:0.0.0"
_NAMES = (
    "CURIE_CONNECTOR_CALLER_PUBLIC_KEY",
    "CURIE_CONNECTOR_CALLER_PREVIOUS_PUBLIC_KEY",
    "CURIE_CONNECTOR_PROXY_IMAGE",
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
    ],
    ids=["not_base64", "not_32_bytes", "no_image", "previous_without_current"],
)
def test_a_proxy_the_render_could_not_use_fails_at_boot(
    env: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    with pytest.raises(ValidationError, match="(?i)connector.caller"):
        Settings()
