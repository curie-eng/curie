"""The worker's application render passes the trusted caller verify keys (ADR 0239, rework 1).

The maintainer's decision: caller verify keys reach the shared proxy only
through the frozen render, from the same trusted configuration the dedicated
render uses (``Settings.connector_proxy().public_keys``, current then
previous). ``GET /v1/internal/shared-connectors/applications/{id}/render``
therefore answers ``render_application(stored spec, caller_public_keys=keys)``
with ``keys`` the configured keys in canonical spelling (stripped, strictly
decoded to 32 bytes, padded standard base64) and without repeats, current
first. With no key configured it refuses ``503 shared_caller_keys_unconfigured``;
a key the frozen render refuses answers 500 with the frozen code and never
the key text.

Every case drives the real route through a TestClient on the disposable
database. The expected keys are computed here, independently of the API.
"""

from __future__ import annotations

import base64
import dataclasses
import json
import types
import uuid
from collections.abc import Iterator
from typing import Any

import nacl.signing
import pytest
from _shared_connector_support import (
    PROXY_IMAGE,
    SharedCleanup,
    SharedStore,
    application_row,
    canonical,
    random_installation,
    recipient_rows,
    shared_app,
)
from curie_api.config import Settings, get_settings
from plugin_format.shared_connector_render import SharedApplication, render_application

PUBLIC_KEYS_ENV = "CURIE_CALLER_PROXY_PUBLIC_KEYS"
CURRENT_ENV = "CURIE_CONNECTOR_CALLER_PUBLIC_KEY"
PREVIOUS_ENV = "CURIE_CONNECTOR_CALLER_PREVIOUS_PUBLIC_KEY"
IMAGE_ENV = "CURIE_CONNECTOR_PROXY_IMAGE"
UNCONFIGURED = "shared_caller_keys_unconfigured"
_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"


def _key() -> str:
    return base64.b64encode(bytes(nacl.signing.SigningKey.generate().verify_key)).decode()


def _stray_bits(key: str) -> str:
    """The same 32 bytes spelled with nonzero unused trailing bits.

    A 32 byte key is 43 base64 digits and one pad; the last digit carries two
    unused bits, zero in the canonical spelling. Python's decoder accepts
    either, the frozen render accepts only the canonical one.
    """

    assert key.endswith("=") and not key.endswith("==")
    last = _ALPHABET.index(key[-2])
    assert last & 0b11 == 0
    spelled = key[:-2] + _ALPHABET[last | 0b01] + "="
    assert spelled != key
    assert base64.b64decode(spelled, validate=True) == base64.b64decode(key, validate=True)
    return spelled


def _canonical(*configured: str) -> tuple[str, ...]:
    keys: list[str] = []
    for text in configured:
        if text.strip():
            raw = base64.b64decode(text.strip(), validate=True)
            assert len(raw) == 32
            key = base64.b64encode(raw).decode()
            if key not in keys:
                keys.append(key)
    return tuple(keys)


@pytest.fixture
def cleanup(migrated: None) -> Iterator[SharedCleanup]:
    tracker = SharedCleanup()
    yield tracker
    tracker.run()


@pytest.fixture
def current() -> str:
    return _key()


@pytest.fixture
def store(
    cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch, current: str
) -> Iterator[SharedStore]:
    installation = cleanup.installation(random_installation())
    monkeypatch.delenv(PREVIOUS_ENV, raising=False)
    with shared_app(
        monkeypatch, installation, **{CURRENT_ENV: current, IMAGE_ENV: PROXY_IMAGE}
    ) as client:
        yield SharedStore(client, installation, cleanup)
    monkeypatch.undo()
    get_settings.cache_clear()


def _configure(monkeypatch: pytest.MonkeyPatch, current: str, previous: str = "") -> None:
    """Point the settings the routes read per request at these keys."""

    monkeypatch.setenv(CURRENT_ENV, current)
    if previous:
        monkeypatch.setenv(PREVIOUS_ENV, previous)
    else:
        monkeypatch.delenv(PREVIOUS_ENV, raising=False)
    get_settings.cache_clear()


def _stored_spec(application_id: str) -> SharedApplication:
    return SharedApplication.model_validate_json(
        json.dumps(application_row(application_id)["spec"])
    )


def _rendered(store: SharedStore, application_id: str) -> dict[str, Any]:
    response = store.application_render(application_id)
    assert response.status_code == 200, response.text
    body: dict[str, Any] = response.json()
    return body


def _container_env(objects: list[dict[str, Any]], name: str) -> dict[str, str]:
    (deployment,) = [obj for obj in objects if obj["kind"] == "Deployment"]
    (container,) = [
        c for c in deployment["spec"]["template"]["spec"]["containers"] if c["name"] == name
    ]
    return {entry["name"]: entry["value"] for entry in container.get("env") or []}


def _assert_renders_with(store: SharedStore, application_id: str, keys: tuple[str, ...]) -> None:
    objects = _rendered(store, application_id)["objects"]
    assert canonical(objects) == canonical(
        render_application(_stored_spec(application_id), caller_public_keys=keys)
    )
    assert _container_env(objects, "proxy")[PUBLIC_KEYS_ENV] == ",".join(keys)
    server = _container_env(objects, "server")
    assert not [name for name in server if name.startswith("CURIE_CALLER_")], server
    assert not any(key in value for key in keys for value in server.values())


def test_render_passes_the_current_and_previous_keys(
    store: SharedStore, monkeypatch: pytest.MonkeyPatch, current: str
) -> None:
    application_id = store.register_application()["application_id"]
    _assert_renders_with(store, application_id, (current,))

    previous, current = current, _key()
    _configure(monkeypatch, current, previous)
    assert Settings().connector_proxy() is not None
    _assert_renders_with(store, application_id, (current, previous))


def test_rotation_changes_only_the_proxy_key_env(
    store: SharedStore, monkeypatch: pytest.MonkeyPatch, current: str
) -> None:
    application_id = store.register_application()["application_id"]
    before = _rendered(store, application_id)["objects"]
    rotated = _key()
    _configure(monkeypatch, rotated, current)
    after = _rendered(store, application_id)["objects"]

    assert _container_env(after, "proxy")[PUBLIC_KEYS_ENV] == f"{rotated},{current}"

    def without_keys(objects: list[dict[str, Any]]) -> bytes:
        stripped = json.loads(json.dumps(objects))
        for obj in stripped:
            if obj["kind"] != "Deployment":
                continue
            for container in obj["spec"]["template"]["spec"]["containers"]:
                container["env"] = [
                    e for e in container.get("env") or [] if e["name"] != PUBLIC_KEYS_ENV
                ]
        return canonical(stripped)

    assert canonical(after) != canonical(before), "the pod template changes, so it rolls"
    assert without_keys(after) == without_keys(before), "nothing but the key env changed"


def test_a_previous_key_equal_to_the_current_renders_one_key(
    store: SharedStore, monkeypatch: pytest.MonkeyPatch, current: str
) -> None:
    application_id = store.register_application()["application_id"]
    _configure(monkeypatch, current, current)
    assert Settings().connector_proxy() is not None, "the settings accept the repeat"
    _assert_renders_with(store, application_id, (current,))


@pytest.mark.parametrize("spelling", ["surrounding-space", "stray-trailing-bits", "both"])
def test_a_configured_key_renders_in_its_canonical_spelling(
    store: SharedStore, monkeypatch: pytest.MonkeyPatch, current: str, spelling: str
) -> None:
    application_id = store.register_application()["application_id"]
    previous = _key()
    configured = {
        "surrounding-space": (f"  {current}\t", f" {previous} "),
        "stray-trailing-bits": (_stray_bits(current), _stray_bits(previous)),
        "both": (f" {_stray_bits(current)} ", previous),
    }[spelling]
    _configure(monkeypatch, *configured)
    assert Settings().connector_proxy() is not None, "the settings accept this spelling"
    keys = _canonical(*configured)
    assert keys == (current, previous)
    _assert_renders_with(store, application_id, keys)
    # The same key spelled two ways is one key.
    _configure(monkeypatch, current, _stray_bits(current))
    _assert_renders_with(store, application_id, (current,))


def test_no_key_configured_refuses_503(
    store: SharedStore, monkeypatch: pytest.MonkeyPatch, current: str
) -> None:
    application_id = store.register_application()["application_id"]
    store.register_recipient(application_id)
    before = recipient_rows(application_id)
    monkeypatch.setenv(CURRENT_ENV, "")
    monkeypatch.delenv(PREVIOUS_ENV, raising=False)
    get_settings.cache_clear()
    assert Settings().connector_proxy() is None

    response = store.application_render(application_id)

    assert response.status_code == 503, response.text
    assert response.json() == {"detail": UNCONFIGURED}, response.text
    assert current not in response.text
    assert recipient_rows(application_id) == before
    # Liveness: configured again, the same application renders.
    _configure(monkeypatch, current)
    _assert_renders_with(store, application_id, (current,))


def test_a_key_the_frozen_render_refuses_answers_500_without_the_key(
    store: SharedStore, monkeypatch: pytest.MonkeyPatch, current: str
) -> None:
    """Defense in depth past the settings' boot check: a 31 byte key reaching the route."""

    application_id = store.register_application()["application_id"]
    short = base64.b64encode(bytes(31)).decode()
    real = Settings.connector_proxy

    def short_key(self: Settings) -> Any:
        proxy = real(self)
        assert proxy is not None
        fields = {field.name: getattr(proxy, field.name) for field in dataclasses.fields(proxy)}
        return types.SimpleNamespace(**{**fields, "public_keys": (short,)})

    monkeypatch.setattr(Settings, "connector_proxy", short_key)
    get_settings.cache_clear()

    response = store.application_render(application_id)

    assert response.status_code == 500, response.text
    assert response.json() == {"detail": "caller_public_key_malformed"}, response.text
    assert short not in response.text
    # Liveness: the real settings render the application again.
    monkeypatch.setattr(Settings, "connector_proxy", real)
    get_settings.cache_clear()
    _assert_renders_with(store, application_id, (current,))


def test_registration_and_render_never_store_a_caller_key(store: SharedStore, current: str) -> None:
    """The keys are a render-time input: the stored row never holds one."""

    application_id = store.register_application(str(uuid.uuid4()))["application_id"]
    _assert_renders_with(store, application_id, (current,))
    assert current not in json.dumps(application_row(application_id), default=str)
