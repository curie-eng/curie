"""Operator initializer for selected HTTP receivers. @spec HTTP-INVOCATION-EXECUTION-19/20.

``CURIE_HTTP_RECEIVER_DIR`` selects one protected ``receivers.json``. It is read
once at startup and never carries a core context key.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, Never

from curie_internal.operator_file import (
    OperatorFileRefusal,
    decode_key_material,
    read_operator_json,
)
from slack_sdk.web import WebClient

from .config import DispatcherConfig
from .http_receiver import (
    HttpReceiverBindingV1,
    HttpReceiverRuntime,
    HttpReceiverScopeV1,
    build_http_receiver_runtime,
)
from .identities import SlackIdentityCredentials
from .preflight import PreflightedIdentity

RECEIVERS_FILE = "receivers.json"
_MAX_ENTRIES = 256

Stage = Literal["unconfigured_directory", "unsafe_file", "invalid_document", "invalid_runtime"]


class HttpReceiverInitializationError(Exception):
    """A fixed startup refusal naming only its stage. @spec HTTP-INVOCATION-EXECUTION-20."""

    def __init__(self, stage: Stage) -> None:
        super().__init__("http receiver initialization refused: " + stage)
        self.stage: Stage = stage


@dataclass(frozen=True, slots=True)
class HttpReceiverRegistration:
    """One validated receiver entry, redacted. @spec HTTP-INVOCATION-EXECUTION-20."""

    scope: HttpReceiverScopeV1 = field(repr=False)
    bindings: tuple[HttpReceiverBindingV1, ...] = field(repr=False)
    key: bytes = field(repr=False)

    def __repr__(self) -> str:
        """@spec HTTP-INVOCATION-EXECUTION-20."""
        return "HttpReceiverRegistration()"


def _refuse(stage: Stage) -> Never:
    """@spec HTTP-INVOCATION-EXECUTION-20."""
    raise HttpReceiverInitializationError(stage) from None


def _object(value: Any, keys: frozenset[str]) -> Mapping[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-20: exactly the declared members."""
    if type(value) is not dict or frozenset(value) != keys:
        raise ValueError
    return value


def _entries(value: Any) -> list[Any]:
    """@spec HTTP-INVOCATION-EXECUTION-20: 1 through 256 entries."""
    if type(value) is not list or not 1 <= len(value) <= _MAX_ENTRIES:
        raise ValueError
    return value


def _wire(value: Any) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-20: validate models in their strict JSON mode."""
    return json.dumps(value, ensure_ascii=False)


def _registration(value: Any) -> HttpReceiverRegistration:
    """@spec HTTP-INVOCATION-EXECUTION-19/20."""
    entry = _object(value, frozenset({"scope", "bindings", "key"}))
    return HttpReceiverRegistration(
        scope=HttpReceiverScopeV1.model_validate_json(_wire(entry["scope"])),
        bindings=tuple(
            HttpReceiverBindingV1.model_validate_json(_wire(item))
            for item in _entries(entry["bindings"])
        ),
        key=decode_key_material(entry["key"]),
    )


def load_http_receiver_registrations(directory: str) -> tuple[HttpReceiverRegistration, ...]:
    """Validated receiver entries from one protected file. @spec HTTP-INVOCATION-EXECUTION-20."""
    try:
        document = read_operator_json(directory, RECEIVERS_FILE)
    except OperatorFileRefusal as refusal:
        _refuse(refusal.stage)
    try:
        root = _object(document, frozenset({"schema_version", "receivers"}))
        if type(root["schema_version"]) is not int or root["schema_version"] != 1:
            raise ValueError
        registrations = tuple(_registration(item) for item in _entries(root["receivers"]))
        names = [item.scope.channel_identity_name for item in registrations]
        if len(set(names)) != len(names):
            raise ValueError
        return registrations
    except Exception:  # noqa: BLE001 - document defects never expose supplied values.
        _refuse("invalid_document")


def build_selected_receivers(
    config: DispatcherConfig,
    admitted: Sequence[PreflightedIdentity],
    registrations: tuple[HttpReceiverRegistration, ...],
    *,
    web_client_factory: Callable[[DispatcherConfig, SlackIdentityCredentials], WebClient],
) -> tuple[HttpReceiverRuntime, ...]:
    """One qualified runtime per selected admitted identity.

    @spec HTTP-INVOCATION-EXECUTION-19/20: an unadmitted selection or failed auth.test
    stops startup rather than starting that identity on the legacy path.
    """
    try:
        by_name = {item.credentials.name: item.credentials for item in admitted}
        receivers = []
        for registration in registrations:
            credentials = by_name[registration.scope.channel_identity_name]
            receivers.append(build_http_receiver_runtime(
                registration.scope, registration.bindings, receiver_key=registration.key,
                web_client=web_client_factory(config, credentials),
                api_base_url=config.api_base_url,
            ))
        return tuple(receivers)
    except Exception:  # noqa: BLE001 - startup errors exclude Slack and secret bytes.
        _refuse("invalid_runtime")
