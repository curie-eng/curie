"""Closed execution-profile codec for the protected worker lane.

The digest binds provisioner bytes to a manifest. It is not evidence that a
runner or substrate actually used these settings, and it is not qualification.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
import uuid
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlsplit

_MAX_BYTES = 65_536
_MAX_USD = Decimal("1000000")
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_IMAGE = re.compile(r"^sha256:[0-9a-f]{64}$")
_GENERATION = re.compile(r"^[1-9][0-9]{0,18}$")
_MAX_GENERATION = 9_223_372_036_854_775_807
_DECIMAL = re.compile(r"^(?:0|[1-9][0-9]{0,6})(?:\.[0-9]{1,6})?$")
_OPAQUE_REF = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$")
_DNS_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_TOP = {"schema_version", "agent_id", "version_id", "runner", "policy", "launch", "bindings"}
_RUNNER = {
    "harness", "plugin_dir", "model", "reviewer_model", "thinking", "api_backend",
    "model_env_key", "model_base_url", "model_credential_ref", "budget", "max_turns",
    "execution_deadline_seconds", "false_completion_check", "fake_model",
}
_POLICY = {
    "tool_access", "lifecycle_hooks", "memory", "workspace", "attachments", "approvals",
    "behavior_packs",
}
_LAUNCH = {
    "runner_image_digest", "substrate", "resources", "init_image_digests", "fetch_image_digest",
}
_BINDINGS = {
    "runner_facing_api_base_url", "history_signing_credential_ref", "qualified_connectors",
}


class ExecutionConfigInvalid(ValueError):
    """A redacted invalid-profile signal; never includes input values."""

    def __init__(self) -> None:
        super().__init__("protected execution configuration is invalid")


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ExecutionConfigInvalid()
        result[key] = value
    return result


def _no_constant(_value: str) -> None:
    raise ExecutionConfigInvalid()


def _no_float(_value: str) -> None:
    raise ExecutionConfigInvalid()


def _require(condition: bool) -> None:
    if not condition:
        raise ExecutionConfigInvalid()


def _object(value: Any, keys: set[str]) -> dict[str, Any]:
    _require(type(value) is dict and set(value) == keys)
    return value  # type: ignore[no-any-return]


def _text(value: Any, *, max_length: int = 512, nonempty: bool = True) -> str:
    _require(type(value) is str and len(value) <= max_length)
    _require(not nonempty or bool(value.strip()))
    _require(not any(ord(ch) < 0x20 for ch in value))
    return value  # type: ignore[no-any-return]


def _integer(value: Any, low: int, high: int) -> int:
    _require(type(value) is int and low <= value <= high)
    return value  # type: ignore[no-any-return]


def _boolean(value: Any) -> bool:
    _require(type(value) is bool)
    return value  # type: ignore[no-any-return]


def _uuid(value: Any) -> str:
    text = _text(value, max_length=36)
    _require(str(uuid.UUID(text)) == text)
    return text


def _credential(value: Any) -> None:
    ref = _object(value, {"id", "generation"})
    credential_id = _text(ref["id"], max_length=256)
    _require(_OPAQUE_REF.fullmatch(credential_id) is not None)
    generation = _text(ref["generation"], max_length=20)
    _require(
        _GENERATION.fullmatch(generation) is not None
        and int(generation) <= _MAX_GENERATION
    )


def _opaque_name(value: Any) -> str:
    text = _text(value, max_length=256)
    _require(_OPAQUE_REF.fullmatch(text) is not None)
    return text


def _url(value: Any) -> None:
    text = _text(value, max_length=2048)
    parsed = urlsplit(text)
    if parsed.scheme != "https" or parsed.hostname is None:
        raise ExecutionConfigInvalid()
    hostname = parsed.hostname
    _require(parsed.username is None and parsed.password is None)
    _require(parsed.query == "" and parsed.fragment == "" and parsed.path == "")
    _require("\\" not in text and hostname.isascii())
    _require(hostname == hostname.lower() and parsed.netloc == parsed.netloc.lower())
    try:
        port = parsed.port
    except ValueError:
        raise ExecutionConfigInvalid() from None
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        _require(
            all(_DNS_LABEL.fullmatch(label) is not None for label in hostname.split("."))
        )
    else:
        _require(address.compressed == hostname)
    _require(port is None or 1 <= port <= 65535)


def _validate(value: dict[str, Any], manifest: Any) -> None:
    _require(value["schema_version"] == 1 and type(value["schema_version"]) is int)
    _uuid(value["agent_id"])
    _uuid(value["version_id"])

    runner = _object(value["runner"], _RUNNER)
    _require(runner["harness"] == "claude-agent-sdk")
    plugin_dir = _text(runner["plugin_dir"], max_length=1024)
    _require(plugin_dir == "/bundles/current")
    _opaque_name(runner["model"])
    _opaque_name(runner["reviewer_model"])
    _require(runner["thinking"] == "disabled")
    _require(runner["api_backend"] == "messages")
    _require(runner["model_env_key"] == "ANTHROPIC_API_KEY")
    _url(runner["model_base_url"])
    model_credential = _object(runner["model_credential_ref"], {"id", "generation"})
    _credential(model_credential)
    budget = _object(runner["budget"], {"max_output_tokens_per_run", "max_usd_per_day"})
    _integer(budget["max_output_tokens_per_run"], 1, 1_000_000)
    usd = _text(budget["max_usd_per_day"], max_length=32)
    _require(_DECIMAL.fullmatch(usd) is not None and not ("." in usd and usd.endswith("0")))
    try:
        amount = Decimal(usd)
    except InvalidOperation:
        raise ExecutionConfigInvalid() from None
    _require(Decimal(0) < amount <= _MAX_USD)
    _integer(runner["max_turns"], 1, 1000)
    _integer(runner["execution_deadline_seconds"], 1, 86_400)
    _require(
        runner["false_completion_check"] is False
        and type(runner["false_completion_check"]) is bool
    )
    _require(runner["fake_model"] is False and type(runner["fake_model"]) is bool)

    policy = _object(value["policy"], _POLICY)
    _require(policy["tool_access"] == "read-only")
    for key in _POLICY - {"tool_access"}:
        _require(_boolean(policy[key]) is False)

    launch = _object(value["launch"], _LAUNCH)
    runner_image = _text(launch["runner_image_digest"], max_length=71)
    _require(_IMAGE.fullmatch(runner_image) is not None)
    substrate = _object(
        launch["substrate"],
        {"kind", "authority_domain_id", "launch_identity", "launch_config_sha256"},
    )
    _require(substrate["kind"] in {"docker", "kubernetes"})
    _uuid(substrate["authority_domain_id"])
    _text(substrate["launch_identity"], max_length=256)
    config_hash = _text(substrate["launch_config_sha256"], max_length=64)
    _require(_HEX_64.fullmatch(config_hash) is not None)
    resources = _object(launch["resources"], {"cpu_millicores", "memory_mib"})
    _integer(resources["cpu_millicores"], 1, 64_000)
    _integer(resources["memory_mib"], 1, 1_048_576)
    init_images = launch["init_image_digests"]
    # The first qualified profile has no init containers. Supporting them
    # requires an independently measured, closed launch sequence.
    _require(type(init_images) is list and init_images == [])
    for image in init_images:
        _require(type(image) is str and _IMAGE.fullmatch(image) is not None)
    fetch_image = _text(launch["fetch_image_digest"], max_length=71)
    _require(_IMAGE.fullmatch(fetch_image) is not None)

    bindings = _object(value["bindings"], _BINDINGS)
    _url(bindings["runner_facing_api_base_url"])
    history_credential = _object(
        bindings["history_signing_credential_ref"], {"id", "generation"}
    )
    _credential(history_credential)
    _require(model_credential != history_credential)
    connectors = bindings["qualified_connectors"]
    _require(type(connectors) is list and connectors == [])

    try:
        manifest_value = manifest.as_dict()
        _require(manifest_value["runner_image_digest"] == runner_image)
        _require(manifest_value["substrate"] == substrate)
        profile_digest = hashlib.sha256(_canonical(value)).hexdigest()
        _require(manifest_value["execution_config_digest"] == profile_digest)
    except (AttributeError, KeyError, TypeError, ValueError):
        raise ExecutionConfigInvalid() from None


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


@dataclass(frozen=True, slots=True, repr=False)
class ProtectedExecutionConfig:
    """Immutable canonical bytes; repr never exposes profile contents."""

    _canonical_bytes: bytes = field(repr=False)

    @property
    def canonical_bytes(self) -> bytes:
        return bytes(self._canonical_bytes)

    @property
    def digest(self) -> str:
        return hashlib.sha256(self._canonical_bytes).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        value = json.loads(self._canonical_bytes)
        return value  # type: ignore[no-any-return]


def parse_execution_config(raw: bytes, *, manifest: Any) -> ProtectedExecutionConfig:
    """Parse strict UTF-8 JSON and bind the exact profile to a manifest."""
    try:
        _require(type(raw) is bytes and 0 < len(raw) <= _MAX_BYTES)
        value = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_pairs,
            parse_constant=_no_constant,
            parse_float=_no_float,
        )
        value = _object(value, _TOP)
        _validate(value, manifest)
        canonical = _canonical(value)
        return ProtectedExecutionConfig(canonical)
    except ExecutionConfigInvalid:
        raise
    except (UnicodeError, ValueError, TypeError, OverflowError, RecursionError, InvalidOperation):
        raise ExecutionConfigInvalid() from None
