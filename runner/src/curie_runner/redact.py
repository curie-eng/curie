"""Shared telemetry policy and the runner's outbound content redaction."""

from __future__ import annotations

import json
import re
from collections.abc import Collection, Iterable, Mapping
from typing import Any, cast

from aci_protocol import BootEnv
from curie_telemetry.bootstrap import _exporter_headers
from curie_telemetry.redact import (
    REDACTION_BOUNDARIES,
    REDACTION_RULES,
    RedactingLogFilter,
    RedactionRule,
    install_stdout_redaction,
    redact_span_attribute,
    redact_text,
)

from .config import RunnerConfig
from .sdk_auth import MODEL_ENV_KEY_ENV, parse_env_keys

_HELD_PLACEHOLDER = "[REDACTED:held_secret]"
_CONNECTOR_SECRET_KEYS_ENV = BootEnv.env_key("connector_secret_keys")
_OTEL_HEADERS_ENV = BootEnv.env_key("otel_headers")
_SECRET_NAME = re.compile(
    r"(?:^|_)(?:TOKEN|SECRET|PASSWORD|PASSWD|PWD|CREDENTIALS?|API_KEY|APIKEY|"
    r"ACCESS_KEY|PRIVATE_KEY)(?:_|$)",
    re.IGNORECASE,
)
_ENV_REFERENCE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_CONTENT_FIELDS = frozenset(
    ("text", "message", "detail", "result", "approval_summary", "approval_display")
)


def collect_held_secrets(
    config: RunnerConfig,
    *,
    environments: Iterable[Mapping[str, str]],
    credential_names: Collection[str],
    connector_names: Collection[str],
    server_groups: Iterable[Mapping[str, Any]],
) -> frozenset[str]:
    """Snapshot actual held values before hosted connector env entries disappear."""

    sources = tuple(environments)
    names = set(credential_names) | set(connector_names)
    names.update(("CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN"))
    for env in sources:
        names.update(name for name in env if _SECRET_NAME.search(name))
        declared = env.get(MODEL_ENV_KEY_ENV, "").strip()
        if declared:
            names.update(parse_env_keys(declared))
        names.update(
            name.strip()
            for name in env.get(_CONNECTOR_SECRET_KEYS_ENV, "").split(",")
            if name.strip()
        )

    values = {
        value
        for value in (
            config.session.credentials_ref,
            config.runner_token,
            config.connector_caller_token,
        )
        if value
    }
    names.discard(_CONNECTOR_SECRET_KEYS_ENV)
    values.update(value for env in sources for name in names if (value := env.get(name)))
    otel_header_maps: list[dict[str, str]] = []
    if config.session.otel.headers is not None:
        otel_header_maps.append(
            _exporter_headers("traces", {_OTEL_HEADERS_ENV: config.session.otel.headers}) or {}
        )
    for env in sources:
        # General headers remain held even when a signal overrides their use.
        otel_header_maps.append(
            _exporter_headers("traces", {_OTEL_HEADERS_ENV: env.get(_OTEL_HEADERS_ENV, "")}) or {}
        )
        otel_header_maps.extend(
            _exporter_headers(signal, env) or {} for signal in ("traces", "metrics", "logs")
        )
    for parsed_otel_headers in otel_header_maps:
        for name, value in parsed_otel_headers.items():
            if _is_auth_header(name):
                _collect_header_value(values, name.replace("-", "_"), value)
    for servers in server_groups:
        for server in servers.values():
            if not isinstance(server, dict):
                continue
            entries: list[tuple[str, str]] = []
            catalog_env = server.get("env")
            if isinstance(catalog_env, dict):
                entries.extend(
                    (name, raw)
                    for name, raw in catalog_env.items()
                    if isinstance(name, str)
                    and isinstance(raw, str)
                    and (name in names or _SECRET_NAME.search(name))
                    and name != _CONNECTOR_SECRET_KEYS_ENV
                )
            headers = server.get("headers")
            if isinstance(headers, dict):
                entries.extend(
                    (name.replace("-", "_"), raw)
                    for name, raw in headers.items()
                    if isinstance(name, str) and isinstance(raw, str) and _is_auth_header(name)
                )
            for normalized, raw in entries:
                references = tuple(_ENV_REFERENCE.finditer(raw))
                if references:
                    # MCP allows a literal fallback with ${VAR:-default}.
                    # Keep it even when a held environment value wins.
                    values.update(
                        default for match in references if (default := match.group(2))
                    )
                    for env in sources:
                        values.update(
                            value
                            for match in references
                            if (value := env.get(match.group(1)))
                        )
                        if all(
                            match.group(1) in env or match.group(2) is not None
                            for match in references
                        ):
                            def expand(
                                match: re.Match[str], held_env: Mapping[str, str] = env
                            ) -> str:
                                key = match.group(1)
                                return held_env[key] if key in held_env else match.group(2) or ""

                            expanded = _ENV_REFERENCE.sub(expand, raw)
                            _collect_header_value(values, normalized, expanded)
                else:
                    _collect_header_value(values, normalized, raw)
    return frozenset(values)


def _is_auth_header(name: str) -> bool:
    return name.lower() in (
        "authorization", "proxy-authorization", "x-curie-caller"
    ) or _SECRET_NAME.search(name.replace("-", "_")) is not None


def _collect_header_value(values: set[str], name: str, value: str) -> None:
    if not value:
        return
    values.add(value)
    if name.lower() in ("authorization", "proxy_authorization"):
        parts = value.split(None, 1)
        if len(parts) == 2 and parts[1]:
            values.add(parts[1])


class OutboundRedactor:
    """Scrub content while retaining possible held secret prefixes between deltas."""

    def __init__(self, held_secrets: frozenset[str]) -> None:
        self._secrets = tuple(
            sorted(filter(None, held_secrets), key=lambda item: (-len(item), item))
        )
        self._pending = ""
        self._pending_record: dict[str, object] | None = None

    def _literal_intervals(self, text: str) -> list[tuple[int, int]]:
        occurrences: list[tuple[int, int]] = []
        for secret in self._secrets:
            start = text.find(secret)
            while start >= 0:
                occurrences.append((start, start + len(secret)))
                start = text.find(secret, start + 1)
        occurrences.sort()
        merged: list[tuple[int, int]] = []
        for start, end in occurrences:
            if merged and start < merged[-1][1]:
                prior_start, prior_end = merged[-1]
                merged[-1] = (prior_start, max(prior_end, end))
            else:
                merged.append((start, end))
        return merged

    def _text(self, text: str) -> str:
        parts: list[str] = []
        cursor = 0
        for start, end in self._literal_intervals(text):
            parts.extend((text[cursor:start], _HELD_PLACEHOLDER))
            cursor = end
        parts.append(text[cursor:])
        return redact_text("".join(parts))

    def _content(self, value: object) -> object:
        if isinstance(value, str):
            return self._text(value)
        if isinstance(value, list):
            return [self._content(item) for item in value]
        if isinstance(value, dict):
            entries = [(self._text(key), item) for key, item in value.items()]
            reserved = {key for key, _ in entries}
            result: dict[str, object] = {}
            collision = 0
            for key, item in entries:
                if key in result:
                    while True:
                        collision += 1
                        key = f"[REDACTED:result_key_{collision}]"
                        if key not in reserved and key not in result:
                            break
                result[key] = self._content(item)
            return result
        return value

    def _encode(self, record: dict[str, object]) -> str:
        return json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"

    def _safe_end(self, text: str) -> int:
        held = 0
        for secret in self._secrets:
            for size in range(min(len(text), len(secret) - 1), held, -1):
                if text.endswith(secret[:size]):
                    held = size
                    break
        end = len(text) - held
        # An overlapping group can extend into a suffix that may grow into
        # another held value. Keep the group until that suffix is resolved.
        if held:
            for start, stop in self._literal_intervals(text):
                if start < end < stop:
                    end = start
                    break
        return end

    def push(self, line: str) -> tuple[str, ...]:
        record = cast("dict[str, object]", json.loads(line))
        if record.get("type") == "text_delta" and isinstance(record.get("text"), str):
            text = self._pending + cast("str", record["text"])
            end = self._safe_end(text)
            template = self._pending_record or record
            self._pending = text[end:]
            self._pending_record = record if self._pending else None
            if not end and text:
                return ()
            return (self._encode({**template, "text": self._text(text[:end])}),)
        emitted: list[str] = []
        if record.get("type") == "final":
            pending = self.finish()
            if pending is not None:
                emitted.append(pending)
        for name in _CONTENT_FIELDS & record.keys():
            record[name] = self._content(record[name])
        emitted.append(self._encode(record))
        return tuple(emitted)

    def finish(self) -> str | None:
        """Flush only on normal completion; cancellation discards the instance."""

        if not self._pending or self._pending_record is None:
            return None
        record = {**self._pending_record, "text": self._text(self._pending)}
        self._pending = ""
        self._pending_record = None
        return self._encode(record)


__all__ = [
    "REDACTION_BOUNDARIES",
    "REDACTION_RULES",
    "RedactingLogFilter",
    "RedactionRule",
    "OutboundRedactor",
    "collect_held_secrets",
    "install_stdout_redaction",
    "redact_span_attribute",
    "redact_text",
]
