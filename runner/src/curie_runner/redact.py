"""Shared telemetry policy and the runner's outbound content redaction."""

from __future__ import annotations

import base64
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


# The break between two text blocks of one turn (#3694). Each text_delta the
# runner emits is one whole TextBlock, and consumers join deltas with "", so the
# boundary is known only here. It is added here rather than in translation so a
# secret split across two blocks is still matched whole: the unbroken text is
# scrubbed, and breaks are placed into that result.
_BLOCK_BREAK = "\n\n"
# Consecutive refused breaks after which the rest of a text gets none.
_MAX_REJECTED_BREAKS = 4
# Encodings shorter than this match ordinary text. A 4 character value
# encodes to 8 characters; shorter values stay exact matches only.
_HELD_ENCODING_MIN_LENGTH = 8


# @spec ACTION-EXECUTOR-9: the sealed envelope grammar, frozen in
# tests/vectors/sealed-snapshot-reply.json and read in other images by the
# worker's ``_snapshot`` and the API's ``undoable`` grammar.
SEALED_ENVELOPE_CONSTANT = "curie.snapshot.v1"
_SEALED_ENVELOPE_KEYS = frozenset(("sealed", "kid", "ciphertext"))
_SEALED_KID = re.compile(r"[A-Za-z0-9._-]{1,64}")
_SEALED_CIPHERTEXT_MAX_BYTES = 65536


def is_sealed_envelope(value: object) -> bool:
    """True when ``value`` is exactly a sealed snapshot envelope.

    Exactly the keys ``sealed`` (the constant), ``kid`` (1 to 64 characters of
    ``[A-Za-z0-9._-]``) and ``ciphertext`` (standard base64 with padding, no
    line breaks, 1 to 65536 decoded bytes). The ciphertext is never opened.
    """

    if not isinstance(value, dict) or set(value) != _SEALED_ENVELOPE_KEYS:
        return False
    kid = value["kid"]
    ciphertext = value["ciphertext"]
    if value["sealed"] != SEALED_ENVELOPE_CONSTANT:
        return False
    if not isinstance(kid, str) or _SEALED_KID.fullmatch(kid) is None:
        return False
    if not isinstance(ciphertext, str) or not ciphertext.isascii():
        return False
    # A cheap bound before decoding: 65536 bytes encode to 87384 characters.
    if len(ciphertext) > 4 * ((_SEALED_CIPHERTEXT_MAX_BYTES + 2) // 3):
        return False
    try:
        decoded = base64.b64decode(ciphertext, validate=True)
    except ValueError:
        return False
    return 1 <= len(decoded) <= _SEALED_CIPHERTEXT_MAX_BYTES


def _held_literals(held_secrets: Collection[str]) -> tuple[str, ...]:
    """Exact held values plus standard and URL-safe base64 of the longer ones."""

    literals: set[str] = set()
    for value in held_secrets:
        if not value:
            continue
        literals.add(value)
        encoded = value.encode()
        for raw in (
            base64.standard_b64encode(encoded),
            base64.urlsafe_b64encode(encoded),
        ):
            text = raw.decode()
            for form in (text, text.rstrip("=")):
                if form and form != value and len(form) >= _HELD_ENCODING_MIN_LENGTH:
                    literals.add(form)
    return tuple(sorted(literals, key=lambda item: (-len(item), item)))


class OutboundRedactor:
    """Scrub content while retaining possible held secret prefixes between deltas.

    One instance serves one turn. It also separates the turn's text blocks
    with ``_BLOCK_BREAK``, except inside a secret or where the model already
    put whitespace.
    """

    def __init__(self, held_secrets: frozenset[str]) -> None:
        self._secrets = _held_literals(held_secrets)
        self._pending = ""
        self._pending_record: dict[str, object] | None = None
        # Offsets into _pending where a later text block began.
        self._pending_breaks: list[int] = []
        # Raw text of every delta this turn, and where each later block began.
        self._streamed = ""
        self._streamed_breaks: list[int] = []
        self._last_raw = ""

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

    def _scrub_with_breaks(self, text: str, breaks: list[int], before: str) -> str:
        """Scrub ``text``, with a block break at each break that may take one.

        The whole text is scrubbed once, and a break goes into that result only
        where the text before it scrubs to exactly the result's next stretch.
        With its breaks removed the result is ``_text(text)`` by construction,
        whatever the held values and rules match, so a break can move no secret
        out of a placeholder. Skipping joins inside a held value or a rule match
        on the raw text only keeps a break from landing beside a placeholder
        that covers both blocks. ``before`` is the raw character ahead of
        ``text``.
        """

        scrubbed = self._text(text)
        spans = self._literal_intervals(text) + [
            match.span() for rule in REDACTION_RULES for match in rule.pattern.finditer(text)
        ]
        parts: list[str] = []
        cursor = 0
        offset = 0
        rejected = 0
        for at in breaks:
            prior = text[at - 1] if at else before
            if (
                not prior
                or prior.isspace()
                or text[at].isspace()
                or any(start < at < stop for start, stop in spans)
            ):
                continue
            head = self._text(text[cursor:at])
            # Past the end of the scrub means a placeholder took the rest.
            if offset + len(head) >= len(scrubbed) or not scrubbed.startswith(head, offset):
                # Each rejection rescans a longer head, so a run of them inside
                # one long match stops placing breaks rather than going quadratic.
                rejected += 1
                if rejected >= _MAX_REJECTED_BREAKS:
                    break
                continue
            rejected = 0
            parts.extend((head, _BLOCK_BREAK))
            cursor = at
            offset += len(head)
        parts.append(scrubbed[offset:])
        return "".join(parts)

    def _stream_text(self, text: str, breaks: list[int]) -> str:
        clean = self._scrub_with_breaks(text, breaks, self._last_raw)
        if text:
            self._last_raw = text[-1]
        return clean

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

    def _matches(self, value: object) -> bool:
        """True when held literals or pattern rules would change ``value``."""

        return self._content(value) != value

    def _held(self, value: object) -> bool:
        """True when a held literal occurs in any string or key of ``value``."""

        if isinstance(value, str):
            return bool(self._literal_intervals(value))
        if isinstance(value, list):
            return any(self._held(item) for item in value)
        if isinstance(value, dict):
            return any(self._held(key) or self._held(item) for key, item in value.items())
        return False

    def _held_in_bytes(self, data: bytes) -> bool:
        """True when a held literal (raw or base64 form) occurs in ``data``.

        @spec ACTION-EXECUTOR-10: the decoded ciphertext of a valid envelope, so
        a plaintext secret that is merely base64-wrapped never crosses. Real
        ciphertext is random bytes and does not contain a held value.
        """

        return any(secret.encode("utf-8") in data for secret in self._secrets)

    def _side_effect_result(self, result: dict[str, object]) -> tuple[dict[str, object], bool]:
        """Scrub a ``side_effect_flag`` result; replay inputs verbatim or withheld.

        @spec ACTION-EXECUTOR-10. The replay inputs are ``prior`` when it
        validates as a sealed envelope, ``version`` and ``target``. They are
        never altered: pattern rules skip a valid envelope's ciphertext but run
        over its ``kid``, ``version`` and ``target``; the held literal check runs
        over all of them, ciphertext included, and over the decoded ciphertext
        bytes. Any match withholds all three
        (set to null). Every other field keeps the ordinary scrubbing. Returns
        the result and whether anything in it was replaced or withheld, which
        is the frozen meaning of ``redacted``.
        """

        prior = result.get("prior")
        sealed = is_sealed_envelope(prior)
        replay = {"version", "target"} | ({"prior"} if sealed else set())
        withhold = any(self._matches(result.get(key)) for key in ("version", "target"))
        if sealed:
            envelope = cast("dict[str, object]", prior)
            withhold = (
                withhold
                or self._matches(envelope["kid"])
                or self._held(envelope["ciphertext"])
                or self._held_in_bytes(base64.b64decode(cast("str", envelope["ciphertext"])))
            )
        others = {key: value for key, value in result.items() if key not in replay}
        scrubbed_others = cast("dict[str, object]", self._content(others))
        altered = withhold or scrubbed_others != others
        if withhold:
            # The three are withheld together, so no partial restore state crosses.
            replay_values: dict[str, object] = {"prior": None, "version": None, "target": None}
            if not sealed:
                replay_values.pop("prior")
        else:
            replay_values = {key: result[key] for key in replay if key in result}
        # Keep the connector's field order; scrubbed keys map one to one, in order.
        renamed = dict(zip(others, scrubbed_others, strict=True))
        out: dict[str, object] = {}
        for key in result:
            if key in replay:
                if key in replay_values:
                    out[key] = replay_values.pop(key)
            else:
                scrubbed_key = renamed[key]
                out[scrubbed_key] = scrubbed_others[scrubbed_key]
        out.update(replay_values)
        return out, altered

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
            incoming = cast("str", record["text"])
            breaks = list(self._pending_breaks)
            if incoming and self._streamed:
                breaks.append(len(self._pending))
                self._streamed_breaks.append(len(self._streamed))
            self._streamed += incoming
            text = self._pending + incoming
            end = self._safe_end(text)
            template = self._pending_record or record
            self._pending = text[end:]
            self._pending_breaks = [at - end for at in breaks if at >= end]
            self._pending_record = record if self._pending else None
            if not end and text:
                return ()
            clean = self._stream_text(text[:end], [at for at in breaks if at < end])
            return (self._encode({**template, "text": clean}),)
        emitted: list[str] = []
        streamed_final: str | None = None
        if record.get("type") == "final":
            pending = self.finish()
            if pending is not None:
                emitted.append(pending)
            # A final that falls back to the streamed text (#107, an approval
            # pause), perhaps behind the connector notice, gets the stream's
            # block breaks. It is scrubbed whole, never as its streamed chunks.
            final_text = record.get("text")
            if (
                self._streamed
                and isinstance(final_text, str)
                and final_text.endswith(self._streamed)
            ):
                lead = len(final_text) - len(self._streamed)
                streamed_final = self._scrub_with_breaks(
                    final_text, [lead + at for at in self._streamed_breaks], ""
                )
        for name in _CONTENT_FIELDS & record.keys():
            scrubbed: object
            if (
                name == "result"
                and record.get("type") == "side_effect_flag"
                and isinstance(record[name], dict)
            ):
                scrubbed, altered = self._side_effect_result(
                    cast("dict[str, object]", record[name])
                )
            else:
                scrubbed = self._content(record[name])
                altered = scrubbed != record[name]
            if name == "result" and record.get("type") == "side_effect_flag" and altered:
                # The worker builds the ledger's restore state from this result;
                # a placeholder in it is not a state anything can put back (#1873).
                record["redacted"] = True
            record[name] = scrubbed
        if streamed_final is not None:
            record["text"] = streamed_final
        emitted.append(self._encode(record))
        return tuple(emitted)

    def finish(self) -> str | None:
        """Flush only on normal completion; cancellation discards the instance."""

        if not self._pending or self._pending_record is None:
            return None
        record = {
            **self._pending_record,
            "text": self._stream_text(self._pending, self._pending_breaks),
        }
        self._pending = ""
        self._pending_breaks = []
        self._pending_record = None
        return self._encode(record)


__all__ = [
    "REDACTION_BOUNDARIES",
    "REDACTION_RULES",
    "RedactingLogFilter",
    "RedactionRule",
    "OutboundRedactor",
    "SEALED_ENVELOPE_CONSTANT",
    "collect_held_secrets",
    "is_sealed_envelope",
    "install_stdout_redaction",
    "redact_span_attribute",
    "redact_text",
]
