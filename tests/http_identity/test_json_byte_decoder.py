"""Shared decoding seam without policy validation. @spec HTTP-IDENTITY-V1-3."""

from __future__ import annotations

import importlib
import json
import sys
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest


def decoder() -> Callable[..., Any]:
    """@spec HTTP-IDENTITY-V1-3. Resolve the new public seam inside test bodies."""
    module = importlib.import_module("curie_api.schemas.http_identity")
    result = getattr(module, "decode_json_bytes", None)
    assert callable(result), "public decode_json_bytes is not implemented"
    return result


class ByteDocument(bytes):
    """@spec HTTP-IDENTITY-V1-3. Existing bytes-instance acceptance includes subclasses."""


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (b"null", None),
        (b" true \n", True),
        (b"-42", -42),
        (
            b'{"items":[1,false,null],"name":"synthetic"}',
            {"items": [1, False, None], "name": "synthetic"},
        ),
        (ByteDocument(b"[]"), []),
    ],
)
def test_decode_ordinary_values_without_envelope_validation(raw: bytes, expected: Any) -> None:
    """@spec HTTP-IDENTITY-V1-3. Decoding alone does not require a version or model."""
    assert decoder()(raw, max_bytes=len(raw)) == expected


@pytest.mark.parametrize("padding", [0, 3])
def test_explicit_inclusive_limit_counts_utf8_bytes(padding: int) -> None:
    """@spec HTTP-IDENTITY-V1-3. The independent limit is bytes including whitespace."""
    decode = decoder()
    raw = b'"\xe2\x98\x83"' + b" " * padding
    assert len(raw) > len(raw.decode("utf-8"))
    assert decode(raw, max_bytes=len(raw)) == "\N{SNOWMAN}"
    with pytest.raises((ValueError, TypeError)):
        decode(raw, max_bytes=len(raw) - 1)


@pytest.mark.parametrize(
    "raw",
    [
        "{}",
        None,
        bytearray(b"{}"),
        memoryview(b"{}"),
        b"\xef\xbb\xbf{}",
        b'"\xff"',
        b'{"a":1,"a":2}',
        b'{"items":[{"a":1,"a":2}]}',
        b"[NaN]",
        b"[Infinity]",
        b"[-Infinity]",
        rb'{"items":["\ud800"]}',
        rb'{"\udfff":0}',
        b"{} false",
        b'{"incomplete":',
        b"",
    ],
)
def test_invalid_byte_documents_raise_without_a_sanitized_refusal_promise(raw: Any) -> None:
    """@spec HTTP-IDENTITY-V1-3. Every consuming boundary still owns refusal mapping."""
    decode = decoder()
    with pytest.raises((ValueError, TypeError)):
        decode(raw, max_bytes=64)


@pytest.mark.parametrize("bound", [True, False, -1, 1.0, "8", None])
def test_invalid_programmer_bound_raises_value_error(bound: Any) -> None:
    """@spec HTTP-IDENTITY-V1-3. Bounds require an actual nonnegative integer."""
    decode = decoder()
    with pytest.raises(ValueError):
        decode(b"0", max_bytes=bound)


@pytest.mark.parametrize("raw", [b"0", b"null", b""])
def test_zero_bound_admits_no_json_document(raw: bytes) -> None:
    """@spec HTTP-IDENTITY-V1-3. Empty input does not become a successful decoded value."""
    decode = decoder()
    with pytest.raises((ValueError, TypeError)):
        decode(raw, max_bytes=0)


@pytest.mark.parametrize("consumer", ["facts", "policy", "change"])
@pytest.mark.parametrize(
    ("raw", "unsupported"),
    [
        (b'{"schema_version":2,"unrelated":1.5}', True),
        (b'{"unrelated":1e999999,"schema_version":2}', True),
        (b'{"schema_version":1.0}', False),
        (b'{"schema_version":1e0}', False),
        (b'{"schema_version":' + b"9" * 5_000 + b',"unrelated":0.5}', True),
    ],
    ids=["decimal-content", "exponent-content", "decimal-version", "exponent-version", "huge"],
)
def test_actual_consumers_keep_deferred_number_version_precedence(
    consumer: str, raw: bytes, unsupported: bool
) -> None:
    """@spec HTTP-IDENTITY-V1-3 AGENT-ROLE-FILTER-V1-6. Assert public outcomes, not markers."""
    previous = sys.get_int_max_str_digits()
    try:
        if consumer == "facts":
            module = importlib.import_module("curie_api.schemas.http_identity")
            vectors = json.loads(
                (Path(__file__).resolve().parents[1] / "vectors/http-identity-v1.json").read_text()
            )
            result = module.validate_lookup(
                raw,
                module.LookupRequestV1.model_validate(vectors["request"]),
                module.FreshnessLimits.model_validate(vectors["limits"]),
                now=datetime.fromisoformat(vectors["now"]),
            )
            assert isinstance(result, module.FactsRefusal)
            code = "unsupported_version" if unsupported else "invalid_response"
        else:
            module = importlib.import_module("curie_api.agent_role_filter")
            models = importlib.import_module("curie_api.schemas.agent_access")
            result = getattr(module, f"parse_agent_role_{consumer}_v1")(raw)
            assert isinstance(result, models.RoleFilterRefusal)
            code = (
                "unsupported_agent_role_configuration_version"
                if unsupported
                else "invalid_agent_role_configuration"
            )
        assert result.model_dump(mode="json") == {"code": code}
    finally:
        assert sys.get_int_max_str_digits() == previous
