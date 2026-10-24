"""Standalone provider artifacts. @spec HTTP-IDENTITY-V1-5."""

import json
from pathlib import Path
from typing import Any

from pydantic.json_schema import models_json_schema

from .schemas.http_identity import MAX_RESPONSE_BYTES, LookupRequestV1, LookupResponseV1


def _schemas(ref_template: str) -> dict[str, Any]:
    """@spec HTTP-IDENTITY-V1-5."""
    _, schema = models_json_schema(
        [(LookupRequestV1, "validation"), (LookupResponseV1, "validation")],
        ref_template=ref_template,
    )
    return dict(schema["$defs"])


def _render(value: dict[str, Any]) -> bytes:
    """@spec HTTP-IDENTITY-V1-5."""
    return (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True) + "\n").encode("utf-8")


def render_json_schema() -> bytes:
    """@spec HTTP-IDENTITY-V1-5."""
    return _render({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://curietech.ai/schemas/http-identity-v1.schema.json",
        "title": "HTTP identity facts v1",
        "$defs": _schemas("#/$defs/{model}"),
    })


def render_openapi() -> bytes:
    """@spec HTTP-IDENTITY-V1-5, HTTP-IDENTITY-V1-6."""
    return _render({
        "openapi": "3.1.0",
        "info": {
            "title": "HTTP identity facts provider",
            "version": "1",
            "description": (
                "Provider-hosted reference operation; deployment maps its lookup URL to "
                "this operation. This document registers no Curie API route. Facts grant "
                "no permission. Transport obligations are published but not implemented "
                "by the pure facts contract. Cross-object issuer equality, exact request "
                "echo and independent freshness require semantic validation. Enable "
                "date-time format assertions for calendar validation of observation "
                "timestamps; patterns retain UTC syntax and fractional precision."
            ),
        },
        "x-curie-transport": {
            "total_deadline_ms": 2000,
            "retries": 0,
            "max_response_bytes": MAX_RESPONSE_BYTES,
            "follow_redirects": False,
            "positive_identity_cache": False,
            "transport_implemented": False,
        },
        "paths": {"/lookup": {"post": {
            "operationId": "lookupIdentityFactsV1",
            "summary": "Resolve captured sender scope to identity facts",
            "requestBody": {
                "required": True,
                "content": {"application/json": {
                    "schema": {"$ref": "#/components/schemas/LookupRequestV1"},
                }},
            },
            "responses": {"200": {
                "description": "Identity facts envelope; unavailable carries no usable facts",
                "content": {"application/json": {
                    "schema": {"$ref": "#/components/schemas/LookupResponseV1"},
                }},
            }},
        }}},
        "components": {"schemas": _schemas("#/components/schemas/{model}")},
    })


def write_artifacts() -> tuple[Path, Path]:
    """@spec HTTP-IDENTITY-V1-5."""
    root = Path(__file__).resolve().parents[4]
    destination = root / "docs" / "interfaces" / "http-identity"
    destination.mkdir(parents=True, exist_ok=True)
    openapi = destination / "http-identity-v1.openapi.json"
    schema = destination / "http-identity-v1.schema.json"
    openapi.write_bytes(render_openapi())
    schema.write_bytes(render_json_schema())
    return openapi, schema


if __name__ == "__main__":
    for artifact in write_artifacts():
        print(f"wrote {artifact}")
