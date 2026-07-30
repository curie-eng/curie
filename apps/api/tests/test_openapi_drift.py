"""The committed OpenAPI document must match the live app."""

from curie_api.export_openapi import openapi_path, render_openapi


def test_committed_openapi_is_current() -> None:
    committed = openapi_path().read_text(encoding="utf-8")
    assert render_openapi() == committed, (
        "apps/api/openapi.json is stale; run `uv run python -m curie_api.export_openapi` and commit"
    )
