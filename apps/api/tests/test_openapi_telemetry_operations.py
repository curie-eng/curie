"""Every API route is declared in the closed HTTP telemetry `operation` domain.

The API labels its HTTP server metrics with the matched route template
(`operation`), and the telemetry manifest (ADR-0076) declares that label's
domain as a closed list. A route missing from the list makes every request to
it raise in the metrics layer, and no database-free test noticed (#1461: the
memory guidance route). This pins the committed OpenAPI paths against the
declared domain of each HTTP server metric, with no database and no server.
"""

import json
from collections.abc import Iterable, Mapping
from typing import Any

from curie_api.export_openapi import openapi_path
from curie_telemetry.metrics import declared_metric_manifest

HTTP_SERVER_METRICS = (
    "curie.http.server.request",
    "curie.http.server.request.duration",
    "curie.http.server.active",
)


def openapi_paths() -> set[str]:
    document = json.loads(openapi_path().read_text(encoding="utf-8"))
    return set(document["paths"])


def missing_operations(paths: Iterable[str], manifest: Mapping[str, Any]) -> dict[str, list[str]]:
    """Map each HTTP server metric to the paths its `operation` domain lacks."""
    missing: dict[str, list[str]] = {}
    for name in HTTP_SERVER_METRICS:
        domain = set(manifest["metrics"][name]["attributes"]["operation"])
        absent = sorted(set(paths) - domain)
        if absent:
            missing[name] = absent
    return missing


def test_openapi_has_paths() -> None:
    # Guards the guard: an empty path set would make the check below vacuous.
    assert len(openapi_paths()) > 50


def test_every_openapi_path_is_a_declared_http_operation() -> None:
    missing = missing_operations(openapi_paths(), declared_metric_manifest())
    assert not missing, (
        "apps/api/openapi.json has routes the telemetry `operation` domain does not "
        "declare, so every request to them raises in the metrics layer. Add them to "
        "_HTTP_OPERATIONS in packages/telemetry/src/curie_telemetry/metrics.py and "
        f"regenerate packages/telemetry/schema/metrics.json: {missing}"
    )


def test_a_missing_route_is_named() -> None:
    manifest = declared_metric_manifest()
    for name in HTTP_SERVER_METRICS:
        manifest["metrics"][name]["attributes"]["operation"].remove(
            "/agents/{agent_id}/memory/guidance"
        )
    assert missing_operations(openapi_paths(), manifest) == {
        name: ["/agents/{agent_id}/memory/guidance"] for name in HTTP_SERVER_METRICS
    }
