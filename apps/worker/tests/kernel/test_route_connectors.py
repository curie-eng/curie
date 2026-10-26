"""Which connectors a turn mints route tokens for (ADR 0240 decision 2).

Read from the version's stored bundle, like the channel read grant beside it.
"""

from __future__ import annotations

import asyncio
import io
import json
import zipfile
from types import SimpleNamespace

from curie_worker.config import WorkerConfig
from curie_worker.kernel import route_tokens

_LIMITS = (64 * 1024 * 1024, 200.0, 4096)
_ROUTE = (
    "    routes:\n      - path: /v1/review\n        max_body_bytes: 1048576\n"
    "        callers: [review-tools]\n"
)
_CONNECTORS = (
    "connectors:\n"
    "  reviewer:\n    image: ghcr.io/example/reviewer:1\n"
    + _ROUTE
    + "  grafana:\n    image: ghcr.io/example/grafana:1\n"
    "  remote:\n    url: https://mcp.example.com/mcp\n"
)


def _zip(files: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(".claude-plugin/plugin.json", json.dumps({"name": "acme-bot"}))
        for name, text in files.items():
            archive.writestr(name, text)
    return buffer.getvalue()


class _Bundles:
    def __init__(self, data: dict[str, bytes], *, fail: bool = False) -> None:
        self.data = data
        self.fail = fail
        self.reads: list[str] = []

    def get(self, key: str) -> bytes:
        self.reads.append(key)
        if self.fail:
            raise OSError("the object store is unavailable")
        return self.data[key]


# @spec ADR-0240 d2
def test_only_hosted_connectors_that_declare_routes_are_named() -> None:
    reader = _Bundles({"b1": _zip({"connectors.yaml": _CONNECTORS})})
    assert route_tokens.read_route_connectors(reader, "b1", "acme-dev", _LIMITS) == frozenset(
        {"reviewer"}
    )


# @spec ADR-0240 d2
def test_deploy_yaml_restricts_the_route_connectors_to_the_agent() -> None:
    deploy = (
        "targets:\n"
        "  dev:\n    agent: acme-dev\n    env: dev\n    slack_channel: C0EXAMPLE2\n"
        "    connectors: [grafana]\n"
        "  prod:\n    agent: acme-review\n    env: prod\n    slack_channel: C0EXAMPLE1\n"
        "    connectors: [reviewer]\n"
    )
    reader = _Bundles({"b1": _zip({"connectors.yaml": _CONNECTORS, "deploy.yaml": deploy})})
    assert route_tokens.read_route_connectors(reader, "b1", "acme-dev", _LIMITS) == frozenset()
    assert route_tokens.read_route_connectors(reader, "b1", "acme-review", _LIMITS) == frozenset(
        {"reviewer"}
    )


# @spec ADR-0240 d2
def test_a_bundle_without_valid_connectors_names_none() -> None:
    reader = _Bundles(
        {
            "none": _zip({}),
            "bad": _zip(
                {"connectors.yaml": "connectors:\n  reviewer:\n    image: x:1\n    nope: 1\n"}
            ),
        }
    )
    assert route_tokens.read_route_connectors(reader, "none", "acme-dev", _LIMITS) == frozenset()
    assert route_tokens.read_route_connectors(reader, "bad", "acme-dev", _LIMITS) == frozenset()


_SEED = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="


def _kernel(reader: _Bundles | None, *, key: str = _SEED) -> SimpleNamespace:
    return SimpleNamespace(
        _bundles=reader,
        _bundle_routes={},
        _bundle_route_lookups={},
        _config=WorkerConfig(connector_caller_signing_key=key),
    )


# @spec ADR-0240 d2: no caller key mints nothing, so the bundle is not read.
def test_without_a_caller_key_the_bundle_is_never_read() -> None:
    reader = _Bundles({"b1": _zip({"connectors.yaml": _CONNECTORS})})
    found = asyncio.run(
        route_tokens.route_connectors(_kernel(reader, key=""), "b1", "acme-dev")  # type: ignore[arg-type]
    )
    assert found == frozenset()
    assert reader.reads == []


# @spec ADR-0240 d2
def test_the_lookup_is_cached_by_bundle_and_agent() -> None:
    reader = _Bundles({"b1": _zip({"connectors.yaml": _CONNECTORS})})
    kernel = _kernel(reader)

    async def go() -> None:
        for _ in range(3):
            found = await route_tokens.route_connectors(kernel, "b1", "acme-dev")  # type: ignore[arg-type]
            assert found == frozenset({"reviewer"})
        await route_tokens.route_connectors(kernel, "b1", "acme-other")  # type: ignore[arg-type]

    asyncio.run(go())
    assert reader.reads == ["b1", "b1"]


# @spec ADR-0240 d2: an unreadable bundle mints nothing and fails no turn.
def test_an_unreadable_bundle_names_no_connector_and_is_not_cached() -> None:
    reader = _Bundles({}, fail=True)
    kernel = _kernel(reader)

    async def go() -> None:
        assert await route_tokens.route_connectors(kernel, "b1", "acme-dev") == frozenset()  # type: ignore[arg-type]
        assert await route_tokens.route_connectors(kernel, None, "acme-dev") == frozenset()  # type: ignore[arg-type]
        assert await route_tokens.route_connectors(_kernel(None), "b1", "acme-dev") == frozenset()  # type: ignore[arg-type]

    asyncio.run(go())
    assert kernel._bundle_routes == {}


# @spec ADR-0240 d2: a slow store is read once, not once per turn.
def test_a_timed_out_lookup_is_awaited_again_rather_than_restarted(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    import threading

    release = threading.Event()

    class _Slow(_Bundles):
        def get(self, key: str) -> bytes:
            self.reads.append(key)
            release.wait(5)
            return _zip({"connectors.yaml": _CONNECTORS})

    reader = _Slow({})
    kernel = _kernel(reader)
    monkeypatch.setattr(route_tokens, "_LOOKUP_TIMEOUT_S", 0.05)

    async def go() -> None:
        assert await route_tokens.route_connectors(kernel, "b1", "acme-dev") == frozenset()
        assert await route_tokens.route_connectors(kernel, "b1", "acme-dev") == frozenset()
        release.set()
        monkeypatch.setattr(route_tokens, "_LOOKUP_TIMEOUT_S", 5.0)
        assert await route_tokens.route_connectors(kernel, "b1", "acme-dev") == frozenset(
            {"reviewer"}
        )

    asyncio.run(go())
    assert reader.reads == ["b1"]
