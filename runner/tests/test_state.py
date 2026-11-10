"""The ``curie-state`` MCP server (#249): the client, the tool ops, the wiring.

The StateApiClient and every tool op are exercised against a tiny in-memory fake
of the #248 state endpoints (GET/PUT/POST-append/DELETE a key, GET a namespace),
so get / set / list / delete / append and the compare-and-set path are verified
end-to-end over real HTTP without the API. A separate durability check writes a
value, tears the client down, and reads it back through a fresh client against
the same fake store -- the unit-testable stand-in for the suspend/resume survival
the acceptance criterion needs a live cluster to prove end to end.
"""

from __future__ import annotations

from typing import Any

import anyio
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner.state import (
    RESERVED_NAMESPACES,
    StateApiClient,
    build_state_server,
    op_append,
    op_delete,
    op_get,
    op_list,
    op_set,
    resolve_state_client,
)
from curie_runner.tool_names import STATE_SERVER_NAME


def _fake_state_app() -> tuple[web.Application, dict]:
    """A minimal fake of the state router under /agents/A/state/<ns>/<key>.

    Stores ``{(namespace, key): {"value", "version"}}`` in a dict. Supports the
    five verbs the client uses, including compare-and-set on PUT (a mismatched
    expected_version is a 409, matching apps/api's state router) and the
    array-append contract (create-as-[item], else append to the stored array).
    """
    store: dict[tuple[str, str], dict[str, Any]] = {}
    app = web.Application()

    async def get_key(request: web.Request) -> web.Response:
        ns, key = request.match_info["ns"], request.match_info["key"]
        entry = store.get((ns, key))
        if entry is None:
            return web.json_response({"detail": "not found"}, status=404)
        return web.json_response(
            {"namespace": ns, "key": key, "value": entry["value"], "version": entry["version"]}
        )

    async def put_key(request: web.Request) -> web.Response:
        ns, key = request.match_info["ns"], request.match_info["key"]
        body = await request.json()
        entry = store.get((ns, key))
        expected = body.get("expected_version")
        if entry is None:
            if expected is not None:
                return web.json_response({"detail": "version mismatch"}, status=409)
            store[(ns, key)] = {"value": body["value"], "version": 0}
        else:
            if expected is not None and expected != entry["version"]:
                return web.json_response({"detail": "version mismatch"}, status=409)
            entry["value"] = body["value"]
            entry["version"] += 1
        stored = store[(ns, key)]
        return web.json_response(
            {"namespace": ns, "key": key, "value": stored["value"], "version": stored["version"]}
        )

    async def append_key(request: web.Request) -> web.Response:
        ns, key = request.match_info["ns"], request.match_info["key"]
        body = await request.json()
        entry = store.get((ns, key))
        if entry is None:
            store[(ns, key)] = {"value": [body["item"]], "version": 0}
        else:
            if not isinstance(entry["value"], list):
                return web.json_response({"detail": "not an array"}, status=409)
            entry["value"] = [*entry["value"], body["item"]]
            entry["version"] += 1
        stored = store[(ns, key)]
        return web.json_response(
            {"namespace": ns, "key": key, "value": stored["value"], "version": stored["version"]}
        )

    async def delete_key(request: web.Request) -> web.Response:
        ns, key = request.match_info["ns"], request.match_info["key"]
        store.pop((ns, key), None)
        return web.Response(status=204)

    async def list_ns(request: web.Request) -> web.Response:
        ns = request.match_info["ns"]
        entries = [
            {"namespace": ns, "key": k, "value": v["value"], "version": v["version"]}
            for (n, k), v in sorted(store.items())
            if n == ns
        ]
        return web.json_response(entries)

    app.router.add_get("/agents/A/state/{ns}/{key}", get_key)
    app.router.add_put("/agents/A/state/{ns}/{key}", put_key)
    app.router.add_post("/agents/A/state/{ns}/{key}/append", append_key)
    app.router.add_delete("/agents/A/state/{ns}/{key}", delete_key)
    app.router.add_get("/agents/A/state/{ns}", list_ns)
    return app, store


def _base(server: TestServer) -> str:
    return str(server.make_url("/agents/A/state"))


# --- resolution --------------------------------------------------------------


def test_resolve_absent_url_is_none() -> None:
    assert resolve_state_client({}) is None


def test_resolve_present_url_builds_client() -> None:
    client = resolve_state_client(
        {"CURIE_STATE_URL": "http://api:8000/agents/A/state", "CURIE_STATE_TOKEN": "k"}
    )
    assert isinstance(client, StateApiClient)


# --- client round-trips ------------------------------------------------------


def test_set_then_get_round_trip() -> None:
    app, _ = _fake_state_app()

    async def go() -> None:
        async with TestServer(app) as server:
            client = StateApiClient(_base(server), token="k")
            put = await client.set("workflow", "step", {"stage": 1}, expected_version=None)
            assert put == {"value": {"stage": 1}, "version": 0}
            got = await client.get("workflow", "step")
            assert got == {"value": {"stage": 1}, "version": 0}

    anyio.run(go)


def test_keys_with_url_special_chars_do_not_collide() -> None:
    # #851: '#'/'?'/space in a key are URL syntax to aiohttp -- without escaping,
    # "config#draft" truncates to "config" and the two silently share one slot.
    # Faithful round trip: aiohttp's real client URL handling + a router that
    # decodes path params exactly as the FastAPI state route does.
    app, store = _fake_state_app()

    async def go() -> None:
        async with TestServer(app) as server:
            client = StateApiClient(_base(server), token="k")
            specials = ["config#draft", "report?v=2", "with space"]
            for key in specials:
                await client.set("prefs", key, {"k": key}, expected_version=None)
            # A distinct plain key must NOT read back a special-char key's value.
            await client.set("prefs", "config", {"k": "plain"}, expected_version=None)
            assert (await client.get("prefs", "config#draft"))["value"] == {"k": "config#draft"}
            assert (await client.get("prefs", "config"))["value"] == {"k": "plain"}
            # Every key landed in its own row, none clobbered.
            for key in [*specials, "config"]:
                assert ("prefs", key) in store

    anyio.run(go)


def test_key_url_percent_encodes_both_segments() -> None:
    # Unit guard (#851): special chars become %-escapes (incl. '/'), and two keys
    # that would have collided now compose distinct URLs.
    client = StateApiClient("http://api/agents/A/state", token=None)
    url = client._key_url("ns#x", "config#draft")
    assert url == "http://api/agents/A/state/ns%23x/config%23draft"
    assert "%2F" in client._key_url("ns", "a/b")
    assert client._key_url("ns", "config#draft") != client._key_url("ns", "config")


def test_get_missing_key_is_none() -> None:
    app, _ = _fake_state_app()

    async def go() -> None:
        async with TestServer(app) as server:
            client = StateApiClient(_base(server), token=None)
            assert await client.get("workflow", "absent") is None

    anyio.run(go)


def test_set_with_stale_version_conflicts() -> None:
    app, _ = _fake_state_app()

    async def go() -> None:
        async with TestServer(app) as server:
            client = StateApiClient(_base(server), token="k")
            await client.set("workflow", "step", "a", expected_version=None)  # version 0
            # A correct CAS bumps to version 1.
            await client.set("workflow", "step", "b", expected_version=0)
            # A stale expected_version is refused by the store (409 -> StateError).
            result = await op_set(
                client,
                {"namespace": "workflow", "key": "step", "value": "c", "expected_version": 0},
            )
            assert result.get("is_error") is True

    anyio.run(go)


def test_append_creates_then_extends_array() -> None:
    app, _ = _fake_state_app()

    async def go() -> None:
        async with TestServer(app) as server:
            client = StateApiClient(_base(server), token="k")
            first = await client.append("events", "log", {"n": 1})
            assert first["value"] == [{"n": 1}]
            second = await client.append("events", "log", {"n": 2})
            assert second["value"] == [{"n": 1}, {"n": 2}]

    anyio.run(go)


def test_list_returns_namespace_entries() -> None:
    app, _ = _fake_state_app()

    async def go() -> None:
        async with TestServer(app) as server:
            client = StateApiClient(_base(server), token="k")
            await client.set("workflow", "a", 1, expected_version=None)
            await client.set("workflow", "b", 2, expected_version=None)
            entries = await client.list("workflow")
            assert [(e["key"], e["value"]) for e in entries] == [("a", 1), ("b", 2)]

    anyio.run(go)


def test_delete_removes_key() -> None:
    app, store = _fake_state_app()

    async def go() -> None:
        async with TestServer(app) as server:
            client = StateApiClient(_base(server), token="k")
            await client.set("workflow", "step", "v", expected_version=None)
            await client.delete("workflow", "step")
            assert ("workflow", "step") not in store
            # Deleting an absent key is idempotent, not an error.
            await client.delete("workflow", "absent")

    anyio.run(go)


def test_value_survives_a_fresh_client_against_the_same_store() -> None:
    """The suspend/resume survival property, unit-testable slice.

    A value written through one client is read back through a SEPARATE client
    (a new aiohttp session, as a resumed pod would be) against the same durable
    store -- nothing rides in-process. The live cross-suspend cycle needs a
    cluster; this proves the store-backed persistence the survival depends on.
    """
    app, _ = _fake_state_app()

    async def go() -> None:
        async with TestServer(app) as server:
            writer = StateApiClient(_base(server), token="k")
            await writer.set("workflow", "cursor", {"offset": 42}, expected_version=None)
            reader = StateApiClient(_base(server), token="k")
            assert await reader.get("workflow", "cursor") == {
                "value": {"offset": 42},
                "version": 0,
            }

    anyio.run(go)


# --- tool ops (validate -> call -> format) -----------------------------------


def test_op_get_reports_found_flag() -> None:
    app, _ = _fake_state_app()

    async def go() -> None:
        async with TestServer(app) as server:
            client = StateApiClient(_base(server), token="k")
            missing = await op_get(client, {"namespace": "workflow", "key": "x"})
            assert '"found": false' in missing["content"][0]["text"]
            await client.set("workflow", "x", "v", expected_version=None)
            found = await op_get(client, {"namespace": "workflow", "key": "x"})
            assert '"found": true' in found["content"][0]["text"]

    anyio.run(go)


def test_ops_refuse_reserved_namespaces() -> None:
    app, _ = _fake_state_app()

    async def go() -> None:
        async with TestServer(app) as server:
            client = StateApiClient(_base(server), token="k")
            for ns in RESERVED_NAMESPACES:
                for coro in (
                    op_get(client, {"namespace": ns, "key": "log"}),
                    op_set(client, {"namespace": ns, "key": "log", "value": 1}),
                    op_append(client, {"namespace": ns, "key": "log", "item": 1}),
                    op_list(client, {"namespace": ns}),
                    op_delete(client, {"namespace": ns, "key": "log"}),
                ):
                    result = await coro
                    assert result.get("is_error") is True
                    assert "reserved" in result["content"][0]["text"]

    anyio.run(go)


def test_op_refuses_empty_namespace() -> None:
    app, _ = _fake_state_app()

    async def go() -> None:
        async with TestServer(app) as server:
            client = StateApiClient(_base(server), token="k")
            result = await op_list(client, {"namespace": ""})
            assert result.get("is_error") is True

    anyio.run(go)


def test_op_surfaces_transport_failure_as_error() -> None:
    # No server listening on this port: the client raises, the op returns is_error.
    client = StateApiClient("http://127.0.0.1:1/agents/A/state", token="k")
    result = anyio.run(op_get, client, {"namespace": "workflow", "key": "x"})
    assert result.get("is_error") is True


# --- server wiring -----------------------------------------------------------


def test_state_server_config_shape() -> None:
    client = StateApiClient("http://api:8000/agents/A/state", token="k")
    config = build_state_server(client)
    assert config["type"] == "sdk"
    assert config["name"] == STATE_SERVER_NAME == "curie-state"


def test_state_server_exposes_the_five_verbs() -> None:
    import mcp.types as mcp_types

    client = StateApiClient("http://api:8000/agents/A/state", token="k")
    config = build_state_server(client)
    entry = config["instance"].get_request_handler("tools/list")
    assert entry is not None

    async def names() -> set[str]:
        result = await entry.handler(None, mcp_types.PaginatedRequestParams())
        return {t.name for t in result.tools}

    assert anyio.run(names) == {"get", "set", "append", "list", "delete"}


def test_token_is_never_in_the_client_repr_or_headers_key_logging() -> None:
    """The scoped token must not leak: it lives only in the X-API-Key header."""
    client = StateApiClient("http://api:8000/agents/A/state", token="super-secret")
    # The token is not exposed on a plain repr/str of the client.
    assert "super-secret" not in repr(client)
    headers = client._headers()
    assert headers["X-API-Key"] == "super-secret"
