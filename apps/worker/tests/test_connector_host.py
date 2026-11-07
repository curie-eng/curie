"""Portable host configuration and upload scope through real HTTP sockets."""

from __future__ import annotations

import asyncio
import base64
import json
import time
from pathlib import Path
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_connector_proxy import caller, hosting, server
from curie_worker import caller_token
from nacl.signing import SigningKey

_SEED = base64.b64encode(bytes(range(32))).decode()
_PUBLIC = base64.b64encode(bytes(SigningKey(bytes(range(32))).verify_key)).decode()


def test_upload_minter_and_verifier_share_the_wire_corpus() -> None:
    vector = json.loads(
        (
            Path(__file__).resolve().parents[3] / "tests/vectors/connector-upload-token.json"
        ).read_text()
    )
    token = caller_token.mint_upload(
        vector["seed"], agent=vector["agent"], connector=vector["connector"], exp=vector["exp"]
    )
    assert token == vector["token"]
    keys = (caller.public_key(vector["public"]),)
    for connector, agent, now, refusal in [
        ("reviewer", "acme-dev", vector["exp"] - 1, None),
        ("other", "acme-dev", vector["exp"] - 1, caller.INVALID),
        ("reviewer", "other", vector["exp"] - 1, caller.NOT_ADMITTED),
        ("reviewer", "acme-dev", vector["exp"], caller.EXPIRED),
    ]:
        assert (
            caller.decide_upload(
                keys, vector["token"], connector=connector, admits=frozenset({agent}), now=now
            ).refusal
            == refusal
        )
    assert (
        caller.decide(
            keys, vector["token"], admits=frozenset({"acme-dev"}), now=vector["exp"] - 1
        ).refusal
        == caller.INVALID
    )
    for invalid in vector["refused"]:
        assert (
            caller.decide_upload(
                keys,
                invalid,
                connector="reviewer",
                admits=frozenset({"acme-dev"}),
                now=vector["exp"] - 1,
            ).refusal
            == caller.INVALID
        )


@pytest.mark.parametrize("agent, connector, exp", [("", "c", 1), ("a", "", 1), ("a", "c", True)])
def test_upload_minter_refuses_missing_scope(agent: str, connector: str, exp: int) -> None:
    with pytest.raises(ValueError):
        caller_token.mint_upload(_SEED, agent=agent, connector=connector, exp=exp)


def host_document() -> dict[str, Any]:
    return {
        "version": 1,
        "connector": "reviewer",
        "listener": {"host": "0.0.0.0", "port": 8080, "insecure_http": True},
        "implementation": {"command": ["python3", "/opt/connector/server.py"], "port": 8000},
        "public_keys": [_PUBLIC],
        "admits": ["acme-dev"],
        "gated_tools": ["reviewer/write*"],
        "routes": [{"path": "/review", "methods": ["POST"], "max_body_bytes": 10}],
        "grant_store": {"host": "valkey", "port": 6379, "username": "reviewer"},
        "upload": {
            "listener": {"host": "0.0.0.0", "port": 8081, "insecure_http": True},
            "upstream_port": 8001,
            "path": "/v1/upload",
            "max_body_bytes": 16,
        },
    }


def test_mounted_config_and_secret_env_build_both_listeners(tmp_path: Path) -> None:
    config_file = tmp_path / "serve.json"
    config_file.write_text(json.dumps(host_document()))
    config = hosting.load_config({hosting.CONFIG_ENV: str(config_file)})
    main, upload = config.proxy_configs({hosting.PASSWORD_ENV: "test-password"})
    assert main.connector == "reviewer"
    assert main.admits == frozenset({"acme-dev"})
    assert main.gated_tools == ("reviewer/write*",)
    assert main.routes == (server.RouteConfig("/review", frozenset({"POST"}), 10),)
    assert upload is not None and upload.upload_only
    assert upload.upstream_port == 8001
    with pytest.raises(ValueError, match="password"):
        config.proxy_configs({})


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("version",), True),
        (("version",), 2),
        (("public_keys",), []),
        (("public_keys",), [_SEED + "invalid"]),
        (("public_keys",), [_SEED]),
        (("private_key",), _SEED),
        (("listener", "port"), True),
        (("listener", "host"), "example.com"),
        (("listener", "insecure_http"), False),
        (("listener", "tls"), {"cert_file": "/cert.pem"}),
        (("listener", "tls"), {"cert_file": "/cert.pem", "key_file": "/key.pem"}),
        (("implementation", "port"), 8080),
        (("implementation", "command"), []),
        (("grant_store", "username"), "default"),
        (("grant_store", "tls"), False),
        (("upload", "listener", "port"), 8080),
        (("upload", "listener", "insecure_http"), False),
        (("upload", "path"), "/mcp"),
        (("upload", "path"), "/v1/%75pload"),
        (("routes",), [{"path": "/mcp", "methods": ["GET"], "max_body_bytes": 5}]),
    ],
)
def test_invalid_config_fails_closed(tmp_path: Path, path: tuple[str, ...], value: Any) -> None:
    document = host_document()
    target = document
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    config_file = tmp_path / "serve.json"
    config_file.write_text(json.dumps(document))
    with pytest.raises(ValueError):
        hosting.load_config({hosting.CONFIG_ENV: str(config_file)})


@pytest.mark.parametrize("text", ["{", "[]", '{"version":1,"version":1}', ""])
def test_invalid_json_config_is_refused(tmp_path: Path, text: str) -> None:
    config_file = tmp_path / "serve.json"
    config_file.write_text(text)
    with pytest.raises(ValueError):
        hosting.load_config({hosting.CONFIG_ENV: str(config_file)})


def test_missing_config_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        hosting.load_config({})
    with pytest.raises(ValueError):
        hosting.load_config({hosting.CONFIG_ENV: str(tmp_path / "absent")})


def test_private_worker_seed_is_not_a_verification_point() -> None:
    worker_seed = base64.b64encode(bytes(caller_token.signing_key(_SEED))).decode()
    with pytest.raises(ValueError, match="Ed25519 point"):
        caller.public_key(worker_seed)
    assert (
        caller.public_key(_PUBLIC).encode() == caller_token.signing_key(_SEED).verify_key.encode()
    )


def test_plaintext_requires_explicit_opt_in_even_on_loopback() -> None:
    document = host_document()
    document["listener"] = {"host": "127.0.0.1", "port": 8080}
    with pytest.raises(ValueError):
        hosting.HostConfig.model_validate(document)
    document["listener"]["insecure_http"] = True
    assert hosting.HostConfig.model_validate(document).listener.port == 8080


def test_upload_scope_is_exclusive_and_cannot_call_mcp() -> None:
    async def exercise() -> None:
        seen: list[bytes] = []

        async def implementation(request: web.Request) -> web.Response:
            seen.append(await request.read())
            assert server.GRANT_HEADER not in request.headers
            assert caller.HEADER not in request.headers
            assert request.headers[caller.AGENT_HEADER] == "acme-dev"
            return web.Response(text="uploaded")

        upstream_app = web.Application()
        upstream_app.router.add_route("*", "/{tail:.*}", implementation)
        async with TestServer(upstream_app) as upstream:
            document = host_document()
            document["upload"]["upstream_port"] = upstream.port
            document["implementation"]["port"] = upstream.port
            config = hosting.HostConfig.model_validate(document)
            main_config, upload_config = config.proxy_configs({hosting.PASSWORD_ENV: "test"})
            assert upload_config is not None
            async with (
                TestServer(server.make_app(upload_config)) as upload,
                TestServer(server.make_app(main_config)) as main,
                aiohttp.ClientSession() as client,
            ):
                exp = int(time.time()) + 60
                token = caller_token.mint_upload(
                    _SEED, agent="acme-dev", connector="reviewer", exp=exp
                )
                async with client.post(
                    upload.make_url("/v1/upload"), headers={caller.HEADER: token}, data=b"file"
                ) as response:
                    assert response.status == 200
                    assert await response.text() == "uploaded"
                wrong_scopes = [
                    caller_token.mint(_SEED, agent="acme-dev", exp=exp),
                    caller_token.mint_route(_SEED, agent="acme-dev", connector="reviewer", exp=exp),
                    caller_token.mint_upload(_SEED, agent="other", connector="reviewer", exp=exp),
                    caller_token.mint_upload(_SEED, agent="acme-dev", connector="other", exp=exp),
                    caller_token.mint_upload(
                        _SEED, agent="acme-dev", connector="reviewer", exp=exp - 120
                    ),
                    "",
                ]
                for bad in wrong_scopes:
                    async with client.post(
                        upload.make_url("/v1/upload"), headers={caller.HEADER: bad}, data=b"bad"
                    ) as response:
                        assert response.status == 403
                for target, path, headers, data in [
                    (upload, "/mcp", {caller.HEADER: token}, b"{}"),
                    (upload, "/v1/UPLOAD", {caller.HEADER: token}, b"bad"),
                    (
                        upload,
                        "/v1/upload",
                        {caller.HEADER: token, server.GRANT_HEADER: "ccg.x"},
                        b"bad",
                    ),
                    (upload, "/v1/upload", {caller.HEADER: token}, b"x" * 17),
                    (main, "/mcp", {caller.HEADER: token}, b"{}"),
                    (main, "/review", {caller.HEADER: token}, b"bad"),
                    (
                        main,
                        "/v1/upload",
                        {caller.HEADER: caller_token.mint(_SEED, agent="acme-dev", exp=exp)},
                        b"bad",
                    ),
                    (
                        main,
                        "/v1/UPLOAD",
                        {caller.HEADER: caller_token.mint(_SEED, agent="acme-dev", exp=exp)},
                        b"bad",
                    ),
                ]:
                    async with client.post(
                        target.make_url(path), headers=headers, data=data
                    ) as response:
                        assert response.status in {403, 413}
                assert seen == [b"file"]

    asyncio.run(exercise())
