"""#3823: a boot-env state token is refused once its sandbox claim is released.

The token is still inside its expiry. The worker reports the shared ``cred``
id on ``POST /v1/internal/state/released-credentials``. The state router then
refuses both the broad ``state`` token and the narrow ``state.app`` token.
A second credential that was not released still works.
"""

from __future__ import annotations

import logging
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_internal.sandbox_token import mint
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

CHANNEL = "C0EXAMPLE1"
RELEASE_URL = "/v1/internal/state/released-credentials"
RELEASED = "this sandbox credential has been released"
STATE_LOGGER = "curie_api.routers.state"


def _worker_headers() -> dict[str, str]:
    return {"X-Curie-Worker-Token": get_settings().internal_worker_token}


def _agent(client: Any, auth_headers: dict[str, str]) -> str:
    resp = client.post(
        "/agents",
        json={
            "name": f"boot-cred-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": CHANNEL},
        },
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    aid: str = resp.json()["id"]
    return aid


def _cred() -> str:
    return uuid.uuid4().hex


def _token(aid: str, scope: str, cred: str, *, exp: int | None = None) -> dict[str, str]:
    claims: dict[str, str | None] = {"cred": cred}
    if scope == "state":
        claims["binding"] = f"slack:{CHANNEL}"
        claims["memory"] = "read"
    token = mint(
        get_settings().api_key,
        agent=aid,
        scope=scope,
        exp=exp if exp is not None else int(time.time()) + 3600,
        claims=claims,
    )
    return {"X-API-Key": token}


def _release(client: Any, aid: str, cred: str) -> Any:
    return client.post(
        RELEASE_URL, json={"agent_id": aid, "credential": cred}, headers=_worker_headers()
    )


def test_release_requires_the_worker_token(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    cred = _cred()
    body = {"agent_id": aid, "credential": cred}
    boot = _token(aid, "state", cred)
    for headers in ({}, {"X-Curie-Worker-Token": "nope"}, auth_headers, boot):
        resp = client.post(RELEASE_URL, json=body, headers=headers)
        assert resp.status_code in (401, 403), (headers.keys(), resp.status_code, resp.text)
    still = client.get(f"/agents/{aid}/state/memory", headers=boot)
    assert still.status_code == 200, still.text


def test_released_boot_token_is_refused_before_expiry(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    aid = _agent(client, auth_headers)
    cred = _cred()
    broad = _token(aid, "state", cred)
    narrow = _token(aid, "state.app", cred)
    assert client.get(f"/agents/{aid}/state/memory", headers=broad).status_code == 200
    assert client.get(f"/agents/{aid}/state/notes", headers=narrow).status_code == 200

    released = _release(client, aid, cred)
    assert released.status_code == 204, released.text

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=STATE_LOGGER):
        for headers, path in (
            (broad, f"/agents/{aid}/state/memory"),
            (narrow, f"/agents/{aid}/state/notes"),
        ):
            resp = client.get(path, headers=headers)
            assert resp.status_code == 403, resp.text
            assert RELEASED in str(resp.json().get("detail", ""))

    refusals = [record for record in caplog.records if record.name == STATE_LOGGER]
    assert len(refusals) == 2
    for record in refusals:
        assert record.levelno == logging.WARNING
        assert record.getMessage() == f"state: refused released sandbox credential for agent {aid}"
    assert cred not in caplog.text
    assert broad["X-API-Key"] not in caplog.text
    assert narrow["X-API-Key"] not in caplog.text

    # The platform key is not a sandbox credential.
    assert client.get(f"/agents/{aid}/state/memory", headers=auth_headers).status_code == 200


@pytest.mark.parametrize(("scope", "namespace"), [("state", "memory"), ("state.app", "notes")])
def test_unavailable_credential_check_refuses_without_logging_secrets(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    scope: str,
    namespace: str,
) -> None:
    aid = _agent(client, auth_headers)
    cred = _cred()
    headers = _token(aid, scope, cred)
    path = f"/agents/{aid}/state/{namespace}"
    assert client.get(path, headers=headers).status_code == 200

    # The real ping below observes Redis including this nonexistent socket path
    # in its connection error. Its generated credential id proves that exception
    # text is also excluded without changing the shared Valkey server.
    socket_path = tmp_path / f"{cred}.sock"
    unavailable = Redis(
        unix_socket_path=str(socket_path), socket_connect_timeout=1, socket_timeout=1
    )
    original_valkey = client.app.state.valkey
    try:
        assert client.portal is not None
        with pytest.raises(RedisConnectionError) as connection_error:
            client.portal.call(unavailable.ping)
        assert cred in str(connection_error.value)
        client.app.state.valkey = unavailable
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger=STATE_LOGGER):
            resp = client.get(path, headers=headers)
    finally:
        client.app.state.valkey = original_valkey
        assert client.portal is not None
        client.portal.call(unavailable.aclose)

    assert resp.status_code == 503, resp.text
    assert resp.json()["detail"] == "could not check this sandbox credential"
    failures = [record for record in caplog.records if record.name == STATE_LOGGER]
    assert len(failures) == 1
    assert failures[0].levelno == logging.WARNING
    assert failures[0].getMessage() == (
        f"state: could not check sandbox credential for agent {aid} (ConnectionError)"
    )
    assert cred not in caplog.text
    assert headers["X-API-Key"] not in caplog.text
    assert str(socket_path) not in caplog.text
    assert client.get(path, headers=headers).status_code == 200


def test_an_unreleased_credential_still_reads(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    closed, live = _cred(), _cred()
    assert _release(client, aid, closed).status_code == 204
    live_headers = _token(aid, "state", live)
    assert client.get(f"/agents/{aid}/state/memory", headers=live_headers).status_code == 200
    dead = client.get(f"/agents/{aid}/state/memory", headers=_token(aid, "state", closed))
    assert dead.status_code == 403, dead.text


def test_an_expired_boot_token_is_refused_without_a_release(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    headers = _token(aid, "state", _cred(), exp=int(time.time()) - 5)
    resp = client.get(f"/agents/{aid}/state/memory", headers=headers)
    assert resp.status_code == 401, resp.text
