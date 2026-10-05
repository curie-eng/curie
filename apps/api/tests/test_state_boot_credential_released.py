"""#3823: a boot-env state token is refused once its sandbox claim is released.

The token is still inside its expiry. The worker reports the shared ``cred``
id on ``POST /v1/internal/state/released-credentials``. The state router then
refuses both the broad ``state`` token and the narrow ``state.app`` token.
A second credential that was not released still works.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

from curie_api.config import get_settings
from curie_internal.sandbox_token import mint

CHANNEL = "C0EXAMPLE1"
RELEASE_URL = "/v1/internal/state/released-credentials"
RELEASED = "this sandbox credential has been released"


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
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    cred = _cred()
    broad = _token(aid, "state", cred)
    narrow = _token(aid, "state.app", cred)
    assert client.get(f"/agents/{aid}/state/memory", headers=broad).status_code == 200
    assert client.get(f"/agents/{aid}/state/notes", headers=narrow).status_code == 200

    released = _release(client, aid, cred)
    assert released.status_code == 204, released.text

    for headers, path in (
        (broad, f"/agents/{aid}/state/memory"),
        (narrow, f"/agents/{aid}/state/notes"),
    ):
        resp = client.get(path, headers=headers)
        assert resp.status_code == 403, resp.text
        assert RELEASED in str(resp.json().get("detail", ""))

    # The platform key is not a sandbox credential.
    assert client.get(f"/agents/{aid}/state/memory", headers=auth_headers).status_code == 200


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
