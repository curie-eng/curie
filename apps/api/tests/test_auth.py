"""Auth and health-endpoint behavior."""

import uuid
from typing import Any

from curie_api.config import get_settings
from curie_api.sandbox_token import mint


def test_health_is_open(client: Any) -> None:
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_agents_require_api_key(client: Any) -> None:
    assert client.get("/agents").status_code == 401
    assert client.get("/agents", headers={"X-API-Key": "wrong"}).status_code == 401


def test_agents_accept_valid_key(client: Any, auth_headers: dict[str, str], clean_db: None) -> None:
    assert client.get("/agents", headers=auth_headers).status_code == 200


def test_scoped_state_token_is_rejected_on_a_crud_route(client: Any) -> None:
    # A scoped sandbox "state" token authorizes the state namespace only; it must
    # be rejected by the shared require_api_key guard on every other route, so the
    # rejection is not special-cased to approvals (#410).
    token = mint(
        get_settings().api_key,
        agent=str(uuid.uuid4()),
        scope="state",
        exp=4102444800,  # 2100-01-01, valid at test time
    )
    assert client.get("/agents", headers={"X-API-Key": token}).status_code == 401
