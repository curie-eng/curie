"""The breaker list route: how an operator finds a breaker id to close.

@spec AUTOMATED-REMEDIATION-11 @spec AUTOMATED-REMEDIATION-3

A breaker opens on any outcome other than ``verified`` and closes only through
``POST /agents/{agent_id}/hooks/{hook}/remediation-policy/breakers/{breaker_id}/close``
(operator principal), but nothing lets an operator discover the id. This pins:

* ``GET /agents/{agent_id}/hooks/{hook}/remediation-policy/breakers`` with an
  optional ``state`` query (``open`` the default, ``closed`` or ``all``), the
  same API-key authentication as the other policy routes, no operator
  principal (reads need none: ``show`` is the precedent; a principal is only
  ever demanded of writes) and ``Cache-Control: no-store``;
* the body is a JSON list of the close route's ``RemediationBreakerOut`` rows
  (``id``, ``agent_id``, ``connector``, ``tool``, ``target``, ``opened_at``,
  ``closed_at``, ``closed_by``, ``close_reason``), newest opened first;
* like the close route it lists only breakers on a connector and tool the
  hook's current policy declares, so another hook's breaker is invisible
  here; an unknown agent is ``404`` ``agent_not_found``, an unknown ``state``
  is ``422``;
* a read changes nothing, and the id it lists is the one the close route
  accepts.

Every identifier is a placeholder.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import pytest
from _migration_support import sql_dicts, sql_rows
from _sealed_actions import executor_enabled  # noqa: F401 - fixture, requested by name
from test_remediation_verifier import (
    ACT_CONNECTOR,
    ACT_TOOL,
    HOOK,
    OPERATOR,
    _agent,
    _bind_policy,
    _document,
    _operator,
)

pytestmark = pytest.mark.usefixtures("clean_db", "executor_enabled")

OTHER_CONNECTOR = "example-other"
KEYS = {
    "id",
    "agent_id",
    "connector",
    "tool",
    "target",
    "opened_at",
    "closed_at",
    "closed_by",
    "close_reason",
}


def _path(agent_id: str, hook: str = HOOK) -> str:
    return f"/agents/{agent_id}/hooks/{hook}/remediation-policy/breakers"


def _breaker(
    agent_id: str, target: str, *, connector: str = ACT_CONNECTOR, closed: bool = False,
    age: int = 0,
) -> uuid.UUID:
    breaker_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.remediation_breakers "
        "(id, agent_id, connector, tool, target, opened_at, closed_at, closed_by, close_reason) "
        "VALUES (:id, :agent_id, :connector, :tool, :target, now() - make_interval(secs => :age), "
        "CASE WHEN :closed THEN now() END, CASE WHEN :closed THEN :by END, "
        "CASE WHEN :closed THEN 'fixed' END)",
        {
            "id": breaker_id,
            "agent_id": uuid.UUID(agent_id),
            "connector": connector,
            "tool": ACT_TOOL,
            "target": target,
            "closed": closed,
            "by": OPERATOR,
            "age": age,
        },
    )
    return breaker_id


def _setup(client: Any, headers: dict[str, str], tmp_path: Path) -> str:
    agent_id = _agent(client, headers, tmp_path)
    _bind_policy(agent_id, _document())
    return agent_id


def test_the_list_names_open_breakers_newest_first(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-11: the id an operator needs to close one."""

    agent_id = _setup(client, auth_headers, tmp_path)
    older = _breaker(agent_id, f'{ACT_CONNECTOR}:"example-api"', age=200)
    newer = _breaker(agent_id, f'{ACT_CONNECTOR}:"example-worker"', age=100)
    _breaker(agent_id, f'{ACT_CONNECTOR}:"example-closed"', closed=True, age=300)

    response = client.get(_path(agent_id), headers=auth_headers)

    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    rows = response.json()
    assert [row["id"] for row in rows] == [str(newer), str(older)]
    for row in rows:
        assert set(row) == KEYS
        assert row["agent_id"] == agent_id
        assert row["connector"] == ACT_CONNECTOR
        assert row["tool"] == ACT_TOOL
        assert row["closed_at"] is None and row["closed_by"] is None


def test_state_selects_open_closed_or_all(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-11: ``open`` by default."""

    agent_id = _setup(client, auth_headers, tmp_path)
    opened = _breaker(agent_id, f'{ACT_CONNECTOR}:"example-api"', age=100)
    closed = _breaker(agent_id, f'{ACT_CONNECTOR}:"example-worker"', closed=True, age=200)

    def ids(state: str | None) -> list[str]:
        suffix = "" if state is None else f"?state={state}"
        response = client.get(_path(agent_id) + suffix, headers=auth_headers)
        assert response.status_code == 200, response.text
        return [row["id"] for row in response.json()]

    assert ids(None) == [str(opened)]
    assert ids("open") == [str(opened)]
    assert ids("closed") == [str(closed)]
    assert ids("all") == [str(opened), str(closed)]
    assert client.get(_path(agent_id) + "?state=weird", headers=auth_headers).status_code == 422


def test_a_breaker_on_a_connector_the_policy_does_not_declare_is_not_listed(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-11: the close route's own scope."""

    agent_id = _setup(client, auth_headers, tmp_path)
    _breaker(agent_id, 'example-other:"example-api"', connector=OTHER_CONNECTOR)

    assert client.get(_path(agent_id), headers=auth_headers).json() == []


def test_the_read_needs_the_api_key_but_no_principal_and_an_unknown_agent_is_404(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-3: reads need no principal; the key is required."""

    agent_id = _setup(client, auth_headers, tmp_path)
    assert client.get(_path(agent_id)).status_code == 401
    assert client.get(_path(agent_id), headers=auth_headers).status_code == 200
    missing = client.get(_path(str(uuid.uuid4())), headers=auth_headers)
    assert missing.status_code == 404, missing.text
    assert missing.json()["detail"]["code"] == "agent_not_found"


def test_a_listed_id_is_the_one_the_close_route_accepts_and_the_read_wrote_nothing(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-11: discover, then close, then it is closed."""

    agent_id = _setup(client, auth_headers, tmp_path)
    _breaker(agent_id, f'{ACT_CONNECTOR}:"example-api"')
    before = sql_dicts("SELECT * FROM curie.remediation_breakers")
    listed = client.get(_path(agent_id), headers=auth_headers).json()
    assert sql_dicts("SELECT * FROM curie.remediation_breakers") == before

    closed = client.post(
        f"{_path(agent_id)}/{listed[0]['id']}/close",
        json={"reason": "connector fixed"},
        headers={**auth_headers, **_operator()},
    )

    assert closed.status_code == 200, closed.text
    assert client.get(_path(agent_id), headers=auth_headers).json() == []
    after = client.get(_path(agent_id) + "?state=closed", headers=auth_headers).json()
    assert [row["id"] for row in after] == [listed[0]["id"]]
