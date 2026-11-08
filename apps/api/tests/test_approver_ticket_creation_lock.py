"""ADR 0271 decision 2 at the final commit: creation and registration serialize.

@spec REQUEST-APPROVAL-2.

Both creation owners judge the route map again on the agent row locked in the
inserting transaction and hold that lock to the commit; the selector
registration writer takes the same row lock. Each test interleaves a real
creation with a real registration through events, never sleeps:

1. The registration commits after the creation's first check and before its
   final one: for the HTTP owner between phase one and its identity writer,
   for the legacy owner before its classification takes the identity reader
   lock (route writes run inside the identity writer fence, so those are the
   windows). The final check refuses with the route-lock code, before
   storage's own guard is ever reached, and no row commits.
2. The creation pauses after its final check, before its insert. A probe that
   locks the agent row like the registration writer cannot get it, so the
   registration waits; the ordinary row commits first and the registration
   lands after it, after which the same request is refused.

The legacy owner creates only for an agent outside HTTP identity selection,
whose requester flag the flag writer refuses to turn on. Storage alone can put
the two together, so its flag is set with SQL; every other step, the
registration included, goes through the operator routes.
"""

from __future__ import annotations

import asyncio
import threading
import uuid
from collections.abc import AsyncIterator, Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager, contextmanager
from types import SimpleNamespace
from typing import Any

import pytest
from _migration_support import sql_dicts, sql_rows
from approver_ticket_support import (
    CEILING_FILTER,
    LIVE,
    OTHER_ROUTE,
    ROUTE,
    agent_state,
    api_headers,
    approval_rows,
    carried,
    create,
    dynamic_request,
    enroll,
    exactly,
    ingress,
    legacy,
    refused_with,
    register,
    route_map,
    selector_key,
    serve,
    set_approver_role,
    write_routes,
)
from approver_ticket_support import operator_case as operator_case
from approver_ticket_support import synthetic_provider as synthetic_provider
from approver_ticket_support import ticket_case as ticket_case
from curie_api import identity_fence
from curie_api.config import get_settings
from curie_api.crud import approvals as crud_approvals
from curie_api.identity import http_approval
from curie_api.routers import approvals as approvals_router
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from apps.api.tests.test_http_approval_creation import accepted

PLAIN: dict[str, Any] = {"target": "acct-example-1"}
WAIT_S = 30


class Pause:
    """One paused call: ``reached`` when it gets there, resumes on ``proceed``."""

    def __init__(self) -> None:
        self.reached, self.proceed = threading.Event(), threading.Event()
        self.calls = 0

    async def hold(self) -> None:
        self.calls += 1
        if self.calls == 1:
            self.reached.set()
            assert await asyncio.to_thread(self.proceed.wait, WAIT_S), "never released"


@contextmanager
def background(call: Callable[[], Any]) -> Iterator[Any]:
    """Run one blocking request on its own thread; yields its future."""
    with ThreadPoolExecutor(max_workers=1) as pool:
        yield pool.submit(call)


def agent_row_is_locked(agent_id: str) -> bool:
    """Whether another transaction holds the agent row the registration writer locks."""

    async def probe() -> bool:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(text("SET LOCAL lock_timeout = '200ms'"))
                await connection.execute(
                    text("SELECT 1 FROM curie.agents WHERE id = :id FOR UPDATE"),
                    {"id": uuid.UUID(agent_id)},
                )
            return False
        except DBAPIError as error:
            assert "lock" in str(error).lower(), error
            return True
        finally:
            await engine.dispose()

    return asyncio.run(probe())


def pause_before_writer(monkeypatch: pytest.MonkeyPatch, pause: Pause) -> None:
    """Hold the HTTP owner between phase one and its identity writer."""
    original = identity_fence.mutation

    @asynccontextmanager
    async def mutation(*args: Any, **kwargs: Any) -> AsyncIterator[Any]:
        await pause.hold()
        async with original(*args, **kwargs) as writer:
            yield writer

    monkeypatch.setattr(
        http_approval, "identity_fence",
        SimpleNamespace(**{**vars(identity_fence), "mutation": mutation}),
    )


def pause_before_insert(monkeypatch: pytest.MonkeyPatch, pause: Pause) -> None:
    """Hold either owner after its final route check, before its insert and commit."""
    original = crud_approvals.insert_approval

    async def insert_approval(*args: Any, **kwargs: Any) -> Any:
        await pause.hold()
        return await original(*args, **kwargs)

    monkeypatch.setattr(crud_approvals, "insert_approval", insert_approval)


# --- the HTTP creation owner -----------------------------------------------------------


def looser(invocation: SimpleNamespace) -> dict[str, Any]:
    """A ticketless creation of the soon-registered tool on the looser bound route."""
    return dynamic_request(invocation, PLAIN, route=OTHER_ROUTE, gate_kind="permission")


def test_http_creation_refuses_a_registration_landing_before_its_writer(
    ticket_case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec REQUEST-APPROVAL-2: the writer's locked map is the one the commit is judged on."""
    case = ticket_case
    agent = case.scope["agent"]
    request, pause = looser(case), Pause()
    with monkeypatch.context() as patched:
        pause_before_writer(patched, pause)
        with background(lambda: create(case.client, request)) as creation:
            assert pause.reached.wait(WAIT_S)
            enroll(case)
            pause.proceed.set()
            exactly(creation.result(WAIT_S), 409, "approval_route_locked")
    assert approval_rows(agent) == []
    owning = ingress(case)
    dynamic = dynamic_request(owning, carried(case, PLAIN))
    accepted(create(case.client, dynamic), dynamic)
    assert [row["selection_mode"] for row in approval_rows(agent)] == ["dynamic"]


def test_http_creation_holds_the_agent_row_until_it_commits(
    ticket_case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec REQUEST-APPROVAL-2: a registration waits for an in-flight ordinary creation."""
    case = ticket_case
    agent = case.scope["agent"]
    request, pause = looser(case), Pause()
    with monkeypatch.context() as patched:
        pause_before_insert(patched, pause)
        with background(lambda: create(case.client, request)) as creation:
            assert pause.reached.wait(WAIT_S)
            assert agent_row_is_locked(agent)
            pause.proceed.set()
            accepted(creation.result(WAIT_S), request)
    assert not agent_row_is_locked(agent)
    enroll(case)
    assert [row["selection_mode"] for row in approval_rows(agent)] == ["ordinary"]
    again = looser(ingress(case))
    exactly(create(case.client, again), 409, "approval_route_locked")


# --- the legacy creation owner ---------------------------------------------------------


def legacy_agent(client: Any) -> dict[str, Any]:
    """A registrable agent whose approvals the legacy owner still creates."""
    address = "C0EXAMPLELEGACY"
    created = client.post(
        "/agents",
        json={"name": "legacy-lock-" + uuid.uuid4().hex[:8],
              "channel": {"kind": "slack", "address": address}},
        headers=api_headers(),
    )
    assert created.status_code == 201, created.text
    agent = str(created.json()["id"])
    tenant = sql_dicts(
        "SELECT tenant_id FROM curie.agents WHERE id = :id", {"id": uuid.UUID(agent)},
    )[0]["tenant_id"]
    scope: dict[str, Any] = {
        "agent": agent, "tenant": str(tenant), "address": address, "role_version": 0,
    }
    bound = write_routes(client, agent, route_map(address))
    assert bound.status_code == 200, bound.text
    serve(client, agent, ((LIVE, ROUTE), ("Bash", OTHER_ROUTE)))
    sql_rows(
        "UPDATE curie.agents SET connector_requester_identity = true WHERE id = :id",
        {"id": uuid.UUID(agent)},
    )
    scope["config_version"] = agent_state(agent)["version"]
    set_approver_role(scope, CEILING_FILTER)
    return scope


def legacy_body(scope: dict[str, Any]) -> dict[str, Any]:
    address = scope["address"]
    return {
        "agent_id": scope["agent"], "conversation_id": address + ":synthetic-thread",
        "author": "U0EXAMPLE2", "summary": "Approve the synthetic access change",
        "reply_kind": "slack", "reply_channel": address,
        "reply_placeholder": "1760000000.000200",
        "dedupe_key": "synthetic-legacy-" + uuid.uuid4().hex, "route": OTHER_ROUTE,
        "gate_kind": "permission", "granted_tool": LIVE, "granted_arguments": PLAIN,
    }


def pause_before_classification(monkeypatch: pytest.MonkeyPatch, pause: Pause) -> None:
    """Hold the legacy owner after its first route check, before it takes the reader lock.

    Classification takes the identity reader lock to the commit, and every
    route write runs inside the identity writer fence, so this is the window a
    registration can land in.
    """
    original = approvals_router.require_legacy_approval_creation

    async def classified(*args: Any, **kwargs: Any) -> None:
        await pause.hold()
        await original(*args, **kwargs)

    monkeypatch.setattr(approvals_router, "require_legacy_approval_creation", classified)


def test_legacy_creation_refuses_a_registration_landing_before_its_commit(
    operator_case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec REQUEST-APPROVAL-2: the legacy owner judges the locked map before inserting."""
    client = operator_case.client
    scope, key = legacy_agent(client), selector_key()
    body, pause = legacy_body(scope), Pause()
    with monkeypatch.context() as patched:
        pause_before_classification(patched, pause)
        with background(lambda: legacy(client, body)) as creation:
            assert pause.reached.wait(WAIT_S)
            registered = register(client, scope, key)
            assert registered.status_code == 200, registered.text
            pause.proceed.set()
            refused_with(creation.result(WAIT_S), 409, "approval_route_locked")
    assert approval_rows(scope["agent"]) == []


def test_legacy_creation_holds_the_agent_row_until_it_commits(
    operator_case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec REQUEST-APPROVAL-2: a registration waits for an in-flight legacy creation."""
    client = operator_case.client
    scope, key = legacy_agent(client), selector_key()
    body, pause = legacy_body(scope), Pause()
    with monkeypatch.context() as patched:
        pause_before_insert(patched, pause)
        with background(lambda: legacy(client, body)) as creation:
            assert pause.reached.wait(WAIT_S)
            assert agent_row_is_locked(scope["agent"])
            pause.proceed.set()
            created = creation.result(WAIT_S)
            assert created.status_code == 201, created.text
    assert not agent_row_is_locked(scope["agent"])
    registered = register(client, scope, key)
    assert registered.status_code == 200, registered.text
    assert [row["selection_mode"] for row in approval_rows(scope["agent"])] == ["ordinary"]
    refused_with(legacy(client, legacy_body(scope)), 409, "approval_route_locked")
