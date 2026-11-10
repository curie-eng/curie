"""A nomination whose approval scope is HTTP-selected ends once, deterministically.

The internal remediation writer refuses a selected scope before insertion, so
the nomination stays owed. The reconcile pass ends it ``refused`` with the
terminal code ``http_identity_selected`` and never selects it again; an
unclassifiable scope stays owed and is retried; a tenant without HTTP
selection is unchanged; and refused rows cannot fill a pass ahead of another
tenant's owed approval. Actual ASGI routes, PostgreSQL and Valkey; synthetic
inputs only.
@spec AUTOMATED-REMEDIATION-15 @spec HTTP-APPROVAL-CONTINUITY-1.
"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Any

import pytest
from _migration_support import sql_rows
from _protected_ingress_harness import (
    _broker,
    ingress_broker_fixture,  # noqa: F401  (fixture)
)
from test_hook_source_support import support_db  # noqa: F401  (fixture)
from test_http_approval_internal_writers import move_agent, tenant_in
from test_remediation_admission import (
    EMAIL_REPLY,
    SECOND_REPLY,
    _owed,
    _reconcile_once,
    add_reply_channel,
    nomination,
    policy,
    q,
    run,
    world,
)

admission_service = _broker.admission_service
pytestmark = pytest.mark.usefixtures("support_db")
CODE = "http_identity_selected"


async def _selected_agent(stage_agent: str, mode: str) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1: the agent moves to a tenant in ``mode``."""
    tenant = await asyncio.to_thread(tenant_in, mode)
    await asyncio.to_thread(move_agent, stage_agent, tenant)


def test_a_selected_nomination_ends_once_and_is_not_selected_again(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec AUTOMATED-REMEDIATION-15 @spec HTTP-APPROVAL-CONTINUITY-1: after one
    pass the owed nomination is ``refused`` ``http_identity_selected`` with no
    approval or execution, and a second pass neither selects nor changes it."""

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind(policy(automatic=False))
            await _selected_agent(w.stage.agent, "http")
            owed = await _owed(w, age_seconds=600)

            await _reconcile_once(w)

            row = await nomination(owed)
            assert row["state"] == "refused", row
            assert row["refusal_code"] == CODE, row
            assert row["approval_id"] is None and row["execution_id"] is None, row
            assert row["decided_at"] is not None, row
            assert await q("SELECT id FROM curie.approvals") == []
            assert await _reconcile_once(w) == 0
            assert await nomination(owed) == row

    run(scenario)


def test_a_nomination_selected_only_by_its_reply_binding_ends(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1: the agent's tenant is unselected but the
    binding the approval would carry is; that is the same positive
    classification public creation refuses on."""

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await add_reply_channel(w.stage)
            await w.bind(policy(automatic=False))
            tenant = await asyncio.to_thread(tenant_in, "http")
            await asyncio.to_thread(
                sql_rows,
                "UPDATE curie.agent_channels SET tenant_id = :tenant "
                "WHERE agent_id = :agent AND address = :channel",
                {
                    "tenant": tenant,
                    "agent": uuid.UUID(w.stage.agent),
                    "channel": SECOND_REPLY["address"],
                },
            )
            owed = await _owed(w, reply=SECOND_REPLY, age_seconds=600)

            await _reconcile_once(w)

            row = await nomination(owed)
            assert (row["state"], row["refusal_code"]) == ("refused", CODE), row

    run(scenario)


def test_an_unclassifiable_scope_stays_owed_and_is_retried(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1: malformed configuration is not a
    positive selected classification; the nomination stays owed and every pass
    selects it again."""

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind(policy(automatic=False))
            await _selected_agent(w.stage.agent, "corrupt")
            owed = await _owed(w, age_seconds=600)

            assert await _reconcile_once(w) == 1
            assert await _reconcile_once(w) == 1

            row = await nomination(owed)
            assert row["state"] == "approval_requested", row
            assert row["refusal_code"] is None and row["approval_id"] is None, row

    run(scenario)


def test_a_tenant_without_http_selection_is_unchanged(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1: its owed approval is raised as before."""

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind(policy(automatic=False))
            await _selected_agent(w.stage.agent, "local")
            owed = await _owed(w, age_seconds=600)

            await _reconcile_once(w)

            row = await nomination(owed)
            assert row["state"] == "approval_requested", row
            assert row["approval_id"] is not None and row["refusal_code"] is None, row

    run(scenario)


def test_selected_rows_beyond_one_pass_do_not_starve_another_tenants_approval(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec AUTOMATED-REMEDIATION-15 @spec HTTP-APPROVAL-CONTINUITY-1: 105 owed
    nominations whose reply binding is HTTP-selected, all older than one on an
    unselected binding, are all ended in one pass and the newer one's approval
    is still raised in that same pass."""

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await add_reply_channel(w.stage)
            await w.bind(policy(automatic=False))
            tenant = await asyncio.to_thread(tenant_in, "http")
            await asyncio.to_thread(
                sql_rows,
                "UPDATE curie.agent_channels SET tenant_id = :tenant "
                "WHERE agent_id = :agent AND address = :channel",
                {
                    "tenant": tenant,
                    "agent": uuid.UUID(w.stage.agent),
                    "channel": SECOND_REPLY["address"],
                },
            )
            selected = [
                await _owed(w, reply=SECOND_REPLY, age_seconds=7200 - index) for index in range(105)
            ]
            raisable = await _owed(w, reply=EMAIL_REPLY, age_seconds=60)

            await _reconcile_once(w)

            states = {(await nomination(item))["refusal_code"] for item in selected}
            assert states == {CODE}
            row = await nomination(raisable)
            assert row["state"] == "approval_requested", row
            assert row["approval_id"] is not None, row

    run(scenario, timeout=240)
