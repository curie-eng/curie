"""Transactional selection and frozen context contracts for later consumers."""

# ruff: noqa: F811

from __future__ import annotations

import asyncio
import hashlib
import time
import uuid
from typing import Any

import personal_execution_support as pe
import pytest
import requester_coding_support as support
from curie_api.config import get_settings
from curie_api.requester_coding_profiles import write_profile
from curie_internal.requester_coding_instructions import (
    AbsentCodingInstructions,
    CodingInstructionsAcknowledgmentV1,
    CodingInstructionsContextV1,
    CodingInstructionsEvidence,
    SelectedCodingInstructions,
    select_coding_instructions,
)
from fastapi.testclient import TestClient
from personal_enrollment_support import Owner
from pydantic import TypeAdapter, ValidationError
from requester_coding_support import api, idp, owned, owner, store  # noqa: F401
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine


def _select(owner: Owner, **kwargs: Any) -> Any:
    async def run(session: AsyncSession) -> Any:
        selected = await select_coding_instructions(
            session,
            tenant_id=owner.tenant_id,
            principal_id=owner.principal_id,
            **{"application_available": True, **kwargs},
        )
        await session.commit()
        return selected.model_dump()

    return pe.with_session(run)


def test_select_exact_normalized_revision_clear_and_policy(api: TestClient, owner: Owner) -> None:
    assert _select(owner) == {"kind": "absent", "reason": "no_profile"}
    saved = support.body(support.put(api, owner, "first\r\nsecond\r  \n"))
    assert _select(owner) == {
        "kind": "selected",
        "principal_id": str(owner.principal_id),
        "revision": saved["revision"],
        "sha256": saved["sha256"],
        "format_version": 1,
        "text": saved["text"],
    }
    pe.rows(
        "INSERT INTO curie.requester_coding_policies (tenant_id, enabled) VALUES (:t, false)",
        {"t": owner.tenant_id},
    )
    assert _select(owner) == {"kind": "absent", "reason": "tenant_disabled"}
    pe.rows(
        "UPDATE curie.requester_coding_policies SET enabled = true WHERE tenant_id = :t",
        {"t": owner.tenant_id},
    )
    support.body(support.clear(api, owner, saved["revision"]))
    assert _select(owner) == {"kind": "absent", "reason": "no_profile"}
    assert _select(owner, application_available=False) == {
        "kind": "absent",
        "reason": "application_unavailable",
    }
    assert _select(owner, historical=True) == {"kind": "absent", "reason": "historical"}


def test_unverified_requester_never_selects_a_profile(api: TestClient, owner: Owner) -> None:
    support.body(support.put(api, owner))

    async def run(session: AsyncSession) -> None:
        for tenant, principal in (
            (owner.tenant_id, None),
            (uuid.uuid4(), owner.principal_id),
            (owner.tenant_id, uuid.uuid4()),
        ):
            selected = await select_coding_instructions(
                session, tenant_id=tenant, principal_id=principal, application_available=True
            )
            assert selected.model_dump() == {"kind": "absent", "reason": "no_verified_requester"}

    pe.with_session(run)


async def _wait_for_lock(observer: AsyncSession, pid: int, task: asyncio.Task[Any]) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        blockers = await observer.scalar(text("SELECT pg_blocking_pids(:pid)"), {"pid": pid})
        if blockers:
            assert not task.done(), "the owner transaction did not remain blocked"
            return
        assert not task.done(), "selection did not serialize against the owner write"
    pytest.fail("the competing transaction never waited for the principal row lock")


@pytest.mark.parametrize("first_write", [True, False])
def test_selection_holds_lock_through_commit_including_first_write(
    api: TestClient, owner: Owner, first_write: bool
) -> None:
    if not first_write:
        support.body(support.put(api, owner, "old text"))
    expected = 0 if first_write else 1

    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with (
                AsyncSession(engine) as selection,
                AsyncSession(engine) as writer,
                AsyncSession(engine) as observer,
            ):
                chosen = await select_coding_instructions(
                    selection,
                    tenant_id=owner.tenant_id,
                    principal_id=owner.principal_id,
                    application_available=True,
                )
                if first_write:
                    assert chosen.model_dump() == {"kind": "absent", "reason": "no_profile"}
                else:
                    assert isinstance(chosen, SelectedCodingInstructions)
                    assert chosen.revision == 1 and chosen.text == "old text"
                writer_pid = await writer.scalar(text("SELECT pg_backend_pid()"))
                assert isinstance(writer_pid, int)

                async def mutate() -> Any:
                    result = await write_profile(
                        writer,
                        tenant_id=owner.tenant_id,
                        principal_id=owner.principal_id,
                        value="new text",
                        expected_revision=expected,
                    )
                    await writer.commit()
                    return result

                task = asyncio.create_task(mutate())
                try:
                    await _wait_for_lock(observer, writer_pid, task)
                    before = await selection.scalar(
                        text(
                            "SELECT text FROM curie.requester_coding_profiles "
                            "WHERE principal_id = :p"
                        ),
                        {"p": owner.principal_id},
                    )
                    assert before == (None if first_write else "old text")
                    await selection.commit()
                    written = await asyncio.wait_for(task, 10)
                    assert written.revision == expected + 1 and written.text == "new text"
                finally:
                    await selection.rollback()
                    if not task.done():
                        task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        finally:
            await engine.dispose()

    asyncio.run(run())
    selected = _select(owner)
    assert selected["revision"] == expected + 1 and selected["text"] == "new text"
    assert selected["sha256"] == hashlib.sha256(b"new text").hexdigest()


@pytest.mark.parametrize("first_write", [True, False])
def test_selection_waits_then_reads_the_committed_new_revision(
    api: TestClient, owner: Owner, first_write: bool
) -> None:
    if not first_write:
        support.body(support.put(api, owner, "old text"))
    expected = 0 if first_write else 1

    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with (
                AsyncSession(engine) as writer,
                AsyncSession(engine) as selection,
                AsyncSession(engine) as observer,
            ):
                written = await write_profile(
                    writer,
                    tenant_id=owner.tenant_id,
                    principal_id=owner.principal_id,
                    value="committed new text",
                    expected_revision=expected,
                )
                pid = await selection.scalar(text("SELECT pg_backend_pid()"))
                assert isinstance(pid, int)
                task = asyncio.create_task(
                    select_coding_instructions(
                        selection,
                        tenant_id=owner.tenant_id,
                        principal_id=owner.principal_id,
                        application_available=True,
                    )
                )
                try:
                    await _wait_for_lock(observer, pid, task)
                    await writer.commit()
                    chosen = await asyncio.wait_for(task, 10)
                    assert isinstance(chosen, SelectedCodingInstructions)
                    assert chosen.revision == written.revision == expected + 1
                    assert chosen.text == written.text == "committed new text"
                    assert chosen.sha256 == written.sha256
                    await selection.commit()
                finally:
                    await writer.rollback()
                    if not task.done():
                        task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        finally:
            await engine.dispose()

    asyncio.run(run())


def test_selection_refuses_snapshot_isolation(api: TestClient, owner: Owner) -> None:
    async def run() -> None:
        engine = create_async_engine(get_settings().database_url, isolation_level="REPEATABLE READ")
        try:
            async with AsyncSession(engine) as session:
                with pytest.raises(ValueError, match="coding_instructions_requires_read_committed"):
                    await select_coding_instructions(
                        session,
                        tenant_id=owner.tenant_id,
                        principal_id=owner.principal_id,
                        application_available=True,
                    )
        finally:
            await engine.dispose()

    asyncio.run(run())


@pytest.mark.parametrize("status", ["active", "disabled", "revoked"])
def test_selection_requires_an_active_principal(api: TestClient, owner: Owner, status: str) -> None:
    saved = support.body(support.put(api, owner))
    pe.rows(
        "UPDATE curie.principals SET status = :status WHERE id = :p",
        {"status": status, "p": owner.principal_id},
    )
    selected = _select(owner)
    if status == "active":
        assert selected["kind"] == "selected"
        assert selected["text"] == saved["text"]
        assert selected["revision"] == saved["revision"]
    else:
        assert selected == {"kind": "absent", "reason": "no_verified_requester"}
    assert support.snapshot(owner)[0]["text"] == saved["text"]


def test_selection_requires_explicit_application_availability(
    api: TestClient, owner: Owner
) -> None:
    async def run(session: AsyncSession) -> None:
        with pytest.raises(TypeError, match="application_available"):
            await select_coding_instructions(  # type: ignore[call-arg]
                session, tenant_id=owner.tenant_id, principal_id=owner.principal_id
            )
        selected = await select_coding_instructions(
            session,
            tenant_id=owner.tenant_id,
            principal_id=owner.principal_id,
            application_available=False,
        )
        assert selected.model_dump() == {"kind": "absent", "reason": "application_unavailable"}

    pe.with_session(run)


def test_context_and_acknowledgment_are_frozen_strict_contracts() -> None:
    identifiers = {
        "work_item_id": str(uuid.uuid4()),
        "execution_request_id": str(uuid.uuid4()),
        "execution_fence": 1,
    }
    selected = SelectedCodingInstructions(
        principal_id=str(uuid.uuid4()),
        revision=1,
        sha256=hashlib.sha256(b"text").hexdigest(),
        text="text",
    )
    context = CodingInstructionsContextV1(
        tenant_id=str(uuid.uuid4()), selection=selected, **identifiers
    )
    assert CodingInstructionsContextV1.model_validate_json(context.model_dump_json()) == context
    with pytest.raises(ValidationError):
        context.execution_fence = 2
    for field, value in (
        ("envelope_version", True),
        ("execution_fence", True),
        ("execution_fence", 0),
        ("unknown", "extra"),
    ):
        with pytest.raises(ValidationError):
            CodingInstructionsContextV1.model_validate({**context.model_dump(), field: value})
    for value in ("bad", "A" * 64, hashlib.sha256(b"other text").hexdigest()):
        with pytest.raises(ValidationError):
            SelectedCodingInstructions.model_validate({**selected.model_dump(), "sha256": value})
    for value in ("text\r\n", "", "text\x00", "x" * 16385, "\ud800"):
        with pytest.raises(ValidationError):
            SelectedCodingInstructions.model_validate({**selected.model_dump(), "text": value})
    for reason in (
        "no_profile",
        "no_verified_requester",
        "tenant_disabled",
        "application_unavailable",
        "historical",
    ):
        assert AbsentCodingInstructions(reason=reason).kind == "absent"
    with pytest.raises(ValidationError):
        AbsentCodingInstructions.model_validate({"reason": "no_profile", "text": "private"})
    for outcome in ("none", "failed", "applied"):
        payload = {**identifiers, "outcome": outcome}
        if outcome == "applied":
            payload["sha256"] = selected.sha256
        acknowledgment = CodingInstructionsAcknowledgmentV1.model_validate(payload)
        assert acknowledgment.outcome == outcome
        assert acknowledgment.model_dump() == {"envelope_version": 1, **payload}
        wrong = {**payload, "sha256": None if outcome == "applied" else selected.sha256}
        with pytest.raises(ValidationError):
            CodingInstructionsAcknowledgmentV1.model_validate(wrong)
    for state in ("pending", "none", "applied", "failed"):
        assert CodingInstructionsEvidence(state=state).state == state
    with pytest.raises(ValidationError):
        TypeAdapter(CodingInstructionsEvidence).validate_python({"state": "unknown"})
