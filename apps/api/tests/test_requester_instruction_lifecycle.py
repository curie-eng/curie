"""Pinned coding selections survive real factory lifecycle operations (#5591)."""

# ruff: noqa: F401, F811

from __future__ import annotations

import hashlib
from collections.abc import Iterator

import personal_execution_support as support
import pytest
import test_personal_execution_lifecycle as scenarios
from curie_api.config import get_settings
from curie_api.requester_coding_profiles import write_profile
from test_personal_execution_lifecycle import api, world

TEXT = "EXAMPLE_PINNED_CODING_SELECTION_5591\n"


@pytest.fixture(autouse=True)
def reset_settings_cache() -> Iterator[None]:
    yield
    get_settings.cache_clear()


def _write(world, *, value, expected_revision):
    async def go(session):
        result = await write_profile(
            session,
            tenant_id=world.tenant_id,
            principal_id=world.owner_id,
            value=value,
            expected_revision=expected_revision,
        )
        await session.commit()
        return result

    return support.with_session(go)


@pytest.fixture
def selected(world, monkeypatch):
    monkeypatch.setenv("CURIE_REQUESTER_CODING_INSTRUCTIONS_APPLY", "true")
    get_settings.cache_clear()
    _write(world, value=TEXT, expected_revision=0)


def _assert_pinned(world):
    items = support.rows(
        "SELECT id, requester_principal_id, coding_instructions_state, "
        "coding_instructions_revision, coding_instructions_sha256 "
        "FROM curie.work_items WHERE tenant_id = :t",
        {"t": world.tenant_id},
    )
    assert items
    for item in items:
        assert item["requester_principal_id"] == world.owner_id
        assert item["coding_instructions_state"] == "selected"
        assert item["coding_instructions_revision"] == 1
        assert item["coding_instructions_sha256"] == hashlib.sha256(TEXT.encode()).hexdigest()
        assert support.rows(
            "SELECT text FROM curie.work_item_coding_snapshots WHERE work_item_id = :id",
            {"id": item["id"]},
        ) == [{"text": TEXT}]
        requests = support.rows(
            "SELECT coding_instructions_selection_id, coding_instructions_evidence "
            "FROM curie.execution_requests WHERE work_item_id = :id",
            {"id": item["id"]},
        )
        assert requests
        assert all(
            row
            == {
                "coding_instructions_selection_id": item["id"],
                "coding_instructions_evidence": "pending",
            }
            for row in requests
        )


@pytest.mark.parametrize(
    "scenario",
    [
        scenarios.test_revision_from_another_requester_snapshots_the_original_owner,
        scenarios.test_relabel_and_base_reconcile_readmissions_keep_the_original_owner,
        scenarios.test_ci_fix_hold_keeps_the_owner_on_the_running_request,
        scenarios.test_owner_lost_successor_carries_the_original_owner,
    ],
    ids=["revision", "relabel", "ci-repair", "recovery"],
)
def test_lifecycle_retains_the_original_selection(api, world, selected, scenario):
    scenario(api, world)
    _assert_pinned(world)
    for revision, value in [(1, "replacement"), (2, "")]:
        _write(world, value=value, expected_revision=revision)
        _assert_pinned(world)


@pytest.mark.usefixtures("valkey")
def test_another_approver_retains_the_original_selection(
    api, world, selected, auth_headers, runs_stream
):
    scenarios.test_approval_by_another_principal_changes_only_the_approval_facts(
        api,
        world,
        auth_headers,
        runs_stream,
    )
    _assert_pinned(world)


def test_edit_and_clear_affect_only_new_work_items_and_never_continuations(api, world, selected):
    from curie_api import workitem_dispatch
    from curie_api.workitems.lifecycle import WorkItemOutcome

    agent = support.insert_agent(world.tenant_id)
    first = support.admit_personal(
        api,
        scenarios.WORKER_HEADERS,
        world,
        agent,
        issue_number=5591800,
    )
    _write(world, value="replacement", expected_revision=1)
    relabel = support.facts(agent, issue_number=5591800, requester=support.OTHER_REQUESTER)
    continued = support.with_session(lambda session: workitem_dispatch.readmit(session, relabel))
    assert isinstance(continued, WorkItemOutcome), continued
    assert continued.work_item.id == first.work_item_id
    _assert_pinned(world)
    second = support.admit_personal(
        api,
        scenarios.WORKER_HEADERS,
        world,
        agent,
        issue_number=5591801,
        conversation="1700000000.000200",
    )
    assert support.rows(
        "SELECT text FROM curie.work_item_coding_snapshots WHERE work_item_id = :id",
        {"id": second.work_item_id},
    ) == [{"text": "replacement"}]
    _write(world, value="", expected_revision=2)
    third = support.admit_personal(
        api,
        scenarios.WORKER_HEADERS,
        world,
        agent,
        issue_number=5591802,
        conversation="1700000000.000300",
    )
    assert support.rows(
        "SELECT coding_instructions_absence_reason FROM curie.work_items WHERE id = :id",
        {"id": third.work_item_id},
    ) == [{"coding_instructions_absence_reason": "no_profile"}]
    assert support.rows(
        "SELECT text FROM curie.work_item_coding_snapshots WHERE work_item_id = :id",
        {"id": first.work_item_id},
    ) == [{"text": TEXT}]


def test_activation_after_admission_never_reselects_the_existing_work_item(api, world, monkeypatch):
    from curie_api import workitem_dispatch
    from curie_api.workitems.lifecycle import WorkItemOutcome

    # Admitted while the capability was off; the default is now on (#5592).
    monkeypatch.setenv("CURIE_REQUESTER_CODING_INSTRUCTIONS_APPLY", "false")
    get_settings.cache_clear()
    _write(world, value=TEXT, expected_revision=0)
    agent = support.insert_agent(world.tenant_id)
    first = support.admit_personal(
        api,
        scenarios.WORKER_HEADERS,
        world,
        agent,
        issue_number=5591900,
    )
    monkeypatch.setenv("CURIE_REQUESTER_CODING_INSTRUCTIONS_APPLY", "true")
    get_settings.cache_clear()
    result = support.with_session(
        lambda session: workitem_dispatch.readmit(
            session,
            support.facts(agent, issue_number=5591900, requester=support.OTHER_REQUESTER),
        )
    )
    assert isinstance(result, WorkItemOutcome), result
    assert result.work_item.coding_instructions.absence_reason == "application_unavailable"
    assert result.request.coding_instructions_evidence == "none"
    assert (
        support.rows(
            "SELECT text FROM curie.work_item_coding_snapshots WHERE work_item_id = :id",
            {"id": first.work_item_id},
        )
        == []
    )
    new = support.admit_personal(
        api,
        scenarios.WORKER_HEADERS,
        world,
        agent,
        issue_number=5591901,
        conversation="1700000000.000200",
    )
    assert support.rows(
        "SELECT text FROM curie.work_item_coding_snapshots WHERE work_item_id = :id",
        {"id": new.work_item_id},
    ) == [{"text": TEXT}]


def test_enrollment_replacement_and_revalidation_keep_the_original_selection(
    world, selected, monkeypatch
):
    import test_personal_execution_revalidation as revalidation
    from curie_api.main import create_app
    from fastapi.testclient import TestClient

    support.configure_api(monkeypatch, worker_token=revalidation.WORKER_TOKEN)
    with TestClient(create_app()) as client:
        revalidation.test_revalidation_refuses_stale_mismatched_inactive_and_live_then_binds_once(
            client,
            world,
        )
    _assert_pinned(world)
