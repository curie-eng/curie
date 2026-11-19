"""A rule candidate stored by a new API replica is settled exactly once when an
older replica records its pull request's terminal state during a rolling
upgrade (#5796, ADR 0287 decision 5).

The API rolls with ``maxUnavailable: 0`` and ``maxSurge: 1``, so a replica
built before learned rule proposals can serve beside one built after. Only a
new replica serves ``POST /work-items/rule-proposal``, so a candidate exists
only once a new replica stored it. An older replica can still record the pull
request merged or closed, through the lineage reconciler or the worker's
publication advance, and it does so without settling: its terminal writers
never call the settlement and its reconciler pass has no settlement sweep.

An older replica is modelled as the current terminal writers with the
settlement removed (`_older_replica`): the name ``crud.lineages`` calls, the
worker store's name, and the reconciler's sweep become no-ops. Each case
drives the production terminal writer under it, checks the candidates are
still ``candidate``, then ends the roll with a new replica's reconciler pass
and checks the outcome: on merge exactly one ``proposed`` rule with the next
number in its scope and the rest ``superseded``, on close all ``discarded``,
with exactly one event per change; a later pass changes nothing. One case
runs two new replicas' sweeps at once over one terminal lineage: it settles
exactly once. The last starts the reconciler as the API does with the forge
pass off (an interval of 0): its settlement-only loop still settles.
"""

# ruff: noqa: F811  (the shared fixtures are imported, then requested)

from __future__ import annotations

import asyncio
import contextlib
import sys
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal

import pytest
from curie_api.config import get_settings
from curie_api.crud import lineages as crud_lineages
from curie_api.forges.types import PullRequestState
from curie_api.lineage_reconciler import LineageReconciler, start_rule_settlement
from curie_worker import publication_store
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_memory_rule_lifecycle import (  # noqa: E402
    RULE_A,
    RULE_B,
    RULE_C,
    _advance_terminal,
    _by_text,
    _events_of,
    _propose,
    _revision_candidates,
    _two_candidates,
)
from test_memory_rule_proposal import (  # noqa: E402
    PullStates,
    ReviewGitHubAPI,
    _complete,
    agent_of,
    lineage_status,
    pull_request,
    reconcile,
    revise,
    rules,  # noqa: F401  (fixture)
    stored_events,
    stored_rules,
    switch,
)
from test_workitem_outcomes import stack  # noqa: E402, F401  (fixture)

PLATFORM = ("platform", "platform")


@contextmanager
def _older_replica() -> Iterator[None]:
    """The terminal writers and the reconciler pass of a replica built before
    learned rule proposals: they record the terminal state and settle nothing."""

    async def no_settlement(*_args: Any, **_kwargs: Any) -> int:
        return 0

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(crud_lineages, "settle_lineage_rule_candidates", no_settlement)
        patch.setattr(publication_store, "settle_lineage_candidates", no_settlement)
        patch.setattr(LineageReconciler, "settle_terminal_lineages", no_settlement)
        yield


def _candidates() -> list[str]:
    return [rule["state"] for rule in stored_rules()]


def _proposed_in_scope(scope_key: str) -> list[int]:
    return sorted(
        rule["number"]
        for rule in stored_rules()
        if rule["scope_key"] == scope_key and rule["state"] == "proposed"
    )


def _assert_promoted(newest: str, older: str, number: int) -> None:
    rows = _by_text()
    assert (rows[newest]["state"], rows[newest]["number"]) == ("proposed", number)
    assert (rows[older]["state"], rows[older]["number"]) == ("superseded", None)
    assert [event[0] for event in _events_of(rows[newest]["id"])] == ["propose", "promote"]
    assert [event[0] for event in _events_of(rows[older]["id"])] == ["propose", "supersede"]
    assert _events_of(rows[newest]["id"])[-1][3:] == PLATFORM


def _assert_discarded(*texts: str) -> None:
    rows = _by_text()
    for rule_text in texts:
        assert (rows[rule_text]["state"], rows[rule_text]["number"]) == ("discarded", None)
        assert [event[0] for event in _events_of(rows[rule_text]["id"])] == [
            "propose",
            "discard",
        ]


def _assert_unchanged_by_a_later_pass() -> None:
    before = (stored_rules(), stored_events())
    assert reconcile({}).settled == 0
    assert (stored_rules(), stored_events()) == before


def test_a_merge_recorded_by_an_older_reconciler_is_promoted_once_the_roll_ends(
    rules: tuple[TestClient, ReviewGitHubAPI],
) -> None:
    client, api = rules
    # A rule already proposed in the repository scope, so the settled number
    # is the next one in the scope, not 1.
    number, first, pr, lineage_id = pull_request(client, api)
    _complete(client, first["id"])
    switch(client, agent_of(first["work_item_id"]), True)
    earlier = revise(client, api, number=number, pr=pr, lineage_id=lineage_id)
    _propose(client, earlier, RULE_C)
    assert reconcile({pr: PullRequestState.MERGED}).merged == 1
    scope_key = _by_text()[RULE_C]["scope_key"]
    assert _proposed_in_scope(scope_key) == [1]

    older, newer = _two_candidates(client, api)
    with _older_replica():
        result = reconcile({newer.pr: PullRequestState.MERGED})
    assert (result.merged, result.settled) == (1, 0)
    assert lineage_status(newer.lineage_id) == "merged"
    rows = _by_text()
    assert (rows[RULE_A]["state"], rows[RULE_B]["state"]) == ("candidate", "candidate")
    events = len(stored_events())

    # The roll ends: a new replica's pass settles the lineage the older one
    # left, once.
    assert reconcile({}).settled == 2
    _assert_promoted(RULE_B, RULE_A, 2)
    assert _proposed_in_scope(scope_key) == [1, 2]
    assert len(stored_events()) == events + 2
    _assert_unchanged_by_a_later_pass()


def test_a_close_recorded_by_an_older_reconciler_is_discarded_once_the_roll_ends(
    rules: tuple[TestClient, ReviewGitHubAPI],
) -> None:
    client, api = rules
    _older, newer = _two_candidates(client, api)
    with _older_replica():
        result = reconcile({newer.pr: PullRequestState.CLOSED})
    assert (result.closed, result.settled) == (1, 0)
    assert lineage_status(newer.lineage_id) == "closed"
    assert _candidates() == ["candidate", "candidate"]
    assert len(stored_events()) == 2

    assert reconcile({}).settled == 2
    _assert_discarded(RULE_A, RULE_B)
    assert len(stored_events()) == 4
    _assert_unchanged_by_a_later_pass()


@pytest.mark.parametrize("state", ["merged", "closed"])
def test_a_terminal_advance_by_an_older_replica_is_settled_once_the_roll_ends(
    stack: TestClient, auth_headers: dict[str, str], state: Literal["merged", "closed"]
) -> None:
    _agent, publication, revision = _revision_candidates(stack, auth_headers, (RULE_A, RULE_B))
    with _older_replica():
        _advance_terminal(stack, revision["id"], state)
    assert lineage_status(uuid.UUID(publication["lineage_id"])) == state
    assert _candidates() == ["candidate", "candidate"]
    assert len(stored_events()) == 2

    assert reconcile({}).settled == 2
    if state == "merged":
        _assert_promoted(RULE_B, RULE_A, 1)
    else:
        _assert_discarded(RULE_A, RULE_B)
    assert len(stored_events()) == 4
    _assert_unchanged_by_a_later_pass()


def test_two_new_replicas_sweeping_one_terminal_lineage_settle_it_once(
    rules: tuple[TestClient, ReviewGitHubAPI],
) -> None:
    client, api = rules
    _older, newer = _two_candidates(client, api)
    with _older_replica():
        assert reconcile({newer.pr: PullRequestState.MERGED}).merged == 1
    assert _candidates() == ["candidate", "candidate"]

    async def both() -> list[int]:
        engines = [create_async_engine(get_settings().database_url) for _ in range(2)]
        try:
            replicas = [
                LineageReconciler(
                    async_sessionmaker(engine, expire_on_commit=False),
                    get_settings(),
                    lambda _repository: PullStates({}),  # type: ignore[arg-type, return-value]
                    interval_seconds=60,
                )
                for engine in engines
            ]
            return list(
                await asyncio.gather(*(replica.settle_terminal_lineages() for replica in replicas))
            )
        finally:
            for engine in engines:
                await engine.dispose()

    settled = asyncio.run(both())

    # One sweep settled both candidates; the other found none left.
    assert sorted(settled) == [0, 2]
    _assert_promoted(RULE_B, RULE_A, 1)
    promotes = [event for event in stored_events() if event["event"] == "promote"]
    assert len(promotes) == 1
    assert [rule["state"] for rule in stored_rules()].count("proposed") == 1
    assert len(stored_events()) == 4
    _assert_unchanged_by_a_later_pass()


@pytest.mark.parametrize("state", ["merged", "closed"])
def test_with_the_work_item_reconciler_off_settlement_still_settles_once(
    stack: TestClient,
    auth_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    state: Literal["merged", "closed"],
) -> None:
    """``CURIE_WORK_ITEM_RECONCILER_ENABLED=false`` (the suite's setting) starts
    no lineage reconciler, yet the API still starts the settlement sweep, and
    that sweep settles what an older replica's publication advance left."""

    assert get_settings().work_item_reconciler_enabled is False
    assert stack.app.state.lineage_reconciler_task is None  # type: ignore[attr-defined]
    running = stack.app.state.rule_settlement_task  # type: ignore[attr-defined]
    assert running is not None and not running.done()

    _agent, publication, revision = _revision_candidates(stack, auth_headers, (RULE_A, RULE_B))
    with _older_replica():
        _advance_terminal(stack, revision["id"], state)
    assert lineage_status(uuid.UUID(publication["lineage_id"])) == state
    assert _candidates() == ["candidate", "candidate"]
    assert len(stored_events()) == 2

    # The same start the API makes, with every switch off and a short interval.
    settings = get_settings().model_copy(
        update={
            "work_item_reconciler_enabled": False,
            "factory_enabled": False,
            "factory_lineage_reconcile_interval_s": 0,
            "memory_rule_settlement_interval_s": 0.01,
        }
    )
    original = LineageReconciler.settle_terminal_lineages
    sweeps: list[int] = []
    swept = asyncio.Event()

    async def observed(self: LineageReconciler) -> int:
        settled = await original(self)
        sweeps.append(settled)
        swept.set()
        return settled

    monkeypatch.setattr(LineageReconciler, "settle_terminal_lineages", observed)

    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
            task = start_rule_settlement(sessionmaker, settings)
            try:
                await asyncio.wait_for(swept.wait(), timeout=60)
            finally:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        finally:
            await engine.dispose()

    asyncio.run(run())

    assert sweeps[0] == 2
    if state == "merged":
        _assert_promoted(RULE_B, RULE_A, 1)
    else:
        _assert_discarded(RULE_A, RULE_B)
    assert len(stored_events()) == 4
    _assert_unchanged_by_a_later_pass()
