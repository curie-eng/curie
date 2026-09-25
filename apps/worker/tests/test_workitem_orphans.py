"""Worker-side recovery of WorkItem runs orphaned by a restart (#3076)."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from types import SimpleNamespace

from curie_worker.workitem_dispatch import WorkItemConflict, WorkItemTransportError

SELF = "w-old"  # a restarted container reuses hostname-pid
PEER = "w-peer"
PROOF_S = 30.0


def _sweeper_cls() -> type:
    from curie_worker.workitem_orphans import WorkItemOrphanSweeper

    return WorkItemOrphanSweeper


def _row(owner: str, epoch: int = 1) -> SimpleNamespace:
    return SimpleNamespace(request_id=uuid.uuid4(), runtime_owner=owner, runtime_epoch=epoch)


@dataclass
class FakeClient:
    rows: list[SimpleNamespace]
    page_size: int = 100
    conflicts: set[uuid.UUID] = field(default_factory=set)
    transport_fail_on: set[uuid.UUID] = field(default_factory=set)
    list_fails: bool = False
    declared: list[tuple[uuid.UUID, str, int]] = field(default_factory=list)
    afters: list[uuid.UUID | None] = field(default_factory=list)

    async def runtime_owners(self, after: uuid.UUID | None = None) -> list[SimpleNamespace]:
        self.afters.append(after)
        if self.list_fails:
            raise WorkItemTransportError("down")
        ordered = sorted(self.rows, key=lambda r: r.request_id)
        if after is not None:
            ordered = [r for r in ordered if r.request_id > after]
        return ordered[: self.page_size]

    async def declare_owner_lost(
        self, request_id: uuid.UUID, *, owner: str, runtime_epoch: int
    ) -> None:
        if request_id in self.transport_fail_on:
            raise WorkItemTransportError("down")
        if request_id in self.conflicts:
            raise WorkItemConflict("stale_owner")
        self.declared.append((request_id, owner, runtime_epoch))
        self.rows = [r for r in self.rows if r.request_id != request_id]


class Liveness:
    def __init__(self, alive: set[str]) -> None:
        self.alive = set(alive)
        self.asked: list[str] = []

    async def __call__(self, owner: str) -> bool:
        self.asked.append(owner)
        return owner in self.alive


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _sweeper(
    client: FakeClient,
    liveness: Liveness,
    *,
    local: set[uuid.UUID] | None = None,
    clock: Clock | None = None,
    interval_s: float = 1.0,
    absence_proof_s: float = PROOF_S,
) -> object:
    owned = local if local is not None else set()
    kwargs: dict[str, object] = {
        "self_name": SELF,
        "locally_owned": lambda request_id: request_id in owned,
        "absence_proof_s": absence_proof_s,
        "interval_s": interval_s,
    }
    if clock is not None:
        kwargs["clock"] = clock
    return _sweeper_cls()(client, liveness, **kwargs)


def _ids(client: FakeClient) -> list[uuid.UUID]:
    return [request_id for request_id, _, _ in client.declared]


def _sweep(sweeper: object) -> int:
    return asyncio.run(sweeper.sweep())  # type: ignore[attr-defined]


def test_same_name_restart_is_declared_on_the_first_sweep() -> None:
    orphan = _row(SELF, epoch=3)
    client = FakeClient(rows=[orphan])
    assert _sweep(_sweeper(client, Liveness({SELF}))) == 1
    assert client.declared == [(orphan.request_id, SELF, 3)]


def test_a_row_this_process_owns_is_never_declared() -> None:
    mine = _row(SELF)
    client = FakeClient(rows=[mine])
    clock = Clock()
    sweeper = _sweeper(client, Liveness(set()), local={mine.request_id}, clock=clock)
    for _ in range(3):
        assert _sweep(sweeper) == 0
        clock.now += PROOF_S * 2
    assert client.declared == []


def test_a_dead_peer_is_declared_only_after_sustained_absence() -> None:
    orphan = _row(PEER, epoch=2)
    client = FakeClient(rows=[orphan])
    clock = Clock()
    sweeper = _sweeper(client, Liveness(set()), clock=clock)
    assert _sweep(sweeper) == 0  # first absent observation is recorded only
    clock.now += PROOF_S - 1
    assert _sweep(sweeper) == 0  # not yet absence_proof_s apart
    assert client.declared == []
    clock.now += 1
    assert _sweep(sweeper) == 1
    assert client.declared == [(orphan.request_id, PEER, 2)]


def test_a_single_missed_heartbeat_then_live_never_declares() -> None:
    row = _row(PEER)
    client = FakeClient(rows=[row])
    clock = Clock()
    liveness = Liveness(set())
    sweeper = _sweeper(client, liveness, clock=clock)
    assert _sweep(sweeper) == 0  # missed once
    clock.now += PROOF_S / 2
    liveness.alive.add(PEER)
    assert _sweep(sweeper) == 0  # live observation clears the record
    clock.now += PROOF_S / 2
    liveness.alive.discard(PEER)
    assert _sweep(sweeper) == 0  # a fresh first absence, not a second
    assert client.declared == []
    clock.now += PROOF_S
    assert _sweep(sweeper) == 1


def test_a_live_peer_is_skipped() -> None:
    client = FakeClient(rows=[_row(PEER)])
    clock = Clock()
    sweeper = _sweeper(client, Liveness({PEER}), clock=clock)
    _sweep(sweeper)
    clock.now += PROOF_S * 2
    assert _sweep(sweeper) == 0
    assert client.declared == []


def test_sweep_pages_until_an_empty_page() -> None:
    rows = [_row(SELF) for _ in range(5)]
    client = FakeClient(rows=list(rows), page_size=2, conflicts={r.request_id for r in rows})
    # Conflicts keep every row listed so paging must advance by `after`.
    assert _sweep(_sweeper(client, Liveness(set()))) == 0
    ordered = sorted(r.request_id for r in rows)
    assert client.afters == [None, ordered[1], ordered[3], ordered[4]]

    fresh = [_row(SELF) for _ in range(5)]
    paged = FakeClient(rows=list(fresh), page_size=2)
    assert _sweep(_sweeper(paged, Liveness(set()))) == 5
    assert sorted(_ids(paged)) == sorted(r.request_id for r in fresh)


def test_a_conflict_on_one_row_does_not_stop_the_next() -> None:
    first, second = sorted([_row(SELF), _row(SELF)], key=lambda r: r.request_id)
    client = FakeClient(rows=[first, second], conflicts={first.request_id})
    assert _sweep(_sweeper(client, Liveness(set()))) == 1
    assert _ids(client) == [second.request_id]


def test_a_transport_error_ends_the_sweep_without_raising() -> None:
    first, second = sorted([_row(SELF), _row(SELF)], key=lambda r: r.request_id)
    client = FakeClient(rows=[first, second], transport_fail_on={first.request_id})
    assert _sweep(_sweeper(client, Liveness(set()))) == 0
    assert client.declared == []

    down = FakeClient(rows=[_row(SELF)], list_fails=True)
    assert _sweep(_sweeper(down, Liveness(set()))) == 0


def test_empty_owner_list_declares_nothing() -> None:
    client = FakeClient(rows=[])
    assert _sweep(_sweeper(client, Liveness(set()))) == 0
    assert client.afters == [None]


def test_run_forever_recovers_a_peer_that_dies_later() -> None:
    later = _row(PEER)
    client = FakeClient(rows=[later])
    liveness = Liveness({PEER})

    async def go() -> None:
        shutdown = asyncio.Event()
        sweeper = _sweeper(client, liveness, interval_s=0.01, absence_proof_s=0.02)
        task = asyncio.create_task(sweeper.run_forever(shutdown))  # type: ignore[attr-defined]
        await asyncio.sleep(0.05)
        assert client.declared == []
        liveness.alive.discard(PEER)
        for _ in range(200):
            if client.declared:
                break
            await asyncio.sleep(0.01)
        shutdown.set()
        await asyncio.wait_for(task, timeout=2)

    asyncio.run(go())
    assert _ids(client) == [later.request_id]


def test_run_forever_does_not_declare_its_own_live_rows() -> None:
    mine = _row(SELF)
    client = FakeClient(rows=[mine])

    async def go() -> None:
        shutdown = asyncio.Event()
        sweeper = _sweeper(
            client, Liveness(set()), local={mine.request_id}, interval_s=0.01,
            absence_proof_s=0.01,
        )
        task = asyncio.create_task(sweeper.run_forever(shutdown))  # type: ignore[attr-defined]
        await asyncio.sleep(0.05)
        shutdown.set()
        await asyncio.wait_for(task, timeout=2)

    asyncio.run(go())
    assert client.declared == []
