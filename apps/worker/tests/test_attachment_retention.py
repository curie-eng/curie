"""The attachment retention ledger: a SIBLING of the workspace one (#2567, S3).

The workspace ownership ledger cannot be reused for attachment bytes, and the
reason is structural rather than stylistic. It is keyed by ``sha256(thread_key)``
under ``_ownership/``, its payload decodes to a typed ``PreparedWorkspace`` (an
object key, a repo full name, a base sha, a checkout mode), and its TTL is the
ROUTE lease -- refreshed by ``touch`` every time sandbox affinity is refreshed,
because its whole job is to say "this thread's sandbox is still using this base".
Attachment objects answer a different question ("may these bytes still be fetched
by a retry of this turn?") on a different clock, and folding them in would put a
foreign object class under a thread-ownership authority. Hence a second ledger,
with its own prefix and its own expiry field, swept from the SAME
``reap_orphans`` tick.

These tests hold both lanes in ONE object store at once, which is the only
arrangement that can show the sibling relationship: each reaper must sweep its
own prefix and leave the other's ledger and objects completely alone.

Slack is the only mocked collaborator; the object-store port is a conforming
implementation (see ``attachment_fixtures.py``).
"""

from __future__ import annotations

import hashlib
import importlib
import json
import sys
import threading
import uuid
from collections.abc import Iterable, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from aci_protocol import Attachment

# importlib import mode does not add this test directory to sys.path.
sys.path.insert(0, str(Path(__file__).parent))

from attachment_fixtures import (  # noqa: E402
    AGENT_ID,
    THREAD_KEY,
    FakeSlackFiles,
    MovableClock,
    RetainingObjectStore,
    limits,
)

ATTACHMENT_THREAD = THREAD_KEY
WORKSPACE_THREAD = "slack:C1:1700000000.000200"
DEPLOYMENT_ID = uuid.UUID("22222222-2222-4222-8222-222222222222")
BASE_ARCHIVE = b"normalized-base-archive"


@pytest.fixture
def attachments() -> Any:
    return importlib.import_module("curie_worker.attachments")


@pytest.fixture
def workspace() -> Any:
    return importlib.import_module("curie_worker.workspace")


def _resolved(
    module: Any,
    store: RetainingObjectStore,
    clock: MovableClock,
    *,
    bounds: Any | None = None,
    payloads: dict[str, list[bytes]] | None = None,
    refs: list[Attachment] | None = None,
) -> tuple[Any, Any]:
    """One resolved attachment set, recorded in the sibling ledger."""

    files = FakeSlackFiles(payloads or {"F1": [b"csv-bytes"]})
    coordinator = module.AttachmentCoordinator(
        files=files,
        objects=store,
        limits=bounds or limits(module),
        clock=clock,
    )
    prepared = coordinator.resolve(
        thread_key=ATTACHMENT_THREAD,
        agent_id=AGENT_ID,
        attachments=refs or [Attachment(id="F1", name="report.csv")],
    )
    return coordinator, prepared


def _owner_keys(module: Any, store: RetainingObjectStore) -> list[str]:
    """Every retention owner record in the store, through the real port.

    One full listing of the ledger prefix, which is also the only scan the
    coordinator itself is allowed: a per-thread subprefix listing would miss the
    pre-existing root record planted by the back-compat case below.
    """

    return list(store.list_keys(module.ATTACHMENT_LEDGER_PREFIX))


def _named_object_keys(store: RetainingObjectStore, owner_keys: Iterable[str]) -> set[str]:
    """The union of every object key these owner records still name.

    Decoded from the version one payload the ledger actually stores, so an owner
    the coordinator forgot to write is indistinguishable from one it never had:
    an object key missing from this union is an object nothing owns and the
    retention sweep can never reach.
    """

    named: set[str] = set()
    for key in owner_keys:
        payload = json.loads(b"".join(store.get_stream(key)))
        named.update(str(object_key) for object_key in payload["object_keys"])
    return named


def _thread_digest(thread_key: str) -> str:
    return hashlib.sha256(thread_key.encode("utf-8")).hexdigest()


def _plant_owner(
    module: Any,
    store: RetainingObjectStore,
    key: str,
    *,
    thread_key: str,
    object_keys: Sequence[str],
    expires_at_epoch: int,
) -> None:
    """Write one owner record straight into the store at an exact key.

    The back-compat case needs a record at the OLD root key, which no code path
    writes any more, so it is planted rather than resolved. The payload is the
    unchanged version one encoding, which is the whole point: an old record and a
    new one must decode identically through the same scan.
    """

    store.put_stream(
        key,
        (
            json.dumps(
                {
                    "version": 1,
                    "thread_key": thread_key,
                    "expires_at_epoch": expires_at_epoch,
                    "object_keys": list(object_keys),
                    "refs": module.encode_attachment_refs(
                        (
                            module.AttachmentRef(
                                name="planted.csv",
                                url="https://objects.example.com/planted?one-object=yes",
                                sha256="c" * 64,
                                size_bytes=9,
                                expires_at_epoch=expires_at_epoch,
                            ),
                        )
                    ),
                },
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8"),
        ),
    )


ATTACHMENT_LEDGER_PREFIX_NEEDLE = "_attachments/"


class _SequencedLedgerStore(RetainingObjectStore):
    """A store that lets the test choose which concurrent resolve records first.

    Only owner writes are sequenced; object writes run free, so the two resolves
    genuinely overlap and only the durable ownership order is pinned. Without
    this the interleaving is the scheduler's choice and the case could pass by
    luck on the order that happens to be benign.
    """

    def __init__(self, order: Sequence[str]) -> None:
        super().__init__()
        self._order = list(order)
        self._condition = threading.Condition()
        self._turn = 0

    def put_stream(self, key: str, chunks: Any) -> None:
        if not key.startswith(f"{ATTACHMENT_LEDGER_PREFIX_NEEDLE}"):
            super().put_stream(key, chunks)
            return
        name = threading.current_thread().name
        with self._condition:
            while self._turn < len(self._order) and self._order[self._turn] != name:
                assert self._condition.wait(timeout=5), "the sequenced owner write stalled"
            super().put_stream(key, chunks)
            self._turn += 1
            self._condition.notify_all()


class _BarrieredSlackFiles(FakeSlackFiles):
    """``FakeSlackFiles`` that parks every download until both have started.

    A real barrier rather than a sleep: both resolves are proven to be in flight
    at once, and the case cannot become a timing flake because nothing proceeds
    until the second party arrives.
    """

    def __init__(self, payloads: dict[str, list[bytes]], barrier: threading.Barrier) -> None:
        super().__init__(payloads)
        self._barrier = barrier

    def fetch(self, file_id: str) -> Any:
        self._barrier.wait(timeout=5)
        return super().fetch(file_id)


def _workspace_lane(
    module: Any,
    store: RetainingObjectStore,
    clock: MovableClock,
    *,
    ownership_ttl_seconds: int,
) -> tuple[Any, Any]:
    """A real ``WorkspaceClaimCoordinator`` owning one base in the same store.

    Only the clone/archive machinery is stubbed -- it is a subprocess + tar path
    with its own suite in ``test_workspace.py``. The ledger writes, the expiry
    arithmetic and the reap entry points are the real ones, because those are
    exactly what must stay untouched when the attachment sibling sweeps beside
    them.
    """

    reference = module.WorkspaceRef(
        url="https://objects.example.com/base.tar.gz?one-object=yes",
        sha256="a" * 64,
        expires_at_epoch=int(clock()) + 300,
    )
    prepared = module.PreparedWorkspace(
        object_key="bases/acme-corp-acme-bot-claim-1.tar.gz",
        sha256="a" * 64,
        clean_clone_url="https://github.com/acme-corp/acme-bot.git",
        repo_full_name="acme-corp/acme-bot",
        base_sha="b" * 40,
        materialized_head="b" * 40,
        checkout_mode=0o40700,
        reference=reference,
    )
    store.put_stream(prepared.object_key, (BASE_ARCHIVE,))
    preparer = SimpleNamespace(
        objects=store,
        credentials=None,
        prepare=lambda **_kwargs: prepared,
        verify=lambda _prepared: None,
        delete=lambda p: store.delete(p.object_key),
    )
    coordinator = module.WorkspaceClaimCoordinator(
        preparer=preparer,
        substrate=SimpleNamespace(claim=lambda *_a, **_k: object()),
        ownership_ttl_seconds=ownership_ttl_seconds,
        wall_clock=clock,
    )
    coordinator.claim_or_resume_with_handle(
        thread_key=WORKSPACE_THREAD,
        deployment_id=DEPLOYMENT_ID,
        env={},
        agent_name="acme-bot",
    )
    return coordinator, prepared


# --- The sibling's own sweep ----------------------------------------------------


def test_an_expired_attachment_ledger_is_reaped_and_its_objects_deleted(
    attachments: Any,
) -> None:
    """Past the retention window the bytes go, and the ledger entry goes with them.

    Split into ``begin``/``finish`` exactly as the workspace reaper is, and for
    the same reason: the object deletes can be slow, so the caller re-checks its
    distributed lock between the two halves (see the kernel-tick test).
    """

    store = RetainingObjectStore()
    clock = MovableClock()
    coordinator, prepared = _resolved(
        attachments, store, clock, bounds=limits(attachments, retention_ttl_seconds=600)
    )

    assert coordinator.enumerate_expired() == []
    clock.advance(601)
    assert coordinator.enumerate_expired() == [ATTACHMENT_THREAD]

    candidate = coordinator.begin_expired_reap(ATTACHMENT_THREAD)
    assert candidate is not None
    assert set(candidate.deleted_object_keys) == set(prepared.object_keys)
    assert coordinator.finish_expired_reap(candidate) is True

    assert all(key not in store.objects for key in prepared.object_keys)
    assert list(store.list_keys(attachments.ATTACHMENT_LEDGER_PREFIX)) == []
    assert coordinator.current(ATTACHMENT_THREAD) is None
    # Idempotent: a second tick finds nothing left to do rather than raising.
    assert coordinator.enumerate_expired() == []
    assert coordinator.begin_expired_reap(ATTACHMENT_THREAD) is None


def test_a_live_attachment_ledger_is_not_reaped(attachments: Any) -> None:
    """Inside the window the bytes stay fetchable, and ``begin`` refuses.

    ``begin_expired_reap`` re-reads the exact ledger rather than trusting the
    enumeration that named it, so a set recorded by another worker after the
    snapshot cannot be deleted out from under a live turn.
    """

    store = RetainingObjectStore()
    clock = MovableClock()
    coordinator, prepared = _resolved(
        attachments, store, clock, bounds=limits(attachments, retention_ttl_seconds=600)
    )

    clock.advance(599)

    assert coordinator.enumerate_expired() == []
    assert coordinator.begin_expired_reap(ATTACHMENT_THREAD) is None
    assert all(key in store.objects for key in prepared.object_keys)
    assert coordinator.current(ATTACHMENT_THREAD) is not None


def test_the_ledger_expiry_is_the_retention_window_not_the_reference_ttl(
    attachments: Any,
) -> None:
    """Its OWN TTL field, on its OWN clock.

    The signed capability is deliberately short-lived (minutes) while the bytes
    are retained long enough for a redelivery to reuse them, so the two numbers
    are independent. A ledger that expired with the reference would delete bytes
    a retry still needs.
    """

    store = RetainingObjectStore()
    clock = MovableClock()
    coordinator, prepared = _resolved(
        attachments,
        store,
        clock,
        bounds=limits(attachments, reference_ttl_seconds=120, retention_ttl_seconds=7200),
    )

    assert prepared.retention_expires_at_epoch == int(clock.now) + 7200
    assert prepared.refs[0].expires_at_epoch == int(clock.now) + 120

    # The capability has long since lapsed on the lane's own clock; the bytes are
    # still owned, because the two windows are independent numbers.
    clock.advance(3600)
    assert prepared.refs[0].expires_at_epoch <= int(clock.now)
    assert prepared.retention_expires_at_epoch > int(clock.now)
    assert coordinator.enumerate_expired() == []


def test_finish_expired_reap_survives_a_new_independent_owner(
    attachments: Any,
) -> None:
    """A fresh resolve is a NEW owner, so it does not invalidate the candidate.

    Under one mutable ledger per thread the next turn overwrote the record the
    reap had observed, and the sweep had to abandon a genuinely expired set and
    wait for another tick. Owner records are immutable and independent, so the
    observed expired owner is still byte-identical: ``finish`` deletes exactly
    that record, returns ``True``, and the newcomer's own owner survives intact.
    """

    store = RetainingObjectStore()
    clock = MovableClock()
    coordinator, expired = _resolved(
        attachments, store, clock, bounds=limits(attachments, retention_ttl_seconds=600)
    )
    clock.advance(601)
    candidate = coordinator.begin_expired_reap(ATTACHMENT_THREAD)
    assert candidate is not None
    observed = set(_owner_keys(attachments, store))

    # A different worker resolves the same thread's next turn.
    _second, replacement = _resolved(
        attachments,
        store,
        clock,
        bounds=limits(attachments, retention_ttl_seconds=600),
        payloads={"F2": [b"newer-bytes"]},
        refs=[Attachment(id="F2", name="newer.csv")],
    )
    newcomer = set(_owner_keys(attachments, store)) - observed
    assert len(newcomer) == 1, "the newer resolve must mint its OWN owner record"

    assert coordinator.finish_expired_reap(candidate) is True
    assert set(_owner_keys(attachments, store)) == newcomer
    assert all(key in store.objects for key in replacement.object_keys)
    assert all(key not in store.objects for key in expired.object_keys)
    current = coordinator.current(ATTACHMENT_THREAD)
    assert current is not None
    assert current.object_keys == replacement.object_keys


def test_finish_expired_reap_refuses_an_owner_discarded_under_it(
    attachments: Any,
) -> None:
    """The bytes comparison is still the second fence behind the route lock.

    The record this candidate observed is the only thing ``finish`` may delete.
    A discard between the two halves removes that exact owner, so the delete no
    longer applies to anything the reap saw and must be refused rather than
    reaching for whatever occupies the thread now.
    """

    store = RetainingObjectStore()
    clock = MovableClock()
    coordinator, prepared = _resolved(
        attachments, store, clock, bounds=limits(attachments, retention_ttl_seconds=600)
    )
    clock.advance(601)
    candidate = coordinator.begin_expired_reap(ATTACHMENT_THREAD)
    assert candidate is not None

    coordinator.discard_prepared(thread_key=ATTACHMENT_THREAD, prepared=prepared)

    assert coordinator.finish_expired_reap(candidate) is False


# --- Concurrent resolves: nothing installed may be left unnamed ------------------


@pytest.mark.parametrize(
    "order", [("resolve-a", "resolve-b"), ("resolve-b", "resolve-a")], ids=["a-first", "b-first"]
)
def test_two_concurrent_resolves_leave_neither_installed_set_unnamed(
    attachments: Any, order: tuple[str, str]
) -> None:
    """Both resolve orders, and BOTH sets keep an owner (#2737, AC1 and AC3).

    Two workers resolve the same thread outside the distributed route lock, so
    either one may record last -- and whichever does, the other's decision may
    still be the one that installs. A single mutable ledger per thread makes the
    late writer the only owner, which leaves the other set's private objects
    named by nothing: no retention sweep can ever reach them and no discard can
    account for them. The requirement is therefore stated the way the kernel
    needs it: two distinct immutable owner records must exist before either
    result may be treated as installed, and every object key of both sets must
    be named by one of them.
    """

    store = _SequencedLedgerStore(order)
    clock = MovableClock()
    barrier = threading.Barrier(2)
    prepared: dict[str, Any] = {}

    def _resolve_one(name: str, file_id: str, generation: str) -> None:
        coordinator = attachments.AttachmentCoordinator(
            files=_BarrieredSlackFiles({file_id: [b"bytes-" + file_id.encode()]}, barrier),
            objects=store,
            limits=limits(attachments, retention_ttl_seconds=600),
            clock=clock,
        )
        prepared[name] = coordinator.resolve(
            thread_key=ATTACHMENT_THREAD,
            agent_id=AGENT_ID,
            attachments=[Attachment(id=file_id, name=f"{file_id}.csv")],
            generation=generation,
        )

    workers = [
        threading.Thread(target=_resolve_one, name="resolve-a", args=("resolve-a", "F1", "gen-a")),
        threading.Thread(target=_resolve_one, name="resolve-b", args=("resolve-b", "F2", "gen-b")),
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=10)
        assert not worker.is_alive(), "a concurrent resolve never finished"

    owners = _owner_keys(attachments, store)
    assert len(set(owners)) == 2, (
        "each resolve must mint its own immutable owner record before either "
        f"result can be treated as installed; found {owners}"
    )
    installed = set(prepared["resolve-a"].object_keys) | set(prepared["resolve-b"].object_keys)
    assert installed <= _named_object_keys(store, owners), (
        "an installed object key named by no owner record is unreachable by "
        "every sweep and every discard"
    )
    # Whichever recorded last, the other set's bytes are still there to install.
    assert all(key in store.objects for key in installed)


# --- Pre-existing root records decode through the same scan ----------------------


def test_a_pre_existing_root_record_is_found_by_current_and_by_the_reap(
    attachments: Any,
) -> None:
    """A record written before owner records existed must still expire naturally.

    Deployments upgrade with live retention in flight, so the old mutable
    ``_attachments/<digest>.json`` key is already in the bucket. Every owner
    discovery path is therefore ONE full scan of the ledger prefix filtered on
    the decoded ``thread_key`` -- never a listing of the thread's own subprefix,
    which would walk straight past a root record and strand its objects forever.
    """

    store = RetainingObjectStore()
    clock = MovableClock()
    coordinator, fresh = _resolved(
        attachments, store, clock, bounds=limits(attachments, retention_ttl_seconds=600)
    )
    root_key = f"{attachments.ATTACHMENT_LEDGER_PREFIX}/{_thread_digest(ATTACHMENT_THREAD)}.json"
    legacy_object = "attachments/legacy/planted/000.bin"
    store.put_stream(legacy_object, (b"planted-bytes",))
    _plant_owner(
        attachments,
        store,
        root_key,
        thread_key=ATTACHMENT_THREAD,
        object_keys=[legacy_object],
        # Outlives the fresh owner, so "greatest expiry wins" must select it and
        # a path that only reads the nested owners cannot produce this answer.
        expires_at_epoch=int(clock.now) + 1200,
    )

    current = coordinator.current(ATTACHMENT_THREAD)
    assert current is not None
    assert current.object_keys == (legacy_object,)

    clock.advance(1201)
    assert coordinator.enumerate_expired() == [ATTACHMENT_THREAD]
    candidate = coordinator.begin_expired_reap(ATTACHMENT_THREAD)
    assert candidate is not None
    assert legacy_object in set(candidate.deleted_object_keys)
    assert coordinator.finish_expired_reap(candidate) is True

    assert root_key not in store.objects
    assert legacy_object not in store.objects
    assert all(key not in store.objects for key in fresh.object_keys)
    assert _owner_keys(attachments, store) == []


# --- Newest plus stale expiry ----------------------------------------------------


def test_the_sweep_removes_every_expired_owner_and_protects_live_overlap(
    attachments: Any,
) -> None:
    """Stale ownership is swept too, and a key a live owner still names survives.

    Preserving a replaced set's keys as stale ownership is only half of AC1; the
    other half is that the sweep can actually reach them. Both an older and a
    newer expired owner go, with every object key only they named, while the
    live owner's keys stay -- including the one it shares with an expired owner,
    which is exactly the key a per-set delete would take out from under it.
    """

    store = RetainingObjectStore()
    clock = MovableClock()
    files = FakeSlackFiles(
        {"F1": [b"stale-bytes"], "F2": [b"replaced-bytes"], "F3": [b"live-bytes"]}
    )
    coordinator = attachments.AttachmentCoordinator(
        files=files,
        objects=store,
        limits=limits(attachments, retention_ttl_seconds=600),
        clock=clock,
    )

    def _resolve(generation: str, refs: list[Attachment]) -> Any:
        return coordinator.resolve(
            thread_key=ATTACHMENT_THREAD,
            agent_id=AGENT_ID,
            attachments=refs,
            generation=generation,
        )

    # A fixed generation is what creates the overlap: ``shared`` set one names
    # the same 000 key the live set below re-parks and still names.
    stale = _resolve("shared", [Attachment(id="F1", name="stale.csv")])
    clock.advance(10)
    replaced = _resolve("replaced", [Attachment(id="F2", name="replaced.csv")])
    clock.advance(90)
    live = _resolve(
        "shared",
        [Attachment(id="F3", name="live.csv"), Attachment(id="F3", name="live-2.csv")],
    )

    assert len(set(_owner_keys(attachments, store))) == 3
    shared_key = stale.object_keys[0]
    assert shared_key in live.object_keys, "the fixed generation must overlap the key"

    # Past both expired owners (600 and 610) and short of the live one (700).
    clock.advance(560)
    assert coordinator.enumerate_expired() == [ATTACHMENT_THREAD], (
        "a thread with several expired owners is still ONE thread to reap"
    )

    candidate = coordinator.begin_expired_reap(ATTACHMENT_THREAD)
    assert candidate is not None
    assert set(candidate.deleted_object_keys) == set(replaced.object_keys)
    assert coordinator.finish_expired_reap(candidate) is True

    assert all(key not in store.objects for key in replaced.object_keys)
    assert all(key in store.objects for key in live.object_keys)
    assert len(set(_owner_keys(attachments, store))) == 1
    current = coordinator.current(ATTACHMENT_THREAD)
    assert current is not None
    assert current.object_keys == live.object_keys
    assert coordinator.enumerate_expired() == []


# --- Sibling, not replacement ---------------------------------------------------


def test_the_two_ledgers_live_under_distinct_prefixes_in_the_same_store(
    attachments: Any, workspace: Any
) -> None:
    store = RetainingObjectStore()
    clock = MovableClock()
    _resolved(attachments, store, clock)
    _workspace_lane(workspace, store, clock, ownership_ttl_seconds=86_400)

    attachment_ledgers = list(store.list_keys(attachments.ATTACHMENT_LEDGER_PREFIX))
    workspace_ledgers = [key for key in sorted(store.objects) if key.startswith("_ownership/")]

    assert len(attachment_ledgers) == 1
    assert len(workspace_ledgers) == 1
    assert attachments.ATTACHMENT_LEDGER_PREFIX != "_ownership"
    assert set(attachment_ledgers).isdisjoint(workspace_ledgers)


def test_reaping_attachments_leaves_the_workspace_ledger_and_its_base_untouched(
    attachments: Any, workspace: Any
) -> None:
    """The sibling sweeps only what it owns.

    The attachment set is past its retention window here while the workspace
    lease is deliberately much longer, so a reaper that swept by "anything
    expired in this bucket" would delete a base a live thread is still running
    on.
    """

    store = RetainingObjectStore()
    clock = MovableClock()
    coordinator, prepared = _resolved(
        attachments, store, clock, bounds=limits(attachments, retention_ttl_seconds=600)
    )
    workspace_lane, base = _workspace_lane(
        workspace, store, clock, ownership_ttl_seconds=86_400
    )

    clock.advance(601)
    assert coordinator.enumerate_expired() == [ATTACHMENT_THREAD]
    candidate = coordinator.begin_expired_reap(ATTACHMENT_THREAD)
    assert candidate is not None
    assert coordinator.finish_expired_reap(candidate) is True

    assert store.objects[base.object_key] == BASE_ARCHIVE
    assert workspace_lane.current(WORKSPACE_THREAD) is not None
    assert workspace_lane.enumerate_expired() == []
    assert all(key not in store.objects for key in prepared.object_keys)


def test_the_workspace_reaper_still_sweeps_its_own_thread_beside_an_attachment_ledger(
    attachments: Any, workspace: Any
) -> None:
    """Thread-ownership sweeping is unchanged by the new neighbour.

    The workspace enumeration walks ``_ownership/`` and decodes every record it
    finds into a typed ``PreparedWorkspace``. An attachment ledger written into
    the same bucket must therefore be invisible to it -- otherwise the first
    resolve in a workspace-enabled deployment turns every reap tick into a
    decode failure.
    """

    store = RetainingObjectStore()
    clock = MovableClock()
    attachment_lane, attachment_set = _resolved(
        attachments, store, clock, bounds=limits(attachments, retention_ttl_seconds=86_400)
    )
    workspace_lane, base = _workspace_lane(
        workspace, store, clock, ownership_ttl_seconds=600
    )

    clock.advance(601)

    assert workspace_lane.enumerate_expired() == [WORKSPACE_THREAD]
    candidate = workspace_lane.begin_expired_reap(WORKSPACE_THREAD)
    assert candidate is not None
    assert workspace_lane.finish_expired_reap(candidate) is True
    assert base.object_key not in store.objects
    assert workspace_lane.current(WORKSPACE_THREAD) is None

    # The attachment lane is still inside its own, longer window.
    assert attachment_lane.enumerate_expired() == []
    assert all(key in store.objects for key in attachment_set.object_keys)
