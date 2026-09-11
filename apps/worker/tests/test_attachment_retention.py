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

import importlib
import sys
import uuid
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


def test_finish_expired_reap_refuses_a_ledger_that_changed_under_it(
    attachments: Any,
) -> None:
    """The exact-record comparison is the second fence behind the route lock.

    Same guarantee ``WorkspaceClaimCoordinator.finish_expired_reap`` gives: a
    fresh resolve landing between ``begin`` and ``finish`` must survive, so the
    ledger delete applies only to the unchanged record.
    """

    store = RetainingObjectStore()
    clock = MovableClock()
    coordinator, _prepared = _resolved(
        attachments, store, clock, bounds=limits(attachments, retention_ttl_seconds=600)
    )
    clock.advance(601)
    candidate = coordinator.begin_expired_reap(ATTACHMENT_THREAD)
    assert candidate is not None

    # A different worker resolves the same thread's next turn.
    _second, replacement = _resolved(
        attachments,
        store,
        clock,
        bounds=limits(attachments, retention_ttl_seconds=600),
        payloads={"F2": [b"newer-bytes"]},
        refs=[Attachment(id="F2", name="newer.csv")],
    )

    assert coordinator.finish_expired_reap(candidate) is False
    assert all(key in store.objects for key in replacement.object_keys)
    assert list(store.list_keys(attachments.ATTACHMENT_LEDGER_PREFIX)) != []


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
