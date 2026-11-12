"""Personal grant custody: staging, publication, retirement and cleanup (#5377 AC2).

ADR 0268 section 4. Each enrollment generation's grant lives in one immutable
Secret a ``personal_claude_custody`` row names. These tests drive the owner
routes over HTTP with real principal sessions and real Postgres; the Secret API
is ``personal_enrollment_support.FakeCustodyStore`` behind the
``CustodySecretStore`` seam, and interleavings are placed deterministically
with store hooks and engine statement hooks, never sleeps. Lease expiry is
written with SQL.

The last test is the gated real-cluster run (``CURIE_PERSONAL_CUSTODY_E2E=1``):
the production ``KubernetesCustodySecretStore`` and the worker's
``KubernetesSecretReader`` against a real API server, each impersonating the
ServiceAccount the chart's rendered RBAC binds, in one namespace the test
creates and deletes.
"""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import threading
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from uuid import UUID

import personal_enrollment_support as support
import personal_execution_support as pe
import pytest
from curie_api import custody_store
from curie_api.config import get_settings
from curie_api.custody_store import (
    CUSTODY_SECRET_KEY,
    CustodyStoreError,
    KubernetesCustodySecretStore,
)
from curie_worker.personal_custody import (
    CustodySecret,
    KubernetesSecretReader,
    PersonalCustodyReader,
    PersonalGrant,
)
from fastapi.testclient import TestClient
from oidc_test_idp import TestIdP
from personal_enrollment_support import FakeCustodyStore, Owner
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.fixture
def owned(clean_db: None) -> Iterator[None]:
    pe.cleanup_owned()
    yield
    assert pe.cleanup_owned() == 0, "an owned tenant or principal survived"


@pytest.fixture
def idp(owned: None) -> Iterator[TestIdP]:
    with support.identity_provider() as provider:
        yield provider


@pytest.fixture
def store() -> FakeCustodyStore:
    return FakeCustodyStore()


@pytest.fixture
def api(idp: TestIdP, store: FakeCustodyStore) -> Iterator[TestClient]:
    with support.enrollment_app(store) as client:
        yield client


@pytest.fixture
def owner(api: TestClient, idp: TestIdP) -> Owner:
    return support.seed_owner(support.seed_tenant(), issuer=idp.issuer)


class _WorkerView:
    """The worker's ``SecretReader`` over the same fake, so its contract checks run."""

    def __init__(self, store: FakeCustodyStore) -> None:
        self._store = store

    def read_secret(self, namespace: str, name: str) -> CustodySecret | None:
        secret = self._store.get(name, namespace=namespace)
        if secret is None:
            return None
        return CustodySecret(
            uid=secret.uid,
            type=secret.type,
            immutable=secret.immutable,
            annotations=dict(secret.annotations),
            data=dict(secret.data),
        )


def _worker_grant(store: FakeCustodyStore, owner: Owner, generation: int) -> Any:
    async def go() -> Any:
        engine = create_async_engine(get_settings().database_url)
        try:
            reader = PersonalCustodyReader(
                engine,
                schema="curie",
                namespace=support.GRANT_NAMESPACE,
                secrets=_WorkerView(store),
            )
            return await reader.read_grant(owner.tenant_id, owner.principal_id, generation)
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _published(owner: Owner) -> dict[str, Any]:
    [row] = [row for row in support.custody(owner) if row["state"] == "published"]
    return row


def _pending_cleanup(api: TestClient, owner: Owner) -> int:
    return int(support.body_of(support.status(api, owner))["teardown"]["pending_custody_cleanup"])


# --- the published Secret --------------------------------------------------------------


def test_published_secret_matches_the_shared_contract(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    support.enrolled(api, owner)
    row = _published(owner)
    enrollment = support.header(owner)
    assert enrollment is not None and row["enrollment_id"] == enrollment["id"]
    name = f"curie-pe-{enrollment['id'].hex}-g1"
    assert (row["secret_namespace"], row["secret_name"], row["secret_key"]) == (
        support.GRANT_NAMESPACE,
        name,
        CUSTODY_SECRET_KEY,
    )
    secret = store.get(name)
    assert secret is not None
    assert (secret.immutable, secret.type, set(secret.data)) == (
        True,
        "Opaque",
        {CUSTODY_SECRET_KEY},
    )
    assert secret.labels == {
        "app.kubernetes.io/managed-by": "curie",
        "curietech.ai/personal-custody": "true",
        "curietech.ai/personal-enrollment": enrollment["id"].hex,
        "curietech.ai/personal-generation": "1",
    }
    assert secret.annotations == {"curietech.ai/personal-custody-id": str(row["id"])}
    assert row["secret_uid"] == secret.uid
    with pytest.raises(CustodyStoreError) as refused:
        store.patch_data(support.GRANT_NAMESPACE, name, {CUSTODY_SECRET_KEY: "Zm9v"})
    assert refused.value.status == 422
    assert store.grant(name) == support.TOKEN

    # The worker's reader accepts exactly what the API wrote.
    grant = _worker_grant(store, owner, 1)
    assert isinstance(grant, PersonalGrant)
    assert grant.claim_env() == {
        "CURIE_MODEL_AUTH_MODE": "personal",
        CUSTODY_SECRET_KEY: support.TOKEN,
    }


def test_failed_secret_creation_keeps_the_previous_generation_usable(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    support.enrolled(api, owner)
    first = _published(owner)
    store.fail("create", CustodyStoreError(500))

    assert support.refusal(support.put(api, owner, "replace", version=1)) == (
        503,
        "custody_unavailable",
    )

    assert support.header(owner) is not None
    assert (support.header(owner)["generation"], support.header(owner)["version"]) == (1, 1)  # type: ignore[index]
    assert support.custody_states(owner) == [(1, "published", False), (2, "abandoned", True)]
    assert _published(owner) == first
    assert store.names() == [first["secret_name"]]
    grant = _worker_grant(store, owner, 1)
    assert isinstance(grant, PersonalGrant)
    assert grant.claim_env()[CUSTODY_SECRET_KEY] == support.TOKEN

    # A create that landed although the call failed leaves nothing behind either.
    def lands(namespace: str, name: str) -> None:
        [staged] = [row for row in support.custody(owner) if row["state"] == "staged"]
        store.put(
            name,
            labels=custody_store.custody_labels(staged["enrollment_id"], 2),
            annotations=custody_store.custody_annotations(staged["id"]),
        )

    store.before["create"] = lands
    store.fail("create", CustodyStoreError(None))
    assert support.refusal(support.put(api, owner, "replace", version=1)) == (
        503,
        "custody_unavailable",
    )
    assert store.names() == [first["secret_name"]]
    assert [state for state in support.custody_states(owner) if state[0] == 2] == [
        (2, "abandoned", True),
        (2, "abandoned", True),
    ]

    replaced = support.body_of(
        support.put(api, owner, "replace", version=1, token=support.REPLACEMENT)
    )
    assert (replaced["generation"], replaced["version"]) == (2, 2)
    assert store.grant(_published(owner)["secret_name"]) == support.REPLACEMENT


def test_a_foreign_same_name_secret_is_never_deleted(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    support.enrolled(api, owner)
    enrollment = support.header(owner)
    assert enrollment is not None
    name = custody_store.custody_secret_name(enrollment["id"], 2)
    foreign = store.put(name, labels={"app.kubernetes.io/managed-by": "someone-else"})

    assert support.refusal(support.put(api, owner, "replace", version=1)) == (
        503,
        "custody_unavailable",
    )
    assert store.get(name) is not None and store.get(name).uid == foreign  # type: ignore[union-attr]
    assert support.custody_states(owner) == [(1, "published", False), (2, "abandoned", True)]

    # An orphan of this enrollment's abandoned row at that name is cleared and replaced.
    abandoned = [row for row in support.custody(owner) if row["state"] == "abandoned"][0]
    store.remove(name)
    support.custody_secret(store, abandoned["id"], enrollment["id"], 2)
    assert support.body_of(support.put(api, owner, "replace", version=1))["generation"] == 2
    assert store.get(_published(owner)["secret_name"]).uid == _published(owner)["secret_uid"]  # type: ignore[union-attr]


# --- losing writers and reclaim --------------------------------------------------------


def test_losing_writer_deletes_only_its_own_secret(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    """A committed disconnect between the writer's staging and its publish wins the CAS."""

    support.enrolled(api, owner)
    first = _published(owner)
    swapped: dict[str, str] = {}

    def disconnect_and_swap(namespace: str, name: str, uid: str) -> None:
        support.body_of(support.disconnect(api, owner, 1))
        # Someone else's Secret now holds the loser's name under another UID.
        store.remove(name)
        swapped[name] = store.put(name, labels={"app.kubernetes.io/managed-by": "someone-else"})

    store.after["create"] = disconnect_and_swap

    assert support.refusal(support.put(api, owner, "replace", version=1)) == (
        409,
        "stale_version",
    )

    header = support.header(owner)
    assert header is not None
    assert (header["generation"], header["version"], header["active"]) == (2, 2, False)
    assert support.custody_states(owner) == [(1, "retired", True), (2, "abandoned", True)]
    [(name, foreign_uid)] = swapped.items()
    survivor = store.get(name)
    assert survivor is not None and survivor.uid == foreign_uid
    assert store.names() == [name] and first["secret_name"] not in store.names()


def test_interrupted_staging_is_reclaimed_after_its_lease(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    support.enrolled(api, owner)
    enrollment = support.header(owner)
    assert enrollment is not None
    row_id = uuid.uuid4()
    uid = support.custody_secret(store, row_id, enrollment["id"], 2)
    support.seed_custody(
        owner, enrollment_id=enrollment["id"], generation=2, state="staged", uid=uid, row_id=row_id
    )
    name = custody_store.custody_secret_name(enrollment["id"], 2)

    # Inside its lease the writer still owns it: no reclaim and no second stager.
    assert support.reconcile(store).reclaimed == 0
    assert store.get(name) is not None
    assert support.refusal(support.put(api, owner, "replace", version=1)) == (
        409,
        "staging_conflict",
    )

    support.expire_staging(owner)
    swept = support.reconcile(store)
    assert (swept.reclaimed, swept.cleared) == (1, 1)
    assert store.get(name) is None
    assert support.custody_states(owner)[1] == (2, "abandoned", True)
    assert support.body_of(support.put(api, owner, "replace", version=1))["generation"] == 2


def test_the_next_write_reclaims_an_expired_staging_inline(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    support.enrolled(api, owner)
    enrollment = support.header(owner)
    assert enrollment is not None
    row_id = uuid.uuid4()
    uid = support.custody_secret(store, row_id, enrollment["id"], 2)
    support.seed_custody(
        owner, enrollment_id=enrollment["id"], generation=2, state="staged", uid=uid, row_id=row_id
    )
    support.expire_staging(owner)

    assert support.body_of(support.put(api, owner, "replace", version=1))["generation"] == 2

    states = support.custody_states(owner)
    assert (2, "abandoned", True) in states and (2, "published", False) in states
    published = _published(owner)
    assert store.names() == [published["secret_name"]]
    assert store.get(published["secret_name"]).uid == published["secret_uid"]  # type: ignore[union-attr]


def test_reclaim_without_a_recorded_uid_matches_only_its_annotation(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    support.enrolled(api, owner)
    enrollment = support.header(owner)
    assert enrollment is not None
    row_id = uuid.uuid4()
    name = custody_store.custody_secret_name(enrollment["id"], 2)
    # Crashed after create, before the UID was recorded; the name holds a Secret
    # that does not carry this row's annotation.
    support.seed_custody(
        owner, enrollment_id=enrollment["id"], generation=2, state="staged", uid=None, row_id=row_id
    )
    stranger = store.put(name, labels=custody_store.custody_labels(enrollment["id"], 2))
    support.expire_staging(owner)

    swept = support.reconcile(store)

    assert swept.reclaimed == 1 and swept.swept == 0
    assert store.get(name) is not None and store.get(name).uid == stranger  # type: ignore[union-attr]
    assert support.custody_states(owner)[1] == (2, "abandoned", True)
    assert store.get(_published(owner)["secret_name"]) is not None

    # The same crash with the Secret carrying this row's annotation: swept by it.
    store.remove(name)
    late = uuid.uuid4()
    support.custody_secret(store, late, enrollment["id"], 3)
    support.seed_custody(
        owner,
        enrollment_id=enrollment["id"],
        generation=3,
        state="abandoned",
        uid=None,
        row_id=late,
    )
    assert support.reconcile(store).swept == 1
    assert store.names() == [_published(owner)["secret_name"]]
    assert (3, "abandoned", True) in support.custody_states(owner)


@pytest.mark.parametrize("winner", ["reclaim_before_uid", "reclaim_before_publish", "publish"])
def test_reclaim_and_publish_are_mutually_exclusive(
    api: TestClient, owner: Owner, store: FakeCustodyStore, winner: str
) -> None:
    support.enrolled(api, owner)
    first = _published(owner)
    background: list[threading.Thread] = []
    passes: list[Any] = []

    def reclaim() -> None:
        support.expire_staging(owner)
        passes.append(support.reconcile(store))

    if winner == "reclaim_before_uid":
        store.after["create"] = lambda namespace, name, uid: reclaim()
    elif winner == "reclaim_before_publish":
        # Tx1 is the first lock-timeout statement of this write, the publish the second.
        support.on_statement(
            api, "SET LOCAL lock_timeout", lambda: support.run_elsewhere(reclaim), occurrence=2
        )
    else:
        # The publish holds our row locked; a reclaim started now waits for its commit.
        def start() -> None:
            thread = threading.Thread(target=reclaim)
            thread.start()
            background.append(thread)

        support.on_statement(api, "UPDATE curie.personal_claude_enrollments", start)

    response = support.put(api, owner, "replace", version=1, token=support.REPLACEMENT)
    for thread in background:
        thread.join(timeout=60)
        assert not thread.is_alive()
    [result] = passes

    if winner == "publish":
        assert support.body_of(response)["generation"] == 2
        assert (result.reclaimed, result.swept) == (0, 0)
        published = _published(owner)
        assert store.grant(published["secret_name"]) == support.REPLACEMENT
        assert store.names() == [published["secret_name"]]
        return
    assert support.refusal(response) == (409, "staging_conflict")
    assert result.reclaimed == 1
    header = support.header(owner)
    assert header is not None and (header["generation"], header["version"]) == (1, 1)
    assert _published(owner) == first
    assert support.custody_states(owner) == [(1, "published", False), (2, "abandoned", True)]
    assert store.names() == [first["secret_name"]]
    assert store.grant(first["secret_name"]) == support.TOKEN


# --- retirement ------------------------------------------------------------------------


def test_replacement_retires_the_previous_secret_and_verifies_absence(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    support.enrolled(api, owner)
    first = _published(owner)
    support.body_of(support.put(api, owner, "replace", version=1, token=support.REPLACEMENT))
    assert support.custody_states(owner) == [(1, "retired", True), (2, "published", False)]
    assert first["secret_name"] not in store.names()
    reads = [
        call
        for call in store.calls
        if call == ("read", support.GRANT_NAMESPACE, first["secret_name"])
    ]
    assert reads, "absence was not verified by a read after the delete"

    # A failed delete leaves the row retiring, counted as pending, until the reconciler.
    second = _published(owner)
    store.fail("delete", CustodyStoreError(500))
    support.body_of(support.put(api, owner, "replace", version=2))
    assert support.custody_states(owner)[1] == (2, "retiring", False)
    assert second["secret_name"] in store.names()
    assert _pending_cleanup(api, owner) == 1

    assert support.reconcile(store).cleared == 1
    assert support.custody_states(owner)[1] == (2, "retired", True)
    assert second["secret_name"] not in store.names()
    assert _pending_cleanup(api, owner) == 0


def test_disconnect_retires_custody_and_creates_no_material(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    support.enrolled(api, owner)
    creates = store.count("create")
    body = support.body_of(support.disconnect(api, owner, 1))
    assert (body["state"], body["generation"]) == ("inactive", 2)
    assert store.count("create") == creates
    assert support.custody_states(owner) == [(1, "retired", True)]
    assert store.names() == []
    refusal = _worker_grant(store, owner, 2)
    assert getattr(refusal, "code", None) == "enrollment_inactive"


def test_orphaned_secret_from_a_reclaimed_slow_writer_is_swept(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    support.enrolled(api, owner)
    first = _published(owner)

    # The writer stalls before its create; a reclaim abandons the row meanwhile and
    # records it absent. The late create then loses at the UID record and removes
    # its own Secret.
    def stall(namespace: str, name: str) -> None:
        support.expire_staging(owner)
        support.reconcile(store)

    store.before["create"] = stall
    assert support.refusal(support.put(api, owner, "replace", version=1)) == (
        409,
        "staging_conflict",
    )
    assert store.names() == [first["secret_name"]]

    # Had that writer died after its late create instead, the sweep removes it.
    enrollment = support.header(owner)
    assert enrollment is not None
    row_id = uuid.uuid4()
    support.seed_custody(
        owner,
        enrollment_id=enrollment["id"],
        generation=2,
        state="abandoned",
        uid=None,
        absent=True,
        row_id=row_id,
    )
    late = custody_store.custody_secret_name(enrollment["id"], 2)
    support.custody_secret(store, row_id, enrollment["id"], 2)
    assert support.reconcile(store).swept == 1
    assert late not in store.names() and first["secret_name"] in store.names()


# --- R10's four cleanup passes ---------------------------------------------------------


def test_crash_after_abandon_is_cleared_by_pass_two(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    support.enrolled(api, owner)
    enrollment = support.header(owner)
    assert enrollment is not None
    row_id = uuid.uuid4()
    uid = support.custody_secret(store, row_id, enrollment["id"], 2)
    support.seed_custody(
        owner,
        enrollment_id=enrollment["id"],
        generation=2,
        state="abandoned",
        uid=uid,
        row_id=row_id,
    )
    assert _pending_cleanup(api, owner) == 1

    swept = support.reconcile(store)

    assert (swept.cleared, swept.swept) == (1, 0)
    assert support.custody_states(owner)[1] == (2, "abandoned", True)
    assert store.names() == [_published(owner)["secret_name"]]
    assert _pending_cleanup(api, owner) == 0


def test_late_create_after_a_recorded_absence_is_swept(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    support.enrolled(api, owner)
    enrollment = support.header(owner)
    assert enrollment is not None
    row_id = uuid.uuid4()
    support.seed_custody(
        owner,
        enrollment_id=enrollment["id"],
        generation=2,
        state="abandoned",
        uid=None,
        absent=True,
        row_id=row_id,
    )
    late = support.custody_secret(store, row_id, enrollment["id"], 2)
    assert _pending_cleanup(api, owner) == 0

    swept = support.reconcile(store)

    assert swept.swept == 1
    assert (
        "delete",
        support.GRANT_NAMESPACE,
        custody_store.custody_secret_name(enrollment["id"], 2),
    ) in store.calls
    assert all(secret.uid != late for secret in store.secrets.values())


def test_unlabelled_or_unannotated_secrets_survive_the_sweep(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    support.enrolled(api, owner)
    abandoned = support.seed_custody(
        owner,
        enrollment_id=uuid.uuid4(),
        generation=1,
        state="abandoned",
        uid=None,
        absent=True,
    )
    labels = {"curietech.ai/personal-custody": "true"}
    store.put("example-unlabelled", annotations=custody_store.custody_annotations(abandoned))
    store.put("example-unannotated", labels=labels)
    store.put(
        "example-malformed", labels=labels, annotations={"curietech.ai/personal-custody-id": "x"}
    )
    # Neighbour: labelled and annotated with no row behind it, so it goes.
    store.put(
        "example-orphan", labels=labels, annotations=custody_store.custody_annotations(uuid.uuid4())
    )

    assert support.reconcile(store).swept == 1
    assert store.names() == sorted(
        [
            "example-malformed",
            "example-unannotated",
            "example-unlabelled",
            _published(owner)["secret_name"],
        ]
    )


def test_a_published_secret_whose_uid_matches_is_never_swept(
    api: TestClient, owner: Owner, store: FakeCustodyStore
) -> None:
    support.enrolled(api, owner)
    published = _published(owner)
    # Neighbour: a copy claiming the published row under another UID is not its Secret.
    impostor = "curie-pe-example-copy"
    store.put(
        impostor,
        labels={"curietech.ai/personal-custody": "true"},
        annotations=custody_store.custody_annotations(published["id"]),
    )

    assert support.reconcile(store).swept == 1
    assert support.reconcile(store).swept == 0
    assert store.names() == [published["secret_name"]]
    assert store.get(published["secret_name"]).uid == published["secret_uid"]  # type: ignore[union-attr]
    assert _published(owner)["state"] == "published"


# --- responses, logs and the fence -----------------------------------------------------


class _Leaky(CustodyStoreError):
    def __init__(self, text: str) -> None:
        super().__init__(500)
        self.args = (text,)


def test_owner_responses_and_logs_never_carry_reference_or_grant(
    api: TestClient,
    owner: Owner,
    store: FakeCustodyStore,
    caplog: pytest.LogCaptureFixture,
    capfd: pytest.CaptureFixture[str],
) -> None:
    caplog.set_level(logging.DEBUG)
    responses = [
        support.put(api, owner, "enroll"),
        support.status(api, owner),
        support.put(api, owner, "replace", version=1, token=support.REPLACEMENT),
    ]
    enrollment = support.header(owner)
    assert enrollment is not None
    leak = f"{support.REPLACEMENT} {custody_store.custody_secret_name(enrollment['id'], 3)}"
    store.fail("create", _Leaky(leak))
    responses.append(support.put(api, owner, "replace", version=2, token=support.REPLACEMENT))
    store.fail("create", RuntimeError(leak))
    responses.append(support.put(api, owner, "replace", version=2, token=support.REPLACEMENT))
    store.fail("delete", _Leaky(leak))
    responses.append(support.disconnect(api, owner, 2))

    assert [response.status_code for response in responses] == [200, 200, 200, 503, 503, 200]
    names = [row["secret_name"] for row in support.custody(owner)]
    forbidden = (
        support.TOKEN,
        support.REPLACEMENT,
        support.GRANT_NAMESPACE,
        CUSTODY_SECRET_KEY,
        *names,
    )
    captured = capfd.readouterr()
    for text in [response.text for response in responses] + [
        caplog.text,
        captured.out,
        captured.err,
    ]:
        for fragment in forbidden:
            assert fragment not in text, fragment
    assert "curie-pe-" not in "".join(response.text for response in responses)


def _personal_requests(api: TestClient, world: pe.World, agent_id: UUID) -> dict[str, UUID]:
    """A running request, a started terminal one, a waiting one and a running service one."""

    running = pe.admit_personal(
        api, support.WORKER_HEADERS, world, agent_id, issue_number=5101, conversation="5101.000100"
    )
    pe.start_request(running.request_id)
    finished = pe.admit_personal(
        api, support.WORKER_HEADERS, world, agent_id, issue_number=5102, conversation="5102.000100"
    )
    pe.finish_failed(finished.request_id, pe.start_request(finished.request_id))
    waiting = pe.admit_personal(
        api, support.WORKER_HEADERS, world, agent_id, issue_number=5103, conversation="5103.000100"
    )
    service = pe.admit_service(
        api, support.WORKER_HEADERS, agent_id, issue_number=5104, conversation="5104.000100"
    )
    pe.start_request(service.request_id, owner=pe.OTHER_RUNTIME_OWNER)
    return {
        "running": running.request_id,
        "running_item": running.work_item_id,
        "finished": finished.request_id,
        "waiting": waiting.request_id,
        "service": service.request_id,
    }


def _request(request_id: UUID) -> dict[str, Any]:
    [row] = pe.rows(
        "SELECT status, terminal_cause, teardown_unconfirmed_at, version, runtime_epoch, "
        "runtime_claim_name, runtime_owner FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )
    return row


@pytest.fixture
def fenced_world(api: TestClient, idp: TestIdP) -> tuple[pe.World, Owner, UUID]:
    world = pe.seed_world()
    owner = support.sign_in(world.tenant_id, world.owner_id, issuer=idp.issuer)
    return world, owner, pe.insert_agent(world.tenant_id)


@pytest.mark.parametrize("verb", ["replace", "disconnect"])
def test_owner_routes_fence_running_and_started_terminal_requests(
    api: TestClient, fenced_world: tuple[pe.World, Owner, UUID], verb: str
) -> None:
    world, owner, agent_id = fenced_world
    ids = _personal_requests(api, world, agent_id)
    before = {name: _request(ids[name]) for name in ("waiting", "service")}

    if verb == "replace":
        response = support.put(api, owner, "replace", version=1)
    else:
        response = support.disconnect(api, owner, 1)
    body = support.body_of(response)

    assert body["generation"] == 2
    assert body["teardown"]["pending_runtimes"] == 2
    running = _request(ids["running"])
    assert (running["status"], running["terminal_cause"]) == (
        "cancellation_requested",
        "enrollment_fenced",
    )
    assert pe.rows(
        "SELECT cancelled_at FROM curie.work_items WHERE id = :id", {"id": ids["running_item"]}
    ) == [{"cancelled_at": None}]
    finished = _request(ids["finished"])
    assert finished["status"] == "failed" and finished["teardown_unconfirmed_at"] is not None
    for name, row in before.items():
        assert _request(ids[name]) == row, name


def test_service_claims_and_requests_stay_live(
    api: TestClient, fenced_world: tuple[pe.World, Owner, UUID]
) -> None:
    world, owner, agent_id = fenced_world
    ids = _personal_requests(api, world, agent_id)
    service = _request(ids["service"])
    assert service["status"] == "running" and service["runtime_claim_name"] == pe.CLAIM_NAME

    support.body_of(support.put(api, owner, "replace", version=1))
    support.body_of(support.disconnect(api, owner, 2))

    assert _request(ids["service"]) == service
    assert pe.request_tuples(
        pe.rows(
            "SELECT work_item_id FROM curie.execution_requests WHERE id = :id",
            {"id": ids["service"]},
        )[0]["work_item_id"]
    ) == {ids["service"]: (world.tenant_id, "service", None, None)}


def test_the_custody_loop_runs_when_configured_and_stops_with_the_app(
    idp: TestIdP, store: FakeCustodyStore
) -> None:
    """Its own task, on by default with a grant namespace, cancelled on shutdown."""

    swept = threading.Event()
    store.before["list"] = lambda namespace, name: swept.set()
    interval = {"CURIE_PERSONAL_CUSTODY_RECONCILE_INTERVAL_SECONDS": "3600"}
    with support.environment(interval), support.enrollment_app(store, reconciler=True) as api:
        task = api.app.state.personal_custody_reconciler_task  # type: ignore[attr-defined]
        assert task is not None
        # The first pass runs at start-up and reaches the namespace listing.
        assert swept.wait(timeout=30)
        assert not task.done()
    assert task.done()
    assert ("list", support.GRANT_NAMESPACE, "") in store.calls

    # Enabled but with no grant namespace, nothing runs.
    with (
        support.environment(interval),
        support.enrollment_app(store, namespace=None, reconciler=True) as api,
    ):
        assert api.app.state.personal_custody_reconciler_task is None  # type: ignore[attr-defined]


# --- the gated real-cluster run (R6) ---------------------------------------------------

E2E = os.environ.get("CURIE_PERSONAL_CUSTODY_E2E") == "1"
CHART_DIR = Path(__file__).resolve().parents[3] / "charts" / "curie"
RBAC_TEMPLATE = "templates/personal-grant-custody-rbac.yaml"


def _kubectl(
    context: str, *args: str, stdin: str | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["kubectl", "--context", context, *args],
        input=stdin,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )


def _core(context: str, *, user: str | None = None) -> Any:
    """A CoreV1 client on its own configuration, impersonating ``user`` when given."""

    from kubernetes import client as k8s_client
    from kubernetes import config as k8s_config

    configuration = k8s_client.Configuration()
    k8s_config.load_kube_config(context=context, client_configuration=configuration)
    configuration.retries = 0
    api_client = k8s_client.ApiClient(configuration=configuration)
    if user is not None:
        api_client.set_default_header("Impersonate-User", user)
    return k8s_client.CoreV1Api(api_client)


def _status(call: Callable[[], Any]) -> int | None:
    from kubernetes.client.exceptions import ApiException

    try:
        call()
    except ApiException as exc:
        return int(exc.status)
    return None


def _render_rbac(grant_ns: str, release_ns: str) -> str:
    """The chart's enabled custody RBAC, exactly as an install would render it."""

    return subprocess.run(
        [
            "helm",
            "template",
            "curie",
            str(CHART_DIR),
            "--namespace",
            release_ns,
            "--set",
            f"personalClaude.grantNamespace={grant_ns}",
            "--show-only",
            RBAC_TEMPLATE,
        ],
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    ).stdout


@pytest.mark.skipif(not E2E, reason="set CURIE_PERSONAL_CUSTODY_E2E=1 with a throwaway context")
def test_kubernetes_custody_store_honours_immutability_and_uid_preconditions(
    request: pytest.FixtureRequest,
) -> None:
    """The production store and the worker reader against a real API server.

    One namespace is owned: the caller names a fresh one, the test refuses an
    existing one, registers its deletion before creating it and verifies it
    gone. The release namespace is only a name in the RoleBinding subjects and
    is never created. Nothing is written outside the owned namespace.
    """

    import yaml
    from kubernetes import client as k8s_client

    context = os.environ["PERSONAL_CUSTODY_KUBE_CONTEXT"]
    grant_ns = os.environ["PERSONAL_CUSTODY_NAMESPACE"]
    release_ns = f"{grant_ns}-release"[:63].rstrip("-")
    existing = _kubectl(context, "get", "namespace", grant_ns, "-o", "name")
    assert existing.returncode != 0 and "NotFound" in existing.stderr, (
        "refusing to adopt an existing namespace; pass a fresh PERSONAL_CUSTODY_NAMESPACE"
    )
    assert "NotFound" in _kubectl(context, "get", "namespace", release_ns).stderr

    def delete_namespace() -> None:
        _kubectl(context, "delete", "namespace", grant_ns, "--ignore-not-found", "--wait=false")
        _kubectl(context, "wait", "--for=delete", f"namespace/{grant_ns}", "--timeout=170s")
        gone = _kubectl(context, "get", "namespace", grant_ns, "-o", "name")
        assert gone.returncode != 0 and "NotFound" in gone.stderr, "the owned namespace survived"

    request.addfinalizer(delete_namespace)
    created = _kubectl(context, "create", "namespace", grant_ns)
    assert created.returncode == 0, created.stderr

    rendered = _render_rbac(grant_ns, release_ns)
    documents = [doc for doc in yaml.safe_load_all(rendered) if doc]
    assert sorted(doc["kind"] for doc in documents) == [
        "Role",
        "Role",
        "RoleBinding",
        "RoleBinding",
    ]
    assert {doc["metadata"]["namespace"] for doc in documents} == {grant_ns}
    subjects = {
        doc["metadata"]["labels"]["app.kubernetes.io/component"]: doc["subjects"][0]
        for doc in documents
        if doc["kind"] == "RoleBinding"
    }
    assert {subject["namespace"] for subject in subjects.values()} == {release_ns}
    applied = _kubectl(context, "apply", "-n", grant_ns, "-f", "-", stdin=rendered)
    assert applied.returncode == 0, applied.stderr

    admin = _core(context)
    api_core = _core(context, user=f"system:serviceaccount:{release_ns}:{subjects['api']['name']}")
    worker_core = _core(
        context, user=f"system:serviceaccount:{release_ns}:{subjects['worker']['name']}"
    )
    store = KubernetesCustodySecretStore(lambda: api_core, timeout_s=10)
    enrollment_id, custody_id = uuid.uuid4(), uuid.uuid4()
    name = custody_store.custody_secret_name(enrollment_id, 1)
    token = f"EXAMPLE-custody-e2e-{uuid.uuid4().hex}"

    # The api identity creates, reads and lists; a second create of the name is 409.
    uid = store.create(
        grant_ns,
        name,
        token=token,
        labels=custody_store.custody_labels(enrollment_id, 1),
        annotations=custody_store.custody_annotations(custody_id),
    )
    read = store.read(grant_ns, name)
    assert read is not None and read.uid == uid and read.custody_id == custody_id
    assert (read.immutable, read.type, read.keys) == (
        True,
        "Opaque",
        frozenset({CUSTODY_SECRET_KEY}),
    )
    assert [secret.name for secret in store.list(grant_ns)] == [name]
    with pytest.raises(custody_store.SecretExists):
        store.create(grant_ns, name, token=token, labels={}, annotations={})

    # Immutability is the API server's: refused even for the cluster administrator.
    patch = {"data": {CUSTODY_SECRET_KEY: "RVhBTVBMRQ=="}}
    assert _status(lambda: admin.patch_namespaced_secret(name, grant_ns, patch)) == 422
    assert _status(lambda: api_core.patch_namespaced_secret(name, grant_ns, patch)) == 403

    # The worker identity reads the grant, and can neither create nor delete.
    found = KubernetesSecretReader(worker_core).read_secret(grant_ns, name)
    assert found is not None and found.uid == uid and set(found.data) == {CUSTODY_SECRET_KEY}
    probe = k8s_client.V1Secret(metadata=k8s_client.V1ObjectMeta(name="curie-pe-worker-probe"))
    assert _status(lambda: worker_core.create_namespaced_secret(grant_ns, probe)) == 403
    assert _status(lambda: worker_core.delete_namespaced_secret(name, grant_ns)) == 403
    assert store.read(grant_ns, "curie-pe-worker-probe") is None

    # Neither identity reads a Secret outside the grant namespace (a GET only).
    for core in (api_core, worker_core):
        assert (
            _status(lambda core=core: core.read_namespaced_secret("curie-pe-probe", "default"))
            == 403
        )

    # Every delete is bound to a UID: the wrong one is refused, the right one works.
    assert store.delete(grant_ns, name, uid=str(uuid.uuid4())) == "uid_mismatch"
    assert store.read(grant_ns, name) is not None
    assert store.delete(grant_ns, name, uid=uid) == "deleted"
    assert store.read(grant_ns, name) is None
    assert store.delete(grant_ns, name, uid=uid) == "absent"
    assert store.list(grant_ns) == []
