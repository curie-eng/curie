"""Shared fixtures for the owner enrollment and custody suites (#5377).

Imported by name (``conftest.py`` puts this directory on ``sys.path``). The
suites drive the real application over HTTP with real principal console
sessions and real Postgres. Kubernetes is faked only behind
``custody_store.CustodySecretStore`` (``FakeCustodyStore``) and the rollout
hold's pod listing (``FakeApiPods``); each fake models the Kubernetes API
answers the protocol relies on, with the source of each shape cited at the
method. The gated real-cluster case in ``test_personal_enrollment_custody.py``
observes the same answers from a live API server.

Tenants and principals reuse ``personal_execution_support``'s mark, so its
``cleanup_owned`` removes them along with every WorkItem, request, enrollment,
custody row and console session. Placeholder identifiers only.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import os
import threading
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from unittest import mock
from uuid import UUID

import personal_execution_support as pe
from curie_api import custody_store, rollout_hold
from curie_api.auth import CONSOLE_SESSION_COOKIE
from curie_api.config import get_settings
from curie_api.crud import console as crud_console
from curie_api.custody_store import (
    CUSTODY_LABEL,
    CUSTODY_SECRET_KEY,
    CustodyStoreError,
    DeleteOutcome,
    SecretExists,
    StoredSecret,
)
from curie_api.db import create_sessionmaker
from curie_api.main import create_app
from curie_api.models import Principal
from curie_api.personal_enrollments import Custody, CustodyPass, reconcile_custody
from curie_api.rollout_hold import ApiPod, RolloutHold
from fastapi.testclient import TestClient
from oidc_test_idp import AUDIENCE, CLIENT_SECRET, REDIRECT_URI, TestIdP, enabled_env
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

ORIGIN = "https://testserver"
ENROLLMENT = "/personal-claude/enrollment"
GRANT_NAMESPACE = "curie-personal-grants-example"
WORKER_TOKEN = "personal-enrollment-worker-token"
WORKER_HEADERS = {"X-Curie-Worker-Token": WORKER_TOKEN}
TOKEN = "sk-ant-oat01-EXAMPLEgrantSENTINEL0123456789"
REPLACEMENT = "sk-ant-oat01-EXAMPLEreplacementSENTINEL9876"
STAGE_LEASE_SECONDS = 300
#: What no owner response may ever contain.
REFERENCE_FRAGMENTS = ("curie-pe-", GRANT_NAMESPACE, CUSTODY_SECRET_KEY)


# --- the fake Kubernetes Secret API ----------------------------------------------------


@dataclass
class _Secret:
    name: str
    uid: str
    labels: dict[str, str]
    annotations: dict[str, str]
    data: dict[str, str] = field(repr=False)
    immutable: bool = True
    type: str = "Opaque"
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def stored(self) -> StoredSecret:
        return StoredSecret(
            name=self.name,
            uid=self.uid,
            labels=dict(self.labels),
            annotations=dict(self.annotations),
            created_at=self.created_at,
            immutable=self.immutable,
            type=self.type,
            keys=frozenset(self.data),
        )


class FakeCustodyStore:
    """An in-memory Secret API with the semantics the custody protocol relies on.

    Each answer mirrors the real API server, which the gated real-cluster test
    in ``test_personal_enrollment_custody.py`` observes directly:

    1. POST of a name that exists is 409 ``AlreadyExists`` (Kubernetes API
       conventions, "AlreadyExists"); the real store raises ``SecretExists``.
    2. DELETE whose ``preconditions.uid`` differs from the object's is 409
       Conflict (``DeleteOptions.preconditions``: "If not possible, a 409
       Conflict status will be returned"); the store answers ``uid_mismatch``.
    3. PATCH of ``data`` on an ``immutable: true`` Secret is 422 Invalid
       (Kubernetes "Secrets" concept, "Immutable Secrets").
    4. GET or DELETE of an absent name is 404 NotFound; read answers None and
       delete ``absent``.
    5. A list with ``labelSelector=curietech.ai/personal-custody=true`` returns
       only Secrets carrying that label.

    ``fail(op, error)`` makes the next call of ``op`` raise; ``before`` and
    ``after`` hooks interleave a test step around one call.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.secrets: dict[tuple[str, str], _Secret] = {}
        self.calls: list[tuple[str, str, str]] = []
        self._failures: dict[str, list[BaseException]] = {}
        self.before: dict[str, Callable[..., None]] = {}
        self.after: dict[str, Callable[..., None]] = {}

    # failure and interleave knobs
    def fail(self, op: str, error: BaseException, *, times: int = 1) -> None:
        with self._lock:
            self._failures.setdefault(op, []).extend([error] * times)

    def _call(self, op: str, namespace: str, name: str) -> None:
        with self._lock:
            self.calls.append((op, namespace, name))
            pending = self._failures.get(op)
            error = pending.pop(0) if pending else None
        hook = self.before.pop(op, None)
        if hook is not None:
            hook(namespace, name)
        if error is not None:
            raise error

    def _done(self, op: str, *args: Any) -> None:
        hook = self.after.pop(op, None)
        if hook is not None:
            hook(*args)

    # the CustodySecretStore protocol
    def create(
        self,
        namespace: str,
        name: str,
        *,
        token: str,
        labels: Mapping[str, str],
        annotations: Mapping[str, str],
    ) -> str:
        self._call("create", namespace, name)
        with self._lock:
            if (namespace, name) in self.secrets:
                raise SecretExists()
            uid = str(uuid.uuid4())
            self.secrets[(namespace, name)] = _Secret(
                name=name,
                uid=uid,
                labels=dict(labels),
                annotations=dict(annotations),
                data={CUSTODY_SECRET_KEY: base64.b64encode(token.encode()).decode()},
            )
        self._done("create", namespace, name, uid)
        return uid

    def read(self, namespace: str, name: str) -> StoredSecret | None:
        self._call("read", namespace, name)
        with self._lock:
            secret = self.secrets.get((namespace, name))
            return None if secret is None else secret.stored()

    def delete(self, namespace: str, name: str, *, uid: str) -> DeleteOutcome:
        self._call("delete", namespace, name)
        with self._lock:
            secret = self.secrets.get((namespace, name))
            if secret is None:
                outcome: DeleteOutcome = "absent"
            elif secret.uid != uid:
                outcome = "uid_mismatch"
            else:
                del self.secrets[(namespace, name)]
                outcome = "deleted"
        self._done("delete", namespace, name, outcome)
        return outcome

    def list(self, namespace: str) -> list[StoredSecret]:
        self._call("list", namespace, "")
        with self._lock:
            return [
                secret.stored()
                for (ns, _), secret in sorted(self.secrets.items())
                if ns == namespace and secret.labels.get(CUSTODY_LABEL) == "true"
            ]

    # test-side access, standing in for kubectl
    def patch_data(self, namespace: str, name: str, data: Mapping[str, str]) -> None:
        with self._lock:
            secret = self.secrets[(namespace, name)]
            if secret.immutable:
                raise CustodyStoreError(422)
            secret.data = dict(data)

    def put(
        self,
        name: str,
        *,
        labels: Mapping[str, str] | None = None,
        annotations: Mapping[str, str] | None = None,
        namespace: str = GRANT_NAMESPACE,
        token: str = TOKEN,
    ) -> str:
        """A Secret written outside the protocol (another writer, or an operator)."""

        uid = str(uuid.uuid4())
        with self._lock:
            self.secrets[(namespace, name)] = _Secret(
                name=name,
                uid=uid,
                labels=dict(labels or {}),
                annotations=dict(annotations or {}),
                data={CUSTODY_SECRET_KEY: base64.b64encode(token.encode()).decode()},
            )
        return uid

    def remove(self, name: str, *, namespace: str = GRANT_NAMESPACE) -> None:
        with self._lock:
            self.secrets.pop((namespace, name), None)

    def get(self, name: str, *, namespace: str = GRANT_NAMESPACE) -> _Secret | None:
        with self._lock:
            return self.secrets.get((namespace, name))

    def grant(self, name: str, *, namespace: str = GRANT_NAMESPACE) -> str:
        secret = self.get(name, namespace=namespace)
        assert secret is not None, name
        return base64.b64decode(secret.data[CUSTODY_SECRET_KEY]).decode()

    def names(self, *, namespace: str = GRANT_NAMESPACE) -> list[str]:
        with self._lock:
            return sorted(name for ns, name in self.secrets if ns == namespace)

    def count(self, op: str) -> int:
        with self._lock:
            return sum(1 for call in self.calls if call[0] == op)


class FakeApiPods:
    """The API pod list the rollout hold reads (``rollout_hold.ApiPodSource``).

    Pods of one Deployment carry the ``pod-template-hash`` label the Deployment
    controller adds to each ReplicaSet's pods (Kubernetes "Deployments",
    "Pod-template-hash label"), plus the chart's selector labels
    (``curie.selectorLabels``: name, instance, component). A list failure
    raises, as ``list_namespaced_pod`` does on any non-2xx answer.
    """

    def __init__(self, pods: list[ApiPod] | None = None) -> None:
        self.pods = list(pods or [])
        self.error: Exception | None = None
        self.calls = 0

    def list_api_pods(self, namespace: str, label_selector: str) -> list[ApiPod]:
        self.calls += 1
        assert label_selector == rollout_hold.API_POD_SELECTOR
        if self.error is not None:
            raise self.error
        return list(self.pods)


def api_pod(name: str, template: str, *, phase: str = "Running", instance: str = "curie") -> ApiPod:
    return ApiPod(
        name=name,
        phase=phase,
        labels={
            rollout_hold.NAME_LABEL: "curie",
            rollout_hold.INSTANCE_LABEL: instance,
            rollout_hold.COMPONENT_LABEL: "api",
            rollout_hold.TEMPLATE_HASH_LABEL: template,
        },
    )


def held_by(pods: FakeApiPods, *, own: str = "curie-api-a") -> RolloutHold:
    return RolloutHold(enabled=True, pods=pods, namespace="curie", pod_name=own)


# --- environment and the application ------------------------------------------------------


@contextlib.contextmanager
def environment(values: Mapping[str, str | None]) -> Iterator[None]:
    """Set (str) or unset (None) env vars and restore them, clearing settings on both edges."""

    saved = {name: os.environ.get(name) for name in values}
    try:
        for name, value in values.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        get_settings.cache_clear()
        yield
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        get_settings.cache_clear()


@contextlib.contextmanager
def identity_provider() -> Iterator[TestIdP]:
    """The repo's in-process OIDC provider, configured as the API's issuer."""

    with TestIdP(client_id=AUDIENCE, client_secret=CLIENT_SECRET, redirect_uri=REDIRECT_URI) as idp:
        with environment(dict(enabled_env(idp))):
            yield idp


def app_env(
    *, namespace: str | None = GRANT_NAMESPACE, hold: bool = False, reconciler: bool = False
) -> dict[str, str | None]:
    return {
        "INTERNAL_WORKER_TOKEN": WORKER_TOKEN,
        "RESUME_RECONCILER_ENABLED": "false",
        "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
        "CURIE_PERSONAL_GRANT_NAMESPACE": namespace or None,
        "CURIE_PERSONAL_CUSTODY_RECONCILER_ENABLED": "true" if reconciler else "false",
        "CURIE_PERSONAL_CUSTODY_STAGE_LEASE_SECONDS": str(STAGE_LEASE_SECONDS),
        "CURIE_PERSONAL_ROLLOUT_HOLD": "true" if hold else "false",
    }


@contextlib.contextmanager
def enrollment_app(
    store: FakeCustodyStore,
    *,
    namespace: str | None = GRANT_NAMESPACE,
    hold: RolloutHold | None = None,
    reconciler: bool = False,
) -> Iterator[TestClient]:
    """``create_app()`` with its real lifespan; only the custody store and hold are fakes.

    The custody loop is off unless ``reconciler``; tests run its passes with
    ``reconcile`` instead, as another replica would.
    """

    def build_store(settings: Any) -> FakeCustodyStore | None:
        return store if settings.personal_grant_namespace else None

    with (
        environment(app_env(namespace=namespace, hold=hold is not None, reconciler=reconciler)),
        mock.patch.object(custody_store, "build_custody_secret_store", build_store),
        mock.patch.object(
            rollout_hold,
            "build_rollout_hold",
            (lambda settings: hold) if hold is not None else rollout_hold.build_rollout_hold,
        ),
        TestClient(create_app(), base_url=ORIGIN) as client,
    ):
        yield client


def on_statement(
    client: TestClient, prefix: str, action: Callable[[], None], *, occurrence: int = 1
) -> None:
    """Run ``action`` once, just before the app's ``occurrence``-th statement starting ``prefix``.

    The listener runs on the app's event loop thread, so ``action`` must not
    need that loop; ``run_elsewhere`` runs an async step on its own loop.
    """

    seen = 0
    engine = client.app.state.engine.sync_engine  # type: ignore[attr-defined]

    def listener(conn: Any, cursor: Any, statement: str, *args: Any) -> None:
        # Never removed from inside its own dispatch; it goes with the engine.
        nonlocal seen
        if not statement.lstrip().startswith(prefix):
            return
        seen += 1
        if seen == occurrence:
            action()

    event.listen(engine, "before_cursor_execute", listener)


def run_elsewhere[T](step: Callable[[], T]) -> T:
    """Run ``step`` on a fresh thread and wait, so it may start its own event loop."""

    box: dict[str, Any] = {}

    def target() -> None:
        try:
            box["value"] = step()
        except BaseException as exc:  # noqa: BLE001 - re-raised on the caller's thread
            box["error"] = exc

    thread = threading.Thread(target=target)
    thread.start()
    thread.join(timeout=60)
    assert not thread.is_alive(), "the interleaved step did not finish"
    if "error" in box:
        raise box["error"]
    return box["value"]  # type: ignore[no-any-return]


@contextlib.contextmanager
def row_locked(statement: str, params: Mapping[str, Any]) -> Iterator[None]:
    """Hold the row locks ``statement`` takes, from another connection, until exit."""

    locked, release = threading.Event(), threading.Event()
    failure: list[BaseException] = []

    async def hold() -> None:
        engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
        try:
            async with engine.connect() as connection:
                await connection.execute(text(statement), dict(params))
                locked.set()
                # Nothing else runs on this thread's loop; the transaction just stays open.
                release.wait(timeout=120)
                await connection.rollback()
        finally:
            await engine.dispose()

    def target() -> None:
        try:
            asyncio.run(hold())
        except BaseException as exc:  # noqa: BLE001 - surfaced on the caller's thread
            failure.append(exc)
            locked.set()

    thread = threading.Thread(target=target)
    thread.start()
    assert locked.wait(timeout=60), "the lock holder never took its lock"
    if failure:
        raise failure[0]
    try:
        yield
    finally:
        release.set()
        thread.join(timeout=60)
        assert not thread.is_alive(), "the lock holder did not finish"


def reconcile(store: FakeCustodyStore, *, namespace: str = GRANT_NAMESPACE) -> CustodyPass:
    """One custody reconciler pass on a fresh engine, as another replica would run it."""

    async def go() -> CustodyPass:
        engine = create_async_engine(get_settings().database_url)
        try:
            custody = Custody(
                sessionmaker=create_sessionmaker(engine),
                store=store,
                namespace=namespace,
                stage_lease_seconds=STAGE_LEASE_SECONDS,
            )
            return await reconcile_custody(custody)
        finally:
            await engine.dispose()

    return asyncio.run(go())


# --- owners and sessions -------------------------------------------------------------------


@dataclass(frozen=True)
class Owner:
    tenant_id: UUID
    principal_id: UUID
    session: str

    def headers(self, *, origin: str | None = ORIGIN) -> dict[str, str]:
        headers = {"Cookie": f"{CONSOLE_SESSION_COOKIE}={self.session}"}
        if origin is not None:
            headers["Origin"] = origin
        return headers


def seed_tenant(*, status: str = "active") -> UUID:
    tenant_id = uuid.uuid4()
    pe.rows(
        "INSERT INTO curie.tenants (id, deployment_id, status) VALUES (:id, :deployment, :status)",
        {"id": tenant_id, "deployment": f"{pe.MARK}{tenant_id}", "status": status},
    )
    return tenant_id


def sign_in(tenant_id: UUID, principal_id: UUID, *, issuer: str) -> Owner:
    """Bind ``principal_id`` to the configured issuer and mint its console session."""

    async def body(session: AsyncSession) -> str:
        await session.execute(
            text("UPDATE curie.principals SET idp_issuer = :issuer WHERE id = :id"),
            {"issuer": issuer, "id": principal_id},
        )
        await session.commit()
        principal = await session.get(Principal, principal_id)
        assert principal is not None
        token, _ = await crud_console.create_principal_console_session(session, principal)
        return token

    return Owner(tenant_id, principal_id, pe.with_session(body))


def seed_owner(tenant_id: UUID, *, issuer: str) -> Owner:
    """A new active human principal of ``tenant_id`` with a live console session."""

    principal_id = uuid.uuid4()
    pe.rows(
        "INSERT INTO curie.principals (id, tenant_id, idp_subject, type, status, display_name) "
        "VALUES (:id, :tenant, :subject, 'human', 'active', 'Acme Owner')",
        {"id": principal_id, "tenant": tenant_id, "subject": f"{pe.MARK}{principal_id}"},
    )
    return sign_in(tenant_id, principal_id, issuer=issuer)


# --- the owner routes ----------------------------------------------------------------------


def status(client: TestClient, owner: Owner) -> Any:
    return client.get(ENROLLMENT, headers=owner.headers(origin=None))


def put(
    client: TestClient,
    owner: Owner,
    intent: str,
    *,
    version: int | None = None,
    token: str = TOKEN,
    origin: str | None = ORIGIN,
    headers: Mapping[str, str] | None = None,
) -> Any:
    body: dict[str, Any] = {"intent": intent, "token": token}
    if version is not None:
        body["expected_version"] = version
    return client.put(
        ENROLLMENT, json=body, headers={**owner.headers(origin=origin), **dict(headers or {})}
    )


def disconnect(
    client: TestClient,
    owner: Owner,
    version: int,
    *,
    origin: str | None = ORIGIN,
    headers: Mapping[str, str] | None = None,
) -> Any:
    return client.delete(
        ENROLLMENT,
        params={"expected_version": version},
        headers={**owner.headers(origin=origin), **dict(headers or {})},
    )


def body_of(response: Any) -> dict[str, Any]:
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    found: dict[str, Any] = response.json()
    return found


def refusal(response: Any) -> tuple[int, Any]:
    assert response.headers["cache-control"] == "no-store", response.headers
    detail = response.json()["detail"]
    return response.status_code, detail["code"] if isinstance(detail, dict) else detail


def enrolled(client: TestClient, owner: Owner, *, token: str = TOKEN) -> dict[str, Any]:
    return body_of(put(client, owner, "enroll", token=token))


# --- durable state -------------------------------------------------------------------------


def header(owner: Owner) -> dict[str, Any] | None:
    found = pe.rows(
        "SELECT id, generation, version, active FROM curie.personal_claude_enrollments "
        "WHERE tenant_id = :tenant AND principal_id = :principal",
        {"tenant": owner.tenant_id, "principal": owner.principal_id},
    )
    return found[0] if found else None


def custody(owner: Owner) -> list[dict[str, Any]]:
    return pe.rows(
        "SELECT id, enrollment_id, generation, secret_namespace, secret_name, secret_key, "
        "secret_uid, state, published_at, retired_at, secret_absent_at, version "
        "FROM curie.personal_claude_custody "
        "WHERE tenant_id = :tenant AND principal_id = :principal ORDER BY generation, staged_at",
        {"tenant": owner.tenant_id, "principal": owner.principal_id},
    )


def custody_states(owner: Owner) -> list[tuple[int, str, bool]]:
    """(generation, state, absence recorded) per row."""

    return [
        (int(row["generation"]), row["state"], row["secret_absent_at"] is not None)
        for row in custody(owner)
    ]


def expire_staging(owner: Owner) -> None:
    pe.rows(
        "UPDATE curie.personal_claude_custody SET lease_expires_at = clock_timestamp() "
        "- interval '1 second' WHERE tenant_id = :tenant AND principal_id = :principal "
        "AND state = 'staged'",
        {"tenant": owner.tenant_id, "principal": owner.principal_id},
    )


def seed_custody(
    owner: Owner,
    *,
    enrollment_id: UUID,
    generation: int,
    state: str,
    uid: str | None,
    absent: bool = False,
    row_id: UUID | None = None,
) -> UUID:
    """A custody row written directly, standing in for a writer that crashed mid-protocol."""

    row_id = row_id or uuid.uuid4()
    pe.rows(
        "INSERT INTO curie.personal_claude_custody (id, tenant_id, principal_id, enrollment_id, "
        "generation, secret_namespace, secret_name, secret_key, secret_uid, state, "
        "lease_expires_at, published_at, retired_at, secret_absent_at) VALUES (:id, :tenant, "
        ":principal, :enrollment, :generation, :namespace, :name, :key, :uid, :state, "
        "clock_timestamp() + interval '5 minutes', "
        "CASE WHEN :state IN ('published', 'retiring', 'retired') THEN clock_timestamp() END, "
        "CASE WHEN :state = 'retired' THEN clock_timestamp() END, "
        "CASE WHEN :absent THEN clock_timestamp() END)",
        {
            "id": row_id,
            "tenant": owner.tenant_id,
            "principal": owner.principal_id,
            "enrollment": enrollment_id,
            "generation": generation,
            "namespace": GRANT_NAMESPACE,
            "name": custody_store.custody_secret_name(enrollment_id, generation),
            "key": CUSTODY_SECRET_KEY,
            "uid": uid,
            "state": state,
            "absent": absent,
        },
    )
    return row_id


def custody_secret(
    store: FakeCustodyStore, row_id: UUID, enrollment_id: UUID, generation: int
) -> str:
    """The labelled, annotated Secret a custody row names, created in the fake."""

    return store.put(
        custody_store.custody_secret_name(enrollment_id, generation),
        labels=custody_store.custody_labels(enrollment_id, generation),
        annotations=custody_store.custody_annotations(row_id),
    )
