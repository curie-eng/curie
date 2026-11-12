"""A personal WorkItem execution runs only as its owner, end to end (#5378, ADR 0268).

The real kernel claims through the real substrate over the fake Kubernetes
client. The personal execution service is built by ``run.build_personal_execution``
exactly as the worker builds it: the real ``BindingResolver`` and
``PersonalCustodyReader`` over real Postgres, and the owner launch lease on real
Valkey. Only the Kubernetes control plane (the claim API and the custody Secret
read), the dispatch API, Slack and the model behind the runner are doubles. The
dispatch double writes to the request row what the real API writes: an acquire
holds the waiting request for the acquiring worker, and a start refuses a moved
enrollment exactly as the API start does.

Each test seeds its own tenant, principal, enrollment, published custody row and
agent, and deletes everything it inserted.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import logging
import os
import secrets
import threading
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from aci_protocol import Final, SessionStatus, TextDelta
from curie_test_support.postgres import pg_connect_or_skip
from curie_worker import caller_token
from curie_worker import run as run_module
from curie_worker.binding import (
    CONNECTOR_CALLER_TOKEN_ENV,
    BindingResolver,
    FactoryContext,
    FactoryWake,
)
from curie_worker.config import WorkerConfig
from curie_worker.kernel import constants as kernel_constants
from curie_worker.kernel.failures import ThreadBusyError
from curie_worker.personal_custody import (
    CUSTODY_ID_ANNOTATION,
    CUSTODY_SECRET_KEY,
    CustodySecret,
    custody_secret_name,
)
from curie_worker.sandbox import SuspendedThreadError
from curie_worker.sandbox.claim_tokens import MODEL_AUTH_MODE_ENV, PERSONAL_TOKEN_ENV
from curie_worker.sandbox.types import (
    OWNER_PRINCIPAL_LABEL,
    ClaimProvenance,
    ClaimView,
    SandboxTermination,
)
from curie_worker.workitem_dispatch import (
    WorkItemAcquireGrant,
    WorkItemConflict,
    WorkItemStartGrant,
    WorkItemStartRefused,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from apps.worker.tests.queue_fixtures import factory_context, factory_conversation, qevent

from .conftest import FakeK8s
from .test_work_item_workspace import (
    DEPLOYMENT_ID,
    ISSUE_URL,
    WORK_ITEM_REPO,
    _Approvals,
    _Binding,
    _fail_settled_release,
    _NoExistingPublication,
    _thread_key,
    _turn,
    _WorkItems,
    _Workspace,
)

_DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    os.environ.get(
        "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres"
    ),
)
_SCHEMA = os.environ.get("TEST_DB_SCHEMA", "curie")
_NAMESPACE = "acme-personal-grants"
# A synthetic sentinel, never a real grant.
_GRANT = "sk-ant-oat01-PLACEHOLDER-kernel-enforcement-grant"
_SECRET_UID = "33333333-aaaa-4bbb-8ccc-000000000003"
_GENERATION = 7
_CLAIM_TIMEOUT_S = 3.0
_BINDING_ID = uuid.UUID("44444444-4444-4444-8444-444444444444")
_SCRIPT = [
    TextDelta(text="Working. "),
    Final(text="Working. Done.", status=SessionStatus.DONE),
]

Hook = Callable[[], Awaitable[None]]
_OWNER_COLUMNS = ", tenant_id, model_auth_mode, owner_principal_id, enrollment_generation"
_OWNER_VALUES = ", :tenant, 'personal', :owner, :generation"


# --- Postgres: one owner and its requests --------------------------------------


@dataclass
class _Secrets:
    """``SecretReader`` over the owner's one published grant; records every read."""

    custody_id: uuid.UUID
    secret_name: str
    reads: list[str] = field(default_factory=list)
    gone: bool = False
    mutable: bool = False

    def read_secret(self, namespace: str, name: str) -> CustodySecret | None:
        self.reads.append(name)
        if self.gone or (namespace, name) != (_NAMESPACE, self.secret_name):
            return None
        return CustodySecret(
            uid=_SECRET_UID,
            type="Opaque",
            immutable=not self.mutable,
            annotations={CUSTODY_ID_ANNOTATION: str(self.custody_id)},
            data={CUSTODY_SECRET_KEY: base64.b64encode(_GRANT.encode()).decode()},
        )


@dataclass
class _Owner:
    engine: AsyncEngine
    tenant_id: uuid.UUID
    principal_id: uuid.UUID
    agent_id: uuid.UUID
    enrollment_id: uuid.UUID
    secrets: _Secrets
    work_items: dict[uuid.UUID, uuid.UUID] = field(default_factory=dict)

    @property
    def provenance(self) -> ClaimProvenance:
        return ClaimProvenance(
            owner_tenant_id=str(self.tenant_id),
            owner_principal_id=str(self.principal_id),
            enrollment_generation=_GENERATION,
        )

    async def sql(self, statement: str, **params: Any) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(text(statement.format(schema=_SCHEMA)), params)

    async def row(self, statement: str, **params: Any) -> Mapping[str, Any] | None:
        async with self.engine.begin() as conn:
            found = (await conn.execute(text(statement.format(schema=_SCHEMA)), params)).first()
        return None if found is None else dict(found._mapping)

    async def status(self, request_id: uuid.UUID) -> str:
        found = await self.row(
            "SELECT status FROM {schema}.execution_requests WHERE id = :id", id=request_id
        )
        assert found is not None
        return str(found["status"])

    async def request(self, *, personal: bool = True) -> uuid.UUID:
        """A waiting, unacquired request; ``personal=False`` is a legacy-shaped
        service request whose rows name none of the owner columns."""

        item_id = await self._work_item(personal=personal)
        request_id = uuid.uuid4()
        await self.sql(
            "INSERT INTO {schema}.execution_requests (id, work_item_id, sequence, status, "
            "wait_deadline, execution_attempts" + (_OWNER_COLUMNS if personal else "") + ") "
            "VALUES (:id, :item, 1, 'waiting', clock_timestamp() + interval '1 hour', 0"
            + (_OWNER_VALUES if personal else "")
            + ")",
            id=request_id,
            item=item_id,
            **(self._owner_params() if personal else {}),
        )
        self.work_items[request_id] = item_id
        return request_id

    async def owner_lost_successor(self) -> uuid.UUID:
        """A WorkItem whose first request ended ``owner_lost`` (its runtime
        observed gone) and the waiting successor admitted for it (ADR 0206),
        both rows carrying the WorkItem's owner tuple as the API copies it."""

        item_id = await self._work_item(personal=True)
        lost, successor = uuid.uuid4(), uuid.uuid4()
        await self.sql(
            "INSERT INTO {schema}.execution_requests (id, work_item_id, sequence, status, "
            "wait_deadline, started_at, execution_deadline, terminal_at, terminal_cause, "
            "termination_observation, execution_attempts" + _OWNER_COLUMNS + ") "
            "VALUES (:id, :item, 1, 'failed', clock_timestamp() - interval '2 minutes', "
            "clock_timestamp() - interval '90 minutes', "
            "clock_timestamp() - interval '30 minutes', "
            "clock_timestamp() - interval '29 minutes', 'owner_lost', :observation, 1"
            + _OWNER_VALUES
            + ")",
            id=lost,
            item=item_id,
            observation=(
                "claims=acme-lost-claim sandboxes=sbx-acme-lost-claim "
                "absent_at=2026-11-11T00:00:00+00:00 observer=acme-worker-b"
            ),
            **self._owner_params(),
        )
        await self.sql(
            "INSERT INTO {schema}.execution_requests (id, work_item_id, sequence, status, "
            "wait_deadline, execution_attempts, owner_lost_retry" + _OWNER_COLUMNS + ") "
            "VALUES (:id, :item, 2, 'waiting', clock_timestamp() + interval '1 hour', 0, true"
            + _OWNER_VALUES
            + ")",
            id=successor,
            item=item_id,
            **self._owner_params(),
        )
        self.work_items[successor] = item_id
        return successor

    def _owner_params(self) -> dict[str, Any]:
        return {"tenant": self.tenant_id, "owner": self.principal_id, "generation": _GENERATION}

    async def _work_item(self, *, personal: bool) -> uuid.UUID:
        item_id = uuid.uuid4()
        await self.sql(
            "INSERT INTO {schema}.work_items (id, code_host_kind, code_host_host, "
            "tracker_kind, tracker_host, tracker_scope_id, repository_project_id, "
            "tracker_issue_id, code_host_installation_id, agent_id, repository_path, "
            "conversation_id, version, base_branch, base_source, base_commit"
            + (_OWNER_COLUMNS if personal else "")
            + ") VALUES (:id, 'github', 'github.com', 'github', 'github.com', '9001', "
            "'9001', :issue, 41, :agent, 'acme-corp/widgets', :conversation, 1, "
            "'main', 'default', :base" + (_OWNER_VALUES if personal else "") + ")",
            id=item_id,
            issue=str(uuid.uuid4().int % 10**9),
            agent=self.agent_id,
            conversation=f"conv-{item_id.hex[:12]}",
            base="a" * 40,
            **(self._owner_params() if personal else {}),
        )
        return item_id


@contextlib.asynccontextmanager
async def _seeded_owner() -> AsyncIterator[_Owner]:
    engine = create_async_engine(_DB_URL)
    try:
        await pg_connect_or_skip(engine)
        tenant_id, principal_id, agent_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        enrollment_id, custody_id = uuid.uuid4(), uuid.uuid4()
        secret_name = custody_secret_name(enrollment_id, _GENERATION)
        owner = _Owner(
            engine=engine,
            tenant_id=tenant_id,
            principal_id=principal_id,
            agent_id=agent_id,
            enrollment_id=enrollment_id,
            secrets=_Secrets(custody_id=custody_id, secret_name=secret_name),
        )
        try:
            await owner.sql(
                "INSERT INTO {schema}.tenants (id, deployment_id, status) "
                "VALUES (:id, gen_random_uuid()::text, 'active')",
                id=tenant_id,
            )
            await owner.sql(
                "INSERT INTO {schema}.principals (id, tenant_id, idp_issuer, idp_subject, "
                "type, status) VALUES (:id, :tenant, 'https://idp.example', :subject, "
                "'human', 'active')",
                id=principal_id,
                tenant=tenant_id,
                subject=uuid.uuid4().hex,
            )
            await owner.sql(
                "INSERT INTO {schema}.personal_claude_enrollments "
                "(id, tenant_id, principal_id, generation, active, version) "
                "VALUES (:id, :tenant, :principal, :generation, true, 1)",
                id=enrollment_id,
                tenant=tenant_id,
                principal=principal_id,
                generation=_GENERATION,
            )
            await owner.sql(
                "INSERT INTO {schema}.personal_claude_custody (id, tenant_id, principal_id, "
                "enrollment_id, generation, secret_namespace, secret_name, secret_key, "
                "secret_uid, state, lease_expires_at, published_at) VALUES (:id, :tenant, "
                ":principal, :enrollment, :generation, :namespace, :name, :key, :uid, "
                "'published', clock_timestamp(), clock_timestamp())",
                id=custody_id,
                tenant=tenant_id,
                principal=principal_id,
                enrollment=enrollment_id,
                generation=_GENERATION,
                namespace=_NAMESPACE,
                name=secret_name,
                key=CUSTODY_SECRET_KEY,
                uid=_SECRET_UID,
            )
            await owner.sql(
                "INSERT INTO {schema}.agents (id, name, tenant_id) VALUES (:id, :name, :tenant)",
                id=agent_id,
                name=f"acme-owner-{agent_id.hex[:8]}",
                tenant=tenant_id,
            )
            yield owner
        finally:
            for item_id in owner.work_items.values():
                await owner.sql(
                    "DELETE FROM {schema}.execution_requests WHERE work_item_id = :item",
                    item=item_id,
                )
                await owner.sql("DELETE FROM {schema}.work_items WHERE id = :item", item=item_id)
            for table, column, value in (
                ("agents", "id", agent_id),
                ("personal_claude_custody", "tenant_id", tenant_id),
                ("personal_claude_enrollments", "tenant_id", tenant_id),
                ("principals", "id", principal_id),
                ("tenants", "id", tenant_id),
            ):
                await owner.sql(
                    f"DELETE FROM {{schema}}.{table} WHERE {column} = :value", value=value
                )
    finally:
        await engine.dispose()


# --- Doubles --------------------------------------------------------------------


@dataclass
class _OwnerK8s(FakeK8s):
    """The fake control plane, recording each create's name and labels.

    ``after_owner_listing`` runs once, on the substrate's worker thread, right
    after the next owner inventory read has been answered.
    ``after_termination_read`` runs once, with the sandbox name, right after
    the next pod termination read for a dropped stream has been answered.
    """

    creates: list[tuple[str, dict[str, str]]] = field(default_factory=list)
    after_owner_listing: Callable[[], None] | None = None
    after_termination_read: Callable[[str], None] | None = None

    def create_claim(self, *args: Any, **kwargs: Any) -> None:
        self.creates.append((str(args[0]), dict(kwargs.get("labels") or {})))
        super().create_claim(*args, **kwargs)

    def list_claims(
        self, *, label_selector: str, request_timeout_seconds: float | None = None
    ) -> list[ClaimView]:
        views = super().list_claims(
            label_selector=label_selector, request_timeout_seconds=request_timeout_seconds
        )
        if label_selector.startswith(f"{OWNER_PRINCIPAL_LABEL}="):
            hook, self.after_owner_listing = self.after_owner_listing, None
            if hook is not None:
                hook()
        return views

    def pod_termination(
        self, name: str, *, request_timeout_seconds: float, since: datetime
    ) -> Any | None:
        found = super().pod_termination(
            name, request_timeout_seconds=request_timeout_seconds, since=since
        )
        hook, self.after_termination_read = self.after_termination_read, None
        if hook is not None:
            hook(name)
        return found

    def owner_creates(self, owner: _Owner) -> list[str]:
        principal = str(owner.principal_id)
        return [
            name for name, labels in self.creates if labels.get(OWNER_PRINCIPAL_LABEL) == principal
        ]


class _OwnerBinding(_Binding):
    """The kernel suite's binding double, for the seeded agent.

    With ``caller_key`` every boot carries a connector caller token that
    outlives any execution deadline, so a started run caps it through the
    credential-capping handoff.
    """

    def __init__(self, agent_id: uuid.UUID, *, caller_key: str | None = None) -> None:
        self.agent_id = agent_id
        self.caller_key = caller_key

    def _deployment(self) -> object:
        return SimpleNamespace(
            agent_id=self.agent_id,
            agent_name="acme-bot",
            deployment_id=DEPLOYMENT_ID,
            endpoint=None,
            adapter=None,
            approval_routes=None,
        )

    async def resolve_factory(self, context: FactoryContext) -> object:
        assert context.agent_id == self.agent_id
        return self._deployment()

    def boot_env(self, _resolved: object, thread_key: str, **_: object) -> dict[str, str]:
        env = super().boot_env(_resolved, thread_key, **_)
        if self.caller_key is not None:
            env[CONNECTOR_CALLER_TOKEN_ENV] = caller_token.mint(
                self.caller_key, agent="acme-bot", exp=int(time.time()) + 7200
            )
        return env


class _OwnerWorkspace(_Workspace):
    """The workspace double, forwarding a personal owner's create as the real
    coordinator does. ``during_preparation`` runs once, on the worker thread,
    after the kernel's entry authorization and before the substrate call."""

    def __init__(self, substrate: object) -> None:
        super().__init__(substrate)
        self.during_preparation: Callable[[], None] | None = None

    def claim_or_resume_with_handle(self, **kwargs: object) -> object:
        self.claimed.append(kwargs.get("repo_full_name"))
        hook, self.during_preparation = self.during_preparation, None
        if hook is not None:
            hook()
        personal = {} if kwargs.get("personal") is None else {"personal": kwargs["personal"]}
        common: dict[str, Any] = {
            "agent_name": kwargs.get("agent_name"),
            "workspace_repo": kwargs.get("repo_full_name"),
            "caller_run": kwargs.get("caller_run"),
            **personal,
        }
        thread_key = str(kwargs["thread_key"])
        substrate: Any = self.substrate
        if kwargs.get("replace_handle") is not None:
            handle = substrate.handoff(
                thread_key,
                expected=kwargs["replace_handle"],
                env=dict(kwargs.get("env") or {}),  # type: ignore[call-overload]
                **common,
            )
            return SimpleNamespace(handle=handle, prepared=None)
        try:
            handle = substrate.claim(thread_key, env=kwargs.get("env"), **common)
        except SuspendedThreadError:
            handle = substrate.resume(thread_key, env=kwargs.get("env"), **common)
        return SimpleNamespace(handle=handle, prepared=None)


class _PgWorkItems(_WorkItems):
    """Dispatch double that writes what the API writes to the request row.

    ``acquire`` holds the waiting request for the acquiring worker and wake
    generation. ``start`` refuses a moved enrollment exactly as the API start
    does (``enrollment_inactive``, ``enrollment_generation_mismatch``) and
    otherwise runs the request at the next runtime epoch, releasing the
    acquire. ``defer`` records the reason and releases the acquire.
    """

    def __init__(self, owner: _Owner) -> None:
        super().__init__()
        self.owner = owner
        self.defers: list[tuple[str, bool]] = []
        self.after_acquire: Hook | None = None
        self.before_start: Hook | None = None
        self.after_start: Hook | None = None

    async def acquire(
        self,
        request_id: uuid.UUID,
        *,
        owner: str,
        generation: int,
        wake: FactoryWake,
        context: FactoryContext | None,
    ) -> WorkItemAcquireGrant:
        self.calls.append("acquire")
        if request_id in self.starts or request_id in self.terminal_outcomes:
            raise WorkItemConflict("not_dispatchable")
        await self.owner.sql(
            "UPDATE {schema}.execution_requests SET "
            "dispatch_generation = GREATEST(dispatch_generation, :generation), "
            "acquire_owner = :owner, acquired_generation = :generation, "
            "acquire_expires_at = clock_timestamp() + interval '1 minute' "
            "WHERE id = :id AND status = 'waiting'",
            owner=owner,
            generation=generation,
            id=request_id,
        )
        existing = self.acquisitions.get(request_id)
        if existing is not None:
            return existing
        grant = WorkItemAcquireGrant(
            generation=generation,
            work_item_id=self.owner.work_items[request_id],
            conversation_id=factory_conversation(wake),
            wait_deadline=datetime.now(UTC) + timedelta(hours=1),
            repository_path=WORK_ITEM_REPO,
            context=factory_context(self.owner.agent_id, wake).model_copy(
                update={"tenant_id": self.owner.tenant_id, "binding_id": _BINDING_ID}
            ),
        )
        self.acquisitions[request_id] = grant
        hook, self.after_acquire = self.after_acquire, None
        if hook is not None:
            await hook()
        return grant

    async def start(
        self, request_id: uuid.UUID, *, context: FactoryContext, **kwargs: object
    ) -> WorkItemStartGrant:
        self.calls.append("start")
        assert context == self.acquisitions[request_id].context
        hook, self.before_start = self.before_start, None
        if hook is not None:
            await hook()
        enrollment = await self.owner.row(
            "SELECT r.model_auth_mode, r.enrollment_generation, e.generation AS current, "
            "e.active FROM {schema}.execution_requests r "
            "LEFT JOIN {schema}.personal_claude_enrollments e "
            "ON e.tenant_id = r.tenant_id AND e.principal_id = r.owner_principal_id "
            "WHERE r.id = :id",
            id=request_id,
        )
        assert enrollment is not None
        if enrollment["model_auth_mode"] == "personal":
            if enrollment["active"] is not True:
                raise WorkItemStartRefused("enrollment_inactive")
            if enrollment["current"] != enrollment["enrollment_generation"]:
                raise WorkItemStartRefused("enrollment_generation_mismatch")
        started_row = await self.owner.row(
            "UPDATE {schema}.execution_requests SET status = 'running', "
            "execution_attempts = 1, started_at = clock_timestamp(), "
            "execution_deadline = clock_timestamp() + interval '50 minutes', "
            "runtime_epoch = runtime_epoch + 1, runtime_owner = :owner, "
            "acquire_owner = NULL, acquired_generation = NULL, acquire_expires_at = NULL "
            "WHERE id = :id AND status = 'waiting' "
            "RETURNING runtime_epoch, execution_deadline",
            owner=str(kwargs.get("owner")),
            id=request_id,
        )
        assert started_row is not None
        started = WorkItemStartGrant(
            runtime_epoch=int(started_row["runtime_epoch"]),
            execution_deadline=started_row["execution_deadline"],
            remaining_s=3000.0,
            heartbeat_interval_s=60.0,
        )
        self.starts[request_id] = started
        hook, self.after_start = self.after_start, None
        if hook is not None:
            await hook()
        return started

    async def defer(self, request_id: uuid.UUID, **kwargs: Any) -> str | None:
        self.calls.append("defer")
        self.defers.append((str(kwargs["reason"]), bool(kwargs["capacity"])))
        await self.owner.sql(
            "UPDATE {schema}.execution_requests SET acquire_owner = NULL, "
            "acquired_generation = NULL, acquire_expires_at = NULL "
            "WHERE id = :id AND status = 'waiting'",
            id=request_id,
        )
        return None


# --- Harness wiring ---------------------------------------------------------------


def _harness(
    make_harness: Callable[..., Any],
    owner: _Owner,
    k8s: _OwnerK8s,
    monkeypatch: pytest.MonkeyPatch,
    *,
    caller_key: str | None = None,
    substrate: str = "kubernetes",
    foreign: bool = False,
    **overrides: object,
) -> Any:
    """A kernel harness whose Kernel carries the personal execution service
    ``run.py`` builds. Only the custody Secret read (the Kubernetes API) is
    replaced. ``foreign`` binds the authority to another tenant, as another
    installation's worker; ``substrate`` is the worker's sandbox substrate."""

    monkeypatch.setattr(run_module, "_LazySecretReader", lambda: owner.secrets)
    tenant = uuid.uuid4() if foreign else owner.tenant_id

    def personal_execution(redis: Any, config: WorkerConfig) -> object:
        return run_module.build_personal_execution(
            config,
            {"CURIE_SANDBOX_SUBSTRATE": substrate},
            engine=owner.engine,
            binding=BindingResolver(owner.engine, config, tenant_id=tenant),
            async_redis=redis,
            claim_timeout_seconds=_CLAIM_TIMEOUT_S,
        )

    return make_harness(
        binding=_OwnerBinding(owner.agent_id, caller_key=caller_key),
        workspace_factory=_OwnerWorkspace,
        publication_creator=_NoExistingPublication(),
        sandbox_client=k8s,
        claim_timeout_seconds=_CLAIM_TIMEOUT_S,
        db_schema=_SCHEMA,
        personal_grant_namespace=_NAMESPACE,
        personal_execution_factory=personal_execution,
        **({"connector_caller_signing_key": caller_key} if caller_key is not None else {}),
        **overrides,
    )


def _enforce(h: Any, owner: _Owner) -> _PgWorkItems:
    """Give the harness kernel the dispatch double and the default script."""

    work_items = _PgWorkItems(owner)
    h.kernel._work_items = work_items
    h.runner.default_script = list(_SCRIPT)
    return work_items


def _owner_key(h: Any, owner: _Owner) -> str:
    return f"{h.config.key_prefix}:personal-owner:{owner.tenant_id}:{owner.principal_id}"


def _execute(request_id: uuid.UUID, *, generation: int = 1, conversation: str | None = None) -> Any:
    return _turn(
        f"work-item-{request_id}-execute-{generation}",
        _PROMPT,
        **({"conversation_id": conversation} if conversation is not None else {}),
    )


_PROMPT = f"Resolve {ISSUE_URL}"
_CONTINUATIONS = (kernel_constants._EARLY_STOP_PROMPT, kernel_constants._UNPUBLISHED_PROMPT)


def _turns(runner: Any) -> list[str]:
    """The prompts ``runner`` was opened with, less the platform's continuation.

    An execute turn that delivers without publishing is re-prompted once on the
    same runtime (``kernel/publication.py``). That open makes no claim and reads
    no grant, so it is neither a launch nor a steer, and these counts leave it
    out. Every other open, including a duplicate request prompt, stays in.
    """

    return [text for text in runner.opened if not any(c in text for c in _CONTINUATIONS)]


def _bridge(loop: asyncio.AbstractEventLoop, step: Awaitable[Any]) -> Callable[[], None]:
    """A worker-thread hook that runs ``step`` on the test loop and waits for it."""

    async def run() -> None:
        await step

    def hook() -> None:
        asyncio.run_coroutine_threadsafe(run(), loop).result(timeout=30)

    return hook


# --- The authorized launch and its service control -------------------------------


def test_an_owned_personal_request_runs_as_its_owner_with_one_grant_read(
    make_harness, monkeypatch, caplog
) -> None:
    """AC1: the request's own snapshot selects the owner. The one create carries
    the owner's labels and grant, the grant is read once and never logged, and
    the owner launch lease is released once the claim is made."""

    caplog.set_level(logging.DEBUG)

    async def exercise() -> None:
        async with _seeded_owner() as owner:
            k8s = _OwnerK8s()
            async with _harness(make_harness, owner, k8s, monkeypatch) as h:
                work_items = _enforce(h, owner)
                request_id = await owner.request()

                await h.kernel.process_event(_execute(request_id))

                [(_name, labels)] = k8s.creates
                assert owner.provenance.labels().items() <= labels.items()
                [env] = k8s.claim_envs
                assert env is not None
                assert env[MODEL_AUTH_MODE_ENV] == "personal"
                assert env[PERSONAL_TOKEN_ENV] == _GRANT
                assert owner.secrets.reads == [owner.secrets.secret_name]
                assert work_items.calls.count("start") == 1
                assert _turns(h.runner) == [_PROMPT]
                assert await owner.status(request_id) == "running"
                assert await h.async_redis.exists(_owner_key(h, owner)) == 0

    asyncio.run(exercise())
    assert _GRANT not in caplog.text


def test_a_legacy_service_request_on_the_same_worker_carries_no_grant_and_no_owner(
    make_harness, monkeypatch
) -> None:
    """The control: a request whose rows predate the owner columns (NULL tenant
    snapshot, service by default) launches unchanged, with no grant read."""

    async def exercise() -> None:
        async with _seeded_owner() as owner:
            k8s = _OwnerK8s()
            async with _harness(make_harness, owner, k8s, monkeypatch) as h:
                work_items = _enforce(h, owner)
                request_id = await owner.request(personal=False)

                await h.kernel.process_event(_execute(request_id))

                [(_name, labels)] = k8s.creates
                assert OWNER_PRINCIPAL_LABEL not in labels
                [env] = k8s.claim_envs
                assert PERSONAL_TOKEN_ENV not in (env or {})
                assert owner.secrets.reads == []
                assert work_items.calls.count("start") == 1
                assert _turns(h.runner) == [_PROMPT]

    asyncio.run(exercise())


# --- Refusals before any claim or grant read --------------------------------------


async def _nothing(_owner: _Owner, _request_id: uuid.UUID) -> None:
    return None


async def _deactivate_enrollment(owner: _Owner, _request_id: uuid.UUID) -> None:
    await owner.sql(
        "UPDATE {schema}.personal_claude_enrollments SET active = false WHERE id = :id",
        id=owner.enrollment_id,
    )


async def _advance_enrollment(owner: _Owner, _request_id: uuid.UUID) -> None:
    await owner.sql(
        "UPDATE {schema}.personal_claude_enrollments SET generation = generation + 1 "
        "WHERE id = :id",
        id=owner.enrollment_id,
    )


async def _disable_principal(owner: _Owner, _request_id: uuid.UUID) -> None:
    await owner.sql(
        "UPDATE {schema}.principals SET status = 'disabled' WHERE id = :id", id=owner.principal_id
    )


async def _suspend_tenant(owner: _Owner, _request_id: uuid.UUID) -> None:
    await owner.sql(
        "UPDATE {schema}.tenants SET status = 'suspended' WHERE id = :id", id=owner.tenant_id
    )


async def _cancel_work_item(owner: _Owner, request_id: uuid.UUID) -> None:
    await owner.sql(
        "UPDATE {schema}.work_items SET cancelled_at = clock_timestamp() WHERE id = :id",
        id=owner.work_items[request_id],
    )


async def _acquired_by_another_worker(owner: _Owner, request_id: uuid.UUID) -> None:
    await owner.sql(
        "UPDATE {schema}.execution_requests SET acquire_owner = 'acme-worker-b' WHERE id = :id",
        id=request_id,
    )


_Change = Callable[[_Owner, uuid.UUID], Awaitable[None]]


@pytest.mark.parametrize(
    ("change", "wiring", "code"),
    [
        pytest.param(_deactivate_enrollment, {}, "enrollment_inactive", id="enrollment-inactive"),
        pytest.param(
            _advance_enrollment, {}, "enrollment_generation_mismatch", id="stale-generation"
        ),
        pytest.param(_disable_principal, {}, "owner_principal_inactive", id="principal-disabled"),
        pytest.param(_suspend_tenant, {}, "owner_tenant_inactive", id="tenant-suspended"),
        pytest.param(_cancel_work_item, {}, "execution_authority_lost", id="work-item-cancelled"),
        pytest.param(
            _acquired_by_another_worker,
            {},
            "execution_authority_lost",
            id="acquired-by-another-worker",
        ),
        pytest.param(
            _nothing, {"foreign": True}, "execution_owner_unknown", id="another-tenants-worker"
        ),
        pytest.param(
            _nothing,
            {"substrate": "docker"},
            "personal_mode_requires_kubernetes",
            id="off-kubernetes",
        ),
    ],
)
def test_a_refused_owner_defers_before_any_claim_or_grant_read(
    make_harness, monkeypatch, change: _Change, wiring: dict[str, Any], code: str
) -> None:
    """AC2 and AC3: the change lands between the acquire and the launch. The
    request stays waiting and defers ``not_started:<code>``; nothing is
    created, no grant is read and no turn opens."""

    async def exercise() -> None:
        async with _seeded_owner() as owner:
            k8s = _OwnerK8s()
            async with _harness(make_harness, owner, k8s, monkeypatch, **wiring) as h:
                work_items = _enforce(h, owner)
                request_id = await owner.request()

                async def after_acquire() -> None:
                    await change(owner, request_id)

                work_items.after_acquire = after_acquire

                await h.kernel.process_event(_execute(request_id))

                assert work_items.defers == [(f"not_started:{code}", False)]
                assert "start" not in work_items.calls
                assert k8s.creates == []
                assert owner.secrets.reads == []
                assert h.runner.opened == []
                assert await owner.status(request_id) == "waiting"

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("secret", "code"),
    [
        pytest.param({"gone": True}, "custody_missing", id="secret-gone"),
        pytest.param({"mutable": True}, "custody_invalid", id="secret-not-immutable"),
    ],
)
def test_a_custody_refusal_defers_authentication_required_with_no_create(
    make_harness, monkeypatch, secret: dict[str, bool], code: str
) -> None:
    """AC3: the grant is read inside the owner lease, after the inventory and
    before the create. A refused read creates nothing and names its code."""

    async def exercise() -> None:
        async with _seeded_owner() as owner:
            for name, value in secret.items():
                setattr(owner.secrets, name, value)
            k8s = _OwnerK8s()
            async with _harness(make_harness, owner, k8s, monkeypatch) as h:
                work_items = _enforce(h, owner)
                request_id = await owner.request()

                await h.kernel.process_event(_execute(request_id))

                assert owner.secrets.reads == [owner.secrets.secret_name]
                assert work_items.defers == [(f"not_started:{code}", False)]
                assert k8s.creates == []
                assert h.runner.opened == []
                assert await owner.status(request_id) == "waiting"
                assert await h.async_redis.exists(_owner_key(h, owner)) == 0

    asyncio.run(exercise())


def test_a_worker_restart_keeps_the_denial(make_harness, monkeypatch) -> None:
    """A new Kernel on the same Valkey and Postgres refuses the next wake the
    same way: the denial is the owner's rows, not process memory."""

    async def exercise() -> None:
        async with _seeded_owner() as owner:
            await _deactivate_enrollment(owner, uuid.uuid4())
            request_id = await owner.request()
            for generation in (1, 2):
                k8s = _OwnerK8s()
                async with _harness(make_harness, owner, k8s, monkeypatch) as h:
                    work_items = _enforce(h, owner)

                    await h.kernel.process_event(_execute(request_id, generation=generation))

                    assert work_items.defers == [("not_started:enrollment_inactive", False)]
                    assert k8s.creates == []
                    assert owner.secrets.reads == []

    asyncio.run(exercise())


# --- Fences after preparation and around the API start -----------------------------


def test_an_owner_disabled_during_workspace_preparation_never_reads_the_grant(
    make_harness, monkeypatch
) -> None:
    """The kernel authorizes at the claim entry; the principal is disabled while
    the workspace prepares. The grant read authorizes again first, so the
    refusal comes with no Secret read and no create."""

    async def exercise() -> None:
        async with _seeded_owner() as owner:
            k8s = _OwnerK8s()
            async with _harness(make_harness, owner, k8s, monkeypatch) as h:
                work_items = _enforce(h, owner)
                request_id = await owner.request()
                h.kernel._workspace.during_preparation = _bridge(
                    asyncio.get_running_loop(), _disable_principal(owner, request_id)
                )

                await h.kernel.process_event(_execute(request_id))

                assert h.kernel._workspace.claimed == [WORK_ITEM_REPO]
                assert work_items.defers == [("not_started:owner_principal_inactive", False)]
                assert owner.secrets.reads == []
                assert k8s.creates == []
                assert h.runner.opened == []

    asyncio.run(exercise())


def test_an_enrollment_moved_before_start_releases_the_claim_and_leaves_it_waiting(
    make_harness, monkeypatch
) -> None:
    """AC3: the enrollment advances after the claim exists and before the API
    start. The start is refused; the claim is deleted and observed gone and the
    request defers, still waiting, so it can be rebound."""

    async def exercise() -> None:
        async with _seeded_owner() as owner:
            k8s = _OwnerK8s()
            async with _harness(make_harness, owner, k8s, monkeypatch) as h:
                work_items = _enforce(h, owner)
                request_id = await owner.request()

                async def before_start() -> None:
                    await _advance_enrollment(owner, request_id)

                work_items.before_start = before_start

                await h.kernel.process_event(_execute(request_id))

                [created] = k8s.owner_creates(owner)
                assert created in k8s.deleted_claims
                assert k8s.claims == {}
                assert h.substrate._affinity.get(_thread_key()) is None
                assert work_items.defers == [("not_started:enrollment_generation_mismatch", False)]
                assert h.runner.opened == []
                assert await owner.status(request_id) == "waiting"

    asyncio.run(exercise())


@pytest.mark.parametrize("fenced", [False, True], ids=["authorized", "epoch-moved"])
def test_the_credential_capping_handoff_after_start_is_authorized_against_the_live_run(
    make_harness, monkeypatch, fenced: bool
) -> None:
    """Oracle advisory 2. A caller token that outlives the execution deadline is
    re-signed after the API start and the runtime handed off, re-entering the
    claim path with the acquire released and the epoch moved. Authorized: the
    successor is created under the owner lease with a fresh grant read, after
    its predecessor is gone. Epoch moved by a takeover between the start and
    the handoff: the handoff is refused and the run finishes escalated."""

    async def exercise() -> None:
        async with _seeded_owner() as owner:
            k8s = _OwnerK8s()
            key = base64.b64encode(secrets.token_bytes(32)).decode()
            async with _harness(make_harness, owner, k8s, monkeypatch, caller_key=key) as h:
                work_items = _enforce(h, owner)
                request_id = await owner.request()
                if fenced:

                    async def after_start() -> None:
                        await owner.sql(
                            "UPDATE {schema}.execution_requests "
                            "SET runtime_epoch = runtime_epoch + 1 WHERE id = :id",
                            id=request_id,
                        )

                    work_items.after_start = after_start

                await h.kernel.process_event(_execute(request_id))

                created = k8s.owner_creates(owner)
                assert work_items.calls.count("start") == 1
                if fenced:
                    assert len(created) == 1
                    assert len(owner.secrets.reads) == 1
                    assert h.runner.opened == []
                    [finish] = work_items.finishes
                    assert finish["outcome"] == "failed"
                    assert finish["cause"] == "runner_escalated"
                    assert "execution_authority_lost" in str(finish["detail"])
                else:
                    assert len(created) == 2
                    assert created[0] in k8s.deleted_claims
                    assert len(owner.secrets.reads) == 2
                    assert _turns(h.runner) == [_PROMPT]
                    assert all(f["cause"] != "runner_escalated" for f in work_items.finishes)

    asyncio.run(exercise())


_FENCES = [
    pytest.param(None, None, id="authorized"),
    pytest.param(_deactivate_enrollment, "enrollment_inactive", id="disconnected"),
    pytest.param(_advance_enrollment, "enrollment_generation_mismatch", id="advanced"),
]


@pytest.mark.parametrize(("fence", "code"), _FENCES)
def test_a_pod_restart_retry_rereads_custody_and_refuses_after_a_fence(
    make_harness, monkeypatch, fence: _Change | None, code: str | None
) -> None:
    """The started request's pod is deleted mid-turn (``sandbox-terminated``,
    retryable, no side effect) and its claim is gone with it. The retry inside
    the same delivery authorizes again and boots a fresh runtime with a fresh
    custody read. An enrollment fence that lands between the attempts refuses
    the retry before any grant read, create or turn, so the old grant is never
    used again, and the started run finishes ``authentication_required``."""

    async def exercise() -> None:
        async with _seeded_owner() as owner:
            k8s = _OwnerK8s()
            k8s.termination = SandboxTermination("Deleted")
            async with _harness(make_harness, owner, k8s, monkeypatch) as h:
                work_items = _enforce(h, owner)
                request_id = await owner.request()
                h.runner.turn_scripts = [[TextDelta(text="partial")]]
                h.runner.abort_after_frames = True
                loop = asyncio.get_running_loop()

                def pod_deleted(sandbox: str) -> None:
                    k8s.delete_claim(sandbox.removeprefix("sbx-"), request_timeout_seconds=1.0)
                    h.runner.abort_after_frames = False
                    if fence is not None:
                        _bridge(loop, fence(owner, request_id))()

                k8s.after_termination_read = pod_deleted

                await h.kernel.process_event(_execute(request_id))

                created = k8s.owner_creates(owner)
                assert k8s.termination_queries, "the first attempt must end sandbox-terminated"
                assert work_items.calls.count("start") == 1
                if fence is None:
                    assert len(created) == 2
                    assert len(owner.secrets.reads) == 2
                    assert _turns(h.runner) == [_PROMPT, _PROMPT]
                    assert all(f["cause"] != "authentication_required" for f in work_items.finishes)
                else:
                    assert len(created) == 1
                    assert len(k8s.claim_envs) == 1
                    assert len(owner.secrets.reads) == 1
                    assert len(h.runner.opened) == 1
                    [finish] = work_items.finishes
                    assert finish["outcome"] == "failed"
                    assert finish["cause"] == "authentication_required"
                    assert str(code) in str(finish["detail"])

    asyncio.run(exercise())


# --- Retained runtimes of another mode ----------------------------------------------


def test_a_service_runtime_on_the_thread_is_replaced_not_adopted_by_a_personal_request(
    make_harness, monkeypatch
) -> None:
    """A chat turn leaves an idle service runtime on the thread. The owner's
    request does not adopt it: the successor boots with the grant and the
    owner's labels, and the service runtime is retired."""

    async def exercise() -> None:
        async with _seeded_owner() as owner:
            k8s = _OwnerK8s()
            async with _harness(make_harness, owner, k8s, monkeypatch) as h:
                _enforce(h, owner)
                await h.kernel.process_event(_turn(f"slack-{uuid.uuid4()}", "hello there"))
                [(service_claim, service_labels)] = k8s.creates
                assert OWNER_PRINCIPAL_LABEL not in service_labels
                request_id = await owner.request()

                await h.kernel.process_event(_execute(request_id))

                assert len(k8s.creates) == 2
                assert k8s.owner_creates(owner) == [k8s.creates[1][0]]
                assert service_claim in k8s.deleted_claims
                assert PERSONAL_TOKEN_ENV not in (k8s.claim_envs[0] or {})
                assert (k8s.claim_envs[1] or {}).get(PERSONAL_TOKEN_ENV) == _GRANT
                assert _turns(h.runner) == ["hello there", _PROMPT]

    asyncio.run(exercise())


def test_a_chat_never_steers_into_a_live_personal_runtime(make_harness, monkeypatch) -> None:
    """The opposite direction. A service work item's live runtime is steered by a
    chat message (``test_chat_steers_a_live_work_item_turn_instead_of_replacing_it``);
    a personal one is never steered, adopted or replaced while live."""

    async def exercise() -> None:
        async with _seeded_owner() as owner:
            k8s = _OwnerK8s()
            async with _harness(make_harness, owner, k8s, monkeypatch) as h:
                _enforce(h, owner)
                _fail_settled_release(h)
                request_id = await owner.request()
                await h.kernel.process_event(_execute(request_id))
                assert len(k8s.claims) == 1
                h.runner.turn_active = True

                with pytest.raises(ThreadBusyError):
                    await h.kernel.process_event(_turn(f"slack-{uuid.uuid4()}", "hello there"))

                assert h.runner.steers == []
                assert len(k8s.creates) == 1
                assert len(k8s.claims) == 1
                assert _turns(h.runner) == [_PROMPT]

    asyncio.run(exercise())


# --- Continuations and successors keep the WorkItem's owner ------------------------

_AWAITING_APPROVAL = [
    Final(
        text="Requesting approval.",
        status=SessionStatus.AWAITING_APPROVAL,
        approval_summary="Run the requested command",
        approval_gate_kind="permission",
        approval_granted_tool="Bash",
    )
]


@pytest.mark.parametrize(("fence", "code"), _FENCES)
def test_a_ci_continuation_keeps_the_owner_and_refuses_after_a_fence_before_any_steer(
    make_harness, monkeypatch, fence: _Change | None, code: str | None
) -> None:
    """The owner's request started and is held on its runtime. A CI fix round
    adopts the same running request through ``_adopt_resumed_work_item`` and
    runs as the same owner. After a disconnect or a replaced enrollment it is
    refused at launch, before any steer, claim, grant read or tool, and the
    run finishes ``authentication_required`` at its own epoch."""

    async def exercise() -> None:
        async with _seeded_owner() as owner:
            k8s = _OwnerK8s()
            async with _harness(make_harness, owner, k8s, monkeypatch, approvals=_Approvals()) as h:
                work_items = _enforce(h, owner)
                request_id = await owner.request()
                h.runner.default_script = list(_AWAITING_APPROVAL)
                await h.kernel.process_event(_execute(request_id))
                [held] = [
                    run
                    for run in h.kernel._held_work_items.values()
                    if run.request_id == request_id
                ]
                assert held.started and not held.finished
                envs_before = list(k8s.claim_envs)
                if fence is not None:
                    await fence(owner, request_id)
                h.runner.default_script = list(_SCRIPT)

                await h.kernel.process_event(
                    _turn(
                        f"work-item-{request_id}-ci-2",
                        "Fix the failing check",
                        placeholder="ci-placeholder",
                    )
                )

                assert work_items.calls.count("acquire") == 1
                assert work_items.calls.count("start") == 1
                assert h.runner.steers == []
                assert [name for name, _ in k8s.creates] == k8s.owner_creates(owner)
                if fence is None:
                    assert len(h.runner.opened) == 2
                    assert all(f["cause"] != "authentication_required" for f in work_items.finishes)
                else:
                    assert len(h.runner.opened) == 1
                    assert k8s.claim_envs == envs_before
                    assert len(owner.secrets.reads) == 1
                    [finish] = work_items.finishes
                    assert finish["outcome"] == "failed"
                    assert finish["cause"] == "authentication_required"
                    assert finish["runtime_epoch"] == held.runtime_epoch
                    assert str(code) in str(finish["detail"])

    asyncio.run(exercise())


@pytest.mark.parametrize(("fence", "code"), _FENCES)
def test_an_owner_lost_successor_keeps_the_owner_and_refuses_after_a_fence(
    make_harness, monkeypatch, fence: _Change | None, code: str | None
) -> None:
    """The request after an ``owner_lost`` one carries the WorkItem's owner
    tuple, and the worker launches it from that snapshot alone: as the same
    owner and generation, or, after a disconnect or a replaced enrollment,
    not at all (``not_started:<code>``, before any claim or grant read)."""

    async def exercise() -> None:
        async with _seeded_owner() as owner:
            k8s = _OwnerK8s()
            async with _harness(make_harness, owner, k8s, monkeypatch) as h:
                work_items = _enforce(h, owner)
                successor = await owner.owner_lost_successor()
                if fence is not None:
                    await fence(owner, successor)

                await h.kernel.process_event(_execute(successor))

                if fence is None:
                    [(name, labels)] = k8s.creates
                    assert k8s.owner_creates(owner) == [name]
                    assert owner.provenance.labels().items() <= labels.items()
                    assert work_items.calls.count("start") == 1
                    assert _turns(h.runner) == [_PROMPT]
                else:
                    assert work_items.defers == [(f"not_started:{code}", False)]
                    assert k8s.creates == []
                    assert owner.secrets.reads == []
                    assert h.runner.opened == []
                    assert await owner.status(successor) == "waiting"

    asyncio.run(exercise())


# --- One native process per owner ---------------------------------------------------


def test_a_held_owner_lease_defers_owner_busy_before_any_grant_read(
    make_harness, monkeypatch
) -> None:
    """ADR 0268 decision 2: another worker holds the owner's launch lease. The
    request waits for it like capacity, with no grant read and no create."""

    async def exercise() -> None:
        async with _seeded_owner() as owner:
            k8s = _OwnerK8s()
            async with _harness(make_harness, owner, k8s, monkeypatch) as h:
                work_items = _enforce(h, owner)
                key = _owner_key(h, owner)
                await h.async_redis.set(key, "acme-worker-b", px=60_000)
                request_id = await owner.request()

                await h.kernel.process_event(_execute(request_id))

                assert work_items.defers == [("owner_busy", True)]
                assert "start" not in work_items.calls
                assert owner.secrets.reads == []
                assert k8s.creates == []
                assert h.runner.opened == []
                assert await h.async_redis.get(key) == "acme-worker-b"
                assert await owner.status(request_id) == "waiting"

    asyncio.run(exercise())


def test_two_workers_never_run_one_owner_twice_when_a_lease_is_lost_mid_inventory(
    make_harness, monkeypatch
) -> None:
    """Oracle advisory 3. Worker A lists the owner's claims (none) and stalls.
    Its lease is deleted from Valkey; worker B, a separate Kernel on the same
    Valkey, Postgres and cluster, launches the owner's other request. A then
    resumes, reads the grant, and its fence before the create finds the lease
    gone: it defers ``owner_busy`` and creates nothing."""

    async def exercise() -> None:
        async with _seeded_owner() as owner:
            k8s = _OwnerK8s()
            async with (
                _harness(
                    make_harness, owner, k8s, monkeypatch, consumer_name="acme-worker-a"
                ) as ha,
                _harness(
                    make_harness, owner, k8s, monkeypatch, consumer_name="acme-worker-b"
                ) as hb,
            ):
                work_a = _enforce(ha, owner)
                work_b = _enforce(hb, owner)
                request_a = await owner.request()
                request_b = await owner.request()
                loop = asyncio.get_running_loop()
                stalled, resume = threading.Event(), threading.Event()
                drop_lease = _bridge(loop, ha.async_redis.delete(_owner_key(ha, owner)))

                def stall() -> None:
                    drop_lease()
                    stalled.set()
                    assert resume.wait(timeout=60)

                k8s.after_owner_listing = stall
                task_a = asyncio.create_task(ha.kernel.process_event(_execute(request_a)))
                try:
                    assert await asyncio.to_thread(stalled.wait, 30)
                    await hb.kernel.process_event(
                        _execute(request_b, conversation="1700000000.000002")
                    )
                finally:
                    resume.set()
                await task_a

                assert len(k8s.owner_creates(owner)) == 1
                assert work_b.calls.count("start") == 1
                assert _turns(hb.runner) == [_PROMPT]
                assert work_a.defers == [("owner_busy", True)]
                assert "start" not in work_a.calls
                assert ha.runner.opened == []
                assert await owner.status(request_a) == "waiting"

    asyncio.run(exercise())


# --- Ownerless lanes and worker start ---------------------------------------------


@pytest.mark.parametrize("mode", ["personal", "service"])
def test_a_personal_installation_refuses_an_ownerless_turn_before_any_claim(
    make_harness, mode: str
) -> None:
    """ADR 0268 decision 1: a personal installation runs only as a WorkItem
    request's owner. A plain run is answered once and never claims; the same
    turn on a service installation runs as before."""

    async def exercise() -> None:
        async with make_harness(factory_model_auth_mode=mode) as h:
            await h.kernel.process_event(qevent("go", event_id=f"evt-ownerless-{mode}"))

            if mode == "personal":
                assert h.runner.opened == []
                assert h.fake_k8s.claim_envs == []
                assert kernel_constants._PERSONAL_OWNER_REQUIRED_REPLY in (h.sink.last_text or "")
            else:
                assert h.runner.opened == ["go"]
                assert len(h.fake_k8s.claim_envs) == 1

    asyncio.run(exercise())


@pytest.mark.parametrize("substrate", ["docker", "static"])
def test_a_personal_installation_refuses_to_start_off_kubernetes(substrate: str) -> None:
    """Worker start refuses before any engine, client or container exists."""

    env = {"CURIE_SANDBOX_SUBSTRATE": substrate}
    with pytest.raises(ValueError, match="^personal_mode_requires_kubernetes"):
        run_module.build(WorkerConfig(factory_model_auth_mode="personal"), env)
    # The controls: a personal installation on Kubernetes, and a service one here.
    run_module.refuse_personal_without_kubernetes(
        WorkerConfig(factory_model_auth_mode="personal"), {"CURIE_SANDBOX_SUBSTRATE": "kubernetes"}
    )
    run_module.refuse_personal_without_kubernetes(WorkerConfig(), env)
