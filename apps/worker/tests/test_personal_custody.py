"""The trusted worker's personal custody reader (#5377, ADR 0268 section 4).

Real Postgres at head schema, never mocked: each test seeds a tenant, an active
principal, its enrollment header and the published custody row by SQL, and
removes them in ``finally``. Kubernetes is faked only behind ``SecretReader``
(and, for the production reader, behind the CoreV1 client it calls), per the
worker's fake-the-control-plane rule. Every grant here is a placeholder.
"""

from __future__ import annotations

import asyncio
import base64
import os
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import pytest
from curie_test_support.postgres import pg_connect_or_skip
from curie_worker.config import WorkerConfig
from curie_worker.personal_custody import (
    CUSTODY_ID_ANNOTATION,
    CustodySecret,
    GrantRefusal,
    KubernetesSecretReader,
    PersonalCustodyReader,
    PersonalGrant,
    SecretReadError,
    build_secret_reader,
    custody_secret_name,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

_DB_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres"
)
_SCHEMA = os.environ.get("TEST_DB_SCHEMA", "curie")
NAMESPACE = "acme-personal-grants"
GRANT = "sk-ant-oat01-PLACEHOLDER-owner-grant"
GENERATION = 3
UID = "11111111-aaaa-4bbb-8ccc-000000000001"


@dataclass
class Seeded:
    tenant_id: uuid.UUID
    principal_id: uuid.UUID
    enrollment_id: uuid.UUID
    custody_id: uuid.UUID
    generation: int

    @property
    def secret_name(self) -> str:
        return custody_secret_name(self.enrollment_id, self.generation)


@dataclass
class FakeSecrets:
    """``SecretReader`` over an in-memory grant namespace; records every read."""

    secrets: dict[tuple[str, str], CustodySecret] = field(default_factory=dict)
    reads: list[tuple[str, str]] = field(default_factory=list)
    on_read: Callable[[], None] | None = None
    error: Exception | None = None

    def read_secret(self, namespace: str, name: str) -> CustodySecret | None:
        self.reads.append((namespace, name))
        if self.on_read is not None:
            self.on_read()
        if self.error is not None:
            raise self.error
        return self.secrets.get((namespace, name))


def _secret(seeded: Seeded, **changes: Any) -> CustodySecret:
    """The Secret the API publishes for ``seeded``, per the shared contract."""

    secret = CustodySecret(
        uid=UID,
        type="Opaque",
        immutable=True,
        annotations={CUSTODY_ID_ANNOTATION: str(seeded.custody_id)},
        data={"CLAUDE_CODE_OAUTH_TOKEN": base64.b64encode(GRANT.encode()).decode()},
    )
    return replace(secret, **changes)


def _run(case: Callable[[AsyncEngine, Seeded], Awaitable[None]]) -> None:
    async def go() -> None:
        engine = create_async_engine(_DB_URL)
        try:
            await pg_connect_or_skip(engine)
            seeded = await _seed(engine)
            try:
                await case(engine, seeded)
            finally:
                await _cleanup(engine, seeded)
        finally:
            await engine.dispose()

    asyncio.run(go())


async def _seed(engine: AsyncEngine) -> Seeded:
    seeded = Seeded(
        tenant_id=uuid.uuid4(),
        principal_id=uuid.uuid4(),
        enrollment_id=uuid.uuid4(),
        custody_id=uuid.uuid4(),
        generation=GENERATION,
    )
    async with engine.begin() as conn:
        await conn.execute(
            text(
                f"INSERT INTO {_SCHEMA}.tenants (id, deployment_id, status) "
                "VALUES (:id, gen_random_uuid()::text, 'active')"
            ),
            {"id": seeded.tenant_id},
        )
        await conn.execute(
            text(
                f"INSERT INTO {_SCHEMA}.principals (id, tenant_id, idp_issuer, idp_subject, "
                "type, status) VALUES (:id, :tenant, 'https://idp.example', :subject, "
                "'human', 'active')"
            ),
            {"id": seeded.principal_id, "tenant": seeded.tenant_id, "subject": uuid.uuid4().hex},
        )
        await conn.execute(
            text(
                f"INSERT INTO {_SCHEMA}.personal_claude_enrollments "
                "(id, tenant_id, principal_id, generation, active, version) "
                "VALUES (:id, :tenant, :principal, :generation, true, :generation)"
            ),
            {
                "id": seeded.enrollment_id,
                "tenant": seeded.tenant_id,
                "principal": seeded.principal_id,
                "generation": seeded.generation,
            },
        )
        await _insert_custody(
            conn, seeded, custody_id=seeded.custody_id, generation=seeded.generation
        )
    return seeded


async def _insert_custody(
    conn: Any,
    seeded: Seeded,
    *,
    custody_id: uuid.UUID,
    generation: int,
    state: str = "published",
    namespace: str = NAMESPACE,
    uid: str = UID,
) -> None:
    await conn.execute(
        text(
            f"INSERT INTO {_SCHEMA}.personal_claude_custody (id, tenant_id, principal_id, "
            "enrollment_id, generation, secret_namespace, secret_name, secret_key, "
            "secret_uid, state, lease_expires_at, published_at) VALUES (:id, :tenant, "
            ":principal, :enrollment, :generation, :namespace, :name, "
            "'CLAUDE_CODE_OAUTH_TOKEN', :uid, :state, clock_timestamp(), clock_timestamp())"
        ),
        {
            "id": custody_id,
            "tenant": seeded.tenant_id,
            "principal": seeded.principal_id,
            "enrollment": seeded.enrollment_id,
            "generation": generation,
            "namespace": namespace,
            "name": custody_secret_name(seeded.enrollment_id, generation),
            "uid": uid,
            "state": state,
        },
    )


async def _cleanup(engine: AsyncEngine, seeded: Seeded) -> None:
    async with engine.begin() as conn:
        for statement in (
            f"DELETE FROM {_SCHEMA}.personal_claude_custody WHERE tenant_id = :tenant",
            f"DELETE FROM {_SCHEMA}.personal_claude_enrollments WHERE tenant_id = :tenant",
            f"DELETE FROM {_SCHEMA}.principals WHERE tenant_id = :tenant",
            f"DELETE FROM {_SCHEMA}.tenants WHERE id = :tenant",
        ):
            await conn.execute(text(statement), {"tenant": seeded.tenant_id})


async def _sql(engine: AsyncEngine, statement: str, **params: Any) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(statement.format(schema=_SCHEMA)), params)


def _reader(
    engine: AsyncEngine, secrets: FakeSecrets, *, namespace: str = NAMESPACE
) -> PersonalCustodyReader:
    return PersonalCustodyReader(engine, schema=_SCHEMA, namespace=namespace, secrets=secrets)


def _published(seeded: Seeded, **changes: Any) -> FakeSecrets:
    return FakeSecrets({(NAMESPACE, seeded.secret_name): _secret(seeded, **changes)})


def _refused(result: object, code: str) -> None:
    assert isinstance(result, GrantRefusal), result
    assert result.code == code
    assert result.classification == "authentication-required"


def test_reader_returns_the_grant_for_the_active_generation() -> None:
    async def case(engine: AsyncEngine, seeded: Seeded) -> None:
        secrets = _published(seeded)
        grant = await _reader(engine, secrets).read_grant(
            seeded.tenant_id, seeded.principal_id, GENERATION
        )

        assert isinstance(grant, PersonalGrant)
        assert (grant.tenant_id, grant.principal_id, grant.generation) == (
            seeded.tenant_id,
            seeded.principal_id,
            GENERATION,
        )
        assert grant.claim_env()["CLAUDE_CODE_OAUTH_TOKEN"] == GRANT
        assert secrets.reads == [(NAMESPACE, seeded.secret_name)]

    _run(case)


def test_reader_refuses_absent_inactive_and_other_generations_before_reading_the_secret() -> None:
    async def case(engine: AsyncEngine, seeded: Seeded) -> None:
        secrets = _published(seeded)

        async def read(
            *, principal: uuid.UUID | None = None, generation: int = GENERATION, **kw: Any
        ) -> object:
            return await _reader(engine, secrets, **kw).read_grant(
                seeded.tenant_id, principal or seeded.principal_id, generation
            )

        _refused(await read(namespace=""), "custody_unconfigured")
        _refused(await read(principal=uuid.uuid4()), "enrollment_absent")
        _refused(await read(generation=GENERATION - 1), "generation_mismatch")
        _refused(await read(generation=GENERATION + 1), "generation_mismatch")
        # The published row retired without a newer one: the header names nothing.
        await _sql(
            engine,
            "UPDATE {schema}.personal_claude_custody SET state = 'retiring' WHERE id = :id",
            id=seeded.custody_id,
        )
        _refused(await read(), "custody_missing")
        await _sql(
            engine,
            "UPDATE {schema}.personal_claude_custody SET state = 'published' WHERE id = :id",
            id=seeded.custody_id,
        )
        await _sql(
            engine,
            "UPDATE {schema}.personal_claude_enrollments SET active = false WHERE id = :id",
            id=seeded.enrollment_id,
        )
        _refused(await read(), "enrollment_inactive")
        assert secrets.reads == []

    _run(case)


def test_reader_refuses_untrusted_namespace_and_misnamed_secret() -> None:
    async def case(engine: AsyncEngine, seeded: Seeded) -> None:
        # The row's namespace is not the one this worker trusts: never read.
        secrets = _published(seeded)
        _refused(
            await _reader(engine, secrets, namespace="another-namespace").read_grant(
                seeded.tenant_id, seeded.principal_id, GENERATION
            ),
            "custody_invalid",
        )
        assert secrets.reads == []
        # A same-name Secret that belongs to another custody row is not ours.
        foreign = _published(seeded, annotations={CUSTODY_ID_ANNOTATION: str(uuid.uuid4())})
        _refused(
            await _reader(engine, foreign).read_grant(
                seeded.tenant_id, seeded.principal_id, GENERATION
            ),
            "custody_invalid",
        )
        unannotated = _published(seeded, annotations={})
        _refused(
            await _reader(engine, unannotated).read_grant(
                seeded.tenant_id, seeded.principal_id, GENERATION
            ),
            "custody_invalid",
        )
        # The name the reader expects is the one migration 0160's CHECK computes.
        async with engine.connect() as conn:
            pinned = (
                await conn.execute(
                    text(
                        "SELECT 'curie-pe-' || replace(CAST(:enrollment AS uuid)::text, '-', '')"
                        " || '-g' || CAST(:generation AS bigint)::text"
                    ),
                    {"enrollment": seeded.enrollment_id, "generation": GENERATION},
                )
            ).scalar_one()
        assert seeded.secret_name == pinned

    _run(case)


@pytest.mark.parametrize(
    ("changes", "error", "code"),
    [
        ({"immutable": False}, None, "custody_invalid"),
        ({"immutable": None}, None, "custody_invalid"),
        ({"type": "kubernetes.io/basic-auth"}, None, "custody_invalid"),
        (
            {
                "data": {
                    "CLAUDE_CODE_OAUTH_TOKEN": base64.b64encode(GRANT.encode()).decode(),
                    "ANTHROPIC_API_KEY": base64.b64encode(b"PLACEHOLDER").decode(),
                }
            },
            None,
            "custody_invalid",
        ),
        ({"data": {}}, None, "custody_invalid"),
        (
            {"data": {"CLAUDE_CODE_OAUTH_TOKEN": base64.b64encode(b"  \n").decode()}},
            None,
            "custody_invalid",
        ),
        ({"data": {"CLAUDE_CODE_OAUTH_TOKEN": "not base64!"}}, None, "custody_invalid"),
        (
            {"data": {"CLAUDE_CODE_OAUTH_TOKEN": base64.b64encode(b"\xff\xfe").decode()}},
            None,
            "custody_invalid",
        ),
        ({"uid": "22222222-aaaa-4bbb-8ccc-000000000002"}, None, "custody_missing"),
        (None, None, "custody_missing"),
        ({}, SecretReadError("secret read failed with HTTP 403"), "custody_unreadable"),
        ({}, RuntimeError(f"transport echoed {GRANT}"), "custody_unreadable"),
    ],
    ids=[
        "mutable",
        "immutable-unset",
        "not-opaque",
        "extra-key",
        "no-key",
        "blank-value",
        "not-base64",
        "not-utf8",
        "uid-mismatch",
        "absent",
        "read-refused",
        "read-failed",
    ],
)
def test_reader_refuses_mutable_extra_key_blank_value_and_uid_mismatch(
    changes: dict[str, Any] | None, error: Exception | None, code: str
) -> None:
    async def case(engine: AsyncEngine, seeded: Seeded) -> None:
        secrets = FakeSecrets(error=error)
        if changes is not None:
            secrets.secrets[(NAMESPACE, seeded.secret_name)] = _secret(seeded, **changes)
        result = await _reader(engine, secrets).read_grant(
            seeded.tenant_id, seeded.principal_id, GENERATION
        )
        _refused(result, code)
        assert GRANT not in repr(result)
        assert secrets.reads == [(NAMESPACE, seeded.secret_name)]

    _run(case)


@pytest.mark.parametrize("move", ["replace", "disconnect"])
def test_reader_rechecks_the_header_after_the_secret_read(move: str) -> None:
    async def case(engine: AsyncEngine, seeded: Seeded) -> None:
        loop = asyncio.get_running_loop()

        async def advance() -> None:
            # The publish transaction a replacement or disconnect commits.
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        f"UPDATE {_SCHEMA}.personal_claude_custody SET state = 'retiring', "
                        "version = version + 1 WHERE id = :id"
                    ),
                    {"id": seeded.custody_id},
                )
                if move == "replace":
                    await _insert_custody(
                        conn,
                        seeded,
                        custody_id=uuid.uuid4(),
                        generation=GENERATION + 1,
                        uid="33333333-aaaa-4bbb-8ccc-000000000003",
                    )
                await conn.execute(
                    text(
                        f"UPDATE {_SCHEMA}.personal_claude_enrollments SET "
                        "generation = generation + 1, version = version + 1, "
                        "active = :active WHERE id = :id"
                    ),
                    {"id": seeded.enrollment_id, "active": move == "replace"},
                )

        secrets = _published(seeded)
        # The read runs on a worker thread; the hook commits on the test's loop.
        secrets.on_read = lambda: asyncio.run_coroutine_threadsafe(advance(), loop).result(10)

        result = await _reader(engine, secrets).read_grant(
            seeded.tenant_id, seeded.principal_id, GENERATION
        )

        _refused(result, "generation_mismatch" if move == "replace" else "enrollment_inactive")
        assert secrets.reads == [(NAMESPACE, seeded.secret_name)]

    _run(case)


def test_grant_repr_and_refusals_never_show_the_token() -> None:
    tenant, principal = uuid.uuid4(), uuid.uuid4()
    grant = PersonalGrant(tenant, principal, 4, GRANT)

    for rendered in (repr(grant), str(grant), f"{grant}", f"{grant!r}", f"{[grant]}"):
        assert GRANT not in rendered
        assert str(tenant) in rendered and str(principal) in rendered and "4" in rendered
    assert not hasattr(grant, "__dict__"), "the grant must live in a slot, not a dict"
    secret = CustodySecret(
        uid=UID,
        type="Opaque",
        immutable=True,
        annotations={},
        data={"CLAUDE_CODE_OAUTH_TOKEN": GRANT},
    )
    assert GRANT not in repr(secret)
    for code in ("custody_missing", "custody_invalid", "custody_unreadable"):
        assert GRANT not in repr(GrantRefusal(code))  # type: ignore[arg-type]


def test_claim_env_is_the_explicit_personal_pair() -> None:
    grant = PersonalGrant(uuid.uuid4(), uuid.uuid4(), 2, GRANT)

    env = grant.claim_env()

    assert env == {"CURIE_MODEL_AUTH_MODE": "personal", "CLAUDE_CODE_OAUTH_TOKEN": GRANT}
    env.clear()
    assert grant.claim_env()["CLAUDE_CODE_OAUTH_TOKEN"] == GRANT


def test_claim_env_grant_leaves_the_claim_through_the_per_claim_secret() -> None:
    """#5374's split moves the grant to ``<claim>-tokens``; only the mode stays."""

    from curie_worker.sandbox.claim_tokens import split_claim_tokens

    grant = PersonalGrant(uuid.uuid4(), uuid.uuid4(), 3, GRANT)

    tokens, rest = split_claim_tokens({"CURIE_SESSION_ID": "s", **grant.claim_env()})

    assert tokens == {"CLAUDE_CODE_OAUTH_TOKEN": GRANT}
    assert rest == {"CURIE_SESSION_ID": "s", "CURIE_MODEL_AUTH_MODE": "personal"}


def test_grant_namespace_is_worker_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CURIE_PERSONAL_GRANT_NAMESPACE", raising=False)
    assert WorkerConfig().personal_grant_namespace == ""
    monkeypatch.setenv("CURIE_PERSONAL_GRANT_NAMESPACE", NAMESPACE)
    assert WorkerConfig().personal_grant_namespace == NAMESPACE


# --- the production SecretReader over a faked CoreV1 control plane -----------------


class _CoreApi:
    """The one CoreV1 call the reader makes, answering a scripted outcome."""

    def __init__(self, outcome: object) -> None:
        self.outcome = outcome
        self.calls: list[tuple[str, str, object]] = []

    def read_namespaced_secret(self, name: str, namespace: str, **kwargs: object) -> object:
        self.calls.append((name, namespace, kwargs.get("_request_timeout")))
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome


def test_kubernetes_secret_reader_maps_absence_failure_and_the_secret() -> None:
    from kubernetes.client import V1ObjectMeta, V1Secret
    from kubernetes.client.exceptions import ApiException

    secret = V1Secret(
        metadata=V1ObjectMeta(name="n", uid=UID, annotations={CUSTODY_ID_ANNOTATION: "row"}),
        type="Opaque",
        immutable=True,
        data={"CLAUDE_CODE_OAUTH_TOKEN": "Zw=="},
    )
    core = _CoreApi(secret)
    read = KubernetesSecretReader(core, timeout_s=4.0).read_secret(NAMESPACE, "n")
    assert read == CustodySecret(
        uid=UID,
        type="Opaque",
        immutable=True,
        annotations={CUSTODY_ID_ANNOTATION: "row"},
        data={"CLAUDE_CODE_OAUTH_TOKEN": "Zw=="},
    )
    assert core.calls == [("n", NAMESPACE, 4.0)]

    assert KubernetesSecretReader(_CoreApi(ApiException(status=404))).read_secret("a", "b") is None
    failure = ApiException(status=500)
    failure.body = f"body echoing {GRANT}"
    with pytest.raises(SecretReadError) as refused:
        KubernetesSecretReader(_CoreApi(failure)).read_secret("a", "b")
    assert GRANT not in str(refused.value) and refused.value.__cause__ is None
    assert refused.value.__suppress_context__ is True


def test_build_secret_reader_uses_a_private_client_without_retries(tmp_path: Path) -> None:
    from kubernetes import client as k8s_client

    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text(
        "apiVersion: v1\nkind: Config\ncurrent-context: example\n"
        "clusters:\n- name: example\n  cluster:\n    server: https://cluster.example:6443\n"
        "contexts:\n- name: example\n  context:\n    cluster: example\n    user: example\n"
        "users:\n- name: example\n  user:\n    token: PLACEHOLDER-kube-token\n",
        encoding="utf-8",
    )
    before = k8s_client.Configuration.get_default_copy()

    reader = build_secret_reader(kubeconfig=str(kubeconfig), timeout_s=3.0)

    configuration = reader._core.api_client.configuration
    assert configuration.host == "https://cluster.example:6443"
    assert configuration.retries == 0
    assert reader._timeout_s == 3.0
    after = k8s_client.Configuration.get_default_copy()
    assert after.host == before.host
    assert after.api_key == before.api_key
