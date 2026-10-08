"""Database/application compatibility window (#2300).

The planner, serve check, expand rollback, irreversible refusal, redacted
output, and crash/retry resume are the behavior this module pins. Production
code lives in ``curie_api.schema_compat``; this file never migrates by calling
``alembic upgrade head`` from an API pod startup path.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from _migration_support import ALEMBIC_DIR, IsolatedMigrationDb, alembic_config, sql_rows
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from curie_api import schema_compat
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.schema_compat import (
    KIND_CONTRACT,
    KIND_EXPAND,
    KIND_IRREVERSIBLE,
    AppWindow,
    apply_upgrade,
    assert_servable,
    can_serve,
    current_revision,
    load_kinds,
    load_window,
    plan_upgrade,
    render_decision,
)
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from fastapi.testclient import TestClient
from redis import Redis
from redis.asyncio import Redis as AsyncRedis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

CONTRACT = "0041"
# @spec PROTECTED-HOOK-SOURCE-2: ledger-dependent candidate requires schema0076.
APP_SCHEMA_MIN = "0076"
REVIEW_SCHEMA_MIN = "0063"
PREV = "0040"


def _migration_head() -> str:
    heads = ScriptDirectory.from_config(alembic_config()).get_heads()
    assert len(heads) == 1, f"expected one migration head, found {heads}"
    return heads[0]


HEAD = _migration_head()


def _exec(sql: str, params: dict[str, Any] | None = None) -> None:
    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text(sql), params or {})
        finally:
            await engine.dispose()

    import asyncio

    asyncio.run(run())


def test_released_application_declares_a_machine_readable_window() -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    window = load_window()
    assert window.schema_min == APP_SCHEMA_MIN
    assert window.schema_head == HEAD
    kinds = load_kinds()
    assert kinds[CONTRACT] == KIND_CONTRACT
    assert kinds[APP_SCHEMA_MIN] == KIND_EXPAND
    assert kinds["0070"] == KIND_CONTRACT
    assert kinds[REVIEW_SCHEMA_MIN] == KIND_CONTRACT
    if HEAD != APP_SCHEMA_MIN:
        assert kinds[HEAD] == KIND_EXPAND
    assert kinds[PREV] == KIND_EXPAND
    assert kinds["0016"] == KIND_IRREVERSIBLE


def test_planner_refuses_0041_contract_without_forward_only() -> None:
    window = AppWindow(schema_min=CONTRACT, schema_head=CONTRACT)
    kinds = {PREV: KIND_EXPAND, CONTRACT: KIND_CONTRACT}
    decision = plan_upgrade(
        current_revision=PREV,
        window=window,
        kinds=kinds,
        pending=(CONTRACT,),
        forward_only=False,
    )
    assert decision.action == "refuse"
    assert decision.rollback_compatible is False
    assert decision.pending[0].revision == CONTRACT
    assert decision.pending[0].kind == KIND_CONTRACT
    assert "forward-only" in decision.reason.lower()


def test_the_route_identity_contract_raises_the_floor_and_needs_forward_only() -> None:
    """@spec PROTECTED-HOOK-SOURCE-2.

    0070 (ADR-0168 decision 3) is a contract, as 0041 was: the app that
    stores `default` cannot serve a database whose 0024 check refuses it."""
    kinds = load_kinds()
    assert kinds["0070"] == KIND_CONTRACT
    retained_window = AppWindow(schema_min="0070", schema_head="0075")
    decision = plan_upgrade(
        current_revision="0069",
        window=retained_window,
        kinds=kinds,
        pending=("0070",),
        forward_only=False,
    )
    assert decision.action == "refuse"
    assert "0070" in decision.reason


def test_planner_refuses_irreversible_before_mutation() -> None:
    window = AppWindow(schema_min="0017", schema_head="0017")
    kinds = {"0016": KIND_IRREVERSIBLE, "0017": KIND_EXPAND}
    decision = plan_upgrade(
        current_revision="0015",
        window=window,
        kinds=kinds,
        pending=("0016", "0017"),
        forward_only=False,
    )
    assert decision.action == "refuse"
    assert decision.rollback_compatible is False
    assert "0016" in decision.reason
    assert "forward-only" in decision.reason.lower()


def test_planner_applies_irreversible_only_with_forward_only() -> None:
    window = AppWindow(schema_min="0017", schema_head="0017")
    kinds = {"0016": KIND_IRREVERSIBLE, "0017": KIND_EXPAND}
    decision = plan_upgrade(
        current_revision="0015",
        window=window,
        kinds=kinds,
        pending=("0016", "0017"),
        forward_only=True,
    )
    assert decision.action == "apply"
    assert decision.rollback_compatible is False
    assert decision.forward_only is True


def test_empty_database_install_does_not_refuse_historical_irreversible() -> None:
    window = AppWindow(schema_min=CONTRACT, schema_head=CONTRACT)
    kinds = load_kinds()
    decision = plan_upgrade(
        current_revision=None,
        window=window,
        kinds=kinds,
        pending=(CONTRACT,),
        forward_only=False,
    )
    assert decision.action == "apply"
    assert decision.rollback_compatible is False


def test_already_at_head_is_noop() -> None:
    window = AppWindow(schema_min=CONTRACT, schema_head=HEAD)
    decision = plan_upgrade(
        current_revision=HEAD,
        window=window,
        kinds=load_kinds(),
        pending=(),
        forward_only=False,
    )
    assert decision.action == "noop"


def test_assert_servable_refuses_below_min(isolated_migration_db: IsolatedMigrationDb) -> None:
    import asyncio

    cfg = alembic_config()
    isolated_migration_db.at(PREV)
    with pytest.raises(RuntimeError, match="below application min"):
        asyncio.run(assert_servable())
    command.upgrade(cfg, HEAD)
    asyncio.run(assert_servable())


def _prepare_schema_startup(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_REVIEW_INGRESS_ENABLED", "false")
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    monkeypatch.setenv("COMMIT_POLL_INTERVAL_S", "0")
    monkeypatch.setenv("OTEL_SDK_DISABLED", "true")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    get_settings.cache_clear()


@pytest.mark.parametrize("revision", ("0041", "0042", "0044", "0067"))
def test_api_lifespan_refuses_schema_missing_required_consumers(
    isolated_migration_db: IsolatedMigrationDb,
    monkeypatch: pytest.MonkeyPatch,
    revision: str,
) -> None:
    isolated_migration_db.at(revision)
    _prepare_schema_startup(monkeypatch)
    try:
        with pytest.raises(RuntimeError, match="below application min"):
            with TestClient(create_app()) as client:
                client.get("/health")
    finally:
        get_settings.cache_clear()


def test_api_lifespan_serves_current_schema_head(
    isolated_migration_db: IsolatedMigrationDb,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    isolated_migration_db.at(HEAD)
    _prepare_schema_startup(monkeypatch)
    try:
        with TestClient(create_app()) as client:
            response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
    finally:
        get_settings.cache_clear()


def test_adapter_0045_upgrade_preserves_principal_subject_through_work_items_and_dispatch(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """The stable adapter schema upgrades into the candidate WorkItems train."""
    cfg = alembic_config()
    isolated_migration_db.at("0045")
    assert current_revision() == "0045"

    approval_id = uuid.uuid4()
    audit_id = uuid.uuid4()
    _exec(
        "INSERT INTO curie.approvals (id, conversation_id, author, summary, "
        "reply_kind, reply_channel, reply_placeholder, dedupe_key, status) "
        "VALUES (:id, :conversation_id, :author, :summary, :reply_kind, "
        ":reply_channel, :reply_placeholder, :dedupe_key, 'pending')",
        {
            "id": approval_id,
            "conversation_id": "th-stable-0045-upgrade",
            "author": "U0EXAMPLE1",
            "summary": "seeded adapter audit",
            "reply_kind": "slack",
            "reply_channel": "C0EXAMPLE1",
            "reply_placeholder": None,
            "dedupe_key": uuid.uuid4().hex,
        },
    )
    _exec(
        "INSERT INTO curie.approval_audit_entries "
        "(id, approval_id, action, actor, decision, authorizer, authorized, "
        "principal_kind, principal_subject, authenticated) "
        "VALUES (:id, :approval_id, 'resolved', 'U0EXAMPLE1', 'approved', "
        "'ExplicitUserListAuthorizer', true, 'adapter', :principal_subject, true)",
        {
            "id": audit_id,
            "approval_id": approval_id,
            "principal_subject": "mail-adapter",
        },
    )

    command.upgrade(cfg, "head")
    # Head moves as expand-only revisions land. The assertions below are what
    # must survive that move: the adapter audit row and the work-item columns.
    assert current_revision() == HEAD
    assert sql_rows(
        "SELECT principal_kind, principal_subject "
        "FROM curie.approval_audit_entries WHERE id = :id",
        {"id": audit_id},
    ) == [("adapter", "mail-adapter")]
    assert sql_rows(
        "SELECT to_regclass('curie.work_items')::text, "
        "to_regclass('curie.execution_requests')::text"
    ) == [("curie.work_items", "curie.execution_requests")]
    assert sql_rows(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'execution_requests' "
        "AND column_name IN ('dispatch_generation', 'dispatch_owner') "
        "ORDER BY column_name"
    ) == [("dispatch_generation",), ("dispatch_owner",)]


def test_n_minus_one_can_serve_an_unknown_newer_expand() -> None:
    future_expand = "future-expand"
    window = AppWindow(schema_min=CONTRACT, schema_head=HEAD)
    known = {HEAD, CONTRACT, PREV}
    assert can_serve(future_expand, window, known) is True
    assert can_serve(HEAD, window, known) is True
    assert can_serve(CONTRACT, window, known) is True
    assert can_serve(PREV, window, known) is False
    assert can_serve(None, window, known) is False


@pytest.mark.parametrize("future_expand", ("0042", "0043", "0044", "0045"))
def test_0041_image_accepts_review_schema_expands_it_does_not_know(
    future_expand: str,
) -> None:
    window = AppWindow(schema_min=CONTRACT, schema_head=CONTRACT)
    known = {CONTRACT, PREV}
    assert can_serve(future_expand, window, known) is True
    assert can_serve(CONTRACT, window, known) is True
    assert can_serve(PREV, window, known) is False
    assert can_serve(None, window, known) is False


def test_decision_json_is_redacted() -> None:
    window = AppWindow(schema_min=CONTRACT, schema_head=CONTRACT)
    decision = plan_upgrade(
        current_revision=PREV,
        window=window,
        kinds={CONTRACT: KIND_CONTRACT},
        pending=(CONTRACT,),
        forward_only=True,
    )
    payload = json.dumps(render_decision(decision))
    lowered = payload.lower()
    assert "postgresql" not in lowered
    assert "password" not in lowered
    assert "database_url" not in lowered
    assert PREV in payload
    assert CONTRACT in payload


def test_0041_contract_requires_forward_only_and_closes_n_minus_one_window(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """0041 cannot run while N-1 remains eligible to serve the database."""
    cfg = alembic_config()
    isolated_migration_db.at(PREV)
    assert current_revision() == PREV

    approval_id = uuid.uuid4()
    _exec(
        "INSERT INTO curie.approvals (id, conversation_id, author, summary, "
        "reply_kind, reply_channel, reply_placeholder, dedupe_key, status) "
        "VALUES (:id, :conversation_id, :author, :summary, :reply_kind, "
        ":reply_channel, :reply_placeholder, :dedupe_key, 'pending')",
        {
            "id": approval_id,
            "conversation_id": "th-compat-2300",
            "author": "U1",
            "summary": "seeded before contract",
            "reply_kind": "slack",
            "reply_channel": "C0EXAMPLE1",
            "reply_placeholder": None,
            "dedupe_key": uuid.uuid4().hex,
        },
    )

    refused = apply_upgrade(
        forward_only=False, before_apply=lambda: None, alembic_config=cfg
    )
    assert refused.action == "refuse"
    assert refused.outcome == "refused"
    assert refused.rollback_compatible is False
    assert refused.pending[0].kind == KIND_CONTRACT
    assert current_revision() == PREV

    outcome = apply_upgrade(
        forward_only=True, before_apply=lambda: None, alembic_config=cfg
    )
    assert outcome.action == "apply"
    assert outcome.outcome == "applied"
    assert outcome.rollback_compatible is False
    assert outcome.forward_only is True
    assert current_revision() == HEAD

    rows = sql_rows(
        "SELECT summary FROM curie.approvals WHERE id = :id",
        {"id": approval_id},
    )
    assert rows == [("seeded before contract",)]

    # The contract migration preserves rows, but its new schema is N-only.
    col = sql_rows("SELECT outcome_history_ready_at FROM curie.publications LIMIT 0")
    assert col == []
    pubs = sql_rows(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'publications' "
        "AND column_name = 'outcome_history_ready_at'"
    )
    assert pubs, "0041 contract column must exist after upgrade"

    # Red-on-revert: the current image still closes the application rollback window.
    n = load_window()
    assert n.schema_min == APP_SCHEMA_MIN
    assert n.schema_head == HEAD
    assert can_serve(PREV, n, {PREV, CONTRACT, HEAD}) is False


def test_crash_retry_does_not_double_apply(
    isolated_migration_db: IsolatedMigrationDb, tmp_path: Path
) -> None:
    """Two pending expands: first lands, second raises, retry resumes.

    Alembic's version table is the durable phase boundary. The first
    revision must not run again; the unique insert in the second must
    land once.
    """
    isolated_migration_db.at(HEAD)
    _exec(
        "CREATE TABLE curie.compat_probe ("
        "rev text primary key, "
        "applied_at timestamptz not null default now())"
    )

    # Synthetic names cannot collide with future production migration numbers.
    alembic_copy = tmp_path / "alembic"
    shutil.copytree(ALEMBIC_DIR, alembic_copy)
    versions = alembic_copy / "versions"
    (versions / "compat_probe_first_compat_first.py").write_text(
        f'''
revision = "compat_probe_first"
down_revision = {HEAD!r}

def upgrade():
    from alembic import op
    op.execute(
        "INSERT INTO curie.compat_probe (rev) VALUES ('compat_probe_first')"
    )

def downgrade():
    from alembic import op
    op.execute("DELETE FROM curie.compat_probe WHERE rev = 'compat_probe_first'")
'''
    )
    (versions / "compat_probe_second_compat_second.py").write_text(
        '''
import os
revision = "compat_probe_second"
down_revision = "compat_probe_first"

def upgrade():
    from alembic import op
    if os.environ.get("CURIE_COMPAT_PROBE_CRASH") == "1":
        raise RuntimeError("injected crash after compat_probe_first")
    op.execute(
        "INSERT INTO curie.compat_probe (rev) VALUES ('compat_probe_second')"
    )

def downgrade():
    from alembic import op
    op.execute("DELETE FROM curie.compat_probe WHERE rev = 'compat_probe_second'")
'''
    )
    probe_cfg = Config()
    probe_cfg.set_main_option("script_location", str(alembic_copy))

    os.environ["CURIE_COMPAT_PROBE_CRASH"] = "1"
    try:
        with pytest.raises(RuntimeError, match="injected crash"):
            apply_upgrade(
                forward_only=False,
                before_apply=lambda: None,
                alembic_config=probe_cfg,
                window=AppWindow(schema_min=HEAD, schema_head="compat_probe_second"),
                kinds={
                    **load_kinds(),
                    "compat_probe_first": KIND_EXPAND,
                    "compat_probe_second": KIND_EXPAND,
                },
            )
    finally:
        os.environ.pop("CURIE_COMPAT_PROBE_CRASH", None)

    assert current_revision() == "compat_probe_first"
    rows = sql_rows("SELECT rev FROM curie.compat_probe ORDER BY rev")
    assert [r[0] for r in rows] == ["compat_probe_first"]

    outcome = apply_upgrade(
        forward_only=False,
        before_apply=lambda: None,
        alembic_config=probe_cfg,
        window=AppWindow(schema_min=HEAD, schema_head="compat_probe_second"),
        kinds={
            **load_kinds(),
            "compat_probe_first": KIND_EXPAND,
            "compat_probe_second": KIND_EXPAND,
        },
    )
    assert outcome.outcome == "applied"
    assert current_revision() == "compat_probe_second"
    rows = sql_rows("SELECT rev FROM curie.compat_probe ORDER BY rev")
    assert [r[0] for r in rows] == ["compat_probe_first", "compat_probe_second"]

    again = apply_upgrade(
        forward_only=False,
        before_apply=lambda: None,
        alembic_config=probe_cfg,
        window=AppWindow(schema_min=HEAD, schema_head="compat_probe_second"),
        kinds={
            **load_kinds(),
            "compat_probe_first": KIND_EXPAND,
            "compat_probe_second": KIND_EXPAND,
        },
    )
    assert again.action == "noop"
    rows = sql_rows("SELECT rev FROM curie.compat_probe ORDER BY rev")
    assert [r[0] for r in rows] == ["compat_probe_first", "compat_probe_second"]


_PAUSE_TEST_LEASE_S = 3.0
_PAUSE_TEST_INTERVAL_S = 1.0


@pytest.fixture
def pause_config(monkeypatch: pytest.MonkeyPatch, valkey: Redis) -> Iterator[dict[str, str]]:
    """Compress the real renewal clocks without replacing time or a store."""
    prefix = f"test:pause-migrate:{uuid.uuid4().hex}"
    settings = {
        "VALKEY_HOST": VALKEY_HOST,
        "VALKEY_PORT": str(VALKEY_PORT),
        "VALKEY_PASSWORD": VALKEY_PW,
        "VALKEY_TLS": "false",
        "CURIE_INSTALLATION_ID": "acme-test",
        "CURIE_UPGRADE_REVISION": "17",
        "CURIE_UPGRADE_LEGACY_QUIESCE": "false",
        "KEY_PREFIX": prefix,
    }
    monkeypatch.delenv("VALKEY_URL", raising=False)
    for name, value in settings.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(schema_compat, "PAUSE_LEASE_S", _PAUSE_TEST_LEASE_S)
    monkeypatch.setattr(schema_compat, "PAUSE_RENEW_INTERVAL_S", _PAUSE_TEST_INTERVAL_S)
    get_settings.cache_clear()
    keys = {
        "authoritative": f"{prefix}:upgrade:quiesce:acme-test",
        "legacy": f"{prefix}:upgrade:quiesce",
    }
    try:
        yield keys
    finally:
        valkey.delete(*keys.values())
        get_settings.cache_clear()


def _pause_probe(
    db: IsolatedMigrationDb,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    kind: str = KIND_EXPAND,
    noop: bool = False,
) -> tuple[threading.Event, threading.Event]:
    """Drive the real CLI planner and a real, deliberately slow Alembic effect."""
    db.at(HEAD)
    copy = tmp_path / "pause-alembic"
    shutil.copytree(ALEMBIC_DIR, copy)
    (copy / "versions" / "compat_pause_probe_probe.py").write_text(
        f'''
revision = "compat_pause_probe"
down_revision = {HEAD!r}

def upgrade():
    from alembic import op
    op.execute("SELECT pg_sleep(4.5)")
    op.execute("CREATE TABLE curie.compat_pause_probe (value integer primary key)")
    op.execute("INSERT INTO curie.compat_pause_probe (value) VALUES (17)")

def downgrade():
    from alembic import op
    op.execute("DROP TABLE curie.compat_pause_probe")
'''
    )
    cfg = Config()
    cfg.set_main_option("script_location", str(copy))
    original_apply = schema_compat.apply_upgrade

    def apply(*, forward_only: bool, before_apply: Callable[[], None]) -> Any:
        return original_apply(
            forward_only=forward_only,
            before_apply=before_apply,
            alembic_config=cfg,
            window=AppWindow(schema_min=HEAD, schema_head=HEAD if noop else "compat_pause_probe"),
            kinds={**load_kinds(), "compat_pause_probe": kind},
        )

    started = threading.Event()
    finished = threading.Event()
    original_upgrade = command.upgrade

    def upgrade(*args: Any, **kwargs: Any) -> Any:
        started.set()
        try:
            return original_upgrade(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(schema_compat, "apply_upgrade", apply)
    monkeypatch.setattr(command, "upgrade", upgrade)
    return started, finished


def _capture_pause_client(
    monkeypatch: pytest.MonkeyPatch,
    action: Callable[[AsyncRedis], Any],
) -> list[asyncio.Task[Any]]:
    """Observe the actual Redis factory; every command uses a real connection."""
    original = AsyncRedis.from_url
    tasks: list[asyncio.Task[Any]] = []

    def from_url(cls: type[AsyncRedis], url: str, **kwargs: Any) -> AsyncRedis:
        client = original(url, **kwargs)
        tasks.append(asyncio.create_task(action(client)))
        return client

    monkeypatch.setattr(AsyncRedis, "from_url", classmethod(from_url))
    return tasks


async def _repoint_pause_client(client: AsyncRedis, port: int) -> None:
    # The installed redis-py pool reset clears cached connections after disconnect.
    # Source: https://redis.readthedocs.io/en/stable/_modules/redis/asyncio/connection.html
    await client.connection_pool.disconnect()
    client.connection_pool.connection_kwargs["port"] = port
    client.connection_pool.reset()


def _observe_pause_renewals(
    monkeypatch: pytest.MonkeyPatch,
    boundary: tuple[threading.Event, threading.Event] | None,
) -> dict[str, Any]:
    """Record real Lua results, transport errors and independent expiry."""
    observed: dict[str, Any] = {
        "renewed": [],
        "during_apply": [],
        "errors": [],
        "inflight": False,
        "expired": [],
        "transport_failed": asyncio.Event(),
        "deadline_expired": asyncio.Event(),
    }
    original_renew = schema_compat.renew_pause
    original_lose = schema_compat._PauseAuthority.lose

    async def renew(*args: Any, **kwargs: Any) -> Any:
        observed["inflight"] = True
        try:
            result = await original_renew(*args, **kwargs)
        except asyncio.CancelledError:
            observed["errors"].append("CancelledError")
            raise
        except Exception as error:
            observed["errors"].append(type(error).__name__)
            observed["transport_failed"].set()
            raise
        else:
            if result == "renewed":
                observed["renewed"].append(time.monotonic())
                if boundary and boundary[0].is_set() and not boundary[1].is_set():
                    observed["during_apply"].append(time.monotonic())
            return result
        finally:
            observed["inflight"] = False

    def lose(authority: Any, reason: str) -> None:
        if reason == "expired":
            observed["expired"].append((time.monotonic(), observed["inflight"]))
            observed["deadline_expired"].set()
        original_lose(authority, reason)

    monkeypatch.setattr(schema_compat, "renew_pause", renew)
    monkeypatch.setattr(schema_compat._PauseAuthority, "lose", lose)
    return observed


def test_upgrade_renews_before_the_first_unreachable_postgres_probe_and_while_waiting(
    pause_config: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    valkey: Redis,
) -> None:
    key = pause_config["authoritative"]
    raw = '{"revision":17,"since":"retained"}'
    valkey.set(key, raw, px=30_000)
    renewals = _observe_pause_renewals(monkeypatch, None)
    observed: list[int] = []
    original = asyncpg.connect

    async def connect(*args: Any, **kwargs: Any) -> Any:
        observed.append(len(renewals["renewed"]))
        return await original(*args, **kwargs)

    monkeypatch.setattr(asyncpg, "connect", connect)
    monkeypatch.setattr(schema_compat, "POSTGRES_ATTEMPTS", 3)
    monkeypatch.setattr(schema_compat, "POSTGRES_RETRY_S", _PAUSE_TEST_INTERVAL_S * 1.25)
    with socket.socket() as unreachable:
        unreachable.bind(("127.0.0.1", 0))
        monkeypatch.setenv(
            "DATABASE_URL",
            "postgresql+asyncpg://postgres:EXAMPLE_NOT_A_SECRET@127.0.0.1:"
            f"{unreachable.getsockname()[1]}/postgres",
        )
        get_settings.cache_clear()
        assert schema_compat.main(["upgrade"]) == 1
    assert len(observed) == 3
    assert observed[0] >= 1, "a real same-revision renewal must precede the first probe"
    assert observed[1] > observed[0] and observed[2] > observed[1], observed
    assert valkey.get(key) == raw


@pytest.mark.parametrize("raw", (None, '{"revision":18}', "not-json"))
def test_upgrade_refuses_absent_or_foreign_before_any_postgres_probe(
    isolated_migration_db: IsolatedMigrationDb,
    pause_config: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    valkey: Redis,
    raw: str | None,
) -> None:
    isolated_migration_db.at(HEAD)
    key = pause_config["authoritative"]
    if raw is not None:
        valkey.set(key, raw, px=30_000)
    probes: list[object] = []
    original = asyncpg.connect

    async def connect(*args: Any, **kwargs: Any) -> Any:
        probes.append(object())
        return await original(*args, **kwargs)

    monkeypatch.setattr(asyncpg, "connect", connect)
    assert schema_compat.main(["upgrade"]) == 1
    assert not probes
    assert valkey.get(key) == raw
    assert current_revision() == HEAD


def test_upgrade_keeps_renewing_during_real_apply(
    isolated_migration_db: IsolatedMigrationDb,
    pause_config: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    valkey: Redis,
) -> None:
    boundary = _pause_probe(isolated_migration_db, tmp_path, monkeypatch)
    renewals = _observe_pause_renewals(monkeypatch, boundary)
    key = pause_config["authoritative"]
    valkey.set(key, '{"revision":17}', px=30_000)
    assert schema_compat.main(["upgrade"]) == 0
    assert current_revision() == "compat_pause_probe"
    assert sql_rows("SELECT value FROM curie.compat_pause_probe") == [(17,)]
    assert len(renewals["during_apply"]) >= 2, (
        "real renewal must continue during the 4.5-second apply"
    )


def test_upgrade_losing_authority_during_started_apply_joins_the_real_effect(
    isolated_migration_db: IsolatedMigrationDb,
    pause_config: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    valkey: Redis,
) -> None:
    started, _finished = _pause_probe(isolated_migration_db, tmp_path, monkeypatch)
    key = pause_config["authoritative"]
    valkey.set(key, '{"revision":17}', px=30_000)

    async def replace_authority(_client: AsyncRedis) -> None:
        while not started.is_set():
            await asyncio.sleep(0.01)
        valkey.set(key, '{"revision":18}', px=30_000)

    tasks = _capture_pause_client(monkeypatch, replace_authority)
    assert schema_compat.main(["upgrade"]) == 1
    assert tasks and tasks[0].done() and tasks[0].exception() is None
    assert current_revision() == "compat_pause_probe"
    assert sql_rows("SELECT value FROM curie.compat_pause_probe") == [(17,)]
    assert valkey.get(key) == '{"revision":18}'


@pytest.mark.parametrize("outcome", ("apply", "noop", "refuse"))
@pytest.mark.parametrize("lose_authority", (False, True))
def test_upgrade_checks_authority_after_planning_and_before_apply(
    isolated_migration_db: IsolatedMigrationDb,
    pause_config: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    valkey: Redis,
    outcome: str,
    lose_authority: bool,
) -> None:
    started, _finished = _pause_probe(
        isolated_migration_db,
        tmp_path,
        monkeypatch,
        kind=KIND_CONTRACT if outcome == "refuse" else KIND_EXPAND,
        noop=outcome == "noop",
    )
    key = pause_config["authoritative"]
    valkey.set(key, '{"revision":17}', px=30_000)
    if lose_authority:
        original = schema_compat.current_revision

        def revision() -> str | None:
            actual = original()
            valkey.set(key, '{"revision":18}', px=30_000)
            time.sleep(_PAUSE_TEST_LEASE_S)  # The real loop observes foreign authority.
            return actual

        monkeypatch.setattr(schema_compat, "current_revision", revision)
    expected = 1 if lose_authority else 2 if outcome == "refuse" else 0
    assert schema_compat.main(["upgrade"]) == expected
    applied = outcome == "apply" and not lose_authority
    assert started.is_set() is applied
    assert current_revision() == ("compat_pause_probe" if applied else HEAD)
    assert sql_rows("SELECT to_regclass('curie.compat_pause_probe') IS NOT NULL") == [(applied,)]


@pytest.mark.parametrize("recover", (False, True))
def test_initial_real_valkey_connection_failure_never_grants_permission_to_probe(
    isolated_migration_db: IsolatedMigrationDb,
    pause_config: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    valkey: Redis,
    recover: bool,
) -> None:
    isolated_migration_db.at(HEAD)
    renewals = _observe_pause_renewals(monkeypatch, None)
    key = pause_config["authoritative"]
    valkey.set(key, '{"revision":17}', px=30_000)
    recovered = False
    probes: list[bool] = []
    original_connect = asyncpg.connect

    async def connect(*args: Any, **kwargs: Any) -> Any:
        probes.append(recovered)
        return await original_connect(*args, **kwargs)

    monkeypatch.setattr(asyncpg, "connect", connect)
    with socket.socket() as unreachable:
        unreachable.bind(("127.0.0.1", 0))
        original_factory = AsyncRedis.from_url

        def from_url(cls: type[AsyncRedis], url: str, **kwargs: Any) -> AsyncRedis:
            client = original_factory(url, **kwargs)
            client.connection_pool.connection_kwargs["port"] = unreachable.getsockname()[1]

            async def restore() -> None:
                nonlocal recovered
                await asyncio.sleep(0.05)
                await _repoint_pause_client(client, VALKEY_PORT)
                recovered = True

            if recover:
                asyncio.create_task(restore())
            return client

        monkeypatch.setattr(AsyncRedis, "from_url", classmethod(from_url))
        assert schema_compat.main(["upgrade"]) == (0 if recover else 1)
    assert all(probes), "an initial transport exception is never confirmed pause ownership"
    assert bool(probes) is recover
    assert renewals["errors"], "the initial real connection attempt must actually fail"
    assert current_revision() == HEAD


@pytest.mark.parametrize("mode", ("recover", "expire", "late_recover", "stall"))
def test_real_valkey_transport_failure_respects_the_last_confirmation_deadline(
    isolated_migration_db: IsolatedMigrationDb,
    pause_config: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    valkey: Redis,
    mode: str,
) -> None:
    boundary = _pause_probe(isolated_migration_db, tmp_path, monkeypatch)
    started, _finished = boundary
    renewals = _observe_pause_renewals(monkeypatch, boundary)
    key = pause_config["authoritative"]
    valkey.set(key, '{"revision":17}', px=30_000)
    interrupted = False
    with socket.socket() as outage:
        outage.bind(("127.0.0.1", 0))
        if mode == "stall":
            outage.listen()  # Real TCP connects, but no process answers the RESP request.

        async def interrupt(client: AsyncRedis) -> None:
            nonlocal interrupted
            while not started.is_set():
                await asyncio.sleep(0.01)
            # Begin at a real confirmation after apply starts, so planning and
            # host scheduling cannot consume this test's outage budget.
            count = len(renewals["renewed"])
            while len(renewals["renewed"]) == count:
                await asyncio.sleep(0.01)
            if mode == "stall":
                client.connection_pool.connection_kwargs["socket_timeout"] = 15
            await _repoint_pause_client(client, outage.getsockname()[1])
            interrupted = True
            if mode == "recover":
                await renewals["transport_failed"].wait()
                await asyncio.sleep(_PAUSE_TEST_INTERVAL_S / 10)
                await _repoint_pause_client(client, VALKEY_PORT)
            elif mode == "late_recover":
                await renewals["deadline_expired"].wait()
                await _repoint_pause_client(client, VALKEY_PORT)

        tasks = _capture_pause_client(monkeypatch, interrupt)
        assert schema_compat.main(["upgrade"]) == (0 if mode == "recover" else 1)
    assert interrupted and tasks and tasks[0].done() and tasks[0].exception() is None
    if mode == "recover":
        assert renewals["errors"], "the successful neighbor must contain a real transport failure"
        assert not renewals["expired"]
    else:
        assert renewals["expired"], "unconfirmed authority must expire independently of apply"
    if mode == "stall":
        assert any(inflight for _, inflight in renewals["expired"])
        assert "CancelledError" in renewals["errors"]
        assert "TimeoutError" not in renewals["errors"], (
            "expiry must precede the stalled read timeout"
        )
    assert current_revision() == "compat_pause_probe"
    assert sql_rows("SELECT value FROM curie.compat_pause_probe") == [(17,)]


def test_moved_postgres_wait_preserves_safe_periodic_diagnostics_and_attempt_bound(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    assert schema_compat.POSTGRES_ATTEMPTS == 60
    assert schema_compat.POSTGRES_RETRY_S == 2
    assert schema_compat.POSTGRES_CONNECT_TIMEOUT_S == 2
    attempts: list[dict[str, Any]] = []
    original = asyncpg.connect

    async def connect(*args: Any, **kwargs: Any) -> Any:
        attempts.append(kwargs)
        return await original(*args, **kwargs)

    monkeypatch.setattr(asyncpg, "connect", connect)
    monkeypatch.setattr(schema_compat, "POSTGRES_RETRY_S", 0.001)
    with socket.socket() as unreachable:
        unreachable.bind(("127.0.0.1", 0))
        monkeypatch.setenv(
            "DATABASE_URL",
            "postgresql+asyncpg://postgres:EXAMPLE_NOT_A_SECRET@127.0.0.1:"
            f"{unreachable.getsockname()[1]}/postgres",
        )
        get_settings.cache_clear()
        assert asyncio.run(schema_compat.wait_for_postgres()) == 1
    output = capsys.readouterr()
    lines = (output.out + output.err).splitlines()
    error_class = "ConnectionRefusedError"
    assert lines == [f"Waiting for Postgres readiness; probe error class: {error_class}"] + [
        f"Still waiting for Postgres readiness after {n} of 60 attempts; "
        f"probe error class: {error_class}"
        for n in (10, 20, 30, 40, 50)
    ] + [
        "Postgres unavailable after 60 readiness attempts; "
        f"final probe error class: {error_class}"
    ]
    assert len(attempts) == 60
    assert all(item["timeout"] == 2 for item in attempts)
    assert "EXAMPLE_NOT_A_SECRET" not in output.out + output.err
    assert "postgresql" not in output.out + output.err
    get_settings.cache_clear()


def test_moved_postgres_wait_extracts_ssl_without_forwarding_it_as_a_server_setting(
    isolated_migration_db: IsolatedMigrationDb, monkeypatch: pytest.MonkeyPatch
) -> None:
    isolated_migration_db.at(HEAD)
    original = asyncpg.connect
    observed: list[tuple[str, dict[str, Any]]] = []

    async def connect(dsn: str, **kwargs: Any) -> Any:
        observed.append((dsn, kwargs))
        return await original(dsn, **kwargs)

    monkeypatch.setattr(asyncpg, "connect", connect)
    url = get_settings().database_url
    monkeypatch.setenv("DATABASE_URL", url + ("&" if "?" in url else "?") + "ssl=disable")
    get_settings.cache_clear()
    try:
        assert asyncio.run(schema_compat.wait_for_postgres()) == 0
        assert len(observed) == 1
        dsn, kwargs = observed[0]
        assert "postgresql+asyncpg" not in dsn
        assert "ssl=" not in dsn
        assert kwargs == {"timeout": 2, "ssl": "disable"}
    finally:
        get_settings.cache_clear()
