"""Database/application compatibility window (#2300).

The planner, serve check, expand rollback, irreversible refusal, redacted
output, and crash/retry resume are the behavior this module pins. Production
code lives in ``curie_api.schema_compat``; this file never migrates by calling
``alembic upgrade head`` from an API pod startup path.
"""

from __future__ import annotations

import json
import os
import shutil
import uuid
from pathlib import Path
from typing import Any

import pytest
from _migration_support import ALEMBIC_DIR, IsolatedMigrationDb, alembic_config, sql_rows
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
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
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

CONTRACT = "0041"
# @spec PROTECTED-HOOK-SOURCE-2 and DEPLOY-NOTICE-RELEASE-1.
# Agent reads require deploy notifications after the published source-control ledger.
APP_SCHEMA_MIN = "0082"
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


@pytest.mark.parametrize("branch_head", ["0093", "0098"])
def test_upgrade_from_either_train_applies_the_missing_sibling_and_preserves_rows(
    isolated_migration_db: IsolatedMigrationDb, branch_head: str
) -> None:
    """A released stable database and a feature database converge at the merge."""
    isolated_migration_db.at(branch_head)
    agent_id = uuid.uuid4()
    _exec(
        "INSERT INTO curie.agents(id,name) VALUES (:id,'merge-example')",
        {"id": agent_id},
    )
    result = apply_upgrade(forward_only=False, alembic_config=alembic_config())
    assert result.action == "apply"
    assert result.outcome == "applied"
    assert result.rollback_compatible is True
    pending = {step.revision for step in result.pending}
    if branch_head == "0093":
        assert {"0082", "0091a", "0092a", "0093a", "0098", "0099"} <= pending
        assert not pending & {"0091", "0092", "0093"}
    else:
        assert pending == {"0091", "0092", "0093", "0099"}
    assert current_revision() == "0099"
    assert sql_rows("SELECT name FROM curie.agents WHERE id=:id", {"id": agent_id}) == [
        ("merge-example",)
    ]
    columns = {
        (table, column)
        for table, column in sql_rows(
            "SELECT table_name,column_name FROM information_schema.columns "
            "WHERE table_schema='curie'"
        )
    }
    assert {
        ("execution_requests", "owner_lost_retry"),
        ("execution_requests", "start_deferrals"),
        ("factory_terminal_notices", "sync_owner"),
        ("remediation_nominations", "execution_code"),
        ("remediation_nominations", "approval_reason"),
    } <= columns


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


def test_agent_reads_refuse_the_schema_before_deploy_notification_expand() -> None:
    """@spec DEPLOY-NOTICE-RELEASE-1."""
    window = load_window()
    known = set(load_kinds())
    for released in (
        "0070",
        "0071",
        "0072",
        "0073",
        "0075",
        "0076",
        "0079",
        "0080",
        "0081",
    ):
        assert can_serve(released, window, known) is False
    assert can_serve("0082", window, known) is True


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

    refused = apply_upgrade(forward_only=False, alembic_config=cfg)
    assert refused.action == "refuse"
    assert refused.outcome == "refused"
    assert refused.rollback_compatible is False
    assert refused.pending[0].kind == KIND_CONTRACT
    assert current_revision() == PREV

    outcome = apply_upgrade(forward_only=True, alembic_config=cfg)
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
