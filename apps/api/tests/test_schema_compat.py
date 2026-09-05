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
from alembic import command
from alembic.config import Config
from curie_api.config import get_settings
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
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"
HEAD = "0041"
PREV = "0040"


def _alembic_config() -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(ALEMBIC_DIR))
    return cfg


def _sql(sql: str, params: dict[str, Any] | None = None) -> list[Any]:
    async def run() -> list[Any]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(text(sql), params or {})
                return list(result.fetchall())
        finally:
            await engine.dispose()

    import asyncio

    return asyncio.run(run())


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
    window = load_window()
    assert window.schema_min == "0043"
    assert window.schema_head == "0043"
    kinds = load_kinds()
    assert kinds["0043"] == KIND_EXPAND
    assert kinds["0042"] == KIND_EXPAND
    assert kinds[HEAD] == KIND_CONTRACT
    assert kinds[PREV] == KIND_EXPAND
    assert kinds["0016"] == KIND_IRREVERSIBLE
    # The current ORM reads the 0042 authority and 0043 delivery tables. Those
    # additive migrations let an older API keep serving, not a newer API boot
    # before its required tables exist. Exercise the actual startup predicate.
    assert not can_serve("0041", window, kinds)
    assert not can_serve("0042", window, kinds)
    assert can_serve("0043", window, kinds)


def test_planner_refuses_0041_contract_without_forward_only() -> None:
    window = AppWindow(schema_min=HEAD, schema_head=HEAD)
    kinds = {PREV: KIND_EXPAND, HEAD: KIND_CONTRACT}
    decision = plan_upgrade(
        current_revision=PREV,
        window=window,
        kinds=kinds,
        pending=(HEAD,),
        forward_only=False,
    )
    assert decision.action == "refuse"
    assert decision.rollback_compatible is False
    assert decision.pending[0].revision == HEAD
    assert decision.pending[0].kind == KIND_CONTRACT
    assert "forward-only" in decision.reason.lower()


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
    window = AppWindow(schema_min=HEAD, schema_head=HEAD)
    kinds = load_kinds()
    decision = plan_upgrade(
        current_revision=None,
        window=window,
        kinds=kinds,
        pending=(HEAD,),
        forward_only=False,
    )
    assert decision.action == "apply"
    assert decision.rollback_compatible is False


def test_already_at_head_is_noop() -> None:
    window = AppWindow(schema_min=HEAD, schema_head=HEAD)
    decision = plan_upgrade(
        current_revision=HEAD,
        window=window,
        kinds={HEAD: KIND_CONTRACT},
        pending=(),
        forward_only=False,
    )
    assert decision.action == "noop"


def test_assert_servable_refuses_below_min(isolated_migration_db: None) -> None:
    import asyncio

    cfg = _alembic_config()
    command.upgrade(cfg, PREV)
    with pytest.raises(RuntimeError, match="below application min"):
        asyncio.run(assert_servable())
    for revision in (HEAD, "0042"):
        command.upgrade(cfg, revision)
        with pytest.raises(RuntimeError, match="below application min"):
            asyncio.run(assert_servable())
    command.upgrade(cfg, "0043")
    asyncio.run(assert_servable())


def test_n_minus_one_can_serve_an_unknown_newer_expand() -> None:
    future_expand = "0042"
    window = AppWindow(schema_min=HEAD, schema_head=HEAD)
    known = {HEAD, PREV}
    assert can_serve(future_expand, window, known) is True
    assert can_serve(HEAD, window, known) is True
    assert can_serve(PREV, window, known) is False
    assert can_serve(None, window, known) is False


def test_decision_json_is_redacted() -> None:
    window = AppWindow(schema_min=HEAD, schema_head=HEAD)
    decision = plan_upgrade(
        current_revision=PREV,
        window=window,
        kinds={HEAD: KIND_CONTRACT},
        pending=(HEAD,),
        forward_only=True,
    )
    payload = json.dumps(render_decision(decision))
    lowered = payload.lower()
    assert "postgresql" not in lowered
    assert "password" not in lowered
    assert "database_url" not in lowered
    assert PREV in payload
    assert HEAD in payload


def test_0041_contract_requires_forward_only_and_closes_n_minus_one_window(
    isolated_migration_db: None,
) -> None:
    """0041 cannot run while N-1 remains eligible to serve the database."""
    cfg = _alembic_config()
    command.upgrade(cfg, PREV)
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
    assert current_revision() == "0043"

    rows = _sql(
        "SELECT summary FROM curie.approvals WHERE id = :id",
        {"id": approval_id},
    )
    assert rows == [("seeded before contract",)]

    # The contract migration preserves rows, but its new schema is N-only.
    col = _sql("SELECT outcome_history_ready_at FROM curie.publications LIMIT 0")
    assert col == []
    pubs = _sql(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'publications' "
        "AND column_name = 'outcome_history_ready_at'"
    )
    assert pubs, "0041 contract column must exist after upgrade"

    # The current application requires the later additive delivery tables too;
    # its window still excludes the database before the 0041 contract.
    n = load_window()
    assert n.schema_min == "0043"
    assert n.schema_head == "0043"
    assert can_serve(PREV, n, load_kinds()) is False


def test_crash_retry_does_not_double_apply(
    isolated_migration_db: None, tmp_path: Path
) -> None:
    """Two pending expands: first lands, second raises, retry resumes.

    Alembic's version table is the durable phase boundary. The first
    revision must not run again; the unique insert in the second must
    land once.
    """
    cfg = _alembic_config()
    command.upgrade(cfg, HEAD)
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
        '''
revision = "compat_probe_first"
down_revision = "0041"

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
    rows = _sql("SELECT rev FROM curie.compat_probe ORDER BY rev")
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
    rows = _sql("SELECT rev FROM curie.compat_probe ORDER BY rev")
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
    rows = _sql("SELECT rev FROM curie.compat_probe ORDER BY rev")
    assert [r[0] for r in rows] == ["compat_probe_first", "compat_probe_second"]
