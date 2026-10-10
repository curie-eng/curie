"""The tune refusal migration (automated remediation plan task 16).

@spec AUTOMATED-REMEDIATION-25

docs/superpowers/specs/2026-10-07-automated-remediation.md, with the maintainer
ruling of 2026-10-07: "an approved ``tune`` request ends ``refused`` with
``tune_execution_not_automated``, with no write call". The nomination's
``refusal_code`` therefore carries that code, so
``remediation_nominations_refusal_ck`` accepts exactly the frozen
``nomination_refusals`` of ``tests/vectors/remediation-codes.json``, which now
include it, and still refuses any other code.

The revision is ``0096``, directly on the remediation escalations revision
``0095`` (plan task 12), and is the only head. If another revision lands first,
renumber: only ``BELOW`` and ``HEAD`` move. Hand-written (ADR 0117). Every
identifier is a placeholder.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts, sql_rows
from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy.exc import IntegrityError

BELOW = "0095"
REVISION = "0096"
# #2911's tenant scope (0101) is the single head.
HEAD = "0101"
TUNE_REFUSAL = "tune_execution_not_automated"
REFUSAL_CHECK = "remediation_nominations_refusal_ck"

_CODES = json.loads(
    (
        Path(__file__).resolve().parents[3] / "tests" / "vectors" / "remediation-codes.json"
    ).read_text("utf-8")
)


def _revision() -> tuple[str, str]:
    script = ScriptDirectory.from_config(alembic_config())
    rev = script.get_revision(REVISION)
    assert rev is not None and rev.path, f"no alembic revision {REVISION}"
    source = Path(rev.path).read_text()
    assert REFUSAL_CHECK in source and TUNE_REFUSAL in source, (
        f"revision {REVISION} does not widen {REFUSAL_CHECK} to {TUNE_REFUSAL} "
        "(AUTOMATED-REMEDIATION-25)"
    )
    assert isinstance(rev.down_revision, str), rev.down_revision
    return rev.revision, rev.down_revision


def _nomination() -> uuid.UUID:
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"tune-{agent_id.hex[:8]}"},
    )
    event_id = f"event-{uuid.uuid4().hex[:10]}"
    sql_rows(
        "INSERT INTO curie.remediation_nomination_submissions "
        "(event_id, agent_id, hook, block_sha256) VALUES (:e, :agent_id, 'alerts', :sha)",
        {"e": event_id, "agent_id": agent_id, "sha": "cd" * 32},
    )
    nomination_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.remediation_nominations "
        "(id, agent_id, hook, event_id, action, kind, arguments, arguments_sha256, target, "
        "reason, state) VALUES (:id, :agent_id, 'alerts', :e, 'tune-alert-rule', 'tune', "
        '\'{"field":"threshold","rule":"example-claim-slow","value"\\:90}\', :sha, '
        "'example-rules:\"example-claim-slow\"', 'example reason', 'approved')",
        {"id": nomination_id, "agent_id": agent_id, "e": event_id, "sha": "ef" * 32},
    )
    return nomination_id


def _refuse(nomination_id: uuid.UUID, code: str) -> None:
    sql_rows(
        "UPDATE curie.remediation_nominations SET state = 'refused', refusal_code = :code, "
        "decided_at = now() WHERE id = :id",
        {"code": code, "id": nomination_id},
    )


def _refusal(nomination_id: uuid.UUID) -> str | None:
    (row,) = sql_dicts(
        "SELECT refusal_code FROM curie.remediation_nominations WHERE id = :id",
        {"id": nomination_id},
    )
    return row["refusal_code"]


def test_one_hand_written_revision_on_the_escalations_revision_and_the_only_head() -> None:
    """@spec AUTOMATED-REMEDIATION-25"""

    revision, down = _revision()
    assert (revision, down) == (REVISION, BELOW)
    assert ScriptDirectory.from_config(alembic_config()).get_heads() == [HEAD]


def test_the_frozen_nomination_refusals_include_the_tune_refusal() -> None:
    """@spec AUTOMATED-REMEDIATION-25: the vocabulary the check mirrors."""

    assert TUNE_REFUSAL in _CODES["nomination_refusals"]
    assert TUNE_REFUSAL in _CODES["approval_resolution_refusals"]


def test_the_refusal_check_accepts_every_frozen_refusal_and_nothing_else(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-25: every ``nomination_refusals`` code, the tune
    refusal included, is accepted; an unknown code violates the check.
    """

    revision, _ = _revision()
    isolated_migration_db.at(revision)
    nomination_id = _nomination()
    for code in _CODES["nomination_refusals"]:
        _refuse(nomination_id, code)
        assert _refusal(nomination_id) == code
    with pytest.raises(IntegrityError, match=REFUSAL_CHECK):
        _refuse(nomination_id, "tune_execution_looked_fine")


def test_below_the_revision_the_tune_refusal_is_not_accepted(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-25: the revision is what widens the check."""

    _, down = _revision()
    isolated_migration_db.at(down)
    nomination_id = _nomination()
    with pytest.raises(IntegrityError, match=REFUSAL_CHECK):
        _refuse(nomination_id, TUNE_REFUSAL)


def test_upgrade_downgrade_upgrade_round_trip_with_a_refused_tune(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-25: real Postgres. The downgrade succeeds with a
    nomination refused ``tune_execution_not_automated`` present (what becomes of
    that row is the revision's choice) and restores the narrower check; the
    upgrade accepts the code again.
    """

    revision, down = _revision()
    cfg = alembic_config()
    isolated_migration_db.at(revision)
    refused = _nomination()
    _refuse(refused, TUNE_REFUSAL)

    command.downgrade(cfg, down)
    assert (
        sql_dicts(
            "SELECT id FROM curie.remediation_nominations WHERE refusal_code = :code",
            {"code": TUNE_REFUSAL},
        )
        == []
    )
    other = _nomination()
    with pytest.raises(IntegrityError, match=REFUSAL_CHECK):
        _refuse(other, TUNE_REFUSAL)

    command.upgrade(cfg, revision)
    _refuse(other, TUNE_REFUSAL)
    assert _refusal(other) == TUNE_REFUSAL
