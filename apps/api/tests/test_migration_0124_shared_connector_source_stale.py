"""Migration 0124 persists shared connector source staleness (ADR 0239, AC4).

Expand only, on 0123. The upgrade adds ``source_stale boolean NOT NULL DEFAULT
false`` to ``curie.shared_connector_bindings`` with the check
``shared_connector_bindings_source_stale_ck`` (``NOT source_stale OR custody =
'platform'``), so connector custody can never hold the flag. Existing rows read
false. The application role gets no new grant. The downgrade drops the
constraint and the column.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    constraint_exists,
    sql_dicts,
    stamped_revision,
)
from _shared_connector_support import (
    SERVER_IMAGE,
    SLOT,
    PgSession,
    Recipient,
    application_spec,
    binding_row,
    owner_connect_kwargs,
    random_installation,
    shared_policy,
    source_fingerprint,
)
from alembic import command
from alembic.script import ScriptDirectory

REVISION_KINDS = Path(__file__).resolve().parents[1] / "src/curie_api/revision_kinds.json"
TABLE = "shared_connector_bindings"
CONSTRAINT = "shared_connector_bindings_source_stale_ck"


def _seed_platform_binding() -> str:
    """One platform binding with its application, recipient and agent, as the owner."""

    owner = PgSession(**owner_connect_kwargs())
    try:
        agent_id, application, binding_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        installation = random_installation()
        owner.execute(
            "INSERT INTO curie.agents (id, name) VALUES ($1, $2)",
            agent_id,
            f"mig-0124-{agent_id.hex[:12]}",
        )
        owner.execute(
            "INSERT INTO curie.shared_connector_applications (id, installation_id, spec, "
            "server_image, credential_slots, provider_origins, snapshot_seal, policy, "
            "pin_digest, custody) VALUES ($1, $2, $3::jsonb, $4, $5::jsonb, $6::jsonb, NULL, "
            "$7::jsonb, $8, 'platform')",
            application,
            installation,
            json.dumps(application_spec(installation, str(application))),
            SERVER_IMAGE,
            json.dumps([SLOT]),
            json.dumps(["https://api.example.com"]),
            json.dumps(shared_policy()),
            "0" * 64,
        )
        recipient = Recipient.generate()
        owner.execute(
            "INSERT INTO curie.shared_connector_recipients "
            "(application_id, recipient_id, algorithm, public_key) "
            "VALUES ($1, $2, 'x25519-sealed-box', $3)",
            application,
            recipient.recipient_id,
            recipient.public_bytes,
        )
        owner.execute(
            "INSERT INTO curie.shared_connector_bindings (id, application_id, agent_id, "
            "provider_account, custody, credential_revision, recipient_id, ciphertext, "
            "source_kind, source_namespace, source_name, source_uid, source_resource_version, "
            "source_fingerprint) VALUES ($1, $2, $3, 'acct-primary', 'platform', 1, $4, $5, "
            "'kubernetes-secret', 'acme', 'acme-secret', 'uid-1', '100', $6)",
            binding_id,
            application,
            agent_id,
            recipient.recipient_id,
            recipient.seal(b'{"access_token": "migrated"}'),
            source_fingerprint("migrated"),
        )
        return str(binding_id)
    finally:
        owner.close()


def _column() -> list[dict[str, Any]]:
    return sql_dicts(
        "SELECT data_type, is_nullable, column_default FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = :table AND column_name = 'source_stale'",
        {"table": TABLE},
    )


def _without_flag(row: dict[str, Any]) -> dict[str, Any]:
    return {name: value for name, value in row.items() if name != "source_stale"}


def _flag_absent() -> None:
    assert "source_stale" not in column_names(TABLE)
    assert _column() == []
    assert not constraint_exists(CONSTRAINT)


def _flag_present() -> None:
    assert "source_stale" in column_names(TABLE)
    assert _column() == [{"data_type": "boolean", "is_nullable": "NO", "column_default": "false"}]
    assert constraint_exists(CONSTRAINT)
    (definition,) = sql_dicts(
        "SELECT pg_get_constraintdef(c.oid) AS definition, c.convalidated FROM pg_constraint c "
        "WHERE c.conrelid = to_regclass(:table) AND c.conname = :name",
        {"table": f"curie.{TABLE}", "name": CONSTRAINT},
    )
    assert definition["convalidated"] is True
    assert definition["definition"].startswith("CHECK (")
    assert "source_stale" in definition["definition"]
    assert "custody = 'platform'" in definition["definition"]


def test_0124_is_an_expand_revision_on_0123() -> None:
    revision = ScriptDirectory.from_config(alembic_config()).get_revision("0124")
    assert revision is not None
    assert revision.down_revision == "0123"
    assert json.loads(REVISION_KINDS.read_text())["0124"] == "expand"


def test_upgrade_adds_the_flag_false_for_existing_rows_and_the_downgrade_removes_it(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at("0123")
    _flag_absent()
    binding_id = _seed_platform_binding()
    before = binding_row(binding_id)
    assert "source_stale" not in before
    config = alembic_config()

    command.upgrade(config, "0124")
    assert stamped_revision() == "0124"
    _flag_present()
    upgraded = binding_row(binding_id)
    assert upgraded["source_stale"] is False
    assert _without_flag(upgraded) == before

    # The flag is writable for a platform binding, and the downgrade discards it.
    sql_dicts(
        "UPDATE curie.shared_connector_bindings SET source_stale = true WHERE id = :id",
        {"id": uuid.UUID(binding_id)},
    )
    assert binding_row(binding_id)["source_stale"] is True

    command.downgrade(config, "0123")
    assert stamped_revision() == "0123"
    _flag_absent()
    assert binding_row(binding_id) == before

    command.upgrade(config, "0124")
    assert stamped_revision() == "0124"
    _flag_present()
    restored = binding_row(binding_id)
    assert restored["source_stale"] is False
    assert _without_flag(restored) == before
