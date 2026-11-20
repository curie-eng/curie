"""Executable upgrade, constraints and guarded downgrade on isolated Postgres."""

from __future__ import annotations

import hashlib
import uuid
from typing import Any

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    sql_dicts,
    sql_rows,
    stamped_revision,
)
from alembic import command
from sqlalchemy.exc import DBAPIError, IntegrityError

REVISION = "0165"
BELOW = "0164"


def _insert(table: str, values: dict[str, Any]) -> None:
    sql_rows(
        f"INSERT INTO curie.{table} ({', '.join(values)}) "
        f"VALUES ({', '.join(':' + name for name in values)})",
        values,
    )


def _owners() -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    tenant, principal, other = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    _insert(
        "tenants", {"id": tenant, "deployment_id": f"acme-profile-{tenant}", "status": "active"}
    )
    _insert("tenants", {"id": other, "deployment_id": f"acme-profile-{other}", "status": "active"})
    _insert(
        "principals",
        {
            "id": principal,
            "tenant_id": tenant,
            "idp_subject": str(principal),
            "type": "human",
            "status": "active",
        },
    )
    return tenant, principal, other


def _profile(tenant: uuid.UUID, principal: uuid.UUID, **changes: Any) -> dict[str, Any]:
    return {
        "tenant_id": tenant,
        "principal_id": principal,
        "revision": 1,
        "text": "exact\n",
        "sha256": hashlib.sha256(b"exact\n").hexdigest(),
        "format_version": 1,
        **changes,
    }


def test_upgrade_existing_owners_and_reject_invalid_profiles(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    tenant, principal, other = _owners()
    before = sql_dicts("SELECT * FROM curie.principals WHERE id = :p", {"p": principal})
    command.upgrade(alembic_config(), REVISION)
    assert stamped_revision() == REVISION
    assert sql_dicts("SELECT * FROM curie.principals WHERE id = :p", {"p": principal}) == before
    assert sql_dicts("SELECT * FROM curie.requester_coding_profiles") == []
    for changes in (
        {"revision": 0},
        {"revision": -1},
        {"revision": 9223372036854775808},
        {"format_version": 0},
        {"format_version": 2},
        {"sha256": "f" * 64},
        {"sha256": hashlib.sha256(b"exact\n").hexdigest().upper()},
        {"sha256": None},
        {"text": None},
        {"text": ""},
        {"text": "exact\r\n", "sha256": hashlib.sha256(b"exact\r\n").hexdigest()},
        {"text": "x" * 16385, "sha256": hashlib.sha256(b"x" * 16385).hexdigest()},
        {"tenant_id": other},
    ):
        with pytest.raises(DBAPIError):
            _insert("requester_coding_profiles", _profile(tenant, principal, **changes))
        assert sql_dicts("SELECT * FROM curie.requester_coding_profiles") == []
    _insert("requester_coding_profiles", _profile(tenant, principal, revision=9223372036854775807))
    with pytest.raises(IntegrityError):
        _insert("requester_coding_profiles", _profile(tenant, principal))
    sql_rows("DELETE FROM curie.requester_coding_profiles")
    _insert("requester_coding_profiles", _profile(tenant, principal, text=None, sha256=None))
    assert sql_dicts("SELECT revision, text, sha256 FROM curie.requester_coding_profiles") == [
        {"revision": 1, "text": None, "sha256": None}
    ]
    _insert("requester_coding_policies", {"tenant_id": tenant})
    assert sql_dicts("SELECT enabled FROM curie.requester_coding_policies") == [{"enabled": True}]
    with pytest.raises(IntegrityError):
        _insert("requester_coding_policies", {"tenant_id": tenant, "enabled": False})
    with pytest.raises(IntegrityError):
        _insert("requester_coding_policies", {"tenant_id": uuid.uuid4(), "enabled": False})


@pytest.mark.parametrize("retained", ["profile", "clear", "disabled_policy", "enabled_policy"])
def test_downgrade_refuses_any_retained_document_or_policy_without_ddl(
    isolated_migration_db: IsolatedMigrationDb, retained: str
) -> None:
    isolated_migration_db.at(REVISION)
    tenant, principal, _ = _owners()
    if retained in {"profile", "clear"}:
        changes = {"text": None, "sha256": None} if retained == "clear" else {}
        _insert("requester_coding_profiles", _profile(tenant, principal, **changes))
    else:
        _insert(
            "requester_coding_policies",
            {"tenant_id": tenant, "enabled": retained == "enabled_policy"},
        )
    before = (
        sql_dicts("SELECT * FROM curie.requester_coding_profiles"),
        sql_dicts("SELECT * FROM curie.requester_coding_policies"),
    )
    with pytest.raises(RuntimeError, match="requester_coding_instructions_present"):
        command.downgrade(alembic_config(), BELOW)
    assert stamped_revision() == REVISION
    assert before == (
        sql_dicts("SELECT * FROM curie.requester_coding_profiles"),
        sql_dicts("SELECT * FROM curie.requester_coding_policies"),
    )
    sql_rows("DELETE FROM curie.requester_coding_profiles")
    sql_rows("DELETE FROM curie.requester_coding_policies")
    command.downgrade(alembic_config(), BELOW)
    assert stamped_revision() == BELOW
    assert (
        sql_dicts(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'curie' "
            "AND tablename IN ('requester_coding_profiles', 'requester_coding_policies')"
        )
        == []
    )
    command.upgrade(alembic_config(), REVISION)
    _insert("requester_coding_profiles", _profile(tenant, principal))
