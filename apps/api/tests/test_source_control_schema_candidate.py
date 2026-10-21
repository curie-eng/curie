"""Ledger candidate prerequisite, @spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import json
import tomllib
import uuid
from pathlib import Path

import pytest
import yaml
from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts, sql_rows
from alembic import command
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.schema_compat import current_revision
from curie_protected_hooks.schema_serving import load_window
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
CATALOG = ROOT / "cli/src/application_schema_windows.json"


def catalog() -> dict:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    return json.loads(CATALOG.read_text())


@pytest.mark.parametrize("resource", ["api", "cli"])
def test_candidate_requires_deploy_notices_after_ledger_schema(resource: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    window = (
        json.loads((ROOT / "apps/api/src/curie_api/schema_compat.json").read_text())
        if resource == "api"
        else catalog()["candidate"]
    )
    # The next-only migrations follow the immutable published 0.12.2 chain.
    # The minimum tracks the newest schema the app reads (#2911's tenant scope);
    # the head is directory identity (0122), an expand above the join.
    assert window == {"schema_min": "0122", "schema_head": "0122"}


@pytest.mark.parametrize("field", ["cargo", "chart", "app"])
def test_current_release_fields_match_cli_version(field: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    expected = tomllib.loads((ROOT / "cli/Cargo.toml").read_text())["package"]["version"]
    if field == "cargo":
        value = expected
    else:
        chart = yaml.safe_load((ROOT / "charts/curie/Chart.yaml").read_text())
        value = chart["version" if field == "chart" else "appVersion"]
    assert value == "0.14.0"
    assert value in catalog()["windows"]


def test_new_candidate_has_its_own_window_and_append_only_revision() -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    data = catalog()
    assert data["windows"].get("0.12.2") == {"schema_min": "0076", "schema_head": "0081"}
    assert data["windows"].get("0.12.3") == {"schema_min": "0076", "schema_head": "0093"}
    assert data["windows"].get("0.12.4") == {"schema_min": "0076", "schema_head": "0093"}
    assert data["windows"].get("0.12.5") == {"schema_min": "0076", "schema_head": "0106"}
    assert data["windows"].get("0.13.0") == {"schema_min": "0108", "schema_head": "0108"}
    assert data["windows"].get("0.13.1") == {"schema_min": "0108", "schema_head": "0108"}
    assert data["windows"].get("0.13.2") == {"schema_min": "0108", "schema_head": "0108"}
    assert data["windows"].get("0.14.0") == {"schema_min": "0122", "schema_head": "0122"}
    assert data["revisions"][-15:] == [
        "0095",
        "0096",
        "0097",
        "0098",
        "0099",
        "0100",
        "0101",
        "0102",
        "0103",
        "0105",
        "0107",
        "0108",
        "0120",
        "0121",
        "0122",
    ]
    kinds = json.loads((ROOT / "apps/api/src/curie_api/revision_kinds.json").read_text())
    assert kinds["0102"] == "expand"
    assert kinds["0103"] == "contract"
    assert kinds["0105"] == "expand"
    assert kinds["0106"] == "expand"
    assert kinds["0107"] == "expand"
    assert kinds["0108"] == "expand"
    assert kinds["0120"] == "expand"
    assert kinds["0121"] == "expand"


def test_every_prior_registered_window_is_exactly_preserved() -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    # Published historical windows through 0.12.0 are pinned from spec-only2c638e50a.
    # Published 0.12.1 retains its ledger window when the feature train advances.
    expected = json.loads(
        (Path(__file__).parent / "fixtures/source_schema_prior_windows.json").read_text()
    )
    actual = catalog()["windows"]
    assert {name: actual[name] for name in expected} == expected
    assert actual["0.12.1"] == {"schema_min": "0076", "schema_head": "0076"}
    assert set(actual) - set(expected) <= {
        "0.12.1", "0.12.2", "0.12.3", "0.12.4", "0.12.5", "0.13.0", "0.13.1", "0.13.2",
        "0.14.0"
    }


def test_released_0076_startup_refuses_without_migrating_then_0101_starts(
    isolated_migration_db: IsolatedMigrationDb, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    isolated_migration_db.at("0076")
    for name, value in {
        "GITHUB_REVIEW_INGRESS_ENABLED": "false",
        "RESUME_RECONCILER_ENABLED": "false",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        "COMMIT_POLL_INTERVAL_S": "0",
        "OTEL_SDK_DISABLED": "true",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "",
    }.items():
        monkeypatch.setenv(name, value)
    get_settings.cache_clear()
    try:
        with pytest.raises(RuntimeError, match="below application min"):
            with TestClient(create_app()):
                pass
        assert current_revision() == "0076"
        # 0100 is still below the min: the Agent ORM reads 0101's columns (#2911).
        command.upgrade(alembic_config(), "0100")
        get_settings.cache_clear()
        with pytest.raises(RuntimeError, match="below application min"):
            with TestClient(create_app()):
                pass
        assert current_revision() == "0100"
        command.upgrade(alembic_config(), load_window().schema_head)
        assert current_revision() == load_window().schema_head
        with TestClient(create_app()) as client:
            response = client.get("/health")
            assert response.status_code == 200
            assert response.json() == {"status": "ok"}
    finally:
        get_settings.cache_clear()


@pytest.mark.parametrize("source", ["0105", "0107", "0106"])
def test_both_train_heads_upgrade_without_losing_existing_rows(
    source: str, isolated_migration_db: IsolatedMigrationDb
) -> None:
    """The join applies each train's expansion and preserves stored agent rows."""
    isolated_migration_db.at(source)
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, 'acme-retained')",
        {"id": agent_id},
    )
    with pytest.raises(RuntimeError, match="below application min"):
        with TestClient(create_app()):
            pass
    assert current_revision() == source

    command.upgrade(alembic_config(), "head")

    assert current_revision() == "0122"
    assert sql_dicts("SELECT name FROM curie.agents WHERE id = :id", {"id": agent_id}) == [
        {"name": "acme-retained"}
    ]
    columns = {
        (row["table_name"], row["column_name"])
        for row in sql_dicts(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = 'curie'"
        )
    }
    assert {("approvals", "reply_identity"), ("publications", "reply_identity")} <= columns
    assert {("agent_channels", "trigger_label"), ("work_items", "trigger_label")} <= columns
