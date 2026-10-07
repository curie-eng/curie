"""Ledger candidate prerequisite, @spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

import pytest
import yaml
from _migration_support import IsolatedMigrationDb, alembic_config
from alembic import command
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.schema_compat import current_revision
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
CATALOG = ROOT / "cli/src/application_schema_windows.json"


def catalog() -> dict:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    return json.loads(CATALOG.read_text())


@pytest.mark.parametrize("resource", ["api", "cli"])
def test_candidate_requires_exact_ledger_schema(resource: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    window = (
        json.loads((ROOT / "apps/api/src/curie_api/schema_compat.json").read_text())
        if resource == "api"
        else catalog()["candidate"]
    )
    assert window == {"schema_min": "0076", "schema_head": "0091"}


@pytest.mark.parametrize("field", ["cargo", "chart", "app"])
def test_new_candidate_release_fields_are_0122(field: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    if field == "cargo":
        value = tomllib.loads((ROOT / "cli/Cargo.toml").read_text())["package"]["version"]
    else:
        chart = yaml.safe_load((ROOT / "charts/curie/Chart.yaml").read_text())
        value = chart["version" if field == "chart" else "appVersion"]
    assert value == "0.12.2"


def test_new_candidate_has_its_own_window_and_append_only_revision() -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    data = catalog()
    assert data["windows"].get("0.12.1") == {"schema_min": "0076", "schema_head": "0076"}
    assert data["windows"].get("0.12.2") == {"schema_min": "0076", "schema_head": "0081"}
    assert data["revisions"][-4:] == ["0079", "0080", "0081", "0091"]
    assert (
        json.loads((ROOT / "apps/api/src/curie_api/revision_kinds.json").read_text())["0076"]
        == "expand"
    )


def test_every_prior_registered_window_is_exactly_preserved() -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    # Published historical windows through 0.12.0 are pinned from spec-only2c638e50a.
    # The unpublished 0.12.1 window is promoted to the current candidate for release.
    expected = json.loads(
        (Path(__file__).parent / "fixtures/source_schema_prior_windows.json").read_text()
    )
    actual = catalog()["windows"]
    assert {name: actual[name] for name in expected} == expected
    assert actual["0.12.1"] == {"schema_min": "0076", "schema_head": "0076"}
    assert set(actual) - set(expected) <= {"0.12.1", "0.12.2"}


def test_0075_startup_refuses_without_migrating_then_0076_starts(
    isolated_migration_db: IsolatedMigrationDb, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    isolated_migration_db.at("0075")
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
        assert current_revision() == "0075"
        command.upgrade(alembic_config(), "0076")
        assert current_revision() == "0076"
        with TestClient(create_app()) as client:
            response = client.get("/health")
            assert response.status_code == 200
            assert response.json() == {"status": "ok"}
    finally:
        get_settings.cache_clear()
