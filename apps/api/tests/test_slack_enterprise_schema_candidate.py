"""Grid organisation namespace schema candidate, @spec HTTP-INVOCATION-EXECUTION-21."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

ROOT = Path(__file__).resolve().parents[3]
MIGRATION = ROOT / "apps/api/alembic/versions/0139_slack_enterprise_namespaces.py"
CANDIDATE = {"schema_min": "0145", "schema_head": "0145"}


def load(relative: str) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-21."""
    return json.loads((ROOT / relative).read_text(encoding="utf-8"))


def script() -> ScriptDirectory:
    """@spec HTTP-INVOCATION-EXECUTION-21: the actual migration graph, not a copied list."""
    config = Config(str(ROOT / "apps/api/alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "apps/api/alembic"))
    return ScriptDirectory.from_config(config)


def test_migration_0139_keeps_parent_0138_below_factory_base_head() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-21: additive revision after approval provenance."""
    graph = script()
    assert graph.get_heads() == ["0145"]
    revision = graph.get_revision("0139")
    assert revision is not None and revision.down_revision == "0138"
    assert Path(revision.path).resolve() == MIGRATION.resolve()
    known = {item.revision for item in graph.walk_revisions()}
    assert not known & {"0134", "0135"}


@pytest.mark.parametrize(
    "relative,extract",
    [
        ("cli/src/application_schema_windows.json", lambda value: value["candidate"]),
        ("apps/api/src/curie_api/schema_compat.json", lambda value: value),
        (
            "packages/protected-hooks/src/curie_protected_hooks/schema_serving.json",
            lambda value: {key: value[key] for key in ("schema_min", "schema_head")},
        ),
        (
            "charts/curie/files/schema-compat.json",
            lambda value: {key: value[key] for key in ("schema_min", "schema_head")},
        ),
    ],
)
def test_every_coupled_candidate_consumer_requires_the_head(relative: str, extract: Any) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-21: Grid reports write slack_enterprise namespaces."""
    assert extract(load(relative)) == CANDIDATE


def test_catalog_kind_parent_and_chart_digest_name_the_exact_migration() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-21: exact graph parent and file digest."""
    revisions = load("cli/src/application_schema_windows.json")["revisions"]
    assert revisions[-7:] == ["0138", "0139", "0141", "0143", "0144", "0140", "0145"]
    kinds = load("apps/api/src/curie_api/revision_kinds.json")
    assert kinds["0139"] == "expand"
    assert kinds["0143"] == "expand"
    assert kinds["0144"] == "expand"
    assert kinds["0145"] == "expand"
    serving = load("packages/protected-hooks/src/curie_protected_hooks/schema_serving.json")
    assert serving["revision_parents"]["0139"] == ["0138"]
    assert serving["revision_parents"]["0143"] == ["0141"]
    assert serving["revision_parents"]["0144"] == ["0143"]
    assert serving["revision_parents"]["0145"] == ["0140", "0144"]
    chart_rows = load("charts/curie/files/schema-compat.json")["revisions"]
    chart = {row["revision"]: row for row in chart_rows}
    assert chart["0139"] == {
        "kind": "expand",
        "parents": ["0138"],
        "revision": "0139",
        "sha256": hashlib.sha256(MIGRATION.read_bytes()).hexdigest(),
    }


def test_no_released_window_serves_the_unreleased_candidate() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-21: released catalog windows are preserved."""
    windows = load("cli/src/application_schema_windows.json")["windows"]
    assert windows
    for version, window in windows.items():
        assert "0139" not in (window.get("schema_min"), window.get("schema_head")), version
