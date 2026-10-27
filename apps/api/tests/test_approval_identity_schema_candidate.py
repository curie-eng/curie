"""Approval provenance schema candidate, @spec HTTP-APPROVAL-CONTINUITY-5."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

ROOT = Path(__file__).resolve().parents[3]
MIGRATION = ROOT / "apps/api/alembic/versions/0137_approval_identity_context.py"
# The floor stays at the column the ORM reads; 0138 only widens a CHECK.
CANDIDATE = {"schema_min": "0137", "schema_head": "0138"}


def load(relative: str) -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-5."""
    return json.loads((ROOT / relative).read_text(encoding="utf-8"))


def script() -> ScriptDirectory:
    """@spec HTTP-APPROVAL-CONTINUITY-5: the actual migration graph, not a copied list."""
    config = Config(str(ROOT / "apps/api/alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "apps/api/alembic"))
    return ScriptDirectory.from_config(config)


def test_migration_0137_is_the_single_head_with_parent_0133() -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-5: additive revision after role configuration."""
    graph = script()
    assert graph.get_heads() == ["0138"]
    revision = graph.get_revision("0137")
    assert revision is not None and revision.down_revision == "0133"
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
def test_every_coupled_candidate_consumer_requires_0137(relative: str, extract: Any) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-5: the ORM reads identity_context."""
    assert extract(load(relative)) == CANDIDATE


def test_catalog_kind_parent_and_chart_digest_name_the_exact_migration() -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-5: exact graph parent and file digest."""
    revisions = load("cli/src/application_schema_windows.json")["revisions"]
    assert revisions[-3:] == ["0133", "0137", "0138"]
    assert load("apps/api/src/curie_api/revision_kinds.json")["0137"] == "expand"
    serving = load("packages/protected-hooks/src/curie_protected_hooks/schema_serving.json")
    assert serving["revision_parents"]["0137"] == ["0133"]
    chart_rows = load("charts/curie/files/schema-compat.json")["revisions"]
    chart = {row["revision"]: row for row in chart_rows}
    assert chart["0137"] == {
        "kind": "expand",
        "parents": ["0133"],
        "revision": "0137",
        "sha256": hashlib.sha256(MIGRATION.read_bytes()).hexdigest(),
    }


def test_no_released_window_serves_the_unreleased_candidate() -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-5: released catalog windows are preserved."""
    windows = load("cli/src/application_schema_windows.json")["windows"]
    assert windows
    for version, window in windows.items():
        assert "0137" not in (window.get("schema_min"), window.get("schema_head")), version
