"""@spec DEPLOY-NOTICE-RELEASE-1."""

from __future__ import annotations

import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def test_notice_migration_appends_to_released_head() -> None:
    """@spec DEPLOY-NOTICE-RELEASE-1."""
    revisions: dict[str, tuple[str | None, str]] = {}
    for path in (ROOT / "apps/api/alembic/versions").glob("*.py"):
        fields = {}
        for node in ast.parse(path.read_text()).body:
            if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                if node.target.id in {"revision", "down_revision"}:
                    fields[node.target.id] = ast.literal_eval(node.value)
        if fields.get("revision") is not None:
            revision = fields["revision"]
            assert revision not in revisions, f"duplicate migration {revision}"
            revisions[revision] = (fields.get("down_revision"), path.name)
    assert revisions["0072"][1] == "0072_work_item_base.py"
    assert revisions["0073"][0] == "0072"
    assert revisions["0074"] == ("0073", "0074_agent_deploy_notifications.py")


def test_candidate_does_not_rewrite_released_windows() -> None:
    """@spec DEPLOY-NOTICE-RELEASE-1."""
    catalog = json.loads((ROOT / "cli/src/application_schema_windows.json").read_text())
    released = {"schema_min": "0070", "schema_head": "0073"}
    # The candidate keeps moving as later migrations land (#2909 added 0075);
    # the point of this test is that `released` above never does.
    candidate = {"schema_min": "0074", "schema_head": "0075"}
    assert catalog["windows"]["0.12.0"] == released
    assert catalog["windows"]["0.12.0-rc.1"] == released
    assert catalog["candidate"] == candidate
    assert catalog["windows"]["0.13.0"] == candidate
    assert catalog["revisions"][-4:] == ["0072", "0073", "0074", "0075"]
    for name in (
        "apps/api/src/curie_api/schema_compat.json",
        "charts/curie/files/schema-compat.json",
    ):
        payload = json.loads((ROOT / name).read_text())
        assert {key: payload[key] for key in candidate} == candidate
