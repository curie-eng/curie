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
    assert "0074" not in revisions
    assert revisions["0075"] == ("0073", "0075_hook_source_policies.py")
    assert revisions["0076"][0] == "0075"
    assert revisions["0079"][0] == "0076"
    assert revisions["0080"][0] == "0079"
    assert revisions["0081"][0] == "0080"
    assert revisions["0082"] == ("0081", "0082_agent_deploy_notifications.py")
    assert revisions["0102"] == ("0101", "0102_execution_request_reply_targets.py")
    assert revisions["0103"] == ("0102", "0103_tracker_issue_identity.py")


def test_candidate_does_not_rewrite_released_windows() -> None:
    """@spec DEPLOY-NOTICE-RELEASE-1."""
    catalog = json.loads((ROOT / "cli/src/application_schema_windows.json").read_text())
    released = {"schema_min": "0070", "schema_head": "0073"}
    stable = {"schema_min": "0076", "schema_head": "0076"}
    # Feature migrations append after the published v0.12.2 head; published
    # application windows remain fixed while the candidate advances. The
    # minimum is the join containing both the stable reply selector columns
    # and the feature train schema. The released v0.13.0 window stays at that
    # join while the candidate advances to team sync state (0120) and agent access
    # policies (0121), directory authority (0122), attachment catalogs (0130),
    # and the stable twelve-hour execution deadline (0140), joined by 0145,
