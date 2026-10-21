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
    # and the feature train schema; the Agent ORM reads agents.learned_skills
    # (ADR 0237), the agent schedule columns (ADR 0238) and
    # agents.agent_wide_learning (ADR 0239), so twin memory, which follows
    # agent schedules, is the floor.
    candidate = {"schema_min": "selfimp_memory_0001", "schema_head": "selfimp_memory_0001"}
    released_013 = {"schema_min": "0108", "schema_head": "0108"}
    assert catalog["windows"]["0.12.0"] == released
    assert catalog["windows"]["0.12.0-rc.1"] == released
    assert catalog["windows"]["0.12.1"] == stable
    assert catalog["windows"]["0.12.2"] == {"schema_min": "0076", "schema_head": "0081"}
    assert catalog["windows"]["0.12.3"] == {"schema_min": "0076", "schema_head": "0093"}
    assert catalog["candidate"] == candidate
    assert catalog["windows"]["0.13.0"] == released_013
    assert catalog["revisions"][-16:] == [
        "0095", "0096", "0097", "0098", "0099", "0100", "0101", "0102", "0103",
        "0105", "0107", "0108", "selfimp_safety_0001", "selfimp_skills_0001",
        "selfimp_sched_0001",
        "selfimp_memory_0001",
    ]
    assert "0074" not in catalog["revisions"]
    prior_windows = json.loads(
        (Path(__file__).parent / "fixtures/source_schema_prior_windows.json").read_text()
    )
    assert {name: catalog["windows"][name] for name in prior_windows} == prior_windows
    for name in (
        "apps/api/src/curie_api/schema_compat.json",
        "charts/curie/files/schema-compat.json",
    ):
        payload = json.loads((ROOT / name).read_text())
        assert {key: payload[key] for key in candidate} == candidate
