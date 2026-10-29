"""The chart carries a generated copy of the API's authoritative schema graph."""

import hashlib
import json
import shutil
import uuid
from pathlib import Path

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    run_script,
    sql_rows,
    stamped_revision,
)
from alembic import command
from alembic.script import ScriptDirectory

REPO = Path(__file__).resolve().parents[3]
ARTIFACT = Path("charts/curie/files/schema-compat.json")


def test_packaged_schema_metadata_matches_api_authority() -> None:
    metadata = json.loads((REPO / ARTIFACT).read_text())
    api = REPO / "apps/api/src/curie_api"
    window = json.loads((api / "schema_compat.json").read_text())
    kinds = json.loads((api / "revision_kinds.json").read_text())
    assert window == {"schema_min": "0144", "schema_head": "0144"}
    assert metadata["schema_min"] == window["schema_min"]
    assert metadata["schema_head"] == window["schema_head"]
    graph = ScriptDirectory(str(REPO / "apps/api/alembic"))
    revisions = {entry["revision"]: entry for entry in metadata["revisions"]}
    assert set(revisions) == set(kinds)
    assert revisions["0141"]["parents"] == ["0139"]
    assert revisions["0143"]["parents"] == ["0141"]
    assert revisions["0144"]["parents"] == ["0143"]
    for revision in graph.walk_revisions():
        parent = revision.down_revision
        parents = list(parent) if isinstance(parent, (list, tuple)) else [parent] if parent else []
        assert revisions[revision.revision] == {
            "revision": revision.revision,
            "parents": parents,
            "kind": kinds[revision.revision],
            "sha256": hashlib.sha256(Path(revision.path).read_bytes()).hexdigest(),
        }


@pytest.mark.parametrize(
    "mutation",
    [
        "kind",
        "parents",
        "schema_min",
        "schema_head",
        "extra_key",
        "missing_row",
        "duplicate_row",
        "digest",
    ],
)
def test_revision_gate_rejects_stale_packaged_metadata(tmp_path: Path, mutation: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    for source in [
        "scripts/check-alembic-revisions.py",
        "apps/api/src/curie_api/schema_compat.json",
        "apps/api/src/curie_api/revision_kinds.json",
        "cli/src/application_schema_windows.json",
        str(ARTIFACT),
    ]:
        target = tmp_path / source
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO / source, target)
    shutil.copytree(REPO / "apps/api/alembic", tmp_path / "apps/api/alembic")
    # @spec PROTECTED-HOOK-SOURCE-2: derive the owned copy from its actual graph.
    graph = {}
    for revision in ScriptDirectory(str(tmp_path / "apps/api/alembic")).walk_revisions():
        parent = revision.down_revision
        graph[revision.revision] = (
            list(parent) if isinstance(parent, (list, tuple)) else [parent] if parent else []
        )
    catalog = json.loads((tmp_path / "cli/src/application_schema_windows.json").read_text())
    candidate = catalog["candidate"]
    owner = tmp_path / "packages/protected-hooks/src/curie_protected_hooks/schema_serving.json"
    owner.parent.mkdir(parents=True, exist_ok=True)
    owner.write_text(json.dumps(dict(**candidate, revision_parents=graph),
                                indent=2, sort_keys=True) + "\n")
    checker = tmp_path / "scripts/check-alembic-revisions.py"
    healthy = run_script(checker)
    assert healthy.returncode == 0, healthy.stderr
    artifact = tmp_path / ARTIFACT
    metadata = json.loads(artifact.read_text())
    if mutation == "kind":
        metadata["revisions"][0]["kind"] = "contract"
    elif mutation == "parents":
        metadata["revisions"][1]["parents"] = []
    elif mutation in {"schema_min", "schema_head"}:
        metadata[mutation] = "0001"
    elif mutation == "extra_key":
        metadata["extra"] = True
    elif mutation == "missing_row":
        metadata["revisions"].pop()
    elif mutation == "duplicate_row":
        metadata["revisions"].append(metadata["revisions"][0])
    elif mutation == "digest":
        metadata["revisions"][0]["sha256"] = "0" * 64
    artifact.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    stale = run_script(checker)
    assert stale.returncode == 1
    assert "schema compatibility metadata" in stale.stderr.lower()


def test_metadata_write_cannot_claim_to_generate_from_a_non_authoritative_tree(
    tmp_path: Path,
) -> None:
    tree = tmp_path / "alembic"
    shutil.copytree(REPO / "apps/api/alembic", tree)
    result = run_script(
        REPO / "scripts/check-alembic-revisions.py",
        "--script-location",
        str(tree),
        "--write-upgrade-metadata",
    )
    assert result.returncode == 2
    assert "authoritative" in result.stderr


def test_0143_to_0144_preserves_retained_base_pins_and_selection_intent(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at("0143")
    agent_id, item_id, request_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents(id, name) VALUES (:id, 'retained-base-example')",
        {"id": agent_id},
    )
    sql_rows(
        "INSERT INTO curie.work_items "
        "(id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "code_host_kind, code_host_host, repository_project_id, code_host_installation_id, "
        "agent_id, repository_path, conversation_id, version, next_sequence, base_branch, "
        "base_source, base_commit, base_resolved_request_id, base_selection_fingerprint, "
        "readmit_base_settle_until, base_requested_branch, base_requested_source) "
        "VALUES (:id, 'github', 'github.com', '101', '7', 'github', 'github.com', "
        "'101', 202, :agent, 'acme-corp/acme-bot', 'retained-base-example', 4, 2, "
        "'main', 'default', :base, :request, 'retained-fingerprint', "
        "'2026-10-28T12:01:00+00:00', 'next', 'label')",
        {"id": item_id, "agent": agent_id, "base": "1" * 40, "request": request_id},
    )
    sql_rows(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, version, execution_attempts, "
        "wait_deadline, base_settle_until) VALUES "
        "(:id, :item, 1, 'waiting', 3, 0, '2026-10-28T13:00:00+00:00', "
        "'2026-10-28T12:01:00+00:00')",
        {"id": request_id, "item": item_id},
    )
    before_item = sql_rows("SELECT * FROM curie.work_items WHERE id = :id", {"id": item_id})
    before_request = sql_rows(
        "SELECT * FROM curie.execution_requests WHERE id = :id", {"id": request_id}
    )
    before_columns = sql_rows(
        "SELECT table_name, column_name, data_type, is_nullable "
        "FROM information_schema.columns WHERE table_schema = 'curie' "
        "AND table_name IN ('work_items', 'execution_requests') "
        "ORDER BY table_name, ordinal_position"
    )
    assert sql_rows("SELECT to_regclass('curie.factory_base_transitions')") == [(None,)]
    assert {
        ("work_items", "base_resolved_request_id"),
        ("work_items", "base_selection_fingerprint"),
        ("work_items", "readmit_base_settle_until"),
        ("work_items", "base_requested_branch"),
        ("work_items", "base_requested_source"),
        ("execution_requests", "base_settle_until"),
    } <= {(row[0], row[1]) for row in before_columns}

    command.upgrade(alembic_config(), "0144")

    assert stamped_revision() == "0144"
    assert sql_rows("SELECT * FROM curie.work_items WHERE id = :id", {"id": item_id}) == before_item
    assert sql_rows(
        "SELECT * FROM curie.execution_requests WHERE id = :id", {"id": request_id}
    ) == before_request
    assert sql_rows(
        "SELECT table_name, column_name, data_type, is_nullable "
        "FROM information_schema.columns WHERE table_schema = 'curie' "
        "AND table_name IN ('work_items', 'execution_requests') "
        "ORDER BY table_name, ordinal_position"
    ) == before_columns
    assert sql_rows("SELECT to_regclass('curie.factory_base_transitions')") == [
        ("curie.factory_base_transitions",)
    ]
    assert sql_rows("SELECT count(*) FROM curie.factory_base_transitions") == [(0,)]

