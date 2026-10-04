"""The chart carries a generated copy of the API's authoritative schema graph."""

import hashlib
import json
import shutil
from pathlib import Path

import pytest
from _migration_support import run_script
from alembic.script import ScriptDirectory

REPO = Path(__file__).resolve().parents[3]
ARTIFACT = Path("charts/curie/files/schema-compat.json")


def test_packaged_schema_metadata_matches_api_authority() -> None:
    metadata = json.loads((REPO / ARTIFACT).read_text())
    api = REPO / "apps/api/src/curie_api"
    window = json.loads((api / "schema_compat.json").read_text())
    kinds = json.loads((api / "revision_kinds.json").read_text())
    assert metadata["schema_min"] == window["schema_min"]
    assert metadata["schema_head"] == window["schema_head"]
    graph = ScriptDirectory(str(REPO / "apps/api/alembic"))
    revisions = {entry["revision"]: entry for entry in metadata["revisions"]}
    assert set(revisions) == set(kinds)
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
