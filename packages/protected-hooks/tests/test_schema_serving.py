"""Installed schema serving owner, @spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import importlib
import importlib.util
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from alembic.script import ScriptDirectory

ROOT = Path(__file__).resolve().parents[3]
RESOURCE = Path("packages/protected-hooks/src/curie_protected_hooks/schema_serving.json")


def shared() -> Any:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    assert importlib.util.find_spec("curie_protected_hooks.schema_serving") is not None, (
        "SOURCE-2 installed application-independent serving decision is missing"
    )
    return importlib.import_module("curie_protected_hooks.schema_serving")


def graph_payload() -> dict[str, Any]:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    script = ScriptDirectory(str(ROOT / "apps/api/alembic"))
    candidate = json.loads((ROOT / "cli/src/application_schema_windows.json").read_text())[
        "candidate"
    ]
    graph = {}
    for revision in script.walk_revisions():
        parent = revision.down_revision
        graph[revision.revision] = (
            list(parent) if isinstance(parent, (list, tuple)) else ([parent] if parent else [])
        )
    return dict(**candidate, revision_parents=graph)


@pytest.mark.parametrize(
    "current,expected",
    [(None, False), ("0075", False), ("0076", True), ("0000", True), ("future-expand", True)],
)
def test_installed_candidate_decision_preserves_unknown_revision_policy(
    current: str | None, expected: bool
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    module = shared()
    metadata = module.load_metadata()
    assert (
        module.can_serve(current, metadata.window, metadata.known_revisions, metadata.parents)
        is expected
    )
    from curie_api import schema_compat

    assert (
        schema_compat.can_serve(current, schema_compat.load_window(), schema_compat.load_kinds())
        is expected
    )


@pytest.mark.parametrize("mutation", ["bounds", "parent", "cycle", "duplicate", "type"])
def test_malformed_metadata_refuses_with_safe_category(mutation: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    module = shared()
    payload = dict(
        schema_min="base", schema_head="head", revision_parents={"base": [], "head": ["base"]}
    )
    if mutation == "bounds":
        payload["schema_min"] = "not-present"
    elif mutation == "parent":
        payload["revision_parents"]["head"] = ["untrusted-input"]
    elif mutation == "cycle":
        payload["revision_parents"]["base"] = ["head"]
    elif mutation == "type":
        payload["revision_parents"]["head"] = "base"
    raw = json.dumps(payload).encode()
    if mutation == "duplicate":
        raw = (
            b'{"schema_min":"base","schema_min":"untrusted-input",'
            b'"schema_head":"head","revision_parents":{"base":[],"head":["base"]}}'
        )
    with pytest.raises(module.SchemaServingUnavailable) as caught:
        module.parse_metadata(raw)
    assert caught.value.code == "schema_metadata_invalid"
    assert "untrusted-input" not in str(caught.value)


def test_multiple_parent_metadata_and_first_parent_serving_preserve_shortcuts(
    tmp_path: Path,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    module = shared()
    parents = dict(base=[], left=["base"], right=["base"], merge=["left", "right"], head=["merge"])
    paths = tmp_path / "versions"
    paths.mkdir()
    for revision, links in parents.items():
        parent = tuple(links) if len(links) > 1 else links[0] if links else None
        (paths / f"{revision}.py").write_text(
            f"revision={revision!r}\ndown_revision={parent!r}\nbranch_labels=None\ndepends_on=None\n"
        )
    script = ScriptDirectory(str(tmp_path))
    from curie_api import schema_compat

    for minimum, expected_merge in (("left", True), ("right", False)):
        metadata = module.parse_metadata(
            json.dumps(
                dict(schema_min=minimum, schema_head="head", revision_parents=parents)
            ).encode()
        )
        assert metadata.parents["merge"] == ("left", "right")
        assert module.can_serve("head", metadata.window, metadata.known_revisions, metadata.parents)
        assert module.can_serve(
            minimum, metadata.window, metadata.known_revisions, metadata.parents
        )
        assert (
            module.can_serve("merge", metadata.window, metadata.known_revisions, metadata.parents)
            is expected_merge
        )
        assert (
            schema_compat.can_serve(
                "merge", schema_compat.AppWindow(minimum, "head"), parents, script
            )
            is expected_merge
        )
        with pytest.raises(TypeError):
            metadata.parents["merge"] = ("right",)
    historical = module.AppWindow("0041", "0041")
    assert module.can_serve(
        "0042", historical, {"0040", "0041"}, dict({"0040": (), "0041": ("0040",)})
    )
    assert not module.can_serve(
        "0040", historical, {"0040", "0041"}, dict({"0040": (), "0041": ("0040",)})
    )


def test_installed_resource_exactly_matches_actual_graph_and_frozen_windows() -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    module = shared()
    payload = graph_payload()
    assert json.loads((ROOT / RESOURCE).read_text()) == payload
    metadata = module.load_metadata()
    assert metadata.parents == {
        revision: tuple(parents) for revision, parents in payload["revision_parents"].items()
    }
    assert metadata.known_revisions == frozenset(payload["revision_parents"])
    frozen = json.loads(
        (ROOT / "apps/api/tests/fixtures/source_schema_prior_windows.json").read_text()
    )
    windows = json.loads((ROOT / "cli/src/application_schema_windows.json").read_text())["windows"]
    assert {version: windows[version] for version in frozen} == frozen


def test_api_exports_one_shared_window_class() -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    module = shared()
    from curie_api import schema_compat

    assert schema_compat.AppWindow is module.AppWindow
    assert schema_compat.load_window() == module.load_metadata().window


def test_built_installed_wheel_loads_without_api_worker_or_alembic(tmp_path: Path) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    shared()
    wheels = tmp_path / "wheels"
    build = subprocess.run(
        ["uv", "build", "packages/protected-hooks", "--wheel", "--out-dir", str(wheels)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert build.returncode == 0, build.stderr
    (wheel,) = wheels.glob("*.whl")
    installed = tmp_path / "installed"
    install = subprocess.run(
        ["uv", "pip", "install", "--no-deps", "--target", str(installed), str(wheel)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert install.returncode == 0, install.stderr
    program = '''
import importlib.abc, sys
sys.path.insert(0, sys.argv[1])
class RejectApplications(importlib.abc.MetaPathFinder):
    """@spec PROTECTED-HOOK-SOURCE-2."""
    def find_spec(self, fullname, path=None, target=None):
        """@spec PROTECTED-HOOK-SOURCE-2."""
        if fullname.split('.')[0] in ('curie_api','curie_worker','alembic'):
            raise ImportError('forbidden_application_import')
sys.meta_path.insert(0,RejectApplications())
try: import curie_api
except ImportError: pass
else: raise AssertionError('import boundary sentinel failed')
from curie_protected_hooks.schema_serving import load_metadata,can_serve
metadata=load_metadata()
assert can_serve(metadata.window.schema_head,metadata.window,
                 metadata.known_revisions,metadata.parents)
assert can_serve('future-expand',metadata.window,metadata.known_revisions,metadata.parents)
assert 'curie_api' not in sys.modules and 'alembic' not in sys.modules
'''
    result = subprocess.run(
        [sys.executable, "-I", "-c", program, str(installed)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("checker", ["check-schema-window.py", "check-alembic-revisions.py"])
def test_actual_checker_refuses_corrupted_shared_runtime_graph(
    tmp_path: Path, checker: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    paths = [
        "scripts/" + checker,
        "apps/api/src/curie_api/schema_compat.json",
        "apps/api/src/curie_api/revision_kinds.json",
        "cli/src/application_schema_windows.json",
        "charts/curie/Chart.yaml",
        "charts/curie/files/schema-compat.json",
        "docs/architecture-atlas/versions.json",
    ]
    for name in paths:
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, target)
    shutil.copytree(ROOT / "apps/api/alembic", tmp_path / "apps/api/alembic")
    resource = tmp_path / RESOURCE
    resource.parent.mkdir(parents=True, exist_ok=True)
    payload = graph_payload()
    resource.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    # The schema window gate reads a released window from its release tag, so
    # the copied tree is a git repository tagged v<appVersion>.
    app_version = next(
        line.split(":", 1)[1].strip().strip("\"'")
        for line in (tmp_path / "charts/curie/Chart.yaml").read_text().splitlines()
        if line.startswith("appVersion:")
    )
    git = [
        "git",
        "-c", "user.name=Schema Serving Test",
        "-c", "user.email=schema-serving@example.invalid",
        "-c", "commit.gpgsign=false",
        "-c", "tag.gpgsign=false",
        "-c", "core.hooksPath=/dev/null",
        "-C", str(tmp_path),
    ]
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run([*git, "add", "-A"], check=True)
    subprocess.run([*git, "commit", "-q", "-m", f"release {app_version}"], check=True)
    subprocess.run([*git, "tag", f"v{app_version}"], check=True)
    command = [sys.executable, str(tmp_path / "scripts" / checker)]
    if checker == "check-schema-window.py":
        command += ["--repo-root", str(tmp_path)]
    healthy = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True, timeout=15)
    assert healthy.returncode == 0, healthy.stderr
    payload["revision_parents"]["0076"] = []
    resource.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    invalid = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True, timeout=15)
    assert invalid.returncode == 1, "SOURCE-2 checker silently accepts corrupted installed ancestry"
