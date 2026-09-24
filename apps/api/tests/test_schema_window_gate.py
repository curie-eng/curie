import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
CHECKER = REPO_ROOT / "scripts" / "check-schema-window.py"
CHECK_COMMAND = "uv run python scripts/check-schema-window.py"


def _write_revision(
    repo_root: Path,
    filename: str,
    revision: str,
    down_revision: str | None,
) -> None:
    versions = repo_root / "apps" / "api" / "alembic" / "versions"
    versions.mkdir(parents=True, exist_ok=True)
    (versions / filename).write_text(
        "\n".join(
            [
                f"revision = {revision!r}",
                f"down_revision = {down_revision!r}",
                "branch_labels = None",
                "depends_on = None",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _write_chart(repo_root: Path, *, app_version: str = "0.8.8") -> None:
    chart = repo_root / "charts" / "curie"
    chart.mkdir(parents=True, exist_ok=True)
    (chart / "Chart.yaml").write_text(
        "\n".join(
            [
                "apiVersion: v2",
                "name: curie",
                "version: 0.8.8",
                f'appVersion: "{app_version}"',
                "",
            ]
        ),
        encoding="utf-8",
    )


def _write_catalog(
    repo_root: Path,
    *,
    revisions: list[str],
    window_heads: dict[str, str],
) -> None:
    catalog_dir = repo_root / "cli" / "src"
    catalog_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "revisions": revisions,
        "windows": {
            version: {
                "schema_min": "0001",
                "schema_head": head,
            }
            for version, head in window_heads.items()
        },
    }
    (catalog_dir / "application_schema_windows.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )


def _write_linear_migrations(repo_root: Path) -> None:
    _write_revision(repo_root, "0001_base.py", "0001", None)
    _write_revision(repo_root, "0002_next.py", "0002", "0001")


def _run_gate(repo_root: Path | None = None) -> subprocess.CompletedProcess[str]:
    command = [sys.executable, str(CHECKER)]
    if repo_root is not None:
        command.extend(["--repo-root", str(repo_root)])
    return subprocess.run(
        command,
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def test_real_tree_window_matches_alembic_head() -> None:
    chart = yaml.safe_load((REPO_ROOT / "charts" / "curie" / "Chart.yaml").read_text())
    app_version = str(chart["appVersion"])
    catalog = json.loads(
        (REPO_ROOT / "cli" / "src" / "application_schema_windows.json").read_text()
    )
    result = _run_gate()

    assert result.returncode == 0, result.stderr
    assert app_version in catalog["windows"]
    assert f"appVersion {app_version}" in result.stdout
    assert "catalog appVersion" in result.stdout
    assert "schema_head" in result.stdout


def test_tree_head_without_newer_catalog_window_fails(tmp_path: Path) -> None:
    _write_chart(tmp_path, app_version="0.9.0")
    _write_catalog(
        tmp_path,
        revisions=["0001", "0002"],
        window_heads={"0.9.0": "0001"},
    )
    _write_linear_migrations(tmp_path)

    result = _run_gate(tmp_path)

    assert result.returncode == 1, result.stderr or result.stdout
    assert "0002" in result.stderr
    err = result.stderr.lower()
    assert "schema_head" in err or "window" in err


def test_older_chart_version_uses_newest_numeric_catalog_window(tmp_path: Path) -> None:
    _write_chart(tmp_path, app_version="0.9.0")
    _write_catalog(
        tmp_path,
        revisions=["0001", "0002"],
        window_heads={
            "0.9.10": "0002",
            "0.9.9": "0001",
            "0.9.0": "0001",
        },
    )
    _write_linear_migrations(tmp_path)

    result = _run_gate(tmp_path)

    assert result.returncode == 0, result.stderr
    assert "chart appVersion 0.9.0" in result.stdout
    assert "catalog appVersion 0.9.10" in result.stdout
    assert "schema_head 0002" in result.stdout


def test_release_candidate_chart_requires_its_exact_catalog_window(tmp_path: Path) -> None:
    _write_chart(tmp_path, app_version="0.10.0-rc.1")
    _write_catalog(
        tmp_path,
        revisions=["0001", "0002"],
        window_heads={"0.10.0": "0002"},
    )
    _write_linear_migrations(tmp_path)

    result = _run_gate(tmp_path)

    assert result.returncode == 1, result.stdout
    assert "no window for appVersion 0.10.0-rc.1" in result.stderr


def test_release_candidate_precedes_stable_in_catalog_order(tmp_path: Path) -> None:
    _write_chart(tmp_path, app_version="0.10.0-rc.1")
    _write_catalog(
        tmp_path,
        revisions=["0001", "0002"],
        window_heads={
            "0.10.0": "0002",
            "0.10.0-rc.1": "0001",
        },
    )
    _write_linear_migrations(tmp_path)

    result = _run_gate(tmp_path)

    assert result.returncode == 0, result.stderr
    assert "chart appVersion 0.10.0-rc.1" in result.stdout
    assert "catalog appVersion 0.10.0 schema_head 0002" in result.stdout


@pytest.mark.parametrize(
    "version",
    ["0.10.0-rc", "0.10.0-rc.x", "0.10.0-rc.01", "0.10.0-rc.1.extra"],
)
def test_malformed_release_candidate_catalog_key_fails(
    tmp_path: Path, version: str
) -> None:
    _write_chart(tmp_path, app_version="0.9.9")
    _write_catalog(
        tmp_path,
        revisions=["0001", "0002"],
        window_heads={"0.9.9": "0002", version: "0002"},
    )
    _write_linear_migrations(tmp_path)

    result = _run_gate(tmp_path)

    assert result.returncode == 1, result.stdout
    assert version in result.stderr


def test_release_candidate_chart_window_matches_alembic_head(tmp_path: Path) -> None:
    _write_chart(tmp_path, app_version="0.10.0-rc.1")
    _write_catalog(
        tmp_path,
        revisions=["0001", "0002"],
        window_heads={"0.9.9": "0001", "0.10.0-rc.1": "0002"},
    )
    _write_linear_migrations(tmp_path)

    result = _run_gate(tmp_path)

    assert result.returncode == 0, result.stderr
    assert "chart appVersion 0.10.0-rc.1" in result.stdout
    assert "catalog appVersion 0.10.0-rc.1 schema_head 0002" in result.stdout


def test_release_candidate_window_must_match_alembic_head(tmp_path: Path) -> None:
    _write_chart(tmp_path, app_version="0.10.0-rc.1")
    _write_catalog(
        tmp_path,
        revisions=["0001", "0002"],
        window_heads={"0.9.9": "0001", "0.10.0-rc.1": "0001"},
    )
    _write_linear_migrations(tmp_path)

    result = _run_gate(tmp_path)

    assert result.returncode == 1, result.stdout
    assert "0.10.0-rc.1" in result.stderr
    assert "schema_head" in result.stderr
    assert "0002" in result.stderr


def test_migration_with_matching_window_passes(tmp_path: Path) -> None:
    _write_chart(tmp_path)
    _write_catalog(
        tmp_path,
        revisions=["0001", "0002"],
        window_heads={"0.8.8": "0002"},
    )
    _write_linear_migrations(tmp_path)

    result = _run_gate(tmp_path)

    assert result.returncode == 0, result.stderr
    assert "0002" in result.stdout


def test_catalog_missing_alembic_revision_fails(tmp_path: Path) -> None:
    _write_chart(tmp_path)
    _write_catalog(
        tmp_path,
        revisions=["0001"],
        window_heads={"0.8.8": "0002"},
    )
    _write_linear_migrations(tmp_path)

    result = _run_gate(tmp_path)

    assert result.returncode == 1, result.stderr or result.stdout
    assert "0002" in result.stderr


def test_catalog_missing_app_version_window_fails(tmp_path: Path) -> None:
    _write_chart(tmp_path, app_version="0.9.9")
    _write_catalog(
        tmp_path,
        revisions=["0001", "0002"],
        window_heads={"0.8.8": "0002"},
    )
    _write_linear_migrations(tmp_path)

    result = _run_gate(tmp_path)

    assert result.returncode == 1, result.stderr or result.stdout
    assert "0.9.9" in result.stderr


def test_python_ci_job_runs_schema_window_after_alembic_gate() -> None:
    workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/ci.yaml").read_text())
    steps = workflow["jobs"]["python"]["steps"]

    matching_steps = [step for step in steps if step.get("run") == CHECK_COMMAND]
    assert len(matching_steps) == 1
    assert matching_steps[0]["name"] == "Schema window gate"

    gate_index = steps.index(matching_steps[0])
    alembic_index = next(
        index
        for index, step in enumerate(steps)
        if step.get("name") == "Alembic revision gate"
    )
    stack_index = next(
        index for index, step in enumerate(steps) if step.get("name") == "Start dev stack"
    )
    assert alembic_index < gate_index < stack_index


def test_rust_ci_job_runs_schema_window_gate() -> None:
    workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/ci.yaml").read_text())
    steps = workflow["jobs"]["rust"]["steps"]

    matching_steps = [step for step in steps if step.get("run") == CHECK_COMMAND]
    assert len(matching_steps) == 1
    assert matching_steps[0]["working-directory"] == "${{ github.workspace }}"

    gate_index = steps.index(matching_steps[0])
    version_index = next(
        index
        for index, step in enumerate(steps)
        if step.get("name") == "Version consistency"
    )
    assert version_index < gate_index
