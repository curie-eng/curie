"""Consumer-path tests for the released-upgrade live-manifest verifier."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "charts" / "curie" / "ci" / "live_manifest_parity.py"
CHART_YAML = REPO_ROOT / "charts" / "curie" / "Chart.yaml"

TIMEOUT_NAME = "CURIE_RUNNER_TOTAL_TIMEOUT_S"


def _worker_deploy(env: list[dict[str, str]], *, name: str = "curie-worker") -> dict:
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "labels": {"app.kubernetes.io/component": "worker"},
        },
        "spec": {
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "env": env}],
                }
            }
        },
    }


def _helm_manifest(env: list[dict[str, str]]) -> str:
    return yaml.safe_dump(_worker_deploy(env), sort_keys=False)


def _metadata(version: str) -> str:
    return f"NAME: curie\nCHART: curie\nVERSION: {version}\nAPP_VERSION: {version}\n"


def _chart_version() -> str:
    for line in CHART_YAML.read_text().splitlines():
        if line.startswith("version:"):
            return line.split(":", 1)[1].strip().strip('"')
    raise AssertionError("charts/curie/Chart.yaml has no version:")


def _run(
    tmp_path: Path,
    *,
    helm_env: list[dict[str, str]],
    live_env: list[dict[str, str]],
    metadata_version: str | None = None,
    chart_yaml: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    helm_manifest = tmp_path / "helm.yaml"
    live_deploy = tmp_path / "live.yaml"
    helm_metadata = tmp_path / "metadata.txt"
    helm_manifest.write_text(_helm_manifest(helm_env))
    live_deploy.write_text(yaml.safe_dump(_worker_deploy(live_env), sort_keys=False))
    helm_metadata.write_text(_metadata(metadata_version or _chart_version()))
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--helm-manifest",
            str(helm_manifest),
            "--live-deploy",
            str(live_deploy),
            "--helm-metadata",
            str(helm_metadata),
            "--chart-yaml",
            str(chart_yaml or CHART_YAML),
        ],
        capture_output=True,
        text=True,
        check=False,
    )


def test_verifier_script_exists() -> None:
    assert SCRIPT.is_file(), f"missing live-manifest verifier at {SCRIPT}"


def test_matching_target_and_live_worker_env_passes(tmp_path: Path) -> None:
    env = [
        {"name": TIMEOUT_NAME, "value": "600"},
        {"name": "CURIE_DELIVERY_BUDGET_S", "value": "600"},
    ]
    completed = _run(tmp_path, helm_env=env, live_env=env)
    assert completed.returncode == 0, completed.stderr
    assert TIMEOUT_NAME in completed.stdout
    assert "target version" in completed.stdout.lower() or "converged" in completed.stdout.lower()


def test_live_missing_timeout_fails(tmp_path: Path) -> None:
    helm_env = [{"name": TIMEOUT_NAME, "value": "600"}]
    live_env = [{"name": "CURIE_DELIVERY_BUDGET_S", "value": "600"}]
    completed = _run(tmp_path, helm_env=helm_env, live_env=live_env)
    assert completed.returncode != 0
    assert TIMEOUT_NAME in completed.stderr


def test_duplicate_live_timeout_fails(tmp_path: Path) -> None:
    helm_env = [{"name": TIMEOUT_NAME, "value": "600"}]
    live_env = [
        {"name": TIMEOUT_NAME, "value": "600"},
        {"name": TIMEOUT_NAME, "value": "1700"},
    ]
    completed = _run(tmp_path, helm_env=helm_env, live_env=live_env)
    assert completed.returncode != 0
    assert "exactly once" in completed.stderr or "duplicate" in completed.stderr.lower()


def test_helm_has_timeout_live_omits_it_fails(tmp_path: Path) -> None:
    """The 2026-09-04 soak: target manifest kept the timeout, live worker did not."""
    helm_env = [
        {"name": TIMEOUT_NAME, "value": "600"},
        {"name": "CURIE_DELIVERY_BUDGET_S", "value": "600"},
    ]
    live_env = [{"name": "CURIE_DELIVERY_BUDGET_S", "value": "600"}]
    completed = _run(tmp_path, helm_env=helm_env, live_env=live_env)
    assert completed.returncode != 0
    assert "live" in completed.stderr.lower()
    assert TIMEOUT_NAME in completed.stderr


def test_chart_version_skew_fails(tmp_path: Path) -> None:
    env = [{"name": TIMEOUT_NAME, "value": "600"}]
    completed = _run(
        tmp_path,
        helm_env=env,
        live_env=env,
        metadata_version="0.0.0-not-the-chart",
    )
    assert completed.returncode != 0
    assert "version" in completed.stderr.lower()


def test_self_test_mode_covers_the_negative_controls() -> None:
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "--self-test"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert "self-test" in completed.stdout.lower()
