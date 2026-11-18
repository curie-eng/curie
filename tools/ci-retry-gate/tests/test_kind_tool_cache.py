"""Every kind install restores its binaries from the Actions cache first.

helm/kind-action downloads kind from github.com releases and kubectl from
dl.k8s.io unless the binary already sits at its tool-cache path. On
2026-11-18 one nightly runner got HTTP 403 from the kind release download for
both attempts while thirty sibling jobs in the same run downloaded it, which
failed the cluster rung. The kind-tool-cache composite action seeds that path
from the Actions cache, so these third-party fetches only happen on a miss.

These tests read the real workflows and the composite action.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
CACHE_ACTION = REPO_ROOT / ".github" / "actions" / "kind-tool-cache" / "action.yml"
CACHE_USES = "./.github/actions/kind-tool-cache"
KIND_ACTION = "helm/kind-action"
# kind-action v1.15.2 at this SHA installs kubectl v1.37.1 by default.
KIND_ACTION_SHA = "ea822c7bf1874f08e5a491df991c228edffc3376"
KIND_VERSION = "v0.24.0"
KUBECTL_VERSION = "v1.37.1"


def _workflow_jobs():
    for path in sorted(WORKFLOWS.glob("*.y*ml")):
        document = yaml.safe_load(path.read_text())
        for job_id, job in (document.get("jobs") or {}).items():
            if isinstance(job, dict) and job.get("steps"):
                yield path.name, job_id, job["steps"]


def _is_kind(step: object) -> bool:
    return isinstance(step, dict) and str(step.get("uses", "")).startswith(f"{KIND_ACTION}@")


def test_every_kind_attempt_is_preceded_by_the_cache_restore() -> None:
    gaps: list[str] = []
    found = 0
    for filename, job_id, steps in _workflow_jobs():
        for index, step in enumerate(steps):
            if not _is_kind(step) or str(step.get("name", "")).endswith(" (retry)"):
                continue
            found += 1
            previous = steps[index - 1] if index else {}
            label = f"{filename} :: {job_id} :: {step.get('name')}"
            if not isinstance(previous, dict) or previous.get("uses") != CACHE_USES:
                gaps.append(f"{label} has no kind-tool-cache step directly before it")
            elif previous.get("if") != step.get("if"):
                gaps.append(f"{label} runs under a different condition than its cache step")
    assert found, "no helm/kind-action steps found; the scan is broken"
    assert not gaps, "\n  ".join(gaps)


def test_kind_steps_install_the_versions_the_cache_holds() -> None:
    wrong: list[str] = []
    for filename, job_id, steps in _workflow_jobs():
        for step in steps:
            if not _is_kind(step):
                continue
            inputs = step.get("with") or {}
            label = f"{filename} :: {job_id} :: {step.get('name')}"
            if step["uses"].split("@", 1)[1].split()[0] != KIND_ACTION_SHA:
                wrong.append(f"{label} pins another kind-action commit")
            if inputs.get("version") != KIND_VERSION:
                wrong.append(f"{label} installs kind {inputs.get('version')}")
            if inputs.get("kubectl_version", KUBECTL_VERSION) != KUBECTL_VERSION:
                wrong.append(f"{label} installs kubectl {inputs['kubectl_version']}")
    assert not wrong, "\n  ".join(wrong)


def test_cache_action_restores_the_kind_action_tool_path_and_pins_hashes() -> None:
    action = yaml.safe_load(CACHE_ACTION.read_text())
    restore, verify = action["runs"]["steps"]
    assert restore["uses"].startswith("actions/cache@")
    assert restore["with"]["path"] == f"${{{{ runner.tool_cache }}}}/kind/{KIND_VERSION}/amd64"
    key = restore["with"]["key"]
    assert KIND_VERSION in key and KUBECTL_VERSION in key
    assert verify["if"] == "steps.restore.outputs.cache-hit == 'true'"
    assert verify["env"]["TOOL_DIR"] == restore["with"]["path"]
    body = verify["run"]
    for binary in ("kind/bin/kind", "kubectl/bin/kubectl"):
        assert re.search(rf"^[0-9a-f]{{64}}  \$\{{TOOL_DIR\}}/{binary}$", body, re.M), binary
    assert 'rm -rf -- "${TOOL_DIR:?}"' in body
