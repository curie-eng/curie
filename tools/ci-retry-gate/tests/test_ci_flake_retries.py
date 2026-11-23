"""Presence checks for issue #3866.

The retry-eligibility allowlist goes stale if a wrap and its row are removed
together. These tests read ci.yaml and helm-ci.yaml and require the acquisition
retry, the kind-registry recreate, and the Docker Hub mirror to still be there.

curl --retry is one process, so the eligibility gate cannot see it. The download
check below is what keeps a tool or chart fetch from dropping its backoff.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import stat
import subprocess
import textwrap
import tomllib
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
SCOPED = ("ci.yaml", "helm-ci.yaml")
ENSURE_SCRIPT = REPO_ROOT / "cli" / "scripts" / "ensure-kind-registry.sh"
MIRROR_SCRIPT = REPO_ROOT / "cli" / "scripts" / "configure-docker-hub-mirror.sh"

# Third-party acquisition actions #3866 requires a trio for, in these two files.
REQUIRED_ACTIONS = frozenset(
    {
        "astral-sh/setup-uv",
        "azure/setup-helm",
        "actions/setup-node",
        "pnpm/action-setup",
        "docker/setup-buildx-action",
        "docker/setup-qemu-action",
        "helm/kind-action",
    }
)

# mirror.gcr.io is Google's public Docker Hub pull-through cache.
# https://cloud.google.com/artifact-registry/docs/pull-cached-dockerhub-images
# Docker then falls back to Docker Hub when the cache misses. BuildKit does not
# read the daemon's registry-mirrors, so setup-buildx needs the same host in
# its buildkitd config. https://docs.docker.com/build/buildkit/toml-configuration/
MIRROR_HOST = "mirror.gcr.io"
BACKOFF_PREFIX = "Back off before retrying "
RETRY_SUFFIX = " (retry)"

DIAGNOSTIC_UPLOADS = {
    "Upload pytest durations",
    "Upload the OIDC evidence",
    "Upload the admitted agent failure digests",
    "Retain safe namespace cleanup evidence",
    "Upload upgrade matrix evidence",
    "Upload scripted factory evidence",
    "Upload scripted learned rules evidence",
}


def test_only_diagnostic_upload_failures_are_nonfatal() -> None:
    # GitHub documents step continue-on-error as permitting a failed step:
    # https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax
    seen = set()
    for _job, steps in _jobs("ci.yaml"):
        for step in steps:
            if _action(step) != "actions/upload-artifact":
                continue
            name = step.get("name")
            if name in DIAGNOSTIC_UPLOADS:
                assert step.get("continue-on-error") is True, name
                seen.add(name)
            else:
                assert not step.get("continue-on-error"), name
    assert seen == DIAGNOSTIC_UPLOADS


@pytest.mark.parametrize("name", ["Upload curie binary", "Upload runtime assertion receipts"])
def test_gate_refuses_to_ignore_a_required_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    document = _load("ci.yaml")
    for job in document["jobs"].values():
        for step in job.get("steps", []):
            if step.get("name") == name:
                step["continue-on-error"] = True
    (tmp_path / "ci.yaml").write_text(yaml.safe_dump(document))
    monkeypatch.setitem(globals(), "WORKFLOWS", tmp_path)
    with pytest.raises(AssertionError, match=name):
        test_only_diagnostic_upload_failures_are_nonfatal()


def _setup_commands(tmp_path: Path, **controls: object) -> tuple[dict[str, str], Path]:
    binaries = tmp_path / "bin"
    binaries.mkdir()
    state = tmp_path / "commands.json"
    state.write_text(json.dumps({"calls": [], "creates": 0, "pulls": 0, **controls}))
    driver = binaries / "driver"
    driver.write_text(textwrap.dedent("""\
        #!/usr/bin/env python3
        import json, os, pathlib, sys
        state = pathlib.Path(os.environ["SETUP_COMMAND_STATE"])
        data = json.loads(state.read_text())
        tool = pathlib.Path(sys.argv[0]).name
        args = sys.argv[1:]
        data["calls"].append([tool, *args])
        status = 0
        if tool == "kind":
            if args[:2] == ["get", "clusters"] and data.get("existing"):
                print("ci-fixture")
            if args[:2] == ["create", "cluster"]:
                data["creates"] += 1
                data["configs"] = [*data.get("configs", []),
                                   pathlib.Path(args[args.index("--config") + 1]).read_text()]
                if data.get("create_failure"):
                    status = 1
        elif tool == "kubectl":
            assert args[:2] == ["--context", "kind-ci-fixture"]
            if "rollout" in args:
                mode = data.get("readiness")
                if mode == "never" or mode == "second" and data["creates"] == 1:
                    status = 1
        elif tool == "curl":
            pathlib.Path(args[args.index("-o") + 1]).write_text("recorded manifest")
        elif tool == "docker":
            assert args[0] == "pull"
            data["pulls"] += 1
            if data["pulls"] <= data.get("pull_failures", 0):
                print(data.get("pull_error", "502 Bad Gateway"), file=sys.stderr)
                status = 1
        state.write_text(json.dumps(data))
        sys.exit(status)
        """))
    driver.chmod(0o755)
    for command in ("kind", "kubectl", "curl", "sha256sum", "docker", "sleep"):
        (binaries / command).symlink_to(driver)
    return {
        **os.environ,
        "PATH": f"{binaries}:{os.environ['PATH']}",
        "SETUP_COMMAND_STATE": str(state),
    }, state


@pytest.mark.parametrize(
    ("controls", "status", "creates", "deletes"),
    [({}, 0, 1, 0), ({"readiness": "second"}, 0, 2, 1),
     ({"readiness": "never"}, 1, 2, 2), ({"create_failure": True}, 1, 2, 2),
     ({"existing": True}, 1, 0, 0)],
)
def test_kind_setup_has_one_owned_recreate_and_preserves_foreign_clusters(
    tmp_path: Path, controls: dict[str, object], status: int, creates: int, deletes: int
) -> None:
    env, state = _setup_commands(tmp_path, **controls)
    output = tmp_path / "output"
    env["GITHUB_OUTPUT"] = str(output)
    config = tmp_path / "kind.yaml"
    config.write_text("kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\n")
    result = subprocess.run(
        ["bash", str(REPO_ROOT / ".github/scripts/prepare-kind.sh"), "ci-fixture", str(config)],
        env=env, capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == status, result.stdout + result.stderr
    assert (output.read_text() if output.exists() else "") == (
        "owned_cluster=ci-fixture\n" if status == 0 else ""
    )
    observed = json.loads(state.read_text())
    assert observed["creates"] == creates
    assert sum(call[:3] == ["kind", "delete", "cluster"] for call in observed["calls"]) == deletes
    assert all("--name" in call and call[call.index("--name") + 1] == "ci-fixture"
               for call in observed["calls"] if call[:3] == ["kind", "delete", "cluster"])


@pytest.mark.parametrize(
    ("failures", "error", "status", "pulls"),
    [(0, "", 0, 1), (2, "502 Bad Gateway", 0, 3),
     (3, "502 Bad Gateway", 1, 3), (3, "401 Unauthorized", 1, 1)],
)
def test_image_pull_retries_only_bounded_gateway_failures(
    tmp_path: Path, failures: int, error: str, status: int, pulls: int
) -> None:
    env, state = _setup_commands(tmp_path, pull_failures=failures, pull_error=error)
    result = subprocess.run(
        ["bash", str(REPO_ROOT / ".github/scripts/pull-image.sh"), "example.com/acme/image:test"],
        env=env, capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == status, result.stdout + result.stderr
    assert json.loads(state.read_text())["pulls"] == pulls


def test_kind_nodes_pull_docker_hub_through_the_mirror(tmp_path: Path) -> None:
    # The kind node's containerd does not read the host daemon's
    # registry-mirrors, so without its own mirror every docker.io image the
    # chart names (busybox for the AVX preflight hook, among others) is pulled
    # anonymously from Docker Hub and hits its shared-IP rate limit (429).
    env, state = _setup_commands(tmp_path)
    config = tmp_path / "kind.yaml"
    config.write_text(
        "kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n  - role: control-plane\n"
    )
    result = subprocess.run(
        ["bash", str(REPO_ROOT / ".github/scripts/prepare-kind.sh"), "ci-fixture", str(config)],
        env=env, capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    (created,) = json.loads(state.read_text())["configs"]
    document = yaml.safe_load(created)
    assert document["nodes"] == [{"role": "control-plane"}]
    patch = tomllib.loads("\n".join(document["containerdConfigPatches"]))
    registry = patch["plugins"]["io.containerd.grpc.v1.cri"]["registry"]
    assert registry["mirrors"]["docker.io"]["endpoint"] == [
        f"https://{MIRROR_HOST}",
        "https://registry-1.docker.io",
    ]


def test_kind_setup_refuses_a_config_that_already_patches_containerd(tmp_path: Path) -> None:
    env, state = _setup_commands(tmp_path)
    config = tmp_path / "kind.yaml"
    config.write_text(
        "kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\n"
        "containerdConfigPatches:\n  - |\n    x = 1\n"
    )
    result = subprocess.run(
        ["bash", str(REPO_ROOT / ".github/scripts/prepare-kind.sh"), "ci-fixture", str(config)],
        env=env, capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "containerdConfigPatches" in result.stderr
    assert json.loads(state.read_text())["creates"] == 0


def test_kind_setup_refuses_a_corrupt_manifest_before_creating_a_cluster(tmp_path: Path) -> None:
    checksum = shutil.which("sha256sum")
    assert checksum is not None
    env, state = _setup_commands(tmp_path)
    executable = tmp_path / "bin/sha256sum"
    executable.unlink()
    executable.symlink_to(checksum)
    config = tmp_path / "kind.yaml"
    config.write_text("kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\n")
    result = subprocess.run(
        ["bash", str(REPO_ROOT / ".github/scripts/prepare-kind.sh"), "ci-fixture", str(config)],
        env=env, capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "FAILED" in result.stdout
    assert json.loads(state.read_text())["creates"] == 0


def _load(name: str) -> dict:
    return yaml.safe_load((WORKFLOWS / name).read_text())


def _jobs(name: str):
    document = _load(name)
    for job_id, job in (document.get("jobs") or {}).items():
        if isinstance(job, dict) and job.get("steps"):
            yield job_id, job["steps"]


def _action(step: dict) -> str | None:
    uses = step.get("uses")
    if not isinstance(uses, str):
        return None
    return uses.split("@", 1)[0]


def _run(step: dict) -> str:
    body = step.get("run")
    return body if isinstance(body, str) else ""


def _backoff_name(base: str) -> str:
    group, separator, label = base.rpartition(": ")
    return group + separator + BACKOFF_PREFIX + label


def test_acquisition_actions_in_ci_and_helm_have_a_retry_trio() -> None:
    """Each tool-install and kind-create step retries once, with backoff."""
    missing: list[str] = []
    for filename in SCOPED:
        for job_id, steps in _jobs(filename):
            names = [step.get("name") for step in steps if isinstance(step, dict)]
            for step in steps:
                if not isinstance(step, dict):
                    continue
                action = _action(step)
                if action not in REQUIRED_ACTIONS:
                    continue
                name = step.get("name")
                if isinstance(name, str) and (
                    name.endswith(RETRY_SUFFIX)
                    or name.rpartition(": ")[2].startswith(BACKOFF_PREFIX)
                ):
                    continue
                if not isinstance(name, str) or not name:
                    missing.append(f"{filename} :: {job_id} :: <unnamed {action}>")
                    continue
                if _backoff_name(name) not in names or f"{name}{RETRY_SUFFIX}" not in names:
                    missing.append(f"{filename} :: {job_id} :: {name}")
                if step.get("continue-on-error") is not True or not isinstance(step.get("id"), str):
                    missing.append(f"{filename} :: {job_id} :: {name} (attempt is not continuable)")
    assert not missing, (
        "these acquisition steps need the #1106 trio (continue-on-error attempt, "
        "backoff, exact retry copy):\n  " + "\n  ".join(missing)
    )


def test_missing_grouped_backoff_fails_acquisition_presence(monkeypatch) -> None:
    steps = _load("ci.yaml")["jobs"]["image-proofs"]["steps"]
    reduced = [
        step
        for step in steps
        if step.get("name") != "Toolchain: Back off before retrying Install uv"
    ]
    monkeypatch.setitem(globals(), "_jobs", lambda _: iter([("image-proofs", reduced)]))
    with pytest.raises(AssertionError, match="Toolchain: Install uv"):
        test_acquisition_actions_in_ci_and_helm_have_a_retry_trio()


def test_tool_and_chart_downloads_retry_with_backoff() -> None:
    """kubeconform, Calico, and published charts retry network and 5xx failures.

    Localhost curls are health checks. Retrying those would hide a service that
    is actually down, so this test ignores them.
    """
    bare: list[str] = []
    for filename in SCOPED:
        for job_id, steps in _jobs(filename):
            for index, step in enumerate(steps):
                if not isinstance(step, dict):
                    continue
                body = _run(step)
                if "https://" not in body and "http://" not in body:
                    continue
                remote = "curl " in body or "kubectl apply -f https://" in body
                if not remote:
                    continue
                if "localhost" in body or "127.0.0.1" in body:
                    continue
                tool_or_chart = any(
                    marker in body
                    for marker in (
                        "kubeconform",
                        "calico.yaml",
                        "projectcalico/calico",
                        "/releases/download/",
                    )
                )
                if not tool_or_chart:
                    continue
                name = step.get("name") or f"<unnamed step {index}>"
                piped = "kubeconform" in body and re.search(
                    r"curl\b[\s\S]*\|\s*(?:sudo\s+)?tar\b", body
                )
                if (
                    "kubectl apply -f https://" in body
                    or piped
                    or not all(
                        flag in body for flag in ("--retry ", "--retry-all-errors", "--retry-delay")
                    )
                ):
                    bare.append(f"{filename} :: {job_id} :: {name}")
    assert not bare, (
        "these downloads must use curl --retry --retry-all-errors --retry-delay "
        "and must not pass a remote URL straight to kubectl apply:\n  " + "\n  ".join(bare)
    )


def test_upgrade_matrix_download_retries_connection_resets() -> None:
    """curl --retry skips a connection reset. The matrix download must not.

    Shard s03 failed with curl exit 35 (connection reset by peer) while
    fetching a published binary. --retry-all-errors covers that class.
    A checksum mismatch after a complete download still fails closed.
    """
    script = (REPO_ROOT / "cli/scripts/cluster-upgrade-matrix.sh").read_text()
    start = script.index("download_pin()")
    body = script[start : script.index("\nfetch_published", start)]
    assert "--retry 5" in body
    assert "--retry-all-errors" in body
    assert "--retry-delay 5" in body
    assert "verify_sha256" in body


def test_kind_creation_recreates_a_missing_kind_registry() -> None:
    """The attempt and its backoff both recreate kind-registry when it is gone."""
    gaps: list[str] = []
    for filename in SCOPED:
        for job_id, steps in _jobs(filename):
            for index, step in enumerate(steps):
                if not isinstance(step, dict) or _action(step) != "helm/kind-action":
                    continue
                name = step.get("name") or ""
                if name.endswith(RETRY_SUFFIX):
                    continue
                earlier = [item for item in steps[:index] if isinstance(item, dict)]
                backoff = next(
                    (
                        candidate
                        for candidate in steps[index + 1 :]
                        if isinstance(candidate, dict)
                        and candidate.get("name") == _backoff_name(name)
                    ),
                    None,
                )
                label = name or "<unnamed>"
                script = "cli/scripts/ensure-kind-registry.sh"
                ensured = any(script in _run(item) for item in earlier)
                if not ensured:
                    gaps.append(f"{filename} :: {job_id} :: {label} has no ensure step before it")
                backoff_body = _run(backoff) if isinstance(backoff, dict) else ""
                if script not in backoff_body:
                    gaps.append(f"{filename} :: {job_id} :: {label} backoff skips the registry")
    assert not gaps, "\n  ".join(gaps)


def _job_pulls_docker_hub(steps: list) -> bool:
    for step in steps:
        if not isinstance(step, dict):
            continue
        action = _action(step) or ""
        if action in {
            "helm/kind-action",
            "docker/bake-action",
            "docker/build-push-action",
        }:
            return True
        body = _run(step)
        if "docker build" in body or "amazonlinux:" in body:
            return True
        if "docker compose" in body and " up" in body:
            return True
    return False


def test_docker_hub_pulls_go_through_the_mirror() -> None:
    """Step-time Docker Hub pulls use the mirror. BuildKit gets its own config.

    A job that only runs `docker compose config` or `docker tag` does not pull.
    """
    gaps: list[str] = []
    for filename in SCOPED:
        for job_id, steps in _jobs(filename):
            if _job_pulls_docker_hub(steps):
                if not any(
                    "cli/scripts/configure-docker-hub-mirror.sh" in _run(step)
                    for step in steps
                    if isinstance(step, dict)
                ):
                    gaps.append(f"{filename} :: {job_id} never configures the Docker Hub mirror")
            for step in steps:
                if not isinstance(step, dict) or _action(step) != "docker/setup-buildx-action":
                    continue
                # `buildkitd-config-inline` is the action's current input name;
                # `config-inline` is its older alias, still used by older jobs.
                inputs = step.get("with") or {}
                config = inputs.get("buildkitd-config-inline") or inputs.get("config-inline")
                name = step.get("name") or "setup-buildx"
                if not isinstance(config, str) or MIRROR_HOST not in config:
                    gaps.append(
                        f"{filename} :: {job_id} :: {name} buildkit config has no {MIRROR_HOST}"
                    )
    assert not gaps, "\n  ".join(gaps)


def _stub_docker(bin_dir: Path, state: Path) -> None:
    """A docker stand-in that records argv and honors inspect / start / run.

    The real daemon is an external service. This stub is the stand-in the script
    execs, and it can represent the three states the script must tell apart.
    """
    script = bin_dir / "docker"
    script.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -euo pipefail
            printf '%s\\n' "$*" >> "$DOCKER_LOG"
            state="${DOCKER_STATE:?}"
            cmd="$1"
            case "$cmd" in
              inspect)
                if [[ ! -f "$state" ]]; then
                  echo "No such container" >&2
                  exit 1
                fi
                if [[ "${2:-}" == "-f" ]]; then
                  cut -d' ' -f1 "$state"
                fi
                exit 0
                ;;
              start)
                echo "true stopped-was" > "$state"
                # mark running in the first field only
                echo "true" > "$state"
                exit 0
                ;;
              run)
                if [[ -f "${DOCKER_FAILS:-}" ]]; then
                  left="$(cat "$DOCKER_FAILS")"
                  if [[ "$left" -gt 0 ]]; then
                    echo $((left - 1)) > "$DOCKER_FAILS"
                    echo "registry blob 5xx" >&2
                    exit 1
                  fi
                fi
                if [[ -f "$state" ]]; then
                  echo "name already in use" >&2
                  exit 1
                fi
                echo "true" > "$state"
                exit 0
                ;;
              *)
                exit 0
                ;;
            esac
            """
        )
    )
    script.chmod(script.stat().st_mode | stat.S_IEXEC)


def test_missing_kind_registry_is_created_and_a_running_one_is_left_alone(tmp_path: Path) -> None:
    """Recreate only when the container is absent. A running registry stays up."""
    assert ENSURE_SCRIPT.is_file(), f"missing {ENSURE_SCRIPT}"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    state = tmp_path / "state"
    log = tmp_path / "docker.log"
    _stub_docker(bin_dir, state)
    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ.get('PATH', '')}",
        "DOCKER_LOG": str(log),
        "DOCKER_STATE": str(state),
        "KIND_REGISTRY_NAME": "kind-registry-test",
        "KIND_REGISTRY_IMAGE": "mirror.gcr.io/library/registry:2",
        "KIND_REGISTRY_PORT": "5000",
    }
    missing = subprocess.run(
        [str(ENSURE_SCRIPT)], env=env, check=False, text=True, capture_output=True
    )
    assert missing.returncode == 0, missing.stderr
    ran = log.read_text().splitlines()
    assert any(line.startswith("run ") for line in ran), ran
    assert "kind-registry-test" in "\n".join(ran)
    assert "mirror.gcr.io/library/registry:2" in "\n".join(ran)
    assert "127.0.0.1:5000:5000" in "\n".join(ran)

    log.write_text("")
    again = subprocess.run(
        [str(ENSURE_SCRIPT)], env=env, check=False, text=True, capture_output=True
    )
    assert again.returncode == 0, again.stderr
    second = log.read_text().splitlines()
    assert not any(line.startswith("run ") for line in second), second
    assert not any(line.startswith("start ") for line in second), second

    state.write_text("false\n")
    log.write_text("")
    stopped = subprocess.run(
        [str(ENSURE_SCRIPT)], env=env, check=False, text=True, capture_output=True
    )
    assert stopped.returncode == 0, stopped.stderr
    third = log.read_text().splitlines()
    assert any(line.startswith("start ") for line in third), third
    assert not any(line.startswith("run ") for line in third), third


def test_kind_registry_pull_retries_once_then_fails_closed(tmp_path: Path) -> None:
    """A transient image pull is retried. Two failures still fail the step.

    A registry that is already running is covered by the test above and must
    not reach docker run at all.
    """
    assert ENSURE_SCRIPT.is_file(), f"missing {ENSURE_SCRIPT}"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    state = tmp_path / "state"
    log = tmp_path / "docker.log"
    fails = tmp_path / "fails"
    _stub_docker(bin_dir, state)
    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ.get('PATH', '')}",
        "DOCKER_LOG": str(log),
        "DOCKER_STATE": str(state),
        "DOCKER_FAILS": str(fails),
        "KIND_REGISTRY_NAME": "kind-registry-test",
        "KIND_REGISTRY_RETRY_DELAY": "0",
    }
    fails.write_text("1\n")
    recovered = subprocess.run(
        [str(ENSURE_SCRIPT)], env=env, check=False, text=True, capture_output=True
    )
    assert recovered.returncode == 0, recovered.stderr
    runs = [line for line in log.read_text().splitlines() if line.startswith("run ")]
    assert len(runs) == 2, log.read_text()

    log.write_text("")
    state.unlink(missing_ok=True)
    fails.write_text("2\n")
    refused = subprocess.run(
        [str(ENSURE_SCRIPT)], env=env, check=False, text=True, capture_output=True
    )
    assert refused.returncode != 0
    refused_runs = [line for line in log.read_text().splitlines() if line.startswith("run ")]
    assert len(refused_runs) == 2, log.read_text()


def test_mirror_render_keeps_daemon_keys_and_refuses_a_local_restart(tmp_path: Path) -> None:
    """--render merges the mirror. Without CI=true the script does not restart Docker."""
    assert MIRROR_SCRIPT.is_file(), f"missing {MIRROR_SCRIPT}"
    daemon = tmp_path / "daemon.json"
    daemon.write_text(
        json.dumps({"log-driver": "json-file", "registry-mirrors": ["https://example.invalid"]})
    )
    rendered = subprocess.run(
        [str(MIRROR_SCRIPT), "--render", "--daemon-json", str(daemon)],
        check=False,
        text=True,
        capture_output=True,
        env={**os.environ, "CI": ""},
    )
    assert rendered.returncode == 0, rendered.stderr
    parsed = json.loads(rendered.stdout)
    assert parsed["log-driver"] == "json-file"
    assert parsed["registry-mirrors"] == [
        "https://example.invalid",
        "https://mirror.gcr.io",
    ]
    assert daemon.read_text().startswith("{")
    assert "mirror.gcr.io" not in daemon.read_text()

    refused = subprocess.run(
        [str(MIRROR_SCRIPT), "--daemon-json", str(daemon)],
        check=False,
        text=True,
        capture_output=True,
        env={key: value for key, value in os.environ.items() if key != "CI"},
    )
    assert refused.returncode != 0
    assert "mirror.gcr.io" not in daemon.read_text()
