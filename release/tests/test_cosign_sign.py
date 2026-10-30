"""Retry contract for release/cosign_sign.py (#5121).

Keyless cosign signing can fail after Rekor has already recorded that
attempt's ephemeral key. A second attempt in the same process repeats the
key and fails again. The wrapper runs the supplied argv in a new process,
up to three times. The release workflow points every `cosign sign` and
`cosign sign-blob` step at it. These tests drive the wrapper as a subprocess
with a fake cosign on PATH and a zero sleep, and they check that the four
release signing steps kept their arguments and call the wrapper.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "release" / "cosign_sign.py"
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "release.yaml"

IMAGE_ARGV = ["cosign", "sign", "--yes", "ghcr.io/example/image@sha256:abc"]
SIGN_COMMAND = re.compile(r"(?:^|[^A-Za-z0-9_])cosign sign(?:-blob)?(?:\s|$)")
MERGE_JOBS = ("merge", "worker-local-merge", "dark-factory-runner-merge")
IMAGE_REFS = (
    "ghcr.io/${{ github.repository_owner }}/curie-${{ matrix.name }}"
    "@${{ steps.index.outputs.digest }}",
    "ghcr.io/${{ github.repository_owner }}/curie-worker-local"
    "@${{ steps.index.outputs.digest }}",
    "ghcr.io/${{ github.repository_owner }}/curie-dark-factory-runner"
    "@${{ steps.index.outputs.digest }}",
)
RETRY_1 = "cosign attempt 1/3 failed (exit 1), retrying in 0s\n"
RETRY_2 = "cosign attempt 2/3 failed (exit 1), retrying in 0s\n"
FAKE_STDOUT = "fake-stdout-distinctive-line\n"
FAKE_STDERR = "fake-stderr-distinctive-line\n"

FAKE_COSIGN = "\n".join(
    [
        "#!/usr/bin/env python3",
        "import json",
        "import os",
        "import sys",
        "from pathlib import Path",
        "",
        'log_path = Path(os.environ["COSIGN_FAKE_LOG"])',
        "raw_codes = os.environ['COSIGN_FAKE_CODES'].split(',')",
        "codes = [int(part) for part in raw_codes]",
        "if log_path.exists():",
        "    previous = log_path.read_text(encoding='utf-8').splitlines()",
        "else:",
        "    previous = []",
        "index = len(previous)",
        "code = codes[index] if index < len(codes) else codes[-1]",
        "# Shebang execution rewrites argv[0] to this script's path.",
        "argv = [Path(sys.argv[0]).name, *sys.argv[1:]]",
        'payload = json.dumps(argv) + "\\n"',
        'with log_path.open("a", encoding="utf-8") as handle:',
        "    handle.write(payload)",
        "if code != 0:",
        '    stdout = os.environ.get("COSIGN_FAKE_FAIL_STDOUT", "")',
        '    stderr = os.environ.get("COSIGN_FAKE_FAIL_STDERR", "")',
        "    if stdout:",
        "        sys.stdout.write(stdout)",
        "    if stderr:",
        "        sys.stderr.write(stderr)",
        "raise SystemExit(code)",
        "",
    ]
)


def _load_cosign_sign():
    """Import the standalone script by path (release/ is not on sys.path)."""
    spec = importlib.util.spec_from_file_location("cosign_sign", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_wrapper(
    directory: Path,
    args: list[str],
    codes: str = "0",
    *,
    sleep: str = "0",
    fail_stdout: str = "",
    fail_stderr: str = "",
    install_fake: bool = True,
) -> tuple[subprocess.CompletedProcess[str], list[list[str]]]:
    """Run `python3 release/cosign_sign.py` with a fake cosign prepended to PATH."""
    directory.mkdir(parents=True, exist_ok=True)
    bin_dir = directory / "bin"
    bin_dir.mkdir()
    log_path = directory / "invocations.jsonl"
    if install_fake:
        fake = bin_dir / "cosign"
        fake.write_text(FAKE_COSIGN, encoding="utf-8", newline="\n")
        fake.chmod(0o755)
    env = os.environ.copy()
    env["PATH"] = str(bin_dir) + os.pathsep + env.get("PATH", "")
    env["COSIGN_SIGN_RETRY_SLEEP_SECONDS"] = sleep
    env["COSIGN_FAKE_LOG"] = str(log_path)
    env["COSIGN_FAKE_CODES"] = codes
    if fail_stdout:
        env["COSIGN_FAKE_FAIL_STDOUT"] = fail_stdout
    if fail_stderr:
        env["COSIGN_FAKE_FAIL_STDERR"] = fail_stderr
    if not install_fake:
        env["PATH"] = str(bin_dir)
    result = subprocess.run(
        [sys.executable, "release/cosign_sign.py", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    log: list[list[str]] = []
    if log_path.exists():
        for line in log_path.read_text(encoding="utf-8").splitlines():
            log.append(json.loads(line))
    return result, log


def load_workflow() -> dict:
    # BaseLoader keeps `on:` a string key and every scalar a string; anchors
    # and aliases still resolve.
    return yaml.load(WORKFLOW_PATH.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def run_of(step: dict) -> str:
    run = step.get("run", "")
    return run if isinstance(run, str) else ""


def command_text(run: str) -> str:
    kept = [line for line in run.splitlines() if not line.strip().startswith("#")]
    return "\n".join(kept)


def is_sign_command(run: str) -> bool:
    """True when `run` invokes `cosign sign` or `cosign sign-blob` as a command."""
    return SIGN_COMMAND.search(command_text(run)) is not None


def wrapper_precedes_sign(script: str) -> bool:
    body = command_text(script)
    wrapper = body.find("release/cosign_sign.py")
    match = SIGN_COMMAND.search(body)
    return wrapper != -1 and match is not None and wrapper < match.start()


def test_retries_twice_then_succeeds(tmp_path: Path) -> None:
    result, log = run_wrapper(tmp_path, ["--", *IMAGE_ARGV], codes="1,1,0")
    assert result.returncode == 0
    assert log == [IMAGE_ARGV, IMAGE_ARGV, IMAGE_ARGV]


def test_third_failure_exits_with_that_code_and_does_not_retry_again(tmp_path: Path) -> None:
    result, log = run_wrapper(tmp_path, ["--", *IMAGE_ARGV], codes="1")
    assert result.returncode == 1
    assert log == [IMAGE_ARGV, IMAGE_ARGV, IMAGE_ARGV]
    assert result.stderr == RETRY_1 + RETRY_2
    assert "cosign attempt 3/3" not in result.stderr


def test_first_success_runs_once(tmp_path: Path) -> None:
    result, log = run_wrapper(tmp_path, ["--", *IMAGE_ARGV], codes="0")
    assert result.returncode == 0
    assert log == [IMAGE_ARGV]


def test_child_output_passes_through_and_the_retry_line_is_added(tmp_path: Path) -> None:
    result, log = run_wrapper(
        tmp_path,
        ["--", *IMAGE_ARGV],
        codes="1,0",
        fail_stdout=FAKE_STDOUT,
        fail_stderr=FAKE_STDERR,
    )
    assert result.returncode == 0
    assert log == [IMAGE_ARGV, IMAGE_ARGV]
    assert result.stdout == FAKE_STDOUT
    assert result.stderr == FAKE_STDERR + RETRY_1


def test_missing_separator_exits_2_without_running_cosign(tmp_path: Path) -> None:
    refused, refused_log = run_wrapper(tmp_path / "missing", [])
    assert refused.returncode == 2
    assert refused_log == []
    assert refused.stderr.strip()
    assert "\n" not in refused.stderr.strip()

    bare, bare_log = run_wrapper(tmp_path / "bare", ["cosign", "sign", "--yes", "ref"])
    assert bare.returncode == 2
    assert bare_log == []

    empty, empty_log = run_wrapper(tmp_path / "empty", ["--"])
    assert empty.returncode == 2
    assert empty_log == []

    valid, valid_log = run_wrapper(
        tmp_path / "valid",
        ["--", "cosign", "sign", "--yes", "ref"],
        codes="0",
    )
    assert valid.returncode != 2
    assert valid.returncode == 0
    assert valid_log == [["cosign", "sign", "--yes", "ref"]]


def test_non_integer_sleep_exits_2_without_running_cosign(tmp_path: Path) -> None:
    result, log = run_wrapper(
        tmp_path,
        ["--", "cosign", "sign", "--yes", "ref"],
        codes="0",
        sleep="nope",
    )
    assert result.returncode == 2
    assert log == []
    assert result.stderr.strip()
    assert "\n" not in result.stderr.strip()
    assert "COSIGN_SIGN_RETRY_SLEEP_SECONDS" in result.stderr


def test_missing_cosign_counts_as_exit_127_and_retries(tmp_path: Path) -> None:
    result, log = run_wrapper(tmp_path, ["--", *IMAGE_ARGV], install_fake=False)
    assert result.returncode == 127
    assert log == []
    assert "cosign attempt 1/3 failed (exit 127), retrying in 0s\n" in result.stderr
    assert "cosign attempt 2/3 failed (exit 127), retrying in 0s\n" in result.stderr
    assert "cosign attempt 3/3" not in result.stderr


def test_default_sleep_seconds_is_20() -> None:
    assert _load_cosign_sign().DEFAULT_SLEEP_SECONDS == 20


def test_release_workflow_signs_through_the_wrapper() -> None:
    text = WORKFLOW_PATH.read_text(encoding="utf-8")
    assert "COSIGN_SIGN_RETRY_SLEEP_SECONDS" not in text
    jobs = load_workflow()["jobs"]
    found: list[tuple[str, dict]] = []
    for job_name, job in jobs.items():
        for step in job.get("steps", []):
            if is_sign_command(run_of(step)):
                found.append((job_name, step))
    assert len(found) == 4
    scripts = [run_of(step) for _, step in found]
    assert all(wrapper_precedes_sign(script) for script in scripts)
    assert all("cosign verify" not in script for script in scripts)
    for script in scripts:
        assert "sleep" not in script
        assert " for " not in script
        assert " while " not in script
        assert " until " not in script
        assert "of 2, retrying in" not in script
        assert "continue-on-error" not in script

    for ref in IMAGE_REFS:
        matches = [
            script for script in scripts if ref in script and "cosign sign --yes" in script
        ]
        assert len(matches) == 1
        assert "sign-blob" not in matches[0]
        assert "${{ steps.index.outputs.digest }}" in matches[0]

    blob_matches = [script for script in scripts if "cosign sign-blob --yes" in script]
    assert len(blob_matches) == 1
    blob = blob_matches[0]
    assert "--bundle dist/checksums.txt.sigstore.json" in blob
    assert any(line.strip() == "dist/checksums.txt" for line in blob.splitlines())

    for job_name in MERGE_JOBS:
        steps = jobs[job_name]["steps"]
        indexes = [
            index for index, step in enumerate(steps) if "cosign sign --yes" in run_of(step)
        ]
        assert len(indexes) == 1
        sign_at = indexes[0]
        checkout = steps[sign_at - 1]
        assert checkout.get("uses") == "actions/checkout@v7"
        assert checkout.get("with", {}).get("persist-credentials") == "false"
        assert steps[sign_at].get("name") == "Sign the image"

    checksum = [
        step
        for step in jobs["release"]["steps"]
        if step.get("name") == "Sign the checksum manifest"
    ]
    assert len(checksum) == 1
    assert is_sign_command(run_of(checksum[0]))
