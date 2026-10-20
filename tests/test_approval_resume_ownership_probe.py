"""The approval-resume e2e must say why its agent-ownership probe failed.

On the first deploy the script tolerates only the unbound-route refusal, and it
decides the agent exists by listing its approvals. A bare failure of that probe
used to end in "first deploy failed for a reason other than the unbound route"
with the probe's own error discarded, so the cause of a nightly failure could
not be recovered. These tests drive the script's real first-deploy block and
helper definitions through bash. Only the `curie` executable (the external CLI
boundary) is faked.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "cli/scripts/e2e-cluster-approval-resume-restarts.sh"

ROUTE = "e2e_resume_fixture"
PROBE_LINE = "agent ownership probe failed"
PROBE_ERROR = "error: approvals API returned 503"
UNBOUND_FAILURE = "first deploy failed for a reason other than the unbound route"

FAKE_CURIE = """#!/usr/bin/env python3
import os, sys

args = sys.argv[1:]
if 'deploy' in args:
    # A refused deploy: the JSON error object is on stdout.
    if os.environ['CURIE_DEPLOY_NAMES_ROUTE'] == '1':
        print('{"error":"approval route ' + os.environ['CURIE_ROUTE'] + ' is not bound"}')
    else:
        print('{"error":"chart render failed"}')
    raise SystemExit(1)
if 'approvals' in args and '--list' in args:
    if os.environ['CURIE_PROBE_OK'] == '1':
        print('{"routes":[]}')
        raise SystemExit(0)
    sys.stderr.write('error: approvals API returned 503\\n')
    raise SystemExit(1)
sys.stderr.write('unrecognized curie call: ' + repr(args) + '\\n')
raise SystemExit(99)
"""


def _slice_to_close(script: str, opener: str) -> str:
    start = script.index(opener)
    end = script.index("\n}\n", start) + len("\n}\n")
    return script[start:end]


def _first_deploy_block(script: str) -> str:
    start = script.index('echo "=== first deploy:')
    refusal = script.index('fail "first deploy failed', start)
    # The inner `fi` is indented, so the first column-zero `fi` closes the if/else.
    end = script.index("\nfi\n", refusal) + len("\nfi\n")
    return script[start:end]


def _run_block(
    tmp_path: Path, *, probe_ok: bool, deploy_names_route: bool
) -> subprocess.CompletedProcess[str]:
    script = SCRIPT.read_text()
    helpers = "\n".join(
        [
            # curie_cluster is a one-liner that precedes approvals_cli.
            script[
                script.index("curie_cluster() {") : script.index(
                    "\n}\n", script.index("approvals_cli() {")
                )
                + len("\n}\n")
            ],
            _slice_to_close(script, "fail() {"),
            _slice_to_close(script, "deploy_agent() {"),
        ]
    )
    block = _first_deploy_block(script)

    curie = tmp_path / "curie"
    curie.write_text(FAKE_CURIE)
    curie.chmod(0o755)
    workdir = tmp_path / "work"
    workdir.mkdir()

    program = "\n".join(
        [
            "set -euo pipefail",
            f"BIN={curie}",
            "CURIE_CTX=()",
            "NAMESPACE=fixture-ns",
            "RELEASE=fixture-release",
            "AGENT=fixture-agent",
            f"ROUTE={ROUTE}",
            "CHANNEL=C0EXAMPLE1",
            f"BUNDLE_DIR={tmp_path}/bundle",
            f"REPO_ROOT={REPO_ROOT}",
            f"WORKDIR={workdir}",
            "AGENT_OWNED=0",
            helpers,
            block,
            'echo "AGENT_OWNED=$AGENT_OWNED"',
        ]
    )
    return subprocess.run(
        ["bash", "-c", program],
        env={
            **os.environ,
            "CURIE_ROUTE": ROUTE,
            "CURIE_PROBE_OK": "1" if probe_ok else "0",
            "CURIE_DEPLOY_NAMES_ROUTE": "1" if deploy_names_route else "0",
        },
        text=True,
        capture_output=True,
        check=False,
    )


def test_failed_ownership_probe_surfaces_its_error_before_the_route_failure(
    tmp_path: Path,
) -> None:
    result = _run_block(tmp_path, probe_ok=False, deploy_names_route=True)

    assert result.returncode != 0, result.stdout
    assert PROBE_LINE in result.stderr, result.stderr
    assert "approvals API returned 503" in result.stderr, result.stderr
    assert UNBOUND_FAILURE in result.stderr, result.stderr
    # The cause must precede the generic failure, or a reader of the tail never sees it.
    assert result.stderr.index(PROBE_LINE) < result.stderr.index(UNBOUND_FAILURE)
    assert result.stderr.index(PROBE_ERROR) < result.stderr.index(UNBOUND_FAILURE)
    assert "AGENT_OWNED=1" not in result.stdout


def test_successful_ownership_probe_with_unbound_route_refusal_passes_quietly(
    tmp_path: Path,
) -> None:
    result = _run_block(tmp_path, probe_ok=True, deploy_names_route=True)

    assert result.returncode == 0, result.stderr
    assert "AGENT_OWNED=1" in result.stdout
    assert PROBE_LINE not in result.stderr, result.stderr
    assert UNBOUND_FAILURE not in result.stderr, result.stderr


def test_refusal_that_does_not_name_the_route_still_fails_without_a_probe_line(
    tmp_path: Path,
) -> None:
    result = _run_block(tmp_path, probe_ok=True, deploy_names_route=False)

    assert result.returncode != 0, result.stdout
    assert UNBOUND_FAILURE in result.stderr, result.stderr
    assert PROBE_LINE not in result.stderr, result.stderr
