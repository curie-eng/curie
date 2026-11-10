"""The recovery hold must reach the source copied by a fresh token-bearing claim.

Kubernetes is the external boundary here. The deployed kind check separately
asserts a real Pending sandbox and consumer-owned PEL before killing its owner.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "cli/scripts/e2e-cluster-rollout-recovery.sh"


@pytest.mark.parametrize("case", ["scoped", "token-free", "missing-agent", "missing-source"])
def test_recovery_hold_targets_source_not_completed_claim_copy(tmp_path: Path, case: str) -> None:
    script = SCRIPT.read_text()
    start = script.index("resolve_claim_sandbox_template() {")
    end = script.index("\npatch_sandbox_template_unschedulable()", start)
    function = script[start:end]
    kubectl = tmp_path / "kubectl"
    kubectl.write_text(
        """#!/usr/bin/env python3
import os, subprocess, sys
args = sys.argv[1:]
if 'exec' in args:
    command = args[args.index('--') + 1:]
    env = {**os.environ, 'CURIE_WARM_POOL': 'acme-runner-pool'}
    raise SystemExit(subprocess.run(command, env=env).returncode)
name = args[args.index('get') + 2]
selector = args[-1]
case = os.environ['CURIE_TEMPLATE_CASE']
if name == 'first-claim':
    print('acme-agent-weather-runner-pool' if case == 'token-free' else
          'first-claim-resources-pool')
elif name == 'first-claim-resources-pool': print('first-claim-resources')
elif name == 'first-claim-resources' and 'agent' in selector:
    print('' if case == 'missing-agent' else 'weather')
elif name == 'acme-agent-weather-runner-pool':
    if case == 'missing-source': raise SystemExit('source pool unavailable')
    print('acme-agent-weather-runner')
else: raise SystemExit('unrecognized Kubernetes read: ' + repr(args))
"""
    )
    kubectl.chmod(0o755)
    result = subprocess.run(
        [
            "bash", "-c", function
            + '\nNAMESPACE=acme-system\nOLD_POD=fixture-worker'
            + '\nresolve_claim_sandbox_template first-claim',
        ],
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}", "CURIE_TEMPLATE_CASE": case},
        text=True,
        capture_output=True,
        check=False,
    )
    if case.startswith("missing-"):
        assert result.returncode != 0
        assert result.stdout == "", "unproved source must not be returned as a hold target"
        return
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "acme-agent-weather-runner", (
        "holding the completed claim copy leaves a fresh claim schedulable: " + result.stdout
    )
