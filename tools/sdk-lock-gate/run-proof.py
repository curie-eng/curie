"""Run the existing live skill ladder and require its approval case observations.

A zero exit without reaching the case is not proof. In particular, a reset/setup
failure before the case must remain a named failure (#2221, #2308). These markers
come from the actual ladder assertions, not model response prose. This does not
replace those assertions or certify a replay as live provider evidence.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

START = "=== case: a live gated tool call is denied and the turn parks (#2094) ==="
PARK = "the gated turn parked: status=awaiting-approval finalized=false approval_summary=present"
ABSENT = "the gated command did not run: /tmp/curie-2094-canary is absent"


def fail(message: str) -> int:
    print(f"::error title=SDK approval-gate proof incomplete::{message}", file=sys.stderr)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ladder", type=Path, default=Path("cli/scripts/e2e-ladder.sh"))
    args = parser.parse_args()
    if os.environ.get("CURIE_E2E_LIVE") != "1" or os.environ.get("CURIE_E2E_TIERS") != "skill":
        return fail("CURIE_E2E_LIVE=1 and CURIE_E2E_TIERS=skill are required")
    observed: set[str] = set()
    with subprocess.Popen(
        ["bash", str(args.ladder)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    ) as process:
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="", flush=True)
            marker = line.rstrip("\r\n")
            if marker in {START, PARK, ABSENT}:
                observed.add(marker)
        code = process.wait()
    if START not in observed:
        return fail(f"runner setup/reset failed before approval-gate case; ladder exit {code}")
    if code != 0:
        return fail(f"ladder failed with exit {code}; the live proof did not pass")
    if PARK not in observed or ABSENT not in observed:
        return fail(
            "approval-gate case did not observe both a parked turn and an absent side effect"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
