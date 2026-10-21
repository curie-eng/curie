"""Classify one workspace diagnostic scenario's combined output and exit status."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def classify(output: str, exit_code: int) -> str:
    lines = output.splitlines()
    if "FAIL: recovery CLI turn failed" not in lines:
        return "pass" if exit_code == 0 else "other-fail"
    prefix = "Recovery diagnostic: "
    for line in lines:
        if not line.startswith(prefix):
            continue
        try:
            diagnostic = json.loads(line[len(prefix) :])
        except ValueError:
            continue
        if not isinstance(diagnostic, dict):
            continue
        status = diagnostic.get("error_http_status")
        if (type(status) is int and status == 500) or diagnostic.get("error_http_500") is True:
            return "recovery-500"
    return "pass" if exit_code == 0 else "other-fail"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exit-code", required=True, type=int)
    parser.add_argument("logfile", type=Path)
    arguments = parser.parse_args()
    try:
        output = arguments.logfile.read_text(encoding="utf-8", errors="replace")
    except OSError:
        parser.error("cannot read logfile")
    print(classify(output, arguments.exit_code))


if __name__ == "__main__":
    main()
