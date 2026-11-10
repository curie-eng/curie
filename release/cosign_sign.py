#!/usr/bin/env python3
"""Retry one cosign invocation in a fresh process, up to three times (#5121).

Keyless cosign mints an ephemeral signing key and publishes it to Rekor. A
failed attempt can leave that key's Rekor entry behind, so a retry inside the
same process repeats the key and fails again on the duplicate. Each attempt
here is a new process, which mints a new key. The release workflow puts this
wrapper in front of `cosign sign` and `cosign sign-blob`. Attestation stays a
single attempt.

The child's stdout and stderr pass through unchanged. Between attempts, and
only then, the wrapper prints one stderr line and sleeps:

    cosign attempt <n>/3 failed (exit <code>), retrying in <seconds>s

The sleep is DEFAULT_SLEEP_SECONDS unless COSIGN_SIGN_RETRY_SLEEP_SECONDS is
set to a non-negative integer (0 is valid). Any other value is an error and
cosign is not started. The release workflow does not set the variable.

Usage:
    python3 release/cosign_sign.py -- <argv...>
"""

from __future__ import annotations

import os
import subprocess
import sys
import time

DEFAULT_SLEEP_SECONDS = 20
ATTEMPTS = 3
SLEEP_ENV = "COSIGN_SIGN_RETRY_SLEEP_SECONDS"
USAGE = "usage: python3 release/cosign_sign.py -- <argv...>"


def _sleep_seconds() -> int | None:
    """Return the delay in seconds, or None when the environment value is refused."""
    raw = os.environ.get(SLEEP_ENV)
    if raw is None:
        return DEFAULT_SLEEP_SECONDS
    # ASCII digits only, so "0" and "20" are accepted and "-1" or "nope" are not.
    if raw.isascii() and raw.isdigit():
        return int(raw)
    print(
        f"cosign_sign: {SLEEP_ENV} must be a non-negative integer, got {raw!r}",
        file=sys.stderr,
    )
    return None


def _run_once(argv: list[str]) -> int:
    """Run argv once. A missing executable counts as exit 127."""
    try:
        completed = subprocess.run(argv, check=False)
    except FileNotFoundError:
        return 127
    return completed.returncode


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) < 2 or args[0] != "--":
        print(USAGE, file=sys.stderr)
        return 2
    seconds = _sleep_seconds()
    if seconds is None:
        return 2
    command = args[1:]
    code = 1
    for attempt in range(1, ATTEMPTS + 1):
        code = _run_once(command)
        if code == 0 or attempt == ATTEMPTS:
            return code
        print(
            f"cosign attempt {attempt}/{ATTEMPTS} failed (exit {code}), retrying in {seconds}s",
            file=sys.stderr,
            flush=True,
        )
        time.sleep(seconds)
    return code


if __name__ == "__main__":
    sys.exit(main())
