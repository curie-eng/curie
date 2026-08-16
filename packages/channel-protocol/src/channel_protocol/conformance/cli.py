"""``curie-adapter-conformance``: the kit's console front door (#1516).

The command a vendor runs in its own repo and quotes in its own README, so the
two properties that matter most are structural here:

* **The exit code follows ``automated_floor``**, and nothing short of a full
  strict pass exits 0. An exit code that cannot tell a full run from a partial
  one is exactly what lets a README claim conformance off an egress only
  invocation.
* **The JSON payload carries the verdict next to the list of what no machine
  checked**, and the human render prints them together, so the verdict is never
  quotable on its own.

**The secret never comes from a profile named environment variable.** A third
party authored file must never choose which of an operator's secrets gets read
and where it is sent: a hostile profile naming another vault entry over a
perfectly valid HTTPS endpoint defeats every shape check that could be written.
The secret comes from ``--secret-file`` or ``--secret-stdin``, supplied at the
invocation boundary, or the command refuses. There is no ``--secret-env`` flag
to misuse, which is the point: ``credentials.egress_secret_env`` in the profile
documents what the ADAPTER reads and is never resolved here.

The kit posts deliberately wrong secrets, so run it against a test instance with
a throwaway secret.
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import cast

import yaml
from pydantic import ValidationError

from ..manifest import ProfileVersionError, load_profile
from .driver import IngressDriver
from .floor import FloorMode, run_floor
from .transport import AdapterUnderTest, redacted, side_effect_probe

# Generous, because the adapter under test is a third party service the kit has
# no other handle on: a slow honest answer must not read as a refusal.
REQUEST_TIMEOUT_S = 30.0

_USAGE_ERROR = 2
_NONCONFORMANT = 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="curie-adapter-conformance",
        description=(
            "Check a running channel adapter against the Curie seven rule wire "
            "floor. Exits 0 only when every machine checkable clause passed."
        ),
    )
    parser.add_argument(
        "--profile",
        type=Path,
        required=True,
        help="Path to the adapter.yaml binding profile.",
    )
    parser.add_argument(
        "--endpoint",
        required=True,
        help="The adapter's reply endpoint, the URL the worker POSTs events to.",
    )
    parser.add_argument(
        "--secret-file",
        type=Path,
        help="Path to a file holding the egress secret. One of the two accepted sources.",
    )
    parser.add_argument(
        "--secret-stdin",
        action="store_true",
        help="Read the egress secret from stdin, so it never touches disk.",
    )
    parser.add_argument(
        "--driver",
        help=(
            "An ingress driver, as module:attr naming a zero argument factory. "
            "Without it floor rules 1, 2 and 7 and clause 3b report not_run, and "
            "not_run makes the verdict fail."
        ),
    )
    parser.add_argument(
        "--mode",
        choices=("strict", "diagnostic"),
        default="strict",
        help=(
            "strict is the only mode that can reach a passing verdict. diagnostic "
            "reports partial results while building and never passes."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Emit the report as JSON instead of a human render.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run one conformance check. ``None`` means read the process arguments.

    The ``None`` default is the console script's calling convention: the
    generated entry point invokes ``main()`` with no arguments. Every other
    caller, this package's tests included, passes an explicit argument vector.
    """

    parser = build_parser()
    args = parser.parse_args(None if argv is None else list(argv))

    try:
        raw = yaml.safe_load(args.profile.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as error:
        print(f"the profile could not be read: {error}", file=sys.stderr)
        return _USAGE_ERROR
    if not isinstance(raw, dict):
        print("the profile is not a YAML mapping", file=sys.stderr)
        return _USAGE_ERROR
    try:
        profile = load_profile(raw)
    except (ProfileVersionError, ValidationError) as error:
        print(f"the profile is not usable: {error}", file=sys.stderr)
        return _USAGE_ERROR

    secret = _read_secret(args.secret_file, stdin=args.secret_stdin)
    if secret is None:
        return _USAGE_ERROR

    driver: IngressDriver | None = None
    if args.driver is not None:
        try:
            driver = _load_driver(args.driver)
        except (ImportError, AttributeError, TypeError, ValueError) as error:
            print(
                f"the driver spec {args.driver!r} could not be resolved: {error}. "
                "Without a driver rules 1, 2 and 7 would silently report not_run, "
                "so a typo is refused rather than run around.",
                file=sys.stderr,
            )
            return _USAGE_ERROR

    adapter = AdapterUnderTest(
        endpoint=args.endpoint,
        secret=secret,
        kind=profile.kind,
        address=profile.address.example,
        timeout_s=REQUEST_TIMEOUT_S,
    )
    report = run_floor(
        adapter,
        driver=driver,
        side_effect_probe=side_effect_probe(adapter),
        mode=cast(FloorMode, args.mode),
    )
    if args.as_json:
        print(json.dumps(report.model_dump(mode="json"), indent=2))
    else:
        print(f"adapter endpoint: {redacted(adapter.endpoint)}")
        print(report.detail())
    return 0 if report.automated_floor == "pass" else _NONCONFORMANT


def _read_secret(secret_file: Path | None, *, stdin: bool) -> str | None:
    """The egress secret, from the invocation boundary or nowhere."""

    if secret_file is not None and stdin:
        print(
            "pass exactly one of --secret-file and --secret-stdin", file=sys.stderr
        )
        return None
    if secret_file is not None:
        return secret_file.read_text(encoding="utf-8").strip()
    if stdin:
        return sys.stdin.read().strip()
    print(
        "the egress secret must be supplied at the invocation boundary: pass "
        "--secret-file PATH or --secret-stdin. credentials.egress_secret_env in the "
        "profile documents what the ADAPTER reads and is never resolved here, so a "
        "profile can never choose which of your secrets gets read or where it is sent.",
        file=sys.stderr,
    )
    return None


def _load_driver(spec: str) -> IngressDriver:
    """Resolve ``module:attr`` to a driver by calling the named factory."""

    module_name, separator, attribute = spec.partition(":")
    if not separator or not module_name or not attribute:
        raise ValueError("a driver spec is module:attr naming a zero argument factory")
    module = importlib.import_module(module_name)
    factory = getattr(module, attribute)
    return cast(IngressDriver, factory())
