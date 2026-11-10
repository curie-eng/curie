"""Command-line entry point: ``python -m unitconv VALUE SRC DST``."""

from __future__ import annotations

import sys

from .convert import UnknownUnit, convert


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 3:
        print("usage: python -m unitconv VALUE SRC DST", file=sys.stderr)
        return 2
    raw, src, dst = args
    try:
        value = float(raw)
    except ValueError:
        print(f"error: {raw!r} is not a number", file=sys.stderr)
        return 2
    try:
        result = convert(value, src, dst)
    except UnknownUnit as exc:
        print(f"error: unknown unit {exc}", file=sys.stderr)
        return 2
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"{result:.4g}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
