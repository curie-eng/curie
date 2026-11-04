"""Project API recovery logs onto fixed diagnostic metadata."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

_IDENTIFIER = r"[A-Za-z_]\w*"
_TYPE = (
    rf"(?:{_IDENTIFIER}\.)*(?:{_IDENTIFIER}(?:Error|Exception|Timeout|Exit|ExceptionGroup)"
    r"|Exception|ExceptionGroup|KeyboardInterrupt)"
)
_QUALIFIED_TYPE = rf"(?:{_IDENTIFIER}\.)+[A-Z]\w*"
_TYPE_PATTERNS = (
    re.compile(rf"(?<![\w.])({_TYPE})(?=[:'\">)]|$)"),
    re.compile(rf"(?<![\w.])({_QUALIFIED_TYPE})(?=[:)]|$)"),
    re.compile(rf"<class ['\"]((?:{_IDENTIFIER}\.)*{_IDENTIFIER})['\"]>"),
)
_COMPLETED = re.compile(
    r"^http request completed method=[A-Z]+ route=(/[^\s?#]*) status_class=5xx$"
)
_SEVERITIES = frozenset({"WARNING", "ERROR", "CRITICAL"})
_UVICORN = re.compile(r"^(WARNING|ERROR|CRITICAL):\s+")
_LOGGER = re.compile(rf"{_IDENTIFIER}(?:\.{_IDENTIFIER})*")


def exception_types(text: str) -> list[str]:
    return sorted({match for pattern in _TYPE_PATTERNS for match in pattern.findall(text)})


def summarize(current: str, *, restart_count: int, previous: str = "") -> dict[str, object]:
    types: set[str] = set()
    routes: set[str] = set()
    warnings: list[tuple[str, str]] = []
    server_errors = 0
    for raw in current.splitlines():
        try:
            record = json.loads(raw)
        except ValueError:
            record = None
        if isinstance(record, dict):
            severity, logger = record.get("severity"), record.get("logger")
            if (
                isinstance(severity, str)
                and severity in _SEVERITIES
                and isinstance(logger, str)
                and _LOGGER.fullmatch(logger)
            ):
                warnings.append((logger, severity))
            texts = [record.get(key) for key in ("message", "exception")]
            message = record.get("message")
            route = _COMPLETED.fullmatch(message) if isinstance(message, str) else None
            if route:
                routes.add(route.group(1))
        else:
            severity_match = _UVICORN.match(raw)
            if severity_match:
                warnings.append(("uvicorn.error", severity_match.group(1)))
            texts = [raw]
        for text in texts:
            if isinstance(text, str):
                server_errors += text.count('" 500')
                types.update(exception_types(text))
    return {
        "phase": "recovery",
        "api_exception_types": sorted(types),
        "api_http_500_lines": server_errors,
        "api_5xx_routes": sorted(routes),
        "api_warning_records": [
            {"logger": logger, "severity": severity} for logger, severity in sorted(warnings)
        ],
        "api_restart_count": restart_count,
        "api_previous_exception_types": exception_types(previous) if restart_count else [],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logfile", type=Path)
    parser.add_argument("--restart-count", required=True, type=int)
    parser.add_argument("--previous-log", type=Path)
    args = parser.parse_args()
    if args.restart_count < 0:
        parser.error("restart count must be nonnegative")
    try:
        current = args.logfile.read_text(errors="replace")
        previous = args.previous_log.read_text(errors="replace") if args.previous_log else ""
    except OSError:
        parser.error("cannot read required API log")
    print(
        "Recovery API diagnostic: "
        + json.dumps(
            summarize(current, restart_count=args.restart_count, previous=previous), sort_keys=True
        )
    )


if __name__ == "__main__":
    main()
