"""Project API recovery logs onto fixed diagnostic metadata."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

_IDENTIFIER = r"[A-Za-z_]\w*"
_UNQUALIFIED_TYPE = (
    rf"{_IDENTIFIER}(?:Error|Exception|Timeout|Exit|ExceptionGroup)"
    r"|Exception|ExceptionGroup|KeyboardInterrupt"
)
_KNOWN_QUALIFIED_TYPE = rf"(?:asyncpg|psycopg|sqlalchemy|builtins)\.(?:{_IDENTIFIER}\.)*[A-Z]\w*"
_TYPE = rf"(?:(?:{_IDENTIFIER}\.)*(?:{_UNQUALIFIED_TYPE})|{_KNOWN_QUALIFIED_TYPE})"
_CLASS_TYPE = rf"(?:{_KNOWN_QUALIFIED_TYPE}|[A-Z]\w*)"
_TRACEBACK_LINE = re.compile(
    rf"^[\s|+\-]*(?:({_TYPE}):|\(({_TYPE})\)|<class ['\"]({_CLASS_TYPE})['\"]>)"
)
_PAREN_TYPE = re.compile(rf"\(({_TYPE})\)")
_CLASS_PATTERN = re.compile(rf"<class ['\"]({_CLASS_TYPE})['\"]>")
_COMPLETED = re.compile(
    r"^http request completed method=[A-Z]+ route=(/[^\s?#]*) status_class=5xx$"
)
# Only GET /agents is recognized from access logs. Adding a route requires proof
# that it has no identifier segments and a privacy regression test.
_AGENT_LIST_ACCESS = re.compile(r' - "GET /agents(?:\?[^"\s]*)? HTTP/[0-9.]+" 5[0-9]{2}(?:\s|$)')
_SEVERITIES = frozenset({"WARNING", "ERROR", "CRITICAL"})
_UVICORN = re.compile(r"^(WARNING|ERROR|CRITICAL):\s+")
_LOGGER = re.compile(rf"{_IDENTIFIER}(?:\.{_IDENTIFIER})*")


def exception_types(text: str) -> list[str]:
    types: set[str] = set()
    for line in text.splitlines():
        match = _TRACEBACK_LINE.match(line)
        if match:
            types.update(name for name in match.groups() if name)
            types.update(_PAREN_TYPE.findall(line))
            types.update(_CLASS_PATTERN.findall(line))
    return sorted(types)


def log_exception_types(log: str) -> list[str]:
    types: set[str] = set()
    for raw in log.splitlines():
        try:
            record = json.loads(raw)
        except ValueError:
            record = None
        text = record.get("exception") if isinstance(record, dict) else raw
        if isinstance(text, str):
            types.update(exception_types(text))
    return sorted(types)[:12]


def summarize(current: str, *, restart_count: int, previous: str = "") -> dict[str, object]:
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
            if logger == "uvicorn.access" and isinstance(message, str):
                if _AGENT_LIST_ACCESS.search(message):
                    routes.add("/agents")
        else:
            severity_match = _UVICORN.match(raw)
            if severity_match:
                warnings.append(("uvicorn.error", severity_match.group(1)))
            if raw.startswith("INFO:") and _AGENT_LIST_ACCESS.search(raw):
                routes.add("/agents")
            texts = [raw]
        for text in texts:
            if isinstance(text, str):
                server_errors += '" 500' in text
    return {
        "phase": "recovery",
        "api_exception_types": log_exception_types(current),
        "api_http_500_lines": server_errors,
        "api_5xx_routes": sorted(routes),
        "api_warning_records": [
            {"logger": logger, "severity": severity} for logger, severity in sorted(warnings)
        ],
        "api_restart_count": restart_count,
        "api_previous_exception_types": log_exception_types(previous) if restart_count else [],
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
