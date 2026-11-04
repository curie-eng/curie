"""Recovery metadata from curie_telemetry JSON and uvicorn stderr shapes."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

PARSER = Path(__file__).with_name("recovery_api_diagnostic.py")


def parse(current: str, previous: str = "", restarts: int = 0) -> dict:
    spec = importlib.util.spec_from_file_location("recovery_api_diagnostic", PARSER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.summarize(current, restart_count=restarts, previous=previous)


# Shapes follow packages/telemetry/src/curie_telemetry/logging.py::_JsonFormatter
# and uvicorn's default stderr formatter. Values are synthetic.
@pytest.mark.parametrize(
    ("log", "types", "records", "routes"),
    [
        (
            json.dumps(
                {
                    "severity": "ERROR",
                    "logger": "uvicorn.error",
                    "message": "Exception in ASGI application",
                    "trace_id": "private-trace",
                    "exception": "Traceback (most recent call last):\n"
                    "  File /private/workspace/app.py, line 1\n"
                    "sqlalchemy.exc.ProgrammingError: private SQL and parameters\n"
                    "(psycopg.errors.InsufficientPrivilege) permission denied",
                }
            ),
            ["psycopg.errors.InsufficientPrivilege", "sqlalchemy.exc.ProgrammingError"],
            [{"logger": "uvicorn.error", "severity": "ERROR"}],
            [],
        ),
        (
            "  + ExceptionGroup: private group (2 sub-exceptions)\n"
            "  +-+---------------- 1 ----------------\n"
            "    | ValueError: private value\n    +---------------- 2 ----------------\n"
            "    | RuntimeError: private value\n    +------------------------------------",
            ["ExceptionGroup", "RuntimeError", "ValueError"],
            [],
            [],
        ),
        (
            "ERROR:    Exception in ASGI application\nValueError: private value",
            ["ValueError"],
            [{"logger": "uvicorn.error", "severity": "ERROR"}],
            [],
        ),
        (
            json.dumps(
                {
                    "severity": "INFO",
                    "logger": "curie_api.main",
                    "message": "http request completed method=GET route=/agents status_class=5xx",
                }
            ),
            [],
            [],
            ["/agents"],
        ),
    ],
)
def test_recorded_shapes(log: str, types: list, records: list, routes: list) -> None:
    summary = parse(log)
    assert summary["api_exception_types"] == types
    assert summary["api_warning_records"] == records
    assert summary["api_5xx_routes"] == routes
    assert "private" not in json.dumps(summary)


def test_success_and_arbitrary_messages_do_not_become_metadata() -> None:
    log = "\n".join(
        [
            json.dumps(
                {
                    "severity": "INFO",
                    "logger": "curie_api.main",
                    "message": "http request completed method=GET route=/agents status_class=2xx",
                }
            ),
            (
                'INFO: 127.0.0.1:1234 - "GET /agents/private-id?secret=private HTTP/1.1" '
                "500 Internal Server Error"
            ),
            json.dumps(
                {
                    "severity": "WARNING",
                    "logger": "curie_api.main",
                    "message": "private value",
                    "trace_id": "private-trace",
                }
            ),
            json.dumps(
                {"severity": "CRITICAL", "logger": "curie_api.main", "message": "private value"}
            ),
            '{"message": "private malformed JSON',
            "[]",
            "null",
        ]
    )
    summary = parse(log)
    assert summary["api_5xx_routes"] == []
    assert summary["api_exception_types"] == []
    assert summary["api_http_500_lines"] == 1
    assert summary["api_warning_records"] == [
        {"logger": "curie_api.main", "severity": "CRITICAL"},
        {"logger": "curie_api.main", "severity": "WARNING"},
    ]
    assert "private" not in json.dumps(summary)


def test_previous_container_exception_types_only_after_restart() -> None:
    previous = "psycopg.OperationalError: private connection detail"
    summary = parse("", previous, 2)
    assert summary["api_restart_count"] == 2
    assert summary["api_previous_exception_types"] == ["psycopg.OperationalError"]
    assert parse("", previous)["api_previous_exception_types"] == []


def test_script_parse_step_on_recorded_files(tmp_path: Path) -> None:
    current, previous = tmp_path / "current.log", tmp_path / "previous.log"
    current.write_text(
        json.dumps(
            {
                "severity": "ERROR",
                "logger": "uvicorn.error",
                "message": "http request completed method=GET route=/agents status_class=5xx",
                "exception": "| sqlalchemy.exc.ProgrammingError: private SQL",
            }
        )
    )
    previous.write_text("| psycopg.OperationalError: private connection detail")
    result = subprocess.run(
        [
            sys.executable,
            str(PARSER),
            str(current),
            "--restart-count",
            "1",
            "--previous-log",
            str(previous),
        ],
        text=True,
        capture_output=True,
        check=True,
    )
    prefix = "Recovery API diagnostic: "
    assert result.stdout.startswith(prefix)
    summary = json.loads(result.stdout.removeprefix(prefix))
    assert summary["api_5xx_routes"] == ["/agents"]
    assert summary["api_exception_types"] == ["sqlalchemy.exc.ProgrammingError"]
    assert summary["api_previous_exception_types"] == ["psycopg.OperationalError"]
    assert summary["api_restart_count"] == 1
    assert "private" not in result.stdout
    assert result.stderr == ""


def test_malformed_fields_and_source_paths_are_not_exception_types() -> None:
    summary = parse(
        "\n".join(
            [
                json.dumps({"severity": [], "logger": {}, "message": 5, "exception": []}),
                '  File "/private/customer.py", line 123, in call',
                "/private/customer.py:123: private message",
                "customer.py",
            ]
        )
    )
    assert summary["api_warning_records"] == []
    assert summary["api_exception_types"] == []


def test_missing_required_log_is_an_error(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(PARSER), str(tmp_path / "missing"), "--restart-count", "0"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert result.stdout == ""
    assert "cannot read required API log" in result.stderr
