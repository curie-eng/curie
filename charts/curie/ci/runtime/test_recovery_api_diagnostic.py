"""Recovery metadata from curie_telemetry JSON and uvicorn stderr shapes."""

from __future__ import annotations

import importlib.util
import json
import os
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


@pytest.mark.parametrize(
    "log",
    [
        'INFO: 127.0.0.1:1234 - "GET /agents HTTP/1.1" 500 Internal Server Error',
        json.dumps(
            {
                "severity": "INFO",
                "logger": "uvicorn.access",
                "message": '127.0.0.1:1234 - "GET /agents?private=value HTTP/1.1" 503',
            }
        ),
    ],
)
def test_unhandled_agent_listing_5xx(log: str) -> None:
    summary = parse(log)
    assert summary["api_5xx_routes"] == ["/agents"]
    assert "private" not in json.dumps(summary)
    assert "127.0.0.1" not in json.dumps(summary)


def test_access_logs_never_promote_identifier_paths_to_templates() -> None:
    log = "\n".join(
        [
            'INFO: 127.0.0.1:1234 - "GET /agents/private-id HTTP/1.1" 500 Internal Server Error',
            'INFO: 127.0.0.1:1234 - "GET /agents HTTP/1.1" 200 OK',
            json.dumps(
                {
                    "severity": "INFO",
                    "logger": "curie_api.main",
                    "message": 'private message contains "GET /agents HTTP/1.1" 500',
                }
            ),
        ]
    )
    assert parse(log)["api_5xx_routes"] == []


@pytest.mark.parametrize(
    "text",
    [
        "user alice.Smith: denied for acme.Corp)",
        "/home/alice/Workspace.Customer: failed",
        "connect to db.Example failed (db.Prod)",
        "<class 'acme_corp.secret_name'>",
    ],
)
@pytest.mark.parametrize("field", [None, "message", "exception"])
def test_arbitrary_identifiers_never_become_exception_types(text: str, field: str | None) -> None:
    log = text if field is None else json.dumps({field: text})
    summary = parse(log, log, 1)
    assert summary["api_exception_types"] == []
    assert summary["api_previous_exception_types"] == []


def test_traceback_shaped_messages_are_not_exceptions() -> None:
    summary = parse(json.dumps({"message": "ValueError: private message"}))
    assert summary["api_exception_types"] == []


def test_type_cap_and_per_text_500_count() -> None:
    exceptions = "\n".join(f"Failure{i:02}Error: private" for i in range(15))
    log = json.dumps({"message": '" 500 twice " 500', "exception": exceptions + '\n" 500 " 500'})
    summary = parse(log, log, 1)
    expected = [f"Failure{i:02}Error" for i in range(12)]
    assert summary["api_exception_types"] == expected
    assert summary["api_previous_exception_types"] == expected
    assert summary["api_http_500_lines"] == 2


def test_known_class_prefix_and_unqualified_type() -> None:
    log = (
        "sqlalchemy.exc.ProgrammingError: <class 'asyncpg.exceptions.InsufficientPrivilegeError'>"
        "\n| <class 'ValueError'>"
    )
    assert parse(log)["api_exception_types"] == [
        "ValueError",
        "asyncpg.exceptions.InsufficientPrivilegeError",
        "sqlalchemy.exc.ProgrammingError",
    ]


KUBECTL_STUB = r"""
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
with Path(os.environ["CALL_LOG"]).open("a") as log:
    log.write(json.dumps(args) + "\n")
if "get" in args:
    if "pods" in args:
        if os.environ["POD_FAILURE"] == "1":
            raise SystemExit(1)
        print(os.environ["POD_NAME"])
    elif "pod" in args:
        if os.environ["COUNT_FAILURE"] == "1":
            raise SystemExit(1)
        print(os.environ["RESTART_COUNT"])
    else:
        raise SystemExit(90)
elif "logs" in args:
    if os.environ["UNREADABLE_LOG"] == "1" and "--previous" not in args:
        path = Path(os.environ["WORKDIR"]) / "recovery-api.log"
        path.unlink()
        path.mkdir()
    else:
        name = "PREVIOUS_LOG" if "--previous" in args else "CURRENT_LOG"
        print(Path(os.environ[name]).read_text())
else:
    raise SystemExit(91)
"""


def run_shell_parse_step(
    tmp_path: Path,
    restart_count: str = "0",
    *,
    pod_name: str = "acme-api",
    pod_failure: bool = False,
    count_failure: bool = False,
    unreadable_log: bool = False,
) -> tuple[subprocess.CompletedProcess[str], list[list[str]]]:
    tools = tmp_path / "bin"
    tools.mkdir()
    kubectl = tools / "kubectl"
    kubectl.write_text(f"#!{sys.executable}\n" + KUBECTL_STUB)
    kubectl.chmod(0o755)
    current, previous, calls = tmp_path / "current", tmp_path / "previous", tmp_path / "calls"
    current.write_text(json.dumps({"exception": "| ValueError: private value"}))
    previous.write_text(json.dumps({"exception": "| psycopg.OperationalError: private value"}))
    calls.touch()
    script = PARSER.with_name("workspace-dependency-diagnostic-runtime.sh").read_text()
    start = script.index("    # Project logs onto fixed metadata;")
    final_assertion = '[[ "$TURN_STATUS" == 0 ]] || fail "recovery CLI turn failed"'
    end = script.index(final_assertion, start) + len(final_assertion)
    # Execute the production shell block, retaining its original failure assertion.
    program = """set -euo pipefail
fail() { echo "FAIL: $*" >&2; exit 1; }
kc() { kubectl --context kind-acme-test -n test-acme "$@"; }
TURN_STATUS=1
if [[ "$TURN_STATUS" != 0 ]]; then
""" + script[start:end]
    environment = {
        "PATH": str(tools) + os.pathsep + os.environ["PATH"],
        "WORKDIR": str(tmp_path),
        "REPO_ROOT": str(PARSER.parents[4]),
        "RELEASE": "acme",
        "RECOVERY_SINCE": "2026-01-01T00:00:00Z",
        "CURRENT_LOG": str(current),
        "PREVIOUS_LOG": str(previous),
        "CALL_LOG": str(calls),
        "RESTART_COUNT": restart_count,
        "POD_NAME": pod_name,
        "POD_FAILURE": str(int(pod_failure)),
        "COUNT_FAILURE": str(int(count_failure)),
        "UNREADABLE_LOG": str(int(unreadable_log)),
    }
    completed = subprocess.run(
        ["bash", "-c", program],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    return completed, [json.loads(line) for line in calls.read_text().splitlines()]


@pytest.mark.parametrize("restarts", ["0", "2"])
def test_shell_collects_current_and_previous_container_metadata(
    tmp_path: Path, restarts: str
) -> None:
    completed, calls = run_shell_parse_step(tmp_path, restarts)
    assert completed.returncode == 1
    assert completed.stderr == "FAIL: recovery CLI turn failed\n"
    summary = json.loads(completed.stdout.removeprefix("Recovery API diagnostic: "))
    assert summary["api_restart_count"] == int(restarts)
    assert summary["api_exception_types"] == ["ValueError"]
    assert summary["api_previous_exception_types"] == (
        ["psycopg.OperationalError"] if restarts == "2" else []
    )
    assert calls[0] == [
        "--context",
        "kind-acme-test",
        "-n",
        "test-acme",
        "get",
        "pods",
        "-l",
        "app.kubernetes.io/instance=acme,app.kubernetes.io/component=api",
        "-o",
        "jsonpath={.items[0].metadata.name}",
    ]
    assert any("pod" in call and "acme-api" in call for call in calls)
    log_calls = [call for call in calls if "logs" in call]
    assert len(log_calls) == (2 if restarts == "2" else 1)
    assert any("--previous" in call for call in log_calls) == (restarts == "2")
    assert all("acme-api" in call and "-c" in call and "api" in call for call in log_calls)


@pytest.mark.parametrize(
    "settings",
    [
        {"pod_failure": True},
        {"pod_name": ""},
        {"restart_count": ""},
        {"restart_count": "not-an-integer"},
        {"count_failure": True},
        {"unreadable_log": True},
    ],
)
def test_shell_diagnostic_failure_preserves_recovery_failure(
    tmp_path: Path, settings: dict
) -> None:
    completed, calls = run_shell_parse_step(tmp_path, **settings)
    assert completed.returncode == 1
    assert completed.stderr.endswith("FAIL: recovery CLI turn failed\n")
    assert not any("--previous" in call for call in calls)
    if not settings.get("unreadable_log"):
        summary = json.loads(completed.stdout.removeprefix("Recovery API diagnostic: "))
        assert summary["api_restart_count"] == 0


def test_traceback_preserves_application_exception_types() -> None:
    assert parse("| curie_api.errors.InternalError: private value")["api_exception_types"] == [
        "curie_api.errors.InternalError"
    ]
