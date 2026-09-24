"""Source contracts for the local parity ladder's OTLP evidence (#1817/#1818).

The expensive proof remains ``CURIE_E2E_TIERS=local curie dev e2e-ladder``.
These fast tests keep that proof from silently collapsing back to "the turn
finished": the local rung must own a real OTLP Collector sink, query its three
exported signal files, and run both healthy and injected-failure controls.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
LADDER_PATH = REPO_ROOT / "cli" / "scripts" / "e2e-ladder.sh"
SINK_CONFIG_PATH = REPO_ROOT / "cli" / "scripts" / "fixtures" / "otel-e2e-sink-config.yaml"
MCP_RECEIPT_FIXTURE = REPO_ROOT / "cli" / "scripts" / "fixtures" / "mcp-receipt"


def _function_body(source: str, name: str, next_marker: str) -> str:
    start_marker = f"{name}() {{"
    assert start_marker in source, f"{LADDER_PATH}: missing {name}"
    start = source.index(start_marker)
    end = source.index(next_marker, start)
    return source[start:end]


def _shell_function(source: str, name: str) -> str:
    """Return one top-level shell function without depending on its successor."""

    start_marker = f"{name}() {{"
    assert start_marker in source, f"{LADDER_PATH}: missing {name}"
    start = source.index(start_marker)
    end = source.index("\n}\n", start) + len("\n}\n")
    return source[start:end]


def _python_heredoc(function: str) -> str:
    """Extract the sole quoted Python heredoc from a shell function."""

    start = function.index("<<'PY'\n") + len("<<'PY'\n")
    end = function.index("\nPY\n", start)
    return function[start:end]


def _run_seed_matcher(
    tmp_path: Path, matcher: str, marker: str, rows: list[object]
) -> subprocess.CompletedProcess[str]:
    private_slice = tmp_path / "bounded-stream.json"
    private_slice.write_text(json.dumps(rows))
    return subprocess.run(
        [sys.executable, "-", str(private_slice), marker],
        input=matcher,
        text=True,
        capture_output=True,
        check=False,
    )


def _run_absent_trace_query(
    tmp_path: Path, query: str, *, error: str
) -> tuple[subprocess.CompletedProcess[str], int]:
    """Run the real shell helper against a deterministic failing candidate CLI."""

    count = tmp_path / "query-count"
    fake_bin = tmp_path / "curie"
    fake_bin.write_text(
        "#!/bin/sh\n"
        'count="$(cat "$FAKE_COUNT" 2>/dev/null || printf 0)"\n'
        'count=$((count + 1))\n'
        'printf "%s" "$count" > "$FAKE_COUNT"\n'
        'printf \'{"error":"%s","fix":"retry exact id"}\\n\' "$FAKE_ERROR"\n'
        "exit 1\n"
    )
    fake_bin.chmod(0o700)
    script = f"""set -u
sanitize_exact_trace_read() {{ return 1; }}
{query}
WORKDIR="$1"
BIN="$2"
OBSERVABILITY_POLL_ATTEMPTS=3
OBSERVABILITY_POLL_INTERVAL_SECONDS=0
CURIE_NAMESPACE=acme-observability
CURIE_RELEASE=acme-observability
query_exact_seed_trace local aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa "" "" absent
"""
    result = subprocess.run(
        ["bash", "-c", script, "bash", str(tmp_path), str(fake_bin)],
        env={**os.environ, "FAKE_COUNT": str(count), "FAKE_ERROR": error},
        text=True,
        capture_output=True,
        check=False,
    )
    return result, int(count.read_text()) if count.exists() else 0


def _run_product_classifier(
    tmp_path: Path,
    *,
    local: dict[str, object] | None,
    cluster: dict[str, object] | None,
) -> subprocess.CompletedProcess[str]:
    """Execute the production ownership classifier against explicit evidence."""

    function = _shell_function(
        LADDER_PATH.read_text(), "classify_product_observability_owner"
    )
    local_path = tmp_path / "local.json"
    cluster_path = tmp_path / "cluster.json"
    if local is not None:
        local_path.write_text(json.dumps({**local, "surface": "local"}))
    if cluster is not None:
        cluster_path.write_text(json.dumps({**cluster, "surface": "cluster"}))
    script = f"""set -u
{function}
LOCAL_PRODUCT_EVIDENCE="$1"
CLUSTER_PRODUCT_EVIDENCE="$2"
classify_product_observability_owner
"""
    return subprocess.run(
        ["bash", "-c", script, "bash", str(local_path), str(cluster_path)],
        text=True,
        capture_output=True,
        check=False,
    )


def _run_local_exact_positive(
    tmp_path: Path,
    *,
    operations: tuple[str, ...],
    expected_state: str = "present",
) -> subprocess.CompletedProcess[str]:
    """Drive the production local exact-read positive against a candidate CLI."""

    source = LADDER_PATH.read_text()
    sanitizer = _shell_function(source, "sanitize_exact_trace_read")
    query = _shell_function(source, "query_exact_seed_trace")
    trace_id = "a" * 32
    response = tmp_path / "response.json"
    response.write_text(
        json.dumps(
            {
                "trace": {"id": trace_id},
                "tree": [
                    {"name": operation, "type": "SPAN", "children": []}
                    for operation in operations
                ],
                "approval_decision": None,
            }
        )
    )
    fake_bin = tmp_path / "curie"
    fake_bin.write_text('#!/bin/sh\ncat "$FAKE_TRACE_RESPONSE"\n')
    fake_bin.chmod(0o700)
    script = f"""set -u
{sanitizer}
{query}
WORKDIR="$1"
BIN="$2"
OBSERVABILITY_POLL_ATTEMPTS=1
OBSERVABILITY_POLL_INTERVAL_SECONDS=0
CURIE_NAMESPACE=acme-observability
CURIE_RELEASE=acme-observability
query_exact_seed_trace local {trace_id} "curie.queue.enqueue,curie.reply.post" "" "$3"
"""
    return subprocess.run(
        [
            "bash",
            "-c",
            script,
            "bash",
            str(tmp_path),
            str(fake_bin),
            expected_state,
        ],
        env={**os.environ, "FAKE_TRACE_RESPONSE": str(response)},
        text=True,
        capture_output=True,
        check=False,
    )


def test_local_ladder_owns_a_real_queryable_otlp_sink() -> None:
    """A plain local rung must create, query, and remove its own OTLP sink."""

    source = LADDER_PATH.read_text()
    rung = _function_body(source, "rung_local", "# local-release mode:")
    trap = _function_body(source, "cleanup", "trap cleanup EXIT")

    required_calls = (
        "start_local_otel_sink",
        "assert_local_otel_healthy_turn",
        "case_local_otel_runner_failure",
    )
    missing = [call for call in required_calls if call not in rung]
    assert not missing, (
        "the local rung can pass without querying the three OTLP signals and "
        f"its healthy/failure controls; missing calls: {missing}"
    )
    assert "stop_local_otel_sink" in trap, (
        "the global trap must reap the disposable sink even when the rung fails"
    )

    # The sink has to exist before `local up`, because service startup telemetry
    # and the ingress root are part of the proof. The query belongs after the
    # real turn finalized, not beside static compose/config assertions.
    local_up = "local_compose_cli_args local up"
    assert local_up in rung, "the local rung must build the current compose source"
    assert "--build" in rung, "the local rung must still pass --build"
    assert rung.index("start_local_otel_sink") < rung.index(local_up)
    assert rung.index("assert_local_otel_healthy_turn") > rung.index(
        'assert_finalized_reply "local"'
    )

    assert SINK_CONFIG_PATH.is_file(), (
        "the local proof must use a checked-in Collector sink config rather "
        "than infer export from the application Collector's configuration"
    )
    sink = yaml.safe_load(SINK_CONFIG_PATH.read_text())
    assert "otlp" in sink["receivers"]
    pipelines = sink["service"]["pipelines"]
    for signal in ("traces", "logs", "metrics"):
        assert pipelines[signal]["receivers"] == ["otlp"]
        exporters = pipelines[signal]["exporters"]
        assert any(name.startswith("file/") for name in exporters), (
            f"the {signal} sink pipeline must retain queryable records, got {exporters}"
        )


def _start_sink_with_stub_docker(
    tmp_path: Path, *, first_run_error: str | None
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    """Run the real start_local_otel_sink against a recording docker stub."""

    function = _shell_function(LADDER_PATH.read_text(), "start_local_otel_sink")
    stubs = tmp_path / "bin"
    stubs.mkdir()
    runs = tmp_path / "docker-runs"
    docker = stubs / "docker"
    docker.write_text(
        """#!/bin/bash
case "$1 $2" in
    "ps -q"|"rm -f"|"logs "*) exit 0 ;;
    "network inspect")
        [[ "${4:-}" == "--format" ]] && echo 172.30.0.1
        exit 0
        ;;
    "run -d")
        printf '%s\\n' "$*" >> "$STUB_RUNS"
        host_port="$(printf '%s\\n' "$*" \\
            | sed -nE 's/.*-p 0\\.0\\.0\\.0:([0-9]*):4318.*/\\1/p')"
        # The first port tried stays taken, as a port another container
        # holds would, so only a fresh choice per attempt can start.
        if [[ -n "$STUB_FIRST_RUN_ERROR" ]]; then
            [[ -s "$STUB_TAKEN" ]] || printf '%s' "$host_port" > "$STUB_TAKEN"
            if [[ "$host_port" == "$(cat "$STUB_TAKEN")" ]]; then
                echo "docker: Error response from daemon: $STUB_FIRST_RUN_ERROR" >&2
                exit 125
            fi
        fi
        echo stub-sink-id
        exit 0
        ;;
    "port "*)
        case "$3" in
            4318/tcp)
                host_port="$(tail -n 1 "$STUB_RUNS" \\
                    | sed -nE 's/.*-p 0\\.0\\.0\\.0:([0-9]*):4318.*/\\1/p')"
                echo "0.0.0.0:$host_port"
                ;;
            13133/tcp) echo 127.0.0.1:41001 ;;
            8888/tcp) echo 127.0.0.1:41002 ;;
        esac
        exit 0
        ;;
esac
echo "unexpected docker $*" >&2
exit 97
"""
    )
    curl = stubs / "curl"
    curl.write_text("#!/bin/sh\nexit 0\n")
    for stub in (docker, curl):
        stub.chmod(0o700)
    script = f"""set -euo pipefail
unset STUB_STATE
REPO_ROOT="$1"
WORKDIR="$2"
COMPOSE_PROJECT=curie
LOCAL_OTEL_SINK_NAME=curie-ladder-otel-sink-test
LOCAL_OTEL_SINK_OWNED=0
LOCAL_OTEL_NETWORK_OWNED=0
LOCAL_OTEL_SINK_ACTIVE=0
LOCAL_OTEL_ENDPOINT=""
LOCAL_OTEL_METRICS_ENDPOINT=""
assert_local_otel_zero_export_control() {{ :; }}
{function}
start_local_otel_sink
printf 'endpoint=%s\\n' "$LOCAL_OTEL_ENDPOINT"
printf 'bridge=%s\\n' "$OTEL_EXPORTER_OTLP_ENDPOINT"
printf 'worker=%s\\n' "$CURIE_WORKER_OTEL_EXPORTER_OTLP_ENDPOINT"
"""
    result = subprocess.run(
        ["bash", "-c", script, "bash", str(REPO_ROOT), str(tmp_path)],
        env={
            **os.environ,
            "PATH": f"{stubs}{os.pathsep}{os.environ['PATH']}",
            "STUB_RUNS": str(runs),
            "STUB_TAKEN": str(tmp_path / "taken-port"),
            "STUB_FIRST_RUN_ERROR": first_run_error or "",
        },
        text=True,
        capture_output=True,
        check=False,
    )
    return result, runs.read_text().splitlines() if runs.exists() else []


def _published_otlp_host_port(run: str) -> str:
    published = [
        spec for spec in re.findall(r"-p (\S+)", run) if spec.endswith(":4318")
    ]
    assert len(published) == 1, f"the sink must publish OTLP once: {run}"
    return published[0].split(":")[1]


def test_local_sink_names_its_otlp_host_port(tmp_path: Path) -> None:
    """Every stack container dials the sink at gateway:host-port.

    Docker Desktop refuses a host port Docker allocated (``-p ip::4318``) from
    inside its VM, at 127.0.0.1 and at every bridge gateway alike, while the
    Mac reaches it; a host port the caller names is reachable from both
    bridge and host-network containers there, as on Linux.
    """

    result, runs = _start_sink_with_stub_docker(tmp_path, first_run_error=None)
    assert len(runs) == 1, result.stderr
    host_port = _published_otlp_host_port(runs[0])
    assert host_port.isdigit() and 0 < int(host_port) < 65536, (
        f"the sink's OTLP host port must be chosen, not Docker-allocated: {runs[0]}"
    )
    assert result.returncode == 0, result.stderr
    endpoint = f"http://172.30.0.1:{host_port}"
    assert f"endpoint={endpoint}" in result.stdout
    assert f"bridge={endpoint}" in result.stdout
    assert f"worker={endpoint}" in result.stdout


@pytest.mark.parametrize(
    "collision",
    [
        "Bind for 0.0.0.0:41999 failed: port is already allocated",
        "ports are not available: exposing port TCP 0.0.0.0:41999 -> "
        "127.0.0.1:0: listen tcp4 0.0.0.0:41999: bind: address already in use",
    ],
)
def test_local_sink_retries_a_chosen_port_that_is_taken(
    tmp_path: Path, collision: str
) -> None:
    """A chosen port can be taken by a container or by a host process."""

    result, runs = _start_sink_with_stub_docker(tmp_path, first_run_error=collision)
    assert len(runs) >= 2, f"a taken port must be retried, not fatal: {result.stderr}"
    taken = _published_otlp_host_port(runs[0])
    host_port = _published_otlp_host_port(runs[-1])
    assert host_port.isdigit() and host_port != taken, (
        f"each attempt must choose its port afresh, not retry {taken}: {runs}"
    )
    assert result.returncode == 0, result.stderr
    assert f"endpoint=http://172.30.0.1:{host_port}" in result.stdout


def test_local_sink_assertion_proves_causality_correlation_and_bounded_metrics() -> None:
    """Pin the observable claims, not merely the sink container's presence."""

    source = LADDER_PATH.read_text()
    healthy_turn = _function_body(
        source,
        "assert_local_otel_healthy_turn",
        "case_local_otel_runner_failure()",
    )
    query = _function_body(
        source,
        "local_otel_query",
        "assert_bounded_metric_attributes()",
    )

    # `local message` delegates its one-shot producer to the real dispatcher,
    # then crosses Valkey through worker and runner before completing the reply.
    # Do not let this lapse into an API-ingress-only proof: the qualifying trace
    # has to include the dispatcher, worker, runner, and reply completion; the
    # same post-baseline observation also requires API telemetry from the live
    # health/readiness path.
    causal_spans = {
        "curie.queue.enqueue",
        "curie.queue.process",
        "curie.turn.process",
        "curie.sandbox.claim",
        "curie.runner.rpc",
        "agent.run",
        "curie.reply.post",
    }
    missing_spans = sorted(span for span in causal_spans if span not in query)
    assert not missing_spans, f"local trace assertion omits spans: {missing_spans}"

    for service in ("curie-dispatcher", "curie-worker", "curie-runner"):
        assert service in query, (
            "the qualifying causal trace must retain the "
            f"{service} service boundary"
        )
    assert "has_reply" in query, (
        "the qualifying causal trace must complete a reply, not stop at runner execution"
    )

    for field in ("traceId", "spanId", "service.name"):
        assert field in query, f"OTLP logs are not proved correlated without checking {field}"
    for service in ("curie-api", "curie-dispatcher", "curie-worker", "curie-runner"):
        assert f'"{service}"' in query, (
            f"the post-turn platform proof must observe fresh telemetry from {service}"
        )
    assert "platform_services = {\"curie-api\"" in query
    assert "exercised == platform_services" in query, (
        "the healthy proof must require fresh spans from every platform service"
    )

    success_metrics = {
        "curie.turn.accepted",
        "curie.turn.completed",
        "curie.turn.duration",
        "curie.queue.message.age",
        "curie.sandbox.lifecycle",
        "curie.runner.rpc.result",
    }
    missing_metrics = sorted(name for name in success_metrics if name not in query)
    assert not missing_metrics, (
        f"local success flow omits required operational metric points: {missing_metrics}"
    )

    # Event/session/sandbox/user identifiers are allowed on traces and logs but
    # never on metric points. The runtime manifest test owns the complete schema;
    # this guards the independent E2E query from accidentally accepting them.
    assert "assert_bounded_metric_attributes" in healthy_turn
    for forbidden in (
        "event.id",
        "session.id",
        "sandbox.id",
        "user.id",
        "trace_id",
        "span_id",
    ):
        assert forbidden in query, (
            f"the sink query never rejects high-cardinality metric key {forbidden!r}"
        )

    # The prompt and a credential-shaped sentinel exercise the negative privacy
    # side of the same collected payload. A sink query that only finds positive
    # names cannot prove sensitive values stayed out.
    assert "assert_local_otel_redacted" in healthy_turn
    assert "OTEL_E2E_SECRET_SENTINEL" in source


def test_local_runner_failure_is_falsifiable_against_the_healthy_control() -> None:
    """A failing command is evidence only when its telemetry differs from healthy."""

    source = LADDER_PATH.read_text()
    failure = _function_body(
        source,
        "case_local_otel_runner_failure",
        "# Rung 1:",
    )
    failure += _function_body(
        source,
        "local_otel_query",
        "assert_bounded_metric_attributes()",
    )

    for required in (
        "inject_local_runner_failure",
        "assert_local_otel_failed_turn",
        "restore_local_runner_health",
        "ERROR",
        "classified_failure",
        "done_delta",
        "independently of prompt",
    ):
        assert required in failure, (
            f"the local failure control does not prove {required!r} against healthy"
        )
    assert "assert_local_otel_healthy_turn" in failure, (
        "the injected failure must restore the runner and observe a subsequent "
        "healthy control, or a permanently broken path could satisfy the negative"
    )

    failed_assertion = failure.index('assert_local_otel_failed_turn "$failure_before"')
    restored = failure.index("\n    restore_local_runner_health\n")
    assert failed_assertion < restored, (
        "the failure's trace/log/metric evidence must be exported before runner "
        "restoration can discard it"
    )
    assert "same traceId" in failure, (
        "the failed turn must pair its ERROR log with the failed trace"
    )


def test_injected_runner_failure_does_not_depend_on_prompt_or_live_model() -> None:
    """#2260: live models answer 'runner failure control'; the inject must force the fault."""

    source = LADDER_PATH.read_text()
    inject = _shell_function(source, "inject_local_runner_failure")
    restore = _shell_function(source, "restore_local_runner_health")
    reap = _shell_function(source, "reap_local_runner_sandboxes")
    case = _function_body(source, "case_local_otel_runner_failure", "# Rung 1:")

    assert "runner failure control" not in inject, (
        "the inject helper must not treat prompt text as the fault"
    )
    assert "$LOCAL_OTEL_ENDPOINT" not in inject, (
        "the OTel sink is a reachable HTTP server; pointing the model at it is "
        "not an unreachable backend and a live credential can still succeed"
    )
    assert "http://127.0.0.1:1" in inject, (
        "the injected backend must be connection-refused on the runner's loopback, "
        "independent of CURIE_E2E_LIVE and of prompt text"
    )
    assert "export CURIE_FAKE_MODEL=0" in inject
    for key in ("CURIE_CREDENTIALS", "ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN"):
        assert f'export {key}=""' in inject, (
            f"{key} must be blanked so a live OpenRouter/Anthropic credential cannot "
            "skip the unreachable base-URL override"
        )
        assert key in restore, f"restore must put {key} back for the healthy control"

    assert "SANDBOX_LABEL" in reap
    assert "docker rm -f" in reap
    assert "LOCAL_OTEL_ENDPOINT" in reap
    assert "OTEL_EXPORTER_OTLP_ENDPOINT" in reap
    assert "reap_local_runner_sandboxes" in inject
    assert "reap_local_runner_sandboxes" in restore
    assert case.index("inject_local_runner_failure") < case.index(
        "runner failure control"
    ), "the message is only a marker; inject must run first"


def _stub_docker_path(tmp_path: Path) -> tuple[Path, Path, Path]:
    """A PATH prefix whose docker/sleep record compose env and ignore sleeps."""

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "docker.log"
    env_dir = tmp_path / "compose-env"
    env_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text(
        "#!/bin/sh\n"
        f'printf "%s\\n" "$*" >> "{log}"\n'
        'if [ "$1" = "ps" ]; then printf "fake-sandbox\\n"; exit 0; fi\n'
        'if [ "$1" = "inspect" ]; then\n'
        '  printf "OTEL_EXPORTER_OTLP_ENDPOINT=http://otel.example:4318\\n"\n'
        "  exit 0\n"
        "fi\n"
        'if [ "$1" = "compose" ]; then\n'
        f'  n=0; [ -f "{env_dir}/n" ] && n="$(cat "{env_dir}/n")"\n'
        f'  printf "%s" "$((n + 1))" > "{env_dir}/n"\n'
        "  python3 -c '\n"
        "import json, os, sys\n"
        "keys = [\n"
        '    "CURIE_FAKE_MODEL", "CURIE_MODEL_BASE_URL", "CURIE_MODEL_API_BACKEND",\n'
        '    "CURIE_CREDENTIALS", "ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN",\n'
        "]\n"
        "json.dump({k: os.environ.get(k) for k in keys}, open(sys.argv[1], \"w\"))\n"
        f"' \"{env_dir}/$n.json\"\n"
        "fi\n"
        "exit 0\n"
    )
    docker.chmod(0o700)
    sleep = bin_dir / "sleep"
    sleep.write_text("#!/bin/sh\nexit 0\n")
    sleep.chmod(0o700)
    return bin_dir, log, env_dir


def test_inject_local_runner_failure_blanks_live_credentials_and_reaps(
    tmp_path: Path,
) -> None:
    """Execute the real inject/restore helpers against a stub docker."""

    source = LADDER_PATH.read_text()
    script = _shell_function(source, "ladder_compose")
    script += _shell_function(source, "container_env_value")
    script += _shell_function(source, "reap_local_runner_sandboxes")
    script += _shell_function(source, "inject_local_runner_failure")
    script += _shell_function(source, "restore_local_runner_health")
    script += """
REPO_ROOT="$1"
COMPOSE_PROJECT=curie
COMPOSE_FILES=("$REPO_ROOT/compose.dev.yaml")
SANDBOX_LABEL="curietech.ai/managed-by=curie-sandbox-substrate"
LOCAL_OTEL_ENDPOINT="http://otel.example:4318"
LIVE=1
inject_local_runner_failure
restore_local_runner_health
"""
    bin_dir, log, env_dir = _stub_docker_path(tmp_path)
    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ.get('PATH', '')}",
        "CURIE_FAKE_MODEL": "0",
        "CURIE_CREDENTIALS": "sk-or-example",
        "ANTHROPIC_API_KEY": "sk-ant-example",
        "CLAUDE_CODE_OAUTH_TOKEN": "sk-ant-oat-example",
        "LOCAL_OTEL_ENDPOINT": "http://otel.example:4318",
    }
    result = subprocess.run(
        ["bash", "-c", script, "bash", str(REPO_ROOT)],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    env_files = sorted(env_dir.glob("*.json"), key=lambda p: int(p.stem))
    assert len(env_files) >= 2, f"expected inject and restore compose calls, got {env_files}"
    injected = json.loads(env_files[0].read_text())
    restored = json.loads(env_files[-1].read_text())
    assert injected["CURIE_FAKE_MODEL"] == "0"
    assert injected["CURIE_MODEL_BASE_URL"] == "http://127.0.0.1:1"
    assert injected["CURIE_MODEL_API_BACKEND"] == "messages"
    assert injected["CURIE_CREDENTIALS"] == ""
    assert injected["ANTHROPIC_API_KEY"] == ""
    assert injected["CLAUDE_CODE_OAUTH_TOKEN"] == ""
    docker_log = log.read_text()
    assert "label=curietech.ai/managed-by=curie-sandbox-substrate" in docker_log
    assert "rm -f" in docker_log
    assert restored["CURIE_FAKE_MODEL"] == "0"
    assert restored["CURIE_MODEL_BASE_URL"] in (None, "")
    assert restored["CURIE_CREDENTIALS"] == "sk-or-example"
    assert restored["ANTHROPIC_API_KEY"] == "sk-ant-example"
    assert restored["CLAUDE_CODE_OAUTH_TOKEN"] == "sk-ant-oat-example"


def test_local_otel_controls_distinguish_a_live_sink_from_a_turn() -> None:
    """Keep the no-turn and failure baselines falsifiable."""

    source = LADDER_PATH.read_text()
    zero_export = _function_body(
        source,
        "assert_local_otel_zero_export_control",
        "assert_local_otel_no_turn_pipeline_live()",
    )
    no_turn = _function_body(
        source,
        "assert_local_otel_no_turn_pipeline_live",
        "assert_local_otel_redacted()",
    )
    query = _function_body(
        source,
        "local_otel_query",
        "assert_bounded_metric_attributes()",
    )
    for metric in (
        "otelcol_receiver_accepted_metric_points",
        "otelcol_exporter_sent_metric_points",
    ):
        assert metric in zero_export and metric in no_turn
    assert "local_otel_query no-turn" in no_turn
    assert "curie.turn.accepted" in query
    assert "curie.turn.completed" in query

    failure = _function_body(source, "case_local_otel_runner_failure", "# Rung 1:")
    settle = failure.index("wait_for_local_otel_metric_settle")
    baseline = failure.index('local_otel_write_snapshot "$failure_before"')
    assert settle < baseline, (
        "a prior healthy metric export must settle before the failure baseline"
    )
    helper = _function_body(
        source,
        "wait_for_local_otel_metric_settle",
        "local_otel_self_metric_value()",
    )
    assert "sleep 12" in helper, (
        "the failure baseline must outwait the 10-second metric export interval"
    )


def test_product_oracle_discovers_only_the_seed_trace_from_bounded_transport(
    tmp_path: Path,
) -> None:
    """A background/newest trace must be unable to satisfy the turn oracle."""

    source = LADDER_PATH.read_text()
    # The unavailable-API negative may still exercise the public runs verb, but
    # no helper may select a turn from that list or dump a selected row. Inspect
    # whole function bodies so `--limit "$limit"` cannot evade this contract.
    functions = {
        match.group(1): _shell_function(source, match.group(1))
        for match in re.finditer(r"(?m)^([A-Za-z0-9_]+)\(\) \{$", source)
    }
    forbidden_selectors = {
        name: body
        for name, body in functions.items()
        if "local observability runs" in body
        and name != "prove_local_observability_queries"
    }
    assert not forbidden_selectors, (
        "a newest-runs selector/raw-dump helper can choose an unrelated background "
        f"trace regardless of limit interpolation: {sorted(forbidden_selectors)}"
    )
    for removed_helper in (
        "discover_local_observability_trace",
        "assert_local_observability_detail",
    ):
        assert removed_helper not in functions, (
            f"obsolete raw selector/detail helper remains: {removed_helper}"
        )

    discover = _shell_function(source, "discover_trace_id_for_seed")
    for required in (
        "stream_start",
        "stream_end",
        "marker",
        "XRANGE",
        "traceparent",
        "[0-9a-f]{32}",
        "umask 077",
    ):
        assert required in discover, (
            "exact discovery must inspect only the marker's bounded stream slice, "
            f"validate its adjacent W3C carrier, and keep raw fields private; missing {required!r}"
        )
    assert "printf '%s\\n' \"$payload\"" not in discover
    assert "printf '%s\\n' \"$traceparent\"" not in discover

    matcher = _python_heredoc(discover)
    marker = "curie-seed-ordinary-example"
    target_trace = "a" * 32
    background_trace = "b" * 32
    background = [
        "1-0",
        [
            "payload",
            json.dumps({"text": "ordinary correlation another-seed"}),
            "traceparent",
            f"00-{background_trace}-{'1' * 16}-01",
        ],
    ]
    target = [
        "2-0",
        [
            "payload",
            json.dumps({"text": f"ordinary correlation {marker}"}),
            "traceparent",
            f"00-{target_trace}-{'2' * 16}-01",
        ],
    ]
    matched = _run_seed_matcher(tmp_path, matcher, marker, [background, target])
    assert matched.returncode == 0, matched.stderr
    assert matched.stdout.strip() == target_trace, (
        "the real matcher must recover the carrier adjacent to a marker embedded "
        "in representative QueuedTurn.text, never the background carrier"
    )

    mismatch = _run_seed_matcher(tmp_path, matcher, marker, [background])
    assert mismatch.returncode != 0, (
        "a background entry without the marker must not satisfy exact discovery"
    )
    duplicate = _run_seed_matcher(
        tmp_path,
        matcher,
        marker,
        [
            target,
            [
                "3-0",
                [
                    "payload",
                    json.dumps({"text": f"ordinary correlation {marker}"}),
                    "traceparent",
                    f"00-{'c' * 32}-{'3' * 16}-01",
                ],
            ],
        ],
    )
    assert duplicate.returncode != 0, (
        "more than one matching adjacent carrier must fail closed"
    )

    query = _shell_function(source, "query_exact_seed_trace")
    assert 'observability run "$trace_id"' in query
    assert "sanitize_exact_trace_read" in query
    assert "observability runs" not in query
    assert "printf '%s\\n' \"$out\"" not in query, (
        "raw Langfuse JSON may contain prompt, user, session, and deployment fields"
    )
    sanitizer = _shell_function(source, "sanitize_exact_trace_read")
    for allowed in (
        "trace_id",
        "service",
        "operation",
        "observation_count",
        "observation_type",
        # Langfuse renames a tool observation to the tool, so the tool identity is
        # the only evidence that a specific tool ran. A built-in or connector tool
        # name is not caller data.
        "tool_name",
        "approval_decision",
    ):
        assert allowed in sanitizer, f"sanitized evidence omits safe field {allowed!r}"
    for private in ("input", "output", "session", "user", "headers"):
        assert private in sanitizer, (
            f"sanitizer must explicitly prevent raw {private!r} from reaching stdout"
        )
    assert "if private_fields.intersection(node):\n        pass" not in sanitizer, (
        "private-field handling must reject or remove data, not be a no-op"
    )


def test_product_evidence_sanitizer_and_failure_paths_never_dump_private_json(
    tmp_path: Path,
) -> None:
    """Raw replies and private Langfuse fields stay in task-owned artifacts."""

    source = LADDER_PATH.read_text()
    sanitizer = _shell_function(source, "sanitize_exact_trace_read")
    sanitizer_python = _python_heredoc(sanitizer)
    trace_id = "d" * 32
    private_sentinels = {
        "input": "PRIVATE_PROMPT_SENTINEL",
        "output": "PRIVATE_REPLY_SENTINEL",
        "session": "PRIVATE_SESSION_SENTINEL",
        "user": "PRIVATE_USER_SENTINEL",
        "headers": {"authorization": "PRIVATE_HEADER_SENTINEL"},
    }
    raw = tmp_path / "raw-trace.json"
    raw.write_text(
        json.dumps(
            {
                "trace": {"id": trace_id},
                "tree": [
                    {
                        "name": "curie.queue.enqueue",
                        "type": "SPAN",
                        "children": [],
                        **private_sentinels,
                    }
                ],
                "approval_decision": None,
            }
        )
    )
    result = subprocess.run(
        [sys.executable, "-", trace_id, str(raw), "curie.queue.enqueue", ""],
        input=sanitizer_python,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    evidence = json.loads(result.stdout)
    assert set(evidence) == {
        "trace_id",
        "service",
        "operation",
        "observation_count",
        "observation_type",
        "tool_name",
        "approval_decision",
    }
    combined_output = result.stdout + result.stderr
    for private_value in (
        "PRIVATE_PROMPT_SENTINEL",
        "PRIVATE_REPLY_SENTINEL",
        "PRIVATE_SESSION_SENTINEL",
        "PRIVATE_USER_SENTINEL",
        "PRIVATE_HEADER_SENTINEL",
    ):
        assert private_value not in combined_output

    finalized = _shell_function(source, "assert_finalized_reply")
    for raw_dump in (
        'printf \'%s\\n\' "$payload"',
        'printf "%s\\n" "$payload"',
        'echo "$payload"',
    ):
        assert raw_dump not in finalized, (
            "message parse/finalization failures must report a bounded verdict, "
            "not the private message JSON"
        )


def test_ordinary_mcp_and_approval_seeds_have_independent_receipts() -> None:
    """An unexercised seed is seed-invalid before telemetry is interpreted."""

    source = LADDER_PATH.read_text()
    for name, required in {
        "seed_ordinary_turn": ("assert_finalized_reply", "ordinary", "seed-invalid"),
        "seed_mcp_read_turn": ("mcp_receipt_call_count", "seed-invalid"),
        "seed_approval_resume_turn": (
            "awaiting-approval",
            "pending",
            "resolve",
            "assert_finalized_reply",
            "seed-invalid",
        ),
    }.items():
        body = _shell_function(source, name)
        missing = [marker for marker in required if marker not in body]
        assert not missing, f"{name} lacks independent outcome evidence: {missing}"
        assert "discover_trace_id_for_seed" in body
        assert "query_exact_seed_trace" in body

    approval = _shell_function(source, "seed_approval_resume_turn")
    assert "curie.approval.suspend" in approval, (
        "the approval oracle must require the worker's real parked-turn span"
    )
    assert "curie.approval.wait" not in approval, (
        "the oracle must not invent a wait span that no current emitter produces"
    )

    assert "seed-invalid" in source
    assert MCP_RECEIPT_FIXTURE.is_dir(), (
        "the MCP observation needs a hosted fixture whose container log is an "
        "independent tools/call receipt ledger"
    )
    server = MCP_RECEIPT_FIXTURE / "server.py"
    assert server.is_file()
    fixture_source = server.read_text()
    assert '"tools/call"' in fixture_source
    assert "print(" in fixture_source
    assert "flush=True" in fixture_source

    receipt = _shell_function(source, "mcp_receipt_call_count")
    assert "connector_container_for_alias" in receipt
    assert "docker logs" in receipt or "kubectl logs" in receipt
    assert "wc -l" in receipt or "count" in receipt
    for forbidden in ("tool input", "arguments", "receipt line"):
        assert forbidden not in receipt.lower(), (
            "public evidence may expose only an aggregate MCP call count"
        )


def test_approval_resume_failure_evidence_is_bounded_and_private_artifacts_are_cleaned(
    tmp_path: Path,
) -> None:
    """A failed background approval turn reports only a safe terminal verdict."""

    source = LADDER_PATH.read_text()
    approval = _shell_function(source, "seed_approval_resume_turn")
    summary = _shell_function(source, "approval_resume_failure_summary")

    assert 'message_stderr_file="$(mktemp "$WORKDIR/approval-message-stderr.XXXXXX")"' in approval
    assert '2> "$message_stderr_file" &' in approval
    assert (
        'approval_resume_failure_summary "$message_file" "$message_stderr_file" "$code" >&2'
        in approval
    )
    for cleanup in (
        'rm -f "$message_file" "$message_stderr_file" "$token_file" "$pending_file"',
        'rm -f "$message_file" "$message_stderr_file" "$pending_file"',
        'rm -f "$message_file" "$message_stderr_file"',
    ):
        assert cleanup in approval, (
            "every post-allocation approval failure path must remove the private "
            "stdout and stderr artifacts"
        )

    private_stdout = tmp_path / "approval-message"
    private_stderr = tmp_path / "approval-message-stderr"
    private_stdout.write_text(
        json.dumps(
            {
                "awaiting_approval": True,
                "finalized": False,
                "reply": "PRIVATE_REPLY_SENTINEL",
                "error": "PRIVATE_TOKEN_SENTINEL",
            }
        )
    )
    private_stderr.write_text("timed out PRIVATE_STDERR_SENTINEL")
    result = subprocess.run(
        [
            "bash",
            "-c",
            summary + '\napproval_resume_failure_summary "$1" "$2" 23\n',
            "bash",
            str(private_stdout),
            str(private_stderr),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "exit_code=23" in result.stdout
    assert "status=awaiting_approval" in result.stdout
    assert "finalized=false" in result.stdout
    assert "error_category=error_field" in result.stdout
    assert "stderr_category=timed_out" in result.stdout
    assert "PRIVATE_REPLY_SENTINEL" not in result.stdout + result.stderr
    assert "PRIVATE_TOKEN_SENTINEL" not in result.stdout + result.stderr
    assert "PRIVATE_STDERR_SENTINEL" not in result.stdout + result.stderr

    private_stdout.write_text("PRIVATE_PARSE_SENTINEL")
    private_stderr.write_text("could not parse PRIVATE_PARSE_STDERR_SENTINEL")
    parse_result = subprocess.run(
        [
            "bash",
            "-c",
            summary + '\napproval_resume_failure_summary "$1" "$2" 24\n',
            "bash",
            str(private_stdout),
            str(private_stderr),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert parse_result.returncode == 0, parse_result.stderr
    assert "status=parse_failure" in parse_result.stdout
    assert "stderr_category=parse_failure" in parse_result.stdout
    assert "PRIVATE_PARSE_SENTINEL" not in parse_result.stdout + parse_result.stderr
    assert "PRIVATE_PARSE_STDERR_SENTINEL" not in parse_result.stdout + parse_result.stderr


def test_approval_seed_background_message_is_owned_by_global_cleanup() -> None:
    """Every approval-seed exit path must release its Slack stub before teardown."""

    source = LADDER_PATH.read_text()
    trap = _shell_function(source, "cleanup")
    approval = _shell_function(source, "seed_approval_resume_turn")

    assert "APPROVAL_SEED_MESSAGE_PID=$!" in approval
    assert "stop_approval_seed_message terminate || true" in trap
    assert trap.index("stop_approval_seed_message terminate || true") < trap.index(
        "local_compose_cli_args local down"
    ), "the background Slack stub must stop before its stack is torn down"
    assert approval.count("stop_approval_seed_message terminate || true") == 2
    assert "stop_approval_seed_message || code=$?" in approval
    for stale_local_wait in ('kill "$message_pid"', 'wait "$message_pid"'):
        assert stale_local_wait not in approval


def test_approval_seed_cleanup_terminates_and_clears_a_stale_pid() -> None:
    """An interrupted seed cannot leave its background Slack stub alive."""

    source = LADDER_PATH.read_text()
    stop = _shell_function(source, "stop_approval_seed_message")
    result = subprocess.run(
        [
            "bash",
            "-c",
            f"""set -u
{stop}
APPROVAL_SEED_MESSAGE_PID=""
sleep 30 &
stale_pid=$!
APPROVAL_SEED_MESSAGE_PID="$stale_pid"
stop_approval_seed_message terminate || true
[[ -z "$APPROVAL_SEED_MESSAGE_PID" ]]
if kill -0 "$stale_pid" 2>/dev/null; then
    exit 9
fi
""",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_product_collector_restore_restarts_and_verifies_every_seed_emitter() -> None:
    """Worker-only restore cannot prove dispatcher, API, or runner delivery."""

    source = LADDER_PATH.read_text()
    restore = _shell_function(source, "route_local_observability_to_product_collector")
    for service in (
        "curie-api",
        "curie-dispatcher",
        "curie-worker",
        "curie-runner",
    ):
        assert service in restore, (
            f"{service} can keep the disposable sink endpoint unless it is recreated "
            "or launched after product routing is restored"
        )
    assert "assert_product_collector_endpoint" in restore
    assert (
        "export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318" in restore
    )
    assert (
        'export CURIE_WORKER_OTEL_EXPORTER_OTLP_ENDPOINT="$PRODUCT_COLLECTOR_WORKER_ENDPOINT"'
        in restore
    )
    assert "http://127.0.0.1:24318" in source
    assert "export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf" in restore
    assert "unset OTEL_EXPORTER_OTLP_ENDPOINT" not in restore
    for protocol in ("http/protobuf", "otel-collector:4318"):
        assert protocol in restore


@pytest.mark.parametrize("owned", [0, 1])
def test_current_source_images_survive_raw_compose_recreation(owned: int) -> None:
    source = LADDER_PATH.read_text()
    function = _shell_function(source, "pin_local_source_images")
    keys = ("CURIE_BASE_TAG", "CURIE_RUNNER_IMAGE", "CURIE_DISPATCHER_IMAGE")
    initial = dict.fromkeys(keys, "stale-example")
    initial["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://collector.example.com:4318"
    script = function + "\nLOCAL_STACK_OWNED=\"$1\"\npin_local_source_images\n"
    script += (
        "exec python3 -c 'import json,os; "
        'print(json.dumps({key: os.environ[key] for key in '
        '["CURIE_BASE_TAG", "CURIE_RUNNER_IMAGE", "CURIE_DISPATCHER_IMAGE", '
        '"OTEL_EXPORTER_OTLP_ENDPOINT"]}))' + "'\n"
    )
    result = subprocess.run(
        ["bash", "-c", script, "bash", str(owned)],
        env={**os.environ, **initial}, text=True, capture_output=True, check=True,
    )
    child_env = json.loads(result.stdout)
    expected = {
        "CURIE_BASE_TAG": "dev",
        "CURIE_RUNNER_IMAGE": "ghcr.io/curie-eng/curie-runner:dev",
        "CURIE_DISPATCHER_IMAGE": "ghcr.io/curie-eng/curie-dispatcher:dev",
    } if owned else {key: initial[key] for key in keys}
    assert {key: child_env[key] for key in keys} == expected
    assert child_env["OTEL_EXPORTER_OTLP_ENDPOINT"] == initial["OTEL_EXPORTER_OTLP_ENDPOINT"]
    rung = _shell_function(source, "rung_local")
    assert rung.index('"$BIN" "${up_args[@]}"') < rung.index("pin_local_source_images")
    assert rung.index("pin_local_source_images") < rung.index("case_local_otel")


@pytest.mark.parametrize(
    ("response", "exit_code", "category"),
    [
        ({"error": "private-error-canary", "fix": "private-fix-canary"}, 3, "query-error"),
        ({"trace": {}, "tree": []}, 0, "malformed-response"),
        ({"trace": {"id": "a" * 32}, "tree": [{
            "name": "agent.run", "type": "SPAN", "children": [],
            "input": "private-input-canary",
        }]}, 0, "incomplete-membership"),
    ],
)
def test_exact_query_failure_reports_sanitized_reason(
    tmp_path: Path, response: dict, exit_code: int, category: str
) -> None:
    source = LADDER_PATH.read_text()
    fake_bin = tmp_path / "curie"
    fake_bin.write_text('#!/bin/sh\nprintf "%s\\n" "$FAKE_RESPONSE"\nexit "$FAKE_EXIT"\n')
    fake_bin.chmod(0o700)
    script = _shell_function(source, "sanitize_exact_trace_read")
    script += _shell_function(source, "query_exact_seed_trace")
    script += '''
WORKDIR="$1"
BIN="$2"
OBSERVABILITY_POLL_ATTEMPTS=1
OBSERVABILITY_POLL_INTERVAL_SECONDS=0
query_exact_seed_trace local aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa curie.queue.enqueue "" present
'''
    result = subprocess.run(
        ["bash", "-c", script, "bash", str(tmp_path), str(fake_bin)],
        env={**os.environ, "FAKE_RESPONSE": json.dumps(response), "FAKE_EXIT": str(exit_code)},
        text=True, capture_output=True, check=False,
    )
    assert result.returncode == 1
    assert category in result.stderr
    assert "private-" not in result.stderr + result.stdout
    if category == "incomplete-membership":
        assert '"operation":["agent.run"]' in result.stderr


def test_invalid_langfuse_auth_proves_permanent_rejection_and_fresh_recovery(
    tmp_path: Path,
) -> None:
    """Pin the real backend negative without redefining exporter semantics."""

    source = LADDER_PATH.read_text()
    negative = _shell_function(source, "case_local_langfuse_invalid_auth")
    query = _shell_function(source, "query_exact_seed_trace")

    # OTLP requires retry for only 429/502/503/504; 401 MUST NOT be retried:
    # https://opentelemetry.io/docs/specs/otlp/#retryable-response-codes
    # The pinned Collector reports accepted+failed spans, permanent401 rejection,
    # and queue0. Restored credentials must admit a NEW complete trace, not
    # replay a permanently rejected batch.
    for required in (
        "LANGFUSE_OTLP_AUTH_HEADER",
        "INVALID_LANGFUSE_OTLP_AUTH_HEADER",
        "langfuse-web",
        "otelcol_receiver_accepted_spans",
        "otelcol_exporter_send_failed_spans",
        "queue_size",
        "Ready",
        "restart",
        "failed_trace_id",
        "recovered_trace_id",
        "HTTP Status Code 401",
        "Permanent error",
        "query_exact_seed_trace",
    ):
        assert required in negative, f"invalid-auth proof omits {required!r}"
    assert "down -v" not in negative
    assert "same_queued_trace_id" not in negative

    invalid_auth = negative.index(
        'export LANGFUSE_OTLP_AUTH_HEADER="$INVALID_LANGFUSE_OTLP_AUTH_HEADER"'
    )
    control = negative.find('query_exact_seed_trace local "$LAST_ORDINARY_TRACE_ID"')
    assert 0 <= control < invalid_auth, (
        "a successful exact read under valid auth must discriminate backend/query "
        "health before the credential is made invalid"
    )

    # Execute the production query helper: absence is a bounded observation,
    # not one lucky first poll, and only the stable not-found error is accepted.
    stable_absence_dir = tmp_path / "stable-absence"
    stable_absence_dir.mkdir()
    absent, polls = _run_absent_trace_query(
        stable_absence_dir, query, error="exact trace not found"
    )
    assert absent.returncode == 0, absent.stderr
    assert polls == 3, (
        "temporary absence must remain not-found for the full declared poll bound"
    )

    wrong_failure_dir = tmp_path / "wrong-failure"
    wrong_failure_dir.mkdir()
    wrong_failure, _ = _run_absent_trace_query(
        wrong_failure_dir, query, error="Langfuse authorization failed"
    )
    assert wrong_failure.returncode != 0, (
        "an arbitrary candidate-CLI exit 1 must not masquerade as stable not-found"
    )

    absent_call = re.search(
        r'query_exact_seed_trace local "\$failed_trace_id"[^\n]*absent',
        negative,
    )
    recovered_call = re.search(
        r'query_exact_seed_trace local "\$recovered_trace_id"',
        negative,
        re.MULTILINE,
    )
    assert (
        absent_call
        and recovered_call
        and absent_call.start() < recovered_call.start()
    ), "the failed ID must be absent before a new complete recovery trace"


@pytest.mark.parametrize(
    "failure",
    [
        "", "no-failed-metric", "no-rejection-log", "stuck-queue", "restart",
        "reply", "absence", "restore", "recovery-query",
    ],
)
def test_invalid_auth_requires_every_observation_and_always_restores(
    tmp_path: Path, failure: str,
) -> None:
    """Execute the real gate under conditional Bash semantics (errexit disabled)."""
    source = LADDER_PATH.read_text()
    function = _shell_function(source, "assert_product_collector_permanent_auth_rejection")
    function += _shell_function(source, "case_local_langfuse_invalid_auth")
    script = r'''
set -u -o pipefail
WORKDIR="$1"
REPO_ROOT="$2"
FAILURE="$3"
BIN=true
LAST_ORDINARY_TRACE_ID=cccccccccccccccccccccccccccccccc
ladder_compose() { docker compose "$@"; }
sleep() { :; }
docker() {
    if [[ "$1" == logs ]]; then
        [[ "$FAILURE" == no-rejection-log ]] ||
            printf '%s\n' 'Permanent error: HTTP Status Code 401; not retryable error'
    else
        printf '%s\n' acme-collector
    fi
}
restart_local_product_collector() { [[ "$FAILURE" != restart ]]; }
wait_product_collector_ready() { return 0; }
restore_local_langfuse_auth() {
    printf '%s\n' restored >> "$WORKDIR/events"
    [[ "$FAILURE" != restore ]]
}
assert_finalized_reply() { [[ "$FAILURE" != reply ]]; }
assert_product_runner_endpoints() { return 0; }
seed_ordinary_turn() { LAST_ORDINARY_TRACE_ID=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb; }
capture_stream_cursor() { printf '%s\n' 1-0; }
discover_trace_id_for_seed() {
    if [[ -e "$WORKDIR/seeded" ]]; then
        printf '%s\n' bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
    else
        touch "$WORKDIR/seeded"
        printf '%s\n' aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    fi
}
product_collector_metric_value() {
    local count=0 file="$WORKDIR/$1"
    [[ ! -e "$file" ]] || read -r count < "$file"
    printf '%s\n' "$((count + 1))" > "$file"
    case "$1" in
        *queue_size) [[ "$FAILURE" != stuck-queue ]] && echo 0 || echo 1 ;;
        *send_failed_spans)
            [[ "$FAILURE" != no-failed-metric && "$count" -gt 0 ]] && echo 10 || echo 0 ;;
        *accepted_spans) [[ "$count" -gt 0 ]] && echo 10 || echo 0 ;;
    esac
}
query_exact_seed_trace() {
    printf '%s\n' "query:$2:${3-}:${5-}" >> "$WORKDIR/events"
    [[ "$2" != bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb || "$FAILURE" != recovery-query ]] || return 1
    [[ "${5-}" != absent || "$FAILURE" != absence ]]
}
'''
    script += function
    script += '\nif case_local_langfuse_invalid_auth acme-agent; then exit 0; else exit 1; fi\n'
    result = subprocess.run(
        ["bash", "-c", script, "bash", str(tmp_path), str(REPO_ROOT), failure],
        text=True, capture_output=True, check=False,
    )
    events = (tmp_path / "events").read_text()
    assert events.count("restored") == 1
    assert result.returncode == (1 if failure else 0), result.stderr
    recovered = "query:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:curie.queue.enqueue,curie.queue.process"
    if failure and failure != "recovery-query":
        assert recovered not in events
    else:
        assert recovered in events
        assert events.index(":absent") < events.index("restored") < events.index(recovered)


def test_default_local_exact_positive_rejects_incomplete_membership(
    tmp_path: Path,
) -> None:
    """Normal local is strict; only ownership diagnostics may retain a miss."""

    source = LADDER_PATH.read_text()
    rung = _function_body(source, "rung_local", "# local-release mode:")
    assert "product_query_state=present" in rung
    assert 'if [[ "$PRODUCT_OBSERVABILITY" == "1" ]]' in rung
    assert rung.count("product_query_state=observe") == 1
    assert 'seed_ordinary_turn local "$agent_id" "$product_query_state"' in rung
    assert "invalid-auth negative skipped" not in rung

    complete_dir = tmp_path / "complete"
    complete_dir.mkdir()
    complete = _run_local_exact_positive(
        complete_dir,
        operations=("curie.queue.enqueue", "curie.reply.post"),
    )
    assert complete.returncode == 0, complete.stderr

    incomplete_dir = tmp_path / "incomplete"
    incomplete_dir.mkdir()
    incomplete = _run_local_exact_positive(
        incomplete_dir,
        operations=("curie.queue.enqueue",),
    )
    assert incomplete.returncode != 0, (
        "the default local positive must fail when exact Langfuse membership is incomplete"
    )

    diagnostic_dir = tmp_path / "diagnostic"
    diagnostic_dir.mkdir()
    diagnostic = _run_local_exact_positive(
        diagnostic_dir,
        operations=("curie.queue.enqueue",),
        expected_state="observe",
    )
    assert diagnostic.returncode == 0, (
        "only explicit product ownership diagnostics may retain incomplete membership"
    )


def test_cluster_product_observability_is_private_preflight_and_query_only() -> None:
    """The ladder may inspect a task release but must never install or remove it."""

    source = LADDER_PATH.read_text()
    preflight = _shell_function(source, "preflight_cluster_product_observability")
    for required in (
        "CURIE_NAMESPACE",
        "CURIE_RELEASE",
        "default",
        "curie",
        "langfuse",
        "otel-collector",
        "api",
        "worker",
        "runner-prewarm",
        "imageID",
        "docker image inspect",
        "curie-api:local",
        "curie-worker:local",
        "curie-runner:latest",
        "mismatch",
    ):
        assert required in preflight, f"cluster preflight omits {required!r}"

    mode = _shell_function(source, "run_cluster_product_observability")
    wrapper = _shell_function(source, "rung_cluster_product")
    assert "preflight_cluster_product_observability" in mode
    assert "seed_cluster_missing_carrier_control" in mode
    assert "cluster_external_ingress_seed" in mode
    assert "CURIE_E2E_CLUSTER_EXTERNAL_INGRESS_RECEIPT" in source
    assert "CURIE_E2E_PRODUCT_RUN_ID" in source
    assert "otelcol_receiver_accepted_spans_delta" in source
    assert "otelcol_exporter_sent_spans_delta" in source
    assert "cluster observability run" in mode
    for manufactured in (
        "enqueue_cluster_carried_turn",
        "CLUSTER_SEEDED_TRACE_ID",
        "secrets.token_hex",
    ):
        assert manufactured not in source, (
            "cluster correlation evidence must be derived from a real accepted "
            f"ingress path, not manufactured by the harness: {manufactured}"
        )
    assert "cluster deploy" in wrapper, (
        "the wrapper must retain the intended agent seed deployment"
    )
    for mutating in (
        "cluster up",
        "cluster down",
        "helm install",
        "helm upgrade",
        "helm uninstall",
        "kubectl delete",
    ):
        for helper_name, helper in (
            ("preflight", preflight),
            ("query", mode),
            ("product wrapper", wrapper),
        ):
            assert mutating not in helper, (
                "product-observability preflight/query/wrapper must not manage the "
                f"release; found {mutating!r} in {helper_name}"
            )
    assert '-o json > "$inventory"' not in preflight, (
        "preflight must query Ready/imageID fields directly, not persist full pod "
        "JSON that may contain private environment values"
    )
    assert "cluster-product-pods" not in preflight, (
        "full pod specifications must not be persisted even in a mode-0600 artifact"
    )


def test_cluster_carrierless_control_and_external_slack_carrier_are_executed(
    tmp_path: Path,
) -> None:
    """The fake cluster path stays negative; only Slack ingress can be positive."""

    source = LADDER_PATH.read_text()
    marker = "curie-seed-external-ordinary-example"
    trace_id = "a" * 32
    span_id = "b" * 16
    payload_only = [
        "2-0",
        [
            "payload",
            json.dumps(
                {
                    "text": f"missing carrier compatibility {marker}",
                    "reply_handle": {"adapter": "curie-cluster-message"},
                }
            ),
        ],
    ]
    carried_slack = [
        "3-0",
        [
            "payload",
            json.dumps(
                {
                    "text": f"ordinary correlation {marker}",
                    "reply_handle": {"kind": "slack", "adapter": None},
                }
            ),
            "traceparent",
            f"00-{trace_id}-{span_id}-01",
        ],
    ]

    negative = _run_seed_matcher(
        tmp_path,
        _python_heredoc(_shell_function(source, "seed_cluster_missing_carrier_control")),
        marker,
        [payload_only],
    )
    assert negative.returncode == 0, negative.stderr

    external_matcher = _python_heredoc(
        _shell_function(source, "discover_cluster_external_trace_id")
    )
    positive = _run_seed_matcher(tmp_path, external_matcher, marker, [carried_slack])
    assert positive.returncode == 0, positive.stderr
    assert positive.stdout.strip() == trace_id

    forged = _run_seed_matcher(
        tmp_path,
        external_matcher,
        marker,
        [
            [
                "4-0",
                [
                    "payload",
                    json.dumps(
                        {
                            "text": f"ordinary correlation {marker}",
                            "reply_handle": {"adapter": "curie-cluster-message"},
                        }
                    ),
                    "traceparent",
                    f"00-{trace_id}-{span_id}-01",
                ],
            ]
        ],
    )
    assert forged.returncode != 0, (
        "a harness/cluster-message-shaped entry must not satisfy real Slack ingress"
    )


def test_adopted_component_stop_requires_successful_export_on_both_product_surfaces(
    tmp_path: Path,
) -> None:
    """Selective ingest loss is a dependency blocker only after Curie is cleared."""

    source = LADDER_PATH.read_text()
    decision = _shell_function(source, "classify_product_observability_owner")
    for required in (
        "raw_emitted_observations",
        "otelcol_receiver_accepted_spans",
        "otelcol_exporter_sent_spans",
        "langfuse_observation_membership",
        "local",
        "cluster",
        "image_ids_match",
        "seed_valid",
        "same_id_raw_collector_receipt",
        "run_id",
        "adopted-component",
    ):
        assert required in decision, f"ownership classification omits {required!r}"
    assert decision.index("otelcol_exporter_sent_spans") < decision.index(
        "adopted-component"
    )
    assert decision.index("langfuse_observation_membership") < decision.index(
        "adopted-component"
    )

    def record(
        *,
        membership: bool,
        same_id_raw: bool = True,
        run_id: str = "run-example",
    ) -> dict[str, object]:
        return {
            "run_id": run_id,
            "seed_valid": True,
            "same_id_raw_collector_receipt": same_id_raw,
            "raw_emitted_observations": 3,
            "otelcol_receiver_accepted_spans": 3,
            "otelcol_exporter_sent_spans": 3,
            "langfuse_observation_membership": membership,
            "image_ids_match": True,
        }

    cases = (
        (None, None, "curie-unresolved", False),
        (record(membership=True, same_id_raw=False), None, "curie-clear", True),
        (None, record(membership=True, same_id_raw=False), "curie-clear", True),
        (record(membership=False, same_id_raw=False), None, "curie-unresolved", False),
        (
            {
                key: value
                for key, value in record(membership=True).items()
                if key != "langfuse_observation_membership"
            },
            record(membership=True),
            "curie-unresolved",
            False,
        ),
        (
            record(membership=True, run_id="run-one"),
            record(membership=True, run_id="run-two"),
            "curie-unresolved",
            False,
        ),
        (record(membership=False), record(membership=False), "adopted-component", False),
        (
            record(membership=False, same_id_raw=False),
            record(membership=False, same_id_raw=False),
            "curie-unresolved",
            False,
        ),
        (record(membership=True), record(membership=False), "curie-owned", False),
        (
            record(membership=True, same_id_raw=False),
            record(membership=True),
            "curie-clear",
            True,
        ),
        (record(membership=True), record(membership=True), "curie-clear", True),
    )
    for index, (local, cluster, verdict, succeeds) in enumerate(cases):
        case_dir = tmp_path / str(index)
        case_dir.mkdir()
        result = _run_product_classifier(case_dir, local=local, cluster=cluster)
        assert verdict in result.stdout
        assert (result.returncode == 0) is succeeds, (
            "only complete same-run, positive export and exact membership may "
            f"clear the STOP; verdict={verdict} stdout={result.stdout!r}"
        )
