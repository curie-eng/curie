"""Shared telemetry: the Discord adapter's process logs are JSON, redacted, exported.

Until #2358 `main()` called `logging.basicConfig(level=logging.INFO)`, which
puts the adapter outside `curie_telemetry` entirely: no `RedactingLogFilter`, no
JSON envelope carrying `service.name`, and no OTLP export of a single log line,
span or metric. It was the last first-party Python entrypoint in this repo in
that state.

"The process's logs are redacted and exported" is a property of the *process*,
not of a function, so every test here drives the real entrypoint through
`_telemetry_process.py` in a subprocess and reads its actual merged
stdout+stderr, or reads a real OTLP collector listening on 127.0.0.1. Calling
`configure_service_logging` directly, or asserting on `caplog`, would prove
nothing about the shipped image — the regression this file exists to catch lives
in `main()`, in the one statement those approaches skip.

Only synthetic credentials appear here. No real Discord token exists in this
suite, no Discord surface is contacted, and no socket leaves 127.0.0.1.

Two environment facts a later author needs.

**`/tmp`, not `$HOME`.** `REDACTION_RULES`' `home_path` rule rewrites
`/(?:home|Users)/[^/\\s]+`, so any adapter path under `$HOME` would arrive in
output as `[REDACTED:home_path]`. `pytest`'s `tmp_path` lives under the
`tempfile` root (`/tmp` on this project's Linux images and CI), which is why the
state path is built from it rather than from a home-relative directory.

**A negative control on the export path must live in the message template.**
`_otlp_body` builds the exported body from `record.msg` with *safe* formatting
args elided back to a bare `%s`, substituting an arg's value only when redaction
changed it — so a secret contributes its bounded marker and nothing else dynamic
travels. The planted records therefore carry `PLANTED_CARRIER` as literal
template text and the credential as the lone `%s` arg. Folding the credential
into the template too would look like a simplification and would silently stop
exercising `_otlp_body`'s arg-redaction path; moving the carrier into an arg
would break the OTLP negative controls by construction rather than by regression.

**Removing `basicConfig` is what makes third-party coverage load-bearing.** That
call had one accidental virtue: it gave `discord`, `uvicorn` and `httpx` a root
handler. Delete it and configure only the `curie_discord_adapter` package logger,
and those libraries have no handler at all — so `logging.lastResort` prints their
WARNING+ records to stderr as plain, unredacted text. That is the exact condition
#2358 exists to end, merely relocated from our package into our dependencies, and
discord.py's own "davey is not installed" warning fires it on every boot. So
`main()` must bootstrap those three logger names too, and the last test here pins
it. A future author whose instinct is to mute the dependency to make that test
pass would be re-creating the leak in the shipped process while turning the suite
green; the fix belongs in `main()`.

**The adapter logs no operator-supplied credential today.** Its call sites pass
ids, status codes and fixed labels — that is the design working. So the planted
record below is what a *future* call site would look like, and the regression
actually guarded is "the shared filter is installed on the package logger by the
real entrypoint", which is exactly what `basicConfig` fails to do.
"""

from __future__ import annotations

import gzip
import json
import os
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest

SERVICE_NAME = "curie-discord-adapter"
DRIVER_PATH = Path(__file__).with_name("_telemetry_process.py")

# A synthetic Curie-minted channel token in the exact `chn.{payload}.{signature}`
# shape `redact.py`'s `channel_token` rule matches. That rule landed in #2359,
# the prerequisite this ticket is deliberately sequenced after, so planting this
# specific shape is what proves the Discord adapter joins the *strengthened*
# common policy rather than some weaker local one. Payload and signature are
# obviously fake and decode to nothing.
PLANTED_CHANNEL_TOKEN = "chn.not-a-real-payload-2358.not-a-real-signature-2358"
REDACTION_MARKER = "[REDACTED:channel_token]"

# Duplicated from `_telemetry_process.PLANTED_CARRIER` rather than imported: this
# suite has no `conftest.py` putting its own directory on `sys.path`, and the root
# run uses `--import-mode=importlib`, under which a sibling test-directory module
# is not importable by bare name. The constant is a plain literal with no logic
# behind it, and if a copy ever drifted the negative controls below would fail
# loudly rather than start passing vacuously.
PLANTED_CARRIER = "planted telemetry probe record for the discord adapter"

# Duplicated from `_telemetry_process.THIRD_PARTY_CHILD_LOGGERS` for the same
# `sys.path` reason as the carrier above. One child per namespace listed in
# `main._THIRD_PARTY_LOG_NAMESPACES`, except `uvicorn`, which is pinned by
# `test_the_uvicorn_access_log_is_redacted_json_too` under a strictly harder
# condition (uvicorn's own `dictConfig` has run by then). Its omission here is
# deliberate, not an oversight: a second, weaker copy of that coverage would add
# no mutation the access-log test does not already kill.
THIRD_PARTY_CHILD_LOGGERS = ("discord.client", "httpx._client")


def free_port() -> int:
    """A port the OS has just confirmed free, released immediately.

    Racy in principle and fine in practice: the window is microseconds and the
    alternative — a fixed port — collides for real whenever this suite runs
    beside another.
    """
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def wait_until(predicate: object, timeout: float, message: str) -> None:
    """Poll instead of sleeping a fixed interval, and fail loudly on timeout.

    A fixed sleep either wastes time or flakes under load; a bare `assert` after
    a sleep loses the reason. Every wait here names what it was waiting for.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():  # type: ignore[operator]
            return
        time.sleep(0.02)
    raise AssertionError(f"timed out after {timeout}s waiting for: {message}")


# --- the local OTLP collector -------------------------------------------------


class OtlpReceiver:
    """A real HTTP OTLP endpoint on 127.0.0.1 that keeps every request verbatim.

    Verbatim matters: the strongest assertion available is "the planted secret's
    bytes appear nowhere in anything the process put on the wire", and that can
    only be made against the raw bodies, not against a parsed view that might
    quietly drop the field carrying it.
    """

    def __init__(self) -> None:
        self.requests: list[tuple[str, bytes, dict[str, str]]] = []
        self._lock = threading.Lock()

    def record(self, path: str, body: bytes, headers: dict[str, str]) -> None:
        with self._lock:
            self.requests.append((path, body, headers))

    def paths(self) -> list[str]:
        with self._lock:
            return [path for path, _, _ in self.requests]

    def bodies(self, path: str) -> list[bytes]:
        with self._lock:
            return [body for request_path, body, _ in self.requests if request_path == path]

    def all_bytes(self) -> bytes:
        with self._lock:
            return b"".join(body for _, body, _ in self.requests)

    def log_requests(self) -> list[ExportLogsServiceRequest]:
        """The `/v1/logs` payloads decoded as real protobuf.

        Decoding is the point of this helper. "A POST arrived" is satisfied by
        an empty or malformed body; only a parsed `ExportLogsServiceRequest`
        shows that the SDK actually built and exported log records.
        """
        decoded = []
        for body in self.bodies("/v1/logs"):
            request = ExportLogsServiceRequest()
            request.ParseFromString(body)
            decoded.append(request)
        return decoded

    def log_record_count(self) -> int:
        return sum(
            len(scope.log_records)
            for request in self.log_requests()
            for resource in request.resource_logs
            for scope in resource.scope_logs
        )

    def logger_names(self) -> set[str]:
        """The logger name of every exported record.

        The SDK's `LoggingHandler` calls `get_logger(record.name)`, so a record's
        originating logger arrives as its instrumentation *scope* name. That is
        the only place the name survives export, and reading it is what lets a
        test say "a record from THIS logger reached the collector" rather than
        the much weaker "some record did".
        """
        return {
            scope.scope.name
            for request in self.log_requests()
            for resource in request.resource_logs
            for scope in resource.scope_logs
        }

    def service_names(self) -> set[str]:
        return {
            attribute.value.string_value
            for request in self.log_requests()
            for resource in request.resource_logs
            for attribute in resource.resource.attributes
            if attribute.key == "service.name"
        }


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self) -> None:  # noqa: N802  (BaseHTTPRequestHandler's naming)
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length)
        # The SDK's HTTP exporter gzips by default. Store the decompressed bytes
        # so a raw-substring assertion is looking at plaintext rather than at a
        # deflate stream in which any string is trivially "absent".
        if (self.headers.get("Content-Encoding") or "").lower() == "gzip":
            body = gzip.decompress(body)
        self.server.receiver.record(  # type: ignore[attr-defined]
            self.path, body, {key.lower(): value for key, value in self.headers.items()}
        )
        self.send_response(200)
        self.send_header("Content-Type", "application/x-protobuf")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args: object) -> None:
        """Silence the default per-request stderr line; it is pytest noise only."""


@pytest.fixture
def otlp() -> Iterator[tuple[OtlpReceiver, str]]:
    receiver = OtlpReceiver()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    server.receiver = receiver  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield receiver, f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


# --- driving the real entrypoint ----------------------------------------------


def driver_env(tmp_path: Path, scenario: str, **extra: str) -> dict[str, str]:
    """A CLOSED environment for the adapter process.

    Built explicitly rather than copied from `os.environ`, because inheriting
    would let an `OTEL_*` variable set on the developer's box, or by the outer
    pytest run, decide which code path the "no endpoint configured" test
    exercises — and that test would then pass while proving the opposite of what
    it claims. `PATH` is the one inherited key: it is what makes the interpreter
    and its venv resolvable, and it carries no telemetry meaning.
    """
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "CURIE_DISCORD_TEST_SCENARIO": scenario,
        "CURIE_DISCORD_TEST_PLANTED_SECRET": PLANTED_CHANNEL_TOKEN,
        # `DiscordConfig` refuses an empty token and an empty secret, so both are
        # required for the process to reach `run()` at all. Synthetic values: the
        # Gateway peer is faked and no HTTP request is ever authenticated.
        "DISCORD_BOT_TOKEN": "synthetic-bot-token-not-a-real-discord-credential",
        # Deliberately short and non-credential-shaped. The repo's pre-commit
        # credential scanner flags a long quoted literal assigned to any
        # secret-named key, and a test placeholder that trips that gate trains
        # people to bypass it. The adapter only requires a non-empty value here.
        "CURIE_DISCORD_ADAPTER_SECRET": "unused",
        # Under `tmp_path` (the `tempfile` root, i.e. `/tmp`) rather than `$HOME`:
        # the `home_path` redaction rule would rewrite a home-relative path in
        # any output assertion, and the test would then be reading a placeholder.
        "CURIE_DISCORD_STATE_PATH": str(tmp_path / "discord-state.sqlite3"),
        "CURIE_DISCORD_REPLY_PORT": str(free_port()),
    }
    env.update(extra)
    return env


def run_driver(env: dict[str, str], *, timeout: float = 60.0) -> tuple[int, str]:
    """Run the entrypoint to completion; return its exit code and merged output.

    Merged stdout+stderr on purpose: the claim under test is that *nothing* the
    process writes escapes the service logger, and splitting the streams would
    let an escapee hide in the one nobody asserted on.
    """
    process = subprocess.run(
        [sys.executable, str(DRIVER_PATH)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
    )
    return process.returncode, process.stdout


def assert_carrier_survived_and_secret_did_not(text: str, *, where: str) -> None:
    """The shared redaction triplet, asserted against one surface: `where`.

    Three assertions, in this exact order, repeat verbatim across the tests
    below regardless of whether `text` is the process's decoded stderr or the
    decoded bytes a local OTLP collector received: the non-secret carrier text
    IS present, the raw planted token is NOT present, and the
    `[REDACTED:channel_token]` marker IS present.

    The carrier assertion runs FIRST, deliberately: without it, the two secret
    assertions that follow are satisfied just as well by output that never
    contained the planted record at all, so a caller could not tell "redacted"
    apart from "never emitted" — the carrier check is what keeps the negative
    controls from passing vacuously.
    """
    assert PLANTED_CARRIER in text, (
        f"the planted record was never emitted to {where}, so the redaction "
        f"assertions below would pass vacuously"
    )
    assert PLANTED_CHANNEL_TOKEN not in text, (
        f"the planted channel token survived into {where}"
    )
    assert REDACTION_MARKER in text, (
        f"{where} carries no channel_token placeholder, so the record never "
        f"passed RedactingLogFilter"
    )


def wait_for_export(receiver: OtlpReceiver, *, where: str) -> None:
    """Poll until the collector has received at least one log record.

    Centralizes the polling shape shared by every exporting test — only the
    `BatchLogRecordProcessor`'s force-flush on exit ever makes a record arrive
    at all, since its 1000 ms schedule delay outlives these short-lived
    processes — so a dropped `telemetry.shutdown()` call on any exit path shows
    up here as a timeout rather than as a wrong value. `where` names what this
    call site expected to see arrive, so a timeout still identifies which path
    is missing its flush.
    """
    wait_until(lambda: receiver.log_record_count() > 0, 10.0, where)


def json_records(output: str) -> list[dict[str, object]]:
    """Every non-blank output line, parsed as a service log record.

    Strict on purpose: `bootstrap_service_telemetry` is the first statement in
    `main()`, so no line of this process's output may legitimately predate the
    JSON handler. A plain-text line here means a logger escaped the
    `curie_discord_adapter` package — precisely the `basicConfig` regression —
    so it is an assertion failure naming the escapee, not something to skip.
    """
    records: list[dict[str, object]] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except ValueError:
            raise AssertionError(
                f"a line of adapter output was not JSON, so it bypassed the "
                f"service logger:\n{line!r}\n\nfull output:\n{output}"
            ) from None
        assert isinstance(record, dict), f"a log line was not a JSON object: {line!r}"
        records.append(record)
    return records


# --- the tests ----------------------------------------------------------------


def test_a_planted_secret_in_a_package_log_line_is_redacted_in_process_output(
    tmp_path: Path,
) -> None:
    """A credential that reaches a log line leaves the process redacted.

    With `basicConfig` there is no `RedactingLogFilter` anywhere in this process,
    so whatever a call site passes goes verbatim into the cluster's log
    retention. The planted `chn.` token stands in for a future call site; what is
    actually pinned is that the shared filter is installed on the *package*
    logger by the real entrypoint.

    Cannot pass vacuously: the negative control asserts the record's own
    non-secret carrier text is present first, so "the raw token is absent" is
    never satisfied by output that simply never contained the record.
    """
    code, output = run_driver(driver_env(tmp_path, "planted_secret"))

    assert code == 0, f"the driver did not exit cleanly; output:\n{output}"
    assert_carrier_survived_and_secret_did_not(
        output, where=f"process output:\n{output}"
    )


def test_log_records_actually_reach_a_local_otlp_collector_redacted(
    tmp_path: Path, otlp: tuple[OtlpReceiver, str]
) -> None:
    """Records really are exported, and really are redacted on the wire.

    The AC that matters most, and the one no in-process assertion can make:
    `basicConfig` installs no `LoggerProvider` at all, so nothing is exported no
    matter how the handler is inspected. Only `OTEL_EXPORTER_OTLP_ENDPOINT` is
    set — the bootstrap appends `/v1/logs` itself (verified against
    `bootstrap._exporter_endpoint`), so pointing at the bare base URL is what a
    real deployment does.

    This is simultaneously the proof of normal-exit cleanup. The
    `BatchLogRecordProcessor` runs on a 1000 ms schedule delay and this process
    exits well inside it, so the only way a record can arrive at all is
    `finally: telemetry.shutdown()` force-flushing it. If the flush is dropped,
    this test fails with zero requests received rather than with a wrong value.
    """
    receiver, base_url = otlp
    env = driver_env(tmp_path, "planted_secret", OTEL_EXPORTER_OTLP_ENDPOINT=base_url)

    code, output = run_driver(env)
    assert code == 0, f"the driver did not exit cleanly; output:\n{output}"

    wait_for_export(
        receiver,
        where="an ExportLogsServiceRequest carrying at least one log record on "
        "/v1/logs — none arrived, which is what a missing bootstrap or a dropped "
        f"telemetry.shutdown() force-flush looks like. Process output:\n{output}",
    )

    assert "/v1/logs" in receiver.paths()
    assert SERVICE_NAME in receiver.service_names(), (
        f"no exported resource identified itself as {SERVICE_NAME}; saw "
        f"{sorted(receiver.service_names())}"
    )

    # Negative control, exactly as in the stderr test: the carrier text must be
    # on the wire before "the secret is not on the wire" means anything.
    exported = receiver.all_bytes()
    assert_carrier_survived_and_secret_did_not(
        exported.decode("utf-8", errors="replace"), where="the collector's socket"
    )


def test_no_otlp_endpoint_configured_is_a_no_op_not_a_boot_failure(
    tmp_path: Path, otlp: tuple[OtlpReceiver, str]
) -> None:
    """No endpoint means no export — and emphatically not a crash.

    The negative control for the test above: it shows the collector assertions
    there are caused by the configuration, not by the receiver observing traffic
    it would have seen anyway. It is also the property every local, offline and
    CI install depends on. The bootstrap runs before `DiscordConfig()`, so a
    raise inside it would replace a precise config error with an opaque OTel
    traceback on every developer's machine.

    The receiver is running and its address is deliberately never given to the
    process, so "received nothing" is a real observation rather than the absence
    of a listener.
    """
    receiver, _ = otlp
    env = driver_env(tmp_path, "quiet")
    assert not [name for name in env if name.startswith("OTEL_")], (
        "driver_env is a closed world; an OTEL_* key here would mean this test "
        "is exercising the configured path instead of the unconfigured one"
    )

    code, output = run_driver(env)

    assert code == 0, f"the adapter failed to boot without an OTLP endpoint:\n{output}"
    assert json_records(output), (
        f"the adapter emitted no log records at all, so this proves nothing "
        f"about the unconfigured path still logging; output:\n{output}"
    )
    assert receiver.requests == [], (
        f"the process contacted a collector it was never pointed at: "
        f"{receiver.paths()}"
    )


def test_the_package_logger_owns_module_loggers_including_ones_added_later(
    tmp_path: Path,
) -> None:
    """One bootstrap on the package logger has to cover every module beneath it.

    `main`, `egress`, `http`, `ingress`, `state` and `config` each hold their own
    `logging.getLogger(__name__)`, reached only because `Logger.callHandlers`
    walks up to the `curie_discord_adapter` package logger, on which
    `configure_service_logging` sets `propagate=False`. The driver logs through a
    module name that does not exist in the tree at all, which is the property
    that separates bootstrapping the package from bootstrapping modules one by
    one: the latter passes for today's six modules and silently leaves the
    seventh unredacted.

    A stray `basicConfig` restoring a plain-text root handler shows up here as a
    non-JSON line (`json_records` raises naming it); a per-module bootstrap shows
    up as a missing logger name.
    """
    code, output = run_driver(driver_env(tmp_path, "descendant"))

    assert code == 0, f"the driver did not exit cleanly; output:\n{output}"
    records = json_records(output)
    assert records, f"the adapter emitted no log records at all:\n{output}"
    for record in records:
        assert record.get("service.name") == SERVICE_NAME, (
            f"a record escaped the service logger: {record}"
        )
    loggers = {str(record.get("logger")) for record in records}
    assert {
        "curie_discord_adapter.main",
        "curie_discord_adapter.a_module_added_later",
    } <= loggers, (
        f"an existing module logger and a not-yet-written one were both expected "
        f"under the package logger; saw {sorted(loggers)}"
    )


def test_telemetry_is_flushed_on_an_error_exit_too(
    tmp_path: Path, otlp: tuple[OtlpReceiver, str]
) -> None:
    """The `finally` runs when `run()` raises, and the failure still surfaces.

    Both halves are load-bearing together. A non-zero exit alone would also be
    produced by a `main()` with no cleanup at all; a delivered record alone would
    also be produced by swallowing the exception and returning normally. Only the
    pair proves `finally: telemetry.shutdown()` ran *on the error path* — the
    process is dying at that moment, and an unflushed `BatchLogRecordProcessor`
    dies with it, taking the last record before a crash, the one an operator most
    needs, along with it.

    Paired with the normal-exit delivery proved above, this covers both exits.
    """
    receiver, base_url = otlp
    env = driver_env(tmp_path, "error_exit", OTEL_EXPORTER_OTLP_ENDPOINT=base_url)

    code, output = run_driver(env)

    assert code != 0, (
        f"the synthetic gateway failure was swallowed; an adapter that exits 0 on "
        f"a Gateway crash never restarts. Output:\n{output}"
    )
    wait_for_export(
        receiver,
        where="a log record exported on the error path — none arrived, so "
        f"telemetry.shutdown() did not run in main()'s finally. Output:\n{output}",
    )

    exported = receiver.all_bytes()
    assert_carrier_survived_and_secret_did_not(
        exported.decode("utf-8", errors="replace"), where="the error path's export"
    )
    # The traceback Python prints for the uncaught RuntimeError is written by the
    # interpreter, outside logging, so it is not asserted to be JSON here. Its
    # message is credential-free by construction (see the driver) precisely so
    # that this test never has to pretend otherwise.
    assert PLANTED_CHANNEL_TOKEN not in output, (
        f"the planted token leaked into process output on the error path:\n{output}"
    )


def test_a_third_party_library_warning_also_leaves_as_redacted_json(
    tmp_path: Path, otlp: tuple[OtlpReceiver, str]
) -> None:
    """`discord`, `uvicorn` and `httpx` records go through the same handler.

    The regression this catches is created by the fix itself. `basicConfig`'s one
    accidental virtue was a root handler that caught every library the adapter
    runs under; removing it without bootstrapping those loggers sends their
    WARNING+ records to `logging.lastResort`, which writes plain unredacted text
    to the very stderr an operator ships to log retention. discord.py fires that
    path unprompted on every boot with its optional-voice warning.

    Driven through a *child* of each namespace — `discord.client`, `httpx._client`
    — so what is pinned is propagation coverage: an implementation that
    configured one logger by exact name would still leave `discord.gateway`,
    `discord.http` and every sibling uncovered, and this test fails on it.

    One record per namespace, and one assertion per namespace, is what makes the
    constant load-bearing entry by entry. Emitting only through `discord.client`
    left `httpx` deletable from `_THIRD_PARTY_LOG_NAMESPACES` with the suite
    green: httpx logs at DEBUG in practice and `logging.lastResort` fires only at
    WARNING+, so an uncovered httpx never printed a plain-text line for
    `json_records` to catch. `uvicorn` is not re-emitted here; it is pinned by
    the access-log test under a harder condition, and duplicating it would add no
    coverage.

    The collector half additionally pins `logger_provider=telemetry.logger_provider`
    in the bootstrap loop: drop it and third-party records still reach the
    redacting JSON handler on stderr — every stderr assertion here stays green —
    while nothing from those namespaces is ever exported. Only an OTLP assertion
    naming those loggers kills that mutation.

    Cannot pass vacuously: `json_records` is strict, so a `lastResort` line is an
    error naming the escapee rather than a skipped line, and the carrier text is
    asserted present before the raw token is asserted absent, on stderr and on
    the wire alike.
    """
    receiver, base_url = otlp
    code, output = run_driver(
        driver_env(tmp_path, "third_party", OTEL_EXPORTER_OTLP_ENDPOINT=base_url)
    )

    assert code == 0, f"the driver did not exit cleanly; output:\n{output}"
    records = json_records(output)
    emitted = {str(record.get("logger")) for record in records}
    for logger_name in THIRD_PARTY_CHILD_LOGGERS:
        assert logger_name in emitted, (
            f"no record from {logger_name} was emitted as service JSON, so its "
            f"namespace either never fired or escaped to lastResort; saw "
            f"{sorted(emitted)}"
        )
    third_party = [
        record for record in records if str(record.get("logger")) in THIRD_PARTY_CHILD_LOGGERS
    ]
    for record in third_party:
        assert record.get("service.name") == SERVICE_NAME, (
            f"a third-party record carries no service identity: {record}"
        )
    assert_carrier_survived_and_secret_did_not(
        output, where=f"third-party process output:\n{output}"
    )

    wait_for_export(
        receiver,
        where="an exported log record from a third-party logger — none arrived, "
        "which is what dropping logger_provider from main()'s third-party "
        f"bootstrap loop looks like while stderr stays green. Output:\n{output}",
    )
    exported_loggers = receiver.logger_names()
    for logger_name in THIRD_PARTY_CHILD_LOGGERS:
        assert logger_name in exported_loggers, (
            f"{logger_name} reached stderr but never the collector, so its "
            f"bootstrap was given no logger_provider; exported "
            f"{sorted(exported_loggers)}"
        )
    exported = receiver.all_bytes()
    assert_carrier_survived_and_secret_did_not(
        exported.decode("utf-8", errors="replace"), where="the third-party export"
    )


def test_the_uvicorn_access_log_is_redacted_json_too(tmp_path: Path) -> None:
    """uvicorn must not be allowed to steal its own access logger back.

    `uvicorn.Config.load()` — the first thing `Server.serve()` does — applies
    uvicorn's default `LOGGING_CONFIG` through `dictConfig`, which attaches a
    plain-text stdout handler to `uvicorn.access` and sets `propagate=False` on
    it. That detaches the access log from the `uvicorn` namespace handler
    `main()` installs, and the access log is precisely where request paths and
    query strings go — the payload the `url_secret_param` redaction rule exists
    for. So bootstrapping the `uvicorn` logger is necessary and not sufficient.

    The mechanism that makes it sufficient is `log_config=None` on the
    `uvicorn.Config` built in `run()`: uvicorn then leaves logging alone and
    `uvicorn.access` stays `handlers=[] propagate=True`, reaching the redacting
    JSON handler like every other logger here. A future author who drops that
    kwarg as redundant will see this test go red, and this docstring is the
    explanation of why.

    Cannot pass vacuously: the fake `serve()` really does call `config.load()`,
    `json_records` is strict so a stolen logger's stdout line is an error naming
    it, and the carrier is asserted present before the token is asserted absent.
    """
    code, output = run_driver(driver_env(tmp_path, "access_log"))

    assert code == 0, f"the driver did not exit cleanly; output:\n{output}"
    records = json_records(output)
    access = [record for record in records if record.get("logger") == "uvicorn.access"]
    assert access, (
        f"no uvicorn.access record was emitted as service JSON, so uvicorn's "
        f"LOGGING_CONFIG kept ownership of it; saw "
        f"{sorted({str(record.get('logger')) for record in records})}"
    )
    for record in access:
        assert record.get("service.name") == SERVICE_NAME, (
            f"an access record carries no service identity: {record}"
        )
    assert_carrier_survived_and_secret_did_not(
        output, where=f"uvicorn access-log output:\n{output}"
    )


def test_telemetry_is_flushed_when_the_process_exits_on_a_base_exception(
    tmp_path: Path, otlp: tuple[OtlpReceiver, str]
) -> None:
    """`SystemExit` is not an `Exception`, and the flush must survive it anyway.

    `main()` is written as `try: asyncio.run(run()) finally: telemetry.shutdown()`,
    which flushes on every exit. A reviewer named the mutation that today's tests
    do not kill:

        try:
            asyncio.run(run())
        except Exception:
            telemetry.shutdown()
            raise
        telemetry.shutdown()

    Normal exit still delivers, and `test_telemetry_is_flushed_on_an_error_exit_too`
    still passes because its scenario raises `RuntimeError` — but every
    `BaseException` exit silently stops flushing. That is not an exotic path:
    `SystemExit` is exactly how a boot refusal and a signal-driven stop leave this
    process, so the mutation would drop the records of the two shutdowns an
    operator is most likely to be reading afterwards.

    Both halves are load-bearing, as in the error-exit test. The exit code proves
    the `BaseException` propagated rather than being swallowed or re-raised as
    something else; the delivered record proves `shutdown()` ran on that path,
    since the `BatchLogRecordProcessor`'s 1000 ms schedule delay outlives this
    process by far and nothing arrives without the force-flush.
    """
    receiver, base_url = otlp
    env = driver_env(tmp_path, "base_exception_exit", OTEL_EXPORTER_OTLP_ENDPOINT=base_url)

    code, output = run_driver(env)

    assert code == 3, (
        f"the driver did not exit with the scenario's SystemExit code, so the "
        f"BaseException was swallowed or replaced; got {code}. Output:\n{output}"
    )
    wait_for_export(
        receiver,
        where="a log record exported on the SystemExit path — none arrived, so "
        "the flush is guarded by `except Exception` rather than `finally`. "
        f"Output:\n{output}",
    )

    exported = receiver.all_bytes()
    assert_carrier_survived_and_secret_did_not(
        exported.decode("utf-8", errors="replace"), where="the SystemExit path's export"
    )
    assert PLANTED_CHANNEL_TOKEN not in output, (
        f"the planted token leaked into process output on the SystemExit path:\n{output}"
    )
