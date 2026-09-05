"""Shared telemetry: the process's logs are JSON and pass the redaction filter.

The adapter is the first-party service that holds `AGENTMAIL_API_KEY`,
`CURIE_CHANNEL_TOKEN` and `CURIE_EGRESS_SECRET` and reads whole email bodies, and
until #2331 it called `logging.basicConfig` and therefore sat outside
`curie_telemetry.redact.RedactingLogFilter` — the one filter that strips PEM
keys, JWTs and URL secret params from every other workload's log lines. "The
process's logs are redacted" is a property of the *process*, so every test here
drives the real `python -m curie_mail_adapter` entry point through `_support` and
reads its actual merged stdout+stderr. Calling `configure_service_logging` or
asserting on a caplog record would prove nothing about the shipped image.

Two things a later author needs to know before adding to this file.

**`REDACTION_RULES` now rewrites home paths.** The `home_path` rule matches
`/(?:home|Users)/[^/\\s]+`, so any assertion on a `/home/...` path in adapter
subprocess output will see `[REDACTED:home_path]` rather than the path. No
existing assertion is at risk: `_support.adapter_env` puts
`CURIE_MAIL_STATE_PATH` under `tempfile`'s root, which is `/tmp` on this
project's Linux images and CI, not under `$HOME`. `PLANTED_JWT_BASE_URL` below
hardcodes `/tmp` for the same reason — resolving the temp root at runtime would
make the planted JWT redact as a home path on a box whose `TMPDIR` lives under
`/home`, and the test would pass for the wrong reason.

**The adapter deliberately logs no operator- or provider-supplied string.** Every
one of its ~40 log call sites passes a status code, a count, a fixed label, or a
one-way `_correlation` token; that is the "logs carry no raw mail PII" invariant
in `apps/mail-adapter/CLAUDE.md` doing its job. The single exception is
`adapter.py`'s `poll: list failed with status=%s cause=%s`, whose cause is
`_transport_cause`'s rendering of a locally-synthesized `{"error": str(OSError)}`
— and that helper's own docstring names the redaction filter as its defence in
depth. So that line is the only real lever for planting a secret, and the first
test below is built on it.

No test here asserts on adapter-authored spans: this change wires the bootstrap
and the redacting handler only and adds no `operation_span` instrumentation.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from _support import (
    IngressState,
    MailState,
    adapter_env,
    exit_of,
    free_port,
    post_raw,
    spawn_adapter,
    stop,
    wait_for_healthz,
    wait_for_readyz,
    wait_until,
)

SERVICE_NAME = "curie-mail-adapter"

# Matches `redact.py`'s `jwt` rule (`\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.…`) and
# is not one of the three credentials `_transport_cause` blanks itself, so it
# reaches the log line intact and only the shared filter can remove it.
PLANTED_JWT = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.c2ln"

# A `file://` base URL whose directory does not exist. `agentmail.request` catches
# the resulting `OSError` and reports status 0 with `{"error": str(exc)}`, and
# urllib's message for a missing file — unlike its message for a refused TCP
# connection or an unresolvable host, both of which were measured and carry no
# URL at all — embeds the full path. That is what carries the JWT into the log.
#
# The `/tmp/<jwt>` shape is load-bearing twice over: `/tmp` keeps `home_path` from
# firing first, and the short prefix keeps the rendered cause inside
# `adapter.CAUSE_MAX_CHARS` (120) so the JWT is complete when the filter sees it.
PLANTED_JWT_BASE_URL = f"file:///tmp/{PLANTED_JWT}"

# The literal prefix of that same log record. Asserting it survives is what stops
# the redaction assertions passing vacuously when the line is simply absent.
POLL_FAILURE_PREFIX = "poll: list failed with status=0 cause="


def json_records(output: str) -> list[dict[str, object]]:
    """Every line of process output, parsed as a service log record.

    Strict on purpose: `bootstrap_service_telemetry` is the first statement in
    `main()`, so nothing in this process may legitimately reach stderr before the
    JSON handler is installed. A plain-text line here means a logger escaped the
    `curie_mail_adapter` package — exactly the regression this file exists to
    catch — so it is a failure, not something to filter out.
    """
    records = []
    for line in output.splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except ValueError:  # pragma: no cover - only on a regression
            raise AssertionError(
                f"a line of adapter output was not JSON, so it bypassed the "
                f"service logger:\n{line!r}\n\nfull output:\n{output}"
            ) from None
        assert isinstance(record, dict), f"a log line was not a JSON object: {line!r}"
        records.append(record)
    return records


def test_a_planted_secret_in_an_adapter_log_line_is_redacted_in_process_output(
    mail: MailState, ingress: IngressState, tmp_path: Path
) -> None:
    """The AC: a secret that reaches a log line leaves the process redacted.

    Without the shared filter the adapter writes whatever `str(OSError)` handed
    it straight into the cluster's log retention. urllib's rendering carries no
    credential *today*, which is precisely why `_transport_cause` documents the
    filter as defence in depth rather than relying on that staying true.

    Two phases, because the cause is only rendered on the restart/steady-state
    path: `prime()` logs a bare status and retries forever on a fresh store, so
    the first run must complete its prime against the working provider and the
    second must reuse that store with the poisoned base URL.
    """
    state_path = str(tmp_path / "mail-state.sqlite3")
    assert not os.path.exists(f"/tmp/{PLANTED_JWT}"), (
        "the planted path must not exist, or the provider call succeeds and the "
        "log record under test is never emitted"
    )

    primed_port = free_port()
    primed = spawn_adapter(
        adapter_env(
            agentmail_base_url=mail.base_url,
            api_url=ingress.url,
            port=primed_port,
            CURIE_MAIL_STATE_PATH=state_path,
        )
    )
    try:
        wait_for_readyz(primed_port, primed)
    finally:
        stop(primed)

    port = free_port()
    proc = spawn_adapter(
        adapter_env(
            agentmail_base_url=PLANTED_JWT_BASE_URL,
            api_url=ingress.url,
            port=port,
            CURIE_MAIL_STATE_PATH=state_path,
        )
    )
    try:
        wait_for_healthz(port, proc)
        # The restart confirmation's first listing runs synchronously at startup
        # and fails on a local stat, so the record is emitted almost immediately;
        # this only bounds the wait. If it were ever missed, the negative control
        # below fails rather than the test passing on an empty output.
        wait_until(lambda: proc.poll() is not None, 2.0)
    finally:
        output = stop(proc)

    # Negative control first: the same message, minus its secret, must be present,
    # or the two assertions after it are satisfied by output that never had the
    # secret in it to begin with.
    assert POLL_FAILURE_PREFIX in output, (
        f"the log record carrying the planted secret was never emitted, so the "
        f"redaction assertions would pass vacuously; output:\n{output}"
    )
    assert PLANTED_JWT not in output, f"the planted JWT survived into output:\n{output}"
    assert "[REDACTED:jwt]" in output, (
        f"the record was emitted but carries no jwt placeholder, so it did not "
        f"pass RedactingLogFilter; output:\n{output}"
    )


def test_boot_with_no_otel_env_present_still_serves(mail: MailState, ingress: IngressState) -> None:
    """No OTLP endpoint is a no-op, not a boot failure.

    The bootstrap runs before `MailAdapterConfig()`, so a raise inside it would
    replace a precise "AGENTMAIL_INBOX is required" crash with an opaque OTel
    stack trace — and every local, offline and CI install runs with no endpoint
    at all. This is also the property the chart's `otelCollector.deploy=false`
    case leans on: providers come back `None` and only the redacting stderr
    handler is installed.
    """
    port = free_port()
    env = adapter_env(agentmail_base_url=mail.base_url, api_url=ingress.url, port=port)
    assert not [name for name in env if name.startswith("OTEL_")], (
        "adapter_env is a closed world; an OTEL_* key here would mean this test "
        "is exercising the configured path instead of the unconfigured one"
    )

    proc = spawn_adapter(env)
    try:
        wait_for_healthz(port, proc)

        assert proc.poll() is None
    finally:
        stop(proc)


def test_a_boot_problem_still_names_its_variable_in_json_output(
    mail: MailState, ingress: IngressState
) -> None:
    """`CrashLoopBackOff` naming the variable is the operator signal.

    The format change from `basicConfig` plain text to single-line JSON must not
    cost the operator the one thing the boot gates exist to give them. Asserted
    as the structured property rather than a substring, so a record that merely
    happens to contain the name somewhere in an unparseable line does not pass.
    """
    env = adapter_env(agentmail_base_url=mail.base_url, api_url=ingress.url, port=free_port())
    del env["CURIE_CHANNEL_TOKEN"]

    code, output = exit_of(spawn_adapter(env))

    assert code != 0
    records = json_records(output)
    assert [
        record
        for record in records
        if record.get("severity") == "ERROR"
        and record.get("service.name") == SERVICE_NAME
        and "CURIE_CHANNEL_TOKEN" in str(record.get("message", ""))
    ], f"no ERROR record named the missing variable; records:\n{records}"


def test_the_service_logger_owns_the_three_module_loggers(
    mail: MailState, ingress: IngressState
) -> None:
    """Bootstrapping the package logger has to cover every module under it.

    `run`, `adapter` and `egress` each hold their own `logging.getLogger(__name__)`
    and are only reached because `Logger.callHandlers` walks up to the
    `curie_mail_adapter` package logger. A future module named outside that
    package, or a stray `basicConfig` restoring a plain-text root handler, shows
    up here as a line that is not JSON or a record without the service name.
    """
    port = free_port()
    proc = spawn_adapter(
        adapter_env(agentmail_base_url=mail.base_url, api_url=ingress.url, port=port)
    )
    try:
        wait_for_readyz(port, proc)
        # Drive `egress` too: an unauthenticated POST is refused with a warning
        # from that module, so all three loggers have emitted before shutdown.
        status, _ = post_raw(f"http://127.0.0.1:{port}/healthz", secret=None)
        assert status == 401
    finally:
        output = stop(proc)

    records = json_records(output)
    assert records, f"the adapter emitted no log records at all:\n{output}"
    for record in records:
        assert record.get("service.name") == SERVICE_NAME, (
            f"a record escaped the service logger: {record}"
        )
    loggers = {str(record.get("logger")) for record in records}
    assert {
        "curie_mail_adapter.run",
        "curie_mail_adapter.adapter",
        "curie_mail_adapter.egress",
    } <= loggers, (
        f"fewer than the three module loggers were observed, so this proves "
        f"nothing about coverage; saw {sorted(loggers)}"
    )
