"""Substrate selection + the local-middle-mode fail-closed credential gate.

Local middle mode (Docker substrate) defaults to a real model; fake model is an
explicit opt-in. A Docker worker with neither a model credential nor
CURIE_FAKE_MODEL must fail loudly instead of silently degrading to a fake.
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
import redis
import redis.asyncio
from curie_worker import run
from curie_worker.config import WorkerConfig
from curie_worker.run import (
    _MAX_TUNABLE_SECONDS,
    _sandbox_client,
    _substrate_config,
    _supervise,
    main,
)
from curie_worker.sandbox import DockerSandboxClient, SubstrateConfig

_SUB = SubstrateConfig(namespace="default", warm_pool="pool")

# A non-secret placeholder credential. The docker fail-closed gate only checks
# that a credential env var is PRESENT, so the value is irrelevant; keep it an
# obvious placeholder behind a named constant so the secret scanner never
# mistakes it for a real token.
_FAKE_SDK_CRED = "oauth-PLACEHOLDER"


def test_substrate_config_claim_timeout_defaults_to_90s() -> None:
    assert _substrate_config({}).claim_timeout_seconds == 90.0


def test_substrate_config_claim_timeout_reads_env() -> None:
    cfg = _substrate_config({"CURIE_CLAIM_TIMEOUT_SECONDS": "45"})
    assert cfg.claim_timeout_seconds == 45.0


def test_substrate_config_route_ttls_default_unchanged() -> None:
    # Exposing these must not change behaviour for anyone who sets nothing.
    cfg = _substrate_config({})
    assert cfg.route_ttl_seconds == 3600
    assert cfg.suspended_route_ttl_seconds == 86400


def test_substrate_config_route_ttl_reads_env() -> None:
    cfg = _substrate_config({"CURIE_ROUTE_TTL_SECONDS": "300"})
    assert cfg.route_ttl_seconds == 300


def test_substrate_config_suspended_route_ttl_reads_env() -> None:
    cfg = _substrate_config({"CURIE_SUSPENDED_ROUTE_TTL_SECONDS": "7200"})
    assert cfg.suspended_route_ttl_seconds == 7200


def test_route_ttl_override_is_independent_of_claim_timeout() -> None:
    # The regression this whole change exists for (#1380): an operator could
    # reach the DEADLINE but not the ACCUMULATION term, so the only available
    # lever made a doomed turn fail slower instead of reducing how many
    # sandboxes were alive. Setting one must not disturb the other.
    cfg = _substrate_config(
        {"CURIE_ROUTE_TTL_SECONDS": "300", "CURIE_CLAIM_TIMEOUT_SECONDS": "45"}
    )
    assert cfg.route_ttl_seconds == 300
    assert cfg.claim_timeout_seconds == 45.0
    assert cfg.suspended_route_ttl_seconds == 86400


def test_claim_timeout_default_stays_under_lock_ttl() -> None:
    # The claim is the dominant term in the per-thread critical section; it must
    # stay below the lock TTL so the lock never lapses mid-claim.
    assert _SUB.claim_timeout_seconds < WorkerConfig().lock_ttl_ms / 1000


def test_valkey_socket_timeout_exceeds_the_block_interval() -> None:
    # redis-py enforces the client socket_timeout on the blocking XREADGROUP, so
    # it must sit above read_block_ms or every idle read raises a timeout instead
    # of returning empty (log flood). Guard the invariant that keeps idle reads
    # quiet across any read_block_ms tuning.
    cfg = WorkerConfig()
    assert cfg.valkey_socket_timeout_s > cfg.read_block_ms / 1000


# --- #1388: the three operator-tunable seconds knobs are bounded at both ends ---
#
# Every assertion below goes through _substrate_config, the loader build() calls
# at run.py:180 -- never through whatever private helper implements the bound.
# A test bound to that helper would still pass if the helper were disconnected
# from the loader, which is the regression these exist to catch.

# The documented ceiling, 365 days. Duplicated as a JSON Schema literal in
# charts/curie/values.schema.json; the accept/reject pair below is one half of
# the four-assertion net that keeps the two literals from drifting apart (the
# chart half is assertions (e) and (h) of
# charts/curie/ci/worker-ttl-bounds-assertions.sh).
_MAX_SECONDS = "31536000"
_OVER_MAX_SECONDS = "31536001"


def test_substrate_config_defaults_unchanged_by_the_bounds_guard() -> None:
    # Full frozen-dataclass equality rather than field-by-field: it pins EVERY
    # field at once, so bounding the three knobs cannot quietly move a default
    # for the installs that set nothing.
    assert _substrate_config({}) == SubstrateConfig(
        namespace="default", warm_pool="curie-runner-pool"
    )


@pytest.mark.parametrize(
    ("var", "raw"),
    [
        ("CURIE_ROUTE_TTL_SECONDS", "0"),
        ("CURIE_ROUTE_TTL_SECONDS", "-1"),
        ("CURIE_ROUTE_TTL_SECONDS", "-5"),
        ("CURIE_SUSPENDED_ROUTE_TTL_SECONDS", "0"),
        ("CURIE_SUSPENDED_ROUTE_TTL_SECONDS", "-1"),
        ("CURIE_SUSPENDED_ROUTE_TTL_SECONDS", "-5"),
        ("CURIE_CLAIM_TIMEOUT_SECONDS", "0"),
        ("CURIE_CLAIM_TIMEOUT_SECONDS", "-1"),
    ],
)
def test_substrate_config_refuses_a_non_positive_knob(var: str, raw: str) -> None:
    # A non-positive value boots healthy today and fails on the FIRST message,
    # unclassified: route_ttl 0 makes AffinityStore.put_if_absent issue
    # SET ... NX EX 0, which real Valkey refuses. The refusal must name the env
    # var so an operator can act on it without reading the traceback.
    with pytest.raises(ValueError) as exc:
        _substrate_config({var: raw})
    assert var in str(exc.value)
    # repr(raw), not raw: the bare "0" params are a substring of the 31536000 in
    # the message's range text, so `raw in ...` would pass against a message that
    # never echoed the operator's value at all. The message formats with {raw!r},
    # so the quoted form is exact for every param and appears nowhere else.
    assert repr(raw) in str(exc.value)


@pytest.mark.parametrize(
    "var",
    [
        "CURIE_ROUTE_TTL_SECONDS",
        "CURIE_SUSPENDED_ROUTE_TTL_SECONDS",
        "CURIE_CLAIM_TIMEOUT_SECONDS",
    ],
)
def test_substrate_config_refuses_above_the_documented_maximum(var: str) -> None:
    with pytest.raises(ValueError) as exc:
        _substrate_config({var: _OVER_MAX_SECONDS})
    assert var in str(exc.value)
    assert _OVER_MAX_SECONDS in str(exc.value)


@pytest.mark.parametrize(
    ("var", "field", "expected"),
    [
        ("CURIE_ROUTE_TTL_SECONDS", "route_ttl_seconds", 31536000),
        ("CURIE_SUSPENDED_ROUTE_TTL_SECONDS", "suspended_route_ttl_seconds", 31536000),
        ("CURIE_CLAIM_TIMEOUT_SECONDS", "claim_timeout_seconds", 31536000.0),
    ],
)
def test_substrate_config_accepts_exactly_the_documented_maximum(
    var: str, field: str, expected: float
) -> None:
    # The accept side of the boundary. Paired with the refusal above, it proves
    # the bound is inclusive at 31536000 and not off by one, and it is what stops
    # the Python constant and the chart schema's `maximum` from drifting apart.
    assert getattr(_substrate_config({var: _MAX_SECONDS}), field) == expected


def test_chart_schema_bounds_match_the_python_bound() -> None:
    # The direct pin on the cross-language seam AGENTS.md names: a deploy-time
    # validator and the runtime loader that re-parses the same value. The
    # behavioural boundary pairs above and assertions (e)/(h) of
    # charts/curie/ci/worker-ttl-bounds-assertions.sh read the seam from two
    # ends; this reads the chart's own JSON and compares the literals, so
    # moving either side alone fails here rather than shipping a helm that
    # accepts a value the worker refuses at boot (or the reverse).
    #
    # Resolved from this file's location, not the working directory, so it
    # holds whether pytest runs from the repo root or from apps/worker.
    repo_root = Path(__file__).resolve().parents[3]
    schema_path = repo_root / "charts" / "curie" / "values.schema.json"
    worker = json.loads(schema_path.read_text())["properties"]["worker"]["properties"]
    drift = (
        f"{schema_path} and _MAX_TUNABLE_SECONDS in curie_worker/run.py have "
        f"drifted apart. They are the same bound expressed in two languages and "
        f"must move together: helm refuses the value at install time, the worker "
        f"refuses it at boot, and changing one alone makes a chart the worker "
        f"will not start under."
    )
    for knob in ("claimTimeoutSeconds", "routeTtlSeconds", "suspendedRouteTtlSeconds"):
        maximum = worker[knob]["maximum"]
        exclusive_minimum = worker[knob]["exclusiveMinimum"]
        # isinstance(True, int) is True in Python, so a draft-4 style rewrite of
        # the schema ("minimum": 0, "exclusiveMinimum": false -- which under
        # draft-4 semantics legalizes the value 0, exactly what this branch
        # exists to forbid) would satisfy `== 0` / `== _MAX_TUNABLE_SECONDS`
        # without this bool exclusion, since False == 0 and (depending on the
        # rewrite) True could stand in for a truthy bound. Require a real
        # number, not a bool wearing one.
        assert (
            isinstance(exclusive_minimum, (int, float))
            and not isinstance(exclusive_minimum, bool)
            and exclusive_minimum == 0
        ), f"{knob}: {drift}"
        assert (
            isinstance(maximum, (int, float))
            and not isinstance(maximum, bool)
            and maximum == _MAX_TUNABLE_SECONDS
        ), f"{knob}: {drift}"


@pytest.mark.parametrize("raw", ["abc", "3600.5", ""])
def test_substrate_config_refuses_an_unparseable_ttl_naming_the_env_var(raw: str) -> None:
    # int("abc") already raises ValueError today, so `pytest.raises(ValueError)`
    # alone would pass against the unguarded loader. The env-var-name assertion
    # is the load-bearing half: the bare message
    # "invalid literal for int() with base 10: 'abc'" names nothing an operator
    # can act on. The "" case is the only gate on the helm explicit-null path --
    # `--set worker.routeTtlSeconds=null` renders CURIE_ROUTE_TTL_SECONDS present
    # with an empty value, which the chart schema structurally cannot see (pinned
    # from the other end by assertion (i) of
    # charts/curie/ci/worker-ttl-bounds-assertions.sh).
    with pytest.raises(ValueError) as exc:
        _substrate_config({"CURIE_ROUTE_TTL_SECONDS": raw})
    assert "CURIE_ROUTE_TTL_SECONDS" in str(exc.value)


@pytest.mark.parametrize("raw", ["inf", "-inf", "nan", "1e400"])
def test_substrate_config_refuses_a_non_finite_claim_timeout(raw: str) -> None:
    # The float-only failure mode a positivity check alone misses. float("inf")
    # (and float("1e400"), which overflows to inf) makes
    # `deadline = time.monotonic() + claim_timeout_seconds` at
    # sandbox/substrate.py:217 never elapse, so the claim wait spins forever
    # inside the per-thread lock. float("nan") is the mirror image: every `<`
    # comparison against it is False, so the wait loop never runs and every
    # claim fails instantly. Neither is caught by `value > 0`.
    with pytest.raises(ValueError) as exc:
        _substrate_config({"CURIE_CLAIM_TIMEOUT_SECONDS": raw})
    assert "CURIE_CLAIM_TIMEOUT_SECONDS" in str(exc.value)
    # The range check rejects all four on its own (inf fails the upper bound;
    # -inf and nan fail `0 < value`), so `pytest.raises(ValueError)` plus the
    # env-var name would pass with the non-finite branch deleted. Pinning the
    # word "finite" is the only observable difference between the two
    # implementations, so it is what makes this test discriminate.
    assert "finite" in str(exc.value)
    assert repr(raw) in str(exc.value)


@pytest.mark.parametrize(
    "var",
    [
        "CURIE_ROUTE_TTL_SECONDS",
        "CURIE_SUSPENDED_ROUTE_TTL_SECONDS",
        "CURIE_CLAIM_TIMEOUT_SECONDS",
    ],
)
def test_substrate_config_refuses_a_huge_integer_literal(var: str) -> None:
    # An operator typo of the leaning-on-the-zero-key kind. int() accepts an
    # arbitrarily large literal, and math.isfinite() converts its argument to a
    # C double, so on the int knobs an unnarrowed isfinite() raises
    # OverflowError -- an ArithmeticError, not a ValueError -- which escapes the
    # loader entirely and reaches the operator as "int too large to convert to
    # float", naming no env var and no value. The refusal must stay a ValueError
    # that names the knob, the same as every other out-of-range input.
    raw = "9" * 400
    with pytest.raises(ValueError) as exc:
        _substrate_config({var: raw})
    assert var in str(exc.value)
    assert repr(raw) in str(exc.value)


def test_docker_without_credential_or_fake_fails_loudly() -> None:
    with pytest.raises(SystemExit) as exc:
        _sandbox_client(WorkerConfig(), {"CURIE_SANDBOX_SUBSTRATE": "docker"}, _SUB)
    msg = str(exc.value)
    assert "CLAUDE_CODE_OAUTH_TOKEN" in msg  # tells the user how to fix it
    assert "CURIE_FAKE_MODEL" in msg


def test_docker_with_sdk_credential_builds_docker_client(monkeypatch) -> None:
    # Keep hermetic: after Stream B, _sandbox_client prewarms the image via
    # DockerSandboxClient.ensure_image; stub it so this test never shells docker.
    monkeypatch.setattr(
        DockerSandboxClient, "ensure_image", lambda self: None, raising=False
    )
    client = _sandbox_client(
        WorkerConfig(),
        {"CURIE_SANDBOX_SUBSTRATE": "docker", "CLAUDE_CODE_OAUTH_TOKEN": _FAKE_SDK_CRED},
        _SUB,
    )
    assert isinstance(client, DockerSandboxClient)


def test_docker_with_curie_credentials_reference_builds_docker_client(monkeypatch) -> None:
    # CURIE_CREDENTIALS alone is a valid credential: forwarded by name and
    # mapped onto an SDK var by the runner, so the gate must accept it.
    monkeypatch.setattr(
        DockerSandboxClient, "ensure_image", lambda self: None, raising=False
    )
    client = _sandbox_client(
        WorkerConfig(credentials="sk-ant-PLACEHOLDER"),
        {"CURIE_SANDBOX_SUBSTRATE": "docker"},
        _SUB,
    )
    assert isinstance(client, DockerSandboxClient)


def test_docker_with_model_base_url_builds_docker_client_without_credential(monkeypatch) -> None:
    monkeypatch.setattr(
        DockerSandboxClient, "ensure_image", lambda self: None, raising=False
    )
    client = _sandbox_client(
        WorkerConfig(model_base_url="http://ollama:11434"),
        {"CURIE_SANDBOX_SUBSTRATE": "docker"},
        _SUB,
    )
    assert isinstance(client, DockerSandboxClient)


def test_docker_with_explicit_fake_model_builds_docker_client(monkeypatch) -> None:
    monkeypatch.setattr(
        DockerSandboxClient, "ensure_image", lambda self: None, raising=False
    )
    client = _sandbox_client(
        WorkerConfig(fake_model=True), {"CURIE_SANDBOX_SUBSTRATE": "docker"}, _SUB
    )
    assert isinstance(client, DockerSandboxClient)


def test_docker_without_otlp_endpoint_warns(caplog) -> None:
    # Docker substrate exports runner traces via OTLP; without an endpoint the
    # traces silently go nowhere, so the boot must warn (not fail).
    with caplog.at_level(logging.WARNING, logger="curie_worker.run"):
        client = _sandbox_client(
            WorkerConfig(fake_model=True), {"CURIE_SANDBOX_SUBSTRATE": "docker"}, _SUB
        )
    assert isinstance(client, DockerSandboxClient)
    warnings = [
        r for r in caplog.records
        if r.name == "curie_worker.run" and "runner OTLP endpoint" in r.getMessage()
    ]
    assert warnings and all(r.levelno == logging.WARNING for r in warnings)


def test_docker_with_otlp_endpoint_does_not_warn(caplog) -> None:
    with caplog.at_level(logging.WARNING, logger="curie_worker.run"):
        client = _sandbox_client(
            WorkerConfig(fake_model=True),
            {
                "CURIE_SANDBOX_SUBSTRATE": "docker",
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector:4318",
            },
            _SUB,
        )
    assert isinstance(client, DockerSandboxClient)
    assert not [
        r for r in caplog.records
        if r.name == "curie_worker.run" and "runner OTLP endpoint" in r.getMessage()
    ]


@pytest.mark.parametrize(
    ("runner_endpoint", "expected_endpoint"),
    [
        ("http://otel-collector:4318", "http://otel-collector:4318"),
        ("", None),
        (None, "http://collector.example.com:4318"),
    ],
)
def test_docker_runner_uses_its_network_specific_otlp_endpoint(
    monkeypatch, runner_endpoint: str | None, expected_endpoint: str | None
) -> None:
    calls: list[list[str]] = []

    def capture_docker(
        _self: DockerSandboxClient,
        args: list[str],
        *,
        request_timeout_seconds: float,
    ) -> str:
        assert request_timeout_seconds > 0
        calls.append(args)
        return ""

    monkeypatch.setattr(DockerSandboxClient, "ensure_image", lambda self: None)
    monkeypatch.setattr(DockerSandboxClient, "_docker", capture_docker)
    env = {
        "CURIE_SANDBOX_SUBSTRATE": "docker",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector.example.com:4318",
    }
    if runner_endpoint is not None:
        env["CURIE_RUNNER_OTEL_EXPORTER_OTLP_ENDPOINT"] = runner_endpoint
    client = _sandbox_client(
        WorkerConfig(fake_model=True),
        env,
        _SUB,
    )
    assert isinstance(client, DockerSandboxClient)
    client.create_claim("acme-sandbox", pool="pool", env={"CURIE_FAKE_MODEL": "1"})
    assert len(calls) == 1
    assert not any(
        arg.startswith("CURIE_RUNNER_OTEL_EXPORTER_OTLP_ENDPOINT=") for arg in calls[0]
    )
    endpoint_args = [arg for arg in calls[0] if arg.startswith("OTEL_EXPORTER_OTLP_ENDPOINT=")]
    assert endpoint_args == (
        [f"OTEL_EXPORTER_OTLP_ENDPOINT={expected_endpoint}"] if expected_endpoint else []
    )


def test_sandbox_client_docker_prepulls_image(monkeypatch) -> None:
    # _sandbox_client must prewarm the runner image exactly once at startup,
    # inside the docker branch, so the first claim is not gated on a cold pull.
    calls: list[object] = []
    monkeypatch.setattr(
        DockerSandboxClient,
        "ensure_image",
        lambda self: calls.append(self),
        raising=False,
    )
    _sandbox_client(
        WorkerConfig(),
        {"CURIE_SANDBOX_SUBSTRATE": "docker", "CLAUDE_CODE_OAUTH_TOKEN": _FAKE_SDK_CRED},
        _SUB,
    )
    assert len(calls) == 1


def test_main_installs_dead_letter_alerting(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    source_logger = logging.getLogger("curie_worker.consumer")
    original_handlers = list(source_logger.handlers)
    original_propagate = source_logger.propagate
    for handler in original_handlers:
        source_logger.removeHandler(handler)
    source_logger.propagate = False

    captured_coroutines: list[Any] = []

    def capture_run(coroutine: Any) -> None:
        captured_coroutines.append(coroutine)

    monkeypatch.setattr(asyncio, "run", capture_run)
    caplog.clear()

    try:
        with caplog.at_level(logging.ERROR):
            main({})
            source_logger.error(
                "dead-lettered entry %s after %d deliveries (reason=%s) -> %s",
                "1730000000000-0",
                2,
                "max-delivery-exceeded",
                "curie:runs:dead",
            )

        assert len(captured_coroutines) == 1
        alerts = [
            record
            for record in caplog.records
            if record.name == "curie_worker.alerts.dead_letter"
            and record.levelno == logging.CRITICAL
        ]
        assert len(alerts) == 1, f"expected one dead letter alert, got {alerts}"
    finally:
        for coroutine in captured_coroutines:
            coroutine.close()
        for handler in list(source_logger.handlers):
            source_logger.removeHandler(handler)
        for handler in original_handlers:
            source_logger.addHandler(handler)
        source_logger.propagate = original_propagate


# -- _supervise: per-task restart + sibling isolation (#673) -----------------


def test_supervise_restarts_a_crashing_task_until_shutdown(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A consumer that crashes is restarted rather than allowed to escape. The
    task settles once it returns cleanly (its own stop was requested)."""

    async def go() -> None:
        shutdown = asyncio.Event()
        calls = {"n": 0}

        async def factory() -> None:
            calls["n"] += 1
            if calls["n"] < 3:
                raise ConnectionError("boom")
            # Third run behaves like a real consumer: return once stopped.
            shutdown.set()

        with caplog.at_level(logging.ERROR, logger="curie_worker.run"):
            await asyncio.wait_for(
                _supervise("evals", factory, shutdown, restart_backoff_s=0),
                timeout=2,
            )

        assert calls["n"] == 3  # crashed twice, restarted twice, then returned
        restarts = [
            r
            for r in caplog.records
            if r.name == "curie_worker.run" and "crashed; restarting" in r.getMessage()
        ]
        assert len(restarts) == 2

    asyncio.run(go())


def test_supervise_does_not_restart_after_shutdown() -> None:
    """A crash arriving as shutdown is requested must not trigger a restart, and
    must not propagate out of the supervisor."""

    async def go() -> None:
        shutdown = asyncio.Event()
        calls = {"n": 0}

        async def factory() -> None:
            calls["n"] += 1
            shutdown.set()
            raise ConnectionError("boom")

        await asyncio.wait_for(
            _supervise("runs", factory, shutdown, restart_backoff_s=0),
            timeout=2,
        )
        assert calls["n"] == 1  # no restart after shutdown

    asyncio.run(go())


def test_supervise_returns_when_task_completes_cleanly() -> None:
    async def go() -> None:
        shutdown = asyncio.Event()
        calls = {"n": 0}

        async def factory() -> None:
            calls["n"] += 1

        await asyncio.wait_for(
            _supervise("heartbeat", factory, shutdown, restart_backoff_s=0),
            timeout=2,
        )
        assert calls["n"] == 1  # returned on first clean completion, no restart

    asyncio.run(go())


def test_crashing_supervised_task_does_not_cancel_its_siblings() -> None:
    """The #673 core: one consumer crashing (and restarting) must not tear down a
    sibling consumer sharing the same event loop under the top-level gather."""

    async def go() -> None:
        shutdown = asyncio.Event()
        sibling_ran = {"ok": False}
        crashes = {"n": 0}

        async def crasher() -> None:
            crashes["n"] += 1
            if crashes["n"] >= 3:
                shutdown.set()  # last crash also asks everything to stop
            raise ConnectionError("boom")

        async def sibling() -> None:
            # Only completes if it was NOT cancelled by the crasher's failure.
            await shutdown.wait()
            sibling_ran["ok"] = True

        await asyncio.wait_for(
            asyncio.gather(
                _supervise("crasher", crasher, shutdown, restart_backoff_s=0),
                _supervise("sibling", sibling, shutdown, restart_backoff_s=0),
                return_exceptions=True,
            ),
            timeout=2,
        )

        assert crashes["n"] == 3
        assert sibling_ran["ok"] is True

    asyncio.run(go())


def test_supervise_emits_restart_metric_and_cause_for_publications(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorded: list[tuple[str, float, dict[str, str]]] = []

    def capture(
        name: str, value: float = 1, *, attributes: Mapping[str, str] | None = None
    ) -> None:
        recorded.append((name, value, dict(attributes or {})))

    monkeypatch.setattr(run, "record_metric", capture)

    async def go() -> None:
        shutdown = asyncio.Event()
        calls = {"n": 0}

        async def factory() -> None:
            calls["n"] += 1
            if calls["n"] < 3:
                raise RuntimeError("claim cas lost")
            shutdown.set()

        with caplog.at_level(logging.ERROR, logger="curie_worker.run"):
            await asyncio.wait_for(
                _supervise("publications", factory, shutdown, restart_backoff_s=0),
                timeout=2,
            )

        assert calls["n"] == 3
        expected = {
            "service.name": "curie-worker",
            "operation": "publications",
            "outcome": "restart",
        }
        assert recorded == [
            ("curie.worker.supervised.restart", 1, expected),
            ("curie.worker.supervised.restart", 1, expected),
        ]
        restarts = [
            r
            for r in caplog.records
            if r.name == "curie_worker.run" and "crashed; restarting" in r.getMessage()
        ]
        assert len(restarts) == 2
        for rec in restarts:
            message = rec.getMessage()
            assert "publications" in message
            assert "RuntimeError" in message
            assert "claim cas lost" in message

    asyncio.run(go())


def test_supervise_emits_restart_metric_for_evals(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorded: list[tuple[str, float, dict[str, str]]] = []

    def capture(
        name: str, value: float = 1, *, attributes: Mapping[str, str] | None = None
    ) -> None:
        recorded.append((name, value, dict(attributes or {})))

    monkeypatch.setattr(run, "record_metric", capture)

    async def go() -> None:
        shutdown = asyncio.Event()
        calls = {"n": 0}

        async def factory() -> None:
            calls["n"] += 1
            if calls["n"] < 3:
                raise RuntimeError("eval stream closed")
            shutdown.set()

        with caplog.at_level(logging.ERROR, logger="curie_worker.run"):
            await asyncio.wait_for(
                _supervise("evals", factory, shutdown, restart_backoff_s=0),
                timeout=2,
            )

        assert calls["n"] == 3
        expected = {
            "service.name": "curie-worker",
            "operation": "evals",
            "outcome": "restart",
        }
        assert recorded == [
            ("curie.worker.supervised.restart", 1, expected),
            ("curie.worker.supervised.restart", 1, expected),
        ]
        restarts = [
            r
            for r in caplog.records
            if r.name == "curie_worker.run" and "crashed; restarting" in r.getMessage()
        ]
        assert len(restarts) == 2
        for rec in restarts:
            message = rec.getMessage()
            assert "evals" in message
            assert "RuntimeError" in message
            assert "eval stream closed" in message

    asyncio.run(go())


def test_supervise_idle_heartbeat_does_not_emit_restart(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorded: list[tuple[str, float, dict[str, str]]] = []

    def capture(
        name: str, value: float = 1, *, attributes: Mapping[str, str] | None = None
    ) -> None:
        recorded.append((name, value, dict(attributes or {})))

    monkeypatch.setattr(run, "record_metric", capture)

    async def go() -> None:
        shutdown = asyncio.Event()

        async def factory() -> None:
            return None

        with caplog.at_level(logging.ERROR, logger="curie_worker.run"):
            await asyncio.wait_for(
                _supervise("heartbeat", factory, shutdown, restart_backoff_s=0),
                timeout=2,
            )

        assert recorded == []
        assert not any(
            "crashed; restarting" in r.getMessage() for r in caplog.records
        )

    asyncio.run(go())


def test_supervise_does_not_emit_restart_metric_after_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: list[tuple[str, float, dict[str, str]]] = []

    def capture(
        name: str, value: float = 1, *, attributes: Mapping[str, str] | None = None
    ) -> None:
        recorded.append((name, value, dict(attributes or {})))

    monkeypatch.setattr(run, "record_metric", capture)

    async def go() -> None:
        shutdown = asyncio.Event()
        calls = {"n": 0}

        async def factory() -> None:
            calls["n"] += 1
            shutdown.set()
            raise ConnectionError("boom")

        await asyncio.wait_for(
            _supervise("runs", factory, shutdown, restart_backoff_s=0),
            timeout=2,
        )
        assert calls["n"] == 1
        assert recorded == []

    asyncio.run(go())


# -- _supervise: bounded restarts (#2637) ------------------------------------


def test_restart_delay_doubles_from_base_and_caps() -> None:
    schedule = [run._restart_delay_s(n, base_s=1.0, max_s=60.0) for n in range(1, 10)]
    assert schedule == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0, 60.0]
    # A very long streak stays at the cap rather than overflowing.
    assert run._restart_delay_s(10_000, base_s=1.0, max_s=60.0) == 60.0
    assert run._restart_delay_s(3, base_s=0.0, max_s=60.0) == 0.0


def _capture_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[str, float, dict[str, str]]]:
    recorded: list[tuple[str, float, dict[str, str]]] = []

    def capture(
        name: str, value: float = 1, *, attributes: Mapping[str, str] | None = None
    ) -> None:
        recorded.append((name, value, dict(attributes or {})))

    monkeypatch.setattr(run, "record_metric", capture)
    return recorded


def test_supervise_waits_the_exponential_schedule_between_restarts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The observed waits between restarts are the capped doubling schedule,
    not a fixed one-second backoff."""
    _capture_metrics(monkeypatch)
    waits: list[float] = []
    real_wait_for = asyncio.wait_for

    async def recording_wait_for(aw, timeout=None):  # type: ignore[no-untyped-def]
        if timeout is not None and timeout >= 1:
            waits.append(timeout)
            aw.close()
            raise TimeoutError
        return await real_wait_for(aw, timeout)

    monkeypatch.setattr(run.asyncio, "wait_for", recording_wait_for)

    async def go() -> None:
        shutdown = asyncio.Event()
        calls = {"n": 0}

        async def factory() -> None:
            calls["n"] += 1
            if calls["n"] <= 6:
                raise RuntimeError("boom")

        await real_wait_for(
            _supervise(
                "runs",
                factory,
                shutdown,
                restart_backoff_s=1.0,
                max_restart_backoff_s=8.0,
                max_consecutive_failures=0,
                clock=lambda: 0.0,
            ),
            timeout=2,
        )
        assert calls["n"] == 7

    asyncio.run(go())
    assert waits == [1.0, 2.0, 4.0, 8.0, 8.0, 8.0]


def test_supervise_parks_task_after_consecutive_failures(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After N consecutive crashes the task is parked with a terminal give_up
    event and is never started again, while shutdown was never requested."""
    recorded = _capture_metrics(monkeypatch)

    async def go() -> None:
        shutdown = asyncio.Event()
        calls = {"n": 0}

        async def factory() -> None:
            calls["n"] += 1
            raise RuntimeError("crash loop")

        with caplog.at_level(logging.ERROR, logger="curie_worker.run"):
            await asyncio.wait_for(
                _supervise(
                    "publications",
                    factory,
                    shutdown,
                    restart_backoff_s=0,
                    max_consecutive_failures=4,
                    clock=lambda: 0.0,
                ),
                timeout=2,
            )
        assert calls["n"] == 4
        assert not shutdown.is_set()

    asyncio.run(go())
    outcomes = [attrs["outcome"] for _, _, attrs in recorded]
    assert outcomes == ["restart", "restart", "restart", "give_up"]
    assert recorded[-1] == (
        "curie.worker.supervised.restart",
        1,
        {
            "service.name": "curie-worker",
            "operation": "publications",
            "outcome": "give_up",
        },
    )
    parked = [r for r in caplog.records if "parked, not restarting" in r.getMessage()]
    assert len(parked) == 1
    assert "publications" in parked[0].getMessage()
    assert "crash loop" in parked[0].getMessage()


def test_supervise_long_healthy_run_resets_the_failure_streak(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A crash after a healthy run of at least the reset window starts a new
    streak, so rare crashes over a long uptime never park the task."""
    recorded = _capture_metrics(monkeypatch)
    now = {"t": 0.0}

    async def go() -> None:
        shutdown = asyncio.Event()
        calls = {"n": 0}

        async def factory() -> None:
            calls["n"] += 1
            if calls["n"] > 6:
                return
            # Every attempt runs past the reset window before it crashes.
            now["t"] += 1000.0
            raise RuntimeError("rare")

        await asyncio.wait_for(
            _supervise(
                "runs",
                factory,
                shutdown,
                restart_backoff_s=0,
                max_consecutive_failures=2,
                failure_reset_s=300.0,
                clock=lambda: now["t"],
            ),
            timeout=2,
        )
        assert calls["n"] == 7

    asyncio.run(go())
    assert [a["outcome"] for _, _, a in recorded] == ["restart"] * 6


def test_supervise_policy_reads_worker_config() -> None:
    config = WorkerConfig(
        CURIE_WORKER_SUPERVISE_BACKOFF_BASE_S=2.0,
        CURIE_WORKER_SUPERVISE_BACKOFF_MAX_S=30.0,
        CURIE_WORKER_SUPERVISE_MAX_CONSECUTIVE_FAILURES=5,
        CURIE_WORKER_SUPERVISE_FAILURE_RESET_S=120.0,
    )
    assert run._supervise_policy(config) == {
        "restart_backoff_s": 2.0,
        "max_restart_backoff_s": 30.0,
        "max_consecutive_failures": 5,
        "failure_reset_s": 120.0,
        "boot_grace_s": run._BOOT_GRACE_S,
    }


# -- #1751: the boot rekey is CALLED by _run, once, before any consumer reads --
#
# Every other test of the legacy approval-card migration drives
# ``ApprovalCardStore.migrate_legacy_thread_keyed_refs`` directly on the store,
# so all of them would still pass with the boot call deleted from ``_run`` --
# and the whole #1751 fix would be dead in production behind a green suite.
# These four pin the call site itself: it happens, it happens BEFORE the
# consumers start (a legacy ref must be rekeyed before any resume turn is
# read), and neither a failure nor a budget overrun stops the worker booting.


class _FakeCardStore:
    """Records the boot rekey, and can fail or stall the way Valkey would.

    ``events`` is the one ordering log shared with the fake consumers, so a
    single list witnesses both that the migration ran and where it ran relative
    to the reads it must precede.
    """

    def __init__(
        self,
        events: list[str],
        *,
        raises: BaseException | None = None,
        stall_s: float = 0.0,
    ) -> None:
        self._events = events
        self._raises = raises
        self._stall_s = stall_s

    async def migrate_legacy_thread_keyed_refs(self) -> None:
        # Recorded before the stall so the ordering assertion still holds on the
        # budget-overrun path, where the call is cancelled rather than returning.
        self._events.append("migrate")
        if self._stall_s:
            await asyncio.sleep(self._stall_s)
        if self._raises is not None:
            raise self._raises


class _FakeSupervisedTask:
    """A consumer whose ``run`` records itself and returns immediately, so each
    ``_supervise`` settles on a clean completion and the top-level gather
    finishes without needing a signal to arrive."""

    def __init__(self, events: list[str], name: str) -> None:
        self._events = events
        self._name = name

    async def run(self) -> None:
        self._events.append(self._name)

    def request_stop(self) -> None:
        # Only reachable through the signal handler, which these tests never fire.
        pass


class _FakeTransport:
    """Stands in for every long-lived resource ``_run``'s finally block disposes;
    one class covers all three disposal verbs the runtime's fields use."""

    async def close(self) -> None:
        pass

    async def aclose(self) -> None:
        pass

    async def dispose(self) -> None:
        pass


class _FakeCronLoop:
    """Stands in for ``CronSchedulerLoop``. By default it records itself and
    returns, like the fake consumers; with ``wait_for_stop`` it blocks on the
    stop event ``_run`` hands it, which is how the cron test proves the loop is
    wired to the shared shutdown flag rather than to a private one."""

    def __init__(self, events: list[str], *, wait_for_stop: bool = False) -> None:
        self._events = events
        self._wait_for_stop = wait_for_stop

    async def run_forever(self, stop: asyncio.Event | None = None) -> None:
        self._events.append("cron")
        if self._wait_for_stop:
            assert stop is not None
            await stop.wait()
            self._events.append("cron-stopped")


class _FakeRuntime:
    """Exactly the attributes ``_run`` touches -- deliberately not a real
    ``Runtime``, so nothing here reaches Valkey, Postgres, or the substrate."""

    def __init__(self, card_store: _FakeCardStore, events: list[str]) -> None:
        self.card_store = card_store
        self.consumer = _FakeSupervisedTask(events, "runs")
        self.killswitch = _FakeSupervisedTask(events, "killswitch")
        self.eval_consumer = _FakeSupervisedTask(events, "evals")
        self.connector_loop = None
        self.cron_loop = _FakeCronLoop(events)
        self.runner = _FakeTransport()
        self.sink = _FakeTransport()
        self.eval_http = _FakeTransport()
        self.async_redis = _FakeTransport()
        self.pressure_async_redis = _FakeTransport()
        self.eval_redis = _FakeTransport()
        self.engine = _FakeTransport()
        self.orphan_sweeper = None


def _boot(
    monkeypatch: pytest.MonkeyPatch,
    *,
    raises: BaseException | None = None,
    stall_s: float = 0.0,
) -> list[str]:
    """Drive one full ``_run`` boot against the fakes and return the event log.

    The card store is built here rather than passed in, so it records into the
    very list the fake consumers append to: one shared log is what makes the
    ordering assertion meaningful, and a store built by the caller would write
    its "migrate" marker into a list this function never returns.

    ``build`` and ``run_heartbeat`` are the only two things stubbed: the first
    because wiring the real runtime would open sockets, the second because the
    real heartbeat never returns and would hang the gather. The migration call
    site under test is untouched.
    """
    events: list[str] = []
    card_store = _FakeCardStore(events, raises=raises, stall_s=stall_s)

    def fake_build(config: WorkerConfig, env: Any) -> _FakeRuntime:
        return _FakeRuntime(card_store, events)

    async def fake_heartbeat(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(run, "build", fake_build)
    monkeypatch.setattr(run, "run_heartbeat", fake_heartbeat)
    # The outer wait_for is a deadlock guard, not the behaviour under test: a
    # regression that leaves _run blocked must fail this suite, not hang CI.
    asyncio.run(asyncio.wait_for(run._run(WorkerConfig(), {}), timeout=10))
    return events


def test_run_supervises_the_cron_loop_until_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The cron loop is always on (#268): _run must start it under the same
    # supervisor as its siblings and hand it the shared shutdown flag, so a
    # SIGTERM that stops the consumers also stops the scheduler.
    events: list[str] = []
    card_store = _FakeCardStore(events)
    runtime = _FakeRuntime(card_store, events)
    runtime.cron_loop = _FakeCronLoop(events, wait_for_stop=True)

    async def stopping_heartbeat(_file: Any, _interval: Any, shutdown: asyncio.Event) -> None:
        # Let the cron loop start first, then request shutdown the way the
        # signal handler would.
        while "cron" not in events:
            await asyncio.sleep(0)
        shutdown.set()

    monkeypatch.setattr(run, "build", lambda config, env: runtime)
    monkeypatch.setattr(run, "run_heartbeat", stopping_heartbeat)
    asyncio.run(asyncio.wait_for(run._run(WorkerConfig(), {}), timeout=10))

    assert events.count("cron") == 1
    assert "cron-stopped" in events


def test_cron_tick_interval_reads_its_env_knob(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CURIE_CRON_TICK_INTERVAL_S", raising=False)
    assert WorkerConfig().cron_tick_interval_s == 30
    monkeypatch.setenv("CURIE_CRON_TICK_INTERVAL_S", "5")
    assert WorkerConfig().cron_tick_interval_s == 5


def test_run_migrates_legacy_card_refs_once_at_boot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _boot(monkeypatch)
    # Exactly once: it is a boot migration, not something supervised or retried,
    # and a second pass would re-walk the keyspace on every restart for nothing.
    assert events.count("migrate") == 1


def test_run_migrates_legacy_card_refs_before_the_consumers_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The ordering the whole fix rests on. A ref still under the pre-#1723 thread
    # key must be rekeyed onto its approval id BEFORE the runs consumer can read
    # a resume turn for it -- otherwise the card the turn settles is the one the
    # migration has not reached yet, which is the stranding #1751 exists to end.
    events = _boot(monkeypatch)
    assert events[0] == "migrate"
    assert "runs" in events


def test_run_boots_the_consumers_when_the_migration_fails(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A Valkey blip during a best-effort boot migration must degrade to "those
    # cards stay live until their TTL", never to a worker that will not start.
    with caplog.at_level(logging.WARNING, logger="curie_worker.run"):
        events = _boot(monkeypatch, raises=RuntimeError("valkey down"))

    assert events.count("migrate") == 1
    assert "runs" in events  # boot continued past the failure
    failures = [
        r
        for r in caplog.records
        if r.name == "curie_worker.run" and "migration failed" in r.getMessage()
    ]
    assert len(failures) == 1
    assert failures[0].levelno == logging.ERROR


def test_run_boots_the_consumers_when_the_migration_exceeds_its_budget(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The bound exists because this pass runs BEFORE the liveness heartbeat
    # starts: unbounded, a degraded Valkey stalls readiness until the exec probe
    # kills the pod, and a best-effort migration becomes a restart loop. The
    # budget is monkeypatched rather than waited out, so the test neither takes
    # 30 seconds nor hard-codes the production number.
    monkeypatch.setattr(run, "_CARD_MIGRATION_BUDGET_S", 0.01)
    with caplog.at_level(logging.WARNING, logger="curie_worker.run"):
        # Far past the patched budget; wait_for cancels it, so nothing waits 30s.
        events = _boot(monkeypatch, stall_s=30.0)

    assert events.count("migrate") == 1
    assert "runs" in events  # boot continued past the cut-short migration
    cut_short = [
        r
        for r in caplog.records
        if r.name == "curie_worker.run" and "cut short" in r.getMessage()
    ]
    assert len(cut_short) == 1
    assert cut_short[0].levelno == logging.WARNING


# --- WorkerConfig.valkey_client_kwargs: the seam shared by all three Valkey client
# constructions in build() (#2315) ------------------------------------------
#
# build() itself is not driven directly here: it constructs
# KubernetesSandboxClient, which loads a kubeconfig at __init__
# (sandbox/k8s.py:226-229), and the sync affinity client it builds is not
# reachable from Runtime anyway. valkey_client_kwargs is the seam precisely because
# all three call sites go through it, so testing it once covers all three.
#
# Assertions read redis-py's own connection-class selection (never a mock of
# the thing under test): ssl=True selects SSLConnection in both the sync and
# async namespaces, which are distinct classes sharing a name -- both are
# checked so a test cannot pass by importing the wrong one.


def test_valkey_kwargs_selects_the_plain_connection_by_default() -> None:
    kwargs = WorkerConfig().valkey_client_kwargs()
    sync_client = redis.Redis(**kwargs)
    assert sync_client.connection_pool.connection_class is redis.connection.Connection
    async_client = redis.asyncio.Redis(**kwargs)
    assert (
        async_client.connection_pool.connection_class
        is redis.asyncio.connection.Connection
    )


def test_valkey_kwargs_selects_ssl_connection_when_tls_is_set() -> None:
    kwargs = WorkerConfig(valkey_tls=True).valkey_client_kwargs()
    sync_client = redis.Redis(**kwargs)
    assert sync_client.connection_pool.connection_class is redis.connection.SSLConnection
    async_client = redis.asyncio.Redis(**kwargs)
    assert (
        async_client.connection_pool.connection_class
        is redis.asyncio.connection.SSLConnection
    )


def test_valkey_kwargs_carries_host_port_password_db_unchanged() -> None:
    # So a future edit cannot satisfy the TLS assertions above by dropping a
    # field: every part of the connection identity must still be present.
    config = WorkerConfig(
        valkey_host="valkey.acme.internal",
        valkey_port=6380,
        valkey_password="s3cret",
        valkey_db=2,
        valkey_tls=True,
    )
    kwargs = config.valkey_client_kwargs()
    assert kwargs["host"] == "valkey.acme.internal"
    assert kwargs["port"] == 6380
    assert kwargs["password"] == "s3cret"
    assert kwargs["db"] == 2
    assert kwargs["ssl"] is True


def test_build_wires_dedicated_single_attempt_pressure_clients(
    monkeypatch: pytest.MonkeyPatch,
    sync_redis: redis.Redis,
) -> None:
    endpoint = sync_redis.connection_pool.connection_kwargs
    config = WorkerConfig(
        fake_model=True,
        valkey_host=str(endpoint["host"]),
        valkey_port=int(endpoint["port"]),
        valkey_password=str(endpoint.get("password") or ""),
        valkey_db=int(endpoint.get("db", 0)),
        s3_access_key="PLACEHOLDER",
        s3_secret_key="PLACEHOLDER",
    )
    monkeypatch.setattr(DockerSandboxClient, "ensure_image", lambda self: None)

    async def exercise() -> None:
        runtime = run.build(config, {"CURIE_SANDBOX_SUBSTRATE": "docker"})
        affinity_redis = runtime.consumer._kernel._substrate._affinity._redis
        try:
            assert isinstance(runtime.async_redis, redis.asyncio.Redis)
            assert isinstance(runtime.pressure_async_redis, redis.asyncio.Redis)
            assert isinstance(runtime.eval_redis, redis.asyncio.Redis)
            assert isinstance(affinity_redis, redis.Redis)
            assert runtime.pressure_async_redis is not runtime.async_redis
            assert runtime.pressure_async_redis is not runtime.eval_redis

            pressure_kwargs = (
                runtime.pressure_async_redis.connection_pool.connection_kwargs
            )
            assert pressure_kwargs["socket_timeout"] == 1.0
            assert pressure_kwargs["socket_connect_timeout"] == 1.0
            assert pressure_kwargs["retry"].get_retries() == 0
            assert pressure_kwargs["driver_info"] is None
            assert await runtime.async_redis.ping()
            assert await runtime.pressure_async_redis.ping()
            assert await runtime.eval_redis.ping()
            assert await asyncio.to_thread(affinity_redis.ping)
            pressure_connection = (
                await runtime.pressure_async_redis.connection_pool.get_connection()
            )
            try:
                assert pressure_connection.driver_info is None
                assert pressure_connection.retry.get_retries() == 0
                # Redis 8.1 normalizes an explicitly disabled configuration to
                # no connection handler rather than retaining the input object.
                assert pressure_connection.maint_notifications_config is None
            finally:
                await runtime.pressure_async_redis.connection_pool.release(
                    pressure_connection
                )
        finally:
            affinity_redis.close()
            await runtime.runner.close()
            await runtime.sink.aclose()
            await runtime.eval_http.aclose()
            await runtime.async_redis.aclose()
            await runtime.pressure_async_redis.aclose()
            await runtime.eval_redis.aclose()
            await runtime.engine.dispose()

    asyncio.run(exercise())


# --- VALKEY_TLS env -> WorkerConfig.valkey_tls (the seam the chart actually
# drives; a field that only works via kwargs is not wired) ------------------


def test_valkey_tls_env_reaches_the_worker_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VALKEY_TLS", "true")
    assert WorkerConfig().valkey_tls is True


def test_valkey_tls_defaults_false_on_a_clean_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # VALKEY_TLS may be set in the ambient host/CI env; clear it explicitly so
    # this default-value assertion cannot be flipped by something outside the
    # test.
    monkeypatch.delenv("VALKEY_TLS", raising=False)
    assert WorkerConfig().valkey_tls is False


def test_substrate_config_reads_agent_sandbox_pools() -> None:
    assert _substrate_config({}).agent_pools == frozenset()
    config = _substrate_config({"CURIE_AGENT_SANDBOX_POOLS": "factory, acme-a,"})
    assert config.agent_pools == frozenset({"factory", "acme-a"})


def test_substrate_config_reads_connector_secret_pools() -> None:
    assert _substrate_config({}).connector_secret_pools == frozenset()
    config = _substrate_config({"CURIE_AGENT_CONNECTOR_SECRET_POOLS": "acme-a, acme-b,"})
    assert config.connector_secret_pools == frozenset({"acme-a", "acme-b"})


# -- _supervise: quiet boot while dependencies come up (#3079) ---------------


def _crash_twice_then_return(exc: BaseException):  # type: ignore[no-untyped-def]
    calls = {"n": 0}

    async def factory() -> None:
        calls["n"] += 1
        if calls["n"] <= 2:
            raise exc

    return factory, calls


def _supervise_once(factory, clock) -> None:  # type: ignore[no-untyped-def]
    async def go() -> None:
        await asyncio.wait_for(
            _supervise(
                "runs",
                factory,
                asyncio.Event(),
                restart_backoff_s=0,
                max_consecutive_failures=0,
                boot_grace_s=120.0,
                clock=clock,
            ),
            timeout=2,
        )

    asyncio.run(go())


@pytest.mark.parametrize(
    "exc",
    [
        socket.gaierror(-2, "Name or service not known"),
        ConnectionRefusedError(111, "Connection refused"),
        redis.exceptions.ConnectionError("Error connecting to valkey:6379"),
    ],
    ids=["dns", "refused", "valkey"],
)
def test_supervise_logs_dependency_not_ready_during_boot_as_one_line_warning(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    exc: BaseException,
) -> None:
    """A dependency that is still coming up during the boot window is a one-line
    warning that says it will retry, with no traceback."""
    _capture_metrics(monkeypatch)
    factory, calls = _crash_twice_then_return(exc)
    with caplog.at_level(logging.DEBUG, logger="curie_worker.run"):
        _supervise_once(factory, clock=lambda: 0.0)

    assert calls["n"] == 3
    records = [r for r in caplog.records if r.name == "curie_worker.run"]
    assert len(records) == 2
    for record in records:
        assert record.levelno == logging.WARNING
        assert record.exc_info is None
        message = record.getMessage()
        assert "\n" not in message
        assert "retrying" in message
        assert type(exc).__name__ in message


def test_supervise_quiet_boot_follows_the_exception_chain(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A library wrapper whose cause is a connection failure is still expected."""
    _capture_metrics(monkeypatch)
    try:
        try:
            raise socket.gaierror(-2, "Name or service not known")
        except OSError as inner:
            raise RuntimeError("could not reach postgres") from inner
    except RuntimeError as wrapped:
        exc = wrapped
    factory, _ = _crash_twice_then_return(exc)
    with caplog.at_level(logging.DEBUG, logger="curie_worker.run"):
        _supervise_once(factory, clock=lambda: 0.0)

    records = [r for r in caplog.records if r.name == "curie_worker.run"]
    assert [r.levelno for r in records] == [logging.WARNING, logging.WARNING]
    assert all(r.exc_info is None for r in records)


def test_supervise_keeps_tracebacks_for_unexpected_failures_during_boot(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A real bug at boot is not a dependency warming up: full traceback."""
    _capture_metrics(monkeypatch)
    factory, _ = _crash_twice_then_return(KeyError("latent bug"))
    with caplog.at_level(logging.DEBUG, logger="curie_worker.run"):
        _supervise_once(factory, clock=lambda: 0.0)

    records = [r for r in caplog.records if r.name == "curie_worker.run"]
    assert len(records) == 2
    assert all(r.levelno == logging.ERROR and r.exc_info for r in records)


def test_supervise_keeps_tracebacks_for_connection_failures_after_boot(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Once the boot window has passed, a lost dependency is unexpected again
    and logs its full traceback."""
    _capture_metrics(monkeypatch)
    ticks = iter([0.0] + [500.0] * 20)
    factory, _ = _crash_twice_then_return(ConnectionRefusedError(111, "refused"))
    with caplog.at_level(logging.DEBUG, logger="curie_worker.run"):
        _supervise_once(factory, clock=lambda: next(ticks))

    records = [r for r in caplog.records if r.name == "curie_worker.run"]
    assert len(records) == 2
    assert all(r.levelno == logging.ERROR and r.exc_info for r in records)


def test_run_warns_in_one_line_when_valkey_is_not_ready_for_the_boot_migration(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Valkey still coming up on a fresh install is expected at boot (#3079): the
    migration is skipped with a one-line warning, not a traceback."""
    with caplog.at_level(logging.DEBUG, logger="curie_worker.run"):
        events = _boot(
            monkeypatch,
            raises=redis.exceptions.ConnectionError("Error connecting to valkey:6379"),
        )

    assert "runs" in events
    records = [
        r for r in caplog.records if r.name == "curie_worker.run" and "migration" in r.getMessage()
    ]
    assert len(records) == 1
    assert records[0].levelno == logging.WARNING
    assert records[0].exc_info is None
    assert "not ready" in records[0].getMessage()


def test_supervise_keeps_tracebacks_for_local_os_errors_during_boot(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A PermissionError is a local fault, not a dependency warming up."""
    _capture_metrics(monkeypatch)
    factory, _ = _crash_twice_then_return(PermissionError(13, "Permission denied"))
    with caplog.at_level(logging.DEBUG, logger="curie_worker.run"):
        _supervise_once(factory, clock=lambda: 0.0)

    records = [r for r in caplog.records if r.name == "curie_worker.run"]
    assert all(r.levelno == logging.ERROR and r.exc_info for r in records)
