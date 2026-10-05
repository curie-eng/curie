"""Regression tests for WorkerConfig env-source resolution.

``populate_by_name=True`` lets tests construct the config with field-name
kwargs, but it must NOT make the env source read the bare uppercased field name
as a fallback for a field that carries a ``validation_alias``. An aliased field
must read only its ``CURIE_*`` alias; a stray generic env var (``API_KEY``,
``CREDENTIALS``, ...) in the pod env must be ignored, as it was before the
BaseSettings refactor.
"""

from __future__ import annotations

import base64
import importlib.util
import json
import os
import socket
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, NamedTuple, cast

import pytest
import yaml
from curie_worker.attachments import AttachmentLimits
from curie_worker.config import WorkerConfig
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.kernel.core import Kernel
from nacl.signing import SigningKey
from pydantic import AliasChoices, ValidationError
from redis.asyncio import Redis as AsyncRedis


def _clear_all_config_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Delete every env var the config could read, for a clean-env baseline.

    ``BaseSettings`` reads the process environment for every field (aliased
    fields via their ``validation_alias``, the rest via the uppercased field
    name). The kernel suite runs against real Valkey/Postgres, so vars like
    ``VALKEY_HOST``/``DATABASE_URL`` may be set in the ambient env; strip them
    all so the defaults assertions below see only the code defaults.
    """
    for name, field in WorkerConfig.model_fields.items():
        alias = field.validation_alias
        if isinstance(alias, str):
            keys = (alias,)
        elif isinstance(alias, AliasChoices):
            keys = tuple(choice for choice in alias.choices if isinstance(choice, str))
        else:
            keys = (name.upper(),)
        for key in keys:
            monkeypatch.delenv(key, raising=False)


class _Row(NamedTuple):
    """One hand-written env contract row for a ``WorkerConfig`` field."""

    field: str
    env: str  # the env var the field must read (its alias, or its bare name)
    raw: str  # sentinel set under ``env``
    expected: object  # value after coercion
    default: object  # clean-env default (``_NO_STATIC_DEFAULT`` for factories)
    bare: str | None = None  # a stray bare-name env var that must be IGNORED
    bare_raw: str | None = None  # decoy set under ``bare``


_NO_STATIC_DEFAULT = object()  # factory default; asserted separately below

# The env contract, written out by hand. NEVER derive this list or its
# expectations from ``WorkerConfig.model_fields``: the point is an independent
# oracle, so a drifted name, alias, coercion or default fails here.
#
# The first block is every env var the OLD hand-rolled ``WorkerConfig.from_env``
# read (on ``origin/main``), under its exact old name: the parity oracle proving
# no name drifted and no var was dropped in the BaseSettings port. The later
# blocks are knobs added after the port, each reading only its ``CURIE_*`` alias.
#
# The alias-read test sets EVERY row's env var AND every bare decoy at once, so
# the raw values must be jointly valid under the cross-field validators, and each
# row also proves its alias wins over its bare name.
_ENV_TABLE: list[_Row] = [
    # --- parity oracle vs the old from_env (review #178) ---
    _Row("valkey_host", "VALKEY_HOST", "valkey.host.example", "valkey.host.example", "localhost"),
    _Row("valkey_port", "VALKEY_PORT", "6380", 6380, 6379),
    _Row("valkey_password", "VALKEY_PASSWORD", "vk-pass", "vk-pass", ""),
    _Row("valkey_db", "VALKEY_DB", "7", 7, 0),
    _Row("slack_bot_token", "SLACK_BOT_TOKEN", "xoxb-sentinel", "xoxb-sentinel", ""),
    _Row(
        "slack_api_base_url", "SLACK_API_BASE_URL", "http://slack.stub:9", "http://slack.stub:9", ""
    ),
    _Row(
        "database_url",
        "DATABASE_URL",
        "postgresql+asyncpg://u:p@db:5432/x",
        "postgresql+asyncpg://u:p@db:5432/x",
        "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres",
    ),
    _Row("db_schema", "DB_SCHEMA", "myschema", "myschema", "curie"),
    _Row(
        "bundle_plugin_dir",
        "CURIE_PLUGIN_DIR",
        "/custom/bundles",
        "/custom/bundles",
        "/bundles/current",
    ),
    _Row("fake_model", "CURIE_FAKE_MODEL", "true", True, False),
    # Deliberately the NON-default value: shimmer defaults to True, so a truthy
    # token would pass even if the alias were never read at all. The True
    # default exists so a reasoning model's pre-token silence is not
    # indistinguishable from a wedge (#1182); it must agree with the
    # dispatcher's default, since one env name drives both services.
    _Row("shimmer", "CURIE_SHIMMER", "no", False, True),
    _Row(
        "credentials",
        "CURIE_CREDENTIALS",
        "cred-sentinel",
        "cred-sentinel",
        "",
        bare="CREDENTIALS",
        bare_raw="stray-creds",
    ),
    _Row(
        "model_base_url", "CURIE_MODEL_BASE_URL", "http://model.local:1", "http://model.local:1", ""
    ),
    _Row("model", "CURIE_MODEL", "claude-sentinel", "claude-sentinel", ""),
    _Row("eval_stream", "CURIE_EVAL_STREAM", "sentinel:evals", "sentinel:evals", "curie:evals"),
    _Row(
        "eval_consumer_group",
        "CURIE_EVAL_CONSUMER_GROUP",
        "sentinel-eval-workers",
        "sentinel-eval-workers",
        "curie-eval-workers",
    ),
    _Row(
        "s3_endpoint_url",
        "S3_ENDPOINT_URL",
        "http://s3.local:2",
        "http://s3.local:2",
        "http://localhost:29000",
    ),
    # S3 keys are empty by default (#1559): a baked-in dev key is still an
    # explicit credential to boto3, so it shadows the ambient cloud identity
    # (IRSA, instance role) the key-free BYO object-store path relies on. Do not
    # restore the RustFS dev pair; compose supplies it via env, and these rows'
    # sentinels stay the guard that an operator value still wins.
    _Row("s3_access_key", "S3_ACCESS_KEY", "ak-sentinel", "ak-sentinel", ""),
    _Row("s3_secret_key", "S3_SECRET_KEY", "sk-sentinel", "sk-sentinel", ""),
    _Row("s3_region", "S3_REGION", "eu-west-9", "eu-west-9", "us-east-1"),
    _Row("bundle_bucket", "BUNDLE_BUCKET", "sentinel-bundles", "sentinel-bundles", "curie-bundles"),
    _Row(
        "api_base_url",
        "CURIE_API_URL",
        "http://api.local:3",
        "http://api.local:3",
        "http://localhost:8000",
    ),
    _Row(
        "api_key",
        "CURIE_API_KEY",
        "key-sentinel",
        "key-sentinel",
        "curie-dev-key",
        bare="API_KEY",
        bare_raw="stray",
    ),
    _Row(
        "langfuse_host",
        "LANGFUSE_HOST",
        "http://lf.local:4",
        "http://lf.local:4",
        "http://localhost:23000",
    ),
    _Row(
        "langfuse_public_key",
        "LANGFUSE_PUBLIC_KEY",
        "pk-sentinel",
        "pk-sentinel",
        "pk-lf-curie-dev",
    ),
    _Row(
        "langfuse_secret_key",
        "LANGFUSE_SECRET_KEY",
        "sk-lf-sentinel",
        "sk-lf-sentinel",
        "sk-lf-curie-dev",
    ),
    _Row("stream", "CURIE_STREAM", "sentinel:runs", "sentinel:runs", "curie:runs"),
    _Row(
        "consumer_group",
        "CURIE_CONSUMER_GROUP",
        "sentinel-workers",
        "sentinel-workers",
        "curie-workers",
    ),
    _Row(
        "consumer_name",
        "CURIE_CONSUMER_NAME",
        "sentinel-consumer",
        "sentinel-consumer",
        _NO_STATIC_DEFAULT,
    ),
    _Row("max_attempts", "CURIE_MAX_ATTEMPTS", "9", 9, 3),
    # --- operator-scoped model wire declaration (#514) ---
    # Mirror model_base_url. Undeclared ("") is the default, so the producer
    # emits nothing and the runner keeps its own pre-#514 defaults.
    _Row(
        "model_api_backend",
        "CURIE_MODEL_API_BACKEND",
        "messages",
        "messages",
        "",
        bare="MODEL_API_BACKEND",
        bare_raw="chat_completions",
    ),
    _Row(
        "model_env_key",
        "CURIE_MODEL_ENV_KEY",
        '["ANTHROPIC_AUTH_TOKEN"]',
        '["ANTHROPIC_AUTH_TOKEN"]',
        "",
        bare="MODEL_ENV_KEY",
        bare_raw="STRAY_NAME",
    ),
    # --- runner-facing API base (#678) ---
    # Distinct from api_base_url (the worker's self-dial URL). Undivided ("") is
    # the default: the runner reaches the API at the worker's own URL (k8s
    # in-cluster, single-host local).
    _Row(
        "runner_api_base_url",
        "CURIE_RUNNER_API_URL",
        "http://curie-api:8000",
        "http://curie-api:8000",
        "",
        bare="RUNNER_API_BASE_URL",
        bare_raw="http://stray:9000",
    ),
    # --- eval claim-creation concurrency bound (#709) ---
    # Single-node-safe by default: claims are created one at a time.
    _Row(
        "eval_max_concurrent_claims",
        "CURIE_EVAL_MAX_CONCURRENT_CLAIMS",
        "4",
        4,
        1,
        bare="EVAL_MAX_CONCURRENT_CLAIMS",
        bare_raw="7",
    ),
    # --- runs-lane turn concurrency per worker (#760) ---
    # The chart renders worker.maxConcurrency here; before it was wired the
    # value was a constructor default no deployment could change.
    _Row(
        "max_concurrency",
        "CURIE_WORKER_MAX_CONCURRENCY",
        "4",
        4,
        16,
        bare="MAX_CONCURRENCY",
        bare_raw="9",
    ),
    # --- delivery budget and ownership lease (ADR-0131, #1971) ---
    # ADR-0131's stated initial defaults: drifting one silently changes the
    # fence's timing on every deployment that does not override it. The chart
    # templates these as first-class env, so a name drift means an operator's
    # --set silently does nothing; a stray bare name must not leak into the
    # fence's timing either. Raw values are jointly valid (90 >= 3 * 20,
    # 10 < 90, 1860 >= 1800 + 45).
    _Row(
        "delivery_budget_s",
        "CURIE_DELIVERY_BUDGET_S",
        "1800",
        1800.0,
        600.0,
        bare="DELIVERY_BUDGET_S",
        bare_raw="1800",
    ),
    _Row(
        "delivery_lease_ttl_s",
        "CURIE_DELIVERY_LEASE_TTL_S",
        "90",
        90.0,
        45.0,
        bare="DELIVERY_LEASE_TTL_S",
        bare_raw="1",
    ),
    _Row(
        "delivery_lease_heartbeat_s",
        "CURIE_DELIVERY_LEASE_HEARTBEAT_S",
        "20",
        20.0,
        10.0,
        bare="DELIVERY_LEASE_HEARTBEAT_S",
        bare_raw="1",
    ),
    _Row(
        "delivery_shutdown_reserve_s",
        "CURIE_DELIVERY_SHUTDOWN_RESERVE_S",
        "45",
        45.0,
        60.0,
        bare="DELIVERY_SHUTDOWN_RESERVE_S",
        bare_raw="0",
    ),
    # None means "no platform grace declared" (compose, tests) and SKIPS the
    # grace validator rather than guessing a value for it.
    _Row(
        "termination_grace_period_s",
        "CURIE_TERMINATION_GRACE_PERIOD_S",
        "1860",
        1860.0,
        None,
        bare="TERMINATION_GRACE_PERIOD_S",
        bare_raw="5",
    ),
    # Bound by a validator: the default 30 < 45 satisfies the ADR's "the reclaim
    # interval is shorter than the lease".
    _Row("reclaim_interval_s", "CURIE_RECLAIM_INTERVAL_S", "10", 10.0, 30.0),
    _Row("work_item_max_turns", "CURIE_WORK_ITEM_MAX_TURNS", "5", 5, 1000),
    # The install's receipt mode (ADR-0180). The sentinel is a NON-default mode
    # so a field that never read its alias cannot pass, and the bare decoy is a
    # third mode so ``populate_by_name`` cannot satisfy the read either.
    _Row(
        "turn_receipt",
        "CURIE_TURN_RECEIPT",
        "failures",
        "failures",
        "all",
        bare="TURN_RECEIPT",
        bare_raw="off",
    ),
    # --- inbound attachment lane envelope (#2567, S4) ---
    # Defaults are asserted AGAINST ``AttachmentLimits`` rather than literals:
    # two independently written copies of "32 MiB" is exactly how a chart
    # override silently stops matching the code it configures. Bare decoys stop
    # ``populate_by_name`` from satisfying the reads. The off switch reads the
    # same way: a drift there is an operator who sets the chart value and still
    # gets a worker that downloads and parks every upload.
    _Row(
        "attachment_enabled",
        "CURIE_ATTACHMENT_ENABLED",
        "true",
        True,
        False,
        bare="ATTACHMENT_ENABLED",
        bare_raw="false",
    ),
    _Row(
        "attachment_max_file_bytes",
        "CURIE_ATTACHMENT_MAX_FILE_BYTES",
        "8388608",
        8388608,
        AttachmentLimits().max_file_bytes,
        bare="ATTACHMENT_MAX_FILE_BYTES",
        bare_raw="999999",
    ),
    _Row(
        "attachment_reference_ttl_seconds",
        "CURIE_ATTACHMENT_REFERENCE_TTL_SECONDS",
        "120",
        120,
        AttachmentLimits().reference_ttl_seconds,
        bare="ATTACHMENT_REFERENCE_TTL_SECONDS",
        bare_raw="999999",
    ),
    _Row(
        "attachment_retention_ttl_seconds",
        "CURIE_ATTACHMENT_RETENTION_TTL_SECONDS",
        "900",
        900,
        AttachmentLimits().retention_ttl_seconds,
        bare="ATTACHMENT_RETENTION_TTL_SECONDS",
        bare_raw="999999",
    ),
    # --- the per-thread attachment budget (ADR 0205 decision 7, #4079) ---
    # Every boot rebuilds the thread's files, so one boot's work is bounded per
    # thread, separately from the per-message ``max_files``. The defaults are
    # literals here on purpose: the ADR names them, and ``AttachmentLimits``
    # carries the same two counts (pinned in test_attachment_thread_set.py).
    # The prepare timeout bounds the worker's own rebuild (ledger read, cache
    # re-mint, channel re-fetch) inside the turn's remaining budget.
    _Row(
        "attachment_thread_max_files",
        "CURIE_ATTACHMENT_THREAD_MAX_FILES",
        "15",
        15,
        20,
        bare="ATTACHMENT_THREAD_MAX_FILES",
        bare_raw="999",
    ),
    _Row(
        "attachment_thread_max_bytes",
        "CURIE_ATTACHMENT_THREAD_MAX_BYTES",
        "134217728",
        134217728,
        256 * 1024 * 1024,
        bare="ATTACHMENT_THREAD_MAX_BYTES",
        bare_raw="999",
    ),
    _Row(
        "attachment_thread_prepare_timeout_seconds",
        "CURIE_ATTACHMENT_THREAD_PREPARE_TIMEOUT_SECONDS",
        "12.5",
        12.5,
        15.0,
        bare="ATTACHMENT_THREAD_PREPARE_TIMEOUT_SECONDS",
        bare_raw="999",
    ),
]


def _row_param(row: _Row) -> object:
    return pytest.param(row, id=row.field)


def test_env_table_has_one_row_per_field_and_unique_env_names() -> None:
    """Sanity of the table itself: one row per field, and no env name reused."""
    fields = [row.field for row in _ENV_TABLE]
    envs = [row.env for row in _ENV_TABLE] + [r.bare for r in _ENV_TABLE if r.bare]
    assert len(fields) == len(set(fields))
    assert len(envs) == len(set(envs))


@pytest.mark.parametrize("row", [_row_param(r) for r in _ENV_TABLE])
def test_field_reads_its_env_var_over_any_bare_name(
    monkeypatch: pytest.MonkeyPatch, row: _Row
) -> None:
    """Every row's env var set to its sentinel under its EXACT name is read into
    the right field with the right coercion, and wins over a bare-name decoy.

    All rows' env vars and decoys are set together, as a real pod env would be.
    """
    _clear_all_config_env(monkeypatch)
    for other in _ENV_TABLE:
        monkeypatch.setenv(other.env, other.raw)
        if other.bare is not None:
            assert other.bare_raw is not None
            monkeypatch.setenv(other.bare, other.bare_raw)

    actual = getattr(WorkerConfig(), row.field)

    assert actual == row.expected, f"{row.env} -> {row.field}: {actual!r}"
    # Coercion parity: ints/bools/floats must be the coerced type, not a raw str.
    assert type(actual) is type(row.expected)


@pytest.mark.parametrize(
    "row", [_row_param(r) for r in _ENV_TABLE if r.default is not _NO_STATIC_DEFAULT]
)
def test_field_default_in_a_clean_env(monkeypatch: pytest.MonkeyPatch, row: _Row) -> None:
    """Config drift on a default is a silent prod break, so each is locked."""
    _clear_all_config_env(monkeypatch)

    actual = getattr(WorkerConfig(), row.field)

    assert actual == row.default
    assert type(actual) is type(row.default)


@pytest.mark.parametrize("row", [_row_param(r) for r in _ENV_TABLE if r.bare])
def test_aliased_field_ignores_its_bare_field_name_env(
    monkeypatch: pytest.MonkeyPatch, row: _Row
) -> None:
    """``populate_by_name`` must not make the env source fall back to the bare
    uppercased field name: a stray generic env var in the pod env stays out."""
    _clear_all_config_env(monkeypatch)
    for other in _ENV_TABLE:
        if other.bare is not None:
            assert other.bare_raw is not None
            monkeypatch.setenv(other.bare, other.bare_raw)

    assert getattr(WorkerConfig(), row.field) == row.default


def test_api_url_accepts_the_deprecated_base_url_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#496: the platform API base URL is canonically CURIE_API_URL, but the
    historical CURIE_API_BASE_URL still resolves for one release, and the
    canonical name wins when both are set."""
    monkeypatch.setenv("CURIE_API_BASE_URL", "http://deprecated:8000")
    assert WorkerConfig().api_base_url == "http://deprecated:8000"

    monkeypatch.setenv("CURIE_API_URL", "http://canonical:8000")
    assert WorkerConfig().api_base_url == "http://canonical:8000"


def test_field_name_kwargs_still_populate() -> None:
    """populate_by_name construction (used by tests) is unchanged."""
    config = WorkerConfig(fake_model=True, api_key="x", credentials="c")

    assert config.fake_model is True
    assert config.api_key == "x"
    assert config.credentials == "c"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("lock_ttl_ms", 0),
        ("lock_ttl_ms", -1),
        ("lock_acquire_timeout_s", 0),
        ("lock_acquire_timeout_s", -0.5),
        ("lock_acquire_timeout_s", float("nan")),
        ("lock_acquire_timeout_s", float("inf")),
        ("lock_acquire_timeout_s", float("-inf")),
        ("lock_poll_interval_s", 0),
        ("lock_poll_interval_s", -0.02),
        ("lock_poll_interval_s", float("nan")),
        ("lock_poll_interval_s", float("inf")),
        ("lock_poll_interval_s", float("-inf")),
    ],
)
def test_thread_lock_settings_must_be_positive_and_finite(
    monkeypatch: pytest.MonkeyPatch, field: str, value: Any
) -> None:
    """#3730: refuse the lock knobs at boot instead of failing every turn.

    ``lock_ttl_ms`` goes straight into ``SET key token NX PX <ttl>``, so a
    non-positive value is rejected by Valkey on every acquire; a non-positive
    poll interval turns a contended acquire into a hot loop; a non-positive
    acquire timeout gives up immediately; inf/nan in either float breaks the
    timeout arithmetic the same way.
    """
    _clear_all_config_env(monkeypatch)
    with pytest.raises(ValidationError):
        WorkerConfig(**{field: value})


def test_thread_lock_settings_env_vars_refuse_non_positive_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The issue's repro shape: LOCK_TTL_MS=0 in the pod env must refuse boot."""
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("LOCK_TTL_MS", "0")
    with pytest.raises(ValidationError):
        WorkerConfig()


def test_thread_lock_settings_accept_positive_finite_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Positive finite values and the defaults are unchanged (#3730)."""
    _clear_all_config_env(monkeypatch)

    config = WorkerConfig()

    assert config.lock_ttl_ms == 120000
    assert config.lock_acquire_timeout_s == 45.0
    assert config.lock_poll_interval_s == 0.02

    explicit = WorkerConfig(lock_ttl_ms=1, lock_acquire_timeout_s=0.5, lock_poll_interval_s=0.001)
    assert explicit.lock_ttl_ms == 1
    assert explicit.lock_acquire_timeout_s == 0.5
    assert explicit.lock_poll_interval_s == 0.001


# --- Env-var parity vs the pre-pydantic from_env (review #178) ---------------


def test_defaults_parity_with_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clean env: fields with no _ENV_TABLE row must equal the exact default the
    old from_env produced. Together with ``test_field_default_in_a_clean_env``
    every field the old from_env set is locked to its old default.
    """
    _clear_all_config_env(monkeypatch)

    config = WorkerConfig()

    # Valkey
    assert config.valkey_host == "localhost"
    assert config.valkey_port == 6379
    assert config.valkey_password == ""
    assert config.valkey_db == 0
    # Slack
    assert config.slack_bot_token == ""
    assert config.slack_api_base_url == ""
    assert config.slack_identities == ()
    # Postgres
    assert (
        config.database_url
        == "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres"
    )
    assert config.db_schema == "curie"
    # Deployment-to-runtime binding
    assert config.default_max_usd_per_day == 10.0
    assert config.default_max_output_tokens_per_run == 100000
    # Read loop
    assert config.read_count == 16
    assert config.read_block_ms == 5000
    # Per-thread lock
    assert config.lock_ttl_ms == 120000
    assert config.lock_acquire_timeout_s == 45.0
    assert config.lock_poll_interval_s == 0.02
    # Retry
    assert config.retry_backoff_base_s == 1.0
    assert config.retry_backoff_max_s == 20.0
    # Markers
    assert config.idempotency_ttl_s == 86400
    # Crash recovery
    assert config.reclaim_min_idle_ms == 900000
    assert config.dead_consumer_idle_ms == 15000
    assert config.consumer_heartbeat_ttl_ms == 15000
    assert config.consumer_capability_ttl_ms == 1800000
    # Slack edit throttle
    assert config.slack_edit_min_interval_s == 0.7
    # Runner HTTP timeouts
    assert config.runner_connect_timeout_s == 10.0
    assert config.runner_total_timeout_s == 600.0
    # Platform API
    assert config.report_max_attempts == 3
    assert config.report_backoff_base_s == 0.5
    # Key prefix
    assert config.key_prefix == "curie:worker"

    # Factory-defaulted names have no static default: the old from_env produced
    # ``f"{hostname}-{pid}"`` via ``_default_consumer_name``. Assert that shape.
    expected_consumer = f"{socket.gethostname()}-{os.getpid()}"
    assert config.consumer_name == expected_consumer
    assert config.eval_consumer_name == expected_consumer


@pytest.mark.parametrize(
    "overrides",
    [
        {"consumer_heartbeat_ttl_ms": 0},
        {"consumer_capability_ttl_ms": 0},
    ],
)
def test_consumer_liveness_ttls_must_be_positive(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        WorkerConfig.model_validate(overrides)


def test_consumer_capability_ttl_must_outlive_reclaim_backstop() -> None:
    with pytest.raises(ValueError, match="must be greater than reclaim_min_idle_ms"):
        WorkerConfig(reclaim_min_idle_ms=50, consumer_capability_ttl_ms=50)

    config = WorkerConfig(reclaim_min_idle_ms=50, consumer_capability_ttl_ms=51)
    assert config.consumer_capability_ttl_ms == 51


def test_consumer_liveness_ttls_read_only_their_curie_aliases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CONSUMER_HEARTBEAT_TTL_MS", "999")
    monkeypatch.setenv("CONSUMER_CAPABILITY_TTL_MS", "999999")
    monkeypatch.setenv("CURIE_CONSUMER_HEARTBEAT_TTL_MS", "25")
    monkeypatch.setenv("CURIE_CONSUMER_CAPABILITY_TTL_MS", "5001")
    monkeypatch.setenv("RECLAIM_MIN_IDLE_MS", "5000")

    config = WorkerConfig()

    assert config.consumer_heartbeat_ttl_ms == 25
    assert config.consumer_capability_ttl_ms == 5001


# --- Operator-scoped model wire declaration (#514) ---------------------------
#
# Default, alias and bare-name coverage lives in the _ENV_TABLE rows.


def test_model_api_backend_and_env_key_populate_by_field_name() -> None:
    """populate_by_name construction (used by the binding tests) works."""
    config = WorkerConfig(model_api_backend="messages", model_env_key="MY_PROVIDER_KEY")

    assert config.model_api_backend == "messages"
    assert config.model_env_key == "MY_PROVIDER_KEY"


# --- Runner-facing API base (#678) -------------------------------------------
#
# The API base a SPAWNED RUNNER dials, distinct from api_base_url (the worker's
# self-dial URL). Default, alias and bare-name coverage lives in _ENV_TABLE.


def test_runner_facing_api_base_url_falls_back_to_self_dial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unset runner_api_base_url resolves to api_base_url, so k8s and single-host
    local -- where the runner reaches the API at the worker's URL -- are unchanged."""
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_API_URL", "http://in-cluster-api:8000")

    config = WorkerConfig()

    assert config.runner_api_base_url == ""
    assert config.runner_facing_api_base_url == "http://in-cluster-api:8000"


def test_runner_facing_api_base_url_prefers_the_runner_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the two networks diverge (docker substrate), the runner-facing base
    wins over the worker's localhost self-dial URL."""
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_API_URL", "http://localhost:28000")
    monkeypatch.setenv("CURIE_RUNNER_API_URL", "http://curie-api:8000")

    config = WorkerConfig()

    assert config.api_base_url == "http://localhost:28000"
    assert config.runner_facing_api_base_url == "http://curie-api:8000"


def test_curie_dead_letter_stream_reaches_the_dead_letter_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CURIE_DEAD_LETTER_STREAM (#505/#668) populates dead_letter_stream and
    is reflected by dead_letter_stream_name(), the graveyard the API's
    dead-letter watcher must agree with (see apps/api/tests/test_config_parity.py)."""
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_STREAM", "operations")
    monkeypatch.setenv("CURIE_DEAD_LETTER_STREAM", "operations:dead")

    config = WorkerConfig()

    assert config.dead_letter_stream == "operations:dead"
    assert config.dead_letter_stream_name() == "operations:dead"


# Worker boolean behavior (review #178)
#
# The old worker ``_b`` accepted only ("1", "true", "yes") as truthy. These
# tests preserve that exact token set.


@pytest.mark.parametrize("token", ["1", "true", "yes", "TRUE", "Yes", " yes "])
def test_bool_shared_truthy_tokens(monkeypatch: pytest.MonkeyPatch, token: str) -> None:
    """The worker truthy tokens parse to True regardless of case or surrounding space."""
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_SHIMMER", token)
    monkeypatch.setenv("CURIE_FAKE_MODEL", token)

    config = WorkerConfig()

    assert config.shimmer is True
    assert config.fake_model is True


@pytest.mark.parametrize("token", ["on", "ON", "0", "no", "off", "", "maybe"])
def test_bool_worker_rejects_on_and_falsy_tokens(
    monkeypatch: pytest.MonkeyPatch, token: str
) -> None:
    """The worker parses "on" and the other rejected tokens as False."""
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_SHIMMER", token)
    monkeypatch.setenv("CURIE_FAKE_MODEL", token)

    config = WorkerConfig()

    assert config.shimmer is False
    assert config.fake_model is False


# --- Eval claim-creation concurrency bound (#709) ----------------------------
#
# A ceiling on eval SandboxClaims created/bound concurrently, so a single-node
# cluster is not flooded. Default, alias and bare-name coverage lives in
# _ENV_TABLE.


def test_eval_max_concurrent_claims_rejects_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Floor of 1: a bound of 0 would create no claims at all (no eval could run)."""
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_EVAL_MAX_CONCURRENT_CLAIMS", "0")

    with pytest.raises(ValueError):
        WorkerConfig()


# --- Runs-lane turn concurrency (#760) ----------------------------------------
#
# ``CURIE_WORKER_MAX_CONCURRENCY`` bounds how many turns one worker runs at once.
# Default, alias and bare-name coverage lives in _ENV_TABLE; these pin the
# bounds the chart schema mirrors (1 through 256) and that the value reaches the
# consumer the way the production entry point builds it.


@pytest.mark.parametrize("raw", ["0", "-1", "257"])
def test_max_concurrency_refuses_out_of_range(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    """0 would admit no turn at all; above 256 is outside the chart's bound."""
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_WORKER_MAX_CONCURRENCY", raw)

    with pytest.raises(ValidationError, match="max_concurrency|CURIE_WORKER_MAX_CONCURRENCY"):
        WorkerConfig()


@pytest.mark.parametrize("raw", ["1", "256"])
def test_max_concurrency_accepts_both_ends_of_its_range(
    monkeypatch: pytest.MonkeyPatch, raw: str
) -> None:
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_WORKER_MAX_CONCURRENCY", raw)

    assert WorkerConfig().max_concurrency == int(raw)


def test_explicit_consumer_concurrency_still_wins_over_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Kernel tests size a consumer to one slot directly; that must still hold."""
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_WORKER_MAX_CONCURRENCY", "4")

    config = WorkerConfig()
    redis = AsyncRedis(host="127.0.0.1", port=1)
    consumer = Consumer(
        redis=redis,
        kernel=cast(Kernel, SimpleNamespace()),
        config=config,
        leases=DeliveryLeaseStore(redis, config),
        max_concurrency=1,
    )

    assert consumer._max_concurrency == 1
    assert consumer._transfer_capacity() == 1


# --- Raw-string ingestion of complex-typed fields -----------------------------
#
# ``slack_trusted_origins`` (tuple) and ``adapter_credentials`` (dict) are
# "complex" types, which pydantic-settings JSON-decodes INSIDE the env source,
# BEFORE any field validator runs. Their BeforeValidators accept a bare
# comma-list and a blank string respectively, but those never got the chance:
# the source raised ``SettingsError`` first and the worker died at boot on the
# exact value compose.dev.yaml exports. ``NoDecode`` on the annotated type
# suppresses that decode so the raw env string reaches the validator.
#
# These MUST go through the real settings source (env, not kwargs) -- kwarg
# construction bypasses the env source entirely and passed all along.


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # The compose.dev.yaml value: a comma list, not JSON.
        pytest.param(
            "http://localhost,http://127.0.0.1,http://host.docker.internal",
            ("http://localhost", "http://127.0.0.1", "http://host.docker.internal"),
            id="bare-comma-list",
        ),
        pytest.param(
            " http://localhost:8080 , , http://a.b ",
            ("http://localhost:8080", "http://a.b"),
            id="whitespace-around-entries",
        ),
        # Blank means "no extra trusted origins": fail closed, never a boot crash.
        pytest.param("", (), id="empty-is-the-closed-default"),
    ],
)
def test_trusted_origins_parsed_from_raw_env(
    monkeypatch: pytest.MonkeyPatch, raw: str, expected: tuple[str, ...]
) -> None:
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_SLACK_TRUSTED_ORIGINS", raw)

    assert WorkerConfig().slack_trusted_origins == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # Same defect class: a blank CURIE_ADAPTER_CREDENTIALS is "none
        # configured" (every non-Slack egress then fails closed), not a boot crash.
        pytest.param("", {}, id="empty-is-an-empty-map"),
        pytest.param('{"acme": "s3cret"}', {"acme": "s3cret"}, id="json"),
    ],
)
def test_adapter_credentials_parsed_from_raw_env(
    monkeypatch: pytest.MonkeyPatch, raw: str, expected: dict[str, str]
) -> None:
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_ADAPTER_CREDENTIALS", raw)

    assert WorkerConfig().adapter_credentials == expected


def test_adapter_credentials_malformed_json_is_a_startup_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failing closed on garbage is deliberate; it must stay a failure."""
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_ADAPTER_CREDENTIALS", "not-json")

    with pytest.raises(ValueError):
        WorkerConfig()


def _load_test_module(module_name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_async_test_body_gate_rejects_a_shallow_corpus(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo_root = Path(__file__).resolve().parents[3]
    module = _load_test_module(
        "issue_1431_async_body_gate",
        repo_root / "apps/worker/tests/binding/test_no_unrun_async_test_bodies.py",
    )
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    gate = module.test_no_test_defines_a_coroutine_it_never_runs
    assert callable(gate)

    with pytest.raises(AssertionError) as exc_info:
        gate()

    message = str(exc_info.value)
    assert str(tmp_path.resolve()) in message
    assert "0" in message


def test_recorder_health_precondition_fails_when_service_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = Path(__file__).resolve().parents[3]
    module = _load_test_module(
        "issue_1431_recorder",
        repo_root / "apps/worker/tests/eval/test_recorder.py",
    )
    unavailable_host = "http://127.0.0.1:1"
    monkeypatch.setattr(module, "_LF_HOST", unavailable_host)
    recorder_test = module.test_records_per_case_results_and_reads_them_back
    assert callable(recorder_test)

    try:
        recorder_test()
    except pytest.skip.Exception as exc:
        pytest.fail(f"health precondition skipped at {unavailable_host}: {exc}")
    except pytest.fail.Exception as exc:
        message = str(exc)
        assert "Langfuse not reachable at" in message
        assert unavailable_host in message
    else:
        pytest.fail("health precondition returned successfully while its service was absent")


# --- Delivery budget and ownership lease (ADR-0131, #1971) --------------------
#
# One deadline and one renewable fenced owner per delivery. The four cross-field
# validators below exist because each relationship is invisible until an
# incident: a lease that cannot span three heartbeat periods drops a healthy turn
# on a single Valkey blip; a reclaim scan slower than the lease leaves an expired
# lease unrecovered for a whole extra scan; a termination grace below
# budget + reserve SIGKILLs a draining worker at the exact moment it would
# settle; and a per-request runner ceiling above the overall budget is dead
# configuration that reads as if it granted more time than it does. The operator
# must learn at boot, not at 2am -- so each rejection NAMES the env vars.

_LEASE_BASELINE: dict[str, object] = {
    "delivery_budget_s": 600.0,
    "delivery_lease_ttl_s": 45.0,
    "delivery_lease_heartbeat_s": 10.0,
    "delivery_shutdown_reserve_s": 60.0,
    "reclaim_interval_s": 30.0,
    "runner_total_timeout_s": 600.0,
    "termination_grace_period_s": None,
}


def _lease_config(**overrides: object) -> WorkerConfig:
    """A self-consistent delivery config with exactly ONE relationship perturbed.

    Validators run in declaration order and the first raise wins, so a test that
    perturbed two relationships at once could assert on the wrong message. Every
    test below moves one knob off this baseline and leaves the rest satisfied.
    """
    values = dict(_LEASE_BASELINE)
    values.update(overrides)
    return WorkerConfig(**values)


@pytest.mark.parametrize(
    ("env", "raw", "expected"),
    [
        pytest.param("CURIE_RUNNER_TOTAL_TIMEOUT_S", "1700", 1700.0, id="canonical"),
        pytest.param("RUNNER_TOTAL_TIMEOUT_S", "1600", 1600.0, id="legacy"),
    ],
)
def test_runner_total_timeout_reads_the_canonical_and_legacy_alias(
    monkeypatch: pytest.MonkeyPatch, env: str, raw: str, expected: float
) -> None:
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv(env, raw)
    monkeypatch.setenv("CURIE_DELIVERY_BUDGET_S", "1800")

    config = WorkerConfig()

    assert config.runner_total_timeout_s == expected


def test_runner_total_timeout_canonical_alias_wins_over_legacy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_RUNNER_TOTAL_TIMEOUT_S", "1700")
    monkeypatch.setenv("RUNNER_TOTAL_TIMEOUT_S", "1600")
    monkeypatch.setenv("CURIE_DELIVERY_BUDGET_S", "1800")

    config = WorkerConfig()

    assert config.runner_total_timeout_s == 1700.0


def test_runner_total_timeout_accepts_short_programmatic_values_and_maximum(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The chart and Python config accept positive timeout values, including
    sub-minute values; only positivity and the 10800s ceiling (#3071) apply."""
    _clear_all_config_env(monkeypatch)

    short = _lease_config(runner_total_timeout_s=0.5)
    maximum = _lease_config(
        delivery_budget_s=10800.0,
        runner_total_timeout_s=10800.0,
    )

    assert short.runner_total_timeout_s == 0.5
    assert maximum.runner_total_timeout_s == 10800.0


@pytest.mark.parametrize(
    ("value", "error_type"),
    [
        (0.0, "greater_than"),
        (-0.1, "greater_than"),
        (10800.1, "less_than_equal"),
        (10801.0, "less_than_equal"),
    ],
)
def test_runner_total_timeout_rejects_programmatic_values_outside_its_bounds(
    monkeypatch: pytest.MonkeyPatch,
    value: float,
    error_type: str,
) -> None:
    _clear_all_config_env(monkeypatch)

    with pytest.raises(ValidationError) as exc_info:
        _lease_config(delivery_budget_s=10800.0, runner_total_timeout_s=value)

    assert any(
        error["loc"] == ("runner_total_timeout_s",) and error["type"] == error_type
        for error in exc_info.value.errors()
    )


def test_runner_total_timeout_rejects_zero_from_the_canonical_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_RUNNER_TOTAL_TIMEOUT_S", "0")

    with pytest.raises(ValidationError) as exc_info:
        WorkerConfig()

    assert any(
        error["loc"] == ("CURIE_RUNNER_TOTAL_TIMEOUT_S",) and error["type"] == "greater_than"
        for error in exc_info.value.errors()
    )


def test_delivery_budget_accepts_the_adr_maximum_and_rejects_above_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#3071 raised the maximum to 10800s (three hours) so a long factory run
    fits one delivery. Anything above it is refused at boot."""
    _clear_all_config_env(monkeypatch)

    assert _lease_config(delivery_budget_s=10800.0).delivery_budget_s == 10800.0

    with pytest.raises(ValueError):
        _lease_config(delivery_budget_s=10801.0)


def test_delivery_budget_maximum_is_read_from_the_canonical_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_DELIVERY_BUDGET_S", "10800")
    monkeypatch.setenv("CURIE_RUNNER_TOTAL_TIMEOUT_S", "10800")

    config = WorkerConfig()

    assert config.delivery_budget_s == 10800.0
    assert config.runner_total_timeout_s == 10800.0

    monkeypatch.setenv("CURIE_DELIVERY_BUDGET_S", "10801")
    with pytest.raises(ValidationError) as exc_info:
        WorkerConfig()
    assert any(
        error["loc"] == ("CURIE_DELIVERY_BUDGET_S",) and error["type"] == "less_than_equal"
        for error in exc_info.value.errors()
    )


def test_runner_ceiling_still_bounded_by_budget_at_the_raised_maximum(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raising both maxima must not loosen the runner <= budget relationship."""
    _clear_all_config_env(monkeypatch)

    with pytest.raises(ValueError) as exc_info:
        _lease_config(delivery_budget_s=600.0, runner_total_timeout_s=10800.0)

    assert "CURIE_RUNNER_TOTAL_TIMEOUT_S" in str(exc_info.value)
    assert "CURIE_DELIVERY_BUDGET_S" in str(exc_info.value)


def test_termination_grace_must_cover_the_raised_maximum_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A declared grace still has to cover budget + reserve at 10800s."""
    _clear_all_config_env(monkeypatch)

    ok = _lease_config(delivery_budget_s=10800.0, termination_grace_period_s=10860.0)
    assert ok.termination_grace_period_s == 10860.0

    with pytest.raises(ValueError) as exc_info:
        _lease_config(delivery_budget_s=10800.0, termination_grace_period_s=10859.0)
    assert "CURIE_TERMINATION_GRACE_PERIOD_S" in str(exc_info.value)


def test_work_item_max_turns_refuses_non_positive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_WORK_ITEM_MAX_TURNS", "0")
    with pytest.raises(ValidationError) as exc_info:
        WorkerConfig()
    assert any(
        error["loc"] == ("CURIE_WORK_ITEM_MAX_TURNS",) and error["type"] == "greater_than"
        for error in exc_info.value.errors()
    )


def test_delivery_budget_accepts_its_floor_and_rejects_below_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A budget under a minute cannot cover claim + one runner request + settle,
    so it is a misconfiguration rather than an aggressive tuning choice."""
    _clear_all_config_env(monkeypatch)

    # runner_total_timeout_s must come down with it -- see the fourth validator.
    assert (
        _lease_config(delivery_budget_s=60.0, runner_total_timeout_s=60.0).delivery_budget_s == 60.0
    )

    with pytest.raises(ValueError):
        _lease_config(delivery_budget_s=59.9, runner_total_timeout_s=59.9)


def test_lease_ttl_must_span_three_heartbeat_periods(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ADR-0131: "the lease spans at least three heartbeat periods". Two lost
    heartbeats must not lose a healthy turn's lease -- reverting this validator
    lets an operator configure a fence that a single Valkey blip breaks."""
    _clear_all_config_env(monkeypatch)

    # The boundary itself PASSES: exactly three periods is the ADR's floor.
    at_the_boundary = _lease_config(delivery_lease_ttl_s=45.0, delivery_lease_heartbeat_s=15.0)
    assert at_the_boundary.delivery_lease_ttl_s == 45.0

    # One tick below the boundary fails.
    with pytest.raises(ValueError) as exc_info:
        _lease_config(delivery_lease_ttl_s=44.9, delivery_lease_heartbeat_s=15.0)

    message = str(exc_info.value)
    assert "CURIE_DELIVERY_LEASE_TTL_S" in message
    assert "CURIE_DELIVERY_LEASE_HEARTBEAT_S" in message


def test_reclaim_interval_must_be_strictly_shorter_than_the_lease_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ADR-0131: "the reclaim interval is shorter than the lease". A scan slower
    than the lease leaves an expired lease unrecovered for a whole extra scan,
    which is exactly the stranded-delivery latency the fence exists to bound."""
    _clear_all_config_env(monkeypatch)

    just_under = _lease_config(delivery_lease_ttl_s=45.0, reclaim_interval_s=44.9)
    assert just_under.reclaim_interval_s == 44.9

    # Equal is NOT shorter: the relationship is strict.
    with pytest.raises(ValueError) as exc_info:
        _lease_config(delivery_lease_ttl_s=45.0, reclaim_interval_s=45.0)

    message = str(exc_info.value)
    assert "CURIE_RECLAIM_INTERVAL_S" in message
    assert "CURIE_DELIVERY_LEASE_TTL_S" in message


def test_termination_grace_must_cover_the_budget_plus_the_shutdown_reserve(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ADR-0131: "platform termination grace is at least the execution budget
    plus shutdown reserve". Below it, a worker draining a maximum-budget turn is
    SIGKILLed at the exact moment it would settle -- the turn's terminal effect
    is lost and the entry is left pending."""
    _clear_all_config_env(monkeypatch)

    exactly_enough = _lease_config(
        delivery_budget_s=600.0,
        delivery_shutdown_reserve_s=60.0,
        termination_grace_period_s=660.0,
    )
    assert exactly_enough.termination_grace_period_s == 660.0

    with pytest.raises(ValueError) as exc_info:
        _lease_config(
            delivery_budget_s=600.0,
            delivery_shutdown_reserve_s=60.0,
            termination_grace_period_s=659.9,
        )

    message = str(exc_info.value)
    assert "CURIE_TERMINATION_GRACE_PERIOD_S" in message
    assert "CURIE_DELIVERY_BUDGET_S" in message
    assert "CURIE_DELIVERY_SHUTDOWN_RESERVE_S" in message


def test_termination_grace_of_none_skips_the_grace_validator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """None means "no platform grace declared", which is the compose and test
    case. Reverting the None guard into a comparison makes every leaseless local
    stack -- and this whole test suite -- fail to construct a config at all."""
    _clear_all_config_env(monkeypatch)

    config = _lease_config(
        delivery_budget_s=1800.0,
        delivery_shutdown_reserve_s=60.0,
        termination_grace_period_s=None,
        runner_total_timeout_s=600.0,
    )

    assert config.termination_grace_period_s is None
    assert config.delivery_budget_s == 1800.0


def test_runner_total_timeout_must_not_exceed_the_delivery_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``runner_total_timeout_s`` is now a per-request ceiling INSIDE the overall
    deadline, not an independent clock. A ceiling above the budget is always dead
    configuration and reads as if it granted more time than it does."""
    _clear_all_config_env(monkeypatch)

    equal = _lease_config(delivery_budget_s=600.0, runner_total_timeout_s=600.0)
    assert equal.runner_total_timeout_s == 600.0

    with pytest.raises(ValueError) as exc_info:
        _lease_config(delivery_budget_s=600.0, runner_total_timeout_s=600.1)

    message = str(exc_info.value)
    assert "RUNNER_TOTAL_TIMEOUT_S" in message
    assert "CURIE_DELIVERY_BUDGET_S" in message


def test_delivery_key_helpers_are_keyed_by_the_delivery_triple(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A delivery is a ``(stream, group, entry_id)``, NOT an event id: the same
    event id can legitimately be redelivered under a new entry id after a
    dead-letter, and keying the lease by event id would fence the wrong thing.
    The two keys are separate because the generation must outlive the lease --
    a generation stored in the short-lived lease key would restart at 1 on
    expiry, and a stale owner holding generation 1 would then validate."""
    _clear_all_config_env(monkeypatch)

    config = WorkerConfig(key_prefix="curie:worker")

    lease_key = config.delivery_lease_key("curie:runs", "curie-workers", "1-0")
    state_key = config.delivery_state_key("curie:runs", "curie-workers", "1-0")

    assert lease_key == "curie:worker:lease:curie:runs:curie-workers:1-0"
    assert state_key == "curie:worker:delivery:curie:runs:curie-workers:1-0"
    assert lease_key != state_key


# --- The lease-expiry reclaim threshold and the not-started copy (#2433) ------
#
# The threshold is an ADDITION to ``reclaim_min_idle_ms``, never a replacement:
# an entry with no delivery state carries no evidence a lease was ever granted
# and stays on the unchanged 900 second window. It is bounded on BOTH sides
# because each end is a distinct silent failure. Below one lease TTL the scan
# can select an entry between a healthy owner's heartbeats, before its lease has
# actually expired, which reintroduces the cross-replica dup-dispatch the lease
# exists to close. At or above the backstop the pass selects nothing XAUTOCLAIM
# was not already claiming, so it is dead code and ADR-0131's "recoverable after
# at most one short lease" bound is silently off.


def test_lease_expiry_idle_defaults_to_exactly_one_lease_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default DERIVES from the lease TTL rather than restating it.

    A static ``Field`` default cannot reference another field, so the derivation
    lives in a resolver both consumer lanes read, which is what keeps runs/eval
    parity structural. Asserting only the 45000 number would pass against a
    hard-coded literal, so the second half lowers the TTL and requires the
    threshold to follow: an operator who shortens the lease must not be left with
    an incoherent pair.
    """
    _clear_all_config_env(monkeypatch)

    config = WorkerConfig()
    assert config.lease_expired_idle_ms is None
    assert config.lease_expired_idle_ms_value() == int(config.delivery_lease_ttl_s * 1000)
    assert config.lease_expired_idle_ms_value() == 45000

    shorter = _lease_config(
        delivery_lease_ttl_s=30.0,
        delivery_lease_heartbeat_s=10.0,
        reclaim_interval_s=20.0,
    )
    assert shorter.lease_expired_idle_ms is None
    assert shorter.lease_expired_idle_ms_value() == 30000


def test_an_explicit_threshold_below_one_lease_ttl_is_rejected_at_boot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Below one lease TTL the pass can transfer a delivery somebody still owns.

    The operator must learn at boot, not at 2am, so the rejection NAMES both env
    vars and both values.
    """
    _clear_all_config_env(monkeypatch)

    with pytest.raises(ValidationError) as exc_info:
        _lease_config(delivery_lease_ttl_s=45.0, lease_expired_idle_ms=44999)

    message = str(exc_info.value)
    assert "CURIE_LEASE_EXPIRED_IDLE_MS" in message
    assert "CURIE_DELIVERY_LEASE_TTL_S" in message


def test_an_explicit_threshold_at_or_above_the_backstop_is_rejected_at_boot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """At or above the backstop the whole pass is dead configuration.

    Rejected at the boundary rather than one past it, because 900000 exactly is
    the value an operator reaches for when they mean "same as the backstop", and
    it selects nothing XAUTOCLAIM was not already claiming.
    """
    _clear_all_config_env(monkeypatch)

    with pytest.raises(ValidationError) as exc_info:
        _lease_config(lease_expired_idle_ms=900000)

    message = str(exc_info.value)
    assert "CURIE_LEASE_EXPIRED_IDLE_MS" in message
    assert "reclaim_min_idle_ms" in message


def test_a_threshold_inside_the_band_is_accepted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The control that stops the two rejections above passing vacuously.

    Without it a validator that rejected every explicit value would keep them
    both green while deleting the operator's ability to tune the knob at all.
    """
    _clear_all_config_env(monkeypatch)

    config = _lease_config(lease_expired_idle_ms=60000)
    assert config.lease_expired_idle_ms == 60000
    assert config.lease_expired_idle_ms_value() == 60000


def test_the_turn_not_started_text_is_operator_overridable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every person-facing string this pipeline emits is operator-tunable (#717).

    ``booting_text`` and ``status_text`` already carry a ``CURIE_`` alias so an
    operator can retune the voice for their own users; a hard-coded literal here
    would be the only exception, on the one line #717 is about.
    """
    _clear_all_config_env(monkeypatch)
    assert WorkerConfig().turn_not_started_text

    monkeypatch.setenv("CURIE_TURN_NOT_STARTED_TEXT", "Sorry, please resend that.")
    assert WorkerConfig().turn_not_started_text == "Sorry, please resend that."


# --- Installation-scoped upgrade drain authority (#2374) --------------------


def test_upgrade_drain_identity_defaults_preserve_standalone_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compose and a bare worker have no Helm installation boundary.

    Blank identity remains the deliberate legacy-key mode, while the hook-only
    revision and compatibility inputs default away so ``--mode status`` can
    construct its config in every worker process.
    """
    _clear_all_config_env(monkeypatch)

    config = WorkerConfig(key_prefix="curie:worker")

    assert config.installation_id == ""
    assert config.upgrade_revision is None
    assert config.upgrade_legacy_quiesce is False
    assert config.upgrade_quiesce_key() == "curie:worker:upgrade:quiesce"


def test_upgrade_drain_identity_reads_only_its_three_curie_aliases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("INSTALLATION_ID", "stray-install")
    monkeypatch.setenv("UPGRADE_REVISION", "99")
    monkeypatch.setenv("UPGRADE_LEGACY_QUIESCE", "false")
    monkeypatch.setenv("CURIE_INSTALLATION_ID", "install-env")
    monkeypatch.setenv("CURIE_UPGRADE_REVISION", "10")
    monkeypatch.setenv("CURIE_UPGRADE_LEGACY_QUIESCE", "true")

    config = WorkerConfig(key_prefix="curie:worker")

    assert config.installation_id == "install-env"
    assert config.upgrade_revision == 10
    assert config.upgrade_legacy_quiesce is True
    assert config.upgrade_quiesce_key() == "curie:worker:upgrade:quiesce:install-env"


@pytest.mark.parametrize("revision", ["nine", "9.5", "0", "-1"])
def test_upgrade_revision_must_be_a_positive_decimal_integer(
    monkeypatch: pytest.MonkeyPatch,
    revision: str,
) -> None:
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_UPGRADE_REVISION", revision)

    with pytest.raises(ValidationError, match="CURIE_UPGRADE_REVISION"):
        WorkerConfig()


def test_deliberately_blank_installation_id_keeps_the_legacy_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_config_env(monkeypatch)

    config = WorkerConfig(
        key_prefix="test:standalone",
        installation_id="",
        upgrade_revision=9,
        upgrade_legacy_quiesce=False,
    )

    assert config.installation_id == ""
    assert config.upgrade_revision == 9
    assert config.upgrade_quiesce_key() == "test:standalone:upgrade:quiesce"


# --- the inbound attachment lane's resource envelope (#2567, S4) -----------
#
# The lane's own defaults live in ``AttachmentLimits``; these fields are how an
# operator moves them. They are asserted AGAINST that dataclass rather than
# against literals, because two independently written copies of "32 MiB" is
# exactly how a chart override silently stops matching the code it configures.


@pytest.mark.parametrize(
    "overrides",
    [
        {"attachment_max_file_bytes": 0},
        {"attachment_max_file_bytes": -1},
        {"attachment_reference_ttl_seconds": 0},
        {"attachment_retention_ttl_seconds": 0},
    ],
)
def test_a_non_positive_attachment_bound_is_refused_not_read_as_unlimited(
    overrides: dict[str, object],
) -> None:
    # Same reason ``AttachmentLimits.__post_init__`` refuses it: a zero cap that
    # silently means "unlimited" is how a bounded ingestion path stops being
    # bounded, and a zero TTL mints a capability that is already expired.
    with pytest.raises(ValidationError):
        WorkerConfig.model_validate(overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        {"attachment_thread_max_files": 0},
        {"attachment_thread_max_bytes": 0},
        {"attachment_thread_max_bytes": -1},
        {"attachment_thread_prepare_timeout_seconds": 0},
        {"attachment_thread_prepare_timeout_seconds": -1.0},
    ],
)
def test_a_non_positive_thread_attachment_bound_is_refused(
    overrides: dict[str, object],
) -> None:
    """ADR 0205 decision 7: the per-thread budget bounds every boot's rebuild."""

    with pytest.raises(ValidationError):
        WorkerConfig.model_validate(overrides)


def test_the_thread_file_budget_cannot_be_smaller_than_one_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A thread budget below the per-message cap would omit files the message
    that carried them is entitled to keep (the current files are always kept),
    so the worker refuses it at startup rather than at the first boot."""

    _clear_all_config_env(monkeypatch)
    per_message = AttachmentLimits().max_files

    with pytest.raises(ValidationError):
        WorkerConfig.model_validate({"attachment_thread_max_files": per_message - 1})
    assert (
        WorkerConfig.model_validate(
            {"attachment_thread_max_files": per_message}
        ).attachment_thread_max_files
        == per_message
    )


@pytest.mark.parametrize("mode", ["all", "failures", "off"])
def test_each_turn_receipt_mode_is_read_from_its_env(
    monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    """ADR-0180 decision 1: the install chooses one of exactly three modes."""

    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_TURN_RECEIPT", mode)

    assert WorkerConfig().turn_receipt == mode


def test_the_turn_receipt_defaults_to_all(monkeypatch: pytest.MonkeyPatch) -> None:
    """An install that sets nothing keeps ADR-0117's receipt as built."""

    _clear_all_config_env(monkeypatch)

    assert WorkerConfig().turn_receipt == "all"


@pytest.mark.parametrize("raw", ["ALL", "Off", "none", "failure", "true", " off", ""])
def test_an_unknown_turn_receipt_mode_refuses_worker_boot(
    monkeypatch: pytest.MonkeyPatch, raw: str
) -> None:
    """Not a fallback to a mode nobody chose: the worker fails at config load.

    Casing and whitespace are refused rather than normalized, because the chart
    schema admits the three lowercase spellings only, and a worker that quietly
    accepted "Off" would read a value the chart would never render.
    """

    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_TURN_RECEIPT", raw)

    with pytest.raises(ValidationError) as exc_info:
        WorkerConfig()

    assert "CURIE_TURN_RECEIPT" in str(exc_info.value)


def test_an_unknown_turn_receipt_mode_is_refused_by_field_name_too() -> None:
    with pytest.raises(ValidationError):
        WorkerConfig(turn_receipt="quiet")


def test_the_chart_offers_exactly_the_turn_receipt_modes_the_worker_accepts() -> None:
    """Two languages, one vocabulary: the chart value and the worker field.

    A mode the schema admits and the worker refuses renders green and
    crash-loops the worker; a mode the worker accepts and the schema refuses is
    unreachable from Helm. The shipped default must agree on both sides too.
    """

    from typing import get_args

    from curie_worker.receipt import TurnReceiptMode

    repo_root = Path(__file__).resolve().parents[3]
    chart = repo_root / "charts" / "curie"
    values = yaml.safe_load((chart / "values.yaml").read_text())
    schema = json.loads((chart / "values.schema.json").read_text())
    offered = schema["properties"]["worker"]["properties"]["turnReceipt"]

    assert offered["type"] == "string"
    assert sorted(offered["enum"]) == sorted(get_args(TurnReceiptMode))
    assert values["worker"]["turnReceipt"] == WorkerConfig.model_fields["turn_receipt"].default
    assert values["worker"]["turnReceipt"] == "all"


def test_the_chart_defaults_match_the_worker_defaults() -> None:
    """The cross-language seam AGENTS.md names: two languages, one envelope.

    The chart templates ``worker.attachments.*`` into the worker's env AND into
    the ``attachments-init`` size cap, so a chart default that drifts from the
    Python default changes behaviour for every install that overrides nothing --
    and drifts the pod's cap away from the worker's, which
    ``charts/curie/ci/attachment-init-assertions.sh`` reads from the other end.

    Resolved from this file's location, not the working directory, so it holds
    whether pytest runs from the repo root or from apps/worker.
    """

    repo_root = Path(__file__).resolve().parents[3]
    values = yaml.safe_load((repo_root / "charts" / "curie" / "values.yaml").read_text())
    chart = values["worker"]["attachments"]
    # The declared defaults, read off the model rather than an instance, so an
    # ambient CURIE_ATTACHMENT_* in the shell cannot decide what this compares.
    fields = WorkerConfig.model_fields

    assert chart["enabled"] == fields["attachment_enabled"].default, (
        "the chart and the worker disagree about whether the inbound-attachment "
        "lane is on. This is the operator's single off switch and it ships OFF; "
        "a chart that says false while the worker defaults true means an "
        "operator who never sets the value gets a worker that downloads, parks "
        "and bills every upload for a sandbox that renders no init container."
    )
    assert chart["enabled"] is False, (
        "worker.attachments.enabled must ship false for this release: the Slack "
        "download has not been exercised against a real workspace, and this is "
        "the one knob that returns a deployment to v0.8.8 behaviour."
    )
    assert chart["maxFileBytes"] == fields["attachment_max_file_bytes"].default
    assert chart["referenceTtlSeconds"] == fields["attachment_reference_ttl_seconds"].default
    assert chart["retentionTtlSeconds"] == fields["attachment_retention_ttl_seconds"].default


def test_a_malformed_slack_identity_declaration_refuses_worker_boot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The worker parses `CURIE_SLACK_IDENTITIES` with the same shared parser
    as the API and dispatcher (ADR-0168 decision 1), so a declaration the
    chart would never render must refuse boot here too, not only at the API."""

    monkeypatch.setenv(
        "CURIE_SLACK_IDENTITIES",
        json.dumps(
            [
                {
                    "name": "second",
                    "app_token_env": "CURIE_SLACK_APP_TOKEN__0",
                    "bot_token_env": "PATH",
                    "signing_secret_env": None,
                }
            ]
        ),
    )

    with pytest.raises(ValidationError, match="CURIE_SLACK_IDENTITIES"):
        WorkerConfig()


# The connector caller signing key (ADR-0168 decision 7).


def _caller_seed() -> str:
    return base64.b64encode(bytes(SigningKey.generate())).decode()


def test_the_caller_signing_key_is_unset_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    # Unset is a stock install: no token is minted and the boot env is the one
    # it had before the key existed.
    _clear_all_config_env(monkeypatch)
    assert WorkerConfig().connector_caller_signing_key == ""


def test_a_whitespace_only_signing_key_counts_as_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A Secret value written with `echo` ends in a newline: an empty value
    # arrives as all-newline. The boot check strips before judging "is
    # anything configured at all", so this constructs cleanly rather than
    # tripping ``CallerSigningKeyError`` -- the same ``.strip()`` gate minting
    # uses (`BindingResolver.boot_env`), so the two agree on what "unset"
    # means.
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_CONNECTOR_CALLER_SIGNING_KEY", "\n")
    assert WorkerConfig().connector_caller_signing_key == "\n"


def test_the_caller_signing_key_reads_only_its_curie_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_config_env(monkeypatch)
    seed = _caller_seed()
    monkeypatch.setenv("CONNECTOR_CALLER_SIGNING_KEY", _caller_seed())
    assert WorkerConfig().connector_caller_signing_key == ""
    monkeypatch.setenv("CURIE_CONNECTOR_CALLER_SIGNING_KEY", seed)
    assert WorkerConfig().connector_caller_signing_key == seed


@pytest.mark.parametrize("raw", ["not base64!", base64.b64encode(b"short").decode()])
def test_a_malformed_caller_signing_key_is_a_startup_error(
    monkeypatch: pytest.MonkeyPatch, raw: str
) -> None:
    # Refusing at boot names the variable; minting at the first turn would fail
    # every turn instead.
    from curie_worker.config import CallerSigningKeyError

    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_CONNECTOR_CALLER_SIGNING_KEY", raw)
    with pytest.raises(CallerSigningKeyError) as refused:
        WorkerConfig()
    assert "CURIE_CONNECTOR_CALLER_SIGNING_KEY" in str(refused.value)
    assert raw not in str(refused.value)
    assert refused.value.__cause__ is None and refused.value.__suppress_context__


def test_the_caller_signing_key_stays_out_of_the_config_repr() -> None:
    seed = _caller_seed()
    config = WorkerConfig(connector_caller_signing_key=seed)
    assert config.connector_caller_signing_key == seed
    assert seed not in repr(config)


def test_an_unrelated_validation_error_does_not_print_the_signing_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A field-level failure only ever carries its OWN raw value, but a
    # model-level ``@model_validator(mode="after")`` failure -- like the
    # dead-letter-equals-stream guard below -- is handed pydantic's whole raw
    # settings input as its error's context. With a valid signing key ALSO
    # set, that context includes the key, and pydantic's default `str()`
    # rendering prints it via `input_value=...`. `hide_input_in_errors`
    # suppresses that clause from `str()`/`repr()` unconditionally, which is
    # the only rendering anything in the worker actually prints today.
    _clear_all_config_env(monkeypatch)
    seed = _caller_seed()
    monkeypatch.setenv("CURIE_CONNECTOR_CALLER_SIGNING_KEY", seed)
    monkeypatch.setenv("CURIE_STREAM", "runs")
    monkeypatch.setenv("CURIE_DEAD_LETTER_STREAM", "runs")

    with pytest.raises(ValidationError) as refused:
        WorkerConfig()

    assert "CURIE_DEAD_LETTER_STREAM" in str(refused.value)
    assert "input_value" not in str(refused.value)
    assert seed not in str(refused.value)


def test_quiesce_ttl_may_be_at_or_below_the_drain_wait() -> None:
    """#3127: the marker is a renewed lease while waiting, so the roll hold no
    longer has to outlast the drain wait; the chart caps it AT the wait."""
    for ttl in (60.0, 30.0):
        config = WorkerConfig(upgrade_drain_timeout_s=60.0, upgrade_quiesce_ttl_s=ttl)
        assert config.upgrade_quiesce_ttl_s == ttl


def test_hook_claim_lease_defaults_to_the_delivery_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cron claim outlives its turn only while the delivery could still be
    running; past the overall budget the turn is dead by construction (#2931)."""
    _clear_all_config_env(monkeypatch)
    config = _lease_config(delivery_budget_s=1800.0)
    assert config.hook_claim_lease_s is None
    assert config.effective_hook_claim_lease_s == 1800.0


def test_hook_claim_lease_reads_its_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_HOOK_CLAIM_LEASE_S", "7200")
    config = _lease_config(delivery_budget_s=600.0)
    assert config.effective_hook_claim_lease_s == 7200.0


def test_hook_claim_lease_shorter_than_the_budget_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A lease shorter than the longest turn would reclaim a live run."""
    _clear_all_config_env(monkeypatch)
    assert _lease_config(delivery_budget_s=600.0, hook_claim_lease_s=600.0)
    with pytest.raises(ValueError) as exc_info:
        _lease_config(delivery_budget_s=600.0, hook_claim_lease_s=599.0)
    assert "CURIE_HOOK_CLAIM_LEASE_S" in str(exc_info.value)


def test_the_worker_reads_no_code_host_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """ADR 0197: the worker receives the code host origin as data with each credential."""

    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_PUBLICATION_GITHUB_API_URL", "https://github.example.com/api/v3")

    config = WorkerConfig()

    assert not hasattr(config, "publication_github_api_url")
    assert not hasattr(config, "publication_github_html_base")


def test_the_publication_job_trust_bundle_comes_from_the_chart_configmap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_config_env(monkeypatch)
    assert WorkerConfig().publication_ca_bundle_config_map == ""
    assert WorkerConfig().publication_ca_bundle_key == "ca.crt"

    monkeypatch.setenv("CURIE_CODE_HOST_CA_CONFIGMAP", "corp-ca")
    monkeypatch.setenv("CURIE_CODE_HOST_CA_CONFIGMAP_KEY", "bundle.pem")

    assert WorkerConfig().publication_ca_bundle_config_map == "corp-ca"
    assert WorkerConfig().publication_ca_bundle_key == "bundle.pem"
