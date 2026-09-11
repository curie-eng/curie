"""Regression tests for WorkerConfig env-source resolution.

``populate_by_name=True`` lets tests construct the config with field-name
kwargs, but it must NOT make the env source read the bare uppercased field name
as a fallback for a field that carries a ``validation_alias``. An aliased field
must read only its ``CURIE_*`` alias; a stray generic env var (``API_KEY``,
``CREDENTIALS``, ...) in the pod env must be ignored, as it was before the
BaseSettings refactor.
"""

from __future__ import annotations

import importlib.util
import os
import socket
from pathlib import Path
from types import ModuleType

import pytest
import yaml
from curie_worker.attachments import AttachmentLimits
from curie_worker.config import WorkerConfig
from pydantic import AliasChoices, ValidationError


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
            keys = tuple(
                choice for choice in alias.choices if isinstance(choice, str)
            )
        else:
            keys = (name.upper(),)
        for key in keys:
            monkeypatch.delenv(key, raising=False)


# Every env var the OLD hand-rolled ``WorkerConfig.from_env`` read (on
# ``origin/main``), paired with a distinct sentinel and the value the field
# should hold after coercion. This is the parity oracle: the names are the exact
# old ones, so the override test proves no name drifted and no var was dropped.
_WORKER_OVERRIDES: dict[str, tuple[str, str, object]] = {
    # env var name -> (field name, raw env value, expected coerced value)
    "VALKEY_HOST": ("valkey_host", "valkey.host.example", "valkey.host.example"),
    "VALKEY_PORT": ("valkey_port", "6380", 6380),
    "VALKEY_PASSWORD": ("valkey_password", "vk-pass", "vk-pass"),
    "VALKEY_DB": ("valkey_db", "7", 7),
    "SLACK_BOT_TOKEN": ("slack_bot_token", "xoxb-sentinel", "xoxb-sentinel"),
    "SLACK_API_BASE_URL": (
        "slack_api_base_url",
        "http://slack.stub:9",
        "http://slack.stub:9",
    ),
    "DATABASE_URL": (
        "database_url",
        "postgresql+asyncpg://u:p@db:5432/x",
        "postgresql+asyncpg://u:p@db:5432/x",
    ),
    "DB_SCHEMA": ("db_schema", "myschema", "myschema"),
    "CURIE_PLUGIN_DIR": ("bundle_plugin_dir", "/custom/bundles", "/custom/bundles"),
    "CURIE_FAKE_MODEL": ("fake_model", "true", True),
    # Deliberately the NON-default value: shimmer now defaults to True, so a
    # truthy-token case would pass even if the alias were never read at all.
    "CURIE_SHIMMER": ("shimmer", "no", False),
    "CURIE_CREDENTIALS": ("credentials", "cred-sentinel", "cred-sentinel"),
    "CURIE_MODEL_BASE_URL": (
        "model_base_url",
        "http://model.local:1",
        "http://model.local:1",
    ),
    "CURIE_MODEL": ("model", "claude-sentinel", "claude-sentinel"),
    "CURIE_EVAL_STREAM": ("eval_stream", "sentinel:evals", "sentinel:evals"),
    "CURIE_EVAL_CONSUMER_GROUP": (
        "eval_consumer_group",
        "sentinel-eval-workers",
        "sentinel-eval-workers",
    ),
    "S3_ENDPOINT_URL": ("s3_endpoint_url", "http://s3.local:2", "http://s3.local:2"),
    "S3_ACCESS_KEY": ("s3_access_key", "ak-sentinel", "ak-sentinel"),
    "S3_SECRET_KEY": ("s3_secret_key", "sk-sentinel", "sk-sentinel"),
    "S3_REGION": ("s3_region", "eu-west-9", "eu-west-9"),
    "BUNDLE_BUCKET": ("bundle_bucket", "sentinel-bundles", "sentinel-bundles"),
    "CURIE_API_URL": (
        "api_base_url",
        "http://api.local:3",
        "http://api.local:3",
    ),
    "CURIE_API_KEY": ("api_key", "key-sentinel", "key-sentinel"),
    "LANGFUSE_HOST": ("langfuse_host", "http://lf.local:4", "http://lf.local:4"),
    "LANGFUSE_PUBLIC_KEY": ("langfuse_public_key", "pk-sentinel", "pk-sentinel"),
    "LANGFUSE_SECRET_KEY": ("langfuse_secret_key", "sk-lf-sentinel", "sk-lf-sentinel"),
    "CURIE_STREAM": ("stream", "sentinel:runs", "sentinel:runs"),
    "CURIE_CONSUMER_GROUP": (
        "consumer_group",
        "sentinel-workers",
        "sentinel-workers",
    ),
    "CURIE_CONSUMER_NAME": (
        "consumer_name",
        "sentinel-consumer",
        "sentinel-consumer",
    ),
    "CURIE_MAX_ATTEMPTS": ("max_attempts", "9", 9),
}


def test_aliased_field_ignores_bare_field_name_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stray bare-name env var must not leak into an aliased field."""
    monkeypatch.setenv("API_KEY", "stray")
    monkeypatch.setenv("CREDENTIALS", "stray-creds")

    config = WorkerConfig()

    assert config.api_key == "curie-dev-key"  # the default, not "stray"
    assert config.credentials == ""  # the default, not "stray-creds"


def test_aliased_field_reads_its_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    """The intended CURIE_* alias is still read from the env."""
    monkeypatch.setenv("CURIE_API_KEY", "intended")
    monkeypatch.setenv("CURIE_CREDENTIALS", "intended-creds")

    config = WorkerConfig()

    assert config.api_key == "intended"
    assert config.credentials == "intended-creds"


def test_alias_wins_over_bare_field_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """With both set, only the alias is read and the bare name is ignored."""
    monkeypatch.setenv("API_KEY", "stray")
    monkeypatch.setenv("CURIE_API_KEY", "intended")

    assert WorkerConfig().api_key == "intended"


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


def test_non_aliased_field_still_reads_plain_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fields without an alias keep reading their uppercased field name."""
    monkeypatch.setenv("VALKEY_HOST", "valkey.internal")
    monkeypatch.setenv(
        "DATABASE_URL", "postgresql+asyncpg://u:p@db:5432/curie"
    )

    config = WorkerConfig()

    assert config.valkey_host == "valkey.internal"
    assert config.database_url == "postgresql+asyncpg://u:p@db:5432/curie"


# --- Env-var parity vs the pre-pydantic from_env (review #178) ---------------


def test_defaults_parity_with_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clean env: every field must equal the exact default the old from_env produced.

    Comprehensive check -- every field is enumerated. Config drift on a default
    is a silent prod break, so this locks each default to the value the
    hand-rolled ``WorkerConfig.from_env`` (on ``origin/main``) resolved to.
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
    # Postgres
    assert (
        config.database_url
        == "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres"
    )
    assert config.db_schema == "curie"
    # Deployment-to-runtime binding
    assert config.bundle_plugin_dir == "/bundles/current"
    assert config.default_max_usd_per_day == 10.0
    assert config.default_max_output_tokens_per_run == 100000
    # Runner model + credentials
    assert config.fake_model is False
    assert config.credentials == ""
    assert config.model_base_url == ""
    assert config.model == ""
    # Shimmer: on by default so a reasoning model's pre-token silence is not
    # indistinguishable from a wedge (#1182). Must agree with the dispatcher's
    # default -- one env name drives both services.
    assert config.shimmer is True
    # Stream / consumer group
    assert config.stream == "curie:runs"
    assert config.consumer_group == "curie-workers"
    # Read loop
    assert config.read_count == 16
    assert config.read_block_ms == 5000
    # Per-thread lock
    assert config.lock_ttl_ms == 120000
    assert config.lock_acquire_timeout_s == 45.0
    assert config.lock_poll_interval_s == 0.02
    # Retry
    assert config.max_attempts == 3
    assert config.retry_backoff_base_s == 1.0
    assert config.retry_backoff_max_s == 20.0
    # Markers
    assert config.idempotency_ttl_s == 86400
    # Crash recovery
    assert config.reclaim_min_idle_ms == 900000
    assert config.reclaim_interval_s == 30.0
    assert config.dead_consumer_idle_ms == 15000
    assert config.consumer_heartbeat_ttl_ms == 15000
    assert config.consumer_capability_ttl_ms == 1800000
    # Slack edit throttle
    assert config.slack_edit_min_interval_s == 0.7
    # Runner HTTP timeouts
    assert config.runner_connect_timeout_s == 10.0
    assert config.runner_total_timeout_s == 600.0
    # Eval stream
    assert config.eval_stream == "curie:evals"
    assert config.eval_consumer_group == "curie-eval-workers"
    # RustFS / S3
    assert config.s3_endpoint_url == "http://localhost:29000"
    # Empty by default (#1559): a baked-in dev key is still an explicit credential
    # to boto3, so it shadows the ambient cloud identity (IRSA, instance role) the
    # key-free BYO object-store path relies on. Do not restore the RustFS dev pair
    # here; compose supplies it via env, and the S3_ACCESS_KEY/S3_SECRET_KEY
    # override entries above stay the guard that an operator value still wins.
    assert config.s3_access_key == ""
    assert config.s3_secret_key == ""
    assert config.s3_region == "us-east-1"
    assert config.bundle_bucket == "curie-bundles"
    # Platform API
    assert config.api_base_url == "http://localhost:8000"
    assert config.api_key == "curie-dev-key"
    assert config.report_max_attempts == 3
    assert config.report_backoff_base_s == 0.5
    # Langfuse
    assert config.langfuse_host == "http://localhost:23000"
    assert config.langfuse_public_key == "pk-lf-curie-dev"
    assert config.langfuse_secret_key == "sk-lf-curie-dev"
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


def test_overrides_parity_with_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every env var the old from_env read, set to a sentinel under its EXACT old
    name, must be read into the right field with the right coercion.

    Proves no env-var name drifted in the BaseSettings port and no read was
    dropped.
    """
    _clear_all_config_env(monkeypatch)
    for env_var, (_field, raw, _expected) in _WORKER_OVERRIDES.items():
        monkeypatch.setenv(env_var, raw)

    config = WorkerConfig()

    for env_var, (field, _raw, expected) in _WORKER_OVERRIDES.items():
        actual = getattr(config, field)
        assert actual == expected, f"{env_var} -> {field}: {actual!r} != {expected!r}"
        # Coercion parity: ints/bools must be the coerced type, not a raw str.
        assert type(actual) is type(expected), (
            f"{env_var} -> {field}: type {type(actual)} != {type(expected)}"
        )


# --- Operator-scoped model wire declaration (#514) ---------------------------
#
# Two new fields mirroring model_base_url: they read only their CURIE_* alias
# and default to "" (not declared). They are deliberately absent from the
# _WORKER_OVERRIDES parity oracle above -- that dict pins the vars the old
# hand-rolled from_env read, and these are new, not a port of anything.


def test_model_api_backend_and_env_key_default_to_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Undeclared is the default, so the producer emits nothing and the runner
    keeps its own pre-#514 defaults."""
    _clear_all_config_env(monkeypatch)

    config = WorkerConfig()

    assert config.model_api_backend == ""
    assert config.model_env_key == ""


def test_worker_config_reads_model_api_backend_and_env_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_MODEL_API_BACKEND", "messages")
    monkeypatch.setenv("CURIE_MODEL_ENV_KEY", '["ANTHROPIC_AUTH_TOKEN"]')

    config = WorkerConfig()

    assert config.model_api_backend == "messages"
    assert config.model_env_key == '["ANTHROPIC_AUTH_TOKEN"]'


def test_model_api_backend_and_env_key_ignore_bare_field_name_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Aliased like every other CURIE_* knob: a stray bare-name env var in the
    pod env must not leak in."""
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("MODEL_API_BACKEND", "chat_completions")
    monkeypatch.setenv("MODEL_ENV_KEY", "STRAY_NAME")

    config = WorkerConfig()

    assert config.model_api_backend == ""
    assert config.model_env_key == ""


def test_model_api_backend_and_env_key_populate_by_field_name() -> None:
    """populate_by_name construction (used by the binding tests) works."""
    config = WorkerConfig(model_api_backend="messages", model_env_key="MY_PROVIDER_KEY")

    assert config.model_api_backend == "messages"
    assert config.model_env_key == "MY_PROVIDER_KEY"


# --- Runner-facing API base (#678) -------------------------------------------
#
# A field distinct from api_base_url (the worker's self-dial URL): the API base a
# SPAWNED RUNNER dials. Defaults to "" (undivided) and reads only its CURIE_*
# alias. Kept out of the _WORKER_OVERRIDES parity oracle -- like the #514 fields,
# it is new, not a port of the old hand-rolled from_env.


def test_runner_api_base_url_defaults_to_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Undivided is the default: the runner reaches the API at the worker's own
    self-dial URL (k8s in-cluster, single-host local)."""
    _clear_all_config_env(monkeypatch)

    config = WorkerConfig()

    assert config.runner_api_base_url == ""


def test_worker_config_reads_runner_api_base_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_RUNNER_API_URL", "http://curie-api:8000")

    config = WorkerConfig()

    assert config.runner_api_base_url == "http://curie-api:8000"


def test_runner_api_base_url_ignores_bare_field_name_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Aliased like every other CURIE_* knob: a stray bare-name env var in the
    pod env must not leak in."""
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("RUNNER_API_BASE_URL", "http://stray:9000")

    config = WorkerConfig()

    assert config.runner_api_base_url == ""


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
def test_bool_shared_truthy_tokens(
    monkeypatch: pytest.MonkeyPatch, token: str
) -> None:
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
# cluster is not flooded. Defaults to 1 (sequential-with-backpressure) and reads
# only its CURIE_* alias; kept out of the _WORKER_OVERRIDES parity oracle since
# it is new, not a port of the old hand-rolled from_env.


def test_eval_max_concurrent_claims_defaults_to_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Single-node-safe by default: claims are created one at a time."""
    _clear_all_config_env(monkeypatch)

    config = WorkerConfig()

    assert config.eval_max_concurrent_claims == 1


def test_worker_config_reads_eval_max_concurrent_claims(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_EVAL_MAX_CONCURRENT_CLAIMS", "4")

    config = WorkerConfig()

    assert config.eval_max_concurrent_claims == 4


def test_eval_max_concurrent_claims_ignores_bare_field_name_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Aliased like every other CURIE_* knob: a stray bare-name env var must not
    leak in."""
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("EVAL_MAX_CONCURRENT_CLAIMS", "7")

    config = WorkerConfig()

    assert config.eval_max_concurrent_claims == 1


def test_eval_max_concurrent_claims_rejects_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Floor of 1: a bound of 0 would create no claims at all (no eval could run)."""
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_EVAL_MAX_CONCURRENT_CLAIMS", "0")

    with pytest.raises(ValueError):
        WorkerConfig()


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


def test_trusted_origins_parsed_from_a_bare_comma_list_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The compose.dev.yaml value: a comma list, not JSON."""
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv(
        "CURIE_SLACK_TRUSTED_ORIGINS",
        "http://localhost,http://127.0.0.1,http://host.docker.internal",
    )

    config = WorkerConfig()

    assert config.slack_trusted_origins == (
        "http://localhost",
        "http://127.0.0.1",
        "http://host.docker.internal",
    )


def test_trusted_origins_tolerates_whitespace_around_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv(
        "CURIE_SLACK_TRUSTED_ORIGINS", " http://localhost:8080 , , http://a.b "
    )

    config = WorkerConfig()

    assert config.slack_trusted_origins == ("http://localhost:8080", "http://a.b")


def test_trusted_origins_empty_env_is_the_closed_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Blank means "no extra trusted origins" -- fail closed, never a boot crash."""
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_SLACK_TRUSTED_ORIGINS", "")

    config = WorkerConfig()

    assert config.slack_trusted_origins == ()


def test_adapter_credentials_empty_env_is_an_empty_map(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same defect class: a blank ``CURIE_ADAPTER_CREDENTIALS`` is "none
    configured" (every non-Slack egress then fails closed), not a boot crash."""
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_ADAPTER_CREDENTIALS", "")

    config = WorkerConfig()

    assert config.adapter_credentials == {}


def test_adapter_credentials_still_parsed_from_json_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_ADAPTER_CREDENTIALS", '{"acme": "s3cret"}')

    config = WorkerConfig()

    assert config.adapter_credentials == {"acme": "s3cret"}


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


def test_worker_boolean_explanations_do_not_cite_removed_parser() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    config_text = (
        repo_root / "apps/worker/src/curie_worker/config.py"
    ).read_text(encoding="utf-8")
    config_start = config_text.index("def _parse_bool")
    config_end = config_text.index("\n\nBool =", config_start)
    config_region = config_text[config_start:config_end]

    test_text = Path(__file__).read_text(encoding="utf-8")
    test_start = test_text.index("# Worker boolean behavior")
    test_end = test_text.index("# --- Eval claim-creation concurrency", test_start)
    test_region = test_text[test_start:test_end]

    removed_helper = "_" + "set" + "_bool"
    retired_service = "dis" + "patcher"

    for region in (config_region, test_region):
        assert removed_helper not in region, f"boolean notes still cite {removed_helper}"
        assert retired_service not in region, (
            f"boolean notes still compare this parser to {retired_service}"
        )


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


def test_delivery_lease_fields_carry_the_adr_initial_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ADR-0131's stated initial values. Drifting a default here silently changes
    the fence's timing on every deployment that does not override it."""
    _clear_all_config_env(monkeypatch)

    config = WorkerConfig()

    assert config.delivery_budget_s == 600.0
    assert config.delivery_lease_ttl_s == 45.0
    assert config.delivery_lease_heartbeat_s == 10.0
    assert config.delivery_shutdown_reserve_s == 60.0
    # None means "no platform grace declared" (compose, tests) and SKIPS the
    # grace validator rather than guessing a value for it.
    assert config.termination_grace_period_s is None
    # Unchanged by this train, but now bound by a validator: 30 < 45 satisfies
    # the ADR's "the reclaim interval is shorter than the lease".
    assert config.reclaim_interval_s == 30.0


def test_delivery_knobs_read_their_curie_aliases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The chart templates these five as first-class env; a name drift here means
    an operator's --set silently does nothing."""
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_DELIVERY_BUDGET_S", "1800")
    monkeypatch.setenv("CURIE_DELIVERY_LEASE_TTL_S", "90")
    monkeypatch.setenv("CURIE_DELIVERY_LEASE_HEARTBEAT_S", "20")
    monkeypatch.setenv("CURIE_DELIVERY_SHUTDOWN_RESERVE_S", "45")
    monkeypatch.setenv("CURIE_TERMINATION_GRACE_PERIOD_S", "1860")
    monkeypatch.setenv("CURIE_RECLAIM_INTERVAL_S", "10")

    config = WorkerConfig()

    assert config.delivery_budget_s == 1800.0
    assert config.delivery_lease_ttl_s == 90.0
    assert config.delivery_lease_heartbeat_s == 20.0
    assert config.delivery_shutdown_reserve_s == 45.0
    assert config.termination_grace_period_s == 1860.0
    assert config.reclaim_interval_s == 10.0


def test_runner_total_timeout_reads_the_canonical_curie_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_RUNNER_TOTAL_TIMEOUT_S", "1700")
    monkeypatch.setenv("CURIE_DELIVERY_BUDGET_S", "1800")

    config = WorkerConfig()

    assert config.runner_total_timeout_s == 1700.0


def test_runner_total_timeout_keeps_the_legacy_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("RUNNER_TOTAL_TIMEOUT_S", "1600")
    monkeypatch.setenv("CURIE_DELIVERY_BUDGET_S", "1800")

    config = WorkerConfig()

    assert config.runner_total_timeout_s == 1600.0


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
    sub-minute values; only positivity and the 1800s ceiling apply here."""
    _clear_all_config_env(monkeypatch)

    short = _lease_config(runner_total_timeout_s=0.5)
    maximum = _lease_config(
        delivery_budget_s=1800.0,
        runner_total_timeout_s=1800.0,
    )

    assert short.runner_total_timeout_s == 0.5
    assert maximum.runner_total_timeout_s == 1800.0


@pytest.mark.parametrize(
    ("value", "error_type"),
    [(0.0, "greater_than"), (-0.1, "greater_than"), (1800.1, "less_than_equal")],
)
def test_runner_total_timeout_rejects_programmatic_values_outside_its_bounds(
    monkeypatch: pytest.MonkeyPatch,
    value: float,
    error_type: str,
) -> None:
    _clear_all_config_env(monkeypatch)

    with pytest.raises(ValidationError) as exc_info:
        _lease_config(delivery_budget_s=1800.0, runner_total_timeout_s=value)

    assert any(
        error["loc"] == ("runner_total_timeout_s",)
        and error["type"] == error_type
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
        error["loc"] == ("CURIE_RUNNER_TOTAL_TIMEOUT_S",)
        and error["type"] == "greater_than"
        for error in exc_info.value.errors()
    )


def test_delivery_knobs_ignore_bare_field_name_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Aliased like every other CURIE_* knob: a stray bare-name env var in the
    pod env must not leak into the fence's timing."""
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("DELIVERY_BUDGET_S", "1800")
    monkeypatch.setenv("DELIVERY_LEASE_TTL_S", "1")
    monkeypatch.setenv("DELIVERY_LEASE_HEARTBEAT_S", "1")
    monkeypatch.setenv("DELIVERY_SHUTDOWN_RESERVE_S", "0")
    monkeypatch.setenv("TERMINATION_GRACE_PERIOD_S", "5")

    config = WorkerConfig()

    assert config.delivery_budget_s == 600.0
    assert config.delivery_lease_ttl_s == 45.0
    assert config.delivery_lease_heartbeat_s == 10.0
    assert config.delivery_shutdown_reserve_s == 60.0
    assert config.termination_grace_period_s is None


def test_delivery_budget_accepts_the_adr_maximum_and_rejects_above_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """1800s is the ADR's stated maximum ("Operators may configure 1,800
    seconds"). A higher value would silently require a termination grace the
    chart schema will not accept, so it is refused here instead."""
    _clear_all_config_env(monkeypatch)

    assert _lease_config(delivery_budget_s=1800.0).delivery_budget_s == 1800.0

    with pytest.raises(ValueError):
        _lease_config(delivery_budget_s=1800.5)


def test_delivery_budget_accepts_its_floor_and_rejects_below_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A budget under a minute cannot cover claim + one runner request + settle,
    so it is a misconfiguration rather than an aggressive tuning choice."""
    _clear_all_config_env(monkeypatch)

    # runner_total_timeout_s must come down with it -- see the fourth validator.
    assert (
        _lease_config(delivery_budget_s=60.0, runner_total_timeout_s=60.0).delivery_budget_s
        == 60.0
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
    assert config.lease_expired_idle_ms_value() == int(
        config.delivery_lease_ttl_s * 1000
    )
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
    assert (
        config.upgrade_quiesce_key()
        == "curie:worker:upgrade:quiesce:install-env"
    )


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


def test_attachment_fields_default_to_the_lanes_own_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_config_env(monkeypatch)

    config = WorkerConfig()
    default = AttachmentLimits()

    assert config.attachment_max_file_bytes == default.max_file_bytes
    assert config.attachment_reference_ttl_seconds == default.reference_ttl_seconds
    assert config.attachment_retention_ttl_seconds == default.retention_ttl_seconds


def test_attachment_fields_read_only_their_curie_aliases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The chart templates these as first-class env; a name drift here means an
    operator's --set silently does nothing. The bare field names are set to
    decoy values so ``populate_by_name`` cannot satisfy the assertions."""
    _clear_all_config_env(monkeypatch)
    for name in (
        "ATTACHMENT_MAX_FILE_BYTES",
        "ATTACHMENT_REFERENCE_TTL_SECONDS",
        "ATTACHMENT_RETENTION_TTL_SECONDS",
    ):
        monkeypatch.setenv(name, "999999")
    # The off switch reads the same way: a name drift here is an operator who
    # sets the chart value, sees it in the pod env, and still gets a worker that
    # downloads and parks every upload.
    monkeypatch.setenv("ATTACHMENT_ENABLED", "false")
    monkeypatch.setenv("CURIE_ATTACHMENT_ENABLED", "true")
    monkeypatch.setenv("CURIE_ATTACHMENT_MAX_FILE_BYTES", "8388608")
    monkeypatch.setenv("CURIE_ATTACHMENT_REFERENCE_TTL_SECONDS", "120")
    monkeypatch.setenv("CURIE_ATTACHMENT_RETENTION_TTL_SECONDS", "900")

    config = WorkerConfig()

    assert config.attachment_enabled is True
    assert config.attachment_max_file_bytes == 8388608
    assert config.attachment_reference_ttl_seconds == 120
    assert config.attachment_retention_ttl_seconds == 900


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
