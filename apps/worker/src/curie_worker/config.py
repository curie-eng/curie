"""Typed configuration for the worker kernel, read from the environment.

The kernel needs a Valkey connection (the stream it consumes plus its locks and
markers), a Slack bot token (to edit the placeholder in place), the stream and
consumer-group identity, and the tunables for retry, per-thread locking, and
crash-recovery reclaim. Substrate wiring (namespace, warm pool, runner port)
lives in ``SubstrateConfig`` and is assembled separately by the entrypoint.

``WorkerConfig`` is a ``pydantic_settings.BaseSettings`` (the house pattern, see
``apps/api``): construct it with no arguments and it reads the environment on
init, falling back to the defaults below for anything absent. The CURIE_-prefixed
knobs map through per-field ``validation_alias``; the rest read the uppercased
field name (VALKEY_HOST, DATABASE_URL, S3_ENDPOINT_URL, LANGFUSE_HOST, ...).
"""

from __future__ import annotations

import json
import os
import socket
from typing import Annotated, Any

from aci_protocol.service_config import (
    API_KEY_ENV,
    DEAD_LETTER_STREAM_ENV,
    EVAL_CONSUMER_GROUP_DEFAULT,
    EVAL_STREAM_DEFAULT,
    HEARTBEAT_FILE_ENV,
    HEARTBEAT_INTERVAL_ENV,
    RUNS_STREAM_DEFAULT,
    SHIMMER_ENV,
    STREAM_ENV,
    WORKER_GROUP_DEFAULT,
    AliasOnlyEnvSource,
    api_url_validation_alias,
    derive_dead_letter_stream_name,
    warn_if_deprecated_api_url_env,
)
from pydantic import AliasChoices, BeforeValidator, Field, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict
from pydantic_settings.sources import (
    PydanticBaseSettingsSource,
)


def _default_consumer_name() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


def _parse_bool(value: object) -> bool:
    """Parse the truthy env-string set the worker has always accepted.

    A real bool passes through (so kwarg construction in tests is unchanged); any
    other string is truthy only when it is one of the accepted tokens, matching
    the previous hand rolled ``_b``.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes")
    return bool(value)


Bool = Annotated[bool, BeforeValidator(_parse_bool)]


def _parse_adapter_credentials(value: object) -> object:
    """Parse ``CURIE_ADAPTER_CREDENTIALS`` (a JSON object) into a dict.

    A real dict passes through, so kwarg construction in tests is unchanged. A
    blank env var is an empty map (no adapter credentials configured), which
    makes every non-Slack egress fail closed rather than send anonymously.
    Malformed JSON is a startup error on purpose: a worker that silently came up
    with no credentials would look healthy and deliver nothing.
    """
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        parsed = json.loads(text)
        if not isinstance(parsed, dict):
            raise ValueError("CURIE_ADAPTER_CREDENTIALS must be a JSON object")
        return parsed
    return value


# ``NoDecode`` is load-bearing, not decoration: see ``TrustedOrigins`` below.
# Without it a blank ``CURIE_ADAPTER_CREDENTIALS`` (the documented "none
# configured" value) is JSON-decoded by the settings source and raises before
# ``_parse_adapter_credentials`` can map it to ``{}``.
AdapterCredentials = Annotated[
    dict[str, str], NoDecode, BeforeValidator(_parse_adapter_credentials)
]


def _parse_trusted_origins(value: object) -> object:
    """Parse ``CURIE_SLACK_TRUSTED_ORIGINS`` (comma-separated URLs) into a tuple.

    A real sequence passes through, so kwarg construction in tests is unchanged.
    Blank entries are dropped, so an empty or whitespace-only env var means "no
    extra trusted origins" -- the closed default, where the configured Slack
    origin is the only one honored.

    Each entry is ``scheme://host[:port]``. An entry WITH a port matches that
    origin exactly. An entry that OMITS the port
    (``http://host.docker.internal``) trusts any port on that scheme+host, which
    exists for one reason: the CLI's dev stub binds an EPHEMERAL port on
    ``curie chat`` and on ``cluster message --listen-port 0``, so no fixed port
    can be configured ahead of time. A portless entry is therefore a DEV-ONLY
    affordance -- it trusts every port on a host, so production must never set
    one (and should usually set nothing at all, leaving the configured Slack
    origin as the only trusted one).
    """
    if isinstance(value, str):
        return tuple(part.strip() for part in value.split(",") if part.strip())
    if isinstance(value, (list, tuple)):
        return tuple(str(part).strip() for part in value if str(part).strip())
    return value


# ``NoDecode`` hands the validator the RAW env string. pydantic-settings treats
# a tuple field as "complex" and JSON-decodes it inside the env source, BEFORE
# any field validator runs -- so the comma list compose.dev.yaml exports
# ("http://localhost,http://127.0.0.1,...") raised SettingsError and killed the
# worker at boot, and the BeforeValidator below was only ever reachable when the
# env var was absent. Same declared-not-parsed defect as the boot env's (#1195).
TrustedOrigins = Annotated[tuple[str, ...], NoDecode, BeforeValidator(_parse_trusted_origins)]
CommaSeparatedNames = Annotated[tuple[str, ...], NoDecode, BeforeValidator(_parse_trusted_origins)]


class WorkerConfig(BaseSettings):
    """Everything the kernel needs, in one typed object."""

    model_config = SettingsConfigDict(frozen=True, populate_by_name=True, extra="ignore")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Swap the env source so aliased fields read only their alias."""
        # Surface the CURIE_API_BASE_URL -> CURIE_API_URL rename (#496) at the
        # single point every WorkerConfig load passes through.
        warn_if_deprecated_api_url_env()
        return (
            init_settings,
            AliasOnlyEnvSource(settings_cls),
            dotenv_settings,
            file_secret_settings,
        )

    # Valkey
    valkey_host: str = "localhost"
    valkey_port: int = 6379
    valkey_password: str = ""
    valkey_db: int = 0
    # TLS transport for a BYO Valkey (VALKEY_TLS, rendered by the chart from
    # valkey.tls). Off by default: the in-chart store and the compose lane are
    # both cleartext by design (#2315).
    valkey_tls: bool = False

    # Slack
    slack_bot_token: str = ""
    # The worker's DEFAULT Slack Web API base URL: the endpoint used to finalize a
    # turn whose reply handle carries no per-turn endpoint (issue #19). Unset = the
    # real Slack API. A turn that carries its own reply endpoint (e.g. a CLI stub)
    # overrides this per turn, so a real workspace and a no-Slack CLI stub can
    # coexist on one worker instead of contending for this single setting.
    slack_api_base_url: str = ""

    # ADDITIONAL Slack origins a per-turn reply endpoint may name (ADR-0096
    # D4.4), comma-separated. Empty by default, which leaves the configured
    # ``slack_api_base_url`` (or real Slack) as the only trusted origin. This
    # exists for the dev/CLI stub: `local message` advertises
    # ``host.docker.internal`` and `cluster message` a routable host, neither of
    # which the single default base URL can also name, so the origin pin would
    # otherwise refuse the local loop. It is deliberately OPERATOR CONFIG and
    # never wire-supplied -- an origin an operator typed is trusted; an origin a
    # turn carried is exactly the credential-capture vector D4.4 closes.
    slack_trusted_origins: TrustedOrigins = Field(
        default=(), validation_alias="CURIE_SLACK_TRUSTED_ORIGINS"
    )

    # Per-ADAPTER egress credentials (ADR-0096 D4.2), a JSON object mapping an
    # operator-chosen adapter slug (the binding row's ``adapter``) to the secret
    # the worker presents as ``X-Curie-Adapter-Secret``. Per-adapter rather than
    # per-kind so compromising one adapter yields one secret, for one binding.
    # An adapter with no entry here makes its egress RAISE rather than send
    # anonymously: an unauthenticated platform request lets any reachable pod be
    # impersonated and gives the adapter no way to tell the platform from an
    # attacker.
    adapter_credentials: AdapterCredentials = Field(
        default_factory=dict, validation_alias="CURIE_ADAPTER_CREDENTIALS"
    )

    # Postgres (read-only): resolve channel -> agent -> deployment -> version.
    # Matches the API's DATABASE_URL / DB_SCHEMA so the worker reads the same DB.
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres"
    db_schema: str = "curie"

    # Deployment-to-runtime binding. The plugin dir is the local path the runner
    # reads; sandbox provisioning fetches CURIE_BUNDLE_REF into it. Platform
    # default budget applies when an agent's budget columns are NULL.
    bundle_plugin_dir: str = Field(default="/bundles/current", validation_alias="CURIE_PLUGIN_DIR")
    default_max_usd_per_day: float = 10.0
    default_max_output_tokens_per_run: int = 100000

    # Runner model + credentials passthrough (injected per-claim into every boot
    # env). fake_model runs the runner's canned FakeModelSession (no Anthropic
    # call, no credential needed) -- the middle-mode default on a laptop.
    # credentials is the opaque CURIE_CREDENTIALS the runner forwards to the
    # model call; it is never logged and never required when fake_model is on.
    fake_model: Bool = Field(default=False, validation_alias="CURIE_FAKE_MODEL")
    credentials: str = Field(default="", validation_alias="CURIE_CREDENTIALS")
    # Local model demo path: the worker can point the runner at an
    # Anthropic-compatible local endpoint without changing the fake-model default.
    model_base_url: str = Field(default="", validation_alias="CURIE_MODEL_BASE_URL")
    # The endpoint's wire protocol and the env var(s) carrying the credential
    # (#514), both declared rather than inferred. Operator scope like
    # model_base_url: they select which env var a credential is read from and
    # which wire protocol is dialed, so an agent author must never set them.
    model_api_backend: str = Field(default="", validation_alias="CURIE_MODEL_API_BACKEND")
    model_env_key: str = Field(default="", validation_alias="CURIE_MODEL_ENV_KEY")
    model: str = Field(default="", validation_alias="CURIE_MODEL")
    # Platform-default thinking depth (#1182, ADR-0098), the lower of the two
    # operator layers; a per-agent `agents.thinking` overrides it. Empty means
    # unset, in which case the runner is sent no thinking configuration at all
    # and the model's own default applies -- the pre-#1182 behavior verbatim.
    # Operator-only by design: never derived from the agent row's bundle, and a
    # bundle has no surface for it at any tier.
    thinking: str = Field(default="", validation_alias="CURIE_THINKING")

    # Opt-in false-completion check (#517, #669), operator scope like
    # model_api_backend/model_env_key: forwarded verbatim into every boot env as
    # CURIE_FALSE_COMPLETION_CHECK, never derived from the agent row. The
    # runner reads it as a direct env var outside the frozen BootEnv contract
    # (runner/src/curie_runner/config.py) since it is authority-free and
    # observe-only; this field is the missing operator-facing producer that
    # closes the loop -- without it the runner-side read is unreachable in any
    # deployed sandbox (#669). Default off preserves current behavior.
    false_completion_check: Bool = Field(
        default=False, validation_alias="CURIE_FALSE_COMPLETION_CHECK"
    )

    # Where this release's hosted connectors live (ADR-0086, #1118). The runner
    # derives each declared connector's MCP URL from the Service Curie created,
    # `<release>-<agent>-mcp-<connector>` in `<namespace>`, and neither value is
    # knowable inside the sandbox: the Helm release name lives with whoever ran
    # `cluster up`. Both default empty, and the boot env carries the connector
    # scope only when the whole set resolves -- so a worker that predates the
    # chart plumbing simply mounts no hosted connector rather than emitting a
    # scope that names a Service which cannot exist.
    connector_release: str = Field(default="", validation_alias="CURIE_RELEASE")
    connector_namespace: str = Field(default="", validation_alias="CURIE_NAMESPACE")

    # The shimmer caption, kept SEPARATE from the dispatcher's placeholder text
    # because the two surfaces have different grammar. Slack renders an
    # assistant-thread status as "<App Name> <status>" and inserts the app name
    # itself, so the status has to read as a CONTINUATION of the app name ("Curie
    # is working on your request...") while the message body is a standalone
    # sentence ("On it. Working on your request."). Reusing one string for both
    # produced "Curie On it. Working on your request." in the shimmer.
    #
    # Read here rather than in the dispatcher since #1312 moved the whole shimmer
    # to this side; the env name is unchanged. It is a plain literal, not a
    # shared constant from
    # aci_protocol.service_config, because that module is for names BOTH services
    # read and only the worker reads this one now.
    # https://docs.slack.dev/reference/methods/assistant.threads.setStatus
    status_text: str = Field(
        default="is working on your request...",
        validation_alias="CURIE_STATUS_TEXT",
    )
    # When true, set the Slack assistant-thread status (the native "shimmer" on
    # the app name) while a turn runs, and clear it when the turn ends. Both
    # halves live here (#1312). The clear is not optional bookkeeping: editing the
    # placeholder does not auto-clear a status, because Slack only auto-clears
    # when the app POSTS a message and this pipeline edits the placeholder.
    #
    # ON by default: during a turn the placeholder only changes when the model
    # emits text, so a model that spends 30s reasoning before its first token
    # leaves the thread visibly frozen, and an operator cannot tell that from a
    # wedge (issue #1182). The shimmer is Slack's own affordance for exactly that,
    # and it is strictly additive -- it neither edits the message nor notifies
    # anyone. A workspace whose app lacks the "Agents & AI Apps" feature just
    # skips it (the calls are best-effort and log at debug), so defaulting it on
    # cannot break a deployment; see slack-app-manifest.yaml for the one-click
    # enablement that has no manifest key.
    #
    # One flag now governs one producer. Before #1312 the dispatcher set the
    # status and the worker cleared it, so the two had to be kept in agreement by
    # an operator setting the same env twice; disagreeing left a caption nothing
    # would clear until Slack's own two-minute timeout.
    shimmer: Bool = Field(default=True, validation_alias=SHIMMER_ENV)

    # When true, suppress intermediate placeholder edits while streaming so the
    # placeholder gets exactly one chat.update (the final) -- rate-limit friendly
    # and flicker-free; pair with the shimmer for liveness. Default false
    # preserves live-edit streaming.
    slack_no_edit_streaming: Bool = Field(
        default=False, validation_alias="CURIE_SLACK_NO_EDIT_STREAMING"
    )

    # Shown by editing the dispatcher's placeholder to a "booting" state before the
    # sandbox claim, so the cold-boot wait is not silent. Best-effort; overridable.
    # Kept free of internal implementation vocabulary ("runner", "sandbox") by
    # default (#717) -- an end user talking to an agent should never see curie's
    # own architecture terms in a status line.
    booting_text: str = Field(
        default="Working on it...",
        validation_alias="CURIE_BOOTING_TEXT",
    )

    # Stream / consumer group (must match the dispatcher's CURIE_STREAM). The
    # defaults are the shared declarations (#492) so a rename cannot drift this
    # lane out of sync with the dispatcher/API/CLI; the validation_alias keeps
    # #496's env-override path intact.
    stream: str = Field(default=RUNS_STREAM_DEFAULT, validation_alias=STREAM_ENV)
    consumer_group: str = Field(
        default=WORKER_GROUP_DEFAULT, validation_alias="CURIE_CONSUMER_GROUP"
    )
    consumer_name: str = Field(
        default_factory=_default_consumer_name,
        validation_alias="CURIE_CONSUMER_NAME",
    )

    # Delivery cap + dead-letter graveyard (#505). ``max_delivery`` is the maximum
    # number of times a stream entry may be DELIVERED to a handler before it is
    # moved to the dead-letter stream and acked off the group. It is NOT
    # ``max_attempts`` below: that governs the kernel's flag-clean per-turn retry
    # *classification* (a completely different mechanism operating inside a single
    # delivery). Conflating the two silently changes kernel retry behavior.
    #
    # The count is read from Valkey's pending-entries list, so it is durable: a
    # restarted worker sees the accumulated count and still caps. The floor is 2
    # because ``max_delivery=1`` would dead-letter every ordinary worker crash on
    # its first reclaim, and values below 3 undermine ADR-0013 crash recovery
    # (which relies on a reclaim actually retrying the entry).
    #
    # A healthy long turn now accrues NO self-inflicted deliveries (ADR-0131).
    # The delivery lease's heartbeat resets same-owner PEL idle with
    # ``XCLAIM ... JUSTID``, which does not increment the delivery counter, and
    # reclaim consults the lease's liveness before dispatching or dead-lettering
    # a claim. This is a change from the pre-lease world: a single delivery
    # could span up to ``max_attempts * runner_total_timeout_s`` (~1800s at
    # defaults), which exceeded ``reclaim_min_idle_ms`` (900s), so another
    # replica could reclaim a turn that was still working and bump its delivery
    # count -- roughly 2 self-inflicted deliveries at defaults. That headroom no
    # longer needs to exist; do not read the lease's arrival as license to lower
    # ``max_delivery`` on the strength of it. The cap and its ``ge=2`` floor
    # DO NOT CHANGE -- ADR-0039 stands, and weakening the cap is the #505 total
    # stall regression, not a simplification.
    max_delivery: int = Field(default=5, ge=2, validation_alias="CURIE_MAX_DELIVERY")
    # Empty means "derive ``<stream>:dead``" at the use site; a static Field
    # default cannot reference ``self.stream``. An explicit override equal to
    # ``stream`` is rejected outright -- see ``_reject_self_targeting_graveyard``.
    dead_letter_stream: str = Field(default="", validation_alias=DEAD_LETTER_STREAM_ENV)
    # The graveyard is capped with an approximate MAXLEN on every XADD. The
    # unparseable-poison path dead-letters per INBOUND entry, so a wire-format
    # drift that makes entries unparseable en masse would otherwise grow the
    # graveyard at full ingest rate -- on the same Valkey that holds the kernel's
    # per-thread locks and side-effect markers, i.e. a platform-wide OOM. The
    # trade is deliberate and lossy: under a flood the oldest dead-letter rows are
    # evicted, so graveyard records are best-effort, not a durable audit log.
    dead_letter_maxlen: int = Field(
        default=10000, ge=1, validation_alias="CURIE_DEAD_LETTER_MAXLEN"
    )

    @model_validator(mode="after")
    def _reject_self_targeting_graveyard(self) -> WorkerConfig:
        """Fail at construction if the graveyard points back at the source stream.

        ``_dead_letter`` XADDs the original payload to the dead-letter stream and
        only then XACKs it. If that target IS the source stream, the payload is
        re-queued to the very stream it was consumed from: a valid failure gets
        re-consumed under a fresh entry id, and an unparseable one forms a hot
        loop that re-creates the permanent stall the delivery cap exists to
        prevent. Rejecting at config/startup means an operator learns at boot
        rather than during an incident; the derived ``<stream>:dead`` default can
        never collide, so only an explicit override trips this.
        """
        if self.dead_letter_stream and self.dead_letter_stream == self.stream:
            raise ValueError(
                "CURIE_DEAD_LETTER_STREAM must not equal CURIE_STREAM "
                f"({self.stream!r}): dead-lettering onto the source stream "
                "re-queues failures forever"
            )
        return self

    @model_validator(mode="after")
    def _connector_reconciler_knows_where_it_is(self) -> WorkerConfig:
        """Fail at construction if the reconciler is on but unaddressed.

        Connector object names are built from the release name and the app name
        (`<release>-<agent>-mcp-<connector>`). With either missing, every name
        the reconciler renders differs from the names that actually exist: it
        would find nothing of its own in the namespace, create a parallel set
        under wrong names, and leave the real connectors unmanaged. That is
        silent and looks like it is working, so it is rejected at boot instead.
        """

        if not self.connector_reconcile_enabled:
            return self
        missing = [
            env
            for env, value in (
                ("CURIE_NAMESPACE", self.connector_namespace),
                ("CURIE_RELEASE", self.connector_release),
                ("CURIE_CONNECTOR_APP_NAME", self.connector_app_name),
            )
            if not value.strip()
        ]
        if missing:
            raise ValueError(
                f"CURIE_CONNECTOR_RECONCILE is on but {', '.join(missing)} "
                "is unset; connector object names are derived from these, so "
                "the reconciler would manage a parallel set under wrong names"
            )
        return self

    @model_validator(mode="after")
    def _lease_spans_three_heartbeats(self) -> WorkerConfig:
        """Fail at construction if the lease cannot survive two lost heartbeats.

        ADR-0131: "the lease spans at least three heartbeat periods". With a
        Valkey blip or a slow renewal, one missed heartbeat is routine; the
        floor of three periods means two consecutive misses still leave a
        healthy owner's lease live. A tighter ratio would drop a healthy long
        turn on a single transient hiccup -- the exact flakiness the lease
        exists to avoid, not introduce.
        """
        if self.delivery_lease_ttl_s < 3 * self.delivery_lease_heartbeat_s:
            raise ValueError(
                "CURIE_DELIVERY_LEASE_TTL_S "
                f"({self.delivery_lease_ttl_s!r}) must be at least 3x "
                "CURIE_DELIVERY_LEASE_HEARTBEAT_S "
                f"({self.delivery_lease_heartbeat_s!r}): a shorter span drops a "
                "healthy owner's lease on a single missed heartbeat"
            )
        return self

    @model_validator(mode="after")
    def _reclaim_scan_shorter_than_lease(self) -> WorkerConfig:
        """Fail at construction if the reclaim scan is not faster than the lease.

        ADR-0131: "the reclaim interval is shorter than the lease". A scan
        cadence at or above the lease TTL leaves an expired lease unrecovered
        for a whole extra scan pass -- directly widening the stranded-delivery
        recovery window the lease exists to bound.
        """
        if self.reclaim_interval_s >= self.delivery_lease_ttl_s:
            raise ValueError(
                "CURIE_RECLAIM_INTERVAL_S "
                f"({self.reclaim_interval_s!r}) must be strictly shorter than "
                f"CURIE_DELIVERY_LEASE_TTL_S ({self.delivery_lease_ttl_s!r}): "
                "a scan slower than the lease leaves an expired lease "
                "unrecovered for a whole extra scan"
            )
        return self

    @model_validator(mode="after")
    def _termination_grace_covers_the_budget(self) -> WorkerConfig:
        """Fail at construction if platform grace cannot cover budget + reserve.

        ADR-0131: "platform termination grace is at least the execution budget
        plus shutdown reserve." Below it, a worker draining a maximum-budget
        turn is SIGKILLed at the exact moment it would settle -- the turn's
        terminal effect is lost and the entry is left pending. ``None`` means
        no platform grace was declared (compose, tests) and skips this check
        entirely rather than guessing a value to compare against.
        """
        if self.termination_grace_period_s is None:
            return self
        required = self.delivery_budget_s + self.delivery_shutdown_reserve_s
        if self.termination_grace_period_s < required:
            raise ValueError(
                "CURIE_TERMINATION_GRACE_PERIOD_S "
                f"({self.termination_grace_period_s!r}) must be at least "
                "CURIE_DELIVERY_BUDGET_S + CURIE_DELIVERY_SHUTDOWN_RESERVE_S "
                f"({self.delivery_budget_s!r} + "
                f"{self.delivery_shutdown_reserve_s!r} = {required!r}): a "
                "shorter grace SIGKILLs a draining worker before it can settle "
                "a maximum-budget turn"
            )
        return self

    @model_validator(mode="after")
    def _runner_request_fits_the_budget(self) -> WorkerConfig:
        """Fail at construction if the per-request ceiling exceeds the budget.

        ``runner_total_timeout_s`` is now a per-request ceiling INSIDE the
        overall delivery budget, not an independent clock. A ceiling above the
        budget is always dead configuration -- the budget expires first on
        every request -- and reads as if it granted more time than it does.
        """
        if self.runner_total_timeout_s > self.delivery_budget_s:
            raise ValueError(
                "CURIE_RUNNER_TOTAL_TIMEOUT_S "
                f"({self.runner_total_timeout_s!r}) must not exceed "
                f"CURIE_DELIVERY_BUDGET_S ({self.delivery_budget_s!r}): a "
                "per-request ceiling above the overall budget is dead "
                "configuration, since the budget always expires first"
            )
        return self

    @model_validator(mode="after")
    def _quiesce_outlives_the_drain_wait(self) -> WorkerConfig:
        """Fail at construction if the quiesce flag can lapse mid-drain.

        The gate sets the flag once and then waits up to
        ``upgrade_drain_timeout_s`` for the in-flight deliveries to settle. A
        TTL at or below that wait expires the flag while the gate is still
        waiting, so the replicas resume claiming into an upgrade that is about
        to roll them -- re-creating the very interruption the gate exists to
        prevent, and doing it silently (the gate would still report a clean
        drain). Strictly greater, so there is real headroom.
        """
        if self.upgrade_quiesce_ttl_s <= self.upgrade_drain_timeout_s:
            raise ValueError(
                "CURIE_UPGRADE_QUIESCE_TTL_S "
                f"({self.upgrade_quiesce_ttl_s!r}) must be strictly greater than "
                "CURIE_UPGRADE_DRAIN_TIMEOUT_S "
                f"({self.upgrade_drain_timeout_s!r}): a flag that lapses mid-drain "
                "lets the replicas resume claiming into a roll that is about to "
                "interrupt them"
            )
        return self

    # Read loop
    read_count: int = 16
    read_block_ms: int = 5000

    # Per-thread lock (serializes the routing decision + turn opening across
    # workers so a thread never has two live sessions). The TTL must exceed the
    # worst-case critical section (a cold claim can take up to the substrate's
    # claim_timeout, default 90s, now bounded end-to-end across the claim's bind
    # and serviceFQDN phases by a single shared deadline in
    # SandboxSubstrate._claim_fresh) so the lock never lapses mid-section and lets
    # a second worker open a concurrent turn. 90s claim + slack/route overhead
    # stays safely under this 120s TTL; if you raise claim_timeout keep it below
    # this.
    lock_ttl_ms: int = 120000
    lock_acquire_timeout_s: float = 45.0
    lock_poll_interval_s: float = 0.02

    # Retry (flag-clean failures only; see the no-retry-after-side-effects rule)
    max_attempts: int = Field(default=3, validation_alias="CURIE_MAX_ATTEMPTS")
    retry_backoff_base_s: float = Field(default=1.0, gt=0)
    retry_backoff_max_s: float = Field(default=20.0, gt=0)

    # Markers
    idempotency_ttl_s: int = 86400

    # The completion outbox (ADR-0096 EB-B6). ``grace`` keeps the sweeper out of
    # the kernel's own emit window, so the normal path is not racing a sweeper on
    # every turn; ``max_retention`` is how long an undelivered completion is kept
    # before it is cleared LOUDLY, well beyond any outage worth riding out.
    completion_sweep_grace_s: float = 60.0
    completion_max_retention_s: float = 604800.0
    # One sweep pass is BOUNDED twice over, because the startup sweep runs
    # against exactly the backlog an outage left behind: at most ``batch``
    # members are sampled per pass, and the pass stops once ``budget`` seconds
    # have elapsed. Each delivery attempt is an HTTP call with the sink's own
    # timeout, so an unbounded pass over an unreachable adapter is measured in
    # hours. The sweeper runs on the maintenance cadence, so the remainder is
    # simply drained by the passes that follow.
    completion_sweep_batch: int = Field(default=64, gt=0)
    completion_sweep_budget_s: float = Field(default=30.0, gt=0)

    # Crash recovery: reclaim stream entries pending longer than this, and run
    # the orphan-claim reaper, on this cadence.
    #
    # This window is now a COMPATIBILITY BACKSTOP behind the delivery lease
    # (ADR-0131), not the primary guard. Before the lease, this was the only
    # signal available: one delivery is not one runner call -- the kernel may
    # retry a flag-clean failure up to max_attempts (3) times WITHIN a single
    # delivery, each bounded by runner_total_timeout_s (600s), so a healthy
    # delivery could legitimately span up to ~max_attempts *
    # runner_total_timeout_s = ~1800s, twice this 900s idle threshold -- and a
    # long healthy turn could therefore be reclaimed by another replica and
    # accrue delivery count. That cross-replica dup-dispatch is the defect this
    # ticket fixes: the lease is checked for liveness before any reclaim path
    # dispatches or dead-letters, so a live-leased entry is skipped regardless
    # of this idle window. This value stays unchanged at 900000 as the backstop
    # for the case a lease itself is somehow absent or already expired (e.g. a
    # crashed owner past its lease TTL, where this threshold provides an
    # independent, coarser second opinion). Raising it past
    # max_attempts * runner_total_timeout_s would close the pre-lease gap
    # entirely, but is not needed now that the lease is the primary guard, and
    # would slow crash recovery for entries the lease mechanism cannot cover.
    reclaim_min_idle_ms: int = 900000
    # Unchanged at 30.0; now bound by ``_reclaim_scan_shorter_than_lease``,
    # which enforces the ADR's actual requirement (scan strictly shorter than
    # the lease TTL) rather than a specific number. See the delivery-lease
    # block above for why 30.0 (not the ADR's stated initial 10.0) is kept: it
    # is the whole maintenance-tick cadence, not a dedicated lease-scan loop.
    reclaim_interval_s: float = Field(
        default=30.0, gt=0, validation_alias="CURIE_RECLAIM_INTERVAL_S"
    )
    # Prompt reclaim for a consumer that has stopped interacting with the
    # group (#1532). Entry idle is the wrong signal: a live replica's
    # A cheap observation threshold for prompt peer recovery (#1532), not a
    # liveness proof. XINFO consumer idle also rises while a live worker drains
    # an in-flight turn or waits at its concurrency limit, so prompt reclaim is
    # gated by the independent renewable lease below. Default is 3x
    # ``read_block_ms`` to avoid probing peers during ordinary read blocking.
    dead_consumer_idle_ms: int = Field(default=15000, ge=0)
    # Independent stream-consumer liveness. A capable worker publishes the
    # short alive lease before it reads and refreshes it throughout graceful
    # in-flight drain. A replacement requires two absent observations separated
    # by a full heartbeat TTL before prompt claim, so neither consumer idle nor
    # one transient Redis read can manufacture process death.
    consumer_heartbeat_ttl_ms: int = Field(
        default=15000,
        gt=0,
        validation_alias="CURIE_CONSUMER_HEARTBEAT_TTL_MS",
    )
    # The capability marker outlives the 15-minute compatibility backstop. It
    # proves the departed consumer knew how to publish alive leases; an old
    # unmarked worker stays exclusively on XAUTOCLAIM rather than being guessed
    # dead. This TTL is renewed beside alive for the process lifetime.
    consumer_capability_ttl_ms: int = Field(
        default=1800000,
        gt=0,
        validation_alias="CURIE_CONSUMER_CAPABILITY_TTL_MS",
    )

    @model_validator(mode="after")
    def _capability_outlives_reclaim_backstop(self) -> WorkerConfig:
        if self.consumer_capability_ttl_ms <= self.reclaim_min_idle_ms:
            raise ValueError(
                "CURIE_CONSUMER_CAPABILITY_TTL_MS must be greater than "
                "reclaim_min_idle_ms so a hard-killed capable consumer remains "
                "distinguishable from a pre-marker worker through the long "
                "XAUTOCLAIM compatibility window"
            )
        return self

    # Slack placeholder edits are throttled to avoid rate limits while streaming.
    slack_edit_min_interval_s: float = 0.7

    # Delivery budget and ownership lease (ADR-0131, #1971).
    #
    # One deadline and one renewable fenced owner per ``(stream, group,
    # entry_id)``. ``delivery_budget_s`` is the OVERALL wall-clock deadline for
    # the whole delivery -- claim, every runner request, every retry backoff,
    # reclaim, and terminal cleanup -- not just one runner call.
    # ``runner_total_timeout_s`` (above) is now a per-request ceiling INSIDE
    # this budget, not an independent clock: a stalled request is cut short by
    # whichever of the two is smaller, but the budget is what actually bounds
    # the delivery. The lease is the renewable proof of ownership that makes a
    # healthy long turn un-reclaimable and a dead owner's turn recoverable
    # after a bounded expiry, replacing the old dead pair of a flat HTTP
    # timeout and a 900s idle-based steal window.
    delivery_budget_s: float = Field(
        default=600.0, ge=60.0, le=1800.0, validation_alias="CURIE_DELIVERY_BUDGET_S"
    )
    delivery_lease_ttl_s: float = Field(
        default=45.0, gt=0, validation_alias="CURIE_DELIVERY_LEASE_TTL_S"
    )
    delivery_lease_heartbeat_s: float = Field(
        default=10.0, gt=0, validation_alias="CURIE_DELIVERY_LEASE_HEARTBEAT_S"
    )
    delivery_shutdown_reserve_s: float = Field(
        default=60.0, ge=0, validation_alias="CURIE_DELIVERY_SHUTDOWN_RESERVE_S"
    )
    # ---- Upgrade drain gate (issue #2010) --------------------------------
    #
    # ADR-0131 made ONE worker's own shutdown safe: grace covers budget +
    # reserve, so a SIGTERMed replica can settle the delivery it owns. It says
    # nothing about the PLATFORM roll around it. A `helm upgrade` rolls the
    # worker and its backing services together, and an already-accepted
    # side-effecting turn whose owner dies mid-flight is reclaimed by the
    # replacement, which correctly refuses to re-run the action and escalates to
    # a human. Duplicate effects are prevented and the requested task still does
    # not complete -- the failure #2010 reports.
    #
    # These three knobs drive the pre-upgrade gate (``upgrade_drain.py``), which
    # quiesces new claims and waits for every live-leased delivery to reach its
    # terminal outcome BEFORE the roll begins, and refuses the upgrade when they
    # do not.
    upgrade_drain_timeout_s: float = Field(
        default=900.0, gt=0, validation_alias="CURIE_UPGRADE_DRAIN_TIMEOUT_S"
    )
    upgrade_drain_poll_interval_s: float = Field(
        default=5.0, gt=0, validation_alias="CURIE_UPGRADE_DRAIN_POLL_INTERVAL_S"
    )
    # How long the quiesce flag lives. FINITE on purpose: an upgrade that is
    # killed between the gate and the post-upgrade release must not leave the
    # fleet permanently unable to claim, so the flag lapses on its own. It must
    # also outlast the drain wait, which is what the validator below enforces.
    upgrade_quiesce_ttl_s: float = Field(
        default=1200.0, gt=0, validation_alias="CURIE_UPGRADE_QUIESCE_TTL_S"
    )

    # The platform's voluntary termination grace, injected by the chart from
    # the SAME value it renders onto the Pod's ``terminationGracePeriodSeconds``
    # so the app's validator and the platform can never drift apart. ``None``
    # means "no platform grace declared" (compose, tests, a bare
    # ``WorkerConfig()`` in a unit test) and SKIPS the grace validator below
    # rather than guessing a value for an environment that has none.
    termination_grace_period_s: float | None = Field(
        default=None, validation_alias="CURIE_TERMINATION_GRACE_PERIOD_S"
    )

    # Runner HTTP timeouts
    runner_connect_timeout_s: float = 10.0
    runner_total_timeout_s: float = Field(
        default=600.0,
        gt=0.0,
        le=1800.0,
        validation_alias=AliasChoices(
            "CURIE_RUNNER_TOTAL_TIMEOUT_S", "RUNNER_TOTAL_TIMEOUT_S"
        ),
    )

    # Eval stream (F3): a separate consumer group on curie:evals runs eval
    # suites and reports results to the platform API and Langfuse.
    eval_stream: str = Field(default=EVAL_STREAM_DEFAULT, validation_alias="CURIE_EVAL_STREAM")
    eval_consumer_group: str = Field(
        default=EVAL_CONSUMER_GROUP_DEFAULT,
        validation_alias="CURIE_EVAL_CONSUMER_GROUP",
    )
    eval_consumer_name: str = Field(default_factory=_default_consumer_name)
    # Upper bound on eval SandboxClaims being created/bound CONCURRENTLY in this
    # worker (#709). Eval jobs are handled by two coroutines racing under one
    # gather -- the blocking read loop and the crash-recovery reclaim loop -- and
    # each provisions its own sandbox, so without a bound they can create claims
    # faster than a small cluster binds them. On a single-node k3s cluster that
    # storm is the observed failure: claims never bind within claim_timeout and
    # claim reads return 504. The default of 1 is single-node-safe (a second claim
    # is not created until the first has bound), giving sequential-with-backpressure
    # claim creation; a multi-node cluster raises this to admit real parallelism.
    # The bound covers only the create/bind phase (the flood source), not the whole
    # suite run: once a claim binds the slot frees so the next claim can begin while
    # the bound sandbox runs its cases. Floor 1 -- 0 would create no claims at all.
    eval_max_concurrent_claims: int = Field(
        default=1, ge=1, validation_alias="CURIE_EVAL_MAX_CONCURRENT_CLAIMS"
    )
    # On first creation the eval group starts at (now - this window) rather than
    # the stream head, so a backlog of ancient entries is not replayed en masse
    # on boot. Recent entries (younger than the window) are still delivered, so a
    # short outage never drops a live eval; only long-dead entries are skipped.
    eval_stream_max_age_hours: int = Field(
        default=24, validation_alias="CURIE_EVAL_STREAM_MAX_AGE_HOURS"
    )
    # RustFS / S3 for plugin bundles (mirrors the API's env names). The consumer
    # fetches a version's bundle by bundle_ref and loads its evals/ suite.
    # The credentials default to EMPTY on purpose (#1559): an empty credential
    # selects the AWS provider chain, which is the key-free BYO path the chart
    # README documents under "Key-free object store auth" (IRSA, an instance
    # role, an ambient profile). The dev static credential now lives in
    # `.env.example` and `compose.dev.yaml`, which set S3_ACCESS_KEY /
    # S3_SECRET_KEY explicitly for the compose stack's RustFS. Never reintroduce
    # a non-empty value as a default here: a baked-in key is precisely what made
    # the key-free path inert, because it is handed to boto3 as an explicit
    # credential and the provider chain is never consulted. This block is a
    # parity seam with the API's `Settings`, so the two must stay identical.
    s3_endpoint_url: str = "http://localhost:29000"
    s3_access_key: str = ""
    s3_secret_key: str = ""
    s3_region: str = "us-east-1"
    bundle_bucket: str = "curie-bundles"
    # Managed repository workspaces use the same object-store endpoint but a
    # private prefix and an exact-object signed read capability. The internal
    # token is distinct from the operator/CLI API key and is mounted only into
    # API and worker. The public dev default is replaced by the chart Secret in
    # a cluster install.
    internal_worker_token: str = Field(
        default="curie-dev-worker-token",
        validation_alias="CURIE_INTERNAL_WORKER_TOKEN",
    )
    workspace_bucket: str = Field(
        default="curie-workspaces", validation_alias="CURIE_WORKSPACE_BUCKET"
    )
    workspace_enabled: bool = Field(
        default=True, validation_alias="CURIE_WORKSPACE_ENABLED"
    )
    workspace_object_prefix: str = Field(
        default="private/workspaces",
        validation_alias="CURIE_WORKSPACE_OBJECT_PREFIX",
    )
    workspace_scratch_root: str = Field(
        default="/tmp/curie-workspaces",
        validation_alias="CURIE_WORKSPACE_SCRATCH_ROOT",
    )
    workspace_clone_timeout_seconds: int = Field(
        default=90, gt=0, validation_alias="CURIE_WORKSPACE_CLONE_TIMEOUT_SECONDS"
    )
    workspace_archive_timeout_seconds: int = Field(
        default=30, gt=0, validation_alias="CURIE_WORKSPACE_ARCHIVE_TIMEOUT_SECONDS"
    )
    workspace_upload_timeout_seconds: int = Field(
        default=30, gt=0, validation_alias="CURIE_WORKSPACE_UPLOAD_TIMEOUT_SECONDS"
    )
    workspace_total_timeout_seconds: int = Field(
        default=150, gt=0, validation_alias="CURIE_WORKSPACE_TOTAL_TIMEOUT_SECONDS"
    )
    workspace_max_checkout_bytes: int = Field(
        default=512 * 1024 * 1024,
        gt=0,
        validation_alias="CURIE_WORKSPACE_MAX_CHECKOUT_BYTES",
    )
    workspace_max_archive_bytes: int = Field(
        default=256 * 1024 * 1024,
        gt=0,
        validation_alias="CURIE_WORKSPACE_MAX_ARCHIVE_BYTES",
    )
    workspace_max_members: int = Field(
        default=50_000, gt=0, validation_alias="CURIE_WORKSPACE_MAX_MEMBERS"
    )
    workspace_max_compression_ratio: float = Field(
        default=20.0,
        gt=0,
        validation_alias="CURIE_WORKSPACE_MAX_COMPRESSION_RATIO",
    )
    workspace_reference_ttl_seconds: int = Field(
        default=300, gt=0, validation_alias="CURIE_WORKSPACE_REFERENCE_TTL_SECONDS"
    )
    workspace_max_concurrent_clones: int = Field(
        default=2, gt=0, validation_alias="CURIE_WORKSPACE_MAX_CONCURRENT_CLONES"
    )
    # Approval-gated publication runs only on the Kubernetes substrate. These
    # values shape the worker-owned Job; none are bundle inputs.
    publication_enabled: bool = Field(
        default=True, validation_alias="CURIE_PUBLICATION_ENABLED"
    )
    publication_namespace: str = Field(
        default="curie-publication", validation_alias="CURIE_PUBLICATION_NAMESPACE"
    )
    publication_patch_max_bytes: int = Field(
        default=900_000,
        gt=0,
        le=900_000,
        validation_alias="CURIE_PUBLICATION_PATCH_MAX_BYTES",
    )
    publication_result_max_attempts: int = Field(
        default=5, gt=0, validation_alias="CURIE_PUBLICATION_RESULT_MAX_ATTEMPTS"
    )
    publication_reconcile_max_attempts: int = Field(
        default=10,
        gt=0,
        validation_alias="CURIE_PUBLICATION_RECONCILE_MAX_ATTEMPTS",
    )
    publication_job_active_deadline_seconds: int = Field(
        default=300,
        gt=0,
        validation_alias="CURIE_PUBLICATION_JOB_ACTIVE_DEADLINE_SECONDS",
    )
    publication_git_command_timeout_seconds: int = Field(
        default=120,
        gt=0,
        validation_alias="CURIE_PUBLICATION_GIT_COMMAND_TIMEOUT_SECONDS",
    )
    publication_github_api_url: str = Field(
        default="https://api.github.com",
        validation_alias="CURIE_PUBLICATION_GITHUB_API_URL",
    )
    publication_reconcile_interval_seconds: float = Field(
        default=2.0,
        gt=0,
        validation_alias="CURIE_PUBLICATION_RECONCILE_INTERVAL_SECONDS",
    )
    publication_lease_seconds: int = Field(
        default=60, gt=0, validation_alias="CURIE_PUBLICATION_LEASE_SECONDS"
    )
    publication_image_pull_policy: str = Field(
        default="IfNotPresent", validation_alias="CURIE_PUBLICATION_IMAGE_PULL_POLICY"
    )
    publication_image_pull_secrets: CommaSeparatedNames = Field(
        default=(), validation_alias="CURIE_PUBLICATION_IMAGE_PULL_SECRETS"
    )
    publication_priority_class_name: str = Field(
        default="curie-platform-critical",
        validation_alias="CURIE_PUBLICATION_PRIORITY_CLASS_NAME",
    )
    publication_service_account_name: str = Field(
        default="curie-publication",
        validation_alias="CURIE_PUBLICATION_SERVICE_ACCOUNT_NAME",
    )
    publication_owner_name: str = Field(
        default="curie-publication-owner",
        validation_alias="CURIE_PUBLICATION_OWNER_NAME",
    )
    publication_git_user_name: str = Field(
        default="Curie Publisher", validation_alias="CURIE_PUBLICATION_GIT_USER_NAME"
    )
    publication_git_user_email: str = Field(
        default="publisher@example.com",
        validation_alias="CURIE_PUBLICATION_GIT_USER_EMAIL",
    )
    publication_cpu_request: str = Field(
        default="100m", validation_alias="CURIE_PUBLICATION_CPU_REQUEST"
    )
    publication_cpu_limit: str = Field(default="1", validation_alias="CURIE_PUBLICATION_CPU_LIMIT")
    publication_memory_request: str = Field(
        default="256Mi", validation_alias="CURIE_PUBLICATION_MEMORY_REQUEST"
    )
    publication_memory_limit: str = Field(
        default="1Gi", validation_alias="CURIE_PUBLICATION_MEMORY_LIMIT"
    )
    publication_ephemeral_request: str = Field(
        default="1Gi", validation_alias="CURIE_PUBLICATION_EPHEMERAL_REQUEST"
    )
    publication_ephemeral_limit: str = Field(
        default="4Gi", validation_alias="CURIE_PUBLICATION_EPHEMERAL_LIMIT"
    )
    # Bundle extraction bounds (ADR-0059 decision 3), applied by the Docker
    # substrate's claim-time bundle-fetch (`sandbox/docker.py`'s
    # `_prepare_bundle` -> `bundle_store.extract_bundle`) and the eval-stream
    # suite loader (`eval/stream.py`'s `load_suite_from_bundle`), both of which
    # route through `plugin_format.safe_extract`. Mirrors the API's `Settings`
    # field names/defaults (apps/api/src/curie_api/config.py) -- the same
    # stored bytes get the same caps regardless of which lane fetches them, so
    # keep the two in sync (a parity seam per AGENTS.md). The Kubernetes
    # substrate fetches/extracts via shell init containers, not this Python
    # path, so this does not reach it; see ADR-0059 decision 3's note that the
    # API-side bound is substrate-neutral, while extraction itself is not on
    # every substrate.
    bundle_max_uncompressed_bytes: int = 1024 * 1024 * 1024  # 1 GiB
    bundle_max_compression_ratio: float = 100.0
    # Member-count cap, enforced incrementally during the pre-scan (#815) so a
    # many-member archive is refused mid-walk. Mirrors the API's Settings field.
    bundle_max_members: int = 10_000
    # Platform API for POST /evals/report. Defaults match the API's dev stack
    # (README serves it on :8000; its shared dev key is curie-dev-key).
    api_base_url: str = Field(
        default="http://localhost:8000", validation_alias=api_url_validation_alias()
    )
    # The API base a SPAWNED RUNNER dials, distinct from api_base_url above, which
    # is the worker's OWN self-dial URL (its /evals/report, binding resolve). The
    # two diverge whenever the worker and the runner sit on different networks:
    # in the docker substrate the worker runs host-net and reaches the API at the
    # published localhost port, but the runner container it spawns joins the
    # bridge runner network and can only reach the API by its in-network service
    # name (compose: http://curie-api:8000). CURIE_MEMORY_REF/CURIE_HISTORY_REF
    # are minted for the runner, so they must be built from THIS base, not the
    # worker's localhost self-dial (#678). Empty means "not split out": the ref
    # falls back to api_base_url, byte-identical to the pre-#678 behavior. That
    # fallback is correct wherever the worker's own api_base_url is already
    # runner-reachable.
    runner_api_base_url: str = Field(default="", validation_alias="CURIE_RUNNER_API_URL")
    api_key: str = Field(default="curie-dev-key", validation_alias=API_KEY_ENV)
    # ---------------------------------------------------------------------
    # Connector reconciler (ADR-0090, #1184). Off by default: enabling it is
    # what makes the worker's Role grant create/patch/delete on Deployments,
    # Services, Secrets and NetworkPolicies, and the chart gates that grant on
    # the same flag. A worker that is not reconciling should not hold it.
    # ---------------------------------------------------------------------
    connector_reconcile_enabled: bool = Field(
        default=False, validation_alias="CURIE_CONNECTOR_RECONCILE"
    )
    connector_reconcile_interval_s: float = Field(
        default=60.0, gt=0, validation_alias="CURIE_CONNECTOR_RECONCILE_INTERVAL_S"
    )
    # The reconciler reuses `connector_release` / `connector_namespace` above --
    # deliberately the same two values the runner's connector scope is built
    # from. They must agree: the runner dials a Service by the name those
    # produce, and the reconciler creates the Service by the same name. Two
    # separate settings could disagree, and the symptom would be a connector
    # that exists and an agent that cannot reach it.
    #
    # `app_name` is the one thing neither of them already needs: it is the
    # chart's nameOverride, and it appears only in the pod selector the
    # NetworkPolicy matches on.
    connector_app_name: str = Field(default="", validation_alias="CURIE_CONNECTOR_APP_NAME")
    report_max_attempts: int = 3
    report_backoff_base_s: float = Field(default=0.5, gt=0)
    # Langfuse for recording eval scores (the matrix reads them back by version).
    langfuse_host: str = "http://localhost:23000"
    langfuse_public_key: str = "pk-lf-curie-dev"
    langfuse_secret_key: str = "sk-lf-curie-dev"

    # The worker's asyncio loop touches heartbeat_file every heartbeat_interval_s
    # so an exec liveness probe can restart a pod whose event loop has wedged.
    heartbeat_file: str = Field(
        default="/tmp/curie-worker.heartbeat",
        validation_alias=HEARTBEAT_FILE_ENV,
    )
    heartbeat_interval_s: float = Field(default=10.0, validation_alias=HEARTBEAT_INTERVAL_ENV)

    key_prefix: str = "curie:worker"

    @property
    def runner_facing_api_base_url(self) -> str:
        """The API base a spawned runner dials, falling back to the self-dial URL.

        ``runner_api_base_url`` overrides only when the runner sits on a different
        network than the worker (the docker substrate: host-net worker, bridge-net
        runner). When it is unset this falls back to api_base_url, byte-identical
        to the pre-#678 behavior -- correct wherever the worker's own api_base_url
        is already runner-reachable. Callers minting runner-facing refs
        (CURIE_MEMORY_REF / CURIE_HISTORY_REF) read this,
        never api_base_url directly (#678).
        """
        return self.runner_api_base_url or self.api_base_url

    def valkey_client_kwargs(self) -> dict[str, Any]:
        """The connection parts every Valkey client in the worker is built from.

        One place -- the three clients in ``run.build`` and the upgrade-drain
        hook's own client -- so they cannot drift on the transport: ``ssl``
        reaching some of them and not the rest is a lane that goes silently
        cleartext against a TLS-only BYO store (#2315), and for the drain hook
        that failure blocks every ``helm upgrade`` on the install (#2010).
        ``socket_timeout`` stays at the call sites: the drain hook deliberately
        does not set one.
        """
        return {
            "host": self.valkey_host,
            "port": self.valkey_port,
            "password": self.valkey_password or None,
            "db": self.valkey_db,
            "decode_responses": True,
            "ssl": self.valkey_tls,
        }

    @property
    def valkey_socket_timeout_s(self) -> float:
        """Socket read timeout for the Valkey clients, kept above the block interval.

        An idle blocking XREADGROUP blocks server-side for ``read_block_ms`` and
        then returns an empty reply, but redis-py enforces the client
        ``socket_timeout`` on that same read (its default is 5s). If the socket
        timeout is not longer than the block, the socket read deadline fires at
        the exact moment the block would return empty, so every idle cycle raises
        a read timeout instead of returning empty and floods the logs. The extra
        headroom past ``read_block_ms`` covers pod-to-pod RTT and Valkey
        processing after the block elapses. Genuine connection blips still raise
        and are logged as real transport errors.
        """
        return self.read_block_ms / 1000 + 5.0

    def done_key(self, event_id: str) -> str:
        return f"{self.key_prefix}:done:{event_id}"

    def side_effect_key(self, event_id: str) -> str:
        return f"{self.key_prefix}:sidefx:{event_id}"

    def delivery_lease_key(self, stream: str, group: str, entry_id: str) -> str:
        """The lease STRING key: the opaque owner token, TTL'd at
        ``delivery_lease_ttl_s``. Keyed by the delivery triple
        ``(stream, group, entry_id)``, NOT the event id -- the same event id
        can legitimately be redelivered under a new entry id after a
        dead-letter, and keying by event id would fence the wrong thing. Absent
        means no live owner; expiry IS how ownership becomes transferable.
        """
        return f"{self.key_prefix}:lease:{stream}:{group}:{entry_id}"

    def delivery_state_key(self, stream: str, group: str, entry_id: str) -> str:
        """The delivery state HASH key: ``deadline_ms`` (absolute, Valkey server
        time, create-if-absent) and ``gen`` (the fencing generation,
        HINCRBY'd on each acquisition). Retained for ``idempotency_ttl_s``
        (86400s) -- deliberately longer than the lease so the generation
        survives lease expiry and a fresh acquisition's deadline is never
        re-minted from an entry that merely lost its short-lived lease.
        Explicitly deleted on terminal ACK and dead-letter settlement; the TTL
        is only the backstop for a crash between the two.
        """
        return f"{self.key_prefix}:delivery:{stream}:{group}:{entry_id}"

    def completion_key(self, event_id: str) -> str:
        # The durable outbox record for this event's ``turn.completed``. NO TTL:
        # a payload that expires under a longer-lived set membership is a
        # completion lost silently (EB-B6(f)).
        return f"{self.key_prefix}:completion:{event_id}"

    def completions_pending_key(self) -> str:
        # The sweep index. A SET, not a SCAN over the keyspace: the maintenance
        # loop must not scan a production Valkey, and a redelivery-only sweep
        # would never reach a turn whose stream entry was already acked.
        return f"{self.key_prefix}:completions:pending"

    def upgrade_quiesce_key(self) -> str:
        # The fleet-wide "stop taking new work" flag the pre-upgrade gate sets
        # (issue #2010). One key for the whole release, not one per replica: the
        # gate runs as a Job that knows nothing about how many replicas exist,
        # and every consumer reads the same flag. Always written with a TTL --
        # see ``upgrade_quiesce_ttl_s`` for why it must never be permanent.
        return f"{self.key_prefix}:upgrade:quiesce"

    def lock_key(self, thread_key: str) -> str:
        return f"{self.key_prefix}:lock:{thread_key}"

    def approval_card_key(self, approval_id: str) -> str:
        # Where a posted approval card lives so its own resolution or expiry can
        # settle it without sharing identity with another approval on the thread.
        return f"{self.key_prefix}:approval-card:{approval_id}"

    def dead_letter_stream_name(self) -> str:
        """The graveyard stream: the explicit override, else derived ``<stream>:dead``.

        ``dead_letter_stream``'s Field default cannot reference ``self.stream``,
        so the derivation lives here rather than at the use site -- next to the
        other derived names, and next to ``_reject_self_targeting_graveyard``,
        which reasons about the same name. The derivation itself now lives in the
        shared ``derive_dead_letter_stream_name`` (#668) so the API's watcher and
        this writer can never drift on the name.
        """
        return derive_dead_letter_stream_name(self.stream, self.dead_letter_stream)

    def eval_dead_letter_stream_name(self) -> str:
        """The eval lane's graveyard: ``<eval_stream>:dead`` (#535).

        Derived from ``eval_stream``, NOT ``dead_letter_stream_name()`` (which is
        keyed to the runs ``stream``): the eval lane runs its own delivery cap, so
        a permanently-failing eval must dead-letter to its own graveyard rather
        than the runs graveyard. Always derived, so it can never collide with
        ``eval_stream`` and needs no self-targeting validator.
        """
        return f"{self.eval_stream}:dead"
