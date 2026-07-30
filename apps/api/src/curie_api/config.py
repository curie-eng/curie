"""Runtime configuration for the Curie API server.

All values default to the compose dev stack (see compose.dev.yaml and
.env.example) so a local run needs no .env. Override any field via the matching
environment variable for shared or production deployments.
"""

from functools import lru_cache

from aci_protocol import (
    DEAD_LETTER_STREAM_ENV,
    RUNS_STREAM_DEFAULT,
    STREAM_ENV,
    derive_dead_letter_stream_name,
)
from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Dev-only default secrets. The production boot gate refuses to start when any of
# these is still in place under ENVIRONMENT=prod.
_DEV_DEFAULT_API_KEY = "curie-dev-key"
_DEV_DEFAULT_WEBHOOK_SECRET = "dev-webhook-secret"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Deploy environment the API boots as, "dev" or "prod" (the chart renders it
    # from api.environment onto the ENVIRONMENT var). "prod" arms the production
    # boot gate below, which refuses the dev-default secrets.
    environment: str = "dev"

    # Postgres (async driver). Dedicated `curie` schema keeps our tables clear
    # of Langfuse's own tables on the same database.
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres"
    db_schema: str = "curie"

    # Single shared API key. Dev-only default; override in any shared deployment.
    api_key: str = "curie-dev-key"

    # Human-readable org/workspace name the UI reads (open /config endpoint) to
    # brand the app. Overridable via ORG_NAME for a white-labeled deployment.
    org_name: str = "Curie"

    # Langfuse proxy target (the dev project keys baked into compose.dev.yaml).
    langfuse_host: str = "http://localhost:23000"
    langfuse_public_key: str = "pk-lf-curie-dev"
    langfuse_secret_key: str = "sk-lf-curie-dev"

    # MinIO / S3 for immutable plugin bundles (compose stack MinIO on 29000).
    s3_endpoint_url: str = "http://localhost:29000"
    s3_access_key: str = "minio"
    s3_secret_key: str = "miniosecret"
    s3_region: str = "us-east-1"
    bundle_bucket: str = "curie-bundles"
    # Bundle ingestion bounds (ADR-0059 decision 3). Three independent caps:
    # - bundle_upload_max_bytes gates the raw upload body, enforced before it
    #   is fully read into memory (routers/bundles.py's `_read_bounded_upload`,
    #   mirroring `_read_bounded_body` below). 200 MiB comfortably covers a
    #   real plugin bundle (source, skill docs, small fixtures) while bounding
    #   a runaway upload.
    # - bundle_max_uncompressed_bytes and bundle_max_compression_ratio gate
    #   `safe_extract`/`check_archive_bounds` (`plugin_format.archive`): the
    #   total size the archive would extract to, and its overall compression
    #   ratio. A legitimate source/text bundle rarely compresses past ~10x; a
    #   zip-bomb-shaped archive routinely clears 1000x, so 100x sits well
    #   above real bundles and well below a bomb. These two also gate
    #   `deploy.revalidate_stored_bundle`'s deploy-time recheck of an
    #   already-stored bundle against the CURRENT caps (the legacy-bundle
    #   backward-compatibility case).
    # - bundle_max_members caps the NUMBER of archive members, enforced
    #   INCREMENTALLY during the pre-scan (#815) so a many-member archive (a
    #   tiny tar.gz of hundreds of thousands of zero-byte members clears both
    #   size caps yet exhausts memory building one TarInfo per member) is
    #   refused mid-walk. A real bundle has hundreds to low thousands of files;
    #   10_000 sits well above that and far below a member-count DoS.
    # Mirrored in the worker's `WorkerConfig` (apps/worker/src/curie_worker/
    # config.py) under the same names/defaults -- the worker's Docker-substrate
    # bundle-fetch and the eval-stream suite loader apply the same caps to the
    # same stored bytes, so keep the two in sync (a parity seam per AGENTS.md).
    bundle_upload_max_bytes: int = 200 * 1024 * 1024  # 200 MiB
    bundle_max_uncompressed_bytes: int = 1024 * 1024 * 1024  # 1 GiB
    bundle_max_compression_ratio: float = 100.0
    bundle_max_members: int = 10_000

    # Git flow (J1). The webhook secret authenticates inbound GitHub events; the
    # two bot identities are the routing targets recorded on each deployment.
    github_webhook_secret: str = "dev-webhook-secret"
    dev_branch: str = "dev"
    prod_branch: str = "main"
    # Outbound GitHub credential. Used for the eval PR check's commit-status
    # API (K1) AND to authenticate the git-flow bundle clone, without which
    # a private repository cannot deploy at all (#1058). Sent as a scoped
    # http.extraheader, never embedded in the clone URL.
    github_api_url: str = "https://api.github.com"
    github_token: str = ""
    # Upper bound on the GitHub webhook request body, enforced before the body is
    # fully buffered, parsed, or HMAC-authenticated (#633) so an unauthenticated
    # oversized request cannot exhaust memory. GitHub caps webhook payloads at
    # 25 MB and does not deliver anything larger, so a legitimately signed push
    # is always under this bound. Keep any ingress/proxy body-size limit fronting
    # the API aligned with (>=) this value so the two agree on what is rejected.
    github_webhook_max_body_bytes: int = 25 * 1024 * 1024  # 25 MiB
    eval_check_context: str = "curie/evals"
    # Suite name put on the fan-out request for a dev-push eval run (the plugin
    # bundle carries the suite itself; the consumer resolves it by this name).
    eval_default_suite: str = "default"
    # Clone-URL schemes the git-flow builder will fetch from. file:// supports
    # the hermetic local-bare-repo tests; anything else (e.g. git ext::) is
    # refused before a subprocess runs.
    git_allowed_schemes: tuple[str, ...] = ("file://", "https://", "http://")

    # Valkey for the kill switch (L1): SET the flag + PUBLISH the kill event.
    # The DSN is built from the parts so the compose VALKEY_PASSWORD override is
    # honored; set valkey_url to override the whole DSN (e.g. TLS, other host).
    valkey_password: str = "valkeypass"
    valkey_host: str = "localhost"
    valkey_port: int = 26379
    valkey_url: str | None = None

    # The runs stream approval resolutions enqueue resume turns onto (#244).
    # Must match the worker's CURIE_STREAM (its consumer side) -- which is why
    # the default is the shared declaration both lanes import (#492) rather than
    # a literal mirrored here. Overridable via RUNS_STREAM (the API's historical
    # name, which still wins if both are set) OR CURIE_STREAM (the worker's
    # name), so an operator who moves the base stream on the worker side moves it
    # here too and the two lanes agree on the graveyard derived from it (#668).
    runs_stream: str = Field(
        default=RUNS_STREAM_DEFAULT,
        validation_alias=AliasChoices("RUNS_STREAM", STREAM_ENV),
    )

    # Dead-letter graveyard watcher (#531). The worker moves a permanently-failing
    # entry to the graveyard (ADR-0039, #505) and acks it; this watcher is the
    # reader that alerts on each new dead-letter so the observable-single-loss
    # trade is actually observable. Interval <= 0 disables it (tests/off-switch).
    # The graveyard name is derived by the shared `derive_dead_letter_stream_name`
    # (#668), so the API now honors the same CURIE_DEAD_LETTER_STREAM /
    # CURIE_STREAM overrides the worker does, natively: the operator and the API
    # agree on the stream name with no manual sync.
    dead_letter_watch_interval_s: float = 30.0

    # The API mirror of the worker's CURIE_DEAD_LETTER_STREAM override, so the
    # graveyard watcher tracks the SAME stream the worker dead-letters to. Empty
    # derives `<runs_stream>:dead`. DISTINCT from `resume_dead_letter_stream` below
    # (the narrower ResumeQueue-only override); this is the general graveyard name.
    dead_letter_stream: str = Field(default="", validation_alias=DEAD_LETTER_STREAM_ENV)

    def dead_letter_stream_name(self) -> str:
        return derive_dead_letter_stream_name(self.runs_stream, self.dead_letter_stream)

    # How often the expiry sweeper scans for lapsed pending approvals (#412) and
    # resumes their stranded sessions. Values <= 0 disable the sweeper (the
    # operator kill lever and the fully-inert-app escape hatch for tests).
    approval_sweep_interval_s: float = 30.0

    # Resume reconciler (#411): the backstop that re-enqueues resume turns for
    # resolved approvals whose inline enqueue failed. enabled is the off-switch
    # for tests/deploys; batch_limit caps one pass's work-list.
    #
    # grace is LOAD-BEARING, not approximate. It MUST exceed the worker's maximum
    # single-turn processing time (runner_total_timeout_s, default 600s in the
    # worker) so the reconciler never re-enqueues while an inline-delivered resume
    # turn is still live: the cross-thread turn lock would steer a duplicate into
    # that live turn and it could re-run the approved action. The worker's
    # done-marker only dedupes a re-enqueue once the turn has reached terminal, so
    # the grace has to outlast the turn. Kept at 900s to stay above the 600s worker
    # max with margin (analogous to the migration's 24h done-marker /
    # idempotency_ttl_s coupling). Residual: worker retry loops (max_attempts,
    # backoff) can extend total processing past a single turn, so a fully airtight
    # guarantee needs a worker-side in-flight lease (follow-up); 900s covers the
    # common single-attempt case with margin.
    resume_reconciler_enabled: bool = True
    resume_reconciler_interval_seconds: int = 30
    resume_reconciler_grace_seconds: int = 900
    resume_reconciler_batch_limit: int = 100

    # Dead-lettered resume backstop (#532): each reconciler pass first scans the
    # graveyard for resume turns that reached the runs stream (resumed_at marked)
    # but then died at the worker's delivery cap (#505) and were dead-lettered,
    # re-opening them (resumed_at -> NULL) so the reconcile pass re-enqueues them.
    #
    # resume_dead_letter_stream overrides the graveyard stream the backstop scans;
    # empty derives `<runs_stream>:dead`. It MUST match the worker's
    # CURIE_DEAD_LETTER_STREAM / its `<stream>:dead` derivation, or the backstop
    # reads the wrong stream.
    #
    # resume_dead_letter_scan_limit caps the graveyard rows scanned per pass
    # (XRANGE COUNT). Resume-turn dead-letters are rare and the graveyard is
    # MAXLEN-bounded, so this only caps a pathological scan; a row beyond the cap
    # is picked up on a later pass as the graveyard trims.
    resume_dead_letter_stream: str = ""
    resume_dead_letter_scan_limit: int = 1000

    # The Slack bot token the API uses for its OWN user-group lookups (#420),
    # rather than trusting a caller's claim about who is in a group. The same
    # token the dispatcher and worker already hold; empty is the normal state
    # for a Slack-free install. Empty does NOT relax anything: a route that
    # declares an approvers group then fails closed at resolve time, while
    # channel and user-list authorizers are unaffected. Left out of the #57 prod
    # boot gate deliberately -- Slack is optional, and that resolve-time denial
    # is the enforcement.
    slack_bot_token: str = ""
    # How long a fetched user-group member set is reused (#420).
    # usergroups.users.list is a Slack Tier 2 method (~20 req/min), so a fetch
    # per click would let a busy approval channel hit the rate limit; 60s of
    # revocation lag against an hours-to-days human flow is negligible. 0 is the
    # operator lever for a per-resolve fetch, trading that headroom for no lag.
    slack_usergroup_cache_ttl_s: float = 60.0

    # Observability (OB1). kube_config_path points the runner-logs proxy at a
    # cluster; when unset the API tries in-cluster config, and if neither is
    # available the logs endpoint degrades to 503 rather than crashing.
    kube_config_path: str | None = None
    metrics_default_window_hours: int = 168  # 7 days
    # Durable state store size caps (#248). A hard non-goal of the store is that
    # it never becomes a database product, so a single value and a whole
    # namespace are both bounded. Sizes are the serialized-JSON byte length.
    state_max_value_bytes: int = 64 * 1024  # 64 KiB per value
    state_max_namespace_bytes: int = 1024 * 1024  # 1 MiB per (agent, namespace)
    # Cap on behavior-packs content per agent (#936, introduced by #883). Packs
    # are stored on the agent row and injected verbatim into the runner context
    # at each bind, so an uncapped pack bloats both the row and the prompt. Size
    # is the serialized-JSON byte length of the whole packs config, mirroring the
    # durable-state per-value cap above.
    behavior_packs_max_bytes: int = 64 * 1024  # 64 KiB
    # Per-agent cap on the NUMBER of namespaces (#852). #840 made the namespace an
    # arbitrary model-supplied string, so without this a single sandbox could
    # create unbounded namespaces -- each under the byte caps -- bloating the
    # shared store and permanently enlarging the enumeration GROUP BY. Only
    # creating a NEW namespace past the cap is refused; existing-namespace writes
    # are unaffected.
    state_max_namespaces: int = 256
    # The namespace the runner sandboxes run in, and the label selector that
    # identifies them (the chart labels sandbox pods
    # app.kubernetes.io/component=runner-sandbox). Used by the pod-list endpoint
    # that populates the Logs tab's pod dropdown.
    runner_namespace: str = "curie"
    runner_pod_label_selector: str = "app.kubernetes.io/component=runner-sandbox"

    def valkey_dsn(self) -> str:
        if self.valkey_url:
            return self.valkey_url
        return f"redis://:{self.valkey_password}@{self.valkey_host}:{self.valkey_port}/0"

    @model_validator(mode="after")
    def _refuse_dev_defaults_in_prod(self) -> "Settings":
        """Production boot gate (#57): with ENVIRONMENT=prod, refuse to start if a
        shared secret is unset or still the shipped dev default, so a prod deploy
        can never silently run on well-known credentials."""
        if self.environment.strip().lower() != "prod":
            return self
        offenders = []
        if self.api_key in ("", _DEV_DEFAULT_API_KEY):
            offenders.append("API_KEY")
        if self.github_webhook_secret in ("", _DEV_DEFAULT_WEBHOOK_SECRET):
            offenders.append("GITHUB_WEBHOOK_SECRET")
        if offenders:
            raise ValueError(
                "ENVIRONMENT=prod but these secrets are unset or still the dev "
                f"default: {', '.join(offenders)}. Set real values before booting "
                "in production."
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
