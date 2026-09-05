"""Runtime configuration for the Curie API server.

All values default to the compose dev stack (see compose.dev.yaml and
.env.example) so a local run needs no .env, with one exception: the two S3
credentials default to empty, because an empty credential is the signal that
selects the AWS provider chain for the key-free BYO object store path (#1559).
A local run against the compose RustFS therefore has to supply them, from .env
or the environment; the dev values live in .env.example and compose.dev.yaml.
Override any field via the matching environment variable for shared or
production deployments.
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

from .workspace_policy import valid_allowlist_entry

# Dev-only default secrets. The production boot gate refuses to start when any of
# these is still in place under ENVIRONMENT=prod.
_DEV_DEFAULT_API_KEY = "curie-dev-key"
_DEV_DEFAULT_WEBHOOK_SECRET = "dev-webhook-secret"
_DEV_DEFAULT_INTERNAL_WORKER_TOKEN = "curie-dev-worker-token"
_DEV_DEFAULT_APPROVAL_CHAT_ATTESTER_SECRET = "curie-dev-approval-chat-attester"


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
    # Dedicated credential with which the dispatcher attests a Slack identity,
    # channel, and approval id.  It has no built-in value and must never fall
    # back to the platform key: sharing those keys would let any API-key holder
    # forge the chat evidence that channel/group approver sets trust.
    approval_chat_attester_secret: str = Field(
        default="",
        validation_alias=AliasChoices(
            "CURIE_APPROVAL_CHAT_ATTESTER_SECRET", "approval_chat_attester_secret"
        ),
    )
    # Separate trust boundary for credential redemption. The operator/CLI API
    # key can administer deployments but cannot redeem the GitHub identity.
    internal_worker_token: str = Field(
        default=_DEV_DEFAULT_INTERNAL_WORKER_TOKEN,
        validation_alias=AliasChoices("CURIE_INTERNAL_WORKER_TOKEN", "INTERNAL_WORKER_TOKEN"),
    )

    # Human-readable org/workspace name the UI reads (open /config endpoint) to
    # brand the app. Overridable via ORG_NAME for a white-labeled deployment.
    org_name: str = "Curie"

    # Level for this service's own loggers (#1270). The API has no entry point
    # of its own -- uvicorn imports the app -- and uvicorn configures only its
    # own three loggers with no root entry, so without this every
    # `curie_api.*` INFO record is discarded by the last-resort handler.
    log_level: str = "INFO"

    # Langfuse proxy target (the dev project keys baked into compose.dev.yaml).
    langfuse_host: str = "http://localhost:23000"
    langfuse_public_key: str = "pk-lf-curie-dev"
    langfuse_secret_key: str = "sk-lf-curie-dev"

    # RustFS / S3 for immutable plugin bundles (compose stack RustFS on 29000).
    # The credentials default to EMPTY on purpose (#1559): an empty credential
    # selects the AWS provider chain, which is the key-free BYO path the chart
    # README documents under "Key-free object store auth" (IRSA, an instance
    # role, an ambient profile). The dev static credential now lives in
    # `.env.example` and `compose.dev.yaml`, which set S3_ACCESS_KEY /
    # S3_SECRET_KEY explicitly for the compose stack's RustFS. Never reintroduce
    # a non-empty value as a default here: a baked-in key is precisely what made
    # the key-free path inert, because it is handed to boto3 as an explicit
    # credential and the provider chain is never consulted.
    s3_endpoint_url: str = "http://localhost:29000"
    s3_access_key: str = ""
    s3_secret_key: str = ""
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
    github_review_ingress_enabled: bool = False
    dev_branch: str = "dev"
    prod_branch: str = "main"
    # Outbound GitHub credential. Used for the eval PR check's commit-status
    # API (K1) AND to authenticate the git-flow bundle clone, without which
    # a private repository cannot deploy at all (#1058). Sent as a scoped
    # http.extraheader, never embedded in the clone URL.
    github_api_url: str = "https://api.github.com"
    github_token: str = ""
    # GitHub App identity (ADR-0092). When both are set the platform mints a
    # one-hour token scoped to the single repository being cloned, instead of
    # using the org-wide `github_token` above. Nothing to rotate, no human
    # owner, and repository access is administered on the App's installation
    # page rather than by re-issuing a credential.
    #
    # `github_token` is NOT deprecated by this: it remains the fallback for a
    # GitHub Enterprise or air-gapped install with no App, and for a first-run
    # operator proving the flow works before registering one. Neither set is
    # also valid -- a public repository clones with no credential.
    github_app_id: str = ""
    # The App's PEM private key, whole. It can mint tokens for every repository
    # in the installation, so it is the most sensitive value here: never log it,
    # never place it in argv.
    github_app_private_key: str = ""
    github_app_timeout_seconds: float = 15.0
    # Durable human-review outbox. Zero disables only the periodic backstop;
    # authenticated ingress still attempts an immediate enqueue.
    github_review_reconciler_interval_s: float = Field(default=5.0, ge=0)
    # Repositories the runtime workspace selector may bind to a thread. Exact
    # owner/repository entries and owner-wide owner/* entries are supported.
    # Empty is fail-closed: enabling workspace capability alone grants no
    # repository access.
    github_repo_allowlist: tuple[str, ...] = ()
    # Commit polling (issue #1239). A self-hosted cluster that accepts no
    # inbound traffic cannot receive a GitHub webhook, so without this it has
    # no push-to-deploy at all. Outbound always works, so the API asks GitHub
    # whether the deploy branches moved.
    #
    # 0 disables it. The webhook stays the fast path wherever it can reach;
    # this is the floor, not a replacement, and a poll after a webhook already
    # handled the push is a no-op.
    commit_poll_interval_s: float = 0.0
    # The one git origin this installation deploys from. The webhook payload's
    # `clone_url` is compared against `<base>/<repo_full_name>.git` and a
    # mismatch is rejected before any subprocess starts; git is then handed the
    # derived URL rather than the payload's, so the credential above can only
    # ever reach this host (#1122). Point it at a GitHub Enterprise Server base
    # to deploy from GHE, exactly as with `github_api_url` above.
    github_clone_base: str = "https://github.com"
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
    # honored. valkey_tls is the supported TLS signal -- chart-rendered from
    # valkey.tls and shared with the worker and dispatcher, so all three agree on
    # the transport of one store (#2315). valkey_url stays the whole-DSN escape
    # for anything the parts cannot express (a different host, a non-default db,
    # credentials embedded differently) and still wins outright.
    valkey_password: str = "valkeypass"
    valkey_host: str = "localhost"
    valkey_port: int = 26379
    valkey_tls: bool = False
    valkey_url: str | None = None

    # Read-only observer of worker terminal markers. WorkerConfig consumes the
    # unprefixed KEY_PREFIX variable; CURIE_KEY_PREFIX is not its configuration.
    worker_key_prefix: str = Field(default="curie:worker", validation_alias="KEY_PREFIX")

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
    # Publication patches are private control-plane inputs. The ingress bound
    # and terminal retention are operator-owned, and the periodic approval
    # sweeper also reaps terminal bytes after this retention window.
    publication_patch_max_bytes: int = Field(
        default=900_000,
        gt=0,
        le=900_000,
        validation_alias="CURIE_PUBLICATION_PATCH_MAX_BYTES",
    )
    publication_patch_retention_seconds: int = Field(
        default=3600,
        gt=0,
        validation_alias="CURIE_PUBLICATION_RETENTION_SECONDS",
    )

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
    # the Helm chart derives its grace from the worker delivery budget plus its
    # shutdown reserve. This non-Helm Settings fallback remains the intentionally
    # conservative 900s default (analogous to the migration's 24h done-marker /
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
    # Disconnected ``curie cluster message`` replies are a short-lived handoff,
    # not a transcript store.  Bound every bucket on all three axes so a worker
    # retry storm or a chatty turn cannot turn the relay into durable unbounded
    # state.  Operators may tighten these without changing the reply protocol.
    cluster_message_replies_ttl_s: int = Field(
        default=3600,
        gt=0,
        le=86400,
        validation_alias="CLUSTER_MESSAGE_REPLIES_TTL_S",
    )
    cluster_message_replies_max_events: int = Field(
        default=512,
        gt=0,
        le=4096,
        validation_alias="CLUSTER_MESSAGE_REPLIES_MAX_EVENTS",
    )
    cluster_message_replies_max_bytes: int = Field(
        default=2 * 1024 * 1024,
        gt=0,
        le=16 * 1024 * 1024,
        validation_alias="CLUSTER_MESSAGE_REPLIES_MAX_BYTES",
    )
    # The namespace the runner sandboxes run in, and the label selector that
    # identifies them (the chart labels sandbox pods
    # app.kubernetes.io/component=runner-sandbox). Used by the pod-list endpoint
    # that populates the Logs tab's pod dropdown.
    runner_namespace: str = "curie"
    runner_pod_label_selector: str = "app.kubernetes.io/component=runner-sandbox"

    # Channel ingress (ADR-0096 phase 2, #1459). The turn body bound is enforced
    # BEFORE authentication and before JSON parsing (`routers/channels.py`), so an
    # oversized body is refused without the server ever HMAC-verifying or
    # deserializing it -- the same posture `github_webhook_max_body_bytes` takes
    # above, two orders of magnitude tighter because a turn is a message, not a
    # git payload, and 256 KiB is far above any real email body an adapter
    # extracts. The route is reachable from outside the first-party network, so
    # the bound is what keeps it from being an unauthenticated memory-pressure
    # surface.
    channel_turn_max_body_bytes: int = 256 * 1024  # 256 KiB
    # The delivery claim's lease: how long ONE in-flight ingress request owns a
    # `delivery_id` before a retry may take it. Deliberately short -- it bounds
    # how long a delivery whose winner died stays un-enqueued (nothing else
    # recovers that case), while being far longer than the enqueue it covers.
    channel_delivery_lease_s: int = 300
    # There is deliberately NO receipt TTL beside the lease above. Once the turn
    # is enqueued the claim key becomes a PERMANENT receipt: an expiry on it lets
    # the same `delivery_id` win a fresh `SET NX` after the window and enqueue a
    # second time, so the correspondent is answered twice, silently, days later
    # (the divergence from the dispatcher's `dedupe_ttl_seconds`, which guards a
    # queue rather than an outward-facing reply).
    #
    # How many NEW deliveries one binding may enqueue per window. A `chn` token
    # is scoped to a binding but not metered by one, so a compromised adapter can
    # otherwise submit unlimited unique `delivery_id`s and fill the shared stream
    # (and, since receipts are permanent, the shared Valkey) until every tenant is
    # affected. Counted per binding and per fixed window, on the claim -- so an
    # adapter's retries of a delivery it already got through are free -- and
    # exceeded requests are refused 429 rather than queued. It bounds the rate of
    # NEW work, not the depth of unconsumed work: the API cannot observe what the
    # worker has consumed without infrastructure phase 2 does not build.
    # Inbound hook ingress (ADR-0079, #269). Body bound sized like the GitHub
    # webhook's rather than the channel turn's: a hook payload is a machine
    # document, not a person's message, and the whole document becomes the turn
    # text today.
    hook_max_body_bytes: int = 1024 * 1024
    # Metered per AGENT: the thing worth bounding is how much work one agent's
    # upstreams can create, and a per-hook counter would let a source multiply
    # its own allowance by inventing hook names.
    hook_backlog_limit: int = 64
    hook_backlog_window_s: int = 60
    channel_binding_backlog_limit: int = 64
    channel_binding_backlog_window_s: int = 60

    def valkey_dsn(self) -> str:
        if self.valkey_url:
            return self.valkey_url
        # The scheme IS the transport: redis-py picks SSLConnection off
        # `rediss://` alone, so this one character is what carries valkey_tls
        # through from_url to the pool (#2315).
        scheme = "rediss" if self.valkey_tls else "redis"
        return f"{scheme}://:{self.valkey_password}@{self.valkey_host}:{self.valkey_port}/0"

    @model_validator(mode="after")
    def _validate_review_ingress(self) -> "Settings":
        if self.github_review_ingress_enabled and (
            not self.github_app_id.strip()
            or not self.github_app_private_key.strip()
            or self.github_webhook_secret.strip() in ("", _DEV_DEFAULT_WEBHOOK_SECRET)
        ):
            raise ValueError(
                "GitHub review ingress requires a configured App and non-default webhook secret"
            )
        return self

    @model_validator(mode="after")
    def _validate_github_repo_allowlist(self) -> "Settings":
        invalid = [
            entry for entry in self.github_repo_allowlist if not valid_allowlist_entry(entry)
        ]
        if invalid:
            raise ValueError("GITHUB_REPO_ALLOWLIST entries must be owner/repository or owner/*")
        return self

    @model_validator(mode="after")
    def _refuse_dev_defaults_in_prod(self) -> "Settings":
        """Production boot gate (#57): with ENVIRONMENT=prod, refuse to start if a
        shared secret is unset or still the shipped dev default, so a prod deploy
        can never silently run on well-known credentials."""
        attester_secret = self.approval_chat_attester_secret
        if attester_secret and not attester_secret.strip():
            raise ValueError("CURIE_APPROVAL_CHAT_ATTESTER_SECRET must be non-blank")
        if attester_secret and attester_secret == self.api_key:
            raise ValueError("CURIE_APPROVAL_CHAT_ATTESTER_SECRET must be distinct from API_KEY")
        if self.environment.strip().lower() != "prod":
            return self
        offenders = []
        if self.api_key in ("", _DEV_DEFAULT_API_KEY):
            offenders.append("API_KEY")
        if self.github_webhook_secret in ("", _DEV_DEFAULT_WEBHOOK_SECRET):
            offenders.append("GITHUB_WEBHOOK_SECRET")
        if self.internal_worker_token in ("", _DEV_DEFAULT_INTERNAL_WORKER_TOKEN):
            offenders.append("CURIE_INTERNAL_WORKER_TOKEN")
        if attester_secret in ("", _DEV_DEFAULT_APPROVAL_CHAT_ATTESTER_SECRET):
            offenders.append("CURIE_APPROVAL_CHAT_ATTESTER_SECRET")
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
