"""Session setup: the typed SessionConfig, the BootEnv superset, and their env
(de)serialization.

``SessionConfig`` mirrors the ACI contract v0.1 SESSION SETUP block (section 0).
A session is configured through environment variables and mounted files;
SessionConfig is the typed view of those variables, with helpers to render them
to and parse them from a process environment.

Env mapping:
    CURIE_PLUGIN_DIR     -> plugin_dir
    CURIE_MEMORY_REF     -> memory_ref        (optional)
    CURIE_CREDENTIALS    -> credentials_ref   (optional)
    CURIE_SESSION_ID     -> session_id
    CURIE_SANDBOX_ID     -> sandbox_id
    CURIE_BUDGET         -> budget            (JSON object)
    OTEL_EXPORTER_OTLP_*   -> otel              (endpoint / headers / protocol)

``BootEnv`` (#488, ADR-0049) is the Curie-platform superset: SessionConfig
COMPOSED as a field plus the platform-operational boot vars (runner token,
pool bootstrap token, bundle ref, approval plumbing, history port, model
routing, the operator knobs).
It is deliberately not an extension of SessionConfig -- see ADR-0049 and the
class docstring.
"""

import json
from collections.abc import Mapping, Sequence
from typing import Any, Literal, cast

from pydantic import Field, ValidationError, model_validator

from .events import (
    _AciModel,
    _malformed_credential_error,
    admit_bounded_credential,
    redact_credential_error,
)


class Budget(_AciModel):
    """Per-agent budget spec, carried as JSON in CURIE_BUDGET.

    ``task_budget_hint`` is the optional hint passed through to the model so it
    self paces (section 6b); it is not a hard ceiling.
    """

    max_output_tokens_per_run: int
    task_budget_hint: int | None = None
    max_usd_per_day: float


class OtelConfig(_AciModel):
    """The OTEL_EXPORTER_OTLP_* subset the runner needs to export traces.

    Section 0 lists OTEL_EXPORTER_OTLP_* as a wildcard. We capture the standard
    fields the prototype used (endpoint, headers, protocol); any others pass
    through as raw env vars untouched and are out of scope for this typed view.
    """

    endpoint: str | None = None
    headers: str | None = None
    protocol: str | None = None


class SessionConfig(_AciModel):
    """The typed session setup contract.

    ``credentials_ref`` is a reference to injected secrets (CURIE_CREDENTIALS);
    section 0 describes these as per-tool secrets via K8s Secret refs, so the
    contract carries the reference, not the secret material itself.
    """

    plugin_dir: str
    session_id: str
    sandbox_id: str
    budget: Budget
    memory_ref: str | None = None
    credentials_ref: str | None = None
    otel: OtelConfig = Field(default_factory=OtelConfig)

    def to_env(self) -> dict[str, str]:
        """Render this config to the process environment variables it maps to.

        Optional fields that are unset are omitted rather than emitted empty.
        """

        env: dict[str, str] = {
            "CURIE_PLUGIN_DIR": self.plugin_dir,
            "CURIE_SESSION_ID": self.session_id,
            "CURIE_SANDBOX_ID": self.sandbox_id,
            "CURIE_BUDGET": self.budget.model_dump_json(),
        }
        if self.memory_ref is not None:
            env["CURIE_MEMORY_REF"] = self.memory_ref
        if self.credentials_ref is not None:
            env["CURIE_CREDENTIALS"] = self.credentials_ref
        if self.otel.endpoint is not None:
            env["OTEL_EXPORTER_OTLP_ENDPOINT"] = self.otel.endpoint
        if self.otel.headers is not None:
            env["OTEL_EXPORTER_OTLP_HEADERS"] = self.otel.headers
        if self.otel.protocol is not None:
            env["OTEL_EXPORTER_OTLP_PROTOCOL"] = self.otel.protocol
        return env

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> "SessionConfig":
        """Parse a SessionConfig from a process environment mapping.

        Missing required variables and a malformed CURIE_BUDGET raise the
        usual pydantic ValidationError via the model constructor.
        """

        otel = OtelConfig(
            endpoint=env.get("OTEL_EXPORTER_OTLP_ENDPOINT"),
            headers=env.get("OTEL_EXPORTER_OTLP_HEADERS"),
            protocol=env.get("OTEL_EXPORTER_OTLP_PROTOCOL"),
        )
        budget = Budget.model_validate_json(env["CURIE_BUDGET"])
        return cls(
            plugin_dir=env["CURIE_PLUGIN_DIR"],
            session_id=env["CURIE_SESSION_ID"],
            sandbox_id=env["CURIE_SANDBOX_ID"],
            budget=budget,
            memory_ref=env.get("CURIE_MEMORY_REF"),
            credentials_ref=env.get("CURIE_CREDENTIALS"),
            otel=otel,
        )


# The producers that write a boot-env key. A key may have SEVERAL: the chart
# bakes a template default the worker then overrides per claim. What matters is
# not the count but the AUTHORITY -- see ``BootEnv`` and ADR-0049.
Producer = Literal["worker", "kernel", "substrate", "operator"]

# Producer tags for the nine keys ``SessionConfig`` owns. They cannot be carried
# on the fields themselves: SessionConfig is the frozen ACI section-0 contract
# and stays byte-identical, so it grows no Curie-platform annotations. This
# companion map is scoped to exactly those nine keys and is pinned by test
# against what ``SessionConfig.to_env`` actually writes, so it cannot drift into
# tagging a key the frozen model does not have.
# Keyed by FIELD NAME (the SessionConfig/OtelConfig attribute) so ``env_key``
# reaches these exactly as it reaches BootEnv's own fields; the OTel trio is
# prefixed because ``endpoint`` alone would be ambiguous in a flat namespace.
_SESSION_ENV: dict[str, tuple[str, tuple[Producer, ...]]] = {
    # Worker-authoritative with a substrate fallback: the chart bakes a warm-pool
    # default into the pod template and the worker's per-claim value wins under
    # ``envVarsInjectionPolicy: Overrides``.
    "plugin_dir": ("CURIE_PLUGIN_DIR", ("worker", "substrate")),
    "session_id": ("CURIE_SESSION_ID", ("worker", "substrate")),
    # Substrate-authoritative: the chart derives it from the pod name via
    # ``fieldRef: metadata.name``. The worker must never write it.
    "sandbox_id": ("CURIE_SANDBOX_ID", ("substrate",)),
    "budget": ("CURIE_BUDGET", ("worker", "substrate")),
    # Worker-only: absent from the runner container in the chart.
    "memory_ref": ("CURIE_MEMORY_REF", ("worker",)),
    "credentials_ref": ("CURIE_CREDENTIALS", ("worker", "substrate")),
    "otel_endpoint": ("OTEL_EXPORTER_OTLP_ENDPOINT", ("substrate",)),
    # Operator-owned: no code producer emits it. The chart writes only the
    # endpoint and the protocol, and compose only the endpoint, so the reachable
    # surface for collector auth headers is ``runner.extraEnv`` or a raw docker
    # ``-e``.
    "otel_headers": ("OTEL_EXPORTER_OTLP_HEADERS", ("operator",)),
    "otel_protocol": ("OTEL_EXPORTER_OTLP_PROTOCOL", ("substrate",)),
}


# Opaque pool bootstrap credential (#1492, ADR-0122). Same character cap as
# ``ADOPTION_CREDENTIAL_MAX_CHARS`` and for the same reason: long enough for a
# typical bearer, short enough to reject a dump. Not a product secret format.
RUNNER_BOOTSTRAP_TOKEN_MAX_CHARS = 4096

_BOOTSTRAP_FIELD = "runner_bootstrap_token"
_BOOTSTRAP_MALFORMED = "malformed runner bootstrap token"


def _malformed_bootstrap_token() -> ValueError:
    """Reject without attaching the presented material to the exception."""

    return ValueError(_BOOTSTRAP_MALFORMED)


def _malformed_boot_env_error() -> ValidationError:
    """A material-free ValidationError for a bad runner_bootstrap_token."""

    return _malformed_credential_error("BootEnv", _BOOTSTRAP_FIELD, _BOOTSTRAP_MALFORMED)


def parse_runner_bootstrap_token(value: Any) -> str | None:
    """Admit an optional bootstrap token, or raise without echoing it.

    ``None`` is "no bootstrap presented" (the key is absent). Any other
    well-formed value is a non-empty string no longer than
    ``RUNNER_BOOTSTRAP_TOKEN_MAX_CHARS`` that is not whitespace-only; an empty
    string is malformed, not unset. Same admission rule and same no-echo
    guarantee as ``parse_adoption_credential``; the two are one helper bound
    twice.
    """

    return admit_bounded_credential(
        value, max_chars=RUNNER_BOOTSTRAP_TOKEN_MAX_CHARS, error=_malformed_bootstrap_token
    )


def _env(key: str, *producers: Producer) -> dict[str, Any]:
    """The json_schema_extra declaring a field's env key and its producers.

    The key rides in the exported schema so codegen can emit the Rust constants
    the CLI and the chart render-assert pin against; without it every lane would
    retype the literal, which is the drift this contract exists to end.
    """

    return {"env": key, "producer": list(producers)}


def _str_or_none(raw: str | None) -> str | None:
    """A declared-but-empty var is "unset", never an empty value.

    Exactly ``env.get(...) or None`` (runner config.py:105): the fake/local
    no-key path must not present an empty bearer token. It deliberately does NOT
    strip -- today a whitespace-only value is truthy and survives verbatim, so
    stripping here would be an unrequested behavior change on a bearer-token
    path. (A secret injected with a trailing newline breaking auth is a real bug,
    but it is its own ticket, not a passenger on this freeze.)
    """

    return raw if raw else None


def _fake_model_or_none(raw: str | None) -> bool | None:
    """Absent means unset; otherwise mirror the runner's strict truthy set.

    The only consumer of ``CURIE_FAKE_MODEL`` on the platform is the runner
    (__main__.py:262), which accepts exactly ``1``/``true``/``yes``
    case-insensitively and treats everything else as off. This parse mirrors that
    set so ``BootEnv.from_env(env).fake_model`` cannot disagree with the running
    sandbox: ``CURIE_FAKE_MODEL=false`` is off in both. A declared-but-empty
    var stays ``None`` (unset), matching the other optional fields, so the
    ``to_env`` round trip (``"1"``/``"0"``) survives unchanged.
    """

    if raw is None or not raw.strip():
        return None
    return raw.strip().lower() in ("1", "true", "yes")


def _stripped_or_none(raw: str | None) -> str | None:
    """Strip, then treat blank as unset.

    The deliberate counterpart to ``_str_or_none``: the approval markers DO strip
    today (config.py:88-92), so parity here means stripping. The two helpers
    differ because the tree differs; unifying them would change one of them.
    """

    return raw.strip() if raw and raw.strip() else None


def _list_or_none(raw: str | None) -> list[str] | None:
    """Parse a comma-joined name list, exactly as config.py:82-86 does.

    Items are stripped and blanks dropped, but an absent var is None while a
    present-but-all-blank one is ``[]``, not None. That asymmetry is today's
    (``[...] if raw else None``), and both are treated as "no gates" downstream.
    """

    if not raw:
        return None
    return [item.strip() for item in raw.split(",") if item.strip()]


def _required_int(raw: str | None) -> int | None:
    """Parse an int, RAISING on garbage. None when the var is absent.

    The deliberate asymmetry against ``_tolerant_int``: ``CURIE_MAX_TURNS``
    (config.py:98) and ``CURIE_RUNNER_PORT`` (config.py:104) use a bare
    ``int()`` today and DO raise. Each var keeps the behavior it has -- unifying
    the two would be a behavior change wearing a consistency costume.
    """

    return None if raw is None else int(raw)


def _tolerant_int(raw: str | None) -> int | None:
    """Parse an int, degrading to None on garbage AND on a nonpositive value.

    Mirrors the runner's ``__main__._int_env`` (__main__.py:219-231) for the
    history-window knobs: a typo in an operator's ``extraEnv`` must not become a
    boot crash, and a nonpositive window is meaningless (``max_turns=0`` slices
    every turn, a nonpositive byte budget can never be met), so it is rejected
    like a bad parse. None hands the consumer its own default.
    """

    if raw is None or not raw.strip():
        return None
    try:
        value = int(raw.strip())
    except ValueError:
        return None
    return value if value > 0 else None


class BootEnv(_AciModel):
    """The full worker-to-runner boot env: the ACI session plus platform ops.

    ``session`` COMPOSES the frozen ``SessionConfig`` rather than extending it.
    That nesting is the ACI-vs-platform boundary: a runner token, approval
    plumbing, and a history port are Curie platform operations, not the
    interface a third-party ACI-conformant runner implements. Inheriting would
    tell every future implementer otherwise (ADR-0049).

    **Multiple producers, one consumer.** The boot env is assembled by the worker
    binding, the worker kernel's resume overlay, the substrate (chart/docker),
    and the operator; only the runner consumes it. So ``from_env`` is the single
    consumer parse of the whole union, while rendering is per-producer: there is
    deliberately no whole-model ``to_env`` on the wire path. ``render_worker`` is
    the one real render surface; ``to_env`` exists for round-trip checks only.

    **Authority, not arity, is the invariant.** ``CURIE_SANDBOX_ID`` and
    ``CURIE_RUNNER_PORT`` are substrate-authoritative: identity derives from
    the pod name (``fieldRef: metadata.name``), and because the chart sets
    ``envVarsInjectionPolicy: Overrides`` a worker write would REPLACE it and
    break the "pod name IS the sandbox id" invariant that trace stamping relies
    on. ``ANTHROPIC_BASE_URL`` is also multi-producer, but there the worker is
    authoritative and worker-wins is the intended layering: the chart branch is a
    baked template default, the worker's value is per-agent model routing. Do not
    collapse either producer list to a single value.
    """

    session: SessionConfig

    # The RustFS object key sandbox provisioning fetches into CURIE_PLUGIN_DIR.
    bundle_ref: str | None = Field(
        default=None, json_schema_extra=_env("CURIE_BUNDLE_REF", "worker")
    )
    # The platform-tracked agent version_label, so a sandboxed agent can name
    # the bundle it is running (#2174). Distinct from bundle_ref, which is the
    # object-store fetch key and is not the agent-facing identity.
    bundle_version: str | None = Field(
        default=None, json_schema_extra=_env("CURIE_BUNDLE_VERSION", "worker")
    )
    # Per-claim bearer token the runner enforces on its ACI POST routes (#63).
    # Enforced only when configured, so local/fake sandboxes are unaffected.
    runner_token: str | None = Field(
        default=None, json_schema_extra=_env("CURIE_RUNNER_TOKEN", "worker")
    )
    # Pool bootstrap credential (#1492; ADR-0116 decision 2, ADR-0122). Written
    # by the SUBSTRATE only: one Secret per warm pool, delivered through the
    # pool template's ``secretKeyRef``. The worker's per-claim render never
    # writes it, and ``render_worker`` has no parameter for it by design.
    #
    # It is a distinct key rather than a reuse of ``runner_token`` because the
    # two credentials carry different AUTHORITY, and an unbound warm runner has
    # to tell them apart at boot: ``CURIE_RUNNER_TOKEN`` means "bound,
    # per-conversation, enforced on every gated ACI route", while a bootstrap
    # may authorize exactly one atomic adoption (an ``Event`` carrying
    # ``adoption_credential``) and is retired for that pod once applied. The
    # chart's ``CURIE_SESSION_ID=warm-unbound`` placeholder is a template value,
    # not a contract signal, and must not be used as the mode switch.
    #
    # Consumer authority policy (documented here; realized by the runner, not
    # by this package):
    #   - ``runner_token`` present: bound per-conversation mode. The bootstrap
    #     value is ignored and never admitted, so the cold path is unchanged
    #     even if a pool template and a per-claim render both wrote a key.
    #   - only ``runner_bootstrap_token`` present: bootstrap mode. The
    #     credential authorizes one adoption; any other gated request that
    #     presents it is refused before a model turn starts, and after the
    #     adoption is applied it authenticates nothing against that pod.
    #   - neither present: today's tokenless legacy boot, unchanged. Nothing
    #     is relaxed and nothing new is enforced by this field's existence.
    #
    # Secret material: omitted from ``repr``/``str``; a present value that is
    # empty, blank, oversize or not a string fails the parse closed with an
    # error that never carries the material (``_admit_runner_bootstrap_token``,
    # on every construction path including ``from_env``). Unlike every other
    # optional boot var, a declared-but-EMPTY value is NOT "unset": the only
    # producer is a pool Secret, an empty ``secretKeyRef`` is a mis-render, and
    # decoding it as "neither token present" would boot a warm pod that
    # enforces nothing. Only an ABSENT key is the compatible legacy case.
    # Declaring the key is not bootstrap mode, adoption, retirement, or a
    # pool; those remain #1492 realizing work.
    runner_bootstrap_token: str | None = Field(
        default=None,
        repr=False,
        min_length=1,
        max_length=RUNNER_BOOTSTRAP_TOKEN_MAX_CHARS,
        pattern=r".*\S.*",
        json_schema_extra=_env("CURIE_RUNNER_BOOTSTRAP_TOKEN", "substrate"),
    )
    # The agent's pinned model (#254), overriding the worker default. The chart
    # also bakes a template default (inference.model, or runner.model) so a warm
    # pod boots resolvable; the per-claim value wins under Overrides.
    model: str | None = Field(
        default=None, json_schema_extra=_env("CURIE_MODEL", "worker", "substrate")
    )
    fake_model: bool | None = Field(
        default=None, json_schema_extra=_env("CURIE_FAKE_MODEL", "worker", "substrate")
    )
    # This thread's transcript key on the state store (#20, ADR-0029).
    # Deliberately NOT derived from memory_ref: memory is per-agent durable
    # lessons, history is this thread's conversation (ADR-0025 keeps them apart).
    history_ref: str | None = Field(
        default=None, json_schema_extra=_env("CURIE_HISTORY_REF", "worker")
    )
    # Scoped ``state`` tokens (ADR-0033, #410), not the raw platform key.
    history_token: str | None = Field(
        default=None, json_schema_extra=_env("CURIE_HISTORY_TOKEN", "worker")
    )
    memory_token: str | None = Field(
        default=None, json_schema_extra=_env("CURIE_MEMORY_TOKEN", "worker")
    )
    # The durable state store exposed to bundle code (#249, epic #23). state_url
    # is the agent's state namespace base on the API state router
    # (``.../agents/<id>/state``); the auto-mounted ``curie-state`` MCP server
    # and any bundle script that talks to the store directly compose
    # ``/<namespace>/<key>`` onto it. state_token is the scoped ``state`` token
    # (ADR-0033) the caller presents as X-API-Key -- the same per-turn scoped
    # derivative used for memory/history, never the raw platform key.
    state_url: str | None = Field(default=None, json_schema_extra=_env("CURIE_STATE_URL", "worker"))
    state_token: str | None = Field(
        default=None, json_schema_extra=_env("CURIE_STATE_TOKEN", "worker")
    )
    # Per-agent permission gates (#245, ADR-0010).
    approval_required_tools: list[str] | None = Field(
        default=None, json_schema_extra=_env("CURIE_APPROVAL_REQUIRED_TOOLS", "worker")
    )
    # One-shot post-approval allowance (#430, ADR-0035) and the authority-free
    # turn-end reconciliation marker (#544). Both are the kernel resume overlay's:
    # the binding never writes them, only the resume path does.
    approval_grant_tool: str | None = Field(
        default=None, json_schema_extra=_env("CURIE_APPROVAL_GRANT_TOOL", "kernel")
    )
    approval_resumed_kind: str | None = Field(
        default=None, json_schema_extra=_env("CURIE_APPROVAL_RESUMED_KIND", "kernel")
    )
    # ADR-0076 Stone 3 (#889, epic #512): the resolved terminal decision
    # ('approved'/'rejected'/'expired') of the approval this resume boot is
    # resuming from, so the runner can stamp it onto the turn's OTel span and
    # close the "did an approval get requested" gap ADR-0038 named open. Also
    # an authority-free fact, like approval_resumed_kind -- it confers no
    # capability, it only reports an outcome the worker already resolved.
    approval_decision: str | None = Field(
        default=None, json_schema_extra=_env("CURIE_APPROVAL_DECISION", "kernel")
    )
    # Names which boot keys are per-agent connector secrets (ADR-0009, #429), so
    # the k8s substrate strips those plaintext values off the claim CR. Declared
    # here so the key is typed, exported, and parseable, but the worker's
    # ``inject_connector_secrets`` stays its SOLE writer: its value is computed
    # from the undeclared operator-named keys, which the model cannot see.
    connector_secret_keys: list[str] | None = Field(
        default=None, json_schema_extra=_env("CURIE_CONNECTOR_SECRET_KEYS", "worker")
    )
    # Where this sandbox's hosted connectors live (ADR-0086, #1063/#1118).
    # Curie derives each declared connector's MCP URL from the Service it
    # created, whose name is `<release>-<agent>-mcp-<connector>` in
    # `<namespace>`. Those three are install-time facts the bundle cannot know:
    # the Helm release name and nameOverride live with whoever ran `cluster up`,
    # and the agent name is assigned at deploy.
    #
    # Optional as a set, and absent is MEANINGFUL rather than a degraded
    # cluster boot: the skill tier hosts nothing, so there is no Service to
    # derive a URL from and a declared connector is correctly reported as
    # "declared but not exercisable here" (#1093). The runner treats all three
    # missing as "no hosted connectors in this tier" and derives nothing.
    connector_release: str | None = Field(
        default=None, json_schema_extra=_env("CURIE_CONNECTOR_RELEASE", "worker")
    )
    connector_agent: str | None = Field(
        default=None, json_schema_extra=_env("CURIE_CONNECTOR_AGENT", "worker")
    )
    connector_namespace: str | None = Field(
        default=None, json_schema_extra=_env("CURIE_CONNECTOR_NAMESPACE", "worker")
    )
    # Substrate-authoritative; see the class docstring's anti-clobber note.
    port: int | None = Field(default=None, json_schema_extra=_env("CURIE_RUNNER_PORT", "substrate"))
    # Worker-authoritative with a chart fallback default.
    base_url: str | None = Field(
        default=None, json_schema_extra=_env("ANTHROPIC_BASE_URL", "worker", "substrate")
    )
    # The endpoint's declared wire protocol (#514, ADR-0047): named rather than
    # inferred, so an OpenAI-shaped endpoint is rejected up front in the runner's
    # sdk_auth instead of being silently mis-dialed.
    api_backend: str | None = Field(
        default=None, json_schema_extra=_env("CURIE_MODEL_API_BACKEND", "worker")
    )
    # How deeply the model may reason before answering (#1182, ADR-0098). A
    # sibling of ``model``: same producer, same consumer, same two-layer operator
    # ownership (platform default, per-agent override, no bundle surface), so it
    # is a declared boot key on the same grounds rather than an undeclared env.
    #
    # Deliberately ``str`` and not an enum here. The vocabulary and its rejection
    # belong to the runner (as ``ApiBackend`` does for ``api_backend`` above), so
    # that the wire contract does not mirror claude-agent-sdk's option shape and a
    # harness swap is not a protocol change.
    #
    # Unset is the pre-#1182 behavior verbatim: the runner sends no thinking
    # configuration and the model's own default stands.
    thinking: str | None = Field(
        default=None, json_schema_extra=_env("CURIE_THINKING", "worker")
    )
    # Which env var(s) carry the model credential (#514): a bare name or a JSON
    # array of them, walked in order. Unset, the runner falls back to
    # CURIE_CREDENTIALS, which is today's behavior.
    model_env_key: str | None = Field(
        default=None, json_schema_extra=_env("CURIE_MODEL_ENV_KEY", "worker")
    )
    # Operator-owned bounds, reachable through the chart's ``runner.extraEnv``
    # and docker ``-e``. No code producer emits them, and they hold no default
    # here: a non-None default would render keys nobody sends and move the wire.
    # The defaults live where the runner consumes the parsed value.
    max_turns: int | None = Field(
        default=None, json_schema_extra=_env("CURIE_MAX_TURNS", "operator")
    )
    history_max_turns: int | None = Field(
        default=None, json_schema_extra=_env("CURIE_HISTORY_MAX_TURNS", "operator")
    )
    history_max_bytes: int | None = Field(
        default=None, json_schema_extra=_env("CURIE_HISTORY_MAX_BYTES", "operator")
    )

    @classmethod
    def model_validate_json(
        cls,
        json_data: str | bytes | bytearray,
        **kwargs: Any,
    ) -> "BootEnv":
        # The redacted error is raised OUTSIDE the except block on purpose:
        # ``raise ... from None`` only suppresses display, and the original
        # error (whose ``json_invalid`` input is the raw material) would stay
        # reachable through ``__context__``. Leaving the block first drops it.
        redacted: ValidationError | None = None
        try:
            return super().model_validate_json(json_data, **kwargs)
        except ValidationError as exc:
            redacted = redact_credential_error(
                exc, title="BootEnv", field=_BOOTSTRAP_FIELD, message=_BOOTSTRAP_MALFORMED
            )
        except json.JSONDecodeError:
            redacted = _malformed_boot_env_error()
        raise redacted

    @model_validator(mode="wrap")
    @classmethod
    def _admit_runner_bootstrap_token(cls, data: Any, handler: Any) -> Any:
        # Validate and (on failure) drop the presented value before pydantic
        # records ``input_value``, exactly as ``Event._admit_adoption_credential``
        # does: the wrap path is what keeps a boot-time log line or an HTTP body
        # that interpolates ``ValidationError`` from echoing the credential.
        # Redacted errors are raised outside their except blocks (see
        # ``model_validate_json``) so no ``__context__`` retains the material.
        redacted: ValidationError | None = None
        if isinstance(data, Mapping) and _BOOTSTRAP_FIELD in data:
            incoming = dict(data)
            raw = incoming.pop(_BOOTSTRAP_FIELD)
            try:
                incoming[_BOOTSTRAP_FIELD] = parse_runner_bootstrap_token(raw)
            except ValueError:
                redacted = _malformed_boot_env_error()
            if redacted is not None:
                raise redacted
            data = incoming
        try:
            return handler(data)
        except ValidationError as exc:
            redacted = redact_credential_error(
                exc, title="BootEnv", field=_BOOTSTRAP_FIELD, message=_BOOTSTRAP_MALFORMED
            )
        raise redacted

    @classmethod
    def _declared(cls) -> dict[str, tuple[str, tuple[Producer, ...]]]:
        """Field name -> (env key, producers) for the whole flattened surface.

        The single source both ``env_keys`` and ``env_key`` derive from, so the
        two cannot disagree by construction: every name ``env_key`` returns is in
        ``env_keys()`` and vice versa, with no assertion needed to keep them
        honest.

        Flattened means the nested ``SessionConfig``/OTel keys are included: the
        chart bakes ``CURIE_RUNNER_PORT`` and the OTel keys into the runner
        container itself, so a non-flattened list would fail the render-assert on
        a default render.
        """

        out: dict[str, tuple[str, tuple[Producer, ...]]] = dict(_SESSION_ENV)
        for name, field in cls.model_fields.items():
            extra = field.json_schema_extra
            if not isinstance(extra, dict) or "env" not in extra:
                continue
            key = extra["env"]
            producers = extra.get("producer")
            if not isinstance(key, str) or not isinstance(producers, list) or not producers:
                # Not defensive padding: an untagged or mistyped key would be
                # exported to Rust as a constant nobody owns, and would slip past
                # the render-surface subset tests that derive their expectations
                # from this map. Fail at import, not at boot.
                raise TypeError(
                    f"BootEnv field declaring env {key!r} must carry a non-empty "
                    f"`producer` list; got {producers!r}"
                )
            if name in out:
                # A BootEnv field shadowing a session field name would make
                # env_key(name) ambiguous and silently return one of the two.
                raise TypeError(f"BootEnv field {name!r} collides with a session field name")
            out[name] = (key, cast(tuple[Producer, ...], tuple(producers)))
        return out

    @classmethod
    def env_keys(cls, producer: Producer | None = None) -> tuple[str, ...]:
        """The declared boot-env keys, sorted; optionally only ``producer``'s.

        Sorted so the generated Rust const module cannot flap the drift gate,
        which regenerates and runs ``git diff --exit-code``.
        """

        return tuple(
            sorted(
                key
                for key, producers in cls._declared().values()
                if producer is None or producer in producers
            )
        )

    @classmethod
    def env_key(cls, field: str) -> str:
        """The env NAME one declared boot field travels as.

        The per-key accessor a producer uses instead of retyping a literal, e.g.
        ``BootEnv.env_key("model") == "CURIE_MODEL"``. It reaches the composed
        ``SessionConfig``/OTel keys by their own field names
        (``BootEnv.env_key("budget") == "CURIE_BUDGET"``) without touching that
        frozen model, so a producer that cannot use ``render_worker`` -- the eval
        consumer sets neither memory_ref nor history_ref and must not emit them --
        still derives every name from this one declaration.

        Raises ``KeyError`` on an unknown field: a typo must fail loudly at
        import rather than return None and silently emit nothing.
        """

        declared = cls._declared()
        if field not in declared:
            raise KeyError(
                f"{field!r} is not a declared boot-env field; known fields: {sorted(declared)}"
            )
        return declared[field][0]

    @classmethod
    def render_worker(
        cls,
        *,
        plugin_dir: str,
        session_id: str,
        budget: Budget,
        memory_ref: str,
        history_ref: str,
        bundle_ref: str | None = None,
        bundle_version: str | None = None,
        runner_token: str | None = None,
        model: str | None = None,
        fake_model: bool | None = None,
        credentials_ref: str | None = None,
        base_url: str | None = None,
        api_backend: str | None = None,
        thinking: str | None = None,
        model_env_key: str | None = None,
        history_token: str | None = None,
        memory_token: str | None = None,
        state_url: str | None = None,
        state_token: str | None = None,
        approval_required_tools: Sequence[str] | None = None,
        connector_release: str | None = None,
        connector_agent: str | None = None,
        connector_namespace: str | None = None,
    ) -> dict[str, str]:
        """Render the worker binding's boot-env subset.

        The one real render surface. Its emitted keys are a subset of the
        ``worker``-producer keys, with the difference exactly
        ``{CURIE_CONNECTOR_SECRET_KEYS}`` -- the worker's
        ``inject_connector_secrets`` sets that marker on the merged dict after
        this returns, keeping the #457 order-independent filter and the #429
        marker semantics byte-identical.

        It never emits ``CURIE_SANDBOX_ID`` or ``CURIE_RUNNER_PORT``: both
        are substrate-authoritative and ``envVarsInjectionPolicy: Overrides``
        would make a worker write clobber the substrate's real value.

        Unset optionals are omitted, never emitted empty.

        Every key comes from ``env_key`` rather than a retyped literal, so a
        rename of a declared env name moves this render with it BY
        CONSTRUCTION. The per-field emit CONDITIONS are deliberately not
        uniform (truthiness here, identity in ``to_env``) and each one is the
        behavior its var has today; only the key's source is derived.
        """

        env: dict[str, str] = {
            cls.env_key("plugin_dir"): plugin_dir,
            cls.env_key("session_id"): session_id,
            cls.env_key("budget"): budget.model_dump_json(),
            cls.env_key("memory_ref"): memory_ref,
            cls.env_key("history_ref"): history_ref,
        }
        if bundle_ref:
            env[cls.env_key("bundle_ref")] = bundle_ref
        if bundle_version:
            env[cls.env_key("bundle_version")] = bundle_version
        if runner_token:
            env[cls.env_key("runner_token")] = runner_token
        if approval_required_tools:
            env[cls.env_key("approval_required_tools")] = ",".join(approval_required_tools)
        if fake_model:
            env[cls.env_key("fake_model")] = "1"
        if credentials_ref:
            env[cls.env_key("credentials_ref")] = credentials_ref
        if base_url:
            env[cls.env_key("base_url")] = base_url
        if api_backend:
            env[cls.env_key("api_backend")] = api_backend
        if thinking:
            env[cls.env_key("thinking")] = thinking
        if model_env_key:
            env[cls.env_key("model_env_key")] = model_env_key
        if model:
            env[cls.env_key("model")] = model
        if history_token:
            env[cls.env_key("history_token")] = history_token
        if memory_token:
            env[cls.env_key("memory_token")] = memory_token
        if state_url:
            env[cls.env_key("state_url")] = state_url
        if state_token:
            env[cls.env_key("state_token")] = state_token
        # Emitted as a SET or not at all: the runner derives a connector URL
        # from all three or derives nothing, so a partial scope would name a
        # Service that cannot exist.
        if connector_release and connector_agent and connector_namespace:
            env[cls.env_key("connector_release")] = connector_release
            env[cls.env_key("connector_agent")] = connector_agent
            env[cls.env_key("connector_namespace")] = connector_namespace
        return env

    def to_env(self) -> dict[str, str]:
        """Render the whole union, for round-trip checks only.

        Nothing on the wire path calls this: the worker cannot build it (it does
        not know ``sandbox_id``) and emitting the union from the worker is the
        clobber path. Use ``render_worker`` to produce a real boot env.

        Keys derive from ``env_key``; the nested frozen session keys stay
        ``SessionConfig.to_env``'s own. As in ``render_worker``, only the key's
        source is derived -- each field keeps the emit condition it has today.
        """

        env = self.session.to_env()
        if self.bundle_ref is not None:
            env[self.env_key("bundle_ref")] = self.bundle_ref
        if self.bundle_version is not None:
            env[self.env_key("bundle_version")] = self.bundle_version
        if self.runner_token is not None:
            env[self.env_key("runner_token")] = self.runner_token
        if self.runner_bootstrap_token is not None:
            env[self.env_key("runner_bootstrap_token")] = self.runner_bootstrap_token
        if self.model is not None:
            env[self.env_key("model")] = self.model
        if self.fake_model is not None:
            env[self.env_key("fake_model")] = "1" if self.fake_model else "0"
        if self.history_ref is not None:
            env[self.env_key("history_ref")] = self.history_ref
        if self.history_token is not None:
            env[self.env_key("history_token")] = self.history_token
        if self.memory_token is not None:
            env[self.env_key("memory_token")] = self.memory_token
        if self.connector_release is not None:
            env[self.env_key("connector_release")] = self.connector_release
        if self.connector_agent is not None:
            env[self.env_key("connector_agent")] = self.connector_agent
        if self.connector_namespace is not None:
            env[self.env_key("connector_namespace")] = self.connector_namespace
        if self.state_url is not None:
            env[self.env_key("state_url")] = self.state_url
        if self.state_token is not None:
            env[self.env_key("state_token")] = self.state_token
        if self.approval_required_tools:
            env[self.env_key("approval_required_tools")] = ",".join(self.approval_required_tools)
        if self.approval_grant_tool is not None:
            env[self.env_key("approval_grant_tool")] = self.approval_grant_tool
        if self.approval_resumed_kind is not None:
            env[self.env_key("approval_resumed_kind")] = self.approval_resumed_kind
        if self.approval_decision is not None:
            env[self.env_key("approval_decision")] = self.approval_decision
        if self.connector_secret_keys:
            env[self.env_key("connector_secret_keys")] = ",".join(self.connector_secret_keys)
        if self.port is not None:
            env[self.env_key("port")] = str(self.port)
        if self.base_url is not None:
            env[self.env_key("base_url")] = self.base_url
        if self.api_backend is not None:
            env[self.env_key("api_backend")] = self.api_backend
        if self.thinking is not None:
            env[self.env_key("thinking")] = self.thinking
        if self.model_env_key is not None:
            env[self.env_key("model_env_key")] = self.model_env_key
        if self.max_turns is not None:
            env[self.env_key("max_turns")] = str(self.max_turns)
        if self.history_max_turns is not None:
            env[self.env_key("history_max_turns")] = str(self.history_max_turns)
        if self.history_max_bytes is not None:
            env[self.env_key("history_max_bytes")] = str(self.history_max_bytes)
        return env

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> "BootEnv":
        """Parse the runner's view of the whole pod env.

        The runner is the single consumer of every producer's union, so this is
        the one full parse. It inherits ``SessionConfig.from_env``'s fail-loud
        requiredness, including the ``KeyError`` on a missing
        ``CURIE_SANDBOX_ID`` -- every real boot surface supplies it, so a miss
        means a genuinely broken substrate and the runner should refuse to boot.

        Parse tolerance is deliberately NOT unified across the knobs: each var
        keeps the behavior it has today (see ``_tolerant_int`` versus the bare
        ``int`` on ``CURIE_MAX_TURNS``, which config.py:98 raises on).
        """

        return cls(
            session=SessionConfig.from_env(env),
            bundle_ref=_str_or_none(env.get("CURIE_BUNDLE_REF")),
            bundle_version=_str_or_none(env.get("CURIE_BUNDLE_VERSION")),
            runner_token=_str_or_none(env.get("CURIE_RUNNER_TOKEN")),
            # Passed through RAW, not via ``_str_or_none``: for this key a
            # declared-but-empty value is a mis-rendered pool Secret, not
            # "unset", and must fail the boot closed rather than decode as the
            # tokenless legacy boot. ``_admit_runner_bootstrap_token`` is the
            # single admission point for absent (None), empty, blank, oversize
            # and non-string values, on every construction path.
            runner_bootstrap_token=env.get("CURIE_RUNNER_BOOTSTRAP_TOKEN"),
            model=_str_or_none(env.get("CURIE_MODEL")),
            fake_model=_fake_model_or_none(env.get("CURIE_FAKE_MODEL")),
            history_ref=_str_or_none(env.get("CURIE_HISTORY_REF")),
            history_token=_str_or_none(env.get("CURIE_HISTORY_TOKEN")),
            memory_token=_str_or_none(env.get("CURIE_MEMORY_TOKEN")),
            state_url=_str_or_none(env.get("CURIE_STATE_URL")),
            state_token=_str_or_none(env.get("CURIE_STATE_TOKEN")),
            approval_required_tools=_list_or_none(env.get("CURIE_APPROVAL_REQUIRED_TOOLS")),
            approval_grant_tool=_stripped_or_none(env.get("CURIE_APPROVAL_GRANT_TOOL")),
            approval_resumed_kind=_stripped_or_none(env.get("CURIE_APPROVAL_RESUMED_KIND")),
            approval_decision=_stripped_or_none(env.get("CURIE_APPROVAL_DECISION")),
            connector_secret_keys=_list_or_none(env.get("CURIE_CONNECTOR_SECRET_KEYS")),
            connector_release=_str_or_none(env.get("CURIE_CONNECTOR_RELEASE")),
            connector_agent=_str_or_none(env.get("CURIE_CONNECTOR_AGENT")),
            connector_namespace=_str_or_none(env.get("CURIE_CONNECTOR_NAMESPACE")),
            port=_required_int(env.get("CURIE_RUNNER_PORT")),
            base_url=_str_or_none(env.get("ANTHROPIC_BASE_URL")),
            # Empty is "not declared" for both, matching sdk_auth's own
            # `env.get(...) or <default>` reads: an empty backend falls back to
            # `messages`, an empty key list to (CURIE_CREDENTIALS,).
            api_backend=_str_or_none(env.get("CURIE_MODEL_API_BACKEND")),
            # Empty is "not declared" here too: an unset or blank knob leaves the
            # runner sending no thinking configuration at all (ADR-0098).
            thinking=_str_or_none(env.get("CURIE_THINKING")),
            model_env_key=_str_or_none(env.get("CURIE_MODEL_ENV_KEY")),
            max_turns=_required_int(env.get("CURIE_MAX_TURNS")),
            history_max_turns=_tolerant_int(env.get("CURIE_HISTORY_MAX_TURNS")),
            history_max_bytes=_tolerant_int(env.get("CURIE_HISTORY_MAX_BYTES")),
        )
