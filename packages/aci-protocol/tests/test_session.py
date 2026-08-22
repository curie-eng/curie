import pytest
from aci_protocol import BootEnv, Budget, EnvProducer, OtelConfig, SessionConfig
from pydantic import BaseModel, ValidationError


def _full_config() -> SessionConfig:
    return SessionConfig(
        plugin_dir="/plugins/demo",
        session_id="thread-123",
        sandbox_id="sbx-abc",
        budget=Budget(max_output_tokens_per_run=4096, task_budget_hint=2000, max_usd_per_day=5.0),
        memory_ref="s3://bucket/memory",
        credentials_ref="k8s://secret/demo",
        otel=OtelConfig(endpoint="http://collector:4318", protocol="http/protobuf"),
    )


def test_to_env_and_from_env_roundtrip() -> None:
    config = _full_config()
    env = config.to_env()
    assert env["CURIE_PLUGIN_DIR"] == "/plugins/demo"
    assert env["CURIE_SESSION_ID"] == "thread-123"
    assert env["CURIE_SANDBOX_ID"] == "sbx-abc"
    assert env["CURIE_MEMORY_REF"] == "s3://bucket/memory"
    assert env["CURIE_CREDENTIALS"] == "k8s://secret/demo"
    assert env["OTEL_EXPORTER_OTLP_ENDPOINT"] == "http://collector:4318"
    assert env["OTEL_EXPORTER_OTLP_PROTOCOL"] == "http/protobuf"

    assert SessionConfig.from_env(env) == config


def test_optional_fields_are_omitted_from_env() -> None:
    config = SessionConfig(
        plugin_dir="/p",
        session_id="s",
        sandbox_id="b",
        budget=Budget(max_output_tokens_per_run=100, max_usd_per_day=1.0),
    )
    env = config.to_env()
    assert "CURIE_MEMORY_REF" not in env
    assert "CURIE_CREDENTIALS" not in env
    assert "OTEL_EXPORTER_OTLP_ENDPOINT" not in env
    assert SessionConfig.from_env(env) == config


def test_budget_travels_as_json() -> None:
    config = _full_config()
    env = config.to_env()
    assert '"max_output_tokens_per_run":4096' in env["CURIE_BUDGET"]


def test_missing_required_env_var_raises() -> None:
    with pytest.raises(KeyError):
        SessionConfig.from_env({"CURIE_PLUGIN_DIR": "/p"})


def test_malformed_budget_json_raises() -> None:
    env = _full_config().to_env()
    env["CURIE_BUDGET"] = "{not json"
    with pytest.raises(ValidationError):
        SessionConfig.from_env(env)


# ---------------------------------------------------------------------------
# BootEnv: the worker-to-runner boot contract (#488).
#
# The boot env has MULTIPLE producers (worker binding, worker kernel resume
# overlay, substrate/chart, operator) and ONE consumer (the runner). BootEnv is
# the consumer-union model; rendering is per-producer. There is deliberately no
# whole-model to_env() on the wire path -- the worker cannot build one, because
# it does not know CURIE_SANDBOX_ID, and emitting the full union from the
# worker is the clobber path (envVarsInjectionPolicy: Overrides, values.yaml:789).
#
# The load-bearing property is preserved per producer: the declaration moved,
# the wire did not.
# ---------------------------------------------------------------------------

# The values the worker composes its refs from. Mirrors binding.boot_env:
# session id is "agent-<agent_id>-thread-<thread_key>" (thread key raw), while
# the history ref path segment is URL-encoded (binding.py:422).
_AGENT_ID = "11111111-2222-3333-4444-555555555555"
_THREAD_KEY = "C0ABC/1720000000.001"
_THREAD_SEGMENT = "C0ABC%2F1720000000.001"
_API_BASE = "https://api.example.test"

_SESSION_ID = f"agent-{_AGENT_ID}-thread-{_THREAD_KEY}"
_MEMORY_REF = f"{_API_BASE}/agents/{_AGENT_ID}/state/memory"
_HISTORY_REF = f"{_API_BASE}/agents/{_AGENT_ID}/state/transcript/{_THREAD_SEGMENT}"

# binding.budget_for builds a Budget with no task_budget_hint, rendered with
# Budget.model_dump_json().
_BUDGET = Budget(max_output_tokens_per_run=4096, max_usd_per_day=5.0)
_BUDGET_JSON = '{"max_output_tokens_per_run":4096,"task_budget_hint":null,"max_usd_per_day":5.0}'

# What the chart/docker substrate contributes to the pod env. The chart supplies
# the sandbox id from `fieldRef: metadata.name` (agent-sandbox.yaml:424-427),
# the docker substrate from the container name (docker.py:165). The runner port
# is chart (agent-sandbox.yaml:430) / docker.py:167.
_SUBSTRATE_ENV = {
    "CURIE_SANDBOX_ID": "curie-sandbox-abc123",
    "CURIE_RUNNER_PORT": "8080",
}

_PRODUCERS = ("worker", "kernel", "substrate", "operator")


def _producers_of(key: str) -> set[str]:
    """Every producer that writes ``key``. A key may have more than one.

    Derived through the public accessor rather than reaching into the model's
    tags, so a rename of the underlying map cannot break these tests.
    """
    return {p for p in _PRODUCERS if key in BootEnv.env_keys(producer=p)}


def _worker_env(**overrides: object) -> dict[str, str]:
    """The worker render surface for a plain bound run, with overrides applied."""
    kwargs: dict[str, object] = {
        "plugin_dir": "/plugins/bundle",
        "session_id": _SESSION_ID,
        "budget": _BUDGET,
        "memory_ref": _MEMORY_REF,
        "history_ref": _HISTORY_REF,
        "bundle_ref": "bundles/demo/abc123.tar.gz",
        "runner_token": "rt-plain-token",
        "model": "claude-opus-4-6",
        "history_token": "st-scoped-token",
        "memory_token": "st-scoped-token",
    }
    kwargs.update(overrides)
    return BootEnv.render_worker(**kwargs)  # type: ignore[arg-type]


def _boot_session(sandbox_id: str = "sbx-plain") -> SessionConfig:
    return SessionConfig(
        plugin_dir="/plugins/bundle",
        session_id=_SESSION_ID,
        sandbox_id=sandbox_id,
        budget=_BUDGET,
        memory_ref=_MEMORY_REF,
    )


def _full_boot_env() -> BootEnv:
    """Every declared BootEnv field set, for the round-trip checks.

    Constructed directly rather than through a render surface: BootEnv is the
    consumer union, so this is the runner's view of a maximal pod env.
    """
    return BootEnv(
        session=SessionConfig(
            plugin_dir="/plugins/bundle",
            session_id=_SESSION_ID,
            sandbox_id="sbx-full",
            budget=Budget(
                max_output_tokens_per_run=4096, task_budget_hint=2000, max_usd_per_day=5.0
            ),
            memory_ref=_MEMORY_REF,
            credentials_ref="k8s://secret/demo",
            otel=OtelConfig(
                endpoint="http://collector:4318",
                headers="authorization=Bearer x",
                protocol="http/protobuf",
            ),
        ),
        bundle_ref="bundles/demo/abc123.tar.gz",
        runner_token="rt-full-token",
        model="claude-opus-4-6",
        fake_model=True,
        history_ref=_HISTORY_REF,
        history_token="st-history-token",
        memory_token="st-memory-token",
        state_url="http://api:8000/agents/agent-abc/state",
        state_token="st-state-token",
        delegate_url="http://api:8000/agents/agent-abc/delegate/calls",
        delegate_token="sbx-delegate-token",
        approval_required_tools=["Bash", "mcp__github__create_pr"],
        approval_grant_tool="Bash",
        approval_resumed_kind="policy",
        approval_decision="approved",
        connector_secret_keys=["GITHUB_TOKEN", "LINEAR_API_KEY"],
        connector_release="curie",
        connector_agent="acme-dev",
        connector_namespace="curie-prod",
        port=9090,
        base_url="http://litellm:4000",
        api_backend="messages",
        thinking="disabled",
        model_env_key="MY_PROVIDER_KEY",
        max_turns=50,
        history_max_turns=10,
        history_max_bytes=2048,
    )


def _unpopulated_fields(model: BaseModel, prefix: str = "") -> list[str]:
    """Every field on ``model`` still at its declared default, recursing into
    nested models.

    Enumerated at runtime off ``model_fields`` rather than from a hand-kept
    list, so a field ADDED to the model tomorrow shows up here the moment the
    fixture does not populate it. Compared against the field's own declared
    default (via ``FieldInfo.get_default``), not a hardcoded ``None``, so a
    future field defaulting to ``False``/``0``/``""`` is still caught. A
    required field has no default (``get_default`` returns
    ``PydanticUndefined``), which never equals a real value, so a populated
    required field is never flagged. Nested lists or dicts of models are not
    walked; no field on these models has that shape today.
    """

    unpopulated: list[str] = []
    for name, field in type(model).model_fields.items():
        value = getattr(model, name)
        path = f"{prefix}{name}"
        default = field.get_default(call_default_factory=True)
        if value == default:
            unpopulated.append(path)
        elif isinstance(value, BaseModel):
            unpopulated.extend(_unpopulated_fields(value, prefix=f"{path}."))
    return unpopulated


# --- Model shape -------------------------------------------------------------


def test_boot_env_roundtrips_every_declared_field() -> None:
    """The class-closing invariant: nothing ``to_env`` emits may fail to parse back.

    A field that ``to_env`` writes but ``from_env`` never reads comes back
    ``None``, and nothing errors: the runner boots fine with the feature
    silently switched off. That is exactly how CURIE_CONNECTOR_RELEASE /
    _AGENT / _NAMESPACE were lost (#1195), and it is a defect the whole model is
    exposed to every time a field is added.

    The population guard is what keeps this test from decaying into a
    tautology. A newly added optional field left unset in the fixture would
    round trip as ``None == None`` and prove nothing, so the guard fails first
    and names the field, forcing whoever adds it to give it a real value here.
    """

    boot = _full_boot_env()
    unpopulated = _unpopulated_fields(boot)
    assert not unpopulated, (
        "the round-trip fixture leaves these fields at their declared default, so "
        "the assertion below would pass over them vacuously; give each a distinct, "
        f"non-default value in _full_boot_env: {', '.join(unpopulated)}"
    )
    assert BootEnv.from_env(boot.to_env()) == boot


def test_boot_env_roundtrips_with_every_optional_omitted() -> None:
    boot = BootEnv(session=_boot_session())
    assert BootEnv.from_env(boot.to_env()) == boot


def test_boot_env_composes_the_frozen_session_config_rather_than_extending_it() -> None:
    """SessionConfig stays the ACI section-0 contract; BootEnv is the superset.

    The nesting is the ACI-vs-platform boundary. A BootEnv that inherited
    SessionConfig would tell every ACI implementer that a runner token and
    approval plumbing are part of the interface.
    """
    boot = _full_boot_env()
    assert isinstance(boot.session, SessionConfig)
    assert not issubclass(BootEnv, SessionConfig)
    for key, value in boot.session.to_env().items():
        assert boot.to_env()[key] == value


def test_boot_env_from_env_coerces_empty_token_strings_to_none() -> None:
    """A declared-but-empty var must not become an empty credential.

    Preserves runner/src/curie_runner/config.py:105
    ``runner_token=env.get("CURIE_RUNNER_TOKEN") or None``. Get this wrong and
    the fake/local no-key path presents an empty bearer token.
    """
    env = BootEnv(session=_boot_session()).to_env()
    env["CURIE_RUNNER_TOKEN"] = ""
    env["CURIE_HISTORY_TOKEN"] = ""
    env["CURIE_MEMORY_TOKEN"] = ""
    boot = BootEnv.from_env(env)
    assert boot.runner_token is None
    assert boot.history_token is None
    assert boot.memory_token is None


@pytest.mark.parametrize("raw", ["1", "true", "TRUE", "True", "yes", "YES"])
def test_boot_env_from_env_reads_the_runners_truthy_fake_model_values(raw: str) -> None:
    env = BootEnv(session=_boot_session()).to_env()
    env["CURIE_FAKE_MODEL"] = raw
    assert BootEnv.from_env(env).fake_model is True


@pytest.mark.parametrize("raw", ["0", "false", "FALSE", "no", "off", "fasle"])
def test_boot_env_from_env_reads_every_other_fake_model_value_as_off(raw: str) -> None:
    """Anything outside the runner's truthy set is OFF, including a typo.

    This is the half that regressed: the parse this replaces was
    ``fake_raw != "0"``, which turned ``false``, ``no``, ``off``, and every typo
    into fake-model ON, while the platform's only consumer
    (runner/src/curie_runner/__main__.py:262) reads
    ``.lower() in ("1", "true", "yes")`` and sees them as OFF. A declared field
    whose parse contradicts its consumer is a trap for the next caller who
    reaches for ``BootEnv.from_env(env).fake_model``.
    """
    env = BootEnv(session=_boot_session()).to_env()
    env["CURIE_FAKE_MODEL"] = raw
    assert BootEnv.from_env(env).fake_model is False


def test_boot_env_from_env_treats_an_absent_or_empty_fake_model_as_unset() -> None:
    """Unset stays ``None``, distinct from an explicit off, so ``to_env`` round trips.

    A declared-but-blank var is "unset" here, the same house rule ``_str_or_none``
    applies to every other optional field. That is not a disagreement with the
    runner (which reads a blank as off): ``None`` is the field's own default and
    resolves to fake-model-off at every consumer, and ``to_env`` never emits the
    key blank, so no producer renders this shape.
    """
    env = BootEnv(session=_boot_session()).to_env()
    assert "CURIE_FAKE_MODEL" not in env
    assert BootEnv.from_env(env).fake_model is None
    for blank in ("", " "):
        env["CURIE_FAKE_MODEL"] = blank
        assert BootEnv.from_env(env).fake_model is None


@pytest.mark.parametrize("declared", [True, False])
def test_boot_env_fake_model_survives_the_to_env_round_trip(declared: bool) -> None:
    boot = BootEnv(session=_boot_session(), fake_model=declared)
    assert BootEnv.from_env(boot.to_env()).fake_model is declared


def test_boot_env_declares_the_approval_resumed_kind_marker() -> None:
    """#544's turn-end reconciliation marker must not be dropped on the floor.

    It is a RunnerConfig field with no feature attached to it, so it is the
    easiest boot var in the set to lose in the move (Edge case 3).
    """
    assert "CURIE_APPROVAL_RESUMED_KIND" in BootEnv.env_keys()
    boot = BootEnv(session=_boot_session(), approval_resumed_kind="policy")
    env = boot.to_env()
    assert env["CURIE_APPROVAL_RESUMED_KIND"] == "policy"
    assert BootEnv.from_env(env).approval_resumed_kind == "policy"


def test_malformed_budget_json_raises_through_boot_env() -> None:
    env = BootEnv(session=_boot_session()).to_env()
    env["CURIE_BUDGET"] = "{not json"
    with pytest.raises(ValidationError):
        BootEnv.from_env(env)


# --- Golden 1: worker parity. Frozen dict literals. --------------------------
#
# Written out explicitly rather than computed from the model under test: a
# literal derived from BootEnv would pass against a broken BootEnv. Each literal
# is what binding.boot_env renders today for that shape, minus CURIE_AGENT_ID.
# These must keep passing when a BootEnv field is RENAMED internally -- the env
# key is the contract, the attribute name is not.


def test_render_worker_matches_the_frozen_plain_bound_run_wire() -> None:
    """A bound run with a bundle, a pinned model, and a platform api_key."""
    assert _worker_env() == {
        "CURIE_PLUGIN_DIR": "/plugins/bundle",
        "CURIE_SESSION_ID": (
            "agent-11111111-2222-3333-4444-555555555555-thread-C0ABC/1720000000.001"
        ),
        "CURIE_BUDGET": _BUDGET_JSON,
        "CURIE_MEMORY_REF": (
            "https://api.example.test/agents/11111111-2222-3333-4444-555555555555/state/memory"
        ),
        "CURIE_BUNDLE_REF": "bundles/demo/abc123.tar.gz",
        "CURIE_RUNNER_TOKEN": "rt-plain-token",
        "CURIE_MODEL": "claude-opus-4-6",
        "CURIE_HISTORY_REF": (
            "https://api.example.test/agents/11111111-2222-3333-4444-555555555555"
            "/state/transcript/C0ABC%2F1720000000.001"
        ),
        "CURIE_HISTORY_TOKEN": "st-scoped-token",
        "CURIE_MEMORY_TOKEN": "st-scoped-token",
    }


def test_render_worker_matches_the_frozen_resume_boot_wire() -> None:
    """A resume boot: the agent carries permission gates (binding.py:406-407).

    The two resume keys themselves are the kernel's overlay (kernel.py:317,334),
    not binding.boot_env's -- see the kernel overlay golden below.
    """
    assert _worker_env(
        runner_token="rt-resume-token",
        approval_required_tools=["Bash", "mcp__github__create_pr"],
    ) == {
        "CURIE_PLUGIN_DIR": "/plugins/bundle",
        "CURIE_SESSION_ID": (
            "agent-11111111-2222-3333-4444-555555555555-thread-C0ABC/1720000000.001"
        ),
        "CURIE_BUDGET": _BUDGET_JSON,
        "CURIE_MEMORY_REF": (
            "https://api.example.test/agents/11111111-2222-3333-4444-555555555555/state/memory"
        ),
        "CURIE_BUNDLE_REF": "bundles/demo/abc123.tar.gz",
        "CURIE_RUNNER_TOKEN": "rt-resume-token",
        "CURIE_MODEL": "claude-opus-4-6",
        "CURIE_HISTORY_REF": (
            "https://api.example.test/agents/11111111-2222-3333-4444-555555555555"
            "/state/transcript/C0ABC%2F1720000000.001"
        ),
        "CURIE_HISTORY_TOKEN": "st-scoped-token",
        "CURIE_MEMORY_TOKEN": "st-scoped-token",
        # binding.py:407 comma-joins the names, order preserved from the agent row.
        "CURIE_APPROVAL_REQUIRED_TOOLS": "Bash,mcp__github__create_pr",
    }


def test_render_worker_matches_the_frozen_fake_local_boot_wire() -> None:
    """The no-api-key fake/local path: no token is minted, so none is set.

    binding.py:430 gates MEMORY_TOKEN/HISTORY_TOKEN on config.api_key, and
    apply_model_env sets CURIE_FAKE_MODEL='1' with no model and no credential.
    """
    assert _worker_env(
        runner_token="rt-fake-token",
        model=None,
        fake_model=True,
        history_token=None,
        memory_token=None,
    ) == {
        "CURIE_PLUGIN_DIR": "/plugins/bundle",
        "CURIE_SESSION_ID": (
            "agent-11111111-2222-3333-4444-555555555555-thread-C0ABC/1720000000.001"
        ),
        "CURIE_BUDGET": _BUDGET_JSON,
        "CURIE_MEMORY_REF": (
            "https://api.example.test/agents/11111111-2222-3333-4444-555555555555/state/memory"
        ),
        "CURIE_BUNDLE_REF": "bundles/demo/abc123.tar.gz",
        "CURIE_RUNNER_TOKEN": "rt-fake-token",
        # apply_model_env renders the flag as the literal "1", never "true".
        "CURIE_FAKE_MODEL": "1",
        "CURIE_HISTORY_REF": (
            "https://api.example.test/agents/11111111-2222-3333-4444-555555555555"
            "/state/transcript/C0ABC%2F1720000000.001"
        ),
    }


def test_render_worker_omits_unset_optionals_rather_than_emitting_empty_strings() -> None:
    """Unset optionals are absent, never present-and-empty (Edge case 7)."""
    env = _worker_env(
        bundle_ref=None, model=None, history_token=None, memory_token=None, runner_token=None
    )
    for key in (
        "CURIE_BUNDLE_REF",
        "CURIE_RUNNER_TOKEN",
        "CURIE_MODEL",
        "CURIE_FAKE_MODEL",
        "CURIE_HISTORY_TOKEN",
        "CURIE_MEMORY_TOKEN",
        "CURIE_APPROVAL_REQUIRED_TOOLS",
        "ANTHROPIC_BASE_URL",
    ):
        assert key not in env, f"{key} must be omitted when unset, not emitted empty"
    assert "" not in env.values()


# --- Golden 2: the anti-clobber negative. MANDATORY. -------------------------


@pytest.mark.parametrize("shape", ["plain", "resume", "fake"])
def test_render_worker_never_emits_substrate_owned_identity_or_port(shape: str) -> None:
    """THE ANTI-CLOBBER GUARD. Do not weaken this test; read why first.

    charts/curie/values.yaml:789 sets ``envVarsInjectionPolicy: Overrides``,
    and agent-sandbox.yaml:15 states that per-claim injection WINS over the pod
    template's own env. So if the worker ever emitted CURIE_SANDBOX_ID, it
    would REPLACE the chart's ``fieldRef: metadata.name`` value
    (agent-sandbox.yaml:422-427) with a worker-invented one and break the
    "pod name IS the sandbox id" invariant that trace stamping (otel.py:43) and
    operator correlation depend on. CURIE_RUNNER_PORT is the same story
    (agent-sandbox.yaml:430, docker.py:167).

    Both keys are substrate-owned. The docker tier is only INCIDENTALLY shielded
    by _WORKER_OWNED_ENV (docker.py:84-86); k8s has no such shield. This test is
    the guard.
    """
    shapes: dict[str, dict[str, object]] = {
        "plain": {},
        "resume": {"approval_required_tools": ["Bash"]},
        "fake": {"model": None, "fake_model": True, "history_token": None, "memory_token": None},
    }
    env = _worker_env(**shapes[shape])
    assert "CURIE_SANDBOX_ID" not in env, (
        "the worker must never emit CURIE_SANDBOX_ID: envVarsInjectionPolicy=Overrides "
        "would clobber the chart's fieldRef metadata.name and break sandbox identity"
    )
    assert "CURIE_RUNNER_PORT" not in env, (
        "the worker must never emit CURIE_RUNNER_PORT: it is substrate-owned "
        "(agent-sandbox.yaml:430, docker.py:167)"
    )


def test_render_worker_emits_exactly_the_worker_owned_key_subset() -> None:
    """The render surface cannot drift from the producer map in either direction.

    The one carve-out is CURIE_CONNECTOR_SECRET_KEYS: inject_connector_secrets
    (binding.py:482-515) stays its sole writer, setting it on the merged dict
    after the render returns (Edge case 9), so the #457 order-independent filter
    and #429 marker semantics are byte-identical to today.
    """
    maximal = _worker_env(
        approval_required_tools=["Bash"],
        fake_model=True,
        base_url="http://litellm:4000",
        credentials_ref="k8s://secret/demo",
        api_backend="messages",
        thinking="disabled",
        model_env_key="MY_PROVIDER_KEY",
        state_url="http://api:8000/agents/agent-abc/state",
        state_token="st-scoped-token",
        delegate_url="http://api:8000/agents/agent-abc/delegate/calls",
        delegate_token="sbx-delegate-token",
        connector_release="curie",
        connector_agent="acme-dev",
        connector_namespace="curie",
    )
    worker_owned = set(BootEnv.env_keys(producer="worker"))
    assert set(maximal) <= worker_owned
    assert worker_owned - set(maximal) == {"CURIE_CONNECTOR_SECRET_KEYS"}


# --- Golden 3: the kernel resume overlay. ------------------------------------


def test_the_kernel_owns_exactly_the_three_approval_resume_keys() -> None:
    """kernel.py layers these onto binding.boot_env's dict after the fact.

    They are a distinct producer: same process, different code path, rendered
    independently. The kernel sets them via the exported constants, so the
    producer map is what pins the overlay's exact extent. ADR-0076/#889 added
    ``CURIE_APPROVAL_DECISION`` alongside the original two.
    """
    assert set(BootEnv.env_keys(producer="kernel")) == {
        "CURIE_APPROVAL_GRANT_TOOL",
        "CURIE_APPROVAL_RESUMED_KIND",
        "CURIE_APPROVAL_DECISION",
    }


def test_the_kernel_overlay_keys_are_not_worker_rendered() -> None:
    """binding.boot_env never sets them; only the resume path does."""
    env = _worker_env(approval_required_tools=["Bash"])
    assert "CURIE_APPROVAL_GRANT_TOOL" not in env
    assert "CURIE_APPROVAL_RESUMED_KIND" not in env
    assert "CURIE_APPROVAL_DECISION" not in env


# --- Golden 4: the consumer parse. -------------------------------------------


def test_from_env_parses_the_worker_subset_plus_the_substrate_fixture() -> None:
    """The runner is the single consumer of the producers' union.

    The typed values asserted here are what RunnerConfig.from_env produces today
    on the same env (config.py:94-106).
    """
    boot = BootEnv.from_env(_worker_env() | _SUBSTRATE_ENV)
    assert boot.session.sandbox_id == "curie-sandbox-abc123"
    assert boot.session.plugin_dir == "/plugins/bundle"
    assert boot.session.session_id == _SESSION_ID
    assert boot.session.budget.max_output_tokens_per_run == 4096
    assert boot.session.budget.max_usd_per_day == 5.0
    assert boot.session.memory_ref == _MEMORY_REF
    assert boot.model == "claude-opus-4-6"
    assert boot.runner_token == "rt-plain-token"
    assert boot.bundle_ref == "bundles/demo/abc123.tar.gz"
    assert boot.history_ref == _HISTORY_REF
    assert boot.history_token == "st-scoped-token"
    assert boot.memory_token == "st-scoped-token"
    assert boot.port == 8080


def test_from_env_on_the_worker_subset_alone_raises() -> None:
    """Fail-loud by design; do not soften into a default.

    The worker subset is not a complete boot env: no producer in the worker lane
    sets CURIE_SANDBOX_ID, and SessionConfig.from_env reaches it by bracket
    access (session.py:107). Every real boot surface supplies it (chart fieldRef,
    docker.py:165, docker.rs:92), so a KeyError here means a genuinely broken
    substrate, which is exactly when the runner should refuse to boot.
    """
    with pytest.raises(KeyError):
        BootEnv.from_env(_worker_env())


def test_from_env_parses_the_resume_overlay() -> None:
    overlay = {
        "CURIE_APPROVAL_GRANT_TOOL": "Bash",
        "CURIE_APPROVAL_RESUMED_KIND": "policy",
    }
    boot = BootEnv.from_env(
        _worker_env(approval_required_tools=["Bash", "mcp__github__create_pr"])
        | _SUBSTRATE_ENV
        | overlay
    )
    assert boot.approval_required_tools == ["Bash", "mcp__github__create_pr"]
    assert boot.approval_grant_tool == "Bash"
    assert boot.approval_resumed_kind == "policy"


# --- The knob-parsing asymmetry (Edge case 12). ------------------------------
#
# Deliberately NOT unified. Each var keeps the behavior it has today. The knobs
# are operator-owned: no code producer emits them, they are int | None = None on
# the model, and the defaults are applied where the runner consumes the parsed
# value. CURIE_HISTORY_MAX_TURNS/BYTES are read through __main__._int_env,
# which degrades to the default on garbage AND on nonpositive values
# (__main__.py:219-231); a ValidationError there would crash the sandbox at boot
# where it used to degrade. CURIE_MAX_TURNS uses a bare int() (config.py:98)
# and does raise. Unifying them is a behavior change wearing a consistency costume.


def test_knobs_are_none_when_absent_so_the_consumer_applies_its_own_defaults() -> None:
    """None means "nobody set this", which is not the same as the default value.

    The defaults live at the consumer: max_turns 20 (config.py:98),
    DEFAULT_PREAMBLE_MAX_TURNS 40 / DEFAULT_PREAMBLE_MAX_BYTES 16_000
    (history.py:190-191). Holding them on the model would render keys the worker
    never sends and move the wire the goldens forbid.
    """
    boot = BootEnv.from_env(_worker_env() | _SUBSTRATE_ENV)
    assert boot.max_turns is None
    assert boot.history_max_turns is None
    assert boot.history_max_bytes is None


def test_knobs_and_port_hold_no_default_value_on_the_model_itself() -> None:
    """The model must never hold a value nobody set (round-2 pinned shape).

    Tested through direct construction, not from_env: a from_env that passes the
    field explicitly would mask a non-None field default. That default is not
    cosmetic -- it would make to_env render CURIE_MAX_TURNS=20 and
    CURIE_RUNNER_PORT=8080, keys no producer sends, moving the wire and (for
    the port) handing the worker a substrate-owned key to clobber.
    """
    boot = BootEnv(session=_boot_session())
    assert boot.max_turns is None
    assert boot.history_max_turns is None
    assert boot.history_max_bytes is None
    assert boot.port is None
    env = boot.to_env()
    for key in (
        "CURIE_MAX_TURNS",
        "CURIE_HISTORY_MAX_TURNS",
        "CURIE_HISTORY_MAX_BYTES",
        "CURIE_RUNNER_PORT",
    ):
        assert key not in env, f"{key} was rendered although no producer set it"


def test_knobs_parse_when_set_through_the_declared_operator_surface() -> None:
    env = (
        _worker_env()
        | _SUBSTRATE_ENV
        | {
            "CURIE_MAX_TURNS": "5",
            "CURIE_HISTORY_MAX_TURNS": "7",
            "CURIE_HISTORY_MAX_BYTES": "512",
        }
    )
    boot = BootEnv.from_env(env)
    assert boot.max_turns == 5
    assert boot.history_max_turns == 7
    assert boot.history_max_bytes == 512


@pytest.mark.parametrize("garbage", ["abc", "", "   ", "0", "-5", "3.5"])
def test_history_window_knobs_degrade_rather_than_raise_on_garbage(garbage: str) -> None:
    """_int_env rejects unparseable AND nonpositive values, using the default.

    Raising would turn a typo in an operator's extraEnv into a boot crash.
    Degrading to None hands the consumer its own default, which is the existing
    behavior expressed through the declared surface.
    """
    env = (
        _worker_env()
        | _SUBSTRATE_ENV
        | {
            "CURIE_HISTORY_MAX_TURNS": garbage,
            "CURIE_HISTORY_MAX_BYTES": garbage,
        }
    )
    boot = BootEnv.from_env(env)
    assert boot.history_max_turns is None
    assert boot.history_max_bytes is None


def test_max_turns_raises_on_garbage_rather_than_degrading() -> None:
    """config.py:98 uses a bare int() today and DOES raise. Keep it raising."""
    env = _worker_env() | _SUBSTRATE_ENV | {"CURIE_MAX_TURNS": "not-a-number"}
    with pytest.raises((ValueError, ValidationError)):
        BootEnv.from_env(env)


def test_no_code_producer_owns_the_knobs() -> None:
    """The operator-owned keys: reachable only through ``runner.extraEnv``/``-e``.

    ``OTEL_EXPORTER_OTLP_HEADERS`` belongs here rather than with the substrate's
    OTel pair: the chart writes only the endpoint (agent-sandbox.yaml:433) and
    the protocol (435), and compose only the endpoint (compose.dev.yaml:461), so
    no code producer emits collector auth headers.
    """
    assert set(BootEnv.env_keys(producer="operator")) == {
        "CURIE_MAX_TURNS",
        "CURIE_HISTORY_MAX_TURNS",
        "CURIE_HISTORY_MAX_BYTES",
        "OTEL_EXPORTER_OTLP_HEADERS",
    }


# --- The declared key surface and its producer tags. -------------------------


def test_env_keys_declares_the_whole_flattened_boot_surface() -> None:
    """The exported key list is flattened: nested SessionConfig and OTel keys too.

    The chart's render-assert compares the runner container's env names against
    this list, and the template itself bakes CURIE_RUNNER_PORT
    (agent-sandbox.yaml:430) and the OTel keys (433/435). A non-flattened list
    would fail a default render.
    """
    assert set(BootEnv.env_keys()) == {
        # via session: SessionConfig
        "CURIE_PLUGIN_DIR",
        "CURIE_SESSION_ID",
        "CURIE_SANDBOX_ID",
        "CURIE_BUDGET",
        "CURIE_MEMORY_REF",
        "CURIE_CREDENTIALS",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_HEADERS",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        # platform-operational, declared on BootEnv itself
        "CURIE_BUNDLE_REF",
        "CURIE_RUNNER_TOKEN",
        "CURIE_MODEL",
        "CURIE_FAKE_MODEL",
        "CURIE_HISTORY_REF",
        "CURIE_HISTORY_TOKEN",
        "CURIE_MEMORY_TOKEN",
        "CURIE_STATE_URL",
        "CURIE_STATE_TOKEN",
        "CURIE_DELEGATE_URL",
        "CURIE_DELEGATE_TOKEN",
        "CURIE_APPROVAL_REQUIRED_TOOLS",
        "CURIE_APPROVAL_GRANT_TOOL",
        "CURIE_APPROVAL_RESUMED_KIND",
        "CURIE_APPROVAL_DECISION",
        "CURIE_CONNECTOR_SECRET_KEYS",
        "CURIE_CONNECTOR_RELEASE",
        "CURIE_CONNECTOR_AGENT",
        "CURIE_CONNECTOR_NAMESPACE",
        "CURIE_RUNNER_PORT",
        "ANTHROPIC_BASE_URL",
        "CURIE_MODEL_API_BACKEND",
        "CURIE_THINKING",
        "CURIE_MODEL_ENV_KEY",
        "CURIE_MAX_TURNS",
        "CURIE_HISTORY_MAX_TURNS",
        "CURIE_HISTORY_MAX_BYTES",
    }


@pytest.mark.parametrize("producer", [None, *_PRODUCERS])
def test_env_keys_is_sorted_and_carries_no_duplicates(producer: EnvProducer | None) -> None:
    """Sorted so the generated Rust const module cannot flap the drift gate.

    Held for the whole surface (``producer=None``) and for every per-producer
    slice, since each is rendered into the generated constants too.
    """
    keys = list(BootEnv.env_keys(producer=producer))
    assert keys == sorted(keys)
    assert len(keys) == len(set(keys))


def test_every_key_declares_at_least_one_producer() -> None:
    """Every declared key is owned. An untagged key has no home.

    Deliberately NOT "exactly one": a key may legitimately have several
    producers (see ANTHROPIC_BASE_URL below). Global single-ownership is
    contradicted by the tree, so asserting it would pin a fiction. The real
    safety invariant is per-key, and it lives in the two tests that follow.
    """
    tagged = {key for producer in _PRODUCERS for key in BootEnv.env_keys(producer=producer)}
    assert tagged == set(BootEnv.env_keys())
    for key in BootEnv.env_keys():
        assert _producers_of(key), f"{key} declares no producer"


def test_substrate_identity_keys_are_substrate_only_and_never_worker_written() -> None:
    """THE ANTI-CLOBBER INVARIANT, at the ownership level. Do not loosen.

    For these two keys the SUBSTRATE is authoritative: sandbox identity derives
    from the pod name via `fieldRef: metadata.name` (agent-sandbox.yaml:422-427)
    and the port is the container's own (agent-sandbox.yaml:430, docker.py:167).
    Because envVarsInjectionPolicy is `Overrides` (values.yaml:789), ANY worker
    write to these wins over the substrate's value -- which is the clobber bug,
    not a layering choice. So `worker` must never appear here.

    Contrast ANTHROPIC_BASE_URL below, where the worker IS authoritative and
    worker-wins is intended. The distinction is which side owns the truth, not
    how many producers a key has.
    """
    for key in ("CURIE_SANDBOX_ID", "CURIE_RUNNER_PORT"):
        assert _producers_of(key) == {"substrate"}, (
            f"{key} is substrate-authoritative; a non-substrate producer would clobber it "
            "under envVarsInjectionPolicy=Overrides"
        )
        assert "worker" not in _producers_of(key)


def test_anthropic_base_url_is_worker_and_substrate_with_the_worker_winning() -> None:
    """A legitimately multi-producer key. Do not "fix" this to one producer.

    The chart sets ANTHROPIC_BASE_URL (agent-sandbox.yaml:387, 404) as a
    fallback default (the LiteLLM sidecar branch, commit e38d149); the worker
    sets it per-agent for model routing (binding.py:475-476). Under
    envVarsInjectionPolicy=Overrides the worker's value wins, and that INTENDED
    layering is why the worker is the authoritative producer here -- unlike the
    substrate-identity keys above, where a worker write is the clobber bug.
    """
    assert _producers_of("ANTHROPIC_BASE_URL") == {"worker", "substrate"}


def test_the_substrate_writes_identity_otel_and_the_warm_pool_defaults() -> None:
    """Every key the chart's runner container writes, by authority class.

    This is an INVENTORY of what the substrate writes, not a claim about who
    wins. Membership here does not make a key substrate-authoritative: only the
    first group is, and that is guarded separately by
    ``test_substrate_identity_keys_are_substrate_only_and_never_worker_written``.
    """
    assert set(BootEnv.env_keys(producer="substrate")) == {
        # Substrate-authoritative: the substrate owns the truth and a worker
        # write would clobber it.
        "CURIE_SANDBOX_ID",
        "CURIE_RUNNER_PORT",
        "OTEL_EXPORTER_OTLP_ENDPOINT",  # agent-sandbox.yaml:433
        "OTEL_EXPORTER_OTLP_PROTOCOL",  # agent-sandbox.yaml:435
        # Worker-authoritative with a substrate fallback: the chart bakes a
        # warm-pool default into the runner container so an unclaimed pod boots
        # resolvable, and the worker's per-claim value legitimately wins under
        # envVarsInjectionPolicy=Overrides.
        "ANTHROPIC_BASE_URL",  # agent-sandbox.yaml:387, 404
        "CURIE_MODEL",  # agent-sandbox.yaml:389, 397
        "CURIE_FAKE_MODEL",  # agent-sandbox.yaml:381
        "CURIE_CREDENTIALS",  # agent-sandbox.yaml:410 (secretKeyRef)
        "CURIE_PLUGIN_DIR",  # agent-sandbox.yaml:416
        "CURIE_SESSION_ID",  # agent-sandbox.yaml:420 ("warm-unbound")
        "CURIE_BUDGET",  # agent-sandbox.yaml:428
    }


def test_the_nine_frozen_session_keys_are_declared_and_tagged() -> None:
    """Pins the companion map for the nested frozen keys against the real model.

    SessionConfig cannot carry per-field producer tags (it is frozen and out of
    scope), so its nine keys are tagged by a companion map. A map that drifts
    from what SessionConfig.to_env actually writes would silently mis-tag a key,
    so the expectation is derived from a fully-populated config's own render.
    """
    rendered = set(_full_config().to_env())
    # _full_config leaves OtelConfig.headers unset, so its key is the one of the
    # nine that a maximal SessionConfig render still omits.
    assert rendered | {"OTEL_EXPORTER_OTLP_HEADERS"} == {
        "CURIE_PLUGIN_DIR",
        "CURIE_SESSION_ID",
        "CURIE_SANDBOX_ID",
        "CURIE_BUDGET",
        "CURIE_MEMORY_REF",
        "CURIE_CREDENTIALS",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_HEADERS",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
    }
    assert rendered <= set(BootEnv.env_keys())
    tagged = {key for producer in _PRODUCERS for key in BootEnv.env_keys(producer=producer)}
    assert rendered <= tagged


def test_agent_id_is_not_a_declared_boot_key() -> None:
    """CURIE_AGENT_ID was written by the worker and read by nothing (#488 AC4)."""
    assert "CURIE_AGENT_ID" not in BootEnv.env_keys()


def test_every_rendered_key_is_a_declared_env_key() -> None:
    """No render surface can invent a key the export does not name.

    This is what keeps the chart assert honest: an env name the runner receives
    but the export never lists would slip past a subset check on a stale list.
    """
    assert set(_full_boot_env().to_env()) <= set(BootEnv.env_keys())
    assert set(_worker_env(fake_model=True, base_url="http://x")) <= set(BootEnv.env_keys())


def test_connector_scope_is_emitted_as_a_set_or_not_at_all() -> None:
    # The runner derives `<release>-<agent>-mcp-<connector>.<namespace>` from all
    # three or derives nothing. A partial scope would name a Service that cannot
    # exist, and the failure would be a connection refused at turn time.
    partial = _worker_env(connector_release="curie", connector_agent="acme-dev")
    assert not [k for k in partial if k.startswith("CURIE_CONNECTOR_")]

    full = _worker_env(
        connector_release="curie", connector_agent="acme-dev", connector_namespace="curie"
    )
    assert full["CURIE_CONNECTOR_RELEASE"] == "curie"
    assert full["CURIE_CONNECTOR_AGENT"] == "acme-dev"
    assert full["CURIE_CONNECTOR_NAMESPACE"] == "curie"


def test_connector_scope_survives_the_worker_render_and_the_consumer_parse() -> None:
    # Emitting the scope is only half the contract: the runner is the single
    # consumer, so a key the worker writes and `from_env` never reads is the
    # same outcome as never writing it. The agent loses its connector tools and
    # nothing errors (#1195).
    #
    # Three DISTINCT values, because the runner composes
    # `<release>-<agent>-mcp-<connector>.<namespace>`: a parse that reads one
    # env key into two fields, or drops one of the three, must not be able to
    # pass here.
    boot = BootEnv.from_env(
        _worker_env(
            connector_release="curie",
            connector_agent="acme-dev",
            connector_namespace="curie-prod",
        )
        | _SUBSTRATE_ENV
    )
    assert boot.connector_release == "curie"
    assert boot.connector_agent == "acme-dev"
    assert boot.connector_namespace == "curie-prod"


def test_connector_scope_is_absent_by_default() -> None:
    # Absent is meaningful, not degraded: the skill tier hosts nothing, so there
    # is no Service to derive a URL from.
    assert not [k for k in _worker_env() if k.startswith("CURIE_CONNECTOR_")]
