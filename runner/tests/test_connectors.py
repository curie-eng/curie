"""Mounting declared connectors into the agent's MCP configuration (#1118).

The property under test is that the author never writes the URL, and that the
URL the agent dials is the one the Service actually has.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import BootEnv, Budget
from curie_runner import RunnerConfig
from curie_runner import __main__ as boot
from curie_runner.__main__ import build_runner
from curie_runner.approval import APPROVAL_SERVER_NAME
from curie_runner.connectors import build_mcp_servers, derive_mcp_servers
from curie_runner.mcp_tool_capability import McpToolCapabilityProbe
from curie_runner.plugin import PluginBundleError
from curie_runner.state import STATE_SERVER_NAME
from plugin_format.connectors import RESERVED_CONNECTOR_NAMES

HOSTED = "connectors:\n  grafana:\n    image: grafana/mcp-grafana:0.17.2\n    secrets: [T]\n"
REMOTE = "connectors:\n  internal:\n    url: https://mcp.internal/mcp\n"

SCOPE = {"release": "curie", "agent": "acme-dev", "namespace": "curie"}


class _CapturedSession:
    def __init__(self, options: Any) -> None:
        self.options = options

    async def connect(self) -> None:
        return None

    async def query(self, _text: str) -> None:
        return None

    async def interrupt(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def receive_turn(self):
        if False:
            yield None


def _boot_options(
    monkeypatch: pytest.MonkeyPatch,
    config: RunnerConfig,
    *,
    potential_write: bool,
    observed_tools: frozenset[str] | None = None,
    readonly_tools: frozenset[str] | None = None,
) -> Any:
    async def probe(*_args: Any, **_kwargs: Any) -> McpToolCapabilityProbe:
        observed = observed_tools or frozenset()
        return McpToolCapabilityProbe(
            complete=True,
            has_potential_write_tool=potential_write,
            tool_count=len(observed) or (1 if potential_write else 0),
            observed_tools=observed,
            readonly_tools=readonly_tools or frozenset(),
        )

    monkeypatch.setattr(boot, "probe_mcp_tool_capability", probe)
    monkeypatch.setattr(boot, "ClaudeAgentSession", _CapturedSession)
    session = build_runner(config, fake_model=False)._factory()
    assert isinstance(session, _CapturedSession)
    return session.options


def _bundle(root: Path, connectors: str | None = None, mcp: dict | None = None) -> Path:
    (root / ".claude-plugin").mkdir(parents=True, exist_ok=True)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "b", "version": "0.1.0", "description": "t"}), encoding="utf-8"
    )
    if connectors is not None:
        (root / "connectors.yaml").write_text(connectors, encoding="utf-8")
    if mcp is not None:
        (root / ".mcp.json").write_text(json.dumps(mcp), encoding="utf-8")
    return root


def test_the_author_never_writes_the_url(tmp_path: Path) -> None:
    # The whole point of ADR-0086. The URL embeds the release, the agent, and
    # the namespace -- all assigned at install and deploy, none knowable to
    # whoever wrote the bundle.
    servers = derive_mcp_servers(_bundle(tmp_path, HOSTED), **SCOPE)
    assert servers["grafana"]["url"] == (
        "http://curie-acme-dev-mcp-grafana.curie.svc.cluster.local:8000/mcp"
    )


def test_two_agents_from_one_bundle_dial_different_servers(tmp_path: Path) -> None:
    # Since #1116 the Service is agent-scoped, so a hand-written URL is
    # guaranteed wrong for at least one of two agents sharing a bundle. Deriving
    # per-boot is what makes one bundle serve both.
    root = _bundle(tmp_path, HOSTED)
    dev = derive_mcp_servers(root, release="curie", agent="acme-dev", namespace="curie")
    prod = derive_mcp_servers(root, release="curie", agent="sre-prod", namespace="curie")
    assert dev["grafana"]["url"] != prod["grafana"]["url"]


def test_no_connectors_file_mounts_nothing(tmp_path: Path) -> None:
    assert derive_mcp_servers(_bundle(tmp_path), **SCOPE) == {}


def test_hosted_connector_without_a_scope_mounts_nothing(tmp_path: Path) -> None:
    # The skill tier hosts nothing, so there is no Service to point at. Mounting
    # a URL that resolves nowhere would turn "not available here" into a
    # connection refused mid-turn.
    servers = derive_mcp_servers(
        _bundle(tmp_path, HOSTED), release=None, agent=None, namespace=None
    )
    assert servers == {}


def test_a_remote_connector_needs_no_scope(tmp_path: Path) -> None:
    # It carries its own absolute url, so it is exercisable in every tier --
    # including the ones that host nothing.
    servers = derive_mcp_servers(
        _bundle(tmp_path, REMOTE), release=None, agent=None, namespace=None
    )
    assert servers["internal"]["url"] == "https://mcp.internal/mcp"


def test_remote_and_hosted_together_mount_only_what_this_tier_can_reach(tmp_path: Path) -> None:
    root = _bundle(tmp_path, HOSTED + REMOTE.replace("connectors:\n", ""))
    both = derive_mcp_servers(root, **SCOPE)
    assert set(both) == {"grafana", "internal"}
    tierless = derive_mcp_servers(root, release=None, agent=None, namespace=None)
    assert set(tierless) == {"internal"}


def test_unreadable_connectors_file_mounts_nothing_rather_than_crashing(tmp_path: Path) -> None:
    # Deploy already validated this, so reaching here means the bundle changed
    # underneath us. Losing the connector's tools is visible; losing the whole
    # session to a boot crash is worse.
    servers = derive_mcp_servers(
        _bundle(tmp_path, "connectors:\n  g:\n   image: [unclosed\n"), **SCOPE
    )
    assert servers == {}


def test_non_utf8_connectors_file_mounts_nothing_rather_than_crashing(tmp_path: Path) -> None:
    root = _bundle(tmp_path)
    (root / "connectors.yaml").write_bytes(b"\x80")
    assert derive_mcp_servers(root, **SCOPE) == {}


def test_explicit_mapping_tag_on_a_scalar_mounts_nothing(tmp_path: Path) -> None:
    servers = derive_mcp_servers(
        _bundle(tmp_path, "connectors: !!map foo\n"), **SCOPE
    )
    assert servers == {}


def test_duplicate_connector_name_mounts_nothing(tmp_path: Path) -> None:
    servers = derive_mcp_servers(
        _bundle(
            tmp_path,
            "connectors:\n"
            "  grafana:\n"
            "    url: https://first.example.com/mcp\n"
            "  grafana:\n"
            "    url: https://second.example.com/mcp\n",
        ),
        **SCOPE,
    )
    assert servers == {}, "duplicate grafana must mount nothing"


def test_invalid_connectors_file_mounts_nothing(tmp_path: Path) -> None:
    servers = derive_mcp_servers(
        _bundle(tmp_path, "connectors:\n  Bad_Name:\n    image: x:1\n"), **SCOPE
    )
    assert servers == {}


def test_no_plugin_dir_is_not_an_error(tmp_path: Path) -> None:
    assert derive_mcp_servers(None, **SCOPE) == {}


# --------------------------------------------------------------------------- #
# Reaching a hosted connector on a tier that cannot host it -- #1160
# --------------------------------------------------------------------------- #
HOSTED_WITH_FALLBACK = (
    "connectors:\n"
    "  grafana:\n"
    "    image: grafana/mcp-grafana:0.17.2\n"
    "    unhosted_url: http://host.docker.internal:8765/mcp\n"
)


def test_a_fallback_makes_a_hosted_connector_reachable_at_the_skill_tier(tmp_path: Path) -> None:
    # The skill tier hosts nothing, but the developer has mcp-grafana running.
    # Without this the eval lane silently loses its connector.
    servers = derive_mcp_servers(
        _bundle(tmp_path, HOSTED_WITH_FALLBACK), release=None, agent=None, namespace=None
    )
    assert servers["grafana"]["url"] == "http://host.docker.internal:8765/mcp"


def test_the_cluster_still_uses_the_service_it_created(tmp_path: Path) -> None:
    # A fallback that won everywhere would repoint a production agent at
    # someone's laptop. The derived URL must win wherever Curie hosts.
    servers = derive_mcp_servers(_bundle(tmp_path, HOSTED_WITH_FALLBACK), **SCOPE)
    assert "svc.cluster.local" in servers["grafana"]["url"]
    assert "8765" not in servers["grafana"]["url"]


def test_a_hosted_connector_without_a_fallback_still_mounts_nothing(tmp_path: Path) -> None:
    servers = derive_mcp_servers(
        _bundle(tmp_path, HOSTED), release=None, agent=None, namespace=None
    )
    assert servers == {}


# --------------------------------------------------------------------------- #
# The whole chain, worker render to mounted server -- #1195
#
# Every test above hands derive_mcp_servers a scope directly, which is the one
# thing a real boot never does. The scope has to survive BootEnv.render_worker,
# BootEnv.from_env, and RunnerConfig before it reaches this function, and a
# break anywhere along that path is invisible: the scope arrives as None, the
# hosted connector is treated as "not exercisable in this tier", and the agent
# boots without its tools while nothing errors.
# --------------------------------------------------------------------------- #

# What the chart/docker substrate contributes; no worker producer writes these.
_SUBSTRATE_ENV = {"CURIE_SANDBOX_ID": "curie-sandbox-abc123", "CURIE_RUNNER_PORT": "8080"}


def _config_for(
    plugin_dir: Path,
    *,
    release: str | None = None,
    agent: str | None = None,
    namespace: str | None = None,
    approval_grant_tool: str | None = None,
) -> RunnerConfig:
    """A RunnerConfig built the way a real boot builds one.

    Through the real producer and the real consumer parse, never by
    constructing the dataclass: a hand-built config would assert on this test's
    own idea of the boot env instead of on the one the worker actually renders.
    """

    env = (
        BootEnv.render_worker(
            plugin_dir=str(plugin_dir),
            session_id="agent-abc-thread-1",
            budget=Budget(max_output_tokens_per_run=4096, max_usd_per_day=5.0),
            memory_ref="http://api:8000/agents/agent-abc/state/memory",
            history_ref="http://api:8000/agents/agent-abc/state/transcript/t1",
            connector_release=release,
            connector_agent=agent,
            connector_namespace=namespace,
        )
        | _SUBSTRATE_ENV
    )
    if approval_grant_tool is not None:
        env[BootEnv.env_key("approval_grant_tool")] = approval_grant_tool
    return RunnerConfig.from_env(env)


def _approval_state(gate: Any) -> tuple[Any, ...]:
    """Every mutable pending/grant value catalog projection must leave alone."""

    return (
        gate.pending_summary,
        gate.pending_route,
        gate.pending_gate_kind,
        gate.pending_granted_tool,
        gate.policy_requested,
        gate.policy_rejected,
        gate.policy_route,
        gate.grant_tool,
        tuple(sorted(gate.grantable_by_route.items())),
        gate.publication_title,
        gate.publication_body,
        gate.pending_halt,
        gate._boot_turn_seen,  # noqa: SLF001 - projection mutation is the assertion
    )


def test_boot_threads_only_policy_hidden_observations_without_spending_gate_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _bundle(
        tmp_path,
        mcp={"mcpServers": {"operations": {"command": "fixture-command"}}},
    )
    manifest_path = root / ".claude-plugin" / "plugin.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["toolPolicy"] = {
        "enforcement": "curie/mcp-tool-policy@1",
        "allow": ["operations/read_allowed"],
        "approvalRequired": ["operations/write_approval"],
        "deny": ["operations/write_denied"],
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    prefix = "mcp__plugin_b_operations__"
    read_allowed = f"{prefix}read_allowed"
    write_approval = f"{prefix}write_approval"
    write_denied = f"{prefix}write_denied"
    write_unmatched = f"{prefix}write_unmatched"
    observed = frozenset(
        {read_allowed, write_approval, write_denied, write_unmatched}
    )

    real_projection = boot.policy_disallowed_tools
    projection_states: list[tuple[tuple[Any, ...], tuple[Any, ...]]] = []

    def observe_projection(gate: Any, tools: frozenset[str]):
        before = _approval_state(gate)
        hidden = tuple(real_projection(gate, tools))
        after = _approval_state(gate)
        projection_states.append((before, after))
        return hidden

    monkeypatch.setattr(boot, "policy_disallowed_tools", observe_projection)
    options = _boot_options(
        monkeypatch,
        _config_for(root, approval_grant_tool=write_approval),
        potential_write=True,
        observed_tools=observed,
        readonly_tools=frozenset({read_allowed}),
    )

    assert set(options.disallowed_tools) == {write_denied, write_unmatched}
    assert len(options.disallowed_tools) == 2
    assert write_approval not in options.disallowed_tools
    assert projection_states
    assert all(before == after for before, after in projection_states)
    assert projection_states[0][0][7] == write_approval


def test_boot_with_no_policy_adds_no_observed_mcp_names_to_disallowed_tools(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _bundle(
        tmp_path,
        mcp={"mcpServers": {"operations": {"command": "fixture-command"}}},
    )
    observed = frozenset(
        {
            "mcp__plugin_b_operations__write_denied",
            "mcp__plugin_b_operations__write_unmatched",
        }
    )

    options = _boot_options(
        monkeypatch,
        _config_for(root),
        potential_write=True,
        observed_tools=observed,
    )

    assert options.disallowed_tools == []


def test_a_scope_rendered_by_the_worker_reaches_the_mounted_connector(tmp_path: Path) -> None:
    # The acceptance criterion for #1195: a cluster boot that declares a hosted
    # connector must end up dialing the Service Curie created for it.
    config = _config_for(
        _bundle(tmp_path, HOSTED), release="curie", agent="acme-dev", namespace="curie-prod"
    )
    servers = derive_mcp_servers(
        config.session.plugin_dir,
        release=config.connector_release,
        agent=config.connector_agent,
        namespace=config.connector_namespace,
    )
    assert servers["grafana"]["url"] == (
        "http://curie-acme-dev-mcp-grafana.curie-prod.svc.cluster.local:8000/mcp"
    )


def test_a_boot_that_renders_no_scope_still_mounts_no_hosted_connector(tmp_path: Path) -> None:
    # The other half. "Declared but not exercisable in this tier" (#1093) is the
    # honest answer when the worker sends no scope, so the fix must reach the
    # scope through the boot env and must never invent one.
    config = _config_for(_bundle(tmp_path, HOSTED))
    servers = derive_mcp_servers(
        config.session.plugin_dir,
        release=config.connector_release,
        agent=config.connector_agent,
        namespace=config.connector_namespace,
    )
    assert servers == {}


# --------------------------------------------------------------------------- #
# A build-form connector reaches the runner identically to an image one
# (#1690, ADR 0113 Stream B2)
#
# The runner never resolves a build: `_read` parses connectors.yaml as shipped
# in the bundle, and `mcp_entry` derives the URL from `is_hosted` and the
# Service DNS naming convention alone -- it never reads `.image` (Section 3,
# `connector_render.py:450`: "no code change ... skill, local and cluster
# produce byte-identical entries"). `is_hosted` is True for a `build:`
# connector whether or not its lock has been applied (`connectors.py:245`), so
# these must already pass with no runner change; the digest resolution these
# pin against lives entirely in `apply_lock`, upstream of the runner.
# --------------------------------------------------------------------------- #
BUILT = (
    "connectors:\n"
    "  conn:\n"
    "    build:\n"
    "      context: connectors/conn\n"
    "      platforms: [linux/amd64]\n"
)
IMAGED = "connectors:\n  conn:\n    image: acme/conn-mcp:1.0\n"


def test_a_build_form_connector_yields_the_same_entry_as_an_image_one(tmp_path: Path) -> None:
    built = derive_mcp_servers(_bundle(tmp_path / "built", BUILT), **SCOPE)
    imaged = derive_mcp_servers(_bundle(tmp_path / "imaged", IMAGED), **SCOPE)
    assert built == imaged
    assert built["conn"]["url"] == (
        "http://curie-acme-dev-mcp-conn.curie.svc.cluster.local:8000/mcp"
    )


def test_a_build_form_connector_with_no_lock_applied_is_still_hosted(tmp_path: Path) -> None:
    # A `build:` connector with no lock applied is hosted-and-unrenderable at
    # the cluster's render step, but the runner's own scope check must not
    # treat it as unhosted: an unresolved build with a scope present still
    # mounts, exactly like an image connector does.
    servers = derive_mcp_servers(_bundle(tmp_path, BUILT), **SCOPE)
    assert "conn" in servers


# --------------------------------------------------------------------------- #
# The #1093 log: fires for a stranded hosted connector, silent once scoped
# --------------------------------------------------------------------------- #
def test_the_1093_log_fires_for_a_scope_less_hosted_connector(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    # "declared but not exercisable in this tier" must still fire when neither
    # a connector scope nor an unhosted_url reaches the bundle -- the honest
    # answer at the skill tier, and the regression this build-form addition
    # must not silently swallow.
    with caplog.at_level(logging.INFO, logger="curie_runner.connectors"):
        derive_mcp_servers(_bundle(tmp_path, HOSTED), release=None, agent=None, namespace=None)
    assert any(
        "declared but not exercisable in this tier" in r.message
        for r in caplog.records
    )


def test_the_1093_log_fires_for_a_scope_less_build_form_connector(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.INFO, logger="curie_runner.connectors"):
        derive_mcp_servers(_bundle(tmp_path, BUILT), release=None, agent=None, namespace=None)
    assert any(
        "declared but not exercisable in this tier" in r.message
        for r in caplog.records
    )


def test_the_1093_log_does_not_fire_once_a_scope_reaches_the_connector(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    # The scope this ticket makes the local tier able to supply must silence
    # the log, exactly as it already does on cluster: a connector that is
    # exercisable here is not "declared but not exercisable" here.
    with caplog.at_level(logging.INFO, logger="curie_runner.connectors"):
        derive_mcp_servers(_bundle(tmp_path, HOSTED), **SCOPE)
    assert not any(
        "declared but not exercisable in this tier" in r.message
        for r in caplog.records
    )


# --------------------------------------------------------------------------- #
# A connector name colliding with a platform server -- #1200
# --------------------------------------------------------------------------- #
BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'


def _boot_env(monkeypatch, tmp_path: Path, suffix: str) -> dict[str, str]:
    monkeypatch.setenv("CURIE_STATE_URL", "http://state.invalid/agents/a/state")
    monkeypatch.setenv("CURIE_STATE_TOKEN", "t")
    return {
        "CURIE_PLUGIN_DIR": str(_bundle(tmp_path)),
        "CURIE_SESSION_ID": f"s-{suffix}",
        "CURIE_SANDBOX_ID": f"b-{suffix}",
        "CURIE_BUDGET": BUDGET,
    }


def test_the_platform_approval_server_survives_a_colliding_connector_name() -> None:
    # Both maps are plain dict keys on one channel, so somebody wins. Losing a
    # connector is visible and diagnosable; losing request_approval is silent --
    # a skill that calls it fails, or a self-imposed approval never raises. The
    # merge therefore fails safe toward the platform.
    approval = {"platform": "approval"}
    state = {"platform": "state"}
    servers = build_mcp_servers(
        platform={APPROVAL_SERVER_NAME: approval, STATE_SERVER_NAME: state},
        derived={
            "curie": {"type": "http", "url": "http://impostor/mcp"},
            "curie-state": {"type": "http", "url": "http://impostor/state/mcp"},
            "grafana": {"type": "http", "url": "http://grafana/mcp"},
        },
    )
    assert servers[APPROVAL_SERVER_NAME] is approval
    assert servers[STATE_SERVER_NAME] is state
    assert servers["grafana"]["url"] == "http://grafana/mcp"


def test_a_declared_connector_still_mounts_alongside_the_platform_servers() -> None:
    # The control on the same seam: safety must not be bought by dropping the
    # connectors the bundle declared.
    grafana = {"type": "http", "url": "http://grafana/mcp"}
    servers = build_mcp_servers(
        platform={APPROVAL_SERVER_NAME: {"platform": "approval"}},
        derived={"grafana": grafana},
    )
    assert servers["grafana"] is grafana
    assert APPROVAL_SERVER_NAME in servers


def test_every_platform_mcp_server_the_boot_path_mounts_is_reserved(tmp_path, monkeypatch) -> None:
    # Every platform MCP server must be MOUNTED here, not merely importable.
    # This pin reads the servers this boot actually mounted, so a
    # conditionally-mounted platform server -- like curie-state, which mounts
    # only when CURIE_STATE_URL is set -- is invisible to the pin unless its
    # mounting env is set in _boot_env. If you add a platform server behind its
    # own condition, set that condition in _boot_env too, or the pin goes blind
    # to it and the reserved list silently stops being complete.
    env = _boot_env(monkeypatch, tmp_path, "pin")
    # fake_model=False: the fake branch never builds mcp_servers at all, so a
    # fake boot would pin nothing.
    mounted = _boot_options(
        monkeypatch,
        RunnerConfig.from_env(env),
        potential_write=True,
    ).mcp_servers

    # The bundle declares no connectors, so every key is a platform key by
    # construction. A third platform server added later reddens this until it
    # is reserved -- which is why the fence is two exact names and not the
    # `curie-` prefix. The write-capable probe is load-bearing: it keeps the pin
    # able to see the conditionally mounted approval server.
    assert set(mounted) == {APPROVAL_SERVER_NAME, STATE_SERVER_NAME}
    assert set(mounted) <= RESERVED_CONNECTOR_NAMES


def test_the_boot_path_mounts_the_platform_approval_server_over_a_colliding_connector(
    tmp_path, monkeypatch
) -> None:
    # The pin on the WIRING, not on the helper. build_mcp_servers can stay
    # perfectly correct while the boot path stops routing through it -- that is
    # exactly the pre-#1200 defect, an inline literal spreading the derived map
    # last -- and every other test here stays green, because the boot-path pin
    # above uses a zero-connector bundle whose set comparison is order-blind.
    #
    # The colliding input is unconstructible through a real bundle by
    # construction: this branch's own validator rejects a connector named
    # `curie`, so connectors.py:_read logs and returns {} before any merge
    # happens. So the collision has to be INJECTED, and the injection point is
    # derive_mcp_servers -- the UPSTREAM INPUT of the closure, at the boundary,
    # not the thing being asserted. What is under test is the precedence wiring
    # inside build_runner's factory; that runs for real. The plan rejected
    # monkeypatching derive_mcp_servers as an ALTERNATIVE to extracting
    # build_mcp_servers; this is not that. Do not delete this as a mock.
    impostor = {"type": "http", "url": "http://impostor/mcp"}
    grafana = {"type": "http", "url": "http://grafana/mcp"}
    monkeypatch.setattr(
        "curie_runner.__main__.derive_mcp_servers",
        lambda *a, **k: {APPROVAL_SERVER_NAME: impostor, "grafana": grafana},
    )
    env = _boot_env(monkeypatch, tmp_path, "collide")
    mounted = _boot_options(
        monkeypatch,
        RunnerConfig.from_env(env),
        potential_write=True,
    ).mcp_servers

    assert mounted[APPROVAL_SERVER_NAME] is not impostor
    # Positive identification, not merely "not the impostor": the platform's
    # approval server is the in-process SDK server, which no connector can be.
    assert mounted[APPROVAL_SERVER_NAME]["type"] == "sdk"
    # The control on the boot path: the platform winning must not cost the
    # bundle the connectors it declared.
    assert mounted["grafana"] is grafana


# --------------------------------------------------------------------------- #
# A third-party server in the bundle's own `.mcp.json` stays external
# (#1690, ADR 0113)
#
# Hosting is a `connectors.yaml` decision and only a `connectors.yaml` decision:
# `_read` opens that one file, and the entries it derives are the only ones
# Curie ever creates a Service and Deployment for. A bundle's own `.mcp.json` is
# a different channel entirely -- it rides `ClaudeAgentOptions.plugins` as the
# read-only bundle directory the SDK loads for itself, never through
# `build_mcp_servers`, which merges only the platform's servers with the
# connectors.yaml-derived ones. So a remote upstream declared there is dialed at
# the address its author wrote, and no container of Curie's is started for it.
#
# The tests below are the pin on that separation. THE RED: make
# `derive_mcp_servers` enumerate `.mcp.json` -- the plausible regression, since
# both files name MCP servers -- and the exact-set assertions
# (`set(servers) == {"conn"}` here, `set(mounted) == {APPROVAL_SERVER_NAME,
# "grafana"}` on the boot path) go red on the extra key. Make it REWRITE the
# upstream entry to a Service DNS URL and the byte-identity assertion on
# `.mcp.json` plus the "no derived entry carries the upstream host" assertion go
# red as well.
# --------------------------------------------------------------------------- #
UPSTREAM = "https://mcp.example-upstream.com/mcp"
UPSTREAM_MCP_JSON = {"mcpServers": {"github-upstream": {"type": "http", "url": UPSTREAM}}}


def test_a_third_party_mcp_json_server_is_not_a_connector_curie_hosts(tmp_path: Path) -> None:
    # One bundle, both channels: a `build:` connector Curie hosts, and a remote
    # upstream the author reaches directly. Only the first is Curie's to create
    # a Service for, so only the first may appear in the derived entries -- the
    # upstream is not there, not renamed, and not rewritten to Service DNS.
    servers = derive_mcp_servers(_bundle(tmp_path, BUILT, mcp=UPSTREAM_MCP_JSON), **SCOPE)
    assert set(servers) == {"conn"}
    assert not any(UPSTREAM in entry.get("url", "") for entry in servers.values())
    # The control on the same bundle: the connector Curie DOES host still gets
    # the Service it created, so "the upstream is absent" is not bought by
    # deriving nothing at all.
    assert "svc.cluster.local" in servers["conn"]["url"]


def test_mounting_connectors_never_rewrites_the_bundles_own_mcp_json(tmp_path: Path) -> None:
    # "Nothing rewrites `.mcp.json`; the bundle stays the read-only artifact that
    # was deployed" (connectors.py). Byte identity is the check, because an
    # injected entry or a rewritten URL would both be a silent edit to a signed,
    # deployed artifact.
    root = _bundle(tmp_path, BUILT, mcp=UPSTREAM_MCP_JSON)
    before = (root / ".mcp.json").read_bytes()
    build_mcp_servers(
        platform={APPROVAL_SERVER_NAME: {"platform": "approval"}},
        derived=derive_mcp_servers(root, **SCOPE),
    )
    assert (root / ".mcp.json").read_bytes() == before
    assert json.loads(before)["mcpServers"]["github-upstream"]["url"] == UPSTREAM


def test_the_boot_path_mounts_nothing_for_a_third_party_mcp_json_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The pin on the WIRING rather than the helper, through a boot env the
    # worker really renders. The bundle's own upstream must reach the SDK on the
    # plugins channel -- the read-only bundle directory, loaded verbatim -- and
    # must never appear on the platform `mcp_servers` channel, which is where
    # the servers Curie hosts and derives URLs for ride.
    monkeypatch.delenv("CURIE_STATE_URL", raising=False)
    root = _bundle(tmp_path, HOSTED, mcp=UPSTREAM_MCP_JSON)
    config = _config_for(root, release="curie", agent="acme-dev", namespace="curie")
    options = _boot_options(monkeypatch, config, potential_write=True)

    mounted = options.mcp_servers
    # Exact, not `"github-upstream" not in mounted`: an extra key of any name is
    # a server Curie would be hosting that the bundle never declared to it.
    assert set(mounted) == {APPROVAL_SERVER_NAME, "grafana"}
    assert "svc.cluster.local" in mounted["grafana"]["url"]

    # The other half of "stays external": it is not dropped either. The SDK gets
    # the bundle directory itself, and the upstream entry in it is untouched.
    assert options.plugins == [{"type": "local", "path": str(root)}]
    loaded = json.loads((Path(options.plugins[0]["path"]) / ".mcp.json").read_text())
    assert loaded["mcpServers"]["github-upstream"] == {"type": "http", "url": UPSTREAM}


def test_a_name_in_both_channels_fails_the_boot_instead_of_picking_a_winner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The collision precedence, pinned where the code actually decides it: there
    # is none. `_reject_connector_name_collisions` refuses the bundle ("one
    # name, one owner"), and the runner runs that same validator through
    # load_plugins at boot, so the bundle never loads. A residual collision
    # therefore cannot reach build_mcp_servers, whose platform-wins rule is a
    # separate fence over Curie's OWN server names (#1200), not over this one.
    monkeypatch.delenv("CURIE_STATE_URL", raising=False)
    collide = {"mcpServers": {"grafana": {"type": "http", "url": UPSTREAM}}}
    config = _config_for(
        _bundle(tmp_path, HOSTED, mcp=collide),
        release="curie",
        agent="acme-dev",
        namespace="curie",
    )
    with pytest.raises(PluginBundleError) as exc:
        build_runner(config, fake_model=False)
    assert "connectors.duplicate_server" in str(exc.value)
    assert "grafana" in str(exc.value)


def test_the_reserved_list_matches_the_runner_constants() -> None:
    # plugin_format re-enumerates these names because runner depends on it and
    # never the reverse. This is the pin that keeps the copy honest: rename
    # either constant here and the deploy-time guard stops fencing it.
    assert RESERVED_CONNECTOR_NAMES == {APPROVAL_SERVER_NAME, STATE_SERVER_NAME}


# --------------------------------------------------------------------------- #
# An agent name that forges the object-name join -- #1446
#
# `mcp_entry` derives the URL through `object_name`, which builds
# `{release}-{agent}-mcp-{connector}`. Since #1446 that function fails closed on
# an agent name that would forge a SECOND `-mcp-` (it contains the delimiter, or
# it ends in `-mcp`), because two different `(agent, connector)` pairs would
# otherwise render one Service, one Deployment, both NetworkPolicies and one
# `app.kubernetes.io/name` pod selector -- and the connector is unauthenticated
# by design (ADR-0086), so that name is the only thing binding a sandbox to a
# credential.
#
# The connector-name half of that rule is already fail-soft here: `_read` runs
# `validate_connectors` and returns None on any error, so a forging CONNECTOR
# name logs and mounts nothing. The AGENT name has no such path -- it arrives on
# the boot env, is never validated in this module, and goes straight into the
# `mcp_entry` comprehension in `derive_mcp_servers`. An uncaught raise there
# contradicts this module's own documented contract (see `_read`): "Log and
# mount nothing rather than fail the turn ... a crashed boot loses the whole
# session." Losing the connector's tools is visible in the agent's tool list and
# recoverable by renaming the agent; losing the boot is the entire session, for
# every turn, until someone reads a stack trace out of a sandbox pod's logs.
# --------------------------------------------------------------------------- #
FORGING_AGENT = "a-mcp-b"


def test_a_forging_agent_name_mounts_nothing_rather_than_crashing_the_boot(
    tmp_path: Path, caplog
) -> None:
    root = _bundle(tmp_path, HOSTED)

    with caplog.at_level(logging.WARNING, logger="curie_runner.connectors"):
        servers = derive_mcp_servers(
            root, release="curie", agent=FORGING_AGENT, namespace="curie"
        )

    assert servers == {}, f"a forging agent name must mount nothing, got {servers}"
    # Silence would be worse than the crash it replaces: the agent boots, its
    # tool list is quietly short one connector, and every tool call fails as
    # "no such tool" with nothing anywhere naming the cause. The warning must
    # carry the AGENT NAME, because that is the single thing an operator has to
    # change and it is assigned at deploy time, not written in the bundle.
    assert any(
        FORGING_AGENT in record.getMessage() for record in caplog.records
    ), caplog.text

    # The control, in the same test on purpose: if `_bundle`, `HOSTED`, or
    # `derive_mcp_servers` broke for any reason unrelated to #1446, the
    # assertion above would pass vacuously on an empty dict. Same bundle, same
    # release, same namespace -- only the agent name differs.
    assert derive_mcp_servers(root, **SCOPE)["grafana"]["url"] == (
        "http://curie-acme-dev-mcp-grafana.curie.svc.cluster.local:8000/mcp"
    )


def test_an_agent_name_that_only_looks_like_the_join_still_mounts(tmp_path: Path) -> None:
    # A LEADING `mcp-` on the agent side is unambiguous -- the only alternative
    # split of `curie-mcp-x-mcp-grafana` leaves an empty agent -- so it must
    # keep working. This is the test that reddens if someone "simplifies" the
    # rule to a bare `'-mcp-' in name` substring ban or a
    # `base.count('-mcp-') == 1` guard: both over-reject, and an over-rejecting
    # rule silently strips a live agent of its connectors for no security gain.
    servers = derive_mcp_servers(
        _bundle(tmp_path, HOSTED), release="curie", agent="mcp-x", namespace="curie"
    )
    assert servers["grafana"]["url"] == (
        "http://curie-mcp-x-mcp-grafana.curie.svc.cluster.local:8000/mcp"
    )


# --------------------------------------------------------------------------- #
# What the sandbox actually mounts for an authenticated hosted server -- #2503
#
# github-mcp-server's HTTP transport authenticates the client: GitHub's docs
# require `Authorization: Bearer <PAT>` on every request, and without it the
# tool-capability probe logs `MCP tool-capability probe failed` and the agent
# lists no `mcp__github__*` tool at all. This file is the one that pins what
# the runner hands the MCP client, so the header has to be provable here and
# not only in plugin-format.
# --------------------------------------------------------------------------- #
GITHUB = (
    "connectors:\n"
    "  github:\n"
    "    image: ghcr.io/github/github-mcp-server:v0.20.1\n"
    "    secrets: [GITHUB_PERSONAL_ACCESS_TOKEN]\n"
)

_BEARER = {"Authorization": "Bearer ${GITHUB_PERSONAL_ACCESS_TOKEN}"}


def test_the_mounted_hosted_server_carries_the_declared_credential(tmp_path: Path) -> None:
    # The credential name is the only thing the author writes; the header, like
    # the URL, is derived. `${VAR}` is expanded by the MCP client from the
    # sandbox environment, so nothing resolved is written to disk here.
    servers = derive_mcp_servers(_bundle(tmp_path, GITHUB), **SCOPE)
    assert servers["github"]["headers"] == _BEARER


def test_materialize_expands_the_bearer_and_drops_it_from_env() -> None:
    # #2559: the derived catalog keeps the placeholder (derive_mcp_servers
    # above); the runner expands in memory and unsets the name so Bash cannot
    # read the PAT. The value must not appear in os.environ or the SDK spawn
    # env after this call, and must not be logged.
    from aci_protocol import BootEnv
    from curie_runner.connectors import materialize_hosted_bearer_headers

    marker = BootEnv.env_key("connector_secret_keys")
    servers = {
        "github": {
            "type": "http",
            "url": "http://example.svc/mcp",
            "headers": {"Authorization": "Bearer ${GITHUB_PERSONAL_ACCESS_TOKEN}"},
        }
    }
    env = {
        "GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_sentinel",
        "STDIO_ONLY": "keep-me",
        marker: "GITHUB_PERSONAL_ACCESS_TOKEN,STDIO_ONLY",
    }
    dropped = materialize_hosted_bearer_headers(servers, env)
    assert dropped == frozenset({"GITHUB_PERSONAL_ACCESS_TOKEN"})
    assert servers["github"]["headers"]["Authorization"] == "Bearer ghp_sentinel"
    assert "GITHUB_PERSONAL_ACCESS_TOKEN" not in env
    assert env["STDIO_ONLY"] == "keep-me"
    assert env[marker] == "STDIO_ONLY"


def test_materialize_leaves_a_missing_bearer_as_the_placeholder() -> None:
    # A SecretRef / unset value still expands empty today (#2519). Dropping a
    # name that was never in env would hide that gap; leave the placeholder.
    from curie_runner.connectors import materialize_hosted_bearer_headers

    servers = {
        "github": {
            "type": "http",
            "headers": {"Authorization": "Bearer ${GITHUB_PERSONAL_ACCESS_TOKEN}"},
        }
    }
    env = {"STDIO_ONLY": "keep-me"}
    dropped = materialize_hosted_bearer_headers(servers, env)
    assert dropped == frozenset()
    assert servers["github"]["headers"] == {
        "Authorization": "Bearer ${GITHUB_PERSONAL_ACCESS_TOKEN}"
    }
    assert env == {"STDIO_ONLY": "keep-me"}


def test_materialize_does_not_drop_an_unrelated_secret() -> None:
    # ADR-0009 stdio / remote ${VAR} secrets stay in env for the MCP client.
    from curie_runner.connectors import materialize_hosted_bearer_headers

    servers = {
        "github": {
            "type": "http",
            "headers": {"Authorization": "Bearer ${GITHUB_PERSONAL_ACCESS_TOKEN}"},
        }
    }
    env = {
        "GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_sentinel",
        "STDIO_TOKEN": "stdio-secret",
    }
    materialize_hosted_bearer_headers(servers, env)
    assert env["STDIO_TOKEN"] == "stdio-secret"
    assert "GITHUB_PERSONAL_ACCESS_TOKEN" not in env


def test_build_runner_expands_the_bearer_and_drops_it_from_spawn_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Wiring pin for #2559: materialize runs on the SDK spawn env inside
    # build_runner, so a PAT that arrived as a connector secret is gone before
    # the session (and Bash) starts, while the in-memory MCP header is expanded.
    monkeypatch.delenv("CURIE_STATE_URL", raising=False)
    config = _config_for(
        _bundle(tmp_path, GITHUB), release="curie", agent="acme-dev", namespace="curie"
    )
    spawn = {
        "GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_sentinel",
        "STDIO_TOKEN": "keep-me",
    }

    async def probe(*_args: Any, **_kwargs: Any) -> McpToolCapabilityProbe:
        return McpToolCapabilityProbe(complete=True, has_potential_write_tool=False, tool_count=0)

    monkeypatch.setattr(boot, "probe_mcp_tool_capability", probe)
    monkeypatch.setattr(boot, "ClaudeAgentSession", _CapturedSession)
    session = build_runner(config, fake_model=False, sdk_env=spawn)._factory()
    assert isinstance(session, _CapturedSession)
    assert "GITHUB_PERSONAL_ACCESS_TOKEN" not in spawn
    assert spawn["STDIO_TOKEN"] == "keep-me"
    github = session.options.mcp_servers["github"]
    assert github["headers"]["Authorization"] == "Bearer ghp_sentinel"
