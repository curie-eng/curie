"""The worker's own declaration readers handle shared entries explicitly (ADR 0239, cp5 AC1).

The worker half of the declaration adapter: ``ActionExecutorBindings`` reads
the stored bundle with the shared parser, restricts it to the acting agent's
``deploy.yaml`` target, and resolves what remains through the real API's
``POST /v1/internal/shared-connectors/resolve`` (``SharedResolveClient`` given
an ``httpx.Client`` whose transport is the real API app, recorded by
``ApiTransport``). ``HttpManifestSource`` maps the render route's shared
refusals to ``SharedRenderRefused``. Every refusal is a typed exception with
its exact code, never ``frozenset()`` and never None.

The binding resolver and bundle store are the ports
``test_action_executor_binding.py`` already fakes (``FakeBinding``,
``FakeBundles``); the agents, applications and memberships behind them are
real API rows.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_action_executor_binding import (  # noqa: E402
    CONNECTOR as DEDICATED_ONLY_CONNECTOR,
)
from test_action_executor_binding import (  # noqa: E402
    CONNECTORS_YAML as DEDICATED_ONLY_YAML,
)
from test_action_executor_binding import FakeBinding, FakeBundles  # noqa: E402
from test_action_executor_loop import valkey  # noqa: E402
from test_shared_connector_apply import (  # noqa: E402
    ALIAS,
    API_BASE,
    APP_NAME,
    DEDICATED,
    DEDICATED_IMAGE,
    NAMESPACE,
    RELEASE,
    RESOLVE_PATH,
    Deployed,
    World,
    _required_valkey,
    archive,
    bundle_files,
    canonical,
    cleanup,
    connectors_yaml,
    fail_render,
    render_path,
    sessions,
    shared_database,
    world,
)

__all__ = ["_required_valkey", "cleanup", "sessions", "shared_database", "valkey", "world"]

# A dedicated connector whose derived MCP entry expands one header secret.
HEADER_SPEC: dict[str, Any] = {
    "image": DEDICATED_IMAGE,
    "bearer_secret": "GRAFANA_TOKEN",
    "secrets": ["GRAFANA_TOKEN"],
}
HEADER_NAMES = frozenset({"GRAFANA_TOKEN"})
DIGEST = DEDICATED_IMAGE.rsplit("@", 1)[1]

Identity = tuple[str, str, str]  # agent_id, agent name, version_id


def _identity(agent: Deployed | tuple[str, str]) -> Identity:
    if isinstance(agent, Deployed):
        return agent.agent_id, agent.name, agent.version_id
    return agent[0], agent[1], str(uuid.uuid4())


def _mixed(application_id: str, *, deploy: str | None = None) -> bytes:
    return archive(
        bundle_files(connectors_yaml({ALIAS: application_id}, dedicated=HEADER_SPEC), deploy=deploy)
    )


def _bindings(
    world: World,
    agent: Deployed | tuple[str, str],
    data: bytes | None,
    *,
    resolve: bool = True,
    token: str | None = None,
) -> Any:
    from curie_api.config import get_settings
    from curie_worker.action_executor_binding import ExecutorBindings
    from curie_worker.binding import ResolvedDeployment
    from curie_worker.connector_loop import HttpManifestSource
    from curie_worker.shared_declarations_client import SharedResolveClient

    agent_id, name, version_id = _identity(agent)
    settings = get_settings()
    client = (
        SharedResolveClient(
            client=world.http,
            base_url=API_BASE,
            internal_worker_token=settings.internal_worker_token if token is None else token,
        )
        if resolve
        else None
    )
    resolved = ResolvedDeployment(
        agent_id=uuid.UUID(agent_id),
        agent_name=name,
        version_id=uuid.UUID(version_id),
        version_label="v1",
        bundle_ref=None if data is None else f"bundles/{agent_id}/{version_id}.tar.gz",
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
    )
    return ExecutorBindings(
        binding=FakeBinding(resolved),  # type: ignore[arg-type]
        bundles=FakeBundles(data or b""),  # type: ignore[arg-type]
        manifests=HttpManifestSource(
            api_base_url=API_BASE,
            api_key=settings.api_key,
            release=RELEASE,
            namespace=NAMESPACE,
            app_name=APP_NAME,
            client=world.http,
        ),
        shared=client,
        release=RELEASE,
        namespace=NAMESPACE,
        max_uncompressed_bytes=10_000_000,
        max_compression_ratio=1000.0,
        max_members=1000,
    )


def _boot(bindings: Any, agent: Deployed | tuple[str, str], connector: str) -> frozenset[str]:
    agent_id = agent.agent_id if isinstance(agent, Deployed) else agent[0]
    boot = asyncio.run(
        bindings.executor_boot(agent_id, connector, thread_key=f"action-exec:{uuid.uuid4()}")
    )
    names: frozenset[str] = boot.header_secret_names
    return names


def _resolves(world: World) -> int:
    return world.transport.count("POST", RESOLVE_PATH)


# =============================================================================
# The executor binding
# =============================================================================


def test_executor_header_names_for_dedicated_in_mixed_bundle(world: World) -> None:
    from curie_worker.shared_declarations_client import SharedDeclarationRefused

    application_id = world.application()
    member = world.agent()
    world.join(application_id, member[0])
    data = _mixed(application_id)

    assert _boot(_bindings(world, member, data), member, DEDICATED) == HEADER_NAMES
    assert _resolves(world) == 1

    outsider = world.agent()
    with pytest.raises(SharedDeclarationRefused) as refused:
        _boot(_bindings(world, outsider, data), outsider, DEDICATED)
    assert refused.value.code == "shared_application_not_member"


def test_executor_refuses_shared_target(world: World) -> None:
    from curie_worker.shared_declarations_client import (
        SharedConnectorUnsupported,
        SharedDeclarationRefused,
    )

    application_id = world.application()
    member = world.agent()
    world.join(application_id, member[0])
    bindings = _bindings(world, member, _mixed(application_id))

    with pytest.raises(SharedConnectorUnsupported) as refused:
        _boot(bindings, member, ALIAS)
    assert refused.value.code == "shared_connector_action_unsupported"
    assert isinstance(refused.value, SharedDeclarationRefused)
    # Liveness: the dedicated sibling in the same bundle still boots.
    assert _boot(bindings, member, DEDICATED) == HEADER_NAMES


def test_in_force_digest_refuses_shared(world: World) -> None:
    from curie_worker.shared_declarations_client import SharedConnectorUnsupported

    application_id = world.application()
    member = world.member(application_id, dedicated=HEADER_SPEC)
    bindings = _bindings(world, member, _mixed(application_id))

    with pytest.raises(SharedConnectorUnsupported) as refused:
        asyncio.run(bindings.in_force_digest(member.agent_id, ALIAS))
    assert refused.value.code == "shared_connector_action_unsupported"
    # Liveness: the dedicated connector's digest from the same render.
    assert asyncio.run(bindings.in_force_digest(member.agent_id, DEDICATED)) == DIGEST


MIXED_ENTRY = (
    "connectors:\n"
    f"  {ALIAS}:\n"
    "    shared:\n"
    "      application: {application}\n"
    "      contract: 1\n"
    f"    image: {DEDICATED_IMAGE}\n"
)


def test_malformed_shared_raises_code(world: World) -> None:
    from curie_worker.shared_declarations_client import ConnectorDeclarationRefused

    application_id = world.application()
    member = world.agent()
    world.join(application_id, member[0])
    data = archive(bundle_files(MIXED_ENTRY.format(application=application_id)))

    with pytest.raises(ConnectorDeclarationRefused) as refused:
        _boot(_bindings(world, member, data), member, DEDICATED)
    assert refused.value.code == "connectors.shared_invalid"
    assert _resolves(world) == 0


def test_dedicated_only_bundle_unchanged(world: World) -> None:
    agent = world.agent()
    data = archive({"connectors.yaml": DEDICATED_ONLY_YAML})
    expected = frozenset({"EXAMPLE_SCALE_TOKEN"})  # test_action_executor_binding's answer

    for resolve in (False, True):
        bindings = _bindings(world, agent, data, resolve=resolve)
        assert _boot(bindings, agent, DEDICATED_ONLY_CONNECTOR) == expected, resolve
    assert _resolves(world) == 0


# =============================================================================
# The render source
# =============================================================================


def test_http_source_maps_shared_refusal(world: World) -> None:
    from curie_api.config import get_settings
    from curie_worker.connector_loop import HttpManifestSource, SharedRenderRefused

    application_id = world.application()
    member = world.member(application_id)
    source = HttpManifestSource(
        api_base_url=API_BASE,
        api_key=get_settings().api_key,
        release=RELEASE,
        namespace=NAMESPACE,
        app_name=APP_NAME,
        client=world.http,
    )

    rendered = source.rendered(agent_id=member.agent_id, version_id=member.version_id)
    (entry,) = rendered.shared
    assert (entry.name, entry.application) == (ALIAS, application_id)
    assert [canonical(obj) for obj in entry.reach] == [
        canonical(obj) for obj in world.expected_reach(application_id, member.agent_id)
    ]

    world.remove_member(application_id, member.agent_id)
    with pytest.raises(SharedRenderRefused) as refused:
        source.rendered(agent_id=member.agent_id, version_id=member.version_id)
    assert refused.value.code == "shared_application_not_member"

    # A 409 whose detail is not a shared code keeps the unchanged path.
    def conflict(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json={"detail": "version is being replaced"})

    world.transport.rules.append(fail_render(member.agent_id, conflict))
    with pytest.raises(httpx.HTTPStatusError) as unchanged:
        source.rendered(agent_id=member.agent_id, version_id=member.version_id)
    assert not isinstance(unchanged.value, SharedRenderRefused)
    assert unchanged.value.response.status_code == 409
    assert world.transport.requests[-1] == (
        "GET",
        render_path(member.agent_id, member.version_id),
    )


def test_blank_worker_token_refuses(world: World) -> None:
    from curie_internal.shared_declarations import SharedEntry
    from curie_worker.shared_declarations_client import (
        SharedDeclarationRefused,
        SharedResolveClient,
    )

    application_id = world.application()
    member = world.agent()
    world.join(application_id, member[0])
    data = _mixed(application_id)

    with pytest.raises(SharedDeclarationRefused) as refused:
        _boot(_bindings(world, member, data, resolve=False), member, DEDICATED)
    assert refused.value.code == "shared_worker_token_missing"
    blank = SharedResolveClient(client=world.http, base_url=API_BASE, internal_worker_token="")
    with pytest.raises(SharedDeclarationRefused) as refused:
        blank.resolve(member[0], (SharedEntry(name=ALIAS, application=application_id),))
    assert refused.value.code == "shared_worker_token_missing"
    assert _resolves(world) == 0

    # Liveness: with the token the same bundle resolves.
    assert _boot(_bindings(world, member, data), member, DEDICATED) == HEADER_NAMES


def _targets(agent_name: str, acting: list[str]) -> str:
    return (
        "targets:\n"
        "  acting:\n"
        f"    agent: {agent_name}\n"
        f"    connectors: [{', '.join(acting)}]\n"
        "  other:\n"
        "    agent: another-agent\n"
        f"    connectors: [{DEDICATED}, {ALIAS}]\n"
    )


def test_acting_agent_target_excludes_shared_alias(world: World) -> None:
    from curie_worker.shared_declarations_client import SharedDeclarationRefused

    application_id = world.application()
    acting = world.agent()  # no membership at all
    restricted = _mixed(application_id, deploy=_targets(acting[1], [DEDICATED]))

    assert _boot(_bindings(world, acting, restricted), acting, DEDICATED) == HEADER_NAMES
    assert _resolves(world) == 0, "an excluded shared alias is never resolved"

    including = _mixed(application_id, deploy=_targets(acting[1], [DEDICATED, ALIAS]))
    with pytest.raises(SharedDeclarationRefused) as refused:
        _boot(_bindings(world, acting, including), acting, DEDICATED)
    assert refused.value.code == "shared_application_not_member"


DUPLICATE_KEY = (
    "connectors:\n"
    f"  {DEDICATED}:\n"
    "    url: https://a.example.invalid/mcp\n"
    f"  {DEDICATED}:\n"
    "    url: https://b.example.invalid/mcp\n"
)
_BOUNDARY: Mapping[str, tuple[str | bytes, str]] = {
    "unreadable-utf8": (b"connectors:\n  grafana:\n    url: \xff\xfe\n", "connectors.unreadable"),
    "duplicate-key": (DUPLICATE_KEY, "connectors.unreadable"),
    "invalid-yaml": ("connectors: [\n", "connectors.unreadable"),
    "malformed-dedicated": (
        "connectors:\n  Bad Name:\n    url: https://example.invalid/mcp\n",
        "connectors.bad_name",
    ),
    "malformed-shared": (MIXED_ENTRY, "connectors.shared_invalid"),
}


@pytest.mark.parametrize("case", list(_BOUNDARY))
def test_reader_boundary_refusals(world: World, case: str) -> None:
    from curie_worker.shared_declarations_client import ConnectorDeclarationRefused

    application_id = world.application()
    agent = world.agent()
    content, code = _BOUNDARY[case]
    if isinstance(content, str):
        content = content.replace("{application}", application_id)
    data = archive(bundle_files(content))

    with pytest.raises(ConnectorDeclarationRefused) as refused:
        _boot(_bindings(world, agent, data), agent, DEDICATED)
    assert refused.value.code == code, case
    assert _resolves(world) == 0


def test_reader_boundary_absent_cases_stay_empty(world: World) -> None:
    agent = world.agent()
    # No bundle_ref, and a bundle without connectors.yaml.
    assert _boot(_bindings(world, agent, None), agent, DEDICATED) == frozenset()
    no_file = archive(bundle_files(None))
    assert _boot(_bindings(world, agent, no_file), agent, DEDICATED) == frozenset()


@pytest.mark.anyio
async def test_reader_boundary_refusal_fails_the_execution(
    valkey: Any, caplog: pytest.LogCaptureFixture
) -> None:
    """Secondary for case 1: the executor loop fails it through sandbox-unavailable."""

    from curie_worker.action_executor_binding import ExecutorBindings
    from curie_worker.binding import ResolvedDeployment
    from executor_loop_fixtures import AGENT_ID, AGENT_NAME
    from test_action_executor_loop import _final, _rig

    unreadable = archive(bundle_files(_BOUNDARY["unreadable-utf8"][0]))
    bindings = ExecutorBindings(
        binding=FakeBinding(  # type: ignore[arg-type]
            ResolvedDeployment(
                agent_id=uuid.UUID(AGENT_ID),
                agent_name=AGENT_NAME,
                version_id=uuid.uuid4(),
                version_label="v1",
                bundle_ref="bundles/unreadable.tar.gz",
                max_usd_per_day=None,
                max_output_tokens_per_run=None,
            )
        ),
        bundles=FakeBundles(unreadable),  # type: ignore[arg-type]
        manifests=None,
        shared=None,
        release=RELEASE,
        namespace=NAMESPACE,
        max_uncompressed_bytes=10_000_000,
        max_compression_ratio=1000.0,
        max_members=1000,
    )
    async with _rig(valkey) as rig:
        execution = rig.api.add_restore()
        loop = rig.loop()
        # The rig's loop with this binding's real reader in place of its stub.
        loop._executor_boot = bindings.executor_boot
        with caplog.at_level(logging.INFO, logger="curie_worker.action_executor_loop"):
            await loop.run_once()

        assert _final(rig, execution) == ("refused", "sandbox_unavailable")
        assert rig.runner.requests == []
        assert "code=connectors.unreadable" in caplog.text
        rig.assert_released()
