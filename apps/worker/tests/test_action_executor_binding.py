"""What the executor loop reads about an agent's in-force binding.

@spec ACTION-EXECUTOR-5 @spec ACTION-EXECUTOR-14. ``ExecutorBindings`` answers
two questions from the agent's in-force deployment: the digest the version
renders the connector's Deployment at (``in_force_digest``), and the boot env
plus the connector secrets the target connector's derived MCP entry headers
expand (``executor_boot``). These tests drive it through its three ports: the
binding resolver, the bundle store (a real ``tar.gz`` with a real
``connectors.yaml``) and the API's connector render.
"""

from __future__ import annotations

import io
import tarfile
import uuid
from typing import Any

import pytest
from curie_worker.action_executor_binding import ExecutorBindings
from curie_worker.binding import ResolvedDeployment
from curie_worker.connector_agent import RenderedConnectors
from plugin_format.connector_render import object_name

pytestmark = pytest.mark.anyio

AGENT_ID = uuid.UUID("00000000-0000-4000-8000-0000000000a1")
VERSION_ID = uuid.UUID("00000000-0000-4000-8000-0000000000b1")
EXECUTION_ID = "00000000-0000-4000-8000-0000000000e1"
AGENT_NAME = "example-agent"
CONNECTOR = "example-scale"
RELEASE = "example-release"
NAMESPACE = "example-ns"
DIGEST = "sha256:" + "ab" * 32
OTHER_DIGEST = "sha256:" + "cd" * 32
BUNDLE_REF = "bundles/example-agent.tar.gz"

CONNECTORS_YAML = f"""connectors:
  {CONNECTOR}:
    image: registry.example/connector@{DIGEST}
    bearer_secret: EXAMPLE_SCALE_TOKEN
    secrets:
      - EXAMPLE_SCALE_TOKEN
      - EXAMPLE_OTHER_TOKEN
  other:
    image: registry.example/other@{OTHER_DIGEST}
    secrets:
      - EXAMPLE_THIRD_TOKEN
"""


def _bundle(files: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, text in files.items():
            data = text.encode("utf-8")
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


def _resolved(bundle_ref: str | None = BUNDLE_REF) -> ResolvedDeployment:
    return ResolvedDeployment(
        agent_id=AGENT_ID,
        agent_name=AGENT_NAME,
        version_id=VERSION_ID,
        version_label="v1",
        bundle_ref=bundle_ref,
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
    )


class FakeBinding:
    def __init__(self, resolved: ResolvedDeployment | None) -> None:
        self.resolved = resolved
        self.boot_thread_keys: list[str] = []

    async def resolve_agent(self, agent_id: uuid.UUID) -> ResolvedDeployment | None:
        assert agent_id == AGENT_ID
        return self.resolved

    def boot_env(
        self, resolved: ResolvedDeployment, thread_key: str, **kwargs: Any
    ) -> dict[str, str]:
        self.boot_thread_keys.append(thread_key)
        return {"CURIE_BUDGET": "{}", "CURIE_BUNDLE_REF": str(resolved.bundle_ref)}


class FakeBundles:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.reads: list[str] = []

    def get(self, key: str) -> bytes:
        self.reads.append(key)
        return self.data


class FakeManifests:
    def __init__(self, manifests: list[dict[str, Any]]) -> None:
        self.manifests = manifests
        self.calls: list[dict[str, str]] = []

    def rendered(self, *, agent_id: str, version_id: str) -> RenderedConnectors:
        self.calls.append({"agent_id": agent_id, "version_id": version_id})
        return RenderedConnectors(manifests=list(self.manifests))


def _deployment(name: str, image: str) -> dict[str, Any]:
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "generation": 1},
        "spec": {
            "replicas": 1,
            "template": {
                "spec": {
                    "containers": [
                        {"name": "caller-proxy", "image": f"registry.example/proxy@{OTHER_DIGEST}"},
                        {"name": "server", "image": image},
                    ]
                }
            },
        },
    }


def _bindings(
    *,
    resolved: ResolvedDeployment | None = None,
    bundle: bytes | None = None,
    manifests: list[dict[str, Any]] | None = None,
    local: bool = False,
) -> tuple[ExecutorBindings, FakeBinding]:
    binding = FakeBinding(resolved if resolved is not None else _resolved())
    source = None if local else FakeManifests(manifests or [])
    built = ExecutorBindings(
        binding=binding,  # type: ignore[arg-type]
        bundles=FakeBundles(
            bundle if bundle is not None else _bundle({"connectors.yaml": CONNECTORS_YAML})
        ),
        manifests=source,  # type: ignore[arg-type]
        release=RELEASE,
        namespace=NAMESPACE,
        max_uncompressed_bytes=10_000_000,
        max_compression_ratio=1000.0,
        max_members=1000,
    )
    return built, binding


_OWN_NAME = object_name(RELEASE, AGENT_NAME, CONNECTOR)


# -- in_force_digest (ACTION-EXECUTOR-14) ------------------------------------


async def test_the_in_force_digest_is_the_server_image_of_the_connectors_own_deployment() -> None:
    bindings, _ = _bindings(
        manifests=[
            _deployment(object_name(RELEASE, AGENT_NAME, "other"), f"r.example/o@{OTHER_DIGEST}"),
            _deployment(_OWN_NAME, f"registry.example/connector@{DIGEST}"),
        ]
    )

    assert await bindings.in_force_digest(str(AGENT_ID), CONNECTOR) == DIGEST


async def test_a_connector_the_version_does_not_render_has_no_in_force_digest() -> None:
    bindings, _ = _bindings(
        manifests=[_deployment(object_name(RELEASE, AGENT_NAME, "other"), f"r.example/o@{DIGEST}")]
    )

    assert await bindings.in_force_digest(str(AGENT_ID), CONNECTOR) is None


async def test_a_tagged_image_has_no_in_force_digest() -> None:
    bindings, _ = _bindings(manifests=[_deployment(_OWN_NAME, "registry.example/connector:1.2.3")])

    assert await bindings.in_force_digest(str(AGENT_ID), CONNECTOR) is None


async def test_the_local_tier_has_no_in_force_digest() -> None:
    bindings, _ = _bindings(local=True)

    assert await bindings.in_force_digest(str(AGENT_ID), CONNECTOR) is None


async def test_an_agent_without_an_in_force_deployment_has_no_in_force_digest() -> None:
    bindings, binding = _bindings(manifests=[_deployment(_OWN_NAME, f"r.example/c@{DIGEST}")])
    binding.resolved = None

    assert await bindings.in_force_digest(str(AGENT_ID), CONNECTOR) is None


# -- executor_boot (ACTION-EXECUTOR-5) ---------------------------------------


async def test_the_header_set_is_only_what_the_target_connectors_headers_expand() -> None:
    bindings, _ = _bindings()

    boot = await bindings.executor_boot(
        str(AGENT_ID), CONNECTOR, thread_key=f"action-exec:{EXECUTION_ID}"
    )

    assert boot.header_secret_names == frozenset({"EXAMPLE_SCALE_TOKEN"})
    assert boot.agent_name == AGENT_NAME


async def test_the_boot_env_is_resolved_for_the_execution_thread_key() -> None:
    """The binding mints session identity and refs per thread: one per execution."""

    bindings, binding = _bindings()

    await bindings.executor_boot(str(AGENT_ID), CONNECTOR, thread_key=f"action-exec:{EXECUTION_ID}")

    assert binding.boot_thread_keys == [f"action-exec:{EXECUTION_ID}"]


@pytest.mark.parametrize(
    "bundle",
    [
        _bundle({"README.md": "no connectors here\n"}),
        _bundle({"connectors.yaml": "connectors: [\n"}),
        _bundle({"connectors.yaml": "connectors:\n  example-scale: {}\n"}),
        _bundle({"connectors.yaml": CONNECTORS_YAML.replace(CONNECTOR, "renamed")}),
    ],
    ids=["no_connectors_file", "unparseable", "invalid", "connector_absent"],
)
async def test_a_bundle_without_a_valid_target_entry_expands_no_secret(bundle: bytes) -> None:
    bindings, _ = _bindings(bundle=bundle)

    boot = await bindings.executor_boot(
        str(AGENT_ID), CONNECTOR, thread_key=f"action-exec:{EXECUTION_ID}"
    )

    assert boot.header_secret_names == frozenset()


async def test_an_agent_without_an_in_force_deployment_has_no_executor_boot() -> None:
    bindings, binding = _bindings()
    binding.resolved = None

    with pytest.raises(LookupError):
        await bindings.executor_boot(
            str(AGENT_ID), CONNECTOR, thread_key=f"action-exec:{EXECUTION_ID}"
        )
