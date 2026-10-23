"""The shared render delivers the caller verify keys the proxy admits with (ADR 0239).

Identity contract 2: key distribution stays the trusted worker and operator
mechanism, and no connector Pod holds signer material. The keys here come from
the dedicated render's own trusted source, the API ``Settings`` reading
``CURIE_CONNECTOR_CALLER_PUBLIC_KEY`` and ``CURIE_CONNECTOR_CALLER_PREVIOUS_PUBLIC_KEY``.
The application is registered through the real API, its stored spec is read
back and rendered with those keys, and the proxy starts on the rendered proxy
env with only host-local names (socket path, mounted Secret file paths,
upstream URL, listen port) substituted. The keys themselves are never
substituted, so a token is admitted only because the render carried its key.

Real Postgres, real Valkey, real API, real resolver; only the provider is faked.
"""

from __future__ import annotations

import base64
import json
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from _fake_provider import FakeProvider
from _migration_support import sql_dicts
from _resolver_harness import Fleet
from _shared_connector_support import SLOT
from _shared_proxy_harness import (
    APPLICATION_ENV,
    GRANT_STORE_FILE_ENV,
    INSTALLATION_ENV,
    PROXY_PORT_ENV,
    PUBLIC_KEYS_ENV,
    UPSTREAM_URL_ENV,
    SharedProxy,
    Worlds,
    agent_name,
    refusal_table,
)
from curie_api.config import Settings, get_settings
from curie_connector_proxy import server, shared
from curie_connector_proxy.resolver.config import ResolverConfig
from curie_worker.caller_token import mint
from nacl.signing import SigningKey
from plugin_format.shared_connector_render import SharedApplication, render_application

REPO = Path(__file__).resolve().parents[4]
RENDER_KEYS_VECTOR_FILE = REPO / "tests" / "vectors" / "shared-render-caller-keys.json"
PROXY_IMAGE = "example.com/acme/proxy@sha256:" + "b" * 64

# The only rendered names whose values are host-local: where this test's files,
# socket and fixture server live, and the port it listens on.
HOST_LOCAL_ENV = frozenset(
    {
        "CURIE_SHARED_RESOLVER_SOCKET",
        "CURIE_SHARED_ENCRYPTION_KEY_FILE",
        "CURIE_SHARED_DATABASE_DSN_FILE",
        GRANT_STORE_FILE_ENV,
        UPSTREAM_URL_ENV,
        PROXY_PORT_ENV,
    }
)


def _public(signing: SigningKey) -> str:
    return base64.b64encode(bytes(signing.verify_key)).decode("ascii")


def _seed(signing: SigningKey) -> str:
    return base64.b64encode(bytes(signing)).decode("ascii")


def _stored_spec(application_id: str) -> SharedApplication:
    """The spec exactly as the API stored it at registration."""

    (row,) = sql_dicts(
        "SELECT spec FROM curie.shared_connector_applications WHERE id = :id",
        {"id": uuid.UUID(application_id)},
    )
    spec = row["spec"]
    if isinstance(spec, str):
        spec = json.loads(spec)
    return SharedApplication.model_validate_json(json.dumps(spec))


def _container(objects: list[dict[str, Any]], name: str) -> dict[str, Any]:
    (deployment,) = [item for item in objects if item["kind"] == "Deployment"]
    found: list[dict[str, Any]] = [
        container
        for container in deployment["spec"]["template"]["spec"]["containers"]
        if container["name"] == name
    ]
    assert len(found) == 1, name
    return found[0]


def _env_of(container: dict[str, Any]) -> dict[str, str]:
    names = [entry["name"] for entry in container["env"]]
    assert len(set(names)) == len(names), names
    return {entry["name"]: entry["value"] for entry in container["env"]}


class Signers:
    """The current and previous caller signers, and one no render lists."""

    def __init__(self) -> None:
        self.current = SigningKey.generate()
        self.previous = SigningKey.generate()
        self.unlisted = SigningKey.generate()


class RenderedEnv:
    """``SharedProxy``'s ``proxy_env`` hook: the rendered proxy env, host paths swapped."""

    def __init__(self, keys: tuple[str, ...]) -> None:
        self.keys = keys
        self.objects: list[dict[str, Any]] = []
        self.rendered: dict[str, str] = {}
        self.harness: dict[str, str] = {}

    def __call__(self, world: SharedProxy, harness_env: dict[str, str]) -> dict[str, str]:
        spec = _stored_spec(world.app.application_id)
        assert spec.application == world.app.application_id
        assert spec.installation == world.app.installation
        self.objects = render_application(spec, caller_public_keys=self.keys)
        self.rendered = _env_of(_container(self.objects, "proxy"))
        self.harness = dict(harness_env)
        env = dict(self.rendered)
        for name in HOST_LOCAL_ENV:
            env[name] = harness_env[name]
        return env


@pytest.fixture
def signers() -> Signers:
    return Signers()


@pytest.fixture
def trusted_keys(
    shared_database: str, monkeypatch: pytest.MonkeyPatch, signers: Signers
) -> tuple[str, ...]:
    """The keys the dedicated render takes, from the API's own settings.

    ``shared_database`` first: it sets the environment the API settings need.
    """

    monkeypatch.setenv("CURIE_CONNECTOR_CALLER_PUBLIC_KEY", _public(signers.current))
    monkeypatch.setenv("CURIE_CONNECTOR_CALLER_PREVIOUS_PUBLIC_KEY", _public(signers.previous))
    monkeypatch.setenv("CURIE_CONNECTOR_PROXY_IMAGE", PROXY_IMAGE)
    proxy = Settings().connector_proxy()
    assert proxy is not None
    assert proxy.public_keys == (_public(signers.current), _public(signers.previous))
    return proxy.public_keys


@pytest.fixture
def worlds(fleet: Fleet, provider: FakeProvider) -> Iterator[Worlds]:
    built = Worlds(fleet, provider)
    try:
        yield built
    finally:
        built.close()


@pytest.fixture
def rendered(trusted_keys: tuple[str, ...]) -> RenderedEnv:
    return RenderedEnv(trusted_keys)


@pytest.fixture
def world(worlds: Worlds, rendered: RenderedEnv) -> SharedProxy:
    return worlds(proxy_env=rendered)


def test_the_proxy_starts_on_the_rendered_env_with_only_host_local_names_swapped(
    world: SharedProxy, rendered: RenderedEnv, trusted_keys: tuple[str, ...]
) -> None:
    env = world.env
    # The harness models every name the render gives the proxy, and no more.
    assert set(rendered.rendered) == set(rendered.harness)
    assert set(env) == set(rendered.rendered)
    assert HOST_LOCAL_ENV <= set(rendered.rendered)
    assert PUBLIC_KEYS_ENV not in HOST_LOCAL_ENV
    changed = {name for name in env if env[name] != rendered.rendered[name]}
    assert changed <= HOST_LOCAL_ENV, changed
    assert PUBLIC_KEYS_ENV not in changed
    for name in set(env) - HOST_LOCAL_ENV:
        assert env[name] == rendered.rendered[name], name
    # What the render carried verbatim is this application's own identity and pins.
    assert rendered.rendered[INSTALLATION_ENV] == world.app.installation
    assert rendered.rendered[APPLICATION_ENV] == world.app.application_id
    assert rendered.rendered["CURIE_SHARED_DSN_ENCODING"] == "bundled-sealed"
    assert json.loads(rendered.rendered["CURIE_SHARED_PROVIDER_ORIGINS"]) == [
        world.provider.origin
    ]
    assert json.loads(rendered.rendered["CURIE_SHARED_CREDENTIAL_SLOTS"]) == [SLOT]
    assert rendered.rendered[PUBLIC_KEYS_ENV] == ",".join(trusted_keys)
    # The running resolver was started on the same values the render carries.
    for name, value in world.resolver.env.items():
        assert env[name] == value, name

    config = shared.SharedProxyConfig.from_env(env)
    assert (config.installation, config.application) == (
        world.app.installation,
        world.app.application_id,
    )
    assert [bytes(key) for key in config.public_keys] == [
        base64.b64decode(key) for key in trusted_keys
    ]
    resolver = ResolverConfig.from_env(env)
    running = world.resolver.config
    assert (
        resolver.installation,
        resolver.application,
        resolver.socket_path,
        resolver.dsn_encoding,
        resolver.provider_origins,
        resolver.credential_slots,
        resolver.dsn,
    ) == (
        running.installation,
        running.application,
        running.socket_path,
        running.dsn_encoding,
        running.provider_origins,
        running.credential_slots,
        running.dsn,
    )


def test_current_and_previous_signers_are_admitted_and_an_unlisted_one_is_refused(
    world: SharedProxy, signers: Signers
) -> None:
    _assert_current_and_previous_admitted(world, signers)


def _assert_current_and_previous_admitted(world: SharedProxy, signers: Signers) -> None:
    member = world.member()

    def token(signing: SigningKey) -> str:
        return mint(
            _seed(signing),
            installation=world.app.installation,
            agent_id=member.agent_id,
            agent=agent_name(member.agent_id),
            exp=int(time.time()) + 900,
        )

    for signing in (signers.current, signers.previous):
        before = world.observe()
        result = world.call(member, "whoami", tokens=[token(signing)]).result
        assert "refused" not in result and "error" not in result, result
        assert (result["binding"], result["account"]) == (member.binding_id, member.account)
        assert result["authorization_sha256"] == world.digest(member)
        calls = [r for r in world.fixture.since(before.upstream) if r.rpc == ("tools/call",)]
        assert len(calls) == 1

    assert refusal_table()["invalid"] == (403, "invalid")
    # The unlisted signer, and the harness's own per-world key the render never saw.
    for refused in (token(signers.unlisted), world.token(member)):
        before = world.observe()
        world.assert_refused(world.call(member, "whoami", tokens=[refused]), "invalid", before)

    # Liveness after the refusals: the current signer is still served.
    result = world.call(member, "whoami", tokens=[token(signers.current)]).result
    assert result["binding"] == member.binding_id


def test_the_rendered_env_without_its_keys_refuses_to_start(world: SharedProxy) -> None:
    shared.SharedProxyConfig.from_env(world.env)
    without = {name: value for name, value in world.env.items() if name != PUBLIC_KEYS_ENV}
    assert set(world.env) - set(without) == {PUBLIC_KEYS_ENV}
    with pytest.raises(ValueError) as refused:
        shared.SharedProxyConfig.from_env(without)
    assert PUBLIC_KEYS_ENV in str(refused.value)


def test_the_rendered_server_container_holds_no_caller_key(
    world: SharedProxy, rendered: RenderedEnv, trusted_keys: tuple[str, ...]
) -> None:
    server_env = _env_of(_container(rendered.objects, "server"))
    assert server_env
    assert not [name for name in server_env if name.startswith("CURIE_CALLER_")]
    for value in server_env.values():
        assert not any(key in value for key in trusted_keys), value
    proxy_env = _env_of(_container(rendered.objects, "proxy"))
    assert proxy_env[PUBLIC_KEYS_ENV] == ",".join(trusted_keys)


def _accepted_vectors() -> list[dict[str, Any]]:
    document = json.loads(RENDER_KEYS_VECTOR_FILE.read_text(encoding="utf-8"))
    accepted = [vector for vector in document["vectors"] if "env" in vector]
    assert accepted
    return accepted


@pytest.mark.parametrize(
    "vector", _accepted_vectors(), ids=lambda vector: str(vector["name"])
)
def test_the_proxy_reads_every_accepted_render_value_as_its_keys(vector: dict[str, Any]) -> None:
    keys = server._public_keys({PUBLIC_KEYS_ENV: vector["env"]})
    assert [bytes(key) for key in keys] == [
        base64.b64decode(text, validate=True) for text in vector["public_keys"]
    ]


# =============================================================================
# Rework 1: the keys come from the API's own render route, never from a table
# =============================================================================


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


class RoutedEnv(RenderedEnv):
    """``proxy_env`` from the worker render route the reconciler applies, unmodified.

    The objects must be byte-identical to the frozen render of the stored spec
    with the trusted keys, so the proxy admits only what the API rendered.
    """

    def __call__(self, world: SharedProxy, harness_env: dict[str, str]) -> dict[str, str]:
        response = world.store.application_render(world.app.application_id)
        assert response.status_code == 200, response.text
        self.objects = response.json()["objects"]
        spec = _stored_spec(world.app.application_id)
        assert _canonical(self.objects) == _canonical(
            render_application(spec, caller_public_keys=self.keys)
        )
        self.rendered = _env_of(_container(self.objects, "proxy"))
        self.harness = dict(harness_env)
        env = dict(self.rendered)
        for name in HOST_LOCAL_ENV:
            env[name] = harness_env[name]
        return env


@pytest.fixture
def routed(trusted_keys: tuple[str, ...]) -> Iterator[RoutedEnv]:
    # The routes read settings per request: the trusted keys are what they render with.
    get_settings.cache_clear()
    try:
        yield RoutedEnv(trusted_keys)
    finally:
        get_settings.cache_clear()


def test_the_api_rendered_env_admits_current_and_previous_and_refuses_unlisted(
    worlds: Worlds, routed: RoutedEnv, signers: Signers, trusted_keys: tuple[str, ...]
) -> None:
    world = worlds(proxy_env=routed)
    assert routed.rendered[PUBLIC_KEYS_ENV] == ",".join(trusted_keys)
    assert world.env[PUBLIC_KEYS_ENV] == ",".join(trusted_keys)
    server_env = _env_of(_container(routed.objects, "server"))
    assert not [name for name in server_env if name.startswith("CURIE_CALLER_")]
    _assert_current_and_previous_admitted(world, signers)


# Assembled so this file does not name what it looks for.
_RESIDUE = (
    "shared_connector_" + "caller_key",
    "read_" + "caller_keys",
    "build_" + "shared_app",
    "publish_" + "caller_keys",
    "caller_keys" + ".publish",
    "caller_keys_" + "unavailable",
)
_SOURCE_SUFFIXES = {".py", ".json", ".sql", ".toml", ".yaml", ".yml", ".sh", ".rs", ".md"}
# Code, the chart and the interface docs; ADRs and atlas snapshots are history.
_SCANNED = ("apps", "packages", "charts", "docs/interfaces")


def test_no_caller_key_table_or_reader_remains() -> None:
    """The rejected key table leaves no migration, model, reader, publisher or test."""

    versions = REPO / "apps" / "api" / "alembic" / "versions"
    assert not sorted(versions.glob("0125_*")), "there is no 0125"
    assert not (REPO / "apps/api/src/curie_api/shared_connectors/caller_keys.py").exists()
    me = Path(__file__).resolve()
    found: list[str] = []
    for root in (REPO / name for name in _SCANNED):
        for path in root.rglob("*"):
            if (
                not path.is_file()
                or path.suffix not in _SOURCE_SUFFIXES
                or path.resolve() == me
                or any(part in {"__pycache__", "node_modules", ".venv"} for part in path.parts)
            ):
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
            found += [f"{path.relative_to(REPO)}: {token}" for token in _RESIDUE if token in text]
    assert found == [], found
    # The shared proxy takes its keys from the rendered env, never as dedicated-only.
    assert PUBLIC_KEYS_ENV not in shared.DEDICATED_ONLY_ENV
