"""The shared proxy trusts the verify keys live API processes publish (ADR 0239, cp5 AC2).

``gap-decision.md`` decision (a): every API process publishes its configured
current and previous caller verify keys, plus an installation heartbeat, into
``curie.shared_connector_caller_keys`` and ``..._heartbeats``; the shared
proxy reads, through its application's own role, the keys seen within
``CALLER_KEY_WINDOW_S`` (300 s) of the newest heartbeat, once at start (none
refuses to start) and again on every request (none, or a read failure, is
``store_unavailable``). ``CURIE_CALLER_PROXY_PUBLIC_KEYS`` is dedicated-only.

Publication here is always the production ``caller_keys.publish``
(``publish_caller_keys`` in the harness); aging a key is a direct write of its
``seen_at`` in the test-owned database, as the API caller-keys suite does.

The end to end cases start ``python -m curie_connector_proxy`` with the proxy
container's environment exactly as the frozen ``render_application`` renders it
for the registered application, except the three Secret file paths and the
resolver socket, which are mount points a host test cannot create and are
relocated into a private directory. The files hold exactly what the worker
writes into the Secrets (one base64 X25519 private key per line; the API's
base64 sealed DSN text). The resolver connects as the real bundled
application role and the grant store is the real Valkey. Only the provider is
faked; the fixture MCP server is the upstream.
"""

from __future__ import annotations

import base64
import http.client
import json
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from _fake_provider import FakeProvider, authorization_digest
from _fixture_mcp_server import MCP_PATH, PROTOCOL_VERSION, FixtureMCPServer
from _migration_support import sql_rows
from _resolver_harness import Fleet, Member, SharedApp, terminate_role_backends
from _shared_connector_support import SLOT, SessionPool, SharedStore, quote_ident
from _shared_proxy_harness import (
    ALIAS_HEADER,
    APPLICATION_ENV,
    CALLER_HEADER,
    GRANT_STORE_FILE_ENV,
    INSTALLATION_ENV,
    PROXY_PORT_ENV,
    PROXY_WAIT_S,
    PUBLIC_KEYS_ENV,
    REPO,
    UPSTREAM_URL_ENV,
    ProxyReply,
    Worlds,
    agent_name,
    closed_port,
    expected_refusal_body,
    process_env,
    publish_caller_keys,
    refusal_table,
    refuses_connection,
    signing_pair,
    valkey_url,
    wait_until,
)
from plugin_format.shared_connector_render import SharedApplication, render_application

KEYS_TABLE = "shared_connector_caller_keys"
HEARTBEATS_TABLE = "shared_connector_caller_key_heartbeats"
# The proxy container's environment names the frozen render points at mount
# points (three Secret files) and at the resolver socket; nothing else moves.
RELOCATED = (
    "CURIE_SHARED_ENCRYPTION_KEY_FILE",
    "CURIE_SHARED_DATABASE_DSN_FILE",
    "CURIE_SHARED_GRANT_STORE_FILE",
    "CURIE_SHARED_RESOLVER_SOCKET",
)


@pytest.fixture
def worlds(fleet: Fleet, provider: FakeProvider) -> Iterator[Worlds]:
    built = Worlds(fleet, provider)
    try:
        yield built
    finally:
        built.close()


def _admitted(reply: ProxyReply, member: Member) -> dict[str, Any]:
    result = reply.result
    assert "refused" not in result and "error" not in result, result
    assert result["binding"] == member.binding_id, result
    return result


def _refusal(reply: ProxyReply, code: str) -> None:
    status, _ = refusal_table()[code]
    assert reply.status == status, (code, reply.status, reply.text)
    assert reply.json == expected_refusal_body(code), (code, reply.text)


def _age(installation: str, public_key: str, seconds: int) -> None:
    """Set one key's ``seen_at`` to the newest heartbeat minus ``seconds``."""

    sql_rows(
        f"UPDATE curie.{KEYS_TABLE} k "
        "SET seen_at = h.seen_at - CAST(:age AS integer) * interval '1 second' "
        f"FROM curie.{HEARTBEATS_TABLE} h "
        "WHERE h.installation_id = k.installation_id "
        "AND k.installation_id = :installation AND k.public_key = :key",
        {"installation": installation, "key": public_key, "age": seconds},
    )


def _clear(installation: str) -> None:
    for table in (KEYS_TABLE, HEARTBEATS_TABLE):
        sql_rows(
            f"DELETE FROM curie.{table} WHERE installation_id = :installation",
            {"installation": installation},
        )


# =============================================================================
# The entry point, from the frozen render
# =============================================================================


@dataclass
class RenderedApplication:
    """One registered application, its frozen proxy env and the files it names."""

    store: SharedStore
    app: SharedApp
    member: Member
    fixture: FixtureMCPServer
    provider: FakeProvider
    env: dict[str, str]
    rendered_env: dict[str, str]
    seed: str
    public_key: str
    processes: list[subprocess.Popen[str]] = field(default_factory=list)

    @property
    def port(self) -> int:
        return int(self.env[PROXY_PORT_ENV])

    @property
    def installation(self) -> str:
        return self.app.installation

    def token(self, seed: str | None = None) -> str:
        from curie_worker.caller_token import mint

        agent_id = self.member.agent_id
        return mint(
            self.seed if seed is None else seed,
            installation=self.installation,
            agent_id=agent_id,
            agent=agent_name(agent_id),
            exp=int(time.time()) + 900,
            run=None,
            work_item=None,
        )

    def start(self) -> subprocess.Popen[str]:
        process = subprocess.Popen(
            [sys.executable, "-m", "curie_connector_proxy"],
            env=process_env(self.env),
            cwd=REPO,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.processes.append(process)
        return process

    def serving(self) -> subprocess.Popen[str]:
        process = self.start()
        wait_until(
            lambda: process.poll() is not None or not refuses_connection(self.port),
            "the shared proxy process to listen",
        )
        assert process.poll() is None, (
            "the shared proxy process exited instead of listening",
            process.communicate(timeout=30),
        )
        return process

    def call(self, tool: str, token: str) -> ProxyReply:
        message = {
            "jsonrpc": "2.0",
            "id": uuid.uuid4().int % 1_000_000,
            "method": "tools/call",
            "params": {"name": tool, "arguments": {}},
        }
        body = json.dumps(message, separators=(",", ":")).encode("utf-8")
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=PROXY_WAIT_S)
        try:
            connection.putrequest("POST", MCP_PATH, skip_accept_encoding=True)
            for name, value in (
                ("Content-Type", "application/json"),
                ("Accept", "application/json, text/event-stream"),
                ("MCP-Protocol-Version", PROTOCOL_VERSION),
                (CALLER_HEADER, token),
                (ALIAS_HEADER, self.member.alias),
                ("Content-Length", str(len(body))),
            ):
                connection.putheader(name, value)
            connection.endheaders(body)
            response = connection.getresponse()
            return ProxyReply(response.status, tuple(response.getheaders()), response.read())
        finally:
            connection.close()

    def close(self) -> None:
        for process in self.processes:
            if process.poll() is None:
                process.terminate()
            try:
                process.communicate(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=30)
        self.fixture.release_all()
        self.fixture.close()


def _render_application(fleet: Fleet, provider: FakeProvider) -> RenderedApplication:
    """Register an application through the API and lay out its frozen proxy env."""

    store = fleet.store
    directory = fleet.directory()
    secret_dir = directory / "s"
    socket_dir = directory / "k"
    secret_dir.mkdir(mode=0o700)
    socket_dir.mkdir(mode=0o700)
    socket_path = socket_dir / "r.sock"
    fixture = FixtureMCPServer(str(socket_path), provider, slot=SLOT)
    try:
        application_id = str(uuid.uuid4())
        body = store.registration_body(application_id, slots=(SLOT,))
        body["application"]["provider_origins"] = [provider.origin]
        # The render's upstream URL and proxy port are spec fields; they are
        # registered as the ports this host test can serve on, so the env stays
        # exactly the render's.
        body["application"]["server_port"] = fixture.port
        body["application"]["proxy_port"] = closed_port()
        registered = store.operator("POST", "/applications", body)
        assert registered.status_code == 201, registered.text
        recipient = store.register_recipient(application_id)
        database, dsn = store.provision_bundled(application_id, recipient)
        app = SharedApp(
            store=store,
            application_id=application_id,
            installation=store.installation,
            recipient=recipient,
            slots=(SLOT,),
            custody="platform",
            origins=(provider.origin,),
            database=database,
            dsn=dsn,
        )
        member = app.add_member()
        seed, public_key = signing_pair()
        publish_caller_keys(store.installation, public_key)

        spec = SharedApplication.model_validate_json(json.dumps(body["application"]))
        deployment = render_application(spec)[0]
        assert deployment["kind"] == "Deployment"
        (proxy,) = [
            container
            for container in deployment["spec"]["template"]["spec"]["containers"]
            if container["name"] == "proxy"
        ]
        rendered_env = {entry["name"]: entry["value"] for entry in proxy["env"]}
        assert PUBLIC_KEYS_ENV not in rendered_env, "the frozen render carries no verify key"
        assert rendered_env[UPSTREAM_URL_ENV] == fixture.url
        assert rendered_env["CURIE_SHARED_RESOLVER_SOCKET"] == spec.resolver_socket

        key_file = secret_dir / "encryption-key"
        # The worker's key Secret content: one standard base64 private key per line.
        key_file.write_text(base64.b64encode(bytes(recipient.private_key)).decode("ascii") + "\n")
        dsn_file = secret_dir / "database-dsn"
        # The worker's DSN Secret content: the API's base64 sealed DSN, verbatim.
        dsn_file.write_text(str(database["sealed_dsn"]))
        grant_file = secret_dir / "grant-store"
        grant_file.write_text(valkey_url() + "\n")
        relocated = {
            "CURIE_SHARED_ENCRYPTION_KEY_FILE": str(key_file),
            "CURIE_SHARED_DATABASE_DSN_FILE": str(dsn_file),
            GRANT_STORE_FILE_ENV: str(grant_file),
            "CURIE_SHARED_RESOLVER_SOCKET": str(socket_path),
        }
        assert set(relocated) == set(RELOCATED)
        for name in ("CURIE_SHARED_ENCRYPTION_KEY_FILE", "CURIE_SHARED_DATABASE_DSN_FILE"):
            assert rendered_env[name].startswith("/run/"), rendered_env[name]
        env = {**rendered_env, **relocated}
        assert set(env) == set(rendered_env), "only the four mount paths may move"
        assert env[INSTALLATION_ENV] == store.installation
        assert env[APPLICATION_ENV] == application_id
        return RenderedApplication(
            store=store,
            app=app,
            member=member,
            fixture=fixture,
            provider=provider,
            env=env,
            rendered_env=rendered_env,
            seed=seed,
            public_key=public_key,
        )
    except BaseException:
        fixture.close()
        raise


@pytest.fixture
def rendered(fleet: Fleet, provider: FakeProvider) -> Iterator[RenderedApplication]:
    built = _render_application(fleet, provider)
    try:
        yield built
    finally:
        built.close()


def test_entrypoint_starts_with_published_keys_end_to_end(rendered: RenderedApplication) -> None:
    rendered.serving()
    member = rendered.member
    before_upstream = rendered.fixture.count
    before_provider = len(rendered.provider.requests)

    reply = rendered.call("whoami", rendered.token())

    result = _admitted(reply, member)
    assert result["authorization_sha256"] == authorization_digest(f"Bearer {member.text}")
    (seen,) = rendered.fixture.since(before_upstream)
    assert seen.rpc == ("tools/call",)
    assert seen.handle is not None
    # The provider's own record: the member's credential, resolved through the
    # real resolver over the real application role.
    observed = [
        (request.path, request.authorization_sha256)
        for request in rendered.provider.requests[before_provider:]
    ]
    assert observed == [("/v1/whoami", authorization_digest(f"Bearer {member.text}"))]

    # Negative: a token signed by a key no API process published.
    unpublished_seed, _ = signing_pair()
    before_upstream = rendered.fixture.count
    before_provider = len(rendered.provider.requests)
    _refusal(rendered.call("whoami", rendered.token(unpublished_seed)), "invalid")
    assert rendered.fixture.count == before_upstream
    assert len(rendered.provider.requests) == before_provider


def test_entrypoint_refuses_without_keys(rendered: RenderedApplication) -> None:
    _clear(rendered.installation)

    process = rendered.start()
    try:
        _, stderr = process.communicate(timeout=120)
    except subprocess.TimeoutExpired:
        process.kill()
        raise
    assert process.returncode == 1, (process.returncode, stderr[-800:])
    assert "caller_keys_unavailable" in stderr, stderr[-800:]
    assert "Traceback" not in stderr, stderr[-800:]
    assert refuses_connection(rendered.port)

    # Liveness: the same environment starts once a live API process publishes.
    publish_caller_keys(rendered.installation, rendered.public_key)
    rendered.serving()
    _admitted(rendered.call("whoami", rendered.token()), rendered.member)


def test_env_keys_are_dedicated_only(tmp_path: Path) -> None:
    from curie_connector_proxy import shared

    store_file = tmp_path / "grant-store"
    store_file.write_text(valkey_url() + "\n")
    env = {
        INSTALLATION_ENV: "acme-install",
        APPLICATION_ENV: str(uuid.uuid4()),
        PROXY_PORT_ENV: "8480",
        UPSTREAM_URL_ENV: "http://127.0.0.1:18080",
        GRANT_STORE_FILE_ENV: str(store_file),
    }
    config = shared.SharedProxyConfig.from_env(env)  # liveness
    assert not hasattr(config, "public_keys")
    assert PUBLIC_KEYS_ENV in shared.DEDICATED_ONLY_ENV
    _, public_key = signing_pair()
    with pytest.raises(ValueError) as refused:
        shared.SharedProxyConfig.from_env({**env, PUBLIC_KEYS_ENV: public_key})
    assert PUBLIC_KEYS_ENV in str(refused.value)
    assert public_key not in str(refused.value)


def test_rotation_applies_next_request_without_restart(rendered: RenderedApplication) -> None:
    process = rendered.serving()
    pid = process.pid
    k1_seed, k1 = rendered.seed, rendered.public_key
    k2_seed, k2 = signing_pair()

    # A rotation in progress: an API process carries k2 with k1 as previous.
    publish_caller_keys(rendered.installation, k2, k1)
    _admitted(rendered.call("whoami", rendered.token(k1_seed)), rendered.member)
    _admitted(rendered.call("whoami", rendered.token(k2_seed)), rendered.member)

    # The rotation completes: only k2 is published, and k1 ages out of the window.
    publish_caller_keys(rendered.installation, k2)
    _age(rendered.installation, k1, 301)
    before = rendered.fixture.count
    _refusal(rendered.call("whoami", rendered.token(k1_seed)), "invalid")
    assert rendered.fixture.count == before
    _admitted(rendered.call("whoami", rendered.token(k2_seed)), rendered.member)
    # The same process, never restarted.
    assert process.poll() is None
    assert process.pid == pid


def test_read_caller_keys_is_the_published_window(worlds: Worlds) -> None:
    """``read_caller_keys`` through the application's own pool (was ``config.public_keys``)."""

    from curie_connector_proxy import shared

    world = worlds()
    pool = world.resolver.runtime.pool

    def accepted() -> set[bytes]:
        keys = world.resolver._wait(shared.read_caller_keys(pool, world.app.installation))
        return {bytes(key) for key in keys}

    assert shared.CALLER_KEY_WINDOW_S == 300
    assert accepted() == {base64.b64decode(world.public_key)}
    _, other = signing_pair()
    publish_caller_keys(world.app.installation, other)
    assert accepted() == {base64.b64decode(world.public_key), base64.b64decode(other)}
    _age(world.app.installation, other, 301)
    assert accepted() == {base64.b64decode(world.public_key)}
    # Inside the window by one second is still accepted.
    _age(world.app.installation, other, 299)
    assert accepted() == {base64.b64decode(world.public_key), base64.b64decode(other)}


@pytest.mark.parametrize("table", [KEYS_TABLE, HEARTBEATS_TABLE])
def test_key_read_failure_is_store_unavailable(
    worlds: Worlds, sessions: SessionPool, table: str
) -> None:
    world = worlds()
    member = world.member()
    _admitted(world.call(member, "whoami"), member)
    owner = sessions.owner()
    role = quote_ident(world.app.role_name)
    owner.execute(f"REVOKE SELECT ON curie.{table} FROM {role}")
    try:
        before = world.observe()
        world.assert_refused(world.call(member, "whoami"), "store_unavailable", before)
    finally:
        owner.execute(f"GRANT SELECT ON curie.{table} TO {role}")
    # Liveness: the regranted read admits the next call, in the same proxy.
    _admitted(world.call(member, "whoami"), member)


def test_grant_verification_uses_db_keys(worlds: Worlds) -> None:
    world = worlds(gated_tools=("create_item",))
    member = world.member()
    arguments = {"title": "ticket"}
    other_seed, other_public = signing_pair()

    def other_grant() -> str:
        saved, world.seed = world.seed, other_seed
        try:
            token, _ = world.grant(member, "create_item", arguments)
        finally:
            world.seed = saved
        return token

    # A grant signed by a key no API process published is refused.
    before = world.observe()
    reply = world.call(member, "create_item", arguments, grants=[other_grant()])
    world.assert_refused(reply, "grant_required", before)
    # The world's own published key verifies its grant.
    published, _ = world.grant(member, "create_item", arguments)
    _admitted(world.call(member, "create_item", arguments, grants=[published]), member)
    # Once another live API process publishes the other key, a fresh grant
    # signed by it verifies too: the grant key set is the database's.
    publish_caller_keys(world.app.installation, other_public)
    _admitted(world.call(member, "create_item", arguments, grants=[other_grant()]), member)


def test_missing_token_needs_no_db(worlds: Worlds, sessions: SessionPool) -> None:
    world = worlds()
    member = world.member()
    _admitted(world.call(member, "whoami"), member)
    owner = sessions.owner()
    role = quote_ident(world.app.role_name)
    owner.execute(f"ALTER ROLE {role} NOLOGIN")
    try:
        terminate_role_backends(world.app.role_name)
        before = world.observe()
        world.assert_refused(world.call(member, "whoami", tokens=[]), "missing", before)
        # Secondary: the database really is unreachable for a call that needs it.
        before = world.observe()
        world.assert_refused(world.call(member, "whoami"), "store_unavailable", before)
    finally:
        owner.execute(f"ALTER ROLE {role} LOGIN")
    # Recovery, as the admission suite reads it: at most two more exact
    # store_unavailable answers while the pool replaces a dead connection.
    for _ in range(3):
        reply = world.call(member, "whoami")
        if reply.status != 503:
            break
        assert reply.json == expected_refusal_body("store_unavailable"), reply.text
        time.sleep(0.2)
    _admitted(reply, member)
