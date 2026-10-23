"""Shared application mode: start refusals and per-call authorization (ADR 0239, AC1).

"Bundle, identity and request contract" 2 and 3 and "Shared membership" 7:
the caller proxy selects shared mode only by explicit configuration, refuses
to start without a nonblank installation, a resolver for its own application,
a loopback upstream and a grant store, and never reads the dedicated
``admits`` list or gated tools. Every request is decided per call: the
``cct2`` token through ``decide_shared``, the alias header as a selector among
the verified agent's own memberships, and the application, membership and
binding rows read through the application role on every call, never cached
and never rendered into the environment.

Every refusal asserts the exact status and body (``curie_caller`` and
``curie_shared``), that the fixture MCP server received nothing, that the
resolver's admissions counter did not move and that the provider saw nothing;
each sits next to the realistic valid request that must be admitted.

Readings of the plan these tests settle:

1. A tool outside the application policy is ``tool_not_allowed`` whether or
   not the server would know it.
2. An alias of valid shape the agent does not hold is ``not_member``; a
   missing, repeated or malformed alias header is ``alias_required``.
3. The database outage is the application role losing ``LOGIN`` with its
   connections terminated; the first answers after ``LOGIN`` returns may still
   be the exact ``store_unavailable`` while the pool replaces a dead
   connection (the cp2 ``settle`` reading).
"""

from __future__ import annotations

import base64
import http.client
import json
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from _fake_provider import FakeProvider
from _fixture_mcp_server import TOOLS
from _resolver_harness import Fleet, terminate_role_backends
from _shared_connector_support import SessionPool, quote_ident, random_installation
from _shared_proxy_harness import (
    ALIAS_HEADER,
    APPLICATION_ENV,
    CALLER_HEADER,
    DEDICATED_ONLY_ENV,
    GRANT_HEADER,
    GRANT_KEY_PREFIX,
    GRANT_STORE_FILE_ENV,
    INSTALLATION_ENV,
    INVOCATION_HEADER,
    LISTEN_PORT,
    PROXY_PORT_ENV,
    PROXY_WAIT_S,
    PUBLIC_KEYS_ENV,
    REPO,
    SESSION_HEADER,
    UPSTREAM_URL_ENV,
    ProxyReply,
    Worlds,
    agent_name,
    closed_port,
    expected_refusal_body,
    process_env,
    refusal_table,
    refuses_connection,
    signing_pair,
    valkey_url,
    wait_until,
)
from curie_connector_proxy import caller, server
from nacl.signing import SigningKey


@pytest.fixture
def worlds(fleet: Fleet, provider: FakeProvider) -> Iterator[Worlds]:
    built = Worlds(fleet, provider)
    try:
        yield built
    finally:
        built.close()


def _admitted_whoami(reply: ProxyReply, binding: str) -> None:
    result = reply.result
    assert "refused" not in result and "error" not in result, result
    assert result["binding"] == binding, result


# =============================================================================
# Configuration and start
# =============================================================================


def _pure_env(tmp_path: Path) -> dict[str, str]:
    """A valid shared-mode proxy environment (no resolver names; config only)."""

    store_file = tmp_path / "grant-store"
    store_file.write_text(valkey_url() + "\n")
    # cp5: no verify key here. A shared proxy reads its keys from the database
    # (test_shared_proxy_caller_keys.py); the dedicated variable refuses start.
    return {
        INSTALLATION_ENV: random_installation(),
        APPLICATION_ENV: str(uuid.uuid4()),
        PROXY_PORT_ENV: LISTEN_PORT,
        UPSTREAM_URL_ENV: "http://127.0.0.1:18080",
        GRANT_STORE_FILE_ENV: str(store_file),
    }


def _write(tmp_path: Path, name: str, text: str) -> str:
    path = tmp_path / name
    path.write_text(text)
    return str(path)


# (id, variable, value builder: None removes the variable)
_START_REFUSALS: list[tuple[str, str, Callable[[Path], str] | None]] = [
    ("installation-blank", INSTALLATION_ENV, lambda _: ""),
    ("installation-absent", INSTALLATION_ENV, None),
    ("installation-with-space", INSTALLATION_ENV, lambda _: "acme install"),
    ("installation-too-long", INSTALLATION_ENV, lambda _: "a" * 129),
    ("application-not-uuid", APPLICATION_ENV, lambda _: "not-a-uuid-at-all"),
    (
        "application-uppercase",
        APPLICATION_ENV,
        lambda _: "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
    ),
    ("port-zero", PROXY_PORT_ENV, lambda _: "0"),
    ("port-text", PROXY_PORT_ENV, lambda _: "eighty"),
    ("upstream-absent", UPSTREAM_URL_ENV, None),
    ("upstream-localhost-name", UPSTREAM_URL_ENV, lambda _: "http://localhost:18080"),
    ("upstream-remote", UPSTREAM_URL_ENV, lambda _: "http://10.20.30.40:18080"),
    ("upstream-other-loopback", UPSTREAM_URL_ENV, lambda _: "http://127.0.0.2:18080"),
    ("upstream-https", UPSTREAM_URL_ENV, lambda _: "https://127.0.0.1:18080"),
    ("upstream-path", UPSTREAM_URL_ENV, lambda _: "http://127.0.0.1:18080/mcp"),
    ("upstream-no-port", UPSTREAM_URL_ENV, lambda _: "http://127.0.0.1"),
    ("upstream-userinfo", UPSTREAM_URL_ENV, lambda _: "http://someone@127.0.0.1:18080"),
    ("grant-store-absent", GRANT_STORE_FILE_ENV, None),
    ("grant-store-missing-file", GRANT_STORE_FILE_ENV, lambda p: str(p / "absent-store")),
    ("grant-store-directory", GRANT_STORE_FILE_ENV, lambda p: str(p)),
    ("grant-store-blank", GRANT_STORE_FILE_ENV, lambda p: _write(p, "blank", "  \n")),
    (
        "grant-store-not-redis",
        GRANT_STORE_FILE_ENV,
        lambda p: _write(p, "http", "http://user:hunter2-not-a-store@store.invalid/0\n"),
    ),
    # cp5: a well-formed verify key list is dedicated-only and refuses start.
    ("public-keys-present-refuses", PUBLIC_KEYS_ENV, lambda _: signing_pair()[1]),
    *[
        (f"dedicated-{name.lower()}", name, lambda _: '["acme-dev"]')
        for name in DEDICATED_ONLY_ENV
    ],
]


def test_authorize_wire_names_are_the_frozen_render_names() -> None:
    from curie_connector_proxy import shared

    assert shared.SHARED_APPLICATION_ENV == APPLICATION_ENV
    assert shared.INSTALLATION_ENV == INSTALLATION_ENV
    assert shared.PROXY_PORT_ENV == PROXY_PORT_ENV
    assert shared.UPSTREAM_URL_ENV == UPSTREAM_URL_ENV
    assert shared.GRANT_STORE_FILE_ENV == GRANT_STORE_FILE_ENV
    assert server.PUBLIC_KEYS_ENV == PUBLIC_KEYS_ENV
    assert shared.ALIAS_HEADER == ALIAS_HEADER
    assert shared.INVOCATION_HEADER == INVOCATION_HEADER
    assert shared.SESSION_HEADER == SESSION_HEADER
    assert shared.GRANT_KEY_PREFIX == GRANT_KEY_PREFIX
    assert caller.HEADER == CALLER_HEADER
    assert server.GRANT_HEADER == GRANT_HEADER
    assert shared.DEFAULT_SESSION_IDLE_SECONDS == 3600.0
    # Every dedicated-only name is the dedicated proxy's own, the verify-key
    # list included (cp5).
    assert set(shared.DEDICATED_ONLY_ENV) == set(DEDICATED_ONLY_ENV)
    assert set(DEDICATED_ONLY_ENV) == {
        server.PUBLIC_KEYS_ENV,
        server.ADMITS_ENV,
        server.GATED_TOOLS_ENV,
        server.CONNECTOR_ENV,
        server.GRANT_STORE_ENV,
        server.LISTEN_PORT_ENV,
        server.UPSTREAM_PORT_ENV,
        server.UPSTREAM_HOST_ENV,
    }
    # The refusal table is the frozen vector's, and every curie_caller is one
    # of the five the runner already names.
    assert dict(shared.REFUSALS) == refusal_table()
    for code, (_, curie_caller) in refusal_table().items():
        assert curie_caller in caller.REFUSALS, code
        assert shared.refusal_body(code) == expected_refusal_body(code), code
        assert str(shared.SharedRefusal(code)) == code


def test_authorize_mode_is_selected_only_by_the_application_variable(tmp_path: Path) -> None:
    env = _pure_env(tmp_path)
    assert server.shared_mode(env) is True
    # Presence selects shared mode; a blank value then refuses to start.
    assert server.shared_mode({APPLICATION_ENV: ""}) is True
    dedicated = {
        server.LISTEN_PORT_ENV: "8480",
        server.UPSTREAM_PORT_ENV: "18080",
        server.PUBLIC_KEYS_ENV: signing_pair()[1],
        server.ADMITS_ENV: '["acme-dev"]',
        INSTALLATION_ENV: env[INSTALLATION_ENV],
    }
    assert server.shared_mode(dedicated) is False
    assert server.shared_mode({}) is False


def test_authorize_config_reads_the_frozen_render(tmp_path: Path) -> None:
    from curie_connector_proxy import shared

    env = _pure_env(tmp_path)
    config = shared.SharedProxyConfig.from_env(env)
    assert config.installation == env[INSTALLATION_ENV]
    assert config.application == env[APPLICATION_ENV]
    assert config.listen_port == int(LISTEN_PORT)
    assert config.upstream_port == 18080
    # cp5: the config carries no verify key; they are read from the database
    # (asserted in test_shared_proxy_caller_keys.py).
    assert not hasattr(config, "public_keys")
    assert config.grant_store_url == valkey_url()
    assert config.session_idle_seconds == shared.DEFAULT_SESSION_IDLE_SECONDS
    # The store URL carries the Valkey password: never in repr.
    assert valkey_url() not in repr(config)
    # A rediss:// store is accepted as well.
    env[GRANT_STORE_FILE_ENV] = _write(tmp_path, "tls", "rediss://store.invalid:6380/0\n")
    tls = shared.SharedProxyConfig.from_env(env)
    assert tls.grant_store_url == "rediss://store.invalid:6380/0"


@pytest.mark.parametrize(
    ("variable", "value"),
    [(variable, value) for _, variable, value in _START_REFUSALS],
    ids=[case_id for case_id, _, _ in _START_REFUSALS],
)
def test_authorize_config_refuses_to_start(
    tmp_path: Path, variable: str, value: Callable[[Path], str] | None
) -> None:
    from curie_connector_proxy import shared

    env = _pure_env(tmp_path)
    shared.SharedProxyConfig.from_env(env)  # liveness: the unchanged env parses
    if value is None:
        env.pop(variable, None)
        shown = None
    else:
        shown = env[variable] = value(tmp_path)
    with pytest.raises(ValueError) as refused:
        shared.SharedProxyConfig.from_env(env)
    message = str(refused.value)
    assert variable in message, message
    if shown is not None and len(shown) >= 8:
        assert shown not in message, "the refusal echoed the refused value"
    assert "hunter2" not in message


def test_authorize_an_admits_list_cannot_start_shared_mode(tmp_path: Path) -> None:
    """The dedicated admits list can never authorize a shared call: it refuses start."""

    from curie_connector_proxy import shared

    env = _pure_env(tmp_path)
    env[server.ADMITS_ENV] = json.dumps(["acme-dev", "agent-00000000"])
    with pytest.raises(ValueError, match=server.ADMITS_ENV):
        shared.SharedProxyConfig.from_env(env)


def test_authorize_make_shared_app_refuses_without_its_own_resolver(
    worlds: Worlds, fleet: Fleet
) -> None:
    from curie_connector_proxy import shared

    world = worlds()
    with pytest.raises(ValueError):
        shared.make_shared_app(world.config, None)
    other = fleet.application()
    other_resolver = fleet.launch(other)
    with pytest.raises(ValueError):
        shared.make_shared_app(world.config, other_resolver.runtime)
    # Liveness: the world's own runtime serves a member.
    member = world.member()
    _admitted_whoami(world.call(member, "whoami"), member.binding_id)


def test_authorize_entrypoint_refuses_a_blank_installation_and_never_listens(
    tmp_path: Path,
) -> None:
    port = closed_port()
    env = _pure_env(tmp_path)
    env[INSTALLATION_ENV] = ""
    env[PROXY_PORT_ENV] = str(port)
    finished = subprocess.run(
        [sys.executable, "-m", "curie_connector_proxy"],
        env=process_env(env),
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert finished.returncode == 2, (finished.returncode, finished.stderr[-800:])
    assert INSTALLATION_ENV in finished.stderr
    assert "Traceback" not in finished.stderr
    assert refuses_connection(port)


def test_authorize_entrypoint_serves_shared_mode_from_the_render(
    worlds: Worlds, fleet: Fleet
) -> None:
    """Liveness for the refusal above: the same entry point starts and forwards.

    The world published its verify key through the API publisher before this
    process starts (cp5); the environment carries none.
    """

    world = worlds()
    member = world.member()
    directory = fleet.directory()
    (directory / "k").mkdir(mode=0o700)
    port = closed_port()
    env = {
        **world.env,
        "CURIE_SHARED_RESOLVER_SOCKET": str(directory / "k" / "r.sock"),
        PROXY_PORT_ENV: str(port),
    }
    process = subprocess.Popen(
        [sys.executable, "-m", "curie_connector_proxy"],
        env=process_env(env),
        cwd=REPO,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_until(
            lambda: process.poll() is not None or not refuses_connection(port),
            "the shared proxy process to listen",
        )
        assert process.poll() is None, "the shared proxy process exited instead of listening"
        before = world.fixture.count
        body = json.dumps(
            world.message(
                "initialize",
                {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t"}},
            )
        ).encode()
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=PROXY_WAIT_S)
        try:
            connection.putrequest("POST", "/mcp", skip_accept_encoding=True)
            for name, value in world.headers(member):
                connection.putheader(name, value)
            connection.putheader("Content-Length", str(len(body)))
            connection.endheaders(body)
            response = connection.getresponse()
            payload = response.read()
            assert response.status == 200, (response.status, payload[-400:])
            assert response.getheader(SESSION_HEADER)
        finally:
            connection.close()
        (seen,) = world.fixture.since(before)
        assert seen.rpc == ("initialize",)
        assert seen.curie_headers == set(), seen.names
    finally:
        process.terminate()
        try:
            process.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=30)


# =============================================================================
# The caller token and the alias selector
# =============================================================================


def test_authorize_caller_token_refusals_keep_their_codes(worlds: Worlds) -> None:
    world = worlds()
    member = world.member()
    other_key = base64.b64encode(bytes(SigningKey.generate())).decode("ascii")
    world_seed, world.seed = world.seed, other_key
    try:
        foreign_signed = world.token(member)
    finally:
        world.seed = world_seed
    cases: list[tuple[str, list[str], str]] = [
        ("no token", [], "missing"),
        ("garbage", ["cct2.not.valid"], "invalid"),
        ("another signing key", [foreign_signed], "invalid"),
        ("name-only blank installation", [world.token(member, installation="")], "invalid"),
        ("two caller headers", [world.token(member), world.token(member)], "invalid"),
        ("expired", [world.token(member, exp=int(time.time()) - 1)], "expired"),
        (
            "foreign installation",
            [world.token(member, installation=random_installation())],
            "not_admitted",
        ),
    ]
    for label, tokens, code in cases:
        before = world.observe()
        reply = world.call(member, "whoami", tokens=tokens)
        try:
            world.assert_refused(reply, code, before)
        except AssertionError as error:
            raise AssertionError(f"{label}: {error}") from None
    # Liveness: the member's own valid token is admitted.
    _admitted_whoami(world.call(member, "whoami"), member.binding_id)


def test_authorize_alias_header_selects_only_the_agents_own_memberships(
    worlds: Worlds,
) -> None:
    world = worlds()
    member = world.member()
    for label, aliases, code in [
        ("no alias header", [], "alias_required"),
        ("two alias headers", ["primary", "primary"], "alias_required"),
        ("malformed alias", ["Bad_Alias"], "alias_required"),
        ("alias too long", ["a" * 64], "alias_required"),
        ("an alias the agent does not hold", ["secondary"], "not_member"),
    ]:
        before = world.observe()
        reply = world.call(member, "whoami", aliases=aliases)
        try:
            world.assert_refused(reply, code, before)
        except AssertionError as error:
            raise AssertionError(f"{label}: {error}") from None
    _admitted_whoami(world.call(member, "whoami"), member.binding_id)


# =============================================================================
# Membership, policy and binding, read per call
# =============================================================================


def test_authorize_a_non_member_is_refused_whatever_its_token_says(worlds: Worlds) -> None:
    world = worlds()
    member = world.member()
    outsider = world.outsider()
    before = world.observe()
    reply = world.call(
        None,
        "whoami",
        tokens=[world.token(agent_id=outsider, agent=agent_name(member.agent_id))],
        alias="primary",
    )
    world.assert_refused(reply, "not_member", before)

    unbound = world.member_without_binding()
    before = world.observe()
    reply = world.call(None, "whoami", tokens=[world.token(agent_id=unbound)], alias="primary")
    world.assert_refused(reply, "binding_missing", before)

    _admitted_whoami(world.call(member, "whoami"), member.binding_id)


def test_authorize_disabling_the_application_refuses_the_next_call(worlds: Worlds) -> None:
    world = worlds()
    member = world.member()
    _admitted_whoami(world.call(member, "whoami"), member.binding_id)
    world.disable_application()
    before = world.observe()
    world.assert_refused(world.call(member, "whoami"), "application_disabled", before)


def test_authorize_disabling_a_binding_refuses_only_that_member(worlds: Worlds) -> None:
    world = worlds()
    first, second = world.member(), world.member()
    _admitted_whoami(world.call(second, "whoami"), second.binding_id)
    world.disable_binding(second)
    before = world.observe()
    world.assert_refused(world.call(second, "whoami"), "binding_disabled", before)
    _admitted_whoami(world.call(first, "whoami"), first.binding_id)


def test_authorize_dropping_an_alias_refuses_it_while_the_kept_alias_works(
    worlds: Worlds,
) -> None:
    world = worlds()
    member = world.member(
        aliases=[{"alias": "primary", "tools": None}, {"alias": "secondary", "tools": None}]
    )
    _admitted_whoami(world.call(member, "whoami", alias="secondary"), member.binding_id)
    world.set_aliases(member, [{"alias": "primary", "tools": None}])
    before = world.observe()
    world.assert_refused(world.call(member, "whoami", alias="secondary"), "not_member", before)
    _admitted_whoami(world.call(member, "whoami", alias="primary"), member.binding_id)


def test_authorize_narrowing_alias_tools_refuses_the_dropped_tool(worlds: Worlds) -> None:
    world = worlds()
    member = world.member()
    reply = world.call(member, "get_item", {"id": "42"})
    _admitted_whoami(reply, member.binding_id)
    world.set_aliases(member, [{"alias": "primary", "tools": ["whoami"]}])
    before = world.observe()
    world.assert_refused(world.call(member, "get_item", {"id": "42"}), "tool_not_allowed", before)
    _admitted_whoami(world.call(member, "whoami"), member.binding_id)


def test_authorize_a_tool_outside_the_application_policy_is_refused(worlds: Worlds) -> None:
    world = worlds(tools=("whoami", "get_item", "hold"))
    member = world.member()
    for tool in ("echo_headers", "delete_everything"):
        before = world.observe()
        world.assert_refused(world.call(member, tool), "tool_not_allowed", before)
    _admitted_whoami(world.call(member, "whoami"), member.binding_id)


def test_authorize_policy_is_read_per_call_and_never_from_the_environment(
    worlds: Worlds,
) -> None:
    world = worlds()
    first = world.member()
    _admitted_whoami(world.call(first, "whoami"), first.binding_id)
    # A member who joined after the proxy started is served with no restart.
    late = world.member(alias="late-joiner")
    _admitted_whoami(world.call(late, "whoami"), late.binding_id)
    # And a narrowing committed between two calls applies to the very next one.
    world.set_aliases(late, [{"alias": "late-joiner", "tools": ["get_item"]}])
    before = world.observe()
    world.assert_refused(world.call(late, "whoami"), "tool_not_allowed", before)

    # Nothing about membership or tools is in the proxy's environment.
    for name in world.env:
        for word in ("ADMITS", "MEMBER", "ALIAS", "TOOL", "GENERATION", "AGENT"):
            assert word not in name, name
    words: set[str] = set()
    for value in world.env.values():
        words.update(part for part in _split_words(value) if part)
    for forbidden in (*TOOLS, "primary", "late-joiner"):
        assert forbidden not in words, forbidden
    for member in (first, late):
        assert all(member.agent_id not in value for value in world.env.values())


def _split_words(value: str) -> list[str]:
    word = ""
    found: list[str] = []
    for char in value:
        if char.isalnum() or char in "_-.":
            word += char
        else:
            found.append(word)
            word = ""
    found.append(word)
    return found


def test_authorize_an_unavailable_database_fails_closed_with_503(
    worlds: Worlds, sessions: SessionPool
) -> None:
    world = worlds()
    member = world.member()
    _admitted_whoami(world.call(member, "whoami"), member.binding_id)
    owner = sessions.owner()
    role = quote_ident(world.app.role_name)
    owner.execute(f"ALTER ROLE {role} NOLOGIN")
    try:
        terminate_role_backends(world.app.role_name)
        for _ in range(2):
            before = world.observe()
            world.assert_refused(world.call(member, "whoami"), "store_unavailable", before)
    finally:
        owner.execute(f"ALTER ROLE {role} LOGIN")
    # Recovery: at most two more exact store_unavailable answers, then admitted.
    for _ in range(2):
        reply = world.call(member, "whoami")
        if reply.status != 503:
            break
        assert reply.json == expected_refusal_body("store_unavailable"), reply.text
        time.sleep(0.2)
    else:
        reply = world.call(member, "whoami")
    _admitted_whoami(reply, member.binding_id)


# =============================================================================
# Body shape
# =============================================================================


def test_authorize_a_body_over_one_mebibyte_is_refused(worlds: Worlds) -> None:
    world = worlds()
    member = world.member()
    before = world.observe()
    reply = world.call(member, "whoami", {"padding": "x" * (1024 * 1024 + 16)})
    world.assert_refused(reply, "body_too_large", before)
    # Liveness: a large body under the bound is forwarded.
    reply = world.call(member, "whoami", {"padding": "x" * (512 * 1024)})
    _admitted_whoami(reply, member.binding_id)


def test_authorize_a_batch_holds_at_most_one_tool_call(worlds: Worlds) -> None:
    world = worlds()
    member = world.member()
    batch = [
        world.call_message("whoami"),
        world.call_message("get_item", {"id": "7"}),
    ]
    before = world.observe()
    world.assert_refused(world.post(member, batch), "multiple_tool_calls", before)

    # A batch of non-tool requests is forwarded, and gets no handle.
    before = world.observe()
    listing = [world.message("tools/list"), world.message("tools/list")]
    reply = world.post(member, listing)
    assert reply.status == 200, reply.text
    assert [len(item["result"]["tools"]) for item in reply.json] == [len(TOOLS)] * 2
    (seen,) = world.fixture.since(before.upstream)
    assert seen.rpc == ("tools/list", "tools/list")
    assert seen.handle is None
    assert world.resolver.admissions == before.admissions


# CR1-1: the proxy authorizes only what it can read. Each coding is applied
# for real where it compresses; an unknown token labels plain JSON bytes.
ENCODINGS = ("gzip", "deflate", "br", "x-made-up", "gzip, gzip")


def test_authorize_an_encoded_body_is_refused_before_it_is_forwarded(worlds: Worlds) -> None:
    world = worlds(
        tools=("whoami", "get_item", "run_task", "create_item", "hold"),
        run_tools=("run_task",),
        gated_tools=("create_item",),
    )
    member = world.member()
    messages = [
        ("a tool outside the policy", world.call_message("echo_headers")),
        ("a run tool without a pair", world.call_message("run_task")),
        ("a gated tool without a grant", world.call_message("create_item", {"title": "t"})),
        ("an allowed tool", world.call_message("whoami")),
        ("a batch hiding two tool calls", [world.call_message("whoami")] * 2),
        ("a non-tool request", world.message("tools/list")),
    ]
    for encoding in ENCODINGS:
        for label, message in messages:
            before = world.observe()
            reply = world.post_encoded(member, message, encoding)
            try:
                world.assert_refused(reply, "body_encoding_unsupported", before)
            except AssertionError as error:
                raise AssertionError(f"{encoding} / {label}: {error}") from None
    # Liveness: the same calls unencoded, or explicitly identity, are inspected
    # and decided on what they say.
    _admitted_whoami(world.call(member, "whoami"), member.binding_id)
    identity = world.post_encoded(member, world.call_message("whoami"), "identity")
    _admitted_whoami(identity, member.binding_id)
    for tool, arguments, code in [
        ("echo_headers", {}, "tool_not_allowed"),
        ("run_task", {}, "run_required"),
        ("create_item", {"title": "t"}, "grant_required"),
    ]:
        before = world.observe()
        world.assert_refused(world.call(member, tool, arguments), code, before)


def test_authorize_a_dedicated_grant_never_authorizes_a_gated_shared_tool(
    worlds: Worlds,
) -> None:
    world = worlds(gated_tools=("create_item",))
    member = world.member()
    arguments = {"title": "ticket"}
    before = world.observe()
    dedicated = world.dedicated_grant(member, "create_item", arguments)
    reply = world.call(member, "create_item", arguments, grants=[dedicated])
    world.assert_refused(reply, "grant_required", before)
    # Liveness: the shared grant for the same call is admitted.
    shared_grant, _ = world.grant(member, "create_item", arguments)
    reply = world.call(member, "create_item", arguments, grants=[shared_grant])
    _admitted_whoami(reply, member.binding_id)


def test_authorize_a_session_less_request_passes_every_check(worlds: Worlds) -> None:
    """A stateless server never assigns a session; requests without one are served."""

    world = worlds()
    member = world.member()
    before = world.observe()
    reply = world.call(member, "whoami")
    _admitted_whoami(reply, member.binding_id)
    (seen,) = world.fixture.since(before.upstream)
    assert seen.session_id is None
    assert seen.handle is not None
