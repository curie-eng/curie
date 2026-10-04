"""Real closed metadata roles, @spec PROTECTED-HOOK-LANE-3/SOURCE-6/7."""

import importlib
import importlib.util
import json
import os
import secrets
import shutil
import subprocess
import time
from uuid import uuid4

import pytest
from curie_protected_hooks.source_fence import SourceFence, SourceFenceConflict
from redis import Redis
from redis.backoff import NoBackoff
from redis.exceptions import AuthenticationError, RedisError, ResponseError
from redis.retry import Retry

AGENT = "11111111-1111-4111-8111-111111111111"
OPERATION = "22222222-2222-4222-8222-222222222222"
HOOK = "incident"
SOURCE = f"protected:source:{AGENT}:{HOOK}"
CONTROL = "protected:control:runtime:manifest"


class DisposableSecret(str):
    """Redact fixture locals, @spec PROTECTED-HOOK-LANE-3."""

    def __repr__(self):
        """Safe diagnostic value, @spec PROTECTED-HOOK-LANE-3."""
        return "<disposable-secret>"


def safe_failure(message):
    """Suppress secret-bearing exception frames, @spec PROTECTED-HOOK-LANE-3."""
    raise pytest.fail.Exception(message, pytrace=False) from None


def replace_writer_password(admin, password):
    """Sanitize credential-bearing failures, @spec PROTECTED-HOOK-LANE-3."""
    try:
        admin.execute_command(
            "ACL", "SETUSER", "source_writer", "resetpass", DisposableSecret(">" + password)
        )
    except RedisError:
        safe_failure("owned writer password update failed")


def metadata():
    """Missing behavior has an explicit red, @spec PROTECTED-HOOK-LANE-3."""
    name = "curie_protected_hooks.broker_metadata"
    assert importlib.util.find_spec(name) is not None, "broker metadata roles not implemented"
    return importlib.import_module(name)


@pytest.fixture(scope="module")
def broker(tmp_path_factory):
    """Own only this disposable endpoint, @spec PROTECTED-HOOK-LANE-3."""
    if shutil.which("docker") is None:
        if os.environ.get("CI_REQUIRE_DOCKER"):
            pytest.fail("Docker is required for metadata role integration")
        pytest.skip("Docker unavailable for real metadata role integration")
    private = tmp_path_factory.mktemp("broker-metadata")
    private.chmod(0o700)
    config = private / "docker"
    config.mkdir(mode=0o700)
    env = {key: DisposableSecret(value) for key, value in os.environ.items()}
    env["DOCKER_CONFIG"] = str(config)
    env.pop("DOCKER_AUTH_CONFIG", None)
    env.pop("DOCKER_CONTEXT", None)

    def docker(*args):
        try:
            return subprocess.run(
                ["docker", *args], env=env, capture_output=True, text=True, timeout=60
            )
        except (OSError, subprocess.TimeoutExpired):
            safe_failure("owned Docker fixture operation failed")

    inventory = docker("ps", "--format", "{{.ID}}")
    if inventory.returncode:
        if os.environ.get("CI_REQUIRE_DOCKER"):
            pytest.fail("Docker required but unavailable for metadata role integration")
        pytest.skip("Docker daemon unavailable for real metadata role integration")
    passwords = {
        role: DisposableSecret(secrets.token_hex(24))
        for role in ("provisioner", "source_writer", "control_reader")
    }
    acl = private / "users.acl"
    name = "curie-metadata-test-" + uuid4().hex[:12]
    cid = None
    cidfile = private / "container.cid"
    clients = {}
    try:
        try:
            acl.write_text(
                "user default off\n"
                + "user provisioner on >"
                + passwords["provisioner"]
                + " ~* &* +@all\n"
                + "".join(
                    "user " + role + " on >" + passwords[role] + " ~* &* +@all (+@all ~*)\n"
                    for role in ("source_writer", "control_reader")
                )
            )
            acl.chmod(0o600)
        except OSError:
            safe_failure("owned credential file setup failed")
        start = docker(
            "run",
            "-d",
            "--name",
            name,
            "--cidfile",
            str(cidfile),
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "--label",
            "curie.test.owner=" + name,
            "-p",
            "127.0.0.1::6379",
            "-v",
            str(acl) + ":/tmp/users.acl:ro",
            "valkey/valkey:8.1.10-alpine",
            "valkey-server",
            "--aclfile",
            "/tmp/users.acl",
            "--save",
            "",
            "--appendonly",
            "no",
        )
        if start.returncode:
            safe_failure("owned Valkey fixture startup failed")
        cid = start.stdout.strip()
        assert len(cid) == 64 and all(c in "0123456789abcdef" for c in cid)
        binding = docker("port", cid, "6379/tcp")
        assert binding.returncode == 0 and binding.stdout.startswith("127.0.0.1:")
        port = int(binding.stdout.strip().rsplit(":", 1)[1])
        assert port != 6379
        for role, password in passwords.items():
            clients[role] = Redis(
                host="127.0.0.1",
                port=port,
                username=role,
                password=password,
                socket_timeout=2,
                socket_connect_timeout=2,
                retry=Retry(NoBackoff(), 0),
            )
        admin = clients["provisioner"]
        for attempt in range(50):
            try:
                admin.ping()
                break
            except RedisError:
                if attempt == 49:
                    safe_failure("owned Valkey endpoint unavailable")
                time.sleep(0.1)
        assert admin.info("server")["valkey_version"] == "8.1.10"
        yield clients, port
    finally:
        try:
            for client in clients.values():
                client.close()
        finally:
            try:
                owned_cid = cidfile.read_text().strip() if cidfile.exists() else cid
                if owned_cid:
                    assert len(owned_cid) == 64
                    assert all(c in "0123456789abcdef" for c in owned_cid)
                    owned = docker("inspect", owned_cid)
                    assert owned.returncode == 0, "owned Valkey identity unavailable"
                    identity = json.loads(owned.stdout)[0]
                    assert identity["Config"]["Labels"]["curie.test.owner"] == name
                    assert identity["Id"] == owned_cid
                    removed = docker("rm", "-f", owned_cid)
                    assert removed.returncode == 0, "owned Valkey fixture cleanup failed"
            finally:
                acl.unlink(missing_ok=True)
                cidfile.unlink(missing_ok=True)


@pytest.fixture
def roles(broker):
    """Install emitted rules over overprivilege, @spec PROTECTED-HOOK-LANE-3."""
    module = metadata()
    clients, _ = broker
    admin = clients["provisioner"]
    admin.flushdb()
    for role in ("source_writer", "control_reader"):
        admin.execute_command("ACL", "SETUSER", role, "+@all", "~*", "&*", "(+@all ~*)")
        admin.execute_command("ACL", "SETUSER", role, *module.metadata_acl_rules(role))
        assert clients[role].ping() is True  # Enabled state and password survived.
        user = {key: value for key, value in admin.acl_getuser(role).items() if key != "passwords"}
        assert user["flags"] == ["on"]
        assert user["channels"] == []
        assert "allcommands" not in user["flags"]
        assert "~*" not in user["keys"]
        assert all(
            not any("+@all" in value for value in selector) for selector in user["selectors"]
        )
    return module, clients


def test_recipes_are_closed_permissions_only():
    """No identity/credential/configuration recipe, @spec PROTECTED-HOOK-LANE-3."""
    module = metadata()
    for role in ("source_writer", "control_reader"):
        rules = module.metadata_acl_rules(role)
        assert isinstance(rules, tuple) and all(isinstance(rule, str) for rule in rules)
        assert {"-@all", "resetkeys", "resetchannels", "clearselectors"} <= set(rules)
        assert not any(
            rule in {"reset", "on", "off", "nopass"} or rule.startswith((">", "<", "#", "!"))
            for rule in rules
        )
    for role in ("worker", "verifier", "provisioner", "", "SOURCE_WRITER"):
        with pytest.raises(ValueError):
            module.metadata_acl_rules(role)


def test_source_writer_real_cas_and_stale_publication(roles):
    """Existing CAS works under emitted ACL, @spec PROTECTED-HOOK-SOURCE-6/7."""
    _, clients = roles
    fence = SourceFence(clients["source_writer"])
    assert fence.reserve_and_revoke(AGENT, HOOK, 0, OPERATION, 0) == 1
    assert fence.reserve_and_revoke(AGENT, HOOK, 0, OPERATION, 0) == 1
    assert fence.publish_ordinary(AGENT, HOOK, 1, OPERATION, "a" * 64)
    assert fence.read(AGENT, HOOK)["active"]["mode"] == "ordinary"
    new_operation = str(uuid4())
    assert fence.reserve_and_revoke(AGENT, HOOK, 1, new_operation, 1) == 2
    assert fence.read(AGENT, HOOK)["active"] is None
    assert not fence.publish_ordinary(AGENT, HOOK, 1, OPERATION, "a" * 64)
    with pytest.raises(SourceFenceConflict):
        fence.reserve_and_revoke(AGENT, HOOK, 1, str(uuid4()), 1)
    assert fence.publish_ordinary(AGENT, HOOK, 2, new_operation, "b" * 64)
    assert clients["source_writer"].evalsha(
        clients["source_writer"].script_load("return redis.call('GET',KEYS[1])"), 1, SOURCE
    ) == clients["source_writer"].get(SOURCE)


@pytest.mark.parametrize("role", ["source_writer", "control_reader"])
@pytest.mark.parametrize(
    "command",
    [
        ("GET", "protected:admission:payload"),
        ("GET", "curie:payload"),
        ("SET", "protected:control:runtime", "x"),
        ("SET", "protected:admission:x", "x"),
        ("DEL", SOURCE),
        ("ACL", "LIST"),
        ("CONFIG", "GET", "*"),
        ("XADD", "protected:runs", "*", "field", "value"),
        ("XREADGROUP", "GROUP", "g", "c", "STREAMS", "protected:runs", ">"),
        ("XACK", "protected:runs", "g", "1-0"),
        ("XCLAIM", "protected:runs", "g", "c", "0", "1-0"),
        ("PUBLISH", "protected:control:runtime", "x"),
        ("EVAL", "return redis.call('GET',KEYS[1])", 1, "protected:admission:x"),
        ("EVAL", "return redis.call('SET','protected:control:x','x')", 0),
        ("EVAL", "return redis.call('DEL',KEYS[1])", 1, SOURCE),
    ],
)
def test_role_crossing_and_privileged_commands_refused(roles, role, command):
    """Real command and inner-script refusal, @spec PROTECTED-HOOK-LANE-3."""
    _, clients = roles
    with pytest.raises(ResponseError):
        clients[role].execute_command(*command)


def test_reader_reads_metadata_and_live_observation(roles):
    """Operation-only consumer uses live facts, @spec PROTECTED-HOOK-LANE-3/SOURCE-6."""
    module, clients = roles
    clients["provisioner"].set(CONTROL, b"metadata")
    reader = module.AuthorityMetadataReader(clients["control_reader"])
    assert reader.read_source(AGENT, HOOK) == {"floor": 0, "operation_id": None, "active": None}
    assert reader.read_control(CONTROL) == b"metadata"
    assert reader.read_control("protected:control:absent") is None
    SourceFence(clients["source_writer"]).reserve_and_revoke(AGENT, HOOK, 0, OPERATION, 0)
    assert reader.read_source(AGENT, HOOK)["floor"] == 1
    before = clients["provisioner"].time()
    observation = reader.observe()
    after = clients["provisioner"].time()
    assert observation.run_id == clients["provisioner"].info("server")["run_id"]
    assert (
        before[0] * 1000 + before[1] // 1000
        <= observation.now_ms
        <= after[0] * 1000 + after[1] // 1000
    )
    with pytest.raises((AttributeError, TypeError)):
        observation.now_ms = 0
    assert (
        clients["control_reader"].eval("return redis.call('GET',KEYS[1])", 1, CONTROL)
        == b"metadata"
    )
    for command in [
        ("SET", SOURCE, "x"),
        ("EVAL", "return redis.call('SET',KEYS[1],'x')", 1, CONTROL),
        ("SCRIPT", "LOAD", "return 1"),
        ("INFO",),
        ("INFO", "clients"),
        ("INFO", "memory"),
        ("INFO", "all"),
        ("INFO", "default"),
    ]:
        with pytest.raises(ResponseError):
            clients["control_reader"].execute_command(*command)
    for command in [("GET", CONTROL), ("INFO", "server"), ("TIME",)]:
        with pytest.raises(ResponseError):
            clients["source_writer"].execute_command(*command)


@pytest.mark.parametrize(
    "key",
    [
        "protected:source:x",
        "protected:control:",
        "protected:control:x y",
        "protected:control:é",
        "protected:control:" + "a" * 257,
        "protected:control:x\n",
    ],
)
def test_reader_invalid_control_keys_refused(roles, key):
    """Key grammar rejects before broker access, @spec PROTECTED-HOOK-LANE-3."""
    module, clients = roles
    with pytest.raises(ValueError):
        module.AuthorityMetadataReader(clients["control_reader"]).read_control(key)


def test_reader_current_wrongtype_and_unavailable_are_safe(roles):
    """Broker refusal cannot leak authority, @spec PROTECTED-HOOK-LANE-3/SOURCE-6."""
    module, clients = roles
    admin = clients["provisioner"]
    reader = module.AuthorityMetadataReader(clients["control_reader"])
    admin.lpush(CONTROL, "x")
    with pytest.raises(module.BrokerMetadataUnavailable) as caught:
        reader.read_control(CONTROL)
    assert caught.value.__cause__ is None
    admin.lpush(SOURCE, "x")
    with pytest.raises(module.BrokerMetadataUnavailable):
        reader.read_source(AGENT, HOOK)
    admin.execute_command("ACL", "SETUSER", "control_reader", "-info")
    with pytest.raises(module.BrokerMetadataUnavailable) as caught:
        reader.observe()
    assert caught.value.__cause__ is None
    assert "127.0.0.1" not in str(caught.value) and "password" not in str(caught.value).lower()


@pytest.mark.parametrize(
    "run_id,now_ms",
    [
        ("a" * 39, 1),
        ("A" * 40, 1),
        ("g" * 40, 1),
        ("a" * 40, -1),
        ("a" * 40, True),
        ("a" * 40, 9007199254740992),
    ],
)
def test_observation_invalid_values_refused(run_id, now_ms):
    """Canonical bounded observations, @spec PROTECTED-HOOK-LANE-2/3."""
    with pytest.raises(ValueError):
        metadata().BrokerObservation(run_id=run_id, now_ms=now_ms)


def test_ordinary_and_default_authentication_refused(broker):
    """Independent broker rejects ordinary identity, @spec PROTECTED-HOOK-LANE-3."""
    _, port = broker
    for username, password in [
        (None, None),
        ("default", "ordinary-disposable"),
        ("ordinary", "ordinary-disposable"),
    ]:
        client = Redis(
            host="127.0.0.1",
            port=port,
            username=username,
            password=password,
            retry=Retry(NoBackoff(), 0),
        )
        try:
            with pytest.raises(AuthenticationError):
                client.ping()
        finally:
            client.close()


def test_password_replacement_requires_owned_session_termination(roles):
    """Retained session revocation is real, @spec PROTECTED-HOOK-LANE-3."""
    _, clients = roles
    admin = clients["provisioner"]
    writer = clients["source_writer"]
    old_secret = writer.connection_pool.connection_kwargs["password"]
    replacement = DisposableSecret(secrets.token_hex(24))
    try:
        assert writer.ping()
        replace_writer_password(admin, replacement)
        assert writer.ping()  # Password replacement alone retains authenticated sessions.
        assert admin.client_kill_filter(user="source_writer") >= 1
        writer.close()
        with pytest.raises(AuthenticationError):
            writer.ping()
        assert clients["control_reader"].ping()
    finally:
        replace_writer_password(admin, old_secret)
        writer.close()
