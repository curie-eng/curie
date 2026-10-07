"""Actual role inventory and selector boundaries, @spec PROTECTED-HOOK-ADMISSION-6/7."""

import inspect

import pytest
from redis.exceptions import NoPermissionError, RedisError, ResponseError

from . import admission_broker as broker_helpers
from .admission_broker import (
    SOURCE,
    client,
    facade,
    install,
    module,
    request,
    safe_error,
    seed,
    snapshot,
)

admission_broker = broker_helpers.admission_broker
admission_service = broker_helpers.admission_service


def test_recipe_closed_roles_preserves_principal_password_and_on_state(admission_broker):
    """@spec PROTECTED-HOOK-ADMISSION-6 PROTECTED-HOOK-LANE-3."""
    b = admission_broker
    acl = module("admission_acl")
    for role in ("enqueue", "verifier"):
        b.command("ACL", "SETUSER", role, "~*", "&*", "+@all", "(+@all ~* &*)")
        before = b.command("ACL", "GETUSER", role)
        rules = acl.admission_acl_rules(role)
        assert type(rules) is tuple and all(type(t) is str for t in rules)
        assert {"-@all", "resetkeys", "resetchannels", "clearselectors"} <= set(rules)
        assert not any(
            t.startswith((">", "#", "<", "!")) or t in {"on", "off", "nopass", "resetpass", "reset"}
            for t in rules
        )
        b.command("ACL", "SETUSER", role, *rules)
        after = b.command("ACL", "GETUSER", role)
        assert after[b"passwords"] == before[b"passwords"]
        assert after[b"flags"] == before[b"flags"]
        conn = client(b, role)
        try:
            assert conn.ping() is True
        finally:
            conn.close()
    for role in ("worker", "source_writer", "", None, True):
        with pytest.raises(ValueError):
            acl.admission_acl_rules(role)


@pytest.mark.parametrize(
    "command",
    [
        ("SET", SOURCE, "forged"),
        ("SET", "protected:control:selection:example", "forged"),
        ("SET", "protected:control:manifest:example", "forged"),
        ("SET", "protected:control:qualification:example", "forged"),
        ("SET", "protected:control:readiness:example", "forged"),
        ("GET", "curie:ordinary"),
        ("SET", "curie:ordinary", "forged"),
        ("XACK", "curie:runs", "group", "1-0"),
        ("XREADGROUP", "GROUP", "group", "consumer", "STREAMS", "curie:runs", ">"),
        ("XAUTOCLAIM", "curie:runs", "group", "consumer", 0, "0-0"),
        ("XGROUP", "CREATE", "curie:runs", "group", "0"),
        ("XDEL", "curie:runs", "1-0"),
        ("XTRIM", "curie:runs", "MAXLEN", 0),
        ("PUBLISH", "channel/example", "value"),
        ("ACL", "LIST"),
        ("CONFIG", "GET", "*"),
        ("INFO", "clients"),
        ("FLUSHDB",),
        ("GET", "protected:admission:unowned:example"),
    ],
)
def test_enqueue_denies_every_nonowned_operation(admission_broker, command):
    """@spec PROTECTED-HOOK-ADMISSION-6/7 PROTECTED-HOOK-LANE-3."""
    b = admission_broker
    acl = module("admission_acl")
    install(b, acl)
    seed(b)
    c = client(b)
    try:
        with pytest.raises(RedisError):
            c.execute_command(*command)
    finally:
        c.close()


@pytest.mark.parametrize(
    "script,key",
    [
        ("return redis.call('SET',KEYS[1],'forged')", SOURCE),
        ("return redis.call('SET',KEYS[1],'forged')", "protected:control:selection:example"),
        ("return redis.call('XACK',KEYS[1],'group','1-0')", "curie:runs"),
        ("return redis.call('GET',KEYS[1])", "curie:ordinary"),
    ],
)
def test_eval_declared_key_selector_does_not_grant_inner_command_authority(
    admission_broker, script, key
):
    """@spec PROTECTED-HOOK-ADMISSION-6/7."""
    b = admission_broker
    acl = module("admission_acl")
    install(b, acl)
    values = seed(b)
    c = client(b)
    before = snapshot(b)
    try:
        with pytest.raises(RedisError):
            c.eval(script, 1, key)
        source = c.eval(
            (
                "local i=redis.call('INFO','server');local t=redis.call('TIME');"
                "return {redis.call('TYPE',KEYS[1]).ok,redis.call('GET',KEYS[1]),"
                "string.match(i,'run_id:([0-9a-f]+)'),t[1]}"
            ),
            1,
            SOURCE,
        )
        assert source[0] == b"string" and source[1] is not None and len(source[2]) == 40
        assert source[1] == b.command("GET", SOURCE) and SOURCE in values
    finally:
        c.close()
    assert snapshot(b) == before


@pytest.mark.parametrize(
    "command",
    [
        ("GET", "curie:runs"),
        ("XRANGE", "curie:runs", "-", "+"),
        ("GET", "protected:admission:intent:example"),
        ("SET", "protected:admission:intent:example", "forged"),
        ("SET", SOURCE, "forged"),
        ("SET", "protected:control:selection:example", "forged"),
        ("SET", "protected:control:manifest:example", "forged"),
        ("SET", "protected:control:qualification:example", "forged"),
        ("EVAL", "return 1", 0),
        ("ACL", "LIST"),
        ("PUBLISH", "channel/example", "value"),
    ],
)
def test_verifier_only_readiness_write(admission_broker, command):
    """@spec PROTECTED-HOOK-ADMISSION-6/7 PROTECTED-HOOK-LANE-3."""
    b = admission_broker
    acl = module("admission_acl")
    install(b, acl)
    seed(b)
    c = client(b, "verifier")
    try:
        assert c.set("protected:control:readiness:example", b"proof") is True
        assert c.get(SOURCE) is not None
        assert c.type(SOURCE) == b"string"
        with pytest.raises(RedisError):
            c.execute_command(*command)
    finally:
        c.close()


def test_missing_inner_info_is_safe_before_intent(admission_broker):
    """@spec PROTECTED-HOOK-ADMISSION-1/4/6/7."""
    b = admission_broker
    r = module("admission_records")
    a = module("atomic_admission")
    acl = module("admission_acl")
    install(b, acl)
    seed(b)
    f = facade(b, a)
    b.command("ACL", "SETUSER", "enqueue", "-info")
    before = snapshot(b)
    with pytest.raises(r.AdmissionUnavailable) as caught:
        f.admit(request(r))
    safe_error(caught.value, b)
    assert snapshot(b) == before


def test_facade_exports_only_closed_operations_and_owns_no_client_lifecycle(admission_broker):
    """@spec PROTECTED-HOOK-ADMISSION-1."""
    b = admission_broker
    a = module("atomic_admission")
    acl = module("admission_acl")
    install(b, acl)
    c = client(b)
    f = a.AtomicAdmission(
        c,
        trusted_manifest=b.manifest(),
        trusted_max_readiness_ms=60000,
        backlog_limit=1,
    )
    assert {name for name in dir(f) if not name.startswith("_")} == {
        "admit",
        "recover",
        "preparing",
    }
    assert list(inspect.signature(a.AtomicAdmission).parameters) == [
        "client",
        "trusted_manifest",
        "trusted_max_readiness_ms",
        "backlog_limit",
    ]
    assert "127.0.0.1" not in repr(f)
    assert c.ping() is True
    c.close()


@pytest.mark.parametrize(
    "field,bad",
    [
        ("backlog_limit", True),
        ("backlog_limit", 0),
        ("backlog_limit", 2147483648),
        ("backlog_limit", "1"),
        ("trusted_max_readiness_ms", True),
        ("trusted_max_readiness_ms", 0),
        ("trusted_max_readiness_ms", 9007199254740992),
        ("trusted_max_readiness_ms", 1.0),
        ("trusted_manifest", "broker_identity"),
        ("trusted_manifest", "manifest_dict"),
        ("trusted_manifest", None),
    ],
)
def test_constructor_invalid_bounds_before_broker_io(admission_broker, field, bad):
    """@spec PROTECTED-HOOK-ADMISSION-1."""
    b = admission_broker
    a = module("atomic_admission")
    r = module("admission_records")
    c = client(b)
    options = dict(
        trusted_manifest=b.manifest(),
        trusted_max_readiness_ms=60000,
        backlog_limit=1,
    )
    if field == "trusted_manifest" and bad is not None:
        # A bare broker identity or a manifest mapping is not the trusted manifest record.
        manifest = b.manifest().as_dict()
        bad = manifest["broker_identity"] if bad == "broker_identity" else manifest
    options[field] = bad
    b.command("ACL", "LOG", "RESET")
    b.command("ACL", "SETUSER", "enqueue", "-info", "-get", "-eval", "-time")
    try:
        with pytest.raises((ValueError, r.AdmissionUnavailable)):
            a.AtomicAdmission(c, **options)
        assert b.command("ACL", "LOG") == []
    finally:
        c.close()


@pytest.mark.parametrize(
    "command",
    [
        ("SET", "curie:runs", "forged"),
        ("DEL", "curie:runs"),
        ("SET", "protected:admission:quota", "forged"),
        ("DEL", "protected:admission:quota"),
        ("ZADD", "protected:admission:intent:cross-product", 1, "forged"),
        ("XADD", "protected:admission:intent:cross-product", "1-0", "payload", "forged"),
    ],
)
@pytest.mark.parametrize("inside_script", [False, True])
def test_enqueue_command_authority_does_not_cross_owned_key_families(
    admission_broker, command, inside_script
):
    """@spec PROTECTED-HOOK-ADMISSION-6/7 PROTECTED-HOOK-LANE-3."""
    b = admission_broker
    acl = module("admission_acl")
    install(b, acl)
    # Absent keys make SET/ZADD/XADD valid writes and DEL a valid zero result;
    # a wrong-type or missing-key error cannot masquerade as an ACL refusal.
    assert b.command("EXISTS", command[1]) == 0
    before = snapshot(b)
    c = client(b)
    try:
        if inside_script:
            with pytest.raises(ResponseError, match="(?i)(permission|noperm)"):
                c.eval(
                    "return redis.call(ARGV[1],KEYS[1],unpack(ARGV,2))",
                    1,
                    command[1],
                    command[0],
                    *command[2:],
                )
        else:
            with pytest.raises(NoPermissionError):
                c.execute_command(*command)
    finally:
        c.close()
    assert snapshot(b) == before
