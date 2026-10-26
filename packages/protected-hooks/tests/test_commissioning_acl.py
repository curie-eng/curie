"""Commissioning broker roles, measured on the pinned Valkey, @spec PROTECTED-COMMISSIONING-1.

The expected authority below is written from the specification's principals
table, independently of the recipe module. Every commissioning role is measured
against every key family with ``ACL DRYRUN`` and with real calls, including Lua
scripts that declare a key and scripts that reach an undeclared key. ``ACL
DRYRUN`` of ``EVAL`` checks only the declared keys; the commands a script runs
are checked again against the caller, so the real call is the authority.

The unchanged production enqueue, worker, verifier, source writer and control
reader are installed from their own recipes and measured to read and write
nothing under ``protected:commissioning:``. Fixture IDs are placeholders.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import secrets
from typing import Any

import pytest
from curie_protected_hooks.admission_acl import admission_acl_rules
from curie_protected_hooks.broker_metadata import metadata_acl_rules
from curie_protected_hooks.execution_namespace import derive_lane_namespace
from curie_protected_hooks.worker_acl import worker_acl_rules
from redis.exceptions import NoPermissionError, ResponseError

from . import worker_broker as wb
from .test_commissioning_records import permit_value

pworker_service = wb.pworker_service

ROLES = (
    "provisioner",
    "issuer",
    "control",
    "observer",
    "candidate_enqueue",
    "candidate_worker",
    "qualification_publisher",
)
CANDIDATES = ("candidate_enqueue", "candidate_worker")
# The fixture manifest's qualification_id; the publisher is confined to it.
QUALIFICATION_ID = "44444444-4444-4444-8444-444444444444"
FOREIGN_QUALIFICATION_ID = "88888888-8888-4888-8888-888888888888"


def options(role: str) -> dict[str, Any]:
    if role in CANDIDATES:
        return {"permit": permit()}
    if role == "qualification_publisher":
        return {"qualification_id": QUALIFICATION_ID}
    return {}


def module() -> Any:
    name = "curie_protected_hooks.commissioning_acl"
    assert importlib.util.find_spec(name) is not None, "commissioning ACL recipes not implemented"
    return importlib.import_module(name)


def permit(**changes: Any) -> Any:
    from curie_protected_hooks.commissioning_records import parse_commissioning_permit

    value = permit_value()
    value.update(changes)
    return parse_commissioning_permit(json.dumps(value).encode())


def lane_digest(value: dict[str, Any]) -> str:
    members = [
        value["runtime_id"],
        value["runtime_generation"],
        value["campaign_id"],
        value["campaign_generation"],
    ]
    encoded = json.dumps(members, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(encoded).hexdigest()


# --- Pure recipe and naming contract -------------------------------------------------


def test_lane_names_derive_from_runtime_and_campaign_identity() -> None:
    api = module()
    record = permit()
    lane = api.derive_commissioning_lane(record)
    digest = lane_digest(permit_value())
    assert lane.prefix == "protected:commissioning:lane:" + digest
    assert lane.stream == lane.prefix + ":runs"
    assert lane.dead == lane.prefix + ":dead"
    assert lane.group == "commissioning-" + digest
    assert api.derive_commissioning_lane(permit()) == lane
    # Limits, time and scope are not lane identity; runtime and campaign identity are.
    assert api.derive_commissioning_lane(permit(expires_at_ms="3000")) == lane
    for member, value in (
        ("campaign_id", "77777777-7777-4777-8777-777777777777"),
        ("campaign_generation", "2"),
        ("runtime_generation", "9"),
    ):
        assert api.derive_commissioning_lane(permit(**{member: value})).prefix != lane.prefix


def test_lane_names_never_overlap_production_names() -> None:
    from curie_protected_hooks.authority_records import parse_manifest

    from .test_authority_records import example_records

    lane = module().derive_commissioning_lane(permit())
    production = derive_lane_namespace(
        parse_manifest(json.dumps(example_records()["manifest"]).encode())
    )
    for name in (lane.prefix, lane.stream, lane.dead):
        assert not name.startswith(("protected:lane:", "protected:control:", "curie:"))
        assert not any(character in name for character in "*?[]\\")
    assert lane.stream != production.stream
    assert lane.group != production.group


@pytest.mark.parametrize("value", [None, "permit", b"{}", permit_value(), object()])
def test_lane_derivation_requires_a_closed_permit(value: Any) -> None:
    with pytest.raises(ValueError):
        module().derive_commissioning_lane(value)


@pytest.mark.parametrize(
    "role", ["", "worker", "enqueue", "verifier", "candidate", "Issuer", 1, None]
)
def test_unknown_roles_refuse(role: Any) -> None:
    with pytest.raises(ValueError):
        module().commissioning_acl_rules(role)


@pytest.mark.parametrize("role", CANDIDATES)
@pytest.mark.parametrize("value", [None, permit_value(), b"{}"])
def test_candidate_roles_require_the_campaign_permit(role: str, value: Any) -> None:
    with pytest.raises(ValueError):
        module().commissioning_acl_rules(role, permit=value)


@pytest.mark.parametrize(
    "role", ["provisioner", "issuer", "control", "observer", "qualification_publisher"]
)
def test_installation_roles_take_no_campaign_permit(role: str) -> None:
    with pytest.raises(ValueError):
        module().commissioning_acl_rules(role, permit=permit(), **options(role))


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        "not-a-uuid",
        "ABCDEFAB-ABCD-4BCD-8BCD-ABCDEFABCDEF",
        1,
        "*",
        QUALIFICATION_ID + ":*",
    ],
)
def test_qualification_publisher_requires_one_qualification_id(value: Any) -> None:
    """Design decision 8: the publisher's keys come from one closed UUID, never a pattern."""
    with pytest.raises(ValueError):
        module().commissioning_acl_rules("qualification_publisher", qualification_id=value)


@pytest.mark.parametrize("role", [r for r in ROLES if r != "qualification_publisher"])
def test_only_the_publisher_takes_a_qualification_id(role: str) -> None:
    with pytest.raises(ValueError):
        module().commissioning_acl_rules(
            role, qualification_id=QUALIFICATION_ID, permit=options(role).get("permit")
        )


def test_recipes_are_closed_distinct_and_start_from_reset() -> None:
    api = module()
    recipes = {role: api.commissioning_acl_rules(role, **options(role)) for role in ROLES}
    assert len(set(recipes.values())) == len(ROLES)
    for rules in recipes.values():
        assert type(rules) is tuple
        assert rules[:4] == ("-@all", "resetkeys", "resetchannels", "clearselectors")
        assert not any("&" in rule or "allkeys" in rule or "+@" in rule for rule in rules)
        assert not any(rule.startswith(("~", "%")) for rule in rules)
    # A later campaign's candidates never inherit an earlier campaign's lane.
    other = permit(campaign_generation="2")
    for role in CANDIDATES:
        assert api.commissioning_acl_rules(role, permit=other) != recipes[role]


# --- Design decision 9: candidate templates ------------------------------------------


@pytest.mark.parametrize("role", CANDIDATES)
def test_candidate_templates_carry_the_lane_only_as_the_placeholder(role: str) -> None:
    api = module()
    template = api.commissioning_acl_template(role)
    assert api.LANE_PLACEHOLDER == "{lane}"
    assert any(api.LANE_PLACEHOLDER in rule for rule in template)
    assert not any("protected:commissioning:lane:" in rule for rule in template)
    lane = api.derive_commissioning_lane(permit())
    rules = api.commissioning_acl_rules(role, permit=permit())
    assert api.instantiate_candidate_rules(role, lane) == rules
    # The template is the instantiated recipe with exactly the lane prefix abstracted.
    assert tuple(rule.replace(lane.prefix, "{lane}") for rule in rules) == template


@pytest.mark.parametrize("role", CANDIDATES)
def test_candidate_template_is_the_same_for_every_campaign(role: str) -> None:
    api = module()
    first, other = permit(), permit(campaign_generation="2")
    assert api.commissioning_acl_rules(role, permit=first) != api.commissioning_acl_rules(
        role, permit=other
    )
    template = api.commissioning_acl_template(role)
    digest = hashlib.sha256(json.dumps(list(template)).encode()).hexdigest()
    for record in (first, other):
        lane = api.derive_commissioning_lane(record)
        instantiated = api.instantiate_candidate_rules(role, lane)
        assert tuple(rule.replace(lane.prefix, "{lane}") for rule in instantiated) == template
    assert hashlib.sha256(json.dumps(list(template)).encode()).hexdigest() == digest


@pytest.mark.parametrize("role", CANDIDATES)
def test_check_accepts_only_the_current_permits_instantiation(role: str) -> None:
    api = module()
    current = permit()
    rules = api.commissioning_acl_rules(role, permit=current)
    api.check_candidate_rules(role, rules, permit=current)
    other_lane = api.commissioning_acl_rules(role, permit=permit(campaign_generation="2"))
    variants = [
        other_lane,
        api.commissioning_acl_template(role),
        (*rules, "+keys"),
        rules[:-1],
        (rules[1], rules[0], *rules[2:]),
        list(rules),
        tuple(rule.replace("+get", "+set") for rule in rules),
    ]
    for variant in variants:
        with pytest.raises(ValueError):
            api.check_candidate_rules(role, variant, permit=current)
    other_role = next(r for r in CANDIDATES if r != role)
    with pytest.raises(ValueError):
        api.check_candidate_rules(
            role, api.commissioning_acl_rules(other_role, permit=current), permit=current
        )


@pytest.mark.parametrize("role", ["observer", "issuer", "worker", "", None])
def test_templates_exist_only_for_candidates(role: Any) -> None:
    with pytest.raises(ValueError):
        module().commissioning_acl_template(role)


def test_instantiation_refuses_a_production_or_malformed_lane() -> None:
    from curie_protected_hooks.authority_records import parse_manifest
    from curie_protected_hooks.execution_namespace import LaneNamespace

    from .test_authority_records import example_records

    api = module()
    manifest = parse_manifest(json.dumps(example_records()["manifest"]).encode())
    production = derive_lane_namespace(manifest)
    good = api.derive_commissioning_lane(permit())
    for lane in (
        production,
        LaneNamespace("{lane}", good.group, "{lane}:runs", good.dead),
        LaneNamespace(good.prefix, good.group, good.prefix + ":dead", good.dead),
        LaneNamespace(good.prefix + ":x", good.group, good.prefix + ":x:runs", good.dead),
        good.prefix,
    ):
        with pytest.raises(ValueError):
            api.instantiate_candidate_rules("candidate_worker", lane)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        api.check_candidate_rules("candidate_worker", (), permit=None)  # type: ignore[arg-type]


# --- Measured matrix on the pinned Valkey ---------------------------------------------

FOREIGN_PERMIT = {"campaign_id": "77777777-7777-4777-8777-777777777777"}


def families() -> dict[str, tuple[str, str]]:
    """Key family -> (concrete key, value type) for one owned and one foreign campaign."""
    own = module().derive_commissioning_lane(permit())
    foreign = module().derive_commissioning_lane(permit(**FOREIGN_PERMIT))
    return {
        "permit": ("protected:commissioning:permit:example", "string"),
        "journal": ("protected:commissioning:journal:example", "string"),
        "floor": ("protected:commissioning:journal:floor:example", "string"),
        "reservation": ("protected:commissioning:reservation:example", "string"),
        "ownership": ("protected:commissioning:ownership:example", "string"),
        "admission": ("protected:commissioning:admission:example", "string"),
        "acceptance": ("protected:commissioning:acceptance:example", "string"),
        "unknown_commissioning": ("protected:commissioning:other:example", "string"),
        "lane_stream": (own.stream, "stream"),
        "lane_key": (own.prefix + ":worker:lock:example", "string"),
        "lane_dead": (own.dead, "stream"),
        "foreign_lane_stream": (foreign.stream, "stream"),
        "foreign_lane_key": (foreign.prefix + ":worker:lock:example", "string"),
        "selection": ("protected:control:selection:example", "string"),
        "manifest": ("protected:control:manifest:example", "string"),
        "qualification": ("protected:control:qualification:example", "string"),
        "own_qualification": (
            "protected:control:qualification:" + QUALIFICATION_ID + ":1",
            "string",
        ),
        "foreign_qualification": (
            "protected:control:qualification:" + FOREIGN_QUALIFICATION_ID + ":1",
            "string",
        ),
        "readiness": ("protected:control:readiness:example", "string"),
        "source": ("protected:source:example", "string"),
        "production_admission": ("protected:admission:intent:example", "string"),
        "production_lane": ("protected:lane:worker:example", "string"),
        "production_stream": ("curie:runs", "stream"),
    }


COMMISSIONING = {
    "permit",
    "journal",
    "floor",
    "reservation",
    "ownership",
    "admission",
    "acceptance",
    "unknown_commissioning",
    "lane_stream",
    "lane_key",
    "lane_dead",
    "foreign_lane_stream",
    "foreign_lane_key",
}
CONTROL = {
    "selection",
    "manifest",
    "qualification",
    "own_qualification",
    "foreign_qualification",
    "readiness",
}
LANE = {"lane_stream", "lane_key", "lane_dead"}

# The specification's principals table, as direct authority per command family.
# "eval" lists the keys a script may declare; what the script then runs is
# checked against the same GET/SET authority.
EXPECTED: dict[str, dict[str, set[str]]] = {
    "provisioner": {
        "get": CONTROL | {"journal", "floor"},
        "set": {"selection", "manifest"},
        "eval": {"selection", "manifest"},
    },
    "issuer": {
        "get": CONTROL | {"permit", "journal", "floor", "acceptance"},
        "setnx": {"permit", "acceptance"},
    },
    "control": {
        "get": CONTROL | {"permit", "journal", "floor", "reservation", "ownership", "admission"},
        "set": {"journal", "floor", "reservation", "ownership"},
        "setnx": {"admission"},
        "eval": {"permit", "journal", "floor", "reservation", "ownership", "admission"},
    },
    "candidate_enqueue": {
        "get": CONTROL | {"permit", "journal", "floor"},
        "xadd": {"lane_stream"},
    },
    # Design decision 7: reads ACL users only, never a key.
    "observer": {"get": set()},
    # Design decision 8: one manifest's Qualification keys, written once, nothing else.
    "qualification_publisher": {
        "get": {"own_qualification"},
        "setnx": {"own_qualification"},
        "type": set(),
    },
    "candidate_worker": {
        "get": CONTROL | {"permit", "journal", "floor", "admission"} | LANE,
        "set": LANE,
        "del": LANE,
        "xadd": LANE,
        "xrange": LANE,
        "eval": LANE,
    },
}
HAS_EVAL = {role for role, rights in EXPECTED.items() if rights.get("eval")}

READ_SCRIPT = "return redis.call('GET', KEYS[1])"
WRITE_SCRIPT = "return redis.call('SET', KEYS[1], ARGV[1])"
UNDECLARED_READ = "return redis.call('GET', ARGV[1])"
UNDECLARED_WRITE = "return redis.call('SET', ARGV[1], ARGV[2])"
OPERATIONS = (
    "get",
    "set",
    "setnx",
    "del",
    "xadd",
    "xrange",
    "eval_get",
    "eval_set",
    "eval0_get",
    "eval0_set",
)


def command(operation: str, key: str) -> tuple[Any, ...]:
    return {
        "get": ("GET", key),
        "set": ("SET", key, "probe"),
        "setnx": ("SETNX", key, "probe"),
        "del": ("DEL", key),
        "xadd": ("XADD", key, "*", "probe", "1"),
        "xrange": ("XRANGE", key, "-", "+"),
        "eval_get": ("EVAL", READ_SCRIPT, 1, key),
        "eval_set": ("EVAL", WRITE_SCRIPT, 1, key, "probe"),
        "eval0_get": ("EVAL", UNDECLARED_READ, 0, key),
        "eval0_set": ("EVAL", UNDECLARED_WRITE, 0, key, "probe"),
    }[operation]


def expected(
    rights: dict[str, set[str]], has_eval: bool, operation: str, family: str
) -> tuple[bool, bool]:
    """(ACL DRYRUN outcome, real outcome) for one call."""
    if operation in ("get", "set", "setnx", "del", "xadd", "xrange"):
        allowed = family in rights.get(operation, set())
        return allowed, allowed
    inner = family in rights.get("get" if operation.endswith("get") else "set", set())
    outer = has_eval and (operation.startswith("eval0") or family in rights.get("eval", set()))
    return outer, outer and inner


def seed(broker: Any, key: str, kind: str) -> None:
    broker.command("DEL", key)
    if kind == "stream":
        broker.command("XADD", key, "1-1", "seed", "1")
    else:
        broker.command("SET", key, "seed")


def dryrun(broker: Any, username: str, call: tuple[Any, ...]) -> bool:
    return broker.command("ACL", "DRYRUN", username, *call) in (b"OK", "OK")


def real(client: Any, call: tuple[Any, ...]) -> bool:
    try:
        client.execute_command(*call)
    except NoPermissionError:
        return False
    except ResponseError as error:
        # A script's inner denial surfaces as a script error naming the ACL refusal.
        text = str(error)
        if "NOPERM" in text or "no permissions" in text or "No permissions" in text:
            return False
        # The ACL check passed; the command then met a value of another type.
        if "WRONGTYPE" not in text:
            raise
    return True


@pytest.fixture
def principals(pworker_service):
    """One installed principal per role, from the product recipes only."""
    pworker_service.command("FLUSHDB")
    installed = {}
    try:
        for role in ROLES:
            rules = module().commissioning_acl_rules(role, **options(role))
            installed[role] = wb.WorkerPrincipal(pworker_service, role=role, rules=rules)
            installed[role].install()
        manifest = pworker_service.manifest()
        production = {
            "production_enqueue": admission_acl_rules("enqueue"),
            "production_enqueue_trusted": admission_acl_rules("enqueue", trusted_manifest=manifest),
            "production_verifier": admission_acl_rules("verifier"),
            "production_worker": worker_acl_rules("worker"),
            "production_source_writer": metadata_acl_rules("source_writer"),
            "production_control_reader": metadata_acl_rules("control_reader"),
        }
        for role, rules in production.items():
            installed[role] = wb.WorkerPrincipal(
                pworker_service, role=role.replace("_", "-"), rules=rules
            )
            installed[role].install()
        yield installed
    finally:
        for principal in installed.values():
            principal.remove()
        pworker_service.command("FLUSHDB")


def measure(
    broker: Any, principal: Any, operations: tuple[str, ...], keys: dict[str, tuple[str, str]]
):
    """Yield (operation, family, dryrun outcome, real outcome) for every call."""
    client = principal.raw(protocol=3)
    try:
        for family, (key, kind) in keys.items():
            for operation in operations:
                seed(broker, key, kind)
                call = command(operation, key)
                yield (
                    operation,
                    family,
                    dryrun(broker, principal.username, call),
                    real(client, call),
                )
    finally:
        client.close()


@pytest.mark.parametrize("role", ROLES)
def test_commissioning_role_matrix_matches_the_principals_table(
    pworker_service, principals, role
) -> None:
    """Each role x each key family x each command, DRYRUN and real calls."""
    keys = families()
    rights = EXPECTED[role]
    mismatches = []
    for operation, family, dry, actual in measure(
        pworker_service, principals[role], OPERATIONS, keys
    ):
        want_dry, want_real = expected(rights, role in HAS_EVAL, operation, family)
        if (dry, actual) != (want_dry, want_real):
            mismatches.append((operation, family, dry, actual, want_dry, want_real))
    assert mismatches == []


@pytest.mark.parametrize(
    "role",
    [
        "production_enqueue",
        "production_enqueue_trusted",
        "production_verifier",
        "production_worker",
        "production_source_writer",
        "production_control_reader",
    ],
)
def test_production_principals_reach_nothing_under_commissioning(
    pworker_service, principals, role
) -> None:
    """Unchanged production rows: no read, write, script read or script write."""
    keys = {family: value for family, value in families().items() if family in COMMISSIONING}
    reached = [
        (operation, family, dry, actual)
        for operation, family, dry, actual in measure(
            pworker_service, principals[role], OPERATIONS, keys
        )
        # DRYRUN of a keyless EVAL only says the script may start; the real call decides.
        if actual or (dry and not operation.startswith("eval0"))
    ]
    client = principals[role].raw(protocol=3)
    try:
        for family, (key, kind) in keys.items():
            seed(pworker_service, key, kind)
            for call in (("TYPE", key), ("EXISTS", key)):
                dry = dryrun(pworker_service, principals[role].username, call)
                actual = real(client, call)
                if dry or actual:
                    reached.append((call[0].lower(), family, dry, actual))
    finally:
        client.close()
    assert reached == []


def test_type_and_exists_follow_the_read_authority(pworker_service, principals) -> None:
    """Metadata probes reveal nothing outside a role's read families."""
    keys = families()
    for role in ROLES:
        rights = EXPECTED[role]
        typed = rights.get("type", rights["get"] | rights.get("xadd", set()))
        for family, (key, kind) in keys.items():
            seed(pworker_service, key, kind)
            username = principals[role].username
            assert dryrun(pworker_service, username, ("TYPE", key)) == (family in typed), (
                role,
                family,
            )
            assert dryrun(pworker_service, username, ("EXISTS", key)) == (
                role == "candidate_worker" and family in LANE
            ), (role, family)


ADMIN = (
    ("CONFIG", "SET", "maxmemory", "0"),
    ("CONFIG", "GET", "maxmemory"),
    ("FLUSHDB",),
    ("FLUSHALL",),
    ("KEYS", "*"),
    ("FUNCTION", "LIST"),
    ("FCALL", "probe", 0),
    ("SCRIPT", "FLUSH"),
    ("PUBLISH", "probe", "1"),
    ("SUBSCRIBE", "probe"),
    ("MONITOR",),
    ("DEBUG", "SLEEP", "0"),
    ("SHUTDOWN",),
    ("SAVE",),
    ("REPLICAOF", "NO", "ONE"),
    ("MODULE", "LIST"),
    ("RENAME", "protected:commissioning:permit:a", "protected:commissioning:permit:b"),
    ("COPY", "protected:commissioning:permit:a", "protected:commissioning:permit:b"),
    ("ACL", "LOG"),
    ("ACL", "LIST"),
    # The provisioner disables principals and kills their sessions; it never
    # deletes a user, reads stored rules or password hashes, or lists clients.
    ("ACL", "DELUSER", "probe-user"),
    ("ACL", "USERS"),
    ("CLIENT", "LIST"),
    ("CLIENT", "PAUSE", "1"),
)
PROVISIONER_ONLY = (
    ("ACL", "SETUSER", "probe-user", "off"),
    ("CLIENT", "KILL", "USER", "probe-user"),
)


@pytest.mark.parametrize("role", ROLES)
def test_administrative_commands_stay_closed(pworker_service, principals, role) -> None:
    username = principals[role].username
    for call in ADMIN:
        assert not dryrun(pworker_service, username, call), (role, call[0])
    for call in PROVISIONER_ONLY:
        assert dryrun(pworker_service, username, call) == (role == "provisioner"), (role, call)
    assert dryrun(pworker_service, username, ("ACL", "GETUSER", "probe-user")) == (
        role == "observer"
    )
    assert dryrun(pworker_service, username, ("PING",))
    for call in (("ACL", "WHOAMI"), ("TIME",)):
        assert dryrun(pworker_service, username, call) == (
            role not in ("observer", "qualification_publisher")
        ), (role, call)
    assert dryrun(pworker_service, username, ("INFO", "server")) == (role != "observer")
    assert dryrun(pworker_service, username, ("SCAN", "0")) == (role == "candidate_worker")


def test_provisioner_reads_no_user_or_client_inventory(pworker_service, principals) -> None:
    """Real calls, not only DRYRUN: the provisioner's administration is SETUSER and KILL."""
    target = principals["candidate_worker"].username
    client = principals["provisioner"].raw(protocol=3)
    try:
        for call in (
            ("ACL", "GETUSER", target),
            ("ACL", "DELUSER", target),
            ("ACL", "USERS"),
            ("ACL", "LIST"),
            ("CLIENT", "LIST"),
        ):
            assert not real(client, call), call
        assert target in {
            u.decode() if isinstance(u, bytes) else u
            for u in pworker_service.command("ACL", "USERS")
        }
    finally:
        client.close()


def test_observer_reads_principal_state_and_nothing_else(pworker_service, principals) -> None:
    """Design decision 7: password hashes and enabled state through ACL GETUSER only."""
    pworker_service.command("FLUSHDB")
    target = principals["candidate_worker"]
    key = "protected:commissioning:permit:example"
    pworker_service.command("SET", key, "seed")
    client = principals["observer"].raw(protocol=3)

    def state() -> tuple[set[str], bool]:
        reply = client.execute_command("ACL", "GETUSER", target.username)
        reply = {(k.decode() if isinstance(k, bytes) else k): v for k, v in dict(reply).items()}
        flags = {f.decode() if isinstance(f, bytes) else f for f in reply["flags"]}
        hashes = {h.decode() if isinstance(h, bytes) else h for h in reply["passwords"]}
        return hashes, "on" in flags

    try:
        # Stored hashes are set directly; no password value is hashed here.
        first, second = secrets.token_hex(32), secrets.token_hex(32)
        pworker_service.command("ACL", "SETUSER", target.username, "resetpass", "#" + first)
        hashes, enabled = state()
        assert hashes == {first} and enabled
        pworker_service.command("ACL", "SETUSER", target.username, "off", "#" + second)
        changed, enabled = state()
        assert not enabled and changed == {first, second}
        for call in (
            ("ACL", "SETUSER", target.username, "on"),
            ("ACL", "SETUSER", principals["observer"].username, "+@all", "~*"),
            ("ACL", "DELUSER", target.username),
            ("ACL", "USERS"),
            ("ACL", "LIST"),
            ("ACL", "LOG"),
            ("ACL", "WHOAMI"),
            ("CLIENT", "KILL", "USER", target.username),
            ("CLIENT", "LIST"),
            ("GET", key),
            ("TYPE", key),
            ("EXISTS", key),
            ("SCAN", "0"),
            ("EVAL", UNDECLARED_READ, 0, key),
            ("PUBLISH", "probe", "1"),
            ("SUBSCRIBE", "probe"),
            ("TIME",),
            ("INFO", "server"),
        ):
            assert not real(client, call), call
        assert state()[1] is False
    finally:
        client.close()


def test_identities_authenticate_as_themselves(pworker_service, principals) -> None:
    """Spec refusal step 3 needs ACL WHOAMI on each commissioning identity."""
    for role in (role for role in ROLES if role not in ("observer", "qualification_publisher")):
        client = principals[role].raw(protocol=3)
        try:
            name = client.execute_command("ACL", "WHOAMI")
            assert (name.decode() if isinstance(name, bytes) else name) == principals[role].username
        finally:
            client.close()


def test_issuer_writes_each_record_once(pworker_service, principals) -> None:
    """The issuer holds SETNX, never SET: a permit or acceptance cannot be replaced."""
    pworker_service.command("FLUSHDB")
    client = principals["issuer"].raw(protocol=3)
    try:
        for key in (
            "protected:commissioning:permit:once",
            "protected:commissioning:acceptance:once",
        ):
            assert client.execute_command("SETNX", key, "first") == 1
            assert client.execute_command("SETNX", key, "second") == 0
            for call in (("SET", key, "second"), ("SET", key, "second", "NX"), ("DEL", key)):
                with pytest.raises(NoPermissionError):
                    client.execute_command(*call)
            assert pworker_service.command("GET", key) in (b"first", "first")
    finally:
        client.close()


def test_publisher_writes_its_qualification_once(pworker_service, principals) -> None:
    """Design decision 8: SETNX on its own Qualification key; never SET, DEL or a script."""
    pworker_service.command("FLUSHDB")
    client = principals["qualification_publisher"].raw(protocol=3)
    own = "protected:control:qualification:" + QUALIFICATION_ID + ":1"
    try:
        # HELLO AUTH named the principal: the session exists only as that user.
        assert client.execute_command("SETNX", own, "first") == 1
        assert client.execute_command("SETNX", own, "second") == 0
        assert client.execute_command("GET", own) in (b"first", "first")
        for call in (
            ("SET", own, "second"),
            ("SET", own, "second", "NX"),
            ("DEL", own),
            ("PEXPIRE", own, "1000"),
            ("EVAL", WRITE_SCRIPT, 1, own, "x"),
            ("SETNX", "protected:control:qualification:" + FOREIGN_QUALIFICATION_ID + ":1", "x"),
            ("SETNX", "protected:control:readiness:example", "x"),
            ("GET", "protected:control:manifest:example"),
            ("ACL", "WHOAMI"),
            ("ACL", "GETUSER", "default"),
            ("PUBLISH", "probe", "1"),
        ):
            assert not real(client, call), call
        assert pworker_service.command("GET", own) in (b"first", "first")
    finally:
        client.close()


def test_control_commits_each_admission_once(pworker_service, principals) -> None:
    pworker_service.command("FLUSHDB")
    client = principals["control"].raw(protocol=3)
    key = "protected:commissioning:admission:once"
    try:
        assert client.execute_command("SETNX", key, "first") == 1
        assert client.execute_command("SETNX", key, "second") == 0
        for call in (("SET", key, "second"), ("DEL", key), ("EVAL", WRITE_SCRIPT, 1, key, "x")):
            assert not real(client, call)
        assert pworker_service.command("GET", key) in (b"first", "first")
    finally:
        client.close()


def test_candidate_enqueue_reaches_only_its_campaign_stream(pworker_service, principals) -> None:
    pworker_service.command("FLUSHDB")
    own = module().derive_commissioning_lane(permit())
    foreign = module().derive_commissioning_lane(permit(**FOREIGN_PERMIT))
    client = principals["candidate_enqueue"].raw(protocol=3)
    try:
        assert client.execute_command("XADD", own.stream, "*", "intent", "1")
        for stream in (foreign.stream, "curie:runs", own.dead):
            assert not real(client, ("XADD", stream, "*", "intent", "1"))
        for call in (
            ("XRANGE", own.stream, "-", "+"),
            ("XREADGROUP", "GROUP", own.group, "c", "STREAMS", own.stream, ">"),
        ):
            assert not real(client, call)
    finally:
        client.close()


@pytest.mark.parametrize("role", ["production_worker", "candidate_worker"])
def test_scan_lists_names_but_reads_no_values(pworker_service, principals, role) -> None:
    """Observed property of the unchanged worker kernel inventory: SCAN is keyless, so
    the production worker's SCAN grant, and the candidate worker's copy of that same
    inventory, enumerate key names across the keyspace, including commissioning names
    (digests of public identities). Values outside the role's families stay unreadable."""
    pworker_service.command("FLUSHDB")
    key = "protected:commissioning:reservation:example"
    pworker_service.command("SET", key, "seed")
    client = principals[role].raw(protocol=3)
    try:
        cursor, names = client.execute_command("SCAN", "0", "COUNT", "1000")
        assert key in {n.decode() if isinstance(n, bytes) else n for n in names}
        for call in (
            ("GET", key),
            ("TYPE", key),
            ("EXISTS", key),
            ("EVAL", UNDECLARED_READ, 0, key),
        ):
            assert not real(client, call)
    finally:
        client.close()
