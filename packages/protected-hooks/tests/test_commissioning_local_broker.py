"""Protected broker of a local installation, @spec PROTECTED-COMMISSIONING-1.

Pure tests cover the users, ACL file, TLS material and objects. One owned TLS
Valkey container loads the rendered ACL file unchanged and proves each user's
identity and a refusal per boundary over the same pinned session the
provisioner uses. The minikube test drives the full install.
"""

from __future__ import annotations

import base64
import hashlib
import importlib
import importlib.util
import json
import os
import shutil
import subprocess
import time
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from cryptography import x509
from cryptography.hazmat.primitives.asymmetric import ec
from redis.exceptions import AuthenticationError, NoPermissionError, ResponseError

from .admission_broker import fail_safely
from .test_commissioning_provisioner import MergedRecipes, profile, profile_fields

OWNER = "curie-commission-0123456789ab"
CANDIDATES = ("candidate_enqueue", "candidate_worker")
NAMESPACE = OWNER + "-protected"
IMAGE = (
    "valkey/valkey:8.1.10-alpine@sha256:"
    "d2e18f3410b6f616de1417f570fa55261af2898b9c5b2cfb6781ce2373ea43d1"
)


def module() -> Any:
    name = "curie_protected_hooks.commissioning_local_broker"
    assert importlib.util.find_spec(name) is not None, "local broker not implemented"
    return importlib.import_module(name)


def provisioned() -> tuple[Any, MergedRecipes]:
    from curie_protected_hooks.commissioning_provisioner import construct

    recipes = MergedRecipes(profile_fields()["qualification_id"])
    return construct(profile(), acl=recipes), recipes


def users() -> dict[str, Any]:
    _, recipes = provisioned()
    return module().broker_users(
        recipes,
        profile(),
        provisioner_id="commissioning/example-provisioner",
    )


def test_users_take_bs_credential_ids_and_the_rules_g_hashed() -> None:
    api = module()
    result, recipes = provisioned()
    made = users()
    assert set(made) == {*api.GUARD_PRINCIPALS, "provisioner"}
    api.check_guard_binding(made, result.guard_config)
    guard = json.loads(result.guard_config)["broker_acl"]
    for name in api.GUARD_PRINCIPALS:
        assert made[name].username == guard[name]["credential"]["id"]
        assert made[name].rules == recipes.rules(name)
    from curie_protected_hooks.commissioning_acl import (
        commissioning_acl_rules,
        commissioning_acl_template,
    )

    assert made["observer"].rules == commissioning_acl_rules("observer")
    assert made["observer"].username == profile_fields()["observer_identity"]["id"]
    for name in CANDIDATES:
        assert made[name].rules == commissioning_acl_template(name)
    assert made["provisioner"].rules == commissioning_acl_rules("provisioner")
    passwords = [user.password for user in made.values()]
    assert len(set(passwords)) == len(passwords) and all(len(p) >= 40 for p in passwords)
    assert all(user.password not in repr(user) for user in made.values())


def test_guard_binding_refuses_other_rules_or_usernames() -> None:
    api = module()
    result, _ = provisioned()
    made = users()
    changed = dict(made)
    changed["worker"] = api.BrokerUser(
        made["worker"].username, (*made["worker"].rules, "+keys"), made["worker"].password
    )
    with pytest.raises(api.BrokerRefused):
        api.check_guard_binding(changed, result.guard_config)
    changed["worker"] = api.BrokerUser("someone-else", made["worker"].rules, "x")
    with pytest.raises(api.BrokerRefused):
        api.check_guard_binding(changed, result.guard_config)


@pytest.mark.parametrize(
    "name", ["default", "credential/example-worker", "commissioning/example-observer"]
)
def test_users_refuse_default_and_repeated_usernames(name: str) -> None:
    api = module()
    _, recipes = provisioned()
    with pytest.raises(api.BrokerRefused):
        api.broker_users(recipes, profile(), provisioner_id=name)


def test_users_refuse_rules_carrying_state_or_passwords() -> None:
    api = module()
    _, merged = provisioned()

    class Leaky:
        def rules(self, principal: str) -> tuple[str, ...]:
            rules = merged.rules(principal)
            return (*rules, ">plaintext") if principal == "issuer" else rules

    with pytest.raises(api.BrokerRefused) as raised:
        api.broker_users(Leaky(), profile(), provisioner_id="p/1")
    assert "plaintext" not in str(raised.value)


def test_acl_file_holds_hashes_only_and_turns_the_default_user_off() -> None:
    api = module()
    made = users()
    raw = api.render_acl_file(made).decode()
    lines = raw.splitlines()
    assert lines[0] == "user default off resetkeys resetchannels -@all"
    assert len(lines) == len(made) + 1
    for name, user in made.items():
        assert user.password not in raw
        digest = hashlib.sha256(user.password.encode()).hexdigest()
        if name in CANDIDATES:
            # Design decision 9: no campaign yet, so no lane and no right at all.
            idle = "resetkeys resetchannels clearselectors -@all"
            assert f"user {user.username} off #{digest} {idle}" in lines
        else:
            assert f"user {user.username} on #{digest} " + " ".join(user.rules) in lines
    assert "{lane}" not in raw
    partial = {name: user for name, user in made.items() if name != "observer"}
    with pytest.raises(api.BrokerRefused):
        api.render_acl_file(partial)


def test_material_names_the_service_and_pins_the_leaf() -> None:
    api = module()
    name = api.server_name(NAMESPACE)
    assert name == "commissioning-broker." + NAMESPACE + ".svc.cluster.local"
    material = api.BrokerMaterial.generate(name)
    leaf = x509.load_pem_x509_certificate(material.certificate_pem.encode())
    ca = x509.load_pem_x509_certificate(material.ca_pem.encode())
    san = leaf.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert san.get_values_for_type(x509.DNSName) == [name]
    ca_key = ca.public_key()
    assert isinstance(ca_key, ec.EllipticCurvePublicKey)
    leaf.verify_directly_issued_by(ca)
    assert material.spki_sha256 == api._spki_sha256(leaf)
    assert material.key_pem not in repr(material)
    assert api.BrokerMaterial.generate(name).spki_sha256 != material.spki_sha256


def objects() -> list[dict[str, Any]]:
    api = module()
    made = users()
    material = api.BrokerMaterial.generate(api.server_name(NAMESPACE))
    return api.broker_objects(
        owner=OWNER,
        namespace=NAMESPACE,
        image=IMAGE,
        material=material,
        acl_file=api.render_acl_file(made),
    )


def test_objects_are_owned_and_the_pod_is_locked_down() -> None:
    from curie_protected_hooks.commissioning_local_installation import OWNER_LABEL

    rendered = objects()
    assert [o["kind"] for o in rendered] == ["Namespace", "Secret", "Service", "Pod"]
    assert all(o["metadata"]["labels"][OWNER_LABEL] == OWNER for o in rendered)
    namespace, secret, service, pod = rendered
    assert namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "restricted"
    assert secret["immutable"] is True
    assert service["spec"]["selector"] == pod["metadata"]["labels"]
    spec = pod["spec"]
    assert spec["restartPolicy"] == "Never"
    assert spec["automountServiceAccountToken"] is False
    assert spec["securityContext"]["runAsNonRoot"] is True
    (container,) = spec["containers"]
    assert container["image"] == IMAGE
    assert container["command"] == ["valkey-server"]
    assert container["args"][:4] == ["--port", "0", "--tls-port", "6379"]
    assert container["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "readOnlyRootFilesystem": True,
        "capabilities": {"drop": ["ALL"]},
    }


def test_objects_carry_no_plaintext_password() -> None:
    api = module()
    made = users()
    material = api.BrokerMaterial.generate(api.server_name(NAMESPACE))
    rendered = api.broker_objects(
        owner=OWNER,
        namespace=NAMESPACE,
        image=IMAGE,
        material=material,
        acl_file=api.render_acl_file(made),
    )
    decoded = json.dumps(rendered) + "".join(
        base64.b64decode(v).decode() for v in rendered[1]["data"].values()
    )
    assert all(user.password not in decoded for user in made.values())


@pytest.mark.parametrize(
    "image", ["valkey/valkey:8.1.10-alpine", "valkey/valkey@sha256:" + "A" * 64, ""]
)
def test_objects_refuse_an_unpinned_image(image: str) -> None:
    api = module()
    material = api.BrokerMaterial.generate(api.server_name(NAMESPACE))
    with pytest.raises(Exception, match="refused"):
        api.broker_objects(
            owner=OWNER, namespace=NAMESPACE, image=image, material=material, acl_file=b"x"
        )


def test_objects_refuse_material_for_another_namespace() -> None:
    api = module()
    material = api.BrokerMaterial.generate(api.server_name("elsewhere"))
    with pytest.raises(api.BrokerRefused):
        api.broker_objects(
            owner=OWNER, namespace=NAMESPACE, image=IMAGE, material=material, acl_file=b"x"
        )


class Valkey:
    """One owned TLS Valkey container started with the rendered files only."""

    material: Any
    users: dict[str, Any]
    port: int
    run_id: str


def _docker(*args: str) -> str:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, check=True, timeout=120
    ).stdout.strip()


@pytest.fixture(scope="module")
def valkey(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Valkey]:
    """Same server arguments and files as the Pod, behind a loopback port."""
    if shutil.which("docker") is None:
        fail_safely("Docker is required for the local broker ACL file check")
    api = module()
    fixture = Valkey()
    fixture.material = api.BrokerMaterial.generate(api.server_name(NAMESPACE))
    fixture.users = users()
    private = tmp_path_factory.mktemp("local-broker")
    files = {
        "ca.crt": fixture.material.ca_pem,
        "tls.crt": fixture.material.certificate_pem,
        "tls.key": fixture.material.key_pem,
        "users.acl": api.render_acl_file(fixture.users).decode(),
    }
    for name, text in files.items():
        (private / name).write_text(text)
        (private / name).chmod(0o600)
    name = "curie-check-local-broker-" + uuid.uuid4().hex[:12]
    pod = objects()[3]["spec"]["containers"][0]
    cid = None
    try:
        cid = _docker(
            "run", "-d", "--name", name, "--label", "curie.test.owner=" + name,
            "--user", f"{os.getuid()}:{os.getgid()}", "--memory", "128m", "--cpus", "0.25",
            "-p", "127.0.0.1::6379", "-v", f"{private}:/tls:ro", "--tmpfs", "/data",
            IMAGE, *pod["command"], *pod["args"],
        )  # fmt: skip
        fixture.port = int(_docker("port", cid, "6379/tcp").rsplit(":", 1)[1])
        deadline = time.monotonic() + 30
        while True:
            try:
                session = api.pinned_session(
                    fixture.material, ("127.0.0.1", fixture.port), fixture.users["provisioner"]
                )
                fixture.run_id = session.info("server")["run_id"]
                session.close()
                break
            except Exception:  # noqa: BLE001  Retry until the owned server accepts TLS.
                if time.monotonic() > deadline:
                    fail_safely("owned Valkey did not accept the provisioner")
                time.sleep(0.2)
        yield fixture
    finally:
        if cid is not None:
            subprocess.run(["docker", "rm", "-fv", cid], capture_output=True, check=False)


def session(valkey: Valkey, name: str, **kwargs: Any) -> Any:
    return module().pinned_session(
        valkey.material, ("127.0.0.1", valkey.port), valkey.users[name], **kwargs
    )


def test_every_user_authenticates_as_its_own_identity(valkey: Valkey) -> None:
    for name, user in valkey.users.items():
        if name in CANDIDATES:
            with pytest.raises(AuthenticationError):
                session(valkey, name).ping()
            continue
        client = session(valkey, name)
        try:
            assert client.ping() is True
            if "+acl|whoami" in user.rules:
                assert client.execute_command("ACL", "WHOAMI") == user.username.encode(), name
            if "+info|server" in user.rules:
                assert client.info("server")["run_id"] == valkey.run_id, name
        finally:
            client.close()
    # Unchanged LANE-3 production recipes grant no ACL WHOAMI; the commissioning ones do.
    assert {
        n for n, u in valkey.users.items() if "+acl|whoami" not in u.rules and n not in CANDIDATES
    } == {
        "enqueue",
        "worker",
        "verifier",
        "qualification_publisher",
        "observer",
    }


def test_default_user_and_wrong_passwords_are_refused(valkey: Valkey) -> None:
    api = module()
    wrong = api.BrokerUser(valkey.users["worker"].username, (), "not-the-password")
    with pytest.raises(AuthenticationError):
        api.pinned_session(valkey.material, ("127.0.0.1", valkey.port), wrong).ping()
    default = api.BrokerUser("default", (), "")
    with pytest.raises((AuthenticationError, ResponseError)):
        api.pinned_session(valkey.material, ("127.0.0.1", valkey.port), default).ping()


def test_pinned_session_refuses_another_key_name_or_epoch(valkey: Valkey) -> None:
    api = module()
    user = valkey.users["observer"]
    other = api.BrokerMaterial.generate(valkey.material.server_name)
    with pytest.raises(Exception):  # noqa: B017, PT011  Any refusal before a credential is sent.
        api.pinned_session(other, ("127.0.0.1", valkey.port), user).ping()
    renamed = api.BrokerMaterial(
        api.server_name("elsewhere"),
        valkey.material.ca_pem,
        valkey.material.certificate_pem,
        valkey.material.key_pem,
        valkey.material.spki_sha256,
    )
    with pytest.raises(Exception):  # noqa: B017, PT011  Hostname verification refuses.
        api.pinned_session(renamed, ("127.0.0.1", valkey.port), user).ping()
    with pytest.raises(api.BrokerRefused):
        session(valkey, "provisioner", run_id="0" * 40)
    session(valkey, "provisioner", run_id=valkey.run_id).close()


def test_observer_reads_acl_state_and_nothing_else(valkey: Valkey) -> None:
    client = session(valkey, "observer")
    try:
        for name, user in valkey.users.items():
            state = client.acl_getuser(user.username)
            assert ("off" if name in CANDIDATES else "on") in state["flags"]
            assert state["passwords"] == [hashlib.sha256(user.password.encode()).hexdigest()]
        with pytest.raises(NoPermissionError):
            client.get("protected:control:selection:x")
        with pytest.raises(NoPermissionError):
            client.execute_command("ACL", "SETUSER", valkey.users["worker"].username, "off")
    finally:
        client.close()


def test_principals_keep_to_their_key_families(valkey: Valkey) -> None:
    worker = session(valkey, "worker")
    provisioner = session(valkey, "provisioner")
    publisher = session(valkey, "qualification_publisher")
    try:
        with pytest.raises(NoPermissionError):
            worker.get("protected:commissioning:permit:x")
        assert worker.get("protected:control:selection:x") is None
        with pytest.raises(NoPermissionError):
            provisioner.get("curie:runs")
        with pytest.raises(NoPermissionError):
            publisher.get("protected:control:selection:x")
    finally:
        for client in (worker, provisioner, publisher):
            client.close()


def test_campaign_start_instantiates_the_templates_for_the_permits_lane(valkey: Valkey) -> None:
    """Design decision 9 on the pinned Valkey: enabled for one lane, nothing else."""
    from curie_protected_hooks.commissioning_acl import (
        check_candidate_rules,
        derive_commissioning_lane,
    )

    from .test_commissioning_acl import permit

    api = module()
    record, other = permit(), permit(campaign_generation="2")
    lane, foreign = derive_commissioning_lane(record), derive_commissioning_lane(other)
    provisioner = session(valkey, "provisioner")
    try:
        api.enable_candidates(provisioner, valkey.users, record)
        enqueue = session(valkey, "candidate_enqueue")
        worker = session(valkey, "candidate_worker")
        try:
            assert enqueue.execute_command("ACL", "WHOAMI") == (
                valkey.users["candidate_enqueue"].username.encode()
            )
            assert enqueue.xadd(lane.stream, {"k": "v"})
            with pytest.raises(NoPermissionError):
                enqueue.xadd(foreign.stream, {"k": "v"})
            assert worker.xlen(lane.stream) == 1
            with pytest.raises(NoPermissionError):
                worker.xlen(foreign.stream)
            with pytest.raises(NoPermissionError):
                worker.xlen("curie:runs")
        finally:
            enqueue.close()
            worker.close()
        observer = session(valkey, "observer")
        try:
            for name in CANDIDATES:
                state = observer.acl_getuser(valkey.users[name].username)
                assert "on" in state["flags"]
                assert state["passwords"] == [
                    hashlib.sha256(valkey.users[name].password.encode()).hexdigest()
                ]
        finally:
            observer.close()
        from curie_protected_hooks.commissioning_acl import commissioning_acl_rules

        for name in CANDIDATES:
            check_candidate_rules(name, commissioning_acl_rules(name, permit=record), permit=record)
        # A user whose rules are not its template refuses before any SETUSER.
        swapped = dict(valkey.users)
        swapped["candidate_worker"] = valkey.users["candidate_enqueue"]
        with pytest.raises(api.BrokerRefused):
            api.enable_candidates(provisioner, swapped, record)
    finally:
        for name in CANDIDATES:
            provisioner.execute_command(
                "ACL", "SETUSER", valkey.users[name].username, "off",
                "resetkeys", "resetchannels", "clearselectors", "-@all",
            )  # fmt: skip
        provisioner.close()
