"""Protected broker of a local installation, @spec PROTECTED-COMMISSIONING-1.

Pure tests cover the principals, TLS material, objects and the provisioner's
grant. One owned TLS Valkey container starts with the Pod's arguments and only
the installer password; the administrator bootstrap's enrollment creates every
user and records the baseline, and the tests prove each user's identity and a
refusal per boundary over the pinned session the provisioner uses. Nothing
here hashes a password: the broker derives every hash, and the tests compare
``ACL GETUSER`` replies with the recorded baseline. The minikube test drives
the full install.
"""

from __future__ import annotations

import base64
import dataclasses
import importlib
import importlib.util
import json
import os
import secrets
import shutil
import subprocess
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
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


def bootstrap() -> Any:
    return importlib.import_module("curie_protected_hooks.commissioning_local_bootstrap")


def provisioned() -> tuple[Any, MergedRecipes]:
    from curie_protected_hooks.commissioning_provisioner import construct

    recipes = MergedRecipes(profile_fields()["qualification_id"])
    return construct(profile(), acl=recipes), recipes


def principals() -> dict[str, Any]:
    _, recipes = provisioned()
    return module().broker_principals(
        recipes, profile(), provisioner_id="commissioning/example-provisioner"
    )


def test_principals_take_the_profiles_ids_and_the_rules_g_hashed() -> None:
    api = module()
    result, recipes = provisioned()
    made = principals()
    assert set(made) == {*api.GUARD_PRINCIPALS, "provisioner"}
    assert all(type(p) is api.Principal and not hasattr(p, "password") for p in made.values())
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
    # Design decision 10: the production metadata principals take B's identities.
    for name in ("control_reader", "source_writer"):
        assert made[name].username == profile_fields()[name + "_identity"]["id"]
    for name in CANDIDATES:
        assert made[name].rules == commissioning_acl_template(name)
    assert made["provisioner"].rules == commissioning_acl_rules("provisioner")


def test_issued_credentials_are_fresh_distinct_and_redacted() -> None:
    issued = bootstrap().issue_credentials(principals())
    passwords = [user.password for user in issued.values()]
    assert len(set(passwords)) == len(passwords) and all(len(p) >= 40 for p in passwords)
    assert all(user.password not in repr(user) for user in issued.values())
    again = bootstrap().issue_credentials(principals())
    assert not set(passwords) & {user.password for user in again.values()}


def test_guard_binding_refuses_other_rules_or_usernames() -> None:
    api = module()
    result, _ = provisioned()
    made = principals()
    changed = dict(made)
    changed["worker"] = api.Principal(made["worker"].username, (*made["worker"].rules, "+keys"))
    with pytest.raises(api.BrokerRefused):
        api.check_guard_binding(changed, result.guard_config)
    changed["worker"] = api.Principal("someone-else", made["worker"].rules)
    with pytest.raises(api.BrokerRefused):
        api.check_guard_binding(changed, result.guard_config)


@pytest.mark.parametrize(
    "name", ["default", "credential/example-worker", "commissioning/example-observer"]
)
def test_principals_refuse_default_and_repeated_usernames(name: str) -> None:
    api = module()
    _, recipes = provisioned()
    with pytest.raises(api.BrokerRefused):
        api.broker_principals(recipes, profile(), provisioner_id=name)


def test_principals_refuse_rules_carrying_state_or_passwords() -> None:
    api = module()
    _, merged = provisioned()

    class Leaky:
        def rules(self, principal: str) -> tuple[str, ...]:
            rules = merged.rules(principal)
            return (*rules, ">plaintext") if principal == "issuer" else rules

    with pytest.raises(api.BrokerRefused) as raised:
        api.broker_principals(Leaky(), profile(), provisioner_id="p/1")
    assert "plaintext" not in str(raised.value)


def test_the_provisioner_never_holds_another_principals_password(tmp_path: Path) -> None:
    """Design decisions 7 and 8: only the administrator bootstrap issues credentials."""
    boot = bootstrap()
    ledger, runner, cluster = admin_cluster(tmp_path)
    admin = bootstrap_for(cluster, ledger, tmp_path)
    admin.issue()
    issued = admin.credentials()
    grant = admin.grant()
    workload = {"enqueue", "control_reader", "source_writer", "worker"} & set(issued)
    assert set(grant.workload) == workload and "enqueue" in workload
    assert grant.provisioner == issued["provisioner"]
    held = repr(vars(cluster)) + json.dumps(
        {
            "principals": {n: [p.username, list(p.rules)] for n, p in grant.principals.items()},
            "provisioner": grant.provisioner.password,
            "workload": {n: u.password for n, u in grant.workload.items()},
        }
    )
    for name, user in issued.items():
        held_here = user.password in held
        assert held_here == (name in {"provisioner", *workload}), name
    for name in ("observer", "qualification_publisher", "issuer", "control", *CANDIDATES):
        assert all(issued[name].password != u.password for u in grant.workload.values())
    # Each credential went straight into its own Secret, never into one shared object.
    secrets_created = {
        key.split("/")[-1]: value
        for key, value in runner.objects.items()
        if key.startswith("v1/Secret/")
    }
    credential_keys = {
        key.split("/")[-1]
        for key in runner.objects
        if key.startswith(f"v1/Secret/{boot.credentials_namespace(NAMESPACE)}/")
    }
    assert credential_keys == {boot.credential_secret_name(name) for name in issued} | {
        boot.MEDIATOR_SECRET
    }
    for name, user in issued.items():
        data = secrets_created[boot.credential_secret_name(name)]["data"]
        assert base64.b64decode(data["password"]).decode() == user.password
        assert base64.b64decode(data["username"]).decode() == user.username
    assert boot.INSTALLER_SECRET in secrets_created


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
    material = api.BrokerMaterial.generate(api.server_name(NAMESPACE))
    return api.broker_objects(
        owner=OWNER,
        namespace=NAMESPACE,
        image=IMAGE,
        material=material,
        installer_secret="commissioning-broker-installer",
    )


def test_objects_are_owned_and_the_pod_is_locked_down() -> None:
    from curie_protected_hooks.commissioning_local_installation import OWNER_LABEL

    rendered = objects()
    assert [o["kind"] for o in rendered] == ["Namespace", "Secret", "Service", "Pod"]
    assert all(o["metadata"]["labels"][OWNER_LABEL] == OWNER for o in rendered)
    namespace, secret, service, pod = rendered
    assert namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "restricted"
    assert secret["immutable"] is True
    assert set(secret["data"]) == {"ca.crt", "tls.crt", "tls.key"}
    assert service["spec"]["selector"] == pod["metadata"]["labels"]
    spec = pod["spec"]
    assert spec["restartPolicy"] == "Never"
    assert spec["automountServiceAccountToken"] is False
    assert spec["securityContext"]["runAsNonRoot"] is True
    (container,) = spec["containers"]
    assert container["image"] == IMAGE
    assert container["command"] == ["valkey-server"]
    assert container["args"][:4] == ["--port", "0", "--tls-port", "6379"]
    assert "--aclfile" not in container["args"]
    requirepass = container["args"].index("--requirepass")
    assert container["args"][requirepass + 1] == "$(INSTALLER_PASSWORD)"
    (env,) = container["env"]
    assert env == {
        "name": "INSTALLER_PASSWORD",
        "valueFrom": {
            "secretKeyRef": {"name": "commissioning-broker-installer", "key": "password"}
        },
    }
    assert container["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "readOnlyRootFilesystem": True,
        "capabilities": {"drop": ["ALL"]},
    }


@pytest.mark.parametrize(
    "image", ["valkey/valkey:8.1.10-alpine", "valkey/valkey@sha256:" + "A" * 64, ""]
)
def test_objects_refuse_an_unpinned_image(image: str) -> None:
    api = module()
    material = api.BrokerMaterial.generate(api.server_name(NAMESPACE))
    with pytest.raises(Exception, match="refused"):
        api.broker_objects(
            owner=OWNER, namespace=NAMESPACE, image=image, material=material, installer_secret="x"
        )


def test_objects_refuse_material_for_another_namespace() -> None:
    api = module()
    material = api.BrokerMaterial.generate(api.server_name("elsewhere"))
    with pytest.raises(api.BrokerRefused):
        api.broker_objects(
            owner=OWNER, namespace=NAMESPACE, image=IMAGE, material=material, installer_secret="x"
        )


class Valkey:
    """One owned TLS Valkey container, enrolled by the administrator bootstrap."""

    material: Any
    users: dict[str, Any]
    installer: Any
    baseline: dict[str, Any]
    port: int
    run_id: str


def _docker(*args: str) -> str:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, check=True, timeout=120
    ).stdout.strip()


@pytest.fixture(scope="module")
def valkey(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Valkey]:
    """Same server arguments as the Pod, behind a loopback port, then enrolled."""
    if shutil.which("docker") is None:
        fail_safely("Docker is required for the local broker enrollment check")
    api, boot = module(), bootstrap()
    fixture = Valkey()
    fixture.material = api.BrokerMaterial.generate(api.server_name(NAMESPACE))
    fixture.users = boot.issue_credentials(principals())
    fixture.installer = api.BrokerUser("default", (), secrets.token_urlsafe(32))
    private = tmp_path_factory.mktemp("local-broker")
    files = {
        "ca.crt": fixture.material.ca_pem,
        "tls.crt": fixture.material.certificate_pem,
        "tls.key": fixture.material.key_pem,
    }
    for name, text in files.items():
        (private / name).write_text(text)
        (private / name).chmod(0o600)
    name = "curie-check-local-broker-" + uuid.uuid4().hex[:12]
    pod = objects()[3]["spec"]["containers"][0]
    args = [fixture.installer.password if a == "$(INSTALLER_PASSWORD)" else a for a in pod["args"]]
    cid = None
    try:
        cid = _docker(
            "run", "-d", "--name", name, "--label", "curie.test.owner=" + name,
            "--user", f"{os.getuid()}:{os.getgid()}", "--memory", "128m", "--cpus", "0.25",
            "-p", "127.0.0.1::6379", "-v", f"{private}:/tls:ro", "--tmpfs", "/data",
            IMAGE, *pod["command"], *args,
        )  # fmt: skip
        fixture.port = int(_docker("port", cid, "6379/tcp").rsplit(":", 1)[1])
        dial = ("127.0.0.1", fixture.port)
        deadline = time.monotonic() + 30
        while True:
            try:
                installer = api.pinned_session(fixture.material, dial, fixture.installer)
                fixture.run_id = installer.info("server")["run_id"]
                break
            except Exception:  # noqa: BLE001  Retry until the owned server accepts TLS.
                if time.monotonic() > deadline:
                    fail_safely("owned Valkey did not accept the installer")
                time.sleep(0.2)
        try:
            fixture.baseline = boot.enroll_users(installer, fixture.users)
            boot.close_installer(installer)
        finally:
            installer.close()
        yield fixture
    finally:
        if cid is not None:
            subprocess.run(["docker", "rm", "-fv", cid], capture_output=True, check=False)


def session(valkey: Valkey, name: str, **kwargs: Any) -> Any:
    return module().pinned_session(
        valkey.material, ("127.0.0.1", valkey.port), valkey.users[name], **kwargs
    )


def test_the_baseline_covers_every_identity_the_installation_creates(valkey: Valkey) -> None:
    """Design decision 7 covers the provisioner and the observer too."""
    assert set(valkey.baseline) == set(valkey.users)
    assert {"provisioner", "observer"} <= set(valkey.baseline)
    assert all(len(state.password_hashes) == 1 for state in valkey.baseline.values())


def test_the_installer_is_closed_after_enrollment(valkey: Valkey) -> None:
    api = module()
    with pytest.raises((AuthenticationError, ResponseError)):
        api.pinned_session(valkey.material, ("127.0.0.1", valkey.port), valkey.installer).ping()
    observer = session(valkey, "observer")
    try:
        state = observer.acl_getuser("default")
        assert "off" in state["flags"] and state["passwords"] == []
    finally:
        observer.close()


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
        "control_reader",
        "source_writer",
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
            assert len(state["passwords"]) == 1
            if name in valkey.baseline:
                assert set(state["passwords"]) == valkey.baseline[name].password_hashes
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
                # Enabling changes no password: the hash set is the recorded baseline.
                assert set(state["passwords"]) == valkey.baseline[name].password_hashes
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


def test_port_forward_output_is_drained_after_the_ready_line() -> None:
    """kubectl logs a line per forwarded connection; an undrained pipe stalls the forward."""
    import sys

    api = module()
    script = (
        "import sys\n"
        "print('Forwarding from 127.0.0.1:41234 -> 6379', flush=True)\n"
        "line = 'Handling connection for 6379\\n'\n"
        "for _ in range(40000):\n"
        "    sys.stdout.write(line)\n"
        "sys.stdout.flush()\n"
    )
    process, port = api._start_forward([sys.executable, "-c", script], dict(os.environ))
    try:
        assert port == 41234
        # Over 1 MB follows the ready line; the child finishes only if it is drained.
        assert process.wait(timeout=20) == 0
    finally:
        process.kill()


def test_port_forward_without_a_ready_line_refuses_and_is_stopped() -> None:
    import sys

    api = module()
    script = "import time; print('error: unable to forward', flush=True); time.sleep(60)"
    with pytest.raises(api.BrokerRefused):
        api._start_forward([sys.executable, "-c", script], dict(os.environ))


def test_provisioner_installs_m_and_a_closed_selection_once(valkey: Valkey) -> None:
    """Clause 6, before the campaign: M and the selection with ``admission_open: false``."""
    from curie_protected_hooks.admission_records import parse_selection

    api = module()
    result, _ = provisioned()
    manifest = result.manifest
    m = manifest.as_dict()
    provisioner = session(valkey, "provisioner")
    worker = session(valkey, "worker")
    try:
        api.install_control_records(provisioner, manifest, qualification_generation="1")
        stored = worker.get("protected:control:manifest:" + manifest.digest)
        assert stored == manifest.canonical_bytes
        selection = parse_selection(worker.get("protected:control:selection:" + m["runtime_id"]))
        assert selection == {
            "schema_version": 1,
            "runtime_id": m["runtime_id"],
            "runtime_generation": m["runtime_generation"],
            "manifest_digest": manifest.digest,
            "qualification_id": m["qualification_id"],
            "qualification_generation": "1",
            "broker_run_id": m["broker_identity"]["run_id"],
            "admission_open": False,
        }
        # Identical bytes are a replay; any other selection for the runtime refuses.
        api.install_control_records(provisioner, manifest, qualification_generation="1")
        with pytest.raises(api.BrokerRefused):
            api.install_control_records(provisioner, manifest, qualification_generation="2")
        with pytest.raises(NoPermissionError):
            worker.set("protected:control:selection:" + m["runtime_id"], b"x")
    finally:
        for client in (provisioner, worker):
            client.close()


def admin_cluster(tmp_path: Path) -> tuple[Any, Any, Any]:
    """The administrator's cluster view, its admin kubeconfig in its own directory."""
    from curie_protected_hooks.commissioning_local_installation import LocalCluster
    from curie_protected_hooks.commissioning_local_ownership import OwnershipLedger

    from .test_commissioning_local_installation import FakeRunner, write_admin_kubeconfig

    tmp_path.mkdir(exist_ok=True)
    ledger = OwnershipLedger.create(tmp_path / "run", owner=OWNER)
    runner = FakeRunner(ledger)
    kubeconfig = write_admin_kubeconfig(tmp_path / "administrator")
    return ledger, runner, LocalCluster(ledger, runner=runner, kubeconfig=kubeconfig)


def bootstrap_for(cluster: Any, ledger: Any, tmp_path: Path) -> Any:
    return bootstrap().AdministratorBootstrap(
        cluster,
        namespace=NAMESPACE,
        principals=principals(),
        baseline_dir=tmp_path / "administrator" / "baseline",
        provisioner_dir=ledger.root,
    )


def _broker_with_a_live_forward(tmp_path: Path) -> tuple[Any, Any, Any]:
    """A broker whose forward is a real process with nothing behind its port."""
    import socket
    import sys

    from curie_protected_hooks.commissioning_local_installation import LocalCluster
    from curie_protected_hooks.commissioning_local_ownership import OwnershipLedger

    from .test_commissioning_local_installation import FakeRunner, write_admin_kubeconfig

    api = module()
    ledger = OwnershipLedger.create(tmp_path / "run", owner=OWNER)
    write_admin_kubeconfig(ledger.root)
    cluster = LocalCluster(ledger, runner=FakeRunner(ledger))
    broker = api.LocalBroker(cluster, namespace=NAMESPACE)
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    script = (
        f"import time; print('Forwarding from 127.0.0.1:{port} -> 6379', flush=True); "
        "time.sleep(60)"
    )
    broker._forward, broker._port = api._start_forward(
        [sys.executable, "-c", script], dict(os.environ)
    )
    broker._material = api.BrokerMaterial.generate(api.server_name(NAMESPACE))
    return api, cluster, broker


def test_a_failed_campaign_start_closes_the_forward(tmp_path: Path) -> None:
    from .test_commissioning_acl import permit

    api, _, broker = _broker_with_a_live_forward(tmp_path)
    process = broker._forward
    provisioner = api.BrokerUser("commissioning/example-provisioner", (), "x")
    with pytest.raises(Exception):  # noqa: B017, PT011  Nothing answers behind the forward.
        broker.start_campaign(provisioner, principals(), permit())
    assert broker._forward is None and process.poll() is not None


def test_a_failed_enrollment_closes_the_forward(tmp_path: Path) -> None:
    api, _, broker = _broker_with_a_live_forward(tmp_path)
    process = broker._forward
    ledger, _, cluster = admin_cluster(tmp_path / "admin-side")
    admin = bootstrap_for(cluster, ledger, tmp_path / "admin-side")
    admin.issue()
    with pytest.raises(bootstrap().BootstrapRefused):
        admin.enroll(broker)
    assert broker._forward is None and process.poll() is not None


def test_the_provisioner_identity_cannot_read_other_credentials() -> None:
    """Its Role reads and deletes Secrets only by name, and names only its own."""
    boot = bootstrap()
    account, role, binding = boot.provisioner_rbac(
        OWNER, NAMESPACE, readable_secrets=boot.PROVISIONER_SECRETS
    )
    assert account["kind"] == "ServiceAccount" and account["automountServiceAccountToken"] is False
    assert binding["roleRef"]["name"] == role["metadata"]["name"] == boot.PROVISIONER_ACCOUNT
    secret_rules = [r for r in role["rules"] if "secrets" in r["resources"]]
    assert {tuple(r["verbs"]) for r in secret_rules if "resourceNames" not in r} == {("create",)}
    named = [r for r in secret_rules if "resourceNames" in r]
    assert named == [
        {
            "apiGroups": [""],
            "resources": ["secrets"],
            "resourceNames": ["commissioning-broker-tls"],
            "verbs": ["get", "delete"],
        }
    ]
    for rule in role["rules"]:
        assert "*" not in rule["verbs"] and "*" not in rule["resources"]
        assert not ({"list", "watch"} & set(rule["verbs"]) and "secrets" in rule["resources"])
    assert not any(
        boot.credential_secret_name(n) in json.dumps(role) for n in ("observer", "provisioner")
    )


def test_the_minted_kubeconfig_is_a_token_identity_for_the_same_cluster(tmp_path: Path) -> None:
    boot = bootstrap()
    ledger, runner, cluster = admin_cluster(tmp_path)
    admin = bootstrap_for(cluster, ledger, tmp_path)
    admin.issue()
    grant = admin.grant()
    assert grant.kubeconfig == ledger.root / boot.PROVISIONER_KUBECONFIG
    assert oct(grant.kubeconfig.stat().st_mode & 0o777) == "0o600"
    value = json.loads(grant.kubeconfig.read_text())
    (cluster,) = value["clusters"]
    assert cluster == {
        "name": OWNER,
        "cluster": {
            "server": "https://127.0.0.1:50123",
            "certificate-authority": "/fixture/ca.crt",
        },
    }
    (user,) = value["users"]
    assert user == {
        "name": boot.PROVISIONER_ACCOUNT,
        "user": {"token": "fixture-service-account-token"},
    }
    assert value["contexts"][0]["context"]["namespace"] == NAMESPACE
    token = next(argv for argv, _ in runner.calls if argv[5:7] == ["create", "token"])
    assert token[7:] == [boot.PROVISIONER_ACCOUNT, "--namespace", NAMESPACE, "--duration", "24h"]
    created = [key for key in runner.objects]
    assert created[:6] == [
        f"v1/Namespace//{NAMESPACE}",
        f"v1/Namespace//{boot.credentials_namespace(NAMESPACE)}",
        f"v1/Namespace//{boot.worker_namespace(NAMESPACE)}",
        f"v1/ServiceAccount/{NAMESPACE}/{boot.PROVISIONER_ACCOUNT}",
        f"rbac.authorization.k8s.io/v1/Role/{NAMESPACE}/{boot.PROVISIONER_ACCOUNT}",
        f"rbac.authorization.k8s.io/v1/RoleBinding/{NAMESPACE}/{boot.PROVISIONER_ACCOUNT}",
    ]


def test_the_provisioner_kubeconfig_never_lands_beside_the_admin_one(tmp_path: Path) -> None:
    boot = bootstrap()
    ledger, _, cluster = admin_cluster(tmp_path)
    with pytest.raises(boot.BootstrapRefused):
        boot.AdministratorBootstrap(
            cluster,
            namespace=NAMESPACE,
            principals=principals(),
            baseline_dir=tmp_path / "baseline",
            provisioner_dir=cluster.kubeconfig.parent,
        )
    from curie_protected_hooks.commissioning_local_installation import (
        InstallationRefused,
        LocalCluster,
    )

    open_dir = tmp_path / "shared"
    open_dir.mkdir(mode=0o755)
    open_dir.chmod(0o755)
    with pytest.raises(InstallationRefused):
        LocalCluster(ledger, kubeconfig=open_dir / "kubeconfig")


def test_a_forward_to_another_port_is_refused() -> None:
    """The ready line must name the expected target port, the broker's by default."""
    import sys

    api = module()
    script = (
        "import time; print('Forwarding from 127.0.0.1:41234 -> 8000', flush=True); time.sleep(60)"
    )
    with pytest.raises(api.BrokerRefused):
        api._start_forward([sys.executable, "-c", script], dict(os.environ))
    process, port = api._start_forward(
        [sys.executable, "-c", script], dict(os.environ), target_port=8000
    )
    api._stop(process)
    assert port == 41234


def test_the_bootstrap_mints_the_launch_identity_and_keeps_its_keys(tmp_path: Path) -> None:
    """The provisioner gets the public half; each private key goes into its own Secret."""
    boot = bootstrap()
    ledger, runner, cluster = admin_cluster(tmp_path)
    admin = bootstrap_for(cluster, ledger, tmp_path)
    admin.issue()
    grant = admin.grant()
    held = boot.credentials_namespace(NAMESPACE)
    mediator = runner.objects[f"v1/Secret/{held}/{boot.MEDIATOR_SECRET}"]["data"]
    worker_ns = boot.worker_namespace(NAMESPACE)
    client = runner.objects[f"v1/Secret/{worker_ns}/{boot.WORKER_CLIENT_SECRET}"]["data"]
    assert set(mediator) == {"ca.crt", "tls.crt", "tls.key"}
    assert set(client) == {"launch-client.pem", "launch-client.key"}
    server = x509.load_pem_x509_certificate(base64.b64decode(mediator["tls.crt"]))
    ca = x509.load_pem_x509_certificate(grant.launch.ca_pem.encode())
    server.verify_directly_issued_by(ca)
    assert api_spki(server) == grant.launch.server_spki_sha256
    assert grant.launch.server_name == boot.mediator_name(NAMESPACE)
    for key in (mediator["tls.key"], client["launch-client.key"]):
        private = base64.b64decode(key).decode()
        assert "PRIVATE KEY" in private
        assert private not in repr([getattr(grant, f.name) for f in dataclasses.fields(grant)])
    assert "PRIVATE KEY" not in json.dumps(dataclasses.asdict(grant.launch))
    # The provisioner may not read either Secret.
    from curie_protected_hooks.commissioning_local_bootstrap import PROVISIONER_SECRETS

    assert boot.WORKER_CLIENT_SECRET not in PROVISIONER_SECRETS


def api_spki(certificate: Any) -> str:
    return module()._spki_sha256(certificate)


def test_the_provisioner_cannot_create_pods_where_the_worker_key_is() -> None:
    boot = bootstrap()
    role, binding = boot.provisioner_worker_rbac(
        OWNER, boot.worker_namespace(NAMESPACE), account_namespace=NAMESPACE
    )
    assert role["metadata"]["namespace"] == boot.worker_namespace(NAMESPACE)
    verbs = {(tuple(r["apiGroups"]), tuple(r["resources"])): set(r["verbs"]) for r in role["rules"]}
    assert verbs == {
        (("",), ("secrets",)): {"create"},
        (("",), ("pods",)): {"get", "list", "watch"},
        (("",), ("pods/log",)): {"get"},
        (("apps",), ("deployments",)): {"get", "list", "watch"},
    }
    assert binding["subjects"] == [
        {"kind": "ServiceAccount", "name": boot.PROVISIONER_ACCOUNT, "namespace": NAMESPACE}
    ]


@pytest.mark.parametrize(
    "change",
    [
        {"kind": "Pod"},
        {"metadata": {"name": "curie-protected-worker", "namespace": NAMESPACE}},
        {"metadata": {"name": "other", "namespace": NAMESPACE + "-worker"}},
    ],
)
def test_the_administrator_installs_only_the_worker_deployment(
    tmp_path: Path, change: dict[str, Any]
) -> None:
    boot = bootstrap()
    ledger, runner, cluster = admin_cluster(tmp_path)
    admin = bootstrap_for(cluster, ledger, tmp_path)
    deployment = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "curie-protected-worker", "namespace": NAMESPACE + "-worker"},
        **change,
    }
    with pytest.raises(boot.BootstrapRefused):
        admin.install_worker(deployment)
    assert runner.objects == {}


def test_issue_binds_the_provisioner_in_the_worker_namespace(tmp_path: Path) -> None:
    boot = bootstrap()
    ledger, runner, cluster = admin_cluster(tmp_path)
    bootstrap_for(cluster, ledger, tmp_path).issue()
    worker_ns = boot.worker_namespace(NAMESPACE)
    rbac = "rbac.authorization.k8s.io/v1"
    for kind in ("Role", "RoleBinding"):
        key = f"{rbac}/{kind}/{worker_ns}/{boot.PROVISIONER_ACCOUNT}"
        assert key in runner.objects, kind
    role = runner.objects[f"{rbac}/Role/{worker_ns}/{boot.PROVISIONER_ACCOUNT}"]
    assert (
        role["rules"]
        == boot.provisioner_worker_rbac(OWNER, worker_ns, account_namespace=NAMESPACE)[0]["rules"]
    )
