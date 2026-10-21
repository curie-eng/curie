"""Administrator installation bootstrap of a local install, @spec PROTECTED-COMMISSIONING-1.

Design decisions 7 and 8 have the administrator-run installation bootstrap,
not the provisioner, issue every broker credential and record the ACL
principal state baseline. This module is that step for a local installation,
run on its own object before the provisioner builds B, G and M:

* ``issue`` generates one password per principal and the broker's one-time
  installer password, and writes each into its own Secret;
* ``enroll`` creates every ACL user over the installer session with
  ``ACL SETUSER ... >password``, so the broker derives the password hash and
  nothing here hashes a password; it records the baseline (each principal's
  password hash set and enabled state, read back with ``ACL GETUSER``), then
  turns the installer off and resets its password;
* ``grant`` gives the provisioner only usernames and rules, its own
  credential and the workload credentials it places in files it renders. The
  provisioner never holds the observer's, the Qualification publisher's or any
  commissioning principal's password.
"""

from __future__ import annotations

import base64
import json
import os
import secrets
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, NoReturn

import yaml
from redis.exceptions import AuthenticationError, ResponseError

from .commissioning_acl import CANDIDATE_ROLES
from .commissioning_local_broker import (
    BROKER_TLS_SECRET,
    CANDIDATE_IDLE,
    INSTALLATION_PRINCIPALS,
    BrokerUser,
    LocalBroker,
    Principal,
    namespace_object,
    pinned_session,
)
from .commissioning_local_installation import OWNER_LABEL, LocalCluster
from .commissioning_producers import AclPrincipalState, parse_getuser_reply
from .commissioning_provisioner import GUARD_PRINCIPALS

__all__ = [
    "INSTALLER_SECRET",
    "PROVISIONER_ACCOUNT",
    "PROVISIONER_KUBECONFIG",
    "PROVISIONER_SECRETS",
    "WORKLOAD_PRINCIPALS",
    "AdministratorBootstrap",
    "BootstrapRefused",
    "ProvisionerGrant",
    "close_installer",
    "credential_secret_name",
    "credentials_namespace",
    "enroll_users",
    "issue_credentials",
    "provisioner_rbac",
    "read_baseline",
]

# @spec PROTECTED-COMMISSIONING-1
INSTALLER_SECRET = "commissioning-broker-installer"
PROVISIONER_ACCOUNT = "commissioning-provisioner"
PROVISIONER_KUBECONFIG = "provisioner-kubeconfig"
# The only Secrets the provisioner's identity may read or delete by name.
PROVISIONER_SECRETS = (BROKER_TLS_SECRET,)
# Credentials the provisioner writes into the runtime files it renders, when G has them.
WORKLOAD_PRINCIPALS = ("enqueue", "control_reader", "source_writer")
_BASELINE = "acl-baseline.json"


class BootstrapRefused(RuntimeError):
    """Redacted bootstrap refusal, @spec PROTECTED-COMMISSIONING-1."""

    def __init__(self) -> None:
        """Never echo a credential or reply, @spec PROTECTED-COMMISSIONING-1."""
        super().__init__("Commissioning installation bootstrap refused")


def _refuse() -> NoReturn:
    raise BootstrapRefused() from None


def _require(condition: bool) -> None:
    if not condition:
        _refuse()


def credentials_namespace(namespace: str) -> str:
    """The namespace holding the credential Secrets, @spec PROTECTED-COMMISSIONING-1."""
    return namespace + "-credentials"


def credential_secret_name(principal: str) -> str:
    """The Secret one principal's credential lives in, @spec PROTECTED-COMMISSIONING-1."""
    return "commissioning-credential-" + principal.replace("_", "-")


@dataclass(frozen=True, slots=True, repr=False)
class ProvisionerGrant:
    """Everything the provisioner receives from the bootstrap, @spec PROTECTED-COMMISSIONING-1.

    ``principals`` are usernames and rules only. ``provisioner`` is its own
    credential and ``workload`` the credentials it renders into runtime files.
    """

    principals: Mapping[str, Principal]
    provisioner: BrokerUser = field(repr=False)
    workload: Mapping[str, BrokerUser] = field(repr=False)
    # The provisioner's own Kubernetes identity: a ServiceAccount token kubeconfig.
    kubeconfig: Path

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-1."""
        return "ProvisionerGrant()"


def provisioner_rbac(
    owner: str, namespace: str, *, readable_secrets: tuple[str, ...]
) -> list[dict[str, Any]]:
    """The provisioner's ServiceAccount, Role and RoleBinding, @spec PROTECTED-COMMISSIONING-1.

    The Role lets the provisioner create and remove its workloads, wait on
    them and port-forward to them. It may create Secrets but reads or deletes
    only the ones named in ``readable_secrets``: Kubernetes cannot restrict
    ``create`` by name, but it can refuse ``get``, ``list`` and ``watch``, so
    no credential Secret of another principal is readable with this identity.
    """
    labels = {OWNER_LABEL: owner}
    meta = {"name": PROVISIONER_ACCOUNT, "namespace": namespace, "labels": labels}
    rules = [
        {
            "apiGroups": [""],
            "resources": ["pods", "services"],
            "verbs": ["create", "get", "list", "watch", "delete"],
        },
        {"apiGroups": [""], "resources": ["pods/portforward"], "verbs": ["create"]},
        {
            "apiGroups": ["apps"],
            "resources": ["deployments"],
            "verbs": ["create", "get", "list", "watch", "delete"],
        },
        {"apiGroups": [""], "resources": ["secrets"], "verbs": ["create"]},
    ]
    if readable_secrets:
        rules.append(
            {
                "apiGroups": [""],
                "resources": ["secrets"],
                "resourceNames": list(readable_secrets),
                "verbs": ["get", "delete"],
            }
        )
    return [
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": dict(meta),
            "automountServiceAccountToken": False,
        },
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "Role",
            "metadata": dict(meta),
            "rules": rules,
        },
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": dict(meta),
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "Role",
                "name": PROVISIONER_ACCOUNT,
            },
            "subjects": [
                {"kind": "ServiceAccount", "name": PROVISIONER_ACCOUNT, "namespace": namespace}
            ],
        },
    ]


def _token_kubeconfig(admin: Path, *, owner: str, namespace: str, token: str) -> dict[str, Any]:
    """A kubeconfig for the same API server and CA as ``admin``, with a token user."""
    value = yaml.safe_load(admin.read_text())
    clusters = [c for c in value["clusters"] if c["name"] == owner]
    _require(len(clusters) == 1)
    cluster = clusters[0]["cluster"]
    _require(type(cluster.get("server")) is str and cluster["server"].startswith("https://"))
    kept = {
        k: cluster[k]
        for k in ("server", "certificate-authority", "certificate-authority-data")
        if k in cluster
    }
    _require("certificate-authority" in kept or "certificate-authority-data" in kept)
    return {
        "apiVersion": "v1",
        "kind": "Config",
        "clusters": [{"name": owner, "cluster": kept}],
        "users": [{"name": PROVISIONER_ACCOUNT, "user": {"token": token}}],
        "contexts": [
            {
                "name": owner,
                "context": {"cluster": owner, "user": PROVISIONER_ACCOUNT, "namespace": namespace},
            }
        ],
        "current-context": owner,
    }


def _b64(value: str) -> str:
    return base64.b64encode(value.encode("ascii")).decode("ascii")


def issue_credentials(principals: Mapping[str, Principal]) -> dict[str, BrokerUser]:
    """One fresh password per principal, @spec PROTECTED-COMMISSIONING-1."""
    _require(set(principals) == {*GUARD_PRINCIPALS, *INSTALLATION_PRINCIPALS})
    _require(all(type(p) is Principal for p in principals.values()))
    return {
        name: BrokerUser(p.username, p.rules, secrets.token_urlsafe(32))
        for name, p in principals.items()
    }


def enroll_users(
    session: Any, credentials: Mapping[str, BrokerUser]
) -> dict[str, AclPrincipalState]:
    """Create every ACL user on an installer session, then record the baseline.

    The broker derives each password hash from ``>password``. Candidates are
    created off with no right; design decision 9 enables them per campaign.
    Returns every created user's ``ACL GETUSER`` state: design decision 7
    covers every identity in the principals table, the provisioner and the
    observer included.
    @spec PROTECTED-COMMISSIONING-1.
    """
    try:
        _require(set(credentials) == {*GUARD_PRINCIPALS, *INSTALLATION_PRINCIPALS})
        for name, user in credentials.items():
            candidate = name in CANDIDATE_ROLES
            rules = CANDIDATE_IDLE if candidate else user.rules
            state = "off" if candidate else "on"
            reply = session.execute_command(
                "ACL", "SETUSER", user.username, "reset", state, ">" + user.password, *rules
            )
            _require(reply in (b"OK", "OK", True))
        return {
            name: parse_getuser_reply(
                session.execute_command("ACL", "GETUSER", credentials[name].username)
            )
            for name in credentials
        }
    except BootstrapRefused:
        raise
    except Exception:  # noqa: BLE001  One redacted refusal.
        _refuse()


def close_installer(session: Any) -> None:
    """Turn the installer (default) user off and reset its password.

    @spec PROTECTED-COMMISSIONING-1.
    """
    try:
        reply = session.execute_command(
            "ACL", "SETUSER", "default", "off", "resetpass", *CANDIDATE_IDLE
        )
        _require(reply in (b"OK", "OK", True))
    except BootstrapRefused:
        raise
    except Exception:  # noqa: BLE001  One redacted refusal.
        _refuse()


class AdministratorBootstrap:
    """Issue credentials, enroll the broker users and record the baseline.

    @spec PROTECTED-COMMISSIONING-1.
    """

    def __init__(
        self,
        cluster: LocalCluster,
        *,
        namespace: str,
        principals: Mapping[str, Principal],
        baseline_dir: Path,
        provisioner_dir: Path,
    ) -> None:
        """The administrator's own cluster view, directories and namespaces.

        ``cluster`` holds the admin kubeconfig. ``provisioner_dir`` is where the
        provisioner's kubeconfig is written; it must not be the directory of
        the admin kubeconfig. @spec PROTECTED-COMMISSIONING-1.
        """
        _require(type(cluster) is LocalCluster)
        _require(provisioner_dir.resolve() != cluster.kubeconfig.parent.resolve())
        _require(set(principals) == {*GUARD_PRINCIPALS, *INSTALLATION_PRINCIPALS})
        _require(all(type(p) is Principal for p in principals.values()))
        self._cluster, self._namespace = cluster, namespace
        self._credentials_namespace = credentials_namespace(namespace)
        self._provisioner_dir = provisioner_dir
        self._principals = dict(principals)
        self._baseline_dir = baseline_dir
        self.__credentials: dict[str, BrokerUser] = {}
        self.__installer: str | None = None
        self._kubeconfig: Path | None = None

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-1."""
        return "AdministratorBootstrap()"

    def issue(self) -> None:
        """Fresh credentials, each written directly into its own Secret.

        @spec PROTECTED-COMMISSIONING-1.
        """
        _require(not self.__credentials and self.__installer is None)
        credentials = issue_credentials(self._principals)
        installer = secrets.token_urlsafe(32)
        owner = self._cluster.owner
        owned = {OWNER_LABEL: owner}
        self._cluster.create(namespace_object(owner, self._namespace))
        # Every principal's credential lives in a namespace the provisioner has no
        # right in: no Secret read and no Pod, so no volume can reach it either.
        self._cluster.create(namespace_object(owner, self._credentials_namespace))
        for item in provisioner_rbac(owner, self._namespace, readable_secrets=PROVISIONER_SECRETS):
            self._cluster.create(item)
        for name, user in credentials.items():
            self._cluster.create(
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {
                        "name": credential_secret_name(name),
                        "namespace": self._credentials_namespace,
                        "labels": owned,
                    },
                    "type": "Opaque",
                    "immutable": True,
                    "data": {"username": _b64(user.username), "password": _b64(user.password)},
                }
            )
        self._cluster.create(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": INSTALLER_SECRET,
                    "namespace": self._namespace,
                    "labels": owned,
                },
                "type": "Opaque",
                "immutable": True,
                "data": {"password": _b64(installer)},
            }
        )
        self.__credentials, self.__installer = credentials, installer
        self._kubeconfig = self._mint_kubeconfig()

    def _mint_kubeconfig(self) -> Path:
        """Mint the provisioner's ServiceAccount token into its own private kubeconfig."""
        token = (
            self._cluster.kubectl(
                "create", "token", PROVISIONER_ACCOUNT, "--namespace", self._namespace,
                "--duration", "24h",
            )
            .decode("ascii")
            .strip()
        )  # fmt: skip
        _require(bool(token) and " " not in token)
        value = _token_kubeconfig(
            self._cluster.kubeconfig,
            owner=self._cluster.owner,
            namespace=self._namespace,
            token=token,
        )
        path = self._provisioner_dir / PROVISIONER_KUBECONFIG
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
        with os.fdopen(os.open(path, flags, 0o600), "w", encoding="utf-8") as stream:
            json.dump(value, stream)
        return path

    def grant(self) -> ProvisionerGrant:
        """Usernames and rules, the provisioner's own credential, workload credentials.

        @spec PROTECTED-COMMISSIONING-1.
        """
        _require(bool(self.__credentials) and self._kubeconfig is not None)
        assert self._kubeconfig is not None
        return ProvisionerGrant(
            kubeconfig=self._kubeconfig,
            principals=dict(self._principals),
            provisioner=self.__credentials["provisioner"],
            workload={
                name: self.__credentials[name]
                for name in WORKLOAD_PRINCIPALS
                if name in self.__credentials
            },
        )

    def enroll(self, broker: LocalBroker) -> dict[str, AclPrincipalState]:
        """Create every ACL user, record the baseline, then turn the installer off.

        Candidates are created off with no right: design decision 9 enables
        them per campaign. The baseline covers every checked principal of
        design decision 7 and is also written, without passwords, to a private
        file. @spec PROTECTED-COMMISSIONING-1.
        """
        _require(bool(self.__credentials) and self.__installer is not None)
        assert self.__installer is not None
        try:
            installer = BrokerUser("default", (), self.__installer)
            session = pinned_session(broker.material, broker.dial, installer)
            try:
                baseline = enroll_users(session, self.__credentials)
                close_installer(session)
            finally:
                session.close()
            try:
                pinned_session(broker.material, broker.dial, installer).ping()
            except (AuthenticationError, ResponseError):
                pass
            else:
                _refuse()
        except BootstrapRefused:
            broker.close()
            raise
        except Exception:  # noqa: BLE001  One redacted refusal; the forward never outlives it.
            broker.close()
            _refuse()
        except BaseException:
            broker.close()
            raise
        self.__installer = None
        self._write_baseline(baseline)
        return baseline

    def _write_baseline(self, baseline: Mapping[str, AclPrincipalState]) -> None:
        record = {
            name: {
                "username": self._principals[name].username,
                "password_hashes": sorted(state.password_hashes),
                "enabled": state.enabled,
            }
            for name, state in baseline.items()
        }
        self._baseline_dir.mkdir(mode=0o700, exist_ok=True)
        path = self._baseline_dir / _BASELINE
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
        with os.fdopen(os.open(path, flags, 0o600), "w", encoding="utf-8") as stream:
            json.dump(record, stream, sort_keys=True, indent=2)
            stream.write("\n")

    def credentials(self) -> Mapping[str, BrokerUser]:
        """Every issued credential, for the administrator's own placement only.

        @spec PROTECTED-COMMISSIONING-1.
        """
        _require(bool(self.__credentials))
        return dict(self.__credentials)


def read_baseline(baseline_dir: Path) -> dict[str, Any]:
    """The recorded baseline file, @spec PROTECTED-COMMISSIONING-1."""
    value: dict[str, Any] = json.loads((baseline_dir / _BASELINE).read_text())
    return value
