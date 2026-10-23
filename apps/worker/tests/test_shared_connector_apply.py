"""The worker reconciles shared applications from the production loop (ADR 0239, cp5 AC3).

Every case drives ``ConnectorReconcileLoop.one_pass()`` built by
``run._build_connector_loop`` from a real ``WorkerConfig``. Only the two
Kubernetes clients are replaced, by one recorded-shape fake of the cluster
(``RecordedCluster``); ``HttpManifestSource`` and ``HttpSharedApplicationApi``
are the production classes given an ``httpx.Client`` whose transport is the
real API (``shared_app``: real routes, real store, the module's own Postgres
16, real RustFS and Valkey). Agents, bundles, deployments and memberships go
through the real API routes. No case calls ``ensure_application`` or a reach
method directly.

Fixture order (r2-6, ``World.member``): register the application, create the
agent, add the membership, upload the bundle naming the alias, then create an
active deployment, because upload resolves membership. A case that seeds a
pre-existing Secret seeds it into the fake and finishes ``World.member``
before its first pass, and reads the render route once to show the
application is declared when the loop runs.

The bundled database is provisioned by the operator only after a first pass
registered the worker's recipient, because the API seals the DSN to the
current recipient: pass 1 creates the key Secret and registers its public
key, provisioning follows, and pass 2 creates the DSN Secret and applies the
application objects (``World.converge``).

This module is also the reconcile harness ``test_shared_connector_ownership.py``
and ``test_shared_connector_declarations.py`` import (the same pattern as
``reconcile/test_connector_target_ranking.py``). Production modules that do
not exist yet are imported inside the helpers, so a missing one fails the
tests that need it rather than collection.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import functools
import hashlib
import importlib.util
import io
import itertools
import json
import logging
import sys
import tarfile
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import nacl.public
import pytest
import yaml

# The connector_proxy fixtures (a disposable module database migrated to head,
# role cleanup, the Valkey contract) are reused as they are. The file is
# loaded by path because these suites live beside, not under, that directory;
# loading it also puts the API test support modules on sys.path.
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
_FIXTURES_PATH = HERE / "connector_proxy" / "conftest.py"
_spec = importlib.util.spec_from_file_location("_shared_reconcile_proxy_fixtures", _FIXTURES_PATH)
assert _spec is not None and _spec.loader is not None
_proxy_fixtures = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_proxy_fixtures)
shared_database = _proxy_fixtures.shared_database
cleanup = _proxy_fixtures.cleanup
sessions = _proxy_fixtures.sessions
_required_valkey = _proxy_fixtures._required_valkey

from _migration_support import sql_rows  # noqa: E402
from _shared_connector_support import (  # noqa: E402
    PROXY_IMAGE,
    SharedCleanup,
    SharedStore,
    operator_headers,
    random_installation,
    role_name_for,
    shared_app,
)
from _shared_proxy_harness import signing_pair  # noqa: E402
from curie_api.config import get_settings  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from kubernetes.client.exceptions import ApiException  # noqa: E402
from plugin_format.shared_connector_render import (  # noqa: E402
    SharedApplication,
    SharedMember,
    render_application,
    render_member_reach,
)
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine  # noqa: E402
from sqlalchemy.pool import NullPool  # noqa: E402

# The registered application's root (``_shared_connector_support.application_spec``);
# the worker is configured for the same release, namespace and app name.
RELEASE = "acme"
NAMESPACE = "acme"
APP_NAME = "curie"
API_BASE = "http://testserver"
ALIAS = "shared-a"
DEDICATED = "grafana"
DEDICATED_IMAGE = "example.com/acme/grafana@sha256:" + "d" * 64
DEDICATED_SPEC: dict[str, Any] = {"image": DEDICATED_IMAGE, "args": ["-t", "streamable-http"]}

APPLICATION_LABEL = "curietech.ai/shared-application"
AGENT_LABEL = "curietech.ai/agent-id"
SECRET_ROLE_LABEL = "curietech.ai/shared-secret"
COMPONENT_LABEL = "app.kubernetes.io/component"
PART_OF_LABEL = "app.kubernetes.io/part-of"
COMPONENT = "shared-connector-application"
OWNER_LABEL = "curie.dev/connector-owner"
KEY_SECRET_PREFIX, KEY_SECRET_KEY = "shared-key-", "private-keys"
DSN_SECRET_PREFIX, DSN_SECRET_KEY = "shared-dsn-", "sealed-dsn"
APPLICATION_KINDS = ("Deployment", "Service", "NetworkPolicy")

STATUS_FIXTURES = HERE / "fixtures" / "shared_deployment_status"
WORKER_LOGGER = "curie_worker"


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def b64(raw: bytes | str) -> str:
    data = raw.encode() if isinstance(raw, str) else raw
    return base64.b64encode(data).decode("ascii")


def key_secret_name(application_id: str) -> str:
    return KEY_SECRET_PREFIX + application_id


def dsn_secret_name(application_id: str) -> str:
    return DSN_SECRET_PREFIX + application_id


def worker_secret_labels(application_id: str, role: str) -> dict[str, str]:
    return {
        APPLICATION_LABEL: application_id,
        SECRET_ROLE_LABEL: role,
        COMPONENT_LABEL: COMPONENT,
        PART_OF_LABEL: RELEASE,
    }


def key_secret_data(private_key: nacl.public.PrivateKey) -> dict[str, str]:
    """The key Secret's ``data``: one base64 private key line, base64 encoded."""

    return {KEY_SECRET_KEY: b64(b64(bytes(private_key)) + "\n")}


def recorded_status(name: str) -> dict[str, Any]:
    shape: dict[str, Any] = json.loads((STATUS_FIXTURES / f"{name}.json").read_text())
    assert shape["_source"], name
    return shape


def template_hash(deployment: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical(deployment["spec"]["template"])).hexdigest()


def logged(caplog: pytest.LogCaptureFixture, code: str) -> bool:
    return any(code in record.getMessage() for record in caplog.records)


# =============================================================================
# The recorded-shape Kubernetes fake
# =============================================================================


class KubernetesRefused(AssertionError):
    """A call the reconciler must never make. Recorded, then raised."""


def _selector_matches(labels: Mapping[str, str], selector: str | Mapping[str, str]) -> bool:
    """Equality-based label selectors, as the API server evaluates them.

    Kubernetes API reference, "Labels and Selectors": ``k=v``, ``k==v``,
    ``k!=v``, ``k`` (exists) and ``!k`` (does not exist), comma separated and
    ANDed. Anything else is refused so an unsupported selector cannot pass
    by accident.
    """

    if isinstance(selector, Mapping):
        return all(labels.get(key) == value for key, value in selector.items())
    for term in (part.strip() for part in selector.split(",") if part.strip()):
        if "!=" in term:
            key, value = term.split("!=", 1)
            if labels.get(key.strip()) == value.strip():
                return False
        elif "==" in term or "=" in term:
            key, value = term.split("==", 1) if "==" in term else term.split("=", 1)
            if labels.get(key.strip()) != value.strip():
                return False
        elif term.startswith("!"):
            if term[1:].strip() in labels:
                return False
        elif any(char in term for char in "() "):
            raise KubernetesRefused(f"unsupported label selector term {term!r}")
        elif term not in labels:
            return False
    return True


class RecordedCluster:
    """One namespace of a Kubernetes API server, with the shapes it really answers.

    Recorded shapes and where they come from:

    1. List responses (``GET .../deployments?labelSelector=``): the items of a
       ``*List`` envelope carry no ``kind`` or ``apiVersion`` (Kubernetes API
       reference, list operations; the reading ``connector_k8s.list_owned``
       already relies on when it re-adds both). ``list_labelled`` and
       ``list_secret`` answer items in that shape.
    2. ``POST .../secrets`` for a name that exists answers 409 with ``Status``
       reason ``AlreadyExists`` (Kubernetes API conventions, HTTP status
       codes); ``create_secret`` answers ``"exists"`` for it and changes
       nothing.
    3. Server-side apply (``PATCH`` ``application/apply-patch+yaml``,
       ``fieldManager``, ``force=true``) stores the applied object; the server
       keeps ``uid`` and ``creationTimestamp``, bumps ``resourceVersion`` only
       when the object changes, and bumps ``metadata.generation`` of a
       Deployment only when its ``spec`` changes (API conventions,
       "metadata.generation").
    4. ``DELETE`` of a missing object answers 404 ``NotFound``; a failed
       apply raises ``ApiException`` as the official client does.
    5. Deployment ``status`` is a recorded shape from
       ``fixtures/shared_deployment_status`` (see each file's ``_source``),
       carried onto the live object's own generation.
    6. The metadata-only read (``Accept:
       application/json;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1``)
       returns no ``data``: here it answers presence only.

    The fake never reads what it stored back through any production
    serializer; tests compare its records with independently computed
    renders.
    """

    def __init__(self, namespace: str = NAMESPACE) -> None:
        self.namespace = namespace
        self.objects: dict[tuple[str, str], dict[str, Any]] = {}
        # (verb, kind, name) for every call, in order. Verbs: list, apply,
        # delete, create (a Secret create, created or 409), list-secret,
        # metadata, dedicated-list, dedicated-apply, dedicated-delete.
        self.calls: list[tuple[str, str, str]] = []
        self.sent: list[dict[str, Any]] = []
        self.dedicated_sent: list[dict[str, Any]] = []
        self.conflicts: list[str] = []
        self.violations: list[str] = []
        self.operator_secrets: set[str] = set()
        self.statuses: dict[str, dict[str, Any]] = {}
        self.fail_apply: Callable[[Mapping[str, Any]], bool] = lambda _obj: False
        # Names whose next exact list misses them: a create race another
        # writer won between this worker's list and its create.
        self.hidden_secrets: set[str] = set()
        self.after_create: Callable[[dict[str, Any]], None] | None = None
        self._versions = itertools.count(100)

    # --- bookkeeping -------------------------------------------------------

    def refuse(self, message: str) -> KubernetesRefused:
        self.violations.append(message)
        return KubernetesRefused(message)

    def _namespace(self, namespace: str) -> None:
        if namespace != self.namespace:
            raise self.refuse(f"a call outside the release namespace: {namespace!r}")

    def calls_since(self, mark: int) -> list[tuple[str, str, str]]:
        return self.calls[mark:]

    def _store(self, obj: Mapping[str, Any]) -> dict[str, Any]:
        kind = str(obj["kind"])
        name = str(obj["metadata"]["name"])
        previous = self.objects.get((kind, name))
        stored = copy.deepcopy(dict(obj))
        metadata = stored.setdefault("metadata", {})
        metadata["namespace"] = self.namespace
        if previous is not None:
            old_meta = previous["metadata"]
            metadata["uid"] = old_meta["uid"]
            metadata["creationTimestamp"] = old_meta["creationTimestamp"]
            if kind == "Deployment":
                same_spec = canonical(previous.get("spec")) == canonical(stored.get("spec"))
                generation = int(old_meta["generation"])
                metadata["generation"] = generation if same_spec else generation + 1
            unchanged = _without_server_fields(previous) == _without_server_fields(stored)
            metadata["resourceVersion"] = (
                old_meta["resourceVersion"] if unchanged else str(next(self._versions))
            )
        else:
            metadata["uid"] = str(uuid.uuid4())
            metadata["creationTimestamp"] = "2026-10-23T00:00:00Z"
            metadata["resourceVersion"] = str(next(self._versions))
            if kind == "Deployment":
                metadata["generation"] = 1
        self.objects[(kind, name)] = stored
        return stored

    def _listed(self, stored: Mapping[str, Any]) -> dict[str, Any]:
        item = copy.deepcopy(dict(stored))
        item.pop("kind", None)
        item.pop("apiVersion", None)
        if stored.get("kind") == "Deployment":
            name = stored["metadata"]["name"]
            shape = self.statuses.get(name)
            if shape is not None:
                offset = int(shape["status"]["observedGeneration"]) - int(
                    shape["metadata"]["generation"]
                )
                status = copy.deepcopy(shape["status"])
                status["observedGeneration"] = int(stored["metadata"]["generation"]) + offset
                item["status"] = status
        return item

    # --- seeding (the cluster administrator's writes) ----------------------

    def seed_secret(
        self,
        name: str,
        *,
        labels: Mapping[str, str],
        data: Mapping[str, str],
        secret_type: str = "Opaque",
        operator: bool = False,
    ) -> dict[str, Any]:
        stored = self._store(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": name, "labels": dict(labels)},
                "type": secret_type,
                "data": dict(data),
            }
        )
        if operator:
            self.operator_secrets.add(name)
        return copy.deepcopy(stored)

    def seed_operator_secret(self, name: str, key: str) -> dict[str, Any]:
        return self.seed_secret(
            name,
            labels={"app.kubernetes.io/managed-by": "operator"},
            data={key: b64(f"operator-{uuid.uuid4().hex}")},
            operator=True,
        )

    def set_status(self, deployment_name: str, shape: str) -> None:
        self.statuses[deployment_name] = recorded_status(shape)

    # --- observations -------------------------------------------------------

    def secret(self, name: str) -> dict[str, Any] | None:
        stored = self.objects.get(("Secret", name))
        return copy.deepcopy(stored) if stored is not None else None

    def reach(self, agent_id: str | None = None) -> dict[str, dict[str, Any]]:
        return {
            name: obj
            for (kind, name), obj in self.objects.items()
            if kind == "NetworkPolicy"
            and AGENT_LABEL in (obj["metadata"].get("labels") or {})
            and (agent_id is None or obj["metadata"]["labels"][AGENT_LABEL] == agent_id)
        }

    def application_objects(self, application_id: str) -> dict[tuple[str, str], dict[str, Any]]:
        return {
            key: obj
            for key, obj in self.objects.items()
            if key[0] in APPLICATION_KINDS
            and (obj["metadata"].get("labels") or {}).get(APPLICATION_LABEL) == application_id
            and AGENT_LABEL not in obj["metadata"]["labels"]
        }

    def sent_named(self, kind: str, name: str) -> list[dict[str, Any]]:
        return [obj for obj in self.sent if obj["kind"] == kind and obj["metadata"]["name"] == name]


def _without_server_fields(obj: Mapping[str, Any]) -> bytes:
    trimmed = copy.deepcopy(dict(obj))
    metadata = trimmed.get("metadata") or {}
    for name in ("uid", "resourceVersion", "creationTimestamp", "generation"):
        metadata.pop(name, None)
    trimmed.pop("status", None)
    return canonical(trimmed)


class _Facade:
    def __init__(self, cluster: RecordedCluster) -> None:
        self.cluster = cluster

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        raise self.cluster.refuse(f"a Kubernetes call outside the recorded surface: {name}")


class FakeSharedClient(_Facade):
    """The ``KubernetesSharedClient`` surface (plan S2 item 8) over ``RecordedCluster``."""

    def list_labelled(
        self, namespace: str, kind: str, selector: str | Mapping[str, str]
    ) -> list[dict[str, Any]]:
        cluster = self.cluster
        cluster._namespace(namespace)
        cluster.calls.append(("list", kind, str(selector)))
        if kind not in APPLICATION_KINDS:
            raise cluster.refuse(f"the shared reconciler listed {kind}")
        return [
            cluster._listed(obj)
            for (stored_kind, _), obj in sorted(cluster.objects.items())
            if stored_kind == kind
            and _selector_matches(obj["metadata"].get("labels") or {}, selector)
        ]

    def apply(self, namespace: str, obj: dict[str, Any]) -> None:
        cluster = self.cluster
        cluster._namespace(namespace)
        kind, name = str(obj.get("kind")), str(obj["metadata"]["name"])
        cluster.calls.append(("apply", kind, name))
        if kind == "Secret":
            raise cluster.refuse(f"server-side apply of Secret {name}")
        if OWNER_LABEL in (obj["metadata"].get("labels") or {}):
            raise cluster.refuse(f"a shared object carrying {OWNER_LABEL}: {name}")
        cluster.sent.append(copy.deepcopy(obj))
        if cluster.fail_apply(obj):
            raise ApiException(status=422, reason="Unprocessable Entity")
        cluster._store(obj)

    def delete(self, namespace: str, kind: str, name: str) -> None:
        cluster = self.cluster
        cluster._namespace(namespace)
        cluster.calls.append(("delete", kind, name))
        if kind == "Secret":
            raise cluster.refuse(f"delete of Secret {name}")
        if cluster.objects.pop((kind, name), None) is None:
            raise ApiException(status=404, reason="Not Found")

    def create_secret(self, namespace: str, body: dict[str, Any]) -> str:
        cluster = self.cluster
        cluster._namespace(namespace)
        name = str(body["metadata"]["name"])
        cluster.calls.append(("create", "Secret", name))
        if "stringData" in body:
            raise cluster.refuse(f"Secret {name} sent stringData")
        if ("Secret", name) in cluster.objects:
            cluster.conflicts.append(name)  # 409 AlreadyExists; nothing changes
            return "exists"
        stored = cluster._store({"apiVersion": "v1", "kind": "Secret", **copy.deepcopy(body)})
        if cluster.after_create is not None:
            cluster.after_create(stored)
        return "created"

    def list_secret(
        self, namespace: str, name: str, labels: Mapping[str, str]
    ) -> dict[str, Any] | None:
        cluster = self.cluster
        cluster._namespace(namespace)
        cluster.calls.append(("list-secret", "Secret", name))
        if name in cluster.operator_secrets:
            raise cluster.refuse(f"read the data of operator Secret {name}")
        if name in cluster.hidden_secrets:
            cluster.hidden_secrets.discard(name)
            return None
        stored = cluster.objects.get(("Secret", name))
        if stored is None or not _selector_matches(stored["metadata"].get("labels") or {}, labels):
            return None
        return cluster._listed(stored)

    def secret_exists_metadata_only(self, namespace: str, name: str) -> bool:
        cluster = self.cluster
        cluster._namespace(namespace)
        cluster.calls.append(("metadata", "Secret", name))
        return ("Secret", name) in cluster.objects


class FakeConnectorClient(_Facade):
    """The dedicated ``ConnectorClient`` surface over the same cluster."""

    def list_owned(self, namespace: str, owner: str) -> list[dict[str, Any]]:
        cluster = self.cluster
        cluster._namespace(namespace)
        cluster.calls.append(("dedicated-list", "*", owner))
        found = []
        for (kind, _), obj in sorted(cluster.objects.items()):
            if (obj["metadata"].get("labels") or {}).get(OWNER_LABEL) != owner:
                continue
            # connector_k8s.list_owned re-adds kind and apiVersion to list items.
            item = copy.deepcopy(obj)
            item["kind"] = kind
            found.append(item)
        return found

    def apply(self, namespace: str, obj: dict[str, Any]) -> None:
        cluster = self.cluster
        cluster._namespace(namespace)
        name = str(obj["metadata"]["name"])
        cluster.calls.append(("dedicated-apply", str(obj.get("kind")), name))
        if name.startswith(("shared-", KEY_SECRET_PREFIX, DSN_SECRET_PREFIX)):
            raise cluster.refuse(f"the dedicated reconciler applied shared object {name}")
        cluster.dedicated_sent.append(copy.deepcopy(obj))
        cluster._store(obj)

    def delete(self, namespace: str, kind: str, name: str) -> None:
        cluster = self.cluster
        cluster._namespace(namespace)
        cluster.calls.append(("dedicated-delete", kind, name))
        if name.startswith(("shared-", KEY_SECRET_PREFIX, DSN_SECRET_PREFIX)):
            raise cluster.refuse(f"the dedicated reconciler deleted shared object {name}")
        if cluster.objects.pop((kind, name), None) is None:
            raise ApiException(status=404, reason="Not Found")


# =============================================================================
# The API, reached over a transport the test can observe
# =============================================================================


_FRAMING_HEADERS = ("host", "content-length", "transfer-encoding")


class ApiTransport(httpx.BaseTransport):
    """Hands each worker request to the real API app and records it.

    ``rules`` stand in for the network between the worker and the API: a rule
    may raise a transport error (``httpx.ConnectError``, ``ReadTimeout``) or
    answer for an unreachable upstream. The API itself is never replaced.
    """

    def __init__(self, client: TestClient) -> None:
        self.client = client
        self.requests: list[tuple[str, str]] = []
        self.rules: list[Callable[[httpx.Request], BaseException | httpx.Response | None]] = []

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        request.read()
        self.requests.append((request.method, request.url.path))
        for rule in self.rules:
            outcome = rule(request)
            if isinstance(outcome, BaseException):
                raise outcome
            if isinstance(outcome, httpx.Response):
                return outcome
        # Starlette's TestClient is built on a separate httpx distribution
        # (``httpx2``), so the worker's request object cannot cross as is: the
        # same method, URL, headers and body are sent again through it. The
        # framing headers are left for each client to compute from the body.
        answered = self.client.request(
            request.method,
            str(request.url),
            content=request.content,
            headers=[
                (name, value)
                for name, value in request.headers.multi_items()
                if name.lower() not in _FRAMING_HEADERS
            ],
            follow_redirects=False,
        )
        # The TestClient hands back the decoded body, so a content-encoding
        # header would make the worker's client decode it twice.
        headers = [
            (name, value)
            for name, value in answered.headers.multi_items()
            if name.lower() not in (*_FRAMING_HEADERS, "content-encoding")
        ]
        return httpx.Response(
            answered.status_code, headers=headers, content=answered.content, request=request
        )

    def count(self, method: str, path: str) -> int:
        return sum(1 for seen in self.requests if seen == (method, path))


def render_path(agent_id: str, version_id: str) -> str:
    return f"/agents/{agent_id}/versions/{version_id}/connectors"


def status_path(application_id: str) -> str:
    return f"/v1/internal/shared-connectors/applications/{application_id}/database/status"


RESOLVE_PATH = "/v1/internal/shared-connectors/resolve"


def fail_render(agent_id: str, error: Callable[[httpx.Request], BaseException | httpx.Response]):
    def rule(request: httpx.Request) -> BaseException | httpx.Response | None:
        if request.url.path.startswith(f"/agents/{agent_id}/versions/"):
            return error(request)
        return None

    return rule


# =============================================================================
# Bundles
# =============================================================================


def archive(files: Mapping[str, str | bytes]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as bundle:
        for name, content in files.items():
            data = content.encode("utf-8") if isinstance(content, str) else content
            info = tarfile.TarInfo(name)
            info.size = len(data)
            bundle.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


def connectors_yaml(
    shared: Mapping[str, str], *, dedicated: Mapping[str, Any] | None = DEDICATED_SPEC
) -> str:
    connectors: dict[str, Any] = {}
    if dedicated is not None:
        connectors[DEDICATED] = dict(dedicated)
    for alias, application_id in shared.items():
        connectors[alias] = {"shared": {"application": application_id, "contract": 1}}
    return yaml.safe_dump({"connectors": connectors}, sort_keys=True)


def bundle_files(
    connectors: str | bytes | None, *, deploy: str | None = None
) -> dict[str, str | bytes]:
    files: dict[str, str | bytes] = {
        ".claude-plugin/plugin.json": json.dumps(
            {"name": "shared-test", "version": "0.1.0", "description": "t"}
        ),
        "skills/shared-test/SKILL.md": "---\nname: shared-test\ndescription: t\n---\nhi\n",
    }
    if connectors is not None:
        files["connectors.yaml"] = connectors
    if deploy is not None:
        files["deploy.yaml"] = deploy
    return files


# =============================================================================
# The world: the API, the cluster and the production loop
# =============================================================================


@dataclass(frozen=True)
class Deployed:
    """An agent with an active deployment of one version."""

    agent_id: str
    name: str
    version_id: str


@dataclass
class World:
    store: SharedStore
    transport: ApiTransport
    http: httpx.Client
    cluster: RecordedCluster
    monkeypatch: pytest.MonkeyPatch
    engine: AsyncEngine
    specs: dict[str, dict[str, Any]] = field(default_factory=dict)
    last: list[tuple[str, str, str]] = field(default_factory=list)
    _loop: Any = None
    _versions: Iterator[int] = field(default_factory=lambda: itertools.count(1))

    @property
    def client(self) -> TestClient:
        return self.store.client

    @property
    def installation(self) -> str:
        return self.store.installation

    # --- applications ------------------------------------------------------

    def application(self, *, dsn_encoding: str = "bundled-sealed") -> str:
        application_id = str(uuid.uuid4())
        body = self.store.registration_body(application_id, dsn_encoding=dsn_encoding)
        registered = self.store.operator("POST", "/applications", body)
        assert registered.status_code == 201, registered.text
        spec = body["application"]
        self.specs[application_id] = spec
        grant_store = spec["grant_store"]
        if ("Secret", grant_store["name"]) not in self.cluster.objects:
            self.cluster.seed_operator_secret(grant_store["name"], grant_store["key"])
        if dsn_encoding == "managed-raw":
            dsn = spec["database_dsn"]
            if ("Secret", dsn["name"]) not in self.cluster.objects:
                self.cluster.seed_operator_secret(dsn["name"], dsn["key"])
        return application_id

    def spec(self, application_id: str) -> SharedApplication:
        return SharedApplication.model_validate_json(json.dumps(self.specs[application_id]))

    def expected_application(self, application_id: str) -> list[dict[str, Any]]:
        return render_application(self.spec(application_id))

    def expected_reach(self, application_id: str, agent_id: str) -> list[dict[str, Any]]:
        member = SharedMember.model_validate_json(
            json.dumps(
                {
                    "agent": agent_id,
                    "runner_labels": {
                        AGENT_LABEL: agent_id,
                        COMPONENT_LABEL: "runner-sandbox",
                        "app.kubernetes.io/name": APP_NAME,
                        "app.kubernetes.io/instance": RELEASE,
                    },
                }
            )
        )
        return render_member_reach(self.spec(application_id), member)

    def deployment_name(self, application_id: str) -> str:
        (deployment,) = [
            obj for obj in self.expected_application(application_id) if obj["kind"] == "Deployment"
        ]
        return str(deployment["metadata"]["name"])

    def provision(self, application_id: str) -> dict[str, Any]:
        self.store.cleanup.role(role_name_for(self.installation, application_id))
        response = self.store.provision(application_id, {"mode": "bundled"})
        assert response.status_code == 201, response.text
        record: dict[str, Any] = response.json()
        return record

    def provision_managed(self, application_id: str) -> dict[str, Any]:
        role = self.store.cleanup.role(f"ops_shared_{uuid.uuid4().hex[:12]}")
        response = self.store.provision(application_id, {"mode": "managed", "role_name": role})
        assert response.status_code == 201, response.text
        record: dict[str, Any] = response.json()
        return record

    def record(self, application_id: str) -> dict[str, Any]:
        return self.store.application(application_id)

    def current_recipients(self, application_id: str) -> list[str]:
        return [
            str(recipient["recipient_id"])
            for recipient in self.record(application_id)["recipients"]
            if recipient["current"]
        ]

    def application_render(self, application_id: str) -> dict[str, Any]:
        response = self.store.worker(
            "GET",
            f"/applications/{application_id}/render?namespace={NAMESPACE}&release={RELEASE}",
        )
        assert response.status_code == 200, response.text
        body: dict[str, Any] = response.json()
        return body

    def disable(self, application_id: str) -> None:
        response = self.store.disable_application(application_id)
        assert response.status_code == 200, response.text

    # --- agents and bundles ------------------------------------------------

    def agent(self) -> tuple[str, str]:
        agent_id = self.store.create_agent()
        response = self.client.get(f"/agents/{agent_id}", headers=operator_headers())
        assert response.status_code == 200, response.text
        return agent_id, str(response.json()["name"])

    def _version(self, agent_id: str) -> str:
        response = self.client.post(
            f"/agents/{agent_id}/versions",
            json={"version_label": f"v{next(self._versions)}", "created_by": "shared-test"},
            headers=operator_headers(),
        )
        assert response.status_code == 201, response.text
        return str(response.json()["id"])

    def _deploy_version(self, agent_id: str, version_id: str) -> None:
        response = self.client.post(
            "/deployments",
            json={"agent_id": agent_id, "version_id": version_id, "environment": "dev"},
            headers=operator_headers(),
        )
        assert response.status_code == 201, response.text

    def upload(self, agent_id: str, version_id: str, data: bytes) -> httpx.Response:
        return self.client.put(
            f"/agents/{agent_id}/versions/{version_id}/bundle",
            files={"file": ("bundle.tar.gz", data)},
            headers=operator_headers(),
        )

    def deploy(
        self,
        agent: tuple[str, str] | Deployed,
        *,
        shared: Mapping[str, str] | None = None,
        dedicated: Mapping[str, Any] | None = DEDICATED_SPEC,
        deploy_yaml: str | None = None,
    ) -> Deployed:
        """A new version of ``agent`` with these connectors, uploaded and active."""

        agent_id, name = (agent.agent_id, agent.name) if isinstance(agent, Deployed) else agent
        version_id = self._version(agent_id)
        data = archive(
            bundle_files(connectors_yaml(shared or {}, dedicated=dedicated), deploy=deploy_yaml)
        )
        uploaded = self.upload(agent_id, version_id, data)
        assert uploaded.status_code == 201, uploaded.text
        self._deploy_version(agent_id, version_id)
        return Deployed(agent_id, name, version_id)

    def join(self, application_id: str, agent_id: str, aliases: Sequence[str] = (ALIAS,)) -> None:
        response = self.store.put_members(
            application_id, agent_id, [{"alias": alias, "tools": None} for alias in aliases]
        )
        assert response.status_code == 200, response.text

    def member(
        self,
        application_id: str,
        agent: tuple[str, str] | None = None,
        *,
        aliases: Sequence[str] = (ALIAS,),
        dedicated: Mapping[str, Any] | None = DEDICATED_SPEC,
    ) -> Deployed:
        """``_declared_member``: membership first, then the upload naming the alias."""

        agent = agent or self.agent()
        self.join(application_id, agent[0], aliases)
        return self.deploy(
            agent, shared={alias: application_id for alias in aliases}, dedicated=dedicated
        )

    def remove_member(self, application_id: str, agent_id: str) -> None:
        response = self.store.operator(
            "DELETE", f"/applications/{application_id}/members/{agent_id}"
        )
        assert response.status_code == 200, response.text

    def store_raw(self, agent: tuple[str, str] | Deployed, connectors: str | bytes) -> Deployed:
        """A deployed version whose bundle bypassed intake (written straight to the store)."""

        agent_id, name = (agent.agent_id, agent.name) if isinstance(agent, Deployed) else agent
        version_id = self._version(agent_id)
        # Deployed bundleless first, so no deployment gate ever reads it.
        self._deploy_version(agent_id, version_id)
        data = archive(bundle_files(connectors))
        key = f"bundles/{agent_id}/{version_id}.tar.gz"
        bundle_store = self.client.app.state.bundle_store
        self.client.portal.call(bundle_store.put, key, data, "application/gzip")
        sql_rows(
            "UPDATE curie.agent_versions SET bundle_ref = :key, bundle_sha256 = :digest "
            "WHERE id = :id",
            {"key": key, "digest": hashlib.sha256(data).hexdigest(), "id": uuid.UUID(version_id)},
        )
        return Deployed(agent_id, name, version_id)

    def render(self, deployed: Deployed) -> httpx.Response:
        return self.client.get(
            render_path(deployed.agent_id, deployed.version_id),
            params={"release": RELEASE, "namespace": NAMESPACE, "app_name": APP_NAME},
            headers=operator_headers(),
        )

    def assert_declared(self, deployed: Deployed, application_id: str) -> None:
        """The precondition r2-6 asks for: the render names the application."""

        response = self.render(deployed)
        assert response.status_code == 200, response.text
        assert application_id in {entry["application"] for entry in response.json()["shared"]}

    # --- the production loop ----------------------------------------------

    def build_loop(self, *, internal_worker_token: str | None = None) -> Any:
        from curie_worker import connector_k8s, connector_loop, run, shared_k8s, shared_reconcile
        from curie_worker.config import WorkerConfig

        cluster = self.cluster
        patch = self.monkeypatch
        originals = _ORIGINALS.setdefault(
            "classes",
            {
                "HttpManifestSource": connector_loop.HttpManifestSource,
                "HttpSharedApplicationApi": shared_reconcile.HttpSharedApplicationApi,
            },
        )
        patch.setattr(
            connector_k8s, "KubernetesConnectorClient", lambda **_: FakeConnectorClient(cluster)
        )
        shared_client = lambda **_: FakeSharedClient(cluster)  # noqa: E731
        patch.setattr(shared_k8s, "KubernetesSharedClient", shared_client)
        patch.setattr(run, "KubernetesSharedClient", shared_client, raising=False)
        manifests = functools.partial(originals["HttpManifestSource"], client=self.http)
        patch.setattr(run, "HttpManifestSource", manifests)
        application_api = functools.partial(originals["HttpSharedApplicationApi"], client=self.http)
        patch.setattr(shared_reconcile, "HttpSharedApplicationApi", application_api)
        patch.setattr(run, "HttpSharedApplicationApi", application_api, raising=False)
        settings = get_settings()
        config = WorkerConfig(
            connector_reconcile_enabled=True,
            connector_namespace=NAMESPACE,
            connector_release=RELEASE,
            connector_app_name=APP_NAME,
            api_base_url=API_BASE,
            api_key=settings.api_key,
            internal_worker_token=(
                settings.internal_worker_token
                if internal_worker_token is None
                else internal_worker_token
            ),
            action_executor_enabled=False,
        )
        loop = run._build_connector_loop(config, self.engine)
        assert loop is not None
        return loop

    @property
    def loop(self) -> Any:
        if self._loop is None:
            self._loop = self.build_loop()
        return self._loop

    def run_pass(self, loop: Any = None) -> Any:
        mark = len(self.cluster.calls)
        summary = asyncio.run((loop or self.loop).one_pass())
        self.last = self.cluster.calls_since(mark)
        return summary

    def converge(self, *applications: str) -> None:
        """Pass 1 registers recipients, the operator provisions, pass 2 applies."""

        self.run_pass()
        for application_id in applications:
            self.provision(application_id)
        self.run_pass()
        for application_id in applications:
            assert self.cluster.application_objects(application_id), application_id

    def last_of(self, verb: str, kind: str | None = None) -> list[str]:
        return [name for seen, k, name in self.last if seen == verb and kind in (None, k)]

    def drop(self, kind: str, name: str) -> None:
        """Drift: the object disappears from the cluster outside the reconciler."""

        assert self.cluster.objects.pop((kind, name), None) is not None, (kind, name)

    def close(self) -> None:
        self.http.close()
        asyncio.run(self.engine.dispose())


_ORIGINALS: dict[str, dict[str, Any]] = {}


@pytest.fixture
def world(
    shared_database: str, cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch
) -> Iterator[World]:
    # Only this test's agents are deployed: earlier tests in the module share
    # the database, and the loop reconciles every active deployment.
    sql_rows("UPDATE curie.deployments SET status = 'inactive' WHERE status = 'active'")
    installation = cleanup.installation(random_installation())
    _, caller_public = signing_pair()
    # A hosted dedicated connector renders only with a caller verify key.
    with shared_app(
        monkeypatch,
        installation,
        CURIE_CONNECTOR_CALLER_PUBLIC_KEY=caller_public,
        CURIE_CONNECTOR_PROXY_IMAGE=PROXY_IMAGE,
    ) as client:
        transport = ApiTransport(client)
        built = World(
            store=SharedStore(client, installation, cleanup),
            transport=transport,
            http=httpx.Client(transport=transport, timeout=30.0),
            cluster=RecordedCluster(),
            monkeypatch=monkeypatch,
            engine=create_async_engine(get_settings().database_url, poolclass=NullPool),
        )
        try:
            yield built
            assert built.cluster.violations == [], built.cluster.violations
        finally:
            built.close()
    monkeypatch.undo()
    get_settings.cache_clear()


@pytest.fixture
def worker_logs(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    caplog.set_level(logging.DEBUG, logger=WORKER_LOGGER)
    return caplog


# =============================================================================
# Assertions shared by the suites
# =============================================================================


def assert_key_secret(secret: Mapping[str, Any] | None, application_id: str) -> bytes:
    """The worker's key Secret shape; answers the raw private key it holds."""

    from curie_connector_proxy.resolver.keys import KeyRing

    assert secret is not None, "no key Secret"
    assert secret["metadata"]["name"] == key_secret_name(application_id)
    assert secret["metadata"]["labels"] == worker_secret_labels(application_id, "encryption-key")
    assert secret["type"] == "Opaque"
    assert set(secret["data"]) == {KEY_SECRET_KEY}
    assert "stringData" not in secret
    text = base64.b64decode(secret["data"][KEY_SECRET_KEY], validate=True).decode("ascii")
    ring = KeyRing.from_text(text)
    assert len(ring.recipient_ids) == 1
    lines = [line for line in text.splitlines() if line.strip()]
    assert len(lines) == 1 and text.endswith("\n"), "one key line, current first"
    raw = base64.b64decode(lines[0], validate=True)
    assert len(raw) == 32
    return raw


def recipient_id_of(private_raw: bytes) -> str:
    public = bytes(nacl.public.PrivateKey(private_raw).public_key)
    return hashlib.sha256(public).hexdigest()


def assert_application_objects_equal_render(world: World, application_id: str) -> None:
    """What the worker last applied for each object is the frozen render, byte for byte."""

    for expected in world.expected_application(application_id):
        sent = world.cluster.sent_named(expected["kind"], expected["metadata"]["name"])
        assert sent, (expected["kind"], expected["metadata"]["name"])
        assert canonical(sent[-1]) == canonical(expected), expected["metadata"]["name"]


def assert_reach_equal_render(world: World, application_id: str, agent_id: str) -> None:
    expected = {
        obj["metadata"]["name"]: obj for obj in world.expected_reach(application_id, agent_id)
    }
    live = world.cluster.reach(agent_id)
    assert set(live) == set(expected)
    for name, obj in expected.items():
        sent = world.cluster.sent_named("NetworkPolicy", name)
        assert sent and canonical(sent[-1]) == canonical(obj), name


# =============================================================================
# AC3
# =============================================================================


def test_first_pass_creates_secrets_registers_and_applies(world: World) -> None:
    application_id = world.application()
    member = world.member(application_id)
    world.assert_declared(member, application_id)

    world.run_pass()

    assert world.last_of("create", "Secret") == [key_secret_name(application_id)]
    private_raw = assert_key_secret(
        world.cluster.secret(key_secret_name(application_id)), application_id
    )
    # The registered recipient is the public half of the key the Secret holds.
    assert world.current_recipients(application_id) == [recipient_id_of(private_raw)]

    database = world.provision(application_id)
    world.run_pass()

    assert world.last_of("create", "Secret") == [dsn_secret_name(application_id)]
    dsn = world.cluster.secret(dsn_secret_name(application_id))
    assert dsn is not None
    assert dsn["metadata"]["labels"] == worker_secret_labels(application_id, "database-dsn")
    assert dsn["type"] == "Opaque"
    assert set(dsn["data"]) == {DSN_SECRET_KEY}
    content = base64.b64decode(dsn["data"][DSN_SECRET_KEY], validate=True).decode("ascii")
    assert content.strip() == database["sealed_dsn"]
    opened = nacl.public.SealedBox(nacl.public.PrivateKey(private_raw)).decrypt(
        base64.b64decode(content.strip(), validate=True)
    )
    assert opened.decode().startswith("postgresql://")
    # Two creates in all, each once.
    assert [name for verb, _, name in world.cluster.calls if verb == "create"] == [
        key_secret_name(application_id),
        dsn_secret_name(application_id),
    ]
    # Byte identity: the frozen render computed here, and the route's objects.
    assert_application_objects_equal_render(world, application_id)
    route = world.application_render(application_id)
    assert [canonical(obj) for obj in route["objects"]] == [
        canonical(obj) for obj in world.expected_application(application_id)
    ]
    assert_reach_equal_render(world, application_id, member.agent_id)


def test_second_pass_never_patches_or_recreates_secrets(world: World) -> None:
    application_id = world.application()
    world.member(application_id)
    world.converge(application_id)
    names = (key_secret_name(application_id), dsn_secret_name(application_id))
    before = {name: world.cluster.secret(name) for name in names}

    world.run_pass()

    assert world.last_of("create", "Secret") == []
    assert {name: world.cluster.secret(name) for name in names} == before
    # Liveness: the pass did reconcile the application.
    assert world.deployment_name(application_id) in world.last_of("apply", "Deployment")
    assert world.cluster.violations == []


@pytest.mark.parametrize("race", ["listed", "lost-create-race"])
def test_conflict_adopts_valid_secret(world: World, race: str) -> None:
    application_id = world.application()
    private = nacl.public.PrivateKey.generate()
    name = key_secret_name(application_id)
    seeded = world.cluster.seed_secret(
        name,
        labels=worker_secret_labels(application_id, "encryption-key"),
        data=key_secret_data(private),
    )
    if race == "lost-create-race":
        world.cluster.hidden_secrets.add(name)
    member = world.member(application_id)
    world.assert_declared(member, application_id)

    world.run_pass()

    assert world.cluster.secret(name) == seeded, "an adopted Secret is never written"
    assert world.current_recipients(application_id) == [recipient_id_of(bytes(private))]
    if race == "lost-create-race":
        # The create answered 409 AlreadyExists, then the exact list adopted it.
        assert name in world.cluster.conflicts
        assert world.last_of("list-secret").count(name) >= 2


_VALID = "valid"
_MALFORMED: dict[str, Callable[[str, nacl.public.PrivateKey], dict[str, Any]]] = {
    "extra-key": lambda app, key: {
        "labels": worker_secret_labels(app, "encryption-key"),
        "data": {**key_secret_data(key), "extra": b64("x")},
    },
    "wrong-type": lambda app, key: {
        "labels": worker_secret_labels(app, "encryption-key"),
        "data": key_secret_data(key),
        "secret_type": "kubernetes.io/basic-auth",
    },
    "wrong-label": lambda app, key: {
        "labels": worker_secret_labels(app, "database-dsn"),
        "data": key_secret_data(key),
    },
    "bad-base64": lambda app, key: {
        "labels": worker_secret_labels(app, "encryption-key"),
        "data": {KEY_SECRET_KEY: b64("not base64 at all!\n")},
    },
    "wrong-length": lambda app, key: {
        "labels": worker_secret_labels(app, "encryption-key"),
        "data": {KEY_SECRET_KEY: b64(b64(bytes(key)[:16]) + "\n")},
    },
    _VALID: lambda app, key: {
        "labels": worker_secret_labels(app, "encryption-key"),
        "data": key_secret_data(key),
    },
}


@pytest.mark.parametrize("shape", list(_MALFORMED))
def test_conflict_refuses_malformed_secret(
    world: World, worker_logs: pytest.LogCaptureFixture, shape: str
) -> None:
    application_id = world.application()
    private = nacl.public.PrivateKey.generate()
    name = key_secret_name(application_id)
    seeded = world.cluster.seed_secret(name, **_MALFORMED[shape](application_id, private))
    member = world.member(application_id)
    world.assert_declared(member, application_id)

    summary = world.run_pass()

    assert world.cluster.secret(name) == seeded, "a refused Secret is never patched"
    if shape == _VALID:
        assert world.current_recipients(application_id) == [recipient_id_of(bytes(private))]
        assert not logged(worker_logs, "shared_key_secret_invalid")
        return
    assert logged(worker_logs, "shared_key_secret_invalid"), shape
    assert world.current_recipients(application_id) == []
    assert summary.failed >= 1


def test_recipient_response_mismatch_refuses(
    world: World, worker_logs: pytest.LogCaptureFixture
) -> None:
    from curie_worker.shared_reconcile import _validate_registration_response

    application_id = str(uuid.uuid4())
    recipient_id = hashlib.sha256(b"recipient").hexdigest()
    # The API's recorded 201 body (``SharedStore.register_recipient`` pins it).
    recorded = {"application_id": application_id, "recipient_id": recipient_id, "current": True}
    _validate_registration_response(  # liveness: the echo matches
        recorded, application_id=application_id, recipient_id=recipient_id
    )
    for label, body in [
        ("another application", {**recorded, "application_id": str(uuid.uuid4())}),
        ("another recipient", {**recorded, "recipient_id": hashlib.sha256(b"x").hexdigest()}),
        ("no recipient echoed", {"application_id": application_id, "current": True}),
    ]:
        with pytest.raises(Exception) as refused:  # noqa: PT011 - the code is asserted
            _validate_registration_response(
                body, application_id=application_id, recipient_id=recipient_id
            )
        assert getattr(refused.value, "code", None) == "shared_recipient_mismatch", label

    # The readback branch: the Secret changes between create and readback (an
    # external writer), so what the worker reads back is not a key ring.
    real_application = world.application()
    member = world.member(real_application)
    world.assert_declared(member, real_application)

    def corrupt(stored: dict[str, Any]) -> None:
        stored["data"] = {KEY_SECRET_KEY: b64("corrupted\n")}

    world.cluster.after_create = corrupt
    world.run_pass()

    assert logged(worker_logs, "shared_key_secret_invalid")
    assert world.current_recipients(real_application) == []


def test_operator_secrets_untouched(world: World) -> None:
    bundled = world.application()
    managed = world.application(dsn_encoding="managed-raw")
    world.member(bundled)
    world.member(managed)
    operator_names = {
        world.specs[bundled]["grant_store"]["name"],
        world.specs[managed]["database_dsn"]["name"],
    }
    before = {name: world.cluster.secret(name) for name in operator_names}
    world.run_pass()
    world.provision(bundled)
    world.provision_managed(managed)
    world.run_pass()
    world.run_pass()

    for name in operator_names:
        verbs = {verb for verb, kind, seen in world.cluster.calls if seen == name}
        assert verbs == {"metadata"}, (name, verbs)
        assert world.cluster.secret(name) == before[name]


def test_managed_mode_mounts_operator_dsn_only_in_resolver(world: World) -> None:
    application_id = world.application(dsn_encoding="managed-raw")
    world.member(application_id)
    world.run_pass()
    world.provision_managed(application_id)
    world.run_pass()

    operator_dsn = world.specs[application_id]["database_dsn"]
    (deployment,) = world.cluster.sent_named("Deployment", world.deployment_name(application_id))[
        -1:
    ]
    pod = deployment["spec"]["template"]["spec"]
    (volume,) = [volume for volume in pod["volumes"] if volume["name"] == "database-dsn"]
    assert volume["secret"]["secretName"] == operator_dsn["name"]
    mounting = {
        container["name"]
        for container in pod["containers"]
        if any(mount["name"] == "database-dsn" for mount in container["volumeMounts"])
    }
    assert mounting == {"proxy"}
    created = [name for verb, _, name in world.cluster.calls if verb == "create"]
    assert created == [key_secret_name(application_id)], "no DSN Secret in managed mode"
    assert ("metadata", "Secret", operator_dsn["name"]) in world.cluster.calls
    assert_application_objects_equal_render(world, application_id)


def test_status_reports_ready_and_failed(world: World) -> None:
    application_id = world.application()
    world.member(application_id)
    world.converge(application_id)
    name = world.deployment_name(application_id)

    world.cluster.set_status(name, "available")
    world.run_pass()
    database = world.record(application_id)["database"]
    assert (database["status"], database["dsn_revision"]) == ("ready", 1)

    world.cluster.set_status(name, "deadline_exceeded")
    world.run_pass()
    database = world.record(application_id)["database"]
    assert (database["status"], database["status_reason"]) == ("failed", "deployment_unavailable")

    # A rotation the create-once DSN Secret cannot follow.
    world.cluster.set_status(name, "available")
    world.run_pass()
    assert world.record(application_id)["database"]["status"] == "ready"
    dsn_before = world.cluster.secret(dsn_secret_name(application_id))
    rotated = world.store.rotate(application_id, 1)
    assert rotated.status_code == 200, rotated.text
    world.run_pass()
    database = world.record(application_id)["database"]
    assert (database["status"], database["status_reason"]) == ("failed", "dsn_secret_stale")
    assert database["dsn_revision"] == 2
    assert world.cluster.secret(dsn_secret_name(application_id)) == dsn_before
    assert world.last_of("create", "Secret") == []


def test_database_unprovisioned_skips_deployment(world: World) -> None:
    application_id = world.application()
    world.member(application_id)

    world.run_pass()

    application_names = {
        obj["metadata"]["name"] for obj in world.expected_application(application_id)
    }
    assert application_names.isdisjoint(world.last_of("apply"))
    assert world.cluster.application_objects(application_id) == {}
    assert world.record(application_id)["database"] is None
    # Liveness: the key half did run.
    assert world.last_of("create", "Secret") == [key_secret_name(application_id)]


def test_render_refusal_prunes_reach_and_applies_no_dedicated(
    world: World, worker_logs: pytest.LogCaptureFixture
) -> None:
    from plugin_format.connector_render import object_name

    application_id = world.application()
    member = world.member(application_id)
    world.converge(application_id)
    assert world.cluster.reach(member.agent_id)
    dedicated_name = object_name(RELEASE, member.name, DEDICATED)
    assert ("Deployment", dedicated_name) in world.cluster.objects
    world.remove_member(application_id, member.agent_id)
    world.drop("Deployment", dedicated_name)

    summary = world.run_pass()

    assert world.cluster.reach(member.agent_id) == {}
    assert world.last_of("dedicated-apply") == [], "a refused render applies no dedicated object"
    assert world.last_of("dedicated-delete") == []
    assert ("Deployment", dedicated_name) not in world.cluster.objects
    assert summary.failed >= 1
    assert logged(worker_logs, "shared_application_not_member")

    # Liveness: membership back, and the same agent converges both halves.
    world.join(application_id, member.agent_id)
    world.run_pass()
    assert ("Deployment", dedicated_name) in world.cluster.objects
    assert_reach_equal_render(world, application_id, member.agent_id)


def test_disabled_application_applies_nothing_new(
    world: World, worker_logs: pytest.LogCaptureFixture
) -> None:
    application_id = world.application()
    world.member(application_id)
    world.converge(application_id)
    before = copy.deepcopy(world.cluster.application_objects(application_id))
    names = {name for _, name in before}
    world.disable(application_id)

    world.run_pass()

    touched = [(verb, name) for verb, _, name in world.last if name in names and verb != "list"]
    assert touched == [], touched
    assert world.cluster.application_objects(application_id) == before
    assert logged(worker_logs, "shared_application_disabled")


def test_shared_objects_never_owner_labelled(world: World) -> None:
    application_id = world.application()
    member = world.member(application_id)
    world.converge(application_id)

    assert world.cluster.sent
    for obj in world.cluster.sent:
        assert OWNER_LABEL not in (obj["metadata"].get("labels") or {}), obj["metadata"]["name"]
    owned = FakeConnectorClient(world.cluster).list_owned(NAMESPACE, member.name)
    assert owned, "liveness: the dedicated objects are owned"
    assert all(APPLICATION_LABEL not in (obj["metadata"].get("labels") or {}) for obj in owned), [
        obj["metadata"]["name"] for obj in owned
    ]


def test_ensure_application_failure_reaches_the_pass_outcome(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_worker import connector_loop
    from plugin_format.connector_render import object_name

    failing, healthy = world.application(), world.application()
    world.member(failing)
    world.member(healthy)
    dedicated_only = world.deploy(world.agent(), shared={})
    world.converge(failing, healthy)
    dedicated_name = object_name(RELEASE, dedicated_only.name, DEDICATED)
    world.drop("Deployment", dedicated_name)
    world.cluster.fail_apply = lambda obj: (
        obj["kind"] == "Deployment"
        and (obj["metadata"].get("labels") or {}).get(APPLICATION_LABEL) == failing
    )
    outcomes: list[str] = []
    real = connector_loop.record_metric

    def capture(name: str, value: Any = 1, *, attributes: Any = None) -> None:
        if name == "curie.background.loop":
            outcomes.append(str((attributes or {}).get("outcome")))
        real(name, value, attributes=attributes)

    monkeypatch.setattr(connector_loop, "record_metric", capture)

    summary = world.run_pass()

    assert summary.failed >= 1
    assert outcomes == ["failure"]
    # Liveness: the other application and the dedicated agent converged.
    assert world.deployment_name(healthy) in world.last_of("apply", "Deployment")
    assert ("Deployment", dedicated_name) in world.cluster.objects


def test_ready_status_from_recorded_shapes(world: World) -> None:
    application_id = world.application()
    world.member(application_id)
    world.converge(application_id)
    name = world.deployment_name(application_id)
    path = status_path(application_id)
    status_before = world.record(application_id)["database"]["status"]
    assert status_before != "ready"

    world.cluster.set_status(name, "progressing")
    world.run_pass()
    assert world.transport.count("PUT", path) == 0
    assert world.record(application_id)["database"]["status"] == status_before

    world.cluster.set_status(name, "available")
    world.run_pass()
    assert world.transport.count("PUT", path) == 1
    assert world.record(application_id)["database"]["status"] == "ready"

    world.run_pass()  # unchanged: no second report
    assert world.transport.count("PUT", path) == 1

    world.cluster.set_status(name, "deadline_exceeded")
    world.run_pass()
    assert world.transport.count("PUT", path) == 2
    database = world.record(application_id)["database"]
    assert (database["status"], database["status_reason"]) == ("failed", "deployment_unavailable")


def test_two_aliases_of_one_application_apply_one_reach_set(world: World) -> None:
    """Edge case 9: two aliases of one application are one member, so one reach set."""

    application_id = world.application()
    member = world.member(application_id, aliases=(ALIAS, "shared-b"))
    world.converge(application_id)

    world.run_pass()

    expected = {
        obj["metadata"]["name"] for obj in world.expected_reach(application_id, member.agent_id)
    }
    assert set(world.cluster.reach(member.agent_id)) == expected
    applied = [name for name in world.last_of("apply", "NetworkPolicy") if name in expected]
    assert sorted(applied) == sorted(expected), "each reach object applied once per pass"
    assert_reach_equal_render(world, application_id, member.agent_id)
