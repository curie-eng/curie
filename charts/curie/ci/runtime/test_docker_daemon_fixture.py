"""Unit tests for the docker daemon fixture's intake, substrate and cleanup contract."""

from __future__ import annotations

import atexit
import contextlib
import copy
import functools
import importlib.util
import json
import re
import signal
import sqlite3
import subprocess
import sys
import tarfile
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

Json = dict[str, Any]
HERE = Path(__file__).resolve().parent
MODULE_PATH = HERE / "docker-daemon-fixture.py"
REPO = HERE.parents[3]
NONCE = "0a1b2c3d"
NONCE_LABEL = "factory-tools/nonce"
REQUEST_LABEL = "factory-tools/request"
SIGNALS = (signal.SIGINT, signal.SIGTERM)
SYSTEM = "agent-sandbox-system"
CRD = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/"
SANDBOXES = CRD + "sandboxes.agents.x-k8s.io"
CLUSTER_KINDS = {"CustomResourceDefinition", "ClusterRole", "ClusterRoleBinding"}
OBJECT = re.compile(r"(/api/v1|/apis/[^/]+/[^/]+)(?:/namespaces/([^/]+))?/([^/]+)/([^/]+)")
COLLECTION = re.compile(r"(/api/v1|/apis/[^/]+/[^/]+)(?:/namespaces/([^/]+))?/([^/]+)")
CONTEXT = "ctx-under-test"
MUTATING = {"create", "delete", "patch", "apply", "replace", "rollout", "edit", "label", "scale"}


def load() -> ModuleType:
    name = f"docker_daemon_fixture_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(name, MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def path_of(obj: Json) -> str:
    version, kind, meta = obj["apiVersion"], obj["kind"], obj["metadata"]
    prefix = "/api/v1" if version == "v1" else "/apis/" + version
    plural = {"NetworkPolicy": "networkpolicies", "Endpoints": "endpoints"}.get(
        kind, kind.lower() + "s"
    )
    namespace = meta.get("namespace")
    return prefix + (f"/namespaces/{namespace}" if namespace else "") + f"/{plural}/{meta['name']}"


def group(prefix: str) -> str:
    return prefix.split("/")[2] if prefix.startswith("/apis/") else prefix


@functools.cache
def vendored_crds() -> tuple[Json, ...]:
    files = sorted((REPO / "charts/curie/crds").glob("*.yaml"))
    return tuple(
        yaml.load(p.read_text(), Loader=getattr(yaml, "CSafeLoader", yaml.SafeLoader))
        for p in files
    )


CRD_PATHS = {CRD + doc["metadata"]["name"] for doc in vendored_crds()}


class FakeCluster:
    """Stands in for module `run`, interpreting kubectl/docker/git argv.

    Shapes mirror live kubectl output from a single-node k3s cluster on 2026-10-16:
    `get --raw` of a CRD returns metadata.uid/resourceVersion and
    status.conditions[type=Established,status=True], with
    spec.conversion.webhook.clientConfig.service.namespace agent-sandbox-system and a
    caBundle; the controller Deployment has metadata.generation 1,
    status.observedGeneration 1, availableReplicas 1 and label app=agent-sandbox-controller;
    the lease holderIdentity is `<pod-name>_<uuid>` with spec.renewTime;
    `get --raw /api/v1/namespaces/<ns>/pods/<pod>:8081/proxy/readyz` returns `ok`;
    NotFound/AlreadyExists surface in kubectl stderr as `Error from server (NotFound)` /
    `Error from server (AlreadyExists)` with returncode 1.
    """

    def __init__(self) -> None:
        self.objects: dict[str, Json] = {}
        self.calls: list[list[str]] = []
        self.creates: list[str] = []
        self.deletes: list[tuple[str, str, bool]] = []  # path, kind, uid precondition held
        self.mutations: list[tuple[str, str]] = []
        self.race: set[str] = set()
        self.readyz = True
        self.version = 1000
        self.listing = {"container": "", "image": ""}  # answers to nonce-label-filtered listings
        self.unfiltered = {"container": "", "image": ""}  # other sessions' resources
        self.on_readyz: Callable[[], None] | None = None
        self.put({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "kube-system"}})
        self.add_node("node-a")

    def add_node(self, name: str, ready: bool = True) -> None:
        self.put(
            {
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": name},
                "status": {
                    "conditions": [{"type": "Ready", "status": str(ready)}],
                    "nodeInfo": {"architecture": "amd64"},
                },
            }
        )

    def uid(self, path: str) -> str:
        return str(self.objects[path]["metadata"]["uid"])

    def put(self, obj: Json, path: str | None = None) -> Json:
        obj = copy.deepcopy(obj)
        meta = obj.setdefault("metadata", {})
        self.version += 1
        meta.setdefault("uid", str(uuid.uuid4()))
        meta["resourceVersion"] = str(self.version)
        meta.setdefault("generation", 1)
        meta.setdefault("creationTimestamp", "2026-10-16T12:00:00Z")
        if obj["kind"] == "CustomResourceDefinition":
            webhook = obj["spec"].get("conversion", {}).get("webhook", {})
            webhook.get("clientConfig", {}).setdefault("caBundle", "LS0tLS1CRUdJTg==")
            obj["status"] = {"conditions": [{"type": "Established", "status": "True"}]}
        self.objects[path or path_of(obj)] = obj
        return obj

    def start_controller(self, namespace: str, healthy: bool = True) -> None:
        dep = self.objects[
            f"/apis/apps/v1/namespaces/{namespace}/deployments/agent-sandbox-controller"
        ]
        ready = 1 if healthy else 0
        dep["status"] = {
            "observedGeneration": dep["metadata"]["generation"],
            "replicas": 1,
            "readyReplicas": ready,
            "availableReplicas": ready,
            "conditions": [{"type": "Available", "status": str(healthy)}],
        }
        pod = "agent-sandbox-controller-7d9f8b6c4-x2x9q"
        app = {"app": "agent-sandbox-controller"}
        self.put(
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": pod, "namespace": namespace, "labels": app},
                "status": {"phase": "Running", "containerStatuses": [{"ready": healthy}]},
            }
        )
        renew = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        self.put(
            {
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "a3317529.agent-sandbox.x-k8s.io", "namespace": namespace},
                "spec": {"holderIdentity": f"{pod}_{uuid.uuid4()}", "renewTime": renew},
            }
        )
        self.put(
            {
                "apiVersion": "v1",
                "kind": "Endpoints",
                "metadata": {"name": "agent-sandbox-webhook-service", "namespace": namespace},
                "subsets": [{"addresses": [{"ip": "10.42.0.17"}], "ports": [{"port": 9443}]}],
            }
        )
        self.readyz = healthy

    def __call__(self, argv: Any, *, data: str | None = None, check: bool = True, **_: Any) -> Any:
        argv = [str(a) for a in argv]
        self.calls.append(argv)
        if argv[0] == "kubectl":
            code, out, err = self.kubectl(argv, data)
        else:
            code, out, err = 0, self.local(argv), ""
        if check and code:
            raise RuntimeError(f"tool_failed:{argv[0]}:{code}")
        return subprocess.CompletedProcess(argv, code, out, err)

    def local(self, argv: list[str]) -> str:
        if argv[:2] == ["docker", "ps"] or argv[:3] == ["docker", "image", "ls"]:
            kind = "container" if argv[1] == "ps" else "image"
            if f"label={NONCE_LABEL}={NONCE}" in argv:
                return self.listing[kind]
            if any(a.startswith("name=") for a in argv) or "--format" in argv:
                return ""
            return self.unfiltered[kind]
        if argv[:2] == ["git", "archive"]:
            # A candidate snapshot holding only what setup reads: the vendored CRDs.
            with tarfile.open(argv[3], "w") as archive:
                for crd in sorted((REPO / "charts/curie/crds").glob("*.yaml")):
                    archive.add(crd, arcname=str(crd.relative_to(REPO)))
            return ""
        if argv[:2] == ["git", "rev-parse"]:
            return "a" * 40 + "\n"
        raise AssertionError(f"unexpected command: {argv}")

    def kubectl(self, argv: list[str], data: str | None) -> tuple[int, str, str]:
        assert argv[1] == "--context" and argv[2], argv
        rest = [a for a in argv[3:] if not a.startswith("--request-timeout")]
        if rest[0] == "--as":
            rest = rest[2:]
        verb = rest[0]
        if verb == "get" and rest[1] == "--raw":
            return self.get(rest[2])
        if verb == "create" and "-f" in rest and data is not None:
            return self.create(json.loads(data))
        if verb == "delete" and "--raw" in rest:
            return self.delete(rest[rest.index("--raw") + 1], data)
        if verb in MUTATING:
            self.mutations.append((verb, " ".join(rest)))
            return 1, "", f"error: fake refuses {verb}"
        raise AssertionError(f"unexpected kubectl: {argv}")

    def get(self, path: str) -> tuple[int, str, str]:
        if path.endswith("/proxy/readyz"):
            if self.on_readyz is not None:
                hook, self.on_readyz = self.on_readyz, None
                hook()
            return (
                (0, "ok", "") if self.readyz else (1, "", "Error from server (ServiceUnavailable)")
            )
        if path in self.objects:
            return 0, json.dumps(self.objects[path]), ""
        if wanted := COLLECTION.fullmatch(path):
            items = [
                obj
                for key, obj in self.objects.items()
                if (found := OBJECT.fullmatch(key))
                and group(found[1]) == group(wanted[1])
                and found[3] == wanted[3]
                and (wanted[2] is None or found[2] == wanted[2])
            ]
            # Real API (observed on single-node k3s, 2026-10-16): list items omit apiVersion/kind;
            # only the list object carries them.
            stripped = []
            for obj in items:
                item = copy.deepcopy(obj)
                item.pop("apiVersion", None)
                item.pop("kind", None)
                stripped.append(item)
            kind = f"{items[0]['kind']}List" if items else "List"
            list_api_version = items[0].get("apiVersion", "v1") if items else "v1"
            return (
                0,
                json.dumps({"apiVersion": list_api_version, "kind": kind, "items": stripped}),
                "",
            )
        return (
            1,
            "",
            "Error from server (NotFound): the server could not find the requested resource",
        )

    def create(self, obj: Json) -> tuple[int, str, str]:
        path = path_of(obj)
        self.mutations.append(("create", path))
        if path in self.race:
            self.race.discard(path)
            foreign = copy.deepcopy(obj)
            foreign["metadata"]["labels"] = {}
            self.put(foreign, path)
        if path in self.objects:
            name = obj["metadata"]["name"]
            return 1, "", f'Error from server (AlreadyExists): "{name}" already exists'
        obj.pop("status", None)
        for key in ("uid", "resourceVersion", "generation"):
            obj["metadata"].pop(key, None)
        stored = self.put(obj, path)
        self.creates.append(path)
        if obj["kind"] == "Deployment" and obj["metadata"]["name"] == "agent-sandbox-controller":
            self.start_controller(obj["metadata"]["namespace"])
        return 0, json.dumps(stored), ""

    def delete(self, path: str, data: str | None) -> tuple[int, str, str]:
        self.mutations.append(("delete", path))
        current = self.objects.get(path)
        if current is None:
            return 1, "", "Error from server (NotFound): not found"
        wanted = json.loads(data or "{}").get("preconditions", {}).get("uid")
        held = wanted == current["metadata"]["uid"]
        self.deletes.append((path, current["kind"], held))
        if not held:
            return 1, "", "Error from server (Conflict): Precondition failed: UID in precondition"
        del self.objects[path]
        if current["kind"] == "Namespace":
            inside = f"/namespaces/{current['metadata']['name']}/"
            for key in [k for k in self.objects if inside in k]:
                del self.objects[key]
        return 0, "", ""


def seed_crds(fake: FakeCluster, namespace: str = SYSTEM) -> None:
    for doc in vendored_crds():
        doc = copy.deepcopy(doc)
        doc["spec"]["conversion"]["webhook"]["clientConfig"]["service"]["namespace"] = namespace
        fake.put(doc)


def seed_controller(fake: FakeCluster, namespace: str = SYSTEM, healthy: bool = True) -> None:
    fake.put({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace}})
    labels = {"app": "agent-sandbox-controller"}
    fake.put(
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "agent-sandbox-controller",
                "namespace": namespace,
                "labels": labels,
            },
            "spec": {"replicas": 1, "selector": {"matchLabels": labels}},
        }
    )
    fake.start_controller(namespace, healthy)


def foreign_sandbox(fake: FakeCluster) -> str:
    """A Sandbox instance in a namespace no run owns."""
    path = "/apis/agents.x-k8s.io/v1beta1/namespaces/someone-else/sandboxes/theirs"
    meta = {"name": "theirs", "namespace": "someone-else"}
    fake.put({"apiVersion": "agents.x-k8s.io/v1beta1", "kind": "Sandbox", "metadata": meta}, path)
    return path


def ledger(fixture: Any) -> dict[str, tuple[str, str, int]]:
    with contextlib.closing(sqlite3.connect(fixture.work / "evidence.sqlite")) as db:
        rows = db.execute("SELECT path, kind, uid, owned FROM resources").fetchall()
    return {path: (kind, uid, owned) for path, kind, uid, owned in rows}


def argv_for(slice_: str = "fixture", nonce: str = NONCE, **changes: str) -> list[str]:
    values = {
        "context": CONTEXT,
        "candidate": "HEAD",
        "namespace": f"fd-{slice_}-{nonce}",
        "release": f"fd-{slice_}-{nonce}",
        "daemon_namespace": f"fd-{slice_}-d-{nonce}",
        "work_dir": f".projects/daemon-{slice_}-{nonce}",
        **changes,
    }
    return [part for key, value in values.items() for part in ("--" + key.replace("_", "-"), value)]


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        self.now += 1
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += max(seconds, 1)


@pytest.fixture
def mod() -> Iterator[ModuleType]:
    module = load()
    yield module
    sys.modules.pop(module.__name__, None)


@pytest.fixture
def fake(mod: ModuleType, monkeypatch: pytest.MonkeyPatch) -> FakeCluster:
    cluster = FakeCluster()
    monkeypatch.setattr(mod, "run", cluster)
    return cluster


@pytest.fixture
def root(mod: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    resolved = tmp_path.resolve()
    monkeypatch.setattr(mod, "ROOT", resolved)
    monkeypatch.setenv("DOCKER_HOST", "ssh://user@engine.example")
    monkeypatch.delenv("FD_FIXTURE_FAULT", raising=False)
    return resolved


@pytest.fixture
def make(mod: ModuleType, fake: FakeCluster, root: Path) -> Iterator[Callable[..., Any]]:
    saved = {sig: signal.getsignal(sig) for sig in SIGNALS}
    built: list[Any] = []

    def build(prime: bool = True) -> Any:
        fixture = mod.Fixture(mod.arguments(argv=argv_for()))
        built.append(fixture)
        if prime:
            fixture.note("cluster_uid", fake.uid("/api/v1/namespaces/kube-system"))
            for key, value in (("initial_containers", []), ("initial_images", [])):
                fixture.note(key, value)
            fixture.note("initial_imports", {})
            fixture.snapshot = REPO
        return fixture

    yield build
    for fixture in built:
        fixture.closed = True
        atexit.unregister(fixture.cleanup)
    for sig, handler in saved.items():
        signal.signal(sig, handler)


def test_loading_is_side_effect_free_and_exports_api(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    handlers = {sig: signal.getsignal(sig) for sig in SIGNALS}
    exits = atexit._ncallbacks()
    module = load()
    sys.modules.pop(module.__name__, None)
    assert {sig: signal.getsignal(sig) for sig in SIGNALS} == handlers
    assert atexit._ncallbacks() == exits
    assert list(tmp_path.iterdir()) == []
    for name in ("require", "run", "arguments", "check_failure_cleanup", "Fixture"):
        assert callable(getattr(module, name)), name
    methods = "setup images get k create note wait meta fault_point cleanup verify".split()
    for name in methods:
        assert callable(getattr(module.Fixture, name)), name


@pytest.mark.parametrize("slice_", ["fixture", "chart", "worker", "proof"])
def test_every_slice_is_accepted_with_derived_names(
    mod: ModuleType, fake: FakeCluster, root: Path, slice_: str
) -> None:
    args = mod.arguments(argv=argv_for(slice_, "deadbeef"))
    assert (args.slice, args.nonce) == (slice_, "deadbeef")
    assert args.namespace == args.release == f"fd-{slice_}-deadbeef"
    assert args.daemon_namespace == f"fd-{slice_}-d-deadbeef"
    assert args.controller_namespace == f"fd-{slice_}-c-deadbeef"
    assert args.work == root / f".projects/daemon-{slice_}-deadbeef"
    assert fake.calls == []


def test_extra_flags_are_parsed(mod: ModuleType, fake: FakeCluster, root: Path) -> None:
    def extra(parser: Any) -> None:
        parser.add_argument("--action", default="setup")

    args = mod.arguments(extra, argv=[*argv_for(), "--action", "cleanup"])
    assert args.action == "cleanup"


@pytest.mark.parametrize(
    ("changes", "category"),
    [
        pytest.param({"context": ""}, "unsafe_intake", id="empty-context"),
        pytest.param({"candidate": "origin/next"}, "unsafe_intake", id="candidate"),
        pytest.param({"slice_": "ab"}, "unsafe_intake", id="two-letter-slice"),
        pytest.param({"slice_": "abcdefghi"}, "unsafe_intake", id="nine-letter-slice"),
        pytest.param({"slice_": "Fixture"}, "unsafe_intake", id="uppercase-slice"),
        pytest.param({"nonce": "0a1b2c3"}, "unsafe_intake", id="seven-hex-nonce"),
        pytest.param({"nonce": "0A1B2C3D"}, "unsafe_intake", id="uppercase-nonce"),
        pytest.param({"release": "fd-fixture-ffffffff"}, "inconsistent_intake", id="release"),
        pytest.param({"daemon_namespace": f"fd-chart-d-{NONCE}"}, "inconsistent_intake", id="d"),
        pytest.param({"work_dir": f".projects/daemon-chart-{NONCE}"}, "inconsistent_intake"),
        pytest.param({"work_dir": f"../daemon-fixture-{NONCE}"}, "inconsistent_intake"),
    ],
)
def test_intake_refusals(
    mod: ModuleType, fake: FakeCluster, root: Path, changes: dict[str, str], category: str
) -> None:
    with pytest.raises(RuntimeError, match=f"^{category}$"):
        mod.arguments(argv=argv_for(**changes))
    assert fake.calls == []
    assert not (root / ".projects").exists()


def test_symlinked_work_component_is_refused(
    mod: ModuleType, fake: FakeCluster, root: Path
) -> None:
    (root / "elsewhere").mkdir()
    (root / ".projects").symlink_to(root / "elsewhere")
    with pytest.raises(RuntimeError, match="^unsafe_work_directory$"):
        mod.arguments(argv=argv_for())
    assert list((root / "elsewhere").iterdir()) == []
    assert fake.calls == []


@pytest.mark.parametrize(
    ("nodes", "accepted"),
    [
        pytest.param([], False, id="none-ready"),
        pytest.param([("node-b", True)], False, id="two-ready"),
        pytest.param([("node-b", False)], True, id="one-ready-one-not"),
    ],
)
def test_setup_requires_exactly_one_ready_node_of_any_name(
    make: Callable[..., Any], fake: FakeCluster, nodes: list[tuple[str, bool]], accepted: bool
) -> None:
    if not nodes and not accepted:
        fake.objects["/api/v1/nodes/node-a"]["status"]["conditions"][0]["status"] = "False"
    for name, ready in nodes:
        fake.add_node(name, ready)
    fixture = make(prime=False)
    fake.put(
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": fixture.args.namespace}}
    )
    # A pre-existing namespace stops a passing node check at the next refusal, still pre-create.
    category = "namespace_exists" if accepted else "single_ready_node_required"
    with pytest.raises(RuntimeError, match=f"^{category}$"):
        fixture.setup()
    assert fake.mutations == []
    if accepted:
        assert fixture.arch == "amd64"


@pytest.mark.parametrize("context", ["ctx-under-test", "any-other-name"])
def test_caller_context_reaches_every_kubectl_call(
    mod: ModuleType, fake: FakeCluster, root: Path, context: str
) -> None:
    fixture = mod.Fixture(mod.arguments(argv=argv_for(context=context)), teardown=False)
    try:
        assert fixture.meta()["context"] == context
        fixture.k("get", "--raw", "/api/v1/namespaces/kube-system", check=False)
        fixture.namespace(fixture.args.namespace, "restricted")
    finally:
        fixture.db.close()
    kubectl = [c for c in fake.calls if c[0] == "kubectl"]
    assert kubectl and all(c[1:3] == ["--context", context] for c in kubectl)


@pytest.mark.parametrize("defect", ["none", "two-controllers", "incompatible-crd"])
def test_substrate_precheck_refuses_a_bad_existing_substrate_without_mutating(
    make: Callable[..., Any], fake: FakeCluster, defect: str
) -> None:
    seed_crds(fake)
    seed_controller(fake)
    if defect == "two-controllers":
        seed_controller(fake, "second-system")
    elif defect == "incompatible-crd":
        fake.objects[SANDBOXES]["spec"]["scope"] = "Cluster"
    fixture = make()
    if defect == "none":
        fixture.substrate_precheck()  # a healthy shared substrate must pass
    else:
        with pytest.raises(RuntimeError, match="^(ambiguous_substrate|incompatible_crd)$"):
            fixture.substrate_precheck()
    assert fake.mutations == []


@pytest.mark.parametrize(
    ("defect", "category"),
    [("two-controllers", "ambiguous_substrate"), ("incompatible-crd", "incompatible_crd")],
)
def test_setup_refuses_a_bad_existing_substrate_before_any_create(
    make: Callable[..., Any], fake: FakeCluster, defect: str, category: str
) -> None:
    seed_crds(fake)
    seed_controller(fake)
    if defect == "two-controllers":
        seed_controller(fake, "second-system")
    else:
        fake.objects[SANDBOXES]["spec"]["scope"] = "Cluster"
    fixture = make(prime=False)
    with pytest.raises(RuntimeError, match=f"^{category}$"):
        fixture.setup()
    assert fake.mutations == []
    assert not any(f"/namespaces/{fixture.args.namespace}" in path for path in fake.objects)


@pytest.mark.parametrize("which", ["namespace", "daemon_namespace", "controller_namespace"])
def test_setup_refuses_existing_target_namespace(
    make: Callable[..., Any], fake: FakeCluster, which: str
) -> None:
    fixture = make(prime=False)
    name = getattr(fixture.args, which)
    fake.put({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}})
    with pytest.raises(RuntimeError, match="^namespace_exists$"):
        fixture.setup()
    assert fake.mutations == []
    assert not (fixture.work / "snapshot").exists()


def test_healthy_substrate_is_reused_read_only(make: Callable[..., Any], fake: FakeCluster) -> None:
    seed_crds(fake)
    seed_controller(fake)
    fixture = make()
    fixture.substrate()
    assert fake.mutations == []
    assert fixture.meta()["substrate"] == "reused"
    assert fixture.meta()["controller_namespace"] == SYSTEM
    rows = ledger(fixture)
    borrowed = {
        *CRD_PATHS,
        f"/apis/apps/v1/namespaces/{SYSTEM}/deployments/agent-sandbox-controller",
        f"/api/v1/namespaces/{SYSTEM}",
    }
    for path in borrowed:
        assert rows[path][1:] == (fake.uid(path), 0), path
    fixture.verify()
    assert fake.mutations == []


@pytest.mark.parametrize("target", ["crd", "deployment"])
def test_verify_detects_changed_borrowed_substrate(
    make: Callable[..., Any], fake: FakeCluster, target: str
) -> None:
    seed_crds(fake)
    seed_controller(fake)
    fixture = make()
    fixture.substrate()
    fixture.verify()
    if target == "crd":
        fake.objects[SANDBOXES]["metadata"]["resourceVersion"] = "999999"
    else:
        dep = fake.objects[
            f"/apis/apps/v1/namespaces/{SYSTEM}/deployments/agent-sandbox-controller"
        ]
        dep["metadata"]["generation"] = 2
        dep["status"]["observedGeneration"] = 2
    with pytest.raises(RuntimeError, match="^borrowed_substrate_changed$"):
        fixture.verify()


def test_cleanup_of_reused_substrate_ignores_borrowed_crd_instances(
    make: Callable[..., Any], fake: FakeCluster
) -> None:
    seed_crds(fake)
    seed_controller(fake)
    fixture = make()
    fixture.substrate()
    foreign_sandbox(fake)
    fixture.cleanup()
    assert fake.mutations == []
    assert fixture.meta()["cleanup_complete"] is True


def test_missing_crds_are_created_owned_for_existing_controller(
    make: Callable[..., Any], fake: FakeCluster
) -> None:
    seed_controller(fake, "agent-sandbox-alt")
    fixture = make()
    fixture.substrate()
    assert set(fake.creates) == CRD_PATHS
    assert {verb for verb, _ in fake.mutations} == {"create"}
    rows = ledger(fixture)
    for path in CRD_PATHS:
        obj = fake.objects[path]
        labels = obj["metadata"]["labels"]
        assert labels["owner"] == "factory-tools"
        assert labels["factory-tools/nonce"] == NONCE
        service = obj["spec"]["conversion"]["webhook"]["clientConfig"]["service"]
        assert service["namespace"] == "agent-sandbox-alt"
        assert rows[path] == ("CustomResourceDefinition", fake.uid(path), 1)
    assert fixture.meta()["substrate"] == "created"
    assert fixture.meta()["controller_namespace"] == "agent-sandbox-alt"


def test_already_exists_race_is_borrowed_never_owned(
    make: Callable[..., Any], fake: FakeCluster
) -> None:
    seed_controller(fake)
    fake.race.add(SANDBOXES)
    fixture = make()
    fixture.substrate()
    assert SANDBOXES not in fake.creates
    assert ledger(fixture)[SANDBOXES][1:] == (fake.uid(SANDBOXES), 0)
    foreign_sandbox(fake)  # instances of a borrowed CRD never block cleanup
    fixture.cleanup()
    assert SANDBOXES in fake.objects
    assert {path for path, _, _ in fake.deletes} == CRD_PATHS - {SANDBOXES}
    assert fixture.meta()["cleanup_complete"] is True


def test_unhealthy_existing_substrate_hits_deadline_without_creating(
    mod: ModuleType, make: Callable[..., Any], fake: FakeCluster, monkeypatch: pytest.MonkeyPatch
) -> None:
    seed_crds(fake)
    seed_controller(fake, healthy=False)
    fixture = make()
    clock = Clock()
    monkeypatch.setattr(mod.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(mod.time, "sleep", clock.sleep)
    with pytest.raises(RuntimeError, match="^readiness_deadline$"):
        fixture.substrate()
    assert fake.mutations == []
    assert fake.creates == []


def test_run_created_crd_with_foreign_instance_is_preserved(
    make: Callable[..., Any], fake: FakeCluster
) -> None:
    seed_controller(fake)
    fixture = make()
    fixture.substrate()
    instance = foreign_sandbox(fake)
    with pytest.raises(RuntimeError, match="^cleanup_failed_resources_preserved$"):
        fixture.cleanup()
    assert SANDBOXES in fake.objects
    assert SANDBOXES not in {path for path, _, _ in fake.deletes}
    assert instance in fake.objects
    assert fixture.meta()["cleanup_complete"] is False


def test_cleanup_deletes_namespaces_before_cluster_scoped_objects(
    make: Callable[..., Any], fake: FakeCluster
) -> None:
    fixture = make()
    args = fixture.args
    for name in (args.namespace, args.daemon_namespace):
        fixture.create({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}})
    fixture.substrate()
    assert fixture.meta()["substrate"] == "created"
    assert fixture.meta()["controller_namespace"] == args.controller_namespace
    controller_ns = fake.objects[f"/api/v1/namespaces/{args.controller_namespace}"]
    assert controller_ns["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "restricted"
    roles = [p for p in fake.creates if "/clusterroles/" in p or "/clusterrolebindings/" in p]
    assert roles and all(p.endswith("-" + NONCE) for p in roles)
    created = list(fake.creates)
    fixture.cleanup()
    kinds = [kind for _, kind, _ in fake.deletes]
    namespaces = [i for i, kind in enumerate(kinds) if kind == "Namespace"]
    cluster = [i for i, kind in enumerate(kinds) if kind in CLUSTER_KINDS]
    assert namespaces and cluster and max(namespaces) < min(cluster)
    assert {p for p, kind, _ in fake.deletes if kind == "Namespace"} == {
        f"/api/v1/namespaces/{name}"
        for name in (args.namespace, args.daemon_namespace, args.controller_namespace)
    }
    assert all(held for _, _, held in fake.deletes)
    assert [p for p in created if p in fake.objects] == []
    assert fixture.meta()["cleanup_complete"] is True


def test_fault_point_marks_signal_ready(make: Callable[..., Any]) -> None:
    fixture = make(prime=False)
    assert fixture.fault_point("images-imported") is None
    meta = fixture.meta()
    assert meta["phase"] == "signal-ready"
    assert meta["fault_phase"] == "images-imported"


def test_fault_point_error_mode_raises_intentional_failure(
    make: Callable[..., Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FD_FIXTURE_FAULT", "error")
    fixture = make(prime=False)
    with pytest.raises(RuntimeError, match="^intentional_failure$"):
        fixture.fault_point("images-imported")
    assert fixture.meta()["phase"] == "signal-ready"


NS = {"apiVersion": "v1", "kind": "Namespace"}


def docker_calls(fake: FakeCluster) -> list[list[str]]:
    return [call for call in fake.calls if call[0] == "docker"]


def test_cleanup_never_deletes_replacement_at_an_owned_path(
    make: Callable[..., Any], fake: FakeCluster
) -> None:
    fixture = make()
    name = fixture.args.namespace
    path = f"/api/v1/namespaces/{name}"
    labels = fixture.create({**NS, "metadata": {"name": name}})["metadata"]["labels"]
    recorded = fake.uid(path)
    replacement = copy.deepcopy(fake.objects[path])
    del replacement["metadata"]["uid"]
    fake.put(replacement, path)  # same path and request label, new uid
    assert fake.uid(path) != recorded and labels[REQUEST_LABEL]
    with pytest.raises(RuntimeError, match="^cleanup_failed_resources_preserved$"):
        fixture.cleanup()
    assert ("delete", path) not in fake.mutations
    assert path in fake.objects
    assert fixture.meta()["cleanup_complete"] is False


def test_cleanup_records_and_deletes_an_intent_whose_create_was_lost(
    make: Callable[..., Any], fake: FakeCluster
) -> None:
    fixture = make()
    name = fixture.args.namespace
    path = f"/api/v1/namespaces/{name}"
    fixture.asset("kube-intent", path, "req-lost")
    labels = {"owner": "factory-tools", NONCE_LABEL: NONCE, REQUEST_LABEL: "req-lost"}
    fake.put({**NS, "metadata": {"name": name, "labels": labels}})
    assert path not in ledger(fixture)
    fixture.cleanup()
    assert path not in fake.objects
    assert fixture.meta()["cleanup_complete"] is True


def test_kube_only_cleanup_issues_no_docker_command(
    make: Callable[..., Any], fake: FakeCluster, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("DOCKER_HOST", raising=False)
    fixture = make()
    fixture.create({**NS, "metadata": {"name": fixture.args.namespace}})
    fixture.cleanup()
    assert fixture.meta()["cleanup_complete"] is True
    assert all(call[0] == "kubectl" for call in fake.calls)


DIGEST = "sha256:" + "b" * 64
PROBE_POD = "/api/v1/namespaces/{namespace}/pods/fd-probe-daemon"


def prepare_probe(
    fixture: Any, fake: FakeCluster, monkeypatch: pytest.MonkeyPatch, status: Json | None
) -> str:
    """Primes the digests probe reads and, when `status` is given, sets it on the created Pod
    just before each wait; returns the Pod path."""
    fixture.note("daemon_digests", {"ref": "localhost:5000/fd/daemon@" + DIGEST, "digest": DIGEST})
    path = PROBE_POD.format(namespace=fixture.args.daemon_namespace)
    real_wait = fixture.wait

    def wait(predicate: Callable[[], Any], seconds: float = 300) -> Any:
        if status is not None:
            fake.objects[path]["status"] = copy.deepcopy(status)
        return real_wait(predicate, seconds)

    monkeypatch.setattr(fixture, "wait", wait)
    return path


def test_probe_creates_if_not_present_digest_pod_and_accepts_matching_image_id(
    make: Callable[..., Any], fake: FakeCluster, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = make()
    done = {
        "phase": "Succeeded",
        "containerStatuses": [
            {
                "name": "daemon",
                "imageID": "localhost:5000/fd/daemon@" + DIGEST,
                "state": {"terminated": {"exitCode": 0}},
            }
        ],
    }
    path = prepare_probe(fixture, fake, monkeypatch, done)
    fixture.probe("daemon", fixture.args.daemon_namespace, ["dockerd", "--version"], False)
    container = fake.objects[path]["spec"]["containers"][0]
    assert container["imagePullPolicy"] == "IfNotPresent"
    assert container["image"] == "localhost:5000/fd/daemon@" + DIGEST
    assert container["command"] == ["dockerd", "--version"]


@pytest.mark.parametrize("reason", ["ImagePullBackOff", "ErrImagePull"])
def test_probe_fails_fast_when_the_node_cannot_pull_the_image(
    mod: ModuleType,
    make: Callable[..., Any],
    fake: FakeCluster,
    monkeypatch: pytest.MonkeyPatch,
    reason: str,
) -> None:
    fixture = make()
    pending = {
        "phase": "Pending",
        "containerStatuses": [{"name": "daemon", "state": {"waiting": {"reason": reason}}}],
    }
    prepare_probe(fixture, fake, monkeypatch, pending)

    def no_sleep(_: float) -> None:
        raise AssertionError("probe kept waiting after the image pull failed")

    monkeypatch.setattr(mod.time, "sleep", no_sleep)
    with pytest.raises(RuntimeError, match="^image_pull_failed$"):
        fixture.probe("daemon", fixture.args.daemon_namespace, ["dockerd", "--version"], False)


def test_probe_keeps_waiting_while_the_container_is_still_creating(
    make: Callable[..., Any], fake: FakeCluster, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = make()
    creating = {
        "phase": "Pending",
        "containerStatuses": [
            {"name": "daemon", "state": {"waiting": {"reason": "ContainerCreating"}}}
        ],
    }
    path = prepare_probe(fixture, fake, monkeypatch, None)
    seen: list[Any] = []

    def wait(predicate: Callable[[], Any], seconds: float = 300) -> Any:
        fake.objects[path]["status"] = copy.deepcopy(creating)
        seen.append(predicate())
        raise RuntimeError("stop_after_one_poll")

    monkeypatch.setattr(fixture, "wait", wait)
    with pytest.raises(RuntimeError, match="^stop_after_one_poll$"):
        fixture.probe("daemon", fixture.args.daemon_namespace, ["dockerd", "--version"], False)
    assert seen == [{}]


@pytest.mark.parametrize("target", ["crd", "deployment"])
def test_borrowed_baseline_is_recorded_before_the_readiness_wait(
    make: Callable[..., Any], fake: FakeCluster, target: str
) -> None:
    seed_crds(fake)
    seed_controller(fake)
    dep = fake.objects[f"/apis/apps/v1/namespaces/{SYSTEM}/deployments/agent-sandbox-controller"]

    def change() -> None:
        if target == "crd":
            fake.objects[SANDBOXES]["metadata"]["resourceVersion"] = "999999"
        else:
            dep["metadata"]["generation"] = 2
            dep["status"]["observedGeneration"] = 2

    fake.on_readyz = change
    fixture = make()
    fixture.substrate()
    with pytest.raises(RuntimeError, match="^borrowed_substrate_changed$"):
        fixture.verify()


@pytest.mark.parametrize(
    ("kind", "category"),
    [("image", "owned_image_remains"), ("container", "owned_container_remains")],
)
def test_verify_checks_owned_containers_and_images_by_nonce_label(
    make: Callable[..., Any],
    fake: FakeCluster,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    category: str,
) -> None:
    fixture = make()
    fixture.asset("container_intent", f"fd-registry-{NONCE}", "req")
    fixture.verify()
    filt = f"label={NONCE_LABEL}={NONCE}"
    assert any(c[:2] == ["docker", "ps"] and filt in c for c in fake.calls)
    assert any(c[:3] == ["docker", "image", "ls"] and filt in c for c in fake.calls)
    fake.listing[kind] = "sha256:" + "a" * 64 + "\n"
    with pytest.raises(RuntimeError, match=f"^{category}$"):
        fixture.verify()


def test_other_sessions_containers_and_images_never_fail_verify(
    make: Callable[..., Any], fake: FakeCluster
) -> None:
    fake.unfiltered = {"container": "c1\nc2\n", "image": "sha256:other\n"}
    fixture = make()
    fixture.asset("container_intent", f"fd-registry-{NONCE}", "req")
    fixture.note("initial_containers", ["removed-between-setup-and-verify"])
    fixture.note("initial_images", ["sha256:removed"])
    fixture.verify()


def test_recovery_without_teardown_leaves_handlers_and_atexit_alone(
    mod: ModuleType, make: Callable[..., Any]
) -> None:
    fixture = make()
    handlers = {sig: signal.getsignal(sig) for sig in SIGNALS}
    exits = atexit._ncallbacks()
    recovered = mod.Fixture(fixture.args, recover=True, teardown=False)
    try:
        assert {sig: signal.getsignal(sig) for sig in SIGNALS} == handlers
        assert atexit._ncallbacks() == exits
    finally:
        recovered.db.close()


def test_tool_failure_category_names_the_subcommand(
    mod: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    def failing(argv: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(argv, 1, "", "boom")

    monkeypatch.setattr(mod.subprocess, "run", failing)
    argv = ["docker", "buildx", "build", "--platform", "linux/amd64", "."]
    with pytest.raises(RuntimeError, match="^tool_failed:docker:buildx build:1$"):
        mod.run(argv)
