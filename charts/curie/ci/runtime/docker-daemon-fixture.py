#!/usr/bin/env python3
"""Disposable scratch substrate and image ownership. Import Fixture for later slices."""

from __future__ import annotations

import argparse
import atexit
import contextlib
import copy
import hashlib
import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import tarfile
import time
import urllib.request
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[4]
type Json = dict[str, Any]
OWNER, NONCE, REQUEST = "owner", "factory-tools/nonce", "factory-tools/request"
SIGNALS = (signal.SIGINT, signal.SIGTERM)
SHA256 = r"sha256:[0-9a-f]{64}"
TAGS = ["docker", "image", "ls", "--format", "{{.Repository}}:{{.Tag}}"]
PLURALS = {
    "NetworkPolicy": "networkpolicies",
    "ValidatingAdmissionPolicy": "validatingadmissionpolicies",
}
CATALOG_EXAMPLE = (
    "fn main() {\n"
    "    let s = curie::repo_runner::RepoRunnerSpec {\n"
    '        tools: vec!["docker-cli".into()],\n'
    "        ..Default::default()\n"
    "    };\n"
    '    println!("{}", serde_json::json!([\n'
    "        curie::repo_runner::catalog_dockerfile(&s),\n"
    "        curie::repo_runner::finalize_dockerfile(&s),\n"
    "    ]));\n"
    "}\n"
)
MANIFEST_TYPES = (
    "application/vnd.oci.image.index.v1+json,"
    "application/vnd.oci.image.manifest.v1+json,"
    "application/vnd.docker.distribution.manifest.list.v2+json,"
    "application/vnd.docker.distribution.manifest.v2+json"
)


def require(condition: object, category: str) -> None:
    if not condition:
        raise RuntimeError(category)


def run(
    argv: Sequence[str | Path],
    *,
    data: str | None = None,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    check: bool = True,
    timeout: float = 600,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        list(map(str, argv)),
        input=data,
        text=True,
        capture_output=True,
        cwd=cwd,
        env=env,
        timeout=timeout,
        check=False,
    )
    if check and result.returncode:
        # Subcommand words only: no flag values, paths, names or stderr reach the category.
        words, previous = [], "-"
        for token in map(str, argv[1:]):
            flag_value = previous.startswith("-") and "=" not in previous and previous != "-"
            if re.fullmatch(r"[a-z]+(-[a-z]+)*", token) and not flag_value:
                words.append(token)
            previous = token
            if len(words) == 2:
                break
        tool = Path(str(argv[0])).name
        raise RuntimeError(f"tool_failed:{tool}:{' '.join(words)}:{result.returncode}")
    return result


def loads(text: str | bytes) -> Json:
    value = json.loads(text)
    require(isinstance(value, dict), "unexpected_json_shape")
    return dict(value)


def arguments(
    extra: Callable[[argparse.ArgumentParser], object] | None = None,
    argv: Sequence[str] | None = None,
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("context", "namespace", "daemon-namespace", "release", "candidate", "work-dir"):
        parser.add_argument("--" + name, required=True)
    if extra:
        extra(parser)
    args = parser.parse_args(argv)
    match = re.fullmatch(r"fd-([a-z]{3,8})-([0-9a-f]{8})", args.namespace)
    # Where to run is the caller's choice: the context is passed to every kubectl call as is.
    require(args.context.strip() and args.candidate == "HEAD" and match, "unsafe_intake")
    assert match is not None
    args.slice, args.nonce = match[1], match[2]
    require(
        args.release == args.namespace
        and args.daemon_namespace == f"fd-{args.slice}-d-{args.nonce}"
        and args.work_dir == f".projects/daemon-{args.slice}-{args.nonce}",
        "inconsistent_intake",
    )
    args.controller_namespace = f"fd-{args.slice}-c-{args.nonce}"
    args.work = ROOT / args.work_dir
    require(
        all(not part.is_symlink() for part in (args.work, *args.work.parents))
        and args.work.resolve().is_relative_to(ROOT),
        "unsafe_work_directory",
    )
    return args


def object_path(obj: Json) -> str:
    version: str = obj["apiVersion"]
    kind: str = obj["kind"]
    meta = obj["metadata"]
    prefix = "/api/v1" if version == "v1" else "/apis/" + version
    namespace = meta.get("namespace")
    plural = PLURALS.get(kind, kind.lower() + "s")
    return prefix + (f"/namespaces/{namespace}" if namespace else "") + f"/{plural}/{meta['name']}"


def version_of(obj: Json) -> str:
    """Borrowed-object change witness: CRD resourceVersion, Deployment generation."""
    meta = obj["metadata"]
    if obj["kind"] == "CustomResourceDefinition":
        return str(meta.get("resourceVersion", ""))
    return str(meta.get("generation", "")) if obj["kind"] == "Deployment" else ""


class Fixture:
    def __init__(
        self, args: argparse.Namespace, recover: bool = False, teardown: bool = True
    ) -> None:
        self.args, self.work, self.closed = args, Path(args.work), False
        self.snapshot = self.work / "snapshot"
        self.registry = self.port = self.node = self.arch = ""
        if recover:
            require((self.work / "evidence.sqlite").is_file(), "ledger_missing")
        else:
            self.work.mkdir(mode=0o700, parents=True, exist_ok=False)
        self.db = sqlite3.connect(self.work / "evidence.sqlite")
        self.db.executescript(
            "CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT);"
            "CREATE TABLE IF NOT EXISTS resources(path TEXT PRIMARY KEY,kind TEXT,"
            "namespace TEXT,uid TEXT,owned INTEGER,version TEXT,request TEXT);"
            "CREATE TABLE IF NOT EXISTS assets(kind TEXT,name TEXT,identity TEXT,"
            "PRIMARY KEY(kind,name));"
        )
        if teardown:  # a verify-only Fixture must leave no handler pointing at it
            atexit.register(self.cleanup)
            for sig in SIGNALS:
                signal.signal(sig, self.interrupted)
        if not recover:
            self.note("nonce", args.nonce)
            self.note("context", args.context)
            self.note("cleanup_complete", False)
        self.candidate: str = self.meta().get("candidate", "")

    def interrupted(self, signum: int, _frame: object) -> None:
        self.note("signal", signum)
        raise SystemExit(128 + signum)

    def note(self, key: str, value: object) -> None:
        with self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO metadata VALUES (?,?)", (key, json.dumps(value))
            )

    def meta(self) -> Json:
        return {key: json.loads(value) for key, value in self.db.execute("SELECT * FROM metadata")}

    def asset(self, kind: str, name: str, identity: str) -> None:
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO assets VALUES (?,?,?)", (kind, name, identity))

    def k(self, *argv: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        context = self.args.context
        return run(["kubectl", "--context", context, "--request-timeout=15s", *argv], **kwargs)

    def get(self, path: str) -> Json:
        result = self.k("get", "--raw", path, check=False, timeout=25)
        if result.returncode and "(NotFound)" in result.stderr:
            return {}
        require(result.returncode == 0, "inventory_unavailable")
        return loads(result.stdout)

    def owned(self, path: str) -> bool | None:
        row = self.db.execute("SELECT owned FROM resources WHERE path=?", (path,)).fetchone()
        return None if row is None else bool(row[0])

    def record(self, obj: Json, owned: bool) -> Json:
        meta = obj["metadata"]
        with self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO resources VALUES (?,?,?,?,?,?,?)",
                (
                    object_path(obj),
                    obj["kind"],
                    meta.get("namespace", ""),
                    meta["uid"],
                    int(owned),
                    version_of(obj),
                    meta.get("labels", {}).get(REQUEST, ""),
                ),
            )
        return obj

    def borrow(self, obj: Json) -> None:
        """Borrowed rows are written at first observation and never overwritten."""
        if self.owned(object_path(obj)) is None:
            self.record(obj, False)

    def create(self, obj: Json, *, identity: str | None = None, race: bool = False) -> Json:
        obj = copy.deepcopy(obj)
        labels = obj["metadata"].setdefault("labels", {})
        labels.update({OWNER: "factory-tools", NONCE: self.args.nonce, REQUEST: uuid.uuid4().hex})
        for key in ("template", "podTemplate"):
            if key in obj.get("spec", {}):
                obj["spec"][key].setdefault("metadata", {}).setdefault("labels", {}).update(
                    {name: labels[name] for name in (OWNER, NONCE, REQUEST)}
                )
        path = object_path(obj)
        self.asset("kube-intent", path, labels[REQUEST])

        def is_ours(found: Json) -> bool:
            return (
                bool(found) and found["metadata"].get("labels", {}).get(REQUEST) == labels[REQUEST]
            )

        try:
            response = self.k(
                *(["--as", identity] if identity else []),
                "create",
                "-f",
                "-",
                "-o",
                "json",
                data=json.dumps(obj),
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired, SystemExit):
            found = self.get(path)
            if is_ours(found):
                self.record(found, True)
            raise
        if response.returncode == 0:
            return self.record(loads(response.stdout), True)
        found = self.get(path)
        if is_ours(found):
            return self.record(found, True)
        if race and found and "(AlreadyExists)" in response.stderr:
            return self.record(found, False)
        raise RuntimeError("atomic_create_refused")

    def namespace(self, name: str, level: str) -> Json:
        return self.create(
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": name,
                    "labels": {
                        "pod-security.kubernetes.io/enforce": level,
                        "pod-security.kubernetes.io/enforce-version": "latest",
                    },
                },
            }
        )

    def wait[T](self, predicate: Callable[[], T], seconds: float = 300) -> T:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(1)
        raise RuntimeError("readiness_deadline")

    def controllers(self) -> list[Json]:
        # Collection items carry no apiVersion or kind; only the list itself does.
        return [
            {"apiVersion": "apps/v1", "kind": "Deployment", **item}
            for item in self.get("/apis/apps/v1/deployments")["items"]
            if item["metadata"]["name"] == "agent-sandbox-controller"
        ]

    def vendored_crds(self) -> list[Json]:
        crds: list[Json] = list(
            yaml.safe_load_all(
                "\n---\n".join(
                    p.read_text()
                    for p in sorted((self.snapshot / "charts/curie/crds").glob("*.yaml"))
                )
            )
        )
        require(len(crds) == 4, "vendored_crd_inventory")
        return crds

    @staticmethod
    def require_compatible(current: Json, doc: Json) -> None:
        require(
            current["spec"]["group"] == doc["spec"]["group"]
            and current["spec"]["names"] == doc["spec"]["names"]
            and current["spec"]["scope"] == "Namespaced"
            and {v["name"] for v in doc["spec"]["versions"] if v.get("served")}.issubset(
                {v["name"] for v in current["spec"]["versions"] if v.get("served")}
            ),
            "incompatible_crd",
        )

    def substrate_precheck(self) -> None:
        """Refuse an ambiguous or incompatible existing substrate before the first create."""
        require(len(self.controllers()) <= 1, "ambiguous_substrate")
        for doc in self.vendored_crds():
            current = self.get(object_path(doc))
            if current:
                self.require_compatible(current, doc)

    def substrate(self) -> None:
        deadline = time.monotonic() + 300
        crds = self.vendored_crds()
        existing = [self.get(object_path(doc)) for doc in crds]
        controllers = self.controllers()
        require(len(controllers) <= 1, "ambiguous_substrate")
        for found in [*filter(None, existing), *controllers]:
            self.borrow(found)
        if controllers:
            self.borrow(self.get(f"/api/v1/namespaces/{controllers[0]['metadata']['namespace']}"))
        bootstrap = not controllers and not any(existing)
        controller_ns: str = (
            controllers[0]["metadata"]["namespace"]
            if controllers
            else self.args.controller_namespace
        )
        if bootstrap:
            self.namespace(controller_ns, "restricted")
        for doc, current in zip(crds, existing, strict=True):
            if not current and (controllers or bootstrap):
                doc["spec"]["conversion"]["webhook"]["clientConfig"]["service"]["namespace"] = (
                    controller_ns
                )
                current = self.create(doc, race=True)
            if current:
                self.require_compatible(current, doc)
                self.borrow(current)
        # A raced CRD means another installer is active: never start a second controller.
        bootstrap = bootstrap and all(self.owned(object_path(d)) for d in crds)
        if bootstrap:
            self.controller_bundle(controller_ns)

        def healthy() -> Json:
            candidates = self.controllers()
            if len(candidates) != 1:
                return {}
            dep = candidates[0]
            ns = dep["metadata"]["namespace"]
            status = dep.get("status", {})
            if (
                status.get("observedGeneration", 0) < dep["metadata"]["generation"]
                or status.get("availableReplicas", 0) < 1
            ):
                return {}
            for doc in crds:
                crd = self.get(object_path(doc))
                if not crd or not any(
                    c["type"] == "Established" and c["status"] == "True"
                    for c in crd.get("status", {}).get("conditions", [])
                ):
                    return {}
                config = crd["spec"]["conversion"]["webhook"]["clientConfig"]
                if config["service"]["namespace"] != ns or not config.get("caBundle"):
                    return {}
                endpoint = self.get(
                    f"/api/v1/namespaces/{ns}/endpoints/{config['service']['name']}"
                )
                if not endpoint or not any(s.get("addresses") for s in endpoint.get("subsets", [])):
                    return {}
                self.get(self.instances_path(crd))
            pods = [
                p
                for p in self.get(f"/api/v1/namespaces/{ns}/pods")["items"]
                if p["metadata"].get("labels", {}).get("app") == "agent-sandbox-controller"
            ]
            leases = self.get(f"/apis/coordination.k8s.io/v1/namespaces/{ns}/leases")["items"]
            now = datetime.now(UTC)
            if not any(
                lease["spec"].get("holderIdentity", "").startswith(p["metadata"]["name"])
                and (
                    now - datetime.fromisoformat(lease["spec"]["renewTime"].replace("Z", "+00:00"))
                ).total_seconds()
                < 60
                for lease in leases
                for p in pods
                if lease.get("spec", {}).get("renewTime")
            ):
                return {}
            for pod in pods:
                readyz = f"/api/v1/namespaces/{ns}/pods/{pod['metadata']['name']}:8081/proxy/readyz"
                if self.k("get", "--raw", readyz, check=False).returncode == 0:
                    return dep
            return {}

        dep = self.wait(healthy, max(0, deadline - time.monotonic()))
        ns = dep["metadata"]["namespace"]
        for doc in [
            dep,
            self.get(f"/api/v1/namespaces/{ns}"),
            *map(self.get, map(object_path, crds)),
        ]:
            self.borrow(doc)  # only objects first seen now, e.g. a concurrent bootstrap
        created = any(self.owned(object_path(d)) for d in crds)
        self.note("controller_namespace", ns)
        self.note("substrate", "created" if created else "reused")
        self.controller = dep

    def controller_bundle(self, controller_ns: str) -> None:
        nonce = self.args.nonce
        bundle = list(
            yaml.safe_load_all(
                (self.snapshot / "charts/curie/files/agent-sandbox/controller.yaml").read_text()
            )
        )
        renames = {
            d["metadata"]["name"]: d["metadata"]["name"] + "-" + nonce
            for d in bundle
            if d["kind"] == "ClusterRole"
        }
        rendered = self.work / "controller.rendered.yaml"
        docs = []
        for doc in bundle:
            if doc["kind"] == "Namespace":
                continue
            meta = doc["metadata"]
            if doc["kind"] in {"ClusterRole", "ClusterRoleBinding"}:
                meta["name"] += "-" + nonce
            else:
                meta["namespace"] = controller_ns
            if "roleRef" in doc and doc["roleRef"]["kind"] == "ClusterRole":
                doc["roleRef"]["name"] = renames[doc["roleRef"]["name"]]
            for subject in doc.get("subjects", []):
                subject["namespace"] = controller_ns
            docs.append(doc)
        rendered.write_text(yaml.safe_dump_all(docs))
        for doc in yaml.safe_load_all(rendered.read_text()):
            self.create(doc)
        rbac = "rbac.authorization.k8s.io"
        subjects = [
            {
                "kind": "ServiceAccount",
                "name": "agent-sandbox-controller",
                "namespace": controller_ns,
            }
        ]
        name, read = "fd-np-" + nonce, ["get", "list", "watch"]
        # Cluster-wide read for the informer, mutation confined to the app namespace.
        for kind, meta, verbs in (
            ("Cluster", {"name": name}, read),
            (
                "",
                {"name": name, "namespace": self.args.namespace},
                read + ["create", "delete", "patch", "update"],
            ),
        ):
            rule = {
                "apiGroups": ["networking.k8s.io"],
                "resources": ["networkpolicies"],
                "verbs": verbs,
            }
            self.create(
                {
                    "apiVersion": rbac + "/v1",
                    "kind": kind + "Role",
                    "metadata": meta,
                    "rules": [rule],
                }
            )
            self.create(
                {
                    "apiVersion": rbac + "/v1",
                    "kind": kind + "RoleBinding",
                    "metadata": meta,
                    "roleRef": {"apiGroup": rbac, "kind": kind + "Role", "name": name},
                    "subjects": subjects,
                }
            )

    @staticmethod
    def instances_path(crd: Json) -> str:
        spec = crd["spec"]
        version = next(v["name"] for v in spec["versions"] if v["storage"])
        return f"/apis/{spec['group']}/{version}/{spec['names']['plural']}"

    def docker_assets(self) -> bool:
        """Only a ledger holding a non-kube asset needs Docker to clean or verify."""
        kinds = self.db.execute("SELECT kind FROM assets WHERE kind!='kube-intent' LIMIT 1")
        return kinds.fetchone() is not None

    def labelled(self, *argv: str) -> list[str]:
        """IDs carrying this execution's nonce label, which no other session can hold."""
        nonce = f"label={NONCE}={self.args.nonce}"
        found = run(["docker", *argv, "--no-trunc", "--filter", nonce]).stdout.split()
        return list(dict.fromkeys(found))

    def setup(self) -> Fixture:
        # Every refusal precedes the first create.
        ready = [
            n
            for n in self.get("/api/v1/nodes")["items"]
            if any(
                c["type"] == "Ready" and c["status"] == "True"
                for c in n.get("status", {}).get("conditions", [])
            )
        ]
        require(len(ready) == 1, "single_ready_node_required")
        self.node = ready[0]["metadata"]["name"]
        self.arch = ready[0]["status"]["nodeInfo"]["architecture"]
        self.note("node_arch", self.arch)
        for name in (
            self.args.namespace,
            self.args.daemon_namespace,
            self.args.controller_namespace,
        ):
            require(not self.get(f"/api/v1/namespaces/{name}"), "namespace_exists")
        self.note("cluster_uid", self.get("/api/v1/namespaces/kube-system")["metadata"]["uid"])
        self.candidate = run(["git", "rev-parse", "HEAD^{commit}"], cwd=ROOT).stdout.strip()
        require(re.fullmatch("[0-9a-f]{40,64}", self.candidate), "candidate_identity")
        self.note("candidate", self.candidate)
        self.note("phase", "setup")
        self.snapshot.mkdir()
        archive = self.work / "candidate.tar"
        run(["git", "archive", "--output", archive, self.candidate], cwd=ROOT)
        with tarfile.open(archive) as source:
            source.extractall(self.snapshot, filter="data")
        archive.unlink()
        self.substrate_precheck()
        self.namespace(self.args.namespace, "restricted")
        self.namespace(self.args.daemon_namespace, "privileged")
        self.substrate()
        return self

    def images(self) -> dict[str, Json]:
        """Build into a registry this run owns on the Docker engine; the node pulls from it.

        The registry publishes on the engine host's loopback, so the single ready node
        reaches it as localhost:<port> only when that node is the engine host. Nothing
        is imported into the node's containerd: no host privilege is needed or used.
        """
        require(self.arch, "setup_required")
        platform = "linux/" + self.arch
        registry = "fd-registry-" + self.args.nonce
        request = uuid.uuid4().hex
        self.asset("container_intent", registry, request)
        container = run(
            ["docker", "create", "--name", registry, "--label", "owner=factory-tools"]
            + ["--label", f"{NONCE}={self.args.nonce}", "--label", f"{REQUEST}={request}"]
            + ["-p", "127.0.0.1::5000", "--tmpfs", "/var/lib/registry"]
            + ["registry:2"]
        ).stdout.strip()
        self.asset("container", container, self.args.nonce)
        run(["docker", "start", container])
        self.port = run(["docker", "port", container, "5000/tcp"]).stdout.strip().rsplit(":", 1)[1]
        self.registry = "localhost:" + self.port
        url = f"http://127.0.0.1:{self.port}/v2/"

        def registry_ready() -> bool:
            try:
                with urllib.request.urlopen(url, timeout=2) as response:
                    return bool(response.status == 200)
            except OSError:
                return False

        self.wait(registry_ready, 60)
        builder = run(["docker", "buildx", "inspect", "default"]).stdout
        require(platform in builder, "default_builder_platform_unavailable")
        empty = self.work / "empty"
        empty.mkdir()

        def build(name: str, dockerfile: Path, context: Path, extra: Sequence[str] = ()) -> Json:
            tag = f"{self.registry}/fd-{self.args.nonce}/{name}:candidate"
            require(tag not in run(TAGS).stdout.split(), "build_tag_occupied")
            request = uuid.uuid4().hex
            self.asset("tag_intent", tag, request)
            metadata = self.work / (name + "-build.json")
            run(
                ["docker", "buildx", "build", "--builder", "default", "--platform", platform]
                + ["--label", f"{REQUEST}={request}", "--label", f"{NONCE}={self.args.nonce}"]
                + ["--provenance=false", "--push", "--metadata-file", str(metadata), "-t", tag]
                + ["-f", str(dockerfile), *extra, str(context)],
                timeout=7200,
            )
            top = str(loads(metadata.read_text())["containerimage.digest"])
            require(re.fullmatch(SHA256, top), "invalid_image_digest")
            ref = tag.rsplit(":", 1)[0] + "@" + top
            self.note(name + "_image", ref)
            return self.resolve(name, ref)

        result = {
            "daemon": build(
                "daemon",
                self.snapshot / "apps/docker-daemon/Dockerfile",
                self.snapshot / "apps/docker-daemon",
            ),
            "platform": build("platform", self.snapshot / "runner/Dockerfile", self.snapshot),
        }
        examples = self.snapshot / "cli/examples"
        examples.mkdir(exist_ok=True)
        (examples / "daemon_catalog.rs").write_text(CATALOG_EXAMPLE)
        cargo = self.work / "cargo"
        env = dict(os.environ, TMPDIR=str(self.work), CARGO_TARGET_DIR=str(cargo))
        files = json.loads(
            run(
                ["cargo", "run", "--locked", "--quiet", "--manifest-path"]
                + [str(self.snapshot / "cli/Cargo.toml"), "--example", "daemon_catalog"],
                env=env,
                timeout=7200,
            ).stdout
        )
        shutil.rmtree(cargo, ignore_errors=True)
        catalog, final = self.work / "Catalog.Dockerfile", self.work / "Final.Dockerfile"
        catalog.write_text(files[0])
        final.write_text(files[1])
        platform_ref = result["platform"]["ref"]
        result["catalog"] = build(
            "catalog", catalog, empty, ["--build-arg", "CURIE_PLATFORM_RUNNER=" + platform_ref]
        )
        result["runner"] = build(
            "runner",
            final,
            empty,
            ["--build-context", "curie-composed=docker-image://" + result["catalog"]["ref"]],
        )
        return result

    def resolve(self, name: str, ref: str) -> Json:
        """Record the manifest and config digests the registry serves for ref."""
        repository, top = ref.split("@")
        base = f"http://127.0.0.1:{self.port}/v2/" + repository.split("/", 1)[1]
        blob_dir = self.work / (name + "-blobs")
        blob_dir.mkdir()

        def fetch(expected: str) -> Json:
            require(re.fullmatch(SHA256, expected), "invalid_blob_digest")
            path = blob_dir / expected.split(":")[1]
            if not path.exists():
                request = urllib.request.Request(
                    base + "/manifests/" + expected,
                    headers={"Accept": MANIFEST_TYPES},
                )
                with (
                    urllib.request.urlopen(request, timeout=120) as response,
                    path.open("wb") as output,
                ):
                    shutil.copyfileobj(response, output)
                with path.open("rb") as content:
                    require(
                        "sha256:" + hashlib.file_digest(content, "sha256").hexdigest() == expected,
                        "registry_digest_mismatch",
                    )
            return loads(path.read_bytes())

        head = fetch(top)
        manifest = top
        if "manifests" in head:
            # An index is accepted, but the node platform must be exactly one of its entries.
            matches = [
                m["digest"]
                for m in head["manifests"]
                if m.get("platform", {}).get("os") == "linux"
                and m["platform"].get("architecture") == self.arch
            ]
            require(len(matches) == 1, "image_platform_missing")
            manifest = matches[0]
        doc = fetch(manifest)
        config = doc["config"]["digest"]
        result = {"ref": ref, "digest": top, "manifest": manifest, "config": config}
        shutil.rmtree(blob_dir)
        self.note(name + "_digests", result)
        return result

    def probe(self, name: str, namespace: str, command: list[str], restricted: bool) -> None:
        digests = self.meta()[name + "_digests"]
        container: Json = {
            "name": name,
            "image": digests["ref"],
            "imagePullPolicy": "IfNotPresent",
            "command": command,
        }
        if restricted:
            container["securityContext"] = {
                "runAsNonRoot": True,
                "runAsUser": 1000,
                "runAsGroup": 1000,
                "allowPrivilegeEscalation": False,
                "capabilities": {"drop": ["ALL"]},
                "seccompProfile": {"type": "RuntimeDefault"},
            }
        pod = self.create(
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "fd-probe-" + name, "namespace": namespace},
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "containers": [container],
                },
            }
        )
        path = object_path(pod)

        def finished() -> Json:
            current = self.get(path)
            phase = current.get("status", {}).get("phase")
            require(phase != "Failed", "probe_failed:" + name)
            waiting = [
                s.get("state", {}).get("waiting", {}).get("reason")
                for s in current.get("status", {}).get("containerStatuses", [])
            ]
            # A node that cannot reach the run's registry fails now, not at the wait limit.
            require(not {"ErrImagePull", "ImagePullBackOff"} & set(waiting), "image_pull_failed")
            return current if phase == "Succeeded" else {}

        status = self.wait(finished, 300)["status"]["containerStatuses"][0]
        terminated = status.get("state", {}).get("terminated", {})
        require(terminated.get("exitCode") == 0, "probe_failed:" + name)
        found = re.search(rf"@({SHA256})$", status.get("imageID", ""))
        require(found and found[1] == digests["digest"], "probe_digest_mismatch")

    def self_test(self) -> Json:
        self.setup()
        images = self.images()
        self.probe("daemon", self.args.daemon_namespace, ["dockerd", "--version"], False)
        self.probe("runner", self.args.namespace, ["docker", "--version"], True)
        self.fault_point("images-imported")
        self.cleanup()
        self.verify()
        result = {
            "candidate": self.candidate,
            "substrate": self.meta()["substrate"],
            "images": {name: image["digest"] for name, image in images.items()},
            "cleanup": True,
        }
        print(json.dumps(result, sort_keys=True))
        return result

    def fault_point(self, phase: str) -> None:
        self.note("fault_phase", phase)
        self.note("phase", "signal-ready")
        mode = os.environ.get("FD_FIXTURE_FAULT", "")
        if mode == "error":
            raise RuntimeError("intentional_failure")
        if mode == "signal":
            deadline = time.monotonic() + 600
            while time.monotonic() < deadline:
                time.sleep(1)
            raise RuntimeError("signal_not_received")

    def delete(self, path: str, uid: str, request: str) -> None:
        obj = self.get(path)
        if not obj:
            return
        labels = obj["metadata"].get("labels", {})
        require(
            obj["metadata"]["uid"] == uid
            and labels.get(NONCE) == self.args.nonce
            and labels.get(OWNER) == "factory-tools"
            and labels.get(REQUEST) == request,
            "cleanup_identity_changed",
        )
        options = {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "preconditions": {"uid": uid},
            "propagationPolicy": "Foreground",
        }
        self.k("delete", "--raw", path, "-f", "-", data=json.dumps(options))
        self.wait(lambda: not self.get(path), 180)

    def crd_blocked(self, path: str, uid: str, owned_namespaces: set[str]) -> bool:
        """A run-created CRD is kept if it changed identity or holds an instance we do not own."""
        crd = self.get(path)
        if not crd:
            return False
        if crd["metadata"]["uid"] != uid:
            return True
        items = self.get(self.instances_path(crd))["items"]
        return any(i["metadata"].get("namespace", "") not in owned_namespaces for i in items)

    def check_cluster(self) -> None:
        meta = self.meta()
        require(
            "cluster_uid" not in meta
            or self.get("/api/v1/namespaces/kube-system")["metadata"]["uid"] == meta["cluster_uid"],
            "cluster_identity_changed",
        )

    def verify(self) -> None:
        self.check_cluster()
        for path, uid, owned, version in self.db.execute(
            "SELECT path,uid,owned,version FROM resources"
        ).fetchall():
            current = self.get(path)
            if owned:
                require(not current, "owned_resource_remains")
            else:
                require(
                    current
                    and current["metadata"]["uid"] == uid
                    and version_of(current) == version,
                    "borrowed_substrate_changed",
                )
        if not self.docker_assets():
            return
        assets = self.db.execute("SELECT kind,name FROM assets").fetchall()
        require(not self.labelled("ps", "-aq"), "owned_container_remains")
        tags = run(TAGS).stdout.split() if any(k == "tag_intent" for k, _ in assets) else []
        for kind, name in assets:
            require(kind != "tag_intent" or name not in tags, "owned_tag_remains")
        require(not self.labelled("image", "ls", "-q"), "owned_image_remains")

    def cleanup(self) -> None:
        if self.closed:
            return
        self.closed = True
        for sig in SIGNALS:
            signal.signal(sig, signal.SIG_IGN)
        try:
            self.check_cluster()
            for path, request in self.db.execute(
                "SELECT name,identity FROM assets WHERE kind='kube-intent'"
            ).fetchall():
                # Re-record only a crash between create and record; a recorded uid stands.
                if self.owned(path) is not None:
                    continue
                current = self.get(path)
                labels = current.get("metadata", {}).get("labels", {})
                if labels.get(REQUEST) == request:
                    self.record(current, True)
            rows: list[tuple[str, str, str, str, str]] = self.db.execute(
                "SELECT path,kind,namespace,uid,request FROM resources WHERE owned=1"
                " ORDER BY rowid DESC"
            ).fetchall()
            namespaces = {
                p.rsplit("/", 1)[1]: (p, u, r) for p, k, _, u, r in rows if k == "Namespace"
            }
            for name in (self.args.namespace, self.args.daemon_namespace):
                if name in namespaces:
                    self.delete(*namespaces[name])
            # Only CRDs this run created are checked; borrowed CRDs are never ours to judge.
            preserved = any(
                self.crd_blocked(p, u, set(namespaces))
                for p, k, _, u, _ in rows
                if k == "CustomResourceDefinition"
            )
            if not preserved:
                if self.args.controller_namespace in namespaces:
                    self.delete(*namespaces[self.args.controller_namespace])
                cluster = [r for r in rows if not r[2] and r[1] != "Namespace"]
                for path, _, _, uid, request in sorted(
                    cluster, key=lambda r: r[1] == "CustomResourceDefinition"
                ):
                    self.delete(path, uid, request)
            self.release_docker_assets()
            require(not preserved, "crd_instances_preserved")
            self.verify()
            self.note("cleanup_complete", True)
        except BaseException as exc:
            self.note("cleanup_complete", False)
            raise RuntimeError("cleanup_failed_resources_preserved") from exc

    def release_docker_assets(self) -> None:
        if not self.docker_assets():
            return
        for kind, name, identity in self.db.execute(
            "SELECT * FROM assets ORDER BY rowid DESC"
        ).fetchall():
            if kind == "tag_intent" and name in run(TAGS).stdout.split():
                labels = self.labels("image", name)
                require(
                    labels.get(REQUEST) == identity and labels.get(NONCE) == self.args.nonce,
                    "image_identity_changed",
                )
                run(["docker", "image", "rm", name])
            elif kind == "container_intent":
                named = run(
                    ["docker", "ps", "-aq", "--no-trunc", "--filter", "name=^/" + name + "$"]
                ).stdout.split()
                for found in dict.fromkeys(named + self.labelled("ps", "-aq")):
                    labels = self.labels("container", found)
                    ours = (
                        labels.get(REQUEST) == identity
                        and labels.get(NONCE) == self.args.nonce
                        and labels.get(OWNER) == "factory-tools"
                    )
                    require(ours or found not in named, "container_identity_changed")
                    if ours:
                        run(["docker", "rm", "-f", found])
        # Untagged leftovers (dangling build results) still carry this execution's nonce.
        for image in self.labelled("image", "ls", "-q"):
            run(["docker", "image", "rm", image])

    @staticmethod
    def labels(kind: str, name: str) -> dict[str, str]:
        inspected = run(["docker", kind, "inspect", "--format", "{{json .Config.Labels}}", name])
        return dict(json.loads(inspected.stdout) or {})


def ledger_phase(path: Path) -> object:
    if not path.is_file():
        return None
    try:
        with contextlib.closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as db:
            row = db.execute("SELECT value FROM metadata WHERE key='phase'").fetchone()
    except sqlite3.Error:
        return None
    return json.loads(row[0]) if row else None


def stop(signum: int, _frame: object) -> None:
    raise SystemExit(128 + signum)


def check_failure_cleanup(
    args: argparse.Namespace, argv_for_nonce: Callable[[str], list[str]]
) -> int:
    """Run normal, error and signal children and prove each one left nothing behind."""
    candidate = ""
    previous = {sig: signal.signal(sig, stop) for sig in SIGNALS}
    try:
        for fault, expected in (("", 0), ("error", 42), ("signal", 143)):
            nonce, slice_ = os.urandom(4).hex(), args.slice
            child_args = argparse.Namespace(**vars(args))
            vars(child_args).update(
                nonce=nonce,
                namespace=f"fd-{slice_}-{nonce}",
                release=f"fd-{slice_}-{nonce}",
                daemon_namespace=f"fd-{slice_}-d-{nonce}",
                controller_namespace=f"fd-{slice_}-c-{nonce}",
                work_dir=f".projects/daemon-{slice_}-{nonce}",
                work=ROOT / f".projects/daemon-{slice_}-{nonce}",
            )
            child = subprocess.Popen(
                argv_for_nonce(nonce),
                env=dict(os.environ, FD_FIXTURE_FAULT=fault),
                stdout=subprocess.DEVNULL,
            )
            try:
                deadline, signalled = time.monotonic() + 21600, False
                while child.poll() is None:
                    require(time.monotonic() < deadline, "child_deadline")
                    ready = ledger_phase(child_args.work / "evidence.sqlite") == "signal-ready"
                    if fault == "signal" and not signalled and ready:
                        child.send_signal(signal.SIGTERM)
                        signalled = True
                    time.sleep(0.5)
            finally:
                if child.poll() is None:
                    child.terminate()  # the child's own teardown still runs
                    try:
                        child.wait(timeout=3600)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait()
                        try:
                            recovery = Fixture(child_args, recover=True, teardown=False)
                            with contextlib.closing(recovery.db):
                                recovery.cleanup()
                        finally:
                            raise RuntimeError("child_deadline")
            require(child.returncode == expected, "unexpected_child_outcome")
            fixture = Fixture(child_args, recover=True, teardown=False)
            with contextlib.closing(fixture.db):
                fixture.verify()
                meta = fixture.meta()
            require(meta.get("cleanup_complete") is True, "child_cleanup_incomplete")
            require(not candidate or meta.get("candidate") == candidate, "candidate_mismatch")
            candidate = meta["candidate"]
            listed = fixture.k("get", "ns", "-o", "name")
            names = {line.removeprefix("namespace/") for line in listed.stdout.split()}
            require(
                not names
                & {
                    child_args.namespace,
                    child_args.daemon_namespace,
                    child_args.controller_namespace,
                },
                "child_namespace_remains",
            )
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    print(
        json.dumps(
            {
                "candidate": candidate,
                "normal": True,
                "failure_cleanup": True,
                "signal_cleanup": True,
            }
        )
    )
    return 0


def main() -> int:
    def extra(parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--action", choices=("setup", "cleanup", "metadata", "self-test"), default="setup"
        )
        parser.add_argument("--check-failure-cleanup", action="store_true")

    code, category, fixture = 1, "", None
    try:
        args = arguments(extra)
        if args.check_failure_cleanup:
            slice_ = args.slice

            def child_argv(n: str) -> list[str]:
                return [sys.executable, __file__, "--action", "self-test"] + [
                    "--context",
                    args.context,
                    "--candidate",
                    "HEAD",
                    "--namespace",
                    f"fd-{slice_}-{n}",
                    "--release",
                    f"fd-{slice_}-{n}",
                    "--daemon-namespace",
                    f"fd-{slice_}-d-{n}",
                    "--work-dir",
                    f".projects/daemon-{slice_}-{n}",
                ]

            code = check_failure_cleanup(args, child_argv)
        else:
            fixture = Fixture(
                args,
                recover=args.action in {"cleanup", "metadata"},
                teardown=args.action != "metadata",
            )
            if args.action == "metadata":
                fixture.closed = True
                with contextlib.closing(fixture.db):
                    print(json.dumps(fixture.meta(), sort_keys=True))
            elif args.action == "setup":
                fixture.setup()
            elif args.action == "self-test":
                fixture.self_test()
            code = 0
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) and exc.code else 1
        category = "signal" if code > 128 else "exit"
    except RuntimeError as exc:
        category = str(exc)
        code = 42 if category == "intentional_failure" else 1
    except (
        OSError,
        ValueError,
        KeyError,
        IndexError,
        TypeError,
        StopIteration,
        sqlite3.Error,
        subprocess.SubprocessError,
    ) as exc:
        category = type(exc).__name__
    if fixture is not None:
        try:
            fixture.cleanup()
        except RuntimeError as exc:
            code, category = 1, str(exc)
    if code:
        print(json.dumps({"error": category or "failure"}), file=sys.stderr)
    return code


if __name__ == "__main__":
    sys.exit(main())
