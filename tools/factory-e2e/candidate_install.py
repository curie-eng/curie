"""An owned candidate install with the TLS GitHub stub, for opt-in live proofs.

The candidate-install proofs (#5427 resource retry, #5429 publication
recovery) share this harness. It installs the caller's candidate chart and
images into a namespace it creates on the caller's throwaway cluster, runs the
recorded TLS GitHub stub (tools/github-stub) as one Pod there on the candidate
worker image, and drives the install through the candidate CLI and API.

Nothing here names a host, user, registry or kube context: every target comes
from the caller's environment through ``read_inputs``. Before anything is
created, ``CandidateInstall.preflight`` proves the CLI runs, every image
resolves to a digest, the context answers, the namespace is absent, the
agent-sandbox controller runs and every chart CRD is served. Every cleanup is
registered before the resource it removes is created; ``remove_install``
uninstalls, deletes the owned namespace and every cluster-scoped object of
the release, then proves each absent.
"""

from __future__ import annotations

import base64
import contextlib
import json
import os
import re
import secrets
import shutil
import subprocess
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import factory_e2e as fe
import personal_owner_enforcement_support as poe
import resource_retry_support as rrs
import tls_code_host as tch
import yaml

ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / "charts" / "curie"
COMPONENTS = ("api", "worker", "runner")
PULL_POLICIES = ("Always", "IfNotPresent")
CLAIM_RESOURCE = "sandboxclaims.extensions.agents.x-k8s.io"
# apps/worker/src/curie_worker/sandbox/k8s.py ADMISSION_CLAIM_LABEL: every claim's pod carries it.
ADMISSION_CLAIM_LABEL = "sandbox.users.io/curie-admission-claim"
STUB_PORT = tch.STUB_PORT_DEFAULT
STUB_DATABASE = tch.STUB_DATABASE
TERMINAL = frozenset({"completed", "failed", "expired", "cancelled"})
HELM_RELEASE_NAME = "meta.helm.sh/release-name"
HELM_RELEASE_NAMESPACE = "meta.helm.sh/release-namespace"
# Match the chart's platform priority so a sandbox runner cannot preempt the forge.
FIXTURE_PRIORITY = 1_000_000
PRIORITY_CLASS_RESOURCE = "priorityclasses.scheduling.k8s.io"
# Cluster-scoped kinds the chart can render from a release, swept by Helm annotation.
CLUSTER_SWEEP = (
    "clusterroles.rbac.authorization.k8s.io",
    "clusterrolebindings.rbac.authorization.k8s.io",
    PRIORITY_CLASS_RESOURCE,
    "validatingadmissionpolicies.admissionregistration.k8s.io",
    "validatingadmissionpolicybindings.admissionregistration.k8s.io",
    "namespaces",
    "runtimeclasses.node.k8s.io",
)
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_DNS_LABEL = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$")

# Run inside the stub Pod: one human-labelled issue, written the way the stub's
# own seed issue 3815 is (tools/github-stub/github_stub.py _issue and
# _label_event), so poll intake admits it like a person's label.
SEED_ISSUE_PROGRAM = f"""
import json, sqlite3, sys
from datetime import UTC, datetime
title, body, label = sys.argv[1:4]
now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
connection = sqlite3.connect({STUB_DATABASE!r}, timeout=30, isolation_level="IMMEDIATE")
try:
    connection.execute("UPDATE objects SET kind=kind WHERE 0")
    seed = json.loads(connection.execute(
        "SELECT body FROM objects WHERE kind='issue' AND id=?", ({tch.STUB_ISSUE},)
    ).fetchone()[0])
    number = int(connection.execute(
        "SELECT COALESCE(MAX(id), 0) FROM objects WHERE kind='issue'"
    ).fetchone()[0]) + 1
    issue = dict(
        seed, id=number, number=number, title=title, body=body, state="open",
        labels=[{{"name": label}}], created_at=now, updated_at=now,
        html_url=seed["html_url"].rsplit("/", 1)[0] + "/" + str(number),
    )
    connection.execute(
        "INSERT INTO objects(kind, id, body) VALUES('issue', ?, ?)", (number, json.dumps(issue))
    )
    event = int(connection.execute(
        "SELECT COALESCE(MAX(id), 0) FROM objects WHERE kind='event'"
    ).fetchone()[0]) + 1
    row = {{
        "id": event, "issue_number": number, "event": "labeled", "label": {{"name": label}},
        "actor": seed["user"], "created_at": now, "performed_via_github_app": None,
    }}
    connection.execute(
        "INSERT INTO objects(kind, id, body) VALUES('event', ?, ?)", (event, json.dumps(row))
    )
    connection.commit()
finally:
    connection.close()
print(json.dumps({{"number": number}}))
"""

# The stub needs its seed tree and issue file in a writable directory; this is
# the boot program personal_owner_enforcement.py runs for the same manifests.
STUB_BOOT = f"""\
import os, shutil, sys
seed, work = {poe.STUB_SEED_DIR!r}, {poe.STUB_WORK_DIR!r}
os.makedirs(os.path.join(work, "tree"))
for name in sorted(os.listdir(os.path.join(seed, "tree"))):
    if not name.startswith("."):
        shutil.copyfile(os.path.join(seed, "tree", name), os.path.join(work, "tree", name))
shutil.copyfile(os.path.join(seed, "issue.md"), os.path.join(work, "issue.md"))
stub = [sys.executable, *sys.argv[1:]]
os.execv(sys.executable, stub)
"""


class Refused(Exception):
    """A missing or unusable prerequisite; the run stops before or without proof."""


# --------------------------------------------------------------------------
# Inputs
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Inputs:
    context: str
    namespace: str
    release: str
    cli: str
    images: dict[str, str]
    pull_policy: str

    @property
    def publication_namespace(self) -> str:
        return f"{self.namespace}-{self.release}-publication"


@dataclass(frozen=True)
class InputNames:
    """The environment variables one live proof reads its inputs from."""

    flag: str
    namespace: str
    release: str
    pull_policy: str
    context: str = "CURIE_FACTORY_KUBE_CONTEXT"
    cli: str = "CURIE_C4_CANDIDATE_CLI"
    images: str = "CURIE_C4_CANDIDATE_IMAGES"


def read_inputs(env: Mapping[str, str], names: InputNames) -> Inputs:
    """Every caller input, or one refusal naming each missing or malformed one. Pure."""

    problems: list[str] = []
    context = env.get(names.context, "").strip()
    if not context:
        problems.append(f"{names.context} (the kube context of a throwaway cluster)")
    namespace = env.get(names.namespace, "").strip()
    if not _DNS_LABEL.match(namespace):
        problems.append(f"{names.namespace} (a new namespace, a DNS label)")
    release = env.get(names.release, "").strip()
    if (
        release == "curie"
        or not release.startswith("curie-")
        or len(release) > 30
        or not _DNS_LABEL.match(release)
    ):
        problems.append(
            f"{names.release} (curie-<suffix>, at most 30 characters, never curie: the chart "
            "names cluster-scoped objects from the release)"
        )
    elif namespace and len(f"{namespace}-{release}-publication") > 63:
        problems.append(
            f"{names.namespace} and {names.release} too long: <namespace>-<release>-publication "
            "must fit 63 characters"
        )
    cli = env.get(names.cli, "").strip()
    if not cli or not os.path.isabs(cli) or not os.access(cli, os.X_OK):
        problems.append(f"{names.cli} (an absolute path to the compiled candidate curie)")
    template = env.get(names.images, "").strip()
    images: dict[str, str] = {}
    if "{component}" not in template:
        problems.append(f"{names.images} (an image reference template containing {{component}})")
    else:
        images = {component: template.format(component=component) for component in COMPONENTS}
        for component, ref in images.items():
            try:
                image_values(ref, "IfNotPresent")
            except Refused as exc:
                problems.append(f"{names.images} for {component}: {exc}")
    pull_policy = env.get(names.pull_policy, "").strip()
    if pull_policy not in PULL_POLICIES:
        problems.append(f"{names.pull_policy} (one of {', '.join(PULL_POLICIES)})")
    if problems:
        raise Refused(
            f"{names.flag}=1 needs every caller input; missing or malformed: " + "; ".join(problems)
        )
    return Inputs(context, namespace, release, cli, images, pull_policy)


def image_values(ref: str, pull_policy: str) -> dict[str, str]:
    """Chart image fields for one reference: a digest wins over a tag. Pure."""

    if "@" in ref:
        repository, digest = ref.split("@", 1)
        if not _DIGEST.fullmatch(digest) or not repository:
            raise Refused(f"{ref!r} has a malformed digest")
        return {"repository": repository, "tag": "", "digest": digest, "pullPolicy": pull_policy}
    slash, colon = ref.rfind("/"), ref.rfind(":")
    if colon <= slash or colon == len(ref) - 1:
        raise Refused(f"{ref!r} names no tag or digest")
    return {
        "repository": ref[:colon],
        "tag": ref[colon + 1 :],
        "digest": "",
        "pullPolicy": pull_policy,
    }


def image_digests(inspect_output: str) -> tuple[str, frozenset[str]]:
    """The top digest and every digest ``docker buildx imagetools inspect`` printed. Pure."""

    top = None
    for line in inspect_output.splitlines():
        name, _, value = line.strip().partition(":")
        if name == "Digest":
            found = _DIGEST.search(value)
            top = found.group(0) if found else None
            break
    if top is None:
        raise Refused("the image inspection printed no Digest line")
    return top, frozenset(_DIGEST.findall(inspect_output))


def pod_digests(pods: list[dict[str, Any]], container: str) -> set[str]:
    found: set[str] = set()
    for pod in pods:
        for status in (pod.get("status") or {}).get("containerStatuses") or []:
            if status.get("name") == container and status.get("imageID"):
                found.update(_DIGEST.findall(str(status["imageID"])))
    return found


# --------------------------------------------------------------------------
# The harness
# --------------------------------------------------------------------------


@dataclass
class CandidateInstall:
    """One owned candidate install on a caller's throwaway cluster, with the TLS GitHub stub.

    Every cleanup is registered on ``teardown`` before the resource it removes
    is created; the caller runs it on every exit.
    """

    inputs: Inputs
    workdir: Path
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    teardown: fe.Teardown = field(default_factory=fe.Teardown)
    digests: dict[str, frozenset[str]] = field(default_factory=dict)
    rendered: list[tuple[str, str]] = field(default_factory=list)
    cluster_resources: dict[tuple[str, str], str] = field(default_factory=dict)
    _fixture_priority_class: str | None = field(default=None, init=False)
    api_url: str = ""
    api_key: str = field(default="", repr=False)
    forward: subprocess.Popen[Any] | None = None
    watch: subprocess.Popen[Any] | None = None
    watch_file: Path | None = None
    stub_ip: str = ""
    stub_ca: str = ""
    system_roots: str = ""
    kube_version: str = ""
    app_key: Path | None = None
    log_name: str = "candidate-install"
    cli_env: str = "CURIE_C4_CANDIDATE_CLI"

    # --- plumbing -----------------------------------------------------

    def log(self, message: str) -> None:
        fe.log(f"{self.log_name} [{self.run_id}]: {message}")

    def kubectl(self, *args: str, check: bool = True, input_text: str | None = None) -> str:
        return fe.run(
            ["kubectl", "--context", self.inputs.context, *args],
            check=check,
            input_text=input_text,
        )

    def helm(self, *args: str, check: bool = True) -> str:
        return fe.run(["helm", "--kube-context", self.inputs.context, *args], check=check)

    def private_file(self, name: str, text: str) -> Path:
        path = self.workdir / name
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            stream.write(text)
        return path

    def sql(self, query: str) -> Any:
        script = 'psql -qU "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -tA -f -'
        out = self.kubectl(
            "-n",
            self.inputs.namespace,
            "exec",
            "-i",
            f"statefulset/{self.inputs.release}-postgres",
            "--",
            "sh",
            "-c",
            script,
            input_text=query,
        )
        return json.loads(out)

    def api(self, method: str, path: str, body: Any = None, *, expected: int = 200) -> Any:
        headers = {"X-API-Key": self.api_key}
        try:
            status, answer = fe.http_json(method, self.api_url + path, headers=headers, body=body)
        except OSError:
            self.port_forward()
            status, answer = fe.http_json(method, self.api_url + path, headers=headers, body=body)
        if status != expected:
            detail = answer if isinstance(answer, (dict, list, str)) else None
            raise AssertionError(
                f"candidate API {method} {path} answered {status}, expected {expected}: {detail}"
            )
        return answer

    def stub_exec(self, *argv: str) -> str:
        return self.kubectl(
            "-n", self.inputs.namespace, "exec", tch.STUB_NAME, "-c", tch.STUB_NAME, "--", *argv
        )

    def stub_api(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        out = self.stub_exec(
            "python3",
            "-c",
            tch._STUB_CLIENT,
            method,
            path,
            json.dumps(body) if body is not None else "",
            str(STUB_PORT),
        )
        reply = json.loads(out.strip().splitlines()[-1])
        return int(reply["status"]), reply["body"]

    # --- preflight (read-only) ------------------------------------------

    def preflight(self) -> None:
        for tool in ("kubectl", "helm", "docker"):
            if shutil.which(tool) is None:
                raise Refused(f"required executable not on PATH: {tool}")
        try:
            fe.resolve_curie_binary(self.inputs.cli)
        except fe.ConfigError as exc:
            raise Refused(f"{self.cli_env}: {exc}") from None
        for component, ref in self.inputs.images.items():
            result = subprocess.run(
                ["docker", "buildx", "imagetools", "inspect", ref],
                capture_output=True,
                text=True,
                check=False,
                timeout=120,
            )
            if result.returncode != 0:
                raise Refused(
                    f"the {component} image {ref} does not resolve: "
                    f"{(result.stderr or result.stdout).strip()[-400:]}"
                )
            top, every = image_digests(result.stdout)
            pinned = image_values(ref, self.inputs.pull_policy)["digest"]
            if pinned and pinned != top:
                raise Refused(f"the {component} image {ref} resolves to {top}, not its digest")
            self.digests[component] = every
            self.log(f"{component} image resolves to {top}")
        try:
            self.kubectl("get", "--raw", "/readyz")
        except fe.PreflightFailed as exc:
            raise Refused(f"the kube context does not answer: {exc}") from None
        version = json.loads(self.kubectl("version", "-o", "json"))["serverVersion"]["gitVersion"]
        major, minor = (int(part) for part in re.findall(r"\d+", version)[:2])
        if (major, minor) < (1, 30):
            raise Refused(f"the cluster runs {version}; runner resources need 1.30 or newer")
        for name in (self.inputs.namespace, self.inputs.publication_namespace):
            if self.kubectl("get", "namespace", name, "--ignore-not-found", "-o", "name").strip():
                raise Refused(f"namespace {name} already exists; pass a new namespace")
        if not self.kubectl(
            "-n",
            fe.SANDBOX_CONTROLLER_NAMESPACE,
            "get",
            "deployment",
            fe.SANDBOX_CONTROLLER_DEPLOYMENT,
            "--ignore-not-found",
            "-o",
            "name",
        ).strip():
            raise Refused(
                "the cluster runs no agent-sandbox controller; this test never installs one"
            )
        for manifest in sorted((CHART / "crds").glob("*.yaml")):
            for document in yaml.safe_load_all(manifest.read_text()):
                if not isinstance(document, dict):
                    continue
                name = document["metadata"]["name"]
                if not self.kubectl("get", "crd", name, "--ignore-not-found", "-o", "name").strip():
                    raise Refused(
                        f"the cluster does not serve CRD {name}; this test never installs a CRD"
                    )
        self.kube_version = version
        for line in self.kubectl(
            "api-resources", "--namespaced=false", "--no-headers"
        ).splitlines():
            parts = line.split()
            if len(parts) < 4:
                continue
            name, api_version, kind = parts[0], parts[-3], parts[-1]
            group = api_version.split("/")[0] if "/" in api_version else ""
            self.cluster_resources[(api_version, kind)] = f"{name}.{group}" if group else name

    def render_cluster_objects(self, values_file: Path) -> None:
        """The cluster-scoped objects the release will create, from the candidate chart."""

        rendered = self.helm(
            "template",
            self.inputs.release,
            str(CHART),
            "-n",
            self.inputs.namespace,
            "-f",
            str(values_file),
            "--skip-crds",
            "--kube-version",
            self.kube_version,
        )
        for document in yaml.safe_load_all(rendered):
            if not isinstance(document, dict) or "kind" not in document:
                continue
            resource = self.cluster_resources.get((document["apiVersion"], document["kind"]))
            if resource is None:
                continue
            name = document["metadata"]["name"]
            self.rendered.append((resource, name))
            if self.kubectl("get", resource, name, "--ignore-not-found", "-o", "name").strip():
                raise Refused(
                    f"{resource}/{name} already exists; another release owns that name. "
                    "Pass another release name."
                )
        self.log(f"tracking {len(self.rendered)} cluster-scoped objects for teardown")

    # --- teardown ---------------------------------------------------------

    def release_owned(self, item: dict[str, Any]) -> bool:
        annotations = item.get("metadata", {}).get("annotations") or {}
        return (
            annotations.get(HELM_RELEASE_NAME) == self.inputs.release
            and annotations.get(HELM_RELEASE_NAMESPACE) == self.inputs.namespace
            and annotations.get(fe.RUN_ANNOTATION, self.run_id) == self.run_id
        )

    def remove_install(self) -> dict[str, Any]:
        """Uninstall, delete the owned namespace and every release object, prove absence."""

        ns = self.inputs.namespace
        problems: list[str] = []
        listed = self.helm("status", self.inputs.release, "-n", ns, check=False)
        if listed.strip():
            uninstall = subprocess.run(
                [
                    "helm",
                    "--kube-context",
                    self.inputs.context,
                    "uninstall",
                    self.inputs.release,
                    "-n",
                    ns,
                    "--wait",
                    "--timeout",
                    "5m",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            if uninstall.returncode != 0 and "not found" not in uninstall.stderr:
                problems.append("helm uninstall: " + uninstall.stderr.strip()[-400:])
        raw = self.kubectl("get", "namespace", ns, "--ignore-not-found", "-o", "json")
        if raw.strip():
            annotations = json.loads(raw)["metadata"].get("annotations") or {}
            if annotations.get(fe.RUN_ANNOTATION) == self.run_id:
                self.kubectl("delete", "namespace", ns, "--ignore-not-found", "--wait=false")
            else:
                problems.append(f"namespace {ns} is not this run's; left in place")
        for resource, name in self.rendered:
            raw = self.kubectl("get", resource, name, "--ignore-not-found", "-o", "json")
            if raw.strip() and self.release_owned(json.loads(raw)):
                self.kubectl("delete", resource, name, "--ignore-not-found", "--wait=false")
        for resource in CLUSTER_SWEEP:
            listing = self.kubectl("get", resource, "-o", "json", check=False)
            for item in (json.loads(listing).get("items") or []) if listing.strip() else []:
                if self.release_owned(item):
                    self.kubectl(
                        "delete",
                        resource,
                        item["metadata"]["name"],
                        "--ignore-not-found",
                        "--wait=false",
                    )

        def remaining() -> list[str]:
            left = [
                f"{resource}/{name}"
                for resource, name in [*self.rendered, ("namespaces", ns)]
                if self.kubectl("get", resource, name, "--ignore-not-found", "-o", "name").strip()
            ]
            for resource in CLUSTER_SWEEP:
                listing = self.kubectl("get", resource, "-o", "json", check=False)
                for item in (json.loads(listing).get("items") or []) if listing.strip() else []:
                    if self.release_owned(item):
                        left.append(f"{resource}/{item['metadata']['name']}")
            return sorted(set(left))

        left = remaining()

        def cleared() -> bool:
            left[:] = remaining()
            return not left

        if left:
            rrs.poll_until(cleared, timeout=900, interval=5)
        if left:
            problems.append("still present after teardown: " + ", ".join(left))
        if problems:
            raise AssertionError("; ".join(problems))
        return {"absent": [f"{r}/{n}" for r, n in self.rendered] + [f"namespaces/{ns}"]}

    # --- setup ------------------------------------------------------------

    def create_namespace(self) -> None:
        self.teardown.push("release, namespace and cluster objects", self.remove_install)
        owner_key, owner_value = fe.OWNER_LABEL.split("=", 1)
        manifest = {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": self.inputs.namespace,
                "labels": {owner_key: owner_value},
                "annotations": {fe.RUN_ANNOTATION: self.run_id},
            },
        }
        self.kubectl("create", "-f", "-", input_text=json.dumps(manifest))
        self.log(f"namespace {self.inputs.namespace} created")

    def _create_fixture_priority_class(self) -> str:
        name = f"{self.inputs.namespace}-fixture"
        if self.kubectl(
            "get", PRIORITY_CLASS_RESOURCE, name, "--ignore-not-found", "-o", "name"
        ).strip():
            raise Refused(f"PriorityClass {name} already exists; left in place")
        self.rendered.append((PRIORITY_CLASS_RESOURCE, name))
        manifest = {
            "apiVersion": "scheduling.k8s.io/v1",
            "kind": "PriorityClass",
            "metadata": {
                "name": name,
                "annotations": {
                    HELM_RELEASE_NAME: self.inputs.release,
                    HELM_RELEASE_NAMESPACE: self.inputs.namespace,
                    fe.RUN_ANNOTATION: self.run_id,
                },
            },
            "value": FIXTURE_PRIORITY,
            "globalDefault": False,
            "preemptionPolicy": "Never",
        }
        self.kubectl("create", "-f", "-", input_text=json.dumps(manifest))
        return name

    def create_fixture_manifests(self, manifests: list[dict[str, Any]]) -> None:
        if self._fixture_priority_class is None:
            self._fixture_priority_class = self._create_fixture_priority_class()
        for manifest in manifests:
            if manifest["kind"] == "Pod":
                manifest["spec"]["priorityClassName"] = self._fixture_priority_class
        listing = {"apiVersion": "v1", "kind": "List", "items": manifests}
        self.kubectl("create", "-f", "-", input_text=json.dumps(listing))

    def stub_manifests(
        self,
        seed_files: Mapping[str, str],
        seed_issue: str,
        *,
        entrypoint: str = "/stub/github_stub.py",
    ) -> list[dict[str, Any]]:
        image = self.inputs.images["worker"]
        manifests = poe.code_host_manifests(
            tch.stub_manifests(self.inputs.namespace, image, STUB_PORT),
            image=image,
            port=STUB_PORT,
            seed_files=seed_files,
            issue_body=seed_issue,
            boot=STUB_BOOT,
            labels={},
        )
        for manifest in manifests:
            if manifest["kind"] == "Pod":
                container = manifest["spec"]["containers"][0]
                container["imagePullPolicy"] = self.inputs.pull_policy
                container["args"][0:0] = [entrypoint, "serve"]
        return manifests

    def start_stub(self, seed_files: Mapping[str, str], seed_issue: str) -> None:
        self.create_fixture_manifests(self.stub_manifests(seed_files, seed_issue))
        self.kubectl(
            "-n",
            self.inputs.namespace,
            "wait",
            "--for=condition=Ready",
            f"pod/{tch.STUB_NAME}",
            "--timeout=600s",
        )

        def started() -> dict[str, str] | None:
            logs = self.kubectl(
                "-n",
                self.inputs.namespace,
                "logs",
                tch.STUB_NAME,
                "-c",
                tch.STUB_NAME,
                check=False,
            )
            with contextlib.suppress(fe.PreflightFailed):
                return tch.parse_stub_startup(logs)
            return None

        startup = fe._wait("the GitHub stub startup line", 120, started, 2)
        expected = f"https://{tch.stub_host(self.inputs.namespace)}:{STUB_PORT}"
        assert startup["base_url"] == expected, f"the stub advertises {startup['base_url']}"
        self.stub_ip = self.kubectl(
            "-n",
            self.inputs.namespace,
            "get",
            "pod",
            tch.STUB_NAME,
            "-o",
            "jsonpath={.status.podIP}",
        ).strip()
        stub = self.kubectl("-n", self.inputs.namespace, "get", "pod", tch.STUB_NAME, "-o", "json")
        self.assert_digest(json.loads(stub), tch.STUB_NAME, "worker")
        # The seed issue carries the default factory label; park it so nothing admits it.
        status, issue = self.stub_api(
            "PATCH", f"/repos/{tch.STUB_REPO}/issues/{tch.STUB_ISSUE}", {"state": "closed"}
        )
        assert status == 200 and issue.get("state") == "closed", f"stub refused to park: {status}"
        ca = self.stub_exec("cat", tch.STUB_CA_FILE)
        roots = self.stub_exec("cat", tch.SYSTEM_ROOTS)
        self.stub_ca, self.system_roots = ca, roots
        assert "CERTIFICATE" in ca and "PRIVATE" not in ca, "the stub CA is not one certificate"
        bundle = self.private_file("code-host-ca.crt", ca.rstrip("\n") + "\n" + roots)
        self.kubectl(
            "-n",
            self.inputs.namespace,
            "create",
            "configmap",
            tch.CA_CONFIG_MAP,
            f"--from-file={tch.CA_KEY}={bundle}",
        )
        key = self.workdir / "app.pem"
        fe.run(
            [
                "openssl",
                "genpkey",
                "-algorithm",
                "RSA",
                "-pkeyopt",
                "rsa_keygen_bits:2048",
                "-out",
                str(key),
            ]
        )
        key.chmod(0o600)
        self.kubectl(
            "-n",
            self.inputs.namespace,
            "create",
            "secret",
            "generic",
            fe.APP_KEY_REF,
            f"--from-file=privateKey={key}",
        )
        self.app_key = key
        self.log("GitHub stub serving, CA and App key in place")

    def trust_code_host_ca(self, certificate: str) -> None:
        """Add one more public CA certificate to the install's code host trust bundle.

        Replaces the bundle ConfigMap ``start_stub`` created, before the install
        reads it: the stub CA, then ``certificate``, then the system roots.
        """

        if not self.stub_ca:
            raise AssertionError("the code host trust bundle is created with the stub first")
        if "CERTIFICATE" not in certificate or "PRIVATE" in certificate:
            raise AssertionError("an added code host CA must be one public certificate")
        bundle = self.private_file(
            "code-host-ca.crt",
            self.stub_ca.rstrip("\n") + "\n" + certificate.rstrip("\n") + "\n" + self.system_roots,
        )
        rendered = self.kubectl(
            "-n",
            self.inputs.namespace,
            "create",
            "configmap",
            tch.CA_CONFIG_MAP,
            f"--from-file={tch.CA_KEY}={bundle}",
            "--dry-run=client",
            "-o",
            "json",
        )
        self.kubectl("-n", self.inputs.namespace, "replace", "-f", "-", input_text=rendered)

    def require_app_key(self) -> Path:
        if self.app_key is None:
            raise AssertionError("the App key is created with the stub, before the values")
        return self.app_key

    def base_values(self, *, model_base_url: str | None = None) -> dict[str, Any]:
        """Install values for the candidate images and the stub, before any proof's own."""

        config = fe.FactoryConfig(
            kube_context=self.inputs.context,
            app_id=tch.STUB_APP_ID,
            installation_id=tch.STUB_INSTALLATION_ID,
            private_key_file=self.require_app_key(),
            repo=tch.STUB_REPO,
            label=tch.STUB_LABEL,
            mention=tch.STUB_MENTION,
            cloudflared="unused",
            priority_classes=None,
            restore_webhook_url=None,
            webhook_secret=secrets.token_hex(24),
            actor_token="",
            model_base_url=model_base_url,
        )
        base = fe.install_values(
            config, candidate="unused", app_key_secret=fe.APP_KEY_REF, consumer_controller=True
        )
        values = tch.tls_values(
            base,
            namespace=self.inputs.namespace,
            ca_config_map=tch.CA_CONFIG_MAP,
            stub_cluster_ip=self.stub_ip,
            port=STUB_PORT,
            model_cidrs=(),
        )
        if model_base_url:
            values["security"]["networkPolicy"]["allowedEgress"].append(
                fe.model_proxy_egress(model_base_url)
            )
        # The API refuses to start unless every GitHub code host's identity host
        # is the host of its GitHub API base (Settings._validate_forge_github_host),
        # so the factory entries name the stub's authority, not github.com.
        authority = urlsplit(values["api"]["githubApiUrl"]).netloc
        for entry in (*values["factory"]["codeHosts"], *values["factory"]["trackers"]):
            if entry.get("kind") == "github":
                entry["host"] = authority
        pull = self.inputs.pull_policy
        for component in ("api", "worker"):
            values[component]["image"] = image_values(self.inputs.images[component], pull)
        for component in ("ui", "dispatcher", "mailAdapter"):
            values.setdefault(component, {})["deploy"] = False
        runner = image_values(self.inputs.images["runner"], pull)
        sandbox = values["agentSandbox"]
        sandbox["controller"] = {"deploy": False}
        sandbox["runner"].update(
            {
                "image": runner["repository"],
                "tag": runner["tag"],
                "digest": runner["digest"],
                "imagePullPolicy": pull,
                "prewarm": {"imagePullPolicy": pull},
                "fakeModel": model_base_url is None,
            }
        )
        factory = values["factory"]
        factory["baseLabelSettleSeconds"] = 0
        for tracker in factory["trackers"]:
            tracker["pollIntervalSeconds"] = 2
        values["langfuse"] = {"deploy": False, "host": "langfuse.example.com"}
        values["clickhouse"] = {"deploy": False}
        values["otelCollector"] = {"deploy": False, "telemetryDisabled": True}
        values["priorityClasses"] = fe.priority_class_values(None, self.inputs.release)
        return values

    def install(self, values: Mapping[str, Any]) -> None:
        values_file = self.private_file("values.json", json.dumps(values))
        self.render_cluster_objects(values_file)
        self.log("helm install (pulls every candidate image)")
        self.helm(
            "install",
            self.inputs.release,
            str(CHART),
            "-n",
            self.inputs.namespace,
            "-f",
            str(values_file),
            "--skip-crds",
            "--timeout",
            "20m",
        )
        values_file.unlink()
        # No --wait: the api's init container waits on the post-install migration hook.
        for kind in ("deployment", "statefulset"):
            for workload in self.kubectl(
                "-n", self.inputs.namespace, "get", kind, "-o", "name"
            ).split():
                try:
                    self.kubectl(
                        "-n", self.inputs.namespace, "rollout", "status", workload, "--timeout=15m"
                    )
                except fe.PreflightFailed:
                    self.report_unready_pods()
                    raise
        pods = json.loads(self.kubectl("-n", self.inputs.namespace, "get", "pods", "-o", "json"))[
            "items"
        ]
        for component in ("api", "worker"):
            selected = [
                pod
                for pod in pods
                if (pod["metadata"].get("labels") or {}).get("app.kubernetes.io/component")
                == component
                and not pod["metadata"].get("deletionTimestamp")
            ]
            assert selected, f"no running {component} pod after install"
            for pod in selected:
                self.assert_digest(pod, component, component)
        self.log("installed; api and worker pods run the candidate digests")

    def report_unready_pods(self) -> None:
        """Log every unready container's state and log tail, so a failed rollout says why."""

        listing = self.kubectl(
            "-n", self.inputs.namespace, "get", "pods", "-o", "json", check=False
        )
        try:
            pods = json.loads(listing).get("items") or []
        except ValueError:
            pods = []
        for pod in pods:
            name = pod["metadata"]["name"]
            status = pod.get("status") or {}
            for entry in [
                *(status.get("initContainerStatuses") or []),
                *(status.get("containerStatuses") or []),
            ]:
                if (
                    entry.get("ready")
                    or (entry.get("state") or {}).get("terminated", {}).get("exitCode") == 0
                ):
                    continue
                state, last = json.dumps(entry.get("state")), json.dumps(entry.get("lastState"))
                self.log(
                    f"unready {name}/{entry.get('name')}: state {state} last {last} "
                    f"restarts {entry.get('restartCount')}"
                )
                for previous in ("--previous", None):
                    tail = self.kubectl(
                        "-n",
                        self.inputs.namespace,
                        "logs",
                        name,
                        "-c",
                        str(entry.get("name")),
                        "--tail=40",
                        *([previous] if previous else []),
                        check=False,
                    )
                    if tail.strip():
                        self.log(f"log tail {name}/{entry.get('name')}:\n{tail.rstrip()}")
                        break

    def assert_digest(self, pod: dict[str, Any], container: str, component: str) -> None:
        seen = pod_digests([pod], container)
        expected = self.digests[component]
        assert seen and seen <= expected, (
            f"pod {pod['metadata']['name']} container {container} runs "
            f"{sorted(seen) or 'nothing'}, not the resolved {component} candidate digests "
            f"{sorted(expected)}"
        )

    def port_forward(self) -> None:
        if self.forward is not None:
            fe._stop(self.forward)
        port = int(self.api_url.rsplit(":", 1)[1]) if self.api_url else fe._free_port()
        if self.forward is None and not self.api_url:
            self.teardown.push(
                "stop the API port-forward", lambda: {"stopped": self.stop_forward()}
            )
        process = subprocess.Popen(
            [
                "kubectl",
                "--context",
                self.inputs.context,
                "-n",
                self.inputs.namespace,
                "port-forward",
                f"svc/{self.inputs.release}-api",
                f"{port}:8000",
                "--address",
                "127.0.0.1",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.forward = process
        self.api_url = f"http://127.0.0.1:{port}"

        def healthy() -> bool:
            try:
                status, _ = fe.http_json("GET", self.api_url + "/health", timeout=5)
            except OSError:
                return False
            return status == 200

        fe._wait("the API through port-forward", 120, healthy, 2)
        if not self.api_key:
            secret = json.loads(
                self.kubectl(
                    "-n",
                    self.inputs.namespace,
                    "get",
                    "secret",
                    f"{self.inputs.release}-secrets",
                    "-o",
                    "json",
                )
            )
            self.api_key = base64.b64decode(secret["data"]["apiKey"]).decode()

    def stop_forward(self) -> bool:
        return self.forward is None or fe._stop(self.forward)

    def start_pod_watch(self) -> None:
        """Every sandbox pod state, streamed to a private file; a pod can live seconds."""

        self.watch_file = self.private_file("sandbox-pods.watch.json", "")
        self.teardown.push("stop the sandbox pod watch", lambda: {"stopped": self.stop_watch()})
        self.spawn_watch()

    def spawn_watch(self) -> None:
        assert self.watch_file is not None
        with open(self.watch_file, "ab") as sink:
            self.watch = subprocess.Popen(
                [
                    "kubectl",
                    "--context",
                    self.inputs.context,
                    "-n",
                    self.inputs.namespace,
                    "get",
                    "pods",
                    "-l",
                    ADMISSION_CLAIM_LABEL,
                    "--watch",
                    "--output-watch-events",
                    "-o",
                    "json",
                ],
                stdout=sink,
                stderr=subprocess.DEVNULL,
            )

    def stop_watch(self) -> bool:
        return self.watch is None or fe._stop(self.watch)

    def pods_for(self, claim: str) -> list[dict[str, Any]]:
        """Every observed state of the pods ``claim`` created, oldest first."""

        if self.watch is not None and self.watch.poll() is not None:
            self.spawn_watch()
        assert self.watch_file is not None
        events = tch._watch_events(self.watch_file.read_text(errors="replace"))
        states = [event["object"] for event in events]
        live = json.loads(
            self.kubectl(
                "-n",
                self.inputs.namespace,
                "get",
                "pods",
                "-l",
                f"{ADMISSION_CLAIM_LABEL}={claim}",
                "-o",
                "json",
            )
        )["items"]
        return [
            pod
            for pod in [*states, *live]
            if (pod.get("metadata", {}).get("labels") or {}).get(ADMISSION_CLAIM_LABEL) == claim
        ]

    def sandbox_claims(self) -> set[str]:
        items = json.loads(
            self.kubectl("-n", self.inputs.namespace, "get", CLAIM_RESOURCE, "-o", "json")
        )["items"]
        return {item["metadata"]["name"] for item in items}

    def sandbox_pod_claims(self) -> set[str]:
        assert self.watch_file is not None
        events = tch._watch_events(self.watch_file.read_text(errors="replace"))
        return {
            str(
                (event["object"].get("metadata", {}).get("labels") or {}).get(ADMISSION_CLAIM_LABEL)
            )
            for event in events
        }

    def create_agent(self, name: str, trigger_label: str) -> str:
        """A GitHub-bound agent on the stub repository; returns its id."""

        agent = self.api(
            "POST",
            "/agents",
            {
                "name": name,
                "repo_full_name": tch.STUB_REPO,
                "channel": {
                    "kind": "github",
                    "address": tch.STUB_REPO,
                    "trigger_label": trigger_label,
                },
            },
            expected=201,
        )
        return str(agent["id"])

    def deploy_bundle(self, name: str, *, policy: str = "auto") -> None:
        """Deploy a minimal bundle for ``name`` and set its publication policy, with the CLI."""

        config_dir = self.workdir / f"curie-config-{name}"
        config_dir.mkdir(mode=0o700)
        env = {**os.environ, "CURIE_API_KEY": self.api_key, "CURIE_CONFIG_DIR": str(config_dir)}
        common = [
            "--namespace",
            self.inputs.namespace,
            "--release",
            self.inputs.release,
            "--api-url",
            self.api_url,
        ]
        bundle = self.workdir / f"bundle-{name}"
        (bundle / ".claude-plugin").mkdir(parents=True)
        (bundle / ".claude-plugin" / "plugin.json").write_text(
            json.dumps(
                {
                    "name": name,
                    "version": "0.1.0",
                    "description": "Live proof agent.",
                    "systemPrompt": "Answer the assignment.",
                }
            )
        )
        for argv in (
            [
                "deploy",
                "--plugin-dir",
                str(bundle),
                "--agent",
                name,
                "--env",
                "prod",
                "--chart",
                str(CHART),
                *common,
            ],
            ["publication-policy", name, "--policy", policy, *common],
        ):
            result = subprocess.run(
                [self.inputs.cli, "cluster", "--context", self.inputs.context, *argv],
                capture_output=True,
                text=True,
                env=env,
                check=False,
                timeout=1200,
            )
            if result.returncode != 0:
                tail = fe._redact((result.stderr or result.stdout).strip(), [self.api_key])
                raise AssertionError(f"curie cluster {argv[0]} failed: {tail[-1500:]}")

    def seed_issue(self, title: str, body: str, label: str) -> int:
        """A new stub issue labelled ``label`` by the stub's human actor; returns its number."""

        out = self.stub_exec("python3", "-c", SEED_ISSUE_PROGRAM, title, body, label)
        return int(json.loads(out.strip().splitlines()[-1])["number"])

    def requests(self, agent_id: str, issue: int) -> list[dict[str, Any]]:
        """Every execution request of the agent's WorkItem for stub issue ``issue``, in order."""

        agent = uuid.UUID(agent_id)
        rows = self.sql(
            "SELECT coalesce(json_agg(row_to_json(r) ORDER BY r.sequence), '[]'::json) FROM ("
            "SELECT e.* FROM curie.execution_requests e "
            "JOIN curie.work_items w ON w.id = e.work_item_id "
            f"WHERE w.agent_id = '{agent}' AND w.tracker_kind = 'github' "
            f"AND w.tracker_issue_id = '{int(issue)}') r"
        )
        assert isinstance(rows, list), f"unexpected request listing {rows!r}"
        return rows


def wait_for(
    what: str, timeout: float, probe: Callable[[], Any], observed: Callable[[], Any]
) -> Any:
    value = rrs.poll_until(probe, timeout=timeout, interval=3)
    if not value:
        raise AssertionError(
            f"timed out after {int(timeout)}s waiting for {what}; observed {observed()}"
        )
    return value
