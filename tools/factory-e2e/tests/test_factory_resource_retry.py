"""Live candidate-install proof for #5427 acceptance criterion 2 (ADR 0269).

A factory request whose runner ran out of memory, or whose pod the kubelet
evicted for ephemeral storage, must not run again under the same limit. Its
one automatic successor waits, sandbox-free, until the agent's runner limit
for that dimension is strictly larger, then starts exactly once with the
raised limit delivered.

Everything inside the changed path is the candidate: the API, worker,
reconciler, scheduler and resource resolution run from the caller's candidate
images, installed by the candidate chart and driven through the candidate
CLI. The only fixtures are the external model (the runner's offline fake tier,
told what to exhaust by ``FAKE_MODEL_DIRECTIVE`` set per agent through
``agentSandbox.runnerEnv``, because a factory turn's text is only the issue
link) and the external forge (the recorded TLS GitHub stub, run as one Pod in
the owned namespace on the candidate worker image).

Runbook
-------
Run only against a throwaway cluster the caller owns::

    CURIE_FACTORY_RESOURCE_RETRY_E2E=1 \\
    CURIE_FACTORY_KUBE_CONTEXT=<kube context> \\
    CURIE_FACTORY_RESOURCE_RETRY_NAMESPACE=<new namespace> \\
    CURIE_FACTORY_RESOURCE_RETRY_RELEASE=curie-<suffix> \\
    CURIE_FACTORY_RESOURCE_RETRY_PULL_POLICY=IfNotPresent \\
    CURIE_C4_CANDIDATE_CLI=<absolute path to the compiled candidate curie> \\
    CURIE_C4_CANDIDATE_IMAGES='<registry>/curie-{component}:<tag or @digest>' \\
    uv run pytest -q tools/factory-e2e/tests/test_factory_resource_retry.py --reruns 0

Inputs, all required once the flag is set (a missing or malformed one is a
refusal, never a skip):

1. ``CURIE_FACTORY_KUBE_CONTEXT``: the kube context every kubectl and helm
   call names. The cluster must run Kubernetes 1.30 or newer, already run the
   agent-sandbox controller and serve every CRD in ``charts/curie/crds``; this
   test never installs a cluster-wide CRD or controller.
2. ``CURIE_FACTORY_RESOURCE_RETRY_NAMESPACE``: a namespace that must not
   exist yet. The test creates it and deletes it.
3. ``CURIE_FACTORY_RESOURCE_RETRY_RELEASE``: the Helm release, ``curie-`` and
   a suffix, at most 30 characters, never ``curie``. The chart names its
   cluster-scoped objects from it.
4. ``CURIE_C4_CANDIDATE_CLI``: the compiled candidate CLI.
5. ``CURIE_C4_CANDIDATE_IMAGES``: an image reference template with
   ``{component}``, expanded for ``api``, ``worker`` and ``runner``. Each
   reference must be a registry tag or digest this host can inspect with
   ``docker buildx imagetools inspect`` and the cluster can pull.
6. ``CURIE_FACTORY_RESOURCE_RETRY_PULL_POLICY``: ``Always`` or
   ``IfNotPresent``, applied to every candidate image.

Before anything is created the test proves the CLI runs, every image resolves
to a digest, the context answers, the namespace is absent and no object the
release would create at cluster scope exists. Every cleanup is registered
before the resource it removes is created and runs on every exit: stop the
port-forward and the pod watch, uninstall the release, delete the namespace,
delete any remaining cluster-scoped object of this release (the rendered
ValidatingAdmissionPolicies and bindings such as ``<release>-runner-resources``,
the per-release PriorityClasses ``<release>-platform`` and
``<release>-sandbox``, the publication namespace, and any ClusterRole or
binding the render names), then prove each one absent.

Sizes stay small so no shared node fills: memory limits 448Mi then 1408Mi
around a 768 MiB hold, ephemeral-storage limits 256Mi then 896Mi around a
384 MiB scratch file in the runner's 512Mi /tmp volume, so the kubelet's
pod-level ephemeral-storage eviction fires, not the volume's own limit.
"""

from __future__ import annotations

import base64
import contextlib
import copy
import json
import os
import re
import secrets
import shutil
import subprocess
import tempfile
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import factory_e2e as fe
import personal_owner_enforcement_support as poe
import pytest
import tls_code_host as tch
import yaml

FLAG = "CURIE_FACTORY_RESOURCE_RETRY_E2E"
if os.environ.get(FLAG) != "1":
    pytest.skip(f"set {FLAG}=1 to run the live resource retry proof", allow_module_level=True)

ROOT = Path(__file__).resolve().parents[3]
CHART = ROOT / "charts" / "curie"
CONTEXT_ENV = "CURIE_FACTORY_KUBE_CONTEXT"
NAMESPACE_ENV = "CURIE_FACTORY_RESOURCE_RETRY_NAMESPACE"
RELEASE_ENV = "CURIE_FACTORY_RESOURCE_RETRY_RELEASE"
PULL_POLICY_ENV = "CURIE_FACTORY_RESOURCE_RETRY_PULL_POLICY"
CLI_ENV = "CURIE_C4_CANDIDATE_CLI"
IMAGES_ENV = "CURIE_C4_CANDIDATE_IMAGES"
COMPONENTS = ("api", "worker", "runner")
PULL_POLICIES = ("Always", "IfNotPresent")
CLAIM_RESOURCE = "sandboxclaims.extensions.agents.x-k8s.io"
# apps/worker/src/curie_worker/sandbox/k8s.py ADMISSION_CLAIM_LABEL: every claim's pod carries it.
ADMISSION_CLAIM_LABEL = "sandbox.users.io/curie-admission-claim"
DIRECTIVE_ENV = "FAKE_MODEL_DIRECTIVE"
STUB_PORT = tch.STUB_PORT_DEFAULT
STUB_DATABASE = tch.STUB_DATABASE
SEED_FILES = {
    "README.md": "# acme-bot\n\nFixture repository for the resource retry proof.\n",
    "example.py": 'def example() -> str:\n    return "example"\n',
}
SEED_ISSUE = "Resource retry proof fixture.\n"
# The API re-offers a gated successor this often (WORK_ITEM_RESOURCE_GATE_PROBE_SECONDS).
PROBE_SECONDS = 20
# How long a waiting successor is watched for a start that must not happen.
GATE_WINDOW_SECONDS = 3 * PROBE_SECONDS + 10
SETTLE_SECONDS = 2 * PROBE_SECONDS + 10
TERMINAL = frozenset({"completed", "failed", "expired", "cancelled"})
HELM_RELEASE_NAME = "meta.helm.sh/release-name"
HELM_RELEASE_NAMESPACE = "meta.helm.sh/release-namespace"
# Cluster-scoped kinds the chart can render from a release, swept by Helm annotation.
CLUSTER_SWEEP = (
    "clusterroles.rbac.authorization.k8s.io",
    "clusterrolebindings.rbac.authorization.k8s.io",
    "priorityclasses.scheduling.k8s.io",
    "validatingadmissionpolicies.admissionregistration.k8s.io",
    "validatingadmissionpolicybindings.admissionregistration.k8s.io",
    "namespaces",
    "runtimeclasses.node.k8s.io",
)
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_QUANTITY = re.compile(r"^([0-9]+)(Ki|Mi|Gi|Ti|k|M|G|T)?$")
_SCALE = {
    None: 1,
    "Ki": 1024,
    "Mi": 1024**2,
    "Gi": 1024**3,
    "Ti": 1024**4,
    "k": 1000,
    "M": 1000**2,
    "G": 1000**3,
    "T": 1000**4,
}
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
stub = [sys.executable, "/stub/github_stub.py", "serve", *sys.argv[1:]]
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


def read_inputs(env: dict[str, str]) -> Inputs:
    """Every caller input, or one refusal naming each missing or malformed one. Pure."""

    problems: list[str] = []
    context = env.get(CONTEXT_ENV, "").strip()
    if not context:
        problems.append(f"{CONTEXT_ENV} (the kube context of a throwaway cluster)")
    namespace = env.get(NAMESPACE_ENV, "").strip()
    if not _DNS_LABEL.match(namespace):
        problems.append(f"{NAMESPACE_ENV} (a new namespace, a DNS label)")
    release = env.get(RELEASE_ENV, "").strip()
    if (
        release == "curie"
        or not release.startswith("curie-")
        or len(release) > 30
        or not _DNS_LABEL.match(release)
    ):
        problems.append(
            f"{RELEASE_ENV} (curie-<suffix>, at most 30 characters, never curie: the chart "
            "names cluster-scoped objects from the release)"
        )
    elif namespace and len(f"{namespace}-{release}-publication") > 63:
        problems.append(
            f"{NAMESPACE_ENV} and {RELEASE_ENV} too long: <namespace>-<release>-publication "
            "must fit 63 characters"
        )
    cli = env.get(CLI_ENV, "").strip()
    if not cli or not os.path.isabs(cli) or not os.access(cli, os.X_OK):
        problems.append(f"{CLI_ENV} (an absolute path to the compiled candidate curie)")
    template = env.get(IMAGES_ENV, "").strip()
    images: dict[str, str] = {}
    if "{component}" not in template:
        problems.append(f"{IMAGES_ENV} (an image reference template containing {{component}})")
    else:
        images = {component: template.format(component=component) for component in COMPONENTS}
        for component, ref in images.items():
            try:
                image_values(ref, "IfNotPresent")
            except Refused as exc:
                problems.append(f"{IMAGES_ENV} for {component}: {exc}")
    pull_policy = env.get(PULL_POLICY_ENV, "").strip()
    if pull_policy not in PULL_POLICIES:
        problems.append(f"{PULL_POLICY_ENV} (one of {', '.join(PULL_POLICIES)})")
    if problems:
        raise Refused(
            f"{FLAG}=1 needs every caller input; missing or malformed: " + "; ".join(problems)
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


def quantity_bytes(raw: Any) -> int:
    """A Kubernetes memory or storage quantity in bytes. Pure."""

    match = _QUANTITY.match(str(raw).strip())
    if match is None:
        raise ValueError(f"unsupported quantity {raw!r}")
    return int(match.group(1)) * _SCALE[match.group(2)]


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


# --------------------------------------------------------------------------
# The two scenarios
# --------------------------------------------------------------------------


def _resources(
    *, memory_request: str, memory_limit: str, storage_request: str, storage_limit: str
) -> dict[str, Any]:
    return {
        "requests": {
            "cpu": "100m",
            "memory": memory_request,
            "ephemeral-storage": storage_request,
        },
        "limits": {"cpu": "1", "memory": memory_limit, "ephemeral-storage": storage_limit},
    }


@dataclass(frozen=True)
class Phase:
    agent: str
    label: str
    directive: str
    cause: str
    condition: str
    dimension: str
    delivered_key: str
    low: dict[str, Any]
    high: dict[str, Any]
    low_bytes: int
    high_bytes: int
    # How long the failing attempt may take to end, kubelet cadence included.
    failure_timeout: float


MEMORY = Phase(
    agent="rr-memory",
    label="rr-memory-work",
    directive="[fake:hold-memory 768]",
    cause="sandbox_memory_exhausted",
    condition="memory_limit",
    dimension="memory",
    delivered_key="memory_bytes",
    low=_resources(
        memory_request="128Mi", memory_limit="448Mi", storage_request="64Mi", storage_limit="512Mi"
    ),
    high=_resources(
        memory_request="128Mi", memory_limit="1408Mi", storage_request="64Mi", storage_limit="512Mi"
    ),
    low_bytes=448 * 1024**2,
    high_bytes=1408 * 1024**2,
    failure_timeout=900,
)
STORAGE = Phase(
    agent="rr-storage",
    label="rr-storage-work",
    directive="[fake:fill-scratch 384]",
    cause="sandbox_storage_exhausted",
    condition="ephemeral_storage_limit",
    dimension="ephemeral_storage",
    delivered_key="ephemeral_storage_bytes",
    low=_resources(
        memory_request="128Mi", memory_limit="768Mi", storage_request="64Mi", storage_limit="256Mi"
    ),
    high=_resources(
        memory_request="128Mi", memory_limit="768Mi", storage_request="64Mi", storage_limit="896Mi"
    ),
    low_bytes=256 * 1024**2,
    high_bytes=896 * 1024**2,
    failure_timeout=1200,
)
PHASES = (MEMORY, STORAGE)


def runner_limit(pod: dict[str, Any], dimension: str) -> int:
    """The limit the kubelet enforces for ``dimension`` on this pod's spec. Pure.

    Memory is the runner container's own limit. Ephemeral storage is the sum of
    the regular containers' limits, the quantity a pod-level eviction compares.
    """

    containers = (pod.get("spec") or {}).get("containers") or []
    if dimension == "memory":
        runner = next((c for c in containers if c.get("name") == "runner"), None)
        if runner is None:
            raise ValueError("the sandbox pod has no runner container")
        return quantity_bytes(runner["resources"]["limits"]["memory"])
    return sum(quantity_bytes(c["resources"]["limits"]["ephemeral-storage"]) for c in containers)


def exhaustion_seen(pods: list[dict[str, Any]], dimension: str) -> bool:
    """Whether any observed state of the pod shows the kernel or kubelet ending it. Pure."""

    for pod in pods:
        status = pod.get("status") or {}
        if dimension == "memory":
            for container in status.get("containerStatuses") or []:
                if container.get("name") != "runner":
                    continue
                for key in ("state", "lastState"):
                    terminated = (container.get(key) or {}).get("terminated") or {}
                    if terminated.get("reason") == "OOMKilled":
                        return True
        elif status.get("reason") == "Evicted" and "ephemeral local storage" in str(
            status.get("message") or ""
        ):
            return True
    return False


def pod_digests(pods: list[dict[str, Any]], container: str) -> set[str]:
    found: set[str] = set()
    for pod in pods:
        for status in (pod.get("status") or {}).get("containerStatuses") or []:
            if status.get("name") == container and status.get("imageID"):
                found.update(_DIGEST.findall(str(status["imageID"])))
    return found


def _brief(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    keys = (
        "id",
        "sequence",
        "status",
        "terminal_cause",
        "retry_safety",
        "blocked_on",
        "start_deferrals",
        "last_deferral_reason",
        "last_start_failure_detail",
        "termination_observation",
        "started_at",
        "runtime_claim_name",
        "delivered_limits",
        "resource_gate",
        "retry_of_request_id",
        "dispatch_not_before",
        "version",
    )
    return {key: row.get(key) for key in keys}


# --------------------------------------------------------------------------
# The live harness
# --------------------------------------------------------------------------


@dataclass
class Live:
    inputs: Inputs
    workdir: Path
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    teardown: fe.Teardown = field(default_factory=fe.Teardown)
    digests: dict[str, frozenset[str]] = field(default_factory=dict)
    rendered: list[tuple[str, str]] = field(default_factory=list)
    cluster_resources: dict[tuple[str, str], str] = field(default_factory=dict)
    api_url: str = ""
    api_key: str = field(default="", repr=False)
    forward: subprocess.Popen[Any] | None = None
    watch: subprocess.Popen[Any] | None = None
    watch_file: Path | None = None
    agents: dict[str, str] = field(default_factory=dict)
    stub_ip: str = ""
    kube_version: str = ""
    app_key: Path | None = None

    # --- plumbing -----------------------------------------------------

    def log(self, message: str) -> None:
        fe.log(f"resource-retry [{self.run_id}]: {message}")

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
        return json.loads(out.strip().splitlines()[-1])

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
            raise Refused(f"{CLI_ENV}: {exc}") from None
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
        self.log(f"release renders {len(self.rendered)} cluster-scoped objects")

    # --- teardown ---------------------------------------------------------

    def release_owned(self, item: dict[str, Any]) -> bool:
        annotations = item.get("metadata", {}).get("annotations") or {}
        return (
            annotations.get(HELM_RELEASE_NAME) == self.inputs.release
            and annotations.get(HELM_RELEASE_NAMESPACE) == self.inputs.namespace
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

        deadline = time.monotonic() + 900
        left = remaining()
        while left and time.monotonic() < deadline:
            time.sleep(5)
            left = remaining()
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

    def start_stub(self) -> None:
        image = self.inputs.images["worker"]
        manifests = poe.code_host_manifests(
            tch.stub_manifests(self.inputs.namespace, image, STUB_PORT),
            image=image,
            port=STUB_PORT,
            seed_files=SEED_FILES,
            issue_body=SEED_ISSUE,
            boot=STUB_BOOT,
            labels={},
        )
        for manifest in manifests:
            if manifest["kind"] == "Pod":
                manifest["spec"]["containers"][0]["imagePullPolicy"] = self.inputs.pull_policy
        listing = {"apiVersion": "v1", "kind": "List", "items": manifests}
        self.kubectl("create", "-f", "-", input_text=json.dumps(listing))
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

    def require_app_key(self) -> Path:
        if self.app_key is None:
            raise AssertionError("the App key is created with the stub, before the values")
        return self.app_key

    def values(self) -> dict[str, Any]:
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
                "fakeModel": True,
            }
        )
        agents = [phase.agent for phase in PHASES]
        sandbox["runnerEnv"] = {phase.agent: {DIRECTIVE_ENV: phase.directive} for phase in PHASES}
        sandbox["agentResourceProfiles"] = dict.fromkeys(agents, "standard")
        factory = values["factory"]
        factory["sandboxEgress"] = dict.fromkeys(agents, True)
        factory["baseLabelSettleSeconds"] = 0
        for tracker in factory["trackers"]:
            tracker["pollIntervalSeconds"] = 2
        values["langfuse"] = {"deploy": False, "host": "langfuse.example.com"}
        values["clickhouse"] = {"deploy": False}
        values["otelCollector"] = {"deploy": False, "telemetryDisabled": True}
        values["priorityClasses"] = fe.priority_class_values(None, self.inputs.release)
        api_env = list(values["api"].get("extraEnv") or [])
        api_env.append(
            {"name": "WORK_ITEM_RESOURCE_GATE_PROBE_SECONDS", "value": str(PROBE_SECONDS)}
        )
        values["api"]["extraEnv"] = api_env
        return values

    def install(self) -> None:
        values_file = self.private_file("values.json", json.dumps(self.values()))
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
                self.kubectl(
                    "-n", self.inputs.namespace, "rollout", "status", workload, "--timeout=15m"
                )
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
        if self.forward is None:
            self.teardown.push(
                "stop the API port-forward", lambda: {"stopped": self.stop_forward()}
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

    def deploy_agents(self) -> None:
        config_dir = self.workdir / "curie-config"
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
        for phase in PHASES:
            agent = self.api(
                "POST",
                "/agents",
                {
                    "name": phase.agent,
                    "repo_full_name": tch.STUB_REPO,
                    "channel": {
                        "kind": "github",
                        "address": tch.STUB_REPO,
                        "trigger_label": phase.label,
                    },
                },
                expected=201,
            )
            self.agents[phase.agent] = str(agent["id"])
            self.set_resources(phase, phase.low)
            bundle = self.workdir / f"bundle-{phase.agent}"
            (bundle / ".claude-plugin").mkdir(parents=True)
            (bundle / ".claude-plugin" / "plugin.json").write_text(
                json.dumps(
                    {
                        "name": phase.agent,
                        "version": "0.1.0",
                        "description": "Resource retry proof agent.",
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
                    phase.agent,
                    "--env",
                    "prod",
                    "--chart",
                    str(CHART),
                    *common,
                ],
                ["publication-policy", phase.agent, "--policy", "auto", *common],
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
            self.log(f"agent {phase.agent} deployed with {phase.dimension} limit {phase.low_bytes}")

    def set_resources(self, phase: Phase, resources: dict[str, Any]) -> None:
        self.api(
            "PATCH",
            f"/agents/{self.agents[phase.agent]}",
            {"runner_resources": copy.deepcopy(resources)},
        )

    def seed_issue(self, phase: Phase) -> int:
        out = self.stub_exec(
            "python3",
            "-c",
            SEED_ISSUE_PROGRAM,
            f"Resource retry proof: {phase.dimension}",
            SEED_ISSUE,
            phase.label,
        )
        return int(json.loads(out.strip().splitlines()[-1])["number"])

    def requests(self, phase: Phase, issue: int) -> list[dict[str, Any]]:
        agent = uuid.UUID(self.agents[phase.agent])
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
    deadline = time.monotonic() + timeout
    while True:
        value = probe()
        if value:
            return value
        if time.monotonic() > deadline:
            raise AssertionError(
                f"timed out after {int(timeout)}s waiting for {what}; observed {observed()}"
            )
        time.sleep(3)


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------


@pytest.fixture
def inputs() -> Inputs:
    try:
        return read_inputs(dict(os.environ))
    except Refused as exc:
        refusal = str(exc)
    pytest.fail(refusal, pytrace=False)


@pytest.fixture(autouse=True)
def candidate_curie(inputs: Inputs, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The real compiled candidate CLI, in place of conftest's fake CURIE_BIN."""

    monkeypatch.setenv("CURIE_BIN", inputs.cli)
    return Path(inputs.cli)


@pytest.fixture
def live(inputs: Inputs) -> Iterator[Live]:
    workdir = Path(tempfile.mkdtemp(prefix="resource-retry-"))
    workdir.chmod(0o700)
    harness = Live(inputs=inputs, workdir=workdir)
    harness.teardown.push("private workdir", lambda: shutil.rmtree(workdir, ignore_errors=True))
    try:
        try:
            harness.preflight()
        except Refused as exc:
            refusal: str | None = f"refused: {exc}"
        else:
            refusal = None
        if refusal is not None:
            pytest.fail(refusal, pytrace=False)
        yield harness
    finally:
        results = harness.teardown.run()
        failed = [f"{r['step']}: {r['detail']}" for r in results if not r["ok"]]
        for result in results:
            harness.log(f"teardown {result['step']}: {'ok' if result['ok'] else 'FAILED'}")
        if failed:
            pytest.fail("teardown did not prove every owned resource absent: " + "; ".join(failed))


# --------------------------------------------------------------------------
# The proof
# --------------------------------------------------------------------------


def prove_phase(live: Live, phase: Phase) -> None:
    issue = live.seed_issue(phase)
    live.log(f"{phase.dimension}: issue {issue} labelled {phase.label}")

    def rows() -> list[dict[str, Any]]:
        return live.requests(phase, issue)

    def brief() -> list[dict[str, Any] | None]:
        return [_brief(row) for row in rows()]

    first = wait_for("the labelled issue to be admitted", 300, lambda: rows()[:1], brief)[0]
    first = wait_for(
        f"the first {phase.dimension} attempt to end",
        phase.failure_timeout,
        lambda: next(
            (r for r in rows() if r["id"] == first["id"] and r["status"] in TERMINAL), None
        ),
        brief,
    )
    assert first["terminal_cause"] == phase.cause and first["retry_safety"] == "no_effects", (
        f"the first attempt ended {first['terminal_cause']!r} with safety "
        f"{first['retry_safety']!r}, expected {phase.cause!r} and 'no_effects': {_brief(first)}"
    )
    claim = first.get("runtime_claim_name")
    assert claim, f"the failed attempt recorded no sandbox claim: {_brief(first)}"
    failed_pods = live.pods_for(str(claim))
    assert failed_pods, f"no pod of claim {claim} was observed"
    assert exhaustion_seen(failed_pods, phase.dimension), (
        f"no observed state of claim {claim}'s pod shows the {phase.dimension} exhaustion: "
        f"{[(p.get('status') or {}).get('reason') for p in failed_pods]}"
    )
    spec_limit = runner_limit(failed_pods[0], phase.dimension)
    delivered = (first.get("delivered_limits") or {}).get(phase.delivered_key)
    assert delivered == spec_limit == phase.low_bytes, (
        f"the failed attempt delivered {phase.delivered_key}={delivered}, its pod spec enforced "
        f"{spec_limit}, the override set {phase.low_bytes}"
    )

    def successors() -> list[dict[str, Any]]:
        return [r for r in rows() if r.get("retry_of_request_id") == first["id"]]

    found = wait_for("the gated successor", 300, successors, brief)
    assert len(found) == 1, f"expected one successor, observed {[_brief(r) for r in found]}"
    successor = found[0]
    gate = successor.get("resource_gate") or {}
    assert (
        successor["status"] == "waiting"
        and successor["blocked_on"] == phase.condition
        and gate.get("condition") == phase.condition
        and gate.get("dimension") == phase.dimension
        and gate.get("failed_limit") == delivered
        and gate.get("source_request_id") == first["id"]
    ), f"the successor is not gated on the failed {phase.dimension} limit: {_brief(successor)}"

    # One probe period and more: the successor stays waiting, uncounted, sandbox-free.
    claims_before = live.sandbox_claims()
    pod_claims_before = live.sandbox_pod_claims()
    deferrals = successor["start_deferrals"]
    probed = False
    versions = {successor["version"]}
    window_end = time.monotonic() + GATE_WINDOW_SECONDS
    while time.monotonic() < window_end:
        time.sleep(3)
        current = next(r for r in rows() if r["id"] == successor["id"])
        assert (
            current["status"] == "waiting"
            and current["blocked_on"] == phase.condition
            and current["start_deferrals"] == deferrals
            and not current.get("runtime_claim_name")
            and not current.get("started_at")
        ), f"the gated successor moved under the unchanged limit: {_brief(current)}"
        if current.get("last_deferral_reason") == "resource_gate_unmet":
            versions.add(current["version"])
            probed = probed or len(versions) > 1
    assert probed, (
        f"no resource_gate_unmet probe was observed within {GATE_WINDOW_SECONDS}s: "
        f"{_brief(next(r for r in rows() if r['id'] == successor['id']))}"
    )
    new_claims = live.sandbox_claims() - claims_before
    new_pods = live.sandbox_pod_claims() - pod_claims_before
    assert not new_claims and not new_pods, (
        f"the gated wait was not sandbox-free: new claims {sorted(new_claims)}, "
        f"new sandbox pods for claims {sorted(new_pods)}"
    )
    assert len(rows()) == 2, f"a request beyond the one successor appeared: {brief()}"
    live.log(f"{phase.dimension}: successor held sandbox-free; raising the limit")

    live.set_resources(phase, phase.high)
    started = wait_for(
        "the successor to start under the raised limit",
        10 * PROBE_SECONDS + 600,
        lambda: next(
            (r for r in rows() if r["id"] == successor["id"] and r.get("started_at")), None
        ),
        brief,
    )
    raised = (started.get("delivered_limits") or {}).get(phase.delivered_key)
    assert started.get("blocked_on") is None and raised == phase.high_bytes, (
        f"the successor started with {phase.delivered_key}={raised} and blocked_on "
        f"{started.get('blocked_on')!r}, expected {phase.high_bytes} and none: {_brief(started)}"
    )
    new_claim = started.get("runtime_claim_name")
    assert new_claim and new_claim != claim, (
        f"the successor names no fresh claim: {_brief(started)}"
    )
    new_pods_seen = wait_for(
        f"a pod of claim {new_claim}", 300, lambda: live.pods_for(str(new_claim)), brief
    )
    assert runner_limit(new_pods_seen[-1], phase.dimension) == phase.high_bytes, (
        f"the successor's pod enforces {runner_limit(new_pods_seen[-1], phase.dimension)}, "
        f"not the raised {phase.high_bytes}"
    )
    runner_digests = wait_for(
        "the successor runner's image id",
        300,
        lambda: pod_digests(live.pods_for(str(new_claim)), "runner"),
        brief,
    )
    assert runner_digests <= live.digests["runner"], (
        f"the successor runner runs {sorted(runner_digests)}, not the candidate runner digests"
    )

    ended = wait_for(
        "the successor to end",
        1800,
        lambda: next(
            (r for r in rows() if r["id"] == successor["id"] and r["status"] in TERMINAL), None
        ),
        brief,
    )
    live.log(f"{phase.dimension}: successor ended {ended['status']} ({ended['terminal_cause']})")
    time.sleep(SETTLE_SECONDS)
    final = rows()
    assert len(final) == 2, (
        f"exactly one fresh attempt was expected; the work item holds {len(final)} requests: "
        f"{[_brief(r) for r in final]}"
    )
    assert ended.get("terminal_cause") != phase.cause, (
        f"the successor exhausted {phase.dimension} again under the raised limit: {_brief(ended)}"
    )


def test_resource_exhaustion_waits_for_a_larger_delivered_limit(
    live: Live, candidate_curie: Path
) -> None:
    assert os.environ["CURIE_BIN"] == str(candidate_curie)
    live.create_namespace()
    live.start_stub()
    live.install()
    live.port_forward()
    live.start_pod_watch()
    live.deploy_agents()
    for phase in PHASES:
        prove_phase(live, phase)
