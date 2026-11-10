"""One binding per resolved tracker scope, refused before rollout (#4622, ADR 0197).

Gauntlet finding C4-B1: on v0.13.0 an operator who followed the Bitbucket
manual-values example added a second binding for the Jira site quickstart had
already bound. The chart rendered it, the CLI applied it, and the API refused
the whole forge document at boot and crash-looped while Helm waited.

`tests/vectors/forge-tracker-scopes.json` holds the shared cases, read lazily
inside fixtures and tests. The fix pin carries its own inline input (the
baseline it installs and the duplicate it applies), so it reaches its product
assertions when the vector is absent; `test_pin_inputs_match_the_vector` keeps
that input identical to the vector's cases. This module drives the real
surfaces:

1. `helm template` of charts/curie, the renderer direct Helm users run.
2. `curie cluster upgrade --values-file`, built from this checkout, against a
   baseline release installed on a kind cluster this module creates.
3. The candidate API image, built from this checkout, booting each rendered
   CURIE_FORGES document against Postgres, Valkey and RustFS containers this
   module owns, plus `load_forges_json` in process.

Every cluster, namespace, release, container, network and image tag is created
by a fixture that registers its cleanup first and verifies the resource is gone
afterwards. A missing prerequisite or an infrastructure failure is a fixture
error, never a skip. Infrastructure helpers raise `InfrastructureError` (a
RuntimeError naming the command and its stderr), never AssertionError, so a
failed probe cannot pass for a behavioral failure. The Docker engine is
whatever the environment configures.
"""

from __future__ import annotations

import copy
import functools
import io
import json
import os
import secrets
import shutil
import subprocess
import tarfile
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import yaml
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from curie_api.forges.loader import load_forges_json

REPO = Path(__file__).resolve().parents[3]
CHART = REPO / "charts" / "curie"
VECTOR_PATH = REPO / "tests" / "vectors" / "forge-tracker-scopes.json"
# The vector's case names, listed here so collection never reads the vector;
# `test_vector_case_names_are_listed` keeps the list exact.
VECTOR_CASE_NAMES = (
    "quickstart_jira_binding",
    "valid_mixed_repositories",
    "duplicate_scope_through_distinct_tracker_names",
    "duplicate_scope_same_tracker",
    "unknown_tracker_reference",
    "distinct_scopes",
    "duplicate_tracker_names",
    "duplicate_binding_names",
    "duplicate_numeric_scope",
    "duplicate_scope_factory_disabled",
    "case_distinct_scope_ids",
    "same_scope_id_different_tracker_kinds",
    "scope_id_null",
    "scope_id_list",
    "scope_id_out_of_range",
)
CHART_VERSION: str = yaml.safe_load((CHART / "Chart.yaml").read_text())["version"]

KIND_NODE_IMAGE = "kindest/node:v1.31.0"
RENDER_KUBE_VERSION = "1.31.0"
# The configuration schema `curie cluster up` stamps on every install
# (cli/src/config_migrate.rs TARGET_SCHEMA_VERSION); cluster upgrade refuses
# a release without a supported one before it reads anything else.
INSTALLED_SCHEMA_VERSION = "0.9.0"
# Images as compose.dev.yaml pins them.
POSTGRES_IMAGE = (
    "postgres:16.15-alpine@sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685"
)
VALKEY_IMAGE = "valkey/valkey:8.1.10-alpine"
RUSTFS_IMAGE = "rustfs/rustfs:1.0.0-beta.12"

LABEL = "curie-test=c4-tracker-scope"
TOKEN = "fixture-token-not-a-secret"
UPGRADE_TIMEOUT_S = 900
API_BOOT_TIMEOUT_S = 180
REQUIRED_EXECUTABLES = ("docker", "kind", "kubectl", "helm", "cargo")


class InfrastructureError(RuntimeError):
    """A probe, setup or teardown step failed: not a behavior under test."""


def _infra(condition: object, message: str) -> None:
    if not condition:
        raise InfrastructureError(message)


def _run(
    argv: list[str],
    *,
    timeout: float,
    env: dict[str, str] | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        argv,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
        check=False,
    )
    if check and result.returncode != 0:
        raise InfrastructureError(
            f"{' '.join(argv)} exited {result.returncode}:\n"
            f"{result.stdout[-4000:]}\n{result.stderr[-4000:]}"
        )
    return result


# ---------------------------------------------------------------------------
# Inputs: the shared vector, read lazily, and the fix pin's own input
# ---------------------------------------------------------------------------


@functools.cache
def _vector() -> dict[str, Any]:
    vector: dict[str, Any] = json.loads(VECTOR_PATH.read_text())
    return vector


def _case(name: str) -> dict[str, Any]:
    (case,) = [case for case in _vector()["cases"] if case["name"] == name]
    found: dict[str, Any] = case
    return found


def _mount() -> str:
    return str(_vector()["credential_mount"])


def _egress(cidr: str, host: str) -> list[dict[str, Any]]:
    return [{"cidr": cidr, "ports": [{"protocol": "TCP", "port": 443}], "host": host}]


_ZERO_ID = "00000000-0000-0000-0000-000000000000"
_ALLOWLIST = {"type": "allowlist", "accounts": [f"557058:{_ZERO_ID}"]}


def _pin_tracker(name: str, host: str) -> dict[str, Any]:
    return {
        "egress": _egress("192.0.2.10/32", "example.atlassian.net"),
        "name": name,
        "kind": "jira_cloud",
        "host": host,
        "apiUrl": "https://example.atlassian.net",
        "label": "curie-factory",
        "mention": f"712020:{_ZERO_ID}",
        "credential": {
            "type": "static_token",
            "username": "factory-bot@example.com",
            "existingSecret": f"curie-{name}-token",
            "existingSecretKey": "token",
        },
    }


def _pin_binding(name: str, tracker: str, alias: str, project: str) -> dict[str, Any]:
    return {
        "name": name,
        "tracker": tracker,
        "trackerScopeId": _ZERO_ID,
        "repos": [
            {"alias": alias, "codeHost": "bitbucket", "projectId": project, "path": f"acme/{alias}"}
        ],
        "defaultRepo": alias,
        "startAuthority": copy.deepcopy(_ALLOWLIST),
    }


# The quickstart Jira binding every refusal test installs as its baseline.
PIN_BASELINE: dict[str, Any] = {
    "codeHosts": [
        {
            "egress": _egress("192.0.2.20/32", "api.bitbucket.org"),
            "name": "bitbucket",
            "kind": "bitbucket_cloud",
            "host": "bitbucket.org",
            "apiUrl": "https://api.bitbucket.org/2.0",
            "credential": {
                "type": "static_token",
                "existingSecret": "curie-bitbucket-token",
                "existingSecretKey": "token",
            },
        }
    ],
    "trackers": [_pin_tracker("jira", "example.atlassian.net")],
    "bindings": [
        _pin_binding("jira", "jira", "example-repo", "{00000000-0000-4000-8000-000000000001}")
    ],
}
# The released Bitbucket manual-values shape over the baseline: a second
# tracker entry for the same Jira site, its host in other case with
# surrounding whitespace, and a second binding with the same trackerScopeId.
PIN_DUPLICATE: dict[str, Any] = {
    "codeHosts": copy.deepcopy(PIN_BASELINE["codeHosts"]),
    "trackers": [
        *copy.deepcopy(PIN_BASELINE["trackers"]),
        _pin_tracker("jira-site", " Example.Atlassian.NET "),
    ],
    "bindings": [
        *copy.deepcopy(PIN_BASELINE["bindings"]),
        _pin_binding(
            "example", "jira-site", "other-repo", "{00000000-0000-4000-8000-000000000002}"
        ),
    ],
}
PIN_OUTCOME: dict[str, Any] = {
    "refused": "duplicate_scope",
    "bindings": ["jira", "example"],
    "scope": {"kind": "jira_cloud", "host": "example.atlassian.net", "trackerScopeId": _ZERO_ID},
}


def _run_id() -> str:
    return f"c4ts-{os.getpid() % 100000}-{secrets.token_hex(3)}"


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _helm_template(values: dict[str, Any], workdir: Path) -> subprocess.CompletedProcess[str]:
    """`helm template` of the candidate chart with one values file."""

    values_file = workdir / f"render-{secrets.token_hex(4)}.json"
    values_file.write_text(json.dumps(values))
    return _run(
        [
            "helm",
            "template",
            "c4-tracker-scope",
            str(CHART),
            "--namespace",
            "c4-tracker-scope",
            "--kube-version",
            RENDER_KUBE_VERSION,
            "-f",
            str(values_file),
        ],
        timeout=120,
        check=False,
    )


def _rendered_forges(manifest: str) -> dict[str, Any]:
    """The CURIE_FORGES document on the rendered api Deployment."""

    found = []
    for document in yaml.safe_load_all(manifest):
        if not isinstance(document, dict) or document.get("kind") != "Deployment":
            continue
        if document["metadata"].get("labels", {}).get("app.kubernetes.io/component") != "api":
            continue
        for container in document["spec"]["template"]["spec"]["containers"]:
            for env in container.get("env", []):
                if env["name"] == "CURIE_FORGES":
                    found.append(json.loads(env["value"]))
    assert len(found) == 1, f"expected one api CURIE_FORGES, found {len(found)}"
    forges: dict[str, Any] = found[0]
    return forges


def _render_accepted(name: str, workdir: Path) -> dict[str, Any]:
    rendered = _helm_template({"factory": _case(name)["values"]}, workdir)
    assert rendered.returncode == 0, f"{name}: helm template refused:\n{rendered.stderr}"
    return _rendered_forges(rendered.stdout)


def _assert_names_refusal(text: str, outcome: dict[str, Any], *, where: str) -> None:
    """The refusal names what the operator must fix; wording is not pinned."""

    if outcome["refused"] == "duplicate_scope":
        scope = outcome["scope"]
        needles = [*outcome["bindings"], scope["kind"], scope["host"], scope["trackerScopeId"]]
    elif outcome["refused"] == "duplicate_name":
        needles = [f"factory.{outcome['section']}", outcome["name"]]
    elif outcome["refused"] == "invalid_scope_id":
        needles = [f"({outcome['binding']}) trackerScopeId must be"]
    else:
        assert outcome["refused"] == "unknown_tracker", outcome
        needles = [outcome["binding"], outcome["tracker"]]
    missing = [needle for needle in needles if needle not in text]
    assert not missing, f"{where} refusal does not name {missing}:\n{text[-4000:]}"


# ---------------------------------------------------------------------------
# Credentials the chart mounts
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def private_key() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


def _credential_files(forges: dict[str, Any], private_key: str) -> dict[str, str]:
    """Every credential file the rendered document names, with fixture content."""

    mount = _mount()
    files: dict[str, str] = {}
    for section in ("code_hosts", "trackers"):
        for entry in forges[section]:
            for field, path in entry["credential"].items():
                if field.endswith("_file"):
                    assert path.startswith(f"{mount}/"), f"{path} is outside the chart mount"
                    files[path] = private_key if field == "private_key_file" else TOKEN
    assert files, "the rendered document names no credential file"
    return files


def _load_in_process(forges: dict[str, Any], root: Path, private_key: str) -> Any:
    """`load_forges_json` on the document, its mount prefix moved under ``root``."""

    for path, content in _credential_files(forges, private_key).items():
        target = root / path.removeprefix(f"{_mount()}/")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
    return load_forges_json(json.dumps(forges).replace(_mount(), str(root)))


def _credential_tar(forges: dict[str, Any], private_key: str) -> bytes:
    """A tar of the credential files at their rendered paths, readable by the
    API image's uid 1000, for `docker cp - <container>:/`."""

    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        directories: set[str] = set()
        for path, content in sorted(_credential_files(forges, private_key).items()):
            parts = path.strip("/").split("/")
            # Never write entries for the image's own /etc or /etc/curie.
            for depth in range(3, len(parts)):
                directory = "/".join(parts[:depth])
                if directory in directories:
                    continue
                directories.add(directory)
                info = tarfile.TarInfo(directory)
                info.type = tarfile.DIRTYPE
                info.mode, info.uid, info.gid = 0o550, 1000, 1000
                archive.addfile(info)
            data = content.encode()
            info = tarfile.TarInfo("/".join(parts))
            info.size, info.mode, info.uid, info.gid = len(data), 0o440, 1000, 1000
            archive.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


# ---------------------------------------------------------------------------
# Infrastructure fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def prerequisites() -> None:
    missing = [tool for tool in REQUIRED_EXECUTABLES if shutil.which(tool) is None]
    if missing:
        pytest.fail(f"required executables are unavailable: {', '.join(missing)}")
    _run(["docker", "version", "--format", "{{.Server.Version}}"], timeout=60)


@dataclass(frozen=True)
class KindCluster:
    name: str
    kubeconfig: Path

    @property
    def context(self) -> str:
        return f"kind-{self.name}"

    def env(self) -> dict[str, str]:
        return {**os.environ, "KUBECONFIG": str(self.kubeconfig), "HELM_KUBECONTEXT": self.context}

    def kubectl(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return _run(
            ["kubectl", "--context", self.context, *args], env=self.env(), timeout=180, check=check
        )

    def helm(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return _run(
            ["helm", "--kube-context", self.context, *args],
            env=self.env(),
            timeout=300,
            check=check,
        )


def _kind_clusters() -> list[str]:
    return _run(["kind", "get", "clusters"], timeout=60).stdout.split()


def _kind_containers(name: str) -> list[str]:
    return _run(
        ["docker", "ps", "-aq", "--filter", f"label=io.x-k8s.kind.cluster={name}"], timeout=60
    ).stdout.split()


@pytest.fixture(scope="module")
def cluster(
    prerequisites: None,
    request: pytest.FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
) -> KindCluster:
    """A kind cluster owned by this module, with a private 0600 kubeconfig."""

    owned = KindCluster(_run_id(), tmp_path_factory.mktemp("kind") / "kubeconfig")
    _infra(owned.name not in _kind_clusters(), "kind cluster name collision")

    def delete() -> None:
        _run(
            [
                "kind",
                "delete",
                "cluster",
                "--name",
                owned.name,
                "--kubeconfig",
                str(owned.kubeconfig),
            ],
            timeout=300,
        )
        _infra(owned.name not in _kind_clusters(), f"kind cluster {owned.name} survived teardown")
        _infra(not _kind_containers(owned.name), f"kind node containers of {owned.name} survived")

    request.addfinalizer(delete)
    owned.kubeconfig.touch(mode=0o600)
    _run(
        [
            "kind",
            "create",
            "cluster",
            "--name",
            owned.name,
            "--image",
            KIND_NODE_IMAGE,
            "--kubeconfig",
            str(owned.kubeconfig),
            "--wait",
            "180s",
        ],
        timeout=600,
    )
    owned.kubeconfig.chmod(0o600)
    configuration = yaml.safe_load(owned.kubeconfig.read_text())
    _infra(
        configuration["current-context"] == owned.context
        and [entry["name"] for entry in configuration["contexts"]] == [owned.context],
        f"kind wrote an unexpected kubeconfig for {owned.name}",
    )
    _infra(
        _poll(lambda: owned.kubectl("get", "--raw", "/readyz", check=False).returncode == 0, 120),
        "the kind API server never became reachable",
    )
    # No release workload schedules, so the test pulls no images into the
    # cluster: the release's recorded objects are the state under test.
    owned.kubectl("cordon", f"{owned.name}-control-plane")
    return owned


@pytest.fixture(scope="module")
def candidate_cli(prerequisites: None) -> Path:
    """The curie CLI built from this checkout (not the conftest stub CURIE_BIN)."""

    target = Path(os.environ.get("CARGO_TARGET_DIR") or REPO / "cli" / "target")
    _run(
        ["cargo", "build", "--locked", "--manifest-path", str(REPO / "cli" / "Cargo.toml")],
        env={**os.environ, "CARGO_TARGET_DIR": str(target)},
        timeout=3600,
    )
    binary = target / "debug" / "curie"
    _infra(binary.is_file(), f"cargo build produced no {binary}")
    return binary


@dataclass(frozen=True)
class ReleaseState:
    history: list[tuple[int, str, str]]
    values: dict[str, Any]
    generations: dict[str, int]


@dataclass(frozen=True)
class BaselineRelease:
    cluster: KindCluster
    name: str
    namespace: str

    def state(self) -> ReleaseState:
        history = json.loads(
            self.cluster.helm("history", self.name, "-n", self.namespace, "-o", "json").stdout
        )
        values = json.loads(
            self.cluster.helm("get", "values", self.name, "-n", self.namespace, "-o", "json").stdout
        )
        deployments = json.loads(
            self.cluster.kubectl("get", "deployments", "-n", self.namespace, "-o", "json").stdout
        )["items"]
        return ReleaseState(
            history=[(row["revision"], row["status"], row["chart"]) for row in history],
            values=values,
            generations={
                item["metadata"]["name"]: item["metadata"]["generation"] for item in deployments
            },
        )

    def upgrade(
        self, cli: Path, values: dict[str, Any], workdir: Path
    ) -> subprocess.CompletedProcess[str]:
        """`curie cluster upgrade --values-file`, the documented manual-values path."""

        overlay = workdir / "overlay.yaml"
        overlay.write_text(yaml.safe_dump(values, sort_keys=False))
        config_dir = workdir / "curie-config"
        config_dir.mkdir(exist_ok=True)
        return _run(
            [
                str(cli),
                "cluster",
                "upgrade",
                "--namespace",
                self.namespace,
                "--release",
                self.name,
                "--chart",
                str(CHART),
                "--to",
                CHART_VERSION,
                "--values-file",
                str(overlay),
                "--yes",
                "--context",
                self.cluster.context,
                "--color",
                "never",
            ],
            env={**self.cluster.env(), "CURIE_CONFIG_DIR": str(config_dir), "NO_COLOR": "1"},
            timeout=UPGRADE_TIMEOUT_S,
            check=False,
        )


@dataclass(frozen=True)
class Baseline:
    """The installed baseline release and its state, captured before the test
    body runs."""

    release: BaselineRelease
    before: ReleaseState


@pytest.fixture
def baseline_release(
    cluster: KindCluster, request: pytest.FixtureRequest, tmp_path: Path
) -> Baseline:
    """The quickstart Jira binding (`PIN_BASELINE`) installed as a release in a
    fresh namespace, with its Helm history, recorded values and Deployment
    generations captured.

    `--no-hooks` without `--wait`: the release, its recorded values and its
    Deployment objects exist, which is all `curie cluster upgrade` reads before
    it mutates anything.
    """

    name = _run_id()
    release = BaselineRelease(cluster, name, name)

    def remove() -> None:
        cluster.helm("uninstall", name, "-n", name, "--no-hooks", "--ignore-not-found", "--wait")
        cluster.kubectl(
            "delete", "namespace", name, "--ignore-not-found", "--wait=true", "--timeout=300s"
        )
        _infra(name not in cluster.helm("list", "-A", "-q").stdout.split(), "release survived")
        _infra(
            not cluster.kubectl(
                "get", "namespace", name, "--ignore-not-found", "-o", "name"
            ).stdout.strip(),
            "namespace survived teardown",
        )

    request.addfinalizer(remove)
    values_file = tmp_path / "baseline.json"
    values_file.write_text(
        json.dumps(
            {
                "config": {"schemaVersion": INSTALLED_SCHEMA_VERSION},
                "connectorCaller": {"existingSecret": f"{name}-connector-caller"},
                "factory": PIN_BASELINE,
            }
        )
    )
    cluster.helm(
        "install",
        name,
        str(CHART),
        "--namespace",
        name,
        "--create-namespace",
        "--no-hooks",
        "-f",
        str(values_file),
    )
    before = release.state()
    _infra([row[1] for row in before.history] == ["deployed"], f"baseline {before.history}")
    _infra(
        any(deployment.endswith("-api") for deployment in before.generations),
        f"baseline has no api Deployment: {before.generations}",
    )
    return Baseline(release, before)


# ---------------------------------------------------------------------------
# Refused before rollout
# ---------------------------------------------------------------------------


def _assert_refused_before_rollout(
    values: dict[str, Any],
    outcome: dict[str, Any],
    cli: Path,
    baseline: Baseline,
    workdir: Path,
) -> None:
    """Rendering refuses first; then the CLI refuses and the release is as the
    fixture captured it. Infrastructure reads come after the first behavioral
    assertion and raise InfrastructureError if they fail."""

    rendered = _helm_template({"factory": values}, workdir)
    assert rendered.returncode != 0, "helm template rendered the document"
    _assert_names_refusal(rendered.stderr, outcome, where="helm template")

    upgraded = baseline.release.upgrade(cli, {"factory": values}, workdir)
    assert upgraded.returncode != 0, "curie cluster upgrade accepted the document"
    _assert_names_refusal(upgraded.stdout + upgraded.stderr, outcome, where="cluster upgrade")

    after = baseline.release.state()
    assert after.history == baseline.before.history, "the refused upgrade added a Helm revision"
    assert after.generations == baseline.before.generations, (
        "the refused upgrade changed a Deployment"
    )
    assert after.values == baseline.before.values, "the refused upgrade changed the recorded values"


def test_duplicate_scope_is_rejected_before_rollout(
    candidate_cli: Path, baseline_release: Baseline, tmp_path: Path
) -> None:
    """FIX PIN. The released Bitbucket manual-values shape over the quickstart
    binding: a second tracker entry for the same Jira site and a second binding
    with its trackerScopeId. Its input is inline (`PIN_DUPLICATE`), so it does
    not read the shared vector."""

    _assert_refused_before_rollout(
        PIN_DUPLICATE, PIN_OUTCOME, candidate_cli, baseline_release, tmp_path
    )


def test_unknown_tracker_reference_is_refused_before_rollout(
    candidate_cli: Path, baseline_release: Baseline, tmp_path: Path
) -> None:
    case = _case("unknown_tracker_reference")
    _assert_refused_before_rollout(
        case["values"], case["outcome"], candidate_cli, baseline_release, tmp_path
    )


def test_vector_case_names_are_listed() -> None:
    assert tuple(case["name"] for case in _vector()["cases"]) == VECTOR_CASE_NAMES


def test_pin_inputs_match_the_vector() -> None:
    """The pin's inline input is the vector's case, byte for byte in meaning."""

    assert PIN_BASELINE == _case("quickstart_jira_binding")["values"]
    duplicate = _case("duplicate_scope_through_distinct_tracker_names")
    assert PIN_DUPLICATE == duplicate["values"]
    assert PIN_OUTCOME == duplicate["outcome"]


@pytest.mark.parametrize("name", VECTOR_CASE_NAMES)
def test_vector_cases_render_as_recorded(name: str, tmp_path: Path) -> None:
    case = _case(name)
    rendered = _helm_template({"factory": case["values"]}, tmp_path)
    if case["outcome"].get("accepted"):
        assert rendered.returncode == 0, f"helm template refused {name}:\n{rendered.stderr}"
        assert _rendered_forges(rendered.stdout) == case["forges"]
    else:
        assert rendered.returncode != 0, f"helm template rendered {name}"
        _assert_names_refusal(rendered.stderr, case["outcome"], where="helm template")


# ---------------------------------------------------------------------------
# The candidate API boots an accepted document and refuses a duplicate
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ApiStack:
    run: str
    image: str
    network: str
    env: dict[str, str]

    def boot(self, suffix: str, forges: dict[str, Any], private_key: str) -> str:
        """Create the API container, copy the credential files in, start it."""

        name = f"{self.run}-api-{suffix}"
        argv = ["docker", "create", "--name", name, "--network", self.network]
        argv += ["--label", LABEL, "--label", f"curie-test-run={self.run}"]
        for key, value in {**self.env, "CURIE_FORGES": json.dumps(forges)}.items():
            argv += ["-e", f"{key}={value}"]
        _run([*argv, self.image], timeout=120)
        copied = subprocess.run(
            ["docker", "cp", "-", f"{name}:/"],
            input=_credential_tar(forges, private_key),
            capture_output=True,
            timeout=120,
            check=False,
        )
        _infra(copied.returncode == 0, f"docker cp - {name}:/ failed: {copied.stderr.decode()}")
        _run(["docker", "start", name], timeout=120)
        return name


def _container_state(name: str) -> dict[str, Any]:
    state: dict[str, Any] = json.loads(
        _run(["docker", "inspect", "--format", "{{json .State}}", name], timeout=60).stdout
    )
    return state


def _logs(name: str) -> str:
    result = _run(["docker", "logs", name], timeout=60, check=False)
    return result.stdout + result.stderr


def _poll(predicate: Callable[[], bool], timeout: float) -> bool:
    """Whether ``predicate`` held within ``timeout``; the caller decides whether
    a timeout is an infrastructure error or a behavioral failure."""

    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            return False
        time.sleep(2)
    return True


def _wait_for_infrastructure(predicate: Callable[[], bool], what: str, timeout: float) -> None:
    _infra(_poll(predicate, timeout), f"timed out waiting for {what}")


def _owned(kind: str, run: str) -> list[str]:
    listing = {
        "container": ["docker", "ps", "-aq"],
        "network": ["docker", "network", "ls", "-q"],
    }[kind]
    return _run([*listing, "--filter", f"label=curie-test-run={run}"], timeout=60).stdout.split()


@pytest.fixture
def api_stack(prerequisites: None, request: pytest.FixtureRequest) -> Iterator[ApiStack]:
    """The candidate API image and owned Postgres, Valkey and RustFS containers
    on a private network, with migrations applied by the image's own migrator."""

    run = _run_id()
    image = f"curie-c4-tracker-scope-api:{run}"
    network = f"{run}-net"
    labels = ["--label", LABEL, "--label", f"curie-test-run={run}"]

    def remove() -> None:
        for container in _owned("container", run):
            _run(["docker", "rm", "-f", "-v", container], timeout=120)
        for owned_network in _owned("network", run):
            _run(["docker", "network", "rm", owned_network], timeout=120)
        _run(["docker", "image", "rm", image], timeout=120, check=False)
        _infra(not _owned("container", run), "owned containers survived teardown")
        _infra(not _owned("network", run), "owned network survived teardown")
        _infra(
            _run(["docker", "image", "inspect", image], timeout=60, check=False).returncode != 0,
            f"image tag {image} survived teardown",
        )

    request.addfinalizer(remove)
    # BuildKit reads apps/api/Dockerfile.dockerignore beside the Dockerfile.
    _run(
        [
            "docker", "build", "-f", str(REPO / "apps" / "api" / "Dockerfile"),
            "-t", image, *labels, str(REPO),
        ],
        timeout=3600,
        env={**os.environ, "DOCKER_BUILDKIT": "1"},
    )  # fmt: skip
    _run(["docker", "network", "create", *labels, network], timeout=120)
    postgres, valkey, rustfs = f"{run}-postgres", f"{run}-valkey", f"{run}-rustfs"
    _run(
        [
            "docker", "run", "-d", "--name", postgres, "--network", network, *labels,
            "--tmpfs", "/var/lib/postgresql/data",
            "-e", "POSTGRES_USER=postgres", "-e", "POSTGRES_PASSWORD=postgres",
            "-e", "POSTGRES_DB=postgres",
            POSTGRES_IMAGE,
        ],
        timeout=600,
    )  # fmt: skip
    _run(
        [
            "docker", "run", "-d", "--name", valkey, "--network", network, *labels,
            VALKEY_IMAGE, "valkey-server", "--requirepass", "valkeypass",
        ],
        timeout=600,
    )  # fmt: skip
    _run(
        [
            "docker", "run", "-d", "--name", rustfs, "--network", network, *labels,
            "--tmpfs", "/data:uid=10001,gid=10001", "--tmpfs", "/logs:uid=10001,gid=10001",
            "-e", "RUSTFS_VOLUMES=/data", "-e", "RUSTFS_ADDRESS=0.0.0.0:9000",
            "-e", "RUSTFS_ACCESS_KEY=rustfs", "-e", "RUSTFS_SECRET_KEY=rustfssecret",
            "-e", "RUSTFS_OBS_LOG_DIRECTORY=/logs",
            RUSTFS_IMAGE,
        ],
        timeout=600,
    )  # fmt: skip
    _wait_for_infrastructure(
        lambda: (
            _run(
                ["docker", "exec", postgres, "pg_isready", "-U", "postgres", "-h", "127.0.0.1"],
                timeout=30,
                check=False,
            ).returncode
            == 0
        ),
        "postgres",
        120,
    )
    _wait_for_infrastructure(
        lambda: (
            "PONG"
            in _run(
                ["docker", "exec", valkey, "valkey-cli", "-a", "valkeypass", "ping"],
                timeout=30,
                check=False,
            ).stdout
        ),
        "valkey",
        120,
    )
    _wait_for_infrastructure(
        lambda: (
            _run(
                ["docker", "exec", rustfs, "curl", "-fsS", "http://127.0.0.1:9000/health"],
                timeout=30,
                check=False,
            ).returncode
            == 0
        ),
        "rustfs",
        120,
    )
    database = f"postgresql+asyncpg://postgres:postgres@{postgres}:5432/postgres"
    # The chart's schema-migrate Job and compose's curie-migrate run this.
    _run(
        [
            "docker", "run", "--rm", "--name", f"{run}-migrate", "--network", network, *labels,
            "-e", f"DATABASE_URL={database}", "-e", "DB_SCHEMA=curie",
            image, "python", "-m", "curie_api.schema_compat", "upgrade",
        ],
        timeout=600,
    )  # fmt: skip
    # What the API needs to finish booting (compose.dev.yaml curie-api, cut to
    # boot): Postgres, Valkey, the bundle store it ensures at startup, and keys.
    env = {
        "DATABASE_URL": database,
        "DB_SCHEMA": "curie",
        "VALKEY_HOST": valkey,
        "VALKEY_PORT": "6379",
        "VALKEY_PASSWORD": "valkeypass",
        "S3_ENDPOINT_URL": f"http://{rustfs}:9000",
        "S3_ACCESS_KEY": "rustfs",
        "S3_SECRET_KEY": "rustfssecret",
        "BUNDLE_BUCKET": "curie-bundles",
        "API_KEY": f"{run}-api-key",
        "FORWARDED_ALLOW_IPS": "",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "",
    }
    yield ApiStack(run=run, image=image, network=network, env=env)


def _healthy(name: str) -> bool:
    state = _container_state(name)
    assert state["Running"], f"{name} exited {state['ExitCode']}:\n{_logs(name)[-4000:]}"
    probe = (
        "import sys, urllib.request; "
        "sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=5)"
        ".status == 200 else 1)"
    )
    return (
        _run(["docker", "exec", name, "python", "-c", probe], timeout=30, check=False).returncode
        == 0
    )


def test_mixed_repository_binding_boots_the_candidate_api(
    api_stack: ApiStack, private_key: str, tmp_path: Path
) -> None:
    configs = {}
    for name in ("valid_mixed_repositories", "distinct_scopes"):
        forges = _render_accepted(name, tmp_path)
        configs[name] = _load_in_process(forges, tmp_path / f"mount-{name}", private_key)
        assert sorted(binding.name for binding in configs[name].bindings) == sorted(
            binding["name"] for binding in _case(name)["values"]["bindings"]
        )
        container = api_stack.boot(name.replace("_", "-"), forges, private_key)
        assert _poll(functools.partial(_healthy, container), API_BOOT_TIMEOUT_S), (
            f"{name}: the API never answered /health 200"
        )
        _run(["docker", "rm", "-f", "-v", container], timeout=120)

    # One binding carries a repository on each code host kind.
    mixed = configs["valid_mixed_repositories"]
    (binding,) = mixed.bindings
    kinds = sorted(mixed.code_host(repo.code_host).kind for repo in binding.repos)
    assert kinds == ["bitbucket_cloud", "github", "gitlab"], kinds

    # The runtime backstop: the duplicate document a chart without the scope
    # check renders stops the API at boot, naming both bindings.
    duplicate = _case("duplicate_scope_through_distinct_tracker_names")
    container = api_stack.boot("duplicate", duplicate["forges"], private_key)

    def exited() -> bool:
        return not _container_state(container)["Running"]

    assert _poll(exited, API_BOOT_TIMEOUT_S), "the duplicate-scope API kept running"
    assert _container_state(container)["ExitCode"] != 0
    logs = _logs(container)
    for binding_name in duplicate["outcome"]["bindings"]:
        assert repr(binding_name) in logs, logs[-4000:]
    assert "same tracker scope" in logs, logs[-4000:]
