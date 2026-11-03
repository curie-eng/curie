"""Live driver of `curie dev factory-e2e run --scenario docker` (ADR 0216, ADR 0217).

It collects a docker-scenario/v1 envelope on a real cluster and judges it with
docker_scenario.judge. Only the GitHub stub and the scripted model are faked.

Inputs: the kube context comes only from the caller (`--context` or
CURIE_FACTORY_KUBE_CONTEXT; there is no default). CURIE_LOCAL_STUB_HOST is the
routable IPv4 the stub and model bind. No model credential or model route is read;
one in the environment is refused, and no real credential is used anywhere.

The install is daemon_lifecycle.py's: the docker-daemon fixture owns fd-docker-<n>
(namespace and Helm release), fd-docker-d-<n> and fd-docker-c-<n>, labelled
owner=factory-tools and factory-tools/nonce=<n>, and builds the daemon, platform,
catalog and runner images from the HEAD snapshot into a run-owned registry container
on the default Docker engine; api and worker build the same way. Unlike that driver it
sets no fullnameOverride, so its workloads are named fd-docker-<n>-curie-* as the CLI
expects, and its factory agent is "fde" so runner pool names stay within 63
characters. This driver adds a second platform runner (platform-next) for the upgrade,
the candidate CLI built for the runner's glibc, and a second repository. The
stub-served acme-corp/acme-bot is the custom repository (its receipt declares
Dockerfile "Dockerfile", and the stub serves it a tree carrying that Dockerfile, the
one its runner was built from), and acme-corp/acme-catalog is a catalog repository
whose runner the upgrade rebuilds, in a second, inert binding of its own; it is never
cloned and takes no turn. No host path is ever bind-mounted.

The production catalog rebuild always builds linux/amd64 and linux/arm64 and refuses
the default docker driver for a multi-platform push, so the run creates its own
docker-container buildx builder fd-builder-<n> (host network, to reach the run-owned
registry on the engine host's loopback), builds platform-next for both platforms on it
and runs the upgrade with BUILDX_BUILDER naming it. The default engine's host therefore
needs linux/arm64 emulation (binfmt). The builder is removed at teardown only when this
run created it.

The release is installed from a run-owned chart copy whose version is <v>-fdbase and
upgraded to the snapshot chart's <v>: an upgrade to the serving version would skip
Apply, so the base install must sit at another version. Its values carry the snapshot's
config.schemaVersion, as a CLI install does, because the upgrade's config migration
would otherwise infer the schema from <v>-fdbase and refuse it.

Flow:

1. concurrent: runs a and b are labelled before either is acquired; the collector
   bundle (the snapshot, plus the runner CLI on a) is streamed into each runner pod and
   docker_scenario.py's probe runs in both at once, then compose and skill-rung on a.
   A collector inherits the runner container's DOCKER_* environment, so it reaches only
   its run's daemon. Each daemon section is read from the host over the probe window.
2. lifecycle: the daemon_lifecycle scenarios, success through control.
3. upgrade: `curie cluster upgrade` pins platform-next; the plan lines and the chart
   values around it are kept, then a turn on the custom repository must be refused
   with stale_base. An upgrade that exits nonzero or does not report "succeeded" fails
   the run once that evidence is recorded.
4. ci: required checks on the stub's main head, read by docker_scenario.py ci.

Teardown always runs, then every owned namespace is proved absent by name, no Docker
object carries the run's nonce label, and the registry container, the builder and its
buildkit container and state volume are gone by exact name. ``run``
passes only when no phase failed, the judge returns nothing and teardown is verified;
``drive`` raises PreflightFailed otherwise, so the command exits non-zero. Evidence is
<work-dir>/docker-evidence.json (0600). Live use is the w2 waypoint and acceptance.
"""

from __future__ import annotations

import argparse
import base64
import copy
import json
import math
import os
import re
import secrets
import shutil
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import daemon_lifecycle as dl
import factory_e2e as fe

Obj = dict[str, Any]

HERE = Path(__file__).resolve().parent
ds = fe.load_docker_scenario()

SCENARIO = SLICE = "docker"
TRANSCRIPT = HERE / "fixtures" / "docker-scenario" / "transcript.json"
RUN_ISSUES = {"a": 4106, "b": 4107}
# The chart names each repository runner pool <fullname>-agent-<agent>-img-<12 hex>-runner-pool
# and refuses one over 63 characters (charts/curie/templates/_helpers.tpl). With no
# fullnameOverride the fullname is fd-docker-<n>-curie (24), so the agent gets 3.
DOCKER_AGENT = "fde"
CUSTOM_REPO = dl.REPO  # stub-served and cloneable; declares its own Dockerfile
CATALOG_REPO = "acme-corp/acme-catalog"  # rebuilt by the upgrade, never cloned
REQUIRED_CHECKS = ("E2E required",)
CI_CLOCK_SECONDS = 1800  # the recording's last check completes 1633 s after its epoch
MODEL_ENV = (
    "CURIE_FACTORY_MODEL_API_KEY",
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "CURIE_MODEL_CREDENTIALS",
    "ANTHROPIC_BASE_URL",
)
# Pinned like the daemon-lifecycle transcript's busybox and runner/Dockerfile's base.
PROBE_IMAGE = (
    "busybox:1.36.1@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"
)
COMPOSE_IMAGE = (
    "python:3.13.15-slim-bookworm"
    "@sha256:ed86c82274b3c69b52fb5820f358f0bd7df0b603332063cb5c6e32bd220c3e6e"
)
# Bookworm like the runner, so the CLI links against the runner's glibc.
PLATFORMS = "linux/amd64,linux/arm64"  # what the production catalog rebuild builds
CLI_BUILD_IMAGE = (
    "rust:1.95.0-bookworm@sha256:6258907abe69656e41cd992e0b705cdcfabcbbe3db374f92ed2d47121282d4a1"
)
RUNNER_CLI_DOCKERFILE = f"""\
FROM {CLI_BUILD_IMAGE}
RUN apt-get update \\
    && apt-get install -y --no-install-recommends cmake \\
    && rm -rf /var/lib/apt/lists/*
COPY . /src
RUN cargo build --locked --release --bin curie --manifest-path /src/cli/Cargo.toml
"""
BUNDLE = "/tmp/fd-docker"
SOURCE, RUNNER_CLI = f"{BUNDLE}/src", f"{BUNDLE}/bin/curie"
# Fixed literal paths only: nothing variable reaches these shell scripts.
EXTRACT_SOURCE = f"mkdir -p {SOURCE} && tar -xf - -C {SOURCE}"
INSTALL_CLI = f"mkdir -p {BUNDLE}/bin && cat > {RUNNER_CLI} && chmod 755 {RUNNER_CLI}"
COLLECTOR = f"{SOURCE}/tools/factory-e2e/docker_scenario.py"
COLLECTOR_TIMEOUTS = {"probe": 900, "compose": 1800, "skill-rung": 3600}
API_PORT = 8000
WINDOW_PAD = 5
RUN_KEYS = ("claim", "daemon_pod", "probe", "daemon")
SECTIONS = ("upgrade", "ci", "compose", "skill_rung")
# Key material the install put in the values; never retained in a values snapshot.
SECRET_VALUES = ("connectorCaller", "sealing")
SCHEMA_CONSTANT = re.compile(r'^pub const TARGET_SCHEMA_VERSION: &str = "([^"]+)";$', re.M)
UPGRADE_TEXT = "Run the Docker scenario upgrade check."


def load_docker_config(env: Mapping[str, str], *, context: str | None) -> fe.FactoryConfig:
    """The scenario's inputs: an explicit kube context and the stub host, nothing else."""

    kube_context = (context or env.get("CURIE_FACTORY_KUBE_CONTEXT") or "").strip()
    if not kube_context:
        raise fe.ConfigError(
            "docker needs an explicit kube context: --context <context> or "
            "CURIE_FACTORY_KUBE_CONTEXT; there is no default"
        )
    try:
        dl.callback_host(dict(env))
    except dl.IntakeError as exc:
        raise fe.ConfigError(f"docker: {exc}") from None
    found = [name for name in MODEL_ENV if env.get(name)]
    if found:
        raise fe.ConfigError(
            f"docker runs a scripted model only; unset {', '.join(found)} (no model "
            "credential or model route is read)"
        )
    return fe.FactoryConfig(
        kube_context=kube_context,
        app_id="",
        installation_id=0,
        private_key_file=Path("/dev/null"),
        repo=dl.REPO,
        label="factory",
        mention="",
        cloudflared="",
        priority_classes=None,
        restore_webhook_url=None,
        webhook_secret="",
        actor_token="",
        model_api_key=None,
    )


def scenario_args(context: str, callback_host: str, nonce: str | None = None) -> argparse.Namespace:
    """The fixture's intake for this run's owned names; no subprocess runs here."""

    nonce = nonce or secrets.token_hex(4)
    namespace = f"fd-{SLICE}-{nonce}"
    names = {"context": context, "namespace": namespace, "release": namespace}
    names |= {"daemon-namespace": f"fd-{SLICE}-d-{nonce}", "candidate": "HEAD"}
    names["work-dir"] = f".projects/daemon-{SLICE}-{nonce}"
    fixture = dl._load("docker_daemon_fixture", dl.FIXTURE_MODULE)
    args: argparse.Namespace = fixture.arguments(argv=[f"--{k}={v}" for k, v in names.items()])
    args.callback_host = callback_host
    return args


def chart_fullname(release: str) -> str:
    """The chart's fullname with no override, as the CLI derives it (ops/verbs.rs)."""

    name = release if "curie" in release else f"{release}-curie"
    name = name[:63]
    return name[:-1] if name.endswith("-") else name


def target_schema_version(source_root: Path) -> str:
    """TARGET_SCHEMA_VERSION of ``source_root``'s cli/src/config_migrate.rs."""

    try:
        text = (source_root / "cli/src/config_migrate.rs").read_text()
    except OSError:
        raise RuntimeError("target_schema_version_missing") from None
    found = SCHEMA_CONSTANT.search(text)
    if found is None:
        raise RuntimeError("target_schema_version_missing")
    return found[1]


def docker_values(values: Obj, images: dict[str, dict[str, str]], schema_version: str) -> Obj:
    """``values`` with acme-bot custom, a catalog repository and daemon limits a skill rung fits."""

    out = copy.deepcopy(values)
    # Stamped as a CLI install stamps it: the upgrade's config migration infers an unstamped
    # release's schema from its chart version and refuses the <v>-fdbase base install.
    out["config"] = dict(out.get("config") or {}) | {"schemaVersion": schema_version}
    # The CLI's schema probe execs deploy/<chart_fullname(release)>-api, ignoring any
    # fullnameOverride, so the release keeps the chart's own fullname.
    out.pop("fullnameOverride", None)
    for section, key in (("factory", "sandboxEgress"), ("agentSandbox", "runnerEnv")):
        parent = out.get(section)
        keyed = parent.get(key) if isinstance(parent, dict) else None
        if isinstance(parent, dict) and isinstance(keyed, dict) and dl.AGENT in keyed:
            parent[key] = {DOCKER_AGENT if k == dl.AGENT else k: v for k, v in keyed.items()}
    bindings = out["factory"]["bindings"]
    custom = next(repo for repo in bindings[0]["repos"] if repo["path"] == CUSTOM_REPO)
    # Only the CLI upgrade planner reads the key; the chart and worker ignore it.
    custom["runner"]["dockerfile"] = "Dockerfile"
    runner: Obj = {"image": images["runner"]["ref"], "base": images["platform"]["digest"]}
    runner |= {"platformDigests": dict(custom["runner"]["platformDigests"])}
    runner |= {"tools": ["docker-cli"]}
    catalog: Obj = {"alias": "acme-catalog", "codeHost": "github", "projectId": "4402"}
    catalog |= {"path": CATALOG_REPO, "runner": runner}
    # A native GitHub binding holds exactly its own repository (curie_api forges/config.py
    # _check_native), so the catalog gets its own binding. It stays inert: GitHub intake
    # polls only agent_channels addresses (factory_label_reconcile.py bound_repositories),
    # the tracker intake skips GitHub trackers (factory_tracker_intake.py), and the harness
    # creates a channel only for acme-bot.
    scope = {"tracker": bindings[0]["tracker"], "trackerScopeId": catalog["projectId"]}
    bindings.append({"name": "acme-catalog", **scope, "repos": [catalog]})
    daemon = out["factory"]["docker"]
    daemon |= {"maxClaimLifetimeSeconds": 3600, "storageSizeLimit": "24Gi"}
    resources = daemon.setdefault("resources", {})
    resources.setdefault("limits", {}).update(
        {"memory": "4Gi", "ephemeral-storage": "32Gi", "cpu": "2"}
    )
    resources.setdefault("requests", {})["ephemeral-storage"] = "8Gi"
    # The docker scenario runs two factory sandboxes at once, so this install asks for
    # 4Gi of process memory only.
    sandbox = out.setdefault("agentSandbox", {})
    profiles = sandbox.setdefault("resourceProfiles", {})
    factory_profile = profiles.setdefault("factory", {})
    factory_resources = factory_profile.setdefault("resources", {})
    factory_resources.setdefault("requests", {})["memory"] = "4Gi"
    factory_resources.setdefault("limits", {})["memory"] = "4Gi"
    return out


def collector_argv(
    command: str,
    run: str,
    *,
    nonce: str,
    image: str | None = None,
    cluster_api: str | None = None,
    api: str | None = None,
    source: str | None = None,
    curie_bin: str | None = None,
    source_revision: str | None = None,
) -> list[str]:
    """docker_scenario.py ``command`` inside a runner pod, writing its section to a file."""

    flags = {"--nonce": nonce, "--image": image, "--cluster-api": cluster_api, "--api": api}
    flags |= {"--source": source, "--curie-bin": curie_bin, "--source-revision": source_revision}
    argv = ["python3", COLLECTOR, command]
    for flag, value in flags.items():
        if value is not None:
            argv += [flag, value]
    return [*argv, "--out", f"{BUNDLE}/{command}-{run}.json"]


def upgrade_values(image: dict[str, str]) -> Obj:
    repository = image["ref"].split("@", 1)[0]
    return {"agentSandbox": {"runner": {"image": repository, "digest": image["digest"]}}}


def upgrade_argv(
    curie: str, context: str, version: str, chart: str, namespace: str, release: str, values: str
) -> list[str]:
    argv = [curie, "--json", "cluster", "--context", context, "upgrade", "--to", version]
    argv += ["--chart", chart, "--namespace", namespace, "--release", release]
    return [*argv, "--yes", "-f", values]


def message_argv(
    curie: str,
    context: str,
    *,
    nonce: str,
    namespace: str,
    release: str,
    chart: str,
    callback_host: str,
) -> list[str]:
    text = f"Repository: https://github.com/{CUSTOM_REPO}\n{UPGRADE_TEXT}"
    argv = [curie, "cluster", "--context", context, "message", text, "--agent", DOCKER_AGENT]
    argv += ["--channel", f"C0EXAMPLE{nonce.upper()}", "--thread", f"fd-upgrade-{nonce}"]
    argv += ["--namespace", namespace, "--release", release, "--chart", chart]
    return [*argv, "--listen-host", callback_host, "--timeout-secs", "300", "--json"]


def plan_text(output: object) -> str:
    plan = output.get("plan") if isinstance(output, dict) else None
    return "\n".join(line for line in plan if isinstance(line, str)) if plan else ""


def message_reply(stdout: str) -> str:
    """``reply`` of the last JSON object line of `curie cluster message --json`."""

    last: Obj = {}
    for line in stdout.splitlines():
        try:
            found = json.loads(line)
        except ValueError:
            continue
        if isinstance(found, dict):
            last = found
    reply = last.get("reply")
    return reply if isinstance(reply, str) else ""


def chart_version(chart: Path) -> str:
    found = re.search(r"^version:\s*[\"']?([^\"'\s]+)", (chart / "Chart.yaml").read_text(), re.M)
    if found is None:
        raise RuntimeError("chart_version_missing")
    return found[1]


def _public_values(values: Obj) -> Obj:
    return {key: value for key, value in values.items() if key not in SECRET_VALUES}


class DockerScenario(dl.Lifecycle):
    agent = DOCKER_AGENT

    def __init__(self, args: argparse.Namespace, candidate: str | None = None) -> None:
        super().__init__(args)
        self.full = chart_fullname(self.release)
        self.candidate = candidate or ""
        self.runs: dict[str, Obj] = {}
        self.sections: dict[str, Obj] = {}
        self.cleanup_evidence: Obj = {}
        self.builder = ""  # set only once this run has proved the name free and created it
        self.lock = threading.Lock()

    # prepare and install

    def prepare(self) -> None:
        super().prepare()
        moved = self.candidate and self.f.candidate != self.candidate
        self.fx.require(not moved, "candidate_moved")
        self.create_builder()
        self.build_platform_next()
        self.build_runner_cli()

    def create_builder(self) -> None:
        """A run-owned docker-container builder for multi-platform pushes."""
        name = f"fd-builder-{self.nonce}"
        found = self.fx.run(["docker", "buildx", "inspect", name], check=False, timeout=60)
        self.fx.require(found.returncode, "builder_name_occupied")
        self.fx.require("no builder" in found.stderr.lower(), "builder_inventory_unavailable")
        self.builder = name  # proved free above, so whatever create leaves is this run's
        argv = ["docker", "buildx", "create", "--name", name, "--driver", "docker-container"]
        self.fx.run([*argv, "--driver-opt", "network=host"], timeout=300)

    def build_platform_next(self) -> None:
        """A second platform runner for both platforms, distinct by label, for the upgrade."""
        self.fx.require(self.builder, "builder_missing")
        image = self.build_image(
            "platform-next",
            builder=self.builder,
            platform=PLATFORMS,
            dockerfile=self.f.snapshot / "runner/Dockerfile",
            labels=("curie.fixture.platform=next",),
        )
        distinct = image["digest"] != self.images["platform"]["digest"]
        self.fx.require(distinct, "platform_next_not_distinct")
        self.images["platform-next"] = image

    def build_runner_cli(self) -> None:
        """The candidate CLI built on bookworm and copied out; no bind mount."""
        fx, f = self.fx, self.f
        tag = f"fd-{self.nonce}-runner-cli:candidate"
        fx.require(tag not in fx.run(fx.TAGS).stdout.split(), "build_tag_occupied")
        dockerfile = f.work / "runner-cli.Dockerfile"
        dockerfile.write_text(RUNNER_CLI_DOCKERFILE)
        owned = ["--label", "owner=factory-tools", "--label", f"{fx.NONCE}={self.nonce}"]
        target = f.work / "runner-cli"
        target.mkdir(mode=0o700, exist_ok=True)
        # Intents first, so the fixture's release_docker_assets reclaims both on any failure.
        request = uuid.uuid4().hex
        f.asset("tag_intent", tag, request)
        try:
            build = ["docker", "build", *owned, "--label", f"{fx.REQUEST}={request}"]
            fx.run([*build, "-t", tag, "-f", str(dockerfile), str(f.snapshot)], timeout=7200)
            name, request = f"fd-{self.nonce}-runner-cli", uuid.uuid4().hex
            f.asset("container_intent", name, request)
            create = ["docker", "create", "--name", name, *owned]
            fx.run([*create, "--label", f"{fx.REQUEST}={request}", tag])
            try:
                source = f"{name}:/src/cli/target/release/curie"
                fx.run(["docker", "cp", source, str(target / "curie")], timeout=300)
            finally:
                fx.run(["docker", "rm", "-f", name])
        finally:
            fx.run(["docker", "image", "rm", tag], check=False)
        (target / "curie").chmod(0o755)

    def install_chart(self) -> Path:
        """A run-owned copy of the snapshot chart at <v>-fdbase, so the upgrade to <v> applies."""
        base = self.f.work / "chart-base"
        if not base.exists():
            text = (self.f.snapshot / "charts/curie/Chart.yaml").read_text()
            line = re.compile(r"^version:[ \t]*[\"']?([^\"'\s]+)[\"']?[ \t]*$", re.M)
            found = line.search(text)
            if found is None:
                raise RuntimeError("chart_version_missing")
            rewritten = text[: found.start()] + f"version: {found[1]}-fdbase" + text[found.end() :]
            shutil.copytree(self.f.snapshot / "charts/curie", base)
            (base / "Chart.yaml").write_text(rewritten)
        return cast(Path, base)

    def seed_tree(self) -> Path:
        """acme-bot's repository: the unitconv fixture plus the Dockerfile its receipt declares."""
        dockerfile = self.f.work / "Catalog.Dockerfile"  # what the fixture built the runner from
        if not dockerfile.is_file():
            raise RuntimeError("custom_dockerfile_missing")
        tree = self.f.work / "seed-tree"
        shutil.rmtree(tree, ignore_errors=True)
        shutil.copytree(dl.SEED_TREE, tree)
        (tree / "Dockerfile").write_text(dockerfile.read_text())
        return cast(Path, tree)

    def start_servers(self) -> None:
        """daemon_lifecycle's servers with this scenario's transcript and seed tree."""
        host = self.args.callback_host
        recording = json.loads(dl.RECORDING.read_text())
        seeds = {"host": host, "seed_tree": self.seed_tree(), "issue_file": dl.ISSUE_FILE}
        self.stub = self.github.GithubStub(self.f.work / "github", recording, **seeds)
        self.stub.start()
        # The stub seeds issue 3815 labelled; this run uses its own issues only.
        self.stub._put("issue", 3815, self.stub._get("issue", 3815) | {"labels": []})
        self.model = self.models.ModelScript(TRANSCRIPT, host=host)
        self.model.start()
        self.proxy = dl.ModelProxy(host, self.model.base_url)

    def values(self, *, docker: bool) -> Obj:
        schema = target_schema_version(self.f.snapshot)
        return docker_values(super().values(docker=docker), self.images, schema)

    # scenarios

    def scenarios(self) -> None:
        self.scenario_concurrent()
        super().scenarios()
        self.scenario_upgrade()
        self.collect_ci()

    def kubectl_exec(
        self, pod: str, *argv: str, timeout: float = 60
    ) -> subprocess.CompletedProcess[str]:
        """A command in a runner container; no request timeout, collectors run long."""
        command = ["kubectl", "--context", self.args.context, "-n", self.ns, "exec", pod]
        result: subprocess.CompletedProcess[str] = self.fx.run(
            [*command, "-c", "runner", "--", *argv], check=False, timeout=timeout
        )
        return result

    def push_bundle(self, pod: str, *, cli: bool = False) -> None:
        """Stream the snapshot (and on run a the runner CLI) into the runner pod."""
        exec_in = ["kubectl", "--context", self.args.context, "-n", self.ns, "exec", "-i", pod]
        exec_in += ["-c", "runner", "--", "sh", "-c"]
        pipe = subprocess.PIPE
        tar = subprocess.Popen(["tar", "-C", str(self.f.snapshot), "-cf", "-", "."], stdout=pipe)
        try:
            pushed = subprocess.run(
                [*exec_in, EXTRACT_SOURCE],
                stdin=tar.stdout,
                capture_output=True,
                timeout=900,
                check=False,
            )
        finally:
            if tar.stdout is not None:
                tar.stdout.close()
            archived = tar.wait(timeout=60)
        self.fx.require(pushed.returncode == 0 and archived == 0, "bundle_push_failed")
        if not cli:
            return
        with (self.f.work / "runner-cli" / "curie").open("rb") as binary:
            installed = subprocess.run(
                [*exec_in, INSTALL_CLI], stdin=binary, capture_output=True, timeout=300, check=False
            )
        self.fx.require(installed.returncode == 0, "runner_cli_push_failed")

    def collector(self, pod: str, argv: list[str], run: str) -> Obj:
        """Run one collector; its written section stands even on exit 1 (the judge decides)."""
        command, out = argv[2], argv[-1]
        code = self.kubectl_exec(pod, *argv, timeout=COLLECTOR_TIMEOUTS[command]).returncode
        with self.lock:
            self.evidence.setdefault("collector_exit_codes", {})[f"{command}-{run}"] = code
        read = self.kubectl_exec(pod, "cat", out)
        section = None
        if read.returncode == 0:
            try:
                section = json.loads(read.stdout)
            except ValueError:
                section = None
        if not isinstance(section, dict):
            raise RuntimeError(f"collector_failed:{command}")
        return section

    def cluster_api(self, pod: str) -> str:
        names = ("KUBERNETES_SERVICE_HOST", "KUBERNETES_SERVICE_PORT")
        result = self.kubectl_exec(pod, "printenv", *names)
        found = result.stdout.split()
        self.fx.require(result.returncode == 0 and len(found) == 2, "cluster_api_unknown")
        return f"{found[0]}:{found[1]}"

    def api_target(self) -> str:
        service = self.f.get(f"/api/v1/namespaces/{self.ns}/services/{self.full}-api")
        address = service.get("spec", {}).get("clusterIP", "")
        self.fx.require(address and address != "None", "api_service_missing")
        return f"{address}:{API_PORT}"

    def concurrent_probes(self, runners: dict[str, str], api: str) -> dict[str, Obj]:
        """The probe collector in both runs at once; their windows must overlap."""
        argvs = {
            run: collector_argv(
                "probe",
                run,
                nonce=self.nonce,
                image=PROBE_IMAGE,
                cluster_api=self.cluster_api(pod),
                api=api,
            )
            for run, pod in runners.items()
        }
        probes: dict[str, Obj] = {}
        errors: list[BaseException] = []

        def probe(run: str) -> None:
            try:
                probes[run] = self.collector(runners[run], argvs[run], run)
            except BaseException as exc:  # noqa: BLE001 - re-raised after both threads end
                errors.append(exc)

        threads = [threading.Thread(target=probe, args=(run,)) for run in runners]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        for run, section in probes.items():
            self.runs[run]["probe"] = section
        if errors:
            raise errors[0]
        return probes

    def scenario_concurrent(self) -> None:
        """Runs a and b: labelled together, probed concurrently, released together."""
        snapshots = {run: self.before(issue) for run, issue in RUN_ISSUES.items()}
        for issue in RUN_ISSUES.values():
            self.set_label(issue, True)
        turns: dict[str, Obj] = {}
        pods: dict[str, Obj] = {}
        for run, issue in RUN_ISSUES.items():
            turns[run], pods[run] = self.acquire(issue, *snapshots[run])
        runners = {run: self.observe(turns[run], pods[run], run=False) for run in RUN_ISSUES}
        for run, turn in turns.items():
            self.runs[run] = {"claim": turn["claim"], "daemon_pod": turn["daemon"]["pod"]}
        for run, pod in runners.items():
            self.push_bundle(pod, cli=run == "a")
        probes = self.concurrent_probes(runners, self.api_target())
        compose = collector_argv("compose", "a", nonce=self.nonce, image=COMPOSE_IMAGE)
        self.sections["compose"] = self.collector(runners["a"], compose, "a")
        skill = collector_argv(
            "skill-rung", "a", nonce=self.nonce, source=SOURCE, curie_bin=RUNNER_CLI,
            source_revision=self.f.candidate,
        )  # fmt: skip
        self.sections["skill_rung"] = self.collector(runners["a"], skill, "a")
        for run in RUN_ISSUES:
            t0, t1 = probes[run]["window"]
            since, until = str(math.floor(t0) - WINDOW_PAD), str(math.ceil(t1) + WINDOW_PAD)
            self.runs[run]["daemon"] = ds.collect(
                context=self.args.context, namespace=self.dns, pod=self.runs[run]["daemon_pod"],
                runner_namespace=self.ns, runner_pod=runners[run],
                nonce=self.nonce, since=since, until=until,
            )  # fmt: skip
        started = time.monotonic()
        for pod in runners.values():
            self.signal(pod, "release")
        for run, turn in turns.items():
            self.settle(turn, "release", started)
            keys = ("settle_seconds", "objects_absent", "request_state", "leaked")
            self.runs[run]["settle"] = {key: turn.get(key) for key in keys}

    def helm_values(self) -> Obj:
        argv = ["helm", "--kube-context", self.args.context, "get", "values", self.release]
        found = json.loads(self.fx.run([*argv, "-n", self.ns, "-o", "json"], timeout=120).stdout)
        return found if isinstance(found, dict) else {}

    def scenario_upgrade(self) -> None:
        """`curie cluster upgrade` to platform-next, then a turn on the custom repository."""
        work, chart = self.f.work, self.f.snapshot / "charts/curie"
        before = self.helm_values()
        values_file = work / "upgrade.values.json"
        values_file.unlink(missing_ok=True)
        dl.private_file(values_file, json.dumps(upgrade_values(self.images["platform-next"])))
        argv = upgrade_argv(
            self.curie, self.args.context, chart_version(chart), str(chart), self.ns,
            self.release, str(values_file),
        )  # fmt: skip
        self.fx.require(self.builder, "builder_missing")
        # The catalog rebuild is a multi-platform push, so it needs the run-owned builder.
        build_env = dict(self.env, BUILDX_BUILDER=self.builder)
        upgraded = self.fx.run(argv, env=build_env, check=False, timeout=3600)
        (work / "upgrade-output.json").write_text(upgraded.stdout)
        self.evidence |= {"upgrade_exit_code": upgraded.returncode}
        self.evidence["upgrade_output"] = upgraded.stdout
        try:
            output = json.loads(upgraded.stdout)
        except ValueError:
            output = None
        status = output.get("status") if isinstance(output, dict) else None
        for key in ("from_version", "target_version"):
            self.evidence[key] = output.get(key) if isinstance(output, dict) else None
        succeeded = upgraded.returncode == 0 and status == "succeeded"
        if succeeded:
            self.rollout("api", "worker")
        after = self.helm_values()
        for name, values in (("values-before.json", before), ("values-after.json", after)):
            (work / name).unlink(missing_ok=True)
            dl.private_file(work / name, json.dumps(_public_values(values), indent=2) + "\n")
        secret = self.f.get(f"/api/v1/namespaces/{self.ns}/secrets/{self.full}-secrets")
        api_key = base64.b64decode(secret["data"]["apiKey"]).decode()
        env = dict(self.env, CURIE_API_KEY=api_key, CURIE_CONFIG_DIR=str(work / "curie-config"))
        message = message_argv(
            self.curie, self.args.context, nonce=self.nonce, namespace=self.ns,
            release=self.release, chart=str(chart), callback_host=self.args.callback_host,
        )  # fmt: skip
        turn = self.fx.run(message, env=env, check=False, timeout=420)
        self.evidence["upgrade_message_exit_code"] = turn.returncode
        reply = message_reply(turn.stdout)
        self.sections["upgrade"] = ds.upgrade_section(
            plan_text(output), before, after, CATALOG_REPO, CUSTOM_REPO, reply
        )
        if not succeeded:  # raised last, so the output, turn and section stay as evidence
            detail = status if status not in (None, "succeeded") else None
            raise RuntimeError(f"upgrade_failed:{detail or f'exit_{upgraded.returncode}'}")

    def collect_ci(self) -> None:
        """Required checks on the stub's main head, read like a real GitHub."""
        self.stub.advance(CI_CLOCK_SECONDS)
        sha = self.stub._find_ref("main")
        self.fx.require(sha, "stub_main_missing")
        out = self.f.work / "ci.json"
        out.unlink(missing_ok=True)
        argv = [sys.executable, str(HERE / "docker_scenario.py"), "ci"]
        argv += ["--api-url", self.stub.base_url + "/api/v3", "--repo", dl.REPO]
        argv += ["--base", "main", "--sha", str(sha)]
        for name in REQUIRED_CHECKS:
            argv += ["--required", name]
        argv += ["--out", str(out)]
        env = dict(os.environ, SSL_CERT_FILE=str(self.stub.ca_file))
        env["GITHUB_TOKEN"] = self.github_token()
        result = self.fx.run(argv, env=env, check=False, timeout=300)
        self.evidence["ci_exit_code"] = result.returncode
        try:
            section = json.loads(out.read_text())
        except (OSError, ValueError):
            section = None
        if not isinstance(section, dict):
            raise RuntimeError("collector_failed:ci")
        self.sections["ci"] = section

    # teardown

    def release_everything(self, errors: list[str]) -> dict[str, bool]:
        cleanup: dict[str, bool] = super().release_everything(errors)
        if self.builder:  # never a builder this run did not create
            argv = ["docker", "buildx", "rm", self.builder]
            try:
                removed = self.fx.run(argv, check=False, timeout=300).returncode == 0
            except (OSError, subprocess.SubprocessError) as exc:
                removed = False
                errors.append(f"builder_remove:{type(exc).__name__}")
            if not removed:
                errors.append("builder_remove_failed")
                cleanup["docker_assets_absent"] = False
        return cleanup

    def scrub_work(self, errors: list[str]) -> None:
        super().scrub_work(errors)
        if self.f is None:
            return
        work = self.f.work
        try:
            (work / "upgrade.values.json").unlink(missing_ok=True)
            directories = ("runner-cli", "chart-base", "seed-tree")
            for directory in directories:
                shutil.rmtree(work / directory, ignore_errors=True)
            left = [d for d in directories if (work / d).exists()]
            if (work / "upgrade.values.json").exists() or left:
                errors.append("work_dir_key_material_remains")
        except OSError as exc:
            errors.append(f"work_dir_scrub:{type(exc).__name__}")

    def verify_teardown(self, errors: list[str]) -> None:
        """Owned namespaces NotFound, no nonce-labelled Docker object, no named run object."""
        names: dict[str, str] = {}
        owned = (self.ns, self.dns, self.args.controller_namespace)
        kubectl = ["kubectl", "--context", self.args.context, "--request-timeout=15s"]
        for name in owned:
            result = self.fx.run([*kubectl, "get", "ns", name], check=False, timeout=30)
            if result.returncode == 0:
                names[name] = "present"
            elif "NotFound" in result.stderr:
                names[name] = "absent"
            else:
                names[name] = "unproved"
        label = f"label={self.fx.NONCE}={self.nonce}"
        listings = {
            "containers": ["docker", "ps", "-aq", "--filter", label],
            "images": ["docker", "images", "-q", "--filter", label],
            "volumes": ["docker", "volume", "ls", "-q", "--filter", label],
            "networks": ["docker", "network", "ls", "-q", "--filter", label],
        }
        registry = ["docker", "ps", "-aq", "--filter", f"name=^fd-registry-{self.nonce}$"]
        docker: dict[str, list[str] | str] = {}
        for kind, argv in (*listings.items(), ("registry", registry)):
            result = self.fx.run(argv, check=False, timeout=60)
            docker[kind] = result.stdout.split() if result.returncode == 0 else "unproved"
        builder = f"fd-builder-{self.nonce}"
        buildkit = f"buildx_buildkit_{builder}0"
        named = {
            "builder": (["docker", "buildx", "inspect", builder], "no builder"),
            "buildkit_container": (["docker", "container", "inspect", buildkit], "no such"),
            "buildkit_volume": (["docker", "volume", "inspect", f"{buildkit}_state"], "no such"),
        }
        for kind, (argv, missing) in named.items():
            result = self.fx.run(argv, check=False, timeout=60)
            if result.returncode == 0:
                docker[kind] = [argv[-1]]
            else:
                docker[kind] = [] if missing in result.stderr.lower() else "unproved"
        self.cleanup_evidence = {"namespaces": names, "docker": docker}
        if any(state != "absent" for state in names.values()):
            errors.append("teardown_unverified: owned namespace not proved absent")
            self.envelope["cleanup"]["namespaces_absent"] = False
        if any(found for found in docker.values()):
            errors.append("teardown_unverified: docker objects of this run remain")
            self.envelope["cleanup"]["docker_assets_absent"] = False

    # run

    def docker_envelope(self) -> Obj:
        """The assembled envelope, or what exists so the judge names each missing section."""
        if all(name in self.sections for name in SECTIONS) and set(self.runs) >= {"a", "b"}:
            try:
                sections = (self.sections[name] for name in SECTIONS)
                return dict(ds.assemble(self.nonce, self.runs, self.envelope, *sections))
            except (KeyError, TypeError, ValueError):
                pass
        envelope: Obj = {"schema": ds.SCHEMA, "nonce": self.nonce}
        runs = {name: {k: run[k] for k in RUN_KEYS if k in run} for name, run in self.runs.items()}
        if runs:
            envelope["runs"] = runs
        try:
            envelope |= ds.from_lifecycle(self.envelope)
        except (KeyError, TypeError, ValueError, AttributeError):
            pass
        return envelope | {name: self.sections[name] for name in SECTIONS if name in self.sections}

    def run(self) -> Obj:
        failures: list[str] = []
        try:
            self.prepare()
            self.install()
            self.scenarios()
            self.check_fixtures()
        except SystemExit as exc:
            code = exc.code if isinstance(exc.code, int) else 1
            failures.append("signal" if code > 128 else "exit")
        except KeyboardInterrupt:
            failures.append("interrupted")
        except RuntimeError as exc:
            failures.append(str(exc))
        except Exception as exc:  # noqa: BLE001 - recorded by class, then torn down
            failures.append(type(exc).__name__)
        evidence = self.failure_evidence(bool(failures))
        teardown_errors: list[str] = []
        try:
            self.envelope["cleanup"] = self.teardown(teardown_errors)
        except Exception as exc:  # noqa: BLE001 - teardown failure fails the run
            teardown_errors.append(type(exc).__name__)
        try:
            self.verify_teardown(teardown_errors)
        except Exception as exc:  # noqa: BLE001 - an unproved teardown fails the run
            teardown_errors.append(f"verify_teardown:{type(exc).__name__}")
        self.envelope["timings"] = self.timings
        envelope = self.docker_envelope()
        reasons = ds.judge(envelope)
        lifecycle = dl.judge(self.envelope)
        self.envelope["result"] = "failed" if failures or teardown_errors or lifecycle else "passed"
        self.envelope["failures"] = failures + teardown_errors + lifecycle
        if evidence := self.evidence | evidence:
            self.envelope["failure_evidence"] = evidence
        self.write_evidence()
        passed = not failures and not teardown_errors and not reasons
        cleanup = dict(self.envelope["cleanup"]) | self.cleanup_evidence
        result: Obj = {"result": "passed" if passed else "failed", "nonce": self.nonce}
        result |= {"failures": failures + teardown_errors + reasons, "envelope": envelope}
        result |= {"cleanup": cleanup, "evidence": f"{self.args.work_dir}/docker-evidence.json"}
        result = json.loads(json.dumps(result))
        if self.f is not None:
            path = self.f.work / "docker-evidence.json"
            path.unlink(missing_ok=True)
            written = result | {"failure_evidence": evidence}
            dl.private_file(path, json.dumps(written, indent=2, sort_keys=True) + "\n")
        summary = {key: result[key] for key in ("result", "nonce", "failures", "evidence")}
        print(json.dumps(summary, sort_keys=True), flush=True)
        return result


def drive(p: Any) -> Obj:
    """The factory_e2e scenario hook: passes only when the run passed."""

    try:
        host = dl.callback_host(dict(os.environ))
    except dl.IntakeError as exc:
        raise fe.ConfigError(f"docker: {exc}") from None
    args = scenario_args(p.config.kube_context, host)
    result: Obj = DockerScenario(args, candidate=p.candidate).run()
    if result.get("result") != "passed":
        failures = result.get("failures") or ["no result"]
        raise fe.PreflightFailed("docker scenario failed: " + "; ".join(map(str, failures)))
    return result
