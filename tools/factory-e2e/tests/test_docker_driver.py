"""Offline checks for the docker scenario driver (ADR 0216, ADR 0217).

`curie dev factory-e2e run --scenario docker` runs docker_driver.DockerScenario: the
daemon-lifecycle install, two concurrent Docker runs whose turns run docker_scenario.py's
collectors, the lifecycle scenarios, an upgrade through `curie cluster upgrade`, CI from the
GitHub stub, then teardown, an envelope and docker_scenario.judge. Every external seam here
is a fake that replays a real shape; the one live object is an in-process GithubStub.

Test names carry the keyword of the plan group they prove (main_run, install, concurrent,
lifecycle, upgrade, ci, judge or exit, teardown) so receipts can select them with ``-k``.
"""

from __future__ import annotations

import base64
import copy
import functools
import inspect
import io
import json
import math
import os
import re
import shlex
import ssl
import subprocess
import sys
import threading
import time
import urllib.request
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.request import Request, urlopen

# factory_e2e first: its bottom binding loads docker_driver, which subclasses the
# daemon_lifecycle driver, so every other module comes through it.
import factory_e2e as fe
import pytest

ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parents[1]
docker_driver = fe.load_docker_driver()
docker_scenario = fe.load_docker_scenario()
dl = fe._load_by_path("daemon_lifecycle", ROOT / "daemon_lifecycle.py")
model_script = fe._load_by_path(
    "daemon_lifecycle_model_script", REPO_ROOT / "tools/model-script/model_script.py"
)
github_stub = fe._load_by_path(
    "daemon_lifecycle_github_stub", REPO_ROOT / "tools/github-stub/github_stub.py"
)

LIFECYCLE_FIXTURES = ROOT / "fixtures" / "daemon-lifecycle"
DOCKER_FIXTURES = ROOT / "fixtures" / "docker-scenario"
RECORDING = REPO_ROOT / "tools/github-stub/recordings/curie-pr-3400.json"
SEED_TREE = ROOT / "fixtures" / "unitconv"

# The passing docker envelope's nonce; the lifecycle fixture is re-nonced to match it so
# both judges see one run.
NONCE = "e8ce1a8c"
LIFECYCLE_NONCE = "1477dbc5"
CONTEXT = "ctx-under-test"
STUB_HOST = "192.0.2.10"
CANDIDATE = "7540c304e5534827783ceacbccd70553ca324f22"  # the lifecycle fixture's candidate
HEAD_SHA = "0123456789abcdef0123456789abcdef01234567"
NAMESPACE = f"fd-docker-{NONCE}"
DAEMON_NAMESPACE = f"fd-docker-d-{NONCE}"
CONTROLLER_NAMESPACE = f"fd-docker-c-{NONCE}"
# The chart's fullname with no fullnameOverride (cli/src/ops/verbs.rs chart_fullname): the
# CLI's live-schema probe execs deploy/<fullname>-api, so the release must render it.
FULL = f"fd-docker-{NONCE}-curie"
DOCKER_AGENT = "fde"
# The custom repository (its own Dockerfile) is the stub-served, cloneable acme-bot; the
# catalog repository is only rebuilt by the CLI, never cloned.
CUSTOM_REPO = "acme-corp/acme-bot"
CATALOG_REPO = "acme-corp/acme-catalog"
BUILDER = f"fd-builder-{NONCE}"
BUILDKIT_CONTAINER = f"buildx_buildkit_{BUILDER}0"
BUILDKIT_VOLUME = f"buildx_buildkit_{BUILDER}0_state"
SRC = "/tmp/fd-docker/src"
COLLECTOR = f"{SRC}/tools/factory-e2e/docker_scenario.py"
API_IP = "10.0.0.5"
CLUSTER_API = "10.96.0.1:443"
MODEL_ENV = (
    "CURIE_FACTORY_MODEL_API_KEY",
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "CURIE_MODEL_CREDENTIALS",
    "ANTHROPIC_BASE_URL",
)
RUNNERS = {"a": "runner-a-0", "b": "runner-b-0"}
IMAGE_NAMES = ("daemon", "platform", "catalog", "runner", "api", "worker", "platform-next")
NEXT_DIGEST = "sha256:" + "5e" * 32


# --------------------------------------------------------------------------
# fixtures and fakes
# --------------------------------------------------------------------------


def _lifecycle_envelope() -> dict[str, Any]:
    text = (LIFECYCLE_FIXTURES / "passing-envelope.json").read_text()
    return dict(json.loads(text.replace(LIFECYCLE_NONCE, NONCE)))


def _docker_envelope() -> dict[str, Any]:
    return dict(json.loads((DOCKER_FIXTURES / "passing-envelope.json").read_text()))


def _section(name: str) -> dict[str, Any]:
    return dict(json.loads((DOCKER_FIXTURES / f"{name}.json").read_text()))


def _images() -> dict[str, dict[str, str]]:
    out = {}
    for index, name in enumerate(IMAGE_NAMES):
        digest = NEXT_DIGEST if name == "platform-next" else "sha256:" + f"{index + 1:x}" * 64
        out[name] = {
            "ref": f"localhost:5000/fd-{NONCE}/{name}@{digest}",
            "digest": digest,
            "manifest": "sha256:" + f"{index + 8:x}" * 64,
            "config": "sha256:" + "f" * 64,
        }
    return out


def _text_mode(kwargs: dict[str, Any]) -> bool:
    return bool(kwargs.get("text") or kwargs.get("universal_newlines") or kwargs.get("encoding"))


def _shell_tokens(argv: list[str]) -> list[str]:
    """argv with any ``sh -c`` script split into words."""

    out: list[str] = []
    for token in argv:
        out += shlex.split(token) if " " in token else [token]
    return out


def _flag(argv: list[str], name: str) -> str | None:
    return argv[argv.index(name) + 1] if name in argv[:-1] else None


def _adjacent(argv: list[str], first: str, second: str) -> bool:
    return any(a == first and b == second for a, b in zip(argv, argv[1:], strict=False))


def _strip_kubectl(argv: list[str]) -> list[str]:
    out, skip = [], False
    for token in argv[1:]:
        if skip:
            skip = False
        elif token in ("--context", "-n", "--namespace"):
            skip = True
        elif token.startswith(("--request-timeout", "--context=", "--namespace=")):
            continue
        else:
            out.append(token)
    return out


def _exec_pod(argv: list[str]) -> str:
    tokens = argv[argv.index("exec") + 1 :]
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token in ("-c", "--container", "-n", "--namespace"):
            index += 2
        elif token.startswith("-"):
            index += 1
        else:
            return token
    raise AssertionError(f"kubectl exec without a pod: {argv}")


Result = tuple[int, str, str]
Handler = Callable[[list[str], dict[str, Any]], Result | None]


class _Process:
    """A finished Popen with the recorded command's output."""

    def __init__(self, result: subprocess.CompletedProcess[str], kwargs: dict[str, Any]) -> None:
        self.text = _text_mode(kwargs)
        self.args, self.pid, self.returncode = result.args, 4242, result.returncode
        self._out, self._err = result.stdout, result.stderr
        self.stdout = io.StringIO(self._out) if self.text else io.BytesIO(self._out.encode())
        self.stderr = io.StringIO(self._err) if self.text else io.BytesIO(self._err.encode())
        self.stdin = io.StringIO() if self.text else io.BytesIO()

    def _value(self, text: str) -> Any:
        return text if self.text else text.encode()

    def communicate(self, *_args: Any, **_kwargs: Any) -> tuple[Any, Any]:
        return self._value(self._out), self._value(self._err)

    def wait(self, *_args: Any, **_kwargs: Any) -> int:
        return self.returncode

    def poll(self) -> int:
        return self.returncode

    def terminate(self) -> None:
        return None

    def kill(self) -> None:
        return None

    def __enter__(self) -> _Process:
        return self

    def __exit__(self, *_args: Any) -> None:
        return None


class Commands:
    """One funnel for every subprocess seam the driver may use: the fixture module's
    ``run`` (and through it ``Fixture.k``), ``subprocess.run`` and ``subprocess.Popen``.
    Unknown commands fail the test, so nothing real ever runs."""

    def __init__(self) -> None:
        self.calls: list[tuple[list[str], dict[str, Any]]] = []
        self.handlers: list[Handler] = []
        self.lock = threading.Lock()

    def handle(self, argv: Any, kwargs: dict[str, Any]) -> subprocess.CompletedProcess[str]:
        argv = [str(a) for a in argv]
        with self.lock:
            self.calls.append((argv, kwargs))
        for handler in self.handlers:
            found = handler(argv, kwargs)
            if found is not None:
                code, stdout, stderr = found
                return subprocess.CompletedProcess(argv, code, stdout, stderr)
        raise AssertionError(f"unexpected command: {argv}")

    def fx_run(self, argv: Any, *, check: bool = True, **kwargs: Any) -> Any:
        """docker-daemon-fixture.py ``run``: text output, check raises RuntimeError."""

        result = self.handle(argv, kwargs)
        if check and result.returncode:
            raise RuntimeError(f"tool_failed:{Path(str(argv[0])).name}::{result.returncode}")
        return result

    def subprocess_run(self, argv: Any, *_args: Any, **kwargs: Any) -> Any:
        check = kwargs.pop("check", False)
        result = self.handle(argv, kwargs)
        if check and result.returncode:
            raise subprocess.CalledProcessError(result.returncode, argv, result.stdout)
        if _text_mode(kwargs):
            return result
        return subprocess.CompletedProcess(
            result.args, result.returncode, result.stdout.encode(), result.stderr.encode()
        )

    def popen(self, argv: Any, *_args: Any, **kwargs: Any) -> _Process:
        return _Process(self.handle(argv, kwargs), kwargs)

    def commands(self, program: str) -> list[tuple[list[str], dict[str, Any]]]:
        return [(a, k) for a, k in self.calls if Path(a[0]).name == program]


# A nested dependency version shows that only the top-level version line is rewritten.
CHART_YAML = (
    'apiVersion: v2\nname: curie\nversion: 9.8.7\nappVersion: "9.8.7"\n'
    "dependencies:\n  - name: valkey\n    version: 1.2.3\n"
)
BASE_VERSION = "9.8.7-fdbase"
# The fake snapshot's cli/src/config_migrate.rs, shaped like the real file's declaration.
SNAPSHOT_SCHEMA_VERSION = "7.6.5"
CONFIG_MIGRATE_RS = (
    "use serde_json::Value;\n\n"
    f'pub const TARGET_SCHEMA_VERSION: &str = "{SNAPSHOT_SCHEMA_VERSION}";\n\n'
    "pub fn infer_schema_version() {}\n"
)
SCHEMA_VERSION = "0.9.0"  # an explicit value for the pure docker_values calls


class FakeFixture:
    """The attributes of docker-daemon-fixture.py's Fixture the driver reads."""

    def __init__(self, root: Path, commands: Commands, args: Any) -> None:
        self.args, self._commands = args, commands
        self.work = root / "work"
        self.work.mkdir(mode=0o700)
        self.snapshot = self.work / "snapshot"
        chart = self.snapshot / "charts/curie"
        chart.mkdir(parents=True)
        (chart / "Chart.yaml").write_text(CHART_YAML)
        (chart / "values-dev.yaml").write_text("ui:\n  deploy: false\n")
        (chart / "templates").mkdir()
        (chart / "templates/configmap.yaml").write_text("kind: ConfigMap\n")
        (self.snapshot / "cli/src").mkdir(parents=True)
        (self.snapshot / "cli/src/config_migrate.rs").write_text(CONFIG_MIGRATE_RS)
        (self.snapshot / "runner").mkdir()
        (self.snapshot / "runner/Dockerfile").write_text("FROM scratch\n")
        self.registry, self.port, self.arch = "localhost:5000", "5000", "amd64"
        self.candidate = CANDIDATE
        self.assets: list[tuple[str, str, str]] = []
        self.notes: dict[str, Any] = {}
        self.resolved: list[tuple[str, str]] = []
        self.objects: dict[str, Any] = {}

    def k(self, *argv: str, **kwargs: Any) -> Any:
        context = ["kubectl", "--context", self.args.context, "--request-timeout=15s"]
        return self._commands.fx_run([*context, *argv], **kwargs)

    def get(self, path: str) -> dict[str, Any]:
        return copy.deepcopy(self.objects.get(path.split("?", 1)[0], {}))

    def asset(self, kind: str, name: str, identity: str) -> None:
        self.assets.append((kind, name, identity))

    def note(self, key: str, value: object) -> None:
        self.notes[key] = value

    def meta(self) -> dict[str, Any]:
        return {"cleanup_complete": True, "candidate": CANDIDATE}

    def resolve(self, name: str, ref: str) -> dict[str, str]:
        self.resolved.append((name, ref))
        digest = ref.split("@", 1)[1]
        manifest, config = "sha256:" + "9" * 64, "sha256:" + "8" * 64
        return {"ref": ref, "digest": digest, "manifest": manifest, "config": config}

    def cleanup(self) -> None:
        return None


def _scenario(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, commands: Commands) -> Any:
    """A real DockerScenario over a fake fixture; every subprocess seam goes to ``commands``."""

    args = docker_driver.scenario_args(CONTEXT, STUB_HOST, nonce=NONCE)
    obj = docker_driver.DockerScenario(args, candidate=CANDIDATE)
    obj.f = FakeFixture(tmp_path, commands, args)
    args.work = obj.f.work  # nothing may land outside tmp_path
    obj.images = _images()
    obj.curie = str(tmp_path / "host-curie")
    monkeypatch.setattr(obj.fx, "run", commands.fx_run)
    monkeypatch.setattr(subprocess, "run", commands.subprocess_run)
    monkeypatch.setattr(subprocess, "Popen", commands.popen)
    return obj


# --------------------------------------------------------------------------
# main_run: the factory_e2e entry point
# --------------------------------------------------------------------------


class _Recorder:
    """Stands in for DockerScenario: records its construction and returns ``result``."""

    def __init__(self, result: dict[str, Any]) -> None:
        self.result = result
        self.calls: list[tuple[Any, Any]] = []

    def __call__(self, args: Any, candidate: Any = None, **_kwargs: Any) -> _Recorder:
        self.calls.append((args, candidate))
        return self

    def run(self) -> dict[str, Any]:
        return copy.deepcopy(self.result)


PASSED = {
    "result": "passed",
    "nonce": NONCE,
    "failures": [],
    "envelope": {"schema": "docker-scenario/v1", "nonce": NONCE},
    "cleanup": {},
    "evidence": f".projects/daemon-docker-{NONCE}/docker-evidence.json",
}
FAILED = PASSED | {"result": "failed", "failures": ["not_concurrent: windows do not overlap"]}


def _main_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, result: dict[str, Any] = PASSED
) -> SimpleNamespace:
    for key in list(os.environ):
        if key.startswith("CURIE_FACTORY_"):
            monkeypatch.delenv(key)
    for key in MODEL_ENV:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("CURIE_LOCAL_STUB_HOST", STUB_HOST)
    monkeypatch.setattr(fe.signal, "signal", lambda *_a, **_k: None)
    seen = SimpleNamespace(
        git=[], loader=[], preflight=[], recorder=_Recorder(result), evidence=tmp_path / "e.json"
    )

    def forbidden(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("the docker scenario must not resolve a published candidate")

    monkeypatch.setattr(fe, "_resolve_candidate", forbidden)
    monkeypatch.setattr(fe, "_images_published", forbidden)
    monkeypatch.setattr(
        fe, "resolve_curie_binary", lambda _binary: ("/opt/curie/bin/curie", "curie 0.0.0-test")
    )

    def head(argv: Any) -> str:
        argv = [str(a) for a in argv]
        seen.git.append(argv)
        if argv[0] == "git" and "rev-parse" in argv:
            return HEAD_SHA + "\n"
        raise AssertionError(f"docker main must run no other command: {argv}")

    def run_text(argv: Any, *_a: Any, **_k: Any) -> str:
        return head(argv)

    def run_process(argv: Any, *_a: Any, **kwargs: Any) -> Any:
        out = head(argv)
        stdout = out if _text_mode(kwargs) else out.encode()
        return subprocess.CompletedProcess(argv, 0, stdout, "" if _text_mode(kwargs) else b"")

    monkeypatch.setattr(fe, "run", run_text)
    monkeypatch.setattr(subprocess, "run", run_process)
    monkeypatch.setattr(subprocess, "check_output", lambda argv, **_k: head(argv))
    monkeypatch.setattr(subprocess, "Popen", forbidden)

    loader = docker_driver.load_docker_config

    def spy(env: Any, *, context: str | None) -> Any:
        seen.loader.append(context)
        return loader(env, context=context)

    monkeypatch.setattr(docker_driver, "load_docker_config", spy)
    original = fe.Preflight.run

    def preflight_run(self: Any, scenario: Any) -> None:
        seen.preflight.append(self.scenario_name)
        original(self, scenario)

    monkeypatch.setattr(fe.Preflight, "run", preflight_run)
    monkeypatch.setattr(docker_driver, "DockerScenario", seen.recorder)
    return seen


def _main(seen: SimpleNamespace, *extra: str) -> int:
    argv = ["run", "--scenario", "docker", "--context", CONTEXT, "--evidence", str(seen.evidence)]
    return int(fe.main([*argv, *extra]))


def test_main_run_docker_is_a_registered_whole_flow_scenario() -> None:
    assert "docker" in fe.SCENARIO_NAMES
    assert "docker" in fe.WHOLE_FLOW_SCENARIOS
    assert fe.SCENARIOS["docker"] is not None
    assert not fe.scenario_opens_seed_issue("docker")
    assert docker_driver.SCENARIO == "docker"


def test_main_run_docker_drives_the_driver_with_the_worktree_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen = _main_env(monkeypatch, tmp_path)
    assert _main(seen) == 0
    assert seen.loader == [CONTEXT]
    assert any("rev-parse" in argv for argv in seen.git)
    assert seen.preflight == ["docker"]
    [(args, candidate)] = seen.recorder.calls
    assert candidate == HEAD_SHA
    assert args.context == CONTEXT
    assert args.callback_host == STUB_HOST
    evidence = json.loads(seen.evidence.read_text())
    assert evidence["mode"] == "run:docker"
    assert evidence["candidate_commit"] == HEAD_SHA
    assert evidence["result"] == "passed"
    assert evidence["scenario"]["result"] == "passed"


@pytest.mark.parametrize("candidate", ["HEAD", HEAD_SHA])
def test_main_run_docker_accepts_head_as_the_candidate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, candidate: str
) -> None:
    seen = _main_env(monkeypatch, tmp_path)
    assert _main(seen, "--candidate", candidate) == 0
    assert [c for _a, c in seen.recorder.calls] == [HEAD_SHA]


def test_main_run_docker_takes_the_context_from_the_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen = _main_env(monkeypatch, tmp_path)
    monkeypatch.setenv("CURIE_FACTORY_KUBE_CONTEXT", CONTEXT)
    argv = ["run", "--scenario", "docker", "--evidence", str(seen.evidence)]
    assert fe.main(argv) == 0
    [(args, _candidate)] = seen.recorder.calls
    assert args.context == CONTEXT


def test_main_run_docker_exits_failed_when_the_driver_reports_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    seen = _main_env(monkeypatch, tmp_path, FAILED)
    assert _main(seen) == fe.EXIT_FAILED
    assert seen.recorder.calls
    evidence = json.loads(seen.evidence.read_text())
    assert evidence["result"] == "failed"
    assert "not_concurrent" in evidence["error"]
    assert "not_concurrent" in capsys.readouterr().err


REFUSALS = [
    pytest.param(["--model-base-url", "https://model.example.com"], {}, id="model-base-url"),
    pytest.param(["--candidate-images", "build"], {}, id="candidate-images-build"),
    pytest.param(["--candidate", "f" * 40], {}, id="foreign-candidate-sha"),
    pytest.param(["--candidate", "main"], {}, id="foreign-candidate-ref"),
    pytest.param([], {"CURIE_LOCAL_STUB_HOST": None}, id="stub-host-unset"),
    pytest.param([], {"CURIE_LOCAL_STUB_HOST": "127.0.0.1"}, id="stub-host-loopback"),
    pytest.param([], {"CURIE_LOCAL_STUB_HOST": "stub.example"}, id="stub-host-name"),
    *[pytest.param([], {name: "fixture-value"}, id=name) for name in MODEL_ENV],
]


@pytest.mark.parametrize(("extra", "env"), REFUSALS)
def test_main_run_docker_refuses_unsafe_inputs_before_the_driver(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    extra: list[str],
    env: dict[str, str | None],
) -> None:
    seen = _main_env(monkeypatch, tmp_path)
    for name, value in env.items():
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    assert _main(seen, *extra) == fe.EXIT_CONFIG
    assert seen.recorder.calls == []
    assert "factory-e2e:" in capsys.readouterr().err


def test_main_run_docker_refuses_a_missing_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    seen = _main_env(monkeypatch, tmp_path)
    argv = ["run", "--scenario", "docker", "--evidence", str(seen.evidence)]
    assert fe.main(argv) == fe.EXIT_CONFIG
    assert seen.recorder.calls == []
    assert "context" in capsys.readouterr().err


def test_main_run_help_lists_docker_and_the_docs_name_the_invocation(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exit_info:
        fe.main(["run", "--help"])
    assert exit_info.value.code == 0
    out = capsys.readouterr().out
    choices = re.search(r"--scenario \{([^}]*)\}", out, re.S)
    assert choices is not None, out
    assert "docker" in [name.strip() for name in choices[1].split(",")]
    with pytest.raises(SystemExit) as exit_info:
        fe.main(["--help"])
    assert exit_info.value.code == 0
    top = capsys.readouterr().out
    assert "run --scenario docker" in (fe.__doc__ or "") + top


# --------------------------------------------------------------------------
# install: config, args, values, servers, transcript, images
# --------------------------------------------------------------------------


def test_install_constants_name_the_scenario() -> None:
    assert docker_driver.SLICE == "docker"
    assert docker_driver.RUN_ISSUES == {"a": 4106, "b": 4107}
    assert docker_driver.CUSTOM_REPO == CUSTOM_REPO == dl.REPO
    assert docker_driver.CATALOG_REPO == CATALOG_REPO
    assert docker_driver.DOCKER_AGENT == DOCKER_AGENT
    assert docker_driver.DockerScenario.agent == DOCKER_AGENT
    assert dl.Lifecycle.agent == dl.AGENT
    assert tuple(docker_driver.REQUIRED_CHECKS) == ("E2E required",)
    assert (
        Path(docker_driver.TRANSCRIPT).resolve() == (DOCKER_FIXTURES / "transcript.json").resolve()
    )


@pytest.mark.parametrize("name", MODEL_ENV)
def test_install_load_docker_config_refuses_a_model_credential(name: str) -> None:
    env = {"CURIE_LOCAL_STUB_HOST": STUB_HOST, name: "fixture-value"}
    with pytest.raises(fe.ConfigError):
        docker_driver.load_docker_config(env, context=CONTEXT)


@pytest.mark.parametrize("host", [None, "", "127.0.0.1", "0.0.0.0", "stub.example", "2001:db8::1"])
def test_install_load_docker_config_refuses_an_unroutable_stub_host(host: str | None) -> None:
    env = {} if host is None else {"CURIE_LOCAL_STUB_HOST": host}
    with pytest.raises(fe.ConfigError):
        docker_driver.load_docker_config(env, context=CONTEXT)


@pytest.mark.parametrize("context", [None, "", "  "])
def test_install_load_docker_config_refuses_a_missing_context(context: str | None) -> None:
    with pytest.raises(fe.ConfigError):
        docker_driver.load_docker_config({"CURIE_LOCAL_STUB_HOST": STUB_HOST}, context=context)


def test_install_load_docker_config_accepts_a_routable_stub_host_without_credentials() -> None:
    env = {"CURIE_LOCAL_STUB_HOST": STUB_HOST, "PATH": "/usr/bin"}
    config = docker_driver.load_docker_config(env, context=CONTEXT)
    assert isinstance(config, fe.FactoryConfig)
    assert config.kube_context == CONTEXT
    assert config.repo == dl.REPO
    assert config.model_api_key is None
    assert config.app_id == ""
    assert config.installation_id == 0
    assert config.private_key_file == Path("/dev/null")
    assert config.webhook_secret == "" and config.actor_token == ""


def test_install_load_docker_config_takes_the_context_from_the_environment() -> None:
    env = {"CURIE_LOCAL_STUB_HOST": STUB_HOST, "CURIE_FACTORY_KUBE_CONTEXT": CONTEXT}
    assert docker_driver.load_docker_config(env, context=None).kube_context == CONTEXT


def test_install_scenario_args_own_the_docker_slice() -> None:
    args = docker_driver.scenario_args(CONTEXT, STUB_HOST, nonce=NONCE)
    assert args.context == CONTEXT
    assert args.namespace == args.release == NAMESPACE
    assert args.daemon_namespace == DAEMON_NAMESPACE
    assert args.controller_namespace == CONTROLLER_NAMESPACE
    assert args.work_dir == f".projects/daemon-docker-{NONCE}"
    assert args.candidate == "HEAD"
    assert args.slice == "docker" and args.nonce == NONCE
    assert args.callback_host == STUB_HOST
    fresh = [docker_driver.scenario_args(CONTEXT, STUB_HOST) for _ in range(2)]
    assert all(re.fullmatch(r"[0-9a-f]{8}", a.nonce) for a in fresh)
    assert all(a.namespace == f"fd-docker-{a.nonce}" for a in fresh)


def _base_values() -> dict[str, Any]:
    return dict(
        dl.sk.scripted_values(
            model_base_url="http://192.0.2.20:8080",
            github_api="https://198.51.100.7:8443/api/v3",
            clone_base="https://198.51.100.7:8443",
            ca_configmap="github-stub-ca",
            runner_image="localhost:5000/fd-e8ce1a8c/platform",
        )
    )


def _lifecycle_values(*, docker: bool) -> dict[str, Any]:
    return dict(
        dl.lifecycle_values(
            _base_values(),
            _images(),
            nonce=NONCE,
            daemon_namespace=DAEMON_NAMESPACE,
            release=NAMESPACE,
            docker=docker,
        )
    )


def _repos(values: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Every factory.bindings[*].repos[*] entry by path, in binding order."""

    bindings = values["factory"]["bindings"]
    return {repo["path"]: repo for binding in bindings for repo in binding["repos"]}


@pytest.mark.parametrize("docker", [True, False], ids=["opted-in", "control"])
def test_install_docker_values_mark_acme_bot_custom_and_add_the_catalog_repository(
    docker: bool,
) -> None:
    source = _lifecycle_values(docker=docker)
    snapshot = copy.deepcopy(source)
    images = _images()
    values = docker_driver.docker_values(source, images, SCHEMA_VERSION)
    assert source == snapshot, "docker_values mutated its input"
    # A native GitHub binding holds exactly its own repository
    # (apps/api/src/curie_api/forges/config.py _check_native), so the catalog
    # repository gets a second binding and binding 0 keeps only acme-bot.
    first, second = values["factory"]["bindings"]
    assert first == snapshot["factory"]["bindings"][0] | {"repos": first["repos"]}
    assert [repo["path"] for repo in first["repos"]] == [CUSTOM_REPO]
    assert {k: second[k] for k in ("name", "tracker", "trackerScopeId")} == {
        "name": "acme-catalog",
        "tracker": first["tracker"],
        "trackerScopeId": "4402",
    }
    assert [repo["path"] for repo in second["repos"]] == [CATALOG_REPO]
    assert second["repos"][0]["projectId"] == second["trackerScopeId"]
    assert second["name"] != first["name"]
    assert second["trackerScopeId"] != first["trackerScopeId"]
    repos = _repos(values)
    assert list(repos) == [CUSTOM_REPO, CATALOG_REPO]
    # upgrade_section reads runner images across every binding.
    assert docker_scenario.runner_image(values, CATALOG_REPO) == images["runner"]["ref"]
    assert docker_scenario.runner_image(values, CUSTOM_REPO) == images["runner"]["ref"]
    custom, catalog = repos[CUSTOM_REPO], repos[CATALOG_REPO]
    # acme-bot keeps its receipt and its docker opt-in; only the Dockerfile key is added.
    original = _repos(snapshot)[CUSTOM_REPO]
    expected_custom = copy.deepcopy(original)
    expected_custom["runner"]["dockerfile"] = "Dockerfile"
    assert custom == expected_custom
    assert ("docker" in custom) is docker
    assert {k: catalog[k] for k in ("alias", "codeHost", "projectId")} == {
        "alias": "acme-catalog",
        "codeHost": "github",
        "projectId": "4402",
    }
    assert "docker" not in catalog
    runner = catalog["runner"]
    assert "dockerfile" not in runner
    assert runner["image"] == images["runner"]["ref"]
    assert runner["base"] == images["platform"]["digest"]
    assert runner["platformDigests"] == original["runner"]["platformDigests"]
    assert runner["tools"] == ["docker-cli"]


def _chart_snake(value: Any) -> Any:
    """charts/curie/templates/_helpers.tpl curie.factory.snake: keys to snake_case
    (interval_seconds -> interval_s), an ``*Id`` value as a string, and the
    ``components`` and ``reviewFeedbackAllowlist`` maps kept as they are."""

    if isinstance(value, list):
        return [_chart_snake(item) for item in value]
    if not isinstance(value, dict):
        return value
    out: dict[str, Any] = {}
    for key, child in value.items():
        name = re.sub(r"(?<!^)(?=[A-Z])", "_", key).lower()
        name = name.replace("interval_seconds", "interval_s")
        if key in ("components", "reviewFeedbackAllowlist"):
            out[name] = child
        elif key.endswith("Id"):
            out[name] = str(child)
        else:
            out[name] = _chart_snake(child)
    return out


def _chart_forges(factory: dict[str, Any], private_key: str) -> dict[str, Any]:
    """The CURIE_FORGES document curie.factory.forgesJson renders from ``factory``, after
    the API loader has read each credential file (apps/api/src/curie_api/forges/loader.py);
    the key file's content is given directly instead of read from a mounted Secret."""

    secrets = ("existingSecret", "existingSecretKey", "privateKey", "clientSecret", "token")
    document: dict[str, Any] = {"code_hosts": [], "trackers": [], "card_base_url": ""}
    document["bindings"] = _chart_snake(factory.get("bindings") or [])
    for key, section in (("codeHosts", "code_hosts"), ("trackers", "trackers")):
        for entry in factory.get(key) or []:
            kept = {k: v for k, v in entry.items() if k not in ("egress", "credential")}
            out = _chart_snake(kept)
            raw = entry["credential"]
            credential = _chart_snake({k: v for k, v in raw.items() if k not in secrets})
            credential["type"] = raw.get("type") or "github_app"
            credential["private_key"] = private_key
            document[section].append(out | {"credential": credential})
    return document


def test_install_docker_values_bindings_load_in_the_api_forge_config() -> None:
    from curie_api.forges.config import ForgesConfig

    values = docker_driver.docker_values(_lifecycle_values(docker=True), _images(), SCHEMA_VERSION)
    document = _chart_forges(values["factory"], "fixture-private-key")
    config = ForgesConfig.model_validate(document)
    assert [binding.name for binding in config.bindings] == ["acme-bot", "acme-catalog"]
    by_name = {binding.name: binding for binding in config.bindings}
    custom, catalog = by_name["acme-bot"].repos, by_name["acme-catalog"].repos
    assert [repo.path for repo in custom] == [CUSTOM_REPO]
    assert [repo.path for repo in catalog] == [CATALOG_REPO]
    assert custom[0].runner is not None and custom[0].runner.dockerfile == "Dockerfile"
    assert catalog[0].runner is not None and catalog[0].runner.dockerfile is None
    assert catalog[0].project_id == by_name["acme-catalog"].tracker_scope_id == "4402"


def test_install_api_forge_config_refuses_two_repositories_in_one_github_binding() -> None:
    """Liveness of the check above: the shape before the fix is refused by the same model."""

    from curie_api.forges.config import ForgesConfig

    values = docker_driver.docker_values(_lifecycle_values(docker=True), _images(), SCHEMA_VERSION)
    first, second = values["factory"]["bindings"]
    merged = copy.deepcopy(values["factory"])
    merged["bindings"] = [first | {"repos": [*first["repos"], *second["repos"]]}]
    document = _chart_forges(merged, "fixture-private-key")
    with pytest.raises(ValueError, match="exactly its own repository"):
        ForgesConfig.model_validate(document)


def test_install_docker_values_raise_the_daemon_limits_for_a_skill_rung() -> None:
    values = docker_driver.docker_values(_lifecycle_values(docker=True), _images(), SCHEMA_VERSION)
    daemon = values["factory"]["docker"]
    assert daemon["maxClaimLifetimeSeconds"] == 3600
    assert daemon["storageSizeLimit"] == "24Gi"
    limits, requests = daemon["resources"]["limits"], daemon["resources"]["requests"]
    assert limits["memory"] == "4Gi"
    assert limits["ephemeral-storage"] == "32Gi"
    assert limits["cpu"] == "2"
    assert requests["ephemeral-storage"] == "8Gi"
    assert daemon["namespace"] == DAEMON_NAMESPACE


def test_install_docker_values_size_factory_sandboxes_at_4gi() -> None:
    source = _lifecycle_values(docker=True)
    snapshot = copy.deepcopy(source)
    values = docker_driver.docker_values(source, _images(), SCHEMA_VERSION)
    assert source == snapshot, "docker_values mutated its input"
    factory = values["agentSandbox"]["resourceProfiles"]["factory"]
    assert factory == {"resources": {"requests": {"memory": "4Gi"}, "limits": {"memory": "4Gi"}}}
    daemon_resources = values["factory"]["docker"]["resources"]
    assert daemon_resources["limits"] == {"memory": "4Gi", "ephemeral-storage": "32Gi", "cpu": "2"}
    assert daemon_resources["requests"]["ephemeral-storage"] == "8Gi"
    assert values["agentSandbox"]["runner"] == source["agentSandbox"]["runner"]


def test_install_values_method_is_docker_values_over_the_lifecycle_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj = _scenario(tmp_path, monkeypatch, Commands())
    obj.base_values = _base_values()
    for docker in (True, False):
        lifecycle = _lifecycle_values(docker=docker)
        expected = docker_driver.docker_values(lifecycle, _images(), SNAPSHOT_SCHEMA_VERSION)
        values = obj.values(docker=docker)
        assert values == expected
        # The snapshot's constant, not this repository's: a release installed by the CLI
        # carries it, and the upgrade's config migration accepts only it as a source.
        assert values["config"]["schemaVersion"] == SNAPSHOT_SCHEMA_VERSION


def test_install_docker_values_stamp_the_config_schema_version() -> None:
    source = _lifecycle_values(docker=True)
    source["config"] = {"logLevel": "debug", "schemaVersion": ""}
    snapshot = copy.deepcopy(source)
    values = docker_driver.docker_values(source, _images(), SCHEMA_VERSION)
    assert source == snapshot, "docker_values mutated its input"
    assert values["config"] == {"logLevel": "debug", "schemaVersion": SCHEMA_VERSION}
    bare = _lifecycle_values(docker=True)
    bare.pop("config", None)
    assert docker_driver.docker_values(bare, _images(), SCHEMA_VERSION)["config"] == {
        "schemaVersion": SCHEMA_VERSION
    }


@pytest.mark.parametrize(
    ("release", "expected"),
    [
        # cli/src/ops/verbs.rs chart_fullname_suffix_cases
        pytest.param("curie", "curie", id="contains_curie_exact"),
        pytest.param("curieish", "curieish", id="contains_curie_prefix"),
        pytest.param("my-curie-prod", "my-curie-prod", id="contains_curie_middle"),
        pytest.param("platform", "platform-curie", id="no_curie_platform"),
        pytest.param("acme-prod", "acme-prod-curie", id="no_curie_acme_prod"),
        pytest.param(NAMESPACE, FULL, id="docker_release"),
        # release_fullname_from_values: trunc 63, then trimSuffix removes exactly one dash.
        pytest.param("a" * 70, "a" * 63, id="truncated_to_63"),
        pytest.param("a" * 62 + "--b-curie", "a" * 62, id="one_dash_trimmed"),
        pytest.param("a" * 61 + "--b-curie", "a" * 61 + "-", id="only_one_dash_trimmed"),
        pytest.param("a" * 58 + "x", "a" * 58 + "x-cur", id="suffix_truncated"),
    ],
)
def test_install_chart_fullname_mirrors_the_cli(release: str, expected: str) -> None:
    assert docker_driver.chart_fullname(release) == expected


def test_install_docker_scenario_full_is_the_chart_fullname(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj = _scenario(tmp_path, monkeypatch, Commands())
    assert obj.full == docker_driver.chart_fullname(obj.release) == FULL
    assert len(FULL) == 24


def test_install_docker_values_drop_the_fullname_override_and_rename_the_agent_keys() -> None:
    source = _lifecycle_values(docker=True)
    assert source["fullnameOverride"] == f"fd-{NONCE}"
    egress = source["factory"]["sandboxEgress"]
    runner_env = source["agentSandbox"]["runnerEnv"]
    assert list(egress) == [dl.AGENT] and list(runner_env) == [dl.AGENT]
    snapshot = copy.deepcopy(source)
    values = docker_driver.docker_values(source, _images(), SCHEMA_VERSION)
    assert source == snapshot, "docker_values mutated its input"
    assert "fullnameOverride" not in values
    assert values["factory"]["sandboxEgress"] == {DOCKER_AGENT: egress[dl.AGENT]}
    assert values["agentSandbox"]["runnerEnv"] == {DOCKER_AGENT: runner_env[dl.AGENT]}
    for key in ("ui", "resourceQuota", "priorityClasses", "api", "worker"):
        assert values[key] == source[key], key


def _longest_pool_name(fullname: str, agent: str) -> str:
    """The chart's repository runner pool name (charts/curie/templates/_helpers.tpl), whose
    image part is a 12-hex digest prefix; the chart refuses one over 63 characters."""

    return f"{fullname}-agent-{agent}-img-{'0' * 12}-runner-pool"


def test_install_repository_runner_pool_names_fit_63_characters() -> None:
    values = docker_driver.docker_values(_lifecycle_values(docker=True), _images(), SCHEMA_VERSION)
    assert "fullnameOverride" not in values
    agents = set(values["agentSandbox"]["runnerEnv"]) | {docker_driver.DockerScenario.agent}
    fullname = docker_driver.chart_fullname(NAMESPACE)
    longest = max((_longest_pool_name(fullname, agent) for agent in agents), key=len)
    assert len(longest) <= 63, longest
    # daemon_lifecycle keeps its override and its agent, and still fits.
    lifecycle = _lifecycle_values(docker=True)
    own = _longest_pool_name(lifecycle["fullnameOverride"], dl.Lifecycle.agent)
    assert len(own) <= 63, own


def _deploy(obj: Any, commands: Commands, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Run the inherited deploy() against fakes; return what it sent."""

    sent: dict[str, Any] = {"agents": [], "cli": []}
    secret = f"/api/v1/namespaces/{obj.ns}/secrets/{obj.full}-secrets"
    encoded = base64.b64encode(API_KEY.encode()).decode()
    meta = {"name": f"{obj.full}-secrets", "namespace": obj.ns}
    obj.f.objects[secret] = {"metadata": meta, "data": {"apiKey": encoded}}
    bundle = obj.f.snapshot / "examples/dark-factory"
    bundle.mkdir(parents=True)
    (bundle / "plugin.json").write_text("{}\n")

    def http_json(method: str, url: str, **kwargs: Any) -> tuple[int, Any]:
        sent["agents"].append((method, url, kwargs.get("body")))
        return 201, {}

    def cli(argv: list[str], _kwargs: dict[str, Any]) -> Result | None:
        if argv[0] == obj.curie:
            sent["cli"].append(argv)
            return 0, "", ""
        return None

    commands.handlers.append(cli)
    monkeypatch.setattr(fe, "http_json", http_json)
    monkeypatch.setattr(obj, "port_forward", lambda: "http://localhost:18000")
    monkeypatch.setattr(obj, "github_token", lambda: "example-installation-token")
    obj.deploy()
    return sent


def _deployed_agent(sent: dict[str, Any]) -> tuple[str, str]:
    [(method, url, body)] = sent["agents"]
    assert (method, url) == ("POST", "http://localhost:18000/agents")
    [deploy] = [argv for argv in sent["cli"] if "deploy" in argv]
    return body["name"], deploy[deploy.index("--agent") + 1]


def test_install_deploy_binds_the_docker_scenario_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    commands = Commands()
    obj = _scenario(tmp_path, monkeypatch, commands)
    assert _deployed_agent(_deploy(obj, commands, monkeypatch)) == (DOCKER_AGENT, DOCKER_AGENT)


def test_install_deploy_of_daemon_lifecycle_keeps_its_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    commands = Commands()
    args = docker_driver.scenario_args(CONTEXT, STUB_HOST, nonce=NONCE)
    obj = dl.Lifecycle(args)
    obj.f = FakeFixture(tmp_path, commands, args)
    obj.images, obj.curie = _images(), str(tmp_path / "host-curie")
    monkeypatch.setattr(obj.fx, "run", commands.fx_run)
    monkeypatch.setattr(subprocess, "run", commands.subprocess_run)
    monkeypatch.setattr(subprocess, "Popen", commands.popen)
    assert obj.full == f"fd-{NONCE}"
    assert _deployed_agent(_deploy(obj, commands, monkeypatch)) == (dl.AGENT, dl.AGENT)


def _rust_constant(path: Path) -> str:
    """TARGET_SCHEMA_VERSION read independently of the driver's parser."""

    prefix = "pub const TARGET_SCHEMA_VERSION: &str = "
    [line] = [line for line in path.read_text().splitlines() if line.startswith(prefix)]
    return line.removeprefix(prefix).removesuffix(";").strip('"')


def test_install_target_schema_version_reads_the_repository_constant() -> None:
    expected = _rust_constant(REPO_ROOT / "cli/src/config_migrate.rs")
    assert re.fullmatch(r"\d+\.\d+\.\d+", expected), expected
    assert docker_driver.target_schema_version(REPO_ROOT) == expected


def test_install_target_schema_version_reads_the_snapshot_file(tmp_path: Path) -> None:
    source = tmp_path / "cli/src/config_migrate.rs"
    source.parent.mkdir(parents=True)
    source.write_text(CONFIG_MIGRATE_RS)
    assert docker_driver.target_schema_version(tmp_path) == SNAPSHOT_SCHEMA_VERSION


@pytest.mark.parametrize(
    "text",
    [
        pytest.param("", id="empty"),
        pytest.param('const OTHER: &str = "1.0.0";\n', id="other-constant"),
        pytest.param("// pub const TARGET_SCHEMA_VERSION_OLD: u32 = 1;\n", id="lookalike"),
    ],
)
def test_install_target_schema_version_refuses_a_file_without_the_constant(
    tmp_path: Path, text: str
) -> None:
    source = tmp_path / "cli/src/config_migrate.rs"
    source.parent.mkdir(parents=True)
    source.write_text(text)
    with pytest.raises(RuntimeError, match="target_schema_version_missing"):
        docker_driver.target_schema_version(tmp_path)


# The fixture's images() writes the catalog Dockerfile acme-bot's runner was built from.
CATALOG_DOCKERFILE = "FROM localhost:5000/fd-e8ce1a8c/platform@sha256:" + "2" * 64 + "\n"


def _servers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, catalog: bool = True
) -> tuple[Any, dict[str, Any]]:
    """A DockerScenario whose stub, model and proxy classes only record their inputs."""

    obj = _scenario(tmp_path, monkeypatch, Commands())
    if catalog:
        (obj.f.work / "Catalog.Dockerfile").write_text(CATALOG_DOCKERFILE)
    seen: dict[str, Any] = {}

    class Stub:
        base_url, unknown_requests = f"https://{STUB_HOST}:4443", []

        def __init__(self, root: Path, recording: dict[str, Any], **seeds: Any) -> None:
            seen["stub"] = (root, recording.get("version"), seeds)

        def start(self) -> None:
            seen["stub_started"] = True

        def _get(self, kind: str, number: int) -> dict[str, Any]:
            return {"number": number, "labels": [{"name": "factory"}]}

        def _put(self, kind: str, number: int, body: dict[str, Any]) -> None:
            seen.setdefault("put", []).append((kind, number, body))

    class Model:
        base_url, unexpected = f"http://{STUB_HOST}:4444", []

        def __init__(self, transcript: Path, *, host: str) -> None:
            seen["model"] = (Path(transcript), host)

        def start(self) -> None:
            seen["model_started"] = True

    class Proxy:
        def __init__(self, host: str, upstream: str) -> None:
            seen["proxy"] = (host, upstream)
            self.base_url = f"http://{host}:4445"

    obj.github = SimpleNamespace(GithubStub=Stub)
    obj.models = SimpleNamespace(ModelScript=Model)
    monkeypatch.setattr(dl, "ModelProxy", Proxy)
    if hasattr(docker_driver, "ModelProxy"):
        monkeypatch.setattr(docker_driver, "ModelProxy", Proxy)
    seen["model_base_url"] = Model.base_url
    return obj, seen


def test_install_start_servers_load_the_docker_scenario_transcript(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj, seen = _servers(tmp_path, monkeypatch)
    obj.start_servers()
    transcript, host = seen["model"]
    assert transcript.resolve() == (DOCKER_FIXTURES / "transcript.json").resolve()
    assert transcript.resolve() != dl.TRANSCRIPT.resolve()
    assert host == STUB_HOST
    assert seen["model_started"] and seen["stub_started"]
    assert seen["proxy"] == (STUB_HOST, seen["model_base_url"])


def _files(root: Path) -> dict[str, bytes]:
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def test_install_start_servers_seed_acme_bot_with_its_declared_dockerfile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj, seen = _servers(tmp_path, monkeypatch)
    source = _files(dl.SEED_TREE)
    obj.start_servers()
    _root, _version, seeds = seen["stub"]
    seed_tree = obj.f.work / "seed-tree"
    assert Path(seeds["seed_tree"]).resolve() == seed_tree.resolve()
    copied = _files(seed_tree)
    assert copied.pop("Dockerfile").decode() == CATALOG_DOCKERFILE
    assert copied == source, "the seed tree must be the unitconv fixture plus the Dockerfile"
    assert _files(dl.SEED_TREE) == source, "the shared fixture must not change"
    assert "Dockerfile" not in source


def test_install_start_servers_refuse_without_the_catalog_dockerfile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj, seen = _servers(tmp_path, monkeypatch, catalog=False)
    with pytest.raises(RuntimeError, match="custom_dockerfile_missing"):
        obj.start_servers()
    assert "stub" not in seen


def test_install_chart_of_daemon_lifecycle_is_the_snapshot_chart(tmp_path: Path) -> None:
    fixture = SimpleNamespace(snapshot=tmp_path / "snapshot")
    lifecycle = SimpleNamespace(f=fixture)
    chart = dl.Lifecycle.install_chart(lifecycle)
    assert Path(chart) == tmp_path / "snapshot" / "charts/curie"


def test_install_chart_of_the_docker_scenario_is_a_prerelease_base_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj = _scenario(tmp_path, monkeypatch, Commands())
    snapshot_chart = obj.f.snapshot / "charts/curie"
    chart = Path(obj.install_chart())
    assert chart.resolve() == (obj.f.work / "chart-base").resolve()
    text = (chart / "Chart.yaml").read_text()
    assert f"\nversion: {BASE_VERSION}\n" in text
    assert '\nappVersion: "9.8.7"\n' in text
    assert "\n    version: 1.2.3\n" in text, "only the top-level version line changes"
    assert (snapshot_chart / "Chart.yaml").read_text() == CHART_YAML
    expected = _files(snapshot_chart)
    expected["Chart.yaml"] = text.encode()
    assert _files(chart) == expected
    assert Path(obj.install_chart()).resolve() == chart.resolve(), "copied once"


def test_install_chart_refuses_a_chart_without_a_version_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj = _scenario(tmp_path, monkeypatch, Commands())
    (obj.f.snapshot / "charts/curie/Chart.yaml").write_text("apiVersion: v2\nname: curie\n")
    with pytest.raises(RuntimeError, match="chart_version_missing"):
        obj.install_chart()


class _Stop(Exception):
    """Raised by the first rollouts: the chart choice is already recorded by then."""


def test_install_and_control_use_the_base_chart_while_the_upgrade_uses_the_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj, fake, commands = _upgrade(tmp_path, monkeypatch)

    def label(argv: list[str], _kwargs: dict[str, Any]) -> Result | None:
        kubectl = Path(argv[0]).name == "kubectl"
        return (0, "", "") if kubectl and _strip_kubectl(argv)[:1] == ["label"] else None

    commands.handlers.append(label)
    helm: list[tuple[str, tuple[str, ...]]] = []
    rollouts: list[tuple[str, ...]] = []

    def rollout(*components: str) -> None:
        rollouts.append(components)
        if len(rollouts) <= 2:
            raise _Stop

    monkeypatch.setattr(obj, "helm", lambda verb, *argv: helm.append((verb, argv)))
    monkeypatch.setattr(obj, "rollout", rollout)
    monkeypatch.setattr(obj, "start_servers", lambda: None)
    monkeypatch.setattr(obj, "values", lambda *, docker: {"factory": {"bindings": []}})
    monkeypatch.setattr(obj, "secret_inputs", lambda: {})
    obj.stub = SimpleNamespace(base_url=f"https://{STUB_HOST}:4443", unknown_requests=[])
    obj.proxy = SimpleNamespace(base_url=f"http://{STUB_HOST}:4445")
    base = str(obj.f.work / "chart-base")
    with pytest.raises(_Stop):
        obj.install()
    with pytest.raises(_Stop):
        obj.scenario_control()
    [(install_verb, install), (upgrade_verb, control)] = helm
    assert (install_verb, upgrade_verb) == ("install", "upgrade")
    assert install[0] == base
    assert _adjacent(list(install), "-f", base + "/values-dev.yaml")
    assert control[0] == base
    obj.scenario_upgrade()
    [(argv, _values, _mode)] = fake.upgrade
    snapshot = str(obj.f.snapshot / "charts/curie")
    assert _adjacent(argv, "--chart", snapshot)
    assert _adjacent(argv, "--to", "9.8.7")
    assert snapshot != base


def test_install_transcript_runs_the_two_docker_lanes_then_the_lifecycle_verbatim() -> None:
    exchanges = model_script.load_transcript(docker_driver.TRANSCRIPT)
    lifecycle = model_script.load_transcript(dl.TRANSCRIPT)
    lanes, rest = exchanges[:14], exchanges[14:]
    assert rest == lifecycle
    assert [e["ordinal"] for e in lanes] == [*range(7), *range(7)]
    assert {e["lane_key"] for e in lanes} == {lifecycle[0]["lane_key"]}
    for exchange in lanes:
        message = exchange["message"]
        if exchange["ordinal"] == 6:
            assert message["stop_reason"] == "end_turn"
            assert message["content"] == [{"type": "text", "text": "Docker scenario run finished."}]
            continue
        assert message["stop_reason"] == "tool_use"
        [block] = message["content"]
        assert block["type"] == "tool_use" and block["name"] == "Bash"
        assert block["input"]["timeout"] == 600000
        assert "/tmp/fd-release" in block["input"]["command"]
        assert "docker info" in block["input"]["command"]


def test_install_collector_images_are_pinned_like_their_sources() -> None:
    lifecycle_text = dl.TRANSCRIPT.read_text()
    busybox = re.search(r"busybox:[^\s\"]+@sha256:[0-9a-f]{64}", lifecycle_text)
    assert busybox is not None
    assert docker_driver.PROBE_IMAGE == busybox[0]
    assert docker_driver.COMPOSE_IMAGE.startswith("python:3.13.15-slim-bookworm@sha256:")
    assert f"FROM {docker_driver.COMPOSE_IMAGE}" in (REPO_ROOT / "runner/Dockerfile").read_text()


def _buildx(digest: str) -> Handler:
    def handle(argv: list[str], _kwargs: dict[str, Any]) -> Result | None:
        if argv[:4] == ["docker", "image", "ls", "--format"]:
            return 0, "", ""
        if argv[:3] == ["docker", "buildx", "build"]:
            metadata = _flag(argv, "--metadata-file")
            assert metadata is not None
            Path(metadata).write_text(json.dumps({"containerimage.digest": digest}))
            return 0, "", ""
        return None

    return handle


def test_install_build_platform_next_pushes_a_labelled_second_platform(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    commands = Commands()
    obj = _scenario(tmp_path, monkeypatch, commands)
    obj.builder = BUILDER  # what prepare() records after creating the run-owned builder
    del obj.images["platform-next"]
    commands.handlers.append(_buildx(NEXT_DIGEST))
    obj.build_platform_next()
    [(argv, _kwargs)] = [c for c in commands.calls if c[0][:3] == ["docker", "buildx", "build"]]
    tag = f"localhost:5000/fd-{NONCE}/platform-next:candidate"
    # The production catalog rebuild builds both platforms and refuses the default
    # docker driver for a multi-platform push (cli/src/repo_runner_build.rs).
    assert _adjacent(argv, "--builder", BUILDER)
    assert _adjacent(argv, "--platform", "linux/amd64,linux/arm64")
    assert "--push" in argv
    assert _adjacent(argv, "--label", "curie.fixture.platform=next")
    assert _adjacent(argv, "-t", tag)
    assert _adjacent(argv, "-f", str(obj.f.snapshot / "runner/Dockerfile"))
    assert argv[-1] == str(obj.f.snapshot)
    assert not {"-v", "--volume", "--mount"} & set(argv)
    assert [a for a in obj.f.assets if a[0] == "tag_intent"][0][1] == tag
    ref = f"localhost:5000/fd-{NONCE}/platform-next@{NEXT_DIGEST}"
    assert obj.f.resolved == [("platform-next", ref)]
    assert obj.images["platform-next"]["digest"] == NEXT_DIGEST
    assert obj.images["platform-next"]["ref"] == ref


def test_install_build_platform_next_refuses_the_platform_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    commands = Commands()
    obj = _scenario(tmp_path, monkeypatch, commands)
    obj.builder = BUILDER
    del obj.images["platform-next"]
    commands.handlers.append(_buildx(obj.images["platform"]["digest"]))
    with pytest.raises(RuntimeError, match="platform_next_not_distinct"):
        obj.build_platform_next()


class _Builders:
    """`docker buildx inspect|create|rm` for the run-owned builder on the default engine."""

    def __init__(self, existing: bool = False) -> None:
        self.existing, self.created = existing, False
        self.calls: list[tuple[str, list[str]]] = []

    def handle(self, argv: list[str], _kwargs: dict[str, Any]) -> Result | None:
        verb = argv[2] if argv[:2] == ["docker", "buildx"] and len(argv) > 2 else ""
        if verb == "inspect":
            self.calls.append(("inspect", argv))
            if self.existing or self.created:
                return 0, f"Name:          {BUILDER}\nDriver:        docker-container\n", ""
            # docker buildx's stderr for an unknown builder name.
            return 1, "", f'ERROR: no builder "{BUILDER}" found\n'
        if verb == "create":
            self.calls.append(("create", argv))
            self.created = True
            return 0, BUILDER + "\n", ""
        if verb == "rm":
            self.calls.append(("rm", argv))
            return 0, "", ""
        return None


def _prepare(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, builders: _Builders
) -> tuple[Any, list[str]]:
    """A DockerScenario whose inherited prepare and image builds only record their order."""

    commands = Commands()
    order: list[str] = []

    def record(argv: list[str], kwargs: dict[str, Any]) -> Result | None:
        found = builders.handle(argv, kwargs)
        if found is not None:
            order.append("buildx " + argv[2])
        return found

    commands.handlers.append(record)
    obj = _scenario(tmp_path, monkeypatch, commands)
    monkeypatch.setattr(dl.Lifecycle, "prepare", lambda _self: order.append("super"))
    monkeypatch.setattr(obj, "build_platform_next", lambda: order.append("platform_next"))
    monkeypatch.setattr(obj, "build_runner_cli", lambda: order.append("runner_cli"))
    return obj, order


def test_install_prepare_creates_a_run_owned_buildx_builder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    builders = _Builders()
    obj, order = _prepare(tmp_path, monkeypatch, builders)
    obj.prepare()
    [create] = [argv for verb, argv in builders.calls if verb == "create"]
    assert _adjacent(create, "--name", BUILDER)
    assert _adjacent(create, "--driver", "docker-container")
    assert _adjacent(create, "--driver-opt", "network=host")
    assert not {"-v", "--volume", "--mount"} & set(create)
    inspected = [argv for verb, argv in builders.calls if verb == "inspect"]
    assert inspected and BUILDER in inspected[0]
    assert order.index("buildx inspect") < order.index("buildx create")
    assert order.index("buildx create") < order.index("platform_next")
    assert "super" in order and "runner_cli" in order
    assert obj.builder == BUILDER


def test_install_prepare_refuses_an_existing_builder_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    builders = _Builders(existing=True)
    obj, order = _prepare(tmp_path, monkeypatch, builders)
    with pytest.raises(RuntimeError, match="builder_name_occupied"):
        obj.prepare()
    assert [verb for verb, _argv in builders.calls if verb == "create"] == []
    assert "platform_next" not in order


def test_install_build_runner_cli_records_intents_and_copies_out_without_a_bind_mount(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    commands = Commands()
    obj = _scenario(tmp_path, monkeypatch, commands)
    container = "c0ffee" * 10 + "abcd"
    dockerfiles: list[str] = []

    def docker(argv: list[str], _kwargs: dict[str, Any]) -> Result | None:
        if argv[0] != "docker":
            return None
        if argv[1] == "images" or argv[1:3] == ["image", "ls"]:
            return 0, "", ""  # tag occupancy check: nothing carries the tag yet
        if argv[1] == "build" or argv[1:3] == ["buildx", "build"]:
            dockerfile = _flag(argv, "-f") or _flag(argv, "--file")
            assert dockerfile is not None
            dockerfiles.append(dockerfile)
            dockerfiles.append(Path(dockerfile).read_text())
            assert any(a[0] == "tag_intent" for a in obj.f.assets), "build before tag_intent"
            return 0, "sha256:" + "d" * 64 + "\n", ""
        if argv[1] == "create" or argv[1:3] == ["container", "create"]:
            intent = any(a[0] == "container_intent" for a in obj.f.assets)
            assert intent, "container created before its container_intent"
            return 0, container + "\n", ""
        if argv[1] == "cp":
            dest = Path(argv[-1])
            target = dest / "curie" if dest.is_dir() else dest
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("#!/bin/sh\n")
            return 0, "", ""
        if argv[1] in ("rm", "rmi") or argv[1:3] in (["image", "rm"], ["container", "rm"]):
            return 0, "", ""
        return None

    commands.handlers.append(docker)
    obj.build_runner_cli()
    calls = [argv for argv, _k in commands.commands("docker")]
    verbs = [
        " ".join(argv[1:3]) if argv[1] in ("image", "container") else argv[1] for argv in calls
    ]
    tag = f"fd-{NONCE}-runner-cli:candidate"
    name = f"fd-{NONCE}-runner-cli"
    build = calls[verbs.index("build")]
    assert _adjacent(build, "--label", "owner=factory-tools")
    assert _adjacent(build, "--label", f"factory-tools/nonce={NONCE}")
    assert _adjacent(build, "-t", tag)
    # Both intents are recorded before the object exists, so the fixture's
    # release_docker_assets reclaims them on any failure.
    [(_kind, _tag, tag_request)] = [a for a in obj.f.assets if a[:2] == ("tag_intent", tag)]
    assert _adjacent(build, "--label", f"factory-tools/request={tag_request}")
    intents = [a for a in obj.f.assets if a[:2] == ("container_intent", name)]
    [(_kind, _name, container_request)] = intents
    assert build[-1] == str(obj.f.snapshot)
    dockerfile_path, dockerfile = Path(dockerfiles[0]), dockerfiles[1]
    assert dockerfile_path.resolve().is_relative_to(obj.f.work.resolve())
    assert not dockerfile_path.resolve().is_relative_to(obj.f.snapshot.resolve())
    assert docker_driver.CLI_BUILD_IMAGE.startswith("rust:")
    assert "bookworm" in docker_driver.CLI_BUILD_IMAGE
    assert f"FROM {docker_driver.CLI_BUILD_IMAGE}" in dockerfile
    assert "COPY . /src" in dockerfile
    cargo = "cargo build --locked --release --bin curie --manifest-path /src/cli/Cargo.toml"
    assert cargo in dockerfile
    create = [v for v in verbs if v in ("create", "container create")][0]
    remove = [v for v in verbs if v in ("rm", "container rm")][0]
    untag = [v for v in verbs if v in ("rmi", "image rm")][0]
    order = [verbs.index(v) for v in ("build", create, "cp", remove, untag)]
    assert order == sorted(order)
    create_argv = calls[verbs.index(create)]
    assert tag in create_argv
    assert _adjacent(create_argv, "--name", name)
    assert _adjacent(create_argv, "--label", "owner=factory-tools")
    assert _adjacent(create_argv, "--label", f"factory-tools/nonce={NONCE}")
    assert _adjacent(create_argv, "--label", f"factory-tools/request={container_request}")
    assert container_request != tag_request
    copy_argv = calls[verbs.index("cp")]
    sources = {f"{who}:/src/cli/target/release/curie" for who in (container, name)}
    assert sources & set(copy_argv)
    assert Path(copy_argv[-1]).resolve().is_relative_to((obj.f.work / "runner-cli").resolve())
    assert {container, name} & set(calls[verbs.index(remove)])
    assert tag in calls[verbs.index(untag)]
    for argv in calls:
        assert not {"-v", "--volume", "--mount"} & set(argv), argv
        assert not any(a.startswith(("type=bind", "--volume=", "--mount=")) for a in argv), argv


# --------------------------------------------------------------------------
# concurrent: runs a and b
# --------------------------------------------------------------------------


def test_concurrent_collector_argv_shapes() -> None:
    compose = docker_driver.collector_argv(
        "compose", "a", nonce=NONCE, image=docker_driver.COMPOSE_IMAGE
    )
    assert compose == [
        "python3", COLLECTOR, "compose",
        "--nonce", NONCE, "--image", docker_driver.COMPOSE_IMAGE,
        "--out", "/tmp/fd-docker/compose-a.json",
    ]  # fmt: skip
    probe = docker_driver.collector_argv(
        "probe", "b", nonce=NONCE, image=docker_driver.PROBE_IMAGE,
        cluster_api=CLUSTER_API, api=f"{API_IP}:8000",
    )  # fmt: skip
    assert probe == [
        "python3", COLLECTOR, "probe",
        "--nonce", NONCE, "--image", docker_driver.PROBE_IMAGE,
        "--cluster-api", CLUSTER_API, "--api", f"{API_IP}:8000",
        "--out", "/tmp/fd-docker/probe-b.json",
    ]  # fmt: skip
    skill = docker_driver.collector_argv(
        "skill-rung", "a", nonce=NONCE, source=SRC,
        curie_bin="/tmp/fd-docker/bin/curie", source_revision=CANDIDATE,
    )  # fmt: skip
    assert skill[-2:] == ["--out", "/tmp/fd-docker/skill-rung-a.json"]
    assert _adjacent(skill, "--curie-bin", "/tmp/fd-docker/bin/curie")
    assert _adjacent(skill, "--source-revision", CANDIDATE)
    parser = docker_scenario.build_parser()
    for argv, command in ((compose, "compose"), (probe, "probe"), (skill, "skill-rung")):
        parsed = parser.parse_args(argv[2:])
        assert parsed.command == command
        assert str(parsed.out) == argv[-1]


class _Pods:
    """The runner pods of runs a and b behind ``kubectl exec`` and the API service."""

    def __init__(self, behaviors: dict[tuple[str, str], str], events: list[Any]) -> None:
        envelope = _docker_envelope()
        self.runs = envelope["runs"]
        self.behaviors, self.events = behaviors, events
        self.files: dict[tuple[str, str], str] = {}
        self.windows: dict[str, list[float]] = {}
        self.collectors: list[tuple[str, str, dict[str, str], list[str]]] = []
        self.lock = threading.Lock()
        self.content = {
            "compose": _section("compose-passing"),
            "skill-rung": _section("skill-rung-passing"),
        }
        self.failing = {
            "compose": _section("compose-failing"),
            "skill-rung": _section("skill-rung-failing"),
        }

    def run_of(self, pod: str) -> str:
        return next(run for run, name in RUNNERS.items() if name == pod)

    def collector(self, argv: list[str], pod: str, tokens: list[str]) -> Result:
        index = next(i for i, t in enumerate(tokens) if t.endswith("docker_scenario.py"))
        assert tokens[index] == COLLECTOR, tokens
        assert tokens[index - 1] == "python3", tokens
        command, rest = tokens[index + 1], tokens[index + 2 :]
        flags = {rest[i]: rest[i + 1] for i in range(len(rest) - 1) if rest[i].startswith("--")}
        run = self.run_of(pod)
        assert _adjacent(argv, "-c", "runner") or "--container=runner" in argv, argv
        started = time.time()
        with self.lock:
            self.events.append(("collector", command, run))
        if command == "probe":
            time.sleep(0.3)  # long enough that sequential probes cannot overlap
        ended = time.time()
        with self.lock:
            self.collectors.append((command, run, flags, argv))
        behavior = self.behaviors.get((command, run), "ok")
        if command == "probe":
            self.windows[run] = [started, ended]
            section = copy.deepcopy(self.runs[run]["probe"]) | {"window": [started, ended]}
        elif behavior == "exit1":
            section = self.failing[command]
        else:
            section = self.content[command]
        if behavior != "missing":
            text = "not json" if behavior == "invalid" else json.dumps(section)
            self.files[(pod, flags["--out"])] = text
        return (1 if behavior in ("exit1", "missing") else 0), "", ""

    def handle(self, argv: list[str], _kwargs: dict[str, Any]) -> Result | None:
        if Path(argv[0]).name != "kubectl":
            return None
        tokens = _shell_tokens(_strip_kubectl(argv))
        if tokens[:2] in (["get", "svc"], ["get", "service"], ["get", "services"]):
            if "json" in tokens:
                return 0, json.dumps({"spec": {"clusterIP": API_IP}}), ""
            return 0, API_IP, ""  # -o jsonpath={.spec.clusterIP}
        if "exec" not in tokens:
            return None
        pod = _exec_pod(tokens)
        if any(t.endswith("docker_scenario.py") for t in tokens):
            return self.collector(argv, pod, tokens)
        if "printenv" in tokens:
            names = tokens[tokens.index("printenv") + 1 :]
            known = {"KUBERNETES_SERVICE_HOST": "10.96.0.1", "KUBERNETES_SERVICE_PORT": "443"}
            return 0, "".join(known[name] + "\n" for name in names), ""
        if "cat" in tokens:
            path = tokens[tokens.index("cat") + 1]
            found = self.files.get((pod, path))
            if found is None:
                return 1, "", f"cat: {path}: No such file or directory\n"
            return 0, found, ""
        return 0, "", ""


def _concurrent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    behaviors: dict[tuple[str, str], str] | None = None,
) -> tuple[Any, _Pods, list[Any], list[dict[str, Any]]]:
    commands = Commands()
    events: list[Any] = []
    pods = _Pods(behaviors or {}, events)
    commands.handlers.append(pods.handle)
    obj = _scenario(tmp_path, monkeypatch, commands)
    service = f"/api/v1/namespaces/{NAMESPACE}/services/{FULL}-api"
    obj.f.objects[service] = {
        "metadata": {"name": f"{FULL}-api", "namespace": NAMESPACE},
        "spec": {"clusterIP": API_IP, "ports": [{"name": "http", "port": 8000}]},
    }
    by_issue = {issue: run for run, issue in docker_driver.RUN_ISSUES.items()}
    runs = pods.runs

    def before(issue: int) -> tuple[set[str], set[str]]:
        events.append(("before", issue))
        return set(), set()

    def set_label(issue: int, on: bool) -> None:
        events.append(("label", issue, on))

    def acquire(issue: int, requests: set[str], claims: set[str]) -> tuple[Any, Any]:
        events.append(("acquire", issue))
        run = by_issue[issue]
        turn = {"issue": issue, "thread": dl.thread_key(issue), "claim": runs[run]["claim"]}
        turn["request_id"] = f"request-{run}"
        pod = {"metadata": {"name": RUNNERS[run], "uid": f"uid-runner-{run}"}}
        return turn, pod

    def observe(turn: dict[str, Any], pod: Any, *, tls: bool = False, run: bool = True) -> str:
        name = by_issue[turn["issue"]]
        events.append(("observe", turn["issue"], run))
        turn["runner"] = {"pod": RUNNERS[name], "pod_uid": f"uid-runner-{name}"}
        turn["daemon"] = {"pod": runs[name]["daemon_pod"], "pod_uid": f"uid-daemon-{name}"}
        return RUNNERS[name]

    def push_bundle(pod: Any, *args: Any, **kwargs: Any) -> None:
        name = pod if isinstance(pod, str) else pod["metadata"]["name"]
        events.append(("push", name))

    def signal(pod: str, name: str, content: str = "") -> None:
        events.append(("signal", pod, name))

    def settle(turn: dict[str, Any], fault: str, started: float) -> None:
        events.append(("settle", turn["issue"], fault))
        turn |= {"fault": fault, "settle_seconds": 1.0, "objects_absent": True, "leaked": []}

    for name, fake in {
        "before": before, "set_label": set_label, "acquire": acquire, "observe": observe,
        "push_bundle": push_bundle, "signal": signal, "settle": settle,
    }.items():  # fmt: skip
        monkeypatch.setattr(obj, name, fake)
    signature = inspect.signature(docker_scenario.collect)
    collected: list[dict[str, Any]] = []

    def collect(*args: Any, **kwargs: Any) -> dict[str, Any]:
        bound = dict(signature.bind(*args, **kwargs).arguments)
        events.append(("collect", bound["pod"]))
        collected.append(bound)
        run = next(r for r in runs if runs[r]["daemon_pod"] == bound["pod"])
        return copy.deepcopy(runs[run]["daemon"])

    monkeypatch.setattr(docker_scenario, "collect", collect)
    return obj, pods, events, collected


def test_concurrent_runs_label_both_turns_before_acquiring_either(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj, _pods, events, _collected = _concurrent(tmp_path, monkeypatch)
    obj.scenario_concurrent()
    labels = [i for i, e in enumerate(events) if e[0] == "label"]
    acquires = [i for i, e in enumerate(events) if e[0] == "acquire"]
    assert {events[i][1:] for i in labels} == {(4106, True), (4107, True)}
    assert len(acquires) == 2
    assert max(labels) < min(acquires)
    assert {e for e in events if e[0] == "observe"} == {
        ("observe", 4106, False),
        ("observe", 4107, False),
    }


def test_concurrent_probe_windows_overlap_and_compose_and_skill_rung_run_on_a(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj, pods, events, _collected = _concurrent(tmp_path, monkeypatch)
    obj.scenario_concurrent()
    (a0, a1), (b0, b1) = pods.windows["a"], pods.windows["b"]
    assert a0 < b1 and b0 < a1, "the two probes did not run concurrently"
    pushed = [i for i, e in enumerate(events) if e[0] == "push"]
    first_collector = min(i for i, e in enumerate(events) if e[0] == "collector")
    assert {events[i][1] for i in pushed} == set(RUNNERS.values())
    assert max(pushed) < first_collector
    ran = sorted((command, run) for command, run, _f, _a in pods.collectors)
    assert ran == [("compose", "a"), ("probe", "a"), ("probe", "b"), ("skill-rung", "a")]
    for command, run, flags, _argv in pods.collectors:
        assert flags["--nonce"] == NONCE
        assert flags["--out"] == f"/tmp/fd-docker/{command}-{run}.json"
        if command == "probe":
            assert flags["--image"] == docker_driver.PROBE_IMAGE
            assert flags["--cluster-api"] == CLUSTER_API
            assert flags["--api"] == f"{API_IP}:8000"
        elif command == "compose":
            assert flags["--image"] == docker_driver.COMPOSE_IMAGE
        else:
            assert flags["--source"] == SRC
            assert flags["--curie-bin"] == "/tmp/fd-docker/bin/curie"
            assert flags["--source-revision"] == CANDIDATE
    assert obj.sections["compose"] == pods.content["compose"]
    assert obj.sections["skill_rung"] == pods.content["skill-rung"]


def test_concurrent_daemon_sections_are_collected_over_each_probe_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj, pods, _events, collected = _concurrent(tmp_path, monkeypatch)
    obj.scenario_concurrent()
    by_pod = {bound["pod"]: bound for bound in collected}
    assert len(collected) == 2
    for run in ("a", "b"):
        bound = by_pod[pods.runs[run]["daemon_pod"]]
        t0, t1 = pods.windows[run]
        assert bound["context"] == CONTEXT
        assert bound["namespace"] == DAEMON_NAMESPACE
        # The daemon is read with the client credential of the same run's runner pod.
        assert bound["runner_namespace"] == obj.ns == NAMESPACE
        assert bound["runner_pod"] == RUNNERS[run]
        assert bound["nonce"] == NONCE
        assert str(bound["since"]) == str(math.floor(t0) - 5)
        assert str(bound["until"]) == str(math.ceil(t1) + 5)


def test_concurrent_turns_are_released_and_settled_after_collection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj, pods, events, _collected = _concurrent(tmp_path, monkeypatch)
    obj.scenario_concurrent()
    releases = [i for i, e in enumerate(events) if e[0] == "signal" and e[2] == "release"]
    assert {events[i][1] for i in releases} == set(RUNNERS.values())
    last_work = max(i for i, e in enumerate(events) if e[0] in ("collector", "collect"))
    assert min(releases) > last_work
    assert sorted(e[1:] for e in events if e[0] == "settle") == [
        (4106, "release"),
        (4107, "release"),
    ]
    assert set(obj.runs) == {"a", "b"}
    for run in ("a", "b"):
        record = obj.runs[run]
        assert set(record) >= {"claim", "daemon_pod", "probe", "daemon"}
        assert record["claim"] == pods.runs[run]["claim"]
        assert record["daemon_pod"] == pods.runs[run]["daemon_pod"]
        assert record["probe"]["window"] == pods.windows[run]
        assert record["daemon"] == pods.runs[run]["daemon"]


def test_concurrent_collector_exit_one_with_a_written_section_is_kept(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj, pods, _events, _collected = _concurrent(tmp_path, monkeypatch, {("compose", "a"): "exit1"})
    obj.scenario_concurrent()
    assert obj.sections["compose"] == pods.failing["compose"]


@pytest.mark.parametrize("behavior", ["missing", "invalid"])
@pytest.mark.parametrize("command", ["compose", "skill-rung"])
def test_concurrent_collector_without_a_section_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: str, behavior: str
) -> None:
    obj, _pods, _events, _collected = _concurrent(tmp_path, monkeypatch, {(command, "a"): behavior})
    with pytest.raises(RuntimeError, match=f"collector_failed:{command}"):
        obj.scenario_concurrent()


def test_concurrent_probe_without_a_section_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj, _pods, _events, _collected = _concurrent(
        tmp_path, monkeypatch, {("probe", "b"): "missing"}
    )
    with pytest.raises(RuntimeError, match="collector_failed:probe"):
        obj.scenario_concurrent()


# --------------------------------------------------------------------------
# lifecycle: the inherited scenarios and their envelope
# --------------------------------------------------------------------------

ORDER = (
    "scenario_concurrent",
    "scenario_success",
    "scenario_daemon_death",
    "scenario_cancel",
    "scenario_worker_loss",
    "scenario_orphan",
    "scenario_control",
    "scenario_upgrade",
    "collect_ci",
)


def test_lifecycle_scenarios_run_between_the_concurrent_runs_and_the_upgrade(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj = _scenario(tmp_path, monkeypatch, Commands())
    order: list[str] = []
    for name in ORDER:
        monkeypatch.setattr(obj, name, functools.partial(order.append, name))
    obj.scenarios()
    assert order == list(ORDER)


# --------------------------------------------------------------------------
# upgrade
# --------------------------------------------------------------------------


def _helm_values(catalog: str, custom: str, platform: dict[str, str]) -> dict[str, Any]:
    """``helm get values -o json``: the user-supplied values, factory bindings included."""

    runner = {"base": platform["digest"], "tools": ["docker-cli"]}
    repos = [
        {
            "alias": "acme-bot", "codeHost": "github", "projectId": "4401", "path": CUSTOM_REPO,
            "runner": runner | {"image": custom, "dockerfile": "Dockerfile"},
            "docker": {"enabled": True},
        },
        {
            "alias": "acme-catalog", "codeHost": "github", "projectId": "4402",
            "path": CATALOG_REPO, "runner": runner | {"image": catalog},
        },
    ]  # fmt: skip
    # One native GitHub binding per repository, as docker_values writes them.
    bindings = [
        {"name": repo["alias"], "tracker": "github", "trackerScopeId": repo["projectId"]}
        | {"repos": [repo]}
        for repo in repos
    ]
    sandbox = {"image": platform["ref"].split("@")[0], "digest": platform["digest"]}
    return {"agentSandbox": {"runner": sandbox}, "factory": {"bindings": bindings}}


IMAGES = _images()
REGISTRY_PATH = f"localhost:5000/fd-{NONCE}"
VALUES_BEFORE = _helm_values(IMAGES["runner"]["ref"], IMAGES["runner"]["ref"], IMAGES["platform"])
CATALOG_AFTER = f"{REGISTRY_PATH}/runner-acme-catalog@sha256:" + "2b" * 32
VALUES_AFTER = _helm_values(CATALOG_AFTER, IMAGES["runner"]["ref"], IMAGES["platform-next"])
# cli/src/ops/upgrade.rs plan_repository_runners: the catalog rebuild line and the
# custom-repository preserved line, with custom_repository_rebuild_command's command.
REBUILD_LINE = (
    f"repository runner rebuild: build catalog repository {CATALOG_REPO} in {REGISTRY_PATH} "
    f"on {IMAGES['platform-next']['ref']} during Apply, then apply its receipt in the single "
    "Helm upgrade"
)
PRESERVED_LINE = (
    f"custom repository runner preserved: {CUSTOM_REPO} declares Dockerfile Dockerfile; after "
    "this upgrade, from the directory of a repo-runner.yaml spec containing its retained tools, "
    f"probes and Dockerfile declaration, run: curie cluster factory --repo-runner {CUSTOM_REPO}"
    f"=repo-runner.yaml --registry {REGISTRY_PATH} --namespace {NAMESPACE} --release {NAMESPACE}"
    " --chart /work/snapshot/charts/curie"
)
# cli/src/ops/upgrade.rs ClusterUpgradeOutput::Completed to_json, the `--json` document;
# completed_output sets status "succeeded" and phase "commit" on success.
UPGRADE_OUTPUT = {
    "status": "succeeded",
    "phase": "commit",
    "target_version": "9.8.7",
    # Installed from the run-owned chart-base copy, so the upgrade is a real transition.
    "from_version": BASE_VERSION,
    "known_good_version": "9.8.7",
    "resumed": False,
    "previous_serving": False,
    "unchanged": False,
    "plan": [REBUILD_LINE, PRESERVED_LINE],
    "convergence": None,
    "canary": None,
    "fail_forward": None,
    "compatibility": None,
}
# repo_runner._POOL_REFUSAL_REPLY's stale_base alternative, which the kernel sends whole
# (apps/worker/src/curie_worker/kernel/attempt.py + sandbox/types.py).
STALE_REPLY = (
    f"This agent cannot start: repository {CUSTOM_REPO} runner image was built on platform "
    f"runner {IMAGES['platform']['ref']}, not this installation's "
    f"{IMAGES['platform-next']['digest']} (stale_base); rebuild and rebind it on the installed "
    f"pinned platform runner with `curie cluster factory --repo-runner {CUSTOM_REPO}=<spec> "
    "--registry <registry> --namespace <namespace> --release <release> --context <context>`; "
    "use a repository runner spec containing its retained tools, probes and Dockerfile "
    "declaration. An operator has to make that change."
)
API_KEY = "fixture-api-key"


def test_upgrade_fixture_reply_is_the_stale_base_refusal() -> None:
    match = fe.load_repo_runner()._POOL_REFUSAL_REPLY.fullmatch(STALE_REPLY)
    assert match is not None
    assert match.group("repository") == CUSTOM_REPO
    assert match.group("stale") == "stale_base"


def test_upgrade_values_pin_the_next_platform_runner() -> None:
    image = IMAGES["platform-next"]
    assert docker_driver.upgrade_values(image) == {
        "agentSandbox": {
            "runner": {"image": f"{REGISTRY_PATH}/platform-next", "digest": NEXT_DIGEST}
        }
    }


def test_upgrade_argv_runs_the_production_cli() -> None:
    argv = docker_driver.upgrade_argv(
        "/w/curie", CONTEXT, "9.8.7", "/w/charts/curie", NAMESPACE, NAMESPACE, "/w/up.json"
    )
    assert argv == [
        "/w/curie", "--json", "cluster", "--context", CONTEXT, "upgrade",
        "--to", "9.8.7", "--chart", "/w/charts/curie",
        "--namespace", NAMESPACE, "--release", NAMESPACE, "--yes", "-f", "/w/up.json",
    ]  # fmt: skip


def test_upgrade_message_argv_turns_on_the_custom_repository() -> None:
    argv = docker_driver.message_argv(
        "/w/curie", CONTEXT, nonce=NONCE, namespace=NAMESPACE, release=NAMESPACE,
        chart="/w/charts/curie", callback_host=STUB_HOST,
    )  # fmt: skip
    assert argv == [
        "/w/curie", "cluster", "--context", CONTEXT, "message",
        f"Repository: https://github.com/{CUSTOM_REPO}\nRun the Docker scenario upgrade check.",
        "--agent", DOCKER_AGENT, "--channel", f"C0EXAMPLE{NONCE.upper()}",
        "--thread", f"fd-upgrade-{NONCE}", "--namespace", NAMESPACE, "--release", NAMESPACE,
        "--chart", "/w/charts/curie", "--listen-host", STUB_HOST,
        "--timeout-secs", "300", "--json",
    ]  # fmt: skip


def test_upgrade_plan_text_joins_the_cli_plan_lines() -> None:
    assert docker_driver.plan_text(UPGRADE_OUTPUT) == REBUILD_LINE + "\n" + PRESERVED_LINE


@pytest.mark.parametrize(
    ("stdout", "reply"),
    [
        pytest.param(
            'waiting\n{"reply": "first"}\n{"reply": "second", "failure_class": null}\n',
            "second",
            id="last-object",
        ),
        pytest.param('{"reply": "only"}', "only", id="no-newline"),
        pytest.param("no json here\n", "", id="no-object"),
        pytest.param("", "", id="empty"),
        pytest.param('{"failure_class": "turn_failed"}\n', "", id="no-reply"),
        pytest.param('{"reply": null}\n', "", id="null-reply"),
    ],
)
def test_upgrade_message_reply_is_the_last_json_reply(stdout: str, reply: str) -> None:
    assert docker_driver.message_reply(stdout) == reply


class _Upgrade:
    """helm, the host CLI's upgrade and message, and the release Secret."""

    def __init__(
        self, curie: str, exit_code: int = 0, output: dict[str, Any] | None = None
    ) -> None:
        self.curie, self.exit_code = curie, exit_code
        self.values_calls: list[list[str]] = []
        self.upgrade: list[tuple[list[str], str, int]] = []
        self.upgrade_env: list[dict[str, str]] = []
        self.message: list[tuple[list[str], dict[str, str]]] = []
        self.order: list[str] = []
        self.stdout = json.dumps(UPGRADE_OUTPUT if output is None else output, indent=2) + "\n"
        # One JSON object per line, the reply field as repo_runner.py reads it.
        self.message_stdout = json.dumps({"reply": STALE_REPLY, "failure_class": None}) + "\n"

    def handle(self, argv: list[str], kwargs: dict[str, Any]) -> Result | None:
        name = Path(argv[0]).name
        if name == "helm" and "get" in argv and "values" in argv:
            self.values_calls.append(argv)
            self.order.append("values")
            values = VALUES_BEFORE if len(self.values_calls) == 1 else VALUES_AFTER
            return 0, json.dumps(values), ""
        if argv[0] == self.curie and "upgrade" in argv:
            values_file = Path(argv[argv.index("-f") + 1])
            mode = values_file.stat().st_mode & 0o777
            self.upgrade.append((argv, values_file.read_text(), mode))
            self.upgrade_env.append(dict(kwargs.get("env") or {}))
            self.order.append("upgrade")
            return self.exit_code, self.stdout, ""
        if argv[0] == self.curie and "message" in argv:
            self.message.append((argv, dict(kwargs.get("env") or {})))
            self.order.append("message")
            return 0, self.message_stdout, ""
        if name == "kubectl":
            tokens = _strip_kubectl(argv)
            if "secret" in tokens or "secrets" in tokens:
                encoded = base64.b64encode(API_KEY.encode()).decode()
                if any(t.startswith("jsonpath") for t in tokens):
                    return 0, encoded, ""
                return 0, json.dumps({"data": {"apiKey": encoded}}), ""
            if "rollout" in tokens:
                return 0, "", ""
        return None


def _upgrade(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    exit_code: int = 0,
    output: dict[str, Any] | None = None,
) -> tuple[Any, _Upgrade, Commands]:
    commands = Commands()
    obj = _scenario(tmp_path, monkeypatch, commands)
    obj.builder = BUILDER  # created by prepare() in a live run
    fake = _Upgrade(obj.curie, exit_code, output)
    commands.handlers.append(fake.handle)
    secret = f"/api/v1/namespaces/{NAMESPACE}/secrets/{FULL}-secrets"
    encoded = base64.b64encode(API_KEY.encode()).decode()
    meta = {"name": f"{FULL}-secrets", "namespace": NAMESPACE}
    obj.f.objects[secret] = {"metadata": meta, "data": {"apiKey": encoded}}
    return obj, fake, commands


def test_upgrade_scenario_runs_the_cli_and_builds_the_section(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj, fake, commands = _upgrade(tmp_path, monkeypatch)
    obj.scenario_upgrade()
    work, chart = obj.f.work, str(obj.f.snapshot / "charts/curie")
    assert fake.order == ["values", "upgrade", "values", "message"]
    for argv in fake.values_calls:
        assert argv[:3] == ["helm", "--kube-context", CONTEXT]
        assert NAMESPACE in argv and _adjacent(argv, "-n", NAMESPACE)
        assert _adjacent(argv, "-o", "json")
    [(argv, values_text, mode)] = fake.upgrade
    values_file = str(work / "upgrade.values.json")
    assert argv == docker_driver.upgrade_argv(
        obj.curie, CONTEXT, "9.8.7", chart, NAMESPACE, NAMESPACE, values_file
    )
    assert json.loads(values_text) == docker_driver.upgrade_values(IMAGES["platform-next"])
    assert mode == 0o600
    # The catalog rebuild is multi-platform, so the CLI must use the run-owned builder.
    [upgrade_env] = fake.upgrade_env
    assert upgrade_env["BUILDX_BUILDER"] == BUILDER
    [(message, env)] = fake.message
    assert message == docker_driver.message_argv(
        obj.curie, CONTEXT, nonce=NONCE, namespace=NAMESPACE, release=NAMESPACE,
        chart=chart, callback_host=STUB_HOST,
    )  # fmt: skip
    assert env["CURIE_API_KEY"] == API_KEY
    assert env["CURIE_CONFIG_DIR"] == str(work / "curie-config")
    for call, _kwargs in commands.calls:
        assert API_KEY not in " ".join(call), call
    assert (work / "upgrade-output.json").read_text() == fake.stdout
    assert json.loads((work / "values-before.json").read_text()) == VALUES_BEFORE
    assert json.loads((work / "values-after.json").read_text()) == VALUES_AFTER
    assert obj.evidence["upgrade_exit_code"] == 0
    assert obj.evidence["from_version"] == BASE_VERSION
    assert obj.evidence["target_version"] == "9.8.7"
    expected = docker_scenario.upgrade_section(
        docker_driver.plan_text(UPGRADE_OUTPUT),
        VALUES_BEFORE, VALUES_AFTER, CATALOG_REPO, CUSTOM_REPO, STALE_REPLY,
    )  # fmt: skip
    assert obj.sections["upgrade"] == expected
    assert docker_scenario._judge_upgrade(obj.sections["upgrade"]) == []


FAILED_UPGRADE = UPGRADE_OUTPUT | {
    "status": "failed",
    "phase": "apply",
    "known_good_version": BASE_VERSION,
}


@pytest.mark.parametrize(
    ("exit_code", "output", "detail"),
    [
        pytest.param(1, FAILED_UPGRADE, "failed", id="exit-1-status-failed"),
        pytest.param(1, UPGRADE_OUTPUT, "", id="exit-1-status-succeeded"),
        pytest.param(0, FAILED_UPGRADE, "failed", id="exit-0-status-failed"),
        pytest.param(0, UPGRADE_OUTPUT | {"status": "in_progress"}, "in_progress", id="pending"),
    ],
)
def test_upgrade_failure_fails_after_recording_its_output_and_section(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    exit_code: int,
    output: dict[str, Any],
    detail: str,
) -> None:
    obj, fake, _commands = _upgrade(tmp_path, monkeypatch, exit_code, output)
    with pytest.raises(RuntimeError) as failure:
        obj.scenario_upgrade()
    assert str(failure.value).startswith("upgrade_failed:")
    assert detail in str(failure.value)
    assert (obj.f.work / "upgrade-output.json").read_text() == fake.stdout
    assert obj.evidence["upgrade_exit_code"] == exit_code
    assert fake.order[-1] == "message", "the custom turn still runs for the evidence"
    assert obj.sections["upgrade"]["catalog"]["repo"] == CATALOG_REPO
    assert obj.sections["upgrade"]["custom"]["repo"] == CUSTOM_REPO


def test_upgrade_succeeded_with_exit_zero_does_not_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj, _fake, _commands = _upgrade(tmp_path, monkeypatch, 0, UPGRADE_OUTPUT)
    obj.scenario_upgrade()  # raises nothing
    assert obj.evidence["upgrade_exit_code"] == 0
    assert docker_scenario._judge_upgrade(obj.sections["upgrade"]) == []


def test_upgrade_failure_fails_the_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def failing_upgrade(obj: Any) -> None:
        _fill_passing(obj)
        raise RuntimeError("upgrade_failed:failed")

    obj, _calls, _leftovers = _run(tmp_path, monkeypatch, failing_upgrade)
    result = obj.run()
    assert result["result"] == "failed"
    assert any(f.startswith("upgrade_failed:") for f in result["failures"])


# --------------------------------------------------------------------------
# ci
# --------------------------------------------------------------------------


def _offset(recording: dict[str, Any], stamp: str) -> float:
    def parsed(value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    return (parsed(stamp) - parsed(recording["epoch"])).total_seconds()


def test_ci_clock_covers_every_recorded_check() -> None:
    recording = json.loads(RECORDING.read_text())
    last = max(_offset(recording, row["completed_at"]) for row in recording["check_runs"])
    assert docker_driver.CI_CLOCK_SECONDS == 1800
    assert last <= docker_driver.CI_CLOCK_SECONDS


def test_ci_collect_advances_the_stub_and_reads_its_main_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    commands = Commands()
    obj = _scenario(tmp_path, monkeypatch, commands)
    events: list[Any] = []
    sha = "a" * 40
    token = "example-installation-token"
    ca_file = tmp_path / "stub-ca.pem"
    ca_file.write_text("-----BEGIN CERTIFICATE-----\n")
    section = {"first_head": sha, "required": ["E2E required"], "checks": []}

    class Stub:
        base_url, unknown_requests = f"https://{STUB_HOST}:4443", []

        def __init__(self) -> None:
            self.ca_file = ca_file

        def advance(self, seconds: float) -> None:
            events.append(("advance", seconds))

        def _find_ref(self, branch: str) -> str | None:
            events.append(("ref", branch))
            return sha if branch == "main" else None

    def ci(argv: list[str], kwargs: dict[str, Any]) -> Result | None:
        if len(argv) > 2 and argv[1].endswith("docker_scenario.py") and argv[2] == "ci":
            events.append(("ci", argv, dict(kwargs.get("env") or {})))
            Path(argv[argv.index("--out") + 1]).write_text(json.dumps(section))
            return 0, "", ""
        return None

    commands.handlers.append(ci)
    obj.stub = Stub()
    monkeypatch.setattr(obj, "github_token", lambda: token)
    obj.collect_ci()
    kinds = [e[0] for e in events]
    assert ("advance", docker_driver.CI_CLOCK_SECONDS) in events
    assert kinds.index("advance") < kinds.index("ci")
    assert ("ref", "main") in events
    [(_kind, argv, env)] = [e for e in events if e[0] == "ci"]
    assert argv[0] == sys.executable
    assert Path(argv[1]).resolve() == (ROOT / "docker_scenario.py").resolve()
    assert argv[2:] == [
        "ci", "--api-url", Stub.base_url + "/api/v3", "--repo", dl.REPO,
        "--base", "main", "--sha", sha, "--required", "E2E required",
        "--out", str(obj.f.work / "ci.json"),
    ]  # fmt: skip
    assert env["SSL_CERT_FILE"] == str(ca_file)
    assert env["GITHUB_TOKEN"] == token
    assert token not in argv
    assert obj.sections["ci"] == section


def test_ci_stub_check_runs_carry_the_requested_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    recording = json.loads(RECORDING.read_text())
    stub = github_stub.GithubStub(
        tmp_path / "github",
        recording,
        host="127.0.0.1",
        seed_tree=SEED_TREE,
        issue_file=LIFECYCLE_FIXTURES / "issue.md",
    )
    try:
        stub.start()
        stub.advance(docker_driver.CI_CLOCK_SECONDS)
        sha = stub._find_ref("main")
        assert sha is not None and re.fullmatch(r"[0-9a-f]{40}", sha)
        context = ssl.create_default_context(cafile=str(stub.ca_file))
        body = json.dumps({"repositories": ["acme-bot"], "permissions": {"issues": "read"}})
        request = Request(
            stub.base_url + "/api/v3/app/installations/5501/access_tokens",
            data=body.encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, context=context, timeout=30) as response:
            token = str(json.load(response)["token"])
        monkeypatch.setenv("SSL_CERT_FILE", str(stub.ca_file))
        # urlopen's shared opener fixes its TLS context when first built, so an
        # earlier test's plain urlopen would otherwise hide SSL_CERT_FILE here.
        monkeypatch.setattr(urllib.request, "_opener", None)
        monkeypatch.setenv("GITHUB_TOKEN", token)
        section = docker_scenario.collect_ci(
            stub.base_url + "/api/v3", dl.REPO, "main", sha, ["E2E required"]
        )
        assert section["first_head"] == sha
        assert section["required"] == ["E2E required"]
        assert section["checks"]
        assert {check["head_sha"] for check in section["checks"]} == {sha}
        assert fe.judge_first_head_ci(section) == []
    finally:
        stub.close()


# --------------------------------------------------------------------------
# teardown: proof that every owned object is gone
# --------------------------------------------------------------------------


def _docker_kind(argv: list[str]) -> str:
    words = [a for a in argv[1:] if not a.startswith("-")]
    first = words[0] if words else ""
    return {
        "ps": "containers",
        "container": "containers",
        "images": "images",
        "image": "images",
        "volume": "volumes",
        "network": "networks",
    }.get(first, first)


class _Leftovers:
    """`kubectl get ns <name>` and the default engine's label and name filters."""

    def __init__(
        self,
        present: tuple[str, ...] = (),
        labelled: dict[str, str] | None = None,
        registry_left: bool = False,
        unreachable: bool = False,
        builder_left: tuple[str, ...] = (),
    ) -> None:
        self.present, self.labelled = set(present), labelled or {}
        self.registry_left, self.unreachable = registry_left, unreachable
        self.builder_left = set(builder_left)  # of "builder", "container", "volume"
        self.namespaces: list[tuple[str | None, str]] = []
        self.docker: list[tuple[str, str]] = []

    def buildkit(self, argv: list[str], joined: str) -> Result | None:
        """The run-owned builder and its buildkit container and state volume, by exact name."""

        if argv[1:3] == ["buildx", "inspect"] and BUILDER in argv:
            self.docker.append(("builder", joined))
            if "builder" in self.builder_left:
                return 0, f"Name:          {BUILDER}\nDriver:        docker-container\n", ""
            return 1, "", f'ERROR: no builder "{BUILDER}" found\n'
        # The state volume's name extends the container's, so match it first.
        for role, object_name in (("volume", BUILDKIT_VOLUME), ("container", BUILDKIT_CONTAINER)):
            if object_name not in joined:
                continue
            self.docker.append((f"buildkit_{role}", joined))
            left = role in self.builder_left
            if "inspect" in argv:
                if left:
                    return 0, json.dumps([{"Name": object_name}]), ""
                return 1, "", f"Error: No such {role}: {object_name}\n"
            return 0, (object_name + "\n" if left else ""), ""
        return None

    def handle(self, argv: list[str], _kwargs: dict[str, Any]) -> Result | None:
        name = Path(argv[0]).name
        if name == "kubectl":
            tokens = _strip_kubectl(argv)
            if tokens[:2] not in (["get", "ns"], ["get", "namespace"], ["get", "namespaces"]):
                return None
            rest, names, skip = tokens[2:], [], False
            for token in rest:
                if skip:
                    skip = False
                elif token in ("-o", "--output", "-l", "--selector"):
                    skip = True
                elif not token.startswith("-"):
                    names.append(token)
            assert len(names) == 1, f"prove each owned namespace by name: {argv}"
            self.namespaces.append((_flag(argv, "--context"), names[0]))
            if self.unreachable:
                return 1, "", "Unable to connect to the server: dial tcp: i/o timeout\n"
            if names[0] in self.present:
                return 0, f"namespace/{names[0]}\n", ""
            # kubectl's stderr for an absent namespace.
            return 1, "", f'Error from server (NotFound): namespaces "{names[0]}" not found\n'
        if name == "docker":
            joined = " ".join(argv)
            found = self.buildkit(argv, joined)
            if found is not None:
                return found
            if f"name=^fd-registry-{NONCE}$" in joined:
                self.docker.append(("registry", joined))
                return 0, ("f00dfeed1234\n" if self.registry_left else ""), ""
            if f"label=factory-tools/nonce={NONCE}" in joined:
                kind = _docker_kind(argv)
                self.docker.append((kind, joined))
                return 0, self.labelled.get(kind, ""), ""
        return None


def _teardown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, leftovers: _Leftovers
) -> tuple[Any, Commands]:
    commands = Commands()
    commands.handlers.append(leftovers.handle)
    return _scenario(tmp_path, monkeypatch, commands), commands


OWNED = (NAMESPACE, DAEMON_NAMESPACE, CONTROLLER_NAMESPACE)


def test_teardown_verify_passes_when_every_namespace_is_not_found_and_nothing_is_labelled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    leftovers = _Leftovers()
    obj, _commands = _teardown(tmp_path, monkeypatch, leftovers)
    errors: list[str] = []
    obj.verify_teardown(errors)
    assert errors == []
    assert sorted(name for _c, name in leftovers.namespaces) == sorted(OWNED)
    assert {context for context, _n in leftovers.namespaces} == {CONTEXT}
    kinds = {kind for kind, _joined in leftovers.docker}
    assert kinds >= {"containers", "images", "volumes", "networks", "registry"}


def test_teardown_verify_passes_when_the_builder_and_its_buildkit_objects_are_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    leftovers = _Leftovers()
    obj, _commands = _teardown(tmp_path, monkeypatch, leftovers)
    errors: list[str] = []
    obj.verify_teardown(errors)
    assert errors == []
    kinds = {kind for kind, _joined in leftovers.docker}
    assert kinds >= {"builder", "buildkit_container", "buildkit_volume"}


@pytest.mark.parametrize("left", ["builder", "container", "volume"])
def test_teardown_verify_fails_when_the_builder_or_its_buildkit_objects_remain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, left: str
) -> None:
    obj, _commands = _teardown(tmp_path, monkeypatch, _Leftovers(builder_left=(left,)))
    errors: list[str] = []
    obj.verify_teardown(errors)
    assert errors


def _release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, builders: _Builders
) -> tuple[Any, list[str]]:
    """release_everything over a recording inherited teardown and the builder engine."""

    obj, order = _prepare(tmp_path, monkeypatch, builders)

    def inherited(_self: Any, _errors: list[str]) -> dict[str, bool]:
        order.append("super_release")
        return dict.fromkeys(dl.CLEANUP_KEYS, True)

    monkeypatch.setattr(dl.Lifecycle, "release_everything", inherited)
    return obj, order


def test_teardown_removes_the_builder_this_run_created(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    builders = _Builders()
    obj, order = _release(tmp_path, monkeypatch, builders)
    obj.prepare()
    errors: list[str] = []
    cleanup = obj.release_everything(errors)
    assert errors == []
    assert all(cleanup[key] is True for key in dl.CLEANUP_KEYS)
    [remove] = [argv for verb, argv in builders.calls if verb == "rm"]
    assert BUILDER in remove
    assert order.index("super_release") < order.index("buildx rm")


@pytest.mark.parametrize("occupied", [True, False], ids=["refused-existing", "never-prepared"])
def test_teardown_keeps_a_builder_this_run_did_not_create(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, occupied: bool
) -> None:
    builders = _Builders(existing=occupied)
    obj, order = _release(tmp_path, monkeypatch, builders)
    if occupied:
        with pytest.raises(RuntimeError, match="builder_name_occupied"):
            obj.prepare()
    obj.release_everything([])
    assert "super_release" in order
    assert [verb for verb, _argv in builders.calls if verb == "rm"] == []


@pytest.mark.parametrize("present", OWNED)
def test_teardown_verify_fails_when_an_owned_namespace_remains(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, present: str
) -> None:
    obj, _commands = _teardown(tmp_path, monkeypatch, _Leftovers(present=(present,)))
    errors: list[str] = []
    obj.verify_teardown(errors)
    assert errors


def test_teardown_verify_fails_when_absence_cannot_be_proved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj, _commands = _teardown(tmp_path, monkeypatch, _Leftovers(unreachable=True))
    errors: list[str] = []
    obj.verify_teardown(errors)
    assert errors


@pytest.mark.parametrize("kind", ["containers", "images", "volumes", "networks"])
def test_teardown_verify_fails_on_a_docker_object_with_the_run_label(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    leftovers = _Leftovers(labelled={kind: "3f2a9c1b7d4e\n"})
    obj, _commands = _teardown(tmp_path, monkeypatch, leftovers)
    errors: list[str] = []
    obj.verify_teardown(errors)
    assert errors


def test_teardown_verify_fails_when_the_registry_container_remains(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj, _commands = _teardown(tmp_path, monkeypatch, _Leftovers(registry_left=True))
    errors: list[str] = []
    obj.verify_teardown(errors)
    assert errors


def test_teardown_scrub_removes_the_run_owned_work_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj, _commands = _teardown(tmp_path, monkeypatch, _Leftovers())
    work = obj.f.work
    (work / "upgrade.values.json").write_text("{}")
    (work / "runner-cli").mkdir()
    (work / "runner-cli/curie").write_text("#!/bin/sh\n")
    for directory in ("chart-base", "seed-tree"):
        (work / directory).mkdir()
        (work / directory / "Chart.yaml").write_text("version: 9.8.7-fdbase\n")
    errors: list[str] = []
    obj.scrub_work(errors)
    assert errors == []
    for name in ("upgrade.values.json", "runner-cli", "chart-base", "seed-tree"):
        assert not (work / name).exists(), name
    assert (work / "snapshot/charts/curie/Chart.yaml").read_text() == CHART_YAML


# --------------------------------------------------------------------------
# run(): judge, exit and teardown together
# --------------------------------------------------------------------------


def _fill_passing(obj: Any) -> None:
    """What a passing live run leaves behind: lifecycle turns, runs and every section."""

    lifecycle = _lifecycle_envelope()
    for key in ("provenance", "turns", "tls", "orphan", "control", "timings"):
        obj.envelope[key] = copy.deepcopy(lifecycle[key])
    obj.timings.update(lifecycle["timings"])
    docker = _docker_envelope()
    obj.runs = copy.deepcopy(docker["runs"])
    for key in ("upgrade", "ci", "compose", "skill_rung"):
        obj.sections[key] = copy.deepcopy(docker[key])


def _run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scenario: Callable[[Any], None] = _fill_passing,
    leftovers: _Leftovers | None = None,
) -> tuple[Any, list[str], _Leftovers]:
    leftovers = leftovers or _Leftovers()
    obj, _commands = _teardown(tmp_path, monkeypatch, leftovers)
    calls: list[str] = []

    def phase(name: str, then: Callable[[], Any] | None = None) -> Callable[..., Any]:
        def fake(*_args: Any, **_kwargs: Any) -> Any:
            calls.append(name)
            return then() if then is not None else None

        return fake

    monkeypatch.setattr(obj, "prepare", phase("prepare"))
    monkeypatch.setattr(obj, "install", phase("install"))
    monkeypatch.setattr(obj, "scenarios", phase("scenarios", functools.partial(scenario, obj)))
    monkeypatch.setattr(obj, "check_fixtures", phase("check_fixtures"))
    monkeypatch.setattr(obj, "failure_evidence", lambda _failed: {})
    cleanup = functools.partial(dict.fromkeys, dl.CLEANUP_KEYS, True)
    monkeypatch.setattr(obj, "release_everything", phase("release_everything", cleanup))
    monkeypatch.setattr(obj, "scrub_work", phase("scrub_work"))
    return obj, calls, leftovers


def test_exit_run_passes_when_failures_reasons_and_teardown_errors_are_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj, calls, leftovers = _run(tmp_path, monkeypatch)
    result = obj.run()
    assert result["result"] == "passed", result["failures"]
    assert result["failures"] == []
    assert result["nonce"] == NONCE
    assert docker_scenario.judge(result["envelope"]) == []
    assert result["envelope"]["schema"] == "docker-scenario/v1"
    assert calls[:3] == ["prepare", "install", "scenarios"]
    assert "release_everything" in calls
    assert sorted(name for _c, name in leftovers.namespaces) == sorted(OWNED)


def test_lifecycle_sections_of_the_envelope_come_from_the_lifecycle_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj, _calls, _leftovers = _run(tmp_path, monkeypatch)
    result = obj.run()
    expected = docker_scenario.from_lifecycle(obj.envelope)
    for key in ("cleanup", "control", "lifecycle"):
        assert result["envelope"][key] == expected[key], key
    assert result["envelope"]["lifecycle"]["failures"] == []


def test_lifecycle_failure_fails_the_docker_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken_cancel(obj: Any) -> None:
        _fill_passing(obj)
        obj.envelope["turns"]["cancel"]["request_state"] = "failed"

    obj, _calls, _leftovers = _run(tmp_path, monkeypatch, broken_cancel)
    result = obj.run()
    assert result["result"] == "failed"
    assert any(f.startswith("incomplete_cleanup: lifecycle") for f in result["failures"])


def test_judge_single_reason_fails_the_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def apart(obj: Any) -> None:
        _fill_passing(obj)
        obj.runs["b"]["probe"]["window"] = [1792669500.0, 1792669510.0]

    obj, _calls, _leftovers = _run(tmp_path, monkeypatch, apart)
    result = obj.run()
    assert result["result"] == "failed"
    assert any(f.startswith("not_concurrent:") for f in result["failures"])


def test_judge_names_a_missing_section(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def no_compose(obj: Any) -> None:
        _fill_passing(obj)
        del obj.sections["compose"]

    obj, _calls, _leftovers = _run(tmp_path, monkeypatch, no_compose)
    result = obj.run()
    assert result["result"] == "failed"
    assert "missing_observation: compose" in result["failures"]
    assert result["envelope"]["schema"] == "docker-scenario/v1"
    assert result["envelope"]["nonce"] == NONCE


def test_exit_scenario_runtime_error_fails_the_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def crash(_obj: Any) -> None:
        raise RuntimeError("collector_failed:probe")

    obj, calls, _leftovers = _run(tmp_path, monkeypatch, crash)
    result = obj.run()
    assert result["result"] == "failed"
    assert "collector_failed:probe" in result["failures"]
    assert "release_everything" in calls


def test_exit_teardown_error_alone_fails_the_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    leftovers = _Leftovers(present=(DAEMON_NAMESPACE,))
    obj, _calls, _leftovers = _run(tmp_path, monkeypatch, leftovers=leftovers)
    result = obj.run()
    assert result["result"] == "failed"
    assert result["failures"]


def test_teardown_runs_and_keeps_cleanup_evidence_when_a_scenario_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def crash(_obj: Any) -> None:
        raise RuntimeError("collector_failed:compose")

    obj, calls, leftovers = _run(tmp_path, monkeypatch, crash)
    result = obj.run()
    assert calls.index("release_everything") > calls.index("scenarios")
    assert sorted(name for _c, name in leftovers.namespaces) == sorted(OWNED)
    cleanup = json.dumps(result["cleanup"])
    for name in OWNED:
        assert name in cleanup, name
    path = obj.f.work / "docker-evidence.json"
    assert Path(result["evidence"]).name == "docker-evidence.json"
    assert path.stat().st_mode & 0o777 == 0o600
    written = json.loads(path.read_text())
    assert written["cleanup"] == result["cleanup"]
    assert written["failures"] == result["failures"]
    assert written["envelope"] == result["envelope"]


def test_teardown_leftover_namespace_and_labelled_object_fail_the_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    leftovers = _Leftovers(present=(NAMESPACE,), labelled={"volumes": "fd-volume\n"})
    obj, _calls, _leftovers = _run(tmp_path, monkeypatch, leftovers=leftovers)
    result = obj.run()
    assert result["result"] == "failed"
    path = obj.f.work / "docker-evidence.json"
    assert path.stat().st_mode & 0o777 == 0o600
    assert NAMESPACE in json.dumps(json.loads(path.read_text())["cleanup"])


def test_exit_drive_returns_a_passing_result(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CURIE_LOCAL_STUB_HOST", STUB_HOST)
    recorder = _Recorder(PASSED)
    monkeypatch.setattr(docker_driver, "DockerScenario", recorder)
    p = SimpleNamespace(config=SimpleNamespace(kube_context=CONTEXT), candidate=HEAD_SHA)
    assert docker_driver.drive(p) == PASSED
    [(args, candidate)] = recorder.calls
    assert candidate == HEAD_SHA
    assert args.context == CONTEXT and args.callback_host == STUB_HOST


def test_exit_drive_raises_preflight_failed_unless_passed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CURIE_LOCAL_STUB_HOST", STUB_HOST)
    monkeypatch.setattr(docker_driver, "DockerScenario", _Recorder(FAILED))
    p = SimpleNamespace(config=SimpleNamespace(kube_context=CONTEXT), candidate=HEAD_SHA)
    with pytest.raises(fe.PreflightFailed, match="docker scenario failed: not_concurrent"):
        docker_driver.drive(p)
