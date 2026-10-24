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
CATALOG_REPO = "acme-corp/acme-bot"
CUSTOM_REPO = "acme-corp/acme-custom"
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


class FakeFixture:
    """The attributes of docker-daemon-fixture.py's Fixture the driver reads."""

    def __init__(self, root: Path, commands: Commands, args: Any) -> None:
        self.args, self._commands = args, commands
        self.work = root / "work"
        self.work.mkdir(mode=0o700)
        self.snapshot = self.work / "snapshot"
        chart = self.snapshot / "charts/curie"
        chart.mkdir(parents=True)
        (chart / "Chart.yaml").write_text(
            'apiVersion: v2\nname: curie\nversion: 9.8.7\nappVersion: "9.8.7"\n'
        )
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
    assert docker_driver.CUSTOM_REPO == CUSTOM_REPO
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
    repos = values["factory"]["bindings"][0]["repos"]
    return {repo["path"]: repo for repo in repos}


@pytest.mark.parametrize("docker", [True, False], ids=["opted-in", "control"])
def test_install_docker_values_add_the_custom_repository(docker: bool) -> None:
    source = _lifecycle_values(docker=docker)
    snapshot = copy.deepcopy(source)
    images = _images()
    values = docker_driver.docker_values(source, images)
    assert source == snapshot, "docker_values mutated its input"
    repos = _repos(values)
    assert list(repos) == [CATALOG_REPO, CUSTOM_REPO]
    catalog, custom = repos[CATALOG_REPO], repos[CUSTOM_REPO]
    assert catalog == _repos(snapshot)[CATALOG_REPO]
    assert ("docker" in catalog) is docker
    assert {k: custom[k] for k in ("alias", "codeHost", "projectId")} == {
        "alias": "acme-custom",
        "codeHost": "github",
        "projectId": "4402",
    }
    assert "docker" not in custom
    runner = custom["runner"]
    assert runner["image"] == images["runner"]["ref"]
    assert runner["base"] == images["platform"]["digest"]
    assert runner["platformDigests"] == catalog["runner"]["platformDigests"]
    assert runner["tools"] == ["docker-cli"]
    assert runner["dockerfile"] == "Dockerfile"


def test_install_docker_values_raise_the_daemon_limits_for_a_skill_rung() -> None:
    values = docker_driver.docker_values(_lifecycle_values(docker=True), _images())
    daemon = values["factory"]["docker"]
    assert daemon["maxClaimLifetimeSeconds"] == 3600
    assert daemon["storageSizeLimit"] == "24Gi"
    limits, requests = daemon["resources"]["limits"], daemon["resources"]["requests"]
    assert limits["memory"] == "4Gi"
    assert limits["ephemeral-storage"] == "32Gi"
    assert limits["cpu"] == "2"
    assert requests["ephemeral-storage"] == "8Gi"
    assert daemon["namespace"] == DAEMON_NAMESPACE


def test_install_values_method_is_docker_values_over_the_lifecycle_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj = _scenario(tmp_path, monkeypatch, Commands())
    obj.base_values = _base_values()
    for docker in (True, False):
        expected = docker_driver.docker_values(_lifecycle_values(docker=docker), _images())
        assert obj.values(docker=docker) == expected


def test_install_start_servers_load_the_docker_scenario_transcript(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj = _scenario(tmp_path, monkeypatch, Commands())
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
    obj.start_servers()
    transcript, host = seen["model"]
    assert transcript.resolve() == (DOCKER_FIXTURES / "transcript.json").resolve()
    assert transcript.resolve() != dl.TRANSCRIPT.resolve()
    assert host == STUB_HOST
    assert seen["model_started"] and seen["stub_started"]
    assert seen["proxy"] == (STUB_HOST, Model.base_url)


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
    del obj.images["platform-next"]
    commands.handlers.append(_buildx(NEXT_DIGEST))
    obj.build_platform_next()
    [(argv, _kwargs)] = [c for c in commands.calls if c[0][:3] == ["docker", "buildx", "build"]]
    tag = f"localhost:5000/fd-{NONCE}/platform-next:candidate"
    assert _adjacent(argv, "--builder", "default")
    assert _adjacent(argv, "--platform", "linux/amd64")
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
    del obj.images["platform-next"]
    commands.handlers.append(_buildx(obj.images["platform"]["digest"]))
    with pytest.raises(RuntimeError, match="platform_next_not_distinct"):
        obj.build_platform_next()


def test_install_build_runner_cli_copies_the_binary_out_without_a_bind_mount(
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
            return 0, "sha256:" + "d" * 64 + "\n", ""
        if argv[1] == "create" or argv[1:3] == ["container", "create"]:
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
    build = calls[verbs.index("build")]
    assert _adjacent(build, "--label", "owner=factory-tools")
    assert _adjacent(build, "--label", f"factory-tools/nonce={NONCE}")
    assert _adjacent(build, "-t", tag)
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
    assert tag in calls[verbs.index(create)]
    copy_argv = calls[verbs.index("cp")]
    assert f"{container}:/src/cli/target/release/curie" in copy_argv
    assert Path(copy_argv[-1]).resolve().is_relative_to((obj.f.work / "runner-cli").resolve())
    assert container in calls[verbs.index(remove)]
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
    service = f"/api/v1/namespaces/{NAMESPACE}/services/fd-{NONCE}-api"
    obj.f.objects[service] = {
        "metadata": {"name": f"fd-{NONCE}-api", "namespace": NAMESPACE},
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
            "alias": "acme-bot", "codeHost": "github", "projectId": "4401", "path": CATALOG_REPO,
            "runner": runner | {"image": catalog}, "docker": {"enabled": True},
        },
        {
            "alias": "acme-custom", "codeHost": "github", "projectId": "4402", "path": CUSTOM_REPO,
            "runner": runner | {"image": custom, "dockerfile": "Dockerfile"},
        },
    ]  # fmt: skip
    binding = {"name": "acme-bot", "tracker": "github", "trackerScopeId": "4401", "repos": repos}
    sandbox = {"image": platform["ref"].split("@")[0], "digest": platform["digest"]}
    return {"agentSandbox": {"runner": sandbox}, "factory": {"bindings": [binding]}}


IMAGES = _images()
REGISTRY_PATH = f"localhost:5000/fd-{NONCE}"
VALUES_BEFORE = _helm_values(IMAGES["runner"]["ref"], IMAGES["runner"]["ref"], IMAGES["platform"])
CATALOG_AFTER = f"{REGISTRY_PATH}/runner-acme-bot@sha256:" + "2b" * 32
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
# cli/src/ops/upgrade.rs ClusterUpgradeOutput::Completed to_json, the `--json` document.
UPGRADE_OUTPUT = {
    "status": "completed",
    "phase": "complete",
    "target_version": "9.8.7",
    "from_version": "9.8.7",
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
        "--agent", dl.AGENT, "--channel", f"C0EXAMPLE{NONCE.upper()}",
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

    def __init__(self, curie: str, exit_code: int = 0) -> None:
        self.curie, self.exit_code = curie, exit_code
        self.values_calls: list[list[str]] = []
        self.upgrade: list[tuple[list[str], str, int]] = []
        self.message: list[tuple[list[str], dict[str, str]]] = []
        self.order: list[str] = []
        self.stdout = json.dumps(UPGRADE_OUTPUT, indent=2) + "\n"
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
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, exit_code: int = 0
) -> tuple[Any, _Upgrade, Commands]:
    commands = Commands()
    obj = _scenario(tmp_path, monkeypatch, commands)
    fake = _Upgrade(obj.curie, exit_code)
    commands.handlers.append(fake.handle)
    secret = f"/api/v1/namespaces/{NAMESPACE}/secrets/fd-{NONCE}-secrets"
    encoded = base64.b64encode(API_KEY.encode()).decode()
    meta = {"name": f"fd-{NONCE}-secrets", "namespace": NAMESPACE}
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
    expected = docker_scenario.upgrade_section(
        docker_driver.plan_text(UPGRADE_OUTPUT),
        VALUES_BEFORE, VALUES_AFTER, CATALOG_REPO, CUSTOM_REPO, STALE_REPLY,
    )  # fmt: skip
    assert obj.sections["upgrade"] == expected
    assert docker_scenario._judge_upgrade(obj.sections["upgrade"]) == []


def test_upgrade_cli_failure_keeps_its_output_and_still_builds_the_section(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj, fake, _commands = _upgrade(tmp_path, monkeypatch, exit_code=1)
    obj.scenario_upgrade()
    assert (obj.f.work / "upgrade-output.json").read_text() == fake.stdout
    assert obj.evidence["upgrade_exit_code"] == 1
    assert obj.sections["upgrade"]["catalog"]["repo"] == CATALOG_REPO
    assert obj.sections["upgrade"]["custom"]["repo"] == CUSTOM_REPO


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
        "ci", "--api-url", Stub.base_url + "/api/v3", "--repo", CATALOG_REPO,
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
        monkeypatch.setenv("GITHUB_TOKEN", token)
        section = docker_scenario.collect_ci(
            stub.base_url + "/api/v3", CATALOG_REPO, "main", sha, ["E2E required"]
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
    ) -> None:
        self.present, self.labelled = set(present), labelled or {}
        self.registry_left, self.unreachable = registry_left, unreachable
        self.namespaces: list[tuple[str | None, str]] = []
        self.docker: list[tuple[str, str]] = []

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


def test_teardown_scrub_removes_the_upgrade_values_and_runner_cli(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    obj, _commands = _teardown(tmp_path, monkeypatch, _Leftovers())
    work = obj.f.work
    (work / "upgrade.values.json").write_text("{}")
    (work / "runner-cli").mkdir()
    (work / "runner-cli/curie").write_text("#!/bin/sh\n")
    errors: list[str] = []
    obj.scrub_work(errors)
    assert errors == []
    assert not (work / "upgrade.values.json").exists()
    assert not (work / "runner-cli").exists()


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
