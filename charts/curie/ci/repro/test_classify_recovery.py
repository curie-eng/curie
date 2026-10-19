"""Classify recorded recovery failures and exercise the repeat harness boundary."""

from __future__ import annotations

import os
import re
import runpy
import shlex
import shutil
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

DIRECTORY = Path(__file__).resolve().parent
SCRIPT = DIRECTORY / "workspace-diagnostic-repeat.sh"
CLASSIFIER = DIRECTORY / "classify_recovery.py"
classify = runpy.run_path(str(CLASSIFIER))["classify"]

# Exact scenario lines, with the Actions step and timestamp prefixes removed.
# https://github.com/curie-eng/curie/actions/runs/38307426333
RECORDED_BOOLEAN = (
    'Recovery diagnostic: {"cli_error": true, "cli_exit": 1, "error_agent_listing": true, '
    '"error_http_500": true, "failed": null, "failure_class": "unrecognized-or-absent", '
    '"finalized": null, "json_object": true, "phase": "recovery", "reply_http_500": false, '
    '"reply_present": false, "reply_repository_selection": false, "timed_out": null}'
)
# https://github.com/curie-eng/curie/actions/runs/38997682583
RECORDED_STATUS = (
    'Recovery diagnostic: {"cli_error": true, "cli_exit": 1, "error_agent_listing": true, '
    '"error_http_status": 500, "error_transport": false, "failed": null, '
    '"failure_class": "unrecognized-or-absent", "finalized": null, "json_object": true, '
    '"phase": "recovery", "reply_http_500": false, "reply_present": false, '
    '"reply_repository_selection": false, "timed_out": null}'
)
RECOVERY_FAILURE = "FAIL: recovery CLI turn failed"
# https://github.com/curie-eng/curie/actions/runs/39039873799
RECORDED_PASS = (
    "PASS: repository-selection HTTP 500 reached workspace-error finalized=false; "
    "restored PostgreSQL finalized the next turn"
)
# An unrelated assertion from https://github.com/curie-eng/curie/actions/runs/39028084379
RECORDED_OTHER = (
    "E           AssertionError: github: the actual runner did not publish its bundle "
    "marker revision (expected 1); private diagnostics retained"
)


@pytest.mark.parametrize(
    ("output", "exit_code", "expected"),
    [
        (RECORDED_PASS, 0, "pass"),
        (RECORDED_BOOLEAN + "\n" + RECOVERY_FAILURE, 1, "recovery-500"),
        (RECORDED_STATUS + "\n" + RECOVERY_FAILURE, 1, "recovery-500"),
        (RECORDED_OTHER, 1, "other-fail"),
        (RECORDED_PASS, 1, "other-fail"),
        ("", 0, "pass"),
        ("", 1, "other-fail"),
    ],
)
def test_recorded_outcomes(output: str, exit_code: int, expected: str) -> None:
    assert classify(output, exit_code) == expected


@pytest.mark.parametrize(
    "diagnostic",
    [
        "",
        "Recovery diagnostic: malformed",
        'Recovery diagnostic: {"error_http_status": 500',
        'Recovery diagnostic: {"error_http_status": 500} trailing',
        "Recovery diagnostic: null",
        "Recovery diagnostic: []",
        "Recovery diagnostic: 500",
        'Recovery diagnostic: "500"',
        "Recovery diagnostic: {}",
        'Recovery diagnostic: {"error_http_status": "500"}',
        'Recovery diagnostic: {"error_http_status": 500.0}',
        'Recovery diagnostic: {"error_http_status": true}',
        'Recovery diagnostic: {"error_http_status": [500]}',
        'Recovery diagnostic: {"error_http_status": 503}',
        'Recovery diagnostic: {"error_http_500": "true"}',
        'Recovery diagnostic: {"error_http_500": 1}',
        'Recovery diagnostic: {"error_http_500": false}',
        'Recovery diagnostic: {"reply_http_500": true}',
        'Recovery diagnostic: {"error": "listing agents failed with 500"}',
        'Recovery diagnostic: {"nested": {"error_http_status": 500}}',
        'Recovery API diagnostic: {"error_http_status": 500}',
        'quoted Recovery diagnostic: {"error_http_status": 500}',
    ],
)
def test_missing_malformed_and_unrelated_diagnostics_are_other_fail(diagnostic: str) -> None:
    assert classify(diagnostic + "\n" + RECOVERY_FAILURE, 1) == "other-fail"


@pytest.mark.parametrize(
    "failure",
    [
        "",
        "quoted " + RECOVERY_FAILURE,
        RECOVERY_FAILURE + " later",
        "FAIL: initial CLI turn failed",
    ],
)
def test_http_500_requires_the_recovery_failure_line(failure: str) -> None:
    assert classify(RECORDED_STATUS + "\n" + failure, 1) == "other-fail"


def test_recovery_signature_takes_precedence_over_success_status() -> None:
    assert classify(RECORDED_BOOLEAN + "\n" + RECOVERY_FAILURE, 0) == "recovery-500"


@pytest.mark.parametrize("exit_code", [0, 1, 7, 124, 143])
def test_exit_codes_use_the_same_recovery_signature(exit_code: int) -> None:
    assert classify(RECORDED_STATUS + "\n" + RECOVERY_FAILURE, exit_code) == "recovery-500"


def test_classifier_cli_reads_the_log_and_prints_only_the_class(tmp_path: Path) -> None:
    logfile = tmp_path / "scenario.log"
    logfile.write_text(RECORDED_STATUS + "\n" + RECOVERY_FAILURE + "\n")
    result = subprocess.run(
        [sys.executable, str(CLASSIFIER), "--exit-code", "1", str(logfile)],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "recovery-500\n"
    assert result.stderr == ""


@pytest.mark.parametrize("arguments", [[], ["--exit-code", "bad"], ["--exit-code", "1"]])
def test_classifier_cli_rejects_missing_or_invalid_arguments(arguments: list[str]) -> None:
    result = subprocess.run(
        [sys.executable, str(CLASSIFIER), *arguments],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 2
    assert result.stdout == ""


def test_classifier_cli_refuses_an_unreadable_log(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(CLASSIFIER), "--exit-code", "1", str(tmp_path)],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 2
    assert result.stdout == ""


# These replace orchestration executables and the scenario process boundary.
# No API, worker, runner, database, or other product service is simulated.
TOOL_STUB = r'''
import os
import shlex
import signal
import sqlite3
import sys
import tempfile
from pathlib import Path

tool, args = Path(sys.argv[0]).name, sys.argv[1:]
cluster = os.environ.get("CURIE_REPRO_CLUSTER", "")
failure = os.environ.get("REPRO_FAILURE", "")
kubeconfig = os.environ.get("KUBECONFIG", "")
with sqlite3.connect(os.environ["REPRO_STATE"], isolation_level=None) as db:
    db.execute(
        "INSERT INTO calls(tool, arguments, cwd, kubeconfig, run_id) VALUES (?, ?, ?, ?, ?)",
        (tool, shlex.join(args), os.getcwd(), kubeconfig,
         os.environ.get("CURIE_SCENARIO_RUN_ID", "")),
    )
    if os.environ.get("REPRO_FORBID") == "1":
        raise SystemExit(91)
    if tool == "kind":
        if args == ["get", "clusters"]:
            if failure == "inventory":
                raise SystemExit(91)
            print("\n".join(row[0] for row in db.execute(
                "SELECT name FROM resources WHERE kind = 'cluster' ORDER BY name"
            )))
        elif args[:2] == ["create", "cluster"]:
            assert args[args.index("--name") + 1] == cluster
            config = Path(args[args.index("--config") + 1]).read_text()
            assert "disableDefaultCNI: true" in config
            assert 'podSubnet: "192.168.0.0/16"' in config
            assert args[args.index("--image") + 1] == "kindest/node:v1.31.0"
            assert args[args.index("--kubeconfig") + 1] == kubeconfig
            assert kubeconfig != os.environ["REPRO_CALLER_KUBECONFIG"]
            Path(kubeconfig).write_text("private kubeconfig\n")
            assert Path(kubeconfig).stat().st_mode & 0o077 == 0
            db.execute("INSERT INTO resources VALUES ('cluster', ?)", (cluster,))
            if failure == "create":
                raise SystemExit(91)
        elif args[:2] == ["delete", "cluster"]:
            assert args[args.index("--name") + 1] == cluster
            assert args[args.index("--kubeconfig") + 1] == kubeconfig
            if failure == "delete":
                raise SystemExit(91)
            db.execute("DELETE FROM resources WHERE kind = 'cluster' AND name = ?", (cluster,))
        elif args[:2] == ["load", "docker-image"]:
            assert args[args.index("--name") + 1] == cluster
            assert set(args[2:args.index("--name")]) == {
                f"curie-{component}:wsdiag-repro-{cluster}"
                for component in ("api", "worker", "runner")
            }
            if failure == "load":
                raise SystemExit(91)
        else:
            raise AssertionError(args)
    elif tool == "docker":
        if args[:2] == ["image", "ls"]:
            reference = args[args.index("--filter") + 1].removeprefix("reference=")
            rows = db.execute("SELECT name FROM resources WHERE kind = 'image' AND name = ?",
                              (reference,))
            print("\n".join(row[0] for row in rows))
        elif args[0] == "build":
            image = args[args.index("-t") + 1]
            component = image.removeprefix("curie-").split(":")[0]
            dockerfile = (
                "runner/Dockerfile" if component == "runner" else f"apps/{component}/Dockerfile"
            )
            assert args == ["build", "-f", dockerfile, "-t", image, "."]
            assert os.getcwd() == os.environ["REPRO_ROOT"]
            db.execute("INSERT INTO resources VALUES ('image', ?)", (image,))
            late_resource = os.environ.get("REPRO_LATE_RESOURCE", "")
            if component == "runner" and late_resource == "cluster":
                db.execute("INSERT INTO resources VALUES ('cluster', ?)", (cluster,))
            elif component == "api" and late_resource == "worker":
                db.execute("INSERT INTO resources VALUES ('image', ?)",
                           (f"curie-worker:wsdiag-repro-{cluster}",))
            if failure == f"build-{component}":
                raise SystemExit(91)
        elif args[:2] == ["image", "rm"]:
            assert len(args) == 3
            if failure == "remove":
                raise SystemExit(91)
            db.execute("DELETE FROM resources WHERE kind = 'image' AND name = ?", (args[2],))
        else:
            raise AssertionError(args)
    elif tool == "cargo":
        assert args == ["build", "--release", "--manifest-path", "cli/Cargo.toml"]
        if failure == "cargo":
            raise SystemExit(91)
        target = Path(os.environ.get("CARGO_TARGET_DIR", "cli/target"))
        if os.environ.get("CARGO_BUILD_TARGET"):
            target /= os.environ["CARGO_BUILD_TARGET"]
        binary = target / "release/curie"
        binary.parent.mkdir(parents=True)
        binary.write_text("#!/bin/sh\nexit 0\n")
        binary.chmod(0o755)
    elif tool == "curl":
        assert args[-1] == (
            "https://raw.githubusercontent.com/projectcalico/calico/v3.28.2/manifests/calico.yaml"
        )
        Path(args[args.index("-o") + 1]).write_text("recorded manifest boundary\n")
    elif tool == "sha256sum":
        checksum, manifest = sys.stdin.read().strip().split(maxsplit=1)
        assert checksum == "be59408bf990e96276f631d2f9285c2a0f9802194c0ad1cecdb6d9c52623a1c8"
        assert Path(manifest).read_text() == "recorded manifest boundary\n"
        if failure == "checksum":
            raise SystemExit(91)
    elif tool == "kubectl":
        assert args[:2] == ["--context", "kind-" + cluster]
        if failure == "apply" and "apply" in args:
            raise SystemExit(91)
        if failure == "signal":
            db.commit()
            os.kill(os.getppid(), signal.SIGTERM)
    elif tool == "bash":
        assert len(args) == 1 and args[0].endswith(
            "/runtime/workspace-dependency-diagnostic-runtime.sh"
        )
        assert os.environ["CURIE_SCENARIO_CONTEXT"] == "kind-" + cluster
        assert Path(os.environ["CURIE_BIN"]).is_file()
        for component in ("API", "WORKER", "RUNNER"):
            assert os.environ[f"CURIE_SCENARIO_{component}_IMAGE"] == (
                f"curie-{component.lower()}:wsdiag-repro-{cluster}"
            )
        assert os.environ["DOCKER_HOST"] == "tcp://docker.example.com:2376"
        scenario_workdir = Path(tempfile.mkdtemp(prefix="wsdiag-scenario-"))
        (scenario_workdir / "diagnostic.log").write_text("scenario diagnostics\n")
        db.execute("INSERT INTO scenario_temps VALUES (?, ?)",
                   (os.environ["CURIE_SCENARIO_RUN_ID"], str(scenario_workdir)))
        number = db.execute("SELECT COUNT(*) FROM calls WHERE tool = 'bash'").fetchone()[0]
        outcome = os.environ.get("REPRO_OUTCOMES", "pass").split(",")[number - 1]
        print("scenario-stderr", file=sys.stderr, flush=True)
        print(os.environ["REPRO_OUTPUT_" + outcome.upper()], flush=True)
        db.commit()
        raise SystemExit(0 if outcome == "pass" else 1)
    else:
        raise AssertionError((tool, args))
'''


@dataclass
class Harness:
    environment: dict[str, str]
    root: Path
    tools: Path
    state: Path
    bash: str

    def run(self) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [self.bash, str(self.root / "charts/curie/ci/repro" / SCRIPT.name)],
            cwd=self.root.parent,
            env=self.environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )

    def calls(self) -> list[tuple[str, list[str], str, str, str]]:
        with sqlite3.connect(self.state) as db:
            return [
                (tool, shlex.split(arguments), cwd, kubeconfig, run_id)
                for tool, arguments, cwd, kubeconfig, run_id in db.execute(
                    "SELECT tool, arguments, cwd, kubeconfig, run_id FROM calls ORDER BY sequence"
                )
            ]

    def resources(self) -> set[tuple[str, str]]:
        with sqlite3.connect(self.state) as db:
            return set(db.execute("SELECT kind, name FROM resources"))

    def occupy(self, kind: str, name: str) -> None:
        with sqlite3.connect(self.state) as db:
            db.execute("INSERT INTO resources VALUES (?, ?)", (kind, name))

    def forbid_tools(self) -> None:
        self.environment["REPRO_FORBID"] = "1"
        for name in (
            "python3", "dirname", "mktemp", "cat", "tee", "rm", "date", "sed", "head", "tr",
        ):
            tool = self.tools / name
            if tool.is_symlink():
                tool.unlink()
            tool.write_text(f"#!{sys.executable}\n" + TOOL_STUB)
            tool.chmod(0o755)


@pytest.fixture
def harness(tmp_path: Path) -> Harness:
    root = tmp_path / "checkout"
    repro = root / "charts/curie/ci/repro"
    repro.mkdir(parents=True)
    for source in (SCRIPT, CLASSIFIER):
        shutil.copyfile(source, repro / source.name)
    tools = tmp_path / "tools"
    tools.mkdir()
    for name in (
        "kind", "docker", "cargo", "curl", "sha256sum", "kubectl", "bash", "helm", "setsid",
    ):
        tool = tools / name
        tool.write_text(f"#!{sys.executable}\n" + TOOL_STUB)
        tool.chmod(0o755)
    python3 = tools / "python3"
    python3.write_text(f'#!/bin/sh\nexec {shlex.quote(sys.executable)} "$@"\n')
    python3.chmod(0o755)
    state = tmp_path / "state.sqlite3"
    with sqlite3.connect(state) as db:
        db.execute("CREATE TABLE resources(kind TEXT, name TEXT, PRIMARY KEY(kind, name))")
        db.execute(
            "CREATE TABLE calls(sequence INTEGER PRIMARY KEY, tool TEXT, arguments TEXT, "
            "cwd TEXT, kubeconfig TEXT, run_id TEXT)"
        )
        db.execute("CREATE TABLE scenario_temps(run_id TEXT PRIMARY KEY, path TEXT)")
    caller_kubeconfig = tmp_path / "caller-kubeconfig"
    caller_kubeconfig.write_text("caller config must survive\n")
    environment = {
        "PATH": str(tools) + os.pathsep + os.environ["PATH"],
        "HOME": str(tmp_path),
        "TMPDIR": str(tmp_path),
        "KUBECONFIG": str(caller_kubeconfig),
        "DOCKER_HOST": "tcp://docker.example.com:2376",
        "CURIE_REPRO_ITERATIONS": "1",
        "CURIE_REPRO_CLUSTER": "acme-repeat",
        "REPRO_STATE": str(state),
        "REPRO_ROOT": str(root),
        "REPRO_CALLER_KUBECONFIG": str(caller_kubeconfig),
        "REPRO_OUTPUT_PASS": RECORDED_PASS,
        "REPRO_OUTPUT_BOOLEAN": RECORDED_BOOLEAN + "\n" + RECOVERY_FAILURE,
        "REPRO_OUTPUT_STATUS": RECORDED_STATUS + "\n" + RECOVERY_FAILURE,
        "REPRO_OUTPUT_OTHER": RECORDED_OTHER,
    }
    bash = shutil.which("bash")
    assert bash is not None
    result = Harness(environment, root, tools, state, bash)
    result.occupy("cluster", "acme-existing")
    result.occupy("image", "curie-api:wsdiag-repro-acme-existing")
    return result


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("CURIE_REPRO_ITERATIONS", None),
        ("CURIE_REPRO_ITERATIONS", ""),
        ("CURIE_REPRO_ITERATIONS", "0"),
        ("CURIE_REPRO_ITERATIONS", "201"),
        ("CURIE_REPRO_ITERATIONS", "-1"),
        ("CURIE_REPRO_ITERATIONS", "1.5"),
        ("CURIE_REPRO_ITERATIONS", "forty"),
        ("CURIE_REPRO_ITERATIONS", "1\n"),
        ("CURIE_REPRO_ITERATIONS", "9" * 100),
        ("CURIE_REPRO_ITERATIONS", "1+1"),
        ("CURIE_REPRO_CLUSTER", None),
        ("CURIE_REPRO_CLUSTER", ""),
        ("CURIE_REPRO_CLUSTER", "Uppercase"),
        ("CURIE_REPRO_CLUSTER", "-leading"),
        ("CURIE_REPRO_CLUSTER", "trailing-"),
        ("CURIE_REPRO_CLUSTER", "with/slash"),
        ("CURIE_REPRO_CLUSTER", "with.dot"),
        ("CURIE_REPRO_CLUSTER", "two names"),
        ("CURIE_REPRO_CLUSTER", "a\nother"),
        ("CURIE_REPRO_CLUSTER", "a" * 50),
    ],
)
def test_invalid_inputs_fail_before_any_tool(
    harness: Harness, variable: str, value: str | None
) -> None:
    if value is None:
        harness.environment.pop(variable)
    else:
        harness.environment[variable] = value
    harness.forbid_tools()
    before = harness.resources()
    result = harness.run()
    assert result.returncode == 2, result.stdout + result.stderr
    assert variable in result.stderr
    assert harness.calls() == []
    assert harness.resources() == before


@pytest.mark.parametrize("iterations", ["1", "200", "001"])
@pytest.mark.parametrize("cluster", ["a", "a" * 49])
def test_valid_boundaries_reach_inventory(harness: Harness, iterations: str, cluster: str) -> None:
    harness.environment.update(
        CURIE_REPRO_ITERATIONS=iterations, CURIE_REPRO_CLUSTER=cluster, REPRO_FAILURE="inventory"
    )
    result = harness.run()
    assert result.returncode == 1, result.stdout + result.stderr
    assert [(tool, args) for tool, args, *_ in harness.calls()] == [("kind", ["get", "clusters"])]


@pytest.mark.parametrize("kind", ["cluster", "api", "worker", "runner"])
def test_existing_resources_are_refused_without_taking_ownership(
    harness: Harness, kind: str
) -> None:
    cluster = harness.environment["CURIE_REPRO_CLUSTER"]
    harness.occupy(
        "cluster" if kind == "cluster" else "image",
        cluster if kind == "cluster" else f"curie-{kind}:wsdiag-repro-{cluster}",
    )
    before = harness.resources()
    result = harness.run()
    assert result.returncode == 1, result.stdout + result.stderr
    assert harness.resources() == before
    assert all(
        (tool == "kind" and args == ["get", "clusters"])
        or (tool == "docker" and args[:2] == ["image", "ls"])
        for tool, args, *_ in harness.calls()
    )


@pytest.mark.parametrize(
    ("late_resource", "built_components"),
    [("cluster", ("api", "worker", "runner")), ("worker", ("api",))],
)
def test_resources_appearing_during_builds_are_refused_without_taking_ownership(
    harness: Harness, late_resource: str, built_components: tuple[str, ...]
) -> None:
    harness.environment["REPRO_LATE_RESOURCE"] = late_resource
    cluster = harness.environment["CURIE_REPRO_CLUSTER"]
    foreign = (
        ("cluster", cluster)
        if late_resource == "cluster"
        else ("image", f"curie-{late_resource}:wsdiag-repro-{cluster}")
    )
    before = harness.resources()
    result = harness.run()
    assert result.returncode == 1, result.stdout + result.stderr
    assert "refusing" in result.stderr
    assert "summary iterations=" not in result.stdout
    calls = harness.calls()
    assert not any(tool == "bash" for tool, *_ in calls)
    assert all(args == ["get", "clusters"] for tool, args, *_ in calls if tool == "kind")
    built = [
        args[args.index("-t") + 1]
        for tool, args, *_ in calls
        if tool == "docker" and args[0] == "build"
    ]
    expected_images = [
        f"curie-{component}:wsdiag-repro-{cluster}" for component in built_components
    ]
    assert built == expected_images
    removed = [
        args[2] for tool, args, *_ in calls if tool == "docker" and args[:2] == ["image", "rm"]
    ]
    assert len(removed) == len(expected_images)
    assert set(removed) == set(expected_images)
    assert harness.resources() == before | {foreign}
    assert Path(harness.environment["KUBECONFIG"]).read_text() == "caller config must survive\n"
    private_configs = {config for _, _, _, config, _ in calls if config}
    private_configs.discard(harness.environment["KUBECONFIG"])
    assert len(private_configs) == 1
    assert all(not Path(config).parent.exists() for config in private_configs)


def test_invocation_from_repro_directory_without_a_slash_reaches_the_scenario(
    harness: Harness,
) -> None:
    before = harness.resources()
    result = subprocess.run(
        [harness.bash, SCRIPT.name],
        cwd=harness.root / "charts/curie/ci/repro",
        env=harness.environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "summary iterations=1 pass=1 recovery_500=0 other_fail=0\n" in result.stdout
    calls = harness.calls()
    assert sum(tool == "bash" for tool, *_ in calls) == 1
    assert all(cwd == str(harness.root) for _, _, cwd, _, _ in calls)
    assert harness.resources() == before


def test_caller_build_targets_and_scenario_temps_remain_owned(harness: Harness) -> None:
    caller_target = harness.root.parent / "caller-target"
    harness.environment.update(
        CARGO_TARGET_DIR=str(caller_target),
        CARGO_BUILD_TARGET="aarch64-unknown-linux-gnu",
        CURIE_REPRO_ITERATIONS="2",
        REPRO_OUTCOMES="other,pass",
    )
    before = harness.resources()
    result = harness.run()
    assert result.returncode == 0, result.stdout + result.stderr
    assert "iteration=1 result=other-fail\n" in result.stdout
    assert "iteration=2 result=pass\n" in result.stdout
    assert "summary iterations=2 pass=1 recovery_500=0 other_fail=1\n" in result.stdout
    assert (harness.root / "cli/target/release/curie").is_file()
    assert not caller_target.exists()
    calls = harness.calls()
    run_ids = [run_id for tool, *_, run_id in calls if tool == "bash"]
    assert len(run_ids) == 2
    private_workdirs = {
        Path(config).parent
        for _, _, _, config, _ in calls
        if config and config != harness.environment["KUBECONFIG"]
    }
    assert len(private_workdirs) == 1
    workdir = private_workdirs.pop()
    with sqlite3.connect(harness.state) as db:
        allocations = db.execute("SELECT run_id, path FROM scenario_temps").fetchall()
    assert {run_id for run_id, _ in allocations} == set(run_ids)
    assert len(allocations) == 2
    assert all(Path(path).is_relative_to(workdir) for _, path in allocations)
    assert all(not Path(path).exists() for _, path in allocations)
    assert not workdir.exists()
    assert harness.resources() == before


def test_all_classes_continue_with_combined_output_and_owned_cleanup(harness: Harness) -> None:
    harness.environment.update(
        CURIE_REPRO_ITERATIONS="4", REPRO_OUTCOMES="pass,boolean,status,other"
    )
    before = harness.resources()
    result = harness.run()
    assert result.returncode == 0, result.stdout + result.stderr
    for number, outcome in enumerate(("pass", "recovery-500", "recovery-500", "other-fail"), 1):
        assert f"iteration={number} result={outcome}\n" in result.stdout
    assert "summary iterations=4 pass=1 recovery_500=2 other_fail=1\n" in result.stdout
    for recorded in (RECORDED_PASS, RECORDED_BOOLEAN, RECORDED_STATUS, RECORDED_OTHER):
        assert recorded + "\n" in result.stdout
    assert result.stdout.count("scenario-stderr\n") == 4
    calls = harness.calls()
    run_ids = [run_id for tool, *_, run_id in calls if tool == "bash"]
    assert len(run_ids) == len(set(run_ids)) == 4
    assert all(re.fullmatch(r"[a-z0-9][a-z0-9-]{0,24}", run_id) for run_id in run_ids)
    kubectl = [args for tool, args, *_ in calls if tool == "kubectl"]
    assert [args[2:] for args in kubectl] == [
        ["apply", "-f", kubectl[0][-1]],
        ["-n", "kube-system", "rollout", "status", "daemonset/calico-node", "--timeout=300s"],
        ["wait", "--for=condition=Ready", "node", "--all", "--timeout=300s"],
    ]
    assert harness.resources() == before
    assert Path(harness.environment["KUBECONFIG"]).read_text() == "caller config must survive\n"
    private_configs = {config for _, _, _, config, _ in calls if config}
    private_configs.discard(harness.environment["KUBECONFIG"])
    assert len(private_configs) == 1
    assert all(not Path(config).parent.exists() for config in private_configs)


@pytest.mark.parametrize(
    "failure",
    [
        "cargo", "build-api", "build-worker", "build-runner", "create", "checksum", "apply",
        "load", "signal",
    ],
)
def test_failed_setup_cleans_partial_owned_resources(harness: Harness, failure: str) -> None:
    harness.environment["REPRO_FAILURE"] = failure
    before = harness.resources()
    result = harness.run()
    assert result.returncode == 1, result.stdout + result.stderr
    assert "summary iterations=" not in result.stdout
    assert not any(tool == "bash" for tool, *_ in harness.calls())
    assert harness.resources() == before


@pytest.mark.parametrize("failure", ["delete", "remove"])
def test_cleanup_failure_changes_a_completed_measurement_to_exit_one(
    harness: Harness, failure: str
) -> None:
    harness.environment["REPRO_FAILURE"] = failure
    result = harness.run()
    assert result.returncode == 1, result.stdout + result.stderr
    assert "summary iterations=1 pass=1 recovery_500=0 other_fail=0\n" in result.stdout
    assert "FAIL:" in result.stderr
    assert ("cluster", "acme-existing") in harness.resources()
    assert ("image", "curie-api:wsdiag-repro-acme-existing") in harness.resources()
    removals = [args for tool, args, *_ in harness.calls() if tool == "docker" and args[1] == "rm"]
    assert len(removals) == 3, "cleanup must attempt every owned tag despite an earlier failure"
