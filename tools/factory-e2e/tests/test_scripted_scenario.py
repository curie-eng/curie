"""Negative controls for the scripted factory scenario (#3814).

The legacy helpers are the #3521 and #3617 behaviors. The test shows that
putting either one back makes the scenario assertion fail. The product does
not call them.
"""

from __future__ import annotations

import copy
import importlib.util
import json
import signal
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

ROOT = Path(__file__).resolve().parents[1]
_ORIGINAL_SUBPROCESS_RUN = subprocess.run


def _with_curie_version_probe(
    command: Callable[..., subprocess.CompletedProcess[str]], candidate_curie: Path
) -> Callable[..., subprocess.CompletedProcess[str]]:
    def run(argv: list[str] | tuple[str, ...], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if argv == [str(candidate_curie), "--version"]:
            return _ORIGINAL_SUBPROCESS_RUN(argv, **kwargs)
        assert isinstance(argv, tuple)
        return command(argv, **kwargs)

    return run


def _load():
    spec = importlib.util.spec_from_file_location(
        "curie_scripted_scenario", ROOT / "scripted_scenario.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


scenario = _load()


def _load_kind():
    spec = importlib.util.spec_from_file_location("curie_scripted_kind", ROOT / "scripted_kind.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_declared_unittest_preflight_is_accepted() -> None:
    scenario.assert_preflight_outcome("passed")
    scenario.assert_preflight_outcome("failed")


def test_unavailable_preflight_is_rejected() -> None:
    with pytest.raises(scenario.ScenarioAssertionError, match="unavailable"):
        scenario.assert_preflight_outcome("unavailable")


def test_reintroducing_hardcoded_preflight_fails_the_scenario() -> None:
    outcome = scenario.legacy_3521_preflight(scenario.FIXTURE_CHECK)
    with pytest.raises(scenario.ScenarioAssertionError):
        scenario.assert_preflight_outcome(outcome)


def test_fixture_paths_are_on_the_publication_request() -> None:
    scenario.assert_publication_paths(list(scenario.FIXTURE_CHANGED_PATHS))


def test_scripted_values_put_the_proxy_on_the_worker() -> None:
    kind = _load_kind()
    values = kind.scripted_values(
        model_base_url="http://10.1.2.3:8080",
        github_api="https://10.9.9.9:8443/api/v3",
        clone_base="https://10.9.9.9:8443",
        ca_configmap="github-stub-ca",
        runner_image="localhost:24111/curie-runner",
    )
    assert values["agentSandbox"]["runner"]["image"] == "localhost:24111/curie-runner"
    assert {"name": "CURIE_MODEL_BASE_URL", "value": "http://10.1.2.3:8080"} in values["worker"][
        "extraEnv"
    ]
    assert "githubStubCaConfigMap" not in values["api"]
    factory = values["factory"]
    assert {entry["apiUrl"] for entry in factory["codeHosts"] + factory["trackers"]} == {
        "https://10.9.9.9:8443/api/v3"
    }
    assert factory["trackers"][0]["pollIntervalSeconds"] == 5
    assert values["codeHostTrust"]["caBundle"]["configMapRef"] == {
        "name": "github-stub-ca", "key": "ca.pem"
    }
    assert {"cidr": "10.1.2.3/32", "ports": [{"protocol": "TCP", "port": 8080}]} in values[
        "security"
    ]["networkPolicy"]["allowedEgress"]


@pytest.mark.parametrize(
    ("github_api", "expected_authority"),
    [
        ("https://github-stub.example.com:8443/api/v3", "github-stub.example.com:8443"),
        ("http://127.0.0.1/api", "127.0.0.1"),
    ],
)
def test_scripted_values_put_github_hosts_on_the_stub(
    github_api: str,
    expected_authority: str,
) -> None:
    kind = _load_kind()
    values = kind.scripted_values(
        model_base_url="http://10.1.2.3:8080",
        github_api=github_api,
        clone_base="https://10.9.9.9:8443",
        ca_configmap="github-stub-ca",
        runner_image="localhost:24111/curie-runner",
    )
    for collection in ("codeHosts", "trackers"):
        github_entries = [
            entry for entry in values["factory"][collection] if entry["kind"] == "github"
        ]
        assert github_entries, f"{collection} must contain GitHub entries"
        for entry in github_entries:
            assert entry["host"] == expected_authority
            assert entry["apiUrl"] == github_api


# Recorded from `kubectl get networkpolicy -A -l app.kubernetes.io/component=publication
# -o json` on the scripted kind install (chart 0.12.4). Trimmed to two GitHub CIDRs,
# with metadata reduced to name, namespace and labels.
RECORDED_PUBLICATION_POLICY: dict[str, Any] = {
    "apiVersion": "networking.k8s.io/v1",
    "kind": "NetworkPolicy",
    "metadata": {
        "name": "curie-publication-egress",
        "namespace": "curie-curie-publication",
        "labels": {
            "app.kubernetes.io/component": "publication",
            "app.kubernetes.io/instance": "curie",
            "app.kubernetes.io/managed-by": "Helm",
            "app.kubernetes.io/name": "curie",
            "app.kubernetes.io/version": "0.12.4",
            "helm.sh/chart": "curie-0.12.4",
        },
    },
    "spec": {
        "egress": [
            {
                "ports": [{"port": 53, "protocol": "UDP"}, {"port": 53, "protocol": "TCP"}],
                "to": [
                    {
                        "namespaceSelector": {
                            "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
                        }
                    }
                ],
            },
            {
                "ports": [{"port": 443, "protocol": "TCP"}],
                "to": [
                    {"ipBlock": {"cidr": "192.30.252.0/22"}},
                    {"ipBlock": {"cidr": "185.199.108.0/22"}},
                ],
            },
        ],
        "podSelector": {"matchLabels": {"curietech.ai/component": "publication"}},
        "policyTypes": ["Ingress", "Egress"],
    },
}


def _policy_list(*items: dict[str, Any]) -> dict[str, Any]:
    return {"apiVersion": "v1", "kind": "List", "items": list(items)}


def test_publication_stub_egress_allows_only_the_stub_for_the_chart_publication_pods() -> None:
    kind = _load_kind()
    policy = kind.publication_stub_egress_policy(
        _policy_list(RECORDED_PUBLICATION_POLICY), "https://172.18.0.1:40095"
    )
    assert policy["apiVersion"] == "networking.k8s.io/v1"
    assert policy["kind"] == "NetworkPolicy"
    assert policy["metadata"] == {
        "name": "scripted-github-stub-publication-egress",
        "namespace": "curie-curie-publication",
    }
    assert policy["spec"]["podSelector"] == {
        "matchLabels": {"curietech.ai/component": "publication"}
    }
    assert policy["spec"]["policyTypes"] == ["Egress"]
    assert policy["spec"]["egress"] == [
        {
            "to": [{"ipBlock": {"cidr": "172.18.0.1/32"}}],
            "ports": [{"protocol": "TCP", "port": 40095}],
        }
    ]


def test_publication_stub_egress_copies_namespace_and_selector_from_the_chart_policy() -> None:
    kind = _load_kind()
    chart_policy = copy.deepcopy(RECORDED_PUBLICATION_POLICY)
    chart_policy["metadata"]["namespace"] = "team-a-curie-publication"
    other_selector = {
        "matchLabels": {"curietech.ai/component": "publication", "example": "other-release"}
    }
    chart_policy["spec"]["podSelector"] = other_selector
    policy = kind.publication_stub_egress_policy(
        _policy_list(chart_policy), "https://10.9.9.9:8443"
    )
    assert policy["metadata"]["namespace"] == "team-a-curie-publication"
    assert policy["spec"]["podSelector"] == other_selector
    assert policy["spec"]["policyTypes"] == ["Egress"]
    assert policy["spec"]["egress"] == [
        {
            "to": [{"ipBlock": {"cidr": "10.9.9.9/32"}}],
            "ports": [{"protocol": "TCP", "port": 8443}],
        }
    ]


@pytest.mark.parametrize("count", [0, 2], ids=["no-chart-policy", "two-chart-policies"])
def test_publication_stub_egress_refuses_an_ambiguous_chart_policy_set(count: int) -> None:
    kind = _load_kind()
    with pytest.raises(kind.ScriptedScenarioError, match="exactly one"):
        kind.publication_stub_egress_policy(
            _policy_list(*[RECORDED_PUBLICATION_POLICY] * count), "https://172.18.0.1:40095"
        )


def test_reintroducing_curie_path_prefixes_fails_the_scenario() -> None:
    filtered = scenario.legacy_3617_paths(scenario.FIXTURE_CHANGED_PATHS)
    with pytest.raises(scenario.ScenarioAssertionError, match="unitconv"):
        scenario.assert_publication_paths(filtered)


@pytest.mark.parametrize("outcome", ["passed", "failed"])
def test_driver_verdict_accepts_an_observed_preflight_and_both_fixture_paths(
    outcome: str,
) -> None:
    kind = _load_kind()
    assert kind.verdict(outcome, list(scenario.FIXTURE_CHANGED_PATHS)) == 0


def test_driver_verdict_rejects_an_unavailable_preflight() -> None:
    kind = _load_kind()
    assert kind.verdict("unavailable", list(scenario.FIXTURE_CHANGED_PATHS)) == 1


@pytest.mark.parametrize("missing", scenario.FIXTURE_CHANGED_PATHS)
def test_driver_verdict_rejects_each_missing_fixture_path(missing: str) -> None:
    kind = _load_kind()
    paths = [path for path in scenario.FIXTURE_CHANGED_PATHS if path != missing]
    assert kind.verdict("passed", paths) == 1


@pytest.mark.parametrize("flag", ["--context", "--model-base-url"])
def test_scripted_parser_refuses_removed_external_cluster_flags(flag: str) -> None:
    import factory_e2e as fe

    with pytest.raises(SystemExit):
        fe.parse_args(["scripted", flag, "example"])


def test_scripted_parser_owns_its_cluster_and_supports_recording_and_image_builds() -> None:
    import factory_e2e as fe

    args = fe.parse_args(["scripted", "--build-images", "--record"])
    assert args.build_images is True
    assert args.record is True
    assert not hasattr(args, "context")
    assert not hasattr(args, "model_base_url")


@pytest.mark.parametrize(
    "failure_step", ["namespace", "get", "describe", "previous", "collector", "interrupt"]
)
def test_failed_diagnostics_preserve_scenario_error_and_owned_cleanup(
    failure_step: str,
    tmp_path: Path,
    candidate_curie: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    kind = _load_kind()
    monkeypatch.setattr(kind, "REPO_ROOT", tmp_path)
    calls: list[tuple[str, ...]] = []
    collector_called = False

    def execute(run: Any) -> None:
        run.cluster_attempted = True
        run.deadline = 0
        raise kind.ScriptedScenarioError("publication clone exited 124")

    def command(argv: tuple[str, ...], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        output = ""
        if argv[0] == "kubectl":
            operation = argv[5:]
            if operation[:2] == ("get", "deployment/curie-worker"):
                if failure_step == "namespace":
                    raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
                output = "curie-publication"
            elif operation[:2] == ("get", "pods"):
                if failure_step == "get":
                    raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
                output = "acme-pod\tFailed\n"
            elif operation[0] == "describe":
                if failure_step == "describe":
                    return subprocess.CompletedProcess(argv, 1, "", "diagnostic command failed")
                output = "Status: Failed\n"
            elif operation[0] == "logs":
                if failure_step == "previous" and "--previous" in operation:
                    raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
                output = "pod diagnostic marker\n"
            else:
                raise AssertionError(f"unexpected diagnostic command: {argv}")
        else:
            assert argv[:3] in {
                ("kind", "delete", "cluster"),
                ("kind", "get", "clusters"),
                ("docker", "ps", "-aq"),
            }
        return subprocess.CompletedProcess(argv, 0, output, "")

    def broken_collector(_run: Any) -> None:
        nonlocal collector_called
        collector_called = True
        if failure_step == "interrupt":
            raise KeyboardInterrupt
        raise AssertionError("unexpected diagnostic failure")

    monkeypatch.setattr(kind.ScriptedRun, "execute", execute)
    if failure_step in ("collector", "interrupt"):
        monkeypatch.setattr(kind.ScriptedRun, "capture_failure_diagnostics", broken_collector)
    monkeypatch.setattr(kind.subprocess, "run", _with_curie_version_probe(command, candidate_curie))

    assert kind.main(kind.fe.parse_args(["scripted"])) == kind.fe.EXIT_FAILED
    (evidence_file,) = (tmp_path / "target/factory-e2e").glob("scripted-*.json")
    evidence = json.loads(evidence_file.read_text())
    assert evidence["error"] == "publication clone exited 124"
    assert evidence["cleanup_verified"] is True
    assert evidence["cleanup_errors"] == []
    assert ("kind", "delete", "cluster", "--name", evidence["cluster"]) in calls
    assert ("kind", "get", "clusters") in calls
    assert (
        "docker", "ps", "-aq", "--filter", f"label=io.x-k8s.kind.cluster={evidence['cluster']}"
    ) in calls
    assert "failure_diagnostics" not in evidence_file.read_text()
    assert "pod diagnostic marker" not in evidence_file.read_text()
    if failure_step in ("namespace", "describe", "previous"):
        assert "pod diagnostic marker" in capsys.readouterr().err
        assert any(argv[0] == "kubectl" and argv[5] == "logs" for argv in calls)
    diagnostic_indexes = [i for i, argv in enumerate(calls) if argv[0] == "kubectl"]
    if failure_step in ("collector", "interrupt"):
        assert collector_called is True
    else:
        assert diagnostic_indexes
        assert max(diagnostic_indexes) < calls.index(
            ("kind", "delete", "cluster", "--name", evidence["cluster"])
        )


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM], ids=["SIGINT", "SIGTERM"])
def test_interrupted_scenario_skips_diagnostics_preserves_failure_and_owned_cleanup(
    signum: int,
    tmp_path: Path,
    candidate_curie: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    kind = _load_kind()
    monkeypatch.setattr(kind, "REPO_ROOT", tmp_path)
    calls: list[tuple[str, ...]] = []
    diagnostic_calls: list[str] = []
    previous_handlers = {
        handled_signum: kind.signal.getsignal(handled_signum)
        for handled_signum in (signal.SIGINT, signal.SIGTERM)
    }

    def execute(run: Any) -> None:
        run.cluster_attempted = True
        run.deadline = 0
        handler = kind.signal.getsignal(signum)
        assert callable(handler)
        assert handler != previous_handlers[signum]
        handler(signum, None)

    def diagnostics(run: Any) -> None:
        diagnostic_calls.append(run.cluster)

    def command(argv: tuple[str, ...], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        assert argv[:3] in {
            ("kind", "delete", "cluster"),
            ("kind", "get", "clusters"),
            ("docker", "ps", "-aq"),
        }
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(kind.ScriptedRun, "execute", execute)
    monkeypatch.setattr(kind.ScriptedRun, "capture_failure_diagnostics", diagnostics)
    monkeypatch.setattr(kind.subprocess, "run", _with_curie_version_probe(command, candidate_curie))

    assert kind.main(kind.fe.parse_args(["scripted"])) == kind.fe.EXIT_FAILED
    (evidence_file,) = (tmp_path / "target/factory-e2e").glob("scripted-*.json")
    evidence = json.loads(evidence_file.read_text())
    assert evidence["result"] == "failed"
    assert evidence["error"] == f"scripted scenario interrupted by signal {signum}"
    assert evidence["cleanup_verified"] is True
    assert evidence["cleanup_errors"] == []
    assert diagnostic_calls == []
    assert ("kind", "delete", "cluster", "--name", evidence["cluster"]) in calls
    assert ("kind", "get", "clusters") in calls
    assert (
        "docker", "ps", "-aq", "--filter", f"label=io.x-k8s.kind.cluster={evidence['cluster']}"
    ) in calls
    assert all(argv[0] != "kubectl" for argv in calls)
    assert "pod diagnostics" not in capsys.readouterr().err
    assert "failure_diagnostics" not in evidence
    for handled_signum, previous_handler in previous_handlers.items():
        assert kind.signal.getsignal(handled_signum) == previous_handler


def test_configuration_failure_before_cluster_skips_diagnostics(
    tmp_path: Path,
    candidate_curie: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    kind = _load_kind()
    monkeypatch.setattr(kind, "REPO_ROOT", tmp_path)
    calls: list[tuple[str, ...]] = []
    diagnostic_calls: list[str] = []

    def execute(run: Any) -> None:
        assert run.cluster_attempted is False
        raise kind.fe.ConfigError("local image acme-worker:local is missing")

    def diagnostics(run: Any) -> None:
        diagnostic_calls.append(run.cluster)

    def command(argv: tuple[str, ...], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        raise AssertionError(f"unexpected command before cluster creation: {argv}")

    monkeypatch.setattr(kind.ScriptedRun, "execute", execute)
    monkeypatch.setattr(kind.ScriptedRun, "capture_failure_diagnostics", diagnostics)
    monkeypatch.setattr(kind.subprocess, "run", _with_curie_version_probe(command, candidate_curie))

    assert kind.main(kind.fe.parse_args(["scripted"])) == kind.fe.EXIT_CONFIG
    (evidence_file,) = (tmp_path / "target/factory-e2e").glob("scripted-*.json")
    evidence = json.loads(evidence_file.read_text())
    assert evidence["result"] == "failed"
    assert evidence["error"] == "local image acme-worker:local is missing"
    assert evidence["cleanup_verified"] is True
    assert evidence["cleanup_errors"] == []
    assert diagnostic_calls == []
    assert calls == []
    assert "pod diagnostics" not in capsys.readouterr().err
    assert "failure_diagnostics" not in evidence


def test_successful_scenario_skips_pod_diagnostics(
    tmp_path: Path,
    candidate_curie: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    kind = _load_kind()
    monkeypatch.setattr(kind, "REPO_ROOT", tmp_path)
    calls: list[tuple[str, ...]] = []

    def execute(run: Any) -> None:
        run.cluster_attempted = True

    def command(argv: tuple[str, ...], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        assert argv[:3] in {
            ("kind", "delete", "cluster"),
            ("kind", "get", "clusters"),
            ("docker", "ps", "-aq"),
        }
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(kind.ScriptedRun, "execute", execute)
    monkeypatch.setattr(kind.subprocess, "run", _with_curie_version_probe(command, candidate_curie))

    assert kind.main(kind.fe.parse_args(["scripted"])) == 0
    (evidence_file,) = (tmp_path / "target/factory-e2e").glob("scripted-*.json")
    evidence = json.loads(evidence_file.read_text())
    assert evidence["result"] == "passed"
    assert evidence["cleanup_verified"] is True
    assert all(argv[0] != "kubectl" for argv in calls)
    assert "pod diagnostics" not in capsys.readouterr().err
    assert "failure_diagnostics" not in evidence


@pytest.mark.parametrize(
    "truncate_key", [False, True], ids=["complete-key", "kubectl-truncated-key"]
)
def test_failure_pod_diagnostics_are_bounded_redacted_and_stderr_only(
    truncate_key: bool,
    tmp_path: Path,
    candidate_curie: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    kind = _load_kind()
    monkeypatch.setattr(kind, "REPO_ROOT", tmp_path)
    calls: list[tuple[tuple[str, ...], float]] = []
    known_secret = "example-known-credential"
    shaped_secret = "ghp_" + "a" * 36
    credential_url = "https://user:password@clone.example.com/repo?token=example-query-secret"
    postgresql_url = "postgresql://user:password@database.example.com/acme"
    redis_url = "redis://:password@cache.example.com/0"
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ).decode()
    private_key_lines = private_key.splitlines()
    # Place the key across the recorder boundary to exercise redaction before truncation.
    key_prefix = "example diagnostic context " * 120 + "\n"
    log_output = (
        f"pod diagnostic marker {known_secret} {shaped_secret} {credential_url}\n"
        f"database: {postgresql_url}\ncache: {redis_url}\n"
        + key_prefix + private_key
        + "é" * 6000
    )
    captured_key_body_lines = private_key_lines[1:-1]
    if truncate_key:
        # Model kubectl cutting the subprocess output before the PEM footer,
        # including a partial final body line that must also be redacted.
        log_output = log_output.encode("utf-8")[:4096].decode("utf-8")
        assert private_key_lines[0] in log_output
        assert private_key_lines[-1] not in log_output
        captured_key_body_lines = log_output.split(private_key_lines[0] + "\n", 1)[1].splitlines()
        assert len(captured_key_body_lines) >= 2
        assert captured_key_body_lines[-1] not in private_key_lines

    def execute(run: Any) -> None:
        run.cluster_attempted = True
        run.deadline = 0
        run.secrets.append(known_secret)
        raise kind.ScriptedScenarioError("publication clone exited 124")

    def command(argv: tuple[str, ...], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((argv, kwargs["timeout"]))
        output = ""
        if argv[0] == "kubectl":
            operation = argv[5:]
            if operation[:2] == ("get", "deployment/curie-worker"):
                output = "curie-publication"
            elif operation[:2] == ("get", "pods"):
                assert ".items[:4]" in operation[-1]
                # Also enforce the pod cap when the external command overproduces.
                output = "".join(f"acme-pod-{i}\tFailed\n" for i in range(6))
            elif operation[0] == "describe":
                output = (
                    "Annotations: arbitrary-annotation-input\n"
                    "Status: Failed\nContainers:\n  worker:\n"
                    "    State: Waiting\n      Reason: CrashLoopBackOff\n"
                    "    Last State: Terminated\n      Exit Code: 124\n"
                    f"      Message: clone failed at {credential_url}\n"
                    "    Ready: False\n    Restart Count: 3\n"
                    "    Command:\n      arbitrary-command-input\n"
                    "    Args:\n      arbitrary-argument-input\n"
                    "    Environment Variables from:\n      arbitrary-env-source\n"
                    "    Environment:\n      RANDOM_SETTING: unlisted-env-credential\n"
                    "      MULTILINE_SETTING:\n        multiline-env-credential\n"
                    "    Mounts:\n      arbitrary-mount-input\n"
                    "Events:\n  Type     Reason   Age  From     Message\n"
                    f"  Warning  BackOff  1s   kubelet  {known_secret}\n"
                )
            elif operation[0] == "logs":
                assert "--tail=40" in operation
                assert "--limit-bytes=4096" in operation
                assert "--pod-running-timeout=2s" in operation
                output = log_output
            else:
                raise AssertionError(f"unexpected diagnostic command: {argv}")
        else:
            assert argv[:3] in {
                ("kind", "delete", "cluster"),
                ("kind", "get", "clusters"),
                ("docker", "ps", "-aq"),
            }
        return subprocess.CompletedProcess(argv, 0, output, "")

    monkeypatch.setattr(kind.ScriptedRun, "execute", execute)
    monkeypatch.setattr(kind.subprocess, "run", _with_curie_version_probe(command, candidate_curie))

    assert kind.main(kind.fe.parse_args(["scripted"])) == kind.fe.EXIT_FAILED
    captured = capsys.readouterr()
    (evidence_file,) = (tmp_path / "target/factory-e2e").glob("scripted-*.json")
    evidence = json.loads(evidence_file.read_text())
    assert captured.out == ""
    assert "pod diagnostic marker" in captured.err
    assert "CrashLoopBackOff" in captured.err
    assert "Exit Code: 124" in captured.err
    assert "[REDACTED]" in captured.err
    assert captured.err.splitlines().count("database: https://example.com") == 16
    assert captured.err.splitlines().count("cache: https://example.com") == 16
    for private_value in (
        known_secret,
        shaped_secret,
        credential_url,
        postgresql_url,
        redis_url,
        "user:password@",
        ":password@",
        *private_key_lines,
        *captured_key_body_lines,
        "example-query-secret",
        "arbitrary-annotation-input",
        "arbitrary-command-input",
        "arbitrary-argument-input",
        "arbitrary-env-source",
        "unlisted-env-credential",
        "multiline-env-credential",
        "arbitrary-mount-input",
    ):
        assert private_value not in captured.err
        assert private_value not in evidence_file.read_text()
    assert "pod diagnostic marker" not in evidence_file.read_text()
    assert "CrashLoopBackOff" not in evidence_file.read_text()
    assert "failure_diagnostics" not in evidence_file.read_text()
    assert evidence["error"] == "publication clone exited 124"
    assert evidence["cleanup_verified"] is True
    diagnostic_calls = [(argv, timeout) for argv, timeout in calls if argv[0] == "kubectl"]
    for argv, timeout in diagnostic_calls:
        assert argv[:3] == ("kubectl", "--context", f"kind-{evidence['cluster']}")
        assert argv[3] == "-n"
        assert argv[4] in {"curie", "curie-publication"}
        assert 0 < timeout <= 10
    for namespace in ("curie", "curie-publication"):
        describe_calls = [
            argv for argv, _ in diagnostic_calls if argv[4:6] == (namespace, "describe")
        ]
        assert len(describe_calls) == 4
        for previous in (False, True):
            log_calls = [
                argv
                for argv, _ in diagnostic_calls
                if argv[4:6] == (namespace, "logs") and ("--previous" in argv) == previous
            ]
            assert len(log_calls) == 4
    unicode_lines = [line for line in captured.err.splitlines() if line.startswith("é")]
    if truncate_key:
        assert unicode_lines == []
        assert captured.err.count("[REDACTED PRIVATE KEY]") == 16
    else:
        assert len(unicode_lines) == 16
        assert all(len(line.encode("utf-8")) < 4096 for line in unicode_lines)


def test_failure_diagnostics_stop_at_budget_and_restore_scenario_deadline(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    kind = _load_kind()
    run = kind.ScriptedRun(kind.fe.parse_args(["scripted"]))
    run.deadline = -1
    evidence_before = run.evidence.copy()
    calls: list[tuple[str, ...]] = []
    elapsed = 0.0

    def command(argv: tuple[str, ...], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        nonlocal elapsed
        calls.append(argv)
        assert argv[:3] == ("kubectl", "--context", run.context)
        assert kwargs["timeout"] > 0.1
        elapsed += kwargs["timeout"]
        operation = argv[5:]
        if operation[:2] == ("get", "deployment/curie-worker"):
            output = "curie-publication"
        elif operation[:2] == ("get", "pods"):
            output = "acme-pod\tFailed\n"
        elif operation[0] == "describe":
            output = "Status: Failed\n"
        else:
            assert operation[0] == "logs"
            output = "previous container diagnostic\n"
        return subprocess.CompletedProcess(argv, 0, output, "")

    monkeypatch.setattr(kind.time, "monotonic", lambda: elapsed)
    monkeypatch.setattr(kind.subprocess, "run", command)

    run.capture_failure_diagnostics()

    assert elapsed == 30
    assert len(calls) == 5
    assert calls[-1][4:6] == ("curie-publication", "logs")
    assert "--previous" in calls[-1]
    assert run.deadline == -1
    assert run.evidence == evidence_before
    assert "previous container diagnostic" in capsys.readouterr().err
