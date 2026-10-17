"""Unit pins for the Forge instance harness (#4706).

These tests load `forge_instance_cluster.py` directly and drive it against a
fake `kubectl`/`helm` executable placed first on PATH. Nothing here provisions
kind, Docker or Helm; the real-cluster regression stays in
`test_c4_forge_instances.py`, which this module never imports.

Two harness defects are pinned:

1. `Cluster.rollout()` used `kubectl wait --for=condition=Ready pod -l ...`,
   which latches onto the replaced API pod while it terminates (deletionTimestamp
   set, Ready=False) and times out although the new pod is Ready. Rollout must
   poll the API pods and accept exactly one Ready pod that is not terminating,
   the same predicate `api_pod()` uses, within a bound.
2. A failed command raised "kubectl --kubeconfig: exit 1; output withheld", so a
   CI log could not say what failed. The message must name the command by its
   positional tokens and carry the redacted tail of stderr, never stdout, and
   withhold stderr entirely for reads of Secrets or Helm values.
"""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


def load_cluster_helpers() -> Any:
    name = "c4_forge_instance_cluster_harness_unit"
    spec = importlib.util.spec_from_file_location(
        name, Path(__file__).with_name("forge_instance_cluster.py")
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("C4 cluster helper could not be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


helpers = load_cluster_helpers()
RELEASE: str = helpers.RELEASE
FULLNAME: str = helpers.FULLNAME
InfrastructureError: type[Exception] = helpers.InfrastructureError
API_SELECTOR = f"app.kubernetes.io/instance={RELEASE},app.kubernetes.io/component=api"

SCENARIO_ENV = "FAKE_FORGE_SCENARIO"
LOG_ENV = "FAKE_FORGE_LOG"

# One script serves as both `kubectl` and `helm`, configured through the
# SCENARIO_ENV and LOG_ENV variables. It appends its argv to the log, picks the
# first rule whose `match` tokens all appear in argv, and replays that rule's
# responses in order, repeating the last one once they run out.
FAKE_TOOL = """
import json
import os
import sys
import time
from pathlib import Path

argv = sys.argv[1:]
with open(os.environ["FAKE_FORGE_LOG"], "a", encoding="utf-8") as log:
    log.write(json.dumps([Path(sys.argv[0]).name, *argv]) + "\\n")
scenario_path = Path(os.environ["FAKE_FORGE_SCENARIO"])
rules = json.loads(scenario_path.read_text(encoding="utf-8"))["rules"]
state_path = scenario_path.with_name(scenario_path.name + ".state")
state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
for index, rule in enumerate(rules):
    if all(token in argv for token in rule["match"]):
        count = state.get(str(index), 0)
        state[str(index)] = count + 1
        state_path.write_text(json.dumps(state), encoding="utf-8")
        response = rule["responses"][min(count, len(rule["responses"]) - 1)]
        time.sleep(response.get("sleep", 0))
        sys.stdout.write(response.get("stdout", ""))
        sys.stderr.write(response.get("stderr", ""))
        sys.exit(response.get("exit", 0))
sys.stderr.write("fake: unscripted command\\n")
sys.exit(97)
"""

# Planted secrets. The hex part is 32 lowercase hex characters, the base64 run
# is 60 characters, and the PEM body would be readable key material.
PLANTED_HEX = hashlib.sha256(b"forge-harness-planted-token").hexdigest()[:32]
TOKEN = f"example-gitlab-a-current-{PLANTED_HEX}"
PEM_BODY = "MIIBOgIBAAJBAKj34GkxFhD90vcNLYLInFEX6Ppy1tPf9Cnzj4p4WGeKLs1Pt8Qu"
PEM = f"-----BEGIN RSA PRIVATE KEY-----\n{PEM_BODY}\n-----END RSA PRIVATE KEY-----\n"
BASE64_RUN = base64.b64encode(bytes(range(45))).decode()
STDOUT_MARKER = "STDOUT-MARKER-forge-harness-only-on-stdout"
HARMLESS = "error: deployment exceeded its progress deadline"


@dataclass
class Harness:
    candidate: Any
    cluster: Any
    scenario: Path
    log: Path

    def script(self, *rules: dict[str, Any]) -> None:
        self.scenario.write_text(json.dumps({"rules": list(rules)}), encoding="utf-8")

    def calls(self) -> list[list[str]]:
        if not self.log.exists():
            return []
        return [json.loads(line) for line in self.log.read_text(encoding="utf-8").splitlines()]


def rule(match: list[str], *responses: dict[str, Any]) -> dict[str, Any]:
    return {"match": match, "responses": list(responses)}


def pod(name: str, *, ready: bool, terminating: bool = False) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "name": name,
        "labels": {
            "app.kubernetes.io/instance": RELEASE,
            "app.kubernetes.io/component": "api",
        },
    }
    if terminating:
        metadata["deletionTimestamp"] = "2026-10-17T12:00:00Z"
    return {
        "metadata": metadata,
        "status": {
            "conditions": [
                {"type": "PodScheduled", "status": "True"},
                {"type": "Ready", "status": "True" if ready else "False"},
            ]
        },
    }


def pods(*items: dict[str, Any]) -> dict[str, Any]:
    return {"exit": 0, "stdout": json.dumps({"kind": "List", "items": list(items)})}


@pytest.fixture
def harness(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Harness:
    source = tmp_path / "source"
    source.mkdir()
    candidate = helpers.Candidate(source)
    candidate.root = tmp_path / "root"
    candidate.root.mkdir()
    candidate.binary = tmp_path / "curie"
    candidate.binary.write_text("#!/bin/sh\nexit 0\n")
    candidate.binary.chmod(0o755)

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    for tool in ("kubectl", "helm"):
        executable = bin_dir / tool
        executable.write_text(f"#!{sys.executable} -I\n{FAKE_TOOL}", encoding="utf-8")
        executable.chmod(0o755)
    scenario = tmp_path / "scenario.json"
    log = tmp_path / "calls.jsonl"
    fake_env = {
        "PATH": f"{bin_dir}:{candidate.env.get('PATH', '')}",
        SCENARIO_ENV: str(scenario),
        LOG_ENV: str(log),
    }
    candidate.env.update(fake_env)
    cluster = helpers.Cluster(candidate, helpers.GROUPS[0])
    cluster.env.update(fake_env)

    # Keep the readiness bound short; the harness reads both at call time.
    monkeypatch.setattr(helpers, "ROLLOUT_READY_SECONDS", 2, raising=False)
    monkeypatch.setattr(helpers, "ROLLOUT_POLL_SECONDS", 0.05, raising=False)
    result = Harness(candidate, cluster, scenario, log)
    result.script()
    return result


ROLLOUT_STATUS_OK = rule(["rollout", "status"], {"exit": 0, "stdout": "successfully rolled out\n"})
WAIT_AS_OBSERVED = rule(
    ["wait"],
    {
        "exit": 1,
        "stderr": "error: timed out waiting for the condition on pods/acme-factory-curie-api-old\n",
    },
)


def assert_no_wait(harness: Harness) -> None:
    waits = [call for call in harness.calls() if "wait" in call]
    assert not waits, f"rollout must not use kubectl wait for pods: {waits}"


def assert_polled_api_pods(harness: Harness) -> None:
    polls = [
        call
        for call in harness.calls()
        if "get" in call and "pods" in call and any(API_SELECTOR in token for token in call)
    ]
    assert polls, "rollout must query the API pods by the release and component selector"


def test_rollout_ignores_terminating_replaced_api_pod(harness: Harness) -> None:
    harness.script(
        ROLLOUT_STATUS_OK,
        WAIT_AS_OBSERVED,
        rule(
            ["get", "pods"],
            pods(
                pod("acme-factory-curie-api-old", ready=False, terminating=True),
                pod("acme-factory-curie-api-new", ready=True),
            ),
        ),
    )

    harness.cluster.rollout()

    assert_no_wait(harness)
    assert_polled_api_pods(harness)
    statuses = [call for call in harness.calls() if "rollout" in call and "status" in call]
    for component in ("api", "worker"):
        assert any(f"deployment/{FULLNAME}-{component}" in call for call in statuses)


def test_rollout_waits_until_new_api_pod_becomes_ready(harness: Harness) -> None:
    pending = pods(
        pod("acme-factory-curie-api-old", ready=False, terminating=True),
        pod("acme-factory-curie-api-new", ready=False),
    )
    harness.script(
        ROLLOUT_STATUS_OK,
        WAIT_AS_OBSERVED,
        rule(
            ["get", "pods"],
            pending,
            pending,
            pods(pod("acme-factory-curie-api-new", ready=True)),
        ),
    )

    harness.cluster.rollout()

    assert_no_wait(harness)
    polls = [call for call in harness.calls() if "get" in call and "pods" in call]
    assert len(polls) >= 3


def test_rollout_fails_when_no_api_pod_becomes_ready_within_bound(harness: Harness) -> None:
    harness.script(
        ROLLOUT_STATUS_OK,
        WAIT_AS_OBSERVED,
        rule(
            ["get", "pods"],
            pods(
                pod("acme-factory-curie-api-old", ready=False, terminating=True),
                pod("acme-factory-curie-api-new", ready=False),
            ),
        ),
    )

    started = time.monotonic()
    with pytest.raises(InfrastructureError) as raised:
        harness.cluster.rollout()

    assert time.monotonic() - started < 30
    assert_no_wait(harness)
    assert_polled_api_pods(harness)
    assert re.search(r"(?<!\d)0(?!\d)", str(raised.value)), str(raised.value)


def test_rollout_fails_with_two_ready_serving_api_pods(harness: Harness) -> None:
    harness.script(
        ROLLOUT_STATUS_OK,
        rule(["wait"], {"exit": 0}),
        rule(
            ["get", "pods"],
            pods(
                pod("acme-factory-curie-api-a", ready=True),
                pod("acme-factory-curie-api-b", ready=True),
            ),
        ),
    )

    with pytest.raises(InfrastructureError):
        harness.cluster.rollout()

    assert_no_wait(harness)


def failing(stderr: str, *, stdout: str = STDOUT_MARKER + "\n") -> dict[str, Any]:
    return rule([], {"exit": 1, "stdout": stdout, "stderr": stderr})


def test_failed_command_names_positionals_and_redacted_stderr_tail(
    harness: Harness, tmp_path: Path
) -> None:
    kubeconfig = tmp_path / "root" / "gitlab" / "kubeconfig"
    filler = "".join(f"warning: earlier event {n} of the rollout\n" for n in range(300))
    harness.script(
        failing(
            filler
            + f"auth: token {TOKEN} rejected\n"
            + PEM
            + f"payload {BASE64_RUN}\n"
            + HARMLESS
            + "\n"
        )
    )

    with pytest.raises(InfrastructureError) as raised:
        harness.candidate.run(
            [
                "kubectl",
                "--kubeconfig",
                str(kubeconfig),
                "--context",
                "kind-x",
                "--namespace",
                "ns",
                "rollout",
                "status",
                f"deployment/{FULLNAME}-api",
                "--timeout=240s",
            ]
        )

    message = str(raised.value)
    assert message.startswith(f"kubectl rollout status deployment/{FULLNAME}-api: exit 1"), message
    assert HARMLESS in message
    for leaked in (
        PLANTED_HEX,
        "PRIVATE KEY",
        PEM_BODY,
        BASE64_RUN,
        STDOUT_MARKER,
        str(kubeconfig),
    ):
        assert leaked not in message
    assert len(message) < 2300


@pytest.mark.parametrize(
    ("argv", "label"),
    [
        (
            ["kubectl", "--kubeconfig", "{kubeconfig}", "get", "secrets,configmaps", "-o", "json"],
            "kubectl get secrets,configmaps",
        ),
        (
            ["helm", "--kubeconfig", "{kubeconfig}", "get", "values", RELEASE],
            f"helm get values {RELEASE}",
        ),
        (
            [
                "kubectl",
                "--kubeconfig",
                "{kubeconfig}",
                "get",
                "Secret",
                "forge-inputs",
                "-o",
                "json",
            ],
            "kubectl get Secret forge-inputs",
        ),
    ],
)
def test_secret_and_values_reads_withhold_stderr(
    harness: Harness, argv: list[str], label: str
) -> None:
    # A fixed path: this test's tmp_path name itself contains "secret".
    kubeconfig = "/k/kubeconfig"
    harness.script(failing(f"error: data {TOKEN} could not be decoded\n{HARMLESS}\n"))

    with pytest.raises(InfrastructureError) as raised:
        harness.candidate.run([kubeconfig if a == "{kubeconfig}" else a for a in argv])

    message = str(raised.value)
    assert message.startswith(label), message
    assert "exit 1" in message
    assert "stderr withheld" in message
    for leaked in (PLANTED_HEX, HARMLESS, STDOUT_MARKER, kubeconfig):
        assert leaked not in message


def test_timeout_names_command_label(harness: Harness, tmp_path: Path) -> None:
    kubeconfig = str(tmp_path / "kubeconfig")
    harness.script(rule([], {"sleep": 10, "exit": 0}))

    started = time.monotonic()
    with pytest.raises(InfrastructureError) as raised:
        harness.candidate.run(
            ["kubectl", "--kubeconfig", kubeconfig, "rollout", "status", "deployment/x"],
            timeout=0.5,
        )

    assert time.monotonic() - started < 8
    message = str(raised.value)
    assert message.startswith("kubectl rollout status deployment/x"), message
    assert "TimeoutExpired" in message
    assert kubeconfig not in message


def test_unstartable_command_names_command_label(harness: Harness, tmp_path: Path) -> None:
    kubeconfig = str(tmp_path / "kubeconfig")

    with pytest.raises(InfrastructureError) as raised:
        harness.candidate.run(["kubectl-absent-4706", "--kubeconfig", kubeconfig, "get", "pods"])

    message = str(raised.value)
    assert message.startswith("kubectl-absent-4706 get pods"), message
    assert kubeconfig not in message


def test_helm_label_skips_option_values_and_shortens_paths(
    harness: Harness, tmp_path: Path
) -> None:
    kubeconfig = str(tmp_path / "kubeconfig")
    harness.script(failing(f"{HARMLESS}\n"))

    with pytest.raises(InfrastructureError) as raised:
        harness.candidate.run(
            [
                "helm",
                "--kubeconfig",
                kubeconfig,
                "--kube-context",
                "kind-x",
                "--namespace",
                "ns",
                "upgrade",
                RELEASE,
                "/abs/path/charts/curie",
                "-f",
                "/abs/values.yaml",
            ]
        )

    message = str(raised.value)
    assert message.startswith(f"helm upgrade {RELEASE} curie: exit 1"), message
    assert HARMLESS in message
    for leaked in ("/abs/path", "/abs/values.yaml", kubeconfig, STDOUT_MARKER):
        assert leaked not in message


@pytest.mark.parametrize(
    ("argv", "label"),
    [
        pytest.param(
            [
                "docker",
                "run",
                "--rm",
                "--name",
                "c4-key",
                "--label",
                "curie-c4-owner=abc",
                "image:tag",
                "python3",
                "-c",
                "print(1)",
            ],
            "docker run",
            id="docker-run-stops-at-first-option",
        ),
        pytest.param(
            [
                "kubectl",
                "--kubeconfig",
                "/k",
                "--context",
                "c",
                "--namespace",
                "n",
                "exec",
                "-i",
                "pod-1",
                "-c",
                "api",
                "--",
                "python3",
                "-",
            ],
            "kubectl exec",
            id="kubectl-exec-stops-at-first-option",
        ),
        pytest.param(
            [
                "/abs/bin/curie",
                "factory",
                "quickstart",
                "--code-host",
                "gitlab",
                "--token-file",
                "/t",
            ],
            "curie factory quickstart",
            id="curie-quickstart-drops-option-values",
        ),
        pytest.param(
            [
                "kubectl",
                "--kubeconfig",
                "/k",
                "rollout",
                "status",
                f"deployment/{FULLNAME}-api",
                "--timeout=240s",
            ],
            f"kubectl rollout status deployment/{FULLNAME}-api",
            id="kubectl-rollout-status",
        ),
        pytest.param(
            [
                "helm",
                "--kubeconfig",
                "/k",
                "--kube-context",
                "kind-x",
                "--namespace",
                "ns",
                "upgrade",
                RELEASE,
                "/abs/path/charts/curie",
                "-f",
                "/abs/values.yaml",
            ],
            f"helm upgrade {RELEASE} curie",
            id="helm-upgrade-shortens-chart-path",
        ),
    ],
)
def test_command_label(argv: list[str], label: str) -> None:
    assert helpers.command_label(argv) == label


def test_registered_secret_values_and_encodings_are_redacted(harness: Harness) -> None:
    # Short and hex-free so no pattern catches it; its encodings differ between
    # the standard and urlsafe alphabets and carry "=" padding.
    secret = "plain-short-secret-\u00fe\u00ff>?"
    standard = base64.b64encode(secret.encode()).decode()
    urlsafe = base64.urlsafe_b64encode(secret.encode()).decode()
    assert standard != urlsafe
    assert standard.endswith("=") and urlsafe.endswith("=")
    unpadded = (standard.rstrip("="), urlsafe.rstrip("="))
    harness.candidate.register_secret(secret)
    harness.script(
        failing(
            f"auth {secret} rejected\nheader {standard}\nquery {urlsafe}\n"
            f"cookie {unpadded[0]};\nstate {unpadded[1]};\n{HARMLESS}\n"
        )
    )

    with pytest.raises(InfrastructureError) as raised:
        harness.candidate.run(["kubectl", "rollout", "status", "deployment/x"])

    message = str(raised.value)
    assert HARMLESS in message
    # The unpadded forms are prefixes of the padded ones, so this covers both.
    for leaked in (secret, *unpadded):
        assert leaked not in message


def test_cluster_registers_its_api_and_control_keys(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Keys no pattern would catch, so only registration can redact them.
    monkeypatch.setattr(
        helpers, "uuid", SimpleNamespace(uuid4=lambda: SimpleNamespace(hex="plainkey"))
    )
    cluster = helpers.Cluster(harness.candidate, helpers.GROUPS[1])
    keys = (cluster.api_key, cluster.control_key)
    harness.script(failing("".join(f"bearer {key} rejected\n" for key in keys) + f"{HARMLESS}\n"))

    with pytest.raises(InfrastructureError) as raised:
        harness.candidate.run(["kubectl", "rollout", "status", "deployment/x"])

    message = str(raised.value)
    assert HARMLESS in message
    for key in keys:
        assert key not in message


def test_uppercase_hex_and_base64url_runs_are_redacted(harness: Harness) -> None:
    upper_hex = "ABCDEF0123456789ABCDEF0123456789"
    base64url = "Ab-_" * 11
    harness.script(failing(f"key {upper_hex}\njwt {base64url}\n{HARMLESS}\n"))

    with pytest.raises(InfrastructureError) as raised:
        harness.candidate.run(["kubectl", "rollout", "status", "deployment/x"])

    message = str(raised.value)
    assert HARMLESS in message
    assert upper_hex not in message
    assert base64url not in message


def test_rollout_does_not_count_a_pods_query_that_outlives_the_bound(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(helpers, "ROLLOUT_READY_SECONDS", 1, raising=False)
    harness.script(
        ROLLOUT_STATUS_OK,
        rule(["get", "pods"], {"sleep": 3, **pods(pod("acme-factory-curie-api-new", ready=True))}),
    )

    started = time.monotonic()
    with pytest.raises(InfrastructureError):
        harness.cluster.rollout()

    assert time.monotonic() - started < 2.5
    assert_no_wait(harness)


def test_timeout_failure_does_not_chain_the_raw_error(harness: Harness) -> None:
    harness.script(rule([], {"sleep": 10, "exit": 0}))

    with pytest.raises(InfrastructureError) as raised:
        harness.candidate.run(["kubectl", "rollout", "status", "deployment/x"], timeout=0.5)

    error = raised.value
    assert error.__context__ is None or error.__suppress_context__, repr(error.__context__)


def test_unstartable_failure_does_not_chain_the_raw_error(harness: Harness) -> None:
    with pytest.raises(InfrastructureError) as raised:
        harness.candidate.run(["kubectl-absent-4706", "get", "pods"])

    error = raised.value
    assert error.__context__ is None or error.__suppress_context__, repr(error.__context__)
