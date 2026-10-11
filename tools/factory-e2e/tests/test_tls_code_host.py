"""Unit tests for the tls-code-host scenario's pure pieces (#4398 decision 2).

The live scenario runs the TLS GitHub stub in the test namespace, trusts its
public CA through ``codeHostTrust.caBundle.configMapRef`` and never touches
real GitHub. These tests pin the shapes ``tools/factory-e2e/tls_code_host.py``
must provide. Shapes chosen here, which the implementation follows:

``stub_manifests(namespace, image, port=8443) -> list[dict]``
    Kubernetes objects for ``kubectl apply``: exactly one Pod and one Service
    (a ConfigMap entry is optional; when present it is labelled like the rest
    and carries only the stub source and its recording). Every object sets
    ``metadata.namespace`` and carries ``fe.OWNER_LABEL`` as a label. The Pod
    runs one container on ``image`` whose ``command + args`` is the stub's
    ``serve`` argv, mounts the stub source from a configMap volume at
    ``/stub`` and its private state (CA key, server key, SQLite, bare repo)
    from an emptyDir volume at ``/state``. The Service is named
    ``github-stub``, ClusterIP, selecting the Pod's labels on ``port``.

``tls_values(base, *, namespace, ca_config_map, stub_cluster_ip, port, model_cidrs) -> dict``
    A new dict; ``base`` (``fe.install_values`` output) is not mutated.
    ``security.networkPolicy.allowedEgress`` becomes exactly the stub
    ``<ip>/32`` on ``port`` plus each model CIDR on 443.

``parse_stub_startup(log_text) -> dict``
    ``{"base_url", "clone_url", "ca_file"}`` from the stub's one JSON startup
    line. Raises ``fe.PreflightFailed`` when it is absent or malformed.

``load_tls_config(env, *, context) -> fe.FactoryConfig``
    Needs only ``CURIE_FACTORY_MODEL_API_KEY`` and a kube context
    (``--context`` or ``CURIE_FACTORY_KUBE_CONTEXT``); the App identity is the
    stub fixture's (App 51, installation 5501, ``acme-corp/acme-bot``). Raises
    ``fe.ConfigError`` naming each missing input. It never reads the GitHub
    App, webhook, actor or fixture repository variables.

``judge_tls_code_host(obs) -> list[str]``
    Empty means passed. ``obs`` keys:
    ``work_item_id`` (str or None), ``request_statuses`` (ordered list, the
    last one is the ending), ``claim_env`` (the sandbox claim's env as a
    dict), ``publication_job`` (the publication Job as ``kubectl get job -o
    json`` returns it, or None), ``publication_record`` (the publications row
    behind that Job: ``id``, ``status``, ``execution_request_id``; completion
    is its ``status == "succeeded"``, since the worker deletes the Job as soon
    as it settles), ``stub_branches`` (branch names in the stub's
    bare repository), ``stub_pull_requests`` (the stub's pull request rows),
    ``stub_unsupported`` (the stub's recorded unsupported requests) and
    ``logs`` (the observed api, worker, publication and stub log text). Each
    failure names its cause with the phrase the matching test below asserts,
    case-insensitively.
"""

from __future__ import annotations

import copy
import json
import os
import subprocess
import types
from pathlib import Path
from typing import Any

import factory_e2e as fe
import pytest
import tls_code_host

NAMESPACE = "test-factory-tls0001"
IMAGE = "registry.example/curie-worker:sha-" + "c" * 40
STUB_HOST = f"github-stub.{NAMESPACE}.svc.cluster.local"
CA_PATH = "/etc/curie/code-host-trust/ca.crt"
STUB_IP = "10.43.12.34"
MODEL_CIDRS = ["203.0.113.10/32", "203.0.113.11/32"]
OWNER_KEY, OWNER_VALUE = fe.OWNER_LABEL.split("=", 1)


# --------------------------------------------------------------------------
# stub_manifests
# --------------------------------------------------------------------------


def _by_kind(manifests: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    return [m for m in manifests if m.get("kind") == kind]


def _pod(manifests: list[dict[str, Any]]) -> dict[str, Any]:
    pods = _by_kind(manifests, "Pod")
    assert len(pods) == 1, f"expected exactly one Pod, got {len(pods)}"
    return pods[0]


def _service(manifests: list[dict[str, Any]]) -> dict[str, Any]:
    services = _by_kind(manifests, "Service")
    assert len(services) == 1, f"expected exactly one Service, got {len(services)}"
    return services[0]


def _argv(pod: dict[str, Any]) -> list[str]:
    containers = pod["spec"]["containers"]
    assert len(containers) == 1
    container = containers[0]
    return [str(part) for part in (container.get("command") or []) + (container.get("args") or [])]


def _flag(argv: list[str], name: str) -> str:
    assert name in argv, f"{name} missing from {argv}"
    return argv[argv.index(name) + 1]


def test_stub_manifests_are_one_pod_and_one_service_in_the_namespace_and_owned() -> None:
    manifests = tls_code_host.stub_manifests(NAMESPACE, IMAGE)
    _pod(manifests)
    _service(manifests)
    assert {m["kind"] for m in manifests} <= {"Pod", "Service", "ConfigMap"}
    for manifest in manifests:
        meta = manifest["metadata"]
        assert meta["namespace"] == NAMESPACE, manifest["kind"]
        assert meta.get("labels", {}).get(OWNER_KEY) == OWNER_VALUE, manifest["kind"]


def test_stub_pod_runs_once_on_the_candidate_image() -> None:
    pod = _pod(tls_code_host.stub_manifests(NAMESPACE, IMAGE))
    assert pod["spec"]["restartPolicy"] == "Never"
    assert pod["spec"]["containers"][0]["image"] == IMAGE
    assert pod["metadata"]["labels"].get(OWNER_KEY) == OWNER_VALUE


def test_stub_serve_argv_binds_all_interfaces_and_advertises_the_service_name() -> None:
    argv = _argv(_pod(tls_code_host.stub_manifests(NAMESPACE, IMAGE)))
    assert "python3" in argv[0]
    assert "/stub/github_stub.py" in argv
    assert argv[argv.index("/stub/github_stub.py") + 1] == "serve"
    assert _flag(argv, "--root") == "/state"
    assert _flag(argv, "--bind") == "0.0.0.0"
    assert _flag(argv, "--host") == STUB_HOST
    assert _flag(argv, "--port") == "8443"


def test_stub_port_flows_to_argv_and_service() -> None:
    manifests = tls_code_host.stub_manifests(NAMESPACE, IMAGE, port=9443)
    assert _flag(_argv(_pod(manifests)), "--port") == "9443"
    ports = _service(manifests)["spec"]["ports"]
    assert [(p["port"], p.get("targetPort", p["port"])) for p in ports] == [(9443, 9443)]


def test_stub_service_is_a_cluster_ip_named_github_stub_selecting_the_pod() -> None:
    manifests = tls_code_host.stub_manifests(NAMESPACE, IMAGE)
    service = _service(manifests)
    pod = _pod(manifests)
    assert service["metadata"]["name"] == "github-stub"
    assert service["spec"].get("type", "ClusterIP") == "ClusterIP"
    selector = service["spec"]["selector"]
    assert selector, "the Service must select the stub Pod"
    for key, value in selector.items():
        assert pod["metadata"]["labels"].get(key) == value
    assert [(p["port"], p.get("targetPort", p["port"])) for p in service["spec"]["ports"]] == [
        (8443, 8443)
    ]


def test_stub_source_comes_from_a_config_map_and_state_stays_in_an_empty_dir() -> None:
    pod = _pod(tls_code_host.stub_manifests(NAMESPACE, IMAGE))
    volumes = {v["name"]: v for v in pod["spec"]["volumes"]}
    mounts = {m["mountPath"]: m["name"] for m in pod["spec"]["containers"][0]["volumeMounts"]}
    assert "configMap" in volumes[mounts["/stub"]]
    state = volumes[mounts["/state"]]
    # The private CA key and server key live only in the pod's own scratch.
    assert set(state) == {"name", "emptyDir"}, state
    for volume in volumes.values():
        assert set(volume) - {"name"} <= {"configMap", "emptyDir"}, volume


def test_stub_manifests_carry_no_secret_or_credential() -> None:
    manifests = tls_code_host.stub_manifests(NAMESPACE, IMAGE)
    assert not _by_kind(manifests, "Secret")
    pod = _pod(manifests)
    assert pod["spec"].get("automountServiceAccountToken") is False
    text = json.dumps(manifests)
    for forbidden in (
        "secretKeyRef",
        "secretName",
        '"secret"',
        "hostPath",
        "persistentVolumeClaim",
        "BEGIN",
        "PRIVATE KEY",
    ):
        assert forbidden not in text, forbidden
    for container in pod["spec"]["containers"]:
        assert not container.get("envFrom")
        for entry in container.get("env") or []:
            assert "valueFrom" not in entry, entry


def test_only_the_stub_container_mounts_the_state_volume() -> None:
    pod = _pod(tls_code_host.stub_manifests(NAMESPACE, IMAGE))
    volumes = {v["name"]: v for v in pod["spec"]["volumes"]}
    state_names = {name for name, v in volumes.items() if "emptyDir" in v}
    for container in pod["spec"].get("initContainers") or []:
        assert not {m["name"] for m in container.get("volumeMounts") or []} & state_names


# --------------------------------------------------------------------------
# tls_values
# --------------------------------------------------------------------------


def _config() -> fe.FactoryConfig:
    return fe.FactoryConfig(
        kube_context="k8",
        app_id="51",
        installation_id=5501,
        private_key_file=Path("app.pem"),
        repo="acme-corp/acme-bot",
        label="curie-factory",
        mention="curie-factory-bot",
        cloudflared="cloudflared",
        priority_classes=None,
        restore_webhook_url=None,
        webhook_secret="tls-test-webhook-secret",
        actor_token="unused",
        model_api_key="test-model-key",
    )


def _base() -> dict[str, Any]:
    return fe.install_values(
        _config(),
        candidate="c" * 40,
        app_key_secret=fe.APP_KEY_REF,
        consumer_controller=True,
    )


def _tls(base: dict[str, Any] | None = None, **overrides: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "namespace": NAMESPACE,
        "ca_config_map": "code-host-ca",
        "stub_cluster_ip": STUB_IP,
        "port": 8443,
        "model_cidrs": MODEL_CIDRS,
    }
    kwargs.update(overrides)
    return tls_code_host.tls_values(_base() if base is None else base, **kwargs)


def test_tls_values_trust_the_ca_through_code_host_trust() -> None:
    values = _tls()
    assert values["codeHostTrust"] == {
        "caBundle": {"configMapRef": {"name": "code-host-ca", "key": "ca.crt"}}
    }


def test_tls_values_leave_the_legacy_stub_ca_path_empty_even_when_base_set_it() -> None:
    assert _tls()["api"].get("githubStubCaConfigMap", "") == ""
    base = _base()
    base["api"]["githubStubCaConfigMap"] = "github-stub-ca"
    assert _tls(base)["api"].get("githubStubCaConfigMap", "") == ""


def test_tls_values_point_github_at_the_stub_service_over_https() -> None:
    api = _tls()["api"]
    assert api["githubApiUrl"] == f"https://{STUB_HOST}:8443/api/v3"
    assert api["githubCloneBase"] == f"https://{STUB_HOST}:8443"
    assert api["githubRepoAllowlist"] == ["acme-corp/acme-bot"]
    assert api["githubAppId"] == "51"
    assert api["githubFactoryIntake"] == "poll"
    assert api["githubFactoryIngressEnabled"] is True


def test_tls_values_port_flows_to_urls_and_egress() -> None:
    values = _tls(port=9443)
    assert values["api"]["githubApiUrl"] == f"https://{STUB_HOST}:9443/api/v3"
    assert values["api"]["githubCloneBase"] == f"https://{STUB_HOST}:9443"
    egress = values["security"]["networkPolicy"]["allowedEgress"]
    assert {"cidr": f"{STUB_IP}/32", "ports": [{"protocol": "TCP", "port": 9443}]} in egress


def test_tls_values_egress_is_exactly_the_stub_and_the_model_provider() -> None:
    egress = _tls()["security"]["networkPolicy"]["allowedEgress"]
    assert {"cidr": f"{STUB_IP}/32", "ports": [{"protocol": "TCP", "port": 8443}]} in egress
    for cidr in MODEL_CIDRS:
        assert {"cidr": cidr, "ports": [{"protocol": "TCP", "port": 443}]} in egress
    # No GitHub /meta ranges: nothing beyond the stub and the model provider.
    assert {entry["cidr"] for entry in egress} == {f"{STUB_IP}/32", *MODEL_CIDRS}


def test_tls_values_drop_github_meta_ranges_a_base_carried() -> None:
    base = fe.install_values(
        _config(),
        candidate="c" * 40,
        app_key_secret=fe.APP_KEY_REF,
        consumer_controller=True,
        egress_cidrs=["140.82.112.0/20", "192.30.252.0/22"],
    )
    egress = _tls(base)["security"]["networkPolicy"]["allowedEgress"]
    assert {entry["cidr"] for entry in egress} == {f"{STUB_IP}/32", *MODEL_CIDRS}


def test_tls_values_keep_the_real_model_and_do_not_mutate_base() -> None:
    base = _base()
    before = copy.deepcopy(base)
    values = _tls(base)
    assert base == before
    assert values is not base
    runner = values["agentSandbox"]["runner"]
    assert runner["fakeModel"] is False
    assert runner["credentials"] == "test-model-key"


# --------------------------------------------------------------------------
# parse_stub_startup
# --------------------------------------------------------------------------

STARTUP = {
    "base_url": f"https://{STUB_HOST}:8443",
    "clone_url": f"https://{STUB_HOST}:8443/acme-corp/acme-bot.git",
    "ca_file": "/state/ca.pem",
}


def test_parse_stub_startup_finds_the_json_line_amid_other_output() -> None:
    log_text = "\n".join(
        [
            "Generating a RSA private key",
            "writing new private key to '/state/ca.key'",
            '{"level": "info", "msg": "unrelated structured line"}',
            json.dumps(STARTUP),
            "127.0.0.1 - - GET /api/v3/repos/acme-corp/acme-bot 200",
        ]
    )
    assert tls_code_host.parse_stub_startup(log_text) == STARTUP


@pytest.mark.parametrize(
    "log_text",
    [
        "",
        "Generating a RSA private key\nserving\n",
        '{"base_url": "https://github-stub.x.svc.cluster.local:8443", "clone_url": ',
        json.dumps({k: v for k, v in STARTUP.items() if k != "ca_file"}),
        json.dumps({**STARTUP, "base_url": f"http://{STUB_HOST}:8443"}),
        "[1, 2, 3]",
    ],
    ids=["empty", "no-json", "truncated", "missing-ca-file", "plain-http", "not-an-object"],
)
def test_parse_stub_startup_refuses_absent_or_malformed_output(log_text: str) -> None:
    with pytest.raises(fe.PreflightFailed):
        tls_code_host.parse_stub_startup(log_text)


# --------------------------------------------------------------------------
# load_tls_config: only the model key and a context
# --------------------------------------------------------------------------


def test_load_tls_config_needs_only_the_model_key_and_a_context(tmp_path: Path) -> None:
    env = {
        "CURIE_FACTORY_MODEL_API_KEY": "test-model-key",
        # Pointing at nothing must not matter: the tls path never reads it.
        "CURIE_FACTORY_APP_DIR": str(tmp_path / "absent"),
    }
    config = tls_code_host.load_tls_config(env, context="k8")
    assert isinstance(config, fe.FactoryConfig)
    assert config.kube_context == "k8"
    assert config.model_api_key == "test-model-key"
    assert config.app_id == "51"
    assert config.installation_id == 5501
    assert config.repo == "acme-corp/acme-bot"
    assert "test-model-key" not in repr(config)


def test_load_tls_config_reads_the_context_from_the_environment() -> None:
    env = {"CURIE_FACTORY_MODEL_API_KEY": "k", "CURIE_FACTORY_KUBE_CONTEXT": "k8"}
    assert tls_code_host.load_tls_config(env, context=None).kube_context == "k8"


def test_load_tls_config_names_the_model_key_and_no_github_app_input() -> None:
    with pytest.raises(fe.ConfigError) as caught:
        tls_code_host.load_tls_config({}, context="k8")
    message = str(caught.value)
    assert "CURIE_FACTORY_MODEL_API_KEY" in message
    for app_only in (
        "CURIE_FACTORY_APP_ID",
        "CURIE_FACTORY_INSTALLATION_ID",
        "CURIE_FACTORY_APP_PRIVATE_KEY_FILE",
        "CURIE_FACTORY_WEBHOOK_SECRET_FILE",
        "CURIE_FACTORY_REPO",
        "CURIE_FACTORY_ACTOR_TOKEN",
        "CURIE_FACTORY_MENTION",
    ):
        assert app_only not in message, app_only


def test_load_tls_config_names_a_missing_context() -> None:
    with pytest.raises(fe.ConfigError) as caught:
        tls_code_host.load_tls_config({"CURIE_FACTORY_MODEL_API_KEY": "k"}, context=None)
    assert "CURIE_FACTORY_KUBE_CONTEXT" in str(caught.value)


# --------------------------------------------------------------------------
# judge_tls_code_host
# --------------------------------------------------------------------------


def _job(
    *, volume: dict[str, Any] | None = None, succeeded: int = 1, failed: int = 0
) -> dict[str, Any]:
    trust = (
        volume
        if volume is not None
        else {
            "name": "code-host-trust",
            "configMap": {"name": "code-host-ca", "items": [{"key": "ca.crt", "path": "ca.crt"}]},
        }
    )
    volumes: list[dict[str, Any]] = [
        {"name": "publication", "configMap": {"name": "curie-publication-1"}},
        {"name": "work", "emptyDir": {"sizeLimit": "2Gi"}},
    ]
    if trust:
        volumes.append(trust)
    status: dict[str, Any] = {}
    if succeeded:
        status["succeeded"] = succeeded
        status["conditions"] = [{"type": "Complete", "status": "True"}]
    if failed:
        status["failed"] = failed
        status["conditions"] = [{"type": "Failed", "status": "True"}]
    return {
        "kind": "Job",
        "metadata": {"name": "curie-publication-1", "namespace": NAMESPACE},
        "spec": {"template": {"spec": {"volumes": volumes}}},
        "status": status,
    }


PUBLICATION_ID = "0123456789abcdef0123456789abcdef"
PUBLICATION_JOB = f"curie-publication-{PUBLICATION_ID[:20]}"


def _passing() -> dict[str, Any]:
    return {
        "work_item_id": "6f1c3a52-0000-4000-8000-000000000001",
        "request_statuses": ["queued", "running", "completed"],
        "claim_env": {
            "CURIE_REPO_ORIGIN": f"https://{STUB_HOST}:8443",
            "CURIE_REPO_PATH": "acme-corp/acme-bot",
            "CURIE_REPO_CA_BUNDLE": CA_PATH,
        },
        "publication_job": _job(),
        "publication_record": {
            "id": PUBLICATION_ID,
            "status": "succeeded",
            "execution_request_id": "6f1c3a52-0000-4000-8000-000000000002",
        },
        "stub_branches": ["main", "curie/factory-3815"],
        "stub_pull_requests": [{"number": 1, "head": "curie/factory-3815", "base": "main"}],
        "stub_unsupported": [],
        # Benign certificate wording must not read as a verification failure.
        "logs": (
            "worker: loaded code host CA certificate bundle from "
            f"{CA_PATH}\nworker: snapshot cloned acme-corp/acme-bot\n"
            "publication: pushed curie/factory-3815\n"
        ),
    }


def _failures_with(obs: dict[str, Any], phrase: str) -> list[str]:
    failures = tls_code_host.judge_tls_code_host(obs)
    assert all(isinstance(f, str) for f in failures)
    matching = [f for f in failures if phrase.lower() in f.lower()]
    assert matching, f"no failure mentions {phrase!r}: {failures}"
    return matching


def test_judge_passes_a_full_tls_run() -> None:
    assert tls_code_host.judge_tls_code_host(_passing()) == []


def test_judge_does_not_mutate_the_observation() -> None:
    obs = _passing()
    obs["request_statuses"] = ["queued", "failed"]
    before = copy.deepcopy(obs)
    tls_code_host.judge_tls_code_host(obs)
    assert obs == before


def test_judge_fails_without_an_admitted_work_item() -> None:
    obs = _passing()
    obs["work_item_id"] = None
    _failures_with(obs, "no WorkItem was admitted")


def test_judge_fails_when_the_request_never_reached_running() -> None:
    obs = _passing()
    obs["request_statuses"] = ["queued", "failed"]
    _failures_with(obs, "never reached running")


def test_judge_fails_when_the_claim_env_lacks_the_ca_bundle() -> None:
    obs = _passing()
    del obs["claim_env"]["CURIE_REPO_CA_BUNDLE"]
    _failures_with(obs, "CURIE_REPO_CA_BUNDLE")


def test_judge_fails_when_the_claim_ca_bundle_is_not_the_code_host_trust_path() -> None:
    obs = _passing()
    obs["claim_env"]["CURIE_REPO_CA_BUNDLE"] = "/etc/curie/github-stub-ca/ca.pem"
    matching = _failures_with(obs, "CURIE_REPO_CA_BUNDLE")
    assert any(CA_PATH in f for f in matching), matching


def test_judge_fails_when_no_publication_job_was_observed() -> None:
    obs = _passing()
    obs["publication_job"] = None
    _failures_with(obs, "publication Job")


def test_judge_fails_when_the_publication_job_lacks_the_trust_volume() -> None:
    obs = _passing()
    obs["publication_job"] = _job(volume={})
    _failures_with(obs, "code-host-trust")


def test_judge_fails_when_the_trust_volume_is_not_the_code_host_ca_config_map() -> None:
    obs = _passing()
    obs["publication_job"] = _job(
        volume={"name": "code-host-trust", "configMap": {"name": "github-stub-ca"}}
    )
    _failures_with(obs, "code-host-ca")


@pytest.mark.parametrize("status", ["failed", "running", "expired"])
def test_judge_fails_when_the_publication_record_did_not_succeed(status: str) -> None:
    obs = _passing()
    obs["publication_record"] = {**obs["publication_record"], "status": status}
    matching = _failures_with(obs, "publication Job did not complete")
    assert any(repr(status) in f for f in matching), matching


def test_judge_fails_without_a_publication_record() -> None:
    obs = _passing()
    obs["publication_record"] = None
    _failures_with(obs, "no publication record")


def test_judge_reads_completion_from_the_record_not_the_last_job_status() -> None:
    # The last poll saw the Job still running; the record says it succeeded.
    obs = _passing()
    obs["publication_job"] = _job(succeeded=0)
    assert tls_code_host.judge_tls_code_host(obs) == []


def test_judge_fails_when_the_stub_repo_has_no_pushed_branch() -> None:
    obs = _passing()
    obs["stub_branches"] = ["main"]
    _failures_with(obs, "no pushed branch")


@pytest.mark.parametrize("count", [0, 2])
def test_judge_fails_unless_the_stub_shows_exactly_one_pull_request(count: int) -> None:
    obs = _passing()
    obs["stub_pull_requests"] = [
        {"number": n + 1, "head": f"curie/factory-{n}", "base": "main"} for n in range(count)
    ]
    matching = _failures_with(obs, "pull request")
    assert any(str(count) in f for f in matching), matching


def test_judge_fails_when_the_stub_recorded_an_unsupported_request() -> None:
    obs = _passing()
    obs["stub_unsupported"] = ["DELETE /api/v3/repos/acme-corp/acme-bot/git/refs/heads/x"]
    matching = _failures_with(obs, "unsupported")
    assert any("DELETE /api/v3/repos/acme-corp/acme-bot" in f for f in matching), matching


def test_judge_fails_when_the_request_did_not_end_completed() -> None:
    obs = _passing()
    obs["request_statuses"] = ["queued", "running", "failed"]
    matching = _failures_with(obs, "did not end completed")
    assert any("failed" in f for f in matching), matching
    assert not [f for f in matching if "never reached running" in f.lower()]


@pytest.mark.parametrize(
    "line",
    [
        "fatal: unable to access 'https://github-stub.x:8443/acme-corp/acme-bot.git/': "
        "SSL certificate problem: unable to get local issuer certificate",
        "ssl.SSLCertVerificationError: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify "
        "failed: self-signed certificate in certificate chain",
        "server certificate verification failed. CAfile: none CRLfile: none",
    ],
    ids=["git-issuer", "python-verify", "git-cafile-none"],
)
def test_judge_fails_on_a_tls_verification_failure_in_the_logs(line: str) -> None:
    obs = _passing()
    obs["logs"] = obs["logs"] + line + "\n"
    _failures_with(obs, "TLS verification failure")


# --------------------------------------------------------------------------
# Scenario registration and the CLI refusal
# --------------------------------------------------------------------------


def test_tls_code_host_is_a_registered_scenario() -> None:
    assert "tls-code-host" in fe.SCENARIO_NAMES
    assert callable(fe.SCENARIOS["tls-code-host"])
    assert fe.resolve_scenario("tls-code-host") is fe.SCENARIOS["tls-code-host"]


def test_tls_code_host_is_a_run_scenario_choice() -> None:
    assert fe.parse_args(["run", "--scenario", "tls-code-host"]).scenario == "tls-code-host"


def _clear_factory_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for key in list(os.environ):
        if key.startswith("CURIE_FACTORY_"):
            monkeypatch.delenv(key)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(fe.signal, "signal", lambda *a, **k: None)


class _Reached(Exception):
    pass


def test_main_refuses_tls_code_host_without_a_model_key_before_installing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _clear_factory_env(monkeypatch, tmp_path)

    def refuse(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("must refuse before running any command")

    monkeypatch.setattr(fe, "run", refuse)
    monkeypatch.setattr(fe, "_resolve_candidate", refuse)
    monkeypatch.setattr(fe.Preflight, "run", refuse)
    monkeypatch.setattr(subprocess, "run", refuse)
    monkeypatch.setattr(subprocess, "Popen", refuse)

    code = fe.main(["run", "--scenario", "tls-code-host", "--context", "k8"])
    err = capsys.readouterr().err
    assert code == fe.EXIT_CONFIG
    assert "tls-code-host" in err
    assert "CURIE_FACTORY_MODEL_API_KEY" in err
    assert "missing required factory credential" not in err
    for app_only in (
        "CURIE_FACTORY_APP_ID",
        "CURIE_FACTORY_APP_PRIVATE_KEY_FILE",
        "CURIE_FACTORY_WEBHOOK_SECRET_FILE",
        "CURIE_FACTORY_REPO",
        "CURIE_FACTORY_ACTOR_TOKEN",
    ):
        assert app_only not in err, app_only


def test_main_runs_tls_code_host_without_any_github_app_or_fixture_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """With only the model key set, config passes and the run reaches its first
    external command; a sentinel stops it there, before anything is installed."""

    _clear_factory_env(monkeypatch, tmp_path)
    monkeypatch.setenv("CURIE_FACTORY_MODEL_API_KEY", "test-model-key")
    calls: list[Any] = []

    def reached(*args: Any, **_k: Any) -> Any:
        calls.append(args)
        raise _Reached("stopped at the first external command")

    monkeypatch.setattr(fe, "run", reached)
    monkeypatch.setattr(subprocess, "run", reached)
    monkeypatch.setattr(subprocess, "Popen", reached)
    monkeypatch.setattr(subprocess, "check_output", reached)
    monkeypatch.setattr(fe.Teardown, "run", lambda self: [])
    monkeypatch.setattr(fe.Preflight, "write_evidence", lambda self: None, raising=False)

    code: int | None
    try:
        code = fe.main(["run", "--scenario", "tls-code-host", "--context", "k8"])
    except _Reached:
        code = None
    err = capsys.readouterr().err
    assert calls, f"tls-code-host refused before any command (exit {code}): {err}"
    assert "missing required factory credential" not in err
    assert "CURIE_FACTORY_APP" not in err
    assert "CURIE_FACTORY_ACTOR" not in err


# --------------------------------------------------------------------------
# publication_job_from_watch
# --------------------------------------------------------------------------


def _watched_job(name: str, *, succeeded: int = 0) -> dict[str, Any]:
    job = _job(succeeded=succeeded)
    job["metadata"] = {
        "name": name,
        "namespace": f"{NAMESPACE}-curie-publication",
        "creationTimestamp": "2026-10-11T00:00:00Z",
    }
    job["apiVersion"] = "batch/v1"
    return job


def _event(kind: str, job: dict[str, Any], *, indent: int | None = 4) -> str:
    # kubectl writes each watch event as an indented JSON object.
    return json.dumps({"type": kind, "object": job}, indent=indent) + "\n"


def _trust_volume(job: dict[str, Any] | None) -> dict[str, Any] | None:
    assert job is not None
    volumes = job["spec"]["template"]["spec"]["volumes"]
    return next((v for v in volumes if v.get("name") == "code-host-trust"), None)


def test_watch_keeps_a_job_seen_only_as_added_then_deleted() -> None:
    stream = _event("ADDED", _watched_job(PUBLICATION_JOB)) + _event(
        "DELETED", _watched_job(PUBLICATION_JOB, succeeded=1)
    )
    job = tls_code_host.publication_job_from_watch(stream)
    assert job is not None
    assert job["metadata"]["name"] == PUBLICATION_JOB
    assert _trust_volume(job) == {
        "name": "code-host-trust",
        "configMap": {"name": "code-host-ca", "items": [{"key": "ca.crt", "path": "ca.crt"}]},
    }
    # The final status reported is the DELETED event's.
    assert job["status"].get("succeeded") == 1
    obs = {**_passing(), "publication_job": job}
    assert tls_code_host.judge_tls_code_host(obs) == []


def test_watch_takes_volumes_from_any_event_for_the_job() -> None:
    bare = _watched_job(PUBLICATION_JOB)
    bare["spec"] = {}
    stream = _event("ADDED", _watched_job(PUBLICATION_JOB)) + _event("DELETED", bare)
    assert _trust_volume(tls_code_host.publication_job_from_watch(stream)) is not None


def test_watch_ignores_unrelated_jobs_and_keeps_the_first_publication_job() -> None:
    other_publication = "curie-publication-" + "f" * 20
    unrelated = _watched_job("curie-cleanup-28817700")
    unrelated["spec"]["template"]["spec"]["volumes"] = []
    second = _watched_job(other_publication)
    second["spec"]["template"]["spec"]["volumes"] = []
    stream = (
        _event("ADDED", unrelated, indent=None)
        + _event("ADDED", _watched_job(PUBLICATION_JOB), indent=None)
        + _event("ADDED", second, indent=None)
        + _event("MODIFIED", unrelated, indent=None)
        + _event("DELETED", _watched_job(PUBLICATION_JOB), indent=None)
    )
    job = tls_code_host.publication_job_from_watch(stream)
    assert job is not None
    assert job["metadata"]["name"] == PUBLICATION_JOB
    assert _trust_volume(job) is not None


def test_watch_skips_malformed_lines_and_a_torn_final_event() -> None:
    torn = _event("MODIFIED", _watched_job(PUBLICATION_JOB))[:-40]
    stream = (
        'error: the server doesn\'t have a resource type "jobz"\n'
        "{not json\n"
        '"a bare string"\n'
        '{"type": "ADDED"}\n' + _event("ADDED", _watched_job(PUBLICATION_JOB)) + "}}}\n" + torn
    )
    job = tls_code_host.publication_job_from_watch(stream)
    assert job is not None
    assert job["metadata"]["name"] == PUBLICATION_JOB
    assert _trust_volume(job) is not None


@pytest.mark.parametrize(
    "stream",
    [
        "",
        "garbage\n",
        _event("ADDED", _watched_job("curie-cleanup-28817700")),
        # The bare Job object, not a watch event.
        json.dumps(_watched_job(PUBLICATION_JOB)) + "\n",
    ],
)
def test_watch_without_a_publication_job_is_none_and_the_judge_says_so(stream: str) -> None:
    assert tls_code_host.publication_job_from_watch(stream) is None
    obs = {**_passing(), "publication_job": None}
    _failures_with(obs, "no publication Job was observed")


# --------------------------------------------------------------------------
# The live collector: the Job is created and deleted between polls
# --------------------------------------------------------------------------


class _FakeWatch:
    """kubectl get jobs --watch: reports ADDED then DELETED, then blocks until stopped."""

    def __init__(self, argv: list[str], *, stdout: Any, stderr: Any) -> None:
        self.argv = argv
        self.terminated = False
        stdout.write(
            (
                _event("ADDED", _watched_job(PUBLICATION_JOB))
                + _event("DELETED", _watched_job(PUBLICATION_JOB))
            ).encode()
        )
        stdout.flush()

    def poll(self) -> int | None:
        return 0 if self.terminated else None

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.terminated = True

    def wait(self, timeout: float | None = None) -> int:
        return 0


class _FakeTeardown:
    def __init__(self) -> None:
        self.steps: list[tuple[str, Any]] = []

    def push(self, name: str, step: Any) -> None:
        self.steps.append((name, step))


class _FakePreflight:
    """The Preflight surface the watch and TlsCodeHostRun.await_ending read."""

    def __init__(self, record_status: str, workdir: Path) -> None:
        self.namespace = NAMESPACE
        self.workdir = workdir
        self.config = types.SimpleNamespace(kube_context="k8")
        self.teardown = _FakeTeardown()
        self.labelled_at = 0.0
        self.record_status = record_status
        self.polls = 0
        self.sql_queries: list[str] = []
        # Started just now, so the wait bound is far off.
        self.started_at = fe._now_iso()

    def publication_namespace(self) -> str:
        return f"{NAMESPACE}-curie-publication"

    def step(self, _what: str, **_facts: Any) -> None:
        return None

    def execution_request(self, _request_id: str) -> dict[str, Any]:
        self.polls += 1
        status = "running" if self.polls < 3 else "completed"
        return {"status": status, "started_at": self.started_at}

    def kubectl(self, *args: str, check: bool = True) -> str:
        if "jobs" in args:
            raise AssertionError("the publication Job is read from the watch, not polled")
        if "sandboxclaims.extensions.agents.x-k8s.io" in args:
            return json.dumps({"items": []})
        if "jsonpath={.status.phase}" in args:
            return "Running"
        raise AssertionError(f"unexpected kubectl {args}")

    def sql(self, query: str) -> list[list[str]]:
        self.sql_queries.append(query)
        return [[PUBLICATION_ID, self.record_status, "6f1c3a52-0000-4000-8000-000000000002"]]


def _collect(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, record_status: str
) -> tuple[tls_code_host.TlsCodeHostRun, _FakePreflight]:
    monkeypatch.setattr(tls_code_host.time, "sleep", lambda _s: None)
    watches: list[_FakeWatch] = []

    def popen(argv: list[str], **kwargs: Any) -> _FakeWatch:
        watch = _FakeWatch(argv, **kwargs)
        watches.append(watch)
        return watch

    monkeypatch.setattr(tls_code_host.subprocess, "Popen", popen)
    fake = _FakePreflight(record_status, tmp_path)
    run = tls_code_host.TlsCodeHostRun(fake)  # type: ignore[arg-type]
    run.request_id = "6f1c3a52-0000-4000-8000-000000000002"
    run.start_job_watch()
    assert len(watches) == 1
    argv = watches[0].argv
    assert argv[:3] == ["kubectl", "--context", "k8"]
    assert argv[argv.index("-n") + 1] == fake.publication_namespace()
    for flag in ("--watch", "--output-watch-events"):
        assert flag in argv
    assert argv[argv.index("-o") + 1] == "json"
    assert run.job_watch_file is not None
    assert (run.job_watch_file.stat().st_mode & 0o777) == 0o600
    assert [name for name, _ in fake.teardown.steps] == ["stop publication Job watch"]
    assert run.await_ending() is True
    # drive() stops the watch and captures once more after the loop.
    assert run.stop_job_watch() is True
    run._capture_publication_job()
    run._capture_publication_record()
    assert watches[0].terminated
    assert len(watches) == 1, "a live watch was restarted"
    assert fake.polls == 3
    assert fake.sql_queries, "the durable publication record was never read"
    assert all(PUBLICATION_ID[:20] in query for query in fake.sql_queries)
    return run, fake


def _collected_obs(run: tls_code_host.TlsCodeHostRun) -> dict[str, Any]:
    return {
        **_passing(),
        "request_statuses": list(run.statuses),
        "publication_job": run.publication_job,
        "publication_record": run.publication_record,
    }


def test_a_job_never_polled_still_passes_on_a_succeeded_record(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    run, _ = _collect(monkeypatch, tmp_path, "succeeded")
    assert run.publication_job is not None
    assert run.publication_job["metadata"]["name"] == PUBLICATION_JOB
    # The watch never reported success: completion is not from the Job.
    assert not run.publication_job["status"].get("succeeded")
    assert run.publication_record == {
        "id": PUBLICATION_ID,
        "status": "succeeded",
        "execution_request_id": "6f1c3a52-0000-4000-8000-000000000002",
    }
    assert tls_code_host.judge_tls_code_host(_collected_obs(run)) == []


def test_a_job_never_polled_fails_on_a_failed_record(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    run, _ = _collect(monkeypatch, tmp_path, "failed")
    assert run.publication_job is not None
    failures = tls_code_host.judge_tls_code_host(_collected_obs(run))
    assert failures and all("publication Job did not complete" in f for f in failures), failures
    assert "'failed'" in failures[0]


def test_the_job_watch_stops_on_teardown(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        tls_code_host.subprocess, "Popen", lambda argv, **kw: _FakeWatch(argv, **kw)
    )
    fake = _FakePreflight("succeeded", tmp_path)
    run = tls_code_host.TlsCodeHostRun(fake)  # type: ignore[arg-type]
    run.start_job_watch()
    ((_, stop),) = fake.teardown.steps
    assert stop() == {"stopped": True}
    assert run.job_watch is not None and run.job_watch.poll() is not None


def test_a_closed_job_watch_is_restarted_and_appends(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    watches: list[_FakeWatch] = []

    def popen(argv: list[str], **kwargs: Any) -> _FakeWatch:
        watches.append(_FakeWatch(argv, **kwargs))
        return watches[-1]

    monkeypatch.setattr(tls_code_host.subprocess, "Popen", popen)
    fake = _FakePreflight("succeeded", tmp_path)
    run = tls_code_host.TlsCodeHostRun(fake)  # type: ignore[arg-type]
    run.start_job_watch()
    watches[0].terminated = True  # the API server closed the watch
    run._keep_job_watch()
    assert len(watches) == 2 and run.job_watch is watches[1]
    assert run.job_watch_restarts == 1
    assert run.job_watch_file is not None
    text = run.job_watch_file.read_text()
    assert text.count('"ADDED"') == 2, "the restart truncated the earlier events"
    run._capture_publication_job()
    assert run.publication_job is not None
