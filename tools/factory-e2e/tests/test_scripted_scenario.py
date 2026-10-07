"""Negative controls for the scripted factory scenario (#3814).

The legacy helpers are the #3521 and #3617 behaviors. The test shows that
putting either one back makes the scenario assertion fail. The product does
not call them.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]


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


def test_empty_transcript_cannot_qualify_a_healthy_install(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A ready Helm release is not an executed #3814 factory scenario."""
    kind = _load_kind()
    transcript = tmp_path / "empty.json"
    transcript.write_text(json.dumps({"version": 1, "exchanges": []}))
    monkeypatch.delenv("CURIE_FACTORY_SCRIPTED_PLAN", raising=False)
    monkeypatch.setenv("CURIE_FACTORY_MODEL_BASE_URL", "http://10.1.2.3:8080")
    # Harness unit control only: the integration lane must execute real Helm.
    monkeypatch.setattr(
        kind.fe.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    args = argparse.Namespace(
        context="kind-example",
        namespace="test-factory-empty",
        transcript=str(transcript),
        model_base_url=None,
        listen_host="10.1.2.3",
    )
    assert kind.main(args) == kind.fe.EXIT_CONFIG
    assert "contains no recorded exchanges" in capsys.readouterr().err


def test_unconsumed_model_recording_fails_cleanup_after_run_success(tmp_path: Path) -> None:
    kind = _load_kind()
    transcript = tmp_path / "unconsumed.json"
    transcript.write_text(json.dumps({"version": 1, "exchanges": [{"match": {}}]}))
    model_module = kind.fixture_module(
        "cleanup_model_fixture", kind.REPO_ROOT / "tools/model-script/model_script.py"
    )
    model = model_module.ModelScript(transcript, require_consumed=True)
    model.start()
    fixtures = kind.fe.Teardown()
    fixtures.push("close model fixture", model.close)
    code, cleanup = kind.finish_run(None, fixtures, model, code=0, record=False)
    assert code == kind.fe.EXIT_FAILED
    assert cleanup[0]["ok"] is False
    assert "unconsumed" in cleanup[0]["detail"]


def test_declared_unittest_preflight_is_accepted() -> None:
    scenario.assert_preflight_outcome("passed")
    scenario.assert_preflight_outcome("failed")


@pytest.mark.parametrize(
    "fault", [None, "timeout", "early_stop", "missing_pending", "wrong_head", "failing"]
)
def test_ci_oracle_requires_published_head_pending_then_green(fault: str | None) -> None:
    head = "a" * 40
    result = {
        "terminal": True,
        "request_status": "completed",
        "terminal_cause": "completed",
        "ending_cause": "completed",
        "work_item_state": "published",
        "ci": {"state": "passing", "head_sha": head},
    }
    observations = [{"state": "pending", "head_sha": head}, {"state": "passing", "head_sha": head}]
    if fault in {"timeout", "early_stop"}:
        result.update(request_status="failed", terminal_cause=fault, ending_cause=fault)
    elif fault == "missing_pending":
        observations = observations[1:]
    elif fault == "wrong_head":
        observations[0]["head_sha"] = "b" * 40
    elif fault == "failing":
        result["ci"]["state"] = "failing"
    if fault is None:
        scenario.assert_ci_completion(result, observations, head)
    else:
        with pytest.raises(scenario.ScenarioAssertionError):
            scenario.assert_ci_completion(result, observations, head)


def test_fixture_bundle_preserves_factory_hooks_and_agents(tmp_path: Path) -> None:
    kind = _load_kind()
    source = kind.REPO_ROOT / "examples" / "dark-factory"
    target = kind.fixture_bundle(tmp_path)
    assert (target / "connectors.yaml").read_text() == "connectors: {}\n"
    for area in ("agents", "skills", "hooks"):
        for path in (source / area).rglob("*"):
            if path.is_file() and "__pycache__" not in path.parts:
                assert (target / path.relative_to(source)).read_bytes() == path.read_bytes()


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
    kind.refuse_context("kind-curie-e2e")
    with pytest.raises(kind.ScriptedScenarioError):
        kind.refuse_context("k8")
    values = kind.scripted_values(
        model_base_url="http://10.1.2.3:8080",
        github_api="https://10.9.9.9:8443/api/v3",
        clone_base="https://10.9.9.9:8443",
        ca_configmap="github-stub-ca",
    )
    assert {"name": "CURIE_MODEL_BASE_URL", "value": "http://10.1.2.3:8080"} in values["worker"][
        "extraEnv"
    ]
    assert values["api"]["githubStubCaConfigMap"] == "github-stub-ca"
    assert {"cidr": "10.1.2.3/32", "ports": [{"protocol": "TCP", "port": 8080}]} in values[
        "security"
    ]["networkPolicy"]["allowedEgress"]


def test_reintroducing_curie_path_prefixes_fails_the_scenario() -> None:
    filtered = scenario.legacy_3617_paths(scenario.FIXTURE_CHANGED_PATHS)
    with pytest.raises(scenario.ScenarioAssertionError, match="unitconv"):
        scenario.assert_publication_paths(filtered)


def test_scripted_release_cannot_take_ownership_of_the_ladder_policy() -> None:
    kind = _load_kind()
    assert kind.ScriptedPreflight.release == "curie-factory-scripted"
    assert kind.ScriptedPreflight.release != kind.fe.Preflight.release


@pytest.mark.parametrize("owner", ["owned-run", "foreign-run", "timeout"])
def test_failure_diagnostics_are_owned_bounded_and_exclude_specs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, owner: str
) -> None:
    kind = _load_kind()
    calls = []

    def kubectl(argv, **kwargs):
        calls.append(argv)
        assert kwargs["timeout"] == 15
        assert "--request-timeout=10s" in argv
        assert "--context" in argv and "kind-example" in argv
        if "namespace" in argv:
            if owner == "timeout":
                raise subprocess.TimeoutExpired(argv, 15)
            return subprocess.CompletedProcess(argv, 0, owner, "")
        assert argv[argv.index("-n") + 1] == "test-factory-diagnostic"
        assert not any(part in {"secrets", "describe", "-A", "yaml", "json"} for part in argv)
        if "logs" in argv:
            assert "job/curie-factory-scripted-schema-migrate" in argv
            return subprocess.CompletedProcess(
                argv,
                0,
                "possibly truncated first line\nWaiting for Postgres readiness "
                "postgresql://user:example-password@postgres/db example-provider-key",
                "",
            )
        return subprocess.CompletedProcess(argv, 0, "schema-migrate Pending Unschedulable", "")

    monkeypatch.setattr(kind.subprocess, "run", kubectl)
    preflight = SimpleNamespace(
        config=SimpleNamespace(kube_context="kind-example", model_api_key="example-provider-key"),
        namespace="test-factory-diagnostic",
        run_id="owned-run",
        release="curie-factory-scripted",
    )
    diagnostics = kind.collect_failure_diagnostics(preflight)
    if owner == "owned-run":
        assert "Unschedulable" in diagnostics["pods"]["output"]
        assert "Waiting for Postgres readiness" in diagnostics["schema_log"]["output"]
        assert "example-password" not in str(diagnostics)
        assert "example-provider-key" not in str(diagnostics)
        assert {"pods", "jobs", "events", "schema_log"} <= diagnostics.keys()
    else:
        assert len(calls) == 1
        assert diagnostics["ownership"] != "verified"


@pytest.mark.parametrize("boundary", ["local", "upstream"])
@pytest.mark.parametrize("material", ["credential", "dsn"])
def test_failure_diagnostics_do_not_publish_cutoff_straddling_credentials(
    monkeypatch: pytest.MonkeyPatch, boundary: str, material: str
) -> None:
    kind = _load_kind()
    secret = "example-secret-prefix-private-suffix"
    sensitive = secret if material == "credential" else f"postgresql://user:{secret}@postgres/db"
    # Local retention starts within the credential. Upstream byte retention
    # has already removed the prefix, leaving an unrecognizable first line.
    if boundary == "local":
        retained_suffix = sensitive[sensitive.index("private-suffix") :]
        captured = sensitive + "x" * (12000 - len(retained_suffix))
    else:
        captured = "private-suffix@postgres/db\nWaiting for Postgres readiness\n"

    def kubectl(argv, **_kwargs):
        output = "owned-run" if "namespace" in argv else "safe status"
        if (boundary == "local" and "events" in argv) or (
            boundary == "upstream" and "logs" in argv
        ):
            output = captured
        return subprocess.CompletedProcess(argv, 0, output, "")

    monkeypatch.setattr(kind.subprocess, "run", kubectl)
    preflight = SimpleNamespace(
        config=SimpleNamespace(
            kube_context="kind-example",
            model_api_key=secret if material == "credential" else "unrelated-example-key",
        ),
        namespace="test-factory-diagnostic",
        run_id="owned-run",
        release="curie-factory-scripted",
    )
    diagnostics = kind.collect_failure_diagnostics(preflight)
    assert "private-suffix" not in str(diagnostics)
    if boundary == "upstream":
        assert diagnostics["schema_log"]["output"] == "Waiting for Postgres readiness\n"


@pytest.mark.parametrize("diagnostic_error", [False, True])
def test_failure_diagnostics_precede_cleanup_without_masking_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, diagnostic_error: bool
) -> None:
    kind = _load_kind()
    order = []

    def diagnostics(_):
        order.append("diagnostic")
        if diagnostic_error:
            raise RuntimeError("example-private-detail")
        return {"ownership": "verified"}

    monkeypatch.setattr(
        kind,
        "collect_failure_diagnostics",
        diagnostics,
        raising=False,
    )
    teardown = kind.fe.Teardown()
    teardown.push("delete owned namespace", lambda: order.append("cleanup") or {})
    workdir = tmp_path / "owned"
    workdir.mkdir()
    preflight = SimpleNamespace(
        teardown=teardown,
        evidence={},
        workdir=workdir,
        write_evidence=lambda: None,
    )
    code, _ = kind.finish_run(
        preflight,
        kind.fe.Teardown(),
        SimpleNamespace(_exchanges=[]),
        code=kind.fe.EXIT_FAILED,
        record=True,
    )
    assert order == ["diagnostic", "cleanup"]
    assert code == kind.fe.EXIT_FAILED
    if diagnostic_error:
        assert preflight.evidence["failure_diagnostics"] == {"error": "RuntimeError"}
    else:
        assert preflight.evidence["failure_diagnostics"]["ownership"] == "verified"
    assert not workdir.exists()


def test_kind_loaded_runner_never_resolves_a_registry_pull(tmp_path: Path) -> None:
    import yaml

    kind = _load_kind()
    values = kind.scripted_values(
        model_base_url="http://10.1.2.3:8080",
        github_api="https://10.9.9.9:8443/api/v3",
        clone_base="https://10.9.9.9:8443",
        ca_configmap="github-stub-ca",
    )
    runner = values["agentSandbox"]["runner"]
    assert runner["image"] == "curie-runner" and runner["tag"] == "latest"
    assert runner["imagePullPolicy"] == "Never"
    assert runner["prewarm"]["imagePullPolicy"] == "Never"
    path = tmp_path / "values.yaml"
    path.write_text(yaml.safe_dump(values))
    rendered = subprocess.check_output(
        [
            "helm",
            "template",
            "curie-factory-scripted",
            str(ROOT.parents[1] / "charts/curie"),
            "--namespace",
            "test-factory-scripted",
            "-f",
            str(path),
        ],
        text=True,
    )
    prewarm = next(d for d in yaml.safe_load_all(rendered) if d and d["kind"] == "DaemonSet")
    image = prewarm["spec"]["template"]["spec"]["containers"][0]
    assert image["image"] == "curie-runner:latest" and image["imagePullPolicy"] == "Never"


@pytest.mark.parametrize("context", ["kind-other", "production"])
def test_ladder_retirement_refuses_any_other_context(context: str) -> None:
    kind = _load_kind()
    with pytest.raises(kind.fe.PreflightFailed, match="job-owned"):
        kind.retire_ci_ladder(context)


def test_ladder_retirement_requires_the_expected_release_identity() -> None:
    kind = _load_kind()
    expected = {"name": "curie", "namespace": "curie", "info": {"status": "deployed"}}
    kind.validate_ladder_release(expected)
    for field, value in [("name", "other"), ("namespace", "other")]:
        wrong = {**expected, field: value}
        with pytest.raises(kind.fe.PreflightFailed, match="release"):
            kind.validate_ladder_release(wrong)


def test_factory_workflow_retires_ladder_only_after_its_required_proofs() -> None:
    import yaml

    workflow = yaml.safe_load((ROOT.parents[1] / ".github/workflows/ci.yaml").read_text())
    steps = workflow["jobs"]["e2e-ladder-cluster"]["steps"]
    names = [x.get("name", "") for x in steps]
    retire = names.index("Retire the proven ladder namespace before the factory scenario")
    assert names.index("Credential free redeploy clears stale secret metadata") < retire
    assert retire < names.index("Scripted factory scenario")
    assert steps[retire]["if"] == "needs.changes.outputs.factory == 'true'"
    assert "retire_ci_ladder" in steps[retire]["run"]
    # Kubernetes is a uv workspace dependency, not guaranteed in system Python.
    assert steps[retire]["run"].startswith("uv run python - <<")


@pytest.mark.parametrize("replaced_shared", [False, True])
def test_ladder_retirement_deletes_only_the_observed_namespace_uid(
    monkeypatch: pytest.MonkeyPatch,
    replaced_shared: bool,
) -> None:
    # Provider contract: mismatched UID preconditions reject deletion with 409.
    # https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/delete-options/
    from kubernetes import client, config

    kind = _load_kind()
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_JOB", "e2e-ladder-cluster")
    monkeypatch.setattr(config, "load_kube_config", lambda **_kwargs: None)
    deleted = []
    core = SimpleNamespace(
        read_namespace=lambda name, **kwargs: SimpleNamespace(
            metadata=SimpleNamespace(uid="owned-namespace-uid", deletion_timestamp=None)
        ),
        delete_namespace=lambda name, body, **kwargs: deleted.append(
            (name, body.preconditions.uid)
        ),
    )
    apps = SimpleNamespace(
        read_namespaced_deployment=lambda name, namespace, **kwargs: SimpleNamespace(
            metadata=SimpleNamespace(uid="controller-uid"),
            status=SimpleNamespace(available_replicas=1),
        )
    )
    monkeypatch.setattr(client, "CoreV1Api", lambda: core)
    monkeypatch.setattr(client, "AppsV1Api", lambda: apps)
    commands = []
    shared_reads = 0

    def run(argv):
        nonlocal shared_reads
        commands.append(argv)
        if argv[0] == "helm":
            return json.dumps(
                {"name": "curie", "namespace": "curie", "info": {"status": "deployed"}}
            )
        if "crds,priorityclasses" in argv:
            shared_reads += 1
            uid = "changed" if replaced_shared and shared_reads == 2 else "original"
            return json.dumps(
                {
                    "items": [
                        {
                            "kind": "CustomResourceDefinition",
                            "metadata": {"name": "sandboxes.example.com", "uid": uid},
                        }
                    ]
                }
            )
        return "sanitized capacity columns"

    monkeypatch.setattr(kind.fe, "run", run)
    if replaced_shared:
        with pytest.raises(kind.fe.PreflightFailed, match="identity changed"):
            kind.retire_ci_ladder("kind-curie-e2e")
    else:
        kind.retire_ci_ladder("kind-curie-e2e")
    assert deleted == [("curie", "owned-namespace-uid")]
    assert any("namespace/curie" in cmd and "--timeout=300s" in cmd for cmd in commands)
    assert not any("uninstall" in cmd for cmd in commands)
