"""Contracts for the pre-runtime repository runner scenario judge.

The fixture samples describe synthetic recorded shapes. These tests prove the
judge accepts or refuses those shapes, not that any live component implements
repository runner routing, a manifest, or a model request.
"""

from __future__ import annotations

import copy
import hashlib
import json
import shutil
import subprocess
import urllib.error
import urllib.request
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import factory_e2e as fe
import pytest
import repo_runner
from channel_protocol.identity import scoped_conversation_id

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "fixtures" / "repo-runner"
REPO_X = "acme-corp/repo-x"
REPO_Y = "acme-corp/repo-y"


def _passing() -> dict[str, Any]:
    return json.loads((FIXTURES / "passing.json").read_text())["scenario"]


def _node(observation: dict[str, Any], path: tuple[Any, ...]) -> Any:
    node: Any = observation
    for part in path:
        node = node[part]
    return node


def _set(observation: dict[str, Any], path: tuple[Any, ...], value: Any) -> None:
    _node(observation, path[:-1])[path[-1]] = value


def _manifest(observation: dict[str, Any], turn: int = 0) -> dict[str, Any]:
    return json.loads(observation["turns"][turn]["manifest_output"])


def _plan() -> dict[str, Any]:
    configs = _passing()["bindings"]
    pools = {
        repository: {
            "pool": "fixture-x" if repository == REPO_X else "fixture-y",
            "digests": [config["image"].split("@", 1)[1], *config["platformDigests"].values()],
            "base": config["base"],
            "docker": False,
        }
        for repository, config in configs.items()
    }
    missing_pool = copy.deepcopy(pools)
    missing_pool[REPO_X]["pool"] = "absent-fixture-x"
    stale_base = copy.deepcopy(pools)
    stale_base[REPO_X]["base"] = "sha256:" + "f" * 64
    return {
        "bindings": [
            {
                "name": "fixture-x" if repository == REPO_X else "fixture-y",
                "tracker": "github",
                "trackerScopeId": "101" if repository == REPO_X else "102",
                "repos": [
                    {
                        "alias": "x" if repository == REPO_X else "y",
                        "codeHost": "github",
                        "projectId": "101" if repository == REPO_X else "102",
                        "path": repository,
                        "runner": config,
                        "docker": {"enabled": False},
                        "bases": ["main"],
                        "ci": {},
                    }
                ],
            }
            for repository, config in configs.items()
        ],
        "turns": [
            {
                "repository": REPO_X,
                "thread": "shared-thread",
                "text": "Read manifest",
                "expect": "completed",
            },
            {
                "repository": REPO_Y,
                "thread": "shared-thread",
                "text": "Read manifest",
                "expect": "completed",
            },
            {
                "repository": REPO_X,
                "thread": "missing-pool-thread",
                "text": "Read manifest",
                "expect": "repo_runner_pool_missing",
                "worker_pools": missing_pool,
            },
            {
                "repository": REPO_X,
                "thread": "stale-base-thread",
                "text": "Read manifest",
                "expect": "repo_runner_base_mismatch",
                "worker_pools": stale_base,
            },
        ],
    }


def _plan_env(tmp_path: Path, plan: Any) -> dict[str, str]:
    path = tmp_path / "repo-runner-plan.json"
    path.write_text(json.dumps(plan))
    return {"CURIE_FACTORY_REPO_RUNNER_PLAN": str(path)}


def _config_env(tmp_path: Path) -> dict[str, str]:
    key = tmp_path / "fixture-private-key.pem"
    key.write_text("fixture private key; no cryptographic operation in config loading")
    secret = tmp_path / "fixture-webhook-secret"
    secret.write_text("fixture-secret")
    return {
        **_plan_env(tmp_path, _plan()),
        "CURIE_FACTORY_KUBE_CONTEXT": "k8",
        "CURIE_LOCAL_STUB_HOST": "198.51.100.10",
        "CURIE_FACTORY_APP_ID": "1",
        "CURIE_FACTORY_INSTALLATION_ID": "1",
        "CURIE_FACTORY_APP_PRIVATE_KEY_FILE": str(key),
        "CURIE_FACTORY_WEBHOOK_SECRET_FILE": str(secret),
        "CURIE_FACTORY_REPO": REPO_X,
        "CURIE_FACTORY_MENTION": "fixture-factory",
        "CURIE_FACTORY_ACTOR_TOKEN": "fixture-token",
        "CURIE_FACTORY_LAYER_REGISTRY": "registry.example.com/acme",
    }


@pytest.mark.parametrize(
    ("field", "value", "diagnostic"),
    [
        ("CURIE_FACTORY_KUBE_CONTEXT", "production", "repo-runner only uses --context k8"),
        (
            "CURIE_LOCAL_STUB_HOST", "127.0.0.1",
            "CURIE_LOCAL_STUB_HOST must be reachable from the k8 cluster",
        ),
        (
            "CURIE_LOCAL_STUB_HOST", "0.0.0.0",
            "CURIE_LOCAL_STUB_HOST must be reachable from the k8 cluster",
        ),
    ],
)
def test_config_refuses_wrong_context_and_unreachable_callback_hosts(
    field: str, value: str, diagnostic: str, tmp_path: Path
) -> None:
    env = _config_env(tmp_path)
    env[field] = value
    with pytest.raises(fe.ConfigError) as refused:
        repo_runner.load_repo_runner_config(env, context=None)
    assert str(refused.value) == diagnostic


def test_config_accepts_its_owned_scripted_model_and_reachable_callback(tmp_path: Path) -> None:
    config = repo_runner.load_repo_runner_config(_config_env(tmp_path), context=None)
    assert config.kube_context == "k8"
    assert config.model_api_key == repo_runner.SCRIPTED_KEY
    assert config.model_base_url is None
    assert config.repo == REPO_X


@pytest.fixture
def preflight(tmp_path: Path) -> Iterator[fe.Preflight]:
    config = fe.FactoryConfig(
        kube_context="k8",
        app_id="1",
        installation_id=1,
        private_key_file=tmp_path / "fixture.pem",
        repo=REPO_X,
        label="fixture",
        mention="fixture",
        cloudflared="cloudflared",
        priority_classes=None,
        restore_webhook_url=None,
        webhook_secret="fixture-secret",
        actor_token="fixture-token",
        model_api_key=repo_runner.SCRIPTED_KEY,
    )
    value = fe.Preflight(
        config,
        repo_root=ROOT.parents[1],
        candidate="c" * 40,
        namespace="test-factory-tools",
        evidence_path=tmp_path / "evidence.json",
        admission_timeout=1,
        scenario_name="repo-runner",
    )
    value.chart_dir = ROOT.parents[1] / "charts" / "curie"
    value.api_key = "fixture-api-key"
    try:
        yield value
    finally:
        shutil.rmtree(value.workdir)


class _KubernetesCli:
    """Synthetic external CLI replies; all product collector methods stay real."""

    def __init__(self, p: fe.Preflight) -> None:
        self.p = p
        self.calls: list[list[str]] = []
        self.patches: list[list[dict[str, Any]]] = []
        self.started = False
        self.exited = False
        self.claims_enabled = True
        self.repository = REPO_X
        self.post_exit_claim: str | None = None
        self.retirement_queries: list[str] = []
        self.retirement_stays = False
        self.probes = [{"status": "done", "turn_active": False}]
        self.probe_calls = 0
        self.cas_conflict = False
        self.outcome: dict[str, Any] = {
            "finalized": True, "failed": False, "reply": "Manifest captured."
        }
        pools = copy.deepcopy(_plan()["turns"][2]["worker_pools"])
        pools[REPO_X]["pool"] = "fixture-x"
        for pool in pools.values():
            pool["egress"] = []
        self.deployment = {
            "metadata": {"resourceVersion": "1"},
            "spec": {"template": {"spec": {"containers": [
                {"name": "sidecar", "env": [{"name": "UNCHANGED", "value": "sidecar"}]},
                {"name": "worker", "env": [
                    {"name": "UNCHANGED", "value": "worker"},
                    {"name": "CURIE_REPO_RUNNER_POOLS", "value": json.dumps(pools)},
                ]},
            ]}}},
        }

    def pool_value(self) -> str:
        return str(self.deployment["spec"]["template"]["spec"]["containers"][1]["env"][1]["value"])

    def run(self, argv: list[str], *, check: bool = True, input_text: str | None = None) -> str:
        self.calls.append(list(argv))
        assert argv[:3] == ["kubectl", "--context", "k8"]
        assert self.p.namespace in argv
        get_resource = argv[argv.index("get") + 1] if "get" in argv else None
        if get_resource == "deployment":
            return json.dumps(self.deployment)
        if "patch" in argv:
            patch = json.loads(argv[argv.index("-p") + 1])
            self.patches.append(patch)
            if self.cas_conflict:
                self.deployment["spec"]["template"]["spec"]["containers"][1]["env"][1][
                    "name"
                ] = "UNRELATED_VARIABLE"
            for operation in patch:
                node: Any = self.deployment
                parts = operation["path"].strip("/").split("/")
                for part in parts[:-1]:
                    node = node[int(part)] if isinstance(node, list) else node[part]
                leaf: Any = int(parts[-1]) if isinstance(node, list) else parts[-1]
                if operation["op"] == "test":
                    if node[leaf] != operation["value"]:
                        raise fe.PreflightFailed("external compare-and-swap rejected changed field")
                else:
                    assert operation["op"] == "replace"
                    node[leaf] = operation["value"]
            return "deployment patched"
        if "rollout" in argv:
            return "deployment rolled out"
        if get_resource == "sandboxclaims.extensions.agents.x-k8s.io":
            claims = []
            if self.started and self.claims_enabled:
                turn = _passing()["turns"][0 if self.repository == REPO_X else 1]
                name = (
                    self.post_exit_claim
                    if self.exited and self.post_exit_claim else turn["claim"]
                )
                claims = [{
                    "metadata": {"name": name},
                    "spec": {"env": [{"name": "CURIE_REPO_PATH", "value": self.repository}]},
                    "status": {"sandbox": {"name": turn["sandbox"]}},
                }]
            return json.dumps({"items": claims})
        if get_resource == "sandboxclaim":
            assert argv[argv.index("sandboxclaim") + 2 :] == ["--ignore-not-found", "-o", "name"]
            name = argv[argv.index("sandboxclaim") + 1]
            self.retirement_queries.append(name)
            if self.retirement_stays or len(self.retirement_queries) == 1:
                return f"sandboxclaim/{name}"
            return ""
        if get_resource == "pod":
            turn = _passing()["turns"][0 if self.repository == REPO_X else 1]
            return json.dumps({
                "metadata": {"uid": turn["pod_uid"]},
                "spec": {"containers": [{
                    "name": "runner", "image": turn["spec_image"],
                    "ports": [{"name": "aci", "containerPort": 4321}],
                }]},
                "status": {"containerStatuses": [{"name": "runner", "imageID": turn["image_id"]}]},
            })
        if "logs" in argv:
            return _passing()["turns"][0 if self.repository == REPO_X else 1]["boot_log"]
        if "exec" in argv:
            assert "/status" in argv[-1]
            status = self.probes[min(self.probe_calls, len(self.probes) - 1)]
            self.probe_calls += 1
            return json.dumps(status)
        if get_resource == "sandboxwarmpool":
            return "" if "absent-fixture-x" in argv else "sandboxwarmpool/fixture-x"
        raise AssertionError(f"unexpected external kubectl command {argv}")

    def subprocess_run(self, argv: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(argv))
        assert argv[:3] == ["kubectl", "config", "view"]
        assert argv[argv.index("--context") + 1] == "k8"
        return subprocess.CompletedProcess(argv, 0, "apiVersion: v1\nkind: Config\n", "")


@pytest.fixture
def offline_driver(
    preflight: fe.Preflight, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[repo_runner.RepoRunnerRun, _KubernetesCli]]:
    preflight.release = "factory-tools"
    driver = repo_runner.RepoRunnerRun(preflight, _plan())
    cli = _KubernetesCli(preflight)
    driver.baseline_pools = json.loads(cli.pool_value())
    driver.installed_base = "sha256:" + "e" * 64
    model = repo_runner.ScriptedManifestModel(host="127.0.0.1", port=0)
    model.start()
    driver.model = model
    class MessageProcess:
        returncode = 0

        def __init__(self, argv: list[str], **_kwargs: Any) -> None:
            assert argv[:5] == ["curie", "cluster", "--context", "k8", "message"]
            assert argv[argv.index("--release") + 1] == "factory-tools"
            assert argv[argv.index("--namespace") + 1] == preflight.namespace
            text = argv[argv.index("message") + 1]
            cli.repository = next(
                repository for repository in (REPO_X, REPO_Y)
                if f"https://github.com/{repository}\n" in text
            )
            cli.started = True
            cli.exited = False
            if cli.claims_enabled:
                first = _first_model_body()
                status, response = _model_request(model, first)
                assert status == 200
                result = _result_model_body(first, response)
                sample = _passing()["turns"][0 if cli.repository == REPO_X else 1]
                result["messages"][-1]["content"][0]["content"] = sample["manifest_output"]
                assert _model_request(model, result)[0] == 200

        def poll(self) -> int:
            cli.exited = True
            return self.returncode

        def communicate(self, *, timeout: int) -> tuple[str, str]:
            return json.dumps(cli.outcome), ""

    monkeypatch.setattr(fe, "run", cli.run)
    monkeypatch.setattr(subprocess, "run", cli.subprocess_run)
    monkeypatch.setattr(subprocess, "Popen", MessageProcess)
    monkeypatch.setenv("CURIE_LOCAL_STUB_HOST", "198.51.100.10")
    try:
        yield driver, cli
    finally:
        assert model.close() == {"stopped": True}


def test_scenario_channel_passes_the_actual_api_channel_write_schema(
    preflight: fe.Preflight,
) -> None:
    from curie_api.schemas.channels import ChannelBindingWrite

    driver = repo_runner.RepoRunnerRun(preflight, _plan())
    accepted = ChannelBindingWrite(kind="slack", address=driver.channel)
    assert accepted.address == driver.channel


def test_namespace_create_payload_already_carries_the_scenario_owner(
    preflight: fe.Preflight, monkeypatch: pytest.MonkeyPatch
) -> None:
    created: list[dict[str, Any]] = []

    def external_run(
        argv: list[str], *, check: bool = True, input_text: str | None = None
    ) -> str:
        assert argv[:3] == ["kubectl", "--context", "k8"]
        if "create" in argv:
            assert [name for name, _ in preflight.teardown._steps] == ["delete namespaces"]
            created.append(json.loads(input_text or "{}"))
        else:
            assert "get" in argv and "namespace" in argv
        return ""

    monkeypatch.setattr(fe, "run", external_run)
    preflight.create_namespace()
    assert len(created) == 1
    labels = created[0]["metadata"]["labels"]
    owner_name, owner_value = fe.OWNER_LABEL.split("=", 1)
    assert labels[owner_name] == owner_value
    assert labels["owner"] == "factory-tools"


def test_claim_selection_uses_the_scoped_thread_hash_in_the_actual_kubectl_argv(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, cli = offline_driver
    thread = "fixture:thread/1"
    assert driver._claims(thread) == []
    scoped = scoped_conversation_id("slack", driver.channel, thread)
    digest = hashlib.sha256(scoped.encode()).hexdigest()[:10]
    command = cli.calls[-1]
    assert command[command.index("-l") + 1] == (
        f"curietech.ai/thread-hash={digest},curietech.ai/agent={fe.FACTORY_AGENT}"
    )


def test_turn_completion_uses_real_claim_pod_probe_and_scripted_http_evidence(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, cli = offline_driver
    turn = driver._turn(0, _plan()["turns"][0])
    assert turn["status"] == "completed"
    assert turn["cli_exit_code"] == 0
    assert turn["runner_status"] == {"status": "done", "turn_active": False}
    assert turn["observed_repository"] == REPO_X
    assert turn["capture_error"] is None
    assert turn["model_requests"] == 2
    assert turn["manifest_output"] == _passing()["turns"][0]["manifest_output"]
    assert turn["errors"] == []
    assert cli.patches == []


def test_turn_reprobes_after_cli_exit_and_includes_claims_first_seen_then(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, cli = offline_driver
    cli.probes = [
        {"status": "running", "turn_active": True},
        {"status": "done", "turn_active": False},
    ]
    cli.post_exit_claim = "fixture-claim-after-exit"
    turn = driver._turn(0, _plan()["turns"][0])
    assert turn["status"] == "completed"
    assert cli.probe_calls >= 2
    assert turn["claim"] == "fixture-claim-after-exit"
    assert "fixture-claim-after-exit" in turn["new_claims"]


@pytest.mark.parametrize("stays", [False, True], ids=["claim-disappears", "claim-remains"])
def test_repository_switch_collects_actual_retirement_and_the_judge_requires_it(
    stays: bool,
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver, cli = offline_driver
    cli.retirement_stays = stays
    ticks = iter(range(0, 1000, 20))
    monkeypatch.setattr(fe.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(fe.time, "sleep", lambda _interval: None)
    first = driver._turn(0, _plan()["turns"][0])
    second = driver._turn(1, _plan()["turns"][1])
    assert first["status"] == second["status"] == "completed"
    assert first["repository"] == REPO_X and second["repository"] == REPO_Y
    assert first["claim"] != second["claim"]
    assert cli.retirement_queries and set(cli.retirement_queries) == {first["claim"]}
    driver.obs["turns"].extend(copy.deepcopy(_passing()["turns"][2:]))
    failures = repo_runner.judge_repo_runner(driver.obs)
    if stays:
        assert second["retired_claims"] == []
        assert any("did not retire" in failure for failure in failures), failures
    else:
        assert cli.retirement_queries == [first["claim"], first["claim"]]
        assert second["retired_claims"] == [first["claim"]]
        assert failures == []


def test_second_turn_in_the_same_repository_does_not_retire_its_retained_claim(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, cli = offline_driver
    planned = _plan()["turns"][0]
    first = driver._turn(0, planned)
    second = driver._turn(1, copy.deepcopy(planned))
    assert first["status"] == second["status"] == "completed"
    assert first["claim"] == second["claim"]
    assert second["retired_claims"] == []
    assert cli.retirement_queries == []


@pytest.mark.parametrize("missing_proof", ["finalized", "done-probe"])
def test_turn_does_not_complete_from_cli_exit_alone(
    missing_proof: str, offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli]
) -> None:
    driver, cli = offline_driver
    if missing_proof == "finalized":
        cli.outcome["finalized"] = False
    else:
        cli.probes = [{"status": "running", "turn_active": True}]
    assert driver._turn(0, _plan()["turns"][0])["status"] == "failed"


@pytest.mark.parametrize("failure", ["workspace-conflict", "generic-runner-failure"])
def test_turn_keeps_named_workspace_diagnostics_and_refuses_no_generic_failure(
    failure: str, offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli]
) -> None:
    driver, cli = offline_driver
    cli.claims_enabled = False
    cli.outcome = {"failed": True, "failure_class": "runner_failed", "reply": "A runner failed."}
    if failure == "workspace-conflict":
        # The worker's workspace.py maps workspace.selection_conflict to this exact reply.
        cli.outcome["reply"] = "This thread is already bound to a different repository."
    turn = driver._turn(0, _plan()["turns"][0])
    if failure == "workspace-conflict":
        assert turn["reason"] == "workspace.selection_conflict"
        assert any(
            "workspace.selection_conflict" in error
            for error in repo_runner.judge_repo_runner(driver.obs)
        )
    else:
        assert turn["status"] == "failed"
        assert turn["reason"] == "runner_failed"


@pytest.mark.parametrize("index", [2, 3], ids=["missing-pool", "stale-base"])
def test_fault_turn_changes_only_its_declared_target_and_preserves_named_refusal(
    index: int, offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli]
) -> None:
    driver, cli = offline_driver
    planned = _plan()["turns"][index]
    before = copy.deepcopy(driver.baseline_pools)
    cli.claims_enabled = False
    cli.outcome = {"failed": True, "failure_class": planned["expect"], "reply": "Refused."}
    turn = driver._turn(index, planned)
    assert turn["status"] == "refused"
    assert turn["reason"] == planned["expect"]
    after = json.loads(cli.pool_value())
    field = "pool" if index == 2 else "base"
    expected = copy.deepcopy(before)
    expected[REPO_X][field] = planned["worker_pools"][REPO_X][field]
    assert after == expected
    assert turn["new_claims"] == []
    assert turn["model_requests"] == 0


@pytest.mark.parametrize("index", [2, 3], ids=["missing-pool", "stale-base"])
def test_fault_turn_rejects_changes_to_an_unrelated_repository_before_patching(
    index: int, offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli]
) -> None:
    driver, cli = offline_driver
    planned = _plan()["turns"][index]
    planned["worker_pools"][REPO_Y]["pool"] = "unrelated-pool"
    with pytest.raises(fe.ConfigError, match="outside its one target fault"):
        driver._turn(index, planned)
    assert cli.patches == []


def test_pool_patch_compares_field_name_and_value_then_restores_the_exact_initial_map(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, cli = offline_driver
    initial = cli.pool_value()
    fault = copy.deepcopy(driver.baseline_pools)
    fault[REPO_X]["pool"] = "absent-fixture-x"
    assert driver._set_pools(fault) == fault
    patch = cli.patches[0]
    pointer = "/spec/template/spec/containers/1/env/1"
    assert {"op": "test", "path": pointer + "/name", "value": "CURIE_REPO_RUNNER_POOLS"} in patch
    assert {"op": "test", "path": pointer + "/value", "value": initial} in patch
    assert all(operation["path"] != "/metadata/resourceVersion" for operation in patch)
    assert driver._set_pools(None) == driver.baseline_pools
    assert cli.pool_value() == initial
    assert driver.fault_active is False


def test_pool_patch_refuses_a_field_name_changed_after_observation(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, cli = offline_driver
    cli.cas_conflict = True
    fault = copy.deepcopy(driver.baseline_pools)
    fault[REPO_X]["pool"] = "absent-fixture-x"
    with pytest.raises(
        fe.PreflightFailed, match="external compare-and-swap rejected changed field"
    ):
        driver._set_pools(fault)


def test_plan_loads_full_chart_bindings_and_four_declared_turns(tmp_path: Path) -> None:
    plan = _plan()
    loaded = repo_runner.load_plan(_plan_env(tmp_path, plan))
    assert loaded["bindings"] == plan["bindings"]
    assert loaded["turns"] == plan["turns"]


@pytest.mark.parametrize("env", [{}, {"CURIE_FACTORY_REPO_RUNNER_PLAN": ""}])
def test_plan_path_is_required(env: dict[str, str]) -> None:
    with pytest.raises(fe.ConfigError, match="CURIE_FACTORY_REPO_RUNNER_PLAN"):
        repo_runner.load_plan(env)


def test_plan_path_must_be_readable(tmp_path: Path) -> None:
    with pytest.raises(fe.ConfigError):
        repo_runner.load_plan(
            {"CURIE_FACTORY_REPO_RUNNER_PLAN": str(tmp_path / "absent-plan.json")}
        )


def test_plan_requires_json(tmp_path: Path) -> None:
    path = tmp_path / "repo-runner-plan.json"
    path.write_text("{")
    with pytest.raises(fe.ConfigError):
        repo_runner.load_plan({"CURIE_FACTORY_REPO_RUNNER_PLAN": str(path)})


@pytest.mark.parametrize("plan", [None, [], "plan", 1, True, {}])
def test_plan_requires_a_complete_mapping(plan: Any, tmp_path: Path) -> None:
    with pytest.raises(fe.ConfigError):
        repo_runner.load_plan(_plan_env(tmp_path, plan))


@pytest.mark.parametrize(
    "path",
    [
        ("bindings",),
        ("turns",),
        ("bindings", 0, "name"),
        ("bindings", 0, "tracker"),
        ("bindings", 0, "trackerScopeId"),
        ("bindings", 0, "repos"),
        ("bindings", 0, "repos", 0, "alias"),
        ("bindings", 0, "repos", 0, "codeHost"),
        ("bindings", 0, "repos", 0, "projectId"),
        ("bindings", 0, "repos", 0, "path"),
        ("bindings", 0, "repos", 0, "runner"),
        ("turns", 0, "repository"),
        ("turns", 0, "thread"),
        ("turns", 0, "text"),
        ("turns", 0, "expect"),
        ("turns", 2, "worker_pools"),
        ("turns", 3, "worker_pools"),
    ],
)
def test_each_required_plan_field_is_required(path: tuple[Any, ...], tmp_path: Path) -> None:
    plan = _plan()
    del _node(plan, path[:-1])[path[-1]]
    with pytest.raises(fe.ConfigError):
        repo_runner.load_plan(_plan_env(tmp_path, plan))


@pytest.mark.parametrize(
    ("path", "value"),
    [
        pytest.param(("bindings",), [], id="empty-chart-bindings"),
        pytest.param(("bindings",), {}, id="chart-bindings-not-list"),
        pytest.param(("bindings", 0, "tracker"), "unknown", id="unknown-tracker"),
        pytest.param(("bindings", 1, "repos", 0, "path"), REPO_X, id="duplicate-repository"),
        pytest.param(("bindings", 0, "repos", 0, "path"), "", id="blank-repository"),
        pytest.param(
            ("bindings", 0, "repos", 0, "runner", "image"), "runner:latest", id="mutable-runner"
        ),
        pytest.param(("turns",), [], id="empty-declared-turns"),
        pytest.param(("turns",), {}, id="turns-not-list"),
        pytest.param(("turns", 0, "repository"), "acme-corp/unbound", id="unbound-turn"),
        pytest.param(("turns", 1, "repository"), REPO_X, id="no-repository-switch"),
        pytest.param(("turns", 1, "thread"), "other-thread", id="no-shared-success-thread"),
        pytest.param(("turns", 0, "text"), "", id="blank-trigger-text"),
        pytest.param(("turns", 0, "expect"), "failed", id="unknown-expectation"),
        pytest.param(("turns", 2, "expect"), "completed", id="missing-pool-refusal-not-planned"),
        pytest.param(
            ("turns", 3, "expect"), "repo_runner_pool_missing", id="stale-base-refusal-not-planned"
        ),
        pytest.param(("turns", 2, "thread"), "shared-thread", id="refusal-reuses-success-thread"),
        pytest.param(
            ("turns", 3, "thread"), "missing-pool-thread", id="refusals-share-thread"
        ),
        pytest.param(("turns", 2, "worker_pools"), {}, id="empty-pool-fault"),
        pytest.param(("turns", 3, "worker_pools"), {}, id="empty-base-fault"),
    ],
)
def test_invalid_plan_is_refused_before_execution(
    path: tuple[Any, ...], value: Any, tmp_path: Path
) -> None:
    plan = _plan()
    _set(plan, path, value)
    with pytest.raises(fe.ConfigError):
        repo_runner.load_plan(_plan_env(tmp_path, plan))


def test_success_turn_cannot_override_worker_pools(tmp_path: Path) -> None:
    plan = _plan()
    plan["turns"][0]["worker_pools"] = copy.deepcopy(plan["turns"][2]["worker_pools"])
    with pytest.raises(fe.ConfigError):
        repo_runner.load_plan(_plan_env(tmp_path, plan))


def test_main_refuses_a_bad_plan_before_any_external_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    for name, value in _plan_env(tmp_path, {}).items():
        monkeypatch.setenv(name, value)

    def forbid_command(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("a malformed plan must launch no external command")

    monkeypatch.setattr(subprocess, "run", forbid_command)
    monkeypatch.setattr(subprocess, "Popen", forbid_command)
    assert fe.main(["run", "--scenario", "repo-runner"]) == fe.EXIT_CONFIG
    assert "plan" in capsys.readouterr().err.lower()


@pytest.mark.parametrize("external_model", ["flag", "api-key", "base-url"])
def test_repo_runner_refuses_external_model_inputs_before_any_command(
    external_model: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    for name, value in _plan_env(tmp_path, _plan()).items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("CURIE_FACTORY_MODEL_API_KEY", raising=False)
    monkeypatch.delenv("CURIE_FACTORY_MODEL_BASE_URL", raising=False)
    argv = ["run", "--scenario", "repo-runner"]
    if external_model == "flag":
        argv.extend(["--model-base-url", "https://model.example.com"])
    elif external_model == "api-key":
        monkeypatch.setenv("CURIE_FACTORY_MODEL_API_KEY", "fixture-model-key")
    else:
        monkeypatch.setenv("CURIE_FACTORY_MODEL_BASE_URL", "https://model.example.com")

    def forbid_command(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("an external model input must launch no command")

    monkeypatch.setattr(subprocess, "run", forbid_command)
    monkeypatch.setattr(subprocess, "Popen", forbid_command)
    assert fe.main(argv) == fe.EXIT_CONFIG
    assert "model" in capsys.readouterr().err.lower()


# Anthropic documents tools/input_schema, assistant tool_use, matching user
# tool_result, and non-streaming Messages responses here:
# https://platform.claude.com/docs/en/agents-and-tools/tool-use/implement-tool-use
def _model_request(model: Any, body: dict[str, Any]) -> tuple[int, Any]:
    request = urllib.request.Request(
        f"http://127.0.0.1:{model.port}/v1/messages",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


def _first_model_body() -> dict[str, Any]:
    return {
        "model": "fixture-model",
        "max_tokens": 256,
        "stream": False,
        "tools": [
            {"name": "Bash", "description": "shell", "input_schema": {"type": "object"}}
        ],
        "messages": [{"role": "user", "content": "Read manifest"}],
    }


def _result_model_body(first: dict[str, Any], response: dict[str, Any]) -> dict[str, Any]:
    body = copy.deepcopy(first)
    tool = next(item for item in response["content"] if item["type"] == "tool_use")
    assert tool["name"] == "Bash"
    assert tool["input"]["command"] == repo_runner.MANIFEST_COMMAND
    body["messages"].extend(
        [
            {"role": "assistant", "content": response["content"]},
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": tool["id"],
                        "content": _passing()["turns"][0]["manifest_output"],
                    }
                ],
            },
        ]
    )
    return body


def test_scripted_model_requests_the_manifest_and_records_only_its_matching_tool_result() -> None:
    model = repo_runner.ScriptedManifestModel(host="127.0.0.1", port=0)
    model.start()
    try:
        model.begin_turn("fixture-turn")
        first = _first_model_body()
        status, response = _model_request(model, first)
        assert status == 200
        result = _result_model_body(first, response)
        status, final = _model_request(model, result)
        assert status == 200
        assert final["stop_reason"] == "end_turn"
        snapshot = model.snapshot()
        assert snapshot["model_requests"] == 2
        assert snapshot["manifest_output"] == _passing()["turns"][0]["manifest_output"]
        assert snapshot["tool_use_id"] == result["messages"][-1]["content"][0]["tool_use_id"]
        assert snapshot["errors"] == []
    finally:
        assert model.close() == {"stopped": True}


def test_scripted_model_stream_emits_the_tool_command_and_terminates() -> None:
    model = repo_runner.ScriptedManifestModel(host="127.0.0.1", port=0)
    model.start()
    try:
        model.begin_turn("fixture-stream-turn")
        body = _first_model_body()
        body["stream"] = True
        request = urllib.request.Request(
            f"http://127.0.0.1:{model.port}/v1/messages",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            assert response.status == 200
            assert response.headers.get_content_type() == "text/event-stream"
            stream = response.read().decode()
        # The Messages stream ends with message_stop after its content and delta:
        # https://platform.claude.com/docs/en/build-with-claude/streaming
        events = [json.loads(line[6:]) for line in stream.splitlines() if line.startswith("data: ")]
        assert events[0]["type"] == "message_start"
        assert events[-1]["type"] == "message_stop"
        tool = next(
            event["content_block"] for event in events if event["type"] == "content_block_start"
        )
        assert tool["type"] == "tool_use"
        assert tool["name"] == "Bash"
        assert tool["id"] == model.snapshot()["tool_use_id"]
        chunks = [
            event["delta"]["partial_json"]
            for event in events
            if event["type"] == "content_block_delta"
            and event["delta"]["type"] == "input_json_delta"
        ]
        assert json.loads("".join(chunks))["command"] == repo_runner.MANIFEST_COMMAND
        assert any(event["type"] == "content_block_stop" for event in events)
        delta = next(event["delta"] for event in events if event["type"] == "message_delta")
        assert delta["stop_reason"] == "tool_use"
        assert model.snapshot()["errors"] == []
    finally:
        assert model.close() == {"stopped": True}


@pytest.mark.parametrize(
    "invalid_request",
    ["wrong-tool-id", "plain-text-manifest", "non-object-tool-input", "extra-call"],
)
def test_scripted_model_refuses_unmatched_text_and_unexpected_calls(invalid_request: str) -> None:
    model = repo_runner.ScriptedManifestModel(host="127.0.0.1", port=0)
    model.start()
    try:
        model.begin_turn("fixture-turn")
        first = _first_model_body()
        status, response = _model_request(model, first)
        assert status == 200
        result = _result_model_body(first, response)
        if invalid_request == "wrong-tool-id":
            result["messages"][-1]["content"][0]["tool_use_id"] = "unmatched-tool-id"
        elif invalid_request == "plain-text-manifest":
            result["messages"][-2]["content"] = [
                {"type": "text", "text": _passing()["turns"][0]["manifest_output"]}
            ]
            result["messages"][-1]["content"] = "That is the manifest"
        elif invalid_request == "non-object-tool-input":
            result["messages"][-2]["content"][0]["input"] = ["unexpected"]
        else:
            assert _model_request(model, result)[0] == 200
            result = first
        status, _ = _model_request(model, result)
        assert status == 422
        snapshot = model.snapshot()
        assert snapshot["errors"]
        if invalid_request != "extra-call":
            assert snapshot["manifest_output"] == ""
    finally:
        assert model.close() == {"stopped": True}


def test_manifest_probe_uses_the_fixed_image_manifest_path() -> None:
    assert repo_runner.MANIFEST_COMMAND == "cat /etc/curie/tool-manifest.json"


def test_scenario_values_copy_inputs_and_preserve_complete_chart_bindings() -> None:
    base = {
        "factory": {"enabled": True, "codeHosts": [{"name": "fixture"}]},
        "worker": {"extraEnv": [{"name": "FIXTURE_SETTING", "value": "value"}]},
    }
    bindings = _plan()["bindings"]
    original_base, original_bindings = copy.deepcopy(base), copy.deepcopy(bindings)

    values = repo_runner.scenario_values(base, bindings)

    assert values["factory"]["bindings"] == bindings
    assert values["factory"]["enabled"] is True
    assert values["factory"]["codeHosts"] == original_base["factory"]["codeHosts"]
    assert values["worker"] == original_base["worker"]
    assert (base, bindings) == (original_base, original_bindings)
    values["factory"]["bindings"][0]["repos"][0]["runner"]["tools"].append("pnpm")
    values["worker"]["extraEnv"][0]["value"] = "changed"
    assert (base, bindings) == (original_base, original_bindings)


def test_scenario_values_add_factory_to_a_base_without_it() -> None:
    bindings = _plan()["bindings"]
    assert repo_runner.scenario_values({}, bindings)["factory"]["bindings"] == bindings


@pytest.mark.parametrize(("sample", "passes"), [("passing.json", True), ("failing.json", False)])
def test_canonical_synthetic_samples(sample: str, passes: bool) -> None:
    observation = json.loads((FIXTURES / sample).read_text())["scenario"]
    assert "synthetic" in observation["provenance"]
    assert "not observed live evidence" in observation["provenance"]
    failures = repo_runner.judge_repo_runner(observation)
    assert isinstance(failures, list)
    assert all(isinstance(failure, str) and failure for failure in failures)
    assert (failures == []) is passes


@pytest.mark.parametrize("turn", [0, 1])
@pytest.mark.parametrize("digest_key", ["index", "linux/amd64", "linux/arm64"])
@pytest.mark.parametrize("identity_kind", ["docker-pullable", "containerd"])
def test_index_and_each_platform_digest_are_valid_runtime_image_identities(
    turn: int, digest_key: str, identity_kind: str
) -> None:
    observation = _passing()
    config = observation["bindings"][observation["turns"][turn]["repository"]]
    digest = config["image"].split("@", 1)[1] if digest_key == "index" else config[
        "platformDigests"
    ][digest_key]
    reference = config["image"].split("@", 1)[0]
    observation["turns"][turn]["image_id"] = (
        f"docker-pullable://{reference}@{digest}"
        if identity_kind == "docker-pullable"
        else f"containerd://{digest}"
    )
    assert repo_runner.judge_repo_runner(observation) == []


def test_manifest_tool_order_does_not_change_the_declared_tool_set() -> None:
    observation = _passing()
    manifest = _manifest(observation)
    manifest["tools"].reverse()
    observation["turns"][0]["manifest_output"] = json.dumps(manifest)
    assert repo_runner.judge_repo_runner(observation) == []


def test_a_declared_catalog_tool_and_an_additional_probe_can_share_the_manifest() -> None:
    observation = _passing()
    observation["bindings"][REPO_X]["probes"] = [
        {"name": "acme-check", "command": ["acme-check", "--version"]}
    ]
    manifest = _manifest(observation)
    manifest["tools"].append(
        {"name": "acme-check", "version": "1.0", "path": "/opt/bin/acme-check", "source": "probe"}
    )
    observation["turns"][0]["manifest_output"] = json.dumps(manifest)
    observation["turns"][0]["boot_log"] = "tool manifest loaded: 3 tools\n"
    assert repo_runner.judge_repo_runner(observation) == []


def test_manifest_must_include_a_binding_declared_probe() -> None:
    observation = _passing()
    observation["bindings"][REPO_X]["probes"] = [
        {"name": "acme-check", "command": ["acme-check", "--version"]}
    ]
    assert repo_runner.judge_repo_runner(observation)


def test_boot_log_may_contain_other_startup_lines() -> None:
    observation = _passing()
    observation["turns"][0]["boot_log"] = (
        "runner starting\n" + observation["turns"][0]["boot_log"] + "runner ready\n"
    )
    assert repo_runner.judge_repo_runner(observation) == []


@pytest.mark.parametrize("docker_cli", [True, False])
def test_manifest_docker_cli_flag_matches_the_declared_docker_tool(docker_cli: bool) -> None:
    observation = _passing()
    observation["bindings"][REPO_X]["tools"].append("docker-cli")
    manifest = _manifest(observation)
    manifest["tools"].append(
        {"name": "docker-cli", "version": "28.0.0", "path": "/usr/bin/docker", "source": "catalog"}
    )
    manifest["docker_cli"] = docker_cli
    observation["turns"][0]["manifest_output"] = json.dumps(manifest)
    observation["turns"][0]["boot_log"] = "tool manifest loaded: 3 tools\n"
    assert (repo_runner.judge_repo_runner(observation) == []) is docker_cli


@pytest.mark.parametrize("observation", [None, [], "", 1, True, {}, {"bindings": {}, "turns": []}])
def test_missing_or_non_mapping_observation_fails_closed(observation: Any) -> None:
    assert repo_runner.judge_repo_runner(observation)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        pytest.param(("bindings",), {}, id="empty-bindings"),
        pytest.param(("bindings",), [], id="non-mapping-bindings"),
        pytest.param(("turns",), [], id="empty-turns"),
        pytest.param(("turns",), {}, id="non-list-turns"),
        pytest.param(("bindings", REPO_X), None, id="missing-runner-config"),
        pytest.param(("bindings", REPO_X, "tools"), [], id="empty-declared-tools"),
        pytest.param(("bindings", REPO_X, "tools"), "uv", id="non-list-declared-tools"),
        pytest.param(("bindings", REPO_X, "tools"), [["uv"]], id="list-declared-tool-name"),
        pytest.param(
            ("bindings", REPO_X, "tools"), [{"name": "uv"}], id="object-declared-tool-name"
        ),
        pytest.param(("bindings", REPO_X, "tools"), ["uv", "uv"], id="duplicate-declared-tools"),
        pytest.param(("bindings", REPO_X, "tools"), ["made-up-tool"], id="unknown-catalog-tool"),
        pytest.param(("bindings", REPO_X, "image"), "runner:latest", id="mutable-image"),
        pytest.param(("bindings", REPO_X, "image"), "runner@sha256:abc", id="short-index-digest"),
        pytest.param(("bindings", REPO_X, "base"), "sha256:abc", id="malformed-base-digest"),
        pytest.param(("bindings", REPO_X, "platformDigests"), {}, id="empty-platform-digests"),
        pytest.param(
            ("bindings", REPO_X, "platformDigests", "linux/amd64"),
            "sha256:abc",
            id="malformed-platform-digest",
        ),
        pytest.param(("turns", 0), None, id="non-mapping-turn"),
        pytest.param(("turns", 0, "repository"), "acme-corp/unbound", id="unbound-repository"),
        pytest.param(("turns", 0, "thread"), "", id="empty-thread"),
        pytest.param(("turns", 0, "sandbox"), "", id="empty-sandbox"),
        pytest.param(("turns", 0, "pod_uid"), "", id="empty-pod-uid"),
        pytest.param(("turns", 0, "image_id"), "", id="empty-image-id"),
        pytest.param(("turns", 0, "spec_image"), "", id="empty-pod-spec-image"),
        pytest.param(("turns", 0, "spec_image"), "runner:latest", id="mutable-pod-spec-image"),
        pytest.param(
            ("turns", 0, "spec_image"),
            "registry.example.com/wrong-runner@sha256:" + "9" * 64,
            id="wrong-pod-spec-image",
        ),
        pytest.param(("turns", 0, "claim"), "", id="empty-claim"),
        pytest.param(("turns", 1, "claim"), "fixture-claim-x", id="claim-reused-across-repos"),
        pytest.param(("turns", 1, "retired_claims"), [], id="prior-claim-not-retired"),
        pytest.param(
            ("turns", 1, "retired_claims"), ["unrelated-claim"], id="unrelated-claim-retired"
        ),
        pytest.param(("turns", 0, "image_id"), "containerd://sha256:abc", id="short-image-digest"),
        pytest.param(
            ("turns", 0, "image_id"),
            "containerd://sha256:" + "9" * 64,
            id="unrelated-image-digest",
        ),
        pytest.param(
            ("turns", 0, "image_id"),
            "containerd://sha256:" + "b" * 64 + "unrelated",
            id="digest-prefix-is-not-exact-identity",
        ),
        pytest.param(("turns", 0, "boot_log"), "", id="absent-boot-log"),
        pytest.param(("turns", 0, "boot_log"), "runner ready", id="no-manifest-boot-line"),
        pytest.param(
            ("turns", 0, "boot_log"), "tool manifest loaded: 1 tools\n", id="wrong-boot-count"
        ),
        pytest.param(("turns", 0, "manifest_output"), "", id="empty-manifest-output"),
        pytest.param(("turns", 0, "manifest_output"), "{", id="invalid-manifest-json"),
        pytest.param(("turns", 0, "manifest_output"), "null", id="null-manifest"),
        pytest.param(("turns", 0, "manifest_output"), "[]", id="non-object-manifest"),
        pytest.param(("turns", 0, "status"), "running", id="nonterminal-turn"),
        pytest.param(("turns", 0, "status"), "failed", id="failed-turn"),
        pytest.param(("turns", 0, "cli_exit_code"), 1, id="failed-cli-exit"),
        pytest.param(("turns", 0, "runner_status", "status"), "running", id="runner-not-done"),
        pytest.param(("turns", 0, "runner_status", "turn_active"), True, id="runner-turn-active"),
        pytest.param(("turns", 0, "observed_repository"), REPO_Y, id="wrong-claim-repository"),
        pytest.param(("turns", 0, "capture_error"), "ambiguous claim", id="capture-failed"),
        pytest.param(("turns", 0, "model_requests"), 0, id="no-model-requests"),
        pytest.param(("turns", 0, "model_requests"), 1, id="only-one-model-request"),
        pytest.param(("turns", 0, "model_requests"), "2", id="noninteger-model-count"),
        pytest.param(("turns", 0, "model_requests"), True, id="boolean-model-count"),
        pytest.param(("turns", 1, "thread"), "other-thread", id="repository-change-outside-thread"),
        pytest.param(
            ("turns", 1, "sandbox"), "fixture-sandbox-x", id="sandbox-reused-across-repos"
        ),
        pytest.param(
            ("turns", 1, "pod_uid"),
            "00000000-0000-4000-8000-000000000001",
            id="pod-reused-across-repos",
        ),
    ],
)
def test_each_observation_shortfall_fails_closed(path: tuple[Any, ...], value: Any) -> None:
    observation = _passing()
    _set(observation, path, value)
    assert repo_runner.judge_repo_runner(observation), path


@pytest.mark.parametrize(
    "path",
    [
        ("bindings",),
        ("turns",),
        ("bindings", REPO_Y),
        ("bindings", REPO_X, "tools"),
        ("bindings", REPO_X, "image"),
        ("bindings", REPO_X, "base"),
        ("bindings", REPO_X, "platformDigests"),
        ("bindings", REPO_X, "platformDigests", "linux/arm64"),
        ("turns", 0, "repository"),
        ("turns", 0, "thread"),
        ("turns", 0, "sandbox"),
        ("turns", 0, "pod_uid"),
        ("turns", 0, "image_id"),
        ("turns", 0, "spec_image"),
        ("turns", 0, "claim"),
        ("turns", 1, "retired_claims"),
        ("turns", 0, "boot_log"),
        ("turns", 0, "manifest_output"),
        ("turns", 0, "status"),
        ("turns", 0, "cli_exit_code"),
        ("turns", 0, "runner_status"),
        ("turns", 0, "observed_repository"),
        ("turns", 0, "capture_error"),
        ("turns", 0, "model_requests"),
    ],
)
def test_each_required_observation_field_is_required(path: tuple[Any, ...]) -> None:
    observation = _passing()
    del _node(observation, path[:-1])[path[-1]]
    assert repo_runner.judge_repo_runner(observation), path


@pytest.mark.parametrize(
    ("path", "value"),
    [
        pytest.param(("schema_version",), 2, id="wrong-schema-version"),
        pytest.param(("schema_version",), "1", id="string-schema-version"),
        pytest.param(("schema_version",), True, id="boolean-schema-version"),
        pytest.param(("tools",), [], id="manifest-missing-declared-tools"),
        pytest.param(("tools",), {}, id="manifest-tools-not-list"),
        pytest.param(("tools", 0), "uv", id="manifest-tool-not-object"),
        pytest.param(("tools", 0, "name"), "pnpm", id="wrong-declared-tool"),
        pytest.param(("tools", 0, "name"), ["uv"], id="list-tool-name"),
        pytest.param(("tools", 0, "name"), {"name": "uv"}, id="object-tool-name"),
        pytest.param(("tools", 0, "version"), "", id="blank-tool-version"),
        pytest.param(("tools", 0, "path"), "", id="blank-tool-path"),
        pytest.param(("tools", 0, "source"), "other", id="unknown-tool-source"),
        pytest.param(("tools", 0, "source"), "probe", id="declared-catalog-tool-is-only-probe"),
        pytest.param(("docker_cli",), "false", id="docker-cli-not-boolean"),
    ],
)
def test_each_manifest_shortfall_fails_closed(path: tuple[Any, ...], value: Any) -> None:
    observation = _passing()
    manifest = _manifest(observation)
    _set(manifest, path, value)
    observation["turns"][0]["manifest_output"] = json.dumps(manifest)
    assert repo_runner.judge_repo_runner(observation), path


@pytest.mark.parametrize(
    "path", [("schema_version",), ("tools",), ("docker_cli",)]
    + [("tools", 0, field) for field in ("name", "version", "path", "source")],
)
def test_each_manifest_field_is_required(path: tuple[Any, ...]) -> None:
    observation = _passing()
    manifest = _manifest(observation)
    del _node(manifest, path[:-1])[path[-1]]
    observation["turns"][0]["manifest_output"] = json.dumps(manifest)
    assert repo_runner.judge_repo_runner(observation), path


@pytest.mark.parametrize("extra_tool", ["catalog", "duplicate"])
def test_manifest_cannot_add_an_undeclared_catalog_tool_or_duplicate(extra_tool: str) -> None:
    observation = _passing()
    manifest = _manifest(observation)
    tool = copy.deepcopy(manifest["tools"][0])
    if extra_tool == "catalog":
        tool["name"] = "pnpm"
    manifest["tools"].append(tool)
    observation["turns"][0]["manifest_output"] = json.dumps(manifest)
    observation["turns"][0]["boot_log"] = "tool manifest loaded: 3 tools\n"
    assert repo_runner.judge_repo_runner(observation)


@pytest.mark.parametrize("turn", [0, 1, 2, 3])
def test_each_success_and_refusal_case_is_required(turn: int) -> None:
    observation = _passing()
    del observation["turns"][turn]
    assert repo_runner.judge_repo_runner(observation)


@pytest.mark.parametrize("turn", [0, 1, 2, 3])
@pytest.mark.parametrize("errors", [None, ["unexpected model request"]])
def test_every_turn_requires_explicit_empty_error_evidence(turn: int, errors: Any) -> None:
    observation = _passing()
    if errors is None:
        del observation["turns"][turn]["errors"]
    else:
        observation["turns"][turn]["errors"] = errors
    assert repo_runner.judge_repo_runner(observation)


@pytest.mark.parametrize("turn", [2, 3], ids=["missing-pool", "stale-base"])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("status", "completed", id="completion-does-not-prove-refusal"),
        pytest.param("reason", "some_other_refusal", id="wrong-refusal-reason"),
        pytest.param("reason", "", id="absent-refusal-reason"),
        pytest.param("sandbox", "fallback-agent-sandbox", id="refusal-created-sandbox"),
        pytest.param("claim", "fallback-agent-claim", id="refusal-created-claim-id"),
        pytest.param("pod_uid", "fallback-pod", id="refusal-created-pod"),
        pytest.param(
            "image_id", "containerd://sha256:" + "e" * 64, id="refusal-used-agent-image"
        ),
        pytest.param("model_requests", 1, id="refusal-contacted-model"),
        pytest.param("model_requests", False, id="refusal-model-count-not-integer"),
        pytest.param("new_claims", ["fallback-agent-sandbox"], id="refusal-created-claim"),
        pytest.param("new_claims", None, id="refusal-claim-evidence-unavailable"),
    ],
)
def test_each_refusal_must_precede_any_model_request_or_sandbox(
    turn: int, field: str, value: Any
) -> None:
    observation = _passing()
    observation["turns"][turn][field] = value
    assert repo_runner.judge_repo_runner(observation), (turn, field)


@pytest.mark.parametrize("turn", [2, 3], ids=["missing-pool", "stale-base"])
@pytest.mark.parametrize("field", ["status", "reason", "model_requests", "new_claims"])
def test_refusal_requires_explicit_terminal_and_no_side_effect_evidence(
    turn: int, field: str
) -> None:
    observation = _passing()
    del observation["turns"][turn][field]
    assert repo_runner.judge_repo_runner(observation), (turn, field)


@pytest.mark.parametrize(
    ("turn", "field", "value"),
    [
        (2, "missing_pool_absent", False),
        (3, "installed_base", ""),
        (3, "configured_base", "sha256:" + "e" * 64),
    ],
)
def test_each_refusal_requires_observed_fault_evidence(turn: int, field: str, value: Any) -> None:
    observation = _passing()
    observation["turns"][turn][field] = value
    assert repo_runner.judge_repo_runner(observation)


@pytest.mark.parametrize(
    ("turn", "field"), [(2, "missing_pool_absent"), (3, "installed_base"), (3, "configured_base")]
)
def test_each_refusal_fault_field_is_required(turn: int, field: str) -> None:
    observation = _passing()
    del observation["turns"][turn][field]
    assert repo_runner.judge_repo_runner(observation)


@pytest.mark.parametrize("turn", [2, 3], ids=["missing-pool", "stale-base"])
def test_refusal_repository_must_have_a_completed_success(turn: int) -> None:
    observation = _passing()
    observation["turns"][turn]["repository"] = "acme-corp/unobserved-repository"
    assert repo_runner.judge_repo_runner(observation)


@pytest.mark.parametrize(
    ("turn", "other"), [(2, 0), (3, 2)], ids=["success-thread", "other-refusal-thread"]
)
def test_each_refusal_uses_a_fresh_thread(turn: int, other: int) -> None:
    observation = _passing()
    observation["turns"][turn]["thread"] = observation["turns"][other]["thread"]
    assert repo_runner.judge_repo_runner(observation)


def test_workspace_selection_conflict_failure_preserves_its_diagnostic_code() -> None:
    observation = _passing()
    observation["turns"][0].update(status="failed", reason="workspace.selection_conflict")
    failures = repo_runner.judge_repo_runner(observation)
    assert any("workspace.selection_conflict" in failure for failure in failures), failures


def test_preflight_release_assignment_selects_the_publication_namespace(tmp_path: Path) -> None:
    config = fe.FactoryConfig(
        kube_context="k8",
        app_id="1",
        installation_id=1,
        private_key_file=tmp_path / "fixture.pem",
        repo=REPO_X,
        label="fixture",
        mention="fixture",
        cloudflared="cloudflared",
        priority_classes=None,
        restore_webhook_url=None,
        webhook_secret="fixture-secret",
        actor_token="fixture-token",
    )
    preflight = fe.Preflight(
        config,
        repo_root=ROOT.parents[1],
        candidate="c" * 40,
        namespace="test-factory-tools",
        evidence_path=tmp_path / "evidence.json",
        admission_timeout=1,
    )
    try:
        assert preflight.release == fe.release_name(preflight.namespace)
        assert preflight.publication_namespace() == (
            f"test-factory-tools-{preflight.release}-publication"
        )
        preflight.release = "factory-tools"
        assert preflight.publication_namespace() == "test-factory-tools-factory-tools-publication"
    finally:
        shutil.rmtree(preflight.workdir)


def test_repo_runner_is_named_by_the_factory_parser_and_its_help(
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = fe.parse_args(["run", "--scenario", "repo-runner"])
    assert args.mode == "run" and args.scenario == "repo-runner"
    assert fe.resolve_scenario("repo-runner") is repo_runner.drive
    with pytest.raises(SystemExit) as exit_info:
        fe.parse_args(["run", "--help"])
    assert exit_info.value.code == 0
    assert "repo-runner" in capsys.readouterr().out


def test_public_judge_parser_takes_an_observation_path() -> None:
    sample = FIXTURES / "failing.json"
    args = fe.parse_args(["judge-repo-runner", "--observation", str(sample)])
    assert args.mode == "judge-repo-runner"
    assert args.observation == sample


def test_public_judge_requires_an_observation_path() -> None:
    with pytest.raises(SystemExit) as exit_info:
        fe.parse_args(["judge-repo-runner"])
    assert exit_info.value.code != 0


@pytest.mark.parametrize(("sample", "code"), [("passing.json", 0), ("failing.json", 1)])
def test_standalone_judge_main_returns_the_sample_verdict(sample: str, code: int) -> None:
    assert repo_runner.main(["judge", str(FIXTURES / sample)]) == code


@pytest.mark.parametrize("contents", ["{", "null", "[]", "{}"])
def test_standalone_judge_fails_closed_on_unreadable_observation(
    contents: str, tmp_path: Path
) -> None:
    observation = tmp_path / "observation.json"
    observation.write_text(contents)
    assert repo_runner.main(["judge", str(observation)]) == 1


@pytest.mark.parametrize("scenario", [None, [], "observation"])
def test_standalone_judge_requires_an_observation_mapping_inside_the_envelope(
    scenario: Any, tmp_path: Path
) -> None:
    path = tmp_path / "evidence.json"
    path.write_text(json.dumps({"scenario": scenario}))
    assert repo_runner.main(["judge", str(path)]) == 1


def test_standalone_judge_refuses_a_raw_observation_without_the_evidence_envelope(
    tmp_path: Path,
) -> None:
    path = tmp_path / "raw-observation.json"
    path.write_text(json.dumps(_passing()))
    assert repo_runner.main(["judge", str(path)]) == 1


@pytest.mark.parametrize(("sample", "code"), [("passing.json", 0), ("failing.json", 1)])
def test_real_judge_command_exits_with_the_sample_verdict(sample: str, code: int) -> None:
    result = subprocess.run(
        ["python3", str(ROOT / "repo_runner.py"), "judge", str(FIXTURES / sample)],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == code, result.stdout + result.stderr


@pytest.mark.parametrize(("sample", "code"), [("passing.json", 0), ("failing.json", 1)])
def test_public_factory_judge_command_exits_with_the_sample_verdict(sample: str, code: int) -> None:
    result = subprocess.run(
        [
            "python3",
            str(ROOT / "factory_e2e.py"),
            "judge-repo-runner",
            "--observation",
            str(FIXTURES / sample),
        ],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == code, result.stdout + result.stderr
