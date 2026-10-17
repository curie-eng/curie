"""Contracts for the repository runner scenario: plan, offline driver and judge.

The fixture samples describe synthetic recorded shapes. These tests prove the
plan loader, the driver's collection against faked external CLIs, and the
judge accept or refuse those shapes. They are not live evidence that any
component implements repository runner routing, a manifest, or a model
request.

The scenario has three bindings (two runner repositories X and Y, one
repository U without a runner) and seven turns: X completes, Y completes, U
runs on the agent image, Y on X's thread is refused with
workspace.selection_conflict, X on its first thread completes after a runner
change, then the missing-pool and stale-base faults are refused.
"""

from __future__ import annotations

import copy
import dataclasses
import hashlib
import itertools
import json
import shutil
import subprocess
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import factory_e2e as fe
import pytest
import repo_runner
from channel_protocol.identity import scoped_conversation_id
from curie_worker.repo_runner_pools import platform_runner_digest
from curie_worker.sandbox.types import RepoRunnerPoolRefused

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "fixtures" / "repo-runner"
REPO_X = "acme-corp/repo-x"
REPO_Y = "acme-corp/repo-y"
REPO_U = "acme-corp/repo-u"
CONTEXT = "k8-scratch"
THREAD_X = "thread-x"
THREAD_Y = "thread-y"
THREAD_U = "thread-u"
INSTALLED_BASE = "sha256:" + "e" * 64
AGENT_POOL = "factory-tools-agent-factory-e2e-runner-pool"
AGENT_TEMPLATE = "factory-tools-agent-factory-e2e"
AGENT_TAG = "registry.example.com/curie-runner:v0.13.0"
CONFLICT_REPLY = "This thread is already bound to a different repository."
TOKEN_NAME = "CLAUDE_CODE_MAX_OUTPUT_TOKENS"
# Claude Code appended these blocks to the manifest probe's Bash tool result
# in live run 4919 (the values are that run's budget readings).
REMINDERS = (
    "\n\n<system-reminder>\n<total_tokens>14999998 tokens left</total_tokens>\n"
    "</system-reminder>\n\n<system-reminder>\n"
    "USD budget: $0.000017999999999999997/$10; $9.999982 remaining\n</system-reminder>"
)


def _reply_outcome(thread: str, reply: str) -> dict[str, Any]:
    """`curie cluster message --json` for a delivered reply (cli/src/message.rs
    message_reply_json). The worker answers every refusal this way, with no
    turn-failure marker, so the CLI reports it finalized and not failed."""

    return {"reply": reply, "thread": thread, "finalized": True}


def _pool_refusal_reply(repository: str, pool: str, reason: str, base: str = "") -> str:
    """The kernel's one reply to a RepoRunnerPoolRefused, built from the worker's
    own exception (apps/worker/src/curie_worker/kernel/attempt.py)."""

    error = RepoRunnerPoolRefused(
        repo=repository,
        pool=pool,
        reason=reason,
        base=base or None,  # type: ignore[arg-type]
        platform_digest=INSTALLED_BASE,
    )
    return f"This agent cannot start: {error}. An operator has to make that change."


def _digest(character: str) -> str:
    return "sha256:" + character * 64


AGENT_PINNED = "registry.example.com/curie-runner@" + _digest("2")
AGENT_DIGESTS = [_digest("2"), _digest("3"), _digest("4")]


# --------------------------------------------------------------------------
# Synthetic sample and plan builders
# --------------------------------------------------------------------------


def _passing() -> dict[str, Any]:
    return json.loads((FIXTURES / "passing.json").read_text())["scenario"]


def _node(observation: Any, path: tuple[Any, ...]) -> Any:
    node: Any = observation
    for part in path:
        node = node[part]
    return node


def _set(observation: Any, path: tuple[Any, ...], value: Any) -> None:
    _node(observation, path[:-1])[path[-1]] = value


def _manifest(observation: dict[str, Any], turn: int = 0) -> dict[str, Any]:
    return json.loads(observation["turns"][turn]["manifest_output"])


def _mentions(failures: list[str], *needles: str) -> bool:
    return any(needle.lower() in failure.lower() for failure in failures for needle in needles)


def _runner_configs() -> dict[str, dict[str, Any]]:
    return _passing()["bindings"]


def _pool_entry(config: dict[str, Any]) -> dict[str, Any]:
    """The chart's rendered entry: name from the first 12 index hex digits."""

    index = config["image"].split("@", 1)[1]
    return {
        "pool": f"factory-tools-agent-factory-e2e-img-{index[7:19]}-runner-pool",
        "digests": [
            index,
            config["platformDigests"]["linux/amd64"],
            config["platformDigests"]["linux/arm64"],
        ],
        "base": config["base"],
        "docker": False,
    }


def _binding(name: str, scope: str, alias: str, path: str, runner: Any) -> dict[str, Any]:
    repo: dict[str, Any] = {"alias": alias, "codeHost": "github", "projectId": scope, "path": path}
    if runner is not None:
        repo["runner"] = runner
        repo["docker"] = {"enabled": False}
    repo["bases"] = ["main"]
    repo["ci"] = {}
    return {"name": name, "tracker": "github", "trackerScopeId": scope, "repos": [repo]}


def _plan(configs: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    configs = copy.deepcopy(configs or _runner_configs())
    pools = {repository: _pool_entry(config) for repository, config in configs.items()}
    changed = {REPO_X: copy.deepcopy(pools[REPO_Y]), REPO_Y: copy.deepcopy(pools[REPO_Y])}
    missing_pool = copy.deepcopy(pools)
    missing_pool[REPO_X]["pool"] = "absent-fixture-x"
    stale_base = copy.deepcopy(pools)
    stale_base[REPO_X]["base"] = _digest("f")
    return {
        "bindings": [
            _binding("fixture-x", "101", "x", REPO_X, configs[REPO_X]),
            _binding("fixture-y", "102", "y", REPO_Y, configs[REPO_Y]),
            _binding("fixture-u", "103", "u", REPO_U, None),
        ],
        "turns": [
            {
                "repository": REPO_X,
                "thread": THREAD_X,
                "text": "Read manifest",
                "expect": "completed",
            },
            {
                "repository": REPO_Y,
                "thread": THREAD_Y,
                "text": "Read manifest",
                "expect": "completed",
            },
            {
                "repository": REPO_U,
                "thread": THREAD_U,
                "text": "Read manifest",
                "expect": "agent_image",
            },
            {
                "repository": REPO_Y,
                "thread": THREAD_X,
                "text": "Read manifest",
                "expect": "workspace.selection_conflict",
            },
            {
                "repository": REPO_X,
                "thread": THREAD_X,
                "text": "Read manifest",
                "expect": "runner_changed",
                "worker_pools": changed,
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


def _plan_repo(plan: dict[str, Any], path: str) -> dict[str, Any]:
    return next(
        repo for binding in plan["bindings"] for repo in binding["repos"] if repo["path"] == path
    )


def _plan_env(tmp_path: Path, plan: Any) -> dict[str, str]:
    path = tmp_path / "repo-runner-plan.json"
    path.write_text(json.dumps(plan))
    return {"CURIE_FACTORY_REPO_RUNNER_PLAN": str(path)}


def _load(tmp_path: Path, plan: Any) -> dict[str, Any]:
    return repo_runner.load_plan(_plan_env(tmp_path, plan))


def _refused(tmp_path: Path, plan: Any) -> None:
    with pytest.raises(fe.ConfigError):
        _load(tmp_path, plan)


def _config_env(tmp_path: Path) -> dict[str, str]:
    key = tmp_path / "fixture-private-key.pem"
    key.write_text("fixture private key; no cryptographic operation in config loading")
    secret = tmp_path / "fixture-webhook-secret"
    secret.write_text("fixture-secret")
    return {
        **_plan_env(tmp_path, _plan()),
        "CURIE_FACTORY_KUBE_CONTEXT": CONTEXT,
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


# --------------------------------------------------------------------------
# Module contract constants
# --------------------------------------------------------------------------


def test_module_contract_constants() -> None:
    assert repo_runner.MANIFEST_COMMAND == (
        "cat /etc/curie/tool-manifest.json 2>/dev/null || echo curie-no-tool-manifest"
    )
    assert repo_runner.NO_MANIFEST_OUTPUT == "curie-no-tool-manifest"
    # runner/src/curie_runner/__main__.py logs exactly this when no manifest exists.
    assert repo_runner.MANIFEST_ABSENT_LINE == "runner tool manifest absent: tool_count=0"
    assert repo_runner.RUNNER_ENV == {TOKEN_NAME: "16000"}
    assert repo_runner.SELECTION_CONFLICT == "workspace.selection_conflict"
    assert repo_runner.FAULT_REASONS == frozenset(
        {"repo_runner_pool_missing", "repo_runner_base_mismatch"}
    )
    assert repo_runner.REFUSAL_REASONS == (
        repo_runner.FAULT_REASONS | {repo_runner.SELECTION_CONFLICT}
    )
    assert repo_runner.TEST_RELEASE == "factory-tools"


# --------------------------------------------------------------------------
# Decision 10: explicit context
# --------------------------------------------------------------------------

CONTEXT_REFUSAL = (
    "repo-runner requires an explicit --context (the throwaway cluster, for example k8-scratch)"
)


@pytest.mark.parametrize("context", [None, "", "   "])
def test_config_requires_an_explicit_context_even_with_the_environment_set(
    context: str | None, tmp_path: Path
) -> None:
    env = _config_env(tmp_path)
    assert env["CURIE_FACTORY_KUBE_CONTEXT"] == CONTEXT
    with pytest.raises(fe.ConfigError) as refused:
        repo_runner.load_repo_runner_config(env, context=context)
    assert str(refused.value) == CONTEXT_REFUSAL


def test_config_accepts_an_explicit_context_and_owned_scripted_model(tmp_path: Path) -> None:
    env = _config_env(tmp_path)
    del env["CURIE_FACTORY_KUBE_CONTEXT"]
    config = repo_runner.load_repo_runner_config(env, context=CONTEXT)
    assert config.kube_context == CONTEXT
    assert config.model_api_key == repo_runner.SCRIPTED_KEY
    assert config.model_base_url is None
    assert config.repo == REPO_X


@pytest.mark.parametrize("host", ["127.0.0.1", "0.0.0.0", "not-an-address"])
def test_config_refuses_unreachable_callback_hosts(host: str, tmp_path: Path) -> None:
    env = _config_env(tmp_path)
    env["CURIE_LOCAL_STUB_HOST"] = host
    with pytest.raises(fe.ConfigError) as refused:
        repo_runner.load_repo_runner_config(env, context=CONTEXT)
    assert "CURIE_LOCAL_STUB_HOST" in str(refused.value)
    if host != "not-an-address":
        assert "reachable from the cluster" in str(refused.value)


def test_factory_main_refuses_repo_runner_without_context_before_any_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    for name, value in _config_env(tmp_path).items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("CURIE_FACTORY_MODEL_API_KEY", raising=False)
    monkeypatch.delenv("CURIE_FACTORY_MODEL_BASE_URL", raising=False)

    def forbid_command(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("a missing context must launch no external command")

    monkeypatch.setattr(subprocess, "run", forbid_command)
    monkeypatch.setattr(subprocess, "Popen", forbid_command)
    assert fe.main(["run", "--scenario", "repo-runner"]) == fe.EXIT_CONFIG
    assert "--context" in capsys.readouterr().err


# --------------------------------------------------------------------------
# Offline Preflight and external CLI fakes
# --------------------------------------------------------------------------


def _preflight_config(tmp_path: Path, context: str = CONTEXT) -> fe.FactoryConfig:
    return fe.FactoryConfig(
        kube_context=context,
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


def _make_preflight(tmp_path: Path, context: str = CONTEXT) -> fe.Preflight:
    value = fe.Preflight(
        _preflight_config(tmp_path, context),
        repo_root=ROOT.parents[1],
        candidate="c" * 40,
        namespace="test-factory-tools",
        evidence_path=tmp_path / "evidence.json",
        admission_timeout=1,
        scenario_name="repo-runner",
    )
    value.chart_dir = ROOT.parents[1] / "charts" / "curie"
    value.api_key = "fixture-api-key"
    return value


@pytest.fixture
def preflight(tmp_path: Path) -> Iterator[fe.Preflight]:
    value = _make_preflight(tmp_path)
    try:
        yield value
    finally:
        shutil.rmtree(value.workdir)


def _profiles() -> dict[str, dict[str, Any]]:
    """Runtime identities the fake cluster hands out, taken from the passing sample."""

    turns = _passing()["turns"]
    return {"x": turns[0], "y": turns[1], "u": turns[2], "x-changed": turns[4]}


def _thread_hash(channel: str, thread: str) -> str:
    return hashlib.sha256(scoped_conversation_id("slack", channel, thread).encode()).hexdigest()[
        :10
    ]


def _claim(name: str, repository: str, sandbox: str, pool: str, thread_hash: str) -> dict[str, Any]:
    """A SandboxClaim as kubectl returns it (spec.warmPoolRef.name, status.sandbox.name)."""

    return {
        "apiVersion": "extensions.agents.x-k8s.io/v1alpha1",
        "kind": "SandboxClaim",
        "metadata": {
            "name": name,
            "namespace": "test-factory-tools",
            "labels": {
                "curietech.ai/thread-hash": thread_hash,
                "curietech.ai/agent": fe.FACTORY_AGENT,
            },
        },
        "spec": {
            "warmPoolRef": {"name": pool},
            "env": [{"name": "CURIE_REPO_PATH", "value": repository}],
        },
        "status": {"sandbox": {"name": sandbox}},
    }


# The fake below replays `docker buildx imagetools inspect` in its two
# documented output forms: the default text form reports the index's
# `Digest:` before its platform manifests, and `--raw` prints the original
# OCI image index JSON, whose `manifests[]` carry `digest` and `platform`.
# BuildKit attestation manifests appear there with platform os/architecture
# "unknown" and must not count as runnable platform images.
# https://docs.docker.com/reference/cli/docker/buildx/imagetools/inspect/
# https://github.com/opencontainers/image-spec/blob/main/image-index.md
AGENT_INDEX = {
    "schemaVersion": 2,
    "mediaType": "application/vnd.oci.image.index.v1+json",
    "manifests": [
        {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": _digest("3"),
            "size": 1612,
            "platform": {"architecture": "amd64", "os": "linux"},
        },
        {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": _digest("4"),
            "size": 1612,
            "platform": {"architecture": "arm64", "os": "linux"},
        },
        {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": _digest("5"),
            "size": 566,
            "annotations": {
                "vnd.docker.reference.digest": _digest("3"),
                "vnd.docker.reference.type": "attestation-manifest",
            },
            "platform": {"architecture": "unknown", "os": "unknown"},
        },
        {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": _digest("6"),
            "size": 566,
            "annotations": {
                "vnd.docker.reference.digest": _digest("4"),
                "vnd.docker.reference.type": "attestation-manifest",
            },
            "platform": {"architecture": "unknown", "os": "unknown"},
        },
    ],
}


def _inspect_text(reference: str, *, with_digest: bool = True) -> str:
    lines = [
        f"Name:      {reference}",
        "MediaType: application/vnd.oci.image.index.v1+json",
    ]
    if with_digest:
        lines.append(f"Digest:    {_digest('2')}")
    lines += [
        "",
        "Manifests:",
        f"  Name:        {reference}@{_digest('3')}",
        "  MediaType:   application/vnd.oci.image.manifest.v1+json",
        "  Platform:    linux/amd64",
        "",
        f"  Name:        {reference}@{_digest('4')}",
        "  MediaType:   application/vnd.oci.image.manifest.v1+json",
        "  Platform:    linux/arm64",
    ]
    return "\n".join(lines) + "\n"


class _KubernetesCli:
    """Synthetic external CLI replies; all product collector methods stay real.

    It models the platform facts the driver observes: a thread binds to the
    first repository that runs on it, a repository's pod follows its current
    worker pool entry, and a replaced claim is retired after the next turn.
    """

    def __init__(self, p: fe.Preflight, channel: str) -> None:
        self.p = p
        self.channel = channel
        self.calls: list[list[str]] = []
        self.docker_calls: list[list[str]] = []
        self.patches: list[list[dict[str, Any]]] = []
        self.messages: list[list[str]] = []
        self.live_claims: list[dict[str, Any]] = []
        self.thread_repository: dict[str, str] = {}
        self.retiring: dict[str, int] = {}
        self.retirement_queries: list[str] = []
        self.retirement_stays = False
        self.probes = [{"status": "done", "turn_active": False}]
        self.probe_calls = 0
        self.cas_conflict = False
        self.forced_outcome: dict[str, Any] | None = None
        self.post_exit_claim: str | None = None
        self.current: dict[str, Any] | None = None
        self.exited = False
        self.runner_env: list[dict[str, Any]] = [
            {"name": TOKEN_NAME, "value": "16000"},
            {
                "name": "ANTHROPIC_API_KEY",
                "valueFrom": {"secretKeyRef": {"name": "factory-tools-model", "key": "api-key"}},
            },
        ]
        self.agent_template_image = AGENT_PINNED
        self.inspect_has_digest = True
        self.warm_pool_reads: list[str] = []
        pools = copy.deepcopy(_passing()["worker_pools"])
        self.deployment = {
            "metadata": {"resourceVersion": "1"},
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {"name": "sidecar", "env": [{"name": "UNCHANGED", "value": "sidecar"}]},
                            {
                                "name": "worker",
                                "env": [
                                    {"name": "UNCHANGED", "value": "worker"},
                                    {"name": "CURIE_REPO_RUNNER_POOLS", "value": json.dumps(pools)},
                                ],
                            },
                        ]
                    }
                }
            },
        }

    def pool_value(self) -> str:
        return str(self.deployment["spec"]["template"]["spec"]["containers"][1]["env"][1]["value"])

    def set_pools(self, pools: dict[str, Any]) -> None:
        self.deployment["spec"]["template"]["spec"]["containers"][1]["env"][1]["value"] = (
            json.dumps(pools)
        )

    def _profile_for_sandbox(self, sandbox: str) -> dict[str, Any]:
        return next(item for item in _profiles().values() if item["sandbox"] == sandbox)

    def start_message(self, argv: list[str]) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        """The platform's response to one `curie cluster message`."""

        self.messages.append(list(argv))
        text = argv[argv.index("message") + 1]
        thread = argv[argv.index("--thread") + 1]
        repository = next(
            repository
            for repository in (REPO_X, REPO_Y, REPO_U)
            if f"https://github.com/{repository}\n" in text
        )
        self.exited = False
        self.current = None
        success = _reply_outcome(thread, "Manifest captured.")
        profile: dict[str, Any] | None
        outcome: dict[str, Any]
        bound = self.thread_repository.get(thread)
        pools = json.loads(self.pool_value())
        baseline = _passing()["worker_pools"]
        if bound is not None and bound != repository:
            # apps/worker/src/curie_worker/workspace.py maps
            # workspace.selection_conflict to this exact public reply.
            profile, outcome = None, _reply_outcome(thread, CONFLICT_REPLY)
        elif repository == REPO_U:
            profile, outcome = _profiles()["u"], success
        elif pools[repository]["pool"].startswith("absent-"):
            reply = _pool_refusal_reply(repository, pools[repository]["pool"], "missing_pool")
            profile, outcome = None, _reply_outcome(thread, reply)
        elif pools[repository]["base"] != INSTALLED_BASE:
            reply = _pool_refusal_reply(
                repository, pools[repository]["pool"], "stale_base", pools[repository]["base"]
            )
            profile, outcome = None, _reply_outcome(thread, reply)
        elif pools[repository]["pool"] == baseline[REPO_X]["pool"]:
            profile, outcome = _profiles()["x"], success
        elif repository == REPO_Y:
            profile, outcome = _profiles()["y"], success
        else:
            profile, outcome = _profiles()["x-changed"], success
        if self.forced_outcome is not None:
            outcome = self.forced_outcome
            if outcome.get("failed"):
                profile = None
        if profile is None:
            return None, outcome
        thread_hash = _thread_hash(self.channel, thread)
        pool = AGENT_POOL if repository == REPO_U else pools[repository]["pool"]
        names = {claim["metadata"]["name"] for claim in self.live_claims}
        for claim in self.live_claims:
            if (
                claim["metadata"]["labels"]["curietech.ai/thread-hash"] == thread_hash
                and claim["metadata"]["name"] != profile["claim"]
            ):
                self.retiring.setdefault(claim["metadata"]["name"], 0)
        if profile["claim"] in names:
            self.current = next(
                claim for claim in self.live_claims if claim["metadata"]["name"] == profile["claim"]
            )
        else:
            self.current = _claim(
                profile["claim"], repository, profile["sandbox"], pool, thread_hash
            )
            self.live_claims.append(self.current)
        self.thread_repository.setdefault(thread, repository)
        return profile, outcome

    def run(self, argv: list[str], *, check: bool = True, input_text: str | None = None) -> str:
        if argv[0] == "docker":
            assert argv[:4] == ["docker", "buildx", "imagetools", "inspect"], argv
            self.docker_calls.append(list(argv))
            reference = argv[-1]
            assert reference.startswith("registry.example.com/curie-runner"), argv
            if "--raw" in argv:
                return json.dumps(AGENT_INDEX)
            return _inspect_text(reference, with_digest=self.inspect_has_digest)
        self.calls.append(list(argv))
        assert argv[:3] == ["kubectl", "--context", CONTEXT], argv
        assert self.p.namespace in argv
        get_resource = argv[argv.index("get") + 1] if "get" in argv else None
        if get_resource == "deployment":
            return json.dumps(self.deployment)
        if "patch" in argv:
            patch = json.loads(argv[argv.index("-p") + 1])
            self.patches.append(patch)
            if self.cas_conflict:
                self.deployment["spec"]["template"]["spec"]["containers"][1]["env"][1]["name"] = (
                    "UNRELATED_VARIABLE"
                )
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
            return "deployment.apps/factory-tools-worker patched"
        if "rollout" in argv:
            return 'deployment "factory-tools-worker" successfully rolled out'
        if get_resource == "sandboxclaims.extensions.agents.x-k8s.io":
            selector = dict(item.split("=", 1) for item in argv[argv.index("-l") + 1].split(","))
            items = [
                copy.deepcopy(claim)
                for claim in self.live_claims
                if all(claim["metadata"]["labels"].get(k) == v for k, v in selector.items())
            ]
            return json.dumps({"apiVersion": "v1", "kind": "List", "items": items})
        if get_resource == "sandboxclaim":
            assert argv[argv.index("sandboxclaim") + 2 :] == ["--ignore-not-found", "-o", "name"]
            name = argv[argv.index("sandboxclaim") + 1]
            self.retirement_queries.append(name)
            live = [claim for claim in self.live_claims if claim["metadata"]["name"] == name]
            if not live:
                return ""
            if name in self.retiring and not self.retirement_stays:
                if self.retiring[name] >= 1:
                    self.live_claims.remove(live[0])
                    return ""
                self.retiring[name] += 1
            return f"sandboxclaim.extensions.agents.x-k8s.io/{name}\n"
        if get_resource == "pod":
            sandbox = argv[argv.index("pod") + 1]
            profile = self._profile_for_sandbox(sandbox)
            return json.dumps(
                {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": sandbox, "uid": profile["pod_uid"]},
                    "spec": {
                        "containers": [
                            {
                                "name": "runner",
                                "image": profile["spec_image"],
                                "env": [
                                    *copy.deepcopy(self.runner_env),
                                    {"name": "CURIE_REPO_PATH", "value": profile["repository"]},
                                ],
                                "ports": [
                                    {"name": "aci", "containerPort": 4321, "protocol": "TCP"}
                                ],
                            }
                        ]
                    },
                    "status": {
                        "containerStatuses": [
                            {
                                "name": "runner",
                                "image": profile["spec_image"],
                                "imageID": profile["image_id"],
                                "ready": True,
                            }
                        ]
                    },
                }
            )
        if "logs" in argv:
            sandbox = argv[argv.index("logs") + 1]
            boot = self._profile_for_sandbox(sandbox)["boot_log"].strip()
            return (
                "2026-10-16 05:00:00,001 INFO curie_runner: runner starting\n"
                f"2026-10-16 05:00:00,002 INFO curie_runner: {boot}\n"
                "2026-10-16 05:00:00,003 INFO curie_runner: runner ready\n"
            )
        if "exec" in argv:
            assert "/status" in argv[-1]
            status = self.probes[min(self.probe_calls, len(self.probes) - 1)]
            self.probe_calls += 1
            return json.dumps(status)
        if get_resource == "sandboxwarmpool":
            name = argv[argv.index("sandboxwarmpool") + 1]
            if argv[-2:] == ["-o", "name"]:
                return "" if name.startswith("absent-") else f"sandboxwarmpool/{name}\n"
            assert argv[-2:] == ["-o", "json"], argv
            self.warm_pool_reads.append(name)
            if name != AGENT_POOL:
                raise fe.PreflightFailed(f'sandboxwarmpools "{name}" not found')
            return json.dumps(
                {
                    "apiVersion": "extensions.agents.x-k8s.io/v1alpha1",
                    "kind": "SandboxWarmPool",
                    "metadata": {"name": name},
                    "spec": {"replicas": 1, "sandboxTemplateRef": {"name": AGENT_TEMPLATE}},
                }
            )
        if get_resource == "sandboxtemplate":
            name = argv[argv.index("sandboxtemplate") + 1]
            assert argv[-2:] == ["-o", "json"], argv
            if name != AGENT_TEMPLATE:
                raise fe.PreflightFailed(f'sandboxtemplates "{name}" not found')
            return json.dumps(
                {
                    "apiVersion": "extensions.agents.x-k8s.io/v1alpha1",
                    "kind": "SandboxTemplate",
                    "metadata": {"name": name},
                    "spec": {
                        "podTemplate": {
                            "spec": {
                                "containers": [
                                    {
                                        "name": "egress-proxy",
                                        "image": "registry.example.com/proxy@" + _digest("7"),
                                    },
                                    {"name": "runner", "image": self.agent_template_image},
                                ]
                            }
                        }
                    },
                }
            )
        raise AssertionError(f"unexpected external kubectl command {argv}")

    def subprocess_run(self, argv: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(argv))
        assert argv[:3] == ["kubectl", "config", "view"]
        assert argv[argv.index("--context") + 1] == CONTEXT
        return subprocess.CompletedProcess(argv, 0, "apiVersion: v1\nkind: Config\n", "")


@pytest.fixture
def offline_driver(
    preflight: fe.Preflight, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[repo_runner.RepoRunnerRun, _KubernetesCli]]:
    preflight.release = "factory-tools"
    driver = repo_runner.RepoRunnerRun(preflight, _plan())
    cli = _KubernetesCli(preflight, driver.channel)
    driver.baseline_pools = json.loads(cli.pool_value())
    driver.installed_base = INSTALLED_BASE
    model = repo_runner.ScriptedManifestModel(host="127.0.0.1", port=0)
    model.start()
    driver.model = model

    class MessageProcess:
        def __init__(self, argv: list[str], **_kwargs: Any) -> None:
            assert argv[:5] == ["curie", "cluster", "--context", CONTEXT, "message"], argv
            assert argv[argv.index("--release") + 1] == "factory-tools"
            assert argv[argv.index("--namespace") + 1] == preflight.namespace
            profile, self.outcome = cli.start_message(argv)
            self.returncode = 1 if self.outcome.get("failed") else 0
            if profile is not None:
                first = _first_model_body()
                status, response = _model_request(model, first)
                assert status == 200
                result = _result_model_body(first, response)
                output = profile["manifest_output"]
                if output == repo_runner.NO_MANIFEST_OUTPUT:
                    output += "\n"  # echo's own newline
                result["messages"][-1]["content"][0]["content"] = output + REMINDERS
                assert _model_request(model, result)[0] == 200

        def poll(self) -> int:
            cli.exited = True
            if cli.post_exit_claim and cli.current is not None:
                cli.current["metadata"]["name"] = cli.post_exit_claim
            return self.returncode

        def communicate(self, *, timeout: int) -> tuple[str, str]:
            return json.dumps(self.outcome) + "\n", ""

    monkeypatch.setattr(fe, "run", cli.run)
    monkeypatch.setattr(subprocess, "run", cli.subprocess_run)
    monkeypatch.setattr(subprocess, "Popen", MessageProcess)
    monkeypatch.setattr(repo_runner.time, "sleep", lambda _interval: None)
    monkeypatch.setenv("CURIE_LOCAL_STUB_HOST", "198.51.100.10")
    try:
        yield driver, cli
    finally:
        assert model.close() == {"stopped": True}


def _run_plan(
    driver: repo_runner.RepoRunnerRun, turns: list[int] | None = None
) -> list[dict[str, Any]]:
    plan = _plan()
    return [driver._turn(index, plan["turns"][index]) for index in (turns or range(7))]


# --------------------------------------------------------------------------
# Whole offline flow (Decisions 2, 4-9)
# --------------------------------------------------------------------------


def test_seven_planned_turns_drive_offline_to_a_passing_judgment(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, cli = offline_driver
    turns = _run_plan(driver)
    assert [turn["status"] for turn in turns] == [
        "completed",
        "completed",
        "completed",
        "refused",
        "completed",
        "refused",
        "refused",
    ]
    assert [turn["expect"] for turn in turns] == [turn["expect"] for turn in _plan()["turns"]]
    x_first, y_first, agent, conflict, changed, missing, stale = turns
    for turn in (x_first, y_first, agent, changed):
        assert turn["runner_env"][TOKEN_NAME] == "16000"
    assert x_first["warm_pool"] == _passing()["worker_pools"][REPO_X]["pool"]
    assert "runner tool manifest loaded: 2 tools" in x_first["boot_log"]
    assert "runner starting" not in x_first["boot_log"]
    assert agent["warm_pool"] == AGENT_POOL
    assert agent["agent_image"] == {"spec_image": AGENT_PINNED, "digests": AGENT_DIGESTS}
    assert agent["manifest_output"] == repo_runner.NO_MANIFEST_OUTPUT + "\n" + REMINDERS
    assert repo_runner.MANIFEST_ABSENT_LINE in agent["boot_log"]
    assert conflict["reason"] == repo_runner.SELECTION_CONFLICT
    assert conflict["model_requests"] == 0 and conflict["new_claims"] == []
    assert changed["prior_claim_present"] is True
    assert changed["claim"] == "fixture-claim-x-changed"
    assert changed["warm_pool"] == _passing()["worker_pools"][REPO_Y]["pool"]
    assert changed["retired_claims"] == ["fixture-claim-x"]
    assert set(cli.retirement_queries) == {"fixture-claim-x"}
    assert missing["reason"] == "repo_runner_pool_missing"
    assert stale["reason"] == "repo_runner_base_mismatch"
    # The worker delivers every refusal as an ordinary reply; the CLI exits 0.
    for turn in (conflict, missing, stale):
        assert turn["cli_exit_code"] == 0 and turn["cli_failure_class"] is None
    assert conflict["cli_reply"] == CONFLICT_REPLY
    assert "(missing_pool)" in missing["cli_reply"]
    assert "(stale_base)" in stale["cli_reply"]
    for turn in (x_first, y_first, agent, changed):
        assert turn["cli_reply"] == "Manifest captured." and turn["cli_failure_class"] is None
    assert len(cli.messages) == 7
    driver.obs["worker_pools"] = copy.deepcopy(driver.baseline_pools)
    assert repo_runner.judge_repo_runner(driver.obs) == []


def test_driver_observation_names_runner_bindings_and_the_unconfigured_repository(
    preflight: fe.Preflight,
) -> None:
    driver = repo_runner.RepoRunnerRun(preflight, _plan())
    assert set(driver.obs["bindings"]) == {REPO_X, REPO_Y}
    assert driver.obs["bindings"][REPO_X] == _runner_configs()[REPO_X]
    assert driver.obs["unconfigured"] == REPO_U


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

    def external_run(argv: list[str], *, check: bool = True, input_text: str | None = None) -> str:
        assert argv[:3] == ["kubectl", "--context", CONTEXT]
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
    command = cli.calls[-1]
    assert command[command.index("-l") + 1] == (
        f"curietech.ai/thread-hash={_thread_hash(driver.channel, thread)},"
        f"curietech.ai/agent={fe.FACTORY_AGENT}"
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
    assert turn["manifest_output"] == _passing()["turns"][0]["manifest_output"] + REMINDERS
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


@pytest.mark.parametrize("missing_proof", ["finalized", "done-probe"])
def test_turn_does_not_complete_from_cli_exit_alone(
    missing_proof: str, offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli]
) -> None:
    driver, cli = offline_driver
    if missing_proof == "finalized":
        cli.forced_outcome = {"finalized": False, "failed": False, "reply": "Manifest captured."}
    else:
        cli.probes = [{"status": "running", "turn_active": True}]
    assert driver._turn(0, _plan()["turns"][0])["status"] == "failed"


def test_generic_runner_failure_is_a_failed_turn_not_a_refusal(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, cli = offline_driver
    cli.forced_outcome = {
        "failed": True,
        "failure_class": "runner_failed",
        "reply": "curie-turn-failure: runner_failed\nA runner failed.",
    }
    turn = driver._turn(0, _plan()["turns"][0])
    assert turn["status"] == "failed"
    assert turn["reason"] == "runner_failed"
    # The CLI outcome is kept, so a failed turn is diagnosable from evidence.
    assert turn["cli_failure_class"] == "runner_failed"
    assert turn["cli_reply"] == "curie-turn-failure: runner_failed\nA runner failed."


def test_a_turn_that_never_reaches_the_cli_records_no_cli_outcome(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, cli = offline_driver
    turn = driver._turn(4, _plan()["turns"][4])
    assert turn["reason"] == "runner_changed_prior_claim_absent"
    assert turn["cli_failure_class"] is None and turn["cli_reply"] is None
    assert cli.messages == []


@pytest.mark.parametrize(
    ("reason", "expected"),
    [("missing_pool", "repo_runner_pool_missing"), ("stale_base", "repo_runner_base_mismatch")],
)
def test_a_runner_pool_refusal_reply_is_its_documented_refusal(
    reason: str, expected: str, offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli]
) -> None:
    driver, cli = offline_driver
    cli.forced_outcome = _reply_outcome(
        THREAD_X, _pool_refusal_reply(REPO_X, "fixture-pool", reason, _digest("f"))
    )
    turn = driver._turn(0, _plan()["turns"][0])
    assert (turn["status"], turn["reason"]) == ("refused", expected)


def test_a_runner_pool_refusal_for_another_repository_is_not_this_turns_refusal(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, cli = offline_driver
    cli.forced_outcome = _reply_outcome(
        THREAD_X, _pool_refusal_reply(REPO_Y, "fixture-pool", "missing_pool")
    )
    assert driver._turn(0, _plan()["turns"][0])["status"] != "refused"


def test_the_live_unpinned_stale_base_reply_is_a_base_mismatch() -> None:
    # Recorded verbatim from the worker in a live reproduction, before the
    # install pinned the platform runner.
    reply = (
        f"This agent cannot start: repository {REPO_X} runner image was built on platform "
        f"runner {_digest('6')}, not this installation's unpinned runner image (stale_base); "
        f"rebuild it with `curie cluster factory --repo {REPO_X}`. An operator has to make "
        "that change."
    )
    outcome = _reply_outcome(THREAD_X, reply)
    assert repo_runner.refusal_reason(outcome, REPO_X) == "repo_runner_base_mismatch"


@pytest.mark.parametrize(
    "reply",
    [
        "No repo_runner_pool_missing occurred.",
        "The repository_runner check passed; workspace.selection_conflict did not happen.",
        f"Earlier I saw: {CONFLICT_REPLY} It is resolved now.",
        _pool_refusal_reply(REPO_X, "fixture-pool", "missing_pool") + " Then it recovered.",
    ],
    ids=["reason-token", "conflict-token", "quoted-conflict", "pool-reply-with-more-text"],
)
def test_a_reply_that_only_mentions_a_refusal_is_not_one(reply: str) -> None:
    assert repo_runner.refusal_reason(_reply_outcome(THREAD_X, reply), REPO_X) is None


def test_model_text_naming_a_pool_reason_is_not_a_refusal(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, cli = offline_driver
    cli.forced_outcome = _reply_outcome(
        THREAD_X, f"The manifest lists uv. Repository {REPO_X} is fine (missing_pool); ok"
    )
    assert driver._turn(0, _plan()["turns"][0])["status"] == "completed"


# --------------------------------------------------------------------------
# Decision 3: rendered baseline pools
# --------------------------------------------------------------------------


@pytest.mark.parametrize("shape", ["includes-unconfigured", "missing-y"])
def test_rendered_pools_must_name_exactly_the_two_runner_repositories(
    shape: str, offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli]
) -> None:
    driver, cli = offline_driver
    pools = copy.deepcopy(_passing()["worker_pools"])
    if shape == "includes-unconfigured":
        pools[REPO_U] = {
            "pool": "factory-tools-agent-factory-e2e-img-222222222222-runner-pool",
            "digests": AGENT_DIGESTS,
            "base": INSTALLED_BASE,
            "docker": False,
            "egress": [],
        }
    else:
        del pools[REPO_Y]
    cli.set_pools(pools)
    with pytest.raises(fe.PreflightFailed):
        driver.read_baseline_pools()


def test_rendered_pools_for_exactly_the_runner_repositories_are_the_baseline(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, _cli = offline_driver
    assert driver.read_baseline_pools() == _passing()["worker_pools"]


# --------------------------------------------------------------------------
# Decision 4: runner output-token env
# --------------------------------------------------------------------------


def _install_base(
    runner_env: dict[str, Any] | None, runner: dict[str, Any] | None = None
) -> dict[str, Any]:
    sandbox: dict[str, Any] = {
        "deploy": False,
        "runner": runner or {"image": "registry.example.com/curie-runner", "tag": "v0.13.0"},
    }
    if runner_env is not None:
        sandbox["runnerEnv"] = runner_env
    return {"factory": {"enabled": True}, "agentSandbox": sandbox, "api": {}}


def _inspect(monkeypatch: pytest.MonkeyPatch, *, owner: str = "registry.example.com") -> list[str]:
    """Replay `docker buildx imagetools inspect` (both documented forms) for one runner."""

    seen: list[str] = []

    def run(argv: list[str], *, check: bool = True, input_text: str | None = None) -> str:
        assert argv[:4] == ["docker", "buildx", "imagetools", "inspect"], argv
        assert argv[-1].startswith(f"{owner}/curie-runner"), argv
        seen.append(argv[-1])
        return json.dumps(AGENT_INDEX) if "--raw" in argv else _inspect_text(argv[-1])

    monkeypatch.setattr(fe, "run", run)
    return seen


@pytest.mark.parametrize("existing", [None, {}, {"other-agent": {"OTHER_SETTING": "kept"}}])
def test_install_values_set_the_factory_agent_runner_env_and_keep_other_agents(
    existing: dict[str, Any] | None, preflight: fe.Preflight, monkeypatch: pytest.MonkeyPatch
) -> None:
    _inspect(monkeypatch)
    preflight.release = "factory-tools"
    driver = repo_runner.RepoRunnerRun(preflight, _plan())
    base = _install_base(copy.deepcopy(existing))
    original = copy.deepcopy(base)
    values = driver.install_values(base)
    runner_env = values["agentSandbox"]["runnerEnv"]
    assert runner_env[fe.FACTORY_AGENT] == {TOKEN_NAME: "16000"}
    for agent, env in (existing or {}).items():
        assert runner_env[agent] == env
    assert set(runner_env) == {fe.FACTORY_AGENT, *(existing or {})}
    assert values["factory"]["bindings"] == _plan()["bindings"]
    unconfigured = values["factory"]["bindings"][2]["repos"][0]
    assert unconfigured["path"] == REPO_U
    assert "runner" not in unconfigured and "docker" not in unconfigured
    assert values["agentSandbox"]["deploy"] is True
    assert set(values["api"]["githubRepoAllowlist"]) == {REPO_X, REPO_Y, REPO_U}
    assert base == original
    runner_env[fe.FACTORY_AGENT][TOKEN_NAME] = "changed"
    assert repo_runner.RUNNER_ENV == {TOKEN_NAME: "16000"}


@pytest.mark.parametrize(
    ("runner", "owner", "reference"),
    [
        pytest.param(
            {"tag": "sha-abc"},
            "ghcr.io/curie-eng",
            "ghcr.io/curie-eng/curie-runner:sha-abc",
            id="published-chart-default-image",
        ),
        pytest.param(
            {"image": "registry.example.com/curie-runner", "tag": "candidate"},
            "registry.example.com",
            "registry.example.com/curie-runner:candidate",
            id="built-candidate-image",
        ),
    ],
)
def test_install_pins_the_platform_runner_the_repository_images_were_built_on(
    runner: dict[str, Any],
    owner: str,
    reference: str,
    preflight: fe.Preflight,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Live run 4919: installed by tag only, the worker had no platform runner
    # digest and refused every runner repository as stale_base before a claim.
    seen = _inspect(monkeypatch, owner=owner)
    driver = repo_runner.RepoRunnerRun(preflight, _plan())
    values = driver.install_values(_install_base(None, runner))
    assert values["agentSandbox"]["runner"]["digest"] == _digest("2")
    assert seen[0] == reference


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed")
def test_the_rendered_worker_reads_the_pinned_platform_runner_digest(
    preflight: fe.Preflight, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _inspect(monkeypatch, owner="ghcr.io/curie-eng")
    driver = repo_runner.RepoRunnerRun(preflight, _plan())
    values = driver.install_values(_install_base(None, {"tag": "sha-abc"}))
    values_file = tmp_path / "values.json"
    values_file.write_text(json.dumps({"agentSandbox": values["agentSandbox"]}))
    rendered = subprocess.run(
        [
            "helm",
            "template",
            "t",
            str(ROOT.parents[1] / "charts" / "curie"),
            "-f",
            str(values_file),
            "--show-only",
            "templates/worker.yaml",
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    image = json.loads(
        rendered.split("- name: CURIE_RUNNER_IMAGE\n", 1)[1].split("value: ", 1)[1].split("\n")[0]
    )
    # The worker's own reader (apps/worker/src/curie_worker/run.py) of this env.
    assert platform_runner_digest(image) == _digest("2")


def test_driver_records_runner_env_from_the_pod_runner_container(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, _cli = offline_driver
    turn = driver._turn(0, _plan()["turns"][0])
    # Only literal values are recorded; the secretKeyRef entry is not a value.
    assert turn["runner_env"] == {TOKEN_NAME: "16000", "CURIE_REPO_PATH": REPO_X}


def test_driver_records_a_missing_token_setting_for_the_judge(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, cli = offline_driver
    cli.runner_env = []
    turn = driver._turn(0, _plan()["turns"][0])
    assert turn["runner_env"] == {"CURIE_REPO_PATH": REPO_X}
    observation = _passing()
    observation["turns"][0] = {**observation["turns"][0], "runner_env": turn["runner_env"]}
    assert _mentions(repo_runner.judge_repo_runner(observation), "turn 1")


@pytest.mark.parametrize("turn", [0, 2, 4], ids=["completed", "agent-image", "runner-changed"])
@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param(lambda env: env.pop(TOKEN_NAME), id="token-missing"),
        pytest.param(lambda env: env.update({TOKEN_NAME: "8000"}), id="token-different"),
        pytest.param(lambda env: env.update({TOKEN_NAME: 16000}), id="token-not-string"),
    ],
)
def test_judge_requires_the_output_token_setting_on_every_completed_turn(
    turn: int, mutation: Callable[[dict[str, Any]], Any]
) -> None:
    observation = _passing()
    mutation(observation["turns"][turn]["runner_env"])
    assert _mentions(repo_runner.judge_repo_runner(observation), f"turn {turn + 1}")


@pytest.mark.parametrize("value", [None, [], "16000"])
def test_judge_requires_runner_env_as_a_mapping(value: Any) -> None:
    observation = _passing()
    observation["turns"][0]["runner_env"] = value
    assert _mentions(repo_runner.judge_repo_runner(observation), "turn 1")


@pytest.mark.parametrize("turn", [3, 5, 6])
@pytest.mark.parametrize("reason", [{}, [], 1], ids=["object", "array", "number"])
def test_judge_fails_closed_on_an_unhashable_or_non_string_refusal_reason(
    turn: int, reason: Any
) -> None:
    observation = _passing()
    observation["turns"][turn]["reason"] = reason
    failures = repo_runner.judge_repo_runner(observation)
    assert failures
    assert _mentions(failures, "unexpected repository runner refusal")


@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param(lambda entry: entry.update({"pool": []}), id="pool-not-string"),
        pytest.param(lambda entry: entry.pop("digests"), id="digests-missing"),
    ],
)
def test_judge_validates_worker_pools_against_the_pool_schema(
    mutation: Callable[[dict[str, Any]], Any],
) -> None:
    observation = _passing()
    mutation(observation["worker_pools"][REPO_X])
    assert repo_runner.judge_repo_runner(observation)


def test_runner_env_may_carry_other_names() -> None:
    observation = _passing()
    observation["turns"][0]["runner_env"]["CURIE_EXTRA"] = "1"
    assert repo_runner.judge_repo_runner(observation) == []


# --------------------------------------------------------------------------
# Decision 5: manifest probe and absent-manifest evidence
# --------------------------------------------------------------------------


def test_agent_turn_records_the_no_manifest_probe_and_the_absent_boot_line(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, _cli = offline_driver
    turn = driver._turn(2, _plan()["turns"][2])
    assert turn["status"] == "completed"
    assert turn["manifest_output"] == repo_runner.NO_MANIFEST_OUTPUT + "\n" + REMINDERS
    assert repo_runner.MANIFEST_ABSENT_LINE in turn["boot_log"]
    assert "runner starting" not in turn["boot_log"]
    assert "runner ready" not in turn["boot_log"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("manifest_output", "manifest-x", id="manifest-json-on-unconfigured"),
        pytest.param("manifest_output", "", id="no-probe-output"),
        pytest.param("boot_log", "runner ready\n", id="absent-line-missing"),
        pytest.param(
            "boot_log",
            "runner tool manifest absent: tool_count=0\nrunner tool manifest loaded: 2 tools\n",
            id="loaded-line-present",
        ),
    ],
)
def test_agent_turn_requires_no_manifest_evidence(field: str, value: str) -> None:
    observation = _passing()
    if value == "manifest-x":
        value = observation["turns"][0]["manifest_output"]
    observation["turns"][2][field] = value
    assert _mentions(repo_runner.judge_repo_runner(observation), "turn 3")


def test_agent_turn_accepts_the_probe_output_with_appended_system_reminders() -> None:
    # Live run 4919 turn 3: the probe's line, then two appended reminder blocks.
    observation = _passing()
    observation["turns"][2]["manifest_output"] = repo_runner.NO_MANIFEST_OUTPUT + "\n" + REMINDERS
    assert repo_runner.judge_repo_runner(observation) == []


@pytest.mark.parametrize(
    "output",
    [
        pytest.param(REMINDERS + "\ncurie-no-tool-manifest", id="reminders-before-the-line"),
        pytest.param("curie-no-tool-manifest\nextra" + REMINDERS, id="text-before-reminders"),
        pytest.param("curie-no-tool-manifest" + REMINDERS + "\nextra", id="text-after-reminders"),
        pytest.param(
            "curie-no-tool-manifest\n<system-reminder>\nnever closed\n", id="unclosed-reminder"
        ),
        pytest.param(
            "curie-no-tool-manifest\n<system-reminder>a<system-reminder>b</system-reminder>",
            id="nested-reminder",
        ),
        pytest.param("curie-no-tool-manifest: absent" + REMINDERS, id="different-first-line"),
        pytest.param(" curie-no-tool-manifest" + REMINDERS, id="indented-first-line"),
        pytest.param(REMINDERS, id="only-reminders"),
        pytest.param(
            "curie-no-tool-manifest<system-reminder>notice</system-reminder>",
            id="reminder-on-the-first-line",
        ),
    ],
)
def test_agent_turn_rejects_anything_but_the_line_and_reminder_blocks(output: str) -> None:
    observation = _passing()
    observation["turns"][2]["manifest_output"] = output
    assert _mentions(repo_runner.judge_repo_runner(observation), "turn 3")


def test_runner_turn_reads_its_manifest_before_appended_system_reminders() -> None:
    observation = _passing()
    observation["turns"][0]["manifest_output"] += REMINDERS
    assert repo_runner.judge_repo_runner(observation) == []


def test_runner_turn_accepts_a_reminder_tag_inside_its_manifest_json() -> None:
    observation = _passing()
    manifest = json.loads(observation["turns"][0]["manifest_output"])
    manifest["tools"][0]["path"] = "/opt/<system-reminder>/bin/tool"
    observation["turns"][0]["manifest_output"] = json.dumps(manifest) + REMINDERS
    assert not _mentions(repo_runner.judge_repo_runner(observation), "turn 1")


def test_runner_turn_rejects_other_text_after_its_manifest() -> None:
    observation = _passing()
    observation["turns"][0]["manifest_output"] += REMINDERS + "\ntrailing text"
    assert _mentions(repo_runner.judge_repo_runner(observation), "turn 1")


def test_agent_turn_accepts_the_echoed_no_manifest_output_with_a_newline() -> None:
    observation = _passing()
    observation["turns"][2]["manifest_output"] = "curie-no-tool-manifest\n"
    observation["turns"][2]["boot_log"] = (
        "runner starting\nrunner tool manifest absent: tool_count=0\n"
    )
    assert repo_runner.judge_repo_runner(observation) == []


# --------------------------------------------------------------------------
# Decision 6: agent image identity
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("reference", "inspections"),
    [(AGENT_TAG, 2), (AGENT_PINNED, 1)],
    ids=["tag", "pinned"],
)
def test_resolve_image_reads_the_index_and_runnable_platform_digests(
    reference: str,
    inspections: int,
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    _driver, cli = offline_driver
    index, platforms = repo_runner.resolve_image(reference)
    assert index == _digest("2")
    assert platforms == [_digest("3"), _digest("4")]
    assert len(cli.docker_calls) == inspections
    raw = [call for call in cli.docker_calls if "--raw" in call]
    assert len(raw) == 1 and raw[0][-1] == reference
    text = [call for call in cli.docker_calls if "--raw" not in call]
    assert all(call == ["docker", "buildx", "imagetools", "inspect", reference] for call in text)


def test_resolve_image_refuses_a_tag_without_an_observed_index_digest(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    _driver, cli = offline_driver
    cli.inspect_has_digest = False
    with pytest.raises(fe.PreflightFailed):
        repo_runner.resolve_image(AGENT_TAG)


@pytest.mark.parametrize("image", [AGENT_TAG, AGENT_PINNED], ids=["tag", "pinned"])
def test_agent_image_follows_warm_pool_to_template_runner_image(
    image: str, offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli]
) -> None:
    driver, cli = offline_driver
    cli.agent_template_image = image
    assert driver._agent_image(AGENT_POOL) == {"spec_image": image, "digests": AGENT_DIGESTS}
    assert cli.warm_pool_reads == [AGENT_POOL]
    template_reads = [call for call in cli.calls if "sandboxtemplate" in call]
    assert template_reads and template_reads[-1][
        template_reads[-1].index("sandboxtemplate") + 1
    ] == (AGENT_TEMPLATE)


@pytest.mark.parametrize("pool", ["x", "y", "", None], ids=["pool-x", "pool-y", "blank", "none"])
def test_agent_image_refuses_a_runner_pool_or_blank_pool_before_any_lookup(
    pool: str | None, offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli]
) -> None:
    driver, cli = offline_driver
    if pool in ("x", "y"):
        pool = driver.baseline_pools[REPO_X if pool == "x" else REPO_Y]["pool"]
    with pytest.raises(fe.PreflightFailed):
        driver._agent_image(pool)  # type: ignore[arg-type]
    assert cli.warm_pool_reads == [] and cli.docker_calls == []


def test_unresolved_agent_image_fails_the_agent_turn(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, cli = offline_driver
    cli.agent_template_image = AGENT_TAG
    cli.inspect_has_digest = False
    turn = driver._turn(2, _plan()["turns"][2])
    assert turn["status"] == "failed"
    assert turn["reason"] == "agent_image_unresolved"


def _x_runner_digests() -> list[str]:
    return _passing()["worker_pools"][REPO_X]["digests"]


@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param(
            lambda t: t.update(image_id="containerd://" + _digest("b")),
            id="imageid-is-x-platform-digest",
        ),
        pytest.param(
            lambda t: (
                t["agent_image"].update(digests=[_digest("2"), _digest("b"), _digest("4")]),
                t.update(image_id="containerd://" + _digest("b")),
            ),
            id="imageid-in-agent-digests-but-equals-x-platform",
        ),
        pytest.param(
            lambda t: (
                t["agent_image"].update(digests=[_digest("2"), _digest("3"), _digest("a")]),
            ),
            id="agent-digests-overlap-x-index",
        ),
        pytest.param(
            lambda t: (
                t["agent_image"].update(digests=[_digest("2"), _digest("3"), _digest("1")]),
            ),
            id="agent-digests-overlap-y-platform",
        ),
        pytest.param(
            lambda t: t.update(image_id="containerd://" + _digest("9")),
            id="imageid-not-in-agent-digests",
        ),
        pytest.param(
            lambda t: t["agent_image"].update(
                spec_image="registry.example.com/other-runner@" + _digest("2")
            ),
            id="spec-image-differs-from-agent-image",
        ),
        pytest.param(
            lambda t: (
                t.update(spec_image="registry.example.com/curie-runner@" + _digest("8")),
                t["agent_image"].update(
                    spec_image="registry.example.com/curie-runner@" + _digest("8")
                ),
            ),
            id="pinned-spec-digest-not-in-agent-digests",
        ),
        pytest.param(
            lambda t: t.update(
                warm_pool="factory-tools-agent-factory-e2e-img-aaaaaaaaaaaa-runner-pool"
            ),
            id="warm-pool-is-x-runner-pool",
        ),
        pytest.param(lambda t: t.update(warm_pool=""), id="warm-pool-blank"),
        pytest.param(lambda t: t.update(warm_pool=None), id="warm-pool-none"),
        pytest.param(lambda t: t.pop("agent_image"), id="agent-image-missing"),
        pytest.param(lambda t: t.update(agent_image=None), id="agent-image-null"),
        pytest.param(lambda t: t["agent_image"].update(digests=[]), id="agent-digests-empty"),
        pytest.param(
            lambda t: t["agent_image"].update(digests=["sha256:abc"]), id="agent-digest-not-exact"
        ),
    ],
)
def test_agent_turn_must_run_on_the_agent_image_not_a_runner_image(
    mutation: Callable[[dict[str, Any]], Any],
) -> None:
    observation = _passing()
    mutation(observation["turns"][2])
    assert _mentions(repo_runner.judge_repo_runner(observation), "turn 3")


@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param(
            lambda t: (
                t.update(spec_image=AGENT_TAG),
                t["agent_image"].update(spec_image=AGENT_TAG),
            ),
            id="tag-spec-image",
        ),
        pytest.param(
            lambda t: t.update(
                image_id="docker-pullable://registry.example.com/curie-runner@" + _digest("4")
            ),
            id="arm64-docker-pullable-identity",
        ),
        pytest.param(
            lambda t: t.update(image_id="containerd://" + _digest("2")), id="index-identity"
        ),
    ],
)
def test_agent_turn_accepts_realistic_agent_image_identities(
    mutation: Callable[[dict[str, Any]], Any],
) -> None:
    observation = _passing()
    mutation(observation["turns"][2])
    assert repo_runner.judge_repo_runner(observation) == []


def test_failing_sample_names_the_agent_turn_that_ran_a_runner_image() -> None:
    observation = json.loads((FIXTURES / "failing.json").read_text())["scenario"]
    assert observation["turns"][2]["image_id"].endswith(_x_runner_digests()[1])
    failures = repo_runner.judge_repo_runner(observation)
    assert failures and all(failure.startswith("turn 3") for failure in failures), failures


# --------------------------------------------------------------------------
# Decision 7: selection conflict is an accepted refusal
# --------------------------------------------------------------------------


def test_driver_maps_the_selection_conflict_reply_to_an_accepted_refusal(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, cli = offline_driver
    first, conflict = _run_plan(driver, [0, 3])
    assert first["status"] == "completed"
    assert conflict["status"] == "refused"
    assert conflict["reason"] == repo_runner.SELECTION_CONFLICT
    assert conflict["expect"] == repo_runner.SELECTION_CONFLICT
    assert conflict["model_requests"] == 0
    assert conflict["new_claims"] == []
    assert conflict["claim"] is None and conflict["sandbox"] is None
    assert cli.patches == []


def test_selection_conflict_is_refused_from_its_reply_without_a_failed_flag(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    # Live run 4919 turn 4: the worker answers the conflict as an ordinary
    # reply, so the CLI reports it finalized, not failed, and exits 0.
    driver, _cli = offline_driver
    first, conflict = _run_plan(driver, [0, 3])
    assert first["status"] == "completed"
    assert conflict["cli_reply"] == CONFLICT_REPLY and conflict["cli_exit_code"] == 0
    assert (conflict["status"], conflict["reason"]) == ("refused", repo_runner.SELECTION_CONFLICT)


def test_a_conflict_reply_on_a_turn_that_ran_is_refused_and_then_rejected(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    # A conflict reply on a turn that also acquired a claim and asked the
    # model is recorded as the refusal it claims to be; the judge's refusal
    # checks, not the classifier, then reject it.
    driver, cli = offline_driver
    cli.forced_outcome = _reply_outcome(THREAD_X, CONFLICT_REPLY)
    turns = _run_plan(driver)
    assert turns[0]["status"] == "refused" and turns[0]["claim"]
    driver.obs["worker_pools"] = copy.deepcopy(driver.baseline_pools)
    failures = repo_runner.judge_repo_runner(driver.obs)
    assert "turn 1: refused turn acquired claim" in failures
    assert "turn 1: refusal must precede every model request" in failures


@pytest.mark.parametrize(
    ("mutation", "needles"),
    [
        pytest.param(
            lambda o: o["turns"][3].update(thread="fixture-fresh-thread"),
            ("turn 4", "selection_conflict", "selection conflict"),
            id="fresh-thread",
        ),
        pytest.param(
            lambda o: o["turns"][3].update(repository=REPO_X),
            ("turn 4", "selection_conflict", "selection conflict"),
            id="same-repository",
        ),
        pytest.param(
            lambda o: o["turns"].insert(0, o["turns"].pop(3)),
            ("turn 1", "selection_conflict", "selection conflict"),
            id="thread-only-used-later",
        ),
        pytest.param(
            lambda o: o["turns"][3].update(thread=o["turns"][5]["thread"]),
            ("selection_conflict", "selection conflict", "turn 4", "turn 6", "thread"),
            id="thread-of-a-refusal-only",
        ),
        pytest.param(
            lambda o: o["turns"][3].update(model_requests=1), ("turn 4",), id="contacted-model"
        ),
        pytest.param(
            lambda o: o["turns"][3].update(new_claims=["c"]), ("turn 4",), id="created-claim"
        ),
        pytest.param(
            lambda o: o["turns"][3].update(sandbox="s"), ("turn 4",), id="acquired-sandbox"
        ),
        pytest.param(lambda o: o["turns"][3].update(claim="c"), ("turn 4",), id="acquired-claim"),
        pytest.param(lambda o: o["turns"][3].update(pod_uid="p"), ("turn 4",), id="acquired-pod"),
        pytest.param(
            lambda o: o["turns"][3].update(image_id="containerd://" + _digest("f")),
            ("turn 4",),
            id="acquired-image",
        ),
    ],
)
def test_selection_conflict_must_follow_another_repository_on_the_same_thread(
    mutation: Callable[[dict[str, Any]], Any], needles: tuple[str, ...]
) -> None:
    observation = _passing()
    mutation(observation)
    failures = repo_runner.judge_repo_runner(observation)
    assert _mentions(failures, *needles), failures


def test_selection_conflict_on_any_earlier_other_repository_thread_is_accepted() -> None:
    observation = _passing()
    observation["turns"][3].update(repository=REPO_X, thread=observation["turns"][1]["thread"])
    assert repo_runner.judge_repo_runner(observation) == []


@pytest.mark.parametrize(
    ("turn", "other"),
    [(5, 0), (5, 1), (5, 2), (6, 5), (5, 3)],
    ids=["x-thread", "y-thread", "agent-thread", "other-fault-thread", "conflict-thread"],
)
def test_each_fault_refusal_uses_a_thread_no_other_turn_used(turn: int, other: int) -> None:
    observation = _passing()
    observation["turns"][turn]["thread"] = observation["turns"][other]["thread"]
    assert repo_runner.judge_repo_runner(observation)


# --------------------------------------------------------------------------
# Decision 8: runner change on one thread
# --------------------------------------------------------------------------


def test_runner_change_records_the_prior_claim_and_its_retirement(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, cli = offline_driver
    first, changed = _run_plan(driver, [0, 4])
    assert changed["status"] == "completed"
    assert changed["prior_claim_present"] is True
    assert changed["claim"] == "fixture-claim-x-changed" != first["claim"]
    assert changed["sandbox"] != first["sandbox"]
    assert changed["pod_uid"] != first["pod_uid"]
    assert changed["image_id"] == _profiles()["x-changed"]["image_id"]
    assert changed["capture_error"] is None
    assert changed["retired_claims"] == [first["claim"]]
    assert cli.retirement_queries[0] == first["claim"]  # the presence check precedes sending
    rendered = json.loads(cli.pool_value())
    assert rendered[REPO_X] == driver.baseline_pools[REPO_Y]


def test_runner_change_without_its_prior_claim_fails_before_sending(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, cli = offline_driver
    first = driver._turn(0, _plan()["turns"][0])
    assert first["status"] == "completed"
    cli.live_claims.clear()
    changed = driver._turn(4, _plan()["turns"][4])
    assert changed["prior_claim_present"] is False
    assert changed["status"] == "failed"
    assert changed["reason"] == "runner_changed_prior_claim_absent"
    assert len(cli.messages) == 1
    assert first["claim"] in cli.retirement_queries


def test_runner_change_refuses_a_target_entry_that_is_not_the_other_baseline_entry(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, cli = offline_driver
    planned = _plan()["turns"][4]
    planned["worker_pools"][REPO_X]["pool"] = "factory-tools-agent-factory-e2e-other-pool"
    planned["worker_pools"][REPO_Y]["pool"] = "factory-tools-agent-factory-e2e-other-pool"
    with pytest.raises(fe.ConfigError):
        driver._turn(4, planned)
    assert cli.patches == []
    assert cli.messages == []


def test_capture_prefers_the_claim_new_to_the_turn_while_the_old_one_is_bound(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, _cli = offline_driver
    thread_hash = _thread_hash(driver.channel, THREAD_X)
    old, new = _profiles()["x"], _profiles()["x-changed"]
    claims = [
        _claim(old["claim"], REPO_X, old["sandbox"], "pool-x", thread_hash),
        _claim(new["claim"], REPO_X, new["sandbox"], "pool-y", thread_hash),
    ]
    turn: dict[str, Any] = {"repository": REPO_X, "thread": THREAD_X, "capture_error": None}
    driver._capture_runner(claims, turn, {old["claim"]})
    assert turn["capture_error"] is None
    assert turn["claim"] == new["claim"]
    assert turn["sandbox"] == new["sandbox"]
    assert turn["pod_uid"] == new["pod_uid"]
    assert turn["warm_pool"] == "pool-y"


def test_capture_refuses_two_new_bound_claims(
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
) -> None:
    driver, _cli = offline_driver
    thread_hash = _thread_hash(driver.channel, THREAD_X)
    old, new = _profiles()["x"], _profiles()["x-changed"]
    claims = [
        _claim(old["claim"], REPO_X, old["sandbox"], "pool-x", thread_hash),
        _claim(new["claim"], REPO_X, new["sandbox"], "pool-y", thread_hash),
    ]
    turn: dict[str, Any] = {"repository": REPO_X, "thread": THREAD_X, "capture_error": None}
    driver._capture_runner(claims, turn, set())
    assert turn["capture_error"]
    assert not turn.get("claim")


@pytest.mark.parametrize("stays", [False, True], ids=["claim-disappears", "claim-remains"])
def test_runner_change_collects_actual_retirement_and_the_judge_requires_it(
    stays: bool,
    offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver, cli = offline_driver
    cli.retirement_stays = stays
    ticks = itertools.count(0, 20)
    monkeypatch.setattr(fe.time, "monotonic", lambda: next(ticks))
    turns = _run_plan(driver)
    changed = turns[4]
    assert changed["status"] == "completed"
    assert set(cli.retirement_queries) == {"fixture-claim-x"}
    driver.obs["worker_pools"] = copy.deepcopy(driver.baseline_pools)
    failures = repo_runner.judge_repo_runner(driver.obs)
    if stays:
        assert changed["retired_claims"] == []
        assert _mentions(failures, "turn 5"), failures
    else:
        assert changed["retired_claims"] == ["fixture-claim-x"]
        assert failures == []


def test_second_turn_with_the_same_claim_does_not_wait_for_retirement(
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


@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param(
            lambda o: o["turns"][4].update(prior_claim_present=False), id="prior-claim-absent"
        ),
        pytest.param(
            lambda o: o["turns"][4].pop("prior_claim_present"), id="prior-claim-unrecorded"
        ),
        pytest.param(
            lambda o: o["turns"][4].update(prior_claim_present="true"), id="prior-claim-not-boolean"
        ),
        pytest.param(
            lambda o: o["turns"][4].update(sandbox=o["turns"][0]["sandbox"]), id="sandbox-reused"
        ),
        pytest.param(
            lambda o: o["turns"][4].update(pod_uid=o["turns"][0]["pod_uid"]), id="pod-reused"
        ),
        pytest.param(
            lambda o: o["turns"][4].update(claim=o["turns"][0]["claim"]), id="claim-reused"
        ),
        pytest.param(
            lambda o: o["turns"][4].update(retired_claims=[]), id="prior-claim-not-retired"
        ),
        pytest.param(
            lambda o: o["turns"][4].update(retired_claims=["unrelated-claim"]),
            id="unrelated-claim-retired",
        ),
        pytest.param(
            lambda o: o["turns"][4].update(image_id="containerd://" + _digest("b")),
            id="imageid-in-own-x-digests",
        ),
        pytest.param(
            lambda o: o["turns"][4].update(spec_image=o["bindings"][REPO_X]["image"]),
            id="spec-image-is-own-x-image",
        ),
        pytest.param(
            lambda o: o["turns"][4].update(
                manifest_output=o["turns"][0]["manifest_output"],
                boot_log="runner tool manifest loaded: 2 tools\n",
            ),
            id="manifest-of-x-tools",
        ),
    ],
)
def test_runner_change_requires_new_identity_retirement_and_the_other_runner(
    mutation: Callable[[dict[str, Any]], Any],
) -> None:
    observation = _passing()
    mutation(observation)
    failures = repo_runner.judge_repo_runner(observation)
    assert _mentions(failures, "turn 5"), failures


# --------------------------------------------------------------------------
# Decision 9: top-level judge requirements
# --------------------------------------------------------------------------


def test_passing_sample_needs_no_same_thread_repository_switch() -> None:
    observation = _passing()
    assert observation["turns"][0]["thread"] != observation["turns"][1]["thread"]
    completed = [turn for turn in observation["turns"] if turn["status"] == "completed"]
    for left in completed:
        for right in completed:
            assert left["thread"] != right["thread"] or left["repository"] == right["repository"]
    assert repo_runner.judge_repo_runner(observation) == []


def test_fault_refusals_may_come_in_either_order() -> None:
    observation = _passing()
    observation["turns"][5], observation["turns"][6] = (
        observation["turns"][6],
        observation["turns"][5],
    )
    assert repo_runner.judge_repo_runner(observation) == []


def test_y_without_an_ordinary_completed_turn_fails() -> None:
    observation = _passing()
    del observation["turns"][1]
    failures = repo_runner.judge_repo_runner(observation)
    assert _mentions(failures, REPO_Y), failures


def test_unconfigured_repository_without_an_agent_turn_fails() -> None:
    observation = _passing()
    del observation["turns"][2]
    failures = repo_runner.judge_repo_runner(observation)
    assert _mentions(failures, REPO_U, "agent"), failures


@pytest.mark.parametrize(
    ("turn", "needles"),
    [
        (3, ("selection_conflict",)),
        (4, ("runner_changed", "runner change", "runner-change")),
        (5, ("repo_runner_pool_missing",)),
        (6, ("repo_runner_base_mismatch",)),
    ],
    ids=["selection-conflict", "runner-changed", "pool-missing", "base-mismatch"],
)
def test_each_required_case_must_be_observed(turn: int, needles: tuple[str, ...]) -> None:
    observation = _passing()
    del observation["turns"][turn]
    failures = repo_runner.judge_repo_runner(observation)
    assert _mentions(failures, *needles), failures


def _duplicate_conflict(observation: dict[str, Any]) -> None:
    observation["turns"].append(copy.deepcopy(observation["turns"][3]))


def _duplicate_fault(index: int, thread: str) -> Callable[[dict[str, Any]], None]:
    def mutate(observation: dict[str, Any]) -> None:
        extra = copy.deepcopy(observation["turns"][index])
        extra["thread"] = thread
        observation["turns"].append(extra)

    return mutate


def _duplicate_runner_change(observation: dict[str, Any]) -> None:
    extra = copy.deepcopy(observation["turns"][4])
    extra.update(
        claim="fixture-claim-x-third",
        sandbox="fixture-sandbox-x-third",
        pod_uid="00000000-0000-4000-8000-000000000008",
        new_claims=["fixture-claim-x-third"],
        retired_claims=["fixture-claim-x-changed"],
    )
    observation["turns"].append(extra)


@pytest.mark.parametrize(
    ("mutation", "needles"),
    [
        pytest.param(_duplicate_conflict, ("selection_conflict",), id="two-selection-conflicts"),
        pytest.param(
            _duplicate_runner_change,
            ("runner_changed", "runner change", "runner-change"),
            id="two-runner-changes",
        ),
        pytest.param(
            _duplicate_fault(5, "fixture-second-missing-pool-thread"),
            ("repo_runner_pool_missing",),
            id="two-pool-missing",
        ),
        pytest.param(
            _duplicate_fault(6, "fixture-second-stale-base-thread"),
            ("repo_runner_base_mismatch",),
            id="two-base-mismatch",
        ),
    ],
)
def test_each_required_case_is_observed_exactly_once(
    mutation: Callable[[dict[str, Any]], None], needles: tuple[str, ...]
) -> None:
    observation = _passing()
    mutation(observation)
    failures = repo_runner.judge_repo_runner(observation)
    assert _mentions(failures, *needles), failures


@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param(lambda o: o.pop("unconfigured"), id="unconfigured-missing"),
        pytest.param(lambda o: o.update(unconfigured=REPO_X), id="unconfigured-is-a-runner-repo"),
        pytest.param(lambda o: o.update(unconfigured="not a path"), id="unconfigured-malformed"),
        pytest.param(lambda o: o.update(unconfigured=None), id="unconfigured-null"),
        pytest.param(lambda o: o.pop("worker_pools"), id="worker-pools-missing"),
        pytest.param(lambda o: o.update(worker_pools=[]), id="worker-pools-not-mapping"),
        pytest.param(lambda o: o["worker_pools"].pop(REPO_Y), id="worker-pools-missing-y"),
        pytest.param(
            lambda o: o["worker_pools"].update({REPO_U: copy.deepcopy(o["worker_pools"][REPO_Y])}),
            id="worker-pools-include-unconfigured",
        ),
        pytest.param(
            lambda o: o["bindings"].update({REPO_U: copy.deepcopy(o["bindings"][REPO_Y])}),
            id="three-runner-bindings",
        ),
        pytest.param(lambda o: o["bindings"].pop(REPO_Y), id="one-runner-binding"),
        pytest.param(
            lambda o: o["bindings"][REPO_Y].update(image=o["bindings"][REPO_X]["image"]),
            id="equal-runner-index-digests",
        ),
        pytest.param(
            lambda o: o["turns"][1].update(repository="acme-corp/unbound"),
            id="turn-repository-unbound",
        ),
    ],
)
def test_observation_shape_is_required(mutation: Callable[[dict[str, Any]], Any]) -> None:
    observation = _passing()
    mutation(observation)
    assert repo_runner.judge_repo_runner(observation)


# --------------------------------------------------------------------------
# Fault overrides (unchanged behavior, seven-turn positions)
# --------------------------------------------------------------------------


@pytest.mark.parametrize("index", [5, 6], ids=["missing-pool", "stale-base"])
def test_fault_turn_changes_only_its_declared_target_and_preserves_named_refusal(
    index: int, offline_driver: tuple[repo_runner.RepoRunnerRun, _KubernetesCli]
) -> None:
    driver, cli = offline_driver
    planned = _plan()["turns"][index]
    before = copy.deepcopy(driver.baseline_pools)
    turn = driver._turn(index, planned)
    assert turn["status"] == "refused"
    assert turn["reason"] == planned["expect"]
    after = json.loads(cli.pool_value())
    field = "pool" if index == 5 else "base"
    expected = copy.deepcopy(before)
    expected[REPO_X][field] = planned["worker_pools"][REPO_X][field]
    assert after == expected
    assert turn["new_claims"] == []
    assert turn["model_requests"] == 0
    if index == 5:
        assert turn["missing_pool_absent"] is True
    else:
        assert (turn["installed_base"], turn["configured_base"]) == (INSTALLED_BASE, _digest("f"))


@pytest.mark.parametrize("index", [5, 6], ids=["missing-pool", "stale-base"])
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


# --------------------------------------------------------------------------
# Driver guard (Decision 10)
# --------------------------------------------------------------------------


def test_drive_refuses_a_blank_context_before_any_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    p = _make_preflight(tmp_path, context="")

    def forbid(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("a blank context must launch no command")

    monkeypatch.setattr(fe, "run", forbid)
    monkeypatch.setattr(subprocess, "run", forbid)
    try:
        with pytest.raises(fe.ConfigError) as refused:
            repo_runner.RepoRunnerRun(p, _plan()).drive()
        assert "required tools" not in str(refused.value)
    finally:
        shutil.rmtree(p.workdir)


def test_drive_accepts_the_throwaway_context_past_its_guard(
    preflight: fe.Preflight, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(repo_runner.shutil, "which", lambda _tool: None)
    with pytest.raises(fe.ConfigError, match="required tools not on PATH"):
        repo_runner.RepoRunnerRun(preflight, _plan()).drive()


# --------------------------------------------------------------------------
# Plan loading (Decisions 1, 2)
# --------------------------------------------------------------------------


def test_plan_loads_three_bindings_and_seven_turns(tmp_path: Path) -> None:
    plan = _plan()
    loaded = _load(tmp_path, plan)
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
    _refused(tmp_path, plan)


def test_plan_refuses_extra_top_level_keys(tmp_path: Path) -> None:
    plan = _plan()
    plan["extra"] = True
    _refused(tmp_path, plan)


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
        ("bindings", 0, "repos", 0, "docker"),
        ("bindings", 0, "repos", 0, "bases"),
        ("bindings", 0, "repos", 0, "ci"),
        ("bindings", 2, "repos", 0, "bases"),
        ("bindings", 2, "repos", 0, "ci"),
        ("bindings", 2, "repos", 0, "alias"),
        ("bindings", 2, "repos", 0, "projectId"),
        ("turns", 0, "repository"),
        ("turns", 0, "thread"),
        ("turns", 0, "text"),
        ("turns", 0, "expect"),
        ("turns", 4, "worker_pools"),
        ("turns", 5, "worker_pools"),
        ("turns", 6, "worker_pools"),
    ],
)
def test_each_required_plan_field_is_required(path: tuple[Any, ...], tmp_path: Path) -> None:
    plan = _plan()
    del _node(plan, path[:-1])[path[-1]]
    _refused(tmp_path, plan)


@pytest.mark.parametrize("count", [2, 4], ids=["two-bindings", "four-bindings"])
def test_plan_needs_exactly_three_bindings(count: int, tmp_path: Path) -> None:
    plan = _plan()
    if count == 2:
        plan["bindings"].pop()
    else:
        plan["bindings"].append(_binding("fixture-v", "104", "v", "acme-corp/repo-v", None))
    _refused(tmp_path, plan)


def _with_runner_on_u(docker: bool, runner: bool) -> Callable[[dict[str, Any]], None]:
    def mutate(plan: dict[str, Any]) -> None:
        repo = _plan_repo(plan, REPO_U)
        if runner:
            config = copy.deepcopy(_runner_configs()[REPO_Y])
            config["image"] = "registry.example.com/acme-runner-u@" + _digest("5")
            repo["runner"] = config
        if docker:
            repo["docker"] = {"enabled": False}

    return mutate


@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param(
            _with_runner_on_u(docker=True, runner=True), id="unconfigured-has-runner-and-docker"
        ),
        pytest.param(_with_runner_on_u(docker=False, runner=True), id="unconfigured-has-runner"),
        pytest.param(_with_runner_on_u(docker=True, runner=False), id="unconfigured-has-docker"),
        pytest.param(
            lambda plan: _plan_repo(plan, REPO_X).update(docker={"enabled": True}),
            id="runner-repo-docker-enabled",
        ),
        pytest.param(
            lambda plan: _plan_repo(plan, REPO_X).update(docker={}), id="runner-repo-docker-empty"
        ),
        pytest.param(
            lambda plan: _plan_repo(plan, REPO_X).pop("runner"), id="runner-repo-without-runner"
        ),
        pytest.param(
            lambda plan: _plan_repo(plan, REPO_U).update(bases=[]), id="unconfigured-empty-bases"
        ),
        pytest.param(
            lambda plan: _plan_repo(plan, REPO_U).update(ci=None), id="unconfigured-ci-not-mapping"
        ),
        pytest.param(
            lambda plan: _plan_repo(plan, REPO_U).update(projectId="999"),
            id="unconfigured-project-differs-from-scope",
        ),
        pytest.param(
            lambda plan: _plan_repo(plan, REPO_U).update(path=REPO_X),
            id="unconfigured-duplicates-x",
        ),
        pytest.param(
            lambda plan: plan["bindings"][2].update(
                trackerScopeId="101",
                repos=[{**plan["bindings"][2]["repos"][0], "projectId": "101"}],
            ),
            id="unconfigured-duplicates-scope",
        ),
        pytest.param(
            lambda plan: plan["bindings"][2].update(agent="fixture"),
            id="unconfigured-binding-agent",
        ),
        pytest.param(
            lambda plan: plan["bindings"][2].update(tracker="jira"),
            id="unconfigured-unknown-tracker",
        ),
    ],
)
def test_plan_has_exactly_two_runner_repositories_and_one_without(
    mutation: Callable[[dict[str, Any]], Any], tmp_path: Path
) -> None:
    plan = _plan()
    mutation(plan)
    _refused(tmp_path, plan)


def test_plan_refuses_runner_repositories_with_equal_index_digests(tmp_path: Path) -> None:
    configs = _runner_configs()
    configs[REPO_Y]["image"] = (
        "registry.example.com/acme-runner-y@" + configs[REPO_X]["image"].split("@", 1)[1]
    )
    _refused(tmp_path, _plan(configs))


def test_plan_accepts_runner_repositories_with_other_distinct_digests(tmp_path: Path) -> None:
    configs = _runner_configs()
    configs[REPO_Y]["image"] = "registry.example.com/acme-runner-y@" + _digest("7")
    plan = _plan(configs)
    assert _load(tmp_path, plan)["turns"][4]["worker_pools"][REPO_X]["digests"][0] == _digest("7")


def _swap(first: int, second: int) -> Callable[[dict[str, Any]], None]:
    def mutate(plan: dict[str, Any]) -> None:
        turns = plan["turns"]
        turns[first], turns[second] = turns[second], turns[first]

    return mutate


def _turn_update(index: int, **fields: Any) -> Callable[[dict[str, Any]], None]:
    def mutate(plan: dict[str, Any]) -> None:
        plan["turns"][index].update(fields)

    return mutate


def _baseline_plan_pools() -> dict[str, Any]:
    return {repository: _pool_entry(config) for repository, config in _runner_configs().items()}


def _add_pools(index: int) -> Callable[[dict[str, Any]], None]:
    def mutate(plan: dict[str, Any]) -> None:
        plan["turns"][index]["worker_pools"] = _baseline_plan_pools()

    return mutate


def _add_unconfigured_pool(index: int) -> Callable[[dict[str, Any]], None]:
    def mutate(plan: dict[str, Any]) -> None:
        plan["turns"][index]["worker_pools"][REPO_U] = {
            "pool": "factory-tools-agent-factory-e2e-img-222222222222-runner-pool",
            "digests": AGENT_DIGESTS,
            "base": INSTALLED_BASE,
            "docker": False,
        }

    return mutate


def _change_pools(**target: Any) -> Callable[[dict[str, Any]], None]:
    def mutate(plan: dict[str, Any]) -> None:
        plan["turns"][4]["worker_pools"][REPO_X].update(target)

    return mutate


def _change_other_pool(**fields: Any) -> Callable[[dict[str, Any]], None]:
    def mutate(plan: dict[str, Any]) -> None:
        pools = plan["turns"][4]["worker_pools"]
        pools[REPO_Y].update(fields)
        pools[REPO_X] = copy.deepcopy(pools[REPO_Y])

    return mutate


def _seven_plus_one(plan: dict[str, Any]) -> None:
    extra = copy.deepcopy(plan["turns"][6])
    extra["thread"] = "another-stale-base-thread"
    plan["turns"].append(extra)


@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param(lambda plan: plan["turns"].pop(), id="six-turns"),
        pytest.param(_seven_plus_one, id="eight-turns"),
        pytest.param(_swap(0, 1), id="y-before-x"),
        pytest.param(_swap(2, 3), id="conflict-before-agent"),
        pytest.param(_swap(3, 4), id="runner-change-before-conflict"),
        pytest.param(_swap(4, 5), id="fault-before-runner-change"),
        pytest.param(_turn_update(0, repository=REPO_U), id="first-turn-unconfigured"),
        pytest.param(_turn_update(0, expect="agent_image"), id="first-turn-agent-image"),
        pytest.param(_turn_update(1, repository=REPO_X), id="second-turn-same-repository"),
        pytest.param(_turn_update(1, thread=THREAD_X), id="second-turn-reuses-first-thread"),
        pytest.param(_turn_update(1, repository=REPO_U), id="second-turn-unconfigured"),
        pytest.param(_turn_update(2, repository=REPO_X), id="agent-turn-runner-repository"),
        pytest.param(_turn_update(2, expect="completed"), id="agent-turn-expects-completed"),
        pytest.param(_turn_update(2, thread=THREAD_X), id="agent-turn-reuses-x-thread"),
        pytest.param(_turn_update(2, thread=THREAD_Y), id="agent-turn-reuses-y-thread"),
        pytest.param(_turn_update(3, repository=REPO_X), id="conflict-same-repository"),
        pytest.param(_turn_update(3, repository=REPO_U), id="conflict-unconfigured"),
        pytest.param(_turn_update(3, thread=THREAD_Y), id="conflict-on-y-thread"),
        pytest.param(_turn_update(3, thread="fresh-thread"), id="conflict-fresh-thread"),
        pytest.param(_turn_update(3, expect="completed"), id="conflict-expects-completed"),
        pytest.param(_turn_update(4, repository=REPO_Y), id="runner-change-on-y"),
        pytest.param(_turn_update(4, thread=THREAD_Y), id="runner-change-on-y-thread"),
        pytest.param(_turn_update(4, thread="fresh-thread"), id="runner-change-fresh-thread"),
        pytest.param(_turn_update(4, expect="completed"), id="runner-change-expects-completed"),
        pytest.param(_turn_update(5, thread=THREAD_X), id="fault-reuses-x-thread"),
        pytest.param(_turn_update(5, thread=THREAD_U), id="fault-reuses-agent-thread"),
        pytest.param(_turn_update(6, thread="missing-pool-thread"), id="faults-share-thread"),
        pytest.param(_turn_update(5, repository=REPO_U), id="fault-on-unconfigured"),
        pytest.param(_turn_update(5, expect="runner_changed"), id="fault-expects-runner-change"),
        pytest.param(_turn_update(6, expect="repo_runner_pool_missing"), id="two-pool-faults"),
        pytest.param(_turn_update(0, text=""), id="blank-trigger-text"),
        pytest.param(_turn_update(0, expect="failed"), id="unknown-expectation"),
        pytest.param(_turn_update(0, repository="acme-corp/unbound"), id="unbound-turn"),
        pytest.param(_turn_update(0, extra="field"), id="unknown-turn-field"),
        pytest.param(_add_pools(0), id="pools-on-turn-1"),
        pytest.param(_add_pools(1), id="pools-on-turn-2"),
        pytest.param(_add_pools(2), id="pools-on-turn-3"),
        pytest.param(_add_pools(3), id="pools-on-turn-4"),
        pytest.param(_add_unconfigured_pool(4), id="runner-change-pools-include-unconfigured"),
        pytest.param(_add_unconfigured_pool(5), id="fault-pools-include-unconfigured"),
        pytest.param(
            lambda plan: plan["turns"][4]["worker_pools"].pop(REPO_Y),
            id="runner-change-pools-missing-other",
        ),
        pytest.param(
            _change_pools(**_baseline_plan_pools()[REPO_X]), id="runner-change-target-unchanged"
        ),
        pytest.param(_change_pools(base=_digest("f")), id="runner-change-target-base-differs"),
        pytest.param(
            _change_pools(digests=_baseline_plan_pools()[REPO_X]["digests"]),
            id="runner-change-target-keeps-own-digests",
        ),
        pytest.param(
            _change_pools(pool="factory-tools-agent-factory-e2e-other-pool"),
            id="runner-change-target-pool-differs",
        ),
        pytest.param(_change_other_pool(base=_digest("f")), id="runner-change-other-base-changed"),
        pytest.param(_change_other_pool(docker=True), id="runner-change-other-docker-changed"),
        pytest.param(
            _change_other_pool(digests=[_digest("7"), _digest("8"), _digest("9")]),
            id="runner-change-other-digests-changed",
        ),
        pytest.param(lambda plan: plan["turns"][5].update(worker_pools={}), id="empty-pool-fault"),
    ],
)
def test_invalid_seven_turn_plan_is_refused_before_execution(
    mutation: Callable[[dict[str, Any]], Any], tmp_path: Path
) -> None:
    plan = _plan()
    mutation(plan)
    _refused(tmp_path, plan)


def test_plan_accepts_faults_in_either_order(tmp_path: Path) -> None:
    plan = _plan()
    plan["turns"][5], plan["turns"][6] = plan["turns"][6], plan["turns"][5]
    assert [turn["expect"] for turn in _load(tmp_path, plan)["turns"][5:]] == [
        "repo_runner_base_mismatch",
        "repo_runner_pool_missing",
    ]


def test_plan_accepts_faults_on_y(tmp_path: Path) -> None:
    plan = _plan()
    for index, field, value in ((5, "pool", "absent-fixture-y"), (6, "base", _digest("f"))):
        pools = _baseline_plan_pools()
        pools[REPO_Y][field] = value
        plan["turns"][index].update(repository=REPO_Y, worker_pools=pools)
    assert _load(tmp_path, plan)["turns"][5]["repository"] == REPO_Y


def test_plan_accepts_a_runner_change_with_explicit_matching_egress(tmp_path: Path) -> None:
    plan = _plan()
    for entry in plan["turns"][4]["worker_pools"].values():
        entry["egress"] = []
    assert _load(tmp_path, plan)["turns"][4]["worker_pools"][REPO_X]["egress"] == []


def test_main_refuses_a_bad_plan_before_any_external_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    for name, value in _plan_env(tmp_path, {}).items():
        monkeypatch.setenv(name, value)

    def forbid_command(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("a malformed plan must launch no external command")

    monkeypatch.setattr(subprocess, "run", forbid_command)
    monkeypatch.setattr(subprocess, "Popen", forbid_command)
    assert fe.main(["run", "--scenario", "repo-runner", "--context", CONTEXT]) == fe.EXIT_CONFIG
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
    argv = ["run", "--scenario", "repo-runner", "--context", CONTEXT]
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


# --------------------------------------------------------------------------
# Scripted model (Decision 5)
# --------------------------------------------------------------------------


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
        "tools": [{"name": "Bash", "description": "shell", "input_schema": {"type": "object"}}],
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


@pytest.mark.parametrize(
    "output",
    [_passing()["turns"][0]["manifest_output"], "curie-no-tool-manifest\n"],
    ids=["manifest", "no-manifest"],
)
def test_scripted_model_issues_the_manifest_command_and_records_its_result(output: str) -> None:
    model = repo_runner.ScriptedManifestModel(host="127.0.0.1", port=0)
    model.start()
    try:
        model.begin_turn("fixture-turn")
        first = _first_model_body()
        status, response = _model_request(model, first)
        assert status == 200
        tool = next(item for item in response["content"] if item["type"] == "tool_use")
        assert tool["input"] == {"command": repo_runner.MANIFEST_COMMAND}
        result = _result_model_body(first, response)
        result["messages"][-1]["content"][0]["content"] = output
        status, final = _model_request(model, result)
        assert status == 200
        assert final["stop_reason"] == "end_turn"
        snapshot = model.snapshot()
        assert snapshot["model_requests"] == 2
        assert snapshot["manifest_output"] == output
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


# --------------------------------------------------------------------------
# Judge: samples, ordinary turns, manifests (retained contracts)
# --------------------------------------------------------------------------


@pytest.mark.parametrize(("sample", "passes"), [("passing.json", True), ("failing.json", False)])
def test_canonical_synthetic_samples(sample: str, passes: bool) -> None:
    observation = json.loads((FIXTURES / sample).read_text())["scenario"]
    assert "synthetic" in observation["provenance"]
    assert "not observed live evidence" in observation["provenance"]
    assert len(observation["turns"]) == 7
    failures = repo_runner.judge_repo_runner(observation)
    assert isinstance(failures, list)
    assert all(isinstance(failure, str) and failure for failure in failures)
    assert (failures == []) is passes


@pytest.mark.parametrize("turn", [0, 1, 4], ids=["x", "y", "x-on-y-runner"])
@pytest.mark.parametrize("digest_key", ["index", "linux/amd64", "linux/arm64"])
@pytest.mark.parametrize("identity_kind", ["docker-pullable", "containerd"])
def test_index_and_each_platform_digest_are_valid_runtime_image_identities(
    turn: int, digest_key: str, identity_kind: str
) -> None:
    observation = _passing()
    # The runner change runs X on Y's pool entry, so Y's image identities apply.
    repository = REPO_Y if turn == 4 else observation["turns"][turn]["repository"]
    config = observation["bindings"][repository]
    digest = (
        config["image"].split("@", 1)[1]
        if digest_key == "index"
        else config["platformDigests"][digest_key]
    )
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
    observation["turns"][0]["boot_log"] = "runner tool manifest loaded: 3 tools\n"
    assert repo_runner.judge_repo_runner(observation) == []


def test_manifest_must_include_a_binding_declared_probe() -> None:
    observation = _passing()
    observation["bindings"][REPO_X]["probes"] = [
        {"name": "acme-check", "command": ["acme-check", "--version"]}
    ]
    assert _mentions(repo_runner.judge_repo_runner(observation), "acme-check")


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
    observation["turns"][0]["boot_log"] = "runner tool manifest loaded: 3 tools\n"
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
        pytest.param(("turns", 2, "sandbox"), "", id="agent-turn-empty-sandbox"),
        pytest.param(("turns", 2, "claim"), "", id="agent-turn-empty-claim"),
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
        pytest.param(
            ("turns", 1, "image_id"),
            "containerd://" + "sha256:" + "b" * 64,
            id="y-runs-x-image",
        ),
        pytest.param(("turns", 0, "boot_log"), "", id="absent-boot-log"),
        pytest.param(("turns", 0, "boot_log"), "runner ready", id="no-manifest-boot-line"),
        pytest.param(
            ("turns", 0, "boot_log"),
            "runner tool manifest loaded: 1 tools\n",
            id="wrong-boot-count",
        ),
        pytest.param(
            ("turns", 0, "boot_log"),
            "runner tool manifest absent: tool_count=0\n",
            id="runner-turn-reports-absent-manifest",
        ),
        pytest.param(("turns", 0, "manifest_output"), "", id="empty-manifest-output"),
        pytest.param(
            ("turns", 0, "manifest_output"),
            "curie-no-tool-manifest",
            id="runner-turn-has-no-manifest",
        ),
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
        pytest.param(("turns", 2, "model_requests"), 1, id="agent-turn-one-model-request"),
        pytest.param(("turns", 2, "cli_exit_code"), 1, id="agent-turn-failed-cli-exit"),
        pytest.param(
            ("turns", 2, "observed_repository"), REPO_X, id="agent-turn-wrong-claim-repository"
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
        ("unconfigured",),
        ("worker_pools",),
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
        ("turns", 0, "runner_env"),
        ("turns", 2, "runner_env"),
        ("turns", 2, "agent_image"),
        ("turns", 2, "warm_pool"),
        ("turns", 2, "boot_log"),
        ("turns", 2, "manifest_output"),
        ("turns", 4, "retired_claims"),
        ("turns", 4, "prior_claim_present"),
        ("turns", 4, "runner_env"),
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
    "path",
    [("schema_version",), ("tools",), ("docker_cli",)]
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
    observation["turns"][0]["boot_log"] = "runner tool manifest loaded: 3 tools\n"
    assert repo_runner.judge_repo_runner(observation)


@pytest.mark.parametrize("turn", range(7))
def test_each_planned_case_is_required(turn: int) -> None:
    observation = _passing()
    del observation["turns"][turn]
    assert repo_runner.judge_repo_runner(observation)


@pytest.mark.parametrize("turn", range(7))
@pytest.mark.parametrize("errors", [None, ["unexpected model request"]])
def test_every_turn_requires_explicit_empty_error_evidence(turn: int, errors: Any) -> None:
    observation = _passing()
    if errors is None:
        del observation["turns"][turn]["errors"]
    else:
        observation["turns"][turn]["errors"] = errors
    assert repo_runner.judge_repo_runner(observation)


@pytest.mark.parametrize(
    "turn", [3, 5, 6], ids=["selection-conflict", "missing-pool", "stale-base"]
)
@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("status", "completed", id="completion-does-not-prove-refusal"),
        pytest.param("reason", "some_other_refusal", id="wrong-refusal-reason"),
        pytest.param("reason", "", id="absent-refusal-reason"),
        pytest.param("sandbox", "fallback-agent-sandbox", id="refusal-created-sandbox"),
        pytest.param("claim", "fallback-agent-claim", id="refusal-created-claim-id"),
        pytest.param("pod_uid", "fallback-pod", id="refusal-created-pod"),
        pytest.param("image_id", "containerd://sha256:" + "e" * 64, id="refusal-used-agent-image"),
        pytest.param("spec_image", AGENT_PINNED, id="refusal-recorded-spec-image"),
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


@pytest.mark.parametrize(
    "turn", [3, 5, 6], ids=["selection-conflict", "missing-pool", "stale-base"]
)
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
        (5, "missing_pool_absent", False),
        (6, "installed_base", ""),
        (6, "configured_base", "sha256:" + "e" * 64),
    ],
)
def test_each_refusal_requires_observed_fault_evidence(turn: int, field: str, value: Any) -> None:
    observation = _passing()
    observation["turns"][turn][field] = value
    assert _mentions(repo_runner.judge_repo_runner(observation), f"turn {turn + 1}")


@pytest.mark.parametrize(
    ("turn", "field"), [(5, "missing_pool_absent"), (6, "installed_base"), (6, "configured_base")]
)
def test_each_refusal_fault_field_is_required(turn: int, field: str) -> None:
    observation = _passing()
    del observation["turns"][turn][field]
    assert repo_runner.judge_repo_runner(observation)


@pytest.mark.parametrize("turn", [5, 6], ids=["missing-pool", "stale-base"])
@pytest.mark.parametrize("repository", ["acme-corp/unobserved-repository", REPO_U])
def test_fault_repository_must_be_a_runner_repository_that_completed(
    turn: int, repository: str
) -> None:
    observation = _passing()
    observation["turns"][turn]["repository"] = repository
    assert repo_runner.judge_repo_runner(observation)


@pytest.mark.parametrize("turn", [0, 2, 3, 4])
def test_failed_turn_preserves_its_diagnostic_code(turn: int) -> None:
    observation = _passing()
    observation["turns"][turn].update(status="failed", reason="workspace.selection_conflict")
    failures = repo_runner.judge_repo_runner(observation)
    assert _mentions(failures, "'failed'"), failures
    assert _mentions(failures, "workspace.selection_conflict"), failures


# --------------------------------------------------------------------------
# Decision 11 and help: public entry points
# --------------------------------------------------------------------------


def test_preflight_release_assignment_selects_the_publication_namespace(tmp_path: Path) -> None:
    config = dataclasses.replace(_preflight_config(tmp_path), model_api_key=None)
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
    out = " ".join(capsys.readouterr().out.split()).lower()
    assert "repo-runner" in out
    assert "three" in out and "binding" in out
    assert "seven" in out and "turn" in out
    assert "same-thread repository" not in out
    assert "four turns" not in out


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
def test_standalone_judge_main_returns_the_sample_verdict(
    sample: str, code: int, capsys: pytest.CaptureFixture[str]
) -> None:
    assert repo_runner.main(["judge", str(FIXTURES / sample)]) == code
    verdict = json.loads(capsys.readouterr().out)
    assert verdict["verdict"] == ("passed" if code == 0 else "failed")


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


# --------------------------------------------------------------------------
# The canonical pool schema's conditional daemon contract (ADR 0217)
# --------------------------------------------------------------------------

_DAEMON = json.loads(
    (Path(__file__).resolve().parents[3] / "tests/vectors/daemon-config.json").read_text()
)["daemon"]


@pytest.mark.parametrize(
    ("docker", "daemon", "valid"),
    [
        (False, None, True),
        (True, _DAEMON, True),
        (True, None, False),
        (False, _DAEMON, False),
        (True, {**_DAEMON, "maxClaimLifetimeSeconds": 0}, False),
    ],
    ids=["plain", "docker-with-daemon", "docker-missing-daemon", "plain-with-daemon", "zero-life"],
)
def test_harness_schema_interpreter_matches_the_conditional_daemon_contract(
    docker: bool, daemon: dict[str, Any] | None, valid: bool
) -> None:
    schema = repo_runner._schema_resource("apps/worker/schema/repo-runner-pools.schema.json")
    entry: dict[str, Any] = {
        "pool": "acme-api-runner",
        "digests": ["sha256:" + c * 64 for c in "abc"],
        "base": "sha256:" + "d" * 64,
        "docker": docker,
        "egress": [],
    }
    if daemon is not None:
        entry["daemon"] = daemon
    failures = repo_runner._schema_failures({REPO_X: entry}, schema, schema, "pools")
    assert (failures == []) is valid, failures
