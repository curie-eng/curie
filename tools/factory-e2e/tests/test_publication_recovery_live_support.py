"""The #5429 AC3 driver's inputs, install values and judgements, without a cluster.

The live driver (test_factory_publication_recovery_live.py) runs only on a
caller's throwaway cluster. These checks pin what it refuses before touching
anything, the values it installs, and that each judgement fails on the
outcome it exists to catch and passes on the shape a correct recovery leaves.
"""

from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import candidate_install as ci
import publication_recovery_live as prl
import pytest
import tls_code_host as tch

ROOT = Path(__file__).resolve().parents[3]
DRIVER = "tools/factory-e2e/tests/test_factory_publication_recovery_live.py"
ORIGINAL = "11111111-1111-1111-1111-111111111111"
SUCCESSOR = "22222222-2222-2222-2222-222222222222"
CANDIDATE = "33333333-3333-3333-3333-333333333333"
BASE_COMMIT = "a" * 40
PUSHED = "b" * 40
BRANCH = "curie/publication-0123"


# --------------------------------------------------------------------------
# Inputs and refusal before anything is touched
# --------------------------------------------------------------------------


def _complete_env(tmp_path: Path) -> dict[str, str]:
    cli = tmp_path / "curie"
    cli.write_text("#!/bin/sh\nexit 0\n")
    cli.chmod(0o755)
    return {
        prl.FLAG: "1",
        "CURIE_FACTORY_KUBE_CONTEXT": "throwaway",
        prl.NAMES.namespace: "pubrec-1",
        prl.NAMES.release: "curie-pubrec",
        prl.NAMES.pull_policy: "IfNotPresent",
        "CURIE_C4_CANDIDATE_CLI": str(cli),
        "CURIE_C4_CANDIDATE_IMAGES": "registry.test/curie-{component}:cand",
    }


def test_inputs_read_back_from_a_complete_environment(tmp_path: Path) -> None:
    inputs = ci.read_inputs(_complete_env(tmp_path), prl.NAMES)
    assert inputs.namespace == "pubrec-1" and inputs.release == "curie-pubrec"
    assert inputs.images == {
        c: f"registry.test/curie-{c}:cand" for c in ("api", "worker", "runner")
    }
    assert inputs.publication_namespace == "pubrec-1-curie-pubrec-publication"


def test_every_missing_input_is_named_in_one_refusal() -> None:
    with pytest.raises(ci.Refused) as refused:
        ci.read_inputs({prl.FLAG: "1"}, prl.NAMES)
    message = str(refused.value)
    for name in (
        "CURIE_FACTORY_KUBE_CONTEXT",
        prl.NAMES.namespace,
        prl.NAMES.release,
        prl.NAMES.pull_policy,
        "CURIE_C4_CANDIDATE_CLI",
        "CURIE_C4_CANDIDATE_IMAGES",
    ):
        assert name in message, name
    assert message.startswith(f"{prl.FLAG}=1 needs every caller input")


def test_the_release_name_curie_is_refused(tmp_path: Path) -> None:
    env = {**_complete_env(tmp_path), prl.NAMES.release: "curie"}
    with pytest.raises(ci.Refused, match="never curie"):
        ci.read_inputs(env, prl.NAMES)


def _run_driver(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    clean = {k: v for k, v in os.environ.items() if not k.startswith("CURIE_")}
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "--reruns", "0", DRIVER],
        cwd=ROOT,
        env={**clean, **env},
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )


def test_the_driver_skips_without_its_flag() -> None:
    result = _run_driver({})
    assert result.returncode == 5 or " skipped" in result.stdout, result.stdout[-2000:]
    assert "passed" not in result.stdout and "error" not in result.stdout.lower()


def test_the_driver_errors_naming_its_inputs_before_touching_anything() -> None:
    # No kube context, so nothing could be created even if it tried.
    result = _run_driver({prl.FLAG: "1"})
    assert result.returncode != 0, result.stdout[-2000:]
    assert "1 error" in result.stdout, result.stdout[-2000:]
    assert f"{prl.FLAG}=1 needs every caller input" in result.stdout
    for name in ("CURIE_FACTORY_KUBE_CONTEXT", prl.NAMES.namespace, "CURIE_C4_CANDIDATE_IMAGES"):
        assert name in result.stdout, name


def test_a_flag_other_than_one_is_an_error_not_a_run() -> None:
    result = _run_driver({prl.FLAG: "yes"})
    assert result.returncode != 0
    assert f"{prl.FLAG} is set but not 1" in result.stdout, result.stdout[-2000:]


# --------------------------------------------------------------------------
# Install values and the proxy
# --------------------------------------------------------------------------


STUB_IP, PROXY_IP = "10.42.0.7", "10.42.0.9"
STUB = "https://github-stub.ns.svc.cluster.local:8443"
PROXY = "https://forge-fault-proxy.ns.svc.cluster.local:8443"


def _base() -> dict[str, Any]:
    """Base values as the harness builds them: the stub's own TLS values."""

    return tch.tls_values(
        {
            "agentSandbox": {"runner": {"fakeModel": True}},
            "factory": {
                # As base_values leaves them: the identity host is the stub's authority.
                "codeHosts": [{"name": "github", "kind": "github", "host": STUB[8:]}],
                "trackers": [{"name": "github", "kind": "github", "host": STUB[8:]}],
                "sandboxEgress": {"factory": True},
            },
        },
        namespace="ns",
        ca_config_map=tch.CA_CONFIG_MAP,
        stub_cluster_ip=STUB_IP,
        port=ci.STUB_PORT,
        model_cidrs=(),
    )


def _egress_cidrs(values: dict[str, Any]) -> list[str]:
    rules = [
        *values["security"]["networkPolicy"]["allowedEgress"],
        *(rule for host in values["factory"]["codeHosts"] for rule in host["egress"]),
    ]
    return [rule["cidr"] for rule in rules]


def test_recovery_values_route_every_forge_address_through_the_proxy() -> None:
    base = _base()
    assert base["api"]["githubApiUrl"] == f"{STUB}/api/v3", "the base fixture is not the stub's"
    assert set(_egress_cidrs(base)) == {f"{STUB_IP}/32"}
    values = prl.recovery_values(base, namespace="ns", stub_ip=STUB_IP, proxy_ip=PROXY_IP)
    assert base == _base(), "the base values were mutated"
    assert values["api"]["githubApiUrl"] == f"{PROXY}/api/v3"
    assert values["api"]["githubCloneBase"] == PROXY
    assert values["factory"]["codeHosts"][0]["apiUrl"] == f"{PROXY}/api/v3"
    assert values["factory"]["trackers"][0]["apiUrl"] == f"{PROXY}/api/v3"
    for entry in (*values["factory"]["codeHosts"], *values["factory"]["trackers"]):
        assert entry["host"] == PROXY.removeprefix("https://")
    assert set(_egress_cidrs(values)) == {f"{PROXY_IP}/32"}
    assert STUB not in json.dumps(values) and STUB_IP not in json.dumps(values)
    # Everything else the stub values set is kept.
    assert values["codeHostTrust"] == base["codeHostTrust"]
    assert values["api"]["githubRepoAllowlist"] == [tch.STUB_REPO]
    assert values["agentSandbox"]["runner"]["workspace"] == {"enabled": True}
    assert values["agentSandbox"]["runner"]["fakeModel"] is True
    assert values["worker"]["workspace"]["publicationCandidates"] == {"enabled": True}
    assert values["worker"]["publication"]["reconcileMaxAttempts"] == prl.RECONCILE_MAX_ATTEMPTS
    directive = values["agentSandbox"]["runnerEnv"][prl.AGENT]["FAKE_MODEL_DIRECTIVE"]
    assert directive.startswith(f"[fake:write-file {prl.CHANGED_FILE} ")
    assert values["factory"]["sandboxEgress"] == {"factory": True, prl.AGENT: True}


def test_recovery_values_refuse_a_base_whose_api_base_is_not_the_stub() -> None:
    base = _base()
    base["api"]["githubApiUrl"] = "https://elsewhere.example/api/v3"
    with pytest.raises(AssertionError, match="api.githubApiUrl"):
        prl.recovery_values(base, namespace="ns", stub_ip=STUB_IP, proxy_ip=PROXY_IP)


def test_recovery_values_swap_only_the_exact_stub_cidr() -> None:
    base = _base()
    base["security"]["networkPolicy"]["allowedEgress"].append(
        {"cidr": f"1{STUB_IP}/32", "ports": [{"protocol": "TCP", "port": 443}]}
    )
    values = prl.recovery_values(base, namespace="ns", stub_ip=STUB_IP, proxy_ip=PROXY_IP)
    assert sorted(_egress_cidrs(values)) == sorted(
        [f"{PROXY_IP}/32", f"{PROXY_IP}/32", f"1{STUB_IP}/32"]
    )


def test_the_proxy_pod_forwards_to_the_stub_with_its_ca_and_holds_no_token() -> None:
    owner = {"owner": "factory-tools"}
    manifests = prl.proxy_manifests("ns", "registry.test/curie-worker:c", "Always", owner=owner)
    by_kind = {m["kind"]: m for m in manifests}
    assert set(by_kind) == {"ConfigMap", "Pod", "Service"}
    spec = by_kind["Pod"]["spec"]
    assert spec["automountServiceAccountToken"] is False
    container = spec["containers"][0]
    args = container["args"]
    assert args[args.index("--upstream") + 1] == STUB
    assert "--prefix" not in args
    assert args[args.index("--upstream-ca") + 1] == "/upstream/ca.crt"
    assert args[args.index("--host") + 1] == "forge-fault-proxy.ns.svc.cluster.local"
    assert container["imagePullPolicy"] == "Always"
    assert "def take_fault" in by_kind["ConfigMap"]["data"]["forge_fault_proxy.py"]
    assert by_kind["Service"]["spec"]["clusterIP"] == "None"


def test_proxy_log_and_ref_parsing() -> None:
    pulls = "/api/v3/repos/acme-corp/acme-bot/pulls"
    rows = [
        {"event": "startup", "base_url": "https://p:8443", "ca_file": "/state/ca.pem"},
        {"event": "request", "method": "GET", "path": pulls, "status": 200, "fault": False},
        {"event": "request", "method": "POST", "path": pulls, "status": 502, "fault": True},
        {"event": "request", "method": "POST", "path": pulls, "status": 201, "fault": False},
        {"event": "request", "method": "POST", "path": pulls + "/1", "status": 200, "fault": False},
    ]
    lines = [json.dumps(row) for row in rows]
    log = "\n".join([*lines[:3], "not json", *lines[3:]])
    assert prl.pull_creates(log) == [(502, True), (201, False)]
    refs = f"refs/heads/main {BASE_COMMIT}\nrefs/heads/{BRANCH} {PUSHED}\nrefs/tags/x {PUSHED}\n"
    assert prl.branch_heads(refs) == {"main": BASE_COMMIT, BRANCH: PUSHED}


# --------------------------------------------------------------------------
# Judgements
# --------------------------------------------------------------------------


def _original() -> dict[str, Any]:
    return {
        "id": ORIGINAL,
        "execution_kind": "implementation",
        "status": "failed",
        "terminal_cause": "publication_failed",
        "runtime_epoch": 1,
        "runtime_claim_name": "claim-a",
        "started_at": "2026-11-16T08:00:00+00:00",
        "terminal_at": "2026-11-16T08:05:00.5+00:00",
    }


def _publication(request: str, status: str, attempts: int, **extra: Any) -> dict[str, Any]:
    return {
        "id": f"pub-{request[:4]}",
        "execution_request_id": request,
        "status": status,
        "reconcile_attempts": attempts,
        "branch": BRANCH,
        "base_sha": BASE_COMMIT,
        "title_sha256": "t" * 64,
        "body_sha256": "d" * 64,
        "pr_number": None,
        **extra,
    }


FAILED_CREATES = [(prl.FAULT_STATUS, True)] * prl.RECONCILE_MAX_ATTEMPTS


def test_judge_original_accepts_the_injected_publication_failure() -> None:
    facts = prl.judge_original(
        _original(), [_publication(ORIGINAL, "failed", prl.RECONCILE_MAX_ATTEMPTS)], FAILED_CREATES
    )
    assert facts["terminal_cause"] == "publication_failed"
    assert facts["injected_pull_creates"] == prl.RECONCILE_MAX_ATTEMPTS


@pytest.mark.parametrize(
    ("change", "publications", "creates", "message"),
    [
        (
            {"status": "expired", "terminal_cause": "execution_deadline"},
            None,
            None,
            "expected failed",
        ),
        ({"terminal_cause": "publication_refused"}, None, None, "publication_refused"),
        (
            {},
            [_publication(ORIGINAL, "failed", prl.RECONCILE_MAX_ATTEMPTS + 1)],
            None,
            "reconcile attempts",
        ),
        ({}, [], None, "0 publications"),
        ({}, None, [(201, False), *FAILED_CREATES], "reached the forge before"),
        ({}, None, FAILED_CREATES[:1], "injected"),
    ],
)
def test_judge_original_refuses_any_other_ending(
    change: dict[str, Any],
    publications: list[dict[str, Any]] | None,
    creates: list[tuple[int, bool]] | None,
    message: str,
) -> None:
    with pytest.raises(AssertionError, match=message):
        prl.judge_original(
            {**_original(), **change},
            publications
            if publications is not None
            else [_publication(ORIGINAL, "failed", prl.RECONCILE_MAX_ATTEMPTS)],
            creates if creates is not None else FAILED_CREATES,
        )


def _candidate() -> dict[str, Any]:
    return {
        "candidate_id": CANDIDATE,
        "status": "complete",
        "available": True,
        "patch": {"sha256": "1" * 64, "size_bytes": 120},
        "base": {"sha256": "2" * 64, "size_bytes": 4096, "base_commit": BASE_COMMIT},
        "changed_path_count": 1,
    }


def _usage(extra_tokens: int = 0) -> dict[str, Any]:
    return {
        "total_tokens": 28 + extra_tokens,
        "models": [{"model": "fake", "role": "main", "input_tokens": 20, "output_tokens": 8}],
        "requests": [{"request_id": ORIGINAL, "tokens": 28 + extra_tokens}],
    }


def _recovered(**override: Any) -> dict[str, Any]:
    successor = {
        "id": SUCCESSOR,
        "retry_of_request_id": ORIGINAL,
        "execution_kind": "publication_recovery",
        "status": "completed",
        "terminal_cause": "completed",
        "runtime_epoch": 1,
        "runtime_claim_name": "claim-b",
        "started_at": "2026-11-16T08:06:00+00:00",
        "model_session_reserved_at": None,
        "recovery_candidate_id": CANDIDATE,
        "recovery_candidate_sha256": "c" * 64,
    }
    kwargs: dict[str, Any] = {
        "requests": [_original(), successor],
        "original": _original(),
        "candidate": _candidate(),
        "candidate_row": {
            "content_sha256": "c" * 64,
            "origin_request_id": ORIGINAL,
            "owner_request_id": SUCCESSOR,
            "title_sha256": "t" * 64,
            "body_sha256": "d" * 64,
        },
        "candidate_after": _candidate(),
        "publications": [
            _publication(ORIGINAL, "failed", prl.RECONCILE_MAX_ATTEMPTS),
            _publication(SUCCESSOR, "succeeded", 0, pr_number=1),
        ],
        "heads_after_failure": {"main": BASE_COMMIT, BRANCH: PUSHED},
        "heads_after_recovery": {"main": BASE_COMMIT, BRANCH: PUSHED},
        "pulls": [{"number": 1, "head": {"ref": BRANCH, "sha": PUSHED}}],
        "creates": [*FAILED_CREATES, (201, False)],
        "usage_at_capture": _usage(),
        "usage_after": _usage(),
        "default_branch": "main",
    }
    for key, value in override.items():
        if key == "successor":
            kwargs["requests"] = [_original(), {**successor, **value}]
        else:
            kwargs[key] = value
    return kwargs


def test_judge_recovery_accepts_one_identical_model_free_publication() -> None:
    facts = prl.judge_recovery(**_recovered())
    assert facts["execution_kind"] == "publication_recovery"
    assert facts["pushed_commit"] == PUSHED and facts["pull_requests"] == 1
    assert facts["tokens_at_capture"] == facts["tokens_after"] == 28


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"successor": {"execution_kind": "implementation"}}, "implementation"),
        ({"successor": {"status": "failed"}}, "expected completed"),
        ({"successor": {"runtime_claim_name": "claim-a"}}, "reused the original"),
        ({"successor": {"started_at": "2026-11-16T08:04:00+00:00"}}, "before the original"),
        ({"successor": {"recovery_candidate_id": "x"}}, "different candidate"),
        ({"successor": {"recovery_candidate_sha256": "e" * 64}}, "digest differs"),
        ({"successor": {"model_session_reserved_at": "2026-11-16T08:06:01Z"}}, "model session"),
        (
            {"candidate_after": {**_candidate(), "patch": {"sha256": "9" * 64, "size_bytes": 1}}},
            "objects changed",
        ),
        ({"candidate_after": None}, "no longer listed"),
        (
            {
                "publications": [
                    _publication(ORIGINAL, "failed", 2),
                    _publication(SUCCESSOR, "succeeded", 0, pr_number=1, base_sha="f" * 40),
                ]
            },
            "another base",
        ),
        (
            {
                "publications": [
                    _publication(ORIGINAL, "failed", 2),
                    _publication(SUCCESSOR, "succeeded", 0, pr_number=1, title_sha256="0" * 64),
                ]
            },
            "title",
        ),
        (
            {"heads_after_recovery": {"main": BASE_COMMIT, BRANCH: "c" * 40}},
            "adopted, not rewritten",
        ),
        (
            {"heads_after_recovery": {"main": BASE_COMMIT, BRANCH: PUSHED, "other": PUSHED}},
            "changed the forge branches",
        ),
        (
            {
                "pulls": [
                    {"number": 1, "head": {"ref": BRANCH, "sha": PUSHED}},
                    {"number": 2, "head": {"ref": BRANCH, "sha": PUSHED}},
                ]
            },
            "2 pull requests",
        ),
        ({"creates": [*FAILED_CREATES, (201, False), (201, False)]}, "reached the forge"),
        ({"usage_after": _usage(extra_tokens=5)}, "usage changed"),
        (
            {
                "usage_at_capture": {
                    **_usage(),
                    "requests": [{"request_id": SUCCESSOR, "tokens": 3}],
                },
                "usage_after": {**_usage(), "requests": [{"request_id": SUCCESSOR, "tokens": 3}]},
            },
            "recorded model tokens",
        ),
    ],
)
def test_judge_recovery_refuses_each_broken_recovery(
    override: dict[str, Any], message: str
) -> None:
    with pytest.raises(AssertionError, match=message):
        prl.judge_recovery(**_recovered(**override))


def test_judge_recovery_refuses_a_second_successor() -> None:
    kwargs = _recovered()
    third = copy.deepcopy(kwargs["requests"][1])
    third["id"] = "44444444-4444-4444-4444-444444444444"
    kwargs["requests"] = [*kwargs["requests"], third]
    with pytest.raises(AssertionError, match="3 requests"):
        prl.judge_recovery(**kwargs)
