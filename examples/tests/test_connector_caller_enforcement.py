"""Black box coverage for the connector caller proxy enforcement gate.

ADR-0168 decision 7. The gate runs against an enforcing cluster in CI; here a
fake kubectl answers as that cluster would, so each leg can be shown failing
on its own.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "check-connector-caller-enforcement.sh"
SERVICE = "curie-weather-mcp-netpol-probe"
POD_IP = "192.0.2.20"
# A stand-in for the store password the fake Secret holds; the gate must
# never print it or put it on an argv.
LEAK_SENTINEL = "leak-sentinel-for-the-grant-store-5550"
STORE_POD = "curie-valkey-0"

_FAKE = """#!/usr/bin/env python3
import base64, json, os, sys

args = sys.argv[1:]
with open(os.environ["FAKE_KUBECTL_LOG"], "a", encoding="utf-8") as stream:
    stream.write(json.dumps(args) + "\\n")
env = os.environ.get

if "delete" in args or "apply" in args or "wait" in args or "rollout" in args:
    raise SystemExit(0)
if "get" in args and "svc" in args:
    if "-l" in args:
        # With a proxy every connector has two Services: the one named after
        # it and its direct one. Both carry the connector's name label.
        names = [n for n in env("FAKE_CONNECTORS", "").split() if n]
        by_label = "labels" in args[-1]
        for name in names:
            print(name)
            print(name if by_label else f"{name}-direct")
        raise SystemExit(0)
    jsonpath = args[-1]
    print(env("FAKE_TARGET_PORT", "caller") if "targetPort" in jsonpath else "8000", end="")
    raise SystemExit(0)
if "get" in args and "secret" in args:
    password = env("FAKE_VALKEY_PASSWORD", "")
    encoded = base64.b64encode(password.encode()).decode()
    print(json.dumps({"kind": "Secret", "data": {"valkeyPassword": encoded}}))
    raise SystemExit(0)
if "get" in args and "deployments,pods,replicasets" in args:
    password = env("FAKE_VALKEY_PASSWORD", "")
    literal = env("FAKE_LITERAL_IN", "")
    svc = env("FAKE_CONNECTORS", "").split()[0]
    ref = {"name": "curie-secrets", "key": "valkeyPassword"}
    if env("FAKE_PROXY_REF") == "wrong-secret":
        ref = {"name": "other-secret", "key": "valkeyPassword"}
    proxy_env = [
        {"name": "CURIE_CALLER_PROXY_GRANT_STORE", "value": "redis://curie-valkey:6379/0"},
        {"name": "CURIE_CALLER_PROXY_GRANT_STORE_PASSWORD", "valueFrom": {"secretKeyRef": ref}},
    ]
    if env("FAKE_PROXY_REF") == "missing":
        proxy_env = proxy_env[:1]
    def leak(kind):
        return [{"name": "LEAKED", "value": password}] if literal == kind else []
    def pod_spec(env_list, name="server"):
        return {"containers": [{"name": name, "env": env_list}]}
    containers = [
        {"name": "caller-proxy", "env": proxy_env},
        {"name": "server", "env": leak("deployment")},
    ]
    items = [
        {
            "kind": "Deployment",
            "metadata": {"name": svc},
            "spec": {"replicas": 1, "template": {"spec": {"containers": containers}}},
        },
        {
            "kind": "Pod",
            "metadata": {"name": svc + "-abc12"},
            "spec": {"containers": [{"name": "caller-proxy", "env": proxy_env + leak("pod")}]},
        },
        {
            "kind": "ReplicaSet",
            "metadata": {"name": svc + "-live"},
            "spec": {"replicas": 1, "template": {"spec": pod_spec(leak("replicaset"))}},
        },
    ]
    if literal == "stale-replicaset":
        items.append(
            {
                "kind": "ReplicaSet",
                "metadata": {"name": svc + "-old"},
                "spec": {
                    "replicas": 0,
                    "template": {"spec": pod_spec([{"name": "OLD", "value": password}])},
                },
            }
        )
    print(json.dumps({"kind": "List", "items": items}))
    raise SystemExit(0)
selector = args[args.index("-l") + 1] if "-l" in args else ""
if "get" in args and "pod" in args and "component=valkey" in selector:
    print("curie-valkey-0", end="")
    raise SystemExit(0)
if "get" in args and "deployment" in args and "component=api" in selector:
    print("curie-api", end="")
    raise SystemExit(0)
if "get" in args and "deployment" in args and "VALKEY_PASSWORD" in args[-1]:
    print("curie-secrets valkeyPassword", end="")
    raise SystemExit(0)
if "get" in args and "pod" in args:
    print("192.0.2.20", end="")
    raise SystemExit(0)
if "get" in args and "deployment" in args:
    if "CURIE_CALLER_PROXY_ADMITS" in args[-1]:
        print(env("FAKE_ADMITS", '["weather"]'), end="")
        raise SystemExit(0)
    print("8000", end="")
    raise SystemExit(0)
if "exec" in args:
    command = args[args.index("--") + 1 :]
    pod = args[args.index("exec") + 1]
    if "other-agent" in pod:
        raise SystemExit(0 if env("FAKE_OTHER_AGENT_REACHES", "0") == "1" else 1)
    if command[0] == "sh" and "valkey" in args:
        state = env("FAKE_STATE_FILE")
        spent = json.load(open(state)) if os.path.exists(state) else []
        found = command[-1] in spent and env("FAKE_KEY_MISSING") != "1"
        print("1" if found else "0", end="")
        raise SystemExit(0)
    if command[0] == "python" and len(command) > 3:
        state = env("FAKE_STATE_FILE")
        spent = json.load(open(state)) if os.path.exists(state) else []
        grant, count, wrong = command[3], int(command[4]), len(command) > 5
        mode = env("FAKE_SPEND", "")
        for _ in range(count):
            if wrong:
                result = mode == "wrong-spends"
            elif mode == "none":
                result = False
            elif mode == "twice":
                result = True
            else:
                result = grant not in spent
            if result:
                spent.append(grant)
            print(result)
        json.dump(spent, open(state, "w"))
        raise SystemExit(0)
    if command[0] == "python":
        raise SystemExit(0 if env("FAKE_SERVER_LISTENS", "1") == "1" else 1)
    url = command[-1]
    if url.startswith("http://192.0.2.20:"):
        raise SystemExit(0 if env("FAKE_DIRECT_REACHABLE") == "1" else 28)
    if "-H" in command:
        token = command[command.index("-H") + 1].split(": ", 1)[1]
        if token == "cct.minted-for.weather":
            print(env("FAKE_ADMITTED_CODE", "400"), end="")
        else:
            body = env(
                "FAKE_OUTSIDER_BODY",
                '{"jsonrpc": "2.0", "error": {"data": {"curie_caller": "not_admitted"}}}',
            )
            print(body + "\\n" + env("FAKE_OUTSIDER_CODE", "403"), end="")
        raise SystemExit(0)
    print(env("FAKE_CODE", "403"), end="")
    raise SystemExit(0)
raise SystemExit(90)
"""


# Mints what the kind lane's worker image would: a token naming the agent it is
# asked for, signed with the seed it reads from its env and never its argv.
_FAKE_DOCKER = """#!/usr/bin/env python3
import json, os, sys

args = sys.argv[1:]
with open(os.environ["FAKE_DOCKER_LOG"], "a", encoding="utf-8") as stream:
    stream.write(json.dumps(args) + "\\n")
if os.environ.get("FAKE_MINT_FAILS") == "1":
    raise SystemExit(1)
if not os.environ.get("CURIE_CALLER_SIGNING_KEY"):
    raise SystemExit(1)
print("cct.minted-for." + args[-1])
"""

_SEED = "seed-that-must-never-reach-an-argv"


def _run(
    tmp_path: Path, *, seed: str | None = _SEED, **fake: str
) -> tuple[subprocess.CompletedProcess[str], list[list[str]]]:
    kubectl = tmp_path / "kubectl"
    kubectl.write_text(_FAKE, encoding="utf-8")
    kubectl.chmod(0o755)
    docker = tmp_path / "docker"
    docker.write_text(_FAKE_DOCKER, encoding="utf-8")
    docker.chmod(0o755)
    log = tmp_path / "kubectl.jsonl"
    env = {
        **os.environ,
        "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}",
        "FAKE_KUBECTL_LOG": str(log),
        "FAKE_DOCKER_LOG": str(tmp_path / "docker.jsonl"),
        "FAKE_CONNECTORS": SERVICE,
        "FAKE_VALKEY_PASSWORD": LEAK_SENTINEL,
        "FAKE_STATE_FILE": str(tmp_path / "spent.json"),
        **fake,
    }
    env.pop("CURIE_CALLER_SIGNING_KEY", None)
    if seed is not None:
        env["CURIE_CALLER_SIGNING_KEY"] = seed
    result = subprocess.run(
        ["bash", str(SCRIPT), "curie", "curie", "curie"],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    calls = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    return result, calls


def _direct_dials(calls: list[list[str]]) -> list[list[str]]:
    return [c for c in calls if "exec" in c and any(a.startswith(f"http://{POD_IP}:") for a in c)]


# @spec ADR-0168 d7
def test_an_enforcing_proxy_passes_every_leg(tmp_path: Path) -> None:
    result, calls = _run(tmp_path)
    assert result.returncode == 0, result.stderr
    assert "lands on the caller proxy" in result.stdout
    assert "cannot reach" in result.stdout
    assert len(_direct_dials(calls)) == 1


# @spec ADR-0168 d7
def test_no_connector_service_is_fatal(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, FAKE_CONNECTORS="")
    assert result.returncode != 0
    assert "vacuous" in result.stderr


# @spec ADR-0168 d7
def test_a_service_still_targeting_the_server_fails(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, FAKE_TARGET_PORT="http")
    assert result.returncode != 0
    assert "not the caller proxy" in result.stderr


# @spec ADR-0168 d7
@pytest.mark.parametrize("code", ["200", "000", "401"])
def test_a_tokenless_call_anything_but_the_proxy_answers_fails(tmp_path: Path, code: str) -> None:
    result, _ = _run(tmp_path, FAKE_CODE=code)
    assert result.returncode != 0
    assert "not the caller proxy's 403" in result.stderr


# @spec ADR-0168 d7
def test_a_server_that_does_not_listen_stops_before_the_direct_leg(tmp_path: Path) -> None:
    result, calls = _run(tmp_path, FAKE_SERVER_LISTENS="0")
    assert result.returncode != 0
    assert "would prove nothing" in result.stderr
    assert _direct_dials(calls) == []


# @spec ADR-0168 d7
def test_a_sandbox_reaching_the_server_port_directly_fails(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, FAKE_DIRECT_REACHABLE="1")
    assert result.returncode != 0
    assert "past the caller proxy" in result.stderr


def _token_dials(calls: list[list[str]]) -> list[str]:
    return [c[c.index("-H") + 1] for c in calls if "exec" in c and "-H" in c]


# @spec ADR-0168 d7
def test_each_connector_is_probed_once_whatever_services_front_it(tmp_path: Path) -> None:
    result, calls = _run(tmp_path)
    assert result.returncode == 0, result.stderr
    assert f"{SERVICE}-direct" not in json.dumps(calls)
    assert result.stdout.count("lands on the caller proxy") == 1


# @spec ADR-0168 d7
def test_an_admitted_caller_reaches_the_server_and_an_outsider_does_not(tmp_path: Path) -> None:
    result, calls = _run(tmp_path)
    assert result.returncode == 0, result.stderr
    assert "admits a caller token for weather" in result.stdout
    assert "refuses an agent it does not admit" in result.stdout
    dialled = _token_dials(calls)
    assert dialled[0] == "X-Curie-Caller: cct.minted-for.weather"
    assert len(dialled) == 2 and "weather" not in dialled[1]
    # The seed rides the mint container's env by name, never an argv.
    assert _SEED not in (tmp_path / "docker.jsonl").read_text(encoding="utf-8")
    assert _SEED not in json.dumps(calls)


# @spec ADR-0168 d7
@pytest.mark.parametrize("code", ["403", "502", "000"])
def test_an_admitted_caller_that_does_not_reach_the_server_fails(tmp_path: Path, code: str) -> None:
    result, _ = _run(tmp_path, FAKE_ADMITTED_CODE=code)
    assert result.returncode != 0
    assert "did not reach the server" in result.stderr


# @spec ADR-0168 d7
@pytest.mark.parametrize(
    ("code", "body"),
    [("200", "{}"), ("403", '{"error": {"data": {"curie_caller": "invalid"}}}')],
    ids=["admitted", "wrong_refusal"],
)
def test_an_outsider_the_proxy_does_not_refuse_as_not_admitted_fails(
    tmp_path: Path, code: str, body: str
) -> None:
    result, _ = _run(tmp_path, FAKE_OUTSIDER_CODE=code, FAKE_OUTSIDER_BODY=body)
    assert result.returncode != 0
    assert "not_admitted" in result.stderr


# @spec ADR-0168 d7
def test_no_signing_key_is_fatal_before_any_admitted_leg(tmp_path: Path) -> None:
    result, calls = _run(tmp_path, seed=None)
    assert result.returncode != 0
    assert "CURIE_CALLER_SIGNING_KEY" in result.stderr
    assert _token_dials(calls) == []


# @spec ADR-0168 d7
def test_a_connector_admitting_nobody_is_not_an_admitted_leg(tmp_path: Path) -> None:
    result, calls = _run(tmp_path, FAKE_ADMITS="[]")
    assert result.returncode != 0
    assert "no connector admits an agent" in result.stderr
    assert _token_dials(calls) == []


# @spec #5550
def test_the_healthy_install_passes_legs_6_to_8(tmp_path: Path) -> None:
    result, calls = _run(tmp_path)
    assert result.returncode == 0, result.stderr
    for leg in ("leg 6", "leg 7", "leg 8"):
        assert f"ok  {leg}" in result.stdout
    assert any(STORE_POD in c for c in calls)


# @spec #5550
@pytest.mark.parametrize(
    ("where", "named"),
    [
        ("deployment", f"Deployment/{SERVICE}"),
        ("pod", f"Pod/{SERVICE}-abc12"),
        ("replicaset", f"ReplicaSet/{SERVICE}-live"),
    ],
)
def test_the_password_literal_in_a_live_workload_fails_leg_6(
    tmp_path: Path, where: str, named: str
) -> None:
    result, _ = _run(tmp_path, FAKE_LITERAL_IN=where)
    assert result.returncode != 0
    assert "leg 6:" in result.stderr
    assert named in result.stderr


# @spec #5550
def test_a_superseded_replicaset_holding_the_literal_is_reported_not_failed(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, FAKE_LITERAL_IN="stale-replicaset")
    assert result.returncode == 0, result.stderr
    assert "--  leg 6: superseded ReplicaSet" in result.stdout
    assert f"{SERVICE}-old" in result.stdout


# @spec #5550
@pytest.mark.parametrize("mode", ["missing", "wrong-secret"])
def test_a_proxy_not_reading_the_password_by_reference_fails_leg_6(
    tmp_path: Path, mode: str
) -> None:
    result, _ = _run(tmp_path, FAKE_PROXY_REF=mode)
    assert result.returncode != 0
    assert "leg 6:" in result.stderr


# @spec #5550
def test_an_empty_secret_key_fails_leg_6(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, FAKE_VALKEY_PASSWORD="")
    assert result.returncode != 0
    assert "leg 6:" in result.stderr
    assert "is empty" in result.stderr


# @spec #5550
@pytest.mark.parametrize(
    "fake",
    [{"FAKE_SPEND": "twice"}, {"FAKE_SPEND": "none"}, {"FAKE_KEY_MISSING": "1"}],
    ids=["double_spend", "no_spend", "key_missing"],
)
def test_a_grant_spend_that_misbehaves_fails_leg_7(tmp_path: Path, fake: dict[str, str]) -> None:
    result, _ = _run(tmp_path, **fake)
    assert result.returncode != 0
    assert "leg 7:" in result.stderr


# @spec #5550
def test_a_wrong_password_that_spends_fails_leg_8(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, FAKE_SPEND="wrong-spends")
    assert result.returncode != 0
    assert "leg 8:" in result.stderr


# @spec #5550
@pytest.mark.parametrize("fake", [{}, {"FAKE_LITERAL_IN": "pod"}], ids=["healthy", "leg_6_failure"])
def test_the_password_never_reaches_an_argv_or_the_output(
    tmp_path: Path, fake: dict[str, str]
) -> None:
    result, calls = _run(tmp_path, **fake)
    assert (result.returncode == 0) == (not fake)
    assert LEAK_SENTINEL not in json.dumps(calls)
    assert LEAK_SENTINEL not in result.stdout
    assert LEAK_SENTINEL not in result.stderr
