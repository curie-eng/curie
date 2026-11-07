"""The harness's generated artifacts, judged by the product code that consumes them.

The runtime document is loaded by the API's own initializer, the signed proof
is verified by the API's own verifier, the caller key pair is read by the
worker's own signer, and a token the worker mints is decoded with its
signature withheld.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

import identity_assets as assets
import identity_e2e as harness
import identity_scenarios
import identity_target as kube
import pytest
from curie_api.identity.http_initializer import load_http_identity_runtime
from curie_api.identity.http_invocation import HttpInvocationRuntime
from curie_internal import identity_context as codec
from curie_worker import caller_token
from nacl.signing import SigningKey

TENANT = "00000000-0000-0000-0000-000000000001"
SELECTION = {
    "mode": "http",
    "issuer": assets.ISSUER,
    "lookup_url": "https://identity-e2e-idp.test-namespace.svc:8443/lookup",
    "credential_ref": {"id": assets.CREDENTIAL_ID, "generation": 1},
    "max_identity_age_ms": 60_000,
    "max_membership_age_ms": 60_000,
    "max_clock_skew_ms": 5_000,
}


def registration(binding: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "receiver_id": str(uuid.uuid4()),
        "generation": 1,
        "tenant_id": TENANT,
        "channel_identity_id": str(uuid.uuid4()),
        "provider_installation_id": str(uuid.uuid4()),
        "binding_ids": [binding],
        "domains": [
            {
                "id": str(uuid.uuid4()),
                "authority": "",
                "kind": "email_domain",
                "key": "example.test",
            }
        ],
    }


def test_the_runtime_document_loads_in_the_api_initializer(tmp_path: Path) -> None:
    document = assets.runtime_document(
        tenant_id=TENANT,
        selection=SELECTION,
        bearer="synthetic-bearer",
        registration=registration(str(uuid.uuid4())),
        receiver_key=kube.b64url(os.urandom(32)),
        context_key=kube.b64url(os.urandom(32)),
    )
    path = tmp_path / "api-runtime.json"
    path.write_text(json.dumps(document))
    os.chmod(path, 0o440)
    runtime = load_http_identity_runtime(str(tmp_path), platform_key="synthetic-platform-key")
    assert type(runtime) is HttpInvocationRuntime


def test_the_in_pod_signer_produces_a_proof_the_api_verifies() -> None:
    key = os.urandom(32)
    body = b'{"kind":"email"}'
    proof = {
        "schema_version": 1,
        "receiver_id": str(uuid.uuid4()),
        "receiver_generation": 1,
        "binding_id": str(uuid.uuid4()),
        "sender": "alice@example.test",
        "delivery_id": "<synthetic@identity-e2e.example.test>",
        "body_sha256": hashlib.sha256(body).hexdigest(),
    }
    result = subprocess.run(
        [sys.executable, "-c", kube.SIGN_SCRIPT],
        input=json.dumps({"key": kube.b64url(key), "proof": proof}),
        capture_output=True,
        text=True,
        check=True,
    )
    signed = json.loads(result.stdout)["proof"]
    ring = codec.build_identity_keyring("receiver", ((proof["receiver_id"], 1, key),))
    verified = codec.verify_email_receiver_proof(signed, ring)
    assert isinstance(verified, codec.EmailReceiverProofV1)
    assert verified.model_dump() == proof
    other = codec.build_identity_keyring("receiver", ((proof["receiver_id"], 1, os.urandom(32)),))
    assert not isinstance(
        codec.verify_email_receiver_proof(signed, other), codec.EmailReceiverProofV1
    )


def test_the_caller_key_pair_signs_tokens_the_verify_key_accepts(tmp_path: Path) -> None:
    seed, verify = assets.ed25519_pair(tmp_path)
    assert list(tmp_path.iterdir()) == []
    signer = caller_token.signing_key(seed)
    assert base64.b64encode(bytes(signer.verify_key)).decode() == verify
    token = caller_token.mint(seed, agent="agent-a", exp=2_000_000_000)
    signing_input, _, signature = token.rpartition(".")
    signer.verify_key.verify(signing_input.encode(), kube.b64url_decode(signature))


def test_a_decoded_caller_token_withholds_the_signature() -> None:
    seed = base64.b64encode(bytes(SigningKey.generate())).decode()
    token = caller_token.mint(seed, agent="agent-a", exp=2_000_000_000)
    decoded = harness.decode_caller_token(token)
    assert decoded == {
        "prefix": "cct",
        "payload": {"agent": "agent-a", "exp": 2_000_000_000},
        "signature": "redacted",
    }
    assert token.rpartition(".")[2] not in json.dumps(decoded)
    with pytest.raises(kube.HarnessError):
        harness.decode_caller_token("sbx.e30.c2ln")


def test_every_action_requires_an_explicit_target() -> None:
    parser = harness.build_parser()
    for action in (
        "up --agent a",
        "send --agent a --sender s --text t",
        "observe --agent a",
        "prove --scenario requester-claim",
        "down",
    ):
        with pytest.raises(SystemExit):
            parser.parse_args(
                [*action.split(), "--namespace", "n", "--release", "r", "--state-dir", "d"]
            )
    parsed = parser.parse_args(
        ["down", "--context", "kind-x", "--namespace", "n", "--release", "r", "--state-dir", "d"]
    )
    assert parsed.context == "kind-x"


def test_state_is_private_and_refused_when_readable_by_others(tmp_path: Path) -> None:
    state = harness.State(tmp_path / "state")
    state.data = {"version": harness.STATE_VERSION, "keys": {"receiver": "synthetic"}}
    state.save()
    assert (tmp_path / "state").stat().st_mode & 0o777 == 0o700
    assert state.path.stat().st_mode & 0o777 == 0o600
    assert harness.State(tmp_path / "state").load()["keys"] == {"receiver": "synthetic"}
    os.chmod(state.path, 0o644)
    with pytest.raises(kube.HarnessError, match="readable by other users"):
        harness.State(tmp_path / "state").load()


def test_the_provider_manifests_mount_what_the_command_reads() -> None:
    manifests = assets.provider_manifests(
        prefix="rel-identity-e2e",
        release="rel",
        image="registry.example.test/curie-api:candidate",
        roster=assets.DEFAULT_ROSTER,
        certificate=b"cert",
        private_key=b"key",
        bearer="synthetic-bearer",
        adapter_secret="synthetic-adapter",
    )
    kinds = {item["kind"]: item for item in manifests}
    assert set(kinds) == {"ConfigMap", "Secret", "Deployment", "Service"}
    code, secret = kinds["ConfigMap"]["data"], kinds["Secret"]["data"]
    assert code["test_idp.py"] == assets.PROVIDER_SCRIPT.read_text()
    assert json.loads(code["roster.json"]) == assets.DEFAULT_ROSTER
    assert "synthetic-bearer" not in json.dumps(kinds["Deployment"])
    command = kinds["Deployment"]["spec"]["template"]["spec"]["containers"][0]["command"]
    mounted = {f"/idp/code/{name}" for name in code} | {f"/idp/secret/{name}" for name in secret}
    assert {arg for arg in command if arg.startswith("/idp/")} <= mounted
    for item in manifests:
        assert item["metadata"]["labels"][assets.OWNER_LABEL] == "rel"


def test_the_requester_claim_scenario_is_registered() -> None:
    assert "requester-claim" in identity_scenarios.SCENARIOS
    with pytest.raises(kube.HarnessError, match="unknown scenario"):
        identity_scenarios.run("no-such-scenario", None, None)  # type: ignore[arg-type]


def test_a_step_failure_is_recorded_and_fails_the_run() -> None:
    evidence = identity_scenarios.Evidence("synthetic")
    with evidence.step("1", "passes") as seen:
        seen["value"] = 1
    with evidence.step("2", "fails"):
        identity_scenarios.check(False, "synthetic failure")
    with evidence.step("3", "still runs"):
        pass
    result = evidence.finish()
    assert [(s["id"], s["passed"]) for s in result["steps"]] == [
        ("1", True), ("2", False), ("3", True)
    ]  # fmt: skip
    assert result["steps"][1]["failure"] == "synthetic failure"
    assert result["passed"] is False


def test_down_removes_paths_that_were_absent_and_keeps_other_changes() -> None:
    current = {
        "api": {
            "httpIdentity": {
                "existingSecret": "rel-identity-e2e-api-runtime",
                "providerCaBundle": True,
            },
            "image": {"tag": "candidate"},
        },
        "worker": {"adapterCredentials": {"identity-e2e": "synthetic", "other": "kept"}},
        "connectorCaller": {"signingKey": "synthetic", "verifyKey": "synthetic"},
    }
    previous = {
        "api.httpIdentity.existingSecret": None,
        "api.httpIdentity.providerCaBundle": None,
        "worker.adapterCredentials.identity-e2e": None,
        "connectorCaller.signingKey": "operator-seed",
        "connectorCaller.verifyKey": None,
    }
    assert assets.restored_values(current, previous) == {
        "api": {"httpIdentity": {}, "image": {"tag": "candidate"}},
        "worker": {"adapterCredentials": {"other": "kept"}},
        "connectorCaller": {"signingKey": "operator-seed"},
    }
    assert current["worker"]["adapterCredentials"]["identity-e2e"] == "synthetic"


class _OrderTarget:
    """A target that records every operation `up` performs, in order."""

    release = "rel"
    namespace = "ns"
    context = "kind-synthetic"
    fullname = "rel-curie"
    chart = Path("chart")

    def __init__(self) -> None:
        self.ops: list[tuple[str, Any]] = []
        self.values: dict[str, Any] = {}
        self.private_config = ""
        self.deployed_with: list[Any] = []

    def api_image(self) -> str:
        return "registry.example.test/curie-api:candidate"

    def component(self, component: str) -> str:
        return f"rel-curie-{component}"

    def kubectl(self, *args: str, **_kwargs: Any) -> Any:
        self.ops.append(("kubectl", args[0]))
        return subprocess.CompletedProcess(args, 0, "", "")

    def rollout(self, *deployments: str, restart: bool = False, timeout: str = "600s") -> None:
        self.ops.append(("rollout", (deployments, restart)))

    def helm_values(self) -> dict[str, Any]:
        return json.loads(json.dumps(self.values))

    def helm(self, *args: str, **_kwargs: Any) -> Any:
        assert args[0] == "upgrade"
        overlay = json.loads(Path(args[args.index("-f") + 1]).read_text())
        self.ops.append(("helm", overlay))
        _merge(self.values, overlay)
        return subprocess.CompletedProcess(args, 0, "", "")

    def pod_python(self, _script: str, _params: Any) -> Any:
        self.ops.append(("pod_python", None))
        return {"created_row": False, "previous": None}

    def sql(self, *statements: Any) -> list[Any]:
        self.ops.append(("sql", None))
        if "DISTINCT tenant_id" in statements[0][0]:
            return [[{"tenant_id": TENANT}]]
        return [[{"id": str(uuid.uuid4())}]]

    def curie(self, *args: str, **_kwargs: Any) -> Any:
        agent = args[args.index("--agent") + 1]
        self.ops.append(("deploy", agent))
        self.deployed_with.append(self.helm_values().get("connectorCaller"))
        return subprocess.CompletedProcess(args, 0, "{}", "")


def _merge(into: dict[str, Any], overlay: dict[str, Any]) -> None:
    for key, value in overlay.items():
        if isinstance(value, dict):
            _merge(into.setdefault(key, {}), value)
        else:
            into[key] = value


class _OrderApi:
    def __init__(self, target: _OrderTarget) -> None:
        self._target = target

    def agents(self) -> dict[str, dict[str, Any]]:
        deployed = [op[1] for op in self._target.ops if op[0] == "deploy"]
        return {name: {"id": str(uuid.uuid4()), "name": name} for name in deployed}

    def ok(self, method: str, path: str, body: Any = None) -> Any:
        return {"version": 1}

    def mint_channel_token(self, address: str) -> dict[str, Any]:
        return {"token": "synthetic-channel-value", "expires_at": 2_000_000_000}


def test_up_sets_the_caller_key_pair_and_rolls_the_api_before_deploying_any_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seed, verify = "synthetic-" + "s" * 12, "synthetic-" + "v" * 12

    def fake_certificates(directory: Path, hosts: Any) -> None:
        for name in ("ca.pem", "server.pem", "server.key"):
            (directory / name).write_bytes(b"synthetic")

    monkeypatch.setattr(harness, "make_certificates", fake_certificates)
    monkeypatch.setattr(harness, "ed25519_pair", lambda _directory: (seed, verify))
    target = _OrderTarget()
    monkeypatch.setattr(
        harness.Harness,
        "api",
        contextlib.contextmanager(lambda self: (yield _OrderApi(target))),
    )
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    subject = harness.Harness(target, harness.State(tmp_path / "state"))  # type: ignore[arg-type]

    subject.up(["agent-a", "agent-b"], bundle, assets.DEFAULT_ROSTER)

    ops = target.ops
    first_deploy = next(i for i, op in enumerate(ops) if op[0] == "deploy")
    assert [op[1] for op in ops if op[0] == "deploy"] == ["agent-a", "agent-b"]

    def sets_pair(overlay: dict[str, Any]) -> bool:
        caller = overlay.get("connectorCaller", {})
        return caller.get("signingKey") == seed and caller.get("verifyKey") == verify

    pair_upgrades = [i for i, op in enumerate(ops) if op[0] == "helm" and sets_pair(op[1])]
    assert pair_upgrades, "no helm upgrade set the caller key pair"
    assert pair_upgrades[0] < first_deploy
    # The API (the verifier of the worker's tokens) rolled to the pair first.
    rolled = [
        i
        for i, op in enumerate(ops)
        if op[0] == "rollout" and "rel-curie-api" in op[1][0] and i > pair_upgrades[0]
    ]
    assert rolled and rolled[0] < first_deploy
    # Every agent was deployed while the release already carried the pair.
    assert target.deployed_with == [{"signingKey": seed, "verifyKey": verify}] * 2
    # No later upgrade (the runtime-document overlay) changes the pair again.
    for op in ops[pair_upgrades[0] + 1 :]:
        if op[0] == "helm":
            caller = op[1].get("connectorCaller", {})
            assert caller.get("signingKey", seed) == seed
            assert caller.get("verifyKey", verify) == verify
    assert target.values["connectorCaller"] == {"signingKey": seed, "verifyKey": verify}
