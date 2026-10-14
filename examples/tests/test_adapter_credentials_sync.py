"""Behavioral contract for SRE-CREDS-1 through SRE-CREDS-8.

Only the external Kubernetes request boundary is replaced. Conditional PATCH
semantics follow https://kubernetes.io/docs/reference/using-api/api-concepts/.
Real API conflict rejection remains an isolated Kubernetes verification gate.
"""

from __future__ import annotations

import base64
import copy
import importlib.util
import io
import json
import sys
import urllib.error
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

SCRIPT = Path(__file__).parents[1] / "sre-bot" / "observability" / "adapter-credentials" / "sync.py"
NAMESPACE = "acme-system"
WORKER = f"/apis/apps/v1/namespaces/{NAMESPACE}/deployments/acme-worker"
TARGET = f"/api/v1/namespaces/{NAMESPACE}/secrets/acme-worker-credentials"
MAIL = f"/api/v1/namespaces/{NAMESPACE}/secrets/acme-mail-source"
CHAT = f"/api/v1/namespaces/{NAMESPACE}/secrets/acme-chat-source"
MAP_KEY = "customReplyMap"
SAFE_KEYS = {
    "status",
    "namespace",
    "workerDeployment",
    "workerContainer",
    "targetSecret",
    "targetKey",
    "adapterCount",
    "changed",
    "secretPatched",
    "workerRolled",
    "error",
}


def encoded(text: str) -> str:
    return base64.b64encode(text.encode()).decode()


def config() -> dict[str, Any]:
    return {
        "namespace": NAMESPACE,
        "workerDeployment": "acme-worker",
        "workerContainer": "worker",
        "adapters": {
            "acme-mail": {"sourceSecret": "acme-mail-source", "sourceKey": "replySecret"},
            "acme-chat": {"sourceSecret": "acme-chat-source", "sourceKey": "sharedKey"},
        },
    }


def secret(name: str, data: dict[str, str]) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": NAMESPACE, "resourceVersion": "41"},
        "type": "Opaque",
        "data": data,
    }


def merge(target: dict[str, Any], patch: dict[str, Any]) -> None:
    """Apply the external merge-patch boundary to test-owned resource state."""
    for key, value in patch.items():
        if value is None:
            target.pop(key, None)
        elif isinstance(value, dict):
            if not isinstance(target.get(key), dict):
                target[key] = {}
            merge(target[key], value)
        else:
            target[key] = copy.deepcopy(value)


class Kubernetes:
    """Synthetic external API, with owned resources and deterministic faults."""

    def __init__(self) -> None:
        self.resources = {
            MAIL: secret("acme-mail-source", {"replySecret": encoded("mail-fixture-value")}),
            CHAT: secret("acme-chat-source", {"sharedKey": encoded("chat-fixture-value")}),
            TARGET: secret(
                "acme-worker-credentials",
                {MAP_KEY: encoded("{}"), "unrelated": encoded("keep-fixture-value")},
            ),
            WORKER: {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "acme-worker",
                    "namespace": NAMESPACE,
                    "resourceVersion": "17",
                },
                "spec": {
                    "replicas": 1,
                    "template": {
                        "metadata": {"annotations": {"example.com/keep": "original"}},
                        "spec": {
                            "containers": [
                                {"name": "sidecar", "env": []},
                                {
                                    "name": "worker",
                                    "env": [
                                        {
                                            "name": "CURIE_ADAPTER_CREDENTIALS",
                                            "valueFrom": {
                                                "secretKeyRef": {
                                                    "name": "acme-worker-credentials",
                                                    "key": MAP_KEY,
                                                }
                                            },
                                        }
                                    ],
                                },
                            ]
                        },
                    },
                },
            },
        }
        self.calls: list[tuple[str, str, Any]] = []
        self.faults: dict[tuple[str, str], Exception] = {}

    def request(self, method: str, path: str, body: Any = None) -> dict[str, Any]:
        self.calls.append((method, path, copy.deepcopy(body)))
        if (method, path) in self.faults:
            raise self.faults[method, path]
        if path not in self.resources:
            raise self.failure(path, 404)
        if method == "GET":
            assert body is None
            return copy.deepcopy(self.resources[path])
        assert method == "PATCH", "No credential issuance or other API operations are allowed"
        merge(self.resources[path], body)
        return copy.deepcopy(self.resources[path])

    @staticmethod
    def failure(path: str, code: int) -> Exception:
        return urllib.error.HTTPError(
            "https://kubernetes.default.svc" + path,
            code,
            "raw-exception-fixture-value",
            {},
            io.BytesIO(b'{"message":"private-error-body-fixture-value"}'),
        )

    def credentials_env(self) -> dict[str, Any]:
        return self.resources[WORKER]["spec"]["template"]["spec"]["containers"][1]["env"][0]

    def writes(self) -> list[tuple[str, str, Any]]:
        return [call for call in self.calls if call[0] != "GET"]


def invoke(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    api: Kubernetes,
    *,
    inventory: Any = None,
    raw: str | None = None,
    dry_run: bool = False,
    missing_file: bool = False,
) -> tuple[int, dict[str, Any]]:
    # Failure here is the intended RED signal until the public program exists.
    assert SCRIPT.is_file(), "SRE-CREDS reconciler is not implemented"
    spec = importlib.util.spec_from_file_location("sre_adapter_credentials_sync", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    config_path = tmp_path / "adapters.json"
    if not missing_file:
        config_path.write_text(
            raw if raw is not None else json.dumps(config() if inventory is None else inventory)
        )
    argv = ["--config", str(config_path)]
    if dry_run:
        argv.append("--dry-run")
    exit_code = module.main(argv, request=api.request)
    output = capsys.readouterr()
    assert output.err == ""
    lines = output.out.splitlines()
    assert len(lines) == 1, "Every command emits exactly one structured result"
    result = json.loads(lines[0])
    assert isinstance(result, dict)
    assert set(result) <= SAFE_KEYS
    assert result.get("status") in {"updated", "unchanged", "would-change", "failed"}
    for key, expected in {
        "namespace": NAMESPACE,
        "workerDeployment": "acme-worker",
        "workerContainer": "worker",
        "targetSecret": "acme-worker-credentials",
        "targetKey": MAP_KEY,
    }.items():
        if key in result:
            assert result[key] == expected
    for key in ("changed", "secretPatched", "workerRolled"):
        if key in result:
            assert isinstance(result[key], bool)
    if "adapterCount" in result:
        assert isinstance(result["adapterCount"], int) and result["adapterCount"] >= 0
    if "error" in result:
        assert result["error"] in {
            "configuration",
            "worker-inspection",
            "target-inspection",
            "source-inspection",
            "inventory-shrinkage",
            "secret-write",
            "worker-rollout",
        }
    for forbidden in (
        "mail-fixture-value",
        "chat-fixture-value",
        "keep-fixture-value",
        "private-error-body-fixture-value",
        "raw-exception-fixture-value",
        encoded("mail-fixture-value"),
        encoded("chat-fixture-value"),
    ):
        assert forbidden not in output.out
    return exit_code, result


# @spec SRE-CREDS-2 SRE-CREDS-3 SRE-CREDS-5 SRE-CREDS-6 SRE-CREDS-7
@pytest.mark.parametrize("existing", ["{}", '{"acme-mail":"stale-mail","acme-chat":"stale-chat"}'])
def test_changed_map_updates_actual_custom_key_then_rolls_worker(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], existing: str
) -> None:
    api = Kubernetes()
    api.resources[TARGET]["data"][MAP_KEY] = encoded(existing)
    code, result = invoke(tmp_path, capsys, api)
    assert code == 0
    assert result == {
        "status": "updated",
        "namespace": NAMESPACE,
        "workerDeployment": "acme-worker",
        "workerContainer": "worker",
        "targetSecret": "acme-worker-credentials",
        "targetKey": MAP_KEY,
        "adapterCount": 2,
        "changed": True,
        "secretPatched": True,
        "workerRolled": True,
    }
    assert [call[:2] for call in api.writes()] == [("PATCH", TARGET), ("PATCH", WORKER)]
    first_write = next(i for i, call in enumerate(api.calls) if call[0] == "PATCH")
    assert {call[1] for call in api.calls[:first_write]} == {MAIL, CHAT, TARGET, WORKER}
    desired = json.loads(base64.b64decode(api.resources[TARGET]["data"][MAP_KEY]))
    assert desired == {"acme-mail": "mail-fixture-value", "acme-chat": "chat-fixture-value"}
    patch = api.writes()[0][2]
    assert set(patch) == {"metadata", "data"}
    assert patch["metadata"] == {"resourceVersion": "41"}
    assert set(patch["data"]) == {MAP_KEY}
    assert api.resources[TARGET]["data"]["unrelated"] == encoded("keep-fixture-value")
    rollout = api.writes()[1][2]
    assert set(rollout) == {"metadata", "spec"}
    assert rollout["metadata"] == {"resourceVersion": "17"}
    assert set(rollout["spec"]) == {"template"}
    assert set(rollout["spec"]["template"]) == {"metadata"}
    annotations = rollout["spec"]["template"]["metadata"]["annotations"]
    assert set(annotations) == {"curietech.ai/adapter-credentials-at"}
    stamp = datetime.fromisoformat(annotations["curietech.ai/adapter-credentials-at"])
    assert stamp.utcoffset() is not None and stamp.utcoffset().total_seconds() == 0
    assert api.resources[WORKER]["spec"]["replicas"] == 1
    assert (
        api.resources[WORKER]["spec"]["template"]["metadata"]["annotations"]["example.com/keep"]
        == "original"
    )


# @spec SRE-CREDS-4 SRE-CREDS-5 SRE-CREDS-6 SRE-CREDS-7
@pytest.mark.parametrize("dry_run", [False, True])
def test_equal_map_ignores_json_order_and_spacing_without_rollout(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], dry_run: bool
) -> None:
    api = Kubernetes()
    api.resources[TARGET]["data"][MAP_KEY] = encoded(
        '{ "acme-mail": "mail-fixture-value", "acme-chat": "chat-fixture-value" }'
    )
    before = copy.deepcopy(api.resources)
    code, result = invoke(tmp_path, capsys, api, dry_run=dry_run)
    assert code == 0 and result["status"] == "unchanged"
    assert not result["changed"] and not result["secretPatched"] and not result["workerRolled"]
    assert api.writes() == [] and api.resources == before


# @spec SRE-CREDS-7
def test_dry_run_performs_complete_reads_but_has_no_mutation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    api = Kubernetes()
    before = copy.deepcopy(api.resources)
    code, result = invoke(tmp_path, capsys, api, dry_run=True)
    assert code == 0 and result["status"] == "would-change"
    assert result["changed"] and not result["secretPatched"] and not result["workerRolled"]
    assert {call[1] for call in api.calls} == {MAIL, CHAT, TARGET, WORKER}
    assert api.writes() == [] and api.resources == before


# @spec SRE-CREDS-1 SRE-CREDS-7
@pytest.mark.parametrize(
    "raw",
    [
        "not-json",
        "[]",
        "{}",
        '{"namespace":"acme-system","namespace":"other"}',
        json.dumps({**config(), "namespace": ""}),
        json.dumps({**config(), "namespace": "bad/name"}),
        json.dumps({**config(), "workerDeployment": 7}),
        json.dumps({**config(), "workerContainer": " "}),
        json.dumps({**config(), "adapters": {}}),
        json.dumps({**config(), "adapters": []}),
        json.dumps(
            {**config(), "adapters": {"": {"sourceSecret": "acme-source", "sourceKey": "x"}}}
        ),
        json.dumps({**config(), "adapters": {"acme-a": {"sourceSecret": "acme-source"}}}),
        json.dumps(
            {
                **config(),
                "adapters": {"acme-a": {"sourceSecret": "acme-source", "sourceKey": "bad/key"}},
            }
        ),
        json.dumps(
            {**config(), "adapters": {"acme-a": {"sourceSecret": "bad/name", "sourceKey": "x"}}}
        ),
        json.dumps(
            {**config(), "adapters": {"acme-a": {"sourceSecret": "acme-source", "sourceKey": " "}}}
        ),
        json.dumps({**config(), "unknown": "ignored-config-is-dangerous"}),
        json.dumps(config()).replace(
            '"sourceKey": "replySecret"', '"sourceKey": "replySecret", "sourceKey": "other"'
        ),
    ],
)
def test_invalid_config_refuses_before_any_cluster_request(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], raw: str
) -> None:
    api = Kubernetes()
    code, result = invoke(tmp_path, capsys, api, raw=raw)
    assert code != 0 and result["status"] == "failed" and result["error"] == "configuration"
    assert api.calls == []


# @spec SRE-CREDS-1 SRE-CREDS-7
def test_missing_config_refuses_before_any_cluster_request(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    api = Kubernetes()
    code, result = invoke(tmp_path, capsys, api, missing_file=True)
    assert code != 0 and result["error"] == "configuration"
    assert api.calls == []


# @spec SRE-CREDS-2 SRE-CREDS-7
@pytest.mark.parametrize(
    "fault",
    [
        "read",
        "container",
        "duplicate-container",
        "missing-env",
        "duplicate-env",
        "literal",
        "key",
        "optional",
        "other-source",
        "identity",
        "version",
    ],
)
def test_uninspectable_worker_never_guesses_or_writes_a_target(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], fault: str
) -> None:
    api = Kubernetes()
    env = api.credentials_env()
    containers = api.resources[WORKER]["spec"]["template"]["spec"]["containers"]
    if fault == "read":
        api.faults["GET", WORKER] = api.failure(WORKER, 403)
    elif fault == "container":
        containers[1]["name"] = "different-worker"
    elif fault == "duplicate-container":
        containers.append(copy.deepcopy(containers[1]))
    elif fault == "missing-env":
        containers[1]["env"] = []
    elif fault == "duplicate-env":
        containers[1]["env"].append(copy.deepcopy(env))
    elif fault == "literal":
        env["value"] = "literal-fixture-map"
    elif fault == "key":
        env["valueFrom"]["secretKeyRef"].pop("key")
    elif fault == "optional":
        env["valueFrom"]["secretKeyRef"]["optional"] = True
    elif fault == "other-source":
        env["valueFrom"]["configMapKeyRef"] = {"name": "acme-config", "key": "map"}
    elif fault == "identity":
        api.resources[WORKER]["metadata"]["namespace"] = "wrong-namespace"
    elif fault == "version":
        api.resources[WORKER]["metadata"].pop("resourceVersion")
    code, result = invoke(tmp_path, capsys, api)
    assert code != 0 and result["error"] == "worker-inspection"
    assert api.writes() == []
    assert not any(call[1] == TARGET for call in api.calls)


# @spec SRE-CREDS-2 SRE-CREDS-4 SRE-CREDS-7
@pytest.mark.parametrize(
    "fault",
    [
        "read",
        "identity",
        "version",
        "immutable",
        "bad-base64",
        "bad-json",
        "array",
        "non-string",
        "blank",
        "duplicate",
    ],
)
def test_invalid_target_prevents_all_writes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], fault: str
) -> None:
    api = Kubernetes()
    target = api.resources[TARGET]
    if fault == "read":
        api.faults["GET", TARGET] = api.failure(TARGET, 404)
    elif fault == "identity":
        target["metadata"]["name"] = "wrong-secret"
    elif fault == "version":
        target["metadata"]["resourceVersion"] = ""
    elif fault == "immutable":
        target["immutable"] = True
    else:
        bad = {
            "bad-base64": "%%%",
            "bad-json": encoded("not-json"),
            "array": encoded("[]"),
            "non-string": encoded('{"acme-mail": 1}'),
            "blank": encoded('{"acme-mail": " "}'),
            "duplicate": encoded('{"acme-mail":"a","acme-mail":"b"}'),
        }
        target["data"][MAP_KEY] = bad[fault]
    before = copy.deepcopy(api.resources)
    code, result = invoke(tmp_path, capsys, api)
    assert code != 0 and result["error"] == "target-inspection"
    assert api.writes() == [] and api.resources == before


# @spec SRE-CREDS-3 SRE-CREDS-7
@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize(
    "fault",
    [
        "missing-secret",
        "read",
        "identity",
        "missing-key",
        "empty",
        "whitespace",
        "bad-base64",
        "bad-utf8",
    ],
)
def test_one_invalid_source_refuses_entire_map_without_leaking_errors(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], fault: str, dry_run: bool
) -> None:
    api = Kubernetes()
    if fault == "missing-secret":
        api.resources.pop(CHAT)
    elif fault == "read":
        api.faults["GET", CHAT] = api.failure(CHAT, 403)
    elif fault == "identity":
        api.resources[CHAT]["metadata"]["namespace"] = "wrong-namespace"
    elif fault == "missing-key":
        api.resources[CHAT]["data"].pop("sharedKey")
    else:
        bad = {
            "empty": "",
            "whitespace": encoded(" \n"),
            "bad-base64": "%%%",
            "bad-utf8": "/w==",
        }
        api.resources[CHAT]["data"]["sharedKey"] = bad[fault]
    before = copy.deepcopy(api.resources)
    code, result = invoke(tmp_path, capsys, api, dry_run=dry_run)
    assert code != 0 and result["error"] == "source-inspection"
    assert api.writes() == [] and api.resources == before


# @spec SRE-CREDS-3
def test_source_credential_is_preserved_verbatim(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    api = Kubernetes()
    api.resources[CHAT]["data"]["sharedKey"] = encoded(" fixture-with-space \n")
    code, result = invoke(tmp_path, capsys, api)
    assert code == 0 and result["status"] == "updated"
    actual = json.loads(base64.b64decode(api.resources[TARGET]["data"][MAP_KEY]))
    assert actual["acme-chat"] == " fixture-with-space \n"


# @spec SRE-CREDS-4
@pytest.mark.parametrize("dry_run", [False, True])
def test_missing_prior_identity_requires_separate_retirement(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], dry_run: bool
) -> None:
    api = Kubernetes()
    api.resources[TARGET]["data"][MAP_KEY] = encoded('{"acme-retained":"old-fixture-value"}')
    before = copy.deepcopy(api.resources)
    code, result = invoke(tmp_path, capsys, api, dry_run=dry_run)
    assert code != 0 and result["error"] == "inventory-shrinkage"
    assert api.writes() == [] and api.resources == before


# @spec SRE-CREDS-4 SRE-CREDS-5
def test_absent_target_key_can_be_initialized_without_removing_other_data(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    api = Kubernetes()
    api.resources[TARGET]["data"].pop(MAP_KEY)
    code, result = invoke(tmp_path, capsys, api)
    assert code == 0 and result["status"] == "updated"
    assert set(api.resources[TARGET]["data"]) == {MAP_KEY, "unrelated"}


# @spec SRE-CREDS-5 SRE-CREDS-6 SRE-CREDS-7
@pytest.mark.parametrize("status", [409, 403, 500])
def test_secret_write_failure_has_no_blind_retry_or_worker_rollout(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], status: int
) -> None:
    api = Kubernetes()
    api.faults["PATCH", TARGET] = api.failure(TARGET, status)
    before = copy.deepcopy(api.resources)
    code, result = invoke(tmp_path, capsys, api)
    assert code != 0 and result["error"] == "secret-write"
    assert not result["secretPatched"] and not result["workerRolled"]
    assert [call[:2] for call in api.writes()] == [("PATCH", TARGET)]
    assert api.resources == before


# @spec SRE-CREDS-6 SRE-CREDS-7
@pytest.mark.parametrize("status", [409, 403])
def test_worker_patch_failure_reports_partial_change_without_rollback(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], status: int
) -> None:
    api = Kubernetes()
    api.faults["PATCH", WORKER] = api.failure(WORKER, status)
    before_worker = copy.deepcopy(api.resources[WORKER])
    code, result = invoke(tmp_path, capsys, api)
    assert code != 0 and result["status"] == "failed" and result["error"] == "worker-rollout"
    assert result["changed"] and result["secretPatched"] and not result["workerRolled"]
    assert [call[:2] for call in api.writes()] == [("PATCH", TARGET), ("PATCH", WORKER)]
    assert api.resources[WORKER] == before_worker
    assert json.loads(base64.b64decode(api.resources[TARGET]["data"][MAP_KEY])) == {
        "acme-mail": "mail-fixture-value",
        "acme-chat": "chat-fixture-value",
    }
