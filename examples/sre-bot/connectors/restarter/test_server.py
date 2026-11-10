"""Behavior of the restarter connector: an idempotent acting connector.

@spec ACTION-EXECUTOR-9 @spec ACTION-EXECUTOR-13 @spec ACTION-EXECUTOR-16
@spec AUTOMATED-REMEDIATION-22. Every test starts ``server.py`` as a real
process and drives it through a real MCP client, so what is pinned is the wire
behavior the runner's ``list`` and ``call`` phases meet. The replies are judged
by the production readers that consume them: the runner's ``OutboundRedactor``
and ``restore_refusal``, the worker's ``_snapshot`` and ``call_outcome``.

ADR 0203 admits a rolling restart only as a REMEDIATION-11 action: not
reversible, but idempotent, bounded to the declared target and verified. Its
qualification is two confirmed forwards with the same canonical arguments whose
records left the same ``post_version``, so the tests below pin exactly that,
plus the refusals that keep the action inside its target.

The Kubernetes API is a local stand-in for ``apps/v1`` Deployments in one
namespace, with the behavior the connector relies on:

* ``GET .../deployments/<name>`` answers the Deployment;
* ``PATCH`` with ``application/merge-patch+json`` merges the body; a
  ``metadata.resourceVersion`` in the body that is not the live one answers
  ``409``, and a ``metadata.uid`` that is not the live one answers ``422``
  (``uid`` is immutable), both writing nothing (observed from a v1.37 API
  server on a disposable kind cluster);
* a change of the pod template moves ``metadata.generation`` and
  ``resourceVersion``; a patch that changes nothing moves neither;
* ``resourceVersion`` also moves while the controller writes status.

The stand-in also answers ``403`` for a Deployment outside its granted names,
as ``resourceNames`` in ``manifests/restarter-access.yaml`` makes a real API
server do.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import http.server
import json
import os
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from aci_protocol import SideEffectFlag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from curie_runner.executor import _Tool, restore_refusal
from curie_runner.redact import OutboundRedactor
from curie_worker import action_executor
from curie_worker.actions import _snapshot
from mcp import Client

_HERE = Path(__file__).resolve().parent
_SERVER = _HERE / "server.py"
_VECTORS = _HERE.parents[3] / "tests" / "vectors"
_SEALED = json.loads((_VECTORS / "sealed-snapshot-reply.json").read_text("utf-8"))
_ENVELOPE = _SEALED["envelope"]

_NAMESPACE = "sre-demo"
_NAME = "example-api"
_OTHER = "example-worker"
_UNGRANTED = "example-billing"
_TARGET = {"kind": "Deployment", "namespace": _NAMESPACE, "name": _NAME}
_TOKEN = "example-restarter-token"
_KID_A = "example-key-a"
_TOOL = "restart_deployment"
_ANNOTATION = "kubectl.kubernetes.io/restartedAt"
_SECRET_ENV_VALUE = "example-literal-env-value"


def _rfc3339(epoch: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(epoch))


# An incident that started an hour ago, and one that started ten minutes ago.
_EARLIER = int(time.time()) - 3600
_LATER = int(time.time()) - 600


def _key(kid: str) -> tuple[str, bytes]:
    raw = os.urandom(32)
    return f"{kid}:{base64.b64encode(raw).decode('ascii')}", raw


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


# --------------------------------------------------------------------------- #
# The Kubernetes API stand-in.
# --------------------------------------------------------------------------- #


class _Deployment:
    def __init__(self, annotation: str | None = None, paused: bool = False) -> None:
        self.uid = str(uuid.uuid4())
        self.resource_version = 100
        self.generation = 1
        self.paused = paused
        self.template_annotations: dict[str, str] = {"example.com/owner": "example-team"}
        if annotation is not None:
            self.template_annotations[_ANNOTATION] = annotation


def _status_write(deployment: _Deployment) -> None:
    """The controller writing status: ``resourceVersion`` moves, the template does not."""

    deployment.resource_version += 1


def _restarted_to(value: str) -> Callable[[_Deployment], None]:
    """Someone else restarting the Deployment, as ``kubectl rollout restart`` does."""

    def change(deployment: _Deployment) -> None:
        deployment.template_annotations[_ANNOTATION] = value
        deployment.generation += 1
        deployment.resource_version += 1

    return change


class _Api:
    """``apps/v1`` Deployments of one namespace, as the connector meets them."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.deployments: dict[str, _Deployment] = {
            _NAME: _Deployment(),
            _OTHER: _Deployment(),
            _UNGRANTED: _Deployment(),
        }
        # The names the credential's Role grants (``resourceNames``).
        self.granted = {_NAME, _OTHER}
        self.requests: list[tuple[str, str, dict[str, Any] | None, str | None]] = []
        # Applied after successive reads, one per read: a concurrent writer
        # between the connector's read and its write.
        self.after_get: list[Callable[[_Deployment], None]] = []

    def document(self, name: str) -> dict[str, Any]:
        deployment = self.deployments[name]
        spec: dict[str, Any] = {
            "replicas": 1,
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {
                    "labels": {"app": name},
                    "annotations": dict(deployment.template_annotations),
                },
                "spec": {
                    "containers": [
                        {
                            "name": "app",
                            "image": "example.com/app:1",
                            "env": [{"name": "EXAMPLE", "value": _SECRET_ENV_VALUE}],
                        }
                    ]
                },
            },
        }
        if deployment.paused:
            spec["paused"] = True
        return {
            "kind": "Deployment",
            "apiVersion": "apps/v1",
            "metadata": {
                "name": name,
                "namespace": _NAMESPACE,
                "uid": deployment.uid,
                "resourceVersion": str(deployment.resource_version),
                "generation": deployment.generation,
                "creationTimestamp": "2026-10-20T00:00:00Z",
            },
            "spec": spec,
            "status": {"observedGeneration": deployment.generation, "replicas": 1},
        }

    def recreate(self, name: str) -> None:
        """Delete and recreate under the same name: a new uid, the same template."""

        with self.lock:
            annotations = self.deployments[name].template_annotations
            self.deployments[name] = _Deployment()
            self.deployments[name].template_annotations = dict(annotations)

    def writes(self) -> list[tuple[dict[str, Any] | None, str | None]]:
        return [(body, ctype) for method, _p, body, ctype in self.requests if method == "PATCH"]


def _merge(target: dict[str, Any], patch: dict[str, Any]) -> None:
    for key, value in patch.items():
        if value is None:
            target.pop(key, None)
        elif isinstance(value, dict) and isinstance(target.get(key), dict):
            _merge(target[key], value)
        else:
            target[key] = copy.deepcopy(value)


def _handler(api: _Api) -> type[http.server.BaseHTTPRequestHandler]:
    prefix = f"/apis/apps/v1/namespaces/{_NAMESPACE}/deployments/"

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_args: object) -> None:
            pass

        def _send(self, code: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _status(self, code: int, reason: str) -> None:
            self._send(
                code, {"kind": "Status", "status": "Failure", "reason": reason, "code": code}
            )

        def _name(self) -> str | None:
            if self.headers.get("Authorization") != f"Bearer {_TOKEN}":
                self._status(401, "Unauthorized")
                return None
            if not self.path.startswith(prefix) or "/" in self.path[len(prefix) :]:
                self._status(404, "NotFound")
                return None
            name = self.path[len(prefix) :]
            if name not in api.granted:
                self._status(403, "Forbidden")
                return None
            if name not in api.deployments:
                self._status(404, "NotFound")
                return None
            return name

        def do_GET(self) -> None:  # noqa: N802 - http.server's naming
            api.requests.append(("GET", self.path, None, None))
            name = self._name()
            if name is None:
                return
            with api.lock:
                payload = api.document(name)
                if api.after_get:
                    api.after_get.pop(0)(api.deployments[name])
            self._send(200, payload)

        def do_PATCH(self) -> None:  # noqa: N802 - http.server's naming
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length) or b"null")
            ctype = self.headers.get("Content-Type")
            api.requests.append(("PATCH", self.path, body, ctype))
            name = self._name()
            if name is None:
                return
            if ctype != "application/merge-patch+json":
                self._status(415, "UnsupportedMediaType")
                return
            with api.lock:
                deployment = api.deployments[name]
                metadata = body.get("metadata") or {}
                if metadata.get("uid") not in (None, deployment.uid):
                    self._status(422, "Invalid")
                    return
                expected = metadata.get("resourceVersion")
                if expected is not None and expected != str(deployment.resource_version):
                    self._status(409, "Conflict")
                    return
                before = api.document(name)
                after = copy.deepcopy(before)
                _merge(after, {"spec": body.get("spec") or {}})
                if after["spec"] != before["spec"]:
                    deployment.template_annotations = dict(
                        after["spec"]["template"]["metadata"].get("annotations") or {}
                    )
                    deployment.generation += 1
                    deployment.resource_version += 1
                payload = api.document(name)
            self._send(200, payload)

    return Handler


@pytest.fixture
def api() -> Iterator[tuple[_Api, str]]:
    state = _Api()
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _handler(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield state, f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


# --------------------------------------------------------------------------- #
# The connector as a real process, a real MCP client against it.
# --------------------------------------------------------------------------- #


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


class _Connector:
    def __init__(self, env: dict[str, str]) -> None:
        port = _free_port()
        child_env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("SNAPSHOT_SEALING_", "RESTARTER_", "KUBECONFIG"))
        }
        child_env.update({"BIND_ADDRESS": "127.0.0.1", "PORT": str(port)})
        child_env.update(env)
        self.process = subprocess.Popen(
            [sys.executable, str(_SERVER)],
            env=child_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        self.url = f"http://127.0.0.1:{port}/mcp"
        self.output = ""
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.output = self.process.stdout.read().decode("utf-8", "replace")  # type: ignore[union-attr]
                return
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                return
            except OSError:
                # Wait on the process, not a sleep: it also notices an early exit.
                try:
                    self.process.wait(timeout=0.05)
                except subprocess.TimeoutExpired:
                    pass
        self.stop()
        pytest.fail("restarter connector did not listen within 20s")

    def stop(self) -> str:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.process.stdout is not None and not self.process.stdout.closed:
            self.output += self.process.stdout.read().decode("utf-8", "replace")
            self.process.stdout.close()
        return self.output

    def tools(self) -> dict[str, Any]:
        async def go() -> dict[str, Any]:
            async with Client(self.url) as client:
                listed = await client.list_tools()
                return {tool.name: tool for tool in listed.tools}

        return asyncio.run(go())

    def call(self, name: str, arguments: dict[str, Any]) -> tuple[bool, Any]:
        """``(is_error, structured)`` exactly as the runner's ``call`` phase reports them."""

        async def go() -> tuple[bool, Any]:
            async with Client(self.url) as client:
                result = await client.call_tool(name, arguments)
                return bool(result.is_error), result.structured_content

        return asyncio.run(go())


@pytest.fixture
def connector(api: tuple[_Api, str], tmp_path: Path) -> Iterator[Any]:
    _state, url = api
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "clusters": [{"name": "example", "cluster": {"server": url}}],
                "users": [{"name": "sre-bot-restarter", "user": {"token": _TOKEN}}],
                "contexts": [
                    {
                        "name": "example",
                        "context": {"cluster": "example", "user": "sre-bot-restarter"},
                    }
                ],
                "current-context": "example",
            }
        ),
        "utf-8",
    )
    started: list[_Connector] = []

    def start(**env: str) -> _Connector:
        for running in started:
            running.stop()
        merged = {
            "KUBECONFIG_PATH": str(kubeconfig),
            "RESTARTER_NAMESPACE": _NAMESPACE,
            "RESTARTER_DEPLOYMENTS": f"{_NAME},{_OTHER},{_UNGRANTED}",
        }
        merged.update(env)
        process = _Connector(merged)
        started.append(process)
        return process

    yield start
    for running in started:
        running.stop()


def _restart(process: _Connector, restart_at: int, name: str = _NAME) -> dict[str, Any]:
    is_error, reply = process.call(_TOOL, {"name": name, "restart_at": restart_at})
    assert is_error is False, reply
    assert isinstance(reply, dict) and reply.get("ok") is True, reply
    return reply


def _recorded(reply: dict[str, Any]) -> Any:
    """The worker's record of ``reply`` after it crossed the runner's redactor."""

    line = json.dumps(
        {
            "type": "side_effect_flag",
            "tool": f"mcp__restarter__{_TOOL}",
            "call_id": "toolu_example",
            "arguments": {"name": _NAME, "restart_at": _EARLIER},
            "result": reply,
        }
    )
    (emitted,) = OutboundRedactor(frozenset({_TOKEN})).push(line)
    return _snapshot(SideEffectFlag.model_validate(json.loads(emitted)))


def _outcome(reply: tuple[bool, Any]) -> tuple[str, str | None]:
    is_error, structured = reply
    return action_executor.call_outcome(is_error=is_error, structured=structured)


def _opened(reply: dict[str, Any], raw: bytes) -> Any:
    sealed = base64.b64decode(reply["prior"]["ciphertext"], validate=True)
    return json.loads(AESGCM(raw).decrypt(sealed[:12], sealed[12:], _canonical(reply["target"])))


def _sealing(current: str | None = None) -> dict[str, str]:
    return {} if current is None else {"SNAPSHOT_SEALING_KEY": current}


# --------------------------------------------------------------------------- #
# The advertised surface (ACTION-EXECUTOR-13).
# --------------------------------------------------------------------------- #


def test_the_connector_advertises_one_idempotent_write_and_no_restore(connector: Any) -> None:
    """@spec ACTION-EXECUTOR-13: the probe never records it as restore capable.

    A restart cannot be put back: writing the old annotation again would roll
    the workload a second time. So the connector serves no ``restore`` and no
    ``observe_version``, and every record it leaves is not undoable.
    """

    current, _ = _key(_KID_A)
    tools = connector(**_sealing(current)).tools()
    assert set(tools) == {_TOOL}

    probed = {
        name: _Tool(
            name,
            tool.annotations.model_dump(by_alias=True, exclude_none=True)
            if tool.annotations is not None
            else {},
            dict(tool.input_schema),
        )
        for name, tool in tools.items()
    }
    assert restore_refusal(probed) == "restore_not_advertised"

    write = probed[_TOOL]
    assert write.read_only is False
    assert write.annotations.get("idempotentHint") is True
    assert write.annotations.get("destructiveHint") is True
    # Flat scalar arguments: a remediation policy bounds only JSON scalars, so
    # the target is the name and the token is an integer a range can bound.
    assert set(write.input_schema["required"]) == {"name", "restart_at"}
    assert set(write.input_schema["properties"]) == {"name", "restart_at"}
    assert write.input_schema["properties"]["restart_at"]["type"] == "integer"


# --------------------------------------------------------------------------- #
# The forward write's sealed reply (ACTION-EXECUTOR-9).
# --------------------------------------------------------------------------- #


def test_a_restart_patches_only_the_annotation_and_replies_sealed(
    connector: Any, api: tuple[_Api, str]
) -> None:
    """@spec ACTION-EXECUTOR-9: ``prior``, ``version`` and ``target``, recorded."""

    state, _ = api
    current, raw = _key(_KID_A)
    deployment = state.deployments[_NAME]
    uid = deployment.uid
    reply = _restart(connector(**_sealing(current)), _EARLIER)

    # The pod template changed, so the controller rolls the workload.
    assert deployment.template_annotations[_ANNOTATION] == _rfc3339(_EARLIER)
    assert deployment.template_annotations["example.com/owner"] == "example-team"
    assert deployment.generation == 2

    assert reply["target"] == _TARGET
    prior = reply["prior"]
    assert set(prior) == set(_ENVELOPE["keys"])
    assert prior["sealed"] == _ENVELOPE["sealed"]
    assert prior["kid"] == _KID_A
    assert _opened(reply, raw) == {"restartedAt": None, "uid": uid}

    version = reply["version"]
    assert version == f"deployments/{uid}/restartedAt={_rfc3339(_EARLIER)}"
    assert 0 < len(version) <= _ENVELOPE["version_max_length"]
    # Nothing of the pod template beyond the annotation leaves the connector.
    assert _SECRET_ENV_VALUE not in json.dumps(reply)
    assert "example.com/app" not in json.dumps(reply)

    # One merge patch, carrying the read's uid and resourceVersion as
    # preconditions, touching nothing but the one annotation.
    ((written, ctype),) = state.writes()
    assert ctype == "application/merge-patch+json"
    assert written == {
        "metadata": {"uid": uid, "resourceVersion": "100"},
        "spec": {"template": {"metadata": {"annotations": {_ANNOTATION: _rfc3339(_EARLIER)}}}},
    }

    recorded = _recorded(reply)
    assert recorded.prior_state == prior
    assert recorded.post_version == version
    assert recorded.target == _TARGET


def test_repeating_the_same_restart_writes_nothing_and_leaves_the_same_post_version(
    connector: Any, api: tuple[_Api, str]
) -> None:
    """@spec AUTOMATED-REMEDIATION-22: the REMEDIATION-11 qualification evidence.

    Two forwards with the same canonical arguments, both confirmed, whose
    records left the same ``post_version``, and the repeat had no effect.
    """

    state, _ = api
    current, raw = _key(_KID_A)
    process = connector(**_sealing(current))
    first = _restart(process, _EARLIER)
    generation = state.deployments[_NAME].generation
    second = _restart(process, _EARLIER)

    assert _outcome((False, first)) == ("confirmed", None)
    assert _outcome((False, second)) == ("confirmed", None)
    assert _recorded(first).post_version == _recorded(second).post_version
    assert len(state.writes()) == 1
    assert state.deployments[_NAME].generation == generation
    assert _opened(second, raw)["restartedAt"] == _rfc3339(_EARLIER)

    # A later incident is a different token, so it restarts again.
    third = _restart(process, _LATER)
    assert third["version"] != first["version"]
    assert state.deployments[_NAME].generation == generation + 1


def test_an_older_token_never_moves_the_restart_backwards(
    connector: Any, api: tuple[_Api, str]
) -> None:
    """A late nomination for an old incident cannot roll the workload again.

    The live annotation already names a restart at or after the requested
    instant, so the workload has been restarted since that incident began.
    """

    state, _ = api
    current, _ = _key(_KID_A)
    process = connector(**_sealing(current))
    later = _restart(process, _LATER)
    generation = state.deployments[_NAME].generation

    older = _restart(process, _EARLIER)
    assert older["version"] == later["version"]
    assert len(state.writes()) == 1
    assert state.deployments[_NAME].generation == generation


def test_a_kubectl_annotation_with_an_offset_is_compared_as_an_instant(
    connector: Any, api: tuple[_Api, str]
) -> None:
    """``kubectl rollout restart`` writes local time with an offset (RFC 3339)."""

    state, _ = api
    current, _ = _key(_KID_A)
    # Ten minutes ago, written as a +09:00 local time.
    local = time.strftime("%Y-%m-%dT%H:%M:%S+09:00", time.gmtime(_LATER + 9 * 3600))
    state.deployments[_NAME].template_annotations[_ANNOTATION] = local
    process = connector(**_sealing(current))

    reply = _restart(process, _EARLIER)
    assert reply["version"].endswith(f"restartedAt={local}")
    assert state.writes() == []

    newer = int(time.time()) - 60
    _restart(process, newer)
    assert state.deployments[_NAME].template_annotations[_ANNOTATION] == _rfc3339(newer)


@pytest.mark.parametrize("value", ["", "yesterday", "2026-11-06T00:00:00"])
def test_an_unreadable_annotation_is_replaced(
    connector: Any, api: tuple[_Api, str], value: str
) -> None:
    """A value that names no instant (or no zone) cannot prove a restart happened."""

    state, _ = api
    current, _ = _key(_KID_A)
    state.deployments[_NAME].template_annotations[_ANNOTATION] = value
    _restart(connector(**_sealing(current)), _EARLIER)
    assert state.deployments[_NAME].template_annotations[_ANNOTATION] == _rfc3339(_EARLIER)


def test_a_recreated_deployment_is_a_different_version(
    connector: Any, api: tuple[_Api, str]
) -> None:
    state, _ = api
    current, _ = _key(_KID_A)
    process = connector(**_sealing(current))
    first = _restart(process, _EARLIER)
    state.recreate(_NAME)
    second = _restart(process, _EARLIER)

    assert second["version"] != first["version"]
    assert len(state.writes()) == 1


# --------------------------------------------------------------------------- #
# Concurrency: the patch is a compare-and-swap.
# --------------------------------------------------------------------------- #


def test_a_concurrent_restart_between_read_and_write_refuses_with_nothing_written(
    connector: Any, api: tuple[_Api, str]
) -> None:
    """@spec ACTION-EXECUTOR-20: someone else restarted it, so ``version_conflict``."""

    state, _ = api
    current, _ = _key(_KID_A)
    other = _rfc3339(_LATER)
    state.after_get = [_restarted_to(other)]
    reply = connector(**_sealing(current)).call(_TOOL, {"name": _NAME, "restart_at": _EARLIER})

    assert reply == (False, {"ok": False, "refused": "version_conflict"})
    assert _outcome(reply) == ("failed", "version_conflict_at_write")
    assert state.deployments[_NAME].template_annotations[_ANNOTATION] == other


def test_a_deployment_recreated_between_read_and_write_is_never_patched(
    connector: Any, api: tuple[_Api, str]
) -> None:
    """The patch names the uid it read, so a new object under the name is refused."""

    state, _ = api
    current, _ = _key(_KID_A)

    def recreate(deployment: _Deployment) -> None:
        state.deployments[_NAME] = _Deployment()
        state.deployments[_NAME].resource_version = deployment.resource_version

    state.after_get = [recreate]
    is_error, reply = connector(**_sealing(current)).call(
        _TOOL, {"name": _NAME, "restart_at": _EARLIER}
    )

    assert is_error is True
    assert reply is None or reply.get("ok") is not True
    assert _ANNOTATION not in state.deployments[_NAME].template_annotations


def test_a_status_write_between_read_and_write_is_retried(
    connector: Any, api: tuple[_Api, str]
) -> None:
    state, _ = api
    current, _ = _key(_KID_A)
    state.after_get = [_status_write]
    reply = _restart(connector(**_sealing(current)), _EARLIER)

    assert state.deployments[_NAME].template_annotations[_ANNOTATION] == _rfc3339(_EARLIER)
    assert len(state.writes()) == 2
    assert reply["version"].endswith(_rfc3339(_EARLIER))


def test_status_writes_that_never_settle_refuse_with_nothing_written(
    connector: Any, api: tuple[_Api, str]
) -> None:
    state, _ = api
    current, _ = _key(_KID_A)
    state.after_get = [_status_write] * 10
    reply = connector(**_sealing(current)).call(_TOOL, {"name": _NAME, "restart_at": _EARLIER})

    assert reply == (False, {"ok": False, "refused": "version_conflict"})
    assert _ANNOTATION not in state.deployments[_NAME].template_annotations
    assert len(state.writes()) == 3


# --------------------------------------------------------------------------- #
# Forward refusals: the allowlist, the token and key custody.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("arguments", "env"),
    [
        ({"name": "example-unlisted", "restart_at": _EARLIER}, {}),
        ({"name": "example-api-2", "restart_at": _EARLIER}, {}),
        ({"name": _NAME, "restart_at": _EARLIER}, {"RESTARTER_DEPLOYMENTS": ""}),
        ({"name": _NAME, "restart_at": _EARLIER}, {"RESTARTER_NAMESPACE": ""}),
        ({"name": _NAME, "restart_at": int(time.time()) + 3600}, {}),
        ({"name": _NAME, "restart_at": 2**62}, {}),
        ({"name": _NAME, "restart_at": -1}, {}),
        ({"name": _NAME, "restart_at": True}, {}),
        ({"name": _NAME, "restart_at": str(_EARLIER)}, {}),
        ({"name": _NAME, "restart_at": _EARLIER + 0.5}, {}),
    ],
    ids=[
        "name_not_allowlisted",
        "name_only_prefixed",
        "no_allowlist",
        "no_namespace",
        "future_token",
        "far_future_token",
        "negative_token",
        "boolean_token",
        "string_token",
        "fractional_token",
    ],
)
def test_a_call_outside_the_configured_bounds_reaches_no_api(
    connector: Any, api: tuple[_Api, str], arguments: dict[str, Any], env: dict[str, str]
) -> None:
    """Refused as a tool error before any request, so nothing is read or written.

    A future token is refused because it would suppress every later restart
    until that instant passed.
    """

    state, _ = api
    current, _ = _key(_KID_A)
    is_error, reply = connector(**_sealing(current), **env).call(_TOOL, arguments)

    assert is_error is True
    assert reply is None or reply.get("ok") is not True
    assert state.requests == []


def test_without_a_sealing_key_the_restart_refuses_and_writes_nothing(
    connector: Any, api: tuple[_Api, str]
) -> None:
    """@spec ACTION-EXECUTOR-16: no unsealed fallback, so no unqualifiable record."""

    state, _ = api
    reply = connector().call(_TOOL, {"name": _NAME, "restart_at": _EARLIER})

    assert reply == (False, {"ok": False, "refused": "sealing_key_unavailable"})
    assert _outcome(reply) == ("failed", "sealing_key_unavailable")
    assert state.requests == []


def test_a_paused_deployment_is_refused_with_nothing_written(
    connector: Any, api: tuple[_Api, str]
) -> None:
    """The annotation would not roll a paused Deployment, so nothing is claimed."""

    state, _ = api
    current, _ = _key(_KID_A)
    state.deployments[_NAME].paused = True
    is_error, reply = connector(**_sealing(current)).call(
        _TOOL, {"name": _NAME, "restart_at": _EARLIER}
    )

    assert is_error is True
    assert reply is None or reply.get("ok") is not True
    assert state.writes() == []


def test_the_api_server_refusing_the_credential_is_a_tool_error(
    connector: Any, api: tuple[_Api, str]
) -> None:
    """A name the Role does not grant: the API's ``403`` refuses, nothing is written.

    The connector's own allowlist names ``example-billing`` here on purpose: the
    Role is the ceiling even when the connector's configuration is wider.
    """

    state, _ = api
    current, _ = _key(_KID_A)
    is_error, reply = connector(**_sealing(current)).call(
        _TOOL, {"name": _UNGRANTED, "restart_at": _EARLIER}
    )

    assert is_error is True
    assert reply is None or reply.get("ok") is not True
    assert state.writes() == []
    assert _ANNOTATION not in state.deployments[_UNGRANTED].template_annotations


# --------------------------------------------------------------------------- #
# Startup and logging.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "env",
    [
        {"SNAPSHOT_SEALING_KEY": "example-key-a:not-base64!"},
        {"SNAPSHOT_SEALING_KEY": f"example-key-a:{base64.b64encode(b'short').decode()}"},
        {"K8S_TIMEOUT_SECONDS": "soon"},
    ],
    ids=["key_not_base64", "key_wrong_length", "timeout_not_a_number"],
)
def test_a_malformed_configuration_refuses_to_start(connector: Any, env: dict[str, str]) -> None:
    process = connector(**env)
    assert process.process.wait(timeout=10) == 1
    assert "refusing to start" in process.stop()


def test_nothing_secret_reaches_the_log(connector: Any) -> None:
    current, _ = _key(_KID_A)
    process = connector(**_sealing(current))
    reply = _restart(process, _EARLIER)
    output = process.stop()

    assert _TOKEN not in output
    assert current.split(":", 1)[1] not in output
    assert reply["prior"]["ciphertext"] not in output
    assert reply["version"] not in output
    assert _SECRET_ENV_VALUE not in output
