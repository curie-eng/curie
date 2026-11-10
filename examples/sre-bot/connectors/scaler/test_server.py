"""Behavior of the scaler connector: an acting connector the platform can qualify.

@spec ACTION-EXECUTOR-9 @spec ACTION-EXECUTOR-13 @spec ACTION-EXECUTOR-15
@spec ACTION-EXECUTOR-16 @spec AUTOMATED-REMEDIATION-22. Every test starts
``server.py`` as a real process and drives it through a real MCP client, so
what is pinned is the wire behavior the runner's ``list``, ``observe`` and
``call`` phases meet. The replies are then judged by the production readers
that consume them: the runner's ``OutboundRedactor`` and ``restore_refusal``,
the worker's ``_snapshot``, ``restore_call`` and ``call_outcome``.

The Kubernetes API is a local stand-in for the ``deployments/scale``
subresource. Its shape is the one observed from a v1.35 API server on a
disposable single-node cluster (recorded in the pull request that added this
connector):

* ``GET .../deployments/<name>/scale`` answers an ``autoscaling/v1`` ``Scale``
  whose ``metadata`` carries ``name``, ``namespace``, ``uid``,
  ``resourceVersion`` and ``creationTimestamp`` and no ``generation``;
* a stopped Deployment's ``Scale`` has ``"spec": {}``: zero replicas is
  omitted, not written as ``0``;
* scaling to the count already in force leaves ``resourceVersion`` unchanged;
* a ``PUT`` carrying a stale ``metadata.resourceVersion`` answers ``409``;
* ``resourceVersion`` also moves while the Deployment controller writes status
  after a change, with the count unchanged.

The stand-in also answers ``403`` for a Deployment outside its granted names,
as ``resourceNames`` in ``manifests/scaler-access.yaml`` makes a real API server
do.
"""

from __future__ import annotations

import asyncio
import base64
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
from curie_worker import action_executor, connector_grant
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
_TOKEN = "example-scaler-token"
_KID_A = "example-key-a"
_KID_B = "example-key-b"
_TOOL = "scale_deployment"


def _status_write(deployment: _Deployment) -> None:
    """The controller writing status: ``resourceVersion`` moves, the count does not."""

    deployment.resource_version += 1


def _scaled_to(replicas: int) -> Callable[[_Deployment], None]:
    """A person scaling the Deployment."""

    def change(deployment: _Deployment) -> None:
        deployment.replicas = replicas
        deployment.resource_version += 1

    return change


def _key(kid: str) -> tuple[str, bytes]:
    raw = os.urandom(32)
    return f"{kid}:{base64.b64encode(raw).decode('ascii')}", raw


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


# --------------------------------------------------------------------------- #
# The Kubernetes API stand-in.
# --------------------------------------------------------------------------- #


class _Deployment:
    def __init__(self, replicas: int) -> None:
        self.uid = str(uuid.uuid4())
        self.resource_version = 100
        self.replicas = replicas


class _Api:
    """The ``deployments/scale`` subresource of one namespace, as observed."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.deployments: dict[str, _Deployment] = {
            _NAME: _Deployment(0),
            _OTHER: _Deployment(2),
            _UNGRANTED: _Deployment(1),
        }
        # The names the credential's Role grants (``resourceNames``).
        self.granted = {_NAME, _OTHER}
        self.requests: list[tuple[str, str, dict[str, Any] | None]] = []
        # Applied after successive reads, one per read: a concurrent writer
        # between the connector's read and its write.
        self.after_get: list[Callable[[_Deployment], None]] = []

    def scale(self, name: str) -> dict[str, Any]:
        deployment = self.deployments[name]
        spec: dict[str, Any] = {}
        if deployment.replicas:
            spec["replicas"] = deployment.replicas
        return {
            "kind": "Scale",
            "apiVersion": "autoscaling/v1",
            "metadata": {
                "name": name,
                "namespace": _NAMESPACE,
                "uid": deployment.uid,
                "resourceVersion": str(deployment.resource_version),
                "creationTimestamp": "2026-10-20T00:00:00Z",
            },
            "spec": spec,
            "status": {"replicas": deployment.replicas, "selector": f"app={name}"},
        }

    def set(self, name: str, replicas: int) -> None:
        """An out-of-band change, as a person with ``kubectl scale`` makes one."""

        with self.lock:
            deployment = self.deployments[name]
            if deployment.replicas != replicas:
                deployment.replicas = replicas
                deployment.resource_version += 1

    def recreate(self, name: str) -> None:
        """Delete and recreate under the same name: a new uid."""

        with self.lock:
            replicas = self.deployments[name].replicas
            self.deployments[name] = _Deployment(replicas)

    def writes(self) -> list[dict[str, Any] | None]:
        return [body for method, _path, body in self.requests if method == "PUT"]


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
            if not (self.path.startswith(prefix) and self.path.endswith("/scale")):
                self._status(404, "NotFound")
                return None
            name = self.path[len(prefix) : -len("/scale")]
            if name not in api.granted:
                self._status(403, "Forbidden")
                return None
            if name not in api.deployments:
                self._status(404, "NotFound")
                return None
            return name

        def do_GET(self) -> None:  # noqa: N802 - http.server's naming
            api.requests.append(("GET", self.path, None))
            name = self._name()
            if name is None:
                return
            with api.lock:
                payload = api.scale(name)
                if api.after_get:
                    api.after_get.pop(0)(api.deployments[name])
            self._send(200, payload)

        def do_PUT(self) -> None:  # noqa: N802 - http.server's naming
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length) or b"null")
            api.requests.append(("PUT", self.path, body))
            name = self._name()
            if name is None:
                return
            with api.lock:
                deployment = api.deployments[name]
                metadata = body.get("metadata") or {}
                expected = metadata.get("resourceVersion")
                if expected is not None and expected != str(deployment.resource_version):
                    self._status(409, "Conflict")
                    return
                replicas = (body.get("spec") or {}).get("replicas", 0)
                if deployment.replicas != replicas:
                    deployment.replicas = replicas
                    deployment.resource_version += 1
                payload = api.scale(name)
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
            if not key.startswith(("SNAPSHOT_SEALING_", "SCALER_", "KUBECONFIG"))
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
                time.sleep(0.05)
        self.stop()
        pytest.fail("scaler connector did not listen within 20s")

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
                "users": [{"name": "sre-bot-scaler", "user": {"token": _TOKEN}}],
                "contexts": [
                    {"name": "example", "context": {"cluster": "example", "user": "sre-bot-scaler"}}
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
            "SCALER_NAMESPACE": _NAMESPACE,
            "SCALER_DEPLOYMENTS": f"{_NAME},{_OTHER},{_UNGRANTED}",
            "SCALER_MAX_REPLICAS": "3",
        }
        merged.update(env)
        process = _Connector(merged)
        started.append(process)
        return process

    yield start
    for running in started:
        running.stop()


def _scale(process: _Connector, replicas: int, name: str = _NAME) -> dict[str, Any]:
    is_error, reply = process.call(_TOOL, {"name": name, "replicas": replicas})
    assert is_error is False, reply
    assert isinstance(reply, dict) and reply.get("ok") is True, reply
    return reply


def _recorded(reply: dict[str, Any]) -> Any:
    """The worker's record of ``reply`` after it crossed the runner's redactor."""

    line = json.dumps(
        {
            "type": "side_effect_flag",
            "tool": f"mcp__scaler__{_TOOL}",
            "call_id": "toolu_example",
            "arguments": {"name": _NAME, "replicas": 1},
            "result": reply,
        }
    )
    (emitted,) = OutboundRedactor(frozenset({_TOKEN})).push(line)
    return _snapshot(SideEffectFlag.model_validate(json.loads(emitted)))


def _restore_arguments(
    process: _Connector, target: dict[str, Any], prior_state: dict[str, Any], recorded: str
) -> dict[str, Any]:
    """The restore arguments as the worker builds them from the live schema."""

    schema = process.tools()["restore"].input_schema
    ruled = connector_grant.canonical_arguments({"target": target, "prior_state": prior_state})
    text = action_executor.restore_call(
        target=target,
        prior_state=prior_state,
        recorded_version=recorded,
        restore_input_schema=schema,
        arguments_sha256=connector_grant.arguments_sha256(ruled),
    )
    return json.loads(text)


def _outcome(reply: tuple[bool, Any]) -> tuple[str, str | None]:
    is_error, structured = reply
    return action_executor.call_outcome(is_error=is_error, structured=structured)


def _sealing(current: str | None = None, *retained: str) -> dict[str, str]:
    env: dict[str, str] = {}
    if current is not None:
        env["SNAPSHOT_SEALING_KEY"] = current
    if retained:
        env["SNAPSHOT_SEALING_KEYS_RETAINED"] = ",".join(retained)
    return env


# --------------------------------------------------------------------------- #
# The advertised surface (ACTION-EXECUTOR-13).
# --------------------------------------------------------------------------- #


def test_the_connector_advertises_one_write_and_the_restore_capable_pair(connector: Any) -> None:
    """@spec ACTION-EXECUTOR-13: the runner's probe finds the pair capable."""

    current, _ = _key(_KID_A)
    tools = connector(**_sealing(current)).tools()
    assert set(tools) == {_TOOL, "observe_version", "restore"}

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
    assert restore_refusal(probed) is None
    assert "expected_version" in tools["restore"].input_schema["properties"]

    write = probed[_TOOL]
    assert write.read_only is False
    assert write.annotations.get("idempotentHint") is True
    # Flat scalar arguments: a remediation policy bounds only JSON scalars, so
    # the target argument must be the Deployment name itself.
    assert set(write.input_schema["required"]) == {"name", "replicas"}
    assert set(write.input_schema["properties"]) == {"name", "replicas"}


# --------------------------------------------------------------------------- #
# The forward write's sealed reply (ACTION-EXECUTOR-9).
# --------------------------------------------------------------------------- #


def test_scaling_a_stopped_deployment_replies_sealed_and_the_worker_records_it(
    connector: Any, api: tuple[_Api, str]
) -> None:
    """@spec ACTION-EXECUTOR-9: ``prior``, ``version`` and ``target``, recorded.

    The stopped Deployment's ``Scale`` omits ``spec.replicas`` (observed); the
    prior state sealed for it is zero, not a refusal.
    """

    state, _ = api
    current, raw = _key(_KID_A)
    reply = _scale(connector(**_sealing(current)), 1)

    assert state.deployments[_NAME].replicas == 1
    assert reply["target"] == _TARGET
    prior = reply["prior"]
    assert set(prior) == set(_ENVELOPE["keys"])
    assert prior["sealed"] == _ENVELOPE["sealed"]
    assert prior["kid"] == _KID_A
    sealed = base64.b64decode(prior["ciphertext"], validate=True)
    opened = json.loads(AESGCM(raw).decrypt(sealed[:12], sealed[12:], _canonical(_TARGET)))
    assert opened == {"replicas": 0, "uid": state.deployments[_NAME].uid}
    assert "replicas" not in json.dumps(prior)

    version = reply["version"]
    assert isinstance(version, str) and 0 < len(version) <= _ENVELOPE["version_max_length"]
    assert all(0x20 <= ord(ch) <= 0x7E for ch in version)

    # The write carried the read's resourceVersion: a compare-and-swap, never blind.
    (written,) = state.writes()
    assert written is not None
    assert written["metadata"]["resourceVersion"] == "100"
    assert written["spec"] == {"replicas": 1}

    recorded = _recorded(reply)
    assert recorded.prior_state == prior
    assert recorded.post_version == version
    assert recorded.target == _TARGET


def test_repeating_the_same_scale_leaves_the_same_post_version(
    connector: Any, api: tuple[_Api, str]
) -> None:
    """@spec AUTOMATED-REMEDIATION-22: two forwards with the same arguments.

    An idempotent qualification needs two confirmed forwards with the same
    canonical arguments whose records left the same ``post_version``. The
    version names the state the write left, so the repeat names the same one,
    and the repeat writes nothing because the count is already in force.
    """

    state, _ = api
    current, raw = _key(_KID_A)
    process = connector(**_sealing(current))
    first = _scale(process, 1)
    second = _scale(process, 1)

    assert _outcome((False, first)) == ("confirmed", None)
    assert _outcome((False, second)) == ("confirmed", None)
    assert _recorded(first).post_version == _recorded(second).post_version
    assert len(state.writes()) == 1
    sealed = base64.b64decode(second["prior"]["ciphertext"], validate=True)
    opened = json.loads(AESGCM(raw).decrypt(sealed[:12], sealed[12:], _canonical(_TARGET)))
    assert opened["replicas"] == 1

    # A different count is a different state and so a different version.
    third = _scale(process, 2)
    assert third["version"] != first["version"]


def test_observe_version_reports_the_version_a_write_left_until_someone_changes_it(
    connector: Any, api: tuple[_Api, str]
) -> None:
    """@spec ACTION-EXECUTOR-15: ``{"version": ...}``, read-only, of the live state."""

    state, _ = api
    current, _ = _key(_KID_A)
    process = connector(**_sealing(current))
    reply = _scale(process, 1)
    writes = len(state.writes())

    observed = process.call("observe_version", action_executor.observe_arguments(_TARGET))
    assert observed == (False, {"version": reply["version"]})

    state.set(_NAME, 3)
    is_error, changed = process.call("observe_version", {"target": _TARGET})
    assert is_error is False
    assert changed["version"] != reply["version"]

    state.recreate(_NAME)
    state.set(_NAME, 1)
    is_error, recreated = process.call("observe_version", {"target": _TARGET})
    assert is_error is False
    # Same count, different object: not the version the write left.
    assert recreated["version"] != reply["version"]
    assert len(state.writes()) == writes


def test_a_concurrent_change_between_read_and_write_refuses_with_nothing_written(
    connector: Any, api: tuple[_Api, str]
) -> None:
    """@spec ACTION-EXECUTOR-20: someone moved the count, so ``version_conflict``."""

    state, _ = api
    current, _ = _key(_KID_A)
    state.after_get = [_scaled_to(3)]
    reply = connector(**_sealing(current)).call(_TOOL, {"name": _NAME, "replicas": 1})

    assert reply == (False, {"ok": False, "refused": "version_conflict"})
    assert _outcome(reply) == ("failed", "version_conflict_at_write")
    assert state.deployments[_NAME].replicas == 3


def test_a_status_write_between_read_and_write_is_retried(
    connector: Any, api: tuple[_Api, str]
) -> None:
    """A ``409`` from the controller's status write alone is not a conflict.

    Observed: ``resourceVersion`` keeps moving while the controller reports
    status after a change, so a raw ``resourceVersion`` swap would refuse
    spuriously. The retry writes only while the uid and count are unchanged.
    """

    state, _ = api
    current, _ = _key(_KID_A)
    state.after_get = [_status_write]
    reply = _scale(connector(**_sealing(current)), 1)

    assert state.deployments[_NAME].replicas == 1
    assert len(state.writes()) == 2
    assert reply["version"].endswith("replicas=1")


def test_status_writes_that_never_settle_refuse_with_nothing_written(
    connector: Any, api: tuple[_Api, str]
) -> None:
    state, _ = api
    current, _ = _key(_KID_A)
    state.after_get = [_status_write] * 10
    reply = connector(**_sealing(current)).call(_TOOL, {"name": _NAME, "replicas": 1})

    assert reply == (False, {"ok": False, "refused": "version_conflict"})
    assert state.deployments[_NAME].replicas == 0
    assert len(state.writes()) == 3


# --------------------------------------------------------------------------- #
# Forward refusals: the allowlist, the ceiling and key custody.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("arguments", "env"),
    [
        ({"name": "example-unlisted", "replicas": 1}, {}),
        ({"name": _NAME, "replicas": 1}, {"SCALER_DEPLOYMENTS": ""}),
        ({"name": _NAME, "replicas": 1}, {"SCALER_NAMESPACE": ""}),
        ({"name": _NAME, "replicas": 4}, {}),
        ({"name": _NAME, "replicas": -1}, {}),
        ({"name": _NAME, "replicas": True}, {}),
        ({"name": _NAME, "replicas": "1"}, {}),
    ],
    ids=[
        "name_not_allowlisted",
        "no_allowlist",
        "no_namespace",
        "above_ceiling",
        "negative",
        "boolean",
        "string",
    ],
)
def test_a_call_outside_the_configured_bounds_reaches_no_api(
    connector: Any, api: tuple[_Api, str], arguments: dict[str, Any], env: dict[str, str]
) -> None:
    """Refused as a tool error before any request, so nothing is read or written."""

    state, _ = api
    current, _ = _key(_KID_A)
    is_error, reply = connector(**_sealing(current), **env).call(_TOOL, arguments)

    assert is_error is True
    assert reply is None or reply.get("ok") is not True
    assert state.requests == []


def test_without_a_sealing_key_the_forward_write_refuses_and_writes_nothing(
    connector: Any, api: tuple[_Api, str]
) -> None:
    """@spec ACTION-EXECUTOR-16: no unsealed fallback."""

    state, _ = api
    reply = connector().call(_TOOL, {"name": _NAME, "replicas": 1})

    assert reply == (False, {"ok": False, "refused": "sealing_key_unavailable"})
    assert _outcome(reply) == ("failed", "sealing_key_unavailable")
    assert state.writes() == []
    assert state.deployments[_NAME].replicas == 0


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
        _TOOL, {"name": _UNGRANTED, "replicas": 2}
    )

    assert is_error is True
    assert reply is None or reply.get("ok") is not True
    assert state.writes() == []
    assert state.deployments[_UNGRANTED].replicas == 1


def test_observe_version_refuses_a_target_outside_the_bounds(
    connector: Any, api: tuple[_Api, str]
) -> None:
    """A target of another kind, namespace or name is never read."""

    state, _ = api
    current, _ = _key(_KID_A)
    process = connector(**_sealing(current))
    for target in (
        {**_TARGET, "kind": "StatefulSet"},
        {**_TARGET, "namespace": "example-other"},
        {**_TARGET, "name": "example-unlisted"},
        {**_TARGET, "extra": "x"},
    ):
        is_error, reply = process.call("observe_version", {"target": target})
        assert is_error is True, target
        assert action_executor.call_outcome(is_error=is_error, structured=reply)[0] == "failed"
    assert state.requests == []


# --------------------------------------------------------------------------- #
# The restore (ACTION-EXECUTOR-15, -16).
# --------------------------------------------------------------------------- #


def test_a_restore_puts_the_prior_count_back(connector: Any, api: tuple[_Api, str]) -> None:
    """@spec ACTION-EXECUTOR-15: the worker's call text, confirmed, state restored."""

    state, _ = api
    current, _ = _key(_KID_A)
    process = connector(**_sealing(current))
    forward = _scale(process, 1)
    recorded = _recorded(forward)

    arguments = _restore_arguments(process, _TARGET, recorded.prior_state, recorded.post_version)
    assert arguments["expected_version"] == forward["version"]
    reply = process.call("restore", arguments)

    assert _outcome(reply) == ("confirmed", None)
    assert reply[1]["target"] == _TARGET
    assert state.deployments[_NAME].replicas == 0
    assert state.writes()[-1]["spec"] == {"replicas": 0}  # type: ignore[index]
    # The restore leaves a new version, observable as such.
    assert process.call("observe_version", {"target": _TARGET}) == (
        False,
        {"version": reply[1]["version"]},
    )


def test_a_restore_refuses_after_someone_changed_the_count(
    connector: Any, api: tuple[_Api, str]
) -> None:
    """@spec ACTION-EXECUTOR-15: ``version_conflict``, and a person's fix stays."""

    state, _ = api
    current, _ = _key(_KID_A)
    process = connector(**_sealing(current))
    forward = _scale(process, 1)
    recorded = _recorded(forward)
    state.set(_NAME, 3)
    writes = len(state.writes())

    arguments = _restore_arguments(process, _TARGET, recorded.prior_state, recorded.post_version)
    reply = process.call("restore", arguments)

    assert reply == (False, {"ok": False, "refused": "version_conflict"})
    assert _outcome(reply) == ("failed", "version_conflict_at_write")
    assert state.deployments[_NAME].replicas == 3
    assert len(state.writes()) == writes


def test_a_restore_refuses_a_recreated_deployment_even_at_the_same_count(
    connector: Any, api: tuple[_Api, str]
) -> None:
    """The sealed uid must be the live one, with or without ``expected_version``."""

    state, _ = api
    current, _ = _key(_KID_A)
    process = connector(**_sealing(current))
    forward = _scale(process, 1)
    state.recreate(_NAME)

    reply = process.call("restore", {"target": _TARGET, "prior_state": forward["prior"]})

    assert reply == (False, {"ok": False, "refused": "version_conflict"})
    assert state.deployments[_NAME].replicas == 1


@pytest.mark.parametrize("with_expected_version", [True, False])
def test_a_concurrent_change_during_a_restore_refuses(
    connector: Any, api: tuple[_Api, str], with_expected_version: bool
) -> None:
    """The restore's own write is a compare-and-swap on the count it checked."""

    state, _ = api
    current, _ = _key(_KID_A)
    process = connector(**_sealing(current))
    forward = _scale(process, 1)
    arguments: dict[str, Any] = {"target": _TARGET, "prior_state": forward["prior"]}
    if with_expected_version:
        arguments["expected_version"] = forward["version"]
    state.after_get = [_scaled_to(3)]

    reply = process.call("restore", arguments)

    assert reply == (False, {"ok": False, "refused": "version_conflict"})
    assert state.deployments[_NAME].replicas == 3


def test_a_status_write_during_a_restore_is_retried(connector: Any, api: tuple[_Api, str]) -> None:
    """A spurious ``409`` must not fail the restore: a failed restore blocks the undo."""

    state, _ = api
    current, _ = _key(_KID_A)
    process = connector(**_sealing(current))
    forward = _scale(process, 1)
    state.after_get = [_status_write]

    reply = process.call(
        "restore",
        {
            "target": _TARGET,
            "prior_state": forward["prior"],
            "expected_version": forward["version"],
        },
    )

    assert _outcome(reply) == ("confirmed", None)
    assert state.deployments[_NAME].replicas == 0


def test_a_restore_refuses_an_envelope_it_cannot_open(
    connector: Any, api: tuple[_Api, str]
) -> None:
    """@spec ACTION-EXECUTOR-16: grammar, kid, then the open; nothing written."""

    state, _ = api
    current, _ = _key(_KID_A)
    process = connector(**_sealing(current))
    prior = _scale(process, 1)["prior"]
    writes = len(state.writes())

    other = {**_TARGET, "name": _OTHER}
    tampered = dict(prior)
    raw = bytearray(base64.b64decode(prior["ciphertext"]))
    raw[-1] ^= 0x01
    tampered["ciphertext"] = base64.b64encode(bytes(raw)).decode("ascii")
    cases = [
        (
            {"target": _TARGET, "prior_state": {"sealed": "curie.snapshot.v1"}},
            "snapshot_unopenable",
        ),
        ({"target": _TARGET, "prior_state": tampered}, "snapshot_unopenable"),
        ({"target": other, "prior_state": prior}, "snapshot_unopenable"),
        (
            {"target": _TARGET, "prior_state": {**prior, "kid": "example-key-z"}},
            "sealing_key_unavailable",
        ),
    ]
    for arguments, code in cases:
        reply = process.call("restore", arguments)
        assert reply == (False, {"ok": False, "refused": code}), arguments
        assert _outcome(reply) == ("failed", code)
    assert len(state.writes()) == writes
    assert state.deployments[_OTHER].replicas == 2


def test_a_retained_key_still_opens_after_rotation(connector: Any, api: tuple[_Api, str]) -> None:
    """@spec ACTION-EXECUTOR-16: ``SNAPSHOT_SEALING_KEYS_RETAINED`` opens, never seals."""

    state, _ = api
    old, _ = _key(_KID_A)
    new, _ = _key(_KID_B)
    prior = _scale(connector(**_sealing(old)), 1)["prior"]

    rotated = connector(**_sealing(new, old))
    assert _scale(rotated, 2)["prior"]["kid"] == _KID_B
    state.set(_NAME, 1)
    reply = rotated.call("restore", {"target": _TARGET, "prior_state": prior})
    assert _outcome(reply) == ("confirmed", None)
    assert state.deployments[_NAME].replicas == 0

    dropped = connector(**_sealing(new))
    assert dropped.call("restore", {"target": _TARGET, "prior_state": prior}) == (
        False,
        {"ok": False, "refused": "sealing_key_unavailable"},
    )


# --------------------------------------------------------------------------- #
# Startup and logging.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "env",
    [
        {"SNAPSHOT_SEALING_KEY": "example-key-a:not-base64!"},
        {"SNAPSHOT_SEALING_KEY": f"example-key-a:{base64.b64encode(b'short').decode()}"},
        {"SCALER_MAX_REPLICAS": "many"},
        {"SCALER_MAX_REPLICAS": "-1"},
    ],
    ids=["key_not_base64", "key_wrong_length", "ceiling_not_a_number", "ceiling_negative"],
)
def test_a_malformed_configuration_refuses_to_start(connector: Any, env: dict[str, str]) -> None:
    process = connector(**env)
    assert process.process.wait(timeout=10) == 1
    assert "refusing to start" in process.stop()


def test_nothing_secret_reaches_the_log(connector: Any) -> None:
    current, _ = _key(_KID_A)
    process = connector(**_sealing(current))
    reply = _scale(process, 1)
    process.call("restore", {"target": _TARGET, "prior_state": reply["prior"]})
    output = process.stop()

    assert _TOKEN not in output
    assert current.split(":", 1)[1] not in output
    assert reply["prior"]["ciphertext"] not in output
    assert reply["version"] not in output
