"""Behavior of the reference reversible connector (plan task 7).

@spec ACTION-EXECUTOR-9 @spec ACTION-EXECUTOR-13 @spec ACTION-EXECUTOR-15
@spec ACTION-EXECUTOR-16. The fixture is a hosted connector: ``server.py`` in
this directory serves MCP over streamable HTTP, and the e2e ladder copies the
directory into a bundle for the cluster tier. Every test here starts it as a
real process and drives it through a real MCP client, so what is pinned is the
wire behavior the runner's ``list``, ``observe`` and ``call`` phases meet.

The disposable resource at this tier is the JSON record store named by
``REFERENCE_STORE_PATH`` (``{"<namespace>/<name>": {"replicas", "version"}}``);
it outlives a process, which is what the key rotation tests need.

Frozen inputs are read through the production readers that consume them:
``tests/vectors/sealed-snapshot-reply.json`` (the envelope grammar, judged by
the worker's ``_snapshot``), ``tests/vectors/executor-restore-calls.json``
(observe arguments and the refusal shapes, mapped by the worker's
``call_outcome``), and the restore call text is built by the worker's
``restore_call`` from the live ``restore`` input schema.

Sealing contract the fixture is held to (key custody is AE-16; the cipher is
the fixture's own choice, pinned here so the reply can be opened
independently): ``SNAPSHOT_SEALING_KEY`` is ``<kid>:<base64 of 32 bytes>``,
``SNAPSHOT_SEALING_KEYS_RETAINED`` a comma separated list of the same form;
``ciphertext`` is ``base64(nonce[12] || AES-256-GCM(ct || tag))`` with the
canonical JSON of ``target`` as associated data and ``{"replicas": n}`` as
plaintext.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import json
import os
import re
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import SideEffectFlag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from curie_worker import action_executor, connector_grant
from curie_worker.actions import _snapshot
from mcp import Client

_HERE = Path(__file__).resolve().parent
_SERVER = _HERE / "server.py"
_VECTORS = _HERE.parents[3] / "tests" / "vectors"
_SEALED = json.loads((_VECTORS / "sealed-snapshot-reply.json").read_text("utf-8"))
_CALLS = json.loads((_VECTORS / "executor-restore-calls.json").read_text("utf-8"))

_TARGET: dict[str, Any] = _CALLS["observe_arguments"]["target"]
_KEY = f"{_TARGET['namespace']}/{_TARGET['name']}"
_OTHER_TARGET = {**_TARGET, "name": "example-api"}
_OTHER_KEY = f"{_OTHER_TARGET['namespace']}/{_OTHER_TARGET['name']}"
_REFUSAL = {entry["name"]: entry for entry in _CALLS["call_replies"]}
_ENVELOPE = _SEALED["envelope"]

_KID_A = "example-key-a"
_KID_B = "example-key-b"


def _key(kid: str) -> tuple[str, bytes]:
    raw = os.urandom(32)
    return f"{kid}:{base64.b64encode(raw).decode('ascii')}", raw


# --------------------------------------------------------------------------- #
# Harness: the fixture as a real process, a real MCP client against it.
# --------------------------------------------------------------------------- #


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


class _Connector:
    """One fixture process; stopped by the ``connector`` fixture's teardown."""

    def __init__(self, store: Path, env: dict[str, str]) -> None:
        if not _SERVER.is_file():
            pytest.fail(f"reference connector fixture missing: {_SERVER} does not exist")
        self.store = store
        port = _free_port()
        child_env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("SNAPSHOT_SEALING_", "REFERENCE_"))
        }
        child_env.update({"BIND_ADDRESS": "127.0.0.1", "PORT": str(port)})
        child_env["REFERENCE_STORE_PATH"] = str(store)
        child_env.update(env)
        self.process = subprocess.Popen(
            [sys.executable, str(_SERVER)],
            env=child_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        self.url = f"http://127.0.0.1:{port}/mcp"
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                output = self.process.stdout.read().decode("utf-8", "replace")  # type: ignore[union-attr]
                pytest.fail(f"reference connector exited {self.process.returncode}: {output}")
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                return
            except OSError:
                time.sleep(0.05)
        self.stop()
        pytest.fail("reference connector did not listen within 20s")

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.process.stdout is not None:
            self.process.stdout.close()

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

    def record(self, key: str = _KEY) -> dict[str, Any]:
        return json.loads(self.store.read_text("utf-8"))[key]


@pytest.fixture
def connector(tmp_path: Path) -> Iterator[Any]:
    """Start a fixture process over a store shared by every process of the test."""

    store = tmp_path / "store.json"
    store.write_text(
        json.dumps(
            {
                _KEY: {"replicas": 3, "version": "seed-1"},
                _OTHER_KEY: {"replicas": 1, "version": "seed-1"},
            }
        ),
        "utf-8",
    )
    started: list[_Connector] = []

    def start(**env: str) -> _Connector:
        for running in started:
            running.stop()
        process = _Connector(store, env)
        started.append(process)
        return process

    yield start
    for running in started:
        running.stop()


def _sealing_env(
    current: str | None, retained: tuple[str, ...] = (), **extra: str
) -> dict[str, str]:
    env = dict(extra)
    if current is not None:
        env["SNAPSHOT_SEALING_KEY"] = current
    if retained:
        env["SNAPSHOT_SEALING_KEYS_RETAINED"] = ",".join(retained)
    return env


def _scale(process: _Connector, replicas: int, target: dict[str, Any] = _TARGET) -> dict[str, Any]:
    is_error, reply = process.call("scale", {"target": target, "replicas": replicas})
    assert is_error is False, reply
    assert isinstance(reply, dict) and reply.get("ok") is True, reply
    return reply


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


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


# --------------------------------------------------------------------------- #
# ACTION-EXECUTOR-9: the sealed reply.
# --------------------------------------------------------------------------- #


def test_a_forward_write_replies_with_a_sealed_envelope_the_worker_records(connector: Any) -> None:
    """@spec ACTION-EXECUTOR-9: ``prior``, ``version`` and ``target``, recorded."""

    current, _ = _key(_KID_A)
    process = connector(**_sealing_env(current))
    reply = _scale(process, 5)

    prior = reply["prior"]
    assert set(prior) == set(_ENVELOPE["keys"])
    assert prior["sealed"] == _ENVELOPE["sealed"]
    assert prior["kid"] == _KID_A
    assert re.fullmatch(_ENVELOPE["kid_pattern"], prior["kid"])
    assert "\n" not in prior["ciphertext"]
    assert 0 < len(base64.b64decode(prior["ciphertext"], validate=True)) <= 65536
    assert reply["target"] == _TARGET
    version = reply["version"]
    assert isinstance(version, str) and 0 < len(version) <= _ENVELOPE["version_max_length"]
    assert version != "seed-1"
    assert _ENVELOPE["redaction_placeholder_prefix"] not in json.dumps(reply)

    frame = SideEffectFlag(
        tool="mcp__example-scale__scale",
        call_id="toolu_example",
        arguments={"target": _TARGET, "replicas": 5},
        result=reply,
        redacted=False,
    )
    recorded = _snapshot(frame)
    assert recorded.prior_state == prior
    assert recorded.post_version == version
    assert process.record() == {"replicas": 5, "version": version}


def test_the_ciphertext_opens_with_the_key_and_holds_the_prior_state(connector: Any) -> None:
    """@spec ACTION-EXECUTOR-9 @spec ACTION-EXECUTOR-16: sealed, not cleartext."""

    current, raw = _key(_KID_A)
    process = connector(**_sealing_env(current))
    reply = _scale(process, 7)

    sealed = base64.b64decode(reply["prior"]["ciphertext"], validate=True)
    nonce, body = sealed[:12], sealed[12:]
    opened = json.loads(AESGCM(raw).decrypt(nonce, body, _canonical(_TARGET)))
    assert opened == {"replicas": 3}
    assert b'"replicas"' not in sealed
    assert "replicas" not in json.dumps(reply["prior"])

    # A second write seals with a fresh nonce.
    again = _scale(process, 8)
    assert again["prior"]["ciphertext"][:16] != reply["prior"]["ciphertext"][:16]


# --------------------------------------------------------------------------- #
# ACTION-EXECUTOR-13: the advertised pair.
# --------------------------------------------------------------------------- #


def _restore_capable(tools: dict[str, Any]) -> bool:
    """AE-13's rule over a live tool list, stated from the spec (no API reader yet)."""

    restore = tools.get("restore")
    observe = tools.get("observe_version")
    if restore is None or observe is None:
        return False
    restore_ro = restore.annotations is not None and restore.annotations.read_only_hint is True
    observe_ro = observe.annotations is not None and observe.annotations.read_only_hint is True
    return (
        not restore_ro
        and {"target", "prior_state"} <= set(restore.input_schema.get("required", []))
        and observe_ro
        and "target" in set(observe.input_schema.get("required", []))
    )


def test_the_conforming_connector_advertises_the_capable_pair(connector: Any) -> None:
    """@spec ACTION-EXECUTOR-13: ``restore`` and a read-only ``observe_version``."""

    current, _ = _key(_KID_A)
    tools = connector(**_sealing_env(current)).tools()
    assert {"scale", "restore", "observe_version"} <= set(tools)
    assert _restore_capable(tools)
    assert "expected_version" in tools["restore"].input_schema["properties"]
    assert tools["scale"].annotations.read_only_hint is not True


def test_the_unpaired_variant_advertises_restore_without_observe_version(connector: Any) -> None:
    """@spec ACTION-EXECUTOR-13: a lone ``restore`` is not capable."""

    current, _ = _key(_KID_A)
    tools = connector(**_sealing_env(current, REFERENCE_CONNECTOR_VARIANT="unpaired")).tools()
    assert "restore" in tools
    assert {"target", "prior_state"} <= set(tools["restore"].input_schema["required"])
    assert "observe_version" not in tools
    assert not _restore_capable(tools)


# --------------------------------------------------------------------------- #
# ACTION-EXECUTOR-15: observe, then compare-and-swap at the connector.
# --------------------------------------------------------------------------- #


def test_observe_version_reports_the_version_the_write_left(connector: Any) -> None:
    """@spec ACTION-EXECUTOR-15: the read verb returns the live version."""

    current, _ = _key(_KID_A)
    process = connector(**_sealing_env(current))
    reply = _scale(process, 5)

    is_error, observed = process.call(
        _CALLS["observe_tool"], action_executor.observe_arguments(_TARGET)
    )
    assert is_error is False
    assert observed["version"] == reply["version"]

    moved = _scale(process, 6)
    _, observed = process.call(_CALLS["observe_tool"], action_executor.observe_arguments(_TARGET))
    assert observed["version"] == moved["version"] != reply["version"]


def test_restore_with_the_matching_expected_version_restores(connector: Any) -> None:
    """@spec ACTION-EXECUTOR-15: equality restores and reports the post version."""

    current, _ = _key(_KID_A)
    process = connector(**_sealing_env(current))
    reply = _scale(process, 5)

    arguments = _restore_arguments(process, _TARGET, reply["prior"], reply["version"])
    assert arguments["expected_version"] == reply["version"]
    is_error, restored = process.call(_CALLS["restore_tool"], arguments)

    assert _outcome((is_error, restored)) == ("confirmed", None)
    assert restored["target"] == _TARGET
    assert restored["version"] not in {reply["version"], "seed-1"}
    assert process.record() == {"replicas": 3, "version": restored["version"]}
    _, observed = process.call("observe_version", {"target": _TARGET})
    assert observed["version"] == restored["version"]


def test_restore_without_expected_version_still_restores(connector: Any) -> None:
    """@spec ACTION-EXECUTOR-15: ``expected_version`` is optional in the schema."""

    current, _ = _key(_KID_A)
    process = connector(**_sealing_env(current))
    reply = _scale(process, 5)
    _scale(process, 6)

    reply_ = process.call("restore", {"target": _TARGET, "prior_state": reply["prior"]})
    assert _outcome(reply_) == ("confirmed", None)
    assert process.record()["replicas"] == 3


def test_a_moved_version_refuses_at_write_and_keeps_the_operators_value(connector: Any) -> None:
    """@spec ACTION-EXECUTOR-15: the CAS refusal is ``version_conflict``, no write."""

    current, _ = _key(_KID_A)
    process = connector(**_sealing_env(current))
    reply = _scale(process, 5)
    operator = _scale(process, 9)

    arguments = _restore_arguments(process, _TARGET, reply["prior"], reply["version"])
    refused = process.call("restore", arguments)

    expected = _REFUSAL["version_conflict_at_write"]
    assert refused == (expected["is_error"], expected["structured"])
    assert _outcome(refused) == (expected["state"], expected["code"])
    assert process.record() == {"replicas": 9, "version": operator["version"]}


def test_the_variant_that_ignores_expected_version_overwrites(connector: Any) -> None:
    """@spec ACTION-EXECUTOR-15: why the platform's own compare is the check."""

    current, _ = _key(_KID_A)
    process = connector(
        **_sealing_env(current, REFERENCE_CONNECTOR_VARIANT="ignores_expected_version")
    )
    tools = process.tools()
    assert "expected_version" in tools["restore"].input_schema["properties"]
    reply = _scale(process, 5)
    _scale(process, 9)

    arguments = _restore_arguments(process, _TARGET, reply["prior"], reply["version"])
    assert arguments["expected_version"] == reply["version"]
    overwritten = process.call("restore", arguments)

    assert _outcome(overwritten) == ("confirmed", None)
    assert process.record()["replicas"] == 3


# --------------------------------------------------------------------------- #
# ACTION-EXECUTOR-16: key custody at the connector.
# --------------------------------------------------------------------------- #


def test_no_sealing_key_refuses_the_forward_write_and_writes_nothing(connector: Any) -> None:
    """@spec ACTION-EXECUTOR-16: no key, no unsealed fallback."""

    process = connector(**_sealing_env(None))
    refused = process.call("scale", {"target": _TARGET, "replicas": 5})

    expected = _REFUSAL["sealing_key_unavailable"]
    assert refused == (expected["is_error"], expected["structured"])
    assert process.record() == {"replicas": 3, "version": "seed-1"}


def test_no_sealing_key_refuses_the_restore(connector: Any) -> None:
    """@spec ACTION-EXECUTOR-16: a missing key surfaces during ``call``."""

    current, _ = _key(_KID_A)
    reply = _scale(connector(**_sealing_env(current)), 5)
    process = connector(**_sealing_env(None))

    refused = process.call("restore", {"target": _TARGET, "prior_state": reply["prior"]})
    expected = _REFUSAL["sealing_key_unavailable"]
    assert refused == (expected["is_error"], expected["structured"])
    assert _outcome(refused) == (expected["state"], expected["code"])
    assert process.record()["replicas"] == 5


def test_a_retained_key_still_restores_an_earlier_record(connector: Any) -> None:
    """@spec ACTION-EXECUTOR-16: rotation keeps old records restorable while retained."""

    old, _ = _key(_KID_A)
    new, _ = _key(_KID_B)
    earlier = _scale(connector(**_sealing_env(old)), 5)

    process = connector(**_sealing_env(new, (old,)))
    # New writes seal under the current key only.
    other = _scale(process, 4, _OTHER_TARGET)
    assert other["prior"]["kid"] == _KID_B

    arguments = _restore_arguments(process, _TARGET, earlier["prior"], earlier["version"])
    restored = process.call("restore", arguments)
    assert _outcome(restored) == ("confirmed", None)
    assert process.record()["replicas"] == 3


def test_a_dropped_key_refuses_the_earlier_record(connector: Any) -> None:
    """@spec ACTION-EXECUTOR-16: drop the retained key and the record is unrestorable."""

    old, _ = _key(_KID_A)
    new, _ = _key(_KID_B)
    earlier = _scale(connector(**_sealing_env(old)), 5)
    process = connector(**_sealing_env(new))

    arguments = _restore_arguments(process, _TARGET, earlier["prior"], earlier["version"])
    refused = process.call("restore", arguments)
    expected = _REFUSAL["sealing_key_unavailable"]
    assert refused == (expected["is_error"], expected["structured"])
    assert process.record()["replicas"] == 5


def test_a_retained_key_never_seals(connector: Any) -> None:
    """@spec ACTION-EXECUTOR-16: retained keys open only; no current key, no write."""

    old, _ = _key(_KID_A)
    process = connector(**_sealing_env(None, (old,)))
    refused = process.call("scale", {"target": _TARGET, "replicas": 5})
    assert refused == (False, _REFUSAL["sealing_key_unavailable"]["structured"])
    assert process.record()["replicas"] == 3


# --------------------------------------------------------------------------- #
# Envelopes the connector cannot open.
# --------------------------------------------------------------------------- #


def test_a_tampered_ciphertext_is_unopenable(connector: Any) -> None:
    """@spec ACTION-EXECUTOR-9: authenticated sealing; a flipped byte is refused."""

    current, _ = _key(_KID_A)
    process = connector(**_sealing_env(current))
    reply = _scale(process, 5)

    raw = bytearray(base64.b64decode(reply["prior"]["ciphertext"]))
    raw[-1] ^= 0x01
    tampered = {**reply["prior"], "ciphertext": base64.b64encode(bytes(raw)).decode("ascii")}
    refused = process.call("restore", {"target": _TARGET, "prior_state": tampered})

    expected = _REFUSAL["snapshot_unopenable"]
    assert refused == (expected["is_error"], expected["structured"])
    assert _outcome(refused) == (expected["state"], expected["code"])
    assert process.record()["replicas"] == 5


def test_an_envelope_is_bound_to_its_target(connector: Any) -> None:
    """@spec ACTION-EXECUTOR-9: one target's snapshot never restores another."""

    current, _ = _key(_KID_A)
    process = connector(**_sealing_env(current))
    reply = _scale(process, 5)

    refused = process.call("restore", {"target": _OTHER_TARGET, "prior_state": reply["prior"]})
    assert refused == (False, _REFUSAL["snapshot_unopenable"]["structured"])
    assert process.record(_OTHER_KEY)["replicas"] == 1


def _vector_priors() -> list[tuple[str, bool, dict[str, Any]]]:
    seen: set[str] = set()
    priors: list[tuple[str, bool, dict[str, Any]]] = []
    for case in _SEALED["vectors"]:
        prior = copy.deepcopy(case["reply"].get("prior"))
        if not isinstance(prior, dict):
            continue
        expand = case.get("expand_ciphertext")
        if expand is not None:
            prior["ciphertext"] = expand["fill"] * expand["length"] + expand["suffix"]
        text = json.dumps(prior, sort_keys=True)
        if text in seen:
            continue
        seen.add(text)
        priors.append((case["name"], case["envelope_valid"], prior))
    return priors


def test_every_vector_envelope_gets_the_grammars_outcome(connector: Any) -> None:
    """@spec ACTION-EXECUTOR-9: the connector judges the shared vector's envelopes.

    An envelope the grammar refuses is ``snapshot_unopenable``; a valid one
    under a kid the connector does not hold is ``sealing_key_unavailable``. No
    vector envelope writes.
    """

    current, _ = _key(_KID_A)
    process = connector(**_sealing_env(current))
    priors = _vector_priors()
    assert any(valid for _, valid, _ in priors) and any(not valid for _, valid, _ in priors)

    outcomes = {
        name: process.call("restore", {"target": _TARGET, "prior_state": prior})
        for name, _, prior in priors
    }
    for name, valid, _ in priors:
        code = "sealing_key_unavailable" if valid else "snapshot_unopenable"
        assert outcomes[name] == (False, _REFUSAL[code]["structured"]), name
    assert process.record() == {"replicas": 3, "version": "seed-1"}
