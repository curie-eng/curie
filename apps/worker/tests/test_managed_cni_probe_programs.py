"""Probe programs and templates of the managed CNI probe kit, ADR 0238 (@spec MANAGED-CNI-2).

The listener and sender programs run here as local processes on loopback. That
proves the record format and attribution only; it observes no policy engine.
"""

from __future__ import annotations

import json
import socket
import subprocess
import sys
import time
from typing import Any

import pytest
from curie_worker.managed_cni_probe_kit import ProbeKitRefused
from curie_worker.managed_cni_probe_programs import (
    LISTENER_PROGRAM,
    PROBE_IMAGE,
    SENDER_PROGRAM,
    TEMPLATES,
    ListenerRecord,
    parse_listener_log,
    parse_sender_log,
    probe_kit_digest,
    probe_pod,
)


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def test_listener_records_nonces_and_sender_reports_each_attempt() -> None:
    port, closed = free_port(), free_port()
    listener = subprocess.Popen(
        [sys.executable, "-I", "-c", LISTENER_PROGRAM, json.dumps({"tcp": [port], "udp": [port]})],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        time.sleep(0.8)
        attempts: list[dict[str, Any]] = [
            {"addr": "127.0.0.1", "port": port, "proto": "tcp", "nonce": "a" * 32},
            {"addr": "127.0.0.1", "port": port, "proto": "udp", "nonce": "b" * 32},
            {"addr": "127.0.0.1", "port": closed, "proto": "tcp", "nonce": "c" * 32},
        ]
        sent = subprocess.run(
            [
                sys.executable,
                "-I",
                "-c",
                SENDER_PROGRAM,
                json.dumps(
                    {"phases": [attempts[:2], attempts[2:]], "interval_ms": 50, "timeout_ms": 500}
                ),
            ],
            capture_output=True,
            text=True,
            timeout=20,
            check=True,
        )
        time.sleep(1.2)
    finally:
        listener.terminate()
        out, _ = listener.communicate(timeout=10)
    sender = parse_sender_log(sent.stdout)
    results = {nonce: result for _addr, _port, _proto, nonce, result in sender.attempts}
    assert results == {"a" * 32: "connected", "b" * 32: "timeout", "c" * 32: "refused"}
    record = parse_listener_log(out)
    assert record.ready
    start, end = sender.start_ms, sender.end_ms + 2000
    assert record.deliveries(nonces=["a" * 32], peers=[], start_ms=start, end_ms=end) == 1
    assert record.deliveries(nonces=["b" * 32], peers=[], start_ms=start, end_ms=end) == 1
    assert record.deliveries(nonces=["c" * 32], peers=[], start_ms=start, end_ms=end) == 0
    assert record.deliveries(nonces=[], peers=["127.0.0.2"], start_ms=start, end_ms=end) == 0
    sampled, covered, _moved = record.window(sender.start_ms, sender.end_ms)
    assert covered
    # Only a Linux network namespace exposes the socket table this samples.
    assert sampled is sys.platform.startswith("linux")


def test_window_requires_ticks_on_both_sides_and_a_steady_overflow_counter() -> None:
    record = ListenerRecord(
        ready=True, events=(), ticks=((100, True, 3), (200, True, 3), (300, True, 3))
    )
    assert record.window(150, 250) == (True, True, False)
    assert record.window(50, 250) == (False, False, False)
    assert record.window(150, 350) == (False, False, False)
    moved = ListenerRecord(ready=True, events=(), ticks=((100, True, 3), (300, True, 4)))
    assert moved.window(150, 250) == (True, True, True)
    blind = ListenerRecord(ready=True, events=(), ticks=((100, False, None), (300, True, 3)))
    assert blind.window(150, 250) == (False, True, False)


def test_socket_entries_count_as_delivery_by_source_address() -> None:
    log = "\n".join(
        [
            json.dumps({"ev": "ready", "tcp": [1], "udp": [1], "t": 1}),
            json.dumps(
                {"ev": "sock", "peer": "10.0.0.9", "port": 1, "state": "03", "proto": "tcp", "t": 2}
            ),
        ]
    )
    record = parse_listener_log(log)
    assert record.deliveries(nonces=[], peers=["10.0.0.9"], start_ms=0, end_ms=10) == 1


def test_source_address_matches_only_inside_the_probe_window() -> None:
    # A pod that reuses the sender's address later, or another probe's own
    # attempt, must not read as this probe's delivery.
    log = "\n".join(
        json.dumps(line)
        for line in [
            {"ev": "ready", "tcp": [1], "udp": [1], "t": 1},
            {"ev": "sock", "peer": "10.0.0.9", "port": 1, "state": "01", "proto": "tcp", "t": 50},
            {"ev": "tcp", "peer": "10.0.0.9", "port": 1, "nonce": "other", "t": 15},
            {"ev": "tcp", "peer": "10.0.0.7", "port": 1, "nonce": "mine", "t": 90},
        ]
    )
    record = parse_listener_log(log)
    start, end = 10, 20
    assert record.deliveries(nonces=[], peers=["10.0.0.9"], start_ms=start, end_ms=end) == 1
    assert (
        record.deliveries(
            nonces=[], peers=["10.0.0.9"], foreign_nonces={"other"}, start_ms=start, end_ms=end
        )
        == 0
    )
    assert record.deliveries(nonces=[], peers=["10.0.0.9"], start_ms=10, end_ms=60) == 2
    # A probe's own nonce is attributable at any time.
    assert record.deliveries(nonces=["mine"], peers=[], start_ms=start, end_ms=end) == 1


def test_a_sampling_gap_inside_a_tick_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["probe", json.dumps({"tcp": [], "udp": []})])
    namespace: dict[str, Any] = {"__name__": "probe_under_test"}
    exec(LISTENER_PROGRAM, namespace)  # noqa: S102 - the fixed program text under test
    reads = iter([True, False, True, True, True, True, True, True, True, True, True])

    def read() -> tuple[set[Any], int]:
        if next(reads):
            return set(), 0
        raise OSError("gap")

    sampler = namespace["Sampler"](read)
    ticks = []
    for step in range(11):
        ticks += [doc for doc in sampler.poll(step / 10) if doc["ev"] == "tick"]
    assert [doc["sockets"] for doc in ticks] == [True, False, True]


@pytest.mark.parametrize("template", sorted(TEMPLATES))
def test_every_template_is_unprivileged_and_tokenless(template: str) -> None:
    pod = probe_pod(
        template=template,
        name="probe-1",
        namespace="probe-a",
        node="node-a",
        campaign="c1",
        role_labels={"curietech.ai/role": "x"},
        argument={"attempts": []},
    )
    spec = pod["spec"]
    assert spec["automountServiceAccountToken"] is False
    assert (spec["hostNetwork"], spec["hostPID"], spec["hostIPC"]) == (False, False, False)
    assert "volumes" not in spec
    (container,) = spec["containers"]
    assert container["image"] == PROBE_IMAGE and "@sha256:" in PROBE_IMAGE
    assert container["securityContext"] == {
        "privileged": False,
        "allowPrivilegeEscalation": False,
        "readOnlyRootFilesystem": True,
        "capabilities": {"drop": ["ALL"]},
    }
    assert "volumeMounts" not in container and "envFrom" not in container
    assert spec["nodeName"] == "node-a"
    assert pod["metadata"]["labels"]["curietech.ai/managed-cni-probe-template"] == template


@pytest.mark.parametrize(
    "changes",
    [
        {"template": "spoofing"},
        {"node": "Node_A"},
        {"role_labels": {"curietech.ai/managed-cni-probe-template": "twin"}},
    ],
)
def test_template_refuses_what_it_does_not_allow(changes: dict[str, Any]) -> None:
    arguments: dict[str, Any] = {
        "template": "twin",
        "name": "probe-1",
        "namespace": "probe-a",
        "node": "node-a",
        "campaign": "c1",
        "role_labels": {},
        "argument": {},
    }
    arguments.update(changes)
    with pytest.raises(ProbeKitRefused):
        probe_pod(**arguments)


def test_probe_kit_digest_is_stable() -> None:
    assert probe_kit_digest() == probe_kit_digest()
    assert len(probe_kit_digest()) == 64
