"""Actual fresh probe processes and their closed v2 log boundary.

Loopback process tests do not measure CNI enforcement or qualify an installation.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest
from curie_worker.managed_cni_probe_kit import ProbeKitRefused
from curie_worker.managed_cni_probe_programs import probe_pod

CHALLENGE = "a" * 32


def fresh() -> Any:
    name = "curie_worker.managed_cni_fresh_programs"
    assert importlib.util.find_spec(name) is not None, (
        "fresh probe executable boundary is not implemented"
    )
    return importlib.import_module(name)


def lines(rows: list[dict[str, Any]]) -> str:
    return "\n".join(json.dumps({"v": 2, "seq": n, **row}) for n, row in enumerate(rows))


def sender_rows() -> list[dict[str, Any]]:
    return [
        {
            "ev": "ready",
            "mode": "deny",
            "source": "127.0.0.1",
            "setup": None,
            "targets": [{"target": "twin", "address": "127.0.0.1", "port": 18080, "echo": True}],
            "excluded_ports": [41000],
        },
        {"ev": "challenge", "challenge": CHALLENGE},
        *[
            {
                "ev": "attempt",
                "target": "twin",
                "source": "127.0.0.1",
                "source_port": 41001 + i,
                "destination": "127.0.0.1",
                "port": 18080,
                "nonce": CHALLENGE + f"00{i:02x}",
                "outcome": "timeout",
                "echo": False,
            }
            for i in range(5)
        ],
        {"ev": "done", "ok": True},
    ]


def test_sender_parser_preserves_complete_ordered_original_tuples() -> None:
    record = fresh().parse_fresh_sender_log(lines(sender_rows()))
    assert record.complete and record.challenge == CHALLENGE
    assert tuple(a.source_port for a in record.attempts) == (41001, 41002, 41003, 41004, 41005)
    assert all(a.outcome == "timeout" for a in record.attempts)
    with pytest.raises((AttributeError, TypeError)):
        record.attempts[0].source_port = 41000  # type: ignore[misc]


@pytest.mark.parametrize(
    "change",
    [
        "unknown",
        "boolean_port",
        "reused_port",
        "excluded",
        "wrong_target",
        "wrong_challenge",
        "missing",
        "extra",
    ],
)
def test_sender_parser_refuses_incomplete_or_substituted_attempts(change: str) -> None:
    rows = sender_rows()
    if change == "unknown":
        rows[2]["extra"] = True
    elif change == "boolean_port":
        rows[2]["source_port"] = True
    elif change == "reused_port":
        rows[3]["source_port"] = 41001
    elif change == "excluded":
        rows[2]["source_port"] = 41000
    elif change == "wrong_target":
        rows[2]["destination"] = "127.0.0.2"
    elif change == "wrong_challenge":
        rows[2]["nonce"] = "b" * 36
    elif change == "missing":
        rows.pop(2)
    elif change == "extra":
        rows.insert(-1, dict(rows[2]))
    with pytest.raises(ProbeKitRefused):
        fresh().parse_fresh_sender_log(lines(rows))


def test_sender_parser_requires_completion_and_allows_only_explicit_prefix_reads() -> None:
    partial = lines(sender_rows()[:1])
    with pytest.raises(ProbeKitRefused):
        fresh().parse_fresh_sender_log(partial)
    assert not fresh().parse_fresh_sender_log(partial, allow_partial=True).complete
    duplicate = lines(sender_rows()).replace('"v": 2', '"v":2,"v":2', 1)
    with pytest.raises(ProbeKitRefused):
        fresh().parse_fresh_sender_log(duplicate)
    with pytest.raises(ProbeKitRefused):
        fresh().parse_fresh_sender_log(lines(sender_rows()) + "\n{")


def test_listener_distinguishes_old_same_address_socket_from_fresh_tuple() -> None:
    attempt = fresh().parse_fresh_sender_log(lines(sender_rows())).attempts[0]
    base = [
        {"ev": "ready", "port": 18080},
        {"ev": "tick", "elapsed_ms": 0, "sampled": True, "overflow": 0},
        {
            "ev": "socket",
            "source": "127.0.0.1",
            "source_port": 41000,
            "destination": "127.0.0.1",
            "port": 18080,
            "state": "01",
        },
        {"ev": "tick", "elapsed_ms": 500, "sampled": True, "overflow": 0},
    ]
    assert fresh().parse_fresh_listener_log(lines(base)).deliveries((attempt,)) == 0
    base.insert(
        3,
        {
            "ev": "socket",
            "source": "127.0.0.1",
            "source_port": 41001,
            "destination": "127.0.0.1",
            "port": 18080,
            "state": "03",
        },
    )
    assert fresh().parse_fresh_listener_log(lines(base)).deliveries((attempt,)) == 1


def template_argument(template: str) -> dict[str, Any]:
    path = Path(__file__).resolve().parents[3] / "tests/vectors/managed-cni-fresh-arguments.json"
    return json.loads(path.read_text())[template]


@pytest.mark.parametrize(
    "template",
    [
        "fresh_twin",
        "fresh_probe_listener",
        "fresh_sender",
        "fresh_setup",
        "fresh_control",
        "fresh_allowed_control",
        "fresh_trigger",
    ],
)
def test_shared_arguments_render_a_tokenless_capability_free_probe(template: str) -> None:
    pod = probe_pod(
        template=template,
        name="fresh-probe",
        namespace="probe-a",
        node="worker-a",
        campaign="trial-a",
        role_labels={},
        argument=template_argument(template),
    )
    spec = pod["spec"]
    assert spec["automountServiceAccountToken"] is False
    assert spec["containers"][0]["securityContext"]["capabilities"] == {"drop": ["ALL"]}
    assert spec["containers"][0]["securityContext"]["readOnlyRootFilesystem"] is True
    assert spec["securityContext"]["runAsNonRoot"] is True


def command(template: str, cfg: dict[str, Any]) -> list[str]:
    pod = probe_pod(
        template=template,
        name="fresh-probe",
        namespace="probe-a",
        node="worker-a",
        campaign="trial-a",
        role_labels={},
        argument=cfg,
    )
    args = pod["spec"]["containers"][0]["command"]
    return [sys.executable, *args[1:]]


@pytest.mark.parametrize(
    "change, expected",
    [
        ("complete", True),
        ("gap", False),
        ("sample_failure", False),
        ("overflow", False),
        ("no_final_tick", False),
    ],
)
def test_listener_requires_continuous_sampling_through_the_fresh_control(
    change: str,
    expected: bool,
) -> None:
    control = fresh().parse_fresh_sender_log(lines(sender_rows())).attempts[0]
    rows = [
        {"ev": "ready", "port": 18080},
        {"ev": "tick", "elapsed_ms": 0, "sampled": True, "overflow": 0},
        {
            "ev": "data",
            "source": "127.0.0.1",
            "source_port": 41001,
            "destination": "127.0.0.1",
            "port": 18080,
            "nonce": CHALLENGE + "0000",
        },
        {"ev": "tick", "elapsed_ms": 500, "sampled": True, "overflow": 0},
    ]
    if change == "gap":
        rows[-1]["elapsed_ms"] = 2000
    elif change == "sample_failure":
        rows[-1]["sampled"] = False
    elif change == "overflow":
        rows[-1]["overflow"] = 1
    elif change == "no_final_tick":
        rows.pop()
    assert fresh().parse_fresh_listener_log(lines(rows)).covers(control) is expected


@pytest.mark.parametrize("change", ["twin_no_echo", "mixed_family", "foreign_setup"])
def test_sender_parser_refuses_impossible_producer_identity_joins(change: str) -> None:
    rows = sender_rows()
    if change == "twin_no_echo":
        rows[0]["targets"][0]["echo"] = False
    elif change == "mixed_family":
        rows[0]["targets"][0]["address"] = "::1"
        for row in rows[2:-1]:
            row["destination"] = "::1"
    else:
        setup = dict(rows[2])
        setup.update(
            source_port=41000, port=18081, nonce=CHALLENGE + "ffff", outcome="connected", echo=True
        )
        rows[0]["setup"] = setup
    with pytest.raises(ProbeKitRefused):
        fresh().parse_fresh_sender_log(lines(rows))


def test_malformed_unicode_log_uses_the_closed_refusal_boundary() -> None:
    with pytest.raises(ProbeKitRefused):
        fresh().parse_fresh_sender_log("\ud800")


@pytest.mark.parametrize("response", [b"unrelated\n", b"", None])
def test_actual_control_does_not_claim_a_round_trip_from_one_way_or_wrong_reply(
    tmp_path: Path,
    response: bytes | None,
) -> None:
    import threading

    receiver = socket.socket()
    receiver.bind(("127.0.0.1", 0))
    receiver.listen(1)
    receiver.settimeout(4)
    port = receiver.getsockname()[1]
    failures: list[Exception] = []

    def serve() -> None:
        try:
            conn, _ = receiver.accept()
            with conn:
                conn.settimeout(3)
                conn.recv(128)
                if response is None:
                    time.sleep(2.2)
                elif response:
                    conn.sendall(response)
        except OSError as exc:
            failures.append(exc)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        cfg = template_argument("fresh_control")
        cfg["targets"][0]["port"] = port
        output = subprocess.run(
            command("fresh_control", cfg), capture_output=True, text=True, timeout=8, check=True
        )
        result = fresh().parse_fresh_sender_log(output.stdout)
        assert result.complete and len(result.attempts) == 1
        assert result.attempts[0].outcome == "connected" and not result.attempts[0].echo
    finally:
        receiver.close()
        thread.join(timeout=5)
        assert not thread.is_alive() and not failures


def test_all_distinct_real_endpoints_are_retained_in_one_bounded_sender() -> None:
    argument = template_argument("fresh_sender")
    argument["targets"] = [
        {"target": "real", "address": f"127.0.0.{i}", "port": 18080, "echo": False}
        for i in range(2, 5)
    ] + [{"target": "twin", "address": "127.0.0.1", "port": 18080, "echo": True}]
    pod = probe_pod(
        template="fresh_sender",
        name="fresh-many",
        namespace="probe-a",
        node="node-a",
        campaign="trial-a",
        role_labels={},
        argument=argument,
    )
    assert json.loads(pod["spec"]["containers"][0]["command"][4])["targets"] == argument["targets"]
    rows = sender_rows()
    rows[0]["targets"] = argument["targets"]
    attempts = []
    for index, target in enumerate(argument["targets"]):
        for ordinal in range(5):
            attempts.append(
                {
                    **rows[2],
                    "target": target["target"],
                    "destination": target["address"],
                    "port": target["port"],
                    "source_port": 42010 + len(attempts),
                    "nonce": CHALLENGE + f"{index:02x}{ordinal:02x}",
                }
            )
    record = fresh().parse_fresh_sender_log(lines([rows[0], rows[1], *attempts, rows[-1]]))
    assert record.complete and len(record.attempts) == 20
    for bad in ([argument["targets"][-1]] * 2, argument["targets"] * 3):
        with pytest.raises(ProbeKitRefused):
            probe_pod(
                template="fresh_sender",
                name="fresh-many",
                namespace="probe-a",
                node="node-a",
                campaign="trial-a",
                role_labels={},
                argument={**argument, "targets": bad},
            )


def test_template_dispatch_is_part_of_the_actual_probe_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import curie_worker.managed_cni_probe_programs as programs

    before = programs.probe_kit_digest()
    monkeypatch.setitem(programs.TEMPLATES, "fresh_sender", ("fresh_listener", "probe"))
    assert programs.probe_kit_digest() != before
