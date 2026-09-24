"""Alertmanager signer injects a legal partition and HMAC-signs the body."""

from __future__ import annotations

import hashlib
import hmac
import importlib.util
import json
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "alert_signer",
    Path(__file__).with_name("server.py"),
)
assert _SPEC is not None and _SPEC.loader is not None
signer = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(signer)


def test_prepare_injects_partition_and_stable_delivery_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CURIE_HOOK_SECRET", "hook-secret")
    payload = {
        "groupKey": '{}:{alertname="Example"}',
        "status": "firing",
        "alerts": [
            {"fingerprint": "fp-b", "labels": {"curie_workload": "api"}},
            {"fingerprint": "fp-a", "labels": {"curie_workload": "api"}},
        ],
    }
    body, signature, delivery = signer.prepare(payload)
    forwarded = json.loads(body)
    assert (
        forwarded["curie_partition"]
        == hashlib.sha256(payload["groupKey"].encode()).hexdigest()[:32]
    )
    assert len(forwarded["curie_partition"]) == 32
    expected = "sha256=" + hmac.new(b"hook-secret", body, hashlib.sha256).hexdigest()
    assert signature == expected
    again = signer.prepare(payload)
    assert again[2] == delivery
    payload["alerts"][0]["startsAt"] = "2026-09-16T00:00:00Z"
    later = signer.prepare(payload)
    assert later[2] != delivery


def test_unsigned_curie_body_is_not_what_the_signer_forwards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CURIE_HOOK_SECRET", "hook-secret")
    original = {"groupKey": "g", "status": "firing", "alerts": []}
    body, _signature, _delivery = signer.prepare(original)
    assert json.loads(body)["curie_partition"]
    assert b"curie_partition" in body
