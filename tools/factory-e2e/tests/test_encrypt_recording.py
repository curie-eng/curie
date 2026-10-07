"""Only encrypted fixture recordings can leave the recording host."""

from __future__ import annotations

import importlib.util
import json
import subprocess
from pathlib import Path

import pytest


def _module():
    spec = importlib.util.spec_from_file_location(
        "encrypt_recording", Path(__file__).resolve().parents[1] / "encrypt_recording.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_recipient_encryption_round_trip_and_plaintext_key_rejection(tmp_path: Path) -> None:
    module = _module()
    key, cert = tmp_path / "key.pem", tmp_path / "cert.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=example-fixture",
            "-keyout",
            str(key),
            "-out",
            str(cert),
        ],
        check=True,
        capture_output=True,
    )
    transcript, encrypted, restored = (
        tmp_path / name for name in ("record.json", "encrypted.bin", "restored.json")
    )
    transcript.write_text(
        json.dumps({"version": 1, "exchanges": [{"match": {"body": "example-provider-key"}}]})
    )
    with pytest.raises(ValueError, match="credential"):
        module.encrypt(transcript, cert, encrypted, credential="example-provider-key")
    assert not encrypted.exists()
    transcript.write_text(
        json.dumps({"version": 1, "exchanges": [{"match": {"body": "anonymous-fixture-content"}}]})
    )
    module.encrypt(transcript, cert, encrypted, credential="example-provider-key")
    assert b"anonymous-fixture-content" not in encrypted.read_bytes()
    subprocess.run(
        [
            "openssl",
            "cms",
            "-decrypt",
            "-binary",
            "-inform",
            "DER",
            "-in",
            str(encrypted),
            "-recip",
            str(cert),
            "-inkey",
            str(key),
            "-out",
            str(restored),
        ],
        check=True,
        capture_output=True,
    )
    assert restored.read_bytes() == transcript.read_bytes()
