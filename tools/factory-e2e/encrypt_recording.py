"""Encrypt anonymous fixture recordings for private review, never export plaintext."""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
from pathlib import Path


def encrypt(transcript: Path, recipient: Path, output: Path, *, credential: str | None) -> None:
    raw = transcript.read_bytes()
    recording = json.loads(raw)
    if recording.get("version") != 1 or not recording.get("exchanges"):
        raise ValueError("recording has no actual exchanges")
    texts = [raw]
    for exchange in recording["exchanges"]:
        if set(exchange) - {"match", "content_type", "raw_body_b64", "message"}:
            raise ValueError("recording includes unexpected fields")
        if exchange.get("raw_body_b64"):
            texts.append(base64.b64decode(exchange["raw_body_b64"], validate=True))
    if credential and any(credential.encode() in value for value in texts):
        raise ValueError("recording contains a provider credential; export refused")
    if output.exists():
        raise ValueError("encrypted export refuses to overwrite an existing file")
    subprocess.run(
        [
            "openssl",
            "cms",
            "-encrypt",
            "-binary",
            "-aes256",
            "-in",
            str(transcript),
            "-out",
            str(output),
            "-outform",
            "DER",
            str(recipient),
        ],
        check=True,
        capture_output=True,
        timeout=30,
    )
    output.chmod(0o600)


if __name__ == "__main__":
    encrypt(
        *(Path(arg) for arg in sys.argv[1:4]),
        credential=os.environ.get("CURIE_FACTORY_MODEL_API_KEY"),
    )
