"""The cron loop reads triggers from the stored bundle, not the render route."""

from __future__ import annotations

import io
import json
import tarfile

from curie_worker.cron_loop import BundleTriggerSource

_LOCK = """\
version: 1
connectors:
  - name: notes
    runtime: local-daemon
"""


def _bundle(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as archive:
        for name, body in files.items():
            data = body.encode()
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return buf.getvalue()


class _Reader:
    def __init__(self, blobs: dict[str, bytes]) -> None:
        self.blobs = blobs

    def get(self, key: str) -> bytes:
        return self.blobs[key]


def _source(blobs: dict[str, bytes]) -> BundleTriggerSource:
    return BundleTriggerSource(
        _Reader(blobs),
        max_uncompressed_bytes=10_000_000,
        max_compression_ratio=1000.0,
        max_members=100,
    )


def test_local_daemon_connector_lock_bundle_yields_its_cron_triggers() -> None:
    cron = {"type": "cron", "name": "nightly", "schedule": "0 3 * * *", "prompt": "Go."}
    manifest = {"name": "demo", "triggers": [cron, "not-a-mapping"]}
    blob = _bundle(
        {
            "demo/.claude-plugin/plugin.json": json.dumps(manifest),
            "demo/connectors.lock.yaml": _LOCK,
        }
    )
    assert _source({"b/1": blob}).triggers("b/1") == [cron]


def test_bundle_without_triggers_yields_nothing() -> None:
    blob = _bundle({"plugin.json": json.dumps({"name": "demo"})})
    assert _source({"b/2": blob}).triggers("b/2") == []
