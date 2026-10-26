"""Shared activation inputs exercise actual chart rendering and Docker claims."""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

import pytest
import yaml
from aci_protocol import BootEnv, Budget

from .conftest import _FakeBundleStore, _RecordingDocker, _volume_mounts

ROOT = Path(__file__).resolve().parents[4]
VECTORS = json.loads((ROOT / "tests/vectors/attachment-access-activation.json").read_text())


@pytest.mark.parametrize("case", VECTORS, ids=lambda case: case["name"])
def test_attachment_activation_substrates(case: dict) -> None:
    rendered = subprocess.run(
        [
            "helm",
            "template",
            "attachment-test",
            str(ROOT / "charts/curie"),
            "--set",
            f"worker.attachments.enabled={str(case['enabled']).lower()}",
            "--set",
            f"worker.attachments.mode={case['mode']}",
            "--set",
            "agentSandbox.runner.fakeModel=false",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    docs = [doc for doc in yaml.safe_load_all(rendered.stdout) if doc]
    templates = [
        doc
        for doc in docs
        if doc["kind"] == "SandboxTemplate"
        and doc["metadata"]["name"] == "attachment-test-curie-runner"
    ]
    assert len(templates) == 1
    pod = templates[0]["spec"]["podTemplate"]["spec"]
    volumes = {v["name"]: v for v in pod["volumes"]}
    assert ("attachments" in volumes) is case["volume"]
    if case["volume"]:
        assert volumes["attachments"]["emptyDir"]["sizeLimit"] == "512Mi"
    init = {c["name"] for c in pod["initContainers"]}
    assert ("attachments-init" in init) is case["eager_init"]
    runner = next(c for c in pod["containers"] if c["name"] == "runner")
    runner_env = {item["name"]: item.get("value") for item in runner["env"]}
    assert (runner_env.get("CURIE_ATTACHMENT_ACCESS_ENABLED") == "1") is case["lazy"]
    brokers = [
        doc
        for doc in docs
        if doc["kind"] == "Service" and doc["metadata"]["name"].endswith("-attachment-broker")
    ]
    assert bool(brokers) is case["lazy"]
    if brokers:
        assert brokers[0]["spec"]["type"] == "ClusterIP"
        assert brokers[0]["spec"]["ports"][0]["port"] == 8093
        policy = next(
            doc
            for doc in docs
            if doc["kind"] == "NetworkPolicy"
            and doc["metadata"]["name"].endswith("-attachment-broker-ingress")
        )
        peer = policy["spec"]["ingress"][0]["from"][0]["podSelector"]["matchLabels"]
        assert peer["app.kubernetes.io/component"] == "api"
    client = _RecordingDocker(
        image="curie-runner", network="activation", bundle_store=_FakeBundleStore(b"")
    )
    client.create_claim(
        "claim-activation",
        pool="pool",
        env=BootEnv.render_worker(
            plugin_dir="/bundles/current",
            session_id="session",
            budget=Budget(max_output_tokens_per_run=1000, max_usd_per_day=1.0),
            memory_ref="http://api/memory",
            history_ref="http://api/history",
            attachment_access_enabled=True if case["lazy"] else None,
        ),
        deadline=time.monotonic() + 300,
    )
    mounts = _volume_mounts(client.runner_argv)
    # Legacy Docker materializes a mount only when a file-set ref is present.
    assert ("/attachments" in mounts) is case["lazy"]
    if case["lazy"]:
        volume, mode = mounts["/attachments"]
        assert mode == "rw"
        assert client.volume_files[volume] == {}
    client.delete_claim("claim-activation", request_timeout_seconds=10)
    assert not client.volumes
