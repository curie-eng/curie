"""Budget and failure contracts for streamed kind imports."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import kind_images
import pytest


@pytest.mark.parametrize("sizes", [[], [0], [-1], [True], ["123"]])
def test_missing_or_invalid_image_sizes_refuse(sizes: list[object]) -> None:
    def command(*_argv: str, **_kwargs: object) -> str:
        return json.dumps([{"Size": size} for size in sizes])

    with pytest.raises(ValueError, match="sizes"):
        kind_images.import_budget(command, ["acme-api:candidate"])


def test_budget_counts_snapshots_largest_import_and_runtime_reserve() -> None:
    def command(*_argv: str, **_kwargs: object) -> str:
        return json.dumps([{"Size": 10}, {"Size": 20}])

    assert kind_images.import_budget(command, ["acme-api:candidate", "acme-worker:candidate"]) == (
        30 + 20 + kind_images.RUNTIME_RESERVE_BYTES
    )


@pytest.mark.parametrize("save_status,import_status", [(0, 0), (1, 0), (0, 1)])
def test_stream_delivers_image_bytes_and_propagates_both_failures(
    save_status: int,
    import_status: int,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    docker = tmp_path / "docker"
    docker.write_text(
        '#!/bin/sh\nif [ "$1 $2" = "image save" ]; then\n'
        '  printf "candidate-image-bytes"\n  exit "$SAVE_STATUS"\n'
        'elif [ "$1" = "exec" ]; then\n'
        '  cat > "$IMPORT_CAPTURE"\n  exit "$IMPORT_STATUS"\nelse\n  exit 2\nfi\n'
    )
    docker.chmod(0o755)
    capture = tmp_path / "imported"
    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("SAVE_STATUS", str(save_status))
    monkeypatch.setenv("IMPORT_STATUS", str(import_status))
    monkeypatch.setenv("IMPORT_CAPTURE", str(capture))

    def command(*argv: str, timeout: float) -> str:
        # Shapes observed from Docker and kind v0.24.0's v1.31.0 node.
        if argv[0] == "kind":
            return "acme-control-plane\n"
        if argv[:2] == ("docker", "version"):
            return "linux/amd64\n"
        if argv[0] == "docker":
            return '[plugins."io.containerd.grpc.v1.cri".containerd]\nsnapshotter="overlayfs"\n'
        return subprocess.run(
            argv,
            capture_output=True,
            text=True,
            check=True,
            timeout=timeout,
        ).stdout

    if save_status or import_status:
        with pytest.raises(subprocess.CalledProcessError):
            kind_images.load_images(command, "acme", ["acme-api:candidate"])
    else:
        kind_images.load_images(command, "acme", ["acme-api:candidate"])
    assert capture.read_bytes() == b"candidate-image-bytes"


@pytest.mark.parametrize("nodes,config", [("", ""), ("acme-control-plane", "")])
def test_missing_nodes_or_snapshotter_refuse_before_image_save(nodes: str, config: str) -> None:
    def command(*argv: str, timeout: float) -> str:
        if argv[0] == "kind":
            return nodes
        if argv[:2] == ("docker", "version"):
            return "linux/amd64"
        if argv[0] == "docker":
            return config
        pytest.fail("image save must not start without a node and CRI snapshotter")

    with pytest.raises(ValueError):
        kind_images.load_images(command, "acme", ["acme-api:candidate"])
