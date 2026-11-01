"""Bound candidate image imports without materializing Docker save archives."""

from __future__ import annotations

import json
import tomllib
from collections.abc import Callable, Sequence
from typing import Any

# The admitted fixture also runs Langfuse, ClickHouse and the OTel collector.
# Its measured node and registry use 9 GB including candidates; reserve 10 GiB
# beyond candidate snapshots and the largest in-flight import.
RUNTIME_RESERVE_BYTES = 10 * 1024**3


def import_budget(command: Callable[..., str], images: Sequence[str]) -> int:
    records = json.loads(command("docker", "image", "inspect", *images, timeout=30))
    sizes: list[int] = [record["Size"] for record in records]
    if (
        len(sizes) != len(images)
        or not sizes
        or any(type(size) is not int or size <= 0 for size in sizes)
    ):
        raise ValueError("candidate image sizes are missing or invalid")
    return sum(sizes) + max(sizes) + RUNTIME_RESERVE_BYTES


def load_images(command: Callable[..., str], cluster: str, images: Sequence[str]) -> None:
    platform = command(
        "docker", "version", "--format", "{{.Server.Os}}/{{.Server.Arch}}", timeout=30
    ).strip()
    nodes = command("kind", "get", "nodes", "--name", cluster, timeout=30).split()
    if not nodes:
        raise ValueError("kind cluster has no nodes for candidate image imports")
    for node in nodes:
        config = tomllib.loads(
            command("docker", "exec", node, "containerd", "config", "dump", timeout=30)
        )
        cri: dict[str, Any] = config.get("plugins", {}).get("io.containerd.grpc.v1.cri", {})
        snapshotter = cri.get("containerd", {}).get("snapshotter")
        if not isinstance(snapshotter, str) or not snapshotter:
            raise ValueError("kind node reports no CRI snapshotter")
        for image in images:
            command(
                "bash",
                "-o",
                "pipefail",
                "-c",
                'docker image save --platform "$1" "$4" | '
                'docker exec -i "$2" ctr --namespace=k8s.io images import '
                '--all-platforms --digests --snapshotter="$3" --discard-unpacked-layers -',
                "kind-image-import",
                platform,
                node,
                snapshotter,
                image,
                timeout=600,
            )
