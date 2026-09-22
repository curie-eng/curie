#!/usr/bin/env python3
"""Validate architecture-atlas manifests, snapshots, references, and geometry."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

MAP_WIDTH = 1300
MAP_HEIGHT = 720
NODE_SIZES = {
    "surface": (104, 40),
    "service": (108, 40),
    "broker": (106, 40),
    "capability": (112, 40),
    "runtime": (104, 40),
    "external": (96, 40),
    "observability": (104, 40),
    "data": (100, 40),
    "substrate": (96, 38),
    "bot-capability": (116, 40),
    "deployed-agent": (116, 40),
}


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[4]
    parser = argparse.ArgumentParser(description="Validate the interactive architecture atlas")
    parser.add_argument(
        "--atlas-dir",
        type=Path,
        default=repo_root / "docs" / "architecture-atlas",
    )
    return parser.parse_args()


def unique_ids(items: list[dict[str, object]], collection: str) -> set[str]:
    ids = [str(item["id"]) for item in items]
    duplicates = sorted({item_id for item_id in ids if ids.count(item_id) > 1})
    if duplicates:
        raise ValueError(f"{collection} contains duplicate ids: {', '.join(duplicates)}")
    return set(ids)


def validate_snapshot(snapshot: dict[str, object], manifest_version: dict[str, object]) -> None:
    required = ("nodes", "seams", "routes", "flows", "adrs", "investments", "zones")
    for key in required:
        if not isinstance(snapshot.get(key), list):
            raise ValueError(f"{manifest_version['id']}: missing array {key}")

    repository = snapshot.get("repository")
    if not isinstance(repository, dict) or repository.get("commit") != manifest_version["commit"]:
        raise ValueError(f"{manifest_version['id']}: snapshot and manifest commits differ")

    nodes = snapshot["nodes"]
    seams = snapshot["seams"]
    routes = snapshot["routes"]
    flows = snapshot["flows"]
    node_ids = unique_ids(nodes, "nodes")
    seam_ids = unique_ids(seams, "seams")
    route_ids = unique_ids(routes, "routes")
    unique_ids(flows, "flows")
    unique_ids(snapshot["adrs"], "adrs")

    groups = snapshot.get("architectureGroups", [])
    group_ids = unique_ids(groups, "architectureGroups")
    member_ids: set[str] = set()
    for group in groups:
        for member_id in group["memberIds"]:
            if member_id not in node_ids:
                raise ValueError(f"group {group['id']} references unknown node {member_id}")
            if member_id in member_ids:
                raise ValueError(f"node {member_id} belongs to more than one architecture group")
            member_ids.add(member_id)

    for route in routes:
        if route["from"] not in node_ids or route["to"] not in node_ids:
            raise ValueError(f"route {route['id']} references an unknown node")
        if route["seamId"] not in seam_ids:
            raise ValueError(f"route {route['id']} references unknown seam {route['seamId']}")
    for flow in flows:
        for step in flow["steps"]:
            if step["routeId"] not in route_ids:
                raise ValueError(f"flow {flow['id']} references unknown route {step['routeId']}")

    zones = {zone["id"]: zone for zone in snapshot["zones"]}
    map_nodes = list(groups) + [node for node in nodes if node["id"] not in member_ids]
    for node in map_nodes:
        zone = zones.get(node["zone"])
        if zone is None:
            raise ValueError(f"node {node['id']} references unknown zone {node['zone']}")
        width, height = NODE_SIZES.get(node["kind"], (104, 40))
        center_x = node["x"] * MAP_WIDTH
        center_y = node["y"] * MAP_HEIGHT
        left = zone["x"] * MAP_WIDTH
        right = (zone["x"] + zone["w"]) * MAP_WIDTH
        top = zone["y"] * MAP_HEIGHT
        bottom = (zone["y"] + zone["h"]) * MAP_HEIGHT
        if (
            center_x - width / 2 < left
            or center_x + width / 2 > right
            or center_y - height / 2 < top
            or center_y + height / 2 > bottom
        ):
            raise ValueError(f"node {node['id']} falls outside its {node['zone']} zone")

    visible_ids = node_ids | group_ids
    if not visible_ids:
        raise ValueError(f"{manifest_version['id']}: no architecture nodes")


def main() -> int:
    atlas_dir = parse_args().atlas_dir.resolve()
    manifest_path = atlas_dir / "versions.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    version_ids = unique_ids(manifest["versions"], "versions")
    if manifest["defaultVersion"] not in version_ids:
        raise ValueError("defaultVersion does not name a registered version")

    for version in manifest["versions"]:
        snapshot_path = (atlas_dir / version["file"]).resolve()
        if atlas_dir not in snapshot_path.parents:
            raise ValueError(f"snapshot path escapes atlas directory: {version['file']}")
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        validate_snapshot(snapshot, version)
        print(
            f"validated {version['id']}: "
            f"{len(snapshot['nodes'])} nodes, {len(snapshot['routes'])} routes, "
            f"{len(snapshot['flows'])} flows"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
