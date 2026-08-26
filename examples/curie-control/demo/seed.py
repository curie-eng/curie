#!/usr/bin/env python3
"""Seed a disposable database and boot an API for the control-agent demo.

Prints the agent id and the rollback-target version id the chat script needs, so
the recording is reproducible from a clean machine and never depends on whatever
happens to be in a shared dev database.
"""

from __future__ import annotations

import argparse
import json
import urllib.request

FLEET = [
    ("sre-bot", "C0DEMO001", [("v1", None), ("v2", None), ("v3", None)], "v3"),
    ("support-bot", "C0DEMO002", [("v7", None)], "v7"),
    ("release-notes", "C0DEMO003", [("v2", None)], "v2"),
    ("oncall-digest", "C0DEMO004", [], None),
]


def call(api: str, key: str, method: str, path: str, body: object = None) -> object:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        f"{api}{path}",
        data=data,
        method=method,
        headers={"X-API-Key": key, **({"Content-Type": "application/json"} if data else {})},
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        raw = response.read().decode()
        return json.loads(raw) if raw else None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", default="http://localhost:28099")
    parser.add_argument("--key", default="demo-platform-key")
    args = parser.parse_args()

    out: dict[str, str] = {}
    for name, channel, versions, live in FLEET:
        agent = call(
            args.api,
            args.key,
            "POST",
            "/agents",
            {"name": name, "channel": {"kind": "slack", "address": channel}},
        )
        assert isinstance(agent, dict)
        made: dict[str, str] = {}
        for label, _ in versions:
            version = call(
                args.api,
                args.key,
                "POST",
                f"/agents/{agent['id']}/versions",
                {"version_label": label, "created_by": "demo"},
            )
            assert isinstance(version, dict)
            made[label] = str(version["id"])
        # Only the LIVE label gets a deployment. Deploying each in turn would be
        # more realistic, but every row lands in the same millisecond here and
        # "most recent active deployment" is decided by that timestamp -- the
        # seed would pick a live version at random. The older versions existing
        # undeployed is exactly the state a rollback chooses from.
        if live:
            call(
                args.api,
                args.key,
                "POST",
                "/deployments",
                {
                    "agent_id": agent["id"],
                    "version_id": made[live],
                    "environment": "prod",
                },
            )
        if name == "sre-bot":
            out["agent_id"] = str(agent["id"])
            out["old_version"] = made["v2"]
    print(json.dumps(out))


if __name__ == "__main__":
    main()
