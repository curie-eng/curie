#!/usr/bin/env python3
"""One-shot, offline migration of protected-worker process.json v1 to v2."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

from curie_protected_hooks.artifact_measurement import _open_root
from curie_worker.protected_entry import load_process_settings
from curie_worker.protected_worker_bootstrap import _pairs, _read, _reject, _require


def migrate(runtime_dir: str, gateway_config: str) -> None:
    runtime_dir = os.path.abspath(runtime_dir)
    root = _open_root(runtime_dir)
    try:
        source = json.loads(
            _read(root, "process.json"),
            object_pairs_hook=_pairs,
            parse_float=_reject,
            parse_constant=_reject,
        )
        _require(
            type(source) is dict
            and source.keys()
            == {
                "schema_version",
                "database_url",
                "db_schema",
                "api_base_url",
                "internal_worker_token",
                "adapter_credentials",
                "slack",
                "launch_tls",
            }
        )
        _require(type(source["schema_version"]) is int and source["schema_version"] == 1)
        slack = source["slack"]
        _require(
            type(slack) is dict
            and slack.keys()
            == {
                "token",
                "base_url",
                "trusted_origins",
                "identity_tokens",
            }
        )
        _require(type(slack["token"]) is str and type(slack["base_url"]) is str)
        _require(
            type(slack["trusted_origins"]) is list
            and all(type(value) is str for value in slack["trusted_origins"])
        )
        tokens = slack["identity_tokens"]
        _require(
            type(tokens) is dict
            and all(
                type(key) is str and bool(key) and type(value) is str and bool(value)
                for key, value in tokens.items()
            )
        )
        gateway_path = Path(os.path.abspath(gateway_config))
        gateway_root = _open_root(str(gateway_path.parent))
        try:
            gateway = json.loads(
                _read(gateway_root, gateway_path.name),
                object_pairs_hook=_pairs,
                parse_float=_reject,
                parse_constant=_reject,
            )
        finally:
            os.close(gateway_root)
        _require(type(gateway) is dict and type(gateway.get("identities")) is list)
        expected = set(tokens) | ({"default"} if slack["token"] else set())
        _require(expected.issubset(set(gateway["identities"])))
        migrated = {key: value for key, value in source.items() if key != "slack"}
        migrated.update(schema_version=2, channel_gateway=gateway)
        with tempfile.TemporaryDirectory(
            prefix=".process-migration-", dir=runtime_dir
        ) as temporary:
            candidate = Path(temporary) / "process.json"
            descriptor = os.open(candidate, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w") as stream:
                json.dump(migrated, stream, indent=2)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            load_process_settings(temporary)
            os.replace(candidate, "process.json", dst_dir_fd=root)
            os.fsync(root)
    finally:
        os.close(root)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", required=True)
    parser.add_argument(
        "--gateway-config", required=True, help="private v2 channel_gateway JSON object"
    )
    args = parser.parse_args()
    try:
        migrate(args.runtime_dir, args.gateway_config)
    except Exception:  # noqa: BLE001  Never print configuration values or exception graphs.
        print(
            "Migration refused. Fix: supply a valid process.json v1 and a TLS gateway "
            "configuration covering every existing Slack identity; run from the source "
            "checkout with uv run python3 scripts/migrate-protected-process.py.",
            file=sys.stderr,
        )
        return 1
    print("Migrated process.json to v2; raw Slack credentials removed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
