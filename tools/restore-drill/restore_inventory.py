"""Declared backup inventory for the #2427 synthetic restore drill.

This is not a disaster-recovery product and does not invent Valkey replay,
RPO, or RTO. It names the existing supported export/restore inputs, refuses an
incomplete or inconsistent backup, and keeps operator keys out of the backup.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA = "curie-restore-inventory-v1"
REQUIRED_DATA_COMPONENTS = (
    "postgres",
    "bundles",
    "mail-adapter-state",
    "valkey",
)
COMPONENT_PATHS = {
    "postgres": "postgres/curie.dump",
    "bundles": "bundles",
    "mail-adapter-state": "mail-adapter/state.sqlite3",
    "valkey": "valkey/dump.rdb",
}
SEPARATELY_SUPPLIED = (
    "POSTGRES_PASSWORD",
    "VALKEY_PASSWORD",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "API_KEY",
)
FORBIDDEN_BACKUP_DIRS = ("keys-and-config", "secrets")
VALKEY_REPLAY_CONTRACT = "missing"

_RECOVERY = {
    "postgres": (
        "postgres dump is missing or empty",
        "Restore postgres/curie.dump taken with this backup, or re-create the "
        "agent/version/binding records from a known-good source. Do not serve "
        "an API against an empty database while bundles still exist.",
    ),
    "bundles": (
        "immutable bundle objects are missing or their digest does not match "
        "the manifest",
        "Restore the object-store backup taken with this postgres dump "
        "(aws s3 sync of the bundle bucket), or re-publish the exact bundle "
        "each active deployment names. Do not serve deployments whose bundle "
        "bytes are absent.",
    ),
    "mail-adapter-state": (
        "mail adapter SQLite state is missing or empty",
        "Restore mail-adapter/state.sqlite3 (stop the writer, copy the "
        "checkpointed file, open the copy before the writer starts). Pending "
        "work must resume or stay refused as known; do not start on a fresh "
        "claim and silently drop deliveries.",
    ),
    "valkey": (
        "Valkey dump is missing or empty",
        "Restore valkey/dump.rdb as a required backup input. Curie does not "
        "define stream/PEL replay: restored keys match the dump, and in-flight "
        "work is not claimed to resume beyond Valkey's own RDB reload.",
    ),
}


class RestoreRefused(Exception):
    """A backup cannot be applied without serving inconsistent state."""

    def __init__(self, error: str, fix: str) -> None:
        super().__init__(error)
        self.error = error
        self.fix = fix

    def as_payload(self) -> dict[str, str]:
        return {"error": self.error, "fix": self.fix}


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def hash_path(path: Path) -> str:
    if path.is_file():
        return _sha256_bytes(path.read_bytes())
    if not path.is_dir():
        raise RestoreRefused(
            f"required path {path.name} is not a file or directory",
            "Recreate the backup with the declared component layout.",
        )
    digest = hashlib.sha256()
    files = sorted(child for child in path.rglob("*") if child.is_file())
    if not files:
        raise RestoreRefused(
            _RECOVERY["bundles"][0] if path.name == "bundles" else f"{path.name} is empty",
            _RECOVERY["bundles"][1]
            if path.name == "bundles"
            else "Replace the empty component with the bytes taken at backup time.",
        )
    for child in files:
        relative = child.relative_to(path).as_posix().encode()
        digest.update(relative)
        digest.update(b"\0")
        digest.update(child.read_bytes())
    return digest.hexdigest()


def _component_path(backup_dir: Path, component: str) -> Path:
    return backup_dir / COMPONENT_PATHS[component]


def _refuse(component: str, detail: str | None = None) -> None:
    error, fix = _RECOVERY[component]
    if detail:
        error = f"{error}: {detail}"
    raise RestoreRefused(error, fix)


def write_manifest(
    backup_dir: Path, *, candidate: str, created_at: str | None = None
) -> dict[str, Any]:
    backup_dir.mkdir(parents=True, exist_ok=True)
    created = created_at or datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    components: dict[str, Any] = {}
    for component in REQUIRED_DATA_COMPONENTS:
        path = _component_path(backup_dir, component)
        if not path.exists():
            _refuse(component, "file is absent")
        digest = hash_path(path)
        components[component] = {
            "path": COMPONENT_PATHS[component],
            "sha256": digest,
            "required": True,
        }
    manifest = {
        "schema": SCHEMA,
        "issue": "2427",
        "created_at": created,
        "candidate": candidate,
        "components": components,
        "separately_supplied": list(SEPARATELY_SUPPLIED),
        "valkey_replay_contract": VALKEY_REPLAY_CONTRACT,
        "rpo_rto_claimed": False,
        "recurring_production_backup_established": False,
    }
    (backup_dir / "MANIFEST.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def _declared_data_files(backup_dir: Path) -> set[Path]:
    allowed: set[Path] = set()
    for relative in COMPONENT_PATHS.values():
        path = backup_dir / relative
        allowed.add(path)
        if path.is_dir():
            allowed.update(child for child in path.rglob("*") if child.is_file())
    return allowed


def _scan_for_secret_values(backup_dir: Path, supplied: Mapping[str, str]) -> None:
    values = [value for value in supplied.values() if len(value) >= 8]
    if not values:
        return
    data_files = _declared_data_files(backup_dir)
    for path in backup_dir.rglob("*"):
        if not path.is_file() or path in data_files:
            continue
        try:
            text = path.read_text(errors="ignore")
        except OSError:
            continue
        for value in values:
            if value and value in text:
                raise RestoreRefused(
                    "backup contains separately supplied secret material",
                    "Remove operator keys and config from the backup. Supply "
                    "POSTGRES_PASSWORD, VALKEY_PASSWORD, S3_ACCESS_KEY, "
                    "S3_SECRET_KEY, and API_KEY at restore time; never copy "
                    "them into the backup directory.",
                )


def check_backup(backup_dir: Path, supplied_config: Mapping[str, str]) -> dict[str, Any]:
    manifest_path = backup_dir / "MANIFEST.json"
    if not manifest_path.is_file():
        raise RestoreRefused(
            "MANIFEST.json is missing",
            "Write the inventory manifest from the backup components before restore.",
        )
    try:
        manifest = json.loads(manifest_path.read_text())
    except json.JSONDecodeError as error:
        raise RestoreRefused(
            "MANIFEST.json is not valid JSON",
            "Replace the manifest with the one written at backup time.",
        ) from error

    if manifest.get("schema") != SCHEMA:
        raise RestoreRefused(
            "backup manifest schema is not curie-restore-inventory-v1",
            "Use the inventory this drill wrote. Do not serve a backup with an unknown layout.",
        )
    if manifest.get("rpo_rto_claimed") is not False:
        raise RestoreRefused(
            "backup claims an RPO/RTO target",
            "This drill records elapsed restore time only. Do not claim an RPO or RTO.",
        )
    if manifest.get("valkey_replay_contract") != VALKEY_REPLAY_CONTRACT:
        raise RestoreRefused(
            "backup invents a Valkey replay contract",
            "Valkey dump restore is a required input; stream/PEL replay is missing. "
            "Keep valkey_replay_contract=missing rather than claiming resume or drop.",
        )
    if manifest.get("recurring_production_backup_established") is not False:
        raise RestoreRefused(
            "backup claims a recurring production backup",
            "This drill is a one-shot synthetic restore. Do not treat it as a scheduled backup.",
        )

    for forbidden in FORBIDDEN_BACKUP_DIRS:
        if (backup_dir / forbidden).exists():
            raise RestoreRefused(
                f"backup contains {forbidden}/; keys and config are separately supplied",
                "Delete operator key material from the backup and pass it through "
                "supplied config at restore time.",
            )

    missing_keys = [name for name in SEPARATELY_SUPPLIED if not supplied_config.get(name)]
    if missing_keys:
        raise RestoreRefused(
            "separately supplied key/config is missing: " + ", ".join(missing_keys),
            "Provide POSTGRES_PASSWORD, VALKEY_PASSWORD, S3_ACCESS_KEY, "
            "S3_SECRET_KEY, and API_KEY from outside the backup. They are "
            "never stored in the backup directory.",
        )

    declared = manifest.get("components") or {}
    for component in REQUIRED_DATA_COMPONENTS:
        path = _component_path(backup_dir, component)
        if not path.exists():
            _refuse(component, "file is absent")
        if path.is_file() and path.stat().st_size == 0:
            _refuse(component, "file is empty")
        actual = hash_path(path)
        expected = (declared.get(component) or {}).get("sha256")
        if expected is None:
            _refuse(component, "manifest has no sha256")
        if actual != expected:
            _refuse(component, "sha256 does not match the manifest")

    _scan_for_secret_values(backup_dir, supplied_config)

    return {
        "ok": True,
        "schema": SCHEMA,
        "issue": "2427",
        "candidate": manifest.get("candidate"),
        "created_at": manifest.get("created_at"),
        "components": sorted(REQUIRED_DATA_COMPONENTS),
        "separately_supplied": list(SEPARATELY_SUPPLIED),
        "valkey_replay_contract": VALKEY_REPLAY_CONTRACT,
        "rpo_rto_claimed": False,
        "recurring_production_backup_established": False,
    }


def seed_mail(path: Path) -> dict[str, list[str]]:
    """Write synthetic pending and known delivery rows using MailState."""
    from curie_channel_gateway.mail.state import MailState

    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    state = MailState(str(path), max_pending=20, max_bytes=8 * 1024 * 1024)
    admitted = state.admit({"message_id": "msg-pending", "thread_id": "thr-pending"})
    if admitted != "admitted":
        raise RestoreRefused(
            f"mail seed failed to admit pending work: {admitted}",
            "Recreate the SQLite file with MailState.admit before backup.",
        )
    known = state.record_terminal("msg-known", "accepted")
    if known != "admitted":
        raise RestoreRefused(
            f"mail seed failed to record a dedup receipt: {known}",
            "Recreate the SQLite file with MailState.record_terminal before backup.",
        )
    state.connection.execute("PRAGMA wal_checkpoint(FULL)")
    state.connection.close()
    return {"pending": ["msg-pending"], "known": ["msg-known", "msg-pending"]}


def verify_mail(path: Path) -> dict[str, list[str]]:
    """Re-open restored SQLite and assert pending/dedup invariants."""
    from curie_channel_gateway.mail.state import MailState

    if not path.is_file() or path.stat().st_size == 0:
        _refuse("mail-adapter-state", "restored file is absent or empty")
    state = MailState(str(path), max_pending=20, max_bytes=8 * 1024 * 1024)
    pending = [row["message_id"] for row in state.pending()]
    known = state.known_message_ids()
    if pending != ["msg-pending"]:
        raise RestoreRefused(
            "restored mail pending work does not match the backup",
            "Restore mail-adapter/state.sqlite3 taken with this backup. Pending "
            "ids must resume; a fresh SQLite file silently drops them.",
        )
    if "msg-known" not in known or "msg-pending" not in known:
        raise RestoreRefused(
            "restored mail dedup receipts are missing",
            "Restore the checkpointed SQLite file so known message ids stay known.",
        )
    if state.admit({"message_id": "msg-pending", "thread_id": "thr-pending"}) != "known":
        raise RestoreRefused(
            "restored mail pending id was not treated as known",
            "Dedup must refuse a duplicate id rather than creating a second delivery.",
        )
    if state.admit({"message_id": "msg-known", "thread_id": "thr-other"}) != "known":
        raise RestoreRefused(
            "restored mail terminal id was not treated as known",
            "Dedup must refuse a duplicate id rather than creating a second delivery.",
        )
    state.connection.close()
    return {"pending": pending, "known": known}


def omit_component(backup_dir: Path, component: str) -> None:
    """Negative control: remove or empty a required backup component."""
    if component not in COMPONENT_PATHS:
        raise RestoreRefused(
            f"unknown backup component {component}",
            "Omit one of: " + ", ".join(REQUIRED_DATA_COMPONENTS),
        )
    path = _component_path(backup_dir, component)
    if path.is_file():
        path.unlink()
        return
    if path.is_dir():
        for child in path.rglob("*"):
            if child.is_file():
                child.unlink()


def _load_supplied(path: Path | None) -> dict[str, str]:
    if path is None:
        return {}
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise RestoreRefused(
            "supplied-config is not a JSON object",
            "Pass a JSON object of environment names to values. Values are not logged.",
        )
    return {str(key): str(value) for key, value in payload.items()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check or write a Curie restore inventory.")
    sub = parser.add_subparsers(dest="command", required=True)
    check = sub.add_parser("check", help="Refuse an incomplete or inconsistent backup")
    check.add_argument("backup_dir")
    check.add_argument("--supplied-config", required=True)
    write = sub.add_parser("write-manifest", help="Write MANIFEST.json from backup components")
    write.add_argument("backup_dir")
    write.add_argument("--candidate", default="unknown")
    seed = sub.add_parser("seed-mail", help="Write synthetic pending and known mail rows")
    seed.add_argument("state_path")
    verify = sub.add_parser("verify-mail", help="Assert restored mail pending and dedup")
    verify.add_argument("state_path")
    omit = sub.add_parser("omit", help="Remove a required component for the negative control")
    omit.add_argument("backup_dir")
    omit.add_argument("component")
    args = parser.parse_args(argv)
    try:
        if args.command == "check":
            result = check_backup(Path(args.backup_dir), _load_supplied(Path(args.supplied_config)))
            json.dump(result, sys.stdout, indent=2, sort_keys=True)
            sys.stdout.write("\n")
            return 0
        if args.command == "write-manifest":
            manifest = write_manifest(Path(args.backup_dir), candidate=args.candidate)
            json.dump(manifest, sys.stdout, indent=2, sort_keys=True)
            sys.stdout.write("\n")
            return 0
        if args.command == "seed-mail":
            result = seed_mail(Path(args.state_path))
            json.dump(result, sys.stdout, indent=2, sort_keys=True)
            sys.stdout.write("\n")
            return 0
        if args.command == "verify-mail":
            result = verify_mail(Path(args.state_path))
            json.dump(result, sys.stdout, indent=2, sort_keys=True)
            sys.stdout.write("\n")
            return 0
        omit_component(Path(args.backup_dir), args.component)
        return 0
    except RestoreRefused as error:
        json.dump(error.as_payload(), sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
