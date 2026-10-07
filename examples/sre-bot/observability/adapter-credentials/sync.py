"""Reconcile an existing worker adapter credential map. @spec SRE-CREDS-1 SRE-CREDS-8"""

from __future__ import annotations

import argparse
import base64
import binascii
import json
import os
import re
import ssl
import sys
import urllib.parse
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Never


class Invalid(Exception):
    """A boundary object violates the specified contract. @spec SRE-CREDS-1"""


class SafeParser(argparse.ArgumentParser):
    """Keep malformed invocation diagnostics out of stderr. @spec SRE-CREDS-7"""

    def error(self, message: str) -> Never:
        raise Invalid() from None


def unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Refuse ambiguous JSON object keys. @spec SRE-CREDS-1 SRE-CREDS-4"""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise Invalid()
        result[key] = value
    return result


def json_object(raw: str) -> dict[str, Any]:
    """Parse a unique-key JSON object. @spec SRE-CREDS-1 SRE-CREDS-4"""
    value = json.loads(raw, object_pairs_hook=unique_pairs)
    if not isinstance(value, dict):
        raise Invalid()
    return value


def nonblank(value: Any) -> bool:
    """Check a nonblank text field. @spec SRE-CREDS-1"""
    return isinstance(value, str) and bool(value.strip())


def resource_name(value: Any) -> bool:
    """Check Kubernetes DNS subdomain names. @spec SRE-CREDS-1 SRE-CREDS-2"""
    if not isinstance(value, str) or len(value) > 253:
        return False
    labels = value.split(".")
    return all(
        len(label) <= 63 and bool(re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?", label))
        for label in labels
    )


def data_key(value: Any) -> bool:
    """Check Kubernetes Secret data keys. @spec SRE-CREDS-1 SRE-CREDS-2"""
    return (
        isinstance(value, str)
        and bool(re.fullmatch(r"[A-Za-z0-9._-]+", value))
        and len(value) <= 253
    )


def configuration(path: str) -> dict[str, Any]:
    """Load and validate the entire explicit inventory. @spec SRE-CREDS-1"""
    value = json_object(Path(path).read_text())
    if set(value) != {"namespace", "workerDeployment", "workerContainer", "adapters"}:
        raise Invalid()
    if (
        not resource_name(value["namespace"])
        or "." in value["namespace"]
        or len(value["namespace"]) > 63
        or not resource_name(value["workerDeployment"])
    ):
        raise Invalid()
    if not nonblank(value["workerContainer"]):
        raise Invalid()
    adapters = value["adapters"]
    if not isinstance(adapters, dict) or not adapters:
        raise Invalid()
    for identity, entry in adapters.items():
        if (
            not nonblank(identity)
            or not isinstance(entry, dict)
            or set(entry) != {"sourceSecret", "sourceKey"}
        ):
            raise Invalid()
        if not resource_name(entry["sourceSecret"]) or not data_key(entry["sourceKey"]):
            raise Invalid()
    return value


def checked_identity(obj: Any, name: str, namespace: str) -> dict[str, Any]:
    """Refuse wrong or unversioned responses. @spec SRE-CREDS-2 SRE-CREDS-3"""
    if not isinstance(obj, dict):
        raise Invalid()
    metadata = obj.get("metadata")
    if (
        not isinstance(metadata, dict)
        or metadata.get("name") != name
        or metadata.get("namespace") != namespace
        or not nonblank(metadata.get("resourceVersion"))
    ):
        raise Invalid()
    return obj


def target_reference(worker: dict[str, Any], container_name: str) -> tuple[str, str]:
    """Read the sole configured worker Secret reference. @spec SRE-CREDS-2"""
    containers = worker["spec"]["template"]["spec"]["containers"]
    if not isinstance(containers, list):
        raise Invalid()
    matches = [c for c in containers if isinstance(c, dict) and c.get("name") == container_name]
    if len(matches) != 1:
        raise Invalid()
    env = matches[0]["env"]
    if not isinstance(env, list):
        raise Invalid()
    entries = [
        e for e in env if isinstance(e, dict) and e.get("name") == "CURIE_ADAPTER_CREDENTIALS"
    ]
    if len(entries) != 1:
        raise Invalid()
    entry = entries[0]
    if (
        set(entry) != {"name", "valueFrom"}
        or not isinstance(entry["valueFrom"], dict)
        or set(entry["valueFrom"]) != {"secretKeyRef"}
    ):
        raise Invalid()
    reference = entry["valueFrom"]["secretKeyRef"]
    if (
        not isinstance(reference, dict)
        or set(reference) - {"name", "key", "optional"}
        or reference.get("optional") not in (None, False)
    ):
        raise Invalid()
    name, key = reference.get("name"), reference.get("key")
    if not resource_name(name) or not data_key(key):
        raise Invalid()
    assert isinstance(name, str) and isinstance(key, str)
    return name, key


def decoded(data: dict[str, Any], key: str) -> str:
    """Strictly decode a nonblank Secret value. @spec SRE-CREDS-3 SRE-CREDS-4"""
    encoded = data[key]
    if not isinstance(encoded, str):
        raise Invalid()
    try:
        value = base64.b64decode(encoded, validate=True).decode("utf-8")
    except (binascii.Error, UnicodeError, ValueError) as exc:
        raise Invalid() from exc
    if not value.strip():
        raise Invalid()
    return value


def secret_data(secret: dict[str, Any]) -> dict[str, Any]:
    """Require a Secret data object. @spec SRE-CREDS-3 SRE-CREDS-4"""
    data = secret.get("data", {})
    if not isinstance(data, dict):
        raise Invalid()
    return data


def current_map(secret: dict[str, Any], key: str) -> dict[str, str]:
    """Decode the existing worker map or an absent empty map. @spec SRE-CREDS-4"""
    data = secret_data(secret)
    if key not in data:
        return {}
    mapping = json_object(decoded(data, key))
    if any(not nonblank(identity) or not nonblank(value) for identity, value in mapping.items()):
        raise Invalid()
    return mapping


class ServiceAccountRequest:
    """Bounded Kubernetes TLS transport. @spec SRE-CREDS-5 SRE-CREDS-8"""

    def __init__(self) -> None:
        sa_dir = Path(os.environ.get("SA_DIR", "/var/run/secrets/kubernetes.io/serviceaccount"))
        token = (sa_dir / "token").read_text().strip()
        if not token:
            raise Invalid()
        self.token = token
        self.context = ssl.create_default_context(cafile=str(sa_dir / "ca.crt"))

    def __call__(self, method: str, path: str, body: Any = None) -> dict[str, Any]:
        """Send one GET or conditional merge PATCH. @spec SRE-CREDS-5 SRE-CREDS-8"""
        if method not in {"GET", "PATCH"} or not path.startswith("/"):
            raise Invalid()
        payload = None if body is None else json.dumps(body, separators=(",", ":")).encode("utf-8")
        headers = {"Authorization": f"Bearer {self.token}", "Accept": "application/json"}
        if method == "PATCH":
            headers["Content-Type"] = "application/merge-patch+json"
        request = urllib.request.Request(
            "https://kubernetes.default.svc" + path, data=payload, headers=headers, method=method
        )
        with urllib.request.urlopen(request, context=self.context, timeout=10) as response:
            result = json.load(response)
        if not isinstance(result, dict):
            raise Invalid()
        return result


def main(argv: list[str] | None = None, *, request: Callable[..., Any] | None = None) -> int:
    """Validate, reconcile, and emit a redacted result. @spec SRE-CREDS-1 SRE-CREDS-8"""
    result: dict[str, Any] = {
        "status": "failed",
        "changed": False,
        "secretPatched": False,
        "workerRolled": False,
    }

    def fail(code: str) -> int:
        """Emit a safe stable failure. @spec SRE-CREDS-7"""
        result["status"] = "failed"
        result["error"] = code
        print(json.dumps(result))
        return 1

    parser = SafeParser(add_help=False)
    parser.add_argument("--config")
    parser.add_argument("--dry-run", action="store_true")
    try:
        args, extra = parser.parse_known_args(argv)
        if extra or not args.config:
            return fail("configuration")
        cfg = configuration(args.config)
        result.update(
            namespace=cfg["namespace"],
            workerDeployment=cfg["workerDeployment"],
            workerContainer=cfg["workerContainer"],
            adapterCount=len(cfg["adapters"]),
        )
    except (OSError, UnicodeError, ValueError, TypeError, Invalid):
        return fail("configuration")

    namespace = cfg["namespace"]
    encoded_namespace = urllib.parse.quote(namespace, safe="")
    worker_name = urllib.parse.quote(cfg["workerDeployment"], safe="")
    worker_path = f"/apis/apps/v1/namespaces/{encoded_namespace}/deployments/{worker_name}"

    try:
        client = request if request is not None else ServiceAccountRequest()
        worker = checked_identity(client("GET", worker_path), cfg["workerDeployment"], namespace)
        target_name, target_key = target_reference(worker, cfg["workerContainer"])
        result.update(targetSecret=target_name, targetKey=target_key)
    except Exception:  # noqa: BLE001 - external client failures must be redacted
        return fail("worker-inspection")

    def secret_path(name: str) -> str:
        """Address only a validated named Secret. @spec SRE-CREDS-2 SRE-CREDS-3"""
        return f"/api/v1/namespaces/{encoded_namespace}/secrets/{urllib.parse.quote(name, safe='')}"

    try:
        target = checked_identity(client("GET", secret_path(target_name)), target_name, namespace)
        if target.get("immutable") is True:
            raise Invalid()
        existing = current_map(target, target_key)
    except Exception:  # noqa: BLE001 - external client failures must be redacted
        return fail("target-inspection")

    desired: dict[str, str] = {}
    try:
        for identity, source in cfg["adapters"].items():
            source_secret = checked_identity(
                client("GET", secret_path(source["sourceSecret"])),
                source["sourceSecret"],
                namespace,
            )
            desired[identity] = decoded(secret_data(source_secret), source["sourceKey"])
    except Exception:  # noqa: BLE001 - external client failures must be redacted
        return fail("source-inspection")

    if not existing.keys() <= desired.keys():
        return fail("inventory-shrinkage")
    if desired == existing:
        result["status"] = "unchanged"
        print(json.dumps(result))
        return 0
    result["changed"] = True
    if args.dry_run:
        result["status"] = "would-change"
        print(json.dumps(result))
        return 0

    encoded_map = base64.b64encode(json.dumps(desired, separators=(",", ":")).encode()).decode(
        "ascii"
    )
    secret_patch = {
        "metadata": {"resourceVersion": target["metadata"]["resourceVersion"]},
        "data": {target_key: encoded_map},
    }
    try:
        client("PATCH", secret_path(target_name), secret_patch)
        result["secretPatched"] = True
    except Exception:  # noqa: BLE001 - external client failures must be redacted
        return fail("secret-write")

    stamp = datetime.now(UTC).isoformat()
    worker_patch = {
        "metadata": {"resourceVersion": worker["metadata"]["resourceVersion"]},
        "spec": {
            "template": {
                "metadata": {"annotations": {"curietech.ai/adapter-credentials-at": stamp}}
            }
        },
    }
    try:
        client("PATCH", worker_path, worker_patch)
        result["workerRolled"] = True
    except Exception:  # noqa: BLE001 - external client failures must be redacted
        return fail("worker-rollout")
    result["status"] = "updated"
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
