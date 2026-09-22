"""Patch one connector value through the Pod's Kubernetes identity."""

import argparse
import base64
import hashlib
import json
import os
import re
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

DEFAULT_TARGET = "acme-harness-acme-fixture-connector-secrets"
KEY = "ROTATED_KEY"
TOKEN_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
NAMESPACE_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
CA_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
DNS_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$")
MAX_VALUE_BYTES = 1024 * 1024
MAX_CONFLICT_ATTEMPTS = 5


class RotationError(RuntimeError):
    """A sanitized failure safe to write to stderr."""


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", action="store_true")
    parser.add_argument("--target", default=DEFAULT_TARGET)
    return parser.parse_args()


def _read_value() -> bytes:
    value = sys.stdin.buffer.read(MAX_VALUE_BYTES + 1)
    if not value:
        raise RotationError("stdin value is empty")
    if len(value) > MAX_VALUE_BYTES:
        raise RotationError("stdin value is too large")
    return value


def _read_runtime_file(path: Path, label: str) -> str:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise RotationError(f"cannot read the in cluster {label}") from exc
    if not value:
        raise RotationError(f"the in cluster {label} is empty")
    return value


class KubernetesClient:
    def __init__(self) -> None:
        host = os.environ.get("KUBERNETES_SERVICE_HOST", "")
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        if not host or not port.isdigit():
            raise RotationError("the in cluster Kubernetes endpoint is unavailable")
        self.namespace = _read_runtime_file(NAMESPACE_PATH, "namespace")
        self.token = _read_runtime_file(TOKEN_PATH, "service account token")
        context = ssl.create_default_context(cafile=str(CA_PATH))
        self.opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            urllib.request.HTTPSHandler(context=context),
        )
        quoted_namespace = urllib.parse.quote(self.namespace, safe="")
        self.base_url = f"https://{host}:{port}/api/v1/namespaces/{quoted_namespace}/secrets"

    def _request(
        self,
        method: str,
        target: str,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self.base_url}/{urllib.parse.quote(target, safe='')}"
        encoded = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        if encoded is not None:
            headers["Content-Type"] = "application/merge-patch+json"
        request = urllib.request.Request(url, data=encoded, headers=headers, method=method)
        try:
            with self.opener.open(request, timeout=10) as response:
                payload = response.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 409:
                raise
            raise RotationError(
                f"Kubernetes API refused Secret access with status {exc.code}"
            ) from exc
        except (OSError, urllib.error.URLError) as exc:
            raise RotationError("Kubernetes API request failed") from exc
        try:
            parsed = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RotationError("Kubernetes API returned an invalid response") from exc
        if not isinstance(parsed, dict):
            raise RotationError("Kubernetes API returned an invalid response")
        return parsed

    def get(self, target: str) -> dict[str, Any]:
        return self._request("GET", target)

    def patch(self, target: str, body: dict[str, Any]) -> None:
        self._request("PATCH", target, body)


def _current_value(document: dict[str, Any]) -> bytes | None:
    data = document.get("data")
    if data is None:
        return None
    if not isinstance(data, dict):
        raise RotationError("Secret data has an invalid shape")
    encoded = data.get(KEY)
    if encoded is None:
        return None
    if not isinstance(encoded, str):
        raise RotationError("the rotated key has an invalid shape")
    try:
        return base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise RotationError("the rotated key has invalid encoding") from exc


def _resource_version(document: dict[str, Any]) -> str:
    metadata = document.get("metadata")
    if not isinstance(metadata, dict):
        raise RotationError("Secret metadata has an invalid shape")
    version = metadata.get("resourceVersion")
    if not isinstance(version, str) or not version:
        raise RotationError("Secret resourceVersion is unavailable")
    return version


def rotate(client: KubernetesClient, target: str, value: bytes, bootstrap: bool) -> bytes:
    for _ in range(MAX_CONFLICT_ATTEMPTS):
        document = client.get(target)
        current = _current_value(document)
        if bootstrap and current is not None:
            return current
        patch = {
            "metadata": {"resourceVersion": _resource_version(document)},
            "data": {KEY: base64.b64encode(value).decode("ascii")},
        }
        try:
            client.patch(target, patch)
        except urllib.error.HTTPError as exc:
            if exc.code == 409:
                continue
            raise
        return value
    raise RotationError("Secret changed during every patch attempt")


def main() -> int:
    args = _arguments()
    if len(args.target) > 253 or DNS_LABEL.fullmatch(args.target) is None:
        raise RotationError("target must be a Kubernetes Secret name")
    value = _read_value()
    result = rotate(KubernetesClient(), args.target, value, args.bootstrap)
    print(hashlib.sha256(result).hexdigest())
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RotationError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from None
