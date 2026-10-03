"""Read the test cluster kubeconfig the connector pod mounts.

Token auth only. An exec credential plugin would run inside the connector,
which is a second code path the admission policy does not see.
"""

from __future__ import annotations

import base64
import ssl
from typing import Any

import httpx
import yaml
from mcp.server.mcpserver.exceptions import ToolError

from curie_e2e_connector.contract import REFUSAL_MISCONFIGURED


class ClusterError(ToolError):
    """A test cluster call failed. The message never includes the token."""


def load_kubeconfig(path: str) -> tuple[str, str, ssl.SSLContext | str | bool]:
    """Return ``(server, token, verify)`` from a kubeconfig file."""

    try:
        with open(path, encoding="utf-8") as handle:
            cfg = yaml.safe_load(handle)
    except FileNotFoundError as exc:
        raise ClusterError(
            f"{REFUSAL_MISCONFIGURED}: no kubeconfig at {path}. "
            "Store E2E_CLUSTER_KUBECONFIG with curie secrets."
        ) from exc
    except (OSError, yaml.YAMLError) as exc:
        raise ClusterError(f"{REFUSAL_MISCONFIGURED}: could not read the kubeconfig") from exc

    try:
        cluster = cfg["clusters"][0]["cluster"]
        user = cfg["users"][0]["user"]
        server = cluster["server"]
    except (KeyError, IndexError, TypeError) as exc:
        raise ClusterError(
            f"{REFUSAL_MISCONFIGURED}: kubeconfig is missing a cluster server"
        ) from exc

    if not isinstance(user, dict) or "exec" in user:
        raise ClusterError(
            f"{REFUSAL_MISCONFIGURED}: kubeconfig must use a bearer token, not an exec plugin"
        )
    token = user.get("token")
    if not isinstance(token, str) or not token:
        raise ClusterError(f"{REFUSAL_MISCONFIGURED}: kubeconfig is missing a user token")
    if not isinstance(server, str) or not server.startswith("https://"):
        raise ClusterError(f"{REFUSAL_MISCONFIGURED}: kubeconfig server must be https")

    verify: ssl.SSLContext | str | bool = True
    ca_data = cluster.get("certificate-authority-data")
    ca_path = cluster.get("certificate-authority")
    if isinstance(ca_data, str) and ca_data:
        try:
            pem = base64.b64decode(ca_data).decode("ascii")
        except (ValueError, UnicodeDecodeError) as exc:
            raise ClusterError(
                f"{REFUSAL_MISCONFIGURED}: kubeconfig certificate authority is not base64 PEM"
            ) from exc
        context = ssl.create_default_context()
        try:
            context.load_verify_locations(cadata=pem)
        except ssl.SSLError as exc:
            raise ClusterError(
                f"{REFUSAL_MISCONFIGURED}: kubeconfig certificate authority is not usable"
            ) from exc
        verify = context
    elif isinstance(ca_path, str) and ca_path:
        verify = ca_path
    elif cluster.get("insecure-skip-tls-verify"):
        raise ClusterError(
            f"{REFUSAL_MISCONFIGURED}: refusing a kubeconfig that skips TLS verification"
        )
    return server, token, verify


def client_from_kubeconfig(path: str, timeout: float) -> httpx.Client:
    server, token, verify = load_kubeconfig(path)
    return httpx.Client(
        base_url=server,
        headers={"Authorization": f"Bearer {token}"},
        verify=verify,
        timeout=timeout,
    )


class ClusterApi:
    """The few Kubernetes calls env_create and env_destroy make."""

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[int, dict[str, Any]]:
        raise NotImplementedError


class HttpxCluster(ClusterApi):
    def __init__(self, client: httpx.Client) -> None:
        self._client = client

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[int, dict[str, Any]]:
        try:
            response = self._client.request(method, path, json=body)
        except httpx.HTTPError as exc:
            raise ClusterError("the test cluster API is unreachable") from exc
        payload: dict[str, Any] = {}
        if response.content:
            try:
                parsed = response.json()
            except ValueError:
                parsed = None
            if isinstance(parsed, dict):
                payload = parsed
        return response.status_code, payload
