"""Read the test cluster kubeconfig the connector pod mounts.

Token auth only. An exec credential plugin would run inside the connector,
which is a second code path the admission policy does not see.
"""

from __future__ import annotations

import base64
import logging
import ssl
import urllib.parse
from typing import Any

import httpx
import yaml
from mcp.server.mcpserver.exceptions import ToolError

from curie_e2e_connector.contract import REFUSAL_MISCONFIGURED

logger = logging.getLogger(__name__)

# read_text keeps at most this many trailing bytes of a text body, so a pod
# that logs gigabytes never lands whole in connector memory.
TEXT_LIMIT_BYTES = 1024 * 1024


class ClusterError(ToolError):
    """A test cluster call failed. The message never includes the token."""


def load_kubeconfig(path: str) -> tuple[str, str, ssl.SSLContext | str | bool]:
    """Return ``(server, token, verify)`` from a kubeconfig file."""

    try:
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
    except FileNotFoundError as exc:
        raise ClusterError(
            f"{REFUSAL_MISCONFIGURED}: no kubeconfig at {path}. "
            "Store E2E_CLUSTER_KUBECONFIG with curie secrets."
        ) from exc
    except OSError as exc:
        raise ClusterError(f"{REFUSAL_MISCONFIGURED}: could not read the kubeconfig") from exc
    return load_kubeconfig_text(text)


def load_kubeconfig_text(text: str) -> tuple[str, str, ssl.SSLContext | str | bool]:
    """Return ``(server, token, verify)`` from kubeconfig contents.

    The worker's reaper (#3245) reads the kubeconfig from the connector Secret
    rather than a mounted file, and gets the same refusals the connector does.
    """

    try:
        cfg = yaml.safe_load(text)
    except yaml.YAMLError as exc:
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
    return _client(*load_kubeconfig(path), timeout=timeout)


def client_from_kubeconfig_text(text: str, timeout: float) -> httpx.Client:
    return _client(*load_kubeconfig_text(text), timeout=timeout)


def _client(
    server: str, token: str, verify: ssl.SSLContext | str | bool, *, timeout: float
) -> httpx.Client:
    return httpx.Client(
        base_url=server,
        headers={"Authorization": f"Bearer {token}"},
        verify=verify,
        timeout=timeout,
    )


class ClusterApi:
    """The few Kubernetes calls the connector tools make."""

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[int, dict[str, Any]]:
        raise NotImplementedError

    def read_text(self, path: str) -> tuple[int, str]:
        """GET a text/plain body, such as a pod log, which ``request`` drops."""

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

    def read_text(self, path: str) -> tuple[int, str]:
        try:
            with self._client.stream("GET", path) as response:
                kept = bytearray()
                for chunk in response.iter_bytes():
                    kept += chunk
                    if len(kept) > 2 * TEXT_LIMIT_BYTES:
                        del kept[:-TEXT_LIMIT_BYTES]
                return response.status_code, tail_text(bytes(kept), TEXT_LIMIT_BYTES)
        except httpx.HTTPError as exc:
            raise ClusterError("the test cluster API is unreachable") from exc


def tail_text(data: bytes, limit: int) -> str:
    """The last ``limit`` bytes of UTF-8 ``data`` as text.

    A cut that lands inside a multibyte character drops that character's
    leftover continuation bytes rather than showing a replacement mark.
    """

    if len(data) > limit:
        data = data[-limit:]
        start = 0
        while start < min(3, len(data)) and data[start] & 0xC0 == 0x80:
            start += 1
        data = data[start:]
    return data.decode("utf-8", errors="replace")


def checked_request(
    cluster: ClusterApi, method: str, path: str, body: dict[str, Any] | None = None
) -> tuple[int, dict[str, Any]]:
    code, payload = cluster.request(method, path, body)
    if code in (401, 403):
        raise ClusterError(f"the test cluster refused {method} {path} ({code})")
    return code, payload


def checked_text(cluster: ClusterApi, path: str) -> tuple[int, str]:
    code, text = cluster.read_text(path)
    if code in (401, 403):
        bare = urllib.parse.urlsplit(path).path
        raise ClusterError(f"the test cluster refused GET {bare} ({code})")
    return code, text


def ensure_object(cluster: ClusterApi, path: str, body: dict[str, Any]) -> None:
    code, _payload = checked_request(cluster, "POST", path, body)
    if code in (200, 201, 409):
        return
    raise ClusterError(f"the test cluster refused to create {body.get('kind')} ({code})")


# Job and pod helpers shared by image_build and run.


def job_state(status: Any) -> tuple[bool, str, str] | None:
    """``(succeeded, reason, message)`` once a Job finished, else None."""

    if not isinstance(status, dict):
        return None
    conditions = status.get("conditions")
    for item in conditions if isinstance(conditions, list) else []:
        if not isinstance(item, dict) or item.get("status") != "True":
            continue
        reason = str(item.get("reason") or "")
        message = str(item.get("message") or "")
        if item.get("type") == "Failed":
            return False, reason, message
        if item.get("type") == "Complete":
            return True, reason, message
    if isinstance(status.get("succeeded"), int) and status["succeeded"] > 0:
        return True, "", ""
    if isinstance(status.get("failed"), int) and status["failed"] > 0:
        return False, "", ""
    return None


def job_pods(cluster: ClusterApi, namespace: str, job: str) -> list[dict[str, Any]]:
    """The pods the Job controller labelled ``job-name=<job>``; empty on any non 200."""

    selector = urllib.parse.quote(f"job-name={job}", safe="")
    code, payload = checked_request(
        cluster, "GET", f"/api/v1/namespaces/{namespace}/pods?labelSelector={selector}"
    )
    if code != 200:
        return []
    items = payload.get("items")
    return [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []


def container_statuses(pod: dict[str, Any], key: str) -> list[dict[str, Any]]:
    """``status.<key>`` of a pod, such as ``containerStatuses``, as a list of dicts."""

    status = pod.get("status")
    found = status.get(key) if isinstance(status, dict) else None
    return [item for item in found if isinstance(item, dict)] if isinstance(found, list) else []


def terminated_state(container: dict[str, Any]) -> dict[str, Any] | None:
    state = container.get("state")
    terminated = state.get("terminated") if isinstance(state, dict) else None
    return terminated if isinstance(terminated, dict) else None


def waiting_reason(container: dict[str, Any]) -> str:
    state = container.get("state")
    waiting = state.get("waiting") if isinstance(state, dict) else None
    reason = waiting.get("reason") if isinstance(waiting, dict) else None
    return reason if isinstance(reason, str) else ""


def delete_quietly(cluster: ClusterApi, path: str, what: str) -> None:
    """Best effort cleanup that never masks the error being raised.

    ``what`` is a fixed description; the log never carries the path, which can
    name a credential Secret.
    """

    try:
        code, _payload = cluster.request("DELETE", path)
    except ClusterError:
        logger.warning("e2e cleanup could not reach the cluster to delete %s", what)
        return
    if not (200 <= code < 300 or code == 404):
        logger.warning("e2e cleanup was refused deleting %s (%s)", what, code)
