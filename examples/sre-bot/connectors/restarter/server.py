"""Restart one allowlisted Deployment, idempotently, for automated remediation.

@spec ACTION-EXECUTOR-9 @spec ACTION-EXECUTOR-16 @spec AUTOMATED-REMEDIATION-22.
One tool over streamable HTTP at ``/mcp``:

* ``restart_deployment {name, restart_at}`` sets
  ``kubectl.kubernetes.io/restartedAt`` on the pod template of the Deployment
  ``name`` in the one configured namespace, the annotation
  ``kubectl rollout restart`` writes, so the controller rolls the workload. It
  replies with a sealed ``prior``, the ``version`` the write left and the
  ``target`` (docs/writing-a-reversible-connector.md).

A rolling restart cannot be undone, so this is not a reversible connector: it
serves no ``restore`` and no ``observe_version``, the capability probe never
records it as restore capable, and every record it leaves is not undoable.
ADR 0203 admits it under REMEDIATION-11 instead: idempotent, bounded to the
policy's declared target and verified. Its qualification is two confirmed
approved forwards with the same canonical arguments whose records left the same
``post_version``, and the ledger records a ``post_version`` only from a sealed
reply. That is the only reason the reply is sealed. Writing the previous
annotation back would roll the workload a second time, not undo the first.

How a restart is idempotent
---------------------------
``restart_at`` is the token that names the restart: a Unix time in seconds,
normally the triggering alert's start time. It is an integer because a
remediation policy bounds an argument only by a closed set or a numeric range,
so a free-form string token could not be bounded. The annotation is set to
``restart_at`` formatted as RFC 3339 UTC.

* The same ``restart_at`` again finds the annotation already at that value,
  writes nothing and replies with the same version.
* The annotation never moves backwards. When it already names an instant at or
  after ``restart_at`` (``kubectl`` writes local time with an offset, so values
  are compared as instants), the workload has been restarted since that
  incident began and the call writes nothing. A late or repeated nomination
  for an old incident cannot roll the workload again.
* A value that names no instant, or names one without a zone, proves nothing
  and is replaced.
* A ``restart_at`` more than ``MAX_SKEW_SECONDS`` in the future is refused,
  because it would suppress every later restart until that instant.

What the version is
-------------------
``deployments/<uid>/restartedAt=<annotation>``: the state the restart controls,
not the write. Two calls with the same arguments leave the same version. A
Deployment deleted and recreated under the same name is a different version.
Other changes to the Deployment are not part of it, as other changes to a
Deployment are not part of the scaler's version.

Order of checks
---------------
Refused before any request: the namespace or allowlist is unset, the name is
not exactly an allowlisted name, ``restart_at`` is negative or in the future.
Then ``sealing_key_unavailable`` without a key. Then the Deployment is read
(any read failure refuses, so nothing is written that was not sealed first); a
paused Deployment is refused, because the annotation would not roll it. The
write is a JSON merge patch of the one annotation that carries the read's
``uid`` and ``resourceVersion`` as preconditions. A stale ``resourceVersion``
answers ``409``, which is retried only while the uid and annotation are still
the ones read (the controller's status writes also move ``resourceVersion``); a
concurrent restart refuses ``version_conflict`` with nothing written. A
Deployment recreated under the name since the read answers ``422`` (``uid`` is
immutable), a tool error with nothing written. Both observed from a v1.37 API
server.

The credential reads and patches whole Deployments, pod templates included, so
nothing of the Deployment beyond its uid and this one annotation leaves the
connector, in a reply or in a log line.

Key custody (ACTION-EXECUTOR-16) is the reference connector's:
``SNAPSHOT_SEALING_KEY=<kid>:<base64 of 32 bytes>``. Nothing here opens an
envelope, so there are no retained keys. The cipher is AES-256-GCM,
``ciphertext = base64(nonce[12] || ct || tag)``, associated data the canonical
JSON of ``target``, plaintext ``{"restartedAt": <previous value or null>,
"uid": "<uid>"}``.

Nothing here logs argument values, ciphertext, versions, tokens or key
material.
"""

# NOTE: no `from __future__ import annotations`, deliberately, as in the other
# connectors: MCPServer introspects tool signatures and string annotations break
# that at import time.

import base64
import binascii
import json
import logging
import os
import re
import ssl
import sys
import time
from datetime import datetime
from typing import Any, Final

import httpx
import yaml
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import StrictInt, StrictStr

log = logging.getLogger("restarter")

SEALED_CONSTANT: Final = "curie.snapshot.v1"
NONCE_BYTES: Final = 12
KEY_BYTES: Final = 32
_KID = re.compile(r"[A-Za-z0-9._-]{1,64}")

# Connector refusal codes (ACTION-EXECUTOR-20).
SEALING_KEY_UNAVAILABLE: Final = "sealing_key_unavailable"
VERSION_CONFLICT: Final = "version_conflict"

TARGET_KIND: Final = "Deployment"
RESTARTED_AT: Final = "kubectl.kubernetes.io/restartedAt"
# How far ahead of this connector's clock a token may name, for clock skew
# between the alert source and the cluster.
MAX_SKEW_SECONDS: Final = 300
# Writes attempted per call while only ``resourceVersion`` moved under them.
CONFLICT_ATTEMPTS: Final = 3


# --------------------------------------------------------------------------- #
# Configuration.
# --------------------------------------------------------------------------- #


class ConfigError(ValueError):
    """A configuration variable is malformed. The message never carries key text."""


def _parse_key(entry: str) -> tuple[str, AESGCM]:
    kid, sep, encoded = entry.strip().partition(":")
    if not sep or _KID.fullmatch(kid) is None:
        raise ConfigError("SNAPSHOT_SEALING_KEY is not <kid>:<base64 of 32 bytes>")
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError):
        raise ConfigError(f"SNAPSHOT_SEALING_KEY for kid {kid} is not standard base64") from None
    if len(raw) != KEY_BYTES:
        raise ConfigError(f"SNAPSHOT_SEALING_KEY for kid {kid} is not {KEY_BYTES} bytes")
    return kid, AESGCM(raw)


class Settings:
    """What this connector may touch, read once at start.

    An empty namespace or allowlist is not an error at start: every call is
    refused instead, so a declared but unconfigured connector fails closed and
    says why on each call.
    """

    def __init__(self, env: dict[str, str]) -> None:
        self.kubeconfig = env.get("KUBECONFIG_PATH", "/secrets/kubeconfig")
        self.namespace = env.get("RESTARTER_NAMESPACE", "").strip()
        self.deployments = frozenset(
            entry.strip()
            for entry in env.get("RESTARTER_DEPLOYMENTS", "").split(",")
            if entry.strip()
        )
        try:
            self.timeout = float(env.get("K8S_TIMEOUT_SECONDS", "30"))
        except ValueError:
            raise ConfigError("K8S_TIMEOUT_SECONDS is not a number") from None
        current = env.get("SNAPSHOT_SEALING_KEY", "")
        self.key: tuple[str, AESGCM] | None = _parse_key(current) if current.strip() else None


# --------------------------------------------------------------------------- #
# The Kubernetes API: get and patch on named Deployments only.
# --------------------------------------------------------------------------- #


def _client(settings: Settings) -> httpx.Client:
    """An httpx client from the mounted kubeconfig, or ``ToolError``.

    The kubeconfig parsing and both CA shapes are the scaler's and the
    self-upgrade connector's, for the same two live failures they record.
    """

    try:
        with open(settings.kubeconfig, encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh)
    except FileNotFoundError:
        raise ToolError(
            f"no kubeconfig at {settings.kubeconfig}: the restarter credential is not mounted"
        ) from None
    except (OSError, yaml.YAMLError) as exc:
        raise ToolError(
            f"could not read the kubeconfig at {settings.kubeconfig}: {type(exc).__name__}"
        ) from None

    try:
        cluster = cfg["clusters"][0]["cluster"]
        user = cfg["users"][0]["user"]
        server = cluster["server"]
        token = user["token"]
    except (KeyError, IndexError, TypeError):
        raise ToolError("kubeconfig is missing a cluster server or a user token") from None

    verify: Any = True
    ca_data = cluster.get("certificate-authority-data")
    ca_path = cluster.get("certificate-authority")
    if ca_data:
        try:
            pem = base64.b64decode(ca_data).decode("ascii")
        except (ValueError, TypeError, UnicodeDecodeError):
            raise ToolError("kubeconfig certificate-authority-data is not a base64 PEM") from None
        ctx = ssl.create_default_context()
        try:
            ctx.load_verify_locations(cadata=pem)
        except ssl.SSLError:
            raise ToolError(
                "kubeconfig certificate-authority-data is not a usable CA certificate"
            ) from None
        verify = ctx
    elif ca_path:
        verify = ca_path
    elif cluster.get("insecure-skip-tls-verify"):
        raise ToolError(
            "kubeconfig sets insecure-skip-tls-verify; refusing to write over an "
            "unverified connection"
        )

    return httpx.Client(
        base_url=server,
        headers={"Authorization": f"Bearer {token}"},
        verify=verify,
        timeout=settings.timeout,
    )


class _Conflict(Exception):
    """The API server answered ``409``: the object moved since it was read."""


class Live:
    """One read of a Deployment: only what a version and a compare-and-swap need."""

    def __init__(
        self, uid: str, resource_version: str, restarted_at: str | None, paused: bool
    ) -> None:
        self.uid = uid
        self.resource_version = resource_version
        self.restarted_at = restarted_at
        self.paused = paused

    @property
    def version(self) -> str:
        return f"deployments/{self.uid}/restartedAt={self.restarted_at or ''}"


def _live(response: httpx.Response, name: str) -> Live:
    """Parse a Deployment reply, or ``ToolError``. Keeps nothing else of it."""

    try:
        body = response.json()
    except ValueError:
        raise ToolError(f"the Deployment {name} is not JSON") from None
    if not isinstance(body, dict) or body.get("kind") != TARGET_KIND:
        raise ToolError(f"the API server did not answer a Deployment for {name}")
    metadata = body.get("metadata")
    spec = body.get("spec")
    if not isinstance(metadata, dict) or not isinstance(spec, dict):
        raise ToolError(f"the Deployment {name} has no metadata or spec")
    uid = metadata.get("uid")
    resource_version = metadata.get("resourceVersion")
    if not (
        isinstance(uid, str) and uid and isinstance(resource_version, str) and resource_version
    ):
        raise ToolError(f"the Deployment {name} has no uid or resourceVersion")
    template = spec.get("template")
    template_metadata = template.get("metadata") if isinstance(template, dict) else None
    annotations = (
        template_metadata.get("annotations") if isinstance(template_metadata, dict) else None
    )
    restarted_at = annotations.get(RESTARTED_AT) if isinstance(annotations, dict) else None
    if restarted_at is not None and not isinstance(restarted_at, str):
        raise ToolError(f"the Deployment {name} has a restartedAt annotation that is not text")
    return Live(uid, resource_version, restarted_at, spec.get("paused") is True)


def _refuse_status(response: httpx.Response, name: str, verb: str) -> None:
    status = response.status_code
    if status == 409:
        raise _Conflict()
    if status == 404:
        raise ToolError(f"no Deployment {name} in this connector's namespace")
    if status in (401, 403):
        raise ToolError(
            f"the restarter credential may not {verb} the Deployment {name} ({status}); "
            "it needs that verb on deployments with this name in resourceNames"
        )
    if status == 422 and verb == "patch":
        raise ToolError(
            f"the API server refused the patch of {name} (422): the Deployment was "
            "recreated since it was read, or the patch is invalid; nothing was written"
        )
    if status >= 400:
        raise ToolError(f"the API server refused to {verb} the Deployment {name}: {status}")


class Deployments:
    """The Deployments of one namespace, read and patched by name."""

    def __init__(self, client: httpx.Client, namespace: str) -> None:
        self.client = client
        self.namespace = namespace

    def __enter__(self) -> "Deployments":
        return self

    def __exit__(self, *exc: object) -> None:
        self.client.close()

    def _path(self, name: str) -> str:
        return f"/apis/apps/v1/namespaces/{self.namespace}/deployments/{name}"

    def read(self, name: str) -> Live:
        try:
            response = self.client.get(self._path(name))
        except httpx.HTTPError as exc:
            raise ToolError(f"could not reach the API server: {type(exc).__name__}") from None
        _refuse_status(response, name, "get")
        return _live(response, name)

    def write(self, name: str, read: Live, value: str) -> Live:
        """Merge-patch the annotation, conditional on the read's uid and resourceVersion."""

        body = {
            "metadata": {"uid": read.uid, "resourceVersion": read.resource_version},
            "spec": {"template": {"metadata": {"annotations": {RESTARTED_AT: value}}}},
        }
        try:
            response = self.client.patch(
                self._path(name),
                content=json.dumps(body).encode("utf-8"),
                headers={"Content-Type": "application/merge-patch+json"},
            )
        except httpx.HTTPError as exc:
            raise ToolError(f"could not reach the API server: {type(exc).__name__}") from None
        _refuse_status(response, name, "patch")
        written = _live(response, name)
        if written.uid != read.uid or written.restarted_at != value:
            raise ToolError(f"the API server did not report the requested restart for {name}")
        return written

    def compare_and_set(self, name: str, read: Live, value: str) -> Live:
        """Write ``value`` while the uid and annotation are the ones read, or ``_Conflict``."""

        current = read
        for _ in range(CONFLICT_ATTEMPTS):
            try:
                return self.write(name, current, value)
            except _Conflict:
                current = self.read(name)
                if current.uid != read.uid or current.restarted_at != read.restarted_at:
                    raise
        raise _Conflict()


# --------------------------------------------------------------------------- #
# Tokens and instants.
# --------------------------------------------------------------------------- #


def _rfc3339(epoch: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(epoch))


def _instant(value: str | None) -> float | None:
    """The instant an annotation value names, or ``None`` when it names none."""

    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.timestamp()


# --------------------------------------------------------------------------- #
# The server.
# --------------------------------------------------------------------------- #

# A write, gated for the model. Idempotent: the same token twice leaves the
# same state. Destructive: it replaces every Pod of the workload.
WRITE = ToolAnnotations(
    read_only_hint=False, destructive_hint=True, idempotent_hint=True, open_world_hint=False
)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _refused(code: str) -> dict[str, Any]:
    return {"ok": False, "refused": code}


def _seal(key: tuple[str, AESGCM], target: dict[str, Any], read: Live) -> dict[str, str]:
    kid, aead = key
    nonce = os.urandom(NONCE_BYTES)
    body = aead.encrypt(
        nonce,
        _canonical({"restartedAt": read.restarted_at, "uid": read.uid}),
        _canonical(target),
    )
    return {
        "sealed": SEALED_CONSTANT,
        "kid": kid,
        "ciphertext": base64.b64encode(nonce + body).decode("ascii"),
    }


def build(settings: Settings) -> MCPServer:
    """The MCP server for one configuration."""

    mcp = MCPServer("restarter")

    def bounded(name: str) -> str:
        if not settings.namespace:
            raise ToolError("RESTARTER_NAMESPACE is not set: this connector refuses every call")
        if not settings.deployments:
            raise ToolError("RESTARTER_DEPLOYMENTS is empty: this connector refuses every call")
        if name not in settings.deployments:
            permitted = ", ".join(sorted(settings.deployments))
            raise ToolError(
                f"{name!r} is not in this connector's allowlist ({permitted}); "
                "widening it is an operator change"
            )
        return name

    @mcp.tool(annotations=WRITE)
    def restart_deployment(name: StrictStr, restart_at: StrictInt) -> dict[str, Any]:
        """Roll every Pod of one allowlisted Deployment, once per ``restart_at``.

        Equivalent to ``kubectl rollout restart deploy/<name>`` in this
        connector's namespace, except that the restart is named by
        ``restart_at``: a Unix time in seconds, normally when the alert you are
        handling started. It changes only the pod template's
        ``kubectl.kubernetes.io/restartedAt`` annotation, never an image, a
        command or a replica count.

        Calling it again with the same ``restart_at`` changes nothing. If the
        workload was already restarted at or after ``restart_at``, nothing
        changes either. A ``restart_at`` in the future is refused.

        This is a WRITE and it is gated: the call pauses for human approval.
        Say which Deployment, which ``restart_at`` and why, before calling.

        The reply is a JSON object: ``ok``, ``summary``, ``prior`` (sealed; a
        restart cannot be undone), ``version`` and ``target``. A refusal is
        ``{"ok": false, "refused": <code>}`` or a tool error, and writes
        nothing.
        """

        bounded(name)
        if restart_at < 0:
            raise ToolError("restart_at must be a Unix time in seconds, not negative")
        if restart_at > time.time() + MAX_SKEW_SECONDS:
            raise ToolError(
                "restart_at is in the future; it names when the incident started, and a "
                "future value would suppress every restart until then"
            )
        if settings.key is None:
            log.info("restart_deployment refused: %s", SEALING_KEY_UNAVAILABLE)
            return _refused(SEALING_KEY_UNAVAILABLE)
        target = {"kind": TARGET_KIND, "namespace": settings.namespace, "name": name}
        value = _rfc3339(restart_at)
        with Deployments(_client(settings), settings.namespace) as deployments:
            read = deployments.read(name)
            if read.paused:
                raise ToolError(
                    f"the Deployment {name} is paused, so a restart would not roll it; "
                    "resuming it is an operator decision"
                )
            prior = _seal(settings.key, target, read)
            already = _instant(read.restarted_at)
            if read.restarted_at == value or (already is not None and already >= restart_at):
                left = read
                summary = f"{settings.namespace}/{name} was already restarted at or after {value}"
                log.info("restart_deployment: already restarted")
            else:
                try:
                    left = deployments.compare_and_set(name, read, value)
                except _Conflict:
                    log.info("restart_deployment refused: %s", VERSION_CONFLICT)
                    return _refused(VERSION_CONFLICT)
                summary = f"restarted {settings.namespace}/{name} for {value}"
                log.info("restart_deployment wrote")
        return {
            "ok": True,
            "summary": f"{summary}; a restart cannot be undone",
            "prior": prior,
            "version": left.version,
            "target": target,
        }

    return mcp


def main() -> int:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), stream=sys.stderr)
    try:
        settings = Settings(dict(os.environ))
    except ConfigError as exc:
        log.error("refusing to start: %s", exc)
        return 1
    log.info(
        "restarter: namespace %s, %d allowlisted deployment(s), sealing key %s",
        "set" if settings.namespace else "unset",
        len(settings.deployments),
        "held" if settings.key is not None else "absent",
    )
    build(settings).run(
        transport="streamable-http",
        host=os.environ.get("BIND_ADDRESS", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
        streamable_http_path="/mcp",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
