"""Set one allowlisted Deployment's replica count, in a way the platform can undo.

@spec ACTION-EXECUTOR-9 @spec ACTION-EXECUTOR-13 @spec ACTION-EXECUTOR-15
@spec ACTION-EXECUTOR-16 @spec AUTOMATED-REMEDIATION-22. Three tools over
streamable HTTP at ``/mcp``:

* ``scale_deployment {name, replicas}`` sets the replica count of the
  Deployment ``name`` in the one configured namespace, through the ``scale``
  subresource, and replies with the sealed prior state, the ``version`` the
  write left and the ``target``
  (docs/writing-a-reversible-connector.md);
* ``observe_version {target}`` (read-only) reports the live version;
* ``restore {target, prior_state, expected_version?}`` opens the envelope and
  writes the prior count back. The runner hides it from the model; only the
  platform's executor calls it, behind a one-shot grant.

Why the arguments are flat
--------------------------
A remediation policy bounds a tool's arguments with JSON scalars and names one
of them as the target (``target.argument``). ``name`` is that argument and
``replicas`` is bounded by a range, so a policy can say "only ``example-api``,
only to 1" in its own terms. The namespace is not an argument: it is this
connector's configuration, and the credential's Role lives only there.

What the version is, and why it is not random
---------------------------------------------
``version`` names the state a write left, not the write: the Deployment's uid
and the replica count in force, ``deployments/scale/<uid>/replicas=<n>``. Two
writes that leave the same count leave the same version, which is what an
idempotent qualification compares (two approved forwards with the same
arguments whose records left the same ``post_version``). Any change of the
count, and a Deployment deleted and recreated under the same name, is a
different version, so ``observe_version`` before a restore refuses when someone
has moved the count since. The ``scale`` subresource carries no
``metadata.generation`` and its ``resourceVersion`` moves with status updates,
so neither is part of the version; reading the Deployment itself for its
generation would need ``get`` on ``deployments``, which exposes the pod template
this credential deliberately cannot see.

Order of checks
---------------
The forward write refuses before any request when the name is not allowlisted,
the namespace or allowlist is unset, or the count is outside ``0`` to
``SCALER_MAX_REPLICAS``; then refuses ``sealing_key_unavailable`` without a
current key; then reads the ``Scale`` (refusing on any read failure, so nothing
is written that could not be undone) and seals what it read. A count already in
force writes nothing. Otherwise the write is a ``PUT`` carrying the read's
``resourceVersion``. A ``409`` is retried only while the uid and count are
still the ones read (the controller's status writes also move
``resourceVersion``); a concurrent change of the count refuses
``version_conflict`` with nothing written.

``restore`` checks envelope grammar (``snapshot_unopenable``), the kid is held
(``sealing_key_unavailable``), the ciphertext opens under that key with the
target as associated data (``snapshot_unopenable``), then reads the live
``Scale``: a different uid or an ``expected_version`` that is not the live
version refuses ``version_conflict``. Its write is the same compare-and-swap.
It is not bounded by ``SCALER_MAX_REPLICAS``: it puts back a count that was in
force, whatever it was.

Key custody (ACTION-EXECUTOR-16) is the reference connector's:
``SNAPSHOT_SEALING_KEY=<kid>:<base64 of 32 bytes>`` seals and opens,
``SNAPSHOT_SEALING_KEYS_RETAINED`` (comma separated, same form) only opens.
The cipher is AES-256-GCM, ``ciphertext = base64(nonce[12] || ct || tag)``,
associated data the canonical JSON of ``target``, plaintext
``{"replicas": n, "uid": "<uid>"}``.

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
from typing import Any, Final

import httpx
import yaml
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import StrictInt, StrictStr

log = logging.getLogger("scaler")

# --------------------------------------------------------------------------- #
# The sealed envelope grammar (ACTION-EXECUTOR-9), copied from the worker's
# ``curie_worker.sealed_snapshot.is_sealed_envelope`` as the reference connector
# copies it: this connector ships in its own image and cannot import the worker.
# --------------------------------------------------------------------------- #

SEALED_CONSTANT: Final = "curie.snapshot.v1"
MAX_CIPHERTEXT_BYTES: Final = 65536
REDACTION_PLACEHOLDER_PREFIX: Final = "[REDACTED:"
NONCE_BYTES: Final = 12
KEY_BYTES: Final = 32

_ENVELOPE_KEYS: Final = frozenset({"sealed", "kid", "ciphertext"})
_KID = re.compile(r"[A-Za-z0-9._-]{1,64}")
_STANDARD_BASE64 = re.compile(r"(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?")

# Connector refusal codes (ACTION-EXECUTOR-20).
SEALING_KEY_UNAVAILABLE: Final = "sealing_key_unavailable"
SNAPSHOT_UNOPENABLE: Final = "snapshot_unopenable"
VERSION_CONFLICT: Final = "version_conflict"

TARGET_KIND: Final = "Deployment"
# Writes attempted per call while only ``resourceVersion`` moved under them.
CONFLICT_ATTEMPTS: Final = 3
_TARGET_KEYS: Final = frozenset({"kind", "namespace", "name"})


def _envelope_ciphertext(value: Any) -> bytes | None:
    """The decoded ciphertext of a grammatical envelope, else ``None``."""

    if not isinstance(value, dict) or set(value) != _ENVELOPE_KEYS:
        return None
    if value["sealed"] != SEALED_CONSTANT:
        return None
    kid = value["kid"]
    if not isinstance(kid, str) or _KID.fullmatch(kid) is None:
        return None
    ciphertext = value["ciphertext"]
    if not isinstance(ciphertext, str) or not ciphertext:
        return None
    if REDACTION_PLACEHOLDER_PREFIX in ciphertext:
        return None
    if len(ciphertext) > 4 * ((MAX_CIPHERTEXT_BYTES + 2) // 3):
        return None
    if _STANDARD_BASE64.fullmatch(ciphertext) is None:
        return None
    try:
        decoded = base64.b64decode(ciphertext, validate=True)
    except (binascii.Error, ValueError):
        return None
    if not 0 < len(decoded) <= MAX_CIPHERTEXT_BYTES:
        return None
    return decoded


# --------------------------------------------------------------------------- #
# Configuration.
# --------------------------------------------------------------------------- #


class ConfigError(ValueError):
    """A configuration variable is malformed. The message never carries key text."""


def _parse_key(entry: str, name: str) -> tuple[str, AESGCM]:
    kid, sep, encoded = entry.strip().partition(":")
    if not sep or _KID.fullmatch(kid) is None:
        raise ConfigError(f"{name}: an entry is not <kid>:<base64 of 32 bytes>")
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError):
        raise ConfigError(f"{name}: key for kid {kid} is not standard base64") from None
    if len(raw) != KEY_BYTES:
        raise ConfigError(f"{name}: key for kid {kid} is not {KEY_BYTES} bytes")
    return kid, AESGCM(raw)


class Keyring:
    """The current sealing key, if any, and every key that may open."""

    def __init__(self, current: str, retained: str) -> None:
        self.current: tuple[str, AESGCM] | None = None
        self.openers: dict[str, AESGCM] = {}
        if current.strip():
            self.current = _parse_key(current, "SNAPSHOT_SEALING_KEY")
            self.openers[self.current[0]] = self.current[1]
        for entry in retained.split(","):
            if entry.strip():
                kid, aead = _parse_key(entry, "SNAPSHOT_SEALING_KEYS_RETAINED")
                if kid in self.openers:
                    raise ConfigError(
                        f"SNAPSHOT_SEALING_KEYS_RETAINED: kid {kid} is already in use"
                    )
                self.openers[kid] = aead


class Settings:
    """What this connector may touch, read once at start.

    An empty namespace or allowlist is not an error at start: every call is
    refused instead, so a declared but unconfigured connector fails closed and
    says why on each call.
    """

    def __init__(self, env: dict[str, str]) -> None:
        self.kubeconfig = env.get("KUBECONFIG_PATH", "/secrets/kubeconfig")
        self.namespace = env.get("SCALER_NAMESPACE", "").strip()
        self.deployments = frozenset(
            entry.strip() for entry in env.get("SCALER_DEPLOYMENTS", "").split(",") if entry.strip()
        )
        ceiling = env.get("SCALER_MAX_REPLICAS", "1").strip()
        if not ceiling.isdigit():
            raise ConfigError("SCALER_MAX_REPLICAS is not a non-negative integer")
        self.max_replicas = int(ceiling)
        try:
            self.timeout = float(env.get("K8S_TIMEOUT_SECONDS", "30"))
        except ValueError:
            raise ConfigError("K8S_TIMEOUT_SECONDS is not a number") from None
        self.keys = Keyring(
            env.get("SNAPSHOT_SEALING_KEY", ""), env.get("SNAPSHOT_SEALING_KEYS_RETAINED", "")
        )


# --------------------------------------------------------------------------- #
# The Kubernetes API, through the scale subresource only.
# --------------------------------------------------------------------------- #


def _client(settings: Settings) -> httpx.Client:
    """An httpx client from the mounted kubeconfig, or ``ToolError``.

    The kubeconfig parsing and both CA shapes are the self-upgrade connector's,
    for the same two live failures it records: a path-only CA check that left
    verification against the system store, and a temp-file CA on a read-only
    root filesystem.
    """

    try:
        with open(settings.kubeconfig, encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh)
    except FileNotFoundError:
        raise ToolError(
            f"no kubeconfig at {settings.kubeconfig}: the scaler credential is not mounted"
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
    """One read of a ``Scale``: what a version and a compare-and-swap need."""

    def __init__(self, uid: str, resource_version: str, replicas: int) -> None:
        self.uid = uid
        self.resource_version = resource_version
        self.replicas = replicas

    @property
    def version(self) -> str:
        return f"deployments/scale/{self.uid}/replicas={self.replicas}"


def _live(response: httpx.Response, name: str) -> Live:
    """Parse a ``Scale`` reply, or ``ToolError``.

    A stopped Deployment's ``Scale`` has ``"spec": {}``: zero replicas is
    omitted, not written (observed from a v1.35 API server), so an absent count
    is zero. A present count that is not a non-negative integer is refused.
    """

    try:
        body = response.json()
    except ValueError:
        raise ToolError(f"the Scale of {name} is not JSON") from None
    if not isinstance(body, dict) or body.get("kind") != "Scale":
        raise ToolError(f"the API server did not answer a Scale for {name}")
    metadata = body.get("metadata")
    spec = body.get("spec")
    if not isinstance(metadata, dict) or not isinstance(spec, dict):
        raise ToolError(f"the Scale of {name} has no metadata or spec")
    uid = metadata.get("uid")
    resource_version = metadata.get("resourceVersion")
    replicas = spec.get("replicas", 0)
    if not (
        isinstance(uid, str) and uid and isinstance(resource_version, str) and resource_version
    ):
        raise ToolError(f"the Scale of {name} has no uid or resourceVersion")
    if not isinstance(replicas, int) or isinstance(replicas, bool) or replicas < 0:
        raise ToolError(f"the Scale of {name} has no usable replica count")
    return Live(uid, resource_version, replicas)


def _refuse_status(response: httpx.Response, name: str, verb: str) -> None:
    status = response.status_code
    if status == 409:
        raise _Conflict()
    if status == 404:
        raise ToolError(f"no Deployment {name} in this connector's namespace")
    if status in (401, 403):
        raise ToolError(
            f"the scaler credential may not {verb} the scale of {name} ({status}); "
            "it needs that verb on deployments/scale with this name in resourceNames"
        )
    if status >= 400:
        raise ToolError(f"the API server refused to {verb} the scale of {name}: {status}")


class Scales:
    """The scale subresource of one namespace."""

    def __init__(self, client: httpx.Client, namespace: str) -> None:
        self.client = client
        self.namespace = namespace

    def __enter__(self) -> "Scales":
        return self

    def __exit__(self, *exc: object) -> None:
        self.client.close()

    def _path(self, name: str) -> str:
        return f"/apis/apps/v1/namespaces/{self.namespace}/deployments/{name}/scale"

    def read(self, name: str) -> Live:
        try:
            response = self.client.get(self._path(name))
        except httpx.HTTPError as exc:
            raise ToolError(f"could not reach the API server: {type(exc).__name__}") from None
        _refuse_status(response, name, "get")
        return _live(response, name)

    def write(self, name: str, read: Live, replicas: int) -> Live:
        """``PUT`` the count, conditional on the read's ``resourceVersion``."""

        body = {
            "apiVersion": "autoscaling/v1",
            "kind": "Scale",
            "metadata": {
                "name": name,
                "namespace": self.namespace,
                "resourceVersion": read.resource_version,
            },
            "spec": {"replicas": replicas},
        }
        try:
            response = self.client.put(self._path(name), json=body)
        except httpx.HTTPError as exc:
            raise ToolError(f"could not reach the API server: {type(exc).__name__}") from None
        _refuse_status(response, name, "update")
        written = _live(response, name)
        if written.uid != read.uid or written.replicas != replicas:
            raise ToolError(f"the API server did not report the requested count for {name}")
        return written

    def compare_and_set(self, name: str, read: Live, replicas: int, still: Any) -> Live:
        """Write ``replicas`` while ``still(live)`` holds, or raise ``_Conflict``.

        ``resourceVersion`` also moves when the Deployment controller writes
        status, which it does for a while after any change (observed), so a
        ``409`` alone does not mean someone moved the count. On a ``409`` this
        reads again and retries only while the uid and count are still the ones
        checked; a real change, or ``CONFLICT_ATTEMPTS`` conflicts, refuses.
        """

        current = read
        for _ in range(CONFLICT_ATTEMPTS):
            try:
                return self.write(name, current, replicas)
            except _Conflict:
                current = self.read(name)
                if not still(current):
                    raise
        raise _Conflict()


# --------------------------------------------------------------------------- #
# The server.
# --------------------------------------------------------------------------- #

# A write, gated for the model. Idempotent: the same count twice leaves the same
# state. Destructive: a count of zero stops the workload.
WRITE = ToolAnnotations(
    read_only_hint=False, destructive_hint=True, idempotent_hint=True, open_world_hint=False
)
RESTORE = ToolAnnotations(
    read_only_hint=False, destructive_hint=True, idempotent_hint=False, open_world_hint=False
)
READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, open_world_hint=False)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _refused(code: str) -> dict[str, Any]:
    return {"ok": False, "refused": code}


def build(settings: Settings) -> MCPServer:
    """The MCP server for one configuration."""

    mcp = MCPServer("scaler")
    keys = settings.keys

    def bounded(name: str) -> str:
        if not settings.namespace:
            raise ToolError("SCALER_NAMESPACE is not set: this connector refuses every call")
        if not settings.deployments:
            raise ToolError("SCALER_DEPLOYMENTS is empty: this connector refuses every call")
        if name not in settings.deployments:
            permitted = ", ".join(sorted(settings.deployments))
            raise ToolError(
                f"{name!r} is not in this connector's allowlist ({permitted}); "
                "widening it is an operator change"
            )
        return name

    def target_name(target: dict[str, Any]) -> str:
        if set(target) != _TARGET_KEYS or target.get("kind") != TARGET_KIND:
            raise ToolError("target must be exactly {kind: Deployment, namespace, name}")
        if target.get("namespace") != settings.namespace or not settings.namespace:
            raise ToolError("target is not in this connector's namespace")
        name = target.get("name")
        if not isinstance(name, str):
            raise ToolError("target name must be a string")
        return bounded(name)

    def scales() -> Scales:
        return Scales(_client(settings), settings.namespace)

    @mcp.tool(annotations=WRITE)
    def scale_deployment(name: StrictStr, replicas: StrictInt) -> dict[str, Any]:
        """Set the replica count of one allowlisted Deployment to exactly ``replicas``.

        Equivalent to ``kubectl scale deploy/<name> --replicas=<n>`` in this
        connector's namespace, through the ``scale`` subresource, so it cannot
        change an image, a command or anything else in the Deployment.

        This is a WRITE and it is gated: the call pauses for human approval.
        Say which Deployment, from what count to what, and why, before calling.

        The reply is a JSON object: ``ok``, ``summary``, ``prior`` (the count
        before the call, sealed so only the platform's undo can open it),
        ``version`` and ``target``. A count already in force changes nothing and
        replies ``ok`` with the same version. A refusal is ``{"ok": false,
        "refused": <code>}`` or a tool error, and writes nothing.
        """

        bounded(name)
        if replicas < 0 or replicas > settings.max_replicas:
            raise ToolError(
                f"replicas must be between 0 and this connector's ceiling of "
                f"{settings.max_replicas}; raising it is an operator change"
            )
        if keys.current is None:
            log.info("scale_deployment refused: %s", SEALING_KEY_UNAVAILABLE)
            return _refused(SEALING_KEY_UNAVAILABLE)
        target = {"kind": TARGET_KIND, "namespace": settings.namespace, "name": name}
        with scales() as client_scales:
            read = client_scales.read(name)
            prior = _seal(keys.current, target, read)
            if read.replicas == replicas:
                left = read
                log.info("scale_deployment: already at the requested count")
            else:
                try:
                    left = client_scales.compare_and_set(
                        name,
                        read,
                        replicas,
                        lambda live: live.uid == read.uid and live.replicas == read.replicas,
                    )
                except _Conflict:
                    log.info("scale_deployment refused: %s", VERSION_CONFLICT)
                    return _refused(VERSION_CONFLICT)
                log.info("scale_deployment wrote")
        return {
            "ok": True,
            "summary": f"scaled {settings.namespace}/{name} from {read.replicas} to {replicas}",
            "prior": prior,
            "version": left.version,
            "target": target,
        }

    @mcp.tool(annotations=READ_ONLY)
    def observe_version(target: dict[str, Any]) -> dict[str, Any]:
        """Report the live version of ``target``. @spec ACTION-EXECUTOR-15."""

        name = target_name(target)
        with scales() as client_scales:
            return {"version": client_scales.read(name).version}

    @mcp.tool(annotations=RESTORE)
    def restore(
        target: dict[str, Any],
        prior_state: dict[str, Any],
        expected_version: str | None = None,
    ) -> dict[str, Any]:
        """Write the sealed ``prior_state`` back to ``target``.

        @spec ACTION-EXECUTOR-15 @spec ACTION-EXECUTOR-16. Called only by the
        platform's executor. ``expected_version``, when given, must equal the
        live version, and the live Deployment must be the one sealed, or the
        call refuses ``version_conflict`` and writes nothing.
        """

        sealed = _envelope_ciphertext(prior_state)
        if sealed is None:
            log.info("restore refused: %s", SNAPSHOT_UNOPENABLE)
            return _refused(SNAPSHOT_UNOPENABLE)
        aead = keys.openers.get(prior_state["kid"])
        if aead is None:
            log.info("restore refused: %s", SEALING_KEY_UNAVAILABLE)
            return _refused(SEALING_KEY_UNAVAILABLE)
        try:
            opened = json.loads(
                aead.decrypt(sealed[:NONCE_BYTES], sealed[NONCE_BYTES:], _canonical(target))
            )
        except (InvalidTag, ValueError):
            opened = None
        replicas = opened.get("replicas") if isinstance(opened, dict) else None
        uid = opened.get("uid") if isinstance(opened, dict) else None
        if (
            not isinstance(replicas, int)
            or isinstance(replicas, bool)
            or replicas < 0
            or not isinstance(uid, str)
        ):
            log.info("restore refused: %s", SNAPSHOT_UNOPENABLE)
            return _refused(SNAPSHOT_UNOPENABLE)
        name = target_name(target)
        with scales() as client_scales:
            live = client_scales.read(name)

            def unchanged(current: Live) -> bool:
                return current.uid == uid and (
                    expected_version is None or current.version == expected_version
                )

            if not unchanged(live):
                log.info("restore refused: %s", VERSION_CONFLICT)
                return _refused(VERSION_CONFLICT)
            try:
                left = (
                    live
                    if live.replicas == replicas
                    else client_scales.compare_and_set(
                        name,
                        live,
                        replicas,
                        lambda current: unchanged(current) and current.replicas == live.replicas,
                    )
                )
            except _Conflict:
                log.info("restore refused: %s", VERSION_CONFLICT)
                return _refused(VERSION_CONFLICT)
        log.info("restore wrote")
        return {"ok": True, "version": left.version, "target": target}

    return mcp


def _seal(current: tuple[str, AESGCM], target: dict[str, Any], read: Live) -> dict[str, str]:
    kid, aead = current
    nonce = os.urandom(NONCE_BYTES)
    body = aead.encrypt(
        nonce, _canonical({"replicas": read.replicas, "uid": read.uid}), _canonical(target)
    )
    return {
        "sealed": SEALED_CONSTANT,
        "kid": kid,
        "ciphertext": base64.b64encode(nonce + body).decode("ascii"),
    }


def main() -> int:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), stream=sys.stderr)
    try:
        settings = Settings(dict(os.environ))
    except ConfigError as exc:
        log.error("refusing to start: %s", exc)
        return 1
    log.info(
        "scaler: namespace %s, %d allowlisted deployment(s), ceiling %d, sealing key %s, "
        "%d opening key(s)",
        "set" if settings.namespace else "unset",
        len(settings.deployments),
        settings.max_replicas,
        "held" if settings.keys.current is not None else "absent",
        len(settings.keys.openers),
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
