"""A write connector that can do exactly one thing: set a Deployment's replicas.

Why a second server rather than a second tool on `k8s-write`
------------------------------------------------------------
`k8s-write`'s own docstring is the reason: "The tool takes `namespace` and
`name`. Nothing else. There is no parameter through which a caller could reach an
image, an env var, a command, or a replica count." Adding a scale tool there
would put caller input into a process whose entire argument is that no caller
input reaches the patch body. The same file also says "This server exposes ONE
tool. There is no second thing for a gate to miss." Both hold here, so scale gets
its own server and keeps the one-tool posture.

The constraint that actually matters, and why this one is NARROWER than restart
------------------------------------------------------------------------------
A rollout restart is a PATCH of the Deployment's pod template, so RBAC `patch` on
`deployments` is the same grant as `set image` and replacing the container
command. There is no RBAC expression that separates them, which is why
`k8s-write` has to enforce the separation in Python.

Scaling does not have that problem. Kubernetes exposes `deployments/scale` as its
own subresource, so an operator can grant `patch` on `deployments/scale` and
nothing else. That grant cannot change an image, a command, or an env var --
Kubernetes itself refuses, not this file. `replicas` is a caller parameter here
because it is the verb's argument rather than a channel into an arbitrary patch,
and the subresource is what makes that safe.

REVERSIBLE, which is the other reason this exists
-------------------------------------------------
Unlike a restart, a scale can be put back: the replica count that was in force
immediately before the patch is enough to restore it. This tool reads that count
on the way past and returns it in a structured reply, so the platform can record
what to restore without asking a model what happened. A restart cannot do this,
and says so.

The reply is JSON rather than prose. A tool that reports state a machine will act
on has to be parseable, and the model reads JSON perfectly well.
"""

# NOTE: no `from __future__ import annotations`. FastMCP introspects tool
# signatures with `issubclass(param.annotation, Context)`, and stringized
# annotations make that raise `TypeError` at import time.

import base64
import json
import logging
import os
import ssl
from typing import Any

import httpx
import yaml
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

log = logging.getLogger("k8s-scale-mcp")

KUBECONFIG = os.environ.get("KUBECONFIG_PATH", "/secrets/kubeconfig")
TIMEOUT = float(os.environ.get("K8S_TIMEOUT_SECONDS", "30"))

# "namespace/name,namespace/name". Empty means nothing is permitted.
ALLOWLIST = frozenset(
    entry.strip()
    for entry in os.environ.get("K8S_SCALE_ALLOWLIST", "").split(",")
    if entry.strip()
)

# A ceiling an operator sets, because "scale to 10000" is a denial-of-service
# with an approval on it. Refused before a client is built, like the allowlist.
MAX_REPLICAS = int(os.environ.get("K8S_SCALE_MAX_REPLICAS", "50"))

mcp = FastMCP(
    "k8s-scale",
    host=os.environ.get("BIND_ADDRESS", "0.0.0.0"),
    port=int(os.environ.get("PORT", "8000")),
    streamable_http_path="/",
)

# idempotentHint=True is the honest value and the difference from restart:
# setting replicas to 3 twice leaves the same cluster state. It is still a write
# and still gated; it just does not compound the way a rollout does.
SCALE = ToolAnnotations(
    readOnlyHint=False,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=True,
)


def _reply(ok, summary, prior=None, target=None):
    """Every return path is this shape, so a caller never has to guess."""
    return json.dumps(
        {"ok": ok, "summary": summary, "prior": prior, "target": target},
        sort_keys=True,
    )


def _client() -> Any:
    """Build an httpx client from the mounted kubeconfig, or return a string.

    The kubeconfig is a static one built for this identity (the bundle README's
    write-path section has the assembly steps).
    Parsed here rather than pulling in the full Kubernetes client, so the
    failure modes are ones this file can explain.
    """

    try:
        with open(KUBECONFIG, encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh)
    except FileNotFoundError:
        return f"no kubeconfig at {KUBECONFIG}: the write credential is not mounted"
    except (OSError, yaml.YAMLError) as exc:
        return f"could not read the kubeconfig at {KUBECONFIG}: {exc}"

    try:
        cluster = cfg["clusters"][0]["cluster"]
        user = cfg["users"][0]["user"]
        server = cluster["server"]
        token = user["token"]
    except (KeyError, IndexError, TypeError):
        return "kubeconfig is missing a cluster server or a user token"

    # Both CA shapes, because a kubeconfig may carry either and getting this
    # wrong fails as CERTIFICATE_VERIFY_FAILED -- which reads like a cluster
    # problem rather than a parsing one. An `aws eks update-kubeconfig` config
    # and every hand-assembled one here use the INLINE `-data` form; only a
    # file-path config uses the other. Handling just the path form (the first
    # version of this file) meant `verify` silently stayed True, httpx checked
    # the EKS certificate against the system trust store, and every write failed
    # after the approval had already been given.
    verify: Any = True
    ca_data = cluster.get("certificate-authority-data")
    ca_path = cluster.get("certificate-authority")
    if ca_data:
        # IN MEMORY, not a temp file. Curie hardens every connector with a
        # read-only root filesystem and mounts no writable scratch here, so
        # `tempfile` raises "no usable temporary directory" -- which surfaced as
        # a tool-side error AFTER a human had approved the write, twice. An
        # SSLContext takes the PEM directly and touches no filesystem.
        try:
            pem = base64.b64decode(ca_data).decode("ascii")
        except (ValueError, TypeError, UnicodeDecodeError) as exc:
            return f"kubeconfig certificate-authority-data is not a valid base64 PEM: {exc}"
        ctx = ssl.create_default_context()
        try:
            ctx.load_verify_locations(cadata=pem)
        except ssl.SSLError as exc:
            return f"kubeconfig certificate-authority-data is not a usable CA certificate: {exc}"
        verify = ctx
    elif ca_path:
        verify = ca_path
    elif cluster.get("insecure-skip-tls-verify"):
        # Never silently. A write path that skips verification is a write path
        # that can be pointed at an impostor API server.
        return (
            "kubeconfig sets insecure-skip-tls-verify; refusing to write over an "
            "unverified connection"
        )

    return httpx.Client(
        base_url=server,
        headers={"Authorization": f"Bearer {token}"},
        verify=verify,
        timeout=TIMEOUT,
    )

@mcp.tool(annotations=SCALE)
def scale_deployment(namespace: str, name: str, replicas: int) -> str:
    """Set a Deployment's replica count.

    Equivalent to `kubectl -n <namespace> scale deploy/<name> --replicas=<n>`,
    performed against the `scale` subresource so the credential this runs with
    cannot touch the pod template.

    This is a WRITE and it is gated -- the call pauses the turn for human
    approval before anything happens. Say what you are about to scale, from what
    to what, and why, before calling it.

    REVERSIBLE. The reply carries `prior`, the replica count read immediately
    before the patch. Scaling back to that number restores what was there.

    The reply is a JSON object: `ok`, `summary`, `prior`, `target`. A refusal is
    the same shape with `ok` false and `prior` null, so a failed read never looks
    like a snapshot.
    """

    namespace = (namespace or "").strip()
    name = (name or "").strip()
    if not namespace or not name:
        return _reply(False, "both namespace and name are required")
    if isinstance(replicas, bool) or not isinstance(replicas, int):
        return _reply(False, "replicas must be an integer")
    if replicas < 0:
        return _reply(False, "replicas must not be negative")
    if replicas > MAX_REPLICAS:
        return _reply(
            False,
            f"refusing: {replicas} exceeds this connector's ceiling of "
            f"{MAX_REPLICAS}. Raising it is an operator change.",
        )

    target = f"{namespace}/{name}"
    if target not in ALLOWLIST:
        permitted = ", ".join(sorted(ALLOWLIST)) or "(none configured)"
        return _reply(
            False,
            f"refusing: {target} is not in this connector's allowlist. "
            f"Permitted: {permitted}. This is a deliberate ceiling -- widening it "
            "is an operator change, not something to work around.",
        )

    client = _client()
    if isinstance(client, str):
        return _reply(False, client)

    # The scale subresource, not the Deployment. This is the narrow grant.
    path = f"/apis/apps/v1/namespaces/{namespace}/deployments/{name}/scale"
    try:
        with client:
            existing = client.get(path)
            if existing.status_code == 404:
                return _reply(
                    False,
                    f"no Deployment {target}. Check the name with the read-only tools.",
                )
            if existing.status_code in (401, 403):
                return _reply(
                    False,
                    f"the write identity may not read the scale of {target} "
                    f"({existing.status_code}). It needs get on deployments/scale.",
                )
            if existing.status_code >= 400:
                return _reply(
                    False, f"could not read the scale of {target}: {existing.status_code}"
                )
            try:
                prior_replicas = (existing.json().get("spec") or {}).get("replicas")
            except (ValueError, AttributeError):
                prior_replicas = None
            if not isinstance(prior_replicas, int):
                # No trustworthy prior state means no snapshot. Refusing here
                # rather than writing anyway keeps "the action happened" and
                # "we know how to undo it" from drifting apart.
                return _reply(
                    False,
                    f"could not read a replica count for {target}; refusing to "
                    "scale without a prior state to restore",
                )

            patched = client.patch(path, {"spec": {"replicas": replicas}})
            if patched.status_code in (401, 403):
                return _reply(
                    False,
                    f"the write identity may not scale {target} "
                    f"({patched.status_code}). It needs patch on deployments/scale.",
                )
            if patched.status_code >= 400:
                return _reply(False, f"scale refused for {target}: {patched.status_code}")
    except httpx.HTTPError as exc:
        return _reply(False, f"could not reach the API server for {target}: {exc}")

    return _reply(
        True,
        f"scaled {target} from {prior_replicas} to {replicas}",
        prior={"spec": {"replicas": prior_replicas}},
        target={"namespace": namespace, "name": name},
    )


def main() -> None:
    if not ALLOWLIST:
        log.warning("K8S_SCALE_ALLOWLIST is empty; every call will be refused")
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
