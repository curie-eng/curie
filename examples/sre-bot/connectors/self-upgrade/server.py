"""Two gated upgrade buttons and one read. None of them takes an argument.

`upgrade_self` redeploys this bot's own bundle. `upgrade_platform` moves the
Curie release underneath it. `latest_release` says what version is available.

The two writes are the same mechanism pointed at different operator-written
CronJob templates, which is why they share `_start_job_from` rather than being
two files kept in step.

Why the bot cannot just do the upgrade itself
---------------------------------------------
Creating an agent version needs the platform API key, and every `/agents/**`
route requires it. The sandbox holds a per-turn `state`-scoped token and nothing
else, deliberately: the whole point of that shape is that a successful prompt
injection -- a crafted Grafana alert body, a poisoned pod log -- can only call
tools the connector already allows, and can never walk away with a credential.

Handing the sandbox the platform key to make "upgrade yourself" work would trade
that property away for a convenience. So the work stays where it already was, in
`self-upgrade/redeploy.py` running as a Job with the key mounted, and this
connector only ever presses the button. The bot's authority is unchanged: it can
start that one Job and learn its name. It cannot say what the Job runs.

Why a separate server, and why zero arguments
----------------------------------------------
There is no parameter through which a caller could reach an image, an
environment variable, or a command. The repository, the branch, the agent
name, the image and the command all come from the CronJob's own `jobTemplate`,
read from the cluster at call time. There is no field a caller can influence, so
there is nothing to validate and nothing to escape.

There are three tools here now, and the "nothing for a gate to miss" argument
holds differently than it did with one. Both writes are gated, and both are
zero-argument buttons that copy a template verbatim -- so the property that
matters is not the COUNT of tools but that no tool takes caller input. A gate
cannot be missed here because there is no unclassified verb: two writes, two
gates, one read that changes nothing and holds no credential.

What would break it is a tool growing a parameter. That is the line, and it is
worth restating because the obvious next request -- "let me name the version" --
is exactly that.

It lives here rather than in its own connector because it answers the same
question from the other side. "What version is available" is what a person asks
immediately before "can we upgrade", and splitting them across two images, two
publish jobs and two declarations buys separation between a read and a write
that share a repository, a domain, and no credential at all.

The grant RBAC cannot narrow, and what stands in for it
--------------------------------------------------------
`create` on `jobs` is namespace-wide -- Kubernetes has no `resourceNames` for a
resource that does not exist yet, so RBAC cannot say "only this Job". The
purpose-built ceiling is enforced here, in Python, by never letting a caller
supply a body. The Job posted is the CronJob's
template verbatim, and the CronJob is named by operator configuration
(`SELF_UPGRADE_CRONJOB`), not by the model.

Read that grant honestly when reviewing this: a leaked credential from this pod
could create an arbitrary Job in this namespace. It is bounded by the same thing
every other connector's credential is bounded by -- it never leaves the pod, and
no tool takes caller input.

NOT REVERSIBLE, and it says so
-------------------------------
Deploying a new agent version cannot be undone by this tool. `prior` is null and
stays null, so the platform never records a snapshot it cannot act on
(ADR-0117). What undoes it is deploying the previous version, which is an
operator action with the platform key -- named in the reply rather than implied,
so nobody reads "ok" as "safely reversible".

One at a time
--------------
`concurrencyPolicy: Forbid` only governs Jobs the CronJob controller creates; it
says nothing about one created here. Two overlapping runs would race on creating
the agent version, and the loser leaves a version row behind. So this refuses
while a Job for the same CronJob is still active.
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
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

log = logging.getLogger("self-upgrade-mcp")

KUBECONFIG = os.environ.get("KUBECONFIG_PATH", "/secrets/kubeconfig")
TIMEOUT = float(os.environ.get("K8S_TIMEOUT_SECONDS", "30"))

# The CronJob whose template this starts. Empty means every call is refused --
# a missing config fails closed, like the other connectors' allowlists.
CRONJOB = os.environ.get("SELF_UPGRADE_CRONJOB", "").strip()

# The namespace it lives in. The connector runs in the release's namespace, so
# this defaults to the one the ServiceAccount is bound to when set.
NAMESPACE = os.environ.get("SELF_UPGRADE_NAMESPACE", "").strip()

# The CronJob that upgrades the PLATFORM, as distinct from this bot's bundle.
# Empty means `upgrade_platform` refuses -- an install that has not decided to
# grant a platform upgrade should not get one by default.
PLATFORM_CRONJOB = os.environ.get("PLATFORM_UPGRADE_CRONJOB", "").strip()

# The repository whose published releases answer "what is the newest version".
# Read-only and public: no credential, and none is added here on purpose -- see
# `latest_release`. Empty disables that tool's answer rather than guessing a repo.
RELEASE_REPO = os.environ.get("SELF_UPGRADE_RELEASE_REPO", "").strip()
RELEASE_API = os.environ.get("SELF_UPGRADE_RELEASE_API", "https://api.github.com").rstrip("/")
RELEASE_TIMEOUT = float(os.environ.get("SELF_UPGRADE_RELEASE_TIMEOUT_SECONDS", "15"))

mcp = MCPServer("self-upgrade")

# destructiveHint=True: it replaces the running bot with a different build of
# itself. idempotentHint=False: calling it twice starts two upgrades, which is
# exactly what the active-Job check below exists to prevent.
# A read. Its own annotation rather than reusing UPGRADE, because a client that
# filters on readOnlyHint must be able to tell these two apart.
READ = ToolAnnotations(
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=True,
)

UPGRADE = ToolAnnotations(
    readOnlyHint=False,
    destructiveHint=True,
    idempotentHint=False,
    openWorldHint=True,
)


def _reply(ok, summary, prior=None, post=None, target=None):
    """Return the successful Job-start result as structured JSON.

    Matches the scale connector's reply so a reader of either learns one shape.
    `prior` is null because this action has no recorded prior state to restore.
    Refusals raise `ToolError` and carry `isError: true` on the MCP wire.
    """
    return json.dumps(
        {"ok": ok, "summary": summary, "prior": prior, "post": post, "target": target},
        sort_keys=True,
    )


def _client() -> httpx.Client:
    """Build an httpx client from the mounted kubeconfig, or raise `ToolError`.

    Parsed here rather than pulling in the full Kubernetes client, so the failure
    modes are ones this file can explain. Kept deliberately identical to the
    scale connector's: the CA handling below is not boilerplate, it is the fix
    for two separate live failures, and diverging it here would reintroduce them.
    """

    try:
        with open(KUBECONFIG, encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh)
    except FileNotFoundError:
        raise ToolError(
            f"no kubeconfig at {KUBECONFIG}: the upgrade credential is not mounted"
        ) from None
    except (OSError, yaml.YAMLError) as exc:
        raise ToolError(f"could not read the kubeconfig at {KUBECONFIG}: {exc}") from exc

    try:
        cluster = cfg["clusters"][0]["cluster"]
        user = cfg["users"][0]["user"]
        server = cluster["server"]
        token = user["token"]
    except (KeyError, IndexError, TypeError):
        raise ToolError("kubeconfig is missing a cluster server or a user token") from None

    # Both CA shapes. Handling only the file-path form left `verify` silently
    # True, so httpx checked the cluster certificate against the system trust
    # store and every call failed after approval had already been given.
    verify: Any = True
    ca_data = cluster.get("certificate-authority-data")
    ca_path = cluster.get("certificate-authority")
    if ca_data:
        # IN MEMORY, not a temp file: the connector's root filesystem is
        # read-only and no writable scratch is mounted, so `tempfile` raises
        # "no usable temporary directory".
        try:
            pem = base64.b64decode(ca_data).decode("ascii")
        except (ValueError, TypeError, UnicodeDecodeError) as exc:
            raise ToolError(
                f"kubeconfig certificate-authority-data is not a valid base64 PEM: {exc}"
            ) from exc
        ctx = ssl.create_default_context()
        try:
            ctx.load_verify_locations(cadata=pem)
        except ssl.SSLError as exc:
            raise ToolError(
                f"kubeconfig certificate-authority-data is not a usable CA certificate: {exc}"
            ) from exc
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
        timeout=TIMEOUT,
    )


def _namespace() -> str:
    """The namespace to act in: configured, else the ServiceAccount's own."""

    if NAMESPACE:
        return NAMESPACE
    try:
        with open(
            "/var/run/secrets/kubernetes.io/serviceaccount/namespace", encoding="utf-8"
        ) as fh:
            return fh.read().strip()
    except OSError:
        return ""


def _active_job(client, namespace: str, cronjob: str) -> str | None:
    """The name of a still-running Job for `cronjob`, if there is one.

    Scoped to the named CronJob, not to this connector: the two upgrade verbs run
    different templates and must not block one another. A platform upgrade and a
    bundle redeploy are independent, and treating either as "an upgrade is
    running" would refuse a call that is safe.

    Returns None when nothing is active. A listing failure also returns None:
    refusing to upgrade because a *status* read failed would be a worse outcome
    than the race it guards, and the race is narrow.
    """

    try:
        listed = client.get(
            f"/apis/batch/v1/namespaces/{namespace}/jobs",
            params={"labelSelector": f"curie.dev/self-upgrade-of={cronjob}"},
        )
    except httpx.HTTPError:
        return None
    if listed.status_code >= 400:
        return None
    try:
        items = listed.json().get("items") or []
    except (ValueError, AttributeError):
        return None
    for job in items:
        status = job.get("status") or {}
        if status.get("active"):
            return (job.get("metadata") or {}).get("name")
    return None


@mcp.tool(annotations=UPGRADE)
def upgrade_self() -> str:
    """Deploy the newest version of this bot's own bundle from its repository.

    Starts the upgrade Job an operator installed alongside this bot. That Job
    reads the bundle from the repository, pins each connector to the image built
    from the same commit, creates a new agent version and deploys it. This tool
    takes no arguments: what runs is fixed by the Job's template, not by you.

    This is a WRITE and it is gated -- the call pauses the turn for human
    approval before anything happens. Before calling it, say plainly that you are
    about to replace your own running version, and from where.

    NOT REVERSIBLE by you. There is no "undo upgrade" tool; putting the previous
    version back is an operator action with the platform API key. Say so rather
    than implying a rollback you cannot perform.

    Starting the Job is not the same as finishing it. The reply carries the Job's
    name; watch it with the read-only Kubernetes tools (`resources_get` on the
    Job, `pods_log` on its pod) and report what actually happened. Your own
    process is replaced when the deploy lands, so the last thing you observe may
    be your own restart -- that is the upgrade working, not a failure.

    The reply is a JSON object: `ok`, `summary`, `prior`, `post`, `target`.
    `prior` is always null, because this action has no prior state to restore.
    Refusals are tool errors rather than successful JSON replies.
    """

    return _start_job_from(CRONJOB, "SELF_UPGRADE_CRONJOB")


@mcp.tool(annotations=UPGRADE)
def upgrade_platform() -> str:
    """Move the Curie platform this bot runs on to the newest published release.

    THIS IS NOT `upgrade_self`. That one redeploys this bot's own bundle and
    leaves the platform alone. This one upgrades the platform underneath every
    agent on it, including you -- api, worker, dispatcher and the rest. Do not
    call it when someone asks you to update yourself.

    Starts an upgrade Job an operator installed. That Job reads the newest
    published release itself, fetches its chart, and runs the Helm upgrade with
    the values this install is already using. This tool takes no arguments: you
    cannot choose the version, the chart or the values. If someone names a
    version, say that what runs is "the newest published release" and let them
    decide whether that is what they wanted.

    This is a WRITE, it is gated, and it is the widest one you have. Before
    calling it, say plainly which version is installed now, which is newest, and
    that every platform component restarts.

    NOT REVERSIBLE by you, and not reliably reversible at all. `helm rollback`
    restores objects but not the database: migrations run as an init container
    and rollback does not undo them, so for a version pair that migrated,
    recovery is restore-from-backup by an operator. Never describe this as
    something that can be undone.

    Starting the Job is not finishing it. The reply carries the Job's name; watch
    it with the read-only Kubernetes tools and report what it actually did. Your
    own sandbox may be replaced while it runs -- that is the upgrade working.

    Refusals are tool errors rather than successful JSON replies.
    """

    return _start_job_from(PLATFORM_CRONJOB, "PLATFORM_UPGRADE_CRONJOB")


@mcp.tool(annotations=READ)
def latest_release() -> str:
    """The newest published release of the platform this bot runs on.

    Answers "what is the latest available version" -- the question a person asks
    right before "can we upgrade". It reads the repository's published releases
    and reports the newest tag; it changes nothing and needs no approval.

    THIS IS NOT WHAT THIS BOT IS RUNNING. The installed platform version is a
    property of the cluster, readable from `app.kubernetes.io/version` on the
    platform's own objects with the read-only Kubernetes tools. Report both and
    say which is which; a newer tag here does not mean anything was upgraded.

    Nor is it your own bundle's version, which moves independently.

    The reply is a JSON object with `tag`, `name`, `url` and `published_at`.
    Refusals are tool errors rather than successful JSON replies.
    """

    if not RELEASE_REPO:
        raise ToolError(
            "refusing: SELF_UPGRADE_RELEASE_REPO is not set, so this connector "
            "does not know which project's releases to read. Setting it is an "
            "operator change."
        )

    url = f"{RELEASE_API}/repos/{RELEASE_REPO}/releases/latest"
    try:
        # No credential, deliberately. A public repository needs none, and a
        # token here would put a credential in a connector whose whole job is to
        # answer one read -- the thing the read-only Kubernetes connector drops
        # `configuration_view` to avoid. The cost is GitHub's unauthenticated
        # rate limit, which a question asked a few times a day does not reach.
        with httpx.Client(timeout=RELEASE_TIMEOUT) as client:
            response = client.get(
                url,
                headers={
                    "Accept": "application/vnd.github+json",
                    "User-Agent": "curie-sre-bot-self-upgrade",
                },
            )
    except httpx.HTTPError as exc:
        raise ToolError(f"could not reach {RELEASE_API}: {exc}") from exc

    if response.status_code == 404:
        raise ToolError(
            f"{RELEASE_REPO} has no published releases, or is not visible without "
            "a credential this connector deliberately does not hold."
        )
    if response.status_code == 403:
        raise ToolError(
            "the releases API refused this read (403). Unauthenticated rate "
            "limits are the usual cause; it clears on its own."
        )
    if response.status_code >= 400:
        raise ToolError(f"the releases API returned HTTP {response.status_code}")

    try:
        payload = response.json()
        tag = payload["tag_name"]
    except (ValueError, KeyError, TypeError) as exc:
        raise ToolError(
            "the releases API returned a body with no tag_name"
        ) from exc

    return json.dumps(
        {
            "summary": (
                f"the newest published release of {RELEASE_REPO} is {tag}. This is "
                "what is available, NOT what this install is running -- read the "
                "installed version from the platform's own Kubernetes objects."
            ),
            "tag": tag,
            "name": payload.get("name"),
            "url": payload.get("html_url"),
            "published_at": payload.get("published_at"),
        },
        sort_keys=True,
    )


def _start_job_from(cronjob: str, env_name: str) -> str:
    """Create a Job from `cronjob`'s template, or raise saying why not.

    Shared by both upgrade verbs because they differ in exactly one thing: which
    operator-written template runs. Everything the security argument rests on --
    no caller input, the template copied verbatim, one at a time -- is a property
    of this function, so it holds for both by construction rather than by two
    call sites being kept in step.
    """

    if not cronjob:
        raise ToolError(
            f"refusing: {env_name} is not set, so this connector does not "
            "know which upgrade to start. Setting it is an operator change."
        )

    namespace = _namespace()
    if not namespace:
        raise ToolError(
            "refusing: could not determine a namespace to act in. Set SELF_UPGRADE_NAMESPACE."
        )

    client = _client()

    with client:
        path = f"/apis/batch/v1/namespaces/{namespace}/cronjobs/{cronjob}"
        try:
            existing = client.get(path)
        except httpx.HTTPError as exc:
            raise ToolError(f"could not reach the API server: {exc}") from exc
        if existing.status_code == 404:
            raise ToolError(
                f"no CronJob {namespace}/{cronjob}. The upgrade job is not installed "
                "on this cluster; installing it is an operator change."
            )
        if existing.status_code in (401, 403):
            raise ToolError(
                f"the upgrade identity may not read {namespace}/{cronjob} "
                f"({existing.status_code}). It needs get on that cronjob."
            )
        if existing.status_code >= 400:
            raise ToolError(
                f"could not read {namespace}/{cronjob}: {existing.status_code}"
            )

        try:
            template = (existing.json().get("spec") or {}).get("jobTemplate") or {}
        except (ValueError, AttributeError):
            template = {}
        job_spec = template.get("spec")
        if not job_spec:
            raise ToolError(
                f"{namespace}/{cronjob} has no jobTemplate.spec to run; the CronJob "
                "is malformed."
            )

        running = _active_job(client, namespace, cronjob)
        if running:
            raise ToolError(
                f"refusing: upgrade job {running} is still running. Wait for it to "
                "finish rather than starting a second one -- two overlapping runs "
                "race on creating the version."
            )

        # generateName, so the server picks a unique suffix and two approvals
        # landing together cannot collide on a name. The label is this
        # connector's own, and is what `_active_job` counts: it must not depend
        # on the CronJob controller's `job-name`/`controller-uid` labels, which a
        # manually created Job does not carry.
        body = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "generateName": f"{cronjob}-",
                "namespace": namespace,
                "labels": {
                    **((template.get("metadata") or {}).get("labels") or {}),
                    "curie.dev/self-upgrade-of": cronjob,
                },
                "annotations": {
                    **((template.get("metadata") or {}).get("annotations") or {}),
                    # What kubectl stamps on `create job --from=cronjob/...`,
                    # kept so an operator reading the Job sees the same marker
                    # from either route.
                    "cronjob.kubernetes.io/instantiate": "manual",
                },
            },
            # VERBATIM. This is the line the security argument rests on: the
            # spec is whatever the operator put in the CronJob, never anything
            # assembled from a caller's input.
            "spec": job_spec,
        }

        try:
            created = client.post(f"/apis/batch/v1/namespaces/{namespace}/jobs", json=body)
        except httpx.HTTPError as exc:
            raise ToolError(f"could not reach the API server: {exc}") from exc
        if created.status_code in (401, 403):
            raise ToolError(
                f"the upgrade identity may not create Jobs in {namespace} "
                f"({created.status_code}). It needs create on jobs."
            )
        if created.status_code >= 400:
            raise ToolError(f"the upgrade job was refused: {created.status_code}")
        try:
            name = (created.json().get("metadata") or {}).get("name") or "(unnamed)"
        except (ValueError, AttributeError):
            name = "(unnamed)"

    return _reply(
        True,
        f"started upgrade job {namespace}/{name}. It is running now; this does not "
        "yet mean the upgrade succeeded. Watch the Job and report what it did. "
        "There is no undo for this: restoring the previous version needs an "
        "operator with the platform API key.",
        # Null on the success path too. Stated rather than omitted so a reader
        # sees the absence is deliberate.
        prior=None,
        post={"job": name},
        target={"kind": "Job", "namespace": namespace, "name": name},
    )


def main() -> None:
    if not CRONJOB:
        log.warning("SELF_UPGRADE_CRONJOB is empty; every call will be refused")
    mcp.run(
        transport="streamable-http",
        host=os.environ.get("BIND_ADDRESS", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
        streamable_http_path="/mcp",
    )


if __name__ == "__main__":
    main()
