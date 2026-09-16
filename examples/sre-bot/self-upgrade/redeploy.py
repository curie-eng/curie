#!/usr/bin/env python3
"""Redeploy this bundle when the repository it lives in has moved.

What this is for
----------------
"The bot upgrades its own version" needs three separate things, and only one of
them is a capability the bot could ever hold:

1. noticing that a newer bundle exists,
2. getting that bundle's bytes,
3. calling the platform to make it the in-force version.

The sandbox can do none of them: it holds a per-turn ``state``-scoped token, and
every ``/agents/**`` route needs the platform key. So the deploy is performed by
something OUTSIDE the sandbox -- this job -- and the bot's own authority is
unchanged. That is the same division Draft ADR-0125 draws for the control plane:
an agent may render and propose; execution sits where a sandbox cannot reach.

Why this exists next to git-flow rather than instead of it
---------------------------------------------------------
Curie already deploys a new version when a repository's branch tip moves
(``commitpoller``). It packages the repository ROOT as the bundle, so it serves a
repo whose root *is* the bundle and cannot serve a bundle living in a
subdirectory of a monorepo.

This bundle lives at ``examples/sre-bot`` inside curie, which is where it should
stay -- it ships as the worked example, and it is reviewed with the platform
changes it exercises. So the choice is not "git-flow or nothing": it is whether
the bundle moves to its own repository, or whether the subdirectory case gets
handled. This handles it, and deliberately does the same three steps git-flow
does rather than inventing a second concept.

**It is not a way around any gate.** It reads a subdirectory of a repository an
operator named and calls two documented endpoints with the platform key an
operator gave it. Nothing here decides what the bot may do; the bundle's own
``approvalPolicy`` and its connectors' ceilings are unchanged by a redeploy.

What it does NOT do
-------------------
**It does not deploy yet, and that is deliberate rather than unfinished.** The
bundle as it sits in the repository declares ``build:`` for three connectors, and
a cluster deploy is refused for an image that exists only in one machine's Docker
daemon. Uploading the repository copy verbatim would create a version whose
connectors cannot come up -- a redeploy that reports success and leaves the bot
with no tools, which is worse than not redeploying. The missing piece is digest
resolution, the same step ``curie example sre-bot install`` already performs for
tempo; it needs the write connectors published first (curie#1945).

So this build does the two steps that are correct on their own -- notice, and say
so loudly enough to act on -- and stops before the one that would lie.


It does not upgrade Curie. A new *platform* version is not something an agent can
apply to itself: the upgrade restarts the worker the agent's turn is running in,
so the turn that performed it cannot report the outcome or roll it back. Platform
upgrades stay an operator action, and this job's only relationship to one is that
it will redeploy the bundle afterwards.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import tarfile
import urllib.error
import urllib.request

# The bundle's path inside the repository. The whole reason this file exists.
BUNDLE_PREFIX = "examples/sre-bot"

# A bundle is these files and nothing else. Everything under the prefix that is
# not one of these is left out, so a stray editor file or a test fixture cannot
# ride into a deployed version.
BUNDLE_MEMBERS = (
    ".claude-plugin/plugin.json",
    "connectors.yaml",
    "deploy.yaml",
    "evals/cases.json",
)
BUNDLE_TREES = ("skills/", "manifests/")

# deploy() always posts this environment. deployed_commit must read the same
# one: git-flow leaves a previous environment's active row in place, so the
# newest active deployment across environments is not what prod is serving.
DEPLOY_ENVIRONMENT = "prod"


class SelfUpgradeError(RuntimeError):
    """Something the operator has to fix, phrased for the operator."""


def _get(url: str, token: str | None, accept: str) -> bytes:
    headers = {"Accept": accept, "User-Agent": "curie-sre-bot-self-upgrade"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body: bytes = response.read()
        return body
    except urllib.error.HTTPError as exc:
        # 404 on a private repository with no token is really 401, and reads as
        # "no such repo" to anyone who has not hit it before. Say the likely
        # cause rather than the status.
        if exc.code in (401, 403, 404) and not token:
            raise SelfUpgradeError(
                f"{url} returned HTTP {exc.code} with no token. A private "
                f"repository needs a read-only token in GITHUB_TOKEN; without one "
                f"this job cannot see whether a newer bundle exists."
            ) from exc
        raise SelfUpgradeError(f"{url} returned HTTP {exc.code}") from exc


def latest_commit(repo: str, branch: str, token: str | None) -> str:
    """The sha at the tip of ``branch``, which is what "newer" means here."""

    body = _get(
        f"https://api.github.com/repos/{repo}/commits/{branch}",
        token,
        "application/vnd.github+json",
    )
    sha = json.loads(body).get("sha")
    if not isinstance(sha, str) or not sha:
        raise SelfUpgradeError(f"{repo}@{branch} returned no commit sha")
    return sha


def _api(
    api_url: str,
    api_key: str,
    path: str,
    *,
    method: str = "GET",
    body: bytes | None = None,
    content_type: str = "application/json",
) -> bytes:
    """One call to the platform API, with the platform key.

    The key lives here, in a job outside the sandbox, and never inside it: an
    agent that could deploy its own version could also deploy one without its own
    approval gates.
    """

    headers = {"X-API-Key": api_key}
    if body is not None:
        headers["Content-Type"] = content_type
    request = urllib.request.Request(
        f"{api_url.rstrip('/')}{path}", data=body, method=method, headers=headers
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            payload: bytes = response.read()
        return payload
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")[:400]
        raise SelfUpgradeError(f"{method} {path} returned HTTP {exc.code}: {detail}") from exc


def deployed_commit(api_url: str, api_key: str, agent: str) -> tuple[str, str | None, str | None]:
    """``(agent_id, commit_sha, version_id)`` for the version this agent is SERVING.

    The active deployment in ``DEPLOY_ENVIRONMENT``, not the newest version row
    and not the newest active row across environments. Those are different
    facts: git-flow never stops the previous environment's row, so an old
    active prod deployment and a newer active dev deployment coexist, and
    taking the newest ``deployed_at`` reports the dev commit as what prod is
    serving.

    ``None`` for the sha when the deployed version records none: a version
    deployed by hand from a working copy has no commit, and that must read as
    "unknown", never as "up to date".
    """

    def get(path: str) -> list[dict[str, object]]:
        request = urllib.request.Request(
            f"{api_url.rstrip('/')}{path}", headers={"X-API-Key": api_key}
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            payload: list[dict[str, object]] = json.load(response)
        return payload

    agents = get("/agents")
    match = next((a for a in agents if a.get("name") == agent), None)
    if match is None:
        raise SelfUpgradeError(
            f"no agent named {agent!r} on {api_url}; this job redeploys an agent "
            f"that already exists rather than creating one"
        )
    agent_id = str(match["id"])

    active = [
        d
        for d in get(f"/deployments?agent_id={agent_id}")
        if d.get("agent_id") == agent_id
        and d.get("status") == "active"
        and d.get("environment") == DEPLOY_ENVIRONMENT
    ]
    if not active:
        return agent_id, None, None
    deployment = sorted(active, key=lambda d: str(d["deployed_at"]))[-1]
    version_id = deployment.get("version_id")

    versions = get(f"/agents/{agent_id}/versions")
    row = next((v for v in versions if v.get("id") == version_id), None)
    if row is None:
        return agent_id, None, None
    # The deployment carries a commit too; prefer the version's own, and fall
    # back rather than reporting "unknown" when only the deployment recorded it.
    commit = row.get("commit_sha") or deployment.get("commit_sha")
    return agent_id, str(commit) if commit else None, str(version_id)


def _wanted(name: str) -> bool:
    if name in BUNDLE_MEMBERS:
        return True
    return any(name.startswith(tree) for tree in BUNDLE_TREES)


def bundle_from_repo_tarball(archive: bytes, prefix: str = BUNDLE_PREFIX) -> bytes:
    """Re-tar the bundle subdirectory out of a repository tarball.

    GitHub's tarball wraps everything in one top-level directory whose name
    carries the sha, so the prefix cannot be hardcoded and is discovered from the
    first member instead.

    Members are filtered to the bundle's own files. Directory entries, symlinks
    and anything outside the prefix are dropped -- a symlink inside an archive is
    how a tar extraction escapes its directory, and this bundle has no use for
    one.
    """

    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as source:
        members = source.getmembers()
        if not members:
            raise SelfUpgradeError("repository tarball is empty")
        root = members[0].name.split("/", 1)[0]
        want = f"{root}/{prefix}/"
        out = io.BytesIO()
        found = 0
        with tarfile.open(fileobj=out, mode="w:gz") as bundle:
            for member in members:
                if not member.isfile() or not member.name.startswith(want):
                    continue
                relative = member.name[len(want) :]
                if not _wanted(relative):
                    continue
                extracted = source.extractfile(member)
                if extracted is None:
                    continue
                data = extracted.read()
                info = tarfile.TarInfo(relative)
                info.size = len(data)
                info.mode = 0o644
                bundle.addfile(info, io.BytesIO(data))
                found += 1
        if found == 0:
            raise SelfUpgradeError(
                f"no bundle files under {prefix!r} in the repository tarball; "
                f"check the path and the branch"
            )
    return out.getvalue()


def resolve_digest(connector: str, commit: str) -> str:
    """The published image digest for one connector at one commit.

    `release.yaml` publishes every example connector on each push to a release
    branch, tagged `sha-<commit>`. So the images that belong with a bundle are
    derivable from the bundle's own commit -- no constant to keep in step, and a
    deploy of commit X can only ever run images built from commit X.

    Anonymous: the packages are public, like the repository. Verified against the
    live registry rather than assumed.
    """

    repository = f"curie-eng/curie-sre-bot-{connector}"
    token = json.loads(
        _get(
            f"https://ghcr.io/token?service=ghcr.io&scope=repository:{repository}:pull",
            None,
            "application/json",
        )
    ).get("token")
    if not token:
        raise SelfUpgradeError(f"ghcr.io issued no pull token for {repository}")
    request = urllib.request.Request(
        f"https://ghcr.io/v2/{repository}/manifests/sha-{commit}",
        headers={
            "Authorization": f"Bearer {token}",
            # An index, not a single manifest: the cluster is arm64 here and
            # amd64 elsewhere, and pinning one architecture's manifest would
            # deploy an image that cannot run on the other.
            "Accept": (
                "application/vnd.oci.image.index.v1+json, "
                "application/vnd.docker.distribution.manifest.list.v2+json"
            ),
            "User-Agent": "curie-sre-bot-self-upgrade",
        },
        method="HEAD",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            digest = response.headers.get("Docker-Content-Digest")
    except urllib.error.HTTPError as exc:
        raise SelfUpgradeError(
            f"no published image for {connector} at commit {commit[:12]} "
            f"(HTTP {exc.code}). Every release-branch push publishes one, so a "
            f"missing image means this commit's release build has not finished "
            f"or did not run."
        ) from exc
    if not digest:
        raise SelfUpgradeError(f"{repository} returned no digest header")
    return str(digest)


_MISSING = object()


def _carried_overrides_declaration(key: str, declared: object, carried: str) -> bool:
    """Whether a running env value may replace the incoming declaration.

    Operator customizations of a still-granted value survive. An incoming empty
    string is how connectors.yaml revokes a grant, and a narrower ``*_ALLOWLIST``
    is a lowered ceiling: neither may be overwritten by the value currently
    running.
    """

    if declared is _MISSING:
        return True
    if declared is None or (isinstance(declared, str) and declared.strip() == ""):
        return False
    if key.endswith("_ALLOWLIST"):
        incoming = {part.strip() for part in str(declared).split(",") if part.strip()}
        running = {part.strip() for part in carried.split(",") if part.strip()}
        if incoming < running:
            return False
    return True


def pin_build_connectors(
    declaration: bytes, commit: str, carried: dict[str, dict[str, str]]
) -> bytes:
    """Turn every `build:` connector into a digest-pinned `image:`.

    Two substitutions: a `build:` block records a LOCAL image id, which the
    cluster tier refuses, and runtime connector environment may differ from the
    repository defaults. Resolve the image and carry the running environment
    across the immutable rebuild, except where the incoming declaration
    narrows or revokes it.
    """

    import yaml

    parsed = yaml.safe_load(declaration)
    for name, spec in (parsed.get("connectors") or {}).items():
        if "build" in spec:
            spec.pop("build")
            spec["image"] = f"ghcr.io/curie-eng/curie-sre-bot-{name}@{resolve_digest(name, commit)}"
        env = spec.setdefault("env", {})
        if not isinstance(env, dict):
            env = {}
            spec["env"] = env
        for key, value in (carried.get(name) or {}).items():
            if _carried_overrides_declaration(key, env.get(key, _MISSING), value):
                env[key] = value
    return yaml.safe_dump(parsed, sort_keys=False).encode()


def running_connector_env(
    api_url: str,
    api_key: str,
    agent_id: str,
    version_id: str,
    release: str,
    namespace: str,
) -> dict[str, dict[str, str]]:
    """The env each connector is running with, read off the rendered objects.

    The SOURCE `connectors.yaml` is not retrievable -- a version's file listing
    carries the authored text (skill, manifest, eval cases) and not the connector
    declaration. The RENDERED objects are, and they carry the resolved values,
    which is what has to survive an upgrade.
    """

    query = f"?release={release}&namespace={namespace}&app_name=curie"
    rendered = json.loads(
        _api(api_url, api_key, f"/agents/{agent_id}/versions/{version_id}/connectors{query}")
    )
    out: dict[str, dict[str, str]] = {}
    for manifest in rendered.get("manifests") or []:
        if manifest.get("kind") != "Deployment":
            continue
        name = manifest["metadata"]["name"].rsplit("-mcp-", 1)[-1]
        container = manifest["spec"]["template"]["spec"]["containers"][0]
        out[name] = {
            entry["name"]: entry["value"]
            for entry in container.get("env") or []
            if "value" in entry
        }
    return out


def member_of(bundle: bytes, name: str) -> bytes:
    """One file out of a bundle tarball."""

    with tarfile.open(fileobj=io.BytesIO(bundle), mode="r:gz") as tar:
        try:
            extracted = tar.extractfile(name)
        except KeyError:
            extracted = None
        if extracted is None:
            raise SelfUpgradeError(f"the bundle carries no {name}")
        return extracted.read()


def replace_member(bundle: bytes, name: str, body: bytes) -> bytes:
    """The same bundle with one file swapped, everything else byte for byte."""

    out = io.BytesIO()
    source = tarfile.open(fileobj=io.BytesIO(bundle), mode="r:gz")
    with source, tarfile.open(fileobj=out, mode="w:gz") as target:
        for member in source.getmembers():
            if member.name == name:
                replacement = tarfile.TarInfo(name)
                replacement.size = len(body)
                replacement.mode = member.mode
                target.addfile(replacement, io.BytesIO(body))
                continue
            target.addfile(member, source.extractfile(member))
    return out.getvalue()


def deploy(
    api_url: str,
    api_key: str,
    agent_id: str,
    bundle: bytes,
    commit: str,
) -> str:
    """Create a version from ``bundle``, upload it, and make it active.

    Three calls because the platform models them as three facts: a version
    exists, its bytes are stored, and it is the one being served. Ordered so a
    failure never leaves a version marked active with no bundle behind it.
    """

    created = json.loads(
        _api(
            api_url,
            api_key,
            f"/agents/{agent_id}/versions",
            method="POST",
            body=json.dumps(
                {
                    "version_label": f"self-upgrade-{commit[:12]}",
                    "commit_sha": commit,
                    "created_by": "sre-bot-self-upgrade",
                }
            ).encode(),
        )
    )
    version_id = str(created["id"])
    # A multipart form with a `file` part, not a raw body. The endpoint is an
    # upload rather than a document write, and a raw PUT is refused with a 422
    # naming a field the caller never knew about:
    #     {"loc": ["body", "file"], "msg": "Field required"}
    boundary = "----curie" + hashlib.sha256(bundle).hexdigest()[:24]
    form = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="file"; filename="bundle.tar.gz"\r\n'
        "Content-Type: application/gzip\r\n\r\n"
    ).encode()
    form += bundle + f"\r\n--{boundary}--\r\n".encode()
    _api(
        api_url,
        api_key,
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        method="PUT",
        body=form,
        content_type=f"multipart/form-data; boundary={boundary}",
    )
    _api(
        api_url,
        api_key,
        "/deployments",
        method="POST",
        body=json.dumps(
            {
                "agent_id": agent_id,
                "version_id": version_id,
                "environment": DEPLOY_ENVIRONMENT,
                "commit_sha": commit,
            }
        ).encode(),
    )
    return version_id


def upgrade_disposition(current: str | None, version_id: str | None) -> str:
    """What to do about a deployed version, given what is known about it.

    Two different absences, and only one is fatal:

    - No VERSION means nothing to read the running connector env off, so a deploy
      would ship the bundle's placeholder allowlists and produce a bot that
      refuses every write while looking healthy. Refuse.
    - No COMMIT means only the comparison is lost. "Am I behind?" becomes
      unanswerable; "can I deploy?" does not.

    Refusing on a missing commit was wrong, and stuck: `curie example sre-bot
    install` creates versions with no commit, so an install deployed the
    supported way could never be upgraded by this job again -- and the refusal
    advised deploying through the installer, which is what caused it (#2128).

    Returns one of ``refuse``, ``deploy-unknown``, ``deploy``.
    """

    if not version_id:
        return "refuse"
    if not current:
        return "deploy-unknown"
    return "deploy"


def main() -> int:
    repo = os.environ.get("CURIE_SELF_UPGRADE_REPO", "curie-eng/curie")
    branch = os.environ.get("CURIE_SELF_UPGRADE_BRANCH", "main")
    agent = os.environ.get("CURIE_SELF_UPGRADE_AGENT", "sre-bot")
    api_url = os.environ.get("CURIE_API_URL")
    api_key = os.environ.get("CURIE_API_KEY")
    token = os.environ.get("GITHUB_TOKEN") or None
    dry_run = os.environ.get("CURIE_SELF_UPGRADE_DRY_RUN", "").lower() in ("1", "true")
    # The rendered connector objects are namespaced by the release that owns
    # them, so the lookup needs both. Defaults match the chart's own.
    release = os.environ.get("CURIE_SELF_UPGRADE_RELEASE", "curie")
    namespace = os.environ.get("CURIE_SELF_UPGRADE_NAMESPACE", "curie")

    if not api_url or not api_key:
        print("CURIE_API_URL and CURIE_API_KEY are required", flush=True)
        return 2

    try:
        tip = latest_commit(repo, branch, token)
        agent_id, current, version_id = deployed_commit(api_url, api_key, agent)
    except SelfUpgradeError as exc:
        # Exit non-zero: a job that cannot tell whether it is behind must not
        # report success. A silent "nothing to do" is the failure this whole file
        # is a reaction to.
        print(f"cannot determine whether {agent} is behind: {exc}", flush=True)
        return 1

    print(f"{agent}: deployed={current or 'unknown'} tip={tip[:12]}", flush=True)
    if current == tip:
        print("already on the repository tip; nothing to do", flush=True)
        return 0
    if dry_run:
        print("dry run: would deploy the bundle at the tip", flush=True)
        return 0

    # Two different absences, and only one of them is fatal.
    #
    # Without a VERSION there is nothing to read the running connector env off,
    # so a deploy would ship the bundle's placeholder allowlists and produce a
    # bot that refuses every write while looking healthy. That is worth refusing.
    #
    # Without a COMMIT the only thing lost is the comparison -- "am I behind?"
    # becomes unanswerable, not "I cannot deploy". Refusing there was wrong, and
    # wrong in a way that stuck: `curie example sre-bot install` creates versions
    # with no commit, so an install deployed the supported way could never be
    # upgraded by this job again, and the message told the reader to do the very
    # thing that caused it (#2128).
    #
    # Deploying the tip is the safe answer to an unknown current: it is where the
    # repository is, and the alternative is an install that can never move.
    disposition = upgrade_disposition(current, version_id)
    if disposition == "refuse":
        print(
            "the deployed version cannot be identified, so there is no connector "
            "declaration to carry forward and a deploy would ship the bundle's "
            "placeholder ceilings. Refusing.",
            flush=True,
        )
        return 1
    if disposition == "deploy-unknown":
        print(
            "the deployed version records no commit, so whether this install is "
            "behind cannot be determined. Deploying the tip, which is the only "
            "answer that leaves it somewhere known.",
            flush=True,
        )

    try:
        archive = _get(
            f"https://api.github.com/repos/{repo}/tarball/{tip}",
            token,
            "application/vnd.github+json",
        )
        bundle = bundle_from_repo_tarball(archive)
        carried = running_connector_env(api_url, api_key, agent_id, version_id, release, namespace)
        bundle = replace_member(
            bundle,
            "connectors.yaml",
            pin_build_connectors(member_of(bundle, "connectors.yaml"), tip, carried),
        )
        new_version = deploy(api_url, api_key, agent_id, bundle, tip)
    except SelfUpgradeError as exc:
        print(f"deploy failed: {exc}", flush=True)
        return 1

    print(f"deployed {agent} version {new_version} at {tip[:12]}", flush=True)
    return 0


if __name__ == "__main__":  # pragma: no cover - entrypoint
    raise SystemExit(main())
