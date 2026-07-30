"""Git-flow engine (J1): turn a pushed commit into a deploy or a promote.

A push to the dev branch builds the plugin bundle at that commit and deploys it
under the dev bot identity; a push to the prod branch promotes the same commit
(reusing the already-built bundle when present) under the prod bot identity. The
bundle is produced by archiving the pushed sha from the repo, so the flow needs
only git-protocol access to the remote (local bare repos in tests) and never the
GitHub API.
"""

import base64
import hashlib
import hmac
import os
import re
import shutil
import subprocess
import tempfile
from urllib.parse import urlsplit

from aci_protocol import EvalJob
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from . import bundles, crud, deploy
from .config import Settings
from .evalqueue import EvalQueue, now_iso
from .models import Environment
from .schemas import WebhookResult
from .storage import ObjectStore

_ZERO_SHA = "0" * 40
# A full lowercase-hex git object id: SHA-1 (40) or SHA-256 (64).
# Unanchored on purpose; paired with fullmatch below so a trailing newline
# (which `$` would tolerate) is rejected.
_SHA_RE = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")


def _is_valid_sha(sha: str) -> bool:
    """True only for a full lowercase-hex SHA-1 or SHA-256 object id."""

    return bool(_SHA_RE.fullmatch(sha))


class GitFlowError(Exception):
    """The repo could not be fetched or archived at the requested commit."""


def verify_signature(secret: str, body: bytes, header: str | None) -> bool:
    """Constant-time check of GitHub's X-Hub-Signature-256 over the raw body."""

    if not header or not header.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header)


def environment_for_ref(ref: str | None, settings: Settings) -> Environment | None:
    """Map a git ref to an environment, or None if it is not a deploy branch.

    The full ref is matched (refs/heads/<branch>), never just the last path
    segment, so a tag named `main` or a branch `feature/dev` cannot masquerade
    as a deploy branch.
    """

    if not ref:
        return None
    if ref == f"refs/heads/{settings.dev_branch}":
        return Environment.dev
    if ref == f"refs/heads/{settings.prod_branch}":
        return Environment.prod
    return None


def _clone_credential_env(clone_url: str, settings: Settings) -> dict[str, str]:
    """Git config env that authenticates the clone, or empty if not applicable.

    Private repositories are the norm for a bundle -- it names internal hosts and
    services -- so an unauthenticated clone makes git-flow unusable for most real
    agents (#1058).

    The credential travels as git config supplied through ``GIT_CONFIG_*``
    environment variables rather than embedded in the URL. That keeps it out of
    ``argv`` (so it cannot be read from ``ps`` or leak through a subprocess error
    that echoes the command) and out of the cloned repo's ``.git/config``, which
    URL-embedded credentials are persisted into.

    The header is scoped to the clone URL's own host, so a webhook naming some
    other host cannot make us send the token there.
    """

    token = settings.github_token
    if not token or not clone_url.startswith("https://"):
        return {}
    host = urlsplit(clone_url).netloc
    if not host:
        return {}
    basic = base64.b64encode(f"x-access-token:{token}".encode()).decode()
    return {
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": f"http.https://{host}/.extraheader",
        "GIT_CONFIG_VALUE_0": f"Authorization: Basic {basic}",
    }


def _git_failure_detail(exc: BaseException) -> str:
    """git's stderr, which says *why* -- 'Repository not found' vs a timeout.

    The old message interpolated the exception, whose repr is the argv and an
    exit code: 'returned non-zero exit status 128' told an operator nothing, and
    the actual reason was discarded despite being captured. argv is credential-
    free by construction here (see ``_clone_credential_env``), but the tail is
    bounded anyway so a hostile remote cannot flood the response.
    """

    stderr = getattr(exc, "stderr", None)
    if isinstance(stderr, bytes):
        stderr = stderr.decode("utf-8", "replace")
    if isinstance(stderr, str) and stderr.strip():
        return stderr.strip()[-400:]
    return str(exc)[:200]


def clone_and_archive(clone_url: str, sha: str, settings: Settings) -> bytes:
    """Mirror-clone the repo and return a tar of the tree at ``sha``.

    Refuses clone URLs outside the configured scheme allowlist and restricts git
    to safe transports, so a webhook cannot coerce an arbitrary git command.
    """

    if not clone_url.startswith(settings.git_allowed_schemes):
        raise GitFlowError(f"clone url scheme not allowed: {clone_url}")
    if not _is_valid_sha(sha):
        raise GitFlowError(f"invalid commit sha: {sha!r}")

    tmp = tempfile.mkdtemp(prefix="gitflow-")
    env = {
        **os.environ,
        "GIT_ALLOW_PROTOCOL": "file:https:http",
        "GIT_TERMINAL_PROMPT": "0",
        **_clone_credential_env(clone_url, settings),
    }
    try:
        try:
            subprocess.run(
                ["git", "clone", "--quiet", "--mirror", clone_url, tmp],
                check=True,
                capture_output=True,
                env=env,
                timeout=120,
            )
            archived = subprocess.run(
                ["git", "-C", tmp, "archive", "--format=tar", "--", sha],
                check=True,
                capture_output=True,
                env=env,
                timeout=120,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            raise GitFlowError(f"could not archive {sha[:12]}: {_git_failure_detail(exc)}") from exc
        return archived.stdout
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def process_push(
    session: AsyncSession,
    store: ObjectStore,
    settings: Settings,
    eval_queue: EvalQueue,
    payload: dict[str, object],
) -> WebhookResult:
    """Deploy (dev) or promote (prod) the pushed commit; ignore other refs."""

    ref = payload.get("ref")
    after = payload.get("after")
    repo = payload.get("repository")
    environment = environment_for_ref(ref if isinstance(ref, str) else None, settings)
    if environment is None:
        return WebhookResult(status="ignored")
    if not isinstance(after, str) or after == _ZERO_SHA or not _is_valid_sha(after):
        return WebhookResult(status="ignored")
    if not isinstance(repo, dict):
        return WebhookResult(status="ignored")

    full_name = repo.get("full_name")
    clone_url = repo.get("clone_url") or repo.get("url")
    if not isinstance(full_name, str) or not isinstance(clone_url, str):
        return WebhookResult(status="ignored")

    agent = await crud.get_agent_by_repo(session, full_name)
    if agent is None:
        return WebhookResult(status="ignored")

    version = await crud.get_version_by_commit(session, agent.id, after)
    # Only a version whose bundle is actually stored may be reused for promote.
    # A row with bundle_ref still None is the residue of a prior attempt that
    # failed after the row committed; rebuild and store into it rather than
    # deploying a bundleless version. This same "new-or-repaired" condition
    # gates the eval fan-out below: a redelivered push for an already-bundled
    # version must not enqueue a second job for the same version.
    bundle_built = version is None or version.bundle_ref is None
    if bundle_built:
        try:
            data = await run_in_threadpool(clone_and_archive, clone_url, after, settings)
            extension, content_type = deploy.validate_archive(data, settings)
        except GitFlowError as exc:
            return WebhookResult(
                status="rejected",
                errors=[{"code": "git.archive_failed", "message": str(exc)}],
            )
        except bundles.UnsupportedArchive as exc:
            return WebhookResult(
                status="rejected",
                errors=[{"code": "bundle.unsupported", "message": str(exc)}],
            )
        except deploy.BundleInvalid as exc:
            return WebhookResult(status="rejected", errors=exc.errors)

        if version is None:
            version = await crud.create_version_row(
                session,
                agent.id,
                version_label=after[:12],
                created_by="git-flow",
                commit_sha=after,
            )
        await deploy.store_bundle(store, session, agent.id, version, data, extension, content_type)

    # Either the version pre-existed with a bundle, or the block above created
    # or repaired it; it is non-None from here on.
    assert version is not None

    # A prod promote (bundle_built is False) reuses a bundle that may have been
    # stored before the current size/ratio caps existed, or under looser ones --
    # revalidate it here (ADR-0059 decision 3's backward-compat commitment). The
    # bundle_built branch above already ran these bytes through
    # `deploy.validate_archive` under the current caps moments ago, so re-fetching
    # and rechecking the identical bytes here would be redundant.
    if not bundle_built:
        try:
            await deploy.revalidate_stored_bundle(store, version, settings)
        except deploy.BundleTooLarge as exc:
            return WebhookResult(
                status="rejected",
                errors=[{"code": "bundle.too_large", "message": str(exc)}],
            )

    deployment = await crud.create_deployment_row(
        session,
        agent.id,
        version.id,
        environment,
        commit_sha=after,
    )

    # Fan out the eval run for a dev deploy (eval-as-CI); prod promote does not.
    # Only when this delivery actually built the bundle, so a redelivered push
    # for an already-bundled version does not spawn a duplicate eval job.
    if environment is Environment.dev and bundle_built:
        await eval_queue.enqueue(
            EvalJob(
                agent_id=agent.id,
                version_id=version.id,
                sha=after,
                suite=settings.eval_default_suite,
                bundle_ref=version.bundle_ref,
                requested_at=now_iso(),
            )
        )

    return WebhookResult(
        status="promoted" if environment is Environment.prod else "deployed",
        environment=environment,
        agent_id=agent.id,
        version_id=version.id,
        deployment_id=deployment.id,
        commit_sha=after,
    )
