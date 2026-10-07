"""Git-flow engine (J1): turn a pushed commit into a deploy or a promote.

A push to the dev branch builds the plugin bundle at that commit and deploys it
under the dev bot identity; a push to the prod branch promotes the same commit
under the prod bot identity, reusing the already-built bundle when present --
the lookup runs BEFORE any clone, so a promote of a sha this repository has
already bundled needs no access to the remote at all (#1211). When the bundle
does have to be built, it is produced by archiving the pushed sha from the repo,
so that path needs only git-protocol access to the remote (local bare repos in
tests) and never the GitHub API.
"""

import base64
import hashlib
import hmac
import logging
import os
import re
import shutil
import subprocess
import tempfile
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

from aci_protocol import EvalJob
from plugin_format.deploy_targets import DeployTargetsFile
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from curie_api.crud import agents as crud_agents
from curie_api.crud import deployments as crud_deployments
from curie_api.crud import versions as crud_versions
from curie_api.schemas.deployments import WebhookResult

from . import bundles, deploy
from .config import Settings
from .evalqueue import EvalQueue, now_iso
from .github_app import credentials_for
from .models import GIT_FLOW_CREATED_BY, Agent, AgentVersion, Environment
from .repo_full_name import InvalidRepoFullName, repo_url_path
from .storage import ObjectStore

logger = logging.getLogger(__name__)

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


class CloneOriginMismatch(GitFlowError):
    """The payload's clone URL is not the registered repository's origin."""


class CommitNotOnBranch(GitFlowError):
    """The pushed commit is not reachable from the payload's deploy branch."""


def verify_signature(secret: str, body: bytes, header: str | None) -> bool:
    """Constant-time check of GitHub's X-Hub-Signature-256 over the raw body.

    A blank secret is not a signing key. Reject it before HMAC so an empty
    shared key cannot authenticate a delivery.
    """

    if not secret.strip():
        return False
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


_DEFAULT_PORTS = {"https": 443, "http": 80}


def trusted_clone_url(repo_full_name: str, settings: Settings) -> str:
    """The one origin this installation is allowed to clone for that repo.

    Derived from configuration plus the repository binding stored on the agent
    row, never from the webhook payload, so the platform GitHub credential can
    only ever travel to the configured host (#1122).
    """

    return f"{settings.github_clone_base.rstrip('/')}/{repo_url_path(repo_full_name)}.git"


def _origin_key(url: str) -> tuple[str, str, str, str, int | None, str, str, str]:
    """A normalized comparison key for a clone URL.

    Covers every dimension that decides where a request actually goes: scheme,
    user information, host, port (with the scheme's default applied so an
    explicit ``:443`` matches), path modulo a trailing ``.git`` and trailing
    slashes, query, and fragment. User information is carried IN the key rather
    than rejected separately, so any credential in the payload URL mismatches
    automatically against a derived URL that never carries one.

    Keys are compared as WHOLE TUPLES for equality. No element may ever be
    compared with ``startswith`` or a substring test: that is exactly what would
    let ``owner/repo-evil`` pass against a registered ``owner/repo``.
    """

    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    port = parts.port
    if port is None:
        port = _DEFAULT_PORTS.get(scheme)
    path = parts.path.rstrip("/")
    if path.endswith(".git"):
        path = path[: -len(".git")]
    path = path.rstrip("/")
    return (
        scheme,
        parts.username or "",
        parts.password or "",
        parts.hostname or "",
        port,
        path,
        parts.query,
        parts.fragment,
    )


def _origins_match(requested: str, trusted: str) -> bool:
    """Whole-key equality between a requested clone URL and the trusted origin.

    An unparseable URL reads as a mismatch rather than crashing inside the
    threadpool: ``urlsplit().port`` raises ``ValueError`` on a non-numeric port
    and ``urlsplit`` itself raises on a malformed IPv6 literal.
    """

    try:
        return _origin_key(requested) == _origin_key(trusted)
    except ValueError:
        return False


def clone_credential_env(
    trusted_url: str, settings: Settings, *, repo_full_name: str, credentials: Any = None
) -> dict[str, str]:
    """Git config env that authenticates the clone, or empty if not applicable.

    Private repositories are the norm for a bundle -- it names internal hosts and
    services -- so an unauthenticated clone makes git-flow unusable for most real
    agents (#1058).

    The credential travels as git config supplied through ``GIT_CONFIG_*``
    environment variables rather than embedded in the URL. That keeps it out of
    ``argv`` (so it cannot be read from ``ps`` or leak through a subprocess error
    that echoes the command) and out of the cloned repo's ``.git/config``, which
    URL-embedded credentials are persisted into.

    The URL reaching this function is the origin derived from the stored
    repository binding (``trusted_clone_url``), never the webhook payload's
    ``clone_url``, so the header is scoped to the one host this installation
    deploys from (#1122).
    """

    # Which credential, per ADR-0092: a GitHub App installation token scoped to
    # this one repository when an App is configured, otherwise the PAT. Only the
    # SOURCE of the token varies -- `x-access-token` below is the username both
    # kinds authenticate with, so nothing downstream changes.
    resolver = credentials if credentials is not None else credentials_for(settings)
    token = resolver.token_for(repo_full_name)
    if not token or not trusted_url.startswith("https://"):
        return {}
    host = urlsplit(trusted_url).netloc
    if not host:
        return {}
    basic = base64.b64encode(f"x-access-token:{token}".encode()).decode()
    return {
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": f"http.https://{host}/.extraheader",
        "GIT_CONFIG_VALUE_0": f"Authorization: Basic {basic}",
    }


def _git_failure_detail(
    exc: BaseException | subprocess.CompletedProcess[bytes],
) -> str:
    """git's stderr, which says *why* -- 'Repository not found' vs a timeout.

    The old message interpolated the exception, whose repr is the argv and an
    exit code: 'returned non-zero exit status 128' told an operator nothing, and
    the actual reason was discarded despite being captured. argv is credential-
    free by construction here (see ``clone_credential_env``), but the tail is
    bounded anyway so a hostile remote cannot flood the response.
    """

    stderr = getattr(exc, "stderr", None)
    if isinstance(stderr, bytes):
        stderr = stderr.decode("utf-8", "replace")
    if isinstance(stderr, str) and stderr.strip():
        return stderr.strip()[-400:]
    return str(exc)[:200]


def verify_push_origin(
    clone_url: str, sha: str, settings: Settings, *, repo_full_name: str
) -> str:
    """Authorize a push against its repository binding, and return the derived URL.

    This is the network-free half of the clone's authorization: the payload
    scheme allowlist, the ``_is_valid_sha`` option-injection gate (#65), the
    ``repo_full_name`` derivation (``InvalidRepoFullName``, #1140), the derived
    URL's own scheme allowlist, and whole-key origin equality (#1122). None of
    it needs the remote.

    It is its own function so the path that reuses a stored bundle and the path
    that clones run THE SAME CODE rather than two copies (#1211).
    ``clone_and_archive`` still calls it, so its contract is unchanged; a
    promote that never clones calls it directly.

    Returns the trusted origin derived from ``repo_full_name`` -- the only URL
    that may ever reach git. Raises ``GitFlowError`` for a refused scheme or a
    malformed sha, ``InvalidRepoFullName`` for an unencodable binding, and
    ``CloneOriginMismatch`` when the payload names a different origin.
    """

    if not clone_url.startswith(settings.git_allowed_schemes):
        raise GitFlowError(f"clone url scheme not allowed: {clone_url}")
    if not _is_valid_sha(sha):
        raise GitFlowError(f"invalid commit sha: {sha!r}")

    trusted_url = trusted_clone_url(repo_full_name, settings)
    if not trusted_url.startswith(settings.git_allowed_schemes):
        # The derived URL, not the payload, is what git actually clones. The
        # comparison below is not a substitute for this check: it is safe
        # today only by transitivity, and a misconfigured `github_clone_base`
        # must fail as a deployment configuration error, not be reported as a
        # forged push. Never interpolate the credential here; this URL never
        # carries one (see `clone_credential_env`), but keep it that way.
        raise GitFlowError(
            f"configured github_clone_base produces a clone url outside the "
            f"allowed schemes {settings.git_allowed_schemes!r}: {trusted_url!r}"
        )
    if not _origins_match(clone_url, trusted_url):
        # Truncated: this lands in the webhook response body and in the warning
        # log, and a forged payload must not be able to flood either.
        raise CloneOriginMismatch(
            f"clone url does not match the registered repository "
            f"{repo_full_name!r}: {clone_url[:200]!r}"
        )
    return trusted_url


def clone_and_archive(
    clone_url: str,
    sha: str,
    settings: Settings,
    *,
    repo_full_name: str,
    ref: str,
    credentials: Any = None,
) -> bytes:
    """Mirror-clone the repo and return a tar of the tree at ``sha``.

    Refuses clone URLs outside the configured scheme allowlist and restricts git
    to safe transports, so a webhook cannot coerce an arbitrary git command.

    ``clone_url`` is the untrusted webhook payload value: it is compared against
    the origin derived from ``repo_full_name`` (which the caller must have read
    from the agent row) and then discarded. Git is handed the derived origin,
    for both the clone argv and the credential header, so a signed-but-forged
    payload cannot choose where the platform GitHub token travels (#1122).
    """

    trusted_url = verify_push_origin(
        clone_url, sha, settings, repo_full_name=repo_full_name
    )

    tmp = tempfile.mkdtemp(prefix="gitflow-")
    env = {
        **os.environ,
        "GIT_ALLOW_PROTOCOL": "file:https:http",
        "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_TERMINAL_PROMPT": "0",
        **clone_credential_env(
            trusted_url, settings, repo_full_name=repo_full_name, credentials=credentials
        ),
    }
    try:
        try:
            # `git help config` (git 2.43.0): "http.followRedirects: Whether git
            # should follow HTTP redirects. ... If set to false, git will treat
            # all redirects as errors. If set to initial, git will follow
            # redirects only for the initial request to a remote, but not for
            # subsequent follow-up HTTP requests. ... The default is initial."
            # The initial request is precisely the one carrying the extraheader,
            # so the default would let a redirect target receive the credential.
            # `-c` must precede the `clone` subcommand to take effect.
            subprocess.run(
                [
                    "git",
                    "-c",
                    "http.followRedirects=false",
                    "clone",
                    "--quiet",
                    "--mirror",
                    "--",
                    trusted_url,
                    tmp,
                ],
                check=True,
                capture_output=True,
                env=env,
                timeout=120,
            )
            ancestry = subprocess.run(
                ["git", "-C", tmp, "merge-base", "--is-ancestor", sha, ref],
                check=False,
                capture_output=True,
                env=env,
                timeout=120,
            )
            if ancestry.returncode == 1:
                raise CommitNotOnBranch(
                    f"commit {sha[:12]} is not reachable from {ref}"
                )
            if ancestry.returncode != 0:
                raise GitFlowError(
                    f"could not verify commit {sha[:12]} against {ref}: "
                    f"{_git_failure_detail(ancestry)}"
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


def log_push_outcome(result: WebhookResult, payload: dict[str, object], *, source: str) -> None:
    """Log a rejected push loudly, whichever lane delivered it (#1066, #1268).

    A rejected push is still acknowledged with 200, because GitHub retries and
    redelivers on a non-2xx and the push is not going to succeed on a retry. The
    cost is that every dashboard reports success: GitHub shows a green delivery,
    the access log shows "POST /github/webhook 200 OK", and no agent, version, or
    deployment appears. The reason exists only in the response body, which
    nothing surfaces.

    That combination made a broken deploy indistinguishable from a working one
    until someone thought to open the delivery payload in GitHub's UI. Logging
    the rejection is what makes it findable from the platform side (#1066).

    It lives here, not in the webhook router, because the POLLING lane needs it
    more and had it less (#1268). A polled push has no HTTP response body to
    carry the errors and no GitHub delivery UI to fall back on -- polling exists
    precisely for clusters GitHub cannot reach. Its only output named the
    outcome "deployed" at INFO and discarded `result.errors` entirely, which is
    #1066 again on the one path with no fallback. Shared rather than copied so
    the two lanes cannot drift.
    """

    if result.status != "rejected":
        return
    repo = payload.get("repository")
    full_name = repo.get("full_name") if isinstance(repo, dict) else None
    codes = [e.get("code", "?") for e in (result.errors or [])]
    logger.warning(
        "%s rejected push: repo=%s ref=%s sha=%s codes=%s errors=%s",
        source,
        full_name,
        payload.get("ref"),
        str(payload.get("after"))[:12],
        ",".join(codes) or "none",
        result.errors,
    )


def _rejected(
    exc: deploy.ApprovalRoutesUnbound | deploy.BundleTooLarge | deploy.SealingKeyCustody,
    agent_id: uuid.UUID | None = None,
) -> WebhookResult:
    """The rejection envelope for the two refusals `process_push` shares with the API.

    `approval_routes.unbound` (#2436) can fire at three sites -- the two bundle
    attachments and the deployment itself -- and `bundle.too_large` (ADR-0059
    decision 3) at four: the reuse path's `deploy.yaml` read, the revalidation
    before deployment, and both route gates, which extract a STORED object to
    learn what it declares and so inherit the caps question. One home for all of
    them, reading the code and the message off the exception itself, so what an
    operator greps for cannot come out different depending on which check got
    there first. The message is the exception's own, identical to the one the
    API returns as a 422 detail.
    """

    return WebhookResult(
        status="rejected",
        agent_id=agent_id,
        errors=[{"code": exc.code, "message": str(exc)}],
    )


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
    if not isinstance(ref, str):
        return WebhookResult(status="ignored")
    environment = environment_for_ref(ref, settings)
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

    # Every agent built from this repository (ADR-0091). A repository binds
    # several on purpose: a dev bot and a prod bot are the same bundle on two
    # channels. Which one THIS push deploys to comes from the bundle's
    # deploy.yaml, so the bundle has to be fetched before the agent is known.
    repo_agents = await crud_agents.get_agents_by_repo(session, full_name)
    # The second clause is unreachable in practice (the lookup's predicate is
    # `Agent.repo_full_name == full_name` with a non-None argument); it exists
    # solely to narrow `str | None` to `str` for the origin derivation below.
    repo_agents = [a for a in repo_agents if a.repo_full_name is not None]
    if not repo_agents:
        casing_matches = await crud_agents.get_agents_by_repo_casefold(session, full_name)
        if casing_matches:
            return WebhookResult(
                status="rejected",
                errors=[{
                    "code": "git.repository_case_mismatch",
                    "message": "the push repository casing differs from the stored agent binding",
                }],
            )
        return WebhookResult(status="ignored")

    # The trust model does not move (ADR-0091). The origin is still derived from
    # what the DATABASE holds, never the payload -- and with several agents it
    # stays unambiguous, because every agent bound to a repository carries the
    # same repo_full_name, so the derived origin is identical whichever row is
    # read. The clone is authorized by the repository binding; the target only
    # decides which agent receives the resulting Version.
    trusted_repo_full_name = str(repo_agents[0].repo_full_name)

    # Set only on the clone path; `detect_format` is the only producer and
    # `store_bundle` the only consumer, and neither is reachable when the bytes
    # came from the store (see the `assert` in the bundle_built block below).
    extension: str | None = None
    content_type: str | None = None

    try:
        # Unconditionally, before any database or object-store work: a forged
        # push is refused and logged before this handler does anything else.
        # Hoisting it out of the clone is what keeps #1122's origin pin, #1140's
        # repo_full_name derivation and #65's sha gate on BOTH paths -- none of
        # them ever needed the network, so none of them was really something the
        # clone was providing.
        verify_push_origin(
            clone_url, after, settings, repo_full_name=trusted_repo_full_name
        )

        # Has anything in this repository already built and stored this exact
        # commit? If so, reuse those bytes and do not clone at all (#1211).
        # The reuse fast path is PROD ONLY; the named gate below is
        # load-bearing, so read WHY PROD ONLY before the reuse argument.
        #
        # WHY PROD ONLY. `CommitNotOnBranch` (#1139) is
        # `git merge-base --is-ancestor` run against the remote, and there is no
        # offline form of it. #1211's acceptance criterion is that the prod
        # promote still succeeds when the remote has been deleted. Those two
        # cannot both hold on one code path, so exactly one lane pays: prod
        # trades the ancestry check for the offline promote, and dev -- which
        # was never asked to work offline -- clones on every delivery exactly as
        # it does on `main` today and keeps #1139 fully enforced.
        #
        # WHAT IS REUSED, AND WHY IT IS SAFE. The stored object passed
        # `validate_bundle` when it was stored and its key is immutable and
        # write-once, so it cannot have changed since. The promote deploys THAT
        # object either way -- before this branch existed the freshly cloned
        # bytes were validated and then discarded -- so cloning to re-validate
        # bytes nobody deploys bought nothing but a network dependency. The
        # bounded read of `deploy.yaml` off the stored bytes is
        # `_read_stored_targets`; its docstring carries the
        # extract-without-revalidate argument in full.
        #
        # WHAT IS PRESERVED. `verify_push_origin` above runs unconditionally, on
        # both paths and in both environments, so #1122's origin pin, both
        # scheme allowlists, #65's sha format gate and #1140's repo_full_name
        # derivation are untouched by this change.
        #
        # WHAT IS NOT PRESERVED, plainly, and only on prod. A holder of the
        # webhook signing secret can POST a signed push with
        # `ref: refs/heads/main` and an `after` sha that has a stored bundle for
        # THIS repository but is not an ancestor of the prod branch -- a sha
        # pushed to dev, bundled, then abandoned rather than merged -- and it
        # will be promoted. Five facts bound that:
        #
        #   1. It is exactly the pre-#1194 posture. Before c655ab9e the promote
        #      performed zero clones and there was no ancestry check at all;
        #      this restores that, it does not open something new.
        #   2. The sha must ALREADY have a stored bundle for this repository, so
        #      it already passed `validate_bundle` on a prior delivery here.
        #      #1139's actual attack -- promoting a hostile fork's
        #      `refs/pull/N/head` sha out of the `--mirror` clone -- is NOT
        #      reachable, because such a sha has no stored bundle. The escape is
        #      "promote a commit of ours that is not on main", not "promote a
        #      stranger's code".
        #   3. Because the dev lane always clones, the only way a sha acquires a
        #      stored bundle in the first place is by passing #1139's ancestry
        #      check on a dev delivery. That is what bounds the prod escape to
        #      "a commit of ours that was on dev and is not on main" rather than
        #      to arbitrary code: the dev gate does the containment work.
        #   4. The clone path is unaffected: any sha without a stored bundle
        #      still clones and still gets the full ancestry check, so #1139's
        #      coverage is intact for every first-time delivery.
        #   5. The attacker is the one `apps/api/CLAUDE.md` already assumes --
        #      "the HMAC signature authenticates the sender, not the payload".
        #
        # This deliberately leaves the dev-redelivery row of #1211's measured
        # table (0 clones before #1194, 1 clone after) unfixed: a re-delivered
        # dev sha still pays a clone. Closing that row means an
        # offline-checkable replacement for #1139, which needs a new invariant
        # and a schema change -- a separate ticket with an ADR, not something to
        # improvise here.
        may_reuse_without_remote_verification = environment is Environment.prod
        prebuilt = (
            await _bundled_version_for_commit(session, repo_agents, after)
            if may_reuse_without_remote_verification
            else None
        )
        if prebuilt is not None:
            archive = await store.get(str(prebuilt.bundle_ref))
            try:
                targets = await run_in_threadpool(_read_stored_targets, archive, settings)
            except bundles.UnsupportedArchive as exc:
                # Exact, not a catch-all. `safe_extract` raises
                # `UnsupportedArchive` for unsafe entries OR for a cap
                # violation, and on already-stored bytes the unsafe-entry causes
                # are unreachable: they were refused before the object could be
                # stored, and the key is immutable. A cap violation is all that
                # is left, and it is what `deploy.revalidate_stored_bundle`
                # -- which this extract now runs in FRONT of -- reports as
                # `bundle.too_large` (ADR-0059 decision 3). Letting it surface
                # as `bundle.unsupported` instead would regress an
                # operator-facing error contract as a side effect of a
                # performance fix.
                raise deploy.BundleTooLarge.for_stored_bundle(prebuilt.id, exc) from exc
        else:
            archive = await run_in_threadpool(
                clone_and_archive,
                clone_url,
                after,
                settings,
                repo_full_name=trusted_repo_full_name,
                ref=ref,
            )
            extension, content_type = await run_in_threadpool(
                deploy.validate_archive, archive, settings
            )
            targets = await run_in_threadpool(_read_targets, archive, settings)
    except InvalidRepoFullName as exc:
        return WebhookResult(
            status="rejected", errors=[{"code": "git.invalid_repository", "message": str(exc)}]
        )
    except CloneOriginMismatch as exc:
        return WebhookResult(
            status="rejected", errors=[{"code": "git.origin_mismatch", "message": str(exc)}]
        )
    except CommitNotOnBranch as exc:
        return WebhookResult(
            status="rejected",
            errors=[{"code": "git.commit_not_on_branch", "message": str(exc)}],
        )
    except GitFlowError as exc:
        return WebhookResult(
            status="rejected", errors=[{"code": "git.archive_failed", "message": str(exc)}]
        )
    except bundles.UnsupportedArchive as exc:
        return WebhookResult(
            status="rejected", errors=[{"code": "bundle.unsupported", "message": str(exc)}]
        )
    except deploy.BundleTooLarge as exc:
        # The same code the `revalidate_stored_bundle` block below returns, so a
        # legacy over-cap bundle reports identically whichever of the two
        # bounds checks reaches it first.
        return _rejected(exc)
    except deploy.BundleInvalid as exc:
        return WebhookResult(status="rejected", errors=exc.errors)

    named = target_agent_name(targets, environment)
    named_elsewhere = (
        await crud_agents.get_agent_by_name(session, named)
        if named and not any(a.name == named for a in repo_agents)
        else None
    )
    try:
        agent = resolve_target_agent(targets, environment, repo_agents, named_elsewhere)
    except TargetUnresolved as exc:
        return WebhookResult(status="rejected", errors=[{"code": exc.code, "message": str(exc)}])
    if agent is None:
        # A branch this bundle declares no target for. Silently doing nothing is
        # correct and is what an unmatched branch already does.
        return WebhookResult(status="ignored")

    version = await crud_versions.get_version_by_commit(
        session, agent.id, after, created_by=GIT_FLOW_CREATED_BY
    )
    # Only a version whose bundle is actually stored may be reused for promote.
    # A row with bundle_ref still None is the residue of a prior attempt that
    # failed after the row committed; rebuild and store into it rather than
    # deploying a bundleless version. This same "new-or-repaired" condition
    # gates the eval fan-out below: a redelivered push for an already-bundled
    # version must not enqueue a second job for the same version.
    bundle_built = version is None or version.bundle_ref is None
    if bundle_built:
        # Bundle-once, bind-many (ADR-0091). A sibling agent in this repository
        # may already hold this exact commit -- the dev push that ran minutes
        # ago. Reuse its stored object rather than uploading the same bytes
        # again: prod then promotes not merely an identical artifact but the
        # SAME one, which is what makes "promote what you validated" a property
        # of the schema rather than of discipline. Resolved before anything is
        # written, because it decides WHICH object the route check below has to
        # read, and the lookup needs no version row of this agent's own.
        sibling = await _bundled_version_for_commit(
            session, repo_agents, after, exclude_agent_id=agent.id
        )
        if version is None:
            # The row, and only the row, precedes the refusal below: it is the
            # BUNDLE that must not be written yet (#2436). A bundleless row is
            # inert -- `_bundled_version_for_commit` returns only bundled rows,
            # the worker's boot join requires a `bundle_ref`
            # (`curie_worker.binding`), and `bundle_built` above stays true for
            # it -- so a refused push leaves exactly the residue a store that
            # failed after its row committed already leaves, and a repaired
            # redelivery still counts as a build. It is created here rather than
            # after the check because the refusal has to NAME a version, and
            # `deploy` builds the one message both envelopes render.
            version = await crud_versions.create_version_row(
                session,
                agent.id,
                version_label=after[:12],
                created_by=GIT_FLOW_CREATED_BY,
                commit_sha=after,
            )
        # The declared/bound approval-route join on the ATTACHMENT (#2436), run
        # on every delivery that builds a bundle and BEFORE that bundle is
        # written -- not, as it first landed, only when an active deployment
        # already referenced this version.
        #
        # WHY IT PRECEDES THE WRITE rather than being left to the deployment
        # gate below. `bundle_built` is `version.bundle_ref is None`, and it is
        # also what gates the eval fan-out: a version's eval-as-CI run happens
        # exactly once, on the delivery that builds its bundle. Both writers
        # here -- `crud.versions.attach_bundle` and `deploy.store_bundle` -- commit
        # immediately, so a refusal arriving after either of them spends that
        # one chance: the operator binds the missing route, redelivers the same
        # sha, `bundle_built` is now false because the bundle is already stored,
        # the deployment succeeds, and no eval ever runs for that version.
        # Refusing first leaves the version bundleless, so the repaired
        # redelivery is still a build and still fans out its eval, exactly once.
        #
        # Running it unconditionally also subsumes the "only when this version
        # is already live" form this gate first had: refusing whether or not an
        # active deployment points at the version is strictly the stronger of
        # the two, and it keeps an unbound bundle out of the object store rather
        # than merely off a deployment.
        #
        # Each branch checks the object it ACTUALLY attaches, and the two can
        # differ: the sibling branch attaches `sibling.bundle_ref`, which a
        # bundleless sibling version may have received as an API-uploaded gated
        # bundle while this delivery cloned the original, ungated commit.
        # Checking the in-hand archive there would pass on the ungated bytes and
        # then commit the gated reference. Neither branch re-clones; the stored
        # sibling object or the bytes already in hand are read (#1211).
        if sibling is not None:
            try:
                await deploy.check_approval_route_bindings(
                    store, sibling, agent.approval_routes, settings
                )
                # @spec ACTION-EXECUTOR-16: the sibling object is stored
                # bytes too, so it gets the custody re-check intake would give.
                await deploy.check_stored_sealing_key_custody(store, sibling, settings)
            except (
                deploy.BundleTooLarge,
                deploy.ApprovalRoutesUnbound,
                deploy.SealingKeyCustody,
            ) as exc:
                # `bundle.too_large` is live here, not theoretical: the sibling
                # object is a DIFFERENT object from the archive
                # `validate_archive` cleared moments ago, and on this path
                # nothing revalidates it -- `bundle_built` is true, so the
                # `revalidate_stored_bundle` block below is skipped. This gate's
                # extract is the only bounds check the attached object gets, so
                # an over-cap legacy sibling is refused here and is not attached
                # (ADR-0059 decision 3).
                return _rejected(exc, agent.id)
            version = await crud_versions.attach_bundle(
                session, version, str(sibling.bundle_ref), str(sibling.bundle_sha256)
            )
        else:
            # Unreachable when the archive came from the store: the pre-clone
            # lookup only returns a bundled version belonging to one of
            # `repo_agents`, so either that version IS this agent's (bundle_built
            # is False and this block does not run) or it is a sibling's and the
            # branch above attaches it. So `archive` here is always freshly
            # cloned, and `validate_archive` has always supplied the pair.
            assert extension is not None and content_type is not None
            try:
                await deploy.check_routes_from_bytes(
                    archive, agent.approval_routes, version.id, settings
                )
            except deploy.ApprovalRoutesUnbound as exc:
                return _rejected(exc, agent.id)
            await deploy.store_bundle(
                store, session, agent.id, version, archive, extension, content_type
            )

    # Either the version pre-existed with a bundle, or the block above created
    # or repaired it; it is non-None from here on.
    assert version is not None

    # A prod promote (bundle_built is False) reuses a bundle that may have been
    # stored before the current size/ratio caps existed, or under looser ones --
    # revalidate it here (ADR-0059 decision 3's backward-compat commitment). The
    # bundle_built branch above ran freshly CLONED bytes through
    # `deploy.validate_archive` under the current caps moments ago, so re-fetching
    # and rechecking the identical bytes here would be redundant.
    #
    # On the reuse path (#1211) `extract_stored_bundle` has already re-applied
    # the same caps to these bytes minutes earlier in this handler, so this call
    # repeats work. It is kept deliberately: this is the single documented home
    # of ADR-0059 decision 3's commitment, it also covers deliveries where the
    # earlier extract did not happen (a sibling attach, where the object the
    # version now points at is not the object `deploy.yaml` was read from), and
    # deleting it would make the commitment depend on the shape of an
    # optimization. Both routes report `bundle.too_large`.
    if not bundle_built:
        try:
            await deploy.revalidate_stored_bundle(store, version, settings)
            # @spec ACTION-EXECUTOR-16: a reused bundle stored before intake
            # refused a non-SecretRef sealing key declaration is refused here,
            # as `POST /deployments` refuses it.
            await deploy.check_stored_sealing_key_custody(store, version, settings)
        except (deploy.BundleTooLarge, deploy.SealingKeyCustody) as exc:
            return _rejected(exc, agent.id)

    # The declared/bound approval-route join at the moment the version becomes
    # the thing that boots (#2436), the push-side twin of the 422 that
    # `POST /deployments` returns. After the revalidation above, so an over-cap
    # legacy bundle still reports `bundle.too_large` first (ADR-0059 decision 3),
    # and after `resolve_target_agent`, so the map judged is the RESOLVED
    # agent's own: one repository builds several agents (ADR-0091), so a prod
    # promote is re-evaluated against the prod agent's bindings and never
    # inherits the dev pass. Reads the stored object, never a re-clone (#1211).
    #
    # REUSE PATH ONLY. This is where a prod promote, and a redelivery of a
    # version that already carries its bundle, are refused -- the deliveries
    # that write no bundle of their own and so never reach the pre-write gate
    # above. On the build path that gate has already judged the very object this
    # delivery attached (the sibling object, or the archive `store_bundle`
    # wrote), against this same resolved agent's map, so repeating the check
    # here would fetch and extract those bytes a second time to reach the
    # identical answer.
    if not bundle_built:
        try:
            await deploy.check_approval_route_bindings(
                store, version, agent.approval_routes, settings
            )
        except (deploy.BundleTooLarge, deploy.ApprovalRoutesUnbound) as exc:
            # `bundle.too_large` is unreachable here while
            # `revalidate_stored_bundle` above guards the same bytes under the
            # same caps, and is caught anyway so the ordering is a property of
            # this block rather than of that one: it stays ahead of
            # `approval_routes.unbound` on the reuse path (ADR-0059 decision 3)
            # even if the revalidation is ever moved or narrowed.
            return _rejected(exc, agent.id)

    deployment = await crud_deployments.create_deployment_row(
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


class TargetUnresolved(Exception):
    """A push cannot be routed to an agent, with an operator-facing reason."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def resolve_target_agent(
    targets: "DeployTargetsFile | None",
    environment: Environment,
    repo_agents: "Sequence[Agent]",
    named_elsewhere: "Agent | None",
) -> "Agent | None":
    """Which agent this push deploys to (ADR-0091).

    Returns None when the push should be ignored -- a branch with no matching
    target, exactly as an unmatched branch is ignored today. Raises
    ``TargetUnresolved`` when it should be REJECTED, which is a different thing:
    the author declared a target and it cannot be honoured, so silence would
    look like a deploy that worked.

    ``named_elsewhere`` is the agent the target names IF it exists but is bound
    to a different repository. That case is the sharpest edge in ADR-0091 and
    the reason this function takes an argument for it: without the check, one
    repository's push deploys over another repository's agent. It is refused.

    What decides the fallback is whether any target is DECLARED, not whether a
    ``deploy.yaml`` exists: a bundle with no such file and a bundle whose
    ``targets:`` map is empty say exactly the same thing about routing (#1210).
    """

    if targets is None or not targets.targets:
        # An empty map is what `curie init` scaffolds, so it is the ordinary
        # case rather than the legacy one (#1210). With several agents bound,
        # nothing says which, and guessing deploys to the wrong bot silently.
        if len(repo_agents) == 1:
            return repo_agents[0]
        missing = (
            "the bundle has no deploy.yaml"
            if targets is None
            else "the bundle's deploy.yaml declares an empty `targets:` map"
        )
        raise TargetUnresolved(
            "deploy.no_targets",
            f"{len(repo_agents)} agents are built from this repository but "
            f"{missing}, so there is nothing to say which one this branch "
            "deploys to. Declare a target (ADR-0089).",
        )

    matching = [
        (key, target)
        for key, target in targets.targets.items()
        if target.env == environment.value
    ]
    agent_names: list[str] = []
    for key, target in matching:
        if target.agent is None:
            raise TargetUnresolved(
                "deploy.missing_agent",
                f"targets.{key}: agent is required for every declared target",
            )
        agent_names.append(target.agent)
    if not matching:
        # Not an error: a repository may deploy only prod from main and leave
        # dev to the CLI. Ignoring matches how an unmatched branch behaves.
        return None
    if len(matching) > 1:
        raise TargetUnresolved(
            "deploy.ambiguous_env",
            f"deploy.yaml declares {len(matching)} targets with env "
            f"{environment.value!r} ({', '.join(sorted(agent_names))}); "
            "one branch cannot deploy to two agents in one push.",
        )

    wanted = agent_names[0]
    for agent in repo_agents:
        if agent.name == wanted:
            return agent

    if named_elsewhere is not None:
        # One repository's push would otherwise deploy over another's agent.
        raise TargetUnresolved(
            "deploy.agent_bound_elsewhere",
            f"deploy.yaml names agent {wanted!r}, which is built from a "
            "different repository. Refusing: a push must not deploy over an "
            "agent another repository owns.",
        )
    raise TargetUnresolved(
        "deploy.unknown_agent",
        f"deploy.yaml names agent {wanted!r}, which does not exist. Create it "
        "once (`curie cluster deploy`, or the console) so the push has "
        "something to deploy to; a webhook does not mint agents.",
    )


class _BundleExtract(Protocol):
    """The bounded-extract call shape the two ``deploy.yaml`` readers share."""

    def __call__(
        self,
        data: bytes,
        dest: Path,
        *,
        max_uncompressed_bytes: int,
        max_compression_ratio: float,
        max_members: int,
    ) -> object: ...


def _read_deploy_targets(
    extract: _BundleExtract, archive: bytes, settings: Settings
) -> DeployTargetsFile | None:
    """Extract ``archive`` under the operator's caps and read its ``deploy.yaml``.

    The three caps are security-relevant (ADR-0059 decision 3), so the argument
    list lives HERE, once: the two wrappers below differ only in which extract
    they hand in, and a change to the caps cannot reach one reader and miss the
    other. Extraction to a temp dir is the only way to read one file out of a
    bundle, and it is blocking, so callers run the wrappers in a threadpool.

    Read the wrapper you were sent from, not this helper, for what the choice of
    ``extract`` means: ``_read_stored_targets``'s docstring carries the
    extract-without-revalidate argument in full.
    """

    with tempfile.TemporaryDirectory() as tmp:
        extract(
            archive,
            Path(tmp),
            max_uncompressed_bytes=settings.bundle_max_uncompressed_bytes,
            max_compression_ratio=settings.bundle_max_compression_ratio,
            max_members=settings.bundle_max_members,
        )
        return bundles.read_deploy_targets(Path(tmp))


def _read_targets(archive: bytes, settings: Settings) -> DeployTargetsFile | None:
    """Read ``deploy.yaml`` out of an already-validated archive.

    Extracted to a temp dir because that is the only way to read one file out
    of the bundle, and run in a threadpool by the caller since it is blocking.
    """

    return _read_deploy_targets(bundles.extract_and_validate, archive, settings)


def _read_stored_targets(archive: bytes, settings: Settings) -> DeployTargetsFile | None:
    """Read ``deploy.yaml`` out of a bundle fetched from the object store.

    THIS PATH STILL EXTRACTS. It extracts the whole archive to a temp dir, under
    exactly the same configured uncompressed-size, compression-ratio and
    member-count caps `_read_targets` uses -- that is the only way to read one
    file out of a bundle, and it is blocking, so the caller runs it in a
    threadpool. What it does NOT do is re-run ``detect_format`` and
    ``validate_bundle``: these bytes came from the immutable, write-once
    storage key, where they passed ``validate_bundle`` on the delivery that
    stored them, and they are the object this delivery deploys either way.

    #1211's acceptance criterion asks for "zero extractions" on a promote. That
    is, precisely, a zero of ``bundles.extract_and_validate`` calls -- which is
    the honest measure here, because ``extract_and_validate`` is the composite
    (detect + extract + validate) whose second and third parts are the redundant
    work, and because it is the function the issue's own instrumentation
    counted. A reader who sees "zero extractions" in a test name and then finds
    a ``safe_extract`` call underneath this function has found the right thing,
    not a lie: the extraction is bounded and unavoidable, the re-validation is
    what went away.

    Raises ``bundles.UnsupportedArchive`` when the current caps refuse bytes
    stored under looser ones; ``process_push`` maps that to
    ``deploy.BundleTooLarge`` so ADR-0059 decision 3's operator-facing code
    survives the reorder.
    """

    return _read_deploy_targets(bundles.extract_stored_bundle, archive, settings)


def target_agent_name(targets: DeployTargetsFile | None, environment: Environment) -> str | None:
    """The agent name this environment's target names, if exactly one does."""

    if targets is None:
        return None
    matching = [t for t in targets.targets.values() if t.env == environment.value]
    return matching[0].agent if len(matching) == 1 else None


async def _bundled_version_for_commit(
    session: AsyncSession,
    repo_agents: "Sequence[Agent]",
    commit_sha: str,
    *,
    exclude_agent_id: uuid.UUID | None = None,
) -> "AgentVersion | None":
    """A version of this repository that already has this commit's bundle stored.

    Two questions, one predicate, on purpose. With ``exclude_agent_id`` it is
    the ADR-0091 bundle-once/bind-many sibling reuse (#1194): has ANOTHER agent
    of this repository already stored this commit, so the target agent can bind
    the same object instead of uploading the bytes again? Without it, it is the
    pre-clone check (#1211): has ANYTHING in this repository already built and
    stored this exact commit, so this delivery needs no remote at all?

    They are the same question with a different exclusion, and #1211 is exactly
    what happens when they drift -- the lookup already existed, it just ran
    after the clone it could have avoided. Keeping one function means the two
    can never disagree about what "already bundled" means.

    ``repo_agents`` arrives in ``Agent.name`` order (``crud.agents.get_agents_by_repo``
    orders it), so the version returned for a repository is deterministic.

    Note the truthiness test on ``bundle_ref``, not ``is not None``: a row whose
    bundle_ref is NULL is the residue of a prior attempt that failed after the
    row committed (d3a0f4b8) and must be rebuilt, and an empty-string ref would
    otherwise sail into ``store.get("")``.
    """

    for candidate in repo_agents:
        if exclude_agent_id is not None and candidate.id == exclude_agent_id:
            continue
        existing = await crud_versions.get_version_by_commit(
            session, candidate.id, commit_sha, created_by=GIT_FLOW_CREATED_BY
        )
        if existing is not None and existing.bundle_ref:
            return existing
    return None
