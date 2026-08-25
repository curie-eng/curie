"""Notice new commits without being told (issue #1239).

Curie is self-hosted: adopters run it in their own cluster, and many of those
clusters accept no inbound traffic. A GitHub webhook cannot reach such a
cluster, so today those installs have no push-to-deploy at all -- not a
degraded one, none.

Outbound always works. So this asks GitHub, on an interval, whether the deploy
branches moved, and runs the ordinary deploy when they have.

**Which branches: the platform's two, not the bundle's targets.** It polls
``settings.dev_branch`` and ``settings.prod_branch`` for every bound repository
-- the same two names ``environment_for_ref`` maps on the webhook path, so both
lanes watch exactly the same refs. It does NOT read each bundle's
``deploy.yaml`` to decide what to watch; ``deploy.yaml`` still decides which
AGENT a push deploys to, downstream, inside ``process_push``. An earlier
version of this docstring claimed otherwise (#1264).

**It does not reimplement deploying.** It synthesizes the same push payload the
webhook would have delivered and hands it to ``gitflow.process_push``. Two
deploy paths that could disagree about what a push means would be worse than
one path that sometimes runs late -- and the webhook remains the fast path
wherever it can reach.

Three properties worth stating, because each is a way this could go wrong:

- **It polls per REPOSITORY, not per agent.** Several agents share one
  repository (ADR-0091), so per-agent polling would make N identical API calls
  and race N deploys of the same commit against each other.
- **It never redeploys a commit already deployed by Git flow.** Only Git flow
  authored versions establish this baseline. A CLI deployment at the same SHA
  must not suppress the build, evaluation, and deployment owned by Git flow.
- **One failing repository does not stop the others.** A repo whose credential
  has been revoked, or that has been deleted, must not silently halt polling
  for every other agent on the cluster.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import httpx
from curie_telemetry import operation_span, record_metric
from opentelemetry.trace import SpanKind, StatusCode

from .config import Settings
from .models import GIT_FLOW_CREATED_BY

logger = logging.getLogger(__name__)


class BranchTip(Protocol):
    """Reads the current sha of a branch. Narrow so tests need no HTTP."""

    def sha_for(self, repo_full_name: str, branch: str) -> str | None: ...


@dataclass(frozen=True)
class PollTarget:
    """One repository and the branches worth watching on it."""

    repo_full_name: str
    clone_url: str
    branches: tuple[str, ...]


@dataclass(frozen=True)
class Move:
    """A branch that has moved to a commit we have not deployed."""

    repo_full_name: str
    clone_url: str
    branch: str
    sha: str

    def as_push_payload(self) -> dict[str, Any]:
        """The webhook payload this would have arrived as.

        Deliberately the same shape `process_push` already parses, so polling
        and the webhook cannot diverge on what a push means. The clone_url is
        the one derived from configuration, which is also what the origin check
        compares against -- so a polled deploy passes that check by
        construction rather than by coincidence.
        """

        return {
            "ref": f"refs/heads/{self.branch}",
            "after": self.sha,
            "repository": {"full_name": self.repo_full_name, "clone_url": self.clone_url},
        }


def moves_to_deploy(
    targets: Sequence[PollTarget],
    tips: BranchTip,
    already_deployed: dict[tuple[str, str], str],
) -> list[Move]:
    """Which branches have moved since we last deployed them.

    Pure: no HTTP, no database. ``already_deployed`` maps
    ``(repo_full_name, branch)`` to the sha last deployed from it.
    """

    moves: list[Move] = []
    for target in targets:
        for branch in target.branches:
            try:
                sha = tips.sha_for(target.repo_full_name, branch)
            except Exception as exc:
                # One unreachable or unauthorized repository must not stop the
                # rest. Logged per repo/branch so the cause is attributable.
                logger.warning(
                    "commit poll failed repo=%s branch=%s: %s",
                    target.repo_full_name,
                    branch,
                    exc,
                )
                continue
            if not sha:
                continue
            if already_deployed.get((target.repo_full_name, branch)) == sha:
                continue
            moves.append(
                Move(
                    repo_full_name=target.repo_full_name,
                    clone_url=target.clone_url,
                    branch=branch,
                    sha=sha,
                )
            )
    return moves


class RateLimited(RuntimeError):
    """GitHub asked us to wait. Carries when it is worth asking again."""

    def __init__(self, repo_full_name: str, retry_after_s: float) -> None:
        super().__init__(f"{repo_full_name}: rate limited, retry in {retry_after_s:.0f}s")
        self.repo_full_name = repo_full_name
        self.retry_after_s = retry_after_s


# How many consecutive throttled passes before this stops reading like a blip.
# A single 429 is routine; a repository throttled this many rounds running is a
# deploy lane that has silently stopped, and must be findable as one (#1269).
_SUSTAINED_THROTTLE_ROUNDS = 3


class GitHubBranchTip:
    """Reads a branch tip from the GitHub API, using the platform credential.

    Honours GitHub's throttling rather than re-asking every interval (#1269).
    An unauthenticated caller gets 60 requests/hour, so a handful of
    repositories on a 60s interval exhausts the budget in minutes -- and the
    naive reaction, asking again next tick, is what turns a brief throttle into
    a permanent one.
    """

    def __init__(self, settings: Settings, credentials: Any, timeout: float = 15.0) -> None:
        self._settings = settings
        self._credentials = credentials
        self._timeout = timeout
        # Per repository: when it is worth asking again, and how many rounds in
        # a row it has been throttled.
        self._retry_at: dict[str, float] = {}
        self._throttled_rounds: dict[str, int] = {}

    @staticmethod
    def _retry_after_seconds(response: httpx.Response) -> float:
        """Seconds to wait, from GitHub's documented throttling headers.

        GitHub sends `Retry-After` (seconds) on a secondary-rate-limit 403/429,
        and on a primary limit sends `x-ratelimit-remaining: 0` with
        `x-ratelimit-reset` as a UTC epoch second. Both are documented under
        "Rate limits for the REST API"; this reads whichever is present.
        """

        raw = response.headers.get("retry-after")
        if raw:
            try:
                return max(0.0, float(raw))
            except ValueError:
                pass
        if response.headers.get("x-ratelimit-remaining") == "0":
            reset = response.headers.get("x-ratelimit-reset")
            if reset:
                try:
                    return max(0.0, float(reset) - time.time())
                except ValueError:
                    pass
        # Throttled but unparseable: wait a bounded default rather than
        # hammering, and rather than backing off forever on a bad header.
        return 60.0

    def sha_for(self, repo_full_name: str, branch: str) -> str | None:
        now = time.time()
        wait_until = self._retry_at.get(repo_full_name, 0.0)
        if now < wait_until:
            # Still inside the window GitHub asked for. Skipping quietly is the
            # point: re-requesting is what extends a throttle.
            raise RateLimited(repo_full_name, wait_until - now)

        token = self._credentials.token_for(repo_full_name)
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        url = f"{self._settings.github_api_url.rstrip('/')}/repos/{repo_full_name}/commits/{branch}"
        with httpx.Client(timeout=self._timeout) as client:
            response = client.get(url, headers=headers)
        if response.status_code in (403, 429):
            # Not `or 60.0`: `Retry-After: 0` is a legitimate "ask again now"
            # and is falsy, so that idiom silently turns it into a minute --
            # and the resulting window then suppresses the round counting
            # below. Caught by the sustained-throttling test.
            delay = self._retry_after_seconds(response)
            self._retry_at[repo_full_name] = time.time() + delay
            rounds = self._throttled_rounds.get(repo_full_name, 0) + 1
            self._throttled_rounds[repo_full_name] = rounds
            if rounds >= _SUSTAINED_THROTTLE_ROUNDS:
                # Per-branch warnings read as transient. This one says the lane
                # has stopped, which is what an operator needs to find (#1269).
                logger.error(
                    "commit poll throttled by GitHub for %d consecutive rounds repo=%s; "
                    "deploys from this repository are NOT happening. Configure a GitHub "
                    "App or token -- an unauthenticated caller gets 60 requests/hour.",
                    rounds,
                    repo_full_name,
                )
            raise RateLimited(repo_full_name, delay)

        self._throttled_rounds.pop(repo_full_name, None)
        self._retry_at.pop(repo_full_name, None)
        if response.status_code == 404:
            # A branch a deploy.yaml names but the repository does not have is
            # normal -- a repo may deploy only prod from main. Not an error.
            return None
        response.raise_for_status()
        sha = response.json().get("sha")
        return str(sha) if isinstance(sha, str) else None


# The last Git flow authored version commit deployed per (repository, environment).
# Only a Git flow authored version with recorded deployment provenance settles
# this baseline. The authoritative value still comes from the version, so a
# mutable deployment value cannot redefine it. Environment rather than branch
# because that is what a Deployment records; the caller maps environments back
# to branch names via Settings, the same mapping `environment_for_ref` uses in
# the other direction.
_DEPLOYED_SQL = """
SELECT DISTINCT ON (a.repo_full_name, d.environment)
       a.repo_full_name AS repo_full_name,
       d.environment    AS environment,
       v.commit_sha     AS commit_sha
FROM {schema}.deployments d
JOIN {schema}.agents a ON a.id = d.agent_id
JOIN {schema}.agent_versions v ON v.id = d.version_id
WHERE a.repo_full_name IS NOT NULL
  AND d.commit_sha IS NOT NULL
  AND v.created_by = :git_flow_created_by
ORDER BY a.repo_full_name, d.environment, d.deployed_at DESC
"""

# These rejections depend on which agents are bound to the repository. They
# settle against that binding snapshot, then get one new attempt when an
# operator changes it without pushing a new commit (#1307).
_TOPOLOGY_REJECTIONS = frozenset(
    {
        "deploy.agent_bound_elsewhere",
        "deploy.no_targets",
        "deploy.unknown_agent",
    }
)
_ARCHIVE_FAILURE = "git.archive_failed"


# Every repository binding. Grouping these rows produces both the unique poll
# targets and the routing snapshot used to reopen a settled topology rejection.
_BINDINGS_SQL = """
SELECT repo_full_name, name
FROM {schema}.agents
WHERE repo_full_name IS NOT NULL AND repo_full_name <> ''
ORDER BY repo_full_name, name
"""


@dataclass(frozen=True)
class Settled:
    """A terminal sha, optionally only for one routing topology."""

    sha: str
    bindings: tuple[str, ...] | None


class CommitPoller:
    """Asks GitHub whether the deploy branches moved, and deploys when they did.

    Runs in the API rather than the worker because the deploy path, the bundle
    store and the credential resolver all already live here. Its only job is to
    notice; ``gitflow.process_push`` does the deploying.
    """

    def __init__(
        self,
        *,
        session_factory: Any,
        store: Any,
        settings: Settings,
        eval_queue: Any,
        tips: BranchTip,
        interval_seconds: float,
    ) -> None:
        self._session_factory = session_factory
        self._store = store
        self._settings = settings
        self._eval_queue = eval_queue
        self._tips = tips
        self._interval = interval_seconds
        # The database records successes only. Intrinsic failures settle for
        # the sha, while routing failures settle until the repository binding
        # snapshot changes. In memory only, so restart can retry once.
        self._settled: dict[tuple[str, str], Settled] = {}
        # Archive failure gets one retry on the next pass, then settles. This
        # bounds network failure clones without making a single blip terminal.
        self._archive_retry: dict[tuple[str, str], str] = {}
        self._last_success_monotonic: float | None = None

    async def run_forever(self) -> None:
        logger.info("commit poller started interval=%ss", self._interval)
        while True:
            try:
                await self.poll_once()
            # No `except CancelledError: raise` here, deliberately.
            # CancelledError derives from BaseException, not Exception, so the
            # handler below never catches it and cancellation propagates on its
            # own. The clause that used to sit here was dead code that only
            # looked protective -- and a mutation replacing its `raise` with
            # `continue` was a real shutdown hang that no test could kill,
            # because a task ignoring cancellation cannot be stopped (#1263).
            except Exception:
                # A poll pass must never kill the loop: the next one may well
                # succeed, and a dead poller on an unreachable cluster means no
                # deploys at all with nothing saying so.
                logger.exception("commit poll pass failed; continuing")
            await asyncio.sleep(self._interval)

    async def poll_once(self) -> list[Move]:
        """Run one measured poll pass without changing its returned moves."""

        moves: list[Move] = []
        error: Exception | None = None
        attributes = {
            "service.name": "curie-api",
            "operation": "commit-poller",
            "role": "background",
        }
        with operation_span(
            "curie.background.commit-poller",
            kind=SpanKind.INTERNAL,
            attributes=attributes,
        ) as span:
            try:
                moves = await self._poll_once()
            except Exception as exc:
                error = exc
                if hasattr(span, "set_status"):
                    span.set_status(StatusCode.ERROR)
                span.add_event(
                    "background.pass.failed",
                    {"outcome": "failure", "error.class": type(exc).__name__},
                )
            else:
                span.add_event("background.pass.completed", {"outcome": "success"})
        outcome = "failure" if error is not None else "success"
        record_metric(
            "curie.background.loop",
            attributes={**attributes, "outcome": outcome},
        )
        now = time.monotonic()
        if error is None:
            self._last_success_monotonic = now
        if self._last_success_monotonic is not None:
            record_metric(
                "curie.background.last_success.age",
                max(0.0, now - self._last_success_monotonic),
                attributes=attributes,
            )
        if error is not None:
            raise error
        return moves

    async def _poll_once(self) -> list[Move]:
        from sqlalchemy import text

        from . import gitflow

        schema = self._settings.db_schema
        branch_for_env = {
            "dev": self._settings.dev_branch,
            "prod": self._settings.prod_branch,
        }

        async with self._session_factory() as session:
            bindings: dict[str, list[str]] = {}
            for repo, agent_name in await session.execute(
                text(_BINDINGS_SQL.format(schema=schema))
            ):
                bindings.setdefault(str(repo), []).append(str(agent_name))
            deployed: dict[tuple[str, str], str] = {}
            deployed_stmt = text(_DEPLOYED_SQL.format(schema=schema)).bindparams(
                git_flow_created_by=GIT_FLOW_CREATED_BY
            )
            for repo, env, sha in await session.execute(deployed_stmt):
                branch = branch_for_env.get(str(env))
                if branch:
                    deployed[(str(repo), branch)] = str(sha)

        binding_snapshots = {repo: tuple(names) for repo, names in bindings.items()}

        targets = [
            PollTarget(
                repo_full_name=repo,
                clone_url=gitflow.trusted_clone_url(repo, self._settings),
                branches=tuple(branch_for_env.values()),
            )
            for repo in bindings
        ]
        # to_thread because the tip reader is sync httpx: a blocking call in
        # the event loop would stall every request the API is serving.
        moves: list[Move] = await asyncio.to_thread(moves_to_deploy, targets, self._tips, deployed)
        # Drop anything that already settled without producing a Deployment.
        # This must happen BEFORE process_push, because the mirror clone is
        # inside it -- at the recommended 60s interval an unchanged
        # non-deploying branch is roughly 1,440 full clones a day (#1267).
        unsettled: list[Move] = []
        for move in moves:
            settled = self._settled.get((move.repo_full_name, move.branch))
            if settled is None or settled.sha != move.sha:
                unsettled.append(move)
                continue
            if (
                settled.bindings is not None
                and settled.bindings != binding_snapshots[move.repo_full_name]
            ):
                unsettled.append(move)
        moves = unsettled

        for move in moves:
            key = (move.repo_full_name, move.branch)
            async with self._session_factory() as session:
                result = await gitflow.process_push(
                    session, self._store, self._settings, self._eval_queue, move.as_push_payload()
                )
            # Shared with the webhook lane, not copied (#1268). Reporting a
            # rejection as "deployed" at INFO is #1066 again, and this is the
            # lane with no GitHub delivery UI to fall back on.
            gitflow.log_push_outcome(result, move.as_push_payload(), source="commit poll")

            if result.status in ("deployed", "promoted"):
                # A Deployment row exists now, so the database is the memory
                # and this must not keep a second copy that a rollback would
                # not clear.
                self._settled.pop(key, None)
                self._archive_retry.pop(key, None)
            elif result.status == "rejected":
                codes = {(error.get("code") or "") for error in (result.errors or [])}
                if codes & _TOPOLOGY_REJECTIONS:
                    self._settled[key] = Settled(move.sha, binding_snapshots[move.repo_full_name])
                    self._archive_retry.pop(key, None)
                elif _ARCHIVE_FAILURE in codes and self._archive_retry.get(key) != move.sha:
                    self._archive_retry[key] = move.sha
                    logger.info(
                        "commit poll will retry repo=%s branch=%s sha=%s (transient)",
                        move.repo_full_name,
                        move.branch,
                        move.sha[:8],
                    )
                else:
                    self._settled[key] = Settled(move.sha, None)
                    self._archive_retry.pop(key, None)
            else:
                # An ignored outcome will keep repeating for this commit.
                # Remember it, or the next pass clones again (#1267).
                self._settled[key] = Settled(move.sha, None)
                self._archive_retry.pop(key, None)

            if result.status != "rejected":
                logger.info(
                    "commit poll deployed repo=%s branch=%s sha=%s status=%s",
                    move.repo_full_name,
                    move.branch,
                    move.sha[:8],
                    result.status,
                )
        return moves
