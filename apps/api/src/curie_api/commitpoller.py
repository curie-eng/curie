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

Four properties worth stating, because each is a way this could go wrong:

- **It polls per REPOSITORY, not per agent.** Several agents share one
  repository (ADR-0091), so per-agent polling would make N identical API calls
  and race N deploys of the same commit against each other.
- **It never redeploys a commit already deployed by Git flow.** Only Git flow
  authored versions establish this baseline. A CLI deployment at the same SHA
  must not suppress the build, evaluation, and deployment owned by Git flow.
- **One failing repository does not stop the others.** A repo whose credential
  has been revoked, or that has been deleted, must not silently halt polling
  for every other agent on the cluster.
- **A clone that keeps failing backs off instead of retrying every interval.**
  A repository that is unreachable, unauthorized, or too large to clone inside
  the 120s timeout fails identically on every pass, so retrying at the interval
  is roughly 1,440 full clones a day for one unchanged sha. Repeated failures
  of the same class for the same sha wait geometrically longer, to an hourly
  ceiling, and the stalled lane is reported rather than left silent (#1309).
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import httpx
from curie_telemetry import operation_span, record_metric
from opentelemetry.trace import SpanKind, StatusCode

from curie_api.schemas.deployments import WebhookResult

from .code_host_trust import code_host_verify
from .config import Settings
from .models import GIT_FLOW_CREATED_BY
from .repo_full_name import InvalidRepoFullName, repo_url_path

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
            except Exception as exc:  # noqa: BLE001 - existing broad catch retained
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
        repository_path = repo_url_path(repo_full_name)
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
        url = (
            f"{self._settings.github_api_url.rstrip('/')}"
            f"/repos/{repository_path}/commits/{branch}"
        )
        with httpx.Client(
            timeout=self._timeout, verify=code_host_verify(self._settings)
        ) as client:
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

# A rejection lands in one of three tiers, and the tier is what decides whether
# the next pass pays for another full mirror clone.
#
# Tier one, TOPOLOGY: these depend on which agents are bound to the repository.
# They settle against that binding snapshot, then get one new attempt when an
# operator changes it without pushing a new commit (#1307).
_TOPOLOGY_REJECTIONS = frozenset(
    {
        "deploy.agent_bound_elsewhere",
        "deploy.no_targets",
        "deploy.unknown_agent",
    }
)
_ARCHIVE_FAILURE = "git.archive_failed"
# Tier two, RETRYABLE: this one says nothing about the commit. The same code
# covers a subprocess error and both 120s timeouts, so it means anything from a
# network blip to a repository that is permanently unreachable, unauthorized,
# missing its clone credential, or too large to clone inside the timeout. It
# must not be terminal, and it must not be retried every interval either -- it
# earns another clone on a capped geometric backoff (#1309).
_RETRYABLE_REJECTIONS = frozenset({_ARCHIVE_FAILURE})
#
# Tier three, EVERYTHING ELSE: an ambiguous environment or an invalid bundle is
# fixed only by pushing a new commit, so those settle for the sha and re-cloning
# would prove nothing.

# The first wait after a retryable failure. Five minutes turns the worst case --
# a repository broken all day on a 60s interval -- from ~1,440 clone attempts
# into 24, while still recovering from a genuine blip inside one pass of an
# operator's attention (#1309).
_RETRY_BASE_DELAY_S = 300.0
# The ceiling on that geometric growth: 5m, 10m, 20m, 40m, then an hour
# forever. Capping rather than settling is deliberate. A private repository
# whose clone credential arrives an hour after the binding was made is exactly
# the shape `docs/operations.md` documents, and a terminal state would strand it
# until someone pushed a new commit or restarted the API. The bound here is on
# the RATE of clones; the stalled lane is made findable by the error below
# instead of by silence.
_RETRY_MAX_DELAY_S = 3600.0
# The same ceiling, expressed as a bound on the EXPONENT, because clamping only
# the product is too late. `2 ** (attempts - 1)` is evaluated in full before
# `min` ever sees it, and at attempt 1,025 that float multiplication raises
# OverflowError -- a repository failing every attempt reaches 1,025 in roughly
# 43 days at the hourly ceiling, well inside the life of an API pod. The
# exception would escape `poll_once` BEFORE the new record is stored, so the
# already-expired record survives and every following pass clones again and
# crashes again: #1309 restored, on exactly the repository this feature exists
# for. Derived from the two delays rather than written down, so it cannot
# silently drift if they change (4 for the current constants -- 300s doubled
# four times is 4,800s, already past the ceiling).
_RETRY_MAX_DOUBLINGS = math.ceil(math.log2(_RETRY_MAX_DELAY_S / _RETRY_BASE_DELAY_S))
# How many consecutive failures before this stops reading like a blip -- the
# same judgement as _SUSTAINED_THROTTLE_ROUNDS above. By the third identical
# failure the deploy lane has stopped, and an operator needs to find it as that
# rather than as one more per-pass INFO line (#1309).
_RETRY_ERROR_ROUNDS = 3
# The failure class of an exception that ESCAPED process_push rather than being
# mapped to a rejection (#3736). A rejection's codes name the failure; an escape
# has none, so the backoff record needs a stable stand-in for the same
# "same sha AND same failure class continues the schedule" rule. Chosen to be
# visibly not a rejection code, since no `process_push` result ever carries it.
_PUSH_RAISED = "commitpoller.process_push_raised"


@dataclass(frozen=True)
class ArchiveBackoff:
    """A retryable rejection that has not yet earned another clone."""

    sha: str
    # The failure class: the rejection's sorted error codes. A different class
    # is a different failure, so it restarts the schedule rather than inheriting
    # the previous one's delay.
    codes: tuple[str, ...]
    attempts: int
    next_attempt_at: float  # monotonic seconds
    # What the deployments query reported for this branch when the failure was
    # recorded. A CHANGE in it means a deploy landed since -- from either lane
    # -- and this record is stale (see the prune in `poll_once`).
    deployed_sha: str | None


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
        notice_queue: Any | None = None,
        tips: BranchTip,
        interval_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._session_factory = session_factory
        self._store = store
        self._settings = settings
        self._eval_queue = eval_queue
        self._notice_queue = notice_queue
        self._tips = tips
        self._interval = interval_seconds
        # Monotonic, not wall clock: a backoff window must not be skipped or
        # extended by an NTP correction. Injectable so the schedule can be
        # asserted without a test that sleeps for an hour.
        self._clock = clock
        # The database records successes only. Intrinsic failures settle for
        # the sha, while routing failures settle until the repository binding
        # snapshot changes. In memory only, so restart can retry once.
        self._settled: dict[tuple[str, str], Settled] = {}
        self._last_success_monotonic: float | None = None
        # A retryable rejection is neither forgotten nor terminal: it waits a
        # geometrically growing, capped delay before earning another clone
        # (#1309). Keyed the same way as _settled, per (repository, branch).
        self._archive_backoff: dict[tuple[str, str], ArchiveBackoff] = {}
        # Only for a Postgres write failure AFTER process_push has settled.
        # Ordinary Valkey failures are already durable in deploy_notice_outbox.
        self._pending_notices: dict[
            tuple[str, str, str], tuple[WebhookResult, dict[str, object]]
        ] = {}

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
            except Exception as exc:  # noqa: BLE001 - existing broad catch retained
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

        # A successful deployment disappears from moves_to_deploy on the next
        # pass. Retry any notice whose SQL outbox insert failed before checking
        # branch tips, or that success would erase its last in-process chance.
        if self._notice_queue is not None:
            for pending_key, (pending_result, pending_payload) in list(
                self._pending_notices.items()
            ):
                try:
                    async with self._session_factory() as notice_session:
                        await self._notice_queue.publish(
                            notice_session, pending_result, pending_payload, self._settings
                        )
                except Exception:
                    logger.exception("could not persist pending git-flow deploy notice")
                else:
                    self._pending_notices.pop(pending_key, None)

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

        # A deployment on the branch retires the failure record, whichever lane
        # produced it. The webhook lane deploys through the same `process_push`
        # without going near the poller, so the success-clearing branch below
        # never runs for a commit it handled -- and the stale record would then
        # survive a rollback to the failed sha and suppress redeploying it, even
        # though the successful deploy is direct evidence the repository is
        # clonable again (AC4 of #1309).
        #
        # The comparison is against the sha captured AT RECORD TIME, not against
        # `move.sha`: a branch normally has an older deployed sha than its tip,
        # so comparing to the tip would prune on the very first failure and
        # disable the backoff entirely. It is the change in the branch's last
        # successful deploy that means one landed since.
        for key, record in list(self._archive_backoff.items()):
            if deployed.get(key) != record.deployed_sha:
                del self._archive_backoff[key]

        targets: list[PollTarget] = []
        for repo in bindings:
            try:
                clone_url = gitflow.trusted_clone_url(repo, self._settings)
            except InvalidRepoFullName as exc:
                logger.warning(
                    "commit poll skipping invalid repository binding repo=%r: %s",
                    repo,
                    exc,
                )
                continue
            targets.append(
                PollTarget(
                    repo_full_name=repo,
                    clone_url=clone_url,
                    branches=tuple(branch_for_env.values()),
                )
            )
        # to_thread because the tip reader is sync httpx: a blocking call in
        # the event loop would stall every request the API is serving.
        moves: list[Move] = await asyncio.to_thread(moves_to_deploy, targets, self._tips, deployed)
        # Drop anything that already settled without producing a Deployment.
        # This must happen BEFORE process_push, because the mirror clone is
        # inside it -- at the recommended 60s interval an unchanged
        # non-deploying branch is roughly 1,440 full clones a day (#1267).
        unsettled: list[Move] = []
        now = self._clock()
        for move in moves:
            key = (move.repo_full_name, move.branch)
            backoff = self._archive_backoff.get(key)
            if backoff is not None:
                if backoff.sha != move.sha:
                    # A record for a different sha suppresses nothing -- a new
                    # commit is a new chance, and may be the fix -- and it is
                    # dropped rather than merely ignored, because it is state
                    # about a commit that is no longer the branch tip (AC4 of
                    # #1309).
                    del self._archive_backoff[key]
                elif now < backoff.next_attempt_at:
                    # Still inside the window this repository's last failure
                    # earned. It has to be checked HERE for the same reason the
                    # settled check is: the mirror clone lives inside
                    # process_push, so a skip decided any later has already paid
                    # for it (#1309).
                    continue
            settled = self._settled.get(key)
            if settled is None or settled.sha != move.sha:
                unsettled.append(move)
                continue
            if (
                settled.bindings is not None
                and settled.bindings != binding_snapshots[move.repo_full_name]
            ):
                unsettled.append(move)
        moves = unsettled

        # A move whose deploy RAISES must not stop the moves behind it (#3736).
        # `process_push` maps git, archive and bundle validation errors to a
        # rejected result, but its later steps -- a stored bundle's object read,
        # the bundle store write, the revalidation, database writes, the eval
        # enqueue -- are unguarded. The first such failure is held here and
        # re-raised AFTER the loop, so the remaining moves still deploy in this
        # pass while `poll_once` still records the pass as a failure rather than
        # letting a broken lane look like a clean one.
        pass_failure: Exception | None = None
        for move in moves:
            key = (move.repo_full_name, move.branch)
            async with self._session_factory() as session:
                # No `except CancelledError: raise`, deliberately -- the same
                # structural guarantee `run_forever` documents: CancelledError
                # derives from BaseException, so `except Exception` below cannot
                # catch it and cancellation propagates on its own.
                try:
                    result = await gitflow.process_push(
                        session,
                        self._store,
                        self._settings,
                        self._eval_queue,
                        move.as_push_payload(),
                    )
                except Exception as exc:
                    # The mirror clone lives inside process_push, so an escape
                    # must earn the same capped backoff a retryable rejection
                    # does (#1309) -- otherwise the next pass re clones it
                    # first and blocks these same repositories again. The
                    # record is a statement about the attempt, not the sha:
                    # `_settled` is not written, so recovery stays possible.
                    logger.exception(
                        "commit poll deploy raised repo=%s branch=%s sha=%s: %s",
                        move.repo_full_name,
                        move.branch,
                        move.sha[:8],
                        exc,
                    )
                    self._record_retryable_failure(move, {_PUSH_RAISED}, deployed.get(key))
                    if pass_failure is None:
                        pass_failure = exc
                    continue
            # Shared with the webhook lane, not copied (#1268). Reporting a
            # rejection as "deployed" at INFO is #1066 again, and this is the
            # lane with no GitHub delivery UI to fall back on.
            gitflow.log_push_outcome(result, move.as_push_payload(), source="commit poll")
            if self._notice_queue is not None:
                notice_payload = move.as_push_payload()
                notice_key = (move.repo_full_name, move.branch, move.sha)
                try:
                    async with self._session_factory() as notice_session:
                        await self._notice_queue.publish(
                            notice_session, result, notice_payload, self._settings
                        )
                except Exception:
                    self._pending_notices[notice_key] = (result, notice_payload)
                    logger.exception("could not persist git-flow deploy notice from commit poll")
                else:
                    self._pending_notices.pop(notice_key, None)

            if result.status in ("deployed", "promoted"):
                # A Deployment row exists now, so the database is the memory
                # and this must not keep a second copy that a rollback would
                # not clear.
                self._settled.pop(key, None)
                self._archive_backoff.pop(key, None)
            elif result.status == "rejected":
                codes = {(error.get("code") or "") for error in (result.errors or [])}
                if codes & _TOPOLOGY_REJECTIONS:
                    self._settled[key] = Settled(
                        move.sha, binding_snapshots[move.repo_full_name]
                    )
                    self._archive_backoff.pop(key, None)
                elif codes & _RETRYABLE_REJECTIONS:
                    self._record_retryable_failure(move, codes, deployed.get(key))
                else:
                    self._settled[key] = Settled(move.sha, None)
                    self._archive_backoff.pop(key, None)
            else:
                # An ignored outcome will keep repeating for this commit.
                # Remember it, or the next pass clones again (#1267).
                self._settled[key] = Settled(move.sha, None)
                self._archive_backoff.pop(key, None)

            if result.status != "rejected":
                logger.info(
                    "commit poll deployed repo=%s branch=%s sha=%s status=%s",
                    move.repo_full_name,
                    move.branch,
                    move.sha[:8],
                    result.status,
                )
        if pass_failure is not None:
            # Only once every move has had its turn (#3736): the pass reports
            # failure through poll_once's span and metric, and run_forever's
            # existing handler keeps the loop alive.
            raise pass_failure
        return moves

    def _record_retryable_failure(
        self,
        move: Move,
        codes: set[str],
        deployed_sha: str | None,
    ) -> None:
        """Make this repository wait longer before it costs another clone.

        The attempt count continues only while the sha AND the failure class
        both hold. A new commit may be the fix, and a rejection that changed
        codes is a different failure -- neither has earned the previous
        failure's accumulated delay (#1309).

        `deployed_sha` is what the deployments query reported for this branch on
        this pass, passed in rather than recomputed. It is the baseline the
        prune in `poll_once` compares against to notice a deploy from the other
        lane.
        """

        key = (move.repo_full_name, move.branch)
        failure = tuple(sorted(codes))
        previous = self._archive_backoff.get(key)
        if previous is not None and previous.sha == move.sha and previous.codes == failure:
            attempts = previous.attempts + 1
        else:
            attempts = 1
        # The exponent is clamped as well as the product: see
        # `_RETRY_MAX_DOUBLINGS`. The outer `min` still earns its place -- four
        # doublings gives 4,800s, and the ceiling is 3,600s. `attempts` itself
        # is deliberately NOT capped: the count is real information, and it is
        # what the stalled-lane error below reports.
        delay = min(
            _RETRY_BASE_DELAY_S * 2 ** min(attempts - 1, _RETRY_MAX_DOUBLINGS),
            _RETRY_MAX_DELAY_S,
        )
        self._archive_backoff[key] = ArchiveBackoff(
            sha=move.sha,
            codes=failure,
            attempts=attempts,
            next_attempt_at=self._clock() + delay,
            deployed_sha=deployed_sha,
        )
        if attempts >= _RETRY_ERROR_ROUNDS:
            # Re-emitted on EVERY subsequent attempt, not once at the
            # threshold: a lane parked at the hourly ceiling would otherwise go
            # quiet again, which is the silence this was meant to end (#1309).
            logger.error(
                "commit poll clone failed %d consecutive times repo=%s branch=%s sha=%s "
                "codes=%s; deploys from this repository are NOT happening. Retrying in "
                "%.0fs. The repository may be unreachable, unauthorized, missing a clone "
                "credential, or too large to clone inside the 120s timeout.",
                attempts,
                move.repo_full_name,
                move.branch,
                move.sha[:8],
                ",".join(failure),
                delay,
            )
        else:
            logger.info(
                "commit poll will retry repo=%s branch=%s sha=%s codes=%s "
                "attempt=%d in %.0fs",
                move.repo_full_name,
                move.branch,
                move.sha[:8],
                ",".join(failure),
                attempts,
                delay,
            )
