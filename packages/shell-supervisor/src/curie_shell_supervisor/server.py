"""The supervisor's HTTPS surface: claim control for the worker, ACI proxy for turns.

One shell serves at most one claim lease at a time. A lease is the worker's
``(claim, generation)`` pair plus that claim's runner bearer. The control API
creates and retires leases; every other request is a proxied ACI call that
must name the current lease in its path and present its bearer, so a delayed
request from an earlier claim never reaches a later child.

Retirement settles the runner first (``/v1/status``: no active turn and
history durable), then stops the whole process group, wipes the claim tree and
the child's leftovers, and only then reports the shell clean. A shell that
cannot prove that is quarantined: it refuses new claims and fails its health
check, so the orchestrator replaces the task.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import logging
import os
import signal
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiohttp
from aiohttp import web

from . import isolation, stage
from .child import RunnerChild, reap_orphans
from .config import SupervisorConfig
from .protocol import (
    CLAIMS_PATH,
    HEALTH_PATH,
    OUTCOME_ABSENT,
    OUTCOME_CLEANED,
    OUTCOME_QUARANTINED,
    PROXY_PREFIX,
    RETIRE_MODES,
    SHELL_PATH,
    STATE_IDLE,
    STATE_QUARANTINED,
    STATE_RUNNING,
    STATE_STAGING,
    STATE_STOPPING,
    valid_claim_name,
    valid_generation,
)

logger = logging.getLogger(__name__)

# Hop-by-hop headers (RFC 9110 section 7.6.1) and the ones aiohttp recomputes.
_HOP_HEADERS = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "host",
        "content-length",
    }
)
_STREAM_CHUNK = 64 * 1024


@dataclass
class Lease:
    claim: str
    generation: int
    digest: str
    bearer: bytes = field(repr=False)
    staged: stage.StagedClaim
    child: RunnerChild
    started_at: str
    ready: bool = False
    tasks: list[asyncio.Task[None]] = field(default_factory=list)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _bearer(request: web.Request) -> bytes | None:
    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        return None
    return header[len("Bearer ") :].encode("utf-8")


class Supervisor:
    """Owns the one lease of this shell and the runner child serving it."""

    def __init__(
        self,
        config: SupervisorConfig,
        *,
        environ: dict[str, str] | None = None,
        fatal: Callable[[str], None] | None = None,
    ) -> None:
        self._config = config
        self._environ = dict(os.environ if environ is None else environ)
        self._control = config.control_token.encode("utf-8")
        self._fatal = fatal or self.die
        self._lock = asyncio.Lock()
        self._lease: Lease | None = None
        self._state = STATE_IDLE
        self._max_generation = 0
        self._quarantine: str | None = None
        self._last: dict[str, Any] | None = None
        self._http: aiohttp.ClientSession | None = None
        self._baseline: isolation.Baseline | None = None
        # Create and retire run as supervisor-owned tasks, so a control client
        # that disconnects mid-call never abandons a half-done operation.
        self._operations: set[asyncio.Task[Any]] = set()

    # -- status -----------------------------------------------------------------

    @property
    def lease(self) -> Lease | None:
        return self._lease

    def status(self) -> dict[str, Any]:
        lease = self._lease
        claim = None
        if lease is not None:
            claim = {
                "name": lease.claim,
                "generation": lease.generation,
                "ready": lease.ready and lease.child.returncode is None,
                "child_exit": lease.child.returncode,
                "started_at": lease.started_at,
            }
        return {
            "slot": self._config.slot,
            "incarnation": self._config.incarnation,
            "isolated": self._config.isolated,
            "state": self._state,
            "claim": claim,
            "max_generation": self._max_generation,
            "quarantine_reason": self._quarantine,
            "last": self._last,
        }

    # -- lifecycle --------------------------------------------------------------

    async def start(self) -> None:
        self._http = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None, sock_connect=5.0),
            auto_decompress=False,
        )
        for path in (self._config.workspace_path, self._config.attachments_path):
            stage.link(path, None)
        # A previous incarnation's leftovers (a crash before cleanup) are wiped
        # before this shell admits any claim.
        claims = self._config.state_root / "claims"
        if claims.exists():
            for leftover in claims.iterdir():
                if not stage.wipe(leftover):
                    self._quarantine_now("stale-claim-directory")
        # A container restarted in place keeps its filesystem: what an earlier
        # child left in the scratch roots goes before the baseline is taken.
        if not stage.scrub(self._config.scrub_paths, self._config.child_uid):
            self._quarantine_now("scrub-failed")
        writer = (
            None
            if self._config.child_uid is None
            else isolation.Writer(
                uid=self._config.child_uid,
                gids=frozenset({self._config.child_gid or self._config.child_uid}),
            )
        )
        # Raises IsolationError when the shell cannot isolate claims; the
        # supervisor then exits before serving.
        self._baseline = await asyncio.to_thread(
            isolation.Baseline.take, writer=writer, scratch_roots=self._config.scrub_paths
        )

    async def close(self) -> None:
        lease = self._lease
        if lease is not None:
            await lease.child.stop(self._config.stop_grace_s)
        if self._http is not None:
            await self._http.close()

    def die(self, reason: str) -> None:
        """A supervisor fault ends the whole shell, child included.

        As PID 1 the exit alone stops every process in the container; outside
        one, the child's parent-death signal and this group kill do.
        """

        logger.critical("supervisor fault, exiting: %s", reason)
        lease = self._lease
        if lease is not None:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(lease.child.pid, signal.SIGKILL)
        os._exit(70)

    def _quarantine_now(self, reason: str) -> None:
        if self._quarantine is None:
            logger.error("shell %s quarantined: %s", self._config.slot, reason)
        self._quarantine = reason
        self._state = STATE_QUARANTINED

    def _watch(self, lease: Lease, coro: Any) -> None:
        task = asyncio.create_task(coro)
        lease.tasks.append(task)

        def done(finished: asyncio.Task[None]) -> None:
            if finished.cancelled():
                return
            error = finished.exception()
            if error is not None:
                self._fatal(f"supervisor task failed: {type(error).__name__}: {error}")

        task.add_done_callback(done)

    async def _await_ready(self, lease: Lease) -> None:
        assert self._http is not None
        url = f"http://127.0.0.1:{self._config.child_port}/healthz"
        while lease.child.returncode is None and not lease.ready:
            with contextlib.suppress(aiohttp.ClientError, TimeoutError):
                async with self._http.get(url, timeout=aiohttp.ClientTimeout(total=1.0)) as resp:
                    if resp.status == 200:
                        lease.ready = True
                        return
            await asyncio.sleep(self._config.ready_poll_s)

    async def _reap_while_running(self, lease: Lease) -> None:
        code = await lease.child.wait()
        if self._lease is lease and self._state != STATE_STOPPING:
            logger.warning("runner child of claim %s exited with %s", lease.claim, code)
        reap_orphans()

    async def create(
        self, claim: str, generation: int, archive: Path, digest: str
    ) -> tuple[int, dict[str, Any]]:
        try:
            return await self._create(claim, generation, archive, digest)
        finally:
            archive.unlink(missing_ok=True)

    async def _create(
        self, claim: str, generation: int, archive: Path, digest: str
    ) -> tuple[int, dict[str, Any]]:
        async with self._lock:
            lease = self._lease
            if lease is not None:
                if (lease.claim, lease.generation, lease.digest) == (claim, generation, digest):
                    return 200, self.status()
                return 409, {"error": "shell is serving another claim", "shell": self.status()}
            if self._quarantine is not None:
                return 503, {"error": "shell is quarantined", "shell": self.status()}
            if generation <= self._max_generation:
                return 409, {"error": "stale lease generation", "shell": self.status()}
            self._max_generation = generation
            self._state = STATE_STAGING
            try:
                staged, manifest = await asyncio.to_thread(
                    stage.prepare,
                    archive,
                    claim=claim,
                    generation=generation,
                    config=self._config,
                )
            except stage.StageError as exc:
                self._state = STATE_IDLE
                return 400, {"error": str(exc), "shell": self.status()}
            except BaseException as exc:
                # A half staged shell is unknown, whatever stopped the staging.
                self._quarantine_now(f"staging-failed:{type(exc).__name__}")
                if not isinstance(exc, Exception):
                    raise
                return 503, {"error": "staging failed", "shell": self.status()}
            bearer = manifest.runner_token.encode("utf-8")
            if hmac.compare_digest(bearer, self._control):
                await asyncio.to_thread(stage.wipe, staged.root)
                self._state = STATE_IDLE
                return 400, {"error": "runner bearer must differ from the control credential"}
            try:
                stage.link(self._config.workspace_path, staged.workspace)
                stage.link(self._config.attachments_path, staged.attachments)
                env = stage.child_environment(
                    manifest, staged, config=self._config, environ=self._environ
                )
                child = await RunnerChild.spawn(
                    self._config.runner_command,
                    env=env,
                    cwd=staged.home,
                    uid=self._config.child_uid,
                    gid=self._config.child_gid,
                )
            except BaseException as exc:
                await asyncio.to_thread(self._clean_paths, staged)
                self._quarantine_now(f"spawn-failed:{type(exc).__name__}")
                if not isinstance(exc, Exception):
                    raise
                return 503, {"error": "runner did not start", "shell": self.status()}
            lease = Lease(
                claim=claim,
                generation=generation,
                digest=digest,
                bearer=bearer,
                staged=staged,
                child=child,
                started_at=_now(),
            )
            self._lease = lease
            self._state = STATE_RUNNING
            self._watch(lease, self._await_ready(lease))
            self._watch(lease, self._reap_while_running(lease))
            logger.info("claim %s generation %d started", claim, generation)
            return 201, self.status()

    async def _settled(self, lease: Lease) -> bool:
        """Whether the runner is idle with durable history (or never served)."""

        if not lease.ready:
            # A runner that never answered healthz never accepted a turn.
            return True
        if lease.child.returncode is not None:
            return False
        assert self._http is not None
        url = f"http://127.0.0.1:{self._config.child_port}/v1/status"
        headers = {"Authorization": "Bearer " + lease.bearer.decode("utf-8")}
        deadline = time.monotonic() + self._config.settle_timeout_s
        while time.monotonic() < deadline:
            try:
                async with self._http.get(
                    url, headers=headers, timeout=aiohttp.ClientTimeout(total=2.0)
                ) as resp:
                    body = await resp.json() if resp.status == 200 else {}
            except (aiohttp.ClientError, TimeoutError, ValueError):
                body = {}
            if body.get("turn_active") is False and body.get("history_durable") is True:
                return True
            if lease.child.returncode is not None:
                return False
            await asyncio.sleep(0.2)
        return False

    def _clean_paths(self, staged: stage.StagedClaim | None) -> list[str]:
        """Remove everything a claim could leave; return what is still there."""

        problems: list[str] = []
        for path in (self._config.workspace_path, self._config.attachments_path):
            try:
                stage.link(path, None)
            except (OSError, stage.StageError):
                problems.append(f"could not remove the link {path}")
        if staged is not None and not stage.wipe(staged.root):
            problems.append("could not remove the claim directory")
        if self._baseline is None:
            problems.append("no isolation baseline")
        else:
            try:
                problems += self._baseline.restore()
            except OSError as exc:
                problems.append(f"isolation check failed: {type(exc).__name__}")
        return problems

    async def _owned(self, operation: Any) -> tuple[int, dict[str, Any]]:
        """Run ``operation`` to completion even if the caller goes away."""

        task: asyncio.Task[tuple[int, dict[str, Any]]] = asyncio.create_task(operation)
        self._operations.add(task)
        task.add_done_callback(self._operations.discard)
        return await asyncio.shield(task)

    async def retire(self, claim: str, generation: int, mode: str) -> tuple[int, dict[str, Any]]:
        async with self._lock:
            lease = self._lease
            if lease is None or (lease.claim, lease.generation) != (claim, generation):
                # Retiring a lease this shell never started fences it: a create
                # for it still in flight (its creator lost the lease) is
                # refused as stale when it lands.
                self._max_generation = max(self._max_generation, generation)
                last = self._last
                if last is not None and (last["name"], last["generation"]) == (claim, generation):
                    return 200, {"outcome": last["outcome"], "shell": self.status()}
                if lease is not None:
                    # The named lease is not here and another one is: the
                    # caller's claim ended long ago. Never touch the live one.
                    return 409, {"outcome": OUTCOME_ABSENT, "shell": self.status()}
                outcome = OUTCOME_QUARANTINED if self._quarantine else OUTCOME_ABSENT
                return 200, {"outcome": outcome, "shell": self.status()}
            self._state = STATE_STOPPING
            settled = await self._settled(lease)
            stopped = await lease.child.stop(self._config.stop_grace_s)
            for task in lease.tasks:
                task.cancel()
            problems = await asyncio.to_thread(self._clean_paths, lease.staged)
            self._lease = None
            if not stopped:
                self._quarantine_now("processes-survived")
            elif problems:
                logger.error("claim %s left state behind: %s", claim, "; ".join(problems))
                self._quarantine_now("cleanup-unverified")
            elif not settled:
                self._quarantine_now("history-unsettled")
            else:
                self._state = STATE_IDLE
            outcome = OUTCOME_QUARANTINED if self._quarantine else OUTCOME_CLEANED
            self._last = {
                "name": claim,
                "generation": generation,
                "outcome": outcome,
                "mode": mode,
                "settled": settled,
                "at": _now(),
            }
            logger.info("claim %s generation %d %s (%s)", claim, generation, outcome, mode)
            return 200, {"outcome": outcome, "shell": self.status()}

    # -- HTTP -------------------------------------------------------------------

    def _authorized(self, request: web.Request) -> bool:
        presented = _bearer(request)
        return presented is not None and hmac.compare_digest(presented, self._control)

    async def handle_health(self, _request: web.Request) -> web.Response:
        status = 503 if self._quarantine else 200
        return web.json_response({"state": self._state}, status=status)

    async def handle_shell(self, request: web.Request) -> web.Response:
        if not self._authorized(request):
            return web.json_response({"error": "control credential required"}, status=401)
        return web.json_response(self.status())

    @staticmethod
    def _lease_identity(request: web.Request) -> tuple[str, int] | None:
        claim = request.match_info["claim"]
        try:
            generation = int(request.match_info["generation"])
        except ValueError:
            return None
        if not valid_claim_name(claim) or not valid_generation(generation):
            return None
        return claim, generation

    async def handle_create(self, request: web.Request) -> web.Response:
        if not self._authorized(request):
            return web.json_response({"error": "control credential required"}, status=401)
        identity = self._lease_identity(request)
        if identity is None:
            return web.json_response({"error": "invalid claim path"}, status=400)
        length = request.content_length
        if length is None:
            return web.json_response({"error": "Content-Length is required"}, status=411)
        if length > self._config.max_stage_bytes:
            return web.json_response({"error": "staging archive too large"}, status=413)
        self._config.state_root.mkdir(parents=True, exist_ok=True, mode=0o711)
        fd, name = tempfile.mkstemp(dir=self._config.state_root, prefix=".upload-")
        archive = Path(name)
        try:
            digest = hashlib.sha256()
            received = 0
            with os.fdopen(fd, "wb") as out:
                async for chunk in request.content.iter_chunked(_STREAM_CHUNK):
                    received += len(chunk)
                    if received > length:
                        return web.json_response({"error": "body exceeds length"}, status=400)
                    digest.update(chunk)
                    out.write(chunk)
            if received != length:
                return web.json_response({"error": "truncated staging archive"}, status=400)
        except BaseException:
            archive.unlink(missing_ok=True)
            raise
        # From here the create owns the archive and runs to its end even if
        # this client disconnects; a retry of the same body then answers 200.
        status, body = await self._owned(self.create(*identity, archive, digest.hexdigest()))
        return web.json_response(body, status=status)

    async def handle_retire(self, request: web.Request) -> web.Response:
        if not self._authorized(request):
            return web.json_response({"error": "control credential required"}, status=401)
        identity = self._lease_identity(request)
        mode = request.query.get("mode", "retire")
        if identity is None or mode not in RETIRE_MODES:
            return web.json_response({"error": "invalid claim path or mode"}, status=400)
        status, body = await self._owned(self.retire(*identity, mode))
        return web.json_response(body, status=status)

    async def handle_proxy(self, request: web.Request) -> web.StreamResponse:
        identity = self._lease_identity(request)
        lease = self._lease
        if identity is None or lease is None or (lease.claim, lease.generation) != identity:
            # Never 409: that status means "no active turn" on the ACI.
            return web.json_response({"error": "claim is not served by this shell"}, status=410)
        presented = _bearer(request)
        if presented is None or not hmac.compare_digest(presented, lease.bearer):
            return web.json_response({"error": "invalid token"}, status=401)
        if lease.child.returncode is not None:
            return web.json_response({"error": "runner exited"}, status=503)
        assert self._http is not None
        tail = request.match_info["tail"]
        url = f"http://127.0.0.1:{self._config.child_port}/{tail}"
        headers = {k: v for k, v in request.headers.items() if k.lower() not in _HOP_HEADERS}
        body = request.content if request.body_exists else None
        try:
            async with self._http.request(
                request.method,
                url,
                params=request.query,
                headers=headers,
                data=body,
                allow_redirects=False,
            ) as upstream:
                response = web.StreamResponse(status=upstream.status, reason=upstream.reason)
                for key, value in upstream.headers.items():
                    if key.lower() not in _HOP_HEADERS:
                        response.headers.add(key, value)
                if upstream.content_length is not None:
                    response.content_length = upstream.content_length
                await response.prepare(request)
                async for chunk in upstream.content.iter_any():
                    await response.write(chunk)
                await response.write_eof()
                return response
        except (aiohttp.ClientConnectionError, TimeoutError) as exc:
            # A worker that went away surfaces as a reset on write; leaving the
            # context above already closed the upstream connection, which is
            # the runner's disconnect signal, so there is no one to answer.
            if isinstance(exc, aiohttp.ClientConnectionResetError) or (
                request.transport is None or request.transport.is_closing()
            ):
                raise
            return web.json_response({"error": "runner unreachable"}, status=502)

    def app(self) -> web.Application:
        application = web.Application(client_max_size=self._config.max_stage_bytes)
        application.add_routes(
            [
                web.get(HEALTH_PATH, self.handle_health),
                web.get(SHELL_PATH, self.handle_shell),
                web.put(CLAIMS_PATH + "/{claim}/{generation}", self.handle_create),
                web.delete(CLAIMS_PATH + "/{claim}/{generation}", self.handle_retire),
                web.route("*", PROXY_PREFIX + "/{claim}/{generation}/{tail:.*}", self.handle_proxy),
            ]
        )

        async def on_startup(_app: web.Application) -> None:
            await self.start()

        async def on_cleanup(_app: web.Application) -> None:
            await self.close()

        application.on_startup.append(on_startup)
        application.on_cleanup.append(on_cleanup)
        return application
