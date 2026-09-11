"""The agent-facing interaction harness: one composed Curie stack, six verbs.

What this is
------------
Stage 2 proved the approval journey runs across real API, worker, dispatcher,
Postgres and Valkey, but did it as one pytest module with the composition
inlined in ``apps/worker/tests/kernel/conftest.py``. This module is that
composition, generalised: a sync context manager that owns a disposable migrated
Postgres database, the REAL ``curie_api`` app under a real uvicorn server on a
loopback port, the real kernel over real Valkey, and the real dispatcher with the
real ``ApprovalResolveClient`` -- and exposes them as six bounded verbs whose
results are frozen, JSON-serialisable dataclasses.

The stand-ins are exactly the two stage 2 named, and no more: **the model**
(behind the REAL ``curie_runner``, booted through ``build_runner(...,
fake_model=True)`` -> ``create_app`` and served to the kernel as ``runner_app=``)
and **the Slack edge** (a mocked ``WebClient`` plus a fake socket). Everything
between them is production code, the runner included: a turn driven through
``send`` compiles a real bundle, runs the real session loop, and emits
production ACI frames. ``send`` steers that turn with the fake model's own
approval MARKER rather than by scripting frames, so the approval the kernel
creates came out of the production ``mcp__curie__request_approval`` path.

Why the verbs look like this
----------------------------
* **``act`` takes a captured action object, never a literal action id.** The
  harness exists to prove a REAL chat click drove the system. An ``act`` that
  accepted a literal id would let every future test skip the card entirely -- the
  render, the ownership probe, the block structure -- and still report a green
  approval journey. The refusal (``UncapturedAction``) is what keeps "the harness
  drove it" from degrading into "the harness POSTed something".
* **Every blocking verb takes an explicit ``deadline_s`` and raises
  ``HarnessTimeout``.** An unbounded wait in a test harness is the worst failure
  mode available: the suite reports nothing, the CI job dies on its outer
  timeout, and the report names no test.
* **Faults revert in a ``finally``.** A leaked fault fails a LATER, unrelated
  test, with nothing pointing back to the cause.

The composition helpers below the harness class (``journey_env``,
``disposable_migrated_database``, ``composed_api_server``,
``resolve_client_factory``, ``JourneyRecorder``, ``gated_call``) are the stage 2
fixtures, moved here WITH their explanatory docstrings. Those docstrings are the
record of four separate traps -- the ``VALKEY_URL`` precedence trap, the
``DATABASE_URL`` ownership collision, the disabled sweeper and the disabled
reconciler -- and deleting them re-opens all four. ``apps/worker/tests/kernel/
conftest.py`` now holds thin wrappers that yield from these, so the journey
module and the harness share ONE implementation rather than two that can drift.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import json
import os
import secrets
import socket
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType, TracebackType
from typing import Any

import redis

from curie_test_support.valkey import (
    VALKEY_HOST as _VALKEY_HOST,
)
from curie_test_support.valkey import (
    VALKEY_PORT as _VALKEY_PORT,
)
from curie_test_support.valkey import (
    VALKEY_PW as _VALKEY_PW,
)
from curie_test_support.valkey import connect_or_skip

from .faults import ArmedFault, arm_fault, wrap_resolve_client
from .results import (
    ActResult,
    ApprovalRecord,
    AuditEntry,
    AuditResult,
    CapturedAction,
    CapturedCard,
    CapturedMessage,
    FaultResult,
    MessagesResult,
    OutcomeResult,
    ResetResult,
    ResumeTurn,
    ResumeTurnsResult,
    SendResult,
    Snapshot,
)

__all__ = [
    "ComposedApi",
    "HarnessTimeout",
    "InteractionHarness",
    "JourneyEnv",
    "JourneyRecorder",
    "UncapturedAction",
    "composed_api_server",
    "disposable_migrated_database",
    "gated_call",
    "journey_env",
    "resolve_client_factory",
]


# Test placeholders hoisted to constants (not inline literals) so the repo's
# secret-shaped-literal gate does not trip on these quoted values.
JOURNEY_PLATFORM_CREDENTIAL = "journey-platform-api-key"
# Deliberately DISTINCT from the platform key: sharing them is what
# ``approval_auth.py:92-97`` refuses outright, and what ADR-0106 (#1531) exists
# to prevent. ``config.py:430-431`` also refuses equal values at boot.
JOURNEY_ATTESTER_VALUE = "journey-chat-attester-secret"

# Env vars the journey composition owns. Snapshotted and restored as a unit so a
# failed run cannot leak API settings into the rest of the process.
JOURNEY_ENV_KEYS = (
    "RUNS_STREAM",
    "CURIE_STREAM",
    "VALKEY_URL",
    "VALKEY_HOST",
    "VALKEY_PORT",
    "VALKEY_PASSWORD",
    "API_KEY",
    "CURIE_APPROVAL_CHAT_ATTESTER_SECRET",
    # NOT ``CURIE_``-prefixed: ``Settings`` has no ``env_prefix`` and these two
    # fields carry no ``validation_alias`` (config.py:237, :270), so pydantic-
    # settings reads the bare field names. With ``extra="ignore"`` (config.py:35)
    # a ``CURIE_``-prefixed key is silently dropped and the guard never binds.
    "APPROVAL_SWEEP_INTERVAL_S",
    "RESUME_RECONCILER_ENABLED",
    # Owned per-run rather than session-wide, so this composition can neither be
    # broken by nor break ``apps/api/tests`` (which owns the same key,
    # apps/api/tests/conftest.py:117-142) in one session.
    "DATABASE_URL",
    "S3_ENDPOINT_URL",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "ENVIRONMENT",
)

# The channel the harness's synthetic person speaks in, and the person itself.
# ``C0EXAMPLE1`` shaped rather than a realistic id: the gitleaks Slack-id rule
# treats a realistic-looking channel id as a finding.
HARNESS_CHANNEL = "C0EXAMPLE1"
HARNESS_AUTHOR = "U0EXAMPLE4"
_DEFAULT_NOTE = "approved through the interaction harness"

# How often a bounded wait re-checks. Short enough that a 1s deadline is not
# quantised into something visibly longer (the contract tests assert the verb
# returns inside 2x its deadline), long enough not to spin a core.
_POLL_INTERVAL_S = 0.02

# The ceiling one HTTP call to the composed API may take when no verb deadline
# is narrower. A ceiling, never the bound: inside a verb the call is clipped to
# what is LEFT of that verb's deadline (``InteractionHarness._budget``).
_HTTP_TIMEOUT_S = 10.0

# The floor on what is handed to ``_call``: a coroutine scheduled with a zero or
# negative timeout reports a timeout it never had a chance to beat.
_MIN_CALL_S = 0.05

# The ceiling one Valkey command may take when no verb deadline is narrower.
# ``connect_or_skip`` sets ``socket_connect_timeout`` but deliberately no
# ``socket_timeout`` -- correct for a fixture whose whole job is one ping, and a
# hole for a bounded verb: a Valkey that ACCEPTS and never answers blocks the
# command inside the socket read, where ``_Deadline.wait`` (which only polls a
# predicate) can never interrupt it. Every Valkey command a verb issues goes
# through ``_redis_call`` instead, clipped to what is left of the verb.
_VALKEY_TIMEOUT_S = 10.0

# How long the real consumer is given to finish ONE entry inside ``send``. A
# ceiling under the verb's own deadline, which is what actually bounds the verb.
_CONSUME_TIMEOUT_S = 120.0


class HarnessTimeout(AssertionError):
    """A bounded verb gave up, naming the verb, the wait and the bound.

    An ``AssertionError`` subclass so an unhandled one reads as a test failure
    rather than an infrastructure error, and so a caller that only catches
    ``AssertionError`` still sees it. The four attributes are part of the
    contract: a bare ``TimeoutError`` from twenty frames down tells an agent
    nothing about which of its scripted steps wedged.
    """

    def __init__(self, *, verb: str, what: str, deadline_s: float, elapsed_s: float) -> None:
        self.verb = verb
        self.what = what
        self.deadline_s = deadline_s
        self.elapsed_s = elapsed_s
        super().__init__(
            f"{verb} timed out after {elapsed_s:.2f}s (deadline {deadline_s:.2f}s) "
            f"waiting for: {what}"
        )


class UncapturedAction(AssertionError):
    """``act`` was handed something that did not come out of ``messages()``.

    Raised for a bare string even when the string is the CORRECT action id. The
    contract is provenance, not validity: the right id in the wrong FORM is still
    a test that never looked at the card.
    """


# --- The disposable database -------------------------------------------------


def _off_loop(work: Callable[[], None], *, what: str, timeout: float = 300.0) -> None:
    """Run blocking, loop-owning work from sync code, loop or no loop.

    ``asyncio.run`` is wrong here, and so is anything that calls it underneath:
    this composition is entered from BOTH shapes -- a plain sync test, and a test
    whose body is already inside ``asyncio.run`` (the repo's convention for async
    kernel tests). Under the second shape ``asyncio.run`` raises "cannot be
    called from a running event loop", and that reaches further than this
    module's own two admin statements: Alembic's ``apps/api/alembic/env.py:96``
    calls ``asyncio.run`` at import time, so even ``command.upgrade`` has to run
    off the caller's loop. A private thread with no loop of its own is correct on
    both shapes, and it is bounded like every other blocking call here.
    """

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        work()
        return

    box: list[BaseException] = []

    def _run() -> None:
        try:
            work()
        except BaseException as exc:  # noqa: BLE001 - re-raised on the caller
            box.append(exc)

    thread = threading.Thread(target=_run, name="interaction-off-loop", daemon=True)
    thread.start()
    thread.join(timeout=timeout)
    assert not thread.is_alive(), f"{what} did not finish within {timeout}s"
    if box:
        raise box[0]


@contextlib.contextmanager
def disposable_migrated_database() -> Iterator[str]:
    """A disposable, migrated database, and the URL every reader must use.

    Recipe from apps/api/tests/conftest.py:127-143 (create / ``alembic upgrade
    head`` / drop). Migrating is not optional: the app lifespan calls
    ``assert_servable()``, which refuses to boot against an unmigrated schema
    with "database schema None is below application min ...".

    It hands back the URL rather than leaving ``DATABASE_URL`` set, so the API
    app, the direct row assertions and any seeding provably read ONE database
    rather than three that merely look alike. ``DATABASE_URL`` is set only for
    the duration of the migration and then RESTORED: :func:`journey_env` re-sets
    it per run. Holding it open would make an interleaved run with
    ``apps/api/tests`` (which owns the same key, apps/api/tests/conftest.py:117-142)
    order-dependent in both directions.
    """

    import secrets
    from datetime import UTC, datetime

    import asyncpg  # type: ignore[import-untyped]
    from alembic import command
    from alembic.config import Config
    from curie_api.config import get_settings
    from sqlalchemy import make_url

    base_url = os.environ.get(
        "TEST_DATABASE_URL",
        "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres",
    )
    base = make_url(base_url)
    run_db = f"curie_journey_{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}_{secrets.token_hex(3)}"

    async def _admin(sql: str) -> None:
        conn = await asyncpg.connect(
            user=base.username,
            password=base.password,
            host=base.host,
            port=base.port,
            database="postgres",
        )
        try:
            await conn.execute(sql)
        finally:
            await conn.close()

    _off_loop(lambda: asyncio.run(_admin(f'CREATE DATABASE "{run_db}"')), what="CREATE DATABASE")
    url = str(base.set(database=run_db).render_as_string(hide_password=False))
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = url
    get_settings.cache_clear()
    try:
        # Inside the try so a failed migration still drops the database.
        cfg = Config()
        cfg.set_main_option("script_location", str(repo_root() / "apps" / "api" / "alembic"))
        # Off the caller's loop: alembic's env.py calls ``asyncio.run`` itself.
        _off_loop(lambda: command.upgrade(cfg, "head"), what="the alembic migration")
        # Hand the key back the moment the migration no longer needs it.
        _restore_database_url(previous)
        get_settings.cache_clear()
        yield url
    finally:
        _off_loop(
            lambda: asyncio.run(_admin(f'DROP DATABASE IF EXISTS "{run_db}" WITH (FORCE)')),
            what="DROP DATABASE",
        )
        _restore_database_url(previous)
        get_settings.cache_clear()


def _restore_database_url(previous: str | None) -> None:
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous


# --- The composed API's environment ------------------------------------------


@dataclass(frozen=True)
class JourneyEnv:
    """The credentials and stream the composed API booted with."""

    api_key: str
    attester_secret: str
    runs_stream: str


@contextlib.contextmanager
def journey_env(*, runs_stream: str, database_url: str) -> Iterator[JourneyEnv]:
    """Point the API's settings at THIS run's Valkey namespace and stack.

    Two apps, two different names for one Valkey. The worker harness reads
    ``curie_test_support.valkey``'s ``TEST_VALKEY_*`` constants, FROZEN at import
    (valkey.py:19-21); the API reads its own ``valkey_host``/``valkey_port``/
    ``valkey_password`` settings, which default to ``localhost:26379``
    (config.py:197-199). On an isolated stack that divergence would leave the API
    writing the resume onto a DIFFERENT store -- or, worse, onto a shared one
    where a pre-existing entry makes an assertion pass for the wrong reason. So
    the parts are copied across from the frozen constants.

    ``VALKEY_URL`` is DELETED rather than set: it wins outright over the parts
    (config.py:404-406, #2315), so setting the parts underneath an inherited URL
    is a silent no-op that re-opens exactly that trap.

    ``DATABASE_URL`` is set HERE, per run, rather than being held for a whole
    session: that key is also owned by ``apps/api/tests/conftest.py:117-142``, so
    a session that interleaves the two suites would otherwise be order-dependent
    in both directions.

    Both background loops the app would otherwise start are disabled. The expiry
    sweeper (config.py:237, 30s) can flip a pending row mid-run and the resume
    reconciler (config.py:270, enabled by default) can enqueue a SECOND resume,
    either of which breaks "exactly one stream entry" for a reason that has
    nothing to do with the code under test. Both are driven deliberately, by
    direct invocation, where a case needs them.

    That disable is ASSERTED against the resolved ``Settings`` below, not merely
    written into ``os.environ``: the keys are unprefixed field names and a future
    rename (or a re-added ``CURIE_`` prefix) would otherwise drop them silently
    under ``extra="ignore"`` and leave both loops running on every composed
    server (main.py:146-167).
    """

    previous = {key: os.environ.get(key) for key in JOURNEY_ENV_KEYS}

    os.environ["DATABASE_URL"] = database_url
    os.environ["RUNS_STREAM"] = runs_stream
    os.environ.pop("VALKEY_URL", None)
    os.environ["VALKEY_HOST"] = _VALKEY_HOST
    os.environ["VALKEY_PORT"] = str(_VALKEY_PORT)
    os.environ["VALKEY_PASSWORD"] = _VALKEY_PW or ""
    os.environ["API_KEY"] = JOURNEY_PLATFORM_CREDENTIAL
    os.environ["CURIE_APPROVAL_CHAT_ATTESTER_SECRET"] = JOURNEY_ATTESTER_VALUE
    os.environ["APPROVAL_SWEEP_INTERVAL_S"] = "0"
    os.environ["RESUME_RECONCILER_ENABLED"] = "false"
    # The lifespan calls ``BundleStore.ensure_bucket()``; the pilot stack serves
    # RustFS on 39000. ``setdefault`` so an exported value wins, mirroring
    # apps/api/tests/conftest.py:41-42.
    os.environ.setdefault("S3_ENDPOINT_URL", "http://localhost:39000")
    os.environ.setdefault("S3_ACCESS_KEY", "rustfs")
    os.environ.setdefault("S3_SECRET_KEY", "rustfssecret")
    # ``dev``: the prod boot gate (config.py:422) is the exclusion suite's
    # subject, not this composition's, and an inherited ENVIRONMENT=prod would
    # refuse boot here.
    os.environ["ENVIRONMENT"] = "dev"

    # Every mutation above precedes the cache clear, which precedes any
    # ``create_app()``: ``get_settings`` is ``lru_cache``d, so building the app
    # first and mutating after is a silent no-op.
    from curie_api.config import get_settings

    get_settings.cache_clear()
    try:
        # The guards above are only real if they BIND. Resolve the settings the
        # app is about to be built from and check the values, so a rename, a
        # re-added prefix or an inherited ``.env`` fails here with a clear
        # message instead of silently re-arming a background loop or pointing
        # the API at a second Valkey.
        settings = get_settings()
        assert settings.approval_sweep_interval_s <= 0, (
            "the expiry sweeper is NOT disabled: APPROVAL_SWEEP_INTERVAL_S did "
            f"not bind (resolved {settings.approval_sweep_interval_s!r}). "
            "main.py:146-167 starts the loop on every composed server."
        )
        assert settings.resume_reconciler_enabled is False, (
            "the resume reconciler is NOT disabled: RESUME_RECONCILER_ENABLED "
            f"did not bind (resolved {settings.resume_reconciler_enabled!r})."
        )
        # ``VALKEY_URL`` popped from ``os.environ`` is not enough: ``Settings``
        # also reads ``env_file=".env"`` (config.py:35) and a URL from there wins
        # outright over the parts (config.py:404-406, #2315). Assert the RESOLVED
        # dsn, which is the only thing that closes the two-Valkey trap.
        assert not settings.valkey_url, (
            "VALKEY_URL is set (most likely from a .env file) and wins over the "
            "host/port parts, so the API would write the resume to a different "
            f"store than the worker reads: {settings.valkey_url!r}"
        )
        assert settings.valkey_host == _VALKEY_HOST and settings.valkey_port == _VALKEY_PORT, (
            "the API is pointed at a different Valkey than the worker harness: "
            f"{settings.valkey_dsn()!r} vs {_VALKEY_HOST}:{_VALKEY_PORT}"
        )
        yield JourneyEnv(
            api_key=JOURNEY_PLATFORM_CREDENTIAL,
            attester_secret=JOURNEY_ATTESTER_VALUE,
            runs_stream=runs_stream,
        )
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        get_settings.cache_clear()


# --- The composed API server -------------------------------------------------


@dataclass(frozen=True)
class ComposedApi:
    """A live loopback API: its base URL, its port, and the app object itself.

    The app is carried so a caller can reach ``app.state.sessionmaker`` and
    ``app.state.resume_queue`` -- the REAL objects the lifespan composed
    (main.py:78-95). A case that invokes the production sweeper or reconciler
    directly needs exactly those, and rebuilding a sessionmaker from
    ``DATABASE_URL`` would hand it a second, look-alike connection pool that is
    not the one the server is writing through.
    """

    base_url: str
    port: int
    app: Any = None


@contextlib.contextmanager
def composed_api_server(*, startup_timeout_s: float = 30.0) -> Iterator[ComposedApi]:
    """Run the REAL ``curie_api`` app on a loopback ephemeral port.

    A real server rather than an ``httpx.ASGITransport``: the transport is an
    ``AsyncBaseTransport`` a sync ``httpx.Client`` will not accept, and it skips
    the app lifespan, which is where ``app.state.sessionmaker`` / ``resume_queue``
    / ``approver_sets`` (the real authorizer and the real enqueue) are composed
    (apps/api/src/curie_api/main.py:78-95).

    Exposed as a context manager (and not only as the harness's own server)
    because the teardown-unreachability assertion has to observe the port AFTER
    teardown, which a caller still inside the scope cannot do.

    The port is picked by binding 0 and closing the probe socket rather than by
    reading it back off ``server.servers[0].sockets[0]``: the client and the
    teardown assertion both need the number, and a pre-picked port is available
    before the server thread exists. The narrow race (something else grabbing the
    port in between) fails loudly as a bind error, never silently.

    A lifespan failure (unmigrated schema, unreachable RustFS) means the server
    never reports ``started``; the captured exception is re-raised here so the
    caller says what actually broke instead of timing out into a confusing
    connection-refused.
    """

    import uvicorn
    from curie_api.config import get_settings
    from curie_api.main import create_app

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = int(probe.getsockname()[1])

    get_settings.cache_clear()
    app = create_app()
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=port,
            log_level="warning",
            lifespan="on",
        )
    )
    failure: list[BaseException] = []

    def _serve() -> None:
        try:
            server.run()
        except BaseException as exc:  # noqa: BLE001 - surfaced below, not swallowed
            failure.append(exc)

    thread = threading.Thread(target=_serve, name=f"journey-api-{port}", daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + startup_timeout_s
        while not server.started:
            if failure:
                raise AssertionError(
                    f"the composed API failed to start on port {port}: {failure[0]!r}"
                ) from failure[0]
            if not thread.is_alive():
                raise AssertionError(
                    f"the composed API server thread exited before starting on port {port}"
                )
            if time.monotonic() > deadline:
                raise AssertionError(
                    f"the composed API did not start within {startup_timeout_s}s on port {port}"
                )
            time.sleep(0.02)
        yield ComposedApi(base_url=f"http://127.0.0.1:{port}", port=port, app=app)
    finally:
        server.should_exit = True
        thread.join(timeout=startup_timeout_s)
        assert not thread.is_alive(), f"the composed API on port {port} did not shut down"


@contextlib.contextmanager
def resolve_client_factory(api: ComposedApi, env: JourneyEnv) -> Iterator[Callable[..., Any]]:
    """Build REAL ``ApprovalResolveClient``s against the loopback API.

    ``client=`` is deliberately omitted so the production default
    ``httpx.Client(timeout=_RESOLVE_TIMEOUT)`` is the transport
    (approval_actions.py:251-262). The injectable seam every other test uses is
    what this composition exists NOT to use.
    """

    from curie_dispatcher.approval_actions import ApprovalResolveClient

    built: list[Any] = []

    def factory(**overrides: Any) -> Any:
        kwargs: dict[str, Any] = {
            "api_base_url": api.base_url,
            "api_key": env.api_key,
            "approval_chat_attester_secret": env.attester_secret,
        }
        kwargs.update(overrides)
        client = ApprovalResolveClient(**kwargs)
        built.append(client)
        return client

    yield factory

    # The class owns its default ``httpx.Client`` and exposes no ``close()``
    # (approval_actions.py:262), so nothing else would ever release the
    # connection pool.
    for client in built:
        with contextlib.suppress(Exception):
            client._client.close()


# --- The action ledger recorder and its gated-tool script --------------------


@dataclass
class JourneyRecorder:
    """Records the calls the kernel makes to the action ledger (ADR-0117).

    Why a recorder at all: the kernel harness defaults ``actions=None`` and the
    fake runner's default script is a bare ``Final``, so with neither a recorder
    nor a gated tool call, BOTH "zero side effects" and "exactly one side effect"
    are assertions that cannot fail.
    """

    recorded: list[dict[str, Any]] = field(default_factory=list)
    completed: list[tuple[str, Any]] = field(default_factory=list)

    async def record(
        self,
        frame: Any,
        *,
        event_id: str,
        conversation_id: str,
        agent_id: str | None,
        gate_approval_id: str | None = None,
    ) -> Any:
        from curie_worker.actions import RecordedAction

        self.recorded.append(
            {
                "frame": frame,
                "event_id": event_id,
                "conversation_id": conversation_id,
                "agent_id": agent_id,
                "gate_approval_id": gate_approval_id,
            }
        )
        return RecordedAction(id=f"a{len(self.recorded)}", status="pending")

    async def complete(self, action_id: str, frame: Any) -> dict[str, Any]:
        self.completed.append((action_id, frame))
        return {
            "tool": frame.tool,
            "result": frame.result,
            "detail": frame.detail,
            "status": "failed" if frame.failed else "succeeded",
            "undoable": bool(frame.result and frame.result.get("prior")),
        }


def gated_call(call_id: str, tool: str = "scale_deployment") -> list[Any]:
    """The two frames one side-effecting tool call produces.

    Mirrors test_action_ledger.py:76-95. A runner script containing one of these
    is what makes ``len(recorder.recorded) == 1`` mutation-sensitive rather than
    an assertion about an empty list that can never fail.
    """

    from aci_protocol import SideEffectFlag

    return [
        SideEffectFlag(
            tool=tool,
            call_id=call_id,
            arguments={"replicas": 10},
            detail="non-idempotent tool executed",
        ),
        SideEffectFlag(
            tool=tool,
            call_id=call_id,
            failed=False,
            result={"ok": True, "prior": {"spec": {"replicas": 3}}},
            detail="non-idempotent tool completed",
        ),
    ]


# --- Locating the kernel half -------------------------------------------------


def repo_root() -> Path:
    """The source checkout this dev-only package is installed from.

    ``packages/test-support/src/curie_test_support/interaction/harness.py`` ->
    five parents up. The package is a uv workspace member installed EDITABLE, so
    this resolves to the checkout rather than to a site-packages copy; a
    non-editable install has no kernel test tree to reach anyway, which is the
    same statement as "this package never ships".
    """

    root = Path(__file__).resolve().parents[5]
    assert (root / "pyproject.toml").is_file(), (
        f"{root} does not look like the Curie checkout; the interaction harness "
        "is dev/test-only and must be run from a source checkout"
    )
    return root


def kernel_conftest() -> ModuleType:
    """The worker kernel conftest, loaded BY PATH and reused if already imported.

    Loaded rather than duplicated, and this is the deliberate direction of the
    dependency: ``kernel_harness`` (the real ``Kernel``, the real
    ``SandboxSubstrate`` over ``FakeK8s``, the real ``RunnerClient``, the fake
    runner server) is ~150 lines of assembly that ~480 existing kernel tests
    already depend on, and the plan's A1 block is explicit that the D2 real
    runner enters through its EXISTING ``runner_app=`` parameter rather than
    through a second seam. Copying it here would fork all of that.

    ``--import-mode=importlib`` with no ``__init__.py`` means the conftest cannot
    be imported by name, and pytest registers it under a mangled, unstable key.
    An already-loaded module with the same resolved ``__file__`` is reused so the
    conftest is never executed twice in one process -- which would give a test
    session two ``FakeRunner`` classes and make ``isinstance`` lie.
    """

    path = repo_root() / "apps" / "worker" / "tests" / "kernel" / "conftest.py"
    assert path.is_file(), f"kernel conftest not found at {path}"
    for module in list(sys.modules.values()):
        file = getattr(module, "__file__", None)
        if file and Path(file).resolve() == path:
            assert isinstance(module, ModuleType)
            return module
    name = "_curie_interaction_kernel_conftest"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Registered BEFORE it executes. ``@dataclass`` resolves its own module out
    # of ``sys.modules`` while processing a class (CPython 3.14 dataclasses.py:
    # ``_is_type``), so a module executed outside ``sys.modules`` dies on the
    # first dataclass in the file with a bare ``NoneType has no __dict__``.
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return module


def _authorize(**_kwargs: Any) -> Any:
    """Bolt's workspace authorization, resolved to a fixed bot identity.

    Mirrors apps/dispatcher/tests/conftest.py:30. Redeclared rather than loaded
    by path: it is four literal fields with no behavior, and reaching into a
    second app's conftest from a package would couple this module to that file's
    layout for no gain, where the kernel conftest above is loaded precisely
    because it has behavior worth not forking.
    """

    from slack_bolt.authorization import AuthorizeResult

    return AuthorizeResult(
        enterprise_id=None,
        team_id="T1",
        bot_token="xoxb-test",
        bot_id="B1",
        bot_user_id="U0BOT",
    )


class _CapturingSocket:
    """Captures the envelope acks Bolt sends back over the socket.

    The ack BODY, not just the id (#1053): a block_actions ack is empty, but a
    view_submission ack is a channel in its own right -- it is where a refused
    submission's reason is rendered, since the approver is standing in an open
    modal. ``act`` reads its refusal verdict from here.
    """

    def __init__(self) -> None:
        import logging

        self.logger = logging.getLogger("interaction-harness-socket")
        self.acked_envelope_ids: list[str] = []
        self.ack_payloads: dict[str, Any] = {}

    def send_socket_mode_response(self, response: Any) -> None:
        self.acked_envelope_ids.append(response.envelope_id)
        self.ack_payloads[response.envelope_id] = getattr(response, "payload", None)

    def ack_payload_for(self, envelope_id: str) -> Any:
        return self.ack_payloads.get(envelope_id)


# --- The harness --------------------------------------------------------------


class InteractionHarness:
    """One composed Curie stack, driven through six bounded verbs.

    Sync on purpose: an agent scripting this over a pipe has no event loop, and
    every async collaborator (the kernel, the substrate, the Valkey client) is
    driven on a private loop owned by this object and running on its own thread.
    That loop is created in ``__enter__`` and stopped in ``__exit__``, so nothing
    it owns can outlive the context -- which is the same property the teardown
    port probe asserts for the API server.
    """

    def __init__(self) -> None:
        self._run_id = uuid.uuid4().hex
        self._entered = False
        self._stack: contextlib.ExitStack | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        self._redis: redis.Redis | None = None
        self._names: dict[str, str] = {}
        self._api: ComposedApi | None = None
        self._env: JourneyEnv | None = None
        self._kernel_harness: Any = None
        self._recorder = JourneyRecorder()
        self._resolver_factory: Callable[..., Any] | None = None
        self._approval_http: Any = None
        # Captured channel state. ``_cards`` keeps the FULL rendered card dict
        # per message id, because a click envelope must carry the real message
        # (blocks included) exactly as Slack would deliver it.
        self._messages: list[CapturedMessage] = []
        self._cards: dict[str, dict[str, Any]] = {}
        self._threads_awaiting: set[str] = set()
        self._faults: dict[str, ArmedFault] = {}
        self._sends = 0
        # The approval this run is currently about, and a short-lived cache of
        # its status. ``await_outcome`` polls tens of times a second, so an
        # uncached read per poll would make every wait a load test of the
        # loopback server instead of a test of the behavior.
        self._approval_id: str | None = None
        self._status_cache: tuple[float, str | None] = (0.0, None)
        self._consumer_obj: Any = None
        self._runner: Any = None
        # The deadline of the verb currently running, if any. Every blocking
        # segment reached from inside a verb (an ``httpx`` request, a Valkey
        # write) clips its own timeout to what is LEFT of this budget, which is
        # what makes the verb's headline bound true of the whole verb rather
        # than only of its local polling loops.
        self._guard: _Deadline | None = None

    # -- deadline plumbing ----------------------------------------------------

    @contextlib.contextmanager
    def _bounded(self, guard: _Deadline) -> Iterator[_Deadline]:
        """Make ``guard`` the budget every nested blocking segment clips to."""

        previous = self._guard
        self._guard = guard
        try:
            yield guard
        finally:
            self._guard = previous

    def _budget(self, default: float, *, what: str) -> float:
        """The timeout one nested blocking call may take, under the active verb.

        Raises rather than returning zero when the budget is already spent: a
        call issued with a zero timeout either fails with a transport error that
        names nothing, or (worse) is treated as "no timeout" by the library.
        ``HarnessTimeout`` names the verb and the segment instead.
        """

        guard = self._guard
        if guard is None:
            return default
        remaining = guard.remaining_s
        if remaining <= 0.0:
            raise guard.expired(what=what)
        return min(default, remaining)

    def _timed_out(
        self, what: str, exc: BaseException, *, timeout: float, default: float
    ) -> BaseException:
        """Report a clipped call's timeout as the VERB's timeout, when it is one.

        A clipped ``httpx`` read that expires because the verb's budget ran out
        is the verb timing out, and must arrive as ``HarnessTimeout`` naming the
        verb -- an agent that sees ``httpx.ReadTimeout`` cannot tell which
        scripted step wedged. A read that expires with budget still left, on its
        OWN full ceiling, is a genuinely slow API and keeps its own exception.

        Which of the two it was is decided by the timeout the call actually
        carried, not by re-reading the clock afterwards. ``remaining_s <= 0``
        was the previous test and it is a race: ``httpx`` fires its own timer a
        few milliseconds early, or the thread is descheduled the other way, and
        the very same wedge surfaces as a bare ``httpx.TimeoutException`` on one
        run and a ``HarnessTimeout`` on the next. If the segment was clipped
        (``timeout < default``) it was the verb's budget that expired, full
        stop.
        """

        guard = self._guard
        if guard is None:
            return exc
        if timeout < default or guard.remaining_s <= 0.0:
            return guard.expired(what=what)
        return exc

    def _redis_call(self, work: Callable[[], Any], *, what: str) -> Any:
        """One Valkey command, bounded by the active verb's remaining budget.

        Two mechanisms, because either alone leaves a hole. The socket timeout
        is clipped so the command itself gives up (the client from
        ``connect_or_skip`` has none, so a hung Valkey would otherwise block
        forever in a C-level socket read). The call is then ALSO run through
        ``_Deadline.run``, on a daemon thread joined under the budget, because a
        socket timeout is per-recv rather than per-command and a trickling peer
        can reset it indefinitely. ``_Deadline.run`` is what makes the verb's
        headline bound true regardless of what the socket does.
        """

        guard = self._guard
        if guard is None:
            return work()
        timeout = self._budget(_VALKEY_TIMEOUT_S, what=what)
        self._clip_redis_socket(timeout)
        return guard.run(work, what=what)

    def _clip_redis_socket(self, timeout: float) -> None:
        """Push ``timeout`` onto the Valkey client's pool AND its live sockets.

        The pool kwargs alone govern only connections created from here on, and
        the harness's client has a live one from ``__enter__``'s ping -- so the
        one connection every verb actually uses would keep the library default
        (none). Both are set.
        """

        pool = self.redis.connection_pool
        pool.connection_kwargs["socket_timeout"] = timeout
        for connection in (
            *getattr(pool, "_available_connections", ()),
            *getattr(pool, "_in_use_connections", ()),
        ):
            connection.socket_timeout = timeout
            sock = getattr(connection, "_sock", None)
            if sock is not None:
                with contextlib.suppress(OSError):
                    sock.settimeout(timeout)

    # -- lifecycle ------------------------------------------------------------

    def __enter__(self) -> InteractionHarness:
        stack = contextlib.ExitStack()
        self._stack = stack
        try:
            token = self._run_id
            self._names = {
                "stream": f"test:curie:runs:{token}",
                "group": f"g-{token}",
                "prefix": f"test:curie:worker:{token}",
                "sandbox_prefix": f"test:curie:sandbox:{token}",
            }
            client = connect_or_skip(decode_responses=True)
            stack.callback(client.close)
            self._redis = client
            stack.callback(self._purge_namespace)
            # The run's namespace must be EMPTY at start. This is what "an
            # isolated stack" actually bought, and unlike a port literal it is
            # true on CI's Valkey and on any developer's.
            assert client.exists(self._names["stream"]) == 0, (
                f"the run's stream {self._names['stream']!r} already exists before "
                "the harness sent anything; two runs are sharing a namespace"
            )

            database_url = stack.enter_context(disposable_migrated_database())
            self._env = stack.enter_context(
                journey_env(runs_stream=self._names["stream"], database_url=database_url)
            )
            self._api = stack.enter_context(composed_api_server())
            self._resolver_factory = stack.enter_context(
                resolve_client_factory(self._api, self._env)
            )
            self._start_loop(stack)
            self._start_kernel(stack)
            self._entered = True
            return self
        except BaseException:
            stack.close()
            self._stack = None
            raise

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # Faults first: a fault is a patch over production code, and leaving one
        # armed past teardown poisons every later test in the process, which is
        # the one failure this harness must never cause.
        for armed in reversed(list(self._faults.values())):
            armed.revert()
        self._faults.clear()
        port = self._api.port if self._api is not None else None
        stack, self._stack = self._stack, None
        self._entered = False
        if stack is not None:
            stack.close()
        if port is not None:
            self._assert_port_unreachable(port)

    def _start_loop(self, stack: contextlib.ExitStack) -> None:
        loop = asyncio.new_event_loop()
        thread = threading.Thread(
            target=loop.run_forever, name=f"interaction-loop-{self._run_id[:8]}", daemon=True
        )
        thread.start()
        self._loop = loop
        self._loop_thread = thread

        def _stop() -> None:
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=30.0)
            assert not thread.is_alive(), "the harness event loop did not stop"
            loop.close()
            self._loop = None

        stack.callback(_stop)

    def _start_kernel(self, stack: contextlib.ExitStack) -> None:
        """Enter the shared ``kernel_harness`` on the private loop.

        ``approvals=`` is the REAL ``ApprovalClient`` pointed at the composed
        API, so an approval the kernel asks for is a real row in the disposable
        database that the real dispatcher can later resolve. A recording fake
        here would mint ids nothing else knows about, and every ``act`` would
        then be driving an approval that does not exist.

        ``runner_app=`` is the REAL ``curie_runner``, booted through its real
        ``build_runner`` -> ``create_app`` path with only the MODEL faked, so the
        verbs below drive production ACI frames rather than a test double's.
        """

        conftest = kernel_conftest()
        assert self._api is not None and self._env is not None and self._redis is not None

        runner, runner_app = self._build_real_runner(stack)

        async def _enter() -> tuple[Any, Any, Any]:
            import httpx
            from curie_worker.approvals import ApprovalClient

            await runner.start()
            http = httpx.AsyncClient(timeout=30.0)
            approvals = ApprovalClient(
                api_base_url=self._api.base_url,  # type: ignore[union-attr]
                api_key=self._env.api_key,  # type: ignore[union-attr]
                client=http,
                read_timeout_s=10.0,
            )
            cm = conftest.kernel_harness(
                self._names,
                self._redis,
                approvals=approvals,
                actions=self._recorder,
                runner_app=runner_app,
            )
            harness = await cm.__aenter__()
            return cm, harness, http

        cm, harness, http = self._call(_enter(), timeout=120.0, verb="__enter__", what="the kernel")
        self._kernel_harness = harness
        self._approval_http = http

        def _exit() -> None:
            async def _close() -> None:
                with contextlib.suppress(Exception):
                    await cm.__aexit__(None, None, None)
                with contextlib.suppress(Exception):
                    await runner.stop()
                with contextlib.suppress(Exception):
                    await http.aclose()

            with contextlib.suppress(Exception):
                self._call(_close(), timeout=60.0, verb="__exit__", what="the kernel")

        stack.callback(_exit)

    def _build_real_runner(self, stack: contextlib.ExitStack) -> tuple[Any, Any]:
        """The REAL runner, with the model seam faked, as an aiohttp app.

        This is what makes "a real runner behind the verbs" a property of
        ``send``/``act`` rather than of one bespoke test: the kernel dials this
        app, so a turn driven through the harness compiles a real bundle, runs
        the real session loop and emits production ACI frames. The MODEL is the
        only stand-in, exactly as the two named ones allow.
        """

        import tempfile

        from curie_runner import create_app
        from curie_runner.__main__ import build_runner
        from curie_runner.config import RunnerConfig

        tmp = tempfile.TemporaryDirectory(prefix="interaction-runner-")
        stack.callback(tmp.cleanup)
        plugin_dir = Path(tmp.name) / "bundle"
        (plugin_dir / ".claude-plugin").mkdir(parents=True)
        (plugin_dir / ".claude-plugin" / "plugin.json").write_text(
            json.dumps({"name": "interaction-bot"}), encoding="utf-8"
        )
        config = RunnerConfig.from_env(
            {
                "CURIE_PLUGIN_DIR": str(plugin_dir),
                "CURIE_SESSION_ID": f"interaction-{self._run_id[:8]}",
                "CURIE_SANDBOX_ID": f"interaction-sandbox-{self._run_id[:8]}",
                "CURIE_BUDGET": '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}',
            }
        )
        runner = build_runner(config, fake_model=True)
        self._runner = runner
        return runner, create_app(runner)

    def _call(self, coro: Any, *, timeout: float, verb: str, what: str) -> Any:
        """Run one coroutine on the private loop, bounded.

        Bounded even for setup and teardown: a wedged kernel coroutine on a
        daemon thread would otherwise hang the process with nothing in the report
        naming it.
        """

        loop = self._loop
        assert loop is not None, "the harness is not entered"
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        started = time.monotonic()
        try:
            return future.result(timeout=timeout)
        except TimeoutError:
            future.cancel()
            raise HarnessTimeout(
                verb=verb,
                what=what,
                deadline_s=timeout,
                elapsed_s=time.monotonic() - started,
            ) from None

    def _assert_port_unreachable(self, port: int) -> None:
        """The composed API's port must refuse connections after teardown.

        Asserted rather than assumed: a server thread that outlived its context
        is a live, authenticated approval surface on a loopback port, and nothing
        else in the process would ever notice.
        """

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.settimeout(1.0)
            try:
                probe.connect(("127.0.0.1", port))
            except OSError:
                return
        raise AssertionError(
            f"the composed API on port {port} still accepts connections after teardown"
        )

    def _purge_namespace(self) -> None:
        client = self._redis
        if client is None:
            return
        for pattern in (
            f"{self._names['prefix']}*",
            f"{self._names['sandbox_prefix']}*",
            self._names["stream"],
        ):
            with contextlib.suppress(Exception):
                keys = list(client.scan_iter(match=pattern))
                if keys:
                    client.delete(*keys)

    def _require_entered(self) -> None:
        if not self._entered:
            raise AssertionError(
                "the InteractionHarness must be used as a context manager: "
                "``with InteractionHarness() as harness:``"
            )

    # -- identity -------------------------------------------------------------

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def stream(self) -> str:
        return self._names["stream"]

    @property
    def redis(self) -> redis.Redis:
        assert self._redis is not None, "the harness is not entered"
        return self._redis

    @property
    def api(self) -> ComposedApi:
        assert self._api is not None, "the harness is not entered"
        return self._api

    @property
    def env(self) -> JourneyEnv:
        assert self._env is not None, "the harness is not entered"
        return self._env

    @property
    def kernel(self) -> Any:
        """The composed kernel harness, for assertions the verbs do not cover."""

        assert self._kernel_harness is not None, "the harness is not entered"
        return self._kernel_harness

    @property
    def runner(self) -> Any:
        """The REAL ``curie_runner`` the kernel dials, with only its model faked.

        Exposed so a test can assert against the runner's own state -- its fake
        model session's recorded queries are the proof that a turn driven
        through ``send`` really went through the production turn loop, and not
        through a scripted double.
        """

        self._require_entered()
        assert self._runner is not None, "the harness is not entered"
        return self._runner

    @property
    def recorder(self) -> JourneyRecorder:
        return self._recorder

    @property
    def sessionmaker(self) -> Any:
        """The REAL sessionmaker the API's lifespan composed (main.py:78-95)."""

        self._require_entered()
        return self.api.app.state.sessionmaker

    @property
    def resume_queue(self) -> Any:
        """The REAL ``ResumeQueue`` the API resolves through."""

        self._require_entered()
        return self.api.app.state.resume_queue

    @property
    def approval_id(self) -> str | None:
        """The approval this run is currently about, if one has been created."""

        return self._approval_id

    @property
    def armed_faults(self) -> tuple[str, ...]:
        return tuple(self._faults)

    # -- verbs ----------------------------------------------------------------

    def send(self, text: str, *, thread: str | None = None, deadline_s: float = 60.0) -> SendResult:
        """Drive one inbound turn through the REAL kernel.

        PRODUCTION shape end to end, and one half of it only: the turn is xadded
        to the run's stream with the dispatcher's own serialiser and then the
        REAL worker ``Consumer`` reads it. Never both an xadd and a direct
        ``process_event`` -- that pairing has no production counterpart and lets
        two processors race one event id.

        The first turn on a thread carries the fake model's approval MARKER, so
        the REAL runner takes the production ``request_approval`` path: the
        kernel creates a real record through the real ``ApprovalClient`` and
        emits the real ``ConfirmIntent`` post. A SECOND turn on a thread that is
        already suspended is a follow-up, not a second approval request, and is
        sent unmarked -- a second card would queue a second pending decision for
        one human and make ``messages()`` ambiguous about which card ``act``
        should drive.
        """

        self._require_entered()
        guard = _Deadline(verb="send", deadline_s=deadline_s)
        with self._bounded(guard):
            return self._send(text, thread=thread, guard=guard)

    def _send(self, text: str, *, thread: str | None, guard: _Deadline) -> SendResult:
        from aci_protocol import QueuedTurn as _QueuedTurn
        from aci_protocol import ReplyHandle, TurnSource
        from curie_runner.fake import APPROVAL_MARKER

        self._sends += 1
        thread_id = thread or f"th-{self._run_id[:8]}-{self._sends}"
        event_id = uuid.uuid4().hex
        first_turn = thread_id not in self._threads_awaiting
        # The turn is steered through the REAL runner's fake-model marker, not
        # by scripting a test double's frames: the fake model's default script
        # branches on this marker and calls the production
        # ``mcp__curie__request_approval`` tool, so the approval the kernel ends
        # up creating came out of the real runner's real turn loop.
        prompt = f"{APPROVAL_MARKER} {text}" if first_turn else text

        qevent = _QueuedTurn(
            event_id=event_id,
            conversation_id=thread_id,
            author=HARNESS_AUTHOR,
            text=prompt,
            reply_handle=ReplyHandle(
                kind="slack", channel=HARNESS_CHANNEL, placeholder=f"p-{self._sends}"
            ),
            received_at=_now_iso(),
            source=TurnSource.SLACK,
        )
        # PRODUCTION SHAPE, and exactly one of the two halves of it. The
        # dispatcher xadds every inbound turn to the run's stream and the worker
        # CONSUMES it; the API's ``ResumeQueue`` xadds resumes to that same
        # stream (resumequeue.py:5,252-257). An earlier version did both -- xadd
        # AND a direct ``kernel.process_event`` on the same ``QueuedTurn`` --
        # which is a shape production never has: the entry is readable by a live
        # ``Consumer`` while ``process_event`` is still in flight, so one event
        # id gets two concurrent processors and the terminal-marker skip only
        # helps the loser. So: xadd, then run the REAL consumer over THIS entry
        # and stop it. The stream still carries the inbound turn (a run's
        # namespace is not empty after a send, and a stream-entry predicate has
        # a truthful thing to observe), and nothing ever processes it twice.
        from curie_dispatcher.queue import to_stream_fields

        fields: Any = to_stream_fields(qevent)
        # The group offset must exist BEFORE the entry lands or the consumer
        # never sees it.
        self._call(
            self._ensure_consumer_group(),
            timeout=max(_MIN_CALL_S, guard.remaining_s),
            verb="send",
            what="the consumer group to exist",
        )
        self._redis_call(
            lambda: self.redis.xadd(self.stream, fields),
            what=f"the inbound turn {event_id} to be queued",
        )
        self._call(
            self._consume_one(event_id),
            timeout=max(_MIN_CALL_S, guard.remaining_s),
            verb="send",
            what=f"the worker to consume the turn {event_id}",
        )
        if first_turn:
            self._threads_awaiting.add(thread_id)
        self._capture()
        return SendResult(
            message_id=f"snd-{event_id[:12]}",
            thread=thread_id,
            run_id=self._run_id,
            event_id=event_id,
            text=text,
        )

    def messages(self) -> MessagesResult:
        """Everything captured on the channel edge, with each card's actions."""

        self._require_entered()
        self._capture()
        messages = tuple(self._messages)
        cards = tuple(
            CapturedCard(
                message_id=message.message_id,
                approval_id=message.approval_id,
                actions=message.actions,
                message=message,
            )
            for message in messages
            if message.actions
        )
        return MessagesResult(messages=messages, cards=cards)

    def act(
        self,
        *,
        message: str | CapturedMessage | CapturedCard,
        action: CapturedAction | Any,
        actor: str,
        note: str | None = None,
        deadline_s: float = 30.0,
    ) -> ActResult:
        """Drive one CAPTURED action through the real dispatcher and API.

        ``action`` must be a :class:`CapturedAction` that came out of
        :meth:`messages`. A bare string raises :class:`UncapturedAction` even
        when the string is the right action id -- see the class docstring for
        why provenance rather than validity is the contract.
        """

        self._require_entered()
        guard = _Deadline(verb="act", deadline_s=deadline_s)
        # Bounded as a WHOLE, not just in its polling loops: the status
        # read-back and every other HTTP call reached from inside this verb
        # clips its own timeout to what is left of ``guard`` (``_budget``).
        with self._bounded(guard):
            return self._act(message=message, action=action, actor=actor, note=note, guard=guard)

    def _act(
        self,
        *,
        message: str | CapturedMessage | CapturedCard,
        action: CapturedAction | Any,
        actor: str,
        note: str | None,
        guard: _Deadline,
    ) -> ActResult:
        captured = self._require_captured_action(message, action)
        card = self._cards[captured.message_id]
        button = _button_for(card, captured)
        from curie_dispatcher.approval_actions import (
            APPROVE_NOTE_ACTION_ID,
            NOTE_MODAL_CALLBACK_ID,
            REJECT_NOTE_ACTION_ID,
        )
        from slack_bolt.adapter.socket_mode import SocketModeHandler

        assert self._resolver_factory is not None

        def _build() -> Any:
            assert self._resolver_factory is not None
            resolver = self._resolver_factory()
            wrap_resolve_client(resolver, self._faults)
            built_app, built_web_client = self._build_dispatcher(resolver, card)
            return built_app, built_web_client, SocketModeHandler(built_app, app_token="xapp-test")

        app, web_client, handler = guard.run(_build, what="the per-call dispatcher app to be built")
        note_path = captured.action_id in {APPROVE_NOTE_ACTION_ID, REJECT_NOTE_ACTION_ID}
        ack_body: Any = None
        try:
            click_socket = _CapturingSocket()
            click_envelope = f"env-{uuid.uuid4().hex[:8]}"
            click_request = _click_request(
                click_envelope,
                card=card,
                button=button,
                user=actor,
                channel=HARNESS_CHANNEL,
            )
            guard.run(
                lambda: handler.handle(click_socket, click_request),
                what="the click to be dispatched",
            )
            if note_path:
                guard.wait(
                    lambda: bool(web_client.views_open.call_args),
                    what="the note modal to open after the click",
                )
                view = web_client.views_open.call_args.kwargs["view"]
                assert view["callback_id"] == NOTE_MODAL_CALLBACK_ID
                metadata = str(view["private_metadata"])
                if "private_metadata_unusable" in self._faults:
                    # Tampered, not replaced: the refusal under test is the
                    # dispatcher refusing metadata it cannot trust, and a wholly
                    # invented string would also fail a shape check that is not
                    # the thing being proven.
                    metadata = metadata[:-1] + ("x" if not metadata.endswith("x") else "y")
                submit_socket = _CapturingSocket()
                submit_envelope = f"env-{uuid.uuid4().hex[:8]}"
                submit_request = _view_submission_request(
                    submit_envelope,
                    private_metadata=metadata,
                    user=actor,
                    note=_DEFAULT_NOTE if note is None else note,
                    callback_id=NOTE_MODAL_CALLBACK_ID,
                )
                guard.run(
                    lambda: handler.handle(submit_socket, submit_request),
                    what="the note submission to be dispatched",
                )
                guard.wait(
                    lambda: submit_envelope in submit_socket.acked_envelope_ids,
                    what="the note submission to be acked",
                )
                ack_body = submit_socket.ack_payload_for(submit_envelope)
            else:
                guard.wait(
                    lambda: click_envelope in click_socket.acked_envelope_ids,
                    what="the click to be acked",
                )
                ack_body = click_socket.ack_payload_for(click_envelope)
            self._drain(app, guard)
        finally:
            # The Bolt listener executor is shut down exactly once, at the end,
            # and never reused: ``shutdown(wait=True)`` is terminal, so a second
            # envelope through the same app dies with "cannot schedule new
            # futures after shutdown". One app per ``act`` is what makes that
            # safe.
            with contextlib.suppress(Exception):
                app.listener_runner.listener_executor.shutdown(wait=False)

        self._status_cache = (0.0, None)
        rendered = web_client.chat_update.call_args
        rendered_text = None if rendered is None else str(rendered.kwargs.get("text", ""))
        outcome = guard.run(
            lambda: self._approval_status(captured.value),
            what="the approval status to be read back after the click",
        )
        # An empty ack body is the ACCEPTED submission: a refusal carries
        # ``response_action: errors`` (handlers.py:637-645).
        accepted = ack_body is None
        self._capture()
        return ActResult(
            action_id=captured.action_id,
            message_id=captured.message_id,
            actor=actor,
            accepted=accepted,
            outcome=outcome,
            note=None if note_path is False else (_DEFAULT_NOTE if note is None else note),
            detail="" if ack_body is None else json.dumps(ack_body, default=str),
            response_action=ack_body if isinstance(ack_body, dict) else None,
            rendered_text=rendered_text,
            approval_id=captured.value,
        )

    def create_approval(self, **overrides: Any) -> ApprovalRecord:
        """Create a pending approval through the REAL ``POST /approvals``.

        The same route an operator hits, with the platform key. Offered beside
        :meth:`send` (which reaches the same route THROUGH the kernel) because a
        failure case often needs a pending record without also needing a turn to
        be suspended behind it -- and inventing a row with a direct SQL insert
        would skip the API's own validation, which is part of what the journey
        asserts.
        """

        self._require_entered()
        body: dict[str, Any] = {
            "conversation_id": f"th-{uuid.uuid4().hex[:8]}",
            "author": HARNESS_AUTHOR,
            "summary": "Scale the payments deployment to 10 replicas",
            "reply_kind": "slack",
            "reply_channel": HARNESS_CHANNEL,
            "reply_placeholder": "p-1",
            "dedupe_key": f"ev-{uuid.uuid4().hex}",
            "card_channel": HARNESS_CHANNEL,
        }
        body.update(overrides)
        payload = self._api_call("POST", "/approvals", json=body, expect=(200, 201))
        self._approval_id = str(payload["id"])
        self._status_cache = (0.0, None)
        return _approval_record(payload)

    def resolve(
        self,
        *,
        approval_id: str,
        decision: str,
        actor: str,
        channel: str = HARNESS_CHANNEL,
        note: str | None = None,
        deadline_s: float = 30.0,
    ) -> ActResult:
        """Resolve an approval through the real resolve client, WITHOUT a card.

        INTERNAL, and deliberately NOT exposed as a CLI verb. It takes a literal
        approval id and decision with no captured card, which is exactly the
        shortcut :meth:`act`'s provenance rule exists to forbid; handing an agent
        scripting the harness that shortcut would retire the rule in a release.
        Its callers are the failure tests that have no card to click.

        Deliberately a separate, explicitly named verb rather than a relaxation
        of :meth:`act`. Some cases -- a resolution whose enqueue was dropped, a
        resolve whose transport fails -- are about the resolve hop itself and
        have no card to click; letting ``act`` take a bare id to serve them would
        hand every future test the same shortcut, and the provenance rule would
        be gone within a release. The name is the honesty: a case that calls
        ``resolve`` is visibly NOT claiming a chat click drove it.
        """

        self._require_entered()
        assert self._resolver_factory is not None
        guard = _Deadline(verb="resolve", deadline_s=deadline_s)
        with self._bounded(guard):
            return self._resolve(
                approval_id=approval_id,
                decision=decision,
                actor=actor,
                channel=channel,
                note=note,
                guard=guard,
            )

    def _resolve(
        self,
        *,
        approval_id: str,
        decision: str,
        actor: str,
        channel: str,
        note: str | None,
        guard: _Deadline,
    ) -> ActResult:
        assert self._resolver_factory is not None
        resolver = self._resolver_factory()
        wrap_resolve_client(resolver, self._faults)
        outcome_box: list[Any] = []
        error_box: list[BaseException] = []

        def _run() -> None:
            try:
                outcome_box.append(
                    resolver.resolve(
                        approval_id,
                        decision=decision,
                        attested_user=actor,
                        attested_channel=channel,
                        note=note,
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced through the box
                error_box.append(exc)

        # On a worker thread with a joined deadline, for the same reason every
        # other wait here is bounded: an armed ``hang`` fault parks the call for
        # longer than the verb's budget, and a direct call would block the
        # caller with no way to report which verb wedged.
        worker = threading.Thread(target=_run, name="interaction-resolve", daemon=True)
        worker.start()
        guard.wait(lambda: not worker.is_alive(), what=f"the resolve of {approval_id}")
        if error_box:
            raise error_box[0]
        outcome = outcome_box[0]
        self._status_cache = (0.0, None)
        record = self.approval(approval_id)
        return ActResult(
            # Empty on purpose, and now informative rather than merely blank:
            # this verb drove no card and no view, so there is no action id, no
            # message id, no ack body and no ``chat_update`` -- inventing any of
            # them would claim a chat surface the product never rendered here.
            # The identity of what WAS driven rides in ``approval_id``, and the
            # resolve hop's real answer in ``status_code``.
            action_id="",
            message_id="",
            actor=actor,
            accepted=200 <= int(outcome.status_code) < 300,
            outcome=record.status,
            note=note,
            detail=str(outcome.detail or ""),
            response_action=None,
            rendered_text=None,
            approval_id=approval_id,
            status_code=int(outcome.status_code),
        )

    def approval(self, approval_id: str) -> ApprovalRecord:
        """One approval row, over the REAL ``GET /approvals/{id}``."""

        self._require_entered()
        payload = self._api_call("GET", f"/approvals/{approval_id}", expect=(200,))
        return _approval_record(payload)

    def audit(self, approval_id: str) -> AuditResult:
        """One approval's audit trail, over the REAL ``GET /approvals/{id}/audit``.

        The audit trail is where the authorizer NAMES itself, which is what keeps
        a refusal assertion honest: an unseeded route refuses everyone through
        ``UnboundRouteBinding``, so a test asserting only "refused" would pass
        without the authorizer under test ever running.
        """

        self._require_entered()
        rows = self._api_call("GET", f"/approvals/{approval_id}/audit", expect=(200,))
        assert isinstance(rows, list), f"audit did not return a list: {rows!r}"
        return AuditResult(
            approval_id=approval_id,
            entries=tuple(_audit_entry(row) for row in rows),
        )

    def resume_turns(self) -> ResumeTurnsResult:
        """Every resume turn currently on the run's stream, decoded.

        Read off the stream the API's real ``ResumeQueue.enqueue`` xadded to, so
        "a resume was queued" means an entry exists rather than meaning a mock
        was called.
        """

        self._require_entered()
        from aci_protocol import STREAM_PAYLOAD_FIELD
        from aci_protocol.ndjson import parse_queued_turn
        from curie_api.resumequeue import parse_resume_event_id

        rows: Any = self._redis_call(
            lambda: self.redis.xrange(self.stream, "-", "+"),
            what=f"the stream {self.stream} to be read back",
        )
        turns = []
        for entry_id, fields in rows or []:
            payload = (fields or {}).get(STREAM_PAYLOAD_FIELD)
            if payload is None:
                continue
            turn = parse_queued_turn(payload)
            # RESUME turns only. The run's stream carries inbound turns too --
            # ``send`` xadds one with the dispatcher's own serialiser, exactly as
            # production does -- so an unfiltered read reported "two resumes" for
            # one send plus one resolve, and every "exactly one resume" assertion
            # in the failure suite was counting the wrong population. The filter
            # is the PRODUCTION discriminator (``parse_resume_event_id``, the
            # inverse of the key ``ResumeQueue`` enqueues under), not a local
            # guess at what a resume looks like.
            if parse_resume_event_id(turn.event_id) is None:
                continue
            turns.append(
                ResumeTurn(
                    entry_id=str(entry_id),
                    event_id=turn.event_id,
                    conversation_id=turn.conversation_id,
                    text=turn.text,
                )
            )
        return ResumeTurnsResult(stream=self.stream, turns=tuple(turns))

    def await_outcome(
        self, *, predicate: Callable[[Snapshot], bool], deadline_s: float
    ) -> OutcomeResult:
        """Poll until ``predicate`` accepts a snapshot, or raise ``HarnessTimeout``."""

        self._require_entered()
        guard = _Deadline(verb="await_outcome", deadline_s=deadline_s)
        started = time.monotonic()
        # Under the guard, not merely around it: the predicate reads
        # ``snapshot()``, which reads the approval's status over HTTP, and that
        # read carries its own 10s timeout. Outside the budget a single hung
        # read holds a 1s verb for ten seconds -- the bound would be honoured by
        # the polling loop and broken by the thing the loop calls.
        with self._bounded(guard):
            guard.wait(
                lambda: predicate(self.snapshot()),
                what="the await_outcome predicate to become true",
            )
            snapshot = self.snapshot()
            record = self.approval(self._approval_id) if self._approval_id else None
            resume_turns = self.resume_turns().turns
        return OutcomeResult(
            satisfied=True,
            elapsed_s=time.monotonic() - started,
            deadline_s=deadline_s,
            stream_entries=snapshot.stream_entries,
            message_count=len(snapshot.messages),
            approval_id=self._approval_id,
            approval_status=None if record is None else record.status,
            resolved_by=None if record is None else record.resolved_by,
            resume_turns=resume_turns,
        )

    def snapshot(self) -> Snapshot:
        """The current observable state, as a value a predicate can read."""

        self._capture()
        return Snapshot(
            run_id=self._run_id,
            messages=tuple(self._messages),
            stream_entries=int(
                self._redis_call(
                    lambda: self.redis.xlen(self.stream),
                    what=f"the stream {self.stream} to report its length",
                )
            ),
            approval_status=self._cached_status(),
            approval_id=self._approval_id,
        )

    def reset(self, *, deadline_s: float = 30.0) -> ResetResult:
        """Empty the run's stream namespace, leaving the harness usable.

        Against the store, not against bookkeeping: a reset that zeroed a counter
        and left the entries in Valkey would make the NEXT step's "exactly one
        stream entry" read two. Idempotent because an agent resets defensively
        between steps and has no cheap way to know whether anything happened.

        Bounded like every other blocking verb. It was the one of the six that
        took no ``deadline_s`` at all, on the reasoning that two Valkey commands
        are fast -- true right up until Valkey is the thing that is wedged, at
        which point the "defensive reset between steps" an agent is told to make
        is an unbounded hang with no verb name on it.
        """

        self._require_entered()
        guard = _Deadline(verb="reset", deadline_s=deadline_s)
        with self._bounded(guard):
            self._redis_call(
                lambda: self.redis.delete(self.stream),
                what=f"the stream {self.stream} to be deleted",
            )
            remaining = int(
                self._redis_call(
                    lambda: self.redis.exists(self.stream),
                    what=f"the stream {self.stream} to report itself gone",
                )
            )
        assert remaining == 0, f"the stream {self.stream!r} survived a reset"
        return ResetResult(stream=self.stream, entries_remaining=0)

    @contextlib.contextmanager
    def inject_fault(self, name: str, **kwargs: Any) -> Iterator[ArmedFault]:
        """Arm one fault for the body, and revert it in a ``finally``.

        The revert is in a ``finally`` specifically because of the exceptional
        path: an implementation without one passes every happy-path test and then
        fails as an unrelated test, in a later module, with no trace back to the
        fault that caused it.
        """

        armed = self.arm_fault(name, **kwargs)
        try:
            yield armed
        finally:
            self.disarm_fault(name)

    def arm_fault(self, name: str, **kwargs: Any) -> ArmedFault:
        """Arm one fault until it is disarmed, WITHOUT a scope to revert it.

        The scoped :meth:`inject_fault` is the in-process form and the one to
        prefer; this pair exists because the ``python -m`` entry point is a
        sequence of independent NDJSON lines with no Python block to hang a
        ``with`` on, so an agent on the pipe could otherwise arm no fault at all
        and half the harness's failure surface was unreachable from it.
        ``__exit__`` reverts whatever is still armed, so a script that forgets
        to disarm still cannot leak a patch past the harness.
        """

        self._require_entered()
        if name in self._faults:
            raise ValueError(f"fault {name!r} is already armed: {self.armed_faults}")
        armed = arm_fault(name, **kwargs)
        self._faults[name] = armed
        return armed

    def disarm_fault(self, name: str) -> FaultResult:
        """Revert one armed fault. Idempotent, so a defensive disarm is safe."""

        self._require_entered()
        armed = self._faults.pop(name, None)
        if armed is not None:
            armed.revert()
        return FaultResult(fault=name, armed=False, armed_faults=self.armed_faults)

    # -- internals ------------------------------------------------------------

    @staticmethod
    def _message_id_of(message: Any) -> str:
        """The message id a caller named, however they named it.

        ``messages()`` hands back three things that all identify one message -- a
        ``CapturedMessage``, a ``CapturedCard`` and a bare id string -- and every
        one of them is a reasonable thing to pass to ``act(message=...)``. Taking
        only the string made the provenance check below compare a ``str`` against
        a dataclass, which is never equal, so a legitimately captured action was
        refused with a message that printed the whole object.
        """

        if isinstance(message, CapturedMessage | CapturedCard):
            return message.message_id
        return str(message)

    def _require_captured_action(
        self, message: str | CapturedMessage | CapturedCard, action: Any
    ) -> CapturedAction:
        if not isinstance(action, CapturedAction):
            raise UncapturedAction(
                f"act() refuses {action!r}: an action must be a CapturedAction "
                "object taken from messages(), not a literal action id. The "
                "harness proves a real click drove the system, and a literal "
                "skips the card render, the ownership probe and the block "
                "structure while still reporting a green journey."
            )
        message_id = self._message_id_of(message)
        # Matched on OBJECT IDENTITY against the actions this harness itself
        # captured, never on content. ``_capture`` appends each
        # ``CapturedMessage`` exactly once and ``messages()`` hands back
        # ``tuple(self._messages)``, so the very objects a caller holds ARE the
        # stored ones -- identity costs a legitimate caller nothing, including
        # one holding an action from an older ``messages()`` snapshot.
        #
        # Content matching is what this replaces, and it was a hole rather than
        # a convenience: ``(message_id, action_id, value)`` is all reconstructible
        # from an imported action-id constant, the harness's ``1700.%04d``
        # message-id scheme and an approval id, so a caller could hand-build a
        # ``CapturedAction`` and skip the card entirely -- the render, the
        # ownership probe, the block structure -- which is the exact shortcut
        # ``act`` exists to refuse.
        seen_on: set[str] = set()
        for candidate in self._messages:
            for known in candidate.actions:
                if known is not action:
                    continue
                seen_on.add(known.message_id)
                if known.message_id == message_id:
                    return known
        if seen_on:
            raise UncapturedAction(
                f"act() refuses {action.action_id!r}: it was captured on "
                f"message(s) {sorted(seen_on)!r}, not {message_id!r}"
            )
        looks_captured = any(
            (known.message_id, known.action_id, known.value)
            == (action.message_id, action.action_id, action.value)
            for candidate in self._messages
            for known in candidate.actions
        )
        if looks_captured:
            raise UncapturedAction(
                f"act() refuses {action.action_id!r}: its fields match a captured "
                "action but the object itself did not come out of messages(). A "
                "hand-built CapturedAction skips the card render, the ownership "
                "probe and the block structure while still reporting a green "
                "journey, so provenance is the object, not its contents -- pass "
                "the action from messages() itself."
            )
        raise UncapturedAction(
            f"act() refuses {action.action_id!r}: no such action was captured on "
            "this harness; it did not come from messages()"
        )

    def _build_dispatcher(self, resolver: Any, card: dict[str, Any]) -> tuple[Any, Any]:
        """The real dispatcher app, with only Slack's own surface faked."""

        from unittest.mock import MagicMock

        from curie_dispatcher.app import build_app
        from curie_dispatcher.config import DispatcherConfig
        from slack_sdk.web import WebClient

        config = DispatcherConfig(
            slack_app_token="xapp-test",
            slack_bot_token="xoxb-test",
            valkey_host=_VALKEY_HOST,
            valkey_port=_VALKEY_PORT,
            valkey_password=_VALKEY_PW,
            stream=self.stream,
            dedupe_prefix=f"test:curie:dedupe:{uuid.uuid4().hex}:",
            dedupe_ttl_seconds=60,
            placeholder_text="Working on it.",
            approval_chat_attester_secret=self.env.attester_secret,
        )
        web_client = WebClient(token="xoxb-test")
        web_client.chat_postMessage = MagicMock(return_value={"ts": "555.000"})  # type: ignore[method-assign]
        web_client.chat_update = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
        web_client.chat_postEphemeral = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
        web_client.views_open = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
        web_client.conversations_replies = MagicMock(  # type: ignore[method-assign]
            return_value={"messages": [card]}
        )
        app = build_app(
            config,
            web_client=web_client,
            redis_client=self.redis,
            authorize=_authorize,
            resolver=resolver,
        )
        return app, web_client

    def _new_consumer(self) -> Any:
        """A REAL worker ``Consumer`` over this run's stream.

        One per ``send``, never reused: ``request_stop`` is terminal, so a second
        ``run()`` on a stopped consumer returns immediately and the entry sits
        unread until the verb's deadline.
        """

        from curie_worker.consumer import Consumer

        return Consumer(
            redis=self.kernel.async_redis,
            kernel=self.kernel.kernel,
            config=self.kernel.config,
        )

    async def _ensure_consumer_group(self) -> None:
        self._consumer_obj = self._new_consumer()
        await self._consumer_obj.ensure_group()

    async def _consume_one(self, event_id: str) -> None:
        """Run the real consumer until it has finished ONE entry, then stop it.

        Run per send rather than left running for the harness's lifetime: a
        background consumer would also pick up the resume entries the API
        enqueues, so a verb that asserts "a resume is queued" would be racing a
        consumer that drives a further turn and posts further messages. One
        consumer, one entry, joined -- the production hop, without a second
        processor anywhere in the picture.
        """

        consumer, self._consumer_obj = self._consumer_obj, None
        assert consumer is not None, "the consumer group was not ensured before the xadd"
        sink = self._kernel_harness.sink
        before = len(sink.posts)
        task = asyncio.create_task(consumer.run())
        try:
            deadline = time.monotonic() + _CONSUME_TIMEOUT_S
            while True:
                done = any(c.event_id == event_id for c in sink.completions)
                if done or len(sink.posts) > before:
                    return
                if time.monotonic() >= deadline:
                    raise HarnessTimeout(
                        verb="send",
                        what=f"the worker to finish the turn {event_id}",
                        deadline_s=_CONSUME_TIMEOUT_S,
                        elapsed_s=_CONSUME_TIMEOUT_S,
                    )
                await asyncio.sleep(0.01)
        finally:
            consumer.request_stop()
            try:
                await asyncio.wait_for(task, timeout=30.0)
            except TimeoutError:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                raise AssertionError(
                    "the consumer did not stop within 30s of request_stop()"
                ) from None

    def _drain(self, app: Any, guard: _Deadline) -> None:
        """Wait for every post-ack listener body to finish, bounded.

        ``shutdown(wait=True)`` takes no deadline, so it runs on a helper thread
        that IS joined with one: a listener body wedged on the loopback API would
        otherwise hang the caller with nothing in the report to find it by.
        """

        executor = app.listener_runner.listener_executor
        closer = threading.Thread(
            target=lambda: executor.shutdown(wait=True), name="interaction-drain", daemon=True
        )
        closer.start()
        guard.wait(lambda: not closer.is_alive(), what="the Bolt listener executor to drain")

    def _api_call(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        expect: tuple[int, ...] = (200,),
    ) -> Any:
        """One authenticated call to the composed API, with the platform key."""

        import httpx

        what = f"{method} {path} to answer"
        timeout = self._budget(_HTTP_TIMEOUT_S, what=what)
        with httpx.Client(timeout=timeout) as client:
            try:
                response = client.request(
                    method,
                    f"{self.api.base_url}{path}",
                    json=json,
                    headers={"X-API-Key": self.env.api_key},
                )
            except httpx.TimeoutException as exc:
                raise self._timed_out(what, exc, timeout=timeout, default=_HTTP_TIMEOUT_S) from exc
        assert response.status_code in expect, (
            f"{method} {path} answered HTTP {response.status_code} "
            f"(expected one of {expect}): {response.text}"
        )
        return response.json()

    def _cached_status(self) -> str | None:
        """The current approval's status, re-read at most every 200ms.

        Cached because ``await_outcome`` polls every 20ms and a predicate that
        reads the status would otherwise issue fifty HTTP requests a second. The
        window is short enough that no wait is meaningfully lengthened by it, and
        every verb that CHANGES a status invalidates the cache rather than
        waiting it out.
        """

        if self._approval_id is None:
            return None
        cached_at, value = self._status_cache
        now = time.monotonic()
        if value is not None and now - cached_at < 0.2:
            return value
        status = self._approval_status(self._approval_id)
        self._status_cache = (now, status)
        return status

    def _approval_status(self, approval_id: str) -> str:
        """The approval row's status, read over the REAL API with the platform key."""

        import httpx

        what = f"the approval {approval_id} to be read back"
        timeout = self._budget(_HTTP_TIMEOUT_S, what=what)
        with httpx.Client(timeout=timeout) as client:
            try:
                response = client.get(
                    f"{self.api.base_url}/approvals/{approval_id}",
                    headers={"X-API-Key": self.env.api_key},
                )
            except httpx.TimeoutException as exc:
                raise self._timed_out(what, exc, timeout=timeout, default=_HTTP_TIMEOUT_S) from exc
        if response.status_code != 200:
            return f"unreadable:{response.status_code}"
        return str(response.json()["status"])

    def _capture(self) -> None:
        """Project any new kernel posts onto the captured channel state.

        The card is rendered HERE, from the ``ConfirmIntent`` the kernel emitted,
        using the production ``approval_card`` renderer with the intent's own
        ``allow_free_text``. That is the coupling that keeps ``act`` honest: the
        action ids and values come out of the real render of the real intent, so
        a change to either moves every captured action with it.
        """

        if self._kernel_harness is None:
            return
        from curie_worker.blocks import approval_card

        posts = self._kernel_harness.sink.posts
        while len(self._messages) < len(posts):
            index = len(self._messages)
            _channel, message, requested_by, thread_ts, _endpoint = posts[index]
            message_id = f"1700.{index:04d}"
            intent = getattr(message, "interaction", None)
            actions: tuple[CapturedAction, ...] = ()
            approval_id: str | None = None
            text = getattr(message, "text", "") or ""
            if intent is not None and getattr(intent, "kind", None) == "confirm":
                approval_id = str(intent.id)
                self._approval_id = approval_id
                self._status_cache = (0.0, None)
                fallback, blocks = approval_card(
                    approval_id=approval_id,
                    summary=intent.prompt,
                    requested_by=requested_by,
                    allow_free_text=intent.allow_free_text,
                )
                text = fallback
                self._cards[message_id] = {
                    "ts": message_id,
                    "thread_ts": thread_ts or message_id,
                    "text": fallback,
                    "blocks": blocks,
                }
                elements = [b for b in blocks if b["type"] == "actions"]
                assert len(elements) == 1, f"expected one actions block, got {len(elements)}"
                actions = tuple(
                    CapturedAction(
                        action_id=str(element["action_id"]),
                        message_id=message_id,
                        value=str(element["value"]),
                        text=str(element.get("text", {}).get("text", "")),
                        # Minted HERE, once, as the action is captured off the
                        # real render. ``secrets`` rather than a counter or a
                        # hash of the fields: anything derived from the card is
                        # reconstructible by a caller who never saw the card,
                        # which is the whole hole this closes.
                        handle=secrets.token_urlsafe(16),
                    )
                    for element in elements[0]["elements"]
                )
            self._messages.append(
                CapturedMessage(
                    message_id=message_id,
                    thread=str(thread_ts or ""),
                    text=text,
                    approval_id=approval_id,
                    actions=actions,
                )
            )


class _Deadline:
    """One verb's bound, shared across the several waits inside it.

    Shared rather than per-wait: ``act`` waits three times (the modal, the ack,
    the drain), and three independent ``deadline_s`` bounds would let the verb
    take three times the bound its caller asked for -- which is exactly the
    overrun the contract tests measure by wall clock.
    """

    def __init__(self, *, verb: str, deadline_s: float) -> None:
        self.verb = verb
        self.deadline_s = deadline_s
        self.started = time.monotonic()

    @property
    def elapsed_s(self) -> float:
        return time.monotonic() - self.started

    @property
    def remaining_s(self) -> float:
        """What is LEFT of this verb's budget, never negative.

        The number every blocking segment inside the verb has to be clipped to.
        A segment that carries its own fixed timeout (an ``httpx`` read, a
        Valkey socket) is bounded in its own right and still unbounded with
        respect to the verb: a 10s HTTP read inside a 1s verb overruns by 10x
        while every local wait looks correctly bounded.
        """

        return max(0.0, self.deadline_s - self.elapsed_s)

    def expired(self, *, what: str) -> HarnessTimeout:
        return HarnessTimeout(
            verb=self.verb,
            what=what,
            deadline_s=self.deadline_s,
            elapsed_s=self.elapsed_s,
        )

    def wait(self, predicate: Callable[[], bool], *, what: str) -> None:
        while True:
            if predicate():
                return
            if self.elapsed_s >= self.deadline_s:
                raise HarnessTimeout(
                    verb=self.verb,
                    what=what,
                    deadline_s=self.deadline_s,
                    elapsed_s=self.elapsed_s,
                )
            time.sleep(_POLL_INTERVAL_S)

    def run(self, work: Callable[[], Any], *, what: str) -> Any:
        """Run one BLOCKING segment on a helper thread, bounded by this deadline.

        The reason every blocking segment goes through here rather than being
        called inline: a call that blocks in C (``ThreadPoolExecutor.shutdown``,
        a socket read inside a Bolt listener that ran the click synchronously,
        an ``httpx`` request) takes no deadline of its own, so an inline call
        spends unbounded wall clock INSIDE a verb whose headline contract is
        that it is bounded. Running it on a daemon thread and joining it with
        ``wait`` puts every one of those segments under the same remaining
        budget, and makes the overrun name the segment it was in instead of
        surfacing as a mystery 8s verb.

        The helper thread is deliberately NOT killed on timeout -- Python has no
        safe way to -- so it is a daemon and the exception it may later raise is
        dropped. Correctness comes from the caller abandoning the verb, not from
        the segment stopping.
        """

        box: dict[str, Any] = {}

        def _target() -> None:
            try:
                box["value"] = work()
            except BaseException as exc:  # re-raised on the caller's thread below
                box["error"] = exc

        worker = threading.Thread(target=_target, name=f"interaction-{self.verb}", daemon=True)
        worker.start()
        self.wait(lambda: not worker.is_alive(), what=what)
        error = box.get("error")
        if error is not None:
            raise error
        return box.get("value")


def _approval_record(payload: Any) -> ApprovalRecord:
    body = dict(payload)
    return ApprovalRecord(
        approval_id=str(body.get("id", "")),
        status=str(body.get("status", "")),
        resolved_by=_optional_str(body.get("resolved_by")),
        resolution_note=_optional_str(body.get("resolution_note")),
        raw=_jsonable(body),
    )


def _audit_entry(payload: Any) -> AuditEntry:
    body = dict(payload)
    authorized = body.get("authorized")
    return AuditEntry(
        action=str(body.get("action", "")),
        actor=_optional_str(body.get("actor")),
        authorized=None if authorized is None else bool(authorized),
        authorizer=_optional_str(body.get("authorizer")),
        principal_kind=_optional_str(body.get("principal_kind")),
        actor_channel=_optional_str(body.get("actor_channel")),
        raw=_jsonable(body),
    )


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _jsonable(body: dict[str, Any]) -> dict[str, Any]:
    """Force a response body through a JSON round trip.

    The bodies here already came OUT of ``response.json()``, so this is cheap and
    normally a no-op -- but it is the single place a non-primitive could enter a
    result, and ``to_dict()``'s whole contract is that it cannot. Asserting it
    here fails at the source rather than at the pipe.
    """

    return dict(json.loads(json.dumps(body)))


def _now_iso() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat()


def _button_for(card: dict[str, Any], action: CapturedAction) -> dict[str, Any]:
    """The rendered element the captured action came from, by id AND value."""

    for block in card["blocks"]:
        if block.get("type") != "actions":
            continue
        for element in block["elements"]:
            if element["action_id"] == action.action_id and element["value"] == action.value:
                return dict(element)
    raise UncapturedAction(
        f"act() cannot find {action.action_id!r} on the card it was captured from; "
        "the card render changed underneath the capture"
    )


def _click_request(
    envelope_id: str,
    *,
    card: dict[str, Any],
    button: dict[str, Any],
    user: str,
    channel: str,
) -> Any:
    """A block_actions envelope whose action id and value are DERIVED.

    ``button`` is an element read straight out of the rendered card, so nothing
    here is a literal: a change to the action ids, or to the ``value`` the
    buttons carry, moves this click with it.
    """

    from slack_sdk.socket_mode.request import SocketModeRequest

    return SocketModeRequest(
        type="interactive",
        envelope_id=envelope_id,
        payload={
            "type": "block_actions",
            "trigger_id": f"trig-{envelope_id}",
            "team": {"id": "T1"},
            "user": {"id": user},
            "api_app_id": "A1",
            "token": "verif",
            "container": {"type": "message", "message_ts": card["ts"]},
            "channel": {"id": channel},
            "message": card,
            "actions": [
                {
                    "type": "button",
                    "action_id": button["action_id"],
                    "action_ts": "2.0",
                    "value": button["value"],
                }
            ],
        },
    )


def _view_submission_request(
    envelope_id: str,
    *,
    private_metadata: str,
    user: str,
    note: str | None,
    callback_id: str,
) -> Any:
    """A view_submission envelope built from the CAPTURED modal metadata.

    ``private_metadata`` is read back off the real ``views_open`` call arguments,
    never hand-built: it is what carries the approval id, channel, card ts and
    decision across a submission that has no channel or message of its own
    (approval_actions.py:56-58).
    """

    from slack_sdk.socket_mode.request import SocketModeRequest

    state: dict[str, Any] = {"values": {}}
    if note is not None:
        state["values"] = {"note": {"note-input": {"type": "plain_text_input", "value": note}}}
    return SocketModeRequest(
        type="interactive",
        envelope_id=envelope_id,
        payload={
            "type": "view_submission",
            "team": {"id": "T1"},
            "user": {"id": user},
            "api_app_id": "A1",
            "token": "verif",
            "trigger_id": f"trig-{envelope_id}",
            "view": {
                "id": f"V-{envelope_id}",
                "type": "modal",
                "callback_id": callback_id,
                "private_metadata": private_metadata,
                "state": state,
                "hash": "1",
                "title": {"type": "plain_text", "text": "Approve request"},
                "blocks": [],
            },
        },
    )
