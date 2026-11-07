"""Static-pool access for the sandbox substrate: leased long-lived harness shells.

``StaticPoolSandboxClient`` implements the ``SandboxClient`` seam over a fixed
inventory of shells the deployment starts (``CURIE_SANDBOX_SUBSTRATE=static``).
Each shell is one container: a trusted supervisor (``curie_shell_supervisor``)
and the unchanged runner as its child, one claim at a time. Nothing is
provisioned on demand; a claim leases a free shell or is refused for capacity.

Model mapping. A claim is a lease of one shell slot, so claim name ==
sandbox name, as on Docker. The lease lives in the substrate's own Valkey,
beside the affinity routes, so any worker can list, adopt or reap a claim
another worker created:

* ``<prefix>:static:slot:<slot>`` (hash): ``state`` free | leased |
  quarantined, the leasing ``claim`` and ``generation``, the shell
  ``incarnation`` that acknowledged it, and ``last_generation``, a per-slot
  counter that only grows, so every lease of a slot has a larger generation
  than the one before it.
* ``<prefix>:static:claim:<claim>`` (hash): slot, generation, pool, labels,
  creation instant, bundle ref, and ``state`` active | suspended. A suspended
  claim is a tombstone: the approval pause stopped and wiped its shell and gave
  the slot back, and the claim is kept only so resume can retire it.
* ``<prefix>:static:claims`` (set): every claim record, for listing.

A slot becomes free only when its supervisor acknowledges that the claim's
child is stopped and its files are gone, or reports a new incarnation (the
task was replaced, which wipes everything). An unreachable shell keeps its
slot leased; a shell that cannot prove itself clean is quarantined until its
task is replaced. Every Valkey transition checks the claim and generation it
expects, so an old claim's late cleanup never releases a later lease.

The worker stages each claim itself, exactly as on Docker (``staging``), and
uploads one tar to the supervisor, so a shell needs no object-store
credential. Turns then reach the runner over HTTPS through the supervisor at
``https://<shell>:<port>/c/<claim>/<generation>``, authenticated by the
claim's own runner bearer.

What this tier does not do, explicitly: Kubernetes quota admission
(``claim_resources``/``capacity_inventory``/``read_quota`` raise), scheduler
diagnostics (always ``None``), per-claim tool daemons (refused), and claim
templates (none). Repository runner pools and the publication lane are refused
at worker start (``run.py``).
"""

from __future__ import annotations

import http.client
import io
import json
import logging
import random
import re
import socket
import ssl
import tarfile
import tempfile
import time
import uuid
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import redis
from aci_protocol import BootEnv
from curie_shell_supervisor import protocol
from curie_shell_supervisor.protocol import (
    ARCHIVE_MEDIA_TYPE,
    ATTACHMENTS_DIR,
    BUNDLE_DIR,
    MANIFEST_NAME,
    MANIFEST_VERSION,
    OUTCOME_CLEANED,
    OUTCOME_QUARANTINED,
    STATE_IDLE,
    WORKSPACE_DIR,
    claim_path,
    proxy_base_path,
)
from plugin_format import (
    DEFAULT_MAX_COMPRESSION_RATIO,
    DEFAULT_MAX_MEMBERS,
    DEFAULT_MAX_UNCOMPRESSED_BYTES,
)

from ..attachments import ATTACHMENTS_REF_ENV, AttachmentLimits
from ..binding import BUNDLE_REF_ENV, RUNNER_TOKEN_ENV
from ..bundle_store import BundleReader
from ..workspace import WORKSPACE_REF_ENV, WORKSPACE_SHA256_ENV, WorkspaceLimits
from .claim_tokens import executor_claim_env, executor_withheld_names
from .staging import WORKER_OWNED_ENV, ClaimInputStaging, NoAttachments
from .types import (
    BUNDLE_REF_LABEL,
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    AdmissionRejection,
    AdmissionUnavailableError,
    CapacityExhaustedError,
    CapacityInventory,
    ClaimNameConflict,
    ClaimTimeoutError,
    ClaimView,
    DaemonObjectKind,
    DaemonObjectView,
    KubeTransientError,
    ListableDaemonKind,
    MissingAgentPoolError,
    OperatingMode,
    QuotaRejection,
    QuotaStatus,
    SandboxError,
    SandboxTermination,
    SandboxView,
    ToolDaemonMount,
    filter_agent_child_env,
)

logger = logging.getLogger(__name__)

_NO_TOOL_DAEMON = "the static-pool substrate runs no per-claim tool daemon (ADR 0217)"
_NO_QUOTA = "the static-pool substrate does not support Kubernetes quota admission"
_ATTACHMENT_ACCESS_ENV = BootEnv.env_key("attachment_access_enabled")
# The capacity dimension a shell lease spends: one runner process, the unit
# Kubernetes admission also counts (``pods``), so the kernel's existing
# capacity evidence checks apply unchanged.
_SLOT_DIMENSION = "pods"
_CONTROL_READ_TIMEOUT_S = 5.0
_RESPONSE_MAX_BYTES = 1024 * 1024
_DNS_SUBDOMAIN = re.compile(
    r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?)*"
)
_SLOT_ID = re.compile(r"[A-Za-z0-9][-A-Za-z0-9_.]{0,127}")

# -- Valkey transitions --------------------------------------------------------

# KEYS: claim record, claims set, candidate slot hashes...
# ARGV: claim, now, pool, labels json, bundle ref, candidate slot ids...
_RESERVE = """
if redis.call('EXISTS', KEYS[1]) == 1 then return {'conflict'} end
local used = 0
for i = 3, #KEYS do
  local state = redis.call('HGET', KEYS[i], 'state')
  if (not state) or state == 'free' then
    local slot = ARGV[i + 3]
    local gen = redis.call('HINCRBY', KEYS[i], 'last_generation', 1)
    redis.call('HSET', KEYS[i], 'state', 'leased', 'claim', ARGV[1],
      'generation', gen, 'updated_at', ARGV[2])
    redis.call('HDEL', KEYS[i], 'reason')
    redis.call('HSET', KEYS[1], 'slot', slot, 'generation', gen, 'pool', ARGV[3],
      'labels', ARGV[4], 'bundle_ref', ARGV[5], 'created_at', ARGV[2],
      'state', 'active', 'incarnation', '')
    redis.call('SADD', KEYS[2], ARGV[1])
    return {'leased', slot, tostring(gen)}
  end
  used = used + 1
end
return {'exhausted', tostring(used)}
"""

# KEYS: slot hash, claim record. ARGV: claim, generation, incarnation.
_ACKNOWLEDGE = """
if redis.call('HGET', KEYS[1], 'claim') ~= ARGV[1]
  or redis.call('HGET', KEYS[1], 'generation') ~= ARGV[2]
  or redis.call('HGET', KEYS[2], 'generation') ~= ARGV[2] then
  return 0
end
redis.call('HSET', KEYS[1], 'incarnation', ARGV[3])
redis.call('HSET', KEYS[2], 'incarnation', ARGV[3])
return 1
"""

# Release a lease after the shell answered for it. Only the exact lease is
# released; the claim record is deleted (retire) or kept as a tombstone
# (suspend) only while it still names the same generation.
# KEYS: slot hash, claim record, claims set.
# ARGV: claim, generation, slot state (free|quarantined), reason, now,
#       claim action (delete|suspend), shell max generation, incarnation.
_RELEASE = """
local owns = redis.call('HGET', KEYS[1], 'claim') == ARGV[1]
  and redis.call('HGET', KEYS[1], 'generation') == ARGV[2]
if owns then
  redis.call('HDEL', KEYS[1], 'claim', 'generation')
  redis.call('HSET', KEYS[1], 'state', ARGV[3], 'updated_at', ARGV[5])
  if ARGV[8] ~= '' then redis.call('HSET', KEYS[1], 'incarnation', ARGV[8]) end
  if ARGV[3] == 'quarantined' then
    redis.call('HSET', KEYS[1], 'reason', ARGV[4])
  else
    redis.call('HDEL', KEYS[1], 'reason')
  end
end
local last = tonumber(redis.call('HGET', KEYS[1], 'last_generation') or '0')
if tonumber(ARGV[7]) > last then
  redis.call('HSET', KEYS[1], 'last_generation', ARGV[7])
end
if redis.call('HGET', KEYS[2], 'generation') == ARGV[2] then
  if ARGV[6] == 'suspend' then
    redis.call('HSET', KEYS[2], 'state', 'suspended', 'incarnation', '')
  else
    redis.call('DEL', KEYS[2])
    redis.call('SREM', KEYS[3], ARGV[1])
  end
end
if owns then return 1 end
return 0
"""

# Forget a claim record that no longer holds its slot (a tombstone, or a lease
# its slot already moved past). Never touches a slot.
# KEYS: claim record, claims set. ARGV: claim, generation.
_FORGET = """
if redis.call('HGET', KEYS[1], 'generation') == ARGV[2] then
  redis.call('DEL', KEYS[1])
  redis.call('SREM', KEYS[2], ARGV[1])
  return 1
end
return 0
"""

# Re-admit a quarantined slot whose shell now answers idle and clean, unless
# the slot changed since it was read.
# KEYS: slot hash. ARGV: the incarnation read with the quarantine, now, the
# shell's max generation, the shell's incarnation.
_READMIT = """
if redis.call('HGET', KEYS[1], 'state') ~= 'quarantined'
  or (redis.call('HGET', KEYS[1], 'incarnation') or '') ~= ARGV[1] then
  return 0
end
redis.call('HSET', KEYS[1], 'state', 'free', 'updated_at', ARGV[2], 'incarnation', ARGV[4])
redis.call('HDEL', KEYS[1], 'reason', 'claim', 'generation')
local last = tonumber(redis.call('HGET', KEYS[1], 'last_generation') or '0')
if tonumber(ARGV[3]) > last then
  redis.call('HSET', KEYS[1], 'last_generation', ARGV[3])
end
return 1
"""


# -- configuration -------------------------------------------------------------


@dataclass(frozen=True)
class StaticShellSlot:
    """One shell the deployment runs: its stable slot id, address and pool.

    ``pool`` is the warm pool name a claim for it names: the generic
    ``CURIE_WARM_POOL`` or a per-agent pool (``agent_warm_pool_name``), so the
    existing ``CURIE_AGENT_SANDBOX_POOLS`` routing assigns agents to shells.
    """

    slot: str
    host: str
    port: int
    pool: str


def parse_static_shells(raw: str | None) -> tuple[StaticShellSlot, ...]:
    """Parse ``CURIE_STATIC_SHELLS``: a JSON list of ``{slot, host, port?, pool}``."""

    if not raw:
        raise ValueError("CURIE_STATIC_SHELLS names the static shell inventory")
    value = json.loads(raw)
    if not isinstance(value, list) or not value:
        raise ValueError("CURIE_STATIC_SHELLS must be a non-empty JSON list")
    slots: list[StaticShellSlot] = []
    seen: set[str] = set()
    endpoints: set[tuple[str, int]] = set()
    for entry in value:
        if not isinstance(entry, dict) or not set(entry) <= {"slot", "host", "port", "pool"}:
            raise ValueError("each static shell is {slot, host, port?, pool}")
        slot, host, pool = entry.get("slot"), entry.get("host"), entry.get("pool")
        port = entry.get("port", protocol.DEFAULT_PORT)
        if not isinstance(slot, str) or _SLOT_ID.fullmatch(slot) is None:
            raise ValueError("a static shell slot id is 1-128 of [A-Za-z0-9-_.]")
        if not isinstance(host, str) or _DNS_SUBDOMAIN.fullmatch(host) is None:
            raise ValueError(f"static shell {slot}: host must be a DNS name the TLS cert names")
        if type(port) is not int or not 0 < port < 65536:
            raise ValueError(f"static shell {slot}: port must be a TCP port")
        if not isinstance(pool, str) or _DNS_SUBDOMAIN.fullmatch(pool) is None:
            raise ValueError(f"static shell {slot}: pool must be a DNS-1123 name")
        if slot in seen or (host, port) in endpoints:
            raise ValueError(f"static shell {slot}: slot ids and endpoints are unique")
        seen.add(slot)
        endpoints.add((host, port))
        slots.append(StaticShellSlot(slot=slot, host=host, port=port, pool=pool))
    return tuple(slots)


def shell_ssl_context(ca_file: str) -> ssl.SSLContext:
    """Trust only the deployment's shell CA, with certificate and host name checks."""

    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_file)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    return context


class ShellUnreachable(KubeTransientError):
    """A shell supervisor did not answer; nothing about its state is proven."""


class ShellRefused(SandboxError):
    """A shell supervisor answered a control call with a refusal."""


@dataclass(frozen=True)
class _Lease:
    slot: StaticShellSlot
    generation: int


@dataclass
class _ClaimRecord:
    name: str
    slot: str
    generation: int
    pool: str
    labels: dict[str, str]
    created_at: datetime | None
    bundle_ref: str | None
    state: str
    incarnation: str

    @classmethod
    def from_hash(cls, name: str, raw: Mapping[bytes | str, bytes | str]) -> _ClaimRecord | None:
        data = {_text(k): _text(v) for k, v in raw.items()}
        if not data.get("slot") or not data.get("generation", "").isdigit():
            return None
        try:
            labels = json.loads(data.get("labels") or "{}")
            created = datetime.fromisoformat(data["created_at"]) if data.get("created_at") else None
        except ValueError:
            labels, created = {}, None
        return cls(
            name=name,
            slot=data["slot"],
            generation=int(data["generation"]),
            pool=data.get("pool", ""),
            labels={str(k): str(v) for k, v in labels.items()} if isinstance(labels, dict) else {},
            created_at=created,
            bundle_ref=data.get("bundle_ref") or None,
            state=data.get("state", "active"),
            incarnation=data.get("incarnation", ""),
        )


def _text(value: bytes | str) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _selector(label_selector: str) -> dict[str, str]:
    wanted: dict[str, str] = {}
    for part in label_selector.split(","):
        key, sep, value = part.partition("=")
        if not sep or not key.strip():
            raise ValueError(f"unsupported label selector {label_selector!r}")
        wanted[key.strip()] = value.strip()
    return wanted


# -- the client ----------------------------------------------------------------


class StaticPoolSandboxClient(ClaimInputStaging):
    """SandboxClient backed by a fixed pool of supervisor-managed shells."""

    def __init__(
        self,
        *,
        slots: Sequence[StaticShellSlot],
        redis_client: redis.Redis,
        control_token: str,
        ssl_context: ssl.SSLContext,
        bundle_store: BundleReader,
        key_prefix: str,
        bundle_max_uncompressed_bytes: int = DEFAULT_MAX_UNCOMPRESSED_BYTES,
        bundle_max_compression_ratio: float = DEFAULT_MAX_COMPRESSION_RATIO,
        bundle_max_members: int = DEFAULT_MAX_MEMBERS,
        workspace_limits: WorkspaceLimits | None = None,
        attachment_limits: AttachmentLimits | None = None,
        retire_timeout_seconds: float = 60.0,
    ) -> None:
        if not slots:
            raise ValueError("a static pool needs at least one shell")
        if len(control_token) < 32:
            raise ValueError("the shell control credential must be at least 32 characters")
        if not ssl_context.check_hostname or ssl_context.verify_mode != ssl.CERT_REQUIRED:
            raise ValueError("the shell TLS context must verify the certificate and host name")
        self._slots = {slot.slot: slot for slot in slots}
        self._redis = redis_client
        self._control = control_token
        self._ssl = ssl_context
        self._prefix = f"{key_prefix}:static"
        self._retire_timeout_s = retire_timeout_seconds
        self._reserve = redis_client.register_script(_RESERVE)
        self._acknowledge = redis_client.register_script(_ACKNOWLEDGE)
        self._release = redis_client.register_script(_RELEASE)
        self._forget = redis_client.register_script(_FORGET)
        self._readmit = redis_client.register_script(_READMIT)
        self._init_staging(
            bundle_store=bundle_store,
            bundle_max_uncompressed_bytes=bundle_max_uncompressed_bytes,
            bundle_max_compression_ratio=bundle_max_compression_ratio,
            bundle_max_members=bundle_max_members,
            workspace_limits=workspace_limits,
            attachment_limits=attachment_limits,
        )

    @property
    def pools(self) -> frozenset[str]:
        return frozenset(slot.pool for slot in self._slots.values())

    # -- keys ---------------------------------------------------------------

    def _slot_key(self, slot: str) -> str:
        return f"{self._prefix}:slot:{slot}"

    def _claim_key(self, name: str) -> str:
        return f"{self._prefix}:claim:{name}"

    @property
    def _claims_key(self) -> str:
        return f"{self._prefix}:claims"

    def _record(self, name: str) -> _ClaimRecord | None:
        raw = self._redis.hgetall(self._claim_key(name))
        return _ClaimRecord.from_hash(name, raw) if raw else None

    def _slot_state(self, slot: str) -> dict[str, str]:
        raw = self._redis.hgetall(self._slot_key(slot))
        return {_text(k): _text(v) for k, v in raw.items()}

    # -- supervisor control ----------------------------------------------------

    def _call(
        self,
        slot: StaticShellSlot,
        method: str,
        path: str,
        *,
        timeout: float,
        body: Any = None,
        length: int | None = None,
    ) -> tuple[int, dict[str, Any]]:
        if timeout <= 0:
            raise ClaimTimeoutError(f"no time left to reach shell {slot.slot}")
        connection = http.client.HTTPSConnection(
            slot.host, slot.port, timeout=timeout, context=self._ssl
        )
        headers = {"Authorization": f"Bearer {self._control}"}
        if body is not None:
            headers["Content-Type"] = ARCHIVE_MEDIA_TYPE
            headers["Content-Length"] = str(length)
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            data = response.read(_RESPONSE_MAX_BYTES)
            payload = json.loads(data) if data else {}
        except (OSError, http.client.HTTPException, ValueError) as exc:
            # Only the class: a TLS or socket message can carry addresses.
            raise ShellUnreachable(
                f"shell {slot.slot} did not answer {method} ({type(exc).__name__})"
            ) from exc
        finally:
            connection.close()
        if not isinstance(payload, dict):
            raise ShellUnreachable(f"shell {slot.slot} answered a non-object body")
        return response.status, payload

    def shell_status(self, slot: StaticShellSlot, *, timeout: float) -> dict[str, Any]:
        status, body = self._call(slot, "GET", protocol.SHELL_PATH, timeout=timeout)
        if status != 200:
            raise ShellUnreachable(f"shell {slot.slot} status answered {status}")
        return body

    def _retire_on_shell(
        self, slot: StaticShellSlot, claim: str, generation: int, *, mode: str, timeout: float
    ) -> tuple[str, dict[str, Any]]:
        """Ask the shell to end one lease; return (outcome, shell status)."""

        status, body = self._call(
            slot, "DELETE", f"{claim_path(claim, generation)}?mode={mode}", timeout=timeout
        )
        shell = body.get("shell")
        if status not in (200, 409) or not isinstance(shell, dict):
            raise ShellUnreachable(f"shell {slot.slot} retirement answered {status}")
        return str(body.get("outcome", "")), shell

    # -- capacity --------------------------------------------------------------

    def _pool_slots(self, pool: str) -> list[StaticShellSlot]:
        return [slot for slot in self._slots.values() if slot.pool == pool]

    def pool_has_headroom(self, pool: str) -> bool:
        """Whether ``pool`` has a free slot right now (idle-route reclamation)."""

        return any(
            self._slot_state(slot.slot).get("state", "free") == "free"
            for slot in self._pool_slots(pool)
        )

    def exhausted_pools(self) -> frozenset[str]:
        return frozenset(pool for pool in self.pools if not self.pool_has_headroom(pool))

    def claim_pool(self, name: str) -> str | None:
        record = self._record(name)
        return None if record is None else record.pool

    def _lease(
        self, name: str, *, pool: str, labels: Mapping[str, str], bundle_ref: str | None
    ) -> _Lease:
        candidates = self._pool_slots(pool)
        random.shuffle(candidates)
        try:
            result = self._reserve(
                keys=[
                    self._claim_key(name),
                    self._claims_key,
                    *(self._slot_key(slot.slot) for slot in candidates),
                ],
                args=[
                    name,
                    _now(),
                    pool,
                    json.dumps(dict(labels), sort_keys=True),
                    bundle_ref or "",
                    *(slot.slot for slot in candidates),
                ],
            )
        except redis.RedisError as exc:
            raise AdmissionUnavailableError("static shell inventory unavailable") from exc
        verdict = [_text(part) for part in result]
        if verdict[0] == "conflict":
            raise ClaimNameConflict(name)
        if verdict[0] == "exhausted":
            rejection = AdmissionRejection(
                quota_name=pool,
                requested={_SLOT_DIMENSION: "1"},
                used={_SLOT_DIMENSION: verdict[1]},
                hard={_SLOT_DIMENSION: str(len(candidates))},
            )
            logger.warning(
                "static pool %s exhausted: %s of %s shells leased or quarantined",
                pool,
                verdict[1],
                len(candidates),
                extra={"evidence_type": "admission"},
            )
            raise CapacityExhaustedError(rejection)
        return _Lease(self._slots[verdict[1]], int(verdict[2]))

    # -- claim lifecycle -------------------------------------------------------

    def create_claim(
        self,
        name: str,
        *,
        pool: str,
        env: dict[str, str] | None = None,
        labels: dict[str, str] | None = None,
        runner_resources: dict[str, object] | None = None,  # noqa: ARG002 - the task size
        deadline: float,
        agent_name: str | None = None,
        executor_secret_names: frozenset[str] | None = None,
        tool_daemon: ToolDaemonMount | None = None,
    ) -> None:
        """Lease a free shell in ``pool``, stage the claim, start its runner.

        A shell's resources are its task definition's; ``runner_resources``
        cannot resize a running task and is not applied. A failure after the
        lease leaves the record for ``delete_claim`` (the substrate's retire),
        which frees the slot only once the shell proves it clean.
        """

        if tool_daemon is not None:
            raise SandboxError(_NO_TOOL_DAEMON)
        if not protocol.valid_claim_name(name):
            raise SandboxError("a static claim name is a DNS-1123 label")
        env = filter_agent_child_env(env)
        withheld: frozenset[str] = frozenset()
        if executor_secret_names is not None:
            withheld = executor_withheld_names(env)
            env = executor_claim_env(env, executor_secret_names)
        if not env.get(RUNNER_TOKEN_ENV):
            # The bearer is what fences one claim's requests from the next
            # claim's child on the same shell; a claim without one is refused.
            raise SandboxError("a static-pool claim requires a per-claim runner bearer")
        if not self._pool_slots(pool):
            raise MissingAgentPoolError(agent_name or "(none)", pool)
        claim_labels = {
            **(labels or {}),
            MANAGED_BY_LABEL: MANAGED_BY_VALUE,
        }
        if env.get(BUNDLE_REF_ENV):
            claim_labels[BUNDLE_REF_LABEL] = env[BUNDLE_REF_ENV]
        lease = self._lease(
            name, pool=pool, labels=claim_labels, bundle_ref=env.get(BUNDLE_REF_ENV)
        )
        attempt = uuid.uuid4().hex
        archive = self._build_archive(
            attempt, name, lease.generation, env, executor_secret_names, withheld
        )
        try:
            self._start_on_shell(name, lease, archive, deadline)
        finally:
            archive.unlink(missing_ok=True)

    def _start_on_shell(self, name: str, lease: _Lease, archive: Path, deadline: float) -> None:
        path = claim_path(name, lease.generation)
        size = archive.stat().st_size
        for attempt in range(2):
            with archive.open("rb") as body:
                status, answer = self._call(
                    lease.slot,
                    "PUT",
                    path,
                    timeout=deadline - time.monotonic(),
                    body=body,
                    length=size,
                )
            shell = answer.get("shell") if status not in (200, 201) else answer
            if status in (200, 201) and isinstance(shell, dict):
                acknowledged = self._acknowledge(
                    keys=[self._slot_key(lease.slot.slot), self._claim_key(name)],
                    args=[name, lease.generation, str(shell.get("incarnation", ""))],
                )
                if not acknowledged:
                    # Our lease ended while the upload ran (a concurrent retire
                    # freed it). The child must not outlive the record.
                    self._retire_on_shell(
                        lease.slot,
                        name,
                        lease.generation,
                        mode="retire",
                        timeout=max(_CONTROL_READ_TIMEOUT_S, deadline - time.monotonic()),
                    )
                    raise ShellRefused(f"claim {name} lost its lease while starting")
                return
            if status == 409 and attempt == 0 and isinstance(shell, dict):
                if self._clear_foreign_claim(lease.slot, shell, deadline):
                    continue
            detail = answer.get("error", "refused")
            raise ShellRefused(f"shell {lease.slot.slot} refused claim {name}: {status} {detail}")

    def _clear_foreign_claim(
        self, slot: StaticShellSlot, shell: Mapping[str, Any], deadline: float
    ) -> bool:
        """Retire a claim a shell runs that no Valkey record owns.

        Such a child is unreachable (no route can name a claim without a
        record), so ending it steals nothing. Returns whether it was ended.
        """

        claim = shell.get("claim")
        if not isinstance(claim, dict):
            return False
        name, generation = claim.get("name"), claim.get("generation")
        if not isinstance(name, str) or not isinstance(generation, int):
            return False
        if self._redis.exists(self._claim_key(name)):
            return False
        logger.warning("shell %s ran unrecorded claim %s; retiring it", slot.slot, name)
        outcome, _ = self._retire_on_shell(
            slot, name, generation, mode="retire", timeout=deadline - time.monotonic()
        )
        return outcome == OUTCOME_CLEANED

    def _build_archive(
        self,
        attempt: str,
        name: str,
        generation: int,
        env: Mapping[str, str],
        executor_secret_names: frozenset[str] | None,
        withheld: frozenset[str],
    ) -> Path:
        """Stage the claim's inputs on the worker and pack them for the shell."""

        try:
            bundle = workspace = attachments = None
            attachment_mode = "none"
            if env.get(BUNDLE_REF_ENV):
                bundle = self._prepare_bundle(attempt, env[BUNDLE_REF_ENV])
            if env.get(WORKSPACE_REF_ENV):
                workspace = self._prepare_workspace(
                    attempt, env[WORKSPACE_REF_ENV], env.get(WORKSPACE_SHA256_ENV, "")
                )
            if env.get(_ATTACHMENT_ACCESS_ENV) == "1":
                attachment_mode = "lazy"
            elif env.get(ATTACHMENTS_REF_ENV):
                try:
                    attachments = self._prepare_attachments(attempt, env[ATTACHMENTS_REF_ENV])
                    attachment_mode = "eager"
                except NoAttachments:
                    pass
            manifest = {
                "v": MANIFEST_VERSION,
                "claim": name,
                "generation": generation,
                # Everything the worker did not redeem itself reaches the
                # child unchanged; the shell's own env supplies the model
                # credential and connector secrets, as a pool template does.
                "env": {k: v for k, v in env.items() if k not in WORKER_OWNED_ENV},
                "executor_secret_names": (
                    None if executor_secret_names is None else sorted(executor_secret_names)
                ),
                "withheld_env": sorted(withheld),
                "attachments": attachment_mode,
                "workspace": workspace is not None,
            }
            handle, path = tempfile.mkstemp(prefix="curie-shell-stage-", suffix=".tar")
            archive = Path(path)
            try:
                with open(handle, "wb") as raw, tarfile.open(fileobj=raw, mode="w") as tar:
                    data = json.dumps(manifest, sort_keys=True).encode("utf-8")
                    info = tarfile.TarInfo(MANIFEST_NAME)
                    info.size = len(data)
                    info.mode = 0o600
                    tar.addfile(info, io.BytesIO(data))
                    for arcname, root in (
                        (BUNDLE_DIR, bundle),
                        (WORKSPACE_DIR, workspace),
                        (ATTACHMENTS_DIR, attachments),
                    ):
                        if root is not None:
                            tar.add(root, arcname=arcname)
            except BaseException:
                archive.unlink(missing_ok=True)
                raise
            return archive
        finally:
            self._cleanup_staged_inputs(attempt)

    def _end_lease(self, record: _ClaimRecord, *, mode: str, timeout: float) -> None:
        """End ``record``'s lease on its shell, then release or quarantine its slot."""

        slot = self._slots.get(record.slot)
        state = self._slot_state(record.slot)
        if (
            slot is None
            or state.get("claim") != record.name
            or state.get("generation") != str(record.generation)
        ):
            # The slot already moved on (or left the inventory): only the
            # record is ours to remove, never whatever holds the slot now.
            if mode == "suspend":
                self._mark_tombstone(record)
            else:
                self._forget(
                    keys=[self._claim_key(record.name), self._claims_key],
                    args=[record.name, record.generation],
                )
            return
        outcome, shell = self._retire_on_shell(
            slot, record.name, record.generation, mode=mode, timeout=timeout
        )
        shell_state = shell.get("state")
        claim = shell.get("claim")
        if outcome == OUTCOME_QUARANTINED or shell_state == "quarantined":
            next_state, reason = "quarantined", str(shell.get("quarantine_reason") or "quarantined")
        elif shell_state == STATE_IDLE and claim is None:
            next_state, reason = "free", ""
        else:
            # The shell is busy with a claim this slot's lease does not name.
            next_state, reason = "quarantined", "foreign-claim"
        if next_state == "quarantined":
            logger.warning(
                "static shell %s quarantined after claim %s: %s",
                record.slot,
                record.name,
                reason,
            )
        self._release(
            keys=[self._slot_key(record.slot), self._claim_key(record.name), self._claims_key],
            args=[
                record.name,
                record.generation,
                next_state,
                reason,
                _now(),
                "suspend" if mode == "suspend" else "delete",
                int(shell.get("max_generation") or 0),
                str(shell.get("incarnation") or ""),
            ],
        )

    def _mark_tombstone(self, record: _ClaimRecord) -> None:
        key = self._claim_key(record.name)
        if self._redis.hget(key, "generation") == str(record.generation).encode():
            self._redis.hset(key, mapping={"state": "suspended", "incarnation": ""})

    def get_claim(self, name: str, *, request_timeout_seconds: float) -> ClaimView | None:
        record = self._record(name)
        if record is None:
            return None
        view = ClaimView(
            name=name,
            ready=False,
            sandbox_name=name,
            created_at=record.created_at,
            quota_rejection=None,
            ready_reason=None,
            ready_message=None,
            bundle_ref=record.bundle_ref,
        )
        if record.state != "active":
            return view
        phase, lease = self._serving(record, request_timeout_seconds)
        if phase == "gone":
            return None
        if phase == "starting":
            return view
        ready = bool(lease.get("ready"))
        return ClaimView(
            name=name,
            ready=ready,
            sandbox_name=name,
            created_at=record.created_at,
            quota_rejection=None,
            ready_reason=None if ready else "RunnerStarting",
            ready_message=None,
            bundle_ref=record.bundle_ref,
        )

    def _serving(
        self, record: _ClaimRecord, timeout: float
    ) -> tuple[Literal["gone", "starting", "serving"], dict[str, Any]]:
        """The shell's view of ``record``'s lease.

        ``gone``: the claim's runner provably no longer runs (the task was
        replaced, or the child exited). ``starting``: the creator has not had
        the shell acknowledge it yet. ``serving``: the shell's lease entry.
        """

        slot = self._slots.get(record.slot)
        if slot is None:
            return "gone", {}
        shell = self.shell_status(slot, timeout=min(timeout, _CONTROL_READ_TIMEOUT_S))
        if record.incarnation and shell.get("incarnation") != record.incarnation:
            return "gone", {}
        claim = shell.get("claim")
        if (
            isinstance(claim, dict)
            and claim.get("name") == record.name
            and claim.get("generation") == record.generation
        ):
            if claim.get("child_exit") is not None:
                return "gone", {}
            return "serving", claim
        if record.incarnation:
            # Acknowledged once, absent now: retired or lost.
            return "gone", {}
        return "starting", {}

    def delete_claim(self, name: str, *, request_timeout_seconds: float) -> None:
        record = self._record(name)
        if record is None:
            return
        if record.state != "active":
            # A tombstone holds no slot: the pause already returned it.
            self._forget(
                keys=[self._claim_key(name), self._claims_key], args=[name, record.generation]
            )
            return
        self._end_lease(
            record,
            mode="retire",
            timeout=max(request_timeout_seconds, self._retire_timeout_s),
        )

    def list_claims(self, *, label_selector: str) -> list[ClaimView]:
        """Every recorded claim matching ``label_selector``, from Valkey alone.

        No shell is contacted, so one unreachable shell cannot hide the rest
        of the inventory from the orphan reaper.
        """

        wanted = _selector(label_selector)
        views: list[ClaimView] = []
        for member in sorted(_text(name) for name in self._redis.smembers(self._claims_key)):
            record = self._record(member)
            if record is None or any(record.labels.get(k) != v for k, v in wanted.items()):
                continue
            views.append(
                ClaimView(
                    name=member,
                    ready=record.state == "active" and bool(record.incarnation),
                    sandbox_name=member,
                    created_at=record.created_at,
                    quota_rejection=None,
                    ready_reason=None,
                    ready_message=None,
                    bundle_ref=record.bundle_ref,
                )
            )
        return views

    def reap_claim_templates(self, *, keep: set[str], created_before: datetime) -> list[str]:
        """No claim templates exist here; the reap tick re-admits clean shells.

        A quarantined slot returns to the pool once its shell answers idle with
        nothing running: a quarantined supervisor never does, so in practice
        that is a replaced task (a new incarnation), or a claim the shell ran
        with no record (its creator lost the lease mid-start) that was retired
        here first. Best effort per slot; an unreachable shell is retried next
        tick.
        """

        del keep, created_before
        for slot in self._slots.values():
            try:
                self._reconcile_slot(slot)
            except Exception as exc:  # noqa: BLE001 - one shell never blocks the sweep
                logger.warning(
                    "static shell %s reconcile failed (%s)", slot.slot, type(exc).__name__
                )
        return []

    def _reconcile_slot(self, slot: StaticShellSlot) -> None:
        state = self._slot_state(slot.slot)
        if state.get("state") != "quarantined":
            return
        shell = self.shell_status(slot, timeout=_CONTROL_READ_TIMEOUT_S)
        if isinstance(shell.get("claim"), dict):
            if not self._clear_foreign_claim(
                slot, shell, time.monotonic() + self._retire_timeout_s
            ):
                return
            shell = self.shell_status(slot, timeout=_CONTROL_READ_TIMEOUT_S)
        # A shell that quarantined itself stays quarantined until its task is
        # replaced; only an idle shell with nothing running is clean.
        if shell.get("state") != STATE_IDLE or shell.get("claim") is not None:
            return
        if self._readmit(
            keys=[self._slot_key(slot.slot)],
            args=[
                state.get("incarnation", ""),
                _now(),
                int(shell.get("max_generation") or 0),
                str(shell.get("incarnation") or ""),
            ],
        ):
            logger.info("static shell %s re-admitted", slot.slot)

    # -- sandbox lifecycle -----------------------------------------------------

    def get_sandbox(self, name: str, *, request_timeout_seconds: float) -> SandboxView | None:
        record = self._record(name)
        if record is None:
            return None
        if record.state != "active":
            return SandboxView(
                name=name, ready=False, service_fqdn=None, operating_mode="Suspended"
            )
        phase, lease = self._serving(record, request_timeout_seconds)
        if phase == "gone":
            return None
        slot = self._slots[record.slot]
        ready = phase == "serving" and bool(lease.get("ready"))
        return SandboxView(
            name=name,
            ready=ready,
            service_fqdn=slot.host if ready else None,
            operating_mode="Running",
            port=slot.port,
            transport="https",
            base_path=proxy_base_path(name, record.generation),
        )

    def set_sandbox_mode(self, name: str, mode: OperatingMode) -> None:
        """Suspend stops and wipes the shell and keeps a tombstone (ADR-0003).

        The substrate's resume always retires the suspended claim and claims a
        fresh shell with the same session and history ref, so ``Running`` has
        nothing to do here.
        """

        if mode != "Suspended":
            return
        record = self._record(name)
        if record is None or record.state != "active":
            return
        self._end_lease(record, mode="suspend", timeout=self._retire_timeout_s)

    # -- what this tier does not have ------------------------------------------

    def claim_resources(
        self,
        *,
        pool: str,
        runner_resources: dict[str, object] | None,
        agent_name: str | None,
        executor_secret_names: frozenset[str] | None,
    ) -> dict[str, str]:
        raise ValueError(_NO_QUOTA)

    def capacity_inventory(
        self,
        *,
        priority_class: str,
        known_claim_resources: Mapping[str, Mapping[str, str]],
        request_timeout_seconds: float,
    ) -> CapacityInventory:
        raise ValueError(_NO_QUOTA)

    def read_quota(self, quota_name: str, *, request_timeout_seconds: float) -> QuotaStatus:
        raise ValueError(_NO_QUOTA)

    def quota_has_headroom(
        self, rejection: QuotaRejection, *, request_timeout_seconds: float
    ) -> bool:
        del rejection, request_timeout_seconds
        return False

    def pod_unschedulable(self, name: str, *, request_timeout_seconds: float) -> str | None:
        # A shell is already running; there is no scheduler to wait on.
        del name, request_timeout_seconds
        return None

    def pod_termination(
        self, name: str, *, since: datetime, request_timeout_seconds: float
    ) -> SandboxTermination | None:
        del name, since, request_timeout_seconds
        return None

    def create_daemon_object(
        self, namespace: str, manifest: Mapping[str, Any], *, request_timeout_seconds: float
    ) -> str:
        raise SandboxError(_NO_TOOL_DAEMON)

    def list_daemon_objects(
        self,
        namespace: str,
        kind: ListableDaemonKind,
        *,
        label_selector: str,
        request_timeout_seconds: float,
    ) -> list[DaemonObjectView]:
        return []

    def delete_daemon_object(
        self,
        namespace: str,
        kind: DaemonObjectKind,
        name: str,
        *,
        uid: str,
        request_timeout_seconds: float,
    ) -> None:
        raise SandboxError(_NO_TOOL_DAEMON)

    def get_daemon_pod(
        self, namespace: str, name: str, *, request_timeout_seconds: float
    ) -> DaemonObjectView | None:
        return None

    def open_daemon_stream(
        self, namespace: str, pod: str, port: int, *, timeout_seconds: float
    ) -> AbstractContextManager[socket.socket]:
        raise SandboxError(_NO_TOOL_DAEMON)


__all__ = [
    "ShellRefused",
    "ShellUnreachable",
    "StaticPoolSandboxClient",
    "StaticShellSlot",
    "parse_static_shells",
    "shell_ssl_context",
]
