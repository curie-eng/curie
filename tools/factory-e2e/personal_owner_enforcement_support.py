"""Pure parts of the owner-binding enforcement proof driver (#5378).

Everything here is deterministic and touches no cluster, Docker engine,
registry or network: the approved live ceiling, flag and input validation, the
stat-only grant check, the private SQLite ledger and its reservation math,
native transcript usage counting, the stub code host and workspace template
transforms, the judge, argv and environment builders, and the LIFO finalizer.
``personal_owner_enforcement.py`` owns every side effect.

No function here returns, logs or raises a held value. Refusals carry fixed
codes and flag names only.
"""

from __future__ import annotations

import copy
import dataclasses
import hashlib
import ipaddress
import json
import os
import re
import sqlite3
import stat
import sys
import textwrap
import traceback
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import personal_profile_support as pps  # noqa: E402

# --------------------------------------------------------------------------
# The approved live ceiling (hard; enforced before every start)
# --------------------------------------------------------------------------

MODEL = "claude-sonnet-5-5"
MAX_SESSIONS = 8
# CURIE_WORK_ITEM_MAX_TURNS: the SDK turn budget of each query.
SESSION_TURNS = 2
# Queries one native session can carry: the execute turn, then at most one
# unpublished continuation the kernel opens on the same runner
# (curie_worker.kernel.publication._continue_unpublished).
SESSION_QUERIES = 2
# The per-session assistant-turn ceiling, across every query of the session:
# SESSION_QUERIES x SESSION_TURNS, the continuation included. The budget judge,
# the reservation and the spend guard compare against this one number.
SESSION_ASSISTANT_TURNS_CEILING = 4
# Every session's assistant turns together, continuation turns included.
TOTAL_ASSISTANT_TURNS_CEILING = 32
MAX_OUTPUT_TOKENS = 8192
SESSION_SECONDS = 180
LIVE_SECONDS = 1440
CLEANUP_SECONDS = 180
PAID_RETRIES = 0
# The ceiling split evenly across the sessions.
SESSION_OUTPUT_TOKENS = MAX_OUTPUT_TOKENS // MAX_SESSIONS
# Per model response, inside the session share (the template's
# CLAUDE_CODE_MAX_OUTPUT_TOKENS).
RESPONSE_OUTPUT_TOKENS = 256
# The runner's per-query cap (DEFAULT_MAX_OUTPUT_TOKENS_PER_RUN): a fresh
# BudgetTracker per runner turn, so two queries must fit one session share.
QUERY_OUTPUT_TOKENS = SESSION_OUTPUT_TOKENS // SESSION_QUERIES
# The runner checks its cap after each response, never mid-response
# (curie_runner/budget.py), so one query can pass it by at most one response;
# its SDK turn budget bounds it independently.
QUERY_WORST_OUTPUT_TOKENS = min(
    SESSION_TURNS * RESPONSE_OUTPUT_TOKENS, QUERY_OUTPUT_TOKENS + RESPONSE_OUTPUT_TOKENS
)
# Recorded in the ledger when the live window opens; the budget judge reads them back.
SPEND_CEILINGS = {
    "ceiling.session_assistant_turns": SESSION_ASSISTANT_TURNS_CEILING,
    "ceiling.total_assistant_turns": TOTAL_ASSISTANT_TURNS_CEILING,
    "ceiling.output_tokens": MAX_OUTPUT_TOKENS,
}
# A session needs at least this much of the live window left to be reserved.
MIN_SESSION_WINDOW = 90
# A reservation whose native process has not started within this window is
# closed unstarted; a later start is unreserved and fences every start.
START_WINDOW = 150
GRANT_MIN_LIFETIME = LIVE_SECONDS + CLEANUP_SECONDS
# Restoring uncommitted workspace files into a fresh process after Pod
# recreation is the epic follow-up #5541, itself blocked by #5092.
BLOCKING_ISSUE = "#5541"
BLOCKING_UPSTREAM = {"#5541": "#5092"}


@dataclasses.dataclass(frozen=True)
class Slot:
    """One planned native session: what it proves and the generation it runs under."""

    name: str
    generation: int
    grant: str  # "valid" or "invalid"
    purpose: str


# Decision 12: six attempts for the three interruption/recovery pairs, one
# rejected recovery under a deliberately invalid grant, one revalidated
# recovery under the valid grant. The connector and Bash proofs share them.
SESSION_PLAN: tuple[Slot, ...] = (
    Slot("process-interrupted", 1, "valid", "restart"),
    Slot("process-recovered", 1, "valid", "restart"),
    Slot("pod-interrupted", 1, "valid", "restart"),
    Slot("pod-recovered", 1, "valid", "restart"),
    Slot("worker-interrupted", 1, "valid", "restart"),
    Slot("worker-recovered", 1, "valid", "restart+fencing"),
    Slot("rejected", 2, "invalid", "authentication-required"),
    Slot("revalidated", 3, "valid", "authentication-required+hosted-header"),
)
SLOT_NAMES = tuple(slot.name for slot in SESSION_PLAN)


def ceiling() -> dict[str, Any]:
    return {
        "model": MODEL,
        "max_sessions": MAX_SESSIONS,
        "session_turns": SESSION_TURNS,
        "session_assistant_turns_ceiling": SESSION_ASSISTANT_TURNS_CEILING,
        "total_assistant_turns_ceiling": TOTAL_ASSISTANT_TURNS_CEILING,
        "max_output_tokens": MAX_OUTPUT_TOKENS,
        "session_output_tokens": SESSION_OUTPUT_TOKENS,
        "response_output_tokens": RESPONSE_OUTPUT_TOKENS,
        "session_seconds": SESSION_SECONDS,
        "live_seconds": LIVE_SECONDS,
        "cleanup_seconds": CLEANUP_SECONDS,
        "paid_retries": PAID_RETRIES,
    }


def shares_fit(
    *,
    sessions: int = MAX_SESSIONS,
    session_tokens: int = SESSION_OUTPUT_TOKENS,
    session_turns: int = SESSION_ASSISTANT_TURNS_CEILING,
    session_seconds: int = SESSION_SECONDS,
    plan: Sequence[Slot] = SESSION_PLAN,
) -> bool:
    """Every planned session's share fits the ceiling before anything is spent."""

    return (
        len(plan) <= sessions <= MAX_SESSIONS
        and session_tokens * sessions <= MAX_OUTPUT_TOKENS
        and session_turns <= SESSION_ASSISTANT_TURNS_CEILING
        and session_turns * sessions <= TOTAL_ASSISTANT_TURNS_CEILING
        and SESSION_QUERIES * SESSION_TURNS <= SESSION_ASSISTANT_TURNS_CEILING
        and SESSION_QUERIES * QUERY_WORST_OUTPUT_TOKENS <= session_tokens
        and session_seconds * sessions <= LIVE_SECONDS
        and session_seconds <= SESSION_SECONDS
        and RESPONSE_OUTPUT_TOKENS * session_turns <= session_tokens
    )


# --------------------------------------------------------------------------
# Refusals
# --------------------------------------------------------------------------

EXIT_PASS = pps.EXIT_PASS
EXIT_FAIL = pps.EXIT_FAIL
EXIT_REFUSED = pps.EXIT_REFUSED
EXIT_NOT_MET = 3

FIXES: dict[str, str] = {
    "input_file_invalid": "--input-file must be a private (0600, yours) JSON object with "
    "kubeconfig, curie_bin and runner_api_host, and optionally grant_probe_cli",
    "context_invalid": "pass --context <context> naming the throwaway kube context",
    "docker_context_invalid": "pass --docker-context naming the scratch Docker context",
    "namespace_invalid": "pass --namespace as a DNS label of at most 42 characters",
    "namespace_exists": "choose a namespace that does not exist; the driver creates and deletes it",
    "cluster_unready": "the cluster's /readyz did not answer ok through --context <context>",
    "rbac_denied": "the caller cannot create, read or delete a required resource in a "
    "fresh namespace",
    "docker_unavailable": "docker info failed through --docker-context",
    "registry_unreachable": "the loopback registry /v2/ endpoint did not answer 200",
    "candidate_dirty": "the checkout must be clean and at --candidate",
    "grant_probe_failed": "the one-turn grant probe failed; nothing was reserved",
    "live_window_exhausted": "the live window cannot hold another session",
    "unreserved_start": "a native session started without a reservation; starts are fenced",
    "criterion_invalid": "pass --criterion as one of the listed criteria",
    "ceiling_invalid": "the session shares do not fit the approved ceiling",
    "python_env_incomplete": "run the driver under the repository environment (uv run) so "
    "the candidate's packages import",
    "candidate_mismatch": "judge the run with the candidate its ledger recorded",
}


class Refusal(pps.Refusal):
    """A refusal before any paid start. Codes and flag names only."""

    def payload(self) -> dict[str, str]:
        fix = FIXES.get(self.code) or pps.FIXES.get(self.code, "see the README")
        if self.detail:
            fix = f"{fix} ({self.detail})"
        return {"error": self.code, "fix": fix}


# --------------------------------------------------------------------------
# Flags, input file and grant file
# --------------------------------------------------------------------------

_CONTEXT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,252}")
INPUT_KEYS_REQUIRED = ("kubeconfig", "curie_bin", "runner_api_host")
INPUT_KEYS_OPTIONAL = ("grant_probe_cli",)
NAMESPACE_LIMIT = 42


@dataclasses.dataclass(frozen=True)
class InputFile:
    """The caller's private input file. Paths and an address; never a credential."""

    kubeconfig: str
    curie_bin: str
    runner_api_host: str
    grant_probe_cli: str = "claude"

    def __repr__(self) -> str:
        return "<input file>"


def require_private_file(path: Path, code: str, flag: str) -> os.stat_result:
    """A regular file (not a symlink) owned by the caller, mode 0600. Stat only."""

    try:
        info = os.lstat(path)
    except OSError:
        raise Refusal(code, flag) from None
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o600
    ):
        raise Refusal(code, flag)
    return info


def load_input_file(path: Path) -> InputFile:
    if not path.is_absolute():
        raise Refusal("input_file_invalid", "--input-file")
    require_private_file(path, "input_file_invalid", "--input-file")
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError):
        raise Refusal("input_file_invalid", "--input-file") from None
    if not isinstance(body, dict):
        raise Refusal("input_file_invalid", "--input-file")
    unknown = sorted(set(body) - set(INPUT_KEYS_REQUIRED) - set(INPUT_KEYS_OPTIONAL))
    missing = [key for key in INPUT_KEYS_REQUIRED if not isinstance(body.get(key), str)]
    if unknown or missing:
        raise Refusal("input_file_invalid", "keys: " + ", ".join(unknown + missing))
    kubeconfig = Path(body["kubeconfig"])
    curie_bin = Path(body["curie_bin"])
    if not kubeconfig.is_absolute() or not kubeconfig.is_file():
        raise Refusal("input_file_invalid", "kubeconfig")
    if not curie_bin.is_absolute() or not os.access(curie_bin, os.X_OK):
        raise Refusal("input_file_invalid", "curie_bin")
    host = body["runner_api_host"]
    try:
        address = ipaddress.IPv4Address(host)
    except ValueError:
        raise Refusal("input_file_invalid", "runner_api_host") from None
    if address.is_loopback or address.is_unspecified or address.is_multicast:
        raise Refusal("input_file_invalid", "runner_api_host")
    probe = body.get("grant_probe_cli", "claude")
    if not isinstance(probe, str) or not re.fullmatch(r"[A-Za-z0-9_./-]{1,255}", probe):
        raise Refusal("input_file_invalid", "grant_probe_cli")
    return InputFile(str(kubeconfig), str(curie_bin), host, probe)


@dataclasses.dataclass(frozen=True)
class Flags:
    candidate: str
    context: str
    namespace: str
    docker_context: str
    registry: str
    registry_port: int
    state_dir: Path
    run_id: str

    @property
    def grant_namespace(self) -> str:
        return f"{self.namespace}-grants"

    @property
    def release(self) -> str:
        return release_name(self.run_id)


def require_flags(
    *,
    candidate: str,
    context: str,
    namespace: str,
    docker_context: str,
    registry: str,
    state_dir: str,
    run_id: str,
) -> Flags:
    pps.require_candidate(candidate)
    if not _CONTEXT.fullmatch(context or ""):
        raise Refusal("context_invalid", "--context")
    if not _CONTEXT.fullmatch(docker_context or ""):
        raise Refusal("docker_context_invalid", "--docker-context")
    try:
        pps.require_dns_name(namespace, "--namespace", limit=NAMESPACE_LIMIT)
    except pps.Refusal:
        raise Refusal("namespace_invalid", "--namespace") from None
    _, port = pps.require_registry(registry)
    directory = Path(state_dir or "")
    if not directory.is_absolute():
        raise Refusal("state_dir_unsafe", "--state-dir")
    pps.require_private_dir(directory)
    pps.require_run_id(run_id)
    return Flags(candidate, context, namespace, docker_context, registry, port, directory, run_id)


def stat_grant_file(path: Path | None) -> None:
    """Preflight: the grant file exists, is private and plausibly sized. Never opened."""

    if path is None or not str(path):
        raise Refusal("grant_missing", "--grant-file")
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        raise Refusal("grant_missing", "--grant-file") from None
    except OSError:
        raise Refusal("grant_unsafe", "--grant-file") from None
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o600
    ):
        raise Refusal("grant_unsafe", "--grant-file")
    if info.st_size == 0:
        raise Refusal("grant_missing", "--grant-file")
    if info.st_size > 4097:
        raise Refusal("grant_unsafe", "--grant-file")


def require_expiry(text: str | None, now: float) -> float:
    """``--grant-expires-at`` leaves the whole live window plus cleanup at first launch."""

    expires_at = pps.parse_expiry(text)
    pps.require_lifetime(expires_at, now, GRANT_MIN_LIFETIME)
    return expires_at


def release_name(run_id: str) -> str:
    """The connector release this run names. Derived, never ``curie``."""

    name = "poe-" + hashlib.sha256(run_id.encode()).hexdigest()[:10]
    assert name != "curie"
    return name


# --------------------------------------------------------------------------
# Argv and environment builders
# --------------------------------------------------------------------------

CHILD_ENV_NAMES = pps.CHILD_ENV_NAMES


def kubectl_argv(context: str, namespace: str | None, *args: str) -> list[str]:
    """Every kubectl call names the caller's context explicitly."""

    argv = ["kubectl", "--context", context]
    if namespace:
        argv += ["--namespace", namespace]
    return [*argv, *args]


def docker_argv(docker_context: str, *args: str) -> list[str]:
    return ["docker", "--context", docker_context, *args]


def child_env(
    environ: Mapping[str, str], *, flags: Flags, inputs: InputFile, evidence_dir: Path
) -> dict[str, str]:
    """The whole environment of the ``env -i`` proof child. Paths and names only."""

    env = {name: environ[name] for name in CHILD_ENV_NAMES if environ.get(name)}
    env.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "DOCKER_CONTEXT": flags.docker_context,
            "KUBECONFIG": inputs.kubeconfig,
            "THROWAWAY_CONTEXT": flags.context,
            "CURIE_PERSONAL_TEST_NAMESPACE": flags.namespace,
            "CURIE_BIN": inputs.curie_bin,
            "CURIE_PERSONAL_RUNNER_API_HOST": inputs.runner_api_host,
            "CURIE_PERSONAL_EVIDENCE_DIR": str(evidence_dir),
            "CURIE_PERSONAL_PROOF_STATE_DIR": str(flags.state_dir / "proof"),
        }
    )
    return env


# --------------------------------------------------------------------------
# Native transcript usage
# --------------------------------------------------------------------------

SYNTHETIC_MODEL = "<synthetic>"
_USAGE_COUNTS = (
    "input_tokens",
    "output_tokens",
    "cache_creation_input_tokens",
    "cache_read_input_tokens",
)


@dataclasses.dataclass(frozen=True)
class SessionUsage:
    """One native session's usage. ``known`` is False when anything is unreadable."""

    output_tokens: int | None
    assistant_turns: int | None
    known: bool
    models: tuple[str, ...] = ()
    error_classes: tuple[dict[str, Any], ...] = ()
    texts: tuple[str, ...] = ()

    @classmethod
    def unknown(cls) -> SessionUsage:
        return cls(None, None, False)


def _seeded(record: Mapping[str, Any], seeds: Sequence[Mapping[str, Any]]) -> bool | None:
    """True for an exactly matched seed, None for a changed one, False otherwise."""

    uuid = record.get("uuid")
    if uuid is None:
        return False
    matches = [seed for seed in seeds if seed.get("uuid") == uuid]
    if not matches:
        return False
    return True if any(dict(record) == dict(seed) for seed in matches) else None


def _zero_usage(usage: Any) -> bool:
    return (
        isinstance(usage, dict)
        and any(key in usage for key in _USAGE_COUNTS)
        and all(usage.get(key, 0) == 0 for key in _USAGE_COUNTS)
    )


def _texts(message: Mapping[str, Any]) -> list[str]:
    content = message.get("content")
    if isinstance(content, str):
        return [content]
    if not isinstance(content, list):
        return []
    return [
        str(block.get("text"))
        for block in content
        if isinstance(block, dict) and isinstance(block.get("text"), str)
    ]


def transcript_usage(
    data: bytes,
    seeds: Sequence[Mapping[str, Any]] = (),
    *,
    scrub: Callable[[str], str] = lambda text: text,
) -> SessionUsage:
    """Usage of one native session file. Pure.

    A final record without its newline (an unterminated write), an unreadable
    line, a changed seed or an assistant record without an integer output
    count makes the usage unknown. Only records that exactly match a seeded
    history entry are excluded from the count.
    """

    if data and not data.endswith(b"\n"):
        return SessionUsage.unknown()
    per_message: dict[str, int] = {}
    models: set[str] = set()
    errors: list[dict[str, Any]] = []
    texts: list[str] = []
    for line in data.splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except ValueError:
            return SessionUsage.unknown()
        if not isinstance(record, dict) or record.get("type") != "assistant":
            continue
        seeded = _seeded(record, seeds)
        if seeded is None:
            return SessionUsage.unknown()
        if seeded:
            continue
        message = record.get("message")
        if not isinstance(message, dict):
            return SessionUsage.unknown()
        model = message.get("model")
        usage = message.get("usage")
        if model == SYNTHETIC_MODEL or _zero_usage(usage):
            found = pps.error_class(record)
            found["first_line"] = scrub(str(found.get("first_line") or ""))
            errors.append(found)
            if model == SYNTHETIC_MODEL:
                continue
        message_id = message.get("id")
        tokens = usage.get("output_tokens") if isinstance(usage, dict) else None
        if not isinstance(message_id, str) or type(tokens) is not int or tokens < 0:
            return SessionUsage.unknown()
        per_message[message_id] = max(per_message.get(message_id, 0), tokens)
        if isinstance(model, str):
            models.add(model)
        texts.extend(scrub(text) for text in _texts(message))
    return SessionUsage(
        output_tokens=sum(per_message.values()),
        assistant_turns=len(per_message),
        known=True,
        models=tuple(sorted(models)),
        error_classes=tuple(errors[: pps.MAX_ERROR_CLASSES]),
        texts=tuple(texts),
    )


def tool_results(data: bytes) -> list[str]:
    """Text of every tool result in a native session file (for marker checks)."""

    found: list[str] = []
    for line in data.splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            continue
        message = record.get("message") if isinstance(record, dict) else None
        content = message.get("content") if isinstance(message, dict) else None
        if not isinstance(content, list):
            continue
        for block in content:
            if isinstance(block, dict) and block.get("type") == "tool_result":
                inner = block.get("content")
                if isinstance(inner, str):
                    found.append(inner)
                elif isinstance(inner, list):
                    found.extend(
                        str(item.get("text"))
                        for item in inner
                        if isinstance(item, dict) and isinstance(item.get("text"), str)
                    )
    return found


# --------------------------------------------------------------------------
# Reservation math (pure)
# --------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class SessionRow:
    seq: int
    slot: str
    settled: bool
    known: bool
    output_tokens: int | None
    assistant_turns: int | None
    elapsed: float | None
    unreserved: bool = False


@dataclasses.dataclass(frozen=True)
class Reservation:
    seq: int
    slot: str
    tokens: int
    turns: int
    deadline: float


def spent(rows: Sequence[SessionRow]) -> tuple[int, int]:
    """Tokens and turns already consumed. Unknown usage consumes the whole ceiling."""

    if any(row.unreserved or (row.settled and not row.known) for row in rows):
        return MAX_OUTPUT_TOKENS, TOTAL_ASSISTANT_TURNS_CEILING
    tokens = turns = 0
    for row in rows:
        if row.settled:
            tokens += row.output_tokens or 0
            turns += row.assistant_turns or 0
        else:
            tokens += SESSION_OUTPUT_TOKENS
            turns += SESSION_ASSISTANT_TURNS_CEILING
    return tokens, turns


@dataclasses.dataclass(frozen=True)
class SpendReading:
    """The known usage so far, an open session's harvested usage included."""

    output_tokens: int
    assistant_turns: int
    # Assistant turns by session seq.
    session_turns: Mapping[int, int]

    @property
    def breach(self) -> str | None:
        """The ceiling passed, or None. Output tokens first: that is the spend guard."""

        if self.output_tokens > MAX_OUTPUT_TOKENS:
            return "output_tokens"
        if any(turns > SESSION_ASSISTANT_TURNS_CEILING for turns in self.session_turns.values()):
            return "session_assistant_turns"
        if self.assistant_turns > TOTAL_ASSISTANT_TURNS_CEILING:
            return "total_assistant_turns"
        return None


def spend_reading(
    rows: Sequence[SessionRow], open_usage: Mapping[int, SessionUsage] | None = None
) -> SpendReading:
    """Sum every session's known usage. Pure.

    ``open_usage`` is what a harvest read, by seq, for a session that may not
    have settled; a settled row's usage only rises with it. Unknown usage adds
    nothing here: ``spent`` already treats it as the whole ceiling.
    """

    tokens = turns = 0
    per_session: dict[int, int] = {}
    for row in rows:
        usage = (open_usage or {}).get(row.seq)
        read = (
            (usage.output_tokens or 0, usage.assistant_turns or 0)
            if usage and usage.known
            else None
        )
        if row.settled and row.known:
            settled = (row.output_tokens or 0, row.assistant_turns or 0)
            read = settled if read is None else (max(read[0], settled[0]), max(read[1], settled[1]))
        if read is None:
            continue
        tokens += read[0]
        turns += read[1]
        per_session[row.seq] = read[1]
    return SpendReading(tokens, turns, per_session)


def next_query_refusal(
    rows: Sequence[SessionRow], *, seq: int | None, session: SessionUsage | None
) -> str | None:
    """Why session ``seq`` may not start one more query, or None when it fits. Pure.

    Checked before any further model call can start: at the gate that holds
    every query's prompt, and before a Stop is released while a continuation
    may still follow. The worst query adds ``SESSION_TURNS`` assistant turns and
    ``QUERY_WORST_OUTPUT_TOKENS`` output tokens; it must fit the session's
    turns and token share and the run's turns and output tokens.
    """

    if seq is None:
        return "unreserved"
    if session is None or not session.known:
        return "usage_unknown"
    reading = spend_reading(rows, {seq: session})
    if reading.breach is not None:
        return reading.breach
    if reading.output_tokens + QUERY_WORST_OUTPUT_TOKENS > MAX_OUTPUT_TOKENS:
        return "output_tokens"
    turns = reading.session_turns.get(seq, 0)
    if turns + SESSION_TURNS > SESSION_ASSISTANT_TURNS_CEILING:
        return "session_assistant_turns"
    if reading.assistant_turns + SESSION_TURNS > TOTAL_ASSISTANT_TURNS_CEILING:
        return "total_assistant_turns"
    if (session.output_tokens or 0) + QUERY_WORST_OUTPUT_TOKENS > SESSION_OUTPUT_TOKENS:
        return "session_output_tokens"
    return None


def over_share(row: SessionRow) -> bool:
    return (
        row.settled
        and row.known
        and (
            (row.output_tokens or 0) > SESSION_OUTPUT_TOKENS
            or (row.assistant_turns or 0) > SESSION_ASSISTANT_TURNS_CEILING
            or (row.elapsed or 0.0) > SESSION_SECONDS
        )
    )


def reservation_decision(
    rows: Sequence[SessionRow],
    *,
    slot: str,
    now: float,
    live_deadline: float | None,
    grant_expires_at: float | None,
) -> Reservation:
    """Admit one more native session or refuse with a code. Pure.

    Refusals: the shares do not fit, an unreserved start or an over-share
    session happened, any usage is unknown or still unsettled, the plan is
    used up, the remaining allowance cannot cover one share, the live window
    cannot hold a session, or the grant would expire inside the window.
    """

    if not shares_fit():
        raise Refusal("ceiling_invalid")
    if slot not in SLOT_NAMES or any(row.slot == slot for row in rows):
        raise Refusal("paid_retry_refused", slot)
    if (
        any(row.unreserved for row in rows)
        or any(over_share(row) for row in rows)
        or spend_reading(rows).breach is not None
    ):
        raise Refusal("budget_exceeded")
    if any(not row.settled or not row.known for row in rows):
        raise Refusal("usage_unknown")
    if len(rows) >= MAX_SESSIONS:
        raise Refusal("allowance_exhausted", "sessions")
    tokens, turns = spent(rows)
    if (
        MAX_OUTPUT_TOKENS - tokens < SESSION_OUTPUT_TOKENS
        or TOTAL_ASSISTANT_TURNS_CEILING - turns < SESSION_ASSISTANT_TURNS_CEILING
    ):
        raise Refusal("allowance_exhausted", "tokens or turns")
    if live_deadline is None or now >= live_deadline:
        raise Refusal("live_window_exhausted")
    deadline = min(now + SESSION_SECONDS, live_deadline)
    if deadline - now < MIN_SESSION_WINDOW:
        raise Refusal("live_window_exhausted")
    if grant_expires_at is None or grant_expires_at < live_deadline + CLEANUP_SECONDS:
        raise Refusal("lifetime_insufficient", "--grant-expires-at")
    return Reservation(
        len(rows) + 1, slot, SESSION_OUTPUT_TOKENS, SESSION_ASSISTANT_TURNS_CEILING, deadline
    )


# --------------------------------------------------------------------------
# The private ledger
# --------------------------------------------------------------------------

LEDGER_FILE = "owner-enforcement.sqlite3"
_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS runs (run_id TEXT PRIMARY KEY, candidate TEXT NOT NULL, "
    "mode TEXT NOT NULL, started_at REAL NOT NULL, live_started_at REAL, live_deadline REAL, "
    "grant_expires_at REAL, cleanup_started_at REAL, cleanup_finished_at REAL, "
    "finished_at REAL, outcome TEXT)",
    "CREATE TABLE IF NOT EXISTS sessions (run_id TEXT NOT NULL, seq INTEGER NOT NULL, "
    "slot TEXT NOT NULL, reserved_at REAL NOT NULL, deadline REAL NOT NULL, "
    "tokens_reserved INTEGER NOT NULL, turns_reserved INTEGER NOT NULL, native_ref TEXT, "
    "started_at REAL, settled_at REAL, known INTEGER, output_tokens INTEGER, "
    "assistant_turns INTEGER, elapsed REAL, models TEXT, error_classes TEXT, "
    "unreserved INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (run_id, seq))",
    "CREATE TABLE IF NOT EXISTS observations (run_id TEXT NOT NULL, criterion TEXT NOT NULL, "
    "key TEXT NOT NULL, observed TEXT NOT NULL, recorded_at REAL NOT NULL, "
    "PRIMARY KEY (run_id, criterion, key))",
    "CREATE TABLE IF NOT EXISTS cleanup (run_id TEXT NOT NULL, kind TEXT NOT NULL, "
    "ref TEXT NOT NULL, status TEXT NOT NULL, detail TEXT NOT NULL, recorded_at REAL NOT NULL)",
)


class OwnerLedger:
    """Private SQLite ledger: reservations, settlements, observations and cleanup."""

    def __init__(self, connection: sqlite3.Connection, run_id: str, scrub: Callable[[str], str]):
        self.db = connection
        self.run_id = run_id
        self.scrub = scrub

    def __repr__(self) -> str:
        return "<owner ledger>"

    @classmethod
    def open(
        cls,
        state_dir: Path,
        run_id: str,
        *,
        create: bool,
        scrub: Callable[[str], str] = lambda text: text,
    ) -> OwnerLedger:
        path = state_dir / LEDGER_FILE
        if create:
            try:
                fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            except OSError:
                raise Refusal("ledger_unsafe") from None
            os.close(fd)
        elif not path.exists():
            raise Refusal("ledger_missing")
        require_private_file(path, "ledger_unsafe", LEDGER_FILE)
        connection = sqlite3.connect(path, timeout=10, isolation_level=None)
        connection.execute("PRAGMA journal_mode=DELETE")
        for statement in _SCHEMA:
            connection.execute(statement)
        ledger = cls(connection, run_id, scrub)
        if not create and ledger.run() is None:
            raise Refusal("ledger_missing")
        return ledger

    def close(self) -> None:
        self.db.close()

    # -- runs ---------------------------------------------------------------

    def run(self) -> dict[str, Any] | None:
        row = self.db.execute(
            "SELECT candidate, mode, started_at, live_started_at, live_deadline, "
            "grant_expires_at, cleanup_started_at, cleanup_finished_at, finished_at, outcome "
            "FROM runs WHERE run_id = ?",
            (self.run_id,),
        ).fetchone()
        if row is None:
            return None
        keys = (
            "candidate",
            "mode",
            "started_at",
            "live_started_at",
            "live_deadline",
            "grant_expires_at",
            "cleanup_started_at",
            "cleanup_finished_at",
            "finished_at",
            "outcome",
        )
        return dict(zip(keys, row, strict=True))

    def begin(self, candidate: str, mode: str, now: float) -> None:
        """One run per run ID. A live run ID is never reused, so no paid retry exists."""

        existing = self.run()
        if existing is not None:
            if existing["mode"] == "rehearse" and mode == "live":
                raise Refusal("rehearsal_run_id")
            raise Refusal("paid_retry_refused", "--run-id")
        self.db.execute(
            "INSERT INTO runs (run_id, candidate, mode, started_at) VALUES (?, ?, ?, ?)",
            (self.run_id, candidate, mode, now),
        )

    def start_live(self, now: float, grant_expires_at: float) -> float:
        deadline = now + LIVE_SECONDS
        self.db.execute(
            "UPDATE runs SET live_started_at = ?, live_deadline = ?, grant_expires_at = ? "
            "WHERE run_id = ? AND live_started_at IS NULL",
            (now, deadline, grant_expires_at, self.run_id),
        )
        # The ceilings this run is held to, for the budget judge and the receipt.
        for key, value in SPEND_CEILINGS.items():
            self.record("budget", key, value, now)
        return deadline

    def mark_cleanup(self, *, started: float | None = None, finished: float | None = None) -> None:
        if started is not None:
            self.db.execute(
                "UPDATE runs SET cleanup_started_at = ? WHERE run_id = ?", (started, self.run_id)
            )
        if finished is not None:
            self.db.execute(
                "UPDATE runs SET cleanup_finished_at = ? WHERE run_id = ?", (finished, self.run_id)
            )

    def finish(self, outcome: str, now: float) -> None:
        self.db.execute(
            "UPDATE runs SET finished_at = ?, outcome = ? WHERE run_id = ?",
            (now, outcome, self.run_id),
        )

    # -- sessions -----------------------------------------------------------

    def rows(self) -> list[SessionRow]:
        return [
            SessionRow(
                seq=row[0],
                slot=row[1],
                settled=row[2] is not None,
                known=bool(row[3]),
                output_tokens=row[4],
                assistant_turns=row[5],
                elapsed=row[6],
                unreserved=bool(row[7]),
            )
            for row in self.db.execute(
                "SELECT seq, slot, settled_at, known, output_tokens, assistant_turns, elapsed, "
                "unreserved FROM sessions WHERE run_id = ? ORDER BY seq",
                (self.run_id,),
            )
        ]

    def sessions(self) -> list[dict[str, Any]]:
        keys = (
            "seq",
            "slot",
            "reserved_at",
            "deadline",
            "tokens_reserved",
            "turns_reserved",
            "native_ref",
            "started_at",
            "settled_at",
            "known",
            "output_tokens",
            "assistant_turns",
            "elapsed",
            "models",
            "error_classes",
            "unreserved",
        )
        found = []
        for row in self.db.execute(
            "SELECT " + ", ".join(keys) + " FROM sessions WHERE run_id = ? ORDER BY seq",
            (self.run_id,),
        ):
            item = dict(zip(keys, row, strict=True))
            item["models"] = json.loads(item["models"] or "[]")
            item["error_classes"] = json.loads(item["error_classes"] or "[]")
            found.append(item)
        return found

    def reserve(self, slot: str, now: float) -> Reservation:
        run = self.run()
        if run is None or run["mode"] != "live":
            raise Refusal("ledger_missing")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            decision = reservation_decision(
                self.rows(),
                slot=slot,
                now=now,
                live_deadline=run["live_deadline"],
                grant_expires_at=run["grant_expires_at"],
            )
            self.db.execute(
                "INSERT INTO sessions (run_id, seq, slot, reserved_at, deadline, "
                "tokens_reserved, turns_reserved) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    self.run_id,
                    decision.seq,
                    slot,
                    now,
                    decision.deadline,
                    decision.tokens,
                    decision.turns,
                ),
            )
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        self.db.execute("COMMIT")
        return decision

    def mark_started(self, seq: int, native_ref: str, now: float) -> None:
        self.db.execute(
            "UPDATE sessions SET native_ref = ?, started_at = ? WHERE run_id = ? AND seq = ?",
            (native_ref, now, self.run_id, seq),
        )

    def settle(self, seq: int, usage: SessionUsage, elapsed: float | None, now: float) -> None:
        """Settle once; a later, larger observation of the same session only raises it."""

        current = self.db.execute(
            "SELECT settled_at, known, output_tokens, assistant_turns FROM sessions "
            "WHERE run_id = ? AND seq = ?",
            (self.run_id, seq),
        ).fetchone()
        if current is None:
            raise Refusal("ledger_missing")
        known = usage.known and (current[0] is None or bool(current[1]))
        tokens = usage.output_tokens if known else None
        turns = usage.assistant_turns if known else None
        if known and current[0] is not None:
            tokens = max(tokens or 0, current[2] or 0)
            turns = max(turns or 0, current[3] or 0)
        errors = [
            {
                key: self.scrub(str(value)) if isinstance(value, str) else value
                for key, value in item.items()
            }
            for item in usage.error_classes
        ]
        self.db.execute(
            "UPDATE sessions SET settled_at = ?, known = ?, output_tokens = ?, "
            "assistant_turns = ?, elapsed = ?, models = ?, error_classes = ? "
            "WHERE run_id = ? AND seq = ?",
            (
                now,
                int(known),
                tokens,
                turns,
                None if elapsed is None else round(elapsed, 3),
                json.dumps(list(usage.models)),
                json.dumps(errors),
                self.run_id,
                seq,
            ),
        )

    def check_spend(self, open_usage: Mapping[int, SessionUsage], now: float) -> SpendReading:
        """Read the spend so far, a harvested open session included, and record it."""

        reading = spend_reading(self.rows(), open_usage)
        self.record("budget", "spend_guard.output_tokens", reading.output_tokens, now)
        self.record("budget", "spend_guard.assistant_turns", reading.assistant_turns, now)
        if reading.breach is not None:
            self.record("budget", "spend_guard.breach", reading.breach, now)
        return reading

    def record_unreserved(self, native_ref: str, now: float) -> None:
        """A native start nothing reserved: it exhausts the allowance and fences starts."""

        seq = len(self.rows()) + 1
        self.db.execute(
            "INSERT INTO sessions (run_id, seq, slot, reserved_at, deadline, tokens_reserved, "
            "turns_reserved, native_ref, started_at, unreserved) "
            "VALUES (?, ?, 'unreserved', ?, ?, 0, 0, ?, ?, 1)",
            (self.run_id, seq, now, now, native_ref, now),
        )

    # -- observations and cleanup -------------------------------------------

    def record(self, criterion: str, key: str, observed: Any, now: float) -> None:
        text = self.scrub(json.dumps(observed, sort_keys=True, default=str))
        self.db.execute(
            "INSERT OR REPLACE INTO observations (run_id, criterion, key, observed, recorded_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (self.run_id, criterion, key, text, now),
        )

    def observations(self, criterion: str) -> dict[str, Any]:
        return {
            key: json.loads(value)
            for key, value in self.db.execute(
                "SELECT key, observed FROM observations WHERE run_id = ? AND criterion = ?",
                (self.run_id, criterion),
            )
        }

    def record_cleanup(self, kind: str, ref: str, status: str, detail: str, now: float) -> None:
        self.db.execute(
            "INSERT INTO cleanup (run_id, kind, ref, status, detail, recorded_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (self.run_id, kind, self.scrub(ref), status, self.scrub(detail), now),
        )

    def cleanup_rows(self) -> list[dict[str, Any]]:
        keys = ("kind", "ref", "status", "detail")
        return [
            dict(zip(keys, row, strict=True))
            for row in self.db.execute(
                "SELECT kind, ref, status, detail FROM cleanup WHERE run_id = ? ORDER BY rowid",
                (self.run_id,),
            )
        ]


# --------------------------------------------------------------------------
# The code host and the repository workspace (pure transforms)
# --------------------------------------------------------------------------

WORKSPACE_MOUNT = "/workspace"
WORKSPACE_SIZE = "1Gi"  # the chart's standard profile workspaceSizeLimit
WORKSPACE_INIT = "workspace-init"
# The claim env the chart's workspace-init declares empty, so the claim's
# Overrides injection has an entry to replace (charts/curie agent-sandbox.yaml).
WORKSPACE_INIT_ENV = (
    "CURIE_WORKSPACE_REF",
    "CURIE_WORKSPACE_SHA256",
    "CURIE_REPO_ORIGIN",
    "CURIE_REPO_PATH",
)
# The chart's codeHostTrust mount and the sandbox client env that reads it.
CODE_HOST_TRUST_VOLUME = "code-host-trust"
CODE_HOST_TRUST_DIR = "/etc/curie/code-host-trust"
CODE_HOST_TRUST_ENV = (
    "CURIE_REPO_CA_BUNDLE",
    "GIT_SSL_CAINFO",
    "SSL_CERT_FILE",
    "REQUESTS_CA_BUNDLE",
)
# The chart's default workspace values, substituted into its init program.
CHART_WORKSPACE_VALUES = {
    "{{ $wf.mountPath }}": WORKSPACE_MOUNT,
    "{{ $wf.fetchTimeoutSeconds }}": "45",
    "{{ $wf.extractTimeoutSeconds }}": "45",
    "{{ .Values.worker.workspace.maxArchiveBytes }}": "268435456",
}
_CHART_NOTICE = re.compile(
    r'\{\{ include "curie\.sandbox\.claimEnvNoOpNotice" \(dict [^)]*\) \| quote \}\}'
)
STUB_SEED_DIR = "/seed"
STUB_WORK_DIR = "/work"


def chart_workspace_init(template: str, values: Mapping[str, str]) -> str:
    """The chart's own ``workspace-init`` program, its template values filled in.

    Read from the candidate's ``agent-sandbox.yaml`` so the proof's sandbox
    stages the checkout exactly as an installed chart does. Raises
    ``ValueError`` when the block is missing or a template expression remains.
    """

    lines = template.splitlines()
    try:
        start = next(
            i for i, line in enumerate(lines) if line.strip() == f"- name: {WORKSPACE_INIT}"
        )
        block = next(i for i in range(start, len(lines)) if lines[i].strip() == "- |")
    except StopIteration:
        raise ValueError("the chart has no workspace-init program") from None
    indent = len(lines[block]) - len(lines[block].lstrip())
    body: list[str] = []
    for line in lines[block + 1 :]:
        if line.strip() and len(line) - len(line.lstrip()) <= indent:
            break
        body.append(line)
    program = textwrap.dedent("\n".join(body)).strip("\n") + "\n"
    program = _CHART_NOTICE.sub(
        lambda _match: repr("no CURIE_WORKSPACE_REF in this container's env: nothing to stage"),
        program,
    )
    for expression, value in values.items():
        program = program.replace(expression, value)
    if "{{" in program or "}}" in program or "CURIE_WORKSPACE_REF" not in program:
        raise ValueError("the chart's workspace-init program did not render")
    compile(program, WORKSPACE_INIT, "exec")
    return program


def workspace_template(
    spec: Mapping[str, Any], *, program: str, ca_config_map: str, ca_key: str
) -> dict[str, Any]:
    """The source SandboxTemplate with the chart's workspace and code host trust.

    Adds the claim-scoped ``workspace`` emptyDir (the chart's default scratch
    type), the ``workspace-init`` init container after the bundle pair, the
    runner's ``/workspace`` mount and ``safe.directory``, and the
    ``codeHostTrust`` CA mount with the chart's four client env names.
    """

    result: dict[str, Any] = copy.deepcopy(dict(spec))
    pod = result["podTemplate"]["spec"]
    inits = pod.get("initContainers") or []
    if not inits or any(item.get("name") == WORKSPACE_INIT for item in inits):
        raise ValueError("the source template cannot take a workspace-init container")
    runner = next((item for item in pod["containers"] if item.get("name") == "runner"), None)
    if runner is None:
        raise ValueError("the source template has no runner container")
    added_env = {"GIT_CONFIG_COUNT", "GIT_CONFIG_KEY_0", "GIT_CONFIG_VALUE_0", *CODE_HOST_TRUST_ENV}
    if any(entry.get("name") in added_env for entry in runner.get("env") or []):
        raise ValueError("the runner already names a workspace or trust variable")
    base = inits[0]
    pod["volumes"] = [
        *(pod.get("volumes") or []),
        {"name": "workspace", "emptyDir": {"sizeLimit": WORKSPACE_SIZE}},
        {
            "name": CODE_HOST_TRUST_VOLUME,
            "configMap": {"name": ca_config_map, "items": [{"key": ca_key, "path": "ca.crt"}]},
        },
    ]
    pod["initContainers"] = [
        *inits,
        {
            "name": WORKSPACE_INIT,
            **{
                key: copy.deepcopy(base[key])
                for key in ("image", "imagePullPolicy", "securityContext", "resources")
                if key in base
            },
            "command": ["python", "-c", program],
            "env": [{"name": name, "value": ""} for name in WORKSPACE_INIT_ENV],
            "volumeMounts": [{"name": "workspace", "mountPath": WORKSPACE_MOUNT}],
        },
    ]
    trust = f"{CODE_HOST_TRUST_DIR}/ca.crt"
    runner["volumeMounts"] = [
        *(runner.get("volumeMounts") or []),
        {"name": "workspace", "mountPath": WORKSPACE_MOUNT},
        {"name": CODE_HOST_TRUST_VOLUME, "mountPath": CODE_HOST_TRUST_DIR, "readOnly": True},
    ]
    runner["env"] = [
        *(runner.get("env") or []),
        {"name": "GIT_CONFIG_COUNT", "value": "1"},
        {"name": "GIT_CONFIG_KEY_0", "value": "safe.directory"},
        {"name": "GIT_CONFIG_VALUE_0", "value": WORKSPACE_MOUNT},
        *({"name": name, "value": trust} for name in CODE_HOST_TRUST_ENV),
    ]
    return result


def code_host_manifests(
    base: Sequence[Mapping[str, Any]],
    *,
    image: str,
    port: int,
    seed_files: Mapping[str, str],
    issue_body: str,
    boot: str,
    labels: Mapping[str, str],
) -> list[dict[str, Any]]:
    """The recorded stub's ConfigMap, Pod and Service, with a seeded repository.

    ``base`` is ``tls_code_host.stub_manifests``. The stub requires a seed tree
    and an issue file; both ride in its ConfigMap, and ``boot`` copies them
    into a Pod-owned directory before it starts the stub with the added flags.
    The Pod runs the digest-pinned candidate image and reports Ready only once
    it listens.
    """

    manifests = [copy.deepcopy(dict(item)) for item in base]
    by_kind = {item["kind"]: item for item in manifests}
    if set(by_kind) != {"ConfigMap", "Pod", "Service"} or len(manifests) != 3:
        raise ValueError("the stub manifests are not one ConfigMap, Pod and Service")
    source, pod = by_kind["ConfigMap"], by_kind["Pod"]
    names = sorted(seed_files)
    if not names or any("/" in name or name.startswith(".") for name in names):
        raise ValueError("seed files must be plain top-level names")
    keys = {f"seed-{index}": name for index, name in enumerate(names)}
    source["data"] = {
        **source["data"],
        **{key: seed_files[name] for key, name in keys.items()},
        "seed-issue": issue_body,
    }
    for item in manifests:
        meta = item["metadata"]
        meta["labels"] = {**(meta.get("labels") or {}), **labels}
    spec = pod["spec"]
    container = spec["containers"][0]
    container.update(
        {
            "image": image,
            "imagePullPolicy": "Always",
            "command": ["python3", "-c", boot],
            "args": [
                *container["args"],
                "--seed-tree",
                f"{STUB_WORK_DIR}/tree",
                "--issue-file",
                f"{STUB_WORK_DIR}/issue.md",
            ],
            "readinessProbe": {"tcpSocket": {"port": port}, "periodSeconds": 1},
        }
    )
    container["volumeMounts"] = [
        *container["volumeMounts"],
        {"name": "seed", "mountPath": STUB_SEED_DIR, "readOnly": True},
        {"name": "work", "mountPath": STUB_WORK_DIR},
    ]
    items = [{"key": key, "path": f"tree/{name}"} for key, name in keys.items()]
    spec["volumes"] = [
        *spec["volumes"],
        {
            "name": "seed",
            "configMap": {
                "name": source["metadata"]["name"],
                "items": [*items, {"key": "seed-issue", "path": "issue.md"}],
            },
        },
        {"name": "work", "emptyDir": {"sizeLimit": "16Mi"}},
    ]
    return manifests


def committed_restoration(
    source: Mapping[str, Any], before: Mapping[str, Any], after: Mapping[str, Any]
) -> dict[str, bool]:
    """Whether the workspace held the stub repository's committed state on both sides.

    ``source`` is the stub's bare repository (main's commit and the SHA-256 of
    each committed blob); ``before`` and ``after`` are the sandbox workspace
    (HEAD and the SHA-256 of each tracked file) before the interruption and
    after recovery.
    """

    head, files = source.get("head"), source.get("files")
    return {
        "committed_head_restored": bool(head)
        and before.get("head") == head
        and after.get("head") == head,
        "committed_files_restored": bool(files)
        and before.get("files") == files
        and after.get("files") == files,
    }


# --------------------------------------------------------------------------
# Runner reachability: the worker runs outside the cluster
# --------------------------------------------------------------------------

# The driver's own source files, named in a failure site.
DRIVER_STEM = "personal_owner_enforcement"
# The worker's default runner port (curie_worker.sandbox.types.SubstrateConfig).
RUNNER_PORT = 8080
# Request statuses that refuse a new admission on the same WorkItem
# (curie_api.workitems.lifecycle._ACTIVE_STATUSES).
ACTIVE_REQUEST_STATUSES = ("waiting", "running", "cancellation_requested")
# The label every namespace this run creates carries, valued with its ownership.
OWNERSHIP_LABEL = "curie-proof-owner"


def runner_suffix(namespace: str) -> str:
    """The DNS suffix of every runner Service the worker dials in ``namespace``."""

    return f".{namespace}.svc.cluster.local"


def runner_loopback(name: str) -> str:
    """The loopback address the proof's worker resolves one runner Service name to.

    Deterministic, so the worker's resolver and the driver's forwards agree
    without sharing state; never 127.0.0.1, where the driver's other relays
    listen. The proof's worker launcher carries this function's source.
    """

    digest = hashlib.sha256(name.lower().rstrip(".").encode()).digest()
    return f"127.{64 + digest[0] % 128}.{digest[1]}.{1 + digest[2] % 254}"


def runner_forwards(
    sandboxes: Sequence[Mapping[str, Any]],
    pods: Sequence[Mapping[str, Any]],
    namespace: str,
) -> dict[str, dict[str, str]]:
    """Each runner Service name the worker dials, mapped to its loopback and Running Pod.

    A Sandbox's Pod carries its name, or an owner reference to it when it was
    adopted; its Service name is ``status.serviceFQDN``, or the Sandbox name
    before that is published. A terminating Pod gets no forward. Two names on
    one address refuse, because the worker would reach the wrong runner.
    """

    suffix = runner_suffix(namespace)
    named: dict[str, Mapping[str, Any]] = {}
    adopted: dict[str, Mapping[str, Any]] = {}
    for pod in pods:
        meta = pod.get("metadata") or {}
        if (pod.get("status") or {}).get("phase") != "Running" or meta.get("deletionTimestamp"):
            continue
        named[str(meta.get("name"))] = meta
        for ref in meta.get("ownerReferences") or []:
            if ref.get("kind") == "Sandbox" and ref.get("name"):
                adopted[str(ref["name"])] = meta
    found: dict[str, dict[str, str]] = {}
    for sandbox in sandboxes:
        name = str((sandbox.get("metadata") or {}).get("name") or "")
        published = (sandbox.get("status") or {}).get("serviceFQDN")
        fqdn = str(published or f"{name}{suffix}").lower().rstrip(".")
        meta = named.get(name) or adopted.get(name)
        if not name or not fqdn.endswith(suffix) or meta is None:
            continue
        found[fqdn] = {
            "address": runner_loopback(fqdn),
            "pod": str(meta.get("name")),
            "uid": str(meta.get("uid")),
        }
    addresses = [target["address"] for target in found.values()]
    if len(set(addresses)) != len(addresses):
        raise ValueError("two runner Service names share one loopback address")
    return found


def history_restored(before: Sequence[str], loaded: Sequence[str] | None) -> bool:
    """The recovered runtime's history holds every record from before, in order.

    A turn that finished before the interruption appends its own record after
    them, so the comparison is a prefix, never a subset.
    """

    return bool(before) and loaded is not None and list(loaded[: len(before)]) == list(before)


def worker_launched(
    row: Mapping[str, Any] | None,
    *,
    claim: str,
    captured_owner: str | None,
    worker_owner: str,
    rehearsal: bool,
) -> bool:
    """The candidate worker, not the driver, acquired and started this request.

    The start route records ``started_at`` and the claim only for the owner
    holding the acquire, and ``runtime_owner`` is that owner until terminal
    settlement clears it, so the driver captures it while the request runs. A
    rehearsal's offline turn can settle before the first poll; there a started,
    settled request on the observed claim is the signal.
    """

    if row is None or row.get("started_at") is None:
        return False
    if not claim or row.get("runtime_claim_name") != claim:
        return False
    if captured_owner is not None:
        return captured_owner == worker_owner
    return rehearsal and row.get("terminal_at") is not None


def retained_request(rows: Sequence[Mapping[str, Any]], claim: str) -> Mapping[str, Any] | None:
    """The latest started request whose runtime is ``claim``: the one a retained claim holds."""

    held = [
        row
        for row in rows
        if claim and row.get("runtime_claim_name") == claim and row.get("started_at") is not None
    ]
    return held[-1] if held else None


def active_requests(rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Requests that refuse a new admission on their WorkItem (``active_request``)."""

    return [row for row in rows if row.get("status") in ACTIVE_REQUEST_STATUSES]


def thread_claim_prefix(thread_key: str, claim_prefix: str = "curie-thread") -> str:
    """The prefix of every claim of a thread: ``SubstrateConfig.claim_name_for(key, "")``."""

    digest = hashlib.sha256(thread_key.encode("utf-8")).hexdigest()[:10]
    return f"{claim_prefix}-{digest}-"


def request_on_claim(
    rows: Sequence[Mapping[str, Any]], claim: str, prefix: str
) -> Mapping[str, Any] | None:
    """The started request a runtime claim of the WorkItem serves, or None.

    The start route writes ``runtime_claim_name`` once per request. A resume
    inside a running request binds a new claim of the same thread without a
    new start, so a claim on the thread's prefix maps to the latest started
    request while that request is still active.
    """

    started = [row for row in rows if row["runtime_claim_name"] and row["started_at"] is not None]
    exact = [row for row in started if row["runtime_claim_name"] == claim]
    if exact:
        return exact[-1]
    if not started or not claim.startswith(prefix):
        return None
    latest = started[-1]
    if not str(latest["runtime_claim_name"]).startswith(prefix):
        return None
    return latest if latest["status"] in ACTIVE_REQUEST_STATUSES else None


def waiting_request_started(rows: Sequence[Mapping[str, Any]], request_id: str) -> bool | None:
    row = next((r for r in rows if str(r["id"]) == request_id), None)
    return None if row is None else row["started_at"] is not None


def namespace_owned(meta: Mapping[str, Any] | None, *, uid: str | None, ownership: str) -> bool:
    """The namespace is the one this run created: its UID and its ownership label.

    A namespace this run did not create (no recorded UID) is never touched.
    """

    if not uid or not meta:
        return False
    labels = meta.get("labels") or {}
    return meta.get("uid") == uid and labels.get(OWNERSHIP_LABEL) == ownership


def fallback_runtimes(
    claims: Mapping[str, Mapping[str, str]],
    pods: Mapping[str, str],
    *,
    observed_claims: Mapping[str, Mapping[str, str]],
    labels: Mapping[str, str],
    owner: Mapping[str, str],
) -> list[str]:
    """Every claim or runner Pod without the owner's personal provenance.

    ``claims`` maps a claim name to its labels; ``pods`` maps a runner Pod to
    the claim it serves. ``labels`` names the mode, tenant and principal label
    keys and ``owner`` their values. A service claim carries no mode label at
    all, so anything short of the owner's personal labels counts, and a Pod
    whose claim was never seen counts too. A deleted claim's previously
    observed labels remain evidence for a Pod still terminating after it.
    Current labels always override that earlier observation.
    """

    def personal(claim_labels: Mapping[str, str]) -> bool:
        return claim_labels.get(labels["mode"]) == "personal" and all(
            claim_labels.get(labels[key]) == owner[key] for key in ("tenant", "principal")
        )

    found = [f"claim/{name}" for name, item in sorted(claims.items()) if not personal(item)]
    found += [
        f"pod/{name}"
        for name, claim in sorted(pods.items())
        if not personal(claims[claim] if claim in claims else observed_claims.get(claim, {}))
    ]
    return found


@dataclasses.dataclass(frozen=True)
class CleanupBudget:
    """One remaining-time budget across every cleanup step."""

    started: float
    seconds: float = CLEANUP_SECONDS
    # Kept back for the absence proof, which always runs last.
    reserve: float = 30.0

    def remaining(self, now: float) -> float:
        return max(0.0, self.started + self.seconds - now)

    def wait_bound(self, now: float) -> float:
        """How long one cleanup wait may still take, leaving the absence proof its time."""

        return max(0.0, self.remaining(now) - self.reserve)


def vanished(exc: BaseException) -> bool:
    """A Kubernetes read or delete of an object that no longer exists (HTTP 404)."""

    return getattr(exc, "status", None) == 404


def failure_site(exc: BaseException, root: Path) -> str:
    """Where an unexpected exception came from: its class and function names, never values.

    Names the innermost frame in a source file under ``root`` (outside any
    virtual environment) and, when it differs, the innermost frame of the
    driver itself, each as ``function (file:line)`` with the file's base name.
    """

    frames = [
        frame
        for frame in traceback.extract_tb(exc.__traceback__)
        if Path(frame.filename).is_relative_to(root) and ".venv" not in Path(frame.filename).parts
    ]
    driver = [frame for frame in frames if Path(frame.filename).name.startswith(DRIVER_STEM)]

    def where(frame: traceback.FrameSummary) -> str:
        return f"{frame.name} ({Path(frame.filename).name}:{frame.lineno})"

    site = type(exc).__name__
    if frames:
        site += f" in {where(frames[-1])}"
    if driver and (not frames or driver[-1] is not frames[-1]):
        site += f" from {where(driver[-1])}"
    return site


# --------------------------------------------------------------------------
# The judge: expected values live here, observed values only in the ledger
# --------------------------------------------------------------------------

CRITERIA = (
    "selection",
    "fencing",
    "authentication-required",
    "disconnect",
    "restart",
    "hosted-header",
    "budget",
    "cleanup",
)
RESTART_KINDS = ("process", "pod", "worker")

EXPECTED: dict[str, dict[str, Any]] = {
    "selection": {
        "admission.model_auth_mode": "personal",
        "admission.enrollment_generation": 1,
        "other_principal.admission_code": "enrollment_inactive",
        "claim.label.model_auth_mode": "personal",
        "claim.label.owner_tenant_matches": True,
        "claim.label.owner_principal_matches": True,
        "claim.label.enrollment_generation": "1",
        "claim.tokens_secret_carries_grant": True,
        "pod.grant_env_from_tokens_secret": True,
        "pod.service_credential_names": [],
        "pod.image_digest_matches": True,
        "pod.revision_label_matches": True,
        "worker.model_credential_names": [],
        "worker.launched_without_driver": True,
    },
    "fencing": {
        "advance.generation": 2,
        "advance.request_cause": "enrollment_fenced",
        "advance.revalidation_while_retained": "runtime_live",
        "advance.old_generation_custody": "generation_mismatch",
        "retained.native_sessions_after_restart": 0,
        "retained.teardown_confirmed": True,
        "retained.claim_absent": True,
        "retained.tokens_secret_absent": True,
        "revalidated.generation": 2,
    },
    "authentication-required": {
        "rejected.http_status": 401,
        "rejected.provider_error_type": "authentication_error",
        "rejected.terminal_cause": "authentication_required",
        "rejected.output_tokens": 0,
        "rejected.transcript_preserved": True,
        "rejected.owner_preserved": True,
        "rejected.service_fallback_claims": 0,
        "revalidated.generation": 3,
        "revalidated.fresh_native_process": True,
        "revalidated.replay_marker_seen": True,
        "revalidated.assistant_turns_positive": True,
    },
    "disconnect": {
        "disconnect.generation": 4,
        "disconnect.state": "inactive",
        "disconnect.waiting_request_started": False,
        "disconnect.custody_secrets": 0,
        "disconnect.claims_after_restarts": 0,
        "disconnect.pods_after_restarts": 0,
        "disconnect.tokens_secrets_after_restarts": 0,
        "disconnect.native_sessions_after_restarts": 0,
        "disconnect.admission_code": "enrollment_inactive",
        "disconnect.revalidation_code": "enrollment_inactive",
        "disconnect.custody_reader_code": "enrollment_inactive",
        "disconnect.route": "absent",
    },
    "restart": {
        **{
            f"{kind}.{key}": True
            for kind in RESTART_KINDS
            for key in (
                "owner_labels_match",
                "history_identity",
                "replay_marker_seen",
                "fresh_native_process",
                "automatic_recovery",
                "old_runtime_gone",
                # The stub repository's commit and committed blobs, in the
                # sandbox workspace before the interruption and after recovery.
                "committed_head_restored",
                "committed_files_restored",
            )
        },
        "process.same_pod": True,
        "process.container_restarted": True,
        # The same Pod keeps its workspace volume, uncommitted files included.
        "process.workspace_nonce_restored": True,
        "pod.new_pod": True,
        # Not met by design today: #5541 (blocked by #5092).
        "pod.workspace_nonce_restored": True,
        "worker.worker_restarted": True,
    },
    "hosted-header": {
        "sentinel.challenge_returned": True,
        "sentinel.authorized_calls_positive": True,
        "sentinel.unauthorized_calls": 0,
        "public_control.returned": True,
        "bash.scan_ran": True,
        "bash.negative_control_found": True,
        "bash.header_found": False,
        # ADR 0268 decision 4: native coding tools are outside the custody
        # boundary, so Bash must not recover the grant (acceptance row 2).
        "bash.grant_found": False,
        # Every runner Pod's logs were kept, so the negatives below are complete.
        "audit.captures_complete": True,
        "audit.header_in_captures": False,
        "audit.grant_in_captures": False,
        "audit.invalid_grant_in_captures": False,
        "audit.header_in_ledger_or_output": False,
    },
}
# Recorded as Not met, naming the blocking issue, when observed differs.
NOT_MET: dict[tuple[str, str], str] = {
    ("restart", "pod.workspace_nonce_restored"): BLOCKING_ISSUE,
}
CLEANUP_KINDS = (
    "namespace",
    "grant_namespace",
    "claims",
    "pods",
    "grant_copies",
    "pvcs",
    "pvs",
    "code_host",
    "registry",
    "containers",
    "port_forwards",
    "processes",
    "private_dir",
    "foreign_untouched",
)


def _check(
    key: str, expected: Any, found: dict[str, Any], blocked_by: str | None
) -> dict[str, Any]:
    if key not in found:
        return {"key": key, "expected": expected, "observed": None, "status": "missing"}
    observed = found[key]
    if observed == expected:
        status = "pass"
    elif blocked_by is not None:
        status = "not_met"
    else:
        status = "fail"
    check = {"key": key, "expected": expected, "observed": observed, "status": status}
    if status == "not_met" and blocked_by is not None:
        check["blocked_by"] = blocked_by
        if blocked_by in BLOCKING_UPSTREAM:
            check["upstream"] = BLOCKING_UPSTREAM[blocked_by]
    return check


def _judge_observed(ledger: OwnerLedger, criterion: str) -> list[dict[str, Any]]:
    found = ledger.observations(criterion)
    return [
        _check(key, expected, found, NOT_MET.get((criterion, key)))
        for key, expected in EXPECTED[criterion].items()
    ]


def _within(key: str, ceiling: int, observed: int | None) -> dict[str, Any]:
    """A count against its ceiling: expected ``<= ceiling``, observed the count."""

    status = "missing" if observed is None else "pass" if observed <= ceiling else "fail"
    return {"key": key, "expected": f"<= {ceiling}", "observed": observed, "status": status}


def _judge_budget(ledger: OwnerLedger) -> list[dict[str, Any]]:
    run = ledger.run() or {}
    sessions = ledger.sessions()
    rows = ledger.rows()
    unknown = any(not item["known"] for item in sessions)
    reading = spend_reading(rows)
    recorded = ledger.observations("budget")
    zero_usage = [item for item in sessions if item["known"] and not item["output_tokens"]]
    live = None
    if run.get("live_started_at") is not None and run.get("cleanup_started_at") is not None:
        live = round(run["cleanup_started_at"] - run["live_started_at"], 3)
    models = sorted({model for item in sessions for model in item["models"]})
    probe = recorded.get("grant_probe.ok")
    found = {
        **{key: recorded[key] for key in SPEND_CEILINGS if key in recorded},
        "spend_guard.breach": recorded.get("spend_guard.breach"),
        "sessions_within_ceiling": len(sessions) <= MAX_SESSIONS,
        "every_session_within_share": not any(over_share(row) for row in rows),
        "usage_known": not unknown,
        "unreserved_starts": sum(bool(item["unreserved"]) for item in sessions),
        "paid_retries": sum(
            1
            for slot in {item["slot"] for item in sessions}
            if [i["slot"] for i in sessions].count(slot) > 1
        ),
        "live_seconds_within_ceiling": live is not None and live <= LIVE_SECONDS,
        "models": [model for model in models if not pps.same_model(model, MODEL)],
        "zero_usage_sessions_classified": all(item["error_classes"] for item in zero_usage),
        "grant_probe_ok": probe is True,
    }
    expected = {
        **SPEND_CEILINGS,
        "spend_guard.breach": None,
        "sessions_within_ceiling": True,
        "every_session_within_share": True,
        "usage_known": True,
        "unreserved_starts": 0,
        "paid_retries": 0,
        "live_seconds_within_ceiling": True,
        "models": [],
        "zero_usage_sessions_classified": True,
        "grant_probe_ok": True,
    }
    if not sessions:
        found.pop("usage_known")
    counts = [
        _within(
            f"session.{item['slot']}.assistant_turns",
            SESSION_ASSISTANT_TURNS_CEILING,
            item["assistant_turns"] if item["known"] else None,
        )
        for item in sessions
        if not item["unreserved"]
    ]
    totals = [
        _within(
            "total.assistant_turns",
            TOTAL_ASSISTANT_TURNS_CEILING,
            None if unknown else reading.assistant_turns,
        ),
        _within(
            "total.output_tokens", MAX_OUTPUT_TOKENS, None if unknown else reading.output_tokens
        ),
    ]
    return [
        *(_check(key, value, found, None) for key, value in expected.items()),
        *counts,
        *totals,
    ]


def _judge_cleanup(ledger: OwnerLedger) -> list[dict[str, Any]]:
    run = ledger.run() or {}
    rows = ledger.cleanup_rows()
    found: dict[str, Any] = {}
    for kind in CLEANUP_KINDS:
        statuses = [row["status"] for row in rows if row["kind"] == kind]
        if statuses:
            found[f"{kind}.absent"] = all(status in {"absent", "ok"} for status in statuses)
    steps = [row for row in rows if row["kind"] == "step"]
    if steps:
        found["finalizer.steps_ok"] = all(row["status"] == "ok" for row in steps)
    started, finished = run.get("cleanup_started_at"), run.get("cleanup_finished_at")
    if started is not None and finished is not None:
        found["cleanup.within_bound"] = finished - started <= CLEANUP_SECONDS
    expected = {f"{kind}.absent": True for kind in CLEANUP_KINDS}
    expected["finalizer.steps_ok"] = True
    expected["cleanup.within_bound"] = True
    return [_check(key, value, found, None) for key, value in expected.items()]


REHEARSAL_ONLY = {
    "authentication-required": {
        "rejected.http_status",
        "rejected.provider_error_type",
        "rejected.terminal_cause",
        "rejected.output_tokens",
        "rejected.transcript_preserved",
        "revalidated.replay_marker_seen",
        "revalidated.assistant_turns_positive",
    },
    "hosted-header": {
        "audit.captures_complete",
        "sentinel.challenge_returned",
        "sentinel.authorized_calls_positive",
        "sentinel.unauthorized_calls",
        "public_control.returned",
        "bash.scan_ran",
        "bash.negative_control_found",
        "bash.header_found",
        "bash.grant_found",
    },
}


def judge_rehearsal(ledger: OwnerLedger) -> dict[str, Any]:
    run = ledger.run() or {}
    if run.get("mode") != "rehearse":
        raise Refusal("rehearsal_ledger", "--criterion rehearsal")
    criteria = []
    for name in CRITERIA:
        if name == "budget":
            checks = [
                _check("native_sessions", 0, {"native_sessions": len(ledger.sessions())}, None),
                {
                    "key": "per_response_output_cap",
                    "expected": RESPONSE_OUTPUT_TOKENS,
                    "observed": "fake model does not emit native provider usage",
                    "status": "rehearsal_only",
                },
            ]
        elif name == "cleanup":
            checks = _judge_cleanup(ledger)
        else:
            checks = _judge_observed(ledger, name)
            for check in checks:
                if check["key"] in REHEARSAL_ONLY.get(name, set()):
                    check["status"] = "rehearsal_only"
                    check["reason"] = "requires the native provider; fake-model reading retained"
        if name == "hosted-header":
            checks.append(
                {
                    "key": "audit.pod_captures_missing",
                    "expected": 0,
                    "observed": ledger.observations(name).get("audit.pod_captures_missing"),
                    "status": "rehearsal_only",
                    "reason": "fake turns cannot hold a quiescent native capture boundary",
                }
            )
        criteria.append({"criterion": name, "verdict": verdict_of(checks), "checks": checks})
    return {
        "run_id": ledger.run_id,
        "candidate": run["candidate"],
        "criterion": "rehearsal",
        "evidence_mode": "unpaid_rehearsal",
        "ceilings": ceiling(),
        "verdict": verdict_of({"status": c["verdict"]} for c in criteria),
        "criteria": criteria,
    }


def judge(ledger: OwnerLedger, criterion: str) -> dict[str, Any]:
    """One criterion's verdict from the ledger only. ``all`` judges every criterion."""

    if criterion == "rehearsal":
        return judge_rehearsal(ledger)
    if criterion != "all" and criterion not in CRITERIA:
        raise Refusal("criterion_invalid", "--criterion")
    run = ledger.run()
    if run is None:
        raise Refusal("ledger_missing")
    if run["mode"] == "rehearse" and criterion != "cleanup":
        raise Refusal("rehearsal_ledger", "--criterion")
    names = CRITERIA if criterion == "all" else (criterion,)
    verdicts = []
    for name in names:
        if name == "budget":
            checks = _judge_budget(ledger)
        elif name == "cleanup":
            checks = _judge_cleanup(ledger)
        else:
            checks = _judge_observed(ledger, name)
        verdicts.append({"criterion": name, "verdict": verdict_of(checks), "checks": checks})
    overall = verdict_of([{"status": item["verdict"]} for item in verdicts])
    return {
        "run_id": ledger.run_id,
        "candidate": run["candidate"],
        "criterion": criterion,
        "verdict": overall,
        "criteria": verdicts,
    }


def verdict_of(checks: Iterable[Mapping[str, Any]]) -> str:
    statuses = [str(check["status"]) for check in checks]
    if any(status in {"fail", "missing"} for status in statuses):
        return "fail" if "fail" in statuses else "missing"
    if any(status == "not_met" for status in statuses):
        return "not_met"
    if any(status == "rehearsal_only" for status in statuses):
        return "rehearsal_only"
    return "pass"


def exit_code(verdict: str) -> int:
    return {"pass": EXIT_PASS, "rehearsal_only": EXIT_PASS, "not_met": EXIT_NOT_MET}.get(
        verdict, EXIT_FAIL
    )


# --------------------------------------------------------------------------
# The finalizer: LIFO, registered before each resource is created
# --------------------------------------------------------------------------


class StepFailed(Exception):
    """A step failed. The message is fixed text, never command output."""


class Finalizer:
    """Every registered step runs, newest first, even after earlier ones fail."""

    def __init__(self, record: Callable[[str, bool, str], None] | None = None) -> None:
        self.record = record
        self.steps: list[tuple[str, Callable[[], None]]] = []

    def __repr__(self) -> str:
        return "<finalizer>"

    def register(self, name: str, step: Callable[[], None]) -> None:
        self.steps.append((name, step))

    def run(self) -> list[tuple[str, bool]]:
        results: list[tuple[str, bool]] = []
        while self.steps:
            name, step = self.steps.pop()
            detail = ""
            try:
                step()
                ok = True
            except BaseException as exc:  # noqa: BLE001 - every registered step runs
                ok = False
                detail = str(exc) if isinstance(exc, (StepFailed, Refusal)) else type(exc).__name__
            results.append((name, ok))
            if self.record is not None:
                self.record(name, ok, detail)
        return results


# --------------------------------------------------------------------------
# Output scrubbing
# --------------------------------------------------------------------------


def sanitized_json(payload: Mapping[str, Any], scrubber: pps.Scrubber) -> str:
    """The only form any payload leaves the driver in."""

    return scrubber.scrub(json.dumps(payload, sort_keys=True, default=str))


__all__ = [
    "CRITERIA",
    "EXPECTED",
    "Finalizer",
    "Flags",
    "InputFile",
    "OwnerLedger",
    "Refusal",
    "Reservation",
    "SESSION_PLAN",
    "SessionRow",
    "SessionUsage",
    "StepFailed",
    "child_env",
    "judge",
    "load_input_file",
    "release_name",
    "require_expiry",
    "require_flags",
    "reservation_decision",
    "shares_fit",
    "stat_grant_file",
    "transcript_usage",
]
