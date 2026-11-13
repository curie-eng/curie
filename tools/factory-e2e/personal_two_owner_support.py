"""Pure core of the two-owner personal factory lifecycle scenario (#5379).

The live driver (``personal_two_owner.py``) runs one fixed scenario in which two
independently enrolled owners share one installation. Everything that decides
whether a paid start may happen, and whether the run passed, lives here so it
can be tested without a cluster:

1. the approved ceilings and the canonical ``--approve-live`` string;
2. the private inputs-file loader and its refusals;
3. the 16-start manifest, the production-transition map and the unpaid
   schedule check (gate G-SESSION-PLAN);
4. an owner-aware subclass of the single-profile ledger with per-owner and
   aggregate guards;
5. the in-pod response gate (a bundle hook and a stream program) and the pure
   decision the driver takes at every hold;
6. Dex, OIDC and chart values for a two-owner install, and a scan for any
   installation-wide grant;
7. the OIDC login flow with an injected opener;
8. issue fixtures, and the evidence contract and judges, with the credential
   capture audit's channels and the approval-card residue record;
9. the native-process interruption program and the per-claim grant-delivery
   readers (digests and names only);
10. the recovery evidence each interruption is judged on: the read-only
    workspace program, committed-state restoration against the owned forge,
    the replayed history marker and the old runtime's epoch confirmation.

Nothing here touches a cluster, a forge or a network. The only IO is the
SQLite ledger file, the inputs file and the files it names (stat and the
existing ``personal_profile_support`` validators) and the issue fixtures. No
function returns, logs or raises a held value; refusals carry codes and field
names only.
"""

from __future__ import annotations

import base64
import copy
import dataclasses
import hashlib
import hmac
import html
import http.cookies
import json
import math
import os
import re
import sqlite3
import stat
import sys
import time
import urllib.parse
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import personal_owner_enforcement_support as poes  # noqa: E402
import personal_profile_support as pps  # noqa: E402

# --------------------------------------------------------------------------
# A. Approved ceilings (coordinator decision 1, 2026-11-11)
# --------------------------------------------------------------------------

#: Native session starts in the whole run, both probes included.
MAX_SESSIONS = 16
#: Assistant turns across every session.
MAX_TURNS = 192
#: Assistant turns in one session (the runner's CURIE_MAX_TURNS).
SESSION_TURNS = 32
#: Output tokens across the run, and the item's aggregate actual-spend guard.
MAX_OUTPUT_TOKENS = 32768
#: Output tokens in one session, checked after every response.
SESSION_OUTPUT_TOKENS = 8192
#: Absolute seconds one session may run before the runner is interrupted.
SESSION_SECONDS = 300
#: Seconds from the live start to the end of paid work.
LIVE_SECONDS = 4800
#: Seconds the finalizer may spend after the live window.
CLEANUP_SECONDS = 600
#: No paid retry, diagnostic rerun or second live start for a run ID.
PAID_RETRIES = 0
#: Output tokens one owner may spend, its probe included.
OWNER_OUTPUT_TOKENS = 16384
#: The isolated grant probe: model, output tokens, turns and seconds.
PROBE_MODEL = "claude-haiku-5-5"
PROBE_OUTPUT_TOKENS = 32
PROBE_TURNS = 1
PROBE_SECONDS = 30
#: One model response at most (the runner's CLAUDE_CODE_MAX_OUTPUT_TOKENS).
RESPONSE_OUTPUT_TOKENS = 2048
#: The agent's per-request execution deadline, set through ``curie overrides``.
EXECUTION_DEADLINE = 3600
#: How long the in-pod gate holds one hook before failing closed.
HOLD_SECONDS = 240
#: The hook timeout in hooks.json; above HOLD_SECONDS so the gate decides first.
HOOK_TIMEOUT = 250
#: The two owners, in manifest order.
OWNERS = ("a", "b")
#: An owner's usable window must cover the live and cleanup windows.
GRANT_MIN_LIFETIME = LIVE_SECONDS + CLEANUP_SECONDS
#: Planned minimum shares: a group with an implementer, any other group, turns.
MIN_IMPLEMENTER_SHARE = 2700
MIN_SHARE = 1000
MIN_TURN_SHARE = 16
#: Worker semaphore: two owners plus a terminate or CI wake.
WORKER_MAX_CONCURRENCY = 3
#: Sandbox quota for two concurrent 8Gi factory sandboxes.
SANDBOX_REQUESTS_MEMORY = "16Gi"


def approved_ceiling() -> str:
    """The canonical ``--approve-live`` string, derived from the constants above."""

    return ",".join(
        [
            f"sessions={MAX_SESSIONS}",
            f"turns={MAX_TURNS}",
            f"session-turns={SESSION_TURNS}",
            f"output-tokens={MAX_OUTPUT_TOKENS}",
            f"session-output-tokens={SESSION_OUTPUT_TOKENS}",
            f"owner-output-tokens={OWNER_OUTPUT_TOKENS}",
            f"session-seconds={SESSION_SECONDS}",
            f"live-seconds={LIVE_SECONDS}",
            f"cleanup-seconds={CLEANUP_SECONDS}",
            f"paid-retries={PAID_RETRIES}",
            f"probe-model={PROBE_MODEL}",
            f"probe-output-tokens={PROBE_OUTPUT_TOKENS}",
            f"probe-turns={PROBE_TURNS}",
            f"probe-seconds={PROBE_SECONDS}",
        ]
    )


def require_live_approval(text: str | None) -> None:
    """Refuse anything but the exact approved ceiling (``live_not_approved``)."""

    if text != approved_ceiling():
        raise pps.Refusal("live_not_approved", "--approve-live")


# Fix texts for this scenario's refusal codes. Codes the single-profile module
# already defines keep its text; only new codes are added, so its contract and
# its tests are unchanged.
FIXES: dict[str, str] = {
    "live_not_approved": "pass --approve-live with the exact two-owner ceiling string",
    "inputs_unsafe": "the inputs file must be a regular file you own, mode 0600, at most 64 KiB, "
    "not a symlink, in a directory no group or other user can write",
    "inputs_invalid": "fix the named inputs-file field; unknown keys are refused",
    "inline_grant_refused": "the inputs file names private files only; move the value into a "
    "private file and reference it",
    "setup_inputs_carry_grants": "a setup inputs file names no owners, grant files or "
    "independence assertion",
    "independence_unasserted": "set independent_accounts to true only when the two grants "
    "belong to independently enrolled accounts",
    "duplicate_owner_grant": "each owner needs its own grant file, distinct by path, inode "
    "and content",
    "session_plan_impossible": "the start manifest does not fit the approved ceiling",
    "group_reserved": "a reservation group is reserved once per run",
    "group_out_of_order": "reserve an owner's groups in manifest order",
    "owner_session_open": "settle the owner's open session before reserving its next group",
}
for _code, _fix in FIXES.items():
    pps.FIXES.setdefault(_code, _fix)

# --------------------------------------------------------------------------
# B. The private inputs file
# --------------------------------------------------------------------------

INPUTS_LIMIT = 64 * 1024
_TARGET_KEYS = ("context", "namespace", "release", "registry", "docker_context", "kubeconfig")
_TOP_KEYS = frozenset(
    {
        "target",
        "forge",
        "gateway",
        "owners",
        "independent_accounts",
        "prior_item_output_tokens",
        "probe_cli",
    }
)
_SCHEMA_KEYS: dict[str, frozenset[str]] = {
    "target": frozenset(_TARGET_KEYS),
    "forge": frozenset({"app_dir"}),
    "gateway": frozenset({"resolution_channel", "slack_bot_token_file"}),
    "owners": frozenset(OWNERS),
    "owner": frozenset({"grant_file", "usable_until"}),
}
# Key names that look like an inline credential. Only these file references
# (and the integer spend) may carry such a word.
_GRANT_KEY = re.compile(
    r"token|grant|secret|passw|credential|oauth|api[_-]?key|cookie|session|bearer|private",
    re.IGNORECASE,
)
_ALLOWED_GRANT_KEYS = frozenset({"grant_file", "slack_bot_token_file", "prior_item_output_tokens"})
# Token-shaped content: known credential prefixes, a bearer value, PEM, or a
# long unbroken run of token alphabet mixing upper case, lower case and digits.
_TOKEN_PREFIX = re.compile(
    r"sk-ant-|xox[a-z]-|gh[opsur]_|github_pat_|glpat-|AKIA[0-9A-Z]{12}|eyJ[A-Za-z0-9_-]{8,}\."
    r"|-----BEGIN|(?i:bearer)\s+\S"
)
_TOKEN_RUN = re.compile(r"[A-Za-z0-9_\-+=]{32,}")
_CONTEXT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,252}")
_PLAIN_VALUE = re.compile(r"[\x21-\x7e]{1,4096}")


def token_shaped(text: str) -> bool:
    """Whether a string holds credential-shaped content. Pure."""

    if _TOKEN_PREFIX.search(text):
        return True
    for match in _TOKEN_RUN.finditer(text):
        run = match.group(0)
        if (
            any(ch.isupper() for ch in run)
            and any(ch.islower() for ch in run)
            and any(ch.isdigit() for ch in run)
        ):
            return True
    return False


@dataclasses.dataclass(frozen=True, repr=False)
class Target:
    """The caller-owned cluster target. Names only; the repr hides them."""

    context: str
    namespace: str
    release: str
    registry: str
    registry_port: int
    docker_context: str
    kubeconfig: Path

    def __repr__(self) -> str:
        return "<target>"

    @property
    def grant_namespace(self) -> str:
        """The owned custody namespace the driver creates before helm."""

        return f"{self.namespace}-grants"


@dataclasses.dataclass(frozen=True, repr=False)
class Forge:
    """The owned forge fixture's GitHub App kit."""

    app_dir: Path
    app: Mapping[str, Any]

    def __repr__(self) -> str:
        return "<forge>"


@dataclasses.dataclass(frozen=True, repr=False)
class Gateway:
    """The channel gateway's bot token file and the route's card channel."""

    slack_bot_token_file: Path
    resolution_channel: str

    def __repr__(self) -> str:
        return "<gateway>"


@dataclasses.dataclass(frozen=True, repr=False)
class OwnerInput:
    """One owner's grant file reference and its caller-stated usable-until bound."""

    owner: str
    grant_file: Path
    usable_until: float

    def __repr__(self) -> str:
        return f"<owner {self.owner} input>"


@dataclasses.dataclass(frozen=True, repr=False)
class ScenarioInputs:
    """A validated inputs file. Never holds grant bytes; the repr hides every path.

    ``owners`` is empty in ``setup`` mode and holds A then B in ``live`` mode.
    """

    mode: Literal["setup", "live"]
    target: Target
    forge: Forge
    gateway: Gateway
    owners: tuple[OwnerInput, ...]
    independent_accounts: bool
    prior_item_output_tokens: int
    probe_cli: str

    def __repr__(self) -> str:
        return f"<scenario inputs mode={self.mode}>"

    def owner(self, name: str) -> OwnerInput:
        """One owner's input, or ``inputs_invalid`` when this file carries none."""

        for item in self.owners:
            if item.owner == name:
                return item
        raise pps.Refusal("inputs_invalid", f"owners.{name}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    keys = [key for key, _value in pairs]
    if len(set(keys)) != len(keys):
        raise pps.Refusal("inputs_invalid", "duplicate key")
    return dict(pairs)


def _read_inputs(path: Path) -> dict[str, Any]:
    """The inputs file's JSON object, after every file-safety check."""

    try:
        info = os.lstat(path)
        parent = os.stat(path.parent)
    except OSError:
        raise pps.Refusal("inputs_unsafe", "--inputs-file") from None
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_size > INPUTS_LIMIT
        or not stat.S_ISDIR(parent.st_mode)
        or parent.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
    ):
        raise pps.Refusal("inputs_unsafe", "--inputs-file")
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        raise pps.Refusal("inputs_unsafe", "--inputs-file") from None
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino) or (
            opened.st_size > INPUTS_LIMIT
        ):
            raise pps.Refusal("inputs_unsafe", "--inputs-file")
        data = os.read(descriptor, INPUTS_LIMIT + 1)
    finally:
        os.close(descriptor)
    if len(data) > INPUTS_LIMIT:
        raise pps.Refusal("inputs_unsafe", "--inputs-file")
    try:
        document = json.loads(data.decode("utf-8"), object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, ValueError):
        raise pps.Refusal("inputs_invalid", "--inputs-file is not one JSON object") from None
    if not isinstance(document, dict):
        raise pps.Refusal("inputs_invalid", "--inputs-file is not one JSON object")
    return document


def _scan_inline(node: Any, where: str) -> None:
    """Refuse a credential-like key or a token-shaped string anywhere."""

    if isinstance(node, dict):
        for key, value in node.items():
            here = f"{where}.{key}" if where else str(key)
            if token_shaped(str(key)) or (
                _GRANT_KEY.search(str(key)) and key not in _ALLOWED_GRANT_KEYS
            ):
                raise pps.Refusal("inline_grant_refused", here)
            _scan_inline(value, here)
    elif isinstance(node, list):
        for index, item in enumerate(node):
            _scan_inline(item, f"{where}[{index}]")
    elif isinstance(node, str) and token_shaped(node):
        raise pps.Refusal("inline_grant_refused", where)


def _has_key(node: Any, name: str) -> bool:
    if isinstance(node, dict):
        return name in node or any(_has_key(value, name) for value in node.values())
    if isinstance(node, list):
        return any(_has_key(item, name) for item in node)
    return False


def _refuse_unknown(document: Mapping[str, Any]) -> None:
    unknown = sorted(set(document) - _TOP_KEYS)
    if unknown:
        raise pps.Refusal("inputs_invalid", unknown[0])
    for section in ("target", "forge", "gateway"):
        value = document.get(section)
        if not isinstance(value, dict):
            raise pps.Refusal("inputs_invalid", section)
        extra = sorted(set(value) - _SCHEMA_KEYS[section])
        if extra:
            raise pps.Refusal("inputs_invalid", f"{section}.{extra[0]}")
    owners = document.get("owners")
    if owners is None:
        return
    if not isinstance(owners, dict):
        raise pps.Refusal("inputs_invalid", "owners")
    extra = sorted(set(owners) - _SCHEMA_KEYS["owners"])
    if extra:
        raise pps.Refusal("inputs_invalid", f"owners.{extra[0]}")
    for name, value in owners.items():
        if not isinstance(value, dict):
            raise pps.Refusal("inputs_invalid", f"owners.{name}")
        extra = sorted(set(value) - _SCHEMA_KEYS["owner"])
        if extra:
            raise pps.Refusal("inputs_invalid", f"owners.{name}.{extra[0]}")


def _string(section: Mapping[str, Any], key: str, where: str) -> str:
    value = section.get(key)
    if not isinstance(value, str) or not _PLAIN_VALUE.fullmatch(value):
        raise pps.Refusal("inputs_invalid", where)
    return value


def _absolute(section: Mapping[str, Any], key: str, where: str) -> Path:
    path = Path(_string(section, key, where))
    if not path.is_absolute():
        raise pps.Refusal("inputs_invalid", f"{where} must be absolute")
    return path


@dataclasses.dataclass
class _Relabel:
    """Re-raise a validator's refusal with the inputs-file field as its detail."""

    where: str

    def __enter__(self) -> None:
        return None

    def __exit__(self, kind: Any, error: Any, _trace: Any) -> Literal[False]:
        if isinstance(error, pps.Refusal):
            code = "inputs_invalid" if error.code == "arguments_invalid" else error.code
            raise pps.Refusal(code, self.where) from None
        return False


def _target(section: Mapping[str, Any]) -> Target:
    context = _string(section, "context", "target.context")
    if not _CONTEXT.fullmatch(context):
        raise pps.Refusal("inputs_invalid", "target.context")
    with _Relabel("target.namespace"):
        # Room for the owned ``<namespace>-grants`` custody namespace.
        namespace = pps.require_dns_name(
            _string(section, "namespace", "target.namespace"), "", limit=56
        )
    with _Relabel("target.release"):
        release = pps.require_release(_string(section, "release", "target.release"))
    registry = _string(section, "registry", "target.registry")
    with _Relabel("target.registry"):
        _address, port = pps.require_registry(registry)
    docker_context = _string(section, "docker_context", "target.docker_context")
    kubeconfig = _absolute(section, "kubeconfig", "target.kubeconfig")
    if not kubeconfig.is_file():
        raise pps.Refusal("inputs_invalid", "target.kubeconfig")
    return Target(
        context=context,
        namespace=namespace,
        release=release,
        registry=f"localhost:{port}",
        registry_port=port,
        docker_context=docker_context,
        kubeconfig=kubeconfig,
    )


def _grant_identity(path: Path, where: str) -> tuple[Path, tuple[int, int], bytes]:
    """Resolved path, (st_dev, st_ino) and an in-memory content digest of one grant."""

    with _Relabel(where):
        pps.check_grant_file(path)
        held = pps.load_grant(path)
    try:
        digest = hashlib.sha256(held.reveal().encode()).digest()
    finally:
        held.clear()
    info = os.stat(path)
    return path.resolve(), (info.st_dev, info.st_ino), digest


def _usable_until(value: Any, where: str) -> float:
    if type(value) is int and value > 0:
        text = str(value)
    elif isinstance(value, str):
        text = value
    else:
        raise pps.Refusal("expiry_unknown", where)
    with _Relabel(where):
        return pps.parse_expiry(text)


def load_inputs_file(path: Path, *, mode: Literal["setup", "live"], now: float) -> ScenarioInputs:
    """Load and validate the private JSON inputs descriptor.

    File safety (``inputs_unsafe``): a regular file (lstat, so never a
    symlink), owned by the current uid, mode exactly 0600, at most 64 KiB, in a
    directory no group or other user can write. Content: a credential-like key
    or token-shaped string anywhere is ``inline_grant_refused``; an unknown key
    at any level, a duplicate key or a missing field is ``inputs_invalid``.
    Every path is absolute.

    ``setup`` mode refuses ``owners``, ``independent_accounts`` and any
    ``grant_file`` (``setup_inputs_carry_grants``). ``live`` mode requires
    ``independent_accounts`` to be exactly ``true``
    (``independence_unasserted``), both owners with grant files distinct by
    resolved path, inode and content digest (``duplicate_owner_grant``; the
    digests are compared in memory and dropped), each ``usable_until`` at least
    LIVE_SECONDS + CLEANUP_SECONDS after ``now`` (``lifetime_insufficient``),
    and an integer ``prior_item_output_tokens`` from 0 to MAX_OUTPUT_TOKENS.
    """

    if mode not in ("setup", "live"):
        raise pps.Refusal("arguments_invalid", "mode")
    document = _read_inputs(path)
    _scan_inline(document, "")
    if mode == "setup" and (
        "owners" in document
        or "independent_accounts" in document
        or _has_key(document, "grant_file")
    ):
        raise pps.Refusal("setup_inputs_carry_grants", "--inputs-file")
    _refuse_unknown(document)
    target = _target(document["target"])
    app_dir = _absolute(document["forge"], "app_dir", "forge.app_dir")
    with _Relabel("forge.app_dir"):
        app = pps.require_app_kit(app_dir)
    gateway_section = document["gateway"]
    bot_file = _absolute(gateway_section, "slack_bot_token_file", "gateway.slack_bot_token_file")
    channel = _string(gateway_section, "resolution_channel", "gateway.resolution_channel")
    with _Relabel("gateway"):
        token_path, channel = pps.require_slack_kit(str(bot_file), channel)
    probe_cli = document.get("probe_cli", "claude")
    if not isinstance(probe_cli, str) or not _PLAIN_VALUE.fullmatch(probe_cli):
        raise pps.Refusal("inputs_invalid", "probe_cli")
    prior = document.get("prior_item_output_tokens", 0 if mode == "setup" else None)
    if type(prior) is not int or not 0 <= prior <= MAX_OUTPUT_TOKENS:
        raise pps.Refusal("inputs_invalid", "prior_item_output_tokens")
    owners: list[OwnerInput] = []
    independent = False
    if mode == "live":
        if document.get("independent_accounts") is not True:
            raise pps.Refusal("independence_unasserted", "independent_accounts")
        independent = True
        section = document.get("owners")
        if not isinstance(section, dict) or set(section) != set(OWNERS):
            raise pps.Refusal("inputs_invalid", "owners")
        identities = []
        for name in OWNERS:
            entry = section[name]
            if set(entry) != _SCHEMA_KEYS["owner"]:
                raise pps.Refusal("inputs_invalid", f"owners.{name}")
            grant = _absolute(entry, "grant_file", f"owners.{name}.grant_file")
            identities.append(_grant_identity(grant, f"owners.{name}.grant_file"))
            usable = _usable_until(entry.get("usable_until"), f"owners.{name}.usable_until")
            owners.append(OwnerInput(name, grant, usable))
        first, second = identities
        if any(left == right for left, right in zip(first, second, strict=True)):
            raise pps.Refusal("duplicate_owner_grant", "owners")
        identities.clear()
        for item in owners:
            if item.usable_until - now < GRANT_MIN_LIFETIME:
                raise pps.Refusal("lifetime_insufficient", f"owners.{item.owner}.usable_until")
    return ScenarioInputs(
        mode=mode,
        target=target,
        forge=Forge(app_dir, dict(app)),
        gateway=Gateway(token_path, channel),
        owners=tuple(owners),
        independent_accounts=independent,
        prior_item_output_tokens=prior,
        probe_cli=probe_cli,
    )


# --------------------------------------------------------------------------
# C. Start manifest, production transitions and the schedule check
# --------------------------------------------------------------------------

PROBE = "probe"
IMPLEMENTER = pps.IMPLEMENTER
PLAN_REVIEWER = pps.PLAN_REVIEWER
DIFF_REVIEWER = pps.DIFF_REVIEWER
ROLES = (PROBE, IMPLEMENTER, PLAN_REVIEWER, DIFF_REVIEWER)
#: Group kinds: how the planning model prices the group's sessions.
KINDS = ("probe", "attempt", "follow-up", "interrupted", "repair")


@dataclasses.dataclass(frozen=True)
class Group:
    """One reservation group: reserved and persisted before the native start it covers.

    ``kind`` prices the group in the planning model: a ``probe`` reserves the
    fixed probe limits, an ``interrupted`` session is held at its prompt and
    spends nothing, and a ``repair`` implementer is priced as the repair.
    """

    name: str
    owner: str
    roles: tuple[str, ...]
    trigger: str
    kind: str = "attempt"


#: The 16 starts, in the order the live timeline reserves them. Each owner's
#: groups keep the plan table's order.
MANIFEST: tuple[Group, ...] = (
    Group("a-probe", "a", (PROBE,), "driver grant probe, empty HOME, before A1", "probe"),
    Group(
        "a1",
        "a",
        (IMPLEMENTER, PLAN_REVIEWER, DIFF_REVIEWER),
        "trusted admission, execute turn; reviewers via Agent",
    ),
    Group("b-probe", "b", (PROBE,), "driver grant probe, empty HOME, before B1", "probe"),
    Group(
        "b1",
        "b",
        (IMPLEMENTER, PLAN_REVIEWER, DIFF_REVIEWER),
        "trusted admission, execute turn",
    ),
    Group(
        "a2",
        "a",
        (IMPLEMENTER, PLAN_REVIEWER, DIFF_REVIEWER),
        "admission of the fresh successor after rejection",
    ),
    Group(
        "b-r1",
        "b",
        (IMPLEMENTER,),
        "factory_ci round-2 continuation consumed after worker restart; held at its prompt",
        "interrupted",
    ),
    Group(
        "a3",
        "a",
        (IMPLEMENTER,),
        "admission of the authorized follow-up; ended by disconnect",
        "follow-up",
    ),
    Group(
        "b-r2",
        "b",
        (IMPLEMENTER, DIFF_REVIEWER, DIFF_REVIEWER),
        "kernel runner-error retry on the same request and epoch; the second diff reviewer "
        "only if the first returns changes",
        "repair",
    ),
)
GROUPS: dict[str, Group] = {group.name: group for group in MANIFEST}
#: Groups still open (reserved, not settled) when a group is reserved: B-r1 is
#: held at its prompt while A3 is reserved (timeline step 9), so A3's
#: reservation charges it at its reserved share. B-r1 settles at zero from its
#: prompt-hold harvest before B-r2 is reserved.
OPEN_DURING: dict[str, tuple[str, ...]] = {"a3": ("b-r1",)}

#: Planning model, output tokens per session (plan section "Start manifest").
PLANNING_USAGE: dict[str, int] = {
    IMPLEMENTER: 2700,
    PLAN_REVIEWER: 1000,
    DIFF_REVIEWER: 800,
    "repair-implementer": 2700,
    "interrupted": 0,
    PROBE: 32,
}
#: Planning model, assistant turns per session. The plan gives tokens only; these
#: turn numbers are this module's assumption, recorded as such.
PLANNING_TURNS: dict[str, int] = {
    IMPLEMENTER: 20,
    PLAN_REVIEWER: 6,
    DIFF_REVIEWER: 6,
    "repair-implementer": 20,
    "interrupted": 0,
    PROBE: 1,
}

STARTS_NOTHING = "starts-nothing"
FENCED = "fenced"
#: Every production transition the plan names, mapped to the group whose
#: native start it is, to STARTS_NOTHING, or to FENCED (refused or fenced: it
#: would be an unplanned start).
TRANSITIONS: dict[str, str] = {
    "driver grant probe for owner A": "a-probe",
    "trusted admission of A's first WorkItem, execute turn": "a1",
    "admission of A's fresh successor after the rejection": "a2",
    "admission of A's authorized follow-up": "a3",
    "driver grant probe for owner B": "b-probe",
    "trusted admission of B's WorkItem, execute turn": "b1",
    "factory_ci round-2 continuation consumed after the worker restart": "b-r1",
    "kernel runner-error retry on the same request and epoch": "b-r2",
    "kernel publish continuation of an execute turn (same native session)": STARTS_NOTHING,
    "reviewer subagent start inside a reserved group (Agent tool)": STARTS_NOTHING,
    "approval resolve (no-wake)": STARTS_NOTHING,
    "approval rejection": STARTS_NOTHING,
    "publication job": STARTS_NOTHING,
    "CI polling and rerun": STARTS_NOTHING,
    "owner-lost refusal while a publication is awaiting": STARTS_NOTHING,
    "runner pod deletion at a quiescent park": STARTS_NOTHING,
    "wrong-owner admission, replay and read controls": STARTS_NOTHING,
    "disconnect terminate": STARTS_NOTHING,
    "worker restart": STARTS_NOTHING,
    "owner-lost successor": FENCED,
    "round-3 repair after B's revision-2 publication succeeds": FENCED,
    "unpublished continuation of a ci turn": FENCED,
    "session the harvest cannot map to a reservation": FENCED,
}
#: The transitions the schedule check requires to be mapped.
PRODUCTION_TRANSITIONS: tuple[str, ...] = tuple(TRANSITIONS)


def planned_key(group: Group, role: str) -> str:
    """The planning-model key of one session of ``group``."""

    if group.kind == "probe":
        return PROBE
    if group.kind == "interrupted":
        return "interrupted"
    if group.kind == "repair" and role == IMPLEMENTER:
        return "repair-implementer"
    return role


def _impossible(reason: str) -> pps.Refusal:
    return pps.Refusal("session_plan_impossible", reason)


def schedule_check(
    prior_item_output_tokens: int,
    *,
    manifest: Sequence[Group] = MANIFEST,
    transitions: Mapping[str, str] = TRANSITIONS,
    usage: Mapping[str, int] = PLANNING_USAGE,
    turns: Mapping[str, int] = PLANNING_TURNS,
    open_during: Mapping[str, Sequence[str]] = OPEN_DURING,
    required_transitions: Sequence[str] = PRODUCTION_TRANSITIONS,
) -> dict[str, Any]:
    """G-SESSION-PLAN: simulate the manifest under the share rule; unpaid and pure.

    Each group's per-session share is ``min(8192, min(owner_left,
    aggregate_left) // sessions)`` and its turn share ``min(32, (192 - used) //
    sessions)``, where earlier groups count at their planned usage, or at their
    reserved share while listed in ``open_during``. A probe reserves the probe
    limits. Raises ``session_plan_impossible`` (with the reason) when the
    sessions are not 16, a transition is unmapped, a group has no single start
    transition, an implementer-bearing share is under 2700, another share is
    under 1000, a turn share is under 16, an owner's planned total is over
    16384, the aggregate is over 32768 minus the prior spend, or a planned
    session exceeds its share. Returns the per-group shares and the totals.
    """

    if type(prior_item_output_tokens) is not int or not (
        0 <= prior_item_output_tokens <= MAX_OUTPUT_TOKENS
    ):
        raise _impossible("prior spend outside 0..32768")
    names = [group.name for group in manifest]
    if len(set(names)) != len(names):
        raise _impossible("duplicate group name")
    sessions = sum(len(group.roles) for group in manifest)
    if sessions != MAX_SESSIONS:
        raise _impossible(f"manifest has {sessions} starts, not {MAX_SESSIONS}")
    targets = {*names, STARTS_NOTHING, FENCED}
    for transition in required_transitions:
        if transition not in transitions:
            raise _impossible(f"transition unmapped: {transition}")
    for transition, target in transitions.items():
        if target not in targets:
            raise _impossible(f"transition unmapped: {transition}")
    started = list(transitions.values())
    for name in names:
        if started.count(name) != 1:
            raise _impossible(f"group {name} needs exactly one start transition")
    for group in manifest:
        if group.owner not in OWNERS or group.kind not in KINDS or not group.roles:
            raise _impossible(f"group {group.name} is malformed")
        if any(role not in ROLES for role in group.roles):
            raise _impossible(f"group {group.name} names an unknown role")
        if (group.kind == "probe") != (group.roles == (PROBE,)):
            raise _impossible(f"group {group.name} mixes probe and role sessions")
    positions = {name: index for index, name in enumerate(names)}
    for name, open_names in open_during.items():
        for other in open_names:
            if (
                other not in positions
                or name not in positions
                or positions[other] >= positions[name]
            ):
                raise _impossible(f"open group {other} is not reserved before {name}")
            if groups_of(manifest)[other].owner == groups_of(manifest)[name].owner:
                raise _impossible(f"open group {other} has the same owner as {name}")

    def planned(group: Group, role: str, table: Mapping[str, int]) -> int:
        value = table.get(planned_key(group, role))
        if type(value) is not int or value < 0:
            raise _impossible(f"no planning value for {planned_key(group, role)}")
        return value

    reserved: dict[str, tuple[int, int]] = {}
    rows: list[dict[str, Any]] = []
    for group in manifest:
        held = set(open_during.get(group.name, ()))
        owner_used = aggregate_used = turns_used = 0
        for earlier in manifest[: positions[group.name]]:
            share, turn_share = reserved[earlier.name]
            if earlier.name in held:
                tokens = share * len(earlier.roles)
                spent_turns = turn_share * len(earlier.roles)
            else:
                tokens = sum(planned(earlier, role, usage) for role in earlier.roles)
                spent_turns = sum(planned(earlier, role, turns) for role in earlier.roles)
            aggregate_used += tokens
            turns_used += spent_turns
            if earlier.owner == group.owner:
                owner_used += tokens
        count = len(group.roles)
        owner_left = OWNER_OUTPUT_TOKENS - owner_used
        aggregate_left = MAX_OUTPUT_TOKENS - prior_item_output_tokens - aggregate_used
        if group.kind == "probe":
            share, turn_share = PROBE_OUTPUT_TOKENS, PROBE_TURNS
            if min(owner_left, aggregate_left) < share * count:
                raise _impossible(f"{group.name}: no room for the probe")
        else:
            share = min(SESSION_OUTPUT_TOKENS, max(0, min(owner_left, aggregate_left)) // count)
            turn_share = min(SESSION_TURNS, max(0, MAX_TURNS - turns_used) // count)
            floor = MIN_IMPLEMENTER_SHARE if IMPLEMENTER in group.roles else MIN_SHARE
            if share < floor:
                raise _impossible(f"{group.name}: share {share} is under {floor}")
            if turn_share < MIN_TURN_SHARE:
                raise _impossible(f"{group.name}: turn share {turn_share} is under 16")
        reserved[group.name] = (share, turn_share)
        rows.append(
            {
                "name": group.name,
                "owner": group.owner,
                "sessions": count,
                "share": share,
                "turn_share": turn_share,
                "planned_output_tokens": sum(planned(group, role, usage) for role in group.roles),
                "planned_turns": sum(planned(group, role, turns) for role in group.roles),
                "owner_left": owner_left,
                "aggregate_left": aggregate_left,
            }
        )
    owner_totals = {
        owner: sum(row["planned_output_tokens"] for row in rows if row["owner"] == owner)
        for owner in OWNERS
    }
    for owner, total in owner_totals.items():
        if total > OWNER_OUTPUT_TOKENS:
            raise _impossible(f"owner {owner} plans {total} output tokens, over 16384")
    aggregate = sum(owner_totals.values())
    limit = MAX_OUTPUT_TOKENS - prior_item_output_tokens
    if aggregate > limit:
        raise _impossible(f"the aggregate plans {aggregate} output tokens, over {limit}")
    total_turns = sum(row["planned_turns"] for row in rows)
    if total_turns > MAX_TURNS:
        raise _impossible(f"the plan needs {total_turns} turns, over {MAX_TURNS}")
    for group, row in zip(manifest, rows, strict=True):
        for role in group.roles:
            if planned(group, role, usage) > row["share"]:
                raise _impossible(f"{group.name}: a planned {role} session exceeds its share")
            if planned(group, role, turns) > row["turn_share"]:
                raise _impossible(f"{group.name}: a planned {role} session exceeds its turns")
    return {
        "verdict": "pass",
        "sessions": sessions,
        "groups": rows,
        "owner_totals": owner_totals,
        "aggregate": aggregate,
        "prior_item_output_tokens": prior_item_output_tokens,
        "aggregate_limit": limit,
        "planned_turns": total_turns,
        "transitions": len(transitions),
    }


def groups_of(manifest: Sequence[Group]) -> dict[str, Group]:
    """Name to group for a (possibly test-supplied) manifest."""

    return {group.name: group for group in manifest}


def group_index(name: str) -> int:
    """The 1-based manifest position, stored as the session's ``attempt``."""

    if name not in GROUPS:
        raise pps.Refusal("arguments_invalid", "group")
    return [group.name for group in MANIFEST].index(name) + 1


def group_of_attempt(attempt: int) -> Group | None:
    """The group a session's ``attempt`` column names, or None."""

    return MANIFEST[attempt - 1] if 1 <= attempt <= len(MANIFEST) else None


def subagent_limits(group_name: str) -> dict[str, int]:
    """Per-pod reviewer starts the gate admits for one group, from its roles.

    Three-role attempts admit one plan and one diff reviewer; A3, B-r1 and the
    probes admit none; B-r2 admits two diff reviewers.
    """

    group = GROUPS.get(group_name)
    if group is None:
        raise pps.Refusal("arguments_invalid", "group")
    return {"plan": group.roles.count(PLAN_REVIEWER), "diff": group.roles.count(DIFF_REVIEWER)}


# --------------------------------------------------------------------------
# D. The owner-aware ledger
# --------------------------------------------------------------------------

_GROUP_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS session_groups (run_id TEXT NOT NULL, seq INTEGER NOT NULL, "
    "owner TEXT NOT NULL, grp TEXT NOT NULL, PRIMARY KEY (run_id, seq))"
)
PRIOR_SPEND_KEY = "prior_item_output_tokens"
UNMAPPED = "unmapped"


def _window_key(owner: str) -> str:
    return f"usable_until_{owner}"


class TwoOwnerLedger(pps.Ledger):
    """The single-profile ledger plus an owner and group per session.

    Every write stays one ``BEGIN IMMEDIATE`` transaction. A session's charge
    is its settled actual usage; an unknown session is charged at its
    reserved share; an open (reserved, unsettled) session is charged at its
    reserved share, or at the live usage a caller supplies.
    """

    @classmethod
    def open(
        cls, state_dir: Path, run_id: str, *, scrubber: pps.Scrubber | None = None
    ) -> TwoOwnerLedger:
        """Open or create the private ledger and the ``session_groups`` table."""

        ledger = cast("TwoOwnerLedger", super().open(state_dir, run_id, scrubber=scrubber))
        with ledger.transaction() as db:
            db.execute(_GROUP_SCHEMA)
        return ledger

    @classmethod
    def open_existing(cls, state_dir: Path, run_id: str) -> TwoOwnerLedger:
        """Read-only open for ``judge``; refuses a ledger without owner groups."""

        ledger = cast("TwoOwnerLedger", super().open_existing(state_dir, run_id))
        row = ledger.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='session_groups'"
        ).fetchone()
        if row is None:
            ledger.close()
            raise pps.Refusal("ledger_missing", "not a two-owner ledger")
        return ledger

    # -- settings recorded once ------------------------------------------

    def _recorded(self, db: sqlite3.Connection, key: str) -> Any:
        row = db.execute(
            "SELECT value_json FROM observations WHERE run_id=? AND criterion='budget' AND key=?",
            (self.run_id, key),
        ).fetchone()
        return None if row is None else json.loads(row[0])

    def _write_once(self, db: sqlite3.Connection, key: str, value: Any) -> None:
        current = self._recorded(db, key)
        if current is not None:
            if current != value:
                raise pps.Refusal("arguments_invalid", f"{key} is already recorded")
            return
        db.execute(
            "INSERT INTO observations(run_id,criterion,key,value_json,recorded_at) "
            "VALUES(?,?,?,?,?)",
            (self.run_id, "budget", key, json.dumps(value), time.time()),
        )

    def set_prior_spend(self, tokens: int) -> None:
        """Record the inputs file's prior item spend before the first reservation."""

        if type(tokens) is not int or not 0 <= tokens <= MAX_OUTPUT_TOKENS:
            raise pps.Refusal("arguments_invalid", PRIOR_SPEND_KEY)
        with self.transaction() as db:
            if self._recorded(db, PRIOR_SPEND_KEY) is None and self._rows(db):
                raise pps.Refusal("arguments_invalid", "prior spend after a reservation")
            self._write_once(db, PRIOR_SPEND_KEY, tokens)

    def prior_spend(self) -> int:
        """The recorded prior item spend, 0 when none was recorded."""

        value = self._recorded(self.db, PRIOR_SPEND_KEY)
        if value is None:
            return 0
        if type(value) is not int or not 0 <= value <= MAX_OUTPUT_TOKENS:
            raise pps.Refusal("ledger_unsafe", PRIOR_SPEND_KEY)
        return value

    def set_owner_window(self, owner: str, usable_until: float) -> None:
        """Record one owner's stated usable-until bound (epoch seconds), once."""

        if owner not in OWNERS:
            raise pps.Refusal("arguments_invalid", "owner")
        if not isinstance(usable_until, (int, float)) or isinstance(usable_until, bool):
            raise pps.Refusal("arguments_invalid", "usable_until")
        if not math.isfinite(float(usable_until)) or usable_until <= 0:
            raise pps.Refusal("arguments_invalid", "usable_until")
        with self.transaction() as db:
            self._write_once(db, _window_key(owner), float(usable_until))

    def owner_window(self, owner: str) -> float | None:
        """One owner's recorded usable-until bound, or None."""

        value = self._recorded(self.db, _window_key(owner))
        return float(value) if isinstance(value, (int, float)) else None

    # -- rows and charges --------------------------------------------------

    def _rows(self, db: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
        cursor = (db or self.db).execute(
            "SELECT s.*, g.owner AS owner, g.grp AS grp FROM sessions s LEFT JOIN "
            "session_groups g ON g.run_id = s.run_id AND g.seq = s.seq "
            "WHERE s.run_id=? ORDER BY s.seq",
            (self.run_id,),
        )
        names = [column[0] for column in cursor.description]
        return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]

    def session_rows(self) -> list[dict[str, Any]]:
        """Every session with its ``owner`` and ``grp`` (group name)."""

        return self._rows()

    @staticmethod
    def _open(row: Mapping[str, Any]) -> bool:
        return row["source"] == "reserved" and row["output_tokens"] is None

    @staticmethod
    def _tokens(row: Mapping[str, Any], live: Mapping[int, int] | None) -> int:
        if row["source"] == "unknown":
            return int(row["reserved_output"])
        if row["output_tokens"] is not None:
            return int(row["output_tokens"])
        if row["source"] == "reserved":
            seen = (live or {}).get(int(row["seq"]))
            if type(seen) is int and seen >= 0:
                return seen
            return int(row["reserved_output"])
        if row["source"] == "unreserved":
            return SESSION_OUTPUT_TOKENS
        return int(row["reserved_output"])

    @staticmethod
    def _turns(row: Mapping[str, Any]) -> int:
        if row["source"] == "unknown":
            return int(row["reserved_turns"])
        if row["assistant_turns"] is not None:
            return int(row["assistant_turns"])
        if row["source"] == "unreserved":
            return SESSION_TURNS
        return int(row["reserved_turns"])

    def owner_of(self, seq: int) -> str | None:
        """The owner of one session, or None."""

        row = self.db.execute(
            "SELECT owner FROM session_groups WHERE run_id=? AND seq=?", (self.run_id, seq)
        ).fetchone()
        return None if row is None else str(row[0])

    def owner_spent(self, owner: str, *, live: Mapping[int, int] | None = None) -> int:
        """One owner's charged output tokens (settled, unknown at share, open at share or live)."""

        return sum(self._tokens(row, live) for row in self._rows() if row["owner"] == owner)

    def aggregate_spent(self, *, live: Mapping[int, int] | None = None) -> int:
        """Every session's charged output tokens plus the recorded prior item spend."""

        return sum(self._tokens(row, live) for row in self._rows()) + self.prior_spend()

    def reservations_of(self, name: str) -> list[pps.Reservation]:
        """The reservations persisted for one group, in seq order."""

        return [
            pps.Reservation(
                int(row["seq"]),
                int(row["attempt"]),
                str(row["role"]),
                int(row["reserved_output"]),
                int(row["reserved_turns"]),
                float(row["reserved_seconds"]),
            )
            for row in self._rows()
            if row["grp"] == name and row["source"] != "unreserved"
        ]

    # -- reservations --------------------------------------------------------

    def reserve(self, roles: Sequence[str], *, attempt: int, now: float) -> list[pps.Reservation]:
        """Refused: the two-owner run reserves whole manifest groups only."""

        raise pps.Refusal("arguments_invalid", "use reserve_group")

    def reserve_group(self, name: str, *, now: float) -> list[pps.Reservation]:
        """Persist one manifest group's reservations before its native start.

        One ``BEGIN IMMEDIATE`` transaction. Refuses an unknown group, a group
        already reserved (``group_reserved``), a group out of manifest order for
        its owner (``group_out_of_order``), a rehearsal ledger, a budget not
        ``ok``, no live start, any unknown usage (``usage_unknown``), an open
        session of the same owner (``owner_session_open``), more than 16
        sessions, too few turns, owner allocation or aggregate guard (prior
        spend included) under the planned minimum share, live seconds remaining
        under SESSION_SECONDS, and an owner's usable window shorter than the
        remaining live window (``lifetime_insufficient``). A probe group
        reserves PROBE_OUTPUT_TOKENS, PROBE_TURNS and PROBE_SECONDS.
        """

        group = GROUPS.get(name)
        if group is None:
            raise pps.Refusal("arguments_invalid", "group")
        with self.transaction() as db:
            run = db.execute(
                "SELECT budget_state, live_started_at FROM runs WHERE run_id=?", (self.run_id,)
            ).fetchone()
            if run is None:
                raise pps.Refusal("ledger_missing", "--run-id")
            budget_state, live_started_at = run
            if self._rehearsal(db):
                raise pps.Refusal("rehearsal_run_id", "--run-id")
            if budget_state != "ok":
                raise pps.Refusal("budget_exceeded")
            if live_started_at is None:
                raise pps.Refusal("allowance_exhausted", "live start not recorded")
            rows = self._rows(db)
            reserved = {row["grp"] for row in rows if row["source"] != "unreserved"}
            if name in reserved:
                raise pps.Refusal("group_reserved", name)
            for earlier in MANIFEST[: group_index(name) - 1]:
                if earlier.owner == group.owner and earlier.name not in reserved:
                    raise pps.Refusal("group_out_of_order", name)
            if any(row["source"] == "unknown" for row in rows):
                raise pps.Refusal("usage_unknown")
            if any(self._open(row) and row["owner"] == group.owner for row in rows):
                raise pps.Refusal("owner_session_open", name)
            count = len(group.roles)
            if len(rows) + count > MAX_SESSIONS:
                raise pps.Refusal("allowance_exhausted", "sessions")
            turns_used = sum(self._turns(row) for row in rows)
            owner_used = sum(self._tokens(row, None) for row in rows if row["owner"] == group.owner)
            aggregate_used = sum(self._tokens(row, None) for row in rows) + self._prior(db)
            owner_left = OWNER_OUTPUT_TOKENS - owner_used
            aggregate_left = self.output_token_ceiling() - aggregate_used
            if group.kind == "probe":
                tokens, turns, seconds = PROBE_OUTPUT_TOKENS, PROBE_TURNS, float(PROBE_SECONDS)
                if owner_left < tokens * count:
                    raise pps.Refusal("allowance_exhausted", "owner allocation")
                if aggregate_left < tokens * count:
                    raise pps.Refusal("allowance_exhausted", "aggregate guard")
                if MAX_TURNS - turns_used < turns * count:
                    raise pps.Refusal("allowance_exhausted", "turns")
            else:
                tokens = min(
                    SESSION_OUTPUT_TOKENS, max(0, min(owner_left, aggregate_left)) // count
                )
                turns = min(SESSION_TURNS, max(0, MAX_TURNS - turns_used) // count)
                seconds = float(SESSION_SECONDS)
                floor = MIN_IMPLEMENTER_SHARE if IMPLEMENTER in group.roles else MIN_SHARE
                if tokens < floor:
                    which = (
                        "owner allocation" if owner_left <= aggregate_left else "aggregate guard"
                    )
                    raise pps.Refusal("allowance_exhausted", which)
                if turns < MIN_TURN_SHARE:
                    raise pps.Refusal("allowance_exhausted", "turns")
            live_remaining = LIVE_SECONDS - (now - float(live_started_at))
            if live_remaining < SESSION_SECONDS:
                raise pps.Refusal("allowance_exhausted", "live seconds")
            window = self._recorded(db, _window_key(group.owner))
            if (
                not isinstance(window, (int, float))
                or float(window) - now < live_remaining + CLEANUP_SECONDS
            ):
                raise pps.Refusal("lifetime_insufficient", f"owner {group.owner} window")
            base = max((int(row["seq"]) for row in rows), default=0)
            attempt = group_index(name)
            reservations = []
            for offset, role in enumerate(group.roles, start=1):
                reservation = pps.Reservation(base + offset, attempt, role, tokens, turns, seconds)
                db.execute(
                    "INSERT INTO sessions(run_id,seq,attempt,role,reserved_output,"
                    "reserved_turns,reserved_seconds,started_at) VALUES(?,?,?,?,?,?,?,?)",
                    (self.run_id, reservation.seq, attempt, role, tokens, turns, seconds, now),
                )
                db.execute(
                    "INSERT INTO session_groups(run_id,seq,owner,grp) VALUES(?,?,?,?)",
                    (self.run_id, reservation.seq, group.owner, name),
                )
                reservations.append(reservation)
            return reservations

    def _prior(self, db: sqlite3.Connection) -> int:
        value = self._recorded(db, PRIOR_SPEND_KEY)
        return value if type(value) is int else 0

    # -- unreserved sessions and bounds ---------------------------------------

    def record_unreserved_session(
        self, owner: str, group: str, role: str, usage: pps.SessionUsage | None
    ) -> int:
        """An observed session no reservation covers: counted, and the budget is exceeded.

        ``owner`` is "a", "b" or "" when the harvest cannot attribute it.
        """

        if owner not in (*OWNERS, ""):
            raise pps.Refusal("arguments_invalid", "owner")
        attempt = group_index(group) if group in GROUPS else 0
        with self.transaction() as db:
            seq = 1 + max((int(row["seq"]) for row in self._rows(db)), default=0)
            db.execute(
                "INSERT INTO sessions(run_id,seq,attempt,role,reserved_output,reserved_turns,"
                "reserved_seconds,started_at,output_tokens,assistant_turns,elapsed,model,source) "
                "VALUES(?,?,?,?,0,0,0,?,?,?,?,?,'unreserved')",
                (
                    self.run_id,
                    seq,
                    attempt,
                    role,
                    0.0,
                    None if usage is None else usage.output_tokens,
                    None if usage is None else usage.assistant_turns,
                    None if usage is None else usage.elapsed,
                    None if usage is None else usage.model,
                ),
            )
            db.execute(
                "INSERT INTO session_groups(run_id,seq,owner,grp) VALUES(?,?,?,?)",
                (self.run_id, seq, owner, group or UNMAPPED),
            )
            self._mark_exceeded(db)
            return seq

    def record_unreserved(self, attempt: int, role: str, usage: pps.SessionUsage | None) -> int:
        """``pps.settle_attempt``'s hook: the attempt column names the group and owner."""

        group = group_of_attempt(attempt)
        if group is None:
            return self.record_unreserved_session("", UNMAPPED, role, usage)
        return self.record_unreserved_session(group.owner, group.name, role, usage)

    def _check_bounds(self, db: sqlite3.Connection) -> None:
        """Mark the budget exceeded past any per-session, owner, aggregate, count or turn bound."""

        rows = self._rows(db)
        actual = [row for row in rows if not self._open(row)]
        exceeded = len(rows) > MAX_SESSIONS
        for row in rows:
            if row["output_tokens"] is None:
                continue
            group = GROUPS.get(str(row["grp"]))
            probe = group is not None and group.kind == "probe"
            limits = (
                (PROBE_OUTPUT_TOKENS, PROBE_TURNS, PROBE_SECONDS)
                if probe
                else (SESSION_OUTPUT_TOKENS, SESSION_TURNS, SESSION_SECONDS)
            )
            if (
                int(row["output_tokens"]) > limits[0]
                or int(row["assistant_turns"] or 0) > limits[1]
                or float(row["elapsed"] or 0.0) > limits[2]
            ):
                exceeded = True
        for owner in OWNERS:
            spent = sum(self._tokens(row, None) for row in actual if row["owner"] == owner)
            exceeded |= spent > OWNER_OUTPUT_TOKENS
        aggregate = sum(self._tokens(row, None) for row in actual) + self._prior(db)
        exceeded |= aggregate > self.output_token_ceiling()
        exceeded |= sum(self._turns(row) for row in actual) > MAX_TURNS
        if exceeded:
            self._mark_exceeded(db)


# --------------------------------------------------------------------------
# E. The in-pod response gate
# --------------------------------------------------------------------------

GATE_DIR = "/tmp/pto-gate"
GATE_FILE = "two_owner_gate.py"
GATE_LIMITS_FILE = "limits.json"
GATE_MODES = ("prompt", "response", "stop", "subagent-stop")
#: Holds after a session's last response: releasing one asks the model for nothing more.
FINAL_MODES = frozenset({"stop", "subagent-stop"})
GATE_HEARTBEAT_SECONDS = 5
#: Hook events the gate is merged into: event, matcher, mode.
GATE_EVENTS: tuple[tuple[str, str | None, str], ...] = (
    ("UserPromptSubmit", None, "prompt"),
    ("PreToolUse", "*", "response"),
    ("Stop", None, "stop"),
    ("SubagentStop", None, "subagent-stop"),
)
GATE_KEY = re.compile(r"(prompt|response|stop|subagent-stop)-[0-9]+-[0-9]+")

# ``python3 ${CLAUDE_PLUGIN_ROOT}/hooks/two_owner_gate.py <mode>``. It finds the
# native CLI by walking its parents for a ``claude`` argv (the HOLD_SCRIPT idea
# in personal_owner_enforcement.py), writes one hold file keyed by its own pid
# and starttime, and waits for the release file with the same key. ``go``
# exits 0. A stop verdict or the hold limit fails closed: a prompt or a tool
# call exits 2 (blocked); a Stop or SubagentStop hook that exits 2 would make
# the model continue, so those modes print ``{"continue": false}`` and exit 0,
# which ends the session. An Agent or Task call first takes one per-pod
# reviewer slot (an O_EXCL file per slot), refused when limits.json is absent
# or the type's slots are spent. PTO_GATE_DIR relocates the state and
# PTO_GATE_HOLD_SECONDS may only shorten the hold (tests use both).
_GATE_HOOK_TEMPLATE = r"""
import json, os, pathlib, sys, threading, time
HOLD_SECONDS = @HOLD_SECONDS@
MODES = ("prompt", "response", "stop", "subagent-stop")
ROOT = pathlib.Path(os.environ.get("PTO_GATE_DIR") or "@GATE_DIR@")
mode = sys.argv[1] if len(sys.argv) > 1 else ""


def fail(reason):
    text = "two-owner gate: " + reason
    if mode in ("stop", "subagent-stop"):
        print(json.dumps({"continue": False, "stopReason": text}))
        raise SystemExit(0)
    sys.stderr.write(text + "\n")
    raise SystemExit(2)


def fields(pid):
    return pathlib.Path("/proc/%s/stat" % pid).read_text().rsplit(")", 1)[1].split()


def cli():
    pid = os.getppid()
    for _ in range(8):
        try:
            argv = pathlib.Path("/proc/%s/cmdline" % pid).read_bytes().split(b"\0")
            stat = fields(pid)
        except OSError:
            return None, None
        if any(b"claude" in part for part in argv[:2]):
            return pid, int(stat[19])
        pid = int(stat[1])
        if pid <= 1:
            return None, None
    return None, None


def size(path):
    if isinstance(path, str) and path:
        try:
            return os.stat(path).st_size
        except OSError:
            return None
    return None


def subagent_kind(tool_input):
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        import review_gate
    except Exception:
        review_gate = None
    if review_gate is not None:
        resolved = review_gate.reviewer_kind(dict(tool_input))
        return {review_gate.PLAN: "plan", review_gate.DIFF: "diff"}.get(resolved)
    named = str(tool_input.get("subagent_type") or "").rsplit(":", 1)[-1].strip()
    return {"plan-reviewer": "plan", "diff-reviewer": "diff"}.get(named)


def take_slot(event, key):
    try:
        limits = json.loads((ROOT / "@LIMITS_FILE@").read_text())
    except (OSError, ValueError):
        fail("subagent limits are not written for this pod")
    if not isinstance(limits, dict):
        fail("subagent limits are unreadable")
    tool_input = event.get("tool_input")
    kind = subagent_kind(tool_input if isinstance(tool_input, dict) else {})
    if kind is None:
        fail("only the reserved plan and diff reviewers may start")
    limit = limits.get(kind)
    if type(limit) is not int or limit < 0:
        fail("no " + kind + " reviewer is reserved for this pod")
    for slot in range(limit):
        path = ROOT / ("subagent-%s-%d" % (kind, slot))
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            continue
        with os.fdopen(descriptor, "w") as handle:
            handle.write(key)
        return kind
    fail("the reserved " + kind + " reviewer allowance for this pod is spent")


try:
    if mode not in MODES:
        fail("unknown gate mode")
    try:
        event = json.load(sys.stdin)
    except ValueError:
        event = None
    if not isinstance(event, dict):
        fail("unreadable hook input")
    ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    hook_pid = os.getpid()
    hook_start = int(fields(hook_pid)[19])
    key = "%s-%d-%d" % (mode, hook_pid, hook_start)
    tool = event.get("tool_name")
    kind = None
    if mode == "response" and tool in ("Agent", "Task"):
        kind = take_slot(event, key)
    cli_pid, cli_start = cli()
    record = {
        "key": key,
        "mode": mode,
        "tool_name": tool if isinstance(tool, str) else None,
        "subagent_kind": kind,
        "session_id": event.get("session_id"),
        "transcript_path": event.get("transcript_path"),
        "transcript_size": size(event.get("transcript_path")),
        "agent_id": event.get("agent_id"),
        "agent_transcript_path": event.get("agent_transcript_path"),
        "agent_transcript_size": size(event.get("agent_transcript_path")),
        "cli_pid": cli_pid,
        "cli_start": cli_start,
        "hook_pid": hook_pid,
        "hook_start": hook_start,
        "written_at": time.time(),
    }
    holding = ROOT / ("holding-%s.json" % key)
    release = ROOT / ("release-%s" % key)
    temporary = ROOT / (".holding-%s.tmp" % key)
    temporary.write_text(json.dumps(record, sort_keys=True))
    os.replace(temporary, holding)
    limit = HOLD_SECONDS
    override = os.environ.get("PTO_GATE_HOLD_SECONDS")
    if override:
        try:
            limit = min(limit, max(0.0, float(override)))
        except ValueError:
            pass
    deadline = time.monotonic() + limit
    pause = threading.Event()
    while not release.exists() and time.monotonic() < deadline:
        pause.wait(0.1)
    try:
        verdict = release.read_text().strip()
    except OSError:
        verdict = ""
    for path in (holding, release):
        try:
            path.unlink()
        except OSError:
            pass
    if verdict == "go":
        raise SystemExit(0)
    if verdict == "stop":
        fail("the acceptance driver stopped this session at its budget gate")
    fail("the acceptance driver did not release this hold in time")
except SystemExit:
    raise
except Exception:
    fail("gate state could not be read or written")
"""
GATE_HOOK_PROGRAM = (
    _GATE_HOOK_TEMPLATE.replace("@HOLD_SECONDS@", str(HOLD_SECONDS))
    .replace("@GATE_DIR@", GATE_DIR)
    .replace("@LIMITS_FILE@", GATE_LIMITS_FILE)
)

# Run by ``kubectl exec -i <pod> -c runner -- python3 -c GATE_STREAM_PROGRAM``.
# Emits one JSON line per new hold file ({"hold": <record>, "key"}), one per
# hold that disappeared ({"gone"}), a heartbeat every few seconds, and the
# outcome of every command. Applies ``{"release": <key>, "verdict": "go" |
# "stop"}`` and ``{"limits": {"plan": n, "diff": n}}`` lines from stdin with
# atomic writes, and exits on stdin EOF.
_GATE_STREAM_TEMPLATE = r"""
import json, os, pathlib, re, sys, threading, time
ROOT = pathlib.Path(os.environ.get("PTO_GATE_DIR") or "@GATE_DIR@")
KEY = re.compile(r"@KEY_PATTERN@")
HEARTBEAT = @HEARTBEAT@
lock = threading.Lock()
done = threading.Event()


def emit(item):
    with lock:
        sys.stdout.write(json.dumps(item, sort_keys=True) + "\n")
        sys.stdout.flush()


def atomic(path, text):
    temporary = path.with_name("." + path.name + ".tmp")
    with open(temporary, "w") as handle:
        handle.write(text)
    os.replace(temporary, path)


def apply(line):
    try:
        command = json.loads(line)
    except ValueError:
        command = None
    if not isinstance(command, dict):
        emit({"error": "unreadable command"})
        return
    if set(command) == {"release", "verdict"}:
        key, verdict = command["release"], command["verdict"]
        if not isinstance(key, str) or not KEY.fullmatch(key) or verdict not in ("go", "stop"):
            emit({"error": "invalid release"})
            return
        atomic(ROOT / ("release-" + key), verdict)
        emit({"applied": key, "verdict": verdict})
    elif set(command) == {"limits"}:
        limits = command["limits"]
        if not isinstance(limits, dict) or any(
            name not in ("plan", "diff") or type(count) is not int or count < 0
            for name, count in limits.items()
        ):
            emit({"error": "invalid limits"})
            return
        atomic(ROOT / "@LIMITS_FILE@", json.dumps(limits, sort_keys=True))
        emit({"limits_applied": limits})
    else:
        emit({"error": "unknown command"})


def reader():
    try:
        while True:
            line = sys.stdin.readline()
            if not line:
                break
            if line.strip():
                apply(line)
    finally:
        done.set()


ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
threading.Thread(target=reader, daemon=True).start()
emit({"ready": True})
seen = set()
last = 0.0
while not done.is_set():
    current = set()
    for path in sorted(ROOT.glob("holding-*.json")):
        key = path.name[len("holding-"):-len(".json")]
        if not KEY.fullmatch(key):
            continue
        current.add(key)
        if key in seen:
            continue
        try:
            record = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        seen.add(key)
        emit({"hold": record, "key": key})
    for key in sorted(seen - current):
        seen.discard(key)
        emit({"gone": key})
    now = time.monotonic()
    if now - last >= HEARTBEAT:
        last = now
        emit({"heartbeat": time.time(), "holds": sorted(current)})
    done.wait(0.1)
"""
GATE_STREAM_PROGRAM = (
    _GATE_STREAM_TEMPLATE.replace("@GATE_DIR@", GATE_DIR)
    .replace("@KEY_PATTERN@", GATE_KEY.pattern)
    .replace("@HEARTBEAT@", str(GATE_HEARTBEAT_SECONDS))
    .replace("@LIMITS_FILE@", GATE_LIMITS_FILE)
)


def gate_command(mode: str) -> str:
    """The hooks.json command for one gate mode."""

    if mode not in GATE_MODES:
        raise pps.Refusal("arguments_invalid", "gate mode")
    return f"python3 ${{CLAUDE_PLUGIN_ROOT}}/hooks/{GATE_FILE} {mode}"


def _commands(hooks: Mapping[str, Any]) -> Iterator[str]:
    events = hooks.get("hooks") if isinstance(hooks, Mapping) else None
    for entries in (events or {}).values():
        for entry in entries if isinstance(entries, list) else []:
            listed = entry.get("hooks") if isinstance(entry, dict) else None
            for hook in listed if isinstance(listed, list) else []:
                if isinstance(hook, dict) and isinstance(hook.get("command"), str):
                    yield hook["command"]


def two_owner_bundle_hooks(hooks: Mapping[str, Any]) -> dict[str, Any]:
    """Merge the four gate hooks (timeout HOOK_TIMEOUT) into a reservation-gated hooks.json.

    ``hooks`` must already carry ``pps.private_bundle_hooks``'s reservation
    gate and no two-owner gate; every existing hook is kept in place.
    """

    commands = list(_commands(hooks))
    if not any(pps.RESERVATION_GATE_FILE in command for command in commands):
        raise pps.Refusal("arguments_invalid", "hooks lack the reservation gate")
    if any(GATE_FILE in command for command in commands):
        raise pps.Refusal("arguments_invalid", "hooks already carry the two-owner gate")
    patched: dict[str, Any] = json.loads(json.dumps(hooks))
    events = patched.setdefault("hooks", {})
    for event, matcher, mode in GATE_EVENTS:
        entry: dict[str, Any] = {
            "hooks": [{"type": "command", "command": gate_command(mode), "timeout": HOOK_TIMEOUT}]
        }
        if matcher is not None:
            entry = {"matcher": matcher, **entry}
        events.setdefault(event, []).append(entry)
    return patched


def gate_decision(
    *,
    session_usage: pps.SessionUsage | None,
    reservation: pps.Reservation,
    owner_spent: int | None,
    aggregate_spent: int | None,
    session_started_at: float | None,
    now: float,
    final: bool,
) -> tuple[str, str]:
    """``("go", "")`` or ``("stop", reason)`` for one hold. Pure.

    ``session_usage`` is the session's usage read to the hold's reported
    transcript size; ``owner_spent`` and ``aggregate_spent`` (the latter with
    prior spend) include this session's live usage. ``session_started_at``
    and ``now`` are readings of one monotonic clock: the session's clock
    starts at the first observation of its native process and never stops.
    Unknown usage stops.

    Every approved ceiling is inclusive: usage equal to a ceiling is within
    it, usage above it is a breach. A ``final`` hold (Stop or SubagentStop)
    asks for no further response, so it passes at a ceiling and stops above
    one. Any other hold precedes one more response, so it stops once the
    share, a turn limit or the session's seconds are reached (the next
    response would cross them), and when one more response
    (RESPONSE_OUTPUT_TOKENS) could cross the per-session cap, the owner
    allocation, or the aggregate guard with one response in flight per owner.
    """

    if session_usage is None or type(owner_spent) is not int or type(aggregate_spent) is not int:
        return "stop", "usage_unknown"
    if not isinstance(session_started_at, (int, float)) or isinstance(session_started_at, bool):
        return "stop", "session_start_unknown"
    tokens, turns = session_usage.output_tokens, session_usage.assistant_turns
    if type(tokens) is not int or type(turns) is not int or tokens < 0 or turns < 0:
        return "stop", "usage_unknown"
    elapsed = now - float(session_started_at)
    seconds = min(float(reservation.seconds), float(SESSION_SECONDS))
    if final:
        if tokens > reservation.output_tokens:
            return "stop", "session_share"
        if tokens > SESSION_OUTPUT_TOKENS:
            return "stop", "session_output_tokens"
        if turns > reservation.turns or turns > SESSION_TURNS:
            return "stop", "session_turns"
        if elapsed > seconds:
            return "stop", "session_seconds"
        if owner_spent > OWNER_OUTPUT_TOKENS:
            return "stop", "owner_allocation"
        if aggregate_spent > MAX_OUTPUT_TOKENS:
            return "stop", "aggregate_guard"
        return "go", ""
    if tokens >= reservation.output_tokens:
        return "stop", "session_share"
    if tokens + RESPONSE_OUTPUT_TOKENS > SESSION_OUTPUT_TOKENS:
        return "stop", "session_output_tokens"
    if turns >= reservation.turns or turns >= SESSION_TURNS:
        return "stop", "session_turns"
    if elapsed >= seconds:
        return "stop", "session_seconds"
    if owner_spent + RESPONSE_OUTPUT_TOKENS > OWNER_OUTPUT_TOKENS:
        return "stop", "owner_allocation"
    if aggregate_spent + len(OWNERS) * RESPONSE_OUTPUT_TOKENS > MAX_OUTPUT_TOKENS:
        return "stop", "aggregate_guard"
    return "go", ""


def role_checks(summaries: Sequence[Mapping[str, Any]]) -> dict[str, bool]:
    """Role facts for one owner from ``pps.session_summary`` dicts. Pure.

    The driver records these under the ``roles`` keys with the owner suffix.
    """

    sessions = [item for item in summaries if isinstance(item, Mapping)]
    implementer, implementer_model = pps._role_ok(sessions, IMPLEMENTER, pps.IMPLEMENTER_MODEL)
    plan, plan_model = pps._role_ok(sessions, PLAN_REVIEWER, pps.REVIEWER_MODEL)
    diff, diff_model = pps._role_ok(sessions, DIFF_REVIEWER, pps.REVIEWER_MODEL)
    return {
        "implementer_session": implementer,
        "plan_reviewer_session": plan,
        "diff_reviewer_session": diff,
        "implementer_model": implementer_model,
        "reviewer_model": plan_model and diff_model,
        "no_synthetic_records": bool(sessions)
        and all(item.get("synthetic") == 0 for item in sessions),
    }


# --------------------------------------------------------------------------
# F. Install values, Dex and the installation-grant scan
# --------------------------------------------------------------------------

#: The Dex pin of tools/oidc-e2e/oidc_e2e.py (multi-arch index digest).
DEX_IMAGE = (
    "ghcr.io/dexidp/dex:v2.41.1"
    "@sha256:bc7cfce7c17f52864e2bb2a4dc1d2f86a41e3019f6d42e81d92a301fad0c8a1d"
)
DEX_PORT = 5556
DEX_CONFIG_KEY = "config.yaml"
OIDC_CLIENT_ID = "curie-console"
OIDC_SCOPES = "openid email profile"
#: Synthetic owner logins for the run's private Dex.
OWNER_EMAILS = {"a": "owner-a@example.com", "b": "owner-b@example.com"}
# The public bcrypt of "password" the repository's other Dex harnesses use
# (tools/oidc-e2e/dex.yaml.template). Dex is reachable only inside the release
# namespace and over the driver's port-forward; runner egress cannot reach it.
DEX_PASSWORD = "password"
DEX_PASSWORD_HASH = "$2a$10$2b2cU8CPhOTaGrs1HRQuAueS7JTT5ZHsHSzYiFPm1leZck7Mc8T4W"
STATE_COOKIE = "__Host-curie_oidc_state"
SESSION_COOKIE = "__Host-curie_console_session"
LOGIN_PATH = "/console/oidc/login"
CALLBACK_PATH = "/console/oidc/callback"
CLAIM_LABEL = "curietech.ai/sandbox-claim"
CUSTODY_LABEL = "curietech.ai/personal-custody"
CLAIM_TOKENS_SUFFIX = "-tokens"
MODEL_AUTH_MODE_ENV = "CURIE_MODEL_AUTH_MODE"
_FORBIDDEN_DEX_LABEL_PREFIXES = ("curietech.ai/", "sandbox.users.io/", "agents.x-k8s.io/")
_FORBIDDEN_DEX_LABELS = ("app.kubernetes.io/instance", "app.kubernetes.io/component")


def dex_issuer(name: str, namespace: str) -> str:
    """The in-cluster issuer of a Dex Service ``name`` in ``namespace``."""

    return f"http://{name}.{namespace}.svc.cluster.local:{DEX_PORT}/dex"


def api_redirect_uri(release: str, namespace: str) -> str:
    """The in-cluster callback URI registered in Dex; the driver rewrites its origin."""

    return (
        f"http://{pps.chart_fullname(release)}-api.{namespace}.svc.cluster.local:8000"
        f"{CALLBACK_PATH}"
    )


def _http_url(value: str, what: str) -> str:
    parts = urllib.parse.urlsplit(value)
    if parts.scheme not in ("http", "https") or not parts.hostname or value.endswith("/"):
        raise pps.Refusal("arguments_invalid", what)
    return value


def dex_config(
    *,
    issuer: str,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    users: Sequence[str],
    password_hash: str = DEX_PASSWORD_HASH,
) -> dict[str, Any]:
    """A Dex config: memory storage, one static client, one password user per email.

    User IDs are uuid5 of the email, so the two owners' subjects differ and are
    stable. ``issuer`` must equal ``CURIE_OIDC_ISSUER`` byte for byte.
    """

    _http_url(issuer, "issuer")
    _http_url(redirect_uri, "redirect_uri")
    emails = list(users)
    if (
        not emails
        or len(set(emails)) != len(emails)
        or not all(
            isinstance(email, str) and re.fullmatch(r"[a-z0-9.+-]+@[a-z0-9.-]+", email)
            for email in emails
        )
    ):
        raise pps.Refusal("arguments_invalid", "dex users")
    if not client_id or not client_secret:
        raise pps.Refusal("arguments_invalid", "dex client")
    return {
        "issuer": issuer,
        "storage": {"type": "memory"},
        "web": {"http": f"0.0.0.0:{DEX_PORT}"},
        "oauth2": {"skipApprovalScreen": True},
        "staticClients": [
            {
                "id": client_id,
                "name": "Curie console (two-owner acceptance)",
                "secret": client_secret,
                "redirectURIs": [redirect_uri],
            }
        ],
        "enablePasswordDB": True,
        "staticPasswords": [
            {
                "email": email,
                "username": email.split("@", 1)[0],
                "userID": str(uuid.uuid5(uuid.NAMESPACE_URL, email)),
                "hash": password_hash,
            }
            for email in emails
        ],
    }


def _require_dex_labels(labels: Mapping[str, str]) -> dict[str, str]:
    """Labels no chart selector matches: chart selectors all pair name with instance."""

    if not labels or not all(isinstance(k, str) and isinstance(v, str) for k, v in labels.items()):
        raise pps.Refusal("arguments_invalid", "dex labels")
    for key, value in labels.items():
        if key in _FORBIDDEN_DEX_LABELS or key.startswith(_FORBIDDEN_DEX_LABEL_PREFIXES):
            raise pps.Refusal("arguments_invalid", f"dex label {key}")
        if key == "app.kubernetes.io/name" and value == "curie":
            raise pps.Refusal("arguments_invalid", "dex label app.kubernetes.io/name")
    return dict(labels)


def dex_manifests(
    *,
    namespace: str,
    name: str,
    image_ref: str,
    config_secret: str,
    labels: Mapping[str, str],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """The Dex Secret (holding ``config``), Deployment and ClusterIP Service on 5556.

    ``labels`` select the Dex pod and must not match any chart selector (no
    ``app.kubernetes.io/instance`` or ``component``, no ``curie`` name, no
    claim or sandbox label), so no chart NetworkPolicy, rollout listing or
    quota scope applies to it.
    """

    pps.require_dns_name(namespace, "namespace", limit=63)
    pps.require_dns_name(name, "dex name", limit=40)
    pps.require_dns_name(config_secret, "dex config secret", limit=63)
    selected = _require_dex_labels(labels)
    if not image_ref or any(ch.isspace() for ch in image_ref):
        raise pps.Refusal("arguments_invalid", "dex image")
    metadata = {"name": name, "namespace": namespace, "labels": dict(selected)}
    return [
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": config_secret, "namespace": namespace, "labels": dict(selected)},
            "type": "Opaque",
            "stringData": {DEX_CONFIG_KEY: json.dumps(dict(config), sort_keys=True)},
        },
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": metadata,
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": dict(selected)},
                "template": {
                    "metadata": {"labels": dict(selected)},
                    "spec": {
                        "automountServiceAccountToken": False,
                        "enableServiceLinks": False,
                        "securityContext": {
                            "runAsNonRoot": True,
                            "runAsUser": 1001,
                            "seccompProfile": {"type": "RuntimeDefault"},
                        },
                        "containers": [
                            {
                                "name": "dex",
                                "image": image_ref,
                                "imagePullPolicy": "IfNotPresent",
                                "command": ["dex", "serve", f"/etc/dex/{DEX_CONFIG_KEY}"],
                                "ports": [{"name": "http", "containerPort": DEX_PORT}],
                                "readinessProbe": {"tcpSocket": {"port": DEX_PORT}},
                                "resources": {
                                    "requests": {"cpu": "50m", "memory": "64Mi"},
                                    "limits": {"cpu": "500m", "memory": "128Mi"},
                                },
                                "securityContext": {
                                    "allowPrivilegeEscalation": False,
                                    "readOnlyRootFilesystem": True,
                                    "capabilities": {"drop": ["ALL"]},
                                },
                                "volumeMounts": [
                                    {"name": "config", "mountPath": "/etc/dex", "readOnly": True},
                                    {"name": "tmp", "mountPath": "/tmp"},
                                ],
                            }
                        ],
                        "volumes": [
                            {"name": "config", "secret": {"secretName": config_secret}},
                            {"name": "tmp", "emptyDir": {}},
                        ],
                    },
                },
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": metadata,
            "spec": {
                "type": "ClusterIP",
                "selector": dict(selected),
                "ports": [{"name": "http", "port": DEX_PORT, "targetPort": DEX_PORT}],
            },
        },
    ]


def _env(name: str, value: str) -> dict[str, Any]:
    return {"name": name, "value": value}


_RUNNER_REPLACED = frozenset(
    {
        MODEL_AUTH_MODE_ENV,
        pps.PERSONAL_GRANT_ENV,
        "CURIE_MAX_TURNS",
        "CLAUDE_CODE_MAX_OUTPUT_TOKENS",
        "ENABLE_TOOL_SEARCH",
    }
)


def two_owner_install_values(
    base: Mapping[str, Any],
    *,
    release: str,
    namespace: str,
    grant_namespace: str,
    issuer: str,
    redirect_uri: str,
    client_id: str,
    client_secret_ref: tuple[str, str],
) -> dict[str, Any]:
    """Helm values for the two-owner install, from ``pps.install_values`` output.

    The runner loses CURIE_MODEL_AUTH_MODE and the release-wide grant
    reference (the worker delivers each owner's grant per claim) and keeps
    CURIE_MAX_TURNS=32 and CLAUDE_CODE_MAX_OUTPUT_TOKENS=RESPONSE_OUTPUT_TOKENS,
    with ENABLE_TOOL_SEARCH=false. ``personalClaude.grantNamespace`` turns on
    custody, FACTORY_MODEL_AUTH_MODE=personal is set on api and worker, the
    api gets the CURIE_OIDC_* settings (client secret by secretKeyRef
    ``client_secret_ref`` = (Secret name, key), admit-all true), the worker
    runs maxConcurrency 3 and the sandbox quota requests 16Gi. The result
    must pass ``installation_grant_findings``. ``base`` is not changed.
    """

    pps.require_release(release)
    pps.require_dns_name(namespace, "namespace", limit=63)
    pps.require_dns_name(grant_namespace, "grant namespace", limit=63)
    if grant_namespace == namespace:
        raise pps.Refusal("arguments_invalid", "grant namespace equals the release namespace")
    _http_url(issuer, "issuer")
    _http_url(redirect_uri, "redirect_uri")
    secret_name, secret_key = client_secret_ref
    pps.require_dns_name(secret_name, "client secret name", limit=63)
    if not client_id or not secret_key:
        raise pps.Refusal("arguments_invalid", "client")
    values: dict[str, Any] = copy.deepcopy(dict(base))

    runner = values.setdefault("agentSandbox", {}).setdefault("runner", {})
    runner["extraEnv"] = [
        entry
        for entry in runner.get("extraEnv") or []
        if not (isinstance(entry, dict) and entry.get("name") in _RUNNER_REPLACED)
    ] + [
        _env("CURIE_MAX_TURNS", str(SESSION_TURNS)),
        _env("CLAUDE_CODE_MAX_OUTPUT_TOKENS", str(RESPONSE_OUTPUT_TOKENS)),
        _env("ENABLE_TOOL_SEARCH", "false"),
    ]
    values.setdefault("personalClaude", {})["grantNamespace"] = grant_namespace

    def without(entries: Any, prefix: str = "") -> list[Any]:
        return [
            entry
            for entry in entries or []
            if not (
                isinstance(entry, dict)
                and (
                    entry.get("name") == "FACTORY_MODEL_AUTH_MODE"
                    or (prefix and str(entry.get("name") or "").startswith(prefix))
                )
            )
        ]

    api = values.setdefault("api", {})
    api["extraEnv"] = without(api.get("extraEnv"), "CURIE_OIDC_") + [
        _env("FACTORY_MODEL_AUTH_MODE", pps.PERSONAL_MODE),
        _env("CURIE_OIDC_ISSUER", issuer),
        _env("CURIE_OIDC_AUDIENCE", client_id),
        _env("CURIE_OIDC_JWKS_URL", f"{issuer}/keys"),
        {
            "name": "CURIE_OIDC_CLIENT_SECRET",
            "valueFrom": {"secretKeyRef": {"name": secret_name, "key": secret_key}},
        },
        _env("CURIE_OIDC_REDIRECT_URI", redirect_uri),
        _env("CURIE_OIDC_SCOPES", OIDC_SCOPES),
        _env("CURIE_OIDC_ADMIT_ALL_AUTHENTICATED", "true"),
    ]
    worker = values.setdefault("worker", {})
    worker["extraEnv"] = without(worker.get("extraEnv")) + [
        _env("FACTORY_MODEL_AUTH_MODE", pps.PERSONAL_MODE)
    ]
    worker["maxConcurrency"] = WORKER_MAX_CONCURRENCY
    worker["workItemMaxTurns"] = SESSION_TURNS
    values.setdefault("resourceQuota", {}).setdefault("hard", {})["requestsMemory"] = (
        SANDBOX_REQUESTS_MEMORY
    )
    findings = installation_grant_findings([values])
    if findings:
        raise pps.Refusal("deployed_service_credential", ", ".join(findings))
    return values


def _claim_of(node: Mapping[str, Any]) -> str | None:
    metadata = node.get("metadata")
    labels = metadata.get("labels") if isinstance(metadata, Mapping) else None
    claim = labels.get(CLAIM_LABEL) if isinstance(labels, Mapping) else None
    return claim if isinstance(claim, str) and claim else None


def _labels(node: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = node.get("metadata")
    labels = metadata.get("labels") if isinstance(metadata, Mapping) else None
    return labels if isinstance(labels, Mapping) else {}


def installation_grant_findings(documents: Iterable[Any]) -> list[str]:
    """Names of any installation-wide model credential or personal grant. Pure.

    Extends ``pps.deployed_credential_findings`` (service credentials,
    helpers, fallbacks, fake model). The only allowed grant reference is
    ``CLAUDE_CODE_OAUTH_TOKEN`` from ``<claim>-tokens`` key
    ``CLAUDE_CODE_OAUTH_TOKEN`` on the ``runner`` container of an object
    labelled with that claim, and CURIE_MODEL_AUTH_MODE=personal only there
    too. A Secret holding a ``CLAUDE_CODE_OAUTH_TOKEN`` key must be that
    claim's tokens Secret or a labelled custody Secret; a ``*-personal-grant``
    Secret or reference is always a finding.
    """

    items = list(documents)
    findings = set(pps.deployed_credential_findings(items, grant_secret=""))
    findings.discard(pps.PERSONAL_GRANT_ENV)

    def allowed(claim: str | None, container: str | None) -> bool:
        return claim is not None and container == pps.RUNNER_CONTAINER

    def env_entry(entry: Mapping[str, Any], claim: str | None, container: str | None) -> None:
        name = entry.get("name")
        source = entry.get("valueFrom")
        ref = source.get("secretKeyRef") if isinstance(source, Mapping) else None
        ref = ref if isinstance(ref, Mapping) else None
        per_claim = (
            name == pps.PERSONAL_GRANT_ENV
            and allowed(claim, container)
            and "value" not in entry
            and ref is not None
            and ref.get("name") == f"{claim}{CLAIM_TOKENS_SUFFIX}"
            and ref.get("key") == pps.PERSONAL_GRANT_ENV
        )
        if name == pps.PERSONAL_GRANT_ENV and not per_claim:
            findings.add(pps.PERSONAL_GRANT_ENV)
        if (
            ref is not None
            and not per_claim
            and (
                ref.get("key") == pps.PERSONAL_GRANT_ENV
                or str(ref.get("name") or "").endswith("-personal-grant")
            )
        ):
            findings.add("grant_secret_ref")
        if name == MODEL_AUTH_MODE_ENV and (
            source is not None or entry.get("value") == pps.PERSONAL_MODE
        ):
            if not (allowed(claim, container) and entry.get("value") == pps.PERSONAL_MODE):
                findings.add(MODEL_AUTH_MODE_ENV)

    def secret(node: Mapping[str, Any]) -> None:
        name = str((node.get("metadata") or {}).get("name") or "")
        keys = {
            key
            for field in ("data", "stringData")
            if isinstance(node.get(field), Mapping)
            for key in node[field]
        }
        if name.endswith("-personal-grant"):
            findings.add("grant_secret")
        if pps.PERSONAL_GRANT_ENV not in keys:
            return
        labels = _labels(node)
        claim = labels.get(CLAIM_LABEL)
        if isinstance(claim, str) and claim and name == f"{claim}{CLAIM_TOKENS_SUFFIX}":
            return
        if labels.get(CUSTODY_LABEL) == "true":
            return
        findings.add("grant_secret")

    def walk(node: Any, claim: str | None, container: str | None) -> None:
        if isinstance(node, Mapping):
            claim = _claim_of(node) or claim
            if node.get("kind") == "Secret":
                secret(node)
            # envFrom secretRef and secret volumes name a Secret without an env entry.
            source = node.get("secretRef")
            named = [
                node.get("secretName"),
                source.get("name") if isinstance(source, Mapping) else None,
            ]
            if any(isinstance(item, str) and item.endswith("-personal-grant") for item in named):
                findings.add("grant_secret_ref")
            is_entry = "name" in node and ("value" in node or "valueFrom" in node)
            if is_entry:
                env_entry(node, claim, container)
            elif isinstance(node.get("name"), str) and ("image" in node or "env" in node):
                container = node["name"]
            for value in node.values():
                walk(value, claim, container)
        elif isinstance(node, list):
            for item in node:
                walk(item, claim, container)

    for document in items:
        walk(document, None, None)
    return sorted(findings)


# --------------------------------------------------------------------------
# G. OIDC login with an injected opener
# --------------------------------------------------------------------------

#: ``opener(method, url, headers, body) -> (status, response headers, body)``.
#: It must not follow redirects. Response headers are (name, value) pairs, so
#: repeated Set-Cookie headers survive.
Opener = Callable[
    [str, str, Mapping[str, str], bytes | None], tuple[int, Sequence[tuple[str, str]], bytes]
]
LOGIN_HOPS = 12
_REDIRECTS = frozenset({301, 302, 303, 307, 308})
_CHOOSER = re.compile(r'href="(/dex/auth/local[^"]*)"')
_FORM_ACTION = re.compile(r'<form\b[^>]*\baction="([^"]*)"', re.IGNORECASE)


class LoginFailed(Exception):
    """An OIDC login step failed. Carries the step name only, never a value."""

    def __init__(self, step: str) -> None:
        super().__init__(step)
        self.step = step


def _origin(url: str) -> str:
    """``scheme://host:port`` with the default port made explicit, or "" when not http(s)."""

    parts = urllib.parse.urlsplit(url)
    scheme = parts.scheme.lower()
    host = (parts.hostname or "").lower()
    if scheme not in ("http", "https") or not host:
        return ""
    try:
        port = parts.port
    except ValueError:
        return ""
    if ":" in host:
        host = f"[{host}]"
    return f"{scheme}://{host}:{port or (443 if scheme == 'https' else 80)}"


def rewrite_origin(url: str, mapping: Mapping[str, str], *, base: str | None = None) -> str:
    """``url`` with its scheme, host and port replaced when its origin is a mapping key.

    A relative ``url`` is resolved against ``base`` (the current URL) first.
    Path, query and fragment are kept. Origins compare with default ports
    explicit; any other URL is returned resolved but unchanged.
    """

    absolute = urllib.parse.urljoin(base, url) if base else url
    targets = {_origin(key): value for key, value in mapping.items() if _origin(key)}
    origin = _origin(absolute)
    if not origin or origin not in targets:
        return absolute
    parts = urllib.parse.urlsplit(absolute)
    destination = urllib.parse.urlsplit(targets[origin])
    return urllib.parse.urlunsplit(
        (destination.scheme, destination.netloc, parts.path, parts.query, parts.fragment)
    )


def _header(headers: Sequence[tuple[str, str]], name: str) -> str | None:
    for key, value in headers:
        if key.lower() == name:
            return value
    return None


def _cookies(headers: Sequence[tuple[str, str]]) -> dict[str, str]:
    jar: dict[str, str] = {}
    for key, value in headers:
        if key.lower() != "set-cookie":
            continue
        parsed: http.cookies.SimpleCookie = http.cookies.SimpleCookie()
        try:
            parsed.load(value)
        except http.cookies.CookieError:
            continue
        for name, morsel in parsed.items():
            jar[name] = morsel.value
    return jar


def oidc_login(
    opener: Opener,
    *,
    api_origin: str,
    issuer_origin: str,
    dex_local_origin: str,
    redirect_origin: str,
    email: str,
    password: str,
) -> pps.HeldSecret:
    """A principal console session from a real Dex password login, held.

    The API half follows apps/api/src/curie_api/routers/console.py:
    ``GET /console/oidc/login`` answers 302 to the issuer's authorization
    endpoint and sets ``__Host-curie_oidc_state``; ``GET
    /console/oidc/callback`` with that cookie answers 303 and sets
    ``__Host-curie_console_session``. The Dex half follows
    tools/oidc-e2e/oidc_e2e.py ``begin_login``: Dex hops may be path-only or
    absolute on the issuer origin, a connector chooser may appear, and the
    password form posts ``login`` and ``password``. Every URL on the issuer
    origin is rewritten to ``dex_local_origin`` and the redirect origin to
    ``api_origin``; any other origin, and the password on any origin but the
    local Dex, is refused (``LoginFailed``).
    """

    origins = {
        "api": _origin(api_origin),
        "issuer": _origin(issuer_origin),
        "dex": _origin(dex_local_origin),
        "redirect": _origin(redirect_origin),
    }
    if not all(origins.values()) or origins["issuer"] in (origins["api"], origins["redirect"]):
        raise LoginFailed("origins")
    mapping = {issuer_origin: dex_local_origin, redirect_origin: api_origin}
    start = api_origin.rstrip("/") + LOGIN_PATH
    status, headers, _body = opener("GET", start, {}, None)
    state = _cookies(headers).get(STATE_COOKIE)
    location = _header(headers, "location")
    if status != 302 or not location:
        raise LoginFailed("login_start")
    if not state:
        raise LoginFailed("state_cookie")
    authorize = urllib.parse.urljoin(start, location)
    if _origin(authorize) != origins["issuer"]:
        raise LoginFailed("authorize_origin")
    current = rewrite_origin(authorize, mapping)
    jar: dict[str, str] = {}
    method, form = "GET", None
    for _hop in range(LOGIN_HOPS):
        if _origin(current) != origins["dex"]:
            raise LoginFailed("unexpected_origin")
        request: dict[str, str] = {}
        if jar:
            request["Cookie"] = "; ".join(f"{key}={value}" for key, value in jar.items())
        if form is not None:
            request["Content-Type"] = "application/x-www-form-urlencoded"
        status, headers, page = opener(method, current, request, form)
        jar.update(_cookies(headers))
        method, form = "GET", None
        if status in _REDIRECTS:
            location = _header(headers, "location")
            if not location:
                raise LoginFailed("dex_location")
            target = rewrite_origin(location, mapping, base=current)
            if (
                _origin(target) == origins["api"]
                and urllib.parse.urlsplit(target).path == CALLBACK_PATH
            ):
                return _finish_login(opener, target, state)
            current = target
            continue
        text = page.decode(errors="replace")
        if status == 200 and 'name="password"' in text:
            action = _FORM_ACTION.search(text)
            if action and action.group(1):
                current = rewrite_origin(html.unescape(action.group(1)), mapping, base=current)
            method = "POST"
            form = urllib.parse.urlencode({"login": email, "password": password}).encode()
            continue
        chooser = _CHOOSER.search(text)
        if status == 200 and chooser:
            current = rewrite_origin(html.unescape(chooser.group(1)), mapping, base=current)
            continue
        raise LoginFailed("dex_unexpected_response")
    raise LoginFailed("dex_hops_exhausted")


def _finish_login(opener: Opener, callback: str, state: str) -> pps.HeldSecret:
    status, headers, _body = opener("GET", callback, {"Cookie": f"{STATE_COOKIE}={state}"}, None)
    session = _cookies(headers).get(SESSION_COOKIE)
    if status not in (302, 303) or not session:
        raise LoginFailed("callback")
    return pps.HeldSecret(session)


# --------------------------------------------------------------------------
# H. Issue fixtures
# --------------------------------------------------------------------------

FIXTURES = HERE / "fixtures"
ISSUE_FIXTURES = {
    "a": FIXTURES / "personal-two-owner-a-issue.md",
    "b": FIXTURES / "personal-two-owner-b-issue.md",
}
_NONCE = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]{7,63}")
#: The exact test owner B's issue asks for. On a pull request (GITHUB_HEAD_REF
#: set) it fails until CI_ACK.md holds ``ci ack: <head ref>``; the head ref is
#: ``curie/publication-<lineage hex>``, unknown until the first publication,
#: so B's first published head fails in real CI and the repair adds the line.
CI_ACK_TEST = '''\
"""Acknowledgement check this repository's CI enforces on pull requests."""

import os
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class CiAckTest(unittest.TestCase):
    def test_ci_ack(self):
        ref = os.environ.get("GITHUB_HEAD_REF", "")
        if not ref:
            self.skipTest("GITHUB_HEAD_REF is not set; this check runs on pull requests")
        expected = "ci ack: " + ref
        path = ROOT / "CI_ACK.md"
        lines = path.read_text().splitlines() if path.is_file() else []
        if expected not in [line.strip() for line in lines]:
            message = "CI_ACK.md must contain the line '" + expected + "'"
            print("::error file=CI_ACK.md,line=1::" + message, flush=True)
            self.fail(message)


if __name__ == "__main__":
    unittest.main()
'''


def render_issue(owner: str, nonce: str) -> tuple[str, str]:
    """(title, body) of one owner's tracker issue; the body keeps its title line."""

    if owner not in ISSUE_FIXTURES:
        raise pps.Refusal("arguments_invalid", "owner")
    if not _NONCE.fullmatch(nonce or ""):
        raise pps.Refusal("arguments_invalid", "nonce")
    text = ISSUE_FIXTURES[owner].read_text()
    body = text.replace("{nonce}", nonce).replace("{ci_ack_test}", CI_ACK_TEST.rstrip("\n"))
    if "{nonce}" in body or "{ci_ack_test}" in body:
        raise pps.Refusal("arguments_invalid", "issue fixture placeholders")
    title = body.splitlines()[0].lstrip("# ").strip()
    return title, body


# --------------------------------------------------------------------------
# I. Evidence contract and judges
# --------------------------------------------------------------------------

CRITERIA = (
    "owners",
    "roles",
    "approval",
    "publication",
    "ci-repair",
    "restarts",
    "wrong-owner",
    "disconnect",
    "budget",
    "cleanup",
)
INTERRUPTIONS = {
    "pod": "A1's runner pod deleted at its quiescent card-1 park; A2 runs in a new pod",
    "worker": "the worker scaled to 0 after card 3's publication and back to 1; the new "
    "worker pod consumes B's round-2 repair",
    "process": "B-r1's native CLI killed and runner PID 1 SIGTERMed at its prompt hold; "
    "B-r2 is the kernel's retry in a fresh process",
}


def _owners(template: Mapping[str, str]) -> dict[str, str]:
    return {
        f"{key}_{owner}": meaning.replace("<owner>", owner.upper())
        for key, meaning in template.items()
        for owner in OWNERS
    }


def _restarts() -> dict[str, str]:
    template = {
        "lineage_before": "request and native session lineage recorded before the interruption",
        "marker_before": "the durable transcript conversation marker was present before",
        "old_runtime_absent": "the old runtime is observed absent: its pod UID, the old worker "
        "pod UIDs, or the held CLI after its container restarted or its pod went",
        "termination_epoch": "the epoch the old runtime held is confirmed: the WorkItem's "
        "binding moved past it (pod) or the request still binds it under the fresh runtime "
        "(worker, process); a terminal status alone never passes",
        "fresh_runtime": "a fresh runtime runs after (new pod UID, worker pod UID, or CLI "
        "pid plus starttime)",
        "same_work_item": "the WorkItem id is unchanged",
        "same_conversation": "the conversation (thread key) is unchanged",
        "owner_unchanged": "the immutable owner principal and tenant are unchanged",
        "generation_unchanged": "the authorized enrollment generation is unchanged",
        "grant_unchanged": "the new claim's delivery still matches the same owner's own grant",
        "marker_after": "the durable marker is in the fresh native session's own transcript "
        "(the product's history replay), not only in central storage",
        "committed_head_restored": "the workspace HEAD equals the branch's committed head on "
        "the owned forge, before (where a runner existed) and in the fresh runtime",
        "committed_files_restored": "every committed blob's SHA-256 equals the forge's before "
        "and after, and the fresh checkout holds them outside its uncommitted paths",
        "continued_native": "native execution continued after (a new native tool observation)",
        "usage_settled": "terminal capture settled the interrupted session's usage as known",
    }
    keys = {
        f"{kind}_{key}": f"{INTERRUPTIONS[kind].split(';')[0]}: {meaning}"
        for kind in INTERRUPTIONS
        for key, meaning in template.items()
    }
    keys.update(
        {
            "process_identity_matched": "the interrupt program matched the held CLI's pid and "
            "starttime (poes.process_interruption_checks)",
            "process_no_pod_delete": "the process interruption deleted no pod",
            "process_claim_still_bound": "B's request kept its claim through the interruption",
            "process_no_successor_claim": "no successor claim appeared for B",
            "process_same_request_epoch": "B-r2 ran on the same request and runtime epoch",
            "worker_candidate_image": "the replacement worker pod runs the candidate worker image",
            "worker_owner_lost_safe": "B's request stayed owner-lost-safe (publication "
            "succeeded) across the worker restart",
        }
    )
    return keys


def _restart_observations() -> dict[str, str]:
    """AC 5's observations (coordinator decision 2): recorded, never required."""

    keys = {
        f"{kind}_{key}": f"{INTERRUPTIONS[kind].split(';')[0]}: {meaning}"
        for kind in INTERRUPTIONS
        for key, meaning in {
            "marker_stored_after": "the marker's record is still in central storage after "
            "(storage, not the replayed history)",
            "git_status_after": "the fresh runtime's porcelain status before its first prompt "
            "release",
        }.items()
    }
    for kind in ("pod", "process"):
        keys[f"{kind}_git_status_before"] = "the interrupted runtime's porcelain status"
        keys[f"{kind}_uncommitted_restored"] = (
            "uncommitted edits in the fresh runtime (restored, not_restored or none_before); "
            "Not met: #5541, never claimed"
        )
    keys.update(
        {
            "process_recovery": "where B-r2 recovered: in_place (same pod, container "
            "restarted) or new_pod (a new pod UID)",
            "process_same_pod": "B-r2 ran in B-r1's pod",
            "process_container_restarted": "B-r1's runner container restarted in place",
            "pod_termination_recorded": "A1's request carries a recorded termination "
            "observation (a publication_denied request owes none)",
            "worker_runtime_owner_moved": "B's request names the new worker's consumer (the "
            "product rewrites it only at a start or a termination claim)",
        }
    )
    return keys


#: The observation keys each criterion requires, with what each one means. The
#: live driver records exactly these keys; the judge reads only them (plus the
#: ledger's own rows for ``budget`` and ``cleanup``).
EXPECTED: dict[str, dict[str, str]] = {
    "owners": {
        **_owners(
            {
                "principal_active": "<owner>'s /console/principal answers status active for its "
                "Dex login session",
                "tenant_is_agent_tenant": "<owner>'s principal tenant equals the agent's tenant",
                "enrollment_active_gen1": "<owner>'s subscription status is active at "
                "generation 1 after enroll",
                "work_item_owner": "every <owner> WorkItem names <owner>'s tenant, principal, "
                "generation 1 and personal mode",
                "request_owners": "every ExecutionRequest of <owner>'s WorkItems carries "
                "<owner>'s owner tuple",
                "cli_owner": "curie cluster work-items --json agrees with the API on <owner>'s "
                "owner tuple",
                "claim_labels": "every <owner> SandboxClaim carries <owner>'s owner-principal, "
                "owner-tenant, enrollment-generation and personal labels",
                "delivery_own_grant": "every <owner> <claim>-tokens Secret digest matches "
                "<owner>'s grant file",
                "delivery_not_other": "no <owner> <claim>-tokens Secret digest matches the other "
                "owner's grant file",
                "runner_grant_ref_per_claim": "every <owner> runner container reads its grant "
                "only from its own <claim>-tokens Secret",
                "runner_images_candidate": "every <owner> runner pod, replacements included, "
                "runs the candidate runner digest with the candidate revision label",
                "one_native_session": "at most one <owner> native process was observed at once",
            }
        ),
        "principal_ids_distinct": "A's and B's principal ids differ",
        "independence_asserted": "the live inputs file asserted independently enrolled accounts",
        "grant_files_distinct": "the grant files differ by resolved path, inode and digest",
        "custody_secret_count": "exactly two custody Secrets exist in the grant namespace "
        "after both enrollments",
        "custody_enrollments_distinct": "the two custody Secrets carry distinct enrollment "
        "labels at generation 1",
        "installation_grant_absent": "installation_grant_findings is empty for the rendered "
        "values and every live manifest",
        "service_credentials_absent": "no service credential, helper or fallback is deployed",
        "worker_images_candidate": "every worker pod, before and after each restart, runs the "
        "candidate worker digest and revision label",
        "platform_images_candidate": "the api and channel gateway pods run the candidate "
        "digests and revision labels",
    },
    "roles": {
        **_owners(
            {
                "implementer_session": "<owner> ran a non-synthetic implementer session",
                "plan_reviewer_session": "<owner> ran a real plan-reviewer subagent",
                "diff_reviewer_session": "<owner> ran a real diff-reviewer subagent",
                "implementer_model": "every <owner> implementer response reports the "
                "implementer model",
                "reviewer_model": "every <owner> reviewer response reports the reviewer model",
                "no_synthetic_records": "no <owner> session has a synthetic record",
                "aci_tool_observed": "<owner> made at least one mcp__curie__ ACI tool call",
                "bundle_hook_observed": "the bundle's review_gate hook ran in <owner>'s session",
            }
        ),
        "repair_implementer_b": "B's repair ran a native implementer on the implementer model",
        "repair_diff_reviewer_b": "B's repair round ran a real diff-reviewer subagent",
        "sessions_mapped": "every observed native session mapped to a reserved role of its "
        "owner's group",
    },
    "approval": {
        "route_explicit_users": "the publication route binds approvers.users to exactly the "
        "allowed operator",
        "principals_distinct": "the allowed operator, the unlisted operator and both owners "
        "are four distinct principals",
        "drill_cards_delivered": "both unpaid drill cards were delivered through the channel "
        "gateway after forge and before the first paid phase",
        "drill_unlisted_status": "the unlisted operator's resolve of the first drill card "
        "answered 403",
        "drill_unlisted_refused_by_list": "the first drill card stayed pending and its audit "
        "names the explicit users authorizer for that refusal",
        "drill_reject_resolved": "the allowed operator rejected the first drill card and the "
        "audit names it as operator",
        "drill_approve_resolved": "the allowed operator approved the second drill card and the "
        "audit names it as operator",
        "drill_started_nothing": "after the drill no runner claim of the run's agent and no "
        "reserved session existed",
        "drill_pushed_nothing": "the approved drill publication's API-reported branch and any "
        "pull request for it were absent on the forge",
        "card1_delivered": "A1's card (card 1) was delivered through the channel gateway",
        "card1_unlisted_status": "the unlisted operator's resolve of card 1 answered 403",
        "card1_pending_after_unlisted": "card 1 stayed pending after the unlisted attempt",
        "card1_unlisted_audit_denied": "card 1's audit records the denial by the explicit "
        "users authorizer",
        "card1_rejected_by_operator": "the allowed operator rejected card 1 and the audit names "
        "it as operator",
        "card2_delivered": "A2's card (card 2) was delivered",
        "card2_approved_by_operator": "the allowed operator approved card 2",
        "card3_delivered": "B1's card (card 3) was delivered",
        "card3_approved_by_operator": "the allowed operator approved card 3",
        "card4_delivered": "B's revision-2 repair card (card 4) was delivered",
        "card4_unlisted_status": "the unlisted operator's resolve of card 4 answered 403",
        "card4_pending_after_unlisted": "card 4 stayed pending after the unlisted attempt",
        "card4_approved_by_operator": "the allowed operator approved card 4",
        "no_push_before_approval": "no branch or pull request existed for any card before "
        "its approval",
        **_owners(
            {
                "owner_unchanged_after_resolution": "<owner>'s owner tuple is unchanged after "
                "every resolution of its cards",
                "grant_unchanged_after_resolution": "<owner>'s claim delivery still matches "
                "<owner>'s own grant after every resolution",
            }
        ),
        "approver_never_owner": "no resolution made the operator an execution owner",
    },
    "publication": {
        "rejected_no_branch": "card 1's rejected publication pushed no branch",
        "rejected_no_pr": "card 1's rejected publication opened no pull request",
        "rejection_stays_rejected": "card 1 stayed rejected after settle",
        "a2_publication_succeeded": "A2's publication succeeded",
        "a2_branch_from_api": "A2's curie/publication-<hex> branch was read from "
        "GET /publications/{id}",
        "a2_pr_ready": "A2's pull request is ready for review, not a draft",
        "a2_tree_matches_reviewed": "A2's pull request head tree equals the reviewed workspace",
        "a2_checks_passed": "A2's latest check runs and statuses all completed successfully",
        "a2_request_completed": "A2's request completed",
        "a2_work_item_published": "A's WorkItem is published",
        "b1_publication_succeeded": "B1's publication succeeded",
        "b1_branch_from_api": "B1's lineage branch was read from GET /publications/{id}",
        "b1_pr_ready": "B1's pull request is ready for review",
        "b1_tree_matches_reviewed": "B1's pull request head tree equals the reviewed workspace",
        "b2_publication_succeeded": "B's revision-2 publication succeeded",
        "b2_same_branch": "revision 2 pushed to the same lineage branch",
        "b2_same_pr": "revision 2 moved the same pull request's head",
        "b2_tree_matches_reviewed": "the repaired head tree equals the repair's reviewed workspace",
        "b_request_completed": "B's request completed",
        "b_work_item_published": "B's WorkItem is published",
        "absence_checks_use_api_branches": "forge ownership and absence checks used the "
        "API-reported publication branches, never a name prefix",
    },
    "ci-repair": {
        "b1_head_check_failed": "a real GitHub Actions check on B's first published head "
        "completed with failure",
        "b1_failure_names_ci_ack": "the failing check's annotation names CI_ACK.md",
        "rerun_settled": "the one production rerun completed, or was refused with 403",
        "round2_event_enqueued": "work-item-<B request>-ci-2 was appended to the runs stream",
        "round2_heartbeat_parked": "B's request heartbeat was parked at its execution deadline",
        "repair_same_request": "the repair ran on B's same ExecutionRequest; no new request",
        "repair_owner_b": "the repair's claim carried B's owner labels and B's own grant",
        "repair_native_execution": "the repair ran a native implementer with a tool call",
        "repaired_head_on_branch": "B's lineage branch moved to the repaired head",
        "repaired_head_adds_ci_ack": "the repaired head holds CI_ACK.md with the ack line",
        "repaired_checks_passed": "the latest check runs and statuses at the repaired head all "
        "completed successfully",
        "required_checks_present": "every required check is present at the repaired head",
        "ci_author_not_owner": "CI event authors and the approver never became the owner",
        "owner_unchanged_through_repair": "B's owner tuple and generation are unchanged "
        "through the repair",
        "no_round3": "no round-3 continuation started",
    },
    "restarts": _restarts(),
    "wrong-owner": {
        "admission_b_on_a_issue_refused": "admission naming B on A's tracker issue answered "
        "409 owner_mismatch",
        "admission_request_count_unchanged": "the refused admissions created no request",
        "replay_owner_b_refused": "a replay of A1's request id naming B answered 409 "
        "owner_mismatch",
        "personal_context_b_hidden": "the owner-scoped personal-context read of A's WorkItem "
        "with B's principal answered 404",
        "adoption_b_refused": "B's revalidation binding A's WorkItem to B's enrollment, with "
        "B's tenant, agent and principal at the current WorkItem, request and enrollment "
        "versions, answered 409 owner_mismatch",
        "adoption_a_binding_unchanged": "after every control A's WorkItem kept A's owner, "
        "generation 1 and its WorkItem and request versions",
        "adoption_no_request": "the controls created no request and started none on A's WorkItem",
        "adoption_owner_control": "A's own revalidation at the same versions passed the owner "
        "check (409 generation_current or runtime_live) and rebound nothing",
        "a_marker_unchanged": "A's durable transcript marker is unchanged by the controls",
        "no_b_claim_on_a_thread": "no claim or pod for B appeared on A's thread",
        "no_foreign_runnable_request": "no runnable request with B's tuple exists on A's WorkItem",
        "controls_before_continuation": "every control ran before A's first authorized "
        "continuation (A2)",
        "a_continuation_original_tuple": "A2 ran with A's original owner tuple and generation "
        "and made a native tool call",
    },
    "disconnect": {
        "disconnect_inactive": "A's disconnect answered state inactive",
        "disconnect_generation_advanced": "A's generation advanced past 1",
        "teardown_pending_runtimes": "A's teardown.pending_runtimes reached 0",
        "teardown_pending_custody_cleanup": "A's teardown.pending_custody_cleanup reached 0",
        "a3_tool_observed": "A3 made one native tool call before the disconnect",
        "a3_failed_enrollment_fenced": "A3's request failed with terminal cause enrollment_fenced",
        "a3_termination_observation": "A3's request carries a termination observation",
        "a3_pod_absent": "A3's runner pod is absent",
        "a3_tokens_secret_absent": "A3's <claim>-tokens Secret is absent",
        "a_claims_absent": "no A-labelled SandboxClaim remains",
        "a_custody_secret_absent": "A's custody Secret is absent from the grant namespace",
        "a3_settled_from_hold": "A3's usage settled known from the hold-time harvest",
        "b_tool_after_a_stop": "B-r2 made a new native tool call after A's confirmed stop",
        "b_grant_unchanged": "B's claim delivery still matched B's own grant after A's disconnect",
        "b_active_gen1_after": "B's status stayed active at generation 1 through the end",
        "b_passing_publication_after": "B reached its passing approved publication after A's stop",
        "post_restart_admission_refused": "after the worker restart, admitting A answered 409 "
        "enrollment_inactive",
        "post_restart_no_a_claim": "no A-labelled claim appeared in the observation window",
        "post_restart_no_a_running": "no A request became running",
        "post_restart_no_a3_successor": "no successor request of A3 was created",
        "post_restart_revalidation_refused": "revalidating A's WorkItem answered 409 "
        "enrollment_inactive",
    },
    "budget": {
        "schedule_check_passed": "G-SESSION-PLAN passed for the recorded prior spend before "
        "the first paid start",
        "probe_a_ok": "A's grant probe completed one turn on the probe model",
        "probe_b_ok": "B's grant probe completed one turn on the probe model",
        "probe_isolated_a": "A's probe ran with an empty HOME, config and working directory and "
        "only A's grant",
        "probe_isolated_b": "B's probe ran with an empty HOME, config and working directory and "
        "only B's grant",
        "gate_every_response": "every hold was released only after the harvest read its "
        "transcript to the reported size and gate_decision answered go",
        "limits_written_before_prompt": "subagent limits were written before each pod's first "
        "prompt release",
        "zero_usage_classified": "every synthetic or zero-usage session kept a sanitized error "
        "class",
        "no_allowance_breach": "no reservation share, turn share or session time was breached",
        "paid_starts_fenced": "paid starts were fenced (worker stopped, runner pods deleted, "
        "B disconnected) before the live window closed",
        "unplanned_starts_fenced": "no owner-lost successor, round-3 repair or unmapped "
        "session started",
        "no_credential_fallback": "no installation-wide or service credential supplied any "
        "execution",
    },
    "cleanup": {
        "foreign_preserved": "every foreign resource seen before the run kept its uid",
        "token_scan_both_grants": "the token scan held both owner grants and both session cookies",
        "card_residue_recorded": "card_residue names every delivered card of the run, drill "
        "cards included, as left in its channel; none is claimed removed",
        "forge_removal_guarded": "every forge object removed was confirmed created and still "
        "at this run's recorded head",
        "cleanup_within_window": "every cleanup step, from the fence that opens finalize to the "
        "last inventory, finished within 600 s of one cleanup clock",
    },
}
#: Numeric observations: (operator, bound). Every other key must be exactly True.
COMPARISONS: dict[str, dict[str, tuple[str, int]]] = {
    "owners": {"custody_secret_count": ("==", 2)},
    "approval": {
        "drill_unlisted_status": ("==", 403),
        "card1_unlisted_status": ("==", 403),
        "card4_unlisted_status": ("==", 403),
    },
    "disconnect": {
        "teardown_pending_runtimes": ("==", 0),
        "teardown_pending_custody_cleanup": ("==", 0),
    },
}
#: Reported, never failing (coordinator decision, Option A).
NOT_MET: dict[str, list[str]] = {
    "restarts": ["uncommitted workspace restoration: Not met, tracked in #5541"]
}
#: Recorded and reported under ``observed``, never required: same pod, an
#: in-place container restart and uncommitted files are observations (AC 5,
#: coordinator decision 2); the uncommitted ones are the #5541 Not met item.
OBSERVATIONS: dict[str, dict[str, str]] = {"restarts": _restart_observations()}
#: The unpaid rehearsal's summary keys (``judge(ledger, "rehearsal")``).
REHEARSAL_EXPECTED: dict[str, str] = {
    "install_no_runner_grant": "the install rendered no runner grant env and "
    "installation_grant_findings was empty",
    "dex_login_a": "a Dex password login minted a principal session for owner A",
    "dex_login_b": "a Dex password login minted a principal session for owner B",
    "principals_distinct": "the two principal ids differ",
    "schedule_check_passed": "G-SESSION-PLAN passed",
    "card_delivered": "the drill card was delivered through the channel gateway",
    "card_unlisted_status": "the unlisted operator's resolve answered 403",
    "card_rejected_by_operator": "the allowed operator rejected a drill card",
    "card_approved_by_operator": "the allowed operator approved a drill card",
    "admission_controls_refused": "with the worker stopped, B's admission, replay, context "
    "read and adoption of A's WorkItem were refused before any request ran, while A's own "
    "read and revalidation passed the owner check",
    "disconnect_refused_after_restart": "after a worker restart, an unenrolled owner's "
    "disconnect and admission were refused",
    "no_grant_loaded": "the rehearsal loaded no grant",
}
REHEARSAL_COMPARISONS: dict[str, tuple[str, int]] = {"card_unlisted_status": ("==", 403)}
INVENTORY_KINDS = (
    *pps.INVENTORY_KINDS,
    "grant_namespace",
    "custody_secrets",
    "claims",
    "claim_tokens",
    "dex",
)
_DONE_SOURCES = frozenset({"native", "not_started", "probe"})

# -- the credential capture audit (AC 9) ---------------------------------------
#
# The single-profile channels (pps.SCAN_CHANNELS) are scanned as bytes pass
# through the driver. Finalize adds, after the fence and before teardown, the
# captures AC 9 names that the driver does not otherwise read in full: the logs
# of every container instance in the release and grant namespaces, both
# namespaces' events, the API's final records of the run, its publications and
# pull requests, and the install's traces. Each is scanned in memory on its own
# channel and never written to disk or printed. The pods the driver removes on
# purpose (worker scale-downs, runner pod deletions, the planned fence) are gone
# by then, so their logs are captured the same way right before each removal;
# the stop path (watchdog, gate stop, paid failure) captures nothing, for speed,
# and names each pod it removed in the pod_logs receipt instead.

POD_LOGS_CHANNEL = "pod_logs"
EVENTS_CHANNEL = "events"
API_CAPTURES_CHANNEL = "api_captures"
PUBLICATION_CAPTURES_CHANNEL = "publication_captures"
TRACES_CHANNEL = "traces"
#: The channels the finalize capture audit fills; each one carries a receipt.
CAPTURE_CHANNELS = (
    POD_LOGS_CHANNEL,
    EVENTS_CHANNEL,
    API_CAPTURES_CHANNEL,
    PUBLICATION_CAPTURES_CHANNEL,
    TRACES_CHANNEL,
)
#: The pod_logs receipt's per-pod record of every pod the driver removed:
#: ``before_deletion`` (every log read before the removal succeeded) and
#: ``uncaptured`` (removed with no complete capture).
REMOVAL_RECORDS = ("before_deletion", "uncaptured")
#: What a removed pod ran: the worker or a runner.
POD_WORKLOADS = ("worker", "runner")
#: The removals that capture first: a worker scale-down, a runner pod deletion
#: and the planned fence (the live end, the rehearsal cut and finalize).
CAPTURED_REMOVALS = ("stop-worker", "remove-pod", "fence", "disconnect", "process-interruption")
#: Every removal step; the stop path's two capture nothing.
REMOVAL_STEPS = (*CAPTURED_REMOVALS, "emergency-stop", "emergency-fence", "capture-missing")
#: Forge file contents read at published heads (``TwoOwnerRun.forge_file``).
FORGE_CHANNEL = "forge"
#: Every channel a live ledger must show scanned, with at least one byte and no token.
LIVE_SCAN_CHANNELS = (*pps.SCAN_CHANNELS, FORGE_CHANNEL, *CAPTURE_CHANNELS)
#: A rehearsal starts no runner pod and reads no published head, so it has no
#: transcript harvest and no forge file to scan.
REHEARSAL_SCAN_CHANNELS = tuple(
    name for name in LIVE_SCAN_CHANNELS if name not in {"harvest", FORGE_CHANNEL}
)
#: Where an install's traces land, from its rendered values (``trace_backend``).
TRACE_BACKENDS = ("in_cluster", "absent", "external")
#: Langfuse's ClickHouse tables (its default database) that hold trace content.
TRACE_TABLES = ("traces", "observations", "scores")
#: ``sh -c`` in the release's ClickHouse container: clickhouse-client logs in
#: with that container's own CLICKHOUSE_USER and CLICKHOUSE_PASSWORD
#: (charts/curie/templates/clickhouse.yaml), so the driver's argv carries the
#: query only; ``$1`` is the query.
CLICKHOUSE_EXEC = (
    'exec clickhouse-client --user "$CLICKHOUSE_USER" '
    '--password "$CLICKHOUSE_PASSWORD" --query "$1"'
)


def _deployed(values: Mapping[str, Any], name: str) -> bool:
    section = values.get(name)
    return not (isinstance(section, Mapping) and section.get("deploy") is False)


def trace_backend(values: Mapping[str, Any]) -> str:
    """Where the install's traces land: ``in_cluster``, ``absent`` or ``external``.

    The chart's defaults (charts/curie/values.yaml) deploy Langfuse, its
    ClickHouse and the OTel collector, which then exports to that Langfuse:
    ``in_cluster``, read from ClickHouse. ``otelCollector.telemetryDisabled``
    true exports no traces at all: ``absent``, the explicit fact the cleanup
    judge accepts in place of a trace capture. Any other shape sends traces
    somewhere the driver cannot read: ``external``, which fails the audit.
    """

    otel = values.get("otelCollector")
    otel = otel if isinstance(otel, Mapping) else {}
    if otel.get("telemetryDisabled") is True:
        return "absent"
    if _deployed(values, "otelCollector") and all(
        _deployed(values, name) for name in ("langfuse", "clickhouse")
    ):
        return "in_cluster"
    return "external"


def trace_query(table: str) -> str:
    """One ClickHouse read of a whole trace table; JSON output, so an empty table still answers."""

    if table not in TRACE_TABLES:
        raise ValueError("not a trace table")
    return f"SELECT * FROM {table} FORMAT JSON"


def capture_receipts() -> dict[str, dict[str, Any]]:
    """Empty receipts: per channel, sources read and failed reads; pod_logs' removals."""

    receipts: dict[str, dict[str, Any]] = {
        name: {"sources": 0, "failures": 0} for name in CAPTURE_CHANNELS
    }
    receipts[POD_LOGS_CHANNEL].update({record: [] for record in REMOVAL_RECORDS})
    return receipts


def deleted_pods(output: bytes) -> list[str]:
    """The pod names ``kubectl delete pod -o name`` printed (one ``pod/<name>`` per line)."""

    names = []
    for line in output.decode(errors="replace").splitlines():
        kind, _, name = line.strip().partition("/")
        if kind == "pod" and name and "/" not in name:
            names.append(name)
    return names


def required_scan_channels(*, rehearsal: bool, traces_backend: Any) -> tuple[str, ...]:
    """The channels a ledger must show scanned; traces only where the install exports them."""

    channels = REHEARSAL_SCAN_CHANNELS if rehearsal else LIVE_SCAN_CHANNELS
    if traces_backend == "absent":
        return tuple(name for name in channels if name != TRACES_CHANNEL)
    return channels


def container_log_reads(pods: Any) -> list[tuple[str, str, bool]]:
    """``(pod, container, previous)`` for every container instance whose log exists.

    A ``kubectl get pods -o json`` listing: every init, regular and ephemeral
    container whose current instance has run (a running or terminated state)
    has a current log; one with a restart count or a terminated last state
    also has a previous instance (``kubectl logs --previous``). A container
    that never started has no log and is not a read.
    """

    reads: list[tuple[str, str, bool]] = []
    items = pods.get("items") if isinstance(pods, Mapping) else None
    for pod in items if isinstance(items, list) else []:
        if not isinstance(pod, Mapping):
            continue
        name = str((pod.get("metadata") or {}).get("name") or "")
        status = pod.get("status") or {}
        if not name or not isinstance(status, Mapping):
            continue
        for key in ("initContainerStatuses", "containerStatuses", "ephemeralContainerStatuses"):
            for item in status.get(key) or []:
                if not isinstance(item, Mapping) or not item.get("name"):
                    continue
                container = str(item["name"])
                state = item.get("state")
                last = item.get("lastState")
                restarts = item.get("restartCount")
                if isinstance(state, Mapping) and ("running" in state or "terminated" in state):
                    reads.append((name, container, False))
                if (isinstance(last, Mapping) and "terminated" in last) or (
                    type(restarts) is int and restarts > 0
                ):
                    reads.append((name, container, True))
    return reads


def _scanned(entry: Any) -> bool:
    if not isinstance(entry, Mapping):
        return False
    size = entry.get("scanned_bytes")
    return type(size) is int and size > 0 and entry.get("token_bytes_found") is False


def _captured(entry: Any) -> bool:
    if not isinstance(entry, Mapping):
        return False
    sources, failures = entry.get("sources"), entry.get("failures")
    return type(sources) is int and sources > 0 and type(failures) is int and failures == 0


def _removal(item: Any, steps: tuple[str, ...]) -> bool:
    return (
        isinstance(item, Mapping)
        and isinstance(item.get("pod"), str)
        and bool(item["pod"])
        and item.get("workload") in POD_WORKLOADS
        and item.get("step") in steps
    )


def removals_recorded(entry: Any) -> bool:
    """The pod_logs receipt names each pod the driver removed, captured first or not.

    Both lists must be present, even empty: an absent list is a silent gap.
    A pod under ``before_deletion`` was removed by a step that captures
    first; one under ``uncaptured`` by any step. A step that captures first
    and missed a pod also counted a failed read, which fails the channel;
    the stop path's uncaptured pods are an observation only, since that
    path has already failed the run.
    """

    if not isinstance(entry, Mapping):
        return False
    before, uncaptured = (entry.get(record) for record in REMOVAL_RECORDS)
    return (
        isinstance(before, list)
        and isinstance(uncaptured, list)
        and all(_removal(item, CAPTURED_REMOVALS) for item in before)
        and all(_removal(item, REMOVAL_STEPS) for item in uncaptured)
    )


def scan_checks(
    scans: Any, captures: Any, *, rehearsal: bool, traces_backend: Any
) -> dict[str, bool]:
    """The cleanup judge's scan verdicts from the recorded token scan and capture receipts.

    Every required channel must be present with a nonzero scanned byte count
    and no token bytes; every capture channel among them must have read at
    least one source with no failed read, and the pod_logs receipt must name
    each pod the driver removed (``removals_recorded``); and the trace
    backend must be recorded, with traces either captured from the install
    or explicitly absent from it.
    """

    scans = scans if isinstance(scans, Mapping) else {}
    captures = captures if isinstance(captures, Mapping) else {}
    required = required_scan_channels(rehearsal=rehearsal, traces_backend=traces_backend)
    return {
        "token_scan_complete": all(_scanned(scans.get(name)) for name in required),
        "capture_audit_complete": all(
            _captured(captures.get(name)) for name in CAPTURE_CHANNELS if name in required
        )
        and removals_recorded(captures.get(POD_LOGS_CHANNEL)),
        "traces_audited": traces_backend == "absent"
        or (
            traces_backend == "in_cluster"
            and _scanned(scans.get(TRACES_CHANNEL))
            and _captured(captures.get(TRACES_CHANNEL))
        ),
        "token_bytes_absent": bool(scans)
        and all(
            isinstance(entry, Mapping) and entry.get("token_bytes_found") is False
            for entry in scans.values()
        ),
    }


# -- approval-card residue (AC 9) ----------------------------------------------

#: An approval observation ``<card>_card`` (``pp.LiveRun.await_card``): the drill's two
#: cards and the four scenario cards.
_CARD_STATE_KEY = re.compile(r"(drill_reject|drill_approve|card[1-4])_card")
#: Residue status of one card, and whether it says the card was delivered. The
#: driver never deletes a channel message, so a delivered card is left there.
CARD_RESIDUE_STATUSES: dict[str, bool] = {"left_in_channel": True, "not_delivered": False}


def delivered_cards(approval: Mapping[str, Any]) -> set[str]:
    """The cards whose recorded delivery state says delivered."""

    found: set[str] = set()
    for key, value in approval.items():
        match = _CARD_STATE_KEY.fullmatch(key)
        if match and isinstance(value, Mapping) and value.get("delivered") is True:
            found.add(match[1])
    return found


def card_residue(
    cards: Mapping[str, str], approval: Mapping[str, Any], *, channel: str
) -> dict[str, Any]:
    """The cleanup record of every card the run created: its reference and residue status.

    ``cards`` maps a card (``drill_reject``, ``card1``...) to the digest of its
    publication; ``approval`` holds each card's recorded delivery state.
    """

    delivered = delivered_cards(approval)
    return {
        "channel": channel,
        "removed": False,
        "cards": {
            key: {
                "publication": ref,
                "delivered": key in delivered,
                "residue": "left_in_channel" if key in delivered else "not_delivered",
            }
            for key, ref in sorted(cards.items())
        },
    }


def card_residue_recorded(residue: Any, approval: Mapping[str, Any]) -> bool:
    """True only when ``residue`` names every delivered card with an explicit residue status.

    Each entry needs a publication reference and a status from
    ``CARD_RESIDUE_STATUSES`` that agrees with its delivered flag, nothing may
    be claimed removed, and every card ``approval`` records as delivered must
    be there as delivered.
    """

    if not isinstance(residue, Mapping) or residue.get("removed") is not False:
        return False
    channel, cards = residue.get("channel"), residue.get("cards")
    if not isinstance(channel, str) or not channel or not isinstance(cards, Mapping):
        return False
    for entry in cards.values():
        if not isinstance(entry, Mapping):
            return False
        status, ref = entry.get("residue"), entry.get("publication")
        if status not in CARD_RESIDUE_STATUSES or not isinstance(ref, str) or not ref:
            return False
        if entry.get("delivered") is not CARD_RESIDUE_STATUSES[status]:
            return False
    return all(
        isinstance(cards.get(key), Mapping) and cards[key].get("delivered") is True
        for key in delivered_cards(approval)
    )


# -- the personal-context read the adoption control compares -------------------

#: Codes A's own revalidation may answer at the scenario's controls: the owner
#: check passed and nothing was rebound (personal_execution.revalidate checks
#: the owner before the live runtime and the generation).
OWNER_REVALIDATION_CODES = frozenset({"generation_current", "runtime_live"})


def context_versions(body: Any) -> dict[str, Any] | None:
    """The revalidation versions from a ``PersonalContextOut`` body, or None.

    apps/api/src/curie_api/routers/work_items.py: ``work_item_version``, the
    latest ``request`` (``id``, ``version``) and the owner's ``enrollment``
    (``version``).
    """

    if not isinstance(body, Mapping):
        return None
    request, enrollment = body.get("request"), body.get("enrollment")
    if not isinstance(request, Mapping) or not isinstance(enrollment, Mapping):
        return None
    versions = {
        "request_id": request.get("id"),
        "expected_work_item_version": body.get("work_item_version"),
        "expected_request_version": request.get("version"),
        "expected_enrollment_version": enrollment.get("version"),
    }
    if not isinstance(versions["request_id"], str) or not versions["request_id"]:
        return None
    if any(type(versions[key]) is not int for key in versions if key != "request_id"):
        return None
    return versions


def context_binding(body: Any) -> dict[str, Any] | None:
    """The owner, bound generation and versions of a ``PersonalContextOut`` body, or None."""

    versions = context_versions(body)
    if versions is None or not isinstance(body, Mapping):
        return None
    request = body.get("request") or {}
    return {
        **versions,
        "tenant_id": body.get("tenant_id"),
        "owner_principal_id": body.get("owner_principal_id"),
        "model_auth_mode": body.get("model_auth_mode"),
        "enrollment_generation": body.get("enrollment_generation"),
        "request_generation": request.get("enrollment_generation"),
    }


def check_value(value: Any, rule: tuple[str, int] | None) -> bool:
    """True only for exactly ``True``, or an int (never a bool) meeting ``rule``."""

    if rule is None:
        return value is True
    operator, bound = rule
    if type(value) is not int:
        return False
    if operator == "==":
        return value == bound
    if operator == ">=":
        return value >= bound
    if operator == "<=":
        return value <= bound
    return False


def _expected_checks(
    observed: Mapping[str, Any],
    expected: Mapping[str, str],
    comparisons: Mapping[str, tuple[str, int]],
) -> dict[str, bool]:
    return {key: check_value(observed.get(key), comparisons.get(key)) for key in expected}


def _judge_budget(ledger: TwoOwnerLedger) -> tuple[dict[str, bool], dict[str, int]]:
    observed = ledger.observations("budget")
    run = ledger.run_row()
    rows = ledger.session_rows()
    known = bool(rows) and all(
        row["output_tokens"] is not None
        and row["assistant_turns"] is not None
        and row["elapsed"] is not None
        for row in rows
    )

    def probe(row: Mapping[str, Any]) -> bool:
        group = GROUPS.get(str(row["grp"]))
        return group is not None and group.kind == "probe"

    def within(row: Mapping[str, Any]) -> bool:
        tokens, turns, seconds = (
            (PROBE_OUTPUT_TOKENS, PROBE_TURNS, PROBE_SECONDS)
            if probe(row)
            else (SESSION_OUTPUT_TOKENS, SESSION_TURNS, SESSION_SECONDS)
        )
        return (
            int(row["output_tokens"]) <= tokens
            and int(row["assistant_turns"]) <= turns
            and float(row["elapsed"]) <= seconds
        )

    tokens = {
        owner: sum(int(row["output_tokens"] or 0) for row in rows if row["owner"] == owner)
        for owner in OWNERS
    }
    total = sum(int(row["output_tokens"] or 0) for row in rows)
    prior = ledger.prior_spend()
    live_start, live_end = run["live_started_at"], run["live_ended_at"]
    cleanup = observed.get("cleanup_elapsed")
    windows = {owner: ledger.owner_window(owner) for owner in OWNERS}
    checks = {
        **_expected_checks(observed, EXPECTED["budget"], {}),
        "sessions_recorded": bool(rows),
        "session_count_within": len(rows) <= MAX_SESSIONS,
        "all_groups_reserved": {str(row["grp"]) for row in rows} >= set(GROUPS)
        and all(len(ledger.reservations_of(group.name)) == len(group.roles) for group in MANIFEST),
        "no_unknown_usage": known,
        "settled_from_native": bool(rows)
        and all(
            row["source"] in _DONE_SOURCES and ((row["source"] == "probe") == probe(row))
            for row in rows
        ),
        "no_unreserved_sessions": not any(row["source"] == "unreserved" for row in rows),
        "session_bounds": known and all(within(row) for row in rows),
        "total_turns_within": known
        and sum(int(row["assistant_turns"]) for row in rows) <= MAX_TURNS,
        **{
            f"owner_output_within_{owner}": known and tokens[owner] <= OWNER_OUTPUT_TOKENS
            for owner in OWNERS
        },
        "aggregate_output_within": known and total + prior <= MAX_OUTPUT_TOKENS,
        "budget_not_exceeded": run["budget_state"] == "ok",
        "no_paid_retry": run["live_starts"] == 1 and "paid_retry" not in observed,
        "live_elapsed_within": live_start is not None
        and live_end is not None
        and 0 <= float(live_end) - float(live_start) <= LIVE_SECONDS,
        "cleanup_elapsed_within": isinstance(cleanup, (int, float))
        and not isinstance(cleanup, bool)
        and 0 <= float(cleanup) <= CLEANUP_SECONDS,
        "owner_windows_covered": live_start is not None
        and live_end is not None
        and all(
            value is not None and value >= float(live_start) + GRANT_MIN_LIFETIME
            for value in windows.values()
        ),
    }
    counts = {
        "sessions": len(rows),
        "assistant_turns": sum(int(row["assistant_turns"] or 0) for row in rows),
        "output_tokens": total,
        "prior_item_output_tokens": prior,
        **{f"output_tokens_{owner}": tokens[owner] for owner in OWNERS},
        "retry_refusals": ledger.observation_int("budget", "retry_refusals"),
    }
    return checks, counts


def _judge_cleanup(ledger: TwoOwnerLedger) -> tuple[dict[str, bool], dict[str, int]]:
    observed = ledger.observations("cleanup")
    rows = ledger.cleanup_rows()
    steps = [row for row in rows if row["kind"].startswith("step:")]
    inventory = [row for row in rows if row["kind"].startswith("inventory:")]
    kinds = {row["kind"].removeprefix("inventory:") for row in inventory}
    raw = observed.get("token_scan")
    scans: Mapping[str, Any] = raw if isinstance(raw, dict) else {}
    checks = {
        **_expected_checks(observed, EXPECTED["cleanup"], {}),
        "steps_recorded": bool(steps),
        "steps_ok": bool(steps) and all(row["status"] == "ok" for row in steps),
        "inventory_complete": set(INVENTORY_KINDS) <= kinds,
        "inventory_absent": bool(inventory) and all(row["status"] == "absent" for row in inventory),
        **scan_checks(
            scans,
            observed.get("capture_audit"),
            rehearsal=ledger.is_rehearsal(),
            traces_backend=observed.get("traces_backend"),
        ),
    }
    # The flag alone is not evidence: the recorded residue must name every card
    # the approval criterion recorded as delivered.
    checks["card_residue_recorded"] = checks["card_residue_recorded"] and card_residue_recorded(
        observed.get("card_residue"), ledger.observations("approval")
    )
    counts = {
        "steps": len(steps),
        "failed_steps": sum(row["status"] != "ok" for row in steps),
        "inventory_queries": len(inventory),
        "residual_or_uncertain": sum(row["status"] != "absent" for row in inventory),
        "scanned_channels": len(scans),
        **removal_counts(observed.get("capture_audit")),
    }
    return checks, counts


def removal_counts(captures: Any) -> dict[str, int]:
    """How many removed pods the pod_logs receipt names as captured first, and as not."""

    captures = captures if isinstance(captures, Mapping) else {}
    entry = captures.get(POD_LOGS_CHANNEL)
    entry = entry if isinstance(entry, Mapping) else {}
    counts: dict[str, int] = {}
    for record in REMOVAL_RECORDS:
        items = entry.get(record)
        counts[f"pods_{record}"] = len(items) if isinstance(items, list) else 0
    return counts


def _judge_expected(
    ledger: TwoOwnerLedger, criterion: str
) -> tuple[dict[str, bool], dict[str, int]]:
    observed = ledger.observations(criterion)
    checks = _expected_checks(observed, EXPECTED[criterion], COMPARISONS.get(criterion, {}))
    counts = {
        "expected": len(checks),
        "recorded": sum(key in observed for key in checks),
        "passed": sum(checks.values()),
    }
    return checks, counts


def _judge_rehearsal(ledger: TwoOwnerLedger) -> tuple[dict[str, bool], dict[str, int]]:
    observed = ledger.observations("rehearsal")
    rows = ledger.session_rows()
    run = ledger.run_row()
    checks = {
        "rehearsal_ledger": ledger.is_rehearsal(),
        **_expected_checks(observed, REHEARSAL_EXPECTED, REHEARSAL_COMPARISONS),
        "no_native_sessions": not rows,
        "no_live_start": not run["live_starts"] and run["live_started_at"] is None,
    }
    return checks, {"sessions": len(rows)}


def judge(ledger: TwoOwnerLedger, criterion: str) -> dict[str, Any]:
    """One criterion's verdict from recorded observations and ledger rows only.

    A check passes only when its value is exactly True or an int meeting its
    comparison; a missing, None or other value fails. ``all`` runs every
    criterion. A rehearsal ledger passes no live criterion: each answers
    ``live_ledger: False``, while ``cleanup`` and ``rehearsal`` judge it.
    ``not_met`` lists what is reported Not met and never fails the verdict;
    ``observed`` carries the recorded ``OBSERVATIONS`` keys, which never fail
    it either.
    """

    if criterion == "all":
        checks: dict[str, bool] = {}
        counts: dict[str, int] = {}
        not_met: list[str] = []
        observed: dict[str, Any] = {}
        names = (*CRITERIA, "rehearsal") if ledger.is_rehearsal() else CRITERIA
        for name in names:
            result = judge(ledger, name)
            checks.update({f"{name}.{key}": value for key, value in result["checks"].items()})
            counts.update({f"{name}.{key}": value for key, value in result["counts"].items()})
            not_met.extend(result["not_met"])
            observed.update({f"{name}.{key}": value for key, value in result["observed"].items()})
        return {
            "criterion": "all",
            "verdict": "pass" if checks and all(checks.values()) else "fail",
            "checks": checks,
            "counts": counts,
            "not_met": not_met,
            "observed": observed,
        }
    if criterion == "rehearsal":
        checks, counts = _judge_rehearsal(ledger)
    elif criterion not in CRITERIA:
        raise pps.Refusal("arguments_invalid", "--criterion")
    elif ledger.is_rehearsal() and criterion != "cleanup":
        checks, counts = {"live_ledger": False}, {}
    elif criterion == "budget":
        checks, counts = _judge_budget(ledger)
    elif criterion == "cleanup":
        checks, counts = _judge_cleanup(ledger)
    else:
        checks, counts = _judge_expected(ledger, criterion)
    recorded = ledger.observations(criterion)
    return {
        "criterion": criterion,
        "verdict": "pass" if checks and all(checks.values()) else "fail",
        "checks": checks,
        "counts": counts,
        "not_met": list(NOT_MET.get(criterion, [])),
        "observed": {
            key: recorded[key] for key in OBSERVATIONS.get(criterion, {}) if key in recorded
        },
    }


# --------------------------------------------------------------------------
# J. Live evidence readers (the paid phases read these shapes)
# --------------------------------------------------------------------------

#: The runner's temporary root the harvester walks (pps.harvest_config).
HOLD_TMP_ROOT = "/tmp"
_CONFIG_DIR = re.compile(r"curie-claude-config-[A-Za-z0-9._-]+")
#: SandboxClaim owner labels the worker writes (apps/worker sandbox/types.py).
CLAIM_OWNER_LABELS = {
    "principal": "curietech.ai/owner-principal",
    "tenant": "curietech.ai/owner-tenant",
    "generation": "curietech.ai/enrollment-generation",
    "mode": "curietech.ai/model-auth-mode",
}
#: The runs-stream entry field holding the QueuedTurn JSON.
STREAM_PAYLOAD_FIELD = "payload"
ACTIONS_APP = "github-actions"
CI_ACK_FILE = "CI_ACK.md"


def hold_relpath(path: Any, *, tmp_root: str = HOLD_TMP_ROOT) -> str | None:
    """The harvest key of a transcript path a gate hold reports. Pure.

    The harvester names files relative to the runner's temporary root
    (``curie-claude-config-<x>/projects/.../<session>.jsonl``). A path outside
    that tree, with a parent hop, or not a ``.jsonl`` file is None: the driver
    cannot prove it read it, so the hold is stopped.
    """

    root = tmp_root.rstrip("/") + "/"
    if not isinstance(path, str) or not path.startswith(root) or not path.endswith(".jsonl"):
        return None
    rel = path[len(root) :]
    parts = PurePosixPath(rel).parts
    if (
        len(parts) < 4
        or not _CONFIG_DIR.fullmatch(parts[0])
        or parts[1] != "projects"
        or any(part in {"", ".", ".."} for part in parts)
        or str(PurePosixPath(*parts)) != rel
    ):
        return None
    return rel


def probe_argv(cli: str) -> list[str]:
    """``pps.grant_probe_argv`` on PROBE_MODEL: one turn, no tools, no persistence. Pure."""

    argv = pps.grant_probe_argv(cli)
    argv[argv.index("--model") + 1] = PROBE_MODEL
    return argv


def probe_outcome(returncode: int, stdout: bytes, stderr: bytes = b"") -> dict[str, Any]:
    """The probe's sanitized outcome and whether it stayed inside the probe limits. Pure.

    ``pps.grant_probe_result`` plus the CLI JSON's ``num_turns``, the models
    its ``modelUsage`` names, and ``within_limits``: output tokens read as an
    int no larger than PROBE_OUTPUT_TOKENS and at most PROBE_TURNS turns. A
    missing usage reading is never inside the limits.
    """

    outcome = pps.grant_probe_result(returncode, stdout, stderr)
    try:
        body = json.loads(stdout)
    except ValueError:
        body = None
    turns = body.get("num_turns") if isinstance(body, dict) else None
    tokens = outcome.get("output_tokens")
    models = [str(name) for name in outcome.get("models") or []]
    outcome["model"] = PROBE_MODEL
    outcome["turns"] = turns if type(turns) is int and turns >= 0 else None
    outcome["on_probe_model"] = bool(models) and all(
        pps.same_model(name, PROBE_MODEL) for name in models
    )
    outcome["within_limits"] = (
        type(tokens) is int
        and 0 <= tokens <= PROBE_OUTPUT_TOKENS
        and outcome["turns"] is not None
        and outcome["turns"] <= PROBE_TURNS
    )
    return outcome


def native_tool_calls(
    files: Mapping[str, bytes],
    *,
    exclude_ids: frozenset[str] = frozenset(),
    not_before: float | None = None,
) -> list[dict[str, Any]]:
    """Every native tool call in harvested transcripts, oldest first. Pure.

    Each item names the tool, its record time (epoch seconds or None),
    whether it was made in a subagent, and whether a ``tool_result`` for it
    was recorded. Records the replay rule of ``pps.parse_native_transcripts``
    excludes (older than ``not_before``, or an assistant message id in
    ``exclude_ids``) are not this session's calls.
    """

    calls: list[dict[str, Any]] = []
    results: set[str] = set()
    for name, data in sorted(files.items()):
        parts = PurePosixPath(name).parts
        if not name.endswith(".jsonl") or "projects" not in parts:
            continue
        records, _malformed, _cut = pps._records(data)
        for record in records:
            for item in pps._content(record):
                if item.get("type") == "tool_result" and isinstance(item.get("tool_use_id"), str):
                    results.add(item["tool_use_id"])
            if record.get("type") != "assistant" or pps._replayed(record, exclude_ids, not_before):
                continue
            stamp = pps._timestamp(record.get("timestamp"))
            for item in pps._content(record):
                if item.get("type") == "tool_use" and isinstance(item.get("id"), str):
                    calls.append(
                        {
                            "id": item["id"],
                            "name": str(item.get("name") or ""),
                            "at": stamp,
                            "subagent": "subagents" in parts,
                        }
                    )
    for call in calls:
        call["result"] = call["id"] in results
    return sorted(calls, key=lambda call: (call["at"] is None, call["at"] or 0.0))


def first_tool_after(calls: Sequence[Mapping[str, Any]], moment: float) -> float | None:
    """The time of the first main-session tool call with a result after ``moment``. Pure."""

    for call in calls:
        at = call.get("at")
        if (
            not call.get("subagent")
            and call.get("result") is True
            and isinstance(at, (int, float))
            and not isinstance(at, bool)
            and at > moment
        ):
            return float(at)
    return None


def claim_labels_match(labels: Any, *, tenant_id: str, principal_id: str, generation: int) -> bool:
    """A SandboxClaim's labels name this owner, generation and personal mode. Pure."""

    if not isinstance(labels, Mapping) or not tenant_id or not principal_id:
        return False
    return (
        labels.get(CLAIM_OWNER_LABELS["principal"]) == principal_id
        and labels.get(CLAIM_OWNER_LABELS["tenant"]) == tenant_id
        and labels.get(CLAIM_OWNER_LABELS["generation"]) == str(generation)
        and labels.get(CLAIM_OWNER_LABELS["mode"]) == pps.PERSONAL_MODE
    )


def marker_turn(marker: str, record_id: str, ts: str) -> dict[str, Any]:
    """One durable transcript record carrying ``marker`` (runner TurnRecord.to_dict shape). Pure."""

    user = f"Remember the proof marker {marker}."
    return {
        "type": "turn",
        "user": user,
        "assistant": "NOTED",
        "ts": ts,
        "messages": [
            {"role": "user", "content": user},
            {"role": "assistant", "content": [{"type": "text", "text": "NOTED"}]},
        ],
        "status": "done",
        "approval": None,
        "harness_replay": None,
        "record_id": record_id,
    }


def stream_has_event(entries: Sequence[tuple[str, Mapping[str, str]]], event_id: str) -> bool:
    """Whether an XRANGE answer holds a QueuedTurn with ``event_id`` in its payload. Pure."""

    for _entry_id, fields in entries:
        raw = fields.get(STREAM_PAYLOAD_FIELD) if isinstance(fields, Mapping) else None
        try:
            payload = json.loads(raw) if isinstance(raw, str) else None
        except ValueError:
            payload = None
        if isinstance(payload, dict) and payload.get("event_id") == event_id:
            return True
    return False


def failing_actions_runs(check_runs: Any) -> list[dict[str, Any]]:
    """Completed GitHub Actions check runs that failed, as ``{"id", "name"}``. Pure.

    Shape: ``GET /repos/{o}/{r}/commits/{sha}/check-runs`` (``check_runs[]``
    with ``id``, ``name``, ``status``, ``conclusion``, ``app.slug``).
    """

    runs = check_runs.get("check_runs") if isinstance(check_runs, dict) else None
    found = []
    for run in runs if isinstance(runs, list) else []:
        if not isinstance(run, dict) or type(run.get("id")) is not int:
            continue
        raw_app = run.get("app")
        app: Mapping[str, Any] = raw_app if isinstance(raw_app, dict) else {}
        if (
            run.get("status") == "completed"
            and run.get("conclusion") == "failure"
            and app.get("slug") == ACTIONS_APP
        ):
            found.append({"id": run["id"], "name": str(run.get("name") or "")})
    return found


def annotations_name(annotations: Any, path: str) -> bool:
    """Whether a check run's annotations point at ``path`` (``GET .../annotations``). Pure."""

    return any(
        isinstance(item, dict)
        and (item.get("path") == path or path in str(item.get("message") or ""))
        for item in (annotations if isinstance(annotations, list) else [])
    )


def rerun_count(check_runs: Any, name: str) -> tuple[int, bool]:
    """Check runs named ``name`` at one head (``filter=all``) and whether all completed. Pure."""

    runs = check_runs.get("check_runs") if isinstance(check_runs, dict) else None
    named = [
        run
        for run in (runs if isinstance(runs, list) else [])
        if isinstance(run, dict) and run.get("name") == name
    ]
    return len(named), bool(named) and all(run.get("status") == "completed" for run in named)


def required_check_names(rules: Any, branch: Any) -> list[str]:
    """Required status check names of a branch from its rules and its protection. Pure.

    Shapes: ``GET /repos/{o}/{r}/rules/branches/{b}`` answers a list of rules,
    the ``required_status_checks`` rule naming ``parameters.
    required_status_checks[].context``; ``GET /repos/{o}/{r}/branches/{b}``
    answers ``protection.required_status_checks`` with ``contexts`` and
    ``checks[].context``.
    """

    names: set[str] = set()
    for rule in rules if isinstance(rules, list) else []:
        if not isinstance(rule, dict) or rule.get("type") != "required_status_checks":
            continue
        raw = rule.get("parameters")
        parameters: Mapping[str, Any] = raw if isinstance(raw, dict) else {}
        for item in parameters.get("required_status_checks") or []:
            if isinstance(item, dict) and isinstance(item.get("context"), str):
                names.add(item["context"])
    protection = branch.get("protection") if isinstance(branch, dict) else None
    required = protection.get("required_status_checks") if isinstance(protection, dict) else None
    if isinstance(required, dict):
        names.update(str(item) for item in required.get("contexts") or [] if isinstance(item, str))
        for item in required.get("checks") or []:
            if isinstance(item, dict) and isinstance(item.get("context"), str):
                names.add(item["context"])
    return sorted(names)


def checks_present(names: Sequence[str], check_runs: Any, statuses: Any) -> bool:
    """Every name has a successful latest check run or commit status at the head. Pure."""

    runs = check_runs.get("check_runs") if isinstance(check_runs, dict) else None
    contexts = statuses.get("statuses") if isinstance(statuses, dict) else None
    passed = {
        str(run.get("name"))
        for run in (runs if isinstance(runs, list) else [])
        if isinstance(run, dict)
        and run.get("status") == "completed"
        and run.get("conclusion") == "success"
    } | {
        str(item.get("context"))
        for item in (contexts if isinstance(contexts, list) else [])
        if isinstance(item, dict) and item.get("state") == "success"
    }
    return bool(names) and all(name in passed for name in names)


def ci_ack_present(content: bytes, branch: str) -> bool:
    """``CI_ACK.md`` holds the line the fixture test asks for on ``branch``. Pure."""

    expected = "ci ack: " + branch
    lines = content.decode("utf-8", errors="replace").splitlines()
    return bool(branch) and expected in [line.strip() for line in lines]


def runner_started_at(pod: Any) -> float | None:
    """The runner container's current start (``state.running.startedAt``) as epoch seconds. Pure.

    None when the pod reports no running runner container or an unreadable time.
    """

    status = pod.get("status") if isinstance(pod, Mapping) else None
    statuses = status.get("containerStatuses") if isinstance(status, Mapping) else None
    for item in statuses if isinstance(statuses, list) else []:
        if isinstance(item, Mapping) and item.get("name") == pps.RUNNER_CONTAINER:
            state = item.get("state")
            running = state.get("running") if isinstance(state, Mapping) else None
            if not isinstance(running, Mapping):
                return None
            return pps.iso_epoch(running.get("startedAt")) or None
    return None


def native_clock_start(*, monotonic: float, wall: float, container_started: float | None) -> float:
    """Where a native runtime's clock starts on the driver's monotonic clock. Pure.

    ``monotonic`` and ``wall`` are read together at the first observation; a
    readable, earlier runner container start moves the start back by the
    container's age. A container start after the observation moves nothing.
    """

    if container_started is None:
        return monotonic
    return monotonic - max(0.0, wall - container_started)


def hold_usage(
    session: pps.SessionFacts | None, *, initial_prompt: bool
) -> pps.SessionUsage | None:
    """A session's usage at a hold, or None when it is unknown. Pure.

    Zero is a reading only at a session's initial prompt hold, before its
    first model request: no session parsed yet, or one with no assistant
    record. At any other hold (a later prompt, a response, a Stop or a
    SubagentStop) the session must be parsed with at least one assistant
    record and known usage. A cut record, a synthetic record or an API error
    leaves usage unknown: the failed request may have spent what the
    transcript does not show.
    """

    if session is None:
        return pps.SessionUsage(0, 0, 0.0, "") if initial_prompt else None
    if session.truncated or session.synthetic or session.errors:
        return None
    if session.assistant_turns == 0:
        return pps.SessionUsage(0, 0, round(session.elapsed, 3), "") if initial_prompt else None
    return session.usage()


def zero_usage_class(summary: Mapping[str, Any], *, planned: str | None = None) -> str | None:
    """The class of a zero-output or synthetic session summary, or None when unclassified. Pure.

    A synthetic or provider-error record keeps its sanitized error class
    (``pps.error_class``: cli_error, http_status, provider_error_type); a
    planned zero (an interrupted prompt hold, an unused reviewer slot) is
    named by the caller. Anything else is unclassified.
    """

    errors = [item for item in summary.get("error_classes") or [] if isinstance(item, Mapping)]
    for item in errors:
        text = pps.error_summary(item)
        if text:
            return text
    if summary.get("synthetic"):
        return None
    return planned


# --------------------------------------------------------------------------
# K. The native-process interruption and per-claim grant delivery
# --------------------------------------------------------------------------

#: Test hook only: the process that stands in for runner PID 1. ``kubectl
#: exec`` forwards no local environment, so in a runner container the program
#: always signals PID 1.
INTERRUPT_RUNNER_ENV = "PTO_INTERRUPT_RUNNER_PID"
INTERRUPT_ACTION = "signal_processes"
INTERRUPT_WITNESS_KEYS = frozenset({"action", "native_identity_matched", "native_gone"})
INTERRUPT_POLL_SECONDS = 2

# Run by ``kubectl exec <pod> -c runner -- python3 -c INTERRUPT_PROGRAM <pid>
# <starttime>``. It is #5378's INTERRUPT_PROGRAM (personal_owner_enforcement.py)
# narrowed to two signals. The held CLI is the process whose /proc stat field 22
# (starttime, in clock ticks, the GATE_HOOK_PROGRAM ``cli_start``) equals the
# argument; only that process gets SIGKILL, and only after the match. The
# program polls for up to 2 s until it is gone, a zombie, or another process,
# prints one witness line, then sends SIGTERM to the runner (PID 1) so the
# container restarts in place and the kernel retries the turn on the same
# request. A mismatch, a malformed argument, or a target that is PID 1, the
# runner or the program itself signals nothing at all.
_INTERRUPT_TEMPLATE = r"""
import json, os, signal, sys, threading, time
RUNNER = int(os.environ.get("@RUNNER_ENV@") or "1")


def identity(pid):
    try:
        with open("/proc/%d/stat" % pid) as handle:
            fields = handle.read().rsplit(")", 1)[1].split()
        return fields[0], int(fields[19])
    except (OSError, IndexError, ValueError):
        return None


def ended(pid, start):
    now = identity(pid)
    return now is None or now[0] == "Z" or now[1] != start


try:
    target, start = int(sys.argv[1]), int(sys.argv[2])
except (IndexError, ValueError):
    target, start = 0, -1
eligible = (
    len(sys.argv) == 3
    and target > 1
    and start >= 0
    and target not in (RUNNER, os.getpid(), os.getppid())
)
before = identity(target) if eligible else None
matched = before is not None and before[1] == start
gone = False
if matched:
    try:
        os.kill(target, signal.SIGKILL)
    except OSError:
        pass
    pause = threading.Event()
    until = time.monotonic() + @POLL_SECONDS@
    gone = ended(target, start)
    while not gone and time.monotonic() < until:
        pause.wait(0.02)
        gone = ended(target, start)
witness = {"action": "@ACTION@", "native_identity_matched": matched, "native_gone": gone}
print(json.dumps(witness), flush=True)
if matched:
    os.kill(RUNNER, signal.SIGTERM)
"""
INTERRUPT_PROGRAM = (
    _INTERRUPT_TEMPLATE.replace("@RUNNER_ENV@", INTERRUPT_RUNNER_ENV)
    .replace("@POLL_SECONDS@", str(INTERRUPT_POLL_SECONDS))
    .replace("@ACTION@", INTERRUPT_ACTION)
)


def interrupt_witness(stdout: bytes) -> dict[str, Any] | None:
    """INTERRUPT_PROGRAM's one witness line, or None for anything else. Pure.

    Exactly one non-blank line holding exactly the three keys, the action
    ``signal_processes`` and two booleans; a process reported gone that was
    never matched is not a witness.
    """

    lines = [line for line in stdout.splitlines() if line.strip()]
    if len(lines) != 1:
        return None
    try:
        value = json.loads(lines[0])
    except ValueError:
        return None
    if not isinstance(value, dict) or set(value) != INTERRUPT_WITNESS_KEYS:
        return None
    matched, gone = value["native_identity_matched"], value["native_gone"]
    if value["action"] != INTERRUPT_ACTION or type(matched) is not bool or type(gone) is not bool:
        return None
    if gone and not matched:
        return None
    return {"action": INTERRUPT_ACTION, "native_identity_matched": matched, "native_gone": gone}


#: The grant's env name in the runner and its sole key in ``<claim>-tokens``
#: and in the custody Secret (custody_store.CUSTODY_SECRET_KEY).
GRANT_KEY = pps.PERSONAL_GRANT_ENV


def grant_digest(value: str) -> bytes:
    """SHA-256 of one held grant, compared and never shown. Pure."""

    return hashlib.sha256(value.encode()).digest()


def claim_grant_delivery(
    document: bytes, *, secret: str, own: bytes, other: bytes
) -> dict[str, bool]:
    """Whether a ``<claim>-tokens`` Secret JSON holds ``own``'s grant and not ``other``'s. Pure.

    ``own`` and ``other`` are ``grant_digest`` values of the two owners'
    distinct grants. The data value is decoded in memory, hashed and dropped;
    only the two comparisons leave. Another Secret name, a missing or empty
    key, a value that is not strict base64, or equal digests answer False for
    both.
    """

    refused = {"own_grant": False, "not_other": False}
    if len(own) != 32 or len(other) != 32 or hmac.compare_digest(own, other):
        return refused
    try:
        parsed = json.loads(document)
    except ValueError:
        return refused
    meta = parsed.get("metadata") if isinstance(parsed, dict) else None
    data = parsed.get("data") if isinstance(parsed, dict) else None
    encoded = data.get(GRANT_KEY) if isinstance(data, dict) else None
    del parsed, data
    if not isinstance(meta, dict) or meta.get("name") != secret or not isinstance(encoded, str):
        return refused
    try:
        decoded = base64.b64decode(encoded, validate=True)
    except ValueError:
        return refused
    finally:
        del encoded
    digest = hashlib.sha256(decoded).digest() if decoded else b""
    del decoded
    if not digest:
        return refused
    return {
        "own_grant": hmac.compare_digest(digest, own),
        "not_other": not hmac.compare_digest(digest, other),
    }


def claim_grant_ref(pods: Any, secret: str) -> bool:
    """Every listed runner pod reads its grant only from ``secret``'s grant key. Pure.

    True only for a non-empty pod list in which each pod has one ``runner``
    container with exactly one ``CLAUDE_CODE_OAUTH_TOKEN`` env entry, whose
    sole source is a required ``secretKeyRef`` to ``secret`` and that key, with
    no literal value. Reads names only; a pod spec carries no Secret value.
    """

    items = pods.get("items") if isinstance(pods, dict) else None
    if not isinstance(items, list) or not items:
        return False
    for pod in items:
        spec = pod.get("spec") if isinstance(pod, dict) else None
        containers = spec.get("containers") if isinstance(spec, dict) else None
        runners = [
            item
            for item in (containers if isinstance(containers, list) else [])
            if isinstance(item, dict) and item.get("name") == "runner"
        ]
        env = runners[0].get("env") if len(runners) == 1 else None
        entries = [
            item
            for item in (env if isinstance(env, list) else [])
            if isinstance(item, dict) and item.get("name") == GRANT_KEY
        ]
        if len(entries) != 1 or set(entries[0]) != {"name", "valueFrom"}:
            return False
        source = entries[0]["valueFrom"]
        ref = source.get("secretKeyRef") if isinstance(source, dict) else None
        if not isinstance(source, dict) or set(source) != {"secretKeyRef"}:
            return False
        if not isinstance(ref, dict) or ref.get("optional") is True:
            return False
        if ref.get("name") != secret or ref.get("key") != GRANT_KEY:
            return False
    return True


#: kubectl jsonpath: ``name|uid|managed-by|custody|enrollment|generation`` for
#: one custody Secret. Metadata only; its data never leaves the cluster.
CUSTODY_METADATA_JSONPATH = (
    'jsonpath={.metadata.name}{"|"}{.metadata.uid}{"|"}'
    '{.metadata.labels.app\\.kubernetes\\.io/managed-by}{"|"}'
    '{.metadata.labels.curietech\\.ai/personal-custody}{"|"}'
    '{.metadata.labels.curietech\\.ai/personal-enrollment}{"|"}'
    "{.metadata.labels.curietech\\.ai/personal-generation}"
)


def custody_row_secret(row: Mapping[str, Any], *, namespace: str) -> str | None:
    """The Secret name one published custody row names, or None when the row is off. Pure.

    ``row`` is one ``personal_claude_custody`` row joined to its enrollment
    header (``header_generation``, ``active``). The name is computed from the
    enrollment UUID and generation (``custody_store.custody_secret_name``), so
    a caller's argv never carries a database string. None unless the row is
    published for the active header's generation in ``namespace`` with the
    grant key, a UID, and exactly that name.
    """

    try:
        enrollment = uuid.UUID(str(row.get("enrollment_id")))
    except ValueError:
        return None
    generation, uid = row.get("generation"), row.get("secret_uid")
    if type(generation) is not int or generation < 1 or not isinstance(uid, str) or not uid.strip():
        return None
    name = f"curie-pe-{enrollment.hex}-g{generation}"
    if (
        row.get("state") == "published"
        and row.get("active") is True
        and row.get("header_generation") == generation
        and row.get("secret_namespace") == namespace
        and row.get("secret_name") == name
        and row.get("secret_key") == GRANT_KEY
    ):
        return name
    return None


def custody_secret_matches(row: Mapping[str, Any], listed: bytes, *, namespace: str) -> bool:
    """The owner's published custody row and the Secret it names agree. Pure.

    ``listed`` is the CUSTODY_METADATA_JSONPATH line for the Secret
    ``custody_row_secret`` names. True only when that Secret carries the name,
    the row's UID and the four ``custody_store.custody_labels`` (managed by
    curie, custody true, the enrollment hex and the generation).
    """

    name = custody_row_secret(row, namespace=namespace)
    if name is None:
        return False
    lines = [line for line in listed.decode(errors="replace").splitlines() if line.strip()]
    if len(lines) != 1:
        return False
    enrollment = uuid.UUID(str(row["enrollment_id"])).hex
    expected = [name, str(row["secret_uid"]), "curie", "true", enrollment, str(row["generation"])]
    return lines[0].strip().split("|") == expected


# --------------------------------------------------------------------------
# L. Recovery evidence: the workspace, the replayed history and the epoch
# --------------------------------------------------------------------------

#: The chart runner's repository workspace: the chart's default workspace
#: mount, which #5378's WORKSPACE_PROGRAM reads too.
RUNNER_WORKSPACE = poes.WORKSPACE_MOUNT
#: Each interruption's fresh runtime, by group. Its workspace is read when the
#: driver adopts its pod, before the gate stream opens, so before any prompt
#: is released and before the model could change anything.
RECOVERED_GROUPS = {"a2": "pod", "b-r1": "worker", "b-r2": "process"}
WORKSPACE_KEYS = frozenset({"head", "files", "committed", "status", "uncommitted", "nonce_paths"})
_SHA256_HEX = re.compile(r"[0-9a-f]{64}")

# Run as ``kubectl exec <pod> -c runner -- python3 -c WORKSPACE_PROGRAM
# <workspace> <nonce>``. It is #5378's WORKSPACE_PROGRAM
# (personal_owner_enforcement.py: HEAD, the SHA-256 of every tracked file and
# the porcelain status), changed only where this scenario needs it:
# 1. it never writes. The runtime it reads continues from this workspace and B's
#    repair publishes from it, so #5378's planted untracked nonce file would
#    enter that publication; the uncommitted observation is instead the run's
#    own issue nonce in the uncommitted paths, and their SHA-256;
# 2. it also hashes every blob of HEAD's tree (``committed``), the committed
#    state any uncommitted edit sits on, so a runtime with edits still shows it;
# 3. git runs with a minimal environment, never the container's, and reads no
#    system or global configuration (HOME is the runner's writable /tmp) and
#    takes no optional lock, so ``status`` never rewrites the index;
# 4. the workspace path is an argument, so the program runs unchanged against a
#    local repository in the tests.
# Importing #5378's driver for its constant would also import its harness
# (factory_e2e and tls_code_host), which the two-owner driver never loads.
WORKSPACE_PROGRAM = r"""
import hashlib, json, os, subprocess, sys
root, nonce = sys.argv[1], sys.argv[2].encode()
env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": "/tmp", "GIT_CONFIG_NOSYSTEM": "1",
       "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_TERMINAL_PROMPT": "0", "LC_ALL": "C",
       "GIT_OPTIONAL_LOCKS": "0"}
git = ["git", "-c", "safe.directory=" + root, "-C", root]


def run(*args, data=None):
    try:
        done = subprocess.run(
            git + list(args), input=data, capture_output=True, env=env, timeout=30
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return done.stdout if done.returncode == 0 else None


def names(raw):
    return [item.decode("utf-8", "replace") for item in (raw or b"").split(b"\0") if item]


def content(path):
    full = os.path.join(root, path)
    try:
        if os.path.islink(full):
            return os.readlink(full).encode()
        if not os.path.isfile(full):
            return None
        with open(full, "rb") as handle:
            return handle.read()
    except OSError:
        return None


def digest(data):
    return None if data is None else hashlib.sha256(data).hexdigest()


state = {"head": None, "files": {}, "committed": {}, "status": None,
         "uncommitted": {}, "nonce_paths": []}
head = run("rev-parse", "--verify", "HEAD")
if head is not None:
    state["head"] = head.decode().strip()
    for path in names(run("ls-files", "-z")):
        state["files"][path] = digest(content(path))
    tree = names(run("ls-tree", "-r", "-z", "--name-only", "HEAD"))
    query = "".join("HEAD:" + path + "\n" for path in tree).encode()
    out = run("cat-file", "--batch", data=query) or b""
    at = 0
    for path in tree:
        end = out.find(b"\n", at)
        if end < 0:
            break
        header = out[at:end].split()
        at = end + 1
        if len(header) != 3 or not header[2].isdigit():
            continue
        size = int(header[2])
        if header[1] == b"blob":
            state["committed"][path] = hashlib.sha256(out[at:at + size]).hexdigest()
        at += size + 1
    status = run("status", "--porcelain=v1", "-z", "--untracked-files=all")
    if status is not None:
        fields = status.split(b"\0")
        entries = []
        index = 0
        while index < len(fields):
            item = fields[index]
            index += 1
            if len(item) < 4:
                continue
            code = item[:2].decode("utf-8", "replace")
            entries.append([code, item[3:].decode("utf-8", "replace")])
            if code[0] in "RC" and index < len(fields):
                entries.append([code, fields[index].decode("utf-8", "replace")])
                index += 1
        state["status"] = sorted(entries)
        found = set()
        for _code, path in entries:
            data = content(path)
            state["uncommitted"][path] = digest(data)
            if nonce and data is not None and nonce in data:
                found.add(path)
        state["nonce_paths"] = sorted(found)
print(json.dumps(state, sort_keys=True))
"""


def _digest_map(value: Any) -> bool:
    return isinstance(value, dict) and all(
        isinstance(path, str)
        and path
        and (digest is None or (isinstance(digest, str) and _SHA256_HEX.fullmatch(digest)))
        for path, digest in value.items()
    )


def workspace_state(stdout: bytes) -> dict[str, Any] | None:
    """WORKSPACE_PROGRAM's one line, or None for anything else. Pure.

    Exactly one non-blank JSON line with exactly WORKSPACE_KEYS: a 40-hex HEAD
    or None, three path-to-SHA-256 (or None) maps, the porcelain status as
    ``[code, path]`` pairs or None, and the sorted nonce paths.
    """

    lines = [line for line in stdout.splitlines() if line.strip()]
    if len(lines) != 1:
        return None
    try:
        value = json.loads(lines[0])
    except ValueError:
        return None
    if not isinstance(value, dict) or set(value) != WORKSPACE_KEYS:
        return None
    head, status, nonce_paths = value["head"], value["status"], value["nonce_paths"]
    if head is not None and not (isinstance(head, str) and pps.is_commit_sha(head)):
        return None
    if not all(_digest_map(value[key]) for key in ("files", "committed", "uncommitted")):
        return None
    if status is not None and not (
        isinstance(status, list)
        and all(
            isinstance(entry, list)
            and len(entry) == 2
            and all(isinstance(part, str) and part for part in entry)
            for entry in status
        )
    ):
        return None
    if not isinstance(nonce_paths, list) or not all(isinstance(p, str) for p in nonce_paths):
        return None
    return copy.deepcopy(value)


def changed_paths(state: Mapping[str, Any]) -> set[str] | None:
    """The paths a workspace's porcelain status names (its uncommitted edits). Pure."""

    status = state.get("status")
    if not isinstance(status, list):
        return None
    return {str(entry[1]) for entry in status}


def workspace_restoration(
    source: Mapping[str, Any],
    before: Mapping[str, Any] | None,
    after: Mapping[str, Any] | None,
) -> dict[str, bool]:
    """Whether the fresh runtime's workspace holds the branch's committed state. Pure.

    ``source`` is the branch head on the owned forge and the SHA-256 of each
    blob in its tree; ``before`` the interrupted runtime's workspace (None
    where no runner existed during the interruption, the worker stop) and
    ``after`` the fresh runtime's, read before its first prompt release. HEAD
    and the committed tree's blobs go through #5378's
    ``poes.committed_restoration`` on both sides, so an uncommitted edit counts
    neither for nor against them. The fresh checkout must also hold exactly
    the committed bytes in every tracked file its status does not name: a
    named path is an uncommitted edit, observed separately (#5541).
    """

    if not isinstance(after, Mapping):
        return {"committed_head_restored": False, "committed_files_restored": False}

    def committed(state: Mapping[str, Any]) -> dict[str, Any]:
        return {"head": state.get("head"), "files": state.get("committed")}

    sides = poes.committed_restoration(
        source, committed(before if isinstance(before, Mapping) else after), committed(after)
    )
    files = source.get("files")
    tracked = after.get("files")
    changed = changed_paths(after)
    checkout = (
        isinstance(files, Mapping)
        and bool(files)
        and isinstance(tracked, Mapping)
        and changed is not None
        and all(
            tracked.get(path) == digest for path, digest in files.items() if path not in changed
        )
        and all(path in files or path in changed for path in tracked)
    )
    return {
        "committed_head_restored": sides["committed_head_restored"],
        "committed_files_restored": sides["committed_files_restored"] and checkout,
    }


def uncommitted_restoration(
    before: Mapping[str, Any] | None, after: Mapping[str, Any] | None
) -> str:
    """Observation only (Not met: #5541): whether uncommitted edits came back. Pure.

    ``none_before`` when the interrupted workspace was unread or had no
    uncommitted path, ``restored`` when the fresh one holds the same paths with
    the same bytes, else ``not_restored``.
    """

    if not isinstance(before, Mapping) or not before.get("uncommitted"):
        return "none_before"
    if isinstance(after, Mapping) and after.get("uncommitted") == before.get("uncommitted"):
        return "restored"
    return "not_restored"


def forge_tree_paths(body: Any) -> list[str] | None:
    """Every blob path of a recursive ``GET /repos/{repo}/git/trees/{sha}`` answer. Pure.

    A truncated or malformed listing, or one with no blob, is None: the
    committed source cannot be read whole, so nothing compares equal to it.
    """

    if not isinstance(body, dict) or body.get("truncated") is not False:
        return None
    tree = body.get("tree")
    if not isinstance(tree, list):
        return None
    paths: list[str] = []
    for entry in tree:
        if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
            return None
        if entry.get("type") == "blob":
            paths.append(entry["path"])
    return sorted(paths) or None


def marker_replayed(files: Mapping[str, bytes], marker: str) -> bool:
    """Whether a main native transcript of a runtime holds ``marker``. Pure.

    ``files`` are the transcripts harvested from the runtime that continued,
    past any earlier runtime's bytes in the same container. The marker is
    appended to the durable thread before the interruption, so finding it here
    means the product replayed that history into the fresh native session;
    reading central storage proves nothing about the runtime. Subagent
    transcripts (under ``subagents``) never carry the conversation, and an
    empty marker matches nothing.
    """

    if not marker:
        return False
    needle = marker.encode()
    return any(
        path.endswith(".jsonl") and "subagents" not in PurePosixPath(path).parts and needle in data
        for path, data in files.items()
    )


def successor_epoch_confirmed(
    old: Mapping[str, Any], new: Mapping[str, Any], *, held_epoch: Any, claim: str
) -> bool:
    """The WorkItem's runtime binding moved past the epoch an interrupted runtime held. Pure.

    For the pod recreation. ``old`` is the interrupted runtime's request, read
    after the fresh runtime bound; ``new`` the fresh runtime's own request,
    read while it runs; ``held_epoch`` the epoch the old runtime held when it
    was observed; ``claim`` the claim the fresh pod serves. The old request
    started, binds no live runtime any more (not waiting, running or
    stopping) and kept or advanced that epoch; a later request of the same
    WorkItem started on the fresh claim at its own epoch under a runtime
    owner. A terminal status alone never confirms it.
    """

    if type(held_epoch) is not int or held_epoch < 1 or not claim:
        return False
    old_epoch, new_epoch = old.get("runtime_epoch"), new.get("runtime_epoch")
    old_sequence, new_sequence = old.get("sequence"), new.get("sequence")
    status = old.get("status")
    return (
        old.get("started") is True
        and isinstance(status, str)
        and status not in (*poes.ACTIVE_REQUEST_STATUSES, "queued")
        and type(old_epoch) is int
        and old_epoch >= held_epoch
        and bool(old.get("id"))
        and new.get("id") != old.get("id")
        and bool(new.get("work_item_id"))
        and new.get("work_item_id") == old.get("work_item_id")
        and type(old_sequence) is int
        and type(new_sequence) is int
        and new_sequence > old_sequence
        and new.get("status") == "running"
        and new.get("started") is True
        and new.get("runtime_claim_name") == claim
        and type(new_epoch) is int
        and new_epoch >= 1
        and bool(new.get("runtime_owner"))
    )


def current_epoch_confirmed(row: Mapping[str, Any], *, request_id: str, held_epoch: Any) -> bool:
    """The request still binds the epoch its interrupted runtime held, under the fresh one. Pure.

    For the worker restart: a CI continuation adopts the same running request
    at the same epoch (the API's heartbeat refuses any other), so the same
    request, still running at exactly the held epoch, shows that no
    termination claim or owner-lost successor superseded the old runtime's
    lease. The process interruption's equivalent is #5378's
    ``poes.process_interruption_checks``.
    """

    return (
        type(held_epoch) is int
        and held_epoch >= 1
        and bool(request_id)
        and row.get("id") == request_id
        and row.get("status") == "running"
        and row.get("runtime_epoch") == held_epoch
    )
