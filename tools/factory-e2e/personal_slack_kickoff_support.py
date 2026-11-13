"""Pure policy, ledger, Slack and judge logic for the live Slack kickoff driver (#5380).

ADR 0268 row 13: a separate sender bot mentions the isolated Curie app with an
issue link, the candidate dispatcher receives it over Socket Mode, trusted
admission records the sender's owner tuple, the factory runs on that owner's
grant, and progress plus the final outcome are read back from the same thread.
A disconnected enrollment then refuses the same sender with no new runnable
state. The orchestration in ``personal_slack_kickoff.py`` gathers observations
and records them under the keys this module's judge reads; the judge computes
every check from a recorded observation and fails a missing one.

Everything here runs without a cluster, Slack, a forge or a network. Standard
library only, plus the stateless helpers borrowed from the #5375 driver in the
single import block below. Nothing here prints, logs or persists a held secret;
every error names a variable or a refusal code, never a value.
"""

from __future__ import annotations

import base64
import contextlib
import dataclasses
import datetime as dt
import json
import os
import re
import sqlite3
import stat
import time
import urllib.parse
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

from curie_internal import driver_declaration
from curie_internal.issue_links import IssueLink, find_issue_links
from curie_internal.slack_mention_receipt import (
    SlackMentionReceipt,
    kickoff_request_id,
    receipt_key,
)

# The one import block from the #5375 driver's support module: stateless helpers
# only (plan, Stream D). Refusal is what every borrowed check raises, and
# HARVEST_PROGRAM is the in-pod program harvest_config configures. Each is
# re-exported through __all__ for the driver. A rename there fails this import,
# and test_every_borrowed_symbol_is_exercised names the path that uses each.
from personal_profile_support import (
    HARVEST_PROGRAM,
    ChannelScan,
    HarvestState,
    HeldSecret,
    Refusal,
    Scrubber,
    TokenScan,
    candidate_refs,
    chart_fullname,
    check_grant_file,
    child_environment,
    grant_probe_argv,
    grant_probe_env,
    grant_probe_result,
    harvest_config,
    image_tag,
    load_grant,
    loopback_get,
    parse_expiry,
    parse_native_transcripts,
    pod_diagnostics,
    ref_digest,
    refuse_ambient_credentials,
    registry_image,
    require_app_kit,
    require_candidate,
    require_dns_name,
    require_lifetime,
    require_private_dir,
    require_registry,
    require_release,
    require_run_id,
    runner_image_check,
    running_image_failures,
    same_model,
    session_summary,
    split_image_ref,
    verify_candidate_images,
)

__all__ = [
    "HARVEST_PROGRAM",
    "ChannelScan",
    "HarvestState",
    "HeldSecret",
    "Refusal",
    "Scrubber",
    "TokenScan",
    "candidate_refs",
    "chart_fullname",
    "check_grant_file",
    "child_environment",
    "grant_probe_argv",
    "grant_probe_env",
    "grant_probe_result",
    "harvest_config",
    "image_tag",
    "load_grant",
    "loopback_get",
    "parse_expiry",
    "parse_native_transcripts",
    "pod_diagnostics",
    "ref_digest",
    "refuse_ambient_credentials",
    "registry_image",
    "require_app_kit",
    "require_candidate",
    "require_dns_name",
    "require_lifetime",
    "require_private_dir",
    "require_registry",
    "require_release",
    "require_run_id",
    "runner_image_check",
    "running_image_failures",
    "same_model",
    "session_summary",
    "split_image_ref",
    "verify_candidate_images",
    "IssueLink",
    "SlackMentionReceipt",
    "find_issue_links",
    "kickoff_request_id",
    "receipt_key",
]

# --------------------------------------------------------------------------
# Names shared with the platform (strings, so a rename fails a test, not a run)
# --------------------------------------------------------------------------

IMPLEMENTER = "implementer"
PLAN_REVIEWER = "plan-reviewer"
DIFF_REVIEWER = "diff-reviewer"
UNATTRIBUTED = "unattributed"
ROLES = (IMPLEMENTER, PLAN_REVIEWER, DIFF_REVIEWER)
LAYER_IMAGE = "curie-factory-runner"
REVISION_LABEL = "org.opencontainers.image.revision"
# A label unique to one run, on every image it builds (see candidate_labels).
RUN_LABEL = "org.curie.e2e.run"
PERSONAL_MODE = "personal"
GRANT_KEY = "CLAUDE_CODE_OAUTH_TOKEN"
CLAIM_TOKENS_SUFFIX = "-tokens"
OWNER_TENANT_LABEL = "curietech.ai/owner-tenant"
OWNER_PRINCIPAL_LABEL = "curietech.ai/owner-principal"
GENERATION_LABEL = "curietech.ai/enrollment-generation"
DEFAULT_IDENTITY = "default"
KICKOFF_ORIGIN = "kickoff"
TERMINAL_POST = "terminal"
PULL_REQUEST_POST = "pull_request"
ROOT_POST = "root"
ADMITTED = "admitted"
ENROLLMENT_INACTIVE = "enrollment_inactive"
TEST_MARK = "[test action]"
RECEIPT_PATTERN = receipt_key("*")
AUTH_TERMINAL_CAUSE = "authentication_required"
MANAGED_BY_LABEL = "app.kubernetes.io/managed-by"
MANAGED_BY_VALUE = "curie-personal-slack-kickoff"
RUN_ANNOTATION = "curie.dev/personal-slack-kickoff-run"
LEDGER_FILE = "slack-kickoff-ledger.sqlite3"
REDACTED = "<redacted>"

EXIT_PASS = 0
EXIT_FAIL = 1
EXIT_REFUSED = 2

# --------------------------------------------------------------------------
# The approved live ceiling (coordinator ruling, 2026-11-12). The string the
# maintainer approves is passed as --approve-live; nothing may exceed these.
# --------------------------------------------------------------------------

CEILING_KEYS = (
    "sessions",
    "turns",
    "session-turns",
    "output-tokens",
    "session-output-tokens",
    "session-seconds",
    "paid-retries",
)
APPROVED_MAXIMUM: Mapping[str, int] = {
    "sessions": 8,
    "turns": 96,
    "session-turns": 32,
    "output-tokens": 16384,
    "session-output-tokens": 8192,
    "session-seconds": 300,
    "paid-retries": 0,
}
# The grant probe stays outside the ceiling only as one haiku turn of at most
# 32 output tokens (ruling 3); anything else fails the run before a reservation.
GRANT_PROBE_MODEL = "claude-haiku-5-5"
GRANT_PROBE_OUTPUT_CAP = 32
GRANT_PROBE_TURNS = 1
# The runner's per-response cap: the overshoot past a reached bound is at most
# the response in flight.
RESPONSE_OUTPUT_CAP = 4096

FIXES: dict[str, str] = {
    "service_credentials": "unset the named service credential variables; the grant comes "
    "only from --grant-file",
    "fake_mode": "unset CURIE_FAKE_MODEL; this proof needs the real model",
    "candidate_invalid": "pass --candidate as a full 40 hex commit SHA",
    "candidate_image_mismatch": "the registry does not serve the candidate's images",
    "candidate_mismatch": "judge the run with the candidate its ledger recorded",
    "state_dir_unsafe": "--state-dir must be an existing directory you own, mode 0700",
    "ledger_missing": "no private ledger exists for this run ID",
    "ledger_unsafe": "the ledger file must be a regular file you own, mode 0600",
    "arguments_invalid": "fix the named argument",
    "release_reserved": "choose a release name other than curie",
    "namespace_exists": "choose a namespace (and grant namespace) that does not exist",
    "registry_port_invalid": "pass --registry-port as a free TCP port number",
    "registry_port_busy": "something already listens on 127.0.0.1 at --registry-port",
    "app_kit_invalid": "--app-dir needs app.json (id, installation_id, repo) and app.pem",
    "grant_missing": "pass --grant-file naming a nonempty private file",
    "grant_unsafe": "the grant file must be a regular file you own, mode 0600, "
    "one ASCII line of 1 to 4096 bytes, not a symlink",
    "expiry_unknown": "pass --grant-expires-at as ISO 8601 with an offset or epoch seconds",
    "lifetime_insufficient": "use a grant whose remaining lifetime covers the live window",
    "live_unapproved": "pass --approve-live with the exact canonical approved ceiling string",
    "slack_kit_invalid": "pass private 0600 one-line token files (xoxb- bot tokens, an "
    'xapp- app-level token) and a channel file {"channel_id": ...}',
    "slack_unavailable": "a Slack Web API check failed; the ledger names the method and code",
    "isolation_unconfirmed": "pass --isolation-file: a private JSON confirmation that names "
    "this app's bot user and workspace with confirmed true",
    "paid_retry_refused": "a run ID gets one live start; choose a new run ID",
    "rehearsal_run_id": "this run ID was a rehearsal; a live run needs a fresh run ID",
    "rehearsal_ledger": "a rehearsal ledger is judged only with slack, refusal or cleanup",
    "usage_unknown": "an earlier session's usage is unknown, so no further start is allowed",
    "budget_exceeded": "an approved bound was reached; no further start is allowed",
    "allowance_exhausted": "the remaining approved allowance cannot cover another start",
    "gate_closed": "paid starts are fenced for this run; no further reservation is allowed",
    "grant_probe_refused": "the grant probe failed or exceeded its one-turn, 32-token bound",
    "registry_invalid": "pass --registry-port as a free loopback TCP port",
    "gate_mismatch": "the runner layer carries other reservation limits; rebuild with images",
    "deployed_service_credential": "the install carries a service credential or fake model",
    "link_invalid": "the fixture issue URL is not exactly one issue link",
}


def refusal_payload(refusal: Refusal) -> dict[str, str]:
    """A refusal as ``{"error", "fix"}``: names and codes only, never a value."""

    fix = FIXES.get(refusal.code, "see tools/factory-e2e/README.md")
    if refusal.detail:
        fix = f"{fix} ({refusal.detail})"
    return {"error": refusal.code, "fix": fix}


# --------------------------------------------------------------------------
# The ceiling
# --------------------------------------------------------------------------

_INT = re.compile(r"0|[1-9][0-9]{0,8}")
_MODEL = re.compile(r"[a-z0-9][a-z0-9.\-]{0,79}")


@dataclasses.dataclass(frozen=True)
class Ceiling:
    """The approved live ceiling, parsed from the exact ``--approve-live`` string."""

    sessions: int
    turns: int
    session_turns: int
    output_tokens: int
    session_output_tokens: int
    session_seconds: int
    paid_retries: int
    role_turns: Mapping[str, int] = dataclasses.field(default_factory=dict)
    models: Mapping[str, str] = dataclasses.field(default_factory=dict)

    def turns_for(self, role: str) -> int:
        return min(self.session_turns, self.role_turns.get(role, self.session_turns))

    def canonical(self) -> str:
        parts = [
            f"sessions={self.sessions}",
            f"turns={self.turns}",
            f"session-turns={self.session_turns}",
            *(f"session-turns.{role}={self.role_turns[role]}" for role in sorted(self.role_turns)),
            f"output-tokens={self.output_tokens}",
            f"session-output-tokens={self.session_output_tokens}",
            f"session-seconds={self.session_seconds}",
            f"paid-retries={self.paid_retries}",
            *(f"model.{role}={self.models[role]}" for role in sorted(self.models)),
        ]
        return ",".join(parts)

    def as_json(self) -> dict[str, Any]:
        return {
            "sessions": self.sessions,
            "turns": self.turns,
            "session_turns": self.session_turns,
            "output_tokens": self.output_tokens,
            "session_output_tokens": self.session_output_tokens,
            "session_seconds": self.session_seconds,
            "paid_retries": self.paid_retries,
            "role_turns": dict(sorted(self.role_turns.items())),
            "models": dict(sorted(self.models.items())),
        }

    @classmethod
    def from_json(cls, value: Any) -> Ceiling | None:
        """The recorded ceiling, or None when the record is missing or malformed."""

        if not isinstance(value, dict):
            return None
        try:
            ceiling = cls(
                sessions=_whole(value["sessions"]),
                turns=_whole(value["turns"]),
                session_turns=_whole(value["session_turns"]),
                output_tokens=_whole(value["output_tokens"]),
                session_output_tokens=_whole(value["session_output_tokens"]),
                session_seconds=_whole(value["session_seconds"]),
                paid_retries=_whole(value["paid_retries"]),
                role_turns={str(k): _whole(v) for k, v in dict(value["role_turns"]).items()},
                models={str(k): str(v) for k, v in dict(value["models"]).items()},
            )
        except (KeyError, TypeError, ValueError):
            return None
        return ceiling


def _whole(value: Any) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("not a whole number")
    return value


def parse_ceiling(text: str | None) -> Ceiling:
    """Parse and bound the exact approval string; refuse anything not canonical.

    Required keys appear once each; ``session-turns.<role>`` and
    ``model.<role>`` are optional per role. Every value is at most the approved
    maximum, ``paid-retries`` is exactly 0, and the string must equal its own
    canonical rendering, so the recorded ceiling is byte for byte what was
    approved.
    """

    if not text:
        raise Refusal("live_unapproved", "--approve-live")
    values: dict[str, str] = {}
    for part in text.split(","):
        key, sep, raw = part.partition("=")
        if not sep or not key or key in values:
            raise Refusal("live_unapproved", "--approve-live")
        values[key] = raw
    numbers: dict[str, int] = {}
    role_turns: dict[str, int] = {}
    models: dict[str, str] = {}
    for key, raw in values.items():
        base, dot, role = key.partition(".")
        if dot:
            if role not in ROLES or base not in {"session-turns", "model"}:
                raise Refusal("live_unapproved", f"--approve-live key {key}")
            if base == "model":
                if not _MODEL.fullmatch(raw):
                    raise Refusal("live_unapproved", f"--approve-live {key}")
                models[role] = raw
                continue
            if not _INT.fullmatch(raw) or not 0 < int(raw) <= APPROVED_MAXIMUM["session-turns"]:
                raise Refusal("live_unapproved", f"--approve-live {key}")
            role_turns[role] = int(raw)
            continue
        if base not in CEILING_KEYS or not _INT.fullmatch(raw):
            raise Refusal("live_unapproved", f"--approve-live key {key}")
        number = int(raw)
        maximum = APPROVED_MAXIMUM[base]
        if base == "paid-retries":
            if number != 0:
                raise Refusal("live_unapproved", "--approve-live paid-retries must be 0")
        elif not 0 < number <= maximum:
            raise Refusal("live_unapproved", f"--approve-live {base} above the approval")
        numbers[base] = number
    missing = [key for key in CEILING_KEYS if key not in numbers]
    if missing:
        raise Refusal("live_unapproved", "--approve-live lacks " + ", ".join(missing))
    ceiling = Ceiling(
        sessions=numbers["sessions"],
        turns=numbers["turns"],
        session_turns=numbers["session-turns"],
        output_tokens=numbers["output-tokens"],
        session_output_tokens=numbers["session-output-tokens"],
        session_seconds=numbers["session-seconds"],
        paid_retries=numbers["paid-retries"],
        role_turns=role_turns,
        models=models,
    )
    if any(value > ceiling.session_turns for value in role_turns.values()):
        raise Refusal("live_unapproved", "--approve-live session-turns.<role> above session-turns")
    if ceiling.canonical() != text:
        raise Refusal("live_unapproved", "--approve-live is not in canonical order")
    return ceiling


# The one attempt's reserved native sessions, one slot per start. The implementer
# runs once and the kernel's publish continuation is a second implementer-role
# native session with its own slot. The bundle runs its plan review once and its
# diff review in a loop; one diff re-review round is reserved, and the bundle's
# reservation gate refuses any reviewer start beyond these.
ATTEMPT_ROLES = (IMPLEMENTER, IMPLEMENTER, PLAN_REVIEWER, DIFF_REVIEWER, DIFF_REVIEWER)


# The full model ID behind the bundle's reviewer agents' ``opus`` alias. Personal
# model authentication refuses a session unless the agent names an explicit
# Claude reviewer model (runner/src/curie_runner/personal_auth.py,
# require_claude_reviewer_model), and the alias is not one.
REVIEWER_MODEL = "claude-opus-5-5"


def reviewer_model(ceiling: Ceiling | None) -> str:
    """The explicit reviewer model the agent carries, refused when the approval disagrees. Pure.

    The approved ceiling's ``model.plan-reviewer`` and ``model.diff-reviewer``
    must name one model; without a ceiling (a rehearsal) or without either key
    the bundle's reviewer model applies.
    """

    if ceiling is None:
        return REVIEWER_MODEL
    named = {
        ceiling.models[role] for role in (PLAN_REVIEWER, DIFF_REVIEWER) if role in ceiling.models
    }
    if len(named) > 1:
        raise Refusal("live_unapproved", "--approve-live reviewer models differ")
    return named.pop() if named else REVIEWER_MODEL


def attempt_roles(ceiling: Ceiling) -> tuple[str, ...]:
    """The attempt's reserved roles, refused when the approved sessions cannot cover them."""

    if ceiling.sessions < len(ATTEMPT_ROLES):
        raise Refusal("live_unapproved", "--approve-live sessions cannot cover the factory roles")
    return ATTEMPT_ROLES


@dataclasses.dataclass(frozen=True)
class Share:
    output_tokens: int
    turns: int
    seconds: float


def reservation_shares(
    ceiling: Ceiling, roles: Sequence[str], *, used_turns: int = 0, used_tokens: int = 0
) -> list[Share]:
    """Each reserved session's share of what remains, capped per session. Pure.

    The first implementer slot is the main implementer session, which #5540
    recorded at 11 to 14 turns, so it takes up to half of what remains. Every
    other slot, the publish continuation's implementer slot included, splits
    the rest equally with the reviewers. Nothing exceeds the per-session caps,
    and the shares never sum past the remaining totals.
    """

    if not roles:
        raise Refusal("arguments_invalid", "roles")
    tokens_left = ceiling.output_tokens - used_tokens
    turns_left = ceiling.turns - used_turns
    lead = roles.index(IMPLEMENTER) if IMPLEMENTER in roles else None
    others = len(roles) - (0 if lead is None else 1)
    lead_tokens = lead_turns = 0
    if lead is not None:
        divisor = 2 if others else 1
        lead_tokens = min(ceiling.session_output_tokens, tokens_left // divisor)
        lead_turns = min(ceiling.turns_for(IMPLEMENTER), turns_left // divisor)
    rest_tokens = (tokens_left - lead_tokens) // others if others else 0
    rest_turns = (turns_left - lead_turns) // others if others else 0
    shares = []
    for index, role in enumerate(roles):
        if index == lead:
            shares.append(Share(lead_tokens, lead_turns, float(ceiling.session_seconds)))
        else:
            shares.append(
                Share(
                    min(ceiling.session_output_tokens, rest_tokens),
                    min(ceiling.turns_for(role), rest_turns),
                    float(ceiling.session_seconds),
                )
            )
    if any(share.turns < 1 for share in shares):
        raise Refusal("allowance_exhausted", "turns")
    if any(share.output_tokens < 1 for share in shares):
        raise Refusal("allowance_exhausted", "output tokens")
    return shares


def reviewer_limits(roles: Sequence[str]) -> dict[str, int]:
    """The bundle reservation gate's per-kind start limits for one attempt's roles.

    Only reviewer starts pass the gate, so the implementer slots (the main
    session and the publish continuation) never change these limits.
    """

    return {"plan": list(roles).count(PLAN_REVIEWER), "diff": list(roles).count(DIFF_REVIEWER)}


# --------------------------------------------------------------------------
# Private input files
# --------------------------------------------------------------------------

_LINE = re.compile(rb"[\x21-\x7e]{1,4096}")
_CHANNEL = re.compile(r"[CG][A-Z0-9]{8,20}")
_TEAM = re.compile(r"T[A-Z0-9]{2,20}")
_USER = re.compile(r"[UW][A-Z0-9]{2,20}")
_BOT = re.compile(r"B[A-Z0-9]{2,20}")
_TS = re.compile(r"[0-9]{9,12}\.[0-9]{6}")
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def _read_private(path: Path | None, *, flag: str, code: str, limit: int) -> bytes:
    if path is None or not str(path):
        raise Refusal(code, flag)
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        raise Refusal(code, flag) from None
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or not 0 < info.st_size <= limit
        ):
            raise Refusal(code, flag)
        return os.read(descriptor, limit + 1)
    finally:
        os.close(descriptor)


def load_token(path: Path | None, *, flag: str, prefix: str) -> HeldSecret:
    """One private one-line token file with the provider's prefix, as a held secret."""

    data = _read_private(path, flag=flag, code="slack_kit_invalid", limit=4097)
    line = data[:-1] if data.endswith(b"\n") else data
    if not _LINE.fullmatch(line) or not line.startswith(prefix.encode()):
        raise Refusal("slack_kit_invalid", flag)
    return HeldSecret(line.decode("ascii"))


def check_token_file(path: Path | None, *, flag: str, prefix: str) -> None:
    """Preflight form of ``load_token``: validate the file and keep nothing."""

    load_token(path, flag=flag, prefix=prefix).clear()


def load_private_json(path: Path | None, *, flag: str, code: str) -> dict[str, Any]:
    data = _read_private(path, flag=flag, code=code, limit=65536)
    try:
        value = json.loads(data)
    except ValueError:
        raise Refusal(code, flag) from None
    if not isinstance(value, dict):
        raise Refusal(code, flag)
    return value


def load_channel(path: Path | None) -> str:
    """The testing channel id from its private JSON file: ``{"channel_id": "C..."}``."""

    value = load_private_json(path, flag="--channel-file", code="slack_kit_invalid")
    channel = value.get("channel_id")
    if (
        set(value) != {"channel_id"}
        or not isinstance(channel, str)
        or not _CHANNEL.fullmatch(channel)
    ):
        raise Refusal("slack_kit_invalid", "--channel-file")
    return channel


@dataclasses.dataclass(frozen=True)
class Isolation:
    """The maintainer's private test-app isolation confirmation (Precondition 4)."""

    team_id: str
    curie_bot_user_id: str


def load_isolation(path: Path | None) -> Isolation:
    """A private JSON confirmation naming this app's bot user and workspace.

    ``{"confirmed": true, "team_id": "T...", "curie_bot_user_id": "U..."}``.
    It binds the confirmation to one app identity: a token for any other bot
    user or workspace fails ``isolation_matches`` before any connection.
    """

    value = load_private_json(path, flag="--isolation-file", code="isolation_unconfirmed")
    team = value.get("team_id")
    user = value.get("curie_bot_user_id")
    if (
        value.get("confirmed") is not True
        or set(value) != {"confirmed", "team_id", "curie_bot_user_id"}
        or not isinstance(team, str)
        or not _TEAM.fullmatch(team)
        or not isinstance(user, str)
        or not _USER.fullmatch(user)
    ):
        raise Refusal("isolation_unconfirmed", "--isolation-file")
    return Isolation(team, user)


def require_registry_port(raw: str) -> int:
    if not (raw or "").isdigit() or not 1024 <= int(raw) < 32768:
        raise Refusal("registry_port_invalid", "--registry-port")
    return int(raw)


def require_slack_id(value: Any, kind: str) -> str:
    """A Slack id of one kind, validated before it reaches SQL, Helm values or argv."""

    pattern = {"channel": _CHANNEL, "team": _TEAM, "user": _USER, "bot": _BOT, "ts": _TS}[kind]
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise Refusal("slack_unavailable", f"malformed {kind} id")
    return value


def sql_uuid(value: Any) -> str:
    """A canonical UUID string, the only shape an identifier takes inside driver SQL."""

    text = str(value or "").lower()
    if not _UUID.fullmatch(text):
        raise ValueError("not a UUID")
    return text


# --------------------------------------------------------------------------
# Slack Web API
# --------------------------------------------------------------------------
#
# Shapes follow https://docs.slack.dev/reference/methods/auth.test ,
# .../apps.connections.open, .../conversations.info, .../chat.postMessage,
# .../conversations.replies, .../conversations.history and .../chat.delete:
# every answer is a JSON object with ``ok`` and, on failure, an ``error`` code.

SLACK_HOST = "slack.com"
SlackTransport = Callable[[str, Mapping[str, str], bytes], tuple[int, Mapping[str, str], bytes]]
_ERROR_CODE = re.compile(r"[a-z][a-z0-9_]{0,63}")


class SlackError(Exception):
    """A Slack method answered not ok. Carries the method and the provider code only."""

    def __init__(self, method: str, code: str) -> None:
        super().__init__(f"{method} {code}")
        self.method = method
        self.code = code


def slack_error_code(body: Any) -> str:
    raw = body.get("error") if isinstance(body, dict) else None
    return raw if isinstance(raw, str) and _ERROR_CODE.fullmatch(raw) else "unreadable"


@dataclasses.dataclass(frozen=True)
class SlackAuth:
    team_id: str
    user_id: str
    bot_id: str


def auth_identity(body: Any) -> SlackAuth | None:
    """``auth.test`` of a bot token: team, bot user and bot id, else None."""

    if not isinstance(body, dict) or body.get("ok") is not True:
        return None
    team, user, bot = body.get("team_id"), body.get("user_id"), body.get("bot_id")
    if not (
        isinstance(team, str)
        and _TEAM.fullmatch(team)
        and isinstance(user, str)
        and _USER.fullmatch(user)
        and isinstance(bot, str)
        and _BOT.fullmatch(bot)
    ):
        return None
    return SlackAuth(team, user, bot)


class SlackWeb:
    """One Slack token's Web API calls through a substitutable transport."""

    def __init__(
        self,
        transport: SlackTransport,
        token: HeldSecret,
        scan: TokenScan,
        *,
        wait: Callable[[float], None],
        max_retries: int = 4,
    ) -> None:
        self.transport = transport
        self.token = token
        self.scan = scan
        self.wait = wait
        self.max_retries = max_retries

    def __repr__(self) -> str:
        return "<slack web>"

    def call(self, method: str, params: Mapping[str, str], *, sensitive: bool = False) -> Any:
        body = urllib.parse.urlencode(dict(params)).encode()
        headers = {
            "Authorization": f"Bearer {self.token.reveal()}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        for _attempt in range(self.max_retries + 1):
            status, response_headers, raw = self.transport(f"/api/{method}", headers, body)
            if not sensitive:
                self.scan.scan("http", raw)
            if status == 429:
                retry = str(response_headers.get("retry-after") or "1")
                self.wait(min(30.0, float(retry) if retry.isdigit() else 1.0))
                continue
            try:
                value = json.loads(raw)
            except ValueError:
                raise SlackError(method, f"http_{status}") from None
            if not isinstance(value, dict) or value.get("ok") is not True:
                raise SlackError(method, slack_error_code(value))
            return value
        raise SlackError(method, "rate_limited")

    def auth(self) -> SlackAuth:
        identity = auth_identity(self.call("auth.test", {}))
        if identity is None:
            raise SlackError("auth.test", "not_a_bot_token")
        return identity

    def connections_open(self) -> bool:
        """``apps.connections.open`` for an app-level token. The URL is never kept or dialed.

        The answer is not scanned: its single-use connection URL carries a
        ticket, and only its presence is read.
        """

        value = self.call("apps.connections.open", {}, sensitive=True)
        return isinstance(value.get("url"), str)

    def is_member(self, channel: str) -> bool:
        value = self.call("conversations.info", {"channel": channel})
        info = value.get("channel")
        return isinstance(info, dict) and info.get("is_member") is True

    def post(self, channel: str, text: str, *, thread_ts: str | None = None) -> str:
        params = {"channel": channel, "text": text, "unfurl_links": "false"}
        if thread_ts:
            params["thread_ts"] = thread_ts
        value = self.call("chat.postMessage", params)
        return require_slack_id(value.get("ts"), "ts")

    def replies(self, channel: str, ts: str) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []
        cursor = ""
        for _page in range(20):
            params = {"channel": channel, "ts": ts, "limit": "200"}
            if cursor:
                params["cursor"] = cursor
            value = self.call("conversations.replies", params)
            messages += [item for item in value.get("messages") or [] if isinstance(item, dict)]
            meta = value.get("response_metadata")
            cursor = str(meta.get("next_cursor") or "") if isinstance(meta, dict) else ""
            if not cursor:
                break
        return messages

    def message_present(self, channel: str, ts: str) -> bool:
        value = self.call(
            "conversations.history",
            {"channel": channel, "latest": ts, "oldest": ts, "inclusive": "true", "limit": "1"},
        )
        return any(
            isinstance(item, dict) and item.get("ts") == ts for item in value.get("messages") or []
        )

    def delete(self, channel: str, ts: str) -> None:
        try:
            self.call("chat.delete", {"channel": channel, "ts": ts})
        except SlackError as error:
            if error.code != "message_not_found":
                raise


def kickoff_text(curie_user: str, issue_url: str) -> str:
    """The declared driver's kickoff mention: the bot mention, the mark, the link."""

    return f"<@{curie_user}> {TEST_MARK} {issue_url}"


def ping_text(curie_user: str) -> str:
    return f"<@{curie_user}> {TEST_MARK} ping"


_OUTCOME = re.compile(r"Outcome: ([a-z][a-z0-9_]{0,63})")


def message_text(message: Mapping[str, Any]) -> str:
    """A message's searchable text: ``text`` plus every string inside its blocks."""

    parts = [str(message.get("text") or "")]

    def walk(value: Any) -> None:
        if isinstance(value, str):
            parts.append(value)
        elif isinstance(value, dict):
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(message.get("blocks"))
    return "\n".join(parts)


def reply_outcome(messages: Iterable[Mapping[str, Any]], *, author: str) -> str | None:
    """The outcome code the first reply by ``author`` names (``Outcome: <code>``)."""

    for message in messages:
        if message.get("user") != author:
            continue
        found = _OUTCOME.search(message_text(message))
        if found:
            return found.group(1)
    return None


def in_thread(message: Mapping[str, Any], root_ts: str) -> bool:
    """Whether ``conversations.replies`` returned this message as part of ``root_ts``'s thread.

    The parent itself has ``ts == root_ts``; a reply carries ``thread_ts == root_ts``.
    A message naming another thread is not in this one, whatever its ts.
    """

    ts, thread = message.get("ts"), message.get("thread_ts")
    if ts == root_ts:
        return thread in (None, root_ts)
    return thread == root_ts


def thread_digests(messages: Iterable[Mapping[str, Any]], root_ts: str) -> list[str]:
    return sorted(
        ref_digest(str(message["ts"]))
        for message in messages
        if isinstance(message.get("ts"), str) and in_thread(message, root_ts)
    )


def foreign_messages(messages: Iterable[Mapping[str, Any]], root_ts: str) -> int:
    return sum(1 for message in messages if not in_thread(message, root_ts))


def posts_in_thread(
    posted_refs: Sequence[str | None], messages: Sequence[Mapping[str, Any]], root_ts: str
) -> bool:
    """Every posted reference is a message of ``root_ts``'s own thread. Pure."""

    inside = set(thread_digests(messages, root_ts))
    return bool(posted_refs) and all(
        isinstance(ref, str) and ref_digest(ref) in inside for ref in posted_refs
    )


def ping_answered(
    messages: Iterable[Mapping[str, Any]], *, curie_user: str, sender_user: str, root_ts: str
) -> bool:
    """The dispatcher's in-thread answer to a declared driver's ping names that driver."""

    mention = f"<@{sender_user}>"
    return any(
        message.get("user") == curie_user
        and message.get("ts") != root_ts
        and in_thread(message, root_ts)
        and mention in message_text(message)
        for message in messages
    )


# --------------------------------------------------------------------------
# Socket Mode receipt and dispatcher log
# --------------------------------------------------------------------------


def parse_receipt(raw: bytes | str | None) -> SlackMentionReceipt | None:
    """A stored receipt through the shared model the dispatcher writes with, else None."""

    if raw is None:
        return None
    try:
        return SlackMentionReceipt.model_validate_json(raw)
    except ValueError:
        return None


def receipt_checks(
    receipt: SlackMentionReceipt | None, *, channel: str, message_ts: str, sender: SlackAuth
) -> dict[str, bool]:
    """A stored mention receipt judged against the post the driver made. Pure.

    A declared driver's receipt is version 2 naming the declared bot user and
    bot id (``curie_internal.slack_mention_receipt``); the body is never read.
    """

    if receipt is None:
        return {"receipt_readable": False}
    return {
        "receipt_readable": True,
        "version_2": receipt.version == 2,
        "channel": receipt.channel == channel,
        "root_is_message": receipt.root_ts == message_ts == receipt.message_ts,
        "user_is_sender": receipt.user_id == sender.user_id,
        "evidence_user_is_sender": receipt.evidence.get("user_id") == sender.user_id,
        "driver_bot_is_sender": receipt.driver_bot_id == sender.bot_id,
        "identity_default": receipt.channel_identity == DEFAULT_IDENTITY,
    }


def find_receipt(
    values: Mapping[str, bytes], *, channel: str, message_ts: str
) -> SlackMentionReceipt | None:
    """The stored receipt whose channel and message ts are this post's, under its own key."""

    for key, raw in sorted(values.items()):
        receipt = parse_receipt(raw)
        if (
            receipt is not None
            and receipt.channel == channel
            and receipt.message_ts == message_ts
            and key == receipt_key(receipt.delivery_id)
        ):
            return receipt
    return None


def enqueue_logged(log: bytes, delivery_id: str) -> bool:
    """The dispatcher logged ``enqueued <kind> <delivery> as stream entry <id>``.

    packages/channel-gateway/src/curie_channel_gateway/slack/handlers.py
    ``_mint_turn``: ``"enqueued %s %s as stream entry %s identity=%s ..."``.
    """

    pattern = re.compile(
        rb"enqueued slack event "
        + re.escape(delivery_id.encode())
        + rb" as stream entry [0-9]+-[0-9]+"
    )
    return pattern.search(log) is not None


# --------------------------------------------------------------------------
# Owner, claim and runner pod facts
# --------------------------------------------------------------------------


def owner_tuple(row: Any) -> dict[str, Any] | None:
    """(tenant, mode, principal, generation) from a work item or request row."""

    if not isinstance(row, dict):
        return None
    return {
        "tenant_id": row.get("tenant_id"),
        "mode": row.get("model_auth_mode"),
        "principal_id": row.get("owner_principal_id"),
        "generation": row.get("enrollment_generation"),
    }


def runner_token_ref(pod: Any) -> tuple[str, str] | None:
    """The Secret and key the runner container's grant variable reads, if any."""

    spec = pod.get("spec") if isinstance(pod, dict) else None
    for container in (spec or {}).get("containers") or []:
        if not isinstance(container, dict) or container.get("name") != "runner":
            continue
        for entry in container.get("env") or []:
            if not isinstance(entry, dict) or entry.get("name") != GRANT_KEY:
                continue
            if "value" in entry:
                return None
            ref = (entry.get("valueFrom") or {}).get("secretKeyRef") or {}
            if isinstance(ref, dict) and ref.get("name") and ref.get("key"):
                return str(ref["name"]), str(ref["key"])
    return None


def pod_grant_checks(pod: Any, claim: Any, owner: Mapping[str, Any]) -> dict[str, bool]:
    """The runner reads the grant from ``<claim>-tokens`` and the claim names the owner."""

    claim_name = str(((claim or {}).get("metadata") or {}).get("name") or "")
    labels = ((claim or {}).get("metadata") or {}).get("labels") or {}
    return {
        "grant_from_claim_tokens": bool(claim_name)
        and runner_token_ref(pod) == (f"{claim_name}{CLAIM_TOKENS_SUFFIX}", GRANT_KEY),
        "claim_owner_labels": labels.get(OWNER_TENANT_LABEL) == owner.get("tenant_id")
        and labels.get(OWNER_PRINCIPAL_LABEL) == owner.get("principal_id")
        and labels.get(GENERATION_LABEL) == str(owner.get("generation")),
    }


def candidate_labels(candidate: str, tag: str) -> list[str]:
    """The labels on every image a run builds: the candidate revision and the run's tag. Pure.

    The run's tag makes each run's image config unique. A node that already holds an
    identical image from an earlier run reports that run's pushed digest as the
    running image ID, so a rebuild at the same revision without it fails the exact
    running-digest check even though the pod runs the right bytes.
    """

    return [f"{REVISION_LABEL}={candidate}", f"{RUN_LABEL}={tag}"]


def run_layer_dockerfile(text: str, tag: str) -> str:
    """The runner layer's Dockerfile with this run's label appended, as candidate_labels. Pure."""

    return f"{text.rstrip(chr(10))}\nLABEL {RUN_LABEL}={tag}\n"


def runner_started_at(pod: Any) -> float | None:
    """When the runner container started running, from a pod record, if it reports one.

    Reads ``status.containerStatuses[name=runner].state.running.startedAt``
    (https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/).
    Pure.
    """

    status = pod.get("status") if isinstance(pod, dict) else None
    for item in (status or {}).get("containerStatuses") or []:
        if not isinstance(item, dict) or item.get("name") != "runner":
            continue
        running = (item.get("state") or {}).get("running")
        stamp = running.get("startedAt") if isinstance(running, dict) else None
        if not isinstance(stamp, str):
            return None
        try:
            parsed = dt.datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed.timestamp() if parsed.tzinfo is not None else None
    return None


def container_terminations(pod: Any) -> list[dict[str, Any]]:
    """Every terminated container of a pod record, init and regular, current and last. Pure.

    Reads ``status.initContainerStatuses`` and ``status.containerStatuses``, each
    entry's ``state.terminated`` and ``lastState.terminated``, and keeps the
    container's name with ``exitCode``, ``reason`` and ``signal``
    (https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#PodStatus).
    The free-text ``message`` is never kept.
    """

    status = pod.get("status") if isinstance(pod, dict) else None
    found: list[dict[str, Any]] = []
    for field, init in (("initContainerStatuses", True), ("containerStatuses", False)):
        for item in (status or {}).get(field) or []:
            if not isinstance(item, dict):
                continue
            for source in ("state", "lastState"):
                terminated = (item.get(source) or {}).get("terminated")
                if not isinstance(terminated, dict):
                    continue
                code, signal = terminated.get("exitCode"), terminated.get("signal")
                reason = terminated.get("reason")
                found.append(
                    {
                        "container": str(item.get("name") or ""),
                        "init": init,
                        "from": source,
                        "exitCode": code if type(code) is int else None,
                        "reason": reason if isinstance(reason, str) else None,
                        "signal": signal if type(signal) is int else None,
                    }
                )
    return found


# A worker log line that can explain an attempt's end, besides one naming the attempt.
_ESCALATION_LINE = re.compile(r"(?i:escalat|refus)|RunnerError|Traceback|ERROR|WARNING")


def escalation_lines(
    log: bytes, ids: Sequence[str], scrub: Callable[[str], str], *, limit: int
) -> list[str]:
    """The latest ``limit`` worker log lines naming an id or an escalation marker. Pure.

    A line is kept when it mentions any of ``ids`` (the attempt's work item and
    request) or matches ``escalat``, ``RunnerError``, ``refus``, ``Traceback``,
    ``ERROR`` or ``WARNING``; each kept line passes through ``scrub``.
    """

    kept = [
        line
        for line in log.decode(errors="replace").splitlines()
        if any(item and item in line for item in ids) or _ESCALATION_LINE.search(line)
    ]
    return [scrub(line) for line in kept[-limit:]] if limit > 0 else []


# --------------------------------------------------------------------------
# Native usage: slots, the watchdog and settlement
# --------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Reservation:
    seq: int
    role: str
    output_tokens: int
    turns: int
    seconds: float


def wall_seconds(session: Any) -> float:
    """A native session's wall clock: its first record to its last. Pure.

    The shared parser's ``elapsed`` leaves out the time an implementer waited
    on its nested reviewers, but the approved seconds bound is absolute, so
    this module bounds and records wall clock and never that figure.
    """

    if session.first is None or session.last is None:
        return 0.0
    return max(0.0, float(session.last) - float(session.first))


def session_record(session: Any) -> dict[str, Any]:
    """The sanitized per-session fact the ledger keeps, its ``elapsed`` wall clock. Pure."""

    return {**session_summary(session), "elapsed": round(wall_seconds(session), 3)}


@dataclasses.dataclass
class SlotUsage:
    """One reserved slot's observed usage: the one native session charged to it.

    ``elapsed`` is the session's wall clock (``wall_seconds``). ``add`` also
    builds the usage of one unreserved session for settlement.
    """

    output_tokens: int = 0
    assistant_turns: int = 0
    elapsed: float = 0.0
    models: tuple[str, ...] = ()
    sessions: int = 0
    known: bool = True
    last: float | None = None

    def add(self, session: Any) -> None:
        self.output_tokens += int(session.output_tokens)
        self.assistant_turns += int(session.assistant_turns)
        self.elapsed = max(self.elapsed, wall_seconds(session))
        self.models = tuple(sorted({*self.models, *session.models}))
        self.sessions += 1
        self.known = self.known and bool(session.usage_known)
        if session.last is not None:
            self.last = session.last if self.last is None else max(self.last, session.last)


def match_slots(
    reservations: Sequence[Reservation], sessions: Sequence[Any]
) -> tuple[dict[int, SlotUsage], list[Any]]:
    """Charge observed sessions to reservations; return the slots and unreserved sessions.

    Every native start takes its own slot: in order of first activity, each
    implementer-role session (the main session, then the publish
    continuation) takes the next implementer slot, and each reviewer subagent
    session the next reviewer slot of its role. A session with no slot left,
    a third implementer session included, is unreserved.
    """

    slots = {item.seq: SlotUsage() for item in reservations}
    pending = {role: [item for item in reservations if item.role == role] for role in ROLES}
    unreserved: list[Any] = []
    for session in sorted(sessions, key=lambda item: (item.first is None, item.first or 0.0)):
        queue = pending.get(session.role)
        if not queue:
            unreserved.append(session)
            continue
        slots[queue.pop(0).seq].add(session)
    return slots, unreserved


def watchdog_breaches(
    reservations: Sequence[Reservation],
    sessions: Sequence[Any],
    ceiling: Ceiling,
    *,
    now: float,
    running: bool,
) -> list[str]:
    """Bounds reached after the latest response: output tokens first, then turns, then seconds.

    The bounds are the approved ceiling's own (coordinator ruling 1): total
    output tokens and turns across every session, each native session's
    output tokens and turns against the per-session caps (each takes its own
    slot), and the absolute wall clock of each native session. The most
    recently active session is charged the time since its last record, and
    while a native process runs (``running``) the implementer main session
    with the latest first record is live and charged ``now`` minus its first
    record, so waiting on a reviewer never pauses its bound; a subagent starts
    after and ends before its parent, so that bound covers it too.

    A session with no assistant record yet is pending and gets only the
    seconds bound. A session whose complete responses lack usage is unknown,
    a session no reservation covers is unreserved, and more native sessions
    than the ceiling allows exceed it; each ends the attempt, and an in-flight
    unterminated final record is none of these. Pure.
    """

    slots, unreserved = match_slots(reservations, sessions)
    by_seq = {item.seq: item for item in reservations}
    latest = max((item.last for item in sessions if item.last is not None), default=None)
    live: Any = None
    if running:
        mains = [item for item in sessions if item.role == IMPLEMENTER and item.first is not None]
        live = max(mains, key=lambda item: float(item.first), default=None)
    tokens: list[str] = []
    turns: list[str] = []
    seconds: list[str] = []
    if sum(int(item.output_tokens) for item in sessions) >= ceiling.output_tokens:
        tokens.append("output_tokens:total")
    if sum(int(item.assistant_turns) for item in sessions) >= ceiling.turns:
        turns.append("turns:total")
    for seq, slot in sorted(slots.items()):
        role = by_seq[seq].role
        if slot.output_tokens >= ceiling.session_output_tokens:
            tokens.append(f"output_tokens:{role}")
        if slot.assistant_turns >= ceiling.turns_for(role):
            turns.append(f"turns:{role}")
    for item in sessions:
        elapsed = wall_seconds(item)
        if latest is not None and item.last == latest:
            elapsed += max(0.0, now - latest)
        if item is live:
            elapsed = max(elapsed, now - float(item.first))
        if elapsed >= ceiling.session_seconds:
            seconds.append(f"seconds:{item.role}")
    unknown = sorted(
        {
            f"usage_unknown:{item.role}"
            for item in sessions
            if not item.usage_known and not item.truncated and int(item.assistant_turns) > 0
        }
    )
    count = ["sessions:total"] if len(sessions) > ceiling.sessions else []
    extra = sorted({f"unreserved:{item.role}" for item in unreserved})
    return [*tokens, *turns, *sorted(set(seconds)), *unknown, *count, *extra]


@dataclasses.dataclass
class ProcessWatch:
    """The native CLI's lifetime from one harvester stream, for the watchdog's deadline.

    Reads the harvester's ``process`` records. The runner holds one long-lived
    session, so its CLI lives until the pod is deleted; settlement reads the
    pod's ``CaptureTrace`` instead.

    ``started`` is the wall-clock time the running native process started: the
    caller's ``now`` when the process count went from 0 to above 0, and for
    the pod's first process no later than the runner container's start
    (``container_started``), since the stream can report it late.
    """

    ran: bool = False
    exited: bool = False
    container_started: float | None = None
    started: float | None = None
    count: int = 0

    @property
    def running(self) -> bool:
        return self.ran and not self.exited

    def deadline(self, seconds: float) -> float | None:
        """When the running native process reaches ``seconds``; None while none runs."""

        if not self.running or self.started is None:
            return None
        return self.started + seconds

    def feed(self, line: bytes, *, now: float) -> None:
        try:
            record = json.loads(line)
        except ValueError:
            return
        if not isinstance(record, dict):
            return
        kind = record.get("kind")
        if kind == "process" and type(record.get("count")) is int:
            count = int(record["count"])
            if count > 0:
                if self.count <= 0:
                    start = now
                    if not self.ran and self.container_started is not None:
                        start = min(start, self.container_started)
                    self.started = start
                self.ran, self.exited = True, False
            elif self.ran:
                self.exited = True
            self.count = count


# --------------------------------------------------------------------------
# Capture affirmation (coordinator rulings of 2026-11-13)
# --------------------------------------------------------------------------

# The harvester's report interval. It reports right after each full read pass,
# so two reports after the runner's turn end prove that a whole pass, started
# after the turn end, found nothing new.
CAPTURE_REPORT_SECONDS = 0.1
CAPTURE_REPORTS_AFTER_TURN_END = 2
# Stop reasons meaning the model stopped generating; anything else is incomplete.
FINAL_STOP_REASONS = frozenset({"end_turn", "stop_sequence", "max_tokens"})
_AGENT_TOOLS = frozenset({"Agent", "Task"})
_SYNTHETIC_MODEL = "<synthetic>"
_UNATTRIBUTED_AGENT = "<unattributed>"
# The runner's own turn records (curie_runner.session): "turn start session=..."
# and "turn end session=... status=... duration_ms=...".
_TURN_KINDS = (("turn start ", "start"), ("turn end ", "end"))
_TURN_START_SESSION = re.compile(r"^turn start session=(\S+)")


def _json_object(line: bytes) -> dict[str, Any] | None:
    try:
        value = json.loads(line)
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


@dataclasses.dataclass
class CaptureTrace:
    """What one runner pod's streams show about its capture. Guarded by the pod's lock.

    ``reports`` holds each harvester size report as (taken at, sizes by path) in
    stream order, ``turns`` each runner turn start or end as (kind, logged at) in
    log order, ``started`` each turn start as (session id, logged at), and
    ``harvester_ended`` whether the harvester stopped on its own deadline. The
    times come from the runner container's clock. ``sanitized`` leaves the
    session id out; the launched set reads ``started`` and keeps only a seq.
    """

    reports: list[tuple[float, dict[str, int]]] = dataclasses.field(default_factory=list)
    turns: list[tuple[str, float]] = dataclasses.field(default_factory=list)
    started: list[tuple[str, float]] = dataclasses.field(default_factory=list)
    harvester_ended: bool = False

    def feed_harvest(self, line: bytes) -> None:
        record = _json_object(line)
        if record is None:
            return
        if record.get("kind") == "end":
            self.harvester_ended = True
            return
        sizes, at = record.get("sizes"), record.get("at")
        if (
            record.get("kind") == "sync"
            and isinstance(sizes, dict)
            and all(isinstance(key, str) and type(value) is int for key, value in sizes.items())
            and isinstance(at, (int, float))
            and not isinstance(at, bool)
        ):
            self.reports.append((float(at), dict(sizes)))

    def feed_log(self, line: bytes) -> None:
        record = _json_object(line)
        if record is None:
            return
        message, stamp = record.get("message"), record.get("timestamp")
        if not isinstance(message, str) or not isinstance(stamp, str):
            return
        kind = next((name for prefix, name in _TURN_KINDS if message.startswith(prefix)), None)
        if kind is None:
            return
        try:
            moment = dt.datetime.fromisoformat(stamp)
        except ValueError:
            return
        if moment.tzinfo is None:
            return
        logged_at = moment.timestamp()
        if kind == "start":
            matched = _TURN_START_SESSION.match(message)
            if matched is not None:
                self.started.append((matched.group(1), logged_at))
        self.turns.append((kind, logged_at))

    def sanitized(self) -> dict[str, Any]:
        """Times and sizes only: each path becomes its digest, and identical consecutive
        reports fold into one run."""

        runs: list[dict[str, Any]] = []
        for at, sizes in self.reports:
            keyed = {ref_digest(path): size for path, size in sorted(sizes.items())}
            if runs and runs[-1]["sizes"] == keyed:
                runs[-1]["last_at"] = at
                runs[-1]["count"] += 1
            else:
                runs.append({"first_at": at, "last_at": at, "count": 1, "sizes": keyed})
        return {
            "reports": runs,
            "turns": [[kind, at] for kind, at in self.turns],
            "harvester_ended": self.harvester_ended,
        }


def capture_failures(
    trace: CaptureTrace,
    files: Mapping[str, bytes],
    *,
    delivered: bool,
    pod_gone: bool,
    request_terminal: bool,
    runner_pods: int,
) -> list[str]:
    """The codes of ``capture_findings``; empty when the capture is affirmed. Pure."""

    findings = capture_findings(
        trace,
        files,
        delivered=delivered,
        pod_gone=pod_gone,
        request_terminal=request_terminal,
        runner_pods=runner_pods,
    )
    return sorted({item["code"] for item in findings})


def _finding(
    code: str, field: str, value: Any = None, *, file: str | None = None
) -> dict[str, Any]:
    """One capture failure: its code, the file (a path digest) and the field it read."""

    return {"code": code, "file": file, "field": field, "value": value}


def capture_findings(
    trace: CaptureTrace,
    files: Mapping[str, bytes],
    *,
    delivered: bool,
    pod_gone: bool,
    request_terminal: bool,
    runner_pods: int,
) -> list[dict[str, Any]]:
    """Why an attempt's capture is not affirmed complete; empty when it is. Pure.

    The coordinator's capture rule of 2026-11-13, anchored on the runner's last
    turn-end record: a completed request turns terminal only after its pull
    request opens, which is after the pod and its harvester are gone. Each
    check reads recorded evidence:

    1. The attempt ran in exactly one runner pod, the runner logged a turn end,
       no turn started after it, and the request reached its terminal state.
    2. Every transcript, subagent transcripts included, ends in a complete
       result frame, every tool use has its result, and every subagent a result
       names has its own transcript (``transcript_failures``).
    3. At least two size reports were taken after the turn end and all of them
       match, so a full read pass started after the turn end found nothing new.
    4. No report drops a file an earlier one listed, the reassembled bytes equal
       the last report (``delivered``), the harvester never stopped on its own
       deadline, and the pod was deleted when the stream ended.

    The runner's turn-end record carries no usage, so the usage agreement check
    has nothing to compare. Any failure leaves every slot unknown.
    """

    found: list[dict[str, Any]] = []
    if runner_pods != 1:
        found.append(_finding("runner_pods", "runner_pods", runner_pods))
    if not request_terminal:
        found.append(_finding("request_not_terminal", "execution_requests.terminal_at"))
    ends = [at for kind, at in trace.turns if kind == "end"]
    if not ends:
        found.append(_finding("no_turn_end", "runner_log.turn_end"))
    else:
        anchor = ends[-1]
        if trace.turns[-1][0] != "end" or any(
            kind == "start" and at >= anchor for kind, at in trace.turns
        ):
            found.append(_finding("turn_started_after_turn_end", "runner_log.turn_start"))
        after = [sizes for at, sizes in trace.reports if at > anchor]
        if len(after) < CAPTURE_REPORTS_AFTER_TURN_END:
            found.append(_finding("reports_after_turn_end", "sync.at", len(after)))
        for path in sorted({path for sizes in after for path in sizes}):
            seen = [sizes.get(path) for sizes in after]
            if len(set(seen)) > 1:
                found.append(
                    _finding(
                        "growth_after_turn_end",
                        "sync.sizes",
                        [seen[0], seen[-1]],
                        file=ref_digest(path),
                    )
                )
    listed: set[str] = set()
    for _at, sizes in trace.reports:
        for path in sorted(listed - sizes.keys()):
            found.append(_finding("report_dropped_a_file", "sync.sizes", file=ref_digest(path)))
        listed |= sizes.keys()
    if trace.harvester_ended:
        found.append(_finding("harvester_deadline", "harvester.end"))
    if not delivered:
        found.append(_finding("bytes_not_delivered", "harvest.bytes"))
    if not pod_gone:
        found.append(_finding("pod_not_deleted", "pod.metadata.deletionTimestamp"))
    found.extend(transcript_findings(files))
    unique = {json.dumps(item, sort_keys=True): item for item in found}
    return [unique[key] for key in sorted(unique)]


def _transcript_records(data: bytes) -> tuple[list[dict[str, Any]], bool]:
    """A transcript's records, and whether every line, the last included, is whole."""

    *lines, tail = data.split(b"\n")
    records = [_json_object(line) for line in lines if line.strip()]
    kept = [record for record in records if record is not None]
    return kept, not tail and len(kept) == len(records)


def _items(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    message = record.get("message")
    content = message.get("content") if isinstance(message, dict) else None
    return [item for item in content if isinstance(item, dict)] if isinstance(content, list) else []


def _result_frame_gap(records: Sequence[Mapping[str, Any]]) -> tuple[str, Any] | None:
    """The field that keeps the last assistant record from being a complete result frame
    (the model stopped generating, with its usage), with the value it read; None when it is
    one."""

    replies = [record for record in records if record.get("type") == "assistant"]
    if not replies:
        return ("type", "no assistant record")
    message = replies[-1].get("message")
    if not isinstance(message, dict):
        return ("message", None)
    if message.get("stop_reason") not in FINAL_STOP_REASONS:
        return ("message.stop_reason", message.get("stop_reason", "absent"))
    if message.get("model") == _SYNTHETIC_MODEL:
        return ("message.model", _SYNTHETIC_MODEL)
    usage = message.get("usage")
    tokens = usage.get("output_tokens") if isinstance(usage, dict) else None
    if type(tokens) is not int or tokens < 0:
        return ("message.usage.output_tokens", tokens)
    return None


def _unanswered_tool_uses(records: Sequence[Mapping[str, Any]]) -> int:
    uses = {
        item.get("id")
        for record in records
        if record.get("type") == "assistant"
        for item in _items(record)
        if item.get("type") == "tool_use"
    }
    answered = {
        item.get("tool_use_id")
        for record in records
        if record.get("type") == "user"
        for item in _items(record)
        if item.get("type") == "tool_result"
    }
    return len(uses - answered)


def _started_subagents(records: Sequence[Mapping[str, Any]]) -> list[str]:
    """The agent id of every subagent tool use that ran; a refused start is an error."""

    agent_uses = {
        item.get("id")
        for record in records
        for item in _items(record)
        if item.get("type") == "tool_use" and item.get("name") in _AGENT_TOOLS
    }
    started: list[str] = []
    for record in records:
        for item in _items(record):
            if (
                item.get("type") != "tool_result"
                or item.get("tool_use_id") not in agent_uses
                or item.get("is_error") is True
            ):
                continue
            result = record.get("toolUseResult")
            agent = result.get("agentId") if isinstance(result, dict) else None
            started.append(agent if isinstance(agent, str) else _UNATTRIBUTED_AGENT)
    return started


def transcript_failures(files: Mapping[str, bytes]) -> list[str]:
    """The codes of ``transcript_findings``. Pure."""

    return sorted({item["code"] for item in transcript_findings(files)})


def transcript_findings(files: Mapping[str, bytes]) -> list[dict[str, Any]]:
    """Why the harvested transcripts are not whole, each naming its file (a path digest)
    and field: a cut line, a missing result frame, an unanswered tool use, or a started
    subagent with no transcript. Pure."""

    found: list[dict[str, Any]] = []
    mains: dict[str, list[dict[str, Any]]] = {}
    subagents: dict[str, list[dict[str, Any]]] = {}
    for name, data in files.items():
        parts = PurePosixPath(name).parts
        if "projects" not in parts or not name.endswith(".jsonl"):
            continue
        records, whole = _transcript_records(data)
        if not whole:
            found.append(_finding("transcript_truncated", "line", file=ref_digest(name)))
        if "subagents" in parts:
            subagents[name.removesuffix(".jsonl")] = records
        else:
            mains[name.removesuffix(".jsonl")] = [r for r in records if not r.get("isSidechain")]
    if not mains:
        found.append(_finding("no_main_transcript", "projects/*.jsonl"))
    for stem, records in (*mains.items(), *subagents.items()):
        file = ref_digest(stem + ".jsonl")
        gap = _result_frame_gap(records)
        if gap is not None:
            found.append(_finding("result_frame_missing", gap[0], gap[1], file=file))
        unanswered = _unanswered_tool_uses(records)
        if unanswered:
            found.append(
                _finding("tool_use_unanswered", "tool_result.tool_use_id", unanswered, file=file)
            )
    for stem, records in mains.items():
        for agent in _started_subagents(records):
            if f"{stem}/subagents/agent-{agent}" not in subagents:
                value = "absent" if agent == _UNATTRIBUTED_AGENT else "no subagent transcript"
                found.append(
                    _finding(
                        "subagent_transcript_missing",
                        "toolUseResult.agentId",
                        value,
                        file=ref_digest(stem + ".jsonl"),
                    )
                )
    return found


def transcript_skeleton(files: Mapping[str, bytes]) -> dict[str, dict[str, Any]]:
    """Each transcript's record shapes, with no content, keyed by path digest. Pure.

    For each record: its byte offset, type, subtype, stop reason, whether it carries
    usage and the usage numbers, its sidechain flag and its agent ids. This is the shape
    evidence a later capture rule is checked against.
    """

    skeleton: dict[str, dict[str, Any]] = {}
    for name, data in sorted(files.items()):
        parts = PurePosixPath(name).parts
        if "projects" not in parts or not name.endswith(".jsonl"):
            continue
        records: list[dict[str, Any]] = []
        offset = 0
        for line in data.split(b"\n"):
            record = _json_object(line) if line.strip() else None
            if line.strip():
                message = record.get("message") if record is not None else None
                message = message if isinstance(message, dict) else {}
                usage = message.get("usage")
                result = record.get("toolUseResult") if record is not None else None
                records.append(
                    {
                        "offset": offset,
                        "whole": record is not None,
                        "type": record.get("type") if record is not None else None,
                        "subtype": record.get("subtype") if record is not None else None,
                        "stop_reason": message.get("stop_reason", "absent"),
                        "has_usage": isinstance(usage, dict),
                        "usage": {
                            key: usage[key]
                            for key in sorted(usage)
                            if isinstance(usage, dict) and type(usage[key]) is int
                        }
                        if isinstance(usage, dict)
                        else None,
                        "is_sidechain": record.get("isSidechain") if record is not None else None,
                        "agent_id": record.get("agentId") if record is not None else None,
                        "result_agent_id": result.get("agentId")
                        if isinstance(result, dict)
                        else None,
                    }
                )
            offset += len(line) + 1
        skeleton[ref_digest(name)] = {
            "kind": "subagent" if "subagents" in parts else "main",
            "bytes": len(data),
            "records": records,
        }
    return skeleton


_TOOL_SEARCH = "ToolSearch"


def tool_search_calls(files: Mapping[str, bytes]) -> int:
    """ToolSearch tool uses in the native transcripts; each is an assistant turn charged
    like any other response, so the query allowance includes it."""

    count = 0
    for name, data in files.items():
        if not name.endswith(".jsonl"):
            continue
        for line in data.split(b"\n"):
            try:
                record = json.loads(line)
            except ValueError:
                continue
            message = record.get("message") if isinstance(record, dict) else None
            content = message.get("content") if isinstance(message, dict) else None
            for item in content if isinstance(content, list) else []:
                if (
                    isinstance(item, dict)
                    and item.get("type") == "tool_use"
                    and (item.get("name") == _TOOL_SEARCH)
                ):
                    count += 1
    return count


_API_STATUS = re.compile(r"\bAPI Error: (\d{3})\b")
_PROVIDER_TYPE = re.compile(r'"type"\s*:\s*"([a-z][a-z0-9_]{0,40}_error)"')
_AUTH_TYPES = frozenset({"authentication_error", "permission_error"})


def stderr_error_classes(lines: Iterable[bytes]) -> list[dict[str, Any]]:
    """Provider error classes the native CLI wrote to the runner's stderr. Status and type
    only; no provider text is kept."""

    found: set[tuple[int | None, str | None]] = set()
    for raw in lines:
        text = raw.decode(errors="replace")
        status = _API_STATUS.search(text)
        kind = _PROVIDER_TYPE.search(text)
        if status or kind:
            found.add((int(status.group(1)) if status else None, kind.group(1) if kind else None))
    return [
        {"http_status": status, "provider_error_type": kind}
        for status, kind in sorted(found, key=lambda item: (item[0] or 0, item[1] or ""))
    ]


def probe_turns(stdout: bytes) -> int | None:
    """``num_turns`` from the CLI's ``--output-format json`` result, when it is a count."""

    try:
        body = json.loads(stdout)
    except ValueError:
        return None
    value = body.get("num_turns") if isinstance(body, dict) else None
    return value if type(value) is int else None


def probe_failures(result: Mapping[str, Any]) -> list[str]:
    """Why a grant probe may not stay outside the ceiling (coordinator ruling 3). Pure.

    It must have answered, in exactly one turn, on the cheapest model, with a
    known output of at most 32 tokens; anything else refuses the run before
    the first reservation.
    """

    failures: list[str] = []
    if result.get("ok") is not True:
        failures.append("not_ok")
    if result.get("turns") != GRANT_PROBE_TURNS:
        failures.append("turns")
    tokens = result.get("output_tokens")
    if type(tokens) is not int or tokens > GRANT_PROBE_OUTPUT_CAP:
        failures.append("output_tokens")
    models = result.get("models")
    if (
        not isinstance(models, list)
        or not models
        or not all(same_model(str(model), GRANT_PROBE_MODEL) for model in models)
    ):
        failures.append("model")
    return failures


def scan_clean(scan: TokenScan) -> bool:
    """At least one channel was scanned, and none saw a held secret's bytes."""

    entries = [entry for entry in scan.channels.values() if isinstance(entry, ChannelScan)]
    return bool(entries) and not any(entry.token_bytes_found for entry in entries)


def is_auth_class(item: Mapping[str, Any]) -> bool:
    return item.get("http_status") in (401, 403) or item.get("provider_error_type") in _AUTH_TYPES


# --------------------------------------------------------------------------
# Private ledger (SQLite)
# --------------------------------------------------------------------------

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS runs (run_id TEXT PRIMARY KEY, candidate TEXT NOT NULL, "
    "started_at REAL NOT NULL, live_kind TEXT, phase TEXT NOT NULL, status TEXT NOT NULL, "
    "budget_state TEXT NOT NULL DEFAULT 'ok', gate TEXT NOT NULL DEFAULT 'open', "
    "live_started_at REAL, live_ended_at REAL, live_starts INTEGER NOT NULL DEFAULT 0, "
    "grant_expires_at REAL)",
    "CREATE TABLE IF NOT EXISTS sessions (run_id TEXT NOT NULL, seq INTEGER NOT NULL, "
    "role TEXT NOT NULL, reserved_output INTEGER NOT NULL, reserved_turns INTEGER NOT NULL, "
    "reserved_seconds REAL NOT NULL, started_at REAL NOT NULL, output_tokens INTEGER, "
    "assistant_turns INTEGER, elapsed REAL, model TEXT, source TEXT NOT NULL DEFAULT "
    "'reserved', PRIMARY KEY (run_id, seq))",
    "CREATE TABLE IF NOT EXISTS observations (run_id TEXT NOT NULL, criterion TEXT NOT NULL, "
    "key TEXT NOT NULL, value_json TEXT NOT NULL, recorded_at REAL NOT NULL, "
    "PRIMARY KEY (run_id, criterion, key))",
    "CREATE TABLE IF NOT EXISTS cleanup (run_id TEXT NOT NULL, seq INTEGER NOT NULL, "
    "kind TEXT NOT NULL, ref_digest TEXT NOT NULL, status TEXT NOT NULL, detail TEXT NOT NULL, "
    "PRIMARY KEY (run_id, seq))",
)


def _private_file_ok(info: os.stat_result) -> bool:
    return (
        stat.S_ISREG(info.st_mode)
        and info.st_uid == os.getuid()
        and stat.S_IMODE(info.st_mode) == 0o600
    )


class Ledger:
    """The run's private record. Every write is one ``BEGIN IMMEDIATE`` transaction."""

    def __init__(self, db: sqlite3.Connection, run_id: str, scrubber: Scrubber) -> None:
        self.db = db
        self.run_id = run_id
        self.scrubber = scrubber

    def __repr__(self) -> str:
        return "<private ledger>"

    @classmethod
    def open(cls, state_dir: Path, run_id: str, *, scrubber: Scrubber) -> Ledger:
        require_private_dir(state_dir)
        path = state_dir / LEDGER_FILE
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        except OSError:
            raise Refusal("ledger_unsafe", LEDGER_FILE) from None
        try:
            info = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        if not _private_file_ok(info):
            raise Refusal("ledger_unsafe", LEDGER_FILE)
        db = sqlite3.connect(path, timeout=10, isolation_level=None)
        db.execute("PRAGMA journal_mode=DELETE")
        ledger = cls(db, run_id, scrubber)
        with ledger.transaction():
            for statement in _SCHEMA:
                db.execute(statement)
        return ledger

    @classmethod
    def open_existing(cls, state_dir: Path, run_id: str) -> Ledger:
        require_private_dir(state_dir)
        path = state_dir / LEDGER_FILE
        try:
            info = os.lstat(path)
        except OSError:
            raise Refusal("ledger_missing", LEDGER_FILE) from None
        if not _private_file_ok(info):
            raise Refusal("ledger_unsafe", LEDGER_FILE)
        db = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10, isolation_level=None)
        ledger = cls(db, run_id, Scrubber())
        try:
            row = db.execute("SELECT 1 FROM runs WHERE run_id=?", (run_id,)).fetchone()
        except sqlite3.Error:
            row = None
        if row is None:
            db.close()
            raise Refusal("ledger_missing", "--run-id")
        return ledger

    def close(self) -> None:
        self.db.close()

    @contextlib.contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield self.db
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        self.db.execute("COMMIT")

    # -- run row ------------------------------------------------------------

    def ensure_run(self, candidate: str, *, phase: str, now: float | None = None) -> None:
        with self.transaction() as db:
            row = db.execute("SELECT candidate FROM runs WHERE run_id=?", (self.run_id,)).fetchone()
            if row is None:
                db.execute(
                    "INSERT INTO runs(run_id,candidate,started_at,phase,status) VALUES(?,?,?,?,?)",
                    (self.run_id, candidate, time.time() if now is None else now, phase, "open"),
                )
            elif row[0] != candidate:
                raise Refusal("candidate_mismatch", "--candidate")
            else:
                db.execute("UPDATE runs SET phase=? WHERE run_id=?", (phase, self.run_id))

    def run_row(self) -> dict[str, Any]:
        cursor = self.db.execute("SELECT * FROM runs WHERE run_id=?", (self.run_id,))
        row = cursor.fetchone()
        if row is None:
            raise Refusal("ledger_missing", "--run-id")
        return dict(zip([column[0] for column in cursor.description], row, strict=True))

    def candidate(self) -> str:
        return str(self.run_row()["candidate"])

    def live_kind(self) -> str | None:
        value = self.run_row()["live_kind"]
        return str(value) if value is not None else None

    def set_phase(self, phase: str, status: str | None = None) -> None:
        with self.transaction() as db:
            if status is None:
                db.execute("UPDATE runs SET phase=? WHERE run_id=?", (phase, self.run_id))
            else:
                db.execute(
                    "UPDATE runs SET phase=?, status=? WHERE run_id=?",
                    (phase, status, self.run_id),
                )

    def mark_live_kind(self, kind: str) -> None:
        """Bind the run ID to ``run`` or ``rehearse`` once; the other refuses it."""

        with self.transaction() as db:
            row = db.execute(
                "SELECT live_kind, live_starts FROM runs WHERE run_id=?", (self.run_id,)
            ).fetchone()
            if row is None:
                raise Refusal("ledger_missing", "--run-id")
            current, starts = row
            if current == "rehearse" and kind == "run":
                raise Refusal("rehearsal_run_id", "--run-id")
            if current == "run" or starts:
                raise Refusal("paid_retry_refused", "--run-id")
            if current is None:
                db.execute("UPDATE runs SET live_kind=? WHERE run_id=?", (kind, self.run_id))
            elif current != kind:
                raise Refusal("arguments_invalid", "--run-id")

    # -- ceiling and budget --------------------------------------------------

    def record_ceiling(self, text: str, ceiling: Ceiling) -> None:
        """Record the exact approval before the first reservation; it never changes after."""

        recorded = self.observations("budget")
        reserved = self.db.execute(
            "SELECT COUNT(*) FROM sessions WHERE run_id=?", (self.run_id,)
        ).fetchone()[0]
        if "approved_ceiling" in recorded:
            if recorded["approved_ceiling"] != text:
                raise Refusal("live_unapproved", "--approve-live differs from the recorded one")
            return
        if reserved:
            raise Refusal("live_unapproved", "a reservation exists before the ceiling")
        self.record("budget", "approved_ceiling", text)
        self.record("budget", "ceiling", ceiling.as_json())

    def record_item_ledger(self, cap: int, charged_before: int, ceiling: Ceiling) -> None:
        """Record the item's cap and what earlier attempts were charged, before the first
        reservation. A run whose full charge would pass the cap refuses to start."""

        if type(cap) is not int or type(charged_before) is not int or not 0 <= charged_before:
            raise Refusal("arguments_invalid", "--item-cap, --item-charged-before")
        if charged_before + ceiling.output_tokens > cap:
            raise Refusal("item_cap_exceeded", "--item-charged-before")
        value = {"cap": cap, "charged_before": charged_before}
        recorded = self.observations("budget").get("item_ledger")
        if recorded is not None:
            if recorded != value:
                raise Refusal("live_unapproved", "the item ledger differs from the recorded one")
            return
        reserved = self.db.execute(
            "SELECT COUNT(*) FROM sessions WHERE run_id=?", (self.run_id,)
        ).fetchone()[0]
        if reserved:
            raise Refusal("live_unapproved", "a reservation exists before the item ledger")
        self.record("budget", "item_ledger", value)

    def ceiling(self) -> Ceiling:
        ceiling = Ceiling.from_json(self.observations("budget").get("ceiling"))
        if ceiling is None:
            raise Refusal("live_unapproved", "no ceiling recorded for this run ID")
        return ceiling

    def begin_live(self, *, now: float, grant_expires_at: float, diagnostic: bool = False) -> None:
        """Open the one live window. ``diagnostic`` is ``rehearse --kickoff``: its kickoff
        runs on a rehearsal ledger with a never-issued token, so only that ledger opens it."""

        with self.transaction() as db:
            row = db.execute(
                "SELECT live_kind, live_starts FROM runs WHERE run_id=?", (self.run_id,)
            ).fetchone()
            if row is None:
                raise Refusal("ledger_missing", "--run-id")
            if row[0] != ("rehearse" if diagnostic else "run"):
                raise Refusal("rehearsal_run_id", "--run-id")
            if row[1]:
                raise Refusal("paid_retry_refused", "--run-id")
            db.execute(
                "UPDATE runs SET live_started_at=?, live_starts=live_starts+1, "
                "grant_expires_at=?, status='live' WHERE run_id=?",
                (now, grant_expires_at, self.run_id),
            )

    def end_live(self, *, now: float) -> None:
        with self.transaction() as db:
            db.execute(
                "UPDATE runs SET live_ended_at=? WHERE run_id=? AND live_ended_at IS NULL",
                (now, self.run_id),
            )

    def close_gate(self) -> None:
        """The private reservation gate: once closed, no reservation is ever granted again."""

        with self.transaction() as db:
            db.execute("UPDATE runs SET gate='closed' WHERE run_id=?", (self.run_id,))

    def gate(self) -> str:
        return str(self.run_row()["gate"])

    def sessions(self) -> list[dict[str, Any]]:
        cursor = self.db.execute(
            "SELECT * FROM sessions WHERE run_id=? ORDER BY seq", (self.run_id,)
        )
        names = [column[0] for column in cursor.description]
        return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]

    def reserve(self, roles: Sequence[str], *, now: float) -> list[Reservation]:
        """Persist reservations for sessions about to start; refuse past any allowance."""

        ceiling = self.ceiling()
        with self.transaction() as db:
            row = db.execute(
                "SELECT budget_state, gate, live_started_at, grant_expires_at FROM runs "
                "WHERE run_id=?",
                (self.run_id,),
            ).fetchone()
            if row is None:
                raise Refusal("ledger_missing", "--run-id")
            budget_state, gate, live_started_at, grant_expires_at = row
            if gate != "open":
                raise Refusal("gate_closed")
            sessions = self.sessions()
            if budget_state == "unknown" or any(item["output_tokens"] is None for item in sessions):
                raise Refusal("usage_unknown")
            if budget_state != "ok":
                raise Refusal("budget_exceeded")
            if live_started_at is None or grant_expires_at is None:
                raise Refusal("allowance_exhausted", "live start not recorded")
            if len(sessions) + len(roles) > ceiling.sessions:
                raise Refusal("allowance_exhausted", "sessions")
            shares = reservation_shares(
                ceiling,
                roles,
                used_turns=sum(int(item["assistant_turns"]) for item in sessions),
                used_tokens=sum(int(item["output_tokens"]) for item in sessions),
            )
            window = sum(share.seconds for share in shares)
            if float(grant_expires_at) - now < window:
                raise Refusal("lifetime_insufficient", "remaining reservation window")
            base = max((int(item["seq"]) for item in sessions), default=0)
            reservations = []
            for offset, (role, share) in enumerate(zip(roles, shares, strict=True), start=1):
                reservation = Reservation(
                    base + offset, role, share.output_tokens, share.turns, share.seconds
                )
                db.execute(
                    "INSERT INTO sessions(run_id,seq,role,reserved_output,reserved_turns,"
                    "reserved_seconds,started_at) VALUES(?,?,?,?,?,?,?)",
                    (
                        self.run_id,
                        reservation.seq,
                        role,
                        share.output_tokens,
                        share.turns,
                        share.seconds,
                        now,
                    ),
                )
                reservations.append(reservation)
            return reservations

    def settle(self, seq: int, usage: SlotUsage | None, *, source: str, check: bool = True) -> None:
        """Record observed usage. ``None`` leaves it unknown: charged its full share.

        ``check`` False defers the bound check to the caller, which settles a
        whole attempt and then checks once: a slot not yet settled is charged
        its full reserved share, so checking between slots could mark a run
        exceeded that its observed totals do not exceed.
        """

        with self.transaction() as db:
            if usage is None:
                db.execute(
                    "UPDATE sessions SET source='unknown' WHERE run_id=? AND seq=?",
                    (self.run_id, seq),
                )
                db.execute(
                    "UPDATE runs SET budget_state='unknown' WHERE run_id=? AND budget_state='ok'",
                    (self.run_id,),
                )
                return
            db.execute(
                "UPDATE sessions SET output_tokens=?, assistant_turns=?, elapsed=?, model=?, "
                "source=? WHERE run_id=? AND seq=?",
                (
                    usage.output_tokens,
                    usage.assistant_turns,
                    round(usage.elapsed, 3),
                    ",".join(usage.models),
                    source,
                    self.run_id,
                    seq,
                ),
            )
        if check:
            self.check_bounds()

    def record_unreserved(self, role: str, usage: SlotUsage | None) -> None:
        with self.transaction() as db:
            seq = 1 + max((int(item["seq"]) for item in self.sessions()), default=0)
            db.execute(
                "INSERT INTO sessions(run_id,seq,role,reserved_output,reserved_turns,"
                "reserved_seconds,started_at,output_tokens,assistant_turns,elapsed,model,source) "
                "VALUES(?,?,?,0,0,0,?,?,?,?,?,'unreserved')",
                (
                    self.run_id,
                    seq,
                    role,
                    time.time(),
                    None if usage is None else usage.output_tokens,
                    None if usage is None else usage.assistant_turns,
                    None if usage is None else round(usage.elapsed, 3),
                    None if usage is None else ",".join(usage.models),
                ),
            )
            db.execute("UPDATE runs SET budget_state='exceeded' WHERE run_id=?", (self.run_id,))

    def check_bounds(self) -> None:
        ceiling = self.ceiling()
        if budget_findings(self.sessions(), ceiling):
            with self.transaction() as db:
                db.execute("UPDATE runs SET budget_state='exceeded' WHERE run_id=?", (self.run_id,))

    def budget_state(self) -> str:
        return str(self.run_row()["budget_state"])

    # -- observations and cleanup ---------------------------------------------

    def record(self, criterion: str, key: str, value: Any) -> None:
        text = self.scrubber.scrub(json.dumps(value, sort_keys=True))
        with self.transaction() as db:
            db.execute(
                "INSERT INTO observations(run_id,criterion,key,value_json,recorded_at) "
                "VALUES(?,?,?,?,?) ON CONFLICT(run_id,criterion,key) DO UPDATE SET "
                "value_json=excluded.value_json, recorded_at=excluded.recorded_at",
                (self.run_id, criterion, key, text, time.time()),
            )

    def observations(self, criterion: str) -> dict[str, Any]:
        rows = self.db.execute(
            "SELECT key, value_json FROM observations WHERE run_id=? AND criterion=?",
            (self.run_id, criterion),
        ).fetchall()
        return {key: json.loads(value) for key, value in rows}

    def recorded_at(self, criterion: str, key: str) -> float | None:
        row = self.db.execute(
            "SELECT recorded_at FROM observations WHERE run_id=? AND criterion=? AND key=?",
            (self.run_id, criterion, key),
        ).fetchone()
        return float(row[0]) if row else None

    def recorded_criteria(self) -> list[str]:
        rows = self.db.execute(
            "SELECT DISTINCT criterion FROM observations WHERE run_id=? ORDER BY criterion",
            (self.run_id,),
        ).fetchall()
        return [str(row[0]) for row in rows]

    def record_cleanup(self, kind: str, ref: str, status: str, detail: str = "") -> None:
        with self.transaction() as db:
            seq = 1 + int(
                db.execute(
                    "SELECT COALESCE(MAX(seq), 0) FROM cleanup WHERE run_id=?", (self.run_id,)
                ).fetchone()[0]
            )
            db.execute(
                "INSERT INTO cleanup(run_id,seq,kind,ref_digest,status,detail) VALUES(?,?,?,?,?,?)",
                (self.run_id, seq, kind, ref_digest(ref), status, self.scrubber.scrub(detail)),
            )

    def cleanup_rows(self) -> list[dict[str, Any]]:
        cursor = self.db.execute(
            "SELECT kind, status, detail FROM cleanup WHERE run_id=? ORDER BY seq",
            (self.run_id,),
        )
        return [
            {"kind": kind, "status": status, "detail": detail}
            for kind, status, detail in cursor.fetchall()
        ]

    def dump(self) -> str:
        """Every recorded section and cleanup row, for the ledger channel's leak scan."""

        return json.dumps(
            [self.observations(name) for name in self.recorded_criteria()]
            + [self.cleanup_rows(), self.sessions()],
            sort_keys=True,
        )


def charged(row: Mapping[str, Any]) -> tuple[int, int, float]:
    """A session row's charge: observed usage, or its full reservation share when unknown."""

    if row.get("output_tokens") is None or row.get("assistant_turns") is None:
        return (
            int(row["reserved_output"]),
            int(row["reserved_turns"]),
            float(row["reserved_seconds"]),
        )
    return int(row["output_tokens"]), int(row["assistant_turns"]), float(row["elapsed"] or 0.0)


def budget_findings(rows: Sequence[Mapping[str, Any]], ceiling: Ceiling) -> list[str]:
    """Every way the recorded sessions exceed the recorded ceiling. Pure."""

    findings: list[str] = []
    if len(rows) > ceiling.sessions:
        findings.append("sessions")
    tokens = turns = 0
    for row in rows:
        out, used, elapsed = charged(row)
        tokens += out
        turns += used
        if row.get("source") == "unreserved":
            findings.append("unreserved")
        if out > ceiling.session_output_tokens:
            findings.append("session_output_tokens")
        if used > ceiling.turns_for(str(row.get("role"))):
            findings.append("session_turns")
        if elapsed > ceiling.session_seconds:
            findings.append("session_seconds")
    if tokens > ceiling.output_tokens:
        findings.append("output_tokens")
    if turns > ceiling.turns:
        findings.append("turns")
    return sorted(set(findings))


def settle_attempt(
    ledger: Ledger,
    reservations: Sequence[Reservation],
    sessions: Sequence[Any] | None,
    *,
    complete: bool,
    malformed: int = 0,
) -> dict[str, int]:
    """Settle every reserved slot from the attempt's sessions, one row per native session.

    Each observed native session holds its own row: its slot's, or an
    unreserved row when no slot was left, so the judge counts every start
    against ``ceiling.sessions``. An incomplete capture, an unreadable line or
    an attempt with no implementer session leaves every slot unknown, charged
    its full share, and every unreserved session unknown. Otherwise each slot
    takes its matched usage and an unused slot settles to zero. A session
    with no assistant usage, one still waiting for its first response
    included, is unknown.
    """

    has_implementer = bool(sessions) and any(
        session.role == IMPLEMENTER for session in sessions or ()
    )
    if not complete or sessions is None or malformed or not has_implementer:
        for item in reservations:
            ledger.settle(item.seq, None, source="unknown")
        _, beyond = match_slots(reservations, sessions or ())
        for session in beyond:
            ledger.record_unreserved(session.role, None)
        return {"settled": 0, "unknown": len(reservations), "unreserved": len(beyond)}
    slots, unreserved = match_slots(reservations, sessions)
    settled = unknown = 0
    for item in reservations:
        slot = slots[item.seq]
        if slot.sessions == 0:
            ledger.settle(item.seq, SlotUsage(), source="not_started", check=False)
            settled += 1
        elif not slot.known:
            ledger.settle(item.seq, None, source="unknown", check=False)
            unknown += 1
        else:
            ledger.settle(item.seq, slot, source="native", check=False)
            settled += 1
    for session in unreserved:
        usage = SlotUsage()
        usage.add(session)
        ledger.record_unreserved(session.role, usage if usage.known else None)
    # Checked once, after every slot holds its observed usage.
    ledger.check_bounds()
    return {"settled": settled, "unknown": unknown, "unreserved": len(unreserved)}


# --------------------------------------------------------------------------
# The bundle's reservation gate (runs in the runner as a PreToolUse hook)
# --------------------------------------------------------------------------

RESERVATION_GATE_FILE = "kickoff_reservation_gate.py"
# Counts reviewer starts at invocation, before any model call, and refuses a
# start past this attempt's reservations or of any other subagent type. Every
# start claims its own slot file with O_EXCL, so parallel hook processes never
# share, overwrite or remove another's gate file. Exit 2 blocks the call; every
# unexpected state also exits 2.
RESERVATION_GATE_PROGRAM = r"""
import json, os, sys
LIMITS = json.loads(sys.argv[1])
def refuse(reason):
    print("reservation gate: " + reason, file=sys.stderr)
    sys.exit(2)
try:
    event = json.load(sys.stdin)
    if not isinstance(event, dict):
        refuse("unreadable hook input")
    if event.get("tool_name") not in ("Agent", "Task"):
        sys.exit(0)
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import review_gate
    resolved = review_gate.reviewer_kind(dict(event.get("tool_input") or {}))
    kind = {review_gate.PLAN: "plan", review_gate.DIFF: "diff"}.get(resolved)
    if kind is None:
        refuse("only the reserved plan and diff reviewers may start in this run")
    folder = os.environ.get("PSK_RESERVATION_DIR") or "/tmp/psk-reservations"
    os.makedirs(folder, mode=0o700, exist_ok=True)
    for slot in range(int(LIMITS[kind])):
        path = os.path.join(folder, kind + "-" + str(slot))
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            continue
        os.close(descriptor)
        sys.exit(0)
    refuse("the reserved " + kind + " review allowance is spent")
except SystemExit:
    raise
except Exception:
    refuse("reservation state could not be updated")
"""


def private_bundle_hooks(hooks: Mapping[str, Any], limits: Mapping[str, int]) -> dict[str, Any]:
    """The private copy's hooks.json with the reservation gate added for Agent and Task.

    Only a hook is added: the bundle's models, prompts and review loop stay
    exactly as published.
    """

    patched: dict[str, Any] = json.loads(json.dumps(hooks))
    encoded = json.dumps(dict(limits), sort_keys=True, separators=(",", ":"))
    command = f"python3 ${{CLAUDE_PLUGIN_ROOT}}/hooks/{RESERVATION_GATE_FILE} '{encoded}'"
    entry = {
        "matcher": "Agent|Task",
        "hooks": [{"type": "command", "command": command, "timeout": 10}],
    }
    patched.setdefault("hooks", {}).setdefault("PreToolUse", []).append(entry)
    return patched


# --------------------------------------------------------------------------
# Dex, OIDC split horizon and install values
# --------------------------------------------------------------------------

DEX_PORT = 5556
OIDC_CLIENT_ID = "curie-console"
OWNER_EMAIL = "owner@example.com"
# The public bcrypt of tools/oidc-e2e's published static login
# (`oidc_e2e.USER_PASSWORD`, the hash its dex.yaml.template uses); this Dex is
# private to the run's namespace and reachable only through its own
# port-forward.
DEX_PASSWORD_HASH = "$2a$10$2b2cU8CPhOTaGrs1HRQuAueS7JTT5ZHsHSzYiFPm1leZck7Mc8T4W"
SESSION_COOKIE = "__Host-curie_console_session"
STATE_COOKIE = "__Host-curie_oidc_state"


@dataclasses.dataclass(frozen=True)
class OidcOrigins:
    """The in-cluster names the API uses, and the loopback forwards the driver dials."""

    issuer: str
    redirect_uri: str

    @property
    def issuer_netloc(self) -> str:
        return urllib.parse.urlsplit(self.issuer).netloc

    @property
    def redirect_netloc(self) -> str:
        return urllib.parse.urlsplit(self.redirect_uri).netloc


def oidc_origins(
    *, namespace: str, dex_service: str, api_service: str, api_port: int
) -> OidcOrigins:
    """Issuer and redirect on the in-cluster Service names, which the API pod resolves."""

    return OidcOrigins(
        issuer=f"http://{dex_service}.{namespace}.svc.cluster.local:{DEX_PORT}/dex",
        redirect_uri=(
            f"http://{api_service}.{namespace}.svc.cluster.local:{api_port}/console/oidc/callback"
        ),
    )


def route_url(
    url: str, origins: OidcOrigins, *, dex_local: int, api_local: int
) -> tuple[str, dict[str, str]] | None:
    """Map an in-cluster origin to the driver's loopback forward, presenting that origin.

    A Dex URL is dialed at the Dex forward with ``Host`` set to the issuer's
    authority, so Dex answers as the issuer the API discovered. The API's
    callback is dialed at the API forward. Any other origin is refused (None):
    the login never leaves the run's two forwards.
    """

    parts = urllib.parse.urlsplit(url)
    path = urllib.parse.urlunsplit(("", "", parts.path, parts.query, ""))
    if parts.scheme == "http" and parts.netloc == origins.issuer_netloc:
        return f"http://127.0.0.1:{dex_local}{path}", {"Host": origins.issuer_netloc}
    if parts.scheme == "http" and parts.netloc == origins.redirect_netloc:
        return f"http://127.0.0.1:{api_local}{path}", {}
    if parts.scheme == "http" and parts.netloc == f"127.0.0.1:{api_local}":
        return url, {}
    return None


def dex_config(origins: OidcOrigins, *, client_secret: str) -> dict[str, Any]:
    return {
        "issuer": origins.issuer,
        "storage": {"type": "memory"},
        "web": {"http": f"0.0.0.0:{DEX_PORT}"},
        "oauth2": {"skipApprovalScreen": True},
        "staticClients": [
            {
                "id": OIDC_CLIENT_ID,
                "name": "Curie console (Slack kickoff proof)",
                "secret": client_secret,
                "redirectURIs": [origins.redirect_uri],
            }
        ],
        "enablePasswordDB": True,
        "staticPasswords": [
            {
                "email": OWNER_EMAIL,
                "username": OWNER_EMAIL.split("@", 1)[0],
                "userID": "kickoff-owner",
                "hash": DEX_PASSWORD_HASH,
            }
        ],
    }


def dex_manifests(
    *,
    name: str,
    labels: Mapping[str, str],
    config: Mapping[str, Any],
    client_secret: str,
    image: str,
) -> list[dict[str, Any]]:
    """Secret (config and client secret), Pod and Service for the run's own Dex.

    ``image`` is the digest-pinned Dex the OIDC harness (tools/oidc-e2e) uses.
    """

    selector = {**labels, "app.kubernetes.io/component": "dex"}
    return [
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": name, "labels": dict(labels)},
            "type": "Opaque",
            "data": {
                "config.yaml": base64.b64encode(json.dumps(config).encode()).decode(),
                "clientSecret": base64.b64encode(client_secret.encode()).decode(),
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name, "labels": selector},
            "spec": {
                "automountServiceAccountToken": False,
                "securityContext": {
                    "runAsNonRoot": True,
                    "seccompProfile": {"type": "RuntimeDefault"},
                },
                "containers": [
                    {
                        "name": "dex",
                        "image": image,
                        "args": ["dex", "serve", "/etc/dex/config.yaml"],
                        "ports": [{"containerPort": DEX_PORT}],
                        "readinessProbe": {"httpGet": {"path": "/dex/healthz", "port": DEX_PORT}},
                        "securityContext": {
                            "allowPrivilegeEscalation": False,
                            "capabilities": {"drop": ["ALL"]},
                        },
                        "volumeMounts": [
                            {"name": "config", "mountPath": "/etc/dex", "readOnly": True}
                        ],
                    }
                ],
                "volumes": [
                    {
                        "name": "config",
                        "secret": {
                            "secretName": name,
                            "items": [{"key": "config.yaml", "path": "config.yaml"}],
                            "defaultMode": 0o444,
                        },
                    }
                ],
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": name, "labels": dict(labels)},
            "spec": {"selector": selector, "ports": [{"port": DEX_PORT, "targetPort": DEX_PORT}]},
        },
    ]


@dataclasses.dataclass(frozen=True)
class InstallPlan:
    release: str
    registry: str
    digests: Mapping[str, str]
    agent: str
    repo: str
    app_id: str
    app_secret: str
    slack_secret: str
    channel: str
    sender: SlackAuth
    grant_namespace: str
    origins: OidcOrigins
    dex_secret: str
    unused_label: str
    priority_classes: tuple[str, str] | None
    controller_deploy: bool
    model_addresses: Sequence[str]
    implementer_turns: int
    response_cap: int


def install_values(plan: InstallPlan) -> dict[str, Any]:
    """Helm values: Socket Mode dispatcher, test installation, OIDC, custody, personal factory.

    No secret value appears here: tokens, the App key and the OIDC client
    secret arrive by Secret reference, and the chart generates the API key,
    worker token and attester secret. Intake stays dormant on an unused label,
    so the only admission is the kickoff's.
    """

    def image(name: str) -> dict[str, Any]:
        return {
            "repository": f"{plan.registry}/{name}",
            "digest": plan.digests[name],
            "pullPolicy": "IfNotPresent",
        }

    credential = {
        "type": "github_app",
        "appId": plan.app_id,
        "existingSecret": plan.app_secret,
        "existingSecretKey": "privateKey",
    }
    github = {
        "name": "github",
        "kind": "github",
        "host": "github.com",
        "apiUrl": "https://api.github.com",
    }
    priority = (
        {
            "platform": {"create": True, "name": f"{plan.release}-platform"},
            "sandbox": {"create": True, "name": f"{plan.release}-sandbox"},
        }
        if plan.priority_classes is None
        else {
            "platform": {"create": False, "name": plan.priority_classes[0]},
            "sandbox": {"create": False, "name": plan.priority_classes[1]},
        }
    )
    oidc = [
        {"name": "CURIE_OIDC_ISSUER", "value": plan.origins.issuer},
        {"name": "CURIE_OIDC_AUDIENCE", "value": OIDC_CLIENT_ID},
        {"name": "CURIE_OIDC_JWKS_URL", "value": f"{plan.origins.issuer}/keys"},
        {
            "name": "CURIE_OIDC_CLIENT_SECRET",
            "valueFrom": {"secretKeyRef": {"name": plan.dex_secret, "key": "clientSecret"}},
        },
        {"name": "CURIE_OIDC_REDIRECT_URI", "value": plan.origins.redirect_uri},
        {"name": "CURIE_OIDC_SCOPES", "value": "openid email profile"},
        {"name": "CURIE_OIDC_ADMIT_ALL_AUTHENTICATED", "value": "false"},
    ]
    personal = {"name": "FACTORY_MODEL_AUTH_MODE", "value": PERSONAL_MODE}
    return {
        "api": {
            "image": image("curie-api"),
            "extraEnv": [*oidc, personal],
            "githubAppId": plan.app_id,
            "githubAppExistingSecret": plan.app_secret,
            "githubRepoAllowlist": [plan.repo],
        },
        "worker": {
            "image": image("curie-worker"),
            "maxConcurrency": 1,
            "workItemMaxTurns": plan.implementer_turns,
            "extraEnv": [dict(personal)],
        },
        "dispatcher": {
            "deploy": True,
            "image": image("curie-dispatcher"),
            "slack": {
                "appTokenExistingSecret": plan.slack_secret,
                "appTokenExistingSecretKey": "slackAppToken",
                "botTokenExistingSecret": plan.slack_secret,
                "botTokenExistingSecretKey": "slackBotToken",
            },
        },
        "channelGateway": {"deploy": True},
        "testInstallation": {
            "enabled": True,
            "drivers": [
                {
                    "channel_id": plan.channel,
                    "bot_id": plan.sender.bot_id,
                    "bot_user_id": plan.sender.user_id,
                }
            ],
        },
        "personalClaude": {"grantNamespace": plan.grant_namespace},
        "ui": {"deploy": False},
        "inference": {"deploy": False},
        "agentSandbox": {
            "controller": {"deploy": plan.controller_deploy},
            "warmPool": {"replicas": 0},
            "runner": {
                "image": f"{plan.registry}/curie-runner",
                "digest": plan.digests["curie-runner"],
                "imagePullPolicy": "IfNotPresent",
                "fakeModel": False,
                "credentials": "",
                "extraEnv": [
                    {"name": "CLAUDE_CODE_MAX_OUTPUT_TOKENS", "value": str(plan.response_cap)},
                ],
            },
        },
        "factory": {
            "enabled": True,
            "intake": "poll",
            "codeHosts": [{**github, "credential": dict(credential)}],
            "trackers": [
                {
                    **github,
                    "credential": dict(credential),
                    "label": plan.unused_label,
                    "mention": plan.unused_label,
                }
            ],
            "sandboxEgress": {plan.agent: True},
        },
        "priorityClasses": priority,
        "security": {
            "gvisor": {"mode": "off"},
            "networkPolicy": {
                "allowedEgress": [
                    {"cidr": f"{address}/32", "ports": [{"protocol": "TCP", "port": 443}]}
                    for address in sorted(set(plan.model_addresses))
                ]
            },
        },
    }


_FORBIDDEN_ENV = frozenset(
    {
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        "ANTHROPIC_BASE_URL",
        "CLAUDE_CODE_OAUTH_TOKEN",
        "CURIE_CREDENTIALS",
        "CURIE_FAKE_MODEL",
        "CURIE_MODEL_BASE_URL",
        "OPENROUTER_API_KEY",
    }
)


def service_credential_findings(values: Any) -> list[str]:
    """A service credential, a static grant or a fake model anywhere in the deployed values."""

    findings: set[str] = set()

    def walk(value: Any, path: str) -> None:
        if isinstance(value, dict):
            name = value.get("name")
            if isinstance(name, str) and name in _FORBIDDEN_ENV:
                findings.add(name)
            for key, item in value.items():
                walk(item, f"{path}.{key}")
        elif isinstance(value, list):
            for item in value:
                walk(item, path)

    walk(values, "")
    runner = (
        (((values or {}).get("agentSandbox") or {}).get("runner") or {})
        if isinstance(values, dict)
        else {}
    )
    if runner.get("fakeModel") not in (False, None) or runner.get("credentials") not in ("", None):
        findings.add("agentSandbox.runner")
    return sorted(findings)


# charts/curie/templates/_test-installation.tpl: the defaults a test installation
# refuses, read from the one module the API and dispatcher share.
PUBLISHED_DEFAULTS = {
    "apiKey": driver_declaration.PUBLISHED_DEFAULT_API_KEY,
    "internalWorkerToken": driver_declaration.PUBLISHED_DEFAULT_INTERNAL_WORKER_TOKEN,
    "approvalChatAttesterSecret": (
        driver_declaration.PUBLISHED_DEFAULT_APPROVAL_CHAT_ATTESTER_SECRET
    ),
    "remediationAuthorityKey": driver_declaration.PUBLISHED_DEFAULT_REMEDIATION_AUTHORITY_KEY,
}


def generated_secrets_ok(data: Mapping[str, str]) -> bool:
    """The release Secret's three test-installation credentials are generated, not defaults."""

    decoded: dict[str, str] = {}
    for key in PUBLISHED_DEFAULTS:
        try:
            decoded[key] = base64.b64decode(data.get(key) or "", validate=True).decode()
        except (ValueError, UnicodeDecodeError):
            return False
    return all(
        len(value) >= 16 and value != PUBLISHED_DEFAULTS[key] for key, value in decoded.items()
    ) and len(set(decoded.values())) == len(decoded)


# --------------------------------------------------------------------------
# Judge
# --------------------------------------------------------------------------

CRITERIA = ("slack", "admission", "owner", "grant", "progress", "refusal", "budget", "cleanup")
REHEARSAL_CRITERIA = ("slack", "refusal", "cleanup")


def _true(observed: Mapping[str, Any], key: str) -> bool:
    return observed.get(key) is True


def _equal(observed: Mapping[str, Any], key: str, expected_key: str) -> bool:
    return (
        key in observed and expected_key in observed and (observed[key] == observed[expected_key])
    )


def _all_true(value: Any) -> bool:
    return isinstance(value, dict) and bool(value) and all(item is True for item in value.values())


def _judge_slack(ledger: Ledger) -> tuple[dict[str, bool], dict[str, int]]:
    observed = ledger.observations("slack")
    checks = {
        "sender_authenticated": _true(observed, "sender_auth_ok"),
        "curie_authenticated": _true(observed, "curie_auth_ok"),
        "app_token_accepted": _true(observed, "app_token_ok"),
        "same_workspace": _true(observed, "same_team"),
        "sender_is_another_app": _true(observed, "sender_distinct"),
        "isolation_confirmed": _true(observed, "isolation_confirmed"),
        "curie_in_channel": _true(observed, "curie_in_channel"),
        "driver_declared_from_auth_test": _equal(observed, "declared_driver", "sender_driver"),
        "ping_answered_over_socket_mode": _true(observed, "ping_answered"),
        "ping_receipt_present": _true(observed, "ping_receipt_present"),
        "control_receipt_present": _true(observed, "control_receipt_present"),
        "control_receipt_matches": _all_true(observed.get("control_receipt_checks")),
        "control_enqueue_logged": _true(observed, "control_enqueue_logged"),
        "socket_mode_ended_at_uninstall": _true(observed, "dispatcher_pods_gone"),
    }
    if ledger.live_kind() != "rehearse":
        checks.update(
            {
                "kickoff_posted_at_root": _true(observed, "kickoff_at_root"),
                "kickoff_receipt_present": _true(observed, "kickoff_receipt_present"),
                "kickoff_receipt_matches": _all_true(observed.get("kickoff_receipt_checks")),
                "kickoff_enqueue_logged": _true(observed, "kickoff_enqueue_logged"),
            }
        )
    return checks, {"ping_messages": int(observed.get("ping_messages") or 0)}


def _judge_admission(ledger: Ledger) -> tuple[dict[str, bool], dict[str, int]]:
    observed = ledger.observations("admission")
    items = observed.get("work_items")
    requests = observed.get("requests")
    item = items[0] if isinstance(items, list) and len(items) == 1 else {}
    request = requests[0] if isinstance(requests, list) and len(requests) == 1 else {}
    checks = {
        "none_before": observed.get("before") == {"work_items": 0, "requests": 0},
        "one_work_item": isinstance(items, list) and len(items) == 1,
        "one_request": isinstance(requests, list) and len(requests) == 1,
        "request_on_work_item": bool(item) and request.get("work_item_id") == item.get("id"),
        "request_id_is_kickoff_id": bool(request.get("id"))
        and request.get("id") == observed.get("expected_request_id"),
        "personal_mode": bool(observed.get("expected_mode"))
        and item.get("model_auth_mode") == observed.get("expected_mode")
        and request.get("model_auth_mode") == observed.get("expected_mode"),
        "no_trigger_label": bool(item) and item.get("trigger_label") is None,
        "objective_is_issue": bool(observed.get("issue_url_digest"))
        and request.get("objective_digest") == observed.get("issue_url_digest"),
        "requester_is_sender": bool(observed.get("expected_requester"))
        and request.get("requester") == observed.get("expected_requester"),
        "issue_matches": bool(item)
        and str(item.get("tracker_issue_id")) == str(observed.get("issue_number")),
        "thread_is_kickoff_root": isinstance(observed.get("thread"), dict)
        and observed.get("thread") == observed.get("expected_thread"),
    }
    return checks, {
        "work_items": len(items) if isinstance(items, list) else 0,
        "requests": len(requests) if isinstance(requests, list) else 0,
    }


def _judge_owner(ledger: Ledger) -> tuple[dict[str, bool], dict[str, int]]:
    observed = ledger.observations("owner")
    expected = observed.get("expected")
    request_owners = observed.get("request_owners")
    complete = isinstance(expected, dict) and all(
        expected.get(key) not in (None, "")
        for key in ("tenant_id", "mode", "principal_id", "generation")
    )
    checks = {
        "expected_owner_recorded": complete,
        "work_item_owner": complete and observed.get("work_item_owner") == expected,
        "request_owners": complete
        and isinstance(request_owners, list)
        and bool(request_owners)
        and all(owner == expected for owner in request_owners),
        "tenant_is_agent_tenant": complete
        and isinstance(expected, dict)
        and observed.get("agent_tenant") == expected.get("tenant_id"),
        "link_names_sender": _true(observed, "link_native_is_sender"),
        "link_names_principal": complete
        and isinstance(expected, dict)
        and observed.get("link_principal") == expected.get("principal_id"),
        "enrollment_active_at_kickoff": observed.get("enrollment_state_at_kickoff") == "active",
        "generation_is_current": complete
        and isinstance(expected, dict)
        and observed.get("enrollment_generation_at_kickoff") == expected.get("generation"),
    }
    return checks, {
        "request_owners": len(request_owners) if isinstance(request_owners, list) else 0
    }


def _role_models_ok(sessions: Sequence[Mapping[str, Any]], models: Mapping[str, Any]) -> bool:
    by_role: dict[str, list[str]] = {}
    for item in sessions:
        by_role.setdefault(str(item.get("role")), []).extend(
            str(model) for model in item.get("models") or []
        )
    if not by_role.get(IMPLEMENTER):
        return False
    for role, observed in by_role.items():
        expected = models.get(role)
        if not observed:
            return False
        if isinstance(expected, str) and not all(same_model(m, expected) for m in observed):
            return False
    return True


def _judge_grant(ledger: Ledger) -> tuple[dict[str, bool], dict[str, int]]:
    observed = ledger.observations("grant")
    pods = observed.get("runner_pods")
    pods = pods if isinstance(pods, list) else []
    sessions = observed.get("sessions")
    sessions = sessions if isinstance(sessions, list) else []
    transcript = observed.get("transcript_error_classes")
    stderr = observed.get("stderr_error_classes")
    cause = observed.get("terminal_cause", "<missing>")
    errors_known = (
        isinstance(transcript, list) and isinstance(stderr, list) and ("terminal_cause" in observed)
    )
    auth_seen = errors_known and any(
        isinstance(item, dict) and is_auth_class(item)
        for item in [*(transcript or []), *(stderr or [])]
    )
    ceiling = Ceiling.from_json(ledger.observations("budget").get("ceiling"))
    checks = {
        "platform_images_are_candidate": _true(observed, "platform_images_are_candidate"),
        "runner_pod_observed": bool(pods),
        "grant_from_claim_tokens": bool(pods)
        and all(_true(pod, "grant_from_claim_tokens") for pod in pods),
        "claim_owner_labels": bool(pods) and all(_true(pod, "claim_owner_labels") for pod in pods),
        "runner_image_is_candidate": bool(pods)
        and all(_all_true(pod.get("image")) for pod in pods),
        "container_saw_grant": bool(pods)
        and all(pod.get("container_credential") == "present" for pod in pods),
        "implementer_session": any(item.get("role") == IMPLEMENTER for item in sessions),
        "no_synthetic_records": observed.get("synthetic_records") == 0,
        "no_zero_usage_session": bool(sessions)
        and all(int(item.get("output_tokens") or 0) > 0 for item in sessions),
        "expected_models": ceiling is not None and _role_models_ok(sessions, ceiling.models),
        "no_provider_auth_error": errors_known and not auth_seen,
        "terminal_frame_agrees": errors_known and (cause == AUTH_TERMINAL_CAUSE) == bool(auth_seen),
    }
    return checks, {"runner_pods": len(pods), "sessions": len(sessions)}


def _judge_progress(ledger: Ledger) -> tuple[dict[str, bool], dict[str, int]]:
    observed = ledger.observations("progress")
    posts = observed.get("posts")
    posts = posts if isinstance(posts, list) else []
    inside = observed.get("thread_message_digests")
    inside = set(inside) if isinstance(inside, list) else set()
    kinds = [str(post.get("kind")) for post in posts if isinstance(post, dict)]
    checks = {
        "thread_is_kickoff_root": isinstance(observed.get("thread"), dict)
        and observed.get("thread") == observed.get("expected_thread"),
        "posts_owed": bool(posts),
        "terminal_posted": TERMINAL_POST in kinds,
        "every_post_delivered": bool(posts)
        and all(post.get("posted") is True and post.get("failed") is False for post in posts),
        "every_post_in_origin_thread": bool(posts)
        and bool(inside)
        and all(post.get("ref_digest") in inside for post in posts),
        "no_generated_root": ROOT_POST not in kinds and observed.get("root_posts") == 0,
        "no_foreign_messages": observed.get("foreign_messages") == 0,
        "admitted_reply_in_thread": observed.get("kickoff_reply_outcome") == ADMITTED,
        "pull_request_posted_when_present": "pull_request_exists" in observed
        and (observed.get("pull_request_exists") is not True or PULL_REQUEST_POST in kinds),
    }
    return checks, {"posts": len(posts), "thread_messages": len(inside)}


_COUNT_KEYS = ("work_items", "requests", "claims", "runner_pods", "sessions")


def _judge_refusal(ledger: Ledger) -> tuple[dict[str, bool], dict[str, int]]:
    observed = ledger.observations("refusal")
    before, after = observed.get("before"), observed.get("after")
    counted = all(
        isinstance(snapshot, dict) and all(type(snapshot.get(key)) is int for key in _COUNT_KEYS)
        for snapshot in (before, after)
    )
    checks = {
        "disconnected_through_owner_surface": observed.get("disconnect")
        == {"exit": 0, "state": "inactive"},
        "inactive_before_control": observed.get("state_before_control") == "inactive",
        "teardown_settled_before_control": _true(observed, "teardown_settled"),
        "control_is_another_issue": _true(observed, "control_issue_distinct"),
        "reply_names_enrollment_inactive": observed.get("control_reply_outcome")
        == observed.get("expected_outcome")
        and bool(observed.get("expected_outcome")),
        "counts_unchanged": counted and before == after,
        "zero_native_sessions": observed.get("control_native_sessions") == 0,
    }
    return checks, {
        key: int((after or {}).get(key) or 0) for key in _COUNT_KEYS if isinstance(after, dict)
    }


def _judge_budget(ledger: Ledger) -> tuple[dict[str, bool], dict[str, int]]:
    observed = ledger.observations("budget")
    probe = ledger.observations("grant_probe")
    ceiling = Ceiling.from_json(observed.get("ceiling"))
    rows = ledger.sessions()
    run = ledger.run_row()
    ceiling_at = ledger.recorded_at("budget", "approved_ceiling")
    first = min((float(row["started_at"]) for row in rows), default=None)
    probe_started = ledger.recorded_at("grant_probe", "started")
    probe_result_at = ledger.recorded_at("grant_probe", "result")
    recorded = probe.get("result")
    result: dict[str, Any] = recorded if isinstance(recorded, dict) else {}
    cap = probe.get("output_cap")
    tokens = result.get("output_tokens")
    checks = {
        "ceiling_recorded": ceiling is not None
        and isinstance(observed.get("approved_ceiling"), str)
        and ceiling.canonical() == observed.get("approved_ceiling"),
        "ceiling_before_first_reservation": ceiling_at is not None
        and first is not None
        and ceiling_at <= first,
        "sessions_reserved": bool(rows),
        "within_ceiling": ceiling is not None and not budget_findings(rows, ceiling),
        "ceiling_enforced": not ceiling_enforcement_failures(ledger),
        "one_live_start": ceiling is not None
        and 1 <= int(run["live_starts"]) <= 1 + ceiling.paid_retries,
        "no_allowance_breach": observed.get("allowance_breach") == [],
        "probe_recorded_before_invocation": probe_started is not None
        and probe_result_at is not None
        and probe_started <= probe_result_at,
        "probe_passed": result.get("ok") is True,
        "probe_one_turn": result.get("turns") == probe.get("turn_cap")
        and isinstance(probe.get("turn_cap"), int),
        "probe_within_token_cap": type(tokens) is int and type(cap) is int and tokens <= cap,
        "probe_on_cheapest_model": bool(result.get("models"))
        and isinstance(probe.get("model"), str)
        and all(same_model(str(m), str(probe.get("model"))) for m in result.get("models") or []),
    }
    totals = [charged(row) for row in rows]
    item = observed.get("item_ledger")
    before = item.get("charged_before") if isinstance(item, dict) else None
    return checks, {
        "sessions": len(rows),
        "output_tokens": sum(entry[0] for entry in totals),
        "assistant_turns": sum(entry[1] for entry in totals),
        "tool_search_calls": int(observed.get("tool_search_calls") or 0),
        "item_charged": (before if type(before) is int else 0) + sum(e[0] for e in totals),
    }


def launched_sessions(
    reservations: Sequence[Reservation],
    pods: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """The runner sessions the product started. The harvest is not an input. Pure.

    ``pods`` is the followed runners, in follow order. ``turn_starts`` is
    ``(session id, timestamp)`` pairs from that pod's runner log, in log order.
    Distinct session ids, in the order each id first appears, are sessions that
    started. A pod whose native process ran (``ran``) and that logged no turn
    start still started one runner session. Each start takes the next implementer
    reservation. A start with none left is recorded with no seq.
    """

    pending = [item for item in reservations if item.role == IMPLEMENTER]
    rows: list[dict[str, Any]] = []
    for pod in pods:
        seen: list[str] = []
        for item in pod.get("turn_starts") or ():
            if not isinstance(item, (tuple, list)) or not item:
                continue
            session = item[0]
            if not isinstance(session, str) or not session or session in seen:
                continue
            seen.append(session)
        count = len(seen) if seen else (1 if pod.get("ran") is True else 0)
        for _index in range(count):
            row: dict[str, Any] = {"role": IMPLEMENTER}
            if pending:
                row["seq"] = pending.pop(0).seq
            rows.append(row)
    return rows


def guard_coverage(
    reservations: Sequence[Reservation],
    sessions: Sequence[Any] | None,
    seen: Iterable[str],
    *,
    checks: int,
    launched: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """The recorded per-response guard: one row per harvested session. Pure.

    ``launched`` is the recorded launched set, never inferred from the harvest.
    A row's ``launched`` is true only when that set names its reservation, or,
    for a session no reservation matched, a seq-less entry of its role. A
    reservation the harvest did not match is omitted, so an unused slot is not
    a missing response. ``guard_after_first`` is true only when that session has
    a response, every one of its response keys was seen, and at least one check
    ran.
    """

    covered_ids = frozenset(seen)
    by_seq = {
        int(item["seq"]): item
        for item in launched
        if isinstance(item, Mapping) and type(item.get("seq")) is int
    }
    unsequenced = [
        item
        for item in launched
        if isinstance(item, Mapping) and "seq" not in item and item.get("role") in ROLES
    ]
    pending = {role: [item for item in reservations if item.role == role] for role in ROLES}
    rows: list[dict[str, Any]] = []
    ordered = sorted(sessions or (), key=lambda item: (item.first is None, item.first or 0.0))
    for session in ordered:
        queue = pending.get(session.role) or []
        slot = queue.pop(0) if queue else None
        keys = frozenset(session.message_ids)
        responses = len(keys)
        checked = len(keys & covered_ids)
        is_launched = False
        if slot is not None:
            record = by_seq.get(slot.seq)
            is_launched = isinstance(record, Mapping) and record.get("role") == session.role
        else:
            for index, item in enumerate(unsequenced):
                if item.get("role") == session.role:
                    is_launched = True
                    del unsequenced[index]
                    break
        row: dict[str, Any] = {
            "role": session.role,
            "turns": int(session.assistant_turns),
            "launched": is_launched,
            "responses": responses,
            "responses_checked": checked,
            "guard_after_first": responses >= 1 and checked == responses and checks >= 1,
        }
        if slot is not None:
            row["seq"] = slot.seq
        rows.append(row)
    return {
        "checks": checks,
        "responses": sum(int(row["responses"]) for row in rows),
        "responses_checked": sum(int(row["responses_checked"]) for row in rows),
        "sessions": rows,
    }


def _guard_record_failures(guard: Any) -> list[str]:
    """Why ``budget.guard_checks`` does not show every launched response guarded."""

    if not isinstance(guard, dict):
        return ["guard_events"]
    checks = guard.get("checks")
    responses = guard.get("responses")
    covered = guard.get("responses_checked")
    sessions = guard.get("sessions")
    if type(checks) is not int or type(responses) is not int or type(covered) is not int:
        return ["guard_events"]
    if not isinstance(sessions, list):
        return ["guard_sessions"]
    failures: list[str] = []
    total_responses = 0
    total_checked = 0
    needs_check = False
    for row in sessions:
        if not isinstance(row, dict):
            failures.append("guard_sessions")
            continue
        row_responses = row.get("responses")
        row_checked = row.get("responses_checked")
        turns = row.get("turns")
        launched = row.get("launched")
        after = row.get("guard_after_first")
        if (
            type(row_responses) is not int
            or type(row_checked) is not int
            or type(turns) is not int
            or type(launched) is not bool
            or type(after) is not bool
            or row_responses < 0
            or row_checked < 0
            or turns < 0
            or ("seq" in row and type(row.get("seq")) is not int)
        ):
            failures.append("guard_sessions")
            continue
        total_responses += row_responses
        total_checked += row_checked
        if not (launched or turns > 0):
            continue
        if row_responses < 1:
            failures.append("responses_absent")
        elif row_checked != row_responses:
            failures.append("guard_missed_response")
        elif after is not True:
            failures.append("no_guard_after_response")
        if row_responses >= 1:
            needs_check = True
    if total_responses != responses or total_checked != covered:
        failures.append("guard_sessions")
    if covered != responses:
        failures.append("guard_missed_response")
    if needs_check and checks < 1:
        failures.append("no_guard_after_response")
    return list(dict.fromkeys(failures))


def _launch_record_ok(item: Any) -> bool:
    if not isinstance(item, dict) or set(item) - {"seq", "role"}:
        return False
    if item.get("role") not in ROLES:
        return False
    return "seq" not in item or type(item.get("seq")) is int


def _launched_row(rows: Sequence[Any], entry: Mapping[str, Any], used: set[int]) -> int | None:
    for index, row in enumerate(rows):
        if index in used or not isinstance(row, dict) or row.get("role") != entry.get("role"):
            continue
        if "seq" in entry:
            if row.get("seq") == entry.get("seq"):
                return index
        elif "seq" not in row:
            return index
    return None


def _launched_set_failures(ledger: Ledger) -> list[str]:
    """Why ``budget.launched_sessions`` does not show every start harvested.

    The set is its own observation. A missing or malformed one fails closed, and
    that is not the same as an empty set. Each entry needs a harvested guard row
    whose ``launched`` flag was taken from this record. A product path that
    opened a pull request needs a launched implementer.
    """

    budget = ledger.observations("budget")
    if "launched_sessions" not in budget:
        return ["launched_set_missing"]
    launched = budget.get("launched_sessions")
    if not isinstance(launched, list) or any(not _launch_record_ok(item) for item in launched):
        return ["launched_set_missing"]
    failures: list[str] = []
    guard = budget.get("guard_checks")
    rows = guard.get("sessions") if isinstance(guard, dict) else None
    if not isinstance(rows, list):
        if launched:
            failures.append("launched_unharvested")
    else:
        used: set[int] = set()
        for entry in launched:
            index = _launched_row(rows, entry, used)
            if index is None:
                failures.append("launched_unharvested")
                continue
            used.add(index)
            if rows[index].get("launched") is not True:
                failures.append("guard_sessions")
        for index, row in enumerate(rows):
            if isinstance(row, dict) and row.get("launched") is True and index not in used:
                failures.append("guard_sessions")
    if ledger.observations("progress").get("pull_request_exists") is True and not any(
        isinstance(item, dict) and item.get("role") == IMPLEMENTER for item in launched
    ):
        failures.append("launched_set_empty")
    return list(dict.fromkeys(failures))


def ceiling_enforcement_failures(ledger: Ledger) -> list[str]:
    """Why the recorded ledger does not show the approved ceiling enforced; empty when it
    does. Reads only recorded observations and session rows.

    Every session's charge, observed or its full reservation share when unknown, stays
    inside its bounds and the attempt's; no bound was breached; every assistant record
    that carries usage is a response, id or not. The launched set is its own
    observation, taken from the pod watcher or a runner log turn start, and a guard
    row's ``launched`` flag is that record. Every launched session has a harvested
    row with at least one response, every response checked, and a guard check after
    its first response. A session with a recorded turn has the same coverage. A
    product path that opened a pull request has at least one launched implementer.
    A check of an empty harvest does not count. The item's earlier charge plus this
    attempt's stays at or under the recorded item cap.
    """

    observed = ledger.observations("budget")
    ceiling = Ceiling.from_json(observed.get("ceiling"))
    rows = ledger.sessions()
    failures: list[str] = []
    if ceiling is None or not rows:
        failures.append("ledger_incomplete")
    else:
        failures.extend(f"bound:{item}" for item in budget_findings(rows, ceiling))
    if observed.get("allowance_breach") != []:
        failures.append("bound_breached")
    failures.extend(_guard_record_failures(observed.get("guard_checks")))
    failures.extend(_launched_set_failures(ledger))
    item = observed.get("item_ledger")
    cap = item.get("cap") if isinstance(item, dict) else None
    before = item.get("charged_before") if isinstance(item, dict) else None
    if type(cap) is not int or type(before) is not int:
        failures.append("item_ledger_missing")
    elif before + sum(charged(row)[0] for row in rows) > cap:
        failures.append("item_cap")
    return failures


def usage_exact(ledger: Ledger) -> dict[str, Any]:
    """An observation, never a pass condition: whether every slot holds observed usage,
    with the capture rule's recorded failure codes."""

    rows = ledger.sessions()
    capture = ledger.observations("budget").get("capture")
    codes = capture.get("failures") if isinstance(capture, dict) else None
    return {
        "exact": bool(rows) and all(row["output_tokens"] is not None for row in rows),
        "unknown_slots": sum(1 for row in rows if row["output_tokens"] is None),
        "failure_codes": list(codes) if isinstance(codes, list) else None,
    }


_INVENTORY = (
    "namespace",
    "grant_namespace",
    "cluster_scoped",
    "registry_container",
    "local_images",
    "processes",
    "work_dir",
    "forge_issues",
    "forge_prs",
    "forge_branches",
    "slack_messages",
    "dispatcher_pods",
    "volumes",
)


def _judge_cleanup(ledger: Ledger) -> tuple[dict[str, bool], dict[str, int]]:
    rows = ledger.cleanup_rows()
    steps = [row for row in rows if row["kind"].startswith("step:")]
    inventory = {
        row["kind"].split(":", 1)[1]: row["status"]
        for row in rows
        if row["kind"].startswith("inventory:")
    }
    observed = ledger.observations("cleanup")
    scan = observed.get("token_scan")
    checks = {
        "steps_recorded": bool(steps),
        "every_step_ok": bool(steps) and all(row["status"] == "ok" for row in steps),
        "inventory_complete": all(kind in inventory for kind in _INVENTORY),
        "everything_absent": bool(inventory)
        and all(status == "absent" for status in inventory.values()),
        "registry_tags_removed": _true(observed, "registry_tags_absent"),
        "no_secret_bytes": isinstance(scan, dict)
        and bool(scan)
        and not any(bool((item or {}).get("token_bytes_found")) for item in scan.values()),
    }
    return checks, {"steps": len(steps), "inventory": len(inventory)}


_JUDGES: dict[str, Callable[[Ledger], tuple[dict[str, bool], dict[str, int]]]] = {
    "slack": _judge_slack,
    "admission": _judge_admission,
    "owner": _judge_owner,
    "grant": _judge_grant,
    "progress": _judge_progress,
    "refusal": _judge_refusal,
    "budget": _judge_budget,
    "cleanup": _judge_cleanup,
}


def judge(ledger: Ledger, criterion: str) -> dict[str, Any]:
    """A verdict from the ledger alone: booleans and counts, no identifiers."""

    rehearsal = ledger.live_kind() == "rehearse"
    if criterion == "all":
        names = list(REHEARSAL_CRITERIA if rehearsal else CRITERIA)
    elif criterion in _JUDGES:
        if rehearsal and criterion not in REHEARSAL_CRITERIA:
            raise Refusal("rehearsal_ledger", "--criterion")
        names = [criterion]
    else:
        raise Refusal("arguments_invalid", "--criterion")
    checks: dict[str, bool] = {}
    counts: dict[str, int] = {}
    for name in names:
        found, number = _JUDGES[name](ledger)
        checks.update({f"{name}.{key}": value for key, value in found.items()})
        counts.update({f"{name}.{key}": value for key, value in number.items()})
    budget = ledger.observations("budget")
    roles = ledger.observations("grant").get("role_models")
    observations: dict[str, Any] = {}
    if "budget" in names:
        observations["budget.usage_exact"] = usage_exact(ledger)
        observations["budget.ceiling_enforcement_failures"] = ceiling_enforcement_failures(ledger)
    return {
        "criterion": criterion,
        "verdict": "pass" if checks and all(checks.values()) else "fail",
        "checks": checks,
        "counts": counts,
        "observations": observations,
        "role_models": roles if isinstance(roles, dict) else {},
        "ceiling": budget.get("ceiling") if isinstance(budget.get("ceiling"), dict) else {},
    }


def role_models(sessions: Sequence[Any]) -> dict[str, list[str]]:
    """Each role's observed models, for the ledger and the judge output."""

    found: dict[str, set[str]] = {}
    for session in sessions:
        found.setdefault(str(session.role), set()).update(str(m) for m in session.models)
    return {role: sorted(models) for role, models in sorted(found.items())}


def transcript_error_classes(sessions: Sequence[Any]) -> list[dict[str, Any]]:
    """Status and provider type of every synthetic or API-error record, no text."""

    return [
        {
            "http_status": item.get("http_status"),
            "provider_error_type": item.get("provider_error_type"),
        }
        for session in sessions
        for item in session.errors
    ]


@dataclasses.dataclass(frozen=True)
class AttemptFacts:
    """The attempt's native sessions across every runner pod, each record counted once."""

    sessions: tuple[Any, ...]
    malformed_lines: int
    synthetic_records: int


def attempt_facts(pods: Sequence[Mapping[str, bytes]], *, not_before: float) -> AttemptFacts:
    """Parse each runner pod's harvested files in the order the pods appeared. Pure.

    A later pod (the publish continuation's, or a restart's) replays the
    conversation into a new native session; an assistant message an earlier
    pod already recorded is that replay and is not charged again.
    """

    sessions: list[Any] = []
    malformed = synthetic = 0
    seen: frozenset[str] = frozenset()
    for files in pods:
        facts = parse_native_transcripts(files, exclude_ids=seen, not_before=not_before)
        sessions += list(facts.sessions)
        malformed += int(facts.malformed_lines)
        synthetic += int(facts.synthetic_records)
        seen = seen | frozenset(facts.message_ids())
    return AttemptFacts(tuple(sessions), malformed, synthetic)


def counts_snapshot(**counts: int) -> dict[str, int]:
    return {key: int(counts[key]) for key in _COUNT_KEYS}
