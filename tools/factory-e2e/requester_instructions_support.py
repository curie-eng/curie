"""Pure logic of the requester coding instructions proof driver (#5592, ADR 0278).

Everything here is offline: the documents the proof stores, the private
ledger (the owner enforcement ledger plus a slot table), the spend caps and
the launch gate decision, the caller's live inputs (stat only where the
preflight must not read), the per-slot assertions over recorded evidence,
the forge cleanup bookkeeping and the judge. The two programs the driver runs
in a clean child and in a runner Pod live here too, each with an internal
deadline under 60 seconds and a failing exit on any input it cannot read.
"""

from __future__ import annotations

import dataclasses
import json
import os
import re
import sqlite3
import stat
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

import personal_owner_enforcement_support as poe
import personal_profile_support as pps
from curie_internal.requester_coding_instructions import text_sha256

LEDGER_FILE = "requester-instructions.sqlite3"
CRITERIA = ("9", "10")
# Precondition 5: at most 3 live slots, 5 USD per model session, at most one
# continuation per slot (so at most two native launches per slot).
MAX_SLOTS = 3
SESSION_USD = 5
MAX_CONTINUATIONS = 1
LAUNCHES_PER_SLOT = 1 + MAX_CONTINUATIONS
ANNOTATION_PREFIX = "Requester preferences:"
# Every program the driver runs in a child or a Pod stops itself before 60 s.
GATE_DEADLINE_SECONDS = 45
# The rehearsal's three actors: two linked to the two OIDC principals, one not.
REHEARSAL_ACTORS = (("first", 9100001), ("second", 9100002), ("unlinked", 9100003))
# Each live slot: who applies the label, and which document is current then.
SLOT_PLAN = {1: ("owner", "owner"), 2: ("second", "second"), 3: ("owner", None)}
PERMISSIONS_WITH_WRITE = frozenset({"admin", "maintain", "write"})
_NAME = re.compile(r"[a-z][a-z0-9]{0,15}")
_NONCE = re.compile(r"[0-9a-f]{8,32}")
_REPO = re.compile(r"[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}")
_DIGITS = re.compile(r"[1-9][0-9]{0,19}")

FIXES: dict[str, str] = {
    "token_file_missing": "pass the named token file: a private (0600, yours) file of one line",
    "token_file_unsafe": "the named token file must be a regular 0600 file you own, 1 to 512 bytes",
    "app_kit_invalid": "--app-dir must be a private directory holding app.json "
    "({id, installation_id, repo}) and a private app.pem",
    "conflict_file_invalid": "--conflict-file must be a private JSON object with preference "
    "(one line), forbidden_added_line_regex (a valid pattern) and rule_path (a repository path)",
    "model_credential_invalid": "--model-credential-file must be a private JSON object with "
    "base_url (https) and api_key",
    "priority_class_invalid": "--priority-class must name an existing PriorityClass",
    "run_id_used": "choose a run ID this ledger has never recorded",
    "slot_cap": "a live run records at most three slots, numbered 1 to 3",
    "slot_recorded": "a slot is recorded once, before its label",
    "slot_unrecorded": "a slot's label is applied only after the slot is recorded",
    "live_probe_failed": "a live precondition probe failed; nothing was labeled or spent",
    "live_mode_required": "slots are recorded only in a live run",
    "criterion_invalid": "pass --criterion 9, 10 or all",
    "live_arguments_invalid": "--slot-seconds must be 300 to 3600 and --model one model id",
}


class Refusal(poe.Refusal):
    """A refusal with this driver's fixes first. Codes and flag names only."""

    def payload(self) -> dict[str, str]:
        fix = FIXES.get(self.code)
        if fix is None:
            return super().payload()
        return {"error": self.code, "fix": f"{fix} ({self.detail})" if self.detail else fix}


def caps() -> dict[str, int]:
    return {
        "max_slots": MAX_SLOTS,
        "session_usd": SESSION_USD,
        "max_continuations_per_slot": MAX_CONTINUATIONS,
        "launches_per_slot": LAUNCHES_PER_SLOT,
    }


# --------------------------------------------------------------------------
# Documents
# --------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Document:
    """One coding instructions document. Its text never leaves the driver's memory."""

    role: str
    text: str
    canary: str
    marker: str
    preferences: tuple[str, ...]

    def __repr__(self) -> str:
        return f"<document {self.role}>"

    @property
    def sha256(self) -> str:
        # The production digest of the stored document (ADR 0278).
        return text_sha256(self.text)

    def needles(self) -> tuple[str, ...]:
        """The strings a leak would carry: the canary line and every preference line.

        The bare marker is not one: a diff reviewer legitimately reads it in the diff.
        """

        return (self.canary, *self.preferences)


def make_document(role: str, nonce: str, *, conflict: str = "") -> Document:
    if not _NAME.fullmatch(role) or not _NONCE.fullmatch(nonce):
        raise ValueError("document role and nonce must be short lowercase tokens")
    if "\n" in conflict or "\r" in conflict:
        raise ValueError("a conflicting preference is one line")
    canary = f"Proof canary RQP-{role.upper()}-{nonce}"
    marker = f"Requester-Marker: rqp-{role}-{nonce}"
    preferences = [f"End every file you create or change with the exact line `{marker}`."]
    if conflict:
        preferences.append(conflict)
    lines = [f"# Requester coding preferences ({role})", canary, ""]
    lines += [f"{index}. {text}" for index, text in enumerate(preferences, 1)]
    return Document(role, "\n".join(lines) + "\n", canary, marker, tuple(preferences))


def needles_of(documents: Iterable[Document]) -> list[str]:
    return sorted({needle for document in documents for needle in document.needles()})


def hits(data: bytes, needles: Sequence[str]) -> int:
    """How many needles occur in ``data``, raw or JSON escaped."""

    found = 0
    for needle in needles:
        forms = {needle.encode(), json.dumps(needle)[1:-1].encode()}
        if any(form in data for form in forms):
            found += 1
    return found


# --------------------------------------------------------------------------
# Live inputs (the preflight only stats secrets)
# --------------------------------------------------------------------------


def stat_token_file(path: Path | None, flag: str) -> None:
    """A user token file exists and is private. Stat only; never opened here."""

    if path is None or not str(path):
        raise Refusal("token_file_missing", flag)
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        raise Refusal("token_file_missing", flag) from None
    except OSError:
        raise Refusal("token_file_unsafe", flag) from None
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o600
        or not 1 <= info.st_size <= 512
    ):
        raise Refusal("token_file_unsafe", flag)


def read_token_file(path: Path, flag: str) -> pps.HeldSecret:
    stat_token_file(path, flag)
    try:
        lines = path.read_text(encoding="ascii").splitlines()
    except (OSError, UnicodeDecodeError):
        raise Refusal("token_file_unsafe", flag) from None
    if len(lines) != 1 or not re.fullmatch(r"[A-Za-z0-9_]{20,255}", lines[0]):
        raise Refusal("token_file_unsafe", flag)
    return pps.HeldSecret(lines[0])


def _require_private_file(path: Path, code: str, flag: str) -> None:
    """The owner driver's stat-only check, refused under this driver's code."""

    try:
        poe.require_private_file(path, code, flag)
    except poe.Refusal:
        raise Refusal(code, flag) from None


def _private_json(path: Path, code: str, flag: str) -> dict[str, Any]:
    if not path.is_absolute():
        raise Refusal(code, flag)
    _require_private_file(path, code, flag)
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError):
        raise Refusal(code, flag) from None
    if not isinstance(body, dict):
        raise Refusal(code, flag)
    return body


def load_app_kit(directory: Path) -> dict[str, str]:
    """``app.json`` is read (identifiers only); ``app.pem`` is stat only."""

    if not directory.is_absolute():
        raise Refusal("app_kit_invalid", "--app-dir")
    try:
        pps.require_private_dir(directory, code="app_kit_invalid")
    except pps.Refusal:
        raise Refusal("app_kit_invalid", "--app-dir") from None
    body = _private_json(directory / "app.json", "app_kit_invalid", "app.json")
    app = {key: str(body.get(key, "")) for key in ("id", "installation_id", "repo")}
    if (
        set(body) - set(app)
        or not _DIGITS.fullmatch(app["id"])
        or not _DIGITS.fullmatch(app["installation_id"])
        or not _REPO.fullmatch(app["repo"])
    ):
        raise Refusal("app_kit_invalid", "app.json")
    _require_private_file(directory / "app.pem", "app_kit_invalid", "app.pem")
    return app


@dataclasses.dataclass(frozen=True)
class Conflict:
    """A preference the fixture repository's guidance overrules, and how to see who won."""

    preference: str
    forbidden_added_line_regex: str
    rule_path: str


def load_conflict(path: Path) -> Conflict:
    body = _private_json(path, "conflict_file_invalid", "--conflict-file")
    preference = body.get("preference")
    pattern = body.get("forbidden_added_line_regex")
    rule = body.get("rule_path")
    if set(body) - {"preference", "forbidden_added_line_regex", "rule_path"}:
        raise Refusal("conflict_file_invalid", "unknown keys")
    if (
        not isinstance(preference, str)
        or not 8 <= len(preference) <= 400
        or "\n" in preference
        or "\r" in preference
    ):
        raise Refusal("conflict_file_invalid", "preference")
    if not isinstance(pattern, str) or not pattern:
        raise Refusal("conflict_file_invalid", "forbidden_added_line_regex")
    try:
        re.compile(pattern)
    except re.error:
        raise Refusal("conflict_file_invalid", "forbidden_added_line_regex") from None
    if (
        not isinstance(rule, str)
        or not re.fullmatch(r"[A-Za-z0-9_.][A-Za-z0-9_./-]{0,200}", rule)
        or ".." in rule.split("/")
    ):
        raise Refusal("conflict_file_invalid", "rule_path")
    return Conflict(preference, pattern, rule)


def stat_model_credential(path: Path | None) -> None:
    if path is None or not str(path) or not path.is_absolute():
        raise Refusal("model_credential_invalid", "--model-credential-file")
    _require_private_file(path, "model_credential_invalid", "--model-credential-file")


def load_model_credential(path: Path) -> tuple[pps.HeldSecret, pps.HeldSecret]:
    """``(base_url, api_key)``, both held. Read only by the proof child."""

    body = _private_json(path, "model_credential_invalid", "--model-credential-file")
    base, key = body.get("base_url"), body.get("api_key")
    if set(body) != {"base_url", "api_key"} or not isinstance(base, str):
        raise Refusal("model_credential_invalid", "--model-credential-file")
    if not re.fullmatch(r"https://[A-Za-z0-9.:-]{1,253}(/[A-Za-z0-9._~/-]*)?", base):
        raise Refusal("model_credential_invalid", "base_url")
    if not isinstance(key, str) or not 8 <= len(key) <= 512 or any(c.isspace() for c in key):
        raise Refusal("model_credential_invalid", "api_key")
    return pps.HeldSecret(base.rstrip("/")), pps.HeldSecret(key)


# --------------------------------------------------------------------------
# Preflight composition
# --------------------------------------------------------------------------

# The owner enforcement driver's read-only checks, then this driver's ledger.
BASE_PREFLIGHT = ("owner_enforcement_checks", "run_id_unused")
LIVE_PREFLIGHT = (
    "app_kit",
    "owner_token_file",
    "second_token_file",
    "model_credential_file",
    "conflict_file",
    "priority_class",
)
# Run by ``run`` itself in the proof child, immediately before the first label.
LIVE_START_PROBES = ("model", "app", "owner", "second")


def preflight_plan(command: str) -> tuple[str, ...]:
    if command == "rehearse":
        return BASE_PREFLIGHT
    if command in ("preflight", "run"):
        return BASE_PREFLIGHT + LIVE_PREFLIGHT
    raise ValueError(f"no preflight plan for {command}")


def run_id_unused(state_dir: Path, run_id: str) -> None:
    if not (state_dir / LEDGER_FILE).exists():
        return
    try:
        ledger = RequesterLedger.open(state_dir, run_id, create=False)
    except poe.Refusal as refusal:
        if refusal.code == "ledger_missing":
            return
        raise
    ledger.close()
    raise Refusal("run_id_used", "--run-id")


# --------------------------------------------------------------------------
# The private ledger: the owner enforcement ledger plus a slot table
# --------------------------------------------------------------------------

_SLOT_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS slots (run_id TEXT NOT NULL, slot INTEGER NOT NULL, "
    "actor TEXT NOT NULL, issue TEXT NOT NULL, recorded_at REAL NOT NULL, labeled_at REAL, "
    "launches INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (run_id, slot))"
)


class RequesterLedger(poe.OwnerLedger):
    """Runs, observations and cleanup as the owner ledger keeps them, plus live slots."""

    def __repr__(self) -> str:
        return "<requester ledger>"

    @classmethod
    def open(
        cls,
        state_dir: Path,
        run_id: str,
        *,
        create: bool,
        scrub: Callable[[str], str] = lambda text: text,
    ) -> RequesterLedger:
        path = state_dir / LEDGER_FILE
        if create:
            try:
                fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            except OSError:
                raise Refusal("ledger_unsafe") from None
            os.close(fd)
        elif not path.exists():
            raise Refusal("ledger_missing")
        poe.require_private_file(path, "ledger_unsafe", LEDGER_FILE)
        connection = sqlite3.connect(path, timeout=10, isolation_level=None)
        connection.execute("PRAGMA journal_mode=DELETE")
        for statement in (*poe._SCHEMA, _SLOT_SCHEMA):
            connection.execute(statement)
        ledger = cls(connection, run_id, scrub)
        if not create and ledger.run() is None:
            connection.close()
            raise Refusal("ledger_missing")
        return ledger

    def slots(self) -> list[dict[str, Any]]:
        keys = ("slot", "actor", "issue", "recorded_at", "labeled_at", "launches")
        return [
            dict(zip(keys, row, strict=True))
            for row in self.db.execute(
                "SELECT " + ", ".join(keys) + " FROM slots WHERE run_id = ? ORDER BY slot",
                (self.run_id,),
            )
        ]

    def record_slot(self, slot: int, actor: str, issue: str, now: float) -> None:
        """Recorded before its label is applied; at most ``MAX_SLOTS``, each once."""

        if (self.run() or {}).get("mode") != "live":
            raise Refusal("live_mode_required")
        if type(slot) is not int or not 1 <= slot <= MAX_SLOTS:
            raise Refusal("slot_cap", str(slot))
        existing = self.slots()
        if any(row["slot"] == slot for row in existing):
            raise Refusal("slot_recorded", str(slot))
        if len(existing) >= MAX_SLOTS:
            raise Refusal("slot_cap", str(slot))
        self.db.execute(
            "INSERT INTO slots (run_id, slot, actor, issue, recorded_at) VALUES (?, ?, ?, ?, ?)",
            (self.run_id, slot, actor, issue, now),
        )

    def mark_labeled(self, slot: int, now: float) -> None:
        row = next((item for item in self.slots() if item["slot"] == slot), None)
        if row is None:
            raise Refusal("slot_unrecorded", str(slot))
        if row["labeled_at"] is not None:
            raise Refusal("slot_recorded", str(slot))
        self.db.execute(
            "UPDATE slots SET labeled_at = ? WHERE run_id = ? AND slot = ?",
            (now, self.run_id, slot),
        )

    def launches(self, slot: int) -> int:
        row = next((item for item in self.slots() if item["slot"] == slot), None)
        return int(row["launches"]) if row is not None else 0

    def count_launch(self, slot: int) -> int:
        self.db.execute(
            "UPDATE slots SET launches = launches + 1 WHERE run_id = ? AND slot = ?",
            (self.run_id, slot),
        )
        return self.launches(slot)


def launch_permitted(
    slot: int | None, *, labeled: bool, launches_so_far: int, live_open: bool
) -> bool:
    """Whether the driver releases one more native launch for ``slot``.

    Unknown slots, unlabeled slots, a closed live window and a slot that already
    used its first session and its one continuation are all refused.
    """

    return (
        live_open
        and labeled
        and type(slot) is int
        and 1 <= slot <= MAX_SLOTS
        and type(launches_so_far) is int
        and 0 <= launches_so_far < LAUNCHES_PER_SLOT
    )


# --------------------------------------------------------------------------
# Programs run outside the driver process
# --------------------------------------------------------------------------

# A clean child (``env -i`` with an empty HOME): one model completion. stdin is
# ``{"base_url", "api_key", "model"}``; argv[1] is the deadline in seconds.
MODEL_PROBE_PROGRAM = """\
import json, signal, sys, urllib.error, urllib.request
def done(ok, status=None, error=None, code=0):
    print(json.dumps({"ok": ok, "status": status, "error": error}))
    raise SystemExit(code)
def expired(*_):
    done(False, error="deadline", code=3)
try:
    deadline = float(sys.argv[1])
    request = json.loads(sys.stdin.read())
    base, key, model = request["base_url"], request["api_key"], request["model"]
    assert 0 < deadline < 60 and base.startswith("https://") and key and model
except Exception:
    done(False, error="input", code=2)
signal.signal(signal.SIGALRM, expired)
signal.setitimer(signal.ITIMER_REAL, deadline)
body = json.dumps({"model": model, "max_tokens": 8,
                   "messages": [{"role": "user", "content": "Reply with OK."}]}).encode()
call = urllib.request.Request(base + "/v1/messages", data=body, method="POST", headers={
    "x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json"})
try:
    with urllib.request.urlopen(call, timeout=deadline) as response:
        answer = json.loads(response.read(1 << 20))
        status = response.status
except urllib.error.HTTPError as error:
    done(False, status=error.code, error="http", code=1)
except Exception as error:
    done(False, error=type(error).__name__, code=1)
ok = status == 200 and isinstance(answer, dict) and bool(answer.get("content"))
done(ok, status=status, error=None if ok else "empty", code=0 if ok else 1)
"""

# In a runner Pod: every reviewer record in the native transcripts, scanned for
# the needles on stdin (a JSON list). argv[1] is the deadline, argv[2:] the roots.
# A reviewer record is any line of a file under a ``subagents`` directory, or a
# line marked ``isSidechain``.
REVIEWER_SCAN_PROGRAM = """\
import json, os, signal, sys
def done(body, code):
    print(json.dumps(body))
    raise SystemExit(code)
def expired(*_):
    done({"error": "deadline"}, 3)
try:
    deadline = float(sys.argv[1])
    roots = sys.argv[2:]
    needles = json.loads(sys.stdin.read())
    assert 0 < deadline < 60 and roots and isinstance(needles, list) and needles
    assert all(isinstance(item, str) and item for item in needles)
except Exception:
    done({"error": "input"}, 2)
signal.signal(signal.SIGALRM, expired)
signal.setitimer(signal.ITIMER_REAL, deadline)
forms = [(n.encode(), json.dumps(n)[1:-1].encode()) for n in needles]
files = records = 0
found = set()
for root in roots:
    for base, _dirs, names in os.walk(root, onerror=lambda error: None):
        for name in names:
            if not name.endswith(".jsonl"):
                continue
            path = os.path.join(base, name)
            side = "subagents" in path.split(os.sep)
            try:
                with open(path, "rb") as stream:
                    lines = stream.read(64 << 20).splitlines()
            except OSError:
                continue
            seen = False
            for line in lines:
                if not side:
                    try:
                        if json.loads(line).get("isSidechain") is not True:
                            continue
                    except (ValueError, AttributeError):
                        continue
                seen = True
                records += 1
                for index, (raw, escaped) in enumerate(forms):
                    if raw in line or escaped in line:
                        found.add(index)
            files += seen
done({"error": None, "files": files, "records": records, "hits": len(found)}, 0)
"""


@dataclasses.dataclass(frozen=True)
class ReviewerScan:
    files: int = 0
    records: int = 0
    hits: int = 0
    error: str | None = "unscanned"


def reviewer_scan_result(returncode: int, stdout: bytes) -> ReviewerScan:
    """A scan answer; anything unreadable is an error, never a clean scan."""

    try:
        body = json.loads(stdout.decode().strip().splitlines()[-1])
    except (ValueError, IndexError, UnicodeDecodeError):
        return ReviewerScan(error="unreadable")
    if not isinstance(body, dict):
        return ReviewerScan(error="unreadable")
    if returncode != 0 or body.get("error") is not None:
        error = body.get("error")
        return ReviewerScan(error=error if isinstance(error, str) else "failed")
    files, records, found = (body.get(key) for key in ("files", "records", "hits"))
    if not (
        type(files) is int
        and type(records) is int
        and type(found) is int
        and min(files, records, found) >= 0
    ):
        return ReviewerScan(error="unreadable")
    return ReviewerScan(files, records, found, None)


def combine_scans(scans: Iterable[ReviewerScan]) -> ReviewerScan:
    """One slot's scans across its Pods; any error makes the slot's scan an error."""

    items = list(scans)
    if not items:
        return ReviewerScan()
    errors = [item.error for item in items if item.error is not None]
    if errors:
        return ReviewerScan(error=errors[0])
    return ReviewerScan(
        sum(item.files for item in items),
        sum(item.records for item in items),
        sum(item.hits for item in items),
        None,
    )


def model_probe_result(returncode: int, stdout: bytes) -> dict[str, Any]:
    try:
        body = json.loads(stdout.decode().strip().splitlines()[-1])
    except (ValueError, IndexError, UnicodeDecodeError):
        return {"ok": False, "status": None, "error": "unreadable"}
    if not isinstance(body, dict):
        return {"ok": False, "status": None, "error": "unreadable"}
    status = body.get("status") if type(body.get("status")) is int else None
    error = body.get("error") if isinstance(body.get("error"), str) else None
    return {"ok": returncode == 0 and body.get("ok") is True, "status": status, "error": error}


def user_probe_result(
    user_status: int, user: Any, permission_status: int, permission: Any
) -> dict[str, Any]:
    """``GET /user`` and the account's permission on the fixture repository."""

    account = user.get("id") if isinstance(user, dict) else None
    login = user.get("login") if isinstance(user, dict) else None
    level = permission.get("permission") if isinstance(permission, dict) else None
    role = permission.get("role_name") if isinstance(permission, dict) else None
    ok = (
        user_status == 200
        and type(account) is int
        and account > 0
        and isinstance(login, str)
        and bool(login)
        and permission_status == 200
        and (level in PERMISSIONS_WITH_WRITE or role in PERMISSIONS_WITH_WRITE)
    )
    return {
        "ok": ok,
        "account_id": account if ok else None,
        "login": login if ok else None,
        "write": ok,
    }


# --------------------------------------------------------------------------
# Per-slot assertions over recorded evidence
# --------------------------------------------------------------------------


def expected_annotation(digest: str) -> str:
    return f"{ANNOTATION_PREFIX} applied; sha256:{digest}"


def annotation_of(body: str | None) -> tuple[str | None, int]:
    """The body's last non-blank line when it is an annotation, and how many it has."""

    lines = [line.rstrip() for line in (body or "").splitlines()]
    while lines and not lines[-1].strip():
        lines.pop()
    count = sum(1 for line in lines if line.startswith(ANNOTATION_PREFIX))
    last = lines[-1] if lines else ""
    return (last if last.startswith(ANNOTATION_PREFIX) else None), count


def added_lines(files: Sequence[Mapping[str, Any]]) -> list[str]:
    """Every added line in a pull request's file patches."""

    found: list[str] = []
    for item in files:
        patch = item.get("patch") if isinstance(item, Mapping) else None
        for line in (patch or "").splitlines() if isinstance(patch, str) else ():
            if line.startswith("+") and not line.startswith("+++"):
                found.append(line[1:])
    return found


@dataclasses.dataclass(frozen=True)
class SlotEvidence:
    """What one live slot recorded, read from the API's records and the pull request."""

    recorded_before_label: bool
    requester_matches: bool
    evidence: str | None
    pull_request_found: bool
    body: str | None
    files: tuple[Mapping[str, Any], ...]
    reviewer: ReviewerScan


def slot_observations(
    slot: int,
    found: SlotEvidence,
    *,
    document: Document,
    conflict_regex: str | None = None,
) -> dict[str, Any]:
    """Criterion 10's observations for one slot.

    ``document`` is the slot's own document for slots 1 and 2, and the owner's
    cleared document for slot 3, whose effect the control must lack.
    """

    last, count = annotation_of(found.body if found.pull_request_found else None)
    added = added_lines(found.files) if found.pull_request_found else []
    reviewer = found.reviewer
    observed: dict[str, Any] = {
        "recorded_before_label": found.recorded_before_label,
        "requester_matches": found.requester_matches,
        "evidence": found.evidence,
        "pull_request_found": found.pull_request_found,
        "reviewer_transcripts_seen": reviewer.error is None and reviewer.records > 0,
        "reviewer_hits": reviewer.hits if reviewer.error is None else None,
    }
    if slot in (1, 2):
        observed["annotation_matches"] = (
            found.pull_request_found and count == 1 and last == expected_annotation(document.sha256)
        )
    if slot == 1:
        observed["marker_in_diff"] = any(document.marker in line for line in added)
        observed["repository_rule_won"] = (
            found.pull_request_found
            and bool(added)
            and conflict_regex is not None
            and not any(re.search(conflict_regex, line) for line in added)
        )
    if slot == 3:
        observed["annotation_absent"] = found.pull_request_found and count == 0
        observed["marker_absent"] = (
            found.pull_request_found
            and bool(added)
            and not any(document.marker in line for line in added)
        )
    return {f"slot{slot}.{key}": value for key, value in observed.items()}


# --------------------------------------------------------------------------
# Forge cleanup bookkeeping
# --------------------------------------------------------------------------


def forge_cleanup_rows(
    *,
    issues: Mapping[str, str | None],
    pulls: Mapping[str, str | None],
    branches: Mapping[str, bool | None],
    label: bool | None,
) -> list[tuple[str, str, str]]:
    """``(kind, ref, status)`` for every forge object the run created.

    Issues cannot be deleted through the REST API, so a closed issue counts as
    removed; a pull request must be closed; a branch and the run's label must be
    gone. ``None`` is an unanswered read and is recorded as present.
    """

    rows: list[tuple[str, str, str]] = []
    for number, state in sorted(issues.items()):
        rows.append(("forge_issues", f"#{number}", "absent" if state == "closed" else "present"))
    for number, state in sorted(pulls.items()):
        rows.append(("forge_pulls", f"#{number}", "absent" if state == "closed" else "present"))
    for name, present in sorted(branches.items()):
        rows.append(("forge_branches", name, "absent" if present is False else "present"))
    rows.append(("forge_label", "run", "absent" if label is False else "present"))
    if not issues:
        rows.append(("forge_issues", "none", "absent"))
    if not pulls:
        rows.append(("forge_pulls", "none", "absent"))
    if not branches:
        rows.append(("forge_branches", "none", "absent"))
    return rows


# --------------------------------------------------------------------------
# The judge: ledger only, sanitized, a missing proof never passes
# --------------------------------------------------------------------------

EXPECTED: dict[str, dict[str, Any]] = {
    "9": {
        "setup.fake_model": True,
        "setup.principals_distinct": True,
        "link.first": True,
        "link.second": True,
        "resolve.first": True,
        "resolve.second": True,
        "resolve.unlinked": True,
        "document.first.digest_matches": True,
        "document.second.digest_matches": True,
        "document.digests_distinct": True,
        "request.first.state": "selected",
        "request.first.evidence": "applied",
        "request.first.digest_matches": True,
        "request.second.state": "selected",
        "request.second.evidence": "applied",
        "request.second.digest_matches": True,
        "request.unlinked.state": "absent",
        "request.unlinked.evidence": "none",
        "logs.api_captured": True,
        "logs.worker_captured": True,
        "logs.runner_captured": True,
        "logs.canary_hits": 0,
    },
    "10": {
        "probe.model": True,
        "probe.app": True,
        "probe.owner": True,
        "probe.second": True,
        "link.owner": True,
        "link.second": True,
        "document.owner.digest_matches": True,
        "document.second.digest_matches": True,
        "document.owner_cleared": True,
        "spend.slots_within_cap": True,
        "spend.launches_within_cap": True,
        **{
            f"slot{slot}.{key}": value
            for slot in (1, 2, 3)
            for key, value in (
                ("recorded_before_label", True),
                ("requester_matches", True),
                ("evidence", "none" if slot == 3 else "applied"),
                ("pull_request_found", True),
                ("reviewer_transcripts_seen", True),
                ("reviewer_hits", 0),
            )
        },
        "slot1.annotation_matches": True,
        "slot1.marker_in_diff": True,
        "slot1.repository_rule_won": True,
        "slot2.annotation_matches": True,
        "slot3.annotation_absent": True,
        "slot3.marker_absent": True,
    },
}
# The run mode that can prove each criterion.
PROOF_MODE = {"9": "rehearse", "10": "live"}
# Cleanup kinds each proof must have recorded, all absent or ok.
CLEANUP_KINDS = {
    "9": ("namespace", "grant_namespace", "registry", "processes", "private_dir"),
    "10": (
        "namespace",
        "grant_namespace",
        "registry",
        "processes",
        "private_dir",
        "forge_issues",
        "forge_pulls",
        "forge_branches",
        "forge_label",
    ),
}
CLEAN_STATUSES = frozenset({"absent", "ok"})


@dataclasses.dataclass(frozen=True)
class RunRecord:
    """Everything the judge reads from one run of the ledger."""

    run_id: str
    candidate: str
    mode: str
    outcome: str | None
    observations: Mapping[str, Mapping[str, Any]]
    cleanup: Sequence[Mapping[str, Any]]
    slots: Sequence[Mapping[str, Any]]


def record_of(ledger: RequesterLedger) -> RunRecord:
    run = ledger.run()
    if run is None:
        raise Refusal("ledger_missing")
    return RunRecord(
        run_id=ledger.run_id,
        candidate=str(run["candidate"]),
        mode=str(run["mode"]),
        outcome=run["outcome"],
        observations={name: ledger.observations(name) for name in CRITERIA},
        cleanup=ledger.cleanup_rows(),
        slots=ledger.slots(),
    )


def _check(key: str, expected: Any, observations: Mapping[str, Any]) -> dict[str, Any]:
    if key not in observations:
        return {"key": key, "expected": expected, "observed": None, "status": "missing"}
    observed = observations[key]
    status = "pass" if observed == expected and type(observed) is type(expected) else "fail"
    return {"key": key, "expected": expected, "observed": observed, "status": status}


def _cleanup_checks(criterion: str, record: RunRecord) -> list[dict[str, Any]]:
    checks = []
    for kind in CLEANUP_KINDS[criterion]:
        rows = [row for row in record.cleanup if row.get("kind") == kind]
        clean = bool(rows) and all(row.get("status") in CLEAN_STATUSES for row in rows)
        checks.append(
            {
                "key": f"cleanup.{kind}",
                "expected": True,
                "observed": clean if rows else None,
                "status": "pass" if clean else ("missing" if not rows else "fail"),
            }
        )
    others = [row for row in record.cleanup if row.get("kind") != "step"]
    every = bool(others) and all(row.get("status") in CLEAN_STATUSES for row in others)
    checks.append(
        {
            "key": "cleanup.every_row_clean",
            "expected": True,
            "observed": every,
            "status": "pass" if every else "fail",
        }
    )
    steps = [row for row in record.cleanup if row.get("kind") == "step"]
    steps_ok = bool(steps) and all(row.get("status") == "ok" for row in steps)
    checks.append(
        {
            "key": "cleanup.finalizer_steps",
            "expected": True,
            "observed": steps_ok if steps else None,
            "status": "pass" if steps_ok else ("missing" if not steps else "fail"),
        }
    )
    return checks


def _slot_checks(record: RunRecord) -> list[dict[str, Any]]:
    """The ledger's slot table: at most three slots, each recorded before its label."""

    slots = list(record.slots)
    ordered = bool(slots) and all(
        row.get("labeled_at") is not None
        and row.get("recorded_at") is not None
        and row["recorded_at"] <= row["labeled_at"]
        for row in slots
    )
    within = len(slots) <= MAX_SLOTS and all(
        int(row.get("launches") or 0) <= LAUNCHES_PER_SLOT for row in slots
    )
    return [
        {
            "key": "ledger.slots_recorded_before_labels",
            "expected": True,
            "observed": ordered,
            "status": "pass" if ordered else "fail",
        },
        {
            "key": "ledger.slots_within_caps",
            "expected": True,
            "observed": within,
            "status": "pass" if within else "fail",
        },
    ]


def judge_one(records: Sequence[RunRecord], criterion: str) -> dict[str, Any]:
    mode = PROOF_MODE[criterion]
    record = next((item for item in records if item.mode == mode), None)
    if record is None:
        return {
            "criterion": criterion,
            "verdict": "missing",
            "reason": f"no {mode} run in the judged ledgers",
            "checks": [],
        }
    if record.outcome is None:
        return {
            "criterion": criterion,
            "verdict": "missing",
            "reason": f"the {mode} run never finished",
            "run_id": record.run_id,
            "checks": [],
        }
    observations = record.observations.get(criterion, {})
    checks = [_check(key, value, observations) for key, value in EXPECTED[criterion].items()]
    checks += _cleanup_checks(criterion, record)
    if criterion == "10":
        checks += _slot_checks(record)
    checks.append(
        {
            "key": "run.outcome",
            "expected": "completed",
            "observed": record.outcome,
            "status": "pass" if record.outcome == "completed" else "fail",
        }
    )
    verdict = "pass" if all(check["status"] == "pass" for check in checks) else "fail"
    return {
        "criterion": criterion,
        "verdict": verdict,
        "run_id": record.run_id,
        "candidate": record.candidate,
        "counts": {
            status: sum(1 for check in checks if check["status"] == status)
            for status in ("pass", "fail", "missing")
        },
        "checks": checks,
    }


def judge(records: Sequence[RunRecord], criterion: str) -> dict[str, Any]:
    if criterion not in (*CRITERIA, "all"):
        raise Refusal("criterion_invalid", "--criterion")
    wanted = CRITERIA if criterion == "all" else (criterion,)
    results = [judge_one(records, name) for name in wanted]
    verdicts = {result["verdict"] for result in results}
    overall = "pass" if verdicts == {"pass"} else ("fail" if "fail" in verdicts else "missing")
    return {"criterion": criterion, "verdict": overall, "criteria": results}


def exit_code(verdict: str) -> int:
    return poe.EXIT_PASS if verdict == "pass" else poe.EXIT_FAIL


__all__ = [
    "Conflict",
    "Document",
    "RequesterLedger",
    "ReviewerScan",
    "RunRecord",
    "SlotEvidence",
    "judge",
    "launch_permitted",
    "make_document",
    "preflight_plan",
    "slot_observations",
]
