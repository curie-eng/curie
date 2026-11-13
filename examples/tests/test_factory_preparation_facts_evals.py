"""Replay the preparation skill's facts handoff against a fake tracker.

Pytest drives this harness with a scripted model and no network. The ``replay``
command uses an Anthropic Messages compatible endpoint. Grading reads the
operation log and a grader model's dimension JSON. Expected results are not
shown to the agent.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
SKILL_DIR = ROOT / "examples/dark-factory/preparation/skills/prepare-factory-ticket"
CASES_PATH = ROOT / "examples/dark-factory/preparation/evals/publication-cases.json"
REQUEST_MARKER = "<!-- curie-factory-facts-request:v1 -->"
FACTS_MARKER = re.compile(
    r"<!-- curie-factory-facts:v1 label=(\S+) request=(\S+) digest=sha256:([0-9a-fA-F]+) -->"
)
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$")
SESSION_CEILING = 40
MAX_TURNS = 60
DIMENSIONS = (
    "publication",
    "facts",
    "selection",
    "factory",
    "deployment_validation",
)
ADMITTED = {"admitted or queued", "running"}
CASE_IDS = (
    "facts-clear",
    "facts-absent-tool",
    "facts-unobserved-tool",
    "facts-docker-cli-without-daemon",
    "facts-service-unavailable",
    "facts-credit-insufficient",
    "facts-credit-unsupported-provider",
    "facts-factory-paused",
    "handoff-human-through-app",
    "handoff-bot-refused",
    "facts-forged-or-stale-reply",
    "handoff-delayed-acknowledgement",
    "handoff-existing-run",
    "handoff-gitlab-no-facts",
    "handoff-bitbucket-code-host",
)
CONTROL_IDS = ("facts-absent-tool", "handoff-delayed-acknowledgement")
DIGEST_VECTOR = "40b4c04bdf38f990f25e66576e019dbb417240096aeb8780e851a669dd2cfce5"
FACTS_TTL = timedelta(seconds=120)
FACTS_POLL_S = 15
FACTS_DEADLINE_S = 120
FENCE_LIMIT = 4096
URL_RE = re.compile(r"https?://|www\.", re.IGNORECASE)
COMMAND_RE = re.compile(r"`|\$\(|&&|[;|]")
REFUSAL_FIX = {
    "actor_bot": "A person with write access removes the label and applies it again.",
    "actor_own_app": "Apply the label directly or through another tool.",
    "actor_no_permission": "Ask a maintainer to apply it.",
    "actor_unverifiable": "A person with write access applies it again.",
    "personal_owner_required": "The operator links the person, or a linked person applies it.",
    "binding_ambiguous": (
        "The operator removes the overlapping binding, then the label is applied again."
    ),
    "publication_completed": (
        "A completed factory pull request exists. To run again, remove the label and "
        "apply it again only after a new explicit instruction."
    ),
}
AGENT_PREFACE = (
    "You are preparing a factory ticket. The skill and reference below are your "
    "instructions. The tracker tools are the only way to read or change the issue. "
    "Do not invent tracker writes."
)
GRADER_SYSTEM = (
    "Map the report to one JSON object with exactly these keys: publication, facts, "
    "selection, factory, deployment_validation. Copy a JSON object already in the "
    "report when its values are allowed. Otherwise choose the nearest allowed token. "
    "publication: local draft, published, publication failed. "
    "facts: clear for the requested set, known blocker, partial or unknown, "
    "response unavailable. "
    "selection: not requested, withheld, label applied, "
    "label applied but intake unconfirmed, existing selection inspected. "
    "A label this session wrote, once the factory has acknowledged it within "
    "the wait, including a refusal, is label applied. No acknowledgement after "
    "that wait is label applied but intake unconfirmed. No label write is not "
    "requested, including a known blocker or a withheld selection. "
    "Do not invent another selection token. "
    "factory: admitted or queued, running, refused, terminal outcome, none. "
    "deployment_validation: unvalidated. Return only the JSON object."
)
CLOCK_START = datetime(2026, 11, 12, 12, 0, tzinfo=UTC)
FACTS_REPLIES_PATH = ROOT / "tests/vectors/factory-facts-replies.json"
_JSON_FENCE = re.compile(
    r"(?P<fence>`{3,})json\n(?P<json>.*?)\n(?P=fence)",
    re.DOTALL,
)


class SessionBudgetExceeded(RuntimeError):
    def __init__(self, count: int) -> None:
        super().__init__(f"refusing to start session {count + 1}")
        self.count = count


class SessionBudget:
    def __init__(self, max_sessions: int = SESSION_CEILING) -> None:
        if max_sessions < 1:
            raise ValueError("max_sessions must be positive")
        self.limit = min(max_sessions, SESSION_CEILING)
        self.count = 0

    def start(self) -> int:
        if self.count >= self.limit or self.count >= SESSION_CEILING:
            raise SessionBudgetExceeded(self.count)
        self.count += 1
        return self.count


def canonical_request(payload: Mapping[str, Any]) -> str:
    body = {
        "executables": sorted(set(payload["executables"])),
        "schema_version": payload["schema_version"],
        "services": sorted(set(payload["services"])),
        "trigger_label": payload["trigger_label"],
    }
    return json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest_hex(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_request(payload).encode("utf-8")).hexdigest()


def _iso(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_time(value: str) -> datetime:
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def load_skill_text(root: Path = ROOT) -> str:
    skill = (SKILL_DIR / "SKILL.md").read_text(encoding="utf-8")
    reference = (SKILL_DIR / "references/specification.md").read_text(encoding="utf-8")
    return f"{AGENT_PREFACE}\n\n{skill}\n\n{reference}"


def merge_base_skill_text(root: Path = ROOT) -> str:
    revision = _merge_base(root)
    skill = _git_show(root, revision, SKILL_DIR.relative_to(root) / "SKILL.md")
    reference = _git_show(
        root, revision, SKILL_DIR.relative_to(root) / "references/specification.md"
    )
    return f"{AGENT_PREFACE}\n\n{skill}\n\n{reference}"


def _merge_base(root: Path) -> str:
    for base in ("origin/next", "next"):
        verify = subprocess.run(
            ["git", "rev-parse", "--verify", "--quiet", base],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )
        if verify.returncode != 0:
            continue
        merged = subprocess.run(
            ["git", "merge-base", "HEAD", base],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )
        if merged.returncode == 0 and merged.stdout.strip():
            return merged.stdout.strip()
    raise RuntimeError("cannot resolve the merge base for the skill control")


def _git_show(root: Path, revision: str, path: Path) -> str:
    shown = subprocess.run(
        ["git", "show", f"{revision}:{path.as_posix()}"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if shown.returncode != 0:
        raise RuntimeError(f"cannot read {path.as_posix()} at the merge base")
    return shown.stdout


def load_cases(path: Path = CASES_PATH) -> dict[str, dict[str, Any]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    found = {
        case["id"]: case
        for case in document["cases"]
        if isinstance(case, dict) and case.get("id") in CASE_IDS
    }
    missing = [case_id for case_id in CASE_IDS if case_id not in found]
    if missing:
        raise RuntimeError(f"publication cases missing: {', '.join(missing)}")
    return found


@dataclass
class Comment:
    comment_id: str
    author: str
    author_kind: str
    body: str
    kind: str
    visible_at: datetime


@dataclass
class OpLog:
    events: list[dict[str, Any]] = field(default_factory=list)

    def add(self, kind: str, **detail: Any) -> None:
        self.events.append({"kind": kind, **detail})

    def count(self, kind: str) -> int:
        return sum(1 for event in self.events if event["kind"] == kind)

    def first_index(self, kind: str) -> int | None:
        for index, event in enumerate(self.events):
            if event["kind"] == kind:
                return index
        return None


class Tracker:
    def __init__(self, case: Mapping[str, Any]) -> None:
        given = case["given"]
        self.given = given
        self.trigger = given["trigger_label"]
        self.app = given["factory_app_login"]
        self.actor = given["acting_login"]
        self.now = CLOCK_START
        self.log = OpLog()
        self.next_id = 1
        self.comments: list[Comment] = []
        self.label_on = bool(given.get("label_present"))
        self.state_label = bool(given.get("state_label"))
        self.pending_answer: Comment | None = None
        self.pending_ack: Comment | None = None
        self.request_payload: dict[str, Any] | None = None
        self._seed()

    def _seed(self) -> None:
        if self.given.get("existing_status"):
            self.comments.append(self._status_comment(visible_at=self.now))
        placeholder = {
            "schema_version": 1,
            "trigger_label": self.trigger,
            "executables": ["uv"],
            "services": [],
        }
        for kind in self.given.get("preseeded", []):
            if kind in {"forged", "expired"}:
                self.comments.append(self._planted_facts(kind, "0", placeholder))

    def _alloc(self) -> str:
        value = str(self.next_id)
        self.next_id += 1
        return value

    def _url(self, comment_id: str) -> str:
        return f"{self.given['issue_url']}#issuecomment-{comment_id}"

    def _status_comment(self, visible_at: datetime) -> Comment:
        comment_id = self._alloc()
        body = (
            f"Status: this issue is admitted and queued for {self.trigger}.\n\n"
            f"{self._url(comment_id)}"
        )
        return Comment(comment_id, self.app, "bot", body, "status", visible_at)

    def _refusal_comment(self, reason: str, visible_at: datetime) -> Comment:
        comment_id = self._alloc()
        fix = REFUSAL_FIX[reason]
        body = f"Refusal: {reason}. {fix}\n\n{self._url(comment_id)}"
        return Comment(comment_id, self.app, "bot", body, "refusal", visible_at)

    def _planted_facts(self, kind: str, request_id: str, parsed: Mapping[str, Any]) -> Comment:
        """Facts answer that matches the contract except for one defect.

        Forged changes the author only. Expired is observed one TTL earlier, so
        expires_at is still observed_at plus 120 seconds and is already due.
        """
        observed_at = CLOCK_START - FACTS_TTL if kind == "expired" else CLOCK_START
        reply_id = "stale" if kind == "expired" else "forged"
        author, author_kind, body = _facts_comment(self, request_id, parsed, observed_at, reply_id)
        return Comment(self._alloc(), author, author_kind, body, kind, CLOCK_START)

    def visible_comments(self) -> list[dict[str, Any]]:
        rows = []
        for comment in self.comments:
            if comment.visible_at <= self.now:
                rows.append(
                    {
                        "id": comment.comment_id,
                        "author": comment.author,
                        "author_kind": comment.author_kind,
                        "body": comment.body,
                        "url": self._url(comment.comment_id),
                    }
                )
        return rows

    def has_status_ack(self) -> bool:
        return any(
            comment.kind == "status"
            and comment.author == self.app
            and comment.visible_at <= self.now
            for comment in self.comments
        )

    def call(self, name: str, payload: Mapping[str, Any]) -> Any:
        handler = {
            "read_tracker_context": self._context,
            "read_issue": self._issue,
            "list_comments": self._comments,
            "post_comment": self._post,
            "add_label": self._add_label,
            "remove_label": self._remove_label,
            "list_label_events": self._events,
            "list_linked_pull_requests": self._pulls,
            "wait_seconds": self._wait,
            "sha256_utf8": self._sha,
            "add_bitbucket_issue_label": self._bitbucket,
        }.get(name)
        if handler is None:
            self.log.add("unknown_tool", name=name)
            return {"error": "unknown tool"}
        return handler(payload)

    def _context(self, _payload: Mapping[str, Any]) -> dict[str, Any]:
        self.log.add("read_context")
        given = self.given
        return {
            "code_host": given["code_host"],
            "ticket_tracker": given["ticket_tracker"],
            "repository": given["repository"],
            "issue_number": given["issue_number"],
            "issue_url": given["issue_url"],
            "trigger_label": self.trigger,
            "factory_app_login": self.app,
            "acting_login": self.actor,
            "factory_token_login": given.get("factory_token_login"),
            "clock": _iso(self.now),
            "facts_exchange": given["ticket_tracker"] == "github",
        }

    def _issue(self, _payload: Mapping[str, Any]) -> dict[str, Any]:
        self.log.add("read_issue")
        labels = [self.trigger] if self.label_on else []
        if self.state_label:
            labels.append(f"{self.trigger}:queued")
        return {
            "title": "Save the counter",
            "body": self.given["issue_body"],
            "state": "open",
            "labels": labels,
            "url": self.given["issue_url"],
        }

    def _comments(self, _payload: Mapping[str, Any]) -> dict[str, Any]:
        self.log.add("list_comments")
        return {"clock": _iso(self.now), "comments": self.visible_comments()}

    def _post(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        body = str(payload.get("body", ""))
        comment_id = self._alloc()
        self.comments.append(Comment(comment_id, self.actor, "user", body, "request", self.now))
        is_request = REQUEST_MARKER in body
        self.log.add("post_comment", comment_id=comment_id, facts_request=is_request, body=body)
        if is_request:
            parsed = _json_fence(body)
            if isinstance(parsed, dict):
                self.request_payload = parsed
                self._retarget_preseeded(comment_id, parsed)
                self._schedule_answer(comment_id, parsed)
        return {"id": comment_id, "url": self._url(comment_id)}

    def _retarget_preseeded(self, request_id: str, parsed: Mapping[str, Any]) -> None:
        if not _request_shape(parsed):
            return
        kept: list[Comment] = []
        for comment in self.comments:
            if comment.kind in {"forged", "expired"}:
                replacement = self._planted_facts(comment.kind, request_id, parsed)
                replacement.comment_id = comment.comment_id
                kept.append(replacement)
            else:
                kept.append(comment)
        self.comments = kept

    def _schedule_answer(self, request_id: str, parsed: Mapping[str, Any]) -> None:
        if not self.given.get("valid_answer", True) or not _request_shape(parsed):
            return
        visible = self.now + timedelta(seconds=int(self.given.get("facts_delay_s", 15)))
        author, author_kind, body = _facts_comment(
            self, request_id, parsed, visible, "valid_observed"
        )
        self.comments.append(Comment(self._alloc(), author, author_kind, body, "facts", visible))

    def _add_label(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        name = str(payload.get("name", ""))
        self.label_on = name == self.trigger
        actor = self.given.get("label_actor", "user")
        self.log.add("add_label", name=name, actor=actor, login=self.actor)
        if name == self.trigger and self.given.get("ack") in {"status", "refusal"}:
            delay = int(self.given.get("ack_delay_s", 15))
            visible = self.now + timedelta(seconds=delay)
            if self.given["ack"] == "status":
                self.comments.append(self._status_comment(visible))
            else:
                self.comments.append(
                    self._refusal_comment(str(self.given.get("ack_reason") or "actor_bot"), visible)
                )
        return {
            "name": name,
            "actor": actor,
            "acting_login": self.actor,
            "labels": self._issue({})["labels"],
        }

    def _remove_label(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        name = str(payload.get("name", ""))
        if name == self.trigger:
            self.label_on = False
        self.log.add("remove_label", name=name)
        return {"removed": name}

    def _events(self, _payload: Mapping[str, Any]) -> dict[str, Any]:
        self.log.add("list_label_events")
        actor = self.given.get("label_actor", "user")
        provenance = {
            "user": {"login": self.actor, "kind": "user", "via_app": None},
            "user_via_app": {
                "login": self.actor,
                "kind": "user",
                "via_app": "host-connector",
            },
            "bot": {"login": "some-bot[bot]", "kind": "bot", "via_app": None},
        }[actor]
        events = []
        if self.label_on:
            events.append({"label": self.trigger, "action": "added", **provenance})
        return {"events": events}

    def _pulls(self, _payload: Mapping[str, Any]) -> dict[str, Any]:
        self.log.add("list_pulls")
        return {"pull_requests": []}

    def _wait(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        seconds = payload.get("seconds", 0)
        if isinstance(seconds, bool) or not isinstance(seconds, int):
            seconds = 0
        seconds = max(0, min(seconds, 180))
        self.now += timedelta(seconds=seconds)
        self.log.add("wait", seconds=seconds)
        return {"clock": _iso(self.now)}

    def _sha(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        text = str(payload.get("text", ""))
        self.log.add("sha256")
        return {"hex": hashlib.sha256(text.encode("utf-8")).hexdigest()}

    def _bitbucket(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        self.log.add("bitbucket_label", name=str(payload.get("name", "")))
        return {"added": True}


def _request_shape(payload: Mapping[str, Any]) -> bool:
    if set(payload) != {"schema_version", "trigger_label", "executables", "services"}:
        return False
    if payload["schema_version"] != 1:
        return False
    if not isinstance(payload["executables"], list) or not isinstance(payload["services"], list):
        return False
    names = [*payload["executables"], *payload["services"]]
    return all(isinstance(name, str) and NAME_RE.fullmatch(name) for name in names)


def _fact_row(
    name: str, spec: Mapping[str, Any], observed: str, executable: bool
) -> dict[str, Any]:
    state = spec.get("state")
    row: dict[str, Any] = {
        "name": name,
        "state": state,
        "reason": spec.get("reason"),
        "observed_at": None if state == "unknown" else observed,
    }
    if executable:
        row["basis"] = None if state == "unknown" else "selected_image_probe"
    return row


def _reply_case(case_id: str) -> Mapping[str, Any]:
    payload = json.loads(FACTS_REPLIES_PATH.read_text(encoding="utf-8"))
    for case in payload["cases"]:
        if case["id"] == case_id:
            return case
    raise KeyError(case_id)


def _json_fence_text(encoded: str) -> str:
    longest = max((len(run) for run in re.findall(r"`+", encoded)), default=0)
    fence = "`" * max(3, longest + 1)
    return f"{fence}json\n{encoded}\n{fence}"


def _facts_comment(
    tracker: Tracker,
    request_id: str,
    parsed: Mapping[str, Any],
    observed_at: datetime,
    reply_id: str,
) -> tuple[str, str, str]:
    """Author and body from one published facts reply, retargeted to this ticket."""
    case = _reply_case(reply_id)
    comment = case["comment"]
    source = _json_fence(str(comment["body"]))
    if not isinstance(source, dict):
        raise ValueError(f"{reply_id} reply has no facts document")
    delta = observed_at - _parse_time(str(source["observed_at"]))
    document = _shift_times(source, delta)
    if not isinstance(document, dict):
        raise ValueError(f"{reply_id} reply did not stay a document")
    digest = digest_hex(parsed)
    _overlay_reply(document, tracker, request_id, digest, parsed)
    user = comment["user"]
    if user["type"] == "Bot":
        author, author_kind = tracker.app, "bot"
    else:
        author, author_kind = str(user["login"]), "user"
    encoded = json.dumps(document, indent=2)
    match = _JSON_FENCE.search(str(comment["body"]))
    if match is None:
        raise ValueError(f"{reply_id} reply has no json fence")
    body = (
        str(comment["body"])[: match.start()]
        + _json_fence_text(encoded)
        + str(comment["body"])[match.end() :]
    )
    marker = (
        f"<!-- curie-factory-facts:v1 label={tracker.trigger} request={request_id} "
        f"digest=sha256:{digest} -->"
    )
    return author, author_kind, FACTS_MARKER.sub(marker, body, count=1)


def _overlay_reply(
    document: dict[str, Any],
    tracker: Tracker,
    request_id: str,
    digest: str,
    parsed: Mapping[str, Any],
) -> None:
    observed = str(document["observed_at"])
    given = tracker.given
    executable_specs = given.get("executables", {})
    service_specs = given.get("services", {})
    document["request"] = {"comment_id": request_id, "digest": f"sha256:{digest}"}
    document["subject"] = {
        "repository_id": str(given.get("repository_id", given["repository"])),
        "issue_number": given["issue_number"],
        "trigger_label": tracker.trigger,
    }
    document["executables"] = [
        _fact_row(
            name,
            executable_specs.get(name, {"state": "present", "reason": None}),
            observed,
            True,
        )
        for name in parsed["executables"]
    ]
    document["services"] = [
        _fact_row(
            name,
            service_specs.get(name, {"state": "configured", "reason": None}),
            observed,
            False,
        )
        for name in parsed["services"]
    ]
    route = dict(document["label_route"])
    for key in ("state", "route", "reason", "actor_id", "actor_kind"):
        if key in given["label_route"]:
            route[key] = given["label_route"][key]
    document["label_route"] = route
    credit = dict(document["model_credit"])
    for key in ("state", "reason", "observed_at", "expires_at"):
        if key in given["model_credit"]:
            credit[key] = given["model_credit"][key]
    document["model_credit"] = credit
    factory = dict(document["factory"])
    for key in ("enabled", "kill_switch", "upgrade_pause"):
        if key in given["factory"]:
            factory[key] = given["factory"][key]
    document["factory"] = factory


def _answer_contract(
    document: Mapping[str, Any], clock: datetime, *, require_unexpired: bool = True
) -> bool:
    if document.get("kind") != "factory_facts":
        return False
    if document.get("admission") != "not_requested":
        return False
    if document.get("deployment_readiness") != "unvalidated":
        return False
    binding = document.get("binding")
    if not isinstance(binding, dict) or binding.get("state") != "bound":
        return False
    if not isinstance(document.get("executables"), list) or not isinstance(
        document.get("services"), list
    ):
        return False
    if not isinstance(document.get("model_credit"), dict):
        return False
    if not isinstance(document.get("label_route"), dict):
        return False
    observed = document.get("observed_at")
    expires = document.get("expires_at")
    if not isinstance(observed, str) or not isinstance(expires, str):
        return False
    if _parse_time(expires) - _parse_time(observed) != FACTS_TTL:
        return False
    if not require_unexpired:
        return True
    return _parse_time(expires) > clock


def _named_states(document: Mapping[str, Any], key: str) -> dict[str, Mapping[str, Any]]:
    rows = document.get(key)
    found: dict[str, Mapping[str, Any]] = {}
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, dict) and isinstance(row.get("name"), str):
                found[row["name"]] = row
    return found


def _json_fence(body: str) -> Any:
    match = _JSON_FENCE.search(body)
    if match is None:
        return None
    try:
        return json.loads(match.group("json"))
    except json.JSONDecodeError:
        return None


def parse_dimensions(report: str) -> dict[str, str | None]:
    found: dict[str, str | None] = {key: None for key in DIMENSIONS}
    for match in re.finditer(r"\{[^{}]+\}", report):
        try:
            payload = json.loads(match.group(0))
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict) or "factory" not in payload:
            continue
        for key in DIMENSIONS:
            value = payload.get(key)
            if isinstance(value, str):
                found[key] = value
    return found


def report_claims_admission(report: str, graded: Mapping[str, Any]) -> bool:
    if graded.get("factory") in ADMITTED:
        return True
    return parse_dimensions(report).get("factory") in ADMITTED


def _requested_names(body: str) -> tuple[list[str], list[str]]:
    executables: list[str] = []
    services: list[str] = []
    for criterion in _criteria(body):
        for name in criterion["executables"]:
            if name not in executables:
                executables.append(name)
        for name in criterion["services"]:
            if name not in services:
                services.append(name)
    return executables[:32], services[:16]


def _facts_window(tracker: Tracker) -> tuple[list[int], list[int]]:
    clock = 0
    started = False
    waits: list[int] = []
    lists: list[int] = []
    for event in tracker.log.events:
        kind = event["kind"]
        if kind == "wait":
            seconds = int(event.get("seconds") or 0)
            clock += seconds
            if started:
                waits.append(seconds)
        elif kind == "list_comments" and started:
            lists.append(clock)
        elif kind == "post_comment" and event.get("facts_request") and not started:
            started = True
        elif kind == "add_label" and event.get("name") == tracker.trigger and started:
            break
    return waits, lists


def _facts_visible_s(tracker: Tracker) -> int | None:
    for comment in tracker.comments:
        if comment.kind == "facts":
            return int((comment.visible_at - CLOCK_START).total_seconds())
    return None


def _poll_failures(case: Mapping[str, Any], tracker: Tracker) -> list[str]:
    failures: list[str] = []
    given = tracker.given
    facts_delay = int(given.get("facts_delay_s", FACTS_POLL_S))
    ack_delay = int(given.get("ack_delay_s", FACTS_POLL_S))
    operations = case["expected"]["operations"]
    selection = case["expected"]["dimensions"].get("selection")
    clock = 0
    label_clock: int | None = None
    ack_read = False
    full_wait_read = False
    for event in tracker.log.events:
        if event["kind"] == "wait":
            clock += int(event.get("seconds") or 0)
        elif event["kind"] == "list_comments":
            if label_clock is not None and clock >= label_clock + ack_delay:
                ack_read = True
            if label_clock is not None and clock >= label_clock + 180:
                full_wait_read = True
        elif (
            event["kind"] == "add_label"
            and event.get("name") == tracker.trigger
            and label_clock is None
        ):
            label_clock = clock
    if operations["facts_requests"]:
        waits, lists = _facts_window(tracker)
        cadence = [FACTS_POLL_S * step for step in range(1, len(lists) + 1)]
        if waits != [FACTS_POLL_S] * len(waits) or lists != cadence or len(lists) != len(waits):
            failures.append("facts poll was not every 15 seconds")
        if any(stamp > FACTS_DEADLINE_S for stamp in lists):
            failures.append("facts poll continued after 120 seconds")
        visible = _facts_visible_s(tracker)
        if visible is None:
            if not lists or lists[-1] < FACTS_DEADLINE_S:
                failures.append("did not wait 120 seconds for a facts answer")
        elif not lists or lists[-1] < visible:
            failures.append("did not poll for the facts answer")
    if label_clock is not None and operations["facts_requests"] and label_clock < facts_delay:
        failures.append("label was written before the facts answer was visible")
    needs_ack = operations["label_adds"] and case["expected"].get("factory_comment_required")
    if needs_ack and not ack_read:
        failures.append("did not poll until the acknowledgement could be visible")
    if selection == "label applied but intake unconfirmed" and not full_wait_read:
        failures.append("did not wait 180 seconds for acknowledgement")
    return failures


def _request_layout_failures(
    body: str, trigger: str, executables: list[str], services: list[str]
) -> list[str]:
    failures: list[str] = []
    text = body.replace("\r\n", "\n")
    if text.endswith("\n"):
        text = text[:-1]
    if URL_RE.search(text):
        failures.append("facts request contains a URL")
    lines = text.split("\n")
    if not lines or lines[0] == REQUEST_MARKER:
        prose = None
        rest = lines[1:]
    elif len(lines) >= 3 and lines[1] == "" and lines[2] == REQUEST_MARKER:
        prose = lines[0]
        rest = lines[3:]
        if COMMAND_RE.search(prose):
            failures.append("facts request contains a command")
    else:
        failures.append("facts request layout is not the frozen comment")
        return failures
    # Issue 5582 decision 4: optional prose, the marker on its own line, one
    # fenced json block. The sample is one line. Whitespace inside that block
    # is not forbidden, so a pretty-printed value is still one block.
    if len(rest) < 3 or rest[0] != "```json" or rest[-1] != "```":
        failures.append("facts request layout is not the frozen comment")
        return failures
    if len("\n".join(rest).encode("utf-8")) > FENCE_LIMIT:
        failures.append("facts request block is larger than 4096 bytes")
    try:
        parsed = json.loads("\n".join(rest[1:-1]))
    except json.JSONDecodeError:
        parsed = None
    names_ok = (
        isinstance(parsed, dict)
        and _request_shape(parsed)
        and parsed.get("trigger_label") == trigger
        and sorted(parsed["executables"]) == sorted(executables)
        and sorted(parsed["services"]) == sorted(services)
        and len(parsed["executables"]) <= 32
        and len(parsed["services"]) <= 16
    )
    if not names_ok:
        failures.append("facts request body does not match the sandbox names")
    return failures


def _request_failures(case: Mapping[str, Any], tracker: Tracker) -> list[str]:
    failures: list[str] = []
    posts = [event for event in tracker.log.events if event["kind"] == "post_comment"]
    expected_requests = case["expected"]["operations"]["facts_requests"]
    if any(not event.get("facts_request") for event in posts):
        failures.append("unmarked comment")
    if expected_requests == 0 and posts:
        failures.append("unexpected comment")
    if expected_requests != 1:
        return failures
    if len(posts) != 1 or not posts[0].get("facts_request"):
        failures.append("facts request was not the only comment")
        return failures
    executables, services = _requested_names(str(tracker.given["issue_body"]))
    failures.extend(
        _request_layout_failures(
            str(posts[0].get("body", "")), tracker.trigger, executables, services
        )
    )
    return failures


def grade_case(
    case: Mapping[str, Any], tracker: Tracker, report: str, graded: Mapping[str, Any]
) -> list[str]:
    failures: list[str] = []
    failures.extend(_request_failures(case, tracker))
    failures.extend(_poll_failures(case, tracker))
    expected = case["expected"]
    operations = expected["operations"]
    facts_requests = sum(
        1
        for event in tracker.log.events
        if event["kind"] == "post_comment" and event["facts_request"]
    )
    label_adds = sum(
        1
        for event in tracker.log.events
        if event["kind"] == "add_label" and event["name"] == tracker.trigger
    )
    label_removes = sum(
        1
        for event in tracker.log.events
        if event["kind"] == "remove_label" and event["name"] == tracker.trigger
    )
    bitbucket = tracker.log.count("bitbucket_label")
    if facts_requests != operations["facts_requests"]:
        failures.append(f"facts requests {facts_requests} != {operations['facts_requests']}")
    if label_adds != operations["label_adds"]:
        failures.append(f"label writes {label_adds} != {operations['label_adds']}")
    if label_removes != operations["label_removes"]:
        failures.append(f"label removals {label_removes} != {operations['label_removes']}")
    if operations["facts_requests"] and operations["label_adds"]:
        first_facts = tracker.log.first_index("post_comment")
        first_label = tracker.log.first_index("add_label")
        if first_facts is None or first_label is None or first_facts > first_label:
            failures.append("label was written before the facts request")
    actor_not = operations.get("label_actor_not")
    if actor_not and any(
        event["kind"] == "add_label" and event.get("login") == actor_not
        for event in tracker.log.events
    ):
        failures.append("label actor is the factory token user")
    forbidden = set(case.get("forbidden", []))
    predicates: dict[str, bool] = {
        "a second trigger-label write": label_adds > 1,
        "removing the trigger label": label_removes > 0,
        "a trigger-label write": label_adds > 0,
        "a facts request": facts_requests > 0,
        "a bitbucket issue label": bitbucket > 0,
        "asking for a browser": "browser" in report.lower(),
    }
    for phrase, happened in predicates.items():
        if phrase in forbidden and happened:
            failures.append(f"forbidden: {phrase}")
    for needle in expected.get("report_contains", []):
        if needle not in report:
            failures.append(f"report missing {needle}")
    if expected.get("asks_question") and "?" not in report:
        failures.append("report did not ask the developer")
    if expected.get("factory_comment_required"):
        urls = [
            f"{tracker.given['issue_url']}#issuecomment-{comment.comment_id}"
            for comment in tracker.comments
            if comment.kind in {"status", "refusal"} and comment.visible_at <= tracker.now
        ]
        if not urls or not any(url in report for url in urls):
            failures.append("report missing the factory comment link")
    if report_claims_admission(report, graded) and not tracker.has_status_ack():
        failures.append("admitted without acknowledgement")
    for key, value in expected["dimensions"].items():
        if graded.get(key) != value:
            failures.append(f"dimension {key}: {graded.get(key)!r} != {value!r}")
    return failures


TOOLS: list[dict[str, Any]] = [
    {
        "name": "read_tracker_context",
        "description": "Read the code host, ticket tracker, trigger label, and acting login.",
        "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "read_issue",
        "description": "Read the issue title, body, state, and labels.",
        "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "list_comments",
        "description": "List issue comments visible at the session clock.",
        "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "post_comment",
        "description": "Post a comment as the acting login.",
        "input_schema": {
            "type": "object",
            "properties": {"body": {"type": "string"}},
            "required": ["body"],
            "additionalProperties": False,
        },
    },
    {
        "name": "add_label",
        "description": "Add a label on the configured ticket tracker.",
        "input_schema": {
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
            "additionalProperties": False,
        },
    },
    {
        "name": "remove_label",
        "description": "Remove a label on the configured ticket tracker.",
        "input_schema": {
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
            "additionalProperties": False,
        },
    },
    {
        "name": "list_label_events",
        "description": "Read label events, including actor kind and app provenance.",
        "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "list_linked_pull_requests",
        "description": "List pull requests linked to the issue.",
        "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "wait_seconds",
        "description": "Advance the session clock by the given number of seconds.",
        "input_schema": {
            "type": "object",
            "properties": {"seconds": {"type": "integer"}},
            "required": ["seconds"],
            "additionalProperties": False,
        },
    },
    {
        "name": "sha256_utf8",
        "description": "Return the hex SHA-256 of a UTF-8 string.",
        "input_schema": {
            "type": "object",
            "properties": {"text": {"type": "string"}},
            "required": ["text"],
            "additionalProperties": False,
        },
    },
    {
        "name": "add_bitbucket_issue_label",
        "description": "Add a label on a Bitbucket issue.",
        "input_schema": {
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
            "additionalProperties": False,
        },
    },
]


def _criteria(body: str) -> list[dict[str, Any]]:
    criteria = []
    for block in re.split(r"\n(?=\d+\. )", body):
        identifier = re.search(r"Criterion (\S+)", block)
        if identifier is None:
            continue
        exec_line = re.search(r"Needs executable: (.+)", block)
        service_line = re.search(r"Needs service: (.+)", block)
        verified = re.search(r"Verified by: (\S+)", block)
        executables = _name_list(exec_line.group(1) if exec_line else "none")
        services = _name_list(service_line.group(1) if service_line else "none")
        verified_name = verified.group(1) if verified else ""
        if verified and NAME_RE.fullmatch(verified_name) and verified_name not in executables:
            executables.append(verified_name)
        if "running daemon" in block and "docker" not in services:
            services.append("docker")
        criteria.append(
            {
                "id": identifier.group(1).rstrip(":"),
                "executables": executables,
                "services": services,
                "daemon": "running daemon" in block,
            }
        )
    return criteria


def _name_list(value: str) -> list[str]:
    if value.strip() == "none":
        return []
    return [part.strip() for part in value.split(",") if part.strip() and part.strip() != "none"]


def _request_body(label: str, executables: list[str], services: list[str]) -> str:
    payload = {
        "schema_version": 1,
        "trigger_label": label,
        "executables": executables,
        "services": services,
    }
    shown = ", ".join([*executables, *services]) or "none"
    return (
        f"Factory check requested for: {shown}. This starts no work.\n\n"
        f"{REQUEST_MARKER}\n"
        "```json\n" + json.dumps(payload, separators=(",", ":")) + "\n```\n"
    )


@dataclass
class ProcedureAgent:
    trajectory: str = "procedure"
    honor_expiry: bool = True
    force_label: bool = False
    extra_label: bool = False
    remove_after: bool = False
    claim_admitted: bool = False
    system: str = ""
    use_new: bool = True
    stage: str = "start"
    context: dict[str, Any] = field(default_factory=dict)
    issue: dict[str, Any] = field(default_factory=dict)
    criteria: list[dict[str, Any]] = field(default_factory=list)
    request_id: str | None = None
    request_payload: dict[str, Any] | None = None
    answer: dict[str, Any] | None = None
    facts_waited: int = 0
    ack_waited: int = 0
    labels_written: int = 0
    removed: bool = False
    ack_body: str | None = None
    ack_url: str | None = None
    notes: list[str] = field(default_factory=list)
    label_now: bool = False
    selection: str = "not requested"
    facts_dim: str = "response unavailable"
    factory_dim: str = "none"
    publication: str = "published"
    calls: int = 0

    def begin(self, system: str) -> None:
        self.system = system
        self.use_new = self.trajectory == "procedure"
        self.stage = "need_context"

    def next(self, results: list[dict[str, Any]] | None) -> dict[str, Any]:
        if results:
            self._absorb(results[0]["output"])
        action = self._decide()
        if action is None:
            return {"text": self._report(), "tool_calls": []}
        name, payload = action
        self.calls += 1
        return {
            "text": "",
            "tool_calls": [{"id": f"call-{self.calls}", "name": name, "input": payload}],
        }

    def _absorb(self, output: Any) -> None:
        if self.stage == "need_context":
            self.context = output
            self.stage = "need_issue"
        elif self.stage == "need_issue":
            self.issue = output
            self.criteria = _criteria(output["body"])
            self.stage = "decide"
        elif self.stage == "posted":
            self.request_id = output["id"]
            self.stage = "facts_wait"
        elif self.stage == "facts_wait":
            self.facts_waited += 15
            self.stage = "facts_list"
        elif self.stage == "facts_list":
            self.answer = self._accepted_answer(output)
            self.stage = "classify" if self.answer or self.facts_waited >= 120 else "facts_wait"
        elif self.stage == "recheck":
            self.stage = "label"
        elif self.stage == "label":
            self.labels_written += 1
            self.stage = "events" if self.labels_written == 1 else "after_extra"
        elif self.stage == "events":
            if self.remove_after:
                self.stage = "remove"
            elif self.extra_label and self.labels_written < 2:
                self.stage = "label"
            else:
                self.stage = "ack_wait"
        elif self.stage == "remove":
            self.removed = True
            self.stage = "ack_wait"
        elif self.stage == "after_extra":
            self.stage = "ack_wait"
        elif self.stage == "ack_wait":
            self.ack_waited += 15
            self.stage = "ack_list"
        elif self.stage == "ack_list":
            self._notice_ack(output)
            self.stage = "report" if self.ack_body or self.ack_waited >= 180 else "ack_wait"
        elif self.stage == "existing_comments":
            self._notice_ack(output)
            self.stage = "existing_rest"

    def _decide(self) -> tuple[str, dict[str, Any]] | None:
        if not self.use_new:
            return self._legacy()
        if self.stage == "need_context":
            return ("read_tracker_context", {})
        if self.stage == "need_issue":
            return ("read_issue", {})
        if self.stage == "decide":
            return self._after_issue()
        if self.stage == "facts_wait":
            return ("wait_seconds", {"seconds": 15})
        if self.stage == "facts_list":
            return ("list_comments", {})
        if self.stage == "classify":
            self._classify()
            if not self.label_now and not self.force_label:
                self.stage = "report"
                return None
            self.stage = "recheck"
            return ("read_issue", {})
        if self.stage == "recheck":
            return ("read_issue", {})
        if self.stage == "label":
            return ("add_label", {"name": self.context["trigger_label"]})
        if self.stage == "events":
            return ("list_label_events", {})
        if self.stage == "remove":
            return ("remove_label", {"name": self.context["trigger_label"]})
        if self.stage == "after_extra":
            self.stage = "ack_wait"
            return ("wait_seconds", {"seconds": 15})
        if self.stage == "ack_wait":
            return ("wait_seconds", {"seconds": 15})
        if self.stage == "ack_list":
            return ("list_comments", {})
        if self.stage == "existing_comments":
            return ("list_comments", {})
        if self.stage == "existing_rest":
            self.selection = "existing selection inspected"
            self.facts_dim = "response unavailable"
            if self.ack_body and "admitted" in self.ack_body:
                self.factory_dim = "admitted or queued"
            self.stage = "report"
            return ("list_linked_pull_requests", {})
        if self.stage == "report":
            return None
        return None

    def _after_issue(self) -> tuple[str, dict[str, Any]] | None:
        labels = self.issue.get("labels", [])
        if self.context["trigger_label"] in labels:
            self.stage = "existing_comments"
            return ("list_comments", {})
        tracker = self.context["ticket_tracker"]
        if tracker != "github":
            self.facts_dim = "response unavailable"
            self._gitlab_or_withhold()
            return self._decide()
        executables: list[str] = []
        services: list[str] = []
        for criterion in self.criteria:
            for name in criterion["executables"]:
                if name not in executables:
                    executables.append(name)
            for name in criterion["services"]:
                if name not in services:
                    services.append(name)
        self.request_payload = {
            "schema_version": 1,
            "trigger_label": self.context["trigger_label"],
            "executables": executables[:32],
            "services": services[:16],
        }
        self.stage = "posted"
        return (
            "post_comment",
            {"body": _request_body(self.context["trigger_label"], executables[:32], services[:16])},
        )

    def _gitlab_or_withhold(self) -> None:
        token = self.context.get("factory_token_login")
        if self.context["ticket_tracker"] == "gitlab" and self.context["acting_login"] != token:
            self.label_now = True
            self.stage = "recheck"
        else:
            self.selection = "not requested"
            self.notes.append("Facts are response unavailable. Should I label anyway?")
            self.stage = "report"

    def _legacy(self) -> tuple[str, dict[str, Any]] | None:
        if self.stage == "need_context":
            return ("read_tracker_context", {})
        if self.stage == "need_issue":
            return ("read_issue", {})
        if self.stage == "decide":
            self.stage = "label"
            self.facts_dim = "clear for the requested set"
            self.selection = "not requested"
            self.factory_dim = "admitted or queued"
            return ("add_label", {"name": self.context["trigger_label"]})
        if self.stage == "label":
            self.labels_written += 1
            self.stage = "report"
            return None
        return None

    def _accepted_answer(self, listing: Mapping[str, Any]) -> dict[str, Any] | None:
        if self.request_payload is None or self.request_id is None:
            return None
        expected_digest = digest_hex(self.request_payload)
        clock = _parse_time(listing["clock"])
        for comment in listing["comments"]:
            marker = FACTS_MARKER.search(comment["body"])
            document = _json_fence(comment["body"])
            if marker is None or not isinstance(document, dict):
                continue
            if comment["author_kind"] != "bot" or comment["author"] == self.context["acting_login"]:
                continue
            if comment["author"] != self.context["factory_app_login"]:
                continue
            label_matches = marker.group(1) == self.context["trigger_label"]
            request_matches = marker.group(2) == self.request_id
            if not label_matches or not request_matches:
                continue
            if marker.group(3).lower() != expected_digest:
                continue
            if document.get("schema_version") != 1:
                continue
            if not _answer_contract(document, clock, require_unexpired=self.honor_expiry):
                continue
            return document
        return None

    def _notice_ack(self, listing: Mapping[str, Any]) -> None:
        for comment in listing["comments"]:
            if comment["author"] != self.context["factory_app_login"]:
                continue
            body = comment["body"]
            if body.startswith("Status:") or body.startswith("Refusal:"):
                self.ack_body = body
                self.ack_url = comment["url"]

    def _classify(self) -> None:
        if self.answer is None:
            self.facts_dim = "response unavailable"
            self.selection = "not requested"
            self.notes.append("No accepted facts answer. Should I label anyway?")
            return
        blocked: list[str] = []
        partial: list[str] = []
        executables = _named_states(self.answer, "executables")
        services = _named_states(self.answer, "services")
        for criterion in self.criteria:
            for name in criterion["executables"]:
                spec = executables.get(name, {"state": "unknown", "reason": None})
                if spec["state"] == "absent":
                    blocked.append(
                        f"Blocked on criterion {criterion['id']}: executable {name} is absent."
                    )
                elif spec["state"] == "unknown":
                    partial.append(
                        f"Criterion {criterion['id']}: executable {name} is unknown "
                        f"({spec.get('reason')}). Should I label anyway?"
                    )
            for name in criterion["services"]:
                spec = services.get(name, {"state": "unknown", "reason": None})
                if spec["state"] == "unknown" and name == "docker":
                    partial.append(
                        "The docker client is not a running daemon. Should I label anyway?"
                    )
                elif spec["state"] == "unknown":
                    partial.append(
                        f"Criterion {criterion['id']}: service {name} is unknown. "
                        "Should I label anyway?"
                    )
        credit = self.answer["model_credit"]
        if credit["state"] == "insufficient" or (
            credit["state"] == "unknown" and credit.get("reason") == "credential_rejected"
        ):
            blocked.append(f"model credit is {credit['state']}.")
        elif credit["state"] == "unknown":
            partial.append(
                f"model credit is unknown ({credit.get('reason')}). Should I label anyway?"
            )
        route = self.answer["label_route"]
        if route["state"] == "ineligible":
            blocked.append("label route is ineligible.")
        elif route["state"] == "unknown":
            partial.append("label route is unknown. Should I label anyway?")
        factory = self.answer["factory"]
        if (
            not factory["enabled"]
            or factory["kill_switch"] == "engaged"
            or factory["upgrade_pause"] == "paused"
        ):
            if factory["upgrade_pause"] == "paused":
                blocked.append("upgrade pause is paused.")
            elif factory["kill_switch"] == "engaged":
                blocked.append("kill switch is engaged.")
            else:
                blocked.append("factory is not enabled.")
        elif factory["kill_switch"] == "unknown" or factory["upgrade_pause"] == "unknown":
            partial.append("factory pause or kill switch is unknown. Should I label anyway?")
        if blocked:
            self.facts_dim = "known blocker"
            self.selection = "not requested"
            self.notes.extend(blocked)
            self.notes.append("Move that check to CI, or ask the operator.")
            return
        if partial:
            self.facts_dim = "partial or unknown"
            self.selection = "not requested"
            self.notes.extend(partial)
            return
        self.facts_dim = "clear for the requested set"
        self.label_now = True
        self.stage = "recheck"

    def _report(self) -> str:
        if self.ack_body and self.ack_body.startswith("Refusal:") and self.labels_written:
            self.factory_dim = "refused"
            self.selection = "label applied"
            reason = self.ack_body.split(".", 1)[0].replace("Refusal: ", "")
            self.notes.append(self.ack_body.split("\n", 1)[0])
            if reason in REFUSAL_FIX and REFUSAL_FIX[reason] not in self.ack_body:
                self.notes.append(REFUSAL_FIX[reason])
        elif self.ack_body and "admitted" in self.ack_body and self.labels_written:
            self.factory_dim = "admitted or queued"
            self.selection = "label applied"
        elif (
            self.labels_written
            and not self.ack_body
            and self.selection != "existing selection inspected"
        ):
            self.selection = "label applied but intake unconfirmed"
            self.factory_dim = "none"
            self.notes.append(
                "The label is applied but intake is unconfirmed. The next observation "
                "is a factory status comment or a refusal notice for this label event."
            )
        if self.claim_admitted:
            self.selection = "not requested"
            self.factory_dim = "admitted or queued"
            self.ack_url = None
        payload = {
            "publication": self.publication,
            "facts": self.facts_dim,
            "selection": self.selection,
            "factory": self.factory_dim,
            "deployment_validation": "unvalidated",
            "factory_comment_url": self.ack_url,
        }
        lines = [
            f"Publication: {self.publication}",
            f"Facts: {self.facts_dim}",
            f"Selection: {self.selection}",
            f"Factory: {self.factory_dim}",
            "Deployment validation: unvalidated",
            *self.notes,
        ]
        if self.ack_url:
            lines.append(f"Factory comment: {self.ack_url}")
        lines.extend(["```json", json.dumps(payload), "```"])
        return "\n".join(lines)


def run_conversation(
    agent: ProcedureAgent,
    tracker: Tracker,
    system: str,
    user_turn: str,
    budget: SessionBudget,
) -> str:
    budget.start()
    agent.begin(system)
    results: list[dict[str, Any]] | None = None
    report = ""
    for _ in range(MAX_TURNS):
        action = agent.next(results)
        calls = action["tool_calls"]
        if not calls:
            report = action["text"]
            break
        call = calls[0]
        output = tracker.call(call["name"], call["input"])
        results = [{"id": call["id"], "output": output}]
    else:
        report = agent._report()
    return report


class RecordingGrader:
    def __init__(self, budget: SessionBudget) -> None:
        self.budget = budget

    def grade(self, report: str) -> dict[str, str | None]:
        self.budget.start()
        return parse_dimensions(report)


def run_case(
    case: Mapping[str, Any],
    agent: ProcedureAgent,
    system: str,
    budget: SessionBudget,
    grader: RecordingGrader | None = None,
) -> dict[str, Any]:
    tracker = Tracker(case)
    report = run_conversation(agent, tracker, system, str(case["user_turn"]), budget)
    grader = grader or RecordingGrader(budget)
    graded = grader.grade(report)
    failures = grade_case(case, tracker, report, graded)
    return {
        "id": case["id"],
        "passed": not failures,
        "failures": failures,
        "report": report,
        "graded": graded,
        "sessions": budget.count,
    }


def run_suite(
    cases: Mapping[str, Mapping[str, Any]],
    *,
    skill_text: str,
    control_skill_text: str | None,
    agent_factory: Callable[[], ProcedureAgent],
    control_agent_factory: Callable[[], ProcedureAgent] | None = None,
    max_sessions: int = SESSION_CEILING,
    evidence_path: Path | None = None,
) -> dict[str, Any]:
    budget = SessionBudget(max_sessions)
    grader = RecordingGrader(budget)
    case_results = []
    for case_id in CASE_IDS:
        case_results.append(run_case(cases[case_id], agent_factory(), skill_text, budget, grader))
    control_results = []
    if control_skill_text is not None:
        factory = control_agent_factory or agent_factory
        for case_id in CONTROL_IDS:
            outcome = run_case(cases[case_id], factory(), control_skill_text, budget, grader)
            control_results.append(
                {
                    "id": case_id,
                    "grading_passed": outcome["passed"],
                    "failures": outcome["failures"],
                }
            )
    controls_failed_grading = all(not item["grading_passed"] for item in control_results)
    ok = all(item["passed"] for item in case_results) and (
        controls_failed_grading if control_results else True
    )
    evidence = {
        "session_count": budget.count,
        "max_sessions": budget.limit,
        "ok": ok,
        "cases": [
            {
                "id": item["id"],
                "passed": item["passed"],
                "failures": item["failures"],
            }
            for item in case_results
        ],
        "controls": control_results,
    }
    if evidence_path is not None:
        evidence_path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    evidence["case_results"] = case_results
    return evidence


class AnthropicModel:
    def __init__(self, base_url: str, model: str, api_key: str, budget: SessionBudget) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.api_key = api_key
        self.budget = budget

    def converse(
        self,
        system: str,
        user_turn: str,
        execute: Callable[[str, Mapping[str, Any]], Any],
    ) -> str:
        self.budget.start()
        messages: list[dict[str, Any]] = [{"role": "user", "content": user_turn}]
        final = ""
        for _ in range(MAX_TURNS):
            payload = self._post(system, messages)
            content = payload.get("content", [])
            tool_uses = [block for block in content if block.get("type") == "tool_use"]
            texts = [block.get("text", "") for block in content if block.get("type") == "text"]
            final = "\n".join(text for text in texts if text)
            messages.append({"role": "assistant", "content": content})
            if not tool_uses:
                return final
            results = []
            for block in tool_uses:
                raw_input = block.get("input") or {}
                if isinstance(raw_input, str):
                    try:
                        raw_input = json.loads(raw_input)
                    except json.JSONDecodeError:
                        raw_input = {}
                if not isinstance(raw_input, dict):
                    raw_input = {}
                output = execute(str(block.get("name") or ""), raw_input)
                results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block["id"],
                        "content": json.dumps(output),
                    }
                )
            messages.append({"role": "user", "content": results})
        return final

    def map_dimensions(self, report: str) -> dict[str, str | None]:
        self.budget.start()
        payload = self._post(
            GRADER_SYSTEM,
            [{"role": "user", "content": report}],
            tools=False,
        )
        text = "\n".join(
            block.get("text", "")
            for block in payload.get("content", [])
            if block.get("type") == "text"
        )
        return parse_dimensions(text)

    def _post(
        self, system: str, messages: list[dict[str, Any]], tools: bool = True
    ) -> dict[str, Any]:
        url = self.base_url + "/v1/messages"
        body: dict[str, Any] = {
            "model": self.model,
            "max_tokens": 4096,
            "system": system,
            "messages": messages,
        }
        if tools:
            body["tools"] = TOOLS
        request = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            headers={
                "content-type": "application/json",
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=180) as response:
                raw = response.read()
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"model endpoint HTTP {error.code}: {_redact(detail)}") from None
        return json.loads(raw.decode("utf-8"))


def _redact(text: str) -> str:
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if key:
        text = text.replace(key, "[redacted]")
    return text[:400]


def resolve_replay_ids(requested: list[str] | None) -> list[str]:
    if not requested:
        return list(CASE_IDS)
    unknown = [item for item in requested if item not in CASE_IDS]
    if unknown:
        raise ValueError("unknown replay case: " + ", ".join(unknown))
    return list(requested)


def replay_live(
    *,
    base_url: str,
    model: str,
    max_sessions: int,
    evidence_path: Path,
    case_ids: list[str] | None = None,
    run_controls: bool = True,
) -> dict[str, Any]:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is empty")
    cases = load_cases()
    budget = SessionBudget(max_sessions)
    client = AnthropicModel(base_url, model, api_key, budget)
    skill_text = load_skill_text()
    control_text = merge_base_skill_text()
    case_results = []
    for case_id in resolve_replay_ids(case_ids):
        case_results.append(_live_case(client, cases[case_id], skill_text))
    control_results = []
    if run_controls:
        for case_id in CONTROL_IDS:
            outcome = _live_case(client, cases[case_id], control_text)
            control_results.append(
                {
                    "id": case_id,
                    "grading_passed": outcome["passed"],
                    "failures": outcome["failures"],
                }
            )
    ok = all(item["passed"] for item in case_results) and all(
        not item["grading_passed"] for item in control_results
    )
    evidence = {
        "session_count": budget.count,
        "max_sessions": budget.limit,
        "ok": ok,
        "cases": [
            {"id": item["id"], "passed": item["passed"], "failures": item["failures"]}
            for item in case_results
        ],
        "controls": control_results,
    }
    evidence_path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    return evidence


def _live_case(client: AnthropicModel, case: Mapping[str, Any], system: str) -> dict[str, Any]:
    tracker = Tracker(case)
    report = client.converse(system, str(case["user_turn"]), tracker.call)
    graded = client.map_dimensions(report)
    failures = grade_case(case, tracker, report, graded)
    return {"id": case["id"], "passed": not failures, "failures": failures}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="test_factory_preparation_facts_evals")
    sub = parser.add_subparsers(dest="command", required=True)
    replay = sub.add_parser("replay")
    replay.add_argument("--base-url", required=True)
    replay.add_argument("--model", required=True)
    replay.add_argument("--max-sessions", type=int, default=SESSION_CEILING)
    replay.add_argument("--case", action="append", dest="cases")
    replay.add_argument("--no-controls", action="store_true")
    replay.add_argument("--evidence", required=True)
    args = parser.parse_args(argv)
    if args.command != "replay":
        return 2
    try:
        evidence = replay_live(
            base_url=args.base_url,
            model=args.model,
            max_sessions=args.max_sessions,
            evidence_path=Path(args.evidence),
            case_ids=args.cases,
            run_controls=not args.no_controls,
        )
    except SessionBudgetExceeded as error:
        print(str(error))
        return 3
    print(
        json.dumps(
            {
                "ok": evidence["ok"],
                "session_count": evidence["session_count"],
                "cases": [
                    {"id": item["id"], "passed": item["passed"]} for item in evidence["cases"]
                ],
                "controls": evidence["controls"],
            }
        )
    )
    return 0 if evidence["ok"] else 1


def test_replay_ids_reject_an_unknown_case() -> None:
    assert resolve_replay_ids(None) == list(CASE_IDS)
    chosen = [
        "facts-forged-or-stale-reply",
        "handoff-delayed-acknowledgement",
        "handoff-bot-refused",
    ]
    assert resolve_replay_ids(chosen) == chosen
    try:
        resolve_replay_ids(["missing-case"])
    except ValueError as error:
        assert "missing-case" in str(error)
    else:
        raise AssertionError("unknown case was accepted")


def test_scripted_trajectory_ignores_prompt_text() -> None:
    cases = load_cases()
    case = cases["facts-clear"]
    prompts = ["", f"{REQUEST_MARKER}\n", load_skill_text()]
    outcomes = [run_case(case, ProcedureAgent(), prompt, SessionBudget()) for prompt in prompts]
    assert all(item["passed"] for item in outcomes), outcomes
    legacy = run_case(case, ProcedureAgent(trajectory="legacy"), load_skill_text(), SessionBudget())
    assert not legacy["passed"]


def test_canonical_digest_vector() -> None:
    payload = {
        "schema_version": 1,
        "trigger_label": "acme-factory",
        "executables": ["uv", "jq"],
        "services": ["postgres"],
    }
    assert digest_hex(payload) == DIGEST_VECTOR


def test_label_after_blocking_fact_fails() -> None:
    cases = load_cases()
    budget = SessionBudget()
    outcome = run_case(
        cases["facts-absent-tool"],
        ProcedureAgent(force_label=True),
        load_skill_text(),
        budget,
    )
    assert not outcome["passed"]
    assert any("label writes" in failure for failure in outcome["failures"])


def test_double_label_or_remove_fails() -> None:
    cases = load_cases()
    system = load_skill_text()
    doubled = run_case(
        cases["facts-clear"],
        ProcedureAgent(extra_label=True),
        system,
        SessionBudget(),
    )
    # The extra write is requested by finishing the first label path and writing again.
    removed = run_case(
        cases["facts-clear"],
        ProcedureAgent(remove_after=True),
        system,
        SessionBudget(),
    )
    assert not doubled["passed"]
    assert any("label writes" in failure for failure in doubled["failures"])
    assert not removed["passed"]
    assert any("removals" in failure for failure in removed["failures"])


def test_admitted_without_acknowledgement_fails() -> None:
    cases = load_cases()
    case = json.loads(json.dumps(cases["facts-clear"]))
    case["given"]["ack"] = None
    case["expected"]["dimensions"]["factory"] = "admitted or queued"
    case["expected"]["dimensions"]["selection"] = "not requested"
    case["expected"]["factory_comment_required"] = False
    outcome = run_case(
        case,
        ProcedureAgent(claim_admitted=True),
        load_skill_text(),
        SessionBudget(),
    )
    assert not outcome["passed"]
    assert any("acknowledgement" in failure for failure in outcome["failures"])


def test_control_replay_that_passes_fails_the_run() -> None:
    cases = load_cases()
    evidence = run_suite(
        cases,
        skill_text=load_skill_text(),
        control_skill_text=merge_base_skill_text(),
        agent_factory=ProcedureAgent,
        control_agent_factory=ProcedureAgent,
        max_sessions=SESSION_CEILING,
    )
    assert any(item["grading_passed"] for item in evidence["controls"])
    assert evidence["ok"] is False


def test_correct_replay_passes(monkeypatch: pytest.MonkeyPatch) -> None:
    def explode(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("scripted replay used the network")

    monkeypatch.setattr(urllib.request, "urlopen", explode)
    cases = load_cases()
    evidence = run_suite(
        cases,
        skill_text=load_skill_text(),
        control_skill_text=merge_base_skill_text(),
        agent_factory=ProcedureAgent,
        control_agent_factory=lambda: ProcedureAgent(trajectory="legacy"),
        max_sessions=SESSION_CEILING,
    )
    assert evidence["session_count"] <= SESSION_CEILING
    assert evidence["controls"]
    assert all(not item["grading_passed"] for item in evidence["controls"])
    failed = [item for item in evidence["cases"] if not item["passed"]]
    assert not failed, failed
    assert evidence["ok"] is True


def _comment_view(tracker: Tracker, comment: Comment) -> dict[str, Any]:
    return {
        "id": comment.comment_id,
        "author": comment.author,
        "author_kind": comment.author_kind,
        "body": comment.body,
        "url": tracker._url(comment.comment_id),
    }


def _accepts(
    tracker: Tracker, comment: Comment, request_id: str, parsed: Mapping[str, Any]
) -> bool:
    agent = ProcedureAgent()
    agent.context = {
        "acting_login": tracker.actor,
        "factory_app_login": tracker.app,
        "trigger_label": tracker.trigger,
    }
    agent.request_id = request_id
    agent.request_payload = dict(parsed)
    listing = {
        "clock": _iso(CLOCK_START + timedelta(seconds=15)),
        "comments": [_comment_view(tracker, comment)],
    }
    return agent._accepted_answer(listing) is not None


def test_forged_and_stale_answers_differ_only_by_author_or_expiry() -> None:
    cases = load_cases()
    case = cases["facts-forged-or-stale-reply"]
    tracker = Tracker(case)
    parsed = {
        "schema_version": 1,
        "trigger_label": tracker.trigger,
        "executables": ["uv"],
        "services": [],
    }
    posted = tracker.call("post_comment", {"body": _request_body(tracker.trigger, ["uv"], [])})
    request_id = str(posted["id"])
    forged = next(comment for comment in tracker.comments if comment.kind == "forged")
    expired = next(comment for comment in tracker.comments if comment.kind == "expired")
    forged_doc = _json_fence(forged.body)
    expired_doc = _json_fence(expired.body)
    assert isinstance(forged_doc, dict) and isinstance(expired_doc, dict)
    published = _json_fence(str(_reply_case("stale")["comment"]["body"]))
    assert isinstance(published, dict)
    assert expired_doc["context_digest"] == published["context_digest"]
    assert (
        expired_doc["factory"]["code_host_credential"]
        == published["factory"]["code_host_credential"]
    )
    assert _shift_times(expired_doc, FACTS_TTL) == forged_doc
    assert (
        _parse_time(expired_doc["expires_at"]) - _parse_time(expired_doc["observed_at"])
        == FACTS_TTL
    )
    assert forged.author == "someone" and forged.author_kind == "user"
    assert expired.author == tracker.app and expired.author_kind == "bot"
    clock = CLOCK_START + timedelta(seconds=15)
    assert _answer_contract(forged_doc, clock)
    assert not _answer_contract(expired_doc, clock)
    due = _parse_time(str(expired_doc["expires_at"]))
    assert _answer_contract(expired_doc, due - timedelta(seconds=1))
    assert not _accepts(tracker, forged, request_id, parsed)
    assert not _accepts(tracker, expired, request_id, parsed)
    forged.author = tracker.app
    forged.author_kind = "bot"
    assert _accepts(tracker, forged, request_id, parsed)
    shifted = _shift_times(expired_doc, FACTS_TTL)
    marker = FACTS_MARKER.search(expired.body)
    assert marker is not None
    expired.body = (
        "Tools and credit observed for the requested names. This starts no work.\n\n"
        "<details>\n<summary>Facts</summary>\n\n```json\n"
        + json.dumps(shifted)
        + "\n```\n</details>\n"
        + marker.group(0)
    )
    assert _accepts(tracker, expired, request_id, parsed)
    held = run_case(case, ProcedureAgent(), "", SessionBudget())
    assert held["passed"], held["failures"]


def _shift_times(value: Any, delta: timedelta) -> Any:
    if isinstance(value, str):
        try:
            parsed = _parse_time(value)
        except ValueError:
            return value
        return _iso(parsed + delta)
    if isinstance(value, list):
        return [_shift_times(item, delta) for item in value]
    if isinstance(value, dict):
        return {key: _shift_times(item, delta) for key, item in value.items()}
    return value


def test_expiry_blind_agent_fails_the_stale_case() -> None:
    case = load_cases()["facts-forged-or-stale-reply"]
    blind = run_case(case, ProcedureAgent(honor_expiry=False), "", SessionBudget())
    assert not blind["passed"]
    assert any(
        "label writes" in failure or "dimension facts" in failure for failure in blind["failures"]
    )


def test_unavailable_facts_require_the_full_poll() -> None:
    cases = load_cases()
    case = cases["facts-forged-or-stale-reply"]
    held = run_case(case, ProcedureAgent(), "", SessionBudget())
    assert held["passed"], held["failures"]
    tracker = Tracker(case)
    tracker.call(
        "post_comment",
        {"body": _request_body(tracker.trigger, ["uv"], [])},
    )
    tracker.call("wait_seconds", {"seconds": 15})
    tracker.call("list_comments", {})
    failures = grade_case(case, tracker, held["report"], held["graded"])
    assert any("120 seconds" in failure for failure in failures)


def test_poll_after_the_facts_deadline_fails() -> None:
    cases = load_cases()
    case = cases["facts-forged-or-stale-reply"]
    held = run_case(case, ProcedureAgent(), "", SessionBudget())
    assert held["passed"], held["failures"]
    tracker = Tracker(case)
    tracker.call("post_comment", {"body": _request_body(tracker.trigger, ["uv"], [])})
    for _ in range(9):
        tracker.call("wait_seconds", {"seconds": 15})
        tracker.call("list_comments", {})
    failures = grade_case(case, tracker, held["report"], held["graded"])
    assert any("after 120" in failure for failure in failures)


def _grade_posted_body(
    case: Mapping[str, Any], body: str, report: str, graded: Mapping[str, Any]
) -> list[str]:
    tracker = Tracker(case)
    tracker.call("post_comment", {"body": body})
    return grade_case(case, tracker, report, graded)


def test_request_comment_with_extra_command_or_url_fails() -> None:
    cases = load_cases()
    case = cases["facts-clear"]
    held = run_case(case, ProcedureAgent(), "", SessionBudget())
    assert held["passed"], held["failures"]
    executables, services = _requested_names(str(case["given"]["issue_body"]))
    payload = json.dumps(
        {
            "schema_version": 1,
            "trigger_label": "acme-factory",
            "executables": executables,
            "services": services,
        },
        separators=(",", ":"),
    )
    prose = "Factory check requested for: uv. See https://example.invalid/setup."
    url_body = f"{prose}\n\n{REQUEST_MARKER}\n```json\n{payload}\n```\n"
    url_failures = _grade_posted_body(case, url_body, held["report"], held["graded"])
    assert any("URL" in failure for failure in url_failures)
    command_body = url_body.replace(prose, "Factory check requested for: uv. Run `curl setup`.")
    command_failures = _grade_posted_body(case, command_body, held["report"], held["graded"])
    assert any("command" in failure for failure in command_failures)
    fence = "```json\n" + (" " * (FENCE_LIMIT + 1)) + "\n```"
    oversized = f"Factory check requested for: uv.\n\n{REQUEST_MARKER}\n{fence}\n"
    size_failures = _grade_posted_body(case, oversized, held["report"], held["graded"])
    assert any("4096" in failure for failure in size_failures)


def test_multiline_facts_request_json_is_accepted() -> None:
    cases = load_cases()
    case = cases["facts-clear"]
    held = run_case(case, ProcedureAgent(), "", SessionBudget())
    assert held["passed"], held["failures"]
    executables, services = _requested_names(str(case["given"]["issue_body"]))
    payload = json.dumps(
        {
            "schema_version": 1,
            "trigger_label": "acme-factory",
            "executables": executables,
            "services": services,
        },
        indent=2,
    )
    body = (
        "Factory check requested for: uv. This starts no work.\n\n"
        f"{REQUEST_MARKER}\n```json\n{payload}\n```\n"
    )
    failures = _grade_posted_body(case, body, held["report"], held["graded"])
    assert not any(
        "layout" in failure or "does not match" in failure or "command" in failure
        for failure in failures
    )


def test_facts_reader_accepts_a_json_fence_longer_than_three_backticks() -> None:
    published = _json_fence(str(_reply_case("valid_observed")["comment"]["body"]))
    assert isinstance(published, dict)
    encoded = json.dumps(published, indent=2)
    fence = "`" * 4
    longer = f"note\n{fence}json\n{encoded}\n{fence}\n"
    assert _json_fence(longer) == published
    assert _json_fence(f"```json\n{encoded}\n```\n") == published


def test_refuses_session_41() -> None:
    budget = SessionBudget(max_sessions=SESSION_CEILING)
    for _ in range(SESSION_CEILING):
        budget.start()
    with pytest.raises(SessionBudgetExceeded):
        budget.start()


if __name__ == "__main__":
    raise SystemExit(main())
