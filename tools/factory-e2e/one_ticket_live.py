"""Live driver of the ``one-ticket`` scenario on Jira Cloud (#5793).

``curie dev factory-e2e run --scenario one-ticket --forge jira_cloud
--answer-file <file>`` runs this after the shared preflight installed the
candidate and ``install_jira`` bound the factory agent to the Jira tracker.
The labeller opens and labels a ticket that leaves one decision open; the
factory investigates and asks; once the asking request has settled the driver
posts the labeller's plain answer; the answered run opens one pull request,
which the driver merges through the GitHub merge step. ``judge`` decides from
an observation of plain numbers and booleans, each beat against its bound:

1. ``Investigating this ticket.`` within 180 s of labelling;
2. a question comment (marker ``curie-factory-questions:``) within 1800 s;
3. the answer only after the issue carries ``curie-factory:questions`` and no
   request is live, no request live between that settlement and the answer,
   and the asking request's terminal status comment carrying a usage total
   within the cost bound;
4. the second request within 180 s of the answer, and ``Answer received.``
   within 180 s;
5. exactly one pull request within 3600 s of the answer, naming the Jira key and
   carrying a line starting ``Answered on the ticket by ``;
6. ``Pull request merged:`` within 600 s of the merge;
7. the Jira status id unchanged (correction 2 of #5793), at most 2 requests,
   the factory's usage total at most 25 USD (correction 3), and no Jira issue
   created by the factory account.

The request and cost bounds also hold during the run: after the answer, every
poll of every later beat reads the work item and the bot's status comments, and
a third request or a usage total over the bound (or ``total cost unknown``)
stops the run at once, unmerged, so teardown removes the install and the spend.

Every value that names the caller's environment (repository, accounts, App,
Jira site, project, cloud, kube context, registry, binary paths, home) is an
identity. ``anonymize`` replaces each with its role name in the evidence, in
every log line and in the outcome summary, and ``python3 one_ticket_live.py
check-clean <file>`` reports which roles a file still carries.

The module top level imports only the standard library, so ``check-clean``
runs under the system python3 from a bare checkout; the live driver imports
the harness lazily.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable, Mapping, Sequence
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

# --- bounds and the factory's own words ----------------------------------------------

INVESTIGATING_SECONDS = 180
QUESTION_SECONDS = 1800
SECOND_REQUEST_SECONDS = 180
ANSWER_RECEIVED_SECONDS = 180
PULL_REQUEST_SECONDS = 3600
MERGED_NOTICE_SECONDS = 600
COST_BOUND_USD = Decimal("25")
MAX_REQUESTS = 2
# How long the asking request may take to settle after its question comment;
# the issue says "a few seconds", this only stops a run that never settles.
SETTLE_SECONDS = 600
# The answered run must end within this many seconds of its pull request being
# recorded before the driver merges (it may still wait for the fixture's CI or
# run a CI fix round); owned here so a stuck run cannot stretch the wait to the
# execution deadline.
ENDING_SECONDS = 2700
# Reads of the work item immediately before the answer, while one is unreadable.
PRE_ANSWER_READ_ATTEMPTS = 3
POLL_SECONDS = 5

# apps/api/src/curie_api/factory_notices.py INVESTIGATING_LINE and
# factory_questions.py ANSWER_RECEIVED_LINE start with these sentences.
INVESTIGATING_LINE = "Investigating this ticket."
ANSWER_RECEIVED_LINE = "Answer received."
# factory_notices.py result_section: a questions-asked terminal comment's result
# line is "Questions asked. ..." or "Questions asked (round N of M). ...".
_QUESTIONS_ASKED = re.compile(r"^Questions asked[ .]")
MERGED_LINE = "Pull request merged:"
CITATION_PREFIX = "Answered on the ticket by "
QUESTIONS_MARKER_PREFIX = "curie-factory-questions:"
STATUS_MARKER_PREFIX = "curie-execution-request:"
QUESTIONS_LABEL = "curie-factory:questions"
# The comment entity property the factory writes on Jira
# (apps/api/src/curie_api/forges/jira_cloud/comments.py): POST
# /comment/list?expand=properties returns it as
# ``"properties": [{"key": "curie.factory", "value": {"marker": ...}}]``
# (tools/factory-e2e/fixtures/jira/comments.json records that shape).
COMMENT_PROPERTY = "curie.factory"
# A request in one of these statuses is still owned by the platform.
LIVE_STATUSES = frozenset({"queued", "waiting", "running", "cancellation_requested"})

ENV_FILE_VAR = "CURIE_FACTORY_JIRA_ENV_FILE"
RESTORE_URL_VAR = "CURIE_FACTORY_WEBHOOK_RESTORE_URL"
JIRA_KEYS = (
    "site",
    "cloud_id",
    "project_key",
    "labeler_email",
    "labeler_token_file",
    "bot_email",
    "bot_account_id",
    "bot_token_file",
)
GATEWAY = "https://api.atlassian.com/ex/jira"
GITHUB_USER_URL = "https://api.github.com/user"

# --- builders over raw Jira REST shapes (pure) ----------------------------------------


def status_id(issue: Any) -> str | None:
    """``fields.status.id`` of a Jira issue GET response, or None."""

    fields = issue.get("fields") if isinstance(issue, Mapping) else None
    status = fields.get("status") if isinstance(fields, Mapping) else None
    found = status.get("id") if isinstance(status, Mapping) else None
    return str(found) if isinstance(found, (str, int)) and str(found) else None


def issue_labels(issue: Any) -> list[str]:
    fields = issue.get("fields") if isinstance(issue, Mapping) else None
    labels = fields.get("labels") if isinstance(fields, Mapping) else None
    return [label for label in labels or [] if isinstance(label, str)]


def comment_marker(comment: Any) -> str | None:
    """The ``curie.factory`` property's marker of an expanded Jira comment, or None."""

    properties = comment.get("properties") if isinstance(comment, Mapping) else None
    for entry in properties if isinstance(properties, list) else []:
        if not isinstance(entry, Mapping) or entry.get("key") != COMMENT_PROPERTY:
            continue
        value = entry.get("value")
        marker = value.get("marker") if isinstance(value, Mapping) else None
        if isinstance(marker, str) and marker:
            return marker
    return None


def _children(node: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    content = node.get("content")
    return [c for c in content if isinstance(c, Mapping)] if isinstance(content, list) else []


def _inline(nodes: Iterable[Mapping[str, Any]]) -> str:
    out: list[str] = []
    for node in nodes:
        kind = node.get("type")
        found = node.get("attrs")
        attrs: Mapping[str, Any] = found if isinstance(found, Mapping) else {}
        if kind == "text":
            out.append(str(node.get("text", "")))
        elif kind == "hardBreak":
            out.append("\n")
        elif kind == "mention":
            out.append(str(attrs.get("text", "")))
        elif kind == "inlineCard":
            out.append(str(attrs.get("url", "")))
        else:
            out.append(_inline(_children(node)))
    return "".join(out)


def _blocks(node: Mapping[str, Any]) -> list[str]:
    kind = node.get("type")
    if kind in {"paragraph", "heading", "codeBlock"}:
        return _inline(_children(node)).split("\n")
    if kind in {"orderedList", "bulletList", "listItem", "blockquote"}:
        return [line for child in _children(node) for line in _blocks(child)]
    if kind == "rule":
        return ["---"]
    if kind == "text":
        return _inline([node]).split("\n")
    return [line for child in _children(node) for line in _blocks(child)]


def comment_text(comment: Any) -> str:
    """An expanded Jira comment's ADF body as plain text, one block per line.

    Node types: https://developer.atlassian.com/cloud/jira/platform/apis/document/structure/
    A link mark keeps only its text; list items are their lines without a lead.
    """

    body = comment.get("body") if isinstance(comment, Mapping) else None
    if isinstance(body, str):
        return body
    return "\n".join(_blocks(body)) if isinstance(body, Mapping) else ""


def has_line(text: str, line: str) -> bool:
    """A line of ``text`` is ``line`` or starts with it followed by a space."""

    for candidate in text.splitlines():
        candidate = candidate.strip()
        if candidate == line or candidate.startswith(line + " "):
            return True
    return False


_AMOUNT = r"\$([0-9][0-9,]*(?:\.[0-9]+)?)"
_TOTAL = re.compile(rf"(?:^|, )total (at least )?{_AMOUNT}")
_UNKNOWN = re.compile(r"(?:^|, )total cost unknown\b")


def usage_total(text: str) -> tuple[str, Decimal | None]:
    """Classify the one ``Usage: `` line of a status comment's text.

    The line is built by ``usage_line`` (apps/api/src/curie_api/factory_usage.py):
    ``("total", X)`` for ``total $X``, ``("at_least", X)`` for ``total at least
    $X``, ``("unknown", None)`` for ``total cost unknown``, and ``("missing",
    None)`` when there is no Usage line or no total in it.
    """

    lines = [line.strip() for line in text.splitlines() if line.strip().startswith("Usage: ")]
    if len(lines) != 1:
        return "missing", None
    rest = lines[0].removeprefix("Usage: ")
    if _UNKNOWN.search(rest):
        return "unknown", None
    found = _TOTAL.search(rest)
    if found is None:
        return "missing", None
    try:
        amount = Decimal(found.group(2).replace(",", ""))
    except InvalidOperation:
        return "missing", None
    return ("at_least" if found.group(1) else "total"), amount


def names_key(body: str | None, key: str) -> bool:
    """The body names the Jira key as a whole token. Case-sensitive. Pure.

    Before the key: no letter, digit, ``_`` or ``-`` (``XACME-12`` and
    ``X_ACME-12`` are other keys). After it: no letter, digit or ``_``
    (``ACME-123``, ``ACME-12A``), and no ``-`` followed by a digit:
    ``ACME-12-1`` is a different token, while ``ACME-12-add-rounding`` (a branch
    name) names the key. Punctuation, brackets and the end of a browse URL
    (``ACME-12.``, ``(ACME-12)``, ``ACME-12,``, ``/browse/ACME-12``) do.
    The key part of ``one_ticket_jira.names_ticket``.
    """

    if not key:
        return False
    pattern = rf"(?<![A-Za-z0-9_-]){re.escape(key)}(?![A-Za-z0-9_]|-[0-9])"
    return re.search(pattern, body or "") is not None


def has_citation(body: str | None) -> bool:
    return any(line.lstrip().startswith(CITATION_PREFIX) for line in (body or "").splitlines())


def live_statuses(detail: Mapping[str, Any] | None) -> list[str]:
    """The statuses of the work item's requests that are still live."""

    return [str(r.get("status")) for r in requests_of(detail) if r.get("status") in LIVE_STATUSES]


def requests_of(detail: Mapping[str, Any] | None) -> list[Mapping[str, Any]]:
    requests = (detail or {}).get("requests")
    found = [r for r in requests if isinstance(r, Mapping)] if isinstance(requests, list) else []

    def sequence(request: Mapping[str, Any]) -> int:
        value = request.get("sequence")
        return value if isinstance(value, int) else 0

    return sorted(found, key=sequence)


# --- the judge (pure) -----------------------------------------------------------------


def _after(start: float | None, end: float | None) -> float | None:
    return None if start is None or end is None else end - start


def judge(obs: Mapping[str, Any]) -> list[str]:
    """Every failure of one observation; empty means the run passed.

    Seconds are relative to the labelling instant, None when never observed.
    Messages carry numbers and fixed words only, never an identity.
    """

    failures: list[str] = []
    investigating = obs.get("investigating_s")
    if investigating is None or investigating > INVESTIGATING_SECONDS:
        failures.append(
            f"no '{INVESTIGATING_LINE}' status line within {INVESTIGATING_SECONDS} s of labelling"
        )
    question = obs.get("question_s")
    if question is None or question > QUESTION_SECONDS:
        failures.append(f"no question comment within {QUESTION_SECONDS} s of labelling")

    answer = obs.get("answer_s")
    settled_s = obs.get("settled_s")
    if obs.get("settled") is not True:
        failures.append(
            "the question ending was never observed settled (the questions label on the issue "
            "and no live request in the same read)"
        )
    elif answer is None:
        failures.append("the answer was never posted")
    elif settled_s is None or answer < settled_s:
        failures.append("the answer was posted before the question ending was observed settled")
    live = list(obs.get("live_before_answer") or [])
    if live:
        failures.append(
            "a request was live between the settlement and the answer: " + ", ".join(sorted(live))
        )
    elif obs.get("settled") is True:
        pre_answer = obs.get("pre_answer_read")
        if pre_answer == "unreadable":
            failures.append("the work item could not be read immediately before the answer")
        elif pre_answer == "no_request":
            failures.append("the work item showed no request immediately before the answer")
        elif pre_answer == "extra_request":
            failures.append(
                "the work item showed more than the one asking request immediately before "
                "the answer"
            )
        elif pre_answer != "ok":
            failures.append("the work item was not read immediately before the answer")
    failures.extend(_judge_pre_answer_usage(obs))

    second = _after(answer, obs.get("second_request_s"))
    if second is None:
        failures.append("no second request after the answer")
    elif second > SECOND_REQUEST_SECONDS:
        failures.append(
            f"the second request came {second:.0f} s after the answer, over "
            f"{SECOND_REQUEST_SECONDS} s"
        )
    received = _after(answer, obs.get("answer_received_s"))
    if received is None or received > ANSWER_RECEIVED_SECONDS:
        failures.append(
            f"no '{ANSWER_RECEIVED_LINE}' line within {ANSWER_RECEIVED_SECONDS} s of the answer"
        )

    pulls = [pr for pr in obs.get("pull_requests") or [] if isinstance(pr, Mapping)]
    if len(pulls) != 1:
        failures.append(f"{len(pulls)} pull requests opened, expected exactly 1")
    if pulls:
        pull = pulls[0]
        opened = _after(answer, pull.get("opened_s"))
        if opened is None or opened > PULL_REQUEST_SECONDS:
            shown = "never" if opened is None else f"{opened:.0f} s after the answer"
            failures.append(
                f"the pull request opened {shown}, over {PULL_REQUEST_SECONDS} s after the answer"
            )
        if pull.get("names_key") is not True:
            failures.append("the pull request body does not name the Jira key")
        if pull.get("has_citation") is not True:
            failures.append(f"the pull request body has no line starting '{CITATION_PREFIX}'")

    stop = obs.get("spend_stop")
    stopped = False
    if isinstance(stop, Mapping):
        stopped = True
        failures.append(_stop_message(stop))
    if obs.get("answered_run_ended") is False:
        failures.append(
            f"the answered run did not end within {ENDING_SECONDS} s of the pull request; "
            "not merged"
        )
    elif obs.get("merge_ok") is not True and not stopped:
        failures.append("the merge failed")
    notice = _after(obs.get("merged_s"), obs.get("merged_notice_s"))
    if notice is None or notice > MERGED_NOTICE_SECONDS:
        failures.append(f"no '{MERGED_LINE}' line within {MERGED_NOTICE_SECONDS} s of the merge")

    unchanged = obs.get("status_id_unchanged")
    if unchanged is None:
        failures.append("the Jira status id could not be read before labelling and after the merge")
    elif unchanged is not True:
        failures.append("the Jira status id changed between labelling and the merged notice")

    count = obs.get("request_count")
    if not isinstance(count, int):
        failures.append("the request count could not be read")
    elif count > MAX_REQUESTS:
        failures.append(f"{count} execution requests, over the bound of {MAX_REQUESTS}")

    failures.extend(_judge_usage(obs.get("usage")))

    created = obs.get("factory_created_issues")
    if created is None:
        failures.append("the Jira issues the factory account created could not be read")
    elif created != 0:
        failures.append(f"the factory account created {created} Jira issues, expected 0")
    return failures


def _judge_pre_answer_usage(obs: Mapping[str, Any]) -> list[str]:
    """The asking request's usage, read before the answer was posted."""

    usage = obs.get("pre_answer_usage")
    if not isinstance(usage, Mapping):
        if obs.get("answer_s") is None:
            return []
        return ["the asking request's usage was not read before the answer"]
    kind = usage.get("kind")
    if kind == "unknown":
        return [
            "the asking request's usage line read 'total cost unknown' before the answer; "
            "not answered"
        ]
    missing = (
        "the asking request's terminal status comment carried no usage total before the "
        "answer; not answered"
    )
    if kind not in ("total", "at_least"):
        return [missing]
    try:
        total = Decimal(str(usage.get("total")))
    except InvalidOperation:
        return [missing]
    if total > COST_BOUND_USD:
        lead = "at least " if kind == "at_least" else ""
        return [
            f"the asking request's estimated cost was {lead}${total} before the answer, "
            f"over the {COST_BOUND_USD} USD bound; not answered"
        ]
    return []


def _stop_message(stop: Mapping[str, Any]) -> str:
    lead = "the run was stopped after the answer: "
    if stop.get("reason") == "requests":
        return (
            f"{lead}the work item showed {stop.get('count')} execution requests, over the bound "
            f"of {MAX_REQUESTS}"
        )
    if stop.get("kind") == "unknown":
        return f"{lead}a status comment's usage line reads 'total cost unknown'"
    at_least = "at least " if stop.get("kind") == "at_least" else ""
    return (
        f"{lead}a status comment's estimated cost is {at_least}${stop.get('total')}, over the "
        f"{COST_BOUND_USD} USD bound"
    )


def _judge_usage(usage: Any) -> list[str]:
    kind = usage.get("kind") if isinstance(usage, Mapping) else None
    if kind == "unknown":
        return ["the usage line reads 'total cost unknown'"]
    if kind not in ("total", "at_least"):
        return ["the merged status comment carries no usage total"]
    try:
        total = Decimal(str(usage.get("total")))
    except InvalidOperation:
        return ["the merged status comment carries no usage total"]
    if total > COST_BOUND_USD:
        lead = "at least " if kind == "at_least" else ""
        return [f"the estimated cost is {lead}${total}, over the {COST_BOUND_USD} USD bound"]
    return []


# --- identities and anonymization -------------------------------------------------------

# Role name -> the raw values that name it.
Identities = dict[str, list[str | int]]

# Matching order when one value appears under two roles.
ROLE_ORDER = (
    "fixture-repo",
    "actor",
    "operator",
    "app",
    "installation",
    "site",
    "cloud",
    "project",
    "labeller",
    "bot",
    "status",
    "context",
    "registry",
    "binary",
    "restore-url",
    "issue",
    "builder",
    "home",
)


class IdentityError(Exception):
    """An identity input is unreadable. The message names a variable or key, never a value."""


def add_identity(identities: Identities, role: str, *values: Any) -> None:
    """Add each present value under ``role`` once."""

    bucket = identities.setdefault(role, [])
    for value in values:
        if value is None or isinstance(value, bool):
            continue
        item: str | int = value if isinstance(value, int) else str(value).strip()
        if item == "" or item in bucket:
            continue
        bucket.append(item)


def add_builder(identities: Identities, name: str | None) -> None:
    """BUILDX_BUILDER's value under ``builder``, unless it is Docker's built-in
    ``default`` builder: that word is generic, and anonymizing it would rewrite
    unrelated text such as ``default_branch``."""

    if name is not None and name.strip().lower() != "default":
        add_identity(identities, "builder", name)


def _entries(identities: Mapping[str, Sequence[str | int]]) -> list[tuple[str, str, bool]]:
    """(value, role, case sensitive), longest first, sensitive first on a tie.

    The project key and display names (values holding whitespace) match
    case-sensitively; everything else (logins, repository names, emails, slugs,
    hosts, paths, ids) case-insensitively. Values under 2 characters are skipped.
    """

    order = {role: index for index, role in enumerate(ROLE_ORDER)}
    seen: set[tuple[str, bool]] = set()
    entries: list[tuple[str, str, bool]] = []
    for role in sorted(identities, key=lambda r: (order.get(r, len(order)), r)):
        for raw in identities[role]:
            if isinstance(raw, bool):
                continue
            text = str(raw).strip()
            if len(text) < 2:
                continue
            sensitive = role == "project" or any(ch.isspace() for ch in text)
            # The same value under a second role keeps the first role.
            dedupe = (text if sensitive else text.lower(), sensitive)
            if dedupe in seen:
                continue
            seen.add(dedupe)
            entries.append((text, role, sensitive))
    entries.sort(key=lambda e: (-len(e[0]), not e[2]))
    return entries


_PATTERNS: dict[tuple[tuple[str, str, bool], ...], tuple[re.Pattern[str], list[str]]] = {}


def _compiled(
    identities: Mapping[str, Sequence[str | int]],
) -> tuple[re.Pattern[str] | None, list[str]]:
    entries = tuple(_entries(identities))
    if not entries:
        return None, []
    cached = _PATTERNS.get(entries)
    if cached is not None:
        return cached
    parts = []
    for index, (value, _role, sensitive) in enumerate(entries):
        escaped = re.escape(value)
        parts.append(f"(?P<g{index}>{escaped if sensitive else f'(?i:{escaped})'})")
    pattern = re.compile(r"(?<![A-Za-z0-9])(?:" + "|".join(parts) + r")(?![A-Za-z0-9])")
    roles = [role for _value, role, _sensitive in entries]
    if len(_PATTERNS) > 64:
        _PATTERNS.clear()
    _PATTERNS[entries] = (pattern, roles)
    return pattern, roles


def _numeric(identities: Mapping[str, Sequence[str | int]]) -> dict[int, str]:
    found: dict[int, str] = {}
    for value, role, _sensitive in _entries(identities):
        if value.isdigit():
            found.setdefault(int(value), role)
    return found


def anonymize(value: Any, identities: Mapping[str, Sequence[str | int]]) -> Any:
    """A copy of ``value`` with every identity replaced by its role name.

    Recurses dicts (keys and values), lists and tuples. In strings each raw
    value is matched on token boundaries, longest first; an int equal to a
    numeric identity becomes the role name. Pure.
    """

    pattern, roles = _compiled(identities)
    numeric = _numeric(identities)

    def text(item: str) -> str:
        if pattern is None:
            return item
        return pattern.sub(lambda m: roles[int(str(m.lastgroup)[1:])], item)

    def visit(item: Any) -> Any:
        if isinstance(item, str):
            return text(item)
        if isinstance(item, bool) or item is None or isinstance(item, float):
            return item
        if isinstance(item, int):
            return numeric.get(item, item)
        if isinstance(item, Mapping):
            return {visit(k): visit(v) for k, v in item.items()}
        if isinstance(item, tuple):
            return tuple(visit(v) for v in item)
        if isinstance(item, list):
            return [visit(v) for v in item]
        return item

    return visit(value)


def find_identities(text: str, identities: Mapping[str, Sequence[str | int]]) -> list[str]:
    """The sorted role names of every raw identity still present in ``text``."""

    pattern, roles = _compiled(identities)
    if pattern is None:
        return []
    return sorted({roles[int(str(m.lastgroup)[1:])] for m in pattern.finditer(text)})


def _jira_facts(environ: Mapping[str, str]) -> tuple[dict[str, str], Path]:
    """The Jira env file's facts (the format of forges/jira_cloud.py's docstring)."""

    named = environ.get(ENV_FILE_VAR)
    if not named:
        raise IdentityError(f"{ENV_FILE_VAR} must name the Jira test site env file")
    env_file = Path(named).expanduser()
    try:
        text = env_file.read_text()
    except OSError:
        raise IdentityError(f"{ENV_FILE_VAR} names an unreadable file") from None
    facts: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        facts[key.strip()] = value.strip()
    missing = [key for key in JIRA_KEYS if not facts.get(key)]
    if missing:
        raise IdentityError(f"the {ENV_FILE_VAR} file sets no {', '.join(missing)}")
    return facts, env_file.parent


def load_identities(
    environ: Mapping[str, str],
    *,
    fetch_myself: Callable[[str], Mapping[str, Any]],
    github_login: Callable[[str], str],
) -> Identities:
    """Every identity the run's environment names, by role.

    Optional inputs that are absent are skipped. An unreadable App directory or
    Jira env file, or a failed account read, raises IdentityError: a check
    without those identities would be falsely clean.
    """

    ids: Identities = {}
    meta: Mapping[str, Any] = {}
    if environ.get("CURIE_FACTORY_APP_DIR"):
        app_json = Path(environ["CURIE_FACTORY_APP_DIR"]).expanduser() / "app.json"
        try:
            loaded = json.loads(app_json.read_text())
        except (OSError, ValueError):
            raise IdentityError("CURIE_FACTORY_APP_DIR holds no readable app.json") from None
        meta = loaded if isinstance(loaded, Mapping) else {}
    repo = environ.get("CURIE_FACTORY_REPO") or str(meta.get("repo") or "")
    if repo:
        owner, _, name = repo.partition("/")
        add_identity(ids, "fixture-repo", repo, name, owner)
    add_identity(ids, "app", environ.get("CURIE_FACTORY_APP_ID") or meta.get("id"))
    slug = environ.get("CURIE_FACTORY_MENTION") or str(meta.get("slug") or "")
    if slug:
        add_identity(ids, "app", slug, f"{slug}[bot]")
    add_identity(
        ids,
        "installation",
        environ.get("CURIE_FACTORY_INSTALLATION_ID") or meta.get("installation_id"),
    )
    if environ.get("CURIE_FACTORY_ACTOR_GH_USER"):
        add_identity(ids, "actor", environ["CURIE_FACTORY_ACTOR_GH_USER"])
    elif environ.get("CURIE_FACTORY_ACTOR_TOKEN"):
        add_identity(ids, "actor", github_login("actor"))
    add_identity(
        ids, "operator", environ.get("CURIE_FACTORY_OPERATOR_LOGIN") or github_login("operator")
    )

    facts, _ = _jira_facts(environ)
    add_identity(ids, "site", facts["site"])
    add_identity(ids, "cloud", facts["cloud_id"])
    add_identity(ids, "project", facts["project_key"])
    add_identity(ids, "labeller", facts["labeler_email"])
    add_identity(ids, "bot", facts["bot_email"], facts["bot_account_id"])
    for actor, role in (("labeler", "labeller"), ("bot", "bot")):
        me = fetch_myself(actor)
        if not isinstance(me, Mapping) or not me.get("accountId"):
            raise IdentityError(f"the Jira {actor} account could not be read from /myself")
        add_identity(ids, role, me.get("accountId"), me.get("displayName"), me.get("emailAddress"))

    add_identity(ids, "context", environ.get("CURIE_FACTORY_KUBE_CONTEXT"))
    registry = environ.get("CURIE_FACTORY_LAYER_REGISTRY") or ""
    if registry:
        add_identity(ids, "registry", registry, registry.split("/", 1)[0])
    for variable in ("CURIE_BIN", "CURIE_FACTORY_CLOUDFLARED"):
        if "/" in environ.get(variable, ""):
            add_identity(ids, "binary", environ[variable])
    # The variable load_config reads into config.restore_webhook_url.
    add_identity(ids, "restore-url", environ.get(RESTORE_URL_VAR))
    # factory_e2e.check_build_tools records the builder's name in the evidence.
    add_builder(ids, environ.get("BUILDX_BUILDER"))
    add_identity(ids, "home", environ.get("HOME"))
    return {role: values for role, values in ids.items() if values}


def default_fetch_myself(environ: Mapping[str, str]) -> Callable[[str], Mapping[str, Any]]:
    """``GET /myself`` through the API gateway, Basic auth from the token file (as
    ``jira_cloud.jira_request`` does)."""

    def fetch(actor: str) -> Mapping[str, Any]:
        facts, base = _jira_facts(environ)
        token_path = base / facts[f"{actor}_token_file"]
        try:
            token = token_path.read_text().strip()
        except OSError:
            raise IdentityError(f"the Jira {actor}_token_file is unreadable") from None
        basic = base64.b64encode(f"{facts[f'{actor}_email']}:{token}".encode()).decode()
        request = urllib.request.Request(
            f"{GATEWAY}/{facts['cloud_id']}/rest/api/3/myself",
            headers={"Authorization": f"Basic {basic}", "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = json.load(response)
        except (OSError, ValueError):
            raise IdentityError(
                f"the Jira {actor} account could not be read from /myself"
            ) from None
        return body if isinstance(body, Mapping) else {}

    return fetch


def default_github_login(environ: Mapping[str, str]) -> Callable[[str], str]:
    """The actor's login from its token, the operator's from ``gh api user``."""

    def login(role: str) -> str:
        if role == "operator":
            try:
                done = subprocess.run(
                    ["gh", "api", "user", "--jq", ".login"],
                    capture_output=True,
                    text=True,
                    timeout=60,
                    check=False,
                )
            except (OSError, subprocess.TimeoutExpired):
                done = None
            name = done.stdout.strip() if done is not None and done.returncode == 0 else ""
            if not name:
                raise IdentityError(
                    "the operator login could not be read; set CURIE_FACTORY_OPERATOR_LOGIN"
                )
            return name
        request = urllib.request.Request(
            GITHUB_USER_URL,
            headers={
                "Authorization": f"Bearer {environ.get('CURIE_FACTORY_ACTOR_TOKEN', '')}",
                "Accept": "application/vnd.github+json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = json.load(response)
        except (OSError, ValueError):
            body = {}
        login_name = body.get("login") if isinstance(body, Mapping) else None
        if not isinstance(login_name, str) or not login_name:
            raise IdentityError("CURIE_FACTORY_ACTOR_TOKEN could not read the actor's login")
        return login_name

    return login


# --- the outcome summary ----------------------------------------------------------------


def _seconds(value: Any) -> str:
    return "not observed" if not isinstance(value, (int, float)) else f"{value:.0f}"


def _usd(value: Any) -> str:
    return "not recorded" if not isinstance(value, (int, float)) else f"{value:.2f} USD"


def outcome_summary(
    evidence: Mapping[str, Any], identities: Mapping[str, Sequence[str | int]] | None = None
) -> str:
    """The lines the pull request body quotes: commit, result, counts, beats and cost."""

    obs = evidence.get("scenario")
    obs = obs if isinstance(obs, Mapping) else {}
    answer = obs.get("answer_s")
    pulls = [pr for pr in obs.get("pull_requests") or [] if isinstance(pr, Mapping)]
    first = pulls[0] if pulls else {}
    recorded = obs.get("usage")
    usage: Mapping[str, Any] = recorded if isinstance(recorded, Mapping) else {}
    kind = usage.get("kind", "missing")
    total = usage.get("total")
    cost = {
        "total": f"total ${total}",
        "at_least": f"total at least ${total}",
        "unknown": "total cost unknown",
    }.get(str(kind), "no usage total")
    result = evidence.get("result") or obs.get("verdict") or "pending"
    lines = [
        f"candidate commit: {evidence.get('candidate_commit')}",
        f"scenario result: {result}",
        f"requests: {obs.get('request_count')}; pull requests: {len(pulls)}; "
        f"factory-created Jira issues: {obs.get('factory_created_issues')}",
        "seconds: "
        f"labelling to investigating {_seconds(obs.get('investigating_s'))}; "
        f"labelling to question {_seconds(obs.get('question_s'))}; "
        f"labelling to settled {_seconds(obs.get('settled_s'))}; "
        f"answer to second request {_seconds(_after(answer, obs.get('second_request_s')))}; "
        f"answer to answer received {_seconds(_after(answer, obs.get('answer_received_s')))}; "
        f"answer to pull request {_seconds(_after(answer, first.get('opened_s')))}; "
        "merge to merged notice "
        f"{_seconds(_after(obs.get('merged_s'), obs.get('merged_notice_s')))}",
        f"cost: {cost} (bound {COST_BOUND_USD} USD)",
    ]
    if obs.get("model_usage_before") is not None or obs.get("model_usage_after") is not None:
        lines.append(
            f"model key usage: before {_usd(obs.get('model_usage_before'))}, "
            f"after {_usd(obs.get('model_usage_after'))}"
        )
    text = "\n".join(lines)
    return text if identities is None else str(anonymize(text, identities))


# --- check-clean --------------------------------------------------------------------------


def main(
    argv: Sequence[str],
    environ: Mapping[str, str] = os.environ,
    *,
    fetch_myself: Callable[[str], Mapping[str, Any]] | None = None,
    github_login: Callable[[str], str] | None = None,
) -> int:
    parser = argparse.ArgumentParser(
        prog="one_ticket_live.py",
        description="Report which identity roles of the run's environment a file still carries.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    clean = sub.add_parser("check-clean", help="print clean, or the identity roles found")
    clean.add_argument("file", type=Path)
    args = parser.parse_args(list(argv))
    try:
        identities = load_identities(
            environ,
            fetch_myself=fetch_myself or default_fetch_myself(environ),
            github_login=github_login or default_github_login(environ),
        )
    except IdentityError as exc:
        print(f"check-clean: {exc}", file=sys.stderr)
        return 2
    try:
        text = args.file.read_text()
    except (OSError, UnicodeDecodeError):
        print("check-clean: the named file could not be read as text", file=sys.stderr)
        return 2
    found = find_identities(text, identities)
    if found:
        print("identities found: " + ", ".join(found))
        return 1
    print("clean")
    return 0


# --- the live driver (I/O; imports the harness lazily) ------------------------------------

_INSTALLED_FILTERS: list[Callable[[str], str]] = []


def evidence_serializer(identities: Identities) -> Callable[[dict[str, Any]], str]:
    """Anonymize, dump as indented JSON, and refuse text that still names an identity."""

    def serialize(evidence: dict[str, Any]) -> str:
        plain = json.loads(json.dumps(evidence, default=str))
        text = json.dumps(anonymize(plain, identities), indent=2, sort_keys=True)
        found = find_identities(text, identities)
        if found:
            raise ValueError("identities remain after anonymizing: " + ", ".join(found))
        return text + "\n"

    return serialize


def install_sanitizer(p: Any, identities: Identities) -> None:
    """The evidence serializer and a log filter, both through ``anonymize``.

    A later call replaces the filter an earlier one installed.
    """

    import factory_e2e as fe

    p.scenario_options["identities"] = identities
    p.evidence_serializer = evidence_serializer(identities)
    for old in _INSTALLED_FILTERS:
        if old in fe.LOG_FILTERS:
            fe.LOG_FILTERS.remove(old)
    _INSTALLED_FILTERS.clear()

    def sanitize(text: str) -> str:
        return str(anonymize(text, identities))

    _INSTALLED_FILTERS.append(sanitize)
    fe.LOG_FILTERS.append(sanitize)


def config_identities(p: Any) -> Identities:
    """The identities the loaded configuration names; no account read."""

    ids: Identities = {}
    owner, _, name = str(p.config.repo).partition("/")
    add_identity(ids, "fixture-repo", p.config.repo, name, owner)
    add_identity(ids, "app", str(p.config.app_id))
    if p.config.mention:
        add_identity(ids, "app", p.config.mention, f"{p.config.mention}[bot]")
    add_identity(ids, "installation", int(p.config.installation_id))
    add_identity(ids, "context", p.config.kube_context)
    add_identity(ids, "operator", p.config.operator_login)
    if p.config.layer_registry:
        add_identity(
            ids, "registry", p.config.layer_registry, p.config.layer_registry.split("/", 1)[0]
        )
    for binary in (p.config.curie_bin, p.config.cloudflared):
        if "/" in str(binary or ""):
            add_identity(ids, "binary", binary)
    add_identity(ids, "restore-url", p.config.restore_webhook_url)
    add_builder(ids, os.environ.get("BUILDX_BUILDER"))
    add_identity(ids, "home", os.environ.get("HOME"))
    return {role: values for role, values in ids.items() if values}


def _merged(*sets: Mapping[str, Sequence[str | int]]) -> Identities:
    merged: Identities = {}
    for one in sets:
        for role, values in one.items():
            add_identity(merged, role, *values)
    return merged


def add_run_identities(p: Any) -> None:
    """Add what the run learned (repository id, actor login) and reinstall the sanitizer."""

    identities: Identities = p.scenario_options.get("identities") or {}
    if getattr(p, "repository_id", 0):
        add_identity(identities, "fixture-repo", int(p.repository_id))
    add_identity(identities, "actor", p.evidence.get("actor_login"))
    install_sanitizer(p, identities)


def before_run(p: Any) -> None:
    """Install the sanitizer before any lock, kubectl or helm call.

    The configuration's identities first (no network), so even a refused account
    read writes no raw value; then every identity of the environment.
    """

    import factory_e2e as fe

    first = config_identities(p)
    install_sanitizer(p, first)
    try:
        loaded = load_identities(
            os.environ,
            fetch_myself=default_fetch_myself(os.environ),
            github_login=default_github_login(os.environ),
        )
    except IdentityError as exc:
        raise fe.ConfigError(f"one-ticket identities: {exc}") from None
    install_sanitizer(p, _merged(first, loaded))


def finish(p: Any, _results: list[dict[str, Any]]) -> bool:
    """After teardown, before the evidence is written: every run identity is known."""

    add_run_identities(p)
    return True


def new_observation() -> dict[str, Any]:
    return {
        "investigating_s": None,
        "question_s": None,
        "settled": False,
        "settled_s": None,
        "pre_answer_read": None,
        "live_before_answer": [],
        "pre_answer_usage": None,
        "answer_s": None,
        "answer_instant": None,
        "second_request_s": None,
        "answer_received_s": None,
        "pull_requests": [],
        "spend_stop": None,
        "answered_run_ended": None,
        "merge_ok": False,
        "merged_s": None,
        "merged_notice_s": None,
        "status_id_unchanged": None,
        "request_count": None,
        "usage": {"kind": "missing", "total": None},
        "factory_created_issues": None,
        "timeline": [],
    }


class _LiveRun:
    """One live one-ticket run: each beat is a method, bounded by its deadline.

    ``clock`` and ``sleep`` are injectable so the beats run offline in tests.
    """

    def __init__(
        self,
        p: Any,
        *,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        import factory_e2e as fe

        self.fe = fe
        self.p = p
        self.clock = clock
        self.sleep = sleep
        self.driver = p.loop_driver()
        self.obs = new_observation()
        self.obs["forge"] = self.driver.forge
        self.obs["pairing"] = self.driver.pairing
        p.evidence["scenario"] = self.obs
        self.issue_id = ""
        self.key = ""
        self.work_item_id = ""
        self.status_before: str | None = None
        self.answer_at: float | None = None
        self.pr_recorded_at: float | None = None
        self.status_markers: set[str] = set()
        self.second_marker: str | None = None
        self.change: Any = None

    # -- small reads --

    def rel(self, at: float | None = None) -> float:
        return round((self.clock() if at is None else at) - float(self.p.labelled_at), 1)

    def note(self, event: str, **facts: Any) -> None:
        self.obs["timeline"].append({"event": event, "at_s": self.rel(), **facts})
        self.fe.log(f"one-ticket: {event}")

    def comments(self) -> list[tuple[str | None, str]]:
        listed = self.driver.bot_comments(self.key) or []
        return [(comment_marker(c), comment_text(c)) for c in listed]

    # -- the spend bound during the run --

    def over_spend(
        self, detail: Mapping[str, Any] | None, comments: Sequence[tuple[str | None, str]]
    ) -> bool:
        """Whether the run must stop now; the first reason is recorded in ``spend_stop``.

        A work item with more than MAX_REQUESTS requests (an automatic successor
        would start a third, workitems/lifecycle.py ``admit_automatic_successor``),
        or any status comment whose Usage line totals over the bound or reads
        ``total cost unknown``. An unreadable work item skips the count only.
        """

        if self.obs["spend_stop"] is not None:
            return True
        reason: dict[str, Any] | None = None
        count = len(requests_of(detail)) if detail is not None else 0
        if count > MAX_REQUESTS:
            reason = {"reason": "requests", "count": count}
        else:
            for marker, text in comments:
                if not (marker and marker.startswith(STATUS_MARKER_PREFIX)):
                    continue
                kind, total = usage_total(text)
                if kind == "unknown" or (total is not None and total > COST_BOUND_USD):
                    reason = {
                        "reason": "usage",
                        "kind": kind,
                        "total": None if total is None else str(total),
                    }
                    break
        if reason is None:
            return False
        self.obs["spend_stop"] = reason
        self.note("the request or cost bound was crossed; stopping the run", **reason)
        return True

    def poll(self) -> tuple[Mapping[str, Any] | None, list[tuple[str | None, str]], bool]:
        """One read of the work item and the comments after the answer, and the spend check."""

        detail: Mapping[str, Any] | None = self.p.work_item_detail(self.work_item_id)
        comments = self.comments()
        return detail, comments, self.over_spend(detail, comments)

    # -- beats --

    def open_ticket(self) -> None:
        title, body = self.p.issue_spec or self.fe.parse_issue_file(self.fe.ONE_TICKET_ISSUE_FILE)

        def before_label(issue_id: str, key: str) -> None:
            # The issue id is in the driver's own failure messages (the label
            # PUT below), and the key names the issue whole: both are identities
            # from here on (another key of the project still loses the project).
            identities = self.p.scenario_options.get("identities") or {}
            add_identity(identities, "issue", issue_id, key)
            install_sanitizer(self.p, identities)
            status, issue = self.driver.issue_fields(issue_id)
            found = status_id(issue) if status == 200 else None
            if found is None:
                raise self.fe.PreflightFailed(
                    f"the Jira issue's status could not be read before labelling (HTTP {status})"
                )
            self.issue_id, self.key, self.status_before = issue_id, key, found
            add_identity(identities, "status", found)
            install_sanitizer(self.p, identities)
            self.p.labelled_at = self.clock()

        self.p.labelled_at = self.clock()
        self.work_item_id = self.driver.open_and_label_ticket(
            title, body, self.p.labelled_at, before_label=before_label
        )
        self.obs["work_item_id"] = self.work_item_id
        self.driver.pin_fixture_base()

    def await_question(self) -> bool:
        start = self.p.labelled_at
        while True:
            for marker, text in self.comments():
                if marker and marker.startswith(STATUS_MARKER_PREFIX):
                    self.status_markers.add(marker)
                    if self.obs["investigating_s"] is None and has_line(text, INVESTIGATING_LINE):
                        self.obs["investigating_s"] = self.rel()
                        self.note("investigating line seen")
                if (
                    marker
                    and marker.startswith(QUESTIONS_MARKER_PREFIX)
                    and self.obs["question_s"] is None
                ):
                    self.obs["question_s"] = self.rel()
                    self.note("question comment seen")
            now = self.clock()
            if self.obs["question_s"] is not None and (
                self.obs["investigating_s"] is not None or now > start + INVESTIGATING_SECONDS
            ):
                return True
            if now > start + QUESTION_SECONDS:
                return self.obs["question_s"] is not None
            self.sleep(POLL_SECONDS)

    def _pre_answer_detail(self, deadline: float) -> Mapping[str, Any] | None:
        """A fresh work item read, retried while unreadable within ``deadline``."""

        for attempt in range(PRE_ANSWER_READ_ATTEMPTS):
            if attempt:
                if self.clock() > deadline:
                    break
                self.sleep(POLL_SECONDS)
            detail: Mapping[str, Any] | None = self.p.work_item_detail(self.work_item_id)
            if detail is not None:
                return detail
        return None

    def settle_and_answer(self, answer: str) -> bool:
        deadline = self.clock() + SETTLE_SECONDS
        while True:
            status, issue = self.driver.issue_fields(self.issue_id)
            detail = self.p.work_item_detail(self.work_item_id)
            if (
                status == 200
                and QUESTIONS_LABEL in issue_labels(issue)
                and requests_of(detail)
                and not live_statuses(detail)
            ):
                self.obs["settled"] = True
                self.obs["settled_s"] = self.rel()
                self.note("question ending settled")
                break
            if self.clock() > deadline:
                self.note("the question ending never settled")
                return False
            self.sleep(POLL_SECONDS)
        for marker, _text in self.comments():
            if marker and marker.startswith(STATUS_MARKER_PREFIX):
                self.status_markers.add(marker)
        if not self._asking_usage_within_bound(deadline):
            return False
        # The last read before the post: it must be readable, hold the first
        # request and show none live, or no answer is posted (and no paid
        # second run starts).
        final = self._pre_answer_detail(deadline)
        if final is None:
            self.obs["pre_answer_read"] = "unreadable"
            self.note("the work item was unreadable before the answer; not answering")
            return False
        if not requests_of(final):
            self.obs["pre_answer_read"] = "no_request"
            self.note("the work item showed no request before the answer; not answering")
            return False
        live = live_statuses(final)
        if live:
            self.obs["pre_answer_read"] = "live"
            self.obs["live_before_answer"] = live
            self.note("a request was live before the answer; not answering")
            return False
        if len(requests_of(final)) != 1:
            self.obs["pre_answer_read"] = "extra_request"
            self.note("more than the asking request existed before the answer; not answering")
            return False
        self.obs["pre_answer_read"] = "ok"
        # A broken earlier beat stops the run before the next paid model call.
        investigating = self.obs["investigating_s"]
        question = self.obs["question_s"]
        if (
            investigating is None
            or investigating > INVESTIGATING_SECONDS
            or question is None
            or question > QUESTION_SECONDS
        ):
            self.note("an earlier beat failed (investigating line or question); not answering")
            return False
        # The answer's instant is Jira's own record of the comment; without a
        # readable one, the clock before the POST (the earliest it can be), so a
        # slow POST never stretches the bounds measured from the answer.
        posted_from = self.clock()
        status, body = self.driver.post_comment(self.issue_id, answer)
        if status != 201:
            self.note("posting the answer failed", http_status=status)
            return False
        created = self.fe.jira_created_epoch(
            body.get("created") if isinstance(body, Mapping) else None
        )
        self.answer_at = posted_from if created is None else created
        self.obs["answer_instant"] = "before_post" if created is None else "jira_created"
        self.obs["answer_s"] = self.rel(self.answer_at)
        self.note("answer posted")
        return True

    def _asking_usage_within_bound(self, deadline: float) -> bool:
        """The asking request's terminal status comment, read and bounded before the answer.

        factory_notices.py ``_render`` writes the Usage line (``usage_line``,
        cumulative over the WorkItem) into every terminal comment, questions
        asked included, and ``usage_line`` returns a line whenever a run
        started (``not reported for N runs`` when none reported). So the
        asking request's terminal comment always carries one: a missing total
        or ``total cost unknown`` refuses the answer (fail closed), as does a
        total over the bound. The comment is awaited within ``deadline``: its
        edit can trail the settlement the driver observed.
        """

        while True:
            terminal: list[str] = []
            for marker, text in self.comments():
                if not (marker and marker.startswith(STATUS_MARKER_PREFIX)):
                    continue
                self.status_markers.add(marker)
                if any(_QUESTIONS_ASKED.match(line.strip()) for line in text.splitlines()):
                    terminal.append(text)
            if terminal:
                break
            if self.clock() > deadline:
                self.obs["pre_answer_usage"] = {"kind": "missing", "total": None}
                self.note("the asking request's terminal status comment never appeared")
                return False
            self.sleep(POLL_SECONDS)
        # More than one terminal comment is not expected; the worst one decides.
        read = [usage_total(text) for text in terminal]
        bad = [(kind, total) for kind, total in read if kind not in ("total", "at_least")]
        kind, total = bad[0] if bad else max(read, key=lambda r: r[1] or Decimal(0))
        self.obs["pre_answer_usage"] = {
            "kind": kind,
            "total": None if total is None else str(total),
        }
        if kind not in ("total", "at_least") or total is None or total > COST_BOUND_USD:
            self.note("the asking request's usage is not within the bound; not answering")
            return False
        return True

    def await_second_request(self) -> bool:
        assert self.answer_at is not None
        deadline = self.answer_at + max(SECOND_REQUEST_SECONDS, ANSWER_RECEIVED_SECONDS)
        while True:
            detail, comments, stop = self.poll()
            if stop:
                return False
            if self.obs["second_request_s"] is None:
                requests = requests_of(detail)
                if len(requests) >= 2:
                    created = self.fe._parse_time(requests[1].get("created_at"))
                    at = created.timestamp() if created is not None else None
                    self.obs["second_request_s"] = self.rel(at)
                    self.note("second request created")
            if self.obs["answer_received_s"] is None:
                for marker, text in comments:
                    if (
                        marker
                        and marker.startswith(STATUS_MARKER_PREFIX)
                        and marker not in self.status_markers
                        and has_line(text, ANSWER_RECEIVED_LINE)
                    ):
                        self.second_marker = marker
                        self.obs["answer_received_s"] = self.rel()
                        self.note("answer received line seen")
                        break
            if (
                self.obs["second_request_s"] is not None
                and self.obs["answer_received_s"] is not None
            ):
                return True
            if self.clock() > deadline:
                return self.obs["second_request_s"] is not None
            self.sleep(POLL_SECONDS)

    def await_pull_request(self) -> bool:
        """The change the WorkItem records, within the pull request bound of the answer."""

        assert self.answer_at is not None
        deadline = self.answer_at + PULL_REQUEST_SECONDS
        while True:
            read, _comments, stop = self.poll()
            if stop:
                return False
            detail = read or {}
            pr = detail.get("pr")
            if isinstance(pr, dict) and pr.get("number"):
                self.change = self.fe.ChangeRef(int(pr["number"]), str(pr.get("url") or ""))
                self.pr_recorded_at = self.clock()
                self.note("pull request recorded on the work item")
                break
            requests = requests_of(detail)
            if len(requests) >= 2 and not live_statuses(detail):
                self.note("the answered run ended without a pull request")
                break
            if self.clock() > deadline:
                self.note("no pull request within the bound")
                break
            self.sleep(POLL_SECONDS)
        self.obs["pull_requests"] = self._list_pull_requests()
        return self.change is not None

    def _list_pull_requests(self) -> list[dict[str, Any]]:
        """Every pull request the run opened, as the judge reads them."""

        pulls: list[dict[str, Any]] = []
        repo = self.p.config.repo
        for number in self.driver.changes_opened():
            status, pull = self.p.as_actor("GET", f"/repos/{repo}/pulls/{number}")
            pull = pull if status == 200 and isinstance(pull, dict) else {}
            created = self.fe._parse_time(pull.get("created_at"))
            body = pull.get("body") if isinstance(pull.get("body"), str) else ""
            pulls.append(
                {
                    "number": number,
                    "opened_s": None if created is None else self.rel(created.timestamp()),
                    "names_key": names_key(body, self.key),
                    "has_citation": has_citation(body),
                }
            )
        return pulls

    def await_answered_run_ending(self) -> bool:
        """At least two requests and none live, within ENDING_SECONDS of the pull request."""

        deadline = (self.pr_recorded_at or self.clock()) + ENDING_SECONDS
        while True:
            read_at = self.clock()
            detail, _comments, stop = self.poll()
            if stop:
                return False
            # An ending read after the deadline does not count.
            if (
                read_at <= deadline
                and detail is not None
                and len(requests_of(detail)) >= 2
                and not live_statuses(detail)
            ):
                self.obs["answered_run_ended"] = True
                self.note("the answered run ended")
                return True
            if read_at > deadline or self.clock() > deadline:
                self.obs["answered_run_ended"] = False
                self.note(f"the answered run did not end within {ENDING_SECONDS} s; not merging")
                return False
            self.sleep(POLL_SECONDS)

    def merge_and_await_notice(self) -> None:
        if not self.await_answered_run_ending():
            return
        # Recount after the run ended: a second pull request must not be merged past.
        self.obs["pull_requests"] = self._list_pull_requests()
        if len(self.obs["pull_requests"]) != 1:
            self.note("the pull requests are not exactly one after the run ended; not merging")
            return
        if self.poll()[2]:
            return
        merge = self.driver.merge(self.change)
        merged_at = self.clock()
        self.obs["merge_ok"] = isinstance(merge, Mapping) and merge.get("merged") is True
        self.obs["merged_s"] = self.rel(merged_at)
        self.note("merge attempted", merged=self.obs["merge_ok"])
        if not self.obs["merge_ok"]:
            return
        deadline = merged_at + MERGED_NOTICE_SECONDS
        while True:
            # The notice is recorded from the same read before the spend check,
            # so a stop on that read still keeps its usage in the evidence.
            detail, comments = self.p.work_item_detail(self.work_item_id), self.comments()
            for marker, text in comments:
                if not (marker and marker.startswith(STATUS_MARKER_PREFIX)):
                    continue
                if self.second_marker is not None and marker != self.second_marker:
                    continue
                if has_line(text, MERGED_LINE):
                    self.obs["merged_notice_s"] = self.rel()
                    kind, total = usage_total(text)
                    self.obs["usage"] = {
                        "kind": kind,
                        "total": None if total is None else str(total),
                    }
                    self.note("merged notice seen")
                    break
            if self.over_spend(detail, comments) or self.obs["merged_notice_s"] is not None:
                return
            if self.clock() > deadline:
                self.note("no merged notice within the bound")
                return
            self.sleep(POLL_SECONDS)

    def final_reads(self) -> None:
        """Read before main's teardown closes the issue (that moves its status to Done)."""

        if self.issue_id:
            status, issue = self.driver.issue_fields(self.issue_id)
            after = status_id(issue) if status == 200 else None
            self.obs["status_id_unchanged"] = (
                None if self.status_before is None or after is None else after == self.status_before
            )
        detail = self.p.work_item_detail(self.work_item_id) if self.work_item_id else None
        self.obs["request_count"] = None if detail is None else len(requests_of(detail))
        self.obs["factory_created_issues"] = self.driver.factory_created_issues(self.p.labelled_at)
        self.obs["model_usage_before"] = self.p.usage_before
        self.obs["model_usage_after"] = self.p.model_usage()


def drive(
    p: Any,
    *,
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Run the beats, judge them, log the anonymized summary, and finish the scenario."""

    import factory_e2e as fe

    add_run_identities(p)
    answer = str(p.scenario_options["answer_text"])
    run = _LiveRun(p, clock=clock, sleep=sleep)
    run.open_ticket()
    if (
        run.await_question()
        and run.settle_and_answer(answer)
        and run.await_second_request()
        and run.await_pull_request()
    ):
        run.merge_and_await_notice()
    run.final_reads()
    failures = judge(run.obs)
    run.obs["verdict"] = "passed" if not failures else "failed"
    identities = p.scenario_options.get("identities") or {}
    summary = outcome_summary({**p.evidence, "scenario": run.obs}, identities)
    p.evidence["outcome_summary"] = summary
    for line in summary.splitlines():
        fe.log(f"one-ticket summary: {line}")
    return dict(fe._finish(p, run.obs, failures))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
