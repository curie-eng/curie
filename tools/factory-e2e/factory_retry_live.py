"""Leg table, install values and pure judgements for the factory retry live driver.

``tests/test_factory_retry_live.py`` drives a candidate install through the
provider and transient failure classes of the factory's automatic retries
(ADR 0269; #5424, #5426, #5431). Everything here is pure, so each judgement is
exercised by ordinary tests with recorded row shapes
(``tests/test_factory_retry_live_support.py``): each one fails on the outcome
it exists to catch and passes on the shape a correct install leaves.

The model is the runner's offline fake tier. Each leg's agent carries one
standing ``FAKE_MODEL_DIRECTIVE`` (``agentSandbox.runnerEnv``), and a
``[fake:model-fault ...]`` directive makes every turn of that agent fail the
way the real provider refuses (``runner/src/curie_runner/fake.py``). A timed
fault names its reset as an absolute Unix time, so the driver rewrites the
agent's directive on its runner SandboxTemplate just before it seeds the leg's
issue (``directive_patch``), and again before a successor starts when the leg
needs the successor to end differently. Sandbox claims cold-create from the
template, so the rewrite reaches the next runner and no running one.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import candidate_install as ci
import tls_code_host as tch

FLAG = "CURIE_FACTORY_RETRY_LIVE"
NAMES = ci.InputNames(
    flag=FLAG,
    namespace="CURIE_FACTORY_RETRY_NAMESPACE",
    release="CURIE_FACTORY_RETRY_RELEASE",
    pull_policy="CURIE_FACTORY_RETRY_PULL_POLICY",
)

DIRECTIVE_ENV = "FAKE_MODEL_DIRECTIVE"
SEED_FILES = {
    "README.md": "# acme-bot\n\nFixture repository for the factory retry proof.\n",
    "example.py": 'def example() -> str:\n    return "example"\n',
}
SEED_ISSUE = "Factory retry proof fixture.\n"

# apps/api/src/curie_api/workitems/lifecycle.py AUTOMATIC_RETRY_LIMIT,
# AUTOMATIC_RETRY_DELAYS_S and AUTOMATIC_RETRY_JITTER; factory_retry_policy.RESET_GRACE.
AUTOMATIC_RETRY_LIMIT = 2
BACKOFF_S = (30.0, 120.0)
JITTER = 0.2
RESET_GRACE_S = 60.0
# Timestamps are stored to the microsecond; a second covers clock rounding.
TIME_SLACK_S = 1.0

NEEDS_HUMAN_LABEL = "curie-factory:needs-human"
# apps/api/src/curie_api/factory_notices.py result_section and _retrying_section.
RETRYING_PREFIX = "Retrying: "
NEEDS_HUMAN_PREFIX = "Could not complete: "

# The terminal causes each fault settles with (kernel/constants.py _ESCALATION_CAUSES).
CAUSE_RATE_LIMITED = "model_rate_limited"
CAUSE_CREDIT = "model_credit_exhausted"
CAUSE_CREDENTIAL = "model_credential_rejected"
CAUSE_UNREACHABLE = "model_unreachable"
CAUSE_OWNER_LOST = "owner_lost"
CANCEL_CAUSES = frozenset({"issue_cancelled"})


def rate_limit(reset: int) -> str:
    return f"[fake:model-fault rate-limit {int(reset)}]"


def daily_budget(reset: int) -> str:
    return f"[fake:model-fault daily-budget {int(reset)}]"


RATE_LIMIT_UNTIMED = "[fake:model-fault rate-limit-untimed]"
GENERIC_403 = "[fake:model-fault generic-403]"
UNREACHABLE = "[fake:model-fault unreachable]"
# A clean turn that holds its runner for 180 s (FILL_SCRATCH_SECONDS) with a
# small scratch file, so the worker can be lost while it runs.
HOLD = "[fake:fill-scratch 8]"
# What every agent's runner says until its leg sets its own directive: a
# terminal refusal that admits nothing.
INSTALLED_DIRECTIVE = GENERIC_403


# Run inside the stub Pod: close one issue the way a person does on GitHub. The
# stub's PATCH only flips the state; GitHub also records a ``closed`` issue
# event by that person, which is what tracker intake reads to cancel a running
# issue (forges/github/tracker.py ``_removal``). The actor is the stub's human,
# the author of its seed issue, as in candidate_install.SEED_ISSUE_PROGRAM.
CLOSE_ISSUE_PROGRAM = f"""
import json, sqlite3, sys
from datetime import UTC, datetime
number = int(sys.argv[1])
now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
connection = sqlite3.connect({tch.STUB_DATABASE!r}, timeout=30, isolation_level="IMMEDIATE")
try:
    connection.execute("UPDATE objects SET kind=kind WHERE 0")
    seed = json.loads(connection.execute(
        "SELECT body FROM objects WHERE kind='issue' AND id=?", ({tch.STUB_ISSUE},)
    ).fetchone()[0])
    issue = json.loads(connection.execute(
        "SELECT body FROM objects WHERE kind='issue' AND id=?", (number,)
    ).fetchone()[0])
    issue.update(state="closed", closed_at=now, updated_at=now)
    connection.execute(
        "UPDATE objects SET body=? WHERE kind='issue' AND id=?", (json.dumps(issue), number)
    )
    event = int(connection.execute(
        "SELECT COALESCE(MAX(id), 0) FROM objects WHERE kind='event'"
    ).fetchone()[0]) + 1
    row = {{
        "id": event, "issue_number": number, "event": "closed", "actor": seed["user"],
        "commit_id": None, "created_at": now, "performed_via_github_app": None,
    }}
    connection.execute(
        "INSERT INTO objects(kind, id, body) VALUES('event', ?, ?)", (event, json.dumps(row))
    )
    connection.commit()
finally:
    connection.close()
print(json.dumps({{"number": number, "event": event}}))
"""


@dataclass(frozen=True)
class Leg:
    key: str
    agent: str
    label: str
    title: str


def _leg(key: str, agent: str, title: str) -> Leg:
    return Leg(key=key, agent=agent, label=f"{agent}-work", title=f"Factory retry proof: {title}")


LEG_A_RATE = _leg("a-rate", "fr-a-rate", "timed rate limit")
LEG_A_BUDGET = _leg("a-budget", "fr-a-budget", "timed daily budget")
LEG_B = _leg("b", "fr-b-untimed", "untimed rate limit")
LEG_C = _leg("c", "fr-c-forbidden", "generic 403")
LEG_D = _leg("d", "fr-d-unreachable", "unreachable chain")
LEG_E = _leg("e", "fr-e-mixed", "owner loss then transient")
LEG_F = _leg("f", "fr-f-closed", "issue closed during the wait")
LEG_G = _leg("g", "fr-g-operator", "operator dry run")
LEGS = (LEG_A_RATE, LEG_A_BUDGET, LEG_B, LEG_C, LEG_D, LEG_E, LEG_F, LEG_G)


def template_name(release: str, agent: str) -> str:
    """The agent's runner SandboxTemplate (charts/curie/templates/agent-sandbox.yaml). Pure."""

    return f"{release}-agent-{agent}-runner"


def install_values(base: Mapping[str, Any], legs: Sequence[Leg] = LEGS) -> dict[str, Any]:
    """The candidate base values plus one runner template per leg agent. Pure."""

    values: dict[str, Any] = json.loads(json.dumps(base))
    agents = [leg.agent for leg in legs]
    sandbox = values["agentSandbox"]
    sandbox["runnerEnv"] = {agent: {DIRECTIVE_ENV: INSTALLED_DIRECTIVE} for agent in agents}
    sandbox["agentResourceProfiles"] = dict.fromkeys(agents, "standard")
    values["factory"]["sandboxEgress"] = dict.fromkeys(agents, True)
    return values


def directive_patch(template: Mapping[str, Any], directive: str) -> list[dict[str, Any]]:
    """A JSON patch setting the runner container's standing directive. Pure.

    The ``test`` operation pins the value it replaces, so a template another
    writer changed since the read refuses the patch instead of losing a write.
    """

    containers = (((template.get("spec") or {}).get("podTemplate") or {}).get("spec") or {}).get(
        "containers"
    ) or []
    for index, container in enumerate(containers):
        if container.get("name") != "runner":
            continue
        for position, entry in enumerate(container.get("env") or []):
            if entry.get("name") == DIRECTIVE_ENV and "value" in entry:
                path = f"/spec/podTemplate/spec/containers/{index}/env/{position}/value"
                return [
                    {"op": "test", "path": path, "value": entry["value"]},
                    {"op": "replace", "path": path, "value": directive},
                ]
        raise ValueError(f"the runner container carries no literal {DIRECTIVE_ENV}")
    raise ValueError("the SandboxTemplate has no runner container")


def standing_directive(template: Mapping[str, Any]) -> str | None:
    """The runner container's standing directive, or None. Pure."""

    for container in (
        ((template.get("spec") or {}).get("podTemplate") or {}).get("spec") or {}
    ).get("containers") or []:
        if container.get("name") == "runner":
            for entry in container.get("env") or []:
                if entry.get("name") == DIRECTIVE_ENV:
                    value = entry.get("value")
                    return value if isinstance(value, str) else None
    return None


# --------------------------------------------------------------------------
# Row facts
# --------------------------------------------------------------------------


def when(value: Any) -> datetime | None:
    """A stored timestamp from ``row_to_json``, or None. Pure."""

    if not isinstance(value, str) or not value:
        return None
    return datetime.fromisoformat(value)


def seconds_between(earlier: Any, later: Any) -> float | None:
    start, end = when(earlier), when(later)
    if start is None or end is None:
        return None
    return (end - start).total_seconds()


def epoch(value: Any) -> float | None:
    moment = when(value)
    return None if moment is None else moment.timestamp()


BRIEF_KEYS = (
    "id",
    "sequence",
    "status",
    "terminal_cause",
    "retry_safety",
    "retry_condition_reason",
    "retry_condition_retry_at",
    "retry_condition_scope_key",
    "retry_of_request_id",
    "retry_origin_request_id",
    "owner_lost_retry",
    "blocked_on",
    "created_at",
    "wait_deadline",
    "dispatch_not_before",
    "dispatch_generation",
    "started_at",
    "execution_deadline",
    "terminal_at",
    "execution_attempts",
    "runtime_epoch",
    "runtime_claim_name",
    "version",
)


def brief(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """The row's retry facts only: ids, states, causes and times. Pure."""

    if row is None:
        return None
    return {key: row.get(key) for key in BRIEF_KEYS}


def backoff_window(work_slot: int) -> tuple[float, float]:
    """The jittered backoff before automatic retry ``work_slot`` (1 or 2). Pure."""

    base = BACKOFF_S[work_slot - 1]
    return base * (1 - JITTER) - TIME_SLACK_S, base * (1 + JITTER) + TIME_SLACK_S


def carrier_problems(
    row: Mapping[str, Any],
    *,
    cause: str,
    reason: str | None,
    retry_at: float | None,
) -> list[str]:
    """What is wrong with a failed row's typed retry carrier. Pure.

    The carrier is what the worker's finish reported and the API persisted:
    the cause, the ``no_effects`` safety fact and the provider condition with
    its reset time. Fake mode records no provider scope key.
    """

    problems: list[str] = []
    if row.get("status") != "failed":
        problems.append(f"status {row.get('status')!r}, not failed")
    if row.get("terminal_cause") != cause:
        problems.append(f"cause {row.get('terminal_cause')!r}, not {cause!r}")
    if row.get("retry_safety") != "no_effects":
        problems.append(f"retry safety {row.get('retry_safety')!r}, not 'no_effects'")
    if row.get("retry_condition_reason") != reason:
        problems.append(f"condition reason {row.get('retry_condition_reason')!r}, not {reason!r}")
    stored = epoch(row.get("retry_condition_retry_at"))
    if retry_at is None:
        if stored is not None:
            problems.append(
                f"condition reset {row.get('retry_condition_retry_at')} on an untimed fault"
            )
    elif stored is None or abs(stored - retry_at) > TIME_SLACK_S:
        problems.append(
            f"condition reset {row.get('retry_condition_retry_at')}, not the directive's {retry_at}"
        )
    if row.get("retry_condition_scope_key") is not None:
        problems.append("a provider scope key on a fake-mode failure")
    return problems


def successor_problems(
    predecessor: Mapping[str, Any],
    successor: Mapping[str, Any],
    *,
    origin: Mapping[str, Any],
    owner_loss: bool = False,
) -> list[str]:
    """What is wrong with a successor's identity and lineage. Pure."""

    problems: list[str] = []
    if successor.get("id") == predecessor.get("id"):
        problems.append("the successor reuses its predecessor's id")
    sequence, previous = successor.get("sequence"), predecessor.get("sequence")
    if not (isinstance(sequence, int) and isinstance(previous, int) and sequence > previous):
        problems.append(f"sequence {sequence} does not follow {previous}")
    if successor.get("retry_of_request_id") != predecessor.get("id"):
        problems.append("retry_of_request_id does not name the predecessor")
    if successor.get("retry_origin_request_id") != origin.get("id"):
        problems.append("retry_origin_request_id does not name the original request")
    if bool(successor.get("owner_lost_retry")) is not owner_loss:
        problems.append(f"owner_lost_retry {successor.get('owner_lost_retry')!r}, not {owner_loss}")
    # The settling transaction admits the successor before it stamps the
    # predecessor's terminal time, so the two differ by milliseconds either way.
    created, settled = epoch(successor.get("created_at")), epoch(predecessor.get("terminal_at"))
    if created is None or settled is None or abs(created - settled) > TIME_SLACK_S:
        problems.append("the successor was not admitted when its predecessor settled")
    previous_wait = when(predecessor.get("wait_deadline"))
    wait = when(successor.get("wait_deadline"))
    if wait is None or previous_wait is None or wait <= previous_wait:
        problems.append("the successor's waiting deadline is not fresh")
    return problems


def scheduled_problems(
    predecessor: Mapping[str, Any],
    successor: Mapping[str, Any],
    *,
    due: float | None,
    work_slot: int,
) -> list[str]:
    """What is wrong with a scheduled successor's due time. Pure.

    ``due`` is the provider reset plus the grace when the fault was timed;
    otherwise the jittered backoff for ``work_slot`` after the predecessor
    settled applies.
    """

    problems: list[str] = []
    if successor.get("blocked_on") is not None:
        problems.append(f"blocked on {successor.get('blocked_on')!r}, not scheduled")
    not_before = epoch(successor.get("dispatch_not_before"))
    if not_before is None:
        return [*problems, "no dispatch_not_before"]
    if due is not None:
        if abs(not_before - due) > TIME_SLACK_S:
            problems.append(f"due at {not_before}, not the reset plus grace {due}")
        return problems
    settled = epoch(predecessor.get("terminal_at"))
    created = epoch(successor.get("created_at"))
    anchor = created if created is not None else settled
    if anchor is None:
        return [*problems, "no admission time to measure the backoff from"]
    low, high = backoff_window(work_slot)
    gap = not_before - anchor
    if not low <= gap <= high:
        problems.append(f"backoff {gap:.1f}s outside retry {work_slot}'s {low:.1f}s to {high:.1f}s")
    return problems


def started_once_problems(row: Mapping[str, Any]) -> list[str]:
    """What is wrong with a successor that must have started exactly once, not early. Pure."""

    problems: list[str] = []
    started, not_before = epoch(row.get("started_at")), epoch(row.get("dispatch_not_before"))
    if started is None:
        return ["never started"]
    if not_before is not None and started + TIME_SLACK_S < not_before:
        problems.append(f"started at {started}, before its due time {not_before}")
    if row.get("execution_attempts") != 1:
        problems.append(f"execution_attempts {row.get('execution_attempts')!r}, not 1")
    deadline = epoch(row.get("execution_deadline"))
    if deadline is None or deadline <= started:
        problems.append("no execution deadline after its own start")
    if not row.get("runtime_claim_name"):
        problems.append("no sandbox claim recorded")
    return problems


def fresh_attempt_problems(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    """Every started attempt has its own identity, claim and deadlines. Pure."""

    problems: list[str] = []
    ids = [row.get("id") for row in rows]
    if len(set(ids)) != len(ids):
        problems.append("two attempts share a request id")
    claims = [row.get("runtime_claim_name") for row in rows if row.get("started_at")]
    if len(set(claims)) != len(claims):
        problems.append("two attempts share a sandbox claim")
    deadlines = [row.get("execution_deadline") for row in rows if row.get("started_at")]
    if len(set(deadlines)) != len(deadlines):
        problems.append("two attempts share an execution deadline")
    waits = [epoch(row.get("wait_deadline")) for row in rows]
    known = [w for w in waits if w is not None]
    if len(known) != len(waits) or any(
        later <= earlier for earlier, later in zip(known, known[1:], strict=False)
    ):
        problems.append("waiting deadlines are not each fresh")
    return problems


def unstarted_problems(row: Mapping[str, Any]) -> list[str]:
    """What shows a waiting or cancelled successor ran anyway. Pure."""

    problems: list[str] = []
    if row.get("started_at") or row.get("execution_attempts"):
        problems.append("started")
    if row.get("runtime_claim_name"):
        problems.append(f"holds sandbox claim {row.get('runtime_claim_name')}")
    return problems


@dataclass(frozen=True)
class Notices:
    retrying: int
    needs_human: int
    other: int
    bodies: tuple[str, ...]


def notices(comments: Sequence[Mapping[str, Any]]) -> Notices:
    """Count the factory status comments by the result each shows. Pure."""

    retrying = needs_human = other = 0
    bodies: list[str] = []
    for comment in comments:
        body = str(comment.get("body") or "")
        if "Cause: " not in body and RETRYING_PREFIX not in body and NEEDS_HUMAN_PREFIX not in body:
            other += 1
            continue
        bodies.append(body)
        if RETRYING_PREFIX in body:
            retrying += 1
        elif NEEDS_HUMAN_PREFIX in body:
            needs_human += 1
        else:
            other += 1
    return Notices(retrying, needs_human, other, tuple(bodies))


def notice_problems(
    found: Notices, *, retrying: int, needs_human: int, used: int | None = None
) -> list[str]:
    """What is wrong with the issue's result comments. Pure.

    ``used`` is the allowance the needs-human comment must name, when one does.
    """

    problems: list[str] = []
    if found.retrying != retrying:
        problems.append(f"{found.retrying} retrying notices, expected {retrying}")
    if found.needs_human != needs_human:
        problems.append(f"{found.needs_human} needs-human notices, expected {needs_human}")
    if used is not None:
        sentence = f"{used} of {AUTOMATIC_RETRY_LIMIT} automatic retries used."
        if not any(NEEDS_HUMAN_PREFIX in body and sentence in body for body in found.bodies):
            problems.append(f"no needs-human notice says {sentence!r}")
    return problems


# --------------------------------------------------------------------------
# The compiled CLI
# --------------------------------------------------------------------------

_REFUSAL = re.compile(r"work item retry refused \(([a-z_]+)\)")


@dataclass(frozen=True)
class CliResult:
    code: int
    stdout: str
    stderr: str

    def payload(self) -> dict[str, Any] | None:
        """The one JSON object ``--json`` printed, from stdout first. Pure."""

        for stream in (self.stdout, self.stderr):
            for line in reversed(stream.strip().splitlines()):
                line = line.strip()
                if line.startswith("{"):
                    try:
                        value = json.loads(line)
                    except ValueError:
                        continue
                    if isinstance(value, dict):
                        return value
            try:
                whole = json.loads(stream)
            except ValueError:
                continue
            if isinstance(whole, dict):
                return whole
        return None


def refusal_problems(result: CliResult, code: str) -> list[str]:
    """What is wrong with a refusal the CLI must report with ``code``. Pure."""

    problems: list[str] = []
    if result.code != 1:
        problems.append(f"exit {result.code}, not 1 (failure)")
    payload = result.payload()
    if payload is None:
        return [*problems, "no JSON object on stdout or stderr"]
    match = _REFUSAL.search(str(payload.get("error") or ""))
    if match is None or match.group(1) != code:
        problems.append(f"error {payload.get('error')!r} does not name refusal {code!r}")
    if not payload.get("fix"):
        problems.append("the refusal names no fix")
    return problems


def dry_run_problems(
    result: CliResult,
    *,
    work_item_id: str,
    predecessor_id: str,
    cause: str,
    due: float,
) -> list[str]:
    """What is wrong with the operator dry run's plan. Pure."""

    problems: list[str] = []
    if result.code != 0:
        problems.append(f"exit {result.code}, not 0")
    plan = result.payload()
    if plan is None:
        return [*problems, "no JSON object on stdout"]
    expected = {
        "work_item_id": work_item_id,
        "dry_run": True,
        "replayed": False,
        "disposition": "implementation",
        "predecessor_request_id": predecessor_id,
        "predecessor_cause": cause,
        "successor_request_id": None,
        "successor_blocked_on": None,
        "recorded_at": None,
        "retries_used": 1,
        "retry_limit": AUTOMATIC_RETRY_LIMIT,
    }
    for key, value in expected.items():
        if plan.get(key) != value:
            problems.append(f"{key} {plan.get(key)!r}, expected {value!r}")
    not_before = epoch(plan.get("successor_dispatch_not_before"))
    if not_before is None or abs(not_before - due) > TIME_SLACK_S:
        problems.append(
            f"planned start {plan.get('successor_dispatch_not_before')}, not the reset plus grace"
        )
    return problems


def unchanged_problems(
    before: Mapping[str, Any], after: Mapping[str, Any], rows_before: int, rows_after: int
) -> list[str]:
    """What a refusal or dry run wrote to the work item. Pure."""

    problems: list[str] = []
    for key in ("version", "automatic_retries_used", "cancelled_at", "readmit_request_id"):
        if before.get(key) != after.get(key):
            problems.append(f"work item {key} moved from {before.get(key)!r} to {after.get(key)!r}")
    if rows_before != rows_after:
        problems.append(f"requests went from {rows_before} to {rows_after}")
    return problems
