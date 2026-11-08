"""Scripted scenarios for `curie dev identity-e2e -- prove --scenario <name>`.

A scenario is a function registered with ``@scenario("<name>")``. It receives
the loaded :class:`identity_e2e.Harness`, the parsed ``prove`` arguments and an
:class:`Evidence` recorder, and runs its steps inside ``evidence.step(...)``
blocks. Every assertion is a :func:`check` that raises :class:`StepFailed`;
a failed step is recorded and the scenario continues, so one run reports every
step, unless the scenario stops itself with :func:`require_passed` because its
later steps build on that one (the run then records ``aborted``). ``prove``
exits nonzero unless every recorded step passed.

Each step drives the install through the same boundaries an operator or a
mail adapter uses (``Harness.send``, ``Harness.observe``, the platform API and
the ``curie`` CLI), and reads what the product logged (the caller proxy's
decisions through ``Harness.proxy_decisions``). Direct SQL and the worker
route record rewrite (``Harness.route_doctor``) are reserved for deliberately
corrupting stored state that no product surface can write, and every such
change is restored in a ``finally`` block.
"""

from __future__ import annotations

import contextlib
import json
import re
import sys
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any

import approver_ticket_driver as tickets
from identity_assets import ed25519_pair
from identity_e2e import Api, Harness, HarnessError, epoch, turn_summary
from identity_target import REPO, http, write_private

# The fake model's test-only directive: the turn dials every hosted connector
# of the agent through the runner's caller header path and answers one line
# per connector (runner/src/curie_runner/fake.py).
CONNECTOR_CALL = "[fake:connector-call]"
# The fake model's long turn: about 8 s of scripted pauses, then `all done`.
PROGRESS_DEMO = "[fake:progress-demo]"
PROGRESS_DEMO_ANSWER = "all done"
# Worker log text: the kernel's deferral of a different human's delivery while
# a turn is live, and the reclaim preflight interrupting a live turn.
PENDING_MESSAGE = "requester handoff pending"
INTERRUPT_MESSAGE = "interrupting before retry"
# The consumer's durable wait for a deferred different-human delivery
# (capacity_wait): `capacity wait parked event_id=<id> cause=requester_handoff`.
HANDOFF_PARK_CAUSE = "requester_handoff"
_PARKED = re.compile(r"\bcapacity wait parked event_id=(?P<event_id>\S+) cause=(?P<cause>\S+)")
DEFERRAL_NOT_EXERCISED = "deferral not exercised"
STEERING_NOT_EXERCISED = "steering not exercised"
# Between the host, the API server's claim stamps and the reply sink's clock.
CLOCK_SKEW_S = 2.0
# A deferred delivery parks in a durable wait whose due wake (about 5 s) runs
# it once the other human's turn ends, so it no longer waits for a 45 s
# redelivery. The bound stays generous: it only caps a failing run.
HANDOFF_WAIT_S = 360.0
SECOND_SEND_DELAY_S = 1.0
LOG_MESSAGE_LIMIT = 300
_CONNECTOR_CALL_LINE = re.compile(
    r"\bconnector-call(?: (?P<name>[^\s:]+))?: (?P<outcome>ok|failed|none)\b(?P<rest>[^\n]*)"
)
_TOOLS = re.compile(r"(?:^|\s)tools=(?P<tools>\d+)(?:\s|$)")
_CALLER = re.compile(r"(?:^|\s)caller=(?P<caller>\S+)")

ScenarioFunc = Callable[[Harness, Any, "Evidence"], None]
SCENARIOS: dict[str, ScenarioFunc] = {}


def scenario(name: str) -> Callable[[ScenarioFunc], ScenarioFunc]:
    def register(func: ScenarioFunc) -> ScenarioFunc:
        if name in SCENARIOS:
            raise ValueError(f"scenario {name} is registered twice")
        SCENARIOS[name] = func
        return func

    return register


class StepFailed(AssertionError):
    """One failed assertion inside a step."""


def check(condition: bool, message: str) -> None:
    if not condition:
        raise StepFailed(message)


class Evidence:
    """The JSON evidence one ``prove`` run writes."""

    def __init__(self, name: str) -> None:
        self.data: dict[str, Any] = {
            "scenario": name,
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "steps": [],
        }

    @contextlib.contextmanager
    def step(self, step_id: str, title: str) -> Iterator[dict[str, Any]]:
        observed: dict[str, Any] = {}
        entry: dict[str, Any] = {"id": step_id, "title": title, "observed": observed}
        started = time.monotonic()
        try:
            yield observed
            entry["passed"] = True
        except StepFailed as exc:
            entry.update(passed=False, failure=str(exc))
        except HarnessError as exc:
            entry.update(passed=False, failure=f"harness: {exc}")
        finally:
            entry["seconds"] = round(time.monotonic() - started, 1)
            self.data["steps"].append(entry)

    def finish(self) -> dict[str, Any]:
        steps = self.data["steps"]
        self.data["passed"] = (
            bool(steps) and "aborted" not in self.data and all(step["passed"] for step in steps)
        )
        self.data["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return self.data


def run(name: str, harness: Harness, args: Any) -> dict[str, Any]:
    """Run one registered scenario and return its finished evidence."""

    if name not in SCENARIOS:
        raise HarnessError(f"unknown scenario {name}; known: {', '.join(sorted(SCENARIOS))}")
    evidence = Evidence(name)
    try:
        SCENARIOS[name](harness, args, evidence)
    except (StepFailed, HarnessError) as exc:
        evidence.data["aborted"] = str(exc)
    return evidence.finish()


# --- shared helpers -----------------------------------------------------------------


def default_agents(
    data: Mapping[str, Any], flag: str | None, control: str | None
) -> tuple[str | None, str | None]:
    """``--flag-agent`` and ``--control-agent``, defaulting to the first two `up` registered.

    The state file sorts its keys, so the registration order comes from its
    ``agent_order`` list, not from the order of ``agents``.
    """

    order = [name for name in data.get("agent_order") or () if name in data.get("agents", {})]
    if flag is None and order:
        flag = order[0]
    if control is None:
        control = next((name for name in order if name != flag), None)
    return flag, control


def two_agents(harness: Harness, args: Any) -> tuple[str, str]:
    flag, control = default_agents(harness.data, args.flag_agent, args.control_agent)
    if flag is None or control is None or flag == control:
        raise HarnessError(
            "this scenario needs two distinct agents; run `up --agent A --agent B` (which "
            "records their order) or pass --flag-agent and --control-agent"
        )
    for name in (flag, control):
        harness.agent_entry(name)
    return flag, control


def roster_subject(harness: Harness, sender: str) -> str:
    entry = harness.data.get("roster", {}).get("senders", {}).get(sender.lower())
    if (
        entry is None
        or entry.get("sender_kind", "human") != "human"
        or entry.get("account_status", "active") != "active"
        or entry.get("outcome") not in (None, "resolved")
    ):
        raise HarnessError(f"{sender} is not a verified human in the roster `up` installed")
    return str(entry["subject"])


def principal_for(harness: Harness, sender: str) -> dict[str, Any]:
    """The canonical principal a verified roster human's caller token carries."""

    return {
        "issuer": harness.data["issuer"],
        "subject": roster_subject(harness, sender),
        "tenant_id": harness.data["tenant_id"],
    }


def requester_flag(api: Api, agent_id: str) -> tuple[Any, Any]:
    """The flag and its version as AgentOut reports them."""

    out = api.ok("GET", f"/agents/{agent_id}")
    check(
        "connector_requester_identity" in out and "connector_requester_identity_version" in out,
        "AgentOut has no connector_requester_identity fields; is the candidate installed?",
    )
    return out["connector_requester_identity"], out["connector_requester_identity_version"]


def set_requester_flag(api: Api, agent_id: str, wanted: bool, *, what: str) -> tuple[Any, Any]:
    """Write the flag to ``wanted`` at its current version when it differs; the result."""

    flag, version = requester_flag(api, agent_id)
    if bool(flag) != wanted:
        status, body = api.call(
            "PATCH",
            f"/agents/{agent_id}",
            {
                "connector_requester_identity": wanted,
                "connector_requester_identity_expected_version": version,
            },
        )
        check(status == 200, f"{what} answered {status}: {body}")
        flag, version = requester_flag(api, agent_id)
    return flag, version


def require_install_ready(harness: Harness, seen: dict[str, Any], agents: Sequence[str]) -> None:
    """The tenant has its HTTP selection; every agent has a registered binding and a connector."""

    rows = harness.target.sql(
        (
            "SELECT http_selection IS NOT NULL AS selected FROM curie.tenant_identity_providers"
            " WHERE tenant_id::text = :t",
            {"t": harness.data["tenant_id"]},
        )
    )[0]
    seen["tenant_http_selection"] = bool(rows and rows[0]["selected"])
    check(seen["tenant_http_selection"], "the tenant has no HTTP selection; rerun `up`")
    registered = set(harness.data.get("registration", {}).get("binding_ids", ()))
    for name in agents:
        check(
            harness.agent_entry(name)["binding_id"] in registered,
            f"{name}'s email binding is not in the receiver registration",
        )
    hosted = {name: harness.connectors(name) for name in agents}
    seen["connectors"] = hosted
    for name, connectors in hosted.items():
        check(
            bool(connectors),
            f"{name} has no hosted connector, so no turn can present its token to one; "
            "deploy the agents with `up --bundle examples/weather`",
        )


def cli_requester_identity(
    harness: Harness, agent: str, mode: str, version: int | None = None
) -> dict[str, Any]:
    argv = ["cluster", "approvals", agent, "--requester-identity", mode, "--json"]
    if version is not None:
        argv += ["--expected-version", str(version)]
    result = harness.target.curie(*argv)
    tail = (result.stderr or result.stdout).strip().splitlines()[-3:]
    check(result.returncode == 0, f"curie {' '.join(argv[:5])} exited {result.returncode}: {tail}")
    try:
        out = json.loads(result.stdout)
    except ValueError:
        raise StepFailed(f"curie {' '.join(argv[:5])} printed no JSON object") from None
    check(
        set(out)
        >= {
            "agent",
            "agent_id",
            "connector_requester_identity",
            "connector_requester_identity_version",
        },
        f"the CLI JSON lacks the requester identity fields: {sorted(out)}",
    )
    return dict(out)


def send_summary(sent: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: sent.get(key)
        for key in ("sender", "delivery_id", "conversation_id", "http_status", "response")
    }


def observed_token(seen: Mapping[str, Any]) -> dict[str, Any]:
    """The non-secret part of an observation, for evidence."""

    keep = (
        "claim",
        "claim_created",
        "claim_created_epoch",
        "caller_token_present",
        "prefix",
        "claims",
        "signature",
        "caller_fingerprint",
        "turn",
    )
    return {key: seen.get(key) for key in keep}


def require_delivered(turn: Mapping[str, Any]) -> None:
    """The turn completed, the worker delivered its reply, and the reply has text."""

    outcome = turn.get("outcome")
    check(
        bool(turn.get("completed")),
        f"the turn never completed (no turn.completed reply event; outcome {outcome!r})",
    )
    check(outcome == "delivered", f"the turn completed with outcome {outcome!r}, not delivered")
    texts = turn.get("texts") or []
    check(
        any(isinstance(text, str) and text.strip() for text in texts),
        "the turn was delivered with no reply text",
    )


def connector_calls(texts: Iterable[Any]) -> list[dict[str, Any]]:
    """The ``connector-call`` lines in reply texts, parsed.

    Each is ``{"name", "outcome", "tools", "reason", "caller"}``; ``name`` is
    None for ``connector-call: none`` and ``caller`` is the fingerprint the
    line carries, or None.
    """

    calls = []
    for text in texts:
        if not isinstance(text, str):
            continue
        for match in _CONNECTOR_CALL_LINE.finditer(text):
            rest = match["rest"]
            tools = _TOOLS.search(rest)
            caller = _CALLER.search(rest)
            words = rest.split()
            calls.append(
                {
                    "name": match["name"],
                    "outcome": match["outcome"],
                    "tools": None if tools is None else int(tools["tools"]),
                    "reason": words[0] if match["outcome"] == "failed" and words else None,
                    "caller": None if caller is None else caller["caller"],
                }
            )
    return calls


def require_connector_admitted(
    texts: Iterable[Any],
    decisions: Sequence[Mapping[str, Any]],
    *,
    agent: str,
    connectors: Sequence[str],
    principal: bool,
) -> dict[str, Any]:
    """The reply dialled every connector and the caller proxy admitted ``agent``.

    Requires a ``connector-call <c>: ok`` line for every connector and no
    failed line; at least one admitted decision for ``agent`` (one per
    connector when the decisions name their connector); no refusal for
    ``agent``; and every admitted decision for ``agent`` logging
    ``principal=present`` when ``principal`` is true, else
    ``principal=absent``. Refusals for other callers are counted, not failed.
    Returns a JSON summary carrying no token.
    """

    expected = "present" if principal else "absent"
    check(bool(connectors), f"{agent} has no hosted connector to call")
    calls = connector_calls(texts)
    shown = [
        f"{call['name'] or '-'}: {call['outcome']}"
        + (f" {call['reason']}" if call["reason"] else "")
        for call in calls
    ]
    failed = [call for call in calls if call["outcome"] == "failed"]
    check(not failed, f"the reply reports a failed connector call: {shown}")
    called = {call["name"] for call in calls if call["outcome"] == "ok"}
    missing = [name for name in connectors if name not in called]
    check(
        not missing,
        f"the reply has no `connector-call <c>: ok` line for {missing}; its lines: {shown}",
    )

    mine = [dict(item) for item in decisions if item.get("caller") == agent]
    refused = [item for item in mine if item.get("outcome") != "admitted"]
    check(not refused, f"the caller proxy refused {agent}: {refused}")
    admitted = [item for item in mine if item.get("outcome") == "admitted"]
    check(bool(admitted), f"the caller proxy logged no admitted request from {agent}")
    if any("connector" in item for item in decisions):
        unseen = [
            name for name in connectors if not any(i.get("connector") == name for i in admitted)
        ]
        check(not unseen, f"no caller proxy admitted {agent} on connector(s) {unseen}")
    logged = sorted({str(item.get("principal")) for item in admitted})
    check(
        all(item.get("principal") == expected for item in admitted),
        f"every admitted request from {agent} should log principal={expected}; the proxy "
        f"logged principal {logged} (None means no principal field: is the candidate "
        "proxy deployed?)",
    )
    others = [item for item in decisions if item.get("caller") != agent]
    return {
        "agent": agent,
        "connectors": list(connectors),
        "calls": calls,
        "unlisted_calls": sorted(
            str(name) for name in called if name is not None and name not in connectors
        ),
        "expected_principal": expected,
        "admitted": len(admitted),
        "admitted_by_connector": {
            name: sum(1 for item in admitted if item.get("connector") == name)
            for name in connectors
        },
        "other_refusals": len(others),
    }


def require_caller_fingerprint(texts: Iterable[Any], fingerprint: str) -> list[str]:
    """Every ``ok`` connector call sent the observed token (by its fingerprint)."""

    ok = [call for call in connector_calls(texts) if call["outcome"] == "ok"]
    check(bool(ok), "the reply has no `connector-call <c>: ok` line to tie to the token")
    wrong = [f"{call['name']}: caller={call['caller']}" for call in ok]
    check(
        all(call["caller"] == fingerprint for call in ok),
        f"a connector call sent another caller token than the claim's ({fingerprint}): {wrong}",
    )
    return [str(call["name"]) for call in ok]


def require_principal(observed: Mapping[str, Any], principal: Any) -> dict[str, Any]:
    """The observed claim's caller token names exactly ``principal``."""

    claims = observed.get("claims")
    actual = claims.get("principal") if isinstance(claims, Mapping) else None
    check(actual is not None, f"the token carries no principal, expected {principal}")
    check(actual == principal, f"the token principal is {actual}, expected {principal}")
    return {"principal": actual}


def require_connector_turn(
    harness: Harness,
    seen: dict[str, Any],
    *,
    agent: str,
    observed: Mapping[str, Any],
    turn: Mapping[str, Any],
    since: float,
    expected_principal: Any,
    after: float | None = None,
) -> None:
    """One human turn's claim, reply and connector decisions, judged.

    The claim's caller token names ``agent`` and carries ``expected_principal``
    (none when it is None); the turn completed with a delivered reply; and the
    reply dialled every hosted connector, which admitted the agent with the
    principal present or absent to match, in the proxy log since ``since``.
    ``after`` drops decisions logged at or before it: a harness request the
    step made itself just before the send, inside the clock-skew margin.
    """

    check(bool(observed.get("caller_token_present")), "no connector caller token reached the claim")
    claims = observed.get("claims") or {}
    check(claims.get("agent") == agent, f"the token names agent {claims.get('agent')}")
    if expected_principal is None:
        check("principal" not in claims, f"the token carries {claims.get('principal')}")
    else:
        seen["principal"] = require_principal(observed, expected_principal)
    require_delivered(turn)
    connectors = harness.connectors(agent)
    seen["connectors"] = connectors
    decisions = harness.proxy_decisions(agent, since=since - 2)
    if after is not None:
        decisions = [item for item in decisions if float(item.get("at_epoch") or 0) > after]
    seen["decisions"] = decisions
    seen["connector_call"] = require_connector_admitted(
        turn.get("texts") or [],
        decisions,
        agent=agent,
        connectors=connectors,
        principal=expected_principal is not None,
    )


def human_turn_step(
    harness: Harness,
    evidence: Evidence,
    *,
    step_id: str,
    title: str,
    agent: str,
    sender: str,
    text: str,
    expected_principal: Any,
    wait: float,
) -> None:
    """One verified human's email turn to ``agent``, judged end to end.

    The send is admitted; the claim's caller token names ``agent`` and carries
    ``expected_principal`` (none when it is None); the turn completes with a
    delivered reply; the reply's ``connector-call`` lines are ok for every
    hosted connector and sent that token; and the caller proxy admitted the
    agent with the principal present or absent to match.
    """

    with evidence.step(step_id, title) as seen:
        started = time.time()
        sent = harness.send(agent, sender, text)
        seen["send"] = send_summary(sent)
        check(sent.get("http_status") == 200, f"the email turn answered {sent.get('http_status')}")
        observed = harness.observe(agent, wait=wait)
        seen["observe"] = observed_token(observed)
        turn = observed.get("turn") or {}
        require_connector_turn(
            harness,
            seen,
            agent=agent,
            observed=observed,
            turn=turn,
            since=started,
            expected_principal=expected_principal,
        )
        fingerprint = observed.get("caller_fingerprint")
        if fingerprint is not None:
            seen["caller_fingerprint_matched"] = require_caller_fingerprint(
                turn.get("texts") or [], str(fingerprint)
            )


def wait_for_refusal(
    harness: Harness, agent: str, sent_at: float, conversation: str, window: float
) -> dict[str, Any]:
    """Claims and reply events for one turn, until it settles or ``window`` ends."""

    deadline = time.monotonic() + window
    while True:
        claims = harness.claims(agent, sent_at - 2)
        replies = harness.replies(agent, sent_at - 2, conversation)
        summary = turn_summary(replies)
        if claims or summary["completed"] or time.monotonic() > deadline:
            return {"claims": [claim["name"] for claim in claims], "turn": summary}
        time.sleep(3)


def worker_messages(harness: Harness, since: float, needle: str) -> list[str]:
    """Worker log messages containing lowercase ``needle`` since ``since`` (unix seconds)."""

    return [
        entry["message"][:LOG_MESSAGE_LIMIT]
        for entry in harness.worker_log(since)
        if needle in entry["message"].lower()
    ]


# --- requester-claim ------------------------------------------------------------------


@scenario("requester-claim")
def requester_claim(harness: Harness, args: Any, evidence: Evidence) -> None:
    """ADR 0270 slice A: the requester principal in the connector caller token.

    Order: preconditions, (4) flag writes (leaving the flag on for the flag
    agent), (1) a verified human's email turn to the flag agent carries the
    principal, (2) the same turn to the control agent carries none and still
    completes, (3) `curie cluster message` to the flag agent carries none, (5)
    a corrupted stored flag refuses the next human turn before any claim.

    Steps 1 to 3 send ``[fake:connector-call]``, so each turn also presents its
    token to every hosted connector of the agent, and the caller proxy's own
    decision log shows whether it admitted a token with a principal.
    """

    flag_agent, control_agent = two_agents(harness, args)
    flag_id = harness.agent_entry(flag_agent)["agent_id"]
    control_id = harness.agent_entry(control_agent)["agent_id"]
    sender = args.sender.lower()
    expected_principal = principal_for(harness, sender)
    evidence.data.update(
        flag_agent=flag_agent,
        control_agent=control_agent,
        sender=sender,
        expected_principal=expected_principal,
    )
    enabled_version: int | None = None

    with (
        evidence.step(
            "0", "preconditions: HTTP selection, registered bindings, hosted connectors"
        ) as seen,
        harness.api() as api,
    ):
        require_install_ready(harness, seen, (flag_agent, control_agent))
        control_flag, control_version = set_requester_flag(
            api, control_id, False, what="disabling the control agent"
        )
        seen["control_agent"] = {"flag": control_flag, "version": control_version}
        check(control_flag is False, "the control agent's flag is not off")

    with (
        evidence.step("4", "flag writes are versioned and apart from the access policy") as seen,
        harness.api() as api,
    ):
        flag, version = requester_flag(api, flag_id)
        seen["initial"] = {"flag": flag, "version": version}
        if flag:
            status, body = api.call(
                "PATCH",
                f"/agents/{flag_id}",
                {
                    "connector_requester_identity": False,
                    "connector_requester_identity_expected_version": version,
                },
            )
            check(status == 200, f"resetting the flag off answered {status}: {body}")
            flag, version = requester_flag(api, flag_id)
        check(flag is False and isinstance(version, int), "could not start from the flag off")

        status, body = api.call(
            "PATCH", f"/agents/{flag_id}", {"connector_requester_identity": True}
        )
        seen["enable_without_expected_version"] = {"status": status}
        check(status == 422, f"enabling without an expected version answered {status}")
        check(
            requester_flag(api, flag_id) == (False, version),
            "a refused write changed the flag or its version",
        )

        stale = version - 1 if version > 1 else version + 7
        status, body = api.call(
            "PATCH",
            f"/agents/{flag_id}",
            {
                "connector_requester_identity": True,
                "connector_requester_identity_expected_version": stale,
            },
        )
        seen["enable_with_stale_version"] = {"status": status, "expected_version": stale}
        check(status == 409, f"a stale expected version answered {status}")
        check(
            "agent.requester_identity_version_conflict" in json.dumps(body),
            f"the 409 does not carry agent.requester_identity_version_conflict: {body}",
        )
        check(
            requester_flag(api, flag_id) == (False, version),
            "a stale write changed the flag or its version",
        )

        enabled = cli_requester_identity(harness, flag_agent, "on", version)
        seen["cli_enable"] = enabled
        check(
            enabled["connector_requester_identity"] is True
            and enabled["connector_requester_identity_version"] == version + 1
            and enabled["agent"] == flag_agent
            and str(enabled["agent_id"]) == flag_id,
            "the CLI enable did not report the flag on at the next version",
        )
        check(
            requester_flag(api, flag_id) == (True, version + 1),
            "the API does not show the enable",
        )
        shown = cli_requester_identity(harness, flag_agent, "show")
        seen["cli_show"] = shown
        check(
            shown["connector_requester_identity"] is True
            and shown["connector_requester_identity_version"] == version + 1,
            "`--requester-identity show` disagrees with the API",
        )
        enabled_version = version + 1

        policy = api.ok("GET", f"/agents/{flag_id}/access-policy")
        write = {
            "invocation_team_id": policy["invocation_team_id"],
            "approver_team_id": policy["approver_team_id"],
            "expected_version": policy["version"],
        }
        status, body = api.call(
            "PUT",
            f"/agents/{flag_id}/access-policy",
            {**write, "connector_requester_identity": False},
        )
        seen["access_policy_with_flag_key"] = {"status": status}
        check(status == 422, f"an access-policy write naming the flag answered {status}")
        check(
            requester_flag(api, flag_id) == (True, enabled_version),
            "an access-policy write changed the flag",
        )
        status, body = api.call("PUT", f"/agents/{flag_id}/access-policy", write)
        seen["access_policy_valid"] = {
            "status": status,
            "policy_version": None if not isinstance(body, dict) else body.get("version"),
        }
        check(status == 200, f"a valid access-policy write answered {status}: {body}")
        check(
            requester_flag(api, flag_id) == (True, enabled_version),
            "a valid access-policy write changed the flag or its version",
        )

    human_turn_step(
        harness,
        evidence,
        step_id="1",
        title="a verified human's email turn to the flag agent names them at its connector",
        agent=flag_agent,
        sender=sender,
        text=f"requester-claim: flag on {CONNECTOR_CALL}",
        expected_principal=expected_principal,
        wait=args.wait,
    )

    human_turn_step(
        harness,
        evidence,
        step_id="2",
        title="the control agent's turn completes and reaches its connector with no principal",
        agent=control_agent,
        sender=sender,
        text=f"requester-claim: flag off {CONNECTOR_CALL}",
        expected_principal=None,
        wait=args.wait,
    )

    with evidence.step(
        "3", "`curie cluster message` to the flag agent names no one at its connector"
    ) as seen:
        started = time.time()
        result = harness.target.curie(
            "cluster",
            "message",
            "--agent",
            flag_agent,
            "--timeout-secs",
            str(int(args.wait)),
            "--json",
            f"requester-claim: negative control {CONNECTOR_CALL}",
            timeout=args.wait + 120,
        )
        try:
            reply = json.loads(result.stdout)
        except ValueError:
            reply = None
        seen["cluster_message"] = {"exit": result.returncode, "reply": reply}
        check(result.returncode == 0, f"curie cluster message exited {result.returncode}")
        text = reply.get("reply") if isinstance(reply, dict) else None
        check(
            isinstance(text, str) and bool(text.strip()),
            "curie cluster message returned no reply text",
        )
        observed = harness.observe(flag_agent, since=started - 2, wait=60)
        seen["observe"] = observed_token(observed)
        check(observed["caller_token_present"], "no connector caller token reached the claim")
        claims = observed.get("claims") or {}
        check(claims.get("agent") == flag_agent, f"the token names agent {claims.get('agent')}")
        check(
            "principal" not in claims, f"a non-HTTP turn's token carries {claims.get('principal')}"
        )
        connectors = harness.connectors(flag_agent)
        seen["connectors"] = connectors
        decisions = harness.proxy_decisions(flag_agent, since=started - 2)
        seen["decisions"] = decisions
        seen["connector_call"] = require_connector_admitted(
            [text], decisions, agent=flag_agent, connectors=connectors, principal=False
        )
        fingerprint = observed.get("caller_fingerprint")
        if fingerprint is not None:
            seen["caller_fingerprint_matched"] = require_caller_fingerprint(
                [text], str(fingerprint)
            )

    with evidence.step("5", "a malformed stored flag refuses the next human turn") as seen:
        if enabled_version is None:
            raise StepFailed("step 4 did not enable the flag, so there is nothing to corrupt")
        restored = False
        try:
            harness.target.sql(
                (
                    "ALTER TABLE curie.agents ALTER COLUMN connector_requester_identity "
                    "DROP NOT NULL",
                    {},
                ),
                (
                    "UPDATE curie.agents SET connector_requester_identity = NULL "
                    "WHERE id::text = :a",
                    {"a": flag_id},
                ),
            )
            seen["corrupted"] = "connector_requester_identity set to SQL NULL"
            sent = harness.send(flag_agent, sender, "requester-claim: corrupted flag")
            seen["send"] = send_summary(sent)
            settle = wait_for_refusal(
                harness,
                flag_agent,
                sent["sent_at"],
                sent["conversation_id"],
                min(float(args.wait), 120.0),
            )
            seen.update(settle)
            check(not settle["claims"], f"a sandbox was claimed: {settle['claims']}")
            if 200 <= sent["http_status"] < 300:
                refusals = worker_messages(harness, sent["sent_at"] - 2, "refused")
                seen["worker_refusals"] = refusals
                turn = settle["turn"]
                check(
                    bool(refusals) or (turn["completed"] and turn["outcome"] != "delivered"),
                    "the turn was admitted and no refusal was observed",
                )
            else:
                seen["refused_at_ingress"] = sent["http_status"]
        finally:
            with contextlib.suppress(HarnessError):
                harness.target.sql(
                    (
                        "UPDATE curie.agents SET connector_requester_identity = true "
                        "WHERE id::text = :a",
                        {"a": flag_id},
                    ),
                    (
                        "ALTER TABLE curie.agents ALTER COLUMN connector_requester_identity "
                        "SET NOT NULL",
                        {},
                    ),
                )
                restored = True
            seen["restored"] = restored
        check(restored, "restoring the stored flag failed; repair it by hand")
        with harness.api() as api:
            check(
                requester_flag(api, flag_id) == (True, enabled_version),
                "the restored flag or version differs from before the corruption",
            )


# --- requester-handoff checks ----------------------------------------------------------


def require_replaced(
    first: Mapping[str, Any], second: Mapping[str, Any], *, sent_at: float
) -> dict[str, Any]:
    """The second turn ran on a new claim with a new caller token.

    ``first`` and ``second`` are observations (``Harness.observe`` shapes).
    Requires a claim for ``second`` other than ``first``'s, created no earlier
    than ``sent_at`` less the clock skew, whose token fingerprint differs from
    ``first``'s. Returns claim names, times and fingerprints, never a token.
    """

    previous, claim = first.get("claim"), second.get("claim")
    created = second.get("claim_created_epoch")
    previous_fingerprint = first.get("caller_fingerprint")
    fingerprint = second.get("caller_fingerprint")
    check(
        claim is not None,
        f"no claim was created for the turn sent at {sent_at:.1f}, so an older claim "
        f"({previous}) served it",
    )
    check(claim != previous, f"claim {claim} served both turns; the route was not replaced")
    created_at = float(created) if isinstance(created, (int, float)) else None
    check(
        created_at is not None and created_at >= sent_at - CLOCK_SKEW_S,
        f"claim {claim} was created at {created}, before the send at {sent_at:.1f} "
        f"(allowing {CLOCK_SKEW_S} s of skew)",
    )
    check(
        fingerprint is not None and fingerprint != previous_fingerprint,
        f"claim {claim} carries the previous claim's caller token ({fingerprint})",
    )
    return {
        "previous_claim": previous,
        "claim": claim,
        "sent_at": round(sent_at, 3),
        "claim_created_epoch": created,
        "created_after_send_s": round(float(created_at or 0.0) - sent_at, 1),
        "previous_fingerprint": previous_fingerprint,
        "fingerprint": fingerprint,
    }


def require_reply_fingerprint(texts: Iterable[Any], fingerprint: str) -> dict[str, Any]:
    """Every ``ok`` connector call in this turn's reply sent the token with ``fingerprint``."""

    return {
        "fingerprint": fingerprint,
        "connectors": require_caller_fingerprint(texts, fingerprint),
    }


def require_deferred(
    messages: Sequence[str], *, a_reply_at: float, b_claim_created: float
) -> dict[str, Any]:
    """The second human met the first's live turn, was deferred, and ran only after it.

    ``messages`` are the worker's log messages since the first human's send.
    Fails when a live turn was interrupted on redelivery, when no message
    records the deferral (``deferral not exercised``: the second turn never met
    the live one, so the run proves nothing about it), and when the second
    human's claim was created before the first's reply, less the clock skew.
    """

    interrupted = [m for m in messages if INTERRUPT_MESSAGE in str(m).lower()]
    pending = [m for m in messages if PENDING_MESSAGE in str(m).lower()]
    check(
        not interrupted,
        "a live turn was interrupted on redelivery: "
        f"{[str(m)[:LOG_MESSAGE_LIMIT] for m in interrupted[:3]]}",
    )
    check(
        bool(pending),
        f"{DEFERRAL_NOT_EXERCISED}: none of {len(messages)} worker messages since the first "
        f"send contains `{PENDING_MESSAGE}`, so the second turn never met the live one",
    )
    check(
        b_claim_created >= a_reply_at - CLOCK_SKEW_S,
        f"the second human's claim was created at {b_claim_created}, "
        f"{a_reply_at - b_claim_created:.1f} s before the first human's reply at {a_reply_at}",
    )
    return {
        "pending": [str(m)[:LOG_MESSAGE_LIMIT] for m in pending[:5]],
        "pending_count": len(pending),
        "messages_read": len(messages),
        "a_reply_at": a_reply_at,
        "b_claim_created": b_claim_created,
        "b_claim_after_a_reply_s": round(b_claim_created - a_reply_at, 1),
    }


def parked_events(messages: Iterable[Any]) -> list[dict[str, str]]:
    """Every ``capacity wait parked event_id=<id> cause=<cause>`` in worker messages."""

    found = []
    for message in messages:
        for match in _PARKED.finditer(str(message)):
            found.append({"event_id": match["event_id"], "cause": match["cause"]})
    return found


def require_parked(messages: Sequence[Any], event_id: str) -> dict[str, Any]:
    """The worker parked ``event_id`` in a durable requester-handoff wait.

    ``messages`` are the worker's log messages since the first human's send.
    Requires a ``capacity wait parked`` line naming exactly ``event_id`` with
    ``cause=requester_handoff``: the deferred delivery waits in the durable
    record rather than for a redelivery. Returns what was seen, no token.
    """

    parked = parked_events(messages)
    mine = [
        entry
        for entry in parked
        if entry["event_id"] == event_id and entry["cause"] == HANDOFF_PARK_CAUSE
    ]
    check(
        bool(mine),
        f"the worker never logged `capacity wait parked event_id={event_id} "
        f"cause={HANDOFF_PARK_CAUSE}`; parked lines since the first send: {parked[:5]}",
    )
    return {"event_id": event_id, "parked_count": len(mine), "parked_lines_seen": len(parked)}


def event_turn(replies: Sequence[Mapping[str, Any]], event_id: str) -> dict[str, Any]:
    """One inbound event's turn, from the reply events of a conversation turns share.

    Its reply events are those after the previous distinct ``turn.completed``
    up to its own (a redelivered completion is not a new boundary). Returns
    :func:`turn_summary` plus ``event_id``, ``completed_at`` (unix seconds of
    the sink's record of its completion) and ``position`` (that record's index,
    to order completions), the last two None until it completes.
    """

    segment: list[Mapping[str, Any]] = []
    finished: set[str] = set()
    for position, reply in enumerate(replies):
        event = reply.get("event") or {}
        if event.get("event") != "turn.completed":
            segment.append(reply)
            continue
        done = str(event.get("event_id"))
        if done in finished:
            continue
        finished.add(done)
        if done == event_id:
            summary = turn_summary([*segment, reply])
            summary.update(
                event_id=event_id, completed_at=epoch(str(reply.get("at", ""))), position=position
            )
            return summary
        segment = []
    summary = turn_summary(segment)
    summary.update(event_id=event_id, completed_at=None, position=None)
    return summary


# --- requester-handoff ------------------------------------------------------------------


def require_passed(evidence: Evidence) -> None:
    """Stop the scenario when its last step failed, because later steps build on it."""

    entry = evidence.data["steps"][-1]
    if not entry["passed"]:
        raise StepFailed(
            f"step {entry['id']} failed, so the scenario stops: {entry.get('failure')}"
        )


def retry_once_on(evidence: Evidence, marker: str, attempt: Callable[[str], None]) -> None:
    """Run ``attempt("")``; when its last step fails with ``marker``, run ``attempt("-retry")``.

    The set-aside failure moves from ``steps`` to ``retried``, so it is kept
    but does not fail the run. Any other failure, or a second one, stops the
    scenario.
    """

    for suffix in ("", "-retry"):
        attempt(suffix)
        entry = evidence.data["steps"][-1]
        if not entry["passed"] and not suffix and marker in str(entry.get("failure", "")):
            evidence.data.setdefault("retried", []).append(evidence.data["steps"].pop())
            continue
        require_passed(evidence)
        return


def accepted_event_id(sent: Mapping[str, Any]) -> str:
    """The event id the API's ingress receipt names for one send."""

    response = sent.get("response")
    event_id = response.get("event_id") if isinstance(response, Mapping) else None
    check(
        isinstance(event_id, str) and bool(event_id),
        f"the API accepted the turn with no event id: {response}",
    )
    return str(event_id)


def await_turns(
    harness: Harness,
    agent: str,
    conversation: str,
    since: float,
    event_ids: Sequence[str],
    wait: float,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    """Each event's :func:`event_turn` once all completed or ``wait`` ran out, and the replies."""

    deadline = time.monotonic() + wait
    while True:
        replies = harness.replies(agent, since, conversation)
        turns = {event_id: event_turn(replies, event_id) for event_id in event_ids}
        if all(turn["completed"] for turn in turns.values()) or time.monotonic() > deadline:
            return turns, replies
        time.sleep(3)


def conversation_turn(
    harness: Harness,
    seen: dict[str, Any],
    *,
    agent: str,
    sender: str,
    text: str,
    conversation: str | None,
    wait: float,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Send one human's turn on ``conversation`` (a new one when None) and await it.

    Returns the send, the newest claim since the send (``Harness.observe``),
    and the turn answering this send's event, selected by its event id since
    the conversation carries other humans' turns too.
    """

    sent = harness.send(agent, sender, text, conversation=conversation)
    seen["send"] = send_summary(sent)
    check(sent.get("http_status") == 200, f"the email turn answered {sent.get('http_status')}")
    event_id = accepted_event_id(sent)
    since = float(sent["sent_at"]) - CLOCK_SKEW_S
    observed = harness.observe(agent, since=since, wait=wait)
    seen["observe"] = observed_token(observed)
    turns, _ = await_turns(harness, agent, str(sent["conversation_id"]), since, [event_id], wait)
    seen["turn"] = turns[event_id]
    return sent, observed, turns[event_id]


def judge_conversation_turn(
    harness: Harness,
    seen: dict[str, Any],
    *,
    agent: str,
    sent: Mapping[str, Any],
    observed: Mapping[str, Any],
    turn: Mapping[str, Any],
    principal: Any,
    after: float | None = None,
) -> None:
    """:func:`require_connector_turn`, then the reply's dials sent the claim's token."""

    require_connector_turn(
        harness,
        seen,
        agent=agent,
        observed=observed,
        turn=turn,
        since=float(sent["sent_at"]),
        expected_principal=principal,
        after=after,
    )
    seen["reply_fingerprint"] = require_reply_fingerprint(
        turn.get("texts") or [], str(observed.get("caller_fingerprint"))
    )


def send_while_live(
    harness: Harness,
    seen: dict[str, Any],
    *,
    agent: str,
    first: str,
    second: str,
    conversation: str,
    step_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """The first human starts a progress-demo turn; the second writes a second later.

    Both proofs are signed before either post, and both posts share one
    port-forward, so neither signing nor a forward stretches the gap.
    """

    first_turn = harness.prepare(
        agent, first, f"requester-handoff: {step_id} {PROGRESS_DEMO}", conversation=conversation
    )
    second_turn = harness.prepare(
        agent, second, f"requester-handoff: {step_id} {CONNECTOR_CALL}", conversation=conversation
    )
    with harness.api() as api:
        first_sent = harness.post(first_turn, api)
        if first_sent.get("http_status") == 200:
            time.sleep(SECOND_SEND_DELAY_S)
            second_sent = harness.post(second_turn, api)
    seen["first_send"] = send_summary(first_sent)
    check(
        first_sent.get("http_status") == 200,
        f"{first}'s turn answered {first_sent.get('http_status')}",
    )
    seen["second_send"] = send_summary(second_sent)
    check(
        second_sent.get("http_status") == 200,
        f"{second}'s turn answered {second_sent.get('http_status')}",
    )
    seen["second_send_after_first_s"] = round(
        float(second_sent["sent_at"]) - float(first_sent["sent_at"]), 1
    )
    return first_sent, second_sent


def answered_progress_demo(texts: Iterable[Any]) -> bool:
    return any(isinstance(text, str) and PROGRESS_DEMO_ANSWER in text for text in texts)


def deferral_step(
    harness: Harness,
    evidence: Evidence,
    *,
    step_id: str,
    agent: str,
    conversation: str,
    first: str,
    second: str,
    previous: Mapping[str, Any],
    principal: Any,
    wait: float,
) -> None:
    """H4: the second human writes during the first's live turn on the flag agent.

    The first turn finishes uninterrupted; the worker logged the second
    delivery's deferral, parked it in a durable requester-handoff wait under
    its own event id, and interrupted nothing; and the second turn ran
    afterwards on a new claim whose token names the second human, as its reply
    proves.
    """

    with evidence.step(
        step_id, f"{second} writes during {first}'s live turn: deferred, then served as {second}"
    ) as seen:
        first_sent, second_sent = send_while_live(
            harness,
            seen,
            agent=agent,
            first=first,
            second=second,
            conversation=conversation,
            step_id=step_id,
        )
        first_event, second_event = accepted_event_id(first_sent), accepted_event_id(second_sent)
        since = float(first_sent["sent_at"]) - CLOCK_SKEW_S
        turns, _ = await_turns(
            harness, agent, conversation, since, [first_event, second_event], wait
        )
        first_turn, second_turn = turns[first_event], turns[second_event]
        seen["first_turn"], seen["second_turn"] = first_turn, second_turn
        require_delivered(first_turn)
        check(
            answered_progress_demo(first_turn["texts"]),
            f"{first}'s reply has no `{PROGRESS_DEMO_ANSWER}`, so its turn did not finish: "
            f"{first_turn['texts'][-3:]}",
        )
        check(
            bool(second_turn["completed"]),
            f"{second}'s turn did not complete within {int(wait)} s of {first}'s send",
        )
        seen["claims_since_first_send"] = [claim["name"] for claim in harness.claims(agent, since)]
        observed = harness.observe(agent, since=since, wait=60)
        seen["observe"] = observed_token(observed)
        check(
            observed.get("claim_created_epoch") is not None,
            f"no claim of {agent} was created since {first}'s send, so an older claim served "
            f"{second}",
        )
        messages = [entry["message"] for entry in harness.worker_log(since)]
        seen["deferral"] = require_deferred(
            messages,
            a_reply_at=float(first_turn["completed_at"]),
            b_claim_created=float(observed["claim_created_epoch"]),
        )
        seen["parked"] = require_parked(messages, second_event)
        seen["replaced"] = require_replaced(
            previous, observed, sent_at=float(second_sent["sent_at"])
        )
        judge_conversation_turn(
            harness,
            seen,
            agent=agent,
            sent=second_sent,
            observed=observed,
            turn=second_turn,
            principal=principal,
        )


def serving_claim(
    served: Mapping[str, Any], *, serving: Any, agent: str, texts: Iterable[Any]
) -> dict[str, Any]:
    """The H5 control's serving claim: principal-free, and every live dial sent its token.

    ``served`` is the newest claim observed since the warm send; it must be
    ``serving``, the claim the live turn ran on. Its caller token names
    ``agent`` and carries no principal, so nothing the second human steered
    into the turn ran as anyone. Every ``connector-call`` line in the live
    turn's replies carries that token's fingerprint. The second human's folded
    message dials nothing of its own (the fake model runs only the live turn's
    script). Returns the evidence, which never holds a token.
    """

    check(
        served.get("claim") == serving,
        f"the claim serving the live turn is {serving}, but the newest observed is "
        f"{served.get('claim')}",
    )
    check(
        bool(served.get("caller_token_present")),
        f"no connector caller token reached the serving claim {serving}",
    )
    token_claims = served.get("claims") or {}
    check(
        token_claims.get("agent") == agent,
        f"the serving claim's token names agent {token_claims.get('agent')}",
    )
    check(
        "principal" not in token_claims,
        f"the serving claim {serving} on a flag-off agent carries a principal",
    )
    live_calls = connector_calls(texts)
    fingerprint = served.get("caller_fingerprint")
    stray = [call for call in live_calls if call.get("caller") != fingerprint]
    check(not stray, f"a dial in the live turn did not send the serving claim's token: {stray[:3]}")
    return {**observed_token(served), "live_turn_calls": live_calls}


def control_step(
    harness: Harness,
    evidence: Evidence,
    *,
    step_id: str,
    agent: str,
    first: str,
    second: str,
    wait: float,
) -> None:
    """H5: the same two humans on the flag-off agent, on a new conversation.

    A warm turn from the first human boots the conversation's sandbox, so the
    progress-demo turn is live within the second send's window. Then: one
    claim for the conversation, no deferral logged, both turns delivered, and
    the second completed before the first, so it was steered into the live
    turn (``steering not exercised`` otherwise).
    """

    with evidence.step(
        step_id, f"control, flag off: {second} during {first}'s live turn is steered on one claim"
    ) as seen:
        warm: dict[str, Any] = {}
        seen["warm"] = warm
        sent, observed, turn = conversation_turn(
            harness,
            warm,
            agent=agent,
            sender=first,
            text=f"requester-handoff: {step_id} warm {CONNECTOR_CALL}",
            conversation=None,
            wait=wait,
        )
        judge_conversation_turn(
            harness, warm, agent=agent, sent=sent, observed=observed, turn=turn, principal=None
        )
        conversation = str(sent["conversation_id"])
        first_sent, second_sent = send_while_live(
            harness,
            seen,
            agent=agent,
            first=first,
            second=second,
            conversation=conversation,
            step_id=step_id,
        )
        first_event, second_event = accepted_event_id(first_sent), accepted_event_id(second_sent)
        since = float(first_sent["sent_at"]) - CLOCK_SKEW_S
        turns, replies = await_turns(
            harness, agent, conversation, since, [first_event, second_event], wait
        )
        first_turn, second_turn = turns[first_event], turns[second_event]
        seen["first_turn"], seen["second_turn"] = first_turn, second_turn
        for who, done in ((first, first_turn), (second, second_turn)):
            check(
                bool(done["completed"]) and done["outcome"] == "delivered",
                f"{who}'s turn ended {done['outcome']!r} (completed: {done['completed']}), "
                "not delivered",
            )
        texts = turn_summary(replies)["texts"]
        check(
            answered_progress_demo(texts),
            f"{first}'s progress-demo turn never answered `{PROGRESS_DEMO_ANSWER}`",
        )
        # A new delivery may replace the warm sandbox (its state token cannot
        # cover the new turn), so the first human's turn can open a claim of
        # its own. The second human, who steers into that live turn, must not.
        claims = [claim["name"] for claim in harness.claims(agent, since)]
        seen["claims"] = claims
        check(
            len(claims) <= 1,
            f"{second} got a claim of their own on a flag-off agent; claims since "
            f"{first}'s send: {claims}",
        )
        # The claim that served the live turn, the replacement when one opened
        # and the warm claim otherwise, is judged itself (:func:`serving_claim`).
        served = harness.observe(agent, since=float(sent["sent_at"]) - CLOCK_SKEW_S, wait=60)
        seen["served"] = serving_claim(
            served,
            serving=claims[0] if claims else observed.get("claim"),
            agent=agent,
            texts=texts,
        )
        pending = worker_messages(harness, since, PENDING_MESSAGE)
        seen["pending"] = pending
        check(not pending, f"a flag-off agent deferred a delivery: {pending[:3]}")
        steered = second_turn["position"] < first_turn["position"]
        seen["steered"] = steered
        seen["folded_notice"] = any(
            isinstance(text, str) and "folded into the in-progress reply" in text.lower()
            for text in texts
        )
        check(
            steered,
            f"{STEERING_NOT_EXERCISED}: {second}'s turn completed after {first}'s, so it never "
            "met the live turn",
        )


@scenario("requester-handoff")
def requester_handoff(harness: Harness, args: Any, evidence: Evidence) -> None:
    """ADR 0270 decision 3: a second human on a conversation never gets the first's principal.

    Two verified humans write on one conversation C to the opted-in flag
    agent, each with ``[fake:connector-call]``, so every reply carries the
    fingerprint of the caller token its dial sent. H1 the first human (A); H2
    the second (B) while idle replaces the claim; H3 A again replaces it back
    and warms A's sandbox; H4 B writes during A's live progress-demo turn, is
    deferred and parked in a durable wait until it ends, then served as B; H5
    the control on the flag-off
    agent steers B into A's live turn on one claim. Each step needs the one
    before it, so a failed step stops the run. H4 runs again once from H3 on a
    new conversation when it fails only because B never met A's live turn,
    and H5 once when B was never steered.
    """

    flag_agent, control_agent = two_agents(harness, args)
    first, second = args.sender.lower(), args.second_sender.lower()
    if first == second:
        raise HarnessError("requester-handoff needs two humans; --second-sender equals --sender")
    principal_a, principal_b = principal_for(harness, first), principal_for(harness, second)
    evidence.data.update(
        flag_agent=flag_agent,
        control_agent=control_agent,
        sender=first,
        second_sender=second,
        expected_principals={first: principal_a, second: principal_b},
    )
    wait = float(args.wait)
    handoff_wait = max(wait, HANDOFF_WAIT_S)

    with (
        evidence.step(
            "H0", "preconditions: the flag agent opted in, the control agent not"
        ) as seen,
        harness.api() as api,
    ):
        require_install_ready(harness, seen, (flag_agent, control_agent))
        for name, wanted in ((flag_agent, True), (control_agent, False)):
            flag, version = set_requester_flag(
                api,
                harness.agent_entry(name)["agent_id"],
                wanted,
                what=f"setting {name}'s flag {'on' if wanted else 'off'}",
            )
            seen[name] = {"flag": flag, "version": version}
            check(flag is wanted, f"{name}'s flag is {flag!r}, not {wanted}")
    require_passed(evidence)

    first_claim: dict[str, Any] = {}
    conversation = ""
    with evidence.step(
        "H1", f"{first} on a new conversation: the claim and the connector name {first}"
    ) as seen:
        sent, first_claim, turn = conversation_turn(
            harness,
            seen,
            agent=flag_agent,
            sender=first,
            text=f"requester-handoff: H1 {CONNECTOR_CALL}",
            conversation=None,
            wait=wait,
        )
        judge_conversation_turn(
            harness,
            seen,
            agent=flag_agent,
            sent=sent,
            observed=first_claim,
            turn=turn,
            principal=principal_a,
        )
        conversation = str(sent["conversation_id"])
    require_passed(evidence)

    last: dict[str, Any] = {}
    with evidence.step(
        "H2", f"{second} on the same conversation while idle: a new claim names {second}"
    ) as seen:
        sent, last, turn = conversation_turn(
            harness,
            seen,
            agent=flag_agent,
            sender=second,
            text=f"requester-handoff: H2 {CONNECTOR_CALL}",
            conversation=conversation,
            wait=wait,
        )
        seen["replaced"] = require_replaced(first_claim, last, sent_at=float(sent["sent_at"]))
        judge_conversation_turn(
            harness,
            seen,
            agent=flag_agent,
            sent=sent,
            observed=last,
            turn=turn,
            principal=principal_b,
        )
    require_passed(evidence)

    def handoff_attempt(suffix: str) -> None:
        nonlocal last
        fresh = bool(suffix)
        title = (
            f"{first} on a new conversation: a new claim names {first}"
            if fresh
            else f"{first} again: the claim is replaced back and names {first}"
        )
        warm: dict[str, Any] = {}
        topic = conversation
        with evidence.step(f"H3{suffix}", title) as seen:
            sent, warm, turn = conversation_turn(
                harness,
                seen,
                agent=flag_agent,
                sender=first,
                text=f"requester-handoff: H3{suffix} {CONNECTOR_CALL}",
                conversation=None if fresh else conversation,
                wait=wait,
            )
            seen["replaced"] = require_replaced(last, warm, sent_at=float(sent["sent_at"]))
            judge_conversation_turn(
                harness,
                seen,
                agent=flag_agent,
                sent=sent,
                observed=warm,
                turn=turn,
                principal=principal_a,
            )
            topic = str(sent["conversation_id"])
        require_passed(evidence)
        last = warm
        deferral_step(
            harness,
            evidence,
            step_id=f"H4{suffix}",
            agent=flag_agent,
            conversation=topic,
            first=first,
            second=second,
            previous=warm,
            principal=principal_b,
            wait=handoff_wait,
        )

    retry_once_on(evidence, DEFERRAL_NOT_EXERCISED, handoff_attempt)

    retry_once_on(
        evidence,
        STEERING_NOT_EXERCISED,
        lambda suffix: control_step(
            harness,
            evidence,
            step_id=f"H5{suffix}",
            agent=control_agent,
            first=first,
            second=second,
            wait=wait,
        ),
    )


# --- approver-ticket-creation -----------------------------------------------------------

# The bundle every agent `up` deploys; the scenario derives its gated versions from it.
TICKET_BUNDLE = REPO / "examples" / "weather"
# The worker's log line for a turn refused before any execution (kernel/lifecycle.py).
HTTP_REFUSED_MESSAGE = "http invocation refused"
NEGATIVE_CONTROL_WAIT_S = 60.0
# A CLI failure that names a refusal, when the API refused the turn at ingress.
_REFUSAL_WORDS = ("refus", "identity", "forbidden", "403", "401")
# (label, who sends it). The approver's turn gives them a canonical principals
# row; each requester turn gives the creation steps one fresh dedupe key.
TICKET_TURNS = (
    ("approver", "approver"),
    ("valid", "requester"),
    ("refuse", "requester"),
    ("race", "requester"),
    ("bypass", "requester"),
)


def refused_with(status: int, body: Any, wanted: int, code: str) -> bool:
    """A refusal with this status whose body names ``code`` (as a detail or a code field)."""

    return status == wanted and code in json.dumps(body)


def api_revision(harness: Harness) -> dict[str, Any]:
    """What API build answered: the image tag's commit when it names one, and the image ids.

    kubectl cannot read an image's labels, so the tag stands in for
    ``org.opencontainers.image.revision``; the running pods' image ids pin the
    exact digest.
    """

    target = harness.target
    image = target.api_image()
    deployment = target.kubectl_json("get", "deployment", target.component("api"))
    labels = deployment["spec"]["selector"]["matchLabels"]
    selector = ",".join(f"{key}={value}" for key, value in sorted(labels.items()))
    image_ids = sorted(
        {
            str(status["imageID"])
            for pod in target.kubectl_json("get", "pod", "-l", selector)["items"]
            for status in (pod.get("status", {}).get("containerStatuses") or [])
            if status.get("name") == "api" and status.get("imageID")
        }
    )
    revision = tickets.revision_from_image(image)
    return {
        "api_revision": revision or image,
        "api_revision_source": "image tag" if revision else "image reference",
        "api_image": image,
        "api_image_ids": image_ids,
    }


class TicketScenario:
    """One ``approver-ticket-creation`` run on one agent, and what it must put back.

    ``changes`` records each thing actually changed, as it changes, so
    :meth:`restore` undoes exactly those even when the run stops part way.
    """

    def __init__(self, harness: Harness, args: Any, evidence: Evidence) -> None:
        self.harness, self.args, self.evidence = harness, args, evidence
        names = list(harness.data.get("agents", {}))
        self.agent = str(args.flag_agent or (names[0] if names else ""))
        if not self.agent:
            raise HarnessError("no agent in the harness state; run `up --agent agent-a ...` first")
        self.agent_id = str(harness.agent_entry(self.agent)["agent_id"])
        self.control = args.control_agent or next((n for n in names if n != self.agent), None)
        self.tenant_id = str(harness.data["tenant_id"])
        self.issuer = str(harness.data["issuer"])
        requester, approver = args.sender.lower(), args.second_sender.lower()
        if requester == approver:
            raise HarnessError(
                "the requester (--sender) and approver (--second-sender) must differ"
            )
        self.senders = {"requester": requester, "approver": approver}
        self.requester = {"issuer": self.issuer, "subject": roster_subject(harness, requester)}
        self.approver = {"issuer": self.issuer, "subject": roster_subject(harness, approver)}
        roster = harness.data.get("roster") or {}
        self.ceiling = tickets.ceiling_subjects(roster)
        # Other verified humans, each named as a ticket's requester in turn.
        self.others: dict[str, dict[str, str]] = {}
        for address, entry in sorted((roster.get("senders") or {}).items()):
            if address == requester or str(entry.get("subject")) not in self.ceiling:
                continue
            self.others[address.split("@", 1)[0]] = {
                "issuer": self.issuer,
                "subject": str(entry["subject"]),
            }
            if len(self.others) == 2:
                break
        self.stamp = time.strftime("%Y%m%d%H%M%S", time.gmtime())
        self.key: dict[str, str] = {}
        self.nonces: dict[str, str] = {}
        self.turns: dict[str, dict[str, Any]] = {}
        self.valid: dict[str, Any] = {}
        self.original: dict[str, Any] = {}
        self.base: dict[str, Any] = {}
        self.changes: dict[str, Any] = {
            "routes": False,
            "flag": False,
            "approver_role": False,
            "versions": [],
            "deployments": [],
            "ended": [],
        }

    # platform API and pod helpers -----------------------------------------------------

    def drive(self, action: str, **params: Any) -> dict[str, Any]:
        """One action of the in-pod driver; a refusal raises :class:`HarnessError`."""

        started = time.monotonic()
        print(f"approver-ticket-creation: driver {action} started", file=sys.stderr, flush=True)
        try:
            result = self.harness.target.pod_python(
                tickets.POD_BOOTSTRAP, tickets.pod_params(action, **params)
            )
        finally:
            print(
                f"approver-ticket-creation: driver {action} ended after "
                f"{time.monotonic() - started:.1f}s",
                file=sys.stderr,
                flush=True,
            )
        if not isinstance(result, dict) or "error" in result:
            error = result.get("error") if isinstance(result, dict) else result
            raise HarnessError(f"the in-pod ticket driver ({action}) refused: {error}")
        return result

    def configuration(self, api: Api) -> tuple[Any, Any, dict[str, Any]]:
        """The requester flag, the shared configuration version and the routes, as read back."""

        out = api.ok("GET", f"/agents/{self.agent_id}")
        check(
            "connector_requester_identity" in out and "connector_requester_identity_version" in out,
            "AgentOut has no connector_requester_identity fields; is the candidate installed?",
        )
        return (
            out["connector_requester_identity"],
            out["connector_requester_identity_version"],
            dict(out.get("approval_routes") or {}),
        )

    def patch(self, api: Api, body: Mapping[str, Any]) -> tuple[int, Any]:
        return api.call("PATCH", f"/agents/{self.agent_id}", dict(body))

    def routes(
        self, registered: Mapping[str, Any] | None = None, *, route: str = tickets.TICKET_ROUTE
    ) -> dict[str, Any]:
        return tickets.routes_with(self.base, registered, route=route)

    def registration(self, api: Api, *, tool: str = tickets.TOOL) -> dict[str, Any]:
        """This run's selector, naming the access policy version as its ceiling."""

        version = int(api.ok("GET", f"/agents/{self.agent_id}/access-policy")["version"])
        return tickets.registration(
            key_id=self.key["key_id"],
            public_key=self.key["public_key"],
            role_version=version,
            tool=tool,
        )

    def write_routes(self, api: Api, routes: Mapping[str, Any]) -> tuple[int, Any]:
        """A route write, versioned when the stored or proposed map names a selector (plan D2)."""

        body: dict[str, Any] = {"approval_routes": dict(routes)}
        _, version, stored = self.configuration(api)
        if tickets.has_registration(stored) or tickets.has_registration(routes):
            body["approval_routes_expected_version"] = version
        return self.patch(api, body)

    def active_deployments(self, api: Api) -> list[dict[str, Any]]:
        return [
            dict(item)
            for item in api.ok("GET", f"/deployments?agent_id={self.agent_id}")
            if item.get("status") == "active"
        ]

    def environments(self) -> list[dict[str, Any]]:
        """The newest original active deployment of each environment."""

        latest: dict[str, dict[str, Any]] = {}
        for item in sorted(self.original["deployments"], key=lambda d: str(d.get("deployed_at"))):
            latest[str(item["environment"])] = item
        return [latest[name] for name in sorted(latest)]

    def deploy_version(self, api: Api, label: str, route: str) -> tuple[str, list[tuple[int, Any]]]:
        """A new version of examples/weather gating the pair on ``route``, deployed per environment.

        The bundle is uploaded (it must be accepted: an inactive version's
        bundle is not route-checked), then ``POST /deployments`` runs once per
        environment the agent serves in; the answers are returned unjudged.
        """

        manifest = json.loads((TICKET_BUNDLE / tickets.MANIFEST_PATH).read_text())
        gate = tickets.live_tool(tickets.CONNECTOR, tickets.TOOL)
        data = tickets.bundle_archive(
            TICKET_BUNDLE, tickets.gated_manifest(manifest, [(gate, route)])
        )
        write_private(self.harness.state.directory / f"approver-ticket-{label}.tar.gz", data)
        version = api.ok(
            "POST",
            f"/agents/{self.agent_id}/versions",
            {"version_label": f"identity-e2e-{label}-{self.stamp}", "created_by": "identity-e2e"},
        )
        version_id = str(version["id"])
        self.changes["versions"].append(version_id)
        body, content_type = tickets.multipart("file", "bundle.tar.gz", data, "application/gzip")
        status, payload = http(
            "PUT",
            f"{api.base}/agents/{self.agent_id}/versions/{version_id}/bundle",
            raw=body,
            headers={
                "X-API-Key": self.harness.target.platform_key(),
                "Content-Type": content_type,
            },
        )
        if status != 201:
            raise HarnessError(
                f"uploading the {label} bundle answered {status}: {json.dumps(payload)[:400]}"
            )
        results = []
        for template in self.environments():
            status, payload = api.call(
                "POST",
                "/deployments",
                {
                    "agent_id": self.agent_id,
                    "version_id": version_id,
                    "environment": template["environment"],
                    "workspace_enabled": bool(template.get("workspace_enabled")),
                },
            )
            if 200 <= status < 300 and isinstance(payload, dict):
                self.changes["deployments"].append(str(payload["id"]))
            results.append((status, payload))
        return version_id, results

    def ensure_registered(self, api: Api) -> None:
        """The flag on and this run's selector registered, whatever the steps before left."""

        flag, version, stored = self.configuration(api)
        if flag is not True:
            self.changes["flag"] = True
            status, body = self.patch(
                api,
                {
                    "connector_requester_identity": True,
                    "connector_requester_identity_expected_version": version,
                },
            )
            if status != 200:
                raise HarnessError(
                    f"enabling the flag answered {status}: {tickets.detail_code(body)}"
                )
            flag, version, stored = self.configuration(api)
        current = (stored.get(tickets.TICKET_ROUTE) or {}).get("approver_ticket") or {}
        if current.get("key_id") != self.key["key_id"]:
            status, body = self.patch(
                api,
                {
                    "approval_routes": self.routes(self.registration(api)),
                    "approval_routes_expected_version": version,
                },
            )
            if status != 200:
                raise HarnessError(
                    f"registering the selector answered {status}: {tickets.detail_code(body)}"
                )

    def create(
        self,
        seen: dict[str, Any],
        cases: Sequence[Mapping[str, Any]],
        *,
        count_turns: Sequence[str] = (),
        count_dedupe: Sequence[str] = (),
        count_nonces: Sequence[str] = (),
        facts_turns: Sequence[str] = (),
        facts_dedupe: Sequence[str] = (),
    ) -> dict[str, Any]:
        """Send ``cases`` from the API pod; record answers, row counts and spent nonces."""

        used = tickets.nonce_labels(cases)
        for label in [*used, *count_nonces]:
            self.nonces.setdefault(label, tickets.new_nonce())
        labels = tickets.turn_labels(cases)
        missing = [label for label in labels if label not in self.turns]
        if missing:
            raise HarnessError(f"no captured turn for {missing}")
        dedupe = {label: self.turns[label]["event_id"] for label in count_turns}
        result = self.drive(
            "create",
            agent_id=self.agent_id,
            key={"seed_hex": self.key["seed_hex"], "key_id": self.key["key_id"]},
            registered={
                "connector": tickets.CONNECTOR,
                "tool": tickets.TOOL,
                "route": tickets.TICKET_ROUTE,
            },
            approvers=[self.approver],
            nonces={label: self.nonces[label] for label in used},
            turns={
                label: {
                    "stream_id": self.turns[label]["stream_id"],
                    "event_id": self.turns[label]["event_id"],
                }
                for label in labels
            },
            cases=[dict(item) for item in cases],
            count_dedupe=[*dedupe.values(), *count_dedupe],
            count_nonces={label: self.nonces[label] for label in count_nonces},
            facts=[*(self.turns[label]["event_id"] for label in facts_turns), *facts_dedupe],
        )
        seen["cases"] = [
            {
                key: item.get(key)
                for key in (
                    "name",
                    "endpoint",
                    "route",
                    "gate_kind",
                    "granted_tool",
                    "ticket",
                    "expected",
                    "results",
                )
            }
            for item in result["cases"]
        ]
        rows = result.get("rows") or {}
        seen["rows"] = {label: rows.get(event) for label, event in dedupe.items()}
        seen["rows"].update({key: rows.get(key) for key in count_dedupe})
        seen["spent_nonces"] = dict(result.get("nonces") or {})
        return result

    # setup and restore ---------------------------------------------------------------

    def prepare(self) -> None:
        """Preconditions, the API revision and the snapshot restore returns to; changes nothing."""

        setup: dict[str, Any] = {}
        self.evidence.data["setup"] = setup
        require_install_ready(self.harness, setup, (self.agent,))
        check(
            tickets.CONNECTOR in setup["connectors"][self.agent],
            f"{self.agent} has no hosted connector {tickets.CONNECTOR}; deploy it from "
            "examples/weather with `up --bundle examples/weather`",
        )
        check(
            self.approver["subject"] in self.ceiling and self.requester["subject"] in self.ceiling,
            "the requester and approver must both be active humans in the roster",
        )
        self.evidence.data.update(api_revision(self.harness))
        with self.harness.api() as api:
            flag, version, _ = self.configuration(api)
            check(isinstance(flag, bool), f"the requester flag reads {flag!r}")
            self.original.update(flag=flag, version=version)
            active = self.active_deployments(api)
            check(bool(active), f"{self.agent} has no active deployment")
            self.original["deployments"] = active
            policy = api.ok("GET", f"/agents/{self.agent_id}/access-policy")
            check(
                int(policy["version"]) >= 1,
                f"{self.agent} has no stored access policy; rerun `up`",
            )
        stored = self.drive("agent_routes", agent_id=self.agent_id)["routes"]
        check(
            not tickets.has_registration(stored),
            f"{self.agent} already stores a selector registration; a previous run did not "
            "restore, so remove it with a versioned route write first",
        )
        self.original["routes"] = stored
        self.base = tickets.base_routes(stored)
        self.original["approver_role"] = self.drive(
            "approver_role", tenant_id=self.tenant_id, agent_id=self.agent_id
        )
        seed, public = ed25519_pair(self.harness.state.directory)
        key = tickets.selector_key(seed, public)
        key["key_id"] = tickets.key_id_for(self.stamp)
        write_private(
            self.harness.state.directory / "approver-selector.json",
            json.dumps(key, sort_keys=True).encode(),
        )
        self.key = key
        role = self.original["approver_role"]
        setup.update(
            key_id=key["key_id"],
            ceiling_subjects=self.ceiling,
            original={
                "flag": flag,
                "version": version,
                "routes": sorted(stored or {}),
                "deployments": [
                    {key: item.get(key) for key in ("id", "version_id", "environment")}
                    for item in active
                ],
                "approver_role": {
                    key: role.get(key)
                    for key in ("version", "row", "configured", "had_role", "legacy_state")
                },
            },
        )

    def install(self) -> None:
        """Bind both owned routes, serve only the gated bundle, and restrict the approver role.

        Both routes are bound before the gated version is deployed, because a
        deployment is refused while its bundle declares a route the agent has
        not bound. Every original active deployment is then ended, because a
        registration needs every active version to gate the pair (plan D5).
        """

        setup = self.evidence.data["setup"]
        with self.harness.api() as api:
            self.changes["routes"] = True
            status, body = self.write_routes(api, self.routes())
            if status != 200:
                raise HarnessError(
                    f"binding {list(tickets.OWNED_ROUTES)} answered {status}: "
                    f"{tickets.detail_code(body)}"
                )
            version_id, results = self.deploy_version(api, "gate", tickets.TICKET_ROUTE)
            setup["gated_version"] = version_id
            refused = [
                (status, tickets.detail_code(body)) for status, body in results if status != 201
            ]
            if refused:
                raise HarnessError(f"deploying the gated version answered {refused}")
            for item in self.original["deployments"]:
                api.ok("DELETE", f"/deployments/{item['id']}")
                self.changes["ended"].append(item)
            serving = sorted({str(item["version_id"]) for item in self.active_deployments(api)})
            setup["serving_versions"] = serving
            if serving != [version_id]:
                raise HarnessError(f"{self.agent} serves {serving}, not only {version_id}")
        self.changes["approver_role"] = True
        role = self.drive(
            "approver_role",
            tenant_id=self.tenant_id,
            agent_id=self.agent_id,
            filter=tickets.approver_filter(self.issuer, self.ceiling),
        )
        setup["approver_role"] = {key: role.get(key) for key in ("action", "version")}

    def restore(self) -> None:
        """Put back what ``changes`` records; a failure fails the run, naming what is left."""

        report: dict[str, Any] = {"done": [], "failed": []}
        self.evidence.data["restore"] = report

        def attempt(name: str, action: Callable[[], Any]) -> None:
            try:
                report["done"].append({"action": name, "result": action()})
            except (HarnessError, StepFailed, KeyError, TypeError, ValueError) as exc:
                report["failed"].append({"action": name, "error": str(exc)[:400]})

        changes = self.changes
        if changes["routes"] or changes["flag"] or changes["deployments"] or changes["ended"]:
            try:
                with self.harness.api() as api:
                    attempt("remove the registration", lambda: self._drop_registration(api))
                    if changes["flag"]:
                        attempt("requester flag", lambda: self._restore_flag(api))
                    if changes["deployments"] or changes["ended"]:
                        attempt("deployments", lambda: self._restore_deployments(api))
                    if changes["routes"]:
                        attempt("approval routes", lambda: self._restore_routes(api))
                    attempt("verify", lambda: self._verify_restored(api))
            except HarnessError as exc:
                report["failed"].append({"action": "port-forward", "error": str(exc)[:400]})
        if changes["approver_role"]:
            attempt("approver role", self._restore_role)
        report["left"] = (
            f"approvals this run created stay pending until they expire "
            f"({tickets.APPROVAL_TTL_S} s); the run's agent versions stay, undeployed"
        )
        if report["failed"]:
            failed = "; ".join(f"{item['action']}: {item['error']}" for item in report["failed"])
            self.evidence.data.setdefault("aborted", f"restore incomplete: {failed}")

    def _drop_registration(self, api: Api) -> str:
        _, version, stored = self.configuration(api)
        if not tickets.has_registration(stored):
            return "no registration stored"
        status, body = self.patch(
            api, {"approval_routes": self.routes(), "approval_routes_expected_version": version}
        )
        if status != 200:
            raise HarnessError(f"answered {status}: {tickets.detail_code(body)}")
        return f"removed at version {version}"

    def _restore_flag(self, api: Api) -> str:
        flag, version, _ = self.configuration(api)
        wanted = self.original["flag"]
        if flag == wanted:
            return "unchanged"
        status, body = self.patch(
            api,
            {
                "connector_requester_identity": wanted,
                "connector_requester_identity_expected_version": version,
            },
        )
        if status != 200:
            raise HarnessError(f"answered {status}: {tickets.detail_code(body)}")
        return f"set back to {wanted}"

    def _restore_deployments(self, api: Api) -> dict[str, Any]:
        recreated = []
        for item in sorted(self.changes["ended"], key=lambda d: str(d.get("deployed_at"))):
            body = {
                "agent_id": self.agent_id,
                "version_id": str(item["version_id"]),
                "environment": item["environment"],
                "workspace_enabled": bool(item.get("workspace_enabled")),
            }
            if item.get("commit_sha"):
                body["commit_sha"] = item["commit_sha"]
            recreated.append(str(api.ok("POST", "/deployments", body)["id"]))
        self.changes["ended"] = []
        ended = []
        for item in self.active_deployments(api):
            if str(item["version_id"]) in self.changes["versions"]:
                api.ok("DELETE", f"/deployments/{item['id']}")
                ended.append(str(item["id"]))
        self.changes["deployments"] = []
        return {"recreated": recreated, "ended": ended}

    def _restore_routes(self, api: Api) -> list[str]:
        original = self.original.get("routes")
        routes = dict(original) if isinstance(original, dict) else {}
        status, body = self.write_routes(api, routes)
        if status != 200:
            raise HarnessError(f"answered {status}: {tickets.detail_code(body)}")
        return sorted(routes)

    def _restore_role(self) -> dict[str, Any]:
        """Back to the original approver role, or to none when the agent had none.

        An absent approver role and a ``filter: null`` one evaluate differently
        (not configured against unrestricted), so a role the run added is
        removed, not left unrestricted.
        """

        original = self.original["approver_role"]
        if original.get("had_role"):
            params: dict[str, Any] = {"filter": original.get("filter")}
        else:
            params = {"remove": True, "configured": bool(original.get("configured"))}
        result = self.drive(
            "approver_role", tenant_id=self.tenant_id, agent_id=self.agent_id, **params
        )
        now = self.drive("approver_role", tenant_id=self.tenant_id, agent_id=self.agent_id)
        wrong = [
            key
            for key in ("configured", "had_role", "legacy_state", "filter")
            if now.get(key) != original.get(key)
        ]
        if wrong:
            raise HarnessError(f"the approver role differs from the original in {wrong}")
        return {"action": result.get("action"), "version": now.get("version")}

    def _verify_restored(self, api: Api) -> dict[str, Any]:
        flag, _, stored = self.configuration(api)
        serving = sorted({str(item["version_id"]) for item in self.active_deployments(api)})
        original = sorted({str(item["version_id"]) for item in self.original["deployments"]})
        result = {
            "flag": flag == self.original["flag"],
            "routes": sorted(stored) == sorted(self.original.get("routes") or {}),
            "no_registration": not tickets.has_registration(stored),
            "deployments": serving == original,
        }
        wrong = [name for name, ok in result.items() if not ok]
        if wrong:
            raise HarnessError(f"not restored: {wrong}")
        return result

    # registration steps ---------------------------------------------------------------

    def registration_refused_flag_off(self) -> None:
        with (
            self.evidence.step(
                "registration-refused-flag-off",
                "with requester transport off, a selector registration is refused",
            ) as seen,
            self.harness.api() as api,
        ):
            self.changes["flag"] = True
            flag, version = set_requester_flag(api, self.agent_id, False, what="disabling the flag")
            check(flag is False, f"the flag reads {flag!r} after disabling it")
            routes = self.routes(self.registration(api))
            seen["registration"] = tickets.redacted(routes[tickets.TICKET_ROUTE])
            status, body = self.patch(
                api, {"approval_routes": routes, "approval_routes_expected_version": version}
            )
            seen["write"] = {"status": status, "detail": tickets.detail_code(body)}
            check(
                refused_with(status, body, 422, tickets.FLAG_REQUIRED),
                f"registering with the flag off answered {status} {tickets.detail_code(body)!r}, "
                f"not 422 {tickets.FLAG_REQUIRED}",
            )
            after = self.configuration(api)
            seen["after"] = {"flag": after[0], "version": after[1]}
            check(
                after[0] is False
                and after[1] == version
                and not tickets.has_registration(after[2]),
                "the refused registration changed the flag, its version or the routes",
            )

    def registration_bundle_gate_refused(self) -> None:
        with (
            self.evidence.step(
                "registration-bundle-gate-refused",
                "a registration whose serving bundle gate is elsewhere or missing is refused",
            ) as seen,
            self.harness.api() as api,
        ):
            self.changes["flag"] = True
            flag, version = set_requester_flag(api, self.agent_id, True, what="enabling the flag")
            check(flag is True, f"the flag reads {flag!r} after enabling it")
            variants = {
                # The serving bundle gates the pair on the owning route, not this one.
                "gate-points-elsewhere": self.routes(
                    self.registration(api), route=tickets.LOOSER_ROUTE
                ),
                # The serving bundle gates no such tool at all.
                "gate-missing": self.routes(self.registration(api, tool=tickets.OTHER_TOOL)),
            }
            for name, routes in variants.items():
                status, body = self.patch(
                    api, {"approval_routes": routes, "approval_routes_expected_version": version}
                )
                seen[name] = {"status": status, "detail": tickets.detail_code(body)}
                check(
                    refused_with(status, body, 422, tickets.BUNDLE_GATE_MISMATCH),
                    f"{name} answered {status} {tickets.detail_code(body)!r}, not 422 "
                    f"{tickets.BUNDLE_GATE_MISMATCH}",
                )
            after = self.configuration(api)
            seen["after"] = {"flag": after[0], "version": after[1]}
            check(
                after[0] is True and after[1] == version and not tickets.has_registration(after[2]),
                "a refused registration changed the flag, its version or the routes",
            )

    def registration_accepted(self) -> None:
        with (
            self.evidence.step(
                "registration-accepted",
                "a registration with the flag on, a restricted approver role and a gating "
                "bundle is stored, pinned and versioned",
            ) as seen,
            self.harness.api() as api,
        ):
            self.changes["flag"] = True
            flag, version = set_requester_flag(api, self.agent_id, True, what="enabling the flag")
            written = self.registration(api)
            status, body = self.patch(
                api,
                {
                    "approval_routes": self.routes(written),
                    "approval_routes_expected_version": version,
                },
            )
            seen["write"] = {"status": status, "detail": tickets.detail_code(body)}
            check(status == 200, f"the registration answered {status}: {tickets.detail_code(body)}")
            flag_after, version_after, stored = self.configuration(api)
            binding = stored.get(tickets.TICKET_ROUTE) or {}
            readback = binding.get("approver_ticket") or {}
            ceiling = readback.get("ceiling") or {}
            seen["readback"] = tickets.redacted(binding)
            seen["version"] = {"before": version, "after": version_after}
            seen["public_key_read_back"] = (
                readback.get("public_key") == self.key["public_key"]
                if "public_key" in readback
                else None
            )
            check(flag_after is True, "the flag is no longer on")
            check(
                version_after == version + 1,
                f"the configuration version went {version} -> {version_after}, not up by one",
            )
            check(
                all(readback.get(key) == written[key] for key in ("connector", "tool", "key_id")),
                "the read-back registration names another connector, tool or key id",
            )
            check(
                ceiling.get("approver_role_version") == written["ceiling"]["approver_role_version"],
                "the read-back ceiling names another approver role version",
            )
            check(
                ceiling.get("approver_role") is not None
                and self.approver["subject"] in json.dumps(ceiling.get("approver_role")),
                "the read-back ceiling has no pinned approver role naming the approver",
            )
            check(
                binding.get("separation") == {"exclude_requester": True},
                f"the stored separation is {binding.get('separation')!r}, not the default "
                "exclude_requester true",
            )
        require_passed(self.evidence)

    def flag_disable_refused_while_registered(self) -> None:
        with (
            self.evidence.step(
                "flag-disable-refused-while-registered",
                "the requester flag cannot be turned off while a selector is registered",
            ) as seen,
            self.harness.api() as api,
        ):
            flag, version, stored = self.configuration(api)
            check(flag is True and tickets.has_registration(stored), "no registration to defend")
            status, body = self.patch(
                api,
                {
                    "connector_requester_identity": False,
                    "connector_requester_identity_expected_version": version,
                },
            )
            seen["write"] = {"status": status, "detail": tickets.detail_code(body)}
            check(
                refused_with(status, body, 422, tickets.SELECTOR_REGISTERED),
                f"disabling answered {status} {tickets.detail_code(body)!r}, not 422 "
                f"{tickets.SELECTOR_REGISTERED}",
            )
            after = self.configuration(api)
            seen["after"] = {"flag": after[0], "version": after[1]}
            check(
                after[0] is True and after[1] == version and tickets.has_registration(after[2]),
                "the refused disable changed the flag, its version or the registration",
            )

    def concurrent_flag_and_registration(self) -> None:
        """Each round: from (flag on, nothing registered) at version v, a disable and a
        registration both naming v race. Exactly one commits, the version moves to v+1,
        and the stored pair is never (flag off, registration present)."""

        with (
            self.evidence.step(
                "concurrent-flag-and-registration",
                "a racing flag disable and registration at one version: exactly one commits",
            ) as seen,
            self.harness.api() as api,
        ):
            self.changes["flag"] = True
            rounds: list[dict[str, Any]] = []
            seen["rounds"] = rounds
            for index in range(tickets.RACE_ROUNDS):
                flag, version, stored = self.configuration(api)
                if tickets.has_registration(stored):
                    status, body = self.patch(
                        api,
                        {
                            "approval_routes": self.routes(),
                            "approval_routes_expected_version": version,
                        },
                    )
                    check(status == 200, f"removing the registration answered {status}")
                    flag, version, stored = self.configuration(api)
                if flag is not True:
                    self.changes["flag"] = True
                    flag, version = set_requester_flag(
                        api, self.agent_id, True, what="enabling the flag"
                    )
                raced = self.drive(
                    "race_flag",
                    agent_id=self.agent_id,
                    version=version,
                    routes=self.routes(self.registration(api)),
                )["results"]
                flag_after, version_after, stored_after = self.configuration(api)
                registered = tickets.has_registration(stored_after)
                winners = sorted(name for name, result in raced.items() if result["http"] == 200)
                entry = {
                    "round": index + 1,
                    "version": version,
                    "results": raced,
                    "winners": winners,
                    "after": {
                        "flag": flag_after,
                        "version": version_after,
                        "registered": registered,
                    },
                }
                rounds.append(entry)
                check(len(winners) == 1, f"round {index + 1}: {len(winners)} writers committed")
                check(
                    version_after == version + 1,
                    f"round {index + 1}: the version went {version} -> {version_after}",
                )
                check(
                    not (flag_after is False and registered),
                    f"round {index + 1}: the stored state is the flag off with a registration",
                )
                if winners == ["flag_off"]:
                    check(
                        flag_after is False and not registered, "the disable won but did not land"
                    )
                    loser, codes = (
                        raced["register"],
                        (
                            tickets.ROUTES_VERSION_CONFLICT,
                            tickets.FLAG_REQUIRED,
                        ),
                    )
                else:
                    check(
                        flag_after is True and registered, "the registration won but did not land"
                    )
                    loser, codes = (
                        raced["flag_off"],
                        (
                            tickets.FLAG_VERSION_CONFLICT,
                            tickets.SELECTOR_REGISTERED,
                        ),
                    )
                check(
                    loser["http"] in (409, 422)
                    and any(code in str(loser.get("detail")) for code in codes),
                    f"round {index + 1}: the losing write answered {loser}, not one of {codes}",
                )

    def deploy_breaking_gate_refused(self) -> None:
        with (
            self.evidence.step(
                "deploy-breaking-gate-refused",
                "deploying a version whose bundle moves the registered gate is refused",
            ) as seen,
            self.harness.api() as api,
        ):
            self.ensure_registered(api)
            before = sorted(str(item["id"]) for item in self.active_deployments(api))
            version_id, results = self.deploy_version(api, "moved-gate", tickets.LOOSER_ROUTE)
            seen["moved_gate_version"] = version_id
            seen["deployments"] = [
                {"status": status, "detail": tickets.detail_code(body)} for status, body in results
            ]
            check(
                bool(results)
                and all(
                    refused_with(status, body, 422, tickets.BUNDLE_GATE_MISMATCH)
                    for status, body in results
                ),
                f"deploying the moved gate answered {seen['deployments']}, not 422 "
                f"{tickets.BUNDLE_GATE_MISMATCH}",
            )
            after = sorted(str(item["id"]) for item in self.active_deployments(api))
            seen["active_deployments"] = {"before": before, "after": after}
            check(after == before, "a refused deployment changed the active deployments")
            check(
                tickets.has_registration(self.configuration(api)[2]),
                "the registration is gone after the refused deployment",
            )

    # turns and creation steps -----------------------------------------------------------

    def send_turns(self) -> None:
        """Real email turns whose ingress capsules the creation steps use.

        The approver writes first, so they have a canonical principals row; then
        the requester writes once per dedupe key the steps need. No step waits
        for any of these turns' sandboxes: creation reads only the ingress
        capsule and payload in the runs stream.
        """

        with self.harness.api() as api:
            self.ensure_registered(api)
        for label, who in TICKET_TURNS:
            sent = self.harness.send(
                self.agent, self.senders[who], f"approver-ticket-creation: {label} turn"
            )
            response = sent.get("response") if isinstance(sent.get("response"), dict) else {}
            if (
                sent.get("http_status") != 200
                or not response.get("event_id")
                or not response.get("stream_id")
            ):
                raise HarnessError(f"the {label} turn was not enqueued: {send_summary(sent)}")
            self.turns[label] = {
                "event_id": str(response["event_id"]),
                "stream_id": str(response["stream_id"]),
                "sender": self.senders[who],
            }
        captured = self.drive(
            "capture",
            turns={
                label: {"stream_id": turn["stream_id"], "event_id": turn["event_id"]}
                for label, turn in self.turns.items()
            },
        )["turns"]
        for label, who in TICKET_TURNS:
            facts = captured[label]
            wanted = self.requester if who == "requester" else self.approver
            if (
                facts.get("canonical") != wanted
                or not facts.get("principal_present")
                or (facts.get("scope") or {}).get("agent_id") != self.agent_id
            ):
                raise HarnessError(
                    f"the {label} turn's capsule does not name {self.senders[who]} as a "
                    f"verified human on {self.agent}: {facts}"
                )
            self.turns[label]["capsule_facts"] = facts
        rows = self.drive(
            "principal",
            tenant_id=self.tenant_id,
            issuer=self.issuer,
            subject=self.approver["subject"],
        )["rows"]
        self.evidence.data["turns"] = self.turns
        self.evidence.data["approver_principal"] = rows
        if not any(row.get("type") == "human" and row.get("status") == "active" for row in rows):
            raise HarnessError(
                f"{self.senders['approver']} has no active human principals row after their turn"
            )

    def creation_valid(self) -> None:
        with self.evidence.step(
            "creation-valid",
            "a valid ticket creates one dynamic approval through the HTTP creation boundary",
        ) as seen:
            result = self.create(
                seen,
                tickets.valid_cases(),
                count_turns=["valid"],
                count_nonces=["valid"],
                facts_turns=["valid"],
            )
            problems = tickets.case_mismatches(result["cases"])
            check(not problems, "; ".join(problems))
            made = result["cases"][0]
            answer = made["results"][0]
            self.valid = {"id": answer.get("id"), "issued_at": made.get("issued_at")}
            check(answer.get("approval_status") == "pending", f"the approval is {answer}")
            check(seen["rows"]["valid"] == 1, f"{seen['rows']['valid']} rows for the valid turn")
            check(seen["spent_nonces"]["valid"] == 1, "the valid ticket's nonce is not spent once")
            stored = result["facts"][self.turns["valid"]["event_id"]]
            check(len(stored) == 1, f"{len(stored)} rows carry the valid turn's dedupe key")
            row = stored[0]
            seen["row"] = row
            check(row.get("id") == answer.get("id"), "the stored row is not the created approval")
            check(
                row.get("selection_mode") == "dynamic"
                and row.get("selection_key_id") == self.key["key_id"]
                and row.get("selection_connector") == tickets.CONNECTOR
                and row.get("selection_tool") == tickets.TOOL,
                "the row does not record a dynamic selection by this run's key and pair",
            )
            check(
                row.get("route") == tickets.TICKET_ROUTE
                and row.get("gate_kind") == "permission"
                and row.get("granted_tool") == tickets.live_tool(tickets.CONNECTOR, tickets.TOOL),
                "the row does not hold the owning route, the permission gate and the pair",
            )
            check(
                bool(row.get("nonce_stored") and row.get("ticket_stored") and row.get("sealed")),
                "the row lacks its nonce, its stored ticket argument or its seal",
            )
            check(
                self.approver["subject"] in json.dumps(row.get("selected_approvers"))
                and self.requester["subject"] in json.dumps(row.get("selection_requester")),
                "the row does not name the selected approver and the verified requester",
            )
            with self.harness.api() as api:
                status, public = api.call("GET", f"/approvals/{answer.get('id')}")
            shown = public if isinstance(public, dict) else {}
            seen["public"] = {
                "status": status,
                **{
                    key: shown.get(key)
                    for key in ("status", "route", "gate_kind", "granted_tool", "card_channel")
                },
            }
            check(
                status == 200 and shown.get("status") == "pending",
                "the approval does not read back",
            )
            check("identity_context" not in shown, "the public approval exposes its seal")

    def refusal_step(
        self, step_id: str, title: str, cases: Sequence[Mapping[str, Any]], *, turn: str = "refuse"
    ) -> None:
        """Every case refused with its code, no approval row on ``turn``, no nonce spent."""

        with self.evidence.step(step_id, title) as seen:
            check(bool(cases), "the step has no case to send (is the roster missing humans?)")
            used = tickets.nonce_labels(cases)
            result = self.create(seen, cases, count_turns=[turn], count_nonces=used)
            problems = tickets.case_mismatches(result["cases"])
            check(not problems, "; ".join(problems))
            check(seen["rows"][turn] == 0, f"refusals left {seen['rows'][turn]} rows on {turn}")
            spent = sorted(label for label, count in seen["spent_nonces"].items() if count)
            check(not spent, f"a refused ticket spent its nonce: {spent}")

    def creation_nonce_replay(self) -> None:
        with self.evidence.step(
            "creation-nonce-replay",
            "the valid ticket's spent nonce is refused under a second dedupe key, which a fresh "
            "nonce then creates",
        ) as seen:
            check(bool(self.valid.get("id")), "creation-valid spent no nonce to replay")
            result = self.create(
                seen,
                tickets.nonce_replay_cases(),
                count_turns=["refuse"],
                count_nonces=["valid", "refuse-ok"],
            )
            problems = tickets.case_mismatches(result["cases"])
            check(not problems, "; ".join(problems))
            check(
                seen["rows"]["refuse"] == 1,
                f"{seen['rows']['refuse']} rows on the second dedupe key, not only the fresh one",
            )
            check(
                seen["spent_nonces"] == {"valid": 1, "refuse-ok": 1},
                f"spent nonces are {seen['spent_nonces']}",
            )

    def creation_conflicting_dedupe(self) -> None:
        with self.evidence.step(
            "creation-conflicting-dedupe",
            "under the valid approval's dedupe key an identical retry returns the original "
            "receipt and a conflicting one is refused",
        ) as seen:
            check(
                bool(self.valid.get("id")) and self.valid.get("issued_at") is not None,
                "creation-valid created nothing to retry",
            )
            result = self.create(
                seen,
                tickets.conflicting_dedupe_cases(int(self.valid["issued_at"])),
                count_turns=["valid"],
                count_nonces=["valid", "conflict"],
            )
            problems = tickets.case_mismatches(result["cases"])
            check(not problems, "; ".join(problems))
            identical = result["cases"][0]["results"][0]
            check(
                identical.get("id") == self.valid["id"],
                "the identical retry did not return the original approval (its seal over the "
                "stored dynamic row did not verify)",
            )
            check(seen["rows"]["valid"] == 1, f"{seen['rows']['valid']} rows for the valid turn")
            check(
                seen["spent_nonces"] == {"valid": 1, "conflict": 0},
                f"spent nonces are {seen['spent_nonces']}",
            )

    def creation_identical_race(self) -> None:
        with self.evidence.step(
            "creation-identical-race",
            f"{tickets.RACE_POSTS} identical creations at once leave one row and one spent nonce",
        ) as seen:
            result = self.create(
                seen, tickets.race_cases(), count_turns=["race"], count_nonces=["race"]
            )
            race, retry = result["cases"]
            answers = race["results"]
            created = [answer for answer in answers if answer["http"] == 201]
            seen["answers"] = [(answer["http"], answer.get("detail")) for answer in answers]
            check(len(created) == 1, f"{len(created)} identical creations answered 201")
            winner = created[0].get("id")
            odd = [
                answer
                for answer in answers
                if answer is not created[0]
                and not (
                    answer["http"] == 200
                    and answer.get("duplicate") is True
                    and answer.get("id") == winner
                )
                and not (answer["http"] == 503 and answer.get("detail") == tickets.UNAVAILABLE)
            ]
            check(not odd, f"a losing creation answered neither the receipt nor 503: {odd}")
            again = retry["results"][0]
            check(
                again["http"] == 200
                and again.get("duplicate") is True
                and again.get("id") == winner,
                f"the identical retry after the race answered {again}",
            )
            check(seen["rows"]["race"] == 1, f"{seen['rows']['race']} rows for the raced turn")
            check(seen["spent_nonces"]["race"] == 1, "the raced nonce is not spent exactly once")

    def legacy_new_policy_refused(self) -> None:
        with self.evidence.step(
            "legacy-new-policy-refused",
            "POST /approvals cannot create on the registered pair, even with a valid ticket",
        ) as seen:
            dedupe = {
                name: f"identity-e2e-ticket-legacy-{uuid.uuid4().hex[:12]}-{name}"
                for name in tickets.LEGACY_POLICY_NAMES
            }
            result = self.create(
                seen,
                tickets.legacy_policy_cases("bypass", dedupe),
                count_dedupe=list(dedupe.values()),
                count_nonces=["legacy"],
            )
            problems = tickets.case_mismatches(result["cases"])
            check(not problems, "; ".join(problems))
            left = {key: count for key, count in seen["rows"].items() if count}
            check(not left, f"the legacy boundary stored {left}")
            check(seen["spent_nonces"]["legacy"] == 0, "a legacy request spent the ticket's nonce")

    def _control_slack_approval(self) -> tuple[dict[str, Any] | None, str]:
        if self.control is None:
            return None, "no second agent in the harness state (`up --agent A --agent B`)"
        control_id = str(self.harness.agent_entry(self.control)["agent_id"])
        with self.harness.api() as api:
            out = api.ok("GET", f"/agents/{control_id}")
        slack = [item for item in out.get("channels") or [] if item.get("kind") == "slack"]
        if not slack:
            return None, f"{self.control} has no Slack binding for an ordinary legacy approval"
        adapter = slack[0].get("adapter")
        return (
            tickets.slack_ordinary_approval(
                agent_id=control_id,
                channel=str(slack[0]["address"]),
                adapter=None if adapter in (None, "default") else str(adapter),
                dedupe_key=f"identity-e2e-ticket-ordinary-{uuid.uuid4().hex[:12]}",
            ),
            "",
        )

    def legacy_ordinary_unchanged(self) -> None:
        with self.evidence.step(
            "legacy-ordinary-unchanged",
            "ordinary legacy creation answers as before: refused on the HTTP-selected binding, "
            "created and idempotent on a legacy binding",
        ) as seen:
            selected = f"identity-e2e-ticket-selected-{uuid.uuid4().hex[:12]}"
            slack, why = self._control_slack_approval()
            seen["control_agent"] = self.control
            facts = [str(slack["dedupe_key"])] if slack is not None else []
            result = self.create(
                seen,
                tickets.legacy_ordinary_cases(turn="bypass", selected_dedupe=selected, slack=slack),
                count_dedupe=[selected, *facts],
                facts_dedupe=facts,
            )
            problems = tickets.case_mismatches(result["cases"])
            check(not problems, "; ".join(problems))
            check(seen["rows"][selected] == 0, "the refused ordinary request stored a row")
            check(slack is not None, why)
            assert slack is not None
            created = result["cases"][1]["results"][0]
            again = result["cases"][2]["results"][0]
            check(
                created.get("agent_id") == slack["agent_id"]
                and created.get("route") is None
                and created.get("gate_kind") == "permission"
                and created.get("granted_tool") == slack["granted_tool"]
                and created.get("dedupe_key") == slack["dedupe_key"]
                and created.get("approval_status") == "pending",
                f"the ordinary legacy approval reads back as {created}",
            )
            check(again.get("id") == created.get("id"), "the legacy retry made a second approval")
            rows = result["facts"][slack["dedupe_key"]]
            seen["row"] = rows
            check(
                len(rows) == 1
                and rows[0].get("selection_mode") == "ordinary"
                and not rows[0].get("nonce_stored")
                and rows[0].get("selection_key_id") is None,
                "the ordinary legacy row is not stored as an ordinary selection",
            )

    def route_bypass_refused(self) -> None:
        with self.evidence.step(
            "route-bypass-refused",
            "the registered pair cannot be approved off its owning route or without its "
            "ticket, whatever the gate kind; an unrelated tool still can",
        ) as seen:
            refusals: dict[str, Any] = {}
            seen["refusals"] = refusals
            cases = tickets.route_bypass_cases()
            used = tickets.nonce_labels(cases)
            result = self.create(refusals, cases, count_turns=["bypass"], count_nonces=used)
            problems = tickets.case_mismatches(result["cases"])
            check(not problems, "; ".join(problems))
            check(refusals["rows"]["bypass"] == 0, "a bypass attempt stored an approval row")
            spent = sorted(label for label, count in refusals["spent_nonces"].items() if count)
            check(not spent, f"a bypass attempt spent a nonce: {spent}")
            liveness: dict[str, Any] = {}
            seen["liveness"] = liveness
            result = self.create(
                liveness,
                tickets.route_bypass_liveness(),
                count_turns=["bypass"],
                facts_turns=["bypass"],
            )
            problems = tickets.case_mismatches(result["cases"])
            check(not problems, "; ".join(problems))
            rows = result["facts"][self.turns["bypass"]["event_id"]]
            liveness["row"] = rows
            check(
                len(rows) == 1 and rows[0].get("selection_mode") == "ordinary",
                "the unrelated tool's approval is not one ordinary row",
            )

    def cluster_message_negative_control(self) -> None:
        with self.evidence.step(
            "cluster-message-negative-control",
            f"`curie cluster message` to {self.agent} is refused and creates no approval",
        ) as seen:
            before = int(self.drive("count_agent", agent_id=self.agent_id)["count"])
            started = time.time()
            wait = int(min(float(self.args.wait), NEGATIVE_CONTROL_WAIT_S))
            result = self.harness.target.curie(
                "cluster",
                "message",
                "--agent",
                self.agent,
                "--timeout-secs",
                str(wait),
                "--json",
                "approver-ticket-creation: negative control",
                timeout=wait + 120,
            )
            try:
                reply = json.loads(result.stdout)
            except ValueError:
                reply = None
            text = reply.get("reply") if isinstance(reply, dict) else None
            answered = isinstance(text, str) and bool(text.strip())
            tail = [
                line[:200] for line in (result.stderr or result.stdout).strip().splitlines()[-3:]
            ]
            seen["cluster_message"] = {
                "exit": result.returncode,
                "answered": answered,
                "tail": tail,
            }
            deadline = time.monotonic() + NEGATIVE_CONTROL_WAIT_S
            refusals = worker_messages(self.harness, started - CLOCK_SKEW_S, HTTP_REFUSED_MESSAGE)
            while not refusals and result.returncode == 0 and time.monotonic() < deadline:
                time.sleep(3)
                refusals = worker_messages(
                    self.harness, started - CLOCK_SKEW_S, HTTP_REFUSED_MESSAGE
                )
            seen["worker_refusals"] = refusals[:5]
            cli_refused = result.returncode != 0 and any(
                word in " ".join(tail).lower() for word in _REFUSAL_WORDS
            )
            seen["refused_by"] = "worker" if refusals else "ingress" if cli_refused else None
            after = int(self.drive("count_agent", agent_id=self.agent_id)["count"])
            seen["approvals"] = {"before": before, "after": after}
            check(not answered, "curie cluster message got a reply, so the turn ran")
            check(
                bool(refusals) or cli_refused,
                "neither the worker logged `HTTP invocation refused` nor did the CLI report a "
                "refusal",
            )
            check(after == before, f"{after - before} approval rows appeared for {self.agent}")


@scenario(tickets.SCENARIO)
def approver_ticket_creation(harness: Harness, args: Any, evidence: Evidence) -> None:
    """ADR 0271 evidence items 1 and 2 for slice 0271a, on one agent of a live install.

    The agent is ``--flag-agent`` (default: the first agent ``up`` registered);
    ``--sender`` is the requester and ``--second-sender`` the selected approver;
    ``--control-agent`` (default: the other agent) supplies a legacy-evaluable
    Slack binding for the ordinary legacy control.

    Setup, changing nothing: preconditions, the API revision, and a snapshot of
    the flag, the stored route map, the active deployments and the approver
    role. Then it binds two requesting-surface routes, deploys a version of
    examples/weather that gates ``mcp__netpol-probe__search_dashboards`` on the
    owning route (ending the original deployments), and restricts the approver
    role to the roster's active humans by issuer and subject. Registration
    steps go through ``PATCH /agents/{id}`` and ``POST /deployments``; then
    real email turns are sent, and every creation step builds its requests in
    the API pod from a turn's ingress capsule in the runs stream and posts them
    to ``/v1/internal/identity/approvals`` (worker token) or ``/approvals``
    (platform key). Each step records status and refusal codes, row counts and
    spent nonces from Postgres. ``finally`` restores the registration, the
    flag, the deployments, the routes and the role, and a failed restore fails
    the run.

    The item 2 check of a direct execution call without the verified-grant
    header needs the 0271c proxy header and is carried there (plan D16).
    """

    run = TicketScenario(harness, args, evidence)
    evidence.data.update(
        agent=run.agent,
        control_agent=run.control,
        requester=run.senders["requester"],
        approver=run.senders["approver"],
        carried={
            "direct-execution-without-verified-grant-header": "0271c (plan D16)",
        },
    )
    run.prepare()
    try:
        run.install()
        run.registration_refused_flag_off()
        run.registration_bundle_gate_refused()
        run.registration_accepted()
        run.flag_disable_refused_while_registered()
        run.concurrent_flag_and_registration()
        run.deploy_breaking_gate_refused()
        run.send_turns()
        run.creation_valid()
        run.refusal_step(
            "creation-bad-signature",
            "a ticket signed by another key, with a tampered signature, or naming an "
            "unregistered key id is refused",
            tickets.bad_signature_cases(),
        )
        run.refusal_step(
            "creation-scope-change",
            "a ticket for another tenant, agent, connector, tool or route is refused",
            tickets.scope_change_cases(),
        )
        run.refusal_step(
            "creation-requester-change",
            "a ticket naming another human than the turn's verified requester is refused",
            tickets.requester_change_cases(run.others),
        )
        run.refusal_step(
            "creation-argument-change",
            "arguments changed after signing are refused",
            tickets.argument_change_cases(),
        )
        run.refusal_step(
            "creation-expired",
            "an expired or not yet valid preparation is refused",
            tickets.expired_cases(),
        )
        run.creation_nonce_replay()
        run.creation_conflicting_dedupe()
        run.creation_identical_race()
        run.legacy_new_policy_refused()
        run.legacy_ordinary_unchanged()
        run.route_bypass_refused()
        run.cluster_message_negative_control()
    finally:
        run.restore()


# The ADR 0270 slice 2 requester scenarios (requester-effect, requester-expiry,
# requester-approval) live in their own module and register here, last, so
# every name they import from this one already exists.
import identity_scenarios_requester  # noqa: E402,F401
