"""Scripted scenarios for `curie dev identity-e2e -- prove --scenario <name>`.

A scenario is a function registered with ``@scenario("<name>")``. It receives
the loaded :class:`identity_e2e.Harness`, the parsed ``prove`` arguments and an
:class:`Evidence` recorder, and runs its steps inside ``evidence.step(...)``
blocks. Every assertion is a :func:`check` that raises :class:`StepFailed`;
a failed step is recorded and the scenario continues, so one run reports every
step. ``prove`` exits nonzero unless every recorded step passed.

Each step drives the install through the same boundaries an operator or a
mail adapter uses (``Harness.send``, ``Harness.observe``, the platform API and
the ``curie`` CLI), and reads what the product logged (the caller proxy's
decisions through ``Harness.proxy_decisions``). Direct SQL is reserved for
deliberately corrupting stored state that no product surface can write, and
every such change is restored in a ``finally`` block.
"""

from __future__ import annotations

import contextlib
import json
import re
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any

from identity_e2e import Api, Harness, HarnessError, turn_summary

# The fake model's test-only directive: the turn dials every hosted connector
# of the agent through the runner's caller header path and answers one line
# per connector (runner/src/curie_runner/fake.py).
CONNECTOR_CALL = "[fake:connector-call]"
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


def two_agents(harness: Harness, args: Any) -> tuple[str, str]:
    names = list(harness.data.get("agents", {}))
    flag = args.flag_agent or (names[0] if names else None)
    control = args.control_agent or next((n for n in names if n != flag), None)
    if flag is None or control is None or flag == control:
        raise HarnessError("this scenario needs two distinct agents; run `up --agent A --agent B`")
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


def requester_flag(api: Api, agent_id: str) -> tuple[Any, Any]:
    """The flag and its version as AgentOut reports them."""

    out = api.ok("GET", f"/agents/{agent_id}")
    check(
        "connector_requester_identity" in out and "connector_requester_identity_version" in out,
        "AgentOut has no connector_requester_identity fields; is the candidate installed?",
    )
    return out["connector_requester_identity"], out["connector_requester_identity_version"]


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
        check(
            bool(observed.get("caller_token_present")),
            "no connector caller token reached the claim",
        )
        claims = observed.get("claims") or {}
        check(claims.get("agent") == agent, f"the token names agent {claims.get('agent')}")
        if expected_principal is None:
            check("principal" not in claims, f"the token carries {claims.get('principal')}")
        else:
            check(
                claims.get("principal") == expected_principal,
                f"the token principal is {claims.get('principal')}, expected {expected_principal}",
            )
        turn = observed.get("turn") or {}
        require_delivered(turn)
        connectors = harness.connectors(agent)
        seen["connectors"] = connectors
        decisions = harness.proxy_decisions(agent, since=started - 2)
        seen["decisions"] = decisions
        seen["connector_call"] = require_connector_admitted(
            turn.get("texts") or [],
            decisions,
            agent=agent,
            connectors=connectors,
            principal=expected_principal is not None,
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
    """Worker log messages containing ``needle`` since ``since`` (unix seconds)."""

    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(since))
    result = harness.target.kubectl(
        "logs",
        f"deployment/{harness.target.component('worker')}",
        f"--since-time={stamp}",
        check=False,
    )
    messages = []
    for line in result.stdout.splitlines():
        try:
            message = str(json.loads(line).get("message", ""))
        except ValueError:
            continue
        if needle in message.lower():
            messages.append(message[:300])
    return messages


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
    subject = roster_subject(harness, sender)
    expected_principal = {
        "issuer": harness.data["issuer"],
        "subject": subject,
        "tenant_id": harness.data["tenant_id"],
    }
    evidence.data.update(
        flag_agent=flag_agent,
        control_agent=control_agent,
        sender=sender,
        expected_principal=expected_principal,
    )
    registered = set(harness.data.get("registration", {}).get("binding_ids", ()))
    enabled_version: int | None = None

    with (
        evidence.step(
            "0", "preconditions: HTTP selection, registered bindings, hosted connectors"
        ) as seen,
        harness.api() as api,
    ):
        rows = harness.target.sql(
            (
                "SELECT http_selection IS NOT NULL AS selected FROM curie.tenant_identity_providers"
                " WHERE tenant_id::text = :t",
                {"t": harness.data["tenant_id"]},
            )
        )[0]
        seen["tenant_http_selection"] = bool(rows and rows[0]["selected"])
        check(seen["tenant_http_selection"], "the tenant has no HTTP selection; rerun `up`")
        for name in (flag_agent, control_agent):
            check(
                harness.agent_entry(name)["binding_id"] in registered,
                f"{name}'s email binding is not in the receiver registration",
            )
        hosted = {name: harness.connectors(name) for name in (flag_agent, control_agent)}
        seen["connectors"] = hosted
        for name, connectors in hosted.items():
            check(
                bool(connectors),
                f"{name} has no hosted connector, so no turn can present its token to one; "
                "deploy the agents with `up --bundle examples/weather`",
            )
        control_flag, control_version = requester_flag(api, control_id)
        if control_flag:
            status, body = api.call(
                "PATCH",
                f"/agents/{control_id}",
                {
                    "connector_requester_identity": False,
                    "connector_requester_identity_expected_version": control_version,
                },
            )
            check(status == 200, f"disabling the control agent answered {status}: {body}")
            control_flag, control_version = requester_flag(api, control_id)
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
