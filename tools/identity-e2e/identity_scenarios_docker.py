"""The requester-handoff proof through the Docker sandbox substrate.

Registers ``requester-handoff-docker`` with the shared scenario registry in
``identity_scenarios``, so ``identity_compose.py prove`` runs it with the same
``Evidence`` recorder and retry rules as the Kubernetes scenarios. A claim
here is a runner container the worker created on the Docker daemon, and its
connector caller token is read from that container's environment.

No caller proxy fronts a local-tier connector, so this scenario never dials
one (no ``[fake:connector-call]``): connector principal observation stays a
Kubernetes-only proof. What it proves is the worker's half: which human each
runner container's caller token names, when the worker replaces a container
for a different human, and that a second human who writes during a live turn
waits for it on a flag-on agent and is steered into it on a flag-off one.

Steps, each needing the one before it:

1. D0 preconditions: the worker runs the Docker substrate with a caller
   signing key, the tenant has its HTTP selection, both bindings are in the
   receiver registration; the flag goes on for the flag agent and off for the
   control agent, each written at its current version.
2. D1 A writes on a new conversation C: a new container whose token names A.
3. D2 B writes on C while idle: a new container, created after B's send, whose
   token names B and differs from A's.
4. D3 A writes on C again: replaced back, names A (and leaves A's container
   warm for D4).
5. D4 A sends a progress demo; one second later B writes (both proofs are
   signed first). The container A's live turn runs on names A. A's reply
   ends with ``all done``; the worker logged ``requester handoff pending``
   (and, with ``--expect-durable-wait``, ``capacity wait parked
   event_id=<B's event> cause=requester_handoff``); B then runs on a new
   container, other than A's, created no earlier than A's reply, whose token
   names B. D3 and D4 run once more on a new conversation when D4 fails only
   with ``deferral not exercised``.
6. D5 the control: on the flag-off agent and a new conversation, a warm turn,
   then A's progress demo and B one second later. Both delivered, A's reply
   says ``all done``, at most one container created after A's send, no
   deferral logged, and B's turn completes before A's. Runs once more when it
   fails only with ``steering not exercised``.
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from identity_e2e import HarnessError
from identity_scenarios import (
    CLOCK_SKEW_S,
    DEFERRAL_NOT_EXERCISED,
    HANDOFF_WAIT_S,
    INTERRUPT_MESSAGE,
    LOG_MESSAGE_LIMIT,
    PENDING_MESSAGE,
    PROGRESS_DEMO,
    PROGRESS_DEMO_ANSWER,
    SECOND_SEND_DELAY_S,
    STEERING_NOT_EXERCISED,
    Evidence,
    StepFailed,
    accepted_event_id,
    answered_progress_demo,
    await_turns,
    check,
    conversation_turn,
    observed_token,
    principal_for,
    require_delivered,
    require_passed,
    require_principal,
    require_replaced,
    retry_once_on,
    scenario,
    send_summary,
    set_requester_flag,
    two_agents,
)

SCENARIO = "requester-handoff-docker"
# The worker's durable wait for a deferred different-human delivery (ADR 0270
# decision 3 remainder). Older workers only retry the delivery.
PARKED_TEMPLATE = "capacity wait parked event_id={event_id} cause=requester_handoff"
CONNECTOR_NOTE = (
    "no caller proxy fronts a local-tier connector, so connector principal observation "
    "is Kubernetes only; this scenario reads the caller token from each runner container"
)


def ends_with_answer(texts: Sequence[Any], answer: str = PROGRESS_DEMO_ANSWER) -> bool:
    """The turn's last non-empty reply text ends with ``answer`` (trailing punctuation aside)."""

    for text in reversed(list(texts)):
        if isinstance(text, str) and text.strip():
            return text.strip().rstrip(".!").lower().endswith(answer)
    return False


def deferral_facts(
    messages: Sequence[str],
    *,
    second_event: str,
    a_reply_at: float,
    b_claim_created: float,
    expect_durable_wait: bool,
) -> dict[str, Any]:
    """The second human met the first's live turn, was deferred, and ran only after it.

    ``messages`` are the worker's log messages since the first human's send.
    Fails when a live turn was interrupted, when nothing records the deferral
    (``deferral not exercised``), when ``expect_durable_wait`` and the durable
    wait for the second human's event was not logged, and when the second
    human's container was created before the first's reply, less the skew.
    The durable wait is always reported, required or not.
    """

    lowered = [str(message).lower() for message in messages]
    interrupted = [m for m, low in zip(messages, lowered, strict=True) if INTERRUPT_MESSAGE in low]
    pending = [m for m, low in zip(messages, lowered, strict=True) if PENDING_MESSAGE in low]
    needle = PARKED_TEMPLATE.format(event_id=second_event)
    parked = [m for m in messages if needle in str(m)]
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
    if expect_durable_wait:
        check(
            bool(parked),
            f"the worker never logged `{needle}`, so the deferred delivery did not park "
            "in a durable wait",
        )
    check(
        b_claim_created >= a_reply_at - CLOCK_SKEW_S,
        f"the second human's container was created at {b_claim_created}, "
        f"{a_reply_at - b_claim_created:.1f} s before the first human's reply at {a_reply_at}",
    )
    return {
        "pending": [str(m)[:LOG_MESSAGE_LIMIT] for m in pending[:5]],
        "pending_count": len(pending),
        "pending_names_second_event": any(second_event in str(m) for m in pending),
        "durable_wait": {
            "expected": expect_durable_wait,
            "seen": bool(parked),
            "line": needle,
            "matches": [str(m)[:LOG_MESSAGE_LIMIT] for m in parked[:3]],
        },
        "messages_read": len(messages),
        "a_reply_at": a_reply_at,
        "b_claim_created": b_claim_created,
        "b_claim_after_a_reply_s": round(b_claim_created - a_reply_at, 1),
    }


def require_container_token(
    observed: Mapping[str, Any], *, agent: str, principal: Any, sent_at: float | None
) -> dict[str, Any]:
    """The observed runner container carries a caller token for ``agent`` naming ``principal``.

    ``principal`` None requires a token with no principal. With ``sent_at``
    the container must have been created after that send, less the skew.
    """

    check(
        bool(observed.get("caller_token_present")),
        f"no runner container of {agent} carries a connector caller token "
        f"(newest container: {observed.get('claim')})",
    )
    claims = observed.get("claims") or {}
    check(claims.get("agent") == agent, f"the token names agent {claims.get('agent')}")
    if principal is None:
        check("principal" not in claims, f"the token carries {claims.get('principal')}")
        named: Any = None
    else:
        named = require_principal(observed, principal)["principal"]
    if sent_at is not None:
        created = observed.get("claim_created_epoch")
        check(
            isinstance(created, (int, float)) and float(created) >= sent_at - CLOCK_SKEW_S,
            f"container {observed.get('claim')} was created at {created}, before the send at "
            f"{sent_at:.1f} (allowing {CLOCK_SKEW_S} s of skew)",
        )
    return {
        "container": observed.get("claim"),
        "created_epoch": observed.get("claim_created_epoch"),
        "fingerprint": observed.get("caller_fingerprint"),
        "principal": named,
        "exp": claims.get("exp"),
    }


def human_turn(
    harness: Any,
    seen: dict[str, Any],
    *,
    agent: str,
    sender: str,
    text: str,
    conversation: str | None,
    principal: Any,
    wait: float,
    fresh_container: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """One human's turn on ``conversation``: delivered, on a container whose token names them."""

    sent, observed, turn = conversation_turn(
        harness,
        seen,
        agent=agent,
        sender=sender,
        text=text,
        conversation=conversation,
        wait=wait,
    )
    seen["caller"] = require_container_token(
        observed,
        agent=agent,
        principal=principal,
        sent_at=float(sent["sent_at"]) if fresh_container else None,
    )
    require_delivered(turn)
    return sent, observed


def send_pair(
    harness: Any,
    seen: dict[str, Any],
    *,
    agent: str,
    first: str,
    second: str,
    conversation: str,
    step_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """The first human starts a progress-demo turn; the second writes one second later.

    Both proofs are signed before either post, so signing in the API container
    does not stretch the gap between the two sends.
    """

    first_turn = harness.prepare(
        agent, first, f"{SCENARIO}: {step_id} {PROGRESS_DEMO}", conversation=conversation
    )
    second_turn = harness.prepare(
        agent, second, f"{SCENARIO}: {step_id} from {second}", conversation=conversation
    )
    first_sent = harness.post(first_turn)
    seen["first_send"] = send_summary(first_sent)
    check(
        first_sent.get("http_status") == 200,
        f"{first}'s turn answered {first_sent.get('http_status')}",
    )
    time.sleep(SECOND_SEND_DELAY_S)
    second_sent = harness.post(second_turn)
    seen["second_send"] = send_summary(second_sent)
    check(
        second_sent.get("http_status") == 200,
        f"{second}'s turn answered {second_sent.get('http_status')}",
    )
    seen["second_send_after_first_s"] = round(
        float(second_sent["sent_at"]) - float(first_sent["sent_at"]), 1
    )
    return first_sent, second_sent


def handoff_step(
    harness: Any,
    evidence: Evidence,
    *,
    step_id: str,
    agent: str,
    conversation: str,
    first: str,
    second: str,
    previous: Mapping[str, Any],
    first_principal: Any,
    principal: Any,
    wait: float,
    expect_durable_wait: bool,
) -> None:
    """D4: the second human writes during the first's live turn on the flag agent.

    ``previous`` is the first human's warm container from D3. The container
    the first human's live turn runs on (the warm one, or a replacement the
    new delivery opened) is read right after the second send, while the
    second human is still deferred, and must name the first human; the
    second human's container must then differ from it.
    """

    with evidence.step(
        step_id, f"{second} writes during {first}'s live turn: deferred, then a new container"
    ) as seen:
        first_sent, second_sent = send_pair(
            harness,
            seen,
            agent=agent,
            first=first,
            second=second,
            conversation=conversation,
            step_id=step_id,
        )
        warm_created = previous.get("claim_created_epoch")
        live = harness.observe(
            agent,
            since=float(warm_created) if isinstance(warm_created, (int, float)) else 0.0,
            wait=30,
        )
        seen["live"] = observed_token(live)
        first_event, second_event = accepted_event_id(first_sent), accepted_event_id(second_sent)
        since = float(first_sent["sent_at"]) - CLOCK_SKEW_S
        turns, _ = await_turns(
            harness, agent, conversation, since, [first_event, second_event], wait
        )
        first_turn, second_turn = turns[first_event], turns[second_event]
        seen["first_turn"], seen["second_turn"] = first_turn, second_turn
        require_delivered(first_turn)
        check(
            ends_with_answer(first_turn["texts"]),
            f"{first}'s reply does not end with `{PROGRESS_DEMO_ANSWER}`, so its turn did not "
            f"finish: {first_turn['texts'][-3:]}",
        )
        check(
            bool(second_turn["completed"]),
            f"{second}'s turn did not complete within {int(wait)} s of {first}'s send",
        )
        require_delivered(second_turn)
        seen["containers_since_first_send"] = [c["name"] for c in harness.claims(agent, since)]
        observed = harness.observe(agent, since=since, wait=60)
        seen["observe"] = observed_token(observed)
        check(
            observed.get("claim_created_epoch") is not None,
            f"no container of {agent} was created since {first}'s send, so an older one "
            f"served {second}",
        )
        messages = [entry["message"] for entry in harness.worker_log(since)]
        seen["deferral"] = deferral_facts(
            messages,
            second_event=second_event,
            a_reply_at=float(first_turn["completed_at"]),
            b_claim_created=float(observed["claim_created_epoch"]),
            expect_durable_wait=expect_durable_wait,
        )
        seen["live_caller"] = require_container_token(
            live, agent=agent, principal=first_principal, sent_at=None
        )
        seen["live_reused_warm"] = live.get("claim") == previous.get("claim")
        seen["replaced"] = require_replaced(live, observed, sent_at=float(second_sent["sent_at"]))
        seen["caller"] = require_container_token(
            observed, agent=agent, principal=principal, sent_at=float(second_sent["sent_at"])
        )


def control_step(
    harness: Any,
    evidence: Evidence,
    *,
    step_id: str,
    agent: str,
    first: str,
    second: str,
    wait: float,
) -> None:
    """D5: the same two humans on the flag-off agent are steered into one live turn."""

    with evidence.step(
        step_id, f"control, flag off: {second} during {first}'s live turn is steered"
    ) as seen:
        warm: dict[str, Any] = {}
        seen["warm"] = warm
        sent, warm_observed = human_turn(
            harness,
            warm,
            agent=agent,
            sender=first,
            text=f"{SCENARIO}: {step_id} warm",
            conversation=None,
            principal=None,
            wait=wait,
        )
        conversation = str(sent["conversation_id"])
        first_sent, second_sent = send_pair(
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
        for who, done in ((first, first_turn), (second, second_turn)):
            check(
                bool(done["completed"]) and done["outcome"] == "delivered",
                f"{who}'s turn ended {done['outcome']!r} (completed: {done['completed']}), "
                "not delivered",
            )
        check(
            answered_progress_demo(first_turn["texts"]),
            f"{first}'s progress-demo turn never answered `{PROGRESS_DEMO_ANSWER}`",
        )
        containers = [c["name"] for c in harness.claims(agent, since)]
        seen["containers_since_first_send"] = containers
        check(
            len(containers) <= 1,
            f"{second} got a container of their own on a flag-off agent; containers since "
            f"{first}'s send: {containers}",
        )
        served = harness.observe(agent, since=float(sent["sent_at"]) - CLOCK_SKEW_S, wait=60)
        seen["served"] = observed_token(served)
        serving = containers[0] if containers else warm_observed.get("claim")
        check(
            served.get("claim") == serving,
            f"the container serving the live turn is {serving}, but the newest is "
            f"{served.get('claim')}",
        )
        require_container_token(served, agent=agent, principal=None, sent_at=None)
        pending = [
            entry["message"][:LOG_MESSAGE_LIMIT]
            for entry in harness.worker_log(since)
            if PENDING_MESSAGE in entry["message"].lower()
        ]
        seen["pending"] = pending
        check(not pending, f"a flag-off agent deferred a delivery: {pending[:3]}")
        steered = (
            second_turn["position"] is not None
            and first_turn["position"] is not None
            and second_turn["position"] < first_turn["position"]
        )
        seen["steered"] = steered
        check(
            steered,
            f"{STEERING_NOT_EXERCISED}: {second}'s turn completed after {first}'s, so it never "
            "met the live turn",
        )


def require_stack_ready(harness: Any, seen: dict[str, Any], agents: Iterable[str]) -> None:
    """The worker runs the Docker substrate with a signing key; HTTP identity is wired."""

    facts = harness.worker_facts()
    seen["stack"] = facts
    check(facts["substrate"] == "docker", f"the worker substrate is {facts['substrate']!r}")
    check(facts["caller_signing_key_set"], "the worker has no CURIE_CONNECTOR_CALLER_SIGNING_KEY")
    check(
        bool(facts["api_http_identity_dir"]),
        "the API has no CURIE_HTTP_IDENTITY_DIR; rerun `up`",
    )
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


@scenario(SCENARIO)
def requester_handoff_docker(harness: Any, args: Any, evidence: Evidence) -> None:
    """ADR 0270 acceptance item 2 on the Docker substrate adapter."""

    if getattr(harness, "substrate", None) != "docker":
        raise HarnessError(f"{SCENARIO} runs through identity_compose.py, not the cluster harness")
    flag_agent, control_agent = two_agents(harness, args)
    first, second = args.sender.lower(), args.second_sender.lower()
    if first == second:
        raise HarnessError(f"{SCENARIO} needs two humans; --second-sender equals --sender")
    principal_a, principal_b = principal_for(harness, first), principal_for(harness, second)
    expect_durable_wait = bool(getattr(args, "expect_durable_wait", False))
    evidence.data.update(
        substrate="docker",
        project=harness.target.project,
        images=harness.data.get("images"),
        flag_agent=flag_agent,
        control_agent=control_agent,
        sender=first,
        second_sender=second,
        expected_principals={first: principal_a, second: principal_b},
        expect_durable_wait=expect_durable_wait,
        connector_observation=CONNECTOR_NOTE,
    )
    wait = float(args.wait)
    handoff_wait = max(wait, HANDOFF_WAIT_S)

    with (
        evidence.step(
            "D0", "preconditions: docker substrate, signing key, flag on and off"
        ) as seen,
        harness.api() as api,
    ):
        require_stack_ready(harness, seen, (flag_agent, control_agent))
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
        "D1", f"{first} on a new conversation: a new container whose token names {first}"
    ) as seen:
        sent, first_claim = human_turn(
            harness,
            seen,
            agent=flag_agent,
            sender=first,
            text=f"{SCENARIO}: D1",
            conversation=None,
            principal=principal_a,
            wait=wait,
        )
        conversation = str(sent["conversation_id"])
    require_passed(evidence)

    last: dict[str, Any] = {}
    with evidence.step(
        "D2", f"{second} on the same conversation while idle: a new container names {second}"
    ) as seen:
        sent, last = human_turn(
            harness,
            seen,
            agent=flag_agent,
            sender=second,
            text=f"{SCENARIO}: D2",
            conversation=conversation,
            principal=principal_b,
            wait=wait,
        )
        seen["replaced"] = require_replaced(first_claim, last, sent_at=float(sent["sent_at"]))
    require_passed(evidence)

    def handoff_attempt(suffix: str) -> None:
        nonlocal last
        fresh = bool(suffix)
        title = (
            f"{first} on a new conversation: a new container names {first}"
            if fresh
            else f"{first} again: the container is replaced back and names {first}"
        )
        warm: dict[str, Any] = {}
        topic = conversation
        with evidence.step(f"D3{suffix}", title) as seen:
            sent, warm = human_turn(
                harness,
                seen,
                agent=flag_agent,
                sender=first,
                text=f"{SCENARIO}: D3{suffix}",
                conversation=None if fresh else conversation,
                principal=principal_a,
                wait=wait,
            )
            seen["replaced"] = require_replaced(last, warm, sent_at=float(sent["sent_at"]))
            topic = str(sent["conversation_id"])
        require_passed(evidence)
        last = warm
        handoff_step(
            harness,
            evidence,
            step_id=f"D4{suffix}",
            agent=flag_agent,
            conversation=topic,
            first=first,
            second=second,
            previous=warm,
            first_principal=principal_a,
            principal=principal_b,
            wait=handoff_wait,
            expect_durable_wait=expect_durable_wait,
        )

    retry_once_on(evidence, DEFERRAL_NOT_EXERCISED, handoff_attempt)

    retry_once_on(
        evidence,
        STEERING_NOT_EXERCISED,
        lambda suffix: control_step(
            harness,
            evidence,
            step_id=f"D5{suffix}",
            agent=control_agent,
            first=first,
            second=second,
            wait=wait,
        ),
    )


__all__ = [
    "SCENARIO",
    "StepFailed",
    "deferral_facts",
    "ends_with_answer",
    "requester_handoff_docker",
    "require_container_token",
]
