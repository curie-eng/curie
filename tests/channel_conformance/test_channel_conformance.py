"""Every registered channel adapter against every check its capabilities make applicable.

The matrix is generated at collection time from each registry entry's STATIC
capabilities, so an adapter gets exactly the checks its declaration says it owes
and no test is ever skipped: an inapplicable check is simply not a case. Adding
an adapter is one registry entry in ``subjects.py``; nothing here names one.

Each case opens its subject fresh and runs one check through the worker's real
egress seam (see ``subjects.py``), validating every ingress body against the
platform's own ``TurnIn`` so "the body the adapter posts" and "the body the API
accepts" are the same claim.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
from _support import ALLOWED_SENDER, STRANGER
from channel_protocol.conformance import (
    CHECKS,
    Check,
    CheckContext,
    Upstream,
    applicable_checks,
)
from curie_api.routers.channels import TurnIn
from curie_mail_adapter.adapter import MailAdapter
from subjects import REGISTRY, REGISTRY_CAPABILITIES, MailSubject

# The four adapters #3830 names. A registry that loses one would silently shrink
# the matrix, so the floor is pinned rather than read off the registry itself.
REQUIRED_SUBJECTS = frozenset({"discord", "mail", "github", "http-reply"})


def _validate_turn(body: Mapping[str, Any]) -> None:
    """Refuse a body the platform's ``POST /channels/turns`` would refuse."""

    TurnIn.model_validate(dict(body))


_CASES = [
    pytest.param(name, check, id=f"{name}-{check.name}")
    for name in REGISTRY
    for check in applicable_checks(REGISTRY_CAPABILITIES[name])
]


@pytest.mark.parametrize(("subject_name", "check"), _CASES)
def test_channel_adapter_conforms(subject_name: str, check: Check, tmp_path: Path) -> None:
    async def run() -> None:
        async with REGISTRY[subject_name](tmp_path) as subject:
            await check.run(subject, CheckContext(validate_turn=_validate_turn))

    asyncio.run(run())


def test_every_named_adapter_is_registered() -> None:
    assert set(REGISTRY) >= REQUIRED_SUBJECTS
    assert set(REGISTRY_CAPABILITIES) == set(REGISTRY)


def test_every_check_runs_against_at_least_one_adapter() -> None:
    """A check no registered adapter is held to proves nothing about the fleet.

    One real run is all hollow-check protection needs: the question here is
    only whether the check ever executes against a real adapter. Whether it can
    tell a conforming adapter from a broken one is proven separately, by the
    kit self-tests' broken subjects. A check owed by a single mode (the
    ambiguous-send check is BUFFERED only, and mail is the one buffered
    adapter) is still a real check.
    """

    held_to = {
        check.name: [name for name, caps in REGISTRY_CAPABILITIES.items() if check.applies(caps)]
        for check in CHECKS
    }
    assert {name: subjects for name, subjects in held_to.items() if not subjects} == {}


def test_every_registered_adapter_gets_checks() -> None:
    assert all(applicable_checks(caps) for caps in REGISTRY_CAPABILITIES.values())


@pytest.mark.parametrize("subject_name", sorted(REGISTRY))
def test_declared_capabilities_match_the_running_subject(subject_name: str, tmp_path: Path) -> None:
    """The static declaration that chose the checks is the subject's own.

    For an ingress adapter, the kind it declares is also the kind its real
    ingress posts, so the egress checks address the channel the turns came from.
    """

    declared = REGISTRY_CAPABILITIES[subject_name]

    async def run() -> list[str]:
        async with REGISTRY[subject_name](tmp_path) as subject:
            assert subject.capabilities == declared
            if not declared.ingress:
                return []
            turns = await subject.open_conversation(
                Upstream(id=str(uuid.uuid4()), text="what kind is this turn")
            )
            return [turn.kind for turn in turns]

    kinds = asyncio.run(run())
    if declared.ingress:
        assert kinds, "an ingress adapter turned an upstream message into no turn"
        assert set(kinds) == {declared.kind}


@pytest.mark.parametrize("thread_surface", ["fresh_thread", "historical_thread"])
@pytest.mark.parametrize(
    ("sender", "labels", "headers"),
    [
        (STRANGER, [], None),
        (ALLOWED_SENDER, [], None),
        (ALLOWED_SENDER, ["authenticated", "dmarc_pass", "dkim_pass"], None),
        (
            ALLOWED_SENDER,
            [],
            {
                "Authentication-Results": (
                    "mx.example.com; spf=pass smtp.mailfrom=example.com; "
                    "dkim=pass header.d=example.com; dmarc=pass header.from=example.com"
                ),
                "Received-SPF": "pass",
                "X-AgentMail-Authenticated": "true",
            },
        ),
    ],
    ids=["unlabelled_spoof", "allowlisted_no_verdict", "positive_labels", "forged_headers"],
)
def test_fresh_mail_is_refused_on_retry_and_restart_without_reply_ownership(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    thread_surface: str,
    sender: str,
    labels: list[str],
    headers: dict[str, str] | None,
) -> None:
    # AgentMail exposes arbitrary labels and headers without a trusted aligned
    # authentication verdict or a header provenance guarantee. See:
    # https://docs.agentmail.to/api-reference/inboxes/messages/list
    # https://docs.agentmail.to/api-reference/inboxes/messages/get
    # https://docs.agentmail.to/knowledge-base/inbound-emails-missing
    async def run() -> None:
        async with REGISTRY["mail"](tmp_path) as subject:
            assert isinstance(subject, MailSubject)
            historical_turns = await subject.open_conversation(
                Upstream(id=str(uuid.uuid4()), text="Historical reply still owed")
            )
            assert len(historical_turns) == 1
            historical = historical_turns[0]
            assert historical.body is None
            historical_reply_ref = historical.reply_ref
            assert historical_reply_ref is not None
            adapter = subject._adapter
            assert adapter is not None
            assert adapter.config.ingress_enabled
            historical_delivery = adapter.state.delivery(historical.delivery_id)
            assert historical_delivery is not None
            assert historical_delivery["state"] == "accepted"
            historical_reply = adapter.state.reply_text(
                historical.conversation_id, historical_reply_ref
            )
            assert historical_reply[0]

            message_id = str(uuid.uuid4())
            thread_id = (
                f"thr-{message_id}"
                if thread_surface == "fresh_thread"
                else historical.conversation_id
            )
            summary = subject._mail.add_inbound(
                message_id,
                thread_id,
                sender=sender,
                labels=labels,
                headers=headers,
                text="Fresh mail must not acquire a reply target",
            )
            if headers is not None:
                summary["headers"] = dict(headers)
            assert adapter.state.delivery(message_id) is None
            listing_count = subject._mail.list_calls

            def assert_refused(candidate: MailAdapter) -> None:
                assert candidate.state.delivery(message_id) == {"state": "rejected", "turn": None}
                assert candidate.state.reply_text(thread_id, message_id) == (False, None)
                expected_refs = [] if thread_surface == "fresh_thread" else [historical_reply_ref]
                assert candidate.state.live_reply_refs(thread_id) == expected_refs
                assert candidate.state.live_reply_refs(historical.conversation_id) == [
                    historical_reply_ref
                ]
                assert candidate.state.reply_text(
                    historical.conversation_id, historical_reply_ref
                ) == historical_reply
                assert candidate.state.delivery(historical.delivery_id) == historical_delivery
                assert candidate.state.pending() == []
                assert subject._ingress.requests == []
                assert subject._ingress.resolves == []
                assert subject._mail.body_calls == {}
                assert subject._mail.replies == []
                assert subject.effects() == []

            for _ in range(2):
                assert await asyncio.to_thread(adapter.poll_once) == 200
                assert_refused(adapter)

            await subject.restart()
            replacement = subject._adapter
            assert replacement is not None and replacement is not adapter
            assert_refused(replacement)
            assert await asyncio.to_thread(replacement.poll_once) == 200
            assert_refused(replacement)
            assert subject._mail.list_calls == listing_count + 3

    with caplog.at_level(logging.WARNING, logger="curie_mail_adapter"):
        asyncio.run(run())
    warnings = [
        record.getMessage()
        for record in caplog.records
        if record.name.startswith("curie_mail_adapter") and record.levelno >= logging.WARNING
    ]
    assert len(warnings) == 1
    assert "authentication_unverifiable" in warnings[0]
    assert sender not in warnings[0]
