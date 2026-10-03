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
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
from channel_protocol.conformance import (
    CHECKS,
    Check,
    CheckContext,
    Upstream,
    applicable_checks,
)
from curie_api.routers.channels import TurnIn
from subjects import REGISTRY, REGISTRY_CAPABILITIES

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
