"""The delegate TARGET's offline answer (PROTOTYPE, Draft ADR-0115).

The fake model cannot reason, so a delegated question would otherwise come back
as the canned ``all done`` -- a non-answer that makes the demo look like the
delivery path worked while the answer did not. These pin the deterministic
responder that replaces it on the target side only, and pin that it stays
scoped: an ordinary fake boot must keep ``default_turn()`` verbatim, because
every other fake-model test in this suite depends on it.
"""

from __future__ import annotations

import pytest
from curie_runner.delegate import is_delegate_target_boot
from curie_runner.fake import FakeModelSession, solve_arithmetic

_HISTORY_BASE = "http://api.invalid/agents/a1/state/transcript"


@pytest.mark.parametrize(
    ("question", "expected"),
    [
        ("what is 2 + 2?", "4"),
        ("7 - 9", "-2"),
        ("6 * 7", "42"),
        ("8 / 2", "4"),
        # Non-integer division reports the quotient rather than rounding it away.
        ("7 / 2", "3.5"),
        # Negative operands parse, so a leading '-' is not read as an operator.
        ("-5 + 3", "-2"),
    ],
)
def test_it_solves_a_two_operand_expression(question: str, expected: str) -> None:
    assert solve_arithmetic(question) == expected


@pytest.mark.parametrize(
    "question",
    [
        "how are you?",
        "",
        # Division by zero has no answer, so it must not invent one.
        "1 / 0",
    ],
)
def test_it_declines_what_it_cannot_solve(question: str) -> None:
    assert solve_arithmetic(question) is None


async def _reply(session: FakeModelSession, text: str) -> str:
    await session.query(text)
    replies = [m async for m in session.receive_turn()]
    return str(replies[-1].result)


@pytest.mark.anyio
async def test_a_delegated_question_is_answered() -> None:
    session = FakeModelSession(answer_arithmetic=True)
    assert await _reply(session, "what is 2 + 2?") == "4"


@pytest.mark.anyio
async def test_an_unanswerable_delegated_question_says_idk() -> None:
    """"idk" rather than a fabricated number: the whole point of the responder is
    that the demo's reply text is trustworthy."""

    session = FakeModelSession(answer_arithmetic=True)
    assert await _reply(session, "what is the airspeed velocity of a swallow?") == "idk"


@pytest.mark.anyio
async def test_an_ordinary_fake_boot_is_unchanged() -> None:
    """The scope pin. Flipping this default would rewrite the expected reply of
    every other fake-model test in this suite."""

    session = FakeModelSession()
    assert await _reply(session, "what is 2 + 2?") == "all done"


def test_a_delegate_target_boot_is_recognized_from_the_history_ref() -> None:
    # The router mints the target's conversation id as `delegate:<call id>`, and
    # the worker URL-encodes it into the history ref, so the ':' arrives escaped.
    env = {"CURIE_HISTORY_REF": f"{_HISTORY_BASE}/delegate%3A7f3c-call-id"}
    assert is_delegate_target_boot(env) is True


@pytest.mark.parametrize(
    "ref",
    [
        # An ordinary Slack thread, and the shape most likely to false-positive:
        # a thread whose own key merely CONTAINS the word.
        f"{_HISTORY_BASE}/slack%3AC0EXAMPLE1%3A1712345678.9",
        f"{_HISTORY_BASE}/slack%3Adelegate-talk%3A1712345678.9",
    ],
)
def test_an_ordinary_boot_is_not_a_delegate_target(ref: str) -> None:
    assert is_delegate_target_boot({"CURIE_HISTORY_REF": ref}) is False


def test_an_absent_history_ref_is_not_a_delegate_target() -> None:
    assert is_delegate_target_boot({}) is False
