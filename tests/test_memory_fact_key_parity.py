"""ADR-0188 (#3623): the fact-key shape and the no-person marker, held in three places.

The runner's memory tools mint fact keys matching ``memory_facts._FACT_ID``.
The state API lets a sandbox credential write only keys matching
``routers.state._FACT_KEY``; a drift would make every tool write a 403, or let a
sandbox write keys the tools never mint (``guidance``, ``log``). The worker
stamps ``NO_PERSON`` as the ``sender`` claim of a turn with no person behind
it, and the runner renders that same string as "no author". None of the three
may import another, so this repo-level test pins them.
"""

from __future__ import annotations

import re

_CASES = (
    ("fact-" + "0123456789abcdef" * 2, True),
    ("fact-" + "0" * 32, True),
    ("fact-" + "0" * 31, False),
    ("fact-" + "0" * 33, False),
    ("fact-" + "A" * 32, False),
    ("fact-" + "g" * 32, False),
    ("xfact-" + "0" * 32, False),
    ("guidance", False),
    ("log", False),
    ("", False),
)


def test_api_fact_key_pattern_is_the_runner_fact_id_pattern() -> None:
    from curie_api.routers.state import _FACT_KEY
    from curie_runner.memory_facts import _FACT_ID

    assert isinstance(_FACT_KEY, re.Pattern)
    assert _FACT_KEY.pattern == _FACT_ID.pattern
    assert _FACT_KEY.flags == _FACT_ID.flags
    for key, expected in _CASES:
        assert (_FACT_KEY.match(key) is not None) is expected, key
        assert (_FACT_ID.match(key) is not None) is expected, key


def test_worker_and_runner_no_person_marker_are_identical() -> None:
    from curie_runner.memory_facts import NO_PERSON as RUNNER_NO_PERSON
    from curie_worker.binding import NO_PERSON as WORKER_NO_PERSON

    assert RUNNER_NO_PERSON == "<no person>"
    assert WORKER_NO_PERSON.encode("utf-8") == RUNNER_NO_PERSON.encode("utf-8")
