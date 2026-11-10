"""The one canonical form of a connector call's arguments (ACTION-EXECUTOR-7).

@spec ACTION-EXECUTOR-7. Sorted keys, ``,`` and ``:`` separators,
``ensure_ascii=False``, UTF-8. The proxy re-canonicalizes the arguments it
forwards and compares them with a ``ccg`` grant's ``args``; the worker mints
that grant over this same text. Both import this module, so the two can never
disagree on any input. It lives in the proxy package because the proxy may not
import ``curie_worker`` (its ``__init__`` loads the kernel), while the worker
may import this dependency-free module.

NaN and the infinities are refused: JSON has no exact form for them, and
Python's default ``json.dumps`` would write the non-JSON tokens ``NaN`` and
``Infinity`` that another image's parser reads differently or not at all.
``tests/vectors/action-canonical-arguments.json`` freezes the bytes.
"""

from __future__ import annotations

import json
from typing import Any


def canonical_arguments(arguments: Any) -> str:
    """The canonical text of ``arguments``.

    Raises ``TypeError`` when ``arguments`` is not a JSON object or holds a value
    JSON cannot carry, and ``ValueError`` for NaN or an infinity at any depth.
    """

    if not isinstance(arguments, dict):
        raise TypeError("connector call arguments must be a JSON object")
    return json.dumps(
        arguments,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
