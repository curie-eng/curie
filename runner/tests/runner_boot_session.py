"""Run a runner's boot initialization before inspecting the session it connects.

Since #4777 a runner's boot initialization (the verification preflight, the
system prompt and the SDK options) runs when the runner starts, not inside
``build_runner``. A test that inspects the session the runner would connect
must therefore run that initialization first, which is what these helpers do.
"""

from __future__ import annotations

from typing import Any

import anyio


async def initialized_session(runner: Any) -> Any:
    """Run the runner's boot initializer, then build the session it would connect."""

    initializer = runner._initializer  # noqa: SLF001 -- the boot wiring is the subject
    if initializer is not None:
        await initializer()
    return runner._factory()  # noqa: SLF001 -- the boot wiring is the subject


def initialized_session_sync(runner: Any) -> Any:
    """``initialized_session`` for synchronous tests."""

    return anyio.run(initialized_session, runner)
