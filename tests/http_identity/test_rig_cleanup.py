"""The owned transport rig drains an open peer connection on cleanup.

@spec HTTP-IDENTITY-TRANSPORT-4.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any


def test_cleanup_finishes_while_a_peer_connection_is_still_open(run_transport: Any) -> None:
    """@spec HTTP-IDENTITY-TRANSPORT-4: cleanup never waits on a connection it still owns."""
    finished = threading.Event()
    errors: list[BaseException] = []
    # The peer stays referenced, so only the rig can end the connection.
    peers: list[Any] = []

    async def scenario(rig: Any) -> None:
        """@spec HTTP-IDENTITY-TRANSPORT-4: a peer that connects and never sends."""
        listener = await rig.listen()
        host, port = listener.url.split("://", 1)[1].split("/", 1)[0].split(":")
        peers.append(await asyncio.open_connection(host, int(port)))
        await asyncio.wait_for(listener.arrived.wait(), 5)

    def drive() -> None:
        """@spec HTTP-IDENTITY-TRANSPORT-4."""
        try:
            run_transport(scenario)
        except BaseException as exc:  # noqa: BLE001 - reported to the asserting thread.
            errors.append(exc)
        finally:
            finished.set()

    threading.Thread(target=drive, daemon=True).start()
    assert finished.wait(20), "rig cleanup waited forever on a connection it owns"
    assert not errors, errors
