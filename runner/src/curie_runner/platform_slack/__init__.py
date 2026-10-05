"""The platform ``curie-slack`` server's SDK-free parts (ADR 0100, #2877).

``capability`` holds the per-turn channel read credential, ``reads`` holds the
read tool operations that present it to the platform route, and ``retention``
keeps retrieved message bodies out of the persisted turn record. The SDK
binding lives in ``curie_runner.harness.claude.platform_slack`` because only
the Claude harness package may import the SDK (ADR 0140). A canvas reader
(#3819) adds a sibling of ``reads`` rather than growing it.
"""

from .capability import ChannelReadTurn

__all__ = ["ChannelReadTurn"]
