"""The channel adapter conformance kit (#1516).

A BLACK BOX HTTP driver a third party runs against its own adapter, in its own
repo, to check it against the documented seven rule wire floor. It imports
nothing from the adapter and knows nothing about its language, so the same kit
works on a Go adapter and on the Python reference one with zero edits to either.

Deliberately NOT imported by ``channel_protocol/__init__.py``: the worker and
the API import this package for its wire models, and the kit brings an HTTP
client with it. A contract package that dragged an HTTP client into every
consumer would be paying for a tool none of them run.

Install it as ``curie-channel-protocol[conformance]`` and use it either as a
library::

    from channel_protocol.conformance import run_floor

    def test_my_adapter_meets_the_curie_floor() -> None:
        report = run_floor(adapter, driver=MyDriver(), side_effect_probe=probe)
        assert report.automated_floor == "pass", report.detail()

or as the ``curie-adapter-conformance`` command.
"""

from .driver import IngressDriver, UpstreamIdentity
from .floor import (
    ClauseResult,
    FloorMode,
    FloorReport,
    FloorResult,
    FloorStatus,
    ManualReviewItem,
    run_floor,
)
from .ingress import FakeIngress, ObservedRequest
from .transport import (
    ADAPTER_SECRET_HEADER,
    MAX_ACK_BODY_BYTES,
    AdapterResponse,
    AdapterUnderTest,
    AdapterUnreachableError,
    side_effect_probe,
)

__all__ = [
    "ADAPTER_SECRET_HEADER",
    "MAX_ACK_BODY_BYTES",
    "AdapterResponse",
    "AdapterUnderTest",
    "AdapterUnreachableError",
    "ClauseResult",
    "FakeIngress",
    "FloorMode",
    "FloorReport",
    "FloorResult",
    "FloorStatus",
    "IngressDriver",
    "ManualReviewItem",
    "ObservedRequest",
    "UpstreamIdentity",
    "run_floor",
    "side_effect_probe",
]
