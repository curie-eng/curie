"""Signed commissioning ingress harness on the owned TLS Valkey.

@spec PROTECTED-COMMISSIONING-2/4.

The shared ``commissioning_tuple_broker`` of ``packages/protected-hooks/tests``
builds the tuple, installs its principals and writes the provisioner-owned
directory; this harness adds deliveries signed with each synthetic source's
secret. Fixture IDs and secrets are placeholders; no real deployment data.
"""

from __future__ import annotations

import importlib
import sys
import time
import types
import uuid
from pathlib import Path
from typing import Any

from curie_api import hook_signing

_name = "commissioning_ingress_broker_tests"
if _name not in sys.modules:
    _package = types.ModuleType(_name)
    _package.__path__ = [
        str(Path(__file__).resolve().parents[3] / "packages/protected-hooks/tests")
    ]
    sys.modules[_name] = _package
wb = importlib.import_module(_name + ".worker_broker")
_tuple = importlib.import_module(_name + ".commissioning_tuple_broker")

HOOK = _tuple.HOOK
GuardProbe = _tuple.GuardProbe
canonical = _tuple.canonical
connections = _tuple.connections
snapshot = _tuple.snapshot


class CommissioningTuple(_tuple.CommissioningTuple):
    """The shared tuple plus signed synthetic deliveries for the ingress."""

    def signed(
        self,
        body: bytes = b'{"alert":"example"}',
        *,
        source: int = 0,
        delivery_id: str | None = None,
        secret: str | None = None,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        delivery = delivery_id or str(uuid.uuid4())
        stamp = timestamp or str(int(time.time()))
        signature = hook_signing.sign(
            secret or self.sources[source]["secret"],
            timestamp=stamp,
            delivery_id=delivery,
            hook=HOOK,
            tool_access="read-only",
            body=body,
        )
        return {
            "source_id": self.sources[source]["source"]["source_id"],
            "body": body,
            "timestamp": stamp,
            "delivery_id": delivery,
            "signature": signature,
        }


pworker_service = wb.pworker_service
