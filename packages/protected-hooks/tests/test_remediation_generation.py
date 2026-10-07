"""The remediation generation in the protected admission records.

@spec AUTOMATED-REMEDIATION-4 @spec AUTOMATED-REMEDIATION-6.

AUTOMATED-REMEDIATION-4 adds the remediation generation active at admission
(or its absence) to the protected admission intent and to the envelope beside
``source_revision``, as ``remediation_generation``: internal transport metadata
versioned with the envelope schema. Its value is a canonical generation string
or ``null`` when no remediation policy is bound. An envelope written before
the field existed still parses (it refuses automatic execution, never the turn),
so the field is accepted absent on read. The caller supplies it on the
``AdmissionRequest`` (``remediation_generation``), read by the ingress under the
agent gate; the facade writes it into the intent and the immutable binding,
which a retried delivery never relabels.

Records are exercised through their real parsers; the facade through the real
atomic admission on the owned disposable TLS broker of ``admission_broker.py``.
"""

from __future__ import annotations

import json

import pytest

from . import admission_broker as broker_helpers
from .admission_broker import (
    AGENT,
    HOOK,
    canonical,
    facade,
    install,
    module,
    queued_payload,
    seed,
    snapshot,
    source_policy,
)
from .test_admission_records import examples

admission_broker = broker_helpers.admission_broker
admission_service = broker_helpers.admission_service

FIELD = "remediation_generation"
BINDING = "protected:admission:binding:event/example"


def _request(records, *, generation, delivery="delivery/example"):
    """The test helper's request, plus the remediation generation, @spec AUTOMATED-REMEDIATION-4."""
    return records.AdmissionRequest(
        identity=records.DeliveryIdentity(agent_id=AGENT, hook=HOOK, delivery_id=delivery),
        source_policy=source_policy(),
        requested_tool_access=None,
        request_body_sha256="a" * 64,
        queued_payload=queued_payload(),
        remediation_generation=generation,
    )


# -- records ----------------------------------------------------------------------------


@pytest.mark.parametrize("kind", ["envelope", "intent", "receipt"])
@pytest.mark.parametrize("value", ["7", "9223372036854775807", None])
def test_the_records_carry_the_remediation_generation(kind, value):
    """Envelope, intent and the receipt derived from it round-trip the field exactly.

    @spec AUTOMATED-REMEDIATION-4.
    """
    r = module("admission_records")
    data = examples()[kind]
    data[FIELD] = value
    parsed = getattr(r, "parse_" + kind)(json.dumps(data, indent=2).encode())
    assert parsed.as_dict() == data
    assert parsed.canonical_bytes == canonical(data)


@pytest.mark.parametrize("kind", ["envelope", "intent"])
def test_a_record_written_before_the_field_still_parses_without_it(kind):
    """A legacy envelope or intent has no field and gains none on read.

    @spec AUTOMATED-REMEDIATION-4 (an envelope missing the field does not refuse the turn).
    """
    r = module("admission_records")
    data = examples()[kind]
    assert FIELD not in data
    parsed = getattr(r, "parse_" + kind)(canonical(data))
    assert parsed.as_dict() == data
    assert parsed.canonical_bytes == canonical(data)


@pytest.mark.parametrize("kind", ["envelope", "intent"])
@pytest.mark.parametrize("bad", ["0", "01", "+1", "-1", "1e2", "9223372036854775808", 7, True, ""])
def test_the_field_follows_the_generation_grammar(kind, bad):
    """@spec AUTOMATED-REMEDIATION-4: a canonical positive generation string or null."""
    r = module("admission_records")
    data = examples()[kind]
    data[FIELD] = bad
    with pytest.raises((ValueError, r.AdmissionUnavailable)):
        getattr(r, "parse_" + kind)(canonical(data))


@pytest.mark.parametrize("value", ["5", None])
def test_the_request_accepts_a_generation_or_none(value):
    """@spec AUTOMATED-REMEDIATION-4."""
    r = module("admission_records")
    request = _request(r, generation=value)
    assert request.remediation_generation == value


@pytest.mark.parametrize("bad", ["0", "01", "-1", 5, True, "five", b"5"])
def test_the_request_refuses_a_generation_outside_the_grammar(bad):
    """@spec AUTOMATED-REMEDIATION-4."""
    r = module("admission_records")
    with pytest.raises(ValueError):
        _request(r, generation=bad)


# -- the facade on the real broker --------------------------------------------------------


def _setup(broker):
    """@spec AUTOMATED-REMEDIATION-4."""
    records = module("admission_records")
    atomic = module("atomic_admission")
    install(broker, module("admission_acl"))
    seed(broker)
    return records, facade(broker, atomic)


@pytest.mark.parametrize("value", ["5", None])
def test_an_admission_writes_the_generation_into_the_intent_and_the_binding(
    admission_broker, value
):
    """The binding and the stream copy carry the request's generation, null included.

    @spec AUTOMATED-REMEDIATION-4.
    """
    b = admission_broker
    r, f = _setup(b)
    req = _request(r, generation=value)

    out = f.admit(req).as_dict()

    assert out["status"] == "accepted", out
    d = r.delivery_digest(req.identity)
    intent = r.parse_intent(b.command("GET", "protected:admission:intent:" + d)).as_dict()
    assert intent[FIELD] == value
    binding = b.command("GET", BINDING)
    envelope = r.parse_envelope(binding).as_dict()
    assert FIELD in envelope and envelope[FIELD] == value
    assert envelope["source_revision"] == source_policy()["generation"]
    ((_, fields),) = b.raw_command("XRANGE", "curie:runs", "-", "+")
    assert fields[3] == binding


def test_a_retry_under_a_new_generation_never_relabels_the_binding(admission_broker):
    """The first admission's generation stays; the retry adds no entry or key.

    A policy write between a delivery and its retry yields the old generation
    or the new one, never a mix: the binding, intent and stream are unchanged.
    @spec AUTOMATED-REMEDIATION-4.
    """
    b = admission_broker
    r, f = _setup(b)
    first = f.admit(_request(r, generation="5")).as_dict()
    assert first["status"] == "accepted", first
    before = snapshot(b)

    retry = f.admit(_request(r, generation="6")).as_dict()

    assert retry["status"] in ("duplicate", "conflict"), retry
    if retry["status"] == "duplicate":
        assert retry["receipt"] == first["receipt"]
    assert snapshot(b) == before
    assert r.parse_envelope(b.command("GET", BINDING)).as_dict()[FIELD] == "5"


def test_the_binding_has_no_expiry(admission_broker):
    """The binding outlives the turn until its submission window closes: no TTL.

    @spec AUTOMATED-REMEDIATION-6 (binding retention).
    """
    b = admission_broker
    r, f = _setup(b)
    assert f.admit(_request(r, generation="5")).as_dict()["status"] == "accepted"
    assert b.command("PTTL", BINDING) == -1
