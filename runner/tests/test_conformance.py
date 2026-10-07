"""The frozen ACI conformance suite must pass against the runner's producer."""

from aci_protocol import run_conformance
from curie_runner import conformance_producer


def test_runner_passes_aci_conformance() -> None:
    report = run_conformance(conformance_producer)
    assert report.passed, report.summary() + " :: " + str(
        [(c.name, c.detail) for c in report.checks if not c.passed]
    )
    # The producer-stream check only runs when a producer is supplied; assert it
    # was exercised so a silently-dropped producer can't pass this gate.
    assert any(c.name == "producer_stream" and c.passed for c in report.checks)
