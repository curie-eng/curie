"""API reader of the frozen derived-bearer vector.

``curie_api.remediation_verifier.header_secret_names`` restates the derived
Authorization header rule for the independence check, beside the renderer's
``_derived_headers`` and the CLI's ``bearer_secret_name``. All three read
``tests/vectors/connector-derived-bearer.json`` so the copies cannot drift:
the name a case expects is the one header secret, and a null expectation
means no header secret at all (which is what ``no_client_auth`` declares).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from curie_api.remediation_verifier import header_secret_names
from plugin_format.connectors import ConnectorSpec

_VECTORS = Path(__file__).resolve().parents[3] / "tests" / "vectors"
_DERIVED_BEARER = json.loads(
    (_VECTORS / "connector-derived-bearer.json").read_text("utf-8")
)


@pytest.mark.parametrize(
    "vector",
    _DERIVED_BEARER["vectors"],
    ids=lambda v: v["name"],
)
def test_header_secret_names_match_the_frozen_vector(vector: dict) -> None:
    connectors = vector["document"]["connectors"]
    assert list(connectors) == ["gh"], vector["name"]
    spec = ConnectorSpec.model_validate(connectors["gh"])
    expected = vector["expected_name"]
    if expected is None:
        assert header_secret_names(spec) == frozenset(), vector["name"]
    else:
        assert header_secret_names(spec) == frozenset({expected}), vector["name"]
