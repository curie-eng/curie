"""Upload credentials follow the bound agent and the bundle's connector allowlist."""

from __future__ import annotations

import base64
import json
import time
from pathlib import Path

import pytest
from curie_connector_proxy import caller
from curie_internal.connector_endpoints import ConnectorEndpointBinding
from curie_worker.connector_uploads import mint_claim_upload_tokens
from nacl.signing import SigningKey

SEED = base64.b64encode(bytes(range(32))).decode()
KEYS = (SigningKey(bytes(range(32))).verify_key,)


def _binding(*, uploads: bool = True, agent: str = "acme-dev") -> ConnectorEndpointBinding:
    return ConnectorEndpointBinding.model_validate(
        {
            "version": 1,
            "agent": agent,
            "connectors": {
                name: {
                    "base_url": "https://connectors.example.com/" + name,
                    **({"upload_url": "https://uploads.example.com/" + name} if uploads else {}),
                }
                for name in ("reviewer", "private", "undeclared")
            },
        }
    )


def _bundle(root: Path) -> Path:
    (root / "connectors.yaml").write_text(
        "connectors:\n  reviewer:\n    image: acme/reviewer:1\n"
        "  private:\n    image: acme/private:1\n"
    )
    (root / "deploy.yaml").write_text(
        "targets:\n  dev:\n    agent: acme-dev\n    connectors: [reviewer]\n"
        "  prod:\n    agent: acme-prod\n    env: prod\n    connectors: [private]\n"
    )
    return root


def test_only_this_agents_declared_upload_gets_a_one_hour_token(tmp_path: Path) -> None:
    before = int(time.time())
    tokens = mint_claim_upload_tokens(SEED, bundle=_bundle(tmp_path), endpoints=_binding())
    after = int(time.time())
    assert set(tokens) == {"reviewer"}
    token = tokens["reviewer"]
    payload = json.loads(base64.urlsafe_b64decode(token.split(".")[1] + "=="))
    assert set(payload) == {"agent", "connector", "exp"}
    assert before < payload["exp"] <= after + 3600
    assert payload["exp"] - before <= 3600
    assert caller.decide_upload(
        KEYS, token, connector="reviewer", admits=frozenset({"acme-dev"}), now=after
    ).admitted
    for connector, agent, now, refusal in (
        ("private", "acme-dev", after, caller.INVALID),
        ("reviewer", "acme-prod", after, caller.NOT_ADMITTED),
        ("reviewer", "acme-dev", payload["exp"], caller.EXPIRED),
    ):
        assert caller.decide_upload(
            KEYS, token, connector=connector, admits=frozenset({agent}), now=now
        ).refusal == refusal


def test_no_upload_listener_needs_no_key_or_bundle(tmp_path: Path) -> None:
    assert mint_claim_upload_tokens("", bundle=None, endpoints=None) == {}
    assert mint_claim_upload_tokens("", bundle=tmp_path, endpoints=_binding(uploads=False)) == {}


def test_an_upload_listener_requires_a_key_and_bundle(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="signing key"):
        mint_claim_upload_tokens("", bundle=_bundle(tmp_path), endpoints=_binding())
    with pytest.raises(ValueError, match="bundle"):
        mint_claim_upload_tokens(SEED, bundle=None, endpoints=_binding())


@pytest.mark.parametrize("name", ["connectors.yaml", "deploy.yaml"])
@pytest.mark.parametrize("contents", [None, "[invalid", "[]"])
def test_required_bundle_inputs_fail_closed(
    tmp_path: Path, name: str, contents: str | None
) -> None:
    bundle = _bundle(tmp_path)
    path = bundle / name
    if contents is None:
        if name == "deploy.yaml":
            path.write_bytes(b"\xff")
        else:
            path.unlink()
    else:
        path.write_text(contents)
    with pytest.raises(ValueError, match="upload"):
        mint_claim_upload_tokens(SEED, bundle=bundle, endpoints=_binding())


def test_absent_deploy_file_allows_declared_hosted_connectors(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    (bundle / "deploy.yaml").unlink()
    assert set(mint_claim_upload_tokens(SEED, bundle=bundle, endpoints=_binding())) == {
        "reviewer", "private"
    }
