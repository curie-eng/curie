"""Independent reference for the future ADR 0274 API signer, not a route test."""

import base64
import hashlib
import hmac
import json
from pathlib import Path


def reference(case: dict) -> dict[str, str]:
    derivation = (
        f"curie.hook.wake.key.v1:{case['agent_id']}:{case['hook']}:{case['generation']}"
    ).encode("ascii")
    key = (
        base64.urlsafe_b64encode(
            hmac.digest(case["installation_key"].encode("utf-8"), derivation, "sha256")
        )
        .rstrip(b"=")
        .decode("ascii")
    )
    context = json.dumps(
        [case["agent_id"], case["hook"]], ensure_ascii=True, separators=(",", ":")
    ).encode("ascii")
    material = (
        case["prefix"].encode("ascii")
        + b"\n"
        + f"{case['timestamp']}.{case['delivery_id']}.{len(context)}:".encode("ascii")
        + context
        + case["body"].encode("utf-8")
    )
    signature = "sha256=" + hmac.new(key.encode("ascii"), material, hashlib.sha256).hexdigest()
    return {
        "derived_key": key,
        "material_hex": material.hex(),
        "signature": signature,
        "installation_scope": reference_scope(case["installation_key"]),
    }


def reference_scope(installation_key: str) -> str:
    return hmac.new(
        installation_key.encode("utf-8"), b"curie.hook.wake.scope.v1", hashlib.sha256
    ).hexdigest()


def test_hook_wake_signing_reference_vectors() -> None:
    corpus = json.loads(
        (Path(__file__).parents[3] / "tests/vectors/hook-wake-signing.json").read_text()
    )
    for case in corpus["vectors"]:
        assert reference(case) == case["expected"], case["id"]
    original = corpus["vectors"][0]["expected"]
    for case in corpus["negative_cases"]:
        actual = reference(case)
        assert actual == case["expected"], case["id"]
        assert actual["signature"] != original["signature"], case["id"]
    assert corpus["vectors"][1]["timestamp"].startswith("0")
    for case in corpus["scope_vectors"]:
        key = case["installation_key"].encode()
        scope = reference_scope(case["installation_key"])
        assert scope == case["expected_scope"]
        assert scope != hashlib.sha256(key).hexdigest()
        assert hmac.digest(bytes.fromhex(scope), b"example-message", "sha256") != hmac.digest(
            key, b"example-message", "sha256"
        )
        if len(key) > 64:
            assert hmac.digest(
                hashlib.sha256(key).digest(), b"example-message", "sha256"
            ) == hmac.digest(key, b"example-message", "sha256")
