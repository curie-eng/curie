"""Closed execution-config integrity, @spec PROTECTED-HOOK-LANE-6/7.

These pure codec tests do not establish running configuration or qualification.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
from dataclasses import FrozenInstanceError

import pytest

from .protected_configuration_vectors import canonical, execution_profile, manifest_for


def configuration():
    """Absent product is a behavioral RED, @spec PROTECTED-HOOK-LANE-6/7."""
    name = "curie_worker.protected_configuration"
    assert importlib.util.find_spec(name) is not None, "protected_configuration module absent"
    product = importlib.import_module(name)
    for member in ("parse_execution_config", "ProtectedExecutionConfig", "ExecutionConfigInvalid"):
        assert hasattr(product, member), f"execution configuration surface absent: {member}"
    return product


def test_exact_digest_canonical_copy_out_immutable_and_redacted():
    """@spec PROTECTED-HOOK-LANE-6/7."""
    product = configuration()
    value = execution_profile()
    parsed = product.parse_execution_config(
        json.dumps(value, indent=2).encode(), manifest=manifest_for(value)
    )
    assert isinstance(parsed, product.ProtectedExecutionConfig)
    assert parsed.canonical_bytes == canonical(value)
    assert parsed.digest == hashlib.sha256(canonical(value)).hexdigest()
    assert parsed.as_dict() == value
    copied = parsed.as_dict()
    copied["runner"]["model"] = "ambient-model"
    copied["bindings"]["history_signing_credential_ref"]["generation"] = "2"
    assert parsed.as_dict() == value
    with pytest.raises((FrozenInstanceError, AttributeError, TypeError)):
        parsed.canonical_bytes = b"{}"
    assert value["runner"]["model"] not in repr(parsed)
    assert "credential/example" not in repr(parsed)
    assert "api.example.com" not in repr(parsed)


@pytest.mark.parametrize(
    "corruption",
    ["extra", "nested_extra", "missing", "duplicate", "bool", "float", "nonfinite", "utf8", "size"],
)
def test_closed_grammar_refuses_structural_ambiguity_without_echo(corruption):
    """@spec PROTECTED-HOOK-LANE-6/7."""
    product = configuration()
    value = execution_profile()
    manifest = manifest_for(value)
    if corruption == "extra":
        value["secret"] = "private-marker"
    elif corruption == "nested_extra":
        value["runner"]["env"] = {"EXAMPLE_KEY": "private-marker"}
    elif corruption == "missing":
        del value["runner"]["model"]
    elif corruption == "bool":
        value["runner"]["max_turns"] = True
    elif corruption == "float":
        value["runner"]["budget"]["max_usd_per_day"] = 10.25
    raw = canonical(value)
    if corruption == "duplicate":
        raw = raw.replace(
            b'"model":"example-model"', b'"model":"private-marker","model":"example-model"'
        )
    elif corruption == "nonfinite":
        raw = b'{"schema_version":NaN,"secret":"private-marker"}'
    elif corruption == "utf8":
        raw = b"\xff"
    elif corruption == "size":
        raw += b" " * 65537
    with pytest.raises(product.ExecutionConfigInvalid) as caught:
        product.parse_execution_config(raw, manifest=manifest)
    assert "private-marker" not in str(caught.value)
    assert "private-marker" not in repr(caught.value)


@pytest.mark.parametrize(
    "usd", ["0", "01", "1.0", "10.2500", "1e2", "-1", "NaN", "1000000.000001", "0.0000001"]
)
def test_usd_requires_positive_bounded_canonical_decimal(usd):
    """Self-consistent digest cannot validate bad semantics, @spec PROTECTED-HOOK-LANE-6/7."""
    product = configuration()
    value = execution_profile()
    value["runner"]["budget"]["max_usd_per_day"] = usd
    with pytest.raises(product.ExecutionConfigInvalid):
        product.parse_execution_config(canonical(value), manifest=manifest_for(value))


@pytest.mark.parametrize(
    "path,bad",
    [
        (("policy", "tool_access"), "full"),
        (("policy", "lifecycle_hooks"), True),
        (("policy", "memory"), True),
        (("policy", "approvals"), True),
        (("runner", "harness"), "custom-shell"),
        (("runner", "thinking"), "off"),
        (("runner", "model"), ""),
        (("runner", "model"), " model "),
        (("runner", "reviewer_model"), "réviewer"),
        (("runner", "model"), "\ud800"),
        (("runner", "reviewer_model"), ""),
        (("runner", "plugin_dir"), "/tmp/other"),
        (("runner", "fake_model"), True),
        (("runner", "false_completion_check"), True),
        (("runner", "thinking"), "low"),
        (("runner", "api_backend"), "responses"),
        (("runner", "model_base_url"), "https://user:password@example.com"),
        (("runner", "model_base_url"), "http://model.example.com"),
        (("runner", "model_base_url"), "https://model.example.com?tenant=private"),
        (("runner", "model_base_url"), "https://model.example.com:bad"),
        (("runner", "model_base_url"), "https://MODEL.example.com"),
        (("runner", "model_base_url"), "https://model.example.com/"),
        (("launch", "init_image_digests"), ["sha256:" + "a" * 64]),
        (("bindings", "qualified_connectors"), [{"id": "connector/example"}]),
        (("runner", "model_credential_ref", "id"), "bad ref"),
        (("bindings", "history_signing_credential_ref"), {
            "id": "credential/example-model", "generation": "1"
        }),
        (("bindings", "history_signing_credential_ref", "generation"), "0"),
        (("bindings", "history_signing_credential_ref", "generation"), "9223372036854775808"),
        (("runner", "budget", "max_output_tokens_per_run"), 1_000_001),
        (("launch", "resources", "cpu_millicores"), 64_001),
        (("launch", "resources", "memory_mib"), 0),
    ],
)
def test_first_profile_refuses_unsupported_behavior_even_with_matching_digest(path, bad):
    """@spec PROTECTED-HOOK-LANE-6/7."""
    product = configuration()
    value = execution_profile()
    target = value
    for member in path[:-1]:
        target = target[member]
    target[path[-1]] = bad
    with pytest.raises(product.ExecutionConfigInvalid):
        product.parse_execution_config(canonical(value), manifest=manifest_for(value))


@pytest.mark.parametrize("disagreement", ["digest", "runner_image", "substrate"])
def test_manifest_tuple_disagreement_refuses_before_boot(disagreement):
    """@spec PROTECTED-HOOK-LANE-6/7."""
    product = configuration()
    value = execution_profile()
    changes = {}
    if disagreement == "digest":
        changes["execution_config_digest"] = "0" * 64
    elif disagreement == "runner_image":
        changes["runner_image_digest"] = "sha256:" + "0" * 64
    else:
        changes["substrate"] = {**value["launch"]["substrate"], "launch_identity": "launch/other"}
    with pytest.raises(product.ExecutionConfigInvalid):
        product.parse_execution_config(canonical(value), manifest=manifest_for(value, **changes))
