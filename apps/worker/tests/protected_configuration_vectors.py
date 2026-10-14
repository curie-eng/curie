"""Independent closed v1 profile vectors, @spec PROTECTED-HOOK-LANE-6/7.

No services, simulated qualification or secret values in execution_config.json.
"""

from __future__ import annotations

import hashlib
import json

from curie_protected_hooks.authority_records import Manifest, parse_manifest

AGENT = "11111111-1111-4111-8111-111111111111"
VERSION = "66666666-6666-4666-8666-666666666666"


def canonical(value: object) -> bytes:
    """Independent canonical ASCII oracle, @spec PROTECTED-HOOK-LANE-6/7."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


def execution_profile(*, agent_id: str = AGENT, version_id: str = VERSION) -> dict:
    """Every supported field is explicit, @spec PROTECTED-HOOK-LANE-6/7."""
    return {
        "schema_version": 1,
        "agent_id": agent_id,
        "version_id": version_id,
        "runner": {
            "harness": "claude-agent-sdk",
            "plugin_dir": "/bundles/current",
            "model": "example-model",
            "reviewer_model": "example-reviewer",
            "thinking": "disabled",
            "api_backend": "messages",
            "model_env_key": "ANTHROPIC_API_KEY",
            "model_base_url": "https://model.example.com",
            "model_credential_ref": {"id": "credential/example-model", "generation": "1"},
            "budget": {"max_output_tokens_per_run": 4096, "max_usd_per_day": "10.25"},
            "max_turns": 8,
            "execution_deadline_seconds": 300,
            "false_completion_check": False,
            "fake_model": False,
        },
        "policy": {
            "tool_access": "read-only",
            "lifecycle_hooks": False,
            "memory": False,
            "workspace": False,
            "attachments": False,
            "approvals": False,
            "behavior_packs": False,
        },
        "launch": {
            "runner_image_digest": "sha256:" + "e" * 64,
            "substrate": {
                "kind": "docker",
                "authority_domain_id": "55555555-5555-4555-8555-555555555555",
                "launch_identity": "launch/example-v1",
                "launch_config_sha256": "2" * 64,
            },
            "resources": {"cpu_millicores": 500, "memory_mib": 512},
            "init_image_digests": [],
            "fetch_image_digest": "sha256:" + "a" * 64,
        },
        "bindings": {
            "runner_facing_api_base_url": "https://api.example.com",
            "history_signing_credential_ref": {
                "id": "credential/example-history",
                "generation": "1",
            },
            "qualified_connectors": [],
        },
    }


def manifest_for(profile: dict, **changes: object) -> Manifest:
    """Manifest integrity vector, not qualification, @spec PROTECTED-HOOK-LANE-6/7."""
    value = {
        "schema_version": 1,
        "runtime_id": "33333333-3333-4333-8333-333333333333",
        "runtime_generation": "1",
        "broker_identity": {
            "instance_id": AGENT,
            "endpoint": {"host": "broker.example.com", "port": 6380},
            "tls_server_name": "broker.example.com",
            "tls_spki_sha256": "a" * 64,
            "run_id": "b" * 40,
            "database": 0,
        },
        "worker_image_digest": "sha256:" + "d" * 64,
        "runner_image_digest": profile["launch"]["runner_image_digest"],
        "bundle_digest": {"sha256": "f" * 64, "object_identity": "bundle/example-v1"},
        "execution_config_digest": hashlib.sha256(canonical(profile)).hexdigest(),
        "qualification_id": "44444444-4444-4444-8444-444444444444",
        "substrate": dict(profile["launch"]["substrate"]),
        "guard_identity": {
            "control_id": "22222222-2222-4222-8222-222222222222",
            "revision": "1",
            "config_sha256": "c" * 64,
        },
        "credential_refs": {
            role: {"id": "credential/example-" + role, "generation": "1"}
            for role in ("enqueue", "worker", "verifier")
        },
    }
    value.update(changes)
    return parse_manifest(canonical(value))
