"""Worker-grade provisioner inputs for the local protected worker, @spec PROTECTED-COMMISSIONING-6.

The provisioner core checks only the members of the execution configuration
and projection it shares with M. The installed worker parses both in full, so
the worker tests give the provisioner one coherent pair the worker's closed
parsers accept. Independent example values; no real deployment data.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .test_commissioning_provisioner import RUNNER, profile, substrate


def canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


_SLOTS = {
    "schema_version": 1,
    "execution_identity": "verified-delivery-v1",
    "history_binding": "protected-history-v1",
    "runner_token": "fresh-owned-launch-v1",
    "history_token": "scoped-history-credential-v1",
    "workload_identity": "control-plane-cold-create-v1",
    "dial_identity": "owned-workload-endpoint-v1",
}
_MODEL_CREDENTIAL = {"id": "credential/example-model", "generation": "1"}
_HISTORY_CREDENTIAL = {"id": "credential/example-history", "generation": "1"}
_RESOURCES = {"cpu_millicores": 500, "memory_mib": 512}


def _entries(value: dict[str, str]) -> list[dict[str, str]]:
    return [{"name": k, "value": v} for k, v in sorted(value.items())]


def projection_value() -> dict[str, Any]:
    """A complete kubernetes-cold-pod-v1 projection, @spec PROTECTED-COMMISSIONING-6."""
    return {
        "schema_version": 1,
        "format": "kubernetes-cold-pod-v1",
        **substrate(),
        "namespace": "commissioning-runner",
        "service_account": "commissioning-runner",
        "runtime_class": "commissioning-runtime",
        "image": {"reference": RUNNER, "digest": "sha256:" + "e" * 64},
        "embedded_bundle": {
            "object_identity": "bundle/example-v1",
            "sha256": "f" * 64,
            "path": "/bundles/current",
        },
        "platform": {
            "os": "linux",
            "architecture": "amd64",
            "variant": None,
            "runtime_profile": "runtime/example",
        },
        "image_environment": _entries(
            {
                "HOME": "/home/runner",
                "CURIE_RUNNER_PORT": "8080",
                "CURIE_PROC_DUMPABLE_PRELOAD": "/usr/local/lib/curie/libproc_dumpable.so",
                "PATH": "/app/.venv/bin:/measured/bin",
                "LANG": "C.UTF-8",
            }
        ),
        "environment": _entries(
            {
                "CURIE_PLUGIN_DIR": "/bundles/current",
                "CURIE_BUDGET": (
                    '{"max_output_tokens_per_run":4096,"task_budget_hint":null,'
                    '"max_usd_per_day":10.25}'
                ),
                "CURIE_BUNDLE_REF": "bundle/example-v1",
                "CURIE_MODEL": "example-model",
                "CURIE_REVIEWER_MODEL": "example-reviewer",
                "CURIE_FAKE_MODEL": "0",
                "ANTHROPIC_BASE_URL": "https://model.example.com",
                "CURIE_MODEL_API_BACKEND": "messages",
                "CURIE_THINKING": "disabled",
                "CURIE_MODEL_ENV_KEY": "ANTHROPIC_API_KEY",
                "CURIE_MAX_TURNS": "8",
                "CURIE_MEMORY_WRITES": "0",
                "CURIE_HARNESS": "claude-agent-sdk",
                "CURIE_FALSE_COMPLETION_CHECK": "0",
            }
        ),
        "model_credential_ref": dict(_MODEL_CREDENTIAL),
        "history_signing_credential_ref": dict(_HISTORY_CREDENTIAL),
        "resources": dict(_RESOURCES),
        "network_policy_ref": {"id": "network/example", "revision": "1", "config_sha256": "a" * 64},
        "delivery_slots": dict(_SLOTS),
    }


def execution_value(projection: bytes) -> dict[str, Any]:
    """A complete v1 execution configuration bound to ``projection``.

    @spec PROTECTED-COMMISSIONING-6.
    """
    return {
        "schema_version": 1,
        "agent_id": "11111111-1111-4111-8111-111111111111",
        "version_id": "66666666-6666-4666-8666-666666666666",
        "runner": {
            "harness": "claude-agent-sdk",
            "plugin_dir": "/bundles/current",
            "model": "example-model",
            "reviewer_model": "example-reviewer",
            "thinking": "disabled",
            "api_backend": "messages",
            "model_env_key": "ANTHROPIC_API_KEY",
            "model_base_url": "https://model.example.com",
            "model_credential_ref": dict(_MODEL_CREDENTIAL),
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
                "kind": "kubernetes",
                **substrate(),
                "launch_config_sha256": hashlib.sha256(projection).hexdigest(),
            },
            "resources": dict(_RESOURCES),
            "init_image_digests": [],
            "fetch_image_digest": "sha256:" + "a" * 64,
        },
        "bindings": {
            "runner_facing_api_base_url": "https://api.example.com",
            "history_signing_credential_ref": dict(_HISTORY_CREDENTIAL),
            "qualified_connectors": [],
        },
    }


def worker_profile(**changes: Any) -> Any:
    """The provisioner profile with the worker-grade pair, @spec PROTECTED-COMMISSIONING-6."""
    projection = canonical(projection_value())
    return profile(
        projection=projection,
        execution_config=canonical(execution_value(projection)),
        **changes,
    )
