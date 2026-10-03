"""Every name in the shared vector equals its connector constant (#3246).

``tests/vectors/e2e-connector-sandbox.json`` is read by the connector, API,
worker and CLI tests. A key ``foo_bar`` freezes ``contract.FOO_BAR``, except
``push_script_sha256``, which freezes the hash of ``PUSH_SCRIPT``.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from curie_e2e_connector import contract

VECTOR = Path(__file__).resolve().parents[3] / "tests" / "vectors" / "e2e-connector-sandbox.json"

# The keys this ticket adds, with the constant each one freezes.
NEW_KEYS = {
    "registry_push_secret": "REGISTRY_PUSH_SECRET",
    "registry_push_mount": "REGISTRY_PUSH_MOUNT",
    "build_cache_secret": "BUILD_CACHE_SECRET",
    "build_cache_mount": "BUILD_CACHE_MOUNT",
    "build_label": "BUILD_LABEL",
    "build_secret_prefix": "BUILD_SECRET_PREFIX",
    "build_push_k8s_secret_prefix": "BUILD_PUSH_K8S_SECRET_PREFIX",
    "build_cache_k8s_secret_prefix": "BUILD_CACHE_K8S_SECRET_PREFIX",
    "build_egress_policy": "BUILD_EGRESS_POLICY",
    "images_configmap": "IMAGES_CONFIGMAP",
    "images_configmap_key": "IMAGES_CONFIGMAP_KEY",
    "close_settle_s": "CLOSE_SETTLE_S",
    "staging_tag_prefix": "STAGING_TAG_PREFIX",
    "final_tag_prefix": "FINAL_TAG_PREFIX",
    "refusal_environment_closing": "REFUSAL_ENVIRONMENT_CLOSING",
    "refusal_registry_not_configured": "REFUSAL_REGISTRY_NOT_CONFIGURED",
    "refusal_environment_required": "REFUSAL_ENVIRONMENT_REQUIRED",
    "refusal_build_argument": "REFUSAL_BUILD_ARGUMENT",
    "refusal_build_pod_security": "REFUSAL_BUILD_POD_SECURITY",
    "refusal_build_failed": "REFUSAL_BUILD_FAILED",
    "refusal_build_timeout": "REFUSAL_BUILD_TIMEOUT",
    "refusal_build_no_digest": "REFUSAL_BUILD_NO_DIGEST",
    "refusal_build_in_progress": "REFUSAL_BUILD_IN_PROGRESS",
    "refusal_registry_delete": "REFUSAL_REGISTRY_DELETE",
}

# The keys #3247 adds for deploy, run, logs and events.
WORKLOAD_KEYS = {
    "refusal_cluster_scoped": "REFUSAL_CLUSTER_SCOPED",
    "refusal_image_not_digest": "REFUSAL_IMAGE_NOT_DIGEST",
    "refusal_deploy_manifest": "REFUSAL_DEPLOY_MANIFEST",
    "refusal_deploy_object": "REFUSAL_DEPLOY_OBJECT",
    "refusal_deploy_failed": "REFUSAL_DEPLOY_FAILED",
    "refusal_run_argument": "REFUSAL_RUN_ARGUMENT",
    "refusal_run_failed": "REFUSAL_RUN_FAILED",
    "refusal_run_timeout": "REFUSAL_RUN_TIMEOUT",
    "refusal_pod_not_found": "REFUSAL_POD_NOT_FOUND",
    "refusal_logs_refused": "REFUSAL_LOGS_REFUSED",
    "run_job_prefix": "RUN_JOB_PREFIX",
    "run_timeout_s": "RUN_TIMEOUT_S",
    "output_limit_bytes": "OUTPUT_LIMIT_BYTES",
}


def vector() -> dict[str, Any]:
    loaded = json.loads(VECTOR.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def expected_value(key: str) -> Any:
    if key == "push_script_sha256":
        return hashlib.sha256(contract.PUSH_SCRIPT.encode("utf-8")).hexdigest()
    value = getattr(contract, key.upper())
    return list(value) if isinstance(value, tuple) else value


def test_the_vector_carries_every_new_key() -> None:
    missing = sorted({*NEW_KEYS, "push_script_sha256"} - set(vector()))
    assert missing == []


@pytest.mark.parametrize("key", sorted(vector()))
def test_every_vector_key_equals_its_constant(key: str) -> None:
    assert hasattr(contract, key.upper()) or key == "push_script_sha256", key
    assert vector()[key] == expected_value(key)


def test_the_vector_carries_every_workload_key() -> None:
    assert sorted(set(WORKLOAD_KEYS) - set(vector())) == []


@pytest.mark.parametrize(("key", "constant"), sorted({**NEW_KEYS, **WORKLOAD_KEYS}.items()))
def test_new_keys_map_to_the_named_constants(key: str, constant: str) -> None:
    assert key.upper() == constant
    assert hasattr(contract, constant)


def test_three_names_are_withheld_from_the_sandbox() -> None:
    assert contract.WITHHELD_FROM_SANDBOX == (
        "E2E_CLUSTER_KUBECONFIG",
        "E2E_REGISTRY_PUSH_CONFIG",
        "E2E_BUILD_CACHE_CONFIG",
    )
    assert vector()["withheld_from_sandbox"] == list(contract.WITHHELD_FROM_SANDBOX)


def test_fixed_values() -> None:
    assert contract.REGISTRY_PUSH_MOUNT == "/secrets/registry/config.json"
    assert contract.BUILD_CACHE_MOUNT == "/secrets/registry-cache/config.json"
    assert contract.BUILD_LABEL == "curietech.ai/e2e-build"
    assert contract.BUILD_PUSH_K8S_SECRET_PREFIX.startswith(contract.BUILD_SECRET_PREFIX)
    assert contract.BUILD_CACHE_K8S_SECRET_PREFIX.startswith(contract.BUILD_SECRET_PREFIX)
    assert contract.IMAGES_CONFIGMAP == "e2e-images"
    assert contract.IMAGES_CONFIGMAP_KEY == "ledger.json"
    assert contract.CLOSE_SETTLE_S == 10
    assert contract.STAGING_TAG_PREFIX == "staging-"
    assert contract.FINAL_TAG_PREFIX == "build-"
    assert "@sha256:" in contract.DEFAULT_PUSH_IMAGE
    assert "@sha256:" in contract.DEFAULT_BUILDER_IMAGE
    assert "@sha256:" in contract.DEFAULT_GIT_IMAGE


def test_workload_fixed_values() -> None:
    assert contract.REFUSAL_CLUSTER_SCOPED == "e2e_cluster_scoped_object"
    assert contract.REFUSAL_IMAGE_NOT_DIGEST == "e2e_image_not_digest"
    assert contract.RUN_JOB_PREFIX == "e2e-run-"
    assert contract.RUN_TIMEOUT_S == 1200
    assert contract.OUTPUT_LIMIT_BYTES == 65536


def test_platform_env_adds_the_nine_build_names_before_port() -> None:
    new = (
        "E2E_REGISTRY",
        "E2E_BUILD_CACHE_REPO",
        "E2E_REGISTRY_INSECURE",
        "E2E_REGISTRY_TOKEN_HOSTS",
        "E2E_BUILDER_IMAGE",
        "E2E_GIT_IMAGE",
        "E2E_PUSH_IMAGE",
        "E2E_BUILD_TIMEOUT_SECONDS",
        "E2E_SOURCE_HOSTS",
    )
    assert contract.PLATFORM_ENV[-1] == "PORT"
    assert contract.PLATFORM_ENV[-1 - len(new) : -1] == new
    assert contract.PLATFORM_ENV[0] == "E2E_KUBECONFIG"
