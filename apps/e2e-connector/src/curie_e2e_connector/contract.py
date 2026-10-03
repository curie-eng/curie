"""Names shared with the factory render and the sandbox bind.

The same spellings are frozen in ``tests/vectors/e2e-connector-sandbox.json``.
A drift there fails the connector, API, worker, and CLI tests together.
"""

from __future__ import annotations

CONNECTOR_NAME = "e2e"
SENTINEL_IMAGE = "curie-e2e-connector"
KUBECONFIG_SECRET = "E2E_CLUSTER_KUBECONFIG"
KUBECONFIG_MOUNT = "/secrets/kubeconfig"
WITHHELD_FROM_SANDBOX = (KUBECONFIG_SECRET,)

RUN_HEADER = "X-Curie-Run"
WORK_ITEM_HEADER = "X-Curie-Work-Item"

OWNER_LABEL = "curietech.ai/e2e-owner"
RUN_LABEL = "curietech.ai/e2e-run"
WORK_ITEM_LABEL = "curietech.ai/e2e-work-item"
EXPIRES_ANNOTATION = "curietech.ai/e2e-expires-at"
POD_SECURITY_LABEL = "pod-security.kubernetes.io/enforce"
POD_SECURITY_LEVEL = "baseline"

REFUSAL_NOT_CONFIGURED = "e2e_connector_not_configured"
REFUSAL_MISCONFIGURED = "e2e_connector_misconfigured"
REFUSAL_NOT_OWNED = "e2e_namespace_not_owned"
REFUSAL_NO_RUN = "e2e_run_identity_required"

SERVER_MODULE = "curie_e2e_connector"

# Platform env the renderer sets. A bundle cannot override these.
PLATFORM_ENV = (
    "E2E_KUBECONFIG",
    "E2E_NAMESPACE_PREFIX",
    "E2E_OWNER_LABEL_KEY",
    "E2E_OWNER_LABEL_VALUE",
    "E2E_SERVICE_ACCOUNT",
    "E2E_SERVICE_ACCOUNT_NAMESPACE",
    "E2E_WORKER_CLUSTER_ROLE",
    "E2E_TTL_SECONDS",
    "E2E_POD_SECURITY",
    "PORT",
)
