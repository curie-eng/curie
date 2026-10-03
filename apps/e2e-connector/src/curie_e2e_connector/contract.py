"""Names shared with the factory render and the sandbox bind.

The same spellings are frozen in ``tests/vectors/e2e-connector-sandbox.json``.
A drift there fails the connector, API, worker, and CLI tests together.
"""

from __future__ import annotations

CONNECTOR_NAME = "e2e"
SENTINEL_IMAGE = "curie-e2e-connector"
KUBECONFIG_SECRET = "E2E_CLUSTER_KUBECONFIG"
KUBECONFIG_MOUNT = "/secrets/kubeconfig"
# Optional docker config.json files (#3246). The push config is mounted only in
# the build pod's push container; the cache config is reachable by Dockerfile
# RUN steps, so it must be scoped to the cache repository only.
REGISTRY_PUSH_SECRET = "E2E_REGISTRY_PUSH_CONFIG"
REGISTRY_PUSH_MOUNT = "/secrets/registry/config.json"
BUILD_CACHE_SECRET = "E2E_BUILD_CACHE_CONFIG"
BUILD_CACHE_MOUNT = "/secrets/registry-cache/config.json"
WITHHELD_FROM_SANDBOX = (KUBECONFIG_SECRET, REGISTRY_PUSH_SECRET, BUILD_CACHE_SECRET)

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

# image_build and image retention (#3246, ADR 0176 decision 6).
BUILD_LABEL = "curietech.ai/e2e-build"
BUILD_SECRET_PREFIX = "e2e-registry-"
BUILD_PUSH_K8S_SECRET_PREFIX = "e2e-registry-push-"
BUILD_CACHE_K8S_SECRET_PREFIX = "e2e-registry-cache-"
BUILD_EGRESS_POLICY = "e2e-build-egress"
IMAGES_CONFIGMAP = "e2e-images"
IMAGES_CONFIGMAP_KEY = "ledger.json"
# ledger.json is {"repositories": [<repo>, ...], "closing_at": "<RFC3339>"};
# closing_at is absent while the environment still admits builds.
CLOSE_SETTLE_S = 10
STAGING_TAG_PREFIX = "staging-"
FINAL_TAG_PREFIX = "build-"

REFUSAL_ENVIRONMENT_CLOSING = "e2e_environment_closing"
REFUSAL_REGISTRY_NOT_CONFIGURED = "e2e_registry_not_configured"
REFUSAL_ENVIRONMENT_REQUIRED = "e2e_environment_required"
REFUSAL_BUILD_ARGUMENT = "e2e_build_argument_refused"
REFUSAL_BUILD_POD_SECURITY = "e2e_build_needs_baseline"
REFUSAL_BUILD_FAILED = "e2e_build_failed"
REFUSAL_BUILD_TIMEOUT = "e2e_build_timeout"
REFUSAL_BUILD_NO_DIGEST = "e2e_build_no_digest"
REFUSAL_BUILD_IN_PROGRESS = "e2e_build_in_progress"
REFUSAL_REGISTRY_DELETE = "e2e_registry_delete_refused"

# An empty E2E_*_IMAGE means these. The defaults live only here.
DEFAULT_BUILDER_IMAGE = (
    "gcr.io/kaniko-project/executor:v1.23.2"
    "@sha256:9e69fd4330ec887829c780f5126dd80edc663df6def362cd22e79bcdf00ac53f"
)
DEFAULT_GIT_IMAGE = (
    "alpine/git:2.49.1@sha256:c0280cf9572316299b08544065d3bf35db65043d5e3963982ec50647d2746e26"
)
DEFAULT_PUSH_IMAGE = (
    "gcr.io/go-containerregistry/crane:debug"
    "@sha256:e78770b31258a3846f878036d9c1f63fbe4c871f9f56990bf77fd95c013e3c1b"
)

# The push container's fixed script. Every value arrives through env, so no
# run, repository or tag text is ever interpolated into shell. It deletes
# nothing: each tag is unique to the build, and teardown retention removes
# every tag's digest. The vector key push_script_sha256 freezes this text.
PUSH_SCRIPT = (
    "set -eu\n"
    'crane push ${CRANE_INSECURE:+--insecure} /out/image.tar "$DEST:$STAGING_TAG" >/dev/null\n'
    'ref=$(crane mutate ${CRANE_INSECURE:+--insecure} "$DEST:$STAGING_TAG" \\\n'
    '  --label "curietech.ai/e2e-owner=$LABEL_OWNER" \\\n'
    '  --label "curietech.ai/e2e-run=$LABEL_RUN" \\\n'
    '  --label "curietech.ai/e2e-work-item=$LABEL_WORK_ITEM" \\\n'
    '  -t "$DEST:$BUILD_TAG")\n'
    "printf '%s' \"$DEST@${ref##*@}\" > /dev/termination-log\n"
)

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
    "E2E_REGISTRY",
    "E2E_BUILD_CACHE_REPO",
    "E2E_REGISTRY_INSECURE",
    "E2E_REGISTRY_TOKEN_HOSTS",
    "E2E_BUILDER_IMAGE",
    "E2E_GIT_IMAGE",
    "E2E_PUSH_IMAGE",
    "E2E_BUILD_TIMEOUT_SECONDS",
    "E2E_SOURCE_HOSTS",
    "PORT",
)
