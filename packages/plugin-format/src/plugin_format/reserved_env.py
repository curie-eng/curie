"""Reserved sandbox boot-env name policy for connector secrets (#457, #445, ADR-0009).

Single source of truth for the names a per-agent connector secret must never
declare, consulted by every write seam so a connector secret cannot shadow a
runner-owned model credential or a platform boot-env key, NOR a generic
redirect/capture-capable key the SDK's HTTP/TLS stack reads (#487):

- API validator ``curie_api.schemas._validate_secret_map``,
- bundle validator ``plugin_format.validate._validate_secrets``,
- worker injection loop ``curie_worker.binding``,
- Helm guard ``charts/curie/templates/agent-connector-secrets.yaml``.

#445 fenced only the ``CURIE_`` prefix, which structurally cannot see the four
non-prefixed credential keys the runner's ``sdk_auth`` owns
(``ANTHROPIC_BASE_URL``, ``ANTHROPIC_API_KEY``, ``CLAUDE_CODE_OAUTH_TOKEN``,
``ANTHROPIC_AUTH_TOKEN``). A connector secret named ``ANTHROPIC_BASE_URL`` would
silently redirect the Claude session to an arbitrary endpoint and hand it the
resolved model credential. #457 closes that gap by naming those keys explicitly
while keeping the whole ``CURIE_`` namespace fenced for forward safety.

The real owners of these names are ``runner/src/curie_runner/sdk_auth.py`` (the
credential keys) and ``apps/worker/src/curie_worker/binding.py`` (the
``CURIE_*`` boot keys). This module re-enumerates them so the policy is
greppable from one place; ``apps/worker/tests/binding/test_reserved_boot_env_pin.py``
is the completeness + cross-language drift pin that fails CI if a boot or
credential key is added there but not covered here (or if the Helm or Rust list
drifts).

``SECRET_NAME_RE`` rides here too. Every seam that consults the reserved fence
also holds a connector secret name to an env-var SHAPE, so both halves of the
name policy come from one import rather than a regex copied per validator.
"""

from __future__ import annotations

import re

# The shape a connector secret name must have, wherever it is declared: the
# value is delivered to the sandbox as an environment variable and consumed by
# ``${VAR}`` expansion, so a name that is not env-var-shaped can never bind.
SECRET_NAME_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")

# The four runner ``sdk_auth`` credential keys that are NOT ``CURIE_``-prefixed.
# These are the exact gap #457 closes. Together with ``_REDIRECT_CAPTURE_KEYS``
# below, these are the non-prefixed members of the set; per the drift pin, the
# Helm ``_helpers.tpl`` reserved list must equal that combined non-prefixed subset.
_CREDENTIAL_KEYS = frozenset(
    {
        "ANTHROPIC_BASE_URL",
        "ANTHROPIC_API_KEY",
        "CLAUDE_CODE_OAUTH_TOKEN",
        "ANTHROPIC_AUTH_TOKEN",
    }
)

# Generic, non-``CURIE_``-prefixed env that the SDK's HTTP/TLS stack (or Node)
# reads to REDIRECT or CAPTURE the model session, rather than being a key the
# worker/runner explicitly owns (#487). Each reaches the same end state #457 closed
# for ``ANTHROPIC_BASE_URL``: a connector secret named one of these could route the
# session (and its resolved credential) through an operator-named proxy, add a
# trusted CA for transparent TLS MITM, or inject arbitrary headers onto model
# calls. Fenced here so the reserved set means "reserved OR redirect/capture-
# capable", not merely "keys we own".
_REDIRECT_CAPTURE_KEYS = frozenset(
    {
        "HTTPS_PROXY",
        "HTTP_PROXY",
        "NODE_EXTRA_CA_CERTS",
        "ANTHROPIC_CUSTOM_HEADERS",
    }
)

# The ``CURIE_``-prefixed boot/config keys the worker and runner own. Every one
# is already caught by the prefix rule in ``is_reserved_boot_env_name``; they are
# enumerated here for greppability and to make the completeness pin
# membership-based. Cross-checked against ``aci_protocol.BootEnv.env_keys()`` (the
# declared boot contract since #488) and ``sdk_auth.MODEL_BASE_URL_ENV``.
#
# This list is enumeration, not enforcement: the prefix rule is what actually
# reserves a ``CURIE_`` name, so adding or dropping an entry here is
# policy-neutral (pinned by test). ``CURIE_AGENT_ID`` was dropped in #488 when
# its write site went away; it stays reserved via the prefix.
_CURIE_BOOT_KEYS = frozenset(
    {
        "CURIE_MODEL_BASE_URL",
        "CURIE_MODEL_API_BACKEND",
        "CURIE_MODEL_ENV_KEY",
        "CURIE_CREDENTIALS",
        "CURIE_MODEL",
        "CURIE_FAKE_MODEL",
        "CURIE_BUDGET",
        "CURIE_SESSION_ID",
        "CURIE_BUNDLE_REF",
        "CURIE_BUNDLE_VERSION",
        "CURIE_PLUGIN_DIR",
        "CURIE_MEMORY_REF",
        "CURIE_MEMORY_TOKEN",
        "CURIE_STATE_URL",
        "CURIE_STATE_TOKEN",
        "CURIE_HISTORY_REF",
        "CURIE_HISTORY_TOKEN",
        "CURIE_RUNNER_TOKEN",
        "CURIE_APPROVAL_REQUIRED_TOOLS",
        "CURIE_CONNECTOR_SECRET_KEYS",
        "CURIE_APPROVAL_GRANT_TOOL",
        "CURIE_SANDBOX_ID",
        "CURIE_RUNNER_PORT",
    }
)

RESERVED_BOOT_ENV: frozenset[str] = _CREDENTIAL_KEYS | _REDIRECT_CAPTURE_KEYS | _CURIE_BOOT_KEYS


def is_reserved_boot_env_name(name: str) -> bool:
    """Whether ``name`` is a reserved sandbox boot-env / model-credential key.

    ``True`` for any explicitly enumerated key in ``RESERVED_BOOT_ENV`` and for
    the whole ``CURIE_`` namespace (the forward-safe catch-all covering future
    boot keys nobody remembers to enumerate). Connector secrets must not declare
    such a name.
    """
    return name in RESERVED_BOOT_ENV or name.startswith("CURIE_")
