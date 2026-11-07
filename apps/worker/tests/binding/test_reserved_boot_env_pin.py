"""Completeness + cross-language drift pin for the reserved boot-env policy (#457).

Four guards, all checked at import time (no Postgres, no fixtures):

(a) Every credential-key literal the runner's ``sdk_auth`` owns is caught by
    ``is_reserved_boot_env_name``. If a new model credential is added to
    ``sdk_auth`` but not to ``RESERVED_BOOT_ENV``, this fails -- the exact
    class of gap #457 closes.  ``CURIE_MODEL_BASE_URL`` / ``CURIE_CREDENTIALS``
    are already safe via the prefix rule, but the pin asserts them anyway so the
    sdk_auth inventory is covered exhaustively.
(b) Every boot key a worker-lane producer WRITES is caught. Retargeted in #488
    from ``curie_worker.binding``'s ``*_ENV`` literals to
    ``aci_protocol.BootEnv``'s declared key list, because #488 moves the
    declaration out of the binding and deletes those constants -- the old guard
    would have gone red (it carries a non-vacuity floor, so it fails loudly
    rather than passing vacuously), and retargeting is what keeps the tripwire
    pointed at the real declaration site instead of a dead one.
(c) Cross-language parity: the Helm ``_helpers.tpl`` reserved list is an
    unavoidable second copy (Helm cannot import Python). Its
    ``curie.reservedConnectorSecretNames`` define MUST list exactly the
    non-``CURIE_`` members of ``RESERVED_BOOT_ENV`` (the prefix rule covers
    ``CURIE_*`` on both sides). Fails CI if the two lists drift.
(d) The same parity for the CLI's Rust copy
    (``cli/src/connector_build.rs``'s ``RESERVED_CONNECTOR_SECRET_NAMES``). It
    is a third language for the same reason Helm is a second: the skill and
    local tiers resolve a connector's declared secret from the operator's
    environment or vault before any Python sees the bundle, so the refusal has
    to exist client-side.
"""

from __future__ import annotations

import re
from pathlib import Path

import curie_runner.sdk_auth as sdk_auth
from aci_protocol import BootEnv
from plugin_format import RESERVED_BOOT_ENV, is_reserved_boot_env_name

# --- (a) sdk_auth credential-key literals ------------------------------------

# An env-var-name string: uppercase alnum, underscore-separated (ANTHROPIC_BASE_URL,
# CURIE_MODEL_BASE_URL, ...). Discriminates a credential-key literal from a base
# URL like "https://openrouter.ai/api" or a tuple constant.
_ENV_VAR_NAME_RE = re.compile(r"^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+$")


def _sdk_auth_credential_env_literals() -> dict[str, str]:
    """Discover the credential/base-url env-var literals sdk_auth owns.

    Every module-level ``*_ENV`` name in ``curie_runner.sdk_auth`` whose value
    is an env-var-name string. Reading the module: all of them
    (``CLAUDE_CODE_OAUTH_TOKEN``, ``ANTHROPIC_API_KEY``, ``ANTHROPIC_BASE_URL``,
    ``ANTHROPIC_AUTH_TOKEN``, ``CURIE_MODEL_BASE_URL``, ``CURIE_CREDENTIALS``)
    are credential/base-url keys that MUST be reserved -- there is no runner-local
    ``*_ENV`` knob here to exclude. The tuple alias ``_SDK_CREDENTIAL_ENV`` is
    skipped by the string check. Dynamic (not a hardcoded list) so a NEW credential
    ``*_ENV`` constant added to sdk_auth is caught even if nobody updates the pin.
    """
    out: dict[str, str] = {}
    for attr in dir(sdk_auth):
        if not attr.endswith("_ENV"):
            continue
        value = getattr(sdk_auth, attr)
        if isinstance(value, str) and _ENV_VAR_NAME_RE.match(value):
            out[attr] = value
    return out


def test_every_sdk_auth_credential_key_is_reserved() -> None:
    literals = _sdk_auth_credential_env_literals()
    # Sanity floor: discovery is not vacuous, and the four non-CURIE_ credential
    # keys plus the CURIE_ base-url alias are all present (guards the predicate
    # silently narrowing and skipping the exact gap #457 closes).
    assert {
        "ANTHROPIC_BASE_URL",
        "ANTHROPIC_API_KEY",
        "CLAUDE_CODE_OAUTH_TOKEN",
        "ANTHROPIC_AUTH_TOKEN",
        "CURIE_MODEL_BASE_URL",
    } <= set(literals.values()), literals
    for attr, value in literals.items():
        assert is_reserved_boot_env_name(value), (
            f"sdk_auth.{attr} == {value!r} is not caught by the reserved policy"
        )


# --- (b) declared worker-lane boot keys --------------------------------------

# The producers whose writes land in the worker lane: the binding's per-claim
# render and the kernel's resume overlay. Deliberately NOT `substrate` or
# `operator` -- their keys include the OTel trio, which is not reserved (a
# connector secret named OTEL_EXPORTER_OTLP_ENDPOINT is a separate policy
# question, out of #488's scope; see #487 for the redirect/capture class). This
# guard's scope is exactly the old one's: the keys the worker lane itself writes.
_WORKER_LANE_PRODUCERS = ("worker", "kernel")


def _worker_lane_boot_env_keys() -> set[str]:
    """The declared boot keys a worker-lane producer writes.

    Read through ``BootEnv.env_keys``, the public accessor, so a rename of the
    model's internals cannot silently narrow this. Dynamic (not a hardcoded
    list) so a NEW worker-written boot key is caught even if nobody updates the
    pin -- the same property the old binding-introspection guard had.
    """
    return {
        key
        for producer in _WORKER_LANE_PRODUCERS
        for key in BootEnv.env_keys(producer=producer)
    }


def test_every_declared_worker_lane_boot_key_is_reserved() -> None:
    keys = _worker_lane_boot_env_keys()
    # Sanity floor: discovery is not vacuous, and the load-bearing keys are all
    # present (guards against the accessor silently narrowing, or a producer
    # retag emptying the set, making this test pass while covering nothing).
    assert keys, "found no worker-lane boot keys declared on aci_protocol.BootEnv"
    assert {
        "CURIE_BUDGET",
        "CURIE_SESSION_ID",
        "CURIE_RUNNER_TOKEN",
        "CURIE_CREDENTIALS",
        "CURIE_APPROVAL_GRANT_TOOL",
        # Non-prefixed, so the prefix catch-all does NOT cover it: this key is
        # what makes the guard bite rather than restate the prefix rule. It is
        # reserved only because #457 enumerated it explicitly.
        "ANTHROPIC_BASE_URL",
    } <= keys, keys
    for key in sorted(keys):
        assert is_reserved_boot_env_name(key), (
            f"BootEnv declares {key!r} as a worker-lane boot key, but it is not "
            "caught by the reserved policy: a connector secret could shadow it"
        )


def test_dropping_agent_id_from_the_enumeration_does_not_unreserve_it() -> None:
    """#488 removed CURIE_AGENT_ID from _CURIE_BOOT_KEYS; that is a no-op.

    The entry was dead enumeration once the write site went away, but the
    ``CURIE_`` prefix catch-all still reserves the name, so no connector secret
    can claim it. Asserted so the removal is provably policy-neutral rather than
    a silent narrowing of the reserved set.
    """
    assert "CURIE_AGENT_ID" not in RESERVED_BOOT_ENV
    assert is_reserved_boot_env_name("CURIE_AGENT_ID")


# --- (c) Helm cross-language drift gate --------------------------------------

_HELPERS_TPL = (
    Path(__file__).resolve().parents[4]
    / "charts"
    / "curie"
    / "templates"
    / "_helpers.tpl"
)

# An env-name token: uppercase, at least one underscore (ANTHROPIC_BASE_URL etc).
_ENV_NAME_RE = re.compile(r"[A-Za-z0-9]+(?:_[A-Za-z0-9]+)+")


def _reserved_names_from_helpers() -> set[str]:
    text = _HELPERS_TPL.read_text(encoding="utf-8")
    # Extract the body of the reservedConnectorSecretNames define, tolerantly.
    match = re.search(
        r'define\s+"curie\.reservedConnectorSecretNames"\s*(?:-?}})?(?P<body>.*?){{-?\s*end',
        text,
        re.DOTALL,
    )
    assert match, (
        "no `curie.reservedConnectorSecretNames` define found in "
        f"{_HELPERS_TPL} -- the Helm reserved-name drift gate has no source"
    )
    tokens = set(_ENV_NAME_RE.findall(match.group("body")))
    # The prefix rule covers CURIE_* on both sides; only the explicitly
    # enumerated credential keys need list-parity.
    return {t for t in tokens if not t.startswith("CURIE_")}


def test_helm_reserved_list_matches_non_prefixed_members() -> None:
    expected = {n for n in RESERVED_BOOT_ENV if not n.startswith("CURIE_")}
    assert _reserved_names_from_helpers() == expected


# --- (d) Rust cross-language drift gate --------------------------------------

# The CLI's copy, and the second unavoidable one (Rust cannot import Python
# either). It exists because the skill and local tiers resolve a connector's
# declared secret from the operator's own environment or vault BEFORE any
# Python validates the bundle, so the client has to refuse a reserved name on
# its own. Same rule as the Helm gate: list parity on the non-``CURIE_``
# members, with the prefix rule covering ``CURIE_*`` on both sides.
_CONNECTOR_BUILD_RS = Path(__file__).resolve().parents[4] / "cli" / "src" / "connector_build.rs"


def _reserved_names_from_rust() -> set[str]:
    text = _CONNECTOR_BUILD_RS.read_text(encoding="utf-8")
    match = re.search(
        r"RESERVED_CONNECTOR_SECRET_NAMES[^=]*=\s*\[(?P<body>.*?)\]",
        text,
        re.DOTALL,
    )
    assert match, (
        "no `RESERVED_CONNECTOR_SECRET_NAMES` array found in "
        f"{_CONNECTOR_BUILD_RS} -- the Rust reserved-name drift gate has no source"
    )
    tokens = set(_ENV_NAME_RE.findall(match.group("body")))
    return {t for t in tokens if not t.startswith("CURIE_")}


def test_rust_reserved_list_matches_non_prefixed_members() -> None:
    expected = {n for n in RESERVED_BOOT_ENV if not n.startswith("CURIE_")}
    assert _reserved_names_from_rust() == expected
