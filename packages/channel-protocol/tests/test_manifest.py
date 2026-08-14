"""The adapter binding profile models (#1516).

``adapter.yaml`` is a PER INSTALL binding profile: the channel kind an adapter
owns, the address shape it accepts, the endpoint this install's worker POSTs
reply events to, and the name of the egress credential identity. It is not
ADR-0096 decision 2's install agnostic composition manifest, and nothing here
should be read as pre-empting that document.

Every assertion below is behavioral: it validates or refuses a real payload, or
compares a value that survived parsing. None of them inspects an internal name.
"""

import ast
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from channel_protocol.manifest import (
    AdapterConformance,
    AdapterProfile,
    AddressShape,
    ProfileVersionError,
    load_profile,
)
from channel_protocol.reply import REPLY_WIRE_VERSION
from pydantic import ValidationError

_REPO_ROOT = Path(__file__).resolve().parents[3]
_API_SCHEMAS = _REPO_ROOT / "apps" / "api" / "src" / "curie_api" / "schemas.py"


def _profile(**overrides: Any) -> dict[str, Any]:
    """A profile every consumer accepts, with the caller's keys replaced."""

    base: dict[str, Any] = {
        "version": "1.0",
        "kind": "email",
        "endpoint": "https://curie-mail-adapter.example.test/curie/reply",
        "address": {
            "description": "The mailbox this adapter owns.",
            "pattern": r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$",
            "example": "agent@example.test",
        },
        "credentials": {
            "egress": "agentmail-sandbox",
            "egress_secret_env": "CURIE_EGRESS_SECRET",
            "ingress_token_env": "CURIE_INGRESS_TOKEN",
        },
        "conformance": {"wire_version": "1.0", "mints_reply_ref": True},
    }
    base.update(overrides)
    return base


def _api_channel_kind_pattern() -> str:
    """The slug pattern source the API validates channel kinds and adapters with.

    Read out of the source rather than imported, so this gate does not drag the
    API's runtime dependencies into a contract package's test run.
    """

    tree = ast.parse(_API_SCHEMAS.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(t, ast.Name) and t.id == "_CHANNEL_KIND" for t in node.targets):
            continue
        call = node.value
        assert isinstance(call, ast.Call), "_CHANNEL_KIND is no longer a re.compile call"
        literal = call.args[0]
        assert isinstance(literal, ast.Constant) and isinstance(literal.value, str)
        return literal.value
    raise AssertionError(f"{_API_SCHEMAS} no longer defines _CHANNEL_KIND")


def test_profile_round_trips() -> None:
    profile = load_profile(_profile())
    decoded = AdapterProfile.model_validate_json(profile.model_dump_json())
    assert decoded.kind == "email"
    assert decoded.endpoint == "https://curie-mail-adapter.example.test/curie/reply"
    assert decoded.address.example == "agent@example.test"
    assert decoded.credentials.egress == "agentmail-sandbox"
    assert decoded.conformance.mints_reply_ref is True


def test_rejects_an_unknown_top_level_key() -> None:
    with pytest.raises(ValidationError):
        AdapterProfile.model_validate(_profile(retries=3))


def test_env_name_pattern_rejects_a_lowercase_name() -> None:
    """The pattern is a legibility constraint on a variable NAME.

    It claims nothing about secrecy: ``DEADBEEF`` matches it. Committed values
    are the repo's gitleaks gate, and which credential gets read is decided at
    the invocation boundary, never by this field.
    """

    credentials = dict(_profile()["credentials"], egress_secret_env="curie_egress_secret")
    with pytest.raises(ValidationError):
        AdapterProfile.model_validate(_profile(credentials=credentials))

    hyphenated = dict(_profile()["credentials"], ingress_token_env="CURIE-INGRESS-TOKEN")
    with pytest.raises(ValidationError):
        AdapterProfile.model_validate(_profile(credentials=hyphenated))


def test_slug_pattern_is_byte_identical_to_the_api() -> None:
    """The API owns the slug rule; the profile copies it verbatim.

    A tightening on either side without the other would let the CLI accept a
    value the API refuses, or refuse one it accepts.
    """

    api_pattern = _api_channel_kind_pattern()
    schema = AdapterProfile.model_json_schema()
    assert schema["properties"]["kind"]["pattern"] == api_pattern
    credentials = schema["$defs"]["AdapterCredentials"]["properties"]
    assert credentials["egress"]["pattern"] == api_pattern


def test_accepts_an_underscore_slug() -> None:
    credentials = dict(_profile()["credentials"], egress="ms_teams_egress")
    profile = load_profile(_profile(kind="ms_teams", credentials=credentials))
    assert profile.kind == "ms_teams"
    assert profile.credentials.egress == "ms_teams_egress"


def test_rejects_an_endpoint_with_userinfo() -> None:
    with pytest.raises(ValidationError):
        AdapterProfile.model_validate(
            _profile(endpoint="https://user:pw@curie-mail-adapter.example.test/curie/reply")
        )
    with pytest.raises(ValidationError):
        AdapterProfile.model_validate(_profile(endpoint="/curie/reply"))


def test_endpoint_is_optional() -> None:
    """A live verb requires a concrete endpoint; the model does not.

    The verbs that POST somewhere (bind, token, smoke-test) demand it at the
    verb boundary, so keeping it optional here costs nothing and leaves the
    schema line open to a profile that names no install specific route.
    """

    raw = _profile()
    del raw["endpoint"]
    profile = load_profile(raw)
    assert profile.endpoint is None
    assert AdapterProfile.model_validate_json(profile.model_dump_json()).endpoint is None


def test_rejects_a_pattern_rust_regex_cannot_compile() -> None:
    """The Rust regex dialect is the floor for ``address.pattern``.

    Python ``re`` compiles lookaround and backreferences and the Rust ``regex``
    crate refuses them, so a pattern that only Python accepts would be a silent
    cross language divergence. This is the admitted paired validator: JSON
    Schema cannot express the rule, so it lives in two implementations kept in
    step by the shared corpus.
    """

    address = _profile()["address"]
    for pattern in (
        "^[a-z",
        "^(?=.*@)[a-z0-9@.]+$",
        "(?<=@)[a-z0-9.]+$",
        r"^([a-z0-9]+)@\1\.example\.test$",
    ):
        with pytest.raises(ValidationError):
            AddressShape.model_validate(dict(address, pattern=pattern))


def test_wire_version_is_the_reply_module_literal() -> None:
    accepted = AdapterConformance.model_validate(
        {"wire_version": REPLY_WIRE_VERSION, "mints_reply_ref": True}
    )
    assert accepted.wire_version == REPLY_WIRE_VERSION
    with pytest.raises(ValidationError):
        AdapterConformance.model_validate({"wire_version": "2.0", "mints_reply_ref": True})


def test_version_mismatch_is_refused_before_schema_validation() -> None:
    """The acceptance rule runs first, and it names both versions.

    Run it after schema validation instead and a 1.1 file trips the closed
    schema first, so the operator reads "additional property not allowed"
    rather than the version they actually need to act on.
    """

    with pytest.raises(ProfileVersionError) as newer:
        load_profile(_profile(version="1.1"))
    assert "1.0" in str(newer.value)
    assert "1.1" in str(newer.value)

    with pytest.raises(ProfileVersionError) as newer_with_unknown_key:
        load_profile(_profile(version="1.1", retries=3))
    assert "1.1" in str(newer_with_unknown_key.value)
    assert "retries" not in str(newer_with_unknown_key.value)

    missing = _profile()
    del missing["version"]
    with pytest.raises(ProfileVersionError):
        load_profile(missing)


def test_bare_import_pulls_no_http_client() -> None:
    """The worker and the API import this package and must not acquire httpx.

    Asserted in a fresh interpreter, because any other test in this session
    could have imported httpx into this one.
    """

    probe = "import channel_protocol, sys; raise SystemExit(1 if 'httpx' in sys.modules else 0)"
    completed = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True)
    assert completed.returncode == 0, (
        f"import channel_protocol pulled in httpx: {completed.stdout}{completed.stderr}"
    )
