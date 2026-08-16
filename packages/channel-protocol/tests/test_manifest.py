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
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from channel_protocol.manifest import (
    PROFILE_VERSION,
    AdapterConformance,
    AdapterProfile,
    AddressShape,
    ProfileVersionError,
    check_version,
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
        "conformance": {"wire_version": "1.0"},
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

    Python ``re`` compiles lookaround, backreferences, atomic and conditional
    groups, inline comments, the ASCII flag, ``\\Z`` and ``\\N{...}``, and the
    Rust ``regex`` crate refuses every one of them, so a pattern that only
    Python accepts would be a silent cross language divergence. Each pattern
    below was compiled against regex 1.13.1 to confirm it really is refused
    there. This is the admitted paired validator: JSON Schema cannot express the
    rule, so it lives in two implementations kept in step by the shared corpus.
    """

    address = _profile()["address"]

    with pytest.raises(ValidationError):
        AddressShape.model_validate(dict(address, pattern="^[a-z"))

    for pattern in (
        "^(?=.*@)[a-z0-9@.]+$",
        "(?!x)[a-z0-9.]+$",
        "(?<=@)[a-z0-9.]+$",
        "(?<!x)[a-z0-9.]+$",
        r"^([a-z0-9]+)@\1\.example\.test$",
        r"^(?P<host>[a-z]+)@(?P=host)$",
        "^(?>a)$",
        r"^([a-z]+)?(?(1)@example\.test|admin)$",
        r"^[a-z0-9]+(?#the local part)@example\.test$",
        r"(?a)^[a-z0-9]+@example\.test$",
        r"^[a-z0-9]+@example\.test\Z",
        r"^[a-z0-9]+\N{COMMERCIAL AT}example\.test$",
    ):
        re.compile(pattern)  # a divergence, not a plain syntax error Python also refuses
        with pytest.raises(ValidationError):
            AddressShape.model_validate(dict(address, pattern=pattern))


def test_accepts_a_pattern_both_dialects_compile() -> None:
    """The false positive control on the paired regex validator.

    A scanner that refuses every ``(?`` would pass the rejection test above
    while making non capturing groups and named groups, which the Rust ``regex``
    crate compiles happily, unusable in a profile.
    """

    address = _profile()["address"]
    for pattern in (
        r"^(?:[a-z0-9]+)@example\.test$",
        r"^(?P<local>[a-z0-9]+)@example\.test$",
        r"(?i)^[A-Z0-9]+@example\.test$",
        r"^[a-z0-9(?=)\\]+@example\.test$",
        "^a*+$",
    ):
        assert AddressShape.model_validate(dict(address, pattern=pattern)).pattern == pattern


def test_wire_version_is_the_reply_module_literal() -> None:
    accepted = AdapterConformance.model_validate({"wire_version": REPLY_WIRE_VERSION})
    assert accepted.wire_version == REPLY_WIRE_VERSION
    with pytest.raises(ValidationError):
        AdapterConformance.model_validate({"wire_version": "2.0"})


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
    with pytest.raises(ProfileVersionError) as absent:
        load_profile(missing)
    assert "no version key" in str(absent.value)


def test_a_non_string_version_names_the_value_it_found() -> None:
    """A present key of the wrong type is not a missing key.

    ``version: 1.1`` written unquoted is a YAML FLOAT, so the key is sitting on
    line one of the file. Reporting that as "declares no version key" sends the
    author looking for something already there, and reporting the unknown
    property instead loses the version entirely.
    """

    with pytest.raises(ProfileVersionError) as numeric:
        load_profile(_profile(version=1.1, future_field=True))
    message = str(numeric.value)
    assert "1.1" in message
    assert "no version key" not in message
    assert "future_field" not in message
    assert '"1.0"' in message, "the message never states the quoted string form required"

    with pytest.raises(ProfileVersionError) as boolean:
        load_profile(_profile(version=True))
    assert "no version key" not in str(boolean.value)

    with pytest.raises(ProfileVersionError) as empty:
        load_profile(_profile(version=""))
    assert "no version key" not in str(empty.value)


def test_a_non_canonical_version_spelling_is_refused_as_a_spelling() -> None:
    """``01.0`` and ``1.00`` are refused, and the refusal says why.

    The Rust CLI compares the declared string to ``1.0`` for equality, so a
    Python side that parsed the two numbers and accepted these would take a
    profile ``curie adapter validate`` refuses. Reporting them as a plain
    mismatch leaves the operator staring at "understands 1.0, declares 01.0",
    two versions that read as equal, with nothing to act on.
    """

    for spelling in ("01.0", "1.00", "1.0.0", "1", "v1.0", " 1.0"):
        with pytest.raises(ProfileVersionError) as refused:
            load_profile(_profile(version=spelling))
        message = str(refused.value)
        assert "canonical" in message, f"{spelling} was not refused as a spelling: {message}"
        assert "leading zeros" in message

    understood = load_profile(_profile(version=PROFILE_VERSION))
    assert understood.version == PROFILE_VERSION

    with pytest.raises(ProfileVersionError) as newer:
        load_profile(_profile(version="1.1"))
    assert "canonical" not in str(newer.value), (
        "a version this build simply does not speak is being reported as a misspelling"
    )


def test_a_later_build_still_accepts_an_earlier_minor(monkeypatch: pytest.MonkeyPatch) -> None:
    """The canonical spelling rule does not cost the compatible minor policy.

    Same major and less or equal minor is what lets a third party keep a 1.0
    profile we cannot force it to upgrade. Asserted by standing ``check_version``
    up as a 1.1 build, because that policy is otherwise unobservable until the
    day someone bumps ``PROFILE_VERSION``.
    """

    monkeypatch.setattr("channel_protocol.manifest.PROFILE_VERSION", "1.1")
    check_version("1.0")
    check_version("1.1")
    with pytest.raises(ProfileVersionError) as older_build:
        check_version("1.2")
    assert "canonical" not in str(older_build.value)
    with pytest.raises(ProfileVersionError):
        check_version("2.0")
    for spelling in ("01.1", "1.10.0", "1.01"):
        with pytest.raises(ProfileVersionError) as misspelled:
            check_version(spelling)
        assert "canonical" in str(misspelled.value)


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
