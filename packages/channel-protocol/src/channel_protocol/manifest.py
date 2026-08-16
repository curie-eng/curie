"""The adapter binding profile (``adapter.yaml``) one install commits (#1516).

This is NOT ADR-0096 decision 2's install agnostic composition manifest (image
reference, config and secret schema, platform performed composition). That
document is separate and later, and nothing in this module pre empts it. What
is modelled here is the PER INSTALL binding profile: the channel kind an
adapter owns, the address shape it accepts, the endpoint this install's worker
POSTs reply events to, and the names of the credentials involved.

**The version check runs FIRST.** ``load_profile`` reads the raw ``version``
key and refuses an unacceptable one before any schema validation, naming both
the version this build understands and the version the file declares. Run it
the other way round and a 1.1 file trips the closed schema first, so the
operator reads "additional property not allowed" instead of the version they
actually have to act on. A missing ``version`` key is the same refusal, never a
default, and a key PRESENT with a non string value is a THIRD refusal that
names the value it found: unquoted ``version: 1.1`` is a YAML float, and
reporting that as a missing key sends the author looking for a key that is
already there. A non canonical SPELLING such as ``01.0`` is a FOURTH refusal
that says so in those terms: the Rust CLI compares the declared string to
``1.0`` for equality, so accepting it here would take a profile
``curie adapter validate`` refuses.

**Three validation tiers, unequally enforced, and the asymmetry is deliberate.**

* Tier 1 and tier 2 (the slugs, the environment variable names, the two
  ``Literal`` versions, the endpoint shape) are ``Field`` constraints, so they
  export into ``schema/adapter-profile.schema.json`` and the Rust CLI gets them
  for free. For these the exported schema genuinely is the single source.
* Tier 3 is ``address.pattern``'s compilability under the Rust ``regex``
  crate. JSON Schema has no keyword for "the Rust regex crate can compile this
  string" and no extension both a Python and a Rust validator would honour, so
  this rule is an ADMITTED PAIRED VALIDATOR: one implementation here, one in
  the Rust CLI, with ``schema/adapter-profile.corpus.json`` as the drift gate
  between them. The exported schema ACCEPTS every tier 3 case and
  ``tests/test_manifest_corpus.py`` asserts that it does, so nobody later reads
  the exported schema as the sole authority and drops the Rust half.

Every regex this module puts into the schema is lookaround free, because the
Rust ``regex`` crate refuses lookaround and would fail to compile the exported
constraint it is handed.
"""

import re
from collections.abc import Mapping
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .reply import ReplyWireVersion

ProfileVersion = Literal["1.0"]
PROFILE_VERSION: ProfileVersion = "1.0"

_STRICT = ConfigDict(extra="forbid")

# Copied VERBATIM from apps/api/src/curie_api/schemas.py::_CHANNEL_KIND. The API
# owns the slug rule and this is a copy of it, so a tightening on either side
# without the other would let the CLI accept a value the API refuses.
# tests/test_manifest.py asserts the two strings are byte identical. Underscores
# are legal: `ms_teams` is a kind the API accepts.
_SLUG_PATTERN = r"^[a-z0-9]+(?:[-_][a-z0-9]+)*$"

# A shell environment variable NAME. This is a LEGIBILITY constraint and it
# claims nothing whatsoever about secrecy: `DEADBEEF` and `ABC123` both match
# it. Preventing a committed secret VALUE is the repo's gitleaks gate, the only
# mechanism that inspects values. What the pattern buys is that a field meant to
# hold `CURIE_EGRESS_SECRET` cannot hold a sentence or a URL.
_ENV_NAME_PATTERN = r"^[A-Z][A-Z0-9_]*$"

# An absolute http or https URL with a host and no userinfo. Lookaround free so
# the Rust `regex` crate compiles the exported constraint. Userinfo is excluded
# by forbidding `@` in the authority, which makes
# `https://user:pw@host/x` fail rather than parse as a host of `user:pw@host`.
# This is FAIL FAST FEEDBACK, not a security control: the API's
# `_validate_channel_endpoint` remains the authority on what may be stored.
_ENDPOINT_PATTERN = r"^https?://[^\s/?#@]+(?:[/?#][^\s]*)?$"

# Group openers the Rust `regex` crate cannot compile while Python `re` can.
# `(?:`, `(?P<name>` and the `i m s x u` flag letters are fine on both sides, so
# the lookbehind forms are matched on their `=` and `!` rather than on `(?<`
# alone, and only the Python only `a` flag letter is listed. Every entry here
# was checked against regex 1.13.1 by compiling it: a construct that crate in
# fact accepts must not be listed, or this validator refuses a profile
# `curie adapter validate` would take.
_RUST_UNSUPPORTED_GROUPS = (
    ("(?=", "lookahead"),
    ("(?!", "negative lookahead"),
    ("(?<=", "lookbehind"),
    ("(?<!", "negative lookbehind"),
    ("(?P=", "a named backreference"),
    ("(?>", "an atomic group"),
    ("(?(", "a conditional group"),
    ("(?#", "an inline comment group"),
    ("(?a", "the Python only ASCII flag"),
)

# Escapes in the same class, keyed by the single character after the backslash.
# `\Z` is Python's end of string anchor and the crate spells that `\z`; `\N` is
# Python's named character escape and the crate has no equivalent. Both are
# refused inside a character class too, which is correct: the crate rejects them
# there as well.
_RUST_UNSUPPORTED_ESCAPES = {
    "Z": "the Python only \\Z anchor",
    "N": "the Python only \\N named character escape",
}


class ProfileVersionError(Exception):
    """A profile declares a format version this build does not understand."""


# The ONLY spelling of a profile version: two plain decimal numbers, neither
# carrying a leading zero. Parsing `major.minor` numerically instead would make
# `01.0` and `1.00` acceptable here, and the Rust CLI compares the declared
# string to `1.0` for equality and refuses both. Python is the side that moves,
# because `01.0` is malformed under any honest reading of the contract and
# tightening here costs the Rust half nothing.
_CANONICAL_VERSION = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")


def _version_parts(value: str) -> tuple[int, int]:
    match = _CANONICAL_VERSION.fullmatch(value)
    if match is None:
        raise ValueError(f"{value!r} is not a canonical major.minor profile version")
    return int(match.group(1)), int(match.group(2))


def _is_acceptable(declared: tuple[int, int]) -> bool:
    """Same major, less or equal minor.

    A 1.0 build refuses 1.1 because it cannot know the property 1.1 added is
    optional, and refuses 2.0 outright. A later 1.1 build still accepts a 1.0
    file, which is what lets a third party commit a profile we cannot force it
    to upgrade.
    """

    understood = _version_parts(PROFILE_VERSION)
    return declared[0] == understood[0] and declared[1] <= understood[1]


def read_profile_version(raw: Mapping[str, Any]) -> str:
    """Read the raw ``version`` key, before anything else looks at the payload.

    A missing key and a key holding something other than a non empty string are
    DIFFERENT refusals. Collapsing them reports ``version: 1.1`` (a YAML float,
    because it was written unquoted) as "declares no version key", which sends
    the author hunting for a key that is sitting on line one of their file.
    """

    understood = f"this curie understands adapter profile {PROFILE_VERSION}; "
    if "version" not in raw:
        raise ProfileVersionError(understood + "the profile declares no version key")
    declared = raw["version"]
    if not isinstance(declared, str):
        raise ProfileVersionError(
            understood + f"the profile declares version {declared!r}, which is a "
            f"{type(declared).__name__} and not a string. A profile version is a quoted "
            f'string, so write version: "{PROFILE_VERSION}"'
        )
    if not declared:
        raise ProfileVersionError(
            understood + "the profile declares an empty version key. A profile version is "
            f'a quoted string, so write version: "{PROFILE_VERSION}"'
        )
    return declared


def check_version(declared: str) -> None:
    """Refuse a version this build cannot read, naming BOTH versions.

    A non canonical SPELLING is its own refusal, separate from a version this
    build does not speak. An operator who typed ``01.0`` needs to be told the
    spelling is wrong: told only "the profile declares 01.0" against an
    understood 1.0, they read a version mismatch between two versions that look
    equal and have nowhere to go.
    """

    understood = f"this curie understands adapter profile {PROFILE_VERSION}; "
    try:
        parts = _version_parts(declared)
    except ValueError:
        raise ProfileVersionError(
            understood + f"the profile declares version {declared!r}, which is not a "
            "canonical major.minor version. Both numbers are plain digits with no "
            f'leading zeros, so write version: "{PROFILE_VERSION}"'
        ) from None
    if _is_acceptable(parts):
        return
    raise ProfileVersionError(understood + f"the profile declares {declared}")


def _rust_unsupported_construct(pattern: str) -> str | None:
    """Name the first construct the Rust ``regex`` crate would refuse, or None.

    Character classes are skipped, because `(` and `\\` mean something else
    inside them and a naive scan would refuse a pattern both dialects accept.
    """

    index = 0
    in_class = False
    while index < len(pattern):
        char = pattern[index]
        if char == "\\":
            following = pattern[index + 1 : index + 2]
            if following.isdigit() and following != "0":
                return f"the backreference \\{following}"
            if following == "g":
                return "a backreference"
            unsupported = _RUST_UNSUPPORTED_ESCAPES.get(following)
            if unsupported is not None:
                return unsupported
            index += 2
            continue
        if in_class:
            in_class = char != "]"
            index += 1
            continue
        if char == "[":
            in_class = True
            index += 1
            continue
        for prefix, name in _RUST_UNSUPPORTED_GROUPS:
            if pattern.startswith(prefix, index):
                return name
        index += 1
    return None


class AddressShape(BaseModel):
    """The address form this adapter owns, as a regex plus an operator legible example.

    ``pattern`` is the tier 3 paired validator: the Rust ``regex`` dialect is
    the floor, so a pattern Python ``re`` compiles and Rust refuses is rejected
    here too. Without that, a profile would validate in the Python kit and fail
    in ``curie adapter validate`` with no shared rule to point at.
    """

    model_config = _STRICT

    description: str = Field(description="What this address identifies, in the channel's terms.")
    pattern: str = Field(
        min_length=1,
        description=(
            "A regex the address must match. It has to compile in BOTH Python re "
            "and the Rust regex crate, so lookaround, backreferences, atomic and "
            "conditional groups, inline comments and Python only escapes are refused."
        ),
    )
    example: str = Field(description="A concrete address of this shape, for operator docs.")

    @field_validator("pattern")
    @classmethod
    def _compiles_in_both_dialects(cls, value: str) -> str:
        try:
            re.compile(value)
        except re.error as error:
            raise ValueError(f"address.pattern is not a valid regex: {error}") from error
        construct = _rust_unsupported_construct(value)
        if construct is not None:
            raise ValueError(
                f"address.pattern uses {construct}, which the Rust regex crate cannot "
                "compile, so curie adapter validate would refuse the same profile"
            )
        return value


class AdapterCredentials(BaseModel):
    """Which credential identities this adapter expects, by NAME only.

    ``egress`` is a non binding SUGGESTION. It is authority for nothing: the
    worker's credential map is indexed by the slug the binding carries, so a
    profile that named another adapter's slug would redirect that adapter's
    secret. ``curie adapter bind`` therefore requires an operator supplied slug
    and the profile's value is only ever a default shown for confirmation.

    ``egress_secret_env`` and ``ingress_token_env`` are DOCUMENTATION of what
    the adapter itself reads. Nothing in Curie resolves them: the conformance
    kit and the CLI take a secret at the invocation boundary instead, so a
    profile can never choose which of an operator's secrets gets read.
    """

    model_config = _STRICT

    egress: str = Field(
        pattern=_SLUG_PATTERN,
        description=(
            "The egress credential identity this adapter suggests. A SUGGESTION only: "
            "bind requires an operator supplied slug and never takes this value alone."
        ),
    )
    egress_secret_env: str = Field(
        pattern=_ENV_NAME_PATTERN,
        description=(
            "The environment variable the ADAPTER reads its egress secret from. "
            "Documentation only. Curie never resolves this name."
        ),
    )
    ingress_token_env: str = Field(
        pattern=_ENV_NAME_PATTERN,
        description=(
            "The environment variable the ADAPTER reads its ingress token from. "
            "Documentation only. Curie never resolves this name."
        ),
    )


class AdapterConformance(BaseModel):
    """What this adapter claims about the reply wire it speaks.

    ``wire_version`` is the reply wire (ADR-0096), imported from ``reply.py``
    and never re typed here. It is INDEPENDENT of the profile's own ``version``:
    conflating the two would let a wire bump silently invalidate every profile.
    """

    model_config = _STRICT

    wire_version: ReplyWireVersion = Field(
        description="The reply wire version this adapter speaks, from channel_protocol.reply."
    )


class AdapterProfile(BaseModel):
    """One install's binding profile for one adapter.

    Closed by construction (``extra='forbid'``), so a typo'd key is refused
    instead of silently ignored. Read it through ``load_profile``, never
    through ``model_validate`` directly: the version check has to run first.
    """

    model_config = _STRICT

    version: ProfileVersion = Field(
        description="The adapter profile format version this file is written against."
    )
    kind: str = Field(
        pattern=_SLUG_PATTERN,
        description="The channel kind this adapter owns, as a lowercase slug.",
    )
    endpoint: str | None = Field(
        default=None,
        pattern=_ENDPOINT_PATTERN,
        description=(
            "The absolute http or https route this install's worker POSTs reply events "
            "to. OPTIONAL here: the verbs that need a concrete route (bind, token, "
            "smoke-test) require it at the verb boundary or take an override."
        ),
    )
    address: AddressShape
    credentials: AdapterCredentials
    conformance: AdapterConformance


def load_profile(raw: Mapping[str, Any]) -> AdapterProfile:
    """Parse a profile payload, version check FIRST.

    This is the entry point every consumer uses. ``AdapterProfile.model_validate``
    on its own skips the acceptance rule and reports a closed schema violation
    where the operator needed to read a version.
    """

    check_version(read_profile_version(raw))
    return AdapterProfile.model_validate(raw)
