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
default.

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

# Constructs the Rust `regex` crate cannot compile. `(?:` and `(?<name>` are
# both fine there, so the lookbehind forms are matched on their `=` and `!`
# rather than on `(?<` alone.
_RUST_UNSUPPORTED_GROUPS = (
    ("(?=", "lookahead"),
    ("(?!", "negative lookahead"),
    ("(?<=", "lookbehind"),
    ("(?<!", "negative lookbehind"),
    ("(?P=", "a named backreference"),
)


class ProfileVersionError(Exception):
    """A profile declares a format version this build does not understand."""


def _version_parts(value: str) -> tuple[int, int]:
    major, separator, minor = value.partition(".")
    if not separator or not major.isdigit() or not minor.isdigit():
        raise ValueError(f"{value!r} is not a major.minor profile version")
    return int(major), int(minor)


def _is_acceptable(declared: str) -> bool:
    """Same major, less or equal minor.

    A 1.0 build refuses 1.1 because it cannot know the property 1.1 added is
    optional, and refuses 2.0 outright. A later 1.1 build still accepts a 1.0
    file, which is what lets a third party commit a profile we cannot force it
    to upgrade.
    """

    understood_major, understood_minor = _version_parts(PROFILE_VERSION)
    try:
        declared_major, declared_minor = _version_parts(declared)
    except ValueError:
        return False
    return declared_major == understood_major and declared_minor <= understood_minor


def read_profile_version(raw: Mapping[str, Any]) -> str:
    """Read the raw ``version`` key, before anything else looks at the payload."""

    declared = raw.get("version")
    if not isinstance(declared, str) or not declared:
        raise ProfileVersionError(
            f"this curie understands adapter profile {PROFILE_VERSION}; "
            "the profile declares no version key"
        )
    return declared


def check_version(declared: str) -> None:
    """Refuse a version this build cannot read, naming BOTH versions."""

    if _is_acceptable(declared):
        return
    raise ProfileVersionError(
        f"this curie understands adapter profile {PROFILE_VERSION}; "
        f"the profile declares {declared}"
    )


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
            "and the Rust regex crate, so lookaround and backreferences are refused."
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
    mints_reply_ref: bool = Field(
        description=(
            "Whether the adapter mints an opaque reply_ref the platform can address "
            "a later edit to. False means every reply is a fresh post."
        )
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
