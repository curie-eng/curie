"""The ACI protocol version and its semver compatibility policy.

This is the single source of truth for the wire version embedded in every
outbound event and in the exported JSON Schema. The ACI is versioned as semver,
independent of the Curie release: the number tracks the wire contract's
compatibility, not the product's cadence.

The compatibility rule a consumer applies is same ``major.minor`` under 0.x
(same ``major`` from 1.0 on). A new optional field is a patch (tolerant
consumers ignore it); every breaking change (new required field, new enum value,
removal, rename, type change) bumps the minor under 0.x. See ``packages/CLAUDE.md``
for the change-class table and ``docs/adr/0036-*`` for the rationale.

``WIRE_VERSION_FIELD`` names the wire field carrying the version. The exporters
special-case it by name (required, semver-constrained, version-guarded) rather
than introspecting a type, so removing the old ``Literal`` does not silently gut
the guard.
"""

import re
from typing import Final

PROTOCOL_VERSION: Final = "0.4.5"

# The wire field carrying the protocol version on every outbound event. Both
# exporters special-case this field by name; do not rename without updating them.
WIRE_VERSION_FIELD: Final = "version"

# A strict three-component numeric semver (major.minor.patch). Prerelease and
# build metadata are deliberately not accepted: the 0.x line does not ship them,
# and a malformed version is rejected rather than ordered. Each component is
# ASCII with no leading zeros; ``[0-9]`` (not ``\d``) so the pattern means the
# same thing in Python and in the ECMA-262 ``pattern`` emitted into the JSON
# Schema, where the Rust lane parses ASCII-only. The ``^...$`` anchors stay
# because JSON Schema/ECMA does not support ``\A\Z``; the trailing-newline slack
# they leave in Python is closed by matching with ``fullmatch`` below.
SEMVER_PATTERN: Final = r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$"

_SEMVER_RE = re.compile(SEMVER_PATTERN)


def is_compatible(wire: str, build: str) -> bool:
    """Return whether a consumer speaking ``build`` accepts a ``wire`` version.

    Compatibility is same ``major.minor`` under 0.x, same ``major`` from 1.0 on.
    Returns ``False`` (never raises) for any malformed input -- ``None``, ``""``,
    a non-semver string, a two-component ``"0.2"``, a prerelease/build string, a
    leading-zero component (``"0.02.0"``), a Unicode-digit string, or a trailing
    newline (``"0.2.0\n"``) -- so the NDJSON decoder can turn a bad version into
    ``ProtocolVersionError`` instead of a stray ``ValueError``/``IndexError``
    escaping its contract. ``fullmatch`` (not ``match``) is what rejects the
    trailing newline the ``$`` anchor would otherwise tolerate in Python.
    """

    if not isinstance(wire, str) or not isinstance(build, str):
        return False
    if not _SEMVER_RE.fullmatch(wire) or not _SEMVER_RE.fullmatch(build):
        return False
    w_major, w_minor, _ = (int(part) for part in wire.split("."))
    b_major, b_minor, _ = (int(part) for part in build.split("."))
    if b_major == 0:
        return w_major == 0 and w_minor == b_minor
    return w_major == b_major
