"""The bump gate: a shape change cannot land at an unchanged profile version.

A third party commits an ``adapter.yaml`` we cannot force it to upgrade, so the
profile carries a compatibility policy: adding an optional property is a minor
bump, and adding a required one, removing or renaming one, changing a type or
tightening a pattern is a major bump. The drift gate in
``test_manifest_schema_compat.py`` proves the committed schema matches the
models; it says nothing about whether a change earned a bump.

The committed baseline is deliberately DEEPER than a names and required list.
A names only baseline stays green on exactly the changes the policy says must
bump: a tightened pattern, a changed type, a narrowed enum, a moved bound. So
the baseline records the canonical validation keywords per property, and
``description`` and ``title`` are excluded so a prose edit is correctly not a
shape change.

Refreshing the baseline is legal only in the commit that also bumps
``PROFILE_VERSION``, and the first test asserts that pairing.
"""

import copy
import json
from typing import Any

from channel_protocol.manifest import PROFILE_VERSION
from channel_protocol.manifest_schema import (
    baseline_path,
    build_baseline,
    build_schema,
    canonical_shape,
)


def _committed_baseline() -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(baseline_path().read_text(encoding="utf-8"))
    return loaded


def test_shape_matches_the_baseline_at_an_unchanged_version() -> None:
    committed = _committed_baseline()
    assert committed["version"] == PROFILE_VERSION, (
        "the committed baseline records a different profile version than the models; "
        "the baseline may only be refreshed in the commit that bumps PROFILE_VERSION"
    )
    assert build_baseline() == committed, (
        "the adapter profile shape changed at an unchanged version. Pick the bump from "
        "the change class table, bump PROFILE_VERSION, then regenerate the baseline with "
        "python -m channel_protocol.manifest_schema --write-baseline"
    )

    live = canonical_shape(build_schema())
    assert live == committed["models"]

    added_optional = copy.deepcopy(build_schema())
    added_optional["$defs"]["AdapterProfile"]["properties"]["retries"] = {"type": "integer"}
    assert canonical_shape(added_optional) != committed["models"], (
        "an added optional property is not recorded, so a minor bump could land unnoticed"
    )

    tightened = copy.deepcopy(build_schema())
    kind = tightened["$defs"]["AdapterProfile"]["properties"]["kind"]
    assert "pattern" in kind, "kind no longer carries an exported pattern"
    kind["pattern"] = r"^[a-z0-9]+(?:-[a-z0-9]+)*$"
    assert canonical_shape(tightened) != committed["models"], (
        "a tightened pattern is not recorded, so a file a conforming author already "
        "wrote could be invalidated at an unchanged version"
    )

    retyped = copy.deepcopy(build_schema())
    retyped["$defs"]["AdapterProfile"]["properties"]["kind"]["type"] = "integer"
    assert canonical_shape(retyped) != committed["models"], (
        "a changed property type is not recorded, which is the hole a names only "
        "baseline leaves open"
    )


def test_a_description_edit_is_not_a_shape_change() -> None:
    """The false positive control.

    A gate that reds on prose becomes noise, and noise gets refreshed
    reflexively, which is how the real shape changes get waved through.
    """

    edited = copy.deepcopy(build_schema())
    profile = edited["$defs"]["AdapterProfile"]
    profile["title"] = "Renamed in the docs only"
    profile["description"] = "A per install binding profile. Prose edited."
    profile["properties"]["kind"]["description"] = "The channel kind this adapter owns."

    assert canonical_shape(edited) == canonical_shape(build_schema())
