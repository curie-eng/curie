"""Deterministic protected runtime lane, @spec PROTECTED-HOOK-LANE-6."""

import hashlib
from dataclasses import dataclass

from .admission_records import _encode, _require
from .authority_records import Manifest, _scalar


@dataclass(frozen=True, slots=True)
class LaneNamespace:
    """Immutable derived broker names, never caller supplied."""

    prefix: str
    group: str
    stream: str
    dead: str


def derive_lane_namespace(manifest: Manifest) -> LaneNamespace:
    """Bind all lane names to the existing immutable runtime generation."""
    _require(type(manifest) is Manifest)
    m = manifest.as_dict()
    _scalar(m["runtime_id"], "uuid")
    _scalar(m["runtime_generation"], "generation")
    digest = hashlib.sha256(_encode([m["runtime_id"], m["runtime_generation"]])).hexdigest()
    prefix = "protected:lane:worker:" + digest
    return LaneNamespace(prefix, "protected-" + digest, "curie:runs", prefix + ":dead")
