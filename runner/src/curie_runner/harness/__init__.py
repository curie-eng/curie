"""The harness contribution manifest and its entry-point registry (ADR-0060)."""

from .claude import CLAUDE_CONTRIBUTION, get_contribution
from .contribution import BundleCompileResult, HarnessContribution
from .registry import (
    BUILTIN_HARNESS_CANONICAL_PATHS,
    ENTRY_POINT_GROUP,
    FlatHarnessPackageError,
    HarnessNameCollisionError,
    MalformedHarnessContributionError,
    UnknownHarnessError,
    UnsupportedHarnessError,
    discover_contributions,
    resolve_harness,
)

__all__ = [
    "HarnessContribution",
    "BundleCompileResult",
    "CLAUDE_CONTRIBUTION",
    "get_contribution",
    "ENTRY_POINT_GROUP",
    "BUILTIN_HARNESS_CANONICAL_PATHS",
    "FlatHarnessPackageError",
    "HarnessNameCollisionError",
    "MalformedHarnessContributionError",
    "UnknownHarnessError",
    "UnsupportedHarnessError",
    "discover_contributions",
    "resolve_harness",
]
