"""Standalone profile contract. @spec TURN-CANARY-16 TURN-CANARY-17 TURN-CANARY-18"""

import json
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

ProbeReason = Literal["profile_match", "profile_mismatch"]


def _normalize_probe_text(text: str) -> str:
    """Canonical literal matching, never a regular expression. @spec TURN-CANARY-16"""
    return " ".join(unicodedata.normalize("NFKC", text).casefold().split())


def _probe_slug(slug: object) -> bool:
    """Bound the configuration key independently of route labels. @spec TURN-CANARY-16"""
    return (
        isinstance(slug, str)
        and 1 <= len(slug) <= 63
        and slug[0] in "abcdefghijklmnopqrstuvwxyz0123456789"
        and all(char in "abcdefghijklmnopqrstuvwxyz0123456789-" for char in slug)
    )


@dataclass(frozen=True)
class ProbeProfile:
    """Explicit read-only text contract. @spec TURN-CANARY-16 TURN-CANARY-18"""

    prompt: str
    required_any: tuple[tuple[str, ...], ...]
    forbidden_any: tuple[str, ...]
    forbidden_prefix_any: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate and freeze bounded fields. @spec TURN-CANARY-16 TURN-CANARY-18"""
        if not isinstance(self.prompt, str) or not self.prompt.strip() or len(self.prompt) > 2048:
            raise ValueError("probe prompt must be a bounded nonblank string")
        if not isinstance(self.required_any, (list, tuple)) or not 1 <= len(self.required_any) <= 8:
            raise ValueError("probe required groups must be bounded and nonempty")
        if not isinstance(self.forbidden_any, (list, tuple)) or len(self.forbidden_any) > 32:
            raise ValueError("probe forbidden markers must be a bounded list")
        if (
            not isinstance(self.forbidden_prefix_any, (list, tuple))
            or len(self.forbidden_prefix_any) > 32
        ):
            raise ValueError("probe forbidden prefix markers must be a bounded list")

        def marker(value: object) -> str:
            """Reject empty and unbounded literal markers. @spec TURN-CANARY-16"""
            if (
                not isinstance(value, str) or not 1 <= len(value) <= 128
                or not _normalize_probe_text(value)
            ):
                raise ValueError("probe markers must be bounded nonblank strings")
            return value

        groups = []
        for group in self.required_any:
            if not isinstance(group, (list, tuple)) or not 1 <= len(group) <= 8:
                raise ValueError("probe alternatives must be bounded and nonempty")
            groups.append(tuple(marker(value) for value in group))
        object.__setattr__(self, "required_any", tuple(groups))
        object.__setattr__(
            self, "forbidden_any", tuple(marker(value) for value in self.forbidden_any)
        )
        object.__setattr__(
            self, "forbidden_prefix_any",
            tuple(marker(value) for value in self.forbidden_prefix_any),
        )

    def matches(self, text: str) -> bool:
        """Evaluate literal groups and exclusions. @spec TURN-CANARY-16 TURN-CANARY-18"""
        normalized = _normalize_probe_text(text)
        return bool(normalized) and all(
            any(_normalize_probe_text(marker) in normalized for marker in group)
            for group in self.required_any
        ) and not any(
            _normalize_probe_text(marker) in normalized for marker in self.forbidden_any
        ) and not any(
            normalized.startswith(_normalize_probe_text(marker))
            for marker in self.forbidden_prefix_any
        )


def validate_probe_profiles(
    profiles: Mapping[str, ProbeProfile] | None,
) -> dict[str, ProbeProfile]:
    """Validate direct cycle callers before any platform operation. @spec TURN-CANARY-16"""
    if profiles is None:
        return {}
    if not isinstance(profiles, Mapping) or len(profiles) > 100 or not all(
        _probe_slug(slug) and isinstance(profile, ProbeProfile)
        for slug, profile in profiles.items()
    ):
        raise ValueError("probe profiles must be a bounded slug-to-profile mapping")
    return dict(profiles)


def parse_probe_profiles(raw: str) -> dict[str, ProbeProfile]:
    """Parse strict, bounded profile configuration. @spec TURN-CANARY-16 TURN-CANARY-18"""
    if len(raw.encode("utf-8")) > 65536:
        raise ValueError("probe profile JSON exceeds its byte limit")

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        """Reject duplicate keys at every object nesting level. @spec TURN-CANARY-16"""
        values = dict(pairs)
        if len(values) != len(pairs):
            raise ValueError("probe profile JSON contains duplicate keys")
        return values

    try:
        values = json.loads(raw, object_pairs_hook=unique_object)
    except RecursionError as exc:
        raise ValueError("probe profile JSON nesting exceeds parser limits") from exc
    if not isinstance(values, dict) or not 1 <= len(values) <= 100:
        raise ValueError("probe profiles must be a nonempty bounded object")
    profiles = {}
    required = {"prompt", "required_any", "forbidden_any"}
    allowed = required | {"forbidden_prefix_any"}
    for slug, value in values.items():
        if (
            not _probe_slug(slug) or not isinstance(value, dict)
            or not required <= value.keys() <= allowed
        ):
            raise ValueError("probe profile has invalid slug or fields")
        profiles[slug] = ProbeProfile(
            prompt=value["prompt"], required_any=value["required_any"],
            forbidden_any=value["forbidden_any"],
            forbidden_prefix_any=value.get("forbidden_prefix_any", ()),
        )
    return profiles
