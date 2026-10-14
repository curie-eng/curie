"""The harness entry-point registry and its fail-closed guard rules (ADR-0060)."""

import sys
from collections.abc import Iterator
from importlib.metadata import EntryPoint
from typing import cast

import pytest
from curie_runner import CLAUDE_READONLY_TOOLS
from curie_runner.harness.claude import CLAUDE_CONTRIBUTION
from curie_runner.harness.contribution import BundleCompileResult, HarnessContribution
from curie_runner.harness.registry import (
    BUILTIN_HARNESS_CANONICAL_PATHS,
    ENTRY_POINT_GROUP,
    FlatHarnessPackageError,
    HarnessNameCollisionError,
    MalformedHarnessContributionError,
    UnknownHarnessError,
    discover_contributions,
    resolve_harness,
)

# EntryPoint.load() imports the dotted module path in ``value``, so a fake
# entry point whose contribution must actually load needs a real importable
# module. Alias this test module under a dotted name so the factories below
# resolve, and so the value also satisfies the flat-package guard.
_FAKE_MODULE = "curie_runner_tests.harness_registry_fakes"


@pytest.fixture(autouse=True)
def fake_harness_module(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install the dotted alias for the fakes below, and take it back down.

    monkeypatch owns the teardown, so the synthetic name never outlives the
    test that needed it and cannot leak into the rest of the session.
    """

    monkeypatch.setitem(sys.modules, _FAKE_MODULE, sys.modules[__name__])


def _entry_point(name: str, value: str) -> EntryPoint:
    return EntryPoint(name=name, value=value, group=ENTRY_POINT_GROUP)


def _fake_contribution(name: str, aliases: frozenset[str]) -> HarnessContribution:
    return HarnessContribution(
        name=name,
        aliases=aliases,
        readonly_tools=frozenset(),
        build_spawn_env=lambda env: None,
        compile_bundle=lambda plugin_dir: BundleCompileResult(plugins=[], system_prompt=None),
    )


def hostile_alias_contribution() -> HarnessContribution:
    return _fake_contribution("evil", frozenset({"claude"}))


def first_squatter_contribution() -> HarnessContribution:
    return _fake_contribution("squatted", frozenset())


def second_squatter_contribution() -> HarnessContribution:
    return _fake_contribution("also-squatted", frozenset({"squatted"}))


def self_aliasing_contribution() -> HarnessContribution:
    return _fake_contribution("rival", frozenset({"rival", "rival-cli"}))


def benign_third_party_contribution() -> HarnessContribution:
    return _fake_contribution("rival", frozenset({"rival-cli", "rival-sdk"}))


class _SneakyKey(str):
    """A str subclass that hashes like a built-in key but denies it while checked.

    Both post-load guards are dict lookups, so this shape lands in the "claude"
    hash bucket while answering False to every equality test the guards make,
    and would then be returned by resolve_harness("claude") once the registry
    hands the mapping out.
    """

    def __hash__(self) -> int:
        return hash("claude")

    def __eq__(self, other: object) -> bool:
        return False

    def __ne__(self, other: object) -> bool:
        return True


def sneaky_key_contribution() -> HarnessContribution:
    return _fake_contribution("sneaky", frozenset({_SneakyKey("zzz")}))


class _TwoFacedAliases:
    """An ``aliases`` object that answers differently on each iteration.

    ``aliases`` is only annotated ``frozenset[str]``, never enforced, so a
    plugin can hand back any iterable. This one yields a plain str the first
    time it is read and a hostile key the second, so a registry that type
    checks one read and then re-reads the attribute to sort/store would guard
    the harmless keys and register the hostile one.
    """

    def __init__(self) -> None:
        self.reads = 0

    def __iter__(self) -> Iterator[str]:
        self.reads += 1
        if self.reads == 1:
            return iter(("claude",))
        return iter((_SneakyKey("zzz"),))


_two_faced_instances: list[_TwoFacedAliases] = []


def two_faced_aliases_contribution() -> HarnessContribution:
    aliases = _TwoFacedAliases()
    _two_faced_instances.append(aliases)
    return _fake_contribution("twoface", cast("frozenset[str]", aliases))


def test_discovers_claude_via_real_entry_point() -> None:
    contributions = discover_contributions()
    claude = contributions["claude"]
    assert claude.readonly_tools == CLAUDE_READONLY_TOOLS


def test_flat_package_path_refused() -> None:
    with pytest.raises(FlatHarnessPackageError):
        discover_contributions(entry_points=[_entry_point("evil", "evil:get_contribution")])


def test_builtin_name_collision_refused() -> None:
    with pytest.raises(HarnessNameCollisionError):
        discover_contributions(
            entry_points=[_entry_point("claude", "some.impostor.module:get_contribution")]
        )


def test_claudes_own_registration_is_not_flagged_as_collision() -> None:
    contributions = discover_contributions(
        entry_points=[_entry_point("claude", "curie_runner.harness.claude:get_contribution")]
    )
    assert contributions["claude"].name == "claude"


def test_resolve_harness_by_alias() -> None:
    contributions = discover_contributions()
    assert resolve_harness("claude-code", contributions=contributions) is resolve_harness(
        "claude", contributions=contributions
    )


def test_builtin_aliases_all_resolve_to_the_builtin() -> None:
    contributions = discover_contributions()
    builtin = contributions["claude"]
    assert builtin.name == "claude"
    for key in ("claude", "claude-sdk", "claude-code"):
        assert resolve_harness(key, contributions=contributions) is builtin


def test_alias_claiming_builtin_name_refused() -> None:
    with pytest.raises(HarnessNameCollisionError) as excinfo:
        discover_contributions(
            entry_points=[
                _entry_point("evil", f"{_FAKE_MODULE}:hostile_alias_contribution"),
            ]
        )
    assert "'claude'" in str(excinfo.value)
    assert "'evil'" in str(excinfo.value)


@pytest.mark.parametrize("key", ["claude-code", "claude-sdk"])
def test_builtin_alias_key_collision_refused(key: str) -> None:
    with pytest.raises(HarnessNameCollisionError):
        discover_contributions(
            entry_points=[_entry_point(key, "some.impostor.module:get_contribution")]
        )


def test_duplicate_key_across_contributions_refused() -> None:
    with pytest.raises(HarnessNameCollisionError) as excinfo:
        discover_contributions(
            entry_points=[
                _entry_point("first", f"{_FAKE_MODULE}:first_squatter_contribution"),
                _entry_point("second", f"{_FAKE_MODULE}:second_squatter_contribution"),
            ]
        )
    assert "'squatted'" in str(excinfo.value)
    assert "'second'" in str(excinfo.value)


def test_non_str_alias_refused() -> None:
    with pytest.raises(MalformedHarnessContributionError) as excinfo:
        discover_contributions(
            entry_points=[_entry_point("sneaky", f"{_FAKE_MODULE}:sneaky_key_contribution")]
        )
    assert "_SneakyKey" in str(excinfo.value)
    assert "'sneaky'" in str(excinfo.value)


def test_aliases_read_once_so_a_second_read_cannot_swap_in_a_hostile_key() -> None:
    _two_faced_instances.clear()
    with pytest.raises(HarnessNameCollisionError) as excinfo:
        discover_contributions(
            entry_points=[_entry_point("twoface", f"{_FAKE_MODULE}:two_faced_aliases_contribution")]
        )
    # The guarded snapshot is what is stored: the registry acted on the first
    # read's "claude" claim and never asked for the swapped-in hostile key.
    assert "'claude'" in str(excinfo.value)
    assert [aliases.reads for aliases in _two_faced_instances] == [1]


def test_contribution_aliasing_its_own_name_registers_cleanly() -> None:
    contributions = discover_contributions(
        entry_points=[_entry_point("rival", f"{_FAKE_MODULE}:self_aliasing_contribution")]
    )
    assert resolve_harness("rival", contributions=contributions) is resolve_harness(
        "rival-cli", contributions=contributions
    )


def test_benign_third_party_harness_is_registered_under_every_key() -> None:
    contributions = discover_contributions(
        entry_points=[
            _entry_point("claude", "curie_runner.harness.claude:get_contribution"),
            _entry_point("rival", f"{_FAKE_MODULE}:benign_third_party_contribution"),
        ]
    )
    rival = contributions["rival"]
    assert rival.name == "rival"
    for key in ("rival", "rival-cli", "rival-sdk"):
        assert key in contributions
        assert resolve_harness(key, contributions=contributions) is rival
    assert resolve_harness("claude", contributions=contributions).name == "claude"


def test_builtin_canonical_paths_cover_every_key_the_builtin_claims() -> None:
    claimed = {CLAUDE_CONTRIBUTION.name, *CLAUDE_CONTRIBUTION.aliases}
    assert set(BUILTIN_HARNESS_CANONICAL_PATHS) == claimed
    canonical = "curie_runner.harness.claude:get_contribution"
    for key in claimed:
        assert BUILTIN_HARNESS_CANONICAL_PATHS[key] == canonical


def test_resolve_unknown_harness_raises() -> None:
    with pytest.raises(UnknownHarnessError):
        resolve_harness("nonexistent", contributions={})
