"""Choosing a ticket's repository at admission (ADR 0197 identity 3)."""

from __future__ import annotations

from collections.abc import Collection
from typing import Any

import pytest
from curie_api.forges.config import BindingConfig, ForgesConfig, RepositoryBindingConfig
from curie_api.forges.resolution import (
    REFUSAL_MARKER,
    Refusal,
    refusal_body,
    resolve_repository,
)
from forge_fakes import config_samples as samples


def _jira(**overrides: Any) -> tuple[ForgesConfig, BindingConfig]:
    config = ForgesConfig.model_validate(samples.jira_document(**overrides))
    return config, config.bindings[0]


def _resolve(
    labels: Collection[str] = (),
    components: Collection[str] = (),
    binding_overrides: dict[str, Any] | None = None,
) -> RepositoryBindingConfig | Refusal:
    config, binding = _jira(**(binding_overrides or {}))
    return resolve_repository(
        binding, tracker=config.tracker(binding.tracker), labels=labels, components=components
    )


def _alias(result: RepositoryBindingConfig | Refusal) -> str:
    assert isinstance(result, RepositoryBindingConfig), result
    return result.alias


def test_a_native_tracker_runs_on_its_own_repository_whatever_the_labels() -> None:
    config = ForgesConfig.model_validate(samples.github_document())
    (binding,) = config.bindings
    chosen = resolve_repository(
        binding,
        tracker=config.tracker(binding.tracker),
        labels={"repo:web", "repo:infra", "bug"},
        components={"Frontend"},
    )
    assert chosen == binding.repos[0]


def test_a_jira_ticket_with_no_label_or_component_runs_on_the_default() -> None:
    assert _alias(_resolve(labels={"bug", "curie"})) == "web"


def test_a_mapped_component_overrides_the_default() -> None:
    assert _alias(_resolve(components={"Platform"})) == "infra"


def test_an_unmapped_component_is_ignored() -> None:
    assert _alias(_resolve(components={"Billing"})) == "web"
    assert _alias(_resolve(components={"Billing", "Platform"})) == "infra"


def test_components_that_agree_choose_their_repository() -> None:
    agreeing = {"components": {"Frontend": "infra", "Platform": "infra"}}
    assert _alias(_resolve(components={"Frontend", "Platform"}, binding_overrides=agreeing)) == (
        "infra"
    )


def test_a_repo_label_overrides_components_and_the_default() -> None:
    assert _alias(_resolve(labels={"repo:infra"}, components={"Frontend"})) == "infra"
    assert _alias(_resolve(labels={"repo:web"})) == "web"


def test_a_repo_label_decides_when_components_disagree() -> None:
    both = {"Frontend", "Platform"}
    assert _alias(_resolve(labels={"repo:web"}, components=both)) == "web"


def test_components_that_disagree_with_no_label_are_refused() -> None:
    refused = _resolve(components={"Frontend", "Platform"})
    assert isinstance(refused, Refusal) and refused.code == "repo_component_conflict"
    assert "`infra`" in refused.reason and "`web`" in refused.reason


def test_more_than_one_repo_label_is_refused() -> None:
    refused = _resolve(labels={"repo:web", "repo:infra"})
    assert isinstance(refused, Refusal) and refused.code == "repo_conflict"
    assert "`repo:infra`, `repo:web`" in refused.reason


def test_an_unknown_alias_is_refused_naming_the_known_ones() -> None:
    refused = _resolve(labels={"repo:mobile"})
    assert isinstance(refused, Refusal) and refused.code == "repo_unknown"
    assert "`mobile`" in refused.reason and "(`infra`, `web`)" in refused.reason


def test_a_label_with_backticks_is_rendered_as_one_code_span() -> None:
    refused = _resolve(labels={"repo:`x`"})
    assert isinstance(refused, Refusal)
    assert "`` `x` ``" in refused.reason


def test_the_refusal_comment_carries_its_marker_and_the_reason() -> None:
    refused = _resolve(labels={"repo:mobile"})
    assert isinstance(refused, Refusal)
    body = refusal_body(refused)
    assert body.startswith("Curie did not start this issue: repository `mobile`")
    assert "Fix the `repo:` label" in body and body.rstrip().endswith(REFUSAL_MARKER)


def test_another_bindings_tracker_is_refused() -> None:
    config, binding = _jira()
    other = ForgesConfig.model_validate(samples.github_document()).trackers[0]
    with pytest.raises(ValueError, match="is not bound to tracker"):
        resolve_repository(binding, tracker=other, labels=set(), components=set())
    assert resolve_repository(
        binding, tracker=config.tracker("jira"), labels=set(), components=set()
    )
