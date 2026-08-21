"""Routing a push through the bundle's deploy.yaml (ADR-0091, #1070).

ADR-0091 named its own sharpest edge and said it "should be the first test
written, not the last": a `deploy.yaml` naming an agent bound to a DIFFERENT
repository would let one repository's push deploy over another's agent. It is
the first test here.

These cover `resolve_target_agent` directly -- it is a pure function of
(targets, environment, the repo's agents, the possibly-foreign named agent), so
every routing decision is testable with no webhook, no clone and no database.
"""

from __future__ import annotations

import uuid

import pytest
from curie_api.gitflow import TargetUnresolved, _target_agent_name, resolve_target_agent
from curie_api.models import Agent, AgentChannel, Environment
from curie_test_support.scaffold import scaffolded_deploy_yaml
from plugin_format.deploy_targets import DeployTarget, DeployTargetsFile, validate_deploy_targets

REPO = "acme-corp/acme-bot"


def agent(name: str, repo: str | None = REPO) -> Agent:
    return Agent(
        id=uuid.uuid4(),
        name=name,
        channels=[AgentChannel(kind="slack", address=f"C-{name}")],
        repo_full_name=repo,
    )


def targets(text: str):
    import yaml

    parsed, errors = validate_deploy_targets(yaml.safe_load(text))
    assert not errors, errors
    return parsed


DEV_AND_PROD = """
targets:
  dev:
    agent: acme-dev
    env: dev
    slack_channel: C000000A01
  prod:
    agent: acme-bot
    env: prod
    slack_channel: C000000A02
"""


# --------------------------------------------------------------------------- #
# The sharpest edge, first
# --------------------------------------------------------------------------- #
def test_a_target_naming_another_repositorys_agent_is_refused() -> None:
    # Without this, anyone who can push to repo A deploys over repo B's bot by
    # naming it in their deploy.yaml.
    foreign = agent("acme-bot", repo="someone-else/their-bot")
    with pytest.raises(TargetUnresolved) as caught:
        resolve_target_agent(
            targets(DEV_AND_PROD), Environment.prod, [agent("acme-dev")], foreign
        )
    assert caught.value.code == "deploy.agent_bound_elsewhere"
    assert "another repository" in str(caught.value)


def test_an_unknown_agent_is_refused_rather_than_created() -> None:
    # A webhook does not mint agents. Creating one would let a push conjure a
    # bot on a channel of its choosing.
    with pytest.raises(TargetUnresolved) as caught:
        resolve_target_agent(targets(DEV_AND_PROD), Environment.prod, [agent("acme-dev")], None)
    assert caught.value.code == "deploy.unknown_agent"
    assert "does not exist" in str(caught.value)


# --------------------------------------------------------------------------- #
# The thing this is for: dev push -> dev agent, main push -> prod agent
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("environment", "expected"),
    [(Environment.dev, "acme-dev"), (Environment.prod, "acme-bot")],
)
def test_each_branch_reaches_its_own_agent(environment, expected) -> None:
    both = [agent("acme-bot"), agent("acme-dev")]
    resolved = resolve_target_agent(targets(DEV_AND_PROD), environment, both, None)
    assert resolved is not None and resolved.name == expected


def test_the_two_targets_resolve_to_different_agents() -> None:
    # The whole point of #1070. Before this, both branches resolved to whichever
    # single agent the repository row named, so a dev merge and a main merge
    # overwrote each other's active version.
    both = [agent("acme-bot"), agent("acme-dev")]
    dev = resolve_target_agent(targets(DEV_AND_PROD), Environment.dev, both, None)
    prod = resolve_target_agent(targets(DEV_AND_PROD), Environment.prod, both, None)
    assert dev is not None and prod is not None
    assert dev.id != prod.id


# --------------------------------------------------------------------------- #
# Ignore vs reject -- a distinction that decides whether silence is correct
# --------------------------------------------------------------------------- #
def test_a_branch_with_no_matching_target_is_ignored_not_rejected() -> None:
    # A repo may deploy only prod from main and leave dev to the CLI. Ignoring
    # matches how an unmatched branch already behaves.
    prod_only = targets(
        "targets:\n  prod:\n    agent: acme-bot\n    env: prod\n    slack_channel: C000000A02\n"
    )
    assert resolve_target_agent(prod_only, Environment.dev, [agent("acme-bot")], None) is None


def test_two_targets_for_one_environment_are_refused() -> None:
    # One push cannot deploy to two agents. Picking one silently would deploy
    # to an agent the author did not intend half the time.
    ambiguous = targets(
        "targets:\n"
        "  a:\n    agent: acme-bot\n    env: prod\n    slack_channel: C000000B01\n"
        "  b:\n    agent: other-bot\n    env: prod\n    slack_channel: C000000B02\n"
    )
    with pytest.raises(TargetUnresolved) as caught:
        resolve_target_agent(ambiguous, Environment.prod, [agent("acme-bot")], None)
    assert caught.value.code == "deploy.ambiguous_env"


def test_a_selected_target_missing_its_agent_is_refused() -> None:
    invalid = DeployTargetsFile(targets={"prod_target": DeployTarget(env="prod")})

    with pytest.raises(TargetUnresolved) as caught:
        resolve_target_agent(invalid, Environment.prod, [agent("acme-bot")], None)

    message = str(caught.value)
    assert caught.value.code == "deploy.missing_agent"
    assert "prod_target" in message
    assert "None" not in message


def test_a_missing_agent_is_reported_before_environment_ambiguity() -> None:
    invalid = DeployTargetsFile(
        targets={
            "missing_target": DeployTarget(env="prod"),
            "valid_target": DeployTarget(agent="acme-bot", env="prod"),
        }
    )

    with pytest.raises(TargetUnresolved) as caught:
        resolve_target_agent(invalid, Environment.prod, [agent("acme-bot")], None)

    message = str(caught.value)
    assert caught.value.code == "deploy.missing_agent"
    assert "missing_target" in message
    assert "None" not in message


def test_the_early_target_lookup_preserves_a_missing_agent() -> None:
    invalid = DeployTargetsFile(targets={"prod_target": DeployTarget(env="prod")})

    assert _target_agent_name(invalid, Environment.prod) is None


# --------------------------------------------------------------------------- #
# Bundles that predate deploy.yaml
# --------------------------------------------------------------------------- #
def test_a_bundle_without_deploy_yaml_still_deploys_its_single_agent() -> None:
    # Back-compat. Every repo that worked before ADR-0089 must keep working.
    only = agent("acme-bot")
    assert resolve_target_agent(None, Environment.prod, [only], None) is only


def test_a_bundle_without_deploy_yaml_and_several_agents_is_refused() -> None:
    # There is no basis to choose, and guessing deploys to the wrong bot
    # silently -- which is the #1070 bug, not a fallback for it.
    with pytest.raises(TargetUnresolved) as caught:
        resolve_target_agent(None, Environment.prod, [agent("a"), agent("b")], None)
    assert caught.value.code == "deploy.no_targets"
    assert "the bundle has no deploy.yaml" in str(caught.value)


# --------------------------------------------------------------------------- #
# Bundles WITH deploy.yaml that declare no targets (#1210)
# --------------------------------------------------------------------------- #
def test_an_empty_targets_map_still_deploys_its_single_agent() -> None:
    # `curie init` scaffolds `targets: {}`. Gating the fallback on the FILE's
    # presence instead of its CONTENT made every push from a scaffolded bundle
    # a silent no-op -- no deploy, no log line, no rejection.
    only = agent("acme-bot")
    assert resolve_target_agent(targets("targets: {}\n"), Environment.dev, [only], None) is only


def test_an_empty_targets_map_with_several_agents_is_refused() -> None:
    # Same reason the no-deploy.yaml case is refused: nothing declares which of
    # the two this branch deploys to, and guessing deploys to the wrong bot.
    with pytest.raises(TargetUnresolved) as caught:
        resolve_target_agent(
            targets("targets: {}\n"), Environment.dev, [agent("a"), agent("b")], None
        )
    assert caught.value.code == "deploy.no_targets"
    assert "the bundle's deploy.yaml declares an empty `targets:` map" in str(caught.value)


def test_a_deploy_yaml_of_only_comments_still_deploys_its_single_agent() -> None:
    # Broader than the literal `targets: {}` key: a file whose every line is
    # commented out parses to None, which validates to the same empty map. The
    # operator sees a deploy.yaml full of guidance and a push that does nothing.
    only = agent("acme-bot")
    commented = "# targets:\n#   dev:\n#     agent: acme-dev\n#     env: dev\n"
    parsed = targets(commented)
    assert parsed.targets == {}
    assert resolve_target_agent(parsed, Environment.dev, [only], None) is only


def test_the_scaffolded_deploy_yaml_is_valid_and_declares_no_targets() -> None:
    # The premise the routing test below rests on, asserted separately so a
    # reshaped scaffold fails HERE, naming the shape that changed, rather than
    # deep inside a routing assertion that would look like a routing bug.
    parsed = targets(scaffolded_deploy_yaml())
    assert parsed.targets == {}, "the scaffold is expected to ship no declared targets"


def test_the_scaffolded_deploy_yaml_deploys_the_repositorys_single_agent() -> None:
    # The regression as an operator meets it: `curie init`, push, nothing
    # happens. Pinned against the scaffold's real bytes so a change to what
    # ships lands here.
    only = agent("acme-bot")
    parsed = targets(scaffolded_deploy_yaml())
    assert resolve_target_agent(parsed, Environment.dev, [only], None) is only
