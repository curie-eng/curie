"""Pins that keep examples/sre-bot's shipped files honest about what they do.

Four independent ways this example has already lied, each of which reached a
reviewer as a confident wrong answer rather than as a missing file:

1. ``docs/STAGING-DEPLOY.md`` described four hand-applied deltas and never
   named the self-upgrade connector or the platform-upgrade Job the tree
   actually installs today. A reviewer following it would reproduce a bot
   that cannot start either upgrade.
2. ``deploy.yaml`` shipped ``C0EXAMPLE1`` / ``C0EXAMPLE2`` as live
   ``slack_channel`` values. Those match the Slack id shape, so
   ``curie cluster deploy --target`` reports success and rebinds the bot to
   a channel that does not exist. The bot then goes silent with nothing in
   any log naming the cause.
3. ``docs/PERMISSION-MAP.md`` said ``restart_deployment`` "ships commented
   out" while ``connectors.yaml`` declares ``k8s-write`` live. The document
   that claims to list every write is then wrong about the one write that
   actually ships.
4. ``platform-upgrade/upgrade.sh`` is the script the platform-upgrade Job
   runs, and nothing in CI shellchecked it. A script with zero tests and no
   CI is a script whose next edit lands unreviewed.

Each test below fails closed on the matching lie. They are deliberately
shallow -- they read the shipped files and the workflow -- because the
failure is the file saying the wrong thing, not a runtime of the bot.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import yaml
from plugin_format.deploy_targets import validate_deploy_targets

REPO = Path(__file__).resolve().parents[2]
BUNDLE = REPO / "examples" / "sre-bot"
CI_WORKFLOW = REPO / ".github" / "workflows" / "ci.yaml"
UPGRADE_SCRIPT = BUNDLE / "platform-upgrade" / "upgrade.sh"
# Named allowlisted ids, not a prefix-plus-digit regex. A bare prefix
# of the sanctioned placeholders matches slack-conversation-id and is
# not a gitleaks stopword; C0EXAMPLE1 and C0EXAMPLE2 are.
PLACEHOLDER_CHANNELS = frozenset({"C0EXAMPLE1", "C0EXAMPLE2"})


def test_staging_deploy_doc_names_what_the_tree_installs_today() -> None:
    """The reproduction doc must describe the installer that exists, including
    the two upgrade paths it currently never mentioned."""

    text = (BUNDLE / "docs" / "STAGING-DEPLOY.md").read_text(encoding="utf-8")
    assert "curie example sre-bot install" in text, (
        "docs/STAGING-DEPLOY.md must name `curie example sre-bot install`; "
        "that is the command the tree actually uses to stand this bot up"
    )
    assert "--platform-upgrade" in text, (
        "docs/STAGING-DEPLOY.md never mentions --platform-upgrade, so a "
        "reviewer following it cannot reproduce the platform-upgrade Job "
        "the tree installs"
    )
    assert "self-upgrade" in text, (
        "docs/STAGING-DEPLOY.md never mentions self-upgrade, so a reviewer "
        "following it cannot reproduce the connector the tree ships"
    )
    assert "platform-upgrade" in text, (
        "docs/STAGING-DEPLOY.md never mentions platform-upgrade, so a "
        "reviewer following it cannot reproduce the Job the tree ships"
    )
    # PR #1923 landed. Describing the installer as a future change is the
    # original lie this file now exists to stop.
    assert "Once that lands" not in text, (
        "docs/STAGING-DEPLOY.md still describes `curie example sre-bot "
        "install` as unlanded. The command is in the tree; this file is "
        "the reproduction doc, not the history of a PR"
    )
    # The installer does not apply self-upgrade/cronjob.yaml. Claiming
    # --platform-upgrade makes upgrade_self live is the next lie: the
    # connector is kept, the bot's own CronJob is not.
    assert "does not apply `self-upgrade/cronjob.yaml`" in text, (
        "docs/STAGING-DEPLOY.md must say the installer does not apply "
        "self-upgrade/cronjob.yaml; --platform-upgrade installs the "
        "platform Job, not this bot's own upgrade CronJob"
    )
    # #2169 retired the bespoke writer and its duplicated allowlist. The
    # reproduction guide must describe the one policy-scoped upstream server,
    # not retain knobs the installer no longer accepts.
    for retired in ("--write-allowlist", "--no-write", "K8S_WRITE_ALLOWLIST"):
        assert retired not in text, (
            f"docs/STAGING-DEPLOY.md still names retired writer surface {retired}"
        )
    assert "K8S_KUBECONFIG" in text
    assert "six mutating core tools require" in text


def test_deploy_yaml_placeholder_channels_cannot_silently_rebind() -> None:
    """A live documentation placeholder is a valid-shaped Slack id.

    ``validate_deploy_targets`` accepts ``C0EXAMPLE1`` because the shape
    check cannot tell a fixture from a real channel. Deploying that file
    with ``--target`` therefore succeeds and rebinds the bot to nothing.
    The shipped file must not carry those as live values, and it must
    still parse: the installer uploads this file through the real bundle
    validator.
    """

    raw = (BUNDLE / "deploy.yaml").read_text(encoding="utf-8")
    data = yaml.safe_load(raw)
    parsed, errors = validate_deploy_targets(data)
    assert errors == [], (
        "examples/sre-bot/deploy.yaml must remain a valid deploy.yaml "
        f"(the installer uploads it through validate_bundle): {errors}"
    )
    assert parsed is not None
    targets = data.get("targets") or {}
    live_placeholders: list[str] = []
    for name, target in targets.items():
        channel = (target or {}).get("slack_channel")
        if channel is None:
            continue
        if str(channel) in PLACEHOLDER_CHANNELS:
            live_placeholders.append(f"targets.{name}.slack_channel={channel}")
    assert not live_placeholders, (
        "examples/sre-bot/deploy.yaml ships documentation placeholder "
        "Slack channel ids as live values: "
        + ", ".join(live_placeholders)
        + ". Those match the Slack id shape, so `curie cluster deploy "
        "--target` reports success and rebinds the bot to a channel that "
        "does not exist. Comment the slack_channel lines out (or put a "
        "real id) so a target cannot silently rebind Slack."
    )


def test_self_upgrade_connector_docstring_names_the_tools_it_ships() -> None:
    """The module docstring is this connector's blast-radius argument.

    After the #2126 forward merge it claimed one zero-argument tool on a
    three-tool file. A reviewer reading the header would stop. The invariant
    that actually guards the connector is that no tool takes caller input
    (curie#2292).
    """

    text = (BUNDLE / "connectors" / "self-upgrade" / "server.py").read_text(encoding="utf-8")
    docstring = text[text.index('"""') + 3 : text.index('"""', 3)]
    for tool in ("upgrade_self", "upgrade_platform", "latest_release"):
        assert tool in docstring, (
            f"self-upgrade module docstring must name {tool}; the file ships "
            "three tools and the header is the security argument a reviewer reads"
        )
    assert "no tool takes caller input" in docstring, (
        "self-upgrade module docstring must state the no-caller-input invariant; "
        "that is the line, not the count of tools"
    )
    assert "one zero-argument tool" not in docstring, (
        "self-upgrade module docstring still claims one zero-argument tool"
    )
    assert "exactly one thing" not in docstring, (
        "self-upgrade module docstring still claims the connector does exactly one thing"
    )


def test_permission_map_matches_the_vanilla_kubernetes_connector() -> None:
    """Retiring the bespoke writers must retire their documentation too."""

    connectors = yaml.safe_load((BUNDLE / "connectors.yaml").read_text(encoding="utf-8"))
    declared = connectors.get("connectors") or {}
    assert "kubernetes" in declared
    assert "k8s-write" not in declared
    assert "k8s-scale" not in declared
    text = (BUNDLE / "docs" / "PERMISSION-MAP.md").read_text(encoding="utf-8")
    assert "mcp__k8s-write__" not in text
    assert "mcp__k8s-scale__" not in text
    assert "kubernetes/resources_create_or_update" in text
    assert "kubernetes/resources_scale" in text


def test_ci_shellchecks_the_platform_upgrade_script() -> None:
    """upgrade.sh is the Job body. CI must run shellcheck on that path.

    A pytest that shells out to shellcheck is not enough on its own: if
    that test were deleted, CI would still have zero coverage of this
    script. The workflow must invoke shellcheck on this path so the gate
    is visible in the job that actually runs on every PR.

    Comments do not count. An earlier pin matched the path and the word
    ``shellcheck`` anywhere in the file, so deleting the ``run:`` line
    left both asserts green.
    """

    workflow = yaml.safe_load(CI_WORKFLOW.read_text(encoding="utf-8")) or {}
    python_job = (workflow.get("jobs") or {}).get("python") or {}
    steps = python_job.get("steps") or []
    runs = [step.get("run") or "" for step in steps if isinstance(step, dict)]
    matching = [
        run
        for run in runs
        if "shellcheck" in run and "examples/sre-bot/platform-upgrade/upgrade.sh" in run
    ]
    assert matching, (
        "the python CI job has no step whose run: invokes shellcheck on "
        "examples/sre-bot/platform-upgrade/upgrade.sh. A comment naming "
        "the path is not a gate. Add a step that runs "
        "`shellcheck --severity=warning examples/sre-bot/platform-upgrade/upgrade.sh`."
    )


def test_platform_upgrade_script_is_shellcheck_clean() -> None:
    """The Job script itself must be clean, not merely mentioned in CI.

    The sibling test asserts the workflow names the path. This one runs
    shellcheck against the script so a syntax error still fails even if
    the workflow step is later pointed at a different file.
    """

    assert UPGRADE_SCRIPT.is_file(), f"missing {UPGRADE_SCRIPT}"
    result = subprocess.run(
        ["shellcheck", "--severity=warning", str(UPGRADE_SCRIPT)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        "shellcheck failed on examples/sre-bot/platform-upgrade/upgrade.sh:\n"
        f"{result.stdout}{result.stderr}"
    )
