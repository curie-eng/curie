"""Validate shipped deployment targets and lint the executable upgrade script.

Installer and connector behavior is covered in their runtime test suites.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import yaml
from plugin_format.deploy_targets import validate_deploy_targets

REPO = Path(__file__).resolve().parents[2]
BUNDLE = REPO / "examples" / "sre-bot"
UPGRADE_SCRIPT = BUNDLE / "platform-upgrade" / "upgrade.sh"
# Named allowlisted ids, not a prefix-plus-digit regex. A bare prefix
# of the sanctioned placeholders matches slack-conversation-id and is
# not a gitleaks stopword; C0EXAMPLE1 and C0EXAMPLE2 are.
PLACEHOLDER_CHANNELS = frozenset({"C0EXAMPLE1", "C0EXAMPLE2"})


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


def test_platform_upgrade_script_is_shellcheck_clean() -> None:
    """The Job script itself must be clean under shellcheck.

    Pytest runs shellcheck against upgrade.sh, so a syntax error fails
    the suite even if a workflow step is later pointed at a different file.
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
