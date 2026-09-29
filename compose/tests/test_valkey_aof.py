"""compose.dev.yaml must start Valkey with AOF fsync (#3349).

The helper is the assertion. The negative feeds it a copy of the command with
the durability flags removed and expects that helper to reject the copy.
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_DEV = REPO_ROOT / "compose.dev.yaml"

_AOF_PAIRS = (("--appendonly", "yes"), ("--appendfsync", "everysec"))


def command_has_aof_flags(command: list[object]) -> bool:
    args = [str(item) for item in command]
    for flag, value in _AOF_PAIRS:
        if not any(
            args[index] == flag and index + 1 < len(args) and args[index + 1] == value
            for index in range(len(args))
        ):
            return False
    return True


def _without_aof_flags(command: list[object]) -> list[object]:
    args = list(command)
    kept: list[object] = []
    index = 0
    while index < len(args):
        pair = (str(args[index]), str(args[index + 1]) if index + 1 < len(args) else "")
        if pair in _AOF_PAIRS:
            index += 2
            continue
        kept.append(args[index])
        index += 1
    return kept


def test_compose_valkey_command_enables_aof() -> None:
    compose = yaml.safe_load(COMPOSE_DEV.read_text())
    command = compose["services"]["valkey"]["command"]
    assert command_has_aof_flags(command), command


def test_aof_helper_rejects_a_command_with_the_flags_removed() -> None:
    compose = yaml.safe_load(COMPOSE_DEV.read_text())
    command = list(compose["services"]["valkey"]["command"])
    stripped = _without_aof_flags(command)
    assert command_has_aof_flags(stripped) is False
