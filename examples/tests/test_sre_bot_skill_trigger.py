"""The SRE bot's skill must load for questions about the bot itself.

A mean tester campaign found the bot answering version, upgrade and drain
questions without its skill: the description named only health, cluster and
observability questions, so the model never loaded the rules it broke.
"""

import re
from pathlib import Path

SKILL = Path(__file__).resolve().parents[2] / "examples/sre-bot/skills/sre-bot/SKILL.md"


def _description() -> str:
    match = re.search(r"^description: (.*)$", SKILL.read_text(), re.M)
    assert match, "SKILL.md must carry a one-line description"
    return match.group(1)


def test_the_description_fits_the_agent_skills_limit():
    assert len(_description()) <= 1024


def test_questions_about_the_bot_itself_load_the_skill():
    description = _description().lower()
    triggers = ("version", "upgrading", "what you can and cannot do", "drain", "cordon", "scale")
    for trigger in triggers:
        assert trigger in description, trigger


def test_the_drain_answer_names_steps_not_tools():
    text = " ".join(SKILL.read_text().split())
    assert "leave tool names out of the reply" in text
