"""`skill up` output must still be printed when the command fails (#2245)."""

from pathlib import Path

E2E = Path(__file__).resolve().parents[2] / "cli" / "scripts" / "e2e.sh"


def test_e2e_sh_prints_skill_up_output_on_a_non_zero_exit() -> None:
    text = E2E.read_text()
    start = text.index("=== curie skill up")
    chunk = text[start : text.index("The boot panel must NAME", start)]
    assert "UP_OUTPUT=" in chunk
    assert "||" in chunk
    assert "printf '%s\\n' \"$UP_OUTPUT\"" in chunk
    assert "UP_RC" in chunk
    assert "exited" in chunk
