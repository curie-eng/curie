"""Cross-language drift pin for the reserved sealing key names (ACTION-EXECUTOR-16).

@spec ACTION-EXECUTOR-16. ``curie_internal.sealing_key.SEALING_KEY_NAMES`` is the
one Python definition the API intake, the ``undoable`` derivation and the
worker's ``inject_connector_secrets`` share. Helm cannot import Python, so the
chart keeps a second copy in ``_helpers.tpl`` (``curie.sealingKeyNames``), and
the render ``fail`` in ``agent-connector-secrets.yaml`` reads it. If the two
drift, a name the API and worker withhold could still render into the per-agent
Secret the runner sandbox reads, or the reverse. Same shape as the boot-env
pin in ``test_reserved_boot_env_pin.py``; checked at import time, no fixtures.
"""

from __future__ import annotations

import re
from pathlib import Path

from curie_internal.sealing_key import SEALING_KEY_NAMES

_CHART = Path(__file__).resolve().parents[4] / "charts" / "curie"
_HELPERS_TPL = _CHART / "templates" / "_helpers.tpl"
_ASSERTIONS = _CHART / "ci" / "connector-secrets-assertions.sh"

_ENV_NAME_RE = re.compile(r"[A-Z0-9]+(?:_[A-Z0-9]+)+")


def _sealing_names_from_helpers() -> set[str]:
    text = _HELPERS_TPL.read_text(encoding="utf-8")
    match = re.search(
        r'define\s+"curie\.sealingKeyNames"\s*(?:-?}})?(?P<body>.*?){{-?\s*end',
        text,
        re.DOTALL,
    )
    assert match, (
        f"no `curie.sealingKeyNames` define found in {_HELPERS_TPL} -- the Helm "
        "sealing-key drift gate has no source"
    )
    return set(_ENV_NAME_RE.findall(match.group("body")))


def test_helm_sealing_key_names_match_the_python_list() -> None:
    """@spec ACTION-EXECUTOR-16"""
    # Non-vacuity floor: the Python list is what the spec names.
    assert {"SNAPSHOT_SEALING_KEY", "SNAPSHOT_SEALING_KEYS_RETAINED"} <= SEALING_KEY_NAMES
    assert _sealing_names_from_helpers() == set(SEALING_KEY_NAMES)


def test_the_chart_render_assertions_exercise_every_sealing_key_name() -> None:
    """@spec ACTION-EXECUTOR-16: the CI script hardcodes the names it renders.

    A name added to both lists above but not to the script would ship with no
    render-time check that the chart refuses it.
    """
    text = _ASSERTIONS.read_text(encoding="utf-8")
    loops = re.findall(r"for key in (?P<names>[A-Z0-9_ ]+); do", text)
    exercised = [set(_ENV_NAME_RE.findall(names)) for names in loops]
    sealing_loops = [names for names in exercised if names & SEALING_KEY_NAMES]
    assert sealing_loops, f"{_ASSERTIONS} renders no sealing key name"
    for names in sealing_loops:
        assert names == set(SEALING_KEY_NAMES), (
            f"a sealing-key render loop in {_ASSERTIONS} covers {sorted(names)}, "
            f"not {sorted(SEALING_KEY_NAMES)}"
        )
