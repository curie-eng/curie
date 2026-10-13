from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(autouse=True)
def candidate_curie(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    binary = tmp_path / "bin" / "curie"
    binary.parent.mkdir()
    binary.write_text(
        '#!/bin/sh\nif [ "$1" = "--version" ]; then\n'
        "printf 'curie 0.0.0-test\\nadditional version detail\\n'\n"
        'else\nprintf "%s\\n" "$0"\nfi\n'
    )
    binary.chmod(0o755)
    monkeypatch.setenv("CURIE_BIN", str(binary))
    return binary
