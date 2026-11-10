"""The CLI's embedded tool catalog v1 is a valid ToolCatalogEntry list (ADR 0216).

`cli/src/tool_catalog_v1.json` is the file `curie build --repo-runner` compiles
in. This pins that the Python contract accepts every entry and that the entry
names are exactly CATALOG_TOOLS in catalog order, so the Rust and Python sides
cannot drift apart.
"""

from __future__ import annotations

import json
from pathlib import Path

from curie_api.forges.runner_config import CATALOG_TOOLS, ToolCatalogEntry

ROOT = Path(__file__).resolve().parents[4]
CATALOG = ROOT / "cli" / "src" / "tool_catalog_v1.json"


def _entries() -> list[dict[str, object]]:
    raw = json.loads(CATALOG.read_text(encoding="utf-8"))
    assert isinstance(raw, list)
    return raw


def test_every_catalog_v1_entry_validates_as_tool_catalog_entry() -> None:
    entries = [ToolCatalogEntry.model_validate(item) for item in _entries()]
    for entry in entries:
        assert set(entry.platforms) == {"linux/amd64", "linux/arm64"}


def test_catalog_v1_names_are_catalog_tools_in_order() -> None:
    assert tuple(item["name"] for item in _entries()) == CATALOG_TOOLS
