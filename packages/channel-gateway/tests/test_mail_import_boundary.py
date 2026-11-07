"""Mail remains independently installable while Slack retains dispatcher helpers."""

import json
import os
import subprocess
import tomllib
from pathlib import Path

import pytest

CONTRACT = "Gateway mail stays independent of Slack and dispatcher"


@pytest.mark.parametrize("forbidden", [None, "curie_channel_gateway.slack", "curie_dispatcher"])
def test_mail_import_boundary(tmp_path: Path, forbidden: str | None) -> None:
    root = Path(__file__).resolve().parents[3]
    settings = tomllib.loads((root / "pyproject.toml").read_text())["tool"]["importlinter"]
    contracts = [item for item in settings["contracts"] if item["name"] == CONTRACT]
    assert len(contracts) == 1
    contract = contracts[0]
    assert contract["source_modules"] == ["curie_channel_gateway.mail"]
    assert set(contract["forbidden_modules"]) == {
        "curie_channel_gateway.slack", "curie_dispatcher",
    }
    for package in ("curie_channel_gateway", "curie_channel_gateway/mail",
                    "curie_channel_gateway/slack", "curie_dispatcher"):
        directory = tmp_path / package
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "__init__.py").write_text("")
    (tmp_path / "curie_channel_gateway/mail/__init__.py").write_text("from . import transport\n")
    (tmp_path / "curie_channel_gateway/mail/transport.py").write_text(
        f"import {forbidden}\n" if forbidden else "VALUE = 1\n"
    )
    config = tmp_path / "pyproject.toml"
    config.write_text(
        '[tool.importlinter]\nroot_packages = ["curie_channel_gateway", "curie_dispatcher"]\n'
        '[[tool.importlinter.contracts]]\n'
        + "".join(f"{key} = {json.dumps(value)}\n" for key, value in contract.items())
    )
    result = subprocess.run(
        ["lint-imports", "--config", str(config)], cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(tmp_path)}, capture_output=True, text=True,
        check=False, timeout=30,
    )
    output = result.stdout + result.stderr
    if forbidden:
        assert result.returncode != 0, output
        assert "BROKEN" in output and CONTRACT in output and forbidden in output
    else:
        assert result.returncode == 0, output
        assert "KEPT" in output and CONTRACT in output
