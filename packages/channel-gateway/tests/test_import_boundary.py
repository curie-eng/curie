"""Execute the repository gateway contract against violating import graphs."""

import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest


def test_gateway_contract_rejects_worker_and_api_imports(tmp_path: Path) -> None:
    repo = Path(__file__).resolve().parents[3]
    contracts = tomllib.loads((repo / "pyproject.toml").read_text())["tool"]["importlinter"][
        "contracts"
    ]
    contract = next(
        item
        for item in contracts
        if item["name"] == "Channel gateway implementations stay independent of API and worker"
    )
    packages = [*contract["source_modules"], *contract["forbidden_modules"]]
    for package in packages:
        directory = tmp_path / package
        directory.mkdir()
        (directory / "__init__.py").write_text("")
        (directory / "entry.py").write_text("")
    config = tmp_path / "imports.ini"
    config.write_text(
        "[importlinter]\nroot_packages =\n"
        + "".join(f"    {name}\n" for name in packages)
        + "\n[importlinter:contract:gateway]\n"
        + f"name = {contract['name']}\ntype = {contract['type']}\nsource_modules =\n"
        + "".join(f"    {name}\n" for name in contract["source_modules"])
        + "forbidden_modules =\n"
        + "".join(f"    {name}\n" for name in contract["forbidden_modules"])
    )
    module = tmp_path / "curie_channel_gateway" / "implementation.py"

    def lint() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                "-c",
                "from importlinter.cli import lint_imports_command; lint_imports_command()",
                "--config",
                str(config),
                "--no-cache",
                "--no-logo",
            ],
            cwd=tmp_path,
            env={**os.environ, "PYTHONPATH": str(tmp_path)},
            capture_output=True,
            text=True,
            check=False,
        )

    for dependency in contract["forbidden_modules"]:
        module.write_text(f"from {dependency} import entry\n")
        result = lint()
        assert result.returncode == 1, result.stdout + result.stderr
        assert "BROKEN" in result.stdout
    module.write_text("value = 1\n")
    result = lint()
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("source,target", [
    ("curie_channel_gateway", "curie_dispatcher"),
    ("curie_dispatcher", "curie_channel_gateway"),
])
@pytest.mark.parametrize("style", ["runtime", "type_checking", "indirect", "independent"])
def test_gateway_dispatcher_import_boundary(
    tmp_path: Path, source: str, target: str, style: str,
) -> None:
    repo = Path(__file__).resolve().parents[3]
    settings = tomllib.loads((repo / "pyproject.toml").read_text())["tool"]["importlinter"]
    contract = next(item for item in settings["contracts"]
                    if item["name"] == "Channel gateway and dispatcher stay independent")
    packages = ["curie_channel_gateway", "curie_dispatcher", "shared_transport"]
    for name in packages:
        directory = tmp_path / name
        directory.mkdir()
        (directory / "__init__.py").write_text("")
        (directory / "entry.py").write_text("")
    dependency = f"from {target} import entry\n"
    if style == "type_checking":
        dependency = "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    " + dependency
    elif style in {"indirect", "independent"}:
        (tmp_path / "shared_transport" / "entry.py").write_text(
            dependency if style == "indirect" else "VALUE = 1\n"
        )
        dependency = "from shared_transport import entry\n"
    (tmp_path / source / "entry.py").write_text(dependency)
    config = tmp_path / "pyproject.toml"
    config.write_text(
        "[tool.importlinter]\n"
        + "".join(
            f"{key} = {json.dumps(packages if key == 'root_packages' else value)}\n"
            for key, value in settings.items() if key != "contracts"
        )
        + "[[tool.importlinter.contracts]]\n"
        + "".join(f"{key} = {json.dumps(value)}\n" for key, value in contract.items())
    )
    result = subprocess.run(
        [sys.executable, "-c",
         "from importlinter.cli import lint_imports_command; lint_imports_command()",
         "--config", str(config), "--no-cache", "--no-logo"],
        cwd=tmp_path, env={**os.environ, "PYTHONPATH": str(tmp_path)},
        capture_output=True, text=True, check=False, timeout=30,
    )
    output = result.stdout + result.stderr
    assert result.returncode == (0 if style == "independent" else 1), output
    assert ("KEPT" if style == "independent" else "BROKEN") in output, output
