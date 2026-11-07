"""Execute the repository gateway contract against violating import graphs."""

import os
import subprocess
import sys
import tomllib
from pathlib import Path


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
