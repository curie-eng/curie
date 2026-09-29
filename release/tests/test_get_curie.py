"""Exercise the released installer on Linux arm64 without touching the network or host install."""

import os
import subprocess
from pathlib import Path

import pytest

INSTALLER = Path(__file__).resolve().parents[2] / "get-curie.sh"
ARM64_ASSET = "curie-aarch64-unknown-linux-gnu"
RELEASE_BASE = "https://github.com/curie-eng/curie/releases/download/v0.11.0"


def _executable(path: Path, body: str) -> None:
    path.write_text(body)
    path.chmod(0o755)


@pytest.mark.parametrize("machine", ["arm64", "aarch64"])
def test_linux_arm64_download_checks_published_asset_before_install(
    tmp_path: Path, machine: str
) -> None:
    """Both uname spellings must select the shipped binary and reject a bad digest."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    requests = tmp_path / "requests.txt"
    cosign_args = tmp_path / "cosign-args.txt"

    _executable(
        bin_dir / "uname",
        '#!/bin/sh\ncase "$1" in\n'
        '  -s) echo Linux ;;\n  -m) echo "$CURIE_TEST_MACHINE" ;;\n  *) exit 2 ;;\nesac\n',
    )
    _executable(
        bin_dir / "curl",
        """#!/usr/bin/env python3
import os
import sys
from pathlib import Path

url = sys.argv[-1]
with open(os.environ["CURIE_TEST_REQUESTS"], "a", encoding="utf-8") as log:
    log.write(url + "\\n")
name = url.rsplit("/", 1)[-1]
if name == "checksums.txt":
    Path(name).write_text("0" * 64 + "  curie-aarch64-unknown-linux-gnu\\n")
elif name == "checksums.txt.sigstore.json":
    Path(name).write_text("{}\\n")
else:
    Path(name).write_bytes(b"deliberately corrupted release binary")
""",
    )
    _executable(
        bin_dir / "cosign",
        '#!/bin/sh\nprintf "%s\\n" "$@" > "$CURIE_TEST_COSIGN_ARGS"\n',
    )
    # A checksum regression must never reach the host installation even if the
    # test is run as root and /usr/local/bin is writable.
    _executable(bin_dir / "chmod", "#!/bin/sh\nexit 93\n")

    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "HOME": str(tmp_path),
        "CURIE_INSTALL_MODE": "download",
        "CURIE_VERSION": "v0.11.0",
        "CURIE_REQUIRE_COSIGN": "1",
        "CURIE_TEST_MACHINE": machine,
        "CURIE_TEST_REQUESTS": str(requests),
        "CURIE_TEST_COSIGN_ARGS": str(cosign_args),
    }
    done = subprocess.run(["bash", str(INSTALLER)], env=env, capture_output=True, text=True)

    assert f"==> downloading {ARM64_ASSET} (v0.11.0)" in done.stdout, done.stdout + done.stderr
    assert requests.read_text().splitlines() == [
        f"{RELEASE_BASE}/{ARM64_ASSET}",
        f"{RELEASE_BASE}/checksums.txt",
        f"{RELEASE_BASE}/checksums.txt.sigstore.json",
    ]
    assert "verify-blob" in cosign_args.read_text().splitlines()
    assert f"{ARM64_ASSET}: FAILED" in done.stdout + done.stderr
    assert done.returncode == 1
    assert not (tmp_path / ".local" / "bin" / "curie").exists()
