#!/usr/bin/env python3
"""Smallest screenshot path that does not need a client stack.

Builds a depth-1 checkout the way the factory prepares a workspace, recovers
the base commit from .git/shallow, renders the base file and the working tree
through a localhost server, and writes PNGs into a gitignored directory.
"""

from __future__ import annotations

import base64
import hashlib
import http.server
import json
import os
import shutil
import socket
import struct
import subprocess
import threading
from pathlib import Path

OUT = Path(os.environ.get("PROOF_OUT", "/tmp/proof-out"))
PATCH_CAP = 900_000
CHROMIUM_CANDIDATES = (
    os.environ.get("CHROMIUM", ""),
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
    "/usr/bin/google-chrome",
)


def run(argv: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=cwd,
        check=True,
        text=True,
        capture_output=True,
    )


def chromium_bin() -> str:
    for candidate in CHROMIUM_CANDIDATES:
        if candidate and Path(candidate).is_file():
            return candidate
    raise SystemExit("chromium is not installed")


def page(label: str, api: str, crowded: bool) -> str:
    width = 280 if crowded else 640
    api_row = (
        f'<span class="api">{api}</span>' if api else '<span class="api absent">no api name</span>'
    )
    return f"""<!doctype html>
<meta charset="utf-8">
<title>{label}</title>
<style>
  html, body {{ margin: 0; background: #161616; }}
  body {{ font-family: "Liberation Sans", sans-serif; color: #f4f4f4; }}
  .canvas {{
    box-sizing: border-box; width: {width}px; height: 120px;
    margin: 24px; padding: 16px; background: #242424; border: 1px solid #3a3a3a;
  }}
  .row {{ display: flex; gap: 8px; align-items: baseline; white-space: nowrap; }}
  .label {{ font-size: 22px; }}
  .api {{ font-size: 13px; color: #8ec8ff; }}
  .absent {{ color: #666; }}
</style>
<div class="canvas"><div class="row"><span class="label">{label}</span>{api_row}</div></div>
"""


def png_size(data: bytes) -> tuple[int, int]:
    if data[:8] != b"\x89PNG\r\n\x1a\n":
        raise SystemExit("screenshot is not a png")
    width, height = struct.unpack(">II", data[16:24])
    return width, height


def shot(binary: str, url: str, dest: Path) -> dict[str, object]:
    dest.parent.mkdir(parents=True, exist_ok=True)
    profile = Path("/tmp/chrome-profile")
    profile.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["HOME"] = "/tmp/chrome-home"
    env["XDG_CONFIG_HOME"] = "/tmp/chrome-home/.config"
    env["XDG_CACHE_HOME"] = "/tmp/chrome-home/.cache"
    Path(env["HOME"]).mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        [
            binary,
            "--headless=new",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--hide-scrollbars",
            "--window-size=800,200",
            f"--user-data-dir={profile}",
            f"--screenshot={dest}",
            url,
        ],
        check=False,
        text=True,
        capture_output=True,
        cwd=dest.parent,
        env=env,
    )
    if not dest.is_file():
        fallback = dest.parent / "screenshot.png"
        if fallback.is_file():
            fallback.replace(dest)
    if proc.returncode != 0 or not dest.is_file():
        raise SystemExit(
            f"chromium failed for {url}: exit {proc.returncode}\n{proc.stderr[-2000:]}"
        )
    data = dest.read_bytes()
    width, height = png_size(data)
    return {
        "path": dest.name,
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "width": width,
        "height": height,
    }


class Handler(http.server.BaseHTTPRequestHandler):
    files: dict[str, bytes] = {}

    def do_GET(self) -> None:  # noqa: N802
        body = self.files.get(self.path)
        if body is None:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: object) -> None:
        return


def serve(files: dict[str, bytes]) -> tuple[threading.Thread, int]:
    Handler.files = files
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    assert isinstance(port, int)
    return thread, port


def main() -> None:
    binary = chromium_bin()
    root = Path("/tmp/proof-repo")
    shutil.rmtree(root, ignore_errors=True)
    origin = root / "origin"
    origin.mkdir(parents=True)
    run(["git", "init", "-b", "master"], origin)
    run(["git", "config", "user.email", "proof@example.com"], origin)
    run(["git", "config", "user.name", "Proof"], origin)
    (origin / "component.html").write_text(page("Amount", "", False), encoding="utf-8")
    (origin / ".gitignore").write_text(".curie-screenshots/\n", encoding="utf-8")
    run(["git", "add", "component.html", ".gitignore"], origin)
    run(["git", "commit", "-m", "base ui"], origin)

    work = root / "work"
    run(
        ["git", "clone", "--depth=1", "--single-branch", "--no-tags", origin.as_uri(), str(work)],
        root,
    )
    shallow = (work / ".git" / "shallow").read_text(encoding="utf-8").split()
    if len(shallow) != 1:
        raise SystemExit(f"expected one shallow root, got {shallow!r}")
    base = shallow[0]
    head = run(["git", "rev-parse", "HEAD"], work).stdout.strip()
    if base != head:
        raise SystemExit("fresh depth-1 HEAD is not the shallow root")

    (work / "component.html").write_text(
        page("Amount", "opportunity.amount_cents", True), encoding="utf-8"
    )
    before_html = run(["git", "show", f"{base}:component.html"], work).stdout
    after_html = (work / "component.html").read_text(encoding="utf-8")
    story = (
        "<!doctype html><meta charset=utf-8><title>story</title>"
        "<body style='margin:0;background:#111;color:#9cf;font-family:sans-serif'>"
        "<p style='margin:8px'>static story frame</p>"
        "<iframe src='/after.html' width='320' height='140' style='border:0'></iframe>"
        "</body>"
    )
    _thread, port = serve(
        {
            "/before.html": before_html.encode(),
            "/after.html": after_html.encode(),
            "/story.html": story.encode(),
        }
    )
    # Bind check: the server is loopback only.
    with socket.create_connection(("127.0.0.1", port), timeout=2):
        pass

    shots = {
        name: shot(binary, f"http://127.0.0.1:{port}/{name}.html", OUT / f"{name}.png")
        for name in ("before", "after", "story")
    }
    shot_dir = work / ".curie-screenshots"
    shot_dir.mkdir()
    for name in shots:
        (shot_dir / f"{name}.png").write_bytes((OUT / f"{name}.png").read_bytes())
    listed = run(
        ["git", "ls-files", "-z", "--others", "--exclude-standard"], work
    ).stdout
    others = [item for item in listed.split("\0") if item]
    if any(item.startswith(".curie-screenshots/") for item in others):
        raise SystemExit(f"gitignored screenshots were listed: {others}")

    run(["git", "add", "-f", ".curie-screenshots/before.png"], work)
    forced = run(["git", "diff", "--binary", "--cached"], work).stdout
    run(["git", "reset", "-q"], work)
    report = {
        "chromium": binary,
        "base_sha": base,
        "recovered_from": ".git/shallow",
        "loopback_port": port,
        "screenshots": shots,
        "gitignored_untracked_listing": others,
        "forced_patch_bytes": len(forced.encode()),
        "patch_cap_bytes": PATCH_CAP,
        "distinct_pixels": shots["before"]["sha256"] != shots["after"]["sha256"],
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    for name in ("before", "after", "story"):
        encoded = base64.b64encode((OUT / f"{name}.png").read_bytes()).decode("ascii")
        print(f"PNG {name} {encoded}")
    if not report["distinct_pixels"]:
        raise SystemExit("before and after screenshots are identical")
    if int(report["forced_patch_bytes"]) <= 0:
        raise SystemExit("forced add produced an empty patch")


if __name__ == "__main__":
    main()
