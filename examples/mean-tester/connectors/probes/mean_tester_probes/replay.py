"""Recorded Slack threads and bundles in place of Slack and GitHub (ADR 0169 d8)."""

import json
from pathlib import Path

from mean_tester_probes.config import RepoRef
from mean_tester_probes.sources import BundleSource


class ReplaySources:
    def __init__(self, root) -> None:
        self._root = Path(root)

    def find(self, channel: str, hint: str | None) -> list[BundleSource]:
        if not hint or not (self._root / hint / "bundle").is_dir():
            return []
        base = self._root / hint / "bundle"
        files = {str(p.relative_to(base)): p.read_text() for p in base.rglob("*") if p.is_file()}
        return [BundleSource(RepoRef("replay", "fixtures", "main"), "0" * 40, hint, files)]


class ReplaySlack:
    """Replays one fixture's thread for every probe.

    `_fixture` is process-global, the same shape the live connector refuses for
    issue filing: two threads replaying at once would read each other's
    fixture. It is left that way because it only ever affects replay, where one
    eval case runs at a time against a fixture nothing outside the suite reads.
    """

    def __init__(self, root) -> None:
        self._root = Path(root)
        self._fixture: str | None = None
        self._count = 0

    def use(self, fixture: str) -> None:
        self._fixture = fixture

    def _thread(self) -> dict:
        fixture = self._fixture or next(p.name for p in sorted(self._root.iterdir()) if p.is_dir())
        return json.loads((self._root / fixture / "thread.json").read_text())

    def channel_info(self, channel: str) -> dict:
        return {"id": channel, "is_ext_shared": False, "is_shared": False}

    def members(self, channel: str) -> set[str]:
        return {self._thread()["target_user"]}

    def post(self, channel: str, text: str) -> str:
        self._count += 1
        return f"1790000000.{self._count:06d}"

    def replies(self, channel: str, ts: str) -> list[dict]:
        return [{"ts": ts, "user": "U0TESTER01", "text": "probe"}, *self._thread()["messages"]]
