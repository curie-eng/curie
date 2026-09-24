"""Read a target's bundle from Git, never from its platform (ADR 0169 d2, d3)."""

import base64
import fnmatch
import json
from collections.abc import Mapping
from dataclasses import dataclass, field

import httpx
import yaml

from mean_tester_probes.config import RepoRef

API = "https://api.github.com"
MANIFEST = ".claude-plugin/plugin.json"
BUNDLE_FILES = (MANIFEST, "skills/*/SKILL.md", "connectors.yaml", "deploy.yaml", "evals/cases.json")
# The optional specification rides in the tool result, and so in the tester's
# context: bound it, and say what was left out rather than truncating a file.
SPEC_MAX_CHARS = 200_000


class SourceError(RuntimeError):
    pass


@dataclass(frozen=True)
class BundleSource:
    repo: RepoRef
    commit: str
    path: str  # repository-relative; "" for a bundle at the repository root
    files: dict[str, str] = field(default_factory=dict)
    spec: dict[str, str] = field(default_factory=dict)  # repository-relative path -> text
    spec_omitted: tuple[str, ...] = ()

    @property
    def name(self) -> str:
        return json.loads(self.files[MANIFEST])["name"]


class GitHubSources:
    def __init__(
        self,
        token: str,
        repos: tuple[RepoRef, ...],
        client: httpx.Client | None = None,
        spec_paths: Mapping[str, str] | None = None,
        spec_max_chars: int = SPEC_MAX_CHARS,
    ) -> None:
        self._repos = repos
        self._client = client or httpx.Client(timeout=30)
        self._headers = {
            "Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
        }
        self._spec_paths = dict(spec_paths or {})
        self._spec_max_chars = spec_max_chars

    def _get(self, path: str) -> dict:
        r = self._client.get(API + path, headers=self._headers)
        if r.status_code != 200:
            raise SourceError(f"GitHub answered {r.status_code} for {path}")
        return r.json()

    def _read(self, repo: RepoRef, commit: str, path: str) -> str:
        body = self._get(f"/repos/{repo.full_name}/contents/{path}?ref={commit}")
        return base64.b64decode(body["content"]).decode()

    def find(self, channel: str, name_hint: str | None) -> list[BundleSource]:
        found = []
        for repo in self._repos:
            commit = self._get(f"/repos/{repo.full_name}/commits/{repo.ref}")["sha"]
            tree = self._get(f"/repos/{repo.full_name}/git/trees/{commit}?recursive=1")
            if tree.get("truncated"):
                raise SourceError(f"Tree for {repo.full_name} at {commit} is truncated")
            paths = [e["path"] for e in tree["tree"] if e["type"] == "blob"]
            for manifest in (p for p in paths if p == MANIFEST or p.endswith("/" + MANIFEST)):
                root = manifest[: -len(MANIFEST)].rstrip("/")
                prefix = root + "/" if root else ""
                files = {
                    rel: self._read(repo, commit, prefix + rel)
                    for rel in (p[len(prefix):] for p in paths if p.startswith(prefix))
                    if any(fnmatch.fnmatch(rel, pattern) for pattern in BUNDLE_FILES)
                }
                name = _bundle_name(files)
                if name is None:
                    continue  # an unreadable manifest names no bundle to test
                if name_hint is not None:
                    matched = name == name_hint
                else:
                    matched = channel in _bundle_deploy_channels(files.get("deploy.yaml", ""))
                if matched:
                    spec, omitted = self._spec(repo, commit, paths, name)
                    found.append(BundleSource(repo, commit, root, files, spec, omitted))
        return found

    def _spec(
        self, repo: RepoRef, commit: str, paths: list[str], name: str
    ) -> tuple[dict[str, str], tuple[str, ...]]:
        """Every `*.md` under the bundle's configured spec directory, at `commit`."""
        directory = self._spec_paths.get(name)
        if directory is None:
            return {}, ()
        spec: dict[str, str] = {}
        omitted: list[str] = []
        used = 0
        for path in sorted(p for p in paths if p.startswith(directory + "/") and p.endswith(".md")):
            if omitted:
                omitted.append(path)
                continue
            text = self._read(repo, commit, path)
            if used + len(text) > self._spec_max_chars:
                omitted.append(path)
                continue
            spec[path] = text
            used += len(text)
        return spec, tuple(omitted)


def _bundle_name(files: dict[str, str]) -> str | None:
    """The manifest's name, or None when it is missing or unreadable."""
    try:
        name = json.loads(files[MANIFEST])["name"]
    except (KeyError, TypeError, json.JSONDecodeError):
        return None
    return name if isinstance(name, str) and name else None


def _bundle_deploy_channels(deploy_yaml: str) -> set[str]:
    """Extract deploy channels, returning an empty set if deploy.yaml is malformed."""
    try:
        return _deploy_channels(deploy_yaml)
    except (yaml.YAMLError, AttributeError, TypeError):
        return set()


def _deploy_channels(deploy_yaml: str) -> set[str]:
    targets = (yaml.safe_load(deploy_yaml) or {}).get("targets") or {}
    return {
        str(t.get("slack_channel"))
        for t in targets.values()
        if isinstance(t, dict) and t.get("slack_channel")
    }
