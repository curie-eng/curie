"""The one write the tester may make, after a person approves it (ADR 0169 d6)."""

import httpx

from mean_tester_probes.config import RepoRef

API = "https://api.github.com"


class GitHubIssues:
    def __init__(self, token: str, client: httpx.Client | None = None) -> None:
        self._client = client or httpx.Client(timeout=30)
        self._headers = {
            "Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
        }

    def find_open(self, repo: RepoRef, query: str) -> list[dict]:
        q = f"{query} repo:{repo.full_name} is:issue is:open"
        r = self._client.get(
            API + "/search/issues", headers=self._headers, params={"q": q, "per_page": 5}
        )
        r.raise_for_status()
        # GitHub ORs repeated `repo:` qualifiers, so the caller's query can
        # widen the search to another repository. Keep only this one's issues.
        own = f"/repos/{repo.full_name}".lower()
        return [
            i for i in r.json()["items"] if str(i.get("repository_url", "")).lower().endswith(own)
        ]

    def create(self, repo: RepoRef, title: str, body: str) -> str:
        r = self._client.post(API + f"/repos/{repo.full_name}/issues", headers=self._headers,
                              json={"title": title, "body": body})
        r.raise_for_status()
        return r.json()["html_url"]
