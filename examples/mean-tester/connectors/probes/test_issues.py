import httpx
from mean_tester_probes.config import RepoRef
from mean_tester_probes.issues import GitHubIssues

REPO = RepoRef("acme", "agents", "main")


def issue(number, repository):
    return {
        "number": number, "title": f"issue {number}", "html_url": f"u{number}",
        "repository_url": f"https://api.github.com/repos/{repository}",
    }


def issues(handler):
    return GitHubIssues("ghp_test", client=httpx.Client(transport=httpx.MockTransport(handler)))


def test_search_is_scoped_to_the_one_repository_and_open_issues():
    def handler(request):
        q = request.url.params["q"]
        assert "repo:acme/agents" in q and "is:issue" in q and "is:open" in q
        return httpx.Response(200, json={"items": [issue(7, "acme/agents")]})
    assert issues(handler).find_open(REPO, "invents a cause")[0]["number"] == 7


def test_create_posts_to_that_repository_only():
    def handler(request):
        assert request.url.path == "/repos/acme/agents/issues"
        return httpx.Response(201, json={"html_url": "https://github.com/acme/agents/issues/8"})
    assert issues(handler).create(REPO, "t", "b").endswith("/issues/8")


def test_search_drops_every_result_from_another_repository():
    # GitHub ORs repeated `repo:` qualifiers, so a query that adds its own
    # `repo:other/x` widens the search. Only the validated repository's
    # results come back.
    def handler(request):
        return httpx.Response(200, json={"items": [
            issue(7, "acme/agents"), issue(8, "other/x"), issue(9, "acme/agents-fork"),
            issue(10, "Acme/Agents"),
        ]})
    found = issues(handler).find_open(REPO, "invents a cause repo:other/x")
    assert [i["number"] for i in found] == [7, 10]
