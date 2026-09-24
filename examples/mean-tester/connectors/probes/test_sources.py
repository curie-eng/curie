import base64
import json

import httpx
import pytest
from mean_tester_probes.config import RepoRef
from mean_tester_probes.sources import GitHubSources, SourceError

REPO = RepoRef("acme", "agents", "main")


def deploys_to(channel, agent="x"):
    return f"targets:\n  dev:\n    agent: {agent}\n    slack_channel: {channel}\n"


FILES = {
    "bundles/assets/.claude-plugin/plugin.json": json.dumps({"name": "asset-search"}),
    "bundles/assets/deploy.yaml": deploys_to("C0EXAMPLE2", "asset-search"),
    "bundles/assets/skills/assets/SKILL.md": "---\nname: assets\n---\nFind assets.",
    "bundles/other/.claude-plugin/plugin.json": json.dumps({"name": "style-guide"}),
    "bundles/other/deploy.yaml": deploys_to("C0EXAMPLE5", "style-guide"),
}


def serve(files, sha="d" * 40):
    """A GitHub double over `files`: 404 for any path it does not hold."""
    def handle(request):
        path = request.url.path
        if path == "/repos/acme/agents/commits/main":
            return httpx.Response(200, json={"sha": sha})
        if path == f"/repos/acme/agents/git/trees/{sha}":
            tree = [{"path": p, "type": "blob"} for p in files]
            return httpx.Response(200, json={"tree": tree, "truncated": False})
        prefix = "/repos/acme/agents/contents/"
        if path.startswith(prefix) and request.url.params.get("ref") == sha:
            content = files.get(path[len(prefix):])
            if content is not None:
                encoded = base64.b64encode(content.encode()).decode()
                return httpx.Response(200, json={"content": encoded, "encoding": "base64"})
        return httpx.Response(404)
    return httpx.Client(transport=httpx.MockTransport(handle))


def sources(files=FILES, sha="a" * 40):
    return GitHubSources("ghp_test", (REPO,), client=serve(files, sha))


def test_finds_the_bundle_whose_deploy_target_names_the_channel():
    [found] = sources().find("C0EXAMPLE2", None)
    assert found.name == "asset-search"
    assert found.commit == "a" * 40
    assert "skills/assets/SKILL.md" in found.files


def test_a_name_hint_selects_by_plugin_name():
    [found] = sources().find("C0EXAMPLE6", "style-guide")
    assert found.path == "bundles/other"


def test_nothing_matches_is_an_empty_list_not_a_guess():
    assert sources().find("C0EXAMPLE7", None) == []


def test_skips_malformed_plugin_json_and_continues_discovering():
    """A broken plugin.json in one bundle doesn't prevent finding others."""
    files = {
        "bundles/broken/.claude-plugin/plugin.json": "{not json",
        "bundles/assets/.claude-plugin/plugin.json": json.dumps({"name": "asset-search"}),
        "bundles/assets/deploy.yaml": deploys_to("C0EXAMPLE2", "asset-search"),
    }
    # Should find asset-search by channel, skipping the broken bundle
    [found] = sources(files).find("C0EXAMPLE2", None)
    assert found.name == "asset-search"
    # Should not find anything by hint "style-guide" since it doesn't exist
    assert sources(files).find("C0EXAMPLE6", "style-guide") == []


def test_skips_malformed_deploy_yaml():
    """A malformed deploy.yaml doesn't prevent discovery by name hint."""
    files = {
        "bundles/broken/.claude-plugin/plugin.json": json.dumps({"name": "broken-deploy"}),
        "bundles/broken/deploy.yaml": "targets: [unclosed",
        "bundles/assets/.claude-plugin/plugin.json": json.dumps({"name": "asset-search"}),
        "bundles/assets/deploy.yaml": deploys_to("C0EXAMPLE2", "asset-search"),
    }
    # Should find by name hint, ignoring the broken deploy.yaml
    [found] = sources(files, "b" * 40).find("C0EXAMPLE6", "broken-deploy")
    assert found.name == "broken-deploy"
    # Should find asset-search by channel, skipping the broken-deploy bundle
    [found] = sources(files, "b" * 40).find("C0EXAMPLE2", None)
    assert found.name == "asset-search"


def test_raises_source_error_for_truncated_tree():
    """A truncated tree response raises SourceError."""

    def truncated_handler(request):
        path = request.url.path
        if path == "/repos/acme/agents/commits/main":
            return httpx.Response(200, json={"sha": "c" * 40})
        if path == f"/repos/acme/agents/git/trees/{'c' * 40}":
            return httpx.Response(200, json={"tree": [], "truncated": True})
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(truncated_handler))
    with pytest.raises(SourceError, match="truncated"):
        GitHubSources("ghp_test", (REPO,), client=client).find("C0EXAMPLE2", None)


def test_a_bundle_at_the_repository_root_is_found():
    files = {
        ".claude-plugin/plugin.json": json.dumps({"name": "root-bundle"}),
        "deploy.yaml": "targets:\n  dev:\n    slack_channel: C0EXAMPLE2\n",
        "skills/root/SKILL.md": "---\nname: root\n---\nRoot.",
        "README.md": "not a bundle file",
    }
    src = sources(files)
    [by_name] = src.find("C0EXAMPLE6", "root-bundle")
    assert by_name.path == ""
    assert set(by_name.files) == {
        ".claude-plugin/plugin.json", "deploy.yaml", "skills/root/SKILL.md",
    }
    [by_channel] = src.find("C0EXAMPLE2", None)
    assert by_channel.name == "root-bundle"


def test_a_bundle_matched_by_channel_with_an_unreadable_name_is_skipped():
    files = {
        "bundles/broken/.claude-plugin/plugin.json": "{not json",
        "bundles/broken/deploy.yaml": "targets:\n  dev:\n    slack_channel: C0EXAMPLE2\n",
        "bundles/nameless/.claude-plugin/plugin.json": json.dumps({"version": "1"}),
        "bundles/nameless/deploy.yaml": "targets:\n  dev:\n    slack_channel: C0EXAMPLE2\n",
        "bundles/good/.claude-plugin/plugin.json": json.dumps({"name": "good"}),
        "bundles/good/deploy.yaml": "targets:\n  dev:\n    slack_channel: C0EXAMPLE2\n",
    }
    src = sources(files)
    assert [b.name for b in src.find("C0EXAMPLE2", None)] == ["good"]


SPEC_FILES = {
    "bundles/assets/.claude-plugin/plugin.json": json.dumps({"name": "asset-search"}),
    "docs/specs/assets/search.md": "# Search\nFinds files.",
    "docs/specs/assets/sub/share.md": "# Share\nNeeds approval.",
    "docs/specs/assets/notes.txt": "not markdown",
    "docs/specs/assets-old/stale.md": "# a sibling directory, not under the spec path",
    "docs/specs/other.md": "# another bundle's spec",
}


def test_a_configured_spec_path_returns_its_markdown_at_the_same_commit():
    src = GitHubSources(
        "ghp_test", (REPO,), client=serve(SPEC_FILES),
        spec_paths={"asset-search": "docs/specs/assets"},
    )
    [found] = src.find("C0EXAMPLE6", "asset-search")
    assert found.spec == {
        "docs/specs/assets/search.md": "# Search\nFinds files.",
        "docs/specs/assets/sub/share.md": "# Share\nNeeds approval.",
    }
    assert found.spec_omitted == ()


def test_a_bundle_without_a_spec_path_reads_no_spec():
    src = GitHubSources(
        "ghp_test", (REPO,), client=serve(SPEC_FILES),
        spec_paths={"someone-else": "docs/specs"},
    )
    [found] = src.find("C0EXAMPLE6", "asset-search")
    assert found.spec == {} and found.spec_omitted == ()


def test_a_spec_over_the_cap_names_what_it_left_out():
    src = GitHubSources(
        "ghp_test", (REPO,), client=serve(SPEC_FILES),
        spec_paths={"asset-search": "docs/specs/assets"}, spec_max_chars=30,
    )
    [found] = src.find("C0EXAMPLE6", "asset-search")
    assert found.spec == {"docs/specs/assets/search.md": "# Search\nFinds files."}
    assert found.spec_omitted == ("docs/specs/assets/sub/share.md",)
