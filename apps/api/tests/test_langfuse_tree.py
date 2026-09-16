"""Unit tests for the observation-tree reconstruction (no I/O)."""

from curie_api.langfuse import build_tree


def _observations() -> list[dict[str, object]]:
    # A 3-level tree: agent.run -> llm.generation -> {search_repo, write_file}.
    return [
        {
            "id": "root",
            "type": "SPAN",
            "name": "agent.run",
            "startTime": "2026-07-05T00:00:00Z",
            "parentObservationId": None,
        },
        {
            "id": "gen",
            "type": "GENERATION",
            "name": "llm.generation",
            "startTime": "2026-07-05T00:00:01Z",
            "model": "claude-opus-4-8",
            "usageDetails": {"input": 1200, "output": 88},
            "parentObservationId": "root",
        },
        {
            "id": "tool-b",
            "type": "SPAN",
            "name": "write_file",
            "startTime": "2026-07-05T00:00:03Z",
            "parentObservationId": "gen",
        },
        {
            "id": "tool-a",
            "type": "SPAN",
            "name": "search_repo",
            "startTime": "2026-07-05T00:00:02Z",
            "parentObservationId": "gen",
        },
    ]


def test_build_tree_reconstructs_three_levels() -> None:
    tree = build_tree(_observations())

    assert len(tree) == 1
    root = tree[0]
    assert root.name == "agent.run"

    assert len(root.children) == 1
    gen = root.children[0]
    assert gen.type == "GENERATION"
    assert gen.model == "claude-opus-4-8"
    assert gen.usageDetails == {"input": 1200, "output": 88}

    # Children are ordered by startTime, so search_repo (t2) precedes write_file (t3).
    assert [c.name for c in gen.children] == ["search_repo", "write_file"]


def test_orphaned_parent_is_promoted_to_root() -> None:
    # An observation whose parent is not in the set is treated as a root, so no
    # data is dropped when a partial page is returned.
    observations = [
        {"id": "a", "type": "SPAN", "name": "a", "parentObservationId": "missing"},
    ]
    tree = build_tree(observations)
    assert [n.id for n in tree] == ["a"]


def _tool_observation(obs_id: str, bag: dict[str, object]) -> dict[str, object]:
    base: dict[str, object] = {
        "id": obs_id,
        "type": "SPAN",
        "name": "execute_tool",
        "parentObservationId": None,
    }
    base.update(bag)
    return base


def test_execute_tool_node_exposes_the_tool_name() -> None:
    # The trace gate must be able to assert WHICH tool ran, not merely that some
    # tool ran, so the tool name is surfaced from every nesting shape Langfuse
    # uses for OTel span attributes.
    observations = [
        _tool_observation("top", {"gen_ai.tool.name": "Bash"}),
        _tool_observation("meta", {"metadata": {"gen_ai.tool.name": "Bash"}}),
        _tool_observation(
            "meta-attrs",
            {"metadata": {"attributes": {"gen_ai.tool.name": "Bash"}}},
        ),
    ]

    tree = build_tree(observations)

    assert [n.toolName for n in tree] == ["Bash", "Bash", "Bash"]


def test_observation_without_the_attribute_exposes_no_tool_name() -> None:
    tree = build_tree([_tool_observation("bare", {})])

    assert tree[0].toolName is None
