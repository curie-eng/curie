"""Status comments and state labels on Jira issues (#4358 AC 5, decisions 15 and 16).

The core hands `upsert_marked` a markdown body and a marker. On Jira the body
becomes ADF (``goldens/status_comment.json`` is the exact document for
``goldens/status_comment.md``), the marker lives only in the comment entity
property ``curie.factory`` as ``{"marker": ..., "body_sha256": ...}``, and
ownership is the property plus the bot's own ``accountId``. Everything is
judged from the fake's state and the requests it received.

ADF conventions the golden pins: a text node without marks has no ``marks``
key; a checklist line ``- [x] X`` is a bullet item whose text starts with
``[x] ``; an image becomes a paragraph holding a link; a paragraph that is only
an HTML comment disappears; the quoted handoff retains inline marks and lists.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import uuid
from pathlib import Path
from typing import Any

import pytest
from curie_api.factory_comment_text import marker_for
from curie_api.factory_notices import result_section, status_body
from curie_api.forges import types
from curie_api.forges.errors import NotFound
from curie_api.forges.types import PullRequestRef, ReplyTarget, RepositoryRef
from forge_fakes.jira import LABEL, JiraFake, flatten, jira_tracker

GOLDENS = Path(__file__).resolve().parent / "goldens"
REQUEST_ID = uuid.UUID("00000000-0000-4000-8000-000000000001")
MARKER = marker_for(REQUEST_ID)
PROPERTY = "curie.factory"
TRUNCATION = "Status truncated to fit Jira's comment limit."


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _sha(body: str) -> str:
    return hashlib.sha256(body.encode()).hexdigest()


def _setup() -> tuple[JiraFake, Any, ReplyTarget]:
    fake = JiraFake()
    issue = fake.seed_issue("Ticket")
    return fake, jira_tracker(fake), ReplyTarget.on_issue(fake.ref(issue))


def _issue_id(target: ReplyTarget) -> str:
    assert target.issue is not None
    return target.issue.issue_id


@pytest.mark.anyio
async def test_jira_comments_the_posted_adf_matches_the_golden() -> None:
    fake, tracker, target = _setup()
    body = (GOLDENS / "status_comment.md").read_text()
    golden = json.loads((GOLDENS / "status_comment.json").read_text())

    result = await tracker.marked_comments.upsert_marked(target, MARKER, body)

    assert result.written is True
    (create,) = fake.requests_of("comment_create")
    assert create.path == f"/rest/api/3/issue/{_issue_id(target)}/comment"
    assert create.json["body"] == golden
    # The first post notifies watchers; only edits are silent.
    assert create.params.get("notifyUsers") != "false"
    (own,) = fake.own_comments(_issue_id(target))
    assert result.comment.id == str(own.id)
    assert result.comment.target == target
    assert own.body == golden
    (quote,) = [node for node in own.body["content"] if node["type"] == "blockquote"]
    strong, code, ordered = quote["content"]
    assert strong["content"] == [
        {
            "type": "text",
            "text": "Maintainer decision required",
            "marks": [{"type": "strong"}],
        }
    ]
    assert code["content"][1] == {
        "type": "text",
        "text": "parser.py",
        "marks": [{"type": "code"}],
    }
    assert ordered["type"] == "orderedList"
    assert [flatten(item).strip() for item in ordered["content"]] == [
        "Confirm the input format.",
        "Pick the parser.",
    ]
    assert all(item["type"] == "listItem" for item in ordered["content"])


@pytest.mark.anyio
async def test_jira_comments_a_rendered_final_handoff_keeps_its_marks_and_list() -> None:
    fake, tracker, target = _setup()
    message = (
        "**Maintainer decision required**\n\n"
        "Choose `parser.py` before retrying.\n\n"
        "1. Confirm the input format.\n"
        "2. Pick the parser."
    )
    body = status_body(
        request_id=REQUEST_ID,
        card_url=None,
        pill_label="FAILED",
        phase_view=None,
        result=result_section("no_pull_request", pr_url=None, detail=message),
    )

    result = await tracker.marked_comments.upsert_marked(target, MARKER, body)

    golden = json.loads((GOLDENS / "status_comment.json").read_text())
    (expected_quote,) = [node for node in golden["content"] if node["type"] == "blockquote"]
    (own,) = fake.own_comments(_issue_id(target))
    (quote,) = [node for node in own.body["content"] if node["type"] == "blockquote"]
    assert result.written is True
    assert quote == expected_quote
    (create,) = fake.requests_of("comment_create")
    assert create.json["body"] == own.body
    assert "Cause: no_pull_request" in flatten(own.body)
    assert "Agent's last message:" in flatten(own.body)
    assert not any(node["type"] == "codeBlock" for node in own.body["content"])
    assert own.properties[PROPERTY]["body_sha256"] == _sha(body)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("message", "visible", "code"),
    [
        ("a & b", "a & b", None),
        ("Vec<T>", "Vec<T>", None),
        ("literal &lt;", "literal &lt;", None),
        ("literal &#64;someone", "literal &#64;someone", None),
        ("![x](https://example.com/x.png)", "![x](https://example.com/x.png)", None),
        ("`a & Vec<T>`", "a &amp; Vec&lt;T>", "inline"),
        ("```text\na & Vec<T>\n```", "a &amp; Vec&lt;T>", "block"),
    ],
)
async def test_jira_comments_handoff_keeps_inert_prose_and_encoded_code(
    message: str, visible: str, code: str | None
) -> None:
    fake, tracker, target = _setup()
    body = status_body(
        request_id=REQUEST_ID,
        card_url=None,
        pill_label="FAILED",
        phase_view=None,
        result=result_section("no_pull_request", pr_url=None, detail=message),
    )
    written = await tracker.marked_comments.upsert_marked(target, MARKER, body)
    assert written.written is True
    (create,) = fake.requests_of("comment_create")
    (quote,) = [node for node in create.json["body"]["content"] if node["type"] == "blockquote"]
    assert flatten(quote).strip() == visible
    nodes = [quote]
    for node in nodes:
        nodes.extend(node.get("content", []))
        assert node["type"] not in {"mention", "media", "mediaSingle", "mediaGroup"}
        assert all(mark["type"] != "link" for mark in node.get("marks", []))
    # CommonMark leaves entities encoded in code after the mandatory global encoder.
    assert any({"type": "code"} in node.get("marks", []) for node in nodes) == (code == "inline")
    assert any(node["type"] == "codeBlock" for node in nodes) == (code == "block")


@pytest.mark.anyio
async def test_jira_comments_raw_markdown_obeys_commonmark_entity_rules() -> None:
    # https://spec.commonmark.org/0.31.2/#entity-and-numeric-character-references
    fake, tracker, target = _setup()
    body = (
        "R&notit; and &copy stay literal; R&D; &amp; &#64; &#x40; &commat; &amp;lt;.\n\n"
        "[query](https://example.com/?a=1&notit=2)"
    )
    written = await tracker.marked_comments.upsert_marked(target, MARKER, body)
    assert written.written is True
    (create,) = fake.requests_of("comment_create")
    prose, link = create.json["body"]["content"]
    assert prose == {
        "type": "paragraph",
        "content": [
            {"type": "text", "text": "R&notit; and &copy stay literal; R&D; & @ @ @ &lt;."}
        ],
    }
    assert link == {
        "type": "paragraph",
        "content": [
            {
                "type": "text",
                "text": "query",
                "marks": [{"type": "link", "attrs": {"href": "https://example.com/?a=1&notit=2"}}],
            }
        ],
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    "url",
    ["https://example.com/?a=1&b=2", "https://example.com/?a=1&amp;b=2", "javascript:alert(1)"],
)
async def test_jira_comments_handoff_links_stay_inert_and_visible(url: str) -> None:
    fake, tracker, target = _setup()
    card_url = "https://example.com/status.svg?a=1&b=2"
    body = status_body(
        request_id=REQUEST_ID,
        card_url=card_url,
        pill_label="FAILED",
        phase_view=None,
        result=result_section("no_pull_request", pr_url=None, detail=f"[query]({url})"),
    )
    written = await tracker.marked_comments.upsert_marked(target, MARKER, body)
    assert written.written is True
    (create,) = fake.requests_of("comment_create")
    (quote,) = [node for node in create.json["body"]["content"] if node["type"] == "blockquote"]
    assert flatten(quote).strip() == f"[query]({url})"
    nodes = [quote]
    for node in nodes:
        nodes.extend(node.get("content", []))
    assert not any(mark["type"] == "link" for node in nodes for mark in node.get("marks", []))
    outside = [node for node in create.json["body"]["content"] if node["type"] != "blockquote"]
    for node in outside:
        outside.extend(node.get("content", []))
    assert [
        mark for node in outside for mark in node.get("marks", []) if mark["type"] == "link"
    ] == [{"type": "link", "attrs": {"href": card_url}}]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("message", "retained"), [("# Heading", "Heading"), ("---", "---"), ("> quoted", "quoted")]
)
async def test_jira_comments_handoff_uses_only_schema_valid_quote_children(
    message: str, retained: str
) -> None:
    fake, tracker, target = _setup()
    message += "\n\n**Ordinary prose.**\n\n1. Ordered item.\n\n- Bullet item.\n\n```text\ncode\n```"
    body = "# Top-level\n\n---\n\n" + status_body(
        request_id=REQUEST_ID,
        card_url=None,
        pill_label="FAILED",
        phase_view=None,
        result=result_section("no_pull_request", pr_url=None, detail=message),
    )
    written = await tracker.marked_comments.upsert_marked(target, MARKER, body)
    assert written.written is True
    (create,) = fake.requests_of("comment_create")
    document = create.json["body"]
    assert [node["type"] for node in document["content"][:2]] == ["heading", "rule"]
    (quote,) = [node for node in document["content"] if node["type"] == "blockquote"]
    # https://go.atlassian.com/adf-json-schema #/definitions/blockquote_node
    # Its content excludes headings, rules and nested blockquotes.
    allowed = {
        "paragraph",
        "orderedList",
        "bulletList",
        "codeBlock",
        "mediaSingle",
        "mediaGroup",
        "extension",
    }
    nodes = [quote]
    for node in nodes:
        children = node.get("content", [])
        if node["type"] == "blockquote":
            assert children and all(child["type"] in allowed for child in children)
        nodes.extend(children)
    assert any(
        node.get("text") == "Ordinary prose." and {"type": "strong"} in node.get("marks", [])
        for node in nodes
    )
    assert {"paragraph", "orderedList", "bulletList", "codeBlock"} <= {
        node["type"] for node in quote["content"]
    }
    assert all(
        text in flatten(quote)
        for text in (retained, "Ordinary prose.", "Ordered item.", "Bullet item.", "code")
    )


@pytest.mark.anyio
async def test_jira_comments_a_non_handoff_fence_keeps_its_language() -> None:
    fake, tracker, target = _setup()

    await tracker.marked_comments.upsert_marked(target, MARKER, "```text\ntests failed: 2\n```\n")

    (own,) = fake.own_comments(_issue_id(target))
    assert own.body["content"] == [
        {
            "type": "codeBlock",
            "attrs": {"language": "text"},
            "content": [{"type": "text", "text": "tests failed: 2"}],
        }
    ]


@pytest.mark.anyio
async def test_jira_comments_the_marker_lives_only_in_the_entity_property() -> None:
    fake, tracker, target = _setup()
    body = (GOLDENS / "status_comment.md").read_text()

    result = await tracker.marked_comments.upsert_marked(target, MARKER, body)

    comment = fake.comment(result.comment.id)
    assert comment.properties == {PROPERTY: {"marker": MARKER, "body_sha256": _sha(body)}}
    visible = json.dumps(comment.body)
    assert str(REQUEST_ID) not in visible
    assert "curie-execution-request" not in visible
    assert "curie-status:final" not in visible
    assert "<!--" not in visible


@pytest.mark.anyio
async def test_jira_comments_a_second_upsert_edits_the_same_comment_silently() -> None:
    fake, tracker, target = _setup()
    comments = tracker.marked_comments
    created = await comments.upsert_marked(target, MARKER, "Status: QUEUED\n")

    edited = await comments.upsert_marked(target, MARKER, "Status: WORKING\n")

    assert edited.written is True
    assert edited.comment.id == created.comment.id
    (update,) = fake.requests_of("comment_update")
    assert update.method == "PUT"
    assert update.path == (f"/rest/api/3/issue/{_issue_id(target)}/comment/{created.comment.id}")
    assert update.params.get("notifyUsers") == "false"
    assert len(fake.requests_of("comment_create")) == 1
    (own,) = fake.own_comments(_issue_id(target))
    assert flatten(own.body).strip() == "Status: WORKING"
    assert own.properties[PROPERTY] == {
        "marker": MARKER,
        "body_sha256": _sha("Status: WORKING\n"),
    }
    found = await comments.find_marked(target, MARKER)
    assert found is not None and found.id == created.comment.id


@pytest.mark.anyio
async def test_jira_comments_an_unchanged_body_writes_nothing() -> None:
    fake, tracker, target = _setup()
    comments = tracker.marked_comments
    created = await comments.upsert_marked(target, MARKER, "Status: WORKING\n")
    writes = fake.writes
    sent = len(fake.requests)

    same = await comments.upsert_marked(target, MARKER, "Status: WORKING\n")

    assert same.written is False
    assert same.comment.id == created.comment.id
    assert fake.writes == writes
    later = fake.requests[sent:]
    assert all(r.method == "GET" or r.kind == "comment_list" for r in later), later


@pytest.mark.anyio
async def test_jira_comments_a_human_comment_carrying_the_property_is_never_found_or_edited() -> (
    None
):
    fake, tracker, target = _setup()
    comments = tracker.marked_comments
    human = fake.add_user("712020:00000000-0000-4000-8000-000000000101", "Human 1")
    body = "Status: WORKING\n"
    pasted = fake.add_comment(
        _issue_id(target),
        human,
        "pasted status",
        properties={PROPERTY: {"marker": MARKER, "body_sha256": _sha(body)}},
    )
    # The bot's own comment for another request's marker is not this one either.
    fake.add_comment(
        _issue_id(target),
        fake.bot,
        "another run",
        properties={PROPERTY: {"marker": marker_for(uuid.uuid4()), "body_sha256": _sha("x")}},
    )

    assert await comments.find_marked(target, MARKER) is None
    created = await comments.upsert_marked(target, MARKER, body)

    assert created.written is True
    assert created.comment.id != pasted
    human_comment = fake.comment(pasted)
    assert flatten(human_comment.body).strip() == "pasted status"
    touched = [
        r.path for r in fake.requests if r.kind in {"comment_update", "comment_property_put"}
    ]
    assert not any(re.search(rf"/comment/{pasted}(/|$)", path) for path in touched)
    # Liveness: our own comment is found, and an unchanged upsert does not post again.
    found = await comments.find_marked(target, MARKER)
    assert found is not None and found.id == created.comment.id
    assert (await comments.upsert_marked(target, MARKER, body)).written is False
    assert len(fake.issue(_issue_id(target)).comments) == 3


@pytest.mark.anyio
async def test_jira_comments_a_body_over_the_limit_is_cut_at_a_line_with_a_note() -> None:
    fake, tracker, target = _setup()
    blocks: list[str] = []
    while sum(len(block) + 2 for block in blocks) < 40_000:
        blocks.append(f"Line {len(blocks):05d} " + "x" * 40)
    body = "\n\n".join(blocks) + "\n"
    assert len(body) >= 40_000

    await tracker.marked_comments.upsert_marked(target, MARKER, body)

    (own,) = fake.own_comments(_issue_id(target))
    paragraphs = own.body["content"]
    note = paragraphs[-1]
    assert note == {
        "type": "paragraph",
        "content": [{"type": "text", "text": TRUNCATION, "marks": [{"type": "em"}]}],
    }
    kept = [flatten(paragraph).strip() for paragraph in paragraphs[:-1]]
    assert kept == blocks[: len(kept)], "every kept line is whole and in order"
    kept_source = "\n\n".join(kept)
    assert 29_000 < len(kept_source) < 30_000
    assert all(p["type"] == "paragraph" for p in paragraphs)


@pytest.mark.anyio
async def test_jira_comments_only_issue_targets_live_on_the_tracker() -> None:
    _fake, tracker, _target = _setup()
    repository = RepositoryRef(types.GITHUB, "github.com", "4401", "acme-corp/acme-bot")
    pull = ReplyTarget.on_pull_request(PullRequestRef(repository, "12"))

    with pytest.raises(NotFound):
        await tracker.marked_comments.find_marked(pull, MARKER)
    with pytest.raises(NotFound):
        await tracker.marked_comments.upsert_marked(pull, MARKER, "Status: WORKING\n")


# State labels (decision 16) --------------------------------------------------------


@pytest.mark.anyio
async def test_jira_comments_set_state_label_is_one_put_of_label_operations() -> None:
    fake = JiraFake()
    human = fake.add_user("712020:00000000-0000-4000-8000-000000000101", "Human 1")
    issue = fake.seed_issue("Ticket")
    fake.add_label(issue.id, LABEL, human)
    fake.add_label(issue.id, "curie-factory:queued", human)
    tracker = jira_tracker(fake)

    await tracker.set_state_label(
        fake.ref(issue),
        add="curie-factory:working",
        remove=["curie-factory:queued", "curie-factory:failed"],
    )

    (put,) = fake.requests_of("issue_update")
    assert (put.method, put.path) == ("PUT", f"/rest/api/3/issue/{issue.id}")
    # Operations, never a whole `fields.labels` value that would drop a label a
    # person added meanwhile.
    assert "fields" not in put.json
    operations = {json.dumps(op, sort_keys=True) for op in put.json["update"]["labels"]}
    assert operations == {
        json.dumps({"add": "curie-factory:working"}),
        json.dumps({"remove": "curie-factory:queued"}),
        json.dumps({"remove": "curie-factory:failed"}),
    }
    assert fake.issue(issue.id).labels == [LABEL, "curie-factory:working"]


@pytest.mark.anyio
async def test_jira_comments_a_refused_state_label_write_is_logged_and_done(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake = JiraFake()
    issue = fake.seed_issue("Ticket")
    tracker = jira_tracker(fake)
    fake.forbid_label_edits = True

    with caplog.at_level(logging.DEBUG):
        await tracker.set_state_label(fake.ref(issue), add="curie-factory:working", remove=())

    assert len(fake.requests_of("issue_update")) == 1
    assert fake.issue(issue.id).labels == []
    assert any(record.name.startswith("curie_api") for record in caplog.records)


def test_the_golden_source_is_the_shape_status_body_writes() -> None:
    """The golden body ends with the marker line the core appends, so dropping
    HTML comments is what keeps the marker out of the visible text."""

    source = (GOLDENS / "status_comment.md").read_text()
    assert source.rstrip("\n").endswith(MARKER)
