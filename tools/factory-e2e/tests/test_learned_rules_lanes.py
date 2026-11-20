"""Lane independence by ticket for the learned rules scenario (#5799, criterion 8).

Every request here is built from the production producers: the revision
objective from ``curie_api.github_factory_review._objective``, the feedback URL
from ``curie_api.factory_reply_target.feedback_url``, wrapped in the platform
sender frame. The model server is real, on loopback, and is posted to over HTTP.
"""

from __future__ import annotations

import json
import threading
import uuid
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import learned_rules_lanes as lrl
import pytest
import scripted_kind as sk
import scripted_lanes as lanes
from curie_api.factory_reply_target import feedback_url
from curie_api.github_factory_review import _objective
from curie_api.github_review_events import UnverifiedFeedback
from curie_runner.sender_frame import frame_user_turn

REPO_ROOT = Path(__file__).resolve().parents[3]
MODEL_SCRIPT = REPO_ROOT / "tools/model-script/model_script.py"
BASE = "https://stub.example.test"
REPO = "acme-corp/acme-bot"
# Pull request numbers by ticket letter, plus one that closes no ticket.
PULLS = {"a": 11, "b": 12, "c": 13}
UNRELATED_PULL = 14
BRANCH = "curie/publication-" + "0" * 32  # publication_branch_name, default prefix


@pytest.fixture
def model_module() -> Any:
    return sk._module("learned_rules_model_script", MODEL_SCRIPT)


class _FakeStub:
    """Pull records shaped like ``GitHubStub`` builds them (``tools/github-stub``,
    the ``POST pulls`` handler): number, body, head.ref, html_url. The body
    carries the factory's closing reference ``Closes #<n>`` (``GitHubTracker.
    closing_reference``)."""

    def __init__(self) -> None:
        self.pulls: dict[int, dict[str, Any]] = {}
        for letter, number in PULLS.items():
            self._add(number, f"Adds a unit.\n\nCloses #{lrl.TICKETS[letter]}")
        self._add(UNRELATED_PULL, "A person's change with no closing reference.")

    def _add(self, number: int, body: str) -> None:
        self.pulls[number] = {
            "number": number,
            "state": "open",
            "title": "Change",
            "body": body,
            "head": {"ref": BRANCH},
            "html_url": f"{BASE}/{REPO}/pull/{number}",
        }

    def _get(self, kind: str, number: int) -> dict[str, Any] | None:
        assert kind == "pull"
        return self.pulls.get(number)


def _label_text(issue: int) -> str:
    # The label objective as ``_facts`` builds it: {html_base}/{repo}/issues/{n}.
    return frame_user_turn("job", "", f"{BASE}/{REPO}/issues/{issue}", "github")


def _feedback(pull: int, fragment: str) -> UnverifiedFeedback:
    return UnverifiedFeedback(
        delivery_id=uuid.UUID(int=1),
        event="issue_comment",
        installation_id=41,
        repository_id=9101,
        repo_full_name=REPO,
        pr_number=pull,
        feedback_id=1,
        sender_id=1,
        sender_login="example-reviewer",
        body="Use assertAlmostEqual with places=6 everywhere, @factory.",
        url=feedback_url(BASE, REPO, pull, fragment),
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        head_sha=None,
        commit_sha=None,
        author_association="MEMBER",
    )


def _revision_text(pull: int, fragment: str = "issuecomment-501") -> str:
    objective = _objective(_feedback(pull, fragment), feedback_url(BASE, REPO, pull, fragment), {})
    return frame_user_turn("job", "", objective, "github")


def _body(text: str, *, ordinal: int = 0, tools: list[str] | None = None) -> dict[str, Any]:
    messages: list[dict[str, Any]] = [{"role": "user", "content": [{"type": "text", "text": text}]}]
    for index in range(ordinal):
        messages += [
            {"role": "assistant", "content": [{"type": "text", "text": f"earlier {index}"}]},
            {"role": "user", "content": [{"type": "text", "text": f"result {index}"}]},
        ]
    payload: dict[str, Any] = {
        "model": "scripted",
        "system": [{"type": "text", "text": "Factory system."}],
        "messages": messages,
    }
    if tools is not None:
        payload["tools"] = [{"name": name} for name in tools]
    return payload


def _post(base: str, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    request = Request(
        base + "/v1/messages",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=10) as response:
            return response.status, json.load(response)
    except HTTPError as exc:
        return exc.code, json.load(exc)


def _answer(status_body: tuple[int, dict[str, Any]]) -> str:
    status, body = status_body
    assert status == 200, body
    return str(body["content"][0]["text"])


def _message(text: str) -> dict[str, Any]:
    return {"type": "message", "role": "assistant", "content": [{"type": "text", "text": text}]}


def _lane_exchanges(model_module: Any, letter: str) -> list[dict[str, Any]]:
    """Label turns 0 and 1, and the revision turn 0, all answered with the ticket's own text."""

    label = model_module.scrub(_label_text(1))
    revision = model_module.scrub(_revision_text(PULLS["a"]))
    return [
        {"lane_key": label, "ordinal": 0, "message": _message(f"{letter}-impl-0")},
        {"lane_key": label, "ordinal": 1, "message": _message(f"{letter}-impl-1")},
        {"lane_key": revision, "ordinal": 0, "message": _message(f"{letter}-revision-0")},
    ]


def _empty_transcript(tmp_path: Path, model_module: Any) -> Path:
    path = tmp_path / "learned-rules-chain-a.json"
    model_module.dump_transcript(path, [])
    return path


@pytest.fixture
def served(model_module: Any, tmp_path: Path) -> Any:
    stub = _FakeStub()
    identity = lrl.issue_identity(stub)
    paths = lrl.transcript_paths(_empty_transcript(tmp_path, model_module))
    for letter, path in paths.items():
        model_module.dump_transcript(path, _lane_exchanges(model_module, letter))
    router = lrl.build_router(model_module, paths, identity)
    model_module.dump_transcript(tmp_path / "empty.json", [])
    script = model_module.ModelScript(
        tmp_path / "empty.json", capture_dir=tmp_path / "capture", issue_of=identity
    )
    router.install(script, model_module, fallback=False)
    script.start()
    yield script, router, tmp_path / "capture"
    script._server.shutdown()
    script._server.server_close()


def test_the_three_label_prompts_collapse_after_scrub(model_module: Any) -> None:
    """Negative control: the model server's own key cannot tell the tickets apart."""

    keys = {model_module.request_key(_body(_label_text(n)))[0] for n in lrl.TICKETS.values()}
    assert len(keys) == 1


@pytest.mark.parametrize(
    "order",
    [["a", "b", "c", "c", "b", "a"], ["c", "a", "a", "b", "c", "b"]],
    ids=["abc-cba", "caabcb"],
)
def test_each_request_is_answered_from_its_own_ticket_lane(served: Any, order: list[str]) -> None:
    script, router, _ = served
    seen = {letter: 0 for letter in lrl.TICKETS}
    for letter in order:
        ordinal = seen[letter]
        seen[letter] += 1
        answer = _answer(
            _post(script.base_url, _body(_label_text(lrl.TICKETS[letter]), ordinal=ordinal))
        )
        assert answer == f"{letter}-impl-{ordinal}"
    assert router.unrouted == []
    assert script.unexpected == []


@pytest.mark.parametrize(
    "fragment", ["discussion_r601", "pullrequestreview-602", "issuecomment-603"]
)
def test_feedback_url_on_a_ticket_pull_request_is_answered_from_that_ticket(
    served: Any, fragment: str
) -> None:
    script, router, _ = served
    for letter in ("a", "b"):
        text = _revision_text(PULLS[letter], fragment)
        assert f"/pull/{PULLS[letter]}#{fragment}" in text
        assert _answer(_post(script.base_url, _body(text))) == f"{letter}-revision-0"
    # The named counterexample: ticket B's pull request never reaches lane A.
    assert router.lanes[("implementation", lrl.TICKETS["a"])].served == [0]
    assert router.lanes[("implementation", lrl.TICKETS["b"])].served == [0]


def test_a_pull_request_that_closes_no_ticket_gets_no_lane(served: Any) -> None:
    script, router, _ = served
    status, _ = _post(script.base_url, _body(_revision_text(UNRELATED_PULL)))
    assert status == 422
    assert router.unrouted == [{"agent": "implementation", "issue": None}]


def test_an_issue_with_no_lane_is_refused_and_consumes_nothing(served: Any) -> None:
    script, router, _ = served
    before = {key: set(lane.used) for key, lane in router.lanes.items()}
    status, _ = _post(script.base_url, _body(_label_text(9999)))
    assert status == 422
    assert {key: set(lane.used) for key, lane in router.lanes.items()} == before
    assert len(script.unexpected) == 1
    assert router.unrouted == [{"agent": "implementation", "issue": None}]


def test_without_issue_of_the_router_routes_as_before(model_module: Any, tmp_path: Path) -> None:
    transcript = tmp_path / "t.json"
    model_module.dump_transcript(
        transcript,
        [
            {
                "lane_key": model_module.scrub(_label_text(1)),
                "ordinal": 0,
                "message": _message("fallback"),
            }
        ],
    )
    router = lanes.LaneRouter({})
    assert router.issue_of is lanes.request_issue
    router.add(lanes.Lane("own", lanes.IMPLEMENTATION, 3815, _lane_exchanges(model_module, "x")))
    script = model_module.ModelScript(transcript)
    router.install(script, model_module)
    script.start()
    try:
        assert _answer(_post(script.base_url, _body(_label_text(3815)))) == "x-impl-0"
        # No lane for 4000, and the default keeps the transcript fallback.
        assert _answer(_post(script.base_url, _body(_label_text(4000)))) == "fallback"
        assert router.unrouted == []
    finally:
        script._server.shutdown()
        script._server.server_close()


def test_ticket_of_pull_reads_the_closing_reference_and_cross_checks_the_branch() -> None:
    stub = _FakeStub()
    assert [lrl.ticket_of_pull(stub, n) for n in (11, 12, 13)] == [3815, 3921, 3922]
    assert lrl.ticket_of_pull(stub, UNRELATED_PULL) is None
    assert lrl.ticket_of_pull(stub, 999) is None
    stub.pulls[11]["body"] = "Closes #9999"
    assert lrl.ticket_of_pull(stub, 11) is None
    stub.pulls[12]["head"]["ref"] = "curie/issue-3815"
    assert lrl.ticket_of_pull(stub, 12) is None


def test_transcript_paths_need_the_a_file_name(tmp_path: Path) -> None:
    paths = lrl.transcript_paths(tmp_path / "chain-a.json")
    assert paths["b"] == tmp_path / "chain-b.json" and paths["c"] == tmp_path / "chain-c.json"
    with pytest.raises(lanes.LaneError):
        lrl.transcript_paths(tmp_path / "chain.json")


def test_capture_carries_the_raw_issue_the_scrubbed_key_lost(served: Any) -> None:
    script, _, capture = served
    text = _label_text(lrl.TICKETS["b"])
    _post(script.base_url, _body(text, tools=["Bash", "propose_rule"]))
    _post(script.base_url, _body(_revision_text(PULLS["b"])))
    first, second = (json.loads(p.read_text()) for p in sorted(capture.glob("*.json")))
    assert first["issue"] == 3921 and second["issue"] == 3921
    assert str(3921) not in first["lane_key"]
    assert first["system"] == "Factory system."
    assert first["tools"] == ["Bash", "propose_rule"]
    assert first["ordinal"] == 0 and first["sequence"] == 1


class _Upstream:
    """A provider that echoes which request it answered, so a test can tell exchanges apart."""

    def __init__(self) -> None:
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt: str, *args: Any) -> None:
                return

            def do_POST(self) -> None:  # noqa: N802
                raw = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                messages = raw["messages"]
                mark = f"{lanes.first_user_text(raw)[-40:]}|{len(messages) // 2}"
                payload = json.dumps(_message(mark)).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


def test_interleaved_recording_splits_into_one_transcript_per_ticket(
    model_module: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CURIE_FACTORY_RECORD_KEY", "placeholder-record-key")
    upstream = _Upstream()
    identity = lrl.issue_identity(_FakeStub())
    recorder = model_module.ModelScript(
        tmp_path / "recorded.json",
        record=True,
        upstream=upstream.url,
        tag=lrl.ticket_tag(identity),
    )
    recorder.start()
    # Ticket A and C by issue URL, B by the feedback URL on its pull request.
    texts = {
        "a": _label_text(lrl.TICKETS["a"]),
        "b": _revision_text(PULLS["b"]),
        "c": _label_text(lrl.TICKETS["c"]),
    }
    start = threading.Barrier(3)

    def drive(letter: str) -> None:
        start.wait(timeout=10)
        for ordinal in range(3):
            assert _post(recorder.base_url, _body(texts[letter], ordinal=ordinal))[0] == 200

    threads = [threading.Thread(target=drive, args=(letter,)) for letter in texts]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        exchanges = list(recorder._exchanges)
        tags = list(recorder.recorded_tags)
    finally:
        recorder.close()
        upstream.close()
    split = lrl.split_recording(exchanges, tags)
    assert {letter: len(items) for letter, items in split.items()} == {"a": 3, "b": 3, "c": 3}
    for letter, items in split.items():
        assert [e["ordinal"] for e in items] == [0, 1, 2]
        assert all(e["lane_key"] == model_module.scrub(texts[letter]) for e in items)
        assert all(
            e["message"]["content"][0]["text"].endswith(f"|{i}") for i, e in enumerate(items)
        )
    out = lrl.transcript_paths(tmp_path / "learned-rules-chain-a.json")
    lrl.write_transcripts(model_module, split, out)
    assert [e["ordinal"] for e in model_module.load_transcript(out["b"])] == [0, 1, 2]


def test_an_untagged_exchange_fails_the_split(
    model_module: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CURIE_FACTORY_RECORD_KEY", "placeholder-record-key")
    upstream = _Upstream()
    recorder = model_module.ModelScript(
        tmp_path / "recorded.json",
        record=True,
        upstream=upstream.url,
        tag=lrl.ticket_tag(lrl.issue_identity(_FakeStub())),
    )
    recorder.start()
    try:
        assert _post(recorder.base_url, _body(_label_text(lrl.TICKETS["a"])))[0] == 200
        assert _post(recorder.base_url, _body(_label_text(9999)))[0] == 200
        exchanges, tags = list(recorder._exchanges), list(recorder.recorded_tags)
    finally:
        recorder.close()
        upstream.close()
    assert tags == ["a", None]
    with pytest.raises(lanes.LaneError):
        lrl.split_recording(exchanges, tags)
    with pytest.raises(lanes.LaneError):
        lrl.split_recording(exchanges, tags[:1])
    with pytest.raises(lanes.LaneError):
        lrl.split_recording(exchanges, ["a", "z"])


def test_scrub_recording_replaces_every_string_and_copies() -> None:
    original = [
        {
            "lane_key": "http://127.0.0.1:9/x",
            "ordinal": 0,
            "message": {"content": [{"text": "see http://127.0.0.1:9/y 10.0.0.5"}]},
        }
    ]
    cleaned = lrl.scrub_recording(
        original, {"http://127.0.0.1:9": "https://stub.example.test", "10.0.0.5": "192.0.2.1"}
    )
    assert cleaned[0]["lane_key"] == "https://stub.example.test/x"
    assert cleaned[0]["message"]["content"][0]["text"] == (
        "see https://stub.example.test/y 192.0.2.1"
    )
    assert original[0]["lane_key"] == "http://127.0.0.1:9/x"


# A revision quotes ticket A's pull request body, whose factory marker names the
# stub's address outside any URL. The recording replaced the address it saw; the
# replay's stub listens elsewhere, so the router must apply its own replacements.
RECORDED_AT = "192.0.2.10:36001"
REPLAYED_AT = "192.0.2.20:41002"


def _marker_revision_text(address: str) -> str:
    body = (
        "Adds Kelvin.\n\n"
        f"<!-- curie-factory-issue kind=github host={address} scope=4401 issue=3815 -->"
    )
    url = feedback_url(BASE, REPO, PULLS["a"], "issuecomment-2")
    objective = _objective(
        _feedback(PULLS["a"], "issuecomment-2"), url, {"pull_request_body": body}
    )
    return frame_user_turn("job", "", objective, "github")


def _replacements(address: str) -> dict[str, str]:
    host = address.split(":")[0]
    return {address: "stub.example.test", host: "stub.example.test"}


@pytest.mark.parametrize("normalized", [True, False])
def test_a_replayed_revision_quoting_the_stub_address_matches_its_recording(
    model_module: Any, tmp_path: Path, normalized: bool
) -> None:
    recorded_key = model_module.scrub(_marker_revision_text(RECORDED_AT))
    assert RECORDED_AT in recorded_key  # the key scrub leaves the bare address
    [exchange] = lrl.scrub_recording(
        [{"lane_key": recorded_key, "ordinal": 0, "message": _message("a-revision-0")}],
        _replacements(RECORDED_AT),
    )
    paths = lrl.transcript_paths(_empty_transcript(tmp_path, model_module))
    for letter, path in paths.items():
        model_module.dump_transcript(path, [exchange] if letter == "a" else [])
    router = lrl.build_router(
        model_module,
        paths,
        lrl.issue_identity(_FakeStub()),
        _replacements(REPLAYED_AT) if normalized else None,
    )
    model_module.dump_transcript(tmp_path / "empty.json", [])
    script = model_module.ModelScript(tmp_path / "empty.json")
    router.install(script, model_module, fallback=False)
    script.start()
    try:
        status, body = _post(script.base_url, _body(_marker_revision_text(REPLAYED_AT)))
    finally:
        script._server.shutdown()
        script._server.server_close()
    if normalized:
        assert status == 200 and body["content"][0]["text"] == "a-revision-0"
    else:
        assert status == 422
        assert router.lanes[("implementation", lrl.TICKETS["a"])].used == set()
