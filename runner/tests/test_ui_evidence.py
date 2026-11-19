"""The runner's UI evidence routes over a real git checkout.

``POST /v1/ui-evidence/index`` and ``POST /v1/ui-evidence/image`` are served by
the real ``create_app(..., ui_evidence_workspace=...)`` over a checkout built in
``tmp_path`` from each case of the shared corpus
``tests/vectors/ui-evidence-cases.json``. The candidate tree C every expectation
names is computed here with plain git (a scratch index, ``git add -A``,
``git write-tree``), never by runner code. Every case also proves the code
publication path is unchanged: the ``/v1/snapshot`` patch and changed paths are
identical before and after the evidence routes ran and with the evidence
directory removed.

The checkout and PNG builders live in ``curie_test_support.ui_evidence`` and are
shared with the worker boundary tests, so both boundaries drive the same fixtures.
"""

from __future__ import annotations

import base64
import contextlib
import copy
import os
import shutil
from collections.abc import AsyncIterator, Mapping
from pathlib import Path
from typing import Any

import anyio
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from curie_runner import RunTracer, SideEffectClassifier, create_app, workspace_snapshot
from curie_runner.executor import EXECUTOR_MODE
from curie_runner.fake import FakeModelSession
from curie_runner.server import create_executor_app
from curie_runner.session import SessionRunner
from curie_test_support.ui_evidence import (
    CASES,
    CORPUS,
    GITHUB,
    MAX_IMAGE_BYTES,
    MAX_MANIFEST_BYTES,
    MAX_TOTAL_BYTES,
    REPO,
    CaseCheckout,
    build_case_checkout,
    png,
    resolve,
    sha256_hex,
)

TOKEN = "runner-token-value"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
TITLE = "Publish the agent change"
BODY = "Carries every local change from the prepared base."
INDEX_ROUTE = "/v1/ui-evidence/index"
IMAGE_ROUTE = "/v1/ui-evidence/image"


# --- Runner app builders -------------------------------------------------------


def session_runner() -> SessionRunner:
    fake = FakeModelSession()
    return SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="ui-evidence-test",
    )


def runner_app(
    runner: SessionRunner, repo: Path | None, *, token: str | None = TOKEN
) -> web.Application:
    """The production app with the production snapshotter and evidence workspace."""

    snapshotter = (
        None
        if repo is None
        else workspace_snapshot.workspace_snapshotter(
            repo,
            trusted_origin=GITHUB,
            repository_path=REPO,
            proposal=lambda: (TITLE, BODY),
        )
    )
    return create_app(runner, token=token, snapshotter=snapshotter, ui_evidence_workspace=repo)


@contextlib.asynccontextmanager
async def runner_http(repo: Path | None, *, token: str | None = TOKEN) -> AsyncIterator[TestClient]:
    runner = session_runner()
    await runner.start()
    async with TestClient(TestServer(runner_app(runner, repo, token=token))) as client:
        yield client


async def post_index(client: TestClient, base_sha: str, budget: float = 60) -> dict[str, Any]:
    response = await client.post(
        INDEX_ROUTE, headers=AUTH, json={"base_sha": base_sha, "budget_seconds": budget}
    )
    assert response.status == 200, await response.text()
    body = await response.json()
    assert isinstance(body, dict)
    return body


async def code_snapshot(client: TestClient, base_sha: str) -> tuple[bytes, list[str]]:
    response = await client.post("/v1/snapshot", headers=AUTH, json={"base_sha": base_sha})
    assert response.status == 200, await response.text()
    body = await response.json()
    return base64.b64decode(body["patch_base64"]), list(body["changed_paths"])


def assert_index_matches(actual: Mapping[str, Any], expected: Mapping[str, Any]) -> None:
    for key, value in expected.items():
        assert key in actual, key
        assert actual[key] == value, (key, actual[key], value)


def _assert_no_evidence_in(patch: bytes, changed_paths: list[str]) -> None:
    assert b".curie/ui-evidence" not in patch
    assert not any(path.startswith(".curie/ui-evidence") for path in changed_paths)


# --- Corpus cases --------------------------------------------------------------

_SERVABLE_REFUSALS = {"unbound", "total_limit"}


@pytest.mark.parametrize("case_id", [case["id"] for case in CORPUS["cases"] if "runner" in case])
def test_corpus_case_at_the_runner(case_id: str, tmp_path: Path) -> None:
    case = CASES[case_id]
    checkout = build_case_checkout(tmp_path / "case", case)
    expected = resolve(case["runner"], checkout)
    fault = case.get("fault") or {}
    transfer = fault.get("kind") == "transfer"

    async def go() -> tuple[tuple[bytes, list[str]], tuple[bytes, list[str]]]:
        async with runner_http(checkout.repo) as client:
            before = await code_snapshot(client, checkout.base)
            index = await post_index(client, checkout.base)
            assert_index_matches(index, expected)

            if transfer:
                # The real-path fault: the file changes after the index read it.
                (checkout.evidence / "after" / "home.png").write_bytes(png("rewritten"))

            for entry in index["images"]:
                sha = entry["sha256"] or "0" * 64
                response = await client.post(
                    IMAGE_ROUTE, headers=AUTH, json={"index": entry["ordinal"], "sha256": sha}
                )
                if entry["refusal"] is None:
                    if transfer and entry["ordinal"] == 1:
                        assert response.status == 409
                        assert (await response.json())["code"] == "image_changed"
                        continue
                    assert response.status == 200, await response.text()
                    assert await response.read() == checkout.originals[entry["path"]]
                elif entry["refusal"] not in _SERVABLE_REFUSALS:
                    assert response.status == 409, entry
                    refused = await response.json()
                    assert refused["code"] == "image_refused"
                    assert refused["refusal"] == entry["refusal"]

            missing = await client.post(
                IMAGE_ROUTE,
                headers=AUTH,
                json={"index": len(index["images"]), "sha256": "0" * 64},
            )
            assert missing.status == 404
            assert (await missing.json())["code"] == "no_such_image"

            after = await code_snapshot(client, checkout.base)
        return before, after

    before, after = anyio.run(go)

    # code_pr_unchanged: evidence never reaches the code publication.
    assert case["code_pr_unchanged"] is True
    assert before == after
    _assert_no_evidence_in(*before)

    shutil.move(str(checkout.evidence), str(tmp_path / "evidence-moved-away"))

    async def without_evidence() -> tuple[bytes, list[str]]:
        async with runner_http(checkout.repo) as client:
            return await code_snapshot(client, checkout.base)

    assert anyio.run(without_evidence) == before


# --- Image route ---------------------------------------------------------------


def test_image_route_serves_only_validated_unchanged_bytes(tmp_path: Path) -> None:
    checkout = build_case_checkout(tmp_path / "case", CASES["complete"])
    before_bytes = checkout.originals["before/home.png"]
    after_bytes = checkout.originals["after/home.png"]

    async def go() -> None:
        async with runner_http(checkout.repo) as client:
            index = await post_index(client, checkout.base)
            entries = index["images"]
            assert [entry["refusal"] for entry in entries] == [None, None]
            assert [entry["bound"] for entry in entries] == [True, True]

            # Liveness: each validated, unchanged image is served whole.
            for entry, data in zip(entries, (before_bytes, after_bytes), strict=True):
                response = await client.post(
                    IMAGE_ROUTE,
                    headers=AUTH,
                    json={"index": entry["ordinal"], "sha256": entry["sha256"]},
                )
                assert response.status == 200
                assert response.headers["Content-Type"].startswith("image/png")
                assert response.headers["X-Curie-Evidence-Sha256"] == sha256_hex(data)
                assert int(response.headers["Content-Length"]) == len(data)
                assert await response.read() == data

            # A digest that is not this entry's.
            wrong = await client.post(
                IMAGE_ROUTE, headers=AUTH, json={"index": 0, "sha256": sha256_hex(after_bytes)}
            )
            assert wrong.status == 409
            assert (await wrong.json())["code"] == "image_changed"

            # An index past the manifest.
            past = await client.post(
                IMAGE_ROUTE, headers=AUTH, json={"index": 2, "sha256": sha256_hex(before_bytes)}
            )
            assert past.status == 404
            assert (await past.json())["code"] == "no_such_image"

            # The file is replaced by a symbolic link to a valid PNG after indexing.
            after_path = checkout.evidence / "after" / "home.png"
            after_path.unlink()
            os.symlink("../before/home.png", after_path)
            linked = await client.post(
                IMAGE_ROUTE, headers=AUTH, json={"index": 1, "sha256": sha256_hex(after_bytes)}
            )
            assert linked.status == 409
            linked_body = await linked.json()
            assert (linked_body["code"], linked_body["refusal"]) == ("image_refused", "symlink")

            # The file is corrupted after indexing: never served, whichever check fires.
            (checkout.evidence / "before" / "home.png").write_bytes(png("before/home.png", "crc"))
            corrupt = await client.post(
                IMAGE_ROUTE, headers=AUTH, json={"index": 0, "sha256": sha256_hex(before_bytes)}
            )
            assert corrupt.status == 409
            body = await corrupt.json()
            assert body["code"] in {"image_refused", "image_changed"}
            if body["code"] == "image_refused":
                assert body["refusal"] == "corrupt_png"

            # The file is rewritten with another valid PNG after indexing.
            (checkout.evidence / "before" / "home.png").write_bytes(png("rewritten"))
            changed = await client.post(
                IMAGE_ROUTE, headers=AUTH, json={"index": 0, "sha256": sha256_hex(before_bytes)}
            )
            assert changed.status == 409
            assert (await changed.json())["code"] == "image_changed"

    anyio.run(go)


# --- Limits (each refusal has its liveness twin) -------------------------------


def _limit_case(images: list[tuple[str, Mapping[str, Any]]]) -> dict[str, Any]:
    return {
        "fixture": {
            "checkout": "dirty",
            "manifest": {
                "version": 1,
                "status": "complete",
                "images": [
                    {"path": path, "label": f"Shot {number}", "state": "before"}
                    for number, (path, _) in enumerate(images)
                ],
            },
            "manifest_raw": None,
            "base": "P",
            "head": None,
            "candidate_dirty": True,
            "tree": "C",
            "files": dict(images),
        }
    }


def test_an_image_of_exactly_10_mib_binds_and_is_served(tmp_path: Path) -> None:
    case = _limit_case([("before/limit.png", {"png": "valid", "size": MAX_IMAGE_BYTES})])
    checkout = build_case_checkout(tmp_path / "case", case)
    data = checkout.originals["before/limit.png"]
    assert len(data) == MAX_IMAGE_BYTES

    async def go() -> None:
        async with runner_http(checkout.repo) as client:
            index = await post_index(client, checkout.base)
            (entry,) = index["images"]
            assert entry["refusal"] is None
            assert entry["bound"] is True
            assert entry["size"] == MAX_IMAGE_BYTES
            response = await client.post(
                IMAGE_ROUTE, headers=AUTH, json={"index": 0, "sha256": entry["sha256"]}
            )
            assert response.status == 200
            assert await response.read() == data

    anyio.run(go)


@pytest.mark.parametrize(("extra", "admitted"), [(0, True), (1, False)])
def test_a_manifest_of_exactly_1_mib_is_read_and_one_byte_more_is_refused(
    tmp_path: Path, extra: int, admitted: bool
) -> None:
    checkout = build_case_checkout(tmp_path / "case", CASES["complete"])
    manifest = checkout.evidence / "manifest.json"
    document = manifest.read_bytes()
    # Trailing whitespace keeps the JSON valid at any length.
    manifest.write_bytes(document + b" " * (MAX_MANIFEST_BYTES + extra - len(document)))
    assert manifest.stat().st_size == MAX_MANIFEST_BYTES + extra

    async def go() -> dict[str, Any]:
        async with runner_http(checkout.repo) as client:
            return await post_index(client, checkout.base)

    index = anyio.run(go)
    if admitted:
        assert index["manifest"] == "valid"
        assert [entry["bound"] for entry in index["images"]] == [True, True]
    else:
        assert index["manifest"] == "invalid"
        assert index["manifest_refusal"] == "too_large"
        assert index["images"] == []


def test_total_evidence_admits_exactly_100_mib_and_refuses_the_next_image(
    tmp_path: Path,
) -> None:
    count = MAX_TOTAL_BYTES // MAX_IMAGE_BYTES
    assert count * MAX_IMAGE_BYTES == MAX_TOTAL_BYTES
    images: list[tuple[str, Mapping[str, Any]]] = [
        (f"before/full-{number:02d}.png", {"png": "valid", "size": MAX_IMAGE_BYTES})
        for number in range(count)
    ]
    images.append(("before/one-more.png", {"png": "valid"}))
    checkout = build_case_checkout(tmp_path / "case", _limit_case(images))

    async def go() -> dict[str, Any]:
        async with runner_http(checkout.repo) as client:
            return await post_index(client, checkout.base, budget=120)

    index = anyio.run(go)
    entries = index["images"]
    assert len(entries) == count + 1
    assert [entry["refusal"] for entry in entries[:count]] == [None] * count
    assert sum(entry["size"] for entry in entries[:count]) == MAX_TOTAL_BYTES
    last = entries[count]
    assert last["refusal"] == "total_limit"
    assert last["bound"] is True
    assert last["size"] == len(checkout.originals["before/one-more.png"])


# --- Route gating --------------------------------------------------------------


def _valid_bodies(checkout: CaseCheckout) -> dict[str, dict[str, object]]:
    return {
        INDEX_ROUTE: {"base_sha": checkout.base, "budget_seconds": 60},
        IMAGE_ROUTE: {"index": 0, "sha256": sha256_hex(checkout.originals["before/home.png"])},
    }


def test_evidence_routes_require_the_bearer(tmp_path: Path) -> None:
    checkout = build_case_checkout(tmp_path / "case", CASES["complete"])

    async def go() -> None:
        async with runner_http(checkout.repo) as client:
            for route, body in _valid_bodies(checkout).items():
                missing = await client.post(route, json=body)
                assert missing.status == 401, route
                wrong = await client.post(
                    route, headers={"Authorization": "Bearer not-the-token"}, json=body
                )
                assert wrong.status == 401, route
                other_scheme = await client.post(
                    route, headers={"Authorization": f"Token {TOKEN}"}, json=body
                )
                assert other_scheme.status == 401, route
                # Liveness: the configured bearer is admitted.
                admitted = await client.post(route, headers=AUTH, json=body)
                assert admitted.status == 200, (route, await admitted.text())

    anyio.run(go)


def test_executor_runner_refuses_evidence_routes(tmp_path: Path) -> None:
    checkout = build_case_checkout(tmp_path / "case", CASES["complete"])

    async def go() -> None:
        async with TestClient(TestServer(create_executor_app({}, TOKEN))) as client:
            for route, body in _valid_bodies(checkout).items():
                response = await client.post(route, headers=AUTH, json=body)
                assert response.status == 409, route
                assert (await response.json())["mode"] == EXECUTOR_MODE

    anyio.run(go)


def test_a_runner_without_a_managed_workspace_answers_no_managed_workspace(
    tmp_path: Path,
) -> None:
    checkout = build_case_checkout(tmp_path / "case", CASES["complete"])

    async def go() -> None:
        async with runner_http(None) as client:
            for route, body in _valid_bodies(checkout).items():
                response = await client.post(route, headers=AUTH, json=body)
                assert response.status == 409, route
                assert (await response.json())["code"] == "no_managed_workspace"

    anyio.run(go)


def test_evidence_request_bodies_are_strict(tmp_path: Path) -> None:
    checkout = build_case_checkout(tmp_path / "case", CASES["complete"])
    sha = sha256_hex(checkout.originals["before/home.png"])
    refused: list[tuple[str, object]] = [
        (INDEX_ROUTE, {"base_sha": checkout.base}),
        (INDEX_ROUTE, {"base_sha": checkout.base, "budget_seconds": 60, "extra": 1}),
        (INDEX_ROUTE, {"base_sha": checkout.base, "budget_seconds": 0}),
        (INDEX_ROUTE, {"base_sha": checkout.base, "budget_seconds": 121}),
        (INDEX_ROUTE, {"base_sha": checkout.base[:12], "budget_seconds": 60}),
        (INDEX_ROUTE, {"base_sha": checkout.base, "budget_seconds": "60"}),
        (INDEX_ROUTE, [checkout.base]),
        (IMAGE_ROUTE, {"index": 0}),
        (IMAGE_ROUTE, {"index": -1, "sha256": sha}),
        (IMAGE_ROUTE, {"index": "0", "sha256": sha}),
        (IMAGE_ROUTE, {"index": 0, "sha256": sha[:40]}),
        (IMAGE_ROUTE, {"index": 0, "sha256": sha, "path": "before/home.png"}),
    ]
    admitted: list[tuple[str, object]] = [
        (INDEX_ROUTE, {"base_sha": checkout.base, "budget_seconds": 1}),
        (INDEX_ROUTE, {"base_sha": checkout.base, "budget_seconds": 120}),
        (IMAGE_ROUTE, {"index": 0, "sha256": sha}),
    ]

    async def go() -> None:
        async with runner_http(checkout.repo) as client:
            not_json = await client.post(INDEX_ROUTE, headers=AUTH, data=b"{not json")
            assert not_json.status == 400
            assert (await not_json.json())["code"] == "invalid_request"
            for route, body in refused:
                response = await client.post(route, headers=AUTH, json=body)
                assert response.status == 400, (route, body)
                assert (await response.json())["code"] == "invalid_request"
            for route, body in admitted:
                response = await client.post(route, headers=AUTH, json=body)
                assert response.status == 200, (route, body, await response.text())

    anyio.run(go)


# --- The code publication path -------------------------------------------------


def test_evidence_never_enters_the_patch_or_changed_paths(tmp_path: Path) -> None:
    case = copy.deepcopy(CASES["complete"])
    # Evidence well past the patch cap must not count toward it.
    large = 2 * workspace_snapshot.MAX_PATCH_BYTES
    case["fixture"]["files"]["after/home.png"] = {"png": "valid", "size": large}
    checkout = build_case_checkout(tmp_path / "case", case)

    async def go() -> tuple[tuple[bytes, list[str]], tuple[bytes, list[str]], dict[str, Any]]:
        async with runner_http(checkout.repo) as client:
            first = await code_snapshot(client, checkout.base)
            index = await post_index(client, checkout.base)
            second = await code_snapshot(client, checkout.base)
            return first, second, index

    first, second, index = anyio.run(go)

    assert [entry["refusal"] for entry in index["images"]] == [None, None]
    assert index["images"][1]["size"] == large
    assert index["candidate_tree"] == checkout.candidate_tree
    patch, changed_paths = first
    assert sorted(changed_paths) == ["README.md", "src/app.py", "src/feature.py"]
    _assert_no_evidence_in(patch, changed_paths)
    assert len(patch) < workspace_snapshot.MAX_PATCH_BYTES
    assert second == first

    shutil.move(str(checkout.evidence), str(tmp_path / "evidence-moved-away"))

    async def without_evidence() -> tuple[bytes, list[str]]:
        async with runner_http(checkout.repo) as client:
            return await code_snapshot(client, checkout.base)

    assert anyio.run(without_evidence) == first
