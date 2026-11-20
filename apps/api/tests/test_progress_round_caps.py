"""A reported round on a phase two loops share, across the runner and the api (#5203).

Service free: no database fixture. The vector at
``tests/vectors/progress-round-caps.json`` says which (phase, round) a
declaration accepts. The api half validates every report with the production
``ProgressReport``. The boundary half drives the production runner tool at an
in-process aiohttp server whose handler runs the posted body through the same
production model, so neither component is faked.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import aiohttp
import anyio
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_api.factory_progress import Declaration, ProgressReport
from curie_runner.progress import (
    ProgressActivity,
    ProgressClient,
    build_progress_tool,
    load_phase_declaration,
)
from pydantic import ValidationError

VECTOR = Path(__file__).resolve().parents[3] / "tests" / "vectors" / "progress-round-caps.json"
TOKEN = "sbx.example-progress-token.signature"
_TOP_LEVEL_KEYS = frozenset({"comment", "cases"})
_CASE_KEYS = frozenset({"name", "declaration", "reports"})
_REPORT_KEYS = frozenset({"phase", "round", "accepted"})


def _cases() -> list[dict[str, Any]]:
    payload = json.loads(VECTOR.read_text(encoding="utf-8"))
    assert set(payload) == _TOP_LEVEL_KEYS, (
        f"{VECTOR} has unexpected top-level keys {sorted(set(payload) - _TOP_LEVEL_KEYS)} "
        f"and is missing {sorted(_TOP_LEVEL_KEYS - set(payload))}. A new key is rejected "
        "on purpose: one this reader cannot see would pass vacuously."
    )
    assert payload["comment"]
    cases = payload["cases"]
    assert cases
    for case in cases:
        assert set(case) == _CASE_KEYS, (
            f"case {case.get('name')!r} has unexpected keys "
            f"{sorted(set(case) - _CASE_KEYS)} and is missing {sorted(_CASE_KEYS - set(case))}."
        )
        assert case["reports"]
        for entry in case["reports"]:
            assert set(entry) == _REPORT_KEYS, (
                f"a report in case {case['name']!r} has unexpected keys "
                f"{sorted(set(entry) - _REPORT_KEYS)} and is missing "
                f"{sorted(_REPORT_KEYS - set(entry))}."
            )
    return cases


def _body(declaration: dict[str, Any], entry: dict[str, Any]) -> dict[str, Any]:
    body: dict[str, Any] = {"phase": entry["phase"], "declaration": declaration}
    if entry["round"] is not None:
        body["round"] = entry["round"]
    return body


def _where(case: dict[str, Any], entry: dict[str, Any]) -> str:
    return f"case {case['name']!r}, phase {entry['phase']}, round {entry['round']}"


def test_the_api_accepts_exactly_the_rounds_the_vector_accepts() -> None:
    for case in _cases():
        for entry in case["reports"]:
            body = _body(case["declaration"], entry)
            if entry["accepted"]:
                ProgressReport.model_validate(body)
            else:
                try:
                    ProgressReport.model_validate(body)
                except ValidationError:
                    continue
                raise AssertionError(f"{_where(case, entry)} was accepted by the api")


def test_loop_for_still_returns_the_first_loop_so_card_rendering_is_untouched() -> None:
    by_name = {case["name"]: case for case in _cases()}
    larger_first = Declaration.model_validate(
        by_name["shared start, larger cap declared first"]["declaration"]
    )
    smaller_first = Declaration.model_validate(
        by_name["shared start, smaller cap declared first"]["declaration"]
    )
    first = larger_first.loop_for("implement")
    second = smaller_first.loop_for("implement")
    assert first is not None
    assert second is not None
    assert (first.review, first.cap) == ("review_diff", 5)
    assert (second.review, second.cap) == ("wait_ci", 3)


async def _drive(case: dict[str, Any], bundle: Path) -> None:
    posted: list[dict[str, Any]] = []
    answered: list[int] = []

    async def report(request: web.Request) -> web.Response:
        body = await request.json()
        posted.append(body)
        try:
            ProgressReport.model_validate(body)
        except ValidationError:
            answered.append(422)
            return web.json_response({"code": "invalid"}, status=422)
        answered.append(201)
        return web.json_response({"recorded": True}, status=201)

    app = web.Application()
    app.router.add_post("/v1/work-item-progress/{request_id}", report)
    async with TestServer(app) as server:
        url = str(server.make_url("/v1/work-item-progress/example-request"))
        loaded = load_phase_declaration(bundle)
        assert loaded is not None
        tool = build_progress_tool(loaded, ProgressClient(url, TOKEN), ProgressActivity())
        for entry in case["reports"]:
            where = _where(case, entry)
            args: dict[str, Any] = {"phase": entry["phase"]}
            if entry["round"] is not None:
                args["round"] = entry["round"]
            before = len(posted)
            result = await tool.handler(args)
            if entry["accepted"]:
                assert result.get("is_error") is not True, f"{where} refused: {result}"
                assert len(posted) == before + 1, where
                assert answered[-1] == 201, where
            else:
                assert result.get("is_error") is True, f"{where} accepted: {result}"
                assert len(posted) == before, f"{where} was posted"
                # The api refuses it on its own, from the declaration the tool posts.
                assert await _post_raw(url, _body(loaded, entry)) == 422, where
                assert answered[-1] == 422, where


def test_the_runner_and_the_api_agree_on_every_round_across_the_http_boundary(
    tmp_path: Path,
) -> None:
    for index, case in enumerate(_cases()):
        bundle = tmp_path / f"case-{index}"
        (bundle / "progress").mkdir(parents=True)
        (bundle / "progress" / "phases.json").write_text(json.dumps(case["declaration"]))
        anyio.run(_drive, case, bundle)


async def _post_raw(url: str, body: dict[str, Any]) -> int:
    async with (
        aiohttp.ClientSession() as session,
        session.post(url, json=body, headers={"X-API-Key": TOKEN}) as response,
    ):
        return response.status
