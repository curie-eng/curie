"""Generate the tier-diff demo artifacts by running the real eval runner.

Why a generator rather than committed JSON: hand-written artifacts would prove
only that the differ can parse a file someone typed. These artifacts are produced
by ``curie_worker.eval.runner.EvalRunner`` talking real HTTP to a scripted runner,
so the graders, the sampling reduction, the result construction, and the
trajectory capture are all the production code paths. The single synthetic part
is the model's replies, which is what a fixture is for.

What is therefore PROVEN by these artifacts:
  * the runner records the tool-call trajectory it used to be discarding
  * a classified-failure turn carries its error text into the artifact
  * the artifact shape round-trips through the differ

What is NOT proven and must not be claimed: that a real Kubernetes cluster
produces this egress denial text, or that a real model takes these routes. The
per-tier ``provenance`` string on every artifact says so in the artifact itself,
so a reader who never opens this file still cannot mistake it for a cluster run.

Usage:
    uv run --directory apps/worker python ../../demo/tier-diff/generate.py
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from aci_protocol import ErrorEvent, Final, SessionStatus, ToolNote
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_worker.eval import EvalSuite
from curie_worker.eval.runner import EvalRunner
from curie_worker.eval.tierdiff import Observation, Tier, TierRun
from curie_worker.runner_client import RunnerClient

OUT_DIR = Path(__file__).resolve().parent / "artifacts"

PROVENANCE = (
    "scripted runner + real curie_worker.eval.runner "
    "(demo/tier-diff/generate.py); NOT a cluster run"
)

# The suite. Six ordinary back-office cases, deliberately generic: a demo suite
# that named a real customer's workflow would make the artifact undistributable.
SUITE = EvalSuite.model_validate(
    {
        "name": "backoffice",
        "cases": [
            {"id": "lookup_order_status", "input": "Where is order 8812?",
             "grader": {"kind": "contains", "expected": "shipped"}},
            {"id": "refund_within_policy", "input": "Refund order 8812, it arrived damaged.",
             "grader": {"kind": "contains", "expected": "refunded"}},
            {"id": "refund_over_limit", "input": "Refund order 9001 for 40000 dollars.",
             "grader": {"kind": "contains", "expected": "approval"}},
            {"id": "escalate_to_human", "input": "This is the third time my order is late.",
             "grader": {"kind": "contains", "expected": "escalated"}},
            {"id": "summarize_thread", "input": "Summarize this ticket thread for the handoff.",
             "grader": {"kind": "contains", "expected": "summary"}},
            {"id": "quarterly_report", "input": "Build the Q3 revenue report.",
             "grader": {"kind": "contains", "expected": "report"}},
            {"id": "sync_crm_record", "input": "Push the closed ticket back to the CRM.",
             "grader": {"kind": "contains", "expected": "synced"}},
        ],
    }
)


class ScriptedRunner:
    """A runner that answers each eval input from a per-case script.

    Modeled on the eval tests' fake runner, with one addition those tests do not
    need: a per-input error message. The attribution table matches on runner error
    TEXT, so a demo of attribution has to be able to drive that text; a fixed
    "boom" would only ever exercise the unclassified path.
    """

    def __init__(self, script: dict[str, dict[str, object]]) -> None:
        """Initialize the scripted runner.

        Args:
            script: Maps a case input to a dict with optional ``reply`` (str),
                ``tools`` (list of tool names emitted as tool_note frames), and
                ``error`` (str; when present the turn ends classified-failure).
        """
        self._script = script
        self.app = web.Application()
        self.app.add_routes(
            [
                web.post("/v1/event", self._event),
                web.post("/v1/reset", self._reset),
                web.get("/status", self._status),
            ]
        )

    async def _status(self, _request: web.Request) -> web.Response:
        return web.json_response({"status": "done", "turn_active": False})

    async def _reset(self, _request: web.Request) -> web.Response:
        return web.json_response({"ok": True})

    async def _event(self, request: web.Request) -> web.StreamResponse:
        """Stream this input's scripted tool notes, then its terminal frame."""
        body = await request.json()
        entry = self._script.get(body["text"], {})
        resp = web.StreamResponse(status=200, headers={"Content-Type": "application/x-ndjson"})
        await resp.prepare(request)
        for tool in entry.get("tools", []):  # type: ignore[union-attr]
            note = ToolNote(text=f"calling {tool}", tool=tool)
            await resp.write((note.model_dump_json() + "\n").encode("utf-8"))
        error = entry.get("error")
        if error:
            # An ErrorEvent then a classified-failure Final is the real path a
            # runner-side failure takes; the runner copies the classification or
            # message into EvalCaseResult.error, which attribution then reads.
            await resp.write((ErrorEvent(message=str(error)).model_dump_json() + "\n").encode("utf-8"))
            status = SessionStatus.CLASSIFIED_FAILURE
        else:
            status = SessionStatus.DONE
        final = Final(text=str(entry.get("reply", "")), status=status, input_tokens=900, output_tokens=120)
        await resp.write((final.model_dump_json() + "\n").encode("utf-8"))
        await resp.write_eof()
        return resp


# The deployed bundle's behavior, identical on all three tiers. Every case passes,
# which is the point: the baseline is a bundle somebody already shipped.
BASELINE_SCRIPT: dict[str, dict[str, object]] = {
    "Where is order 8812?": {
        "reply": "Order 8812 shipped on Tuesday.",
        "tools": ["lookup_order"],
    },
    "Refund order 8812, it arrived damaged.": {
        "reply": "Order 8812 has been refunded.",
        "tools": ["lookup_order", "refund"],
    },
    "Refund order 9001 for 40000 dollars.": {
        "reply": "That exceeds my limit, so it needs approval from a manager.",
        "tools": ["lookup_order", "request_approval"],
    },
    "This is the third time my order is late.": {
        "reply": "I have escalated this to a human agent.",
        # The deployed bundle issues a goodwill refund on the way to escalating.
        "tools": ["lookup_order", "refund", "escalate"],
    },
    "Summarize this ticket thread for the handoff.": {
        "reply": "Here is the summary of the thread.",
        "tools": ["read_thread"],
    },
    "Build the Q3 revenue report.": {
        "reply": "The Q3 revenue report is ready.",
        "tools": ["query_ledger", "build_report"],
    },
    "Push the closed ticket back to the CRM.": {
        "reply": "The ticket is synced to the CRM.",
        "tools": ["crm_write"],
    },
}

# The one behavior change the candidate bundle actually makes: it stops issuing the
# goodwill refund before escalating. The graded answer is UNCHANGED, so a pass-rate
# diff reports nothing at all here. This is the case that argues for trajectory.
_CANDIDATE_ROUTE_CHANGE = {
    "This is the third time my order is late.": {
        "reply": "I have escalated this to a human agent.",
        "tools": ["lookup_order", "escalate"],
    },
}

# Environment-only failures. Neither is a behavior change: the same bundle passes
# these cases on the lighter rungs. They exist only in the cluster's script, which
# is exactly what makes them findings about the environment and not the change.
_CLUSTER_ONLY_FAILURES = {
    "Build the Q3 revenue report.": {
        "reply": "",
        "tools": ["query_ledger"],
        # Matches the attribution table, so the report names the cause and prints
        # the remedy.
        "error": "egress denied to quickbooks.api.intuit.com by sandbox network policy",
    },
    "Push the closed ticket back to the CRM.": {
        "reply": "",
        "tools": ["crm_write"],
        # Deliberately matches NO signature. The report must say "unclassified"
        # here rather than reach for the nearest-looking cause, and this artifact
        # is what proves it does.
        "error": "unexpected end of stream from the sandbox supervisor",
    },
}

# The flaky case: the cluster fails to summarize on the third repeat only. Its
# classification therefore differs between repeats, so the report must fold it into
# a flake rate instead of announcing a behavior change.
_FLAKE_ON_THIRD_REPEAT = {
    "Summarize this ticket thread for the handoff.": {
        "reply": "",
        "tools": ["read_thread"],
        "error": "read_thread timed out after 30s",
    },
}


def candidate_script(tier: Tier, repeat: int) -> dict[str, dict[str, object]]:
    """Build the candidate bundle's script for one tier and one repeat.

    Args:
        tier: The ladder rung being scripted.
        repeat: 1-based repeat index, used only to inject the flake.

    Returns:
        The input-to-behavior script for that tier and repeat.
    """
    script = {**BASELINE_SCRIPT, **_CANDIDATE_ROUTE_CHANGE}
    if tier is Tier.CLUSTER:
        script.update(_CLUSTER_ONLY_FAILURES)
        if repeat == 3:
            script.update(_FLAKE_ON_THIRD_REPEAT)
    return script


async def _run(script: dict[str, dict[str, object]], version: str) -> object:
    """Run the whole suite once against a scripted runner and return the result.

    Args:
        script: The input-to-behavior script backing this run.
        version: The bundle version to stamp on the run result.

    Returns:
        The ``EvalRunResult`` the real eval runner produced.
    """
    runner = ScriptedRunner(script)
    server = TestServer(runner.app)
    await server.start_server()
    client = RunnerClient(total_timeout_s=30.0)
    try:
        return await EvalRunner(client).run(
            SUITE, base_url=f"http://127.0.0.1:{server.port}", version=version
        )
    finally:
        await client.close()
        await server.close()


async def _build(version: str, script_for: object) -> Observation:
    """Assemble one observation by running every tier's script.

    Args:
        version: The bundle version this observation describes.
        script_for: Callable taking a tier and returning that tier's script.

    Returns:
        The observation, with provenance stamped on every tier.
    """
    tiers = []
    for tier in (Tier.SKILL, Tier.LOCAL, Tier.CLUSTER):
        run = await _run(script_for(tier), version)  # type: ignore[operator]
        tiers.append(TierRun(tier=tier, provenance=f"{PROVENANCE}; tier={tier.value}", run=run))
    return Observation(version=version, tiers=tuple(tiers))


async def main() -> None:
    """Write the baseline artifact and three candidate repeats to ``artifacts/``."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    baseline = await _build("v0.7.0", lambda _tier: BASELINE_SCRIPT)
    _write(OUT_DIR / "deployed-v0.7.0.json", baseline)
    for repeat in (1, 2, 3):
        candidate = await _build("v0.7.1-rc.1", lambda tier, r=repeat: candidate_script(tier, r))
        _write(OUT_DIR / f"candidate-v0.7.1-rc.1-run{repeat}.json", candidate)


def _write(path: Path, observation: Observation) -> None:
    """Write one observation artifact as indented JSON and report it."""
    path.write_text(json.dumps(json.loads(observation.model_dump_json()), indent=2) + "\n")
    print(f"  wrote {path.name}")


if __name__ == "__main__":
    asyncio.run(main())
