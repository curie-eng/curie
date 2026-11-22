"""Test-only zero-cost lifecycle for the two-owner driver, on the fake model tier.

``rehearse --publish-probe`` replaces the owned runner template's command with
the runner on its fake model, which calls no model and loads no grant. Each
turn writes native-shaped transcripts (an implementer session and its plan and
diff reviewers, under a model name that is plainly fake), writes one file and
calls ``publish_changes``, so the product really parks the session on its
publication approval and suspends it. The driver's real paid phases then run
against that, in the live order, with the capture, settlement and judge code
unchanged, until the first phase the fake tier cannot reach. The ledger stays a
rehearsal (``mark_zero_cost``), so no live criterion can pass on it.
"""

from __future__ import annotations

import json
import traceback
from typing import Any

import personal_profile as pp
import personal_profile_support as pps
import personal_two_owner as pt
import personal_two_owner_support as pto
from channel_protocol import scoped_conversation_id

#: The model every fake transcript names; no live criterion accepts it.
FAKE_MODEL = "zero-cost-fake-model"

#: How long a fake turn waits for the driver's subagent limits before working.
LIMITS_WAIT_SECONDS = 180
#: Writes one turn's native-shaped transcripts under ``root`` (the runner's /tmp).
#: Shared by the runner program and the tests, which parse what it writes.
TRANSCRIPT_BUILDER = r"""
import datetime as _dt, json as _json, os as _os, uuid as _uuid
def write_native_turn(root, model):
    turn = _uuid.uuid4().hex[:12]
    project = _os.path.join(root, "curie-claude-config-zc" + turn, "projects", "-workspace")
    session = str(_uuid.uuid4())
    _os.makedirs(_os.path.join(project, session, "subagents"), exist_ok=True)
    def stamp():
        return _dt.datetime.now(_dt.timezone.utc).isoformat().replace("+00:00", "Z")
    def assistant(content, tokens, sidechain, agent=None):
        record = {"type": "assistant", "uuid": _uuid.uuid4().hex, "timestamp": stamp(),
                  "sessionId": session, "isSidechain": sidechain,
                  "message": {"id": "msg_zc_" + _uuid.uuid4().hex, "model": model,
                              "role": "assistant", "content": content,
                              "usage": {"input_tokens": 1, "output_tokens": tokens}}}
        if agent:
            record["agentId"] = agent
        return record
    def write(path, records):
        with open(path, "w") as handle:
            for record in records:
                handle.write(_json.dumps(record) + "\n")
    main = [{"type": "user", "timestamp": stamp(), "sessionId": session, "isSidechain": False,
             "message": {"role": "user", "content": "zero-cost lifecycle turn"}},
            assistant([{"type": "text", "text": "Planning."}], 2, False)]
    for role in ("plan-reviewer", "diff-reviewer"):
        tool = "toolu_zc_" + _uuid.uuid4().hex
        agent = "a" + _uuid.uuid4().hex[:16]
        main.append(assistant([{"type": "tool_use", "id": tool, "name": "Agent",
                                "input": {"subagent_type": "dark-factory:" + role,
                                          "prompt": "review"}}], 1, False))
        write(_os.path.join(project, session, "subagents", "agent-" + agent + ".jsonl"),
              [assistant([{"type": "text", "text": "VERDICT: APPROVE"}], 3, True, agent)])
        with open(_os.path.join(project, session, "subagents",
                                "agent-" + agent + ".meta.json"), "w") as handle:
            handle.write(_json.dumps({"toolUseId": tool}))
        main.append({"type": "user", "timestamp": stamp(), "sessionId": session,
                     "isSidechain": False,
                     "toolUseResult": {"agentId": agent, "status": "completed"},
                     "message": {"role": "user", "content": [
                         {"type": "tool_result", "tool_use_id": tool,
                          "content": "VERDICT: APPROVE"}]}})
    main.append(assistant([{"type": "tool_use", "id": "toolu_zc_" + _uuid.uuid4().hex,
                            "name": "mcp__curie__publish_changes", "input": {}}], 2, False))
    write(_os.path.join(project, session + ".jsonl"), main)
"""

RUNNER_PROGRAM = (
    """
import os
os.environ["CURIE_FAKE_MODEL"] = "1"
from curie_runner import fake
from claude_agent_sdk import ToolUseBlock
"""
    + TRANSCRIPT_BUILDER
    + f"""
def publication_turn(self):
    # A native prompt is held until the driver opened capture and wrote the
    # subagent limits; the fake has no hook, so it waits for those limits.
    import time
    limits = os.path.join(os.environ.get("PTO_GATE_DIR") or {pto.GATE_DIR!r},
                          {pto.GATE_LIMITS_FILE!r})
    deadline = time.monotonic() + {LIMITS_WAIT_SECONDS}
    while not os.path.exists(limits) and time.monotonic() < deadline:
        time.sleep(0.2)
    write_native_turn("/tmp", {FAKE_MODEL!r})
    path = "/workspace/NONCE.md"
    content = "Unpaid publication correlation control.\\n"
    return [
        fake._assistant(ToolUseBlock(id="write-control", name="Write",
            input={{"file_path": path, "content": content}})),
        fake._FileWrite("write-control", path, content),
        fake._assistant(ToolUseBlock(id="publish-control",
            name="mcp__curie__publish_changes", input={{
                "title": "Exercise publication correlation",
                "body": "Publish the unpaid fixture change for the approval control."
            }})),
        fake._result(),
    ]
fake.FakeModelSession._default_script = publication_turn
from curie_runner.__main__ import main
main()
"""
)

#: The paid phases the fake tier is driven through, in the live order. The
#: probes need a real grant and model, so each is reserved and settled unstarted.
PROBES = {"probe-a": "a-probe", "probe-b": "b-probe"}
#: The zero-cost lifecycle's one phase name, after the drill (never a paid phase).
PHASE = "zero-cost-lifecycle"


class ParkProbe(pt.TwoOwnerRun):
    """Reuse the real install, admission, operator, capture and cleanup without a model."""

    probe_run: pt.PodRun | None = None
    fake_runner = False

    def __init__(self, **kwargs: Any) -> None:
        if kwargs.get("stop_after") is None:
            kwargs["stop_after"] = PHASE
        super().__init__(**kwargs)

    def phases(self) -> list[tuple[str, Any]]:
        unpaid = [(name, phase) for name, phase in super().phases() if name in pt.UNPAID_PHASES]
        return [*unpaid, (PHASE, self.phase_zero_cost)]

    def require_live(self, name: str) -> None:
        """The paid phase methods run only on the fake runner with no grant loaded."""

        if not (self.fake_runner and self.no_grant_loaded() and self.owner_ledger.is_zero_cost()):
            raise pps.Refusal("rehearsal_run_id", f"{name} is a paid phase")

    def require(self, checks: Any, what: str) -> None:
        """Record a phase's failed checks and go on: the fake tier cannot meet every one.

        The checks stay in the ledger for the judge, which fails them. Capture,
        settlement and every other ``fail_paid`` still fence the run.
        """

        self.check_paid()
        failed = sorted(key for key, value in checks.items() if value is not True)
        if failed:
            unmet = self.ledger.observations("zero-cost").get("unmet", [])
            self.ledger.record("zero-cost", "unmet", [*unmet, {"what": what, "failed": failed}])

    def work_item_of(self, work_item_id: str) -> dict[str, Any]:
        item = super().work_item_of(work_item_id)
        if self.probe_run is not None and not self.watch_stop.is_set():
            states = self.ledger.observations("park-probe").get("states", [])
            state = {"state": item.get("state"), "at": self.clock.now()}
            if not states or states[-1]["state"] != state["state"]:
                self.ledger.record("park-probe", "states", [*states, state])
            self.ledger.record("park-probe", "work_item_fields", sorted(item))
            self.ledger.record(
                "park-probe",
                "request_fields",
                sorted((item.get("requests") or [{}])[-1]),
            )
        return item

    def approval(self, approval_id: str) -> dict[str, Any]:
        approval = super().approval(approval_id)
        run = self.probe_run
        if run is not None and not self.watch_stop.is_set():
            self.ledger.record("park-probe", "approval_fields", sorted(approval))
            self.ledger.record(
                "park-probe",
                "conversation_equal",
                approval.get("conversation_id") == run.conversation_id,
            )
            self.ledger.record(
                "park-probe",
                "route_conversation_equal",
                scoped_conversation_id(
                    "github",
                    str(approval.get("reply_channel") or ""),
                    str(approval.get("conversation_id") or ""),
                )
                == run.conversation_id,
            )
        return approval

    def new_run(self, group: str, **kwargs: Any) -> pt.PodRun:
        run = super().new_run(group, **kwargs)
        if group == "a1":
            self.probe_run = run
        return run

    def use_fake_runner(self) -> None:
        templates = self.kube.json("get", "sandboxtemplates.extensions.agents.x-k8s.io")
        changed = 0
        for template in templates.get("items") or []:
            spec = template.get("spec", {}).get("podTemplate", {}).get("spec", {})
            for container in spec.get("containers") or []:
                if container.get("name") == pps.RUNNER_CONTAINER:
                    container["command"] = ["python", "-c", RUNNER_PROGRAM]
                    changed += 1
            self.kube.ctl("replace", "-f", "-", stdin=json.dumps(template).encode())
        if not changed:
            raise pp.StepFailed("no owned runner template for the zero-cost lifecycle")
        self.fake_runner = True

    def begin_zero_cost(self) -> None:
        """The fake runner, a zero-cost ledger and the live clocks; never a grant."""

        if not self.no_grant_loaded() or self.owner_ledger.session_rows():
            raise pp.StepFailed("the zero-cost lifecycle requires markers and zero reservations")
        self.use_fake_runner()
        now = self.clock.now()
        self.owner_ledger.mark_zero_cost()
        for owner in pto.OWNERS:
            self.owner_ledger.set_owner_window(owner, now + pto.LIVE_SECONDS + pto.CLEANUP_SECONDS)
        self.ledger.begin_live(now=now, grant_expires_at=now + pto.LIVE_SECONDS)
        self.ledger.record("budget", "allowance_breach", [])
        self.start_watchdog()

    def unstarted_probe(self, group: str) -> None:
        """A probe needs a real grant and model: reserved in order, settled not started."""

        self.check_paid()
        now = self.clock.now()
        for item in self.owner_ledger.reserve_group(group, now=now):
            self.ledger.settle(item.seq, pps.SessionUsage(0, 0, 0.0, ""), source="not_started")
        self.reserved_at[group] = now

    def phase_zero_cost(self) -> None:
        self.begin_zero_cost()
        methods = dict(self.paid_phases())
        reached: list[str] = []
        stopped: dict[str, Any] | None = None
        for name in pt.PAID_PHASES:
            started = self.clock.now()
            try:
                if name in PROBES:
                    self.unstarted_probe(PROBES[name])
                else:
                    methods[name]()
            except Exception as exc:  # noqa: BLE001 - the first unreachable phase ends the walk
                frame = traceback.extract_tb(exc.__traceback__)[-1]
                stopped = {
                    "phase": name,
                    "error_class": type(exc).__name__,
                    "message": self.scrubber.scrub(str(exc))[:300],
                    "at": f"{frame.name}:{frame.lineno}",
                    "elapsed": round(self.clock.now() - started, 3),
                }
                break
            reached.append(name)
            self.ledger.record("zero-cost", "reached", list(reached))
        self.ledger.record("zero-cost", "reached", reached)
        self.ledger.record("zero-cost", "stopped", stopped)
        self.ledger.record("zero-cost", "pod_runs", len(self.pods))
        if self.owner_ledger.session_rows() and any(
            row["model"] not in (None, "", FAKE_MODEL) for row in self.owner_ledger.session_rows()
        ):
            raise pp.StepFailed("a zero-cost session settled on a model other than the fake")
