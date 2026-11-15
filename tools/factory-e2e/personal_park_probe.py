"""Test-only publication stimulus for the two-owner driver's real park predicate."""

from __future__ import annotations

import json
import uuid
from typing import Any

import personal_profile as pp
import personal_profile_support as pps
import personal_two_owner as pt
from channel_protocol import scoped_conversation_id

RUNNER_PROGRAM = """
import os
os.environ["CURIE_FAKE_MODEL"] = "1"
from curie_runner import fake
from claude_agent_sdk import ToolUseBlock
def publication_turn(self):
    path = "/workspace/NONCE.md"
    content = "Unpaid publication correlation control.\\n"
    return [
        fake._assistant(ToolUseBlock(id="write-control", name="Write",
            input={"file_path": path, "content": content})),
        fake._FileWrite("write-control", path, content),
        fake._assistant(ToolUseBlock(id="publish-control",
            name="mcp__curie__publish_changes", input={
                "title": "Exercise publication correlation",
                "body": "Publish the unpaid fixture change for the approval control."
            })),
        fake._result(),
    ]
fake.FakeModelSession._default_script = publication_turn
from curie_runner.__main__ import main
main()
"""


class ParkProbe(pt.TwoOwnerRun):
    """Reuse the real install, admission, operator and cleanup without a paid start."""

    probe_run: pt.PodRun | None = None
    probe_until: float = 0.0

    def deadline(self) -> float:
        return self.probe_until or super().deadline()

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
            self.ledger.record("park-probe", "approval_status", approval.get("status"))
        return approval

    def phase_drill(self) -> None:
        super().phase_drill()
        if not self.no_grant_loaded() or self.owner_ledger.session_rows():
            raise pp.StepFailed("the unpaid probe requires markers and zero reservations")
        templates = self.kube.json("get", "sandboxtemplates.extensions.agents.x-k8s.io")
        changed = 0
        for template in templates.get("items") or []:
            spec = template.get("spec", {}).get("podTemplate", {}).get("spec", {})
            for container in spec.get("containers") or []:
                if container.get("name") == "runner":
                    container["command"] = ["python", "-c", RUNNER_PROGRAM]
                    changed += 1
            self.kube.ctl("replace", "-f", "-", stdin=json.dumps(template).encode())
        if not changed:
            raise pp.StepFailed("no owned runner template for the unpaid probe")
        run = self.new_run("a1", card=1)
        owner = self.owners["a"]
        status, body = self.admit_owner(owner, issue_owner=owner)
        if status != 200 or not isinstance(body, dict):
            raise pp.StepFailed("unpaid probe admission failed")
        run.work_item_id = str(uuid.UUID(str(body["work_item"]["id"])))
        run.request_id = str(uuid.UUID(str(body["request"]["id"])))
        run.sequence = body["request"]["sequence"]
        run.conversation_id = body["work_item"]["conversation_id"]
        self.attempts[run.number] = run
        self.probe_run = run
        self.probe_until = self.clock.now() + 180
        self.ledger.record("park-probe", "admission_fields", sorted(body["work_item"]))
        self.ledger.record("park-probe", "admitted_at", self.clock.now())
        self.work_item_of(run.work_item_id)
        self.wait_until(
            lambda: self.find_runner_pod(run), until=self.deadline(), what="unpaid runner"
        )
        self.check_runner_image(run)
        self.await_park(run)
        self.ledger.record("park-probe", "parked_at", self.clock.now())
        self.deliver_card(run)
        self.unlisted_control(run)
        self.resolve_card(run, "approved")
        publication = self.await_publication(run)
        branch = str(publication.get("branch") or "")
        number = publication.get("pr_number")
        if publication.get("status") != "succeeded" or type(number) is not int:
            raise pp.StepFailed("unpaid publication did not open its pull request")
        pull = self.require_forge().call("GET", self.require_forge().repo_path(f"/pulls/{number}"))
        head = str((pull.get("head") or {}).get("sha") or "")
        if not pps.is_commit_sha(head) or (pull.get("head") or {}).get("ref") != branch:
            raise pp.StepFailed("unpaid pull request head does not match its branch")
        self.published_heads[branch] = head
        self.ledger.record("park-probe", "pr_observed_at", self.clock.now())
        self.ledger.record("park-probe", "pr_opened", True)
        if self.owner_ledger.session_rows():
            raise pp.StepFailed("the unpaid probe reserved a paid session")
