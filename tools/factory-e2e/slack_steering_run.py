"""Drive native Slack guidance on both sides of a scripted factory publication."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import scripted_kind as sk
from scripted_scenario import SLACK_CHANNEL
from slack_steering_model import ANCHOR, INSTRUCTION, SteeringRouter

_INGRESS = Path(__file__).with_name("slack_steering_ingress.py")


def verify_revision(
    before: bytes,
    after: bytes,
    paths: bytes,
    nonce: str,
    initial_pr: tuple[int, str],
    current_pr: tuple[int, str],
) -> None:
    anchor = ANCHOR.encode()
    if initial_pr != current_pr:
        raise sk.ScriptedScenarioError("steering changed the pull request or branch")
    expected = before.replace(anchor, f"Run note {nonce}\n".encode() + anchor)
    if (
        before.count(anchor) != 1
        or nonce.encode() in before
        or paths.splitlines() != [b"README.md"]
        or expected != after
    ):
        raise sk.ScriptedScenarioError("steering did not produce the exact requested README edit")


def verify_consumers(
    pubs: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    stamps: list[str],
    url: str,
    first_revision: str,
) -> None:
    if (
        len(pubs) != 3
        or len(stamps) != 2
        or len({p["request"] for p in pubs}) != 3
        or len({p["lineage"] for p in pubs}) != 1
        or not pubs[0]["lineage"]
        or pubs[2]["prior_head"] != first_revision
        or any(p["url"] != url for p in pubs)
        or rows
        != [
            {"ts": stamp, "state": "applied", "request": pub["request"]}
            for stamp, pub in zip(stamps, pubs[1:], strict=True)
        ]
    ):
        raise sk.ScriptedScenarioError(
            "steering did not have exactly one consuming publication per reply"
        )


class SlackSteeringRun(sk.ScriptedRun):
    runs_one_ticket = False

    def __init__(self, args: argparse.Namespace) -> None:
        super().__init__(args)
        self.namespace = f"test-slack-steering-{self.run_id}"
        self.release = f"curie-{self.run_id}"
        if args.build_images:
            self.owned_build_tag = f"steering-{self.run_id}"
            self.runner_source = f"curie-runner:{self.owned_build_tag}"
            self.image_refs = tuple(
                f"{image.rsplit(':', 1)[0]}:{self.owned_build_tag}" for image in self.image_refs
            )
        self.guidance = SteeringRouter()
        self.advanced: set[str] = set()

    def gateway_enabled(self) -> bool:
        return True

    def extend_install_values(self, values: dict[str, Any]) -> None:
        if self.owned_build_tag:
            for component in ("api", "worker", "ui", sk.GATEWAY_COMPONENT):
                values[component]["image"]["tag"] = self.owned_build_tag

    def make_router(self, model_module: Any) -> SteeringRouter:
        return self.guidance

    def ingress(self, operation: str, body: dict[str, str] | None = None) -> Any:
        raw = self.kubectl(
            "-n",
            self.namespace,
            "exec",
            f"deployment/{self.release}-api",
            "--",
            "python",
            "-c",
            _INGRESS.read_text(),
            operation,
            json.dumps(body or {}),
        )
        return json.loads(raw)

    def wait_for(self, name: str, ready: Callable[[], Any]) -> Any:
        assert self.deadline is not None
        while time.monotonic() < self.deadline:
            if self.model.unexpected or self.stub.unknown_requests:
                raise sk.ScriptedScenarioError(
                    f"unexpected provider request while waiting for {name}"
                )
            if result := ready():
                return result
            time.sleep(1)
        raise sk.ScriptedScenarioError(f"timed out waiting for {name}")

    def root(self) -> tuple[str, str] | None:
        rows = self.sql(
            "SELECT coalesce(json_agg(json_build_array(t.work_item_id,t.root_ts)), '[]') "
            "FROM curie.factory_slack_public_threads t JOIN curie.work_items w "
            "ON w.id=t.work_item_id WHERE w.tracker_issue_id='3815' AND t.root_ts IS NOT NULL"
        )
        if not rows:
            return None
        assert self.slack is not None
        (row,) = rows
        item, ts = row
        posts = [
            p.ts
            for p in self.slack.posts()
            if p.channel == SLACK_CHANNEL and p.method == "chat.postMessage" and p.thread_ts is None
        ]
        if posts != [ts]:
            raise sk.ScriptedScenarioError("expected one posted public root for the WorkItem")
        return str(uuid.UUID(item)), ts

    def ledger(self, item: str) -> list[dict[str, Any]]:
        return self.sql(
            "SELECT coalesce(json_agg(json_build_object('ts', source_facts->>'message_ts',"
            "'state',state,'request',execution_request_id) ORDER BY created_at),'[]') "
            f"FROM curie.factory_steering_ledger WHERE work_item_id='{uuid.UUID(item)}'"
        )

    def publications(self, item: str) -> list[dict[str, Any]]:
        return self.sql(
            "SELECT coalesce(json_agg(json_build_object('id',p.id,'request',r.id,"
            "'status',p.status,'execution_status',r.status,'prior_head',p.expected_prior_head,"
            "'lineage',p.lineage_id,'url',p.result_url) ORDER BY r.sequence),'[]') "
            "FROM curie.publications p JOIN curie.execution_requests r "
            "ON r.id=p.execution_request_id "
            f"WHERE r.work_item_id='{uuid.UUID(item)}'"
        )

    def settled(self, item: str, count: int) -> list[dict[str, Any]] | None:
        pubs = self.publications(item)
        for pub in pubs:
            if pub["id"] not in self.advanced:
                self.stub.advance(1800)
                self.advanced.add(pub["id"])
            if pub["status"] in {"failed", "rejected", "abandoned"}:
                raise sk.ScriptedScenarioError("a fixture publication failed")
        if len(pubs) > count:
            raise sk.ScriptedScenarioError("steering produced a duplicate publication")
        if len(pubs) == count and all(
            p["status"] == "succeeded" and p["execution_status"] == "completed" for p in pubs
        ):
            return pubs
        return None

    def delivery(self, root: str, ts: str, nonce: str) -> dict[str, str]:
        assert self.slack is not None
        return {
            "slack": self.slack.base_url,
            "channel": SLACK_CHANNEL,
            "root": root,
            "bot_token": sk.SCRIPTED_SLACK_BOT_TOKEN,
            "event_id": "Ev" + uuid.uuid4().hex.upper(),
            "ts": ts,
            "text": INSTRUCTION.format(nonce),
        }

    def code_host(self) -> tuple[tuple[int, str], str, str]:
        pulls = self.stub._rows("pull")
        if len(pulls) != 1:
            raise sk.ScriptedScenarioError("expected exactly one pull request in the fixture")
        pull = self.stub._fresh_pull(pulls[0])
        return (int(pull["number"]), pull["head"]["ref"]), pull["head"]["sha"], pull["html_url"]

    def repo_bytes(self, *args: str) -> bytes:
        return self.stub._run("git", "--git-dir", str(self.stub._repository), *args)

    def run_publication(self, github: Any) -> None:
        self.bind_slack(public=True)
        person = self.ingress("seed")
        self.facts_api(
            "POST",
            f"/identity/admin/{person['tenant_id']}/code-host-links",
            {
                "principal_id": person["principal_id"],
                "code_host_kind": "github",
                "host": urlsplit(self.stub.base_url).netloc,
                "account_id": str(github.HUMAN["id"]),
                "created_by_principal_id": person["principal_id"],
            },
        )
        early, late = uuid.uuid4().hex, uuid.uuid4().hex
        self.secrets.extend((early, late))
        self.label_issue(github)
        try:
            item, ts = self.wait_for("the posted public root", self.root)
            self.wait_for("the initial runner request", self.guidance.initial_call.is_set)
            first = self.delivery(ts, "1800000500.000001", early)
            self.ingress("deliver", first)
            self.wait_for("prepublication acceptance", lambda: self.ledger(item))
            if self.ledger(item) != [
                {"ts": first["ts"], "state": "queued", "request": None}
            ] or self.stub._rows("pull"):
                raise sk.ScriptedScenarioError(
                    "early guidance was not queued before the pull request"
                )
        finally:
            self.guidance.release_initial.set()
        self.wait_for("the first pull request", lambda: self.stub._rows("pull"))
        original_pull, _, url = self.code_host()
        pubs = self.wait_for("the early same-branch revision", lambda: self.settled(item, 2))
        pull, head1, _ = self.code_host()
        initial = pubs[1]["prior_head"]
        before = self.repo_bytes("show", f"{initial}:README.md")
        after = self.repo_bytes("show", f"{head1}:README.md")
        if before != (sk.FIXTURE / "README.md").read_bytes():
            raise sk.ScriptedScenarioError(
                "the initial publication already changed the fixture README"
            )
        verify_revision(
            before,
            after,
            self.repo_bytes("diff", "--name-only", initial, head1),
            early,
            original_pull,
            pull,
        )

        second = self.delivery(ts, "1800000500.000002", late)
        self.ingress("deliver", second)
        self.ingress("deliver", second)
        pubs = self.wait_for("the postpublication revision", lambda: self.settled(item, 3))
        final_pull, head2, final_url = self.code_host()
        verify_revision(
            after,
            self.repo_bytes("show", f"{head2}:README.md"),
            self.repo_bytes("diff", "--name-only", head1, head2),
            late,
            original_pull,
            final_pull,
        )
        rows = self.ledger(item)
        verify_consumers(pubs, rows, [first["ts"], second["ts"]], url, head1)
        if self.guidance.received != [early, late] or final_url != url or self.root() != (item, ts):
            raise sk.ScriptedScenarioError(
                "runner inputs, consuming executions and published heads disagree"
            )
        self.evidence["slack_steering"] = {
            "work_item_id": item,
            "pull_request_number": original_pull[0],
            "publication_heads": [initial, head1, head2],
            "applied_execution_ids": [r["request"] for r in rows],
            "runner_input_sha256": [
                hashlib.sha256(INSTRUCTION.format(n).encode()).hexdigest()
                for n in self.guidance.received
            ],
        }
