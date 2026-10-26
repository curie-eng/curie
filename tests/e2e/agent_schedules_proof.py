"""End to end proof of agent-created schedules (ADR 0248) on a running local stack.

Drives a real model through a persistent HTTP test channel
(``tests/e2e/http_test_channel.py``) and records every observation in one JSON
evidence file, so a later ``verify`` can check each acceptance criterion
without rerunning the paid turns.

    run     set up the fixture agent and run the selected proofs
    verify  exit 0 only when the evidence file records that proof as passed

The stack, its API key and the advertised listener address are arguments; this
module names no host. Model spend is bounded by the number of turns the proofs
send plus the scheduled runs they wait for.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))

from http_test_channel import (  # noqa: E402
    TestChannelServer,
    bind_channel,
    send_turn,
    utc_now,
)

ADAPTER = "schedule-test"
ZONE = "America/New_York"
SLOT_DEADLINE_S = 150
BUNDLE = {
    "name": "schedule-proof",
    "version": "0.1.0",
    "description": "A plain assistant used to prove agent-created schedules end to end.",
    "systemPrompt": (
        "You are a concise assistant. When asked to schedule, change, list or cancel "
        "recurring or one-time work, use your schedule tools and then state the stored "
        "schedule back exactly as the tool returned it, including its time zone. Keep "
        "every reply short. When a prompt asks you to call a tool even if you expect a "
        "refusal, call it and quote its result: the platform enforces its own rules, and "
        "the test needs the tool's answer."
    ),
}


class ProofFailed(AssertionError):
    pass


def check(condition: bool, message: str) -> None:
    if not condition:
        raise ProofFailed(message)


class Api:
    def __init__(self, base: str, key: str) -> None:
        self.base = base.rstrip("/")
        self.key = key

    def call(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(
            f"{self.base}{path}",
            data=data,
            method=method,
            headers={"X-API-Key": self.key, "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
                return resp.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                return exc.code, json.loads(raw)
            except ValueError:
                return exc.code, raw.decode(errors="replace")

    def ok(self, method: str, path: str, body: Any = None) -> Any:
        status, payload = self.call(method, path, body)
        check(200 <= status < 300, f"{method} {path} answered {status}: {payload}")
        return payload


class Proof:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.api = Api(args.api_url, Path(args.api_key_file).read_text().strip())
        self.db = str(Path(args.evidence).with_suffix(".deliveries.sqlite"))
        self.evidence_path = Path(args.evidence)
        self.evidence: dict[str, Any] = (
            json.loads(self.evidence_path.read_text()) if self.evidence_path.exists() else {}
        )
        self.evidence.setdefault("proofs", {})
        self.address = f"schedule-proof-{args.agent}"
        self.agent_id = ""
        self.log: list[dict[str, Any]] = []

    # --- recording -----------------------------------------------------------

    def note(self, what: str, **values: Any) -> None:
        entry = {"at": utc_now(), "what": what, **values}
        self.log.append(entry)
        print(json.dumps(entry, default=str), flush=True)

    def save(self) -> None:
        self.evidence_path.parent.mkdir(parents=True, exist_ok=True)
        self.evidence_path.write_text(json.dumps(self.evidence, indent=2, default=str) + "\n")

    def record(self, number: int, passed: bool, detail: str) -> None:
        self.evidence["proofs"][str(number)] = {
            "passed": passed,
            "detail": detail,
            "recorded_at": utc_now(),
            "agent": self.args.agent,
            "model": self.args.model,
            "head": self.args.head,
            "log": self.log,
        }
        self.save()
        self.log = []

    # --- fixture -------------------------------------------------------------

    def find_agent(self) -> dict[str, Any] | None:
        for agent in self.api.ok("GET", "/agents"):
            if agent["name"] == self.args.agent:
                return dict(agent)
        return None

    def write_bundle(self) -> Path:
        bundle = self.evidence_path.parent / "bundle"
        manifest = bundle / ".claude-plugin" / "plugin.json"
        manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest.write_text(json.dumps(BUNDLE, indent=2) + "\n")
        return bundle

    def setup(self) -> None:
        agent = self.find_agent()
        if agent is None:
            subprocess.run(
                [
                    self.args.curie_bin,
                    "local",
                    "deploy",
                    "--agent",
                    self.args.agent,
                    "--plugin-dir",
                    str(self.write_bundle()),
                    "--slack-channel",
                    self.args.slack_channel,
                    "--api-url",
                    self.args.api_url,
                ],
                check=False,
            )
            agent = self.find_agent()
            check(agent is not None, f"deploy did not create {self.args.agent}")
        assert agent is not None
        self.agent_id = str(agent["id"])
        endpoint = f"http://{self.args.advertise_host}:{self.args.port}/"
        bind_channel(
            self.args.api_url, self.api.key, self.agent_id, self.address, endpoint, ADAPTER
        )
        self.patch_agent(
            {"agent_schedules": True, "schedule_timezone": ZONE, "model": self.args.model}
        )
        self.note("fixture", agent_id=self.agent_id, address=self.address, endpoint=endpoint)

    def patch_agent(self, body: dict[str, Any]) -> dict[str, Any]:
        agent: dict[str, Any] = self.api.ok("PATCH", f"/agents/{self.agent_id}", body)
        self.note("patch_agent", body=body)
        return agent

    def schedules(self, include_all: bool = False) -> list[dict[str, Any]]:
        suffix = "?all=true" if include_all else ""
        payload = self.api.ok("GET", f"/agents/{self.agent_id}/schedules{suffix}")
        return list(payload["schedules"])

    def show(self, schedule_id: str) -> dict[str, Any]:
        result: dict[str, Any] = self.api.ok(
            "GET", f"/agents/{self.agent_id}/schedules/{schedule_id}"
        )
        return result

    # --- channel -------------------------------------------------------------

    def chat(self, text: str, timeout: float = 240) -> dict[str, Any]:
        since = utc_now()
        conversation = f"proof-{uuid.uuid4().hex[:10]}"
        sent = send_turn(
            self.args.api_url,
            self.api.key,
            self.agent_id,
            self.address,
            ADAPTER,
            text,
            conversation_id=conversation,
            author=self.args.author,
        )
        self.note("chat_sent", text=text, conversation=conversation, event_id=sent["event_id"])
        reply = self.wait_reply(conversation, since, timeout)
        self.note("chat_reply", conversation=conversation, text=reply.get("text"))
        return {"conversation": conversation, "since": since, "reply": reply}

    def deliveries(self, since: str, contains: str | None = None) -> list[dict[str, Any]]:
        import sqlite3

        conn = sqlite3.connect(self.db)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT received_at, event, conversation_id, text, raw FROM deliveries "
                "WHERE received_at >= ? ORDER BY received_at",
                (since,),
            ).fetchall()
        finally:
            conn.close()
        found = [dict(r) for r in rows]
        if contains is not None:
            found = [r for r in found if contains in (r["text"] or "")]
        return found

    def wait_reply(self, conversation: str, since: str, timeout: float) -> dict[str, Any]:
        """The turn's last text once ``turn.completed`` arrives in ``conversation``."""

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            rows = [r for r in self.deliveries(since) if r["conversation_id"] == conversation]
            done = [r for r in rows if r["event"] == "turn.completed"]
            if done:
                outcome = json.loads(done[-1]["raw"]).get("outcome")
                check(outcome == "delivered", f"the turn in {conversation} ended {outcome}")
                texts = [r for r in rows if r["text"]]
                return dict(texts[-1]) if texts else {"text": ""}
            time.sleep(2)
        seen = [r for r in self.deliveries(since) if r["conversation_id"] == conversation]
        raise ProofFailed(f"no completed reply in {conversation} within {timeout}s; saw {seen}")

    def scheduled_conversation(self, schedule_id: str) -> str:
        """The conversation every run of one schedule posts in (shared by its slots)."""

        return f"hook:{self.agent_id}:agent-schedule:{schedule_id}"

    def completions(self, schedule_id: str) -> list[dict[str, Any]]:
        """One entry per completed scheduled turn of ``schedule_id``: its slot, read
        from the ``turn.completed`` event id ``cron:<agent>:<name>:<slot>``, and
        when the channel received it. Two runs of one slot are two entries."""

        prefix = f"cron:{self.agent_id}:agent-schedule:{schedule_id}:"
        conv = self.scheduled_conversation(schedule_id)
        found = []
        for row in self.deliveries("0"):
            if row["conversation_id"] != conv or row["event"] != "turn.completed":
                continue
            event_id = json.loads(row["raw"]).get("event_id") or ""
            check(event_id.startswith(prefix), f"unexpected event id {event_id!r} in {conv}")
            slot = _parse(event_id[len(prefix) :])
            found.append(
                {
                    "slot": slot,
                    "received_at": row["received_at"],
                    "event_id": event_id,
                    "outcome": json.loads(row["raw"]).get("outcome"),
                }
            )
        return found

    def delivered_with(self, schedule_id: str, run: dict[str, Any], marker: str) -> bool:
        """Whether the run that completed as ``run`` delivered a reply carrying
        ``marker``: a text in the schedule's conversation after the run's slot and
        no later than its ``turn.completed``."""

        conv = self.scheduled_conversation(schedule_id)
        return any(
            row["conversation_id"] == conv
            and marker in (row["text"] or "")
            and row["received_at"] <= run["received_at"]
            for row in self.deliveries(run["slot"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
        )

    def runner_logs(self, needle: str) -> str:
        """The logs of every runner container whose log mentions ``needle``."""

        names = subprocess.run(
            ["docker", "ps", "-a", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=True,
            timeout=60,
        ).stdout.split()
        found = []
        for name in names:
            if not name.startswith("curie-thread-"):
                continue
            logs = subprocess.run(
                ["docker", "logs", name], capture_output=True, text=True, timeout=60
            )
            text = logs.stdout + logs.stderr
            if needle in text:
                found.append(text)
        return "\n".join(found)

    def find(self, marker: str) -> list[dict[str, Any]]:
        return [s for s in self.schedules(include_all=True) if marker in s["prompt"]]

    def mounted(self, conversation: str) -> bool | None:
        """Whether the runner that served ``conversation`` logged the schedule tool
        mount. None when no runner log names the conversation."""

        text = self.runner_logs(f"{conversation} model=")
        if not text:
            return None
        return any(
            "agent schedules tools mounted" in line and conversation in line
            for line in text.splitlines()
        )


def nonce() -> str:
    return uuid.uuid4().hex[:8].upper()


def sleep_until(when: datetime) -> None:
    delay = (when - datetime.now(UTC)).total_seconds()
    if delay > 0:
        time.sleep(delay)


def next_minute(after: datetime) -> datetime:
    return after.replace(second=0, microsecond=0) + timedelta(minutes=1)


def proof_1(p: Proof) -> str:
    marker = f"STANDUP-{nonce()}"
    chat = p.chat(
        "Please set up a recurring schedule for every weekday at 8:00 AM. Its prompt "
        f"should be exactly: Post the word {marker} as a standup reminder."
    )
    rows = p.find(marker)
    check(len(rows) == 1, f"expected one stored schedule carrying {marker}, found {rows}")
    row = rows[0]
    p.note("stored", schedule=row)
    check(row["state"] == "active", f"state is {row['state']}")
    check(row["timezone"] == ZONE, f"timezone {row['timezone']} is not the agent zone {ZONE}")
    check(row["created_source"] == "agent", f"created_source is {row['created_source']}")
    check(row["created_by"] == p.args.author, f"created_by is {row['created_by']}")
    fields = row["cron"].split()
    check(
        len(fields) == 5 and fields[0] == "0" and fields[1] == "8" and fields[4] == "1-5",
        f"cron {row['cron']} is not weekdays at 08:00",
    )
    local = _parse(row["next_run_at"]).astimezone(ZoneInfo(ZONE))
    check(local.hour == 8 and local.minute == 0, f"next run {local} is not 08:00 {ZONE}")
    reply = chat["reply"].get("text") or ""
    check(
        "8" in reply and any(z in reply for z in ("America/New_York", "New York", "Eastern")),
        f"reply does not read back the time and zone: {reply!r}",
    )
    p.evidence["proof1_schedule"] = row["id"]
    return f"stored {row['id']} cron {row['cron']} {row['timezone']}; next {local.isoformat()}"


def proof_2(p: Proof) -> str:
    p.patch_agent({"schedule_min_interval_minutes": 1})
    listed = p.api.ok("GET", f"/agents/{p.agent_id}/schedules")
    check(listed["min_interval_minutes"] == 1, f"minimum reads back {listed}")
    marker = f"PING-{nonce()}"
    p.chat(
        "Create a schedule that runs every minute. Its prompt should be exactly: "
        f"Reply with the single word {marker}"
    )
    rows = p.find(marker)
    check(len(rows) == 1, f"expected one every-minute schedule, found {rows}")
    row = rows[0]
    p.note("stored", schedule=row)
    check(row["cron"].split()[:2] == ["*", "*"], f"cron {row['cron']} is not every minute")
    slot = _parse(listed_next(p, row["id"]))
    p.note("waiting", slot=slot.isoformat())
    deadline = slot + timedelta(seconds=SLOT_DEADLINE_S)
    while datetime.now(UTC) < deadline:
        runs = [c for c in p.completions(row["id"]) if c["slot"] == slot]
        if runs:
            check(len(runs) == 1, f"slot {slot} completed {len(runs)} times: {runs}")
            check(runs[0]["outcome"] == "delivered", f"slot {slot} ended {runs[0]['outcome']}")
            check(
                p.delivered_with(row["id"], runs[0], marker),
                f"slot {slot} completed without delivering {marker}",
            )
            lag = (_parse(runs[0]["received_at"]) - slot).total_seconds()
            p.note("delivered", run=runs[0], lag_s=lag)
            check(0 <= lag <= SLOT_DEADLINE_S, f"slot {slot} completed {lag}s after the slot")
            p.evidence["proof2_schedule"] = row["id"]
            p.evidence["proof2_marker"] = marker
            return (
                f"slot {slot.isoformat()} completed at {runs[0]['received_at']}, "
                f"{lag:.1f}s after the slot ({runs[0]['event_id']})"
            )
        time.sleep(3)
    raise ProofFailed(f"slot {slot} did not complete within {SLOT_DEADLINE_S}s")


def listed_next(p: Proof, schedule_id: str) -> str:
    row = p.show(schedule_id)
    check(row["next_run_at"] is not None, f"schedule has no next run: {row}")
    return str(row["next_run_at"])


def proof_3(p: Proof) -> str:
    marker = f"ONESHOT-{nonce()}"
    at = next_minute(datetime.now(UTC) + timedelta(minutes=2)).astimezone(ZoneInfo(ZONE))
    p.chat(
        f"Schedule a one-time task for {at:%Y-%m-%d} at {at:%H:%M} ({ZONE}). Its prompt "
        f"should be exactly: Reply with the single word {marker}"
    )
    rows = p.find(marker)
    check(len(rows) == 1, f"expected one one-shot, found {rows}")
    row = rows[0]
    p.note("stored", schedule=row)
    check(row["cron"] is None and row["run_at"], f"not a one-shot: {row}")
    run_at = _parse(row["run_at"])
    sleep_until(run_at + timedelta(seconds=SLOT_DEADLINE_S))
    runs = p.completions(row["id"])
    final = p.show(row["id"])
    revisions = p.api.ok("GET", f"/agents/{p.agent_id}/schedules/{row['id']}/revisions")
    completes = [r for r in revisions["revisions"] if r["action"] == "complete"]
    p.note("after", runs=runs, schedule=final, completes=len(completes))
    check(len(runs) == 1, f"expected exactly one completed run, saw {runs}")
    slot_gap = abs((runs[0]["slot"] - run_at).total_seconds())
    check(slot_gap < 1, f"the run's slot {runs[0]['slot']} is not run_at {run_at}")
    check(runs[0]["outcome"] == "delivered", f"the one-shot ended {runs[0]['outcome']}")
    check(
        p.delivered_with(row["id"], runs[0], marker),
        f"the one-shot completed without delivering {marker}",
    )
    lag = (_parse(runs[0]["received_at"]) - run_at).total_seconds()
    check(0 <= lag <= SLOT_DEADLINE_S, f"completed {lag}s after run_at")
    check(final["state"] == "done", f"state is {final['state']}")
    check(len(completes) == 1, f"expected one complete revision, saw {len(completes)}")
    return (
        f"one-shot {row['id']} at {run_at.isoformat()} completed once "
        f"({runs[0]['event_id']}, {lag:.1f}s), done"
    )


def proof_4(p: Proof) -> str:
    schedule_id = p.evidence.get("proof2_schedule")
    marker = p.evidence.get("proof2_marker")
    check(bool(schedule_id and marker), "proof 2 has not stored its every-minute schedule")
    # Cancel right after a slot completes, so no run is in flight and the
    # silence window below needs no allowance for earlier slots.
    seen = len(p.completions(schedule_id))
    deadline = time.monotonic() + 3 * 60
    while len(p.completions(schedule_id)) == seen:
        check(time.monotonic() < deadline, "no slot completed before the cancel")
        time.sleep(1)
    p.chat(f"Please cancel the every-minute schedule whose prompt mentions {marker}.")
    row = p.show(schedule_id)
    p.note("after_cancel", schedule=row)
    check(row["state"] == "cancelled", f"state is {row['state']}")
    cancelled_at = _parse(row["updated_at"])
    former_next = next_minute(cancelled_at)
    window_end = former_next + timedelta(seconds=SLOT_DEADLINE_S)
    sleep_until(window_end)
    late = [r for r in p.completions(schedule_id) if r["slot"] >= former_next]
    conv = p.scheduled_conversation(schedule_id)
    stray = [
        r
        for r in p.deliveries(former_next.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
        if r["conversation_id"] == conv and _parse(r["received_at"]) <= window_end
    ]
    final = p.show(schedule_id)
    p.note(
        "quiet",
        former_next=former_next.isoformat(),
        late=late,
        stray=[{k: r[k] for k in ("received_at", "event")} for r in stray],
        schedule=final,
    )
    check(not late, f"turns ran for slots at or after the former next slot: {late}")
    check(not stray, f"deliveries within {SLOT_DEADLINE_S}s after the former next slot: {stray}")
    last = final.get("last_fire_at")
    check(
        last is None or _parse(last) < former_next,
        f"fired at {last}, after the former next slot {former_next}",
    )
    return (
        f"cancelled at {cancelled_at.isoformat()}; no turn and no delivery from "
        f"{former_next.isoformat()} through {window_end.isoformat()}"
    )


def proof_5(p: Proof) -> str:
    p.patch_agent({"schedule_min_interval_minutes": 15})
    listed = p.api.ok("GET", f"/agents/{p.agent_id}/schedules")
    check(listed["min_interval_minutes"] == 15, f"minimum reads back {listed}")
    marker = f"TEN-{nonce()}"
    chat = p.chat(
        "Create a schedule that runs every 10 minutes. Its prompt should be exactly: "
        f"Reply with the single word {marker}"
    )
    check(not p.find(marker), f"a 10 minute schedule was stored: {p.find(marker)}")
    refusals = [
        line
        for line in p.runner_logs(chat["conversation"]).splitlines()
        if "agent schedule write refused by the API tool=create_schedule status=422" in line
        and "scheduled runs must be at least 15 minutes apart" in line
    ]
    p.note("refused_chat", reply=chat["reply"].get("text"), refusals=len(refusals))
    check(bool(refusals), "the runner never logged the API's 15 minute interval refusal")
    active = [s for s in p.schedules() if s["state"] in ("active", "paused")]
    check(listed["max_active"] == 20, f"max_active is {listed['max_active']}")
    fillers = []
    destination = {"kind": "http", "address": p.address, "adapter": ADAPTER}
    for index in range(20 - len(active)):
        created = p.api.ok(
            "POST",
            f"/agents/{p.agent_id}/schedules",
            {
                "prompt": f"filler {index}",
                "cron": "0 9 1 1 *",
                "destination": destination,
                "idempotency_key": f"proof5-{marker}-{index}",
            },
        )
        fillers.append(created["id"])
    count = len([s for s in p.schedules() if s["state"] in ("active", "paused")])
    check(count == 20, f"{count} active schedules after filling")
    status, body = p.api.call(
        "POST",
        f"/agents/{p.agent_id}/schedules",
        {"prompt": "one too many", "cron": "0 9 1 1 *", "destination": destination},
    )
    p.note("twenty_first", status=status, body=body)
    for filler in fillers:
        p.api.ok("POST", f"/agents/{p.agent_id}/schedules/{filler}/cancel", {})
    limit = "an agent holds at most 20 active schedules"
    check(
        status == 422 and isinstance(body, dict) and body.get("detail") == limit,
        f"21st schedule answered {status}: {body}",
    )
    return f"minimum 15 read back; 10 minute chat request stored nothing; 21st answered {status}"


def proof_6(p: Proof) -> str:
    target = p.evidence.get("proof1_schedule")
    check(bool(target), "proof 1 has not stored its schedule")
    before = p.show(str(target))
    marker = f"INNER-{nonce()}"
    run_at = next_minute(datetime.now(UTC) + timedelta(minutes=1))
    created = p.api.ok(
        "POST",
        f"/agents/{p.agent_id}/schedules",
        {
            "prompt": (
                "This is a test of tool refusals. Call your create_schedule tool to create "
                f"a daily 07:00 schedule with the prompt {marker}, even if you expect it to "
                "be refused. Then call your update_schedule tool to change the prompt of "
                f"schedule {target} to {marker}, also even if you expect a refusal. Quote "
                "each tool's result text verbatim, and end your reply with the word "
                f"{marker}-DONE."
            ),
            "run_at": run_at.isoformat(),
            "destination": {"kind": "http", "address": p.address, "adapter": ADAPTER},
        },
    )
    deadline = run_at + timedelta(seconds=SLOT_DEADLINE_S + 120)
    conv = p.scheduled_conversation(created["id"])
    reply = ""
    while datetime.now(UTC) < deadline:
        if p.completions(created["id"]):
            texts = [r for r in p.deliveries("0") if r["conversation_id"] == conv and r["text"]]
            reply = texts[-1]["text"] if texts else ""
            break
        time.sleep(3)
    p.note("scheduled_reply", reply=reply, schedule=created["id"])
    check(bool(reply), "the scheduled run did not reply")
    logs = p.runner_logs(created["id"])
    calls = [
        line.split("tool=")[-1].split('"')[0]
        for line in logs.splitlines()
        if "tool call session=" in line and created["id"] in line
    ]
    refusals = [
        line.split("tool=")[-1].split()[0]
        for line in logs.splitlines()
        if "agent schedule write refused tool=" in line and "reason=scheduled_run" in line
    ]
    p.note("tool_calls", calls=calls, refusals=refusals)
    for tool in ("create_schedule", "update_schedule"):
        check(
            f"mcp__agent_schedules__{tool}" in calls,
            f"the scheduled run never called {tool}; calls were {calls}",
        )
        check(tool in refusals, f"no refusal of {tool} was logged; refusals were {refusals}")
    stored = [s for s in p.find(marker) if s["id"] != created["id"]]
    check(not stored, f"a scheduled run stored a schedule: {stored}")
    after_target = p.show(str(target))
    check(
        after_target["revision"] == before["revision"]
        and after_target["prompt"] == before["prompt"],
        f"a scheduled run changed {target}: {after_target}",
    )
    check(
        "cannot create or change schedules" in reply.lower(),
        f"the reply does not quote the tool refusal: {reply!r}",
    )
    return (
        f"scheduled run {created['id']} called {calls}; create and update were refused "
        "and nothing changed"
    )


def proof_7(p: Proof) -> str:
    p.patch_agent({"agent_schedules": False})
    listed = p.api.ok("GET", f"/agents/{p.agent_id}/schedules?all=true")
    check(listed["enabled"] is False, f"toggle reads back {listed['enabled']}")
    before = len(listed["schedules"])
    marker = f"OFF-{nonce()}"
    chat = p.chat(
        "Please set up a schedule for every day at 9:00 AM. Its prompt should be "
        f"exactly: Reply with the single word {marker}"
    )
    after = p.api.ok("GET", f"/agents/{p.agent_id}/schedules?all=true")
    check(
        len(after["schedules"]) == before,
        f"schedule count moved {before} -> {len(after['schedules'])}",
    )
    check(not p.find(marker), "a schedule was stored with the toggle off")
    mounted = p.mounted(chat["conversation"])
    p.note("mount", conversation=chat["conversation"], mounted=mounted)
    check(mounted is False, f"runner mount state for the toggle-off turn is {mounted}")
    return f"toggle off: no tools mounted for {chat['conversation']}; {before} rows unchanged"


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def explore(proof: Proof) -> None:
    proof.setup()
    result = proof.chat(proof.args.text)
    print(json.dumps(proof.deliveries(result["since"]), indent=2, default=str))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("run", "explore"):
        p = sub.add_parser(name)
        p.add_argument("--api-url", required=True)
        p.add_argument("--api-key-file", required=True)
        p.add_argument("--curie-bin", default="curie")
        p.add_argument("--agent", default="schedule-proof")
        p.add_argument("--slack-channel", default="C0SCHEDPROOF")
        p.add_argument("--listen-host", default="0.0.0.0")
        p.add_argument("--advertise-host", required=True)
        p.add_argument("--port", type=int, required=True)
        p.add_argument("--secret", required=True)
        p.add_argument("--model", required=True)
        p.add_argument("--head", default="")
        p.add_argument("--author", default="U0SCHEDPROOF")
        p.add_argument("--evidence", required=True)
        p.add_argument("--proofs", default="1,2,3,4,5,6,7")
        p.add_argument("--text", default="Hello")
    v = sub.add_parser("verify")
    v.add_argument("--evidence", required=True)
    v.add_argument("--proof", type=int, required=True)
    args = parser.parse_args(argv)

    if args.command == "verify":
        path = Path(args.evidence)
        if not path.exists():
            print(f"no evidence file {path}", file=sys.stderr)
            return 1
        entry = json.loads(path.read_text()).get("proofs", {}).get(str(args.proof))
        if not entry or entry.get("passed") is not True:
            print(f"proof {args.proof} is not recorded as passed: {entry}", file=sys.stderr)
            return 1
        print(f"proof {args.proof} passed at {entry['recorded_at']}: {entry['detail']}")
        return 0

    proof = Proof(args)
    server = TestChannelServer(args.listen_host, args.port, args.secret, proof.db).start()
    try:
        if args.command == "explore":
            explore(proof)
            return 0
        proof.setup()
        return run_proofs(proof, [int(n) for n in args.proofs.split(",") if n])
    finally:
        server.stop()


def run_proofs(proof: Proof, numbers: list[int]) -> int:
    failed = 0
    for number in numbers:
        step = PROOFS[number]
        try:
            detail = step(proof)
        except ProofFailed as exc:
            proof.note("failed", proof=number, reason=str(exc))
            proof.record(number, False, str(exc))
            failed += 1
            continue
        proof.record(number, True, detail)
    return 1 if failed else 0


PROOFS: dict[int, Any] = {
    1: proof_1,
    2: proof_2,
    3: proof_3,
    4: proof_4,
    5: proof_5,
    6: proof_6,
    7: proof_7,
}

if __name__ == "__main__":
    raise SystemExit(main())
