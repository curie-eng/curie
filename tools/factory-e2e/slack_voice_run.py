"""Replay a Slack clip through local recognition, confirmation and same-PR publication."""

from __future__ import annotations

import argparse
import hashlib
import html
import itertools
import re
import shutil
import uuid
from pathlib import Path
from typing import Any

import scripted_kind as sk
import slack_voice_fixture as fixture
from scripted_scenario import SLACK_CHANNEL
from slack_steering_ingress import BOT_USER, TEAM, USER
from slack_steering_model import FixtureEdit, SteeringRouter
from slack_steering_run import SlackSteeringRun

BUTTON = '<button type="button" data-timeout-ms="1500">Try again</button>'
RETRY = BUTTON.replace("Try again", "Retry")
FILE = "retry.html"
CLIP = "F0EXAMPLE1"
OTHER = "U0EXAMPLE8"
UNLINKED = "U0EXAMPLE7"
PROMPT = re.compile(r"Voice transcript\n(.+?)\n\n.*\nconfirm ([a-f0-9]{12}\.[1-9][0-9]*)\n", re.S)


def digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


class VoiceRouter(SteeringRouter):
    """Act only on the exact text shown to and confirmed by the engineer."""

    def __init__(self) -> None:
        super().__init__()
        self.expected: str | None = None
        self.release_initial.set()

    def instruction_key(self, body: str) -> str | None:
        if self.expected is None or body != self.expected:
            return None
        return digest(body)[:32]

    def edit_for(self, key: str) -> FixtureEdit:
        if self.expected is None or digest(self.expected)[:32] != key:
            raise sk.ScriptedScenarioError("review requested an unknown voice revision")
        return FixtureEdit(FILE, BUTTON, RETRY, RETRY)


class SlackVoiceRun(SlackSteeringRun):
    def __init__(self, args: argparse.Namespace) -> None:
        super().__init__(args)
        self.namespace = f"test-slack-voice-{self.run_id}"
        if args.build_images:
            self.owned_build_tag = f"voice-{self.run_id}"
            self.runner_source = f"curie-runner:{self.owned_build_tag}"
            self.image_refs = tuple(
                f"{image.rsplit(':', 1)[0]}:{self.owned_build_tag}" for image in self.image_refs
            )
        self.guidance = VoiceRouter()
        self.timestamps = itertools.count(1)

    def prerequisites(self) -> None:
        fixture.validate_model(self)
        super().prerequisites()

    def fixture_tree(self) -> Path:
        assert self.workdir is not None
        tree = self.workdir / "voice-repository"
        shutil.copytree(sk.FIXTURE, tree)
        (tree / FILE).write_text(f"<!doctype html>\n{BUTTON}\n")
        return tree

    def before_install(self, registry_ref: str) -> None:
        fixture.prepare(self)

    def extend_install_values(self, values: dict[str, Any]) -> None:
        super().extend_install_values(values)
        values["factory"]["voiceModelClaim"] = fixture.MODEL_CLAIM

    def before_label(self, github: Any) -> None:
        fixture.attach_gateway(self)

    def send(
        self,
        root: str,
        text: str,
        *,
        actor: str = USER,
        channel: str = SLACK_CHANNEL,
        file_id: str | None = None,
        admission_may_refuse: bool = False,
    ) -> tuple[dict[str, Any], str | None]:
        assert self.slack is not None
        event: dict[str, Any] = {
            "type": "app_mention",
            "channel": channel,
            "thread_ts": root,
            "ts": f"1800000500.{next(self.timestamps):06d}",
            "user": actor,
            "user_team": TEAM,
            "team": TEAM,
            "text": f"<@{BOT_USER}> {text}",
        }
        if file_id:
            recording = fixture.MEDIA.get(
                file_id, fixture.Recording("unavailable.m4a", "audio/mp4")
            )
            event.update(
                subtype="file_share",
                files=[
                    {
                        "id": file_id,
                        "mode": "hosted",
                        "subtype": "slack_audio",
                        "name": recording.filename,
                        "mimetype": recording.mime,
                        "is_external": False,
                        "user": actor,
                        "user_team": TEAM,
                    }
                ],
            )
            self.slack.add_voice_message(event, accessible=file_id in fixture.MEDIA)
        message = {
            "slack": self.slack.base_url,
            "event_id": "Ev" + uuid.uuid4().hex.upper(),
            "event": event,
            "bot_token": sk.SCRIPTED_SLACK_BOT_TOKEN,
        }
        start = len(self.slack.posts())
        ack = self.ingress("deliver", message)
        if not ack.get("acked"):
            raise sk.ScriptedScenarioError("Socket Mode replay was not acknowledged")
        if admission_may_refuse and not ack["claimed"] and ack["receipt_version"] is None:
            return message, None
        if not ack["claimed"] or ack["receipt_version"] != (3 if file_id else 1):
            raise sk.ScriptedScenarioError("Socket Mode replay did not create the expected receipt")
        placeholders = [
            p.ts
            for p in self.slack.posts()[start:]
            if p.method == "chat.postMessage"
            and p.channel == channel
            and p.thread_ts == root
            and p.text == ack["placeholder_text"]
        ]
        if len(placeholders) != 1:
            raise sk.ScriptedScenarioError("voice delivery did not receive one source placeholder")
        return message, placeholders[0]

    def status(self, placeholder: str | None, expected: tuple[str, ...]) -> None:
        assert self.slack is not None
        if placeholder is None:
            return
        self.wait_for(
            "a bounded voice status",
            lambda: any(
                p.method == "chat.update"
                and p.ts == placeholder
                and any(word in (p.text or "").lower() for word in expected)
                for p in self.slack.posts()
            ),
        )

    def thread_text(
        self, root: str, start: int, *, refusal: bool = False
    ) -> tuple[str, str] | bool:
        assert self.slack is not None
        for post in self.slack.posts()[start:]:
            text = html.unescape(post.text or "")
            matched = PROMPT.match(text)
            if matched:
                transcript, code = matched.groups()
                self.secrets[:0] = [transcript, code, code.split(".")[0]]
            if not matched and not text.startswith("Voice feedback refused ("):
                continue
            if post.channel != SLACK_CHANNEL or post.thread_ts != root:
                raise sk.ScriptedScenarioError("voice callback left its source thread")
            if refusal and "(no_speech)" in text:
                return True
            if not refusal and matched:
                return matched.group(1), matched.group(2)
        return False

    def draft(self, root: str, start: int) -> tuple[str, str]:
        displayed, code = self.wait_for(
            "the source transcript", lambda: self.thread_text(root, start)
        )
        prefix, version = code.split(".")
        self.wait_for(
            "acknowledged source transcript",
            lambda: self.sql(
                "SELECT EXISTS (SELECT 1 FROM curie.factory_voice_drafts "
                f"WHERE code_prefix='{prefix}' AND version={int(version)} AND status='ready')::text"
            ),
        )
        return displayed, code

    def request_ids(self, item: str) -> list[str]:
        return self.sql(
            "SELECT coalesce(json_agg(id ORDER BY sequence), '[]') FROM curie.execution_requests "
            f"WHERE work_item_id='{uuid.UUID(item)}'"
        )

    def run_publication(self, github: Any) -> None:
        assert self.slack is not None
        principal = self.bind_sender(github)
        self.ingress("seed", {"user": OTHER})
        self.label_issue(github)
        item, root = self.wait_for("the public root", self.root)
        (initial,) = self.wait_for("the initial publication", lambda: self.settled(item, 1))
        original_pr, head, url = self.code_host()
        first_requests = [initial["request"]]

        def no_revision() -> None:
            if self.ledger(item) or self.request_ids(item) != first_requests:
                raise sk.ScriptedScenarioError("unconfirmed or refused feedback admitted work")

        for channel, thread, actor, media, allowed in (
            (SLACK_CHANNEL, "1800000000.000999", USER, CLIP, ("mapped",)),
            ("C0EXAMPLE8", root, USER, CLIP, ("mapped",)),
            (SLACK_CHANNEL, root, UNLINKED, CLIP, ("cannot", "verified")),
            (SLACK_CHANNEL, root, USER, "F0EXAMPLE3", ("changed", "cannot")),
        ):
            _, slot = self.send(
                thread,
                "Review this clip.",
                channel=channel,
                actor=actor,
                file_id=media,
                admission_may_refuse=channel != SLACK_CHANNEL or actor == UNLINKED,
            )
            self.status(slot, allowed)
            no_revision()

        start = len(self.slack.posts())
        self.send(root, "Review the silent clip.", file_id="F0EXAMPLE2")
        self.wait_for(
            "the silent clip refusal", lambda: self.thread_text(root, start, refusal=True)
        )
        no_revision()

        start = len(self.slack.posts())
        old, _ = self.send(root, "Check the expiring source.", file_id="F0EXAMPLE4")
        _, old_code = self.draft(root, start)
        if self.ingress("expire", {"event_id": old["event_id"]}) != {"expired": True}:
            raise sk.ScriptedScenarioError("could not expire the control source receipt")
        _, slot = self.send(root, "confirm " + old_code)
        self.status(slot, ("changed", "verified"))
        no_revision()

        start = len(self.slack.posts())
        clip, _ = self.send(root, "Please review this recording.", file_id=CLIP)
        self.ingress("deliver", clip)
        transcript, code = self.draft(root, start)
        words = " ".join(re.findall(r"[a-z0-9']+", transcript.lower()))
        if "change the button label to retry" not in words:
            raise sk.ScriptedScenarioError(
                "recognized text did not contain the spoken label change"
            )
        no_revision()
        _, slot = self.send(root, "confirm " + code, actor=OTHER)
        self.status(slot, ("cannot",))
        no_revision()

        edited = transcript + " Keep the existing timeout."
        self.secrets.insert(0, edited)
        start_edit = len(self.slack.posts())
        self.send(root, f"edit {code} {edited}")
        display, current = self.draft(root, start_edit)
        if display != edited or current != code.rsplit(".", 1)[0] + ".2":
            raise sk.ScriptedScenarioError("edited feedback was not displayed with its new code")
        _, slot = self.send(root, "confirm " + code)
        self.status(slot, ("latest version",))
        no_revision()

        self.guidance.expected = edited
        decision, slot = self.send(root, "confirm " + current)
        self.ingress("deliver", decision)
        self.status(slot, ("queued", "applied"))
        _, duplicate = self.send(root, "confirm " + current)
        self.status(duplicate, ("queued", "applied"))
        _, revision = self.wait_for("one revised publication", lambda: self.settled(item, 2))
        final_pr, new_head, final_url = self.code_host()
        before = self.repo_bytes("show", f"{head}:{FILE}")
        after = self.repo_bytes("show", f"{new_head}:{FILE}")
        if (
            final_pr != original_pr
            or final_url != url
            or new_head == head
            or revision["prior_head"] != head
            or revision["lineage"] != initial["lineage"]
            or self.repo_bytes("diff", "--name-only", head, new_head).splitlines()
            != [FILE.encode()]
            or before.count(BUTTON.encode()) != 1
            or after != before.replace(BUTTON.encode(), RETRY.encode())
            or self.request_ids(item) != [*first_requests, revision["request"]]
            or self.guidance.received != [digest(edited)[:32]]
        ):
            raise sk.ScriptedScenarioError(
                "confirmed feedback did not produce exactly one same-PR edit"
            )
        rows = self.sql(
            "SELECT coalesce(json_agg(json_build_array(source_facts->>'principal_id', "
            "source_facts->>'author',source_facts->>'file_id',source_facts->>'message_ts',"
            "source_channel,source_root_ts,accepted_text_sha256,state,applied_request_id)), '[]') "
            f"FROM curie.factory_steering_ledger WHERE work_item_id='{uuid.UUID(item)}'"
        )
        if rows != [
            [
                principal,
                USER,
                CLIP,
                clip["event"]["ts"],
                SLACK_CHANNEL,
                root,
                digest(edited),
                "applied",
                revision["request"],
            ]
        ]:
            raise sk.ScriptedScenarioError(
                "the confirmed text, source, actor or consuming request differed"
            )
        for post in self.slack.posts():
            if any(value in (post.text or "") for value in (code, current, transcript)) and (
                post.channel != SLACK_CHANNEL or post.thread_ts != root
            ):
                raise sk.ScriptedScenarioError("transcript or prompt reached another destination")
        self.evidence["slack_voice"] = {
            "ingress": "local Socket Mode replay",
            "work_item_id": item,
            "pull_request_number": original_pr[0],
            "publication_heads": [head, new_head],
            "consuming_execution_id": revision["request"],
            "accepted_text_sha256": digest(edited),
            "unchanged_timeout_ms": 1500,
            "button_label": "Retry",
            "controls": [
                "wrong_root",
                "unbound_channel",
                "unlinked_actor",
                "inaccessible_media",
                "silence",
                "expired_receipt",
                "wrong_confirmer",
                "stale_version",
                "replay",
            ],
        }
