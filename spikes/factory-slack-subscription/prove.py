"""Throwaway proof for the factory Slack and per-tagger credential spike.

This is not product code. It calls the intake, reply-route, Slack split, and
runner credential functions on this checkout, then models the two gaps those
functions leave. Run from the repository root:

    uv run python spikes/factory-slack-subscription/prove.py
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from curie_api.forges.github.review_polling import human_actor
from curie_api.forges.trackers import reply_route
from curie_api.forges.types import GITHUB, TrackerIssueRef
from curie_api.github_factory_events import FactoryRefused, parse_factory_event
from curie_runner.sdk_auth import (
    API_BACKEND_ENV,
    API_KEY_ENV,
    AUTH_TOKEN_ENV,
    BASE_URL_ENV,
    CREDENTIALS_ENV,
    OAUTH_TOKEN_ENV,
    OPENROUTER_BASE_URL,
    UnsupportedApiBackendError,
    UnsupportedCredentialError,
    resolve_sdk_env,
)
from curie_worker.sandbox.docker import _WORKER_OWNED_ENV
from curie_worker.sandbox.k8s import CREDENTIALS_ENV as K8S_CREDENTIALS_ENV
from curie_worker.slack_text import SLACK_ANSWER_MAX_BYTES, split_answer

ROOT = Path(__file__).resolve().parents[2]
LABEL = "curie-factory"
REPO = "acme/fixture"
ALICE = "github:101:alice"
BOB = "github:202:bob"
CARA = "github:303:cara"


def shaped(prefix: str, owner: str) -> str:
    """A low-entropy fixture token. The prefix is assembled so the source
    file does not contain a contiguous provider secret."""

    return "sk-" + prefix + "-" + owner


class MissingTaggerCredential(Exception):
    """The tagger has no credential. The run must not borrow another one."""

    def __init__(self, requester: str) -> None:
        self.requester = requester
        super().__init__(requester)


class TaggerVault:
    """One credential per tracker requester. Login text is not a key."""

    def __init__(self, rows: Mapping[str, str]) -> None:
        self._rows = dict(rows)

    def credential(self, requester: str) -> str:
        try:
            return self._rows[requester]
        except KeyError:
            raise MissingTaggerCredential(requester) from None


def issue_payload(
    *,
    sender_type: str,
    login: str,
    sender_id: int,
    via_app: dict[str, str] | None = None,
) -> dict[str, Any]:
    sender: dict[str, Any] = {"id": sender_id, "login": login, "type": sender_type}
    if via_app is not None:
        sender["performed_via_github_app"] = via_app
    return {
        "action": "labeled",
        "installation": {"id": 1},
        "repository": {"id": 2, "full_name": REPO},
        "sender": sender,
        "issue": {"number": 42, "state": "open", "body": "fixture"},
        "label": {"name": LABEL},
    }


def check_label_actor() -> None:
    """A human label is an admission. An app or bot label is not that human."""

    human = parse_factory_event(
        "issues",
        issue_payload(sender_type="User", login="alice", sender_id=101),
        str(uuid.uuid4()),
        labels={LABEL},
        mention="curie",
    )
    assert human.disposition == "admit"
    assert human.sender_id == 101
    assert human.sender_login == "alice"
    assert f"github:{human.sender_id}:{human.sender_login}" == ALICE

    try:
        parse_factory_event(
            "issues",
            issue_payload(sender_type="Bot", login="curie-app[bot]", sender_id=999),
            str(uuid.uuid4()),
            labels={LABEL},
            mention="curie",
        )
    except FactoryRefused as refused:
        assert refused.code == "app_authored"
    else:
        raise AssertionError("a bot label add was admitted")

    # The poll path drops an app-performed event. The issues webhook parser
    # rejects type Bot and does not consult performed_via_github_app, so an
    # app acting through a User sender still parses as that user.
    app_event = {
        "event": "labeled",
        "performed_via_github_app": {"id": 7},
        "actor": {"id": 101, "login": "alice", "type": "User"},
        "label": {"name": LABEL},
        "issue": {"number": 42},
    }
    assert human_actor(app_event) is None
    bot_event = {
        "event": "labeled",
        "actor": {"id": 999, "login": "curie-app[bot]", "type": "Bot"},
        "label": {"name": LABEL},
        "issue": {"number": 42},
    }
    assert human_actor(bot_event) is None
    user_event = {
        "event": "labeled",
        "actor": {"id": 101, "login": "alice", "type": "User"},
        "label": {"name": LABEL},
        "issue": {"number": 42},
    }
    assert human_actor(user_event) == (101, "alice")


def check_reply_route_is_the_tracker() -> None:
    issue = TrackerIssueRef(GITHUB, "example.com", "2", "42")
    kind, address, conversation = reply_route(cast(Any, None), issue, REPO)
    assert (kind, address, conversation) == ("github", REPO, "issue-42")


@dataclass
class PhoneLink:
    slack_user: str
    requester: str
    may_start: bool


@dataclass
class PhoneBridge:
    """The smallest follow path the current reply route does not provide.

    A DM from a mapped person with start authority records that person's
    tracker requester and later progress is posted only into that DM.
    Applying the factory label as the GitHub App is not this path: the
    label checks above refuse a bot actor.
    """

    links: dict[str, PhoneLink]
    dms: dict[str, list[str]] = field(default_factory=dict)
    admissions: list[tuple[str, str]] = field(default_factory=list)

    def message(self, slack_user: str, channel_type: str, text: str, *, bot: bool) -> str:
        if bot or channel_type != "im":
            return "ignored"
        link = self.links.get(slack_user)
        if link is None:
            return "unmapped"
        if not link.may_start:
            return "no_authority"
        if not text.startswith("run "):
            return "not_a_start"
        self.admissions.append((link.requester, text.removeprefix("run ")))
        self.dms.setdefault(slack_user, []).append(f"started {text.removeprefix('run ')}")
        return "started"

    def progress(self, requester: str, body: str) -> None:
        owners = [link.slack_user for link in self.links.values() if link.requester == requester]
        assert len(owners) == 1
        for part in split_answer(body):
            encoded = part.text.encode()
            assert 0 < len(encoded) <= SLACK_ANSWER_MAX_BYTES
            self.dms[owners[0]].append(part.text)


def check_phone_follow() -> None:
    phases = json.loads(
        (ROOT / "examples/dark-factory/progress/phases.json").read_text()
    )["phases"]
    bridge = PhoneBridge(
        links={
            "U0EXAMPLE1": PhoneLink("U0EXAMPLE1", ALICE, True),
            "U0EXAMPLE2": PhoneLink("U0EXAMPLE2", BOB, False),
        }
    )
    assert bridge.message("U0EXAMPLE9", "im", f"run {REPO}#42", bot=False) == "unmapped"
    assert bridge.message("U0EXAMPLE2", "im", f"run {REPO}#42", bot=False) == "no_authority"
    assert bridge.message("U0EXAMPLE1", "channel", f"run {REPO}#42", bot=False) == "ignored"
    assert bridge.message("U0EXAMPLE1", "im", f"run {REPO}#42", bot=True) == "ignored"
    assert bridge.message("U0EXAMPLE1", "im", f"run {REPO}#42", bot=False) == "started"
    assert bridge.admissions == [(ALICE, f"{REPO}#42")]

    for phase in phases:
        bridge.progress(ALICE, phase["label"])
    before = len(bridge.dms["U0EXAMPLE1"])
    long_note = "n" * (SLACK_ANSWER_MAX_BYTES + 1000)
    bridge.progress(ALICE, long_note)
    pieces = bridge.dms["U0EXAMPLE1"][before:]
    assert len(pieces) >= 2
    assert "".join(pieces) == long_note
    alice_dm = "\n".join(bridge.dms["U0EXAMPLE1"])
    assert "Publish PR" in alice_dm
    assert "U0EXAMPLE2" not in bridge.dms
    assert bridge.dms["U0EXAMPLE1"].count("started acme/fixture#42") == 1


def sdk_env(credential: str, **extra: str) -> dict[str, str]:
    env = {CREDENTIALS_ENV: credential, **extra}
    resolve_sdk_env(env)
    return env


def check_runner_accepts_one_claude_shape() -> None:
    alice = shaped("ant-oat", "alice")
    shared = shaped("ant-api", "shared")
    routed = shaped("or-v1", "router")
    codex = shaped("proj", "codex")

    oauth = sdk_env(alice)
    assert oauth[OAUTH_TOKEN_ENV] == alice
    assert oauth.get(API_KEY_ENV, "") == ""

    api_key = sdk_env(shared)
    assert api_key[API_KEY_ENV] == shared
    assert api_key.get(OAUTH_TOKEN_ENV, "") == ""

    openrouter = sdk_env(routed)
    assert openrouter[API_KEY_ENV] == routed
    assert openrouter[BASE_URL_ENV] == OPENROUTER_BASE_URL
    assert openrouter.get(AUTH_TOKEN_ENV, "") == ""

    gateway = "https://gateway.example.com/anthropic"
    override = resolve_sdk_env({CREDENTIALS_ENV: alice, BASE_URL_ENV: gateway})
    assert override is not None
    assert override[OAUTH_TOKEN_ENV] == ""
    assert alice not in override.values()
    assert override[BASE_URL_ENV] == gateway

    try:
        resolve_sdk_env({CREDENTIALS_ENV: codex})
    except UnsupportedCredentialError:
        pass
    else:
        raise AssertionError("a non-Anthropic sk- token was accepted")

    untouched = {CREDENTIALS_ENV: codex, API_BACKEND_ENV: "responses"}
    try:
        resolve_sdk_env(dict(untouched))
    except UnsupportedApiBackendError:
        pass
    else:
        raise AssertionError("an OpenAI-shaped backend was dialed")
    try:
        resolve_sdk_env({CREDENTIALS_ENV: alice, API_BACKEND_ENV: "chat_completions"})
    except UnsupportedApiBackendError:
        pass
    else:
        raise AssertionError("chat_completions was dialed")


def docker_credential(worker: Mapping[str, str], claim: Mapping[str, str]) -> str:
    """What the container receives for the model credential.

    Mirrors docker.create_claim: the claim value is worker-owned, so it is
    not copied, and the worker process value is forwarded by name.
    """

    assert CREDENTIALS_ENV in _WORKER_OWNED_ENV
    assert claim.get(CREDENTIALS_ENV) not in docker_value_env(claim).values()
    byo = worker.get(CREDENTIALS_ENV, "")
    base_override = BASE_URL_ENV in claim
    if byo and not (base_override and byo.startswith("sk-" + "ant-oat")):
        return byo
    return ""


def docker_value_env(claim: Mapping[str, str]) -> dict[str, str]:
    return {key: value for key, value in claim.items() if key not in _WORKER_OWNED_ENV}


def k8s_credential(template: str, claim: Mapping[str, str]) -> str:
    """The warm template secret stays once the claim value is stripped."""

    assert K8S_CREDENTIALS_ENV == CREDENTIALS_ENV
    stripped = {key: value for key, value in claim.items() if key != CREDENTIALS_ENV}
    assert CREDENTIALS_ENV not in stripped
    return template


def check_delivery_ignores_the_tagger() -> None:
    shared = shaped("ant-api", "shared")
    alice = shaped("ant-oat", "alice")
    claim = {CREDENTIALS_ENV: alice, "CURIE_MODEL": "claude-sonnet-5"}
    assert docker_credential({CREDENTIALS_ENV: shared}, claim) == shared
    assert k8s_credential(shared, claim) == shared
    source = (ROOT / "apps/worker/src/curie_worker/sandbox/docker.py").read_text()
    assert "byo = self._environ.get(CREDENTIALS_ENV)" in source
    k8s = (ROOT / "apps/worker/src/curie_worker/sandbox/k8s.py").read_text()
    assert "CREDENTIALS_ENV," in k8s
    binding = (ROOT / "apps/worker/src/curie_worker/binding.py").read_text()
    assert "credentials_ref=self._config.credentials" in binding


def check_vault_isolation() -> None:
    alice = shaped("ant-oat", "alice")
    bob = shaped("ant-oat", "bob")
    shared = shaped("ant-api", "shared")
    vault = TaggerVault({ALICE: alice, BOB: bob})

    alice_env = sdk_env(vault.credential(ALICE))
    bob_env = sdk_env(vault.credential(BOB))
    assert alice_env[OAUTH_TOKEN_ENV] == alice
    assert bob_env[OAUTH_TOKEN_ENV] == bob
    assert alice not in bob_env.values()
    assert bob not in alice_env.values()
    assert shared not in alice_env.values()
    assert shared not in bob_env.values()

    try:
        vault.credential("alice")
    except MissingTaggerCredential as missing:
        assert missing.requester == "alice"
    else:
        raise AssertionError("a login was accepted as a requester")

    try:
        vault.credential(CARA)
    except MissingTaggerCredential as missing:
        assert missing.requester == CARA
    else:
        raise AssertionError("a missing tagger borrowed a credential")

    # A per-claim secret is the delivery the two substrates do not do.
    # Resolved on its own, it never contains the other tagger's token.
    for requester, token in ((ALICE, alice), (BOB, bob)):
        delivered = {CREDENTIALS_ENV: vault.credential(requester)}
        resolve_sdk_env(delivered)
        assert delivered[OAUTH_TOKEN_ENV] == token
        assert shared not in delivered.values()


def main() -> None:
    check_label_actor()
    check_reply_route_is_the_tracker()
    check_phone_follow()
    check_runner_accepts_one_claude_shape()
    check_delivery_ignores_the_tagger()
    check_vault_isolation()
    print("phone follow isolated to the starter DM")
    print("bot label refused; human label admitted")
    print("reply route is the tracker issue")
    print("tagger vault isolated; release secret is what docker and k8s deliver")
    print("oauth token not forwarded to a gateway base URL")
    print("codex-shaped key and non-messages backends refused")


if __name__ == "__main__":
    main()
