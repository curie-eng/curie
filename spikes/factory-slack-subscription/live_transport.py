"""Live Slack HTTP transport probe using caller-provided test credentials.

Posts only fixture text and deletes every owned message. A user credential
proves thread transport and readback, not bot scopes or Socket Mode ingress.
No token, channel identifier, message reference or provider payload is printed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
from pathlib import Path
from typing import Any

from channel_protocol.reply import ReplyTarget, ReplyUpdate
from curie_worker.reply_sink import TargetRoute
from curie_worker.slack_sink import SlackReplyAdapter
from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient


def credential(path: Path, key: str) -> str:
    for line in path.read_text().splitlines():
        match = re.match(r"\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)", line)
        if match is not None and match[1] == key:
            value = match[2].strip().strip("\"'")
            if value:
                return value
    raise ValueError("named test credential is unavailable")


async def run(args: argparse.Namespace) -> dict[str, Any]:
    token = credential(Path(args.credential_env_file), args.credential_key)
    client = AsyncWebClient(token=token)
    identity = await client.auth_test()
    if str(identity.get("team", "")).casefold() != args.expected_workspace.casefold():
        raise ValueError("credential workspace does not match the requested test workspace")
    channel = None
    cursor = None
    while True:
        page = await client.conversations_list(
            types="public_channel",
            exclude_archived=True,
            limit=200,
            cursor=cursor,
        )
        channels: list[dict[str, Any]] = page.get("channels", [])
        channel = next((row for row in channels if row.get("name") == args.channel_name), None)
        if channel is not None:
            break
        metadata: dict[str, Any] = page.get("response_metadata", {})
        cursor = metadata.get("next_cursor")
        if not cursor:
            raise ValueError("named test channel is unavailable")
    if not channel.get("is_member"):
        raise ValueError("test credential is not already a member of the test channel")
    if channel.get("is_ext_shared") or channel.get("is_org_shared"):
        raise ValueError("a shared channel is outside this probe")
    channel_id = channel["id"]
    sink = SlackReplyAdapter(token)
    owned: list[str] = []
    results: dict[str, Any] = {"credential_kind": "bot" if identity.get("bot_id") else "user"}
    root = None
    cleanup_errors: list[str] = []
    try:

        async def post(body: str, thread: str | None) -> str:
            result = await sink.emit(
                ReplyUpdate(
                    version="1.0",
                    event="reply.update",
                    text=body,
                    target=ReplyTarget(
                        kind="slack", address=channel_id, conversation_id=thread, reply_ref=None
                    ),
                ),
                route=TargetRoute(),
            )
            if result.ref is None:
                raise RuntimeError("Slack accepted a message without an addressable reference")
            owned.append(result.ref)
            return result.ref

        bodies = [
            "Temporary factory steering transport probe. "
            "Fixture data only; messages will be deleted.",
            "Fixture progress: implementation is in progress.",
            'Fixture steering: keep the API stable. Use "blue" as the label.',
            "Fixture acknowledgement: reply received; no factory run was started.",
        ]
        root = await post(bodies[0], None)
        await post(bodies[1], root)
        steering = await client.chat_postMessage(channel=channel_id, thread_ts=root, text=bodies[2])
        owned.append(steering["ts"])
        await post(bodies[3], root)
        readback = await client.conversations_replies(channel=channel_id, ts=root, limit=100)
        messages: list[dict[str, Any]] = readback.get("messages", [])
        assert len(messages) == 4, "unexpected fixture thread contents"
        assert [message["text"] for message in messages] == bodies, "thread text did not round-trip"
        assert {message["ts"] for message in messages} == set(owned), "thread ownership mismatch"
        assert all(message.get("thread_ts", root) == root for message in messages), (
            "thread mismatch"
        )
        assert all(message.get("user") == identity["user_id"] for message in messages), (
            "author mismatch"
        )
        results.update(
            {
                "root_and_three_replies_verified": True,
                "exact_text_roundtrip": True,
                "same_thread_verified": True,
                "sender_verified": True,
            }
        )
    finally:
        for reference in reversed(owned):
            try:
                await client.chat_delete(channel=channel_id, ts=reference)
            except SlackApiError as error:
                cleanup_errors.append(str(error.response.get("error", "unknown")))
        if root is not None:
            try:
                remaining = await client.conversations_replies(
                    channel=channel_id, ts=root, limit=100
                )
            except SlackApiError as error:
                if error.response.get("error") != "thread_not_found":
                    cleanup_errors.append(str(error.response.get("error", "unknown")))
            else:
                remaining_messages: list[dict[str, Any]] = remaining.get("messages", [])
                if any(message.get("ts") in owned for message in remaining_messages):
                    cleanup_errors.append("owned_messages_remain")
        if cleanup_errors:
            raise RuntimeError("owned message cleanup could not be verified")
        results["owned_messages_deleted_and_absence_verified"] = True
    results["not_proved"] = [
        "bot credential scopes",
        "Socket Mode delivery",
        "phone push notification",
    ]
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credential-env-file", required=True)
    parser.add_argument("--credential-key", required=True)
    parser.add_argument("--expected-workspace", required=True)
    parser.add_argument("--channel-name", required=True)
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    try:
        results = asyncio.run(run(args))
    except SlackApiError as error:
        print(
            json.dumps({"passed": False, "provider_error": error.response.get("error", "unknown")})
        )
        raise SystemExit(1) from None
    except Exception as error:  # noqa: BLE001 - never print credential-bearing provider payloads
        print(json.dumps({"passed": False, "error_class": type(error).__name__}))
        raise SystemExit(1) from None
    print(json.dumps({"passed": True, **results}, indent=2))


if __name__ == "__main__":
    main()
