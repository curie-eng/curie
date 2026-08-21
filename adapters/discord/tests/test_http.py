from channel_protocol import ReplyAck
from curie_discord_adapter.http import create_reply_app
from fastapi.testclient import TestClient


class Recorder:
    def __init__(self) -> None:
        self.events = []

    async def deliver(self, event):
        self.events.append(event)
        return ReplyAck(ref=event.target.reply_ref)


def payload() -> dict:
    return {
        "version": "1.0",
        "event": "turn.status",
        "target": {
            "kind": "discord",
            "address": "111",
            "conversation_id": "222",
            "reply_ref": "333",
        },
        "status": "working",
    }


def test_reply_endpoint_authenticates_and_parses_the_neutral_wire() -> None:
    recorder = Recorder()
    client = TestClient(create_reply_app(recorder, "reply-secret"))

    assert client.post("/replies", json=payload()).status_code == 401
    accepted = client.post(
        "/replies",
        json=payload(),
        headers={"X-Curie-Adapter-Key": "reply-secret"},
    )

    assert accepted.status_code == 200
    assert accepted.json() == {"ref": "333"}
    assert len(recorder.events) == 1
    assert recorder.events[0].event == "turn.status"
