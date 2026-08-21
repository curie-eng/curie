from curie_discord_adapter.ingress import DiscordBinding, DiscordMessage, build_turn


def test_parent_mention_becomes_a_stable_curie_turn() -> None:
    binding = DiscordBinding(parent_channel_id="111", address="111", token="chn_test")
    message = DiscordMessage(
        id="9001",
        channel_id="111",
        thread_id="222",
        author_id="333",
        author_name="Ada",
        content="<@42> summarize this",
        mentioned_user_ids=frozenset({"42"}),
    )

    turn = build_turn(message, bot_user_id="42", binding=binding, reply_ref="9002")

    assert turn == {
        "kind": "discord",
        "address": "111",
        "delivery_id": "9001",
        "conversation_id": "222",
        "author": "Ada (333)",
        "text": "summarize this",
        "reply_ref": "9002",
    }


def test_message_without_the_bot_mention_is_not_a_new_turn() -> None:
    binding = DiscordBinding(parent_channel_id="111", address="111", token="chn_test")
    message = DiscordMessage(
        id="9001",
        channel_id="111",
        thread_id="222",
        author_id="333",
        author_name="Ada",
        content="summarize this",
        mentioned_user_ids=frozenset(),
    )

    assert build_turn(message, bot_user_id="42", binding=binding, reply_ref="9002") is None
