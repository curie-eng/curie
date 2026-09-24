from mean_tester_probes.observe import observe

TARGET = "U0TARGET01"
PROBE = "1790000000.000100"


def msg(ts, text, user=TARGET, edited=None, blocks=None):
    m = {"ts": ts, "text": text, "user": user}
    if edited:
        m["edited"] = {"ts": edited}
    if blocks:
        m["blocks"] = blocks
    return m


def test_no_reply_yet_is_not_final():
    thread = [msg(PROBE, "[mean test] <@U0TARGET01> hi", user="U0TESTER01")]
    o = observe(thread, TARGET, PROBE, now=1790000010.0, settle_s=20)
    assert o.final is False and o.text is None


def test_a_placeholder_is_never_the_answer():
    thread = [msg("1790000001.0", "On it. Working on your request.")]
    o = observe(thread, TARGET, PROBE, now=1790000100.0, settle_s=20)
    assert o.final is False


def test_a_reply_is_final_only_after_it_stops_changing():
    thread = [msg("1790000001.0", "Here is the answer", edited="1790000090.0")]
    assert observe(thread, TARGET, PROBE, now=1790000100.0, settle_s=20).final is False
    o = observe(thread, TARGET, PROBE, now=1790000111.0, settle_s=20)
    assert o.final is True and o.text == "Here is the answer"
    assert o.replied_after_s == 1.0


def test_an_approval_card_is_seen_by_its_action_ids():
    card = [{"type": "actions", "elements": [
        {"action_id": "curie-approval-approve"}, {"action_id": "curie-approval-reject"},
    ]}]
    thread = [msg("1790000001.0", "Approval required: share the file", blocks=card)]
    assert observe(thread, TARGET, PROBE, now=1790000100.0, settle_s=20).approval_card is True


def test_platform_failure_text_is_flagged_even_inside_a_good_answer():
    text = "This agent is at capacity right now. Please try again shortly."
    o = observe([msg("1790000001.0", text)], TARGET, PROBE, now=1790000100.0, settle_s=20)
    assert o.failure_marker == "This agent is at capacity right now"


def test_replies_from_anyone_but_the_target_are_ignored():
    thread = [msg("1790000001.0", "I can answer that", user="U0SOMEONE1")]
    assert observe(thread, TARGET, PROBE, now=1790000100.0, settle_s=20).text is None
