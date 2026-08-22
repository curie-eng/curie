# VHS tape for the ADR-0115 delegate-call PROTOTYPE (Draft ADR, not accepted).
# Regenerate with: vhs docs/demo/adr-0115-delegate.tape
#
# See docs/demo/ADR-0115-PROTOTYPE-NOTES.md for what this prototype cuts from
# the full ADR-0115 decision, and why it exists at all despite the ADR being
# Draft -- per ADR-0085/ADR-0102 a Draft authorizes no implementation; this
# branch is a demo-only prototype, never proposed for merge as the real thing.
#
# Requires: a Curie API + worker reachable at $API (this recording used a
# hand-run isolated stack on http://localhost:38000 -- a clean single-developer
# machine would just use `curie local up` and the default :28000), running
# this branch's code, and `jq` on PATH. CURIE_FAKE_MODEL=1 on the worker: the
# offline fake model cannot reason about tool descriptions, so alice's bundle
# uses a `[fake:delegate:bob]` marker to script the curie-delegate tool call --
# see runner/src/curie_runner/fake.py.

Output docs/demo/adr-0115-delegate.gif

Set Shell "bash"
Set FontSize 16
Set Width 1300
Set Height 800
Set Theme "Dracula"
Set TypingSpeed 45ms
Set Padding 20

Hide
Type "export API=http://localhost:38000 KEY=curie-dev-key"
Enter
Type "cd /Users/alexrao/curietech/agentos/.claude/worktrees/prototype-adr-0115-agent-delegate"
Enter
Sleep 500ms
Show

# --- Deploy both agents. Alice is human-facing and gets the auto-mounted
# curie-delegate MCP tool automatically -- no manifest change needed, it is
# platform-injected, per ADR-0115 part 1. Bob is backend-only. ---

Type `curie local deploy --plugin-dir examples/delegate-alice --agent alice --slack-channel C0AGENT001 --api-url $API`
Enter
Sleep 2s

Type `curie local deploy --plugin-dir examples/delegate-bob --agent bob --slack-channel C0ELSE001 --api-url $API`
Enter
Sleep 2s

Hide
Type `ALICE=$(curl -s $API/agents -H "X-API-Key: $KEY" | jq -r '.[] | select(.name=="alice") | .id')`
Enter
Type `BOB=$(curl -s $API/agents -H "X-API-Key: $KEY" | jq -r '.[] | select(.name=="bob") | .id')`
Enter
Sleep 300ms
Show

# --- Unarmed: default closed (ADR-0115 part 5, "the bundle declares, the
# operator arms"). This calls the SAME route alice's curie-delegate tool
# calls -- the platform refuses it for real, 403, not a silent no-op. ---

Type `curl -s -X POST $API/agents/$ALICE/delegate/calls -H "X-API-Key: $KEY" -H "Content-Type: application/json" -d '{"target_agent":"bob","message":"what is 2 + 2?","caller_conversation_id":"demo"}' -w '\nHTTP %{http_code}\n'`
Enter
Sleep 2.5s

# --- Arm it: an operator opt-in, per agent pair, directional. ---

Type `curl -s -X POST $API/delegate/grants -H "X-API-Key: $KEY" -H "Content-Type: application/json" -d '{"caller_agent":"alice","target_agent":"bob","armed":true}'`
Enter
Sleep 2s

# --- Now ask alice for real, through the product surface: a message with the
# fake-model delegate marker triggers the SAME curie-delegate tool, for real,
# through the SAME first-party route -- this time it is armed. ---

Type `curie local message --channel C0AGENT001 --api-url $API "[fake:delegate:bob] what is 2 + 2?"`
Enter
Sleep 4s

# --- The call is asynchronous (ADR-0115 part 3): alice's turn already ended
# by the time the tool call returned "Asking bob about that." Bob's turn has
# since run and the round trip has already landed -- the durable call record
# (not part of the ADR's design; a demo/ops convenience this prototype adds)
# shows it: status "delivered", bob's answer captured, resolved in well under
# a second, no third party anywhere in the path.

Type `curl -s "$API/agents/$ALICE/delegate/calls" -H "X-API-Key: $KEY" | jq .`
Enter
Sleep 3s
