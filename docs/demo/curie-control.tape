# VHS tape for the control agent (ADR-0125).
# Regenerate with: vhs docs/demo/curie-control.tape
#
# What is real and what is staged, stated up front because a demo GIF is a
# claim: every screen, label, button, refusal and audit id below is a live HTTP
# response from the fleet control plane, running against a real Postgres that
# run.sh creates and drops. The HUMAN's lines are a fixed script and no model
# runs -- so the recording is deterministic and needs no model credential.
#
# The chat frame is drawn in the terminal because that is what a terminal
# recording can show honestly. In an install with Slack connected the same
# payloads render as Block Kit; the screens are channel-neutral (ADR-0020) and
# this is one more renderer, not a mock of another one.
#
# Requires on PATH: uv, curl, docker (for the compose Postgres/Valkey).
# Requires running: `curie local up --minimal`.
#
# Install VHS: brew install charmbracelet/tap/vhs

Output docs/demo/curie-control.gif

Require uv
Require curl

Set Shell "bash"
Set FontSize 15
Set Width 1160
Set Height 860
Set Theme "Dracula"
Set Padding 18
Set TypingSpeed 45ms

# Boot the disposable stack off-camera: creating a database and starting an API
# is setup, not the demo. run.sh clears the screen before the conversation.
Hide
Type "cd $(git rev-parse --show-toplevel) && clear"
Enter
Sleep 500ms
Show

Type "bash examples/curie-control/demo/run.sh"
Enter

# The script paces itself (a beat between messages, longer on a screen), so the
# tape only has to wait for it rather than script each step.
Sleep 60s
