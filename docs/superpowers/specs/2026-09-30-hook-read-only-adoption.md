# Conditional read-only adoption for authenticated hooks

Related: #3603. This is producer adoption of the existing TOOL-ACCESS contract
in `docs/interfaces/aci-producer/INTERFACE.md`, implemented by #3582, #3590
and #3591. It introduces no ACI or plugin-format member.

`POST /hooks/{agent_id}/{hook}` accepts optional query `tool_access=read-only`.
Omission keeps the existing null policy and ordinary approval behavior. The
signed hook sender selects this restriction outside the untrusted request body;
payload text cannot select it. A narrowed query is an opt-in request, not a
mandatory source policy. The existing signature authenticates the body, so
operators must protect the configured request URL and opt in on every delivery.

A newly accepted delivery carries the selected value on `QueuedTurn.tool_access`
and returns it as `HookAccepted.tool_access`. The receipt proves the queued
policy, not investigation, runtime enforcement, or reply delivery. A completed
duplicate returns the original stored policy only when it matches the retry's
policy. A mismatch returns 409 without changing the first claim or enqueueing.
When the original queued payload was trimmed or is otherwise unavailable, no
policy can be attested and the retry returns 409. Pending restricted retries
also return 409; existing ordinary pending retries retain their 202 receipt,
whose null policy and unknown conversation do not attest an accepted turn.

Before submitting a restricted turn, the operator must establish homogeneous
worker artifacts implementing TOOL-ACCESS-6 and compatible runner artifacts.
API OpenAPI support, ACI protocol version and consumer heartbeat capability are
not worker enforcement evidence. The worker checks the exact target runner's
advertisement before delivering the restricted event. Old or mixed worker
fleets remain an installation blocker: this change does not automatically
identify or exclude them before enqueue, and does not close #3603. Automatic
fleet admission and mandatory source policy require separate reviewed design.

Tests first verify authenticated selection, default null, ignored payload
policy, invalid policy, bad signature, completed duplicate agreement and both
mismatch directions, and unknown original-policy refusal. Tests use isolated
real Postgres and Valkey. Runtime verification must exercise the local hook,
queue, worker and runner path, plus cluster CI. Skill is not applicable because
it bypasses API ingress; local-release is not applicable because no released
artifact or install command changes. Live-provider and external integration
are not applicable to this narrow producer change: it changes neither model
invocation nor PreToolUse/MCP classification or provider request shape. Existing
runner enforcement evidence remains a prerequisite, not new evidence from this
API receipt. A draft PR records any outstanding runtime gates explicitly.
