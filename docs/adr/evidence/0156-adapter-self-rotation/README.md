# ADR 0156 implementation note

[ADR 0156](../../0156-adapter-principal-with-a-scoped-credential.md), decision 4,
authorizes an adapter to obtain its successor before expiry using its current
credential and the same subject and bindings. This note records implementation;
it neither amends the decision nor changes the Accepted record.

The existing `rotate_adapter_principal` route in
`apps/api/src/curie_api/routers/approvals.py` already accepts only a live adapter
principal, preserves its subject, never widens its binding set, caps the requested
TTL, and refuses the platform key. Explicit subject or binding overrides are
refused. Deleted bindings may be dropped from the successor. The adapter accepts
and restores that narrower principal only while it still serves its channel.
Rotation emits a credential-free audit log naming the adapter, following
the existing channel-mint audit convention. Approval decisions retain their
separate durable `approval_audit` rows.

`packages/channel-gateway/src/curie_channel_gateway/mail/credential_rotation.py` realizes
the adapter side, behind `CURIE_MAIL_SELF_ROTATE=false` by default. It renews both
credentials through the existing API routes, commits an owner-only SQLite snapshot
beside the delivery journal, and restores newer unexpired credentials on restart.
The mail adapter's turn submission and channel mint share a lock because the API
revokes a predecessor channel token immediately. Principal predecessors remain
valid until expiry. The adapter holds no platform key and updates no external
secret store.
Principal renewal starts at half-life and continues during a snapshot write
failure. Only channel mints wait for persistence to recover, since they revoke
their predecessors immediately.

`apps/mail-adapter/tests/test_self_rotation.py` exercises scheduling, overlap with
in-flight ingress, snapshot restore, storage failures, refusal and recovery.
`apps/api/tests/test_adapter_principal.py` exercises real API authentication,
subject and binding bounds, TTL bounds, expiry and audit identity.
`apps/mail-adapter/integration/test_self_rotation.py` drives the adapter against a running
API and real Postgres, Valkey and object storage with one-minute credentials,
two renewals, and ingress during both intervals.

The [mail adapter README](../../../../apps/mail-adapter/README.md) documents the
opt-in, failure signals and the expanded trust placed in the journal volume.
