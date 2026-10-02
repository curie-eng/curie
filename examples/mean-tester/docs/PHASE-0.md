# Validator phase 0: observed limits and remaining measurements

Observed on 2026-10-01 against platform source
`9693123e5ba622f5bff94cbcd41b98fa204cb164`. Source inspection and a local pure
attachment-function probe are distinguished below from provider observations.
No existing installation was changed, no card was resolved by this tester,
and no external upload was performed. The proposed test mark is not active.

## Approval principal: conditional capability, missing tester consumer

The premise “bots cannot resolve approvals” is too broad. The authenticated API
resolver is separate from Slack buttons. Source inspection:

```bash
rg -n 'operator|channel' apps/api/src/curie_api/authorizer.py
rg -n 'eligible|class RequesterOnly' apps/api/src/curie_api/approvers.py
rg -n 'principals|resolve' apps/api/src/curie_api/routers/approvals.py
```

The resolver accepts authenticated principal kinds and evaluates the selected
approver set; it does not accept an arbitrary actor in the request body.
Operators are eligible only for explicit-user-list routes. They cannot invent
Slack channel or group membership. Subject-bound console principals and
binding-scoped adapters have different eligibility; adapters cannot impersonate
Slack IDs in explicit lists. Non-Slack requester-only routes require the serving
adapter and authenticated requester. These are distinct authorization cases,
not an invitation to change a live route's approvers for the tester.

The current example declares only seven Slack/GitHub tools and holds no platform
API key or resolve credential. There is no consumer for an automated marked-run
approval capability in this example. Slice 2 needs reviewed principal issuance,
mark validation, target scope, route eligibility and card ownership. An operator
may technically resolve an explicitly permitted route; that does not establish a
safe automated channel/group resolver or grant this tester that capability.

Relevant existing integration selectors are
`apps/api/tests/test_approval_authenticated_principals.py::test_operator_principals_are_explicit_user_only_even_if_group_members`
and
`apps/api/tests/test_approval_authenticated_principals.py::test_authorized_solo_requester_can_self_confirm_but_membership_still_denies`.
Execution evidence is recorded separately when the isolated backing stack is
available. Source assertions alone are not a completed integration measurement.

## Attachment without a name: local mechanism confirmed, external cause open

The dispatcher uses `inbound_attachments.derive_attachments`; `_to_attachment`
requires both `id` and `name`. It does not use `title` as a fallback. Run the
following with the workspace dependencies active (the observation used
`uv run --active --no-sync python`, with `PYTHONPATH` pointing to this candidate's
dispatcher source):

```python
from curie_dispatcher.inbound_attachments import derive_attachments
print('missing_name_refs=', len(derive_attachments({
    'files': [{'id': 'F0EXAMPLE1', 'title': 'acme-report.pdf'}]})))
print('named_refs=', len(derive_attachments({
    'files': [{'id': 'F0EXAMPLE1', 'name': 'acme-report.pdf',
               'title': 'acme-report.pdf'}]})))
```

Observed stdout:

```text
missing_name_refs= 0
named_refs= 1
```

This confirms the local drop mechanism and its named-file secondary path. It
**does not prove** that external Slack uploads omit `name`, or that this was the
cause of the reported missing-file reply. The existing dispatcher integration
selector
`apps/dispatcher/tests/test_inbound_attachments.py::test_malformed_files_never_raise_and_yield_no_refs[entry-missing-name]`
asserts the same drop through intake. That fixture requires real Valkey and was
not substituted for an external upload observation.

A later approved measurement must capture the external-upload message event,
its `files` metadata, the queued attachment references and the runner attachment
manifest under one correlation identity. Preserve live IDs outside committed
files. Determine whether Slack hydration, reference delivery or byte download
failed before proposing a platform fix. Slice 1 never uploads files.

## Click-time approvers and denied audit

Source inspection of `routers.approvals.resolve_approval` shows the route
binding read fresh before authorization. `decision.allowed == False` appends
an audit entry with `action='denied'`, `authorized=False`, the authorizer and
membership evidence, then returns HTTP 403 before resolution. A rejected attempt
does not resolve the pending approval. `/approvals/{id}/audit` reads these
entries with API authorization; the tester currently has no audit-read tool.

Relevant integration selectors:

- `apps/api/tests/test_approvals.py::test_audit_log_records_attempts_with_authorizer_snapshots`
- `apps/api/tests/test_approvals.py::test_user_list_bound_route_denies_an_unlisted_actor_without_calling_slack`
- `apps/api/tests/test_approvals.py::test_clearing_a_bound_route_makes_a_pending_approval_unresolvable_not_wider`

These can prove the real Postgres/Valkey resolver and secondary paths with Slack
membership simulated, not a real Slack click. Fresh-binding source plus the
existing assertions support the design; isolated execution is still needed to
close the local measurement. A live narrow-and-restore approver experiment
would alter a target installation and is outside this slice's authorization.

## Decision record

The action validator remains blocked on an authenticated scoped principal and
reviewed mark/restore contracts. Missing-name attachment dropping is reproduced
locally, while the reported external event's shape and causal failure remain
unresolved. Non-approver denial and audit have concrete resolver paths and test
selectors; never report provider confirmation from those source reads.

The request's ADR 0174 pointer is incorrect. The existing
[ADR 0174](../../../docs/adr/0174-publication-prechecks-use-an-execution-scoped-capability.md)
is about publication prechecks. ADR 0181 remains Accepted and unmodified;
[VALIDATOR.md](VALIDATOR.md) lists the owner decisions before any proposed
exception or slice 2 implementation.
