## Parity seams: cover the sibling or file it

Sibling-path drift is the dominant historical bug class here: logic or hardening
lands on one side of a structural seam while its twin keeps the old behavior.
Known seam pairs, each tagged with what actually enforces it -- `vector:` (a
frozen two-sided vector file), `gate:` (a test or CI job), `by construction:` (a
shared module both sides route through), or `convention` (no gate covers the pair
as a whole; remembered only):

1. worker vs CLI credential forwarding -- `_SDK_PASSTHROUGH_ENV`
  (`apps/worker/src/curie_worker/sandbox/docker.py`) and the CLI picker
  (`cli/src/commands/credential_env.rs`) can't share code across Python/Rust, so they are frozen
  together in `tests/vectors/model-credential-forwarding.json`.
  [vector: `tests/vectors/model-credential-forwarding.json`]
2. dispatcher vs CLI approval action ids -- the approval-card action-id constants
  (`apps/dispatcher/src/curie_dispatcher/approval_actions.py`) and the CLI's
  `APPROVE_ACTION_ID_PREFIX` (`cli/src/chat.rs`) can't share code across
  Python/Rust, so they are frozen together in
  `tests/vectors/approval-action-ids.json`.
  [vector: `tests/vectors/approval-action-ids.json`]
3. worker vs CLI progress block ids -- the Slack progress card and milestone
  `block_id` prefixes (`PROGRESS_CARD_BLOCK_ID_PREFIX` and
  `PROGRESS_MILESTONE_BLOCK_ID_PREFIX` in `apps/worker/src/curie_worker/blocks.py`)
  and the CLI stub's copies (`cli/src/chat.rs`), which tell a progress post or
  edit from the turn's answer, can't share code across Python/Rust, so they are
  frozen together in `tests/vectors/progress-blocks.json`.
  [vector: `tests/vectors/progress-blocks.json`]
4. dispatcher vs API approval-principal tokens -- the dispatcher mint codec
  (`apps/dispatcher/src/curie_dispatcher/approval_principal.py`) and API verifier
  (`apps/api/src/curie_api/approval_principal.py`) are frozen together in
  `tests/vectors/approval-principal.json`.
  [vector: `tests/vectors/approval-principal.json`]
5. dispatcher vs API approval-row miss -- the API 404 detail
  (`apps/api/src/curie_api/routers/approvals.py`) and the dispatcher's
  ownership-miss match (`apps/dispatcher/src/curie_dispatcher/approval_actions.py`)
  cannot share code, so they are frozen together in
  `tests/vectors/approval-ownership.json`.
  [vector: `tests/vectors/approval-ownership.json`]
6. API vs worker approval re-raise refusal code -- the 409 code
  (`apps/api/src/curie_api/routers/approvals.py`) and the worker's refusal
  match (`apps/worker/src/curie_worker/approvals.py`) cannot share code, so
  they are frozen together in `tests/vectors/approval-reraise-refusal.json`.
  [vector: `tests/vectors/approval-reraise-refusal.json`]
7. API vs worker vs CLI thread-reset SET -- `THREAD_RESET_SET` /
  `THREAD_RESET_INFLIGHT_SET` (`apps/api/src/curie_api/threadreset.py`,
  `apps/worker/src/curie_worker/consumer.py`) and the CLI's `THREAD_RESET_SET`
  (`cli/src/queue.rs`) can't share code across Python/Rust. Operator
  `reset-thread` and `local`/`cluster` eval (#1534) both SADD the same set.
  Frozen in `tests/vectors/thread-reset-set.json`.
  [vector: `tests/vectors/thread-reset-set.json`]
8. worker vs CLI eval-isolate prefix -- `EVAL_ISOLATE_THREAD_PREFIX`
  (`apps/worker/src/curie_worker/binding.py`) and the CLI copy
  (`cli/src/queue.rs`, stamped in `cli/src/message.rs`)
  cannot share code across Python/Rust, so they are frozen together in
  `tests/vectors/eval-memory-isolation.json`.
  [vector: `tests/vectors/eval-memory-isolation.json`]
9. worker vs API workspace-selection refusal codes -- the 409 `detail.code`
  values the API's selection router emits
  (`apps/api/src/curie_api/routers/workspaces.py`) and the worker's prose map
  (`_SELECTION_REFUSAL_MESSAGES` in `apps/worker/src/curie_worker/workspace.py`)
  cannot share code because the worker does not import the API package at
  runtime; the worker maps each code to its own prose and treats an unmapped
  code as an invalid refusal response (#2684).
  [vector: `tests/vectors/workspace-selection-refusal.json`]
  [gate: `apps/api/tests/test_workspace_control_plane.py::test_first_repo_selection_is_sticky_allowlisted_and_conflict_safe`]
  [gate: `apps/worker/tests/test_workspace.py::test_internal_workspace_selection_409_maps_machine_code_not_detail_prose`]
10. API vs CLI snapshot sealing key custody -- the reserved names, refusal
  reason and reference grammar (`packages/curie-internal/src/curie_internal/sealing_key.py`)
  with the API's bundle check (`apps/api/src/curie_api/bundles.py::sealing_key_custody_issues`)
  and the CLI deploy preflight's mirror (`cli/src/sealing_key.rs`) can't share
  code across Python/Rust, so the names, the reason verbatim and every bundle's
  refusals with their locations are frozen together in
  `tests/vectors/sealing-key-custody.json` (ACTION-EXECUTOR-23).
  [vector: `tests/vectors/sealing-key-custody.json`]
11. real SDK vs fake model session in the runner (`FakeModelSession`,
  `runner/src/curie_runner/fake.py`).
  [by construction: `runner/src/curie_runner/adapter.py::ModelSession`]
12. runs lane vs eval lane stream consumers (`apps/worker/src/curie_worker/consumer.py`
  vs `apps/worker/src/curie_worker/eval/stream.py`, both on the shared `stream_consumer.py`).
  [by construction: `apps/worker/src/curie_worker/stream_consumer.py`]
13. CLI-side vs API-side input validation (validate at the API/persistence
  boundary, mirror in the CLI). [convention]
14. `local up` vs `local down` compose profile sets.
  [gate: `cli/src/local.rs`, test `down_passes_every_up_profile`]
15. `compose.dev.yaml` vs the generated release compose.
  [by construction: `compose/generate_release_compose.py`]
16. `core` vs `full` compose profiles.
  [gate: `cli/src/local.rs`, test `compose_file_declares_core_and_full_profiles`]
17. `local` vs `cluster` verb pairs (reachability defaults, outcome enums). [convention]
18. CLI `--json` DTOs vs the API models they mirror. [gate: `cli/tests/api_field_parity.rs`]
19. deploy-time validators vs the runtime loaders that re-parse the same value
  (share normalization code). [convention]
20. worker caller-token minter vs connector proxy verifier -- `mint`
  (`apps/worker/src/curie_worker/caller_token.py`) and `decide`
  (`apps/worker/src/curie_connector_proxy/caller.py`) run in different pods,
  and the proxy may not import the worker, so both read one frozen corpus; the
  CLI derives its generated public keys against the same seeds.
  [vector: `tests/vectors/connector-caller-token.json`]
21. connector proxy refusal vs runner diagnosis -- the proxy's refusal body
  (`apps/worker/src/curie_connector_proxy/server.py`) and the runner's
  `caller_refused` reason (`runner/src/curie_runner/mcp_tool_capability.py`)
  ship in different images, so they are frozen together.
  [vector: `tests/vectors/connector-caller-refusal.json`]
22. worker vs runner vs API deliberate progress capability (ADR-0130) -- the
  runner control headers and `turn.progress` token scope the worker mints
  (`apps/worker/src/curie_worker/turn_progress.py`), the runner reads
  (`runner/src/curie_runner/turn_progress.py`) and the API verifies, and the
  inbox stream the API appends to (`apps/api/src/curie_api/turn_progress.py`)
  and the worker's pump reads, cross three images, so they are frozen together.
  [vector: `tests/vectors/turn-progress-capability.json`]
23. worker vs runner executor route (ACTION-EXECUTOR-24) -- the worker's
  `execute` client and executor mode variable
  (`apps/worker/src/curie_worker/runner_client.py`) and the runner's
  `/v1/execute` route and executor mode status body
  (`runner/src/curie_runner/server.py::create_executor_app`) ship in different
  images and cannot share code at runtime, so the request and response of every
  phase, the route's refusal codes and the worker code each maps to, the status
  body and the mode variable are frozen together. The `observe_version` and
  `restore` call arguments and the reply-to-outcome mapping the two sides apply
  around those phases are frozen beside it. The remediation `read` phase
  (AUTOMATED-REMEDIATION-12: the request's `pointer`, one `read` per sandbox,
  `tool_not_read_only` without dialing, the read refusals' worker codes) and
  the codes it adds, which the API's
  `apps/api/src/curie_api/action_execution_codes.py` also reads (the
  worker-reported pre-dispatch code and the sample results that are never
  refusals), are frozen in the same vector.
  [vector: `tests/vectors/runner-execute.json`]
  [vector: `tests/vectors/executor-restore-calls.json`]
24. runner vs worker remediation sample (AUTOMATED-REMEDIATION-12, -17) -- the
  runner's pointer extraction in the `read` phase
  (`runner/src/curie_runner/executor.py`: structured content, or a single text
  block's strict JSON, otherwise `result_unstructured`) and the worker's sample
  report (`apps/worker/src/curie_worker/action_executor.py::sample_report`) ship
  in different images, so every pointer, result shape and sample kind is frozen
  together with the comparator cases the API's evaluator
  (`apps/api/src/curie_api/remediation_predicate.py::evaluate_sample`, which
  judges each verifier sample, plan task 11) reads too: ordering comparators
  read a JSON number or a strict numeric string, `eq`, `ne` and `in` never
  coerce, and every unsuccessful sample (including the API's own `skipped`) is
  `unsuccessful`. Read by `apps/api/tests/test_remediation_predicate_vector.py`.
  [vector: `tests/vectors/remediation-predicate.json`]
25. API vs CLI remediation policy validation and policy refusal codes
  (AUTOMATED-REMEDIATION-3) -- the API validator
  (`apps/api/src/curie_api/remediation_policy_document.py`) and the CLI's
  mirrored validation (`cli/src/remediation_policy.rs`) cannot share code
  across Python/Rust, so the valid and invalid documents, each refusal's code
  and path, and the numeric edge cases are frozen together; the closed policy
  refusal codes the API returns (`POLICY_REFUSALS` in
  `apps/api/src/curie_api/remediation_codes.py`) are the set the CLI renders
  (`POLICY_REFUSALS` in `cli/src/remediation.rs`), frozen as `policy_refusals`.
  The receipt vocabulary (AUTOMATED-REMEDIATION-20) is a third reader: the
  worker's thread receipts (`apps/worker/src/curie_worker/remediation_receipts.py`:
  `RECEIPT_STAGES`, `VERIFICATION_OUTCOMES`, `AUTHORITIES`, `RECEIPT_CODES`), the
  API's nomination read (`remediation_codes.py`) and the CLI's `remediation list`
  and `show` (`NOMINATION_STATES`, `RECEIPT_STAGES` and the rest in
  `cli/src/remediation.rs`) all read the same frozen sets.
  [vector: `tests/vectors/remediation-policy.json`]
  [vector: `tests/vectors/remediation-codes.json`]
26. runner vs worker sealed envelope (ACTION-EXECUTOR-9, -10) -- the runner's
  redactor (`runner/src/curie_runner/redact.py::OutboundRedactor`) decides
  whether an envelope crosses unaltered and the worker's `_snapshot`
  (`apps/worker/src/curie_worker/actions.py`) decides whether it is recorded;
  both validate the same envelope grammar in different images, so they read one
  sealed-reply vector, as does the API's `undoable` grammar
  (`apps/api/src/curie_api/sealed_snapshot.py`).
  [vector: `tests/vectors/sealed-snapshot-reply.json`]
27. API vs worker vs proxy vs runner canonical arguments (ACTION-EXECUTOR-7) --
  the undo ruling's `arguments_sha256`
  (`apps/api/src/curie_api/routers/actions.py::restore_arguments_sha256`), the
  worker's canonicalizer and digest (`apps/worker/src/curie_worker/connector_grant.py`),
  the caller proxy's production parser
  (`apps/worker/src/curie_connector_proxy/server.py::_canonical_arguments`) and
  the runner's `call` preflight must agree byte for byte, and they ship in
  different images, so the canonical texts and digests are frozen together.
  [vector: `tests/vectors/action-canonical-arguments.json`]
28. attachments-init vs docker `_prepare_attachments` (ADR 0205) -- the program
  the chart renders into the sandbox's `attachments-init` container
  (`charts/curie/templates/agent-sandbox.yaml`) cannot import the worker, so the
  docker driver (`apps/worker/src/curie_worker/sandbox/docker.py`) reimplements
  it: exact names, current files first and all-or-nothing, earlier files best
  effort with a recorded reason, digest mismatch always fatal, and the hidden
  `.curie-attachments-status.json`, and the pre-ADR rules for a payload
  without "c" (mixed-version rollout). Both run one payload-to-outcome corpus
  (`charts/curie/ci/attachment-init-behavior-assertions.sh` and
  `apps/worker/tests/sandbox/test_docker_attachment_claim.py`).
  [vector: `tests/vectors/attachment-init-outcomes.json`]

A PR touching one side of a seam must route the behavior through a shared helper
both sides call, change both sides in the same PR, or name the sibling in the PR
body with a follow-up issue number. Prefer parity by construction over remembered
duplication. Ship a test that arms the behavior via the secondary path only
(bundle-manifest-only gate, fake-tier-only, minimal-profile-only) and asserts it
matches the primary path.

