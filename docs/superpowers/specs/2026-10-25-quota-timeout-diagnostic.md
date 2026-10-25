# Temporary quota timeout observations

This is a diagnostic experiment, not a product fix or merge candidate. It adds observations for one controlled execution of the existing quota-progress scenario. It does not classify the original timeout as pre-existing or waive any required check.

## Preserve the scenario

<!-- @spec QUOTA-TIMEOUT-DIAGNOSTIC-1 -->
Keep sixteen CLI launches and their arguments, quota two, the 300-second CLI timer, the 320-second aggregate deadline, existing finished() semantics, loop ordering/sleeps, quota and post-loop assertions, and shell cleanup unchanged. Diagnostics never replace the existing completed snapshot or affect the verdict. Do not add a poller, process, thread, network call, dependency, workflow permission or selector change. Existing raw outputs and private failure files remain private.

## Bounded summary

<!-- @spec QUOTA-TIMEOUT-DIAGNOSTIC-2 -->
Emit exactly one line on loop exit, including exceptions, with prefix `Quota burst diagnostic: ` and JSON containing only:

- schema_version: integer 1.
- elapsed_seconds: monotonic time since the original deadline origin.
- prior_completed_count: integer 0..16, or null without a complete sample.
- prior_sample_age_seconds: sample age, or null.
- fresh_exited_count and fresh_running_count: integers 0..16 from fresh owned-process poll() calls.
- last_observation_seconds and max_observation_seconds: duration of existing quota_pods(), measured in finally even when it raises.
- processes: exactly sixteen records, ordinal order, each containing index0..15, state exited/running, and strict integer returncode or null.
- diagnostic_unavailable: boolean.

Round finite nonnegative durations to three decimal places. Serialize a newly allowlisted object, never a turn/process dictionary. Exclude PID, names, arguments, stdout/stderr, paths, URLs, tokens, payloads, tenant or thread identifiers and arbitrary strings. Output is under4,096 bytes. Invalid inputs, polling/serialization errors or excess size use only the same prefix plus {"schema_version":1,"diagnostic_unavailable":true}; no exception text. Attempt delivery of the single constructed line once. Delivery is best effort if the output stream itself fails: suppress that failure without further output or exception text. Diagnostic failure must neither replace an original exception nor fail an otherwise successful scenario.

The helper lives at charts/curie/ci/runtime/quota_burst_diagnostics.py and has no import-time side effects. render_summary accepts only the documented numbers/nulls and sixteen return codes. Instrument only the existing burst_sixteen observation loop in idle-capacity-reclamation-runtime.sh; its cleanup remains the sole resource owner.

## Evidence and limits

<!-- @spec QUOTA-TIMEOUT-DIAGNOSTIC-3 -->
Focused tests execute the actual embedded burst_sixteen AST definition against deterministic external observation/process/clock boundaries, without executing installation or cluster setup. A stale sample of fifteen followed by a blocking observation and sixteen fresh exits MUST still raise the original aggregate-timeout exception. Test observation timing, original exceptions, success followup assertions, cleanup, exact allowed keys/types, secret-marker exclusion, invalid/nonfinite/oversize inputs and fixed fallback output. Test diagnostics never terminate/wait/kill processes or read outputs.

Fresh exit observations prove state only at sampling time, not completion before either deadline or the internal CLI stage. A passing diagnostic execution does not fix, waive or relabel the original failure.

After focused/static/docs review, publish only a temporary draft diagnostic PR against an explicit task/... alias of the already-published candidate to use the repository's supported feature-base CI. Allow one instrumented CI execution and retain source/merge/artifact identity plus its sanitized summary. Do not retry until green, merge this experiment, weaken selectors/assertions or alter main/next. Collect evidence before cleaning only the owned temporary refs.
