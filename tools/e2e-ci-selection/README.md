# Upgrade matrix timing verdict

`@spec CI-UPGRADE-MATRIX-BUDGET` — The required cluster upgrade matrix must
still run and pass whenever selected. The `E2E required` job must reject a
failed, skipped, or missing selected matrix result. Its wall-clock check
records the shared image-build duration and the longest matrix-run duration in
the job summary, with a visible warning when their sum exceeds 20 minutes.
Time alone does not fail an otherwise successful matrix: GitHub runner speed
and old-release upgrade latency vary independently of the candidate's
correctness. A completed shard can omit its matrix-run step timestamps for a
few seconds after the job finishes, so that one miss is fetched again. An
unreadable, truncated, or still-missing jobs payload fails the timing step,
because it cannot produce a trustworthy measurement.

This keeps the outcome gate distinct from the timing signal. It uses the
warning option allowed by [#2823](https://github.com/curie-eng/curie/issues/2823)
without removing any upgrade scenario or weakening the selected-outcome check.
