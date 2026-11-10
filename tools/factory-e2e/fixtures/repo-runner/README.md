# Repository runner judge samples

These samples are synthetic recorded-shape expectations for the repo-runner
judge. They are not observations from a live deployment, build, sandbox, or
model request, and they do not show that repository routing or a tool manifest
works on a cluster.

Each file uses the Preflight evidence envelope: its `scenario` field holds the
observation that the offline judge reads. The observation names two runner
repositories in `bindings` (`acme-corp/repo-x` and `acme-corp/repo-y`), one
repository without a runner in `unconfigured` (`acme-corp/repo-u`), the
rendered baseline `worker_pools` map for the two runner repositories, and
seven turns in plan order:

1. X on its own thread completes on X's runner image and manifest.
2. Y on a second thread completes on Y's runner image and manifest.
3. U on a third thread completes on the agent's own image. The turn records the
   agent warm pool, the agent image digests, the `curie-no-tool-manifest` probe
   output, and the runner boot line `runner tool manifest absent: tool_count=0`.
4. Y on X's thread is refused with `workspace.selection_conflict` before any
   model request, claim, sandbox, or pod.
5. X on its first thread completes after the operator points X's worker pool
   at Y's pool entry. It runs on a new claim, sandbox, and pod with Y's image
   and manifest, and X's turn-1 claim is retired.
6. A missing-pool fault is refused with `repo_runner_pool_missing` on a fresh
   thread.
7. A stale-base fault is refused with `repo_runner_base_mismatch` on a fresh
   thread.

Every completed turn records CLI, runner status, repository, capture, and error
evidence, plus the runner container env with
`CLAUDE_CODE_MAX_OUTPUT_TOKENS=16000`. Every turn declares an empty error list.
Runner boot lines use the runner's real phrases, `runner tool manifest loaded:
N tools` and `runner tool manifest absent: tool_count=0`.

`failing.json` is identical except that U's runner imageID equals X's
linux/amd64 digest. A repository without a runner must run on the agent image,
so the judge refuses an agent turn whose image is one of the runner images.

The command `curie dev factory-e2e judge-repo-runner --observation <sample>`
must return zero for `passing.json` and nonzero for `failing.json`.
