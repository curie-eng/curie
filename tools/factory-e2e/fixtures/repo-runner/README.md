# Repository runner judge samples

These samples are synthetic recorded-shape expectations for the repo-runner
judge before its runtime components exist. They are not observations from a
live deployment, build, sandbox, or model request.

`passing.json` describes the evidence a future live collector must obtain:
two repositories run sequentially in one thread with separate sandbox and pod
identities, matching immutable runner images, boot manifest logs, and manifest
contents. The missing-pool and stale-base cases refuse before sandbox creation
or a model request.

`failing.json` describes the same expectation with an unrelated image identity
for the second repository. The command
`curie dev factory-e2e judge-repo-runner --observation <sample>` must return
zero for the passing sample and nonzero for the failing sample.

The boot-log phrase is an expected future format. It does not demonstrate that
the runner currently emits a tool manifest or that repository routing works.
