# Scripted factory qualification

`curie dev factory-e2e scripted --context kind-curie-e2e --listen-host <gateway-ip>`
runs the existing factory driver against the candidate images already loaded
into a disposable kind cluster. It owns a fresh `test-factory-*` namespace and a distinct
`curie-factory-scripted` release;
the existing ladder release, controller, priority classes and CRDs remain in
place. The driver uses actual API admission, Postgres, worker execution,
sandbox runner, Git clone/push, publication, CI reconciliation and terminal
comment paths. Only GitHub and the model provider are external fixtures.

The repository fixture is `fixtures/unitconv`. A private copy of the default
dark-factory bundle keeps its agents, skills and hooks byte-for-byte, replacing
only `connectors.yaml` with `connectors: {}`: this stdlib Python fixture needs
none of the default layer's uv/Rust/pnpm repository toolchains. Evidence records
the resulting bundle hash and this difference. This qualifies that fixture,
not the unchanged default bundle's release artifact.

The GitHub fixture serves trusted TLS, its actual bare repository, human actor
events and signed webhook deliveries. Its captured CI lifecycle starts at the
first actual PR and runs at 20 times recorded speed; product deadlines are
unchanged. The model endpoint rejects unexpected requests and requires every
recorded exchange to be consumed. Empty recordings, missing actual fixture
preflight/publication paths, or incomplete cleanup fail the command.

The first recording must come from an actual run, never a hand-authored model
conversation. The CI dispatch input `factory_record=true` uses the provider
credential only on the host proxy. Pass an ephemeral public PEM certificate
as `factory_recording_recipient`; the workflow refuses to start without it,
and uploads only CMS DER encrypted bytes for local private review. Never pass
a private key to CI. Review the decrypted recording's request, response and
decoded raw bodies before committing a replay fixture. Ordinary PR CI replays
that reviewed fixture without a provider credential.

Focused checks:

```bash
uv run pytest -q tools/factory-e2e/tests tools/github-stub/tests tools/model-script/tests
uv run ruff check tools/factory-e2e tools/github-stub tools/model-script
```

These checks prove harness controls. The actual kind command and its owned
cleanup must also pass before claiming factory qualification.
