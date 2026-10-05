# Recorded GitHub factory fixture

`curie dev github-stub` runs from a source checkout. It needs Python 3, Git,
and OpenSSL. Capture also needs an authenticated `gh` client with read access
to the public upstream repository.

Capture a completed pull request's check lifecycle:

```bash
curie dev github-stub capture --repository curie-eng/curie --pull-request 3400 \
  --output tools/github-stub/recordings/curie-pr-3400.json
```

Capture retains only public repository, pull request and head identity, check
names, lifecycle timestamps, status, conclusion, App slug, and commit-status
context and state. It omits people, URLs, descriptions, output, annotations,
issue bodies and credentials. Capture is restricted to the public upstream.
Review the exact recording before publishing it.

The recording reconstructs each check's lifecycle from GitHub's completed
check rows. A check is absent before its `started_at`, in progress until its
`completed_at`, and then has the captured final result. This is a retrospective
lifecycle reconstruction, not a recording of historical polling responses.
The #3400 recording preserves the delayed Python aggregate and the three
pytest shards that precede it.

Serve it with isolated state:

```bash
curie dev github-stub serve --root "${STUB_STATE_DIR:?set a private execution directory}" \
  --recording tools/github-stub/recordings/curie-pr-3400.json
```

The server prints one JSON startup object containing `base_url`, `clone_url`
and `ca_file`. Configure `api.githubApiUrl` with the HTTPS base plus `/api/v3`
and `api.githubCloneBase` with the HTTPS base. Trust the generated CA with
`SSL_CERT_FILE` for Python clients and `GIT_SSL_CAINFO` for Git. Preserve TLS
verification. The API accepts both root and Enterprise `/api/v3` paths.

The fixture repository is `acme-corp/acme-bot`, repository id 4401, installation
id 5501, and App id 51. Issue 3815 starts labelled `factory` by a human example
actor, `octocat`, id 6601, with write permission. App comments carry Bot identity
and App provenance. Git clone and push use actual Git smart HTTP against a
private bare repository; PR head and base identities follow its real refs.

Mutable fixture state lives in SQLite inside the supplied root. Each execution
requires a fresh root. The server defaults to loopback. To expose it to an
isolated test cluster, use a separate listening address and advertised service
name, for example `--bind 0.0.0.0 --host github-stub.test-3815.svc.cluster.local`.
The generated certificate includes that advertised DNS name or IP address.
Distribute only its public CA certificate to the test clients; keep the CA and
server private keys in this execution's private state directory.
Stop it with SIGINT or
SIGTERM and remove only that execution's owned state. An unsupported call returns
HTTP 501 with `unsupported_request`, is retained in SQLite, and makes the server
exit unsuccessfully. The Python API also raises on `close()` after such a call.

Tests can construct `GithubStub(root, recording)`, call `start()`, set clients'
trust to `ca_file`, advance replay time with `advance(seconds)`, and call `close()`
in teardown. CLI serving advances time automatically. A replay proves local
factory boundary behavior; it does not replace the required live factory and
external integration evidence.
