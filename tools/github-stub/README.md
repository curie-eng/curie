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
  --recording tools/github-stub/recordings/curie-pr-3400.json \
  --seed-tree tools/factory-e2e/fixtures/unitconv \
  --issue-file tools/factory-e2e/fixtures/unitconv-issue.md
```

The server prints one JSON startup object containing `base_url`, `clone_url`
and `ca_file`. Configure `api.githubApiUrl` with the HTTPS base plus `/api/v3`
and `api.githubCloneBase` with the HTTPS base. Trust the generated CA with
`SSL_CERT_FILE` for Python clients and `GIT_SSL_CAINFO` for Git. Preserve TLS
verification. The API accepts both root and Enterprise `/api/v3` paths.

The required `--seed-tree` directory becomes the repository's initial Git tree,
and the required `--issue-file` Markdown supplies the initial issue body. The
fixture repository is `acme-corp/acme-bot`, repository id 4401, installation
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
An unknown bearer on `GET /user` returns 401 `{"message":"Bad credentials"}` and
appends `GET /user` to `rejected_auth`. The bearer itself is not stored. That
rejection is not an unsupported request, so `unknown_requests` stays empty and
`close()` does not treat it as a failure. A known bearer returns 200 and appends
nothing to either list.

Tests can construct `GithubStub(root, recording, seed_tree=seed, issue_file=issue)`,
call `start()`, set clients' trust to `ca_file`, advance replay time with
`advance(seconds)`, and call `close()` in teardown. CLI serving advances time
automatically. A replay proves local
factory boundary behavior; it does not replace the required live factory and
external integration evidence.

Two more seeded accounts exist for learned rule scenarios. `RULE_WRITER`, id 6630,
login `rule-writer`, has write permission. `RULE_READER`, id 6631, login
`rule-reader`, has read permission. `GET /user/{id}` (also under `/api/v3`)
answers any seeded or added collaborator by numeric id with its user object, and
answers 404 for an unknown id, as GitHub does
(https://docs.github.com/en/rest/users/users#get-a-user-using-their-id).
`add_issue_comment` and `append_comment` take an optional `author_association`
such as `MEMBER` or `COLLABORATOR`, and a comment row carries it only when given.

A pull request's conversation behaves like GitHub's: `GET` and `POST` on
`issues/{n}/comments` work for a pull request number even though the stub keeps
no issue row for it, and both comment helpers accept a pull request number.
Comments on a pull request link to `/pull/{n}`. `merge_pull(number)` fast-forwards
`main` to the pull request head, marks the pull request closed and merged with
`merged_at` and `merge_commit_sha`, and returns the fresh pull request. It raises
`ValueError` when the head is not a fast-forward of the base or the pull request
is not open. Pull request reads carry `merged_at`, `draft` and `updated_at`.
