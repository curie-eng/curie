# Bitbucket Data Center response captures

Anonymized response bodies of Bitbucket Data Center 9.4 under `rest/api/latest`.
`forge_fakes/bitbucket_dc.py` answers in these shapes, and
`test_bitbucket_dc_fake.py` fails when a fake payload drifts from them. The
quickstart stub (`cli/src/factory_quickstart/bitbucket_dc.rs`) serves some of
them too.

The first set was written from Atlassian's Bitbucket Data Center 9.4 REST
reference, https://developer.atlassian.com/server/bitbucket/rest/v904/intro/
(API groups: system maintenance for `application-properties`, repository,
pull requests, builds and deployments). Replace each file with the response the
Data Center fixture (`curie dev forge-fixture bitbucket-dc up`) returns for the
same call, anonymized:

1. Hosts become `bbdc.example.invalid`.
2. Display names become `placeholder-user-<n>` and email addresses
   `<slug>@example.invalid`.
3. The project stays `FAC` and the repository `demo`, as the fixture seeds them.
4. No token, cookie or `Authorization` value is ever kept.

| File | Request |
|---|---|
| `application-properties.json` | `GET /rest/api/latest/application-properties` |
| `repos.json` | `GET /rest/api/latest/repos` |
| `repository.json` | `GET /rest/api/latest/projects/FAC/repos/demo` |
| `default-branch.json` | `GET .../repos/demo/default-branch` |
| `branches.json` | `GET .../repos/demo/branches?filterText=...` |
| `commit.json` | `GET .../repos/demo/commits/<sha>` |
| `pull-request.json`, `pull-requests.json` | `GET .../pull-requests/<id>`, `GET .../pull-requests?at=...&direction=OUTGOING&state=ALL` |
| `pull-request-merge.json` | `GET .../pull-requests/<id>/merge` |
| `activities.json` | `GET .../pull-requests/<id>/activities` |
| `comment.json`, `comment-markup.json` | `GET .../pull-requests/<id>/comments/<id>` without and with `?markup=true` |
| `builds.json` | `GET .../repos/demo/commits/<sha>/builds?key=<key>` (one status; without `key` it answers 400) |
| `build-status-commit.json` | `GET /rest/build-status/latest/commits/<sha>` (every key on the commit, server-wide) |
| `error-*.json` | the error body of that status (`error-400-build-key.json`: the repository builds read without `key`) |

Confirmed on the 9.4.24 fixture: the repository builds `GET` needs `key` (400
without it) and returns that one status, so the build files above are the
fixture's answers, anonymized. A reply adds no activity of its own and an edit
adds no `EDITED` activity: a reply appears only nested in its root's
`comments`, and a reply under an inline root carries that root's `anchor`, so
`activities.json` holds no `REPLIED` entry and the nested reply in it,
`comment.json` and `comment-markup.json` has an `anchor`. Still to confirm: the
`X-RateLimit-*` header names on a 429.
