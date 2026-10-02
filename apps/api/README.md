# apps/api

FastAPI server backed by Postgres and RustFS/S3 from the dev compose stack. It
provides:

- agents/versions/deployments CRUD (Create, Read, Update, Delete)
- auth
- plugin bundle validate/store/fetch
- Langfuse proxy endpoints
- GitHub App integration (the git-flow engine that promotes on merge)
- the Langfuse-backed metrics/logs endpoints

## Approval presentation

Approval responses expose a computed `display_summary` for ordinary displays.
The stored `summary`, grant identifier and exact arguments keep their original
meaning. Old permission records receive the same plain display fallback without
a migration. Display wording never determines authorization or grant binding.

Approval metadata references follow the same sentence boundary rules across API,
worker, runner and the UI fallback for older API responses. A tool identifier
followed by a sentence-ending period (including whitespace or closing punctuation)
uses its plain action label. Filenames, extensions, paths, URLs and identifiers
embedded within another word remain literal data, including Unicode text. The
stored summary, exact grant target, arguments and nested content remain unchanged.

## Factory test isolation

Factory integration fixtures use their own disposable Postgres database and
request identities while sharing a test Valkey. Fixture setup must preserve
other requests' CI claims and enqueue markers. Teardown must remove both round
keys and Actions rerun records, including locks, for requests created in the
fixture's database. Cleanup runs after a failing test as well as a passing one,
so a retry begins without a previous attempt's rerun decision. Assertions about
one continuation or one external rerun remain unchanged.
Independent GitHub stand-ins must give independent timeline events distinct
identities, even when tests use the same repository and issue. Repeated reads
and redeliveries inside one fixture keep the same event identity; relabeling
advances it.
