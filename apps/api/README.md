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
