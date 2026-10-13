# GitLab CE forge fixture

A disposable GitLab CE 19.4.1 server with a shell runner, seeded with the
groups, projects, users and tokens the GitLab adapter tests and the
`gitlab+gitlab@fixture` acceptance entry need (#4399).

```
curie dev forge-fixture gitlab up        --state-dir <dir> [--advertise-host <host>] [--port <port>]
curie dev forge-fixture gitlab self-test --state-dir <dir>
curie dev forge-fixture gitlab down      --state-dir <dir>
```

`up` prints the path of `state.json` on stdout and progress on stderr. Point
the fixture-backed tests at it with `CURIE_GITLAB_FIXTURE=<dir>/state.json`.

## Requirements

1. A Docker daemon with about 4 GiB of memory free. GitLab is capped at 4 GiB
   and settles around 2 to 3 GiB; the runner is capped at 512 MiB. The images
   are `gitlab/gitlab-ce:19.4.1-ce.0` and `gitlab/gitlab-runner:v19.4.0`.
2. `docker` with the compose plugin, `openssl`, `git` and `python3` on the
   client. The script uses the Python standard library only.
3. A free port, 28443 by default. `up` refuses a port that already accepts
   connections on `127.0.0.1` or that any container publishes.

`up` takes about 3 minutes with the images already pulled (measured
2026-10-13: first HTTP answer after 130 s, root token 28 s, seeding and runner
registration about 10 s, 169 s in total); the gitlab-mapping spike measured
about 7 minutes on a slower host. GitLab settles at 2 to 3 GiB. `up` gives up
after 15 minutes, prints the last 100 GitLab log lines, and tears everything
down. Setting `CURIE_FORGE_FIXTURE_KEEP_ON_FAILURE=1` leaves a failed fixture
running for debugging; `down` still removes it.

## Remote daemon design

The Docker daemon may be remote (`DOCKER_HOST=ssh://...`), so nothing is
bind-mounted and no host path is referenced. `up` writes the compose file into
the state directory, runs `docker compose create`, copies the TLS material in
with `docker compose cp`, then starts GitLab. After seeding it registers the
runner, copies `config.toml` and the CA into the runner container the same
way, and starts it. State inside the containers lives in named volumes that
carry the compose project label.

The published port is the same number inside and outside the container, and
nginx listens on it, so `external_url` (`https://<advertise-host>:<port>`) is
valid from this machine and from any host that reaches the advertise host.
The fixture assumes ports published on the daemon host are reachable at
`127.0.0.1:<port>` from the client (a local daemon, or a port forwarder).

## TLS

`up` makes a throwaway CA with `openssl` and a server certificate for
`localhost`, `127.0.0.1` and `--advertise-host` (an IP SAN when it parses as an
IP, a DNS SAN otherwise). The CA is `ca.crt` in the state directory. Clients
must trust it explicitly; default trust refuses the server. The runner trusts
it through its `certs/ca.crt` and `tls-ca-file`.

## What is seeded

1. Group `factory-fixture` and subgroup `factory-fixture/nested`.
2. Private project `factory-fixture/nested/widgets` with issues enabled and a
   protected `main`. Its first commit holds `widget.sh`, `test.sh` and a
   `.gitlab-ci.yml` whose untagged `test` job runs `sh test.sh`. `add` in
   `widget.sh` subtracts, so the pipeline on `main` is red; changing `-` to
   `+` on that one line makes it green.
3. Private project `factory-fixture/nested/renamable` with a README, for the
   rename tests.
4. Users `developer` (Developer, 30) and `reporter` (Reporter, 20), members of
   the top group, so `GET /projects/:id/members/all/:user_id` reports the
   inherited level on both projects. Act as them with the root token and a
   `Sudo: <username>` header.
5. A group service account `factory-bot` (Developer). If the server refuses
   group service accounts, a regular user with the same name stands in;
   `users.service_account.kind` says which.
6. Tokens, each valid 30 days: the service account with `api` and
   `write_repository`; `reporter` with `api` and `write_repository`; and a
   second service account token with `read_api` only.
7. An instance runner using the shell executor that runs untagged jobs and
   clones from `https://localhost:<port>`. It is created with
   `POST /api/v4/user/runners` (`runner_type=instance_type`), and its
   `glrt-` authentication token goes into a `config.toml` that is copied into
   the runner container, with the CA as `/etc/gitlab-runner/certs/ca.crt` and
   `tls-ca-file`, before the container starts. `up` waits up to 3 minutes for
   `GET /runners/all?status=online` to list it.

Git authenticates as `oauth2` with a Basic header,
`Authorization: Basic base64(oauth2:<token>)`. GitLab refuses a Bearer header
for git over HTTP.

## state.json

The state directory has mode 0700. `state.json`, `ca.crt` and every token file
have mode 0600. The state directory must not be inside a git repository.

```
{
  "project": "curie-forge-gitlab-<8 hex>",
  "status": "up",
  "base_url": "https://<advertise-host>:<port>",
  "local_base_url": "https://localhost:<port>",
  "api_url": "<base_url>/api/v4",
  "local_api_url": "<local_base_url>/api/v4",
  "ca_path": "<dir>/ca.crt",
  "advertise_host": "<host>",
  "port": 28443,
  "version": "19.4.1",
  "server_version": "<GET /version>",
  "root_token_file": "<dir>/token-root",
  "group": {"id": 0, "path": "factory-fixture"},
  "subgroup": {"id": 0, "path": "factory-fixture/nested"},
  "projects": {
    "widgets": {"id": 0, "path": "factory-fixture/nested/widgets", "default_branch": "main", "http_url_to_repo": "..."},
    "renamable": {"id": 0, "path": "factory-fixture/nested/renamable", "default_branch": "main"}
  },
  "users": {
    "root": {"id": 1, "username": "root"},
    "developer": {"id": 0, "username": "developer", "access_level": 30},
    "reporter": {"id": 0, "username": "reporter", "access_level": 20},
    "service_account": {"id": 0, "username": "...", "access_level": 30, "kind": "group_service_account", "bot": true}
  },
  "tokens": {
    "service_account": {"file": "<dir>/token-service-account", "expires_at": "YYYY-MM-DD", "scopes": ["api", "write_repository"], "token_id": 0},
    "reporter": {"file": "<dir>/token-reporter", "expires_at": "YYYY-MM-DD", "scopes": ["api", "write_repository"], "token_id": 0},
    "read_api_bot": {"file": "<dir>/token-read-api-bot", "expires_at": "YYYY-MM-DD", "scopes": ["read_api"], "token_id": 0, "user_id": 0, "username": "..."}
  },
  "runner": {"id": 0}
}
```

The root token (scopes `api`, `sudo`, `admin_mode`, `read_api`,
`write_repository`) expires after 4 days. Read tokens from their files; they
never appear in `state.json` or in any output.

## self-test

`self-test` prints one line per check and exits 0 only when all pass:

1. `tls`: the API verifies with the fixture CA and default trust refuses it.
2. `widgets-path`: `widgets` has a 3-segment path.
3. `access-levels`: `developer` is 30 and `reporter` is 20 on `widgets`.
4. `git-basic-oauth2`: the service account token clones `widgets` and pushes a
   throwaway branch with the Basic header passed through the
   `GIT_CONFIG_COUNT`/`GIT_CONFIG_KEY_0=http.extraHeader` environment variables
   (never argv) and the CA through `GIT_SSL_CAINFO`; the same push with a
   Bearer header is refused. The branch is deleted afterwards.
5. `runner-job`: an instance runner is online and the pipeline on `main`
   (created by the seed commit) has every job finished on the runner within 5
   minutes. `failed` is the expected result because of the seeded bug.

## API throttle

The rate limit test changes the authenticated API throttle through the
application settings API with the root token, for example
`PUT /api/v4/application/settings` with
`throttle_authenticated_api_enabled=true`,
`throttle_authenticated_api_requests_per_period=5` and
`throttle_authenticated_api_period_in_seconds=60`, and restores
`throttle_authenticated_api_enabled=false` afterwards. The fixture leaves the
throttle off.

## Teardown

`down` runs `docker compose down -v --remove-orphans` for the project, then
lists containers, volumes and networks labelled
`com.docker.compose.project=<project>`. If any listing fails or anything
remains, it exits non-zero and keeps the state directory; otherwise it deletes
the state directory. `down` with a missing state directory exits non-zero.
To check by hand:

```
docker ps -a      --filter label=com.docker.compose.project=<project>
docker volume ls  --filter label=com.docker.compose.project=<project>
docker network ls --filter label=com.docker.compose.project=<project>
```
