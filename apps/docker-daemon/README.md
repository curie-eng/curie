# Curie per-run Docker daemon

The image a Docker-enabled factory run reaches instead of a host Docker socket
(ADR 0217, decision 4). It is the stock `docker:dind` image with three
additions:

1. `curie-docker-authz`, a Docker authorization plugin built from this crate,
   started by the entrypoint before dockerd and served on
   `/run/docker/plugins/curie-authz.sock`.
2. An entrypoint that starts dockerd as `dockerd
   --authorization-plugin=curie-authz --userns-remap=default <your
   arguments>`. The two flags come right after `dockerd`, before every caller
   argument, so no value-taking caller flag (`--label`, `-H`, ...) can swallow
   them. It refuses (exit 1) any caller `--userns-remap` option
   other than the exact duplicate `--userns-remap=default`, since a later
   remap flag would override the injected one. It also refuses `--config-file`
   in either spelling, since a caller config file would replace
   `daemon.json` and its entitlements. The daemon starts through
   `curie-docker-authz cgroup-isolate -- dockerd-entrypoint.sh dockerd ...`,
   which unshares the cgroup and mount namespaces, remounts cgroup2 on
   `/sys/fs/cgroup`, checks that `/proc/self/cgroup` is `0::/`, and only then
   execs dockerd. It refuses to start when `/sys/fs/cgroup` is not cgroup2.
   containerd gives a privileged container no private cgroup namespace (a
   Kubernetes privileged pod), so without this step dockerd would see the
   node's cgroup tree. A command whose first argument is a flag is a daemon
   start too (`dockerd` is implied); any other command (a shell, a client
   command) runs unchanged without the plugin.
3. `daemon.json`, which turns off the BuildKit `network.host`,
   `security.insecure` and `device` entitlements. A BuildKit build travels
   over `POST /grpc`, which an authorization plugin cannot read, so the daemon
   itself has to refuse those.

## Policy

The plugin is an allowlist. A request is allowed only if its method and path
are in the endpoint table and, for the five bodies that carry privilege
(container create, exec create, network create, network connect, volume
create), every inspected field passes. Anything it cannot classify or parse is
refused. Each refusal names a stable rule id, for example `privileged`,
`cap-add`, `host-namespace`, `host-bind`, `devices`, `sysctls`,
`security-opt`, `cgroup-parent`, `log-driver`, `runtime`, `volume-driver`,
`volume-driver-options`, `network-driver`, `network-driver-options`,
`exec-privileged`, `image-import`, `form-body`,
`endpoint-not-allowed`, `field-unknown`, `field-ambiguous`, `body-required`,
`body-unparseable`, `uri-invalid` and `request-unparseable`. `src/policy.rs` is the source of
truth for the table and the rules.

`form-body` refuses any request, on every endpoint, whose `Content-Type` is
`application/x-www-form-urlencoded` or `multipart/form-data`. dockerd forwards
only JSON bodies to the plugin, but moby reads query parameters with Go's
`r.FormValue`, which also reads form bodies (body values win over the URL
query). A form body would be a second parameter channel the plugin cannot see.

The forwarded Content-Type is not the whole story: dockerd forwards only the
last value of a repeated Content-Type header, while Go's form parser reads the
first. So the two endpoints whose query parameters carry privilege do not trust
it. `POST /images/create` refuses any Content-Type header (rule `form-body`; a
pull sends none), and the classic builder's `POST /build` and
`POST /build/cancel` are not in the table at all (`endpoint-not-allowed`).
BuildKit builds from Docker CLI 29, compose and buildx use `POST /grpc` and
`POST /session` instead.

The client sees `authorization denied by plugin curie-authz: refused by rule
<rule>: <detail>`.

Refusals that surprise people:

1. `docker inspect <name>` without `--type` on a name that does not exist
   falls through to `GET /plugins/<name>/json`, which is not allowed. Pass
   `--type container`, `--type image` and so on.
2. `--cap-add` is limited to Docker's default capability set. Dropping
   everything and adding a default one back (`--cap-drop ALL --cap-add
   NET_BIND_SERVICE`) works; `SYS_ADMIN`, `NET_ADMIN` or `ALL` do not.
3. `docker build --network=host` with BuildKit fails with a daemon error naming
   `network.host`, not a plugin rule, because the daemon entitlement setting
   refuses it.
4. `DOCKER_BUILDKIT=0 docker build` (the classic builder) fails with
   `endpoint-not-allowed` on `POST /build`. Use BuildKit, the default.

## Residual: BuildKit over /grpc

`docker build` needs `POST /grpc` and `POST /session`, so both are allowed.
BuildKit's control API on `/grpc` is a gRPC stream over a hijacked
connection, opaque to an authorization plugin. Two things bound what a build
can ask for. `daemon.json` turns off the `network.host`, `security.insecure`
and `device` entitlements, which the daemon itself enforces. And an LLB cgroup
parent resolves inside dockerd's own cgroup namespace, whose root is the
daemon container's cgroup, so it stays inside the daemon pod's limits. That
holds whatever the container runtime does, because the entrypoint always
starts dockerd through `cgroup-isolate` and that step refuses to exec dockerd
unless the namespace and the cgroup2 remount are in place.

## Build, run, test

Build the image (both arches cross compile on the build platform):

    docker build -t curie-docker-daemon apps/docker-daemon

Run it as the ADR does. The entrypoint adds both required flags itself, so the
default command is plain `dockerd` and the duplicates below are harmless; add
listeners as needed:

    docker run -d --privileged curie-docker-daemon \
      dockerd --userns-remap=default --authorization-plugin=curie-authz \
      -H tcp://0.0.0.0:2375 --tls=false

An explicit `dockerd` command skips the dind image's automatic TLS certificate
setup, so whoever starts the daemon must pass the mTLS flags (`--tlsverify
--tlscacert --tlscert --tlskey` with `-H tcp://0.0.0.0:2376`); the plain tcp
2375 form above is for local testing only.

Unit tests for the plugin, from `apps/docker-daemon`:

    cargo test --locked

End to end escape test against a built image (starts its own daemon and client
containers on a private network and removes them afterwards):

    bash apps/docker-daemon/tests/e2e.sh curie-docker-daemon

Extra arguments after the image are passed to the inner dockerd, for example
`--registry-mirror=https://mirror.gcr.io`.

CI builds both arches and runs the unit tests and `tests/e2e.sh` in the
`docker-daemon` job of `.github/workflows/ci.yaml`. Publishing to
`ghcr.io/curie-eng/curie-docker-daemon` happens in `.github/workflows/release.yaml`
after merge.
