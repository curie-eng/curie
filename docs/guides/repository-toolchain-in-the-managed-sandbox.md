# Repository toolchain and real checks in the managed sandbox

An agent that edits a repository has to be able to *check* the repository: install
its dependencies, run its documented test command, and report the exit status
before asking for publication. This guide is the supported recipe for doing that
inside a managed Curie sandbox, and — just as importantly — the honest record of
what the sandbox will and will not let you reach while doing it.

Nothing here loosens the sandbox. The recipe works because it puts the
virtualenv where the repository already lives. There is no `docker exec` or
`kubectl exec` fix-up step anywhere in it; if you find yourself reaching for one,
the recipe has been mis-followed.

The executable form of everything below is the committed proof harness
`runner/tests/test_repo_toolchain_proof.py`, which runs each step in a real
container under the real isolation flags. When this guide and that harness
disagree, the harness is right and this file is stale.

## 1. The pinned toolchain

The runner image is built from `runner/Dockerfile`, whose base is
`python:3.13.15-slim-bookworm`, pinned by digest. A rebuild at a given commit
therefore produces the same toolchain. The image carries:

| Tool | Version | Where it comes from |
|---|---|---|
| Python | 3.13.15 | the digest-pinned base image |
| `pip`, `venv` | as shipped with that Python | the base image |
| `git` | 2.39.5 | Debian bookworm packages |
| Node and npm | 22 | copied from the official `node` image, also digest-pinned |
| `curl`, `ca-certificates` | bookworm | Debian bookworm packages |

Those pins move through Dependabot, which raises base-image bumps as pull
requests. That is the supported way the toolchain changes: do not pin a
different base out-of-band, and do not install a toolchain into a running
sandbox and expect it to survive.

A toolchain the image does not carry is not silently unavailable — it fails
loudly. See section 6.

## 2. The sandbox posture the recipe works within

The Docker sandbox driver runs the runner with a read-only root filesystem,
all capabilities dropped (`--cap-drop ALL`), `no-new-privileges`, and a bounded
pid, memory and CPU envelope. The writable paths are exactly three:

| Path | Kind | Notes |
|---|---|---|
| `/tmp` | tmpfs | scratch; **mounted `noexec`** |
| `/home/runner` | tmpfs | the runner's `HOME`; **mounted `noexec`** |
| `/workspace` | bind mount | the repository checkout; read-write and exec-able |

Everything else, `/usr/local/lib` included, refuses writes with
`Read-only file system`. That is deliberate: the image's own site-packages are
not a place for a repository's dependencies.

### Why the virtualenv must go in `/workspace`

This is the single fact that decides whether the recipe works, and the one an
operator hits within a minute of improvising.

The two tmpfs mounts are `noexec`. A virtualenv created under `/tmp` or
`/home/runner` therefore has **console scripts that cannot execute**:
`.../bin/pip`, `.../bin/pytest` and friends fail with `Permission denied`. The
same virtualenv's `bin/python` still works, because that entry is a symlink onto
the exec-able root filesystem — which is exactly what makes the failure
confusing. You get a working `python` and a broken `pip` in the same directory.

The identical virtualenv created under the bind-mounted `/workspace` works
completely, console scripts included. So the recipe is: **make the virtualenv
where the repository already is.**

The Kubernetes sandbox driver renders those same writable paths as `emptyDir`
volumes, which are exec-able. A `/tmp` recipe would therefore pass on a cluster
and fail on local Compose — a difference that would only ever be discovered by
whoever ran it locally last. `/workspace` is correct on both, so it is the only
location this guide documents.

## 3. The recipe

Two profiles. **Profile A is the default**, because it is the only one that
satisfies "only the egress this repository needs" exactly.

### Profile A — vendored dependencies, no registry egress

Stage the dependency wheels into the workspace and install from there:

```sh
python -m venv /workspace/.venv
/workspace/.venv/bin/pip install --disable-pip-version-check \
    --no-index --find-links /workspace/.wheels <pinned requirements>
```

Required package-registry egress: **none**. `--no-index` forbids contacting an
index at all and `--find-links` points pip at the local wheelhouse, so the
install completes with the container on `--network none`. That is not an
argument that it needs no egress; it is a proof by construction, and the proof
harness runs it that way.

How the wheelhouse gets there is the repository's business — committed wheels, a
build step from committed source, or a wheelhouse staged into the workspace
before the sandbox starts. The proof harness's fixture builds its one dependency
from committed source with a stdlib-only wheel builder, which keeps the offline
closure honest without committing a third-party binary.

Then run the repository's **own documented** check command from `/workspace`.
For the harness fixture that command is:

```sh
cd /workspace && /workspace/.venv/bin/python -m unittest discover -s tests -t . -v
```

Use the command the repository documents, not one you invented for it. That is
also what the publication contract requires of the coder.

### Profile B — live registry

```sh
python -m venv /workspace/.venv
/workspace/.venv/bin/pip install --disable-pip-version-check <pinned requirements>
```

Identical apart from where the packages come from, and it needs egress. See
section 4 before choosing it.

## 4. Egress

`security.networkPolicy.allowedEgress` in the chart is **fail-closed and empty by
default**, and entries are additive: nothing reaches a package registry until an
operator says so.

Profile A needs no entry at all.

Profile B needs pip to reach **both** `pypi.org` (the index) and
`files.pythonhosted.org` (the files). The only operator lever that can carry an
arbitrary destination is:

```sh
curie cluster up --allow-web-egress <CIDR>
```

`--allow-egress-host` cannot do it: it accepts a **closed enum of model
providers** (`anthropic`, `openrouter`, `zhipu`, `moonshot`, `deepseek`) and
resolves their hostnames at install time. A registry is not in that enum, so the
resolve-a-hostname path is structurally unavailable and you are left writing
CIDRs by hand.

**State plainly what that means.** [ADR-0075](../adr/0075-the-agent-proxy-credential-and-egress-boundary.md)
records that there is no package-manager story: enforcement is CIDR-based while
registry hosts are CDN-fronted with rotating IPs. Any CIDR range you write for
`pypi.org` or `files.pythonhosted.org` is therefore **broader than this
repository needs, and it will drift** as the CDN's addressing changes. Profile B
is a known-imprecise fallback. It is not a minimal allowlist, and this guide will
not describe it as one. The "trusted registries" preset ADR-0075 contemplates is
not implemented; there is no such flag or chart value today.

**Edge case worth knowing before you need it:** `curie doctor` can recommend a
`cluster up` re-run as remediation, and a re-run applies the allowlist it is
given. Registry CIDRs added by an earlier, differently-flagged invocation are not
reproduced by a later one, so a doctor-suggested re-run can silently narrow an
allowlist back. Keep the flags you used with the rest of your install
configuration and pass them again.

## 5. The credential boundary

**GitHub write credentials never enter the sandbox.** They stay outside it, in
the worker:

- the worker redeems the deployment-derived GitHub credential and performs the
  clone itself, outside the sandbox;
- before the workspace is accepted, `remote.origin.url` is rewritten to the
  credential-free URL and `.git/config` is scanned for credential material — a
  checkout that still carries any is refused, not mounted;
- what the sandbox receives is a checkout with no usable push credential in it.

So an agent inside the sandbox cannot publish, and is not asked to try. The
in-sandbox affordance is the built-in `publish_changes` tool, whose own
description says, in these words, **"Do not push with git"**, and which requires
the coder to identify the repository's documented check command, run it from
`/workspace`, and report the exact command and its exit status before requesting
publication. The tool never publishes anything itself: the platform captures a
patch, asks for human approval in the requesting thread, and publishes from a
separate trusted job only after that approval. Calling the tool directly grants
no capability — it returns an error and mutates nothing.

## 6. Bounded, truthful failures

Both failure modes an operator actually meets are non-zero, terminating, and say
what went wrong. Neither is ever reported as success, and under the publication
contract the coder must report the failure and **not** publish.

**Wrong or unreachable registry.** pip exhausts its retries and exits non-zero
with the requirement named:

```text
ERROR: Could not find a version that satisfies the requirement <name> (from versions: none)
ERROR: No matching distribution found for <name>
```

No `Successfully installed` line is printed. The step terminates well inside its
timeout rather than hanging.

**Missing toolchain.** A check command whose toolchain the image does not carry
fails immediately with a `command not found`-class error and a non-zero status —
for example `poetry: not found`. It is not swallowed into a silent pass.

## 7. Applicability

What the committed evidence does and does not cover.

| Surface | Applicability | Evidence |
|---|---|---|
| local (Docker sandbox driver) | **Supported and proved.** | The proof harness runs every step in the real runner image under the driver's own isolation flags: vendored install, the documented check red → green → red across a fix and its revert, both negative controls, and a read-only-rootfs control. |
| cluster (Kubernetes sandbox driver) | **Applies by posture, not proved here.** | The chart renders the same writable paths (`/tmp`, `/home/runner`) as `emptyDir`, and `/workspace` is the same mounted checkout, so the venv-in-`/workspace` recipe is posture-identical. No cluster-tier run is included in this evidence. |
| live provider | **Not covered.** | The harness makes no model call. The recipe is about the toolchain, not the session. |
| slack | **Not covered.** | No Slack external-integration run is included. The publication approval a Slack thread would carry is asserted statically here, not exercised. |
| GitHub | **Boundary asserted statically.** | The credential handling in section 5 is read from the worker's workspace acquisition path and the publication tool's contract; no live human publication approval was exercised. |
| Profile B against an enforcing NetworkPolicy | **Documented, not proved.** | Proving that a hand-written `--allow-web-egress` CIDR really admits PyPI requires a cluster with an enforcing NetworkPolicy. |
| repeat in a newly acquired workspace, and after restart/handoff | **Open.** | Not demonstrated by this evidence. Treat it as unproved rather than as working. |

Every container the harness starts is `--rm` and every workspace it creates is a
temporary directory; it asserts observably that neither survives the run.
