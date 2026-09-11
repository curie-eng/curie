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
`python:3.13.15-slim-bookworm`, pinned by digest, with Node copied in from the
official `node` image, also pinned by digest. A rebuild at a given commit
therefore reproduces the Python and Node layers exactly. The Dockerfile also
`apt-get install`s `git`, `curl`, and `ca-certificates` from the Debian
bookworm archive with no version or digest pin, so those packages are not
part of that guarantee: the archive moves under a later rebuild of the same
commit, and the image can pick up a different `git`/`curl`/`ca-certificates`
version with no change to the repository at all.

| Tool | Version | Where it comes from | Reproducible from the commit? |
|---|---|---|---|
| Python | 3.13.15 | the digest-pinned base image | Yes |
| `pip`, `venv` | as shipped with that Python | the base image | Yes |
| Node and npm | 22 | copied from the official `node` image, also digest-pinned | Yes |
| `git` | 2.39.5, as observed at build time | Debian bookworm packages | No |
| `curl`, `ca-certificates` | bookworm, as observed at build time | Debian bookworm packages | No |

In practice: an operator whose checks depend on a specific `git`, `curl`, or
CA bundle version cannot rely on the image's commit alone to pin it, since
those come from whatever the Debian archive serves on the day of the rebuild.

Those base-image pins move through Dependabot, which raises digest bumps for
the Python and Node base images as pull requests; it does not pin the apt-
installed packages. That is the supported way the digest-pinned toolchain
changes: do not pin a different base out-of-band, and do not install a
toolchain into a running sandbox and expect it to survive.

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
volumes, which are exec-able. That divergence is measured, not inferred: probing
a cluster-tier sandbox on 2026-09-11 found `/tmp`, `/home/runner` and
`/workspace` all exec-able, and the virtualenv's console scripts ran from every
one of them. A `/tmp` recipe would therefore pass on a cluster
and fail on local Compose — a difference that would only ever be discovered by
whoever ran it locally last. `/workspace` is correct on both, so it is the only
location this guide documents.

### Nothing in the sandbox survives a pod replacement

Read this before you have work worth losing.

The recipe's writable paths are all `emptyDir` at cluster tier and tmpfs or a
bind mount locally. None of them is storage. A restart that keeps the *pod* —
the runner container dying and being restarted in place — preserves everything:
the checkout, its history, the head, the origin, the virtualenv. A restart that
replaces the **pod** preserves none of it.

**On a pod replacement your workspace is reset to the head the platform
published, and everything the sandbox produced is gone:**

| | in-place container restart | pod replacement |
|---|---|---|
| repository present, credential-free origin | preserved | re-fetched, so present |
| head | preserved | **reset to the archive head** |
| commits made in the sandbox, not published | preserved | **lost** |
| uncommitted edits and untracked files | preserved | **lost** |
| the virtualenv, and anything under `/tmp` or `/home/runner` | preserved | **lost** |

This is structural, not a gap to be fixed by being careful. A replacement pod
re-materializes `/workspace` through `workspace-init`, which fetches the signed
archive the trusted worker built from *its own* clone of the remote. Nothing
anywhere captures a live sandbox's `/workspace` back into an archive, so there
is no mechanism by which in-sandbox work could survive. `workspace-init` also
erases `/workspace` before extracting, deliberately, so that an interrupted
extraction can never be overlaid.

Three ordinary things replace the pod, and only the first is obvious:

- the late workspace handoff of ADR-0136, when a conversation acquires a
  repository after it has already started;
- suspend/resume (ADR-0003 suspend *is* pod deletion), and any eviction, node
  drain or rescheduling — here the claim, the route and the session id are all
  unchanged, so there is no claim-level event that says work was discarded;
- any pod restart that is not an in-place container restart.

So: **the only durable place for work is publication.** `publish_changes` is not
a nicety at the end of a task, it is the step that makes the work exist outside a
pod. Commit early if you like, but understand that a commit is not durability
here — an unpublished commit dies with the pod exactly as an uncommitted edit
does.

One consequence worth knowing when a sandbox will not come back: the signed
workspace reference is short-lived (five minutes by default). A pod recreated
after it expires cannot re-fetch at all — `workspace-init` exits non-zero with
`workspace-fetch: signed reference expired` and the pod sits in
`Init:CrashLoopBackOff`. That is the platform failing closed rather than serving
a stale workspace; recovery is a fresh claim, which the worker prepares with a
new reference.

### Is a persistent workspace volume the answer?

Not today, and not one flag away. `SandboxClaim.spec.volumeClaimTemplates`
exists in the CRD, but nothing in Curie sets it, and the shipped runner
`SandboxTemplate` leaves `volumeClaimTemplatesPolicy` at its CRD default of
`Disallowed` — a claim that asks for a volume is refused outright with
`VolumeClaimTemplatesError`, creating neither pod nor PVC.

Allowing it does not get you a persistent workspace either. With the policy
flipped and a PVC bound at `/workspace`, the volume genuinely does survive a pod
replacement — and `workspace-init` then wipes it on the new pod's first start,
because its unconditional clean is what guarantees a partial extraction is never
overlaid. Persisting the workspace would mean changing that, and trading away
the property the wipe exists to provide.

The platform's answer to durability is elsewhere and is already in the recipe:
committed work becomes durable by publication, and conversation state survives
in the replayed history the replacement runner boots from.

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

**Wrong or unreachable registry.** pip exits non-zero and names the unsatisfied
requirement:

```text
ERROR: Could not find a version that satisfies the requirement <name> (from versions: none)
ERROR: No matching distribution found for <name>
```

No `Successfully installed` line is printed. Under the chart's fail-closed
egress default the same command talking to real `pypi.org` is also a refusal
(`Network is unreachable`, exit 1) — it is not a hang. What *looks* like a hang
is pip's default retry budget: `retries=5` at a 15s socket timeout, across every
resolved address.

| What you ran | Where | Wall clock | What bounded it |
|---|---|---|---|
| `.venv/bin/pip install requests==2.32.3` (pip defaults) | live kind cluster, chart fail-closed egress, `curie-runner:0.8.7`, 2026-09-11 | **~368 s**, exit 1, `[Errno 101] Network is unreachable (pypi.org:443)` | pip default retries=5, not Curie |
| wrong-but-reachable index (`--index-url https://192.0.2.1/simple`) | same cluster proof | **~8 s** | connection refused / TEST-NET |
| missing toolchain (`poetry install`) | same cluster proof | **well under 1 s**, exit 127 | shell `command not found` |
| `git push` to unreachable github.com | same cluster proof | **~135 s**, exit 128 | git's own HTTP retry |
| default-index pip, `PIP_RETRIES=0` | local analog, `pypi.org` pinned to TEST-NET-1 so DNS "succeeds" and TCP sits on the socket timeout | **18.6 s** (venv + one 15s connect) | image `/etc/pip.conf` |
| default-index pip, `PIP_RETRIES=0` | local analog, `--network none` (fails at DNS) | **2.8 s** | image `/etc/pip.conf` |

The runner image now ships `/etc/pip.conf` with `timeout = 15` and `retries = 0`,
copied after the image's own pip installs so a flaky registry during the *build*
still gets pip's default retries. Workspace venvs created at runtime read that
site-wide file, so the naive Profile B command — the one an agent actually types —
fails on the first unreachable attempt instead of spending six minutes of a run
budget discovering the same ENETUNREACH. Override with `--retries` / `PIP_RETRIES`
when a reachable index is flaky. Released `curie-runner:0.8.7` does **not** carry
this file; that is the image the 368s number was measured against.

**Missing toolchain.** A check command whose toolchain the image does not carry
fails immediately with a `command not found`-class error and a non-zero status —
for example `poetry: not found`. It is not swallowed into a silent pass.

## 7. Applicability

What the committed evidence does and does not cover.

| Surface | Applicability | Evidence |
|---|---|---|
| local (Docker sandbox driver) | **Supported and proved.** | The proof harness runs every step in the real runner image under the driver's own isolation flags: vendored install, the documented check red → green → red across a fix and its revert, both negative controls, and a read-only-rootfs control. |
| cluster (Kubernetes sandbox driver) | **Supported and proved.** | Run on a kind cluster against the released `curie-runner:0.8.7` image on 2026-09-11: two independently claimed sandboxes each installed the dependency offline and ran the documented check red → green → red. See `ac5-cluster-evidence/`. |
| live provider | **Not covered.** | The harness makes no model call. The recipe is about the toolchain, not the session. |
| slack | **Not covered.** | No Slack external-integration run is included. The publication approval a Slack thread would carry is asserted statically here, not exercised. |
| GitHub | **Boundary asserted statically.** | The credential handling in section 5 is read from the worker's workspace acquisition path and the publication tool's contract; no live human publication approval was exercised. |
| Profile B against an enforcing NetworkPolicy | **Refusal proved; admission not.** | Under the chart's fail-closed default, a live-registry install fails truthfully (`Network is unreachable`, exit 1). Unconfigured pip on `curie-runner:0.8.7` took **~368 s**; the image now ships `/etc/pip.conf` with `retries = 0` so the same command fails on the first attempt (see section 6). That a hand-written `--allow-web-egress` CIDR then *admits* PyPI is still unproved. |
| repeat in a newly acquired workspace, and after an in-place restart | **Supported and proved.** | A second, independently claimed sandbox reproduced the whole recipe from the acquired head with no state carried over, producing its own distinct commits. An in-place container restart preserved the workspace, its three-commit history, the expected head and the credential-free origin. See `ac5-cluster-evidence/`. |
| after a pod-replacing handoff | **Proved — and it discards everything the sandbox made.** | Exercised on a kind cluster on 2026-09-11 in both shapes: the ADR-0136 cold-claim handoff, and a pod deleted under an unchanged live claim. Both re-fetched the signed archive, so the repository and its credential-free origin came back — at the **archive head**, with the in-sandbox commit, the uncommitted edits, the untracked files and the virtualenv all gone. Recorded for operators under *Nothing in the sandbox survives a pod replacement*. See `pod-replacing-handoff-evidence/`. |
| a persistent workspace volume | **Not the answer; not supported today.** | Nothing in Curie sets `SandboxClaim.spec.volumeClaimTemplates`, and the shipped template leaves the policy at the CRD default `Disallowed`, so such a claim is refused with no pod and no PVC. With the policy flipped, the PVC does survive the pod replacement and `workspace-init` wipes it anyway. A persistent workspace would require changing that wipe. |

Every container the harness starts is `--rm` and every workspace it creates is a
temporary directory; it asserts observably that neither survives the run.
