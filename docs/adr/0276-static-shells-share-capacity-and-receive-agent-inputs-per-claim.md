# 276. Static shells share capacity and receive agent inputs per claim

Date: 2026-11-08

Status: Draft

Amends [ADR 0009](0009-per-agent-connector-auth.md) for the static substrate only.
This Draft needs maintainer acceptance. The implementation follows the explicit
maintainer ruling to share static capacity while preserving immutable bundle code.

## Context

A static shell starts a fresh runner for each claim and can receive an archive
before that runner exists. It does not have Kubernetes' value-only claim and
startup-only Secret reference constraints. Baking agent secrets into shell
environments nevertheless tied authenticated agents to dedicated idle pools.
It also retained those credentials between claims.

## Decision

Static agents share the generic pool unless an operator explicitly assigns
an agent its own pool through the existing pool settings. Pool size follows
concurrent claims rather than the number of deployed agents. Kubernetes
selection and credential handling remain unchanged.

The worker sends agent-specific runtime configuration and credentials in each
claim archive. The root supervisor writes runner-owned mode `0400` files in
the fresh claim tree. Its launcher restores a new environment before executing
the runner, which retains the existing BootEnv parser and held-secret inventory.
The worker's `CURIE_AGENT_CREDENTIAL_FILES` declaration supplies named external
tool credentials from files without changing the frozen bundle format.

Bundle code and immutable plugin templates arrive in the same claim archive
but retain their root ownership and read-only permissions. Executable code
retains execute bits. The runner cannot modify or chmod that code. The root
supervisor removes it with the credential files at retirement.

A deployment-wide model credential may remain on a shell because it is common
to that deployment's agents. The connector CA is shared public trust material,
not agent authorization. Connector secrets, caller and upload tokens, identity,
endpoints and agent-specific runtime configuration never remain on an idle shell.

`StaticPoolSandboxClient.create_claim`, `SandboxSubstrate._claim_pool`,
`curie_shell_supervisor.stage.prepare`, `curie_shell_supervisor.launch` and the
runner's existing `collect_held_secrets` realize the delivery boundary. Retirement
uses the existing process sweep, claim-tree wipe, scratch restoration and
quarantine checks. Exact claim and generation fencing remains authoritative
when consecutive leases belong to different agents.

## Rejected alternatives

1. Require one pool per authenticated agent. This reserves idle capacity and
   retains agent credentials in shells when no claim needs them.
2. Put agent credentials or identity in shared shell environment defaults.
   This lets a later claim inherit a different agent's configuration.
3. Give immutable bundle code to the runner UID. Its owner could chmod and
   rewrite that code despite a read-only initial mode.
4. Add another runner boot parser or a frozen bundle-format field for external
   bearers. The shell launcher and existing named-secret substitution suffice.
5. Change Kubernetes delivery at the same time. Its provisioning and Secret
   constraints differ and need their own reviewed change.

## Consequences and verification

Workers need access to per-agent credential source files. Credentials are read
on each new claim; rotating a file does not update an already-running claim.
Explicit dedicated pools retain an isolation option without changing defaults.

Supervisor tests exercise private claim files, malformed input refusals and
wipe. Static qualification alternates agents on one shell and drives the real
runner with a scripted model to check configuration and redaction. Container
proofs check runner UID permissions and alternate agents before the existing
worker kill matrix. A failure to prove retirement still quarantines capacity.
