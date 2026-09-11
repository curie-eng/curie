# 152. Registry destinations are operator-declared, not a platform preset

Date: 2026-09-11

Status: Draft

This Draft proposes to supersede, if accepted, only the package-manager preset
commitment in
[ADR-0075](0075-the-agent-proxy-credential-and-egress-boundary.md). That
commitment is already conditional on FQDN capability. The proposal leaves
ADR-0075's Agent Proxy, credential boundary, per-agent scoping, and audit intent
unchanged.

## Context

ADR-0075 says Curie will ship a curated "trusted registries" bundle on top of
FQDN-aware enforcement. Its ordering is sound: the preset is conditional on
the FQDN capability. The open question is whether Curie should commit to
maintaining a multi-registry bundle after that capability arrives.

That bundle is a product and maintenance boundary of its own. Package managers
cross registry, archive, distribution, redirect, mirror, and shared-hosting
boundaries that change independently. One successful PyPI installation does
not justify a maintained list for npm, PyPI, crates.io, apt archives, ghcr, and
their peers.

FQDN enforcement is not the system available today. The current NetworkPolicy
boundary admits IP addresses and CIDRs. The named-provider option resolves a
hostname during installation and stores `/32` routes, while arbitrary
destinations require an operator-supplied `--allow-web-egress <CIDR>` value.

An install-time DNS answer is an address snapshot. It does not constrain later
traffic by hostname, and it can drift when a CDN changes addresses. Shared CDN
addresses may also serve destinations outside the intended registry. Calling
such a snapshot a trusted-registry or hostname boundary would overstate what
the enforcement layer proves.

The [managed-sandbox toolchain guide](../guides/repository-toolchain-in-the-managed-sandbox.md)
therefore describes live registry dependencies as a known-imprecise fallback.
The reproducible
[registry-egress check](../../scripts/check-registry-egress.py) creates a
disposable Calico kind cluster from the current chart, resolves live PyPI
addresses into `/32` TCP/443 allowances, and exercises denied, admitted, and
revoked pip and direct-TCP controls. Its generated output records the address
snapshot and result of that invocation. A successful run proves that the
selected addresses admitted the selected flow at that time; it does not prove a
durable registry or domain boundary.

## Decision

**If accepted, Curie will not carry a standing commitment to ship a
platform-maintained "trusted registries" preset, including after FQDN-aware
enforcement arrives.** Under this proposal, operators declare the destinations
their repositories require. Curie does not approximate a preset by resolving
registries during installation, storing the resulting CIDRs, or describing
those CIDRs as hostname-scoped enforcement.

Bundled dependencies, with no live registry egress, remain the default managed
sandbox strategy. Live registry dependencies remain available to operators who
explicitly provide raw CIDRs through `--allow-web-egress`. Documentation and
operator output identify that route as known-imprecise and subject to address
drift. It is not a trusted-registries preset.

A future platform-maintained preset requires a separate explicit decision. It
must define the supported ecosystems and destinations, redirect and mirror
behavior, ownership, update cadence, security review, compatibility policy,
and live proof. The FQDN-aware proxy substrate still requires its own Accepted
ADR, implementation, and live positive and negative enforcement checks. This
proposal does not select the proxy topology, TLS behavior, domain matching
rules, or any registry contents.

If this Draft is accepted, it supersedes only ADR-0075's commitment to
ship the package-manager preset after FQDN capability. ADR-0075's accepted
direction for proxy-held credentials, separate destination and credential
bindings, per-agent scoping, and complete audit events remains intact. While
this ADR is Draft, ADR-0075 stays unchanged and Accepted, with no status or
backlink edit.

## Consequences and security limits

- Operators retain an explicit live-registry escape hatch, but they must own
  the selected CIDRs and refresh them when registry addressing changes.
- A raw CIDR can admit unrelated services sharing an address or range. Curie
  cannot make a comprehensive registry or domain claim for that path.
- A narrow `/32` may fail closed after address rotation. A broader range may
  reduce failures while admitting more destinations. Neither tradeoff creates
  hostname enforcement.
- Bundled dependencies avoid this boundary by construction and remain the
  precise default for repository toolchains whose dependencies can be vendored.
- Future FQDN proxy evidence must remain separate from current CIDR admission
  evidence. A successful live-registry admission check cannot stand in for
  proxy implementation or live proxy enforcement checks.
- Operator-declared FQDN destinations become the package-manager path after the
  proxy arrives. Curie does not own their completeness or freshness.
- Any future curated preset must earn and maintain its own support boundary. It
  is a reconsideration through a new decision, not an implied proxy feature.

## Alternatives considered

1. **Retain ADR-0075's existing conditional preset commitment.** Rejected
   because FQDN enforcement makes domain lists possible but does not establish
   which ecosystems, redirects, mirrors, distributions, or archive versions
   Curie can maintain. The existing promise is conditional, but still broader
   than the evidence and named ownership.
2. **Ship a preset made from install-time `/32` resolutions.** Rejected because
   it would label a temporary address snapshot as a hostname boundary and would
   inherit CDN drift and shared-address exposure.
3. **Ship and maintain broad registry CIDR lists.** Rejected because the lists
   would be operationally brittle, broader than the named registries, and still
   unable to prove which domain received traffic.
4. **Remove live registry dependencies and require bundling everywhere.**
   Rejected because an operator may knowingly accept the imprecision for a
   repository that cannot bundle its toolchain dependencies. The raw CIDR
   control makes that choice explicit.
5. **Treat the future Agent Proxy as available now.** Rejected because
   ADR-0075 deliberately leaves the substrate and TLS decision to a follow-up.
   Architecture intent is not an implemented enforcement boundary.
