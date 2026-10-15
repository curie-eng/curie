# Factory preparation resources

The `skills/prepare-factory-ticket` folder contains the text-only preparation
workflow for a developer's existing coding agent. Install it directly with
`curie factory prepare install --agent codex` (or `claude-code`, `copilot`,
`gemini`, `cursor`). Add `--global` for personal installation across projects.
The installer copies only that skill folder and its references, offline.
It requires no Python interpreter,
parser installation, container runtime or reachable Curie deployment.

The `src/curie_factory_preparation` library is the factory-owned structural
contract for managed evaluation and contributor verification. It is not a client
installation prerequisite. Future deployment evaluation uses the isolated policy
boundary in ADR 0222; platform applications must not import the library directly.
No deployed admission path consumes this library in this release.

The library exports `parse_specification` and `parse_ticket_set`. They preserve
contract version 1, exact UTF-8 body fingerprints and safe `spec_*`/`set_*`
diagnostics. Structural validity does not establish resolved intent, substantive
coverage, merged prerequisites, usable verification routes or deployment readiness.
Ticket-set fingerprints must come from actual canonicalization; the preparation
skill must never invent a digest or claim a parser/evaluator ran.

Contributor checks run from the Curie source workspace:

```bash
uv run pytest examples/tests/test_factory_specification.py -q
```

The first local skill release writes reviewed drafts with deployment readiness
unvalidated. Connected validation and explicit handoff remain #4488 and #4490.
See the factory bundle README for rendering and provider-specific installation.
