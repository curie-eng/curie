# Memory census

Run a read-only snapshot with Curie's Python database dependencies installed:

```sh
python examples/memory-census/memory_census.py
```

Supply `DATABASE_URL` through your secret-management mechanism. Use a database
role with SELECT on `curie.agents` and `curie.workflow_state_entries`, and USAGE
on the `curie` schema. Keep the URL out of command arguments and logs.

The utility emits JSON Lines containing entry names and byte lengths, per-agent
row counts, and a total. It never emits memory contents. Names and keys can still
be sensitive; retain the output in your installation's monitoring system.
A failed collection exits with a fixed JSON error on stderr and no successful
snapshot on stdout.

The [contract](docs/CONTRACT.md) describes the snapshot and its limits. Scheduling,
historical comparison, and alert delivery are operator integrations. A lower
row count detects a count change; it does not establish content integrity or
catch every deletion.

The repository test suite collects `examples/tests/test_memory_census.py`.
Runtime qualification additionally needs isolated PostgreSQL with the actual
current migrations, synthetic memory and other-namespace rows, a read-only
role, an empty snapshot, and a failed connection. Boundary tests alone do not
establish that integration.
