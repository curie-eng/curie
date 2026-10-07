# Memory census example contract

This optional utility reads the current memory store and emits one complete JSON
Lines snapshot for an operator to retain or compare. It does not schedule itself,
retain history, choose an alert threshold, or change platform behavior. Run it in
an environment with the Python database dependencies already installed and a
`DATABASE_URL` for the intended PostgreSQL database. Do not pass credentials as
command arguments.

## MC001: Snapshot source and output

1. The utility reads the current `curie.workflow_state_entries` table, selecting
   only rows whose `namespace` is `memory`, and joins each `agent_id` to
   `curie.agents.id` for the agent name. This is a read-only snapshot; it does not
   create, update, or delete database records.
2. For every selected row it emits a JSON object with
   `memory_census: "entry"`, `agent`, `key`, and `bytes`. `bytes` is the
   PostgreSQL byte length of the JSONB value rendered as text. It never emits
   `value`, contents, credential material, or another state namespace.
3. For every agent with at least one selected row it emits a JSON object with
   `memory_census: "agent"`, `agent`, and `rows`. A final object contains
   `memory_census: "total"`, `rows`, and `agents`. An empty store emits only
   `{"memory_census":"total","rows":0,"agents":0}`. Entry and agent objects
   have deterministic agent-name and key ordering.
4. The utility buffers the entire result before writing stdout. If connection,
   query, decoding, or collection fails, it exits with status 1, writes no
   success objects to stdout, and writes a JSON error with a type-only stable
   `error` value to stderr. It never prints the exception message or URL.
   Missing `DATABASE_URL` is also a typed error.

A lower row count on a later snapshot detects a count change. It does not prove
content integrity, show which deletion occurred, or detect every deletion. The
operator owns scheduling, snapshot retention, comparison, and alert policy.
