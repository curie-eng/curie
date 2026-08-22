# action-ledger demo

Everything needed to reproduce the recording, and the honest boundary around what
it proves.

## What is real and what is not

**Real:** the connector
(`examples/sre-bot/connectors/k8s-scale/server.py`), including its allowlist,
its ceiling, the `deployments/scale` subresource path and every refusal branch.
The `claude_agent_sdk` message types. The ACI translate seam
(`runner/src/curie_runner/translate.py::translate_message`), which is what turns
one tool call into the two frames the record is built from. The API
(`apps/api/src/curie_api/routers/actions.py`) answering over HTTP against a real
Postgres, including the conflict check and the audit write. The receipt renderer
(`apps/worker/src/curie_worker/blocks.py::receipt_card`), whose output the demo
prints as text rather than posting to Slack.

**Not real:** the Kubernetes API server, which is a dict holding one number. No
cluster, no network, no model. The receipt is printed rather than rendered by
Slack, so what you see is the card's content and not its appearance.

**Not wired yet:** the worker does not post the record; the demo calls
`POST /actions` itself, standing in for the kernel branch that will. And the
platform does not execute the restore. The API rules on the undo and the demo
performs the connector call the ruling authorizes, because nothing in the
platform can reach a connector today: neither the api nor the worker has an MCP
client. Deciding where that executor lives is open work, and the ADR says so.

## Reproduce it

Bring up a Postgres and run the API from source against it:

```bash
curie local up
DATABASE_URL="postgresql+asyncpg://postgres:postgres@localhost:25432/postgres" \
API_KEY=curie-dev-key AWS_ACCESS_KEY_ID=rustfs AWS_SECRET_ACCESS_KEY=rustfssecret \
S3_ENDPOINT_URL=http://localhost:29000 BUNDLE_BUCKET=curie-bundles \
  uv run --project apps/api uvicorn curie_api.main:app --host 127.0.0.1 --port 28999
```

Then, in another shell:

```bash
uv run --project apps/worker --with pyyaml python demo/action-ledger/run_demo.py
```

Re-record with:

```bash
asciinema rec --command "uv run --project apps/worker --with pyyaml python demo/action-ledger/run_demo.py" \
  --window-size 104x34 --overwrite demo/action-ledger/undo.cast
agg --speed 1.4 --idle-time-limit 1 demo/action-ledger/undo.cast docs/demo/adr-0117-undo.gif
```

## What the five steps show

1. The connector reports the replica count it read on the way past. Today's write
   connector reads the same thing and discards it.
2. The runner emits one frame for the call and one for its result, and the API
   records two actions. One carries a snapshot and one does not, and neither is
   special-cased: the second is not undoable because its connector answered in
   prose.
3. The receipt puts both on one card. The irreversible line states its reason.
4. The undo is ruled on by the API and the recorded prior state goes back.
5. The same button, after a human changed the target by hand, is refused with
   both states named, and the manual fix survives. This is the step the feature
   lives on; without it an undo control is a way for the platform to overwrite an
   operator.
