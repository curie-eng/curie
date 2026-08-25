# ADR-0122 spike: retiring a bootstrap token

Throwaway. Answers the one question review asked that could not be answered on
paper: what "the runner stops accepting the bootstrap" means concretely.

`server_adopt_spike.py` is `runner/src/curie_runner/server.py` with two changes,
both marked `SPIKE`:

- the auth middleware reads the token from a holder per request instead of a
  closure captured at app construction (the file's own comment said "The
  configured token is invariant for the process");
- a gated `POST /v1/adopt` swaps the holder's value.

One active credential, replaced outright. No set, no overlap window.

## Running it

No cluster and no image rebuild: bind-mount the patched module over the one in
the image.

```bash
docker run -d --name adopt-spike -p 7300:8080 \
  -v "$PWD/server_adopt_spike.py:/app/.venv/lib/python3.13/site-packages/curie_runner/server.py:ro" \
  -v "/path/to/bundle:/bundle:ro" \
  -e CURIE_PLUGIN_DIR=/bundle -e CURIE_FAKE_MODEL=1 -e CURIE_RUNNER_PORT=8080 \
  -e CURIE_SESSION_ID=spike -e CURIE_SANDBOX_ID=spike-box \
  -e CURIE_RUNNER_TOKEN=BOOTSTRAP-AAA \
  -e CURIE_BUDGET='{"max_output_tokens_per_run":100000,"max_usd_per_day":5}' \
  ghcr.io/curie-eng/curie-runner:dev

python3 test.py       # adoption and retirement, 8 checks
python3 fail.py       # what a failed adoption leaves behind, 8 checks
python3 inflight.py   # rotation during a live turn (needs a real model)

`pin.py` needs no cluster and no runner: it drives the shipped
`SandboxSubstrate.adopt` with fakes to show that an adopted thread presents
the token its route recorded rather than a rotated source. Run it from the
repo root so the worker package resolves:

```
uv run python prototypes/adr-0122-pool-token/pin.py
```
```

`inflight.py` needs turns slow enough to rotate underneath, so point the runner
at a local Ollama instead of the fake model: `CURIE_MODEL=qwen2.5:0.5b`,
`ANTHROPIC_BASE_URL=http://host.docker.internal:11434`, and any
`CURIE_CREDENTIALS` value.

## Results

All three files pass. The bootstrap returns 401 immediately after adoption and
cannot re-adopt; a turn that was genuinely running (`/status` reporting
`turn_active: true`) completed with HTTP 200 across the swap, because the gate
runs at admission in middleware rather than continuously; and every malformed
adopt returns 400 leaving the current credential working.

What this does not cover, and what stays unresolved in the ADR: generation skew
during a pool roll, and concurrent pool creation. Both need a cluster and the
reconciler that does not exist yet.
