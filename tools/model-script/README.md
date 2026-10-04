# Scripted Anthropic Messages endpoint

`curie dev model-script` runs from a source checkout. It needs Python 3.

Serve a transcript. Requests match by normalized content, so a plan reviewer
call and a diff reviewer call are different entries even when both are
subagent turns. Volatile ids, commit hashes, timestamps, and URLs are ignored.
A request the transcript does not contain returns HTTP 422 and the process
exits unsuccessfully.

```bash
curie dev model-script serve --transcript tools/model-script/transcripts/unitconv-issue.json \
  --host 0.0.0.0 --port 8080
```

The process prints one JSON object with `base_url`. Point the worker at that
URL with `CURIE_MODEL_BASE_URL`. The SDK appends `/v1/messages`.

Record by proxying to an Anthropic-compatible provider. The default upstream
is `https://openrouter.ai/api`. The server forwards `x-api-key` and does not
write it into the transcript.

```bash
curie dev model-script record --output /tmp/factory-transcript.json --host 0.0.0.0 --port 8080
```

Stop either mode with SIGINT or SIGTERM. Review a recorded transcript before
publishing it: normalize or drop anything that names a private repository.
