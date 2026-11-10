# Scripted Anthropic Messages endpoint

`curie dev model-script` runs from a source checkout. It needs Python 3.

Serve a transcript. Each request selects an unused exchange by the scrubbed
text of its first user message and the number of assistant messages in its
conversation. This keeps interleaved implementer and reviewer conversations
in separate lanes. System prompts, tool definitions, model names and later
messages do not affect matching. The scrubber replaces volatile ids, commit
hashes, timestamps, plain dates, decimal durations in seconds and URLs.
A lane or ordinal the transcript does not contain returns HTTP 422 and the
process exits unsuccessfully with an unexpected model request.

```bash
curie dev model-script serve --transcript tools/model-script/transcripts/unitconv-issue.json \
  --host 0.0.0.0 --port 8080
```

The process prints one JSON object with `base_url`. Point the worker at that
URL with `CURIE_MODEL_BASE_URL`. The SDK appends `/v1/messages`.

Record by proxying to an Anthropic-compatible provider. The default upstream
is `https://openrouter.ai/api`. Set `CURIE_FACTORY_RECORD_KEY` in the recorder's
environment. The recorder uses that key for the upstream `x-api-key` header
and preserves the Anthropic version and beta headers. Client authentication
headers do not supply the upstream key.

```bash
curie dev model-script record --output /tmp/factory-transcript.json --host 0.0.0.0 --port 8080
```

Stop either mode with SIGINT or SIGTERM. Review a recorded transcript before
publishing it: replace anything that names a private repository with obvious
placeholders. Each exchange stores only `lane_key`, `ordinal` and the assembled
response `message`. Request bodies, system prompts, raw provider responses and
authentication headers are not stored.
