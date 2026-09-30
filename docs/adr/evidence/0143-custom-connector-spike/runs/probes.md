# Other probes

## curie skill check on the bundle
```json
{"check":"mcp-load","declared":[],"hints":[],"matches":[],"plugin_dir":"/plugin","reasons":[],"registered":[],"verdict":"green","version":1}

```

## unknown keys in connectors.yaml
```
$ curie build --plugin-dir <copy with serviceAccount: and reaches: added>
Error: parse connectors.yaml
$ curie build --plugin-dir <same> --json
{"error":"parse connectors.yaml: connectors.stubfin: unknown field `serviceAccount`, expected one of `image`, `build`, `args`, `env`, `port`, `unhosted_url`, `url`, `headers`, `secrets`, `sealed_secrets`, `secret_files` at line 18 column 5","fix":null}

```

## ENTRYPOINT probe (docker, same image with an ENTRYPOINT added, container args = [python, /app/other.py])
```
{"event": "startup", "api_base": "http://x", "credential_present": false, "tmp_writable": true, "tmpdir": "/tmp", "argv": ["python", "/app/other.py"], "ts": "2026-09-04T16:41:56Z"}
```

## read-only root filesystem probe (docker --read-only --user 65532, the rendered securityContext)
```
mkstemp FAILED: FileNotFoundError [Errno 2] No usable temporary directory found in ['/tmp', '/var/tmp', '/usr/tmp', '/app']
/dev/shm writable: True
```

## active deployment rows after three deploys
```
active deployment rows for the agent: 3 of 3
```

## released API: no credential-broker routes
```
65 paths; oauth/grant/capability routes: []
```

## worker env on the released install
```
env names matching RECONCILE or CONNECTOR: CURIE_PUBLICATION_RECONCILE_MAX_ATTEMPTS, CURIE_PUBLICATION_RECONCILE_INTERVAL_SECONDS (no CURIE_CONNECTOR_RECONCILE)
```
