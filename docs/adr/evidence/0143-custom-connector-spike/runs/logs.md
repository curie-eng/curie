# Connector and provider logs

## connector stdout after turn 2
```
{"event": "startup", "api_base": "http://stubfin-api:8080", "credential_present": true, "tmp_writable": false, "tmpdir": null, "argv": [], "ts": "2026-09-04T16:44:51Z"}
{"event": "token_persisted", "store": "stubfin-credentials", "key": "FIN_REFRESH_TOKEN", "attempt": 1, "ts": "2026-09-04T16:45:43Z"}
{"event": "token_refresh", "ok": true, "upstream_status": 200, "duration_ms": 31, "rotated": true, "expires_in": 45, "ts": "2026-09-04T16:45:43Z"}
{"event": "tool_call", "tool": "list_invoices", "ok": true, "upstream_status": 200, "duration_ms": 46, "period": "2026-Q2", "ts": "2026-09-04T16:45:43Z"}
```

## connector stdout after the restart and turn 3
```
{"event": "startup", "api_base": "http://stubfin-api:8080", "credential_present": true, "tmp_writable": false, "tmpdir": null, "argv": [], "ts": "2026-09-04T16:46:22Z"}
{"event": "token_persisted", "store": "stubfin-credentials", "key": "FIN_REFRESH_TOKEN", "attempt": 1, "ts": "2026-09-04T16:46:47Z"}
{"event": "token_refresh", "ok": true, "upstream_status": 200, "duration_ms": 21, "rotated": true, "expires_in": 45, "ts": "2026-09-04T16:46:47Z"}
{"event": "tool_call", "tool": "invoice", "ok": true, "upstream_status": 200, "duration_ms": 34, "invoice_id": "INV-2026-042", "ts": "2026-09-04T16:46:47Z"}
```

## negative: RoleBinding removed, turn 4
```
== remove the write-back grant (the state that lives only in the cluster)
rolebinding.rbac.authorization.k8s.io "stubfin-connector-token" deleted from curie-adr namespace
== wait past the 45s access-token life so the next call must refresh
message exit=0
finalized True
I couldn't retrieve the invoices — the finance system returned an error.

```
Error executing tool list_invoices: could not store the reissued refresh token in
Secret stubfin-credentials: 403 {"kind":"Status","apiVersion":"v1","metadata":{},
"status":"Failure","message":"secrets \"stubfin-credentials\" is forbidden: User
\"system:serviceaccount:curie-adr:default\" cannot patch resource \"se.
The provider has already retired the previous token, so the new one exists only in
this process. Do not restart this connector until the Role or Secret is fixed.
```

**What happened:** The `stubfin` connector tried to reissue its refresh token but was denied permission to update the `stubfin-credentials
== connector log
{"event": "token_refresh", "ok": true, "upstream_status": 200, "duration_ms": 21, "rotated": true, "expires_in": 45, "ts": "2026-09-04T16:46:47Z"}
{"event": "tool_call", "tool": "invoice", "ok": true, "upstream_status": 200, "duration_ms": 34, "invoice_id": "INV-2026-042", "ts": "2026-09-04T16:46:47Z"}
{"event": "token_refresh", "ok": false, "rotated": true, "persisted": false, "error": "could not store the reissued refresh token in Secret stubfin-credentials: 403 {\"kind\":\"Status\",\"apiVersion\":\"v1\",\"metadata\":{},\"status\":\"Failure\",\"message\":\"secrets \\\"stubfin-credentials\\\" is forbidden: User \\\"system:serviceaccount:curie-adr:default\\\" cannot patch resource \\\"se. The provider has already retired the previous token, so the new one exists only in this process. Do not restart this connector until the Role or Secret is fixed.", "ts": "2026-09-04T16:49:05Z"}
== secret vs provider
secret: rt-0003
provider: {"current_refresh": "rt-0004", "retired": ["rt-0001", "rt-0002", "rt-0003"], "exchanges": 3, "reads": 2, "rejected_exchanges": 1}
== restore the grant
```

## recovery: RoleBinding restored, turn 5
```
role.rbac.authorization.k8s.io/stubfin-connector-token unchanged
rolebinding.rbac.authorization.k8s.io/stubfin-connector-token created
message exit=0
finalized True
Here are the invoices for **2026-Q3**:

| Invoice | Customer | Amount | Status |
|---|---|---|---|
| INV-2026-057 | Contoso Ltd | 4,100.00 | open |

That's the only invoice the finance system holds for this period.

_What I changed:_
• called `Skill` — non-idempotent tool completed
== connector log tail
{"event": "token_persisted", "store": "stubfin-credentials", "key": "FIN_REFRESH_TOKEN", "attempt": 1, "ts": "2026-09-04T16:50:47Z"}
{"event": "token_refresh", "ok": true, "upstream_status": 200, "duration_ms": 6, "rotated": true, "expires_in": 45, "ts": "2026-09-04T16:50:47Z"}
{"event": "tool_call", "tool": "list_invoices", "ok": true, "upstream_status": 200, "duration_ms": 16, "period": "2026-Q3", "ts": "2026-09-04T16:50:47Z"}
secret: rt-0005
provider: {"current_refresh": "rt-0005", "retired": ["rt-0001", "rt-0002", "rt-0003", "rt-0004"], "exchanges": 4, "reads": 3, "rejected_exchanges": 1}
```

## runner log line during turn 1 (connector crash-looping)
```
{"logger":"curie_runner.mcp_tool_capability","message":"MCP tool-capability probe failed server=stubfin; keeping approval tool: unhandled errors in a TaskGroup (1 sub-exception)","service.name":"curie-runner","severity":"WARNING","span_id":null,"timestamp":"20
```

## runner log line during turn 2 (surface classified read-only)
```
curie_runner | request_approval omitted: observed MCP surface has no actionable tools tool_count=2 probe_complete=True failures=0
```
