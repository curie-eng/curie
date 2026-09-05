{{/* Opt-in changes hook lifecycle metadata only, never phase implementations. */}}
{{- define "curie.upgradeRecovery.operation" -}}
{{- $recovery := .Values.upgradeRecovery | default dict -}}
{{- if $recovery.enabled -}}
{{- if not (and .Values.worker.deploy .Values.worker.upgradeDrain.enabled .Values.api.deploy .Values.api.migrate.enabled) -}}
{{- fail "upgradeRecovery requires deployed worker/API and all three drain/migration/release hooks" -}}
{{- end -}}
{{- $operation := $recovery.operationId | default "" -}}
{{- if not (regexMatch "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$" $operation) -}}
{{- fail "upgradeRecovery.operationId must be a fresh UUIDv4 supplied by the transactional CLI" -}}
{{- end -}}
{{- $operation -}}
{{- end -}}
{{- end -}}
