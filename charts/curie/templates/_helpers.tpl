{{/*
Shared template helpers for the Curie umbrella chart.

Naming: every backing store's Service name is derived here so both the store's
own template and its consumers (Langfuse, the OTel Collector) agree. When a
store is BYO (`<dep>.deploy: false`), the helper returns the operator-supplied
host instead of the in-cluster Service name. This is the single-block BYO idiom
lifted from Langfuse's chart: flip `deploy` and fill `host` on the same block.
*/}}

{{- define "curie.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "curie.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "curie.labels" -}}
app.kubernetes.io/name: {{ include "curie.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
{{- end -}}

{{/* Stable labels for backing StatefulSet pod templates. Release metadata
     stays on the owning objects, while chart and application version labels
     stay out of pod templates so metadata updates do not restart data pods. */}}
{{- define "curie.statefulPodLabels" -}}
app.kubernetes.io/name: {{ include "curie.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Component selector labels. Pass a dict with "root" (the top context) and
     "component" (the component name). */}}
{{- define "curie.selectorLabels" -}}
app.kubernetes.io/name: {{ include "curie.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/* Resolve one named placement class. Pass a dict with "root" (the top
     context) and "class" (the placement class name). Every class lookup in the
     chart goes through this helper rather than indexing the placement values
     directly, so that a legacy release whose retained values carry
     `placement: null` (issue #2008) degrades to the chart's empty defaults
     instead of crashing the render. Helm's coalescing deletes a null-valued key
     outright, so when `helm upgrade --reuse-values` replays the stored config of
     a release created before placement classes existed, the placement map is nil
     -- even though values.yaml defines all five classes -- and every direct
     per-class dereference in a template would panic on that nil. The
     empty-dict substitution on the class lookup itself likewise covers a
     placement map that is present but missing this class.

     The kind checks are the fail-CLOSED half of that tolerance, and they are
     not optional: this helper hands its result to consumers as YAML, and
     Helm's `fromYaml` on a non-map document returns an error map rather than
     raising, so a malformed class (say `placement.platform: spot`) would
     resolve `.podLabels`, `.annotations`, and `.nodeSelector` to nothing and
     render clean -- silently dropping every scheduling constraint the operator
     meant to apply and letting workloads land on unintended nodes. The
     pre-#2008 templates dereferenced the class directly and aborted on that
     shape; refusing here keeps that behavior while still degrading a *nil*
     placement to the chart's empty defaults. The refusal lives in the template
     rather than in `values.schema.json` because that schema is deliberately
     permissive and does not type `placement` at all (see charts/curie/CLAUDE.md),
     so a template-level refusal is this chart's established backstop for the
     gap. Note the kind tests use `kindIs`/`kindOf` and not truthiness: an
     `and $class (...)` guard would read a `false` or `0` class as absent and
     default it away, which is the same fail-open bug in a different costume. */}}
{{- define "curie.placement.class" -}}
{{- $classes := .root.Values.placement | default dict -}}
{{- if not (kindIs "map" $classes) -}}
{{- fail (printf "placement must be a map of placement classes, got %s" (kindOf $classes)) -}}
{{- end -}}
{{- $class := index $classes .class -}}
{{- if kindIs "invalid" $class -}}
{{- $class = dict -}}
{{- else if not (kindIs "map" $class) -}}
{{- fail (printf "placement.%s must be a map of placement fields (podLabels, annotations, nodeSelector, tolerations, affinity), got %s" .class (kindOf $class)) -}}
{{- end -}}
{{- toYaml $class -}}
{{- end -}}

{{- define "curie.placement.labels" -}}
{{- with (fromYaml (include "curie.placement.class" .)).podLabels }}
{{- toYaml . }}
{{- end }}
{{- end -}}

{{- define "curie.placement.annotations" -}}
{{- with (fromYaml (include "curie.placement.class" .)).annotations }}
{{- toYaml . }}
{{- end }}
{{- end -}}

{{- define "curie.placement.spec" -}}
{{- $class := fromYaml (include "curie.placement.class" .) -}}
{{- with $class.nodeSelector }}
nodeSelector:
{{- toYaml . | nindent 2 }}
{{- end }}
{{- with $class.tolerations }}
tolerations:
{{- toYaml . | nindent 2 }}
{{- end }}
{{- with $class.affinity }}
affinity:
{{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{/* Secret name that carries all credential material. */}}
{{- define "curie.secretName" -}}
{{- printf "%s-secrets" (include "curie.fullname" .) -}}
{{- end -}}

{{/* Dedicated namespace for short-lived publication resources. */}}
{{- define "curie.publicationNamespace" -}}
{{- default (printf "%s-%s-publication" .Release.Namespace (include "curie.fullname" .)) .Values.worker.publication.namespace | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* ---- Reserved connector-secret boot-env names (#457, ADR-0009) ----
     The non-CURIE_-prefixed runner credential keys a per-agent connector
     secret must never declare, kept in list-parity with the Python source of
     truth in packages/plugin-format (module reserved_env). This is the
     unavoidable second copy -- Helm cannot import Python -- so the completeness
     pin apps/worker/tests/binding/test_reserved_boot_env_pin.py parses THIS
     define's body and fails CI if the two lists drift.

     IMPORTANT: the pin scans this body for env-name-shaped uppercase tokens, so
     the body must contain EXACTLY these eight keys and no other stray ones (this
     comment lives OUTSIDE the define so it is never scanned): the four runner
     credential keys plus the four redirect/capture-capable keys (#487). The whole
     CURIE_ namespace is fenced separately by the hasPrefix rule in the
     connector-secret guard, so it is intentionally absent here. Emitted
     space-separated for consumption via `splitList " "`. */}}
{{- define "curie.reservedConnectorSecretNames" -}}
ANTHROPIC_BASE_URL ANTHROPIC_API_KEY CLAUDE_CODE_OAUTH_TOKEN ANTHROPIC_AUTH_TOKEN HTTPS_PROXY HTTP_PROXY NODE_EXTRA_CA_CERTS ANTHROPIC_CUSTOM_HEADERS
{{- end -}}

{{/* ---- Backing-store hosts (in-cluster Service name, or BYO host) ---- */}}

{{- define "curie.postgres.host" -}}
{{- if .Values.postgres.deploy -}}
{{- printf "%s-postgres" (include "curie.fullname" .) -}}
{{- else -}}
{{- required "postgres.deploy is false: set postgres.host to your external Postgres" .Values.postgres.host -}}
{{- end -}}
{{- end -}}

{{- define "curie.valkey.host" -}}
{{- if .Values.valkey.deploy -}}
{{- printf "%s-valkey" (include "curie.fullname" .) -}}
{{- else -}}
{{- required "valkey.deploy is false: set valkey.host to your external Valkey/Redis" .Values.valkey.host -}}
{{- end -}}
{{- end -}}

{{/* Whether consumers reach Valkey over TLS, as the literal string "true" or
     "false" (#2315). ONE helper, included from BOTH curie.env.valkey and
     curie.langfuse.env: the bug class this chart has hit twice (#2052, #2327)
     is "two consumer groups read the same valkey.* field and only one of them
     was updated", and a shared helper makes that divergence structurally
     impossible rather than merely tested against.

     Why the guard. The in-chart valkey/valkey:8-alpine StatefulSet is started
     with --requirepass and serves no TLS listener, so rendering TLS against it
     would break every consumer at once behind a perfectly healthy-looking
     manifest and no failing preflight -- silent and total. Refuse at render
     time instead, naming both keys and both ways out.

     Why toString. Go templates read any non-empty string as truthy, so a
     quoted "false" arriving from --set-string would otherwise turn TLS on
     against a cleartext store; this is the same scar curie.managedSecret
     carries for security.allowDevDefaults. A nil -- a --reuse-values upgrade
     of a release created before this key existed coalesces it away -- is not
     "true" either, and renders the pre-change "false". */}}
{{- define "curie.valkey.tls" -}}
{{- $tls := eq (toString .Values.valkey.tls) "true" -}}
{{- if and $tls .Values.valkey.deploy -}}
{{- fail "valkey.tls is true but valkey.deploy is also true: the in-chart Valkey serves no TLS listener, so every consumer would fail to connect. Set valkey.deploy=false and point valkey.host at your external TLS store, or leave valkey.tls=false." -}}
{{- end -}}
{{- $tls -}}
{{- end -}}

{{- define "curie.clickhouse.host" -}}
{{- if .Values.clickhouse.deploy -}}
{{- printf "%s-clickhouse" (include "curie.fullname" .) -}}
{{- else -}}
{{- required "clickhouse.deploy is false: set clickhouse.host to your external ClickHouse" .Values.clickhouse.host -}}
{{- end -}}
{{- end -}}

{{/* URL scheme for the ClickHouse endpoint Langfuse connects to (#2314).
     Same shape as curie.rustfs.scheme (#1447): an explicit clickhouse.scheme
     wins, otherwise a BYO server on the conventional ClickHouse HTTPS port
     (8443) is assumed to speak TLS and everything else stays cleartext. The
     chart-owned server is always http. */}}
{{- define "curie.clickhouse.scheme" -}}
{{- $scheme := .Values.clickhouse.scheme | default "" -}}
{{- if $scheme -}}
{{- if not (has $scheme (list "http" "https")) -}}
{{- fail (printf "clickhouse.scheme must be either \"http\" or \"https\", got %q" $scheme) -}}
{{- end -}}
{{- $scheme -}}
{{- else if and (not .Values.clickhouse.deploy) (eq (printf "%v" .Values.clickhouse.httpPort) "8443") -}}
https
{{- else -}}
http
{{- end -}}
{{- end -}}

{{/* The one HTTP endpoint every ClickHouse consumer uses. Call sites include
     this helper rather than re-composing scheme://host:port (#2314). */}}
{{- define "curie.clickhouse.httpUrl" -}}
{{- include "curie.clickhouse.scheme" . }}://{{ include "curie.clickhouse.host" . }}:{{ .Values.clickhouse.httpPort }}
{{- end -}}

{{/* Langfuse's ClickHouse migration DSN, on the native port. Deliberately
     bare: Langfuse's `up.sh` appends its own query string
     (`${CLICKHOUSE_MIGRATION_URL}?username=...`), so anything added here would
     produce two `?` and break the migration. TLS is selected out of band by
     CLICKHOUSE_MIGRATION_SSL (curie.clickhouse.migrationSsl). */}}
{{- define "curie.clickhouse.migrationUrl" -}}
clickhouse://{{ include "curie.clickhouse.host" . }}:{{ .Values.clickhouse.nativePort }}
{{- end -}}

{{/* Whether the Langfuse migration connection on the native port uses TLS
     (#2314). Tracks the HTTP scheme: a TLS ClickHouse endpoint terminates TLS
     on both ports. */}}
{{- define "curie.clickhouse.migrationSsl" -}}
{{- if eq (include "curie.clickhouse.scheme" .) "https" -}}
true
{{- else -}}
false
{{- end -}}
{{- end -}}

{{- define "curie.clickhouse.loggingConfig" -}}
<clickhouse>
  <logger>
    <level>{{ .Values.clickhouse.logLevel }}</level>
    <!-- Also to stdout, so `kubectl logs` works. The image logs only to
         files under /var/log/clickhouse-server/, which in Kubernetes means
         the diagnostics live inside the container and die with the pod,
         exactly when you most want them. Cheap at `warning`. -->
    <console>1</console>
  </logger>
  <!-- text_log is KEPT, and level-filtered rather than removed.
       The image ships <text_log><level>trace</level></text_log>, and that
       `level` is the whole problem: an hour of it produced 253,727 Debug
       and 58,186 Trace rows against 15 Information. Filtering to
       `{{ .Values.clickhouse.logLevel }}` drops the volume by ~4 orders of
       magnitude while keeping the table queryable, and a queryable
       text_log is precisely what diagnosed the incident this fixes. It also
       outlives the pod, which the log file does not. -->
  <text_log>
    <level>{{ .Values.clickhouse.logLevel }}</level>
    <ttl>event_date + INTERVAL {{ .Values.clickhouse.systemLogs.retentionDays }} DAY DELETE</ttl>
  </text_log>
{{- if .Values.clickhouse.systemLogs.enabled }}
  <!-- Profiling and per-second resource sampling, on but TTL-bounded. -->
  <trace_log><ttl>event_date + INTERVAL {{ .Values.clickhouse.systemLogs.retentionDays }} DAY DELETE</ttl></trace_log>
  <metric_log><ttl>event_date + INTERVAL {{ .Values.clickhouse.systemLogs.retentionDays }} DAY DELETE</ttl></metric_log>
  <asynchronous_metric_log><ttl>event_date + INTERVAL {{ .Values.clickhouse.systemLogs.retentionDays }} DAY DELETE</ttl></asynchronous_metric_log>
{{- else }}
  <!-- Removed. Unlike text_log these have no level filter; they sample on
       a timer regardless of whether anything is happening, so the only
       lever is on/off. metric_log was the table whose merge wedged and
       started the spiral. Prometheus and node metrics already cover what
       they measure, from outside the process that is failing. -->
  <trace_log remove="1"/>
  <metric_log remove="1"/>
  <asynchronous_metric_log remove="1"/>
{{- end }}
  <!-- Kept regardless: low volume, useful when ClickHouse itself
       misbehaves. TTL'd so none can become the next text_log.
       processors_profile_log is here because a live boot showed the image
       creates it too. It is easy to miss precisely because it is small
       today, which is what text_log also was once. -->
  <query_log><ttl>event_date + INTERVAL {{ .Values.clickhouse.systemLogs.retentionDays }} DAY DELETE</ttl></query_log>
  <part_log><ttl>event_date + INTERVAL {{ .Values.clickhouse.systemLogs.retentionDays }} DAY DELETE</ttl></part_log>
  <error_log><ttl>event_date + INTERVAL {{ .Values.clickhouse.systemLogs.retentionDays }} DAY DELETE</ttl></error_log>
  <processors_profile_log><ttl>event_date + INTERVAL {{ .Values.clickhouse.systemLogs.retentionDays }} DAY DELETE</ttl></processors_profile_log>
</clickhouse>
{{- end }}

{{- define "curie.rustfs.host" -}}
{{- if .Values.rustfs.deploy -}}
{{- printf "%s-rustfs" (include "curie.fullname" .) -}}
{{- else -}}
{{- required "rustfs.deploy is false: set rustfs.host to your external S3-compatible hostname" .Values.rustfs.host -}}
{{- end -}}
{{- end -}}

{{- define "curie.rustfs.scheme" -}}
{{- if and (not .Values.rustfs.deploy) (eq (printf "%v" .Values.rustfs.port) "443") -}}
https
{{- else -}}
http
{{- end -}}
{{- end -}}

{{- define "curie.rustfs.endpoint" -}}
{{- include "curie.rustfs.scheme" . }}://{{ include "curie.rustfs.host" . }}:{{ .Values.rustfs.port }}
{{- end -}}

{{/* Whether the object-store clients (api, worker, and the sandbox
     bundle-fetch init container) present static credentials.

     Non-empty when `rustfs.auth.accessKey` is set, which is the default and the
     only mode the in-chart RustFS supports. Empty when the operator cleared it,
     which is the BYO key-free path (#1325): every credential env var is omitted
     so the AWS SDK falls through its provider chain to the web-identity
     provider (`AWS_ROLE_ARN` + `AWS_WEB_IDENTITY_TOKEN_FILE`), fed by a
     projected ServiceAccount token.

     Web identity is the ONLY key-free path this chart supports, deliberately.
     The instinct on AWS is to drop the keys and let the node's instance role
     answer, and that must not be made to work here: Rail 1 denies
     169.254.169.254 by construction, and `security-networkpolicy.yaml` computes
     an `except` so a broad operator `allowedEgress` CIDR cannot re-permit the
     metadata address. NetworkPolicy selects pods, not containers, so opening
     IMDS for the bundle-fetch init container would also open it for the runner
     -- a prompt-injectable agent -- handing it the node's IAM role. Web
     identity reads a mounted token instead of a network endpoint, so it needs
     no metadata access and leaves Rail 1 intact. */}}
{{- define "curie.rustfs.staticCredentials" -}}
{{- if .Values.rustfs.auth.accessKey -}}
{{- if and .Values.rustfs.deploy (not .Values.rustfs.existingSecret) (not .Values.rustfs.auth.secretKey) -}}
{{- fail "rustfs.auth.secretKey is empty but rustfs.deploy is true and rustfs.existingSecret is empty. The in-chart RustFS requires secret material for static credentials. Either set rustfs.auth.secretKey, or set rustfs.existingSecret to a Secret containing rustfsSecretKey." -}}
{{- end -}}
true
{{- else if .Values.rustfs.deploy -}}
{{- fail "rustfs.auth.accessKey is empty but rustfs.deploy is true. The in-chart RustFS is configured with those static credentials and has no web-identity path, so clearing the key would leave every bundle read and write unauthenticated against it. Either set rustfs.auth.accessKey, or set rustfs.deploy=false and point rustfs.host at an external store that accepts the ServiceAccount's projected token (see the chart README, 'Key-free object store auth')." -}}
{{- end -}}
{{- end -}}

{{- define "curie.langfuse.webHost" -}}
{{- if .Values.langfuse.deploy -}}
{{- printf "%s-langfuse-web" (include "curie.fullname" .) -}}
{{- else -}}
{{- required "langfuse.deploy is false: set langfuse.host to your external Langfuse hostname" .Values.langfuse.host -}}
{{- end -}}
{{- end -}}

{{/* URL scheme for the Langfuse endpoint every consumer talks to (#2314).
     Same shape as curie.rustfs.scheme (#1447): an explicit langfuse.scheme
     wins, otherwise a BYO endpoint on 443 is assumed to speak TLS and
     everything else stays cleartext. The chart-owned Service is always http. */}}
{{- define "curie.langfuse.scheme" -}}
{{- $scheme := .Values.langfuse.scheme | default "" -}}
{{- if $scheme -}}
{{- if not (has $scheme (list "http" "https")) -}}
{{- fail (printf "langfuse.scheme must be either \"http\" or \"https\", got %q" $scheme) -}}
{{- end -}}
{{- $scheme -}}
{{- else if and (not .Values.langfuse.deploy) (eq (printf "%v" .Values.langfuse.web.service.port) "443") -}}
https
{{- else -}}
http
{{- end -}}
{{- end -}}

{{/* The one Langfuse base URL. Call sites include this helper rather than
     re-composing scheme://host:port, which is how the hardcoded `http://`
     reached five consumers at once (#2314). */}}
{{- define "curie.langfuse.url" -}}
{{- include "curie.langfuse.scheme" . }}://{{ include "curie.langfuse.webHost" . }}:{{ .Values.langfuse.web.service.port }}
{{- end -}}

{{/* Shared ServiceAccount name for both Langfuse Deployments. Empty
     langfuse.serviceAccount.name falls back to <release>-langfuse so a
     key-free install can bind one role without duplicating the name on
     web and worker (#2211). */}}
{{- define "curie.langfuse.serviceAccountName" -}}
{{- .Values.langfuse.serviceAccount.name | default (printf "%s-langfuse" (include "curie.fullname" .)) -}}
{{- end -}}

{{/* Base URL of the platform API for a first-party service that calls it. Call
     with a dict: root (the top context) and baseUrl (the caller's own BYO
     override). An empty override derives the in-chart API Service; a set value
     renders verbatim and is the BYO answer, including when api.deploy is false.
     Keep this separate from curie.env.api so callers such as the mail adapter
     can receive the API URL without also receiving the platform API key. */}}
{{- define "curie.api.url" -}}
{{- .baseUrl | default (printf "http://%s-api:%v" (include "curie.fullname" .root) .root.Values.api.service.port) -}}
{{- end -}}

{{/* base64("<publicKey>:<secretKey>") for the OTel Collector config checksum,
     and the operand the default-credential gate judges. The header this chart
     actually emits is resolved in secrets.yaml, from the managed-secret value
     of the Langfuse init project secret key; this helper composes from the raw
     .Values inputs instead, so a generated per-release credential does not
     churn the checksum.

     Three branches (issue #1563):
       1. the operator override wins whenever it is set;
       2. when the collector reads a Secret this chart does not manage, this
          renders EMPTY. The header lives in the operator's Secret, which a
          .Values composition cannot read, so the helper must report only what
          this chart actually ships and never a dev-key-derived value the chart
          puts nowhere. Deciding that off curie.otlpAuthHeaderSecretName is also
          what keeps this helper and the secrets.yaml emission condition the
          same decision;
       3. otherwise the chart-managed Secret is the one the collector reads, so
          the header derives from the Langfuse init keys. */}}
{{- define "curie.otlpAuthHeader" -}}
{{- if .Values.otelCollector.otlpAuthHeader -}}
{{- .Values.otelCollector.otlpAuthHeader -}}
{{- else if ne (include "curie.otlpAuthHeaderSecretName" .) (include "curie.secretName" .) -}}
{{- else -}}
{{- printf "Basic %s" (printf "%s:%s" .Values.langfuse.init.projectPublicKey .Values.langfuse.init.projectSecretKey | b64enc) -}}
{{- end -}}
{{- end -}}

{{/* Secret the OTel Collector reads its otlpAuthHeader key from. An explicit
     otelCollector.otlpAuthHeader materialises into the chart-managed Secret, so
     that override reads from the chart's own Secret; otherwise the collector
     follows the same BYO idiom as every other Langfuse consumer (#169).
     This MUST agree with the otlpAuthHeader emission condition in secrets.yaml:
     they are the same decision written twice, and disagreement is the desync
     issue #1563 closes. */}}
{{- define "curie.otlpAuthHeaderSecretName" -}}
{{- if .Values.otelCollector.otlpAuthHeader -}}
{{- include "curie.secretName" . -}}
{{- else -}}
{{- .Values.langfuse.existingSecret | default (include "curie.secretName" .) -}}
{{- end -}}
{{- end -}}

{{/* Reject absent, malformed, negative, and zero-equivalent retry durations.
     Keep max_interval and max_elapsed_time on this one validation path so the
     finite retry bound cannot harden one field while leaving its sibling open. */}}
{{- define "curie.otelCollector.requirePositiveDuration" -}}
{{- $value := trim (toString .value) -}}
{{- $durationPattern := "^\\+?(?:(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:ns|us|µs|μs|ms|s|m|h))+$" -}}
{{- $zeroPattern := "^\\+?(?:(?:0+(?:\\.0*)?|\\.0+)(?:ns|us|µs|μs|ms|s|m|h))+$" -}}
{{- if or (empty $value) (not (regexMatch $durationPattern $value)) (regexMatch $zeroPattern $value) -}}
{{- fail (printf "otelCollector.extraExporters[%q] retry_on_failure.%s must use a supported finite, non-zero positive duration." .exporter .field) -}}
{{- end -}}
{{- end -}}

{{- define "curie.otelCollector.config" -}}
{{- $debugEnabled := .Values.otelCollector.debugExporter.enabled -}}
{{- $builtInExporters := dict "otlphttp/langfuse" true "nop/logs" true "nop/metrics" true -}}
{{- /* Reserve built-in names even when the development-only debug exporter is disabled.
      An extra exporter with one of these keys would otherwise replace chart-owned
      configuration in the rendered ConfigMap. */ -}}
{{- $reservedExporterNames := dict "otlphttp/langfuse" true "nop/logs" true "nop/metrics" true "debug" true -}}
{{- if $debugEnabled -}}
{{- $_ := set $builtInExporters "debug" true -}}
{{- end -}}
{{- $pipelineExporters := dict
      "extraPipelineExporters" .Values.otelCollector.extraPipelineExporters
      "extraLogPipelineExporters" .Values.otelCollector.extraLogPipelineExporters
      "extraMetricPipelineExporters" .Values.otelCollector.extraMetricPipelineExporters -}}
{{- range $valueName, $exporters := $pipelineExporters -}}
{{- range $exporter := $exporters -}}
{{- if not (or (hasKey $builtInExporters $exporter) (hasKey $.Values.otelCollector.extraExporters $exporter)) -}}
{{- fail (printf "otelCollector.%s references undefined exporter %q. Add it under otelCollector.extraExporters." $valueName $exporter) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- range $name, $config := .Values.otelCollector.extraExporters -}}
{{- if hasKey $reservedExporterNames $name -}}
{{- fail (printf "otelCollector.extraExporters[%q] must not replace built-in exporter %q." $name $name) -}}
{{- end -}}
{{- $exporterType := first (splitList "/" $name) -}}
{{- if not (or (eq $exporterType "nop") (eq $exporterType "debug")) -}}
{{- if not (kindIs "map" $config) -}}
{{- fail (printf "otelCollector.extraExporters[%q] is a network exporter and must be a map with retry_on_failure and sending_queue settings." $name) -}}
{{- end -}}
{{- $headers := get $config "headers" -}}
{{- if and $headers (not (kindIs "map" $headers)) -}}
{{- fail (printf "otelCollector.extraExporters[%q].headers must be a map." $name) -}}
{{- end -}}
{{- if kindIs "map" $headers -}}
{{- $sensitiveHeaderPattern := "(?i)(^|[-_])(authorization|token|api[-_]?key|secret|password|credential)([-_]|$)" -}}
{{- $collectorEnvPattern := "^\\$\\{env:[A-Za-z_][A-Za-z0-9_]*\\}$" -}}
{{- range $headerName, $headerValue := $headers -}}
{{- if and (regexMatch $sensitiveHeaderPattern (lower (toString $headerName))) (not (regexMatch $collectorEnvPattern (trim (toString $headerValue)))) -}}
{{- fail (printf "otelCollector.extraExporters[%q].headers[%q] is sensitive and must use Collector environment expansion ${env:NAME}; put its value in otelCollector.extraEnv via valueFrom.secretKeyRef." $name $headerName) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $retry := get $config "retry_on_failure" -}}
{{- if not (kindIs "map" $retry) -}}
{{- fail (printf "otelCollector.extraExporters[%q] must configure retry_on_failure with enabled: true and finite max_interval/max_elapsed_time." $name) -}}
{{- end -}}
{{- if ne (lower (toString (get $retry "enabled"))) "true" -}}
{{- fail (printf "otelCollector.extraExporters[%q] retry_on_failure must be enabled and use finite, non-zero max_interval and max_elapsed_time values." $name) -}}
{{- end -}}
{{- include "curie.otelCollector.requirePositiveDuration" (dict "exporter" $name "field" "max_interval" "value" (get $retry "max_interval")) -}}
{{- include "curie.otelCollector.requirePositiveDuration" (dict "exporter" $name "field" "max_elapsed_time" "value" (get $retry "max_elapsed_time")) -}}
{{- $queue := get $config "sending_queue" -}}
{{- if not (kindIs "map" $queue) -}}
{{- fail (printf "otelCollector.extraExporters[%q] must configure sending_queue with enabled: true, storage: file_storage, and a finite queue_size." $name) -}}
{{- end -}}
{{- $queueSize := int (get $queue "queue_size") -}}
{{- if or (ne (lower (toString (get $queue "enabled"))) "true") (ne (toString (get $queue "storage")) "file_storage") (le $queueSize 0) (gt $queueSize 100000) -}}
{{- fail (printf "otelCollector.extraExporters[%q] sending_queue must be enabled, use storage: file_storage, and set queue_size between 1 and 100000." $name) -}}
{{- end -}}
{{- end -}}
{{- end -}}
# Receives OTLP traces, logs, and metrics over gRPC (4317) and HTTP (4318).
# Traces go to Langfuse over HTTP; logs and metrics retain explicit nop defaults
# until #1765 supplies their backends. Langfuse OTLP ingest is HTTP-only (gRPC
# is silently unsupported), so the collector adapts trace traffic to otlphttp.
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318
processors:
  memory_limiter:
    check_interval: {{ .Values.otelCollector.memoryLimiter.checkInterval }}
    limit_percentage: {{ .Values.otelCollector.memoryLimiter.limitPercentage }}
    spike_limit_percentage: {{ .Values.otelCollector.memoryLimiter.spikeLimitPercentage }}
  batch: {}
exporters:
  otlphttp/langfuse:
    endpoint: {{ include "curie.langfuse.url" . }}/api/public/otel
    headers:
      Authorization: ${env:LANGFUSE_OTLP_AUTH_HEADER}
    retry_on_failure:
      enabled: true
      initial_interval: 5s
      max_interval: 30s
      max_elapsed_time: 5m
    sending_queue:
      enabled: true
      storage: file_storage
      queue_size: 1000
      num_consumers: 2
  nop/logs: {}
  nop/metrics: {}
{{- if $debugEnabled }}
  debug:
    verbosity: normal
{{- end }}
{{- with .Values.otelCollector.extraExporters }}
{{ toYaml . | nindent 2 }}
{{- end }}
extensions:
  health_check:
    endpoint: 0.0.0.0:13133
  file_storage:
    directory: {{ .Values.otelCollector.persistence.mountPath }}
    timeout: 1s
    create_directory: true
    fsync: true
    compaction:
      on_start: true
      on_rebound: true
      directory: {{ .Values.otelCollector.persistence.mountPath }}
      cleanup_on_start: true
service:
  extensions: [health_check, file_storage]
  telemetry:
    metrics:
      level: normal
      address: 0.0.0.0:{{ .Values.otelCollector.service.metricsPort }}
  pipelines:
    traces:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [otlphttp/langfuse{{- if $debugEnabled }}, debug{{- end }}{{- range .Values.otelCollector.extraPipelineExporters }}, {{ . }}{{- end }}]
    logs:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [nop/logs{{- if $debugEnabled }}, debug{{- end }}{{- range .Values.otelCollector.extraLogPipelineExporters }}, {{ . }}{{- end }}]
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [nop/metrics{{- if $debugEnabled }}, debug{{- end }}{{- range .Values.otelCollector.extraMetricPipelineExporters }}, {{ . }}{{- end }}]
{{- end }}

{{/* Chart-owned OTLP destination (#1819). In-cluster Service while deploy is
     true; otelCollector.endpoint when the operator brings their own collector.
     Empty when telemetry is explicitly disabled or when no-endpoint mode is
     valid (gate off, deploy false, endpoint empty). */}}
{{- define "curie.otel.endpoint" -}}
{{- if .Values.otelCollector.deploy -}}
http://{{ include "curie.fullname" . }}-otel-collector:{{ .Values.otelCollector.service.httpPort }}
{{- else if not .Values.otelCollector.telemetryDisabled -}}
{{- .Values.otelCollector.endpoint | default "" -}}
{{- end -}}
{{- end -}}

{{/* Fail closed on contradictory OTEL values always. Fail closed on accidental
     missing only when security.checkDefaultCredentials is on: local/offline
     no-endpoint remains valid outside that gate. The instrumented set is
     exactly the workloads that include curie.env.otel, recorded in
     charts/curie/CLAUDE.md; extraEnv-only does not satisfy the production gate
     because it would configure each of them independently and any one can
     drift. */}}
{{- define "curie.otel.validate" -}}
{{- $otel := .Values.otelCollector -}}
{{- if and $otel.telemetryDisabled $otel.deploy -}}
{{- fail "otelCollector.telemetryDisabled cannot be true while otelCollector.deploy is true. Deploy the chart-managed collector, or set deploy to false and keep telemetryDisabled true." -}}
{{- end -}}
{{- if and $otel.telemetryDisabled (not (empty $otel.endpoint)) -}}
{{- fail "otelCollector.telemetryDisabled cannot be true while otelCollector.endpoint is set. Set one destination or acknowledge that telemetry is disabled." -}}
{{- end -}}
{{- if and $otel.telemetryDisabled (not (empty $otel.headers)) -}}
{{- fail "otelCollector.telemetryDisabled cannot be true while otelCollector.headers is set." -}}
{{- end -}}
{{- if and $otel.telemetryDisabled (not (empty $otel.headersExistingSecret)) -}}
{{- fail "otelCollector.telemetryDisabled cannot be true while otelCollector.headersExistingSecret is set." -}}
{{- end -}}
{{- if and (not (empty $otel.headers)) (not (empty $otel.headersExistingSecret)) -}}
{{- fail "otelCollector.headers and otelCollector.headersExistingSecret cannot both be set. Use headersExistingSecret for credentials." -}}
{{- end -}}
{{- $sensitiveHeaderPattern := "(?i)(^|[,;\\s])(authorization|token|api[-_]?key|secret|password|credential)=" -}}
{{- if and (not (empty $otel.headers)) (regexMatch $sensitiveHeaderPattern (toString $otel.headers)) -}}
{{- fail "otelCollector.headers is sensitive and must use headersExistingSecret so the value never enters Helm values or the rendered workload env as a literal." -}}
{{- end -}}
{{- $protocol := $otel.protocol | default "http/protobuf" -}}
{{- if and (not (empty $protocol)) (not (or (eq $protocol "http/protobuf") (eq $protocol "grpc") (eq $protocol "http/json"))) -}}
{{- fail "otelCollector.protocol must be grpc, http/protobuf, or http/json." -}}
{{- end -}}
{{- if and .Values.security.checkDefaultCredentials (not $otel.deploy) (not $otel.telemetryDisabled) (empty $otel.endpoint) -}}
{{- fail "security.checkDefaultCredentials is on but neither a chart-managed collector nor otelCollector.endpoint is configured. Set otelCollector.endpoint to the external collector, keep otelCollector.deploy true, or set otelCollector.telemetryDisabled=true to acknowledge that telemetry is disabled." -}}
{{- end -}}
{{- end -}}

{{/* Standard OTEL_EXPORTER_OTLP_* env for every instrumented workload. extraEnv
     still wins per variable. Include with nindent 12. Call with
     dict "root" $ "extraEnv" .Values.<workload>.extraEnv */}}
{{- define "curie.env.otel" -}}
{{- include "curie.otel.validate" .root -}}
{{- $extra := .extraEnv | default list -}}
{{- $hasEndpoint := false -}}
{{- $hasProtocol := false -}}
{{- $hasHeaders := false -}}
{{- range $extra -}}
{{- if eq .name "OTEL_EXPORTER_OTLP_ENDPOINT" -}}{{- $hasEndpoint = true -}}{{- end -}}
{{- if eq .name "OTEL_EXPORTER_OTLP_PROTOCOL" -}}{{- $hasProtocol = true -}}{{- end -}}
{{- if eq .name "OTEL_EXPORTER_OTLP_HEADERS" -}}{{- $hasHeaders = true -}}{{- end -}}
{{- end -}}
{{- $endpoint := include "curie.otel.endpoint" .root | trim -}}
{{- $protocol := .root.Values.otelCollector.protocol | default "http/protobuf" -}}
{{- if and (not $hasEndpoint) (ne $endpoint "") }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ $endpoint | quote }}
{{- end }}
{{- if and (not $hasProtocol) (ne $endpoint "") }}
- name: OTEL_EXPORTER_OTLP_PROTOCOL
  value: {{ $protocol | quote }}
{{- end }}
{{- if and (not $hasHeaders) (not .root.Values.otelCollector.deploy) (not .root.Values.otelCollector.telemetryDisabled) (ne $endpoint "") }}
{{- if not (empty .root.Values.otelCollector.headersExistingSecret) }}
- name: OTEL_EXPORTER_OTLP_HEADERS
  valueFrom:
    secretKeyRef:
      name: {{ .root.Values.otelCollector.headersExistingSecret | quote }}
      key: {{ .root.Values.otelCollector.headersSecretKey | default "headers" | quote }}
{{- else if not (empty .root.Values.otelCollector.headers) }}
- name: OTEL_EXPORTER_OTLP_HEADERS
  value: {{ .root.Values.otelCollector.headers | quote }}
{{- end }}
{{- end }}
{{- end -}}

{{/* ---- Default-credential gate (issue #198) ----
     When security.checkDefaultCredentials is on, refuse to render if a Langfuse
     chart input for a bootstrap identity still carries the published dev default
     from values.yaml. The first two checks compare INPUTS, before managed secret
     resolution: with security.allowDevDefaults on, that input is what ships, and
     an operator who leaves the published value in a values file has said what
     they intend regardless of what the sealed path would have generated. These
     init identities seed the org/project on first boot (a different lifecycle
     from the nine store/control-plane secrets), so #57 deliberately excludes them
     from its render-time gate; this closes that gap. The published admin password
     is a Langfuse admin-takeover risk on a reachable UI, and on the path where
     the chart-managed Secret is the one the collector reads, the project secret
     key also feeds the OTel Collector auth header. The operator clears these two
     checks by overriding the value or pointing langfuse.existingSecret at a
     Secret this chart does not manage (the #169 secretKeyRef escape carries both
     keys, and on that path the operator's Secret supplies otlpAuthHeader
     directly).

     The guard is "does the chart-managed Secret still supply these credentials",
     written with the same existingSecret-defaults-to-our-own-Secret idiom every
     other consumer uses, NOT "is existingSecret non-empty". Naming the chart's
     own Secret changes nothing about where the credentials come from: the chart
     still fills those keys from the langfuse.init values, so the checks have to
     run there. Guarding on non-emptiness disabled the check on a path that ships
     a default credential.

     The third check, on the rendered collector header (issue #1563), is
     unconditional: langfuse.existingSecret does NOT clear it, because that
     header ships to the collector whatever the Langfuse credential source is,
     whether it came from an otelCollector.otlpAuthHeader override or the chart
     composed it from the langfuse.init keys.

     Off by default so the flagship zero-secret bare install stays green and the
     dev/e2e overlays render unchanged; flip it on for a shared/production
     cluster. #57 will fold the store/control-plane secrets into this same helper
     (hence the general name) once its design pass lands. */}}
{{- define "curie.checkDefaultCredentials" -}}
{{- if .Values.security.checkDefaultCredentials -}}
{{- if eq (.Values.langfuse.existingSecret | default (include "curie.secretName" .)) (include "curie.secretName" .) -}}
{{- if eq .Values.langfuse.init.projectSecretKey "sk-lf-curie-dev" -}}
{{- fail "security.checkDefaultCredentials is on but langfuse.init.projectSecretKey is still the published dev default \"sk-lf-curie-dev\". Override it (or set langfuse.existingSecret) before installing on a shared/production cluster -- this key also feeds the OTel Collector auth header." -}}
{{- end -}}
{{- if eq .Values.langfuse.init.userPassword "curie-dev-password" -}}
{{- fail "security.checkDefaultCredentials is on but langfuse.init.userPassword is still the published dev default \"curie-dev-password\". Override it (or set langfuse.existingSecret) before installing on a shared/production cluster -- the published admin password allows Langfuse admin takeover on a reachable UI." -}}
{{- end -}}
{{- end -}}
{{/* Outside the existingSecret guard on purpose (issue #1563): the header
     reaches the collector on every path, so a BYO Langfuse Secret does not make
     the published header safe. That is the case this check exists for: with a
     foreign langfuse.existingSecret the two checks above are skipped entirely,
     and an otelCollector.otlpAuthHeader carrying the published dev credential
     would otherwise render unchallenged.

     The operand is the RENDERED header (curie.otlpAuthHeader), not the
     otelCollector.otlpAuthHeader input, so the same expression covers the
     override and the composed-from-langfuse.init spellings without enumerating
     them. The composed spelling is in practice preempted by the projectSecretKey
     check above, which fires first on exactly the inputs that would compose it;
     reading the rendered header keeps the two in agreement rather than relying
     on that ordering. Composed via b64enc rather than pasted so it cannot drift
     from curie.otlpAuthHeader.

     THREAT MODEL, so the next reader stops enumerating spellings: this guards
     against an operator shipping this repository's published credential by
     accident. It is not an adversarial control, and cannot be one: anyone who
     can set otelCollector.otlpAuthHeader can equally set
     security.checkDefaultCredentials=false. Normalising the scheme's case and
     the credential's whitespace covers the spellings a copy/paste, a line wrap
     or a templating tool actually produces; it deliberately does NOT chase
     encodings only a deliberate bypass would produce.

     The header is split ONCE into scheme and credential, on its first run of
     whitespace, and each half is then normalised on its own terms. The scheme
     token is case-insensitive (RFC 9110), so it is compared lowercased against
     "basic". The credential is compared with ALL whitespace removed and base64
     padding stripped, because the receiver strips whitespace before decoding:
     interior whitespace from a wrapped paste decodes to the same credential,
     and taking the last whitespace-separated field instead would compare only
     its tail. The credential is NOT lowercased: base64 is case-significant, and
     folding its case would widen the match to strings that are not this
     credential. An empty rendered header (the chart ships nothing, the
     operator's Secret carries it) yields an empty scheme, which is not "basic",
     so it matches nothing. b64dec is not usable here, since sprig returns an
     error string rather than the plaintext for unpadded input.

     Deliberately independent of otelCollector.deploy: the chart writes this
     credential into its Secret either way, so a release that does not run a
     collector still ships the published dev key for one that later does. */}}
{{- $header := trim (include "curie.otlpAuthHeader" .) -}}
{{- $parts := regexSplit "[[:space:]]+" $header 2 -}}
{{- $scheme := first $parts -}}
{{- $credential := "" -}}
{{- if gt (len $parts) 1 -}}
{{- $credential = index $parts 1 -}}
{{- end -}}
{{- if and (eq (lower $scheme) "basic") (eq (trimAll "=" (regexReplaceAll "[[:space:]]+" $credential "")) (trimAll "=" (b64enc "pk-lf-curie-dev:sk-lf-curie-dev"))) -}}
{{- fail "security.checkDefaultCredentials is on but the chart would ship the published dev header \"Basic cGstbGYtY3VyaWUtZGV2OnNrLWxmLWN1cmllLWRldg==\" as the OTel Collector auth credential in its Secret (auth scheme spelling, whitespace and base64 padding aside), which the collector authenticates with when deployed. That is the dev project key pk-lf-curie-dev:sk-lf-curie-dev, which anyone reading this repository holds. That header arrives either from otelCollector.otlpAuthHeader set to it directly, or from the chart composing it out of langfuse.init.projectPublicKey and langfuse.init.projectSecretKey when the chart-managed Secret is the one the collector reads. Set otelCollector.otlpAuthHeader from your own project keys, override those two langfuse.init values, or point langfuse.existingSecret at your own Secret and supply otlpAuthHeader there." -}}
{{- end -}}
{{- end -}}
{{- include "curie.otel.validate" . -}}
{{- end -}}

{{/* ---- Auto-generated per-release chart credential (issue #195) ----
     Resolve one chart-owned secret value, generating a strong random per release
     for a sealed install instead of shipping the published dev default. Call with
     a dict: root (the top context), key (the stringData key, matching an existing
     Secret's data), value (.Values.<path>), default (the published dev default),
     hex (true for the 64-hex encryption key, else false).

     The existing Secret's data is looked up ONCE by the caller (secrets.yaml) and
     passed in as `.existingData` (an always-present dict, empty under `helm
     template`/--dry-run/first install), so this helper does no per-key lookup.

     A legacy retained values set can omit a key that a newer chart adds. Treat
     that nil as the declared default before applying the four branches below,
     so it is not mistaken for an explicit override. A persisted key whose
     decoded value is blank is likewise absent: retaining it would keep every
     consumer crash looping instead of healing the release.

     Four branches, in PRECEDENCE order, and WHY this order is correct:
       1. allowDevDefaults: the deterministic dev/CI escape hatch (values-dev.yaml
          sets it true). Return the value verbatim so the dev/e2e path renders the
          published defaults unchanged, byte-for-byte reproducible. Taking this
          first also means `--dev` reverts to the defaults even if a random was
          previously generated into the release Secret. Gate on positive equality
          against the literal "true" (`eq (toString ...) "true"`), NOT plain
          truthiness: Go templates treat any non-empty string as truthy, so a
          quoted `--set security.allowDevDefaults="false"` would otherwise read as
          truthy and ship the published default -- a fail-OPEN regression.
       2. Explicit override: if the operator/CLI supplied a value that differs from
          the published default (`ne value default`), it wins even on `helm
          upgrade`. For the eleven non init credentials, this supports rotation or
          recovery. The Langfuse init credentials are first boot inputs, so an
          upgrade only changes the Secret; it does not rotate Langfuse records.
          The override must sit ahead of the persist branch or an explicit value
          on upgrade would be silently ignored.
       3. Persist existing: no override, so if a prior install already GENERATED
          this key with a nonblank value, re-use it. `helm upgrade` must NEVER rotate a live store
          credential (Postgres would reject the new password against its persisted
          data), so we return the stored value from `.existingData` when present.
          Generated secrets always have value==published-default (nobody set them),
          so they never take branch 2 and always land here on upgrade -- exactly
          the "upgrade must not rotate" guarantee. `.existingData` is always a dict
          (the caller applies `| default dict`), empty under `helm
          template`/--dry-run and on first install, so a missing key falls through
          to generation.
       4. Generate: a first sealed install (value still equals the published
          default, no prior Secret) gets a strong random. `randAlphaNum` is
          crypto-backed (Sprig). hex=true hashes it to 64 lowercase-hex chars (the
          encryption key format); otherwise a 32-char alphanumeric.

     Net effect: an operator who forgets to re-pass `--set` on a later upgrade
     safely reverts value to the default, which then reuses the persisted generated
     value via branch 3 rather than rotating it. */}}
{{- define "curie.managedSecret" -}}
{{- $value := .value -}}
{{- if kindIs "invalid" $value -}}
{{- $value = .default -}}
{{- end -}}
{{- $existingValue := "" -}}
{{- if hasKey .existingData .key -}}
{{- $existingValue = index .existingData .key | b64dec -}}
{{- end -}}
{{- if eq (toString .root.Values.security.allowDevDefaults) "true" -}}{{/* string-coercion safety -- a quoted "false" must not read as truthy and silently ship a published default (fail closed to generation). */}}
{{- $value -}}
{{- else if ne (toString $value) (toString .default) -}}
{{- $value -}}
{{- else if ne (trim $existingValue) "" -}}
{{- $existingValue -}}
{{- else if .hex -}}
{{- randAlphaNum 32 | sha256sum -}}
{{- else -}}
{{- randAlphaNum 32 -}}
{{- end -}}
{{- end -}}

{{/* ---- Shared first-party-app environment fragments ---- */}}

{{/* Postgres connection env for the app services. POSTGRES_PASSWORD comes from
     the Secret and DATABASE_URL is composed with $(POSTGRES_PASSWORD) so the
     password never lands in the rendered manifest. Both the API and the worker
     use the asyncpg driver and the dedicated `curie` schema. */}}
{{- define "curie.env.postgres" -}}
- name: POSTGRES_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.postgres.existingSecret | default (include "curie.secretName" .) }}
      key: postgresPassword
- name: DATABASE_URL
  value: postgresql+asyncpg://{{ .Values.postgres.auth.username }}:$(POSTGRES_PASSWORD)@{{ include "curie.postgres.host" . }}:{{ .Values.postgres.port }}/{{ .Values.postgres.auth.database }}
- name: DB_SCHEMA
  value: curie
{{- end -}}

{{/* Valkey connection env for the app services (host/port + password from the
     Secret, plus the transport). The apps build their own redis DSN from these
     parts, and TLS is one of the parts -- rendered always, both values, so an
     install that never set valkey.tls cannot be confused with a broken
     template and a --reuse-values upgrade cannot leave a stale "true" behind
     (#2315). */}}
{{- define "curie.env.valkey" -}}
- name: VALKEY_HOST
  value: {{ include "curie.valkey.host" . | quote }}
- name: VALKEY_PORT
  value: {{ .Values.valkey.port | quote }}
- name: VALKEY_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.valkey.existingSecret | default (include "curie.secretName" .) }}
      key: valkeyPassword
- name: VALKEY_TLS
  value: {{ include "curie.valkey.tls" . | quote }}
{{- end -}}

{{/* Platform API connection env for first party services that call the API.
     Keep the URL and key as separate helpers so callers can include only the
     credentials they need. The composed helper preserves the existing
     dispatcher contract.

     The API URL env has been forgotten three times on new callers because
     those callers had a shared helper to include and the worker did not.
     New API callers should include the granular URL helper rather than derive
     the URL inline. This follows the same shared helper pattern as
     `curie.env.postgres` and `curie.env.valkey`.

     The BYO override is .Values.dispatcher.apiBaseUrl. Note the deliberate
     absence of a `required` call for the api.deploy=false case that the sibling
     `X.host` helpers use: an empty override with api.deploy=false yields a
     CrashLoopBackOff by design (documented in NOTES.txt and the README), not a
     render-time failure. Include with `nindent 12` to land at a container's env
     column. */}}
{{- define "curie.env.apiUrl" -}}
# Where the platform API lives. The dispatcher POSTs an approval
# resolve here when someone clicks Approve in Slack, so an unwired
# value means the click dead-ends: the code default
# http://localhost:8000 is, inside this pod, the dispatcher itself.
# Empty dispatcher.apiBaseUrl (the default) derives the in-chart API
# Service; a set value renders verbatim and is the BYO answer, and
# the only correct one when api.deploy is false. The port comes from
# api.service.port so the two sides cannot drift.
- name: CURIE_API_URL
  value: {{ include "curie.api.url" (dict "root" . "baseUrl" .Values.dispatcher.apiBaseUrl) | quote }}
{{- end -}}

{{- define "curie.env.apiKey" -}}
# The same chart Secret key api.yaml consumes as API_KEY, so the
# caller and the API cannot drift apart. By reference only: an inline
# value would put the shared platform key into `helm get manifest`
# output and into any rendered artifact CI uploads.
- name: CURIE_API_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "curie.secretName" . }}
      key: apiKey
{{- end -}}

{{- define "curie.env.api" -}}
{{- include "curie.env.apiUrl" . }}
{{ include "curie.env.apiKey" . }}
{{- end -}}

{{/* Coalesce the worker's chart-managed egress credentials and the first-party
     mail adapter's chart-managed paired credential. The chart Secret and the
     worker rollout checksum must use this same rendered JSON so a rotation
     reaches both sides. mailAdapter.egressSecret is the source of truth on
     that path; accepting an equal hand-written worker entry keeps migrations
     from hand-rolled manifests possible, while a disagreement fails rather
     than deploying a reply path that can only return 401. When the adapter
     uses egressSecretExistingSecret, the required external worker map is the
     independent source of truth: never derive from or compare the unused plain
     Helm value. */}}
{{- define "curie.adapterCredentials" -}}
{{- $creds := deepCopy (.Values.worker.adapterCredentials | default dict) -}}
{{- if and .Values.mailAdapter.deploy (not .Values.mailAdapter.egressSecretExistingSecret) -}}
{{- $slug := .Values.mailAdapter.adapterSlug -}}
{{- $derived := .Values.mailAdapter.egressSecret -}}
{{- if hasKey $creds $slug -}}
{{- $existing := get $creds $slug -}}
{{- if ne $existing $derived -}}
{{- fail (printf "worker.adapterCredentials.%s and mailAdapter.egressSecret are set to DIFFERENT values. mailAdapter.egressSecret is the source of truth for both halves of the mail adapter's egress pair: the chart derives worker.adapterCredentials.%s from it. Fix the two configuration keys to the same value. Neither value is printed here because both are live egress credentials." $slug $slug) -}}
{{- end -}}
{{- else -}}
{{- $_ := set $creds $slug $derived -}}
{{- end -}}
{{- end -}}
{{- $creds | toJson -}}
{{- end -}}

{{/* Keep the historical inline checksum byte-for-byte while also rolling the
     worker when an operator switches the BYO Secret source. */}}
{{- define "curie.adapterCredentialsChecksumSource" -}}
{{- $creds := include "curie.adapterCredentials" . -}}
{{- if not (empty .Values.worker.adapterCredentialsExistingSecret) -}}
{{- printf "%s|%s|%s" $creds .Values.worker.adapterCredentialsExistingSecret .Values.worker.adapterCredentialsExistingSecretKey -}}
{{- else -}}
{{- $creds -}}
{{- end -}}
{{- end -}}

{{/* Heartbeat exec probes for the worker and dispatcher. Neither has an HTTP
     port, so an exec probe checks CURIE_HEARTBEAT_FILE freshness (< 30s)
     instead of hitting a port. Each Deployment sets its own heartbeat path via
     that env var, so the probe body is path-agnostic and both callers share
     identical timings -- the helper therefore takes no params. Include with
     `nindent 10` so the probe keys land at the container's 10-space column. */}}
{{- define "curie.heartbeatProbes" -}}
readinessProbe:
  exec:
    command:
      - python
      - -c
      - |
        import os, sys, time
        p = os.environ["CURIE_HEARTBEAT_FILE"]
        sys.exit(0 if os.path.exists(p) and time.time() - os.path.getmtime(p) < 30 else 1)
  initialDelaySeconds: 10
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3
livenessProbe:
  exec:
    command:
      - python
      - -c
      - |
        import os, sys, time
        p = os.environ["CURIE_HEARTBEAT_FILE"]
        sys.exit(0 if os.path.exists(p) and time.time() - os.path.getmtime(p) < 30 else 1)
  initialDelaySeconds: 30
  periodSeconds: 15
  timeoutSeconds: 5
  failureThreshold: 4
{{- end }}

{{/* ---- Langfuse shared environment (mirrors compose.dev.yaml's
        x-langfuse-env anchor). Rendered into both web and worker. ---- */}}
{{- define "curie.langfuse.env" -}}
{{- /* These three honour their store's own existingSecret, the same escape
       curie.env.postgres and the CLICKHOUSE_PASSWORD/REDIS_AUTH refs below
       already use. They were pinned to the chart Secret while every sibling
       read the BYO one. On a BYO-Postgres install the api and worker
       authenticated against the real instance while both Langfuse Deployments
       presented the chart-generated password and crash-looped at Prisma auth,
       with every other component green. On a BYO langfuse.existingSecret
       install the operator's langfuseEncryptionKey was silently unused, and a
       later regeneration of the chart Secret left previously written encrypted
       columns undecryptable. See #2327. */}}
- name: POSTGRES_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.postgres.existingSecret | default (include "curie.secretName" .) }}
      key: postgresPassword
- name: DATABASE_URL
  value: postgresql://{{ .Values.postgres.auth.username }}:$(POSTGRES_PASSWORD)@{{ include "curie.postgres.host" . }}:{{ .Values.postgres.port }}/{{ .Values.postgres.auth.database }}
- name: SALT
  valueFrom:
    secretKeyRef:
      name: {{ .Values.langfuse.existingSecret | default (include "curie.secretName" .) }}
      key: langfuseSalt
- name: ENCRYPTION_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.langfuse.existingSecret | default (include "curie.secretName" .) }}
      key: langfuseEncryptionKey
- name: TELEMETRY_ENABLED
  value: {{ .Values.langfuse.telemetryEnabled | quote }}
- name: LANGFUSE_ENABLE_EXPERIMENTAL_FEATURES
  value: {{ .Values.langfuse.enableExperimentalFeatures | quote }}
- name: CLICKHOUSE_MIGRATION_URL
  value: {{ include "curie.clickhouse.migrationUrl" . }}
- name: CLICKHOUSE_URL
  value: {{ include "curie.clickhouse.httpUrl" . }}
{{- if eq (include "curie.clickhouse.migrationSsl" .) "true" }}
{{- /* Langfuse's documented switch for a TLS migration connection on the
       native port; its migrator appends `secure=true` to the DSN itself, which
       is why CLICKHOUSE_MIGRATION_URL above stays bare. Rendered only on the
       https path so a cleartext install is byte-identical (#2314). */}}
- name: CLICKHOUSE_MIGRATION_SSL
  value: "true"
{{- end }}
- name: CLICKHOUSE_USER
  value: {{ .Values.clickhouse.auth.username | quote }}
{{- /* Both of these honour the store's own existingSecret, the same escape
       clickhouse.yaml:101 and curie.env.valkey already use. They were pinned to
       the chart Secret while every other consumer read the BYO one, so a
       `deploy=false` + `host` + `existingSecret` install left Langfuse alone
       authenticating with the chart-generated password and trace ingestion died
       silently with the rest of the release healthy. With clickhouse.deploy=true
       and clickhouse.existingSecret set it was worse: the in-chart server and
       Langfuse disagreed on the same password (split-brain auth). See #2052. */}}
- name: CLICKHOUSE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.clickhouse.existingSecret | default (include "curie.secretName" .) }}
      key: clickhousePassword
- name: CLICKHOUSE_CLUSTER_ENABLED
  value: {{ .Values.clickhouse.clusterEnabled | quote }}
- name: REDIS_HOST
  value: {{ include "curie.valkey.host" . }}
- name: REDIS_PORT
  value: {{ .Values.valkey.port | quote }}
- name: REDIS_AUTH
  valueFrom:
    secretKeyRef:
      name: {{ .Values.valkey.existingSecret | default (include "curie.secretName" .) }}
      key: valkeyPassword
{{- /* Same helper as curie.env.valkey, so the two Langfuse Deployments and the
       first-party apps cannot disagree about the transport of the one store
       they share (#2315). Langfuse's REDIS_TLS_CA/_CERT/_KEY siblings are
       deliberately NOT rendered: system-CA verification is the boundary for
       every consumer here, and giving Langfuse alone a private-CA capability
       is the asymmetry this helper exists to prevent. */}}
- name: REDIS_TLS_ENABLED
  value: {{ include "curie.valkey.tls" . | quote }}
- name: LANGFUSE_S3_EVENT_UPLOAD_BUCKET
  value: {{ .Values.rustfs.bucket | quote }}
{{- /* Both upload regions come from rustfs.region, never the literal
       `auto` that in-chart RustFS accepts. Real S3 rejects `auto` with
       AuthorizationHeaderMalformed and drops every trace while the
       release reports healthy. See #2214. */}}
- name: LANGFUSE_S3_EVENT_UPLOAD_REGION
  value: {{ .Values.rustfs.region | quote }}
{{- /* Credential env is gated the same way as api/worker/bundle-fetch
       (#2211). An empty rustfs.auth.accessKey on the key-free path must
       omit these, not emit them empty: Langfuse's AWS SDK treats an empty
       explicit credential as a credential and never reaches the
       web-identity provider. */}}
{{- if include "curie.rustfs.staticCredentials" . }}
- name: LANGFUSE_S3_EVENT_UPLOAD_ACCESS_KEY_ID
  value: {{ .Values.rustfs.auth.accessKey | quote }}
- name: LANGFUSE_S3_EVENT_UPLOAD_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.rustfs.existingSecret | default (include "curie.secretName" .) }}
      key: rustfsSecretKey
{{- end }}
{{- /* Both endpoints go through curie.rustfs.endpoint, never a literal
       scheme. They were hardcoded http:// while api/worker/sandbox used the
       helper, so a BYO store on rustfs.port 443 got https:// everywhere except
       Langfuse, and trace ingestion died at the TLS handshake -- with the rest
       of the release healthy, which is why nothing pointed at the cause. */}}
- name: LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT
  value: {{ include "curie.rustfs.endpoint" . }}
- name: LANGFUSE_S3_EVENT_UPLOAD_FORCE_PATH_STYLE
  value: "true"
- name: LANGFUSE_S3_EVENT_UPLOAD_PREFIX
  value: events/
- name: LANGFUSE_S3_MEDIA_UPLOAD_BUCKET
  value: {{ .Values.rustfs.bucket | quote }}
- name: LANGFUSE_S3_MEDIA_UPLOAD_REGION
  value: {{ .Values.rustfs.region | quote }}
{{- if include "curie.rustfs.staticCredentials" . }}
- name: LANGFUSE_S3_MEDIA_UPLOAD_ACCESS_KEY_ID
  value: {{ .Values.rustfs.auth.accessKey | quote }}
- name: LANGFUSE_S3_MEDIA_UPLOAD_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.rustfs.existingSecret | default (include "curie.secretName" .) }}
      key: rustfsSecretKey
{{- end }}
- name: LANGFUSE_S3_MEDIA_UPLOAD_ENDPOINT
  value: {{ include "curie.rustfs.endpoint" . }}
- name: LANGFUSE_S3_MEDIA_UPLOAD_FORCE_PATH_STYLE
  value: "true"
- name: LANGFUSE_S3_MEDIA_UPLOAD_PREFIX
  value: media/
{{- end -}}

{{/* ---- gVisor tri-state (security.gvisor.mode: auto|require|off) ----

     curie.gvisor.className: the RuntimeClass NAME to use/verify when gVisor is
     intended at all (empty only for mode=off). Deterministic (no cluster lookup);
     used by the enforcement preflight, the optional RuntimeClass object, and the
     probe's admission test.

     curie.gvisor.runtimeClassName: the EFFECTIVE runtimeClassName to stamp on a
     runner pod. off -> empty; require -> className; auto -> className when the
     chart itself creates the RuntimeClass (installRuntimeClass=true), otherwise
     only if the class is found by `lookup`. The installRuntimeClass shortcut
     exists because `lookup` cannot see the RuntimeClass the same install is about
     to create (nor anything under `helm template`/--dry-run), which would leave
     first-install runner pods with no runtimeClassName despite the chart
     guaranteeing the object. */}}
{{- define "curie.gvisor.className" -}}
{{- $g := .Values.security.gvisor -}}
{{- if eq ($g.mode | default "auto") "off" -}}
{{- else -}}
{{- $g.runtimeClassName | default "gvisor" -}}
{{- end -}}
{{- end -}}

{{- define "curie.gvisor.runtimeClassName" -}}
{{- $g := .Values.security.gvisor -}}
{{- $mode := $g.mode | default "auto" -}}
{{- $name := $g.runtimeClassName | default "gvisor" -}}
{{- if eq $mode "off" -}}
{{- else if eq $mode "require" -}}
{{- $name -}}
{{- else if $g.installRuntimeClass -}}
{{- $name -}}
{{- else -}}
{{- if lookup "node.k8s.io/v1" "RuntimeClass" "" $name -}}
{{- $name -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* ---- gVisor enforcement gate ----
     curie.gvisor.preflightRequired: non-empty ("true") when the blocking
     gVisor enforcement preflight Job must render, else empty. It renders in
     `require` (always) and in `auto` WHEN the runner runs a real (non-fake)
     model -- i.e. untrusted agent code executes, so a missing/downgraded runsc
     RuntimeClass must fail the install CLOSED instead of silently landing on
     the host kernel. `auto` with the fake model (the bare-install default)
     still degrades gracefully with only a NOTES warning; `off` never renders.
     Real-model detection mirrors the CURIE_FAKE_MODEL gate in
     agent-sandbox.yaml (fake is in effect only when runner.fakeModel AND NOT
     inference.deploy), so real code runs when `(not fakeModel) OR inference.deploy`.
     Also respects security.gvisorPreflight.enabled and agentSandbox.deploy. */}}
{{- define "curie.gvisor.preflightRequired" -}}
{{- $mode := .Values.security.gvisor.mode | default "auto" -}}
{{- $realModel := or (not .Values.agentSandbox.runner.fakeModel) .Values.inference.deploy -}}
{{- if and .Values.agentSandbox.deploy .Values.security.gvisorPreflight.enabled -}}
{{- if or (eq $mode "require") (and (eq $mode "auto") $realModel) -}}
true
{{- end -}}
{{- end -}}
{{- end -}}

{{/* ---- First-party image reference ----
     Render a fully-qualified image ref for a first-party (GHCR) workload,
     preferring an immutable content digest over a mutable tag. Call with a dict:
       repository  the image repo (e.g. ghcr.io/curie-eng/curie-api)
       tag         optional explicit tag; empty falls back to defaultTag
       digest      optional "sha256:..." -- when set, wins and pins by digest
       defaultTag  the fallback tag when `tag` is empty (pass .Chart.AppVersion)
     - digest set -> "<repository>@sha256:..."  (fully immutable + verifiable)
     - else       -> "<repository>:<tag|defaultTag>"
     An empty tag defaulting to the chart appVersion is what makes a given chart
     version render a deterministic image ref (same chart version -> same ref,
     installable and rollback-able) without every install pinning a field.  */}}
{{- define "curie.image" -}}
{{- $repo := required "image.repository is required" .repository -}}
{{- if .digest -}}
{{- printf "%s@%s" $repo .digest -}}
{{- else -}}
{{- printf "%s:%s" $repo (.tag | default .defaultTag | default "latest") -}}
{{- end -}}
{{- end -}}

{{/* ---- BYO existingSecret escape for a direct-passthrough credential
     (issue #1759) ----

     Eleven keys (agentCredentials, adapterCredentials, githubToken,
     sealingPrivateKey, sealingPreviousPrivateKey, slackAppToken,
     slackBotToken, slackSigningSecret, mailChannelToken, mailEgressSecret,
     mailAgentmailApiKey) each grew a per-field
     `<field>ExistingSecret` / `<field>ExistingSecretKey` pair mirroring
     api.githubAppExistingSecret (ADR-0092): set, it wins over the plain
     value and the consumer's secretKeyRef points straight at the operator's
     Secret, so a BYO Secret missing the key fails that pod loudly with
     CreateContainerConfigError instead of the chart emitting an empty
     credential.

     One generic helper for all eleven, following the dict-argument pattern
     `curie.image`/`curie.managedSecret` already use in this file, rather than
     a bespoke per-key helper or a hand-copied if/else at each consumer: every
     consumer of the SAME key -- there are three for slackBotToken, two for
     agentCredentials -- calls this with the same arguments and so cannot
     resolve the escape differently, exactly the parity-seam trap
     `curie.env.postgres`/`curie.env.valkey` already exist to avoid for the
     backing stores; a single-consumer key gets the same guarantee for free
     if it ever grows a second one.

     Pass a dict:
       root               the top context
       existingSecret     .Values.<field>ExistingSecret
       existingSecretKey  .Values.<field>ExistingSecretKey
       defaultKey         this credential's published key in the chart's own
                           Secret (secrets.yaml), used when existingSecret is
                           empty
     Renders the two lines a secretKeyRef needs (`name:` / `key:`); include
     with `nindent 18` to land at a container's secretKeyRef column. */}}
{{- define "curie.secretRef" -}}
{{- if .existingSecret -}}
name: {{ .existingSecret | quote }}
key: {{ .existingSecretKey | quote }}
{{- else -}}
name: {{ include "curie.secretName" .root }}
key: {{ .defaultKey }}
{{- end -}}
{{- end -}}

{{/* ---- Dispatcher gating ----
     The Slack dispatcher only deploys when it has both tokens; without them it
     would crash-loop the reconnect supervisor forever, so a token-less default
     install skips the Deployment entirely (NOTES prints the connect command).
     A token counts as present whether it arrives as the plain value or via its
     *ExistingSecret (issue #1759) -- a dispatcher configured entirely through
     BYO Secrets must still deploy. */}}
{{- define "curie.dispatcher.enabled" -}}
{{- $appTokenSet := or .Values.dispatcher.slack.appToken .Values.dispatcher.slack.appTokenExistingSecret -}}
{{- $botTokenSet := or .Values.dispatcher.slack.botToken .Values.dispatcher.slack.botTokenExistingSecret -}}
{{- if and .Values.dispatcher.deploy $appTokenSet $botTokenSet -}}
true
{{- end -}}
{{- end -}}

{{/* ---- Sandbox container hardening (Rail 3) ----
     The identical container-level lockdown applied to the runner and every
     helper container in the sandbox pod (bundle-fetch, bundle-extract,
     workspace-init).
     Extracted so the four copies cannot drift (#493); callers keep their own
     `{{- if $runner.hardening.enabled }}` guard and apply `nindent 10`. This is
     the container securityContext only -- the pod-level securityContext
     (runAsUser/fsGroup/seccomp) is a separate, non-duplicated block. */}}
{{- define "curie.sandboxHardening.securityContext" -}}
securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  runAsNonRoot: true
  capabilities:
    drop: [ALL]
{{- end -}}

{{/* ---- First-party service container securityContext ----
     The `securityContext:` + `toYaml` wrapper the four first-party services (api,
     worker, dispatcher, ui) each render from their own
     `.Values.<svc>.containerSecurityContext`. Extracted so the wrapper lives once
     (#493). Call with the container-security-context VALUE inside the existing
     `{{- with .Values.<svc>.containerSecurityContext }}` guard and apply
     `nindent 10`; the `with` handles the empty case exactly as before. */}}
{{- define "curie.containerSecurityContext" -}}
securityContext:
{{- toYaml . | nindent 2 }}
{{- end -}}

{{/* ---- ADR-0131 drain-budget relationship (worker) ----
     `worker.terminationGracePeriodSeconds` must cover
     `worker.deliveryBudgetSeconds` + `worker.deliveryShutdownReserveSeconds`.
     The chart renders that same grace value BOTH onto the Pod's
     `spec.terminationGracePeriodSeconds` and into the worker's
     `CURIE_TERMINATION_GRACE_PERIOD_S`, where `WorkerConfig` re-checks the
     inequality at boot -- and that check raises before `asyncio.run`, so the
     supervisor cannot catch it and the pod CrashLoopBackOffs.

     Without this render-time guard, an existing install that overrides
     `worker.terminationGracePeriodSeconds` to any value the schema accepts but
     the inequality rejects `helm upgrade`s CLEANLY and then takes the entire
     turn plane down: a silent breaking upgrade. `values.schema.json` cannot
     close it -- JSON Schema has no cross-field arithmetic -- and the CI
     render-assertion never sees operator values. So the fence has to be here,
     where `helm template`/`install`/`upgrade` all pass through it.

     This does NOT replace the worker's boot validator, which remains the
     backstop for the non-Helm substrates (Compose, bare env). It only moves the
     Helm-shaped failure from pod boot to render time, where it is actionable. */}}
{{/* Drop extraEnv entries whose names collide with first-class worker timeout
     and delivery-budget env. A v0.8.4 retained worker.extraEnv override of
     CURIE_RUNNER_TOTAL_TIMEOUT_S used to render a second copy next to the
     first-class key; Kubernetes then rejected the Deployment patch after Helm
     had begun applying other resources (#2097, 2026-09-04 soak). First-class
     values win. Non-colliding extraEnv entries still render. */}}
{{- define "curie.worker.extraEnv" -}}
{{- $reserved := dict
  "CURIE_CLAIM_TIMEOUT_SECONDS" true
  "CURIE_ROUTE_TTL_SECONDS" true
  "CURIE_SUSPENDED_ROUTE_TTL_SECONDS" true
  "CURIE_DELIVERY_BUDGET_S" true
  "CURIE_RUNNER_TOTAL_TIMEOUT_S" true
  "CURIE_DELIVERY_LEASE_TTL_S" true
  "CURIE_DELIVERY_LEASE_HEARTBEAT_S" true
  "CURIE_DELIVERY_SHUTDOWN_RESERVE_S" true
  "CURIE_TERMINATION_GRACE_PERIOD_S" true
-}}
{{- $kept := list -}}
{{- range .Values.worker.extraEnv }}
{{- if and .name (not (hasKey $reserved .name)) -}}
{{- $kept = append $kept . -}}
{{- end -}}
{{- end -}}
{{- if $kept -}}
{{- toYaml $kept -}}
{{- end -}}
{{- end -}}

{{- define "curie.worker.validateDrainBudget" -}}
{{- $grace := int64 .Values.worker.terminationGracePeriodSeconds -}}
{{- $budget := int64 .Values.worker.deliveryBudgetSeconds -}}
{{- $reserve := int64 .Values.worker.deliveryShutdownReserveSeconds -}}
{{- $required := add $budget $reserve -}}
{{- if lt $grace $required -}}
{{- fail (printf "worker.terminationGracePeriodSeconds (%d) must be at least worker.deliveryBudgetSeconds (%d) + worker.deliveryShutdownReserveSeconds (%d) = %d (ADR-0131). At %d a worker draining a full-budget delivery is SIGKILLed before it can settle, and the worker refuses this configuration at boot, so the Pod CrashLoopBackOffs instead of starting. Fix: raise worker.terminationGracePeriodSeconds to %d or more, or lower worker.deliveryBudgetSeconds and/or worker.deliveryShutdownReserveSeconds so their sum is at most %d." $grace $budget $reserve $required $grace $required $grace) -}}
{{- end -}}
{{- end -}}

{{/* ---- Resume reconciler grace arithmetic (API) ----
     The reconciler must wait through a worker's complete delivery budget and
     terminal-settlement reserve before it can enqueue a replacement resume
     turn. The default therefore derives from those two worker settings.

     An explicit api.resumeReconciler.graceSeconds is an operator-selected
     extension of that wait, not an independent timeout. Refusing a value below
     the derived floor prevents a clean Helm operation from producing a
     reconciler that can enqueue while the preceding delivery remains active. */}}
{{- define "curie.api.resumeReconciler.grace" -}}
{{- $budget := int64 .Values.worker.deliveryBudgetSeconds -}}
{{- $reserve := int64 .Values.worker.deliveryShutdownReserveSeconds -}}
{{- $required := add $budget $reserve -}}
{{- if and (hasKey .Values.api.resumeReconciler "graceSeconds") (not (kindIs "invalid" .Values.api.resumeReconciler.graceSeconds)) -}}
{{- $grace := int64 .Values.api.resumeReconciler.graceSeconds -}}
{{- if lt $grace $required -}}
{{- fail (printf "api.resumeReconciler.graceSeconds (%d) must be at least worker.deliveryBudgetSeconds (%d) + worker.deliveryShutdownReserveSeconds (%d) = %d. At %d, a duplicate resume delivery can reach an active turn and repeat an approved action. Fix: raise api.resumeReconciler.graceSeconds to %d or more, or lower worker.deliveryBudgetSeconds and/or worker.deliveryShutdownReserveSeconds so their sum is at most %d." $grace $budget $reserve $required $grace $required $grace) -}}
{{- end -}}
{{- $grace -}}
{{- else -}}
{{- $required -}}
{{- end -}}
{{- end -}}

{{/* ---- Runner ceiling / delivery-budget relationship (worker) ----
     Each runner request is bounded by `worker.runnerTotalTimeoutSeconds`, but
     the request still has to fit inside the delivery's overall
     `worker.deliveryBudgetSeconds`. JSON Schema cannot express this
     cross-field relationship, so refuse it before an invalid worker Pod is
     created. The worker's boot validator remains the backstop for non-Helm
     configuration. */}}
{{- define "curie.worker.validateRunnerBudget" -}}
{{- $runnerCeiling := float64 .Values.worker.runnerTotalTimeoutSeconds -}}
{{- $deliveryBudget := float64 .Values.worker.deliveryBudgetSeconds -}}
{{- if gt $runnerCeiling $deliveryBudget -}}
{{- fail (printf "worker.runnerTotalTimeoutSeconds (%g) must be <= worker.deliveryBudgetSeconds (%g). Fix: lower worker.runnerTotalTimeoutSeconds to %g or less, or raise worker.deliveryBudgetSeconds to at least %g." $runnerCeiling $deliveryBudget $deliveryBudget $runnerCeiling) -}}
{{- end -}}
{{- end -}}

{{/* ---- Upgrade drain gate arithmetic (issue #2010) ----
     The gate's two clocks are DERIVED, with the values as floors, and only a
     self-contradictory pair is refused outright. The split is deliberate.

     `timeoutSeconds` vs the delivery budget is a CROSS-FAMILY relationship an
     operator does not author together: raising `deliveryBudgetSeconds` is a
     decision about how long a turn may run, made for reasons that have nothing
     to do with upgrades. Refusing that render would break configurations that
     are valid today, on a chart upgrade, for a value the operator never touched
     -- so the effective wait is raised to cover the budget instead. The gate
     must never give up on a delivery that is still inside the budget ADR-0131
     already promised it; a gate that refuses upgrades during ordinary traffic
     is a gate that gets switched off in its first week.

     The quiesce TTL is then derived above that, because the worker's OWN boot
     validator refuses a TTL that does not outlast the wait -- so a rendered
     pair the app would reject is a green `helm upgrade` followed by a
     CrashLoopBackOff, the same failure `validateDrainBudget` above exists to
     prevent.

     What IS refused is the one pair an operator writes together and can only
     get wrong by contradicting themselves: a `quiesceTtlSeconds` at or below
     the `timeoutSeconds` they set beside it. Silently raising that one would
     hide a stated intent rather than an unrelated default. */}}
{{- define "curie.worker.upgradeDrain.timeout" -}}
{{- max (int64 .Values.worker.upgradeDrain.timeoutSeconds) (add (int64 .Values.worker.deliveryBudgetSeconds) (int64 .Values.worker.deliveryShutdownReserveSeconds)) -}}
{{- end -}}

{{/* Headroom over the effective wait, so the flag cannot lapse in the moments
     between the gate's last poll and the roll it clears the way for. */}}
{{- define "curie.worker.upgradeDrain.quiesceTtl" -}}
{{- max (int64 .Values.worker.upgradeDrain.quiesceTtlSeconds) (add (int64 (include "curie.worker.upgradeDrain.timeout" .)) 60) -}}
{{- end -}}

{{- define "curie.worker.validateUpgradeDrain" -}}
{{- $timeout := int64 .Values.worker.upgradeDrain.timeoutSeconds -}}
{{- $quiesce := int64 .Values.worker.upgradeDrain.quiesceTtlSeconds -}}
{{- if le $quiesce $timeout -}}
{{- fail (printf "worker.upgradeDrain.quiesceTtlSeconds (%d) must be strictly greater than worker.upgradeDrain.timeoutSeconds (%d) (issue #2010). As set, the fleet-wide quiesce flag lapses while the gate is still waiting, so the replicas resume claiming into a roll that is about to interrupt them -- and the gate would still report a clean drain. Fix: raise worker.upgradeDrain.quiesceTtlSeconds above %d, or lower worker.upgradeDrain.timeoutSeconds below %d." $quiesce $timeout $timeout $quiesce) -}}
{{- end -}}
{{- end -}}

{{/* ---- Langfuse ClickHouse startup gate (issue #2009) ----
     Both Langfuse deployments run their ClickHouse migrations during boot, so a
     Helm upgrade that recreates the ClickHouse Service can start them before the
     name resolves; Langfuse then exits with `failed to open database: dial tcp:
     lookup <release>-clickhouse ... no such host` and the rollout converges only
     through CrashLoopBackOff. This init container polls ClickHouse's HTTP
     `/ping` until it answers 200, so the application container is not started
     until the dependency is actually accepting connections -- the same
     wait-then-hand-over shape `templates/api.yaml` uses for Postgres.

     `node` is the Langfuse images' own runtime, so the probe needs no extra
     tooling in the image. Bounded like the Postgres gate: after `maxAttempts`
     polls the container exits non-zero and the kubelet restarts it, which keeps
     a genuinely-down ClickHouse visible instead of hanging forever. Every probe
     setting (attempts, interval, per-request timeout) comes from values rather
     than the template, per the chart's probe-settings invariant -- a BYO
     ClickHouse that answers slowly needs a longer timeout, not a patched chart.

     Call with a dict: `root` (the chart context), `image` (the component's
     fully-rendered image reference, built by curie.image so a digest pin
     reaches the gate too), `containerSecurityContext` and `resources` (the
     component's, so the gate inherits the same posture and the pod's effective
     request is unchanged -- init and app container requests are maxed, not
     summed). */}}
{{- define "curie.langfuse.clickhouseGate" -}}
{{- $root := .root -}}
- name: wait-for-clickhouse
  image: {{ .image | quote }}
  imagePullPolicy: {{ $root.Values.global.imagePullPolicy }}
  {{- with .containerSecurityContext }}
  securityContext:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  command: ["/bin/sh", "-c"]
  args:
    - |
      attempt=1
      max_attempts={{ $root.Values.langfuse.clickhouseReadiness.maxAttempts }}
      interval={{ $root.Values.langfuse.clickhouseReadiness.intervalSeconds }}
      probe_timeout_ms={{ mulf $root.Values.langfuse.clickhouseReadiness.timeoutSeconds 1000 | int }}
      while [ "$attempt" -le "$max_attempts" ]; do
        if PROBE_TIMEOUT_MS="$probe_timeout_ms" node -e '
      const client = require(process.env.CLICKHOUSE_URL.startsWith("https:") ? "https" : "http");
      const request = client.get(process.env.CLICKHOUSE_URL + "/ping", { timeout: Number(process.env.PROBE_TIMEOUT_MS) }, (response) => {
        response.resume();
        process.exit(response.statusCode === 200 ? 0 : 1);
      });
      request.on("timeout", () => { request.destroy(); process.exit(1); });
      request.on("error", () => { process.exit(1); });
      ' 2>/dev/null; then
          echo "ClickHouse ready after $attempt attempt(s); starting Langfuse"
          exit 0
        fi
        if [ "$attempt" -eq 1 ]; then
          echo "Waiting for ClickHouse readiness at $CLICKHOUSE_URL"
        fi
        if [ "$attempt" -lt "$max_attempts" ]; then
          sleep "$interval"
        fi
        attempt=$((attempt + 1))
      done
      echo "ClickHouse unreachable at $CLICKHOUSE_URL after $max_attempts readiness attempts; exiting for init container restart" >&2
      exit 1
  env:
    - name: CLICKHOUSE_URL
      value: {{ include "curie.clickhouse.httpUrl" $root }}
  resources:
    {{- toYaml .resources | nindent 4 }}
{{- end -}}
{{/*
Render the runner-sandbox egress rules for a list of {cidr, ports, except?}
entries. The dot IS the entry list, and the caller supplies the indentation:

    {{- include "curie.egress.ipBlockRules" .Values.api.egress | trim | nindent 4 }}

Every ipBlock peer rail 1 renders goes through here, so the metadata carve-out
is stated ONCE. That carve-out is the security invariant: NetworkPolicy allows
are additive, so an entry broad enough to contain 169.254.169.254 re-permits the
cloud metadata endpoint rail 1 otherwise denies. An explicit per-entry `except:`
list wins (including an empty one, a deliberate operator override); otherwise
curie.metadataExcept returns a same-family, subset-safe carve-out for ANY CIDR
that contains the metadata address -- not just an exact /0 -- and "" for CIDRs
that cannot reach it. See that helper for the containment/family rules.
*/}}
{{- define "curie.egress.ipBlockRules" -}}
{{- range . }}
- to:
    - ipBlock:
        cidr: {{ .cidr }}
        {{- if .except }}
        except:
          {{- toYaml .except | nindent 10 }}
        {{- else }}
        {{- $auto := include "curie.metadataExcept" .cidr | trim }}
        {{- if $auto }}
        except:
          - {{ $auto }}
        {{- end }}
        {{- end }}
  {{- with .ports }}
  ports:
    {{- toYaml . | nindent 4 }}
  {{- end }}
{{- end }}
{{- end -}}
