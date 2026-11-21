{{/* Shared helpers for the Curie chart. Store Service names are derived here; with `<dep>.deploy: false` (BYO) the operator host is returned. */}}

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

{{/* Fail a render when a fullname-derived object name exceeds the 63 character
     DNS and label limit. The helper returns the name unchanged. */}}
{{- define "curie.checkedObjectName" -}}
{{- $name := .name -}}
{{- if gt (len $name) 63 -}}
{{- fail (printf "%s name \"%s\" is %d characters; Kubernetes allows at most 63. Use a shorter release name or fullnameOverride." .kind $name (len $name)) -}}
{{- end -}}
{{- $name -}}
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

{{/* Stable labels for backing StatefulSet pod templates; no chart or app version labels, so metadata updates do not restart data pods. */}}
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

{{/* Resolve one placement class (dict: root, class); never index placement directly, since `placement: null` on legacy releases (#2008) must degrade to empty defaults. A non-map class is refused so constraints are never silently dropped. */}}
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

{{/* Chart-owned runner token Secret name (#3821), independent of every existingSecret override. */}}
{{- define "curie.runnerTokenSecretName" -}}
{{- printf "%s-runner-token" (include "curie.fullname" .) -}}
{{- end -}}

{{/* Resolve a store credential Secret, defaulting to the chart Secret. */}}
{{- define "curie.storeSecretName" -}}
{{- .store.existingSecret | default (include "curie.secretName" .root) -}}
{{- end -}}

{{/* Dedicated namespace for short-lived publication resources. */}}
{{- define "curie.publicationNamespace" -}}
{{- default (printf "%s-%s-publication" .Release.Namespace (include "curie.fullname" .)) .Values.worker.publication.namespace | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* ADR 0217: per-run Docker daemon namespace; factory.docker is read through default dict for --reuse-values. */}}
{{- define "curie.factory.dockerNamespace" -}}
{{- $docker := get (.Values.factory | default dict) "docker" | default dict -}}
{{- get $docker "namespace" | default (printf "%s-%s-docker" .Release.Namespace (include "curie.fullname" .)) | toString | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Reserved connector-secret boot-env names (#457, ADR-0009), parity copy of plugin-format reserved_env. test_reserved_boot_env_pin.py parses this body: keep exactly the enumerated keys. */}}
{{- define "curie.reservedConnectorSecretNames" -}}
ANTHROPIC_BASE_URL ANTHROPIC_API_KEY CLAUDE_CODE_OAUTH_TOKEN ANTHROPIC_AUTH_TOKEN HTTPS_PROXY HTTP_PROXY ALL_PROXY SSL_CERT_FILE SSL_CERT_DIR NODE_EXTRA_CA_CERTS NODE_TLS_REJECT_UNAUTHORIZED NODE_OPTIONS ANTHROPIC_CUSTOM_HEADERS https_proxy http_proxy all_proxy ssl_cert_file ssl_cert_dir node_extra_ca_certs node_tls_reject_unauthorized node_options anthropic_custom_headers
{{- end -}}

{{/* @spec ACTION-EXECUTOR-16: reserved snapshot sealing key names, kept apart from the boot-env drift-pinned define. */}}
{{- define "curie.sealingKeyNames" -}}
SNAPSHOT_SEALING_KEY SNAPSHOT_SEALING_KEYS_RETAINED
{{- end -}}

{{/* Per-agent literal runner environment; names follow plugin_format.reserved_env.SECRET_NAME_RE. */}}
{{- define "curie.agentSandboxRunnerEnv" -}}
{{- $env := dict -}}
{{- if hasKey .Values.agentSandbox "runnerEnv" -}}
{{- $env = get .Values.agentSandbox "runnerEnv" -}}
{{- if not (kindIs "map" $env) -}}
{{- fail "agentSandbox.runnerEnv must be a map of agent names to environment maps" -}}
{{- end -}}
{{- end -}}
{{- $reserved := splitList " " (trim (include "curie.reservedConnectorSecretNames" .)) -}}
{{- range $agent, $entries := $env -}}
{{- if or (gt (len $agent) 40) (not (regexMatch "^[a-z0-9]([a-z0-9-]*[a-z0-9])?$" $agent)) -}}
{{- fail (printf "agentSandbox.runnerEnv.%s has an invalid agent name" $agent) -}}
{{- end -}}
{{- if not (kindIs "map" $entries) -}}
{{- fail (printf "agentSandbox.runnerEnv.%s must be a map of environment names to strings" $agent) -}}
{{- end -}}
{{- range $name, $value := $entries -}}
{{- if not (regexMatch "^[A-Z_][A-Z0-9_]*$" $name) -}}
{{- fail (printf "agentSandbox.runnerEnv.%s contains invalid environment variable name %q" $agent $name) -}}
{{- end -}}
{{- if or (hasPrefix "CURIE_" $name) (has $name $reserved) -}}
{{- fail (printf "agentSandbox.runnerEnv.%s contains reserved environment variable %s; configure the runner credentials or platform values instead" $agent $name) -}}
{{- end -}}
{{- if hasPrefix "OTEL_" $name -}}
{{- fail (printf "agentSandbox.runnerEnv.%s contains telemetry environment variable %s; configure otelCollector or agentSandbox.runner.extraEnv globally so chart-managed telemetry egress matches the endpoint" $agent $name) -}}
{{- end -}}
{{- if not (kindIs "string" $value) -}}
{{- fail (printf "agentSandbox.runnerEnv.%s.%s must be a string; quote numeric and boolean values" $agent $name) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- toJson $env -}}
{{- end -}}

{{/* ---- Backing-store hosts (in-cluster Service name, or BYO host) ---- */}}

{{- define "curie.postgres.host" -}}
{{- if .Values.postgres.deploy -}}
{{- printf "%s-postgres" (include "curie.fullname" .) -}}
{{- else -}}
{{- required "postgres.deploy is false: set postgres.host to your external Postgres" .Values.postgres.host -}}
{{- end -}}
{{- end -}}

{{/* Per-driver TLS suffix for a Postgres DSN (#2431, #2507), shared by curie.env.postgres and curie.langfuse.env. Only "require" is accepted (read raw). Refused with the in-chart Postgres (no TLS). No certificate verification (#2508, #2476). */}}
{{- define "curie.postgres.dsnParams" -}}
{{- $root := required "curie.postgres.dsnParams requires root" .root -}}
{{- $driver := required "curie.postgres.dsnParams requires driver" .driver -}}
{{- if not (has $driver (list "asyncpg" "prisma")) -}}
{{- fail (printf "curie.postgres.dsnParams driver must be asyncpg or prisma, got %q" $driver) -}}
{{- end -}}
{{- $mode := index $root.Values.postgres "sslMode" -}}
{{- $empty := or (not (hasKey $root.Values.postgres "sslMode")) (kindIs "invalid" $mode) (and (kindIs "string" $mode) (eq $mode "")) -}}
{{- if $empty -}}
{{- else if eq (toString $mode) "require" -}}
{{- if $root.Values.postgres.deploy -}}
{{- fail "postgres.sslMode is require but postgres.deploy is also true: the in-chart Postgres serves no TLS listener, so every consumer would fail to connect. Set postgres.deploy=false and point postgres.host at your external TLS store, or leave postgres.sslMode empty." -}}
{{- end -}}
{{- if eq $driver "asyncpg" -}}?ssl=require{{- else -}}?sslmode=require&sslaccept=accept_invalid_certs{{- end -}}
{{- else -}}
{{- fail (printf "postgres.sslMode must be empty or \"require\", got %q" (printf "%v" $mode)) -}}
{{- end -}}
{{- end -}}

{{- define "curie.valkey.host" -}}
{{- if .Values.valkey.deploy -}}
{{- printf "%s-valkey" (include "curie.fullname" .) -}}
{{- else -}}
{{- required "valkey.deploy is false: set valkey.host to your external Valkey/Redis" .Values.valkey.host -}}
{{- end -}}
{{- end -}}

{{/* Whether consumers reach Valkey over TLS, "true" or "false" (#2315). One helper for curie.env.valkey and curie.langfuse.env so groups cannot diverge (#2052, #2327). Refused against the in-chart Valkey. */}}
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

{{/* ClickHouse URL scheme for Langfuse (#2314), like curie.rustfs.scheme (#1447): explicit clickhouse.scheme, else 8443 is TLS, else http. */}}
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

{{/* The one HTTP endpoint every ClickHouse consumer uses; do not re-compose scheme://host:port (#2314). */}}
{{- define "curie.clickhouse.httpUrl" -}}
{{- include "curie.clickhouse.scheme" . }}://{{ include "curie.clickhouse.host" . }}:{{ .Values.clickhouse.httpPort }}
{{- end -}}

{{/* Langfuse ClickHouse migration DSN on the native port. Keep it bare: Langfuse's up.sh appends its own query string. */}}
{{- define "curie.clickhouse.migrationUrl" -}}
clickhouse://{{ include "curie.clickhouse.host" . }}:{{ .Values.clickhouse.nativePort }}
{{- end -}}

{{/* Whether the Langfuse migration connection on the native port uses TLS (#2314); tracks the HTTP scheme. */}}
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

{{/* RustFS data claim storage request (#4408). volumeClaimTemplates are immutable, so keep the existing StatefulSet's size, else default. */}}
{{- define "curie.rustfs.claimSize" -}}
{{- $size := "" -}}
{{- range (dig "spec" "volumeClaimTemplates" list (.statefulSet | default dict)) -}}
{{- if and (not $size) (eq (dig "metadata" "name" "" .) "data") -}}
{{- $size = dig "spec" "resources" "requests" "storage" "" . -}}
{{- end -}}
{{- end -}}
{{- $size | default .default -}}
{{- end -}}

{{/* Whether object-store clients present static credentials. Empty rustfs.auth.accessKey is the BYO key-free path (#1325): credential env is omitted and the SDK uses web identity. Web identity is the only key-free path; do not make the node instance role work, since Rail 1 denies 169.254.169.254 and IMDS would expose the node IAM role to the runner. */}}
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

{{/* Langfuse URL scheme (#2314), like curie.rustfs.scheme (#1447): explicit langfuse.scheme, else BYO on 443 is TLS, else http. */}}
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

{{/* The one Langfuse base URL; call sites include this helper rather than re-composing scheme://host:port (#2314). */}}
{{- define "curie.langfuse.url" -}}
{{- include "curie.langfuse.scheme" . }}://{{ include "curie.langfuse.webHost" . }}:{{ .Values.langfuse.web.service.port }}
{{- end -}}

{{/* Shared ServiceAccount name for both Langfuse Deployments; empty langfuse.serviceAccount.name falls back to <release>-langfuse (#2211). */}}
{{- define "curie.langfuse.serviceAccountName" -}}
{{- .Values.langfuse.serviceAccount.name | default (printf "%s-langfuse" (include "curie.fullname" .)) -}}
{{- end -}}

{{/* Platform API base URL for first-party callers (dict: root, baseUrl BYO override); separate from curie.env.api so callers do not get the API key. */}}
{{- define "curie.api.url" -}}
{{- .baseUrl | default (printf "http://%s-api:%v" (include "curie.fullname" .root) .root.Values.api.service.port) -}}
{{- end -}}

{{/* base64(publicKey:secretKey) for the collector checksum and default-credential gate (#1563), from raw .Values so generated credentials do not churn it. Empty when the collector reads a Secret this chart does not manage. */}}
{{- define "curie.otlpAuthHeader" -}}
{{- if .Values.otelCollector.otlpAuthHeader -}}
{{- .Values.otelCollector.otlpAuthHeader -}}
{{- else if ne (include "curie.otlpAuthHeaderSecretName" .) (include "curie.secretName" .) -}}
{{- else -}}
{{- printf "Basic %s" (printf "%s:%s" .Values.langfuse.init.projectPublicKey .Values.langfuse.init.projectSecretKey | b64enc) -}}
{{- end -}}
{{- end -}}

{{/* Secret the collector reads otlpAuthHeader from: the chart Secret when otelCollector.otlpAuthHeader is set, else Langfuse BYO (#169). Must agree with secrets.yaml (#1563). */}}
{{- define "curie.otlpAuthHeaderSecretName" -}}
{{- if .Values.otelCollector.otlpAuthHeader -}}
{{- include "curie.secretName" . -}}
{{- else -}}
{{- include "curie.storeSecretName" (dict "root" . "store" .Values.langfuse) -}}
{{- end -}}
{{- end -}}

{{/* Reject absent, malformed, negative and zero-equivalent retry durations; keep max_interval and max_elapsed_time on this one validation path. */}}
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
{{- /* @spec charts/curie/README.md: Optional trace processors. */ -}}
{{- $extraProcessors := dict -}}
{{- if hasKey .Values.otelCollector "extraProcessors" -}}
{{- $extraProcessors = get .Values.otelCollector "extraProcessors" -}}
{{- end -}}
{{- $traceProcessors := list -}}
{{- if hasKey .Values.otelCollector "extraTracePipelineProcessors" -}}
{{- $traceProcessors = get .Values.otelCollector "extraTracePipelineProcessors" -}}
{{- end -}}
{{- $builtInProcessors := dict "memory_limiter" true "batch" true "transform/runner_identity" true "filter/langfuse_platform_noise" true -}}
{{- $componentIDPattern := "^[a-z][a-z0-9_]*(/[A-Za-z0-9_-]+)?$" -}}
{{- if not (kindIs "map" $extraProcessors) -}}
{{- fail "otelCollector.extraProcessors must be a map of Collector processor IDs to configuration maps." -}}
{{- end -}}
{{- if not (kindIs "slice" $traceProcessors) -}}
{{- fail "otelCollector.extraTracePipelineProcessors must be a list of processor IDs." -}}
{{- end -}}
{{- range $name, $config := $extraProcessors -}}
{{- if hasKey $builtInProcessors $name -}}
{{- fail (printf "otelCollector.extraProcessors[%q] must not replace built-in processor %q." $name $name) -}}
{{- end -}}
{{- if not (regexMatch $componentIDPattern $name) -}}
{{- fail (printf "otelCollector.extraProcessors[%q] has invalid component ID." $name) -}}
{{- end -}}
{{- if not (kindIs "map" $config) -}}
{{- fail (printf "otelCollector.extraProcessors[%q] must be a map." $name) -}}
{{- end -}}
{{- end -}}
{{- $seenTraceProcessors := dict -}}
{{- range $processor := $traceProcessors -}}
{{- if not (kindIs "string" $processor) -}}
{{- fail "otelCollector.extraTracePipelineProcessors entries must be processor IDs." -}}
{{- end -}}
{{- if hasKey $builtInProcessors $processor -}}
{{- fail (printf "otelCollector.extraTracePipelineProcessors must not select built-in processor %q." $processor) -}}
{{- end -}}
{{- if hasKey $seenTraceProcessors $processor -}}
{{- fail (printf "otelCollector.extraTracePipelineProcessors duplicates processor %q." $processor) -}}
{{- end -}}
{{- if not (hasKey $extraProcessors $processor) -}}
{{- fail (printf "otelCollector.extraTracePipelineProcessors references undefined processor %q." $processor) -}}
{{- end -}}
{{- $_ := set $seenTraceProcessors $processor true -}}
{{- end -}}
{{- $builtInExporters := dict "otlphttp/langfuse" true "nop/logs" true "nop/metrics" true -}}
{{- /* Reserve built-in exporter names even when the debug exporter is disabled, so an extra exporter cannot replace chart-owned configuration. */ -}}
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
{{- $eventsEnabled := (get (.Values.otelCollector.kubernetesEvents | default dict) "enabled") -}}
{{- if and $eventsEnabled (not $debugEnabled) (eq (len .Values.otelCollector.extraLogPipelineExporters) 0) -}}
{{- fail "otelCollector.kubernetesEvents.enabled routes Kubernetes Events into the logs pipeline, which exports only to nop by default. Set otelCollector.extraLogPipelineExporters to a durable log exporter (or enable debugExporter) so the events are recorded." -}}
{{- end -}}
{{- /* traces/langfuse always exports the filtered stream to Langfuse; the unfiltered pipeline is for debug and extra exporters (#5496). */ -}}
{{- $extraTraceExporters := .Values.otelCollector.extraPipelineExporters | default list -}}
{{- $unfilteredTraceExporters := list -}}
{{- if $debugEnabled -}}
{{- $unfilteredTraceExporters = append $unfilteredTraceExporters "debug" -}}
{{- end -}}
{{- $unfilteredTraceExporters = concat $unfilteredTraceExporters $extraTraceExporters -}}
{{- /* @spec charts/curie/README.md: Collector exporters without exporterhelper. Keep each exemption explicit, documented, and attached to a configured exporter. */ -}}
{{- $noHelper := .Values.otelCollector.exportersWithoutExporterHelper | default dict -}}
{{- range $name, $reason := $noHelper -}}
{{- if not (hasKey $.Values.otelCollector.extraExporters $name) -}}
{{- fail (printf "otelCollector.exportersWithoutExporterHelper[%q] names no configured exporter. Remove it, or add the exporter under otelCollector.extraExporters." $name) -}}
{{- end -}}
{{- if ne (first (splitList "/" $name)) "awsemf" -}}
{{- fail (printf "otelCollector.exportersWithoutExporterHelper[%q] is invalid: only awsemf exporters may bypass exporterhelper durability validation on Collector 0.119.0." $name) -}}
{{- end -}}
{{- if not (and (kindIs "string" $reason) (trim $reason)) -}}
{{- fail (printf "otelCollector.exportersWithoutExporterHelper[%q] must give a reason, naming the exporter and the Collector version its schema was checked against." $name) -}}
{{- end -}}
{{- end -}}
{{- range $name, $config := .Values.otelCollector.extraExporters -}}
{{- if hasKey $reservedExporterNames $name -}}
{{- fail (printf "otelCollector.extraExporters[%q] must not replace built-in exporter %q." $name $name) -}}
{{- end -}}
{{- $exporterType := first (splitList "/" $name) -}}
{{- if not (or (eq $exporterType "nop") (eq $exporterType "debug")) -}}
{{- if not (kindIs "map" $config) -}}
{{- fail (printf "otelCollector.extraExporters[%q] is a network exporter and must be a map with retry_on_failure and a bounded queue." $name) -}}
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
{{- if not (hasKey $noHelper $name) -}}
{{- $retry := get $config "retry_on_failure" -}}
{{- if not (kindIs "map" $retry) -}}
{{- fail (printf "otelCollector.extraExporters[%q] must configure retry_on_failure with enabled: true and finite max_interval/max_elapsed_time." $name) -}}
{{- end -}}
{{- if ne (lower (toString (get $retry "enabled"))) "true" -}}
{{- fail (printf "otelCollector.extraExporters[%q] retry_on_failure must be enabled and use finite, non-zero max_interval and max_elapsed_time values." $name) -}}
{{- end -}}
{{- include "curie.otelCollector.requirePositiveDuration" (dict "exporter" $name "field" "max_interval" "value" (get $retry "max_interval")) -}}
{{- include "curie.otelCollector.requirePositiveDuration" (dict "exporter" $name "field" "max_elapsed_time" "value" (get $retry "max_elapsed_time")) -}}
{{- if eq $exporterType "prometheusremotewrite" -}}
{{- if hasKey $config "sending_queue" -}}
{{- fail (printf "otelCollector.extraExporters[%q] is prometheusremotewrite and must use remote_write_queue, not sending_queue. Collector 0.119 rejects sending_queue on this exporter." $name) -}}
{{- end -}}
{{- $queue := get $config "remote_write_queue" -}}
{{- if not (kindIs "map" $queue) -}}
{{- fail (printf "otelCollector.extraExporters[%q] must configure remote_write_queue with enabled: true and a finite queue_size." $name) -}}
{{- end -}}
{{- $queueSize := int (get $queue "queue_size") -}}
{{- if or (ne (lower (toString (get $queue "enabled"))) "true") (le $queueSize 0) (gt $queueSize 100000) -}}
{{- fail (printf "otelCollector.extraExporters[%q] remote_write_queue must be enabled and set queue_size between 1 and 100000." $name) -}}
{{- end -}}
{{- else -}}
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
{{- end -}}
{{- end -}}
# Receives OTLP traces, logs, and metrics over gRPC (4317) and HTTP (4318).
# traces/langfuse drops platform noise and exports only to Langfuse over HTTP.
# The traces pipeline, when debug or an extra trace exporter is configured,
# keeps those spans for that backend. Logs and metrics retain explicit nop
# defaults until #1765 supplies their backends. Langfuse OTLP ingest is
# HTTP-only (gRPC is silently unsupported), so the collector adapts trace
# traffic to otlphttp.
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318
{{- if $eventsEnabled }}
  # Kubernetes Events for the release namespace (#2954), watched so each new
  # or updated Event becomes one log record in the logs pipeline.
  k8sobjects/events:
    auth_type: serviceAccount
    objects:
      - name: events
        group: events.k8s.io
        mode: watch
        namespaces: [{{ .Release.Namespace | quote }}]
{{- end }}
processors:
  memory_limiter:
    check_interval: {{ .Values.otelCollector.memoryLimiter.checkInterval }}
    limit_percentage: {{ .Values.otelCollector.memoryLimiter.limitPercentage }}
    spike_limit_percentage: {{ .Values.otelCollector.memoryLimiter.spikeLimitPercentage }}
  batch: {}
  filter/langfuse_platform_noise:
    error_mode: ignore
    traces:
      span:
        - >-
          name == "http.server.request" and
          attributes["service.name"] == "curie-api" and
          attributes["curie.source"] == "GET" and
          (attributes["curie.operation"] == "/health" or
          attributes["curie.operation"] == "/ready" or
          attributes["curie.operation"] == "/agents" or
          attributes["curie.operation"] == "/agents/{agent_id}" or
          attributes["curie.operation"] == "/approvals" or
          attributes["curie.operation"] == "/approvals/{approval_id}" or
          attributes["curie.operation"] == "/approvals/{approval_id}/audit" or
          attributes["curie.operation"] == "/langfuse/traces" or
          attributes["curie.operation"] == "/langfuse/traces/{trace_id}" or
          attributes["curie.operation"] == "/observability/metrics/summary" or
          attributes["curie.operation"] == "/observability/metrics/series" or
          attributes["curie.operation"] == "/observability/runners" or
          attributes["curie.operation"] == "/cluster-message-replies/{reply_ref}" or
          attributes["curie.operation"] == "/v1/internal/work-items/runtime-owners")
        - 'IsMatch(name, "^curie\\.background\\.")'
        - 'name == "curie.sandbox.cleanup"'
  transform/runner_identity:
    error_mode: ignore
    metric_statements:
      - context: datapoint
        statements:
          - 'set(attributes["service.instance.id"], resource.attributes["service.instance.id"]) where resource.attributes["service.name"] == "curie-runner"'
{{- with $extraProcessors }}
{{ toYaml . | nindent 2 }}
{{- end }}
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
    traces/langfuse:
      receivers: [otlp]
      processors: [memory_limiter{{- range $traceProcessors }}, {{ . }}{{- end }}, filter/langfuse_platform_noise, batch]
      exporters: [otlphttp/langfuse]
{{- if $unfilteredTraceExporters }}
    traces:
      receivers: [otlp]
      processors: [memory_limiter{{- range $traceProcessors }}, {{ . }}{{- end }}, batch]
      exporters: [{{ join ", " $unfilteredTraceExporters }}]
{{- end }}
    logs:
      receivers: [otlp{{- if $eventsEnabled }}, k8sobjects/events{{- end }}]
      processors: [memory_limiter, batch]
      exporters: [nop/logs{{- if $debugEnabled }}, debug{{- end }}{{- range .Values.otelCollector.extraLogPipelineExporters }}, {{ . }}{{- end }}]
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, transform/runner_identity, batch]
      exporters: [nop/metrics{{- if $debugEnabled }}, debug{{- end }}{{- range .Values.otelCollector.extraMetricPipelineExporters }}, {{ . }}{{- end }}]
{{- end }}

{{/* Chart-owned OTLP destination (#1819): in-cluster Service while deploy is true, else otelCollector.endpoint; empty when telemetry is disabled. */}}
{{- define "curie.otel.endpoint" -}}
{{- if .Values.otelCollector.deploy -}}
http://{{ include "curie.fullname" . }}-otel-collector:{{ .Values.otelCollector.service.httpPort }}
{{- else if not .Values.otelCollector.telemetryDisabled -}}
{{- .Values.otelCollector.endpoint | default "" -}}
{{- end -}}
{{- end -}}

{{/* Fail closed on contradictory OTEL values always; on accidental missing only when security.checkDefaultCredentials is on. */}}
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
{{- $temporality := "delta" -}}
{{- if hasKey $otel "metricsTemporalityPreference" -}}
{{- $temporality = get $otel "metricsTemporalityPreference" -}}
{{- end -}}
{{- if not (and (kindIs "string" $temporality) (has $temporality (list "delta" "cumulative" "lowmemory"))) -}}
{{- fail "otelCollector.metricsTemporalityPreference must be delta, cumulative, or lowmemory." -}}
{{- end -}}
{{- if and .Values.security.checkDefaultCredentials (not $otel.deploy) (not $otel.telemetryDisabled) (empty $otel.endpoint) -}}
{{- fail "security.checkDefaultCredentials is on but neither a chart-managed collector nor otelCollector.endpoint is configured. Set otelCollector.endpoint to the external collector, keep otelCollector.deploy true, or set otelCollector.telemetryDisabled=true to acknowledge that telemetry is disabled." -}}
{{- end -}}
{{- end -}}

{{/* Standard OTEL_EXPORTER_OTLP_* env for every instrumented workload; extraEnv wins per variable. nindent 12. Dict: root, extraEnv. */}}
{{- define "curie.env.otel" -}}
{{- include "curie.otel.validate" .root -}}
{{- $extra := .extraEnv | default list -}}
{{- $hasEndpoint := false -}}
{{- $hasProtocol := false -}}
{{- $hasHeaders := false -}}
{{- $hasMetricsTemporalityPreference := false -}}
{{- range $extra -}}
{{- if eq .name "OTEL_EXPORTER_OTLP_ENDPOINT" -}}{{- $hasEndpoint = true -}}{{- end -}}
{{- if eq .name "OTEL_EXPORTER_OTLP_PROTOCOL" -}}{{- $hasProtocol = true -}}{{- end -}}
{{- if eq .name "OTEL_EXPORTER_OTLP_HEADERS" -}}{{- $hasHeaders = true -}}{{- end -}}
{{- if eq .name "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE" -}}{{- $hasMetricsTemporalityPreference = true -}}{{- end -}}
{{- end -}}
{{- $endpoint := include "curie.otel.endpoint" .root | trim -}}
{{- $protocol := .root.Values.otelCollector.protocol | default "http/protobuf" -}}
{{- $temporality := .root.Values.otelCollector.metricsTemporalityPreference | default "delta" -}}
{{- if and (not $hasEndpoint) (ne $endpoint "") }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ $endpoint | quote }}
{{- end }}
{{- if and (not $hasProtocol) (ne $endpoint "") }}
- name: OTEL_EXPORTER_OTLP_PROTOCOL
  value: {{ $protocol | quote }}
{{- end }}
{{- if and (not $hasMetricsTemporalityPreference) (ne $endpoint "") }}
- name: OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE
  value: {{ $temporality | quote }}
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

{{/* Default-credential gate (#198): with security.checkDefaultCredentials on, refuse a Langfuse bootstrap input carrying the published dev default. The first two checks compare inputs and are cleared by an override or an existingSecret this chart does not manage (#169); do not guard on existingSecret merely being non-empty. The third, on the rendered collector header (#1563), is unconditional. */}}
{{- define "curie.checkDefaultCredentials" -}}
{{- if .Values.security.checkDefaultCredentials -}}
{{- if eq (include "curie.storeSecretName" (dict "root" . "store" .Values.langfuse)) (include "curie.secretName" .) -}}
{{- if eq .Values.langfuse.init.projectSecretKey "sk-lf-curie-dev" -}}
{{- fail "security.checkDefaultCredentials is on but langfuse.init.projectSecretKey is still the published dev default \"sk-lf-curie-dev\". Override it (or set langfuse.existingSecret) before installing on a shared/production cluster -- this key also feeds the OTel Collector auth header." -}}
{{- end -}}
{{- if eq .Values.langfuse.init.userPassword "curie-dev-password" -}}
{{- fail "security.checkDefaultCredentials is on but langfuse.init.userPassword is still the published dev default \"curie-dev-password\". Override it (or set langfuse.existingSecret) before installing on a shared/production cluster -- the published admin password allows Langfuse admin takeover on a reachable UI." -}}
{{- end -}}
{{- end -}}
{{/* Outside the existingSecret guard on purpose (#1563): the header reaches the collector on every path. Operand is the rendered curie.otlpAuthHeader. Guards accidental shipping of the published credential, not an adversary: normalise only scheme case and whitespace, do not chase deliberate encodings. Independent of otelCollector.deploy. */}}
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

{{/* Resolve the installation identity once per render. Sealed installs mint a new identity; dev derives one from namespace and release; upgrades reuse the stored one (the first upgrade from a chart without installationId adopts the live Secret UID). A client-only upgrade gets a placeholder marked unobserved so the hook refuses before contacting Valkey. */}}
{{- define "curie.installationIdentity.init" -}}
{{- if not (hasKey . "_curieManagedSecret") -}}
{{- $managedSecret := lookup "v1" "Secret" .Release.Namespace (include "curie.secretName" .) | default dict -}}
{{- $_ := set . "_curieManagedSecret" $managedSecret -}}
{{- $managedSecretData := get $managedSecret "data" | default dict -}}
{{- $installationId := "" -}}
{{- $observed := true -}}
{{- $legacy := false -}}
{{- if .Release.IsInstall -}}
{{- if eq (toString .Values.security.allowDevDefaults) "true" -}}
{{- /* #1135, ADR-0144: dev renders are reproducible; sealed installs remain unique. */ -}}
{{- $installationId = (list .Release.Namespace .Release.Name | toJson | sha256sum | trunc 32) -}}
{{- else -}}
{{- $installationId = randAlphaNum 32 -}}
{{- end -}}
{{- else if .Release.IsUpgrade -}}
{{- $encodedId := get $managedSecretData "installationId" | default "" -}}
{{- $decodedId := "" -}}
{{- if ne (trim (toString $encodedId)) "" -}}
{{- $decodedId = $encodedId | b64dec -}}
{{- end -}}
{{- if ne (trim $decodedId) "" -}}
{{- $installationId = $decodedId -}}
{{- else -}}
{{- $legacy = true -}}
{{- $metadata := get $managedSecret "metadata" | default dict -}}
{{- $secretUid := get $metadata "uid" | default "" -}}
{{- if ne (trim (toString $secretUid)) "" -}}
{{- $installationId = toString $secretUid -}}
{{- else -}}
{{- $installationId = randAlphaNum 32 -}}
{{- $observed = false -}}
{{- end -}}
{{- end -}}
{{- else -}}
{{- if eq (toString .Values.security.allowDevDefaults) "true" -}}
{{- /* #1135, ADR-0144: dev renders are reproducible; sealed installs remain unique. */ -}}
{{- $installationId = (list .Release.Namespace .Release.Name | toJson | sha256sum | trunc 32) -}}
{{- else -}}
{{- $installationId = randAlphaNum 32 -}}
{{- end -}}
{{- end -}}
{{- $_ = set . "_curieInstallationId" $installationId -}}
{{- $_ = set . "_curieInstallationIdObserved" $observed -}}
{{- $_ = set . "_curieUpgradeLegacyQuiesce" $legacy -}}
{{- end -}}
{{- end -}}

{{- define "curie.installationId" -}}
{{- include "curie.installationIdentity.init" . -}}
{{- get . "_curieInstallationId" -}}
{{- end -}}

{{- define "curie.installationIdObserved" -}}
{{- include "curie.installationIdentity.init" . -}}
{{- get . "_curieInstallationIdObserved" | toString -}}
{{- end -}}

{{- define "curie.upgradeLegacyQuiesce" -}}
{{- include "curie.installationIdentity.init" . -}}
{{- get . "_curieUpgradeLegacyQuiesce" | toString -}}
{{- end -}}

{{/* Resolve one chart-owned secret value (#195); dict: root, key, value, default, hex. Precedence: 1 allowDevDefaults via eq (toString) "true", not truthiness. 2 explicit override wins on upgrade, before persist. 3 persist a nonblank .existingData value: upgrade must never rotate a live store credential. 4 generate randAlphaNum. */}}
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

{{/* Resolve the full plaintext Secret once per root render (#1135) so checksums, emission and the derived collector header cannot disagree. */}}
{{- define "curie.secretContent.init" -}}
{{- if not (hasKey . "_curieSecretContent") -}}
{{- include "curie.checkDefaultCredentials" . }}
{{- if eq (toString .Values.api.approvalChatAttesterSecret) (toString .Values.api.apiKey) }}
{{- fail "api.approvalChatAttesterSecret and api.apiKey must differ. Set the Slack approval attestation credential independently; neither value is printed because both are live credentials." }}
{{- end }}
{{- if eq (toString .Values.api.remediationAuthorityKey) (toString .Values.api.apiKey) }}
{{- fail "api.remediationAuthorityKey and api.apiKey must differ. Binding and arming a remediation policy need an authority the platform key cannot derive (ADR 0231); neither value is printed because both are live credentials." }}
{{- end }}
{{- include "curie.installationIdentity.init" . -}}
{{- $existingSecret := get . "_curieManagedSecret" | default dict -}}
{{- $existingSecretData := $existingSecret.data | default dict -}}
{{- include "curie.connectorCallerRefuseKeylessDevExit" (dict "root" . "existingData" $existingSecretData) -}}
{{- include "curie.testInstallation.check" (dict "root" . "existingData" $existingSecretData) -}}
{{- $apiKey := .Values.api.apiKey -}}
{{- if kindIs "invalid" $apiKey -}}
{{- $apiKey = "curie-dev-key" -}}
{{- end -}}
{{- if eq (trim (toString $apiKey)) "" -}}
{{- fail "api.apiKey must be nonblank. Set a nonblank API key." -}}
{{- end -}}
{{- if and (ne (toString .Values.security.allowDevDefaults) "true") (eq (toString $apiKey) "curie-dev-key") (hasKey $existingSecretData "apiKey") -}}
{{- if eq (trim (index $existingSecretData "apiKey" | b64dec)) "" -}}
{{- fail "api.apiKey: retained apiKey in the existing chart managed Secret must be nonblank. Set api.apiKey to a nonblank replacement." -}}
{{- end -}}
{{- end -}}
{{- $langfuseInitProjectSecretKey := include "curie.managedSecret" (dict "root" . "key" "langfuseInitProjectSecretKey" "value" .Values.langfuse.init.projectSecretKey "default" "sk-lf-curie-dev" "hex" false "existingData" $existingSecretData) -}}
{{- $langfuseInitUserPassword := include "curie.managedSecret" (dict "root" . "key" "langfuseInitUserPassword" "value" .Values.langfuse.init.userPassword "default" "curie-dev-password" "hex" false "existingData" $existingSecretData) -}}
{{- $content := dict -}}
{{- $gateway := .Values.channelGateway | default dict -}}
{{- range $caller := list "api" "worker" -}}
{{- $key := printf "channelGateway%sToken" (title $caller) -}}
{{- $_ := set $content $key (include "curie.managedSecret" (dict "root" $ "key" $key "value" (index $gateway (printf "%sToken" $caller)) "default" (printf "curie-dev-gateway-%s" $caller) "hex" false "existingData" $existingSecretData)) -}}
{{- end -}}

{{- $_ := set $content "installationId" ((include "curie.installationId" .) | toString) -}}
{{- $_ := set $content "postgresPassword" ((include "curie.managedSecret" (dict "root" . "key" "postgresPassword" "value" .Values.postgres.auth.password "default" "postgres" "hex" false "existingData" $existingSecretData)) | toString) -}}
{{- $_ := set $content "valkeyPassword" ((include "curie.managedSecret" (dict "root" . "key" "valkeyPassword" "value" .Values.valkey.password "default" "valkeypass" "hex" false "existingData" $existingSecretData)) | toString) -}}
{{- $_ := set $content "clickhousePassword" ((include "curie.managedSecret" (dict "root" . "key" "clickhousePassword" "value" .Values.clickhouse.auth.password "default" "clickhouse" "hex" false "existingData" $existingSecretData)) | toString) -}}
{{- $_ := set $content "rustfsSecretKey" ((include "curie.managedSecret" (dict "root" . "key" "rustfsSecretKey" "value" .Values.rustfs.auth.secretKey "default" "rustfssecret" "hex" false "existingData" $existingSecretData)) | toString) -}}
{{- $_ := set $content "langfuseSalt" ((include "curie.managedSecret" (dict "root" . "key" "langfuseSalt" "value" .Values.langfuse.salt "default" "dev-salt-change-me" "hex" false "existingData" $existingSecretData)) | toString) -}}
{{- $_ := set $content "langfuseEncryptionKey" ((include "curie.managedSecret" (dict "root" . "key" "langfuseEncryptionKey" "value" .Values.langfuse.encryptionKey "default" "0000000000000000000000000000000000000000000000000000000000000000" "hex" true "existingData" $existingSecretData)) | toString) -}}
{{- $_ := set $content "langfuseNextauthSecret" ((include "curie.managedSecret" (dict "root" . "key" "langfuseNextauthSecret" "value" .Values.langfuse.nextauthSecret "default" "dev-nextauth-secret-change-me" "hex" false "existingData" $existingSecretData)) | toString) -}}
{{- $_ := set $content "langfuseInitProjectSecretKey" (($langfuseInitProjectSecretKey) | toString) -}}
{{- $_ := set $content "langfuseInitUserPassword" (($langfuseInitUserPassword) | toString) -}}
{{- if eq (include "curie.otlpAuthHeaderSecretName" .) (include "curie.secretName" .) -}}
{{- $_ := set $content "otlpAuthHeader" ((.Values.otelCollector.otlpAuthHeader | default (printf "Basic %s" (printf "%s:%s" .Values.langfuse.init.projectPublicKey $langfuseInitProjectSecretKey | b64enc))) | toString) -}}
{{- end -}}
{{- $_ := set $content "agentCredentials" (ternary "" (toString .Values.agentSandbox.runner.credentials) (kindIs "invalid" .Values.agentSandbox.runner.credentials)) -}}
{{- $_ := set $content "adapterCredentials" ((include "curie.adapterCredentials" .) | toString) -}}
{{- $_ := set $content "apiKey" ((include "curie.managedSecret" (dict "root" . "key" "apiKey" "value" .Values.api.apiKey "default" "curie-dev-key" "hex" false "existingData" $existingSecretData)) | toString) -}}
{{- $_ := set $content "approvalChatAttesterSecret" ((include "curie.managedSecret" (dict "root" . "key" "approvalChatAttesterSecret" "value" .Values.api.approvalChatAttesterSecret "default" "curie-dev-approval-chat-attester" "hex" false "existingData" $existingSecretData)) | toString) -}}
{{- $_ := set $content "remediationAuthorityKey" ((include "curie.managedSecret" (dict "root" . "key" "remediationAuthorityKey" "value" .Values.api.remediationAuthorityKey "default" "curie-dev-remediation-authority" "hex" false "existingData" $existingSecretData)) | toString) -}}
{{/* Compare what the managed Secret will actually hold, not only the values. */}}
{{- if eq (index $content "remediationAuthorityKey") (index $content "apiKey") -}}
{{- fail "api.remediationAuthorityKey and api.apiKey resolve to the same retained credential. Set api.remediationAuthorityKey to an independent value (ADR 0231); neither value is printed because both are live credentials." -}}
{{- end -}}
{{- $_ := set $content "internalWorkerToken" ((include "curie.managedSecret" (dict "root" . "key" "internalWorkerToken" "value" .Values.worker.internalWorkerToken "default" "curie-dev-worker-token" "hex" false "existingData" $existingSecretData)) | toString) -}}
{{- $_ := set $content "githubWebhookSecret" ((include "curie.managedSecret" (dict "root" . "key" "githubWebhookSecret" "value" .Values.api.githubWebhookSecret "default" "dev-webhook-secret" "hex" false "existingData" $existingSecretData)) | toString) -}}
{{- $_ := set $content "githubToken" ((.Values.api.githubToken | default "") | toString) -}}
{{- $_ := set $content "githubAppPrivateKey" ((.Values.api.githubAppPrivateKey | default "") | toString) -}}
{{- $_ := set $content "sealingPrivateKey" ((.Values.sealing.privateKey | default "") | toString) -}}
{{- $_ := set $content "sealingPreviousPrivateKey" ((.Values.sealing.previousPrivateKey | default "") | toString) -}}
{{- $_ := set $content "connectorCallerSigningKey" ((include "curie.connectorCallerSigningKey" .) | toString) -}}
{{- $_ := set $content "connectorCallerVerifyKey" ((include "curie.connectorCallerVerifyKey" .) | toString) -}}
{{- $_ := set $content "slackAppToken" (ternary "" (toString .Values.dispatcher.slack.appToken) (kindIs "invalid" .Values.dispatcher.slack.appToken)) -}}
{{- $_ := set $content "slackBotToken" (ternary "" (toString .Values.dispatcher.slack.botToken) (kindIs "invalid" .Values.dispatcher.slack.botToken)) -}}
{{- $_ := set $content "slackSigningSecret" (ternary "" (toString .Values.dispatcher.slack.signingSecret) (kindIs "invalid" .Values.dispatcher.slack.signingSecret)) -}}
{{- if not .Values.mailAdapter.agentmail.apiKeyExistingSecret -}}
{{- $_ := set $content "mailAgentmailApiKey" (ternary "" (toString .Values.mailAdapter.agentmail.apiKey) (kindIs "invalid" .Values.mailAdapter.agentmail.apiKey)) -}}
{{- end -}}
{{- if not .Values.mailAdapter.channelTokenExistingSecret -}}
{{- $_ := set $content "mailChannelToken" (ternary "" (toString .Values.mailAdapter.channelToken) (kindIs "invalid" .Values.mailAdapter.channelToken)) -}}
{{- end -}}
{{- if not .Values.mailAdapter.egressSecretExistingSecret -}}
{{- $_ := set $content "mailEgressSecret" (ternary "" (toString .Values.mailAdapter.egressSecret) (kindIs "invalid" .Values.mailAdapter.egressSecret)) -}}
{{- end -}}
{{- $_ := set . "_curieSecretContent" $content -}}
{{- end -}}
{{- end -}}

{{/* Hash only chart-owned keys this pod consumes (dict: root, keys, refs, env). Never look up foreign Secret contents. */}}
{{- define "curie.secretChecksum" -}}
{{- include "curie.secretContent.init" .root -}}
{{- $secretName := include "curie.secretName" .root -}}
{{- $content := get .root "_curieSecretContent" -}}
{{- $refs := .refs | default list -}}
{{- range (.keys | default list) -}}
{{- $refs = append $refs (dict "name" $secretName "key" .) -}}
{{- end -}}
{{- $env := fromYaml (printf "env:\n%s" ((.env | default "") | indent 2)) -}}
{{- range ($env.env | default list) -}}
{{- $ref := get (.valueFrom | default dict) "secretKeyRef" | default dict -}}
{{- if $ref -}}{{- $refs = append $refs $ref -}}{{- end -}}
{{- end -}}
{{- $subset := dict -}}
{{- range $refs -}}
{{- if eq .name $secretName -}}
{{- $_ := set $subset .key (index $content .key) -}}
{{- end -}}
{{- end -}}
{{- if $subset -}}{{- $subset | toJson | sha256sum -}}{{- end -}}
{{- end -}}

{{/* ---- Shared first-party-app environment fragments ---- */}}

{{/* Postgres env for app services: DATABASE_URL is composed with $(POSTGRES_PASSWORD) so the password never lands in the manifest. */}}
{{- define "curie.env.postgres" -}}
- name: POSTGRES_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "curie.storeSecretName" (dict "root" . "store" .Values.postgres) }}
      key: postgresPassword
- name: DATABASE_URL
  value: postgresql+asyncpg://{{ .Values.postgres.auth.username }}:$(POSTGRES_PASSWORD)@{{ include "curie.postgres.host" . }}:{{ .Values.postgres.port }}/{{ .Values.postgres.auth.database }}{{ include "curie.postgres.dsnParams" (dict "root" . "driver" "asyncpg") }}
- name: DB_SCHEMA
  value: curie
{{- end -}}

{{/* Valkey env for app services. TLS is always rendered, both values, so --reuse-values cannot leave a stale "true" (#2315). */}}
{{- define "curie.env.valkey" -}}
- name: VALKEY_HOST
  value: {{ include "curie.valkey.host" . | quote }}
- name: VALKEY_PORT
  value: {{ .Values.valkey.port | quote }}
- name: VALKEY_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "curie.storeSecretName" (dict "root" . "store" .Values.valkey) }}
      key: valkeyPassword
- name: VALKEY_TLS
  value: {{ include "curie.valkey.tls" . | quote }}
{{- end -}}

{{/* Platform API env for first-party services; URL and key are separate helpers. New API callers must include the URL helper, not derive it inline. BYO override is .Values.dispatcher.apiBaseUrl; no `required` for api.deploy=false, which CrashLoopBackOffs by design. nindent 12. */}}
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

{{/* Literal extraEnv value for a name, sentinel __valueFrom__ when it has no string value, empty when absent. A present empty value is __empty__: worker.yaml treats presence as an override, so Rail 1 must not fall back to the in-chart URL. */}}
{{- define "curie.extraEnv.lookup" -}}
{{- $found := "" -}}
{{- range (.env | default list) -}}
{{- if eq .name $.name -}}
{{- if and (hasKey . "value") (kindIs "string" .value) -}}
{{- if eq (trim .value) "" -}}
{{- $found = "__empty__" -}}
{{- else -}}
{{- $found = .value -}}
{{- end -}}
{{- else if hasKey . "value" -}}
{{- $found = printf "%v" .value -}}
{{- else -}}
{{- $found = "__valueFrom__" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $found -}}
{{- end -}}

{{/* Hostname of a URL, port stripped. Bracketed IPv6 stays intact; a scheme-less host:port is taken raw so curie-api:8000 still matches. */}}
{{- define "curie.endpoint.host" -}}
{{- $raw := trim . -}}
{{- if $raw -}}
{{- $parsed := urlParse $raw -}}
{{- $host := $parsed.host | default "" -}}
{{- if not $host -}}
{{- $host = $raw -}}
{{- end -}}
{{- if contains "[" $host -}}
{{- $host -}}
{{- else -}}
{{- regexReplaceAll ":[0-9]+$" $host "" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* TCP port a URL dials: the URL port, else https 443, http 80. Empty scheme and no port emit empty; do not invent 4318. Does not fail. */}}
{{- define "curie.endpoint.dialPort" -}}
{{- $raw := trim . -}}
{{- if $raw -}}
{{- $parsed := urlParse $raw -}}
{{- $hostPort := $parsed.host | default "" -}}
{{- $scheme := lower ($parsed.scheme | default "") -}}
{{/* Unlike curie.endpoint.host, a bracketed IPv6 host is not skipped here: the port must be found. */}}
{{- $urlPort := trimPrefix ":" (regexFind ":[0-9]+$" $hostPort) -}}
{{- if $urlPort -}}
{{- $urlPort -}}
{{- else if eq $scheme "https" -}}
443
{{- else if eq $scheme "http" -}}
80
{{- end -}}
{{- end -}}
{{- end -}}

{{/* True when host is this release's Service DNS for component (api or
     otel-collector), including the usual cluster.local FQDNs. */}}
{{- define "curie.host.inChartService" -}}
{{- $fullname := include "curie.fullname" .root -}}
{{- $ns := .root.Release.Namespace -}}
{{- $svc := printf "%s-%s" $fullname .component -}}
{{- $h := lower (trim .host) -}}
{{- if or (eq $h $svc) (eq $h (printf "%s.%s" $svc $ns)) (eq $h (printf "%s.%s.svc" $svc $ns)) (eq $h (printf "%s.%s.svc.cluster.local" $svc $ns)) -}}
true
{{- end -}}
{{- end -}}

{{/* API URL a spawned runner dials (#2367): extraEnv CURIE_RUNNER_API_URL, then CURIE_API_URL, then dispatcher.apiBaseUrl or the in-chart Service. */}}
{{- define "curie.runner.effectiveApiUrl" -}}
{{- $runner := include "curie.extraEnv.lookup" (dict "env" .Values.worker.extraEnv "name" "CURIE_RUNNER_API_URL") | trim -}}
{{- $api := include "curie.extraEnv.lookup" (dict "env" .Values.worker.extraEnv "name" "CURIE_API_URL") | trim -}}
{{- if $runner -}}
{{- $runner -}}
{{- else if $api -}}
{{- $api -}}
{{- else -}}
{{- include "curie.api.url" (dict "root" . "baseUrl" .Values.dispatcher.apiBaseUrl) -}}
{{- end -}}
{{- end -}}

{{- define "curie.runner.effectiveApiSource" -}}
{{- $runner := include "curie.extraEnv.lookup" (dict "env" .Values.worker.extraEnv "name" "CURIE_RUNNER_API_URL") | trim -}}
{{- $api := include "curie.extraEnv.lookup" (dict "env" .Values.worker.extraEnv "name" "CURIE_API_URL") | trim -}}
{{- if $runner -}}
worker.extraEnv CURIE_RUNNER_API_URL
{{- else if $api -}}
worker.extraEnv CURIE_API_URL
{{- else if trim (printf "%v" (.Values.dispatcher.apiBaseUrl | default "")) -}}
dispatcher.apiBaseUrl
{{- else -}}
the in-chart API Service
{{- end -}}
{{- end -}}

{{- define "curie.runner.apiIsExternal" -}}
{{- $url := include "curie.runner.effectiveApiUrl" . | trim -}}
{{- if or (eq $url "__valueFrom__") (eq $url "__empty__") -}}
true
{{- else if empty $url -}}
{{- else if include "curie.host.inChartService" (dict "root" . "host" (include "curie.endpoint.host" $url) "component" "api") -}}
{{- else -}}
true
{{- end -}}
{{- end -}}

{{/* True when the operator overrode the runner-facing API URL, so api.deploy false plus dispatcher.apiBaseUrl still requires api.egress (#2317). */}}
{{- define "curie.runner.apiOverrideSet" -}}
{{- $base := trim (printf "%v" (.Values.dispatcher.apiBaseUrl | default "")) -}}
{{- $runner := include "curie.extraEnv.lookup" (dict "env" .Values.worker.extraEnv "name" "CURIE_RUNNER_API_URL") | trim -}}
{{- $api := include "curie.extraEnv.lookup" (dict "env" .Values.worker.extraEnv "name" "CURIE_API_URL") | trim -}}
{{- if or $base $runner $api -}}
true
{{- end -}}
{{- end -}}

{{/* OTLP URL the runner sandbox dials (#2367): signal-specific extraEnv, then OTEL_EXPORTER_OTLP_ENDPOINT, then curie.otel.endpoint. */}}
{{- define "curie.runner.effectiveOtlpEndpoint" -}}
{{- $traces := include "curie.extraEnv.lookup" (dict "env" .Values.agentSandbox.runner.extraEnv "name" "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") | trim -}}
{{- $metrics := include "curie.extraEnv.lookup" (dict "env" .Values.agentSandbox.runner.extraEnv "name" "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") | trim -}}
{{- $logs := include "curie.extraEnv.lookup" (dict "env" .Values.agentSandbox.runner.extraEnv "name" "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT") | trim -}}
{{- $generic := include "curie.extraEnv.lookup" (dict "env" .Values.agentSandbox.runner.extraEnv "name" "OTEL_EXPORTER_OTLP_ENDPOINT") | trim -}}
{{- if $traces -}}
{{- $traces -}}
{{- else if $metrics -}}
{{- $metrics -}}
{{- else if $logs -}}
{{- $logs -}}
{{- else if $generic -}}
{{- $generic -}}
{{- else -}}
{{- include "curie.otel.endpoint" . | trim -}}
{{- end -}}
{{- end -}}

{{- define "curie.runner.effectiveOtlpSource" -}}
{{- $traces := include "curie.extraEnv.lookup" (dict "env" .Values.agentSandbox.runner.extraEnv "name" "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") | trim -}}
{{- $metrics := include "curie.extraEnv.lookup" (dict "env" .Values.agentSandbox.runner.extraEnv "name" "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") | trim -}}
{{- $logs := include "curie.extraEnv.lookup" (dict "env" .Values.agentSandbox.runner.extraEnv "name" "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT") | trim -}}
{{- $generic := include "curie.extraEnv.lookup" (dict "env" .Values.agentSandbox.runner.extraEnv "name" "OTEL_EXPORTER_OTLP_ENDPOINT") | trim -}}
{{- if $traces -}}
agentSandbox.runner.extraEnv OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
{{- else if $metrics -}}
agentSandbox.runner.extraEnv OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
{{- else if $logs -}}
agentSandbox.runner.extraEnv OTEL_EXPORTER_OTLP_LOGS_ENDPOINT
{{- else if $generic -}}
agentSandbox.runner.extraEnv OTEL_EXPORTER_OTLP_ENDPOINT
{{- else -}}
curie.otel.endpoint
{{- end -}}
{{- end -}}

{{- define "curie.runner.otlpIsExternal" -}}
{{- $url := include "curie.runner.effectiveOtlpEndpoint" . | trim -}}
{{- if or (eq $url "__valueFrom__") (eq $url "__empty__") -}}
true
{{- else if empty $url -}}
{{- else if include "curie.host.inChartService" (dict "root" . "host" (include "curie.endpoint.host" $url) "component" "otel-collector") -}}
{{- else -}}
true
{{- end -}}
{{- end -}}

{{/* OTLP URL the mail adapter dials (#2361): exactly curie.otel.endpoint, since the adapter has no extraEnv key. */}}
{{- define "curie.mailAdapter.effectiveOtlpEndpoint" -}}
{{- include "curie.otel.endpoint" . | trim -}}
{{- end -}}

{{/* True when the adapter's endpoint is not covered by the in-chart collector peer (#2361); keyed off deploy and host. */}}
{{- define "curie.mailAdapter.otlpIsExternal" -}}
{{- $url := include "curie.mailAdapter.effectiveOtlpEndpoint" . | trim -}}
{{- if empty $url -}}
{{- else if and .Values.otelCollector.deploy (include "curie.host.inChartService" (dict "root" . "host" (include "curie.endpoint.host" $url) "component" "otel-collector")) -}}
{{- else -}}
true
{{- end -}}
{{- end -}}

{{/* TCP port for the adapter's external-OTLP ipBlock rule (#2361): explicit port, else the URL's port, else the scheme default; a disagreement is refused. */}}
{{- define "curie.mailAdapter.otelEgressPort" -}}
{{- $endpoint := include "curie.mailAdapter.effectiveOtlpEndpoint" . | trim -}}
{{- $parsed := urlParse $endpoint -}}
{{- $hostPort := $parsed.host | default "" -}}
{{- $scheme := lower ($parsed.scheme | default "") -}}
{{/* Unlike curie.endpoint.host, a bracketed IPv6 host is not skipped here: the port must be found. */}}
{{- $urlPort := trimPrefix ":" (regexFind ":[0-9]+$" $hostPort) -}}
{{/* The port the SDK dials, written or not (portless https:// is 443). The explicit key is compared against this, not a ":port" substring. */}}
{{- $dialPort := $urlPort -}}
{{- if not $dialPort -}}
{{- if eq $scheme "https" -}}
{{- $dialPort = "443" -}}
{{- else if eq $scheme "http" -}}
{{- $dialPort = "80" -}}
{{- end -}}
{{- end -}}
{{- $explicit := trim (printf "%v" (.Values.mailAdapter.otelEgress.port | default "")) -}}
{{- $port := "" -}}
{{- if $explicit -}}
{{- if and $dialPort (regexMatch "^[0-9]+$" $explicit) (ne (int $explicit) (int $dialPort)) -}}
{{- fail (printf "mailAdapter.otelEgress.port is %s but the OTLP endpoint %s is dialed on port %s (the port its URL names, or the scheme default when the URL names none). Set one of them, or make them agree: opening %s while the exporter connects to %s renders a peer that looks configured and still drops every export." $explicit $endpoint $dialPort $explicit $dialPort) -}}
{{- end -}}
{{- $port = $explicit -}}
{{- else -}}
{{- $port = $dialPort -}}
{{- end -}}
{{- if or (not (regexMatch "^[0-9]+$" $port)) (lt (int $port) 1) (gt (int $port) 65535) -}}
{{- fail (printf "mailAdapter.otelEgress.port must be an integer from 1 through 65535, or empty to derive it from the resolved OTLP endpoint %s" $endpoint) -}}
{{- end -}}
{{- $port -}}
{{- end -}}

{{/* Coalesce worker egress credentials and the mail adapter's paired credential; chart Secret and worker checksum use this same JSON. mailAdapter.egressSecret is the source; a disagreeing worker entry fails. With egressSecretExistingSecret the worker map is the source: never compare the plain value. */}}
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
{{/* Connector caller key pair in the release Secret (ADR-0168 decision 7): operator halves, or with security.allowDevDefaults exactly "true" and neither set, the published dev pair (#195). Without a pair the API refuses hosted connectors (#3552). */}}
{{- define "curie.connectorCallerDevSigningKey" -}}Y3VyaWUtZGV2LWNvbm5lY3Rvci1jYWxsZXItc2VlZCE={{- end -}}

{{- define "curie.connectorCallerDevVerifyKey" -}}tkmNbO5SSLE0IM84sH4uJ94DxtriNZ/APXha3FiyP6c={{- end -}}

{{/* Refuse leaving dev mode with no replacement pair while the Secret holds the published one (#3552). */}}
{{- define "curie.connectorCallerRefuseKeylessDevExit" -}}
{{- $caller := .root.Values.connectorCaller | default dict -}}
{{- $live := "" -}}
{{- if hasKey .existingData "connectorCallerVerifyKey" -}}
{{- $live = index .existingData "connectorCallerVerifyKey" | b64dec -}}
{{- end -}}
{{- if and (eq $live (include "curie.connectorCallerDevVerifyKey" .root)) (eq (include "curie.connectorCallerVerifyKey" .root) "") (eq ((get $caller "existingSecret") | default "") "") -}}
{{- fail "connectorCaller: leaving dev defaults would remove the published dev caller pair with no replacement, and hosted connector proxies already running would keep trusting it. Set connectorCaller.signingKey and connectorCaller.verifyKey (or connectorCaller.existingSecret), or run `curie cluster up`, which generates a pair, then redeploy each agent that hosts a connector so its proxy moves to the new key." -}}
{{- end -}}
{{- end -}}

{{- define "curie.connectorCallerSigningKey" -}}
{{- $caller := .Values.connectorCaller | default dict -}}
{{- $signing := (get $caller "signingKey") | default "" -}}
{{- $verify := (get $caller "verifyKey") | default "" -}}
{{- if and (eq (toString .Values.security.allowDevDefaults) "true") (eq (toString $signing) "") (eq (toString $verify) "") -}}
{{- $signing = include "curie.connectorCallerDevSigningKey" . -}}
{{- end -}}
{{- $signing -}}
{{- end -}}

{{- define "curie.connectorCallerVerifyKey" -}}
{{- $caller := .Values.connectorCaller | default dict -}}
{{- $signing := (get $caller "signingKey") | default "" -}}
{{- $verify := (get $caller "verifyKey") | default "" -}}
{{- if and (eq (toString .Values.security.allowDevDefaults) "true") (eq (toString $signing) "") (eq (toString $verify) "") -}}
{{- $verify = include "curie.connectorCallerDevVerifyKey" . -}}
{{- end -}}
{{- $verify -}}
{{- end -}}

{{/* What rolls the api and worker when the caller pair changes: public material and references only. */}}
{{- define "curie.connectorCallerChecksumSource" -}}
{{- $caller := .Values.connectorCaller | default dict -}}
{{- printf "%s|%s|%s|%s|%s" (include "curie.connectorCallerVerifyKey" .) ((get $caller "previousVerifyKey") | default "") ((get $caller "existingSecret") | default "") ((get $caller "signingKeyKey") | default "") ((get $caller "verifyKeyKey") | default "") -}}
{{- end -}}

{{- define "curie.adapterCredentialsChecksumSource" -}}
{{- $creds := include "curie.adapterCredentials" . -}}
{{- if not (empty .Values.worker.adapterCredentialsExistingSecret) -}}
{{- printf "%s|%s|%s" $creds .Values.worker.adapterCredentialsExistingSecret .Values.worker.adapterCredentialsExistingSecretKey -}}
{{- else -}}
{{- $creds -}}
{{- end -}}
{{- end -}}

{{/* Heartbeat exec probes for worker and dispatcher: CURIE_HEARTBEAT_FILE fresher than 30s; no params. Include with nindent 10. */}}
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
{{- /* These three honour their store's own existingSecret like curie.env.postgres (#2327). */}}
- name: POSTGRES_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "curie.storeSecretName" (dict "root" . "store" .Values.postgres) }}
      key: postgresPassword
- name: DATABASE_URL
  value: postgresql://{{ .Values.postgres.auth.username }}:$(POSTGRES_PASSWORD)@{{ include "curie.postgres.host" . }}:{{ .Values.postgres.port }}/{{ .Values.postgres.auth.database }}{{ include "curie.postgres.dsnParams" (dict "root" . "driver" "prisma") }}
- name: SALT
  valueFrom:
    secretKeyRef:
      name: {{ include "curie.storeSecretName" (dict "root" . "store" .Values.langfuse) }}
      key: langfuseSalt
- name: ENCRYPTION_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "curie.storeSecretName" (dict "root" . "store" .Values.langfuse) }}
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
{{- /* TLS migration switch on the native port; only on the https path so cleartext installs stay byte-identical (#2314). The URL stays bare. */}}
- name: CLICKHOUSE_MIGRATION_SSL
  value: "true"
{{- end }}
- name: CLICKHOUSE_USER
  value: {{ .Values.clickhouse.auth.username | quote }}
{{- /* Both honour the store's own existingSecret like curie.env.valkey (#2052). */}}
- name: CLICKHOUSE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "curie.storeSecretName" (dict "root" . "store" .Values.clickhouse) }}
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
      name: {{ include "curie.storeSecretName" (dict "root" . "store" .Values.valkey) }}
      key: valkeyPassword
{{- /* Same helper as curie.env.valkey (#2315). REDIS_TLS_CA, _CERT, _KEY are deliberately NOT rendered. */}}
- name: REDIS_TLS_ENABLED
  value: {{ include "curie.valkey.tls" . | quote }}
- name: LANGFUSE_S3_EVENT_UPLOAD_BUCKET
  value: {{ .Values.rustfs.bucket | quote }}
{{- /* Both upload regions come from rustfs.region, never the literal `auto`: real S3 rejects it and drops every trace. See #2214. */}}
- name: LANGFUSE_S3_EVENT_UPLOAD_REGION
  value: {{ .Values.rustfs.region | quote }}
{{- /* Credential env gated like api/worker/bundle-fetch (#2211): with empty rustfs.auth.accessKey omit these, never emit empty. */}}
{{- if include "curie.rustfs.staticCredentials" . }}
- name: LANGFUSE_S3_EVENT_UPLOAD_ACCESS_KEY_ID
  value: {{ .Values.rustfs.auth.accessKey | quote }}
- name: LANGFUSE_S3_EVENT_UPLOAD_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "curie.storeSecretName" (dict "root" . "store" .Values.rustfs) }}
      key: rustfsSecretKey
{{- end }}
{{- /* Both endpoints go through curie.rustfs.endpoint, never a literal http://, or a BYO store on 443 breaks Langfuse ingestion. */}}
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
      name: {{ include "curie.storeSecretName" (dict "root" . "store" .Values.rustfs) }}
      key: rustfsSecretKey
{{- end }}
- name: LANGFUSE_S3_MEDIA_UPLOAD_ENDPOINT
  value: {{ include "curie.rustfs.endpoint" . }}
- name: LANGFUSE_S3_MEDIA_UPLOAD_FORCE_PATH_STYLE
  value: "true"
- name: LANGFUSE_S3_MEDIA_UPLOAD_PREFIX
  value: media/
{{- end -}}

{{/* gVisor tri-state (security.gvisor.mode: auto, require, off). requiredRuntimeClassName is stamped by a real install: empty for off and fake-model auto, set for require and real-model auto (not fakeModel, or inference.deploy). runtimeClassName: required if non-empty; else off is empty, installRuntimeClass gives className, auto gives it only if lookup finds the class. */}}
{{/* gVisor's sentry reports LinuxVersion via uname -v, independent of pod names (google/gvisor pkg/sentry/kernel/version/version.go). */}}
{{- define "curie.gvisor.sentryKernelVersion" -}}
{{- "#1 SMP Sun Jan 10 15:06:54 PST 2016" -}}
{{- end -}}

{{- define "curie.gvisor.className" -}}
{{- $g := .Values.security.gvisor -}}
{{- if eq ($g.mode | default "auto") "off" -}}
{{- else -}}
{{- $g.runtimeClassName | default "gvisor" -}}
{{- end -}}
{{- end -}}

{{- define "curie.gvisor.requiredRuntimeClassName" -}}
{{- $g := .Values.security.gvisor -}}
{{- $mode := $g.mode | default "auto" -}}
{{- $realModel := or (not .Values.agentSandbox.runner.fakeModel) .Values.inference.deploy -}}
{{- if eq $mode "off" -}}
{{- else if or (eq $mode "require") (and (eq $mode "auto") $realModel) -}}
{{- $g.runtimeClassName | default "gvisor" -}}
{{- end -}}
{{- end -}}

{{- define "curie.gvisor.runtimeClassName" -}}
{{- $required := include "curie.gvisor.requiredRuntimeClassName" . | trim -}}
{{- if $required -}}
{{- $required -}}
{{- else -}}
{{- $g := .Values.security.gvisor -}}
{{- $mode := $g.mode | default "auto" -}}
{{- $name := $g.runtimeClassName | default "gvisor" -}}
{{- if eq $mode "off" -}}
{{- else if $g.installRuntimeClass -}}
{{- $name -}}
{{- else -}}
{{- if lookup "node.k8s.io/v1" "RuntimeClass" "" $name -}}
{{- $name -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* gVisor enforcement gate: preflightRequired is "true" when the blocking preflight Job must render, in require and in auto with a real model (not fakeModel, or inference.deploy), so a missing runsc class fails closed. */}}
{{- define "curie.gvisor.preflightRequired" -}}
{{- $mode := .Values.security.gvisor.mode | default "auto" -}}
{{- $realModel := or (not .Values.agentSandbox.runner.fakeModel) .Values.inference.deploy -}}
{{- if and .Values.agentSandbox.deploy .Values.security.gvisorPreflight.enabled -}}
{{- if or (eq $mode "require") (and (eq $mode "auto") $realModel) -}}
true
{{- end -}}
{{- end -}}
{{- end -}}

{{/* First-party image ref (dict: repository, tag, digest, defaultTag). A digest wins; else repository:tag, falling back to defaultTag for a deterministic ref. */}}
{{- define "curie.image" -}}
{{- $repo := required "image.repository is required" .repository -}}
{{- if .digest -}}
{{- printf "%s@%s" $repo .digest -}}
{{- else -}}
{{- printf "%s:%s" $repo (.tag | default .defaultTag | default "latest") -}}
{{- end -}}
{{- end -}}

{{/* BYO existingSecret escape for a direct-passthrough credential (#1759, ADR-0092), for the eleven `<field>ExistingSecret` and `<field>ExistingSecretKey` pairs: set, it wins and a missing key fails loudly. One helper so all consumers of a key agree. Dict: root, existingSecret, existingSecretKey, defaultKey. nindent 18. */}}
{{- define "curie.secretRef" -}}
{{- if .existingSecret -}}
name: {{ .existingSecret | quote }}
key: {{ .existingSecretKey | quote }}
{{- else -}}
name: {{ include "curie.secretName" .root }}
key: {{ .defaultKey }}
{{- end -}}
{{- end -}}

{{/* Dispatcher gating: deploys only when identity `default` has both Slack tokens (#1759), else it would crash-loop. */}}
{{- define "curie.dispatcher.enabled" -}}
{{- $appTokenSet := or .Values.dispatcher.slack.appToken .Values.dispatcher.slack.appTokenExistingSecret -}}
{{- $botTokenSet := or .Values.dispatcher.slack.botToken .Values.dispatcher.slack.botTokenExistingSecret -}}
{{- $listed := .Values.dispatcher.slack.identities -}}
{{- if and .Values.dispatcher.deploy (or (and $appTokenSet $botTokenSet) $listed) -}}
true
{{- end -}}
{{- end -}}

{{/* Sandbox container hardening (Rail 3) shared by runner and helper containers (#493); nindent 10. */}}
{{- define "curie.sandboxHardening.securityContext" -}}
securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  runAsNonRoot: true
  capabilities:
    drop: [ALL]
{{- end -}}

{{/* Container securityContext wrapper shared by api, worker, dispatcher, ui (#493); nindent 10. */}}
{{- define "curie.containerSecurityContext" -}}
securityContext:
{{- toYaml . | nindent 2 }}
{{- end -}}

{{/* ADR-0131 drain budget (worker). Since #3071 effective grace is max(terminationGracePeriodSeconds, deliveryBudgetSeconds + deliveryShutdownReserveSeconds): the configured value is a floor. */}}
{{- define "curie.worker.terminationGrace" -}}
{{- max (int64 .Values.worker.terminationGracePeriodSeconds) (add (int64 .Values.worker.deliveryBudgetSeconds) (int64 .Values.worker.deliveryShutdownReserveSeconds)) -}}
{{- end -}}

{{/* Drop extraEnv entries colliding with first-class worker timeout and budget env; first-class wins (#2097). */}}
{{- define "curie.worker.extraEnv" -}}
{{- $reserved := dict
  "CURIE_CLAIM_TIMEOUT_SECONDS" true
  "CURIE_ROUTE_TTL_SECONDS" true
  "CURIE_SUSPENDED_ROUTE_TTL_SECONDS" true
  "CURIE_STREAM_RETENTION_MIN_AGE_S" true
  "CURIE_DELIVERY_BUDGET_S" true
  "CURIE_RUNNER_TOTAL_TIMEOUT_S" true
  "CURIE_DELIVERY_LEASE_TTL_S" true
  "CURIE_DELIVERY_LEASE_HEARTBEAT_S" true
  "CURIE_DELIVERY_SHUTDOWN_RESERVE_S" true
  "CURIE_TERMINATION_GRACE_PERIOD_S" true
  "CURIE_WORK_ITEM_MAX_TURNS" true
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

{{/* Resume reconciler grace (API): default derives from the worker delivery budget plus shutdown reserve; a lower explicit value is refused. */}}
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

{{/* Runner ceiling vs delivery budget (worker): runnerTotalTimeoutSeconds must fit inside deliveryBudgetSeconds. */}}
{{- define "curie.worker.validateRunnerBudget" -}}
{{- $runnerCeiling := float64 .Values.worker.runnerTotalTimeoutSeconds -}}
{{- $deliveryBudget := float64 .Values.worker.deliveryBudgetSeconds -}}
{{- if gt $runnerCeiling $deliveryBudget -}}
{{- fail (printf "worker.runnerTotalTimeoutSeconds (%g) must be <= worker.deliveryBudgetSeconds (%g). Fix: lower worker.runnerTotalTimeoutSeconds to %g or less, or raise worker.deliveryBudgetSeconds to at least %g." $runnerCeiling $deliveryBudget $deliveryBudget $runnerCeiling) -}}
{{- end -}}
{{- end -}}

{{/* Upgrade drain gate arithmetic (#2010): both clocks are derived with values as floors; only a self-contradictory pair is refused. The wait covers deliveryBudgetSeconds (ADR-0131); quiesce TTL is capped at it (#3127). */}}
{{- define "curie.worker.upgradeDrain.timeout" -}}
{{- max (int64 .Values.worker.upgradeDrain.timeoutSeconds) (add (int64 .Values.worker.deliveryBudgetSeconds) (int64 .Values.worker.deliveryShutdownReserveSeconds)) -}}
{{- end -}}

{{/* The drain Job may run for 120 seconds beyond its effective wait. */}}
{{- define "curie.worker.upgradeDrain.jobDeadline" -}}
{{- add (int64 (include "curie.worker.upgradeDrain.timeout" .)) 120 -}}
{{- end -}}

{{/* Upgrade timeout covers the drain Job deadline, one worker grace, and 60 seconds for scheduling and Helm. */}}
{{- define "curie.worker.minimumHelmTimeoutSeconds" -}}
{{- add (int64 (include "curie.worker.upgradeDrain.jobDeadline" .)) (int64 (include "curie.worker.terminationGrace" .)) 60 -}}
{{- end -}}

{{/* The roll hold: min(quiesceTtlSeconds, effective wait). */}}
{{- define "curie.worker.upgradeDrain.quiesceTtl" -}}
{{- min (int64 .Values.worker.upgradeDrain.quiesceTtlSeconds) (int64 (include "curie.worker.upgradeDrain.timeout" .)) -}}
{{- end -}}

{{/* No ordering rule between quiesceTtlSeconds and timeoutSeconds remains (#3127). Kept as a hook for including templates. */}}
{{- define "curie.worker.validateUpgradeDrain" -}}
{{- end -}}

{{/* Langfuse Postgres startup gate (#1853, #2330), shared by both Langfuse Deployments, before the ClickHouse gate. `langfuse.postgresReadiness` is canonical; `langfuse.web.postgresReadiness` is a deprecated alias honoured for both. Bounded pg_isready on the Postgres image, so securityContext is the caller's floored to non-root (runAsUser 1001, #351). Dict: root, containerSecurityContext. */}}
{{- define "curie.langfuse.postgresReadinessValues" -}}
{{/* Effective Postgres readiness config: `langfuse.postgresReadiness` with `langfuse.web.postgresReadiness` merged over it. Never fail here: --reuse-values replays stored blobs. `keyPath` only names the path in curie.langfuse.postgresGate's fail. */}}
{{- $root := . -}}
{{- $effective := deepCopy $root.Values.langfuse.postgresReadiness -}}
{{- $keyPath := "langfuse.postgresReadiness" -}}
{{- $legacy := get ($root.Values.langfuse.web | default dict) "postgresReadiness" -}}
{{- if and $legacy (kindIs "map" $legacy) (gt (len $legacy) 0) -}}
{{- $effective = mergeOverwrite $effective (deepCopy $legacy) -}}
{{- $keyPath = "langfuse.web.postgresReadiness" -}}
{{- end -}}
{{- $_ := set $effective "keyPath" $keyPath -}}
{{- toYaml $effective -}}
{{- end -}}

{{- define "curie.langfuse.postgresGate" -}}
{{- $root := .root -}}
{{- $postgresReadiness := fromYaml (include "curie.langfuse.postgresReadinessValues" $root) -}}
{{- $readinessSecurityContext := deepCopy (.containerSecurityContext | default dict) -}}
{{- $_ := set $readinessSecurityContext "runAsNonRoot" true -}}
{{- if not (hasKey $readinessSecurityContext "runAsUser") -}}
{{- $_ := set $readinessSecurityContext "runAsUser" 1001 -}}
{{- else if eq (int64 (get $readinessSecurityContext "runAsUser")) 0 -}}
{{- $_ := set $readinessSecurityContext "runAsUser" 1001 -}}
{{- end -}}
{{- range $field := list "attempts" "intervalSeconds" "probeTimeoutSeconds" -}}
{{- $value := index $postgresReadiness $field -}}
{{- $integerKind := or (kindIs "int" $value) (kindIs "int64" $value) (kindIs "float64" $value) -}}
{{- if or (not $integerKind) (not (regexMatch "^[0-9]+$" (printf "%v" $value))) (lt (int64 $value) 1) -}}
{{- fail (printf "%s.%s must be an integer >= 1" $postgresReadiness.keyPath $field) -}}
{{- end -}}
{{- end -}}
- name: wait-for-postgres
  image: {{ $postgresReadiness.image | default $root.Values.postgres.image | quote }}
  imagePullPolicy: {{ $root.Values.global.imagePullPolicy }}
  securityContext:
    {{- toYaml $readinessSecurityContext | nindent 4 }}
  command:
    - sh
    - -ec
    - |
      echo "waiting for Postgres readiness"
      attempt=1
      last_exit_code=1
      while [ "$attempt" -le "$POSTGRES_READINESS_ATTEMPTS" ]; do
        if pg_isready \
          -h "$POSTGRES_HOST" \
          -p "$POSTGRES_PORT" \
          -U "$POSTGRES_USER" \
          -d "$POSTGRES_DATABASE" \
          -t "$POSTGRES_READINESS_PROBE_TIMEOUT_SECONDS" \
          >/dev/null 2>&1; then
          exit 0
        else
          last_exit_code=$?
        fi
        if [ "$attempt" -lt "$POSTGRES_READINESS_ATTEMPTS" ]; then
          sleep "$POSTGRES_READINESS_INTERVAL_SECONDS"
        fi
        attempt=$((attempt + 1))
      done
      echo "Postgres readiness exhausted for ${POSTGRES_HOST}:${POSTGRES_PORT} after ${POSTGRES_READINESS_ATTEMPTS} attempts: pg_isready=${last_exit_code} (0=accepting, 1=rejecting, 2=no-response, 3=no-attempt)" >&2
      exit 1
  env:
    - name: POSTGRES_HOST
      value: {{ include "curie.postgres.host" $root | quote }}
    - name: POSTGRES_PORT
      value: {{ $root.Values.postgres.port | quote }}
    - name: POSTGRES_USER
      value: {{ $root.Values.postgres.auth.username | quote }}
    - name: POSTGRES_DATABASE
      value: {{ $root.Values.postgres.auth.database | quote }}
    - name: POSTGRES_READINESS_ATTEMPTS
      value: {{ $postgresReadiness.attempts | quote }}
    - name: POSTGRES_READINESS_INTERVAL_SECONDS
      value: {{ $postgresReadiness.intervalSeconds | quote }}
    - name: POSTGRES_READINESS_PROBE_TIMEOUT_SECONDS
      value: {{ $postgresReadiness.probeTimeoutSeconds | quote }}
{{- end -}}

{{/* Langfuse ClickHouse startup gate (#2009): bounded init container polling ClickHouse /ping until 200. Dict: root, image, containerSecurityContext, resources. */}}
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
{{/* Runner-sandbox egress rules (dict: entries, root). Every rail 1 ipBlock goes through here so carve-outs live once: allows are additive, so a broad entry must not re-permit 169.254.169.254 or private ranges. Except list: per-entry except (empty refused by curie.objectStore.egressEntry), else curie.metadataExcept, plus curie.privateExcept ranges (#3842). */}}
{{/* Except list of one egress entry as JSON: its own `except`, else the metadata carve-out, plus private ranges its CIDR contains. Shared with the publication Job policy. */}}
{{- define "curie.egress.ipBlockExcept" -}}
{{- $excepts := list }}
{{- if .entry.except }}
{{- $excepts = .entry.except }}
{{- else }}
{{- $excepts = without (splitList "," (include "curie.metadataExcept" .entry.cidr | trim)) "" }}
{{- end }}
{{- range without (splitList "," (include "curie.privateExcept" (dict "cidr" .entry.cidr "root" .root) | trim)) "" }}
{{- if not (has . $excepts) }}
{{- $excepts = append $excepts . }}
{{- end }}
{{- end }}
{{- toJson $excepts }}
{{- end -}}
{{- define "curie.egress.ipBlockRules" -}}
{{- $root := .root }}
{{- range .entries }}
- to:
    - ipBlock:
        cidr: {{ .cidr }}
        {{- $excepts := include "curie.egress.ipBlockExcept" (dict "entry" . "root" $root) | fromJsonArray }}
        {{- if $excepts }}
        except:
          {{- toYaml $excepts | nindent 10 }}
        {{- end }}
  {{- with .ports }}
  ports:
    {{- toYaml . | nindent 4 }}
  {{- end }}
{{- end }}
{{- end -}}

{{/* Claim-env no-op notice for sandbox init containers (#2612): a SandboxClaim spec.env entry without containerName reaches only the runner. The no-op stays (warm pods must boot) but logs it. */}}
{{- define "curie.sandbox.claimEnvNoOpNotice" -}}
no {{ .key }} in this container's env: nothing to stage. This is expected for a warm or unbound pod. If you set {{ .key }} on a SandboxClaim and expected staging, note that a spec.env entry with no containerName reaches the RUNNER container only -- repeat it with containerName: {{ .container }}.
{{- end -}}

{{/* Agents with a per-agent runner SandboxTemplate and warm pool, as a JSON array: connectorSecrets, registryEgress (#3083), runnerImages (ADR-0173), runnerEnv, workspaceSizeLimits (#3523), agentResourceProfiles (#4889), factory.sandboxEgress, poolAgents. */}}
{{- define "curie.agentSandboxPoolAgents" -}}
{{- $extra := list -}}
{{- if kindIs "slice" .Values.agentSandbox.poolAgents -}}
{{- $extra = .Values.agentSandbox.poolAgents -}}
{{- end -}}
{{- $agents := concat (keys (.Values.agentSandbox.connectorSecrets | default dict)) (keys (.Values.agentSandbox.registryEgress | default dict)) (keys (.Values.agentSandbox.runnerImages | default dict)) (keys (include "curie.agentSandboxRunnerEnv" . | fromJson)) (keys (.Values.agentSandbox.workspaceSizeLimits | default dict)) (keys (get .Values.agentSandbox "agentResourceProfiles" | default dict)) (include "curie.factory.sandboxAgents" . | fromJsonArray) $extra -}}
{{- $agents | uniq | sortAlpha | toJson -}}
{{- end -}}

{{/* Action executor switch (ACTION-EXECUTOR-1): one "true" or "false" into API and worker so they cannot disagree (#3505, #3544). */}}
{{- define "curie.actionExecutorEnabled" -}}
{{- if (get (.Values.actionExecutor | default dict) "enabled") -}}true{{- else -}}false{{- end -}}
{{- end -}}

{{/* Executor connector reads without the reconcile loop (#4800, ACTION-EXECUTOR-12): "true" or "false". Requires the executor; refused with the reconciler on. */}}
{{- define "curie.actionExecutorConnectorReads" -}}
{{- if (get (.Values.actionExecutor | default dict) "connectorReadsWithoutReconciler") -}}
{{- if ne (include "curie.actionExecutorEnabled" .) "true" -}}
{{- fail "actionExecutor.connectorReadsWithoutReconciler=true requires actionExecutor.enabled=true: only the executor reads connector Deployments" -}}
{{- end -}}
{{- if .Values.worker.connectorReconciler.enabled -}}
{{- fail "actionExecutor.connectorReadsWithoutReconciler=true is for worker.connectorReconciler.enabled=false: with the reconciler on the executor already reads connector Deployments, so leave it unset" -}}
{{- end -}}
true
{{- else -}}false{{- end -}}
{{- end -}}

{{/* Installation-wide executor sandbox cap (AUTOMATED-REMEDIATION-12, amendment E9): one integer rendered into both API and worker. */}}
{{- define "curie.actionExecutorMaxConcurrentSandboxes" -}}
{{- $cap := get (.Values.actionExecutor | default dict) "maxConcurrentSandboxes" -}}
{{- if kindIs "invalid" $cap -}}2{{- else -}}{{ int $cap }}{{- end -}}
{{- end -}}

{{/* Automated remediation switch (AUTOMATED-REMEDIATION-1): "true" or "false" into API and worker. Requires the action executor. */}}
{{- define "curie.remediationEnabled" -}}
{{- if (get (.Values.remediation | default dict) "enabled") -}}
{{- if ne (include "curie.actionExecutorEnabled" .) "true" -}}
{{- fail "remediation.enabled=true requires actionExecutor.enabled=true (AUTOMATED-REMEDIATION-1): automated remediation executes through the connector action executor" -}}
{{- end -}}
true
{{- else -}}false{{- end -}}
{{- end -}}

{{/* Workspace recovery (ADR 0251): off by default; enabling requires a positive installation-wide byte cap. One helper for API and worker. */}}
{{- define "curie.workspaceRecoveryEnabled" -}}
{{- $recovery := dig "workspace" "recovery" dict (.Values.worker | default dict) -}}
{{- if get $recovery "enabled" -}}
{{- $_ := include "curie.workspaceRecoveryMaxBytes" . -}}
true
{{- else -}}false{{- end -}}
{{- end -}}

{{- define "curie.workspaceRecoveryMaxBytes" -}}
{{- $recovery := dig "workspace" "recovery" dict (.Values.worker | default dict) -}}
{{- $cap := get $recovery "maxBytes" -}}
{{- if or (kindIs "invalid" $cap) (kindIs "string" $cap) (not (or (kindIs "float64" $cap) (kindIs "int64" $cap) (kindIs "int" $cap))) -}}
{{- fail "worker.workspace.recovery.enabled=true requires worker.workspace.recovery.maxBytes to be a positive integer (ADR 0251): the installation-wide cap counts every recovery byte" -}}
{{- end -}}
{{- $number := float64 $cap -}}
{{- if or (le $number 0.0) (ne $number (floor $number)) -}}
{{- fail "worker.workspace.recovery.maxBytes must be a positive integer (ADR 0251): the installation-wide cap counts every recovery byte" -}}
{{- end -}}
{{- $cap | int64 -}}
{{- end -}}

{{/* Publication candidates (#5428). On when the value is true and workspaces are on.
     Workspaces off renders the switch off instead of failing: other dependent
     switches fail an explicit true, but a default of true would then refuse
     every install that turns workspaces off. */}}
{{- define "curie.publicationCandidatesEnabled" -}}
{{- $candidates := dig "workspace" "publicationCandidates" dict (.Values.worker | default dict) -}}
{{- $workspaces := dig "runner" "workspace" "enabled" false (.Values.agentSandbox | default dict) -}}
{{- if and (get $candidates "enabled") $workspaces -}}
{{- $_ := include "curie.publicationCandidateMaxBytes" . -}}
true
{{- else -}}false{{- end -}}
{{- end -}}

{{- define "curie.publicationCandidateMaxBytes" -}}
{{- $candidates := dig "workspace" "publicationCandidates" dict (.Values.worker | default dict) -}}
{{- $cap := get $candidates "maxBytes" -}}
{{- $bad := "worker.workspace.publicationCandidates.maxBytes must be a positive integer (#5428)" -}}
{{- if not (or (kindIs "float64" $cap) (kindIs "int64" $cap) (kindIs "int" $cap)) -}}{{- fail $bad -}}{{- end -}}
{{- if or (le (float64 $cap) 0.0) (ne (float64 $cap) (floor (float64 $cap))) -}}{{- fail $bad -}}{{- end -}}
{{- $cap | int64 -}}
{{- end -}}

{{/* Code host trust (ADR 0197): renders nothing when no ConfigMap is named. */}}
{{- define "curie.codeHostTrust.name" -}}
{{- dig "caBundle" "configMapRef" "name" "" (.Values.codeHostTrust | default dict) -}}
{{- end -}}
{{- define "curie.codeHostTrust.key" -}}
{{- dig "caBundle" "configMapRef" "key" "ca.crt" (.Values.codeHostTrust | default dict) -}}
{{- end -}}
{{- define "curie.codeHostTrust.dir" -}}/etc/curie/code-host-trust{{- end -}}
{{- define "curie.codeHostTrust.path" -}}{{ include "curie.codeHostTrust.dir" . }}/ca.crt{{- end -}}
{{- define "curie.codeHostTrust.volume" -}}
{{- if include "curie.codeHostTrust.name" . }}
- name: code-host-trust
  configMap:
    name: {{ include "curie.codeHostTrust.name" . | quote }}
    items:
      - key: {{ include "curie.codeHostTrust.key" . | quote }}
        path: ca.crt
{{- end }}
{{- end -}}
{{- define "curie.codeHostTrust.volumeMount" -}}
{{- if include "curie.codeHostTrust.name" . }}
- name: code-host-trust
  mountPath: {{ include "curie.codeHostTrust.dir" . }}
  readOnly: true
{{- end }}
{{- end -}}
{{/* git and HTTP clients in a sandbox read the one bundle. */}}
{{- define "curie.codeHostTrust.clientEnv" -}}
{{- if include "curie.codeHostTrust.name" . }}
{{- $path := include "curie.codeHostTrust.path" . }}
- name: CURIE_REPO_CA_BUNDLE
  value: {{ $path | quote }}
- name: GIT_SSL_CAINFO
  value: {{ $path | quote }}
- name: SSL_CERT_FILE
  value: {{ $path | quote }}
- name: REQUESTS_CA_BUNDLE
  value: {{ $path | quote }}
{{- end }}
{{- end -}}

{{/* The factory (#4397): factory.* is read through `get`, so a release
     without the block renders. */}}
{{- define "curie.factory.mount" -}}/etc/curie/forges{{- end -}}

{{/* The installation's model auth mode (ADR 0268), rendered as
     FACTORY_MODEL_AUTH_MODE into BOTH the api and the worker from this one
     helper so the two cannot drift. Absent or null means service. The schema
     enum refuses other values; this guard also covers renders that skip the
     schema. The api reads its settings case-insensitively, so an extraEnv
     entry whose name differs only in case would still override the chart
     value: refuse every case variant in api.extraEnv and worker.extraEnv
     (curie.extraEnv already refuses the exact name). Personal mode needs the
     Kubernetes substrate: the worker refuses to start
     (personal_mode_requires_kubernetes) unless CURIE_SANDBOX_SUBSTRATE,
     stripped and lower-cased, is kubernetes, and an empty value is a startup
     error. A worker.extraEnv entry for it must therefore be the literal
     kubernetes; a valueFrom entry resolves only at runtime and is refused. */}}
{{- define "curie.factory.modelAuthMode" -}}
{{- $mode := get (.Values.factory | default dict) "modelAuthMode" | default "service" | toString -}}
{{- if not (has $mode (list "service" "personal")) -}}
{{- fail (printf "factory.modelAuthMode must be service or personal, got %q." $mode) -}}
{{- end -}}
{{- range $workload := list "api" "worker" -}}
{{- range (get (get $.Values $workload | default dict) "extraEnv" | default list) -}}
{{- if and (eq (upper (toString .name)) "FACTORY_MODEL_AUTH_MODE") (ne (toString .name) "FACTORY_MODEL_AUTH_MODE") -}}
{{- fail (printf "%s.extraEnv sets %s, which the %s reads as FACTORY_MODEL_AUTH_MODE; remove it from extraEnv and configure factory.modelAuthMode instead." $workload (toString .name) $workload) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if eq $mode "personal" -}}
{{- range (.Values.worker.extraEnv | default list) -}}
{{- if eq (toString .name) "CURIE_SANDBOX_SUBSTRATE" -}}
{{- if or (hasKey . "valueFrom") (not (hasKey . "value")) -}}
{{- fail "factory.modelAuthMode=personal needs the Kubernetes sandbox substrate, but worker.extraEnv sets CURIE_SANDBOX_SUBSTRATE from valueFrom, which cannot be verified before the worker starts; remove that entry (kubernetes is the default) or give it the literal value kubernetes." -}}
{{- end -}}
{{- $substrate := .value | toString | trim | lower -}}
{{- if eq $substrate "" -}}
{{- fail "factory.modelAuthMode=personal needs the Kubernetes sandbox substrate, but worker.extraEnv sets CURIE_SANDBOX_SUBSTRATE to an empty value, which the worker refuses at startup; remove that entry (kubernetes is the default) or set it to kubernetes." -}}
{{- end -}}
{{- if ne $substrate "kubernetes" -}}
{{- fail (printf "factory.modelAuthMode=personal needs the Kubernetes sandbox substrate, but worker.extraEnv sets CURIE_SANDBOX_SUBSTRATE=%s; remove that entry (kubernetes is the default) or set factory.modelAuthMode=service." (toString .value)) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $mode -}}
{{- end -}}

{{/* A number as a plain string, never in exponent form. */}}
{{- define "curie.factory.str" -}}
{{- if kindIs "float64" . }}{{ . | int64 | toString }}{{ else }}{{ . | toString }}{{ end -}}
{{- end -}}

{{- define "curie.factory.secretField" -}}
{{- $type := get . "type" | default "github_app" -}}
{{- if eq $type "oauth_client_credentials" }}client_secret{{ else if eq $type "static_token" }}token{{ else }}private_key{{ end -}}
{{- end -}}

{{- define "curie.factory.credentials" -}}
{{- $factory := .Values.factory | default dict -}}
{{- $found := list -}}
{{- range $key := list "codeHosts" "trackers" -}}
{{- range $index, $entry := (get $factory $key | default list) -}}
{{- $credential := get $entry "credential" | default dict -}}
{{- $field := include "curie.factory.secretField" $credential -}}
{{- $path := printf "%s-%s/%s" (snakecase $key) (get $entry "name" | default "" | toString) $field -}}
{{- $found = append $found (dict "key" $key "index" $index "name" (get $entry "name" | default "") "entry" (omit $entry "credential") "credential" $credential "field" $field "path" $path) -}}
{{- end -}}
{{- end -}}
{{- toJson $found -}}
{{- end -}}

{{/* .v in snake_case as {"v": ...}; an *Id is a string. */}}
{{- define "curie.factory.snake" -}}
{{- $v := .v -}}
{{- if kindIs "map" $v -}}
{{- $out := dict -}}
{{- range $k, $c := $v -}}
{{- $n := snakecase $k | replace "interval_seconds" "interval_s" -}}
{{- if has $k (list "components" "reviewFeedbackAllowlist") }}{{ $_ := set $out $n $c }}
{{- else if hasSuffix "Id" $k }}{{ $_ := set $out $n (include "curie.factory.str" $c) }}
{{- else }}{{ $_ := set $out $n (include "curie.factory.snake" (dict "v" $c) | fromJson).v }}{{ end -}}
{{- end -}}
{{- $v = $out -}}
{{- else if kindIs "slice" $v -}}
{{- $out := list -}}
{{- range $v }}{{ $out = append $out (include "curie.factory.snake" (dict "v" .) | fromJson).v }}{{ end -}}
{{- $v = $out -}}
{{- end -}}
{{- toJson (dict "v" $v) -}}
{{- end -}}

{{- define "curie.factory.forgesJson" -}}
{{- $factory := .Values.factory | default dict -}}
{{- $doc := dict "code_hosts" list "trackers" list "card_base_url" (get $factory "cardBaseUrl" | default "" | toString) -}}
{{- $_ := set $doc "bindings" (include "curie.factory.snake" (dict "v" (get $factory "bindings" | default list)) | fromJson).v -}}
{{- range (include "curie.factory.credentials" . | fromJsonArray) -}}
{{- $out := (include "curie.factory.snake" (dict "v" (omit .entry "egress")) | fromJson).v -}}
{{- $credential := (include "curie.factory.snake" (dict "v" (omit .credential "existingSecret" "existingSecretKey" "privateKey" "clientSecret" "token")) | fromJson).v -}}
{{- $_ := set $credential "type" (get .credential "type" | default "github_app") -}}
{{- $_ := set $credential (printf "%s_file" .field) (printf "%s/%s" (include "curie.factory.mount" .) .path) -}}
{{- $_ := set $out "credential" $credential -}}
{{- $section := snakecase .key -}}
{{- $_ := set $doc $section (append (get $doc $section) $out) -}}
{{- end -}}
{{- toJson $doc -}}
{{- end -}}

{{- define "curie.factory.hasCredentials" -}}
{{- if include "curie.factory.credentials" . | fromJsonArray }}true{{ end -}}
{{- end -}}
{{- define "curie.factory.credentialVolume" -}}
{{- $credentials := include "curie.factory.credentials" . | fromJsonArray -}}
{{- if $credentials }}
- name: forge-credentials
  projected:
    defaultMode: 0440
    sources:
      {{- range $credentials }}
      {{- $credential := .credential }}
      - secret:
          name: {{ get $credential "existingSecret" | quote }}
          items:
            - key: {{ get $credential "existingSecretKey" | quote }}
              path: {{ .path | quote }}
      {{- end }}
{{- end }}
{{- end -}}
{{- define "curie.factory.credentialMount" -}}
{{- if include "curie.factory.hasCredentials" . }}
- name: forge-credentials
  mountPath: {{ include "curie.factory.mount" . }}
  readOnly: true
{{- end }}
{{- end -}}

{{/* Refuse a removed key set to other than null or its old default, and a removed env name in extraEnv. */}}
{{- define "curie.factory.legacyKeyGuard" -}}
{{- range $name := (.Files.Get "files/legacy-factory-env.yaml" | fromYamlArray) -}}
{{- range $workload := list "api" "worker" -}}
{{- range (get (get $.Values $workload | default dict) "extraEnv" | default list) -}}
{{- if eq (get . "name") $name -}}
{{- fail (printf "%s.extraEnv %s is no longer read; use `curie cluster upgrade` to migrate retained factory settings to factory.* (repository CI and bases under factory.bindings[].repos[]), or remove unsupported raw entries" $workload $name) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- range $key := (.Files.Get "files/legacy-factory-keys.yaml" | fromYamlArray) -}}
{{- $node := $.Values -}}
{{- range splitList "." $key }}{{ $node = and (kindIs "map" $node) (get $node .) }}{{ end -}}
{{- if and $node (not (has (include "curie.factory.str" $node) (list "poll" "45" "1200"))) -}}
{{- fail (printf "%s is no longer read; use `curie cluster upgrade` to migrate retained factory settings to factory.* (repository CI and bases under factory.bindings[].repos[]), or remove unsupported raw values" $key) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "curie.factory.validate" -}}
{{- include "curie.factory.legacyKeyGuard" . -}}
{{- $factory := .Values.factory | default dict -}}
{{- $appId := "" -}}
{{- with .Values.api.githubAppId }}{{ $appId = include "curie.factory.str" . }}{{ end -}}
{{- range (include "curie.factory.credentials" . | fromJsonArray) -}}
{{- $where := printf "factory.%s[%v] (%s)" .key .index .name -}}
{{- if not (and (get .credential "existingSecret") (get .credential "existingSecretKey")) -}}
{{- fail (printf "%s credential needs existingSecret and existingSecretKey" $where) -}}
{{- end -}}
{{- range (get .entry "egress" | default list) }}{{ include "curie.objectStore.egressEntry" (dict "key" (printf "%s egress" $where) "entry" .) }}{{ end -}}
{{- $own := get .credential "appId" -}}
{{- if and $appId $own (eq (get .credential "type" | default "github_app") "github_app") (ne (include "curie.factory.str" $own) $appId) -}}
{{- fail (printf "%s credential appId %s differs from api.githubAppId %s; the factory and git-flow must use one GitHub App" $where (include "curie.factory.str" $own) $appId) -}}
{{- end -}}
{{- end -}}
{{- /* One binding per tracker scope (#4622, ADR 0197), whether or not factory.enabled. Scope is (tracker kind, host stripped and lowercased, trackerScopeId). */ -}}
{{- $trackers := dict -}}
{{- range $index, $tracker := (get $factory "trackers" | default list) -}}
{{- $name := get $tracker "name" | default "" | toString -}}
{{- if hasKey $trackers $name -}}
{{- fail (printf "factory.trackers[%v] (%s) repeats a tracker name; tracker names must be unique" $index $name) -}}
{{- end -}}
{{- $_ := set $trackers $name $tracker -}}
{{- end -}}
{{- $bindings := dict -}}
{{- $scopes := dict -}}
{{- range $index, $binding := (get $factory "bindings" | default list) -}}
{{- $name := get $binding "name" | default "" | toString -}}
{{- if hasKey $bindings $name -}}
{{- fail (printf "factory.bindings[%v] (%s) repeats a binding name; binding names must be unique" $index $name) -}}
{{- end -}}
{{- $_ := set $bindings $name true -}}
{{- end -}}
{{- range $index, $binding := (get $factory "bindings" | default list) -}}
{{- $name := get $binding "name" | default "" | toString -}}
{{- $trackerName := get $binding "tracker" | default "" | toString -}}
{{- if not (hasKey $trackers $trackerName) -}}
{{- fail (printf "factory.bindings[%v] (%s) names unknown tracker %s; add it to factory.trackers or name an existing tracker" $index $name $trackerName) -}}
{{- end -}}
{{- $tracker := get $trackers $trackerName -}}
{{- $kind := get $tracker "kind" | default "" | toString -}}
{{- $host := get $tracker "host" | default "" | toString | trim | lower -}}
{{- /* A trackerScopeId is a nonempty string taken as is, or a whole number below 2^53. Anything else is refused, as the CLI preflight does. */ -}}
{{- $rawScope := get $binding "trackerScopeId" -}}
{{- $validScope := false -}}
{{- if kindIs "string" $rawScope }}{{ $validScope = ne $rawScope "" }}
{{- else if or (kindIs "float64" $rawScope) (kindIs "int64" $rawScope) (kindIs "int" $rawScope) -}}
{{- $f := float64 $rawScope -}}
{{- $validScope = and (eq $f (floor $f)) (lt $f 9007199254740992.0) (gt $f -9007199254740992.0) -}}
{{- end -}}
{{- if not $validScope -}}
{{- fail (printf "factory.bindings[%v] (%s) trackerScopeId must be a nonempty string or a whole number; quote the tracker's scope id" $index $name) -}}
{{- end -}}
{{- $scopeId := include "curie.factory.str" $rawScope -}}
{{- $key := toJson (list $kind $host $scopeId) -}}
{{- if hasKey $scopes $key -}}
{{- $first := get $scopes $key -}}
{{- fail (printf "factory.bindings %s and %s claim the same tracker scope (%s on %s, trackerScopeId %s); keep one binding per tracker scope: add the other repositories to binding %s repos and select one with a repo:<alias> label" $first $name $kind $host $scopeId $first) -}}
{{- end -}}
{{- $_ := set $scopes $key $name -}}
{{- end -}}
{{- /* factory.enabled needs a tracker and a code host of any installed kind (#4399); a GitHub entry in either list keeps GitHub's own pairing. */ -}}
{{- if get $factory "enabled" -}}
{{- $anyGithub := false -}}
{{- range concat (get $factory "trackers" | default list) (get $factory "codeHosts" | default list) }}{{ if eq (get . "kind" | default "") "github" }}{{ $anyGithub = true }}{{ end }}{{ end -}}
{{- range $section, $key := dict "tracker" "trackers" "code host" "codeHosts" -}}
{{- $entries := get $factory $key | default list -}}
{{- $github := false -}}
{{- range $entries }}{{ if eq (get . "kind" | default "") "github" }}{{ $github = true }}{{ end }}{{ end -}}
{{- if not $entries -}}
{{- fail (printf "factory.enabled needs a %s in factory.%s; run `curie cluster factory` to write one" $section $key) -}}
{{- end -}}
{{- if and $anyGithub (not $github) -}}
{{- fail (printf "factory.enabled with a github entry needs a github %s in factory.%s; run `curie cluster factory` to write one" $section $key) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{/* Forge egress (#4397): an entry's own, else the built-in GitHub set. */}}
{{- define "curie.factory.egress" -}}
{{- $f := .root.Values.factory | default dict -}}
{{- $gh := list -}}
{{- range splitList " " "192.30.252.0/22 185.199.108.0/22 140.82.112.0/20 143.55.64.0/20 2a0a:a440::/29 2606:50c0::/32 20.201.28.151/32 20.201.28.148/32 20.205.243.166/32 20.205.243.168/32 20.87.245.0/32 20.87.245.6/32 4.237.22.38/32 4.237.22.34/32 4.228.31.150/32 4.228.31.149/32 20.207.73.82/32 20.207.73.85/32 20.27.177.113/32 20.27.177.116/32 20.200.245.247/32 20.200.245.245/32 20.175.192.147/32 20.175.192.149/32 20.233.83.145/32 20.233.83.146/32 20.29.134.23/32 20.29.134.17/32 20.199.39.232/32 20.199.39.228/32 20.217.135.5/32 20.217.135.0/32 4.225.11.194/32 4.225.11.201/32 4.208.26.197/32 4.208.26.200/32 20.26.156.215/32 20.26.156.210/32 172.182.252.133/32 172.182.252.137/32 4.249.131.164/32 4.249.131.166/32 48.202.248.40/32 48.202.248.39/32 48.204.201.5/32 48.204.201.2/32" }}{{ $gh = append $gh (dict "cidr" . "ports" (list (dict "protocol" "TCP" "port" 443))) }}{{ end -}}
{{- $hosts := get $f "codeHosts" | default list -}}
{{- $out := ternary $gh list (not $hosts) -}}
{{- range concat $hosts (ternary (get $f "trackers" | default list) list (hasKey . "sandbox")) -}}
{{- $out = concat $out (get . "egress" | default (ternary $gh list (eq (get . "kind") "github"))) -}}
{{- end -}}
{{- toJson (uniq $out) -}}
{{- end -}}
{{- define "curie.factory.sandboxAgents" -}}
{{- $out := list -}}
{{- range $a, $on := get (.Values.factory | default dict) "sandboxEgress" | default dict -}}
{{- if or (gt (len $a) 40) (not (regexMatch "^[a-z0-9]([a-z0-9-]*[a-z0-9])?$" $a)) }}{{ fail (printf "factory.sandboxEgress.%s has an invalid agent name" $a) }}{{ end -}}
{{- if $on }}{{ $out = append $out $a }}{{ end -}}
{{- end -}}
{{- toJson $out -}}
{{- end -}}

{{/* ADR 0217: resolve and validate the worker's daemon handoff at the root. */}}
{{- define "curie.factory.dockerConfig" -}}
{{- $factory := .Values.factory | default dict -}}
{{- $docker := get $factory "docker" -}}
{{- if or (not (hasKey $factory "docker")) (kindIs "invalid" $docker) }}{{ $docker = dict }}{{ end -}}
{{- if not (kindIs "map" $docker) }}{{ fail "factory.docker must be a map" }}{{ end -}}
{{- $image := get $docker "daemonImage" -}}
{{- if not (and (kindIs "string" $image) (regexMatch "^[A-Za-z0-9][A-Za-z0-9._/:-]*@sha256:[0-9a-f]{64}$" $image)) }}{{ fail "factory.docker.daemonImage must be a CEL-safe digest-pinned image reference when a repository enables docker" }}{{ end -}}
{{- $portRaw := "2376" -}}
{{- if hasKey $docker "port" }}{{ $portRaw = get $docker "port" | toString }}{{ end -}}
{{- if not (regexMatch "^[0-9]{1,5}$" $portRaw) }}{{ fail "factory.docker.port must be an integer from 1 through 65535" }}{{ end -}}
{{- $port := int $portRaw -}}
{{- if or (lt $port 1) (gt $port 65535) }}{{ fail "factory.docker.port must be an integer from 1 through 65535" }}{{ end -}}
{{- $namespaceRaw := get $docker "namespace" -}}
{{- if and (not (kindIs "invalid" $namespaceRaw)) (not (kindIs "string" $namespaceRaw)) }}{{ fail "factory.docker.namespace must be a string" }}{{ end -}}
{{- $namespace := include "curie.factory.dockerNamespace" . -}}
{{- if or (gt (len $namespace) 63) (not (regexMatch "^[a-z0-9]([a-z0-9-]*[a-z0-9])?$" $namespace)) }}{{ fail "factory.docker.namespace must resolve to a Kubernetes namespace DNS label" }}{{ end -}}
{{- $selector := include "curie.selectorLabels" (dict "root" . "component" "docker-daemon") | fromYaml -}}
{{- range $value := $selector -}}
{{- if not (and (kindIs "string" $value) (le (len $value) 63) (regexMatch "^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$" $value)) }}{{ fail "factory.docker selector labels must be nonempty Kubernetes label values of at most 63 characters" }}{{ end -}}
{{- end -}}
{{- $resources := get $docker "resources" -}}
{{- if or (not (hasKey $docker "resources")) (kindIs "invalid" $resources) }}{{ $resources = dict }}{{ end -}}
{{- if not (kindIs "map" $resources) }}{{ fail "factory.docker.resources must be a map" }}{{ end -}}
{{- if omit $resources "requests" "limits" }}{{ fail "factory.docker.resources accepts only requests and limits" }}{{ end -}}
{{- $resolved := dict -}}
{{- range $kind, $defaults := dict "requests" (dict "cpu" "500m" "memory" "1Gi" "ephemeral-storage" "10Gi") "limits" (dict "cpu" "2" "memory" "4Gi" "ephemeral-storage" "20Gi") -}}
{{- $supplied := get $resources $kind -}}
{{- if or (not (hasKey $resources $kind)) (kindIs "invalid" $supplied) }}{{ $supplied = dict }}{{ end -}}
{{- if not (kindIs "map" $supplied) }}{{ fail (printf "factory.docker.resources.%s must be a map" $kind) }}{{ end -}}
{{- if omit $supplied "cpu" "memory" "ephemeral-storage" }}{{ fail "factory.docker resources accept only cpu, memory and ephemeral-storage" }}{{ end -}}
{{- $group := dict -}}
{{- range $key, $fallback := $defaults -}}
{{- $value := get $supplied $key -}}
{{- if or (not (hasKey $supplied $key)) (kindIs "invalid" $value) }}{{ $value = $fallback }}{{ end -}}
{{- $_ := set $group $key $value -}}
{{- end -}}
{{- $_ := set $resolved $kind $group -}}
{{- end -}}
{{- $out := dict "namespace" $namespace "image" $image "port" $port "selectorLabels" $selector "resources" $resolved -}}
{{- range $key, $fallback := dict "storageSizeLimit" "20Gi" "maxClaimLifetimeSeconds" 86400 "pingTimeoutSeconds" 2 -}}
{{- $value := get $docker $key -}}
{{- if or (not (hasKey $docker $key)) (kindIs "invalid" $value) }}{{ $value = $fallback }}{{ end -}}
{{- if and (ne $key "storageSizeLimit") (not (regexMatch "^[1-9][0-9]*$" (toJson $value))) }}{{ fail (printf "factory.docker.%s must be a positive integer" $key) }}{{ end -}}
{{- $_ := set $out $key $value -}}
{{- end -}}
{{- $quantity := "^\\+?(?:[0-9]*[1-9][0-9]*(?:\\.[0-9]*)?|[0-9]*\\.[0-9]*[1-9][0-9]*)(?:[eE][+-]?[0-9]{1,3}|[numkMGTPE]|[KMGTPE]i)?$" -}}
{{- range $value := concat (values $resolved.requests) (values $resolved.limits) (list $out.storageSizeLimit) -}}
{{- if not (and (kindIs "string" $value) (le (len $value) 128) (not (regexMatch "\\s" $value)) (regexMatch $quantity $value)) }}{{ fail "factory.docker resources and storageSizeLimit must be positive Kubernetes quantity strings of at most 128 characters" }}{{ end -}}
{{- end -}}
{{- toJson $out -}}
{{- end -}}

{{/* ADR 0216: one descriptor per repository, sharing pools by index digest. */}}
{{- define "curie.factory.repoRunnerDescriptors" -}}
{{- $root := . -}}
{{- $entries := list -}}
{{- $hasRunners := false -}}
{{- range $bi, $binding := get (.Values.factory | default dict) "bindings" | default list -}}
{{- range $ri, $repo := get $binding "repos" | default list -}}
{{- $entries = append $entries (dict "repo" $repo "key" (printf "factory.bindings[%d].repos[%d]" $bi $ri)) -}}
{{- if get $repo "runner" }}{{ $hasRunners = true }}{{ end -}}
{{- end -}}
{{- end -}}
{{- $descriptors := dict -}}
{{- if $hasRunners -}}
{{- $agents := list -}}
{{- range $agent, $on := get (.Values.factory | default dict) "sandboxEgress" | default dict -}}
{{- if not (kindIs "bool" $on) }}{{ fail (printf "factory.sandboxEgress.%s must be a boolean when repository runners are configured" $agent) }}{{ end -}}
{{- if $on }}{{ $agents = append $agents $agent }}{{ end -}}
{{- end -}}
{{- if ne (len $agents) 1 }}{{ fail "repository runners require exactly one true factory agent in factory.sandboxEgress" }}{{ end -}}
{{- $agent := first $agents -}}
{{- $poolAgents := include "curie.agentSandboxPoolAgents" . | fromJsonArray -}}
{{- if not .Values.agentSandbox.deploy }}{{ fail "repository runners require agentSandbox.deploy=true for chart-managed sandbox pools" }}{{ end -}}
{{- $basePool := printf "%s-runner-pool" (include "curie.fullname" .) -}}
{{- $warmPool := get (.Values.worker | default dict) "warmPool" | default $basePool -}}
{{- if ne $warmPool $basePool }}{{ fail "repository runners require worker.warmPool to be empty or the chart-managed runner pool" }}{{ end -}}
{{- $declarations := dict -}}
{{- $identities := dict -}}
{{- $prefixes := dict -}}
{{- range $entries -}}
{{- $repo := .repo -}}
{{- $path := get $repo "path" | default "" | toString -}}
{{- $declaration := toJson $repo -}}
{{- if and (hasKey $declarations $path) (ne (get $declarations $path) $declaration) }}{{ fail (printf "conflicting duplicate repository path %q in %s; repository declarations must be identical" $path .key) }}{{ end -}}
{{- $_ := set $declarations $path $declaration -}}
{{- $source := printf "%s.runner" .key -}}
{{- with get $repo "runner" -}}
{{- if not (regexMatch "^[A-Za-z0-9_][A-Za-z0-9._~-]*(/[A-Za-z0-9_][A-Za-z0-9._~-]*)+$" $path) }}{{ fail (printf "%s path %q is not a repository path" $source $path) }}{{ end -}}
{{- $image := get . "image" | default "" | toString -}}
{{- if not (regexMatch "^[A-Za-z0-9][A-Za-z0-9._/:-]*@sha256:[0-9a-f]{64}$" $image) }}{{ fail (printf "%s.image must be a CEL-safe digest image reference" $source) }}{{ end -}}
{{- $index := last (splitList "@" $image) -}}
{{- $base := get . "base" | default "" | toString -}}
{{- $platforms := get . "platformDigests" | default dict -}}
{{- $amd64 := get $platforms "linux/amd64" | default "" | toString -}}
{{- $arm64 := get $platforms "linux/arm64" | default "" | toString -}}
{{- range $digest := list $base $amd64 $arm64 -}}
{{- if not (regexMatch "^sha256:[0-9a-f]{64}$" $digest) }}{{ fail (printf "%s base and platformDigests must be sha256 digests" $source) }}{{ end -}}
{{- end -}}
{{- $identity := toJson (dict "image" $image "base" $base "platformDigests" $platforms) -}}
{{- if and (hasKey $identities $index) (ne (get $identities $index) $identity) }}{{ fail (printf "conflicting repository runner image identity for index digest %s" $index) }}{{ end -}}
{{- $_ := set $identities $index $identity -}}
{{- $prefix := trimPrefix "sha256:" $index | trunc 12 -}}
{{- if and (hasKey $prefixes $prefix) (ne (get $prefixes $prefix) $index) }}{{ fail (printf "repository runner digest prefix collision for %s" $prefix) }}{{ end -}}
{{- $_ := set $prefixes $prefix $index -}}
{{- if has (printf "%s-img-%s" $agent $prefix) $poolAgents }}{{ fail (printf "repository runner name collision with ordinary agent %s-img-%s" $agent $prefix) }}{{ end -}}
{{- $template := printf "%s-agent-%s-img-%s-runner" (include "curie.fullname" $root) $agent $prefix -}}
{{- $pool := printf "%s-pool" $template -}}
{{- if or (gt (len $pool) 63) (not (regexMatch "^[a-z0-9]([a-z0-9-]*[a-z0-9])?$" $pool)) }}{{ fail (printf "repository runner pool name %q must be a DNS label of at most 63 characters" $pool) }}{{ end -}}
{{- $docker := get $repo "docker" | default dict -}}
{{- $dockerOn := get $docker "enabled" | default false -}}
{{- $egress := list -}}
{{- if $dockerOn -}}
{{- range get $docker "egressAllowlist" | default list -}}
{{- $ports := list -}}
{{- range .ports -}}
{{- $port := deepCopy . -}}
{{- if not (hasKey $port "protocol") }}{{ $_ := set $port "protocol" "TCP" }}{{ end -}}
{{- $ports = append $ports $port -}}
{{- end -}}
{{- $egress = append $egress (set (deepCopy .) "ports" $ports) -}}
{{- end -}}
{{- end -}}
{{- $descriptor := dict "path" $path "agent" $agent "prefix" $prefix "template" $template "pool" $pool "image" $image "digests" (list $index $amd64 $arm64) "base" $base "docker" $dockerOn "egress" $egress "source" $source -}}
{{- if and $dockerOn $root.Values.worker.deploy -}}
{{- $_ := set $descriptor "daemon" (include "curie.factory.dockerConfig" $root | fromJson) -}}
{{- end -}}
{{- $_ := set $descriptors $path $descriptor -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $out := list -}}
{{- range $path := keys $descriptors | sortAlpha }}{{ $out = append $out (get $descriptors $path) }}{{ end -}}
{{- toJson $out -}}
{{- end -}}

{{- define "curie.factory.repoRunnerPoolsJson" -}}
{{- $out := dict -}}
{{- range . -}}
{{- $_ := set $out .path (pick . "pool" "digests" "base" "docker" "egress" "daemon") -}}
{{- end -}}
{{- toJson $out -}}
{{- end -}}

{{/* HTTP identity operator files (@spec HTTP-INVOCATION-EXECUTION-20): each names an operator-created Secret or renders empty. Read through dig for --reuse-values. */}}
{{- define "curie.httpIdentity.apiSecret" -}}
{{- dig "httpIdentity" "existingSecret" "" (.Values.api | default dict) -}}
{{- end -}}
{{- define "curie.httpIdentity.providerCaBundle" -}}
{{- if dig "httpIdentity" "providerCaBundle" false (.Values.api | default dict) -}}
{{- if not (include "curie.httpIdentity.apiSecret" .) -}}
{{- fail "api.httpIdentity.providerCaBundle requires api.httpIdentity.existingSecret: the CA bundle is a key of that Secret" -}}
{{- end -}}
true
{{- end -}}
{{- end -}}
{{- define "curie.httpIdentity.receiverSecret" -}}
{{- $name := dig "httpIdentity" "existingSecret" "" (.Values.dispatcher | default dict) -}}
{{- if and $name (not (include "curie.httpIdentity.apiSecret" .)) -}}
{{- fail "dispatcher.httpIdentity.existingSecret requires api.httpIdentity.existingSecret: the API must hold the receiver registrations it verifies" -}}
{{- end -}}
{{- $name -}}
{{- end -}}
{{/* Named operator Secret keys as 0440 files; only the named workload mounts them. */}}
{{- define "curie.httpIdentity.volume" -}}
- name: {{ .name }}
  secret:
    secretName: {{ .secret | quote }}
    defaultMode: 0440
    items:
      {{- range .files }}
      - key: {{ . }}
        path: {{ . }}
      {{- end }}
{{- end -}}
