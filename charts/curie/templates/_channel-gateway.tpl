{{- define "curie.channelGateway.values" -}}
{{- $raw := required "files/channel-gateway-defaults.yaml is required; restore the chart defaults file" (.Files.Get "files/channel-gateway-defaults.yaml") -}}
{{- $defaults := $raw | fromYaml -}}
{{- if or (not $defaults) (hasKey $defaults "Error") -}}
{{- fail "files/channel-gateway-defaults.yaml is invalid; restore the chart defaults file" -}}
{{- end -}}
{{- mergeOverwrite $defaults (.Values.channelGateway | default dict) | toYaml -}}
{{- end -}}

{{- define "curie.channelGateway.deploy" -}}
{{- $gateway := include "curie.channelGateway.values" . | fromYaml -}}
{{- if kindIs "bool" $gateway.deploy -}}
{{- $gateway.deploy -}}
{{- else if not (kindIs "invalid" $gateway.deploy) -}}
{{- fail "channelGateway.deploy must be true, false or null (automatic)" -}}
{{- else -}}
{{- $ids := include "curie.slack.identities" . | fromJson -}}
{{- and (not $gateway.url) (or (include "curie.slack.blockConfigured" .) (gt (int $ids.listed) 0)) | toString -}}
{{- end -}}
{{- end -}}

{{- define "curie.channelGateway.tlsSecret" -}}
{{- $gateway := include "curie.channelGateway.values" . | fromYaml -}}
{{- if $gateway.tlsSecret -}}
{{- $gateway.tlsSecret -}}
{{- else if and (eq (include "curie.channelGateway.deploy" .) "true") (not $gateway.allowInsecure) -}}
{{- include "curie.fullname" . }}-channel-gateway-tls
{{- end -}}
{{- end -}}

{{- define "curie.channelGateway.clientMount" -}}
{{- if include "curie.channelGateway.tlsSecret" . }}
- name: gateway-tls
  mountPath: /var/run/curie/gateway-tls
  readOnly: true
{{- end -}}
{{- end -}}

{{- define "curie.channelGateway.clientVolume" -}}
{{- $secret := include "curie.channelGateway.tlsSecret" . -}}
{{- if $secret }}
- name: gateway-tls
  secret:
    secretName: {{ $secret }}
    items:
      - key: ca.crt
        path: ca.crt
{{- end -}}
{{- end -}}

{{- define "curie.env.channelGateway" -}}
{{- $root := .root -}}
{{- $gateway := include "curie.channelGateway.values" $root | fromYaml -}}
{{- if or (eq (include "curie.channelGateway.deploy" $root) "true") $gateway.url -}}
{{- $http := and (eq (toString $gateway.allowInsecure) "true") (eq (include "curie.channelGateway.tlsSecret" $root) "") -}}
{{- $scheme := ternary "http" "https" $http -}}
- name: CURIE_CHANNEL_GATEWAY_URL
  value: {{ $gateway.url | default (printf "%s://%s-channel-gateway:%v" $scheme (include "curie.fullname" $root) $gateway.port) | quote }}
- name: CURIE_CHANNEL_GATEWAY_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ include "curie.secretName" $root }}
      key: {{ printf "channelGateway%sToken" (title .caller) }}
{{- if include "curie.channelGateway.tlsSecret" $root }}
- name: CURIE_CHANNEL_GATEWAY_CA_FILE
  value: /var/run/curie/gateway-tls/ca.crt
{{- end }}
- name: CURIE_CHANNEL_GATEWAY_ALLOW_INSECURE
  value: {{ $http | quote }}
{{- end -}}
{{- end -}}
