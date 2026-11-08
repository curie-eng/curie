{{- define "curie.env.channelGateway" -}}
{{- $root := .root -}}
{{- $gateway := $root.Values.channelGateway | default dict -}}
{{- if or $gateway.deploy $gateway.url -}}
{{- $http := and $gateway.allowInsecure (not $gateway.tlsSecret) -}}
{{- $scheme := ternary "http" "https" $http -}}
- name: CURIE_CHANNEL_GATEWAY_URL
  value: {{ $gateway.url | default (printf "%s://%s-channel-gateway:%v" $scheme (include "curie.fullname" $root) $gateway.port) | quote }}
- name: CURIE_CHANNEL_GATEWAY_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ include "curie.secretName" $root }}
      key: {{ printf "channelGateway%sToken" (title .caller) }}
- name: CURIE_CHANNEL_GATEWAY_ALLOW_INSECURE
  value: {{ $http | quote }}
{{- end -}}
{{- end -}}
