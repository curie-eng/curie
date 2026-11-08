{{- define "curie.env.channelGateway" -}}
{{- $root := .root -}}
{{- if or $root.Values.channelGateway.deploy $root.Values.channelGateway.url -}}
{{- $scheme := ternary "http" "https" $root.Values.channelGateway.allowInsecure -}}
- name: CURIE_CHANNEL_GATEWAY_URL
  value: {{ $root.Values.channelGateway.url | default (printf "%s://%s-channel-gateway:%v" $scheme (include "curie.fullname" $root) $root.Values.channelGateway.port) | quote }}
- name: CURIE_CHANNEL_GATEWAY_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ include "curie.secretName" $root }}
      key: {{ printf "channelGateway%sToken" (title .caller) }}
- name: CURIE_CHANNEL_GATEWAY_ALLOW_INSECURE
  value: {{ $root.Values.channelGateway.allowInsecure | quote }}
{{- end -}}
{{- end -}}
