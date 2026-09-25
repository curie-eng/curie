{{/* Slack identities (ADR-0168 decision 1).

     dispatcher.slack is the identity named `default`; dispatcher.slack.identities
     lists more, each by existingSecret reference only (#1759). With no list
     entries this renders exactly the SLACK_* entries it always did and nothing
     else. The rules the render refuses are the ones
     packages/aci-protocol/src/aci_protocol/slack_identities.py refuses at boot;
     charts/curie/ci/slack-identities-assertions.sh feeds the rendered JSON
     through that parser so the two cannot drift. */}}

{{- define "curie.slack.blockConfigured" -}}
{{- $s := .Values.dispatcher.slack -}}
{{- if or $s.appToken $s.appTokenExistingSecret $s.botToken $s.botTokenExistingSecret $s.signingSecret $s.signingSecretExistingSecret -}}true{{- end -}}
{{- end -}}

{{/* The validated identities as JSON {"items": [...]}, default first. Each item
     carries its env names and its secret references. Only fromJson-able types:
     helm's include returns a string. */}}
{{- define "curie.slack.identities" -}}
{{- $s := .Values.dispatcher.slack -}}
{{- $list := $s.identities | default list -}}
{{- if not (kindIs "slice" $list) -}}
{{- fail (printf "dispatcher.slack.identities must be a list of identities; got %s." (kindOf $list)) -}}
{{- end -}}
{{- $allowed := list "name" "appTokenExistingSecret" "appTokenExistingSecretKey" "botTokenExistingSecret" "botTokenExistingSecretKey" "signingSecretExistingSecret" "signingSecretExistingSecretKey" -}}
{{- $block := include "curie.slack.blockConfigured" . -}}
{{- $items := list -}}
{{- $listed := list -}}
{{- $seen := dict -}}
{{- range $i, $entry := $list -}}
{{- $where := printf "dispatcher.slack.identities[%v]" $i -}}
{{- if not (kindIs "map" $entry) -}}
{{- fail (printf "%s must be a mapping; got %s." $where (kindOf $entry)) -}}
{{- end -}}
{{- range $key := keys $entry | sortAlpha -}}
{{- if has $key (list "appToken" "botToken" "signingSecret") -}}
{{- fail (printf "%s.%s is a plain secret value; an identity takes its secrets only by reference (#1759). Set %s.%sExistingSecret and %sExistingSecretKey instead." $where $key $where $key $key) -}}
{{- end -}}
{{- if not (has $key $allowed) -}}
{{- fail (printf "%s has unknown key %q; allowed keys are %s." $where $key (join ", " $allowed)) -}}
{{- end -}}
{{- end -}}
{{- $name := get $entry "name" -}}
{{- if not (kindIs "string" $name) -}}
{{- fail (printf "%s.name must be a string." $where) -}}
{{- end -}}
{{- if or (gt (len $name) 40) (not (regexMatch "^[a-z0-9]([a-z0-9-]*[a-z0-9])?$" $name)) -}}
{{- fail (printf "%s.name %q must match ^[a-z0-9]([a-z0-9-]*[a-z0-9])?$ and be at most 40 characters, the deploy target name shape." $where $name) -}}
{{- end -}}
{{- if hasKey $seen $name -}}
{{- fail (printf "%s.name %q repeats %s; identity names are unique." $where $name (get $seen $name)) -}}
{{- end -}}
{{- $_ := set $seen $name $where -}}
{{- if and (eq $name "default") $block -}}
{{- fail (printf "%s names the identity \"default\", which dispatcher.slack already configures. Configure it in one place: remove this entry, or clear the dispatcher.slack token fields." $where) -}}
{{- end -}}
{{- range $token := list "appToken" "botToken" -}}
{{- $ref := get $entry (printf "%sExistingSecret" $token) -}}
{{- if not (and (kindIs "string" $ref) $ref) -}}
{{- fail (printf "%s.%sExistingSecret must name the Secret holding this identity's %s." $where $token $token) -}}
{{- end -}}
{{- end -}}
{{- $isDefault := eq $name "default" -}}
{{- $signing := get $entry "signingSecretExistingSecret" | default "" -}}
{{- $item := dict
      "name" $name
      "where" $where
      "appEnv" (ternary "SLACK_APP_TOKEN" (printf "CURIE_SLACK_APP_TOKEN__%v" $i) $isDefault)
      "botEnv" (ternary "SLACK_BOT_TOKEN" (printf "CURIE_SLACK_BOT_TOKEN__%v" $i) $isDefault)
      "signingEnv" (ternary "" (ternary "SLACK_SIGNING_SECRET" (printf "CURIE_SLACK_SIGNING_SECRET__%v" $i) $isDefault) (eq $signing ""))
      "appSecret" (get $entry "appTokenExistingSecret")
      "appKey" (get $entry "appTokenExistingSecretKey" | default "slackAppToken")
      "botSecret" (get $entry "botTokenExistingSecret")
      "botKey" (get $entry "botTokenExistingSecretKey" | default "slackBotToken")
      "signingSecret" $signing
      "signingKey" (get $entry "signingSecretExistingSecretKey" | default "slackSigningSecret") -}}
{{- $listed = append $listed $item -}}
{{- end -}}
{{- if $listed -}}
{{- if and $block (not (and (or $s.appToken $s.appTokenExistingSecret) (or $s.botToken $s.botTokenExistingSecret))) -}}
{{- fail "dispatcher.slack is half-configured: alongside dispatcher.slack.identities it is the identity \"default\" and needs both an app token and a bot token (plain or existingSecret)." -}}
{{- end -}}
{{- if and (not $block) (not (hasKey $seen "default")) -}}
{{- fail "dispatcher.slack.identities declares no identity named \"default\". Every Slack route that names no identity means \"default\", so configure it in dispatcher.slack or as a list entry named default." -}}
{{- end -}}
{{- end -}}
{{- if or $block (not $listed) -}}
{{- $items = append $items (dict
      "name" "default"
      "where" "dispatcher.slack"
      "appEnv" "SLACK_APP_TOKEN" "botEnv" "SLACK_BOT_TOKEN" "signingEnv" "SLACK_SIGNING_SECRET"
      "appSecret" ($s.appTokenExistingSecret | default "") "appKey" $s.appTokenExistingSecretKey
      "botSecret" ($s.botTokenExistingSecret | default "") "botKey" $s.botTokenExistingSecretKey
      "signingSecret" ($s.signingSecretExistingSecret | default "") "signingKey" $s.signingSecretExistingSecretKey) -}}
{{- end -}}
{{- range $item := $listed -}}
{{- if eq $item.name "default" -}}
{{- $items = prepend $items $item -}}
{{- else -}}
{{- $items = append $items $item -}}
{{- end -}}
{{- end -}}
{{- toJson (dict "items" $items "listed" (len $listed)) -}}
{{- end -}}

{{/* CURIE_SLACK_IDENTITIES: each identity's name and the env names holding its
     tokens. One string for all three workloads. */}}
{{- define "curie.slack.identitiesJson" -}}
{{- $ids := include "curie.slack.identities" . | fromJson -}}
{{- $out := list -}}
{{- range $item := $ids.items -}}
{{- $out = append $out (dict "name" $item.name "app_token_env" $item.appEnv "bot_token_env" $item.botEnv "signing_secret_env" (ternary nil $item.signingEnv (eq $item.signingEnv ""))) -}}
{{- end -}}
{{- toJson $out -}}
{{- end -}}

{{/* The Slack env entries for one workload: dict "root" "workload". The
     dispatcher gets all three tokens per identity; the worker and API get only
     bot tokens. */}}
{{- define "curie.env.slack" -}}
{{- $root := .root -}}
{{- $dispatcher := eq .workload "dispatcher" -}}
{{- $ids := include "curie.slack.identities" $root | fromJson -}}
{{- range $item := $ids.items }}
{{- if $dispatcher }}
- name: {{ $item.appEnv }}
  valueFrom:
    secretKeyRef:
      {{- include "curie.secretRef" (dict "root" $root "existingSecret" $item.appSecret "existingSecretKey" $item.appKey "defaultKey" "slackAppToken") | nindent 6 }}
{{- end }}
- name: {{ $item.botEnv }}
  valueFrom:
    secretKeyRef:
      {{- include "curie.secretRef" (dict "root" $root "existingSecret" $item.botSecret "existingSecretKey" $item.botKey "defaultKey" "slackBotToken") | nindent 6 }}
{{- if and $dispatcher $item.signingEnv }}
- name: {{ $item.signingEnv }}
  valueFrom:
    secretKeyRef:
      {{- include "curie.secretRef" (dict "root" $root "existingSecret" $item.signingSecret "existingSecretKey" $item.signingKey "defaultKey" "slackSigningSecret") | nindent 6 }}
{{- end }}
{{- end }}
{{- if gt (int $ids.listed) 0 }}
- name: CURIE_SLACK_IDENTITIES
  value: {{ include "curie.slack.identitiesJson" $root | quote }}
{{- end }}
{{- end -}}

{{/* The per-identity names this render owns, as a JSON map for curie.extraEnv's
     additionalReserved. The legacy SLACK_* names and CURIE_SLACK_IDENTITIES are
     reserved statically in files/reserved-env.yaml. */}}
{{- define "curie.slack.reservedEnv" -}}
{{- $dispatcher := eq .workload "dispatcher" -}}
{{- $ids := include "curie.slack.identities" .root | fromJson -}}
{{- $reserved := dict -}}
{{- range $item := $ids.items -}}
{{- if hasPrefix "CURIE_" $item.botEnv -}}
{{- $_ := set $reserved $item.botEnv $item.where -}}
{{- if $dispatcher -}}
{{- $_ := set $reserved $item.appEnv $item.where -}}
{{- if $item.signingEnv -}}{{- $_ := set $reserved $item.signingEnv $item.where -}}{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- toJson $reserved -}}
{{- end -}}
