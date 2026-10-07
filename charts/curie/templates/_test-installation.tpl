{{/* The test installation declaration (ADR 0202 decision 1).

     testInstallation.enabled renders as CURIE_TEST_INSTALLATION_ENABLED and
     testInstallation.drivers as CURIE_TEST_INSTALLATION_DRIVERS, into the API
     and the dispatcher alike. Both helpers read the block through
     `.Values.testInstallation | default dict`, so a --reuse-values upgrade
     from a release that predates the key renders the default.

     The entry rules and the three published defaults repeat
     packages/curie-internal/src/curie_internal/driver_declaration.py, the one
     parser both services read the value with. They are duplicated here so a
     bad value fails the render rather than the boot; the duplication is pinned
     by charts/curie/ci/test-installation-assertions.sh, which feeds the
     rendered value through both services' real settings. */}}

{{- define "curie.testInstallation.enabled" -}}
{{- $block := .Values.testInstallation | default dict -}}
{{- if not (kindIs "map" $block) -}}
{{- fail (printf "testInstallation must be a mapping; got %s." (kindOf $block)) -}}
{{- end -}}
{{- $enabled := false -}}
{{- if hasKey $block "enabled" -}}{{- $enabled = index $block "enabled" -}}{{- end -}}
{{- if kindIs "invalid" $enabled -}}{{- $enabled = false -}}{{- end -}}
{{- if not (kindIs "bool" $enabled) -}}
{{- fail (printf "testInstallation.enabled must be true or false; got %s." (kindOf $enabled)) -}}
{{- end -}}
{{- ternary "true" "false" $enabled -}}
{{- end -}}

{{/* Validate testInstallation.drivers and encode it as the JSON list the
     services read. Each entry is rebuilt from its known keys, so nothing the
     operator added can reach the wire. Validated whether or not the
     declaration is on, so a typo fails before the day it is switched on. */}}
{{- define "curie.testInstallation.drivers" -}}
{{- $block := .Values.testInstallation | default dict -}}
{{- $drivers := list -}}
{{- if and (kindIs "map" $block) (hasKey $block "drivers") -}}{{- $drivers = index $block "drivers" | default list -}}{{- end -}}
{{- if not (kindIs "slice" $drivers) -}}
{{- fail (printf "testInstallation.drivers must be a list of driver entries; got %s." (kindOf $drivers)) -}}
{{- end -}}
{{- $allowed := list "channel_id" "bot_id" "bot_user_id" "agent" -}}
{{- $normalized := list -}}
{{- range $i, $entry := $drivers -}}
{{- $where := printf "testInstallation.drivers[%v]" $i -}}
{{- if not (kindIs "map" $entry) -}}
{{- fail (printf "%s must be a mapping of channel_id, bot_id, bot_user_id and, for a sibling driver, agent; got %s." $where (kindOf $entry)) -}}
{{- end -}}
{{- range $key := keys $entry | sortAlpha -}}
{{- if not (has $key $allowed) -}}
{{- fail (printf "%s has unknown key %s; a driver entry takes channel_id, bot_id, bot_user_id and, for a sibling driver, agent." $where $key) -}}
{{- end -}}
{{- end -}}
{{- $channel := get $entry "channel_id" -}}
{{- if or (not (hasKey $entry "channel_id")) (empty $channel) -}}
{{- fail (printf "%s has no channel: set %s.channel_id to the channel the driver acts in (ADR 0202)." $where $where) -}}
{{- end -}}
{{- $out := dict -}}
{{- range $field := list (list "channel_id" "^[CG][A-Z0-9]+$") (list "bot_id" "^B[A-Z0-9]+$") (list "bot_user_id" "^U[A-Z0-9]+$") -}}
{{- $name := index $field 0 -}}
{{- $pattern := index $field 1 -}}
{{- $value := get $entry $name -}}
{{- if not (hasKey $entry $name) -}}
{{- fail (printf "%s.%s is required." $where $name) -}}
{{- end -}}
{{- if not (kindIs "string" $value) -}}
{{- fail (printf "%s.%s must be a string." $where $name) -}}
{{- end -}}
{{- if not (regexMatch $pattern $value) -}}
{{- fail (printf "%s.%s %q does not match %s." $where $name $value $pattern) -}}
{{- end -}}
{{- $_ := set $out $name $value -}}
{{- end -}}
{{- if hasKey $entry "agent" -}}
{{- $agent := get $entry "agent" -}}
{{- if or (not (kindIs "string" $agent)) (eq (trim (toString $agent)) "") -}}
{{- fail (printf "%s.agent must be a nonblank agent name when present." $where) -}}
{{- end -}}
{{- $_ := set $out "agent" $agent -}}
{{- end -}}
{{- $normalized = append $normalized $out -}}
{{- end -}}
{{- toJson $normalized -}}
{{- end -}}

{{/* The render refusals. Included from secrets.yaml, which renders on every
     install, upgrade and template, with the chart managed Secret's existing
     data so a secret retained from an earlier render is judged as the value
     the services will actually read. */}}
{{- define "curie.testInstallation.check" -}}
{{- $root := .root -}}
{{- $_ := include "curie.testInstallation.drivers" $root -}}
{{- if eq (include "curie.testInstallation.enabled" $root) "true" -}}
{{- if eq (lower (trim (toString $root.Values.api.environment))) "prod" -}}
{{- fail "testInstallation.enabled=true but api.environment is prod. A test installation admits a listed bot's actions and approval replies, so the chart refuses it on an installation that says it is production (ADR 0202). Turn testInstallation.enabled off, or deploy the test installation separately." -}}
{{- end -}}
{{- $offenders := list -}}
{{- range $secret := list (list "apiKey" "api.apiKey" $root.Values.api.apiKey "curie-dev-key") (list "internalWorkerToken" "worker.internalWorkerToken" $root.Values.worker.internalWorkerToken "curie-dev-worker-token") (list "approvalChatAttesterSecret" "api.approvalChatAttesterSecret" $root.Values.api.approvalChatAttesterSecret "curie-dev-approval-chat-attester") -}}
{{- $default := index $secret 3 -}}
{{- $resolved := include "curie.managedSecret" (dict "root" $root "key" (index $secret 0) "value" (index $secret 2) "default" $default "hex" false "existingData" $.existingData) -}}
{{- if eq $resolved $default -}}
{{- $offenders = append $offenders (index $secret 1) -}}
{{- end -}}
{{- end -}}
{{- if $offenders -}}
{{- fail (printf "testInstallation.enabled=true but these secrets are still the published default: %s. A test installation admits bot-driven actions, so it may not run on a secret anyone reading this repository holds (ADR 0202). Set real values, or leave them unset without security.allowDevDefaults so the chart generates them." (join ", " $offenders)) -}}
{{- end -}}
{{- end -}}
{{- end -}}
