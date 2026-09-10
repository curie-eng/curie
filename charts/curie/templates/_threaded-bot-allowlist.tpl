{{/* Validate dispatcher.threadedBotAllowlist against the dispatcher's own
     ThreadedBotAdmission contract and encode it as the JSON array
     CURIE_SLACK_THREADED_BOT_ALLOWLIST carries. Fail at RENDER, not pod start.
     Rebuild each entry rather than re-serializing the operator's map, so an
     unknown key can never reach the wire even if the shape check is loosened.

     Source of truth for both regexes and for the exactly-two-keys rule:
     apps/dispatcher/src/curie_dispatcher/config.py::ThreadedBotAdmission
     (extra="forbid"). They are duplicated here on purpose so a malformed entry
     is refused by `helm upgrade` instead of crash-looping the dispatcher; the
     duplication is pinned against drift by
     charts/curie/ci/threaded-bot-allowlist-assertions.sh, which feeds the
     rendered string through that real parser. */}}
{{- define "curie.dispatcher.threadedBotAllowlist" -}}
{{- $normalized := list -}}
{{- range $i, $entry := .Values.dispatcher.threadedBotAllowlist -}}
{{- $where := printf "dispatcher.threadedBotAllowlist[%v]" $i -}}
{{- if not (kindIs "map" $entry) -}}
{{- fail (printf "%s must be a mapping with exactly channel_id and bot_id; got %s." $where (kindOf $entry)) -}}
{{- end -}}
{{- $names := keys $entry | sortAlpha -}}
{{- if ne (join "," $names) "bot_id,channel_id" -}}
{{/* Name both full paths: helm DROPS a key set to an empty value (`--set-string
     ...channel_id=` on helm 3.20.0 leaves the key absent, not empty), so this
     branch -- not the regex below -- is where a blanked field lands, and the
     operator needs to see which field it was. */}}
{{- fail (printf "%s must have exactly the keys channel_id and bot_id (%s.channel_id and %s.bot_id); got [%s]." $where $where $where (join " " $names)) -}}
{{- end -}}
{{- $channel := get $entry "channel_id" -}}
{{- $bot := get $entry "bot_id" -}}
{{- if not (kindIs "string" $channel) -}}{{- fail (printf "%s.channel_id must be a string." $where) -}}{{- end -}}
{{- if not (kindIs "string" $bot) -}}{{- fail (printf "%s.bot_id must be a string." $where) -}}{{- end -}}
{{- if not (regexMatch "^[CG][A-Z0-9]+$" $channel) -}}
{{- fail (printf "%s.channel_id %q does not match ^[CG][A-Z0-9]+$ (apps/dispatcher ThreadedBotAdmission)." $where $channel) -}}
{{- end -}}
{{- if not (regexMatch "^B[A-Z0-9]+$" $bot) -}}
{{- fail (printf "%s.bot_id %q does not match ^B[A-Z0-9]+$ (apps/dispatcher ThreadedBotAdmission)." $where $bot) -}}
{{- end -}}
{{- $normalized = append $normalized (dict "channel_id" $channel "bot_id" $bot) -}}
{{- end -}}
{{- toJson $normalized -}}
{{- end -}}
