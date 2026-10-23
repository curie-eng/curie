{{/*
Sandbox resource profiles (#4889, ADR-0059 "every ceiling is an
operator-overridable chart value").

curie.sandboxProfileName: (dict "root" $ "agent" <name, "" for the generic
template>) -> the profile name for that template:
  1. agentSandbox.agentResourceProfiles.<agent> when set;
  2. `factory` when factory.enabled and factory.sandboxEgress.<agent> is true;
  3. `standard` otherwise (always for the generic template).
*/}}
{{- define "curie.sandboxProfileName" -}}
{{- $root := .root -}}
{{- $agent := .agent -}}
{{- $explicit := "" -}}
{{- if $agent -}}
{{- $explicit = toString (get (get $root.Values.agentSandbox "agentResourceProfiles" | default dict) $agent | default "") -}}
{{- end -}}
{{- if $explicit -}}
{{- $explicit -}}
{{- else if and $agent (get ($root.Values.factory | default dict) "enabled") (has $agent (include "curie.factory.sandboxAgents" $root | fromJsonArray)) -}}
factory
{{- else -}}
standard
{{- end -}}
{{- end -}}

{{/*
curie.sandboxEnvelope.bytes: a quantity -> its byte count as a decimal string,
or "" when the form is not one this render-time check compares. Only bare
integers and the binary suffixes Ki, Mi, Gi and Ti are compared; decimal SI
(1G), fractions, exponents, `m`, Pi and Ei are left to the API server, which
still refuses request > limit at pod create.
*/}}
{{- define "curie.sandboxEnvelope.bytes" -}}
{{- $q := toString . -}}
{{- if regexMatch "^(0|[1-9][0-9]*)(Ki|Mi|Gi|Ti)?$" $q -}}
{{- $digits := regexFind "^[0-9]+" $q -}}
{{- $mult := get (dict "" 1 "Ki" 1024 "Mi" 1048576 "Gi" 1073741824 "Ti" 1099511627776) (trimPrefix $digits $q) -}}
{{- mul (int64 $digits) $mult -}}
{{- end -}}
{{- end -}}

{{/*
curie.sandboxEnvelope.nonpositive: a quantity -> "true" when its numeric part
is zero or it starts with "-", else "".
*/}}
{{- define "curie.sandboxEnvelope.nonpositive" -}}
{{- $q := toString . -}}
{{- if or (hasPrefix "-" $q) (regexMatch "^\\+?(0+\\.?0*|\\.0+)([eE][+-]?[0-9]+|[A-Za-z]*)$" $q) -}}true{{- end -}}
{{- end -}}

{{/*
curie.sandboxEnvelope: (dict "root" $ "agent" <name or "">) -> JSON
  {"profile", "resources": {"requests", "limits"}, "workspaceSizeLimit",
   "writable": [one sizeLimit per runner.hardening.writablePaths entry]}

The profile catalog (agentSandbox.resourceProfiles) is consulted only for a
field no explicit value supplies, so a values set without the catalog (a
release made by a chart <= 0.14.0, reused with --reuse-values) renders from its
explicit generic keys. Precedence:
  resources, per pair (cpu, memory, ephemeral-storage): when either
    agentSandbox.runner.resources.requests.<r> or .limits.<r> is set, the pair
    comes from those generic keys for EVERY template, the unset side from the
    `standard` profile; otherwise both sides come from the selected profile.
  workspace: agentSandbox.workspaceSizeLimits.<agent> > runner.workspace.sizeLimit
    > profile workspaceSizeLimit (only while runner.workspace.enabled).
  writable path: runner.hardening.writablePathSizeLimit (every path) > profile
    homeSizeLimit for runner.hardening.home, tmpSizeLimit for /tmp,
    writablePathSizeLimit for any other path (only while hardening.enabled).
Every needed field must resolve (ADR-0059: finite bounds), and memory and
ephemeral-storage requests must not exceed their limits (see
curie.sandboxEnvelope.bytes for the forms compared); each failure names the
template, the profile and the keys to set.
*/}}
{{- define "curie.sandboxEnvelope" -}}
{{- $root := .root -}}
{{- $agent := .agent -}}
{{- $who := "generic template" -}}
{{- if $agent }}{{ $who = printf "agent %s" $agent }}{{ end -}}
{{- $sb := $root.Values.agentSandbox -}}
{{- $runner := $sb.runner -}}
{{- $name := include "curie.sandboxProfileName" (dict "root" $root "agent" $agent) -}}
{{- $rawCatalog := get $sb "resourceProfiles" -}}
{{- $catalog := $rawCatalog | default dict -}}
{{- if and $agent (hasKey (get $sb "agentResourceProfiles" | default dict) $agent) (kindIs "map" $rawCatalog) (not (hasKey $catalog $name)) -}}
{{- fail (printf "agentSandbox.agentResourceProfiles.%s names profile %q, which is not a key of agentSandbox.resourceProfiles (have: %s)" $agent $name (keys $catalog | sortAlpha | join ", ")) -}}
{{- end -}}
{{- $profile := get $catalog $name | default dict -}}
{{- $pkey := printf "agentSandbox.resourceProfiles.%s" $name -}}
{{- $std := get $catalog "standard" | default dict -}}
{{- $gen := $runner.resources | default dict -}}
{{- $genSides := dict "requests" (get $gen "requests" | default dict) "limits" (get $gen "limits" | default dict) -}}
{{- $profSides := dict "requests" (get (get $profile "resources" | default dict) "requests" | default dict) "limits" (get (get $profile "resources" | default dict) "limits" | default dict) -}}
{{- $stdSides := dict "requests" (get (get $std "resources" | default dict) "requests" | default dict) "limits" (get (get $std "resources" | default dict) "limits" | default dict) -}}
{{- /* Start from the generic maps so any extra resource an operator set
       (an extended resource, hugepages) still reaches every container. */ -}}
{{- $out := dict "requests" (deepCopy $genSides.requests) "limits" (deepCopy $genSides.limits) -}}
{{- range $r := list "cpu" "memory" "ephemeral-storage" -}}
{{- $gr := get $genSides.requests $r -}}
{{- $gl := get $genSides.limits $r -}}
{{- $genPair := or (and (hasKey $genSides.requests $r) (not (kindIs "invalid" $gr)) (ne (toString $gr) "")) (and (hasKey $genSides.limits $r) (not (kindIs "invalid" $gl)) (ne (toString $gl) "")) -}}
{{- $val := dict -}}
{{- $src := dict -}}
{{- range $side := list "requests" "limits" -}}
{{- $v := "" -}}
{{- $k := "" -}}
{{- if $genPair -}}
{{- $v = get (get $genSides $side) $r -}}
{{- if kindIs "invalid" $v }}{{ $v = "" }}{{ end -}}
{{- $k = printf "agentSandbox.runner.resources.%s.%s" $side $r -}}
{{- if eq (toString $v) "" -}}
{{- $v = get (get $stdSides $side) $r -}}
{{- if kindIs "invalid" $v }}{{ $v = "" }}{{ end -}}
{{- $k = printf "agentSandbox.resourceProfiles.standard.resources.%s.%s" $side $r -}}
{{- end -}}
{{- else -}}
{{- $v = get (get $profSides $side) $r -}}
{{- if kindIs "invalid" $v }}{{ $v = "" }}{{ end -}}
{{- $k = printf "%s.resources.%s.%s" $pkey $side $r -}}
{{- end -}}
{{- if eq (toString $v) "" -}}
{{- fail (printf "sandbox resources for the %s (profile %s): %s resolves to no value; set %s, or agentSandbox.runner.resources.%s.%s for every template (ADR-0059 requires every sandbox container to declare cpu, memory and ephemeral-storage requests and limits)" $who $name $r $k $side $r) -}}
{{- end -}}
{{- $_ := set $val $side $v -}}
{{- $_ := set $src $side $k -}}
{{- $_ := set (get $out $side) $r $v -}}
{{- end -}}
{{- if ne $r "cpu" -}}
{{- $reqB := include "curie.sandboxEnvelope.bytes" $val.requests -}}
{{- $limB := include "curie.sandboxEnvelope.bytes" $val.limits -}}
{{- if and $reqB $limB -}}
{{- if gt (int64 $reqB) (int64 $limB) -}}
{{- fail (printf "sandbox resources for the %s (profile %s): %s request %s (from %s) exceeds its limit %s (from %s); raise the limit or lower the request so request <= limit" $who $name $r (toString $val.requests) $src.requests (toString $val.limits) $src.limits) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $ws := "" -}}
{{- $wf := $runner.workspace | default dict -}}
{{- if $wf.enabled -}}
{{- $k := "" -}}
{{- $wa := "" -}}
{{- if $agent }}{{ $wa = get ($sb.workspaceSizeLimits | default dict) $agent }}{{ end -}}
{{- if kindIs "invalid" $wa }}{{ $wa = "" }}{{ end -}}
{{- $wg := get $wf "sizeLimit" -}}
{{- if kindIs "invalid" $wg }}{{ $wg = "" }}{{ end -}}
{{- if ne (toString $wa) "" -}}
{{- $ws = $wa -}}
{{- $k = printf "agentSandbox.workspaceSizeLimits.%s" $agent -}}
{{- else if ne (toString $wg) "" -}}
{{- $ws = $wg -}}
{{- $k = "agentSandbox.runner.workspace.sizeLimit" -}}
{{- else -}}
{{- $ws = get $profile "workspaceSizeLimit" -}}
{{- if kindIs "invalid" $ws }}{{ $ws = "" }}{{ end -}}
{{- $k = printf "%s.workspaceSizeLimit" $pkey -}}
{{- end -}}
{{- if eq (toString $ws) "" -}}
{{- fail (printf "sandbox workspace for the %s (profile %s) resolves to no sizeLimit; set %s, agentSandbox.runner.workspace.sizeLimit for every template, or agentSandbox.workspaceSizeLimits.<agent> (ADR-0059 requires a finite sizeLimit on every writable emptyDir)" $who $name $k) -}}
{{- end -}}
{{- if include "curie.sandboxEnvelope.nonpositive" $ws -}}
{{- fail (printf "sandbox workspace sizeLimit %v for the %s (profile %s) from %s is not a positive quantity; the kubelet does not enforce a zero or negative emptyDir sizeLimit (ADR-0059 requires a finite sizeLimit)" $ws $who $name $k) -}}
{{- end -}}
{{- end -}}
{{- $writable := list -}}
{{- $h := $runner.hardening | default dict -}}
{{- if $h.enabled -}}
{{- $gw := get $h "writablePathSizeLimit" -}}
{{- if kindIs "invalid" $gw }}{{ $gw = "" }}{{ end -}}
{{- range $p := $h.writablePaths | default list -}}
{{- $v := $gw -}}
{{- $vk := "agentSandbox.runner.hardening.writablePathSizeLimit" -}}
{{- if eq (toString $v) "" -}}
{{- $field := "writablePathSizeLimit" -}}
{{- if eq (toString $p) (toString (get $h "home" | default "")) }}{{ $field = "homeSizeLimit" }}{{ else if eq (toString $p) "/tmp" }}{{ $field = "tmpSizeLimit" }}{{ end -}}
{{- $v = get $profile $field -}}
{{- if kindIs "invalid" $v }}{{ $v = "" }}{{ end -}}
{{- $vk = printf "%s.%s" $pkey $field -}}
{{- if eq (toString $v) "" -}}
{{- fail (printf "sandbox writable path %s for the %s (profile %s) resolves to no sizeLimit; set %s.%s, or agentSandbox.runner.hardening.writablePathSizeLimit for every path and template (ADR-0059 requires a finite sizeLimit on every writable emptyDir)" $p $who $name $pkey $field) -}}
{{- end -}}
{{- end -}}
{{- if include "curie.sandboxEnvelope.nonpositive" $v -}}
{{- fail (printf "sandbox writable path %s sizeLimit %v for the %s (profile %s) from %s is not a positive quantity; the kubelet does not enforce a zero or negative emptyDir sizeLimit (ADR-0059 requires a finite sizeLimit)" $p $v $who $name $vk) -}}
{{- end -}}
{{- $writable = append $writable $v -}}
{{- end -}}
{{- end -}}
{{- toJson (dict "profile" $name "resources" $out "workspaceSizeLimit" $ws "writable" $writable) -}}
{{- end -}}

{{/*
curie.sandboxEnvelope.validate: refuse an invalid agentResourceProfiles entry
before any template renders from it.
*/}}
{{- define "curie.sandboxEnvelope.validate" -}}
{{- $sb := .Values.agentSandbox -}}
{{- $rawCatalog := get $sb "resourceProfiles" -}}
{{- $catalog := $rawCatalog | default dict -}}
{{- range $agent, $name := get $sb "agentResourceProfiles" | default dict -}}
{{- if or (gt (len $agent) 40) (not (regexMatch "^[a-z0-9]([a-z0-9-]*[a-z0-9])?$" $agent)) -}}
{{- fail (printf "agentSandbox.agentResourceProfiles.%s has an invalid agent name" $agent) -}}
{{- end -}}
{{- if not (kindIs "string" $name) -}}
{{- fail (printf "agentSandbox.agentResourceProfiles.%s must be a profile name (a key of agentSandbox.resourceProfiles), got %q" $agent (toString $name)) -}}
{{- end -}}
{{- if eq $name "" -}}
{{- fail (printf "agentSandbox.agentResourceProfiles.%s is empty; name a key of agentSandbox.resourceProfiles or remove the entry" $agent) -}}
{{- end -}}
{{- if and (kindIs "map" $rawCatalog) (not (hasKey $catalog $name)) -}}
{{- fail (printf "agentSandbox.agentResourceProfiles.%s names profile %q, which is not a key of agentSandbox.resourceProfiles (have: %s)" $agent $name (keys $catalog | sortAlpha | join ", ")) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
curie.sandboxEnvelope.summary: (dict "root" $ "env" <curie.sandboxEnvelope
fromJson>) -> one NOTES line body naming every effective limit separately:
"memory <req>/<limit>, cpu <req>/<limit>, ephemeral-storage <req>/<limit>,
workspace <size>, home <size>, tmp <size>" ("none" where no volume renders).
*/}}
{{- define "curie.sandboxEnvelope.summary" -}}
{{- $env := .env -}}
{{- $h := .root.Values.agentSandbox.runner.hardening | default dict -}}
{{- $req := $env.resources.requests -}}
{{- $lim := $env.resources.limits -}}
{{- $home := "none" -}}
{{- $tmp := "none" -}}
{{- range $i, $p := $h.writablePaths | default list -}}
{{- if lt $i (len $env.writable) -}}
{{- if and (eq (toString $p) (toString (get $h "home" | default ""))) (eq $home "none") }}{{ $home = toString (index $env.writable $i) }}{{ end -}}
{{- if and (eq (toString $p) "/tmp") (eq $tmp "none") }}{{ $tmp = toString (index $env.writable $i) }}{{ end -}}
{{- end -}}
{{- end -}}
{{- printf "memory %v/%v, cpu %v/%v, ephemeral-storage %v/%v, workspace %s, home %s, tmp %s" (get $req "memory") (get $lim "memory") (get $req "cpu") (get $lim "cpu") (get $req "ephemeral-storage") (get $lim "ephemeral-storage") (toString ($env.workspaceSizeLimit | default "none")) $home $tmp -}}
{{- end -}}

{{/*
curie.sandboxEnvelope.differing: root -> JSON list of {"agent", "env"} for
every per-agent pool whose effective envelope differs from the generic
template's in any field (#4889 A6). Used by NOTES.
*/}}
{{- define "curie.sandboxEnvelope.differing" -}}
{{- $root := . -}}
{{- $generic := include "curie.sandboxEnvelope" (dict "root" $root "agent" "") | fromJson -}}
{{- $gkey := toJson (list $generic.resources $generic.workspaceSizeLimit $generic.writable) -}}
{{- $out := list -}}
{{- range $agent := include "curie.agentSandboxPoolAgents" $root | fromJsonArray -}}
{{- $env := include "curie.sandboxEnvelope" (dict "root" $root "agent" $agent) | fromJson -}}
{{- if ne $gkey (toJson (list $env.resources $env.workspaceSizeLimit $env.writable)) -}}
{{- $out = append $out (dict "agent" $agent "env" $env) -}}
{{- end -}}
{{- end -}}
{{- toJson $out -}}
{{- end -}}
