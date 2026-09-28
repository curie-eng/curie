{{/*
Sandbox capacity arithmetic (#2949). The sandbox ResourceQuota is an ADMISSION
ceiling, not a scheduling guarantee: it counts requests against `hard`, never
against what nodes can hold. These helpers compare the two so NOTES.txt can say
plainly how many sandboxes actually fit. Pure functions of their inputs (no
lookup here) so ci/sandbox-capacity-fit-assertions.sh can drive them with
fixture Node/Pod objects.
*/}}

{{/*
curie.capacity.cpuMillis: a CPU quantity ("2", "0.5", "1500m", 2) as integer
millicores. Empty renders 0.
*/}}
{{- define "curie.capacity.cpuMillis" -}}
{{- $q := toString . | trim -}}
{{- if or (eq $q "") (eq $q "<nil>") -}}0
{{- else if hasSuffix "m" $q -}}{{ trimSuffix "m" $q | float64 | floor | int64 }}
{{- else -}}{{ mulf (float64 $q) 1000 | floor | int64 }}
{{- end -}}
{{- end -}}

{{/*
curie.capacity.memBytes: a memory quantity (bytes, k/M/G/T/P, Ki/Mi/Gi/Ti/Pi,
decimals and a decimal exponent such as 1e9 or 1.5E3 allowed, plus the milli
"m" suffix Kubernetes itself accepts, e.g. "1500m" = 1.5 bytes) as integer
bytes, rounded down (a value under 1 byte becomes 0). Empty renders 0. Any
other form (a typo, an unrecognized suffix) FAILS rendering with the quantity
named, rather than silently reading as 0 and hiding the capacity warning.
*/}}
{{- define "curie.capacity.memBytes" -}}
{{- $q := toString . | trim -}}
{{- $units := dict "Ki" 1024.0 "Mi" 1048576.0 "Gi" 1073741824.0 "Ti" 1099511627776.0 "Pi" 1125899906842624.0 "k" 1000.0 "K" 1000.0 "M" 1000000.0 "G" 1000000000.0 "T" 1000000000000.0 "P" 1000000000000000.0 -}}
{{- $num := regexFind "^[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?" $q -}}
{{- $suffix := trimPrefix $num $q -}}
{{- if or (eq $q "") (eq $q "<nil>") -}}0
{{- else if eq $num "" -}}{{- fail (printf "curie.capacity.memBytes: unsupported memory quantity %q (#2949)" $q) -}}
{{- else if eq $suffix "" -}}{{ float64 $num | floor | int64 }}
{{- else if eq $suffix "m" -}}{{ divf (float64 $num) 1000 | floor | int64 }}
{{- else if hasKey $units $suffix -}}{{ mulf (float64 $num) (get $units $suffix) | floor | int64 }}
{{- else -}}{{- fail (printf "curie.capacity.memBytes: unsupported memory quantity %q (suffix %q); use bytes, an exponent, m, or k/M/G/T/P, Ki/Mi/Gi/Ti/Pi (#2949)" $q $suffix) -}}
{{- end -}}
{{- end -}}

{{/*
curie.capacity.podRequests: one Pod object's effective requests as
{"cpu": millicores, "mem": bytes}: per resource, max(sum over containers,
max over initContainers) -- the value the scheduler reserves.
*/}}
{{- define "curie.capacity.podRequests" -}}
{{- $spec := .spec | default dict -}}
{{- $cpu := 0 -}}{{- $mem := 0 -}}
{{- range ($spec.containers | default list) -}}
{{- $r := (.resources | default dict).requests | default dict -}}
{{- $cpu = add $cpu (include "curie.capacity.cpuMillis" $r.cpu | int64) -}}
{{- $mem = add $mem (include "curie.capacity.memBytes" $r.memory | int64) -}}
{{- end -}}
{{- range ($spec.initContainers | default list) -}}
{{- $r := (.resources | default dict).requests | default dict -}}
{{- $cpu = max $cpu (include "curie.capacity.cpuMillis" $r.cpu | int64) -}}
{{- $mem = max $mem (include "curie.capacity.memBytes" $r.memory | int64) -}}
{{- end -}}
{{- dict "cpu" $cpu "mem" $mem | toJson -}}
{{- end -}}

{{/*
curie.capacity.tolerated: "true" when the taint (.taint) is tolerated by one of
.tolerations. Simple match: an Exists toleration with an empty key tolerates
everything; otherwise the key must match, and for Equal (the default operator)
the value must too. A toleration's effect, when set, must match the taint's.
*/}}
{{- define "curie.capacity.tolerated" -}}
{{- $t := .taint -}}
{{- $ok := false -}}
{{- range (.tolerations | default list) -}}
{{- $op := .operator | default "Equal" -}}
{{- $effectOk := or (not .effect) (eq (toString .effect) (toString $t.effect)) -}}
{{- if $effectOk -}}
{{- if and (eq $op "Exists") (not .key) -}}{{- $ok = true -}}
{{- else if eq (toString .key) (toString $t.key) -}}
{{- if eq $op "Exists" -}}{{- $ok = true -}}
{{- else if eq (toString (.value | default "")) (toString ($t.value | default "")) -}}{{- $ok = true -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if $ok -}}true{{- end -}}
{{- end -}}

{{/*
curie.sandboxCapacity: how many sandboxes fit on the nodes vs. how many the
quota admits. Input dict:
  nodes                 list of v1 Node objects
  pods                  list of v1 Pod objects
  perSandbox            runner resources.requests ({cpu, memory})
  perSandboxLimits      runner resources.limits ({cpu, memory})
  sandboxPriorityClass  the sandbox PriorityClass name
  tolerations           placement.sandbox.tolerations
  nodeSelector          placement.sandbox.nodeSelector
  hard                  resourceQuota.hard
Renders JSON {"fits": N, "ceiling": M, "nodes": eligibleNodeCount}.

fits: sum over eligible nodes (schedulable, matching nodeSelector, every
NoSchedule/NoExecute taint tolerated) of floor(free / per-sandbox request),
minimum over cpu and memory, where free = allocatable minus the requests of
pods bound to the node that are not Succeeded/Failed and are NOT sandbox pods
(existing sandboxes are part of the answer, not a deduction from it). A
dimension whose per-sandbox request is 0 is skipped.
ceiling: min(pods, requestsCpu / request cpu, requestsMemory / request memory,
limitsCpu / limit cpu, limitsMemory / limit memory). The quota caps limits too,
so with the shipped defaults (limitsCpu 8, 1 cpu limit per sandbox) limits.cpu
is the binding term. A dimension whose per-sandbox value is 0 or unset is
skipped.
*/}}
{{- define "curie.sandboxCapacity" -}}
{{- $perCpu := include "curie.capacity.cpuMillis" (.perSandbox | default dict).cpu | int64 -}}
{{- $perMem := include "curie.capacity.memBytes" (.perSandbox | default dict).memory | int64 -}}
{{- $sel := .nodeSelector | default dict -}}
{{- $sandboxClass := .sandboxPriorityClass -}}
{{- $used := dict -}}
{{- range (.pods | default list) -}}
{{- $node := (.spec | default dict).nodeName | default "" -}}
{{- $phase := (.status | default dict).phase | default "" -}}
{{- $class := (.spec | default dict).priorityClassName | default "" -}}
{{- if and $node (not (has $phase (list "Succeeded" "Failed"))) (ne $class $sandboxClass) -}}
{{- $r := include "curie.capacity.podRequests" . | fromJson -}}
{{- $prev := get $used $node | default (dict "cpu" 0 "mem" 0) -}}
{{- $_ := set $used $node (dict "cpu" (add $prev.cpu (int64 $r.cpu)) "mem" (add $prev.mem (int64 $r.mem))) -}}
{{- end -}}
{{- end -}}
{{- $fits := 0 -}}
{{- $eligible := 0 -}}
{{- $tolerations := .tolerations -}}
{{- range (.nodes | default list) -}}
{{- $n := . -}}
{{- $ok := not ($n.spec | default dict).unschedulable -}}
{{- $labels := ($n.metadata | default dict).labels | default dict -}}
{{- range $k, $v := $sel -}}
{{- if ne (toString (get $labels $k)) (toString $v) -}}{{- $ok = false -}}{{- end -}}
{{- end -}}
{{- range (($n.spec | default dict).taints | default list) -}}
{{- if and (has (toString .effect) (list "NoSchedule" "NoExecute")) (not (include "curie.capacity.tolerated" (dict "taint" . "tolerations" $tolerations))) -}}{{- $ok = false -}}{{- end -}}
{{- end -}}
{{- if $ok -}}
{{- $eligible = add1 $eligible -}}
{{- $alloc := ($n.status | default dict).allocatable | default dict -}}
{{- $u := get $used $n.metadata.name | default (dict "cpu" 0 "mem" 0) -}}
{{- $freeCpu := sub (include "curie.capacity.cpuMillis" $alloc.cpu | int64) (int64 $u.cpu) -}}
{{- $freeMem := sub (include "curie.capacity.memBytes" $alloc.memory | int64) (int64 $u.mem) -}}
{{- $counts := list -}}
{{- if gt $perCpu 0 -}}{{- $counts = append $counts (div (max $freeCpu 0) $perCpu) -}}{{- end -}}
{{- if gt $perMem 0 -}}{{- $counts = append $counts (div (max $freeMem 0) $perMem) -}}{{- end -}}
{{- if $counts -}}{{- $fits = add $fits (max 0 (min (first $counts) (last $counts))) -}}{{- end -}}
{{- end -}}
{{- end -}}
{{- $hard := .hard | default dict -}}
{{- $ceiling := int64 (toString $hard.sandboxPodCount | default "0") -}}
{{- if gt $perCpu 0 -}}{{- $ceiling = min $ceiling (div (include "curie.capacity.cpuMillis" $hard.requestsCpu | int64) $perCpu) -}}{{- end -}}
{{- if gt $perMem 0 -}}{{- $ceiling = min $ceiling (div (include "curie.capacity.memBytes" $hard.requestsMemory | int64) $perMem) -}}{{- end -}}
{{- $limCpu := include "curie.capacity.cpuMillis" (.perSandboxLimits | default dict).cpu | int64 -}}
{{- $limMem := include "curie.capacity.memBytes" (.perSandboxLimits | default dict).memory | int64 -}}
{{- if gt $limCpu 0 -}}{{- $ceiling = min $ceiling (div (include "curie.capacity.cpuMillis" $hard.limitsCpu | int64) $limCpu) -}}{{- end -}}
{{- if gt $limMem 0 -}}{{- $ceiling = min $ceiling (div (include "curie.capacity.memBytes" $hard.limitsMemory | int64) $limMem) -}}{{- end -}}
{{- dict "fits" $fits "ceiling" $ceiling "nodes" $eligible | toJson -}}
{{- end -}}

{{/*
curie.sandboxCapacity.notes: the NOTES.txt block for a computed capacity.
Input dict: fits, ceiling, cpu, memory (per-sandbox request strings),
fullname, namespace. NOTES.txt calls this after its lookups; the CI script
calls it directly with numbers.
*/}}
{{- define "curie.sandboxCapacity.notes" -}}
{{- if gt (int64 .ceiling) (int64 .fits) }}
  - !! WARNING: the sandbox ResourceQuota admits {{ .ceiling }} sandboxes, but the
       nodes fit only {{ .fits }} at current requests (per sandbox cpu {{ .cpu }}, memory {{ .memory }}).
       Sandboxes past {{ .fits }} are admitted and then stay Pending until the claim times out.
       The quota is an admission ceiling, not a scheduling guarantee.
       Tell the two refusals apart:
         quota refusal: pod create rejected with "exceeded quota"
           kubectl describe resourcequota {{ .fullname }}-sandbox-quota -n {{ .namespace }}
         unschedulable: pod Pending with FailedScheduling events
           kubectl get events --field-selector reason=FailedScheduling -n {{ .namespace }}
       Remedy: lower resourceQuota.hard.* to match, or add node capacity.
{{- else }}
  - Sandbox quota admits {{ .ceiling }}; the nodes currently fit {{ .fits }} (per sandbox cpu {{ .cpu }}, memory {{ .memory }}).
{{- end }}
{{- end -}}
