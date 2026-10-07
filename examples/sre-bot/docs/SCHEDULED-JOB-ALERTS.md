# Scheduled job health alerts

Feature #4251. This opt-in example watches maintenance CronJobs separately from application workload availability. It does not change the default SRE installation.

## Contract

`SRE-SCHEDULED-JOBS c1`: a CronJob with `kube_cronjob_spec_suspend == 1` in the selected namespace pages after one minute. A nonsuspended job does not fire.

`SRE-SCHEDULED-JOBS c2`: a job whose last schedule timestamp exceeds its last successful timestamp, or whose schedule has no successful timestamp, pages after fifteen minutes. An unscheduled job and a latest successful schedule do not fire. This is a schedule/success comparison, not a missed-schedule or exporter-liveness detector.

`SRE-SCHEDULED-JOBS c3`: both rules retain namespace and cronjob labels, set severity `page` and component `scheduled-job`, and use installation supplied impact text. Namespace is an explicit validated Kubernetes namespace argument. No identifiers or credentials from an installation are defaults.

`SRE-SCHEDULED-JOBS c4`: `python3 examples/sre-bot/observability/scheduled_job_alerts.py --namespace acme-system --description "Maintenance credentials and inventory depend on these jobs."` writes one JSON Helm values object locally. JSON is valid YAML. It needs no third party Python package, cluster, registry, Helm invocation or credential. Invalid arguments fail before output.

`SRE-SCHEDULED-JOBS c5`: the output uses `serverFiles.alerts.groups`, separate from the default example's `serverFiles.alerting_rules.yml`. Apply it as an additional values file with the example's existing Prometheus values. If another overlay already defines `serverFiles.alerts.groups`, combine those lists deliberately before Helm; Helm replaces lists and a second file does not append groups. Opt in only for namespaces where suspension represents failure, rather than an intentionally disabled template.

## Verification

The rules are evaluated with Prometheus 3.5.0, observed using `docker run --rm --entrypoint /bin/promtool prom/prometheus:v3.5.0 --version`. Positive and negative evaluator cases must cover the suspension hold, unsuccessful schedules with and without previous success, successful schedules, unscheduled jobs and namespace exclusion. A running Prometheus must load and evaluate the generated expressions through its query surface before completion. This qualifies the rule consumer, not Alertmanager delivery, scheduling correctness or an application's credential validity.
