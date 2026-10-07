# Optional CloudWatch source

Contract: [SRE-CW-1 through SRE-CW-8](../../docs/CLOUDWATCH-ALARMS.md).
This source reads alarms for an existing SNS topic. The default example installer
starts no reader and loads no CloudWatch rules. Keep installation identities and
all AWS values in private operator configuration.

Provide a `cloudwatch-alarms` ServiceAccount in the Prometheus namespace before
applying the manifest. On EKS with IRSA, privately annotate it with the read-only
role ARN using `eks.amazonaws.com/role-arn`. The EKS identity webhook must inject
`AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE` and the projected token volume into
the reader pod. `automountServiceAccountToken: false` disables the Kubernetes API
token, not the explicit AWS token projection. On another web-identity setup,
provide equivalent environment variables and a read-only projected token mount
in a private manifest overlay. Static access keys and Kubernetes API permissions
are unnecessary.

The role's trust grants `sts:AssumeRoleWithWebIdentity` to the exact namespace and
ServiceAccount subject, with the provider's expected audience. Its identity policy
allows only `cloudwatch:DescribeAlarms`. Reading composite alarms requires this
permission on `*`; an alarm-resource-only grant cannot return composites. Configure
trust and permissions privately; this example creates neither. See the
[AWS DescribeAlarms contract](https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_DescribeAlarms.html)
and [web identity contract](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html).

From the repository root, set the installation inputs privately and create both
ConfigMaps. These commands are an operator procedure, not automatic installer
steps. Send any notice required by your installation before applying an upgrade.

```bash
(
set -euo pipefail
: "${KUBE_CONTEXT:?Set the installation context}"
: "${TOPIC_ARN:?Set the existing SNS topic ARN}"
: "${AWS_REGION:?Set its AWS region}"
OBS_NAMESPACE=${OBS_NAMESPACE:-observability}
# Default consumers below use curie_cloudwatch. An existing consumer may retain
# its old names by setting METRIC_PREFIX and changing the optional rules too.
METRIC_PREFIX=${METRIC_PREFIX:-curie_cloudwatch}
kubectl --context "$KUBE_CONTEXT" -n "$OBS_NAMESPACE" create configmap cloudwatch-alarms-config \
  --from-literal="TOPIC_ARN=$TOPIC_ARN" --from-literal="AWS_REGION=$AWS_REGION" \
  --from-literal="METRIC_PREFIX=$METRIC_PREFIX" --dry-run=client -o yaml |
  kubectl --context "$KUBE_CONTEXT" -n "$OBS_NAMESPACE" apply -f -
kubectl --context "$KUBE_CONTEXT" -n "$OBS_NAMESPACE" create configmap cloudwatch-alarms-code \
  --from-file=cloudwatch_alarms.py=examples/sre-bot/observability/cloudwatch-alarms/cloudwatch_alarms.py \
  --dry-run=client -o yaml |
  kubectl --context "$KUBE_CONTEXT" -n "$OBS_NAMESPACE" apply -f -
kubectl --context "$KUBE_CONTEXT" -n "$OBS_NAMESPACE" apply \
  -f examples/sre-bot/observability/cloudwatch-alarms.yaml
kubectl --context "$KUBE_CONTEXT" -n "$OBS_NAMESPACE" rollout status deployment/cloudwatch-alarms
)
```

The annotated Service is discovered by the example Prometheus within its own
namespace. Its port 9108 serves `/metrics`; the pod is not annotated, preventing
a second annotation-driven scrape. Probes establish that the listener is alive,
not that AWS reads succeeded. Check `curie_cloudwatch_alarm_poll_ok` and
`curie_cloudwatch_alarm_last_success_timestamp_seconds` for provider health.
Failed reads retain diagnostic alarm evidence until a complete successful poll.

Add `prometheus-cloudwatch.yaml` **last** to your existing Prometheus Helm upgrade.
Use the example's pinned chart 29.27.0. Preserve your full existing overlay list,
including Alertmanager notification, token-mount and heartbeat overlays when
those features are enabled. For example, a source without those optional features:

```bash
helm --kube-context "$KUBE_CONTEXT" upgrade prometheus prometheus-community/prometheus \
  --version 29.27.0 -n "$OBS_NAMESPACE" \
  -f examples/sre-bot/observability/prometheus-values.yaml \
  -f examples/sre-bot/observability/prometheus-cloudwatch.yaml --wait
```

Helm replaces `rule_files` lists. The CloudWatch overlay carries all four chart
default entries, the `heartbeat_rules*.yml` pattern and its own separate rule file. With the
heartbeat disabled, that pattern has no match and loads no rule. With
heartbeat enabled, apply `alertmanager-heartbeat.yaml` before the CloudWatch
overlay so both files load. A later private overlay replacing `rule_files` must
retain these entries. The reliability groups in `alerting_rules.yml` remain
untouched. The default example installer uses its default overlays on each run;
reapply your complete opt-in overlay set after using it to upgrade Prometheus.

Render the exact overlay set and inspect `prometheus.yml` plus the rule ConfigMap
before upgrading. Afterward verify the actual Prometheus `/api/v1/targets`,
`/api/v1/rules` and `/api/v1/alerts`: the reader is scraped, both CloudWatch rules
are loaded, and default reliability and any enabled heartbeat rules remain.
`CurieCloudWatchAlarmsUnread` warns after 15 minutes without successful reads;
a healthy empty account stays quiet. Alarm descriptions are untrusted data.
A retained alarm and transition timestamp require reader health and a fresh exact
provider read or history before concluding current state. Notification delivery
requires your separately configured Alertmanager path and separate verification.

Code ConfigMaps are not regenerated from the checkout automatically. Recreate
`cloudwatch-alarms-code` from an updated checkout, then restart only
`deployment/cloudwatch-alarms`. Restart that deployment after changing startup
configuration too. Projected tokens rotate without a restart: each new STS assume
rereads the token file; credentials refresh when fewer than 300 seconds remain.
Role-policy and trust changes are operator operations. Keep credentials out of
logs and diagnostics. A successful listener, fixture suite or rendered chart
does not qualify live STS, AWS reads, cluster security or notification delivery.
