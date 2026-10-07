# Opt-in CloudWatch alarm reader

Feature: [#4253](https://github.com/curie-eng/curie/issues/4253).

The SRE example needs a read-only CloudWatch source that operators can enable
for an SNS topic they already own. The source is optional: the default example
install and its existing Prometheus, Alertmanager and heartbeat configuration
keep their behavior. IAM roles, topic ARNs, cluster names and notification routes
are installation inputs. This feature does not create AWS resources, publish to
SNS, grant Kubernetes mutations, or assert that an autonomous recovery path works.

The implementation lives in
`observability/cloudwatch-alarms/cloudwatch_alarms.py`; the optional deployment
is `observability/cloudwatch-alarms.yaml`. A separate Prometheus overlay,
`observability/prometheus-cloudwatch.yaml`, consumes its metrics. These are
example artifacts rather than a platform architectural decision.

## Acceptance criteria

- **SRE-CW-1 — opt-in inputs.** The program imports and runs with Python's
  standard library alone. `TOPIC_ARN`, `AWS_REGION`, `AWS_ROLE_ARN` and
  `AWS_WEB_IDENTITY_TOKEN_FILE` are required at startup; missing values exit 2
  and report only missing variable names. `LISTEN_ADDR` defaults to
  `0.0.0.0:9108`, `POLL_SECONDS` to 60 and `METRIC_PREFIX` to
  `curie_cloudwatch`. The prefix produces `<prefix>_alarm_in_alarm`,
  `<prefix>_alarm_poll_ok` and
  `<prefix>_alarm_last_success_timestamp_seconds`; an operator can retain an
  existing installation's metric names by explicitly setting its previous
  prefix. Accept only `[a-zA-Z_][a-zA-Z0-9_]*` prefixes and positive finite
  polling intervals. Invalid configuration exits 2 without opening a listener
  or requesting credentials. A refused listener bind exits 2 with a bounded
  configuration error and no traceback, exception text or installation values. The STS session name is `curie-cloudwatch-alarms`.

- **SRE-CW-2 — signed complete reads.** Read the projected web identity token
  from its file for every AssumeRoleWithWebIdentity request to regional STS.
  Cache temporary credentials until fewer than 300 seconds remain; then assume
  again and sign subsequent requests with the newly issued credentials.
  DescribeAlarms uses regional CloudWatch, StateValue ALARM, ActionPrefix equal
  to TOPIC_ARN, both MetricAlarm and CompositeAlarm, MaxRecords 100 and every
  returned NextToken, correctly form encoded. ActionPrefix is a provider-side
  candidate filter: publish only returned alarms whose StateValue is exactly
  ALARM and whose AlarmActions contains TOPIC_ARN as an exact member. A matching
  OKActions or InsufficientDataActions entry, or a similarly prefixed topic in
  AlarmActions, does not qualify. `parse_alarms(xml, topic_arn)` receives the
  configured topic explicitly. Requests use AWS SigV4, signing
  the exact body and session-token header. Only direct alarm members count;
  nested dimensions or metrics do not. ActionsEnabled false suppresses both
  kinds; absent descriptions become empty strings. Requests time out after
  20 seconds. Malformed XML, missing result or credential fields and naive
  credential expiry are failures, never an empty successful account.

- **SRE-CW-3 — last successful snapshot.** Before the first completed poll,
  export poll_ok 0, no alarms and no last-success sample. Publish alarms,
  poll_ok 1 and the clock's successful-completion timestamp atomically only
  after every page succeeds. Any failed STS or CloudWatch request, including a
  later page, retains the previous complete alarms and last-success timestamp
  while setting poll_ok 0. A later complete empty result removes prior alarms;
  a later successful populated result replaces them and restores poll_ok 1.

- **SRE-CW-4 — stable diagnostic evidence.** Prometheus text format 0.0.4
  exports one gauge of value 1 per distinct `(alarm, description,
  state_transitioned_at)` tuple, with HELP/TYPE and a final newline. Escape
  label quotes, backslashes and newlines; collapse description whitespace.
  Use StateTransitionedTimestamp as episode evidence; changes only to
  StateReason or StateUpdatedTimestamp do not create a new series. This
  timestamp does not establish current state or continuous service impact.
  A configured prefix changes only metric names, keeping tuple and values
  compatible with existing consumers.

- **SRE-CW-5 — credential redaction.** Failures log one line with the failed
  step (`assume` or `describe`) and HTTP status or exception class. Never log
  response bodies, exception messages, projected tokens, secret keys or
  session tokens, including when an error echoes a signed request. Temporary
  credential repr hides secret key and session token. Metrics carry diagnostic
  alarm data, never authentication material. SIGTERM exits 0 even during a poll.

- **SRE-CW-6 — executable metrics endpoint.** Serve the current snapshot on
  GET /metrics with status 200 and `text/plain; version=0.0.4; charset=utf-8`.
  Other paths return 404. Start serving before the first poll; failed polls do
  not stop serving. Idle connections have a bounded timeout of at most 30s.

- **SRE-CW-7 — optional consumers and failure signal.** The optional overlay
  loads a separate CloudWatch rule file while retaining the pinned Prometheus
  chart's default rule files and any enabled heartbeat rule file. It must not
  replace `alerting_rules.yml` or default reliability rule groups. The
  CurieCloudWatchAlarm rule aggregates by the diagnostic tuple, tests the
  default-prefix in_alarm gauge for 1 and uses keep_firing_for 3m to bridge a
  short reader restart; a successful empty poll eventually resolves it.
  CurieCloudWatchAlarmsUnread fires after 15m of absent or zero poll_ok. A
  healthy reader with zero alarms remains quiet. Descriptions identify alarm
  descriptions as untrusted data and require a fresh exact provider read or
  history plus reader health before concluding current state. The public
  consumer makes no assumption about a human SNS route or existing delivery.
  Operators changing METRIC_PREFIX also update consumer expressions. Publish
  executable promtool vectors for healthy empty, firing, failed read,
  missing reader, recovery, duplicate readers and short restart scenarios.
  The root-collected vector test uses PROMTOOL, an installed promtool, or the
  same actual evaluator in pinned Docker image prom/prometheus:v3.5.0; lack
  of an evaluator fails the check rather than skipping the vectors. The
  focused example workflow publishes a distinct job/check name and adds no
  root-CI dependency requirement.

- **SRE-CW-8 — neutral installation.** The optional manifest uses the pinned
  standard-library Python image already used by alert-signer, one replica,
  a read-only code ConfigMap, non-root identity, read-only filesystem,
  no privilege escalation, dropped capabilities and bounded resources.
  It references an operator-created `cloudwatch-alarms-config` ConfigMap for
  TOPIC_ARN and AWS_REGION. Operators provide the web-identity ServiceAccount
  and read-only role trust and permissions; no real role annotation or account
  appears upstream. IAM permissions are sts:AssumeRoleWithWebIdentity and
  cloudwatch:DescribeAlarms only. Installation docs include the required
  ConfigMap/code creation, manifest application, scrape/rule loading, prefix
  compatibility, overlays and credential rotation steps, keeping all actual
  installation values private. Installing the default SRE example starts no
  reader and adds no CloudWatch rule or AWS credential dependency.

## Verification boundary

Recorded 2026-10-07: a prior reader's standard-library behavior and its
isolated fake STS/CloudWatch campaign passed 21 tests. The campaign exercised
SigV4 vectors, both alarm kinds, pagination, action suppression, stable
transition identity, credential rotation, failed-page retention, redaction,
HTTP serving, stdlib import and SIGTERM. This is measured fixture behavior,
not a measurement of current AWS or a released upstream artifact. Its
independent signing vector used botocore 1.34.137 and the AWS SigV4 IAM example.
Provider contracts:
[DescribeAlarms](https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_DescribeAlarms.html),
[AssumeRoleWithWebIdentity](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html),
[SigV4 canonical requests](https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html).

The unit command is
`uv run pytest examples/tests/test_cloudwatch_alarms.py -q`.
The Prometheus command is `promtool test rules <rendered-vectors>` using the
rendered production rules and the pinned chart version. Render and execute
the optional manifest in an isolated cluster, scrape before and after a failed
poll and verify the actual Prometheus rule outputs. A renderer alone proves
configuration shape, not metrics or delivery.

Required implementation tiers: cluster (example manifest and its security
boundary), live provider (STS credential resolution/authentication), external
integration (read-only AWS API shape). Skill, local, local-release and factory
are not reached by this isolated example source. Unit fixtures cannot close
the required real tiers. Missing authorized credentials, provider access,
cluster or notification evidence remains an explicit blocker; a discovery
waiver must name an open issue. A read-only AWS proof uses existing
operator-provided permissions and compares a real empty or filtered read to a
refused request without publishing identities or credentials. No new alarm,
SNS publication, mutation, approval resolution or deployment is authorized by
these instructions.
