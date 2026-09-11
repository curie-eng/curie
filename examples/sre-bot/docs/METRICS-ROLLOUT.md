# Retained metrics overlay: rollout and rollback

This is the operator handoff for turning Curie application metrics and
reliability alerts on. It does not authorize unattended changes to the
permanent soak, credential rotation, timer changes, or notifications to
people. Public examples use placeholders such as `C0EXAMPLE1`.

## Evidence classes

Keep these three classes separate. A source-only completion cannot claim the
permanent overlay is active.

| Class | What it proves | What it does not prove |
| --- | --- | --- |
| Locally rendered | `helm template` of the shipped values plus overlay shows `prometheusremotewrite/soak` on the metrics pipeline, the remote-write receiver, 9101-only node-exporter scrape when that overlay is applied, and the alert groups. | Nothing about a live Collector or Prometheus. |
| Disposable runtime-tested | A task-owned install on a disposable namespace retains queryable Curie run, queue, completion-delivery, and RPC series, fires and recovers alerts, and treats absent data as a failure. | The permanent soak is unchanged. |
| Actually deployed | Helm revision, Collector ConfigMap, and Prometheus API reads on the named live release. | Source commits, render logs, or a disposable proof from another namespace. |

The 2026-09-06 soak read observed metrics export to `nop/metrics`, zero Curie
metric names, zero alert-rule groups, and two `node_memory_MemAvailable_bytes`
series (`:9100` and `:9101`). Re-check that live state before calling the
permanent overlay deployed.

## Rollout (operator)

1. Render only. Do not apply yet.

   ```bash
   helm template prometheus prometheus-community/prometheus \
     --namespace observability \
     -f examples/sre-bot/observability/prometheus-values.yaml
   helm template curie charts/curie \
     --namespace curie \
     -f examples/sre-bot/observability/curie-values.yaml
   ```

   If a soak overlay is in play, add it with a later `-f`. Reuse that overlay.
   Do not create a second exporter or a second scrape-isolation file.

2. Confirm the render contains all of:

   - `prometheusremotewrite/soak` under Collector exporters
   - that name on `service.pipelines.metrics.exporters` alongside `nop/metrics`
   - `--web.enable-remote-write-receiver` on Prometheus
   - the `curie-reliability` alert group
   - a single intended node-exporter port when the isolation overlay is applied

3. Apply only to a disposable release/namespace first. Record the Helm
   revision. Query Prometheus:

   ```text
   count by (__name__) ({__name__=~"curie_.*"})
   count by (instance) (node_memory_MemAvailable_bytes{curie_source="curie-sre-bot"})
   ```

   Expected: Curie run, queue, completion-delivery, and RPC names are present
   after a known consumer operation, and `node_memory_MemAvailable_bytes` has
   one series.

4. Break export on that disposable install (remove
   `extraMetricPipelineExporters` or block the receiver).
   `CurieApplicationMetricsAbsent` must fire. Restore the exporter; the alert
   must recover.

5. Permanent soak remains an operator step after that disposable proof. It is
   not performed by implementation tasks.

## Rollback

Roll Prometheus and the Curie release back to the prior Helm revision. Verify
the Collector metrics pipeline, Prometheus flags, and `/api/v1/rules` against
the evidence class you are claiming. Do not delete persistent volumes as part
of a routine rollback.

## Correlation without private bodies

High-cardinality identity belongs in logs and traces, not metric labels. See
the correlation recipe in `examples/sre-bot/README.md`. Do not inspect message
bodies or log credential values to close an alert.
