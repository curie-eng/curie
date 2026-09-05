import { useCallback, useEffect, useState } from "react";
import { C } from "../../tokens";
import { Button, Dot } from "../../primitives";
import { useStore } from "../../state/store";
import { getRunnerLogs, listRunnerPods, ApiError, type PodLogs } from "../../api/client";

type Result =
  | { kind: "idle" }
  | { kind: "loading" }
  | { kind: "ok"; data: PodLogs }
  | { kind: "no-cluster"; message: string }
  | { kind: "not-found"; message: string }
  | { kind: "cluster-error"; message: string }
  | { kind: "error"; message: string };

// The pod list backing the dropdown. "no-cluster" mirrors the logs 503 state so
// the dev box (no cluster) degrades honestly instead of an empty silent select.
type Pods =
  | { kind: "loading" }
  | { kind: "ok"; pods: string[] }
  | { kind: "no-cluster"; message: string }
  | { kind: "error"; message: string };

const ALL = ""; // the "All runner pods" sentinel (aggregate across the list)

const inputStyle = {
  background: C.input,
  border: "1px solid " + C.borderStrong,
  borderRadius: 8,
  padding: "8px 11px",
  color: C.text,
  fontFamily: C.mono,
  fontSize: 12.5,
} as const;

function StateBanner({ tone, title, body }: { tone: "warn" | "muted" | "error"; title: string; body: string }) {
  const color = tone === "warn" ? C.warn : tone === "error" ? C.failure : C.mutedStatus;
  const bg =
    tone === "warn" ? "rgba(191,135,0,.07)" : tone === "error" ? "rgba(207,34,41,.06)" : "rgba(139,148,158,.06)";
  const border =
    tone === "warn" ? "rgba(191,135,0,.3)" : tone === "error" ? "rgba(207,34,41,.28)" : C.border;
  return (
    <div
      data-testid="logs-state"
      style={{ padding: "18px 20px", background: bg, border: "1px solid " + border, borderRadius: 12, display: "flex", gap: 12, alignItems: "center" }}
    >
      <Dot color={color} size={9} />
      <div>
        <div style={{ fontSize: 14, fontWeight: 500, color: C.text, marginBottom: 2 }}>{title}</div>
        <div style={{ fontSize: 13, color: C.text2, fontFamily: C.mono }}>{body}</div>
      </div>
    </div>
  );
}

// Map an ApiError from a single-pod log read to a distinct Result state.
function resultFromError(e: unknown): Result {
  if (e instanceof ApiError) {
    if (e.status === 503) return { kind: "no-cluster", message: e.message };
    if (e.status === 404) return { kind: "not-found", message: e.message };
    if (e.status === 502) return { kind: "cluster-error", message: e.message };
    return { kind: "error", message: `${e.status}: ${e.message}` };
  }
  return { kind: "error", message: e instanceof Error ? e.message : String(e) };
}

// Cap on pod-log reads in flight at once. The cap exists to protect the API's
// Kubernetes proxy from a burst of concurrent reads, not because the client
// could not issue more.
const LOG_FETCH_CONCURRENCY = 6;

// Run `fn` over `items` with at most `limit` calls in flight. Results are
// returned in ITEM order regardless of completion order, and a rejection is
// captured as a settled result so one failing item never rejects the batch.
async function mapWithLimit<T, R>(
  items: T[],
  limit: number,
  fn: (item: T) => Promise<R>,
): Promise<PromiseSettledResult<R>[]> {
  if (items.length === 0) return [];
  const results = new Array<PromiseSettledResult<R>>(items.length);
  let cursor = 0;
  // A shared index cursor hands each worker the next unclaimed item, so a slow
  // item parks one worker instead of stalling the whole batch behind it.
  const worker = async () => {
    while (cursor < items.length) {
      const i = cursor++;
      try {
        results[i] = { status: "fulfilled", value: await fn(items[i]) };
      } catch (reason) {
        results[i] = { status: "rejected", reason };
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, () => worker()));
  return results;
}

// Wired Logs tab: pick a runner pod (or all of them) and tail its logs, proxying
// the K8s pod-logs API. The pod list comes from the runner-pods endpoint; the
// distinct cluster states are designed (503 no cluster, 404 pod not found, 502
// other). On the dev box (no cluster) the 503 degraded state is expected.
export function RealLogs() {
  // When the trace detail's "View sandbox logs" set a prefill, preselect it. The
  // sandbox_id doubles as the pod name: docker names the container after the
  // sandbox, and on k8s the sandbox pod name is ASSUMED equal (to be confirmed on
  // the cluster). Absent that identity, the prefill is a best-effort preselect.
  const { state } = useStore();
  const prefill = state.logsPod;
  const [namespace, setNamespace] = useState("curie");
  const [pods, setPods] = useState<Pods>({ kind: "loading" });
  const [selected, setSelected] = useState<string>(prefill ?? ALL);
  const [result, setResult] = useState<Result>({ kind: "idle" });

  const loadPods = useCallback(async (ns: string, keep: string = ALL) => {
    setPods({ kind: "loading" });
    setSelected(keep);
    setResult({ kind: "idle" });
    try {
      const data = await listRunnerPods(ns.trim() || undefined);
      setPods({ kind: "ok", pods: data.pods });
    } catch (e) {
      if (e instanceof ApiError && e.status === 503) {
        setPods({ kind: "no-cluster", message: e.message });
      } else {
        setPods({ kind: "error", message: e instanceof ApiError ? e.message : e instanceof Error ? e.message : String(e) });
      }
    }
  }, []);

  // Load pods on mount and whenever a new sandbox prefill arrives, keeping the
  // prefilled pod selected across the reload (a namespace change resets to ALL).
  useEffect(() => {
    void loadPods("curie", prefill ?? ALL);
  }, [loadPods, prefill]);

  // Aggregate logs across every listed pod, one labeled block per pod, reading
  // up to LOG_FETCH_CONCURRENCY pods at a time. A single pod short-circuits to
  // the plain per-pod result so its distinct error states (404/502/503) are
  // preserved; only the multi-pod "All" view concatenates.
  const fetchAll = async (ns: string, list: string[]) => {
    const settled = await mapWithLimit(list, LOG_FETCH_CONCURRENCY, (pod) =>
      getRunnerLogs(ns, pod, { tail_lines: 200 }),
    );
    // A cluster-wide failure (503) still aborts the whole aggregate, but because
    // the reads now overlap it is detected in the results rather than mid-loop:
    // the earliest 503 in list order stands in for the one the serial read would
    // have hit first.
    for (const outcome of settled) {
      if (outcome.status === "rejected" && outcome.reason instanceof ApiError && outcome.reason.status === 503) {
        setResult({ kind: "no-cluster", message: outcome.reason.message });
        return;
      }
    }
    // Per-pod failures (a 404 for a pod that has since gone) are noted inline so
    // one missing pod does not blank the others.
    const blocks = list.map((pod, i) => {
      const outcome = settled[i];
      if (outcome.status === "fulfilled") {
        const data = outcome.value;
        return `=== ${pod} ===\n${data.logs.trim() === "" ? "(no log lines)" : data.logs}`;
      }
      const e = outcome.reason;
      const msg = e instanceof ApiError ? `${e.status}: ${e.message}` : e instanceof Error ? e.message : String(e);
      return `=== ${pod} ===\n(could not fetch: ${msg})`;
    });
    setResult({
      kind: "ok",
      data: { namespace: ns, pod: `all runner pods (${list.length})`, container: null, logs: blocks.join("\n\n") },
    });
  };

  const fetchLogs = async () => {
    const ns = namespace.trim();
    if (pods.kind === "no-cluster") {
      setResult({ kind: "no-cluster", message: pods.message });
      return;
    }
    const list = pods.kind === "ok" ? pods.pods : [];
    setResult({ kind: "loading" });
    if (selected !== ALL) {
      try {
        const data = await getRunnerLogs(ns, selected, { tail_lines: 200 });
        setResult({ kind: "ok", data });
      } catch (e) {
        setResult(resultFromError(e));
      }
      return;
    }
    if (list.length === 0) {
      setResult({ kind: "error", message: "No runner pods to tail in this namespace." });
      return;
    }
    await fetchAll(ns, list);
  };

  const fetchedPods = pods.kind === "ok" ? pods.pods : [];
  // Keep the selected pod in the dropdown even when the fetched list doesn't
  // include it (a prefilled sandbox id, or a dev box with no cluster / empty
  // list), so the <select> value always has a matching <option>.
  const podOptions =
    selected !== ALL && !fetchedPods.includes(selected) ? [selected, ...fetchedPods] : fetchedPods;
  const canFetch = pods.kind !== "loading";

  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 14, flexWrap: "wrap" }}>
        <input
          aria-label="namespace"
          value={namespace}
          onChange={(e) => setNamespace(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") void loadPods(namespace);
          }}
          onBlur={() => void loadPods(namespace)}
          placeholder="namespace"
          style={{ ...inputStyle, width: 150 }}
        />
        <select
          data-testid="logs-pod-select"
          aria-label="pod"
          value={selected}
          onChange={(e) => setSelected(e.target.value)}
          style={{ ...inputStyle, flex: 1, minWidth: 200 }}
        >
          <option value={ALL}>{`All runner pods${fetchedPods.length ? ` (${fetchedPods.length})` : ""}`}</option>
          {podOptions.map((p) => (
            <option key={p} value={p}>
              {p}
            </option>
          ))}
        </select>
        <Button label="Fetch logs" variant="primary" onClick={() => void fetchLogs()} disabled={!canFetch} />
      </div>

      {pods.kind === "loading" ? (
        <div style={{ fontSize: 12, color: C.muted, marginBottom: 12, fontFamily: C.mono }}>Loading runner pods…</div>
      ) : pods.kind === "no-cluster" ? (
        <div data-testid="pods-note" style={{ fontSize: 12, color: C.warn, marginBottom: 12, fontFamily: C.mono }}>
          No cluster configured — pod list unavailable.
        </div>
      ) : pods.kind === "error" ? (
        <div data-testid="pods-note" style={{ fontSize: 12, color: C.failure, marginBottom: 12, fontFamily: C.mono }}>
          Could not list runner pods: {pods.message}
        </div>
      ) : pods.pods.length === 0 ? (
        <div data-testid="pods-note" style={{ fontSize: 12, color: C.muted, marginBottom: 12, fontFamily: C.mono }}>
          No runner pods found in {namespace || "curie"}.
        </div>
      ) : null}

      {result.kind === "idle" ? (
        <div style={{ padding: "40px 20px", textAlign: "center", color: C.muted, fontSize: 13 }}>
          Pick a runner pod (or all of them) and fetch to tail its logs. Logs proxy the serving sandbox pods.
        </div>
      ) : null}
      {result.kind === "loading" ? (
        <div style={{ padding: "40px 20px", textAlign: "center", color: C.muted, fontSize: 13 }}>Fetching pod logs…</div>
      ) : null}
      {result.kind === "no-cluster" ? (
        <StateBanner tone="warn" title="No cluster configured" body={result.message} />
      ) : null}
      {result.kind === "not-found" ? (
        <StateBanner tone="muted" title="Pod not found" body={result.message} />
      ) : null}
      {result.kind === "cluster-error" ? (
        <StateBanner tone="error" title="Cluster error" body={result.message} />
      ) : null}
      {result.kind === "error" ? <StateBanner tone="error" title="Could not fetch logs" body={result.message} /> : null}
      {result.kind === "ok" ? (
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8, fontFamily: C.mono, fontSize: 12, color: C.muted }}>
            <span style={{ color: C.text2 }}>
              {result.data.namespace}/{result.data.pod}
              {result.data.container ? ` · ${result.data.container}` : ""}
            </span>
            <span style={{ display: "flex", alignItems: "center", gap: 6, color: C.brand }}>
              <span style={{ width: 7, height: 7, borderRadius: "50%", background: C.brand }} /> live
            </span>
          </div>
          {result.data.logs.trim() === "" ? (
            <div style={{ padding: "30px 20px", textAlign: "center", color: C.muted, fontSize: 13 }}>Pod returned no log lines.</div>
          ) : (
            <pre
              data-testid="logs-output"
              style={{
                margin: 0,
                background: C.darkest,
                border: "1px solid " + C.border,
                borderRadius: 12,
                padding: "12px 14px",
                fontFamily: C.mono,
                fontSize: 12.5,
                lineHeight: 1.6,
                color: C.text2,
                whiteSpace: "pre-wrap",
                maxHeight: 460,
                overflow: "auto",
              }}
            >
              {result.data.logs}
            </pre>
          )}
        </div>
      ) : null}
    </div>
  );
}
