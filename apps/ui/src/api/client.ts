// Typed client for the B1/B2 API, reached through the same-origin /api proxy.
// Every call carries the X-API-Key header. Shapes mirror apps/api/openapi.json.

import { API_PREFIX, apiKey } from "./config";

// Open (unauthenticated) app config: the configurable org/workspace name.
export interface AppConfig {
  org_name: string;
}

// A channel-neutral binding: an agent binds one or more channels (ADR-0116),
// so the wire carries a list, ordered `(kind, address)` server-side. `kind`
// selects which address shape applies; `address` is the channel-kind identifier
// the worker resolves against. Non-Slack reply routing is supplied only on the
// write shape below.
export interface ChannelBinding {
  kind: string;
  address: string;
}

// Reply routing is accepted on writes but deliberately never returned by the
// API. Keeping the write shape separate prevents a refetch from being mistaken
// for a source of adapter credentials.
export interface ChannelBindingWrite extends ChannelBinding {
  endpoint?: string;
  adapter?: string;
}

// The worker resolves an agent's binding against `channel.address`, not a
// bare `slack_channel` column. This is the console's fast local check for the
// Slack kind; it is a soft check (warns, never blocks) because
// the authoritative gate lives server-side (apps/api schemas.py).
export const SLACK_ADDRESS_RE = /^[CDG][A-Z0-9]+$/;

export interface AgentOut {
  id: string;
  name: string;
  // One or more channel bindings (ADR-0116), ordered `(kind, address)`
  // server-side.
  channels: ChannelBinding[];
  // Per-agent model id, forwarded as CURIE_MODEL at boot (#254). null uses the
  // platform default model.
  model: string | null;
  created_at: string;
}

export interface VersionOut {
  id: string;
  agent_id: string;
  version_label: string;
  bundle_ref: string | null;
  bundle_sha256: string | null;
  created_by: string;
  created_at: string;
}

export interface BundleOut {
  version_id: string;
  bundle_ref: string;
  bundle_sha256: string;
  size_bytes: number;
}

// One issue from the frozen plugin_format validator, surfaced on a 422.
export interface BundleIssue {
  code: string;
  message: string;
  location: string;
}

export interface ObservationNode {
  id: string;
  type: string;
  name: string | null;
  model: string | null;
  startTime: string | null;
  usageDetails: Record<string, unknown> | null;
  children: ObservationNode[];
}

export interface TraceTree {
  trace: Record<string, unknown>;
  tree: ObservationNode[];
  // The serving sandbox id (curie.sandbox_id), hoisted server-side from the
  // trace/observation resource attributes; null when the trace predates it.
  sandbox_id: string | null;
}

// A raw Langfuse trace row (opaque; we read a few well-known fields defensively).
export type RawTrace = Record<string, unknown>;

// An eval case in the frozen eval-case format (#8/#259): an input prompt plus a
// deterministic grader. Returned by promoteTraceToEvalCase.
export interface GraderOut {
  kind: "exact" | "contains" | "regex" | "tool_called";
  expected: string;
  case_sensitive: boolean;
}
export interface EvalCaseOut {
  id: string;
  input: string;
  grader: GraderOut;
}

/** Thrown when the bundle validator rejects the archive (HTTP 422). */
export class BundleValidationError extends Error {
  issues: BundleIssue[];
  constructor(issues: BundleIssue[]) {
    super("bundle failed validation");
    this.name = "BundleValidationError";
    this.issues = issues;
  }
}

/** Thrown for any other non-2xx response. */
export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

function url(path: string): string {
  return `${API_PREFIX}${path}`;
}

function headers(extra?: Record<string, string>): Record<string, string> {
  return { "X-API-Key": apiKey(), ...extra };
}

async function jsonOrThrow<T>(resp: Response): Promise<T> {
  if (resp.ok) return (await resp.json()) as T;
  const body = await resp.json().catch(() => null);
  throw new ApiError(resp.status, describeError(body) ?? resp.statusText);
}

function describeError(body: unknown): string | null {
  if (body && typeof body === "object" && "detail" in body) {
    const detail = (body as { detail: unknown }).detail;
    if (typeof detail === "string") return detail;
    if (detail && typeof detail === "object" && "detail" in detail) {
      const inner = (detail as { detail: unknown }).detail;
      if (typeof inner === "string") return inner;
    }
    // FastAPI field-validation errors: detail is an array of {loc, msg, type}.
    if (Array.isArray(detail) && detail.length > 0) {
      const first = detail[0] as { loc?: unknown[]; msg?: unknown };
      const field = Array.isArray(first.loc) ? first.loc[first.loc.length - 1] : undefined;
      const msg = typeof first.msg === "string" ? first.msg : "invalid value";
      return field ? `${field}: ${msg}` : msg;
    }
  }
  return null;
}

export async function createAgent(input: {
  name: string;
  channel: ChannelBinding;
  // Optional per-agent model id (#254). Omit for the platform default.
  model?: string;
}): Promise<AgentOut> {
  const resp = await fetch(url("/agents"), {
    method: "POST",
    headers: headers({ "Content-Type": "application/json" }),
    body: JSON.stringify(input),
  });
  return jsonOrThrow<AgentOut>(resp);
}

export async function createVersion(
  agentId: string,
  input: { version_label: string; created_by: string },
): Promise<VersionOut> {
  const resp = await fetch(url(`/agents/${agentId}/versions`), {
    method: "POST",
    headers: headers({ "Content-Type": "application/json" }),
    body: JSON.stringify(input),
  });
  return jsonOrThrow<VersionOut>(resp);
}

/**
 * PUT the bundle archive as multipart/form-data. On 422 the plugin validator's
 * issues are extracted and thrown as BundleValidationError so the editor can
 * render them inline; any other failure throws ApiError.
 */
export async function uploadBundle(
  agentId: string,
  versionId: string,
  archive: Blob,
): Promise<BundleOut> {
  const form = new FormData();
  form.append("file", archive, "bundle.zip");
  const resp = await fetch(url(`/agents/${agentId}/versions/${versionId}/bundle`), {
    method: "PUT",
    headers: headers(),
    body: form,
  });
  if (resp.ok) return (await resp.json()) as BundleOut;
  const body = await resp.json().catch(() => null);
  const issues = extractIssues(body);
  if (resp.status === 422 && issues) throw new BundleValidationError(issues);
  throw new ApiError(resp.status, describeError(body) ?? resp.statusText);
}

// The bundle 422 body is { detail: { detail: "...", errors: [ {code,message,location} ] } }.
function extractIssues(body: unknown): BundleIssue[] | null {
  if (!body || typeof body !== "object" || !("detail" in body)) return null;
  const detail = (body as { detail: unknown }).detail;
  if (!detail || typeof detail !== "object" || !("errors" in detail)) return null;
  const errors = (detail as { errors: unknown }).errors;
  if (!Array.isArray(errors)) return null;
  return errors.map((e) => {
    const o = (e ?? {}) as Record<string, unknown>;
    return {
      code: String(o.code ?? "unknown"),
      message: String(o.message ?? ""),
      location: String(o.location ?? ""),
    };
  });
}

// List recent traces. With agentId, the API filters to that agent's runs (its
// traces carry the `agent-<id>` name token); without it, all recent traces.
export async function listTraces(limit = 20, agentId?: string): Promise<RawTrace[]> {
  const resp = await fetch(url(`/langfuse/traces${query({ limit, agent_id: agentId })}`), {
    headers: headers(),
  });
  return jsonOrThrow<RawTrace[]>(resp);
}

export async function getTrace(traceId: string): Promise<TraceTree> {
  const resp = await fetch(url(`/langfuse/traces/${encodeURIComponent(traceId)}`), {
    headers: headers(),
  });
  return jsonOrThrow<TraceTree>(resp);
}

// Promote a trace into an anonymized, runnable eval case (#259). The API reads
// the trace, scrubs PII, and returns a case in the frozen eval-case format.
export async function promoteTraceToEvalCase(traceId: string): Promise<EvalCaseOut> {
  const resp = await fetch(url(`/langfuse/traces/${encodeURIComponent(traceId)}/eval-case`), {
    method: "POST",
    headers: headers(),
  });
  return jsonOrThrow<EvalCaseOut>(resp);
}

// ---- K1: the eval matrix — cases × versions grid + per-model rollup ----

// One cell of the eval matrix: a case's outcome on a version column.
// `plumbing_ok` means the case ran to completion but no grader judged it (the
// fake-model tier); it is neither a pass nor a fail and must never read green.
export type EvalStatus = "pass" | "fail" | "plumbing_ok" | "missing";

export interface EvalCell {
  version: string;
  status: EvalStatus;
  // The model the result was produced under, or null when the run was unlabelled.
  model: string | null;
}

export interface EvalMatrixRow {
  case_id: string;
  cells: EvalCell[];
}

// A per-model rollup across the suite. `passed`/`total` exclude non-graded
// (plumbing) rows, counted separately in `plumbing`; `completed` (⊆ `total`) is
// the graded rows whose turn actually reached a verdict, so `total > 0` with
// `completed === 0` is a model that never answered — distinct from a real 0%.
export interface EvalModelSummary {
  model: string | null;
  passed: number;
  total: number;
  cost_usd: number | null;
  plumbing: number;
  completed: number;
}

export interface EvalMatrix {
  suite: string;
  // Version columns, most-recently-exercised first, capped at the requested N.
  versions: string[];
  cases: string[];
  rows: EvalMatrixRow[];
  models: (string | null)[];
  model_summaries: EvalModelSummary[];
}

// Read the eval matrix for a suite. The matrix is filtered by suite (the real
// dimension on eval traces); `versions` caps the number of version columns.
export async function getEvalMatrix(suite: string, versions = 5): Promise<EvalMatrix> {
  const resp = await fetch(url(`/evals/matrix${query({ suite, versions })}`), {
    headers: headers(),
  });
  return jsonOrThrow<EvalMatrix>(resp);
}

// ---- observability (OB1): Langfuse-backed metrics + runner-pod log proxy ----

export type MetricKey = "runs" | "latency_p95_ms" | "tokens" | "cost_usd" | "error_rate";
export type Granularity = "hour" | "day" | "week";

export interface MetricsSummary {
  start: string;
  end: string;
  runs: number;
  latency_p95_ms: number;
  tokens: number;
  cost_usd: number;
  error_rate: number;
}

export interface MetricPoint {
  ts: string;
  value: number;
}

export interface MetricSeries {
  metric: string;
  granularity: string;
  start: string;
  end: string;
  points: MetricPoint[];
}

export interface PodLogs {
  namespace: string;
  pod: string;
  container: string | null;
  logs: string;
}

export interface RunnerPods {
  namespace: string;
  pods: string[];
}

// List the runner sandbox pods in a namespace (populates the Logs dropdown).
// Non-2xx throws ApiError carrying the status: 503 (no cluster), 502 (other).
export async function listRunnerPods(namespace?: string): Promise<RunnerPods> {
  const resp = await fetch(url(`/observability/runners${query({ namespace })}`), {
    headers: headers(),
  });
  return jsonOrThrow<RunnerPods>(resp);
}

// The per-agent filter is a trace-name substring server-side, so it is passed as
// a plain `agent` query param, not a promise of exact matching.
export interface MetricFilter {
  environment?: string;
  agent?: string;
}

function query(params: Record<string, string | number | boolean | undefined>): string {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== "") q.set(k, String(v));
  }
  const s = q.toString();
  return s ? `?${s}` : "";
}

export async function getMetricsSummary(filter: MetricFilter = {}): Promise<MetricsSummary> {
  const resp = await fetch(url(`/observability/metrics/summary${query({ ...filter })}`), {
    headers: headers(),
  });
  return jsonOrThrow<MetricsSummary>(resp);
}

export async function getMetricSeries(
  metric: MetricKey,
  granularity: Granularity,
  filter: MetricFilter = {},
): Promise<MetricSeries> {
  const resp = await fetch(
    url(`/observability/metrics/series${query({ metric, granularity, ...filter })}`),
    { headers: headers() },
  );
  return jsonOrThrow<MetricSeries>(resp);
}

export interface RunnerLogsQuery {
  container?: string;
  tail_lines?: number;
  previous?: boolean;
}

// Fetch runner-pod logs. Non-2xx throws ApiError carrying the status so the view
// can render distinct states: 503 (no cluster), 404 (missing pod), 502 (other).
export async function getRunnerLogs(
  namespace: string,
  pod: string,
  opts: RunnerLogsQuery = {},
): Promise<PodLogs> {
  const resp = await fetch(
    url(
      `/observability/runners/${encodeURIComponent(namespace)}/${encodeURIComponent(pod)}/logs${query({ ...opts })}`,
    ),
    { headers: headers() },
  );
  return jsonOrThrow<PodLogs>(resp);
}

// ---- L1: per-agent cost, budget, and the kill switch ----

export interface CostReport {
  start: string;
  end: string;
  total_usd: number;
  points: MetricPoint[];
}

// Both fields nullable; null means platform defaults. Values must be > 0.
export interface BudgetConfig {
  max_usd_per_day: number | null;
  max_output_tokens_per_run: number | null;
}

export interface KillState {
  killed: boolean;
}

// The forced-thread-reset state (#737/#735). `requested` is true from the moment
// the POST enqueues a reset until the worker's maintenance tick has actually
// released the thread's sandbox, then false. Operators poll it to know the
// release landed (mirrors the CLI reset-thread verb's wait loop).
export interface ThreadResetState {
  requested: boolean;
}

export async function getAgents(): Promise<AgentOut[]> {
  const resp = await fetch(url("/agents"), { headers: headers() });
  return jsonOrThrow<AgentOut[]>(resp);
}

// The open /config endpoint (no API key required) carries the configurable
// org/workspace name the shared chrome renders.
export async function getConfig(): Promise<AppConfig> {
  const resp = await fetch(url("/config"));
  return jsonOrThrow<AppConfig>(resp);
}

// PATCH an agent's mutable fields. Returns the updated agent. Mirrors
// createAgent's JSON-body shape; non-2xx throws ApiError. The live worker keeps
// its config until the next deploy; this only updates the stored config the
// next deployment reads.
//
// `model` is a THREE-way field (#1310, #1355): omitted leaves it unchanged,
// explicit null clears the pin back to the platform default, and a string pins
// it. `null` has to be in the type or the console cannot express the clear at
// all: it used to send `""`, which the API now refuses, because an empty
// override reaches the worker falsy, emits no boot key, and skips the very
// platform default that clearing is supposed to restore.
export async function updateAgent(
  agentId: string,
  patch: { model?: string | null },
): Promise<AgentOut> {
  const resp = await fetch(url(`/agents/${agentId}`), {
    method: "PATCH",
    headers: headers({ "Content-Type": "application/json" }),
    body: JSON.stringify(patch),
  });
  return jsonOrThrow<AgentOut>(resp);
}

// Move one surface binding to a new kind/address (ADR-0116). The pair being
// moved is named by `selector` (its CURRENT kind/address) and rides in the
// query string, never the body, so it can never be confused with `next`, the
// replacement value. Reply route fields are write only and omitted here, which
// tells the API to preserve them. Returns the updated agent.
export async function patchAgentChannel(
  agentId: string,
  selector: ChannelBinding,
  next: ChannelBinding,
): Promise<AgentOut> {
  const resp = await fetch(
    url(`/agents/${agentId}/channels${query({ kind: selector.kind, address: selector.address })}`),
    {
      method: "PATCH",
      headers: headers({ "Content-Type": "application/json" }),
      body: JSON.stringify(next),
    },
  );
  return jsonOrThrow<AgentOut>(resp);
}

export async function addAgentSurface(
  agentId: string,
  surface: ChannelBindingWrite,
): Promise<AgentOut> {
  const resp = await fetch(url(`/agents/${agentId}/channels`), {
    method: "POST",
    headers: headers({ "Content-Type": "application/json" }),
    body: JSON.stringify(surface),
  });
  return jsonOrThrow<AgentOut>(resp);
}

export async function removeAgentSurface(
  agentId: string,
  surface: ChannelBinding,
): Promise<void> {
  const resp = await fetch(
    url(`/agents/${agentId}/channels${query({ kind: surface.kind, address: surface.address })}`),
    { method: "DELETE", headers: headers() },
  );
  if (resp.ok) return;
  const body = await resp.json().catch(() => null);
  throw new ApiError(resp.status, describeError(body) ?? resp.statusText);
}

// Delete an agent (cascades its versions/deployments server-side; 204 No Content
// on success). A 409 (active deployment) surfaces via the thrown ApiError.
export async function deleteAgent(agentId: string): Promise<void> {
  const resp = await fetch(url(`/agents/${agentId}`), { method: "DELETE", headers: headers() });
  if (resp.ok) return;
  const body = await resp.json().catch(() => null);
  throw new ApiError(resp.status, describeError(body) ?? resp.statusText);
}

export async function getCost(agentId: string, range: { start?: string; end?: string } = {}): Promise<CostReport> {
  const resp = await fetch(url(`/agents/${agentId}/cost${query({ ...range })}`), { headers: headers() });
  return jsonOrThrow<CostReport>(resp);
}

export async function getBudget(agentId: string): Promise<BudgetConfig> {
  const resp = await fetch(url(`/agents/${agentId}/budget`), { headers: headers() });
  return jsonOrThrow<BudgetConfig>(resp);
}

// PUT the budget. A non-positive value 422s server-side (Field(gt=0)); the
// ApiError message carries the field-level reason for inline display.
export async function putBudget(agentId: string, budget: BudgetConfig): Promise<BudgetConfig> {
  const resp = await fetch(url(`/agents/${agentId}/budget`), {
    method: "PUT",
    headers: headers({ "Content-Type": "application/json" }),
    body: JSON.stringify(budget),
  });
  return jsonOrThrow<BudgetConfig>(resp);
}

export async function getKillState(agentId: string): Promise<KillState> {
  const resp = await fetch(url(`/agents/${agentId}/kill`), { headers: headers() });
  return jsonOrThrow<KillState>(resp);
}

// ---- FX2: agent detail — versions, bundle files, and version activation ----

export type Environment = "prod" | "dev";

export interface DeploymentOut {
  id: string;
  agent_id: string;
  version_id: string;
  environment: Environment;
  commit_sha: string | null;
  status: string;
  deployed_at: string;
}

// One unwrapped file from a stored bundle. `path` is bundle-root-relative, e.g.
// "skills/deal-desk/SKILL.md" or ".claude-plugin/plugin.json".
export interface BundleFile {
  path: string;
  content: string;
}

export interface BundleFiles {
  files: BundleFile[];
}

export async function listVersions(agentId: string): Promise<VersionOut[]> {
  const resp = await fetch(url(`/agents/${agentId}/versions`), { headers: headers() });
  return jsonOrThrow<VersionOut[]>(resp);
}

export async function listDeployments(agentId: string): Promise<DeploymentOut[]> {
  const resp = await fetch(url(`/deployments${query({ agent_id: agentId })}`), { headers: headers() });
  return jsonOrThrow<DeploymentOut[]>(resp);
}

// Every deployment across all agents (GET /deployments with no agent_id). Used
// by the env-scoped Agents/Overview views to decide which agents are live in the
// selected environment.
export async function listAllDeployments(): Promise<DeploymentOut[]> {
  const resp = await fetch(url("/deployments"), { headers: headers() });
  return jsonOrThrow<DeploymentOut[]>(resp);
}

/**
 * Read the unwrapped files of a version's stored bundle (FX2 headline: the agent
 * detail surface renders and edits these). A 404 means no bundle is stored for
 * the version yet; callers distinguish it via the thrown ApiError's status.
 */
export async function getVersionFiles(agentId: string, versionId: string): Promise<BundleFiles> {
  const resp = await fetch(url(`/agents/${agentId}/versions/${versionId}/files`), { headers: headers() });
  return jsonOrThrow<BundleFiles>(resp);
}

export type DeploymentStatus = "active" | "inactive";

export interface DeploymentCreate {
  agent_id: string;
  version_id: string;
  environment: Environment;
  // Optional; the API's DeploymentCreate defaults to "active" server-side. The
  // rollback path sets it explicitly to redeploy an old version as active.
  status?: DeploymentStatus;
}

// Activate a version by creating a deployment for it (status defaults to active
// server-side). This is the third step of the redeploy sequence, after POST
// version + PUT bundle.
export async function createDeployment(input: DeploymentCreate): Promise<DeploymentOut> {
  const resp = await fetch(url("/deployments"), {
    method: "POST",
    headers: headers({ "Content-Type": "application/json" }),
    body: JSON.stringify(input),
  });
  return jsonOrThrow<DeploymentOut>(resp);
}

// ---- Agent memory: inspect / edit / delete learned memory (#267) ----

// Where a memory entry was learned from (#264 Provenance shape). Provenance is
// the differentiator: an operator can see which session/traces taught a lesson.
export interface MemoryProvenance {
  learned_from_session_id: string | null;
  source_trace_ids: string[];
  recorded_at: string;
}

// One learned memory entry. `index` is its position in the append only memory
// log. It is valid only with the accompanying parent log version.
export interface MemoryEntry {
  index: number;
  version: number;
  content: string;
  provenance: MemoryProvenance;
}

// List an agent's learned memory, oldest first (empty for a fresh agent).
export async function listMemory(agentId: string): Promise<MemoryEntry[]> {
  const resp = await fetch(url(`/agents/${agentId}/memory`), { headers: headers() });
  return jsonOrThrow<MemoryEntry[]>(resp);
}

// Edit one entry's content in place; the server preserves its provenance. The
// change is reflected at the agent's next session boot (it rehydrates the log).
export async function editMemory(
  agentId: string,
  index: number,
  content: string,
  expectedVersion: number,
): Promise<MemoryEntry> {
  const resp = await fetch(url(`/agents/${agentId}/memory/${index}`), {
    method: "PUT",
    headers: headers({ "Content-Type": "application/json" }),
    body: JSON.stringify({ content, expected_version: expectedVersion }),
  });
  return jsonOrThrow<MemoryEntry>(resp);
}

// Delete exactly one memory entry (204 on success). Remaining entries keep order.
export async function deleteMemory(
  agentId: string,
  index: number,
  expectedVersion: number,
): Promise<void> {
  const resp = await fetch(
    url(`/agents/${agentId}/memory/${index}${query({ expected_version: expectedVersion })}`),
    {
      method: "DELETE",
      headers: headers(),
    },
  );
  if (resp.ok) return;
  const body = await resp.json().catch(() => null);
  throw new ApiError(resp.status, describeError(body) ?? resp.statusText);
}

// ---- Durable state store: operator read/inspect surface (#250) ----

// One namespace in an agent's durable state store, summarized for the inspector:
// how many keys it holds and when it was last written.
export interface StateNamespace {
  namespace: string;
  key_count: number;
  last_updated: string;
}

// A single durable state entry (a namespace+key holding arbitrary JSON), with the
// compare-and-set version and the last write time.
export interface StateEntry {
  namespace: string;
  key: string;
  value: unknown;
  version: number;
  updated_at: string;
}

// List the namespaces an agent has stored, most-recently-written first (empty
// for an agent that has stored nothing).
export async function listStateNamespaces(agentId: string): Promise<StateNamespace[]> {
  const resp = await fetch(url(`/agents/${agentId}/state`), { headers: headers() });
  return jsonOrThrow<StateNamespace[]>(resp);
}

// List every key stored under one namespace (get-by-key is not needed for the
// read surface: the list carries each entry's value, version, and write time).
export async function listStateEntries(agentId: string, namespace: string): Promise<StateEntry[]> {
  const resp = await fetch(url(`/agents/${agentId}/state/${encodeURIComponent(namespace)}`), {
    headers: headers(),
  });
  return jsonOrThrow<StateEntry[]>(resp);
}

export async function killAgent(agentId: string): Promise<KillState> {
  const resp = await fetch(url(`/agents/${agentId}/kill`), { method: "POST", headers: headers() });
  return jsonOrThrow<KillState>(resp);
}

export async function resumeAgent(agentId: string): Promise<KillState> {
  const resp = await fetch(url(`/agents/${agentId}/resume`), { method: "POST", headers: headers() });
  return jsonOrThrow<KillState>(resp);
}

// Force a thread's sandbox to be released (#737): the worker's next maintenance
// tick deletes the thread's claim/route, so the next message cold-creates a
// fresh sandbox. Interrupts a live turn on the thread; does not delete
// conversation history. `agent_id` scopes the action; the release is
// thread-keyed. The POST only *queues* the release — poll getThreadResetState to
// confirm it landed. The thread key is arbitrary (e.g. a Slack thread ts), so it
// is URL-encoded into the path.
export async function resetThread(agentId: string, threadKey: string): Promise<ThreadResetState> {
  const resp = await fetch(
    url(`/agents/${agentId}/threads/${encodeURIComponent(threadKey)}/reset`),
    { method: "POST", headers: headers() },
  );
  return jsonOrThrow<ThreadResetState>(resp);
}

// Poll whether a forced reset (the POST above) is still outstanding for this
// thread (#735). Stays true across the whole release and flips to false only
// once the sandbox has actually been released.
export async function getThreadResetState(
  agentId: string,
  threadKey: string,
): Promise<ThreadResetState> {
  const resp = await fetch(
    url(`/agents/${agentId}/threads/${encodeURIComponent(threadKey)}/reset`),
    { headers: headers() },
  );
  return jsonOrThrow<ThreadResetState>(resp);
}

// ---- Durable approvals: operator visibility + resolve-once (#867, ADR-0010) ----

// A durable approval record (mirrors ApprovalOut). The worker creates one when a
// run pauses on a permission/policy gate and suspends the session; an operator
// resolves it here or from the Slack card. `status` is one of
// pending/approved/rejected/expired.
export interface ApprovalOut {
  id: string;
  agent_id: string | null;
  conversation_id: string;
  author: string;
  summary: string;
  reply_channel: string;
  reply_placeholder: string | null;
  reply_endpoint: string | null;
  dedupe_key: string;
  route: string | null;
  card_channel: string | null;
  // Gate provenance (#544): which gate fired, and the tool a grant is bound to.
  gate_kind: string | null;
  granted_tool: string | null;
  status: string;
  expires_at: string | null;
  resolved_by: string | null;
  resolution_note: string | null;
  created_at: string;
  resolved_at: string | null;
}

// One audit-trail entry for an approval (mirrors ApprovalAuditOut, #247): each
// resolution attempt with the authorizer snapshot that counted or refused it.
export interface ApprovalAudit {
  id: string;
  approval_id: string;
  action: string;
  actor: string;
  actor_channel: string | null;
  decision: string;
  authorizer: string;
  authorized: boolean;
  reason: string | null;
  evidence: Record<string, unknown> | null;
  created_at: string;
}

export interface ApprovalListQuery {
  // Passed as the `status_filter` query param; omit for all statuses.
  status?: string;
  agentId?: string;
  conversationId?: string;
  limit?: number;
}

// List approvals, newest first (server clamps limit to 1..200). Without a status
// filter, every status is returned; the operator surface defaults to pending.
export async function listApprovals(opts: ApprovalListQuery = {}): Promise<ApprovalOut[]> {
  const resp = await fetch(
    url(
      `/approvals${query({
        status_filter: opts.status,
        agent_id: opts.agentId,
        conversation_id: opts.conversationId,
        limit: opts.limit,
      })}`,
    ),
    { headers: headers() },
  );
  return jsonOrThrow<ApprovalOut[]>(resp);
}

export async function getApproval(approvalId: string): Promise<ApprovalOut> {
  const resp = await fetch(url(`/approvals/${encodeURIComponent(approvalId)}`), { headers: headers() });
  return jsonOrThrow<ApprovalOut>(resp);
}

// The approval's audit trail, oldest first (empty until the first resolution
// attempt). A 404 (approval gone) surfaces via the thrown ApiError.
export async function getApprovalAudit(approvalId: string): Promise<ApprovalAudit[]> {
  const resp = await fetch(url(`/approvals/${encodeURIComponent(approvalId)}/audit`), { headers: headers() });
  return jsonOrThrow<ApprovalAudit[]>(resp);
}

export interface ApprovalResolveInput {
  decision: "approved" | "rejected";
  // Who is resolving (server requires non-empty); the authorizer blocks
  // self-approval against the record's author.
  resolved_by: string;
  note?: string;
  // The channel the resolution is asserted from; an API-key operator asserts it
  // explicitly so the channel-membership authorizer can count them.
  actor_channel?: string;
}

// Resolve an approval (resolve-once compare-and-set). The server-side authorizer
// runs first; distinct failure statuses carried on the thrown ApiError: 403
// (not authorized / self-approval), 409 (already resolved), 410 (expired).
export async function resolveApproval(approvalId: string, input: ApprovalResolveInput): Promise<ApprovalOut> {
  const resp = await fetch(url(`/approvals/${encodeURIComponent(approvalId)}/resolve`), {
    method: "POST",
    headers: headers({ "Content-Type": "application/json" }),
    body: JSON.stringify(input),
  });
  return jsonOrThrow<ApprovalOut>(resp);
}

// ---- Behavior packs: per-agent opt-in deterministic behaviors (#870) ----
//
// Shapes mirror apps/api schemas.BehaviorPacksConfig verbatim. The packs ride on
// the agent's stored config (like BudgetConfig mirrors the ACI Budget), so the
// shape is duplicated here rather than shared. A NULL agent row reads back as the
// all-off default; the API always returns the fully-defaulted object.

export interface LoadPack {
  enabled: boolean;
  // Rotating "working…" load lines shown while the agent is thinking.
  lines: string[];
}

export interface TipsPack {
  enabled: boolean;
  // Rotating capability tips (what the agent CAN do, vs. what it is doing now).
  tips: string[];
}

export interface GreetingPack {
  enabled: boolean;
  // Trigger phrases that short-circuit to the deterministic reply below.
  phrases: string[];
  reply: string;
}

export interface HelpPack {
  enabled: boolean;
  // Trigger phrases (e.g. "what can you do") that short-circuit to the reply.
  phrases: string[];
  reply: string;
}

// One declared user-editable runtime knob. The settings pack is schema-only today
// (the override store + per-user edit UI are a deferred runtime), so the console
// surfaces the declared knobs read-only and round-trips them unchanged.
export interface SettingConfig {
  key: string;
  label: string;
  kind: string;
  default: string;
  help: string;
  choices: string[];
  applies_live: boolean;
}

export interface SettingsPack {
  enabled: boolean;
  settings: SettingConfig[];
}

export interface NavPack {
  enabled: boolean;
  // The no-dead-ends hub button label + command for this agent.
  hub_label: string;
  hub_command: string;
}

export interface BehaviorPacksConfig {
  load: LoadPack;
  tips: TipsPack;
  greeting: GreetingPack;
  help: HelpPack;
  settings: SettingsPack;
  nav: NavPack;
}

export async function getBehaviorPacks(agentId: string): Promise<BehaviorPacksConfig> {
  const resp = await fetch(url(`/agents/${agentId}/behavior-packs`), { headers: headers() });
  return jsonOrThrow<BehaviorPacksConfig>(resp);
}

// PUT the full behavior-packs config (the API validates + persists the whole
// object; there is no partial patch). A validation failure 422s server-side and
// the ApiError message carries the field-level reason for inline display. The
// change is read by the worker at the agent's next bind, not mid-turn.
export async function putBehaviorPacks(
  agentId: string,
  config: BehaviorPacksConfig,
): Promise<BehaviorPacksConfig> {
  const resp = await fetch(url(`/agents/${agentId}/behavior-packs`), {
    method: "PUT",
    headers: headers({ "Content-Type": "application/json" }),
    body: JSON.stringify(config),
  });
  return jsonOrThrow<BehaviorPacksConfig>(resp);
}
