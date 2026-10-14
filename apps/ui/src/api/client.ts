// Typed client for the B1/B2 API, reached through the same-origin /api proxy.
// Every call authenticates with the same-origin HttpOnly console session cookie
// (ADR-0083). API wire shapes are aliases generated from apps/api/openapi.json
// by `pnpm gen:api-types`; UI helpers and undeclared error shapes stay local.

import { API_PREFIX } from "./config";
import type { components } from "./generated";

type Schemas = components["schemas"];

// Open (unauthenticated) app config: the configurable org/workspace name.
export type AppConfig = Schemas["AppConfig"];

// A channel-neutral binding: an agent binds one or more channels (ADR-0118),
// so the wire carries a list, ordered `(kind, address)` server-side. `kind`
// selects which address shape applies; `address` is the channel-kind
// identifier the worker resolves against; `adapter` names the bot identity the
// route answers as (ADR-0168 decision 3) -- a Slack binding with no stored
// identity reads back "default", and a non-Slack binding reads its adapter,
// or null. Non-Slack reply routing is supplied only on the write shape below.
export type ChannelBinding = Schemas["ChannelBindingOut"];

// Reply routing is accepted on writes but deliberately never returned by the
// API. Keeping the write shape separate prevents a refetch from being mistaken
// for a source of adapter credentials: the identity (`adapter`) comes back on
// every read; the endpoint never does.
export type ChannelBindingWrite = Schemas["ChannelBindingWrite"];

// The worker resolves an agent's binding against `channel.address`, not a
// bare `slack_channel` column. This is the console's fast local check for the
// Slack kind; it is a soft check (warns, never blocks) because
// the authoritative gate lives in apps/api/src/curie_api/schemas/channels.py.
export const SLACK_ADDRESS_RE = /^[CDG][A-Z0-9]+$/;

export type AgentOut = Schemas["AgentOut"];
export type AgentCreate = Schemas["AgentCreate"];
export type AgentUpdate = Schemas["AgentUpdate"];

export type VersionOut = Schemas["VersionOut"];
export type VersionCreate = Schemas["VersionCreate"];

export type BundleOut = Schemas["BundleOut"];

// One issue from the frozen plugin_format validator, surfaced on a 422.
export interface BundleIssue {
  code: string;
  message: string;
  location: string;
}

export type ObservationNode = Schemas["ObservationNode"];

export type TraceTree = Schemas["TraceTree"];

// A raw Langfuse trace row (opaque; we read a few well-known fields defensively).
export type RawTrace = Record<string, unknown>;

// An eval case in the frozen eval-case format (#8/#259): an input prompt plus a
// deterministic grader. Returned by promoteTraceToEvalCase.
export type GraderOut = Schemas["GraderOut"];
export type EvalCaseOut = Schemas["EvalCaseOut"];

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

/** True for a 401 from the API: no credential the route accepts. */
export function isUnauthorized(e: unknown): boolean {
  return e instanceof ApiError && e.status === 401;
}

function url(path: string): string {
  return `${API_PREFIX}${path}`;
}

type UnauthorizedListener = () => void;

const unauthorizedListeners = new Set<UnauthorizedListener>();

// Subscribe to 401s from any protected call. The console session gate uses this
// to re-check the session; returns the unsubscribe.
export function onUnauthorized(listener: UnauthorizedListener): () => void {
  unauthorizedListeners.add(listener);
  return () => {
    unauthorizedListeners.delete(listener);
  };
}

// The one wrapper every protected call goes through: the session cookie is the
// only credential, and a 401 notifies the listeners above. It neither throws on
// a status nor reads the body; callers keep their own error mapping.
async function request(path: string, init: RequestInit = {}): Promise<Response> {
  const resp = await fetch(url(path), { ...init, credentials: "same-origin" });
  if (resp.status === 401) {
    for (const listener of [...unauthorizedListeners]) listener();
  }
  return resp;
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

// `model` is optional (#254); omit it for the platform default.
export async function createAgent(input: AgentCreate): Promise<AgentOut> {
  const resp = await request("/agents", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });
  return jsonOrThrow<AgentOut>(resp);
}

export async function createVersion(
  agentId: string,
  input: VersionCreate,
): Promise<VersionOut> {
  const resp = await request(`/agents/${agentId}/versions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
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
  const resp = await request(`/agents/${agentId}/versions/${versionId}/bundle`, {
    method: "PUT",
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
  const resp = await request(`/langfuse/traces${query({ limit, agent_id: agentId })}`);
  return jsonOrThrow<RawTrace[]>(resp);
}

export async function getTrace(traceId: string): Promise<TraceTree> {
  const resp = await request(`/langfuse/traces/${encodeURIComponent(traceId)}`);
  return jsonOrThrow<TraceTree>(resp);
}

// Promote a trace into an anonymized, runnable eval case (#259). The API reads
// the trace, scrubs PII, and returns a case in the frozen eval-case format.
export async function promoteTraceToEvalCase(traceId: string): Promise<EvalCaseOut> {
  const resp = await request(`/langfuse/traces/${encodeURIComponent(traceId)}/eval-case`, {
    method: "POST",
  });
  return jsonOrThrow<EvalCaseOut>(resp);
}

// ---- K1: the eval matrix — cases × versions grid + per-model rollup ----

// One cell of the eval matrix: a case's outcome on a version column.
// `plumbing_ok` means the case ran to completion but no grader judged it (the
// fake-model tier); it is neither a pass nor a fail and must never read green.
export type EvalStatus = Schemas["EvalCell"]["status"];

export type EvalCell = Schemas["EvalCell"];

export type EvalMatrixRow = Schemas["EvalMatrixRow"];

// A per-model rollup across the suite. `passed`/`total` exclude non-graded
// (plumbing) rows, counted separately in `plumbing`; `completed` (⊆ `total`) is
// the graded rows whose turn actually reached a verdict, so `total > 0` with
// `completed === 0` is a model that never answered — distinct from a real 0%.
export type EvalModelSummary = Schemas["EvalModelSummary"];

export type EvalMatrix = Schemas["EvalMatrix"];

// Read the eval matrix for a suite. The matrix is filtered by suite (the real
// dimension on eval traces); `versions` caps the number of version columns.
export async function getEvalMatrix(suite: string, versions = 5): Promise<EvalMatrix> {
  const resp = await request(`/evals/matrix${query({ suite, versions })}`);
  return jsonOrThrow<EvalMatrix>(resp);
}

// ---- observability (OB1): Langfuse-backed metrics + runner-pod log proxy ----

export type MetricKey = "runs" | "latency_p95_ms" | "tokens" | "cost_usd" | "error_rate";
export type Granularity = "hour" | "day" | "week";

export type MetricsSummary = Schemas["MetricsSummary"];

export type MetricPoint = Schemas["MetricPoint"];

export type MetricSeries = Schemas["MetricSeries"];

export type PodLogs = Schemas["PodLogs"];

export type RunnerPods = Schemas["RunnerPods"];

// List the runner sandbox pods in a namespace (populates the Logs dropdown).
// Non-2xx throws ApiError carrying the status: 503 (no cluster), 502 (other).
export async function listRunnerPods(namespace?: string): Promise<RunnerPods> {
  const resp = await request(`/observability/runners${query({ namespace })}`);
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
  const resp = await request(`/observability/metrics/summary${query({ ...filter })}`);
  return jsonOrThrow<MetricsSummary>(resp);
}

export async function getMetricSeries(
  metric: MetricKey,
  granularity: Granularity,
  filter: MetricFilter = {},
): Promise<MetricSeries> {
  const resp = await request(
    `/observability/metrics/series${query({ metric, granularity, ...filter })}`,
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
  const resp = await request(
    `/observability/runners/${encodeURIComponent(namespace)}/${encodeURIComponent(pod)}/logs${query({ ...opts })}`,
  );
  return jsonOrThrow<PodLogs>(resp);
}

// ---- L1: per-agent cost, budget, and the kill switch ----

export type CostReport = Schemas["CostReport"];

// Both fields optional and nullable; absent or null means platform defaults.
// Values must be > 0.
export type BudgetConfig = Schemas["BudgetConfig"];

export type KillState = Schemas["KillState"];

// The forced-thread-reset state (#737/#735). `requested` is true from the moment
// the POST enqueues a reset until the worker's maintenance tick has actually
// released the thread's sandbox, then false. Operators poll it to know the
// release landed (mirrors the CLI reset-thread verb's wait loop).
// `route_existed` is what the worker found once it drained the reset (#3699):
// false when the key matched no route, so nothing was released; true when a
// route existed and was released; null/absent while pending, when the outcome
// expired, or against an API that predates the field.
export type ThreadResetState = Schemas["ThreadResetState"];

export async function getAgents(): Promise<AgentOut[]> {
  const resp = await request("/agents");
  return jsonOrThrow<AgentOut[]>(resp);
}

// ---- Work items (#2577): factory outcomes for a GitHub-issue-driven agent ----

export type WorkItemState = Schemas["WorkItemOutcomeOut"]["state"];

export type WorkItemPr = Schemas["WorkItemPrOut"];

export type WorkItemPublication = Schemas["WorkItemPublicationOut"];

// The console never re-derives correctness from the diff; it renders exactly
// what the API asserts (currently always unasserted, owned by the bundle).
export type WorkItemCorrectness = Schemas["WorkItemCorrectnessOut"];

export type WorkItemCi = Schemas["WorkItemCiOut"];

export type WorkItemRequest = Schemas["WorkItemRequestOut"];

export type WorkItemOutcome = Schemas["WorkItemOutcomeOut"];

export type WorkItemsList = Schemas["WorkItemOutcomeList"];

// Stage progress (#4102): the same phase_view derivation the GitHub status
// card draws, so the console and the card always agree.
export type WorkItemProgress = Schemas["WorkItemProgressOut"];

export type WorkItemStage = Schemas["WorkItemStageOut"];

// Token usage and estimated cost summed over every request of a work item (#3223).
export type WorkItemUsage = Schemas["WorkItemUsageOut"];

export async function listWorkItems(params: { agentId?: string } = {}): Promise<WorkItemsList> {
  const resp = await request(`/work-items${query({ agent_id: params.agentId })}`);
  return jsonOrThrow<WorkItemsList>(resp);
}

export async function getWorkItem(id: string): Promise<WorkItemOutcome> {
  const resp = await request(`/work-items/${encodeURIComponent(id)}`);
  return jsonOrThrow<WorkItemOutcome>(resp);
}

export async function getWorkItemUsage(id: string): Promise<WorkItemUsage> {
  const resp = await request(`/work-items/${encodeURIComponent(id)}/usage`);
  return jsonOrThrow<WorkItemUsage>(resp);
}

// The open /config endpoint (no credential required) carries the configurable
// org/workspace name the shared chrome renders.
export async function getConfig(): Promise<AppConfig> {
  const resp = await request("/config");
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
  patch: AgentUpdate,
): Promise<AgentOut> {
  const resp = await request(`/agents/${agentId}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(patch),
  });
  return jsonOrThrow<AgentOut>(resp);
}

// Move one surface binding to a new kind/address (ADR-0118). The pair being
// moved is named by `selector` (its CURRENT kind/address) and rides in the
// query string, never the body, so it can never be confused with `next`, the
// replacement value. `selector.adapter` selects the IDENTITY the pair is
// bound under (ADR-0168 decision 3) and is sent only when it is not null --
// omitting it is what keeps this working against an API that predates the
// ADR and has no such query parameter, and "default" is a real, sendable
// value equivalent to omitting it. Reply route fields are write only and
// omitted here, which tells the API to preserve them. Returns the updated
// agent.
export async function patchAgentChannel(
  agentId: string,
  selector: ChannelBinding,
  next: ChannelBinding,
): Promise<AgentOut> {
  const resp = await request(
    `/agents/${agentId}/channels${query({
      kind: selector.kind,
      address: selector.address,
      adapter: selector.adapter ?? undefined,
    })}`,
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(next),
    },
  );
  return jsonOrThrow<AgentOut>(resp);
}

export async function addAgentSurface(
  agentId: string,
  surface: ChannelBindingWrite,
): Promise<AgentOut> {
  const resp = await request(`/agents/${agentId}/channels`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(surface),
  });
  return jsonOrThrow<AgentOut>(resp);
}

// Unbind one surface. `surface.adapter` selects the IDENTITY the pair is
// bound under (ADR-0168 decision 3), on the same terms as `patchAgentChannel`
// above: sent only when not null, so this still works against an API that
// predates the ADR.
export async function removeAgentSurface(
  agentId: string,
  surface: ChannelBinding,
): Promise<void> {
  const resp = await request(
    `/agents/${agentId}/channels${query({
      kind: surface.kind,
      address: surface.address,
      adapter: surface.adapter ?? undefined,
    })}`,
    { method: "DELETE" },
  );
  if (resp.ok) return;
  const body = await resp.json().catch(() => null);
  throw new ApiError(resp.status, describeError(body) ?? resp.statusText);
}

// Delete an agent (cascades its versions/deployments server-side; 204 No Content
// on success). A 409 (active deployment) surfaces via the thrown ApiError.
export async function deleteAgent(agentId: string): Promise<void> {
  const resp = await request(`/agents/${agentId}`, { method: "DELETE" });
  if (resp.ok) return;
  const body = await resp.json().catch(() => null);
  throw new ApiError(resp.status, describeError(body) ?? resp.statusText);
}

export async function getCost(agentId: string, range: { start?: string; end?: string } = {}): Promise<CostReport> {
  const resp = await request(`/agents/${agentId}/cost${query({ ...range })}`);
  return jsonOrThrow<CostReport>(resp);
}

export async function getBudget(agentId: string): Promise<BudgetConfig> {
  const resp = await request(`/agents/${agentId}/budget`);
  return jsonOrThrow<BudgetConfig>(resp);
}

// PUT the budget. A non-positive value 422s server-side (Field(gt=0)); the
// ApiError message carries the field-level reason for inline display.
export async function putBudget(agentId: string, budget: BudgetConfig): Promise<BudgetConfig> {
  const resp = await request(`/agents/${agentId}/budget`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(budget),
  });
  return jsonOrThrow<BudgetConfig>(resp);
}

export async function getKillState(agentId: string): Promise<KillState> {
  const resp = await request(`/agents/${agentId}/kill`);
  return jsonOrThrow<KillState>(resp);
}

// ---- FX2: agent detail — versions, bundle files, and version activation ----

export type Environment = Schemas["Environment"];

export type DeploymentOut = Schemas["DeploymentOut"];

// One unwrapped file from a stored bundle. `path` is bundle-root-relative, e.g.
// "skills/deal-desk/SKILL.md" or ".claude-plugin/plugin.json".
export type BundleFile = Schemas["BundleFile"];

export type BundleFiles = Schemas["BundleFiles"];

export async function listVersions(agentId: string): Promise<VersionOut[]> {
  const resp = await request(`/agents/${agentId}/versions`);
  return jsonOrThrow<VersionOut[]>(resp);
}

export async function listDeployments(agentId: string): Promise<DeploymentOut[]> {
  const resp = await request(`/deployments${query({ agent_id: agentId })}`);
  return jsonOrThrow<DeploymentOut[]>(resp);
}

// Every deployment across all agents (GET /deployments with no agent_id). Used
// by the env-scoped Agents/Overview views to decide which agents are live in the
// selected environment.
export async function listAllDeployments(): Promise<DeploymentOut[]> {
  const resp = await request("/deployments");
  return jsonOrThrow<DeploymentOut[]>(resp);
}

/**
 * Read the unwrapped files of a version's stored bundle (FX2 headline: the agent
 * detail surface renders and edits these). A 404 means no bundle is stored for
 * the version yet; callers distinguish it via the thrown ApiError's status.
 */
export async function getVersionFiles(agentId: string, versionId: string): Promise<BundleFiles> {
  const resp = await request(`/agents/${agentId}/versions/${versionId}/files`);
  return jsonOrThrow<BundleFiles>(resp);
}

export type DeploymentCreate = Schemas["DeploymentCreate"];

// Activate a version by creating a deployment for it (status defaults to active
// server-side). This is the third step of the redeploy sequence, after POST
// version + PUT bundle.
export async function createDeployment(input: DeploymentCreate): Promise<DeploymentOut> {
  const resp = await request("/deployments", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });
  return jsonOrThrow<DeploymentOut>(resp);
}

// ---- Agent memory: inspect / edit / delete learned memory (#267) ----

// Where a memory entry was learned from (#264 Provenance shape). Provenance is
// the differentiator: an operator can see which session/traces taught a lesson.
export type MemoryProvenance = Schemas["MemoryProvenanceOut"];

// One learned memory entry. `index` is its position in the append only memory
// log. It is valid only with the accompanying parent log version.
export type MemoryEntry = Schemas["MemoryEntryOut"];

// List an agent's learned memory, oldest first (empty for a fresh agent).
export async function listMemory(agentId: string): Promise<MemoryEntry[]> {
  const resp = await request(`/agents/${agentId}/memory`);
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
  const resp = await request(`/agents/${agentId}/memory/${index}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
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
  const resp = await request(
    `/agents/${agentId}/memory/${index}${query({ expected_version: expectedVersion })}`,
    { method: "DELETE" },
  );
  if (resp.ok) return;
  const body = await resp.json().catch(() => null);
  throw new ApiError(resp.status, describeError(body) ?? resp.statusText);
}

// ---- Durable state store: operator read/inspect surface (#250) ----

// One namespace in an agent's durable state store, summarized for the inspector:
// how many keys it holds and when it was last written.
export type StateNamespace = Schemas["StateNamespaceOut"];

// A single durable state entry (a namespace+key holding arbitrary JSON), with the
// compare-and-set version and the last write time.
export type StateEntry = Schemas["StateEntryOut"];

// List the namespaces an agent has stored, most-recently-written first (empty
// for an agent that has stored nothing).
export async function listStateNamespaces(agentId: string): Promise<StateNamespace[]> {
  const resp = await request(`/agents/${agentId}/state`);
  return jsonOrThrow<StateNamespace[]>(resp);
}

// List every key stored under one namespace (get-by-key is not needed for the
// read surface: the list carries each entry's value, version, and write time).
export async function listStateEntries(agentId: string, namespace: string): Promise<StateEntry[]> {
  const resp = await request(`/agents/${agentId}/state/${encodeURIComponent(namespace)}`);
  return jsonOrThrow<StateEntry[]>(resp);
}

export async function killAgent(agentId: string): Promise<KillState> {
  const resp = await request(`/agents/${agentId}/kill`, { method: "POST" });
  return jsonOrThrow<KillState>(resp);
}

export async function resumeAgent(agentId: string): Promise<KillState> {
  const resp = await request(`/agents/${agentId}/resume`, { method: "POST" });
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
  const resp = await request(
    `/agents/${agentId}/threads/${encodeURIComponent(threadKey)}/reset`,
    { method: "POST" },
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
  const resp = await request(
    `/agents/${agentId}/threads/${encodeURIComponent(threadKey)}/reset`,
  );
  return jsonOrThrow<ThreadResetState>(resp);
}

// ---- Durable approvals: operator visibility + resolve-once (#867, ADR-0010) ----

// A durable approval record (mirrors ApprovalOut). The worker creates one when a
// run pauses on a permission/policy gate and suspends the session; an operator
// resolves it here or from the Slack card. `status` is one of
// pending/approved/rejected/expired.
export type ApprovalOut = Schemas["ApprovalOut"];

// One audit-trail entry for an approval (mirrors ApprovalAuditOut, #247): each
// resolution attempt with the authorizer snapshot that counted or refused it.
export type ApprovalAudit = Schemas["ApprovalAuditOut"];

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
  const resp = await request(
    `/approvals${query({
      status_filter: opts.status,
      agent_id: opts.agentId,
      conversation_id: opts.conversationId,
      limit: opts.limit,
    })}`,
  );
  return jsonOrThrow<ApprovalOut[]>(resp);
}

export async function getApproval(approvalId: string): Promise<ApprovalOut> {
  const resp = await request(`/approvals/${encodeURIComponent(approvalId)}`);
  return jsonOrThrow<ApprovalOut>(resp);
}

// The approval's audit trail, oldest first (empty until the first resolution
// attempt). A 404 (approval gone) surfaces via the thrown ApiError.
export async function getApprovalAudit(approvalId: string): Promise<ApprovalAudit[]> {
  const resp = await request(`/approvals/${encodeURIComponent(approvalId)}/audit`);
  return jsonOrThrow<ApprovalAudit[]>(resp);
}

export type ApprovalResolveInput = Schemas["ApprovalResolve"];

export type ConsoleSession = Schemas["ConsoleSessionOut"];

// Inspect the HttpOnly same-origin console session. No platform key is sent:
// the ambient cookie is the only credential on this identity boundary.
// Bypasses request() on purpose: a 401 here means signed out, not an expired
// session, so it must not fire the unauthorized listeners.
export async function getConsoleSession(): Promise<ConsoleSession> {
  const resp = await fetch(url("/console/session"), {
    credentials: "same-origin",
  });
  return jsonOrThrow<ConsoleSession>(resp);
}

// Exchange a single-use CLI-minted login code. The response never exposes the
// session token; the API installs it as an HttpOnly cookie.
// Bypasses request() on purpose: a 401 here means a bad code, not an expired
// session, so it must not fire the unauthorized listeners.
export async function exchangeConsoleLoginCode(code: string): Promise<ConsoleSession> {
  const resp = await fetch(url("/console/session"), {
    method: "POST",
    credentials: "same-origin",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ code }),
  });
  return jsonOrThrow<ConsoleSession>(resp);
}

// Resolve an approval (resolve-once compare-and-set) as the immutable subject
// in the HttpOnly console session. Like every call, it carries only the console
// cookie and no caller-asserted identity/channel fields. Designed failures are 401
// (session missing/revoked/expired), 403 (not authorized), 409 (already
// resolved), and 410 (expired).
export async function resolveApproval(approvalId: string, input: ApprovalResolveInput): Promise<ApprovalOut> {
  const resp = await request(`/approvals/${encodeURIComponent(approvalId)}/resolve`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });
  return jsonOrThrow<ApprovalOut>(resp);
}

// ---- Behavior packs: per-agent opt-in deterministic behaviors (#870) ----
//
// The packs ride on the agent's stored config. A NULL agent row reads back as
// the all-off default, but the schema declares every pack and every pack field
// optional, so consumers fill absent fields from the same all-off defaults.

export type LoadPack = Schemas["LoadPackConfig"];

export type TipsPack = Schemas["TipsPackConfig"];

export type GreetingPack = Schemas["GreetingPackConfig"];

export type HelpPack = Schemas["HelpPackConfig"];

// One declared user-editable runtime knob. The settings pack is schema-only today
// (the override store + per-user edit UI are a deferred runtime), so the console
// surfaces the declared knobs read-only and round-trips them unchanged.
export type SettingConfig = Schemas["SettingConfig"];

export type SettingsPack = Schemas["SettingsPackConfig"];

export type NavPack = Schemas["NavPackConfig"];

export type BehaviorPacksConfig = Schemas["BehaviorPacksConfig"];

export async function getBehaviorPacks(agentId: string): Promise<BehaviorPacksConfig> {
  const resp = await request(`/agents/${agentId}/behavior-packs`);
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
  const resp = await request(`/agents/${agentId}/behavior-packs`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
  });
  return jsonOrThrow<BehaviorPacksConfig>(resp);
}
