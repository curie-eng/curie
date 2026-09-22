export type Nav =
  | "overview"
  | "agents"
  | "work-items"
  | "evals"
  | "observability"
  | "versions"
  | "connections"
  | "settings";

export type Env = "prod" | "dev";

export type ObsTab = "traces" | "metrics" | "logs" | "memory" | "usage" | "cost" | "approvals";
export type ModalKind = "new-agent";

// A plugin-format validator issue, surfaced inline when a wired deploy is
// rejected (mirrors api/client BundleIssue without importing across layers).
export interface DeployIssue {
  code: string;
  message: string;
  location: string;
}

export interface AppState {
  nav: Nav;
  env: Env;
  obsTab: ObsTab;
  modal: ModalKind | null;
  toast: string | null;
  confetti: boolean;
  confettiDone: boolean;
  deploying: boolean;
  agentDetail: string | null;
  traceOpen: string | null;
  // When set, the Traces list opens pre-filtered to this agent id; null means
  // all agents. Set by an agent card's "View traces" action.
  tracesAgentId: string | null;
  // When set, the Logs tab opens preselected to this runner pod / sandbox id.
  // Set by the trace detail's "View sandbox logs" action; null otherwise.
  logsPod: string | null;
  // Wired-deploy feedback surfaced in the create-agent modal.
  deployIssues: DeployIssue[] | null;
  deployError: string | null;
  // Eval cases promoted from real traces (#259), newest first. Anonymized by the
  // API before they land here.
  promotedEvalCases: PromotedEvalCase[];
}

// An anonymized eval case promoted from a trace (mirrors the API EvalCaseOut).
export interface PromotedEvalCase {
  id: string;
  input: string;
  grader: {
    kind: "exact" | "contains" | "regex" | "tool_called";
    expected: string;
    case_sensitive: boolean;
  };
}

export type Action =
  | { type: "go"; nav: Nav }
  | { type: "openModal"; modal: ModalKind }
  | { type: "closeModal" }
  | { type: "toast"; message: string | null }
  | { type: "setEnv"; env: Env }
  | { type: "setObsTab"; tab: ObsTab }
  | { type: "viewTraces"; agentId: string | null }
  | { type: "openTrace"; id: string }
  | { type: "closeTrace" }
  | { type: "openLogs"; sandboxId: string }
  | { type: "openAgentDetail"; id: string }
  | { type: "closeAgentDetail" }
  | { type: "deployStart" }
  | { type: "confettiFire" }
  | { type: "deployFailedValidation"; issues: DeployIssue[] }
  | { type: "deployFailed"; message: string }
  | { type: "clearDeployErrors" }
  | { type: "addPromotedEvalCase"; evalCase: PromotedEvalCase }
  | { type: "confettiDone" };
