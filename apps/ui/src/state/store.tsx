import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useReducer,
  type Dispatch,
  type ReactNode,
} from "react";
import type { Action, AppState } from "./types";

export function initialState(): AppState {
  return {
    nav: "overview",
    env: "prod",
    obsTab: "traces",
    modal: null,
    toast: null,
    confetti: false,
    confettiDone: false,
    deploying: false,
    agentDetail: null,
    traceOpen: null,
    tracesAgentId: null,
    logsPod: null,
    deployIssues: null,
    deployError: null,
    promotedEvalCases: [],
  };
}

export function reducer(s: AppState, a: Action): AppState {
  switch (a.type) {
    case "go":
      return { ...s, nav: a.nav, agentDetail: null, traceOpen: null, tracesAgentId: null, logsPod: null };
    case "openModal":
      return { ...s, modal: a.modal };
    case "closeModal":
      return { ...s, modal: null, deployIssues: null, deployError: null };
    case "toast":
      return { ...s, toast: a.message };
    case "addPromotedEvalCase":
      // Newest first; dedupe by id so re-promoting the same trace replaces it.
      return {
        ...s,
        promotedEvalCases: [
          a.evalCase,
          ...s.promotedEvalCases.filter((c) => c.id !== a.evalCase.id),
        ],
      };
    case "setEnv":
      return { ...s, env: a.env };
    case "setObsTab":
      // A manual tab switch clears any sandbox-logs prefill (openLogs sets the
      // tab directly, so its prefill survives; a subsequent user click drops it).
      return { ...s, obsTab: a.tab, traceOpen: null, tracesAgentId: null, logsPod: null };
    case "viewTraces":
      // Jump to the Traces tab pre-filtered to one agent.
      return {
        ...s,
        nav: "observability",
        obsTab: "traces",
        agentDetail: null,
        traceOpen: null,
        tracesAgentId: a.agentId,
      };
    case "openTrace":
      return { ...s, traceOpen: a.id };
    case "openFactoryTrace":
      return { ...s, nav: "observability", obsTab: "traces", traceOpen: a.id, agentDetail: null, tracesAgentId: null, logsPod: null };
    case "closeTrace":
      return { ...s, traceOpen: null };
    case "openLogs":
      // Jump from a trace's detail into the Logs tab preselected to the serving
      // sandbox. The sandbox id doubles as the pod-name prefill (RealLogs notes
      // the sandbox_id<->pod-name assumption).
      return {
        ...s,
        nav: "observability",
        obsTab: "logs",
        traceOpen: null,
        agentDetail: null,
        tracesAgentId: null,
        logsPod: a.sandboxId,
      };
    case "openAgentDetail":
      return { ...s, agentDetail: a.id };
    case "closeAgentDetail":
      return { ...s, agentDetail: null, nav: "overview" };
    case "deployStart":
      return { ...s, deploying: true, deployIssues: null, deployError: null };
    case "confettiFire":
      // Backend-driven deploy success: close the modal, land on Overview, and
      // fire confetti once. The wired shell renders its own honest post-deploy
      // panel from real state.
      return {
        ...s,
        modal: null,
        deploying: false,
        deployIssues: null,
        deployError: null,
        nav: "overview",
        agentDetail: null,
        confetti: !s.confettiDone,
        confettiDone: true,
      };
    case "deployFailedValidation":
      return { ...s, deploying: false, deployIssues: a.issues, deployError: null };
    case "deployFailed":
      return { ...s, deploying: false, deployError: a.message, deployIssues: null };
    case "clearDeployErrors":
      return s.deployIssues || s.deployError ? { ...s, deployIssues: null, deployError: null } : s;
    case "confettiDone":
      return { ...s, confetti: false };
    default:
      return s;
  }
}

interface Store {
  state: AppState;
  dispatch: Dispatch<Action>;
  envDev: boolean;
}

const StoreContext = createContext<Store | null>(null);

export function StoreProvider({ children }: { children: ReactNode }) {
  const [state, dispatch] = useReducer(reducer, undefined, initialState);

  // Auto-clear toasts after 2.5s, matching the canon's toast lifetime.
  useEffect(() => {
    if (!state.toast) return;
    const id = setTimeout(() => dispatch({ type: "toast", message: null }), 2500);
    return () => clearTimeout(id);
  }, [state.toast]);

  const value = useMemo<Store>(
    () => ({
      state,
      dispatch,
      envDev: state.env === "dev",
    }),
    [state],
  );

  return <StoreContext.Provider value={value}>{children}</StoreContext.Provider>;
}

export function useStore(): Store {
  const s = useContext(StoreContext);
  if (!s) throw new Error("useStore must be used within StoreProvider");
  return s;
}
