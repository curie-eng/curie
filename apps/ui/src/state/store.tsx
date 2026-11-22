import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useReducer,
  useRef,
  type Dispatch,
  type ReactNode,
} from "react";
import type { Action, AppState, Nav } from "./types";
import { pathForRoute, routeForPath } from "./route";

// Views other than Settings share "/". Their history entry records which one,
// because the path alone cannot. Settings is named by its path.
const SHARED_NAV = new Set<string>([
  "overview",
  "agents",
  "work-items",
  "factory",
  "memory",
  "evals",
  "observability",
  "versions",
  "connections",
]);

function restoredNav(value: unknown): Nav | null {
  if (!value || typeof value !== "object" || !("nav" in value)) return null;
  const nav = (value as { nav: unknown }).nav;
  return typeof nav === "string" && SHARED_NAV.has(nav) ? (nav as Nav) : null;
}

// The first paint opens Settings for the two Settings paths; every other URL,
// including /agents, stays on Overview.
export function navForPath(pathname: string): Nav {
  return routeForPath(pathname).nav;
}

export function initialState(): AppState {
  const pathname = typeof window === "undefined" ? "/" : window.location.pathname;
  const route = routeForPath(pathname);
  // "/" does not name the view. A reload keeps the history entry, so the first
  // paint shows the view that entry recorded. A fresh load has none and stays
  // on Overview. A Settings path names its own view and ignores the entry.
  const recorded = typeof window === "undefined" ? null : restoredNav(window.history.state);
  const nav = pathname === "/" && recorded ? recorded : route.nav;
  return {
    nav,
    settingsSection: route.settingsSection,
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
    case "settingsSection":
      return {
        ...s,
        nav: "settings",
        settingsSection: a.section,
        agentDetail: null,
        traceOpen: null,
        tracesAgentId: null,
        logsPod: null,
      };
    case "locate": {
      const route = routeForPath(a.pathname);
      const nav = a.pathname === "/" && a.nav && SHARED_NAV.has(a.nav) ? a.nav : route.nav;
      return {
        ...s,
        nav,
        settingsSection: route.settingsSection,
        agentDetail: null,
        traceOpen: null,
        tracesAgentId: null,
        logsPod: null,
      };
    }
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

  // Keep the address bar on the section the console shows, and show the section
  // the bar names after Back or Forward. The first render adopts the URL it was
  // opened at, so an unknown path is left alone until the person navigates.
  // Entries for "/" carry the view, because Agents and Overview share that path.
  const synced = useRef({ path: pathForRoute(state.nav, state.settingsSection), nav: state.nav });
  useEffect(() => {
    const onPop = (event: PopStateEvent) =>
      dispatch({ type: "locate", pathname: window.location.pathname, nav: restoredNav(event.state) });
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);
  useEffect(() => {
    const path = pathForRoute(state.nav, state.settingsSection);
    if (path === synced.current.path && state.nav === synced.current.nav) return;
    synced.current = { path, nav: state.nav };
    // Already showing what the address names (after Back or Forward): no new entry.
    const shown = routeForPath(window.location.pathname);
    const shownPath = pathForRoute(shown.nav, shown.settingsSection);
    const recorded = restoredNav(window.history.state);
    const already =
      shownPath === path &&
      (path !== "/" || recorded === state.nav || (recorded === null && shown.nav === state.nav));
    if (!already) window.history.pushState({ nav: state.nav }, "", path);
  }, [state.nav, state.settingsSection]);

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
