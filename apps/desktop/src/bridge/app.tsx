// App-wide state: where we are, what machine we are on, which bundle is open,
// and which platform API we are pointed at.
//
// The "context" here is the thing that makes the GUI worth using over the raw
// CLI. Almost every `curie` command needs the same handful of values --
// `--plugin-dir`, `--api-url`, `--api-key`, `--namespace`, `--agent` -- and
// retyping them is the actual cost of driving this CLI by hand. The app holds
// them once and pre-fills every form from them, while still showing the fully
// expanded command so nothing is happening off-screen.

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";

import { bridge } from "./bridge";
import type {
  ApiConnection,
  ShellEnvironment,
  ThemePreference,
  ThemeState,
  Workspace,
} from "./bridge";

export type Route =
  | "overview"
  | "build"
  | "resources"
  | "canvas"
  | "commands"
  | "activity"
  | "settings";

export interface AgentSummary {
  id: string;
  name: string;
  model?: string | null;
  thinking?: string | null;
  repo_full_name?: string | null;
  secrets?: string[] | null;
  approval_required_tools?: string[] | null;
  channel?: { kind?: string; channel_id?: string; workspace_id?: string } | null;
  created_at?: string;
}

interface AppValue {
  readonly route: Route;
  /** `focus` names something inside the destination -- a command id, an agent
   *  name -- and is passed with the route so the two can never be set in the
   *  wrong order. */
  navigate(route: Route, focus?: string): void;
  /** Set when a route wants to land on something specific -- a command id from
   *  the palette, an agent from the canvas. Consumed by the target view. */
  readonly focus: string | null;
  setFocus(value: string | null): void;

  readonly env: ShellEnvironment | null;
  refreshEnv(): void;

  readonly theme: ThemeState | null;
  setTheme(preference: ThemePreference): void;

  readonly workspaces: readonly Workspace[];
  readonly workspace: Workspace | null;
  selectWorkspace(path: string | null): void;
  openWorkspace(): Promise<void>;
  forgetWorkspace(path: string): Promise<void>;

  readonly api: ApiConnection | null;
  connectApi(baseUrl: string, apiKey: string | null): Promise<void>;
  refreshApi(): void;

  readonly agents: readonly AgentSummary[];
  readonly agentsError: string | null;
  refreshAgents(): void;

  /** Values remembered across command forms (see STICKY_FLAGS). */
  readonly sticky: Readonly<Record<string, string>>;
  remember(flag: string, value: string): void;

  readonly paletteOpen: boolean;
  setPaletteOpen(open: boolean): void;
}

const Ctx = createContext<AppValue | null>(null);

const STICKY_KEY = "curie.desktop.sticky";

function loadSticky(): Record<string, string> {
  try {
    return JSON.parse(localStorage.getItem(STICKY_KEY) ?? "{}") as Record<string, string>;
  } catch {
    return {};
  }
}

export function AppProvider({ children }: { children: ReactNode }) {
  const [route, setRoute] = useState<Route>("overview");
  const [focus, setFocus] = useState<string | null>(null);
  const [env, setEnv] = useState<ShellEnvironment | null>(null);
  const [workspaces, setWorkspaces] = useState<readonly Workspace[]>([]);
  const [workspacePath, setWorkspacePath] = useState<string | null>(null);
  const [api, setApi] = useState<ApiConnection | null>(null);
  const [agents, setAgents] = useState<readonly AgentSummary[]>([]);
  const [agentsError, setAgentsError] = useState<string | null>(null);
  const [theme, setThemeState] = useState<ThemeState | null>(null);
  const [sticky, setSticky] = useState<Record<string, string>>(loadSticky);
  const [paletteOpen, setPaletteOpen] = useState(false);

  const refreshEnv = useCallback(() => {
    void bridge().env().then(setEnv);
  }, []);

  const refreshWorkspaces = useCallback(async () => {
    const list = await bridge().workspace.list();
    setWorkspaces(list);
    // Keep a selection if one is still valid; otherwise fall to the most
    // recently opened bundle so the app is never in a "no bundle" limbo when
    // one is available.
    setWorkspacePath((prev) => (prev && list.some((w) => w.path === prev) ? prev : (list[0]?.path ?? null)));
  }, []);

  /**
   * Put the effective theme on <html>, which is what `styles.css` keys the
   * palette off.
   *
   * Written to the DOM rather than held only in React state because the palette
   * is CSS, not props: every colour in `tokens.ts` is a `var(--x)`, so one
   * attribute swaps sixteen screens at once and no component re-renders to
   * change colour.
   */
  const applyTheme = useCallback((next: ThemeState) => {
    setThemeState(next);
    document.documentElement.dataset.theme = next.effective;
  }, []);

  const setTheme = useCallback(
    (preference: ThemePreference) => {
      void bridge().theme.set(preference).then(applyTheme);
    },
    [applyTheme],
  );

  const refreshApi = useCallback(() => {
    void bridge().api.connection().then(setApi);
  }, []);

  const refreshAgents = useCallback(async () => {
    const res = await bridge().api.request<AgentSummary[]>({ method: "GET", path: "/agents" });
    if (res.ok && Array.isArray(res.body)) {
      setAgents(res.body);
      setAgentsError(null);
    } else {
      // An empty list and a failed call look identical if you only keep the
      // list, so the error is kept alongside it and the views say which is which.
      setAgents([]);
      setAgentsError(res.error ?? "could not read /agents");
    }
  }, []);

  // One awaited pass at mount rather than three fire-and-forget calls: the
  // `cancelled` guard means a window closed mid-probe cannot land state on an
  // unmounted tree, and awaiting keeps the setStates out of the effect body.
  useEffect(() => {
    let cancelled = false;
    void (async () => {
      const [shellEnv, list, connection] = await Promise.all([
        bridge().env(),
        bridge().workspace.list(),
        bridge().api.connection(),
      ]);
      if (cancelled) return;
      setEnv(shellEnv);
      setWorkspaces(list);
      setWorkspacePath((prev) =>
        prev && list.some((w) => w.path === prev) ? prev : (list[0]?.path ?? null),
      );
      setApi(connection);
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!api?.reachable) return;
    let cancelled = false;
    void (async () => {
      const res = await bridge().api.request<AgentSummary[]>({ method: "GET", path: "/agents" });
      if (cancelled) return;
      if (res.ok && Array.isArray(res.body)) {
        setAgents(res.body);
        setAgentsError(null);
      } else {
        setAgents([]);
        setAgentsError(res.error ?? "could not read /agents");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [api?.reachable]);

  const openWorkspace = useCallback(async () => {
    const opened = await bridge().workspace.open();
    await refreshWorkspaces();
    if (opened) setWorkspacePath(opened.path);
  }, [refreshWorkspaces]);

  const forgetWorkspace = useCallback(
    async (path: string) => {
      await bridge().workspace.forget(path);
      await refreshWorkspaces();
    },
    [refreshWorkspaces],
  );

  // Read the theme once, then follow the shell. The subscription matters only
  // while the preference is "system", which is the default, so most installs
  // depend on it.
  useEffect(() => {
    let cancelled = false;
    void bridge()
      .theme.get()
      .then((state) => {
        if (!cancelled) applyTheme(state);
      });
    const off = bridge().theme.onChange(applyTheme);
    return () => {
      cancelled = true;
      off();
    };
  }, [applyTheme]);

  // The native menu drives navigation through one channel; a route it sends
  // that this app does not know is ignored rather than crashing the view.
  useEffect(() => {
    const off = window.curieNav?.onNavigate((target) => {
      if (target === "palette") return setPaletteOpen(true);
      if (target === "workspace:open") return void openWorkspace();
      if (target.startsWith("commands:")) {
        setRoute("commands");
        return setFocus(target.slice("commands:".length));
      }
      const known: Route[] = [
        "overview",
        "build",
        "resources",
        "canvas",
        "commands",
        "activity",
        "settings",
      ];
      if ((known as string[]).includes(target)) setRoute(target as Route);
    });
    return off;
  }, [openWorkspace]);

  const connectApi = useCallback(async (baseUrl: string, apiKey: string | null) => {
    setApi(await bridge().api.connect(baseUrl, apiKey));
  }, []);

  const remember = useCallback((flag: string, value: string) => {
    setSticky((prev) => {
      if (prev[flag] === value) return prev;
      const next = { ...prev, [flag]: value };
      try {
        localStorage.setItem(STICKY_KEY, JSON.stringify(next));
      } catch {
        // A full or disabled localStorage is not a reason to lose the value for
        // this session; it just will not survive a restart.
      }
      return next;
    });
  }, []);

  const workspace = useMemo(
    () => workspaces.find((w) => w.path === workspacePath) ?? null,
    [workspaces, workspacePath],
  );

  const navigate = useCallback((next: Route, nextFocus?: string) => {
    setRoute(next);
    setFocus(nextFocus ?? null);
  }, []);

  const value = useMemo<AppValue>(
    () => ({
      route,
      navigate,
      focus,
      setFocus,
      env,
      refreshEnv,
      theme,
      setTheme,
      workspaces,
      workspace,
      selectWorkspace: setWorkspacePath,
      openWorkspace,
      forgetWorkspace,
      api,
      connectApi,
      refreshApi,
      agents,
      agentsError,
      refreshAgents,
      sticky,
      remember,
      paletteOpen,
      setPaletteOpen,
    }),
    [
      route,
      navigate,
      focus,
      env,
      refreshEnv,
      theme,
      setTheme,
      workspaces,
      workspace,
      openWorkspace,
      forgetWorkspace,
      api,
      connectApi,
      refreshApi,
      agents,
      agentsError,
      refreshAgents,
      sticky,
      remember,
      paletteOpen,
    ],
  );

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useApp(): AppValue {
  const value = useContext(Ctx);
  if (!value) throw new Error("useApp must be used inside <AppProvider>");
  return value;
}
