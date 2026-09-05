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
import type { AgentChannel } from "../lib/channels";
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
  | "tiers"
  | "resources"
  | "observability"
  | "canvas"
  | "commands"
  | "activity"
  | "settings";

/** Every route, in sidebar order. Exported so the menu, the keyboard shortcuts
 *  and the placement map cannot drift from the type. */
export const ROUTES: readonly Route[] = [
  "overview",
  "build",
  "tiers",
  "resources",
  "canvas",
  "activity",
  // Commands and Settings are the foot of the sidebar, below the spacer.
  "commands",
  "settings",
];

/**
 * Values a contextual control hands to the command form it opens.
 *
 * This is what makes a button on an agent row better than the same command
 * found by searching: "Memory" on `billing-bot` opens
 * `curie local memory billing-bot`, already filled, rather than a blank form
 * with the agent name to be retyped. Agent-scoped commands take the agent as a
 * *positional*, so the sticky-flag mechanism cannot carry it -- hence this.
 *
 * It is a seed, not a lock: the form is fully editable afterwards, and the
 * rendered command string still shows exactly what will run.
 */
export interface Prefill {
  readonly positionals?: readonly string[];
  readonly flags?: Readonly<Record<string, string | boolean>>;
}

export interface AgentSummary {
  id: string;
  name: string;
  model?: string | null;
  thinking?: string | null;
  repo_full_name?: string | null;
  secrets?: string[] | null;
  approval_required_tools?: string[] | null;
  /** ADR-0118: an agent holds SEVERAL bindings. Read these through
   *  `lib/channels.ts` rather than indexing them at each call site. */
  channels?: readonly AgentChannel[] | null;
  created_at?: string;
}

interface AppValue {
  readonly route: Route;
  /** `focus` names something inside the destination -- a command id, an agent
   *  name -- and is passed with the route so the two can never be set in the
   *  wrong order. */
  navigate(route: Route, focus?: string, prefill?: Prefill): void;
  /** Set when a route wants to land on something specific -- a command id from
   *  the palette, an agent from the canvas. Consumed by the target view. */
  readonly focus: string | null;
  setFocus(value: string | null): void;
  /** Seed values for whatever `focus` names. Set and cleared with `focus`, so a
   *  stale prefill can never land on the next command opened without one. */
  readonly prefill: Prefill | null;

  readonly env: ShellEnvironment | null;
  refreshEnv(): void;

  readonly theme: ThemeState | null;
  setTheme(preference: ThemePreference): void;

  readonly workspaces: readonly Workspace[];
  readonly workspace: Workspace | null;
  selectWorkspace(path: string | null): void;
  openWorkspace(): Promise<void>;
  forgetWorkspace(path: string): Promise<void>;
  /** Delete the bundle directory. Resolves to the shell's verdict. */
  deleteWorkspace(path: string): Promise<{ ok: true } | { ok: false; error: string }>;
  /** Scaffold a new agent from a template and open it. */
  createAgent(opts: {
    parentDir: string;
    name: string;
    files: Record<string, string>;
  }): Promise<{ ok: true } | { ok: false; error: string }>;

  readonly api: ApiConnection | null;
  connectApi(baseUrl: string, apiKey: string | null): Promise<void>;
  signOutApi(): Promise<void>;
  refreshApi(): void;

  readonly agents: readonly AgentSummary[];
  readonly agentsError: string | null;
  refreshAgents(): void;

  /** Values remembered across command forms (see STICKY_FLAGS). */
  readonly sticky: Readonly<Record<string, string>>;
  remember(flag: string, value: string): void;

  readonly paletteOpen: boolean;
  setPaletteOpen(open: boolean): void;

  /**
   * The command whose form is open in the run sheet, if any.
   *
   * A control on an agent's row that navigated to the Commands list would be
   * answering "where do I do this" with "go to the list and find it" -- which is
   * the thing the placement map exists to stop. So a contextual control opens
   * the same generated form *in place*, over the screen the operator is already
   * reading, and the list stays what it is: the reference.
   */
  readonly runTarget: { readonly id: string; readonly prefill: Prefill | null } | null;
  runCommand(id: string, prefill?: Prefill): void;
  closeRun(): void;
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
  const [focus, setFocusState] = useState<string | null>(null);
  const [prefill, setPrefill] = useState<Prefill | null>(null);
  const [runTarget, setRunTarget] = useState<{ id: string; prefill: Prefill | null } | null>(null);
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

  /**
   * A command finished, so what the platform says may have changed.
   *
   * Without this the agent list only refetched when the API's *reachability*
   * flipped, which for a stack that stays up means never: deploying an agent
   * left every view that reads `agents` -- the Build panel's "Running now", the
   * list's live dot, the Overview count, the Canvas -- showing the state from
   * before the deploy. It looked like a thirty-second lag and was really "not
   * until something else happened to refetch".
   *
   * Any successful run, not an allowlist of the ones that mutate the platform.
   * That list would be a second copy of the command surface, ninety-three
   * entries and growing, and it would be wrong the first time somebody added a
   * command without remembering it existed. One GET against localhost after a
   * command the operator sat and watched is not worth the bookkeeping.
   */
  useEffect(() => {
    return bridge().cli.onResult((result) => {
      if (result.state === "ok") void refreshAgents();
    });
  }, [refreshAgents]);

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

  /**
   * Keep asking whether the API still answers.
   *
   * It used to be probed exactly once, at mount, and after that only when
   * somebody pressed Recheck or saved Settings. So the toolbar went on saying
   * "Connected" and the Overview went on saying "the platform is up" for as
   * long as the window stayed open after the stack went down -- a status
   * surface reporting a dead API as live, which is the one thing this app's
   * rules say a monitor must never do.
   *
   * Fifteen seconds: one `GET /config` is cheap against localhost and polite
   * against a cluster, and it is the same order as the metrics poll next to it.
   * `connection()` swallows its own failures and returns `reachable: false`, so
   * a down API produces a state change rather than an unhandled rejection.
   */
  useEffect(() => {
    let cancelled = false;
    const tick = () => {
      void bridge()
        .api.connection()
        .then((next) => {
          if (!cancelled) setApi(next);
          // The agent list on the same tick. Refreshing it only after a run
          // this app started covers your own actions and nothing else: an
          // agent deployed or deleted from a terminal, or by a colleague
          // against a shared API, left this window asserting the opposite
          // indefinitely -- "Running now" for an agent that had been deleted
          // half an hour ago. Two reads against the same endpoint the
          // connection probe already talks to.
          if (!cancelled && next.reachable) void refreshAgents();
        });
    };
    const t = setInterval(tick, 15_000);
    return () => {
      cancelled = true;
      clearInterval(t);
    };
  }, [refreshAgents]);

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

  const createAgent = useCallback(
    async (opts: { parentDir: string; name: string; files: Record<string, string> }) => {
      const res = await bridge().workspace.createAgent(opts);
      await refreshWorkspaces();
      // Open what was just made. Creating an agent and then leaving the operator
      // on the list they started from is the app asking "so where is it".
      if (res.ok) setWorkspacePath(res.workspace.path);
      return res.ok ? ({ ok: true } as const) : res;
    },
    [refreshWorkspaces],
  );

  /** Delete the bundle from disk. The shell decides whether it may; this only
   *  refreshes the list and hands the refusal back for the caller to show. */
  const deleteWorkspace = useCallback(
    async (path: string) => {
      const res = await bridge().workspace.delete(path);
      await refreshWorkspaces();
      return res;
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
        setPrefill(null);
        return setFocusState(target.slice("commands:".length));
      }
      if ((ROUTES as readonly string[]).includes(target)) setRoute(target as Route);
    });
    return off;
  }, [openWorkspace]);

  const connectApi = useCallback(async (baseUrl: string, apiKey: string | null) => {
    setApi(await bridge().api.connect(baseUrl, apiKey));
  }, []);

  const signOutApi = useCallback(async () => {
    setApi(await bridge().api.signOut());
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

  // Focus and prefill move together: a prefill outliving the focus it was meant
  // for would silently fill the *next* command someone opened.
  const setFocus = useCallback((value: string | null) => {
    setFocusState(value);
    setPrefill(null);
  }, []);

  const runCommand = useCallback((id: string, nextPrefill?: Prefill) => {
    setRunTarget({ id, prefill: nextPrefill ?? null });
  }, []);

  const closeRun = useCallback(() => setRunTarget(null), []);

  const navigate = useCallback((next: Route, nextFocus?: string, nextPrefill?: Prefill) => {
    // Moving somewhere else dismisses the sheet: a panel floating over a screen
    // it was not opened from has lost the context that made it make sense.
    setRunTarget(null);
    setRoute(next);
    setFocusState(nextFocus ?? null);
    setPrefill(nextFocus ? (nextPrefill ?? null) : null);
  }, []);

  const value = useMemo<AppValue>(
    () => ({
      route,
      navigate,
      focus,
      setFocus,
      prefill,
      env,
      refreshEnv,
      theme,
      setTheme,
      workspaces,
      workspace,
      selectWorkspace: setWorkspacePath,
      openWorkspace,
      forgetWorkspace,
      deleteWorkspace,
      createAgent,
      api,
      connectApi,
      signOutApi,
      refreshApi,
      agents,
      agentsError,
      refreshAgents,
      sticky,
      remember,
      paletteOpen,
      setPaletteOpen,
      runTarget,
      runCommand,
      closeRun,
    }),
    [
      route,
      navigate,
      focus,
      setFocus,
      prefill,
      env,
      refreshEnv,
      theme,
      setTheme,
      workspaces,
      workspace,
      openWorkspace,
      forgetWorkspace,
      deleteWorkspace,
      createAgent,
      api,
      connectApi,
      signOutApi,
      refreshApi,
      agents,
      agentsError,
      refreshAgents,
      sticky,
      remember,
      paletteOpen,
      runTarget,
      runCommand,
      closeRun,
    ],
  );

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useApp(): AppValue {
  const value = useContext(Ctx);
  if (!value) throw new Error("useApp must be used inside <AppProvider>");
  return value;
}
