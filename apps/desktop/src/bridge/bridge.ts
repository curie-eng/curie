// Typed access to the native shell, plus the one place that copes with it not
// being there.
//
// The renderer is a plain web app: it runs under Vitest and under `vite dev` in
// a normal tab, where `window.curie` does not exist. Rather than sprinkle
// `if (window.curie)` through the views, this module substitutes a shell that
// refuses every privileged call with a legible message. The UI then renders its
// real states -- "curie is not reachable from here" -- instead of blank panels,
// and tests get a seam to stub.

import type { CurieBridge } from "../../electron/shared/contract";
import { webConnection, webRequest, webSignOut } from "./webApi";

export type {
  ApiConnection,
  DaemonCapacity,
  PortBinding,
  ApiRequest,
  ApiResponse,
  CliInvocation,
  CurieBridge,
  ResolvedCommand,
  ResourceFrame,
  ResourceSample,
  RunChunk,
  RunHandle,
  RunResult,
  RunState,
  ShellEnvironment,
  ThemePreference,
  ThemeState,
  Workspace,
} from "../../electron/shared/contract";

declare global {
  interface Window {
    curie?: CurieBridge;
    curieNav?: { onNavigate(cb: (route: string) => void): () => void };
  }
}

export class NoShellError extends Error {
  constructor(what: string) {
    super(`${what} needs the Curie desktop shell; this window is running without it.`);
    this.name = "NoShellError";
  }
}

const noop = () => () => {};
const reject = (what: string) => () => Promise.reject(new NoShellError(what));

const detached: CurieBridge = {
  env: async () => ({
    cliPath: null,
    cliVersion: null,
    sourceCheckout: false,
    repoRoot: null,
    dockerAvailable: false,
    kubectlAvailable: false,
    helmAvailable: false,
    platform: "darwin",
    // Empty, not a guess: with no shell there is no working directory to report,
    // and the UI renders "not known yet" rather than inventing a path.
    defaultCwd: "",
    appVersion: "0.0.0",
    electronVersion: "",
    chromeVersion: "",
    drift: null,
  }),
  cli: {
    run: reject("Running a command"),
    cancel: async () => {},
    write: async () => {},
    onChunk: noop,
    onResult: noop,
  },
  resources: {
    start: async () => {},
    stop: async () => {},
    onFrame: noop,
    logs: async () => "",
  },
  dialog: {
    pick: reject("choose a path"),
    pathForFile: () => null,
  },

  workspace: {
    list: async () => [],
    open: async () => null,
    add: async () => null,
    forget: async () => {},
    delete: reject("delete a bundle"),
    createAgent: reject("create an agent"),
    files: async () => [],
    readFile: reject("Reading a bundle file"),
    writeFile: reject("Writing a bundle file"),
    revealInFileManager: async () => {},
  },
  // The one part of the detached shell that is not a refusal. Without a main
  // process the API is still reachable, same-origin, the way `apps/ui` reaches
  // it -- so a browser tab is a working console rather than a demo of one. See
  // `webApi.ts`; credentials are cookies, never a key.
  api: {
    connection: webConnection,
    // Pointing a browser tab at a different API is not this app's job: the
    // origin serving the page decides, via its own proxy. In the shell this is
    // a real setting.
    connect: reject("Choosing a different platform API"),
    signOut: webSignOut,
    request: webRequest,
  },
  secrets: {
    list: async () => [],
    set: reject("Saving a secret"),
    unset: reject("Removing a secret"),
  },
  graph: { load: async () => null, save: async () => {} },
  theme: {
    get: async () => ({ preference: "system" as const, effective: "dark" as const, appearance: "dark" as const }),
    set: reject("Changing the theme"),
    onChange: noop,
  },

  shell: {
    openExternal: async (url: string) => {
      window.open(url, "_blank", "noopener");
    },
    copy: async (text: string) => navigator.clipboard?.writeText(text),
  },
};

export function bridge(): CurieBridge {
  return window.curie ?? detached;
}

/** True when the privileged surface is actually present. Views use this to
 *  explain themselves rather than to hide -- an unavailable action stays
 *  visible and says why it cannot run. */
export function hasShell(): boolean {
  return typeof window !== "undefined" && !!window.curie;
}
