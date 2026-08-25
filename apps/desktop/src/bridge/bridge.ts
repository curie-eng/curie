// Typed access to the native shell, plus the one place that copes with it not
// being there.
//
// The renderer is a plain web app: it runs under Vitest and under `vite dev` in
// a normal tab, where `window.curie` does not exist. Rather than sprinkle
// `if (window.curie)` through the views, this module substitutes a shell that
// refuses every privileged call with a legible message. The UI then renders its
// real states -- "curie is not reachable from here" -- instead of blank panels,
// and tests get a seam to stub.

import type { ApiResponse, CurieBridge } from "../../electron/shared/contract";

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
    appVersion: "0.0.0",
    electronVersion: "",
    chromeVersion: "",
    drift: null,
  }),
  cli: {
    preview: reject("Previewing a command"),
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
  workspace: {
    list: async () => [],
    open: async () => null,
    add: async () => null,
    forget: async () => {},
    files: async () => [],
    readFile: reject("Reading a bundle file"),
    writeFile: reject("Writing a bundle file"),
    revealInFileManager: async () => {},
  },
  api: {
    connection: async () => ({
      baseUrl: "",
      hasKey: false,
      reachable: false,
      checkedAt: Date.now(),
    }),
    connect: reject("Connecting to the platform API"),
    // The generic is the caller's expectation, not a promise about the body: a
    // failed call has no body, and every caller must check `ok` first anyway.
    request: async <T>() =>
      ({ status: 0, ok: false, body: undefined as T, error: "no desktop shell" }) as ApiResponse<T>,
  },
  secrets: {
    list: async () => [],
    set: reject("Saving a secret"),
    unset: reject("Removing a secret"),
  },
  graph: { load: async () => null, save: async () => {} },
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
