// The one boundary between the Chromium renderer and the native shell.
//
// Everything the UI can do that a browser tab cannot -- run the `curie` binary,
// read Docker's stats stream, open a file picker, keep secrets off the page --
// crosses here and nowhere else. Keeping the surface this small is deliberate:
// it is the seam a different shell (Tauri, a CEF host, a headless test double)
// re-implements, so the renderer never learns which shell it is running in.
//
// Types only, plus channel-name constants. No Node imports: the renderer type
// checks against this file too.

/** A single `curie ...` invocation, described structurally rather than as a
 *  pre-joined string. The shell is what turns this into argv, so the renderer
 *  never builds a shell command and nothing is ever passed through a shell. */
export interface CliInvocation {
  /** Dotted manifest path, e.g. `local.deploy`. Resolved against the committed
   *  command manifest in the main process before anything is spawned. */
  readonly action: string;
  /** Positional values, in the manifest's declared order. */
  readonly positionals?: readonly string[];
  /** `--long` flag values keyed by the manifest's `long` name. `true` renders a
   *  bare flag, a string renders `--flag value`, `false`/undefined omit it. */
  readonly flags?: Readonly<Record<string, string | boolean | undefined>>;
  /** Working directory for the run (a plugin bundle dir, usually). */
  readonly cwd?: string;
  /** Ask the CLI for `--json` and parse the payload into `result`. */
  readonly json?: boolean;
}

/** What the shell resolved an invocation to, before it runs. The renderer shows
 *  this verbatim so the operator always sees the exact command being run --
 *  the UI is never a black box wrapped around the CLI. */
export interface ResolvedCommand {
  readonly argv: readonly string[];
  /** Display form, shell-quoted for copy/paste into a terminal. */
  readonly display: string;
  readonly cwd: string;
}

export type RunState = "pending" | "running" | "ok" | "failed" | "cancelled";

export interface RunChunk {
  readonly runId: string;
  readonly stream: "stdout" | "stderr";
  readonly text: string;
  /** Millis since the run started, for the timeline gutter. */
  readonly at: number;
}

export interface RunResult {
  readonly runId: string;
  readonly state: RunState;
  readonly exitCode: number | null;
  readonly durationMs: number;
  /** Parsed `--json` payload when `json` was requested and stdout parsed. */
  readonly result?: unknown;
  /** Set when `--json` was requested but stdout was not valid JSON. */
  readonly jsonError?: string;
}

export interface RunHandle {
  readonly runId: string;
  readonly command: ResolvedCommand;
}

/** A published or exposed port. `host` is null when the port is exposed by the
 *  image but not bound to the host, which is a different fact from "bound to
 *  port 0" and the UI renders it differently. */
export interface PortBinding {
  readonly host: number | null;
  readonly container: number;
  readonly proto: string;
}

/** What the Docker daemon has to give, which is the denominator every CPU and
 *  memory number in the UI should be read against.
 *
 *  Without it a summed CPU figure is meaningless: "121%" is alarming on a
 *  2-core machine and idle on a 12-core one. Docker Desktop gets this right by
 *  always showing usage over capacity, and so should this. */
export interface DaemonCapacity {
  readonly cpus: number | null;
  readonly memBytes: number | null;
  readonly serverVersion: string | null;
}

/** One row of the resource monitor. Shaped after `docker stats` because that is
 *  the mental model operators already have, but sourced from whichever tier is
 *  live: Docker for `skill`/`local`, the platform API's runner pods for
 *  `cluster`. `origin` says which, so the UI never implies a number is more
 *  authoritative than it is. */
export interface ResourceSample {
  readonly id: string;
  readonly name: string;
  readonly origin: "docker" | "kubernetes";
  /** Agent this workload belongs to, when it could be attributed. */
  readonly agent?: string;
  /** Compose project, from `com.docker.compose.project`. Null for a container
   *  started outside compose, such as a `curie skill up` runner. */
  readonly project: string | null;
  /** Compose service name, when this container is part of a project. */
  readonly service: string | null;
  /** `runner`, `api`, `worker`, `postgres`, ... -- drives grouping and color. */
  readonly role: string;
  readonly state: string;
  readonly cpuPercent: number | null;
  readonly memBytes: number | null;
  readonly memLimitBytes: number | null;
  readonly netRxBytes: number | null;
  readonly netTxBytes: number | null;
  readonly blockReadBytes: number | null;
  readonly blockWriteBytes: number | null;
  readonly pids: number | null;
  readonly startedAt: string | null;
  readonly image: string | null;
  readonly ports: readonly PortBinding[];
  readonly at: number;
}

export interface ResourceFrame {
  readonly at: number;
  readonly samples: readonly ResourceSample[];
  /** Null when the daemon could not be asked. The UI then omits the ceiling
   *  rather than inventing one. */
  readonly capacity: DaemonCapacity | null;
  /** Present when the source could not be reached; the UI degrades honestly
   *  rather than showing a frozen last-good frame as if it were live. */
  readonly error?: string;
}

/** The host OS. Spelled out rather than reusing `NodeJS.Platform`, because this
 *  file is type-checked by the renderer too, which has no Node types. */
export type Platform = "darwin" | "win32" | "linux" | "aix" | "freebsd" | "openbsd" | "sunos";

/** How the installed CLI differs from the manifest this app was built against.
 *  See `electron/ipc/manifest.ts` for why the two directions are not equally
 *  bad. Null when the comparison could not be made at all (no CLI, or its
 *  schema output could not be read). */
export interface ManifestDrift {
  readonly cliVersion: string | null;
  /** The CLI has these; the app does not offer them. */
  readonly missingFromApp: readonly string[];
  /** The app offers these; the CLI does not have them. */
  readonly missingFromCli: readonly string[];
}

/** What the shell knows about the machine it is on, refreshed on demand. */
export interface ShellEnvironment {
  readonly cliPath: string | null;
  readonly cliVersion: string | null;
  /** True when the resolved `curie` came from a source checkout, which is what
   *  gates the `dev` command namespace. */
  readonly sourceCheckout: boolean;
  readonly repoRoot: string | null;
  readonly dockerAvailable: boolean;
  readonly kubectlAvailable: boolean;
  readonly helmAvailable: boolean;
  readonly platform: Platform;
  readonly appVersion: string;
  readonly electronVersion: string;
  readonly chromeVersion: string;
  readonly drift: ManifestDrift | null;
}

/** A plugin bundle the operator has opened. The desktop app keeps a list of
 *  these the way an editor keeps recent projects; `curie skill *` and the
 *  `deploy` commands all run against one. */
export interface Workspace {
  readonly path: string;
  readonly name: string;
  /** Parsed `.claude-plugin/plugin.json`, when present. */
  readonly plugin?: { name?: string; version?: string; description?: string };
  readonly skills: readonly string[];
  readonly hasEvals: boolean;
  readonly hasMcp: boolean;
  readonly lastOpened: number;
}

/** Platform API request proxied through the shell. Going through main rather
 *  than `fetch` in the renderer is what lets the desktop app talk to an API on
 *  any host without CORS, and keeps the API key out of the page. */
export interface ApiRequest {
  readonly method: "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
  readonly path: string;
  readonly query?: Readonly<Record<string, string | number | boolean | undefined>>;
  readonly body?: unknown;
}

export interface ApiResponse<T = unknown> {
  readonly status: number;
  readonly ok: boolean;
  readonly body: T;
  readonly error?: string;
}

export interface ApiConnection {
  readonly baseUrl: string;
  /** Never the key itself -- only whether one is held, so the renderer can show
   *  connection state without the material ever entering the page. */
  readonly hasKey: boolean;
  readonly reachable: boolean;
  readonly orgName?: string;
  readonly checkedAt: number;
}

/** The full preload surface, mirrored by `window.curie`. */
export interface CurieBridge {
  env(): Promise<ShellEnvironment>;

  cli: {
    /** Resolve an invocation to argv without running it -- powers the live
     *  command preview under every form in the app. */
    preview(inv: CliInvocation): Promise<ResolvedCommand>;
    run(inv: CliInvocation): Promise<RunHandle>;
    cancel(runId: string): Promise<void>;
    /** stdin for the interactive commands (`init`, `skill eval-init`). */
    write(runId: string, data: string): Promise<void>;
    onChunk(cb: (chunk: RunChunk) => void): () => void;
    onResult(cb: (result: RunResult) => void): () => void;
  };

  resources: {
    start(intervalMs: number): Promise<void>;
    stop(): Promise<void>;
    onFrame(cb: (frame: ResourceFrame) => void): () => void;
    /** Container/pod logs for the drill-down drawer. */
    logs(id: string, tailLines: number): Promise<string>;
  };

  workspace: {
    list(): Promise<readonly Workspace[]>;
    open(): Promise<Workspace | null>;
    add(path: string): Promise<Workspace | null>;
    forget(path: string): Promise<void>;
    /** Paths, relative to the bundle root, of the files a human edits. The
     *  walker lives in the shell because it needs the filesystem; what counts as
     *  worth showing is decided in the renderer. */
    files(root: string): Promise<readonly string[]>;
    readFile(root: string, relative: string): Promise<string>;
    writeFile(root: string, relative: string, contents: string): Promise<void>;
    revealInFileManager(path: string): Promise<void>;
  };

  api: {
    connection(): Promise<ApiConnection>;
    connect(baseUrl: string, apiKey: string | null): Promise<ApiConnection>;
    request<T = unknown>(req: ApiRequest): Promise<ApiResponse<T>>;
  };

  secrets: {
    /** Names only. Values live in the CLI's private storage and never transit
     *  this bridge in either direction. */
    list(): Promise<readonly string[]>;
    set(name: string, value: string): Promise<void>;
    unset(name: string): Promise<void>;
  };

  graph: {
    load(): Promise<unknown>;
    save(doc: unknown): Promise<void>;
  };

  shell: {
    openExternal(url: string): Promise<void>;
    copy(text: string): Promise<void>;
  };
}

export const CH = {
  env: "curie:env",
  cliPreview: "curie:cli:preview",
  cliRun: "curie:cli:run",
  cliCancel: "curie:cli:cancel",
  cliWrite: "curie:cli:write",
  cliChunk: "curie:cli:chunk",
  cliResult: "curie:cli:result",
  resStart: "curie:res:start",
  resStop: "curie:res:stop",
  resFrame: "curie:res:frame",
  resLogs: "curie:res:logs",
  wsList: "curie:ws:list",
  wsOpen: "curie:ws:open",
  wsAdd: "curie:ws:add",
  wsForget: "curie:ws:forget",
  wsFiles: "curie:ws:files",
  wsRead: "curie:ws:read",
  wsWrite: "curie:ws:write",
  wsReveal: "curie:ws:reveal",
  apiConnection: "curie:api:connection",
  apiConnect: "curie:api:connect",
  apiRequest: "curie:api:request",
  secList: "curie:sec:list",
  secSet: "curie:sec:set",
  secUnset: "curie:sec:unset",
  graphLoad: "curie:graph:load",
  graphSave: "curie:graph:save",
  openExternal: "curie:shell:open",
  copy: "curie:shell:copy",
} as const;
